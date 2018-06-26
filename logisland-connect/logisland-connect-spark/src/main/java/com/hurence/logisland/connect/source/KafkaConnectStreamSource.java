/*
 * Copyright (C) 2018 Hurence (support@hurence.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.hurence.logisland.connect.source;


import com.hurence.logisland.stream.spark.StreamOptions;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.OffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetStorageReaderImpl;
import org.apache.kafka.connect.storage.OffsetStorageWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.execution.streaming.Offset;
import org.apache.spark.sql.execution.streaming.Source;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Kafka connect to spark sql streaming bridge.
 *
 * @author amarziali
 */
public class KafkaConnectStreamSource implements Source {

    private final static Logger LOGGER = LoggerFactory.getLogger(KafkaConnectStreamSource.class);

    /**
     * The Schema used for this source.
     */
    public final static StructType SCHEMA = new StructType(new StructField[]{
            new StructField(StreamOptions.KAFKA_CONNECT_CONNECTOR_PROPERTIES().getName(),
                    DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType), false, Metadata.empty()),
            new StructField(StreamOptions.KAFKA_CONNECT_KEY_CONVERTER().getName(),
                    DataTypes.StringType, false, Metadata.empty()),
            new StructField(StreamOptions.KAFKA_CONNECT_KEY_CONVERTER_PROPERTIES().getName(),
                    DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType), false, Metadata.empty()),
            new StructField(StreamOptions.KAFKA_CONNECT_VALUE_CONVERTER().getName(),
                    DataTypes.StringType, false, Metadata.empty()),
            new StructField(StreamOptions.KAFKA_CONNECT_VALUE_CONVERTER_PROPERTIES().getName(),
                    DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType), false, Metadata.empty()),
            new StructField(StreamOptions.KAFKA_CONNECT_MAX_TASKS().getName(),
                    DataTypes.createMapType(DataTypes.IntegerType, DataTypes.StringType), false, Metadata.empty())
    });
    /**
     * The schema used to represent the outgoing dataframe.
     */
    public final static StructType DATA_SCHEMA = new StructType(new StructField[]{
            new StructField("topic", DataTypes.StringType, false, Metadata.empty()),
            new StructField("partition", DataTypes.IntegerType, true, Metadata.empty()),
            new StructField("key", DataTypes.BinaryType, true, Metadata.empty()),
            new StructField("value", DataTypes.BinaryType, false, Metadata.empty())

    });


    private final SourceConnector connector;
    private ExecutorService executorService;
    private final List<SourceThread> sourceThreads = new ArrayList<>();
    private final OffsetBackingStore offsetBackingStore;
    private final AtomicBoolean startWatch = new AtomicBoolean(false);

    private final SharedSourceTaskContext sharedSourceTaskContext;
    private final SQLContext sqlContext;
    private final Converter keyConverter;
    private final Converter valueConverter;
    private final int maxTasks;


    /**
     * Base constructor. Should be called by {@link KafkaConnectStreamSourceProvider}
     *
     * @param sqlContext          the spark sql context.
     * @param connectorProperties the connector related properties.
     * @param keyConverter        the converter for the data key
     * @param valueConverter      the converter for the data body
     * @param offsetBackingStore  the backing store implementation (can be in-memory, file based, kafka based, etc...)
     * @param maxTasks            the maximum theoretical number of tasks this source should spawn.
     * @param connectorClass      the class of kafka connect source connector to wrap.
     *                            =
     */
    public KafkaConnectStreamSource(SQLContext sqlContext,
                                    Map<String, String> connectorProperties,
                                    Converter keyConverter,
                                    Converter valueConverter,
                                    OffsetBackingStore offsetBackingStore,
                                    int maxTasks,
                                    Class<? extends SourceConnector> connectorClass) {
        try {
            this.sqlContext = sqlContext;
            this.maxTasks = maxTasks;
            //instantiate connector
            connector = connectorClass.newInstance();
            //create converters
            this.keyConverter = keyConverter;
            this.valueConverter = valueConverter;
            final Converter internalConverter = createInternalConverter();

            //Create the connector context
            final ConnectorContext connectorContext = new ConnectorContext() {
                @Override
                public void requestTaskReconfiguration() {
                    try {
                        stopAllThreads();
                        startAllThreads();
                    } catch (Throwable t) {
                        LOGGER.error("Unable to reconfigure tasks for connector " + connectorName(), t);
                    }
                }

                @Override
                public void raiseError(Exception e) {
                    LOGGER.error("Connector " + connectorName() + " raised error : " + e.getMessage(), e);
                }
            };

            LOGGER.info("Starting connector {}", connectorClass.getCanonicalName());
            connector.initialize(connectorContext);
            connector.start(connectorProperties);
            this.offsetBackingStore = offsetBackingStore;
            offsetBackingStore.start();
            sharedSourceTaskContext = new SharedSourceTaskContext(
                    new OffsetStorageReaderImpl(offsetBackingStore, connectorClass.getCanonicalName(), internalConverter, internalConverter),
                    new OffsetStorageWriter(offsetBackingStore, connectorClass.getCanonicalName(), internalConverter, internalConverter));

            //create and start tasks
            startAllThreads();
        } catch (Exception e) {
            try {
                stopAllThreads();
            } catch (Throwable t) {
                LOGGER.error("Unable to properly stop threads of connector " + connectorName(), t);
            }
            throw new DataException("Unable to create connector " + connectorName(), e);
        }

    }

    /**
     * Create all the {@link Runnable} workers needed to host the source threads.
     *
     * @return
     * @throws IllegalAccessException if task instantiation fails.
     * @throws InstantiationException if task instantiation fails.
     */
    private List<SourceThread> createThreadTasks() throws IllegalAccessException, InstantiationException {
        Class<? extends SourceTask> taskClass = (Class<? extends SourceTask>) connector.taskClass();
        List<Map<String, String>> configs = connector.taskConfigs(maxTasks);
        List<SourceThread> ret = new ArrayList<>();
        LOGGER.info("Creating {} tasks for connector {}", configs.size(), connectorName());
        for (Map<String, String> conf : configs) {
            //create the task
            final SourceThread t = new SourceThread(taskClass, conf, sharedSourceTaskContext);
            ret.add(t);
        }
        return ret;
    }

    /**
     * Start all threads.
     *
     * @throws IllegalAccessException if task instantiation fails.
     * @throws InstantiationException if task instantiation fails.
     */
    private void startAllThreads() throws IllegalAccessException, InstantiationException {
        if (!startWatch.compareAndSet(false, true)) {
            throw new IllegalStateException("Connector is already started");
        }
        //Give a meaningful name to thread belonging to this connector
        final ThreadGroup threadGroup = new ThreadGroup(connector.getClass().getSimpleName());
        final List<SourceThread> sourceThreads = createThreadTasks();
        //Configure a new executor service        ]
        executorService = Executors.newFixedThreadPool(sourceThreads.size(), r -> {
            Thread t = new Thread(threadGroup, r);
            t.setDaemon(true);
            return t;
        });
        createThreadTasks().forEach(st -> {
            executorService.execute(st.start());
            sourceThreads.add(st);
        });
    }


    /**
     * Create a converter to be used to translate internal data.
     * Child classes can override this method to provide alternative converters.
     *
     * @return an instance of {@link Converter}
     */
    protected Converter createInternalConverter() {
        JsonConverter internalConverter = new JsonConverter();
        internalConverter.configure(Collections.singletonMap("schemas.enable", "false"), false);
        return internalConverter;
    }

    /**
     * Gets the connector name used by this stream source.
     *
     * @return
     */
    private String connectorName() {
        return connector.getClass().getCanonicalName();
    }


    @Override
    public StructType schema() {
        return SCHEMA;
    }

    @Override
    public Option<Offset> getOffset() {
        Optional<Offset> offset = sharedSourceTaskContext.lastOffset();
        return Option.<Offset>apply(offset.orElse(null));

    }


    @Override
    public Dataset<Row> getBatch(Option<Offset> start, Offset end) {
        return sqlContext.createDataFrame(
                sharedSourceTaskContext.read(start.isDefined() ? Optional.of(start.get()) : Optional.empty(), end)
                        .stream()
                        .map(record -> new GenericRow(new Object[]{
                                record.topic(),
                                record.kafkaPartition(),
                                keyConverter.fromConnectData(record.topic(), record.keySchema(), record.key()),
                                valueConverter.fromConnectData(record.topic(), record.valueSchema(), record.value())
                        })).collect(Collectors.toList()),
                DATA_SCHEMA);
    }

    @Override
    public void commit(Offset end) {
        sharedSourceTaskContext.commit(end);
    }

    /**
     * Stops every threads running and serving for this connector.
     */
    private void stopAllThreads() {
        LOGGER.info("Stopping every threads for connector {}", connectorName());
        while (!sourceThreads.isEmpty()) {
            try {
                sourceThreads.remove(0).stop();
            } catch (Throwable t) {
                LOGGER.warn("Error occurring while stopping a thread of connector " + connectorName(), t);
            }
        }
    }

    @Override
    public void stop() {
        if (!startWatch.compareAndSet(true, false)) {
            throw new IllegalStateException("Connector is not started");
        }
        LOGGER.info("Stopping connector {}", connectorName());
        stopAllThreads();
        sharedSourceTaskContext.clean();
        offsetBackingStore.stop();
        connector.stop();
    }

    /**
     * Check the stream source state.
     *
     * @return
     */
    public boolean isRunning() {
        return startWatch.get();
    }
}

