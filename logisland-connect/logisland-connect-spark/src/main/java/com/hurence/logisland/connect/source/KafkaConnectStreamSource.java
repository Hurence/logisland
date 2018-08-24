/**
 * Copyright (C) 2016 Hurence (support@hurence.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.connect.source;


import com.hurence.logisland.stream.spark.StreamOptions;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.runtime.WorkerSourceTaskContext;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.OffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetStorageReaderImpl;
import org.apache.kafka.connect.storage.OffsetStorageWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.execution.streaming.LongOffset;
import org.apache.spark.sql.execution.streaming.Offset;
import org.apache.spark.sql.execution.streaming.SerializedOffset;
import org.apache.spark.sql.execution.streaming.Source;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.Tuple2;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Kafka connect to spark sql streaming bridge.
 *
 * @author amarziali
 */
public class KafkaConnectStreamSource implements Source {

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
    private final static Logger LOGGER = LoggerFactory.getLogger(KafkaConnectStreamSource.class);
    private final SourceConnector connector;
    private final List<SourceTask> sourceTasks = new ArrayList<>();
    private final OffsetBackingStore offsetBackingStore;
    private final OffsetStorageWriter offsetStorageWriter;
    private final AtomicBoolean startWatch = new AtomicBoolean(false);
    private final String connectorName;
    private final SortedMap<Long, List<Tuple2<SourceTask, SourceRecord>>> bufferedRecords =
            Collections.synchronizedSortedMap(new TreeMap<>());
    private final SortedMap<Long, List<Tuple2<SourceTask, SourceRecord>>> uncommittedRecords =
            Collections.synchronizedSortedMap(new TreeMap<>());

    private final SQLContext sqlContext;
    private final Converter keyConverter;
    private final Converter valueConverter;
    private final int maxTasks;
    private volatile long counter = 0;


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
            this.connectorName = connectorClass.getCanonicalName();
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
                        stopAllTasks();
                        createAndStartAllTasks();
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
            //new OffsetStorageReaderImpl(offsetBackingStore, connectorClass.getCanonicalName(), internalConverter, internalConverter),

            offsetStorageWriter = new OffsetStorageWriter(offsetBackingStore, connectorClass.getCanonicalName(), internalConverter, internalConverter);
            //create and start tasks
            createAndStartAllTasks();
        } catch (Exception e) {
            try {
                stopAllTasks();
            } catch (Throwable t) {
                LOGGER.error("Unable to properly stop tasks of connector " + connectorName(), t);
            }
            throw new DataException("Unable to create connector " + connectorName(), e);
        }

    }

    /**
     * Create all the {@link Runnable} workers needed to host the source tasks.
     *
     * @return
     * @throws IllegalAccessException if task instantiation fails.
     * @throws InstantiationException if task instantiation fails.
     */
    private void createAndStartAllTasks() throws IllegalAccessException, InstantiationException {
        if (!startWatch.compareAndSet(false, true)) {
            throw new IllegalStateException("Connector is already started");
        }
        Class<? extends SourceTask> taskClass = (Class<? extends SourceTask>) connector.taskClass();
        List<Map<String, String>> configs = connector.taskConfigs(maxTasks);
        counter = 0;
        LOGGER.info("Creating {} tasks for connector {}", configs.size(), connectorName());
        for (Map<String, String> conf : configs) {
            //create the task
            SourceTask task = taskClass.newInstance();
            task.initialize(new WorkerSourceTaskContext(new OffsetStorageReaderImpl(offsetBackingStore,
                    connector.getClass().getCanonicalName(), keyConverter, valueConverter)));
            task.start(conf);
            sourceTasks.add(task);

        }
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
        return connectorName;
    }


    @Override
    public StructType schema() {
        return SCHEMA;
    }

    @Override
    public synchronized Option<Offset> getOffset() {
        if (!uncommittedRecords.isEmpty()) {
            return Option.apply(SerializedOffset.apply(Long.toString(counter++)));
        }
        if (bufferedRecords.isEmpty()) {

            List<Tuple2<SourceTask, SourceRecord>> buffer = sourceTasks.parallelStream().flatMap(sourceTask -> {
                Stream<SourceRecord> ret = Stream.empty();
                try {
                    ret = sourceTask.poll().stream();
                } catch (InterruptedException ie) {
                    LOGGER.warn("Task {} interrupted while waiting.", sourceTask.getClass().getCanonicalName());
                }
                return ret.map(sourceRecord -> Tuple2.apply(sourceTask, sourceRecord));
            }).collect(Collectors.toList());
            if (!buffer.isEmpty()) {
                LongOffset nextOffset = LongOffset.apply(counter++);
                bufferedRecords.put(nextOffset.offset(), buffer);
            }
        }
        if (!bufferedRecords.isEmpty()) {
            return Option.apply(SerializedOffset.apply(bufferedRecords.lastKey().toString()));
        }
        return Option.empty();
    }


    @Override
    public Dataset<Row> getBatch(Option<Offset> start, Offset end) {

        Long startOff = start.isDefined() ? Long.parseLong(start.get().json()) :
                !bufferedRecords.isEmpty() ? bufferedRecords.firstKey() : 0L;


        return sqlContext.createDataFrame(
                bufferedRecords.subMap(startOff, Long.parseLong(end.json()) + 1).keySet().stream()
                        .flatMap(offset -> {
                            List<Tuple2<SourceTask, SourceRecord>> srl = bufferedRecords.remove(offset);
                            if (srl != null) {
                                uncommittedRecords.put(offset, srl);
                                return srl.stream();
                            }
                            return Stream.empty();
                        })
                        .map(Tuple2::_2)
                        .map(sourceRecord -> new GenericRow(new Object[]{
                                sourceRecord.topic(),
                                sourceRecord.kafkaPartition(),
                                keyConverter.fromConnectData(sourceRecord.topic(), sourceRecord.keySchema(), sourceRecord.key()),
                                valueConverter.fromConnectData(sourceRecord.topic(), sourceRecord.valueSchema(), sourceRecord.value())
                        }))
                        .collect(Collectors.toList()), DATA_SCHEMA);

    }

    @Override
    public void commit(Offset end) {
        if (uncommittedRecords.isEmpty()) {
            return;
        }
        //first commit all offsets already given
        List<Tuple2<SourceTask, SourceRecord>> recordsToCommit =
                uncommittedRecords.subMap(uncommittedRecords.firstKey(), Long.parseLong(end.json()) + 1).keySet().stream()
                        .flatMap(key -> uncommittedRecords.remove(key).stream())
                        .collect(Collectors.toList());

        recordsToCommit.forEach(tuple -> {
            try {
                offsetStorageWriter.offset(tuple._2().sourcePartition(), tuple._2().sourceOffset());
                tuple._1().commitRecord(tuple._2());
            } catch (Exception e) {
                LOGGER.warn("Unable to commit record " + tuple._2(), e);
            }
        });
        recordsToCommit.stream().map(Tuple2::_1).distinct().forEach(sourceTask -> {
            try {
                sourceTask.commit();
            } catch (Exception e) {
                LOGGER.warn("Unable to bulk commit offset for connector " + connectorName, e);
            }
        });
        //now flush offset writer
        try {
            if (offsetStorageWriter.beginFlush()) {
                offsetStorageWriter.doFlush((error, result) -> {
                    if (error == null) {
                        LOGGER.debug("Flushing till offset {} with result {}", end, result);
                    } else {
                        LOGGER.error("Unable to commit records till source offset " + end, error);

                    }
                }).get(30, TimeUnit.SECONDS);
            }
        } catch (Exception e) {
            LOGGER.error("Unable to commit records till source offset " + end, e);
        }
    }

    /**
     * Stops every tasks running and serving for this connector.
     */
    private void stopAllTasks() {
        LOGGER.info("Stopping every tasks for connector {}", connectorName());
        while (!sourceTasks.isEmpty()) {
            try {
                sourceTasks.remove(0).stop();
            } catch (Throwable t) {
                LOGGER.warn("Error occurring while stopping a task of connector " + connectorName(), t);
            }
        }
    }

    @Override
    public void stop() {
        if (!startWatch.compareAndSet(true, false)) {
            throw new IllegalStateException("Connector is not started");
        }
        LOGGER.info("Stopping connector {}", connectorName());
        stopAllTasks();
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

