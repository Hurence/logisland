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

package com.hurence.logisland.util.kafkaconnect.source;


import com.hurence.logisland.stream.StreamProperties;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.storage.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.execution.streaming.LongOffset;
import org.apache.spark.sql.execution.streaming.Offset;
import org.apache.spark.sql.execution.streaming.Source;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
    public final static StructType SCHEMA = new StructType(new StructField[] {
            new StructField(StreamProperties.KAFKA_CONNECT_CONNECTOR_PROPERTIES().getName(),
                    DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType), false, Metadata.empty()),
            new StructField(StreamProperties.KAFKA_CONNECT_KEY_CONVERTER().getName(),
                    DataTypes.StringType, false, Metadata.empty()),
            new StructField(StreamProperties.KAFKA_CONNECT_KEY_CONVERTER_PROPERTIES().getName(),
                    DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType), false, Metadata.empty()),
            new StructField(StreamProperties.KAFKA_CONNECT_VALUE_CONVERTER().getName(),
                    DataTypes.StringType, false, Metadata.empty()),
            new StructField(StreamProperties.KAFKA_CONNECT_VALUE_CONVERTER_PROPERTIES().getName(),
                    DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType), false, Metadata.empty()),
            new StructField(StreamProperties.KAFKA_CONNECT_MAX_TASKS().getName(),
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
    private final ConnectorContext connectorContext;
    private final ExecutorService executorService;
    private final List<SourceThread> sourceThreads = new ArrayList<>();
    private final OffsetBackingStore offsetBackingStore;

    private final SharedSourceTaskContext sharedSourceTaskContext;
    private final SQLContext sqlContext;
    private final Converter keyConverter;
    private final Converter valueConverter;


    public KafkaConnectStreamSource(SQLContext sqlContext,
                                    Map<String, String> connectorProperties,
                                    Converter keyConverter,
                                    Converter valueConverter,
                                    int maxTasks,
                                    Class<? extends SourceConnector> connectorClass) throws Exception {
        this.sqlContext = sqlContext;
        connector = connectorClass.newInstance();
        this.keyConverter = keyConverter;
        this.valueConverter = valueConverter;
        //handle task configurations

        //TODO: handle reconfiguration
        connectorContext = new ConnectorContext() {
            @Override
            public void requestTaskReconfiguration() {
                //do nothing
            }

            @Override
            public void raiseError(Exception e) {
                LOGGER.error("Connector " + connectorClass.getCanonicalName() + " raised error : " + e.getMessage(), e);
            }
        };
        LOGGER.info("Starting connector {}", connectorClass.getCanonicalName());
        connector.initialize(connectorContext);
        connector.start(connectorProperties);
        offsetBackingStore = new MemoryOffsetBackingStore();
        offsetBackingStore.start();
        //TODO: add different storage here
        JsonConverter internalConverter = new JsonConverter();
        sharedSourceTaskContext = new SharedSourceTaskContext(
                new OffsetStorageReaderImpl(offsetBackingStore, connectorClass.getCanonicalName(), internalConverter, internalConverter),
                new OffsetStorageWriter(offsetBackingStore, connectorClass.getCanonicalName(), internalConverter, internalConverter));
        //create tasks
        Class<? extends SourceTask> taskClass = (Class<? extends SourceTask>) connector.taskClass();
        List<Map<String, String>> configs = connector.taskConfigs(maxTasks);
        LOGGER.info("Starting {} tasks for connector {}", configs.size(), connectorClass.getCanonicalName());
        executorService = Executors.newFixedThreadPool(configs.size());
        for (Map<String, String> cfg : configs) {
            SourceTask t = taskClass.newInstance();
            SourceThread st = new SourceThread(t, sqlContext, cfg, sharedSourceTaskContext);
            executorService.execute(st.start());
            sourceThreads.add(st);
        }
    }

    @Override
    public StructType schema() {
        return SCHEMA;
    }

    @Override
    public Option<Offset> getOffset() {
        Optional<Long> offset = sharedSourceTaskContext.lastOffset();
        return offset.isPresent() ? Option.<Offset>apply(LongOffset.apply(offset.get())) : Option.<Offset>empty();

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

    @Override
    public void stop() {
        LOGGER.info("Stopping connector {}", connector.getClass().getCanonicalName());
        executorService.shutdownNow();
        sourceThreads.forEach(SourceThread::stop);
        offsetBackingStore.stop();
        connector.stop();
    }
}

