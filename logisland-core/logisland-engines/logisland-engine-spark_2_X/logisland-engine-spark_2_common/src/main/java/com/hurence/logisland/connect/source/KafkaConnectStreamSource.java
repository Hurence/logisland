/*
 *  * Copyright (C) 2018 Hurence (support@hurence.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.hurence.logisland.connect.source;


import com.hurence.logisland.connect.AbstractKafkaConnectComponent;
import com.hurence.logisland.stream.spark.provider.StreamOptions;
import com.hurence.logisland.util.spark.SparkPlatform;
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
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.streaming.Offset;
import org.apache.spark.sql.execution.streaming.SerializedOffset;
import org.apache.spark.sql.execution.streaming.Source;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.Tuple2;
import scala.collection.JavaConversions;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Kafka connect to spark sql streaming bridge.
 *
 * @author amarziali
 */
public class KafkaConnectStreamSource extends AbstractKafkaConnectComponent<SourceConnector, SourceTask> implements Source {


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
            new StructField("sourcePartition", DataTypes.StringType, false, Metadata.empty()),
            new StructField("sourceOffset", DataTypes.StringType, false, Metadata.empty()),
            new StructField("key", DataTypes.BinaryType, true, Metadata.empty()),
            new StructField("value", DataTypes.BinaryType, false, Metadata.empty())

    });
    private final static Logger LOGGER = LoggerFactory.getLogger(KafkaConnectStreamSource.class);

    private final AtomicLong counter = new AtomicLong();

    protected final OffsetStorageWriter offsetStorageWriter;
    private final SortedMap<Long, List<Tuple2<SourceTask, SourceRecord>>> bufferedRecords =
            Collections.synchronizedSortedMap(new TreeMap<>());
    private final SortedMap<Long, List<Tuple2<SourceTask, SourceRecord>>> uncommittedRecords =
            Collections.synchronizedSortedMap(new TreeMap<>());
    private final Map<SourceTask, Thread> busyTasks = Collections.synchronizedMap(new IdentityHashMap<>());

    private final SparkPlatform sparkPlatform = StreamSupport.stream(
            Spliterators.spliteratorUnknownSize(ServiceLoader.load(SparkPlatform.class).iterator(), Spliterator.ORDERED),
            false).findFirst().orElseThrow(() -> new IllegalStateException("SparkPlatform service spi not defined. " +
            "Unable to continue"));


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
                                    String connectorClass) {
        super(sqlContext, connectorProperties, keyConverter, valueConverter, offsetBackingStore, maxTasks, connectorClass);
        this.offsetStorageWriter = new OffsetStorageWriter(offsetBackingStore, connector.getClass().getCanonicalName(),
                createInternalConverter(true), createInternalConverter(false));
    }


    @Override
    protected void initialize(SourceTask task) {
        task.initialize(new WorkerSourceTaskContext(new OffsetStorageReaderImpl(offsetBackingStore,
                connector.getClass().getCanonicalName(), createInternalConverter(true), createInternalConverter(false))));
    }


    @Override
    public StructType schema() {
        return SCHEMA;
    }

    @Override
    protected void createAndStartAllTasks() throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        counter.set(0);
        super.createAndStartAllTasks();
    }

    @Override
    public synchronized Option<Offset> getOffset() {
        if (!uncommittedRecords.isEmpty()) {
            return Option.apply(SerializedOffset.apply(Long.toString(counter.incrementAndGet())));
        }
        if (bufferedRecords.isEmpty()) {
            tasks.forEach(t -> busyTasks.computeIfAbsent(t, sourceTask -> {
                Thread thread = new Thread(() -> {
                    try {
                        List<Tuple2<SourceTask, SourceRecord>> tmp = sourceTask.poll().stream()
                                .map(sourceRecord -> Tuple2.apply(sourceTask, sourceRecord))
                                .collect(Collectors.toList());
                        if (!tmp.isEmpty()) {
                            bufferedRecords.put(counter.incrementAndGet(), tmp);
                        }
                    } catch (InterruptedException ie) {
                        LOGGER.warn("Task {} interrupted while waiting.", sourceTask.getClass().getCanonicalName());
                    } finally {
                        busyTasks.remove(t);
                    }
                });
                thread.start();
                return thread;
            }));
        } else {
            return Option.apply(SerializedOffset.apply(bufferedRecords.lastKey().toString()));

        }
        return Option.empty();
    }


    @Override
    public Dataset<Row> getBatch(Option<Offset> start, Offset end) {
        Long startOff = start.isDefined() ? Long.parseLong(start.get().json()) :
                !bufferedRecords.isEmpty() ? bufferedRecords.firstKey() : 0L;

        Map<Integer, List<InternalRow>> current =
                new LinkedHashMap<>(bufferedRecords.subMap(startOff, Long.parseLong(end.json()) + 1))
                        .keySet().stream()
                        .flatMap(offset -> {
                            List<Tuple2<SourceTask, SourceRecord>> srl = bufferedRecords.remove(offset);
                            if (srl != null) {
                                uncommittedRecords.put(offset, srl);
                                return srl.stream();
                            }
                            return Stream.empty();
                        })
                        .map(Tuple2::_2)
                        .map(sourceRecord -> InternalRow.fromSeq(JavaConversions.<Object>asScalaBuffer(Arrays.asList(
                                toUTFString(sourceRecord.topic()),
                                toUTFString(sourceRecord.sourcePartition()),
                                toUTFString(sourceRecord.sourceOffset()),
                                keyConverter.fromConnectData(sourceRecord.topic(), sourceRecord.keySchema(), sourceRecord.key()),
                                valueConverter.fromConnectData(sourceRecord.topic(), sourceRecord.valueSchema(), sourceRecord.value())
                        )).toSeq()))
                        .collect(Collectors.groupingBy(row -> Objects.hashCode((row.getString(1)))));
        return sparkPlatform.createStreamingDataFrame(sqlContext, new SimpleRDD(sqlContext.sparkContext(), current), DATA_SCHEMA);


    }

    private UTF8String toUTFString(Object o) {
        if (o != null) {
            return UTF8String.fromString(o.toString());
        }
        return UTF8String.EMPTY_UTF8;
    }

    @Override
    public void commit(Offset end) {
        if (uncommittedRecords.isEmpty()) {
            return;
        }
        //first commit all offsets already given
        List<Tuple2<SourceTask, SourceRecord>> recordsToCommit =
                new LinkedHashMap<>(uncommittedRecords.subMap(uncommittedRecords.firstKey(), Long.parseLong(end.json()) + 1)).keySet().stream()
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


    @Override
    public void stop() {
        super.stop();
    }

}


