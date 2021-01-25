/**
 * Copyright (C) 2016 Hurence (support@hurence.com)
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
 */
package com.hurence.logisland.connect.sink;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;
import com.hurence.logisland.connect.AbstractKafkaConnectComponent;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.OffsetBackingStore;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Kafka {@link SinkConnector} to logisland adapter.
 *
 * @author amarziali
 */
public class KafkaConnectStreamSink extends AbstractKafkaConnectComponent<SinkConnector, SinkTask> {


    private final ListMultimap<Integer, SinkRecord> bufferedRecords = Multimaps.synchronizedListMultimap(
            Multimaps.newListMultimap(new HashMap<>(), ArrayList::new));
    private final Map<SinkTask, SimpleSinkTaskContext> contexts = new IdentityHashMap<>();

    private final Map<Integer, Tuple2<SinkTask, SimpleSinkTaskContext>> partitions = Collections.synchronizedMap(new HashMap<>());


    private final String topic;
    private final AtomicLong counter = new AtomicLong();

    /**
     * Base constructor.
     *
     * @param sqlContext          the spark sql context.
     * @param connectorProperties the connector related properties.
     * @param keyConverter        the converter for the data key
     * @param valueConverter      the converter for the data body
     * @param offsetBackingStore  the backing store implementation (can be in-memory, file based, kafka based, etc...)
     * @param maxTasks            the maximum theoretical number of tasks this source should spawn.
     * @param connectorClass      the class of kafka connect source connector to wrap.
     * @param streamId            the id of the underlying stream
     */
    public KafkaConnectStreamSink(SQLContext sqlContext,
                                  Map<String, String> connectorProperties,
                                  Converter keyConverter,
                                  Converter valueConverter,
                                  OffsetBackingStore offsetBackingStore,
                                  int maxTasks,
                                  String topic,
                                  String connectorClass,
                                  String streamId) {
        super(sqlContext, connectorProperties, keyConverter, valueConverter, offsetBackingStore, maxTasks, connectorClass, streamId);
        this.topic = topic;
    }


    @Override
    protected void initialize(SinkTask task) {
        SimpleSinkTaskContext sstc = new SimpleSinkTaskContext(topic);
        task.initialize(sstc);
        contexts.put(task, sstc);
    }

    public boolean openPartition(int partition) {
        Tuple2<SinkTask, SimpleSinkTaskContext> ret = partitions.computeIfAbsent(partition,
                part -> {
                    SinkTask task = tasks.get(partition % tasks.size());
                    TopicPartition tp = new TopicPartition(topic, part);
                    task.open(Collections.singleton(tp));
                    SimpleSinkTaskContext tk = contexts.get(task);
                    return Tuple2.apply(task, tk);
                });

        return ret._2().assignThenState(partition);
    }

    public void enqueueOnPartition(int partition, byte[] key, byte[] value) {
        SchemaAndValue keySV = keyConverter.toConnectData(topic, key);
        SchemaAndValue valueSV = valueConverter.toConnectData(topic, value);

        bufferedRecords.put(partition,
                new SinkRecord(topic,
                        partition,
                        keySV.schema(),
                        keySV.value(),
                        valueSV.schema(),
                        valueSV.value(),
                        counter.incrementAndGet()));
    }

    public void flushPartition(int partition) {
        List<SinkRecord> records = bufferedRecords.get(partition);
        if (!records.isEmpty()) {
            partitions.get(partition)._1().put(records);
            partitions.get(partition)._1().flush(Collections.singletonMap(
                    new TopicPartition(topic, partition),
                    new OffsetAndMetadata(records.get(records.size() - 1).kafkaOffset()))
            );
            bufferedRecords.removeAll(partition);

        }
    }

    @Override
    protected void stopAllTasks() {
        try {
            super.stopAllTasks();
        } finally {
            counter.set(0);
            contexts.clear();
            bufferedRecords.clear();
            partitions.clear();
        }

    }
}
