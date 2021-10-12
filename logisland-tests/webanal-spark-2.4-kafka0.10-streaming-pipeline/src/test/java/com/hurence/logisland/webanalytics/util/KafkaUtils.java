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
package com.hurence.logisland.webanalytics.util;

import com.hurence.logisland.record.Record;
import com.hurence.logisland.webanalytics.test.util.TestMappings;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.util.Map;
import java.util.concurrent.Future;

public class KafkaUtils {

    private static Logger logger = LoggerFactory.getLogger(KafkaUtils.class);

    public final KafkaEmbedded embeddedKafka;

    public KafkaUtils(KafkaEmbedded embeddedKafka) {
        this.embeddedKafka = embeddedKafka;
    }


    public void addingEventsToTopicPartition(String topicName, int partitionId, Record record) throws InterruptedException {
        String key = record.getField(TestMappings.eventsInternalFields.getTimestampField()).asString();
        addingEventsToTopicPartition(topicName, partitionId, key, record);
    }

    public void addingEventsToTopicPartition(String topicName, int partitionId, String key, Record record) throws InterruptedException {
        // Define the record we want to produce
        final ProducerRecord<String, Record> producerRecord = new ProducerRecord<String, Record>(
                topicName,
                partitionId,
                key,
                record
        );

        Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
        senderProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        senderProps.put("value.serializer", "com.hurence.logisland.serializer.KafkaRecordSerializer");
        try (final KafkaProducer<String, Record> producer = new KafkaProducer<>(senderProps)) {
            final Future<RecordMetadata> future = producer.send(producerRecord);
            producer.flush();
            while (!future.isDone()) {
                Thread.sleep(500L);
            }
            logger.trace("Produce completed:{}", producerRecord);
        }  catch (Exception e) {
            logger.error("error while sending data to kafka", e);
        }
    }
}
