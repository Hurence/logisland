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
package com.hurence.logisland.webanalytics;

import com.hurence.logisland.record.Record;
import com.hurence.logisland.webanalytics.test.util.EventsGenerator;
import com.hurence.logisland.webanalytics.util.KafkaUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

public class KafkaSimpleTest {

    private static Logger logger = LoggerFactory.getLogger(KafkaSimpleTest.class);

    final static String inputTopic1 = "my_topics_1";
    final static String inputTopic2 = "my_topics_2";

    @ClassRule
    public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(
            1, true, 2,
            inputTopic1, inputTopic2);

    private static KafkaUtils kafkaUtils = new KafkaUtils(embeddedKafka);

    @Test
    public void testWithEvent() throws Exception {
        logger.info("Starting test");
        EventsGenerator eventGen = new EventsGenerator("divolte_1");
        logger.info("Adding an event in topic");

        Record event = eventGen.generateEvent(0, "url");
        kafkaUtils.addingEventsToTopicPartition(inputTopic2, 0, event);
        kafkaUtils.addingEventsToTopicPartition(inputTopic2, 0, "session1", event);
        kafkaUtils.addingEventsToTopicPartition(inputTopic2, 0, "session2", event);

        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("sampleRawConsumer", "false", embeddedKafka);
        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("auto.offset.reset", "earliest");
        final CountDownLatch latch = new CountDownLatch(3);
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.execute(() -> {
            KafkaConsumer<String, Record> kafkaConsumer = new KafkaConsumer<>(consumerProps);
            kafkaConsumer.subscribe(Arrays.asList(inputTopic2, inputTopic1));
            try {
                while (true) {
                    ConsumerRecords<String, Record> records = kafkaConsumer.poll(100);
                    for (ConsumerRecord<String, Record> record : records) {
                        logger.info("consuming from topic = {}, partition = {}, offset = {}, key = {}, value = {}",
                                record.topic(), record.partition(), record.offset(), record.key(), record.value());
                        latch.countDown();
                    }
                }
            } finally {
                kafkaConsumer.close();
            }
        });

        assertTrue(latch.await(30, TimeUnit.SECONDS));
    }


    /**
     *
     */
    @Test
    public void simpleTest() throws Exception {
        Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
        KafkaProducer<Integer, String> producer = new KafkaProducer<>(senderProps);
        producer.send(new ProducerRecord<>(inputTopic1, 0, 0, "message0")).get();
        producer.send(new ProducerRecord<>(inputTopic1, 0, 1, "message1")).get();
        producer.send(new ProducerRecord<>(inputTopic1, 1, 2, "message2")).get();
        producer.send(new ProducerRecord<>(inputTopic1, 1, 3, "message3")).get();

        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("sampleRawConsumer", "false", embeddedKafka);
        consumerProps.put("auto.offset.reset", "earliest");

        final CountDownLatch latch = new CountDownLatch(4);
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.execute(() -> {
            KafkaConsumer<Integer, String> kafkaConsumer = new KafkaConsumer<>(consumerProps);
            kafkaConsumer.subscribe(Arrays.asList(inputTopic1, inputTopic2));
            try {
                while (true) {
                    ConsumerRecords<Integer, String> records = kafkaConsumer.poll(100);
                    for (ConsumerRecord<Integer, String> record : records) {
                        logger.info("consuming from topic = {}, partition = {}, offset = {}, key = {}, value = {}",
                                record.topic(), record.partition(), record.offset(), record.key(), record.value());
                        latch.countDown();
                    }
                }
            } finally {
                kafkaConsumer.close();
            }
        });

        assertTrue(latch.await(30, TimeUnit.SECONDS));
    }
}