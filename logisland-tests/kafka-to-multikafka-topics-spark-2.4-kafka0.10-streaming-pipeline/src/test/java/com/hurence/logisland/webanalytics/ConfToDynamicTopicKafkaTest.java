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

import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.stream.spark.structured.provider.KafkaProperties;
import com.hurence.logisland.webanalytics.test.util.ConfJobHelper;
import com.hurence.logisland.webanalytics.test.util.EventsGenerator;
import com.hurence.logisland.webanalytics.util.KafkaUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

@Ignore
public class ConfToDynamicTopicKafkaTest {

    private static Logger logger = LoggerFactory.getLogger(ConfToDynamicTopicKafkaTest.class);

    final static String inputTopic1 = "my_topics_1";
    final static String inputTopic2 = "my_topics_2";
    final static String outputTopic1 = "output_topic1";
    final static String outputTopic2 = "output_topic2";

    @ClassRule
    public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(
            1, true, 2,
            inputTopic1, inputTopic2, outputTopic1);

    private static KafkaUtils kafkaUtils = new KafkaUtils(embeddedKafka);

    EventsGenerator eventGen = new EventsGenerator("divolte_1");

    @Test
    public void myWebAnalDebugTest() throws IOException, InterruptedException, InitializationException {
        String confFilePath = getClass().getClassLoader().getResource("conf/conf-to-multisink.yaml").getFile();
        ConfJobHelper confJob = new ConfJobHelper(confFilePath);
        Map<String, String> confKafka = new HashMap<>();
        confKafka.put(KafkaProperties.KAFKA_ZOOKEEPER_QUORUM().getName(), embeddedKafka.getZookeeperConnectionString());
        confKafka.put(KafkaProperties.KAFKA_METADATA_BROKER_LIST().getName(), embeddedKafka.getBrokersAsString());
        confJob.modifyControllerServiceConf("all_topics", confKafka);
        confJob.initJob();
        confJob.startJob();
        Thread.sleep(5000L);//wait logisland to be ready
        final long padding = 500L;
        long ts = 10000L;
        int counter = 0;
        while (counter < 10) {
            counter++;
            logger.info("Adding an event in topic");
            Record event = eventGen.generateEvent(ts, outputTopic1);
            kafkaUtils.addingEventsToTopicPartition(inputTopic1, 0, event);
            event = eventGen.generateEvent(ts, outputTopic2);
            kafkaUtils.addingEventsToTopicPartition(inputTopic2, 0, event);
            ts += padding;
        }

        //Verify data injected in dynamic topic
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("sampleRawConsumer", "false", embeddedKafka);
        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("auto.offset.reset", "earliest");
        final CountDownLatch latch = new CountDownLatch(20);
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.execute(() -> {
            KafkaConsumer<String, Record> kafkaConsumer = new KafkaConsumer<>(consumerProps);
            kafkaConsumer.subscribe(Arrays.asList(outputTopic1, outputTopic2));
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

        confJob.stopJob();
    }

}