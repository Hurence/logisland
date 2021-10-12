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
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.regex.Pattern;

import static org.junit.Assert.assertTrue;

@Ignore
public class KafkaRegexInputIgnoreingSomeTopicsTest {

    private static Logger logger = LoggerFactory.getLogger(KafkaRegexInputIgnoreingSomeTopicsTest.class);

    final static String PREFIX = "AAAA";
    final static String SUFFIX = PREFIX;
    final static String inputTopic1 = "my_topics_1";
    final static String inputTopic1WithPrefix = PREFIX + inputTopic1;
    final static String inputTopic1WithSuffix = inputTopic1 + SUFFIX;
    final static String inputTopic1withPrefixAndSuffix = PREFIX + inputTopic1 + SUFFIX;
    final static String inputTopic2 = "my_topics_2";
    final static String inputTopic2WithPrefix = PREFIX + inputTopic2;
    final static String inputTopic2WithSuffix = inputTopic2 + SUFFIX;
    final static String inputTopic2withPrefixAndSuffix = PREFIX + inputTopic2 + SUFFIX;
    final static String inputTopic3 = "my_topics_3";
    final static String inputTopic3WithPrefix = PREFIX + inputTopic3;
    final static String inputTopic3WithSuffix = inputTopic3 + SUFFIX;
    final static String inputTopic3withPrefixAndSuffix = PREFIX + inputTopic3 + SUFFIX;
    final static String inputTopic4 = "my_topics_4";
    final static String inputTopic4WithPrefix = PREFIX + inputTopic4;
    final static String inputTopic4WithSuffix = inputTopic4 + SUFFIX;
    final static String inputTopic4withPrefixAndSuffix = PREFIX + inputTopic4 + SUFFIX;
    final static String inputTopicAnyThingElse = "anythingelse";

    List<String> topicList = Arrays.asList(inputTopic1, inputTopic1WithPrefix, inputTopic1WithSuffix, inputTopic1withPrefixAndSuffix,
            inputTopic2, inputTopic2WithPrefix, inputTopic2WithSuffix, inputTopic2withPrefixAndSuffix,
            inputTopic3, inputTopic3WithPrefix, inputTopic3WithSuffix, inputTopic3withPrefixAndSuffix,
            inputTopic4, inputTopic4WithPrefix, inputTopic4WithSuffix, inputTopic4withPrefixAndSuffix,
            inputTopicAnyThingElse);

    @ClassRule
    public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(
            1, true, 2,
            inputTopic1, inputTopic1WithPrefix, inputTopic1WithSuffix, inputTopic1withPrefixAndSuffix,
            inputTopic2, inputTopic2WithPrefix, inputTopic2WithSuffix, inputTopic2withPrefixAndSuffix,
            inputTopic3, inputTopic3WithPrefix, inputTopic3WithSuffix, inputTopic3withPrefixAndSuffix,
            inputTopic4, inputTopic4WithPrefix, inputTopic4WithSuffix, inputTopic4withPrefixAndSuffix,
            inputTopicAnyThingElse);

    private static KafkaUtils kafkaUtils = new KafkaUtils(embeddedKafka);

    @Test
    public void javaPatternTests() throws Exception {
        Pattern pattern = Pattern.compile("my_topics.*");
        Assert.assertTrue(pattern.matcher("my_topics_2").matches());

        pattern = Pattern.compile("^((?!my_topics_2$).*my_topics.*)*$");
        Assert.assertFalse(pattern.matcher("my_topics_2").matches());
        Assert.assertTrue(pattern.matcher("my_topics_1").matches());
        Assert.assertTrue(pattern.matcher("my_topics_3").matches());
        Assert.assertTrue(pattern.matcher("my_topics_4").matches());
        Assert.assertTrue(pattern.matcher("AAAAmy_topics_1AAAA").matches());
        Assert.assertTrue(pattern.matcher("AAAAmy_topics_2").matches());
        Assert.assertTrue(pattern.matcher("my_topics_2AAAA").matches());
        Assert.assertTrue(pattern.matcher("AAAAmy_topics_2AAAA").matches());
        Assert.assertTrue(pattern.matcher("AAAAmy_topics_3AAAA").matches());
        Assert.assertTrue(pattern.matcher("AAAAmy_topics_4AAAA").matches());
        Assert.assertFalse(pattern.matcher("anythingelse").matches());

        pattern = Pattern.compile("^((?!(my_topics_2|my_topics_3)$).*my_topics.*)*$");
        Assert.assertFalse(pattern.matcher("my_topics_2").matches());
        Assert.assertTrue(pattern.matcher("my_topics_1").matches());
        Assert.assertFalse(pattern.matcher("my_topics_3").matches());
        Assert.assertTrue(pattern.matcher("my_topics_4").matches());
        Assert.assertTrue(pattern.matcher("AAAAmy_topics_1AAAA").matches());
        Assert.assertTrue(pattern.matcher("AAAAmy_topics_2").matches());
        Assert.assertTrue(pattern.matcher("my_topics_2AAAA").matches());
        Assert.assertTrue(pattern.matcher("AAAAmy_topics_2AAAA").matches());
        Assert.assertTrue(pattern.matcher("AAAAmy_topics_3").matches());
        Assert.assertTrue(pattern.matcher("my_topics_3AAAA").matches());
        Assert.assertTrue(pattern.matcher("AAAAmy_topics_3AAAA").matches());
        Assert.assertTrue(pattern.matcher("AAAAmy_topics_4AAAA").matches());
        Assert.assertFalse(pattern.matcher("anythingelse").matches());

        pattern = Pattern.compile("^((?!(my_topics_2|my_topics_3)$)my_topics.*)*$");
        Assert.assertFalse(pattern.matcher("my_topics_2").matches());
        Assert.assertTrue(pattern.matcher("my_topics_1").matches());
        Assert.assertFalse(pattern.matcher("my_topics_3").matches());
        Assert.assertTrue(pattern.matcher("my_topics_4").matches());
        Assert.assertFalse(pattern.matcher("AAAAmy_topics_1AAAA").matches());
        Assert.assertFalse(pattern.matcher("AAAAmy_topics_2").matches());
        Assert.assertTrue(pattern.matcher("my_topics_2AAAA").matches());
        Assert.assertFalse(pattern.matcher("AAAAmy_topics_2AAAA").matches());
        Assert.assertFalse(pattern.matcher("AAAAmy_topics_3").matches());
        Assert.assertTrue(pattern.matcher("my_topics_3AAAA").matches());
        Assert.assertFalse(pattern.matcher("AAAAmy_topics_3AAAA").matches());
        Assert.assertFalse(pattern.matcher("AAAAmy_topics_4AAAA").matches());
        Assert.assertFalse(pattern.matcher("anythingelse").matches());
    }

    @Test
    public void matchAllPattern() throws Exception {
        runTestWithPatternAndExpectMatch("my_topics.*", 8 * 2);
    }


    @Test
    public void matchAllPatternExcept2() throws Exception {
        runTestWithPatternAndExpectMatch("^((?!my_topics_2$).*my_topics.*)*$", 3*4+3);
    }


    @Test
    public void matchAllPatternExcept2And3() throws Exception {
        runTestWithPatternAndExpectMatch("^((?!(my_topics_2|my_topics_3)$).*my_topics.*)*$", 2*4+2*3);
    }

    private void runTestWithPatternAndExpectMatch(String pattern,int eventmatches) throws InterruptedException, java.util.concurrent.ExecutionException {
        Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
        final KafkaProducer<Integer, String> producer = new KafkaProducer<>(senderProps);
        logger.info("will produce events");
        topicList.forEach(topic -> {
            try {
                producer.send(new ProducerRecord<>(topic, 0, 0, "message0")).get();
                producer.send(new ProducerRecord<>(topic, 1, 1, "message1")).get();
            } catch (InterruptedException | ExecutionException e) {
                logger.error("error", e);
            }
        });
        logger.info("produced 4 events");
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("sampleRawConsumer", "false", embeddedKafka);
        consumerProps.put("auto.offset.reset", "earliest");

        final CountDownLatch latch = new CountDownLatch(eventmatches);
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.execute(() -> {
            KafkaConsumer<Integer, String> kafkaConsumer = new KafkaConsumer<>(consumerProps);

            Pattern rgexp = Pattern.compile(pattern);
            kafkaConsumer.subscribe(rgexp, new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                    collection.forEach(p -> {
                        logger.info("revoke partition {} of topic {}", p.partition(), p.topic());
                    });
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                    collection.forEach(p -> {
                        logger.info("assigned partition {} of topic {}", p.partition(), p.topic());
                    });
                }
            });
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