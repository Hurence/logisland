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
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.test.rule.KafkaEmbedded;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Ignore
public class ConfToDynamicOutputTopicKafkaAndRewindForOnlyOneTopicTest {

    private static Logger logger = LoggerFactory.getLogger(ConfToDynamicOutputTopicKafkaAndRewindForOnlyOneTopicTest.class);

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
    final static String outputTopic1 = "output_topic1";
    final static String outputTopic2 = "output_topic2";

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
            inputTopicAnyThingElse, outputTopic1, outputTopic2);

    private static KafkaUtils kafkaUtils = new KafkaUtils(embeddedKafka);
    EventsGenerator eventGen = new EventsGenerator("divolte_1");

    @Test
    public void myTestMultiSourceAllTopics() throws IOException, InterruptedException, InitializationException {
        injectDataIntoTopics();
        String confFilePath = getClass().getClassLoader().getResource("conf/conf-to-multisink-rewind-only-one-topic.yaml").getFile();
        ConfJobHelper confJob = new ConfJobHelper(confFilePath);
        Map<String, String> confKafka = new HashMap<>();
        confKafka.put(KafkaProperties.KAFKA_ZOOKEEPER_QUORUM().getName(), embeddedKafka.getZookeeperConnectionString());
        confKafka.put(KafkaProperties.KAFKA_METADATA_BROKER_LIST().getName(), embeddedKafka.getBrokersAsString());
        confJob.modifyControllerServiceConf("all_topics", confKafka);
        confJob.modifyControllerServiceConf("rewind_topics", confKafka);
        confJob.initJob();
        confJob.startJob();
        Thread.sleep(5000L);//wait logisland to be ready
        final long padding = 500L;
        long ts = 10000L;
        int counter = 0;
        while (counter < 10) {
            counter++;
            logger.info("Adding an event in topic");
            for (String inputTopic : topicList) {
                Record event = eventGen.generateEvent(ts, outputTopic2, inputTopic);
                kafkaUtils.addingEventsToTopicPartition(inputTopic, 0, event);
            }
            ts += padding;
        }

        Thread.sleep(10000L);
        confJob.stopJob();
    }

    @Test
    public void myTestMultiSourceWithOneTopicInRewind() throws IOException, InterruptedException, InitializationException {
        injectDataIntoTopics();
        String confFilePath = getClass().getClassLoader().getResource("conf/conf-to-multisink-rewind-only-one-topic-version-rewind.yaml").getFile();
        ConfJobHelper confJob = new ConfJobHelper(confFilePath);
        Map<String, String> confKafka = new HashMap<>();
        confKafka.put(KafkaProperties.KAFKA_ZOOKEEPER_QUORUM().getName(), embeddedKafka.getZookeeperConnectionString());
        confKafka.put(KafkaProperties.KAFKA_METADATA_BROKER_LIST().getName(), embeddedKafka.getBrokersAsString());
        confJob.modifyControllerServiceConf("all_topics", confKafka);
        confJob.modifyControllerServiceConf("rewind_topics", confKafka);
        confJob.initJob();
        confJob.startJob();
        Thread.sleep(5000L);//wait logisland to be ready
        final long padding = 500L;
        long ts = 10000L;
        int counter = 0;
        while (counter < 10) {
            counter++;
            logger.info("Adding an event in topic");
            for (String inputTopic : topicList) {
                Record event = eventGen.generateEvent(ts, outputTopic2, inputTopic);
                kafkaUtils.addingEventsToTopicPartition(inputTopic, 0, event);
            }
            ts += padding;
        }

        Thread.sleep(30000L);
        confJob.stopJob();
    }

    private void injectDataIntoTopics() throws InterruptedException {
        EventsGenerator eventGen = new EventsGenerator("events_before_running_job");
        long ts = 0L;
        while (ts < 1000) {
            logger.info("Adding an event in topics");
            for (String inputTopic : topicList) {
                Record event = eventGen.generateEvent(ts, outputTopic1, inputTopic);
                kafkaUtils.addingEventsToTopicPartition(inputTopic, 0, event);
            }
            ts += 100;
        }
    }
}