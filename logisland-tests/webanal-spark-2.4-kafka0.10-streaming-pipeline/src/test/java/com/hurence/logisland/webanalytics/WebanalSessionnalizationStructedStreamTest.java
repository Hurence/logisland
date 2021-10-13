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
import com.hurence.logisland.service.elasticsearch.ElasticsearchClientService;
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
import java.util.HashMap;
import java.util.Map;

public class WebanalSessionnalizationStructedStreamTest {

    private static Logger logger = LoggerFactory.getLogger(WebanalSessionnalizationStructedStreamTest.class);

    final static String logisland_raw = "logisland_raw";

    @ClassRule
    public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(
            1, true, 2,
            logisland_raw);

    private static KafkaUtils kafkaUtils = new KafkaUtils(embeddedKafka);

    EventsGenerator eventGen = new EventsGenerator("divolte_1");

    /**
     */
    @Test
    @Ignore
    public void mySimpleDebugTest() throws IOException, InterruptedException, InitializationException {
        String confFilePath = getClass().getClassLoader().getResource("conf/my-simple-conf-test").getFile();
        ConfJobHelper confJob = new ConfJobHelper(confFilePath);
        Map<String, String> confKafka = new HashMap<>();
        confKafka.put(KafkaProperties.KAFKA_ZOOKEEPER_QUORUM().getName(), embeddedKafka.getZookeeperConnectionString());
        confKafka.put(KafkaProperties.KAFKA_METADATA_BROKER_LIST().getName(), embeddedKafka.getBrokersAsString());
        confJob.modifyControllerServiceConf("kafka_service", confKafka);
        Map<String, String> confOpenDistro = new HashMap<>();
        confOpenDistro.put(ElasticsearchClientService.ENABLE_SSL.getName(), "true");
        confJob.modifyControllerServiceConf("opendistro_service", confOpenDistro);
        confJob.initJob();
        confJob.startJob();
        boolean running = true;
        long ts = 0L;
        while (running) {
            logger.info("Adding an event in topic");
            Record event = eventGen.generateEvent(ts, "url");
            kafkaUtils.addingEventsToTopicPartition(logisland_raw, 0, event);
            long increment = 5000L;
            ts += increment;
            running = ts != increment * 10;
        }
        Thread.sleep(10000L);
        confJob.stopJob();
    }

    @Test
    @Ignore
    public void mySimpleDebugTestWithServices() throws IOException, InterruptedException, InitializationException {
        final long padding = 30000L;
        String confFilePath = getClass().getClassLoader().getResource("conf/my-conf-test-with-services.yaml").getFile();
        ConfJobHelper confJob = new ConfJobHelper(confFilePath);
        Map<String, String> confKafka = new HashMap<>();
        confKafka.put(KafkaProperties.KAFKA_ZOOKEEPER_QUORUM().getName(), embeddedKafka.getZookeeperConnectionString());
        confKafka.put(KafkaProperties.KAFKA_METADATA_BROKER_LIST().getName(), embeddedKafka.getBrokersAsString());
        confJob.modifyControllerServiceConf("kafka_service", confKafka);
        Map<String, String> confOpenDistro = new HashMap<>();
        confOpenDistro.put(ElasticsearchClientService.ENABLE_SSL.getName(), "true");
        confJob.modifyControllerServiceConf("opendistro_service", confOpenDistro);
        confJob.initJob();
        confJob.startJob();
        boolean running = true;
        long ts = 0L;
        while (running) {
            logger.info("Adding an event in topic");
            Record event = eventGen.generateEvent(ts, "url");
            kafkaUtils.addingEventsToTopicPartition(logisland_raw, 0, event);
            ts += padding;
            running = ts != padding * 10;
            Thread.sleep(padding);
        }
        Thread.sleep(10000L);
        confJob.stopJob();
    }

    @Test
    @Ignore
    public void myWebAnalDebugTest() throws IOException, InterruptedException, InitializationException {
        String confFilePath = getClass().getClassLoader().getResource("conf/my-webanal-conf-test.yaml").getFile();
        ConfJobHelper confJob = new ConfJobHelper(confFilePath);
        Map<String, String> confKafka = new HashMap<>();
        confKafka.put(KafkaProperties.KAFKA_ZOOKEEPER_QUORUM().getName(), embeddedKafka.getZookeeperConnectionString());
        confKafka.put(KafkaProperties.KAFKA_METADATA_BROKER_LIST().getName(), embeddedKafka.getBrokersAsString());
        confJob.modifyControllerServiceConf("kafka_service", confKafka);
        Map<String, String> confOpenDistro = new HashMap<>();
        confOpenDistro.put(ElasticsearchClientService.ENABLE_SSL.getName(), "true");
        confJob.modifyControllerServiceConf("opendistro_service", confOpenDistro);
        confJob.initJob();
        confJob.startJob();
        final long padding = 30000L;
        boolean running = true;
        long ts = 0L;
        while (running) {
            logger.info("Adding an event in topic");
            Record event = eventGen.generateEvent(ts, "url");
            kafkaUtils.addingEventsToTopicPartition(logisland_raw, 0, event);
            ts += padding;
            running = ts != padding * 10;
            Thread.sleep(padding);
        }
        Thread.sleep(10000L);
        confJob.stopJob();
    }
}