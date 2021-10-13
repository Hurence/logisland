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
package com.hurence.logisland.engine;

import com.hurence.logisland.component.ComponentFactory;
import com.hurence.logisland.component.ComponentType;
import com.hurence.logisland.config.EngineConfiguration;
import com.hurence.logisland.config.ProcessorConfiguration;
import com.hurence.logisland.config.StreamConfiguration;
import com.hurence.logisland.engine.spark.KafkaStreamProcessingEngine;
import com.hurence.logisland.stream.StreamProperties;
import com.hurence.logisland.stream.spark.structured.provider.KafkaProperties;
import com.hurence.logisland.util.runner.MockProcessor;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.stream.spark.KafkaRecordStreamParallelProcessing;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertTrue;

/**
 * Empty Java class for source jar generation (need to publish on OSS sonatype)
 */
public class ProgrammaticStreamProcessingIntegrationTest extends AbstractStreamProcessingIntegrationTest {


    public static final String MAGIC_STRING = "the world is so big";


    private static Logger logger = LoggerFactory.getLogger(ProgrammaticStreamProcessingIntegrationTest.class);


    Optional<EngineContext> getEngineContext() {
        Map<String, String> properties = new HashMap<>();
        properties.put(KafkaStreamProcessingEngine.SPARK_APP_NAME().getName(), "testApp");
        properties.put(KafkaStreamProcessingEngine.SPARK_STREAMING_BATCH_DURATION().getName(), "500");
        properties.put(KafkaStreamProcessingEngine.SPARK_MASTER().getName(), "local[4]");
        properties.put(KafkaStreamProcessingEngine.SPARK_STREAMING_TIMEOUT().getName(), "12000");


        EngineConfiguration conf = new EngineConfiguration();
        conf.setComponent(KafkaStreamProcessingEngine.class.getName());
        conf.setType(ComponentType.ENGINE.toString());
        conf.setConfiguration(properties);
        conf.addPipelineConfigurations(createStreamConfig());

        return ComponentFactory.buildAndSetUpEngineContext(conf);
    }


    private StreamConfiguration createStreamConfig() {
        Map<String, String> properties = new HashMap<>();
        properties.put(KafkaProperties.KAFKA_METADATA_BROKER_LIST().getName(), BROKERHOST + ":" + BROKERPORT);
        properties.put(KafkaProperties.KAFKA_ZOOKEEPER_QUORUM().getName(), ZKHOST + ":" + zkServer.port());
        properties.put(KafkaProperties.KAFKA_TOPIC_DEFAULT_REPLICATION_FACTOR().getName(), "1");
        properties.put(KafkaProperties.KAFKA_TOPIC_DEFAULT_PARTITIONS().getName(), "1");
        properties.put(KafkaProperties.INPUT_SERIALIZER().getName(), StreamProperties.KRYO_SERIALIZER().getValue());
        properties.put(KafkaProperties.OUTPUT_SERIALIZER().getName(), StreamProperties.KRYO_SERIALIZER().getValue());
        properties.put(KafkaProperties.KAFKA_MANUAL_OFFSET_RESET().getName(), StreamProperties.LATEST_OFFSET().getValue());

        properties.put(KafkaProperties.INPUT_TOPICS().getName(), INPUT_TOPIC);
        properties.put(KafkaProperties.OUTPUT_TOPICS().getName(), OUTPUT_TOPIC);

        StreamConfiguration conf = new StreamConfiguration();
        conf.setComponent(KafkaRecordStreamParallelProcessing.class.getName());
        conf.setType(ComponentType.STREAM.toString());
        conf.setConfiguration(properties);
        conf.setStream("KafkaStream");
        conf.addProcessorConfiguration(createProcessorConfiguration());

        return conf;
    }

    private ProcessorConfiguration createProcessorConfiguration() {
        Map<String, String> properties = new HashMap<>();
        properties.put(MockProcessor.FAKE_MESSAGE.getName(), MAGIC_STRING);

        ProcessorConfiguration conf = new ProcessorConfiguration();
        conf.setComponent(MockProcessor.class.getName());
        conf.setType(ComponentType.PROCESSOR.toString());
        conf.setConfiguration(properties);
        conf.setProcessor("mock");

        return conf;
    }


    @Test
    @Ignore
    public void validateIntegration() throws NoSuchFieldException, IllegalAccessException, InterruptedException, IOException {

        final List<Record> records = new ArrayList<>();

        Runnable testRunnable = () -> {


            // send message
            Record record = new StandardRecord("cisco");
            record.setId("firewall_record1");
            record.setField("method", FieldType.STRING, "GET");
            record.setField("ip_source", FieldType.STRING, "123.34.45.123");
            record.setField("ip_target", FieldType.STRING, "255.255.255.255");
            record.setField("url_scheme", FieldType.STRING, "http");
            record.setField("url_host", FieldType.STRING, "origin-www.20minutes.fr");
            record.setField("url_port", FieldType.STRING, "80");
            record.setField("url_path", FieldType.STRING, "/r15lgc-100KB.js");
            record.setField("request_size", FieldType.INT, 1399);
            record.setField("response_size", FieldType.INT, 452);
            record.setField("is_outside_office_hours", FieldType.BOOLEAN, false);
            record.setField("is_host_blacklisted", FieldType.BOOLEAN, false);
            record.setField("tags", FieldType.ARRAY, new ArrayList<>(Arrays.asList("spam", "filter", "mail")));


            try {
                Thread.sleep(8000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            try {
                sendRecord(INPUT_TOPIC, record);
            } catch (IOException e) {
                e.printStackTrace();
            }


            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            records.addAll(readRecords(OUTPUT_TOPIC));
        };

        Thread t = new Thread(testRunnable);
        logger.info("starting validation thread {}", t.getId());
        t.start();


        try{
            Thread.sleep(15000);
            assertTrue(records.size() == 1);
            assertTrue(records.get(0).size() == 13);
            assertTrue(records.get(0).getField("message").asString().equals(MAGIC_STRING));
        }catch (Exception e){
            logger.error("issue durring validation {}", e.getMessage());
        }


    }
}
