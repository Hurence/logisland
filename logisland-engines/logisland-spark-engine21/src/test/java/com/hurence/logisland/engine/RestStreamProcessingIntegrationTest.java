/**
 * Copyright (C) 2016 Hurence (bailet.thomas@gmail.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.engine;

import com.hurence.logisland.agent.rest.client.MockConfigsApiClient;
import com.hurence.logisland.agent.rest.client.MockJobsApiClient;
import com.hurence.logisland.agent.rest.client.MockTopicsApiClient;
import com.hurence.logisland.component.RestComponentFactory;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertTrue;

/**
 * Empty Java class for source jar generation (need to publish on OSS sonatype)
 */
public class RestStreamProcessingIntegrationTest extends AbstractStreamProcessingIntegrationTest {


    public static final String MAGIC_STRING = "the world is so big";


    private static Logger logger = LoggerFactory.getLogger(RestStreamProcessingIntegrationTest.class);


    Optional<EngineContext> getEngineContext() {

        String zkPort = String.valueOf(zkServer.port());
        String kafkaPort = String.valueOf(BROKERPORT);
        RestComponentFactory componentFactory =
                new RestComponentFactory(
                        new MockJobsApiClient(),
                        new MockTopicsApiClient(),
                        new MockConfigsApiClient(zkPort, kafkaPort));

        return componentFactory.getEngineContext(MockJobsApiClient.MOCK_PROCESSING_JOB);
    }


    @Test
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


        Thread.sleep(15000);
        assertTrue(records.size() == 1);
        assertTrue(records.get(0).size() == 13);
        assertTrue(records.get(0).getField("message").asString().equals(MAGIC_STRING));

    }
}
