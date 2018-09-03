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
package com.hurence.logisland.connect.opc.da;

import com.google.gson.Gson;
import com.hurence.logisland.connect.opc.CommonDefinitions;
import com.hurence.logisland.connect.opc.OpcRecordFields;
import com.hurence.logisland.util.Tuple;
import javafx.util.Pair;
import org.junit.Ignore;
import org.junit.Test;

import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class OpcDaSourceConnectorTest {


    @Test
    @Ignore
    public void e2eTest() throws Exception {
        OpcDaSourceConnector connector = new OpcDaSourceConnector();
        Map<String, String> properties = new HashMap<>();
        properties.put(OpcDaSourceConnector.PROPERTY_AUTH_NTLM_DOMAIN, "OPC-9167C0D9342");
        properties.put(CommonDefinitions.PROPERTY_CONNECTION_SOCKET_TIMEOUT, "2000");
        properties.put(OpcDaSourceConnector.PROPERTY_AUTH_NTLM_PASSWORD, "opc");
        properties.put(OpcDaSourceConnector.PROPERTY_AUTH_NTLM_USER, "OPC");
        properties.put(CommonDefinitions.PROPERTY_SERVER_URI, "opc.da://192.168.99.100");
        properties.put(OpcDaSourceConnector.PROPERTY_SERVER_CLSID, "F8582CF2-88FB-11D0-B850-00C0F0104305");
        properties.put(CommonDefinitions.PROPERTY_TAGS_ID, "Random.Real8,Triangle Waves.Int4");
        properties.put(CommonDefinitions.PROPERTY_TAGS_STREAM_MODE, "SUBSCRIBE,POLL");
        properties.put(CommonDefinitions.PROPERTY_TAGS_SAMPLING_RATE, "PT3S,PT1S");
        properties.put(OpcDaSourceConnector.PROPERTY_SESSION_REFRESH_PERIOD, "1000");
        properties.put(OpcDaSourceConnector.PROPERTY_TAGS_DATA_TYPE_OVERRIDE, "0,8");



        connector.start(properties);
        OpcDaSourceTask task = new OpcDaSourceTask();
        task.start(connector.taskConfigs(1).get(0));
        ScheduledExecutorService es = Executors.newSingleThreadScheduledExecutor();
        Gson json = new Gson();
        es.scheduleAtFixedRate(() -> {
            try {
                task.poll().stream().map(a->new Tuple<>(new Date((Long)a.sourceOffset().get(OpcRecordFields.SAMPLED_TIMESTAMP)), json.toJson(a))).forEach(System.out::println);
            } catch (InterruptedException e) {
                //do nothing
            }
        }, 0, 10, TimeUnit.MILLISECONDS);

        Thread.sleep(600000);
        task.stop();
        es.shutdown();
        connector.stop();
    }


}
