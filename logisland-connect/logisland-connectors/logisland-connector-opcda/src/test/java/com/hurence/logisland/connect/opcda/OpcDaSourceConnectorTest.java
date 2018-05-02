/*
 *  * Copyright (C) 2018 Hurence (support@hurence.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.hurence.logisland.connect.opcda;

import com.hurence.opc.OpcTagInfo;
import com.hurence.opc.da.OpcDaConnectionProfile;
import com.hurence.opc.da.OpcDaOperations;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class OpcDaSourceConnectorTest {

    @Test(expected = IllegalArgumentException.class)
    public void parseFailureTest() {
        OpcDaSourceConnector.parseTag("test1:2aj", 500L);
    }

    @Test
    public void tagParseTest() {
        Map.Entry<String, Long> toTest = OpcDaSourceConnector.parseTag("test1:1000", 500L);
        Assert.assertEquals("test1", toTest.getKey());
        Assert.assertEquals(new Long(1000), toTest.getValue());

        toTest = OpcDaSourceConnector.parseTag("test2", 500L);
        Assert.assertEquals("test2", toTest.getKey());
        Assert.assertEquals(new Long(500), toTest.getValue());
    }

    @Test
    public void configParseAndPartitionTest() {
        OpcDaSourceConnector connector = new OpcDaSourceConnector();
        Map<String, String> properties = new HashMap<>();
        properties.put(OpcDaSourceConnector.PROPERTY_DOMAIN, "domain");
        properties.put(OpcDaSourceConnector.PROPERTY_SOCKET_TIMEOUT, "2000");
        properties.put(OpcDaSourceConnector.PROPERTY_PASSWORD, "password");
        properties.put(OpcDaSourceConnector.PROPERTY_USER, "user");
        properties.put(OpcDaSourceConnector.PROPERTY_HOST, "host");
        properties.put(OpcDaSourceConnector.PROPERTY_CLSID, "clsId");
        properties.put(OpcDaSourceConnector.PROPERTY_TAGS, "tag1:1000,tag2,tag3:3000,tag4:3000");
        connector.start(properties);
        List<Map<String, String>> configs = connector.taskConfigs(2);
        Assert.assertEquals(2, configs.size());
        System.out.println(configs);
        configs.stream().map(m -> m.get(OpcDaSourceConnector.PROPERTY_TAGS))
                .map(s -> s.split(",")).forEach(a -> Assert.assertEquals(2, a.length));
    }

    @Test
    @Ignore
    public void e2eTest() throws Exception {
        AtomicInteger atomicInteger = new AtomicInteger(5);
        Random r = new Random();
        OpcDaSourceConnector connector = new OpcDaSourceConnector();
        Map<String, String> properties = new HashMap<>();
        properties.put(OpcDaSourceConnector.PROPERTY_DOMAIN, "OPC-9167C0D9342");
        properties.put(OpcDaSourceConnector.PROPERTY_SOCKET_TIMEOUT, "2000");
        properties.put(OpcDaSourceConnector.PROPERTY_PASSWORD, "opc");
        properties.put(OpcDaSourceConnector.PROPERTY_USER, "OPC");
        properties.put(OpcDaSourceConnector.PROPERTY_HOST, "192.168.56.101");
        properties.put(OpcDaSourceConnector.PROPERTY_CLSID, "F8582CF2-88FB-11D0-B850-00C0F0104305");
        properties.put(OpcDaSourceConnector.PROPERTY_TAGS, listAllTags().stream()
                .map(s -> s + ":" + atomicInteger.getAndAdd(r.nextInt(130)))
                .collect(Collectors.joining(","))
        );
        //"Read Error.Int4:1000,Square Waves.Real8,Random.ArrayOfString:100"
        connector.start(properties);
        OpcDaSourceTask task = new OpcDaSourceTask();
        task.start(connector.taskConfigs(1).get(0));
        ScheduledExecutorService es = Executors.newSingleThreadScheduledExecutor();
        es.scheduleAtFixedRate(() -> {
            try {
                task.poll().forEach(System.out::println);
            } catch (InterruptedException e) {
                //do nothing
            }
        }, 0, 10, TimeUnit.MILLISECONDS);

        Thread.sleep(10000);
        task.stop();
        es.shutdown();
        connector.stop();
    }

    private Collection<String> listAllTags() throws Exception {
        //create a connection profile
        OpcDaConnectionProfile connectionProfile = new OpcDaConnectionProfile()
                .withComClsId("F8582CF2-88FB-11D0-B850-00C0F0104305")
                .withDomain("OPC-9167C0D9342")
                .withUser("OPC")
                .withPassword("opc")
                .withHost("192.168.56.101")
                .withSocketTimeout(Duration.of(1, ChronoUnit.SECONDS));

        //Create an instance of a da operations
        try (OpcDaOperations opcDaOperations = new OpcDaOperations()) {
            //connect using our profile
            opcDaOperations.connect(connectionProfile);
            if (!opcDaOperations.awaitConnected()) {
                throw new IllegalStateException("Unable to connect");
            }
            return opcDaOperations.browseTags().stream().map(OpcTagInfo::getName).collect(Collectors.toList());
        }
    }

}
