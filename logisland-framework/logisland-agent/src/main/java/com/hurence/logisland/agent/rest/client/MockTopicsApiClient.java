/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.agent.rest.client;

import com.hurence.logisland.agent.rest.model.Topic;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;


public class MockTopicsApiClient implements TopicsApiClient {

    public static final String APACHE_RAW = "apache_raw";
    public static final String APACHE_RECORDS = "apache_records";
    public static final String ERRORS = "_errors";
    public static final String METRICS = "_metrics";
    public static final String MOCK_IN = "mock_in";
    public static final String MOCK_OUT = "mock_out";

    public MockTopicsApiClient() {

        addTopic(new Topic().id(1234L)
                .partitions(2)
                .replicationFactor(0)
                .documentation("apache logs")
                .dateModified(new Date())
                .name(APACHE_RAW)
                .serializer("none"));

        addTopic(new Topic().id(1235L)
                .partitions(2)
                .replicationFactor(0)
                .documentation("apache records")
                .dateModified(new Date())
                .name(APACHE_RECORDS)
                .serializer("com.hurence.logisland.serializer.KryoSerializer"));

        addTopic(new Topic().id(1236L)
                .partitions(1)
                .replicationFactor(0)
                .documentation("errors")
                .dateModified(new Date())
                .name(ERRORS)
                .serializer("com.hurence.logisland.serializer.KryoSerializer"));

        addTopic(new Topic().id(1236L)
                .partitions(1)
                .replicationFactor(0)
                .documentation("metrics")
                .dateModified(new Date())
                .name(METRICS)
                .serializer("com.hurence.logisland.serializer.KryoSerializer"));


        addTopic(new Topic().id(1237L)
                .partitions(1)
                .replicationFactor(0)
                .documentation(MOCK_IN)
                .dateModified(new Date())
                .name(MOCK_IN)
                .serializer("com.hurence.logisland.serializer.KryoSerializer"));

        addTopic(new Topic().id(1237L)
                .partitions(1)
                .replicationFactor(0)
                .documentation(MOCK_OUT)
                .dateModified(new Date())
                .name(MOCK_OUT)
                .serializer("com.hurence.logisland.serializer.KryoSerializer"));
    }

    Map<String, Topic> topics = new HashMap<>();

    @Override
    public Topic addTopic(Topic topic) {
        topics.put(topic.getName(), topic);

        return topic;
    }

    @Override
    public Topic getTopic(String name) {
        return topics.get(name);
    }
}
