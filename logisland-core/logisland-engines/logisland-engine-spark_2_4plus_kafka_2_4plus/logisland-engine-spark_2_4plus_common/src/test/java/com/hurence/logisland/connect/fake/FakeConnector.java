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
package com.hurence.logisland.connect.fake;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.util.*;
import java.util.concurrent.SynchronousQueue;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class FakeConnector extends SourceConnector {


    public static class FakeTask extends SourceTask {

        private SynchronousQueue<String> queue = new SynchronousQueue<>();
        private final Timer timer = new Timer();


        @Override
        public void start(Map<String, String> props) {


        }

        @Override
        public List<SourceRecord> poll() throws InterruptedException {
            Random random = new Random();

            return IntStream.range(0, 1000).mapToObj(i -> {
                        int p = random.nextInt(10);
                        Schema schema = SchemaBuilder.struct()
                                .field("partition", SchemaBuilder.int32())
                                .field("val", SchemaBuilder.string())
                                .build();
                        return new SourceRecord(
                                Collections.singletonMap("partition", p),
                                Collections.singletonMap("offset", System.currentTimeMillis()),
                                "",
                                null,
                                schema,
                                new Struct(schema)
                                        .put("partition", p)
                                        .put("val", RandomStringUtils.randomAscii(30)));
                    }
            ).collect(Collectors.toList());
        }


        @Override
        public void stop() {
            timer.cancel();
        }

        @Override
        public String version() {
            return "1.0";
        }

    }

    @Override
    public String version() {
        return "1.0";
    }

    @Override
    public void start(Map<String, String> props) {
    }

    @Override
    public Class<? extends Task> taskClass() {
        return FakeTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> ret = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            ret.add(Collections.emptyMap());
        }
        return ret;
    }

    @Override
    public void stop() {

    }

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }
}
