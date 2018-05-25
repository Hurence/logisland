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

package com.hurence.logisland.connect.fake;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.util.*;
import java.util.concurrent.SynchronousQueue;

public class FakeConnector extends SourceConnector {


    public static class FakeTask extends SourceTask {

        private SynchronousQueue<String> queue = new SynchronousQueue<>();
        private final Timer timer = new Timer();


        @Override
        public void start(Map<String, String> props) {
            timer.scheduleAtFixedRate(new TimerTask() {
                @Override
                public void run() {
                    try {
                        queue.put(RandomStringUtils.randomAscii(30));
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }, 0, 500);

        }

        @Override
        public List<SourceRecord> poll() throws InterruptedException {

            return Collections.singletonList(new SourceRecord(null ,
                    Collections.singletonMap("offset", System.currentTimeMillis()),
                    "",
                    null,
                    Schema.STRING_SCHEMA,
                    queue.take()));
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
        return Collections.singletonList(Collections.emptyMap());
    }

    @Override
    public void stop() {

    }

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }
}
