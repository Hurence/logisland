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

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.util.Collection;
import java.util.Map;

public class TestSinkTask extends SinkTask {

    @Override
    public void start(Map<String, String> props) {
        System.out.println("Task started");
    }

    @Override
    public void put(Collection<SinkRecord> records) {

        System.out.println("Adding " + records.size() + " records");
        records.stream().findFirst().ifPresent(System.out::println);
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
        System.out.println("Flushed offset: " +offsets);
    }

    @Override
    public void stop() {
        System.out.println("Task stopped");

    }

    @Override
    public String version() {
        return "";
    }
}
