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
package com.hurence.logisland.connect.sink;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkTaskContext;

import java.util.*;
import java.util.stream.Collectors;

/**
 * A simple version of {@link SinkTaskContext}
 *
 * @author amarziali
 */
public class SimpleSinkTaskContext implements SinkTaskContext {

    private final Map<Integer, Boolean> state = Collections.synchronizedMap(new HashMap<>());
    private final String topic;

    public SimpleSinkTaskContext(String topic) {
        this.topic = topic;
    }

    @Override
    public void offset(Map<TopicPartition, Long> offsets) {
        //not implemented
    }

    @Override
    public void offset(TopicPartition tp, long offset) {
        //not implemented
    }

    @Override
    public void timeout(long timeoutMs) {
        //not implemented
    }

    @Override
    public Set<TopicPartition> assignment() {
        return state.entrySet().stream().filter(Map.Entry::getValue)
                .map(entry -> new TopicPartition(topic, entry.getKey()))
                .collect(Collectors.toSet());
    }

    @Override
    public void pause(TopicPartition... partitions) {
        Arrays.stream(partitions).map(TopicPartition::partition).forEach(p -> state.put(p, false));

    }

    @Override
    public void resume(TopicPartition... partitions) {
        Arrays.stream(partitions).map(TopicPartition::partition).forEach(p -> state.put(p, true));

    }

    @Override
    public void requestCommit() {

    }

    public boolean assignThenState(int partition) {
        return state.computeIfAbsent(partition, p -> true);
    }

}
