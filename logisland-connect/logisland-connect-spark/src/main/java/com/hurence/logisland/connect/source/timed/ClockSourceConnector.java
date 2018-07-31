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
package com.hurence.logisland.connect.source.timed;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A connector that emits an empty record at fixed rate waking up the processing pipeline.
 *
 * @author amarziali
 */
public class ClockSourceConnector extends SourceConnector {

    public static final String RATE = "rate";

    private static final ConfigDef CONFIG = new ConfigDef()
            .define(RATE, ConfigDef.Type.LONG, null, ConfigDef.Importance.HIGH, "The clock rate in milliseconds");

    private long rate;


    @Override
    public String version() {
        return "1.0";
    }

    @Override
    public void start(Map<String, String> props) {
        rate = (Long) CONFIG.parse(props).get(RATE);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return ClockSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        return Collections.singletonList(Collections.singletonMap(RATE, Long.toString(rate)));
    }

    @Override
    public void stop() {
    }

    @Override
    public ConfigDef config() {
        return CONFIG;
    }
}
