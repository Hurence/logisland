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

package com.hurence.logisland.connect.source.timed;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * {@link SourceTask} for {@link ClockSourceConnector}
 *
 * @author amarziali
 */
public class ClockSourceTask extends SourceTask {

    private long rate;

    @Override
    public void start(Map<String, String> props) {
        rate = Long.parseLong(props.get(ClockSourceConnector.RATE));

    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        Thread.sleep(rate);
        return Collections.singletonList(new SourceRecord(null, null, "",
                Schema.STRING_SCHEMA, ""));

    }

    @Override
    public void stop() {

    }

    @Override
    public String version() {
        return "1.0";
    }
}
