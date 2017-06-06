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
package com.hurence.logisland.config;

import java.util.ArrayList;
import java.util.List;


/**
 * A stream is a component + a set of  processorConfigurations
 */
public class StreamConfiguration extends AbstractComponentConfiguration {

    private String stream = "";

    private List<ProcessorConfiguration> processorConfigurations = new ArrayList<>();

    public String getStream() {
        return stream;
    }

    public void setStream(String stream) {
        this.stream = stream;
    }

    public List<ProcessorConfiguration> getProcessorConfigurations() {
        return processorConfigurations;
    }

    public void addProcessorConfiguration(ProcessorConfiguration processorConfiguration) {
        this.processorConfigurations.add(processorConfiguration);
    }

    @Override
    public String toString() {
        return "StreamConfiguration{" +
                "stream='" + stream + '\'' +
                ", processorConfigurations=" + processorConfigurations +
                '}';
    }
}
