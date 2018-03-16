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
package com.hurence.logisland.config.v2;

import java.util.ArrayList;
import java.util.List;


/**
 * A stream is a component + a set of  processorConfigurations
 */
public class StreamConfig extends AbstractComponentConfig {

    private String stream = "";

    private SourceProviderConfig source;

    private SinkProviderConfig sink = new SinkProviderConfig();

    private List<ProcessorConfig> processors = new ArrayList<>();

    public String getStream() {
        return stream;
    }

    public void setStream(String stream) {
        this.stream = stream;
    }

    public List<ProcessorConfig> getProcessors() {
        return processors;
    }

    public SourceProviderConfig getSource() {
        return source;
    }

    public void setSource(SourceProviderConfig source) {
        this.source = source;
    }

    public SinkProviderConfig getSink() {
        return sink;
    }

    public void setSink(SinkProviderConfig sink) {
        this.sink = sink;
    }

    public void setProcessors(List<ProcessorConfig> processors) {
        this.processors = processors;
    }

    @Override
    public String toString() {
        return "StreamConfig{" +
                "stream='" + stream + '\'' +
                ", processors=" + processors +
                '}';
    }
}
