/**
 * Copyright (C) 2016 Hurence (bailet.thomas@gmail.com)
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
 * A processorChain is a component + a set of  processorConfigurations
 */
public class ProcessorChainConfiguration extends AbstractComponentConfiguration {

    private String processorChain = "";

    private List<ProcessorConfiguration> processorConfigurations = new ArrayList<>();

    public String getProcessorChain() {
        return processorChain;
    }

    public void setProcessorChain(String processorChain) {
        this.processorChain = processorChain;
    }

    public List<ProcessorConfiguration> getProcessorConfigurations() {
        return processorConfigurations;
    }

    public void addProcessorConfiguration(ProcessorConfiguration processorConfiguration) {
        this.processorConfigurations.add(processorConfiguration);
    }

    @Override
    public String toString() {
        return "ProcessorChainConfiguration{" +
                "processorChain='" + processorChain + '\'' +
                ", processorConfigurations=" + processorConfigurations +
                '}';
    }
}
