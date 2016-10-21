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
package com.hurence.logisland.processor.chain;

import com.hurence.logisland.component.AbstractConfiguredComponent;
import com.hurence.logisland.component.ConfigurableComponent;
import com.hurence.logisland.component.ValidationContext;
import com.hurence.logisland.component.ValidationResult;
import com.hurence.logisland.processor.StandardProcessorInstance;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;


public class StandardProcessorChainInstance extends AbstractConfiguredComponent {


    private List<StandardProcessorInstance> processors = new ArrayList<>();

    public StandardProcessorChainInstance(ConfigurableComponent processor, String id) {
        super(processor, id);

    }

    public void addProcessorInstance(StandardProcessorInstance processorInstance){
        processors.add(processorInstance);

    }


    public List<StandardProcessorInstance> getProcessorInstances() {
        return processors;
    }

    @Override
    public void verifyModifiable() throws IllegalStateException {

    }
}
