/*
 * Copyright 2016 Hurence
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.hurence.logisland.processor;

import com.hurence.logisland.component.ComponentContext;
import com.hurence.logisland.record.Record;

import java.util.Collection;
import java.util.List;
import java.util.Stack;


public abstract class AbstractProcessorChain extends AbstractProcessor implements ProcessorChain {

    private List<Processor> processors = new Stack<>();

    @Override
    public List<Processor> getProcessors() {
        return processors;
    }

    @Override
    public void addProcessor(Processor processor) {
        processors.add(processor);
    }

    /**
     * Process the given records and send the output records
     * to the next processor in the list
     *
     * @param context the context of processor
     * @param records the input inital records
     * @return
     */
    @Override
    public Collection<Record> process(ComponentContext context, Collection<Record> records) {
        Collection<Record> processedRecords = records;
        for (Processor processor : processors) {
            processedRecords = processor.process(context, processedRecords);
        }
        return processedRecords;
    }

}
