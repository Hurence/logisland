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

import com.hurence.logisland.component.AbstractConfigurableComponent;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;


public abstract class AbstractProcessor extends AbstractConfigurableComponent implements Processor {


    private static Logger logger = LoggerFactory.getLogger(AbstractProcessor.class);

    @Override
    public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {
        logger.info("property {} value changed from {} to {}", descriptor.getName(), oldValue, newValue);
    }

    @Override
    public void init(ProcessContext context) {
        logger.info("init");
    }


    /**
     * Call process with a singleton list made with the single record
     *
     * @param context the current process context
     * @param record the record to handle
     * @return
     */
    @Override
    public Collection<Record> process(ProcessContext context, Record record) {
        return process(context, Collections.singleton(record));
    }
}
