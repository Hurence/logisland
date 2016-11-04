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
package com.hurence.logisland.stream;

import com.hurence.logisland.component.AbstractConfigurableComponent;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.validator.StandardValidators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class AbstractRecordStream extends AbstractConfigurableComponent implements RecordStream {


    public static final PropertyDescriptor INCLUDE_INPUT_RECORDS = new PropertyDescriptor.Builder()
            .name("include.input.records")
            .description("if set to true all the input records are copied to output")
            .required(false)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("true")
            .build();

    private static Logger logger = LoggerFactory.getLogger(AbstractRecordStream.class);

    @Override
    public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {
        logger.info("property {} value changed from {} to {}", descriptor.getName(), oldValue, newValue);
    }

    @Override
    public void start(){
        logger.info("start stream processing");
    }

    @Override
    public void stop(){
        logger.info("stop stream processing");
    }


}
