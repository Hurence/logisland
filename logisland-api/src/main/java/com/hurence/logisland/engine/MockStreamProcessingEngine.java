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
package com.hurence.logisland.engine;

import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.processor.StandardProcessorInstance;
import com.hurence.logisland.validator.StandardPropertyValidators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


public class MockStreamProcessingEngine extends AbstractStreamProcessingEngine {


    private static Logger logger = LoggerFactory.getLogger(MockStreamProcessingEngine.class);
    public static final PropertyDescriptor FAKE_SETTINGS = new PropertyDescriptor.Builder()
            .name("fake.settings")
            .description("")
            .required(false)
            .addValidator(StandardPropertyValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("oups")
            .build();

    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(FAKE_SETTINGS);

        return Collections.unmodifiableList(descriptors);
    }

    @Override
    public void start(EngineContext engineContext) {

        logger.info("engine start");
    }

    @Override
    public void shutdown(EngineContext engineContext) {
        logger.info("engine shutdown");
    }


}
