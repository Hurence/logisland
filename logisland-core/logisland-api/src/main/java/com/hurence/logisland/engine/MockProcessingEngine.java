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
package com.hurence.logisland.engine;

import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.validator.StandardValidators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


public class MockProcessingEngine extends AbstractProcessingEngine {


    private static Logger logger = LoggerFactory.getLogger(MockProcessingEngine.class);
    public static final PropertyDescriptor FAKE_SETTINGS = new PropertyDescriptor.Builder()
            .name("fake.settings")
            .description("")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("oups")
            .build();

    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(FAKE_SETTINGS);

        return Collections.unmodifiableList(descriptors);
    }

    @Override
    public void init(EngineContext engineContext) {

    }

    @Override
    public void start(EngineContext engineContext) {
        logger.info("engine start");
    }

    @Override
    public void stop(EngineContext engineContext) {
        logger.info("engine reset");
        engineContext.getStreamContexts().clear();
        engineContext.getControllerServiceContexts().clear();
        logger.info("engine shutdown");
    }

    @Override
    public void softStop(EngineContext engineContext) {
        stop(engineContext);
    }

    @Override
    public void awaitTermination(EngineContext engineContext) {
        logger.info("engine awaitTermination");
    }

}
