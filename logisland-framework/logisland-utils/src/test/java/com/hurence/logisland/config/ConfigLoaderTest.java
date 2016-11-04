/**
 * Copyright (C) 2016 Hurence (bailet.thomas@gmail.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.config;

import com.hurence.logisland.component.ComponentFactory;
import com.hurence.logisland.engine.EngineContext;
import com.hurence.logisland.engine.MockProcessingEngine;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author tom
 */
public class ConfigLoaderTest {

    private static final String SAMPLE_CONFIG_PATH = "/configuration-template.yml";

    private static Logger logger = LoggerFactory.getLogger(ConfigLoaderTest.class);

    @Test
    public void testLoadConfig() throws Exception {


        LogislandConfiguration config =
                ConfigReader.loadConfig(this.getClass().getResource(SAMPLE_CONFIG_PATH).getFile());


        logger.info(config.toString());

        Optional<EngineContext> context = ComponentFactory.getEngineContext(config.getEngine());

        assertTrue(context.isPresent());

        assertEquals(301, context.get().getPropertyValue(MockProcessingEngine.FAKE_SETTINGS).asInteger().intValue());

        assertEquals(1, context.get().getStreamContexts().size());
        //   engineInstance.get().getProcessorChainInstances().get(0)

        assertTrue(context.get().isValid());
    }
}