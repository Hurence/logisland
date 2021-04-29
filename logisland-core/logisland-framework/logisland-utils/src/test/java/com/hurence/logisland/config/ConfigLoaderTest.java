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

import com.fasterxml.jackson.databind.JsonMappingException;
import com.hurence.logisland.component.ComponentFactory;
import com.hurence.logisland.engine.EngineContext;
import com.hurence.logisland.engine.MockProcessingEngine;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * @author tom
 */
public class ConfigLoaderTest {

    private static final String SAMPLE_CONFIG_PATH = "/configuration-template.yml";
    private static final String SAMPLE_CONFIG_PATH_2 = "/configuration-templatev2.yml";
    private static final String BAD_SAMPLE_CONFIG_PATH = "/bad-configuration-template-duplicate-conf.yml";
    private static final String BAD_SAMPLE_CONFIG_PATH_2 = "/bad-configuration-template-missing-component-in-engine.yml";



    private static Logger logger = LoggerFactory.getLogger(ConfigLoaderTest.class);

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Test
    public void testLoadConfig() throws Exception {


        LogislandConfiguration config =
                ConfigReader.loadConfig(this.getClass().getResource(SAMPLE_CONFIG_PATH).getFile());

        logger.info(config.toString());

        EngineConfiguration engineConf = config.getEngine();
        assertEquals("com.hurence.logisland.engine.MockProcessingEngine", engineConf.getComponent());
        assertEquals("Main Logisland job entry point", engineConf.getDocumentation());
        assertEquals("engine", engineConf.getType());
        assertEquals(1, engineConf.getConfiguration().size());
        assertTrue(engineConf.getConfiguration().containsKey("fake.settings"));
        assertEquals(1, engineConf.getStreamConfigurations().size());
        assertEquals("parsing_stream", engineConf.getStreamConfigurations().get(0).getStream());
        assertEquals("com.hurence.logisland.stream.MockRecordStream", engineConf.getStreamConfigurations().get(0).getComponent());
        assertEquals(1, engineConf.getStreamConfigurations().get(0).getProcessorConfigurations().size());
        assertEquals(0, engineConf.getControllerServiceConfigurations().size());

        Optional<EngineContext> context = ComponentFactory.buildAndSetUpEngineContext(engineConf);

        assertTrue(context.isPresent());

//        assertNull(context.get().getLogger());

        assertEquals(301, context.get().getPropertyValue(MockProcessingEngine.FAKE_SETTINGS).asInteger().intValue());

        assertEquals(1, context.get().getStreamContexts().size());
        //   engineInstance.get().getProcessorChainInstances().get(0)

        assertTrue(context.get().isValid());
    }


    @Test
    public void testLoadConfig2() throws Exception {


        LogislandConfiguration config =
                ConfigReader.loadConfig(this.getClass().getResource(SAMPLE_CONFIG_PATH_2).getFile());

        logger.info(config.toString());

        EngineConfiguration engineConf = config.getEngine();
        assertEquals(2, engineConf.getStreamConfigurations().size());
        StreamConfiguration stream1 = engineConf.getStreamConfigurations().get(0);
        StreamConfiguration stream2 = engineConf.getStreamConfigurations().get(1);
        assertEquals("parsing_stream1", stream1.getStream());
        assertEquals("parsing_stream2", stream2.getStream());
        assertEquals("com.hurence.logisland.stream.MockRecordStream", stream1.getComponent());
        assertEquals("com.hurence.logisland.stream.MockRecordStream", stream2.getComponent());
        assertEquals(1, stream1.getProcessorConfigurations().size());
        assertEquals(2, stream2.getProcessorConfigurations().size());
        assertEquals(2, engineConf.getControllerServiceConfigurations().size());

//        Optional<EngineContext> context = ComponentFactory.buildAndSetUpEngineContext(engineConf);
//
//        assertTrue(context.isPresent());
//
//        assertEquals(2, context.get().getStreamContexts().size());
//
//        assertTrue(context.get().isValid());
    }

    /**
     * verify that a conf file with duplicate key configuration fail
     * @throws Exception
     */
    @Test
    public void testBadLoadConfig() throws Exception {
        expectedEx.expect(JsonMappingException.class);
        expectedEx.expectMessage("Duplicate field 'fake.settings'");
        LogislandConfiguration config = ConfigReader.loadConfig(this.getClass().getResource(BAD_SAMPLE_CONFIG_PATH).getFile());
    }

    /**
     * verify that a conf file with duplicate key configuration fail
     * @throws Exception
     */
    @Test
    public void testBadLoadConfig2() throws Exception {
        expectedEx.expect(IllegalArgumentException.class);
        expectedEx.expectMessage("key 'component' is missing or empty for engine in configuration file");
        LogislandConfiguration config = ConfigReader.loadConfig(this.getClass().getResource(BAD_SAMPLE_CONFIG_PATH_2).getFile());
    }



}