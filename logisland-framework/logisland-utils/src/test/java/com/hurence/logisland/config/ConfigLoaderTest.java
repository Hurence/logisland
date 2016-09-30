package com.hurence.logisland.config;

import com.hurence.logisland.engine.StandardEngineInstance;
import com.hurence.logisland.component.StandardComponentContext;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

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

        Optional<StandardEngineInstance> engineInstance = ComponentFactory.getEngineInstance(config.getEngine());

        Assert.assertTrue(engineInstance.isPresent());

        StandardComponentContext context = new StandardComponentContext(engineInstance.get());

        Assert.assertEquals(301, context.getProperty("fake.settings").asInteger().intValue());

        Assert.assertEquals(1, engineInstance.get().getProcessorChainInstances().size());
        //   engineInstance.get().getProcessorChainInstances().get(0)

    }
}