package com.hurence.logisland.processor;

import com.hurence.logisland.components.ComponentsFactory;
import com.hurence.logisland.config.ComponentConfiguration;
import com.hurence.logisland.config.LogislandSessionConfigReader;
import com.hurence.logisland.config.LogislandSessionConfiguration;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author tom
 */
public class ConfigLoaderTest {

    private static final String SAMPLE_CONFIG_PATH = "/sample-config.yml";

    private static Logger logger = LoggerFactory.getLogger(ConfigLoaderTest.class);

    @Test
    public void testLoadConfig() {


        LogislandSessionConfigReader read = new LogislandSessionConfigReader();

        LogislandSessionConfiguration config = read.loadConfig(this.getClass().getResource(SAMPLE_CONFIG_PATH).getFile());
        logger.info(config.toString());


        for (ComponentConfiguration compoConfig : config.getProcessors()) {
            ComponentsFactory.getProcessorInstance(compoConfig);
        }

    }
}