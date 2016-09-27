package com.hurence.logisland.processor;

import com.hurence.logisland.config.ComponentFactory;
import com.hurence.logisland.config.AbstractComponentConfiguration;
import com.hurence.logisland.config.ConfigReader;
import com.hurence.logisland.config.LogislandConfiguration;
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
    public void testLoadConfig() throws Exception {


        ConfigReader read = new ConfigReader();

        LogislandConfiguration config = read.loadConfig(this.getClass().getResource(SAMPLE_CONFIG_PATH).getFile());
        logger.info(config.toString());



    }
}