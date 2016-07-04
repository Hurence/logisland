/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hurence.logisland.config;

import com.hurence.logisland.components.ComponentsFactory;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author tom
 */
public class LogislandSessionConfigReaderTest {

    private static final String SAMPLE_CONFIG_PATH = "/data/sample-config.yml";

    private static Logger logger = LoggerFactory.getLogger(LogislandSessionConfigReaderTest.class);

    @Test
    public void testLoadConfig() {


        LogislandSessionConfigReader read = new LogislandSessionConfigReader();

        LogislandSessionConfiguration config = read.loadConfig(this.getClass().getResource(SAMPLE_CONFIG_PATH).getFile());
        logger.info(config.toString());


        for (ComponentConfiguration compoConfig : config.getProcessors()) {
            ComponentsFactory.getComponent(compoConfig);
        }

    }
}