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

    private static final String SAMPLE_CONFIG_PATH = "/logisland-common-parsers-plugin/src/test/resources/data/sample-config.yml";

    private static Logger logger = LoggerFactory.getLogger(LogislandSessionConfigReaderTest.class);

    @Test
    public void testLoadConfig() {



        try {
            LogislandSessionConfigReader read = new LogislandSessionConfigReader();
            LogislandSessionConfiguration config =  read.loadConfig(this.getClass().getResource(SAMPLE_CONFIG_PATH).getFile());
            config.getComponents().forEach(ComponentsFactory::getProcessorInstance);
            logger.info(config.toString());
        } catch (Exception e) {
            logger.error(e.getMessage());
        }





    }
}