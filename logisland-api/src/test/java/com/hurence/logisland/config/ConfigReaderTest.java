/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hurence.logisland.config;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author tom
 */
public class ConfigReaderTest {

    private static final String SAMPLE_CONFIG_PATH = "/logisland-common-parsers-plugin/src/test/resources/data/sample-config.yml";

    private static Logger logger = LoggerFactory.getLogger(ConfigReaderTest.class);

    @Test
    public void testLoadConfig() {


        try {
            ConfigReader read = new ConfigReader();
            LogislandConfiguration config = read.loadConfig(this.getClass().getResource(SAMPLE_CONFIG_PATH).getFile());
            logger.info(config.toString());
        } catch (Exception e) {
            logger.error(e.getMessage());
        }


    }
}