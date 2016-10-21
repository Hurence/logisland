/**
 * Copyright (C) 2016 Hurence (bailet.thomas@gmail.com)
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