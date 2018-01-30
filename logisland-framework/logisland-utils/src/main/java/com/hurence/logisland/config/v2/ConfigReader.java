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
package com.hurence.logisland.config.v2;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.hurence.logisland.config.v2.JobConfig;

import java.io.File;
import java.io.FileNotFoundException;


public class ConfigReader {


    /**
     * Loads a YAML config file
     *
     * @param configFilePath the path of the config file
     * @return a LogislandSessionConfiguration
     * @throws Exception
     */
    public static JobConfig loadConfig(String configFilePath) throws Exception {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        File configFile = new File(configFilePath);

        if (!configFile.exists()) {
            throw new FileNotFoundException("Error: File " + configFilePath + " not found!");
        }

        return mapper.readValue(configFile, JobConfig.class);
    }

}
