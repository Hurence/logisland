/**
 * Copyright (C) 2016 Hurence (support@hurence.com)
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


import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.hurence.logisland.util.string.StringUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;


public class ConfigReader {


    static String readFile(String path, Charset encoding)
            throws IOException {
        byte[] encoded = Files.readAllBytes(Paths.get(path));
        return new String(encoded, encoding);
    }


    /**
     * Loads a YAML config file
     *
     * @param configFilePath the path of the config file
     * @return a LogislandSessionConfiguration
     * @throws Exception
     */
    public static LogislandConfiguration loadConfig(String configFilePath) throws Exception {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        File configFile = new File(configFilePath);

        if (!configFile.exists()) {
            throw new FileNotFoundException("Error: File " + configFilePath + " not found!");
        }

        // replace all host from environment variables
        String fileContent = StringUtils.resolveEnvVars(readFile(configFilePath, Charset.defaultCharset()), "localhost");
        //String fileContent = readFile(configFilePath, Charset.defaultCharset());

        return mapper.readValue(fileContent, LogislandConfiguration.class);
    }

}
