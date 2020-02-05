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


import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.hurence.logisland.util.string.StringUtils;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;


public class ConfigReader {


    static String readFile(String path, Charset encoding)
            throws IOException {
        byte[] encoded = Files.readAllBytes(Paths.get(path));
        return new String(encoded, encoding);
    }


    /**
     * Loads a YAML config file (file located in the local file system)
     *
     * @param configFilePath the path of the config file
     * @return a LogislandSessionConfiguration
     * @throws Exception
     */
    public static LogislandConfiguration loadConfig(String configFilePath) throws Exception {

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory()
                .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION));

        File configFile = new File(configFilePath);

        if (!configFile.exists()) {
            throw new FileNotFoundException("Error: File " + configFilePath + " not found!");
        }

        // replace all host from environment variables
        String fileContent = StringUtils.resolveEnvVars(readFile(configFilePath, Charset.defaultCharset()), "localhost");

        LogislandConfiguration logislandConf = mapper.readValue(fileContent, LogislandConfiguration.class);
        checkLogislandConf(logislandConf);

        return logislandConf;
    }

    /**
     * Loads a YAML config file using (file located in the shared filesystem)
     *
     * @param configFilePath the path of the config file
     * @return a LogislandSessionConfiguration
     * @throws Exception
     */
    public static LogislandConfiguration loadConfigFromSharedFS(String configFilePath) throws Exception {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

        /**
         * In Databricks, developers should utilize the shared SparkContext instead of creating one using the constructor.
         * When running a job, you can access the shared context by calling SparkContext.getOrCreate().
         *
         * Also in databricks, a path like /path/to/a/file will be loaded from DBFS so will be interpreted like
         * dbfs:/path/to/a/file
         */

        SparkContext sparkContext = SparkContext.getOrCreate();

        RDD<String> configRdd = sparkContext.textFile(configFilePath, 1);
        String[] configStringArray = (String[])configRdd.collect();
        String configString = String.join("\n", Arrays.asList(configStringArray));

        System.out.println("DBFS Configuration:\n" + configString);

        // replace all host from environment variables
        String fileContent = StringUtils.resolveEnvVars(configString, "localhost");

        System.out.println("Resolved Configuration:\n" + fileContent);

        LogislandConfiguration logislandConf = mapper.readValue(fileContent, LogislandConfiguration.class);
        checkLogislandConf(logislandConf);

        return logislandConf;
    }

    private static void checkLogislandConf(LogislandConfiguration conf) throws IllegalArgumentException {
        if (conf.getEngine().getComponent() == null || conf.getEngine().getComponent().isEmpty()) {
            throw new IllegalArgumentException("key 'component' is missing or empty for engine in configuration file");
        }
    }

}
