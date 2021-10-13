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
package com.hurence.logisland.util.spark;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.hurence.logisland.config.LogislandConfiguration;
import com.hurence.logisland.util.string.StringUtils;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;

import java.util.Arrays;

import static com.hurence.logisland.config.ConfigReader.checkLogislandConf;

/**
 * This configuration reader depends on spark. We do not want to place methods in this class in the
 * com.hurence.logisland.config.ConfigReader class where the loadConfig (from local filesystem) method
 * resides, as it would introduce a spark dependency in the logisland-framework module. Only the spark
 * engine should have a spark dependency. So this class should be loaded from the StreamProcessingRunner
 * and this will succeed only in environments where a spark 2 engine is available and used, otherwise it
 * will fail to load. This will for instance be successful in the databricks environment, which is by the
 * way the first purpose for which this class is being introduced.
 */
public class SparkConfigReader {

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

        // replace all host from environment variables
        String fileContent = StringUtils.resolveEnvVars(configString, "localhost");

        System.out.println("Configuration:\n" + fileContent);

        LogislandConfiguration logislandConf = mapper.readValue(fileContent, LogislandConfiguration.class);
        checkLogislandConf(logislandConf);

        return logislandConf;
    }
}
