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
package com.hurence.logisland.engine;

import com.hurence.logisland.component.ComponentFactory;
import com.hurence.logisland.config.ConfigReader;
import com.hurence.logisland.config.LogislandConfiguration;
import com.hurence.logisland.util.spark.SparkUtils;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;


public class StreamDebuggerTest {
    private static Logger logger = LoggerFactory.getLogger(StreamDebuggerTest.class);

    private static final String JOB_CONF_FILE = "/conf/structured-stream.yml";

    @Test
    @Ignore
    public void remoteTest() {


        logger.info("starting StreamProcessingRunner");

        Optional<EngineContext> engineInstance = Optional.empty();
        try {

            String configFile = StreamDebuggerTest.class.getResource(JOB_CONF_FILE).getPath();

            // load the YAML config
            LogislandConfiguration sessionConf = ConfigReader.loadConfig(configFile);

            // instanciate engine and all the processor from the config
            engineInstance = ComponentFactory.getEngineContext(sessionConf.getEngine());
            assert engineInstance.isPresent();
            assert engineInstance.get().isValid();

            logger.info("starting Logisland session version {}", sessionConf.getVersion());
            logger.info(sessionConf.getDocumentation());
        } catch (Exception e) {
            logger.error("unable to launch runner : {}", e);
        }

        try {
            // start the engine
            EngineContext engineContext = engineInstance.get();
            engineInstance.get().getEngine().start(engineContext);
            SparkUtils.customizeLogLevels();
            engineContext.getEngine().awaitTermination(engineContext);
        } catch (Exception e) {
            logger.error("something went bad while running the job : {}", e);
            System.exit(-1);
        }






    }

}
