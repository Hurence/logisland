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
package com.hurence.logisland.utils;

import com.hurence.logisland.component.ComponentFactory;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.config.ConfigReader;
import com.hurence.logisland.config.ControllerServiceConfiguration;
import com.hurence.logisland.config.LogislandConfiguration;
import com.hurence.logisland.engine.EngineContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

public class ConfJobHelper {

    private static Logger logger = LoggerFactory.getLogger(ConfJobHelper.class);

    private final LogislandConfiguration jobConfig;
    private EngineContext engineContext;

    public ConfJobHelper(LogislandConfiguration jobConfig) {
        this.jobConfig = jobConfig;
    }

    public ConfJobHelper(String pathConfFile) throws IOException {
        this(ConfigReader.loadConfig(pathConfFile));
    }

    public void modifyControllerServiceConf(String id, Map<String, String> confToModify) {
        ControllerServiceConfiguration service = jobConfig.getEngine().getControllerServiceConfigurations()
                .stream()
                .filter(c -> id.equals(c.getControllerService()))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("service with id " + id + " does not exist"));
        service.getConfiguration().putAll(confToModify);
    }

    public void initJob() throws InitializationException {
        // instantiate engine and all the processor from the config
        // this init the engine
        Optional<EngineContext> engineInstance = ComponentFactory.buildAndSetUpEngineContext(jobConfig.getEngine());
        if (!engineInstance.isPresent()) {
            throw new IllegalArgumentException("engineInstance could not be instantiated");
        }
        if (!engineInstance.get().isValid()) {
            throw new IllegalArgumentException("engineInstance is not valid with input configuration !");
        }
        engineContext = engineInstance.get();
        logger.info("Initialized Logisland job version {}", jobConfig.getVersion());
        logger.info(jobConfig.getDocumentation());
    }

    public void stopJob() {
        engineContext.getEngine().stop(engineContext);
    }

    public void softStop() {
        engineContext.getEngine().softStop(engineContext);
    }

    public void startJob() {
        String engineName = engineContext.getEngine().getIdentifier();
        try {
            logger.info("Init engine {}", engineName);
            engineContext.getEngine().init(engineContext);
            logger.info("Start engine {}", engineName);
            engineContext.getEngine().start(engineContext);
            engineContext.getEngine().awaitTermination(engineContext);
        } catch (Exception e) {
            logger.error("Something went bad while running the job {} : {}", engineName, e);
            System.exit(-1);
        }
    }

    public void awaitTermination() {
        String engineName = engineContext.getEngine().getIdentifier();
        try {
            logger.info("Waiting termination of engine {}", engineName);
            logger.info("Engine {} terminated", engineName);
            System.exit(0);
        } catch (Exception e) {
            logger.error("Something went bad while running the job {} : {}", engineName, e);
            System.exit(-1);
        }
    }

    public void startThenAwaitTermination() {
        startJob();
        awaitTermination();
    }
}
