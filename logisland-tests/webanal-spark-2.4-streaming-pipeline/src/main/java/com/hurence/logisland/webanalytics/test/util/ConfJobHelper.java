package com.hurence.logisland.webanalytics.test.util;

import com.hurence.logisland.component.ComponentFactory;
import com.hurence.logisland.config.ConfigReader;
import com.hurence.logisland.config.ControllerServiceConfiguration;
import com.hurence.logisland.config.LogislandConfiguration;
import com.hurence.logisland.engine.EngineContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

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

    public void initEngineContext() {
        this.engineContext = ComponentFactory.getEngineContext(jobConfig.getEngine())
                .orElseThrow(() -> new IllegalArgumentException("engineInstance could not be instantiated"));
        if (!engineContext.isValid()) {
            throw new IllegalArgumentException("engineInstance is not valid with input configuration !");
        }
    }

    public EngineContext getEngineContext() {
        return engineContext;
    }

    public void startJob() {
        String engineName = engineContext.getEngine().getIdentifier();
        try {
            logger.info("Start engine {}", engineName);
            engineContext.getEngine().start(engineContext);
        } catch (Exception e) {
            logger.error("Something went bad while running the job {} : {}", engineName, e);
            System.exit(-1);
        }
    }

    public void startThenAwaitTermination() {
        String engineName = engineContext.getEngine().getIdentifier();
        startJob();
        try {
            logger.info("Waiting termination of engine {}", engineName);
            engineContext.getEngine().awaitTermination(engineContext);
            logger.info("Engine {} terminated", engineName);
            System.exit(0);
        } catch (Exception e) {
            logger.error("Something went bad while running the job {} : {}", engineName, e);
            System.exit(-1);
        }
    }


}
