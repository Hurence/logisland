package com.hurence.logisland.webanalytics.test.util;

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
        Optional<EngineContext> engineInstance = ComponentFactory.getEngineContext(jobConfig.getEngine());
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

    public void awaitTermination() {
        String engineName = engineContext.getEngine().getIdentifier();
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

    public void startThenAwaitTermination() {
        startJob();
        awaitTermination();
    }
}
