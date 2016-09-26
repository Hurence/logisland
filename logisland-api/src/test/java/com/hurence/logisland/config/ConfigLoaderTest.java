package com.hurence.logisland.config;

import com.hurence.logisland.component.ComponentsFactory;
import com.hurence.logisland.engine.StandardEngineContext;
import com.hurence.logisland.engine.StandardEngineInstance;
import com.hurence.logisland.log.StandardParserInstance;
import com.hurence.logisland.processor.StandardProcessorInstance;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;

/**
 * @author tom
 */
public class ConfigLoaderTest {

    private static final String SAMPLE_CONFIG_PATH = "/configuration-template.yml";

    private static Logger logger = LoggerFactory.getLogger(ConfigLoaderTest.class);

    @Test
    public void testLoadConfig() throws Exception {


        LogislandSessionConfiguration config = new LogislandSessionConfigReader()
                .loadConfig(this.getClass().getResource(SAMPLE_CONFIG_PATH).getFile());


        logger.info(config.toString());

        Optional<StandardEngineInstance> engineInstance = ComponentsFactory.getEngineInstance(config);

        Assert.assertTrue(engineInstance.isPresent());

        StandardEngineContext context = new StandardEngineContext(engineInstance.get());

        Assert.assertEquals(context.getProperty("fake.settings").getRawValue(), "oullala");


        List<StandardProcessorInstance> processors = ComponentsFactory.getAllProcessorInstances(config);
        List<StandardParserInstance> parserInstances = ComponentsFactory.getAllParserInstances(config);
        engineInstance.get().getEngine().start(context, processors, parserInstances);
    }
}