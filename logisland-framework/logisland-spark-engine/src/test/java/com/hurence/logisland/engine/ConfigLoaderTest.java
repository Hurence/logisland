package com.hurence.logisland.engine;

import com.hurence.logisland.components.ComponentsFactory;
import com.hurence.logisland.components.PropertyDescriptor;
import com.hurence.logisland.components.PropertyValue;
import com.hurence.logisland.config.LogislandSessionConfigReader;
import com.hurence.logisland.config.LogislandSessionConfiguration;
import com.hurence.logisland.processor.StandardProcessorInstance;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * @author tom
 */
public class ConfigLoaderTest {

    private static final String SAMPLE_CONFIG_PATH = "/configuration-template.yml";

    private static Logger logger = LoggerFactory.getLogger(ConfigLoaderTest.class);

    @Test
    public void testLoadConfig() {


        LogislandSessionConfigReader read = new LogislandSessionConfigReader();

        LogislandSessionConfiguration config = read.loadConfig(this.getClass().getResource(SAMPLE_CONFIG_PATH).getFile());
        logger.info(config.toString());

        Optional<StandardEngineInstance> engineInstance = ComponentsFactory.getEngineInstance(config);

        Assert.assertTrue(engineInstance.isPresent());

        StandardEngineContext context = new StandardEngineContext(engineInstance.get());

        Assert.assertEquals(context.getProperty("spark.master").getValue(), "local[8]");
        Assert.assertEquals(context.getProperty("spark.streaming.blockInterval").asInteger().intValue(), 350);


        List<StandardProcessorInstance> processors = ComponentsFactory.getAllProcessorInstances(config);
       // engineInstance.get().getEngine().start(context, processors);
    }
}