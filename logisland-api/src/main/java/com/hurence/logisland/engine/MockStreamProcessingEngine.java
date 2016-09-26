package com.hurence.logisland.engine;

import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.log.StandardParserInstance;
import com.hurence.logisland.processor.StandardProcessorInstance;
import com.hurence.logisland.validators.StandardValidators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by tom on 06/07/16.
 */
public class MockStreamProcessingEngine extends AbstractStreamProcessingEngine {


    private static Logger logger = LoggerFactory.getLogger(MockStreamProcessingEngine.class);
    public static final PropertyDescriptor FAKE_SETTINGS = new PropertyDescriptor.Builder()
            .name("fake.settings")
            .description("")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("oups")
            .build();

    public List<PropertyDescriptor> getSupportedPropertyDescriptors()  {
        List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(FAKE_SETTINGS);

        return Collections.unmodifiableList(descriptors);
    }

    @Override
    public void start(EngineContext engineContext, List<StandardProcessorInstance> processorInstances, List<StandardParserInstance> parserInstances) {

        logger.info("engine start");
    }

    @Override
    public void shutdown(EngineContext engineContext) {

    }

    @Override
    public String getIdentifier() {
        return null;
    }
}
