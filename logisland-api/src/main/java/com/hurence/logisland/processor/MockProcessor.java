package com.hurence.logisland.processor;

import com.hurence.logisland.components.PropertyDescriptor;
import com.hurence.logisland.event.Event;
import com.hurence.logisland.validators.StandardValidators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Created by fprunier on 15/04/16.
 */
public class MockProcessor extends AbstractEventProcessor {


    public static final PropertyDescriptor FAKE_MESSAGE = new PropertyDescriptor.Builder()
            .name("fake.message")
            .description("a fake message")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("yoyo")
            .build();


    private static Logger logger = LoggerFactory.getLogger(MockProcessor.class);

    private static String EVENT_TYPE_NAME = "mock";


    @Override
    public void init(final ProcessContext context) {
        logger.info("init MockProcessor");
    }

    @Override
    public Collection<Event> process(final ProcessContext context, final Collection<Event> collection) {


      //  collection.stream().forEach(event -> logger.info("mock processing event : {}", event));

        Event mockEvent = new Event(EVENT_TYPE_NAME);
        mockEvent.put("incomingEventsCount", "int", collection.size());
        mockEvent.put("message", "string", context.getProperty(FAKE_MESSAGE).getValue());


        List<Event> mockResults = new ArrayList<>();
        mockResults.add(mockEvent);
        logger.info("mock processing event : {}", mockEvent);
        return mockResults;
    }


    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(ERROR_TOPICS);
        descriptors.add(INPUT_TOPICS);
        descriptors.add(OUTPUT_TOPICS);
        descriptors.add(INPUT_SCHEMA);
        descriptors.add(OUTPUT_SCHEMA);
        descriptors.add(FAKE_MESSAGE);

        return Collections.unmodifiableList(descriptors);
    }

    @Override
    public String getIdentifier() {
        return null;
    }
}
