package com.hurence.logisland.processor;

import com.hurence.logisland.components.PropertyDescriptor;
import com.hurence.logisland.record.Record;
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
public class MockProcessor extends AbstractRecordProcessor {


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
    public Collection<Record> process(final ProcessContext context, final Collection<Record> collection) {


      //  collection.stream().forEach(event -> logger.info("mock processing event : {}", event));

        Record mockRecord = new Record(EVENT_TYPE_NAME);
        mockRecord.setField("incomingEventsCount", "int", collection.size());
        mockRecord.setField("message", "string", context.getProperty(FAKE_MESSAGE).getRawValue());


        List<Record> mockResults = new ArrayList<>();
        mockResults.add(mockRecord);
        logger.info("mock processing event : {}", mockRecord);
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
