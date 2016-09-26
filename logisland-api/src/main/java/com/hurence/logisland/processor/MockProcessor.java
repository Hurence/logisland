/*
 * Copyright 2016 Hurence
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.hurence.logisland.processor;

import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.validators.StandardValidators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;


public class MockProcessor extends KafkaStreamProcessor {


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
