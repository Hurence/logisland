/**
 * Copyright (C) 2016 Hurence (bailet.thomas@gmail.com)
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
package com.hurence.logisland.util.runner;

import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.processor.AbstractProcessor;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.validator.StandardValidators;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

@Tags({"record", "mock", "test"})
@CapabilityDescription("This is a processor that add a fake message to each incoming records")
public class MockProcessor extends AbstractProcessor {


    public static final PropertyDescriptor FAKE_MESSAGE = new PropertyDescriptor.Builder()
            .name("fake.message")
            .description("a fake message")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("yoyo")
            .build();


    @Override
    public Collection<Record> process(final ProcessContext context, final Collection<Record> collection) {

        final String message = context.getPropertyValue(FAKE_MESSAGE).asString();
        final List<Record> outputRecords = new ArrayList<>(collection);
        outputRecords.forEach(record -> record.setStringField("message", message));

        return outputRecords;
    }


    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(FAKE_MESSAGE);

        return Collections.unmodifiableList(descriptors);
    }

}
