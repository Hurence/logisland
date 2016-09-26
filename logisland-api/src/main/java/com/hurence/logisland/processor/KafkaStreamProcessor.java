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

import com.hurence.logisland.component.AbstractConfigurableComponent;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.validators.StandardValidators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


public class KafkaStreamProcessor extends AbstractProcessorChain {

    public static final String DEAD_LETTER_TOPIC = "logisland-dead-letter-queue";
    private static Logger logger = LoggerFactory.getLogger(KafkaStreamProcessor.class);


    public static final PropertyDescriptor OUTPUT_TOPICS = new PropertyDescriptor.Builder()
            .name("kafka.output.topics")
            .description("Sets the output Kafka topic name")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue(DEAD_LETTER_TOPIC)
            .build();

    public static final PropertyDescriptor INPUT_TOPICS = new PropertyDescriptor.Builder()
            .name("kafka.input.topics")
            .description("Sets the input Kafka topic name")
            .required(true)
            .defaultValue(DEAD_LETTER_TOPIC)
            .build();

    public static final PropertyDescriptor ERROR_TOPICS = new PropertyDescriptor.Builder()
            .name("kafka.error.topics")
            .description("Sets the error topics Kafka topic name")
            .required(true)
            .defaultValue(DEAD_LETTER_TOPIC)
            .build();



    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(ERROR_TOPICS);
        descriptors.add(INPUT_TOPICS);
        descriptors.add(OUTPUT_TOPICS);

        return Collections.unmodifiableList(descriptors);
    }

    @Override
    public String getIdentifier() {
        return null;
    }
}
