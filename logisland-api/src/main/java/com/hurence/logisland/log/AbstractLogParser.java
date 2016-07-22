package com.hurence.logisland.log;

import com.hurence.logisland.components.AbstractConfigurableComponent;
import com.hurence.logisland.components.PropertyDescriptor;
import com.hurence.logisland.validators.StandardValidators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * sdf sd
 *   test
 */
public abstract class AbstractLogParser extends AbstractConfigurableComponent implements LogParser {


    public static final String DEAD_LETTER_TOPIC = "logisland-dead-letter-queue";
    private static Logger logger = LoggerFactory.getLogger(AbstractLogParser.class);
    //private final EventStreamConfig config;

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

    public static final PropertyDescriptor INPUT_SCHEMA = new PropertyDescriptor.Builder()
            .name("avro.input.schema")
            .description("the avro schema definition")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor OUTPUT_SCHEMA = new PropertyDescriptor.Builder()
            .name("avro.output.schema")
            .description("the avro schema definition for the output serialization")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    @Override
    public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {
        logger.info("property {} value changed from {} to {}", descriptor.getName(), oldValue, newValue);
    }
}
