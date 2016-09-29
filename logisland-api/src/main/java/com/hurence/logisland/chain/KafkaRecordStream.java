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
package com.hurence.logisland.chain;

import com.hurence.logisland.component.AllowableValue;
import com.hurence.logisland.component.ComponentContext;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.serializer.AvroSerializer;
import com.hurence.logisland.serializer.JsonSerializer;
import com.hurence.logisland.serializer.KryoSerializer;
import com.hurence.logisland.validator.StandardPropertyValidators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;


public class KafkaRecordStream extends AbstractProcessorChain {


    public static final String RAW_TOPIC = "logisland_raw";
    public static final String EVENTS_TOPIC = "logisland_events";
    public static final String ERROR_TOPIC = "logisland_errors";


    private static Logger logger = LoggerFactory.getLogger(KafkaRecordStream.class);

    public static final PropertyDescriptor INPUT_TOPICS = new PropertyDescriptor.Builder()
            .name("kafka.input.topics")
            .description("Sets the input Kafka topic name")
            .required(true)
            .defaultValue(RAW_TOPIC)
            .build();

    public static final PropertyDescriptor OUTPUT_TOPICS = new PropertyDescriptor.Builder()
            .name("kafka.output.topics")
            .description("Sets the output Kafka topic name")
            .required(true)
            .addValidator(StandardPropertyValidators.NON_EMPTY_VALIDATOR)
            .defaultValue(EVENTS_TOPIC)
            .build();

    public static final PropertyDescriptor ERROR_TOPICS = new PropertyDescriptor.Builder()
            .name("kafka.error.topics")
            .description("Sets the error topics Kafka topic name")
            .required(true)
            .defaultValue(ERROR_TOPIC)
            .build();

    public static final PropertyDescriptor AVRO_INPUT_SCHEMA = new PropertyDescriptor.Builder()
            .name("avro.input.schema")
            .description("the avro schema definition")
            .required(false)
            .addValidator(StandardPropertyValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor AVRO_OUTPUT_SCHEMA = new PropertyDescriptor.Builder()
            .name("avro.output.schema")
            .description("the avro schema definition for the output serialization")
            .required(false)
            .addValidator(StandardPropertyValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final AllowableValue AVRO_SERIALIZER = new AllowableValue(AvroSerializer.class.getName(),
            "avro serialization",
            "serialize events as avro blocs");

    public static final AllowableValue JSON_SERIALIZER = new AllowableValue(JsonSerializer.class.getName(),
            "avro serialization",
            "serialize events as json blocs");

    public static final AllowableValue KRYO_SERIALIZER = new AllowableValue(KryoSerializer.class.getName(),
            "kryo serialization",
            "serialize events as json blocs");

    public static final AllowableValue NO_SERIALIZER = new AllowableValue("none",
            "no serialization",
            "send events as bytes");

    public static final PropertyDescriptor INPUT_SERIALIZER = new PropertyDescriptor.Builder()
            .name("kafka.input.topics.serializer")
            .description("")
            .required(false)
            .addValidator(StandardPropertyValidators.NON_EMPTY_VALIDATOR)
            .allowableValues(KRYO_SERIALIZER, JSON_SERIALIZER, AVRO_SERIALIZER, NO_SERIALIZER)
            .defaultValue(KRYO_SERIALIZER.getValue())
            .build();

    public static final PropertyDescriptor OUTPUT_SERIALIZER = new PropertyDescriptor.Builder()
            .name("kafka.output.topics.serializer")
            .description("")
            .required(false)
            .addValidator(StandardPropertyValidators.NON_EMPTY_VALIDATOR)
            .allowableValues(KRYO_SERIALIZER, JSON_SERIALIZER, AVRO_SERIALIZER, NO_SERIALIZER)
            .defaultValue(KRYO_SERIALIZER.getValue())
            .build();

    public static final PropertyDescriptor ERROR_SERIALIZER = new PropertyDescriptor.Builder()
            .name("kafka.error.topics.serializer")
            .description("")
            .required(false)
            .addValidator(StandardPropertyValidators.NON_EMPTY_VALIDATOR)
            .defaultValue(JSON_SERIALIZER.getValue())
            .allowableValues(KRYO_SERIALIZER, JSON_SERIALIZER, AVRO_SERIALIZER, NO_SERIALIZER)
            .build();

    public static final PropertyDescriptor METRICS_TOPIC = new PropertyDescriptor.Builder()
            .name("kafka.metrics.topic")
            .description("a topic to send metrics of processing. no output if not set")
            .required(false)
            .addValidator(StandardPropertyValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor KAFKA_TOPIC_AUTOCREATE = new PropertyDescriptor.Builder()
            .name("kafka.topic.autoCreate")
            .description("define wether a topic should be created automatically if not already exists")
            .required(false)
            .addValidator(StandardPropertyValidators.BOOLEAN_VALIDATOR)
            .defaultValue("true")
            .build();

    public static final PropertyDescriptor KAFKA_TOPIC_DEFAULT_PARTITIONS = new PropertyDescriptor.Builder()
            .name("kafka.topic.default.partitions")
            .description("if autoCreate is set to true, this will set the number of partition at topic creation time")
            .required(false)
            .addValidator(StandardPropertyValidators.INTEGER_VALIDATOR)
            .defaultValue("8")
            .build();

    public static final PropertyDescriptor KAFKA_TOPIC_DEFAULT_REPLICATION_FACTOR = new PropertyDescriptor.Builder()
            .name("kafka.topic.default.replicationFactor")
            .description("if autoCreate is set to true, this will set the number of replica for each partition at topic creation time")
            .required(false)
            .addValidator(StandardPropertyValidators.INTEGER_VALIDATOR)
            .defaultValue("2")
            .build();

    public static final PropertyDescriptor KAFKA_METADATA_BROKER_LIST = new PropertyDescriptor.Builder()
            .name("kafka.metadata.broker.list")
            .description("")
            .required(true)
            .addValidator(StandardPropertyValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("sandbox:9092")
            .build();

    public static final PropertyDescriptor KAFKA_ZOOKEEPER_QUORUM = new PropertyDescriptor.Builder()
            .name("kafka.zookeeper.quorum")
            .description("")
            .required(true)
            .addValidator(StandardPropertyValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("sandbox:2181")
            .build();

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(ERROR_TOPICS);
        descriptors.add(INPUT_TOPICS);
        descriptors.add(OUTPUT_TOPICS);
        descriptors.add(METRICS_TOPIC);
        descriptors.add(AVRO_INPUT_SCHEMA);
        descriptors.add(AVRO_OUTPUT_SCHEMA);
        descriptors.add(INPUT_SERIALIZER);
        descriptors.add(OUTPUT_SERIALIZER);
        descriptors.add(ERROR_SERIALIZER);
        descriptors.add(KAFKA_TOPIC_AUTOCREATE);
        descriptors.add(KAFKA_TOPIC_DEFAULT_PARTITIONS);
        descriptors.add(KAFKA_TOPIC_DEFAULT_REPLICATION_FACTOR);
        descriptors.add(KAFKA_METADATA_BROKER_LIST);
        descriptors.add(KAFKA_ZOOKEEPER_QUORUM);

        return Collections.unmodifiableList(descriptors);
    }

    @Override
    public Collection<Record> process(ComponentContext context, Collection<Record> records) {
        return Collections.emptyList();
    }
}
