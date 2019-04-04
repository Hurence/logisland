/**
 * Copyright (C) 2016 Hurence (support@hurence.com)
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
package com.hurence.logisland.engine.vanilla.stream.kafka;

import com.hurence.logisland.component.AllowableValue;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.serializer.*;
import com.hurence.logisland.validator.StandardValidators;

public interface StreamProperties {

    AllowableValue AVRO_SERIALIZER = new AllowableValue(AvroSerializer.class.getName(),
            "avro serialization", "serialize events as avro blocs");
    AllowableValue JSON_SERIALIZER = new AllowableValue(JsonSerializer.class.getName(),
            "json serialization", "serialize events as json blocs");
    AllowableValue EXTENDED_JSON_SERIALIZER = new AllowableValue(ExtendedJsonSerializer.class.getName(),
            "extended json serialization", "serialize events as json blocs supporting nested objects/arrays");
    AllowableValue KRYO_SERIALIZER = new AllowableValue(KryoSerializer.class.getName(),
            "kryo serialization", "serialize events as binary blocs");
    AllowableValue STRING_SERIALIZER = new AllowableValue(StringSerializer.class.getName(),
            "string serialization", "serialize events as string");
    AllowableValue BYTESARRAY_SERIALIZER = new AllowableValue(BytesArraySerializer.class.getName(),
            "byte array serialization", "serialize events as byte arrays");
    AllowableValue KURA_PROTOCOL_BUFFER_SERIALIZER = new AllowableValue(KuraProtobufSerializer.class.getName(),
            "Kura Protobuf serialization", "serialize events as Kura protocol buffer");
    AllowableValue NO_SERIALIZER = new AllowableValue("none", "no serialization", "send events as bytes");

    PropertyDescriptor PROPERTY_APPLICATION_ID = new PropertyDescriptor.Builder()
            .name("application.id")
            .description("The application ID")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();

    PropertyDescriptor PROPERTY_BOOTSTRAP_SERVERS = new PropertyDescriptor.Builder()
            .name("bootstrap.servers")
            .description("List of kafka nodes to connect to")
            .addValidator(StandardValidators.COMMA_SEPARATED_LIST_VALIDATOR)
            .required(true)
            .build();

    AllowableValue LATEST_OFFSET = new AllowableValue("latest", "latest", "the offset to the latest offset");
    AllowableValue EARLIEST_OFFSET = new AllowableValue("earliest", "earliest offset", "the offset to the earliest offset");
    AllowableValue NONE_OFFSET = new AllowableValue("none", "none offset", "the latest saved  offset");


    PropertyDescriptor KAFKA_MANUAL_OFFSET_RESET = new PropertyDescriptor.Builder()
            .name("kafka.manual.offset.reset")
            .description("What to do when there is no initial offset in Kafka or if the current offset does not exist " +
                    "any more on the server (e.g. because that data has been deleted):\n" +
                    "earliest: automatically reset the offset to the earliest offset\n" +
                    "latest: automatically reset the offset to the latest offset\n" +
                    "none: throw exception to the consumer if no previous offset is found for the consumer's group\n" +
                    "anything else: throw exception to the consumer.")
            .required(false)
            .allowableValues(LATEST_OFFSET, EARLIEST_OFFSET, NONE_OFFSET)
            .defaultValue(EARLIEST_OFFSET.getValue())
            .build();

    PropertyDescriptor READ_TOPICS = new PropertyDescriptor.Builder()
            .name("read.topics")
            .description("The input path for any topic to be read from")
            .defaultValue("")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();

    PropertyDescriptor READ_TOPICS_SERIALIZER = new PropertyDescriptor.Builder()
            .name("read.topics.serializer")
            .description("The serializer to use")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .allowableValues(KRYO_SERIALIZER, JSON_SERIALIZER, EXTENDED_JSON_SERIALIZER, AVRO_SERIALIZER, BYTESARRAY_SERIALIZER, STRING_SERIALIZER, NO_SERIALIZER, KURA_PROTOCOL_BUFFER_SERIALIZER)
            .defaultValue(NO_SERIALIZER.getValue())
            .build();


    PropertyDescriptor WRITE_TOPICS = new PropertyDescriptor.Builder()
            .name("write.topics")
            .description("The input path for any topic to be written to")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("")
            .required(true)
            .build();

    PropertyDescriptor WRITE_TOPICS_SERIALIZER = new PropertyDescriptor.Builder()
            .name("write.topics.serializer")
            .description("The serializer to use")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .allowableValues(KRYO_SERIALIZER, JSON_SERIALIZER, EXTENDED_JSON_SERIALIZER, AVRO_SERIALIZER, BYTESARRAY_SERIALIZER, STRING_SERIALIZER, NO_SERIALIZER, KURA_PROTOCOL_BUFFER_SERIALIZER)
            .defaultValue(NO_SERIALIZER.getValue())
            .build();


    PropertyDescriptor AVRO_INPUT_SCHEMA = new PropertyDescriptor.Builder()
            .name("avro.input.schema")
            .description("The avro schema definition")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    PropertyDescriptor AVRO_OUTPUT_SCHEMA = new PropertyDescriptor.Builder()
            .name("avro.output.schema")
            .description("The avro schema definition for the output serialization")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

}
