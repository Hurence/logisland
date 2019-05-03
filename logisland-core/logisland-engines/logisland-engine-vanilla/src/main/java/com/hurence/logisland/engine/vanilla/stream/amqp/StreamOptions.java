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
package com.hurence.logisland.engine.vanilla.stream.amqp;

import com.hurence.logisland.component.AllowableValue;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.serializer.*;
import com.hurence.logisland.validator.StandardValidators;

public interface StreamOptions {

    PropertyDescriptor CONTAINER_ID = new PropertyDescriptor.Builder()
            .name("container.id")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .description("AMQP container ID")
            .build();

    PropertyDescriptor CONNECTION_HOST = new PropertyDescriptor.Builder()
            .name("connection.host")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .description("Connection host name")
            .build();


    PropertyDescriptor CONNECTION_PORT = new PropertyDescriptor.Builder()
            .name("connection.port")
            .required(false)
            .defaultValue("5672")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .description("Connection port")
            .build();

    PropertyDescriptor CONNECTION_RECONNECT_INITIAL_DELAY = new PropertyDescriptor.Builder()
            .name("connection.reconnect.initial.delay")
            .required(false)
            .defaultValue("1000")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .description("Initial reconnection delay in milliseconds")
            .build();

    PropertyDescriptor CONNECTION_RECONNECT_MAX_DELAY = new PropertyDescriptor.Builder()
            .name("connection.reconnect.max.delay")
            .required(false)
            .defaultValue("30000")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .description("Maximum reconnection delay in milliseconds")
            .build();

    PropertyDescriptor CONNECTION_RECONNECT_BACKOFF = new PropertyDescriptor.Builder()
            .name("connection.reconnect.backoff")
            .required(false)
            .defaultValue("2.0")
            .addValidator(StandardValidators.DOUBLE_VALIDATOR)
            .description("Reconnection delay linear backoff")
            .build();

    PropertyDescriptor CONNECTION_AUTH_USERNAME = new PropertyDescriptor.Builder()
            .name("connection.auth.user")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .description("Connection authenticated user name")
            .build();

    PropertyDescriptor CONNECTION_AUTH_PASSWORD = new PropertyDescriptor.Builder()
            .name("connection.auth.password")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .description("Connection authenticated password")
            .build();


    PropertyDescriptor CONNECTION_AUTH_CA_CERT = new PropertyDescriptor.Builder()
            .name("connection.auth.ca.cert")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .description("Connection TLS CA cert (PEM file path)")
            .build();

    PropertyDescriptor CONNECTION_AUTH_TLS_CERT = new PropertyDescriptor.Builder()
            .name("connection.auth.tls.cert")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .description("Connection TLS public certificate (PEM file path)")
            .build();

    PropertyDescriptor CONNECTION_AUTH_TLS_KEY = new PropertyDescriptor.Builder()
            .name("connection.auth.tls.key")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .description("Connection TLS private key (PEM file path)")
            .build();

    PropertyDescriptor LINK_CREDITS = new PropertyDescriptor.Builder()
            .name("link.credits")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("1024")
            .description("Flow control. How many credits for this links. Higher means higher prefetch (prebuffered number of messages")
            .build();



    AllowableValue AVRO_SERIALIZER = new AllowableValue(AvroSerializer.class.getName(),
            "avro serialization", "serialize events as avro blocs");
    AllowableValue JSON_SERIALIZER = new AllowableValue(JsonSerializer.class.getName(),
            "json serialization", "serialize events as json blocs");
    AllowableValue BSON_SERIALIZER = new AllowableValue(BsonSerializer.class.getName(),
            "bson serialization", "serialize events as bson");
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


    PropertyDescriptor READ_TOPIC = new PropertyDescriptor.Builder()
            .name("read.topic")
            .description("The input path for any topic to be read from")
            .defaultValue("")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();

    PropertyDescriptor READ_TOPIC_SERIALIZER = new PropertyDescriptor.Builder()
            .name("read.topic.serializer")
            .description("The serializer to use")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .allowableValues(BSON_SERIALIZER, KRYO_SERIALIZER, JSON_SERIALIZER, EXTENDED_JSON_SERIALIZER, AVRO_SERIALIZER, BYTESARRAY_SERIALIZER, STRING_SERIALIZER, NO_SERIALIZER, KURA_PROTOCOL_BUFFER_SERIALIZER)
            .defaultValue(NO_SERIALIZER.getValue())
            .build();


    PropertyDescriptor WRITE_TOPIC = new PropertyDescriptor.Builder()
            .name("write.topic")
            .description("The input path for any topic to be written to")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("")
            .required(true)
            .build();

    PropertyDescriptor WRITE_TOPIC_CONTENT_TYPE = new PropertyDescriptor.Builder()
            .name("write.topic.content.type")
            .description("The content type to set in the output message")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .build();

    PropertyDescriptor WRITE_TOPIC_SERIALIZER = new PropertyDescriptor.Builder()
            .name("write.topic.serializer")
            .description("The serializer to use")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .allowableValues(BSON_SERIALIZER, KRYO_SERIALIZER, JSON_SERIALIZER, EXTENDED_JSON_SERIALIZER, AVRO_SERIALIZER, BYTESARRAY_SERIALIZER, STRING_SERIALIZER, NO_SERIALIZER, KURA_PROTOCOL_BUFFER_SERIALIZER)
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
