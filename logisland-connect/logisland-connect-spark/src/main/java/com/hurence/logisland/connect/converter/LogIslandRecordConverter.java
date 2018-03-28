/*
 *  * Copyright (C) 2018 Hurence (support@hurence.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.hurence.logisland.connect.converter;

import com.hurence.logisland.record.*;
import com.hurence.logisland.serializer.RecordSerializer;
import com.hurence.logisland.serializer.SerializerProvider;
import com.hurence.logisland.stream.StreamProperties;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class LogIslandRecordConverter implements Converter {

    /**
     * Record serializer class (instance of {@link com.hurence.logisland.serializer.RecordSerializer})
     */
    private static final String PROPERTY_RECORD_SERIALIZER = "record.serializer";
    /**
     * Avro schema to use (only apply to {@link com.hurence.logisland.serializer.AvroSerializer})
     */
    private static final String PROPERTY_AVRO_SCHEMA = "avro.schema";

    /**
     * The record type to use. If not provided {@link LogIslandRecordConverter#PROPERTY_RECORD_TYPE} will be used.
     */
    private static final String PROPERTY_RECORD_TYPE = StreamProperties.RECORD_TYPE().getName();

    /**
     * The default type for logisland {@link Record} created by this converter.
     */
    private static final String DEFAULT_RECORD_TYPE = "kafka_connect";

    private RecordSerializer recordSerializer;
    private String recordType;
    private boolean isKey;


    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        recordSerializer = SerializerProvider.getSerializer((String) configs.get(PROPERTY_RECORD_SERIALIZER), (String) configs.get(PROPERTY_AVRO_SCHEMA));
        recordType = ((Map<String, Object>) configs).getOrDefault(PROPERTY_RECORD_TYPE, DEFAULT_RECORD_TYPE).toString();
        this.isKey = isKey;
    }

    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            recordSerializer.serialize(baos,
                    new StandardRecord(recordType).setField(toFieldRecursive(FieldDictionary.RECORD_VALUE, schema, value)));
            return baos.toByteArray();
        } catch (IOException ioe) {
            throw new DataException("Unexpected IO Exception occurred while serializing data [topic " + topic + "]", ioe);
        }

    }

    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        throw new UnsupportedOperationException("Not yet implemented! Please try later on ;-)");
    }

    private Field toFieldRecursive(String name, Schema schema, Object value) {
        try {
            final Schema.Type schemaType;
            if (schema == null) {
                schemaType = ConnectSchema.schemaType(value.getClass());
                if (schemaType == null)
                    throw new DataException("Java class " + value.getClass() + " does not have corresponding schema type.");
            } else {
                schemaType = schema.type();
            }
            switch (schemaType) {
                case INT8:
                case INT16:
                case INT32:
                    return new Field(name, FieldType.INT, value);
                case INT64:
                    return new Field(name, FieldType.LONG, value);
                case FLOAT32:
                case FLOAT64:
                    return new Field(name, FieldType.FLOAT, value);
                case BOOLEAN:
                    return new Field(name, FieldType.BOOLEAN, value);
                case STRING:
                    return new Field(name, FieldType.STRING, value);
                case BYTES:
                    byte[] bytes = null;
                    if (value instanceof byte[]) {
                        bytes = (byte[]) value;
                    } else if (value instanceof ByteBuffer) {
                        bytes = ((ByteBuffer) value).array();
                    } else {
                        throw new DataException("Invalid type for bytes type: " + value.getClass());
                    }
                    return new Field(name, FieldType.BYTES, bytes);
                case ARRAY: {
                    return new Field(name, FieldType.ARRAY, value);
                }
                case MAP: {
                    return new Field(name, FieldType.MAP, value);
                }
                case STRUCT: {
                    Struct struct = (Struct) value;

                    if (struct.schema() != schema) {
                        throw new DataException("Mismatching schema.");
                    }
                    if (isKey) {
                        Map<String, Object> ret = new HashMap<>();
                        struct.schema().fields().forEach(field -> ret.put(field.name(), toFieldRecursive(field.name(), field.schema(), struct.get(field)).getRawValue()));
                        return new Field(name, FieldType.MAP, ret);
                    } else {
                        Record ret = new StandardRecord();
                        struct.schema().fields().forEach(field -> ret.setField(toFieldRecursive(field.name(), field.schema(), struct.get(field))));
                        return new Field(name, FieldType.RECORD, ret);
                    }

                }
            }

            throw new DataException("Couldn't convert " + value + " to JSON.");
        } catch (ClassCastException e) {
            throw new DataException("Invalid type for " + schema.type() + ": " + value.getClass());
        }
    }
}
