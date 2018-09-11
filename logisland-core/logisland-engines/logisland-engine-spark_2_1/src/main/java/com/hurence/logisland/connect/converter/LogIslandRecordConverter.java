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

import com.hurence.logisland.record.Field;
import com.hurence.logisland.record.*;
import com.hurence.logisland.serializer.RecordSerializer;
import com.hurence.logisland.serializer.SerializerProvider;
import com.hurence.logisland.stream.StreamProperties;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class LogIslandRecordConverter implements Converter {

    /**
     * Record serializer class (instance of {@link RecordSerializer})
     */
    public static final String PROPERTY_RECORD_SERIALIZER = "record.serializer";
    /**
     * Avro schema to use (only apply to {@link com.hurence.logisland.serializer.AvroSerializer})
     */
    public static final String PROPERTY_AVRO_SCHEMA = "avro.schema";

    /**
     * The record type to use. If not provided {@link LogIslandRecordConverter#PROPERTY_RECORD_TYPE} will be used.
     */
    public static final String PROPERTY_RECORD_TYPE = StreamProperties.RECORD_TYPE().getName();

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
                    new StandardRecord(recordType).setField(toFieldRecursive(FieldDictionary.RECORD_VALUE, schema, value, isKey)));
            return baos.toByteArray();
        } catch (IOException ioe) {
            throw new DataException("Unexpected IO Exception occurred while serializing data [topic " + topic + "]", ioe);
        }

    }

    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(value)) {
            Record r = recordSerializer.deserialize(bais);
            Schema schema = toSchemaRecursive(r);
            return new SchemaAndValue(schema, toObjectRecursive(r, schema));
        } catch (IOException ioe) {
            throw new DataException("Unexpected IO Exception occurred while serializing data [topic " + topic + "]", ioe);
        }    }


    public Object toObjectRecursive(Object o, Schema schema) {
        if (o instanceof Collection) {
            return  ((Collection<?>) o).stream().map(elem -> toObjectRecursive(elem, schema.schema()));
        }
        else if (o instanceof Map) {
            Struct ret = new Struct(schema);
            ((Map<?, ?>) o).forEach((k, v) -> ret.put(k.toString(), toObjectRecursive(o, schema.field(k.toString()).schema())));
            return ret;
        } else if (o instanceof Record) {
            Struct ret = new Struct(schema);
            ((Record) o).getAllFieldsSorted().forEach(field -> ret.put(field.getName(), toObjectRecursive(field.getRawValue(), schema.field(field.getName()).schema())));
            return ret;
        }
        return o;
    }

    private SchemaBuilder toSchemaRecursive(Object o) {
        if (o instanceof Byte) {
            return SchemaBuilder.bytes().optional();
        } else if (o instanceof Short) {
            return SchemaBuilder.int16().optional();
        } else if (o instanceof Integer) {
            return SchemaBuilder.int32().optional();

        } else if (o instanceof Long) {
            return SchemaBuilder.int64().optional();

        } else if (o instanceof Float) {
            return SchemaBuilder.float32().optional();

        } else if (o instanceof Double) {
            return SchemaBuilder.float64().optional();

        } else if (o instanceof Boolean) {
            return SchemaBuilder.bool().optional();
        } else if (o instanceof byte[]) {
            return SchemaBuilder.bytes().optional();
        } else if (o instanceof Collection) {
            return SchemaBuilder.array(toSchemaRecursive((Array.getLength(o) > 0 ? Array.get(o, 0) : null))).optional();
        } else if (o instanceof Map) {
            SchemaBuilder sb = SchemaBuilder.struct();
            ((Map<?, ?>) o).forEach((k, v) -> sb.field(k.toString(), toSchemaRecursive(v)));
            return sb.optional();
        } else if (o instanceof Record) {
            SchemaBuilder sb = SchemaBuilder.struct();
            ((Record) o).getAllFieldsSorted().forEach(field -> sb.field(field.getName(), toSchemaRecursive(field.getRawValue())));
            return sb.optional();
        }
        return SchemaBuilder.string().optional();
    }


    private Field toFieldRecursive(String name, Schema schema, Object value, boolean isKey) {
        try {
            if (value == null) {
                return new Field(name, FieldType.NULL, null);
            }
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
                    return new Field(name, FieldType.FLOAT, value);
                case FLOAT64:
                    return new Field(name, FieldType.DOUBLE, value);
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
                    return new Field(name, FieldType.ARRAY,
                            ((Collection<?>) value).stream().map(item -> {
                                Schema valueSchema = schema == null ? null : schema.valueSchema();
                                return toFieldRecursive(FieldDictionary.RECORD_VALUE, valueSchema, item, true);
                            })
                                    .map(Field::getRawValue)
                                    .collect(Collectors.toList()));
                }
                case MAP: {
                    return new Field(name, FieldType.MAP, new LinkedHashMap<>((Map) value));
                }
                case STRUCT: {
                    Struct struct = (Struct) value;

                    if (struct.schema() != schema) {
                        throw new DataException("Mismatching schema.");
                    }
                    if (isKey) {
                        Map<String, Object> ret = new HashMap<>();
                        struct.schema().fields().stream().filter(field -> !(field.schema().isOptional() && struct.get(field) == null))
                                .forEach(field -> ret.put(field.name(), toFieldRecursive(field.name(), field.schema(), struct.get(field), true).getRawValue()));
                        return new Field(name, FieldType.MAP, ret);
                    } else {
                        Record ret = new StandardRecord();
                        struct.schema().fields().stream()
                                .filter(field -> !(field.schema().isOptional() && struct.get(field) == null))
                                .forEach(field -> ret.setField(toFieldRecursive(field.name(), field.schema(), struct.get(field), true)));
                        return new Field(name, FieldType.RECORD, ret);
                    }

                }
            }
            throw new DataException("Couldn't convert " + value + " to a logisland Record.");
        } catch (ClassCastException e) {
            throw new DataException("Invalid type for " + schema.type() + ": " + value.getClass());
        }
    }
}
