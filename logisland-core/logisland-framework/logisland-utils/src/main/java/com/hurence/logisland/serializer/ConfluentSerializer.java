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
/*
 Copyright 2016 Hurence

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package com.hurence.logisland.serializer;


import com.hurence.logisland.record.*;
import com.hurence.logisland.record.Record;
import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;

public class ConfluentSerializer implements RecordSerializer {
    protected Schema schema;
    protected KafkaAvroDeserializer deserializer;
    protected KafkaAvroSerializer serializer;

    private Logger logger = LoggerFactory.getLogger(ConfluentSerializer.class);

    public ConfluentSerializer(String schemaInfo){
        try {
            JSONObject obj = new JSONObject(schemaInfo);
            String schemaName = obj.getString("schemaName");
            String schemaUrl = obj.getString("schemaUrl");
            Integer schemaVersion = obj.getInt("schemaVersion");

            CachedSchemaRegistryClient schemaRegistryClient  = new CachedSchemaRegistryClient(schemaUrl, 10);

            deserializer = new KafkaAvroDeserializer(schemaRegistryClient);
            serializer = new KafkaAvroSerializer(schemaRegistryClient);
            logger.info(" schema name  " + schemaName +" type "+ schemaName.getClass().getSimpleName()  + " schema version " + schemaVersion.intValue() +  " type " + schemaVersion.getClass().getSimpleName());
            schema = schemaRegistryClient.getBySubjectAndID(schemaName,schemaVersion.intValue());
        } catch (Exception e) {
            logger.error("Error initalizing schema registry", e);
            throw new RuntimeException(e);
        }

    }
    public ConfluentSerializer(String schemaUrl,Schema schemaInput){
        try {
            CachedSchemaRegistryClient schemaRegistryClient  = new CachedSchemaRegistryClient(schemaUrl, 10);
            schemaRegistryClient.getAllSubjects().forEach(System.out::println);
            deserializer = new KafkaAvroDeserializer(schemaRegistryClient);
            serializer = new KafkaAvroSerializer(schemaRegistryClient);
            schema = schemaInput;
        } catch (Exception e) {
            logger.error("Error initalizing schema registry", e);
            throw new RuntimeException(e);
        }

    }


    @Override
    public void serialize(OutputStream objectDataOutput, Record record) throws RecordSerializationException {
        throw new  RecordSerializationException(" Serialization not implemented");
    }

    @Override
    public Record deserialize(InputStream objectDataInput) throws RecordSerializationException {
        byte[] bytes = new byte[0];
        try {
            bytes = IOUtils.toByteArray(objectDataInput);
        } catch (IOException e) {
            e.printStackTrace();
        }
        GenericRecord genericRecord = (GenericRecord) deserializer.deserialize(schema.getFullName(), bytes,schema);
        Record record = new StandardRecord("record");
        readAvro(record,genericRecord);
        return record;
    }

    /**
     * Converts the AVRO GenericRecord to a logisland record.
     *
     * @param record the logisland record to fill.
     *
     * @param genericRecord the AVRO record to convert.
     *
     * @return a logisland record built from the specified AVRO record.
     */
    private Record readAvro(final Record record,
                            final GenericRecord genericRecord)
    {
        return readAvro(record,
                new AvroRecord()
                {
                    @Override
                    public Schema getSchema() { return genericRecord.getSchema(); }

                    @Override
                    public Object get(String fieldName) { return genericRecord.get(fieldName); }
                });
    }

    private Record readAvro(final GenericData.Record avroRecord)
    {
        return readAvro(new StandardRecord(avroRecord.getSchema().getName()),
                new AvroRecord()
                {
                    @Override
                    public Schema getSchema() { return avroRecord.getSchema(); }

                    @Override
                    public Object get(String fieldName) { return avroRecord.get(fieldName); }
                });
    }

    //*This interface provides similar accesses to GenericData.Record and GenericRecord.
     //
    private interface AvroRecord
    {
        Schema getSchema();
        Object get(String fieldName);
    }

/*
     * Returns {@code null} if the provided value is a reference to JsonProperties.NULL_VALUE;
     * the same value is returned otherwise.
     *
     * @param fieldValue the value to check.
     *
     * @return {@code null} if the provided value is a reference to JsonProperties.NULL_VALUE;
     *         the same value otherwise.** //
*/
    private Object handleJsonNull(Object fieldValue)
    {
        if ( fieldValue == JsonProperties.NULL_VALUE )
        {
            fieldValue = null;
        }
        return fieldValue;
    }

    /*
     * Converts the AVRO record to a logisland record.
     *
     * @param record the logisland record to fill.
     *
     * @param avroRecord the AVRO record to convert.
     *
     * @return a logisland record built from the specified AVRO record.
     */
    private Record readAvro(final Record record,
                            final AvroRecord avroRecord)
    {
        final Schema avroSchema = avroRecord.getSchema();
        for (final Schema.Field schemaField : avroSchema.getFields())
        {
            final String fieldName = schemaField.name();


            Object fieldValue = avroRecord.get(fieldName);
            if ( fieldValue == null )
            {
                fieldValue = schemaField.defaultVal();
            }
            fieldValue = handleJsonNull(fieldValue);

            Schema.Type type = schemaField.schema().getType();

            if ( type == Schema.Type.UNION )
            {
                // Handle optional value
                for(final Schema subSchema: schemaField.schema().getTypes())
                {
                    if ( subSchema.getType() == Schema.Type.NULL )
                    {
                        continue;
                    }
                    type = subSchema.getType();
                }
            }

            switch (type)
            {
                case ARRAY:
                    record.setField(fieldName, FieldType.ARRAY, fieldValue!=null?readAvro((GenericData.Array)fieldValue):null);
                    break;

                case RECORD:
                    record.setField(fieldName, FieldType.RECORD, fieldValue!=null?readAvro((GenericData.Record)fieldValue):null);
                    break;

                case BOOLEAN:
                    // Prevent simple type with the null value
                    if ( fieldValue != null )
                    {
                        record.setField(fieldName, FieldType.BOOLEAN, fieldValue);
                    }
                    break;

                case BYTES:
                    // Prevent simple type with the null value
                    if ( fieldValue != null )
                    {
                        record.setField(fieldName, FieldType.BYTES, fieldValue);
                    }
                    break;

                case DOUBLE:
                    // Prevent simple type with the null value
                    if ( fieldValue != null )
                    {
                        record.setField(fieldName, FieldType.DOUBLE, fieldValue);
                    }
                    break;

                case FLOAT:
                    // Prevent simple type with the null value
                    if ( fieldValue != null )
                    {
                        record.setField(fieldName, FieldType.FLOAT, fieldValue);
                    }
                    break;

                case INT:
                    // Prevent simple type with the null value
                    if ( fieldValue != null )
                    {
                        record.setField(fieldName, FieldType.INT, fieldValue);
                    }
                    break;

                case LONG:
                    // Prevent simple type with the null value
                    if ( fieldValue != null )
                    {
                        record.setField(fieldName, FieldType.LONG, fieldValue);
                    }
                    break;

                case STRING:
                    record.setField(fieldName, FieldType.STRING, fieldValue!=null?fieldValue.toString():null);
                    break;

                case NULL:
                    record.setField(fieldName, FieldType.STRING, null);
                    break;

                case ENUM:
                    break;
                case FIXED:
                    break;
                case MAP:
                    break;
                case UNION:
                    break;
                default:
                    throw new UnsupportedOperationException("No support for AVRO type " + type);

            }
        }

        return record;
    }

    /**
     * Returns a List instance of objects filled from the provided AVRO array.
     *
     * @param avroArray the AVRO array that contains the values to convert.
     *
     * @return a List instance of objects filled from the provided AVRO array.
     */
    private List readAvro(final GenericData.Array avroArray)
    {
        final Schema.Type type = avroArray.getSchema().getElementType().getType();

        final List result = new ArrayList(avroArray.size());

        for(Object item : avroArray)
        {
            item = handleJsonNull(item);
            Object value = null;
            switch(type)
            {
                case ARRAY:
                    value = item!=null?readAvro((GenericData.Array)item):null;
                    break;

                case RECORD:
                    value = item!=null?readAvro((GenericData.Record)item):null;
                    break;

                case BOOLEAN:
                case BYTES:
                case DOUBLE:
                case FLOAT:
                case INT:
                case LONG:
                    // Prevent simple type with the null value
                    if ( item == null )
                    {
                        continue;
                    }
                    break;

                case STRING:
                    value = item!=null?item.toString():null;
                    break;

                case NULL:
                    value = null;
                    break;

                case ENUM:
                case FIXED:
                case MAP:
                case UNION:

                default:
                    throw new UnsupportedOperationException("No support for AVRO type " + type);
            }
            result.add(value);
        }

        return result;
    }



}