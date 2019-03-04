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
import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class AvroSerializer implements RecordSerializer {

    private final Schema schema;
    private Logger logger = LoggerFactory.getLogger(AvroSerializer.class);

    public AvroSerializer(final Schema schema) {
        this.schema = schema;
    }

    public AvroSerializer(final String strSchema) {
        final Schema.Parser parser = new Schema.Parser();
        try {
            schema = parser.parse(strSchema);
        } catch (Exception e) {
            throw new RecordSerializationException("unable to create serializer", e);
        }
    }

    public AvroSerializer(final InputStream inputStream) {
        assert inputStream != null;
        final Schema.Parser parser = new Schema.Parser();
        try {
             schema = parser.parse(inputStream);
        } catch (IOException e) {
            throw new RecordSerializationException("unable to create serializer", e);
        }
    }

    protected static final byte MAGIC_BYTE = 0x0;
    protected static final int idSize = 4;

    @Override
    public void serialize(final OutputStream out, final Record record) throws RecordSerializationException {

        try {
            /**
             * convert the logIsland Event to an Avro GenericRecord
             */
            GenericRecord eventRecord = new GenericData.Record(schema);
            for (Map.Entry<String, Field> entry : record.getFieldsEntrySet()) {
                // retrieve event field
                String key = entry.getKey();
                Field field = entry.getValue();
                Object value = field.getRawValue();

                // dump event field as record attribute
                eventRecord.put(key, value);
            }

            /**
             *
             */
            DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
            Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            datumWriter.write(eventRecord, encoder);
            encoder.flush();

            out.flush();
        } catch (IOException | RuntimeException e) {
            // avro serialization can throw AvroRuntimeException, NullPointerException,
            // ClassCastException, etc
            throw new RecordSerializationException("Error serializing Avro message", e);
        }
    }


    public static List<Object> copyArray(GenericData.Array<Object> avroArray, List<Object> list) {
        for (Object avroRecord : avroArray) {
            if (avroRecord instanceof org.apache.avro.util.Utf8) {
                list.add(avroRecord.toString());
            } else {
                list.add(avroRecord);
            }
        }
        return list;
    }

    public Record deserialize(final InputStream in) throws RecordSerializationException {
        try {
            Decoder decoder = DecoderFactory.get().binaryDecoder(in, null);
            DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
            GenericRecord genericRecord = datumReader.read(null, decoder);

            Record record = new StandardRecord(genericRecord.get(FieldDictionary.RECORD_TYPE).toString());
            this.readAvro(record, genericRecord);

            return record;
        } catch (Throwable t) {
            logger.error("error while deserializing", t);
            throw new RecordSerializationException(t.getMessage(), t.getCause());
        }
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

    /**
     * This interface provides similar accesses to GenericData.Record and GenericRecord.
     */
    private interface AvroRecord
    {
        Schema getSchema();
        Object get(String fieldName);
    }

    /**
     * Converts the AVRO GenericRecord to a new created logisland record.
     *
     * @param avroRecord the AVRO record to convert.
     *
     * @return a logisland record built from the specified AVRO record.
     */
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

    /**
     * Returns {@code null} if the provided value is a reference to JsonProperties.NULL_VALUE;
     * the same value is returned otherwise.
     *
     * @param fieldValue the value to check.
     *
     * @return {@code null} if the provided value is a reference to JsonProperties.NULL_VALUE;
     *         the same value otherwise.
     */
    private Object handleJsonNull(Object fieldValue)
    {
        if ( fieldValue == JsonProperties.NULL_VALUE )
        {
            fieldValue = null;
        }
        return fieldValue;
    }

    /**
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
                case FIXED:
                case MAP:
                case UNION:

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