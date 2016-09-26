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

import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.Field;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class AvroRecordSerializer implements RecordSerializer {

    private Schema schema;

    public AvroRecordSerializer(Schema schema) {
        this.schema = schema;
    }

    protected static final byte MAGIC_BYTE = 0x0;
    protected static final int idSize = 4;


    public void serialize(OutputStream out, Record record) throws RecordSerializationException {

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


          //  Object fieldValue = avroRecord.getField(0);


            if (avroRecord instanceof org.apache.avro.util.Utf8) {
                list.add( avroRecord.toString());
            }else {
                list.add(avroRecord);
            }


        }
        return list;
    }

    public Record deserialize(InputStream in) throws RecordSerializationException {
        try {

            Decoder decoder = DecoderFactory.get().binaryDecoder(in, null);
            DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
            GenericRecord genericRecord = datumReader.read(null, decoder);


            Record record = new Record(genericRecord.get(Record.RECORD_TYPE).toString());


            for (final Schema.Field schemaField : schema.getFields()) {

                String fieldName = schemaField.name();
                Object fieldValue = genericRecord.get(fieldName);
                String fieldType = schemaField.schema().getType().getName();

                if (Objects.equals(fieldName, Record.RECORD_ID)) {
                    record.setId(fieldValue.toString());
                } else if (!Objects.equals(fieldName, Record.RECORD_TYPE)) {
                    if (fieldValue instanceof org.apache.avro.util.Utf8) {
                        record.setField(fieldName, fieldType, fieldValue.toString());
                    } else if (fieldValue instanceof GenericData.Array) {
                        GenericData.Array avroArray = (GenericData.Array) fieldValue;
                        List<Object> list = new ArrayList<>();
                        record.setField(fieldName, fieldType, list);
                        copyArray(avroArray, list);
                    } else {
                        record.setField(fieldName, fieldType, fieldValue);
                    }

                }
            }



            /*
            * for (  String fieldName : avroRecord.keySet()) {
    Object value=avroRecord.getField(fieldName);
    if (value == null) {
      continue;
    }
    if (value instanceof GenericRecord) {
      record.setField(fieldName,copyRecord((GenericRecord)value,new HashMap<String,Object>()));
    }
 else     if (value instanceof GenericArray) {
      GenericArray avroArray=(GenericArray)value;
      List<Map<String,Object>> list=new ArrayList<Map<String,Object>>((int)avroArray.size());
      record.setField(fieldName,list);
      copyArray(avroArray,list);
    }
 else     if (value instanceof Utf8) {
      record.setField(fieldName,((Utf8)value).toString());
    }
 else {
      record.setField(fieldName,value);
    }
  }*/

            return record;
        } catch (Throwable t) {
            t.printStackTrace();
            throw new RecordSerializationException(t.getMessage(), t.getCause());
        }
    }
}