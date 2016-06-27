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

package com.hurence.logisland.event.serializer;

import com.hurence.logisland.event.Event;
import com.hurence.logisland.event.EventField;
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

public class EventAvroSerializer implements EventSerializer {

    private Schema schema;

    public EventAvroSerializer(Schema schema) {
        this.schema = schema;
    }

    protected static final byte MAGIC_BYTE = 0x0;
    protected static final int idSize = 4;


    public void serialize(OutputStream out, Event event) throws EventSerdeException {

        try {
            /**
             * convert the logIsland Event to an Avro GenericRecord
             */
            GenericRecord eventRecord = new GenericData.Record(schema);
            for (Map.Entry<String, EventField> entry : event.entrySet()) {
                // retrieve event field
                String key = entry.getKey();
                EventField field = entry.getValue();
                Object value = field.getValue();

                // dump event field as record attribute
                eventRecord.put(key, value);
            }
            eventRecord.put("_type", event.getType());
            eventRecord.put("_id", event.getId());

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
            throw new EventSerdeException("Error serializing Avro message", e);
        }
    }


    public static List<Object> copyArray(GenericData.Array<Object> avroArray, List<Object> list) {
        for (Object avroRecord : avroArray) {


          //  Object fieldValue = avroRecord.get(0);


            if (avroRecord instanceof org.apache.avro.util.Utf8) {
                list.add( avroRecord.toString());
            }else {
                list.add(avroRecord);
            }


        }
        return list;
    }

    public Event deserialize(InputStream in) throws EventSerdeException {
        try {

            Decoder decoder = DecoderFactory.get().binaryDecoder(in, null);
            DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
            GenericRecord genericRecord = datumReader.read(null, decoder);


            Event event = new Event(genericRecord.get("_type").toString());


            for (final Schema.Field schemaField : schema.getFields()) {

                String fieldName = schemaField.name();
                Object fieldValue = genericRecord.get(fieldName);
                String fieldType = schemaField.schema().getType().getName();

                if (Objects.equals(fieldName, "_id")) {
                    event.setId(fieldValue.toString());
                } else if (!Objects.equals(fieldName, "_type")) {
                    if (fieldValue instanceof org.apache.avro.util.Utf8) {
                        event.put(fieldName, fieldType, fieldValue.toString());
                    } else if (fieldValue instanceof GenericData.Array) {
                        GenericData.Array avroArray = (GenericData.Array) fieldValue;
                        List<Object> list = new ArrayList<>();
                        event.put(fieldName, fieldType, list);
                        copyArray(avroArray, list);
                    } else {
                        event.put(fieldName, fieldType, fieldValue);
                    }

                }
            }



            /*
            * for (  String fieldName : avroRecord.keySet()) {
    Object value=avroRecord.get(fieldName);
    if (value == null) {
      continue;
    }
    if (value instanceof GenericRecord) {
      record.put(fieldName,copyRecord((GenericRecord)value,new HashMap<String,Object>()));
    }
 else     if (value instanceof GenericArray) {
      GenericArray avroArray=(GenericArray)value;
      List<Map<String,Object>> list=new ArrayList<Map<String,Object>>((int)avroArray.size());
      record.put(fieldName,list);
      copyArray(avroArray,list);
    }
 else     if (value instanceof Utf8) {
      record.put(fieldName,((Utf8)value).toString());
    }
 else {
      record.put(fieldName,value);
    }
  }*/

            return event;
        } catch (Throwable t) {
            t.printStackTrace();
            throw new EventSerdeException(t.getMessage(), t.getCause());
        }
    }
}