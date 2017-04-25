/**
 * Copyright (C) 2016 Hurence (bailet.thomas@gmail.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.hurence.logisland.record.Field;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

public class JsonSerializer implements RecordSerializer {

    private static Logger logger = LoggerFactory.getLogger(JsonSerializer.class);

    class EventSerializer extends StdSerializer<Record> {

        public EventSerializer() {
            this(null);
        }

        public EventSerializer(Class<Record> t) {
            super(t);
        }

        @Override
        public void serialize(Record record, JsonGenerator jgen, com.fasterxml.jackson.databind.SerializerProvider provider)
                throws IOException, JsonProcessingException {
            jgen.writeStartObject();
            jgen.writeStringField("id", record.getId());
            jgen.writeStringField("type", record.getType());
            jgen.writeStringField("creationDate", record.getTime().toString());

            jgen.writeObjectFieldStart("fields");
            for (Map.Entry<String, Field> entry : record.getFieldsEntrySet()) {
                // retrieve event field
                String fieldName = entry.getKey();
                Field field = entry.getValue();
                Object fieldValue = field.getRawValue();
                String fieldType = field.getType().toString();

                // dump event field as record attribute

                try {
                    switch (fieldType.toLowerCase()) {
                        case "string":
                            jgen.writeStringField(fieldName, (String) fieldValue);
                            break;
                        case "integer":
                            jgen.writeNumberField(fieldName, (int) fieldValue);
                            break;
                        case "long":
                            jgen.writeNumberField(fieldName, (long) fieldValue);
                            break;
                        case "float":
                            jgen.writeNumberField(fieldName, (float) fieldValue);
                            break;
                        case "double":
                            jgen.writeNumberField(fieldName, (double) fieldValue);
                            break;
                        case "boolean":
                            jgen.writeBooleanField(fieldName, (boolean) fieldValue);
                            break;
                        default:
                            jgen.writeObjectField(fieldName, fieldValue);
                            break;
                    }
                } catch (Exception ex) {
                    logger.debug("exception while serializing field {}", field);
                }

            }
            jgen.writeEndObject();
            jgen.writeEndObject();
        }


    }

    @Override
    public void serialize(OutputStream out, Record record) throws RecordSerializationException {

        ObjectMapper mapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addSerializer(StandardRecord.class, new EventSerializer());
        mapper.registerModule(module);

        //map json to student

        try {
            mapper.enable(SerializationFeature.INDENT_OUTPUT);
            mapper.setPropertyNamingStrategy(PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);
            String jsonString = mapper.writeValueAsString(record);

            out.write(jsonString.getBytes());
            out.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    // @TODO implements ARray deserialization
    class EventDeserializer extends StdDeserializer<Record> {

        protected EventDeserializer() {
            this(null);
        }

        protected EventDeserializer(Class<Record> t) {
            super(t);
        }

        @Override
        public Record deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
            JsonToken t = jp.getCurrentToken();

            SimpleDateFormat sdf = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy");
            sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
            String id = null;
            String type = null;
            Date creationDate = null;
            JsonToken currentToken = null;
            Map<String, Field> fields = new HashMap<>();

            boolean processingFields = false;
            while ((currentToken = jp.nextValue()) != null) {

                switch (currentToken) {

                    case START_OBJECT:
                        processingFields = true;
                        break;
                    case END_OBJECT:
                        processingFields = true;
                        break;
                    case VALUE_NUMBER_INT:
                        try {
                            fields.put(jp.getCurrentName(), new Field(jp.getCurrentName(), FieldType.INT, jp.getIntValue()));
                        } catch (JsonParseException ex) {
                            fields.put(jp.getCurrentName(), new Field(jp.getCurrentName(), FieldType.LONG, jp.getLongValue()));
                        }
                        break;

                    case VALUE_NUMBER_FLOAT:
                        try {
                            fields.put(jp.getCurrentName(), new Field(jp.getCurrentName(), FieldType.FLOAT, jp.getFloatValue()));
                        } catch (JsonParseException ex) {
                            fields.put(jp.getCurrentName(), new Field(jp.getCurrentName(), FieldType.DOUBLE, jp.getDoubleValue()));
                        }
                        break;
                    case VALUE_FALSE:
                    case VALUE_TRUE:
                        fields.put(jp.getCurrentName(), new Field(jp.getCurrentName(), FieldType.BOOLEAN, jp.getBooleanValue()));
                        break;
                    case START_ARRAY:
                        logger.info(jp.getCurrentName());
                        break;

                    case END_ARRAY:
                        break;
                    case VALUE_STRING:

                        if (jp.getCurrentName() != null) {
                            switch (jp.getCurrentName()) {
                                case "id":
                                    id = jp.getValueAsString();
                                    break;
                                case "type":
                                    type = jp.getValueAsString();
                                    break;
                                case "creationDate":
                                    try {
                                        creationDate = sdf.parse(jp.getValueAsString()); // "Thu Sep 08 12:11:08 CEST 2016\"
                                    } catch (ParseException e) {
                                        e.printStackTrace();
                                    }
                                    break;
                                default:
                                    fields.put(jp.getCurrentName(), new Field(jp.getCurrentName(), FieldType.STRING, jp.getValueAsString()));

                                    break;
                            }
                        }

                        break;
                    default:
                        break;
                }
            }

            Record record = new StandardRecord(type);
            record.setId(id);
            record.setType(type);
            record.setTime(creationDate);
            record.setFields(fields);

            return record;

        }

    }


    @Override
    public Record deserialize(InputStream in) throws RecordSerializationException {

        ObjectMapper mapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addDeserializer(Record.class, new EventDeserializer());
        mapper.registerModule(module);

        Record record = null;
        try {
            return record = mapper.readValue(in, Record.class);
        } catch (IOException e) {
            logger.error(e.toString());
            throw new RecordSerializationException("unable to deserialize record");
        }

    }
}