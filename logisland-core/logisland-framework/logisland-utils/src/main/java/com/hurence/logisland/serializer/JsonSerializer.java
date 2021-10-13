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

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.hurence.logisland.record.*;
import com.hurence.logisland.util.ListUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;

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
            jgen.writeNumberField("creationDate", record.getTime().getTime());

            jgen.writeObjectFieldStart("fields");
            for (Map.Entry<String, Field> entry : record.getFieldsEntrySet()) {
                // retrieve event field
                String fieldName = entry.getKey();
                Field field = entry.getValue();
                //  Object fieldValue = field.getRawValue();
                String fieldType = field.getType().toString();

                // dump event field as record attribute

                try {
                    switch (fieldType.toLowerCase()) {
                        case "string":
                            jgen.writeStringField(fieldName, field.asString());
                            break;
                        case "integer":
                        case "int":
                            jgen.writeNumberField(fieldName, field.asInteger());
                            break;
                        case "long":
                            jgen.writeNumberField(fieldName, field.asLong());
                            break;
                        case "float":
                            jgen.writeNumberField(fieldName, field.asFloat());
                            break;
                        case "double":
                            jgen.writeNumberField(fieldName, field.asDouble());
                            break;
                        case "boolean":
                            jgen.writeBooleanField(fieldName, field.asBoolean());
                            break;
                        case "array":
                            jgen.writeArrayFieldStart(fieldName);
                            //  jgen.writeStartArray();
                            String[] items = field.asString().split(",");
                            for (String item : items) {
                                jgen.writeString(item);
                            }
                            jgen.writeEndArray();
                            break;

                        default:
                            jgen.writeObjectField(fieldName, field.asString());
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
            logger.debug(e.toString());
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

            String id = null;
            String type = null;
            Date creationDate = null;
            JsonToken currentToken = null;
            Map<String, Field> fields = new HashMap<>();

            boolean processingFields = false;
            Map<String, List<String>> arrays = new HashMap<>();
            String currentArrayName = null;
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
                        } catch (Exception ex) {
                            // May have JsonParseException or InputCoercionException (for instance for long instead of int)
                            // This also depends on jackson version.
                            // The simplest is to catch any exception, as not sure to be exhaustive and precise handling all

                            // special case for creationDate (not a field)
                            if (jp.getCurrentName() != null && jp.getCurrentName().equals("creationDate")) {
                                try {
                                    creationDate = new Date(jp.getValueAsLong());
                                } catch (Exception e) {
                                    logger.error("error while creating a date", e);
                                }
                            } else {
                                fields.put(jp.getCurrentName(), new Field(jp.getCurrentName(), FieldType.LONG, jp.getLongValue()));
                            }
                        }
                        break;


                    case VALUE_NUMBER_FLOAT:
                        try {

                            fields.put(jp.getCurrentName(), new Field(jp.getCurrentName(), FieldType.DOUBLE, jp.getDoubleValue()));
                        } catch (Exception ex) {

                            fields.put(jp.getCurrentName(), new Field(jp.getCurrentName(), FieldType.FLOAT, jp.getFloatValue()));
                        }
                        break;
                    case VALUE_FALSE:
                    case VALUE_TRUE:
                        fields.put(jp.getCurrentName(), new Field(jp.getCurrentName(), FieldType.BOOLEAN, jp.getBooleanValue()));
                        break;
                    case START_ARRAY:

                        currentArrayName = jp.getCurrentName();
                        arrays.put(currentArrayName, new ArrayList<>());
                        break;

                    case END_ARRAY:

                        String itemString = ListUtils.mkString(arrays.get(currentArrayName), String::toString, ", ");

                            fields.put(currentArrayName, new Field(jp.getCurrentName(), FieldType.ARRAY, itemString));

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
                         /*   case "creationDate":
                                try {
                                    creationDate = new Date(jp.getValueAsLong());
                                } catch (Exception e) {
                                    logger.error("error while creating a date", e);
                                }
                                break;*/
                                default:
                                    fields.put(jp.getCurrentName(), new Field(jp.getCurrentName(), FieldType.STRING, jp.getValueAsString()));

                                    break;
                            }
                        } else {
                            arrays.get(currentArrayName).add(jp.getValueAsString());


                        }

                        break;
                    default:
                        break;
                }
            }

            Record record = new StandardRecord();
            if (id != null) {
                record.setId(id);
            }
            if (type != null) {
                record.setType(type);
            }
            if (creationDate != null) {
                record.setTime(creationDate);
            }
            record.addFields(fields);

            return record;

        }

    }


    @Override
    public Record deserialize(InputStream in) throws RecordSerializationException {

        ObjectMapper mapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addDeserializer(Record.class, new EventDeserializer());
        mapper.registerModule(module);
        try {
            return mapper.readValue(in, Record.class);
        } catch (IOException e) {
            throw new RecordSerializationException("unable to deserialize record", e);
        }

    }
}