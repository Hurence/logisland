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

package com.hurence.logisland.serializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hurence.logisland.record.Field;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Array;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Basic but complete Json {@link RecordSerializer}.
 * It support nested object and arrays.
 * <p>
 * Useful when the input data is coming directly in JSON format from Kafka.
 *
 * @author amarziali
 */
public class ExtendedJsonSerializer implements RecordSerializer {


    private static Logger logger = LoggerFactory.getLogger(com.hurence.logisland.serializer.JsonSerializer.class);


    @Override
    public void serialize(OutputStream out, Record record) throws RecordSerializationException {
        final ObjectMapper mapper = new ObjectMapper();
        try {

            Map<String, Object> json = record.getAllFieldsSorted().stream().collect(Collectors.toMap(Field::getName, Field::getRawValue,
                    (u, v) -> {
                        throw new IllegalStateException(String.format("Duplicate key %s", u));
                    },
                    LinkedHashMap::new));
            json.put("id", record.getId());
            if (record.getTime() != null) {
                json.put("creationDate", record.getTime().getTime());
            }
            json.put("type", record.getType());

            mapper.writeValue(out, json);
            out.flush();
        } catch (IOException e) {
            logger.debug(e.toString());
        }

    }


    private Field doDeserializeField(String name, Object value) {
        Field ret;
        if (value == null) {
            ret = new Field(name, FieldType.NULL, null);
        } else if (value.getClass().isArray()) {
            ArrayList tmp = new ArrayList();
            for (int i = 0; i < Array.getLength(value); i++) {
                tmp.add(doDeserializeField("", Array.get(value, i)).getRawValue());
            }
            ret = new Field(name, FieldType.ARRAY, tmp);
        } else if (value instanceof Map) {
            ret = new Field(name, FieldType.MAP, value);
        } else {
            if (value instanceof Boolean) {
                ret = new Field(name, FieldType.BOOLEAN, value);
            } else if (value instanceof Float) {
                ret = new Field(name, FieldType.FLOAT, value);
            } else if (value instanceof Long) {
                ret = new Field(name, FieldType.LONG, value);
            } else if (value instanceof Number) {
                ret = new Field(name, FieldType.INT, value);
            } else if (value instanceof Double) {
                ret = new Field(name, FieldType.DOUBLE, value);
            } else if (value instanceof Date) {
                ret = new Field(name, FieldType.LONG, ((Date) value).getTime());
            } else {
                ret = new Field(name, FieldType.STRING, value);
            }
        }
        return ret;
    }


    @Override
    public Record deserialize(InputStream in) throws RecordSerializationException {
        final ObjectMapper mapper = new ObjectMapper();
        try {
            Map<String, Object> map = mapper.readValue(in, mapper.getTypeFactory().constructMapType(LinkedHashMap.class, String.class, Object.class));
            Record record = new StandardRecord();
            if (map.containsKey("type")) {
                record.setType((String) map.get("type"));
                map.remove("type");
            }
            if (map.containsKey("id")) {
                record.setId((String) map.get("id"));
                map.remove("id");
            }
            if (map.containsKey("creationDate")) {
                Object tmp = map.get("creationDate");
                Long date = null;
                if (tmp instanceof Long) {
                    date = (Long) tmp;
                } else if (tmp != null) {
                    try {
                        Date d = DateFormat.getInstance().parse(tmp.toString());
                        if (d != null) {
                            date = d.getTime();
                        }
                    } catch (Exception e) {
                        //just do not serialize the information for now
                        logger.warn("Record date cannot be serialized", e);
                    }
                }
                if (date != null) {
                    record.setTime(date);
                }
                map.remove("creationDate");
            }
            map.forEach((k, v) -> record.setField(doDeserializeField(k, v)));
            return record;

        } catch (IOException e) {
            logger.error(e.toString());
            throw new RecordSerializationException("unable to deserialize record");
        }

    }
}
