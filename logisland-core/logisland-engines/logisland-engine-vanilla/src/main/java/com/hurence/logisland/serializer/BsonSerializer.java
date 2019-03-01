/*
 * Copyright (C) 2019 Hurence (support@hurence.com)
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
 *
 */

package com.hurence.logisland.serializer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hurence.logisland.record.Field;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import de.undercouch.bson4jackson.BsonFactory;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;

public class BsonSerializer implements RecordSerializer {

    private final ObjectMapper objectMapper = new ObjectMapper(new BsonFactory());

    @Override
    public void serialize(OutputStream objectDataOutput, Record record) throws RecordSerializationException {
        try {
            objectMapper.writeValue(objectDataOutput,
                    record.getAllFieldsSorted().stream().collect(LinkedHashMap::new,
                            (m, v) -> m.put(v.getName(), v.getRawValue()), HashMap::putAll));
        } catch (Exception e) {
            throw new RecordSerializationException("Unable to serialize record", e);
        }
    }

    @Override
    public Record deserialize(InputStream objectDataInput) throws RecordSerializationException {
        try {
            Map<String, Object> tmp = objectMapper.readValue(objectDataInput,
                    new TypeReference<Map<String, Object>>() {
                    });
            Record ret = new StandardRecord();
            tmp.forEach((k, v) -> ret.setField(doDeserializeField(k, v)));
            return ret;
        } catch (Exception e) {
            throw new RecordSerializationException("Unable to deserialize record", e);
        }
    }

    private Field doDeserializeField(String name, Object value) {
        Field ret;
        if (value == null) {
            ret = new Field(name, FieldType.NULL, null);
        } else if (value instanceof List) {
            ret = new Field(name, FieldType.ARRAY, ((List) value).toArray());
        } else if (value instanceof Map) {
            ret = new Field(name, FieldType.MAP, value);
        } else {
            if (value instanceof Boolean) {
                ret = new Field(name, FieldType.BOOLEAN, value);
            } else if (value instanceof Float) {
                ret = new Field(name, FieldType.FLOAT, value);
            } else if (value instanceof Long) {
                ret = new Field(name, FieldType.LONG, value);
            } else if (value instanceof Double) {
                ret = new Field(name, FieldType.DOUBLE, value);
            } else if (value instanceof Number) {
                ret = new Field(name, FieldType.INT, value);
            } else if (value instanceof Date) {
                ret = new Field(name, FieldType.DATETIME, value);
            } else {
                ret = new Field(name, FieldType.STRING, value);
            }
        }
        return ret;
    }
}
