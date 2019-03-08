/**
 * Copyright (C) 2016 Hurence (support@hurence.com)
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
package com.hurence.logisland.serializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hurence.logisland.record.*;
import com.hurence.logisland.util.time.DateUtil;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Array;
import java.text.DateFormat;
import java.text.ParseException;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.*;
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

    private final Schema schema;

    private transient volatile ObjectMapper mapper;

    public ExtendedJsonSerializer(Schema schema) {
        this.schema = schema;
    }

    public ExtendedJsonSerializer() {
        this.schema = null;
    }

    public void doFilter(Map<String, Object> map, String name, Object value, Schema schema) {
        final Schema.Field field = name != null ? schema.getField(name) : null;
        if (field != null || name == null) {
            if (value == null) {
                map.put(name, null);
            } else {
                final Schema currentSchema = field != null ? field.schema() : schema;
                LogicalType logicalType = currentSchema.getLogicalType();
                if (logicalType instanceof LogicalTypes.Date ||
                        logicalType instanceof LogicalTypes.TimestampMillis) {
                    if (value instanceof Number) {
                        map.put(name, new Date(((Number) value).longValue()));
                    } else {
                        try {
                            map.put(name, new Date(Instant.parse(value.toString()).toEpochMilli()));
                        } catch (DateTimeParseException ignored) {
                            try {
                                //falling back to logisland date parser
                                map.put(name, DateUtil.parse(value.toString()));
                            } catch (ParseException pe) {
                                logger.warn("Error converting field {} with value {} to date : {}",
                                        field.name(), value, pe.getMessage());
                            }
                        }
                    }

                } else {
                    switch (currentSchema.getType()) {
                        case FLOAT:
                            map.put(name, Float.parseFloat(value.toString()));
                            break;
                        case LONG:
                            map.put(name, Long.parseLong(value.toString()));
                            break;
                        case ARRAY:
                            final Map<String, Object> tmpMap = new LinkedHashMap<>();
                            List<Object> filtered = ((List<Object>) value).stream().map(o -> {
                                tmpMap.clear();
                                doFilter(tmpMap, null, o, currentSchema.getElementType());
                                return tmpMap.get(null);
                            }).collect(Collectors.toList());
                            map.put(name, filtered);
                            break;
                        case INT:
                            map.put(name, Integer.parseInt(value.toString()));
                            break;
                        case DOUBLE:
                            map.put(name, Double.parseDouble(value.toString()));
                            break;
                        case BOOLEAN:
                            map.put(name, Boolean.parseBoolean(value.toString()));
                            break;
                        case MAP:
                            Map<String, Object> tmpStruct = new LinkedHashMap<>();
                            ((Map<String, Object>) value).forEach((k, v) -> doFilter(tmpStruct, k, v, currentSchema.getValueType()));
                            map.put(name, tmpStruct);
                            break;
                        case RECORD:
                            Map<String, Object> tmpRecord = new LinkedHashMap<>();
                            ((Map<String, Object>) value).forEach((k, v) -> doFilter(tmpRecord, k, v, currentSchema));
                            map.put(name, tmpRecord);
                            break;
                        default:
                            map.put(name, value.toString());
                            break;
                    }
                }
            }
        }

    }

    public Map<String, Object> filterWithSchema(Map<String, Object> in) {
        Map<String, Object> ret = in;
        if (schema != null) {
            ret = new LinkedHashMap<>();
            doFilter(ret, null, in, schema);
            ret = (Map<String, Object>) ret.getOrDefault(null, Collections.<String, Object>emptyMap());
        }

        return ret;
    }

    public ExtendedJsonSerializer(String schemaString) {
        if (schemaString != null) {
            final Schema.Parser parser = new Schema.Parser().setValidate(false);
            try {
                schema = parser.parse(schemaString);
            } catch (Exception e) {
                throw new RecordSerializationException("unable to create serializer", e);
            }
        } else {
            schema = null;
        }
    }

    @Override
    public void serialize(OutputStream out, Record record) throws RecordSerializationException {
        try {
            mapper().writeValue(out, filterWithSchema(createJsonObject(record)));
        } catch (IOException e) {
            logger.warn(e.toString());
        }

    }

    protected Map<String, Object> createJsonObject(Record record) {
        Map<String, Object> json = new LinkedHashMap<>();

        record.getAllFieldsSorted().forEach(f -> json.put(f.getName(), f.getRawValue()));
        json.put("id", record.getId());
        if (record.getTime() != null) {
            json.put("creationDate", record.getTime().getTime());
        }
        json.put("type", record.getType());
        return json;
    }

    private Field doDeserializeField(String name, Object value) {
        Field ret;
        if (value == null) {
            ret = new Field(name, FieldType.NULL, null);
        } else if (value.getClass().isArray()) {
            ArrayList<Object> tmp = new ArrayList();
            for (int i = 0; i < Array.getLength(value); i++) {
                tmp.add(doDeserializeField("", Array.get(value, i)).getRawValue());
            }
            ret = new Field(name, FieldType.ARRAY, tmp);
        } else if (value instanceof Collection) {
            ArrayList<Object> tmp = new ArrayList();
            for (Object item: (Collection)value) {
                tmp.add(doDeserializeField("", item).getRawValue());
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
                ret = new Field(name, FieldType.DATETIME, value);
            } else {
                ret = new Field(name, FieldType.STRING, value);
            }
        }
        return ret;
    }


    @Override
    public Record deserialize(InputStream in) throws RecordSerializationException {
        final ObjectMapper mapper = mapper();
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
            filterWithSchema(map).forEach((k, v) -> record.setField(doDeserializeField(k, v)));
            return record;

        } catch (IOException e) {
            logger.error(e.toString());
            throw new RecordSerializationException("unable to deserialize record");
        }

    }

    protected synchronized ObjectMapper mapper() {
        if (mapper == null) {
            mapper = new ObjectMapper();
        }
        return mapper;
    }
}
