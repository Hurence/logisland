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
package com.hurence.logisland.record;

import java.lang.reflect.Array;
import java.util.*;

public final class RecordUtils {

    public static Record getKeyValueRecord(String key, String value) {
        final Record record = new StandardRecord("kv_record");
        record.setStringField(FieldDictionary.RECORD_KEY, key);
        record.setStringField(FieldDictionary.RECORD_VALUE, value);
        return record;
    }

    public static Record getKeyValueRecord(byte[] key, byte[] value) {
        final Record record = new StandardRecord("kv_record");
        record.setField(FieldDictionary.RECORD_KEY, FieldType.BYTES, key);
        record.setField(FieldDictionary.RECORD_VALUE, FieldType.BYTES, value);
        return record;
    }

    public static Record getRecordOfString(String... kvs) {
        if (kvs.length % 2 != 0) throw new IllegalArgumentException("input array does not have an even number as length.");
        final Record record = new StandardRecord("kv_record");
        for(int i=0;i<kvs.length;i+=2) {
            record.setField(kvs[i], FieldType.STRING, kvs[i+1]);
        }
        return record;
    }

    /**
     * Returns the conversion of a record to a map where all {@code null} values were removed.
     *
     * @param record the record to convert.
     * @param filterInnerRecord if {@code true} special dictionnary fields are ignored; included otherwise.
     *
     * @return the conversion of a record to a map where all {@code null} values were removed.
     */
    public static Map<String, Object> toMap(final Record record,
                                            final boolean filterInnerRecord) {
        try {
            final Map<String, Object> result = new HashMap<>();

            record.getFieldsEntrySet()
                    .stream()
                    .forEach(entry ->
                    {
                        if (!filterInnerRecord || (filterInnerRecord && !FieldDictionary.contains(entry.getKey()))) {
                            Object value = entry.getValue().getRawValue();
                            if (value != null) {
                                switch (entry.getValue().getType()) {
                                    case RECORD:
                                        value = toMap((Record) value, true);
                                        break;
                                    case ARRAY:
                                        Collection collection;
                                        if (value.getClass().isArray()) {
                                            collection = new ArrayList<>();
                                            for (int i = 0; i < Array.getLength(value); i++) {
                                                collection.add(Array.get(value, i));
                                            }
                                        } else if (value instanceof Collection) {
                                            collection = (Collection) value;
                                        } else {
                                            collection = Arrays.asList(value);
                                        }
                                        final List list = new ArrayList(collection.size());
                                        for (final Object item : collection) {
                                            if (item instanceof Record) {
                                                list.add(toMap((Record) item, true));
                                            } else {
                                                list.add(item);
                                            }
                                        }
                                        value = list;
                                        break;
                                    default:
                                }
                                result.put(entry.getKey(), value);
                            }
                        }
                    });
            return result;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
