/**
 * Copyright (C) 2016 Hurence (bailet.thomas@gmail.com)
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

import java.io.Serializable;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.Set;

public interface Record extends Serializable {
    Date getTime();

    Record setTime(Date recordTime);

    Record setFields(Map<String, Field> fields);

    Record addFields(Map<String, Field> fields);

    Record setType(String type);

    String getType();

    Record setId(String id);

    String getId();

    boolean hasField(String fieldName);

    Record setField(Field field);

    Record setField(String fieldName, FieldType fieldType, Object value);

    Record setStringField(String fieldName, String value);

    Field removeField(String fieldName);

    Field getField(String fieldName);

    Record setStringFields(Map<String, String> entrySets);

    Collection<Field> getAllFieldsSorted();

    Collection<Field> getAllFields();

    Set<String> getAllFieldNames();

    Set<Map.Entry<String, Field>> getFieldsEntrySet();

    boolean isEmpty();

    boolean isValid();

    int size();

    int sizeInBytes();
}
