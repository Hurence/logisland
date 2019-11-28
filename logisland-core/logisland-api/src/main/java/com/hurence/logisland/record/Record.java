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

import com.hurence.logisland.logging.ComponentLog;

import java.io.Serializable;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.Set;

public interface Record extends Serializable {

    Position getPosition();

    Record setPosition(Position position);

    boolean hasPosition();

    Date getTime();

    Record setTime(Date recordTime);

    Record setTime(long timestamp);

    Record setFields(Map<String, Field> fields);

    Record addFields(Map<String, Field> fields);

    Record setType(String type);

    String getType();

    Record setId(String id);

    String getId();

    boolean hasField(String fieldName);

    boolean isInternField(String fieldName);

    boolean isInternField(Field field);

    Record setField(Field field);

    Record setField(String fieldName, FieldType fieldType, Object value);

    Record setStringField(String fieldName, String value);

    Record setCheckedField(String fieldName, FieldType fieldType, Object value) throws FieldTypeException;

    Record setLongField(String fieldName, Long value);

    Record setIntField(String fieldName, Integer value);

    Record setFloatField(String fieldName, Float value);

    Record setDoubleField(String fieldName, Double value);

    Record setBooleanField(String fieldName, Boolean value);

    Record setRecordField(String fieldName, Record value);

    Record setBytesField(String fieldName, byte[] value);

    Record setBytesField(String fieldName, Byte[] value);

    Record setArrayField(String fieldName, Collection value);

    Record setObjectField(String fieldName, Object value);

    Record setDateTimeField(String fieldName, Date value);

    Record setMapField(String fieldName, Map value);

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

    /**
     * add error in record
     *
     * @param errorType
     * @param errorMessage
     * @return
     */
    Record addError(String errorType, String errorMessage);
    /**
     * add error in record
     *
     * @param errorType
     * @return
     */
    Record addError(String errorType);

    /**
     * Log error and add it in record
     *
     * @param errorType
     * @param logger
     * @param errorMessage
     * @return
     */
    Record addError(String errorType, ComponentLog logger, String errorMessage);

    /**
     * Log error and add it in record
     *
     * @param errorType
     * @param logger
     * @param errorMessage
     * @param os
     * @return
     */
    Record addError(String errorType, ComponentLog logger, String errorMessage, Object[] os);


    Collection<String> getErrors();
}
