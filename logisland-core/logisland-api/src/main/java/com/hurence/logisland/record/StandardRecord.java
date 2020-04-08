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
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


/**
 * Encapsulation of an Event a map of Fields
 *
 * @author Tom Bailet
 */
public class StandardRecord implements Record {

    private static Logger logger = LoggerFactory.getLogger(StandardRecord.class);

    public static String DEFAULT_RECORD_TYPE = "generic";

    private Map<String, Field> fields = new TreeMap<>();

    private List<String> errors = new ArrayList<>();

    public StandardRecord() {
        this(DEFAULT_RECORD_TYPE);
    }

    public StandardRecord(String type) {
        this.setType(type);
        this.setTime(new Date());
        this.setId(UUID.randomUUID().toString());
    }

    public StandardRecord(Record toClone) {
        this();
        this.setType(toClone.getType());
        this.setTime(toClone.getTime());
        this.setId(UUID.randomUUID().toString());
        toClone.getAllFieldsSorted().forEach(this::setField);
        this.errors = (List<String>) toClone.getErrors();
    }

    @Override
    public String toString() {
        return "Record{" +
                "fields=" + fields +
                ", time=" + getTime() +
                ", type='" + getType() + '\'' +
                ", id='" + getId() + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null) return false;
        if (!(o instanceof Record)) return false;

        Record record = (Record) o;

        if (getAllFields() == null || record.getAllFields() == null ||
                !CollectionUtils.isEqualCollection(this.getAllFields(), record.getAllFields()))

            return false;
        return getId() != null ? getId().equals(record.getId()) : record.getId() == null;

    }

    @Override
    public int hashCode() {
        int result = fields != null ? fields.hashCode() : 0;
        result = 31 * result + (getId() != null ? getId().hashCode() : 0);
        return result;
    }

    @Override
    public Position getPosition() {
        if (hasPosition())
            return (Position) getField(FieldDictionary.RECORD_POSITION).asRecord();
        else return null;
    }

    @Override
    public Record setPosition(Position position) {
        if (position != null)
            setRecordField(FieldDictionary.RECORD_POSITION, position);
        return this;
    }

    @Override
    public boolean hasPosition() {
        return hasField(FieldDictionary.RECORD_POSITION);
    }

    @Override
    public Date getTime() {
        try {
            return new Date((long) getField(FieldDictionary.RECORD_TIME).getRawValue());
        } catch (Exception ex) {
            return null;
        }
    }

    @Override
    public Record setTime(Date recordTime) {
        if (recordTime != null)
            setLongField(FieldDictionary.RECORD_TIME, recordTime.getTime());
        return this;
    }

    @Override
    public Record setTime(long timestamp) {
        setLongField(FieldDictionary.RECORD_TIME, timestamp);
        return this;
    }

    @Override
    public Record setFields(Map<String, Field> fields) {
        this.fields = fields;
        return this;
    }

    @Override
    public Record addFields(Map<String, Field> fields) {
        fields.values().forEach(this::setField);
        return this;
    }

    @Override
    public Record setType(String type) {
        if (type != null) {
            this.setStringField(FieldDictionary.RECORD_TYPE, type);
        }
        return this;
    }

    /**
     * get the
     *
     * @return
     */
    @Override
    public String getType() {
        return getField(FieldDictionary.RECORD_TYPE).asString();
    }

    /**
     * retrieve record id
     *
     * @return the record id
     */
    @Override
    public String getId() {
        return getField(FieldDictionary.RECORD_ID).asString();
    }

    /**
     * sets Record id
     *
     * @param id
     */
    public Record setId(String id) {
        setStringField(FieldDictionary.RECORD_ID, id);
        return this;
    }

    /**
     * checks if a field is defined
     *
     * @param fieldName
     * @return
     */
    @Override
    public boolean hasField(String fieldName) {
        return fields.containsKey(fieldName);
    }

    @Override
    public boolean isInternField(String fieldName) {
        return FieldDictionary.TECHNICAL_FIELDS.contains(fieldName);
    }

    @Override
    public boolean isInternField(Field field) {
        if (field != null) return isInternField(field.getName());
        return false;
    }

    /**
     * set a field value
     *
     * @param field
     */
    @Override
    public Record setField(Field field) {
        fields.put(field.getName(), field);
        return this;
    }

    /**
     * set a field value
     *  @param fieldName
     * @param value
     */
    @Override
    public Record setField(String fieldName, FieldType fieldType, Object value) {
        setField(new Field(fieldName, fieldType, value));
        return this;
    }

    @Override
    public Record setCheckedField(String fieldName, FieldType fieldType, Object value) throws FieldTypeException {
        setField(new CheckedField(fieldName, fieldType, value));
        return this;
    }

    /**
     * set a field value as a String value
     *
     * @param fieldName the name of the string field
     * @param value     the value to be added
     */
    @Override
    public Record setStringField(String fieldName, String value) {
        return setField(fieldName, FieldType.STRING, value);
    }

    @Override
    public Record setLongField(String fieldName, Long value) {
        return setField(fieldName, FieldType.LONG, value);
    }

    @Override
    public Record setIntField(String fieldName, Integer value) {
        return setField(fieldName, FieldType.INT, value);
    }

    @Override
    public Record setFloatField(String fieldName, Float value) {
        return setField(fieldName, FieldType.FLOAT, value);
    }

    @Override
    public Record setDoubleField(String fieldName, Double value) {
        return setField(fieldName, FieldType.DOUBLE, value);
    }

    @Override
    public Record setBooleanField(String fieldName, Boolean value) {
        return setField(fieldName, FieldType.BOOLEAN, value);
    }


    @Override
    public Record setRecordField(String fieldName, Record value) {
        return setField(fieldName, FieldType.RECORD, value);
    }

    @Override
    public Record setBytesField(String fieldName, byte[] value) {
        return setField(fieldName, FieldType.BYTES, value);
    }

    @Override
    public Record setBytesField(String fieldName, Byte[] value) {
        return setField(fieldName, FieldType.BYTES, value);
    }

    @Override
    public Record setArrayField(String fieldName, Collection value) {
        return setField(fieldName, FieldType.ARRAY, value);
    }


    @Override
    public Record setObjectField(String fieldName, Object value) {
        return setField(fieldName, FieldType.OBJECT, value);
    }

    @Override
    public Record setDateTimeField(String fieldName, Date value) {
        return setField(fieldName, FieldType.DATETIME, value);
    }

    @Override
    public Record setMapField(String fieldName, Map value) {
        return setField(fieldName, FieldType.MAP, value);
    }

    /**
     * remove a field by its name
     *
     * @param fieldName
     */
    @Override
    public Field removeField(String fieldName) {
        if (fieldName.equals(FieldDictionary.RECORD_TIME)) {
            logger.debug("trying to remove record_time field. we won't let you do that !!");
            return fields.get(FieldDictionary.RECORD_TIME);
        } else {
            return fields.remove(fieldName);
        }
    }

    /**
     * retrieve a field by its name
     *
     * @param fieldName
     */
    @Override
    public Field getField(String fieldName) {
        return fields.get(fieldName);
    }

    @Override
    public Record setStringFields(Map<String, String> entrySets) {
        Objects.requireNonNull(entrySets, "Argument can not be null");
        for (Map.Entry<String, String> entry : entrySets.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            this.setStringField(key, value);
        }
        return this;
    }

    @Override
    public Collection<Field> getAllFieldsSorted() {
        List<Field> fields = new ArrayList<>(getAllFields());
        Collections.sort(fields, (left, right) -> left.getName().compareTo(right.getName()));
        return fields;
    }

    @Override
    public Collection<Field> getAllFields() {
        return fields.values();
    }

    @Override
    public Set<String> getAllFieldNames() {
        return fields.keySet();
    }

    @Override
    public Set<Map.Entry<String, Field>> getFieldsEntrySet() {
        return fields.entrySet();
    }

    /**
     * checks if record has no fields other than id, time and type
     *
     * @return true if fields is emty
     */
    @Override
    public boolean isEmpty() {
        return fields.size() == 3;
    }

    @Override
    public boolean isValid() {
        for (final Field field : getAllFields()) {
            boolean isValid = true;
            try {
                if (field.isSet()) {
                    try {
                        CheckedField.checkType(field.getType(), field.getRawValue());
                    } catch (FieldTypeException ex) {
                        isValid = false;
                    }
                }
            } catch (Throwable ex) {
                return false;
            }
            if (!isValid) {
                logger.info("field {} is not an instance of type {}", field.getName(), field.getType());
                return false;
            }
        }
        return true;
    }

    /**
     * The number of fields (minus the 3 technical ones)
     *
     * @return number of real fields
     */
    @Override
    public int size() {
        return fields.size() - 3;
    }

    /**
     * compute roughly the size in bytes for an event
     * id, type and creationDate are ignored
     *
     * @return
     */
    @Override
    public int sizeInBytes() {

        int size = 0;

        for (Map.Entry<String, Field> entry : getFieldsEntrySet()) {

            Field field = entry.getValue();
            Object fieldValue = field.getRawValue();
            FieldType fieldType = field.getType();

            // dump event field as record attribute

            try {
                switch (fieldType) {
                    case STRING:
                        size += ((String) fieldValue).getBytes().length;
                        break;
                    case INT:
                        size += 4;
                        break;
                    case LONG:
                        size += 8;
                        break;
                    case FLOAT:
                        size += 4;
                        break;
                    case DOUBLE:
                        size += 8;
                        break;
                    case BOOLEAN:
                        size += 1;
                        break;
                    default:
                        break;
                }
            } catch (Exception ex) {
                // nothing to do
            }

        }

        return size;
    }

    @Override
    public Record addError(final String type, final String message) {
        StringBuilder finalMessage = new StringBuilder();
        finalMessage.append(type);
        if (message == null || !message.isEmpty()) {
            finalMessage.append(": ");
            finalMessage.append(message);
        }
        errors.add(finalMessage.toString());
        if (!hasField(FieldDictionary.RECORD_ERRORS)) {
            setArrayField(FieldDictionary.RECORD_ERRORS, errors);
        }
        return this;
    }

    @Override
    public Record addError(String errorType) {
        return addError(errorType, null);
    }

    @Override
    public Record addError(String errorType, ComponentLog logger, String errorMessage) {
        logger.error(errorMessage);
        return addError(errorType, errorMessage);
    }

    @Override
    public Record addError(String errorType, ComponentLog logger, String errorMessage, Object[] os) {
        logger.error(errorMessage, os);
        return addError(errorType, errorMessage);
    }

    @Override
    public Collection<String> getErrors() {
        return new ArrayList<>(errors);
    }
}
