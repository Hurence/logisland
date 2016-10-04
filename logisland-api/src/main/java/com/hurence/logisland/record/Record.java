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

package com.hurence.logisland.record;

import java.io.Serializable;
import java.util.*;

/**
 * Encapsulation of an Event a map of Fields
 *
 * @author Tom Bailet
 */
public class Record implements Serializable {

    public static String DEFAULT_RECORD_TYPE = "generic";

    private Map<String, Field> fields = new HashMap<>();

    public Record() {
        this(DEFAULT_RECORD_TYPE);
    }

    public Record(String type) {
        this.setType(type);
        this.setTime(new Date());
        this.setId(UUID.randomUUID().toString());
    }

    public Record(Record toClone) {
        this.setType(toClone.getType());
        this.setTime(toClone.getTime());
        this.setId(UUID.randomUUID().toString());
        toClone.getAllFieldsSorted().forEach(this::setField);
    }

    @Override
    public String toString() {
        return "Event{" +
                "fields=" + fields +
                ", creationDate=" + getTime() +
                ", type='" + getType() + '\'' +
                ", id='" + getId() + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Record record = (Record) o;

        if (fields != null ? !fields.equals(record.fields) : record.fields != null) return false;
        return getId() != null ? getId().equals(record.getId()) : record.getId() == null;

    }

    @Override
    public int hashCode() {
        int result = fields != null ? fields.hashCode() : 0;
        result = 31 * result + (getId() != null ? getId().hashCode() : 0);
        return result;
    }

    public Date getTime() {
        try{
            return new Date((long)getField(FieldDictionary.RECORD_TIME).getRawValue());
        }catch(Exception ex){
            return null;
        }
    }

    public void setTime(Date recordTime) {
        setField(FieldDictionary.RECORD_TIME, FieldType.LONG, recordTime.getTime());
    }

    public void setFields(Map<String, Field> fields) {
        this.fields = fields;
    }

    public void setType(String type) {
        this.setField(FieldDictionary.RECORD_TYPE, FieldType.STRING, type);
    }

    /**
     * get the
     * @return
     */
    public String getType() {
        return getField(FieldDictionary.RECORD_TYPE).asString();
    }

    /**
     * retrieve record id
     *
     * @return the record id
     */
    public String getId() {
        return getField(FieldDictionary.RECORD_ID).asString();
    }

    /**
     * sets Record id
     * @param id
     */
    public void setId(String id) {
        setField(FieldDictionary.RECORD_ID, FieldType.STRING, id);
    }

    /**
     * checks if a field is defined
     *
     * @param fieldName
     * @return
     */
    public boolean hasField(String fieldName) { return fields.containsKey(fieldName); }

    /**
     * set a field value
     *
     * @param field
     */
    public void setField(Field field) {
        fields.put(field.getName(), field);
    }

    /**
     * set a field value
     *
     * @param fieldName
     * @param value
     */
    public void setField(String fieldName, FieldType fieldType, Object value) {
        setField(new Field(fieldName, fieldType, value));
    }

    /**
     * set a field value as a String value
     *
     * @param fieldName the name of the string field
     * @param value the value to be added
     */
    public void setStringField(String fieldName, String value) {
        setField(new Field(fieldName, FieldType.STRING, value));
    }

    /**
     * remove a field by its name
     *
     * @param fieldName
     */
    public Field removeField(String fieldName) {
        return fields.remove(fieldName);
    }

    /**
     * retrieve a field by its name
     *
     * @param fieldName
     */
    public Field getField(String fieldName) {
        return fields.get(fieldName);
    }

    public void setStringFields(Map<String, String> entrySets) {
        Objects.requireNonNull(entrySets, "Argument can not be null");
        for (Map.Entry<String, String> entry : entrySets.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            this.setStringField(key, value);
        }
    }

    public Collection<Field> getAllFieldsSorted() {
        List<Field> fields = new ArrayList<>(getAllFields());
        Collections.sort(fields, (left, right) -> left.getName().compareTo(right.getName()));
        return fields;
    }

    public Collection<Field> getAllFields() {
        return fields.values();
    }

    public Set<String> getAllFieldNames() {
        return fields.keySet();
    }

    public Set<Map.Entry<String, Field>> getFieldsEntrySet() {
        return fields.entrySet();
    }

    /**
     * checks if record has no fields other than id, time and type
     *
     * @return true if fields is emty
     */
    public boolean isEmpty() {
        return fields.size() == 3;
    }

    public int size(){
        return fields.size();
    }
}
