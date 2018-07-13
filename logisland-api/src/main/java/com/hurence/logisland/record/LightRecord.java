package com.hurence.logisland.record;

import com.hurence.logisland.logging.ComponentLog;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Record without additional fields
 * Created by a.ait-bachir on 13/07/2018.
 */
public class LightRecord  implements Record  {
    private static Logger logger = LoggerFactory.getLogger(LightRecord.class);

    public static String DEFAULT_RECORD_TYPE = "light";

    private Map<String, Field> fields = new HashMap<>();

    private List<String> errors = new ArrayList<>();

    public LightRecord() {
        this(DEFAULT_RECORD_TYPE);
    }

    public LightRecord(String type) {
        this.setType(type);
    }

    public LightRecord(Record toClone) {
        this();
        this.setType(toClone.getType());
        toClone.getAllFieldsSorted().forEach(this::setField);
        this.errors = (List<String>) toClone.getErrors();
    }

    @Override
    public String toString() {
        return "Record{" +
                "fields=" + fields +
                ", type='" + getType() + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LightRecord record = (LightRecord) o;

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
            setField(FieldDictionary.RECORD_POSITION, FieldType.RECORD, position);
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
            setField(FieldDictionary.RECORD_TIME, FieldType.LONG, recordTime.getTime());
        return this;
    }

    @Override
    public Record setTime(long timestamp) {
        setField(FieldDictionary.RECORD_TIME, FieldType.LONG, timestamp);
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
            this.setField(FieldDictionary.RECORD_TYPE, FieldType.STRING, type);
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
        setField(FieldDictionary.RECORD_ID, FieldType.STRING, id);
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
     *
     * @param fieldName
     * @param value
     */
    @Override
    public Record setField(String fieldName, FieldType fieldType, Object value) {
        setField(new Field(fieldName, fieldType, value));
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
        setField(new Field(fieldName, FieldType.STRING, value));
        return this;
    }

    /**
     * remove a field by its name
     *
     * @param fieldName
     */
    @Override
    public Field removeField(String fieldName) {
        return fields.remove(fieldName);
    }

    /**
     * retrieve a field by its name
     *
     * @param fieldName
     */
    @Override
    public Field getField(String fieldName) {
        return fields.get(fieldName) != null ? fields.get(fieldName) : new Field();
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
        return fields.size() == 0;
    }

    @Override
    public boolean isValid() {


        for (final Field field : getAllFields()) {
            boolean isValid = true;
            try {


                if (field.isSet()) {
                    switch (field.getType()) {
                        case STRING:
                            isValid = field.getRawValue() instanceof String;
                            break;
                        case INT:
                            isValid = field.getRawValue() instanceof Integer;
                            break;
                        case LONG:
                            isValid = field.getRawValue() instanceof Long;
                            break;
                        case FLOAT:
                            isValid = field.getRawValue() instanceof Float;
                            break;
                        case DOUBLE:
                            isValid = field.getRawValue() instanceof Double;
                            break;
                        case BOOLEAN:
                            isValid = field.getRawValue() instanceof Boolean;
                            break;
                        case ARRAY:
                            isValid = field.getRawValue() instanceof Collection;
                            break;
                        case RECORD:
                            isValid = field.getRawValue() instanceof Record;
                            break;
                        default:
                            isValid = false;
                            break;
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
        return fields.size();
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
        setField(FieldDictionary.RECORD_ERRORS, FieldType.ARRAY, errors);
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
