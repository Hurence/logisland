package com.hurence.logisland.record;

import java.util.Collection;
import java.util.Date;
import java.util.Map;

public class CheckedField extends Field {

    public CheckedField(String name, FieldType type, Object rawValue) throws FieldTypeException {
        super(name, type, rawValue);
        checkType(this.type, this.rawValue);
    }

    public static void checkType(FieldType type, Object rawValue) throws FieldTypeException {
        if (rawValue == null) return;
        switch (type) {
            case NULL:
                throw new FieldTypeException("field of type " + FieldType.NULL +
                        " is not null !");
            case STRING:
                if (!(rawValue instanceof String)) throw new FieldTypeException("field of type " + FieldType.STRING +
                        " value's is of type " + rawValue.getClass());
                break;
            case INT:
                if (!(rawValue instanceof Integer)) throw new FieldTypeException("field of type " + FieldType.INT +
                        " value's is of type " + rawValue.getClass());
                break;
            case LONG:
                if (!(rawValue instanceof Long)) throw new FieldTypeException("field of type " + FieldType.LONG +
                        " value's is of type " + rawValue.getClass());
                break;
            case ARRAY:
                if (!(rawValue instanceof Collection || rawValue.getClass().isArray())) throw new FieldTypeException("field of type " + FieldType.ARRAY +
                        " value's is of type " + rawValue.getClass());
                break;
            case FLOAT:
                if (!(rawValue instanceof Float)) throw new FieldTypeException("field of type " + FieldType.FLOAT +
                        " value's is of type " + rawValue.getClass());
                break;
            case DOUBLE:
                if (!(rawValue instanceof Double)) throw new FieldTypeException("field of type " + FieldType.DOUBLE +
                        " value's is of type " + rawValue.getClass());
                break;
            case BYTES:
                if (!(rawValue instanceof byte[] || rawValue instanceof Byte[])) throw new FieldTypeException("field of type " + FieldType.BYTES +
                        " value's is of type " + rawValue.getClass());
                break;
            case RECORD:
                if (!(rawValue instanceof Record)) throw new FieldTypeException("field of type " + FieldType.RECORD +
                        " value's is of type " + rawValue.getClass());
                break;
            case MAP:
                if (!(rawValue instanceof Map)) throw new FieldTypeException("field of type " + FieldType.MAP +
                        " value's is of type " + rawValue.getClass());
                break;
            case ENUM:
                if (!(rawValue instanceof Enum)) throw new FieldTypeException("field of type " + FieldType.ENUM +
                        " value's is of type " + rawValue.getClass());
                break;
            case BOOLEAN:
                if (!(rawValue instanceof Boolean)) throw new FieldTypeException("field of type " + FieldType.BOOLEAN +
                        " value's is of type " + rawValue.getClass());
                break;
            case UNION:
                //no check
                break;
            case DATETIME:
                if (!(rawValue instanceof Date)) throw new FieldTypeException("field of type " + FieldType.DATETIME +
                        " value's is of type " + rawValue.getClass());
                break;
        }
    }
}
