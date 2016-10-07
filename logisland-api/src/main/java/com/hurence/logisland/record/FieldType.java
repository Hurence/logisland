package com.hurence.logisland.record;

/**
 * Set of allowed values for a field type
 * <p>
 * https://avro.apache.org/docs/1.8.1/spec.html#schema_primitive
 */
public enum FieldType {

    NULL,
    STRING,
    INT,
    LONG,
    ARRAY,
    FLOAT,
    DOUBLE,
    BYTES,
    RECORD,
    MAP,
    ENUM,
    BOOLEAN;

    public String toString() {
        return name().toLowerCase();
    }
}
