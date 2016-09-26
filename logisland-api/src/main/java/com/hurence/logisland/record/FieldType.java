package com.hurence.logisland.record;

/**
 * Set of allowed values for a field type
 *
 * https://avro.apache.org/docs/1.8.1/spec.html#schema_primitive
 */
public class FieldType {

    public static String NULL = "null";
    public static String STRING = "string";
    public static String INT = "int";
    public static String LONG = "long";
    public static String ARRAY = "array";
    public static String FLOAT = "float";
    public static String DOUBLE = "double";
    public static String BYTES = "bytes";
    public static String RECORD = "record";
    public static String MAP = "map";
    public static String ENUM = "enum";
    public static String BOOLEAN = "boolean";
}
