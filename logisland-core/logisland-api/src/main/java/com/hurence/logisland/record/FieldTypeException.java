package com.hurence.logisland.record;

public class FieldTypeException extends Exception {

    public FieldTypeException(String explanation) {
        super(explanation);
    }

    public FieldTypeException(Throwable cause) {
        super(cause);
    }

    public FieldTypeException(String explanation, Throwable cause) {
        super(explanation, cause);
    }
}
