package com.hurence.logisland.service.datastore;

public class TermQueryRecord {
    private final String fieldName;
    private final String fieldValue;

    public TermQueryRecord(String fieldName, String fieldValue) {
        this.fieldName = fieldName;
        this.fieldValue = fieldValue;
    }

    public String getFieldName() {
        return fieldName;
    }

    public String getFieldValue() {
        return fieldValue;
    }


    @Override
    public String toString() {
        return "TermQueryRecord{" +
                "fieldName='" + fieldName + '\'' +
                ", fieldValue='" + fieldValue + '\'' +
                '}';
    }
}
