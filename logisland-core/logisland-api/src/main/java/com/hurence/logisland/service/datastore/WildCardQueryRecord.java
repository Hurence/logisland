package com.hurence.logisland.service.datastore;

public class WildCardQueryRecord {
    private final String fieldName;
    private final String fieldValue;

    public WildCardQueryRecord(String fieldName, String fieldValue) {
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
        return "WildCardQueryRecord{" +
                "fieldName='" + fieldName + '\'' +
                ", fieldValue='" + fieldValue + '\'' +
                '}';
    }
}
