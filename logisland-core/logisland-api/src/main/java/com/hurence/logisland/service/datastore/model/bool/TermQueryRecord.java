package com.hurence.logisland.service.datastore.model.bool;

import com.hurence.logisland.service.datastore.model.bool.BoolQueryRecord;

public class TermQueryRecord implements BoolQueryRecord {
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
