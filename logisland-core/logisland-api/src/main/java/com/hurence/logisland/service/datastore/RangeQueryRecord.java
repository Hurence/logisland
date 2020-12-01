package com.hurence.logisland.service.datastore;

public class RangeQueryRecord {
    public static final boolean DEFAULT_INCLUDE_UPPER = true;
    public static final boolean DEFAULT_INCLUDE_LOWER = true;

    private String fieldName;
    private Object from = null;
    private Object to = null;
    private boolean includeLower = DEFAULT_INCLUDE_LOWER;
    private boolean includeUpper = DEFAULT_INCLUDE_UPPER;

    public RangeQueryRecord(String fieldName) {
        this.fieldName = fieldName;
    }

    public String getFieldName() {
        return fieldName;
    }

    public RangeQueryRecord setFieldName(String fieldName) {
        this.fieldName = fieldName;
        return this;
    }

    public Object getFrom() {
        return from;
    }

    public RangeQueryRecord setFrom(Object from) {
        this.from = from;
        return this;
    }

    public Object getTo() {
        return to;
    }

    public RangeQueryRecord setTo(Object to) {
        this.to = to;
        return this;
    }

    public boolean isIncludeLower() {
        return includeLower;
    }

    public RangeQueryRecord setIncludeLower(boolean includeLower) {
        this.includeLower = includeLower;
        return this;
    }

    public boolean isIncludeUpper() {
        return includeUpper;
    }

    public RangeQueryRecord setIncludeUpper(boolean includeUpper) {
        this.includeUpper = includeUpper;
        return this;
    }

    @Override
    public String toString() {
        return "RangeQueryRecord{" +
                "fieldName='" + fieldName + '\'' +
                ", from=" + from +
                ", to=" + to +
                ", includeLower=" + includeLower +
                ", includeUpper=" + includeUpper +
                '}';
    }
}
