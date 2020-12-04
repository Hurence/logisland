package com.hurence.logisland.service.datastore;

public class SortQueryRecord {

    private String fieldName;
    private SortOrder sortingOrder = SortOrder.ASC;

    public SortQueryRecord(String fieldName) {
        this.fieldName = fieldName;
    }

    public SortQueryRecord(String fieldName, SortOrder sortingOrder) {
        this.fieldName = fieldName;
        this.sortingOrder = sortingOrder;
    }

    public String getFieldName() {
        return fieldName;
    }

    public SortQueryRecord setFieldName(String fieldName) {
        this.fieldName = fieldName;
        return this;
    }

    public boolean isAscendent() {
        return sortingOrder == SortOrder.ASC;
    }

    public boolean isDescendant() {
        return sortingOrder == SortOrder.DESC;
    }

    public SortOrder getSortingOrder() {
        return sortingOrder;
    }

    public SortQueryRecord setSortingOrder(SortOrder sortingOrder) {
        this.sortingOrder = sortingOrder;
        return this;
    }
}
