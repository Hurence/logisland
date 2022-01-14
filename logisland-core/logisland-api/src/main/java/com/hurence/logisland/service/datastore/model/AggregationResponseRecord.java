package com.hurence.logisland.service.datastore.model;

public abstract class AggregationResponseRecord {
    protected String aggregationName;
    protected String aggregationType;

    public AggregationResponseRecord(String aggregationName, String aggregationType) {
        this.aggregationName = aggregationName;
        this.aggregationType = aggregationType;
    }
}
