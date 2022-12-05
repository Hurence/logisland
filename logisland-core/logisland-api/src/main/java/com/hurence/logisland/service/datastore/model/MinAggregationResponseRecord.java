package com.hurence.logisland.service.datastore.model;

public class MinAggregationResponseRecord extends AggregationResponseRecord{
    private final Long minimum;

    public MinAggregationResponseRecord(String aggregationName, String aggregationType, Long minimum) {
        super(aggregationName, aggregationType);
        this.minimum = minimum;
    }

    public Long getMinimum() {
        return minimum;
    }
}
