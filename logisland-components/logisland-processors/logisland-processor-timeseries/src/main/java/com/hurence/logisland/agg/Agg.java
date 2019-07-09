package com.hurence.logisland.agg;

/**
 * Set of allowed values for aggregations
 *
 */
public enum Agg {

    MAX,
    MIN,
    AVG;

    public String toString() {
        return name;
    }
    private String name;

    Agg() {
        this.name = this.name().toLowerCase();
    }

    public String getName() {
        return name;
    }
}
