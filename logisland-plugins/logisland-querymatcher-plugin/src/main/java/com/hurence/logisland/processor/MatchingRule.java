package com.hurence.logisland.processor;

/**
 * Created by fprunier on 15/04/16.
 */
public class MatchingRule {
    private String name = null;
    private String query = null;

    public MatchingRule(String name, String query) {
        this.name = name;
        this.query = query;
    }

    public String getName() {
        return name;
    }

    public String getQuery() {
        return query;
    }
}
