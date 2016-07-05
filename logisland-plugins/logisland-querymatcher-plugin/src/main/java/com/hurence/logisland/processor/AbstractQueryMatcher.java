package com.hurence.logisland.processor;

import com.hurence.logisland.event.Event;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Created by fprunier on 15/04/16.
 */
public abstract class AbstractQueryMatcher {

    public static String EVENT_MATCH_TYPE_NAME = "querymatch";

    private List<MatchingRule> rules = Collections.emptyList();

    protected List<MatchingRule> getRules() {
        return rules;
    }

    public void setRules(List<MatchingRule> rules) {
        this.rules = rules;
    }
}
