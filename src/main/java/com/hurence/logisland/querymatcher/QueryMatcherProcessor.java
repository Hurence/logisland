package com.hurence.logisland.querymatcher;

import com.hurence.logisland.event.Event;
import com.hurence.logisland.event.EventProcessor;

import org.apache.log4j.Logger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

/**
 * Created by fprunier on 15/04/16.
 */
public class QueryMatcherProcessor implements EventProcessor {

    private static final Logger LOG = Logger.getLogger(QueryMatcherProcessor.class);
    private QueryMatcherBase matcher;

    /**
     * Constructor for query matcher
     * @param matcher
     */
    public QueryMatcherProcessor(QueryMatcherBase matcher) {
        this.matcher = matcher;
    }

    /**
     * Process the incoming events
     * @param events
     * @return
     */
    public Collection<Event> process(Collection<Event> events) {

        return matcher.process(events);
    }
}
