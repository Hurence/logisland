package com.hurence.logisland.processor;

import com.hurence.logisland.event.Event;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import static org.junit.Assert.assertTrue;

/**
 * Created by fprunier on 15/04/16.
 */
public class LuwakQueryMatcherTest {

    @Test
    public void testSimpleMatch() throws IOException {

        MatchingRule rule1 = new MatchingRule("rule1", "name:luke");

        ArrayList<MatchingRule> rules = new ArrayList<>();
        rules.add(rule1);

        LuwakQueryMatcher matcher = new LuwakQueryMatcher();
        matcher.init(rules);

        Event ev1 = new Event("mytype");
        ev1.setId("myid");
        ev1.put("name","string", "luke");

        Collection<Event> eventsOut = matcher.process(Collections.singletonList(ev1));

        assertTrue(eventsOut.size() == 1);
    }

}
