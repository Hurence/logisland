package com.hurence.logisland.querymatcher;

import com.hurence.logisland.event.Event;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertTrue;

/**
 * Created by fprunier on 15/04/16.
 */
public class LuwakQueryMatcherTests {

    @Test
    public void testSimpleMatch() {

        MatchingRule rule1 = new MatchingRule("rule1", "name:luke");

        ArrayList<MatchingRule> rules = new ArrayList<>();
        rules.add(rule1);

        LuwakQueryMatcher matcher = new LuwakQueryMatcher(rules);

        Event ev1 = new Event("mytype");
        ev1.put("name","string", "luke");

        Collection<Event> eventsOut = matcher.process(Arrays.asList(ev1));

        assertTrue(eventsOut.size() == 1);
    }
}
