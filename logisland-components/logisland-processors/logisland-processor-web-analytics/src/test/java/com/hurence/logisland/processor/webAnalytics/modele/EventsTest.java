package com.hurence.logisland.processor.webAnalytics.modele;

import com.hurence.logisland.processor.webAnalytics.util.WebEvent;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Iterator;

public class EventsTest {

    private String SESSIONS_ID = "sessions";
    private String USER_ID = "user";
    private String URL = "url";

    /**
     * This test ensure Events does not store twice events with the same id.
     * Also it tests than it stores events with same timestamps but different ids.
     */
    @Test
    public void testEventsAdd() {
        Events events = new Events(Collections.emptyList());
        Event event0 = new Event(
                new WebEvent("0", SESSIONS_ID,USER_ID, 0L, URL),
                TestMappings.eventsInternalFields
        );
        Event event1 = new Event(
                new WebEvent("1", SESSIONS_ID,USER_ID, 1L, URL),
                TestMappings.eventsInternalFields
        );
        Event eventSameIdThan0 = new Event(
                new WebEvent("0", SESSIONS_ID,USER_ID, 2L, URL),
                TestMappings.eventsInternalFields
        );
        events.add(event0);
        Assert.assertEquals(1, events.size());
        events.add(event1);
        Assert.assertEquals(2, events.size());
        events.add(eventSameIdThan0);
        Assert.assertEquals(2, events.size());
        Assert.assertTrue(events.contains(event0));
        Assert.assertTrue(events.contains(event1));
        Assert.assertTrue(events.contains(eventSameIdThan0));//use id to compare !
        Event eventSameTimestampButDifferentIdThan0 = new Event(
                new WebEvent("2", SESSIONS_ID,USER_ID, 0L, URL),
                TestMappings.eventsInternalFields
        );
        events.add(eventSameTimestampButDifferentIdThan0);
        Assert.assertEquals(3, events.size());
        Assert.assertTrue(events.contains(event0));
        Assert.assertTrue(events.contains(event1));
        Assert.assertTrue(events.contains(eventSameTimestampButDifferentIdThan0));
    }

    @Test
    public void testEventsWithSameTimestampStoredOnlyOnceIfSameId() {
        Events events = new Events(Collections.emptyList());
        Event event0 = new Event(
                new WebEvent("0", SESSIONS_ID,USER_ID, 0L, URL),
                TestMappings.eventsInternalFields
        );
        Event event1 = new Event(
                new WebEvent("1", SESSIONS_ID,USER_ID, 0L, URL),
                TestMappings.eventsInternalFields
        );
        Event event2 = new Event(
                new WebEvent("3", SESSIONS_ID,USER_ID, 0L, URL),
                TestMappings.eventsInternalFields
        );
        Event event3 = new Event(
                new WebEvent("3", SESSIONS_ID,USER_ID, 3L, URL),
                TestMappings.eventsInternalFields
        );
        events.add(event0);
        Assert.assertEquals(1, events.size());
        events.add(event1);
        Assert.assertEquals(2, events.size());
        events.add(event2);
        Assert.assertEquals(3, events.size());
        events.add(event3);
        Assert.assertEquals(3, events.size());
        Assert.assertTrue(events.contains(event0));
        Assert.assertTrue(events.contains(event1));
        Assert.assertTrue(events.contains(event2));
        Assert.assertTrue(events.contains(event3));//same id
        Iterator<Event> it = events.iterator();
        Assert.assertEquals(event0, it.next());
        Assert.assertEquals(event1, it.next());
        Event lastEvent = it.next();
        Assert.assertEquals(event2, lastEvent);
        Assert.assertNotEquals(event3, lastEvent);
    }

//       milli  tmestamp     d/M/yyyy à H:mm:ss








}
