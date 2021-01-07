package com.hurence.logisland.processor.webAnalytics.util;

import com.hurence.logisland.processor.webAnalytics.modele.*;
import com.hurence.logisland.util.runner.MockRecord;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SessionCalculatorTest {

    private String SESSIONS_ID = "sessions";
    private String USER_ID = "user";
    private String URL = "url";

    @Test
    public void testDateFormatters() {
        SessionsCalculator calculator = new SessionsCalculator(
                Arrays.asList(
                        // Day overlap
                        (session, event) ->
                        {
                            return new InvalidSessionCheckResult("not valid");
                        }
                ),
                1800,
                TestMappings.sessionInternalFields,
                TestMappings.eventsInternalFields,
                Collections.emptyList(),
                SESSIONS_ID
        );
        Events events = new Events(Collections.emptyList());
        Event event0 = new Event(
                new WebEvent("0", SESSIONS_ID, USER_ID, 0L, URL),
                TestMappings.eventsInternalFields
        );
        Event event1 = new Event(
                new WebEvent("1", SESSIONS_ID, USER_ID, 1L, URL),
                TestMappings.eventsInternalFields
        );
        Event event2 = new Event(
                new WebEvent("2", SESSIONS_ID, USER_ID, 2L, URL),
                TestMappings.eventsInternalFields
        );
        events.add(event0);
        events.add(event1);
        events.add(event2);
        calculator.processEvents(events, false, null);
        List<WebSession> sessions = new ArrayList<>(calculator.getCalculatedSessions());
        assertEquals(3, sessions.size());
        new MockRecord(event0.getRecord())
                .assertFieldNotExists(TestMappings.eventsInternalFields.getOriginalSessionIdField())
                .assertFieldEquals(TestMappings.eventsInternalFields.getSessionIdField(), SESSIONS_ID);
        new MockRecord(event1.getRecord())
                .assertFieldEquals(TestMappings.eventsInternalFields.getOriginalSessionIdField(), SESSIONS_ID)
                .assertFieldEquals(TestMappings.eventsInternalFields.getSessionIdField(), SESSIONS_ID + "#2");
        new MockRecord(event2.getRecord())
                .assertFieldEquals(TestMappings.eventsInternalFields.getOriginalSessionIdField(), SESSIONS_ID)
                .assertFieldEquals(TestMappings.eventsInternalFields.getSessionIdField(), SESSIONS_ID + "#3");

    }


    @Test
    public void testWithBadSessionBinding() {
        SessionsCalculator calculator = new SessionsCalculator(
                Arrays.asList(
                        // Day overlap
                        (session, event) ->
                        {
                            return new InvalidSessionCheckResult("not valid");
                        }
                ),
                1800,
                TestMappings.sessionInternalFields,
                TestMappings.eventsInternalFields,
                Collections.emptyList(),
                SESSIONS_ID
        );
        Events events = new Events(Collections.emptyList());
        Event event0 = new Event(
                new WebEvent("0", SESSIONS_ID, USER_ID, 0L, URL),
                TestMappings.eventsInternalFields
        );
        Event event1 = new Event(
                new WebEvent("1", SESSIONS_ID, USER_ID, 1L, URL),
                TestMappings.eventsInternalFields
        );
        Event event2 = new Event(
                new WebEvent("2", SESSIONS_ID + "#2", USER_ID, 2L, URL),
                TestMappings.eventsInternalFields
        );
        events.add(event0);
        events.add(event1);
        events.add(event2);
        calculator.processEvents(events, false, null);
        List<WebSession> sessions = new ArrayList<>(calculator.getCalculatedSessions());
        assertEquals(3, sessions.size());
        new MockRecord(event0.getRecord())
                .assertFieldNotExists(TestMappings.eventsInternalFields.getOriginalSessionIdField())
                .assertFieldEquals(TestMappings.eventsInternalFields.getSessionIdField(), SESSIONS_ID);
        new MockRecord(event1.getRecord())
                .assertFieldEquals(TestMappings.eventsInternalFields.getOriginalSessionIdField(), SESSIONS_ID)
                .assertFieldEquals(TestMappings.eventsInternalFields.getSessionIdField(), SESSIONS_ID + "#2");
        new MockRecord(event2.getRecord())
                .assertFieldEquals(TestMappings.eventsInternalFields.getOriginalSessionIdField(), SESSIONS_ID)
                .assertFieldEquals(TestMappings.eventsInternalFields.getSessionIdField(), SESSIONS_ID + "#3");

    }
}
