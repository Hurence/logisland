package com.hurence.logisland.processor.webAnalytics.modele;

import com.hurence.logisland.processor.webAnalytics.util.WebEvent;
import com.hurence.logisland.util.runner.MockRecord;
import org.junit.Assert;
import org.junit.Test;

public class EventTest {

    /**
     * This test ensure Events does not store twice events with the same id.
     * Also it tests than it stores events with same timestamps but different ids.
     */
    @Test
    public void testClone() {
        Event event0 = new Event(
                new WebEvent("0", "session","user", 0L, "url"),
                TestMappings.eventsInternalFields
        );
        MockRecord cloneEvent = new MockRecord(event0.cloneRecord());
        MockRecord eventRecord = new MockRecord(event0.record);
        Assert.assertEquals(eventRecord, cloneEvent);
        eventRecord.setBooleanField("newField", true);
        Assert.assertNotEquals(eventRecord, cloneEvent);
    }
}
