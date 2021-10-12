/**
 * Copyright (C) 2016 Hurence (support@hurence.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.processor.webanalytics.modele;

import com.hurence.logisland.processor.webanalytics.util.WebEvent;
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
