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
package com.hurence.logisland.processor.webanalytics.util;

import com.hurence.logisland.processor.webanalytics.modele.TestMappings;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.StandardRecord;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

/**
 * The class represents a web event.
 */
public class WebEvent extends StandardRecord
{
    public static final String CURRENT_CART = "currentCart";
    /**
     * Creates a new instance of this class with the provided parameter.
     *
     * @param id the event identifier.
     * @param sessionId the session identifier.
     * @param userId the user identifier.
     * @param timestamp the h2kTimestamp.
     * @param url the visited address.
     */
    public WebEvent(final String id, final String sessionId, final String userId, final Long timestamp,
                    final String url)
    {
        this.setField(TestMappings.eventsInternalFields.getSessionIdField(), FieldType.STRING, sessionId)
                .setField(TestMappings.eventsInternalFields.getUserIdField(), FieldType.STRING, userId)
                .setField(TestMappings.eventsInternalFields.getTimestampField(), FieldType.STRING, timestamp)
                .setField(TestMappings.eventsInternalFields.getVisitedPageField(), FieldType.STRING, url)
                .setField(CURRENT_CART, FieldType.ARRAY, null)
                .setField("record_id", FieldType.STRING, id);
    }

    public WebEvent add(final String name, final String value)
    {
        this.setStringField(name, value);
        return this;
    }

    ZonedDateTime getZonedDateTime() {
        long epochMilli = this.getField(TestMappings.eventsInternalFields.getTimestampField()).asLong();
        return ZonedDateTime.ofInstant(Instant.ofEpochMilli(epochMilli), ZoneId.systemDefault());
    }

    long getTimestamp() {
        return this.getField(TestMappings.eventsInternalFields.getTimestampField()).asLong();
    }
}