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

import com.hurence.logisland.processor.webanalytics.modele.WebSession;
import com.hurence.logisland.record.Field;
import com.hurence.logisland.record.Record;
import org.junit.Assert;

import java.util.Date;

import static com.hurence.logisland.processor.webanalytics.modele.TestMappings.sessionInternalFields;

/**
 * A class for testing web session.
 */
public class WebSessionChecker
{
    private static final String CURRENT_CART = "currentCart";
    private final Record record;

    /**
     * Creates a new instance of this class with the provided parameter.
     *
     * @param record the fields to check.
     */
    public WebSessionChecker(final Record record)
    {
        this.record = record;
    }

    public WebSessionChecker(final WebSession session) {
        this(session.getRecord());
    }

    public WebSessionChecker sessionId(final Object value) { return check(sessionInternalFields.getSessionIdField(), value); }
    public WebSessionChecker userId(final Object value) { return check(sessionInternalFields.getUserIdField(), value); }
    public WebSessionChecker firstUserVisitDateTime(final long value) { return check(sessionInternalFields.getFirstUserVisitDateTimeField(), new Date(value).toString()); }
    public WebSessionChecker firstUserVisitEpochSeconds(final Long value) { return check(sessionInternalFields.getFirstUserVisitEpochSecondsField(), value); }
    public WebSessionChecker recordType(final Object value) { return check("record_type", value); }
    public WebSessionChecker recordId(final Object value) { return check("record_id", value); }
    public WebSessionChecker currentCart(final Object value) { return check(CURRENT_CART, value); }
    public WebSessionChecker firstEventDateTime(final long value) { return check(sessionInternalFields.getFirstEventDateTimeField(), new Date(value).toString()); }
    public WebSessionChecker firstEventEpochSeconds(final long value) { return check(sessionInternalFields.getFirstEventEpochSecondsField(), value); }
    public WebSessionChecker h2kTimestamp(final long value) { return check(sessionInternalFields.getTimestampField(), value); }
    public WebSessionChecker firstVisitedPage(final Object value) { return check(sessionInternalFields.getFirstVisitedPageField(), value); }
    public WebSessionChecker eventsCounter(final long value) { return check(sessionInternalFields.getEventsCounterField(), value); }
    public WebSessionChecker lastEventDateTime(final long value) { return check(sessionInternalFields.getLastEventDateTimeField(), new Date(value).toString()); }
    public WebSessionChecker lastEventEpochSeconds(final long value) { return check(sessionInternalFields.getLastEventEpochSecondsField(), value); }
    public WebSessionChecker lastVisitedPage(final Object value) { return check(sessionInternalFields.getLastVisitedPageField(), value); }
    public WebSessionChecker pageviewsCounter(final long value) { return check(sessionInternalFields.getPageviewsCounterField(), value); }
    public WebSessionChecker sessionDuration(final Long value) { return check(sessionInternalFields.getSessionDurationField(), value); }
    public WebSessionChecker isSessionActive(final Object value) { return check(sessionInternalFields.getIsSessionActiveField(), value); }
    public WebSessionChecker sessionInactivityDuration(final Object value) { return check(sessionInternalFields.getSessionInactivityDurationField(), value); }
    public WebSessionChecker record_time(final Object value) { return check("record_time", value); }

    /**
     * Checks the value associated to the specified name against the provided expected value.
     * An exception is thrown if the check fails.
     *
     * @param name the name of the field to check.
     * @param expectedValue the expected value.
     *
     * @return this object for convenience.
     */
    public WebSessionChecker check(final String name, final Object expectedValue)
    {
        final Field field = this.record.getField(name);
        Assert.assertEquals(expectedValue,
                field!=null?field.getRawValue():null);
        return this;
    }
}