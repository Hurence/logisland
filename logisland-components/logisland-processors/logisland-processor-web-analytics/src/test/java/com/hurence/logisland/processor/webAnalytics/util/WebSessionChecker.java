package com.hurence.logisland.processor.webAnalytics.util;

import com.hurence.logisland.record.Field;
import com.hurence.logisland.record.Record;
import org.junit.Assert;

import java.util.Date;

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

    public WebSessionChecker sessionId(final Object value) { return check("sessionId", value); }
    public WebSessionChecker Userid(final Object value) { return check("Userid", value); }
    public WebSessionChecker record_type(final Object value) { return check("record_type", value); }
    public WebSessionChecker record_id(final Object value) { return check("record_id", value); }
    public WebSessionChecker currentCart(final Object value) { return check(CURRENT_CART, value); }
    public WebSessionChecker firstEventDateTime(final long value) { return check("firstEventDateTime", new Date(value).toString()); }
    public WebSessionChecker h2kTimestamp(final long value) { return check("h2kTimestamp", value); }
    public WebSessionChecker firstVisitedPage(final Object value) { return check("firstVisitedPage", value); }
    public WebSessionChecker eventsCounter(final long value) { return check("eventsCounter", value); }
    public WebSessionChecker lastEventDateTime(final long value) { return check("lastEventDateTime", new Date(value).toString()); }
    public WebSessionChecker lastVisitedPage(final Object value) { return check("lastVisitedPage", value); }
    public WebSessionChecker sessionDuration(final Object value) { return check("sessionDuration", value); }
    public WebSessionChecker is_sessionActive(final Object value) { return check("is_sessionActive", value); }
    public WebSessionChecker sessionInactivityDuration(final Object value) { return check("sessionInactivityDuration", value); }
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