package com.hurence.logisland.processor.webAnalytics.util;

import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.StandardRecord;

/**
 * The class represents a web event.
 */
public class WebEvent extends StandardRecord
{
    public static final String SESSION_ID = "sessionId";
    public static final String TIMESTAMP = "h2kTimestamp";
    public static final String VISITED_PAGE = "VISITED_PAGE";
    public static final String CURRENT_CART = "currentCart";
    public static final String USER_ID = "Userid";
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
        this.setField(SESSION_ID, FieldType.STRING, sessionId)
                .setField(USER_ID, FieldType.STRING, userId)
                .setField(TIMESTAMP, FieldType.STRING, timestamp)
                .setField(VISITED_PAGE, FieldType.STRING, url)
                .setField(CURRENT_CART, FieldType.ARRAY, null)
                .setField("record_id", FieldType.STRING, id);
    }

    public WebEvent add(final String name, final String value)
    {
        this.setStringField(name, value);
        return this;
    }
}