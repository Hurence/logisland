package com.hurence.logisland.processor.webAnalytics.modele;

import com.hurence.logisland.processor.webAnalytics.IncrementalWebSession;
import com.hurence.logisland.processor.webAnalytics.util.DateUtils;
import com.hurence.logisland.record.Field;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;

import java.time.*;
import java.util.ArrayList;
import java.util.Collection;

import static com.hurence.logisland.processor.webAnalytics.util.Utils.isFieldAssigned;

/**
 * This class represents a session which can be created from a given web-event or an existing session
 * represented by a record.
 */
public class WebSession
        extends RecordItem
        implements Comparable<WebSession> {

    private final IncrementalWebSession processor;
    /**
     * Creates a new instance of this class with:
     * - the session identifier set from the web event's session identifier
     * - the first and last timestamps set from the web event's timestamp.
     *
     * @param webEvent the web event to fetch information from.
     */
    public WebSession(final WebEvent webEvent, IncrementalWebSession processor) {
        super(new StandardRecord(IncrementalWebSession.OUTPUT_RECORD_TYPE));
        this.processor = processor;
        this.record.setId(webEvent.getSessionId());
        this.record.setField(processor._SESSION_ID_FIELD, FieldType.STRING, webEvent.getSessionId());
        final long eventTimestamp = webEvent.getTimeStampAsLong();
        this.setFirstEvent(eventTimestamp);
        this.setLastEvent(eventTimestamp);
        if ((processor._FIELDS_TO_RETURN != null) && (!processor._FIELDS_TO_RETURN.isEmpty())) {
            for (final String fieldnameToAdd : processor._FIELDS_TO_RETURN) {
                final Field field = webEvent.record.getField(fieldnameToAdd);
                if (isFieldAssigned(field)) {
                    record.setField(field); // Field immutable.
                }
            }
        }
    }

    public String getSessionId() {
        return this.getStringValue(processor._SESSION_ID_FIELD);
    }

    /**
     * Creates a new instance of this class that wraps the provided record.
     *
     * @param saveSession the embedded record.
     */
    public WebSession(final Record saveSession, IncrementalWebSession processor) {
        super(saveSession);
        this.processor = processor;
    }

    /**
     * Adds the specified event to this sessions by updating fields such as lastVisitedPage.
     *
     * @param event the event to apply.
     */
    public void add(final WebEvent event) {
        // Handle case where web-event is older that first event of session.
        // In case there are few events older than the current web-session, all those events must
        // be taken into account despite the fact that setting the timestamp of the first event
        // will 'hide' the next ones.

        final Field eventTimestampField = event.record.getField(processor._TIMESTAMP_FIELD);
        final long eventTimestamp = eventTimestampField.asLong();

        // Sanity check.
        final Field lastEventField = record.getField(IncrementalWebSession._LAST_EVENT_EPOCH_FIELD);
        if (lastEventField != null) {
            final long lastEvent = lastEventField.asLong();

            if (lastEvent > 0 && eventTimestamp > 0 && eventTimestamp < lastEvent) {
                // The event is older that current web session; ignore.
                return;
            }
        }

        // EVENTS_COUNTER
        Field field = record.getField(processor._EVENTS_COUNTER_FIELD);
        long eventsCounter = field == null ? 0 : field.asLong();
        eventsCounter++;
        record.setField(processor._EVENTS_COUNTER_FIELD, FieldType.LONG, eventsCounter);

        // TIMESTAMP
        // Set the session create timestamp to the create timestamp of the first event on the session.
        long creationTimestamp;
        field = record.getField(processor._TIMESTAMP_FIELD);
        if (!isFieldAssigned(field)) {
            record.setField(processor._TIMESTAMP_FIELD, FieldType.LONG, eventTimestamp);
            creationTimestamp = eventTimestamp;
        } else {
            creationTimestamp = field.asLong();
        }

        final Field visitedPage = event.record.getField(processor._VISITED_PAGE_FIELD);
        // FIRST_VISITED_PAGE
        if (!isFieldAssigned(record.getField(processor._FIRST_VISITED_PAGE_FIELD))) {
            record.setField(processor._FIRST_VISITED_PAGE_FIELD, FieldType.STRING, visitedPage.asString());
        }

        // LAST_VISITED_PAGE
        if (isFieldAssigned(visitedPage)) {
            record.setField(processor._LAST_VISITED_PAGE_FIELD, FieldType.STRING, visitedPage.asString());
        }

        // FIRST_EVENT_DATETIME
        if (!isFieldAssigned(record.getField(processor._FIRST_EVENT_DATETIME_FIELD))) {
            this.setFirstEvent(eventTimestamp);
        }

        // LAST_EVENT_DATETIME
        if (isFieldAssigned(eventTimestampField)) {
            this.setLastEvent(eventTimestamp);
        }

        // USERID
        // Add the userid record if available
        final Field userIdField = record.getField(processor._USERID_FIELD);
        if ((!isFieldAssigned(userIdField) || "undefined".equalsIgnoreCase(userIdField.asString()))
                && isFieldAssigned(event.record.getField(processor._USERID_FIELD))) {
            final String userId = event.record.getField(processor._USERID_FIELD).asString();
            if (userId != null) {
                record.setField(processor._USERID_FIELD, FieldType.STRING, userId);
            }
        }

        LocalDateTime now = LocalDateTime.now();
        LocalDateTime eventLocalDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(eventTimestamp),
                ZoneId.systemDefault());

        // Compute session inactivity duration (in milliseconds)
        final long sessionInactivityDuration = Duration.between(eventLocalDateTime, now).getSeconds();

        if (sessionInactivityDuration > processor._SESSION_INACTIVITY_TIMEOUT) {
            // Mark the session as closed
            record.setField(processor._IS_SESSION_ACTIVE_FIELD, FieldType.BOOLEAN, Boolean.FALSE);

            // Max out the sessionInactivityDuration - only pertinent in case of topic rewind.
            record.setField(processor._SESSION_INACTIVITY_DURATION_FIELD, FieldType.LONG, processor._SESSION_INACTIVITY_TIMEOUT);
        } else {
            record.setField(processor._IS_SESSION_ACTIVE_FIELD, FieldType.BOOLEAN, Boolean.TRUE);
        }

        final long sessionDuration = Duration.between(Instant.ofEpochMilli(creationTimestamp),
                Instant.ofEpochMilli(eventTimestamp)).getSeconds();
        if (sessionDuration > 0) {
            record.setField(processor._SESSION_DURATION_FIELD, FieldType.LONG, sessionDuration);
        }

        // Extra
        final Field transactionIdField = event.record.getField("transactionId");
        if (isFieldAssigned(transactionIdField)
                && (!"undefined".equalsIgnoreCase(transactionIdField.asString()))
                && (!transactionIdField.asString().isEmpty())) {
            final Field transactionIdsField = this.record.getField("transactionIds");
            Collection<String> transactionIds;
            if (!isFieldAssigned(transactionIdField)) {
                transactionIds = (Collection<String>) transactionIdsField.getRawValue();
            } else {
                transactionIds = new ArrayList<>();
                this.record.setField(processor._TRANSACTION_IDS, FieldType.ARRAY, transactionIds);
            }
            transactionIds.add(transactionIdField.asString());
        }

        if (!record.isValid()) {
            record.getFieldsEntrySet().forEach(entry ->
            {
                final Field f = entry.getValue();
                processor.debug("INVALID field type=%s, class=%s", f.getType(), f.getRawValue().getClass());
            });
        }
    }

    /**
     * Returns {@code true} if the specified timestamp is enclosed within the first and last timestamp of this
     * session; {@code false} otherwise.
     *
     * @param timestamp the timestamp to check against this session.
     * @return {@code true} if the specified timestamp is enclosed within the first and last timestamp of this
     * session; {@code false} otherwise.
     */
    public boolean containsTimestamp(final ZonedDateTime timestamp) {
        return this.getFirstEvent().compareTo(timestamp) <= 0 && timestamp.compareTo(this.getLastEvent()) <= 0;
    }

    @Override
    public int compareTo(final WebSession session) {
        if (this.getLastEvent().compareTo(session.getFirstEvent()) < 0) {
            return -1;
        } else if (session.getLastEvent().compareTo(this.getFirstEvent()) < 0) {
            return 1;
        } else {
            throw new IllegalStateException("Two sessions can no share same timestamp:" + this.toString()
                    + " vs " + session.toString());
        }
    }

    public ZonedDateTime getFirstEvent() {
        final Field field = record.getField(IncrementalWebSession._FIRST_EVENT_EPOCH_FIELD);
        if (field == null) {
            // Fallback by parsing the equivalent human readable field.
            return fromEpoch(DateUtils.toEpoch(record.getField(processor._FIRST_EVENT_DATETIME_FIELD).asString()));
        }
        return fromEpoch(field.asLong() * 1000);
    }

    private void setFirstEvent(final long eventTimestamp) {
        this.record.setField(processor._FIRST_EVENT_DATETIME_FIELD, FieldType.STRING, DateUtils.toFormattedDate(eventTimestamp));
        this.record.setField(IncrementalWebSession._FIRST_EVENT_EPOCH_FIELD, FieldType.LONG, eventTimestamp / 1000);
    }

    private void setLastEvent(final long eventTimestamp) {
        this.record.setField(processor._LAST_EVENT_DATETIME_FIELD, FieldType.STRING, DateUtils.toFormattedDate(eventTimestamp));
        this.record.setField(IncrementalWebSession._LAST_EVENT_EPOCH_FIELD, FieldType.LONG, eventTimestamp / 1000);
    }

    public ZonedDateTime getLastEvent() {
        final Field field = record.getField(IncrementalWebSession._LAST_EVENT_EPOCH_FIELD);
        if (field == null) {
            // Fallback by parsing the equivalent human readable field.
            return fromEpoch(DateUtils.toEpoch(record.getField(processor._LAST_EVENT_DATETIME_FIELD).asString()));
        }
        return fromEpoch(field.asLong() * 1000);
    }

    public String getSourceOfTraffic() {
        return concatFieldsOfTraffic((String) this.getValue(processor._SOT_SOURCE_FIELD),
                (String) this.getValue(processor._SOT_MEDIUM_FIELD),
                (String) this.getValue(processor._SOT_CAMPAIGN_FIELD),
                (String) this.getValue(processor._SOT_KEYWORD_FIELD),
                (String) this.getValue(processor._SOT_CONTENT_FIELD));
    }

    @Override
    public String toString() {
        return "WebSession{" + record.getField(processor._FIRST_EVENT_DATETIME_FIELD).asString() +
                "-" + record.getField(processor._LAST_EVENT_DATETIME_FIELD).asString() + "}";
    }
}
