package com.hurence.logisland.processor.webAnalytics.modele;

import com.hurence.logisland.processor.webAnalytics.IncrementalWebSession;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;

import java.time.ZonedDateTime;

/**
 * This class represents a web event that can be optionally renamed if a new session should have
 * been created.
 */
public class Event
        extends RecordItem
        implements Comparable<Event> {

    /**
     * The timestamp to sort web event from.
     */
    private final ZonedDateTime timestamp;
    private final IncrementalWebSession processor;

    public Event(final Record record, IncrementalWebSession processor) {
        super(record);
        this.processor = processor;
        this.timestamp = this.fromEpoch(getEpochTimeStampMilli());
    }

    public long getEpochTimeStampMilli() {
        return record.getField(processor._TIMESTAMP_FIELD).asLong();
    }

    public long getEpochTimeStampSeconds() {
        return record.getField(processor._TIMESTAMP_FIELD).asLong() / 1000L;
    }

    @Override
    public int compareTo(final Event webEvent) {
        return this.timestamp.compareTo(webEvent.getTimestamp());
    }

    /**
     * Returns the timestamp of this event.
     *
     * @return the timestamp of this event.
     */
    public ZonedDateTime getTimestamp() {
        return this.timestamp;
    }

    public String getVisitedPage() {
        return this.getStringValue(processor._VISITED_PAGE_FIELD);
    }

    public String getSessionId() {
        return this.getStringValue(processor._SESSION_ID_FIELD);
    }

    public void setSessionId(final String sessionId) {
        processor.debug("Rename session of event " + this.record.getId() + " from " + getSessionId() + " to " + sessionId);
        this.record.setField("originalSessionId", FieldType.STRING, getSessionId());
        this.record.setField(processor._SESSION_ID_FIELD, FieldType.STRING, sessionId);
    }

    public String getSourceOfTraffic() {
        return concatFieldsOfTraffic(this.getStringValue(processor._SOT_SOURCE_FIELD),
                this.getStringValue(processor._SOT_MEDIUM_FIELD),
                this.getStringValue(processor._SOT_CAMPAIGN_FIELD),
                this.getStringValue(processor._SOT_KEYWORD_FIELD),
                this.getStringValue(processor._SOT_CONTENT_FIELD));
    }

    /**
     * Returns a copy of the inner record.
     *
     * @return a copy of the inner record.
     */
    public Record cloneRecord() {
        final Record result = new StandardRecord();
        this.record.getFieldsEntrySet()
                .forEach(entry ->
                {
                    if (entry.getValue() != null) {
                        result.setField(entry.getValue());
                    }
                });

        result.setField(processor._SESSION_ID_FIELD, FieldType.STRING, this.getSessionId());

        return result;
    }

    @Override
    public String toString() {
        return "WebEvent{sessionId='" + this.getSessionId() + "', timestamp=" + timestamp + '}';
    }
}
