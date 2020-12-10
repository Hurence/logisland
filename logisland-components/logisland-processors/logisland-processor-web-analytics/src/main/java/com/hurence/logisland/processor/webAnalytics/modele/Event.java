package com.hurence.logisland.processor.webAnalytics.modele;

import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZonedDateTime;

/**
 * This class represents a web event that can be optionally renamed if a new session should have
 * been created.
 */
public class Event
        extends RecordItem
        implements Comparable<Event> {

    private static Logger logger = LoggerFactory.getLogger(Event.class);

    /**
     * The timestamp to sort web event from.
     */
    private final ZonedDateTime timestamp;
    private final InternalFields fieldsNames;

    public Event(final Record record, InternalFields fieldsNames) {
        super(record);
        this.fieldsNames = fieldsNames;
        this.timestamp = this.fromEpochMilli(getEpochTimeStampMilli());
    }

    public long getEpochTimeStampMilli() {
        return record.getField(fieldsNames.getTimestampField()).asLong();
    }

    public long getEpochTimeStampSeconds() {
        return record.getField(fieldsNames.getTimestampField()).asLong() / 1000L;
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
        return this.getStringValue(fieldsNames.visitedPageField);
    }

    public String getSessionId() {
        return this.getStringValue(fieldsNames.sessionIdField);
    }

    public void setSessionId(final String sessionId) {
        logger.debug("change sessionId of event " + this.record.getId() + " from " + getSessionId() + " to " + sessionId);
        this.record.setField("originalSessionId", FieldType.STRING, getSessionId());
        this.record.setField(fieldsNames.sessionIdField, FieldType.STRING, sessionId);
    }

    public String getSourceOfTraffic() {
        return concatFieldsOfTraffic(this.getStringValue(fieldsNames.sourceOffTrafficSourceField),
                this.getStringValue(fieldsNames.sourceOffTrafficMediumField),
                this.getStringValue(fieldsNames.sourceOffTrafficCampaignField),
                this.getStringValue(fieldsNames.sourceOffTrafficKeyWordField),
                this.getStringValue(fieldsNames.sourceOffTrafficContentField));
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

        result.setField(fieldsNames.sessionIdField, FieldType.STRING, this.getSessionId());

        return result;
    }

    @Override
    public String toString() {
        return "WebEvent{sessionId='" + this.getSessionId() + "', timestamp=" + timestamp + '}';
    }

    public static class InternalFields {
        private String timestampField;
        private String visitedPageField;
        private String sessionIdField;
        private String sourceOffTrafficSourceField;
        private String sourceOffTrafficMediumField;
        private String sourceOffTrafficCampaignField;
        private String sourceOffTrafficKeyWordField;
        private String sourceOffTrafficContentField;
        private String newSessionReasonField;
        private String userIdField;


        public InternalFields() { }

        public String getTimestampField() {
            return timestampField;
        }

        public InternalFields setTimestampField(String timestampField) {
            this.timestampField = timestampField;
            return this;
        }

        public String getVisitedPageField() {
            return visitedPageField;
        }

        public InternalFields setVisitedPageField(String visitedPageField) {
            this.visitedPageField = visitedPageField;
            return this;
        }

        public String getSessionIdField() {
            return sessionIdField;
        }

        public InternalFields setSessionIdField(String sessionIdField) {
            this.sessionIdField = sessionIdField;
            return this;
        }

        public String getSourceOffTrafficSourceField() {
            return sourceOffTrafficSourceField;
        }

        public InternalFields setSourceOffTrafficSourceField(String sourceOffTrafficSourceField) {
            this.sourceOffTrafficSourceField = sourceOffTrafficSourceField;
            return this;
        }

        public String getSourceOffTrafficMediumField() {
            return sourceOffTrafficMediumField;
        }

        public InternalFields setSourceOffTrafficMediumField(String sourceOffTrafficMediumField) {
            this.sourceOffTrafficMediumField = sourceOffTrafficMediumField;
            return this;
        }

        public String getSourceOffTrafficCampaignField() {
            return sourceOffTrafficCampaignField;
        }

        public InternalFields setSourceOffTrafficCampaignField(String sourceOffTrafficCampaignField) {
            this.sourceOffTrafficCampaignField = sourceOffTrafficCampaignField;
            return this;
        }

        public String getSourceOffTrafficKeyWordField() {
            return sourceOffTrafficKeyWordField;
        }

        public InternalFields setSourceOffTrafficKeyWordField(String sourceOffTrafficKeyWordField) {
            this.sourceOffTrafficKeyWordField = sourceOffTrafficKeyWordField;
            return this;
        }

        public String getSourceOffTrafficContentField() {
            return sourceOffTrafficContentField;
        }

        public InternalFields setSourceOffTrafficContentField(String sourceOffTrafficContentField) {
            this.sourceOffTrafficContentField = sourceOffTrafficContentField;
            return this;
        }

        public String getNewSessionReasonField() {
            return newSessionReasonField;
        }

        public InternalFields setNewSessionReasonField(String newSessionReasonField) {
            this.newSessionReasonField = newSessionReasonField;
            return this;
        }

        public String getUserIdField() {
            return userIdField;
        }

        public InternalFields setUserIdField(String userIdField) {
            this.userIdField = userIdField;
            return this;
        }
    }
}
