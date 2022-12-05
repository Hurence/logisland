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

import com.hurence.logisland.processor.webanalytics.IncrementalWebSession;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZonedDateTime;
import java.util.Map;

/**
 * This class represents a web event that can be optionally renamed if a new session should have
 * been created.
 */
public class Event
        extends RecordItem
        implements Comparable<Event> {

    private static Logger logger = LoggerFactory.getLogger(Event.class);

    /**
     * return a new Event based on the specified map that represents a web event in elasticsearch.
     *
     * @param sourceAsMap the event stored in elasticsearch.
     * @param recordType the recordType value for record.
     * @return a new Event based on the specified map that represents a web event in elasticsearch.
     */
    public static Event fromMap(final Map<String, Object> sourceAsMap, InternalFields eventsInternalFields, String recordType) {
        final Record record = new StandardRecord(recordType);
        sourceAsMap.forEach((key, value) -> {
            record.setField(key, FieldType.STRING, value);
        });
        return new Event(record, eventsInternalFields);
    }


    /**
     * The timestamp to sort web event from.
     */
    private final ZonedDateTime timestamp;
    private final InternalFields fieldNames;

    public Event(final Record record, InternalFields fieldNames) {
        super(record);
        this.fieldNames = fieldNames;
        this.timestamp = this.fromEpochMilli(getEpochTimeStampMilli());
    }

    public long getEpochTimeStampMilli() {
        return record.getField(fieldNames.getTimestampField()).asLong();
    }

    public long getEpochTimeStampSeconds() {
        return record.getField(fieldNames.getTimestampField()).asLong() / 1000L;
    }

    @Override
    public int compareTo(final Event webEvent) {
        if (this.getId() != null && webEvent.getId() != null) {
            int idDiff = this.getId().compareTo(webEvent.getId());
            if (idDiff == 0) {
                return 0;
            } else {
                int timestampDiff = this.timestamp.compareTo(webEvent.getTimestamp());
                if (timestampDiff != 0) return timestampDiff;
                return idDiff;
            }
        }
        int timestampDiff = this.timestamp.compareTo(webEvent.getTimestamp());
        if (timestampDiff != 0) return timestampDiff;
        if (this.getId() == null && webEvent.getId() == null) {
            return 0;
        }
        if (this.getId() == null) {
            return -1;
        } else {
            return 1;
        }
    }

    public String getId() {
        return this.getRecord().getId();
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
        return this.getStringValue(fieldNames.visitedPageField);
    }

    public void setSessionId(final String sessionId) {
        if (getSessionId().equals(sessionId)) return;//do nothing if not changing
        logger.debug("change sessionId of event " + this.record.getId() + " from " + getSessionId() + " to " + sessionId);
        this.record.setField(fieldNames.originalSessionIdField, FieldType.STRING, calculOriginalSessionId(sessionId));
        this.record.setField(fieldNames.sessionIdField, FieldType.STRING, sessionId);
    }

    private String calculOriginalSessionId(String sessionId) {
        final String[] sessionIdSplitted = sessionId.split(IncrementalWebSession.EXTRA_SESSION_DELIMITER);
        return sessionIdSplitted[0];
    }

    public String getSessionId() {
        return this.getStringValue(fieldNames.sessionIdField);
    }

    /**
     * May be null
     * @return
     */
    public String getOriginalSessionId() {
        return this.getStringValue(fieldNames.originalSessionIdField);
    }

    public String getOriginalSessionIdOrSessionId() {
        String orignalSessionId = getOriginalSessionId();
        if (orignalSessionId==null) return getSessionId();
        return orignalSessionId;
    }


    public String getSourceOfTraffic() {
        return concatFieldsOfTraffic(this.getStringValue(fieldNames.sourceOfTrafficSourceField),
                this.getStringValue(fieldNames.sourceOfTrafficMediumField),
                this.getStringValue(fieldNames.sourceOfTrafficCampaignField),
                this.getStringValue(fieldNames.sourceOfTrafficKeyWordField),
                this.getStringValue(fieldNames.sourceOfTrafficContentField));
    }

    /**
     * Returns a copy of the inner record.
     *
     * @return a copy of the inner record.
     */
    public Record cloneRecord() {
        return new StandardRecord(this.record);
    }

    @Override
    public String toString() {
        return "WebEvent{sessionId='" + this.getSessionId() + "', timestamp=" + timestamp + '}';
    }

    public static class InternalFields {
        private String timestampField;
        private String visitedPageField;
        private String sessionIdField;
        private String originalSessionIdField;
        private String sourceOfTrafficSourceField;
        private String sourceOfTrafficMediumField;
        private String sourceOfTrafficCampaignField;
        private String sourceOfTrafficKeyWordField;
        private String sourceOfTrafficContentField;
        private String newSessionReasonField;
        private String userIdField;
        private String transactionIdField;
        private String transactionIdsField;


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
            return sourceOfTrafficSourceField;
        }

        public InternalFields setSourceOfTrafficSourceField(String sourceOfTrafficSourceField) {
            this.sourceOfTrafficSourceField = sourceOfTrafficSourceField;
            return this;
        }

        public String getSourceOffTrafficMediumField() {
            return sourceOfTrafficMediumField;
        }

        public InternalFields setSourceOfTrafficMediumField(String sourceOfTrafficMediumField) {
            this.sourceOfTrafficMediumField = sourceOfTrafficMediumField;
            return this;
        }

        public String getSourceOffTrafficCampaignField() {
            return sourceOfTrafficCampaignField;
        }

        public InternalFields setSourceOfTrafficCampaignField(String sourceOfTrafficCampaignField) {
            this.sourceOfTrafficCampaignField = sourceOfTrafficCampaignField;
            return this;
        }

        public String getSourceOffTrafficKeyWordField() {
            return sourceOfTrafficKeyWordField;
        }

        public InternalFields setSourceOfTrafficKeyWordField(String sourceOfTrafficKeyWordField) {
            this.sourceOfTrafficKeyWordField = sourceOfTrafficKeyWordField;
            return this;
        }

        public String getSourceOffTrafficContentField() {
            return sourceOfTrafficContentField;
        }

        public InternalFields setSourceOfTrafficContentField(String sourceOfTrafficContentField) {
            this.sourceOfTrafficContentField = sourceOfTrafficContentField;
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

        public String getOriginalSessionIdField() {
            return originalSessionIdField;
        }

        public InternalFields setOriginalSessionIdField(String originalSessionIdField) {
            this.originalSessionIdField = originalSessionIdField;
            return this;
        }

        public String getTransactionIdField() {
            return transactionIdField;
        }

        public InternalFields setTransactionIdField(String transactionId) {
            this.transactionIdField = transactionId;
            return this;
        }

        public String getTransactionIdsField() {
            return transactionIdsField;
        }

        public InternalFields setTransactionIdsField(String transactionIdsField) {
            this.transactionIdsField = transactionIdsField;
            return this;
        }
    }
}
