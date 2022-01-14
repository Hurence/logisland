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
import com.hurence.logisland.processor.webanalytics.util.DateUtils;
import com.hurence.logisland.processor.webanalytics.util.SessionsCalculator;
import com.hurence.logisland.record.*;

import java.time.*;
import java.util.Collection;
import java.util.Map;

import static com.hurence.logisland.processor.webanalytics.util.Utils.isFieldAssigned;

/**
 * This class represents a session which can be created from a given web-event or an existing session
 * represented by a record.
 */
public class WebSession
        extends RecordItem
        implements Comparable<WebSession> {


    private final InternalFields fieldNames;
    /**
     * Creates a new instance of this class with:
     * - the session identifier set from the web event's session identifier
     * - the first and last timestamps set from the web event's timestamp.
     *
     * @param webEvent the web event to fetch information from.
     */
    public static WebSession fromWebEvent(final Event webEvent,
                      InternalFields fieldsNames,
                      Collection<String> fieldsToCopyToWebSession) {
        WebSession webSession = fromWebEvent(webEvent, fieldsNames);
        if ((fieldsToCopyToWebSession != null) && (!fieldsToCopyToWebSession.isEmpty())) {
            for (final String fieldnameToAdd : fieldsToCopyToWebSession) {
                final Field field = webEvent.record.getField(fieldnameToAdd);
                if (isFieldAssigned(field)) {
                    webSession.getRecord().setField(field);
                }
            }
        }
        return webSession;
    }
    /**
     * Creates a new instance of this class with:
     * - the session identifier set from the web event's session identifier
     * - the first and last timestamps set from the web event's timestamp.
     *
     * @param webEvent the web event to fetch information from.
     */
    public static WebSession fromWebEvent(final Event webEvent,
                                          InternalFields fieldsNames) {

        StandardRecord record = new StandardRecord(IncrementalWebSession.OUTPUT_RECORD_TYPE);
        record.setId(webEvent.getSessionId());
        record.setField(fieldsNames.sessionIdField, FieldType.STRING, webEvent.getSessionId());
        final long eventTimestamp = webEvent.getEpochTimeStampMilli();

        WebSession webSession = new WebSession(record, fieldsNames);
        webSession.setFirstEvent(eventTimestamp);
        webSession.setLastEvent(eventTimestamp);
        webSession.setIsSinglePageVisit(true);
        return webSession;
    }



    /**
     * Returns a new WebSession based on the specified map that represents a web session in elasticsearch.
     *
     * @param sourceAsMap the web session stored in elasticsearch.
     * @param recordType the recordType value for record.
     * @return a new WebSession based on the specified map that represents a web session in elasticsearch.
     */
    public static WebSession fromMap(final Map<String, Object> sourceAsMap,
                                     InternalFields sessionInternalFields,
                                     String recordType) {
        final Record record = new StandardRecord(recordType);
        sourceAsMap.forEach((key, value) ->
        {
                record.setField(key, FieldType.STRING, value);
        });
        record.setId(record.getField(sessionInternalFields.getSessionIdField()).asString());
        return new WebSession(record, sessionInternalFields);
    }


    /**
     * Creates a new instance of this class that wraps the provided record.
     *
     * @param recordRepresentingSession the embedded record.
     */
    public WebSession(final Record recordRepresentingSession,
                      InternalFields fieldNames) {
        super(recordRepresentingSession);
        this.fieldNames = fieldNames;
        record.getAllFields().forEach(field -> {
            String key = field.getName();
            String value = field.asString();
            if (value != null) {
                if (fieldNames.getIsSessionActiveField().equals(key)
                        || fieldNames.getIsSinglePageVisit().equals(key)) {
                    record.setField(key, FieldType.BOOLEAN, Boolean.valueOf(value));
                } else if (fieldNames.getSessionDurationField().equals(key)
                        || fieldNames.getEventsCounterField().equals(key)
                        || fieldNames.getPageviewsCounterField().equals(key)
                        || fieldNames.getTimestampField().equals(key)
                        || fieldNames.getSessionInactivityDurationField().equals(key)
                        || fieldNames.getFirstEventEpochSecondsField().equals(key)
                        || fieldNames.getLastEventEpochSecondsField().equals(key)
                        || fieldNames.getFirstUserVisitEpochSecondsField().equals(key)
                        || FieldDictionary.RECORD_TIME.equals(key)) {
                    record.setField(key, FieldType.LONG, Long.valueOf(value));
                } else {
                    record.setField(key, FieldType.STRING, value);
                }
            }
        });
    }

    public String getSessionId() {
        return this.getStringValue(fieldNames.sessionIdField);
    }

    public String getOriginalSessionId() {
        String sessionsId = getSessionId();
        return SessionsCalculator.extractOrignalSessionsId(sessionsId);
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

    public boolean timestampFromPast(final ZonedDateTime timestamp) {
        return timestamp.compareTo(this.getLastEvent()) < 0;
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
        return fromEpochSecond(getFirstEventEpochSeconds());
    }

    public long getFirstEventEpochSeconds() {
        final Field field = record.getField(fieldNames.firstEventEpochSecondsField);
        if (field == null) {
            // Fallback by parsing the equivalent human readable field.
            return DateUtils.toEpochSecond(record.getField(fieldNames.firstEventDateTimeField).asString());
        }
        return field.asLong();
    }

    public ZonedDateTime getLastEvent() {
        return fromEpochSecond(getLastEventEpochSeconds());
    }

    public long getLastEventEpochSeconds() {
        final Field field = record.getField(fieldNames.lastEventEpochSecondsField);
        if (field == null) {
            // Fallback by parsing the equivalent human readable field.
            return DateUtils.toEpochSecond(record.getField(fieldNames.lastEventDateTimeField).asString());
        }
        return field.asLong();
    }

    public String getSourceOfTraffic() {
        return concatFieldsOfTraffic((String) this.getValue(fieldNames.sourceOfTrafficSourceField),
                (String) this.getValue(fieldNames.sourceOfTrafficMediumField),
                (String) this.getValue(fieldNames.sourceOfTrafficCampaignField),
                (String) this.getValue(fieldNames.sourceOfTrafficKeyWordField),
                (String) this.getValue(fieldNames.sourceOfTrafficContentField));
    }

    @Override
    public String toString() {
        return "WebSession{" + record.getField(fieldNames.firstEventDateTimeField).asString() +
                "-" + record.getField(fieldNames.lastEventDateTimeField).asString() + "}";
    }

    public void setFirstEvent(final long eventTimestamp) {
        this.setDateTimeFieldAndTimestampField(eventTimestamp, fieldNames.firstEventDateTimeField, fieldNames.firstEventEpochSecondsField);
    }

    public void setLastEvent(final long eventTimestamp) {
        this.setDateTimeFieldAndTimestampField(eventTimestamp, fieldNames.lastEventDateTimeField, fieldNames.lastEventEpochSecondsField);
    }

    public void setFirstUserVisitTimestamp(final long timestamp) {
        this.setDateTimeFieldAndTimestampField(timestamp, fieldNames.firstUserVisitDateTimeField, fieldNames.firstUserVisitEpochSecondsField);
    }

    public void setDateTimeFieldAndTimestampField(final long timestamp, final String dateTimeField, final String timestampField) {
        this.record.setField(dateTimeField, FieldType.STRING, DateUtils.toFormattedDate(timestamp));
        this.record.setField(timestampField, FieldType.LONG, timestamp / 1000);
    }

    public void setIsSinglePageVisit(final Boolean isSinglePageVisit) {
        this.record.setField(fieldNames.isSinglePageVisit, FieldType.BOOLEAN, isSinglePageVisit);
    }

    public static class InternalFields {
        private String timestampField;
        private String sessionIdField;
        private String sourceOfTrafficSourceField;
        private String sourceOfTrafficMediumField;
        private String sourceOfTrafficCampaignField;
        private String sourceOfTrafficKeyWordField;
        private String sourceOfTrafficContentField;

        private String eventsCounterField;

        private String firstVisitedPageField;
        private String lastVisitedPageField;
        private String pageviewsCounterField;
        private String firstEventDateTimeField;
        private String lastEventDateTimeField;
        private String firstEventEpochSecondsField;
        private String lastEventEpochSecondsField;

        private String userIdField;

        private String firstUserVisitDateTimeField;
        private String firstUserVisitEpochSecondsField;
        private String isSinglePageVisit;

        private String isSessionActiveField;
        private String sessionInactivityDurationField;
        private String sessionDurationField;
        private String transactionIdsField;

        public InternalFields() { }

        public String getTimestampField() {
            return timestampField;
        }

        public WebSession.InternalFields setTimestampField(String timestampField) {
            this.timestampField = timestampField;
            return this;
        }

        public String getSessionIdField() {
            return sessionIdField;
        }

        public WebSession.InternalFields setSessionIdField(String sessionIdField) {
            this.sessionIdField = sessionIdField;
            return this;
        }

        public String getSourceOfTrafficSourceField() {
            return sourceOfTrafficSourceField;
        }

        public WebSession.InternalFields setSourceOfTrafficSourceField(String sourceOfTrafficSourceField) {
            this.sourceOfTrafficSourceField = sourceOfTrafficSourceField;
            return this;
        }

        public String getSourceOfTrafficMediumField() {
            return sourceOfTrafficMediumField;
        }

        public WebSession.InternalFields setSourceOfTrafficMediumField(String sourceOfTrafficMediumField) {
            this.sourceOfTrafficMediumField = sourceOfTrafficMediumField;
            return this;
        }

        public String getSourceOfTrafficCampaignField() {
            return sourceOfTrafficCampaignField;
        }

        public WebSession.InternalFields setSourceOfTrafficCampaignField(String sourceOfTrafficCampaignField) {
            this.sourceOfTrafficCampaignField = sourceOfTrafficCampaignField;
            return this;
        }

        public String getSourceOfTrafficKeyWordField() {
            return sourceOfTrafficKeyWordField;
        }

        public WebSession.InternalFields setSourceOfTrafficKeyWordField(String sourceOfTrafficKeyWordField) {
            this.sourceOfTrafficKeyWordField = sourceOfTrafficKeyWordField;
            return this;
        }

        public String getSourceOfTrafficContentField() {
            return sourceOfTrafficContentField;
        }

        public WebSession.InternalFields setSourceOfTrafficContentField(String sourceOfTrafficContentField) {
            this.sourceOfTrafficContentField = sourceOfTrafficContentField;
            return this;
        }

        public String getEventsCounterField() {
            return eventsCounterField;
        }

        public InternalFields setEventsCounterField(String eventsCounterField) {
            this.eventsCounterField = eventsCounterField;
            return this;
        }

        public String getFirstVisitedPageField() {
            return firstVisitedPageField;
        }

        public InternalFields setFirstVisitedPageField(String firstVisitedPageField) {
            this.firstVisitedPageField = firstVisitedPageField;
            return this;
        }

        public String getLastVisitedPageField() {
            return lastVisitedPageField;
        }

        public InternalFields setLastVisitedPageField(String lastVisitedPageField) {
            this.lastVisitedPageField = lastVisitedPageField;
            return this;
        }

        public String getPageviewsCounterField() {
            return pageviewsCounterField;
        }

        public InternalFields setPageviewsCounterField(String pageviewsCounterField) {
            this.pageviewsCounterField = pageviewsCounterField;
            return this;
        }

        public String getFirstEventDateTimeField() {
            return firstEventDateTimeField;
        }

        public InternalFields setFirstEventDateTimeField(String firstEventDateTimeField) {
            this.firstEventDateTimeField = firstEventDateTimeField;
            return this;
        }

        public String getLastEventDateTimeField() {
            return lastEventDateTimeField;
        }

        public InternalFields setLastEventDateTimeField(String lastEventDateTimeField) {
            this.lastEventDateTimeField = lastEventDateTimeField;
            return this;
        }

        public String getUserIdField() {
            return userIdField;
        }

        public InternalFields setUserIdField(String userIdField) {
            this.userIdField = userIdField;
            return this;
        }

        public String getFirstUserVisitDateTimeField() {
            return firstUserVisitDateTimeField;
        }

        public InternalFields setFirstUserVisitDateTimeField(String firstUserVisitDateTimeField) {
            this.firstUserVisitDateTimeField = firstUserVisitDateTimeField;
            return this;
        }

        public String getFirstUserVisitEpochSecondsField() {
            return firstUserVisitEpochSecondsField;
        }

        public InternalFields setFirstUserVisitEpochSecondsField(String firstUserVisitEpochSecondsField) {
            this.firstUserVisitEpochSecondsField = firstUserVisitEpochSecondsField;
            return this;
        }

        public String getIsSinglePageVisit() {
            return isSinglePageVisit;
        }

        public InternalFields setIsSinglePageVisit(String isSinglePageVisit) {
            this.isSinglePageVisit = isSinglePageVisit;
            return this;
        }

        public String getIsSessionActiveField() {
            return isSessionActiveField;
        }

        public InternalFields setIsSessionActiveField(String isSessionActiveField) {
            this.isSessionActiveField = isSessionActiveField;
            return this;
        }

        public String getSessionInactivityDurationField() {
            return sessionInactivityDurationField;
        }

        public InternalFields setSessionInactivityDurationField(String sessionInactivityDurationField) {
            this.sessionInactivityDurationField = sessionInactivityDurationField;
            return this;
        }

        public String getSessionDurationField() {
            return sessionDurationField;
        }

        public InternalFields setSessionDurationField(String sessionDurationField) {
            this.sessionDurationField = sessionDurationField;
            return this;
        }

        public String getTransactionIdsField() {
            return transactionIdsField;
        }

        public InternalFields setTransactionIdsField(String transactionIdsField) {
            this.transactionIdsField = transactionIdsField;
            return this;
        }

        public String getFirstEventEpochSecondsField() {
            return firstEventEpochSecondsField;
        }

        public InternalFields setFirstEventEpochSecondsField(String firstEventEpochSecondsField) {
            this.firstEventEpochSecondsField = firstEventEpochSecondsField;
            return this;
        }

        public String getLastEventEpochSecondsField() {
            return lastEventEpochSecondsField;
        }

        public InternalFields setLastEventEpochSecondsField(String lastEventEpochSecondsField) {
            this.lastEventEpochSecondsField = lastEventEpochSecondsField;
            return this;
        }
    }
}
