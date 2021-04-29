package com.hurence.logisland.webanalytics.test.util;

import com.hurence.logisland.processor.webanalytics.IncrementalWebSession;
import com.hurence.logisland.processor.webanalytics.modele.Event;
import com.hurence.logisland.processor.webanalytics.modele.WebSession;

import static com.hurence.logisland.processor.webanalytics.IncrementalWebSession.DEFAULT_SOURCE_OF_TRAFFIC_PREFIX;

public class TestMappings {


    public static Event.InternalFields eventsInternalFields = new Event.InternalFields()
            .setSessionIdField("sessionId")
            .setTimestampField("h2kTimestamp")
            .setVisitedPageField("VISITED_PAGE")
            .setSourceOffTrafficCampaignField(IncrementalWebSession.DEFAULT_SOURCE_OF_TRAFFIC_PREFIX + IncrementalWebSession.SOURCE_OF_TRAFFIC_FIELD_CAMPAIGN)
            .setSourceOffTrafficContentField(IncrementalWebSession.DEFAULT_SOURCE_OF_TRAFFIC_PREFIX + IncrementalWebSession.SOURCE_OF_TRAFFIC_FIELD_CONTENT)
            .setSourceOffTrafficKeyWordField(IncrementalWebSession.DEFAULT_SOURCE_OF_TRAFFIC_PREFIX + IncrementalWebSession.SOURCE_OF_TRAFFIC_FIELD_KEYWORD)
            .setSourceOffTrafficMediumField(IncrementalWebSession.DEFAULT_SOURCE_OF_TRAFFIC_PREFIX + IncrementalWebSession.SOURCE_OF_TRAFFIC_FIELD_MEDIUM)
            .setSourceOffTrafficSourceField(IncrementalWebSession.DEFAULT_SOURCE_OF_TRAFFIC_PREFIX + IncrementalWebSession.SOURCE_OF_TRAFFIC_FIELD_SOURCE)
            .setNewSessionReasonField("reasonForNewSession")
            .setUserIdField("Userid")
            .setOriginalSessionIdField("originalSessionId")
            .setTransactionIdField("transactionId")
            .setTransactionIdsField("transactionIds");


    public static WebSession.InternalFields sessionInternalFields = new WebSession.InternalFields()
            .setSessionIdField("sessionId")
                .setTimestampField("h2kTimestamp")
                .setSourceOffTrafficCampaignField(IncrementalWebSession.DEFAULT_SOURCE_OF_TRAFFIC_PREFIX + IncrementalWebSession.SOURCE_OF_TRAFFIC_FIELD_CAMPAIGN)
                .setSourceOffTrafficContentField(IncrementalWebSession.DEFAULT_SOURCE_OF_TRAFFIC_PREFIX + IncrementalWebSession.SOURCE_OF_TRAFFIC_FIELD_CONTENT)
                .setSourceOffTrafficKeyWordField(IncrementalWebSession.DEFAULT_SOURCE_OF_TRAFFIC_PREFIX + IncrementalWebSession.SOURCE_OF_TRAFFIC_FIELD_KEYWORD)
                .setSourceOffTrafficMediumField(IncrementalWebSession.DEFAULT_SOURCE_OF_TRAFFIC_PREFIX + IncrementalWebSession.SOURCE_OF_TRAFFIC_FIELD_MEDIUM)
                .setSourceOffTrafficSourceField(IncrementalWebSession.DEFAULT_SOURCE_OF_TRAFFIC_PREFIX + IncrementalWebSession.SOURCE_OF_TRAFFIC_FIELD_SOURCE)
                .setIsSessionActiveField("is_sessionActive")
                .setSessionDurationField("sessionDuration")
                .setSessionInactivityDurationField("sessionInactivityDuration")
                .setEventsCounterField("eventsCounter")
                .setFirstEventDateTimeField("firstEventDateTime")
                .setFirstEventEpochSecondsField("firstEventEpochSeconds")
                .setFirstVisitedPageField("firstVisitedPage")
                .setLastEventDateTimeField("lastEventDateTime")
                .setLastEventEpochSecondsField("lastEventEpochSeconds")
                .setLastVisitedPageField("lastVisitedPage")
                .setTransactionIdsField("transactionIds")
                .setUserIdField("Userid");
}
