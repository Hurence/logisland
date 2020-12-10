package com.hurence.logisland.processor.webAnalytics.modele;

import com.hurence.logisland.processor.webAnalytics.IncrementalWebSession;

public class TestMappings {



    public static Event.InternalFields eventsInternalFields = new Event.InternalFields()
            .setSessionIdField("sessionId")
                .setTimestampField("h2kTimestamp")
                .setVisitedPageField("VISITED_PAGE")
            .setSourceOffTrafficCampaignField("source_of_traffic" +
                    IncrementalWebSession.SOURCE_OF_TRAFFIC_FIELD_CAMPAIGN)
            .setSourceOffTrafficContentField("source_of_traffic" +
                    IncrementalWebSession.SOURCE_OF_TRAFFIC_FIELD_CONTENT)
            .setSourceOffTrafficKeyWordField("source_of_traffic" +
                    IncrementalWebSession.SOURCE_OF_TRAFFIC_FIELD_KEYWORD)
            .setSourceOffTrafficMediumField("source_of_traffic" +
                    IncrementalWebSession.SOURCE_OF_TRAFFIC_FIELD_MEDIUM)
            .setSourceOffTrafficSourceField("source_of_traffic" +
                    IncrementalWebSession.SOURCE_OF_TRAFFIC_FIELD_SOURCE)
                .setNewSessionReasonField("reasonForNewSession")
                .setUserIdField("Userid");

    public static WebSession.InternalFields sessionInternalFields = new WebSession.InternalFields()
            .setSessionIdField("sessionId")
                .setTimestampField("h2kTimestamp")
                .setSourceOffTrafficCampaignField("source_of_traffic" +
                        IncrementalWebSession.SOURCE_OF_TRAFFIC_FIELD_CAMPAIGN)
                .setSourceOffTrafficContentField("source_of_traffic" +
                        IncrementalWebSession.SOURCE_OF_TRAFFIC_FIELD_CONTENT)
                .setSourceOffTrafficKeyWordField("source_of_traffic" +
                        IncrementalWebSession.SOURCE_OF_TRAFFIC_FIELD_KEYWORD)
                .setSourceOffTrafficMediumField("source_of_traffic" +
                        IncrementalWebSession.SOURCE_OF_TRAFFIC_FIELD_MEDIUM)
                .setSourceOffTrafficSourceField("source_of_traffic" +
                        IncrementalWebSession.SOURCE_OF_TRAFFIC_FIELD_SOURCE)
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
