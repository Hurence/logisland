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
package com.hurence.logisland.webanalytics.test.util;

import com.hurence.logisland.processor.webanalytics.IncrementalWebSession;
import com.hurence.logisland.processor.webanalytics.modele.Event;
import com.hurence.logisland.processor.webanalytics.modele.WebSession;

import static com.hurence.logisland.processor.webanalytics.IncrementalWebSession.DEFAULT_SOURCE_OF_TRAFFIC_PREFIX;

public class TestMappings {


    public static Event.InternalFields eventsInternalFields = new Event.InternalFields()
            .setSessionIdField("sessionId")
            .setTimestampField("h2kTimestamp")
            .setVisitedPageField("location")
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
