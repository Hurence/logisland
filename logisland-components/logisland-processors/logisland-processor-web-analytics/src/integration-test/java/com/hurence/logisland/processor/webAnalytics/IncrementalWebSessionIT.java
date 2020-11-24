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
package com.hurence.logisland.processor.webAnalytics;

import com.hurence.junit5.extension.EsDockerExtension;
import com.hurence.logisland.classloading.PluginProxy;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.processor.webAnalytics.util.WebEvent;
import com.hurence.logisland.processor.webAnalytics.util.WebSessionChecker;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.service.elasticsearch.ElasticsearchClientService;
import com.hurence.logisland.service.elasticsearch.Elasticsearch_2_4_0_ClientService;
import com.hurence.logisland.util.runner.MockRecord;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.DockerComposeContainer;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.hurence.logisland.processor.webAnalytics.util.WebEvent.SESSION_INDEX;

/**
 * Test incremental web-session processor.
 */
@ExtendWith({EsDockerExtension.class})
public class IncrementalWebSessionIT
{
    private static Logger logger = LoggerFactory.getLogger(IncrementalWebSessionIT.class);



    @BeforeEach
    public void clean(Client esClient) throws InterruptedException, ExecutionException {
        ClusterHealthRequest clHealtRequest = new ClusterHealthRequest();
        ClusterHealthResponse response = esClient.admin().cluster().health(clHealtRequest).get();
        Set<String> indices = response.getIndices().keySet();

        if (!indices.isEmpty()) {
            DeleteIndexRequest deleteRequest = new DeleteIndexRequest(indices.toArray(new String[0]));
            Assert.assertTrue(esClient.admin().indices().delete(deleteRequest).get().isAcknowledged());
        }

        PutIndexTemplateRequest templateRequest = new PutIndexTemplateRequest(
                "my-template"
        ).template("*");
        templateRequest.settings(Settings.builder()
                .put("index.number_of_shards", 5)
                .put("index.number_of_replicas", 0)
        );
        AcknowledgedResponse putTemplateResponse = esClient.admin().indices().putTemplate(templateRequest).actionGet();
        logger.info("putTemplateResponse is " + putTemplateResponse);
    }

    private MockRecord getFirstRecordWithId(final String id, final List<MockRecord> records)
    {
        return records.stream().filter(record -> record.getId().equals(id)).findFirst().get();
    }


    private final long SESSION_TIMEOUT = 1800L;
    private final String MAPPING_COLLECTION = "openanalytics_mappings";
    private ElasticsearchClientService elasticsearchClientService;
    /**
     * Creates a new TestRunner set with the appropriate properties.
     *
     * @return a new TestRunner set with the appropriate properties.
     *
     * @throws InitializationException in case the runner could not be instantiated.
     */
    private TestRunner newTestRunner(DockerComposeContainer container)
            throws InitializationException
    {
        final TestRunner runner = TestRunners.newTestRunner(new IncrementalWebSession());
        final String FIELDS_TO_RETURN = Stream.of("partyId",  "B2BUnit").collect(Collectors.joining(","));

//        fields.to.return: partyId,Company,remoteHost,tagOrigin,sourceOrigin,spamOrigin,referer,userAgentString,utm_source,utm_campaign,utm_medium,utm_content,utm_term,alert_match_name,alert_match_query,referer_hostname,DeviceClass,AgentName,ImportanceCode,B2BUnit,libelle_zone,Userid,customer_category,source_of_traffic_source,source_of_traffic_medium,source_of_traffic_keyword,source_of_traffic_campaign,source_of_traffic_organic_search,source_of_traffic_content,source_of_traffic_referral_path,websessionIndex

        configureElasticsearchClientService(runner, container);
        runner.setProperty(IncrementalWebSession.ELASTICSEARCH_CLIENT_SERVICE, "elasticsearchClient");
        runner.setProperty(IncrementalWebSession.ES_SESSION_INDEX_FIELD, SESSION_INDEX);
        runner.setProperty(IncrementalWebSession.ES_SESSION_TYPE_NAME, "sessions");
        runner.setProperty(IncrementalWebSession.ES_EVENT_INDEX_PREFIX, "openanalytics_webevents");
        runner.setProperty(IncrementalWebSession.ES_EVENT_TYPE_NAME, "event");
        runner.setProperty(IncrementalWebSession.ES_MAPPING_EVENT_TO_SESSION_INDEX_NAME, MAPPING_COLLECTION);
        runner.setProperty(IncrementalWebSession.SESSION_ID_FIELD, "sessionId");
        runner.setProperty(IncrementalWebSession.TIMESTAMP_FIELD, "h2kTimestamp");
        runner.setProperty(IncrementalWebSession.VISITED_PAGE_FIELD, "VISITED_PAGE");
        runner.setProperty(IncrementalWebSession.USER_ID_FIELD, "Userid");
        runner.setProperty(IncrementalWebSession.SESSION_INACTIVITY_TIMEOUT, String.valueOf(SESSION_TIMEOUT));
        runner.setProperty(IncrementalWebSession.FIELDS_TO_RETURN, FIELDS_TO_RETURN);
        this.elasticsearchClientService = PluginProxy.unwrap(runner.getProcessContext()
                .getPropertyValue(IncrementalWebSession.ELASTICSEARCH_CLIENT_SERVICE).asControllerService());
        return runner;
    }

    private void configureElasticsearchClientService(final TestRunner runner,
                                                                           DockerComposeContainer container) throws InitializationException
    {
        final Elasticsearch_2_4_0_ClientService elasticsearchClientService = new Elasticsearch_2_4_0_ClientService();
        runner.addControllerService("elasticsearchClient", elasticsearchClientService);
        runner.setProperty(elasticsearchClientService,
                Elasticsearch_2_4_0_ClientService.HOSTS, EsDockerExtension.getEsTcpUrl(container));
        runner.setProperty(elasticsearchClientService,
                Elasticsearch_2_4_0_ClientService.CLUSTER_NAME, "elasticsearch");
        runner.setProperty(elasticsearchClientService,
                Elasticsearch_2_4_0_ClientService.BATCH_SIZE, "2000");
        runner.setProperty(elasticsearchClientService,
                Elasticsearch_2_4_0_ClientService.FLUSH_INTERVAL, "2");
        runner.assertValid(elasticsearchClientService);
        runner.enableControllerService(elasticsearchClientService);
    }


/*
    A bug when deleting session mapping of es every day when divolt send events with same sessionId on several days !
    A bug when mult !

    Giver events input
        eventID | @timestamp | h2kTimestamp  | sessionId
     1 | 1601629314416 (ven. 02 oct. 2020 09:01:54 GMT) | "0:kfdxb7hf:U4e3OplHDO8Hda8yIS3O2iCdBOcVE_al"
     2 | 1601629317974 (ven. 02 oct. 2020 09:01:57 GMT) | "0:kfdxb7hf:U4e3OplHDO8Hda8yIS3O2iCdBOcVE_al"
     3 | 1601629320331 (ven. 02 oct. 2020 09:02:00 GMT) | "0:kfdxb7hf:U4e3OplHDO8Hda8yIS3O2iCdBOcVE_al"
     4 | 1601629320450 (ven. 02 oct. 2020 09:02:00 GMT) | "0:kfdxb7hf:U4e3OplHDO8Hda8yIS3O2iCdBOcVE_al"
     5 | 1601639001984 (ven. 02 oct. 2020 11:43:21 GMT) | "0:kfdxb7hf:U4e3OplHDO8Hda8yIS3O2iCdBOcVE_al#2"
     6 | 1601639014885 (ven. 02 oct. 2020 11:43:34 GMT) | "0:kfdxb7hf:U4e3OplHDO8Hda8yIS3O2iCdBOcVE_al#2"
     7 | 1601639015025 (ven. 02 oct. 2020 11:43:35 GMT) | "0:kfdxb7hf:U4e3OplHDO8Hda8yIS3O2iCdBOcVE_al#2"
     8 | 1601882662402 (lun. 05 oct. 2020 07:24:22 GMT) | "0:kfdxb7hf:U4e3OplHDO8Hda8yIS3O2iCdBOcVE_al#2"
     9 | 1601882676592 (lun. 05 oct. 2020 07:24:36 GMT) | "0:kfdxb7hf:U4e3OplHDO8Hda8yIS3O2iCdBOcVE_al#2"
     Expect output events
     1 | 1601629314416 (ven. 02 oct. 2020 09:01:54 GMT) | "0:kfdxb7hf:U4e3OplHDO8Hda8yIS3O2iCdBOcVE_al"
     2 | 1601629317974 (ven. 02 oct. 2020 09:01:57 GMT) | "0:kfdxb7hf:U4e3OplHDO8Hda8yIS3O2iCdBOcVE_al"
     3 | 1601629320331 (ven. 02 oct. 2020 09:02:00 GMT) | "0:kfdxb7hf:U4e3OplHDO8Hda8yIS3O2iCdBOcVE_al"
     4 | 1601629320450 (ven. 02 oct. 2020 09:02:00 GMT) | "0:kfdxb7hf:U4e3OplHDO8Hda8yIS3O2iCdBOcVE_al"
     5 | 1601639001984 (ven. 02 oct. 2020 11:43:21 GMT) | "0:kfdxb7hf:U4e3OplHDO8Hda8yIS3O2iCdBOcVE_al#2"
     6 | 1601639014885 (ven. 02 oct. 2020 11:43:34 GMT) | "0:kfdxb7hf:U4e3OplHDO8Hda8yIS3O2iCdBOcVE_al#2"
     7 | 1601639015025 (ven. 02 oct. 2020 11:43:35 GMT) | "0:kfdxb7hf:U4e3OplHDO8Hda8yIS3O2iCdBOcVE_al#2"
     8 | 1601882662402 (lun. 05 oct. 2020 07:24:22 GMT) | "0:kfdxb7hf:U4e3OplHDO8Hda8yIS3O2iCdBOcVE_al#3"
     9 | 1601882676592 (lun. 05 oct. 2020 07:24:36 GMT) | "0:kfdxb7hf:U4e3OplHDO8Hda8yIS3O2iCdBOcVE_al#3"
     And expect output sessions
     sessionId | firstEventDate | lastEventDate | counter
     "0:kfdxb7hf:U4e3OplHDO8Hda8yIS3O2iCdBOcVE_al"   | 1601629314416 (ven. 02 oct. 2020 09:01:54 GMT) | 1601629320450 (ven. 02 oct. 2020 09:02:00 GMT) | 4
     "0:kfdxb7hf:U4e3OplHDO8Hda8yIS3O2iCdBOcVE_al#2" | 1601639001984 (ven. 02 oct. 2020 11:43:21 GMT) | 1601639015025 (ven. 02 oct. 2020 11:43:35 GMT) | 3
     "0:kfdxb7hf:U4e3OplHDO8Hda8yIS3O2iCdBOcVE_al#3" | 1601882662402 (lun. 05 oct. 2020 07:24:22 GMT) | 1601882676592 (lun. 05 oct. 2020 07:24:36 GMT) | 2
*/
    @Test
    public void testBugWhenNotFlushingMappingAndHighFrequencyBatch(DockerComposeContainer container)
            throws Exception
    {
        final String url = "https://orexad.preprod.group-iph.com/fr/entretien-de-fluides/c-20-50-10";
        final String session =  "0:kfdxb7hf:U4e3OplHDO8Hda8yIS3O2iCdBOcVE_al";
        final String user =  "user";
        final TestRunner testRunner = newTestRunner(container);
        testRunner.assertValid();
//        this.elasticsearchClientService.createCollection(MAPPING_COLLECTION, 5, 0);
        //first run
        final long time1 = 1601629314416L;
        final long time2 = 1601629317974L;
        final long time3 = 1601629320331L;
        final long time4 = 1601629320450L;
        final long time5 = 1601639001984L;
        final long time6 = 1601639014885L;
        final long time7 = 1601639015025L;
        List<Long> times = Arrays.asList(time1, time2, time3, time4, time5, time6, time7);
        List<Record> events = createEvents(url, session, user, times);

        testRunner.enqueue(events);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);
        // One webSession expected.
        testRunner.assertOutputRecordsCount(2);
        final MockRecord session1 = getFirstRecordWithId(session, testRunner.getOutputRecords());
        final MockRecord session2 = getFirstRecordWithId(session + "#2", testRunner.getOutputRecords());

        new WebSessionChecker(session1).sessionId(session)
                .Userid(user)
                .record_type("consolidate-session")
                .record_id(session)
                .firstEventDateTime(time1)
                .h2kTimestamp(time1)
                .firstVisitedPage(url)
                .eventsCounter(4)
                .lastEventDateTime(time4)
                .lastVisitedPage(url)
                .sessionDuration(6L)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);

        new WebSessionChecker(session2).sessionId(session + "#2")
                .Userid(user)
                .record_type("consolidate-session")
                .record_id(session + "#2")
                .firstEventDateTime(time5)
                .h2kTimestamp(time5)
                .firstVisitedPage(url)
                .eventsCounter(3)
                .lastEventDateTime(time7)
                .lastVisitedPage(url)
                .sessionDuration(13L)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);
        //saves sessions
        this.elasticsearchClientService.bulkPut(SESSION_INDEX + ",sessions", session1);
        this.elasticsearchClientService.bulkPut(SESSION_INDEX + ",sessions", session2);
        this.elasticsearchClientService.bulkFlush();
        //second run
        final long time8 = 1601882662402L;
        final long time9 = 1601882676592L;
        times = Arrays.asList(time8, time9);
        events = createEvents(url, session, user, times);
        testRunner.clearQueues();
        testRunner.enqueue(events);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);
        // One webSession expected.
        testRunner.assertOutputRecordsCount(2);

        final MockRecord session2Updated = getFirstRecordWithId(session + "#2", testRunner.getOutputRecords());
        final MockRecord session3 = getFirstRecordWithId(session + "#3", testRunner.getOutputRecords());

        new WebSessionChecker(session2Updated).sessionId(session + "#2")
                .Userid(user)
                .record_type("consolidate-session")
                .record_id(session + "#2")
                .firstEventDateTime(time5)
                .h2kTimestamp(time5)
                .firstVisitedPage(url)
                .eventsCounter(3)
                .lastEventDateTime(time7)
                .lastVisitedPage(url)
                .sessionDuration(13L)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);

        new WebSessionChecker(session3).sessionId(session + "#3")
                .Userid(user)
                .record_type("consolidate-session")
                .record_id(session + "#3")
                .firstEventDateTime(time8)
                .h2kTimestamp(time8)
                .firstVisitedPage(url)
                .eventsCounter(2)
                .lastEventDateTime(time9)
                .lastVisitedPage(url)
                .sessionDuration(14L)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);
    }

    @Test
    public void testBugWhenNotFlushingMappingAndHighFrequencyBatch2(DockerComposeContainer container)
            throws Exception
    {
        final String url = "https://orexad.preprod.group-iph.com/fr/entretien-de-fluides/c-20-50-10";
        final String session =  "0:kfdxb7hf:U4e3OplHDO8Hda8yIS3O2iCdBOcVE_al";
        final String user =  "user";
        final TestRunner testRunner = newTestRunner(container);
        testRunner.assertValid();
        //first run
        final long time1 = 1601629314416L;
        final long time2 = time1 + (SESSION_TIMEOUT + 1L) * 1000L;
        final long time3 = time2 + (SESSION_TIMEOUT + 1L) * 1000L;
        final long time4 = time3 + (SESSION_TIMEOUT + 1L) * 1000L;
        final long time5 = time4 + (SESSION_TIMEOUT + 1L) * 1000L;
        List<Long> times = Arrays.asList(time1, time2, time3, time4, time5);
        List<Record> events = createEvents(url, session, user, times);

        testRunner.enqueue(events);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);
        testRunner.assertOutputRecordsCount(5);
        MockRecord session1 = getFirstRecordWithId(session, testRunner.getOutputRecords());
        MockRecord session2 = getFirstRecordWithId(session + "#2", testRunner.getOutputRecords());
        MockRecord session3 = getFirstRecordWithId(session + "#3", testRunner.getOutputRecords());
        MockRecord session4 = getFirstRecordWithId(session + "#4", testRunner.getOutputRecords());
        MockRecord session5 = getFirstRecordWithId(session + "#5", testRunner.getOutputRecords());

        new WebSessionChecker(session1).sessionId(session)
                .Userid(user)
                .record_type("consolidate-session")
                .record_id(session)
                .firstEventDateTime(time1)
                .h2kTimestamp(time1)
                .firstVisitedPage(url)
                .eventsCounter(1)
                .lastEventDateTime(time1)
                .lastVisitedPage(url)
                .sessionDuration(null)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);

        new WebSessionChecker(session5).sessionId(session + "#5")
                .Userid(user)
                .record_type("consolidate-session")
                .record_id(session + "#5")
                .firstEventDateTime(time5)
                .h2kTimestamp(time5)
                .firstVisitedPage(url)
                .eventsCounter(1)
                .lastEventDateTime(time5)
                .lastVisitedPage(url)
                .sessionDuration(null)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);

        this.elasticsearchClientService.bulkPut(SESSION_INDEX + ",sessions", session1);
        this.elasticsearchClientService.bulkPut(SESSION_INDEX + ",sessions", session2);
        this.elasticsearchClientService.bulkPut(SESSION_INDEX + ",sessions", session3);
        this.elasticsearchClientService.bulkPut(SESSION_INDEX + ",sessions", session4);
        this.elasticsearchClientService.bulkPut(SESSION_INDEX + ",sessions", session5);
        this.elasticsearchClientService.bulkFlush();
        //second run
        final long time6 = time5 + 24L * 60L * 60L * 1000L;
        final long time7 = time6 + 1000L;
        final long time8 = time7 + 1000L;
        times = Arrays.asList(time6, time7, time8);
        events = createEvents(url, session, user, times);
        testRunner.clearQueues();
        testRunner.enqueue(events);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);
        testRunner.assertOutputRecordsCount(2);
        session5 = getFirstRecordWithId(session+ "#5", testRunner.getOutputRecords());
        MockRecord session6 = getFirstRecordWithId(session + "#6", testRunner.getOutputRecords());

        new WebSessionChecker(session5).sessionId(session + "#5")
                .Userid(user)
                .record_type("consolidate-session")
                .record_id(session + "#5")
                .firstEventDateTime(time5)
                .h2kTimestamp(time5)
                .firstVisitedPage(url)
                .eventsCounter(1)
                .lastEventDateTime(time5)
                .lastVisitedPage(url)
                .sessionDuration(null)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);

        new WebSessionChecker(session6).sessionId(session + "#6")
                .Userid(user)
                .record_type("consolidate-session")
                .record_id(session + "#6")
                .firstEventDateTime(time6)
                .h2kTimestamp(time6)
                .firstVisitedPage(url)
                .eventsCounter(3)
                .lastEventDateTime(time8)
                .lastVisitedPage(url)
                .sessionDuration(2L)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);

        //saves sessions
        this.elasticsearchClientService.bulkPut(SESSION_INDEX + ",sessions", session5);
        this.elasticsearchClientService.bulkPut(SESSION_INDEX + ",sessions", session6);
        this.elasticsearchClientService.bulkFlush();
        //third run
        final long time9 = time8 + 1000L;
        final long time10 = time9 + 1000L;
        times = Arrays.asList(time9, time10);
        events = createEvents(url, session, user, times);
        testRunner.clearQueues();
        testRunner.enqueue(events);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);
        testRunner.assertOutputRecordsCount(1);

        session6 = getFirstRecordWithId(session+ "#6", testRunner.getOutputRecords());

        new WebSessionChecker(session6).sessionId(session+ "#6")
                .Userid(user)
                .record_type("consolidate-session")
                .record_id(session+ "#6")
                .firstEventDateTime(time6)
                .h2kTimestamp(time6)
                .firstVisitedPage(url)
                .eventsCounter(5)
                .lastEventDateTime(time10)
                .lastVisitedPage(url)
                .sessionDuration(4L)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);
    }


    private int eventCount = 0;
    @NotNull
    public List<Record> createEvents(String url, String session, String user, List<Long> times) {
        List<Record> events = new ArrayList<>();
        for (Long time : times) {
            events.add(new WebEvent(eventCount++, session, user, time, url));
        }
        return events;
    }
}
