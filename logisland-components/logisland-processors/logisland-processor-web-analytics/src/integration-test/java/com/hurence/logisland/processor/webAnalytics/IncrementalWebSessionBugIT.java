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

import com.hurence.junit5.extension.Es7DockerExtension;
import com.hurence.logisland.classloading.PluginProxy;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.processor.webAnalytics.modele.WebSession;
import com.hurence.logisland.processor.webAnalytics.util.ElasticsearchServiceUtil;
import com.hurence.logisland.processor.webAnalytics.util.WebEvent;
import com.hurence.logisland.processor.webAnalytics.util.WebSessionChecker;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.service.cache.CacheService;
import com.hurence.logisland.service.cache.LRUKeyValueCacheService;
import com.hurence.logisland.service.elasticsearch.ElasticsearchClientService;
import com.hurence.logisland.service.elasticsearch.Elasticsearch_7_x_ClientService;
import com.hurence.logisland.util.runner.MockRecord;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.PutIndexTemplateRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.DockerComposeContainer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.hurence.logisland.processor.webAnalytics.util.ElasticsearchServiceUtil.EVENT_INDEX_PREFIX;
import static com.hurence.logisland.processor.webAnalytics.util.ElasticsearchServiceUtil.SESSION_INDEX_PREFIX;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test incremental web-session processor.
 */
@ExtendWith({Es7DockerExtension.class})
public class IncrementalWebSessionBugIT
{
    private static Logger logger = LoggerFactory.getLogger(IncrementalWebSessionBugIT.class);

    private final long SESSION_TIMEOUT = 1800L;
    private ElasticsearchClientService elasticsearchClientService;
    private CacheService<String, WebSession> lruCache;

    @BeforeEach
    public void clean(RestHighLevelClient esClient) throws IOException {
        try {
            Set<String> indices = Arrays.stream(esClient.indices().get(
                    new GetIndexRequest("*"),
                    RequestOptions.DEFAULT).getIndices()
            ).collect(Collectors.toSet());

            if (!indices.isEmpty()) {
                logger.info("Will delete following indices :{}", indices);
                DeleteIndexRequest deleteRequest = new DeleteIndexRequest(indices.toArray(new String[0]));
                Assert.assertTrue(esClient.indices().delete(deleteRequest, RequestOptions.DEFAULT).isAcknowledged());
            }
        } catch (Exception ex) {
            //when there is no index
        }
        PutIndexTemplateRequest templateRequest = new PutIndexTemplateRequest("all-indices")
                .patterns(Arrays.asList("*"))
                .settings(Settings.builder()
                        .put("index.number_of_shards", 5)
                        .put("index.number_of_replicas", 0)
                );
        String mappingJson = TestFileHelper.loadFromFile("/rawStringMappingFile.json");
        templateRequest.mapping(mappingJson, XContentType.JSON);
        AcknowledgedResponse putTemplateResponse = esClient.indices().putTemplate(templateRequest, RequestOptions.DEFAULT);
        logger.info("putTemplateResponse is " + putTemplateResponse);

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
        injectSessions(Arrays.asList(session1, session2));
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

    private void injectSessions(List<MockRecord> sessions) {
        ElasticsearchServiceUtil.injectSessions(this.elasticsearchClientService, sessions);
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

        injectSessions(Arrays.asList(session1, session2, session3, session4, session5));
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
        injectSessions(Arrays.asList(session5, session6));
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


    @Test
    public void testRewind1(RestHighLevelClient esclient, DockerComposeContainer container)
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

        injectSessions(Arrays.asList(session1, session2, session3, session4, session5));
        SearchResponse rsp = getAllSessions(esclient);
        assertEquals(5, rsp.getHits().getTotalHits().value);
        //rewind batch1
        times = Arrays.asList(time1, time2);
        events = createEvents(url, session, user, times);
        resetCache(testRunner);
        testRunner.clearQueues();
        testRunner.enqueue(events);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);
        testRunner.assertOutputRecordsCount(2);
        session1 = getFirstRecordWithId(session, testRunner.getOutputRecords());
        session2 = getFirstRecordWithId(session + "#2", testRunner.getOutputRecords());

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

        new WebSessionChecker(session2).sessionId(session + "#2")
                .Userid(user)
                .record_type("consolidate-session")
                .record_id(session + "#2")
                .firstEventDateTime(time2)
                .h2kTimestamp(time2)
                .firstVisitedPage(url)
                .eventsCounter(1)
                .lastEventDateTime(time2)
                .lastVisitedPage(url)
                .sessionDuration(null)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);
        //rewind batch 2
        injectSessions(Arrays.asList(session1, session2));
        rsp = getAllSessions(esclient);
        assertEquals(2, rsp.getHits().getTotalHits().value);
        //third run
        //rewind
        times = Arrays.asList(time3, time4, time5);
        events = createEvents(url, session, user, times);
        testRunner.clearQueues();
        testRunner.enqueue(events);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);
        testRunner.assertOutputRecordsCount(4);
        session2 = getFirstRecordWithId(session+ "#2", testRunner.getOutputRecords());
        session3 = getFirstRecordWithId(session+ "#3", testRunner.getOutputRecords());
        session4 = getFirstRecordWithId(session + "#4", testRunner.getOutputRecords());
        session5 = getFirstRecordWithId(session + "#5", testRunner.getOutputRecords());

        new WebSessionChecker(session2).sessionId(session + "#2")
                .Userid(user)
                .record_type("consolidate-session")
                .record_id(session + "#2")
                .firstEventDateTime(time2)
                .h2kTimestamp(time2)
                .firstVisitedPage(url)
                .eventsCounter(1)
                .lastEventDateTime(time2)
                .lastVisitedPage(url)
                .sessionDuration(null)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);

        new WebSessionChecker(session3).sessionId(session+ "#3")
                .Userid(user)
                .record_type("consolidate-session")
                .record_id(session+ "#3")
                .firstEventDateTime(time3)
                .h2kTimestamp(time3)
                .firstVisitedPage(url)
                .eventsCounter(1)
                .lastEventDateTime(time3)
                .lastVisitedPage(url)
                .sessionDuration(null)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);

        new WebSessionChecker(session4).sessionId(session + "#4")
                .Userid(user)
                .record_type("consolidate-session")
                .record_id(session + "#4")
                .firstEventDateTime(time4)
                .h2kTimestamp(time4)
                .firstVisitedPage(url)
                .eventsCounter(1)
                .lastEventDateTime(time4)
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

        injectSessions(Arrays.asList(session2, session3, session4, session5));

        rsp = getAllSessions(esclient);
        assertEquals(5, rsp.getHits().getTotalHits().value);
    }

    public void resetCache(TestRunner testRunner) {
        testRunner.disableControllerService(lruCache);
        testRunner.enableControllerService(lruCache);
    }

    public SearchResponse getAllSessions(RestHighLevelClient esclient) throws IOException {
        this.elasticsearchClientService.waitUntilCollectionIsReadyAndRefreshIfAnyPendingTasks(SESSION_INDEX_PREFIX + "*", 100000L);
        SearchRequest searchRequest = new SearchRequest(SESSION_INDEX_PREFIX + "*");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchRequest.source(searchSourceBuilder);
        return esclient.search(searchRequest, RequestOptions.DEFAULT);
    }

    @Test
    public void testRewind2(RestHighLevelClient esclient, DockerComposeContainer container)
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

        injectSessions(Arrays.asList(session1, session2, session3, session4, session5));

        SearchResponse rsp = getAllSessions(esclient);
        assertEquals(5, rsp.getHits().getTotalHits().value);
        //rewind from time3
        times = Arrays.asList(time3, time4, time5);
        events = createEvents(url, session, user, times);
        testRunner.clearQueues();
        testRunner.enqueue(events);
        testRunner.run();
        List<MockRecord> outputSessions = testRunner.getOutputRecords();
        injectSessions(outputSessions);

        rsp = getAllSessions(esclient);
        assertEquals(5, rsp.getHits().getTotalHits().value);
    }

    @Test
    public void testRewindFailThenRestart(RestHighLevelClient esclient, DockerComposeContainer container)
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

        injectSessions(Arrays.asList(session1, session2, session3, session4, session5));

        SearchResponse rsp = getAllSessions(esclient);
        assertEquals(5, rsp.getHits().getTotalHits().value);
        //rewind from time3 but fail during regestering session so regestering only session 3
        times = Arrays.asList(time3, time4, time5);
        events = createEvents(url, session, user, times);
        testRunner.clearQueues();
        testRunner.enqueue(events);
        testRunner.run();
        testRunner.assertOutputRecordsCount(3);
        session3 = getFirstRecordWithId(session + "#3", testRunner.getOutputRecords());
        injectSessions(Arrays.asList(session3));

        rsp = getAllSessions(esclient);
        assertEquals(3, rsp.getHits().getTotalHits().value);
        //restart from time3 because offset was not commited
        times = Arrays.asList(time3, time4, time5);
        events = createEvents(url, session, user, times);
        testRunner.clearQueues();
        testRunner.enqueue(events);
        testRunner.run();
        testRunner.assertOutputRecordsCount(3);
        List<MockRecord> outputSessions = testRunner.getOutputRecords();
        injectSessions(outputSessions);

        rsp = getAllSessions(esclient);
        assertEquals(5, rsp.getHits().getTotalHits().value);
    }

    private List<Record> createEvents(String url, String session, String user, List<Long> times) {
        List<Record> events = new ArrayList<>();
        for (Long time : times) {
            String id = "event-" + time + "-" + session;
            events.add(new WebEvent(id, session, user, time, url));
        }
        return events;
    }

    private MockRecord getFirstRecordWithId(final String id, final List<MockRecord> records)
    {
        return records.stream().filter(record -> record.getId().equals(id)).findFirst().get();
    }



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
        configureCacheService(runner);
        runner.setProperty(IncrementalWebSession.CONFIG_CACHE_SERVICE, "lruCache");
        runner.setProperty(IncrementalWebSession.ELASTICSEARCH_CLIENT_SERVICE_CONF, "elasticsearchClient");
        runner.setProperty(IncrementalWebSession.ES_SESSION_INDEX_PREFIX_CONF, SESSION_INDEX_PREFIX);
        runner.setProperty(IncrementalWebSession.ES_SESSION_INDEX_SUFFIX_FORMATTER_CONF, "yyyy.MM");
        runner.setProperty(IncrementalWebSession.ES_SESSION_TYPE_NAME_CONF, "sessions");
        runner.setProperty(IncrementalWebSession.ES_EVENT_INDEX_PREFIX_CONF, EVENT_INDEX_PREFIX);
        runner.setProperty(IncrementalWebSession.ES_EVENT_INDEX_SUFFIX_FORMATTER_CONF, "yyyy.MM.dd");
        runner.setProperty(IncrementalWebSession.ES_EVENT_TYPE_NAME_CONF, "event");
        runner.setProperty(IncrementalWebSession.SESSION_ID_FIELD_CONF, "sessionId");
        runner.setProperty(IncrementalWebSession.TIMESTAMP_FIELD_CONF, "h2kTimestamp");
        runner.setProperty(IncrementalWebSession.VISITED_PAGE_FIELD, "VISITED_PAGE");
        runner.setProperty(IncrementalWebSession.USER_ID_FIELD, "Userid");
        runner.setProperty(IncrementalWebSession.SESSION_INACTIVITY_TIMEOUT_CONF, String.valueOf(SESSION_TIMEOUT));
        runner.setProperty(IncrementalWebSession.FIELDS_TO_RETURN, FIELDS_TO_RETURN);
        runner.setProperty(IncrementalWebSession.DEBUG_CONF, "true");
        this.elasticsearchClientService = PluginProxy.unwrap(runner.getProcessContext()
                .getPropertyValue(IncrementalWebSession.ELASTICSEARCH_CLIENT_SERVICE_CONF).asControllerService());
        return runner;
    }

    private void configureCacheService(TestRunner runner) throws InitializationException {
        final LRUKeyValueCacheService<String, WebSession> cacheService = new LRUKeyValueCacheService<>();
        runner.addControllerService("lruCache", cacheService);
        runner.setProperty(cacheService,
                LRUKeyValueCacheService.CACHE_SIZE, "1000");
        runner.assertValid(cacheService);
        runner.enableControllerService(cacheService);
        this.lruCache = cacheService;
    }

    private void configureElasticsearchClientService(final TestRunner runner,
                                                     DockerComposeContainer container) throws InitializationException {
        final Elasticsearch_7_x_ClientService elasticsearchClientService = new Elasticsearch_7_x_ClientService();
        runner.addControllerService("elasticsearchClient", elasticsearchClientService);
        runner.setProperty(elasticsearchClientService,
                Elasticsearch_7_x_ClientService.HOSTS, Es7DockerExtension.getEsHttpUrl(container));
        runner.setProperty(elasticsearchClientService,
                Elasticsearch_7_x_ClientService.CLUSTER_NAME, "elasticsearch");
        runner.setProperty(elasticsearchClientService,
                Elasticsearch_7_x_ClientService.BATCH_SIZE, "2000");
        runner.setProperty(elasticsearchClientService,
                Elasticsearch_7_x_ClientService.FLUSH_INTERVAL, "2");
        runner.assertValid(elasticsearchClientService);
        runner.enableControllerService(elasticsearchClientService);
    }
}
