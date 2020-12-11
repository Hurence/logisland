/**
 * Copyright (C) 2016 Hurence (support@hurence.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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
import com.hurence.logisland.processor.webAnalytics.modele.TestMappings;
import com.hurence.logisland.processor.webAnalytics.modele.WebSession;
import com.hurence.logisland.processor.webAnalytics.util.ElasticsearchServiceUtil;
import com.hurence.logisland.processor.webAnalytics.util.WebEvent;
import com.hurence.logisland.processor.webAnalytics.util.WebSessionChecker;
import com.hurence.logisland.record.FieldDictionary;
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
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.DockerComposeContainer;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.hurence.logisland.processor.webAnalytics.util.ElasticsearchServiceUtil.EVENT_INDEX_PREFIX;
import static com.hurence.logisland.processor.webAnalytics.util.ElasticsearchServiceUtil.SESSION_INDEX_PREFIX;
import static com.hurence.logisland.processor.webAnalytics.util.UtilsTest.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test incremental web-session processor.
 */
@ExtendWith({Es7DockerExtension.class})
public class IncrementalWebSessionBugIT {
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
            throws Exception {
        final String url = "https://orexad.preprod.group-iph.com/fr/entretien-de-fluides/c-20-50-10";
        final String divoltSession = "0:kfdxb7hf:U4e3OplHDO8Hda8yIS3O2iCdBOcVE_al";
        final String user = "user";
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
        testRunner.enqueue(createEvents(url, divoltSession, user, times));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);
        // One webSession expected.
        testRunner.assertOutputRecordsCount(2);
        final MockRecord session_1 = getFirstRecordWithId(divoltSession, testRunner.getOutputRecords());
        final MockRecord session_2 = getFirstRecordWithId(divoltSession + "#2", testRunner.getOutputRecords());
        new WebSessionChecker(session_1).sessionId(divoltSession)
                .Userid(user)
                .record_type("consolidate-session")
                .record_id(divoltSession)
                .firstEventDateTime(time1)
                .h2kTimestamp(time1)
                .firstVisitedPage(url)
                .eventsCounter(4)
                .lastEventDateTime(time4)
                .lastVisitedPage(url)
                .sessionDuration(6L)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);

        new WebSessionChecker(session_2).sessionId(divoltSession + "#2")
                .Userid(user)
                .record_type("consolidate-session")
                .record_id(divoltSession + "#2")
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
        injectSessionsWithoutRefreshing(Arrays.asList(session_1, session_2));
        //second run
        final long time8 = 1601882662402L;
        final long time9 = 1601882676592L;
        times = Arrays.asList(time8, time9);
        testRunner.clearQueues();
        testRunner.enqueue(createEvents(url, divoltSession, user, times));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);
        // One webSession expected.
        testRunner.assertOutputRecordsCount(2);

        final MockRecord session_2Updated = getFirstRecordWithId(divoltSession + "#2", testRunner.getOutputRecords());
        final MockRecord session_3 = getFirstRecordWithId(divoltSession + "#3", testRunner.getOutputRecords());
        new WebSessionChecker(session_2Updated).sessionId(divoltSession + "#2")
                .Userid(user)
                .record_type("consolidate-session")
                .record_id(divoltSession + "#2")
                .firstEventDateTime(time5)
                .h2kTimestamp(time5)
                .firstVisitedPage(url)
                .eventsCounter(3)
                .lastEventDateTime(time7)
                .lastVisitedPage(url)
                .sessionDuration(13L)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);

        new WebSessionChecker(session_3).sessionId(divoltSession + "#3")
                .Userid(user)
                .record_type("consolidate-session")
                .record_id(divoltSession + "#3")
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

    private void injectSessionsThenRefresh(List<MockRecord> sessions) {
        ElasticsearchServiceUtil.injectSessionsThenRefresh(this.elasticsearchClientService, sessions);
    }

    private void injectSessionsWithoutRefreshing(List<MockRecord> sessions) {
        ElasticsearchServiceUtil.injectSessionsWithoutRefreshing(this.elasticsearchClientService, sessions);
    }

    @Test
    public void testBugWhenNotFlushingMappingAndHighFrequencyBatch2(DockerComposeContainer container)
            throws Exception {
        final String url = "https://orexad.preprod.group-iph.com/fr/entretien-de-fluides/c-20-50-10";
        final String session = "0:kfdxb7hf:U4e3OplHDO8Hda8yIS3O2iCdBOcVE_al";
        final String user = "user";
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

        injectSessionsWithoutRefreshing(Arrays.asList(session1, session2, session3, session4, session5));
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
        session5 = getFirstRecordWithId(session + "#5", testRunner.getOutputRecords());
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
        injectSessionsWithoutRefreshing(Arrays.asList(session5, session6));
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

        session6 = getFirstRecordWithId(session + "#6", testRunner.getOutputRecords());

        new WebSessionChecker(session6).sessionId(session + "#6")
                .Userid(user)
                .record_type("consolidate-session")
                .record_id(session + "#6")
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
            throws Exception {
        final String url = "https://orexad.preprod.group-iph.com/fr/entretien-de-fluides/c-20-50-10";
        final String session = "0:kfdxb7hf:U4e3OplHDO8Hda8yIS3O2iCdBOcVE_al";
        final String user = "user";
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

        injectSessionsWithoutRefreshing(Arrays.asList(session1, session2, session3, session4, session5));
        SearchResponse rsp = getAllSessionsAfterRefreshing(esclient);
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
        injectSessionsWithoutRefreshing(Arrays.asList(session1, session2));
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
        session2 = getFirstRecordWithId(session + "#2", testRunner.getOutputRecords());
        session3 = getFirstRecordWithId(session + "#3", testRunner.getOutputRecords());
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

        new WebSessionChecker(session3).sessionId(session + "#3")
                .Userid(user)
                .record_type("consolidate-session")
                .record_id(session + "#3")
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

        injectSessionsThenRefresh(Arrays.asList(session2, session3, session4, session5));
        rsp = getAllSessionsAfterRefreshing(esclient);
        assertEquals(5, rsp.getHits().getTotalHits().value);
    }

    public void resetCache(TestRunner testRunner) {
        testRunner.disableControllerService(lruCache);
        testRunner.enableControllerService(lruCache);
    }

    public SearchResponse getAllSessionsAfterRefreshing(RestHighLevelClient esclient) throws IOException {
        try {
            Thread.sleep(1000L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        this.elasticsearchClientService.waitUntilCollectionIsReadyAndRefreshIfAnyPendingTasks(SESSION_INDEX_PREFIX + "*", 100000L);
        SearchRequest searchRequest = new SearchRequest(SESSION_INDEX_PREFIX + "*");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchRequest.source(searchSourceBuilder);
        return esclient.search(searchRequest, RequestOptions.DEFAULT);
    }

    public SearchResponse getAllEventsAfterRefreshing(RestHighLevelClient esclient) throws IOException {
        try {
            Thread.sleep(1000L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        this.elasticsearchClientService.waitUntilCollectionIsReadyAndRefreshIfAnyPendingTasks(EVENT_INDEX_PREFIX + "*", 100000L);
        SearchRequest searchRequest = new SearchRequest(EVENT_INDEX_PREFIX + "*");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchRequest.source(searchSourceBuilder);
        return esclient.search(searchRequest, RequestOptions.DEFAULT);
    }

    @Test
    public void testRewind2(RestHighLevelClient esclient, DockerComposeContainer container)
            throws Exception {
        final String url = "https://orexad.preprod.group-iph.com/fr/entretien-de-fluides/c-20-50-10";
        final String session = "0:kfdxb7hf:U4e3OplHDO8Hda8yIS3O2iCdBOcVE_al";
        final String user = "user";
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

        injectSessionsWithoutRefreshing(Arrays.asList(session1, session2, session3, session4, session5));

        SearchResponse rsp = getAllSessionsAfterRefreshing(esclient);
        assertEquals(5, rsp.getHits().getTotalHits().value);
        //rewind from time3
        times = Arrays.asList(time3, time4, time5);
        events = createEvents(url, session, user, times);
        testRunner.clearQueues();
        testRunner.enqueue(events);
        testRunner.run();
        List<MockRecord> outputSessions = testRunner.getOutputRecords();
        injectSessionsThenRefresh(outputSessions);

        rsp = getAllSessionsAfterRefreshing(esclient);
        assertEquals(5, rsp.getHits().getTotalHits().value);
    }

    @Test
    public void testRewindFailThenRestart(RestHighLevelClient esclient, DockerComposeContainer container)
            throws Exception {
        final String url = "https://orexad.preprod.group-iph.com/fr/entretien-de-fluides/c-20-50-10";
        final String session = "0:kfdxb7hf:U4e3OplHDO8Hda8yIS3O2iCdBOcVE_al";
        final String user = "user";
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

        injectSessionsWithoutRefreshing(Arrays.asList(session1, session2, session3, session4, session5));

        //rewind from time3 but fail during regestering session so regestering only session 3
        times = Arrays.asList(time3, time4, time5);
        events = createEvents(url, session, user, times);
        testRunner.clearQueues();
        testRunner.enqueue(events);
        testRunner.run();
        testRunner.assertOutputRecordsCount(3);
        session3 = getFirstRecordWithId(session + "#3", testRunner.getOutputRecords());
        injectSessionsWithoutRefreshing(Arrays.asList(session3));

        //restart from time3 because offset was not commited
        times = Arrays.asList(time3, time4, time5);
        events = createEvents(url, session, user, times);
        testRunner.clearQueues();
        testRunner.enqueue(events);
        testRunner.run();
        testRunner.assertOutputRecordsCount(3);
        List<MockRecord> outputSessions = testRunner.getOutputRecords();
        injectSessionsThenRefresh(outputSessions);

        SearchResponse rsp = getAllSessionsAfterRefreshing(esclient);
        assertEquals(5, rsp.getHits().getTotalHits().value);
    }

    @Test
    public void testRewind2DivoltId(RestHighLevelClient esclient, DockerComposeContainer container)
            throws Exception {
        final String url = "https://orexad.preprod.group-iph.com/fr/entretien-de-fluides/c-20-50-10";
        final String divoltSession = "0:kfdxb7hf:U4e3OplHDO8Hda8yIS3O2iCdBOcVE_al";
        final String divoltSession2 = "0:kfdxb7hf:alpha";
        final String user = "user";
        final TestRunner testRunner = newTestRunner(container);
        testRunner.assertValid();
        //first run
        final long time1 = 1601629314416L;
        final long time2 = time1 + (SESSION_TIMEOUT + 1L) * 1000L;
        final long time3 = time2 + (SESSION_TIMEOUT + 1L) * 1000L;
        final long time4 = time3 + (SESSION_TIMEOUT + 1L) * 1000L;
        final long time5 = time4 + (SESSION_TIMEOUT + 1L) * 1000L;
        List<Long> times = Arrays.asList(time1, time2, time3, time4, time5);
        testRunner.enqueue(createEvents(url, divoltSession, user, times));
        testRunner.enqueue(createEvents(url, divoltSession2, user, times));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);
        testRunner.assertOutputRecordsCount(10);
        MockRecord session_1 = getFirstRecordWithId(divoltSession, testRunner.getOutputRecords());
        MockRecord session_5 = getFirstRecordWithId(divoltSession + "#5", testRunner.getOutputRecords());

        new WebSessionChecker(session_1).sessionId(divoltSession)
                .Userid(user)
                .record_type("consolidate-session")
                .record_id(divoltSession)
                .firstEventDateTime(time1)
                .h2kTimestamp(time1)
                .firstVisitedPage(url)
                .eventsCounter(1)
                .lastEventDateTime(time1)
                .lastVisitedPage(url)
                .sessionDuration(null)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);

        new WebSessionChecker(session_5).sessionId(divoltSession + "#5")
                .Userid(user)
                .record_type("consolidate-session")
                .record_id(divoltSession + "#5")
                .firstEventDateTime(time5)
                .h2kTimestamp(time5)
                .firstVisitedPage(url)
                .eventsCounter(1)
                .lastEventDateTime(time5)
                .lastVisitedPage(url)
                .sessionDuration(null)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);

        injectSessionsThenRefresh(testRunner.getOutputRecords());
        SearchResponse rsp = getAllSessionsAfterRefreshing(esclient);
        assertEquals(10, rsp.getHits().getTotalHits().value);
        //rewind from time1
        times = Arrays.asList(time1, time2);
        testRunner.clearQueues();
        testRunner.enqueue(createEvents(url, divoltSession, user, times));
        testRunner.enqueue(createEvents(url, divoltSession2, user, times));
        testRunner.run();
        testRunner.assertOutputErrorCount(0);
        testRunner.assertOutputRecordsCount(4);
        List<MockRecord> outputSessions = testRunner.getOutputRecords();
        rsp = getAllSessionsAfterRefreshing(esclient);
        assertEquals(2, rsp.getHits().getTotalHits().value);
        injectSessionsThenRefresh(outputSessions);
        rsp = getAllSessionsAfterRefreshing(esclient);
        assertEquals(4, rsp.getHits().getTotalHits().value);

        //end of rewind should be as start
        times = Arrays.asList(time3, time4, time5);
        testRunner.clearQueues();
        testRunner.enqueue(createEvents(url, divoltSession, user, times));
        testRunner.enqueue(createEvents(url, divoltSession2, user, times));
        testRunner.run();
        testRunner.assertOutputErrorCount(0);
        testRunner.assertOutputRecordsCount(8);
        outputSessions = testRunner.getOutputRecords();
        rsp = getAllSessionsAfterRefreshing(esclient);
        assertEquals(4, rsp.getHits().getTotalHits().value);
        injectSessionsThenRefresh(outputSessions);
        rsp = getAllSessionsAfterRefreshing(esclient);
        assertEquals(10, rsp.getHits().getTotalHits().value);
        List<WebSession> sessions = ElasticsearchServiceUtil.getAllSessions(elasticsearchClientService, esclient);
        String finalSession = divoltSession;
        getWebSessionCheckerForSession(finalSession, sessions)
                .sessionId(finalSession)
                .Userid(user)
                .record_type("consolidate-session")
                .record_id(finalSession)
                .firstEventDateTime(time1)
                .h2kTimestamp(time1)
                .firstVisitedPage(url)
                .eventsCounter(1)//TODO system de checking Long != Int
                .lastEventDateTime(time1)
                .lastVisitedPage(url)
                .sessionDuration(null)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);
        finalSession = divoltSession + "#2";
        getWebSessionCheckerForSession(finalSession, sessions)
                .sessionId(finalSession)
                .Userid(user)
                .record_type("consolidate-session")
                .record_id(finalSession)
                .firstEventDateTime(time2)
                .h2kTimestamp(time2)
                .firstVisitedPage(url)
                .eventsCounter(1)
                .lastEventDateTime(time2)
                .lastVisitedPage(url)
                .sessionDuration(null)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);
        finalSession = divoltSession + "#3";
        getWebSessionCheckerForSession(finalSession, sessions)
                .sessionId(finalSession)
                .Userid(user)
                .record_type("consolidate-session")
                .record_id(finalSession)
                .firstEventDateTime(time3)
                .h2kTimestamp(time3)
                .firstVisitedPage(url)
                .eventsCounter(1)
                .lastEventDateTime(time3)
                .lastVisitedPage(url)
                .sessionDuration(null)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);
        finalSession = divoltSession + "#4";
        getWebSessionCheckerForSession(finalSession, sessions)
                .sessionId(finalSession)
                .Userid(user)
                .record_type("consolidate-session")
                .record_id(finalSession)
                .firstEventDateTime(time4)
                .h2kTimestamp(time4)
                .firstVisitedPage(url)
                .eventsCounter(1)
                .lastEventDateTime(time4)
                .lastVisitedPage(url)
                .sessionDuration(null)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);
        finalSession = divoltSession + "#5";
        getWebSessionCheckerForSession(finalSession, sessions)
                .sessionId(finalSession)
                .Userid(user)
                .record_type("consolidate-session")
                .record_id(finalSession)
                .firstEventDateTime(time5)
                .h2kTimestamp(time5)
                .firstVisitedPage(url)
                .eventsCounter(1)
                .lastEventDateTime(time5)
                .lastVisitedPage(url)
                .sessionDuration(null)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);

        finalSession = divoltSession2;
        getWebSessionCheckerForSession(finalSession, sessions)
                .sessionId(finalSession)
                .Userid(user)
                .record_type("consolidate-session")
                .record_id(finalSession)
                .firstEventDateTime(time1)
                .h2kTimestamp(time1)
                .firstVisitedPage(url)
                .eventsCounter(1)
                .lastEventDateTime(time1)
                .lastVisitedPage(url)
                .sessionDuration(null)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);
        finalSession = divoltSession2 + "#2";
        getWebSessionCheckerForSession(finalSession, sessions)
                .sessionId(finalSession)
                .Userid(user)
                .record_type("consolidate-session")
                .record_id(finalSession)
                .firstEventDateTime(time2)
                .h2kTimestamp(time2)
                .firstVisitedPage(url)
                .eventsCounter(1)
                .lastEventDateTime(time2)
                .lastVisitedPage(url)
                .sessionDuration(null)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);
        finalSession = divoltSession2 + "#3";
        getWebSessionCheckerForSession(finalSession, sessions)
                .sessionId(finalSession)
                .Userid(user)
                .record_type("consolidate-session")
                .record_id(finalSession)
                .firstEventDateTime(time3)
                .h2kTimestamp(time3)
                .firstVisitedPage(url)
                .eventsCounter(1)
                .lastEventDateTime(time3)
                .lastVisitedPage(url)
                .sessionDuration(null)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);
        finalSession = divoltSession2 + "#4";
        getWebSessionCheckerForSession(finalSession, sessions)
                .sessionId(finalSession)
                .Userid(user)
                .record_type("consolidate-session")
                .record_id(finalSession)
                .firstEventDateTime(time4)
                .h2kTimestamp(time4)
                .firstVisitedPage(url)
                .eventsCounter(1)
                .lastEventDateTime(time4)
                .lastVisitedPage(url)
                .sessionDuration(null)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);
        finalSession = divoltSession2 + "#5";
        getWebSessionCheckerForSession(finalSession, sessions)
                .sessionId(finalSession)
                .Userid(user)
                .record_type("consolidate-session")
                .record_id(finalSession)
                .firstEventDateTime(time5)
                .h2kTimestamp(time5)
                .firstVisitedPage(url)
                .eventsCounter(1)
                .lastEventDateTime(time5)
                .lastVisitedPage(url)
                .sessionDuration(null)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);
    }

    @Test
    public void testRewind2DivoltId2(RestHighLevelClient esclient, DockerComposeContainer container)
            throws Exception {
        final String url = "https://orexad.preprod.group-iph.com/fr/entretien-de-fluides/c-20-50-10";
        final String divoltSession = "0:kfdxb7hf:U4e3OplHDO8Hda8yIS3O2iCdBOcVE_al";
        final String divoltSession2 = "0:kfdxb7hf:alpha";
        final String user = "user";
        final TestRunner testRunner = newTestRunner(container);
        testRunner.assertValid();
        //first run
        final long time1 = 1601629314416L;
        final long time2 = time1 + (SESSION_TIMEOUT + 1L) * 1000L;
        final long time3 = time2 + (SESSION_TIMEOUT + 1L) * 1000L;
        final long time4 = time3 + (SESSION_TIMEOUT + 1L) * 1000L;
        final long time5 = time4 + (SESSION_TIMEOUT + 1L) * 1000L;
        List<Long> times = Arrays.asList(time1, time2, time3, time4, time5);
        testRunner.enqueue(createEvents(url, divoltSession, user, times));
        testRunner.enqueue(createEvents(url, divoltSession2, user, times));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);
        testRunner.assertOutputRecordsCount(10);
        MockRecord session_1 = getFirstRecordWithId(divoltSession, testRunner.getOutputRecords());
        MockRecord session_5 = getFirstRecordWithId(divoltSession + "#5", testRunner.getOutputRecords());

        new WebSessionChecker(session_1).sessionId(divoltSession)
                .Userid(user)
                .record_type("consolidate-session")
                .record_id(divoltSession)
                .firstEventDateTime(time1)
                .h2kTimestamp(time1)
                .firstVisitedPage(url)
                .eventsCounter(1)
                .lastEventDateTime(time1)
                .lastVisitedPage(url)
                .sessionDuration(null)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);

        new WebSessionChecker(session_5).sessionId(divoltSession + "#5")
                .Userid(user)
                .record_type("consolidate-session")
                .record_id(divoltSession + "#5")
                .firstEventDateTime(time5)
                .h2kTimestamp(time5)
                .firstVisitedPage(url)
                .eventsCounter(1)
                .lastEventDateTime(time5)
                .lastVisitedPage(url)
                .sessionDuration(null)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);

        injectSessionsThenRefresh(testRunner.getOutputRecords());
        SearchResponse rsp = getAllSessionsAfterRefreshing(esclient);
        assertEquals(10, rsp.getHits().getTotalHits().value);
        //rewind from time1
        times = Arrays.asList(time1, time2);
        testRunner.clearQueues();
        testRunner.enqueue(createEvents(url, divoltSession, user, times));
        testRunner.enqueue(createEvents(url, divoltSession2, user, times));
        testRunner.run();
        testRunner.assertOutputErrorCount(0);
        testRunner.assertOutputRecordsCount(4);
        List<MockRecord> outputSessions = testRunner.getOutputRecords();
        rsp = getAllSessionsAfterRefreshing(esclient);
        assertEquals(2, rsp.getHits().getTotalHits().value);
        injectSessionsWithoutRefreshing(outputSessions);

        //end of rewind should be as start
        times = Arrays.asList(time3, time4, time5);
        testRunner.clearQueues();
        testRunner.enqueue(createEvents(url, divoltSession, user, times));
        testRunner.enqueue(createEvents(url, divoltSession2, user, times));
        testRunner.run();
        testRunner.assertOutputErrorCount(0);
        testRunner.assertOutputRecordsCount(8);
        outputSessions = testRunner.getOutputRecords();
        rsp = getAllSessionsAfterRefreshing(esclient);
        assertEquals(4, rsp.getHits().getTotalHits().value);
        injectSessionsThenRefresh(outputSessions);
        rsp = getAllSessionsAfterRefreshing(esclient);
        assertEquals(10, rsp.getHits().getTotalHits().value);
        List<WebSession> sessions = ElasticsearchServiceUtil.getAllSessions(elasticsearchClientService, esclient);
        String finalSession = divoltSession;
        getWebSessionCheckerForSession(finalSession, sessions)
                .sessionId(finalSession)
                .Userid(user)
                .record_type("consolidate-session")
                .record_id(finalSession)
                .firstEventDateTime(time1)
                .h2kTimestamp(time1)
                .firstVisitedPage(url)
                .eventsCounter(1)//TODO system de checking Long != Int
                .lastEventDateTime(time1)
                .lastVisitedPage(url)
                .sessionDuration(null)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);
        finalSession = divoltSession + "#2";
        getWebSessionCheckerForSession(finalSession, sessions)
                .sessionId(finalSession)
                .Userid(user)
                .record_type("consolidate-session")
                .record_id(finalSession)
                .firstEventDateTime(time2)
                .h2kTimestamp(time2)
                .firstVisitedPage(url)
                .eventsCounter(1)
                .lastEventDateTime(time2)
                .lastVisitedPage(url)
                .sessionDuration(null)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);
        finalSession = divoltSession + "#3";
        getWebSessionCheckerForSession(finalSession, sessions)
                .sessionId(finalSession)
                .Userid(user)
                .record_type("consolidate-session")
                .record_id(finalSession)
                .firstEventDateTime(time3)
                .h2kTimestamp(time3)
                .firstVisitedPage(url)
                .eventsCounter(1)
                .lastEventDateTime(time3)
                .lastVisitedPage(url)
                .sessionDuration(null)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);
        finalSession = divoltSession + "#4";
        getWebSessionCheckerForSession(finalSession, sessions)
                .sessionId(finalSession)
                .Userid(user)
                .record_type("consolidate-session")
                .record_id(finalSession)
                .firstEventDateTime(time4)
                .h2kTimestamp(time4)
                .firstVisitedPage(url)
                .eventsCounter(1)
                .lastEventDateTime(time4)
                .lastVisitedPage(url)
                .sessionDuration(null)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);
        finalSession = divoltSession + "#5";
        getWebSessionCheckerForSession(finalSession, sessions)
                .sessionId(finalSession)
                .Userid(user)
                .record_type("consolidate-session")
                .record_id(finalSession)
                .firstEventDateTime(time5)
                .h2kTimestamp(time5)
                .firstVisitedPage(url)
                .eventsCounter(1)
                .lastEventDateTime(time5)
                .lastVisitedPage(url)
                .sessionDuration(null)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);
        finalSession = divoltSession2;
        getWebSessionCheckerForSession(finalSession, sessions)
                .sessionId(finalSession)
                .Userid(user)
                .record_type("consolidate-session")
                .record_id(finalSession)
                .firstEventDateTime(time1)
                .h2kTimestamp(time1)
                .firstVisitedPage(url)
                .eventsCounter(1)
                .lastEventDateTime(time1)
                .lastVisitedPage(url)
                .sessionDuration(null)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);
        finalSession = divoltSession2 + "#2";
        getWebSessionCheckerForSession(finalSession, sessions)
                .sessionId(finalSession)
                .Userid(user)
                .record_type("consolidate-session")
                .record_id(finalSession)
                .firstEventDateTime(time2)
                .h2kTimestamp(time2)
                .firstVisitedPage(url)
                .eventsCounter(1)
                .lastEventDateTime(time2)
                .lastVisitedPage(url)
                .sessionDuration(null)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);
        finalSession = divoltSession2 + "#3";
        getWebSessionCheckerForSession(finalSession, sessions)
                .sessionId(finalSession)
                .Userid(user)
                .record_type("consolidate-session")
                .record_id(finalSession)
                .firstEventDateTime(time3)
                .h2kTimestamp(time3)
                .firstVisitedPage(url)
                .eventsCounter(1)
                .lastEventDateTime(time3)
                .lastVisitedPage(url)
                .sessionDuration(null)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);
        finalSession = divoltSession2 + "#4";
        getWebSessionCheckerForSession(finalSession, sessions)
                .sessionId(finalSession)
                .Userid(user)
                .record_type("consolidate-session")
                .record_id(finalSession)
                .firstEventDateTime(time4)
                .h2kTimestamp(time4)
                .firstVisitedPage(url)
                .eventsCounter(1)
                .lastEventDateTime(time4)
                .lastVisitedPage(url)
                .sessionDuration(null)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);
        finalSession = divoltSession2 + "#5";
        getWebSessionCheckerForSession(finalSession, sessions)
                .sessionId(finalSession)
                .Userid(user)
                .record_type("consolidate-session")
                .record_id(finalSession)
                .firstEventDateTime(time5)
                .h2kTimestamp(time5)
                .firstVisitedPage(url)
                .eventsCounter(1)
                .lastEventDateTime(time5)
                .lastVisitedPage(url)
                .sessionDuration(null)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);
    }

    /**
     * The purpose of this test is to ensure that events and session stored es are the same.
     * That they are injected to ES directly as input of the processor
     * Or that they have been fetched from remote in ES.
     *
     * @param esclient
     * @param container
     * @throws Exception
     */
    @Test
    public void testConsistenceInEs(RestHighLevelClient esclient, DockerComposeContainer container)
            throws Exception {
        final String url = "https://orexad.preprod.group-iph.com/fr/entretien-de-fluides/c-20-50-10";
        final String divoltSession = "0:kfdxb7hf:U4e3OplHDO8Hda8yIS3O2iCdBOcVE_al";
        final String user = "user";
        final TestRunner testRunner = newTestRunner(container);
        testRunner.assertValid();
        //first run
        final long time1 = 1601629314416L;
        final long time2 = time1 + 1L;
        final long time3 = time2 + 1L;
        final long time4 = time3 + 1L;
        final long time5 = time4 + (SESSION_TIMEOUT + 1L) * 1000L;
        final long time6 = time5 + 1L;
        final long time7 = time6 + 1L;
        List<Long> times = Arrays.asList(time1, time2, time3, time4, time5, time6, time7);
        testRunner.enqueue(createEvents(url, divoltSession, user, times));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);
        testRunner.assertOutputRecordsCount(2);
        injectSessionsThenRefresh(testRunner.getOutputRecords());

        SearchResponse webSessionRsp = getAllSessionsAfterRefreshing(esclient);
        assertEquals(2, webSessionRsp.getHits().getTotalHits().value);
        SearchHit[] webSessionsDocs = webSessionRsp.getHits().getHits();
        SearchHit session = Arrays.stream(webSessionsDocs)
                .filter(hit -> {
                    return hit.getId().equals(divoltSession);
                })
                .findFirst()
                .get();
        SearchHit session2 = Arrays.stream(webSessionsDocs)
                .filter(hit -> {
                    return hit.getId().equals(divoltSession + "#2");
                })
                .findFirst()
                .get();

        SearchResponse webEventRsp = getAllEventsAfterRefreshing(esclient);
        assertEquals(7, webEventRsp.getHits().getTotalHits().value);
        SearchHit[] eventsDoc = webEventRsp.getHits().getHits();
        SearchHit event1 = Arrays.stream(eventsDoc)
                .filter(hit -> {
                    return hit.getId().equals(buildId(time1, divoltSession));
                })
                .findFirst()
                .get();
        SearchHit event2 = Arrays.stream(eventsDoc)
                .filter(hit -> {
                    return hit.getId().equals(buildId(time2, divoltSession));
                })
                .findFirst()
                .get();
        SearchHit event3 = Arrays.stream(eventsDoc)
                .filter(hit -> {
                    return hit.getId().equals(buildId(time3, divoltSession));
                })
                .findFirst()
                .get();
        SearchHit event4 = Arrays.stream(eventsDoc)
                .filter(hit -> {
                    return hit.getId().equals(buildId(time4, divoltSession));
                })
                .findFirst()
                .get();
        SearchHit event5 = Arrays.stream(eventsDoc)
                .filter(hit -> {
                    return hit.getId().equals(buildId(time5, divoltSession));
                })
                .findFirst()
                .get();
        SearchHit event6 = Arrays.stream(eventsDoc)
                .filter(hit -> {
                    return hit.getId().equals(buildId(time6, divoltSession));
                })
                .findFirst()
                .get();
        SearchHit event7 = Arrays.stream(eventsDoc)
                .filter(hit -> {
                    return hit.getId().equals(buildId(time7, divoltSession));
                })
                .findFirst()
                .get();


        //TODO test the values of documents

        //rewind from time3 to time4
        times = Arrays.asList(time3, time4);
        testRunner.clearQueues();
        testRunner.enqueue(createEvents(url, divoltSession, user, times));
        testRunner.run();
        testRunner.assertOutputErrorCount(0);
        testRunner.assertOutputRecordsCount(1);
        injectSessionsWithoutRefreshing(testRunner.getOutputRecords());
        assertEquals(1, getAllSessionsAfterRefreshing(esclient).getHits().getTotalHits().value);
        assertEquals(7, getAllEventsAfterRefreshing(esclient).getHits().getTotalHits().value);

        //rewind from time5 to time7
        times = Arrays.asList(time5, time6, time7);
        testRunner.clearQueues();
        testRunner.enqueue(createEvents(url, divoltSession, user, times));
        testRunner.run();
        testRunner.assertOutputErrorCount(0);
        testRunner.assertOutputRecordsCount(2);
        injectSessionsWithoutRefreshing(testRunner.getOutputRecords());

        SearchResponse webSessionRsp2 = getAllSessionsAfterRefreshing(esclient);
        assertEquals(2, webSessionRsp2.getHits().getTotalHits().value);
        SearchHit[] webSessionsDocs2 = webSessionRsp2.getHits().getHits();
        SearchHit session2_1 = Arrays.stream(webSessionsDocs2)
                .filter(hit -> {
                    return hit.getId().equals(divoltSession);
                })
                .findFirst()
                .get();
        SearchHit session2_2 = Arrays.stream(webSessionsDocs2)
                .filter(hit -> {
                    return hit.getId().equals(divoltSession + "#2");
                })
                .findFirst()
                .get();

        SearchResponse webEventRsp2 = getAllEventsAfterRefreshing(esclient);
        assertEquals(7, webEventRsp2.getHits().getTotalHits().value);
        SearchHit[] eventsDoc2 = webEventRsp2.getHits().getHits();
        SearchHit event2_1 = Arrays.stream(eventsDoc2)
                .filter(hit -> {
                    return hit.getId().equals(buildId(time1, divoltSession));
                })
                .findFirst()
                .get();
        SearchHit event2_2 = Arrays.stream(eventsDoc2)
                .filter(hit -> {
                    return hit.getId().equals(buildId(time2, divoltSession));
                })
                .findFirst()
                .get();
        SearchHit event2_3 = Arrays.stream(eventsDoc2)
                .filter(hit -> {
                    return hit.getId().equals(buildId(time3, divoltSession));
                })
                .findFirst()
                .get();
        SearchHit event2_4 = Arrays.stream(eventsDoc2)
                .filter(hit -> {
                    return hit.getId().equals(buildId(time4, divoltSession));
                })
                .findFirst()
                .get();
        SearchHit event2_5 = Arrays.stream(eventsDoc2)
                .filter(hit -> {
                    return hit.getId().equals(buildId(time5, divoltSession));
                })
                .findFirst()
                .get();
        SearchHit event2_6 = Arrays.stream(eventsDoc2)
                .filter(hit -> {
                    return hit.getId().equals(buildId(time6, divoltSession));
                })
                .findFirst()
                .get();
        SearchHit event2_7 = Arrays.stream(eventsDoc2)
                .filter(hit -> {
                    return hit.getId().equals(buildId(time7, divoltSession));
                })
                .findFirst()
                .get();
        assertEquals(-1, event1.getVersion());
        assertEquals(-1, event2_1.getVersion());
        assertEquals(-1, event4.getVersion());
        assertEquals(-1, event2_4.getVersion());
        assertEquals(-1, event7.getVersion());
        assertEquals(-1, event2_7.getVersion());
        assertMapsAreEquals(event1.getSourceAsMap(), event2_1.getSourceAsMap());
        assertMapsAreEquals(event2.getSourceAsMap(), event2_2.getSourceAsMap());
        assertMapsAreEqualsIgnoringSomeKeys(event3.getSourceAsMap(), event2_3.getSourceAsMap(), FieldDictionary.RECORD_TIME);
        assertMapsAreEqualsIgnoringSomeKeys(event4.getSourceAsMap(), event2_4.getSourceAsMap(), FieldDictionary.RECORD_TIME);
        assertMapsAreEqualsIgnoringSomeKeys(event5.getSourceAsMap(), event2_5.getSourceAsMap(), FieldDictionary.RECORD_TIME);
        assertMapsAreEqualsIgnoringSomeKeys(event6.getSourceAsMap(), event2_6.getSourceAsMap(), FieldDictionary.RECORD_TIME);
        assertMapsAreEqualsIgnoringSomeKeys(event7.getSourceAsMap(), event2_7.getSourceAsMap(), FieldDictionary.RECORD_TIME);

        assertEquals(-1, session.getVersion());
        assertEquals(-1, session2.getVersion());
        assertEquals(-1, session2_1.getVersion());
        assertEquals(-1, session2_2.getVersion());
        assertMapsAreEqualsIgnoringSomeKeys(session.getSourceAsMap(), session2_1.getSourceAsMap() , FieldDictionary.RECORD_TIME, "@timestamp");
        assertMapsAreEqualsIgnoringSomeKeys(session2.getSourceAsMap(), session2_2.getSourceAsMap(), FieldDictionary.RECORD_TIME, "@timestamp");
    }

    private WebSessionChecker getWebSessionCheckerForSession(final String divoltSession,
                                                             final List<WebSession> sessions) {
        WebSession session = sessions.stream().filter(s -> s.getSessionId().equals(divoltSession)).findFirst().get();
        return new WebSessionChecker(session);
    }

    private List<Record> createEvents(String url, String divoltSession, String user, List<Long> times) {
        List<Record> events = new ArrayList<>();
        for (Long time : times) {
            String id = buildId(time, divoltSession);
            events.add(new WebEvent(id, divoltSession, user, time, url));
        }
        return events;
    }
    private String buildId(long time, String divoltSession) {
        return "event-" + time + "-" + divoltSession;
    }

    private MockRecord getFirstRecordWithId(final String id, final List<MockRecord> records) {
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
            throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(new IncrementalWebSession());
        final String FIELDS_TO_RETURN = Stream.of("partyId", "B2BUnit").collect(Collectors.joining(","));
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
