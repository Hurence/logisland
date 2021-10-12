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
package com.hurence.logisland.processor.webanalytics;

import com.hurence.junit5.extension.Es7DockerExtension;
import com.hurence.logisland.classloading.PluginProxy;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.processor.elasticsearch.BulkAddElasticsearch;
import com.hurence.logisland.processor.webanalytics.modele.TestMappings;
import com.hurence.logisland.processor.webanalytics.modele.WebSession;
import com.hurence.logisland.processor.webanalytics.util.ElasticsearchServiceUtil;
import com.hurence.logisland.processor.webanalytics.util.WebEvent;
import com.hurence.logisland.processor.webanalytics.util.WebSessionChecker;
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
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.hurence.logisland.processor.webanalytics.IncrementalWebSession.defaultOutputFieldNameForEsIndex;
import static com.hurence.logisland.processor.webanalytics.util.ElasticsearchServiceUtil.*;
import static com.hurence.logisland.processor.webanalytics.util.UtilsTest.assertMapsAreEqualsIgnoringSomeKeys;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test incremental web-session processor.
 */
@ExtendWith({Es7DockerExtension.class})
public class IncrementalWebSessionIT {
    private static Logger logger = LoggerFactory.getLogger(IncrementalWebSessionIT.class);
    private static boolean jobDoDeleteSessions = false;//we decided currently to never delete sessions

    private final long SESSION_TIMEOUT = 1800L;
    private ElasticsearchClientService elasticsearchClientService;
    private CacheService<String, WebSession> lruCache;
    private IncrementalWebSession proc;


    private final String USER1 = "user1";
    private final String USER2 = "user2";
    private final String USER3 = "user3";

    private final String SESSION1 = "session1";
    private final String SESSION2 = "session2";
    private final String SESSION3 = "session3";

    private final String URL = "url";

    private final DockerComposeContainer container;
    private final RestHighLevelClient esclient;

    private final TestRunner bulkRunner;

    IncrementalWebSessionIT(DockerComposeContainer container,
                            RestHighLevelClient esclient) throws InitializationException {
        this.container = container;
        this.esclient = esclient;
        this.bulkRunner = newBulkAddTestRunner(container);
        this.elasticsearchClientService = PluginProxy.unwrap(bulkRunner.getProcessContext()
                .getPropertyValue(BulkAddElasticsearch.ELASTICSEARCH_CLIENT_SERVICE).asControllerService());
    }

    @BeforeEach
    public void clean() throws IOException {
        if (proc != null) {
            this.proc.resetNumberOfRewindForProcInstance();
        }
        try {
            Thread.sleep(500L);
            Set<String> indices = Arrays.stream(esclient.indices().get(
                    new GetIndexRequest("*"),
                    RequestOptions.DEFAULT).getIndices()
            ).collect(Collectors.toSet());
            while (!indices.isEmpty()) {
                logger.info("Will delete following indices :{}", indices);
                DeleteIndexRequest deleteRequest = new DeleteIndexRequest(indices.toArray(new String[0]));
                Assert.assertTrue(esclient.indices().delete(deleteRequest, RequestOptions.DEFAULT).isAcknowledged());
                indices = Arrays.stream(esclient.indices().get(
                        new GetIndexRequest("*"),
                        RequestOptions.DEFAULT).getIndices()
                ).collect(Collectors.toSet());
            }
        } catch (Exception ex) {
//            ex.printStackTrace();
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
        AcknowledgedResponse putTemplateResponse = esclient.indices().putTemplate(templateRequest, RequestOptions.DEFAULT);
        logger.info("putTemplateResponse is " + putTemplateResponse);

    }


    /**
     *  T2 is one second later than T1
     *
     * events :                   T1--T2--T3
     * changement de session:     ----------
     * batch 1: T1
     * batch 2: T2
     * batch 3: T3
     * resultat attendu: S1 -> T1,T2, T3
     *
     *
     * @throws Exception
     */
    @Test
    public void testUpdateOneWebSession()
            throws Exception
    {
        String URL1 = "URL1";
        String URL2 = "URL2";
        String URL3 = "URL3";
        Instant firstEvent = Instant.now().minusSeconds(8);
        TestRunner testRunner = newTestRunner(container);
        testRunner.assertValid();
        testRunner.enqueue(Arrays.asList(new WebEvent("1", SESSION1, USER1, firstEvent.toEpochMilli(), URL1)));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(2);
        injectOutputIntoEsWithoutRefreshing(testRunner.getOutputRecords());

        Instant nextEvent = firstEvent.plusSeconds(2);
        testRunner.clearQueues();
        testRunner.enqueue(Arrays.asList(new WebEvent("2", SESSION1, USER1, nextEvent.toEpochMilli(), URL2)));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(2);
        injectOutputIntoEsWithoutRefreshing(testRunner.getOutputRecords());

        Instant lastEvent = nextEvent.plusSeconds(4);
        testRunner.clearQueues();
        testRunner.enqueue(Arrays.asList(new WebEvent("3", SESSION1, USER1, lastEvent.toEpochMilli(), URL3)));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(2);
        testRunner.assertOutputErrorCount(0);
        injectOutputIntoEsWithoutRefreshing(testRunner.getOutputRecords());

        List<WebSession> sessions = ElasticsearchServiceUtil.getAllSessions(
                this.elasticsearchClientService, esclient);
        Set<String> ids = sessions.stream().map(WebSession::getSessionId).collect(Collectors.toSet());
        Assert.assertTrue(ids.contains(SESSION1));

        final MockRecord session = getFirstRecordWithId(SESSION1, testRunner.getOutputRecords());
        new WebSessionChecker(session).sessionId(SESSION1)
                .Userid(USER1)
                .record_type("consolidate-session")
                .record_id(SESSION1)
                .firstEventDateTime(firstEvent.toEpochMilli())
                .h2kTimestamp(firstEvent.toEpochMilli())
                .firstVisitedPage(URL1)
                .eventsCounter(3)
                .lastEventDateTime(lastEvent.toEpochMilli())
                .lastVisitedPage(URL3)
                .sessionDuration(Duration.between(firstEvent, lastEvent).getSeconds())
                .is_sessionActive(true)
                .sessionInactivityDuration(null);
        Assert.assertEquals(0, this.proc.getNumberOfRewindForProcInstance());

    }

    /**
     * T1 is now - SESSION_TIMEOUT - 1. T2 is one second later than T2.
     * events :                   T1--T2
     * changement de session:     ------
     * batch 1: T1   session should be tagged as inactive because now - T1 > SESSION_TIMEOUT.
     * batch 2: T2   session shoulb be updated even if it is already considered as inactive.
     * resultat attendu: S1 -> T1,T2
     *
     *
     * @throws Exception
     */
    @Test
    public void testUpdateOneWebSessionInactive()
            throws Exception
    {
        int eventCount = 0;
        String URL1 = "URL1";
        String URL2 = "URL2";
        Instant firstEvent = Instant.now().minusSeconds(SESSION_TIMEOUT+1);
        TestRunner testRunner = newTestRunner(container);
        testRunner.assertValid();
        testRunner.enqueue(Arrays.asList(new WebEvent(String.valueOf(eventCount++), SESSION1, USER1, firstEvent.toEpochMilli(), URL1)));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(2);
        injectOutputIntoEsThenSleepThenRefresh(testRunner.getOutputRecords());

        Record doc = ElasticsearchServiceUtil.getSessionFromEs(elasticsearchClientService, esclient,
                SESSION1,
                TestMappings.sessionInternalFields);
        new WebSessionChecker(doc)
                .lastVisitedPage(URL1)
                .sessionDuration(null)
                .eventsCounter(1)
                .firstEventDateTime(firstEvent.toEpochMilli())
                .lastEventDateTime(firstEvent.toEpochMilli())
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);

        // Update web session with timestamp 1s before timeout.
        Instant instantOneSecondBeforeTimeout = firstEvent.plusSeconds(1);
        testRunner.clearQueues();
        testRunner.enqueue(Arrays.asList(new WebEvent(String.valueOf(eventCount++), SESSION1, USER1, instantOneSecondBeforeTimeout.toEpochMilli(), URL2)));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(2);
        injectOutputIntoEsThenRefresh(testRunner.getOutputRecords());

        doc = ElasticsearchServiceUtil.getSessionFromEs(elasticsearchClientService, esclient,
                SESSION1,
                TestMappings.sessionInternalFields);
        new WebSessionChecker(doc)
                .lastVisitedPage(URL2)
                .sessionDuration(instantOneSecondBeforeTimeout.getEpochSecond() - firstEvent.getEpochSecond())
                .eventsCounter(2)
                .firstEventDateTime(firstEvent.toEpochMilli())
                .lastEventDateTime(instantOneSecondBeforeTimeout.toEpochMilli())
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);
        Assert.assertEquals(0, this.proc.getNumberOfRewindForProcInstance());
    }

    /**
     * T1 is now - SESSION_TIMEOUT - 2. T2 is now.
     * events :                   T1--T2
     * changement de session:     ----X
     * batch 1: T1   session should be tagged as inactive because now - T1 > SESSION_TIMEOUT.
     * batch 2: T2   a new session should be created as more than SESSION_TIMEOUT seconds has passed
     * resultat attendu: S1 -> T1, S1#2 -> T2
     *
     *
     * @throws Exception
     */
    @Test
    public void testUpdateOneWebSessionTimeout()
            throws Exception
    {

        int eventCount = 0;
        String URL1 = "URL1";
        String URL2 = "URL2";
        // Create a web session with timestamp 2s before timeout.
        Instant firstEvent = Instant.now().minusSeconds(SESSION_TIMEOUT + 2);
        TestRunner testRunner = newTestRunner(container);
        testRunner.assertValid();
        testRunner.enqueue(Arrays.asList(new WebEvent(String.valueOf(eventCount++), SESSION1, USER1, firstEvent.toEpochMilli(), URL1)));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(2);
        injectOutputIntoEsWithoutRefreshing(testRunner.getOutputRecords());

        //Update web session with a timestamp that is timeout.
        Instant timedoutEvent = Instant.now();
        testRunner.clearQueues();
        testRunner.enqueue(Arrays.asList(new WebEvent(String.valueOf(eventCount++), SESSION1, USER1, timedoutEvent.toEpochMilli(), URL2)));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(2 + 1);//1 event et 2 session
        testRunner.assertOutputErrorCount(0);
        injectOutputIntoEsWithoutRefreshing(testRunner.getOutputRecords());

        List<WebSession> sessions = ElasticsearchServiceUtil.getAllSessions(
                this.elasticsearchClientService, esclient);
        Set<String> ids = sessions.stream().map(WebSession::getSessionId).collect(Collectors.toSet());
        Assert.assertTrue(ids.contains(SESSION1));

        Record doc = ElasticsearchServiceUtil.getSessionFromEs(elasticsearchClientService, esclient,
                SESSION1,
                TestMappings.sessionInternalFields);
        new WebSessionChecker(doc).sessionId(SESSION1)
                .Userid(USER1)
                .record_type("consolidate-session")
                .record_id(SESSION1)
                .firstEventDateTime(firstEvent.toEpochMilli())
                .h2kTimestamp(firstEvent.toEpochMilli())
                .firstVisitedPage(URL1)
                .eventsCounter(1)
                .lastEventDateTime(firstEvent.toEpochMilli())
                .lastVisitedPage(URL1)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);

        final String EXTRA_SESSION = SESSION1+"#2";
        doc = ElasticsearchServiceUtil.getSessionFromEs(elasticsearchClientService, esclient,
                EXTRA_SESSION,
                TestMappings.sessionInternalFields);
        new WebSessionChecker(doc).sessionId(EXTRA_SESSION)
                .Userid(USER1)
                .record_type("consolidate-session")
                .record_id(EXTRA_SESSION)
                .firstEventDateTime(timedoutEvent.toEpochMilli())
                .h2kTimestamp(timedoutEvent.toEpochMilli())
                .firstVisitedPage(URL2)
                .eventsCounter(1)
                .lastEventDateTime(timedoutEvent.toEpochMilli())
                .lastVisitedPage(URL2)
                .is_sessionActive(true);
        Assert.assertEquals(0, this.proc.getNumberOfRewindForProcInstance());
    }

    /**
     *
     * T1 is now - SESSION_TIMEOUT - 2. T2 is now.
     * events :                   T1--T2--T3--T4--T5--T6--T7--T8--T9
     * changement de session:     ----------------X-----------X-----
     * batch 1: T1,T2,T3,T4,T5,T6,T7
     *          S1 -> T1,T2,T3,T4
     *          S1#2 -> T5,T6,T7
     * batch 2: T8,T9
     *          S1#2 -> T5,T6,T7
     *          S1#3 -> T8,T9
     *
     *
     * @throws Exception
     */
    @Test
    public void testBugWhenNotFlushingMappingAndHighFrequencyBatch()
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
        testRunner.assertOutputRecordsCount(7 + 2);
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
        injectOutputIntoEsWithoutRefreshing(testRunner.getOutputRecords());



        //second run
        final long time8 = 1601882662402L;
        final long time9 = 1601882676592L;
        times = Arrays.asList(time8, time9);
        testRunner.clearQueues();
        testRunner.enqueue(createEvents(url, divoltSession, user, times));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);
        testRunner.assertOutputRecordsCount(2 + 2);

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
        Assert.assertEquals(0, this.proc.getNumberOfRewindForProcInstance());
    }

    /**
     *
     * T1 is now - SESSION_TIMEOUT - 2. T2 is now.
     * events :                   T1--T2--T3--T4--T5--T6--T7--T8--T9--T10
     * changement de session:     ----X---X---X---X---X------------------
     * batch 1: T1,T2,T3,T4,T5
     *          S1 -> T1
     *          S1#2 -> T2
     *          S1#3 -> T3
     *          S1#4 -> T4
     *          S1#5 -> T5
     * batch 2: T6,T7,T8
     *          S1#5 -> T5
     *          S1#6 -> T6,T7,T8
     * batch 2: T9,T10
     *          S1#6 -> T9,T10 (+ T6,T7,T8, qui ne sont pas retournés)
     *
     * @throws Exception
     */
    @Test
    public void testBugWhenNotFlushingMappingAndHighFrequencyBatch2()
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
        testRunner.assertOutputRecordsCount(5 + 5);//session + event
        MockRecord session1 = getFirstRecordWithId(session, testRunner.getOutputRecords());
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

        injectOutputIntoEsWithoutRefreshing(testRunner.getOutputRecords());
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
        testRunner.assertOutputRecordsCount(2 + 3);//session + event
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
        injectOutputIntoEsWithoutRefreshing(testRunner.getOutputRecords());
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
        testRunner.assertOutputRecordsCount(1 + 2);//session + event

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
        Assert.assertEquals(0, this.proc.getNumberOfRewindForProcInstance());
    }


    /**
     * Here we simulate a rewind by resetting cache and reprocessing events
     *
     * events :                   T1--T2--T3--T4--T5
     * changement de session:     ----X---X---X---X-
     * batch 1: T1,T2,T3,T4 et T5
     *          S1 -> T1
     *          S1#2 -> T2
     *          S1#3 -> T3
     *          S1#4 -> T4
     *          S1#5 -> T5
     *
     * Simulate a rewind by reseting cache
     * batch 2: T1,T2
     *          S1 -> T1
     *          S1#2 -> T2
     * batch 3: T3,T4,T5
     *          S1#2 -> T2
     *          S1#3 -> T3
     *          S1#4 -> T4
     *          S1#5 -> T5
     *
     *
     * @throws Exception
     */
    @Test
    public void testRewind1()
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
        testRunner.assertOutputRecordsCount(5 + 5);
        MockRecord session1 = getFirstRecordWithId(session, testRunner.getOutputRecords());
        MockRecord session5 = getFirstRecordWithId(session + "#5", testRunner.getOutputRecords());
        Assert.assertEquals(0, this.proc.getNumberOfRewindForProcInstance());
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
        injectOutputIntoEsThenSleepThenRefresh(testRunner.getOutputRecords());
        SearchResponse rsp = getAllSessionsAfterRefreshing(esclient);
        assertEquals(5, rsp.getHits().getTotalHits().value);

        //rewind batch1
        times = Arrays.asList(time1, time2);
        events = createEvents(url, session, user, times);
        testRunner.clearQueues();
        resetCache(testRunner);
        testRunner.enqueue(events);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);
        testRunner.assertOutputRecordsCount(2 + 2);
        session1 = getFirstRecordWithId(session, testRunner.getOutputRecords());
        MockRecord session2 = getFirstRecordWithId(session + "#2", testRunner.getOutputRecords());
        Assert.assertEquals(1, this.proc.getNumberOfRewindForProcInstance());
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
        injectOutputIntoEsWithoutRefreshing(testRunner.getOutputRecords());

        //rewind batch 2
        times = Arrays.asList(time3, time4, time5);
        events = createEvents(url, session, user, times);
        testRunner.clearQueues();
        testRunner.enqueue(events);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);
        testRunner.assertOutputRecordsCount(4 + 3);
        session2 = getFirstRecordWithId(session + "#2", testRunner.getOutputRecords());
        MockRecord session3 = getFirstRecordWithId(session + "#3", testRunner.getOutputRecords());
        MockRecord session4 = getFirstRecordWithId(session + "#4", testRunner.getOutputRecords());
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

        injectOutputIntoEsWithoutRefreshing(Arrays.asList(session2, session3, session4, session5));


        rsp = getAllSessionsAfterRefreshing(esclient);
        assertEquals(5, rsp.getHits().getTotalHits().value);
        Assert.assertEquals(1, this.proc.getNumberOfRewindForProcInstance());
    }

    /**
     * Here we simulate a rewind by resetting cache and reprocessing events
     *
     * events :                   T1--T2--T3--T4--T5
     * changement de session:     ----X---X---X---X-
     * batch 1: T1,T2,T3,T4 et T5
     *          S1 -> T1
     *          S1#2 -> T2
     *          S1#3 -> T3
     *          S1#4 -> T4
     *          S1#5 -> T5
     *
     * Simulate a rewind by reseting cache
     * batch 2: T3,T4,T5
     *          S1#3 -> T3
     *          S1#4 -> T4
     *          S1#5 -> T5
     *
     *
     * @throws Exception
     */
    @Test
    public void testRewind2()
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
        testRunner.assertOutputRecordsCount(5 + 5);
        MockRecord session1 = getFirstRecordWithId(session, testRunner.getOutputRecords());
        MockRecord session5 = getFirstRecordWithId(session + "#5", testRunner.getOutputRecords());
        Assert.assertEquals(0, this.proc.getNumberOfRewindForProcInstance());
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

        injectOutputIntoEsThenSleepThenRefresh(testRunner.getOutputRecords());
        SearchResponse rsp = getAllSessionsAfterRefreshing(esclient);
        assertEquals(5, rsp.getHits().getTotalHits().value);
        //rewind from time3
        times = Arrays.asList(time3, time4, time5);
        events = createEvents(url, session, user, times);
        testRunner.clearQueues();
        resetCache(testRunner);
        testRunner.enqueue(events);
        testRunner.run();
        testRunner.assertOutputRecordsCount(3 + 3);
        List<MockRecord> outputSessions = testRunner.getOutputRecords();
        injectOutputIntoEsWithoutRefreshing(outputSessions);

        rsp = getAllSessionsAfterRefreshing(esclient);
        assertEquals(5, rsp.getHits().getTotalHits().value);
        Assert.assertEquals(1, this.proc.getNumberOfRewindForProcInstance());
    }

    /**
     * Here we simulate a rewind by resetting cache and reprocessing events.
     * We suppose that some events/session were effectively saved but some not before the job crash (power cut).
     *
     * events :                   T1--T2--T3--T4--T5
     * changement de session:     ----X---X---X---X-
     * batch 1: T1,T2,T3,T4 et T5
     *          S1 -> T1
     *          S1#2 -> T2
     *          S1#3 -> T3
     *          S1#4 -> T4
     *          S1#5 -> T5
     *
     * Simulate a rewind by reseting cache
     * batch 3: T3,T4,T5
     *          S1#3 -> T3
     *          S1#4 -> T4
     *          S1#5 -> T5
     *  => Here we only register session 3 in es
     * Simulate a rewind by reseting cache (because of a power cut offset was not commited previously)
     * batch 3: T3,T4,T5
     *          S1#3 -> T3
     *          S1#4 -> T4
     *          S1#5 -> T5
     *
     * @throws Exception
     */
    @Test
    public void testRewindFailThenRestart()
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
        testRunner.assertOutputRecordsCount(5 + 5);
        MockRecord session1 = getFirstRecordWithId(session, testRunner.getOutputRecords());
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
        injectOutputIntoEsThenSleepThenRefresh(testRunner.getOutputRecords());
        Assert.assertEquals(0, this.proc.getNumberOfRewindForProcInstance());
        //rewind from time3 but fail during registering session so registering only session 3
        times = Arrays.asList(time3, time4, time5);
        events = createEvents(url, session, user, times);
        testRunner.clearQueues();
        resetCache(testRunner);
        testRunner.enqueue(events);
        testRunner.run();
        testRunner.assertOutputRecordsCount(3 + 3);
        MockRecord session3 = getFirstRecordWithId(session + "#3", testRunner.getOutputRecords());
        injectOutputIntoEsWithoutRefreshing(Arrays.asList(session3));
        Assert.assertEquals(1, this.proc.getNumberOfRewindForProcInstance());

        //restart from time3 because offset was not commited
        times = Arrays.asList(time3, time4, time5);
        events = createEvents(url, session, user, times);
        testRunner.clearQueues();
        resetCache(testRunner);
        testRunner.enqueue(events);
        testRunner.run();
        testRunner.assertOutputRecordsCount(3 + 3);
        List<MockRecord> outputSessions = testRunner.getOutputRecords();
        injectOutputIntoEsWithoutRefreshing(outputSessions);

        SearchResponse rsp = getAllSessionsAfterRefreshing(esclient);
        assertEquals(5, rsp.getHits().getTotalHits().value);
        Assert.assertEquals(2, this.proc.getNumberOfRewindForProcInstance());
    }

    /**
     * Here we simulate a rewind by resetting cache and reprocessing events.
     * We suppose that some events/session were effectively saved but some not before the job crash (power cut).
     *
     * events DivolteSession1:                   T1--T2--T3--T4--T5
     * events DivolteSession2:                   T1--T2--T3--T4--T5
     * changement de session:                    ----X---X---X---X-
     * batch 1: DivolteSession1: T1,T2,T3,T4 et T5, DivolteSession2: T1,T2,T3,T4 et T5
     *
     *          DivolteSession1: S1 -> T1     DivolteSession2: S1 -> T1
     *                           S1#2 -> T2                    S1#2 -> T2
     *                           S1#3 -> T3                    S1#3 -> T3
     *                           S1#4 -> T4                    S1#4 -> T4
     *                           S1#5 -> T5                    S1#5 -> T5
     *
     * Simulate a rewind by reseting cache
     * batch 2: DivolteSession1: T1,T2    DivolteSession2: T1,T2
     *
     *          DivolteSession1: S1 -> T1   DivolteSession2: S1 -> T1
     *                           S1#2 -> T2                  S1#2 -> T2
     *
     *  => Here we only register session 3 in es
     * Simulate a rewind by reseting cache (because of a power cut offset was not commited previously)
     * batch 3: DivolteSession1: T3,T4,T5    DivolteSession2: T3,T4,T5
     *
     *          DivolteSession1: S1#3 -> T3  DivolteSession2: S1#3 -> T3
     *                           S1#4 -> T4                   S1#4 -> T4
     *                           S1#5 -> T5                   S1#5 -> T5
     *
     *
     *
     * @throws Exception
     */
    @Test
    public void testRewind2MultipleDivoltId()
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
        testRunner.assertOutputRecordsCount(10 + 10);
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

        injectOutputIntoEsThenSleepThenRefresh(testRunner.getOutputRecords());

        SearchResponse rsp = getAllSessionsAfterRefreshing(esclient);
        assertEquals(10, rsp.getHits().getTotalHits().value);
        Assert.assertEquals(0, this.proc.getNumberOfRewindForProcInstance());
        //rewind from time1
        times = Arrays.asList(time1, time2);
        testRunner.clearQueues();
        resetCache(testRunner);
        testRunner.enqueue(createEvents(url, divoltSession, user, times));
        testRunner.enqueue(createEvents(url, divoltSession2, user, times));
        testRunner.run();
        testRunner.assertOutputErrorCount(0);
        testRunner.assertOutputRecordsCount(4 + 4);
        Assert.assertEquals(1, this.proc.getNumberOfRewindForProcInstance());
        if (jobDoDeleteSessions) {
            rsp = getAllSessionsAfterRefreshing(esclient);
            assertEquals(2, rsp.getHits().getTotalHits().value);
        }


        injectOutputIntoEsWithoutRefreshing(testRunner.getOutputRecords());
        if (jobDoDeleteSessions) {
            rsp = getAllSessionsAfterRefreshing(esclient);
            assertEquals(4, rsp.getHits().getTotalHits().value);
        }
        //end of rewind should be as start
        times = Arrays.asList(time3, time4, time5);
        testRunner.clearQueues();
        testRunner.enqueue(createEvents(url, divoltSession, user, times));
        testRunner.enqueue(createEvents(url, divoltSession2, user, times));
        testRunner.run();
        testRunner.assertOutputErrorCount(0);
        testRunner.assertOutputRecordsCount(8 + 6);
        if (jobDoDeleteSessions) {
            rsp = getAllSessionsAfterRefreshing(esclient);
            assertEquals(4, rsp.getHits().getTotalHits().value);
        }
        injectOutputIntoEsWithoutRefreshing(testRunner.getOutputRecords());
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
                .eventsCounter(1)
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
        Assert.assertEquals(1, this.proc.getNumberOfRewindForProcInstance());
    }


    /**
     * Here we simulate a rewind by resetting cache and reprocessing events.
     * We suppose that some events/session were effectively saved but some not before the job crash (power cut).
     *
     * events DivolteSession1:                   T1--T2--T3--T4--T5--T6--T7
     * events DivolteSession2:                   T1--T2--T3--T4--T5--T6--T7
     * changement de session:                    ----------------X---------
     *
     * batch 1: DivolteSession1: T1,T2,T3,T4,T5,T6,T7
     *          DivolteSession2: T1,T2,T3,T4,T5,T6,T7
     *          DivolteSession3: T1,T2,T3,T4,T5,T6,T7
     *
     * DivolteSession1: S1 -> T1,T2,T3,T4     DivolteSession2: S1 -> T1,T2,T3,T4       DivolteSession3: S1 -> T1,T2,T3,T4
     *                  S1#2 -> T5,T6,T7                       S1#2 -> T5,T6,T7                         S1#2 -> T5,T6,T7
     *
     * Simulate a rewind by reseting cache
     * batch 2: DivolteSession1: T3,T4
     *
     *          DivolteSession1: S1 -> T1,T2,T3,T4
     *
     * batch 3: DivolteSession2: T3,T4
     *
     *          DivolteSession2: S1 -> T1,T2,T3,T4
     *
     * batch 4: DivolteSession1: T5,T6,T7     DivolteSession2: T5,T6,T7
     *
     *          DivolteSession1: S1#2 -> T5,T6,T7      DivolteSession2: S1#2 -> T5,T6,T7
     *
     *
     * @throws Exception
     */
    @Test
    public void testRewindMultipleUsers() throws Exception {
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
        testRunner.enqueue(createEventsUser1(times));
        testRunner.enqueue(createEventsUser2(times));
        testRunner.enqueue(createEventsUser3(times));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);
        testRunner.assertOutputRecordsCount(6 + 3 * 7);
        injectOutputIntoEsThenSleepThenRefresh(testRunner.getOutputRecords());
//        events (with 3 different users therefore divolteId:   T1--T2--T3--T4--T5--T6---T7
//        changement de traffic:                                ----------------X----------

        getWebSessionCheckerForSessionFromRecords(SESSION1, testRunner.getOutputRecords())
                .sessionId(SESSION1)
                .Userid(USER1)
                .record_type("consolidate-session")
                .record_id(SESSION1)
                .firstEventDateTime(time1)
                .h2kTimestamp(time1)
                .firstVisitedPage(URL)
                .eventsCounter(4)
                .lastEventDateTime(time4)
                .lastVisitedPage(URL)
                .sessionDuration(null)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);
        getWebSessionCheckerForSessionFromRecords(SESSION1+ "#2", testRunner.getOutputRecords())
                .sessionId(SESSION1+ "#2")
                .Userid(USER1)
                .record_type("consolidate-session")
                .record_id(SESSION1+ "#2")
                .firstEventDateTime(time5)
                .h2kTimestamp(time5)
                .firstVisitedPage(URL)
                .eventsCounter(3)
                .lastEventDateTime(time7)
                .lastVisitedPage(URL)
                .sessionDuration(null)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);

        getWebSessionCheckerForSessionFromRecords(SESSION3, testRunner.getOutputRecords())
                .sessionId(SESSION3)
                .Userid(USER3)
                .record_type("consolidate-session")
                .record_id(SESSION3)
                .firstEventDateTime(time1)
                .h2kTimestamp(time1)
                .firstVisitedPage(URL)
                .eventsCounter(4)
                .lastEventDateTime(time4)
                .lastVisitedPage(URL)
                .sessionDuration(null)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);
        getWebSessionCheckerForSessionFromRecords(SESSION3+ "#2", testRunner.getOutputRecords())
                .sessionId(SESSION3+ "#2")
                .Userid(USER3)
                .record_type("consolidate-session")
                .record_id(SESSION3+ "#2")
                .firstEventDateTime(time5)
                .h2kTimestamp(time5)
                .firstVisitedPage(URL)
                .eventsCounter(3)
                .lastEventDateTime(time7)
                .lastVisitedPage(URL)
                .sessionDuration(null)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);


        SearchResponse webSessionRsp = getAllSessionsAfterRefreshing(esclient);
        assertEquals(6, webSessionRsp.getHits().getTotalHits().value);
        SearchHit[] webSessionsDocs = webSessionRsp.getHits().getHits();
        SearchHit sessionUser1 = Arrays.stream(webSessionsDocs)
                .filter(hit -> {
                    return hit.getId().equals(SESSION1);
                })
                .findFirst()
                .get();
        SearchHit session2User1 = Arrays.stream(webSessionsDocs)
                .filter(hit -> {
                    return hit.getId().equals(SESSION1 + "#2");
                })
                .findFirst()
                .get();
        SearchHit sessionUser2 = Arrays.stream(webSessionsDocs)
                .filter(hit -> {
                    return hit.getId().equals(SESSION2);
                })
                .findFirst()
                .get();
        SearchHit session2User2 = Arrays.stream(webSessionsDocs)
                .filter(hit -> {
                    return hit.getId().equals(SESSION2 + "#2");
                })
                .findFirst()
                .get();

        SearchResponse webEventRsp = getAllEventsAfterRefreshing(esclient);
        assertEquals(3 * 7, webEventRsp.getHits().getTotalHits().value);
        Assert.assertEquals(0, this.proc.getNumberOfRewindForProcInstance());

        //rewind only sessions from user 1
        times = Arrays.asList(time3, time4);
        testRunner.clearQueues();
        resetCache(testRunner);
        testRunner.enqueue(createEventsUser1(times));
        testRunner.run();
        testRunner.assertOutputErrorCount(0);
        testRunner.assertOutputRecordsCount(1 + 2 + 2);//session + input events + events from es
        injectOutputIntoEsWithoutRefreshing(testRunner.getOutputRecords());
        //        events (with 3 different users therefore divolteId:   T1--T2--T3--T4--T5--T6---T7
        //        changement de traffic:                                ----------------X----------
        //user 1 :  T3 et T4
        if (jobDoDeleteSessions) {
            assertEquals(5, getAllSessionsAfterRefreshing(esclient).getHits().getTotalHits().value);
        }

        assertEquals(3 * 7, getAllEventsAfterRefreshing(esclient).getHits().getTotalHits().value);

        Assert.assertEquals(1, this.proc.getNumberOfRewindForProcInstance());
        //rewind only sessions from user 2
        times = Arrays.asList(time3, time4);
        testRunner.clearQueues();
        testRunner.enqueue(createEventsUser2(times));
        testRunner.run();
        testRunner.assertOutputErrorCount(0);
        testRunner.assertOutputRecordsCount(1 + 2 + 2);//session + input events + events from es
        injectOutputIntoEsWithoutRefreshing(testRunner.getOutputRecords());
        //        events (with 3 different users therefore divolteId:   T1--T2--T3--T4--T5--T6---T7
        //        changement de traffic:                                ----------------X----------
        //user 2 :  T3 et T4
        if (jobDoDeleteSessions) {
            assertEquals(4, getAllSessionsAfterRefreshing(esclient).getHits().getTotalHits().value);
        }
        assertEquals(3 * 7, getAllEventsAfterRefreshing(esclient).getHits().getTotalHits().value);
        Assert.assertEquals(2, this.proc.getNumberOfRewindForProcInstance());

        //rewind from time5 to time7
        times = Arrays.asList(time5, time6, time7);
        testRunner.clearQueues();
        testRunner.enqueue(createEventsUser1(times));
        testRunner.enqueue(createEventsUser2(times));
        testRunner.run();
        testRunner.assertOutputErrorCount(0);
        injectOutputIntoEsWithoutRefreshing(testRunner.getOutputRecords());

        SearchResponse webSessionRsp2 = getAllSessionsAfterRefreshing(esclient);
        assertEquals(6, webSessionRsp2.getHits().getTotalHits().value);
        SearchHit[] webSessionsDocs2 = webSessionRsp2.getHits().getHits();
        SearchHit session2User1_1 = Arrays.stream(webSessionsDocs2)
                .filter(hit -> {
                    return hit.getId().equals(SESSION1);
                })
                .findFirst()
                .get();
        SearchHit session2User1_2 = Arrays.stream(webSessionsDocs2)
                .filter(hit -> {
                    return hit.getId().equals(SESSION1 + "#2");
                })
                .findFirst()
                .get();
        SearchHit session2User2_1 = Arrays.stream(webSessionsDocs2)
                .filter(hit -> {
                    return hit.getId().equals(SESSION2);
                })
                .findFirst()
                .get();
        SearchHit session2User2_2 = Arrays.stream(webSessionsDocs2)
                .filter(hit -> {
                    return hit.getId().equals(SESSION2 + "#2");
                })
                .findFirst()
                .get();

        SearchResponse webEventRsp2 = getAllEventsAfterRefreshing(esclient);
        assertEquals(3 * 7, webEventRsp2.getHits().getTotalHits().value);

        assertMapsAreEqualsIgnoringSomeKeys(sessionUser1.getSourceAsMap(), session2User1_1.getSourceAsMap() , FieldDictionary.RECORD_TIME, "@timestamp");
        assertMapsAreEqualsIgnoringSomeKeys(session2User1.getSourceAsMap(), session2User1_2.getSourceAsMap(), FieldDictionary.RECORD_TIME, "@timestamp");
        assertMapsAreEqualsIgnoringSomeKeys(sessionUser2.getSourceAsMap(), session2User2_1.getSourceAsMap() , FieldDictionary.RECORD_TIME, "@timestamp");
        assertMapsAreEqualsIgnoringSomeKeys(session2User2.getSourceAsMap(), session2User2_2.getSourceAsMap(), FieldDictionary.RECORD_TIME, "@timestamp");
        Assert.assertEquals(2, this.proc.getNumberOfRewindForProcInstance());
    }

    /**
     * The purpose of this test is to ensure that events and session stored es are the same.
     * That they are injected to ES directly as input of the processor
     * Or that they have been fetched from remote in ES.
     *
     *        events :                   T1--T2--T3--T4--T5--T6---T7
     *        changement de traffic:     ----------------X----------
     * @throws Exception
     */
    @Test
    public void testConsistenceInEs()
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
        testRunner.assertOutputRecordsCount(2 + 7);
        injectOutputIntoEsThenSleepThenRefresh(testRunner.getOutputRecords());

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

        //rewind from time3 to time4
        times = Arrays.asList(time3, time4);
        testRunner.clearQueues();
        resetCache(testRunner);
        testRunner.enqueue(createEvents(url, divoltSession, user, times));
        testRunner.run();
        testRunner.assertOutputErrorCount(0);
        testRunner.assertOutputRecordsCount(1 + 2 + 2);//session + input events + events from es
        injectOutputIntoEsWithoutRefreshing(testRunner.getOutputRecords());
        //        events :                   T1--T2--T3--T4--T5--T6---T7
        //        changement de traffic:     ----------------X----------
        //T3 et T4

        if (jobDoDeleteSessions) {
            assertEquals(1, getAllSessionsAfterRefreshing(esclient).getHits().getTotalHits().value);
        }

        assertEquals(7, getAllEventsAfterRefreshing(esclient).getHits().getTotalHits().value);

        //from time5 to time7
        times = Arrays.asList(time5, time6, time7);
        testRunner.clearQueues();
        testRunner.enqueue(createEvents(url, divoltSession, user, times));
        testRunner.run();
        testRunner.assertOutputErrorCount(0);
        testRunner.assertOutputRecordsCount(2 + 3);//session + input events + events from es
        injectOutputIntoEsWithoutRefreshing(testRunner.getOutputRecords());
        //        events :                   T1--T2--T3--T4--T5--T6---T7
        //        changement de traffic:     ----------------X----------
        //T5,T6 et T7

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
        assertMapsAreEqualsIgnoringSomeKeys(event1.getSourceAsMap(), event2_1.getSourceAsMap(), FieldDictionary.RECORD_TIME, "@timestamp");
        assertMapsAreEqualsIgnoringSomeKeys(event2.getSourceAsMap(), event2_2.getSourceAsMap(), FieldDictionary.RECORD_TIME, "@timestamp");
        assertMapsAreEqualsIgnoringSomeKeys(event3.getSourceAsMap(), event2_3.getSourceAsMap(), FieldDictionary.RECORD_TIME, "@timestamp");
        assertMapsAreEqualsIgnoringSomeKeys(event4.getSourceAsMap(), event2_4.getSourceAsMap(), FieldDictionary.RECORD_TIME, "@timestamp");
        assertMapsAreEqualsIgnoringSomeKeys(event5.getSourceAsMap(), event2_5.getSourceAsMap(), FieldDictionary.RECORD_TIME, "@timestamp");
        assertMapsAreEqualsIgnoringSomeKeys(event6.getSourceAsMap(), event2_6.getSourceAsMap(), FieldDictionary.RECORD_TIME, "@timestamp");
        assertMapsAreEqualsIgnoringSomeKeys(event7.getSourceAsMap(), event2_7.getSourceAsMap(), FieldDictionary.RECORD_TIME, "@timestamp");

        assertEquals(-1, session.getVersion());
        assertEquals(-1, session2.getVersion());
        assertEquals(-1, session2_1.getVersion());
        assertEquals(-1, session2_2.getVersion());
        assertMapsAreEqualsIgnoringSomeKeys(session.getSourceAsMap(), session2_1.getSourceAsMap() , FieldDictionary.RECORD_TIME, "@timestamp");
        assertMapsAreEqualsIgnoringSomeKeys(session2.getSourceAsMap(), session2_2.getSourceAsMap(), FieldDictionary.RECORD_TIME, "@timestamp");
        Assert.assertEquals(1, this.proc.getNumberOfRewindForProcInstance());
    }


    /**
     * The purpose of this test is to ensure that it handles correctly sessions even if it receive events in the wrong order
     * and with some event causing a change of session (traffic source for exemple) So that first runs output wrong session
     * but at the end session should be okay.
     *
     * events:                  T1------T2------T3---T4-------------T5
     * changement de traffic:   --------X-----------------------------
     *
     * First batch: T1,T3,T4
     *              S1 -> T1,T3,T4
     * Second batch: T2,T5
     *               S1 => T1
     *               S1#2 => T2,T3,T4,T5
     *
     * @throws Exception
     */
    @Test
    public void testNotOrderedIncomingEvents() throws Exception {
        final TestRunner testRunner = newTestRunner(container);
        testRunner.assertValid();
        //first run
        final long time1 = 1601629314416L;
        final long time2 = time1 + 1000L;//1601629315416L
        final long time3 = time2 + 1000L;//1601629316416L
        final long time4 = time3 + 1000L;//1601629317416L
        final long time5 = time4 + 1000L;//1601629318416L
        Record event1 = createEvent("url1", SESSION1, USER1, time1);
        Record event2 = createEvent("url2", SESSION1, USER1, time2);
        event2.setStringField(TestMappings.eventsInternalFields.getSourceOffTrafficSourceField(), "not direct");
        event2.setStringField(TestMappings.eventsInternalFields.getSourceOffTrafficMediumField(), "medium 1");
        Record event3 = createEvent("url3", SESSION1, USER1, time3);
        Record event4 = createEvent("url4", SESSION1, USER1, time4);
        Record event5 = createEvent("url5", SESSION1, USER1, time5);
        testRunner.enqueue(event4, event3, event1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);
        testRunner.assertOutputRecordsCount(1 + 3);
        Assert.assertEquals(0, this.proc.getNumberOfRewindForProcInstance());

        injectOutputIntoEsThenSleepThenRefresh(testRunner.getOutputRecords());
        getWebSessionCheckerForSessionFromRecords(SESSION1, testRunner.getOutputRecords())
                .sessionId(SESSION1)
                .Userid(USER1)
                .record_type("consolidate-session")
                .record_id(SESSION1)
                .firstEventDateTime(time1)
                .firstEventEpochSeconds(time1 / 1000)
                .h2kTimestamp(time1)
                .firstVisitedPage("url1")
                .eventsCounter(3)
                .lastEventDateTime(time4)
                .lastEventEpochSeconds(time4 / 1000)
                .lastVisitedPage("url4")
                .sessionDuration(3L)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);

        //events from past arriving (distributed environment does not guaranty order)
        testRunner.clearQueues();
        testRunner.enqueue(event5, event2);
        testRunner.run();
        testRunner.assertOutputErrorCount(0);
        testRunner.assertOutputRecordsCount(2 + 2 + 3);//2 session + 2 events + 3 events from past
        Assert.assertEquals(1, this.proc.getNumberOfRewindForProcInstance());

        getWebSessionCheckerForSessionFromRecords(SESSION1, testRunner.getOutputRecords())
                .sessionId(SESSION1)
                .Userid(USER1)
                .record_type("consolidate-session")
                .record_id(SESSION1)
                .firstEventDateTime(time1)
                .h2kTimestamp(time1)
                .firstVisitedPage("url1")
                .eventsCounter(1)
                .lastEventDateTime(time1)
                .lastVisitedPage("url1")
                .sessionDuration(null)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);
        getWebSessionCheckerForSessionFromRecords(SESSION1+ "#2", testRunner.getOutputRecords())
                .sessionId(SESSION1+ "#2")
                .Userid(USER1)
                .record_type("consolidate-session")
                .record_id(SESSION1+ "#2")
                .firstEventDateTime(time2)
                .h2kTimestamp(time2)
                .firstVisitedPage("url2")
                .eventsCounter(4)
                .lastEventDateTime(time5)
                .lastVisitedPage("url5")
                .sessionDuration(3L)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);
    }

    /**
     * The purpose of this test is to ensure that it handles correctly sessions even if it receive events in the wrong order
     * and with some event causing a change of session (traffic source for exemple) So that first runs output wrong session
     * but at the end session should be okay.
     *
     * Tous les évènements si dessous sont des évènements qui ont a peu près le même timestamp.
     * events (chronological order):      T1------T2------T3---T4-------------T5---T6-------T7
     * changement de traffic:             --------X------------X-------------------X----------
     *
     * First batch: T1,T3,T4
     *      expected in ES: 2 session
     *        S1 -> T1,T3
     *        S1#2 -> T4
     * Second batch: T5,T7
     *      expected in ES: 2 sessions and fixed session so that
     *        S1 => T1,T3
     *        S1#2 => T4, T5, T7
     * Third batch: T2,T6
     *      expected in ES: expected 4 sessions and fixed session so that
     *      S1 => T1
     *		S1#2 => T2, T3
     *      S1#3 => T4, T5
     *      S1#4 => T6, T7
     * @throws Exception
     */
    @Test
    public void testNotOrderedIncomingEvents2() throws Exception {
        final TestRunner testRunner = newTestRunner(container);
        testRunner.assertValid();
        //first run
        final long time1 = 1601629314416L;
        final long time2 = time1 + 1000L;
        final long time3 = time2 + 1000L;
        final long time4 = time3 + 1000L;
        final long time5 = time4 + 1000L;
        final long time6 = time5 + 1000L;
        final long time7 = time6 + 1000L;
        Record event1 = createEvent("url1", SESSION1, USER1, time1);
        Record event2 = createEvent("url2", SESSION1, USER1, time2);
        event2.setStringField(TestMappings.eventsInternalFields.getSourceOffTrafficSourceField(), "not direct");
        event2.setStringField(TestMappings.eventsInternalFields.getSourceOffTrafficMediumField(), "medium 1");
        Record event3 = createEvent("url3", SESSION1, USER1, time3);
        Record event4 = createEvent("url4", SESSION1, USER1, time4);
        event4.setStringField(TestMappings.eventsInternalFields.getSourceOffTrafficSourceField(), "not direct");
        event4.setStringField(TestMappings.eventsInternalFields.getSourceOffTrafficMediumField(), "medium 2");
        Record event5 = createEvent("url5", SESSION1, USER1, time5);
        Record event6 = createEvent("url6", SESSION1, USER1, time6);
        event6.setStringField(TestMappings.eventsInternalFields.getSourceOffTrafficSourceField(), "not direct");
        event6.setStringField(TestMappings.eventsInternalFields.getSourceOffTrafficMediumField(), "medium 3");
        Record event7 = createEvent("url7", SESSION1, USER1, time7);
        testRunner.enqueue(event4, event3, event1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);
        testRunner.assertOutputRecordsCount(2 + 3);//2session + 3 event

        injectOutputIntoEsThenSleepThenRefresh(testRunner.getOutputRecords());
        getWebSessionCheckerForSessionFromRecords(SESSION1, testRunner.getOutputRecords())
                .sessionId(SESSION1)
                .Userid(USER1)
                .record_type("consolidate-session")
                .record_id(SESSION1)
                .firstEventDateTime(time1)
                .h2kTimestamp(time1)
                .firstVisitedPage("url1")
                .eventsCounter(2)
                .lastEventDateTime(time3)
                .lastVisitedPage("url3")
                .sessionDuration(2L)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);
        getWebSessionCheckerForSessionFromRecords(SESSION1 + "#2", testRunner.getOutputRecords())
                .sessionId(SESSION1 + "#2")
                .Userid(USER1)
                .record_type("consolidate-session")
                .record_id(SESSION1 + "#2")
                .firstEventDateTime(time4)
                .h2kTimestamp(time4)
                .firstVisitedPage("url4")
                .eventsCounter(1)
                .lastEventDateTime(time4)
                .lastVisitedPage("url4")
                .sessionDuration(null)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);

        //events from past arriving (distributed environment does not guaranty order)
        testRunner.clearQueues();
        testRunner.enqueue(event5, event7);
        testRunner.run();
        testRunner.assertOutputErrorCount(0);
        testRunner.assertOutputRecordsCount(1 + 2);//1 session + 2 events
        injectOutputIntoEsWithoutRefreshing(testRunner.getOutputRecords());

        getWebSessionCheckerForSessionFromRecords(SESSION1+ "#2", testRunner.getOutputRecords())
                .sessionId(SESSION1+ "#2")
                .Userid(USER1)
                .record_type("consolidate-session")
                .record_id(SESSION1+ "#2")
                .firstEventDateTime(time4)
                .h2kTimestamp(time4)
                .firstVisitedPage("url4")
                .eventsCounter(3)
                .lastEventDateTime(time7)
                .lastVisitedPage("url7")
                .sessionDuration(3L)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);

        //events from past arriving (distributed environment does not guaranty order)
        testRunner.clearQueues();
        testRunner.enqueue(event2, event6);
        testRunner.run();
        testRunner.assertOutputErrorCount(0);//TODO random failure here...
        testRunner.assertOutputRecordsCount(5 + 4);//7 events et 4 sessions

        getFirstRecordWithId(buildId(time1, SESSION1), testRunner.getOutputRecords())
                .assertFieldEquals(TestMappings.eventsInternalFields.getSessionIdField(), SESSION1);

        getFirstRecordWithId(buildId(time2, SESSION1), testRunner.getOutputRecords())
                .assertFieldEquals(TestMappings.eventsInternalFields.getSessionIdField(), SESSION1 + "#2")
                .assertFieldEquals(TestMappings.eventsInternalFields.getOriginalSessionIdField(), SESSION1);

        getFirstRecordWithId(buildId(time3, SESSION1), testRunner.getOutputRecords())
                .assertFieldEquals(TestMappings.eventsInternalFields.getSessionIdField(), SESSION1+ "#2")
                .assertFieldEquals(TestMappings.eventsInternalFields.getOriginalSessionIdField(), SESSION1);

        getFirstRecordWithId(buildId(time4, SESSION1), testRunner.getOutputRecords())
                .assertFieldEquals(TestMappings.eventsInternalFields.getSessionIdField(), SESSION1+ "#3")
                .assertFieldEquals(TestMappings.eventsInternalFields.getOriginalSessionIdField(), SESSION1);

        getFirstRecordWithId(buildId(time5, SESSION1), testRunner.getOutputRecords())
                .assertFieldEquals(TestMappings.eventsInternalFields.getSessionIdField(), SESSION1+ "#3")
                .assertFieldEquals(TestMappings.eventsInternalFields.getOriginalSessionIdField(), SESSION1);

        getFirstRecordWithId(buildId(time6, SESSION1), testRunner.getOutputRecords())
                .assertFieldEquals(TestMappings.eventsInternalFields.getSessionIdField(), SESSION1+ "#4")
                .assertFieldEquals(TestMappings.eventsInternalFields.getOriginalSessionIdField(), SESSION1);

        getFirstRecordWithId(buildId(time7, SESSION1), testRunner.getOutputRecords())
                .assertFieldEquals(TestMappings.eventsInternalFields.getSessionIdField(), SESSION1+ "#4")
                .assertFieldEquals(TestMappings.eventsInternalFields.getOriginalSessionIdField(), SESSION1);

        getWebSessionCheckerForSessionFromRecords(SESSION1, testRunner.getOutputRecords())
                .sessionId(SESSION1)
                .Userid(USER1)
                .record_type("consolidate-session")
                .record_id(SESSION1)
                .firstEventDateTime(time1)
                .h2kTimestamp(time1)
                .firstVisitedPage("url1")
                .eventsCounter(1)
                .lastEventDateTime(time1)
                .lastVisitedPage("url1")
                .sessionDuration(null)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);

        getWebSessionCheckerForSessionFromRecords(SESSION1+ "#2", testRunner.getOutputRecords())
                .sessionId(SESSION1+ "#2")
                .Userid(USER1)
                .record_type("consolidate-session")
                .record_id(SESSION1+ "#2")
                .firstEventDateTime(time2)
                .h2kTimestamp(time2)
                .firstVisitedPage("url2")
                .eventsCounter(2)
                .lastEventDateTime(time3)
                .lastVisitedPage("url3")
                .sessionDuration(1L)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);

        getWebSessionCheckerForSessionFromRecords(SESSION1+ "#3", testRunner.getOutputRecords())
                .sessionId(SESSION1+ "#3")
                .Userid(USER1)
                .record_type("consolidate-session")
                .record_id(SESSION1+ "#3")
                .firstEventDateTime(time4)
                .h2kTimestamp(time4)
                .firstVisitedPage("url4")
                .eventsCounter(2)
                .lastEventDateTime(time5)
                .lastVisitedPage("url5")
                .sessionDuration(1L)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);

        getWebSessionCheckerForSessionFromRecords(SESSION1+ "#4", testRunner.getOutputRecords())
                .sessionId(SESSION1+ "#4")
                .Userid(USER1)
                .record_type("consolidate-session")
                .record_id(SESSION1+ "#4")
                .firstEventDateTime(time6)
                .h2kTimestamp(time6)
                .firstVisitedPage("url6")
                .eventsCounter(2)
                .lastEventDateTime(time7)
                .lastVisitedPage("url7")
                .sessionDuration(1L)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);
    }

    /**
     * The purpose of this test is to ensure that it handles correctly sessions even if it receive events in the wrong order
     * and with some event causing a change of session (traffic source for exemple). Here we first run sessionazation for all events at once.
     * The  we simulate a rewind and simulate one batch with one record at a time. The output should be the same nonetheless !
     *
     * Tous les évènements si dessous sont des évènements qui ont a peu près le même timestamp.
     * events (chronological order):      T1------T2------T3---T4-------------T5---T6-------T7
     * changement de traffic:             --------X------------X-------------------X----------
     * batch 1: T4
     *        S1 -> T4
     *
     * batch 2: T3
     *        S1 => T3
     *        S1#2 => T4
     *
     * batch 3: T1
     *        S1 => T1,T3
     *
     * batch 4: T2
     *        S1 => T1
     *        S1#2 => T2, T3
     *
     *        Here there is an error as T4 is not considered anymore in  S1#2
     *
     * batch 5: T5
     *        S1#2 => T2, T3, T5
     *
     *        T4 is forgotten here ! Becaus of batch 4 which did not get enough events in futur
     *        (see test testNotOrderedIncomingEventsOneByOneWithConfSessionPlus2 for a solution with configuration)
     *
     * batch 6: T6
     *        S1#2 => T2, T3, T5
     *        S1#3 => T6
     *
     * batch 7: T7
     *        S1#3 => T6, T7
     *
     * At the end we get :
     *
     * S1 => T1
     * S1#2 => T2, T3, T5 , T4 with eventcounter = 3
     * S1#3 => T6, T7
     *
     * instead of in reality
     *
     * S1 => T1
     * S1#2 => T2, T3
     * S1#3 => T4, T5
     * S1#4 => T6, T7
     *
     * @throws Exception
     */
    @Test
    public void testNotOrderedIncomingEventsOneByOneWithConfSessionPlus1() throws Exception {
        final TestRunner testRunner = newTestRunner(container);
        testRunner.assertValid();
        //first run
        final long time1 = 1601629314416L;
        final long time2 = time1 + 1000L;
        final long time3 = time2 + 1000L;
        final long time4 = time3 + 1000L;
        final long time5 = time4 + 1000L;
        final long time6 = time5 + 1000L;
        final long time7 = time6 + 1000L;
        Record event1 = createEvent("url1", SESSION1, USER1, time1);
        Record event2TrafficSource = createEvent("url2", SESSION1, USER1, time2);
        event2TrafficSource.setStringField(TestMappings.eventsInternalFields.getSourceOffTrafficSourceField(), "not direct");
        event2TrafficSource.setStringField(TestMappings.eventsInternalFields.getSourceOffTrafficMediumField(), "medium 1");
        Record event3 = createEvent("url3", SESSION1, USER1, time3);
        Record event4TrafficSource = createEvent("url4", SESSION1, USER1, time4);
        event4TrafficSource.setStringField(TestMappings.eventsInternalFields.getSourceOffTrafficSourceField(), "not direct");
        event4TrafficSource.setStringField(TestMappings.eventsInternalFields.getSourceOffTrafficMediumField(), "medium 2");
        Record event5 = createEvent("url5", SESSION1, USER1, time5);
        Record event6TrafficSource = createEvent("url6", SESSION1, USER1, time6);
        event6TrafficSource.setStringField(TestMappings.eventsInternalFields.getSourceOffTrafficSourceField(), "not direct");
        event6TrafficSource.setStringField(TestMappings.eventsInternalFields.getSourceOffTrafficMediumField(), "medium 3");
        Record event7 = createEvent("url7", SESSION1, USER1, time7);

        testRunner.clearQueues();
        testRunner.enqueue(event4TrafficSource);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);
        testRunner.assertOutputRecordsCount(1 + 1);//1 session + 1 event
        injectOutputIntoEsThenSleepThenRefresh(testRunner.getOutputRecords());// event
        //T1------T2------T3---T4-------------T5---T6-------T7
        //--------X------------X-------------------X----------
        //T4
        getWebSessionCheckerForSessionFromRecords(SESSION1, testRunner.getOutputRecords())
                .sessionId(SESSION1)
                .Userid(USER1)
                .record_type("consolidate-session")
                .record_id(SESSION1)
                .firstEventDateTime(time4)
                .h2kTimestamp(time4)
                .firstVisitedPage("url4")
                .eventsCounter(1)
                .lastEventDateTime(time4)
                .lastVisitedPage("url4")
                .sessionDuration(null)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);
        assertEquals(0, this.proc.getNumberOfRewindForProcInstance());
        Thread.sleep(2000L);//wait for session index to be created

        testRunner.clearQueues();
        testRunner.enqueue(event3);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);
        testRunner.assertOutputRecordsCount(2 + 2);//2 session + 2 event
        injectOutputIntoEsWithoutRefreshing(testRunner.getOutputRecords());
        //T1------T2------T3---T4-------------T5---T6-------T7
        //--------X------------X-------------------X----------
        //T4 | T3
        getWebSessionCheckerForSessionFromRecords(SESSION1, testRunner.getOutputRecords())
                .sessionId(SESSION1)
                .Userid(USER1)
                .record_type("consolidate-session")
                .record_id(SESSION1)
                .firstEventDateTime(time3)
                .h2kTimestamp(time3)
                .firstVisitedPage("url3")
                .eventsCounter(1)
                .lastEventDateTime(time3)
                .lastVisitedPage("url3")
                .sessionDuration(null)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);
        getWebSessionCheckerForSessionFromRecords(SESSION1 + "#2", testRunner.getOutputRecords())
                .sessionId(SESSION1 + "#2")
                .Userid(USER1)
                .record_type("consolidate-session")
                .record_id(SESSION1 + "#2")
                .firstEventDateTime(time4)
                .h2kTimestamp(time4)
                .firstVisitedPage("url4")
                .eventsCounter(1)
                .lastEventDateTime(time4)
                .lastVisitedPage("url4")
                .sessionDuration(null)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);
        assertEquals(1, this.proc.getNumberOfRewindForProcInstance());

        testRunner.clearQueues();
        testRunner.enqueue(event1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);
        testRunner.assertOutputRecordsCount(2 + 2);//session + event
        injectOutputIntoEsWithoutRefreshing(testRunner.getOutputRecords());
        //T1------T2------T3---T4-------------T5---T6-------T7
        //--------X------------X-------------------X----------
        //T4 | T3 | T1
        getWebSessionCheckerForSessionFromRecords(SESSION1, testRunner.getOutputRecords())
                .sessionId(SESSION1)
                .Userid(USER1)
                .record_type("consolidate-session")
                .record_id(SESSION1)
                .firstEventDateTime(time1)
                .h2kTimestamp(time1)
                .firstVisitedPage("url1")
                .eventsCounter(2)
                .lastEventDateTime(time3)
                .lastVisitedPage("url3")
                .sessionDuration(2L)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);
        assertEquals(2, this.proc.getNumberOfRewindForProcInstance());

        testRunner.clearQueues();
        testRunner.enqueue(event2TrafficSource);

        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);
        testRunner.assertOutputRecordsCount(2 + 3);//session + event
        injectOutputIntoEsWithoutRefreshing(testRunner.getOutputRecords());
        //T1------T2------T3---T4-------------T5---T6-------T7
        //--------X------------X-------------------X----------
        //T4 | T3 | T1 | T2
        getWebSessionCheckerForSessionFromRecords(SESSION1, testRunner.getOutputRecords())
                .sessionId(SESSION1)
                .Userid(USER1)
                .record_type("consolidate-session")
                .record_id(SESSION1)
                .firstEventDateTime(time1)
                .h2kTimestamp(time1)
                .firstVisitedPage("url1")
                .eventsCounter(1)
                .lastEventDateTime(time1)
                .lastVisitedPage("url1")
                .sessionDuration(null)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);
        getWebSessionCheckerForSessionFromRecords(SESSION1 + "#2", testRunner.getOutputRecords())
                .sessionId(SESSION1 + "#2")
                .Userid(USER1)
                .record_type("consolidate-session")
                .record_id(SESSION1 + "#2")
                .firstEventDateTime(time2)
                .h2kTimestamp(time2)
                .firstVisitedPage("url2")
                .eventsCounter(2)
                .lastEventDateTime(time3)
                .lastVisitedPage("url3")
                .sessionDuration(1L)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);
        assertEquals(3, this.proc.getNumberOfRewindForProcInstance());

        testRunner.clearQueues();
        testRunner.enqueue(event5);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);
        testRunner.assertOutputRecordsCount(1 + 1);//session + event
        injectOutputIntoEsWithoutRefreshing(testRunner.getOutputRecords());
        //T1------T2------T3---T4-------------T5---T6-------T7
        //--------X------------X-------------------X----------
        //A partir d'ici une erreur se propage car on a pas "réparé" T4.
        //T4 | T3 | T1 | T2 | T5
        getWebSessionCheckerForSessionFromRecords(SESSION1 + "#2", testRunner.getOutputRecords())
                .sessionId(SESSION1 + "#2")
                .Userid(USER1)
                .record_type("consolidate-session")
                .record_id(SESSION1 + "#2")
                .firstEventDateTime(time2)
                .h2kTimestamp(time2)
                .firstVisitedPage("url2")
                .eventsCounter(3)
                .lastEventDateTime(time5)
                .lastVisitedPage("url5")
                .sessionDuration(3L)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);
        assertEquals(3, this.proc.getNumberOfRewindForProcInstance());

        testRunner.clearQueues();
        testRunner.enqueue(event6TrafficSource);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);
        testRunner.assertOutputRecordsCount(2 + 1);//session + event
        injectOutputIntoEsWithoutRefreshing(testRunner.getOutputRecords());
        //T1------T2------T3---T4-------------T5---T6-------T7
        //--------X------------X-------------------X----------
        //T4 | T3 | T1 | T2 | T5 | T6
        getWebSessionCheckerForSessionFromRecords(SESSION1 + "#2", testRunner.getOutputRecords())
                .sessionId(SESSION1 + "#2")
                .Userid(USER1)
                .record_type("consolidate-session")
                .record_id(SESSION1 + "#2")
                .firstEventDateTime(time2)
                .h2kTimestamp(time2)
                .firstVisitedPage("url2")
                .eventsCounter(3)
                .lastEventDateTime(time5)
                .lastVisitedPage("url5")
                .sessionDuration(3L)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);
        getWebSessionCheckerForSessionFromRecords(SESSION1+ "#3", testRunner.getOutputRecords())
                .sessionId(SESSION1+ "#3")
                .Userid(USER1)
                .record_type("consolidate-session")
                .record_id(SESSION1+ "#3")
                .firstEventDateTime(time6)
                .h2kTimestamp(time6)
                .firstVisitedPage("url6")
                .eventsCounter(1)
                .lastEventDateTime(time6)
                .lastVisitedPage("url6")
                .sessionDuration(null)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);
        assertEquals(3, this.proc.getNumberOfRewindForProcInstance());

        testRunner.clearQueues();
        testRunner.enqueue(event7);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);
        testRunner.assertOutputRecordsCount(1 + 1);//session + event
        injectOutputIntoEsWithoutRefreshing(testRunner.getOutputRecords());
        //T1------T2------T3---T4-------------T5---T6-------T7
        //--------X------------X-------------------X----------
        getWebSessionCheckerForSessionFromRecords(SESSION1+ "#3", testRunner.getOutputRecords())
                .sessionId(SESSION1+ "#3")
                .Userid(USER1)
                .record_type("consolidate-session")
                .record_id(SESSION1+ "#3")
                .firstEventDateTime(time6)
                .h2kTimestamp(time6)
                .firstVisitedPage("url6")
                .eventsCounter(2)
                .lastEventDateTime(time7)
                .lastVisitedPage("url7")
                .sessionDuration(1L)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);
//        assertEquals(6, this.proc.getNumberOfRewindForProcInstance());

//        FINAL RESULT With an error
        List<WebSession> sessionInEs = getAllSessions(this.elasticsearchClientService, esclient);
        getWebSessionCheckerForSession(SESSION1, sessionInEs)
                .sessionId(SESSION1)
                .Userid(USER1)
                .record_type("consolidate-session")
                .record_id(SESSION1)
                .firstEventDateTime(time1)
                .h2kTimestamp(time1)
                .firstVisitedPage("url1")
                .eventsCounter(1)
                .lastEventDateTime(time1)
                .lastVisitedPage("url1")
                .sessionDuration(null)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);

        getWebSessionCheckerForSession(SESSION1 + "#2", sessionInEs)
                .sessionId(SESSION1 + "#2")
                .Userid(USER1)
                .record_type("consolidate-session")
                .record_id(SESSION1 + "#2")
                .firstEventDateTime(time2)
                .h2kTimestamp(time2)
                .firstVisitedPage("url2")
                .eventsCounter(3)
                .lastEventDateTime(time5)
                .lastVisitedPage("url5")
                .sessionDuration(3L)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);

        getWebSessionCheckerForSession(SESSION1 + "#3", sessionInEs)
                .sessionId(SESSION1+ "#3")
                .Userid(USER1)
                .record_type("consolidate-session")
                .record_id(SESSION1+ "#3")
                .firstEventDateTime(time6)
                .h2kTimestamp(time6)
                .firstVisitedPage("url6")
                .eventsCounter(2)
                .lastEventDateTime(time7)
                .lastVisitedPage("url7")
                .sessionDuration(1L)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);


        assertEquals(3, this.proc.getNumberOfRewindForProcInstance());
    }

    /**
     * The purpose of this test is to ensure that it handles correctly sessions even if it receive events in the wrong order
     * and with some event causing a change of session (traffic source for exemple). Here we first run sessionazation for all events at once.
     * The  we simulate a rewind and simulate one batch with one record at a time. The output should be the same nonetheless !
     *
     * Tous les évènements si dessous sont des évènements qui ont a peu près le même timestamp.
     * events (chronological order):      T1------T2------T3---T4-------------T5---T6-------T7
     * changement de traffic:             --------X------------X-------------------X----------
     *
     * Pour ce test on modifie la configuration du processeur pour prendre plus d'évènements du futur afin de corriger
     * l'erreur du test précédent  testNotOrderedIncomingEventsOneByOneWithConfFutureSessionEqual1.
     * EN effet nous ne pouvons pas nous permettre de prendre tous les évènements du futur car dans le cas d'un rewind
     * sur plusieurs mois cela ferais trop d'évènements !
     *
     * batch 1: T4
     *        S1 -> T4
     *
     * batch 2: T3
     *        S1 => T3
     *        S1#2 => T4
     *
     * batch 3: T1
     *        S1 => T1,T3
     *
     * batch 4: T2
     *        S1 => T1
     *        S1#2 => T2, T3
     *        S1#3 => T4
     *
     *        Here event T4 is taken into consideration thanks to the conf we added !
     *
     * batch 5: T5
     *        S1#3 => T4, T5
     *
     * batch 6: T6
     *        S1#3 => T4, T5
     *        S1#4 => T6
     *
     * batch 7: T7
     *        S1#4 => T6, T7
     *
     * The final result is correct !
     *
     * S1 => T1
     * S1#2 => T2, T3
     * S1#3 => T4, T5
     * S1#4 => T6, T7
     *
     * @throws Exception
     */
    @Test
    public void testNotOrderedIncomingEventsOneByOneWithConfFutureSessionEqual2() throws Exception {
        Map<String, String> customConf = new HashMap<>();
        customConf.put(IncrementalWebSession.NUMBER_OF_FUTURE_SESSION.getName(), "2");
        final TestRunner testRunner = newTestRunner(container, customConf);
        testRunner.assertValid();
        //first run
        final long time1 = 1601629314416L;
        final long time2 = time1 + 1000L;
        final long time3 = time2 + 1000L;
        final long time4 = time3 + 1000L;
        final long time5 = time4 + 1000L;
        final long time6 = time5 + 1000L;
        final long time7 = time6 + 1000L;
        Record event1 = createEvent("url1", SESSION1, USER1, time1);
        Record event2TrafficSource = createEvent("url2", SESSION1, USER1, time2);
        event2TrafficSource.setStringField(TestMappings.eventsInternalFields.getSourceOffTrafficSourceField(), "not direct");
        event2TrafficSource.setStringField(TestMappings.eventsInternalFields.getSourceOffTrafficMediumField(), "medium 1");
        Record event3 = createEvent("url3", SESSION1, USER1, time3);
        Record event4TrafficSource = createEvent("url4", SESSION1, USER1, time4);
        event4TrafficSource.setStringField(TestMappings.eventsInternalFields.getSourceOffTrafficSourceField(), "not direct");
        event4TrafficSource.setStringField(TestMappings.eventsInternalFields.getSourceOffTrafficMediumField(), "medium 2");
        Record event5 = createEvent("url5", SESSION1, USER1, time5);
        Record event6TrafficSource = createEvent("url6", SESSION1, USER1, time6);
        event6TrafficSource.setStringField(TestMappings.eventsInternalFields.getSourceOffTrafficSourceField(), "not direct");
        event6TrafficSource.setStringField(TestMappings.eventsInternalFields.getSourceOffTrafficMediumField(), "medium 3");
        Record event7 = createEvent("url7", SESSION1, USER1, time7);

        testRunner.clearQueues();
        testRunner.enqueue(event4TrafficSource);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);
        testRunner.assertOutputRecordsCount(1 + 1);//1 session + 1 event
        injectOutputIntoEsThenSleepThenRefresh(testRunner.getOutputRecords());// event
        //T1------T2------T3---T4-------------T5---T6-------T7
        //--------X------------X-------------------X----------
        //T4
        getWebSessionCheckerForSessionFromRecords(SESSION1, testRunner.getOutputRecords())
                .sessionId(SESSION1)
                .Userid(USER1)
                .record_type("consolidate-session")
                .record_id(SESSION1)
                .firstEventDateTime(time4)
                .h2kTimestamp(time4)
                .firstVisitedPage("url4")
                .eventsCounter(1)
                .lastEventDateTime(time4)
                .lastVisitedPage("url4")
                .sessionDuration(null)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);
        assertEquals(0, this.proc.getNumberOfRewindForProcInstance());
//        Thread.sleep(2000L);//wait for session index to be created

        testRunner.clearQueues();
        testRunner.enqueue(event3);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);
        testRunner.assertOutputRecordsCount(2 + 2);//2 session + 2 event
        injectOutputIntoEsWithoutRefreshing(testRunner.getOutputRecords());
        //T1------T2------T3---T4-------------T5---T6-------T7
        //--------X------------X-------------------X----------
        //T4 | T3
        getWebSessionCheckerForSessionFromRecords(SESSION1, testRunner.getOutputRecords())
                .sessionId(SESSION1)
                .Userid(USER1)
                .record_type("consolidate-session")
                .record_id(SESSION1)
                .firstEventDateTime(time3)
                .h2kTimestamp(time3)
                .firstVisitedPage("url3")
                .eventsCounter(1)
                .lastEventDateTime(time3)
                .lastVisitedPage("url3")
                .sessionDuration(null)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);
        getWebSessionCheckerForSessionFromRecords(SESSION1 + "#2", testRunner.getOutputRecords())
                .sessionId(SESSION1 + "#2")
                .Userid(USER1)
                .record_type("consolidate-session")
                .record_id(SESSION1 + "#2")
                .firstEventDateTime(time4)
                .h2kTimestamp(time4)
                .firstVisitedPage("url4")
                .eventsCounter(1)
                .lastEventDateTime(time4)
                .lastVisitedPage("url4")
                .sessionDuration(null)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);
        assertEquals(1, this.proc.getNumberOfRewindForProcInstance());

        testRunner.clearQueues();
        testRunner.enqueue(event1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);
        testRunner.assertOutputRecordsCount(2 + 2);//session + event
        injectOutputIntoEsWithoutRefreshing(testRunner.getOutputRecords());
        //T1------T2------T3---T4-------------T5---T6-------T7
        //--------X------------X-------------------X----------
        //T4 | T3 | T1
        getWebSessionCheckerForSessionFromRecords(SESSION1, testRunner.getOutputRecords())
                .sessionId(SESSION1)
                .Userid(USER1)
                .record_type("consolidate-session")
                .record_id(SESSION1)
                .firstEventDateTime(time1)
                .h2kTimestamp(time1)
                .firstVisitedPage("url1")
                .eventsCounter(2)
                .lastEventDateTime(time3)
                .lastVisitedPage("url3")
                .sessionDuration(2L)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);
        getWebSessionCheckerForSessionFromRecords(SESSION1 + "#2", testRunner.getOutputRecords())
                .sessionId(SESSION1 + "#2")
                .Userid(USER1)
                .record_type("consolidate-session")
                .record_id(SESSION1 + "#2")
                .firstEventDateTime(time4)
                .h2kTimestamp(time4)
                .firstVisitedPage("url4")
                .eventsCounter(1)
                .lastEventDateTime(time4)
                .lastVisitedPage("url4")
                .sessionDuration(null)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);
        assertEquals(2, this.proc.getNumberOfRewindForProcInstance());

        testRunner.clearQueues();
        testRunner.enqueue(event2TrafficSource);

        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);
        testRunner.assertOutputRecordsCount(3 + 4);//session + event
        injectOutputIntoEsWithoutRefreshing(testRunner.getOutputRecords());
        //T1------T2------T3---T4-------------T5---T6-------T7
        //--------X------------X-------------------X----------
        //T4 | T3 | T1 | T2
        getWebSessionCheckerForSessionFromRecords(SESSION1, testRunner.getOutputRecords())
                .sessionId(SESSION1)
                .Userid(USER1)
                .record_type("consolidate-session")
                .record_id(SESSION1)
                .firstEventDateTime(time1)
                .h2kTimestamp(time1)
                .firstVisitedPage("url1")
                .eventsCounter(1)
                .lastEventDateTime(time1)
                .lastVisitedPage("url1")
                .sessionDuration(null)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);
        getWebSessionCheckerForSessionFromRecords(SESSION1 + "#2", testRunner.getOutputRecords())
                .sessionId(SESSION1 + "#2")
                .Userid(USER1)
                .record_type("consolidate-session")
                .record_id(SESSION1 + "#2")
                .firstEventDateTime(time2)
                .h2kTimestamp(time2)
                .firstVisitedPage("url2")
                .eventsCounter(2)
                .lastEventDateTime(time3)
                .lastVisitedPage("url3")
                .sessionDuration(1L)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);
        getWebSessionCheckerForSessionFromRecords(SESSION1 + "#3", testRunner.getOutputRecords())
                .sessionId(SESSION1 + "#3")
                .Userid(USER1)
                .record_type("consolidate-session")
                .record_id(SESSION1 + "#3")
                .firstEventDateTime(time4)
                .h2kTimestamp(time4)
                .firstVisitedPage("url4")
                .eventsCounter(1)
                .lastEventDateTime(time4)
                .lastVisitedPage("url4")
                .sessionDuration(null)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);
        assertEquals(3, this.proc.getNumberOfRewindForProcInstance());

        testRunner.clearQueues();
        testRunner.enqueue(event5);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);
        testRunner.assertOutputRecordsCount(1 + 1);//session + event
        injectOutputIntoEsWithoutRefreshing(testRunner.getOutputRecords());
        //T1------T2------T3---T4-------------T5---T6-------T7
        //--------X------------X-------------------X----------
        //T4 | T3 | T1 | T2 | T5
        getWebSessionCheckerForSessionFromRecords(SESSION1 + "#3", testRunner.getOutputRecords())
                .sessionId(SESSION1 + "#3")
                .Userid(USER1)
                .record_type("consolidate-session")
                .record_id(SESSION1 + "#3")
                .firstEventDateTime(time4)
                .h2kTimestamp(time4)
                .firstVisitedPage("url4")
                .eventsCounter(2)
                .lastEventDateTime(time5)
                .lastVisitedPage("url5")
                .sessionDuration(1L)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);
        assertEquals(3, this.proc.getNumberOfRewindForProcInstance());

        testRunner.clearQueues();
        testRunner.enqueue(event6TrafficSource);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);
        testRunner.assertOutputRecordsCount(2 + 1);//session + event
        injectOutputIntoEsWithoutRefreshing(testRunner.getOutputRecords());
        //T1------T2------T3---T4-------------T5---T6-------T7
        //--------X------------X-------------------X----------
        //T4 | T3 | T1 | T2 | T5 | T6
        getWebSessionCheckerForSessionFromRecords(SESSION1 + "#3", testRunner.getOutputRecords())
                .sessionId(SESSION1 + "#3")
                .Userid(USER1)
                .record_type("consolidate-session")
                .record_id(SESSION1 + "#3")
                .firstEventDateTime(time4)
                .h2kTimestamp(time4)
                .firstVisitedPage("url4")
                .eventsCounter(2)
                .lastEventDateTime(time5)
                .lastVisitedPage("url5")
                .sessionDuration(1L)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);
        getWebSessionCheckerForSessionFromRecords(SESSION1+ "#4", testRunner.getOutputRecords())
                .sessionId(SESSION1+ "#4")
                .Userid(USER1)
                .record_type("consolidate-session")
                .record_id(SESSION1+ "#4")
                .firstEventDateTime(time6)
                .h2kTimestamp(time6)
                .firstVisitedPage("url6")
                .eventsCounter(1)
                .lastEventDateTime(time6)
                .lastVisitedPage("url6")
                .sessionDuration(null)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);
        assertEquals(3, this.proc.getNumberOfRewindForProcInstance());

        testRunner.clearQueues();
        testRunner.enqueue(event7);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);
        testRunner.assertOutputRecordsCount(1 + 1);//session + event
        injectOutputIntoEsWithoutRefreshing(testRunner.getOutputRecords());
        //T1------T2------T3---T4-------------T5---T6-------T7
        //--------X------------X-------------------X----------
        getWebSessionCheckerForSessionFromRecords(SESSION1+ "#4", testRunner.getOutputRecords())
                .sessionId(SESSION1+ "#4")
                .Userid(USER1)
                .record_type("consolidate-session")
                .record_id(SESSION1+ "#4")
                .firstEventDateTime(time6)
                .h2kTimestamp(time6)
                .firstVisitedPage("url6")
                .eventsCounter(2)
                .lastEventDateTime(time7)
                .lastVisitedPage("url7")
                .sessionDuration(1L)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);
//        assertEquals(6, this.proc.getNumberOfRewindForProcInstance());

//        FINAL RESULT Without error
        List<WebSession> sessionInEs = getAllSessions(this.elasticsearchClientService, esclient);
        getWebSessionCheckerForSession(SESSION1, sessionInEs)
                .sessionId(SESSION1)
                .Userid(USER1)
                .record_type("consolidate-session")
                .record_id(SESSION1)
                .firstEventDateTime(time1)
                .h2kTimestamp(time1)
                .firstVisitedPage("url1")
                .eventsCounter(1)
                .lastEventDateTime(time1)
                .lastVisitedPage("url1")
                .sessionDuration(null)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);

        getWebSessionCheckerForSession(SESSION1 + "#2", sessionInEs)
                .sessionId(SESSION1 + "#2")
                .Userid(USER1)
                .record_type("consolidate-session")
                .record_id(SESSION1 + "#2")
                .firstEventDateTime(time2)
                .h2kTimestamp(time2)
                .firstVisitedPage("url2")
                .eventsCounter(2)
                .lastEventDateTime(time3)
                .lastVisitedPage("url3")
                .sessionDuration(1L)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);

        getWebSessionCheckerForSession(SESSION1 + "#3", sessionInEs)
                .sessionId(SESSION1+ "#3")
                .Userid(USER1)
                .record_type("consolidate-session")
                .record_id(SESSION1+ "#3")
                .firstEventDateTime(time4)
                .h2kTimestamp(time4)
                .firstVisitedPage("url4")
                .eventsCounter(2)
                .lastEventDateTime(time5)
                .lastVisitedPage("url5")
                .sessionDuration(1L)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);

        getWebSessionCheckerForSession(SESSION1 + "#4", sessionInEs)
                .sessionId(SESSION1+ "#4")
                .Userid(USER1)
                .record_type("consolidate-session")
                .record_id(SESSION1+ "#4")
                .firstEventDateTime(time6)
                .h2kTimestamp(time6)
                .firstVisitedPage("url6")
                .eventsCounter(2)
                .lastEventDateTime(time7)
                .lastVisitedPage("url7")
                .sessionDuration(1L)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);


        assertEquals(3, this.proc.getNumberOfRewindForProcInstance());
    }

    /**
     * The purpose of this test is to ensure that it handles correctly sessions even if it receive events in the wrong order
     * and with some event causing a change of session (traffic source for exemple). Here we first run sessionazation for all events at once.
     * The  we simulate a rewind and simulate one batch with one record at a time. The output should be the same nonetheless !
     *
     * Tous les évènements si dessous sont des évènements qui ont a peu près le même timestamp.
     * events (chronological order):      T1--T2--T3--T4--T5--T6--T7
     * changement de traffic:             ----X-------X-------X-----
     * batch 1: events T1,T2,T2,T3,T4,T5,T6,T7
     *      S1 -> T1
     *      S1#2 -> T2,T3
     *      S1#3 -> T4,T5
     *      S1#4 -> T6,T7
     *
     * Simulate rewind reset cache
     *
     * batch 2: T4
     * batch 3: T3
     * batch 4: T1
     * batch 5: T2
     * batch 6: T5
     * batch 7: T6
     * batch 8: T7
     *
     * Then just ensure final result in es is coprrect, which should be :
     *  S1 -> T1
     *  S1#2 -> T2,T3
     *  S1#3 -> T4,T5
     *  S1#4 -> T6,T7
     *
     * @throws Exception
     */
    @Test
    public void testNoRewindInSpecificCases() throws Exception {
        final TestRunner testRunner = newTestRunner(container);
        testRunner.assertValid();
        //first run
        final long time1 = 1601629314416L;
        final long time2 = time1 + 1000L;
        final long time3 = time2 + 1000L;
        final long time4 = time3 + 1000L;
        final long time5 = time4 + 1000L;
        final long time6 = time5 + 1000L;
        final long time7 = time6 + 1000L;
        Record event1 = createEvent("url1", SESSION1, USER1, time1);
        Record event2TrafficSource = createEvent("url2", SESSION1, USER1, time2);
        event2TrafficSource.setStringField(TestMappings.eventsInternalFields.getSourceOffTrafficSourceField(), "not direct");
        event2TrafficSource.setStringField(TestMappings.eventsInternalFields.getSourceOffTrafficMediumField(), "medium 1");
        Record event3 = createEvent("url3", SESSION1, USER1, time3);
        Record event4TrafficSource = createEvent("url4", SESSION1, USER1, time4);
        event4TrafficSource.setStringField(TestMappings.eventsInternalFields.getSourceOffTrafficSourceField(), "not direct");
        event4TrafficSource.setStringField(TestMappings.eventsInternalFields.getSourceOffTrafficMediumField(), "medium 2");
        Record event5 = createEvent("url5", SESSION1, USER1, time5);
        Record event6TrafficSource = createEvent("url6", SESSION1, USER1, time6);
        event6TrafficSource.setStringField(TestMappings.eventsInternalFields.getSourceOffTrafficSourceField(), "not direct");
        event6TrafficSource.setStringField(TestMappings.eventsInternalFields.getSourceOffTrafficMediumField(), "medium 3");
        Record event7 = createEvent("url7", SESSION1, USER1, time7);
        testRunner.enqueue(event4TrafficSource, event3, event1, event2TrafficSource, event5, event6TrafficSource, event7);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);
        testRunner.assertOutputRecordsCount(7 + 4);//4 session + 7 event

        injectOutputIntoEsThenSleepThenRefresh(testRunner.getOutputRecords());
        getWebSessionCheckerForSessionFromRecords(SESSION1, testRunner.getOutputRecords())
                .sessionId(SESSION1)
                .Userid(USER1)
                .record_type("consolidate-session")
                .record_id(SESSION1)
                .firstEventDateTime(time1)
                .h2kTimestamp(time1)
                .firstVisitedPage("url1")
                .eventsCounter(1)
                .lastEventDateTime(time1)
                .lastVisitedPage("url1")
                .sessionDuration(null)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);

        getWebSessionCheckerForSessionFromRecords(SESSION1 + "#2", testRunner.getOutputRecords())
                .sessionId(SESSION1 + "#2")
                .Userid(USER1)
                .record_type("consolidate-session")
                .record_id(SESSION1 + "#2")
                .firstEventDateTime(time2)
                .h2kTimestamp(time2)
                .firstVisitedPage("url2")
                .eventsCounter(2)
                .lastEventDateTime(time3)
                .lastVisitedPage("url3")
                .sessionDuration(1L)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);

        getWebSessionCheckerForSessionFromRecords(SESSION1 + "#3", testRunner.getOutputRecords())
                .sessionId(SESSION1+ "#3")
                .Userid(USER1)
                .record_type("consolidate-session")
                .record_id(SESSION1+ "#3")
                .firstEventDateTime(time4)
                .h2kTimestamp(time4)
                .firstVisitedPage("url4")
                .eventsCounter(2)
                .lastEventDateTime(time5)
                .lastVisitedPage("url5")
                .sessionDuration(1L)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);

        getWebSessionCheckerForSessionFromRecords(SESSION1+ "#4", testRunner.getOutputRecords())
                .sessionId(SESSION1+ "#4")
                .Userid(USER1)
                .record_type("consolidate-session")
                .record_id(SESSION1+ "#4")
                .firstEventDateTime(time6)
                .h2kTimestamp(time6)
                .firstVisitedPage("url6")
                .eventsCounter(2)
                .lastEventDateTime(time7)
                .lastVisitedPage("url7")
                .sessionDuration(1L)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);

        assertEquals(0, this.proc.getNumberOfRewindForProcInstance());
        //RESET CACHE ==> REWIND
        testRunner.clearQueues();
        resetCache(testRunner);
        testRunner.enqueue(event4TrafficSource);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);
        //T1------T2------T3---T4-------------T5---T6-------T7
        //--------X------------X-------------------X----------
        assertEquals(1, this.proc.getNumberOfRewindForProcInstance());

        testRunner.clearQueues();
        testRunner.enqueue(event3);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);
//        //T1------T2------T3---T4-------------T5---T6-------T7
//        //--------X------------X-------------------X----------
        assertEquals(2, this.proc.getNumberOfRewindForProcInstance());

        testRunner.clearQueues();
        testRunner.enqueue(event1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);
//        //T1------T2------T3---T4-------------T5---T6-------T7
//        //--------X------------X-------------------X----------
        assertEquals(3, this.proc.getNumberOfRewindForProcInstance());

        testRunner.clearQueues();
        testRunner.enqueue(event2TrafficSource);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);
//        //T1------T2------T3---T4-------------T5---T6-------T7
//        //--------X------------X-------------------X----------
        assertEquals(3, this.proc.getNumberOfRewindForProcInstance());

        testRunner.clearQueues();
        testRunner.enqueue(event5);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);
//        //T1------T2------T3---T4-------------T5---T6-------T7
//        //--------X------------X-------------------X----------
        assertEquals(3, this.proc.getNumberOfRewindForProcInstance());

        testRunner.clearQueues();
        testRunner.enqueue(event6TrafficSource);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);
//        //T1------T2------T3---T4-------------T5---T6-------T7
//        //--------X------------X-------------------X----------
        assertEquals(3, this.proc.getNumberOfRewindForProcInstance());

        testRunner.clearQueues();
        testRunner.enqueue(event7);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);
//        //T1------T2------T3---T4-------------T5---T6-------T7
//        //--------X------------X-------------------X----------
        assertEquals(3, this.proc.getNumberOfRewindForProcInstance());

        List<WebSession> sessionInEs = getAllSessions(this.elasticsearchClientService, esclient);
        getWebSessionCheckerForSession(SESSION1, sessionInEs)
                .sessionId(SESSION1)
                .Userid(USER1)
                .record_type("consolidate-session")
                .record_id(SESSION1)
                .firstEventDateTime(time1)
                .h2kTimestamp(time1)
                .firstVisitedPage("url1")
                .eventsCounter(1)
                .lastEventDateTime(time1)
                .lastVisitedPage("url1")
                .sessionDuration(null)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);
        getWebSessionCheckerForSession(SESSION1 + "#2", sessionInEs)
                .sessionId(SESSION1 + "#2")
                .Userid(USER1)
                .record_type("consolidate-session")
                .record_id(SESSION1 + "#2")
                .firstEventDateTime(time2)
                .h2kTimestamp(time2)
                .firstVisitedPage("url2")
                .eventsCounter(2)
                .lastEventDateTime(time3)
                .lastVisitedPage("url3")
                .sessionDuration(1L)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);
        getWebSessionCheckerForSession(SESSION1 + "#3", sessionInEs)
                .sessionId(SESSION1 + "#3")
                .Userid(USER1)
                .record_type("consolidate-session")
                .record_id(SESSION1 + "#3")
                .firstEventDateTime(time4)
                .h2kTimestamp(time4)
                .firstVisitedPage("url4")
                .eventsCounter(2)
                .lastEventDateTime(time5)
                .lastVisitedPage("url5")
                .sessionDuration(1L)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);
        getWebSessionCheckerForSession(SESSION1 + "#4", sessionInEs)
                .sessionId(SESSION1+ "#4")
                .Userid(USER1)
                .record_type("consolidate-session")
                .record_id(SESSION1+ "#4")
                .firstEventDateTime(time6)
                .h2kTimestamp(time6)
                .firstVisitedPage("url6")
                .eventsCounter(2)
                .lastEventDateTime(time7)
                .lastVisitedPage("url7")
                .sessionDuration(1L)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);

    }
    /**
     * The purpose of this test is to ensure that intempestive rewind are not triggered in nominal mode.
     * Some events are delayed into next batch but that should not trigger a rewind !
     *
     * Tous les évènements si dessous sont des évènements qui ont a peu près le même timestamp.
     * events (chronological order):      T1------T2------T3---T4-------------T5---T6-------T7--T8
     * changement de traffic:             --------X------------X-------------------X--------------
     *
     * batch 1: events T1,T3,T4
     * batch 2: events T5,T7 => no rewind
     * batch 3: events T2,T6 => 1 rewind
     * batch 4: events T6modifié, T7 => no rewind
     * batch 5: events T7modifié, T8 => no rewind
     * batch 6: events T7modifié => actuellement 1 rewind //TODO garder en cache des events pour eviter un rewind
     * @throws Exception
     */
    @Test
    public void testNotTooManyRewind() throws Exception {
        final TestRunner testRunner = newTestRunner(container);
        testRunner.assertValid();
        //first run
        final long time1 = 1601629314416L;
        final long time2 = time1 + 1000L;
        final long time3 = time2 + 1000L;
        final long time4 = time3 + 1000L;
        final long time5 = time4 + 1000L;
        final long time6 = time5 + 1000L;
        final long time7 = time6 + 1000L;
        final long time8 = time7 + 1000L;
        Record event1 = createEvent("url1", SESSION1, USER1, time1);
        Record event2 = createEvent("url2", SESSION1, USER1, time2);
        event2.setStringField(TestMappings.eventsInternalFields.getSourceOffTrafficSourceField(), "not direct");
        event2.setStringField(TestMappings.eventsInternalFields.getSourceOffTrafficMediumField(), "medium 1");
        Record event3 = createEvent("url3", SESSION1, USER1, time3);
        Record event4 = createEvent("url4", SESSION1, USER1, time4);
        event4.setStringField(TestMappings.eventsInternalFields.getSourceOffTrafficSourceField(), "not direct");
        event4.setStringField(TestMappings.eventsInternalFields.getSourceOffTrafficMediumField(), "medium 2");
        Record event5 = createEvent("url5", SESSION1, USER1, time5);
        Record event6 = createEvent("url6", SESSION1, USER1, time6);
        event6.setStringField(TestMappings.eventsInternalFields.getSourceOffTrafficSourceField(), "not direct");
        event6.setStringField(TestMappings.eventsInternalFields.getSourceOffTrafficMediumField(), "medium 3");
        Record event6Modifie = createEvent("url6Modifie", SESSION1, USER1, time6);
        Record event7 = createEvent("url7", SESSION1, USER1, time7);
        Record event7Modifie = createEvent("url7Modifie", SESSION1, USER1, time7);
        Record event8 = createEvent("url7", SESSION1, USER1, time8);

        Assert.assertEquals(0, this.proc.getNumberOfRewindForProcInstance());
        testRunner.enqueue(event4, event3, event1);//T1,T3,T4
        testRunner.run();
        injectOutputIntoEsThenSleepThenRefresh(testRunner.getOutputRecords());
        Assert.assertEquals(0, this.proc.getNumberOfRewindForProcInstance());

        testRunner.clearQueues();
        testRunner.enqueue(event5, event7);//T5,T7 => no rewind
        testRunner.run();
        injectOutputIntoEsWithoutRefreshing(testRunner.getOutputRecords());
        Assert.assertEquals(0, this.proc.getNumberOfRewindForProcInstance());

        testRunner.clearQueues();
        testRunner.enqueue(event2, event6);//T2,T6 => 1 rewind
        testRunner.run();
        injectOutputIntoEsWithoutRefreshing(testRunner.getOutputRecords());
        Assert.assertEquals(1, this.proc.getNumberOfRewindForProcInstance());

        testRunner.clearQueues();
        testRunner.enqueue(event6Modifie, event7);//T6modifié, T7 => no rewind
        testRunner.run();
        injectOutputIntoEsWithoutRefreshing(testRunner.getOutputRecords());
        Assert.assertEquals(1, this.proc.getNumberOfRewindForProcInstance());

        testRunner.clearQueues();
        testRunner.enqueue(event7Modifie, event8);//T7modifié, T8 => no rewind
        testRunner.run();
        injectOutputIntoEsWithoutRefreshing(testRunner.getOutputRecords());
        Assert.assertEquals(1, this.proc.getNumberOfRewindForProcInstance());

        testRunner.clearQueues();
        testRunner.enqueue(event7Modifie);//T7Modifie => 1 rewind ideallement no rewind
        testRunner.run();
        injectOutputIntoEsWithoutRefreshing(testRunner.getOutputRecords());
        Assert.assertEquals(2, this.proc.getNumberOfRewindForProcInstance());
    }

    private WebSessionChecker getWebSessionCheckerForSession(final String divoltSession,
                                                             final List<WebSession> sessions) {
        WebSession session = sessions.stream().filter(s -> s.getSessionId().equals(divoltSession)).findFirst().get();
        return new WebSessionChecker(session);
    }

    private WebSessionChecker getWebSessionCheckerForSessionFromRecords(final String divoltSession,
                                                             final List<MockRecord> sessions) {
        Record session = sessions.stream().filter(s -> divoltSession.equals(s.getId())).findFirst().get();
        return new WebSessionChecker(session);
    }

    private List<Record> createEvents(String url, String divoltSession, String user, List<Long> times) {
        List<Record> events = new ArrayList<>();
        for (Long time : times) {
            events.add(createEvent(url, divoltSession, user, time));
        }
        return events;
    }

    private Record createEvent(String url, String divoltSession, String user, Long time) {
        String id = buildId(time, divoltSession);
        return new WebEvent(id, divoltSession, user, time, url);
    }

    private List<Record> createEventsUser1(List<Long> times) {
        return createEvents(URL, SESSION1, USER1, times);
    }

    private List<Record> createEventsUser2(List<Long> times) {
        return createEvents(URL, SESSION2, USER2, times);
    }

    private List<Record> createEventsUser3(List<Long> times) {
        return createEvents(URL, SESSION3, USER3, times);
    }

    private String buildId(long time, String divoltSession) {
        return "event-" + time + "-" + divoltSession;
    }

    private MockRecord getFirstRecordWithId(final String id, final List<MockRecord> records) {
        return records.stream().filter(record -> record.getId().equals(id)).findFirst().get();
    }


    private void injectOutputIntoEsThenRefresh(List<MockRecord> output) throws InitializationException {
        injectOutputIntoEsWithoutRefreshing(output);
        String[] indicesToWaitFor = output.stream()
                .map(record -> record.getField(defaultOutputFieldNameForEsIndex).asString())
                .toArray(String[]::new);
        this.elasticsearchClientService.waitUntilCollectionIsReadyAndRefreshIfAnyPendingTasks(indicesToWaitFor, 100000L);
    }

    private void injectOutputIntoEsThenSleepThenRefresh(List<MockRecord> output) throws InitializationException {
        injectOutputIntoEsWithoutRefreshing(output);
        try {
            Thread.sleep(1500L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        String[] indicesToWaitFor = output.stream()
                .map(record -> record.getField(defaultOutputFieldNameForEsIndex).asString())
                .toArray(String[]::new);
        this.elasticsearchClientService.waitUntilCollectionIsReadyAndRefreshIfAnyPendingTasks(indicesToWaitFor, 100000L);
    }

    private void injectOutputIntoEsWithoutRefreshing(List<MockRecord> output) {
        bulkRunner.assertValid();
        bulkRunner.enqueue(output);
        bulkRunner.run();
        bulkRunner.assertOutputRecordsCount(output.size());
        this.elasticsearchClientService.bulkFlush();
        bulkRunner.clearQueues();
    }


    private void resetCache(TestRunner testRunner) {
        testRunner.disableControllerService(lruCache);
        testRunner.enableControllerService(lruCache);
    }

    private SearchResponse getAllSessionsAfterRefreshing(RestHighLevelClient esclient) throws IOException {
        try {
            Thread.sleep(500L);
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

    private SearchResponse getAllEventsAfterRefreshing(RestHighLevelClient esclient) throws IOException {
        try {
            Thread.sleep(500L);
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

    private TestRunner newBulkAddTestRunner(DockerComposeContainer container) throws InitializationException {
        return newBulkAddTestRunner(container, Collections.emptyMap());
    }

    private TestRunner newBulkAddTestRunner(DockerComposeContainer container, Map<String, String> customConf) throws InitializationException {
        BulkAddElasticsearch proc = new BulkAddElasticsearch();
        final TestRunner runner = TestRunners.newTestRunner(proc);
        configureElasticsearchClientService(runner, container);
        runner.setProperty(BulkAddElasticsearch.ELASTICSEARCH_CLIENT_SERVICE, "elasticsearchClient");
        runner.setProperty(BulkAddElasticsearch.ES_INDEX_FIELD, defaultOutputFieldNameForEsIndex);
        runner.setProperty(BulkAddElasticsearch.ES_TYPE_FIELD, IncrementalWebSession.defaultOutputFieldNameForEsType);
        runner.setProperty(BulkAddElasticsearch.DEFAULT_INDEX, "test");
        runner.setProperty(BulkAddElasticsearch.DEFAULT_TYPE, "test");
        customConf.forEach(runner::setProperty);
        return runner;
    }
    /**
     * Creates a new TestRunner set with the appropriate properties.
     *
     * @return a new TestRunner set with the appropriate properties.
     *
     * @throws InitializationException in case the runner could not be instantiated.
     */
    private TestRunner newTestRunner(DockerComposeContainer container) throws InitializationException {
        return newTestRunner(container, Collections.emptyMap());
    }


    private TestRunner newTestRunner(DockerComposeContainer container, Map<String, String> customConf) throws InitializationException {
        this.proc = new IncrementalWebSession();
        final TestRunner runner = TestRunners.newTestRunner(this.proc);
        final String FIELDS_TO_RETURN = Stream.of("partyId", "B2BUnit").collect(Collectors.joining(","));
//        fields.to.return: partyId,Company,remoteHost,tagOrigin,sourceOrigin,spamOrigin,referer,userAgentString,utm_source,utm_campaign,utm_medium,utm_content,utm_term,alert_match_name,alert_match_query,referer_hostname,DeviceClass,AgentName,ImportanceCode,B2BUnit,libelle_zone,Userid,customer_category,source_of_traffic_source,source_of_traffic_medium,source_of_traffic_keyword,source_of_traffic_campaign,source_of_traffic_organic_search,source_of_traffic_content,source_of_traffic_referral_path,websessionIndex
        configureElasticsearchClientService(runner, container);
        configureCacheService(runner);
        runner.setProperty(IncrementalWebSession.CONFIG_CACHE_SERVICE, "lruCache");
        runner.setProperty(IncrementalWebSession.ELASTICSEARCH_CLIENT_SERVICE_CONF, "elasticsearchClient");
        runner.setProperty(IncrementalWebSession.ES_SESSION_INDEX_PREFIX_CONF, SESSION_INDEX_PREFIX);
        runner.setProperty(IncrementalWebSession.ES_SESSION_INDEX_SUFFIX_FORMATTER_CONF, SESSION_SUFFIX_FORMATTER_STRING);
        runner.setProperty(IncrementalWebSession.ES_SESSION_TYPE_NAME_CONF, "sessions");
        runner.setProperty(IncrementalWebSession.ES_EVENT_INDEX_PREFIX_CONF, EVENT_INDEX_PREFIX);
        runner.setProperty(IncrementalWebSession.ES_EVENT_INDEX_SUFFIX_FORMATTER_CONF, EVENT_SUFFIX_FORMATTER_STRING);
        runner.setProperty(IncrementalWebSession.ES_EVENT_TYPE_NAME_CONF, "event");
        runner.setProperty(IncrementalWebSession.SESSION_ID_FIELD_CONF, "sessionId");
        runner.setProperty(IncrementalWebSession.TIMESTAMP_FIELD_CONF, "h2kTimestamp");
        runner.setProperty(IncrementalWebSession.VISITED_PAGE_FIELD, "VISITED_PAGE");
        runner.setProperty(IncrementalWebSession.USER_ID_FIELD, "Userid");
        runner.setProperty(IncrementalWebSession.SESSION_INACTIVITY_TIMEOUT_CONF, String.valueOf(SESSION_TIMEOUT));
        runner.setProperty(IncrementalWebSession.FIELDS_TO_RETURN, FIELDS_TO_RETURN);
        runner.setProperty(IncrementalWebSession.DEBUG_CONF, "true");
        customConf.forEach(runner::setProperty);

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
        ElasticsearchClientService elasticsearchClientService;
        if (this.elasticsearchClientService == null) {
            elasticsearchClientService = new Elasticsearch_7_x_ClientService();
        } else {
            elasticsearchClientService = this.elasticsearchClientService;
        }

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
