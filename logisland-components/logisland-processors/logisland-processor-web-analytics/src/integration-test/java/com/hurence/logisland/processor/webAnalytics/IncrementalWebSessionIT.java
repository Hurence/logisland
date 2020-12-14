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
import com.hurence.logisland.processor.webAnalytics.modele.TestMappings;
import com.hurence.logisland.processor.webAnalytics.modele.WebSession;
import com.hurence.logisland.processor.webAnalytics.util.ElasticsearchServiceUtil;
import com.hurence.logisland.processor.webAnalytics.util.WebEvent;
import com.hurence.logisland.processor.webAnalytics.util.WebSessionChecker;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.service.cache.LRUKeyValueCacheService;
import com.hurence.logisland.service.elasticsearch.ElasticsearchClientService;
import com.hurence.logisland.service.elasticsearch.Elasticsearch_7_x_ClientService;
import com.hurence.logisland.util.runner.MockRecord;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.PutIndexTemplateRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
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

import static com.hurence.logisland.processor.webAnalytics.util.ElasticsearchServiceUtil.*;

/**
 * Test incremental web-session processor.
 */
@ExtendWith({Es7DockerExtension.class})
public class IncrementalWebSessionIT
{
    private static final Logger logger = LoggerFactory.getLogger(IncrementalWebSessionIT.class);

    private static final String SESSIONID = "SESSIONID";
    private static final String SESSION_ID = "sessionId";
    private static final String TIMESTAMP = "h2kTimestamp";
    private static final String VISITED_PAGE = "VISITED_PAGE";
    private static final String CURRENT_CART = "currentCart";
    private static final String USER_ID = "Userid";

    private static final String SESSION1 = "session1";
    private static final String SESSION2 = "session2";

    private static final long SESSION_TIMEOUT_SECONDS = 1800L; // but should be 30*60;

    private static final String URL1 = "http://page1";
    private static final String URL2 = "http://page2";
    private static final String URL3 = "http://page3";

    private static final String USER1 = "user1";
    private static final String USER2 = "user2";

    private static final String PARTY_ID1 = "partyId1";

    private static final Long DAY1 = 1493197966584L;  // Wed Apr 26 11:12:46 CEST 2017
    private static final Long DAY2 = 1493297966584L;  // Thu Apr 27 14:59:26 CEST 2017

    private static final String PARTY_ID = "partyId";
    private static final String B2BUNIT = "B2BUnit";

    private IncrementalWebSession proc;
    private ElasticsearchClientService elasticsearchClientService;

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

    @Test
    public void testCreateOneSessionOneEvent(DockerComposeContainer container)
            throws Exception
    {
        int eventCount = 0;

        TestRunner testRunner = newTestRunner(container);
        testRunner.assertValid();
        testRunner.enqueue(Arrays.asList(new WebEvent(String.valueOf(eventCount++), SESSION1, USER1, DAY1, URL1)));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);

        // One webSession expected.
        testRunner.assertOutputRecordsCount(1);
        final MockRecord session = getFirstRecordWithId(SESSION1, testRunner.getOutputRecords());


        new WebSessionChecker(session).sessionId(SESSION1)
                                  .Userid(USER1)
                                  .record_type("consolidate-session")
                                  .record_id(SESSION1)
                                  .firstEventDateTime(DAY1)
                                  .h2kTimestamp(DAY1)
                                  .firstVisitedPage(URL1)
                                  .eventsCounter(1)
                                  .lastEventDateTime(DAY1)
                                  .lastVisitedPage(URL1)
                                  .sessionDuration(null)
                                  .is_sessionActive(false)
                                  .sessionInactivityDuration(SESSION_TIMEOUT_SECONDS);
    }

    @Test
    public void testCreateOneSessionMultipleEvents(DockerComposeContainer container)
            throws Exception
    {
        
        int eventCount = 0;

        TestRunner testRunner = newTestRunner(container);
        testRunner.assertValid();
        testRunner.enqueue(Arrays.asList(new WebEvent(String.valueOf(eventCount++), SESSION1, null, DAY1, URL1),
                                         new WebEvent(String.valueOf(eventCount++), SESSION1, USER1, DAY1+1000L, URL2),
                                         new WebEvent(String.valueOf(eventCount++), SESSION1, null, DAY1+2000L, URL3)));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);

        // One webSession expected.
        testRunner.assertOutputRecordsCount(1);
        final MockRecord doc = getFirstRecordWithId(SESSION1, testRunner.getOutputRecords());

        new WebSessionChecker(doc).sessionId(SESSION1)
                                  .Userid(USER1)
                                  .record_type("consolidate-session")
                                  .record_id(SESSION1)
                                  .firstEventDateTime(DAY1)
                                  .h2kTimestamp(DAY1)
                                  .firstVisitedPage(URL1)
                                  .eventsCounter(3)
                                  .lastEventDateTime(DAY1+2000L)
                                  .lastVisitedPage(URL3)
                                  .sessionDuration(2L)
                                  .is_sessionActive(false)
                                  .sessionInactivityDuration(SESSION_TIMEOUT_SECONDS);
    }

    @Test
    public void testCreateOneSessionMultipleEventsData2(DockerComposeContainer container)
            throws Exception
    {
        Object[] data2 = new Object[]{
                new Object[]{
                        "h2kTimestamp" , 1538753339113L,
                        "location" , "https://www.zitec-shop.com/en/",
                        "sessionId" , SESSIONID // "0:jmw62hh3:ef5kWpbpKDNBG5IyGBARUQLemW3JP0PP"
                }, new Object[]{
                "h2kTimestamp" , 1538753689109L,
                "location" , "https://www.zitec-shop.com/en/",
                "sessionId" , SESSIONID
        }, new Object[]{
                "h2kTimestamp" , 1538753753964L,
                "location" , "https://www.zitec-shop.com/en/",
                "sessionId" , SESSIONID
        }, new Object[]{
                "h2kTimestamp" , 1538753768489L,
                "location" , "https://www.zitec-shop.com/en/schragkugellager-718-tn/p-G1156005137",
                "sessionId" , SESSIONID
        }, new Object[]{
                "h2kTimestamp" , 1538756201154L, // timeout
                "location" , "https://www.zitec-shop.com/en/schragkugellager-718-tn/p-G1156005137",
                "sessionId" , SESSIONID
        }, new Object[]{
                "h2kTimestamp" , 1538756215043L,
                "location" , "https://www.zitec-shop.com/en/search/?text=rotex%2Cgg%2C48",
                "sessionId" , SESSIONID
        }, new Object[]{
                "h2kTimestamp" , 1538756216242L,
                "location" , "https://www.zitec-shop.com/en/search/?text=rotex%2Cgg%2C48",
                "sessionId" , SESSIONID
        }, new Object[]{
                "h2kTimestamp" , 1538756232483L,
                "location" , "https://www.zitec-shop.com/en/rotex-48-gg/p-G1184000392",
                "sessionId" , SESSIONID
        }, new Object[]{
                "h2kTimestamp" , 1538756400671L,
                "location" , "https://www.zitec-shop.com/en/rotex-48-gg/p-G1184000392",
                "sessionId" , SESSIONID
        }, new Object[]{
                "h2kTimestamp" , 1538756417237L,
                "location" , "https://www.zitec-shop.com/en/kugelfuhrungswagen-vierreihig/p-G1156007584",
                "sessionId" , SESSIONID
        }, new Object[]{
                "h2kTimestamp" , 1538767062429L, // timeout
                "location" , "https://www.zitec-shop.com/en/kugelfuhrungswagen-vierreihig/p-G1156007584",
                "sessionId" , SESSIONID
        }, new Object[]{
                "h2kTimestamp" , 1538767070188L,
                "location" , "https://www.zitec-shop.com/en/search/?text=nah25",
                "sessionId" , SESSIONID
        }, new Object[]{
                "h2kTimestamp" , 1538767073907L,
                "location" , "https://www.zitec-shop.com/en/search/?text=nah25",
                "sessionId" , SESSIONID
        }, new Object[]{
                "h2kTimestamp" , 1538767077273L,
                "location" , "https://www.zitec-shop.com/en/search/?text=nah",
                "sessionId" , SESSIONID
        }, new Object[]{
                "h2kTimestamp" , 1538780539746L, // timeout
                "location" , "https://www.zitec-shop.com/en/search/?text=nah",
                "sessionId" , SESSIONID
        }, new Object[]{
                "h2kTimestamp" , 1538780546243L,
                "location" , "https://www.zitec-shop.com/en/search/?text=hj234",
                "sessionId" , SESSIONID
        }, new Object[]{
                "h2kTimestamp" , 1538780578259L,
                "location" , "https://www.zitec-shop.com/en/search/?text=hj234",
                "sessionId" , SESSIONID
        }, new Object[]{
                "h2kTimestamp" , 1538780595932L,
                "location" , "https://www.zitec-shop.com/en/search/?text=hj+234",
                "sessionId" , SESSIONID
        }, new Object[]{
                "h2kTimestamp" , 1538780666747L,
                "location" , "https://www.zitec-shop.com/en/search/?text=hj+234",
                "sessionId" , SESSIONID
        }, new Object[]{
                "h2kTimestamp" , 1538780680777L,
                "location" , "https://www.zitec-shop.com/en/sealed-spherical-roller-bearings-ws222-e1/p-G1112006937",
                "sessionId" , SESSIONID
        }, new Object[]{
                "h2kTimestamp" , 1538780684232L,
                "location" , "https://www.zitec-shop.com/en/login",
                "sessionId" , SESSIONID
        }, new Object[]{
                "h2kTimestamp" , 1538780691752L,
                "location" , "https://www.zitec-shop.com/en/sealed-spherical-roller-bearings-ws222-e1/p-G1112006937",
                "sessionId" , SESSIONID
        }, new Object[]{
                "h2kTimestamp" , 1538780748044L,
                "location" , "https://www.zitec-shop.com/en/sealed-spherical-roller-bearings-ws222-e1/p-G1112006937",
                "sessionId" , SESSIONID
        }, new Object[]{
                "h2kTimestamp" , 1538780763016L,
                "location" , "https://www.zitec-shop.com/en/roller-bearing-spherical-radial-multi-row/p-G1321019550",
                "sessionId" , SESSIONID}};

        int eventCount = 0;

        TestRunner testRunner = newTestRunner(container);
        testRunner.setProperty(IncrementalWebSession.SESSION_INACTIVITY_TIMEOUT_CONF, "1800");
        testRunner.assertValid();
        List<Record> events = new ArrayList<>(3);
        for(final Object line: data2)
        {
            final Iterator iterator = Arrays.asList((Object[])line).iterator();
            final Map fields = new HashMap();
            while (iterator.hasNext())
            {
                Object name = iterator.next();
                Object value = iterator.next();
                fields.put(name, value);
            }
            events.add(new WebEvent(String.valueOf(eventCount++),
                                    (String)fields.get("sessionId"),
                                    (String)fields.get("userId"),
                                    (Long)fields.get("h2kTimestamp"),
                                    (String)fields.get("location")));
        }
        testRunner.enqueue(events);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);

        // One webSession expected.
        testRunner.assertOutputRecordsCount(4);
        List<MockRecord> sessions = testRunner.getOutputRecords();
        sessions.sort(new Comparator<MockRecord>() {
            @Override
            public int compare(MockRecord o1, MockRecord o2) {
                long t1 = (long) o1.getField("h2kTimestamp").getRawValue();
                long t2 = (long) o2.getField("h2kTimestamp").getRawValue();
                return Long.compare(t1, t2);
            }
        });
        new WebSessionChecker(sessions.get(0)).sessionId(SESSIONID)
                .Userid(null)
                .record_type("consolidate-session")
                .record_id(SESSIONID)
                .firstEventDateTime(1538753339113L)
                .h2kTimestamp(1538753339113L)
                .firstVisitedPage("https://www.zitec-shop.com/en/")
                .eventsCounter(4)
                .lastEventDateTime(1538753768489L)
                .lastVisitedPage("https://www.zitec-shop.com/en/schragkugellager-718-tn/p-G1156005137")
                .sessionDuration(429L)
                .is_sessionActive(false)
                .sessionInactivityDuration(1800L);
        new WebSessionChecker(sessions.get(1)).sessionId(SESSIONID + "#2")
                .Userid(null)
                .record_type("consolidate-session")
                .record_id(SESSIONID + "#2")
                .firstEventDateTime(1538756201154L)
                .h2kTimestamp(1538756201154L)
                .firstVisitedPage("https://www.zitec-shop.com/en/schragkugellager-718-tn/p-G1156005137")
                .eventsCounter(6)
                .lastEventDateTime(1538756417237L)
                .lastVisitedPage("https://www.zitec-shop.com/en/kugelfuhrungswagen-vierreihig/p-G1156007584")
                .sessionDuration(216L)
                .is_sessionActive(false)
                .sessionInactivityDuration(1800L);
        new WebSessionChecker(sessions.get(2)).sessionId(SESSIONID + "#3")
                .Userid(null)
                .record_type("consolidate-session")
                .record_id(SESSIONID + "#3")
                .firstEventDateTime(1538767062429L)
                .h2kTimestamp(1538767062429L)
                .firstVisitedPage("https://www.zitec-shop.com/en/kugelfuhrungswagen-vierreihig/p-G1156007584")
                .eventsCounter(4)
                .lastEventDateTime(1538767077273L)
                .lastVisitedPage("https://www.zitec-shop.com/en/search/?text=nah")
                .sessionDuration(14L)
                .is_sessionActive(false)
                .sessionInactivityDuration(1800L);
        new WebSessionChecker(sessions.get(3)).sessionId(SESSIONID + "#4")
                .Userid(null)
                .record_type("consolidate-session")
                .record_id(SESSIONID + "#4")
                .firstEventDateTime(1538780539746L)
                .h2kTimestamp(1538780539746L)
                .firstVisitedPage("https://www.zitec-shop.com/en/search/?text=nah")
                .eventsCounter(10)
                .lastEventDateTime(1538780763016L)
                .lastVisitedPage("https://www.zitec-shop.com/en/roller-bearing-spherical-radial-multi-row/p-G1321019550")
                .sessionDuration(223L)
                .is_sessionActive(false)
                .sessionInactivityDuration(1800L);
    }

    @Test
    public void testCreateOneSessionMultipleEventsData(DockerComposeContainer container)
            throws Exception
    {
        Object[] data = new Object[]{
                new Object[]{"sessionId", SESSION1,
                        "partyId", PARTY_ID1,
                        "location", "https://orexad.preprod.group-iph.com/fr/entretien-de-fluides/c-20-50-10",
                        "h2kTimestamp", 1529673800671L,
                        "userId", null},
                new Object[]{"sessionId", SESSION1,
                        "partyId", PARTY_ID1,
                        "location", "https://orexad.preprod.group-iph.com/fr/entretien-de-fluides/c-20-50-10?utm_source=TEST&utm_medium=email&utm_campaign=HT",
                        "h2kTimestamp", 1529673855863L,
                        "userId", null},
                new Object[]{"sessionId", SESSION1,
                        "partyId", PARTY_ID1,
                        "location", "https://orexad.preprod.group-iph.com/fr/entretien-de-fluides/c-20-50-10",
                        "h2kTimestamp", 1529673912936L,
                        "userId", null}};
        int eventCount = 0;

        TestRunner testRunner = newTestRunner(container);
        testRunner.assertValid();
        List<Record> events = new ArrayList<>(3);
        for(final Object line: data)
        {
            final Iterator iterator = Arrays.asList((Object[])line).iterator();
            final Map fields = new HashMap();
            while (iterator.hasNext())
            {
                Object name = iterator.next();
                Object value = iterator.next();
                fields.put(name, value);
            }
            events.add(new WebEvent(String.valueOf(eventCount++),
                                    (String)fields.get("sessionId"),
                                    (String)fields.get("userId"),
                                    (Long)fields.get("h2kTimestamp"),
                                    (String)fields.get("location")));
        }
        testRunner.enqueue(events);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);

        // One webSession expected.
        testRunner.assertOutputRecordsCount(1);

        final Record firstEvent = events.get(0);
        final Record lastEvent = events.get(2);
        final String user = firstEvent.getField(USER_ID)==null?null:firstEvent.getField(USER_ID).asString();

        final MockRecord doc = getFirstRecordWithId((String)firstEvent.getField(SESSION_ID).getRawValue(), testRunner.getOutputRecords());

        new WebSessionChecker(doc).sessionId(firstEvent.getField(SESSION_ID).getRawValue())
                                  .Userid(user)
                                  .record_type("consolidate-session")
                                  .record_id(firstEvent.getField(SESSION_ID).getRawValue())
                                  .firstEventDateTime(firstEvent.getField(TIMESTAMP).asLong())
                                  .h2kTimestamp((Long)firstEvent.getField(TIMESTAMP).getRawValue())
                                  .firstVisitedPage(firstEvent.getField(VISITED_PAGE).getRawValue())
                                  .eventsCounter(3)
                                  .lastEventDateTime(lastEvent.getField(TIMESTAMP).asLong())
                                  .lastVisitedPage(lastEvent.getField(VISITED_PAGE).getRawValue())
                                  .sessionDuration((lastEvent.getField(TIMESTAMP).asLong() // lastEvent.getField(TIMESTAMP)
                                                            -firstEvent.getField(TIMESTAMP).asLong())/1000)
                                  .is_sessionActive((Instant.now().toEpochMilli()
                                                             -lastEvent.getField(TIMESTAMP).asLong())/1000< SESSION_TIMEOUT_SECONDS)
                                  .sessionInactivityDuration(SESSION_TIMEOUT_SECONDS);
    }

    @Test
    public void testCreateTwoSessionTwoEvents(DockerComposeContainer container)
            throws Exception
    {
        
        int eventCount = 0;

        TestRunner testRunner = newTestRunner(container);
        testRunner.assertValid();
        testRunner.enqueue(Arrays.asList(new WebEvent(String.valueOf(eventCount++), SESSION1, USER1, DAY1, URL1),
                                         new WebEvent(String.valueOf(eventCount++), SESSION2, USER2, DAY2, URL2)));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);

        // Two webSession expected.
        testRunner.assertOutputRecordsCount(2);

        final MockRecord doc1 = getFirstRecordWithId(SESSION1, testRunner.getOutputRecords());
        final MockRecord doc2 = getFirstRecordWithId(SESSION2, testRunner.getOutputRecords());

        new WebSessionChecker(doc1).sessionId(SESSION1)
                                   .Userid(USER1)
                                   .record_type("consolidate-session")
                                   .record_id(SESSION1)
                                   .firstEventDateTime(DAY1)
                                   .h2kTimestamp(DAY1)
                                   .firstVisitedPage(URL1)
                                   .eventsCounter(1)
                                   .lastEventDateTime(DAY1)
                                   .lastVisitedPage(URL1)
                                   .sessionDuration(null)
                                   .is_sessionActive(false)
                                   .sessionInactivityDuration(SESSION_TIMEOUT_SECONDS);

        new WebSessionChecker(doc2).sessionId(SESSION2)
                                   .Userid(USER2)
                                   .record_type("consolidate-session")
                                   .record_id(SESSION2)
                                   .firstEventDateTime(DAY2)
                                   .h2kTimestamp(DAY2)
                                   .firstVisitedPage(URL2)
                                   .eventsCounter(1)
                                   .lastEventDateTime(DAY2)
                                   .lastVisitedPage(URL2)
                                   .sessionDuration(null)
                                   .is_sessionActive(false)
                                   .sessionInactivityDuration(SESSION_TIMEOUT_SECONDS);
    }

    @Test
    public void testCreateOneActiveSessionOneEvent(DockerComposeContainer container)
            throws Exception
    {
        
        int eventCount = 0;

        final long now = Instant.now().toEpochMilli();

        TestRunner testRunner = newTestRunner(container);
        testRunner.assertValid();
        testRunner.enqueue(Arrays.asList(new WebEvent(String.valueOf(eventCount++), SESSION1, USER1, now, URL1)));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);

        // One webSession expected.
        testRunner.assertOutputRecordsCount(1);
        final MockRecord doc = getFirstRecordWithId(SESSION1, testRunner.getOutputRecords());

        new WebSessionChecker(doc).sessionId(SESSION1)
                                  .Userid(USER1)
                                  .record_type("consolidate-session")
                                  .record_id(SESSION1)
                                  .firstEventDateTime(now)
                                  .h2kTimestamp(now)
                                  .firstVisitedPage(URL1)
                                  .eventsCounter(1)
                                  .lastEventDateTime(now)
                                  .lastVisitedPage(URL1)
                                  .sessionDuration(null)
                                  .is_sessionActive(true)
                                  .sessionInactivityDuration(null);
    }

    @Test
    public void testCreateIgnoreOneEventWithoutSessionId(DockerComposeContainer container)
            throws Exception
    {
        
        int eventCount = 0;

        TestRunner testRunner = newTestRunner(container);
        testRunner.assertValid();
        testRunner.enqueue(Arrays.asList(new WebEvent(String.valueOf(eventCount++), SESSION1, USER1, DAY1, URL1),
                                         new WebEvent(String.valueOf(eventCount++), null, USER1, DAY1, URL1)));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);

        // One webSession expected.
        testRunner.assertOutputRecordsCount(1);
        final MockRecord doc = getFirstRecordWithId(SESSION1, testRunner.getOutputRecords());

        new WebSessionChecker(doc).sessionId(SESSION1)
                                  .Userid(USER1)
                                  .record_type("consolidate-session")
                                  .record_id(SESSION1)
                                  .firstEventDateTime(DAY1)
                                  .h2kTimestamp(DAY1)
                                  .firstVisitedPage(URL1)
                                  .eventsCounter(1)
                                  .lastEventDateTime(DAY1)
                                  .lastVisitedPage(URL1)
                                  .sessionDuration(null)
                                  .is_sessionActive(false)
                                  .sessionInactivityDuration(SESSION_TIMEOUT_SECONDS);

    }

    @Test
    public void testCreateIgnoreOneEventWithoutTimestamp(DockerComposeContainer container)
            throws Exception
    {
        
        int eventCount = 0;

        TestRunner testRunner = newTestRunner(container);
        testRunner.assertValid();
        testRunner.enqueue(Arrays.asList(new WebEvent(String.valueOf(eventCount++), SESSION1, USER1, DAY1, URL1),
                                         new WebEvent(String.valueOf(eventCount++), SESSION1, USER1, null, URL2)));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);

        // One webSession expected .
        testRunner.assertOutputRecordsCount(1);
        final MockRecord doc = getFirstRecordWithId(SESSION1, testRunner.getOutputRecords());

        new WebSessionChecker(doc).sessionId(SESSION1)
                                  .Userid(USER1)
                                  .record_type("consolidate-session")
                                  .record_id(SESSION1)
                                  .firstEventDateTime(DAY1)
                                  .h2kTimestamp(DAY1)
                                  .firstVisitedPage(URL1)
                                  .eventsCounter(1)
                                  .lastEventDateTime(DAY1)
                                  .lastVisitedPage(URL1)
                                  .sessionDuration(null)
                                  .is_sessionActive(false)
                                  .sessionInactivityDuration(SESSION_TIMEOUT_SECONDS);

    }

    @Test
    public void testBuggyResume(DockerComposeContainer container)
            throws Exception
    {
        
        int eventCount = 0;
        final long DAY1 = 1531492035025L;
        final long TIME2 = 1531494495026L;
        final String URL1 = "https://orexad.preprod.group-iph.com/fr/cart";
        final String URL2 = "https://orexad.preprod.group-iph.com/fr/checkout/single/summary";
        final long SESSION_TIMEOUT = 1800;

        final Collection<Record> events = Arrays.asList(
            new WebEvent(String.valueOf(eventCount++), SESSION1, USER1, DAY1,
                         URL1),
            new WebEvent(String.valueOf(eventCount++), SESSION1, USER1, 1531492435034L,
                         "https://orexad.preprod.group-iph.com/fr/cart"),
            new WebEvent(String.valueOf(eventCount++), SESSION1, USER1, 1531493380029L,
                         "https://orexad.preprod.group-iph.com/fr/search/?text=Vis"),
            new WebEvent(String.valueOf(eventCount++), SESSION1, USER1, 1531493805028L,
                         "https://orexad.preprod.group-iph.com/fr/search/?text=Vis"),
            new WebEvent(String.valueOf(eventCount++), SESSION1, USER1, 1531493810026L,
                         "https://orexad.preprod.group-iph.com/fr/vis-traction-complete-p-kit-k300/p-G1296007152?l=G1296007152"),
            new WebEvent(String.valueOf(eventCount++), SESSION1, USER1, 1531494175027L,
                         "https://orexad.preprod.group-iph.com/fr/cart"),
            new WebEvent(String.valueOf(eventCount++), SESSION1, USER1, 1531494180026L,
                         "https://orexad.preprod.group-iph.com/fr/cart"),
            new WebEvent(String.valueOf(eventCount++), SESSION1, USER1, 1531494480026L,
                         "https://orexad.preprod.group-iph.com/fr/cart"),
            new WebEvent(String.valueOf(eventCount++), SESSION1, USER1, TIME2,
                         URL2));

        TestRunner testRunner = newTestRunner(container);
        testRunner.setProperty(IncrementalWebSession.SESSION_INACTIVITY_TIMEOUT_CONF, String.valueOf(SESSION_TIMEOUT));
        testRunner.assertValid();
        testRunner.enqueue(events);

        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);

        // One webSession expected .
        testRunner.assertOutputRecordsCount(1);
        final MockRecord doc = getFirstRecordWithId(SESSION1, testRunner.getOutputRecords());

        new WebSessionChecker(doc).sessionId(SESSION1)
                                  .Userid(USER1)
                                  .record_type("consolidate-session")
                                  .record_id(SESSION1)
                                  .firstEventDateTime(DAY1)
                                  .h2kTimestamp(DAY1)
                                  .firstVisitedPage(URL1)
                                  .eventsCounter(events.size())
                                  .lastEventDateTime(TIME2)
                                  .lastVisitedPage(URL2)
                                  .sessionDuration((TIME2-DAY1)/1000)
                                  .is_sessionActive(false)
                                  .sessionInactivityDuration(SESSION_TIMEOUT);
    }

    @Test
    public void testCreateGrabOneFieldPresentEveryWhere(DockerComposeContainer container)
            throws Exception
    {
        
        int eventCount = 0;

        TestRunner testRunner = newTestRunner(container);
        testRunner.assertValid();
        testRunner.enqueue(Arrays.asList(new WebEvent(String.valueOf(eventCount++), SESSION1, USER1, DAY1, URL1).add(PARTY_ID, PARTY_ID1)));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);

        // One webSession expected.
        testRunner.assertOutputRecordsCount(1);
        final MockRecord doc = getFirstRecordWithId(SESSION1, testRunner.getOutputRecords());

        new WebSessionChecker(doc).sessionId(SESSION1)
                                  .Userid(USER1)
                                  .record_type("consolidate-session")
                                  .record_id(SESSION1)
                                  .firstEventDateTime(DAY1)
                                  .h2kTimestamp(DAY1)
                                  .firstVisitedPage(URL1)
                                  .eventsCounter(1)
                                  .lastEventDateTime(DAY1)
                                  .lastVisitedPage(URL1)
                                  .sessionDuration(null)
                                  .is_sessionActive(false)
                                  .sessionInactivityDuration(SESSION_TIMEOUT_SECONDS)
                                  .check(PARTY_ID, PARTY_ID1)
                                  .check(B2BUNIT, null);
    }

    @Test
    public void testCreateGrabTwoFieldsPresentEveryWhere(DockerComposeContainer container)
            throws Exception
    {
        
        int eventCount = 0;

        TestRunner testRunner = newTestRunner(container);
        testRunner.assertValid();
        testRunner.enqueue(Arrays.asList(new WebEvent(String.valueOf(eventCount++), SESSION1, USER1, DAY1, URL1).add(PARTY_ID, PARTY_ID1)));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);

        // One webSession expected.
        testRunner.assertOutputRecordsCount(1);
        final MockRecord doc = getFirstRecordWithId(SESSION1, testRunner.getOutputRecords());

        new WebSessionChecker(doc).sessionId(SESSION1)
                                  .Userid(USER1)
                                  .record_type("consolidate-session")
                                  .record_id(SESSION1)
                                  .firstEventDateTime(DAY1)
                                  .h2kTimestamp(DAY1)
                                  .firstVisitedPage(URL1)
                                  .eventsCounter(1)
                                  .lastEventDateTime(DAY1)
                                  .lastVisitedPage(URL1)
                                  .sessionDuration(null)
                                  .is_sessionActive(false)
                                  .sessionInactivityDuration(SESSION_TIMEOUT_SECONDS)
                                  .check(PARTY_ID, PARTY_ID1)
                                  .check(B2BUNIT, null);
    }

    private void injectSessions(List<MockRecord> sessions) {
        ElasticsearchServiceUtil.injectSessionsThenRefresh(this.elasticsearchClientService, sessions);
    }

    @Test
    public void testUpdateOneWebSessionNow(DockerComposeContainer container, RestHighLevelClient esclient)
            throws Exception
    {
        
        int eventCount = 0;

        Instant firstEvent = Instant.now().minusSeconds(8);
        TestRunner testRunner = newTestRunner(container);
        testRunner.assertValid();
        testRunner.enqueue(Arrays.asList(new WebEvent(String.valueOf(eventCount++), SESSION1, USER1, firstEvent.toEpochMilli(), URL1)));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);
        injectSessions(testRunner.getOutputRecords());

        Instant lastEvent = firstEvent.plusSeconds(2);
        testRunner = newTestRunner(container);
        testRunner.enqueue(Arrays.asList(new WebEvent(String.valueOf(eventCount++), SESSION1, USER1, lastEvent.toEpochMilli(), URL2)));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);
        injectSessions(testRunner.getOutputRecords());

        lastEvent = lastEvent.plusSeconds(4);
        testRunner = newTestRunner(container);
        testRunner.enqueue(Arrays.asList(new WebEvent(String.valueOf(eventCount++), SESSION1, USER1, lastEvent.toEpochMilli(), URL3)));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);
        injectSessions(testRunner.getOutputRecords());

        // One webSession expected.
        testRunner.assertOutputRecordsCount(1);

        List<WebSession> sessions = ElasticsearchServiceUtil.getAllSessions(
                this.elasticsearchClientService, esclient);
        Set<String> ids = sessions.stream().map(WebSession::getSessionId).collect(Collectors.toSet());
        Assert.assertTrue(ids.contains(SESSION1));

        final MockRecord doc = getFirstRecordWithId(SESSION1, testRunner.getOutputRecords());

        new WebSessionChecker(doc).sessionId(SESSION1)
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

        testRunner.assertOutputErrorCount(0);
    }

    @Test
    public void testUpdateOneWebSessionInactive(DockerComposeContainer container,
                                                RestHighLevelClient esclient)
            throws Exception
    {
        
        int eventCount = 0;

        // Create a web session with timestamp 2s before timeout.
        Instant firstEvent = Instant.now().minusSeconds(SESSION_TIMEOUT_SECONDS -2);
        TestRunner testRunner = newTestRunner(container);
        testRunner.assertValid();
        testRunner.enqueue(Arrays.asList(new WebEvent(String.valueOf(eventCount++), SESSION1, USER1, firstEvent.toEpochMilli(), URL1)));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);
        injectSessions(testRunner.getOutputRecords());

//        Record doc = this.elasticsearchClientService.getSessionFromEs(SESSION1);
        Record doc = ElasticsearchServiceUtil.getSessionFromEs(elasticsearchClientService, esclient,
                SESSION1,
                TestMappings.sessionInternalFields);
        new WebSessionChecker(doc).lastVisitedPage(URL1);

        // Update web session with timestamp 1s before timeout.
        Instant event = firstEvent.plusSeconds(1);
        testRunner = newTestRunner(container);
        testRunner.enqueue(Arrays.asList(new WebEvent(String.valueOf(eventCount++), SESSION1, USER1, event.toEpochMilli(), URL2)));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);
        injectSessions(testRunner.getOutputRecords());

        doc = ElasticsearchServiceUtil.getSessionFromEs(elasticsearchClientService, esclient,
                SESSION1,
                TestMappings.sessionInternalFields);
        new WebSessionChecker(doc).lastVisitedPage(URL2);

        Thread.sleep(5000); // Make sure the Instant.now performed in the processor will exceed timeout.

        // Update web session with NOW+2s+SESSION_TIMEOUT.
        Instant lastEvent = event.plusSeconds(1);
        testRunner = newTestRunner(container);
        testRunner.enqueue(Arrays.asList(new WebEvent(String.valueOf(eventCount++), SESSION1, USER1, lastEvent.toEpochMilli(), URL3)));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        injectSessions(testRunner.getOutputRecords());

        // One webSession + 2 webEvents + 1 mapping expected in elasticsearch.
        //TODO ?
        List<WebSession> sessions = ElasticsearchServiceUtil.getAllSessions(
                this.elasticsearchClientService, esclient);
        Set<String> ids = sessions.stream().map(WebSession::getSessionId).collect(Collectors.toSet());
        Assert.assertTrue(ids.contains(SESSION1));

        doc = ElasticsearchServiceUtil.getSessionFromEs(elasticsearchClientService, esclient,
                SESSION1,
                TestMappings.sessionInternalFields);

        new WebSessionChecker(doc).sessionId(SESSION1)
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
                                  .is_sessionActive(false)
                                  .sessionInactivityDuration(SESSION_TIMEOUT_SECONDS);

        testRunner.assertOutputRecordsCount(1);
        testRunner.assertOutputErrorCount(0);
    }

    @Test
    public void testUpdateOneWebSessionTimedout(DockerComposeContainer container,
                                                RestHighLevelClient esclient)
            throws Exception
    {
        
        int eventCount = 0;

        // Create a web session with timestamp 2s before timeout.
        Instant firstEvent = Instant.now().minusSeconds(SESSION_TIMEOUT_SECONDS +2);
        TestRunner testRunner = newTestRunner(container);
        testRunner.assertValid();
        testRunner.enqueue(Arrays.asList(new WebEvent(String.valueOf(eventCount++), SESSION1, USER1, firstEvent.toEpochMilli(), URL1)));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);
        injectSessions(testRunner.getOutputRecords());

        // Update web session with a timestamp that is timeout.
        Instant timedoutEvent = Instant.now();
        testRunner = newTestRunner(container);
        testRunner.enqueue(Arrays.asList(new WebEvent(String.valueOf(eventCount++), SESSION1, USER1, timedoutEvent.toEpochMilli(), URL2)));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(2);
        injectSessions(testRunner.getOutputRecords());

        // 2 webSessions + 2 webEvents + 1 mapping expected in elasticsearch.
        //TODO ?
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
                                  .sessionInactivityDuration(SESSION_TIMEOUT_SECONDS);

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

        testRunner.assertOutputRecordsCount(2);
        testRunner.assertOutputErrorCount(0);
    }

    @Test
    public void testAdword(DockerComposeContainer container,
                           RestHighLevelClient esclient)
            throws Exception
    {
        
        int eventCount = 0;

        String URL = URL1+"?gclid=XXX";

        TestRunner testRunner = newTestRunner(container);
        testRunner.assertValid();
        testRunner.enqueue(Arrays.asList(new WebEvent(String.valueOf(eventCount++), SESSION1, USER1, DAY1, URL),
                                         new WebEvent(String.valueOf(eventCount++), SESSION1, USER1, DAY2, URL2)));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);
        // Two webSessions expected .
        testRunner.assertOutputRecordsCount(2);
        injectSessions(testRunner.getOutputRecords());
        Record doc = ElasticsearchServiceUtil.getSessionFromEs(elasticsearchClientService, esclient,
                SESSION1,
                TestMappings.sessionInternalFields);

        new WebSessionChecker(doc).sessionId(SESSION1)
                                  .Userid(USER1)
                                  .record_type("consolidate-session")
                                  .record_id(SESSION1)
                                  .firstEventDateTime(DAY1)
                                  .h2kTimestamp(DAY1)
                                  .firstVisitedPage(URL)
                                  .eventsCounter(1)
                                  .lastEventDateTime(DAY1)
                                  .lastVisitedPage(URL)
                                  .sessionDuration(null)
                                  .is_sessionActive(false)
                                  .sessionInactivityDuration(SESSION_TIMEOUT_SECONDS);

        String SESSION = SESSION1+"#2";
        doc = ElasticsearchServiceUtil.getSessionFromEs(elasticsearchClientService, esclient,
                SESSION,
                TestMappings.sessionInternalFields);

        new WebSessionChecker(doc).sessionId(SESSION)
                                  .Userid(USER1)
                                  .record_type("consolidate-session")
                                  .record_id(SESSION)
                                  .firstEventDateTime(DAY2)
                                  .h2kTimestamp(DAY2)
                                  .firstVisitedPage(URL2)
                                  .eventsCounter(1)
                                  .lastEventDateTime(DAY2)
                                  .lastVisitedPage(URL2)
                                  .sessionDuration(null)
                                  .is_sessionActive(false)
                                  .sessionInactivityDuration(SESSION_TIMEOUT_SECONDS);

    }


    @Test
    public void testEventHandleCorrectlyNullArrays(DockerComposeContainer container,
                                                   RestHighLevelClient esclient)
            throws Exception
    {
        
        int eventCount = 0;

        TestRunner testRunner = newTestRunner(container);
        testRunner.assertValid();
        WebEvent inputEvent = new WebEvent(String.valueOf(eventCount++), SESSION1, USER1, DAY1, URL1);
        testRunner.enqueue(Arrays.asList(inputEvent));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);

        // One webSession expected.
        testRunner.assertOutputRecordsCount(1);
        Map<String, Object> event = ElasticsearchServiceUtil.getEventFromEs(elasticsearchClientService, esclient, inputEvent);

        Assert.assertNull(event.get(CURRENT_CART));

        final MockRecord doc = getFirstRecordWithId(SESSION1, testRunner.getOutputRecords());


        new WebSessionChecker(doc).sessionId(SESSION1)
                .Userid(USER1)
                .record_type("consolidate-session")
                .record_id(SESSION1)
                .firstEventDateTime(DAY1)
                .h2kTimestamp(DAY1)
                .currentCart(null)
                .firstVisitedPage(URL1)
                .eventsCounter(1)
                .lastEventDateTime(DAY1)
                .lastVisitedPage(URL1)
                .sessionDuration(null)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT_SECONDS);
    }
//
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
        proc = new IncrementalWebSession();
        final TestRunner runner = TestRunners.newTestRunner(proc);
        final String FIELDS_TO_RETURN = Stream.of("partyId",  "B2BUnit").collect(Collectors.joining(","));
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
        runner.setProperty(IncrementalWebSession.SESSION_INACTIVITY_TIMEOUT_CONF, String.valueOf(SESSION_TIMEOUT_SECONDS));
        runner.setProperty(IncrementalWebSession.FIELDS_TO_RETURN, FIELDS_TO_RETURN);
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

    private MockRecord getFirstRecordWithId(final String id, final List<MockRecord> records)
    {
        return records.stream().filter(record -> record.getId().equals(id)).findFirst().get();
    }
}
