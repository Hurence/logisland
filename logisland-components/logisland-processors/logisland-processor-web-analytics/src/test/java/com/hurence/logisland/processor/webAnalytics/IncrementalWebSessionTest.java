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

import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.controller.AbstractControllerService;
import com.hurence.logisland.record.Field;
import com.hurence.logisland.record.FieldDictionary;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.service.elasticsearch.ElasticsearchClientService;
import com.hurence.logisland.service.elasticsearch.multiGet.MultiGetQueryRecord;
import com.hurence.logisland.service.elasticsearch.multiGet.MultiGetResponseRecord;
import com.hurence.logisland.util.runner.MockRecord;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.Assert;
import org.junit.Test;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Test incremental web-session processor.
 */
public class IncrementalWebSessionTest
{
    Object[] data = new Object[]{
            new Object[]{"sessionId", "0;jiq0aaj2;6osgJiqVwdTq8CGHpIRXdeufGD71mxCW",
                    "partyId", "0:jiq0aaj2:plz07Fik~eNyLzMsxTk6tg00YqKkgBvd",
                    "location", "https://orexad.preprod.group-iph.com/fr/entretien-de-fluides/c-20-50-10",
                    "h2kTimestamp", 1529673800671L,
                    "userId", null},
            new Object[]{"sessionId", "0;jiq0aaj2;6osgJiqVwdTq8CGHpIRXdeufGD71mxCW",
                    "partyId", "0:jiq0aaj2:plz07Fik~eNyLzMsxTk6tg00YqKkgBvd",
                    "location", "https://orexad.preprod.group-iph.com/fr/entretien-de-fluides/c-20-50-10?utm_source=TEST&utm_medium=email&utm_campaign=HT",
                    "h2kTimestamp", 1529673855863L,
                    "userId", null},
            new Object[]{"sessionId", "0;jiq0aaj2;6osgJiqVwdTq8CGHpIRXdeufGD71mxCW",
                    "partyId", "0:jiq0aaj2:plz07Fik~eNyLzMsxTk6tg00YqKkgBvd",
                    "location", "https://orexad.preprod.group-iph.com/fr/entretien-de-fluides/c-20-50-10",
                    "h2kTimestamp", 1529673912936L,
                    "userId", null}};

    Object[] data2 = new Object[]{
            new Object[]{
        "h2kTimestamp" , 1538753339113L,
        "location" , "https://www.zitec-shop.com/en/",
        "sessionId" , "SESSIONID" // "0:jmw62hh3:ef5kWpbpKDNBG5IyGBARUQLemW3JP0PP"
    }, new Object[]{
        "h2kTimestamp" , 1538753689109L,
        "location" , "https://www.zitec-shop.com/en/",
        "sessionId" , "SESSIONID"
    }, new Object[]{
        "h2kTimestamp" , 1538753753964L,
        "location" , "https://www.zitec-shop.com/en/",
        "sessionId" , "SESSIONID"
    }, new Object[]{
        "h2kTimestamp" , 1538753768489L,
        "location" , "https://www.zitec-shop.com/en/schragkugellager-718-tn/p-G1156005137",
        "sessionId" , "SESSIONID"
    }, new Object[]{
        "h2kTimestamp" , 1538756201154L, // timeout
        "location" , "https://www.zitec-shop.com/en/schragkugellager-718-tn/p-G1156005137",
        "sessionId" , "SESSIONID"
    }, new Object[]{
        "h2kTimestamp" , 1538756215043L,
        "location" , "https://www.zitec-shop.com/en/search/?text=rotex%2Cgg%2C48",
        "sessionId" , "SESSIONID"
    }, new Object[]{
        "h2kTimestamp" , 1538756216242L,
        "location" , "https://www.zitec-shop.com/en/search/?text=rotex%2Cgg%2C48",
        "sessionId" , "SESSIONID"
    }, new Object[]{
        "h2kTimestamp" , 1538756232483L,
        "location" , "https://www.zitec-shop.com/en/rotex-48-gg/p-G1184000392",
        "sessionId" , "SESSIONID"
    }, new Object[]{
        "h2kTimestamp" , 1538756400671L,
        "location" , "https://www.zitec-shop.com/en/rotex-48-gg/p-G1184000392",
        "sessionId" , "SESSIONID"
    }, new Object[]{
        "h2kTimestamp" , 1538756417237L,
        "location" , "https://www.zitec-shop.com/en/kugelfuhrungswagen-vierreihig/p-G1156007584",
        "sessionId" , "SESSIONID"
    }, new Object[]{
        "h2kTimestamp" , 1538767062429L, // timeout
        "location" , "https://www.zitec-shop.com/en/kugelfuhrungswagen-vierreihig/p-G1156007584",
        "sessionId" , "SESSIONID"
    }, new Object[]{
        "h2kTimestamp" , 1538767070188L,
        "location" , "https://www.zitec-shop.com/en/search/?text=nah25",
        "sessionId" , "SESSIONID"
    }, new Object[]{
        "h2kTimestamp" , 1538767073907L,
        "location" , "https://www.zitec-shop.com/en/search/?text=nah25",
        "sessionId" , "SESSIONID"
    }, new Object[]{
        "h2kTimestamp" , 1538767077273L,
        "location" , "https://www.zitec-shop.com/en/search/?text=nah",
        "sessionId" , "SESSIONID"
    }, new Object[]{
        "h2kTimestamp" , 1538780539746L, // timeout
        "location" , "https://www.zitec-shop.com/en/search/?text=nah",
        "sessionId" , "SESSIONID"
    }, new Object[]{
        "h2kTimestamp" , 1538780546243L,
        "location" , "https://www.zitec-shop.com/en/search/?text=hj234",
        "sessionId" , "SESSIONID"
    }, new Object[]{
        "h2kTimestamp" , 1538780578259L,
        "location" , "https://www.zitec-shop.com/en/search/?text=hj234",
        "sessionId" , "SESSIONID"
    }, new Object[]{
        "h2kTimestamp" , 1538780595932L,
        "location" , "https://www.zitec-shop.com/en/search/?text=hj+234",
        "sessionId" , "SESSIONID"
    }, new Object[]{
        "h2kTimestamp" , 1538780666747L,
        "location" , "https://www.zitec-shop.com/en/search/?text=hj+234",
        "sessionId" , "SESSIONID"
    }, new Object[]{
        "h2kTimestamp" , 1538780680777L,
        "location" , "https://www.zitec-shop.com/en/sealed-spherical-roller-bearings-ws222-e1/p-G1112006937",
        "sessionId" , "SESSIONID"
    }, new Object[]{
        "h2kTimestamp" , 1538780684232L,
        "location" , "https://www.zitec-shop.com/en/login",
        "sessionId" , "SESSIONID"
    }, new Object[]{
        "h2kTimestamp" , 1538780691752L,
        "location" , "https://www.zitec-shop.com/en/sealed-spherical-roller-bearings-ws222-e1/p-G1112006937",
        "sessionId" , "SESSIONID"
    }, new Object[]{
        "h2kTimestamp" , 1538780748044L,
        "location" , "https://www.zitec-shop.com/en/sealed-spherical-roller-bearings-ws222-e1/p-G1112006937",
        "sessionId" , "SESSIONID"
    }, new Object[]{
        "h2kTimestamp" , 1538780763016L,
        "location" , "https://www.zitec-shop.com/en/roller-bearing-spherical-radial-multi-row/p-G1321019550",
        "sessionId" , "SESSIONID"}};


    Object[] data3 = new Object[]{
            new Object[]{
        "h2kTimestamp" , 1538753753964L,
        "location" , "https://www.zitec-shop.com/en/",
        "sessionId" , "0:jmw62hh3:ef5kWpbpKDNBG5IyGBARUQLemW3JP0PP"
    }, new Object[]{
        "h2kTimestamp" , 1538753768489L,
        "location" , "https://www.zitec-shop.com/en/schragkugellager-718-tn/p-G1156005137",
        "sessionId" , "0:jmw62hh3:ef5kWpbpKDNBG5IyGBARUQLemW3JP0PP"
    }, new Object[]{
        "h2kTimestamp" , 1538767062429L,
        "location" , "https://www.zitec-shop.com/en/kugelfuhrungswagen-vierreihig/p-G1156007584",
        "sessionId" , "0:jmw62hh3:ef5kWpbpKDNBG5IyGBARUQLemW3JP0PP"
    }, new Object[]{
        "h2kTimestamp" , 1538756216242L,
        "location" , "https://www.zitec-shop.com/en/search/?text=rotex%2Cgg%2C48",
        "sessionId" , "0:jmw62hh3:ef5kWpbpKDNBG5IyGBARUQLemW3JP0PP"
    }, new Object[]{
        "h2kTimestamp" , 1538756215043L,
        "location" , "https://www.zitec-shop.com/en/search/?text=rotex%2Cgg%2C48",
        "sessionId" , "0:jmw62hh3:ef5kWpbpKDNBG5IyGBARUQLemW3JP0PP"
    }, new Object[]{
        "h2kTimestamp" , 1538753689109L,
        "location" , "https://www.zitec-shop.com/en/",
        "sessionId" , "0:jmw62hh3:ef5kWpbpKDNBG5IyGBARUQLemW3JP0PP"
    }, new Object[]{
        "h2kTimestamp" , 1538756417237L,
        "location" , "https://www.zitec-shop.com/en/kugelfuhrungswagen-vierreihig/p-G1156007584",
        "sessionId" , "0:jmw62hh3:ef5kWpbpKDNBG5IyGBARUQLemW3JP0PP"
    }, new Object[]{
        "h2kTimestamp" , 1538753339113L,
        "location" , "https://www.zitec-shop.com/en/",
        "sessionId" , "0:jmw62hh3:ef5kWpbpKDNBG5IyGBARUQLemW3JP0PP"
    }, new Object[]{
        "h2kTimestamp" , 1538756201154L,
        "location" , "https://www.zitec-shop.com/en/schragkugellager-718-tn/p-G1156005137",
        "sessionId" , "0:jmw62hh3:ef5kWpbpKDNBG5IyGBARUQLemW3JP0PP"
    }, new Object[]{
        "h2kTimestamp" , 1538767070188L,
        "location" , "https://www.zitec-shop.com/en/search/?text=nah25",
        "sessionId" , "0:jmw62hh3:ef5kWpbpKDNBG5IyGBARUQLemW3JP0PP"
    }, new Object[]{
        "h2kTimestamp" , 1538756232483L,
        "location" , "https://www.zitec-shop.com/en/rotex-48-gg/p-G1184000392",
        "sessionId" , "0:jmw62hh3:ef5kWpbpKDNBG5IyGBARUQLemW3JP0PP"
    }, new Object[]{
        "h2kTimestamp" , 1538756400671L,
        "location" , "https://www.zitec-shop.com/en/rotex-48-gg/p-G1184000392",
        "sessionId" , "0:jmw62hh3:ef5kWpbpKDNBG5IyGBARUQLemW3JP0PP"
    }, new Object[]{
        "h2kTimestamp" , 1538767073907L,
        "location" , "https://www.zitec-shop.com/en/search/?text=nah25",
        "sessionId" , "0:jmw62hh3:ef5kWpbpKDNBG5IyGBARUQLemW3JP0PP"
    }, new Object[]{
        "h2kTimestamp" , 1538767077273L,
        "location" , "https://www.zitec-shop.com/en/search/?text=nah",
        "sessionId" , "0:jmw62hh3:ef5kWpbpKDNBG5IyGBARUQLemW3JP0PP"
    }, new Object[]{
        "h2kTimestamp" , 1538780595932L,
        "location" , "https://www.zitec-shop.com/en/search/?text=hj+234",
        "sessionId" , "0:jmw62hh3:ef5kWpbpKDNBG5IyGBARUQLemW3JP0PP"
    }, new Object[]{
        "h2kTimestamp" , 1538780680777L,
        "location" , "https://www.zitec-shop.com/en/sealed-spherical-roller-bearings-ws222-e1/p-G1112006937",
        "sessionId" , "0:jmw62hh3:ef5kWpbpKDNBG5IyGBARUQLemW3JP0PP"
    }, new Object[]{
        "h2kTimestamp" , 1538780666747L,
        "location" , "https://www.zitec-shop.com/en/search/?text=hj+234",
        "sessionId" , "0:jmw62hh3:ef5kWpbpKDNBG5IyGBARUQLemW3JP0PP"
    }, new Object[]{
        "h2kTimestamp" , 1538780539746L,
        "location" , "https://www.zitec-shop.com/en/search/?text=nah",
        "sessionId" , "0:jmw62hh3:ef5kWpbpKDNBG5IyGBARUQLemW3JP0PP"
    }, new Object[]{
        "h2kTimestamp" , 1538780546243L,
        "location" , "https://www.zitec-shop.com/en/search/?text=hj234",
        "sessionId" , "0:jmw62hh3:ef5kWpbpKDNBG5IyGBARUQLemW3JP0PP"
    }, new Object[]{
        "h2kTimestamp" , 1538780578259L,
        "location" , "https://www.zitec-shop.com/en/search/?text=hj234",
        "sessionId" , "0:jmw62hh3:ef5kWpbpKDNBG5IyGBARUQLemW3JP0PP"
    }, new Object[]{
        "h2kTimestamp" , 1538780691752L,
        "location" , "https://www.zitec-shop.com/en/sealed-spherical-roller-bearings-ws222-e1/p-G1112006937",
        "sessionId" , "0:jmw62hh3:ef5kWpbpKDNBG5IyGBARUQLemW3JP0PP"
    }, new Object[]{
        "h2kTimestamp" , 1538780684232L,
        "location" , "https://www.zitec-shop.com/en/login",
        "sessionId" , "0:jmw62hh3:ef5kWpbpKDNBG5IyGBARUQLemW3JP0PP"
    }, new Object[]{
        "h2kTimestamp" , 1538780763016L,
        "location" , "https://www.zitec-shop.com/en/roller-bearing-spherical-radial-multi-row/p-G1321019550",
        "sessionId" , "0:jmw62hh3:ef5kWpbpKDNBG5IyGBARUQLemW3JP0PP"
    }, new Object[]{
        "h2kTimestamp" , 1538780748044L,
        "location" , "https://www.zitec-shop.com/en/sealed-spherical-roller-bearings-ws222-e1/p-G1112006937",
        "sessionId" , "0:jmw62hh3:ef5kWpbpKDNBG5IyGBARUQLemW3JP0PP"}
    };

    private static final String SESSION_INDEX = "openanalytics_websessions";
    private static final String SESSION_TYPE = "sessions";
    private static final String EVENT_INDEX = "openanalytics_webevents";
    private static final String EVENT_TYPE = "event";
    private static final String MAPPING_INDEX = "openanalytics_mappings";

    private static final String SESSION_ID = "sessionId";
    private static final String TIMESTAMP = "h2kTimestamp";
    private static final String VISITED_PAGE = "VISITED_PAGE";
    private static final String CURRENT_CART = "currentCart";
    private static final String USER_ID = "Userid";

    private static final String SESSION1 = "session1";
    private static final String SESSION2 = "session2";

    private static final long SESSION_TIMEOUT = 10; // but should be 30*60;

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

    private static final String FIELDS_TO_RETURN = Stream.of(PARTY_ID, B2BUNIT).collect(Collectors.joining(","));

    private final ESC elasticsearchClient = new ESC();

    private MockRecord getRecord(final String session, final List<MockRecord> records)
    {
        return records.stream().filter(record -> record.getId().equals(session)).findFirst().get();
    }

    @Test
    public void testCreateOneSessionOneEvent()
            throws Exception
    {
        this.elasticsearchClient.documents.clear();
        int eventCount = 0;

        TestRunner testRunner = newTestRunner();
        testRunner.assertValid();
        testRunner.enqueue(Arrays.asList(new WebEvent(eventCount++, SESSION1, USER1, DAY1, URL1)));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);

        // One webSession expected.
        testRunner.assertOutputRecordsCount(1);
        final MockRecord doc = getRecord(SESSION1, testRunner.getOutputRecords());


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
                                  .sessionInactivityDuration(SESSION_TIMEOUT);
    }

    @Test
    public void testCreateOneSessionMultipleEvents()
            throws Exception
    {
        this.elasticsearchClient.documents.clear();
        int eventCount = 0;

        TestRunner testRunner = newTestRunner();
        testRunner.assertValid();
        testRunner.enqueue(Arrays.asList(new WebEvent(eventCount++, SESSION1, null, DAY1, URL1),
                                         new WebEvent(eventCount++, SESSION1, USER1, DAY1+1000L, URL2),
                                         new WebEvent(eventCount++, SESSION1, null, DAY1+2000L, URL3)));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);

        // One webSession expected.
        testRunner.assertOutputRecordsCount(1);
        final MockRecord doc = getRecord(SESSION1, testRunner.getOutputRecords());

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
                                  .sessionInactivityDuration(SESSION_TIMEOUT);
    }

    @Test
    public void testCreateOneSessionMultipleEventsData2()
            throws Exception
    {
        this.elasticsearchClient.documents.clear();
        int eventCount = 0;

        TestRunner testRunner = newTestRunner();
        testRunner.setProperty(IncrementalWebSession.SESSION_INACTIVITY_TIMEOUT, "1800");
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
            events.add(new WebEvent(eventCount++,
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
    }

    //    @Test
    public void testCreateOneSessionMultipleEventsData()
            throws Exception
    {
        this.elasticsearchClient.documents.clear();
        int eventCount = 0;

        TestRunner testRunner = newTestRunner();
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
            events.add(new WebEvent(eventCount++,
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

        final MockRecord doc = getRecord((String)firstEvent.getField(SESSION_ID).getRawValue(), testRunner.getOutputRecords());

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
                                                             -lastEvent.getField(TIMESTAMP).asLong())/1000<SESSION_TIMEOUT)
                                  .sessionInactivityDuration(SESSION_TIMEOUT);
    }

    @Test
    public void testCreateTwoSessionTwoEvents()
            throws Exception
    {
        this.elasticsearchClient.documents.clear();
        int eventCount = 0;

        TestRunner testRunner = newTestRunner();
        testRunner.assertValid();
        testRunner.enqueue(Arrays.asList(new WebEvent(eventCount++, SESSION1, USER1, DAY1, URL1),
                                         new WebEvent(eventCount++, SESSION2, USER2, DAY2, URL2)));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);

        // Two webSession expected.
        testRunner.assertOutputRecordsCount(2);

        final MockRecord doc1 = getRecord(SESSION1, testRunner.getOutputRecords());
        final MockRecord doc2 = getRecord(SESSION2, testRunner.getOutputRecords());

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
                                   .sessionInactivityDuration(SESSION_TIMEOUT);

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
                                   .sessionInactivityDuration(SESSION_TIMEOUT);
    }

    @Test
    public void testCreateOneActiveSessionOneEvent()
            throws Exception
    {
        this.elasticsearchClient.documents.clear();
        int eventCount = 0;

        final long now = Instant.now().toEpochMilli();

        TestRunner testRunner = newTestRunner();
        testRunner.assertValid();
        testRunner.enqueue(Arrays.asList(new WebEvent(eventCount++, SESSION1, USER1, now, URL1)));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);

        // One webSession expected.
        testRunner.assertOutputRecordsCount(1);
        final MockRecord doc = getRecord(SESSION1, testRunner.getOutputRecords());

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
    public void testCreateIgnoreOneEventWithoutSessionId()
            throws Exception
    {
        this.elasticsearchClient.documents.clear();
        int eventCount = 0;

        TestRunner testRunner = newTestRunner();
        testRunner.assertValid();
        testRunner.enqueue(Arrays.asList(new WebEvent(eventCount++, SESSION1, USER1, DAY1, URL1),
                                         new WebEvent(eventCount++, null, USER1, DAY1, URL1)));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);

        // One webSession expected.
        testRunner.assertOutputRecordsCount(1);
        final MockRecord doc = getRecord(SESSION1, testRunner.getOutputRecords());

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
                                  .sessionInactivityDuration(SESSION_TIMEOUT);

    }

    @Test
    public void testCreateIgnoreOneEventWithoutTimestamp()
            throws Exception
    {
        this.elasticsearchClient.documents.clear();
        int eventCount = 0;

        TestRunner testRunner = newTestRunner();
        testRunner.assertValid();
        testRunner.enqueue(Arrays.asList(new WebEvent(eventCount++, SESSION1, USER1, DAY1, URL1),
                                         new WebEvent(eventCount++, SESSION1, USER1, null, URL2)));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);

        // One webSession expected .
        testRunner.assertOutputRecordsCount(1);
        final MockRecord doc = getRecord(SESSION1, testRunner.getOutputRecords());

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
                                  .sessionInactivityDuration(SESSION_TIMEOUT);

    }

    @Test
    public void testBuggyResume()
            throws Exception
    {
        this.elasticsearchClient.documents.clear();
        int eventCount = 0;
        final long DAY1 = 1531492035025L;
        final long TIME2 = 1531494495026L;
        final String URL1 = "https://orexad.preprod.group-iph.com/fr/cart";
        final String URL2 = "https://orexad.preprod.group-iph.com/fr/checkout/single/summary";
        final long SESSION_TIMEOUT = 1800;

        final Collection<Record> events = Arrays.asList(
            new WebEvent(eventCount++, SESSION1, USER1, DAY1,
                         URL1),
            new WebEvent(eventCount++, SESSION1, USER1, 1531492435034L,
                         "https://orexad.preprod.group-iph.com/fr/cart"),
            new WebEvent(eventCount++, SESSION1, USER1, 1531493380029L,
                         "https://orexad.preprod.group-iph.com/fr/search/?text=Vis"),
            new WebEvent(eventCount++, SESSION1, USER1, 1531493805028L,
                         "https://orexad.preprod.group-iph.com/fr/search/?text=Vis"),
            new WebEvent(eventCount++, SESSION1, USER1, 1531493810026L,
                         "https://orexad.preprod.group-iph.com/fr/vis-traction-complete-p-kit-k300/p-G1296007152?l=G1296007152"),
            new WebEvent(eventCount++, SESSION1, USER1, 1531494175027L,
                         "https://orexad.preprod.group-iph.com/fr/cart"),
            new WebEvent(eventCount++, SESSION1, USER1, 1531494180026L,
                         "https://orexad.preprod.group-iph.com/fr/cart"),
            new WebEvent(eventCount++, SESSION1, USER1, 1531494480026L,
                         "https://orexad.preprod.group-iph.com/fr/cart"),
            new WebEvent(eventCount++, SESSION1, USER1, TIME2,
                         URL2));

        TestRunner testRunner = newTestRunner();
        testRunner.setProperty(IncrementalWebSession.SESSION_INACTIVITY_TIMEOUT, String.valueOf(SESSION_TIMEOUT));
        testRunner.assertValid();
        testRunner.enqueue(events);

        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);

        // One webSession expected .
        testRunner.assertOutputRecordsCount(1);
        final MockRecord doc = getRecord(SESSION1, testRunner.getOutputRecords());

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
    public void testCreateGrabOneFieldPresentEveryWhere()
            throws Exception
    {
        this.elasticsearchClient.documents.clear();
        int eventCount = 0;

        TestRunner testRunner = newTestRunner();
        testRunner.assertValid();
        testRunner.enqueue(Arrays.asList(new WebEvent(eventCount++, SESSION1, USER1, DAY1, URL1).add(PARTY_ID, PARTY_ID1)));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);

        // One webSession expected.
        testRunner.assertOutputRecordsCount(1);
        final MockRecord doc = getRecord(SESSION1, testRunner.getOutputRecords());

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
                                  .sessionInactivityDuration(SESSION_TIMEOUT)
                                  .check(PARTY_ID, PARTY_ID1)
                                  .check(B2BUNIT, null);
    }

    @Test
    public void testCreateGrabTwoFieldsPresentEveryWhere()
            throws Exception
    {
        this.elasticsearchClient.documents.clear();
        int eventCount = 0;

        TestRunner testRunner = newTestRunner();
        testRunner.assertValid();
        testRunner.enqueue(Arrays.asList(new WebEvent(eventCount++, SESSION1, USER1, DAY1, URL1).add(PARTY_ID, PARTY_ID1)));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);

        // One webSession expected.
        testRunner.assertOutputRecordsCount(1);
        final MockRecord doc = getRecord(SESSION1, testRunner.getOutputRecords());

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
                                  .sessionInactivityDuration(SESSION_TIMEOUT)
                                  .check(PARTY_ID, PARTY_ID1)
                                  .check(B2BUNIT, null);
    }


    @Test
    public void testUpdateOneWebSessionNow()
            throws Exception
    {
        this.elasticsearchClient.documents.clear();
        int eventCount = 0;

        Instant firstEvent = Instant.now().minusSeconds(8);
        TestRunner testRunner = newTestRunner();
        testRunner.assertValid();
        testRunner.enqueue(Arrays.asList(new WebEvent(eventCount++, SESSION1, USER1, firstEvent.toEpochMilli(), URL1)));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);
        testRunner.getOutputRecords().forEach(record -> this.elasticsearchClient.save(record));

        Instant lastEvent = firstEvent.plusSeconds(2);
        testRunner = newTestRunner();
        testRunner.enqueue(Arrays.asList(new WebEvent(eventCount++, SESSION1, USER1, lastEvent.toEpochMilli(), URL2)));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);
        testRunner.getOutputRecords().forEach(record -> this.elasticsearchClient.save(record));

        lastEvent = lastEvent.plusSeconds(4);
        testRunner = newTestRunner();
        testRunner.enqueue(Arrays.asList(new WebEvent(eventCount++, SESSION1, USER1, lastEvent.toEpochMilli(), URL3)));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.getOutputRecords().forEach(record -> this.elasticsearchClient.save(record));

        // One webSession expected.
        Assert.assertEquals(1+1+eventCount, this.elasticsearchClient.documents.size());
        testRunner.assertOutputRecordsCount(1);
        Set<String> ids = this.elasticsearchClient.documents.keySet().stream().map(id->id.getKeyProperty("id")).collect(Collectors.toSet());
        Assert.assertTrue(ids.contains(SESSION1));

        final MockRecord doc = getRecord(SESSION1, testRunner.getOutputRecords());

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
    public void testUpdateOneWebSessionInactive()
            throws Exception
    {
        this.elasticsearchClient.documents.clear();
        int eventCount = 0;

        // Create a web session with timestamp 2s before timeout.
        Instant firstEvent = Instant.now().minusSeconds(SESSION_TIMEOUT-2);
        TestRunner testRunner = newTestRunner();
        testRunner.assertValid();
        testRunner.enqueue(Arrays.asList(new WebEvent(eventCount++, SESSION1, USER1, firstEvent.toEpochMilli(), URL1)));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);
        testRunner.getOutputRecords().forEach(record -> this.elasticsearchClient.save(record));

        Record doc = this.elasticsearchClient.getSession(SESSION1);
        new WebSessionChecker(doc).lastVisitedPage(URL1);

        // Update web session with timestamp 1s before timeout.
        Instant event = firstEvent.plusSeconds(1);
        testRunner = newTestRunner();
        testRunner.enqueue(Arrays.asList(new WebEvent(eventCount++, SESSION1, USER1, event.toEpochMilli(), URL2)));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);
        testRunner.getOutputRecords().forEach(record -> this.elasticsearchClient.save(record));

        doc = this.elasticsearchClient.getSession(SESSION1);
        new WebSessionChecker(doc).lastVisitedPage(URL2);

        Thread.sleep(5000); // Make sure the Instant.now performed in the processor will exceed timeout.

        // Update web session with NOW+2s+SESSION_TIMEOUT.
        Instant lastEvent = event.plusSeconds(1);
        testRunner = newTestRunner();
        testRunner.enqueue(Arrays.asList(new WebEvent(eventCount++, SESSION1, USER1, lastEvent.toEpochMilli(), URL3)));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.getOutputRecords().forEach(record -> this.elasticsearchClient.save(record));

        // One webSession + 2 webEvents + 1 mapping expected in elasticsearch.
        Assert.assertEquals(1+eventCount+1, this.elasticsearchClient.documents.size());
        Set<String> ids = this.elasticsearchClient.documents.keySet().stream().map(id->id.getKeyProperty("id")).collect(Collectors.toSet());
        Assert.assertTrue(ids.contains(SESSION1));

        doc = this.elasticsearchClient.getSession(SESSION1);

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
                                  .sessionInactivityDuration(SESSION_TIMEOUT);

        testRunner.assertOutputRecordsCount(1);
        testRunner.assertOutputErrorCount(0);
    }

    @Test
    public void testUpdateOneWebSessionTimedout()
            throws Exception
    {
        this.elasticsearchClient.documents.clear();
        int eventCount = 0;

        // Create a web session with timestamp 2s before timeout.
        Instant firstEvent = Instant.now().minusSeconds(SESSION_TIMEOUT+2);
        TestRunner testRunner = newTestRunner();
        testRunner.assertValid();
        testRunner.enqueue(Arrays.asList(new WebEvent(eventCount++, SESSION1, USER1, firstEvent.toEpochMilli(), URL1)));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);
        testRunner.getOutputRecords().forEach(record -> this.elasticsearchClient.save(record));

        // Update web session with a timestamp that is timeout.
        Instant timedoutEvent = Instant.now();
        testRunner = newTestRunner();
        testRunner.enqueue(Arrays.asList(new WebEvent(eventCount++, SESSION1, USER1, timedoutEvent.toEpochMilli(), URL2)));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(2);
        testRunner.getOutputRecords().forEach(record -> this.elasticsearchClient.save(record));

        // 2 webSessions + 2 webEvents + 1 mapping expected in elasticsearch.
        Assert.assertEquals(2+eventCount+1, this.elasticsearchClient.documents.size());
        Set<String> ids = this.elasticsearchClient.documents.keySet().stream().map(id->id.getKeyProperty("id")).collect(Collectors.toSet());
        Assert.assertTrue(ids.contains(SESSION1));

        Record doc = this.elasticsearchClient.getSession(SESSION1);
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
        doc = this.elasticsearchClient.getSession(EXTRA_SESSION);
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
    public void testAdword()
            throws Exception
    {
        this.elasticsearchClient.documents.clear();
        int eventCount = 0;

        String URL = URL1+"?gclid=XXX";

        TestRunner testRunner = newTestRunner();
        testRunner.assertValid();
        testRunner.enqueue(Arrays.asList(new WebEvent(eventCount++, SESSION1, USER1, DAY1, URL),
                                         new WebEvent(eventCount++, SESSION1, USER1, DAY2, URL2)));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);

        // Two webSessions expected .
        testRunner.assertOutputRecordsCount(2);
        testRunner.getOutputRecords().forEach(record -> this.elasticsearchClient.save(record));
        Record doc = this.elasticsearchClient.getSession(SESSION1);

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
                                  .sessionInactivityDuration(SESSION_TIMEOUT);

        String SESSION = SESSION1+"#2";
        doc = this.elasticsearchClient.getSession(SESSION);

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
                                  .sessionInactivityDuration(SESSION_TIMEOUT);

    }


    @Test
    public void testEventHandleCorrectlyNullArrays()
            throws Exception
    {
        this.elasticsearchClient.documents.clear();
        int eventCount = 0;

        TestRunner testRunner = newTestRunner();
        testRunner.assertValid();
        testRunner.enqueue(Arrays.asList(new WebEvent(eventCount++, SESSION1, USER1, DAY1, URL1)));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);

        // One webSession expected.
        testRunner.assertOutputRecordsCount(1);
        Map event = this.elasticsearchClient.documents.get(ESC.toId(
                EVENT_INDEX + "." + java.time.format.DateTimeFormatter.ofPattern("yyyy.MM.dd",
                Locale.ENGLISH).format(ZonedDateTime.ofInstant(Instant.ofEpochMilli(DAY1),
                        ZoneId.systemDefault())), EVENT_TYPE, "0"));

        Assert.assertNull(event.get(CURRENT_CART));

        final MockRecord doc = getRecord(SESSION1, testRunner.getOutputRecords());


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
                .sessionInactivityDuration(SESSION_TIMEOUT);
    }

    /**
     * Creates a new TestRunner set with the appropriate properties.
     *
     * @return a new TestRunner set with the appropriate properties.
     *
     * @throws InitializationException in case the runner could not be instantiated.
     */
    private TestRunner newTestRunner()
            throws InitializationException
    {
        final TestRunner runner = TestRunners.newTestRunner(new IncrementalWebSession());

        runner.addControllerService("elasticsearchClient", elasticsearchClient);
        runner.enableControllerService(elasticsearchClient);
        runner.setProperty(setSourceOfTraffic.ELASTICSEARCH_CLIENT_SERVICE, "elasticsearchClient");

        runner.setProperty(IncrementalWebSession.ES_SESSION_INDEX_FIELD, SESSION_INDEX);
        runner.setProperty(IncrementalWebSession.ES_SESSION_TYPE_NAME, SESSION_TYPE);
        runner.setProperty(IncrementalWebSession.ES_EVENT_INDEX_PREFIX, EVENT_INDEX);
        runner.setProperty(IncrementalWebSession.ES_EVENT_TYPE_NAME, EVENT_TYPE);
        runner.setProperty(IncrementalWebSession.ES_MAPPING_EVENT_TO_SESSION_INDEX_NAME, MAPPING_INDEX);

        runner.setProperty(IncrementalWebSession.SESSION_ID_FIELD, SESSION_ID);
        runner.setProperty(IncrementalWebSession.TIMESTAMP_FIELD, TIMESTAMP);
        runner.setProperty(IncrementalWebSession.VISITED_PAGE_FIELD, VISITED_PAGE);
        runner.setProperty(IncrementalWebSession.USER_ID_FIELD, USER_ID);
        runner.setProperty(IncrementalWebSession.SESSION_INACTIVITY_TIMEOUT, String.valueOf(SESSION_TIMEOUT));

        runner.setProperty(IncrementalWebSession.FIELDS_TO_RETURN, FIELDS_TO_RETURN);
        return runner;
    }

    /**
     * The class represents a web event.
     */
    private static class WebEvent extends StandardRecord
    {
        /**
         * Creates a new instance of this class with the provided parameter.
         *
         * @param id the event identifier.
         * @param sessionId the session identifier.
         * @param userId the user identifier.
         * @param timestamp the h2kTimestamp.
         * @param url the visited address.
         */
        public WebEvent(final int id, final String sessionId, final String userId, final Long timestamp,
                        final String url)
        {
            this.setField(SESSION_ID, FieldType.STRING, sessionId)
                .setField(USER_ID, FieldType.STRING, userId)
                .setField(TIMESTAMP, FieldType.STRING, timestamp)
                .setField(SESSION_INDEX, FieldType.STRING, SESSION_INDEX)
                .setField(VISITED_PAGE, FieldType.STRING, url)
                .setField(CURRENT_CART, FieldType.ARRAY, null)
                .setField("record_id", FieldType.STRING, String.valueOf(id));
        }

        public WebEvent add(final String name, final String value)
        {
            this.setStringField(name, value);
            return this;
        }
    }

    /**
     * A class for testing web session.
     */
    private static class WebSessionChecker
    {
        private final Record record;

        /**
         * Creates a new instance of this class with the provided parameter.
         *
         * @param record the fields to check.
         */
        public WebSessionChecker(final Record record)
        {
            this.record = record;
        }

        public WebSessionChecker sessionId(final Object value) { return check("sessionId", value); }
        public WebSessionChecker Userid(final Object value) { return check("Userid", value); }
        public WebSessionChecker record_type(final Object value) { return check("record_type", value); }
        public WebSessionChecker record_id(final Object value) { return check("record_id", value); }
        public WebSessionChecker currentCart(final Object value) { return check(CURRENT_CART, value); }
        public WebSessionChecker firstEventDateTime(final long value) { return check("firstEventDateTime", new Date(value).toString()); }
        public WebSessionChecker h2kTimestamp(final long value) { return check("h2kTimestamp", value); }
        public WebSessionChecker firstVisitedPage(final Object value) { return check("firstVisitedPage", value); }
        public WebSessionChecker eventsCounter(final long value) { return check("eventsCounter", value); }
        public WebSessionChecker lastEventDateTime(final long value) { return check("lastEventDateTime", new Date(value).toString()); }
        public WebSessionChecker lastVisitedPage(final Object value) { return check("lastVisitedPage", value); }
        public WebSessionChecker sessionDuration(final Object value) { return check("sessionDuration", value); }
        public WebSessionChecker is_sessionActive(final Object value) { return check("is_sessionActive", value); }
        public WebSessionChecker sessionInactivityDuration(final Object value) { return check("sessionInactivityDuration", value); }
        public WebSessionChecker record_time(final Object value) { return check("record_time", value); }

        /**
         * Checks the value associated to the specified name against the provided expected value.
         * An exception is thrown if the check fails.
         *
         * @param name the name of the field to check.
         * @param expectedValue the expected value.
         *
         * @return this object for convenience.
         */
        public WebSessionChecker check(final String name, final Object expectedValue)
        {
            final Field field = this.record.getField(name);
            Assert.assertEquals(expectedValue,
                                field!=null?field.getRawValue():null);
            return this;
        }
    }

    /**
     * A test implementation of ElasticsearchClientService that performs read/write in a map.
     */
    public static final class ESC
            extends AbstractControllerService
            implements ElasticsearchClientService
    {
        /**
         * A map that stores elasticsearch documents as sourceAsMap.
         */
        private final Map<ObjectName/*toId*/, Map<String, ?>/*sourceAsMap*/> documents = new HashMap<>();

        /**
         * Returns the concatenation of provided parameters as docIndex/docType/optionalId.
         *
         * @param docIndex the elasticsearch index
         * @param docType the elasticsearch type
         * @param optionalId the elasticsearch document identifier.
         *
         * @return the concatenation of provided parameters as docIndex/docType/optionalId.
         */
        private static ObjectName toId(String docIndex, String docType, String optionalId)
        {
            try
            {
                return new ObjectName(String.format("es:index=%s,type=%s,id=%s",
                                                    docIndex, docType, optionalId));
            }
            catch(MalformedObjectNameException e)
            {
                throw new RuntimeException(e);
            }
        }

        private Record getSession(final String sessionId)
        {
            final ObjectName filter = toId(SESSION_INDEX, SESSION_TYPE, sessionId);

            Map<String, ?> doc = documents.entrySet().stream().filter(entry-> filter.apply(entry.getKey()))
                                          .map(entry-> entry.getValue()).findFirst().get();

            Record result = new StandardRecord();
            doc.entrySet().stream()
               .forEach(entry->
               {
                   final Object value = entry.getValue();
                   if ( value instanceof Long )
                   {
                       result.setField(entry.getKey(), FieldType.LONG, value);
                   }
                   else if ( value instanceof Boolean )
                   {
                       result.setField(entry.getKey(), FieldType.BOOLEAN, value);
                   }
                   else
                   {
                       result.setStringField(entry.getKey(), (String)value);
                   }

               });

            return result;
        }

        private void save(final Record record)
        {
            final String sessionId = record.getId();
            final String index = SESSION_INDEX;
            final Map<String, ?> sourceAsMap = record.getFieldsEntrySet().stream()
                .map(entry -> new AbstractMap.SimpleEntry<>(entry.getKey(),
                                                            entry.getValue()==null?null:entry.getValue().getRawValue()))
                .collect(Collectors.toMap(Map.Entry::getKey,
                                          Map.Entry::getValue));

            this.documents.put(toId(index, SESSION_TYPE, sessionId), sourceAsMap);
        }

        @Override
        public List<MultiGetResponseRecord> multiGet(List<MultiGetQueryRecord> multiGetQueryRecords)
        {
            final List<MultiGetResponseRecord> result = new ArrayList<>();

            for(final MultiGetQueryRecord request: multiGetQueryRecords)
            {
                for(final String id: request.getDocumentIds())
                {
                    final String index = request.getIndexName();
                    final String type = request.getTypeName();
                    final ObjectName filter = toId(index, type, id);

                    documents.entrySet().forEach(entry->
                    {
                        if (filter.apply(entry.getKey()))
                        {
                            Map<String, String> map = new HashMap<>();
                            entry.getValue().entrySet().stream().forEach(kv -> map.put(kv.getKey(), kv.getValue().toString()));
                            result.add(new MultiGetResponseRecord(index, type, id, map));
                        }
                    });
//                    System.out.println("Requested document _id="+_id+" present="+(document!=null));
                }
            }

            return result;
        }

        @Override
        public void bulkPut(String docIndex, String docType, String document, Optional<String> OptionalId)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void bulkPut(String docIndex, String docType, Map<String, ?> document, Optional<String> optionalId)
        {
            optionalId.orElseThrow(IllegalArgumentException::new);

//            final Map<String, ?> map = document.entrySet().stream()
//                                                    .map(entry -> new AbstractMap.SimpleEntry<>(entry.getKey(),
//                                                                                                entry.getValue()==null?null:entry.getValue()))
//                                                    .collect(Collectors.toMap(Map.Entry::getKey,
//                                                                              Map.Entry::getValue));

//            System.out.println(String.format("Saving index=%s,type=%s,id=%s",
//                                             docIndex, docType, optionalId.get()));
            documents.put(toId(docIndex, docType, optionalId.get()), document);
        }

        @Override
        public void flushBulkProcessor() {}

        @Override
        public boolean existsIndex(String indexName) throws IOException { return false; }

        @Override
        public void refreshIndex(String indexName) throws Exception {}

        @Override
        public void saveAsync(String indexName, String doctype, Map<String, Object> doc) throws Exception {}

        @Override
        public void saveSync(String indexName, String doctype, Map<String, Object> doc) throws Exception {}

        @Override
        public long countIndex(String indexName) throws Exception { return 0; }

        @Override
        public void createIndex(int numShards, int numReplicas, String indexName) throws IOException {}

        @Override
        public void dropIndex(String indexName) throws IOException {}

        @Override
        public void copyIndex(String reindexScrollTimeout, String srcIndex, String dstIndex) throws IOException {}

        @Override
        public void createAlias(String indexName, String aliasName) throws IOException {}

        @Override
        public boolean putMapping(String indexName, String doctype, String mappingAsJsonString) throws IOException { return false; }

        @Override
        public long searchNumberOfHits(String docIndex, String docType, String docName, String docValue) { return 0; }

        @Override
        public String convertRecordToString(Record record) { return convertToString(record);}

        @Override
        public List<PropertyDescriptor> getSupportedPropertyDescriptors(){ return Collections.emptyList(); }
    }

    /**
     * Converts an Event into an Elasticsearch document
     * to be indexed later
     *
     * @param record
     * @return
     */
    public static String convertToString(Record record) {
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
            sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
            String document = "";

            // convert event_time as ISO for ES
            if (record.hasField(FieldDictionary.RECORD_TIME)) {
                try {
                    DateTimeFormatter dateParser = ISODateTimeFormat.dateTimeNoMillis();
                    document += "@timestamp : ";
                    document += dateParser.print(record.getField(FieldDictionary.RECORD_TIME).asLong()) + ", ";
                } catch (Exception ex) {
                    System.err.println(String.format("unable to parse record_time iso date for {}", record));
                }
            }

            // add all other records
            for (Iterator<Field> i = record.getAllFieldsSorted().iterator(); i.hasNext();) {
                Field field = i.next();
                String fieldName = field.getName().replaceAll("\\.", "_");

                switch (field.getType()) {

                    case STRING:
                        document += fieldName + " : " + field.asString();
                        break;
                    case INT:
                        document += fieldName + " : " + field.asInteger().toString();
                        break;
                    case LONG:
                        document += fieldName + " : " + field.asLong().toString();
                        break;
                    case FLOAT:
                        document += fieldName + " : " + field.asFloat().toString();
                        break;
                    case DOUBLE:
                        document += fieldName + " : " + field.asDouble().toString();
                        break;
                    case BOOLEAN:
                        document += fieldName + " : " + field.asBoolean().toString();
                        break;
                    default:
                        document += fieldName + " : " + field.getRawValue().toString();
                        break;
                }
            }

            return document;
        } catch (Throwable ex) {
            System.err.println(String.format("unable to convert record : %s, %s", record, ex.toString()));
        }
        return null;
    }
}
