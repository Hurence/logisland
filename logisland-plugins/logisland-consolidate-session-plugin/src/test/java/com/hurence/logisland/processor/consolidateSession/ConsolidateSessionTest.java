/**
 * Copyright (C) 2017 Hurence 
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
package com.hurence.logisland.processor.consolidateSession;

import com.hurence.logisland.controller.ControllerService;
import com.hurence.logisland.record.FieldDictionary;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.util.runner.MockRecord;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;

import static org.junit.Assert.assertEquals;

import java.util.*;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test consolidate session processor.
 */
public class ConsolidateSessionTest {
    
    private static Logger logger = LoggerFactory.getLogger(ConsolidateSessionTest.class);

    @Test
    public void testOneEventOnlyPerSession()
    {
        Collection<Record> records = new ArrayList<>();

        records.add(new StandardRecord()
                .setField("s", FieldType.STRING, "s1")
                .setField("t", FieldType.STRING, "1493197966584")
                .setField("v", FieldType.STRING, "http://page1"));

        records.add(new StandardRecord()
                .setField("s", FieldType.STRING, "s2")
                .setField("t", FieldType.STRING, "1493197966585")
                .setField("v", FieldType.STRING, "http://page2"));

        records.add(new StandardRecord()
                .setField("s", FieldType.STRING, "s3")
                .setField("t", FieldType.STRING, "1493197966590")
                .setField("v", FieldType.STRING, "http://page3"));


        records.add(new StandardRecord()
                .setField("s", FieldType.STRING, "s4")
                .setField("t", FieldType.STRING, "1493197966584")
                .setField("v", FieldType.STRING, "http://pageX"));


        records.add(new StandardRecord()
                .setField("s", FieldType.STRING, "s5")
                .setField("t", FieldType.STRING, "1493197966584")
                .setField("v", FieldType.STRING, "http://pageY"));

        TestRunner testRunner = TestRunners.newTestRunner(new ConsolidateSession());
        testRunner.setProperty(ConsolidateSession.SESSION_ID_FIELD, "s");
        testRunner.setProperty(ConsolidateSession.TIMESTAMP_FIELD, "t");
        testRunner.setProperty(ConsolidateSession.VISITED_PAGE_FIELD, "v");
        testRunner.assertValid();
        testRunner.enqueue(records);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(5);
        testRunner.assertOutputErrorCount(0);
    }

    @Test
    public void testSeveralEventsPerSession(){
        Collection<Record> records = new ArrayList<>();

        records.add(new StandardRecord()
                .setField("s", FieldType.STRING, "s1")
                .setField("t", FieldType.STRING, "1493197966584")
                .setField("v", FieldType.STRING, "http://page1"));

        records.add(new StandardRecord()
                .setField("s", FieldType.STRING, "s2")
                .setField("t", FieldType.STRING, "1493197966585")
                .setField("v", FieldType.STRING, "http://page2"));

        records.add(new StandardRecord()
                .setField("s", FieldType.STRING, "s1")
                .setField("t", FieldType.STRING, "1493197966590")
                .setField("v", FieldType.STRING, "http://page3"));


        records.add(new StandardRecord()
                .setField("s", FieldType.STRING, "s1")
                .setField("t", FieldType.STRING, "1493197966600")
                .setField("v", FieldType.STRING, "http://pageX"));


        records.add(new StandardRecord()
                .setField("s", FieldType.STRING, "s5")
                .setField("t", FieldType.STRING, "1493197966584")
                .setField("v", FieldType.STRING, "http://pageY"));

        TestRunner testRunner = TestRunners.newTestRunner(new ConsolidateSession());
        testRunner.setProperty(ConsolidateSession.SESSION_ID_FIELD, "s");
        testRunner.setProperty(ConsolidateSession.TIMESTAMP_FIELD, "t");
        testRunner.setProperty(ConsolidateSession.VISITED_PAGE_FIELD, "v");
        testRunner.assertValid();
        testRunner.enqueue(records);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(3);
        testRunner.assertOutputErrorCount(0);
    }

    @Test
    public void testSessionInactive()
    {
        Collection<Record> records = new ArrayList<>();
        TestRunner testRunner = TestRunners.newTestRunner(new ConsolidateSession());

        long now = System.currentTimeMillis();
        long sessionInactivityTimeout = 180; // in seconds
        long margin = 60000; // 60 seconds
        long event3_timestamp = now - sessionInactivityTimeout*1000 - margin;
        long event2_timestamp = event3_timestamp - margin;
        long event1_timestamp = event2_timestamp - margin;

        records.add(new StandardRecord()
                .setField("s", FieldType.STRING, "s1")
                .setField("t", FieldType.STRING, String.valueOf(event1_timestamp))
                .setField("v", FieldType.STRING, "http://page1"));

        records.add(new StandardRecord()
                .setField("s", FieldType.STRING, "s2")
                .setField("t", FieldType.STRING, "1493197966585")
                .setField("v", FieldType.STRING, "http://page2"));

        records.add(new StandardRecord()
                .setField("s", FieldType.STRING, "s1")
                .setField("t", FieldType.STRING, String.valueOf(event2_timestamp))
                .setField("v", FieldType.STRING, "http://page3"));

        records.add(new StandardRecord()
                .setField("s", FieldType.STRING, "s1")
                .setField("t", FieldType.STRING, String.valueOf(event3_timestamp))
                .setField("v", FieldType.STRING, "http://pageX"));


        records.add(new StandardRecord()
                .setField("s", FieldType.STRING, "s5")
                .setField("t", FieldType.STRING, "1493197966584")
                .setField("v", FieldType.STRING, "http://pageY"));

        testRunner.setProperty(ConsolidateSession.SESSION_ID_FIELD, "s");
        testRunner.setProperty(ConsolidateSession.TIMESTAMP_FIELD, "t");
        testRunner.setProperty(ConsolidateSession.VISITED_PAGE_FIELD, "v");
        testRunner.setProperty(ConsolidateSession.SESSION_INACTIVITY_TIMEOUT, String.valueOf(sessionInactivityTimeout));
        testRunner.setProperty(ConsolidateSession.IS_SESSION_ACTIVE_FIELD, "i");
        testRunner.assertValid();
        testRunner.enqueue(records);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(3);
        testRunner.assertOutputErrorCount(0);
        List<MockRecord> outputRecords=testRunner.getOutputRecords();
        try {
            MockRecord cs = outputRecords
                    .stream()
                    .filter(p -> (p.getField("s").asString()).equals("s1"))
                    .findFirst()
                    .get();

            Assert.assertFalse(cs.getField("i").asBoolean());
        }
        catch (Exception e) {
            Assert.assertFalse(false);
        }
    }


    @Test
    public void testSessionActive()
    {
        Collection<Record> records = new ArrayList<>();
        TestRunner testRunner = TestRunners.newTestRunner(new ConsolidateSession());

        long now = System.currentTimeMillis();
        long sessionInactivityTimeout = 180; // in seconds
        long margin = 60000; // 60 seconds
        long event3_timestamp = now - sessionInactivityTimeout*1000 + margin;
        long event2_timestamp = event3_timestamp - margin;
        long event1_timestamp = event2_timestamp - margin;

        records.add(new StandardRecord()
                .setField("s", FieldType.STRING, "s1")
                .setField("t", FieldType.STRING, String.valueOf(event1_timestamp))
                .setField("v", FieldType.STRING, "http://page1"));

        records.add(new StandardRecord()
                .setField("s", FieldType.STRING, "s2")
                .setField("t", FieldType.STRING, "1493197966585")
                .setField("v", FieldType.STRING, "http://page2"));

        records.add(new StandardRecord()
                .setField("s", FieldType.STRING, "s1")
                .setField("t", FieldType.STRING, String.valueOf(event2_timestamp))
                .setField("v", FieldType.STRING, "http://page3"));

        records.add(new StandardRecord()
                .setField("s", FieldType.STRING, "s1")
                .setField("t", FieldType.STRING, String.valueOf(event3_timestamp))
                .setField("v", FieldType.STRING, "http://pageX"));


        records.add(new StandardRecord()
                .setField("s", FieldType.STRING, "s5")
                .setField("t", FieldType.STRING, "1493197966584")
                .setField("v", FieldType.STRING, "http://pageY"));

        testRunner.setProperty(ConsolidateSession.SESSION_ID_FIELD, "s");
        testRunner.setProperty(ConsolidateSession.TIMESTAMP_FIELD, "t");
        testRunner.setProperty(ConsolidateSession.VISITED_PAGE_FIELD, "v");
        testRunner.setProperty(ConsolidateSession.SESSION_INACTIVITY_TIMEOUT, String.valueOf(sessionInactivityTimeout));
        testRunner.setProperty(ConsolidateSession.IS_SESSION_ACTIVE_FIELD, "i");
        testRunner.assertValid();
        testRunner.enqueue(records);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(3);
        testRunner.assertOutputErrorCount(0);
        List<MockRecord> outputRecords=testRunner.getOutputRecords();
        try {
            MockRecord cs = outputRecords
                    .stream()
                    .filter(p -> (p.getField("s").asString()).equals("s1"))
                    .findFirst()
                    .get();

            Assert.assertTrue(cs.getField("i").asBoolean());
        }
        catch (Exception e) {
            Assert.assertFalse(false);
        }
    }


    @Test
    public void testFirstVisitedPage()
    {
        Collection<Record> records = new ArrayList<>();

        records.add(new StandardRecord()
                .setField("s", FieldType.STRING, "s1")
                .setField("t", FieldType.STRING, "1493197966584")
                .setField("v", FieldType.STRING, "http://page1"));

        records.add(new StandardRecord()
                .setField("s", FieldType.STRING, "s2")
                .setField("t", FieldType.STRING, "1493197966585")
                .setField("v", FieldType.STRING, "http://page2"));

        records.add(new StandardRecord()
                .setField("s", FieldType.STRING, "s1")
                .setField("t", FieldType.STRING, "1493197966590")
                .setField("v", FieldType.STRING, "http://page3"));


        records.add(new StandardRecord()
                .setField("s", FieldType.STRING, "s1")
                .setField("t", FieldType.STRING, "1493197966600")
                .setField("v", FieldType.STRING, "http://pageX"));


        records.add(new StandardRecord()
                .setField("s", FieldType.STRING, "s5")
                .setField("t", FieldType.STRING, "1493197966584")
                .setField("v", FieldType.STRING, "http://pageY"));

        TestRunner testRunner = TestRunners.newTestRunner(new ConsolidateSession());
        testRunner.setProperty(ConsolidateSession.SESSION_ID_FIELD, "s");
        testRunner.setProperty(ConsolidateSession.TIMESTAMP_FIELD, "t");
        testRunner.setProperty(ConsolidateSession.VISITED_PAGE_FIELD, "v");
        testRunner.assertValid();
        testRunner.enqueue(records);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(3);
        testRunner.assertOutputErrorCount(0);
        List<MockRecord> outputRecords=testRunner.getOutputRecords();
        MockRecord cs= outputRecords
                .stream()
                .filter(p->(p.getField("s").asString()).equals("s1"))
                .findFirst()
                .get();
        Assert.assertTrue((cs.getField(ConsolidateSession.FIRST_VISITED_PAGE_FIELD.getDefaultValue()).asString())
                .equals("http://page1"));
    }

    @Test
    public void testLastVisitedPage(){
        Collection<Record> records = new ArrayList<>();

        records.add(new StandardRecord()
                .setField("s", FieldType.STRING, "s1")
                .setField("t", FieldType.STRING, "1493197966584")
                .setField("v", FieldType.STRING, "http://page1"));

        records.add(new StandardRecord()
                .setField("s", FieldType.STRING, "s2")
                .setField("t", FieldType.STRING, "1493197966585")
                .setField("v", FieldType.STRING, "http://page2"));

        records.add(new StandardRecord()
                .setField("s", FieldType.STRING, "s1")
                .setField("t", FieldType.STRING, "1493197966590")
                .setField("v", FieldType.STRING, "http://page3"));


        records.add(new StandardRecord()
                .setField("s", FieldType.STRING, "s1")
                .setField("t", FieldType.STRING, "1493197966600")
                .setField("v", FieldType.STRING, "http://pageX"));


        records.add(new StandardRecord()
                .setField("s", FieldType.STRING, "s5")
                .setField("t", FieldType.STRING, "1493197966584")
                .setField("v", FieldType.STRING, "http://pageY"));

        TestRunner testRunner = TestRunners.newTestRunner(new ConsolidateSession());
        testRunner.setProperty(ConsolidateSession.SESSION_ID_FIELD, "s");
        testRunner.setProperty(ConsolidateSession.TIMESTAMP_FIELD, "t");
        testRunner.setProperty(ConsolidateSession.VISITED_PAGE_FIELD, "v");
        testRunner.assertValid();
        testRunner.enqueue(records);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(3);
        testRunner.assertOutputErrorCount(0);
        List<MockRecord> outputRecords=testRunner.getOutputRecords();
        MockRecord cs= outputRecords
                .stream()
                .filter(p->(p.getField("s").asString()).equals("s1"))
                .findFirst()
                .get();
        Assert.assertTrue((cs.getField(ConsolidateSession.LAST_VISITED_PAGE_FIELD.getDefaultValue()).asString())
                .equals("http://pageX"));
    }


    @Test
    public void testGrabUserIdField()
    {
        Collection<Record> records = new ArrayList<>();
        String authenticated_user = "authuser";

        records.add(new StandardRecord()
                .setField("s", FieldType.STRING, "s1")
                .setField("t", FieldType.STRING, "1493197966584")
                .setField("v", FieldType.STRING, "http://page1"));

        records.add(new StandardRecord()
                .setField("s", FieldType.STRING, "s2")
                .setField("t", FieldType.STRING, "1493197966585")
                .setField("v", FieldType.STRING, "http://page2"));

        records.add(new StandardRecord()
                .setField("s", FieldType.STRING, "s1")
                .setField("t", FieldType.STRING, "1493197966590")
                .setField("v", FieldType.STRING, "http://page3"));


        records.add(new StandardRecord()
                .setField("s", FieldType.STRING, "s1")
                .setField("t", FieldType.STRING, "1493197966600")
                .setField("v", FieldType.STRING, "http://pageX")
                .setField("u", FieldType.STRING, authenticated_user));


        records.add(new StandardRecord()
                .setField("s", FieldType.STRING, "s1")
                .setField("t", FieldType.STRING, "1493197967000")
                .setField("v", FieldType.STRING, "http://pageY"));

        TestRunner testRunner = TestRunners.newTestRunner(new ConsolidateSession());
        testRunner.setProperty(ConsolidateSession.SESSION_ID_FIELD, "s");
        testRunner.setProperty(ConsolidateSession.TIMESTAMP_FIELD, "t");
        testRunner.setProperty(ConsolidateSession.VISITED_PAGE_FIELD, "v");
        testRunner.setProperty(ConsolidateSession.USERID_FIELD, "u");
        testRunner.assertValid();
        testRunner.enqueue(records);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(2);
        testRunner.assertOutputErrorCount(0);
        List<MockRecord> outputRecords=testRunner.getOutputRecords();

        MockRecord cs= outputRecords
                .stream()
                .filter(p->(p.getField("s").asString()).equals("s1"))
                .findFirst()
                .get();

        Assert.assertTrue((cs.getField("u").asString())
                        .equals(authenticated_user));
    }

    @Test
    public void testIgnoreOneEventWithoutSessionId()
    {
        Collection<Record> records = new ArrayList<>();

        records.add(new StandardRecord()
                .setField("s", FieldType.STRING, "s1")
                .setField("t", FieldType.STRING, "1493197966584")
                .setField("v", FieldType.STRING, "http://page1"));

        records.add(new StandardRecord()
                .setField("s", FieldType.STRING, "s2")
                .setField("t", FieldType.STRING, "1493197966585")
                .setField("v", FieldType.STRING, "http://page2"));

        records.add(new StandardRecord()
                .setField("s", FieldType.STRING, "s3")
                .setField("t", FieldType.STRING, "1493197966590")
                .setField("v", FieldType.STRING, "http://page3"));


        records.add(new StandardRecord()
                .setField("s", FieldType.STRING, "s4")
                .setField("t", FieldType.STRING, "1493197966584")
                .setField("v", FieldType.STRING, "http://pageX"));


        records.add(new StandardRecord()
                .setField("Z", FieldType.STRING, "ZZZ")
                .setField("t", FieldType.STRING, "1493197966584")
                .setField("v", FieldType.STRING, "http://pageY"));

        TestRunner testRunner = TestRunners.newTestRunner(new ConsolidateSession());
        testRunner.setProperty(ConsolidateSession.SESSION_ID_FIELD, "s");
        testRunner.setProperty(ConsolidateSession.TIMESTAMP_FIELD, "t");
        testRunner.setProperty(ConsolidateSession.VISITED_PAGE_FIELD, "v");
        testRunner.assertValid();
        testRunner.enqueue(records);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(4);
        testRunner.assertOutputErrorCount(0);
    }


    @Test
    public void testIgnoreOneEventWithoutTimestamp()
    {
        Collection<Record> records = new ArrayList<>();

        records.add(new StandardRecord()
                .setField("s", FieldType.STRING, "s1")
                .setField("t", FieldType.STRING, "1493197966584")
                .setField("v", FieldType.STRING, "http://page1"));

        records.add(new StandardRecord()
                .setField("s", FieldType.STRING, "s2")
                .setField("t", FieldType.STRING, "1493197966585")
                .setField("v", FieldType.STRING, "http://page2"));

        records.add(new StandardRecord()
                .setField("s", FieldType.STRING, "s3")
                .setField("t", FieldType.STRING, "1493197966590")
                .setField("v", FieldType.STRING, "http://page3"));

        records.add(new StandardRecord()
                .setField("s", FieldType.STRING, "s4")
                //.setField("t", FieldType.STRING, "1493197966700")
                .setField("v", FieldType.STRING, "http://pageX"));

        TestRunner testRunner = TestRunners.newTestRunner(new ConsolidateSession());
        testRunner.setProperty(ConsolidateSession.SESSION_ID_FIELD, "s");
        testRunner.setProperty(ConsolidateSession.TIMESTAMP_FIELD, "t");
        testRunner.setProperty(ConsolidateSession.VISITED_PAGE_FIELD, "v");
        testRunner.assertValid();
        testRunner.enqueue(records);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(3);
        testRunner.assertOutputErrorCount(0);
    }

    @Test
    public void testGrabOneFieldPresentEveryWhere()
    {
        Collection<Record> records = new ArrayList<>();
        String partyId = "123456";

        records.add(new StandardRecord()
                .setField("s", FieldType.STRING, "s1")
                .setField("t", FieldType.STRING, "1493197966584")
                .setField("v", FieldType.STRING, "http://page3")
                .setField("f", FieldType.STRING, partyId));

        records.add(new StandardRecord()
                .setField("s", FieldType.STRING, "s2")
                .setField("t", FieldType.STRING, "1493197966585")
                .setField("v", FieldType.STRING, "http://page3")
                .setField("f", FieldType.STRING, "654321"));

        records.add(new StandardRecord()
                .setField("s", FieldType.STRING, "s1")
                .setField("t", FieldType.STRING, "1493197966590")
                .setField("v", FieldType.STRING, "http://page3")
                .setField("f", FieldType.STRING, partyId));


        records.add(new StandardRecord()
                .setField("s", FieldType.STRING, "s1")
                .setField("t", FieldType.STRING, "1493197966600")
                .setField("v", FieldType.STRING, "http://page3")
                .setField("f", FieldType.STRING, partyId));


        records.add(new StandardRecord()
                .setField("s", FieldType.STRING, "s1")
                .setField("t", FieldType.STRING, "1493197967000")
                .setField("v", FieldType.STRING, "http://page3")
                .setField("f", FieldType.STRING, partyId));

        TestRunner testRunner = TestRunners.newTestRunner(new ConsolidateSession());
        testRunner.setProperty(ConsolidateSession.SESSION_ID_FIELD, "s");
        testRunner.setProperty(ConsolidateSession.TIMESTAMP_FIELD, "t");
        testRunner.setProperty(ConsolidateSession.FIELDS_TO_RETURN, "f");
        testRunner.setProperty(ConsolidateSession.VISITED_PAGE_FIELD, "v");
        testRunner.assertValid();
        testRunner.enqueue(records);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(2);
        testRunner.assertOutputErrorCount(0);
        List<MockRecord> outputRecords=testRunner.getOutputRecords();

        MockRecord cs= outputRecords
                .stream()
                .filter(p->(p.getField("s").asString()).equals("s1"))
                .findFirst()
                .get();

        Assert.assertTrue((cs.getField("f").asString())
                .equals(partyId));
    }

    @Test
    public void testGrabSeveralFieldsPresent()
    {
        Collection<Record> records = new ArrayList<>();
        String partyId = "123456";
        String B2BUnit = "999999";

        records.add(new StandardRecord()
                .setField("s", FieldType.STRING, "s1")
                .setField("t", FieldType.STRING, "1493197966584")
                .setField("v", FieldType.STRING, "http://page3")
                .setField("f", FieldType.STRING, partyId));

        records.add(new StandardRecord()
                .setField("s", FieldType.STRING, "s2")
                .setField("t", FieldType.STRING, "1493197966585")
                .setField("v", FieldType.STRING, "http://page3")
                .setField("f", FieldType.STRING, "654321"));

        records.add(new StandardRecord()
                .setField("s", FieldType.STRING, "s1")
                .setField("t", FieldType.STRING, "1493197966590")
                .setField("v", FieldType.STRING, "http://page3")
                .setField("f", FieldType.STRING, partyId));


        records.add(new StandardRecord()
                .setField("s", FieldType.STRING, "s1")
                .setField("t", FieldType.STRING, "1493197966600")
                .setField("v", FieldType.STRING, "http://page3")
                .setField("b", FieldType.STRING, B2BUnit)
                .setField("f", FieldType.STRING, partyId));


        records.add(new StandardRecord()
                .setField("s", FieldType.STRING, "s1")
                .setField("t", FieldType.STRING, "1493197967000")
                .setField("v", FieldType.STRING, "http://page3")
                .setField("f", FieldType.STRING, partyId));

        TestRunner testRunner = TestRunners.newTestRunner(new ConsolidateSession());
        testRunner.setProperty(ConsolidateSession.SESSION_ID_FIELD, "s");
        testRunner.setProperty(ConsolidateSession.TIMESTAMP_FIELD, "t");
        testRunner.setProperty(ConsolidateSession.FIELDS_TO_RETURN, "f,b");
        testRunner.setProperty(ConsolidateSession.VISITED_PAGE_FIELD, "v");
        testRunner.assertValid();
        testRunner.enqueue(records);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(2);
        testRunner.assertOutputErrorCount(0);
        List<MockRecord> outputRecords=testRunner.getOutputRecords();

        MockRecord cs= outputRecords
                .stream()
                .filter(p->(p.getField("s").asString()).equals("s1"))
                .findFirst()
                .get();

        Assert.assertTrue((cs.getField("f").asString())
                .equals(partyId));

        Assert.assertTrue((cs.getField("b").asString())
                .equals(B2BUnit));
    }

    @Test
    public void testCalculateSessionDuration()
    {
        Collection<Record> records = new ArrayList<>();
        TestRunner testRunner = TestRunners.newTestRunner(new ConsolidateSession());

        long now = System.currentTimeMillis();
        long sessionInactivityTimeout = 180; // in seconds
        long margin = 60000; // 60 seconds
        long event3_timestamp = now - sessionInactivityTimeout*1000 - margin;
        long event2_timestamp = event3_timestamp - margin;
        long event1_timestamp = event2_timestamp - margin;
        long sessionDuration = 2*margin/1000; // in seconds

        records.add(new StandardRecord()
                .setField("s", FieldType.STRING, "s1")
                .setField("t", FieldType.STRING, String.valueOf(event1_timestamp))
                .setField("v", FieldType.STRING, "http://page1"));

        records.add(new StandardRecord()
                .setField("s", FieldType.STRING, "s2")
                .setField("t", FieldType.STRING, "1493197966585")
                .setField("v", FieldType.STRING, "http://page2"));

        records.add(new StandardRecord()
                .setField("s", FieldType.STRING, "s1")
                .setField("t", FieldType.STRING, String.valueOf(event2_timestamp))
                .setField("v", FieldType.STRING, "http://page3"));

        records.add(new StandardRecord()
                .setField("s", FieldType.STRING, "s1")
                .setField("t", FieldType.STRING, String.valueOf(event3_timestamp))
                .setField("v", FieldType.STRING, "http://pageX"));


        records.add(new StandardRecord()
                .setField("s", FieldType.STRING, "s5")
                .setField("t", FieldType.STRING, "1493197966584")
                .setField("v", FieldType.STRING, "http://pageY"));

        testRunner.setProperty(ConsolidateSession.SESSION_ID_FIELD, "s");
        testRunner.setProperty(ConsolidateSession.TIMESTAMP_FIELD, "t");
        testRunner.setProperty(ConsolidateSession.VISITED_PAGE_FIELD, "v");
        testRunner.setProperty(ConsolidateSession.SESSION_INACTIVITY_TIMEOUT, String.valueOf(sessionInactivityTimeout));
        testRunner.setProperty(ConsolidateSession.IS_SESSION_ACTIVE_FIELD, "i");
        testRunner.assertValid();
        testRunner.enqueue(records);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(3);
        testRunner.assertOutputErrorCount(0);
        List<MockRecord> outputRecords=testRunner.getOutputRecords();
        try {
            MockRecord cs = outputRecords
                    .stream()
                    .filter(p -> (p.getField("s").asString()).equals("s1"))
                    .findFirst()
                    .get();

            Assert.assertEquals(
                    cs.getField(ConsolidateSession.SESSION_DURATION_FIELD.getDefaultValue()).asLong(),
                    new Long (sessionDuration));
        }
        catch (Exception e) {
            Assert.assertFalse(false);
        }
    }

    @Test
    public void testSessionInactivityDuration()
    {
        Collection<Record> records = new ArrayList<>();
        TestRunner testRunner = TestRunners.newTestRunner(new ConsolidateSession());

        long now = System.currentTimeMillis();
        long sessionInactivityTimeout = 180; // in seconds
        long margin = 60000; // 60 seconds
        // Make sure the session already timed out to check the computed
        //  sessionInactivityDuration is maxed out to the defined session timeout
        long event3_timestamp = now - sessionInactivityTimeout*1000 - margin;
        long event2_timestamp = event3_timestamp - margin;
        long event1_timestamp = event2_timestamp - margin;
        long sessionDuration = 2*margin/1000; // in seconds

        records.add(new StandardRecord()
                .setField("s", FieldType.STRING, "s1")
                .setField("t", FieldType.STRING, String.valueOf(event1_timestamp))
                .setField("v", FieldType.STRING, "http://page1"));

        records.add(new StandardRecord()
                .setField("s", FieldType.STRING, "s2")
                .setField("t", FieldType.STRING, "1493197966585")
                .setField("v", FieldType.STRING, "http://page2"));

        records.add(new StandardRecord()
                .setField("s", FieldType.STRING, "s1")
                .setField("t", FieldType.STRING, String.valueOf(event2_timestamp))
                .setField("v", FieldType.STRING, "http://page3"));

        records.add(new StandardRecord()
                .setField("s", FieldType.STRING, "s1")
                .setField("t", FieldType.STRING, String.valueOf(event3_timestamp))
                .setField("v", FieldType.STRING, "http://pageX"));


        records.add(new StandardRecord()
                .setField("s", FieldType.STRING, "s5")
                .setField("t", FieldType.STRING, "1493197966584")
                .setField("v", FieldType.STRING, "http://pageY"));

        testRunner.setProperty(ConsolidateSession.SESSION_ID_FIELD, "s");
        testRunner.setProperty(ConsolidateSession.TIMESTAMP_FIELD, "t");
        testRunner.setProperty(ConsolidateSession.VISITED_PAGE_FIELD, "v");
        testRunner.setProperty(ConsolidateSession.SESSION_INACTIVITY_TIMEOUT, String.valueOf(sessionInactivityTimeout));
        testRunner.setProperty(ConsolidateSession.IS_SESSION_ACTIVE_FIELD, "i");
        testRunner.assertValid();
        testRunner.enqueue(records);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(3);
        testRunner.assertOutputErrorCount(0);
        List<MockRecord> outputRecords=testRunner.getOutputRecords();
        try {
            MockRecord cs = outputRecords
                    .stream()
                    .filter(p -> (p.getField("s").asString()).equals("s1"))
                    .findFirst()
                    .get();

            Assert.assertEquals(
                    cs.getField(ConsolidateSession.SESSION_INACTIVITY_DURATION_FIELD.getDefaultValue()).asLong(),
                    new Long (sessionInactivityTimeout));
        }
        catch (Exception e) {
            Assert.assertFalse(false);
        }
    }
}
