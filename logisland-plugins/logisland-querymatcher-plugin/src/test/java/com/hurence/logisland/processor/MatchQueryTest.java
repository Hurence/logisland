/**
 * Copyright (C) 2016-2017 Hurence (support@hurence.com)
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
package com.hurence.logisland.processor;


import com.hurence.logisland.record.Field;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.util.runner.MockRecord;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class MatchQueryTest {

    private static String EXCEPTION_RECORD = "exception_record";

    private static Logger logger = LoggerFactory.getLogger(MatchQueryTest.class);


    @Test
    public void validateNoMatch() {
        final TestRunner testRunner = TestRunners.newTestRunner(new MatchQuery());
        testRunner.setProperty("too_many_attempts_exception", "exception:TooManyAttemptsException");
        testRunner.setProperty("some_message", "message:wrong");
        testRunner.assertValid();

        Record[] records = {
                new StandardRecord(EXCEPTION_RECORD)
                        .setId("id1")
                        .setStringField("exception", "NullPointerException")
                        .setStringField("message", "something good"),
                new StandardRecord(EXCEPTION_RECORD)
                        .setId("id2")
                        .setStringField("exception", "IllegalStateException")
                        .setStringField("message", "something messy"),
                new StandardRecord(EXCEPTION_RECORD)
                        .setId("id3")
                        .setStringField("exception", "BadBoyException")
                        .setStringField("message", "oh bad boy!")
        };
        testRunner.clearQueues();
        testRunner.enqueue(records);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(0);
    }
    @Test
    public void testSimpleMatch() {
        final TestRunner testRunner = TestRunners.newTestRunner(new MatchQuery());
        testRunner.setProperty("too_many_attempts_exception", "exception:TooManyAttemptsException");
        testRunner.setProperty("some_message", "message:wrong");
        testRunner.assertValid();

        Record[] records = {
                new StandardRecord(EXCEPTION_RECORD)
                        .setId("id1")
                        .setStringField("exception", "NullPointerException")
                        .setStringField("message", "something wrong"),
                new StandardRecord(EXCEPTION_RECORD)
                        .setId("id2")
                        .setStringField("exception", "IllegalStateException")
                        .setStringField("message", "something messy"),
                new StandardRecord(EXCEPTION_RECORD)
                        .setId("id3")
                        .setStringField("exception", "TooManyAttemptsException")
                        .setStringField("message", "oh bad boy!")
        };
        testRunner.clearQueues();
        testRunner.enqueue(records);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(2);
    }


    @Test
    public void testDotMatch() {
        final TestRunner testRunner = TestRunners.newTestRunner(new MatchQuery());
        testRunner.setProperty("too_many_attempts_exception", "exception:(+too +many +sushi)");
        testRunner.assertValid();

        Record[] records = {
                new StandardRecord(EXCEPTION_RECORD)
                        .setId("id1")
                        .setStringField("exception", "NullPointerException"),
                new StandardRecord(EXCEPTION_RECORD)
                        .setId("id2")
                        .setStringField("exception", "too.many.sushi"),
                new StandardRecord(EXCEPTION_RECORD)
                        .setId("id3")
                        .setStringField("exception", "too.many.attempts")
        };
        testRunner.clearQueues();
        testRunner.enqueue(records);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);
    }

    @Test
    public void testWildcardMatch() {
        final TestRunner testRunner = TestRunners.newTestRunner(new MatchQuery());
        testRunner.setProperty("some_message", "message:wrong");
        testRunner.assertValid();

        Record[] records = {
                new StandardRecord(EXCEPTION_RECORD)
                        .setId("id1")
                        .setStringField("message", "something.wrong"),
                new StandardRecord(EXCEPTION_RECORD)
                        .setId("id2")
                        .setStringField("message", "something.messy"),
                new StandardRecord(EXCEPTION_RECORD)
                        .setId("id3")
                        .setStringField("message", "oh bad boy!")
        };
        testRunner.clearQueues();
        testRunner.enqueue(records);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);
    }

    @Test
    public void testNumericRangeQuery() {
        final TestRunner testRunner = TestRunners.newTestRunner(new MatchQuery());
        testRunner.setProperty(MatchQuery.NUMERIC_FIELDS, "exception_count,average_bytes");
        testRunner.setProperty("too_many_exceptions", "exception_count:[346 TO 2000]");
        testRunner.setProperty("too_many_bytes", "average_bytes:[100 TO 50000]");
        testRunner.assertValid();

        Record[] records = {
                new StandardRecord(EXCEPTION_RECORD)
                        .setId("id1")
                        .setStringField("exception", "NullPointerException")
                        .setStringField("message", "something wrong")
                        .setField("average_bytes", FieldType.FLOAT, 12.4f)
                        .setField("exception_count", FieldType.INT, 345),
                new StandardRecord(EXCEPTION_RECORD)
                        .setId("id2")
                        .setStringField("exception", "IllegalStateException")
                        .setStringField("message", "something messy")
                        .setField("average_bytes", FieldType.FLOAT, 123.4f)
                        .setField("exception_count", FieldType.INT, 1345),
                new StandardRecord(EXCEPTION_RECORD)
                        .setId("id3")
                        .setStringField("exception", "TooManyAttemptsException")
                        .setStringField("message", "oh bad boy!")
                        .setField("average_bytes", FieldType.FLOAT, 3412.4f)
                        .setField("exception_count", FieldType.INT, 3450)
        };
        testRunner.clearQueues();
        testRunner.enqueue(records);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.getOutputRecords().forEach(System.out::println);
        testRunner.assertOutputRecordsCount(3);
    }

    /**
     * A bunch of records for testing purpose.
     */
    private static final Record[] RECORDS = {
            new StandardRecord(EXCEPTION_RECORD)
                    .setId("id1")
                    .setStringField("exception", "miss")
                    .setStringField("message", "miss"),
            new StandardRecord(EXCEPTION_RECORD)
                    .setId("id2")
                    .setStringField("exception", "miss")
                    .setStringField("message", "match"),
            new StandardRecord(EXCEPTION_RECORD)
                    .setId("id3")
                    .setStringField("exception", "match")
                    .setStringField("message", "miss"),
            new StandardRecord(EXCEPTION_RECORD)
                    .setId("id4")
                    .setStringField("exception", "match")
                    .setStringField("message", "match")
    };

    /**
     * Legacy behaviour.
     * Only matching records are not filtered out.
     * Records that match more than one query are duplicated.
     */
    @Test
    public void validateLegacy() {
        final TestRunner testRunner = TestRunners.newTestRunner(new MatchQuery());
        testRunner.setProperty("match_exception", "exception:match");
        testRunner.setProperty("match_message", "message:match");
        testRunner.assertValid();
        testRunner.clearQueues();
        testRunner.enqueue(RECORDS);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(4);

        List<MockRecord> records = testRunner.getOutputRecords();
        records.forEach(record -> {
            final String id = record.getId();
            final Field field = record.getField(MatchQuery.ALERT_MATCH_NAME);
            // Records without match should be ignored.
            Assert.assertNotNull(field);

            final Object _matchName = field.getRawValue();
            // One match name per record.
            Assert.assertEquals(_matchName.getClass(),
                                String.class);
            final String matchName = (String)_matchName;

            if ("id1".equals(id)) {
                // No match
                Assert.fail("Unexpected match for id1");
            }
            else if ("id2".equals(id)) {
                Assert.assertEquals(matchName,
                                    "match_message");
            }
            else if ("id3".equals(id)) {
                Assert.assertEquals(matchName,
                                    "match_exception");
            }
            else if ("id4".equals(id)) {
                Assert.assertTrue("Expected 'match_exception' or 'match_message'",
                                  "match_exception".equals(matchName)||"match_message".equals(matchName));
            }
        });
    }

    /**
     * No filtering (all records are sent out).
     * Records that match more than one query are duplicated.
     */
    @Test
    public void validateNoFiltering() {
        final TestRunner testRunner = TestRunners.newTestRunner(new MatchQuery());
        testRunner.setProperty(MatchQuery.ON_MISS_POLICY, MatchQuery.OnMissPolicy.forward.toString());
        testRunner.setProperty("match_exception", "exception:match");
        testRunner.setProperty("match_message", "message:match");
        testRunner.assertValid();
        testRunner.clearQueues();
        testRunner.enqueue(RECORDS);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        List<MockRecord> records = testRunner.getOutputRecords();
        testRunner.assertOutputRecordsCount(5); // All records + id4 duplicated.

        records.forEach(record -> {
            final String id = record.getId();

            final Field field = record.getField(MatchQuery.ALERT_MATCH_NAME);

            Object matchName = null;
            if (field!=null) {
                final Object _matchName = field.getRawValue();
                // One match name per record.
                Assert.assertEquals(_matchName.getClass(),
                                    String.class);
                matchName = (String) _matchName;
            }

            if ("id1".equals(id)) {
                // No match
                Assert.assertNull(field);
            }
            else if ("id2".equals(id)) {
                Assert.assertEquals(matchName,
                                    "match_message");
            }
            else if ("id3".equals(id)) {
                Assert.assertEquals(matchName,
                                    "match_exception");
            }
            else if ("id4".equals(id)) {
                Assert.assertTrue("Expected 'match_exception' or 'match_message'",
                                  "match_exception".equals(matchName)||"match_message".equals(matchName));
            }
        });
    }

    /**
     * Only matching records are not filtered out.
     * Records that match more than one query are NOT duplicated but the matching query information are
     * concatenated in record fields.
     */
    @Test
    public void validateFilteredConcat() {
        final TestRunner testRunner = TestRunners.newTestRunner(new MatchQuery());
        testRunner.setProperty(MatchQuery.ON_MATCH_POLICY, MatchQuery.OnMatchPolicy.all.toString());
        testRunner.setProperty("match_exception", "exception:match");
        testRunner.setProperty("match_message", "message:match");
        testRunner.assertValid();
        testRunner.clearQueues();
        testRunner.enqueue(RECORDS);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        List<MockRecord> records = testRunner.getOutputRecords();
        testRunner.assertOutputRecordsCount(3);

        records.forEach(record -> {
            final String id = record.getId();

            final Field field = record.getField(MatchQuery.ALERT_MATCH_NAME);

            String[] matchName = null;
            if (field!=null) {
                final Object _matchName = field.getRawValue();
                // One match name per record.
                Assert.assertEquals(_matchName.getClass(),
                                    String[].class);
                matchName = (String[]) _matchName;
            }

            if ("id1".equals(id)) {
                // No match
                Assert.fail("Unexpected match for id1");
            }
            else if ("id2".equals(id)) {
                Assert.assertEquals(matchName[0],
                                    "match_message");
            }
            else if ("id3".equals(id)) {
                Assert.assertEquals(matchName[0],
                                    "match_exception");
            }
            else if ("id4".equals(id)) {
                Assert.assertTrue("Expected 'match_exception' or 'match_message' but has "+matchName[0],
                                  "match_exception".equals(matchName[0])||"match_message".equals(matchName[0]));

                Assert.assertTrue("Expected 'match_exception' or 'match_message' but has "+matchName[1],
                                  "match_exception".equals(matchName[1])||"match_message".equals(matchName[1]));
            }
        });
    }

    /**
     * No filtering (all records are sent out).
     * Records that match more than one query are NOT duplicated but the matching query information are
     * concatenated in record fields.
     */
    @Test
    public void validateNoFilteringConcat() {
        final TestRunner testRunner = TestRunners.newTestRunner(new MatchQuery());
        testRunner.setProperty(MatchQuery.ON_MATCH_POLICY, MatchQuery.OnMatchPolicy.all.toString());
        testRunner.setProperty(MatchQuery.ON_MISS_POLICY, MatchQuery.OnMissPolicy.forward.toString());
        testRunner.setProperty("match_exception", "exception:match");
        testRunner.setProperty("match_message", "message:match");
        testRunner.assertValid();
        testRunner.clearQueues();
        testRunner.enqueue(RECORDS);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        List<MockRecord> records = testRunner.getOutputRecords();
        testRunner.assertOutputRecordsCount(4);

        records.forEach(record -> {
            final String id = record.getId();

            final Field field = record.getField(MatchQuery.ALERT_MATCH_NAME);

            String[] matchName = null;
            if (field!=null) {
                final Object _matchName = field.getRawValue();
                // One match name per record.
                Assert.assertEquals(_matchName.getClass(),
                                    String[].class);
                matchName = (String[]) _matchName;
            }

            if ("id1".equals(id)) {
                // No match
                Assert.assertNull(field);
            }
            else if ("id2".equals(id)) {
                Assert.assertEquals(matchName[0],
                                    "match_message");
            }
            else if ("id3".equals(id)) {
                Assert.assertEquals(matchName[0],
                                    "match_exception");
            }
            else if ("id4".equals(id)) {
                Assert.assertTrue("Expected 'match_exception' or 'match_message' but has "+matchName[0],
                                  "match_exception".equals(matchName[0])||"match_message".equals(matchName[0]));

                Assert.assertTrue("Expected 'match_exception' or 'match_message' but has "+matchName[1],
                                  "match_exception".equals(matchName[1])||"match_message".equals(matchName[1]));
            }
        });
    }

    /**
     * Queries do not match any record but as the onmiss.policy:forward, all non-matching records are forwarded anyway.
     */
    @Test
    public void validateNoMatchForward() {
        final TestRunner testRunner = TestRunners.newTestRunner(new MatchQuery());
        testRunner.setProperty(MatchQuery.ON_MISS_POLICY, "forward");
        testRunner.assertValid();
        testRunner.clearQueues();
        testRunner.enqueue(RECORDS);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(4);
    }

    /**
     * Queries match fields that contain 'good' (id1) or 'wrong' (id2) and all non-matching records are forwarded
     * anyway.
     */
    @Test
    public void validateOneMatchForward() {
        final TestRunner testRunner = TestRunners.newTestRunner(new MatchQuery());
        testRunner.setProperty(MatchQuery.ON_MISS_POLICY, "forward");
        testRunner.setProperty("too_many_attempts_exception", "exception:TooManyAttemptsException");
        testRunner.setProperty("some_message", "message:(good|wrong)");
        testRunner.assertValid();

        Record[] records = {
                new StandardRecord(EXCEPTION_RECORD)
                        .setId("id1")
                        .setStringField("exception", "NullPointerException")
                        .setStringField("message", "something good"),
                new StandardRecord(EXCEPTION_RECORD)
                        .setId("id2")
                        .setStringField("exception", "IllegalStateException")
                        .setStringField("message", "something wrong"),
                new StandardRecord(EXCEPTION_RECORD)
                        .setId("id3")
                        .setStringField("exception", "BadBoyException")
                        .setStringField("message", "oh bad boy!")
        };
        testRunner.clearQueues();
        testRunner.enqueue(records);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(3);
    }
}
