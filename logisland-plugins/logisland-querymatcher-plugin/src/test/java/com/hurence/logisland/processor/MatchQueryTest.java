/**
 * Copyright (C) 2016 Hurence (bailet.thomas@gmail.com)
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
package com.hurence.logisland.processor;


import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MatchQueryTest {

    static String docspath = "data/documents/frenchpress";
    static String rulespath = "data/rules";
    static String EXCEPTION_RECORD = "exception_record";

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

}
