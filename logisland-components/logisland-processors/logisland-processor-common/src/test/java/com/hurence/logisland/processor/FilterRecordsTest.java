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
package com.hurence.logisland.processor;

import com.hurence.logisland.processor.util.BaseSyslogTest;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.util.runner.MockRecord;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;

public class FilterRecordsTest extends BaseSyslogTest {

    private static final Logger logger = LoggerFactory.getLogger(FilterRecordsTest.class);

    @Test
    public void testValidity() {
        TestRunner testRunner = TestRunners.newTestRunner(new FilterRecords());
        testRunner.assertNotValid();
        testRunner.setProperty(FilterRecords.FIELD_NAME, "a");
        testRunner.assertNotValid();
        testRunner.setProperty(FilterRecords.FIELD_VALUE, "a4");
        testRunner.assertValid();
        testRunner.removeProperty(FilterRecords.FIELD_NAME);
        testRunner.assertNotValid();
        testRunner.removeProperty(FilterRecords.FIELD_VALUE);
        testRunner.setProperty("dynamicprop", "Hi there !");
        testRunner.assertValid();
        testRunner.setProperty(FilterRecords.FIELD_NAME, "a");
        testRunner.setProperty(FilterRecords.FIELD_VALUE, "a4");
        testRunner.assertValid();
        testRunner.setProperty(FilterRecords.LOGIC, "or");
        testRunner.assertNotValid();
        testRunner.setProperty(FilterRecords.LOGIC, "OR");
        testRunner.assertValid();
        testRunner.setProperty(FilterRecords.LOGIC, "Or");
        testRunner.assertNotValid();
        testRunner.setProperty(FilterRecords.LOGIC, "Ore");
        testRunner.assertNotValid();
    }

    @Test
    public void testNothingRemaining() {

        Collection<Record> records = new ArrayList<>();


        records.add(new StandardRecord()
                .setField("a", FieldType.STRING, "a1")
                .setField("b", FieldType.STRING, "b1")
                .setField("c", FieldType.LONG, 1));

        records.add(new StandardRecord()
                .setField("a", FieldType.STRING, "a2")
                .setField("b", FieldType.STRING, "b2")
                .setField("c", FieldType.LONG, 2));

        records.add(new StandardRecord()
                .setField("a", FieldType.STRING, "a3")
                .setField("b", FieldType.STRING, "b3")
                .setField("c", FieldType.LONG, 3));


        TestRunner testRunner = TestRunners.newTestRunner(new FilterRecords());
        testRunner.setProperty(FilterRecords.FIELD_NAME, "a");
        testRunner.setProperty(FilterRecords.FIELD_VALUE, "a4");
        testRunner.assertValid();
        testRunner.enqueue(records);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(0);
        testRunner.assertOutputErrorCount(0);
    }

    @Test
    public void testRemoveOneRecord() {

        Collection<Record> records = new ArrayList<>();


        records.add(new StandardRecord()
                .setField("a", FieldType.STRING, "a1")
                .setField("b", FieldType.STRING, "b1")
                .setField("c", FieldType.LONG, 1));

        records.add(new StandardRecord()
                .setField("a", FieldType.STRING, "a1")
                .setField("b", FieldType.STRING, "b2")
                .setField("c", FieldType.LONG, 2));

        records.add(new StandardRecord()
                .setField("a", FieldType.STRING, "a1")
                .setField("b", FieldType.STRING, "b3")
                .setField("c", FieldType.LONG, 3));

        records.add(new StandardRecord()
                .setField("a", FieldType.STRING, "a2")
                .setField("b", FieldType.STRING, "b2")
                .setField("c", FieldType.LONG, 2));

        TestRunner testRunner = TestRunners.newTestRunner(new FilterRecords());
        testRunner.setProperty(FilterRecords.FIELD_NAME, "a");
        testRunner.setProperty(FilterRecords.FIELD_VALUE, "a1");
        testRunner.assertValid();
        testRunner.enqueue(records);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(3);
        testRunner.assertOutputErrorCount(0);

    }


    @Test
    public void testRemoveTwoRecord() {

        Collection<Record> records = new ArrayList<>();


        records.add(new StandardRecord()
                .setField("a", FieldType.STRING, "a1")
                .setField("b", FieldType.STRING, "b1")
                .setField("c", FieldType.LONG, 1));

        records.add(new StandardRecord()
                .setField("a", FieldType.STRING, "a1")
                .setField("b", FieldType.STRING, "b2")
                .setField("c", FieldType.LONG, 2));

        records.add(new StandardRecord()
                .setField("a", FieldType.STRING, "a2")
                .setField("b", FieldType.STRING, "b2")
                .setField("c", FieldType.LONG, 2));


        records.add(new StandardRecord()
                .setField("a", FieldType.STRING, "a2")
                .setField("b", FieldType.STRING, "b2")
                .setField("c", FieldType.LONG, 2));

        TestRunner testRunner = TestRunners.newTestRunner(new FilterRecords());
        testRunner.setProperty(FilterRecords.FIELD_NAME, "a");
        testRunner.setProperty(FilterRecords.FIELD_VALUE, "a1");
        testRunner.assertValid();
        testRunner.enqueue(records);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(2);
        testRunner.assertOutputErrorCount(0);

    }

    @Test
    public void testRemoveOneLongRecord() {

        Collection<Record> records = new ArrayList<>();


        records.add(new StandardRecord()
                .setField("a", FieldType.STRING, "a1")
                .setField("b", FieldType.STRING, "b1")
                .setField("c", FieldType.LONG, 1));

        records.add(new StandardRecord()
                .setField("a", FieldType.STRING, "a1")
                .setField("b", FieldType.STRING, "b2")
                .setField("c", FieldType.LONG, 2));

        records.add(new StandardRecord()
                .setField("a", FieldType.STRING, "a2")
                .setField("b", FieldType.STRING, "b2")
                .setField("c", FieldType.LONG, 2));


        records.add(new StandardRecord()
                .setField("a", FieldType.STRING, "a2")
                .setField("b", FieldType.STRING, "b2")
                .setField("c", FieldType.LONG, 2));

        TestRunner testRunner = TestRunners.newTestRunner(new FilterRecords());
        testRunner.setProperty(FilterRecords.FIELD_NAME, "c");
        testRunner.setProperty(FilterRecords.FIELD_VALUE, "2");
        testRunner.assertValid();
        testRunner.enqueue(records);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(3);
        testRunner.assertOutputErrorCount(0);

    }

    /*
    Added to ensure this works with OR logic (had a bug about that)
     */
    @Test
    public void testRemoveOneLongRecord_2() {

        Collection<Record> records = new ArrayList<>();


        records.add(new StandardRecord()
                .setField("a", FieldType.STRING, "a1")
                .setField("b", FieldType.STRING, "b1")
                .setField("c", FieldType.LONG, 1));

        records.add(new StandardRecord()
                .setField("a", FieldType.STRING, "a1")
                .setField("b", FieldType.STRING, "b2")
                .setField("c", FieldType.LONG, 2));

        records.add(new StandardRecord()
                .setField("a", FieldType.STRING, "a2")
                .setField("b", FieldType.STRING, "b2")
                .setField("c", FieldType.LONG, 2));


        records.add(new StandardRecord()
                .setField("a", FieldType.STRING, "a2")
                .setField("b", FieldType.STRING, "b2")
                .setField("c", FieldType.LONG, 2));

        TestRunner testRunner = TestRunners.newTestRunner(new FilterRecords());
        testRunner.setProperty(FilterRecords.LOGIC, "OR");
        testRunner.setProperty(FilterRecords.FIELD_NAME, "c");
        testRunner.setProperty(FilterRecords.FIELD_VALUE, "2");
        testRunner.assertValid();
        testRunner.enqueue(records);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(3);
        testRunner.assertOutputErrorCount(0);

    }

    @Test
    public void testRemoveNoRecordNonExistingField() {

        Collection<Record> records = new ArrayList<>();


        records.add(new StandardRecord()
                .setField("a", FieldType.STRING, "a1")
                .setField("b", FieldType.STRING, "b1")
                .setField("c", FieldType.LONG, 1));

        records.add(new StandardRecord()
                .setField("a", FieldType.STRING, "a2")
                .setField("b", FieldType.STRING, "b2")
                .setField("c", FieldType.LONG, 2));

        records.add(new StandardRecord()
                .setField("a", FieldType.STRING, "a2")
                .setField("b", FieldType.STRING, "b2")
                .setField("c", FieldType.LONG, 2));


        records.add(new StandardRecord()
                .setField("a", FieldType.STRING, "a2")
                .setField("b", FieldType.STRING, "b2")
                .setField("c", FieldType.LONG, 2));

        TestRunner testRunner = TestRunners.newTestRunner(new FilterRecords());
        testRunner.setProperty(FilterRecords.FIELD_NAME, "d");
        testRunner.setProperty(FilterRecords.FIELD_VALUE, "a1");
        testRunner.assertValid();
        testRunner.enqueue(records);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(0);
        testRunner.assertOutputErrorCount(0);

    }


    @Test
    public void testComplexMethodsWithExpressionLanguage() {
        Record record1 = new StandardRecord();
        record1.setField("alphabet", FieldType.STRING, "abcdefg");
        record1.setField("age", FieldType.INT, 18);
        record1.setField("hello", FieldType.STRING, "Hello World !!!");
        Record record2 = new MockRecord(record1);
        record2.setField("age", FieldType.INT, 25);
        Record record3 = new MockRecord(record1);
        record3.setField("age", FieldType.INT, 8);

        TestRunner testRunner = TestRunners.newTestRunner(new FilterRecords());
        testRunner.setProperty("age_older_than_18",
                "${return age > 18}");
        testRunner.assertValid();
        testRunner.enqueue(record1, record2, record3);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertFieldEquals("age", 25);
    }


    @Test
    public void testComplexMethodsWithExpressionLanguage_2() {
        Record record1 = new StandardRecord();
        record1.setField("alphabet", FieldType.STRING, "abcdefg");
        record1.setField("age", FieldType.INT, 18);
        record1.setField("hello", FieldType.STRING, "Hello World !!!");
        Record record2 = new MockRecord(record1);
        record2.setField("age", FieldType.INT, 25);
        Record record3 = new MockRecord(record1);
        record3.setField("age", FieldType.INT, 8);
        Record record4 = new MockRecord(record2);
        record4.setField("alphabet", FieldType.STRING, "zrop");

        TestRunner testRunner = TestRunners.newTestRunner(new FilterRecords());
        testRunner.setProperty(FilterRecords.FIELD_NAME, "alphabet");
        testRunner.setProperty(FilterRecords.FIELD_VALUE, "abcdefg");
        testRunner.setProperty("age_older_than_18",
                "${return age > 18}");
        testRunner.assertValid();
        testRunner.enqueue(record1, record2, record3, record4);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertFieldEquals("age", 25);
    }

    @Test
    public void testComplexMethodsWithExpressionLanguageAndLogicOR() {
        Record record1 = new StandardRecord();
        record1.setField("alphabet", FieldType.STRING, "abcdefg");
        record1.setField("age", FieldType.INT, 18);
        record1.setField("hello", FieldType.STRING, "Hello World !!!");
        Record record2 = new MockRecord(record1);
        record2.setField("age", FieldType.INT, 25);
        Record record3 = new MockRecord(record1);
        record3.setField("age", FieldType.INT, 8);
        Record record4 = new MockRecord(record3);
        record4.setField("alphabet", FieldType.STRING, "zrop");

        TestRunner testRunner = TestRunners.newTestRunner(new FilterRecords());
        testRunner.setProperty(FilterRecords.LOGIC, "OR");
        testRunner.setProperty("age_older_than_18",
                "${return age > 18}");
        testRunner.setProperty("start_with_zr",
                "${return alphabet.startsWith(\"zr\")}");
        testRunner.assertValid();
        testRunner.enqueue(record1, record2, record3, record4);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(2);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertFieldEquals("age", 25);

        MockRecord out2 = testRunner.getOutputRecords().get(1);
        out2.assertFieldEquals("alphabet", "zrop");
    }

    @Test
    public void testComplexMethodsWithExpressionLanguageAndLogicAND() {
        Record record1 = new StandardRecord();
        record1.setField("alphabet", FieldType.STRING, "abcdefg");
        record1.setField("age", FieldType.INT, 18);
        record1.setField("hello", FieldType.STRING, "Hello World !!!");
        Record record2 = new MockRecord(record1);
        record2.setField("age", FieldType.INT, 25);
        Record record3 = new MockRecord(record1);
        record3.setField("age", FieldType.INT, 8);
        Record record4 = new MockRecord(record3);
        record4.setField("alphabet", FieldType.STRING, "zrop");

        TestRunner testRunner = TestRunners.newTestRunner(new FilterRecords());
        testRunner.setProperty(FilterRecords.LOGIC, "AND");
        testRunner.setProperty("age_older_than_18",
                "${return age > 18}");
        testRunner.setProperty("start_with_zr",
                "${return alphabet.startsWith(\"zr\")}");
        testRunner.assertValid();
        testRunner.enqueue(record1, record2, record3, record4);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(0);
    }

    @Test
    public void testHandleOfRuntimeError() {
        Record record1 = new StandardRecord();
        record1.setField("alphabet", FieldType.STRING, "abcdefg");
        record1.setField("age", FieldType.INT, 18);
        record1.setField("hello", FieldType.STRING, "Hello World !!!");
        Record record2 = new MockRecord(record1);
        record2.setField("age", FieldType.INT, 25);
        Record record3 = new MockRecord(record1);
        record3.setField("age", FieldType.INT, 8);
        Record record4 = new MockRecord(record3);
        record4.setField("alphabet", FieldType.STRING, "zrop");

        TestRunner testRunner = TestRunners.newTestRunner(new FilterRecords());
        testRunner.setProperty("npe_at_runtime",
                "${return age.contains(\"a\")}");
        testRunner.assertValid();
        testRunner.enqueue(record1, record2, record3, record4);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsIncludingErrorsCount(4);
        testRunner.assertOutputRecordsCount(0);
        testRunner.assertOutputErrorCount(4);
    }

    @Test
    public void testHandleOfRuntimeError_2() {
        Record record1 = new StandardRecord();
        record1.setField("alphabet", FieldType.STRING, "abcdefg");
        record1.setField("age", FieldType.INT, 18);
        record1.setField("hello", FieldType.STRING, "Hello World !!!");
        Record record2 = new MockRecord(record1);
        record2.setField("age", FieldType.INT, 25);
        Record record3 = new MockRecord(record1);
        record3.setField("age", FieldType.INT, 8);
        Record record4 = new MockRecord(record3);
        record4.setField("alphabet", FieldType.STRING, "zrop");

        TestRunner testRunner = TestRunners.newTestRunner(new FilterRecords());
        testRunner.setProperty(FilterRecords.KEEP_ERRORS, "false");
        testRunner.setProperty("npe_at_runtime",
                "${return age.contains(\"a\")}");
        testRunner.assertValid();
        testRunner.enqueue(record1, record2, record3, record4);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsIncludingErrorsCount(0);
        testRunner.assertOutputRecordsCount(0);
        testRunner.assertOutputErrorCount(0);
    }
}
