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

import com.hurence.logisland.processor.util.BaseSyslogTest;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
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


}
