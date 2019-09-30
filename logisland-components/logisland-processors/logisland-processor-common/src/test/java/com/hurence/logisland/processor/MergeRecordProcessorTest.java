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

import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.util.runner.MockRecord;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class MergeRecordProcessorTest {

    private static final Logger logger = LoggerFactory.getLogger(MergeRecordProcessorTest.class);


    @Test
    public void testDefaultMerge() {
        Record record1 = new StandardRecord();
        record1.setField("string1", FieldType.STRING, "value1");
        record1.setField("string2", FieldType.STRING, "value2");
        record1.setField("long1", FieldType.LONG, 1L);
        record1.setField("long2", FieldType.LONG, 2L);
        Record record2 = new StandardRecord();
        record2.setField("string1", FieldType.FLOAT, 1f);
        record2.setField("string2", FieldType.FLOAT, 2f);
        record2.setField("long1", FieldType.INT, 3);
        record2.setField("long2", FieldType.INT, 4);
        Record record3 = new StandardRecord();
        record3.setField("string1", FieldType.FLOAT, 2f);
        record3.setField("string2", FieldType.FLOAT, 2f);
        record3.setField("long1", FieldType.INT, 5);
        record3.setField("long2", FieldType.INT, 6);
        Record record4 = new StandardRecord();
        record4.setField("string1", FieldType.FLOAT, 3f);
        record4.setField("string2", FieldType.FLOAT, 2f);
        record4.setField("long1", FieldType.INT, 7);
        record4.setField("long2", FieldType.INT, 8);

        TestRunner testRunner = TestRunners.newTestRunner(new MergeRecordProcessor());
        testRunner.setProcessorIdentifier("testProc");
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.enqueue(record2);
        testRunner.enqueue(record3);
        testRunner.enqueue(record4);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(4);
        out.assertFieldEquals("string1", "value1");
        out.assertFieldTypeEquals("string1", FieldType.STRING);
        out.assertFieldEquals("string2", "value2");
        out.assertFieldTypeEquals("string2", FieldType.STRING);
        out.assertFieldEquals("long1", 1L);
        out.assertFieldTypeEquals("long1", FieldType.LONG);
        out.assertFieldEquals("long2", 2L);
        out.assertFieldTypeEquals("long2", FieldType.LONG);
    }


    @Test
    public void testTakeArrayMerge() {
        Record record1 = new StandardRecord();
        record1.setField("string1", FieldType.STRING, "value1");
        record1.setField("string2", FieldType.STRING, "value2");
        record1.setField("long1", FieldType.LONG, 1L);
        record1.setField("long2", FieldType.LONG, 2L);
        Record record2 = new StandardRecord();
        record2.setField("string1", FieldType.FLOAT, 1f);
        record2.setField("string2", FieldType.FLOAT, 2f);
        record2.setField("long1", FieldType.INT, 3);
        record2.setField("long2", FieldType.INT, 4);
        Record record3 = new StandardRecord();
        record3.setField("string1", FieldType.FLOAT, 2f);
        record3.setField("string2", FieldType.FLOAT, 2f);
        record3.setField("long1", FieldType.INT, 5);
        record3.setField("long2", FieldType.INT, 6);
        Record record4 = new StandardRecord();
        record4.setField("string1", FieldType.FLOAT, 3f);
        record4.setField("string2", FieldType.FLOAT, 2f);
        record4.setField("long1", FieldType.INT, 7);
        record4.setField("long2", FieldType.INT, 8);

        TestRunner testRunner = TestRunners.newTestRunner(new MergeRecordProcessor());
        testRunner.setProcessorIdentifier("testProc");
        testRunner.setProperty("longs1", "long1");
        testRunner.setProperty("longs1.strategy", "array");
        testRunner.setProperty("longMerged", "long2");
        testRunner.setProperty("strings1", "string1");
        testRunner.setProperty("strings1.strategy", "array");
        testRunner.setProperty("stringMerged", "string2");
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.enqueue(record2);
        testRunner.enqueue(record3);
        testRunner.enqueue(record4);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(8);
        out.assertFieldEquals("string1", "value1");
        out.assertFieldTypeEquals("string1", FieldType.STRING);
        out.assertFieldEquals("string2", "value2");
        out.assertFieldTypeEquals("string2", FieldType.STRING);
        out.assertFieldEquals("long1", 1L);
        out.assertFieldTypeEquals("long1", FieldType.LONG);
        out.assertFieldEquals("long2", 2L);
        out.assertFieldTypeEquals("long2", FieldType.LONG);
        //merged fields
        out.assertFieldEquals("strings1", Arrays.asList("value1", 1f ,2f ,3f));
        out.assertFieldTypeEquals("strings1", FieldType.ARRAY);
        out.assertFieldEquals("stringMerged", "value2");
        out.assertFieldTypeEquals("stringMerged", FieldType.STRING);
        out.assertFieldEquals("longs1", Arrays.asList(1L, 3 ,5 ,7));
        out.assertFieldTypeEquals("longs1", FieldType.ARRAY);
        out.assertFieldEquals("longMerged", 2L);
        out.assertFieldTypeEquals("longMerged", FieldType.LONG);
    }

    @Test
    public void testSortedMerge() {
        Record record1 = new StandardRecord();
        record1.setField("string1", FieldType.STRING, "value1");
        record1.setField("string2", FieldType.STRING, "value2");
        record1.setField("long1", FieldType.LONG, 1L);
        record1.setField("long2", FieldType.LONG, 2L);
        Record record2 = new StandardRecord();
        record2.setField("string1", FieldType.FLOAT, 1f);
        record2.setField("string2", FieldType.FLOAT, 2f);
        record2.setField("long1", FieldType.INT, 3);
        record2.setField("long2", FieldType.INT, 4);
        Record record3 = new StandardRecord();
        record3.setField("string1", FieldType.FLOAT, 2f);
        record3.setField("string2", FieldType.FLOAT, 2f);
        record3.setField("long1", FieldType.INT, 5);
        record3.setField("long2", FieldType.INT, 6);
        Record record4 = new StandardRecord();
        record4.setField("string1", FieldType.FLOAT, 3f);
        record4.setField("string2", FieldType.FLOAT, 2f);
        record4.setField("long1", FieldType.INT, 7);
        record4.setField("long2", FieldType.INT, 8);

        TestRunner testRunner = TestRunners.newTestRunner(new MergeRecordProcessor());
        testRunner.setProcessorIdentifier("testProc");
        testRunner.setProperty("longs1", "long1");
        testRunner.setProperty("longs1.strategy", "array");
        testRunner.setProperty(MergeRecordProcessor.SORT_BY_FIELD, "long1");
        testRunner.setProperty(MergeRecordProcessor.REVERSE_SORTING, "false");
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.enqueue(record2);
        testRunner.enqueue(record3);
        testRunner.enqueue(record4);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(5);
        out.assertFieldEquals("string1", "value1");
        out.assertFieldTypeEquals("string1", FieldType.STRING);
        out.assertFieldEquals("string2", "value2");
        out.assertFieldTypeEquals("string2", FieldType.STRING);
        out.assertFieldEquals("long1", 1L);
        out.assertFieldTypeEquals("long1", FieldType.LONG);
        out.assertFieldEquals("long2", 2L);
        out.assertFieldTypeEquals("long2", FieldType.LONG);
        //merged fields
        out.assertFieldEquals("longs1", Arrays.asList(1L, 3, 5, 7));
//        out.assertFieldEquals("longs1", Arrays.asList(7, 5, 6, 1L));
        out.assertFieldTypeEquals("longs1", FieldType.ARRAY);
    }


    @Test
    public void testReverseSortedMerge() {
        Record record1 = new StandardRecord();
        record1.setField("string1", FieldType.STRING, "value1");
        record1.setField("string2", FieldType.STRING, "value2");
        record1.setField("long1", FieldType.LONG, 1L);
        record1.setField("long2", FieldType.LONG, 2L);
        Record record2 = new StandardRecord();
        record2.setField("string1", FieldType.FLOAT, 1f);
        record2.setField("string2", FieldType.FLOAT, 2f);
        record2.setField("long1", FieldType.INT, 3);
        record2.setField("long2", FieldType.INT, 4);
        Record record3 = new StandardRecord();
        record3.setField("string1", FieldType.FLOAT, 2f);
        record3.setField("string2", FieldType.FLOAT, 2f);
        record3.setField("long1", FieldType.INT, 5);
        record3.setField("long2", FieldType.INT, 6);
        Record record4 = new StandardRecord();
        record4.setField("string1", FieldType.FLOAT, 3f);
        record4.setField("string2", FieldType.FLOAT, 2f);
        record4.setField("long1", FieldType.INT, 7);
        record4.setField("long2", FieldType.INT, 8);

        TestRunner testRunner = TestRunners.newTestRunner(new MergeRecordProcessor());
        testRunner.setProcessorIdentifier("testProc");
        testRunner.setProperty("longs1", "long1");
        testRunner.setProperty("longs1.strategy", "array");
        testRunner.setProperty(MergeRecordProcessor.SORT_BY_FIELD, "long1");
        testRunner.setProperty(MergeRecordProcessor.REVERSE_SORTING, "true");
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.enqueue(record2);
        testRunner.enqueue(record3);
        testRunner.enqueue(record4);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(5);
        out.assertFieldEquals("string1", 3f);
        out.assertFieldTypeEquals("string1", FieldType.FLOAT);
        out.assertFieldEquals("string2", 2f);
        out.assertFieldTypeEquals("string2", FieldType.FLOAT);
        out.assertFieldEquals("long1", 7);
        out.assertFieldTypeEquals("long1", FieldType.INT);
        out.assertFieldEquals("long2", 8);
        out.assertFieldTypeEquals("long2", FieldType.INT);
        //merged fields
//        out.assertFieldEquals("longs1", Arrays.asList(1L, 3, 5, 7));
        out.assertFieldEquals("longs1", Arrays.asList(7, 5, 3, 1L));
        out.assertFieldTypeEquals("longs1", FieldType.ARRAY);
    }

    @Test
    public void testTakeFirstMerge() {
        Record record1 = new StandardRecord();
        record1.setField("string1", FieldType.STRING, "value1");
        record1.setField("string2", FieldType.STRING, "value2");
        record1.setField("long1", FieldType.LONG, 1L);
        record1.setField("long2", FieldType.LONG, 2L);
        Record record2 = new StandardRecord();
        record2.setField("string1", FieldType.FLOAT, 1f);
        record2.setField("string2", FieldType.FLOAT, 2f);
        record2.setField("long1", FieldType.INT, 3);
        record2.setField("long2", FieldType.INT, 4);
        Record record3 = new StandardRecord();
        record3.setField("string1", FieldType.FLOAT, 2f);
        record3.setField("string2", FieldType.FLOAT, 2f);
        record3.setField("long1", FieldType.INT, 5);
        record3.setField("long2", FieldType.INT, 6);
        Record record4 = new StandardRecord();
        record4.setField("string1", FieldType.FLOAT, 3f);
        record4.setField("string2", FieldType.FLOAT, 2f);
        record4.setField("long1", FieldType.INT, 7);
        record4.setField("long2", FieldType.INT, 8);

        TestRunner testRunner = TestRunners.newTestRunner(new MergeRecordProcessor());
        testRunner.setProcessorIdentifier("testProc");
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(4);
        out.assertFieldEquals("string1", "value1");
        out.assertFieldTypeEquals("string1", FieldType.STRING);
        out.assertFieldEquals("string2", "value2");
        out.assertFieldTypeEquals("string2", FieldType.STRING);
        out.assertFieldEquals("long1", 1L);
        out.assertFieldTypeEquals("long1", FieldType.LONG);
        out.assertFieldEquals("long2", 2L);
        out.assertFieldTypeEquals("long2", FieldType.LONG);
    }


}
