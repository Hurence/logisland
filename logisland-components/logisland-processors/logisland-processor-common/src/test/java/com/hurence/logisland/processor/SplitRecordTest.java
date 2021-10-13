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

public class SplitRecordTest {
    @Test
    public void testValidity() {
        final TestRunner testRunner = TestRunners.newTestRunner(new SplitRecord());
        testRunner.assertValid();
        testRunner.setProperty(SplitRecord.KEEP_PARENT_RECORD, "00");
        testRunner.assertNotValid();
        testRunner.setProperty(SplitRecord.KEEP_PARENT_RECORD, "false");
        testRunner.assertValid();
        testRunner.setProperty(SplitRecord.KEEP_PARENT_RECORD_TIME, "false");
        testRunner.assertValid();
        testRunner.setProperty(SplitRecord.KEEP_PARENT_RECORD_TYPE, "tru");
        testRunner.assertNotValid();
    }

    @Test
    public void testRecords() {
        Record record1 = new StandardRecord();
        record1.setField("field1", FieldType.STRING, "Hello World");
        record1.setField("field2", FieldType.STRING, "Logisland");
        record1.setField("field3", FieldType.INT, 1000);

        TestRunner testRunner = TestRunners.newTestRunner(new SplitRecord());
        testRunner.setProperty("record_type1", "field1, field2");
        testRunner.setProperty("record_type2", "field3");
        testRunner.setProperty("record_type3", "field1, field3");
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(3);

        MockRecord out = testRunner.getOutputRecords().get(2);
        out.assertRecordSizeEquals(3);
        out.assertFieldTypeEquals("field1", FieldType.STRING);
        out.assertFieldTypeEquals("field2", FieldType.STRING);
        out.assertFieldTypeEquals("record_time", FieldType.LONG);
        out.assertFieldTypeEquals("record_type", FieldType.STRING);
        out.assertFieldEquals("field1", "Hello World");
        out.assertFieldEquals("field2", "Logisland");
        out.assertFieldEquals("record_type", "record_type1");
        out.assertFieldEquals("record_time", record1.getTime().getTime());

        MockRecord out1 = testRunner.getOutputRecords().get(0);
        out1.assertRecordSizeEquals(2);
        out1.assertFieldTypeEquals("field3", FieldType.INT);
        out1.assertFieldTypeEquals("record_time", FieldType.LONG);
        out1.assertFieldTypeEquals("record_type", FieldType.STRING);
        out1.assertFieldEquals("field3", 1000);
        out1.assertFieldEquals("record_type", "record_type2");
        out1.assertFieldEquals("record_time", record1.getTime().getTime());

        MockRecord out2 = testRunner.getOutputRecords().get(1);
        out2.assertRecordSizeEquals(3);
        out2.assertFieldTypeEquals("field1", FieldType.STRING);
        out2.assertFieldTypeEquals("field3", FieldType.INT);
        out2.assertFieldTypeEquals("record_time", FieldType.LONG);
        out2.assertFieldTypeEquals("record_type", FieldType.STRING);
        out2.assertFieldEquals("field1", "Hello World");
        out2.assertFieldEquals("field3", 1000);
        out2.assertFieldEquals("record_type", "record_type3");
        out2.assertFieldEquals("record_time", record1.getTime().getTime());
    }
    @Test
    public void testRecordsKeepParent() {
        Record record1 = new StandardRecord();
        record1.setField("field1", FieldType.STRING, "Hello World");
        record1.setField("field2", FieldType.STRING, "Logisland");
        record1.setField("field3", FieldType.INT, 1000);

        TestRunner testRunner = TestRunners.newTestRunner(new SplitRecord());
        testRunner.setProperty(SplitRecord.KEEP_PARENT_RECORD, "true");
        testRunner.setProperty("record_type1", "field1, field2");
        testRunner.setProperty("record_type2", "field3");
        testRunner.setProperty("record_type3", "field1, field3");
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(4);

        MockRecord out = testRunner.getOutputRecords().get(2);
        out.assertRecordSizeEquals(3);
        out.assertFieldTypeEquals("field1", FieldType.STRING);
        out.assertFieldTypeEquals("field2", FieldType.STRING);
        out.assertFieldEquals("field1", "Hello World");
        out.assertFieldEquals("field2", "Logisland");

        MockRecord out1 = testRunner.getOutputRecords().get(0);
        out1.assertRecordSizeEquals(2);
        out1.assertFieldTypeEquals("field3", FieldType.INT);
        out1.assertFieldEquals("field3", 1000);

        MockRecord out2 = testRunner.getOutputRecords().get(1);
        out2.assertRecordSizeEquals(3);
        out2.assertFieldTypeEquals("field1", FieldType.STRING);
        out2.assertFieldTypeEquals("field3", FieldType.INT);
        out2.assertFieldEquals("field1", "Hello World");
        out2.assertFieldEquals("field3", 1000);

        MockRecord out3 = testRunner.getOutputRecords().get(3);
        out3.assertRecordSizeEquals(3);
        out3.assertFieldTypeEquals("field1", FieldType.STRING);
        out3.assertFieldTypeEquals("field2", FieldType.STRING);
        out3.assertFieldTypeEquals("field3", FieldType.INT);
        out3.assertFieldEquals("field1", "Hello World");
        out3.assertFieldEquals("field2", "Logisland");
        out3.assertFieldEquals("field3", 1000);

    }
    @Test
    public void testRecordsKeepAll() {
        Record record1 = new StandardRecord();
        record1.setField("field1", FieldType.STRING, "Hello World");
        record1.setField("field2", FieldType.STRING, "Logisland");
        record1.setField("field3", FieldType.INT, 1000);

        TestRunner testRunner = TestRunners.newTestRunner(new SplitRecord());
        testRunner.setProperty(SplitRecord.KEEP_PARENT_RECORD, "true");
        testRunner.setProperty(SplitRecord.KEEP_PARENT_RECORD_TIME, "false");
        testRunner.setProperty(SplitRecord.KEEP_PARENT_RECORD_TYPE, "true");
        testRunner.setProperty("record_type1", "field1, field2");
        testRunner.setProperty("record_type2", "field3");
        testRunner.setProperty("record_type3", "field1, field3");
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(4);

        MockRecord out = testRunner.getOutputRecords().get(2);
        out.assertRecordSizeEquals(3);
        out.assertFieldTypeEquals("field1", FieldType.STRING);
        out.assertFieldTypeEquals("field2", FieldType.STRING);
        out.assertFieldTypeEquals("record_type", FieldType.STRING);
        out.assertFieldTypeEquals("record_time", FieldType.LONG);
        out.assertFieldEquals("field1", "Hello World");
        out.assertFieldEquals("field2", "Logisland");
        out.assertFieldEquals("record_type", record1.getType());

        MockRecord out1 = testRunner.getOutputRecords().get(0);
        out1.assertRecordSizeEquals(2);
        out1.assertFieldTypeEquals("field3", FieldType.INT);
        out1.assertFieldTypeEquals("record_type", FieldType.STRING);
        out1.assertFieldTypeEquals("record_time", FieldType.LONG);
        out1.assertFieldEquals("field3", 1000);
        out1.assertFieldEquals("record_type", record1.getType());

        MockRecord out2 = testRunner.getOutputRecords().get(1);
        out2.assertRecordSizeEquals(3);
        out2.assertFieldTypeEquals("field1", FieldType.STRING);
        out2.assertFieldTypeEquals("field3", FieldType.INT);
        out2.assertFieldTypeEquals("record_type", FieldType.STRING);
        out2.assertFieldTypeEquals("record_time", FieldType.LONG);
        out2.assertFieldEquals("field1", "Hello World");
        out2.assertFieldEquals("field3", 1000);
        out2.assertFieldEquals("record_type", record1.getType());

        MockRecord out3 = testRunner.getOutputRecords().get(3);
        out3.assertRecordSizeEquals(3);
        out3.assertFieldTypeEquals("field1", FieldType.STRING);
        out3.assertFieldTypeEquals("field2", FieldType.STRING);
        out3.assertFieldTypeEquals("field3", FieldType.INT);
        out3.assertFieldTypeEquals("record_type", FieldType.STRING);
        out3.assertFieldTypeEquals("record_time", FieldType.LONG);
        out3.assertFieldEquals("field1", "Hello World");
        out3.assertFieldEquals("field2", "Logisland");
        out3.assertFieldEquals("field3", 1000);
        out3.assertFieldEquals("record_type", record1.getType());
    }
    @Test
    public void testValidFields() {
        Record record1 = new StandardRecord();
        record1.setField("field1", FieldType.STRING, "Hello World");
        record1.setField("field2", FieldType.STRING, "Logisland");
        record1.setField("field3", FieldType.INT, 1000);

        TestRunner testRunner = TestRunners.newTestRunner(new SplitRecord());
        testRunner.setProperty("record_type1", "field1, field2");
        testRunner.setProperty("record_type2", "field3");
        testRunner.setProperty("record_type3", "field1, field3");
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(3);

        MockRecord out = testRunner.getOutputRecords().get(2);
        out.assertRecordSizeEquals(3);
        out.assertFieldTypeEquals("record_time", FieldType.LONG);
        out.assertFieldTypeEquals("record_type", FieldType.STRING);
        out.assertFieldEquals("record_type", "record_type1");
        out.assertFieldEquals("record_time", record1.getTime().getTime());

        MockRecord out1 = testRunner.getOutputRecords().get(0);
        out1.assertRecordSizeEquals(2);
        out1.assertFieldTypeEquals("record_time", FieldType.LONG);
        out1.assertFieldTypeEquals("record_type", FieldType.STRING);
        out1.assertFieldEquals("record_type", "record_type2");
        out1.assertFieldEquals("record_time", record1.getTime().getTime());

        MockRecord out2 = testRunner.getOutputRecords().get(1);
        out2.assertRecordSizeEquals(3);
        out2.assertFieldTypeEquals("record_time", FieldType.LONG);
        out2.assertFieldTypeEquals("record_type", FieldType.STRING);
        out2.assertFieldEquals("record_type", "record_type3");
        out2.assertFieldEquals("record_time", record1.getTime().getTime());

        Record record2 = new StandardRecord();
        record2.setField("field1", FieldType.STRING, "Hello World");
        record2.setField("field2", FieldType.STRING, "Logisland");
        record2.setField("field3", FieldType.INT, 1000);

        TestRunner testRunner1 = TestRunners.newTestRunner(new SplitRecord());
        testRunner1.setProperty(SplitRecord.KEEP_PARENT_RECORD, "true");
        testRunner1.setProperty(SplitRecord.KEEP_PARENT_RECORD_TIME, "false");
        testRunner1.setProperty(SplitRecord.KEEP_PARENT_RECORD_TYPE, "true");
        testRunner1.setProperty("record_type1", "field1, field2");
        testRunner1.setProperty("record_type2", "field3");
        testRunner1.setProperty("record_type3", "field1, field3");
        testRunner1.assertValid();
        testRunner1.enqueue(record2);
        testRunner1.run();
        testRunner1.assertAllInputRecordsProcessed();
        testRunner1.assertOutputRecordsCount(4);

        MockRecord out4 = testRunner1.getOutputRecords().get(2);
        out4.assertRecordSizeEquals(3);
        out4.assertFieldTypeEquals("record_type", FieldType.STRING);
        out4.assertFieldTypeEquals("record_time", FieldType.LONG);
        out4.assertFieldEquals("record_type", record2.getType());

        MockRecord out5 = testRunner1.getOutputRecords().get(0);
        out5.assertRecordSizeEquals(2);
        out5.assertFieldTypeEquals("record_type", FieldType.STRING);
        out5.assertFieldTypeEquals("record_time", FieldType.LONG);
        out5.assertFieldEquals("record_type", record2.getType());

        MockRecord out6 = testRunner1.getOutputRecords().get(1);
        out6.assertRecordSizeEquals(3);
        out6.assertFieldTypeEquals("record_type", FieldType.STRING);
        out6.assertFieldTypeEquals("record_time", FieldType.LONG);
        out6.assertFieldEquals("record_type", record2.getType());

        MockRecord out3 = testRunner1.getOutputRecords().get(3);
        out3.assertRecordSizeEquals(3);

        out3.assertFieldTypeEquals("record_type", FieldType.STRING);
        out3.assertFieldTypeEquals("record_time", FieldType.LONG);
        out3.assertFieldEquals("record_type", record2.getType());
    }
    @Test
    public void testRecordsError() {
        Record record1 = new StandardRecord();
        record1.setField("field1", FieldType.STRING, "Hello World");
        record1.setField("field2", FieldType.STRING, "Logisland");
        record1.setField("field3", FieldType.INT, 1000);

        TestRunner testRunner = TestRunners.newTestRunner(new SplitRecord());
        testRunner.setProperty("record_type1", "field5, field2");
        testRunner.setProperty("record_type2", "field789");
        testRunner.setProperty("record_type3", "field1, field3");
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(2);
        out.assertRecordSizeEquals(3);
        out.assertFieldTypeEquals("field2", FieldType.STRING);
        out.assertFieldTypeEquals("record_time", FieldType.LONG);
        out.assertFieldTypeEquals("record_type", FieldType.STRING);
        out.assertFieldTypeEquals("record_errors", FieldType.ARRAY);
        out.assertFieldEquals("field2", "Logisland");
        out.assertFieldEquals("record_type", "record_type1");
        out.assertFieldEquals("record_time", record1.getTime().getTime());

        MockRecord out1 = testRunner.getOutputRecords().get(0);
        out1.assertRecordSizeEquals(2);
        out1.assertFieldTypeEquals("record_time", FieldType.LONG);
        out1.assertFieldTypeEquals("record_type", FieldType.STRING);
        out1.assertFieldTypeEquals("record_errors", FieldType.ARRAY);
        out1.assertFieldEquals("record_type", "record_type2");
        out1.assertFieldEquals("record_time", record1.getTime().getTime());

        MockRecord out2 = testRunner.getOutputRecords().get(1);
        out2.assertRecordSizeEquals(3);
        out2.assertFieldTypeEquals("field1", FieldType.STRING);
        out2.assertFieldTypeEquals("field3", FieldType.INT);
        out2.assertFieldTypeEquals("record_time", FieldType.LONG);
        out2.assertFieldTypeEquals("record_type", FieldType.STRING);
        out2.assertFieldEquals("field1", "Hello World");
        out2.assertFieldEquals("field3", 1000);
        out2.assertFieldEquals("record_type", "record_type3");
        out2.assertFieldEquals("record_time", record1.getTime().getTime());
    }


}
