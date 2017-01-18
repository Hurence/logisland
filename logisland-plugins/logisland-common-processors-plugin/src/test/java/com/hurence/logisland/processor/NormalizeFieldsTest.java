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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class NormalizeFieldsTest extends BaseSyslogTest {

    private static final Logger logger = LoggerFactory.getLogger(NormalizeFieldsTest.class);

    private Record getRecord1() {
        Record record1 = new StandardRecord();
        record1.setField("string1", FieldType.STRING, "value1");
        record1.setField("string2", FieldType.STRING, "value2");
        record1.setField("long1", FieldType.LONG, 1);
        record1.setField("long2", FieldType.LONG, 2);
        return record1;
    }

    @Test
    public void testNoMapping() {
        Record record1 = getRecord1();

        TestRunner testRunner = TestRunners.newTestRunner(new NormalizeFields());
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

    @Test
    public void testNormalizeSimple() {

        Record record1 = getRecord1();

        TestRunner testRunner = TestRunners.newTestRunner(new NormalizeFields());
        testRunner.setProperty("string3", "string1");
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(4);
        out.assertFieldNotExists("string1");
        out.assertFieldEquals("string3", "value1");
        out.assertFieldTypeEquals("string3", FieldType.STRING);
        out.assertFieldEquals("string2", "value2");
        out.assertFieldTypeEquals("string2", FieldType.STRING);
        out.assertFieldEquals("long1", 1L);
        out.assertFieldTypeEquals("long1", FieldType.LONG);
        out.assertFieldEquals("long2", 2L);
        out.assertFieldTypeEquals("long2", FieldType.LONG);
    }

    @Test
    public void testNormalizeMultiple() {

        Record record1 = getRecord1();

        TestRunner testRunner = TestRunners.newTestRunner(new NormalizeFields());
        testRunner.setProperty("string3", "string14,string5,string1");
        testRunner.setProperty("string32", "string14,string5,string2");
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(4);
        out.assertFieldNotExists("string1");
        out.assertFieldNotExists("string2");
        out.assertFieldEquals("string3", "value1");
        out.assertFieldTypeEquals("string3", FieldType.STRING);
        out.assertFieldEquals("string32", "value2");
        out.assertFieldTypeEquals("string32", FieldType.STRING);
        out.assertFieldEquals("long1", 1L);
        out.assertFieldTypeEquals("long1", FieldType.LONG);
        out.assertFieldEquals("long2", 2L);
        out.assertFieldTypeEquals("long2", FieldType.LONG);
    }

    @Test
    public void testNormalizeWithConflictDoNothing() {

        Record record1 = getRecord1();

        TestRunner testRunner = TestRunners.newTestRunner(new NormalizeFields());
        testRunner.setProperty("long1", "string1");
        testRunner.setProperty(NormalizeFields.CONFLICT_RESOLUTION_POLICY, NormalizeFields.DO_NOTHING);
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

    @Test
    public void testNormalizeWithConflictKeepOnlyOld() {

        Record record1 = getRecord1();

        TestRunner testRunner = TestRunners.newTestRunner(new NormalizeFields());
        testRunner.setProperty("long1", "string1");
        testRunner.setProperty(NormalizeFields.CONFLICT_RESOLUTION_POLICY, NormalizeFields.KEEP_ONLY_OLD_FIELD);
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(3);
        out.assertFieldEquals("long1", 1L);
        out.assertFieldTypeEquals("long1", FieldType.LONG);
        out.assertFieldEquals("string2", "value2");
        out.assertFieldTypeEquals("string2", FieldType.STRING);
        out.assertFieldNotExists("string1");
        out.assertFieldEquals("long2", 2L);
        out.assertFieldTypeEquals("long2", FieldType.LONG);
    }

    @Test
    public void testNormalizeWithConflictOverwrite() {

        Record record1 = getRecord1();

        TestRunner testRunner = TestRunners.newTestRunner(new NormalizeFields());
        testRunner.setProperty("long1", "string1");
        testRunner.setProperty(NormalizeFields.CONFLICT_RESOLUTION_POLICY, NormalizeFields.OVERWRITE_EXISTING);
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(3);
        out.assertFieldEquals("long1", "value1");
        out.assertFieldTypeEquals("long1", FieldType.STRING);
        out.assertFieldEquals("string2", "value2");
        out.assertFieldTypeEquals("string2", FieldType.STRING);
        out.assertFieldNotExists("string1");
        out.assertFieldEquals("long2", 2L);
        out.assertFieldTypeEquals("long2", FieldType.LONG);
    }

    @Test
    public void testNormalizeWithConflictKeepBoth() {

        Record record1 = getRecord1();

        TestRunner testRunner = TestRunners.newTestRunner(new NormalizeFields());
        testRunner.setProperty("string1bis", "string1");
        testRunner.setProperty(NormalizeFields.CONFLICT_RESOLUTION_POLICY, NormalizeFields.KEEP_BOTH_FIELDS);
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(5);
        out.assertFieldEquals("string1bis", "value1");
        out.assertFieldTypeEquals("string1bis", FieldType.STRING);
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
    public void testNormalizeMappingNotUsed() {

        Record record1 = getRecord1();

        TestRunner testRunner = TestRunners.newTestRunner(new NormalizeFields());
        testRunner.setProperty("string4", "string3");
        testRunner.setProperty("string6", "string5");
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
