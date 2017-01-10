package com.hurence.logisland.processor;

import com.hurence.logisland.processor.util.BaseSyslogTest;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.util.runner.MockRecord;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import org.json.simple.parser.ParseException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class RemoveFieldsTest extends BaseSyslogTest {

	private static final Logger logger = LoggerFactory.getLogger(RemoveFieldsTest.class);

	private Record getRecord1() {
		Record record1 = new StandardRecord();
		record1.setField("string1", FieldType.STRING, "value1");
		record1.setField("string2", FieldType.STRING, "value2");
		record1.setField("long1", FieldType.LONG, 1);
		record1.setField("long2", FieldType.LONG, 2);
		return record1;
	}

	@Test
	public void testNothingToRemove() {
		Record record1 = getRecord1();
		TestRunner testRunner = TestRunners.newTestRunner(new RemoveFields());
		testRunner.setProperty(RemoveFields.FIELDS_TO_REMOVE, "");
		testRunner.assertValid();
		testRunner.enqueue(record1);
		testRunner.run();
		testRunner.assertAllInputRecordsProcessed();
		testRunner.assertOutputRecordsCount(1);
        MockRecord outputRecord = testRunner.getOutputRecords().get(0);

        outputRecord.assertRecordSizeEquals(4);
        outputRecord.assertFieldEquals("string1",  "value1");
        outputRecord.assertFieldEquals("string2",  "value2");
        outputRecord.assertFieldEquals("long1",  1);
        outputRecord.assertFieldEquals("long2",  2);
	}

	@Test
	public void testRemoveOneField() {

		Record record1 = getRecord1();
		TestRunner testRunner = TestRunners.newTestRunner(new RemoveFields());
		testRunner.setProperty(RemoveFields.FIELDS_TO_REMOVE, "string1");
		testRunner.assertValid();
		testRunner.enqueue(record1);
		testRunner.run();
		testRunner.assertAllInputRecordsProcessed();
		testRunner.assertOutputRecordsCount(1);

		MockRecord outputRecord = testRunner.getOutputRecords().get(0);

		outputRecord.assertRecordSizeEquals(3);
		outputRecord.assertFieldNotExists("string1");
		outputRecord.assertFieldEquals("string2",  "value2");
		outputRecord.assertFieldEquals("long1",  1);
		outputRecord.assertFieldEquals("long2",  2);

	}

    @Test
    public void testRemove2Fields() {

        Record record1 = getRecord1();
        TestRunner testRunner = TestRunners.newTestRunner(new RemoveFields());
        testRunner.setProperty(RemoveFields.FIELDS_TO_REMOVE, "string1,long1");
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord outputRecord = testRunner.getOutputRecords().get(0);

        outputRecord.assertRecordSizeEquals(2);
        outputRecord.assertFieldNotExists("string1");
        outputRecord.assertFieldNotExists("long1");
        outputRecord.assertFieldEquals("string2",  "value2");
        outputRecord.assertFieldEquals("long2",  2);
    }


    @Test
    public void testRemoveNonExistingField() {

        Record record1 = getRecord1();
        TestRunner testRunner = TestRunners.newTestRunner(new RemoveFields());
        testRunner.setProperty(RemoveFields.FIELDS_TO_REMOVE, "string3");
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord outputRecord = testRunner.getOutputRecords().get(0);

        outputRecord.assertRecordSizeEquals(4);
        outputRecord.assertFieldEquals("string1",  "value1");
        outputRecord.assertFieldEquals("string2",  "value2");
        outputRecord.assertFieldEquals("long1",  1);
        outputRecord.assertFieldEquals("long2",  2);
    }

	@Test//(expected = ProcessException.class)
	public void testRemoveTwiceAfield() throws FileNotFoundException, IOException, ParseException, URISyntaxException {
        Record record1 = getRecord1();
        TestRunner testRunner = TestRunners.newTestRunner(new RemoveFields());
        testRunner.setProperty(RemoveFields.FIELDS_TO_REMOVE, "string1,string1");
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord outputRecord = testRunner.getOutputRecords().get(0);


        outputRecord.assertRecordSizeEquals(3);
        outputRecord.assertFieldNotExists("string1");
        outputRecord.assertFieldEquals("string2",  "value2");
        outputRecord.assertFieldEquals("long1",  1);
        outputRecord.assertFieldEquals("long2",  2);
	}




}
