package com.hurence.logisland.processor;

import com.hurence.logisland.processor.util.BaseSyslogTest;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
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

public class NormalizeFieldsTest extends BaseSyslogTest {

	private static final Logger logger = LoggerFactory.getLogger(NormalizeFieldsTest.class);

	@Test
	public void testNoMapping() {

		Record record1 = getRecord1();

		TestRunner testRunner = TestRunners.newTestRunner(new NormalizeFields());
		testRunner.setProperty(NormalizeFields.FIELDS_NAME_MAPPING, "{}");
		testRunner.assertValid();
		testRunner.enqueue(record1);
		testRunner.run();
		testRunner.assertAllInputRecordsProcessed();
		testRunner.assertOutputRecordsCount(1);

		assertEquals(7, record1.getAllFields().size());
		assertEquals("value1", record1.getField("string1").getRawValue());
		assertEquals(FieldType.STRING, record1.getField("string1").getType());
		assertEquals("value2", record1.getField("string2").getRawValue());
		assertEquals(FieldType.STRING, record1.getField("string2").getType());
		assertEquals(1, record1.getField("long1").getRawValue());
		assertEquals(FieldType.LONG, record1.getField("long1").getType());
		assertEquals(2, record1.getField("long2").getRawValue());
		assertEquals(FieldType.LONG, record1.getField("long2").getType());
	}

	@Test
	public void testNormalize_1() {

		Record record1 = getRecord1();

		TestRunner testRunner = TestRunners.newTestRunner(new NormalizeFields());
		testRunner.setProperty(NormalizeFields.FIELDS_NAME_MAPPING, "{\"string1\": \"string3\"}");
		testRunner.assertValid();
		testRunner.enqueue(record1);
		testRunner.run();
		testRunner.assertAllInputRecordsProcessed();
		testRunner.assertOutputRecordsCount(1);

		assertEquals(7, record1.getAllFields().size());
		assertNull(record1.getField("string1"));
		assertEquals("value1", record1.getField("string3").getRawValue());
		assertEquals(FieldType.STRING, record1.getField("string2").getType());
		assertEquals("value2", record1.getField("string2").getRawValue());
		assertEquals(FieldType.STRING, record1.getField("string2").getType());
		assertEquals(1, record1.getField("long1").getRawValue());
		assertEquals(FieldType.LONG, record1.getField("long1").getType());
		assertEquals(2, record1.getField("long2").getRawValue());
		assertEquals(FieldType.LONG, record1.getField("long2").getType());
	}

	@Test
	public void testNormalize_2() {

		Record record1 = getRecord1();

		TestRunner testRunner = TestRunners.newTestRunner(new NormalizeFields());
		testRunner.setProperty(NormalizeFields.FIELDS_NAME_MAPPING, "{\"string1\": \"long1\"}");
		testRunner.assertValid();
		testRunner.enqueue(record1);
		testRunner.run();
		testRunner.assertAllInputRecordsProcessed();
		testRunner.assertOutputRecordsCount(1);

		assertEquals(6, record1.getAllFields().size());
		assertNull(record1.getField("string1"));
		assertEquals(FieldType.STRING, record1.getField("string2").getType());
		assertEquals("value2", record1.getField("string2").getRawValue());
		assertEquals(FieldType.STRING, record1.getField("string2").getType());
		assertEquals("value1", record1.getField("long1").getRawValue());
		assertEquals(FieldType.STRING, record1.getField("long1").getType());
		assertEquals(2, record1.getField("long2").getRawValue());
		assertEquals(FieldType.LONG, record1.getField("long2").getType());
	}

	@Test
	public void testNormalize_MappingNotUsed() {

		Record record1 = getRecord1();

		TestRunner testRunner = TestRunners.newTestRunner(new NormalizeFields());
		testRunner.setProperty(NormalizeFields.FIELDS_NAME_MAPPING, "{\"string3\": \"string4\", \"string5\": \"string6\"}");
		testRunner.assertValid();
		testRunner.enqueue(record1);
		testRunner.run();
		testRunner.assertAllInputRecordsProcessed();
		testRunner.assertOutputRecordsCount(1);

		assertEquals(7, record1.getAllFields().size());
		assertEquals("value1", record1.getField("string1").getRawValue());
		assertEquals(FieldType.STRING, record1.getField("string2").getType());
		assertEquals("value2", record1.getField("string2").getRawValue());
		assertEquals(FieldType.STRING, record1.getField("string2").getType());
		assertEquals(1, record1.getField("long1").getRawValue());
		assertEquals(FieldType.LONG, record1.getField("long1").getType());
		assertEquals(2, record1.getField("long2").getRawValue());
		assertEquals(FieldType.LONG, record1.getField("long2").getType());
	}

	@Test(expected = ProcessException.class)
	public void testNormalize_MappingKeyConflict() throws FileNotFoundException, IOException, ParseException, URISyntaxException {
		TestRunner testRunner = TestRunners.newTestRunner(new NormalizeFields());
		testRunner.setProperty(NormalizeFields.FIELDS_NAME_MAPPING, "{\"string1\": \"string3\", \"string1\": \"string4\"}");
		testRunner.assertValid();
	}

	@Test
	public void testNormalize_MappingChain() {

		Record record1 = getRecord1();

		TestRunner testRunner = TestRunners.newTestRunner(new NormalizeFields());
		testRunner.setProperty(NormalizeFields.FIELDS_NAME_MAPPING, "{\"string1\": \"string2\", \"string2\": \"string3\"}");
		testRunner.assertNotValid();
	}

	private Record getRecord1() {
		Record record1 = new StandardRecord();
		record1.setField("string1", FieldType.STRING, "value1");
		record1.setField("string2", FieldType.STRING, "value2");
		record1.setField("long1", FieldType.LONG, 1);
		record1.setField("long2", FieldType.LONG, 2);
		return record1;
	}

}
