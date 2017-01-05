package com.hurence.logisland.processor;

import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;

public class ParsePropertiesTest extends BaseSyslogTest {

	private static final Logger logger = LoggerFactory.getLogger(ParsePropertiesTest.class);

	@Test
	public void testNoProperties() {
		Record record1 = getRecord1();

		TestRunner testRunner = TestRunners.newTestRunner(new ParseProperties());
		testRunner.setProperty(ParseProperties.PROPERTIES_FIELD, "properties");
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
	public void testPropertiesClean() {
		Record record1 = getRecord1();
		record1.setField("properties", FieldType.STRING, "a=1 b=2 c=3");

		TestRunner testRunner = TestRunners.newTestRunner(new ParseProperties());
		testRunner.setProperty(ParseProperties.PROPERTIES_FIELD, "properties");
		testRunner.assertValid();
		testRunner.enqueue(record1);
		testRunner.run();
		testRunner.assertAllInputRecordsProcessed();
		testRunner.assertOutputRecordsCount(1);

		assertEquals(10, record1.getAllFields().size());
		assertEquals("value1", record1.getField("string1").getRawValue());
		assertEquals(FieldType.STRING, record1.getField("string1").getType());
		assertEquals("value2", record1.getField("string2").getRawValue());
		assertEquals(FieldType.STRING, record1.getField("string2").getType());
		assertEquals(1, record1.getField("long1").getRawValue());
		assertEquals(FieldType.LONG, record1.getField("long1").getType());
		assertEquals(2, record1.getField("long2").getRawValue());
		assertEquals(FieldType.LONG, record1.getField("long2").getType());
		assertEquals("1", record1.getField("a").getRawValue());
		assertEquals("2", record1.getField("b").getRawValue());
		assertEquals("3", record1.getField("c").getRawValue());
	}

	@Test
	public void testPropertiesWithHeadAndTrailingSpaces() {
		Record record1 = getRecord1();
		record1.setField("properties", FieldType.STRING, "   a=1 b=2 c=3 ");

		TestRunner testRunner = TestRunners.newTestRunner(new ParseProperties());
		testRunner.setProperty(ParseProperties.PROPERTIES_FIELD, "properties");
		testRunner.assertValid();
		testRunner.enqueue(record1);
		testRunner.run();
		testRunner.assertAllInputRecordsProcessed();
		testRunner.assertOutputRecordsCount(1);

		assertEquals(10, record1.getAllFields().size());
		assertField(record1, "string1", "value1", FieldType.STRING);
		assertField(record1, "string2", "value2", FieldType.STRING);
		assertField(record1, "long1", 1, FieldType.LONG);
		assertField(record1, "long2", 2, FieldType.LONG);
		assertField(record1, "a", "1", FieldType.STRING);
		assertField(record1, "b", "2", FieldType.STRING);
		assertField(record1, "c", "3", FieldType.STRING);
	}

	private void assertField(Record record, String key, Object value, FieldType type) {
		assertEquals(value, record.getField(key).getRawValue());
		assertEquals(type, record.getField(key).getType());
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
