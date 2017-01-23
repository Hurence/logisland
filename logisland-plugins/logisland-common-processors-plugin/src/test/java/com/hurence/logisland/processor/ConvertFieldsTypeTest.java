/**
 * Copyright (C) 2016 Hurence (bailet.thomas@gmail.com)
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
import org.json.simple.parser.ParseException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ConvertFieldsTypeTest extends BaseSyslogTest {

	private static final Logger logger = LoggerFactory.getLogger(ConvertFieldsTypeTest.class);


	private Record getRecord() {
		Record record = new StandardRecord();
		record.setField("string1", FieldType.STRING, "value1");
		record.setField("int1", FieldType.STRING, "1");
		record.setField("long1", FieldType.STRING, "1");
		record.setField("float1", FieldType.STRING, "3.4f");
        record.setField("float2", FieldType.FLOAT, 3.4f);
		return record;
	}


	@Test
	public void testBasicConversion() {

		Record record = getRecord();

        TestRunner testRunner = TestRunners.newTestRunner(new ConvertFieldsType());
        testRunner.setProperty("int1", "int");
        testRunner.setProperty("long1", "long");
        testRunner.setProperty("float2", "string");
        testRunner.assertValid();
        testRunner.enqueue(record);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);
        MockRecord outputRecord = testRunner.getOutputRecords().get(0);
        outputRecord.assertRecordSizeEquals(5);
        outputRecord.assertFieldEquals("string1",  "value1");
        outputRecord.assertFieldEquals("int1",  1);
        outputRecord.assertFieldNotEquals("int1",  "1");
        outputRecord.assertFieldEquals("long1",  1L);
        outputRecord.assertFieldEquals("float1",  3.4f);
        outputRecord.assertFieldEquals("float2",  "3.4");
	}

    @Test
    public void testWrongConversion() {

        Record record = getRecord();

        TestRunner testRunner = TestRunners.newTestRunner(new ConvertFieldsType());
        testRunner.setProperty("string1", "int");
        testRunner.setProperty("int1", "float");
        testRunner.setProperty("long1", "bool");
        testRunner.setProperty("float2", "int");
        testRunner.setProperty("float3", "int");

        testRunner.assertValid();
        testRunner.enqueue(record);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);
        MockRecord outputRecord = testRunner.getOutputRecords().get(0);
        outputRecord.assertRecordSizeEquals(5);
        outputRecord.assertFieldEquals("string1",  "value1");
        outputRecord.assertFieldEquals("int1",  1);
        outputRecord.assertFieldNotEquals("int1",  "1");
        outputRecord.assertFieldEquals("long1",  1L);
        outputRecord.assertFieldEquals("float1",  3.4f);
        outputRecord.assertFieldEquals("float2",  3);
    }

}
