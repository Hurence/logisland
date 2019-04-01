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

public class ConvertSimpleDateFormatFieldsTest extends BaseSyslogTest {

    private static final Logger logger = LoggerFactory.getLogger(ConvertSimpleDateFormatFieldsTest.class);

    private static final String DATE1_UNIX_EPOCH_TIME = "1551858837000";
    private static final String DATE2_UNIX_EPOCH_TIME = "1520322837000";

    private Record getRecord1() {
        Record record1 = new StandardRecord();
        record1.setField("date1_format1", FieldType.STRING, "20190306085357000");
        record1.setField("date1_format2", FieldType.STRING, "20190306 085357000");
        record1.setField("date1_format3", FieldType.STRING, "2019-03-06 08:53:57");
        record1.setField("dateNOK", FieldType.STRING, "WRONG SIMPLE DATE FORMAT");
        record1.setField("date2_format1", FieldType.STRING, "20180306085357000");
        record1.setField("date2_typeLong_format1", FieldType.LONG, "20180306085357000");
        return record1;
    }

    @Test
    public void testNoConversion() {
        Record record1 = getRecord1();
        int recSizeOri=record1.size();

        TestRunner testRunner = TestRunners.newTestRunner(new ConvertSimpleDateFormatFields());
        testRunner.setProperty("input.date.format", "yyyyMMddHHmmssSSS");
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertFieldEquals("date1_format1", "20190306085357000");
        out.assertFieldTypeEquals("date1_format1", FieldType.STRING);
        out.assertFieldEquals("date1_format2", "20190306 085357000");
        out.assertFieldTypeEquals("date1_format1", FieldType.STRING);
        out.assertRecordSizeEquals(recSizeOri);
    }

    @Test
    public void testConvertSimpleDate1Value() {

        Record record1 = getRecord1();
        int recSizeOri=record1.size();

        TestRunner testRunner = TestRunners.newTestRunner(new ConvertSimpleDateFormatFields());
        testRunner.setProperty("unixDate", "${date1_format1}");
        testRunner.setProperty("input.date.format", "yyyyMMddHHmmssSSS");
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertFieldEquals("unixDate", DATE1_UNIX_EPOCH_TIME);
        out.assertFieldTypeEquals("unixDate", FieldType.LONG);
        out.assertRecordSizeEquals(recSizeOri+1);
    }

    @Test
    public void testConvertSimpleDate2Value() {

        Record record1 = getRecord1();
        int recSizeOri=record1.size();

        TestRunner testRunner = TestRunners.newTestRunner(new ConvertSimpleDateFormatFields());
        testRunner.setProperty("unixDate", "${date1_format2}");
        testRunner.setProperty("input.date.format", "yyyyMMdd HHmmssSSS");
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertFieldEquals("unixDate", DATE1_UNIX_EPOCH_TIME);
        out.assertFieldTypeEquals("unixDate", FieldType.LONG);
        out.assertRecordSizeEquals(recSizeOri+1);
    }

    @Test
    public void testConvertSimpleDate3Value() {

        Record record1 = getRecord1();
        int recSizeOri=record1.size();

        TestRunner testRunner = TestRunners.newTestRunner(new ConvertSimpleDateFormatFields());
        testRunner.setProperty("unixDate", "${date1_format3}");
        testRunner.setProperty("input.date.format", "yyyy-MM-dd HH:mm:ss");
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertFieldEquals("unixDate", DATE1_UNIX_EPOCH_TIME);
        out.assertFieldTypeEquals("unixDate", FieldType.LONG);
        out.assertRecordSizeEquals(recSizeOri+1);
    }

    @Test
    public void testConvertSimpleDateAsLongValue() {

        Record record1 = getRecord1();
        int recSizeOri=record1.size();

        TestRunner testRunner = TestRunners.newTestRunner(new ConvertSimpleDateFormatFields());
        testRunner.setProperty("unixDate", "${date2_typeLong_format1}");
        testRunner.setProperty("input.date.format", "yyyyMMddHHmmssSSS");
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertFieldEquals("unixDate", DATE2_UNIX_EPOCH_TIME);
        out.assertFieldTypeEquals("unixDate", FieldType.LONG);
        out.assertRecordSizeEquals(recSizeOri+1);
    }

    @Test
    public void testWrongSimpleDate3Value() {

        Record record1 = getRecord1();
        int recSizeOri=record1.size();

        TestRunner testRunner = TestRunners.newTestRunner(new ConvertSimpleDateFormatFields());
        testRunner.setProperty("unixDate", "${dateNOK}");
        testRunner.setProperty("input.date.format", "yyyy-MM-dd HH:mm:ss");
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertFieldNotExists("unixDate");
        out.assertRecordSizeEquals(recSizeOri);
    }

    @Test
    public void testConvertMultipleSimpleDateValue() {

        Record record1 = getRecord1();
        int recSizeOri=record1.size();

        TestRunner testRunner = TestRunners.newTestRunner(new ConvertSimpleDateFormatFields());
        testRunner.setProperty("unixDate1", "${date1_format1}");
        testRunner.setProperty("unixDate2", "${date2_format1}");
        testRunner.setProperty("input.date.format", "yyyyMMddHHmmssSSS");
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertFieldEquals("unixDate1", DATE1_UNIX_EPOCH_TIME);
        out.assertFieldTypeEquals("unixDate1", FieldType.LONG);
        out.assertFieldEquals("unixDate2", DATE2_UNIX_EPOCH_TIME);
        out.assertFieldTypeEquals("unixDate2", FieldType.LONG);
        out.assertRecordSizeEquals(recSizeOri+2);
    }

    @Test
    public void testConvertSimpleDateAndOverwriteValue() {

        Record record1 = getRecord1();
        int recSizeOri=record1.size();

        TestRunner testRunner = TestRunners.newTestRunner(new ConvertSimpleDateFormatFields());
        testRunner.setProperty("date1_format2", "${date1_format2}");
        testRunner.setProperty("input.date.format", "yyyyMMdd HHmmssSSS");
        testRunner.setProperty("conflict.resolution.policy", "overwrite_existing");
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertFieldEquals("date1_format2", DATE1_UNIX_EPOCH_TIME);
        out.assertFieldTypeEquals("date1_format2", FieldType.LONG);
        out.assertRecordSizeEquals(recSizeOri);
    }

}
