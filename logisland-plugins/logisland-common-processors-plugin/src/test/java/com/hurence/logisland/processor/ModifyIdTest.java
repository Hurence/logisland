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

import com.hurence.logisland.record.FieldDictionary;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.util.record.RecordSchemaUtil;
import com.hurence.logisland.util.runner.MockRecord;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import com.hurence.logisland.util.time.DateUtil;
import org.apache.avro.Schema;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
//import static org.hamcrest.Matchers.*;

import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.util.List;
import java.util.UUID;

import static com.hurence.logisland.processor.ModifyId.CHARSET_TO_USE_FOR_HASH;

/**
 * Created by gregoire on 08/02/17.
 */
public class ModifyIdTest {

    private static final Logger logger = LoggerFactory.getLogger(ModifyIdTest.class);

    private Record getRecord1() {
        Record record1 = new StandardRecord();
        record1.setField("string1", FieldType.STRING, "value1");
        record1.setField("string2", FieldType.STRING, "value2");
        record1.setField("long1", FieldType.LONG, 1);
        record1.setField("long2", FieldType.LONG, 2);
        return record1;
    }


    @Test
    public void testValidity() {
        final TestRunner testRunner = TestRunners.newTestRunner(new ModifyId());
        testRunner.assertValid();
        testRunner.setProperty(ModifyId.STRATEGY, ModifyId.JAVA_FORMAT_STRING_WITH_FIELDS_STRATEGY.getValue());
        testRunner.assertNotValid();
        testRunner.setProperty(ModifyId.JAVA_FORMAT_STRING, "fzgzgzh");
        testRunner.assertValid();
    }

    @Test
    public void testHashStrategy() {
        final TestRunner testRunner = TestRunners.newTestRunner(new ModifyId());
        testRunner.setProperty(ModifyId.STRATEGY, ModifyId.HASH_FIELDS_STRATEGY.getValue());
        testRunner.setProperty(ModifyId.FIELDS_TO_USE, "string1");
        testRunner.assertValid();

        Record record1 = getRecord1();
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
        outputRecord.assertFieldEquals("record_id",  "3c968317f9e4bf33dfbedd26bf143fd72de9b9dd145441b75f6447ea28e");

    }

    @Test
    public void testRandomUidStrategy() {
        final TestRunner testRunner = TestRunners.newTestRunner(new ModifyId());
        testRunner.setProperty(ModifyId.STRATEGY, ModifyId.RANDOM_UUID_STRATEGY.getValue());
        testRunner.setProperty(ModifyId.FIELDS_TO_USE, "string1");
        testRunner.assertValid();

        Record record1 = getRecord1();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);
        MockRecord outputRecord = testRunner.getOutputRecords().get(0);

        String uid = UUID.randomUUID().toString();

        outputRecord.assertRecordSizeEquals(4);
        outputRecord.assertFieldEquals("string1",  "value1");
        outputRecord.assertFieldEquals("string2",  "value2");
        outputRecord.assertFieldEquals("long1",  1);
        outputRecord.assertFieldEquals("long2",  2);
        String recordId = outputRecord.getId();
        Assert.assertTrue("recordId should be an UUID", UUID.fromString(recordId) != null);
    }

    @Test
    public void testFormatStrategy() {
        final TestRunner testRunner = TestRunners.newTestRunner(new ModifyId());
        testRunner.setProperty(ModifyId.STRATEGY, ModifyId.JAVA_FORMAT_STRING_WITH_FIELDS_STRATEGY.getValue());
        testRunner.setProperty(ModifyId.FIELDS_TO_USE, "string1, string2");
        testRunner.setProperty(ModifyId.JAVA_FORMAT_STRING, "field 1 is : '%1$2s'");
        testRunner.assertValid();

        /**
         * ERRORS
         */
        Record record1 = getRecord1();

        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(0);
        testRunner.assertOutputErrorCount(1);
        MockRecord outputRecord = testRunner.getOutputRecords().get(0);

        outputRecord.assertRecordSizeEquals(5);
        outputRecord.assertFieldEquals("string1",  "value1");
        outputRecord.assertFieldEquals("string2",  "value2");
        outputRecord.assertFieldEquals("long1",  1);
        outputRecord.assertFieldEquals("long2",  2);
        outputRecord.assertFieldEquals(FieldDictionary.RECORD_ERRORS,
                "[config_setting_error: could not build id with format : 'field 1 is : '%1$2s'' \n" +
                        "fields: '[string1,  string2]' \n" +
                        " because field: ' string2' does not exist]");

        testRunner.setProperty(ModifyId.FIELDS_TO_USE, "string1,string2,long1,long2");
        testRunner.setProperty(ModifyId.JAVA_FORMAT_STRING, "string1 is %s, string2 is %s, long1 is %f, long2 is %f");
        testRunner.clearQueues();
        record1 = getRecord1();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(1);
        testRunner.assertOutputRecordsCount(0);
        outputRecord = testRunner.getOutputRecords().get(0);

        outputRecord.assertRecordSizeEquals(5);
        outputRecord.assertFieldEquals("string1",  "value1");
        outputRecord.assertFieldEquals("string2",  "value2");
        outputRecord.assertFieldEquals("long1",  1);
        outputRecord.assertFieldEquals("long2",  2);
        outputRecord.assertFieldEquals(FieldDictionary.RECORD_ERRORS,"[string_format_error: f != java.lang.Integer]");

        /**
         * WORKING FINE
         */
        testRunner.setProperty(ModifyId.FIELDS_TO_USE, "string1,string2");
        testRunner.setProperty(ModifyId.JAVA_FORMAT_STRING, "field 1 is : '%1$2s'");
        testRunner.clearQueues();
        record1 = getRecord1();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);
        outputRecord = testRunner.getOutputRecords().get(0);

        outputRecord.assertRecordSizeEquals(4);
        outputRecord.assertFieldEquals("string1",  "value1");
        outputRecord.assertFieldEquals("string2",  "value2");
        outputRecord.assertFieldEquals("long1",  1);
        outputRecord.assertFieldEquals("long2",  2);
        outputRecord.assertFieldEquals("record_id",  "field 1 is : 'value1'");

        testRunner.setProperty(ModifyId.FIELDS_TO_USE, "string1,string2,long1,long2");
        testRunner.setProperty(ModifyId.JAVA_FORMAT_STRING, "string1 is %s, string2 is %s, long1 is %d, long2 is %d");
        testRunner.clearQueues();
        record1 = getRecord1();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);
        outputRecord = testRunner.getOutputRecords().get(0);

        outputRecord.assertRecordSizeEquals(4);
        outputRecord.assertFieldEquals("string1",  "value1");
        outputRecord.assertFieldEquals("string2",  "value2");
        outputRecord.assertFieldEquals("long1",  1);
        outputRecord.assertFieldEquals("long2",  2);
        outputRecord.assertFieldEquals("record_id",  "string1 is value1, string2 is value2, long1 is 1, long2 is 2");

    }

    @Test
    public void testTypeTimeHashStrategy() throws ParseException {
        final TestRunner testRunner = TestRunners.newTestRunner(new ModifyId());
        testRunner.setProperty(ModifyId.STRATEGY, ModifyId.TYPE_TIME_HASH_STRATEGY.getValue());
        testRunner.setProperty(ModifyId.FIELDS_TO_USE, "string1,string2");
        testRunner.assertValid();

        Record record1 = getRecord1();
        record1.setTime(DateUtil.parse("2018"));
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
        outputRecord.assertFieldEquals("record_id",  "generic-1514764800000-\b ���qt:��1$\u0006�v���{�Xd \u001F\u001B��� \\�");


    }

    @Test
    public void testPerf() {
        Record record1 = getRecord1();
        Schema schema = RecordSchemaUtil.generateSchema(record1);

        TestRunner generator = TestRunners.newTestRunner(new GenerateRandomRecord());
        generator.setProperty(GenerateRandomRecord.OUTPUT_SCHEMA, schema.toString());
        generator.setProperty(GenerateRandomRecord.MIN_EVENTS_COUNT, "10000");
        generator.setProperty(GenerateRandomRecord.MAX_EVENTS_COUNT, "20000");
        generator.run();

        final Record[] records;
        {
            List<MockRecord> recordsList = generator.getOutputRecords();
            Record[] recordsArray = new Record[recordsList.size()];
            records = recordsList.toArray(recordsArray);
        }

        TestRunner modifierIdProcessor = TestRunners.newTestRunner(new ModifyId());
        //hash
        modifierIdProcessor.setProperty(ModifyId.STRATEGY, ModifyId.HASH_FIELDS_STRATEGY.getValue());
        modifierIdProcessor.setProperty(ModifyId.FIELDS_TO_USE, "string1,string2,long1,long2,string1,string2,string1,string2");
        modifierIdProcessor.assertValid();
        testProcessorPerfByRecord(modifierIdProcessor, records, 100);
        //randomUID
        modifierIdProcessor.clearQueues();
        modifierIdProcessor.setProperty(ModifyId.STRATEGY, ModifyId.RANDOM_UUID_STRATEGY.getValue());
        modifierIdProcessor.removeProperty(ModifyId.FIELDS_TO_USE);
        modifierIdProcessor.assertValid();
        testProcessorPerfByRecord(modifierIdProcessor, records, 100);
        //TODO FORMAT STRING and TYPE_TIME_FORMAT_STRING
//        testRunner.setProperty(ModifyId.STRATEGY, ModifyId.HASH_FIELDS_STRATEGY.getValue());
//        testRunner.setProperty(ModifyId.FIELDS_TO_USE, "string1,string2,long1,long2,string1,string2,string1,string2");
//        testRunner.assertValid();
//        testProcessorPerfByRecord(testRunner, records, 100);
    }

    /**
     *
     * @param runner processor
     * @param records input
     * @param maxTimeByRecord (milliseconds)
     */
    public void testProcessorPerfByRecord(TestRunner runner, Record[] records, long maxTimeByRecord) {
        runner.assertValid();
        long startTime = System.currentTimeMillis();
        runner.enqueue(records);
        runner.run();
        long endTime = System.currentTimeMillis();
        runner.assertOutputRecordsCount(records.length);
        long processingTimeByRecord = (endTime - startTime) / records.length;
        logger.info("timeProcessByRecordWas '{}'", processingTimeByRecord);
        Assert.assertTrue("maxTimeByRecord should be inferior to " + maxTimeByRecord, processingTimeByRecord <= maxTimeByRecord);
    }

    @Test
    public void testEncoding() throws NoSuchAlgorithmException {
        final TestRunner testRunner = TestRunners.newTestRunner(new ModifyId());
        testRunner.setProperty(ModifyId.STRATEGY, ModifyId.HASH_FIELDS_STRATEGY.getValue());
        testRunner.setProperty(CHARSET_TO_USE_FOR_HASH, "US-ASCII");
        testRunner.assertValid();

        /**
         * ERRORS
         */
        String rawValue = "a,b,c,12.5";
        Record record1 = getRecord1().setStringField(FieldDictionary.RECORD_VALUE,rawValue);
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);
        MockRecord outputRecord = testRunner.getOutputRecords().get(0);

        StringBuilder stb = new StringBuilder();
        stb.append(rawValue);
        final MessageDigest digest = MessageDigest.getInstance("SHA-256");
        final Charset charset = Charset.forName("US-ASCII");
        digest.update(stb.toString().getBytes(charset));
        byte[] digested = digest.digest();


        StringBuffer hexString = new StringBuffer();
        for (int i=0;i<digested.length;i++) {
            hexString.append(Integer.toHexString(0xFF & digested[i]));
        }

        String id = hexString.toString();

        outputRecord.assertFieldEquals(FieldDictionary.RECORD_ID,  id);


    }
}
