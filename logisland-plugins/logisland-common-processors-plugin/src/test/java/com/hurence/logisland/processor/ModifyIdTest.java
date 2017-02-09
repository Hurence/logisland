package com.hurence.logisland.processor;

import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.util.avro.eventgenerator.DataGenerator;
import com.hurence.logisland.util.record.RecordSchemaUtil;
import com.hurence.logisland.util.runner.MockRecord;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import org.apache.avro.Schema;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
//import static org.hamcrest.Matchers.*;

import java.util.List;
import java.util.UUID;

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

    //TODO all options must be tested (charset for ex)

    @Test
    public void testHashStrategy() {
        final TestRunner testRunner = TestRunners.newTestRunner(new ModifyId());
        testRunner.setProperty(ModifyId.STRATEGY, ModifyId.GENERATE_HASH.getValue());
        testRunner.setProperty(ModifyId.FIELDS_TO_USE_FOR_HASH, "string1");
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
        outputRecord.assertFieldEquals("record_id",  "��ok\u007Fwc��\"ݦ\u000B*\u000B��A*�\u0018K�N�x�M7�\u001D\u000B");


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
        modifierIdProcessor.setProperty(ModifyId.STRATEGY, ModifyId.GENERATE_HASH.getValue());
        modifierIdProcessor.setProperty(ModifyId.FIELDS_TO_USE_FOR_HASH, "string1,string2,long1,long2,string1,string2,string1,string2");
        modifierIdProcessor.assertValid();
        testProcessorPerfByRecord(modifierIdProcessor, records, 100);
        //randomUID
        modifierIdProcessor.clearQueues();
        modifierIdProcessor.setProperty(ModifyId.STRATEGY, ModifyId.GENERATE_RANDOM_UUID.getValue());
        modifierIdProcessor.removeProperty(ModifyId.FIELDS_TO_USE_FOR_HASH);
        modifierIdProcessor.assertValid();
        testProcessorPerfByRecord(modifierIdProcessor, records, 100);
        //TODO FORMAT STRING
//        testRunner.setProperty(ModifyId.STRATEGY, ModifyId.GENERATE_HASH.getValue());
//        testRunner.setProperty(ModifyId.FIELDS_TO_USE_FOR_HASH, "string1,string2,long1,long2,string1,string2,string1,string2");
//        testRunner.assertValid();
//        testProcessorPerfByRecord(testRunner, records, 100);
    }
    @Test
    public void testRandomUidStrategy() {
        final TestRunner testRunner = TestRunners.newTestRunner(new ModifyId());
        testRunner.setProperty(ModifyId.STRATEGY, ModifyId.GENERATE_RANDOM_UUID.getValue());
        testRunner.setProperty(ModifyId.FIELDS_TO_USE_FOR_HASH, "string1");
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
}
