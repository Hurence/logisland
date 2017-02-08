package com.hurence.logisland.processor;

import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.util.runner.MockRecord;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import org.junit.Test;

/**
 * Created by gregoire on 08/02/17.
 */
public class ModifyIdTest {
    private Record getRecord1() {
        Record record1 = new StandardRecord();
        record1.setField("string1", FieldType.STRING, "value1");
        record1.setField("string2", FieldType.STRING, "value2");
        record1.setField("long1", FieldType.LONG, 1);
        record1.setField("long2", FieldType.LONG, 2);
        return record1;
    }
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
}
