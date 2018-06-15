package com.hurence.logisland.processor;

import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.util.runner.MockRecord;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class OnlineLinearRegressionTest {

    private List<Record> getRecordList() {

        double[][] data = new double[][]{
                {1509692069,85802.85},
                {1509691169,85837.55},
                {1509690269,85876.15},
                {1509689369,85927.66},
                {1509688469,86607.3},
                {1509687569,86843.21},
                {1509686669,87480.77},
                {1509685769,87675.41},
                {1509684869,87767.05},
                {1509683969,87882.41}
        };

        String metricId = "72c8df96-c165-4ecb-b1a8-ef131e526d74";
        String metricName = "rta";
        String groupId = "9333fdcb-1f73-4073-b982-74480a016155";

        List<Record> res = new ArrayList<Record>();

        for(int i = 0 ; i < 10; i++){
            Record record = new StandardRecord();
            record.setField("metricId", FieldType.STRING, metricId);
            record.setField("metricName", FieldType.STRING, metricName);
            record.setField("groupId", FieldType.STRING, groupId);

            record.setField("timestamp", FieldType.LONG, data[i][0]);
            record.setField("value", FieldType.FLOAT, data[i][1]);
            res.add(record);
        }

        return res;
    }


    @Test
    public void testBasicLinearRegression(){

        List<Record> recordList = getRecordList();

        TestRunner testRunner = TestRunners.newTestRunner(new OnlineLinearRegression());
        testRunner.setProperty(OnlineLinearRegression.PREDICTION_HORIZON_SIZE, "36000");
        testRunner.setProperty(OnlineLinearRegression.TRAINING_HISTORY_SIZE, "10");
        testRunner.setProperty(OnlineLinearRegression.RECORD_TYPE, "metric_predcition");
        testRunner.assertValid();
        testRunner.enqueue(recordList);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);
        MockRecord outputRecord = testRunner.getOutputRecords().get(0);
        outputRecord.assertRecordSizeEquals(7);

    }


}
