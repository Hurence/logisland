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

import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.util.runner.MockRecord;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class ComputeAggsProcessorTest {

    private static final Logger logger = LoggerFactory.getLogger(ComputeAggsProcessorTest.class);

    @Test
    public void testValidity() {
        TestRunner testRunner = TestRunners.newTestRunner(new ComputeAggsProcessor());
        testRunner.setProcessorIdentifier("testProc");
        testRunner.assertNotValid();
        testRunner.setProperty("max", "long1");
        testRunner.setProperty("max.agg", "MAX");
        testRunner.assertNotValid();
        testRunner.setProperty("max.type", "LONG");
        testRunner.assertValid();
        testRunner.removeProperty("max.agg");
        testRunner.assertNotValid();
        testRunner.setProperty("max.agg", "MAX");
        testRunner.assertValid();
        testRunner.removeProperty("max");
        testRunner.assertNotValid();
    }

    @Test
    public void testAggs() {
        final List<Float> values = Arrays.asList(9.45f, 1f ,2f ,3f);
        Record record1 = new StandardRecord();
        record1.setField("string1", FieldType.STRING, "value1");
        record1.setField("string2", FieldType.STRING, "value2");
        record1.setField("values", FieldType.ARRAY, values);

        TestRunner testRunner = TestRunners.newTestRunner(new ComputeAggsProcessor());
        testRunner.setProcessorIdentifier("testProc");
        testRunner.setProperty("max", "values");
        testRunner.setProperty("max.agg", "MAX");
        testRunner.setProperty("max.type", "FLOAT");
        testRunner.setProperty("max2", "values");
        testRunner.setProperty("max2.agg", "MAX");
        testRunner.setProperty("max2.type", "LONG");
        testRunner.setProperty("min", "values");
        testRunner.setProperty("min.agg", "MIN");
        testRunner.setProperty("min.type", "FLOAT");
        testRunner.setProperty("min2", "values");
        testRunner.setProperty("min2.agg", "MIN");
        testRunner.setProperty("min2.type", "INT");
        testRunner.setProperty("avg", "values");
        testRunner.setProperty("avg.agg", "AVG");
        testRunner.setProperty("avg.type", "FLOAT");
        testRunner.setProperty("avg2", "values");
        testRunner.setProperty("avg2.agg", "AVG");
        testRunner.setProperty("avg2.type", "DOUBLE");
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        //first output
        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(9);
        out.assertFieldEquals("string1", "value1");
        out.assertFieldTypeEquals("string1", FieldType.STRING);
        out.assertFieldEquals("string2", "value2");
        out.assertFieldTypeEquals("string2", FieldType.STRING);
        out.assertFieldEquals("values", values);
        out.assertFieldTypeEquals("values", FieldType.ARRAY);
        //aggs
        out.assertFieldEquals("max", 9.45f);
        out.assertFieldTypeEquals("max", FieldType.FLOAT);
        out.assertFieldEquals("max2", 9L);
        out.assertFieldTypeEquals("max2", FieldType.LONG);
        out.assertFieldEquals("min", 1f);
        out.assertFieldTypeEquals("min", FieldType.FLOAT);
        out.assertFieldEquals("min2", 1);
        out.assertFieldTypeEquals("min2", FieldType.INT);
        out.assertFieldEquals("avg", 3.8625f);
        out.assertFieldTypeEquals("avg", FieldType.FLOAT);
        out.assertFieldEquals("avg2", 3.8625d);
        out.assertFieldTypeEquals("avg2", FieldType.DOUBLE);
    }
}
