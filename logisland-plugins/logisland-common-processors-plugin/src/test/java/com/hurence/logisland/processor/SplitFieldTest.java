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

public class SplitFieldTest {

    private Record getRecord() {
        Record record1 = new StandardRecord();
        record1.setField("location", FieldType.STRING, "https://www.mycompany.com/fr/search");
        record1.setField("description", FieldType.STRING, "text1-text2");
        record1.setField("description1", FieldType.STRING, "outil+meuleuse+rapide");
        record1.setField("description2", FieldType.STRING, "outil+meu.leu.se+rapide++");
        record1.setField("long1", FieldType.LONG, 1);
        record1.setField("long2", FieldType.LONG, 2);
        return record1;
    }

    @Test
    public void testNoRegexp() {
        Record record1 = getRecord();

        TestRunner testRunner = TestRunners.newTestRunner(new SplitField());
        testRunner.setProperty("location", "companyName");
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(6);
    }

    @Test
    public void testEmptyRegexp() {
        Record record1 = getRecord();

        TestRunner testRunner = TestRunners.newTestRunner(new SplitField());
        testRunner.setProperty("location", "companyName:");
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(7);
    }

    @Test
    public void testRegexpSimple() {

        Record record1 = getRecord();

        TestRunner testRunner = TestRunners.newTestRunner(new SplitField());
        //testRunner.setProperty("description", "textArray:https\\:\\/\\/www\\.(\\w+\\.\\w+)\\/?.*");
        testRunner.setProperty("description", "textArray:-");
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(7);
        //out.assertFieldEquals("companyName", "mycompany.com");
    }

    @Test
    public void testRegexpAdvanced01() {

        Record record1 = getRecord();

        TestRunner testRunner = TestRunners.newTestRunner(new SplitField());
        //testRunner.setProperty("description", "textArray:https\\:\\/\\/www\\.(\\w+\\.\\w+)\\/?.*");
        testRunner.setProperty("description1", "descArray:\\s*\\+\\s*");
        testRunner.setProperty("split.counter.enable","true");
        testRunner.setProperty("split.counter.suffix", "Counter");
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(8);
        out.assertFieldEquals("descArrayCounter", "3");
    }


    @Test
    public void testRegexpAdvanced02() {

        Record record1 = getRecord();

        TestRunner testRunner = TestRunners.newTestRunner(new SplitField());
        //testRunner.setProperty("description", "textArray:https\\:\\/\\/www\\.(\\w+\\.\\w+)\\/?.*");
        testRunner.setProperty("description2", "descArray:\\s*\\+\\s*");
        testRunner.setProperty("split.counter.enable","true");
        testRunner.setProperty("split.counter.suffix", "Counter");
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(8);
        out.assertFieldEquals("descArrayCounter", "5");
    }
}
