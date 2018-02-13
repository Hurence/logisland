/**
 * Copyright (C) 2016 Hurence (support@hurence.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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
import com.hurence.logisland.util.runner.MockRecord;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import org.junit.Test;

public class FlatMapTest {

    private Record getRecord() {
        Record record1 = new StandardRecord();
        record1.setField("location", FieldType.STRING, "https://www.mycompany.com/fr/search");
        record1.setField("description", FieldType.STRING, "text1-text2");
        record1.setField("description1", FieldType.STRING, "outil+meuleuse+rapide");
        record1.setField("description2", FieldType.STRING, "outil+meu.leu.se+rapide++");


        record1.setField("metric1",
                FieldType.RECORD,
                new StandardRecord(FieldType.DOUBLE.getName())
                        .setField(FieldDictionary.RECORD_VALUE, FieldType.DOUBLE, 34.23));

        record1.setField("metric2",
                FieldType.RECORD,
                new StandardRecord(FieldType.INT.getName())
                        .setField(FieldDictionary.RECORD_VALUE, FieldType.INT, 3));

        return record1;
    }

    @Test
    public void basicValidation() {
        Record record1 = getRecord();

        TestRunner testRunner = TestRunners.newTestRunner(new FlatMap());
        testRunner.setProperty(FlatMap.COPY_ROOT_RECORD_FIELDS, "false");
        testRunner.setProperty(FlatMap.KEEP_ROOT_RECORD, "false");
        testRunner.setProperty(FlatMap.LEAF_RECORD_TYPE, "metric");
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(2);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(1);
    }

    @Test
    public void basicCopyRoot() {
        Record record1 = getRecord();

        TestRunner testRunner = TestRunners.newTestRunner(new FlatMap());
        testRunner.setProperty(FlatMap.COPY_ROOT_RECORD_FIELDS, "false");
        testRunner.setProperty(FlatMap.KEEP_ROOT_RECORD, "true");
        testRunner.setProperty(FlatMap.LEAF_RECORD_TYPE, "metric");
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(3);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(6);
    }

}
