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
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;

public class DebugStreamTest {

    @Test
    public void testLogOfDebugStream() {
        Collection<Record> records = new ArrayList<>();


        records.add(new StandardRecord()
                .setField("a", FieldType.STRING, "a1")
                .setField("b", FieldType.STRING, "b1")
                .setField("c", FieldType.LONG, 1));

        records.add(new StandardRecord()
                .setField("a", FieldType.STRING, "a2")
                .setField("b", FieldType.STRING, "b2")
                .setField("c", FieldType.LONG, 2));

        records.add(new StandardRecord()
                .setField("a", FieldType.STRING, "a3")
                .setField("b", FieldType.STRING, "b3")
                .setField("c", FieldType.LONG, 3));

        TestRunner testRunner = TestRunners.newTestRunner(new DebugStream());
        testRunner.setProcessorIdentifier("debug_1");
        testRunner.assertValid();
        testRunner.enqueue(records);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(3);
        testRunner.assertOutputErrorCount(0);
        testRunner.enqueue(records);
        testRunner.run();
        testRunner.assertOutputRecordsCount(6);
        testRunner.enqueue(records);
        testRunner.run();
        testRunner.assertOutputRecordsCount(9);
        testRunner.enqueue(records);
        testRunner.run();
        testRunner.assertOutputRecordsCount(12);
        testRunner.enqueue(records);
        testRunner.run();
        testRunner.assertOutputRecordsCount(15);
        testRunner.enqueue(records);
        testRunner.run();
        testRunner.assertOutputRecordsCount(18);
        testRunner.enqueue(records);
        testRunner.run();
        testRunner.assertOutputRecordsCount(21);
    }
}
