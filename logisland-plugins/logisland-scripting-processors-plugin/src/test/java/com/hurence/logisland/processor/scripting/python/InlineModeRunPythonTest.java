/**
 * Copyright (C) 2016 Hurence 
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
package com.hurence.logisland.processor.scripting.python;

import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.util.runner.MockRecord;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test the ability of calling a python processor and executing some python code in the processor.
 * The python code is defined inline (inline mode).
 */
public class InlineModeRunPythonTest {
    
    private static Logger logger = LoggerFactory.getLogger(InlineModeRunPythonTest.class);

    private static String INDENT = "    ";
    private static final String CODE_PROCESS =
            INDENT + "print \"Inside process of inline processor python code\"\n"  +
            INDENT + "# Copy the records and add python_field field in it\n" +
            INDENT + "outputRecords = []\n" +
            INDENT + "for record in records:\n" +
            INDENT + "    copyRecord = StandardRecord(record)\n" +
            INDENT + "    # Check that one can read values coming from java\n" +
            INDENT + "    javaFieldValue = copyRecord.getField(\"java_field\").getRawValue()\n" +
            INDENT + "    expectedValue = \"java_field_value\"\n" +
            INDENT + "    assert (javaFieldValue == expectedValue) , \"Expected \" + expectedValue + \" but got \" + javaFieldValue\n" +
            INDENT + "    copyRecord.setStringField('python_field', 'python_field_value')\n" +
            INDENT + "    outputRecords.append(copyRecord)\n" +
            INDENT + "return outputRecords";
    
    @Test
    public void testSimple() {
        final TestRunner testRunner = TestRunners.newTestRunner(new RunPython());
        testRunner.setProperty(RunPython.SCRIPT_CODE_PROCESS, CODE_PROCESS);
        testRunner.assertValid();
        Record record = new StandardRecord("simple_record");
        record.setStringField("java_field", "java_field_value");
        testRunner.enqueue(record);
        testRunner.clearQueues();
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertFieldExists("java_field");
        out.assertFieldEquals("java_field", "java_field_value");
        // The python_field is added when processing the record from python code
        out.assertFieldExists("python_field");
        out.assertFieldEquals("python_field", "python_field_value");
        out.assertRecordSizeEquals(2);
    }

}
