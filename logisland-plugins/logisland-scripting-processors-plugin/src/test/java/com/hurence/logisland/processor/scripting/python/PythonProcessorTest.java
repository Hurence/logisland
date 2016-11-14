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

public class PythonProcessorTest {
    
    private static Logger logger = LoggerFactory.getLogger(PythonProcessorTest.class);
    
    private static final String PYTHON_PROCESSOR = "src/main/python/MyProcessor.py";

    @Test
    public void testSimple() {
        final TestRunner testRunner = TestRunners.newTestRunner(new PythonProcessor());
        testRunner.setProperty(PythonProcessor.PYTHON_PROCESSOR_SCRIPT, PYTHON_PROCESSOR);
        testRunner.assertValid();
        Record record = new StandardRecord("simple_record"); 
        testRunner.enqueue(record);
        testRunner.clearQueues();
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(0);


//        MockRecord out = testRunner.getOutputRecords().get(0);
//        out.assertFieldExists("src_ip");
//        out.assertFieldNotExists("src_ip2");
//        out.assertFieldEquals("src_ip", "10.3.10.134");
//        out.assertRecordSizeEquals(9);
    }

}
