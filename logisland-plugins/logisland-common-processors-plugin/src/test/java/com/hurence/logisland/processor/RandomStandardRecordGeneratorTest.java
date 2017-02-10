/**
 * Copyright (C) 2016 Hurence (bailet.thomas@gmail.com)
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

import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;


public class RandomStandardRecordGeneratorTest {

    private static final Logger logger = LoggerFactory.getLogger(RandomStandardRecordGeneratorTest.class);

    @Test
    public void testLoadConfig() throws Exception {

    	String avroSchema = loadResurceFileToString("/schemas/testLoadConfig-schema.json");
    	
        final TestRunner testRunner = TestRunners.newTestRunner(new GenerateRandomRecord());
        testRunner.setProperty(GenerateRandomRecord.OUTPUT_SCHEMA.getName(), avroSchema);
        testRunner.setProperty(GenerateRandomRecord.MIN_EVENTS_COUNT.getName(), "5");
        testRunner.setProperty(GenerateRandomRecord.MAX_EVENTS_COUNT.getName(), "20");

        testRunner.assertValid();
        testRunner.clearQueues();
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();

        Assert.assertTrue(testRunner.getOutputRecords().size() <= 20);
        Assert.assertTrue(testRunner.getOutputRecords().size() >= 5);
    }

    @Test
    public void testUnionTypeInSchema() throws Exception {

        String avroSchema = loadResurceFileToString("/schemas/testSchemaWithUnion.json");

        final TestRunner testRunner = TestRunners.newTestRunner(new GenerateRandomRecord());
        testRunner.setProperty(GenerateRandomRecord.OUTPUT_SCHEMA.getName(), avroSchema);
        testRunner.setProperty(GenerateRandomRecord.MIN_EVENTS_COUNT.getName(), "1");
        testRunner.setProperty(GenerateRandomRecord.MAX_EVENTS_COUNT.getName(), "2");

        testRunner.assertValid();
        testRunner.clearQueues();
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();

        Assert.assertTrue(testRunner.getOutputRecords().size() <= 2);
        Assert.assertTrue(testRunner.getOutputRecords().size() >= 1);
    }

	private String loadResurceFileToString(String resourceFile) throws Exception {
		Path resourcePath = Paths.get(getClass().getResource(resourceFile).toURI());
		return FileUtils.readFileToString(resourcePath.toFile());
	}


}
