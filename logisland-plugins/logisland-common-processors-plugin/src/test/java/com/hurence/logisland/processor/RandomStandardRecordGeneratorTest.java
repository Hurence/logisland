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

import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;


public class RandomStandardRecordGeneratorTest {

    private static final Logger logger = LoggerFactory.getLogger(RandomStandardRecordGeneratorTest.class);

    @Test
    public void testLoadConfig() throws Exception {

    	String avroSchema = loadResurceFileToString("/schemas/testLoadConfig-schema.json");
    	
        final TestRunner testRunner = TestRunners.newTestRunner(new RandomRecordGenerator());
        testRunner.setProperty(RandomRecordGenerator.OUTPUT_SCHEMA.getName(), avroSchema);
        testRunner.setProperty(RandomRecordGenerator.MIN_EVENTS_COUNT.getName(), "5");
        testRunner.setProperty(RandomRecordGenerator.MAX_EVENTS_COUNT.getName(), "20");

        testRunner.assertValid();
        testRunner.clearQueues();
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();

        Assert.assertTrue(testRunner.getOutputRecords().size() <= 20);
        Assert.assertTrue(testRunner.getOutputRecords().size() >= 5);
    }

	private String loadResurceFileToString(String resourceFile) throws Exception {
		Path resourcePath = Paths.get(getClass().getResource(resourceFile).toURI());
		return FileUtils.readFileToString(resourcePath.toFile());
	}
}
