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
import com.hurence.logisland.util.string.Multiline;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RandomStandardRecordGeneratorTest {

    private static Logger logger = LoggerFactory.getLogger(RandomStandardRecordGeneratorTest.class);


    /**
     * {
     * "version": 1,
     * "type": "record",
     * "namespace": "com.hurence.logisland",
     * "name": "Event",
     * "fields": [
     * {
     * "name": "_type",
     * "type": "string"
     * },
     * {
     * "name": "_id",
     * "type": "string"
     * },
     * {
     * "name": "timestamp",
     * "type": "long"
     * },
     * {
     * "name": "method",
     * "type": "string"
     * },
     * {
     * "name": "ipSource",
     * "type": "string"
     * },
     * {
     * "name": "ipTarget",
     * "type": "string"
     * },
     * {
     * "name": "urlScheme",
     * "type": "string"
     * },
     * {
     * "name": "urlHost",
     * "type": "string"
     * },
     * {
     * "name": "urlPort",
     * "type": "string"
     * },
     * {
     * "name": "urlPath",
     * "type": "string"
     * },
     * {
     * "name": "requestSize",
     * "type": "int"
     * },
     * {
     * "name": "responseSize",
     * "type": "int"
     * },
     * {
     * "name": "isOutsideOfficeHours",
     * "type": "boolean"
     * },
     * {
     * "name": "isHostBlacklisted",
     * "type": "boolean"
     * },
     * {
     * "name": "tags",
     * "type": {
     * "type": "array",
     * "items": "string"
     * }
     * }
     * ]
     * }
     */
    @Multiline
    public String avroSchema;


    @Test
    public void testLoadConfig() throws Exception {

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
}
