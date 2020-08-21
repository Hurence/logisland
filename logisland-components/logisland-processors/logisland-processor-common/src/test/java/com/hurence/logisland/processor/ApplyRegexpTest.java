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

public class ApplyRegexpTest {

    private static final Logger logger = LoggerFactory.getLogger(ApplyRegexpTest.class);

    private Record getRecord() {
        Record record1 = new StandardRecord();
        record1.setField("location", FieldType.STRING, "https://www.mycompany.com/fr/search");
        record1.setField("description", FieldType.STRING, "text1-text2");
        record1.setField("long1", FieldType.LONG, 1);
        record1.setField("long2", FieldType.LONG, 2);
        return record1;
    }

    @Test
    public void testNoRegexp() {
        Record record1 = getRecord();

        TestRunner testRunner = TestRunners.newTestRunner(new ApplyRegexp());
        testRunner.setProperty("location", "companyName");
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(4);
    }

    @Test
    public void testEmptyRegexp() {
        Record record1 = getRecord();

        TestRunner testRunner = TestRunners.newTestRunner(new ApplyRegexp());
        testRunner.setProperty("location", "companyName:");
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(4);
    }

    @Test
    public void testRegexpWithoutGroup() {
        Record record1 = getRecord();

        TestRunner testRunner = TestRunners.newTestRunner(new ApplyRegexp());
        testRunner.setProperty("location", "companyName:nogroup");
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(4);
    }


    @Test
    public void testRegexpSimple() {

        Record record1 = getRecord();

        TestRunner testRunner = TestRunners.newTestRunner(new ApplyRegexp());
        testRunner.setProperty("location", "companyName:https\\:\\/\\/www\\.(\\w+\\.\\w+)\\/?.*");
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(5);
        out.assertFieldEquals("companyName", "mycompany.com");
    }

    @Test
    public void testRegexpMultiple() {

        Record record1 = getRecord();

        TestRunner testRunner = TestRunners.newTestRunner(new ApplyRegexp());
        testRunner.setProperty("location", "companyName:https\\:\\/\\/www\\.(\\w+\\.\\w+)\\/?.*");
        testRunner.setProperty("description", "part1,part2:(.+)-(.+)");
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(7);
        out.assertFieldEquals("companyName", "mycompany.com");
        out.assertFieldEquals("part1", "text1");
        out.assertFieldEquals("part2", "text2");
    }

    @Test
    public void testRegexpMultipleMoreOutputVarsThanGroups() {

        Record record1 = getRecord();

        TestRunner testRunner = TestRunners.newTestRunner(new ApplyRegexp());
        testRunner.setProperty("location", "companyName:https\\:\\/\\/www\\.(\\w+\\.\\w+)\\/?.*");
        testRunner.setProperty("description", "part1,part2,part3:(.+)-(.+)");
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(7);
        out.assertFieldEquals("companyName", "mycompany.com");
        out.assertFieldEquals("part1", "text1");
        out.assertFieldEquals("part2", "text2");
    }

    @Test
    public void testIpRegexWithPort() {

        Record record1 = getRecord();
        record1.setField("host", FieldType.STRING, "84.209.99.184:8888");

        TestRunner testRunner = TestRunners.newTestRunner(new ApplyRegexp());
        testRunner.setProperty("host", "part1:((.+)(?=\\:)|\\b(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\b)");
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(6);
        out.assertFieldEquals("part1", "84.209.99.184");
    }

    @Test
    public void testIpRegexWithoutPort() {

        Record record1 = getRecord();
        record1.setField("remoteHost", FieldType.STRING, "84.209.99.184");

        TestRunner testRunner = TestRunners.newTestRunner(new ApplyRegexp());
        testRunner.setProperty("remoteHost", "remoteHost:((.+)(?=\\:)|\\b(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\b)");
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(5);
        out.assertFieldEquals("remoteHost", "84.209.99.184");
    }


    @Test
    public void testRegexpMultipleLessOutputVarsThanGroups() {

        Record record1 = getRecord();

        TestRunner testRunner = TestRunners.newTestRunner(new ApplyRegexp());
        testRunner.setProperty("location", "companyName:https\\:\\/\\/www\\.(\\w+\\.\\w+)\\/?.*");
        testRunner.setProperty("description", "part1:(.+)-(.+)");
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(6);
        out.assertFieldEquals("companyName", "mycompany.com");
        out.assertFieldEquals("part1", "text1");
    }
}
