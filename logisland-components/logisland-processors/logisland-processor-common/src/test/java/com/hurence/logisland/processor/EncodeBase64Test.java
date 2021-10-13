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
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.util.runner.MockRecord;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Test;

import java.util.Base64;

import static org.junit.jupiter.api.Assertions.*;

public class EncodeBase64Test {

    private TestRunner createTestRunner() {
        TestRunner testRunner = TestRunners.newTestRunner(new EncodeBase64());
        testRunner.setProcessorIdentifier("encode_base_64");
        testRunner.setProperty(DecodeBase64.SOURCE_FIELDS, "source_field_1");
        testRunner.setProperty(DecodeBase64.DESTINATION_FIELDS, "encoded_field_1");
        testRunner.assertValid();
        return testRunner;
    }

    @Test
    public void testValidProcessing() {
        TestRunner testRunner = createTestRunner();
        byte[] bytes = RandomUtils.nextBytes(100);
        testRunner.enqueue(new StandardRecord("test").setBytesField("source_field_1", bytes));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);
        testRunner.assertOutputRecordsCount(1);
        MockRecord out = testRunner.getOutputRecords().stream().findFirst().get();
        assertTrue(out.getErrors().isEmpty());
        assertNotNull(out.getField("encoded_field_1"));
        assertNotNull(out.getField("encoded_field_1").asString());
        out.assertFieldTypeEquals("encoded_field_1", FieldType.STRING);
        assertEquals(Base64.getEncoder().encodeToString(bytes), out.getField("encoded_field_1").asString());
    }

    @Test
    public void testValidProcessingOverwrite() {
        TestRunner testRunner = TestRunners.newTestRunner(new EncodeBase64());
        testRunner.setProcessorIdentifier("encode_base_64");
        testRunner.setProperty(DecodeBase64.SOURCE_FIELDS, "source_field_1");
        testRunner.setProperty(DecodeBase64.DESTINATION_FIELDS, "source_field_1");
        testRunner.assertValid();
        byte[] bytes = RandomUtils.nextBytes(100);
        testRunner.enqueue(new StandardRecord("test").setBytesField("source_field_1", bytes));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);
        testRunner.assertOutputRecordsCount(1);
        MockRecord out = testRunner.getOutputRecords().stream().findFirst().get();
        assertTrue(out.getErrors().isEmpty());
        assertNotNull(out.getField("source_field_1"));
        assertNotNull(out.getField("source_field_1").asString());
        out.assertFieldTypeEquals("source_field_1", FieldType.STRING);
        assertEquals(Base64.getEncoder().encodeToString(bytes), out.getField("source_field_1").asString());
    }

    @Test
    public void testErrorIfBadFieldType() {
        TestRunner testRunner = createTestRunner();
        testRunner.enqueue(new StandardRecord("test").setBooleanField("source_field_1", true));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(1);
        testRunner.assertOutputRecordsCount(0);
    }

}