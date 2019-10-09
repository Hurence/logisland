package com.hurence.logisland.processor;

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
        assertEquals(Base64.getEncoder().encodeToString(bytes), out.getField("encoded_field_1").asString());
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