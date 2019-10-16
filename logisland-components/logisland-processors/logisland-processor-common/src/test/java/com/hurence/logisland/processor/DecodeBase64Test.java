package com.hurence.logisland.processor;

import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.util.runner.MockRecord;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Test;

import java.util.Base64;

import static org.junit.jupiter.api.Assertions.*;

public class DecodeBase64Test {

    private TestRunner createTestRunner() {
        TestRunner testRunner = TestRunners.newTestRunner(new DecodeBase64());
        testRunner.setProcessorIdentifier("decode_base_64");
        testRunner.setProperty(DecodeBase64.SOURCE_FIELDS, "encoded_field_1");
        testRunner.setProperty(DecodeBase64.DESTINATION_FIELDS, "decoded_field_1");
        testRunner.assertValid();
        return testRunner;
    }

    @Test
    public void testValidProcessing() {
        TestRunner testRunner = createTestRunner();
        byte[] bytes = RandomUtils.nextBytes(100);
        testRunner.enqueue(new StandardRecord("test").setStringField("encoded_field_1",
                Base64.getEncoder().encodeToString(bytes)));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);
        testRunner.assertOutputRecordsCount(1);
        MockRecord out = testRunner.getOutputRecords().stream().findFirst().get();
        assertTrue(out.getErrors().isEmpty());
        assertNotNull(out.getField("decoded_field_1"));
        assertNotNull(out.getField("decoded_field_1").asBytes());
        assertArrayEquals(bytes, out.getField("decoded_field_1").asBytes());
    }

    @Test
    public void testErrorIfBadFieldType() {
        TestRunner testRunner = createTestRunner();
        testRunner.enqueue(new StandardRecord("test").setBooleanField("encoded_field_1", true));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(1);
        testRunner.assertOutputRecordsCount(0);
    }

}