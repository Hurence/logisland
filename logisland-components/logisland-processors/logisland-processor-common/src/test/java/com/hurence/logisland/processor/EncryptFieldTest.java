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

import java.util.HashMap;
import java.util.Map;

public class EncryptFieldTest {

    private static final Logger logger = LoggerFactory.getLogger(ModifyIdTest.class);

    private Record getRecord1() {
        Record record1 = new StandardRecord();
        record1.setField("string1", FieldType.STRING, "Logisland");
        record1.setField("string1", FieldType.RECORD, new StandardRecord());
        record1.setField("string2", FieldType.STRING, "Hello world !");
        return record1;
    }

    @Test
    public void testValidity() {
        final TestRunner testRunner = TestRunners.newTestRunner(new EncryptField());
        testRunner.assertValid();
        testRunner.setProperty(EncryptField.ALGO, "AE");
        testRunner.assertNotValid();
        testRunner.setProperty(EncryptField.ALGO, "AES");
        testRunner.assertValid();
        testRunner.setProperty(EncryptField.MODE, "azert");
        testRunner.assertNotValid();
        testRunner.setProperty(EncryptField.MODE, EncryptField.ENCRYPT_MODE);
        testRunner.assertValid();
        testRunner.setProperty(EncryptField.KEY, "azerty1234567890");
        testRunner.assertValid();
    }


    @Test
    public void testProcessingEncryptionString() {
        Record record1 = new StandardRecord();
        /*int a[] = {1, 2, 3};*/
        record1.setField("string1", FieldType.STRING, "Logisland");
        record1.setField("string2", FieldType.STRING, "Nouri");

        TestRunner testRunner = TestRunners.newTestRunner(new EncryptField());
        testRunner.setProperty(EncryptField.MODE, EncryptField.ENCRYPT_MODE);
        testRunner.setProperty(EncryptField.ALGO, "AES");
        testRunner.setProperty(EncryptField.KEY, "azerty1234567890");
        testRunner.setProperty("string1", "string");
        testRunner.setProperty("string2", "");
        testRunner.setProperty("string3", "");
        testRunner.setProcessorIdentifier("encrypt_1");
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(2);
        out.assertFieldTypeEquals("string1", FieldType.BYTES);
        out.assertFieldTypeEquals("string2", FieldType.BYTES);


        TestRunner testRunner2 = TestRunners.newTestRunner(new EncryptField());
        testRunner2.setProperty(EncryptField.MODE, EncryptField.DECRYPT_MODE);
        testRunner2.setProperty(EncryptField.ALGO, "AES");
        testRunner2.setProperty(EncryptField.KEY, "azerty1234567890");
        testRunner2.setProperty("string1", "string");
        testRunner2.setProperty("string2", "string");
        testRunner2.assertValid();
        testRunner2.enqueue(record1);
        testRunner2.run();
        testRunner2.assertAllInputRecordsProcessed();
        testRunner2.assertOutputRecordsCount(1);
        //TODO configure and decrypt

        MockRecord out1 = testRunner2.getOutputRecords().get(0);
        out1.assertRecordSizeEquals(2);
        out1.assertFieldTypeEquals("string1", FieldType.STRING);
        out1.assertFieldTypeEquals("string2", FieldType.STRING);
        /*byte[] expectedBytes = new byte[] {104, -35, -44, -34, -100, 49, 75, 15, 56, -8, 54, -58, -65, -8, 108, -106, 95, -59, -25, -99, 31, 27, 44, -13, -3, -35, 59, -61, -112, -128, -3, -113};*///TODO real array eventually use external tools to determine array of byte
        out1.assertFieldEquals("string1", "Logisland");
        out1.assertFieldEquals("string2", "Nouri");
    }

    @Test
    public void testProcessingDeccryptionString() {
        Record record1 = new StandardRecord();
        byte[] inputBytes = new byte[]{104, -35, -44, -34, -100, 49, 75, 15, 56, -8, 54, -58, -65, -8, 108, -106, 95, -59, -25, -99, 31, 27, 44, -13, -3, -35, 59, -61, -112, -128, -3, -113};//TODO real array eventually use external tools to determine array of byte
        record1.setField("string1", FieldType.BYTES, inputBytes);

        TestRunner testRunner = TestRunners.newTestRunner(new EncryptField());
        testRunner.setProperty(EncryptField.MODE, EncryptField.DECRYPT_MODE);
        testRunner.setProperty(EncryptField.ALGO, "AES/ECB/PKCS5Padding");
        testRunner.setProperty(EncryptField.KEY, "azerty1234567890");
        testRunner.setProperty("string1", "string");
        testRunner.setProcessorIdentifier("encrypt_1");
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(1);

        out.assertFieldTypeEquals("string1", FieldType.STRING);
        out.assertFieldEquals("string1", "Logisland");
    }

    @Test
    public void testProcessingEncryptionInteger() {
        Record record1 = new StandardRecord();
        /*int a[] = {1, 2, 3};*/
        record1.setField("string1", FieldType.INT, 1994);
        record1.setField("string2", FieldType.INT, 987654321);

        TestRunner testRunner = TestRunners.newTestRunner(new EncryptField());
        testRunner.setProperty(EncryptField.MODE, EncryptField.ENCRYPT_MODE);
        testRunner.setProperty(EncryptField.ALGO, "AES");
        testRunner.setProperty(EncryptField.KEY, "azerty1234567890");
        testRunner.setProperty("string1", "int");
        testRunner.setProperty("string2", "");
        testRunner.setProperty("string3", "");
        testRunner.setProcessorIdentifier("encrypt_1");
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(2);
        out.assertFieldTypeEquals("string1", FieldType.BYTES);
        out.assertFieldTypeEquals("string2", FieldType.BYTES);


        TestRunner testRunner2 = TestRunners.newTestRunner(new EncryptField());
        testRunner2.setProperty(EncryptField.MODE, EncryptField.DECRYPT_MODE);
        testRunner2.setProperty(EncryptField.ALGO, "AES");
        testRunner2.setProperty(EncryptField.KEY, "azerty1234567890");
        testRunner2.setProperty("string1", "int");
        testRunner2.setProperty("string2", "int");
        testRunner2.assertValid();
        testRunner2.enqueue(record1);
        testRunner2.run();
        testRunner2.assertAllInputRecordsProcessed();
        testRunner2.assertOutputRecordsCount(1);
        //TODO configure and decrypt

        MockRecord out1 = testRunner2.getOutputRecords().get(0);
        out1.assertRecordSizeEquals(2);
        out1.assertFieldTypeEquals("string1", FieldType.INT);
        out1.assertFieldTypeEquals("string2", FieldType.INT);
        out1.assertFieldEquals("string1", 1994);
        out1.assertFieldEquals("string2", 987654321);
    }

    @Test
    public void testProcessingEncryptionLong() {
        Record record1 = new StandardRecord();
        /*int a[] = {1, 2, 3};*/
        record1.setField("string1", FieldType.LONG, 19941994);
        record1.setField("string2", FieldType.LONG, 1234567890123L);

        TestRunner testRunner = TestRunners.newTestRunner(new EncryptField());
        testRunner.setProperty(EncryptField.MODE, EncryptField.ENCRYPT_MODE);
        testRunner.setProperty(EncryptField.ALGO, "AES");
        testRunner.setProperty(EncryptField.KEY, "azerty1234567890");
        testRunner.setProperty("string1", "long");
        testRunner.setProperty("string2", "");
        testRunner.setProperty("string3", "");
        testRunner.setProcessorIdentifier("encrypt_1");
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(2);
        out.assertFieldTypeEquals("string1", FieldType.BYTES);
        out.assertFieldTypeEquals("string2", FieldType.BYTES);


        TestRunner testRunner2 = TestRunners.newTestRunner(new EncryptField());
        testRunner2.setProperty(EncryptField.MODE, EncryptField.DECRYPT_MODE);
        testRunner2.setProperty(EncryptField.ALGO, "AES");
        testRunner2.setProperty(EncryptField.KEY, "azerty1234567890");
        testRunner2.setProperty("string1", "long");
        testRunner2.setProperty("string2", "long");
        testRunner2.assertValid();
        testRunner2.enqueue(record1);
        testRunner2.run();
        testRunner2.assertAllInputRecordsProcessed();
        testRunner2.assertOutputRecordsCount(1);
        //TODO configure and decrypt

        MockRecord out1 = testRunner2.getOutputRecords().get(0);
        out1.assertRecordSizeEquals(2);
        out1.assertFieldTypeEquals("string1", FieldType.LONG);
        out1.assertFieldTypeEquals("string2", FieldType.LONG);
        out1.assertFieldEquals("string1", 19941994);
        out1.assertFieldEquals("string2", 1234567890123L);
    }

    @Test
    public void testProcessingEncryptionBytes() {
        Record record1 = new StandardRecord();
        byte[] a = {7, 75, 15, 56,-3, -35, 59, -61, -112, -128, -3, -113};
        byte[] b = {5, -58, 64, 12, -3};
        record1.setField("string1", FieldType.BYTES, a);
        record1.setField("string2", FieldType.BYTES, b);

        TestRunner testRunner = TestRunners.newTestRunner(new EncryptField());
        testRunner.setProperty(EncryptField.MODE, EncryptField.ENCRYPT_MODE);
        testRunner.setProperty(EncryptField.ALGO, "AES");
        testRunner.setProperty(EncryptField.KEY, "azerty1234567890");
        testRunner.setProperty("string1", "byte");
        testRunner.setProperty("string2", "");
        testRunner.setProperty("string3", "");
        testRunner.setProcessorIdentifier("encrypt_1");
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(2);
        out.assertFieldTypeEquals("string1", FieldType.BYTES);
        out.assertFieldTypeEquals("string2", FieldType.BYTES);


        TestRunner testRunner2 = TestRunners.newTestRunner(new EncryptField());
        testRunner2.setProperty(EncryptField.MODE, EncryptField.DECRYPT_MODE);
        testRunner2.setProperty(EncryptField.ALGO, "AES");
        testRunner2.setProperty(EncryptField.KEY, "azerty1234567890");
        testRunner2.setProperty("string1", "bytes");
        testRunner2.setProperty("string2", "bytes");
        testRunner2.assertValid();
        testRunner2.enqueue(record1);
        testRunner2.run();
        testRunner2.assertAllInputRecordsProcessed();
        testRunner2.assertOutputRecordsCount(1);
        //TODO configure and decrypt

        MockRecord out1 = testRunner2.getOutputRecords().get(0);
        out1.assertRecordSizeEquals(2);
        out1.assertFieldTypeEquals("string1", FieldType.BYTES);
        out1.assertFieldTypeEquals("string2", FieldType.BYTES);
        out1.assertFieldEquals("string1", a);
        out1.assertFieldEquals("string2", b);
    }

    @Test
    public void testProcessingEncryptionRecord() {
        Record record1 = new StandardRecord();
        Record record2 = new StandardRecord();
        Record record3 = new StandardRecord();
        record1.setField("string1", FieldType.RECORD, record2);
        record1.setField("string2", FieldType.RECORD, record3);

        TestRunner testRunner = TestRunners.newTestRunner(new EncryptField());
        testRunner.setProperty(EncryptField.MODE, EncryptField.ENCRYPT_MODE);
        testRunner.setProperty(EncryptField.ALGO, "AES");
        testRunner.setProperty(EncryptField.KEY, "azerty1234567890");
        testRunner.setProperty("string1", "record");
        testRunner.setProperty("string2", "");
        testRunner.setProperty("string3", "");
        testRunner.setProcessorIdentifier("encrypt_1");
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(2);
        out.assertFieldTypeEquals("string1", FieldType.BYTES);
        out.assertFieldTypeEquals("string2", FieldType.BYTES);


        TestRunner testRunner2 = TestRunners.newTestRunner(new EncryptField());
        testRunner2.setProperty(EncryptField.MODE, EncryptField.DECRYPT_MODE);
        testRunner2.setProperty(EncryptField.ALGO, "AES");
        testRunner2.setProperty(EncryptField.KEY, "azerty1234567890");
        testRunner2.setProperty("string1", "record");
        testRunner2.setProperty("string2", "record");
        testRunner2.assertValid();
        testRunner2.enqueue(record1);
        testRunner2.run();
        testRunner2.assertAllInputRecordsProcessed();
        testRunner2.assertOutputRecordsCount(1);
        //TODO configure and decrypt

        MockRecord out1 = testRunner2.getOutputRecords().get(0);
        out1.assertRecordSizeEquals(2);
        out1.assertFieldTypeEquals("string1", FieldType.RECORD);
        out1.assertFieldTypeEquals("string2", FieldType.RECORD);
        /*out1.assertFieldEquals("string1", record2);
        out1.assertFieldEquals("string2", record3);*/
    }

    @Test
    public void testProcessingEncryptionMap() {
        Record record1 = new StandardRecord();
        Map map1 = new HashMap();
        map1.put("Germany", "Berlin");
        map1.put("Spain", "Madrid");
        map1.put("Greece", "Athens");
        map1.put("Turkey", "Ankara");
        Map map2 = new HashMap();

        map2.put("A", "1");
        map2.put("B", "2");
        map2.put("C", "3");
        record1.setField("string1", FieldType.MAP, map1);
        record1.setField("string2", FieldType.MAP, map2);

        TestRunner testRunner = TestRunners.newTestRunner(new EncryptField());
        testRunner.setProperty(EncryptField.MODE, EncryptField.ENCRYPT_MODE);
        testRunner.setProperty(EncryptField.ALGO, "AES");
        testRunner.setProperty(EncryptField.KEY, "azerty1234567890");
        testRunner.setProperty("string1", "map");
        testRunner.setProperty("string2", "");
        testRunner.setProperty("string3", "");
        testRunner.setProcessorIdentifier("encrypt_1");
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(2);
        out.assertFieldTypeEquals("string1", FieldType.BYTES);
        out.assertFieldTypeEquals("string2", FieldType.BYTES);


        TestRunner testRunner2 = TestRunners.newTestRunner(new EncryptField());
        testRunner2.setProperty(EncryptField.MODE, EncryptField.DECRYPT_MODE);
        testRunner2.setProperty(EncryptField.ALGO, "AES");
        testRunner2.setProperty(EncryptField.KEY, "azerty1234567890");
        testRunner2.setProperty("string1", "map");
        testRunner2.setProperty("string2", "map");
        testRunner2.assertValid();
        testRunner2.enqueue(record1);
        testRunner2.run();
        testRunner2.assertAllInputRecordsProcessed();
        testRunner2.assertOutputRecordsCount(1);
        //TODO configure and decrypt

        MockRecord out1 = testRunner2.getOutputRecords().get(0);
        out1.assertRecordSizeEquals(2);
        out1.assertFieldTypeEquals("string1", FieldType.MAP);
        out1.assertFieldTypeEquals("string2", FieldType.MAP);
        out1.assertFieldEquals("string1", map1);
        out1.assertFieldEquals("string2", map2);
    }


}
