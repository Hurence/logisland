package com.hurence.logisland.processor;

import com.hurence.logisland.processor.encryption.EncryptionMethod;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.util.runner.MockRecord;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class EncryptFieldTest {

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
        testRunner.setProperty(EncryptField.ALGO, "rsa");
        testRunner.assertNotValid();
        testRunner.setProperty(EncryptField.ALGO, "RSA");
        testRunner.assertValid();
        testRunner.setProperty(EncryptField.KEY, "123");
        testRunner.assertValid();
        testRunner.setProperty(EncryptField.KEY, "azerty1234567890");
        testRunner.assertValid();
        testRunner.setProperty(EncryptField.KEYFILE, "/home/ubuntu/public.der");
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
        record1.setField("string1", FieldType.STRING, "Logisland1234567");
        record1.setField("string2", FieldType.STRING, "Nouri12345678900");

        TestRunner testRunner = TestRunners.newTestRunner(new EncryptField());
        testRunner.setProperty(EncryptField.MODE, EncryptField.ENCRYPT_MODE);
        testRunner.setProperty(EncryptField.ALGO, "RSA/ECB/OAEPWithSHA-256AndMGF1Padding");
        /*testRunner.setProperty(EncryptField.KEY, "azerty1234567890");*/
        /*testRunner.setProperty(EncryptField.IV, "1234567812345678");*/
        testRunner.setProperty(EncryptField.KEYFILE, "/home/ubuntu/public.der");
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
        testRunner2.setProperty(EncryptField.ALGO, "RSA/ECB/OAEPWithSHA-256AndMGF1Padding");
        /*testRunner2.setProperty(EncryptField.KEY, "azerty1234567890");*/
        /*testRunner2.setProperty(EncryptField.IV, "1234567812345678");*/
        testRunner2.setProperty(EncryptField.KEYFILE, "/home/ubuntu/private.der");
        testRunner2.setProperty("string1", "string");
        testRunner2.setProperty("string2", "string");
        testRunner2.assertValid();
        testRunner2.enqueue(out);
        testRunner2.run();
        testRunner2.assertAllInputRecordsProcessed();
        testRunner2.assertOutputRecordsCount(1);

        MockRecord out1 = testRunner2.getOutputRecords().get(0);
        out1.assertRecordSizeEquals(2);
        out1.assertFieldTypeEquals("string1", FieldType.STRING);
        out1.assertFieldTypeEquals("string2", FieldType.STRING);
        out1.assertFieldEquals("string1", "Logisland1234567");
        out1.assertFieldEquals("string2", "Nouri12345678900");
    }

    @Test
    public void testProcessingDecryptionString() {
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
        record1.setField("string1", FieldType.INT, 199419441);
        record1.setField("string2", FieldType.INT, 987654321);

        TestRunner testRunner = TestRunners.newTestRunner(new EncryptField());
        testRunner.setProperty(EncryptField.MODE, EncryptField.ENCRYPT_MODE);
        testRunner.setProperty(EncryptField.ALGO, "DES/CBC/PKCS5Padding");
        testRunner.setProperty(EncryptField.KEY, "azerty1234567890");
        testRunner.setProperty(EncryptField.IV, "1234567890123456");
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
        testRunner2.setProperty(EncryptField.ALGO, "DES/CBC/PKCS5Padding");
        testRunner2.setProperty(EncryptField.KEY, "azerty1234567890");
        testRunner2.setProperty(EncryptField.IV, "1234567890123456");
        testRunner2.setProperty("string1", "int");
        testRunner2.setProperty("string2", "int");
        testRunner2.assertValid();
        testRunner2.enqueue(out);
        testRunner2.run();
        testRunner2.assertAllInputRecordsProcessed();
        testRunner2.assertOutputRecordsCount(1);

        MockRecord out1 = testRunner2.getOutputRecords().get(0);
        out1.assertRecordSizeEquals(2);
        out1.assertFieldTypeEquals("string1", FieldType.INT);
        out1.assertFieldTypeEquals("string2", FieldType.INT);
        out1.assertFieldEquals("string1", 199419441);
        out1.assertFieldEquals("string2", 987654321);
    }

    @Test
    public void testProcessingEncryptionLong() {
        Record record1 = new StandardRecord();
        record1.setField("string1", FieldType.LONG, 1994197917494971264L);
        record1.setField("string2", FieldType.LONG, 1234567811262490123L);

        TestRunner testRunner = TestRunners.newTestRunner(new EncryptField());
        testRunner.setProperty(EncryptField.MODE, EncryptField.ENCRYPT_MODE);
        testRunner.setProperty(EncryptField.ALGO, "RSA/ECB/OAEPWithSHA-256AndMGF1Padding");
        testRunner.setProperty(EncryptField.KEY, "azerty1234567890");
        testRunner.setProperty(EncryptField.KEYFILE, "/home/ubuntu/public.der");
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
        testRunner2.setProperty(EncryptField.ALGO, "RSA/ECB/OAEPWithSHA-256AndMGF1Padding");
        testRunner2.setProperty(EncryptField.KEY, "azerty1234567890");
        testRunner2.setProperty(EncryptField.KEYFILE, "/home/ubuntu/private.der");
        testRunner2.setProperty("string1", "long");
        testRunner2.setProperty("string2", "long");
        testRunner2.assertValid();
        testRunner2.enqueue(out);
        testRunner2.run();
        testRunner2.assertAllInputRecordsProcessed();
        testRunner2.assertOutputRecordsCount(1);

        MockRecord out1 = testRunner2.getOutputRecords().get(0);
        out1.assertRecordSizeEquals(2);
        out1.assertFieldTypeEquals("string1", FieldType.LONG);
        out1.assertFieldTypeEquals("string2", FieldType.LONG);
        out1.assertFieldEquals("string1", 1994197917494971264L);
        out1.assertFieldEquals("string2", 1234567811262490123L);
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
        testRunner.setProperty(EncryptField.ALGO, "AES/CBC/PKCS5Padding");
        testRunner.setProperty(EncryptField.KEY, "azerty1234567890");
        testRunner.setProperty(EncryptField.IV, "azerty1234567891");
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
        testRunner2.setProperty(EncryptField.ALGO, "AES/CBC/PKCS5Padding");
        testRunner2.setProperty(EncryptField.KEY, "azerty1234567890");
        testRunner2.setProperty(EncryptField.IV, "azerty1234567891");
        testRunner2.setProperty("string1", "bytes");
        testRunner2.setProperty("string2", "bytes");
        testRunner2.assertValid();
        testRunner2.enqueue(out);
        testRunner2.run();
        testRunner2.assertAllInputRecordsProcessed();
        testRunner2.assertOutputRecordsCount(1);

        MockRecord out1 = testRunner2.getOutputRecords().get(0);
        out1.assertRecordSizeEquals(2);
        out1.assertFieldTypeEquals("string1", FieldType.BYTES);
        out1.assertFieldTypeEquals("string2", FieldType.BYTES);
        /*out1.assertFieldEquals("string1", a);
        out1.assertFieldEquals("string2", b);*/
        Assert.assertTrue(Arrays.equals(a, (byte[]) out1.getField("string1").getRawValue()));
        Assert.assertTrue(Arrays.equals(b, (byte[]) out1.getField("string2").getRawValue()));
    }

    @Test
    public void testProcessingEncryptionRecord() {
        final Record record1Expected = new StandardRecord();
        Record record2 = new StandardRecord();
        Record record3 = new StandardRecord();
        record2.setField("string9", FieldType.STRING, "Nouri");
        record3.setField("string9", FieldType.STRING, "Feiz");
        record1Expected.setField("string1", FieldType.RECORD, record2);
        record1Expected.setField("string2", FieldType.RECORD, record3);

        TestRunner testRunner = TestRunners.newTestRunner(new EncryptField());
        testRunner.setProperty(EncryptField.MODE, EncryptField.ENCRYPT_MODE);
        testRunner.setProperty(EncryptField.ALGO, "DES/ECB/PKCS5Padding");
        testRunner.setProperty(EncryptField.KEY, "azerty1234567890");
        testRunner.setProperty("string1", "record");
        testRunner.setProperty("string2", "");
        testRunner.setProperty("string3", "");
        testRunner.setProcessorIdentifier("encrypt_1");
        testRunner.assertValid();
        testRunner.enqueue(new StandardRecord(record1Expected));//as processor modify inputs records we clone expected record so it is not modified
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(2);
        out.assertFieldTypeEquals("string1", FieldType.BYTES);
        out.assertFieldTypeEquals("string2", FieldType.BYTES);


        TestRunner testRunner2 = TestRunners.newTestRunner(new EncryptField());
        testRunner2.setProperty(EncryptField.MODE, EncryptField.DECRYPT_MODE);
        testRunner2.setProperty(EncryptField.ALGO, "DES/ECB/PKCS5Padding");
        testRunner2.setProperty(EncryptField.KEY, "azerty1234567890");
        testRunner2.setProperty("string1", "record");
        testRunner2.setProperty("string2", "record");
        testRunner2.assertValid();
        testRunner2.enqueue(out);
        testRunner2.run();
        testRunner2.assertAllInputRecordsProcessed();
        testRunner2.assertOutputRecordsCount(1);

        MockRecord out1 = testRunner2.getOutputRecords().get(0);
        out1.assertRecordSizeEquals(2);
        out1.assertFieldTypeEquals("string1", FieldType.RECORD);
        out1.assertFieldTypeEquals("string2", FieldType.RECORD);
        out1.assertContentEquals(record1Expected);
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
        testRunner.setProperty(EncryptField.ALGO, "AES/ECB/PKCS5Padding");
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
        testRunner2.setProperty(EncryptField.ALGO, "AES/ECB/PKCS5Padding");
        testRunner2.setProperty(EncryptField.KEY, "azerty1234567890");
        testRunner2.setProperty("string1", "map");
        testRunner2.setProperty("string2", "map");
        testRunner2.assertValid();
        testRunner2.enqueue(out);
        testRunner2.run();
        testRunner2.assertAllInputRecordsProcessed();
        testRunner2.assertOutputRecordsCount(1);

        MockRecord out1 = testRunner2.getOutputRecords().get(0);
        out1.assertRecordSizeEquals(2);
        out1.assertFieldTypeEquals("string1", FieldType.MAP);
        out1.assertFieldTypeEquals("string2", FieldType.MAP);
        out1.assertFieldEquals("string1", map1);
        out1.assertFieldEquals("string2", map2);
    }

    @Test
    public void testProcessingEncryptionFloat() {
        Record record1 = new StandardRecord();

        float a = 3.215f;
        float b =454.54f;
        record1.setField("string1", FieldType.FLOAT, a);
        record1.setField("string2", FieldType.FLOAT, b);

        TestRunner testRunner = TestRunners.newTestRunner(new EncryptField());
        testRunner.setProperty(EncryptField.MODE, EncryptField.ENCRYPT_MODE);
        testRunner.setProperty(EncryptField.ALGO, "AES");
        testRunner.setProperty(EncryptField.KEY, "azerty1234567890");
        testRunner.setProperty("string1", "float");
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
        testRunner2.setProperty("string1", "float");
        testRunner2.setProperty("string2", "float");
        testRunner2.assertValid();
        testRunner2.enqueue(out);
        testRunner2.run();
        testRunner2.assertAllInputRecordsProcessed();
        testRunner2.assertOutputRecordsCount(1);

        MockRecord out1 = testRunner2.getOutputRecords().get(0);
        out1.assertRecordSizeEquals(2);
        out1.assertFieldTypeEquals("string1", FieldType.FLOAT);
        out1.assertFieldTypeEquals("string2", FieldType.FLOAT);
        out1.assertFieldEquals("string1", a);
        out1.assertFieldEquals("string2", b);
    }

    @Test
    public void testProcessingEncryptionDouble() {
        Record record1 = new StandardRecord();

        double a = 3.215;
        double b =454.54;
        record1.setField("string1", FieldType.DOUBLE, a);
        record1.setField("string2", FieldType.DOUBLE, b);

        TestRunner testRunner = TestRunners.newTestRunner(new EncryptField());
        testRunner.setProperty(EncryptField.MODE, EncryptField.ENCRYPT_MODE);
        testRunner.setProperty(EncryptField.ALGO, "AES");
        testRunner.setProperty(EncryptField.KEY, "azerty1234567890");
        testRunner.setProperty("string1", "double");
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
        testRunner2.setProperty("string1", "double");
        testRunner2.setProperty("string2", "double");
        testRunner2.assertValid();
        testRunner2.enqueue(out);
        testRunner2.run();
        testRunner2.assertAllInputRecordsProcessed();
        testRunner2.assertOutputRecordsCount(1);

        MockRecord out1 = testRunner2.getOutputRecords().get(0);
        out1.assertRecordSizeEquals(2);
        out1.assertFieldTypeEquals("string1", FieldType.DOUBLE);
        out1.assertFieldTypeEquals("string2", FieldType.DOUBLE);
        out1.assertFieldEquals("string1", a);
        out1.assertFieldEquals("string2", b);
    }

    @Test
    public void testProcessingEncryptionBoolean() {
        Record record1 = new StandardRecord();

        record1.setField("string1", FieldType.BOOLEAN, true);
        record1.setField("string2", FieldType.BOOLEAN, false);

        TestRunner testRunner = TestRunners.newTestRunner(new EncryptField());
        testRunner.setProperty(EncryptField.MODE, EncryptField.ENCRYPT_MODE);
        testRunner.setProperty(EncryptField.ALGO, "AES");
        testRunner.setProperty(EncryptField.KEY, "azerty1234567890");
        testRunner.setProperty("string1", "boolean");
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
        testRunner2.setProperty("string1", "boolean");
        testRunner2.setProperty("string2", "boolean");
        testRunner2.assertValid();
        testRunner2.enqueue(out);
        testRunner2.run();
        testRunner2.assertAllInputRecordsProcessed();
        testRunner2.assertOutputRecordsCount(1);

        MockRecord out1 = testRunner2.getOutputRecords().get(0);
        out1.assertRecordSizeEquals(2);
        out1.assertFieldTypeEquals("string1", FieldType.BOOLEAN);
        out1.assertFieldTypeEquals("string2", FieldType.BOOLEAN);
        out1.assertFieldEquals("string1", true);
        out1.assertFieldEquals("string2", false);
    }

    @Test
    public void testProcessingEncryptionEnum() throws Exception{
        Record record1 = new StandardRecord();
        /*FieldType type = FieldType.ENUM;*/
        EncryptionMethod type = EncryptionMethod.AES_CTR;
        EncryptionMethod method = EncryptionMethod.AES_CBC;

        record1.setField("string1", FieldType.ENUM, type);
        record1.setField("string2", FieldType.ENUM, method);

        TestRunner testRunner = TestRunners.newTestRunner(new EncryptField());
        testRunner.setProperty(EncryptField.MODE, EncryptField.ENCRYPT_MODE);
        testRunner.setProperty(EncryptField.ALGO, "AES");
        testRunner.setProperty(EncryptField.KEY, "azerty1234567890");
        testRunner.setProperty("string1", "enum");
        testRunner.setProperty("string2", "");
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
        testRunner2.setProperty("string1", "enum");
        testRunner2.setProperty("string2", "enum");
        testRunner2.assertValid();
        testRunner2.enqueue(out);
        testRunner2.run();
        testRunner2.assertAllInputRecordsProcessed();
        testRunner2.assertOutputRecordsCount(1);

        MockRecord out1 = testRunner2.getOutputRecords().get(0);
        out1.assertRecordSizeEquals(2);
        out1.assertFieldTypeEquals("string1", FieldType.ENUM);
        out1.assertFieldTypeEquals("string2", FieldType.ENUM);
        /*out1.assertFieldEquals("string1",(Object) type);
        out1.assertFieldEquals("string2", method);*/
        byte[] expectedBytes = EncryptField.toByteArray(type);
        byte[] expectedBytes1 = EncryptField.toByteArray(method);
        Assert.assertTrue(Arrays.equals(expectedBytes,EncryptField.toByteArray(out.getField("string1").getRawValue())));
        Assert.assertTrue(Arrays.equals( expectedBytes1, EncryptField.toByteArray(out.getField("string2").getRawValue())));
    }

    @Test
    public void testProcessingEncryptionDateTime() throws  Exception{
        Record record1 = new StandardRecord();
        Date date = new Date();
        Date date1 = new Date();

        record1.setField("string1", FieldType.DATETIME, date);
        record1.setField("string2", FieldType.DATETIME, date1);

        TestRunner testRunner = TestRunners.newTestRunner(new EncryptField());
        testRunner.setProperty(EncryptField.MODE, EncryptField.ENCRYPT_MODE);
        testRunner.setProperty(EncryptField.ALGO, "AES");
        testRunner.setProperty(EncryptField.KEY, "azerty1234567890");
        testRunner.setProperty("string1", "datetime");
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
        testRunner2.setProperty("string1", "datetime");
        testRunner2.setProperty("string2", "datetime");
        testRunner2.assertValid();
        testRunner2.enqueue(out);
        testRunner2.run();
        testRunner2.assertAllInputRecordsProcessed();
        testRunner2.assertOutputRecordsCount(1);

        MockRecord out1 = testRunner2.getOutputRecords().get(0);
        out1.assertRecordSizeEquals(2);
        out1.assertFieldTypeEquals("string1", FieldType.DATETIME);
        out1.assertFieldTypeEquals("string2", FieldType.DATETIME);
        /*out1.assertFieldEquals("string1", date);
        out1.assertFieldEquals("string2", date1);*/
        byte[] expectedBytes = EncryptField.toByteArray(date);
        byte[] expectedBytes1 = EncryptField.toByteArray(date1);
        Assert.assertTrue(Arrays.equals(expectedBytes,EncryptField.toByteArray(out.getField("string1").getRawValue())) );
        Assert.assertTrue(Arrays.equals( expectedBytes1, EncryptField.toByteArray(out.getField("string2").getRawValue())));
    }

    @Test
    public void testProcessingEncryption() throws Exception{
        Record record1 = new StandardRecord();
        Record record2 = new StandardRecord();
        Record record3 = new StandardRecord();
        record2.setField("string9", FieldType.STRING, "Nouri");
        record3.setField("string9", FieldType.STRING, "Feiz");
        record1.setField("string1", FieldType.RECORD, record2);
        record1.setField("string2", FieldType.RECORD, record3);
        byte[] recordByte = EncryptField.toByteArray(record1);
        Object objectRecord = EncryptField.toObject(recordByte);
        /*Record objectRecord1 = (Record) EncryptField.toObject(recordByte);*/
        Assert.assertEquals(record1, objectRecord);


    }

    @Test
    public void testProcessingEncryptionECBPad() {
        Record record1 = new StandardRecord();
        record1.setField("string1", FieldType.STRING, "Logisland1234567");
        /*record1.setField("string2", FieldType.STRING, "nouri");*/

        TestRunner testRunner = TestRunners.newTestRunner(new EncryptField());
        testRunner.setProperty(EncryptField.MODE, EncryptField.ENCRYPT_MODE);
        testRunner.setProperty(EncryptField.ALGO, "AES/ECB/NoPadding");
        testRunner.setProperty(EncryptField.KEY, "azerty1234567890azerty1234567890");
        testRunner.setProperty("string1", "string");
        /*testRunner.setProperty("string2", "");*/
        testRunner.setProcessorIdentifier("encrypt_1");
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(1);
        out.assertFieldTypeEquals("string1", FieldType.BYTES);
        /*out.assertFieldTypeEquals("string2", FieldType.BYTES);*/


        TestRunner testRunner2 = TestRunners.newTestRunner(new EncryptField());
        testRunner2.setProperty(EncryptField.MODE, EncryptField.DECRYPT_MODE);
        testRunner2.setProperty(EncryptField.ALGO, "AES/ECB/NoPadding");
        testRunner2.setProperty(EncryptField.KEY, "azerty1234567890azerty1234567890");
        testRunner2.setProperty("string1", "string");
        /*testRunner2.setProperty("string2", "string");*/
        testRunner2.assertValid();
        testRunner2.enqueue(out);
        testRunner2.run();
        testRunner2.assertAllInputRecordsProcessed();
        testRunner2.assertOutputRecordsCount(1);

        MockRecord out1 = testRunner2.getOutputRecords().get(0);
        out1.assertRecordSizeEquals(1);
        out1.assertFieldTypeEquals("string1", FieldType.STRING);
        /*out1.assertFieldTypeEquals("string2", FieldType.STRING);*/
        out1.assertFieldEquals("string1", "Logisland1234567");
        /*out1.assertFieldEquals("string2", "nouri");*/
    }

    @Test
    public void testProcessingEncryptionCBCPad() {
        Record record1 = new StandardRecord();
        record1.setField("string3", FieldType.STRING, "Nouri1");
        record1.setField("string2", FieldType.STRING, "Nouri2");

        TestRunner testRunner = TestRunners.newTestRunner(new EncryptField());
        testRunner.setProperty(EncryptField.MODE, EncryptField.ENCRYPT_MODE);
        testRunner.setProperty(EncryptField.ALGO, "DES/CBC/PKCS5Padding");
        testRunner.setProperty(EncryptField.KEY, "azerty1234567890");
        testRunner.setProperty("string3", "");
        testRunner.setProperty("string2", "");
        testRunner.setProcessorIdentifier("encrypt_1");
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(4);
        out.assertFieldTypeEquals("string3", FieldType.BYTES);
        out.assertFieldTypeEquals("string2", FieldType.BYTES);


        TestRunner testRunner2 = TestRunners.newTestRunner(new EncryptField());
        testRunner2.setProperty(EncryptField.MODE, EncryptField.DECRYPT_MODE);
        testRunner2.setProperty(EncryptField.ALGO, "DES/CBC/PKCS5Padding");
        testRunner2.setProperty(EncryptField.KEY, "azerty1234567890");
        testRunner2.setProperty("string3", "string");
        testRunner2.setProperty("string2", "string");
        testRunner2.assertValid();
        testRunner2.enqueue(out);
        testRunner2.run();
        testRunner2.assertAllInputRecordsProcessed();
        testRunner2.assertOutputRecordsCount(1);

        MockRecord out1 = testRunner2.getOutputRecords().get(0);
        out1.assertRecordSizeEquals(4);
        out1.assertFieldTypeEquals("string3", FieldType.STRING);
        out1.assertFieldTypeEquals("string2", FieldType.STRING);
        out1.assertFieldEquals("string3", "Nouri1");
        out1.assertFieldEquals("string2", "Nouri2");
    }

    @Test
    public void testProcessingEncryptionMixed() throws Exception {
        Record record1 = new StandardRecord();
        record1.setField("string1", FieldType.STRING, "Nouri1");
        record1.setField("string2", FieldType.STRING, "Nouri2");
        record1.setField("int1", FieldType.INT, 1994);
        record1.setField("int2", FieldType.INT, 987654321);
        record1.setField("long1", FieldType.LONG, 19941994L);
        record1.setField("long2", FieldType.LONG, 1234567890123L);
        byte[] a_byte = {7, 75, 15, 56,-3, -35, 59, -61, -112, -128, -3, -113};
        byte[] b_byte = {5, -58, 64, 12, -3};
        record1.setField("byte1", FieldType.BYTES, a_byte);
        record1.setField("byte2", FieldType.BYTES, b_byte);
        Map map1 = new HashMap();
        map1.put("Germany", "Berlin");
        map1.put("Spain", "Madrid");
        map1.put("Greece", "Athens");
        map1.put("Turkey", "Ankara");
        Map map2 = new HashMap();

        map2.put("A", "1");
        map2.put("B", "2");
        map2.put("C", "3");
        record1.setField("map1", FieldType.MAP, map1);
        record1.setField("map2", FieldType.MAP, map2);
        float a = 3.215f;
        float b =454.54f;
        record1.setField("float1", FieldType.FLOAT, a);
        record1.setField("float2", FieldType.FLOAT, b);
        double c = 3.215;
        double d =454.54;
        record1.setField("double1", FieldType.DOUBLE, c);
        record1.setField("double2", FieldType.DOUBLE, d);
        record1.setField("boolean1", FieldType.BOOLEAN, true);
        record1.setField("boolean2", FieldType.BOOLEAN, false);
        EncryptionMethod type = EncryptionMethod.AES_CTR;
        EncryptionMethod method = EncryptionMethod.AES_CBC;

        record1.setField("enum1", FieldType.ENUM, type);
        record1.setField("enum2", FieldType.ENUM, method);
        Date date = new Date();
        Date date1 = new Date();

        record1.setField("date1", FieldType.DATETIME, date);
        record1.setField("date2", FieldType.DATETIME, date1);

        TestRunner testRunner = TestRunners.newTestRunner(new EncryptField());
        testRunner.setProperty(EncryptField.MODE, EncryptField.ENCRYPT_MODE);
        testRunner.setProperty(EncryptField.ALGO, "RSA/ECB/OAEPWithSHA-256AndMGF1Padding");
        testRunner.setProperty(EncryptField.KEY, "azerty1234567890");
        testRunner.setProperty(EncryptField.IV, "azerty1234567891");
        testRunner.setProperty(EncryptField.KEYFILE, "/home/ubuntu/public.der");
        testRunner.setProperty("string1", "");
        testRunner.setProperty("string2", "");
        testRunner.setProperty("int1", "");
        testRunner.setProperty("int2", "");
        testRunner.setProperty("long1", "");
        testRunner.setProperty("long2", "");
        testRunner.setProperty("byte1", "");
        testRunner.setProperty("byte2", "");
        testRunner.setProperty("map1", "");
        testRunner.setProperty("map2", "");
        testRunner.setProperty("float1", "");
        testRunner.setProperty("float2", "");
        testRunner.setProperty("double1", "");
        testRunner.setProperty("double2", "");
        testRunner.setProperty("boolean1", "");
        testRunner.setProperty("boolean2", "");
        testRunner.setProperty("enum1", "");
        testRunner.setProperty("enum2", "");
        testRunner.setProperty("date1", "");
        testRunner.setProperty("date2", "");
        testRunner.setProcessorIdentifier("encrypt_1");

        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(20);
        out.assertFieldTypeEquals("string1", FieldType.BYTES);
        out.assertFieldTypeEquals("string2", FieldType.BYTES);
        out.assertFieldTypeEquals("int1", FieldType.BYTES);
        out.assertFieldTypeEquals("int2", FieldType.BYTES);
        out.assertFieldTypeEquals("long1", FieldType.BYTES);
        out.assertFieldTypeEquals("long2", FieldType.BYTES);
        out.assertFieldTypeEquals("byte1", FieldType.BYTES);
        out.assertFieldTypeEquals("byte2", FieldType.BYTES);
        out.assertFieldTypeEquals("map1", FieldType.BYTES);
        out.assertFieldTypeEquals("map2", FieldType.BYTES);
        out.assertFieldTypeEquals("float1", FieldType.BYTES);
        out.assertFieldTypeEquals("float2", FieldType.BYTES);
        out.assertFieldTypeEquals("double1", FieldType.BYTES);
        out.assertFieldTypeEquals("double2", FieldType.BYTES);
        out.assertFieldTypeEquals("boolean1", FieldType.BYTES);
        out.assertFieldTypeEquals("boolean2", FieldType.BYTES);
        out.assertFieldTypeEquals("enum1", FieldType.BYTES);
        out.assertFieldTypeEquals("enum2", FieldType.BYTES);
        out.assertFieldTypeEquals("date1", FieldType.BYTES);
        out.assertFieldTypeEquals("date2", FieldType.BYTES);


        TestRunner testRunner2 = TestRunners.newTestRunner(new EncryptField());
        testRunner2.setProperty(EncryptField.MODE, EncryptField.DECRYPT_MODE);
        testRunner2.setProperty(EncryptField.ALGO, "RSA/ECB/OAEPWithSHA-256AndMGF1Padding");
        testRunner2.setProperty(EncryptField.KEY, "azerty1234567890");
        testRunner2.setProperty(EncryptField.IV, "azerty1234567891");
        testRunner2.setProperty(EncryptField.KEYFILE, "/home/ubuntu/private.der");
        testRunner2.setProperty("string1", "string");
        testRunner2.setProperty("string2", "string");
        testRunner2.setProperty("int1", "int");
        testRunner2.setProperty("int2", "int");
        testRunner2.setProperty("long1", "long");
        testRunner2.setProperty("long2", "long");
        testRunner2.setProperty("byte1", "bytes");
        testRunner2.setProperty("byte2", "bytes");
        testRunner2.setProperty("map1", "map");
        testRunner2.setProperty("map2", "map");
        testRunner2.setProperty("float1", "float");
        testRunner2.setProperty("float2", "float");
        testRunner2.setProperty("double1", "double");
        testRunner2.setProperty("double2", "double");
        testRunner2.setProperty("boolean1", "boolean");
        testRunner2.setProperty("boolean2", "boolean");
        testRunner2.setProperty("enum1", "enum");
        testRunner2.setProperty("enum2", "enum");
        testRunner2.setProperty("date1", "datetime");
        testRunner2.setProperty("date2", "datetime");
        testRunner2.assertValid();
        testRunner2.enqueue(out);
        testRunner2.run();
        testRunner2.assertAllInputRecordsProcessed();
        testRunner2.assertOutputRecordsCount(1);

        MockRecord out1 = testRunner2.getOutputRecords().get(0);
        out1.assertRecordSizeEquals(20);
        out1.assertFieldTypeEquals("string1", FieldType.STRING);
        out1.assertFieldTypeEquals("string2", FieldType.STRING);
        out1.assertFieldTypeEquals("int1", FieldType.INT);
        out1.assertFieldTypeEquals("int2", FieldType.INT);
        out1.assertFieldTypeEquals("long1", FieldType.LONG);
        out1.assertFieldTypeEquals("long2", FieldType.LONG);
        out1.assertFieldTypeEquals("byte1", FieldType.BYTES);
        out1.assertFieldTypeEquals("byte2", FieldType.BYTES);
        out1.assertFieldTypeEquals("map1", FieldType.MAP);
        out1.assertFieldTypeEquals("map2", FieldType.MAP);
        out1.assertFieldTypeEquals("float1", FieldType.FLOAT);
        out1.assertFieldTypeEquals("float2", FieldType.FLOAT);
        out1.assertFieldTypeEquals("double1", FieldType.DOUBLE);
        out1.assertFieldTypeEquals("double2", FieldType.DOUBLE);
        out1.assertFieldTypeEquals("boolean1", FieldType.BOOLEAN);
        out1.assertFieldTypeEquals("boolean2", FieldType.BOOLEAN);
        out1.assertFieldTypeEquals("enum1", FieldType.ENUM);
        out1.assertFieldTypeEquals("enum2", FieldType.ENUM);
        out1.assertFieldTypeEquals("date1", FieldType.DATETIME);
        out1.assertFieldTypeEquals("date2", FieldType.DATETIME);
        out1.assertFieldEquals("string1", "Nouri1");
        out1.assertFieldEquals("string2", "Nouri2");
        out1.assertFieldEquals("int1", 1994);
        out1.assertFieldEquals("int2", 987654321);
        out1.assertFieldEquals("long1", 19941994);
        out1.assertFieldEquals("long2", 1234567890123L);
        Assert.assertTrue(Arrays.equals(a_byte, (byte[]) out1.getField("byte1").getRawValue()));
        Assert.assertTrue(Arrays.equals(b_byte, (byte[]) out1.getField("byte2").getRawValue()));
        out1.assertFieldEquals("map1", map1);
        out1.assertFieldEquals("map2", map2);
        out1.assertFieldEquals("float1", a);
        out1.assertFieldEquals("float2", b);
        out1.assertFieldEquals("double1", c);
        out1.assertFieldEquals("double2", d);
        out1.assertFieldEquals("boolean1", true);
        out1.assertFieldEquals("boolean2", false);
        byte[] expectedBytes = EncryptField.toByteArray(type);
        byte[] expectedBytes1 = EncryptField.toByteArray(method);
        Assert.assertTrue(Arrays.equals(expectedBytes,EncryptField.toByteArray(out.getField("enum1").getRawValue())));
        Assert.assertTrue(Arrays.equals( expectedBytes1, EncryptField.toByteArray(out.getField("enum2").getRawValue())));
        byte[] expectedBytes2 = EncryptField.toByteArray(date);
        byte[] expectedBytes3 = EncryptField.toByteArray(date1);
        Assert.assertTrue(Arrays.equals(expectedBytes2,EncryptField.toByteArray(out.getField("date1").getRawValue())) );
        Assert.assertTrue(Arrays.equals( expectedBytes3, EncryptField.toByteArray(out.getField("date2").getRawValue())));

    }

    @Test
    public void testProcessingEncryptionIntegerRSA() {
        Record record1 = new StandardRecord();
        record1.setField("string1", FieldType.INT, 199419441);
        record1.setField("string2", FieldType.INT, 987654321);

        TestRunner testRunner = TestRunners.newTestRunner(new EncryptField());
        testRunner.setProperty(EncryptField.MODE, EncryptField.ENCRYPT_MODE);
        testRunner.setProperty(EncryptField.ALGO, "RSA/ECB/PKCS1Padding");
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
        testRunner2.setProperty(EncryptField.ALGO, "RSA/ECB/PKCS1Padding");
        testRunner2.setProperty(EncryptField.KEY, "azerty1234567890");
        testRunner2.setProperty("string1", "int");
        testRunner2.setProperty("string2", "int");
        testRunner2.assertValid();
        testRunner2.enqueue(out);
        testRunner2.run();
        testRunner2.assertAllInputRecordsProcessed();
        testRunner2.assertOutputRecordsCount(1);
        //TODO configure and decrypt

        MockRecord out1 = testRunner2.getOutputRecords().get(0);
        out1.assertRecordSizeEquals(2);
        out1.assertFieldTypeEquals("string1", FieldType.INT);
        out1.assertFieldTypeEquals("string2", FieldType.INT);
        out1.assertFieldEquals("string1", 199419941);
        out1.assertFieldEquals("string2", 987654321);
    }


}
