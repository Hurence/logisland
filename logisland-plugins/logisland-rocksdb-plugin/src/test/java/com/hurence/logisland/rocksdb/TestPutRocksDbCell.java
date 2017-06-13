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

package com.hurence.logisland.rocksdb;

import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.processor.rocksdb.PutRocksDbCell;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.RecordUtils;
import com.hurence.logisland.serializer.KryoSerializer;
import com.hurence.logisland.serializer.RecordSerializer;
import com.hurence.logisland.serializer.SerializerProvider;
import com.hurence.logisland.service.rocksdb.put.PutRecord;
import com.hurence.logisland.util.runner.MockRecord;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import org.junit.Test;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestPutRocksDbCell {

    private static Logger logger = LoggerFactory.getLogger(TestPutRocksDbCell.class);
    public static final String FAMILY_FIELD = "column_family";
    public static final String KEY_FIELD = "column_qualifier";

    public static final String KEY1 = "qualifier1";
    public static final String KEY2 = "qualifier2";
    public static final byte[] KEY1_BYTES = KEY1.getBytes(StandardCharsets.UTF_8);
    public static final byte[] KEY2_BYTES = KEY2.getBytes(StandardCharsets.UTF_8);
    public static final String FAMILY1 = "family1";
    public static final String FAMILY2 = "family2";
    public static final String KEY = "some key";
    public static final String VALUE = "some content";
    public static final String ROW_BINARY = "\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00" +
            "\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00" +
            "\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00" +
            "\\x00\\x00\\x00\\x00\\x00\\x01\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00" +
            "\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x01\\x01\\x00\\x00\\x00\\x00\\x00" +
            "\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00" +
            "\\x00\\x00\\x00\\x01\\x00\\x00\\x01\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00" +
            "\\x01\\x00\\x00\\x01\\x00\\x00\\x00\\x00\\x01\\x01\\x01\\x00\\x01\\x00\\x01\\x01\\x01\\x00\\x00\\x00" +
            "\\x00\\x00\\x00\\x01\\x01\\x01\\x01\\x00\\x00\\x00\\x00\\x00\\x00\\x01\\x01\\x00\\x01\\x00\\x01\\x00" +
            "\\x00\\x01\\x01\\x01\\x01\\x00\\x00\\x01\\x01\\x01\\x00\\x01\\x00\\x00";

    private byte[] serialize(Record record) {


        RecordSerializer serializer = SerializerProvider.getSerializer(KryoSerializer.class.getName(), null);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        serializer.serialize(baos, record);
        final byte[] buffer = baos.toByteArray();
        try {
            baos.close();
        } catch (IOException e) {
            logger.info(e.toString());
        }
        return buffer;
    }

    private Record deserialize(byte[] cell) {
        RecordSerializer serializer = SerializerProvider.getSerializer(KryoSerializer.class.getName(), null);
        try {
            //  final byte[] row = Arrays.copyOfRange(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength() + cell.getRowOffset());
            ByteArrayInputStream bais = new ByteArrayInputStream(cell);
            Record deserializedRecord = serializer.deserialize(bais);
            bais.close();
            return deserializedRecord;

        } catch (Exception e) {
            logger.info(e.toString());
            return null;
        }
    }

    @Test
    public void testSerializing() {
        final Record inputRecord = new MockRecord(RecordUtils.getKeyValueRecord("key", "value"));

        final byte[] serialized = serialize(inputRecord);

        assertEquals(inputRecord, new MockRecord(deserialize(serialized)));
    }

    @Test
    public void testSingleRecord() throws RocksDBException, InitializationException {

        final TestRunner runner = getTestRunner();

        final MockRocksdbClientService rocksdbClient = getRocksdbClientService(runner);

        final Record inputRecord = getRecord();
        runner.enqueue(inputRecord);
        runner.run();
        runner.assertAllInputRecordsProcessed();


        final MockRecord outputRecord = runner.getOutputRecords().get(0);
        outputRecord.assertContentEquals(inputRecord);


        MockRecord out = new MockRecord(deserialize(rocksdbClient.get(FAMILY1, KEY1_BYTES)));
        out.assertContentEquals(inputRecord);

    }

    private Record getRecord() {
        final Record inputRecord = new MockRecord(RecordUtils.getKeyValueRecord(KEY, VALUE));
        inputRecord.setStringField(FAMILY_FIELD, FAMILY1);
        inputRecord.setStringField(KEY_FIELD, KEY1);
        return inputRecord;
    }

    private TestRunner getTestRunner() {
        final TestRunner runner = TestRunners.newTestRunner(PutRocksDbCell.class);
        runner.setProperty(PutRocksDbCell.FAMILY_NAME_FIELD, FAMILY_FIELD);
        runner.setProperty(PutRocksDbCell.KEY_FIELD, KEY_FIELD);
        runner.setProperty(PutRocksDbCell.RECORD_SERIALIZER, KryoSerializer.class.getName());
        return runner;
    }


    @Test
    public void testMultipleRecordsSameFamilyDifferentKey() throws RocksDBException, InitializationException {

        final TestRunner runner = getTestRunner();
        final MockRocksdbClientService rocksdbClientService = getRocksdbClientService(runner);
        final Record inputRecord1 = getRecord().setStringField(FAMILY_FIELD, FAMILY1);
        final Record inputRecord2 = getRecord().setStringField(FAMILY_FIELD, FAMILY1)
                .setStringField(KEY_FIELD, KEY2);
        runner.enqueue(inputRecord1, inputRecord2);


        runner.run();
        runner.assertAllInputRecordsProcessed();
        runner.assertOutputErrorCount(0);

        final MockRecord outFile = runner.getOutputRecords().get(0);
        outFile.assertContentEquals(inputRecord1);

        MockRecord out = new MockRecord(deserialize(rocksdbClientService.get(FAMILY1, KEY1_BYTES)));
        out.assertContentEquals(inputRecord1);

        out = new MockRecord(deserialize(rocksdbClientService.get(FAMILY1, KEY2_BYTES)));
        out.assertContentEquals(inputRecord2);
    }

    @Test
    public void testMultipleRecordsDifferentFamily() throws RocksDBException, InitializationException {
        final TestRunner runner = getTestRunner();
        final MockRocksdbClientService rocksdbClientService = getRocksdbClientService(runner);
        final Record inputRecord1 = getRecord().setStringField(FAMILY_FIELD, FAMILY1);
        final Record inputRecord2 = getRecord().setStringField(FAMILY_FIELD, FAMILY2);
        runner.enqueue(inputRecord1, inputRecord2);


        runner.run();
        runner.assertAllInputRecordsProcessed();
        runner.assertOutputErrorCount(0);

        final MockRecord outFile = runner.getOutputRecords().get(0);
        outFile.assertContentEquals(inputRecord1);

        MockRecord out = new MockRecord(deserialize(rocksdbClientService.get(FAMILY1, KEY1_BYTES)));
        out.assertContentEquals(inputRecord1);

        out = new MockRecord(deserialize(rocksdbClientService.get(FAMILY2, KEY1_BYTES)));
        out.assertContentEquals(inputRecord2);
    }

    private MockRocksdbClientService getRocksdbClientService(TestRunner runner) throws InitializationException {
        final MockRocksdbClientService rocksdbClient = new MockRocksdbClientService();
        rocksdbClient.setIdentifier("rocksdbClient");
        runner.addControllerService(rocksdbClient);
        runner.enableControllerService(rocksdbClient);
        runner.setProperty(PutRocksDbCell.ROCKSDB_CLIENT_SERVICE, "rocksdbClient");
        return rocksdbClient;
    }


}
