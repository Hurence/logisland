
package com.hurence.logisland.processor.hbase;

import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.service.hbase.put.PutColumn;
import com.hurence.logisland.service.hbase.put.PutRecord;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.RecordUtils;
import com.hurence.logisland.serializer.KryoSerializer;
import com.hurence.logisland.serializer.RecordSerializer;
import com.hurence.logisland.serializer.SerializerProvider;
import com.hurence.logisland.util.runner.MockRecord;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestPutHBaseCell {

    private static Logger logger = LoggerFactory.getLogger(TestPutHBaseCell.class);
    public static final String TABLE_NAME_KEY = "table_name";
    public static final String ROW_ID_KEY = "row_id";
    public static final String COLUMN_FAMILY_KEY = "column_family";
    public static final String COLUMN_QUALIFIER_KEY = "column_qualifier";


    public static final String TABLE_NAME = "logisland";
    public static final String QUALIFIER = "qualifier1";
    public static final String ROW_ID_1 = "id1";
    public static final String ROW_ID_2 = "id2";
    public static final String FAMILY = "family1";
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
    public void testSingleRecord() throws IOException, InitializationException {

        final TestRunner runner = getTestRunner();

        final MockHBaseClientService hBaseClient = getHBaseClientService(runner);

        final Record inputRecord = getRecord();
        runner.enqueue(inputRecord);
        runner.run();
        runner.assertAllInputRecordsProcessed();


        final MockRecord outputRecord = runner.getOutputRecords().get(0);
        outputRecord.assertContentEquals(inputRecord);

        assertNotNull(hBaseClient.getRecordPuts());
        assertEquals(1, hBaseClient.getRecordPuts().size());

        List<PutRecord> puts = hBaseClient.getRecordPuts().get(TABLE_NAME);
        assertEquals(1, puts.size());
        verifyPut(ROW_ID_1, FAMILY, QUALIFIER, inputRecord, puts.get(0));

    }

    private Record getRecord() {
        final Record inputRecord = new MockRecord(RecordUtils.getKeyValueRecord(KEY, VALUE));

        inputRecord.setStringField(ROW_ID_KEY, ROW_ID_1);
        inputRecord.setStringField(COLUMN_FAMILY_KEY, FAMILY);
        inputRecord.setStringField(COLUMN_QUALIFIER_KEY, QUALIFIER);
        inputRecord.setStringField(TABLE_NAME_KEY, TABLE_NAME);
        return inputRecord;
    }

    private TestRunner getTestRunner() {
        final TestRunner runner = TestRunners.newTestRunner(PutHBaseCell.class);
        runner.setProperty(PutHBaseCell.TABLE_NAME_FIELD, TABLE_NAME_KEY);
        runner.setProperty(PutHBaseCell.ROW_ID_FIELD, ROW_ID_KEY);
        runner.setProperty(PutHBaseCell.COLUMN_FAMILY_FIELD, COLUMN_FAMILY_KEY);
        runner.setProperty(PutHBaseCell.COLUMN_QUALIFIER_FIELD, COLUMN_QUALIFIER_KEY);
        runner.setProperty(PutHBaseCell.BATCH_SIZE, "1");
        runner.setProperty(PutHBaseCell.RECORD_SERIALIZER, KryoSerializer.class.getName());
        return runner;
    }


    @Test
    public void testMultipleRecordsSameTableDifferentRow() throws IOException, InitializationException {


        final TestRunner runner = getTestRunner();
        final MockHBaseClientService hBaseClient = getHBaseClientService(runner);
        final Record inputRecord1 = getRecord().setStringField(ROW_ID_KEY, ROW_ID_1);
        final Record inputRecord2 = getRecord().setStringField(ROW_ID_KEY, ROW_ID_2);
        runner.enqueue(inputRecord1, inputRecord2);


        runner.run();
        runner.assertAllInputRecordsProcessed();
        runner.assertOutputErrorCount(0);

        final MockRecord outFile = runner.getOutputRecords().get(0);
        outFile.assertContentEquals(inputRecord1);

        assertNotNull(hBaseClient.getRecordPuts());
        assertEquals(1, hBaseClient.getRecordPuts().size());

        List<PutRecord> puts = hBaseClient.getRecordPuts().get(TABLE_NAME);
        assertEquals(2, puts.size());
        verifyPut(ROW_ID_1, FAMILY, QUALIFIER, inputRecord1, puts.get(0));
        verifyPut(ROW_ID_2, FAMILY, QUALIFIER, inputRecord2, puts.get(1));

    }


    @Test
    public void testMultipleRecordsSameTableDifferentRowFailure() throws IOException, InitializationException {


        final TestRunner runner = getTestRunner();
        final MockHBaseClientService hBaseClient = getHBaseClientService(runner);
        hBaseClient.setThrowException(true);

        final Record inputRecord1 = getRecord().setStringField(ROW_ID_KEY, ROW_ID_1);
        final Record inputRecord2 = getRecord().setStringField(ROW_ID_KEY, ROW_ID_2);
        runner.enqueue(inputRecord1, inputRecord2);

        runner.run();
        runner.assertOutputErrorCount(2);

    }

    @Test
    public void testMultipleRecordsSameTableSameRow() throws IOException, InitializationException {
        final TestRunner runner = getTestRunner();
        final MockHBaseClientService hBaseClient = getHBaseClientService(runner);


        final Record inputRecord1 = getRecord().setStringField(ROW_ID_KEY, ROW_ID_1);
        final Record inputRecord2 = getRecord().setStringField(ROW_ID_KEY, ROW_ID_1);
        runner.enqueue(inputRecord1, inputRecord2);

        runner.run();
        runner.assertAllInputRecordsProcessed();

        final MockRecord outFile = runner.getOutputRecords().get(0);
        outFile.assertContentEquals(inputRecord1);

        assertNotNull(hBaseClient.getRecordPuts());
        assertEquals(1, hBaseClient.getRecordPuts().size());

        List<PutRecord> puts = hBaseClient.getRecordPuts().get(TABLE_NAME);
        assertEquals(2, puts.size());
        verifyPut(ROW_ID_1, FAMILY, QUALIFIER, inputRecord1, puts.get(0));
        verifyPut(ROW_ID_1, FAMILY, QUALIFIER, inputRecord2, puts.get(1));


    }

    @Test
    public void testSingleRecordWithBinaryRowKey() throws IOException, InitializationException {


        final TestRunner runner = TestRunners.newTestRunner(PutHBaseCell.class);
        runner.setProperty(PutHBaseCell.TABLE_NAME_FIELD, TABLE_NAME_KEY);
        runner.setProperty(PutHBaseCell.ROW_ID_FIELD, ROW_ID_KEY);
        runner.setProperty(PutHBaseCell.ROW_ID_ENCODING_STRATEGY, PutHBaseCell.ROW_ID_ENCODING_BINARY.getValue());
        runner.setProperty(PutHBaseCell.COLUMN_FAMILY_FIELD, COLUMN_FAMILY_KEY);
        runner.setProperty(PutHBaseCell.COLUMN_QUALIFIER_FIELD, COLUMN_QUALIFIER_KEY);
        runner.setProperty(PutHBaseCell.BATCH_SIZE, "1");

        final MockHBaseClientService hBaseClient = getHBaseClientService(runner);

        final byte[] expectedRowKey = hBaseClient.toBytesBinary(ROW_BINARY);

        final Record inputRecord1 = getRecord().setStringField(ROW_ID_KEY, ROW_BINARY);
        runner.enqueue(inputRecord1);
        runner.run();
        runner.assertAllInputRecordsProcessed();
        runner.assertOutputRecordsCount(1);

        final MockRecord outFile = runner.getOutputRecords().get(0);
        outFile.assertContentEquals(inputRecord1);

        assertNotNull(hBaseClient.getRecordPuts());
        assertEquals(1, hBaseClient.getRecordPuts().size());

        List<PutRecord> puts = hBaseClient.getRecordPuts().get(TABLE_NAME);
        assertEquals(1, puts.size());
        verifyPut(expectedRowKey, FAMILY.getBytes(StandardCharsets.UTF_8), QUALIFIER.getBytes(StandardCharsets.UTF_8), inputRecord1, puts.get(0));

    }




    private MockHBaseClientService getHBaseClientService(TestRunner runner) throws InitializationException {
        final MockHBaseClientService hBaseClient = new MockHBaseClientService();
        hBaseClient.setIdentifier("hbaseClient");
        runner.addControllerService(hBaseClient);
        runner.enableControllerService(hBaseClient);
        runner.setProperty(PutHBaseCell.HBASE_CLIENT_SERVICE, "hbaseClient");
        return hBaseClient;
    }

    private void verifyPut(String row, String columnFamily, String columnQualifier, Record content, PutRecord put) {
        verifyPut(row.getBytes(StandardCharsets.UTF_8), columnFamily.getBytes(StandardCharsets.UTF_8),
                columnQualifier.getBytes(StandardCharsets.UTF_8), content, put);
    }

    private void verifyPut(byte[] row, byte[] columnFamily, byte[] columnQualifier, Record content, PutRecord put) {
        assertEquals(new String(row, StandardCharsets.UTF_8), new String(put.getRow(), StandardCharsets.UTF_8));

        assertNotNull(put.getColumns());
        assertEquals(1, put.getColumns().size());

        final PutColumn column = put.getColumns().iterator().next();
        assertEquals(new String(columnFamily, StandardCharsets.UTF_8), new String(column.getColumnFamily(), StandardCharsets.UTF_8));
        assertEquals(new String(columnQualifier, StandardCharsets.UTF_8), new String(column.getColumnQualifier(), StandardCharsets.UTF_8));

        MockRecord out = new MockRecord(deserialize(column.getBuffer()));
        out.assertContentEquals(content);
    }

}
