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

package com.hurence.logisland.processor.hbase;

import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.RecordUtils;
import com.hurence.logisland.serializer.JsonSerializer;
import com.hurence.logisland.serializer.RecordSerializer;
import com.hurence.logisland.util.runner.MockRecord;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TestFetchHBaseRow {


    public static final String FAMILY = "cf";
    public static final String COLUMN_QUALIFIER_1 = "cq1";
    public static final String COLUMN_QUALIFIER_2 = "cq2";
    private FetchHBaseRow proc;
    private MockHBaseClientService hBaseClientService;
    private TestRunner runner;

    public static final String TABLE_NAME_KEY = "table_name";
    public static final String ROW_ID_KEY = "row_id";
    public static final String COLUMNS_KEY = "columns";


    public static final String TABLE_NAME = "logisland";
    public static final String QUALIFIER = "qualifier1";
    public static final String ROW_ID_1 = "id1";
    public static final String ROW_ID_2 = "id2";

    public static final String KEY = "some key";
    public static final String VALUE = "some content";


        final Record inputRecord = new MockRecord(RecordUtils.getKeyValueRecord(KEY, VALUE))
                .setStringField(ROW_ID_KEY, ROW_ID_1)
                .setStringField(COLUMNS_KEY, FAMILY + ":" + COLUMN_QUALIFIER_1)
                .setStringField(TABLE_NAME_KEY, TABLE_NAME);




    private String serializeRecord(RecordSerializer serializer, Record inputRecord) throws IOException {


        inputRecord.setStringField(ROW_ID_KEY, ROW_ID_1);
        inputRecord.setStringField(COLUMNS_KEY, FAMILY + ":" + COLUMN_QUALIFIER_1);
        inputRecord.setStringField(TABLE_NAME_KEY, TABLE_NAME);



        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        serializer.serialize(baos, inputRecord);
        baos.close();


        return new String(baos.toByteArray());

    }

    @Before
    public void setup() throws InitializationException {
        proc = new FetchHBaseRow();
        runner = TestRunners.newTestRunner(proc);

        hBaseClientService = new MockHBaseClientService();
        runner.addControllerService("hbaseClient", hBaseClientService);
        runner.enableControllerService(hBaseClientService);
        runner.setProperty(FetchHBaseRow.HBASE_CLIENT_SERVICE, "hbaseClient");

        runner.setProperty(FetchHBaseRow.TABLE_NAME_FIELD, TABLE_NAME_KEY);
        runner.setProperty(FetchHBaseRow.ROW_ID_FIELD, ROW_ID_KEY);
        runner.setProperty(FetchHBaseRow.COLUMNS_FIELD, COLUMNS_KEY);
        runner.setProperty(FetchHBaseRow.RECORD_SERIALIZER, JsonSerializer.class.getName());
    }

    @Test
    public void testColumnsValidation() {
        runner.setProperty(FetchHBaseRow.HBASE_CLIENT_SERVICE, "hbaseService");
        runner.setProperty(FetchHBaseRow.TABLE_NAME_FIELD, "table1");
        runner.setProperty(FetchHBaseRow.ROW_ID_FIELD, "row1");
        runner.assertValid();

        runner.setProperty(FetchHBaseRow.COLUMNS_FIELD, "cf1:cq1");
        runner.assertValid();

        runner.setProperty(FetchHBaseRow.COLUMNS_FIELD, "cf1");
        runner.assertValid();

        runner.setProperty(FetchHBaseRow.COLUMNS_FIELD, "cf1:cq1,cf2:cq2,cf3:cq3");
        runner.assertValid();

        runner.setProperty(FetchHBaseRow.COLUMNS_FIELD, "cf1,cf2:cq1,cf3");
        runner.assertValid();

        runner.setProperty(FetchHBaseRow.COLUMNS_FIELD, "cf1 cf2,cf3");
        runner.assertNotValid();

        runner.setProperty(FetchHBaseRow.COLUMNS_FIELD, "cf1:,cf2,cf3");
        runner.assertNotValid();

        runner.setProperty(FetchHBaseRow.COLUMNS_FIELD, "cf1:cq1,");
        runner.assertNotValid();
    }

    @Test
    public void testNoIncomingRecord() {
        runner.setProperty(FetchHBaseRow.TABLE_NAME_FIELD, "table1");
        runner.setProperty(FetchHBaseRow.ROW_ID_FIELD, "row1");

        runner.run();
        runner.assertOutputErrorCount(0);
        runner.assertOutputRecordsCount(0);

        Assert.assertEquals(0, hBaseClientService.getNumScans());
    }


    @Test
    public void testFetchToAttributesWithStringValues() throws IOException {
        final Map<String, String> cells = new HashMap<>();
        cells.put(COLUMN_QUALIFIER_1, serializeRecord(new JsonSerializer(), inputRecord));
        cells.put(COLUMN_QUALIFIER_2, "val2");

        final long ts1 = 123456789;
        hBaseClientService.addResult(FAMILY + ":" + COLUMN_QUALIFIER_1, cells, ts1);


        Record inRecord = inputRecord;
        runner.enqueue(inRecord);
        runner.run();

        runner.assertOutputErrorCount(0);
        runner.assertOutputRecordsCount(1);

        final MockRecord record = runner.getOutputRecords().get(0);
        record.assertContentEquals(inRecord);

        Assert.assertEquals(1, hBaseClientService.getNumScans());
    }

    @Test
    public void testFetchSpecificColumnsToAttributesWithStringValues() throws IOException {
        final Map<String, String> cells = new HashMap<>();
        cells.put(COLUMN_QUALIFIER_1, "val1");
        cells.put(COLUMN_QUALIFIER_2, serializeRecord(new JsonSerializer(), inputRecord));

        final long ts1 = 123456789;
        hBaseClientService.addResult("row1", cells, ts1);


        Record inRecord = inputRecord.setStringField(COLUMNS_KEY, FAMILY + ":cq2");
        runner.enqueue(inRecord);
        runner.run();

        runner.assertOutputErrorCount(0);
        runner.assertOutputRecordsCount(1);

        final MockRecord record = runner.getOutputRecords().get(0);


        record.assertRecordSizeEquals(5);

        Assert.assertEquals(1, hBaseClientService.getNumScans());
    }
/*
    @Test
    public void testFetchToAttributesWithBase64Values() {
        final Map<String, String> cells = new HashMap<>();
        cells.put("cq1", "val1");
        cells.put("cq2", "val2");

        final long ts1 = 123456789;
        hBaseClientService.addResult("row1", cells, ts1);

        runner.setProperty(FetchHBaseRow.TABLE_NAME_FIELD, "table1");
        runner.setProperty(FetchHBaseRow.ROW_ID_FIELD, "row1");
        runner.setProperty(FetchHBaseRow.DESTINATION, FetchHBaseRow.DESTINATION_ATTRIBUTES);
        runner.setProperty(FetchHBaseRow.JSON_VALUE_ENCODING, FetchHBaseRow.ENCODING_BASE64);

        runner.enqueue("trigger flow file");
        runner.run();

        runner.assertTransferCount(FetchHBaseRow.REL_FAILURE, 0);
        runner.assertTransferCount(FetchHBaseRow.REL_SUCCESS, 1);
        runner.assertTransferCount(FetchHBaseRow.REL_NOT_FOUND, 0);

        final String rowBase64 = Base64.encodeBase64String("row1".getBytes(StandardCharsets.UTF_8));

        final String fam1Base64 = Base64.encodeBase64String("logisland".getBytes(StandardCharsets.UTF_8));
        final String qual1Base64 = Base64.encodeBase64String("cq1".getBytes(StandardCharsets.UTF_8));
        final String val1Base64 = Base64.encodeBase64String("val1".getBytes(StandardCharsets.UTF_8));

        final String fam2Base64 = Base64.encodeBase64String("logisland".getBytes(StandardCharsets.UTF_8));
        final String qual2Base64 = Base64.encodeBase64String("cq2".getBytes(StandardCharsets.UTF_8));
        final String val2Base64 = Base64.encodeBase64String("val2".getBytes(StandardCharsets.UTF_8));

        final MockRecord record = runner.getRecordsForRelationship(FetchHBaseRow.REL_SUCCESS).get(0);
        record.assertAttributeEquals(FetchHBaseRow.HBASE_ROW_ATTR,
                "{\"row\":\"" + rowBase64 + "\", \"cells\": [" +
                        "{\"fam\":\"" + fam1Base64 + "\",\"qual\":\"" + qual1Base64 + "\",\"val\":\"" + val1Base64 + "\",\"ts\":" + ts1 + "}, " +
                        "{\"fam\":\"" + fam2Base64 + "\",\"qual\":\"" + qual2Base64 + "\",\"val\":\"" + val2Base64 + "\",\"ts\":" + ts1 + "}]}");

        Assert.assertEquals(1, hBaseClientService.getNumScans());
    }

    @Test
    public void testFetchToAttributesNoResults() {
        runner.setProperty(FetchHBaseRow.TABLE_NAME_FIELD, "table1");
        runner.setProperty(FetchHBaseRow.ROW_ID_FIELD, "row1");
        runner.setProperty(FetchHBaseRow.DESTINATION, FetchHBaseRow.DESTINATION_ATTRIBUTES);

        runner.enqueue("trigger flow file");
        runner.run();

        runner.assertTransferCount(FetchHBaseRow.REL_FAILURE, 0);
        runner.assertTransferCount(FetchHBaseRow.REL_SUCCESS, 0);
        runner.assertTransferCount(FetchHBaseRow.REL_NOT_FOUND, 1);

        Assert.assertEquals(1, hBaseClientService.getNumScans());
    }

    @Test
    public void testFetchToContentWithStringValues() {
        final Map<String, String> cells = new HashMap<>();
        cells.put("cq1", "val1");
        cells.put("cq2", "val2");

        final long ts1 = 123456789;
        hBaseClientService.addResult("row1", cells, ts1);

        runner.setProperty(FetchHBaseRow.TABLE_NAME_FIELD, "table1");
        runner.setProperty(FetchHBaseRow.ROW_ID_FIELD, "row1");
        runner.setProperty(FetchHBaseRow.DESTINATION, FetchHBaseRow.DESTINATION_CONTENT);

        runner.enqueue("trigger flow file");
        runner.run();

        runner.assertTransferCount(FetchHBaseRow.REL_FAILURE, 0);
        runner.assertTransferCount(FetchHBaseRow.REL_SUCCESS, 1);
        runner.assertTransferCount(FetchHBaseRow.REL_NOT_FOUND, 0);

        final MockRecord record = runner.getRecordsForRelationship(FetchHBaseRow.REL_SUCCESS).get(0);
        record.assertContentEquals("{\"row\":\"row1\", \"cells\": [" +
                "{\"fam\":\"logisland\",\"qual\":\"cq1\",\"val\":\"val1\",\"ts\":" + ts1 + "}, " +
                "{\"fam\":\"logisland\",\"qual\":\"cq2\",\"val\":\"val2\",\"ts\":" + ts1 + "}]}");

        Assert.assertEquals(1, hBaseClientService.getNumScans());
    }

    @Test
    public void testFetchSpecificColumnsToContentWithStringValues() {
        final Map<String, String> cells = new HashMap<>();
        cells.put("cq1", "val1");
        cells.put("cq2", "val2");

        final long ts1 = 123456789;
        hBaseClientService.addResult("row1", cells, ts1);

        runner.setProperty(FetchHBaseRow.TABLE_NAME_FIELD, "table1");
        runner.setProperty(FetchHBaseRow.ROW_ID_FIELD, "row1");
        runner.setProperty(FetchHBaseRow.DESTINATION, FetchHBaseRow.DESTINATION_CONTENT);
        runner.setProperty(FetchHBaseRow.COLUMNS_FIELD, "logisland:cq2");

        runner.enqueue("trigger flow file");
        runner.run();

        runner.assertTransferCount(FetchHBaseRow.REL_FAILURE, 0);
        runner.assertTransferCount(FetchHBaseRow.REL_SUCCESS, 1);
        runner.assertTransferCount(FetchHBaseRow.REL_NOT_FOUND, 0);

        final MockRecord record = runner.getRecordsForRelationship(FetchHBaseRow.REL_SUCCESS).get(0);
        record.assertContentEquals("{\"row\":\"row1\", \"cells\": [{\"fam\":\"logisland\",\"qual\":\"cq2\",\"val\":\"val2\",\"ts\":" + ts1 + "}]}");

        Assert.assertEquals(1, hBaseClientService.getNumScans());
    }

    @Test
    public void testFetchSpecificColumnsToContentWithBase64() {
        final Map<String, String> cells = new HashMap<>();
        cells.put("cq1", "val1");
        cells.put("cq2", "val2");

        final long ts1 = 123456789;
        hBaseClientService.addResult("row1", cells, ts1);

        runner.setProperty(FetchHBaseRow.TABLE_NAME_FIELD, "table1");
        runner.setProperty(FetchHBaseRow.ROW_ID_FIELD, "row1");
        runner.setProperty(FetchHBaseRow.DESTINATION, FetchHBaseRow.DESTINATION_CONTENT);
        runner.setProperty(FetchHBaseRow.JSON_VALUE_ENCODING, FetchHBaseRow.ENCODING_BASE64);

        runner.enqueue("trigger flow file");
        runner.run();

        runner.assertTransferCount(FetchHBaseRow.REL_FAILURE, 0);
        runner.assertTransferCount(FetchHBaseRow.REL_SUCCESS, 1);
        runner.assertTransferCount(FetchHBaseRow.REL_NOT_FOUND, 0);

        final String rowBase64 = Base64.encodeBase64String("row1".getBytes(StandardCharsets.UTF_8));

        final String fam1Base64 = Base64.encodeBase64String("logisland".getBytes(StandardCharsets.UTF_8));
        final String qual1Base64 = Base64.encodeBase64String("cq1".getBytes(StandardCharsets.UTF_8));
        final String val1Base64 = Base64.encodeBase64String("val1".getBytes(StandardCharsets.UTF_8));

        final String fam2Base64 = Base64.encodeBase64String("logisland".getBytes(StandardCharsets.UTF_8));
        final String qual2Base64 = Base64.encodeBase64String("cq2".getBytes(StandardCharsets.UTF_8));
        final String val2Base64 = Base64.encodeBase64String("val2".getBytes(StandardCharsets.UTF_8));

        final MockRecord record = runner.getRecordsForRelationship(FetchHBaseRow.REL_SUCCESS).get(0);
        record.assertContentEquals("{\"row\":\"" + rowBase64 + "\", \"cells\": [" +
                "{\"fam\":\"" + fam1Base64 + "\",\"qual\":\"" + qual1Base64 + "\",\"val\":\"" + val1Base64 + "\",\"ts\":" + ts1 + "}, " +
                "{\"fam\":\"" + fam2Base64 + "\",\"qual\":\"" + qual2Base64 + "\",\"val\":\"" + val2Base64 + "\",\"ts\":" + ts1 + "}]}");

        Assert.assertEquals(1, hBaseClientService.getNumScans());
    }

    @Test
    public void testFetchToContentWithQualifierAndValueJSON() {
        final Map<String, String> cells = new HashMap<>();
        cells.put("cq1", "val1");
        cells.put("cq2", "val2");

        hBaseClientService.addResult("row1", cells, System.currentTimeMillis());

        runner.setProperty(FetchHBaseRow.TABLE_NAME_FIELD, "table1");
        runner.setProperty(FetchHBaseRow.ROW_ID_FIELD, "row1");
        runner.setProperty(FetchHBaseRow.DESTINATION, FetchHBaseRow.DESTINATION_CONTENT);
        runner.setProperty(FetchHBaseRow.JSON_FORMAT, FetchHBaseRow.JSON_FORMAT_QUALIFIER_AND_VALUE);

        runner.enqueue("trigger flow file");
        runner.run();

        runner.assertTransferCount(FetchHBaseRow.REL_FAILURE, 0);
        runner.assertTransferCount(FetchHBaseRow.REL_SUCCESS, 1);
        runner.assertTransferCount(FetchHBaseRow.REL_NOT_FOUND, 0);

        final MockRecord record = runner.getRecordsForRelationship(FetchHBaseRow.REL_SUCCESS).get(0);
        record.assertContentEquals("{\"cq1\":\"val1\", \"cq2\":\"val2\"}");

        Assert.assertEquals(1, hBaseClientService.getNumScans());
    }

    @Test
    public void testFetchWithExpressionLanguage() {
        final Map<String, String> cells = new HashMap<>();
        cells.put("cq1", "val1");
        cells.put("cq2", "val2");

        final long ts1 = 123456789;
        hBaseClientService.addResult("row1", cells, ts1);

        runner.setProperty(FetchHBaseRow.TABLE_NAME_FIELD, "${hbase.table}");
        runner.setProperty(FetchHBaseRow.ROW_ID_FIELD, "${hbase.row}");
        runner.setProperty(FetchHBaseRow.COLUMNS_FIELD, "${hbase.cols}");
        runner.setProperty(FetchHBaseRow.DESTINATION, FetchHBaseRow.DESTINATION_CONTENT);

        final Map<String,String> attributes = new HashMap<>();
        attributes.put("hbase.table", "table1");
        attributes.put("hbase.row", "row1");
        attributes.put("hbase.cols", "logisland:cq2");

        runner.enqueue("trigger flow file", attributes);
        runner.run();

        runner.assertTransferCount(FetchHBaseRow.REL_FAILURE, 0);
        runner.assertTransferCount(FetchHBaseRow.REL_SUCCESS, 1);
        runner.assertTransferCount(FetchHBaseRow.REL_NOT_FOUND, 0);

        final MockRecord record = runner.getRecordsForRelationship(FetchHBaseRow.REL_SUCCESS).get(0);
        record.assertContentEquals("{\"row\":\"row1\", \"cells\": [{\"fam\":\"logisland\",\"qual\":\"cq2\",\"val\":\"val2\",\"ts\":" + ts1 + "}]}");

        Assert.assertEquals(1, hBaseClientService.getNumScans());
    }

    @Test
    public void testFetchWhenScanThrowsException() {
        hBaseClientService.setThrowException(true);

        runner.setProperty(FetchHBaseRow.TABLE_NAME_FIELD, "table1");
        runner.setProperty(FetchHBaseRow.ROW_ID_FIELD, "row1");
        runner.setProperty(FetchHBaseRow.DESTINATION, FetchHBaseRow.DESTINATION_ATTRIBUTES);

        runner.enqueue("trigger flow file");
        runner.run();

        runner.assertTransferCount(FetchHBaseRow.REL_FAILURE, 1);
        runner.assertTransferCount(FetchHBaseRow.REL_SUCCESS, 0);
        runner.assertTransferCount(FetchHBaseRow.REL_NOT_FOUND, 0);

        Assert.assertEquals(0, hBaseClientService.getNumScans());
    }
*/
}
