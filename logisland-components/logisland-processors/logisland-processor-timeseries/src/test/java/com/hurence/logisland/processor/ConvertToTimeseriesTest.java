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


import com.hurence.logisland.record.*;
import com.hurence.logisland.timeseries.converter.compaction.BinaryCompactionConverter;
import com.hurence.logisland.util.runner.MockRecord;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ConvertToTimeseriesTest {

    static String SAMPLED_RECORD = "sampled_record";

    private static Logger logger = LoggerFactory.getLogger(ConvertToTimeseriesTest.class);

    private Record createRecord(String name, long time, double value) {
        return new StandardRecord(SAMPLED_RECORD)
                .setField(FieldDictionary.RECORD_VALUE, FieldType.DOUBLE, value)
                .setField(FieldDictionary.RECORD_TIME, FieldType.LONG, time)
                .setField(FieldDictionary.RECORD_NAME, FieldType.STRING, name);
    }

    private List<Record> loadData(String filename) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(filename));
        try {
            List<Record> res = new ArrayList<>();
            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.startsWith("#")) {
                    continue;
                }
                int pos = line.indexOf(",");
                String ts = line.substring(0, pos);
                String val = line.substring(pos + 1);
                Long lts = Long.parseLong(ts);
                Double dval = Double.parseDouble(val);
                res.add(createRecord(filename, lts, dval));
            }
            return res;
        } finally {
            br.close();
        }
    }

    private Record[] createRawData(String name, int sampleCount) {
        Record[] res = new Record[sampleCount];
        for (int i = 0; i < sampleCount; i++) {
            res[i] = createRecord(
                    name,
                    i + 1000000L,
                    Math.sin(4 * Math.PI * i / sampleCount)
            );
        }

        return res;
    }

    private void printRecords(List<MockRecord> records) {
        records.forEach(r ->
                System.out.println(r.getField(FieldDictionary.RECORD_TIME).asLong() + ":" +
                        r.getField(FieldDictionary.RECORD_VALUE).asDouble())
        );

    }


    @Test
    public void validateChunking() {
        final String name = "cpu.load";
        final TestRunner testRunner = TestRunners.newTestRunner(new ConvertToTimeseries());
        testRunner.setProperty(ConvertToTimeseries.GROUPBY, FieldDictionary.RECORD_NAME);

        //testRunner.setProperty(ConvertToTimeseries.METRIC, FieldDictionary.RECORD_TIME);
        testRunner.assertValid();

        int recordsCount = 2000;
        testRunner.clearQueues();
        testRunner.enqueue(createRawData(name, recordsCount));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertFieldExists(FieldDictionary.CHUNK_START);
        out.assertFieldExists(FieldDictionary.CHUNK_END);
        out.assertFieldExists(FieldDictionary.RECORD_NAME);
        out.assertFieldExists(FieldDictionary.RECORD_TYPE);
        out.assertFieldExists(FieldDictionary.CHUNK_VALUE);

        out.assertFieldEquals(FieldDictionary.CHUNK_START, 1000000);
        out.assertFieldEquals(FieldDictionary.CHUNK_END, 1001999);
        out.assertFieldEquals(FieldDictionary.RECORD_NAME, "cpu.load");
        out.assertFieldEquals(FieldDictionary.RECORD_TYPE, SAMPLED_RECORD);
        out.assertFieldTypeEquals(FieldDictionary.CHUNK_VALUE, FieldType.BYTES);
        out.assertFieldEquals(FieldDictionary.CHUNK_SIZE, 2000);
        out.assertFieldEquals(FieldDictionary.CHUNK_SIZE_BYTES, 8063);

        out.assertRecordSizeEquals(7);


        byte[] binaryTimeseries = out.getField(FieldDictionary.CHUNK_VALUE).asBytes();


        BinaryCompactionConverter.Builder builder = new BinaryCompactionConverter.Builder();
        BinaryCompactionConverter converter = builder.build();


        try {
            List<Point> points = converter.unCompressPoints(binaryTimeseries, 1000000, 1001999);

            assertEquals(points.size(), recordsCount);
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    @Test
    public void validateMetrics() {
        final String name = "cpu.load";
        final TestRunner testRunner = TestRunners.newTestRunner(new ConvertToTimeseries());
        testRunner.setProperty(ConvertToTimeseries.GROUPBY, FieldDictionary.RECORD_NAME);
        testRunner.setProperty(ConvertToTimeseries.METRIC, "min;max;avg;trend;outlier;sax:10,0.01,20");
        testRunner.assertValid();

        int recordsCount = 2000;
        Record[] records = createRawData(name, recordsCount);
        testRunner.clearQueues();
        testRunner.enqueue(records);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);

        out.assertFieldExists(FieldDictionary.CHUNK_MIN);
        out.assertFieldExists(FieldDictionary.CHUNK_MAX);
        out.assertFieldExists(FieldDictionary.CHUNK_AVG);
        out.assertFieldExists(FieldDictionary.CHUNK_TREND);
        out.assertFieldExists(FieldDictionary.CHUNK_OUTLIER);
        out.assertFieldExists(FieldDictionary.CHUNK_SAX);

        out.assertFieldEquals(FieldDictionary.CHUNK_MIN, -1.0);
        out.assertFieldEquals(FieldDictionary.CHUNK_MAX, 1.0);
        out.assertFieldEquals(FieldDictionary.CHUNK_AVG, 0.0);
        out.assertFieldEquals(FieldDictionary.CHUNK_TREND, false);
        out.assertFieldEquals(FieldDictionary.CHUNK_OUTLIER, false);
        out.assertFieldEquals(FieldDictionary.CHUNK_SAX, "gijigdbabdgijigdbabd");
        out.assertRecordSizeEquals(13);

    }

    @Test
    public void validateSumAndFirst() {
        final String name = "cpu.load";
        final TestRunner testRunner = TestRunners.newTestRunner(new ConvertToTimeseries());
        testRunner.setProperty(ConvertToTimeseries.GROUPBY, FieldDictionary.RECORD_NAME);
        testRunner.setProperty(ConvertToTimeseries.METRIC, "sum;first");
        testRunner.assertValid();

        int recordsCount = 3;
        Record[] records = createRawData(name, recordsCount);
        testRunner.clearQueues();
        testRunner.enqueue(
                createRecord(name, 1000000L, 0),
                createRecord(name, 1000002L, 2),
                createRecord(name, 1000004L, 3.5)
        );
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);

        out.assertFieldEquals(FieldDictionary.CHUNK_SIZE, 3);
        out.assertFieldEquals(FieldDictionary.CHUNK_SUM, 5.5);
        out.assertFieldEquals(FieldDictionary.CHUNK_FIRST_VALUE, 0.0);
        out.assertRecordSizeEquals(9);

    }
}
