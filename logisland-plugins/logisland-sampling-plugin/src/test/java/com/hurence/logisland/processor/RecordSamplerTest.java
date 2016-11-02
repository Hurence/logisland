/**
 * Copyright (C) 2016 Hurence (bailet.thomas@gmail.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.processor;


import com.hurence.logisland.record.FieldDictionary;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
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

public class RecordSamplerTest {

    static String RAW_DATA1 = "/data/raw-data1.txt";
    static String RAW_DATA2 = "/data/raw-data2.txt";
    static String SAMPLED_RECORD = "sampled_record";

    private static Logger logger = LoggerFactory.getLogger(RecordSamplerTest.class);

    private Record createRecord(long time, double value) {
        return new StandardRecord(SAMPLED_RECORD)
                .setField(FieldDictionary.RECORD_VALUE, FieldType.DOUBLE, value)
                .setField(FieldDictionary.RECORD_TIME, FieldType.LONG, time);
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
                res.add(createRecord(lts, dval));
            }
            return res;
        } finally {
            br.close();
        }
    }

    private Record[] createRawData(int sampleCount) {
        Record[] res = new Record[sampleCount];
        for (int i = 0; i < sampleCount; i++) {
            res[i] = createRecord(
                    ((long) i + 1000000L),
                    (Math.sin((double) i / 300)) * 300);
        }

        return res;
    }


    @Test
    public void validateNoSampling() {
        final TestRunner testRunner = TestRunners.newTestRunner(new RecordSampler());
        testRunner.setProperty(RecordSampler.VALUE_FIELD, FieldDictionary.RECORD_VALUE);
        testRunner.setProperty(RecordSampler.TIME_FIELD, FieldDictionary.RECORD_TIME);
        testRunner.setProperty(RecordSampler.SAMPLING_ALGORITHM, "none");
        testRunner.setProperty(RecordSampler.SAMPLING_PARAMETER, "0");
        testRunner.assertValid();

        int recordsCount = 2000;
        testRunner.clearQueues();
        testRunner.enqueue(createRawData(recordsCount));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(recordsCount);
    }

    @Test
    public void validateFirstItemSampling() {
        int recordsCount = 2000;
        final TestRunner testRunner = TestRunners.newTestRunner(new RecordSampler());
        testRunner.setProperty(RecordSampler.VALUE_FIELD, FieldDictionary.RECORD_VALUE);
        testRunner.setProperty(RecordSampler.TIME_FIELD, FieldDictionary.RECORD_TIME);
        testRunner.setProperty(RecordSampler.SAMPLING_ALGORITHM, "first_item");
        testRunner.setProperty(RecordSampler.SAMPLING_PARAMETER, "230"); // bucket size => 9 buckets
        testRunner.assertValid();


        testRunner.clearQueues();
        testRunner.enqueue(createRawData(recordsCount));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(9);
        testRunner.getOutputRecords().forEach(System.out::println);
    }
}
