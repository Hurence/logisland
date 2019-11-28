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
package com.hurence.logisland.timeseries.sampling;

import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.timeseries.sampling.record.RecordSampler;
import com.hurence.logisland.util.runner.MockRecord;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class RecordSamplerTest {

    private static final Logger logger = LoggerFactory.getLogger(RecordSamplerTest.class);
    private static final String VALUE = "value";
    private static final String TIMESTAMP = "timestamp";

    private List<Record> getRecords() {
        List<Record> records = new ArrayList<>();
        Record record1 = new StandardRecord();
        record1.setField(VALUE, FieldType.DOUBLE, 48d);
        record1.setField(TIMESTAMP, FieldType.LONG, 1L);
        records.add(record1);
        Record record2 = new StandardRecord();
        record2.setField(VALUE, FieldType.DOUBLE, 52d);
        record2.setField(TIMESTAMP, FieldType.LONG, 2L);
        records.add(record2);
        Record record3 = new StandardRecord();
        record3.setField(VALUE, FieldType.DOUBLE, 60d);
        record3.setField(TIMESTAMP, FieldType.LONG, 3L);
        records.add(record3);
        return records;
    }

    @Test
    public void testAvgSampler() {
        RecordSampler sampler = SamplerFactory.getRecordSampler(SamplingAlgorithm.AVERAGE, VALUE, TIMESTAMP, 3);
        List<Record> sampled = sampler.sample(getRecords());
        Assertions.assertEquals(1, sampled.size());
        MockRecord record1 = new MockRecord(sampled.get(0));
        record1.assertFieldEquals(TIMESTAMP, 1L);
        record1.assertFieldEquals(VALUE, 53.333333333333336d);
    }

    @Test
    public void testAvgSamplerNoFullBucket() {
        RecordSampler sampler = SamplerFactory.getRecordSampler(SamplingAlgorithm.AVERAGE, VALUE, TIMESTAMP, 2);
        List<Record> sampled = sampler.sample(getRecords());
        Assertions.assertEquals(2, sampled.size());
        MockRecord record1 = new MockRecord(sampled.get(0));
        record1.assertFieldEquals(TIMESTAMP, 1L);
        record1.assertFieldEquals(VALUE, 50d);
        MockRecord record2 = new MockRecord(sampled.get(1));
        record2.assertFieldEquals(TIMESTAMP, 3L);
        record2.assertFieldEquals(VALUE, 60d);
    }

    @Test
    public void testFirstItemSampler() {
        RecordSampler sampler = SamplerFactory.getRecordSampler(SamplingAlgorithm.FIRST_ITEM, VALUE, TIMESTAMP, 3);
        List<Record> sampled = sampler.sample(getRecords());
        Assertions.assertEquals(1, sampled.size());
        MockRecord record1 = new MockRecord(sampled.get(0));
        record1.assertFieldEquals(TIMESTAMP, 1L);
        record1.assertFieldEquals(VALUE, 48d);
    }

    @Test
    public void testFirstItemSamplerNoFullBucket() {
        RecordSampler sampler = SamplerFactory.getRecordSampler(SamplingAlgorithm.FIRST_ITEM, VALUE, TIMESTAMP, 2);
        List<Record> sampled = sampler.sample(getRecords());
        Assertions.assertEquals(2, sampled.size());
        MockRecord record1 = new MockRecord(sampled.get(0));
        record1.assertFieldEquals(TIMESTAMP, 1L);
        record1.assertFieldEquals(VALUE, 48d);
        MockRecord record2 = new MockRecord(sampled.get(1));
        record2.assertFieldEquals(TIMESTAMP, 3L);
        record2.assertFieldEquals(VALUE, 60d);
    }

    //TODO
//    @Test
//    public void testLTTBSampler() {
//        RecordSampler sampler = SamplerFactory.getRecordSampler(SamplingAlgorithm.LTTB, VALUE, TIMESTAMP, 3);
//        List<Record> sampled = sampler.sample(getRecords());
//        Assertions.assertEquals(1, sampled.size());
//        MockRecord record1 = new MockRecord(sampled.get(0));
//        record1.assertFieldEquals(TIMESTAMP, 1L);
//        record1.assertFieldEquals(VALUE, 48d);
//    }
//
//    @Test
//    public void testLTTBSamplerNoFullBucket() {
//        RecordSampler sampler = SamplerFactory.getRecordSampler(SamplingAlgorithm.LTTB, VALUE, TIMESTAMP, 2);
//        List<Record> sampled = sampler.sample(getRecords());
//        Assertions.assertEquals(2, sampled.size());
//        MockRecord record1 = new MockRecord(sampled.get(0));
//        record1.assertFieldEquals(TIMESTAMP, 1L);
//        record1.assertFieldEquals(VALUE, 48d);
//        MockRecord record2 = new MockRecord(sampled.get(1));
//        record2.assertFieldEquals(TIMESTAMP, 3L);
//        record2.assertFieldEquals(VALUE, 60d);
//    }
//
//    @Test
//    public void testMinMaxSampler() {
//        RecordSampler sampler = SamplerFactory.getRecordSampler(SamplingAlgorithm.MIN_MAX, VALUE, TIMESTAMP, 3);
//        List<Record> sampled = sampler.sample(getRecords());
//        Assertions.assertEquals(1, sampled.size());
//        MockRecord record1 = new MockRecord(sampled.get(0));
//        record1.assertFieldEquals(TIMESTAMP, 1L);
//        record1.assertFieldEquals(VALUE, 48d);
//    }
//
//    @Test
//    public void testMinMaxSamplerNoFullBucket() {
//        RecordSampler sampler = SamplerFactory.getRecordSampler(SamplingAlgorithm.MIN_MAX, VALUE, TIMESTAMP, 2);
//        List<Record> sampled = sampler.sample(getRecords());
//        Assertions.assertEquals(2, sampled.size());
//        MockRecord record1 = new MockRecord(sampled.get(0));
//        record1.assertFieldEquals(TIMESTAMP, 1L);
//        record1.assertFieldEquals(VALUE, 48d);
//        MockRecord record2 = new MockRecord(sampled.get(1));
//        record2.assertFieldEquals(TIMESTAMP, 3L);
//        record2.assertFieldEquals(VALUE, 60d);
//    }
//
//    @Test
//    public void testModeMedianSampler() {
//        RecordSampler sampler = SamplerFactory.getRecordSampler(SamplingAlgorithm.MODE_MEDIAN, VALUE, TIMESTAMP, 3);
//        List<Record> sampled = sampler.sample(getRecords());
//        Assertions.assertEquals(1, sampled.size());
//        MockRecord record1 = new MockRecord(sampled.get(0));
//        record1.assertFieldEquals(TIMESTAMP, 1L);
//        record1.assertFieldEquals(VALUE, 48d);
//    }
//
//    @Test
//    public void testModeMedianSamplerNoFullBucket() {
//        RecordSampler sampler = SamplerFactory.getRecordSampler(SamplingAlgorithm.MODE_MEDIAN, VALUE, TIMESTAMP, 2);
//        List<Record> sampled = sampler.sample(getRecords());
//        Assertions.assertEquals(2, sampled.size());
//        MockRecord record1 = new MockRecord(sampled.get(0));
//        record1.assertFieldEquals(TIMESTAMP, 1L);
//        record1.assertFieldEquals(VALUE, 48d);
//        MockRecord record2 = new MockRecord(sampled.get(1));
//        record2.assertFieldEquals(TIMESTAMP, 3L);
//        record2.assertFieldEquals(VALUE, 60d);
//    }

    @Test
    public void testNoneSampler() {
        RecordSampler sampler = SamplerFactory.getRecordSampler(SamplingAlgorithm.NONE, VALUE, TIMESTAMP, 3);
        List<Record> sampled = sampler.sample(getRecords());
        Assertions.assertEquals(3, sampled.size());
        MockRecord record1 = new MockRecord(sampled.get(0));
        record1.assertFieldEquals(TIMESTAMP, 1L);
        record1.assertFieldEquals(VALUE, 48d);
        MockRecord record2 = new MockRecord(sampled.get(1));
        record2.assertFieldEquals(TIMESTAMP, 2L);
        record2.assertFieldEquals(VALUE, 52d);
        MockRecord record3 = new MockRecord(sampled.get(2));
        record3.assertFieldEquals(TIMESTAMP, 3L);
        record3.assertFieldEquals(VALUE, 60d);
    }

    @Test
    public void testNoneSamplerNoFullBucket() {
        RecordSampler sampler = SamplerFactory.getRecordSampler(SamplingAlgorithm.NONE, VALUE, TIMESTAMP, -1);
        List<Record> sampled = sampler.sample(getRecords());
        Assertions.assertEquals(3, sampled.size());
        MockRecord record1 = new MockRecord(sampled.get(0));
        record1.assertFieldEquals(TIMESTAMP, 1L);
        record1.assertFieldEquals(VALUE, 48d);
        MockRecord record2 = new MockRecord(sampled.get(1));
        record2.assertFieldEquals(TIMESTAMP, 2L);
        record2.assertFieldEquals(VALUE, 52d);
        MockRecord record3 = new MockRecord(sampled.get(2));
        record3.assertFieldEquals(TIMESTAMP, 3L);
        record3.assertFieldEquals(VALUE, 60d);
    }
}
