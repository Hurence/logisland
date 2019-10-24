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
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class SamplingUtilsTest {

    private static final Logger logger = LoggerFactory.getLogger(SamplingUtilsTest.class);
    @Test
    public void fitBucketSizeTestWithWanted1() {
        Assertions.assertEquals(1, SamplingUtils.fitBucketSize(IntStream.range(0, 1).boxed().collect(Collectors.toList()), 1));
        Assertions.assertEquals(1, SamplingUtils.fitBucketSize(IntStream.range(0, 2).boxed().collect(Collectors.toList()), 1));
        Assertions.assertEquals(1, SamplingUtils.fitBucketSize(IntStream.range(0, 3).boxed().collect(Collectors.toList()), 1));
        Assertions.assertEquals(1, SamplingUtils.fitBucketSize(IntStream.range(0, 4).boxed().collect(Collectors.toList()), 1));
        Assertions.assertEquals(1, SamplingUtils.fitBucketSize(IntStream.range(0, 5).boxed().collect(Collectors.toList()), 1));
        Assertions.assertEquals(1, SamplingUtils.fitBucketSize(IntStream.range(0, 50).boxed().collect(Collectors.toList()), 1));
    }

    @Test
    public void fitBucketSizeTestWithWanted2() {
        Assertions.assertEquals(1, SamplingUtils.fitBucketSize(IntStream.range(0, 1).boxed().collect(Collectors.toList()), 2));
        Assertions.assertEquals(2, SamplingUtils.fitBucketSize(IntStream.range(0, 2).boxed().collect(Collectors.toList()), 2));
        Assertions.assertEquals(2, SamplingUtils.fitBucketSize(IntStream.range(0, 3).boxed().collect(Collectors.toList()), 2));
        Assertions.assertEquals(2, SamplingUtils.fitBucketSize(IntStream.range(0, 4).boxed().collect(Collectors.toList()), 2));
        Assertions.assertEquals(2, SamplingUtils.fitBucketSize(IntStream.range(0, 5).boxed().collect(Collectors.toList()), 2));
        Assertions.assertEquals(2, SamplingUtils.fitBucketSize(IntStream.range(0, 50).boxed().collect(Collectors.toList()), 2));
    }

    @Test
    public void fitBucketSizeTestWithWanted5() {
        Assertions.assertEquals(1, SamplingUtils.fitBucketSize(IntStream.range(0, 1).boxed().collect(Collectors.toList()), 5));
        Assertions.assertEquals(1, SamplingUtils.fitBucketSize(IntStream.range(0, 2).boxed().collect(Collectors.toList()), 5));
        Assertions.assertEquals(1, SamplingUtils.fitBucketSize(IntStream.range(0, 3).boxed().collect(Collectors.toList()), 5));
        Assertions.assertEquals(1, SamplingUtils.fitBucketSize(IntStream.range(0, 4).boxed().collect(Collectors.toList()), 5));
        Assertions.assertEquals(5, SamplingUtils.fitBucketSize(IntStream.range(0, 5).boxed().collect(Collectors.toList()), 5));
        Assertions.assertEquals(5, SamplingUtils.fitBucketSize(IntStream.range(0, 50).boxed().collect(Collectors.toList()), 5));
    }


    @Test
    public void groupedWithBucket1() {
        List<List<Integer>> grouped = SamplingUtils.grouped(IntStream.range(0, 1).boxed().collect(Collectors.toList()), 1).collect(Collectors.toList());
        Assertions.assertEquals(1, grouped.size());
        Assertions.assertEquals(1, grouped.get(0).size());
        List<List<Integer>> grouped2 = SamplingUtils.grouped(IntStream.range(0, 2).boxed().collect(Collectors.toList()), 1).collect(Collectors.toList());
        Assertions.assertEquals(2, grouped2.size());
        Assertions.assertEquals(1, grouped2.get(0).size());
        Assertions.assertEquals(1, grouped2.get(grouped2.size() - 1).size());
        List<List<Integer>> grouped3 = SamplingUtils.grouped(IntStream.range(0, 3).boxed().collect(Collectors.toList()), 1).collect(Collectors.toList());
        Assertions.assertEquals(3, grouped3.size());
        Assertions.assertEquals(1, grouped3.get(0).size());
        Assertions.assertEquals(1, grouped3.get(grouped3.size() - 1).size());
        List<List<Integer>> grouped4 = SamplingUtils.grouped(IntStream.range(0, 5).boxed().collect(Collectors.toList()), 1).collect(Collectors.toList());
        Assertions.assertEquals(5, grouped4.size());
        Assertions.assertEquals(1, grouped4.get(0).size());
        Assertions.assertEquals(1, grouped4.get(grouped4.size() - 1).size());
        List<List<Integer>> grouped5 = SamplingUtils.grouped(IntStream.range(0, 50).boxed().collect(Collectors.toList()), 1).collect(Collectors.toList());
        Assertions.assertEquals(50, grouped5.size());
        Assertions.assertEquals(1, grouped5.get(0).size());
        Assertions.assertEquals(1, grouped5.get(grouped5.size() - 1).size());
        List<List<Integer>> grouped6 = SamplingUtils.grouped(IntStream.range(0, 100).boxed().collect(Collectors.toList()), 1).collect(Collectors.toList());
        Assertions.assertEquals(100, grouped6.size());
        Assertions.assertEquals(1, grouped6.get(0).size());
        Assertions.assertEquals(1, grouped6.get(grouped6.size() - 1).size());
        List<List<Integer>> grouped7 = SamplingUtils.grouped(IntStream.range(0, 200).boxed().collect(Collectors.toList()), 1).collect(Collectors.toList());
        Assertions.assertEquals(200, grouped7.size());
        Assertions.assertEquals(1, grouped7.get(0).size());
        Assertions.assertEquals(1, grouped7.get(grouped7.size() - 1).size());
        List<List<Integer>> grouped8 = SamplingUtils.grouped(IntStream.range(0, 800).boxed().collect(Collectors.toList()), 1).collect(Collectors.toList());
        Assertions.assertEquals(800, grouped8.size());
        Assertions.assertEquals(1, grouped8.get(0).size());
        Assertions.assertEquals(1, grouped8.get(grouped8.size() - 1).size());
    }

    @Test
    public void groupedWithBucket11() {
        List<List<Integer>> grouped = SamplingUtils.grouped(IntStream.range(0, 1).boxed().collect(Collectors.toList()), 11).collect(Collectors.toList());
        Assertions.assertEquals(1, grouped.size());
        Assertions.assertEquals(1, grouped.get(0).size());
        List<List<Integer>> grouped2 = SamplingUtils.grouped(IntStream.range(0, 2).boxed().collect(Collectors.toList()), 11).collect(Collectors.toList());
        Assertions.assertEquals(1, grouped2.size());
        Assertions.assertEquals(2, grouped2.get(0).size());
        Assertions.assertEquals(2, grouped2.get(grouped2.size() - 1).size());
        List<List<Integer>> grouped3 = SamplingUtils.grouped(IntStream.range(0, 3).boxed().collect(Collectors.toList()), 11).collect(Collectors.toList());
        Assertions.assertEquals(1, grouped3.size());
        Assertions.assertEquals(3, grouped3.get(0).size());
        Assertions.assertEquals(3, grouped3.get(grouped3.size() - 1).size());
        List<List<Integer>> grouped4 = SamplingUtils.grouped(IntStream.range(0, 5).boxed().collect(Collectors.toList()), 11).collect(Collectors.toList());
        Assertions.assertEquals(1, grouped4.size());
        Assertions.assertEquals(5, grouped4.get(0).size());
        Assertions.assertEquals(5, grouped4.get(grouped4.size() - 1).size());
        List<List<Integer>> grouped5 = SamplingUtils.grouped(IntStream.range(0, 50).boxed().collect(Collectors.toList()), 11).collect(Collectors.toList());
        Assertions.assertEquals(5, grouped5.size());
        Assertions.assertEquals(11, grouped5.get(0).size());
        Assertions.assertEquals(6, grouped5.get(grouped5.size() - 1).size());
        List<List<Integer>> grouped6 = SamplingUtils.grouped(IntStream.range(0, 100).boxed().collect(Collectors.toList()), 11).collect(Collectors.toList());
        Assertions.assertEquals(10, grouped6.size());
        Assertions.assertEquals(11, grouped6.get(0).size());
        Assertions.assertEquals(1, grouped6.get(grouped6.size() - 1).size());
        List<List<Integer>> grouped7 = SamplingUtils.grouped(IntStream.range(0, 200).boxed().collect(Collectors.toList()), 11).collect(Collectors.toList());
        Assertions.assertEquals(19, grouped7.size());
        Assertions.assertEquals(11, grouped7.get(0).size());
        Assertions.assertEquals(2, grouped7.get(grouped7.size() - 1).size());
        List<List<Integer>> grouped8 = SamplingUtils.grouped(IntStream.range(0, 800).boxed().collect(Collectors.toList()), 11).collect(Collectors.toList());
        Assertions.assertEquals(73, grouped8.size());
        Assertions.assertEquals(11, grouped8.get(0).size());
        Assertions.assertEquals(8, grouped8.get(grouped8.size() - 1).size());
    }
}
