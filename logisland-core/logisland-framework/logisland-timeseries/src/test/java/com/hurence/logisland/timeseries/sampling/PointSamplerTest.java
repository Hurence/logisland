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

import com.hurence.logisland.record.Point;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class PointSamplerTest {

    private static final Logger logger = LoggerFactory.getLogger(PointSamplerTest.class);

    private List<Point> getPoints() {
        return Arrays.asList(
                new Point(0, 1L, 48d),
                new Point(0, 2L, 52d),
                new Point(0, 3L, 60d)
        );
    }

    @Test
    public void testAvgSampler() {
        Sampler<Point> sampler = SamplerFactory.getPointSampler(SamplingAlgorithm.AVERAGE, 3);
        List<Point> sampled = sampler.sample(getPoints());
        Assertions.assertEquals(1, sampled.size());
        Point point1 = sampled.get(0);
        Assertions.assertEquals(1L, point1.getTimestamp());
        Assertions.assertEquals(53.333333333333336d, point1.getValue());
    }

    @Test
    public void testAvgSamplerNoFullBucket() {
        Sampler<Point> sampler = SamplerFactory.getPointSampler(SamplingAlgorithm.AVERAGE,2);
        List<Point> sampled = sampler.sample(getPoints());
        Assertions.assertEquals(2, sampled.size());
        Point point1 = sampled.get(0);
        Assertions.assertEquals(1L, point1.getTimestamp());
        Assertions.assertEquals(50d, point1.getValue());
        Point point2 = sampled.get(1);
        Assertions.assertEquals(3L, point2.getTimestamp());
        Assertions.assertEquals(60d, point2.getValue());
    }

    @Test
    public void testFirstItemSampler() {
        Sampler<Point> sampler = SamplerFactory.getPointSampler(SamplingAlgorithm.FIRST_ITEM, 3);
        List<Point> sampled = sampler.sample(getPoints());
        Assertions.assertEquals(1, sampled.size());
        Point point1 = sampled.get(0);
        Assertions.assertEquals(1L, point1.getTimestamp());
        Assertions.assertEquals(48d, point1.getValue());
    }

    @Test
    public void testFirstItemSamplerNoFullBucket() {
        Sampler<Point> sampler = SamplerFactory.getPointSampler(SamplingAlgorithm.FIRST_ITEM,2);
        List<Point> sampled = sampler.sample(getPoints());
        Assertions.assertEquals(2, sampled.size());
        Point point1 = sampled.get(0);
        Assertions.assertEquals(1L, point1.getTimestamp());
        Assertions.assertEquals(48d, point1.getValue());
        Point point2 = sampled.get(1);
        Assertions.assertEquals(3L, point2.getTimestamp());
        Assertions.assertEquals(60d, point2.getValue());
    }

    @Test
    public void testNoneSampler() {
        Sampler<Point> sampler = SamplerFactory.getPointSampler(SamplingAlgorithm.NONE, 3);
        List<Point> sampled = sampler.sample(getPoints());
        Assertions.assertEquals(3, sampled.size());
        Point point1 = sampled.get(0);
        Assertions.assertEquals(1L, point1.getTimestamp());
        Assertions.assertEquals(48d, point1.getValue());
        Point point2 = sampled.get(1);
        Assertions.assertEquals(2L, point2.getTimestamp());
        Assertions.assertEquals(52d, point2.getValue());
        Point point3 = sampled.get(2);
        Assertions.assertEquals(3L, point3.getTimestamp());
        Assertions.assertEquals(60d, point3.getValue());
    }

    @Test
    public void testNoneSamplerNoFullBucket() {
        Sampler<Point> sampler = SamplerFactory.getPointSampler(SamplingAlgorithm.NONE,2);
        List<Point> sampled = sampler.sample(getPoints());
        Assertions.assertEquals(3, sampled.size());
        Point point1 = sampled.get(0);
        Assertions.assertEquals(1L, point1.getTimestamp());
        Assertions.assertEquals(48d, point1.getValue());
        Point point2 = sampled.get(1);
        Assertions.assertEquals(2L, point2.getTimestamp());
        Assertions.assertEquals(52d, point2.getValue());
        Point point3 = sampled.get(2);
        Assertions.assertEquals(3L, point3.getTimestamp());
        Assertions.assertEquals(60d, point3.getValue());
    }
}
