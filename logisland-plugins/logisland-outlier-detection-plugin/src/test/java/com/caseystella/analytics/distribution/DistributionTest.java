/**
 * Copyright (C) 2016 Hurence (bailet.thomas@gmail.com)
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
package com.caseystella.analytics.distribution;

import com.caseystella.analytics.DataPoint;
import com.caseystella.analytics.distribution.scaling.ScalingFunctions;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class DistributionTest {
    @Test
    public void testQuantiles() {
        Random r = new Random(0);
        List<DataPoint> points = new ArrayList<>();
        DescriptiveStatistics stats = new DescriptiveStatistics();
        Distribution distribution = null;
        for(int i = 0; i < 100;++i) {
            double val = r.nextDouble()*1000;
            DataPoint dp = (new DataPoint(i, val, null, "foo"));
            points.add(dp);
            stats.addValue(val);
            if(distribution == null) {
                distribution = new Distribution(dp, ScalingFunctions.NONE, new GlobalStatistics());
            }
            else {
                distribution.addDataPoint(dp, ScalingFunctions.NONE);
            }
        }
        double realMedian = stats.getPercentile(50);
        double approxMedian = distribution.getMedian();
        System.out.println("mean and std dev: " + stats.getMean() + ", " + Math.sqrt(stats.getVariance()));
        System.out.println("Real : " + realMedian + ", approx: " + approxMedian);
        Assert.assertTrue(Math.abs(realMedian - approxMedian) < 5);
    }
}
