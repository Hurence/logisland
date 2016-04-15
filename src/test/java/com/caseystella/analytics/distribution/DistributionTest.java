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
