/**
 * Copyright (C) 2016 Hurence (support@hurence.com)
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
package com.caseystella.analytics.outlier.streaming.mad;

import com.caseystella.analytics.DataPoint;
import com.caseystella.analytics.outlier.Severity;
import com.caseystella.analytics.outlier.streaming.OutlierConfig;
import com.caseystella.analytics.util.JSONUtil;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class SketchyMovingMADTest {

    /**

     */
    public static String madConfig = " {\n" +
            "     \"rotationPolicy\" : {\n" +
            "     \"type\" : \"BY_AMOUNT\"\n" +
            "     ,\"amount\" : 100\n" +
            "     ,\"unit\" : \"POINTS\"\n" +
            "     }\n" +
            "     ,\"chunkingPolicy\" : {\n" +
            "     \"type\" : \"BY_AMOUNT\"\n" +
            "     ,\"amount\" : 10\n" +
            "     ,\"unit\" : \"POINTS\"\n" +
            "     }\n" +
            "     ,\"globalStatistics\" : {\n" +
            "     \"min\" : -10000\n" +
            "     }\n" +
            "     ,\"sketchyOutlierAlgorithm\" : \"SKETCHY_MOVING_MAD\"\n" +
            "     ,\"config\" : {\n" +
            "     \"minAmountToPredict\" : 100\n" +
            "     ,\"zscoreCutoffs\" : {\n" +
            "     \"NORMAL\" : 3.5\n" +
            "     ,\"MODERATE_OUTLIER\" : 5\n" +
            "     }\n" +
            "     }\n" +
            "     }";

    public static double getValAtModifiedZScore(double zScore, double mad, double median) {
        return (mad * zScore) / SketchyMovingMAD.ZSCORE + median;

    }

    @Test
    public void testSketchyMovingMAD() throws IOException {
        Random r = new Random(0);
        List<DataPoint> points = new ArrayList<>();
        DescriptiveStatistics stats = new DescriptiveStatistics();
        DescriptiveStatistics medianStats = new DescriptiveStatistics();
        OutlierConfig config = JSONUtil.INSTANCE.load(madConfig, OutlierConfig.class);
        SketchyMovingMAD madAlgo = ((SketchyMovingMAD) config.getSketchyOutlierAlgorithm()).withConfig(config);
        int i = 0;
        for (i = 0; i < 10000; ++i) {
            double val = r.nextDouble() * 1000 - 10000;
            stats.addValue(val);
            DataPoint dp = (new DataPoint(i, val, null, "foo"));
            madAlgo.analyze(dp);
            points.add(dp);
        }
        for (DataPoint dp : points) {
            medianStats.addValue(Math.abs(dp.getValue() - stats.getPercentile(50)));
        }
        double mad = medianStats.getPercentile(50);
        double median = stats.getPercentile(50);
        {
            double val = getValAtModifiedZScore(3.6, mad, median);
            System.out.println("MODERATE => " + val);
            DataPoint dp = (new DataPoint(i++, val, null, "foo"));
            Severity s = madAlgo.analyze(dp).getSeverity();
            Assert.assertTrue(s == Severity.MODERATE_OUTLIER);
        }
        {
            double val = getValAtModifiedZScore(6, mad, median);
            System.out.println("SEVERE => " + val);
            DataPoint dp = (new DataPoint(i++, val, null, "foo"));
            Severity s = madAlgo.analyze(dp).getSeverity();
            Assert.assertTrue(s == Severity.SEVERE_OUTLIER);
        }

        Assert.assertTrue(madAlgo.getMedianDistributions().get("foo").getAmount() <= 110);
        Assert.assertTrue(madAlgo.getMedianDistributions().get("foo").getChunks().size() <= 12);
    }
}
