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
package com.caseystella.analytics.distribution;

import com.caseystella.analytics.DataPoint;
import com.caseystella.analytics.outlier.streaming.OutlierConfig;
import com.caseystella.analytics.util.JSONUtil;
import com.hurence.logisland.util.string.Multiline;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class RotationTest {

    /**
     {
     "rotationPolicy" : {
                        "type" : "BY_AMOUNT"
                       ,"amount" : 100
                       ,"unit" : "POINTS"
                        }
     ,"chunkingPolicy" : {
                        "type" : "BY_AMOUNT"
                       ,"amount" : 10
                       ,"unit" : "POINTS"
                         }
     }
     */
    @Multiline
    public static String amountConfig;

    public static DataPoint nextDataPoint(Random r, long ts, long delta, List<DataPoint> points) {
        double val = r.nextDouble() * 1000;
        DataPoint dp = (new DataPoint(ts, val, null, "foo"));
        if(points != null) {
            points.add(dp);
        }
        ts +=  delta;
        return dp;
    }

    @Test
    public void rotationTest() throws Exception {
        OutlierConfig config = JSONUtil.INSTANCE.load(amountConfig, OutlierConfig.class);
        final int[] numChunksAdded = {0};
        final int[] numRotations = {0};
        Distribution.Context context = new Distribution.Context(0, 0) {
            @Override
            protected void addChunk(Distribution d) {
                super.addChunk(d);
                numChunksAdded[0] += 1;
            }

            @Override
            protected void rotate() {
                super.rotate();
                numRotations[0] += 1;
            }
        };
        GlobalStatistics globalStats= new GlobalStatistics();
        Random r = new Random(0);
        List<DataPoint> points = new ArrayList<>();
        DescriptiveStatistics stats = new DescriptiveStatistics();
        long ts = 0L;
        Assert.assertEquals(context.getAmount(), 0);
        context.addDataPoint(nextDataPoint(r, ts, 1, points), config.getRotationPolicy(), config.getChunkingPolicy(), config.getScalingFunction(), globalStats);
        Assert.assertEquals(context.getAmount(), 1);
        Assert.assertEquals(context.getChunks().size(), 1);
        Assert.assertEquals(numChunksAdded[0], 1);
        Assert.assertEquals(numRotations[0], 0);
        for(int i = 1; i < 10;++i) {
            context.addDataPoint(nextDataPoint(r, ts, 1, points), config.getRotationPolicy(), config.getChunkingPolicy(), config.getScalingFunction(), globalStats);
            Assert.assertEquals(context.getChunks().size(), 1);
        }
        //at the 11th point, we should create a new chunk
        context.addDataPoint(nextDataPoint(r, ts, 1, points), config.getRotationPolicy(), config.getChunkingPolicy(), config.getScalingFunction(), globalStats);
        Assert.assertEquals(context.getChunks().size(), 2);
        Assert.assertEquals(numChunksAdded[0], 2);
        Assert.assertEquals(context.getAmount(), 11);
        Assert.assertEquals(numRotations[0], 0);
        for(int i = 12;i <= 110;++i) {
            context.addDataPoint(nextDataPoint(r, ts, 1, points), config.getRotationPolicy(), config.getChunkingPolicy(), config.getScalingFunction(), globalStats);
        }
        Assert.assertEquals(11, numChunksAdded[0]);
        Assert.assertEquals(0, numRotations[0]);
        //at the 111th point, we should create a rotation
        context.addDataPoint(nextDataPoint(r, ts, 1, points), config.getRotationPolicy(), config.getChunkingPolicy(), config.getScalingFunction(), globalStats);
        Assert.assertEquals(12, numChunksAdded[0]);
        Assert.assertEquals(11, context.getChunks().size());
        Assert.assertEquals(1, numRotations[0]);
        //rotates just past the rotation cutoff (ensuring that we keep at least the last 100 entries in there)
        Assert.assertEquals(context.getAmount(), 101);
        for(int i = 111;i <= 150;++i) {
            context.addDataPoint(nextDataPoint(r, ts, 1, points), config.getRotationPolicy(), config.getChunkingPolicy(), config.getScalingFunction(), globalStats);
        }
        //no matter how far we go in the stream, we always stay at 11 chunks and a total number of values in the distribution of <= 110 (i.e. between the cutoff and cutoff + a chunk)
        Assert.assertEquals(11, context.getChunks().size());
        Assert.assertTrue(context.getAmount() <= 110);
    }
}
