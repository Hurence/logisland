package com.caseystella.analytics.distribution;

import com.caseystella.analytics.DataPoint;
import com.caseystella.analytics.outlier.streaming.OutlierConfig;
import com.caseystella.analytics.util.JSONUtil;
import org.adrianwalker.multilinestring.Multiline;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
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
    public static DataPoint nextDataPoint(Random r, LongWritable ts, long delta, List<DataPoint> points) {
        double val = r.nextDouble() * 1000;
        DataPoint dp = (new DataPoint(ts.get(), val, null, "foo"));
        if(points != null) {
            points.add(dp);
        }
        ts.set(ts.get() + delta);
        return dp;
    }
    @Test
    public void rotationTest() throws Exception {
        OutlierConfig config = JSONUtil.INSTANCE.load(amountConfig, OutlierConfig.class);
        final IntWritable numChunksAdded = new IntWritable(0);
        final IntWritable numRotations= new IntWritable(0);
        Distribution.Context context = new Distribution.Context(0, 0) {
            @Override
            protected void addChunk(Distribution d) {
                super.addChunk(d);
                numChunksAdded.set(numChunksAdded.get() + 1);
            }

            @Override
            protected void rotate() {
                super.rotate();
                numRotations.set(numRotations.get() + 1);
            }
        };
        GlobalStatistics globalStats= new GlobalStatistics();
        Random r = new Random(0);
        List<DataPoint> points = new ArrayList<>();
        DescriptiveStatistics stats = new DescriptiveStatistics();
        LongWritable ts = new LongWritable(0L);
        Assert.assertEquals(context.getAmount(), 0);
        context.addDataPoint(nextDataPoint(r, ts, 1, points), config.getRotationPolicy(), config.getChunkingPolicy(), config.getScalingFunction(), globalStats);
        Assert.assertEquals(context.getAmount(), 1);
        Assert.assertEquals(context.getChunks().size(), 1);
        Assert.assertEquals(numChunksAdded.get(), 1);
        Assert.assertEquals(numRotations.get(), 0);
        for(int i = 1; i < 10;++i) {
            context.addDataPoint(nextDataPoint(r, ts, 1, points), config.getRotationPolicy(), config.getChunkingPolicy(), config.getScalingFunction(), globalStats);
            Assert.assertEquals(context.getChunks().size(), 1);
        }
        //at the 11th point, we should create a new chunk
        context.addDataPoint(nextDataPoint(r, ts, 1, points), config.getRotationPolicy(), config.getChunkingPolicy(), config.getScalingFunction(), globalStats);
        Assert.assertEquals(context.getChunks().size(), 2);
        Assert.assertEquals(numChunksAdded.get(), 2);
        Assert.assertEquals(context.getAmount(), 11);
        Assert.assertEquals(numRotations.get(), 0);
        for(int i = 12;i <= 110;++i) {
            context.addDataPoint(nextDataPoint(r, ts, 1, points), config.getRotationPolicy(), config.getChunkingPolicy(), config.getScalingFunction(), globalStats);
        }
        Assert.assertEquals(11, numChunksAdded.get());
        Assert.assertEquals(0, numRotations.get());
        //at the 111th point, we should create a rotation
        context.addDataPoint(nextDataPoint(r, ts, 1, points), config.getRotationPolicy(), config.getChunkingPolicy(), config.getScalingFunction(), globalStats);
        Assert.assertEquals(12, numChunksAdded.get());
        Assert.assertEquals(11, context.getChunks().size());
        Assert.assertEquals(1, numRotations.get());
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
