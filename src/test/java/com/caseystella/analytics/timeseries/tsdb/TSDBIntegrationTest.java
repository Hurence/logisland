package com.caseystella.analytics.timeseries.tsdb;

import com.caseystella.analytics.DataPoint;
import com.caseystella.analytics.distribution.SimpleTimeRange;
import com.caseystella.analytics.integration.ComponentRunner;
import com.caseystella.analytics.integration.UnableToStartException;
import com.caseystella.analytics.integration.components.TSDBComponent;
import com.caseystella.analytics.outlier.Severity;
import com.caseystella.analytics.timeseries.TimeseriesDatabaseHandlers;
import com.caseystella.analytics.util.DistributionUtil;
import com.google.common.base.Function;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;

public class TSDBIntegrationTest {


    @Test
    public void test() throws UnableToStartException, IOException, InterruptedException {
        Random r = new Random(0);
        List<DataPoint> points = new ArrayList<>();
        long offset = System.currentTimeMillis();
        for(int i = 0; i < 100;++i) {
            double val = r.nextDouble()*1000;
            DataPoint dp = (new DataPoint(offset + i, val, new HashMap<String, String>(), "foo"));
            points.add(dp);
        }
        String metric = "test.metric";
        final TSDBComponent tsdb = new TSDBComponent().withMetrics( metric
                                                                  );
        //setLogging();
        ComponentRunner runner = new ComponentRunner.Builder()
                                                    .withComponent("tsdb", tsdb)
                                                    .build();
        runner.start();
        try {
            TSDBHandler handler = new TSDBHandler();
            handler.configure(new HashMap<String, Object>() {{
               for(Map.Entry<String, String> kv : tsdb.getConfig()) {
                   put(kv.getKey(), kv.getValue());
               }
            }});

            int i = 0;
            final DescriptiveStatistics latencyStats = new DescriptiveStatistics();
            int numPtsAdded = 0;
            long start = System.currentTimeMillis();
            for (DataPoint dp : points) {
                if ( i % 7 == 0) {
                    numPtsAdded++;
                    handler.persist(metric
                                   , dp
                                   , TimeseriesDatabaseHandlers.getTags(dp, TimeseriesDatabaseHandlers.PROSPECTIVE_TYPE, null )
                                   , new TimingCallback(System.currentTimeMillis(), latencyStats)
                                   );
                }
                numPtsAdded++;
                handler.persist(metric
                               , dp
                               , TimeseriesDatabaseHandlers.getTags(dp, TimeseriesDatabaseHandlers.RAW_TYPE, null)
                                , new TimingCallback(System.currentTimeMillis(), latencyStats)
                               );
                i++;
            }
            while(latencyStats.getN() != numPtsAdded) {
                Thread.sleep(500);
            }
            double latency = 1.0*(System.currentTimeMillis() - start)/1000;
            System.out.println("Took " + latency + " ms to add 100 pts w/ a throughput of " + 100/latency + " pts/s");
            DistributionUtil.INSTANCE.summary("TSDB Latency Stats", latencyStats);
            DataPoint evaluationPoint = points.get(50);
            handler.persist(metric
                           ,evaluationPoint
                           , TimeseriesDatabaseHandlers.getTags(evaluationPoint, TimeseriesDatabaseHandlers.OUTLIER_TYPE, null)
                           );
            List<DataPoint> context = handler.retrieve(metric, evaluationPoint, new SimpleTimeRange(offset, offset + 50), new HashMap<String, String>(), -1);
            Assert.assertEquals(50, context.size());
            for (i = 0; i < context.size(); ++i) {
                Assert.assertEquals(context.get(i).getTimestamp(), points.get(i).getTimestamp());
                Assert.assertEquals(context.get(i).getValue(), points.get(i).getValue(), 1e-7);
            }
        }
        finally {
            runner.stop();
        }
    }
    static class TimingCallback implements Function<Object, Void> {
        long s;
        DescriptiveStatistics stats;
        public TimingCallback(long s, DescriptiveStatistics stats) {
            this.s = s;
            this.stats = stats;
        }
        @Nullable
        @Override
        public Void apply(@Nullable Object o) {
            synchronized (stats) {
                stats.addValue(1.0 * (System.currentTimeMillis() - s) / 1000);
            }
            return null;
        }
    }
}
