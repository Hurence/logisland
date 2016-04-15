package com.caseystella.analytics.outlier.streaming.mad;

import com.caseystella.analytics.outlier.Outlier;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.hadoop.io.LongWritable;
import org.junit.Assert;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class ConfusionMatrix {
    public static enum ResultType {
        OUTLIER, NON_OUTLIER;
    }
    public static class ConfusionEntry {
        ResultType observed;
        ResultType actual;

        public ConfusionEntry(ResultType observed, ResultType actual) {
            this.observed = observed;
            this.actual = actual;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ConfusionEntry that = (ConfusionEntry) o;

            if (observed != that.observed) return false;
            return actual == that.actual;

        }

        @Override
        public int hashCode() {
            int result = observed != null ? observed.ordinal() : 0;
            result = 31 * result + (actual != null ? actual.ordinal() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "(" +
                    "observed=" + observed +
                    ", actual=" + actual +
                    ')';
        }
        public static void increment(ConfusionEntry entry, Map<ConfusionEntry, Long> map) {
            map.put(entry, map.get(entry) + 1);
        }
    }

    private static boolean setContains(Set<Long> outliers, long prospectiveOutlier, long meanDiffBetweenTs, int timeBounds) {
        for(Long outlier : outliers) {
            if(Math.abs(outlier - prospectiveOutlier) <= timeBounds*meanDiffBetweenTs) {
                return true;
            }
        }
        return false;
    }

    public static long closest(long ts, Set<Long> set) {
        long dist = Long.MAX_VALUE;
        long ret = Long.MAX_VALUE;
        for(Long s : set) {
            long d = Math.abs(ts - s);
            if(d < dist) {
                dist = d;
                ret = s;
            }
        }
        return ret;
    }

    public static String timeConversion(long ms) {

        final int MINUTES_IN_AN_HOUR = 60;
        final int SECONDS_IN_A_MINUTE = 60;
        int totalSeconds = (int) (ms/1000);
        int seconds = totalSeconds % SECONDS_IN_A_MINUTE;
        int totalMinutes = totalSeconds / SECONDS_IN_A_MINUTE;
        int minutes = totalMinutes % MINUTES_IN_AN_HOUR;
        int hours = totalMinutes / MINUTES_IN_AN_HOUR;

        return String.format("%02d", hours) + ":" + String.format("%02d", minutes) + ":" + String.format("%02d", seconds);
    }
    public static void printStats(String title, DescriptiveStatistics scoreStats) {
        System.out.println(title + ": "
                + "\n\tMin: " + scoreStats.getMin()
                + "\n\t1th: " + scoreStats.getPercentile(1)
                + "\n\t5th: " + scoreStats.getPercentile(5)
                + "\n\t10th: " + scoreStats.getPercentile(10)
                + "\n\t25th: " + scoreStats.getPercentile(25)
                + "\n\t50th: " + scoreStats.getPercentile(50)
                + "\n\t90th: " + scoreStats.getPercentile(90)
                + "\n\t95th: " + scoreStats.getPercentile(95)
                + "\n\t99th: " + scoreStats.getPercentile(99)
                + "\n\tMax: " + scoreStats.getMax()
        );
    }
    public static Map<ConfusionEntry, Long> getConfusionMatrix( Set<Long> expectedOutliers
                                                              , Set<Long> computedOutliers
                                                              , LongWritable numObservations
                                                              , long meanDiffBetweenTs
                                                              , int timeBounds
                                                              , Map<Long, Outlier> outlierMap
                                                              , DescriptiveStatistics globalExpectedOutlierScoreStats
                                                              )
    {
        Map<ConfusionEntry, Long> ret = new HashMap<>();
        for(ResultType r : ResultType.values()) {
            for(ResultType s : ResultType.values()) {
                ret.put(new ConfusionEntry(r, s), 0L);
            }
        }
        int unionSize = 0;
        DescriptiveStatistics expectedOutlierScoreStats= new DescriptiveStatistics();
        for(Long expectedOutlier : expectedOutliers) {
            Outlier o = outlierMap.get(expectedOutlier);
            if (o.getScore() != null) {
                expectedOutlierScoreStats.addValue(o.getScore());
                globalExpectedOutlierScoreStats.addValue(o.getScore());
            }
            if(setContains(computedOutliers, expectedOutlier, meanDiffBetweenTs, timeBounds)) {
                ConfusionEntry entry = new ConfusionEntry(ResultType.OUTLIER, ResultType.OUTLIER);
                ConfusionEntry.increment(entry, ret);
                unionSize++;
            }
            else {
                ConfusionEntry entry = new ConfusionEntry(ResultType.NON_OUTLIER, ResultType.OUTLIER);
                long closest = closest(expectedOutlier, computedOutliers);
                long delta = Math.abs(expectedOutlier - closest);
                if(closest != Long.MAX_VALUE) {
                    System.out.println("Missed an outlier (" + expectedOutlier + ") wasn't in computed outliers (" + o + "), closest point is " + closest + " which is " + timeConversion(delta) + "away. - E[delta t] " + timeConversion(meanDiffBetweenTs) + "");
                }
                else {
                    System.out.println("Missed an outlier (" + expectedOutlier + ") wasn't in computed outliers (" + o + "), which is empty. - E[delta t] " + timeConversion(meanDiffBetweenTs) + "");
                }
                ConfusionEntry.increment(entry, ret);
                unionSize++;
            }
        }
        printStats("Expected Outlier Score Stats", expectedOutlierScoreStats );
        DescriptiveStatistics computedOutlierScoreStats= new DescriptiveStatistics();
        for(Long computedOutlier : computedOutliers) {
            if(!setContains(expectedOutliers, computedOutlier, meanDiffBetweenTs, timeBounds)) {
                Outlier o = outlierMap.get(computedOutlier);
                if(o.getScore() != null) {
                    computedOutlierScoreStats.addValue(o.getScore());
                }
                ConfusionEntry entry = new ConfusionEntry(ResultType.OUTLIER , ResultType.NON_OUTLIER);
                ConfusionEntry.increment(entry, ret);
                unionSize++;
            }
        }
        printStats("Computed Outlier Scores", computedOutlierScoreStats);
        ret.put(new ConfusionEntry(ResultType.NON_OUTLIER, ResultType.NON_OUTLIER), numObservations.get() - unionSize);
        Assert.assertEquals(numObservations.get(), getTotalNum(ret));
        return ret;
    }

    public static Map<ConfusionEntry, Long> merge(Map<ConfusionEntry, Long> m1, Map<ConfusionEntry, Long> m2) {
        Map<ConfusionEntry, Long> ret = new HashMap<>();
        for(Map.Entry<ConfusionEntry, Long> kv : m1.entrySet()) {
            ret.put(kv.getKey(), kv.getValue());
        }
        for(Map.Entry<ConfusionEntry, Long> kv : m2.entrySet()) {
            Long existingVal = ret.get(kv.getKey());
            ret.put(kv.getKey(), (existingVal == null?0:existingVal) + kv.getValue());
        }
        return ret;
    }

    public static long getTotalNum(Map<ConfusionEntry, Long> confusionMatrix) {
        long totalNum = 0;

        for(ResultType r : ResultType.values()) {
            for (ResultType s : ResultType.values()) {
                ConfusionEntry entry = new ConfusionEntry(r, s);
                totalNum += confusionMatrix.get(entry);
            }
        }
        return totalNum;
    }

    public static void printConfusionMatrix(Map<ConfusionEntry, Long> confusionMatrix) {
        long totalNum = 0;

        for(ResultType r : ResultType.values()) {
            for (ResultType s : ResultType.values()) {
                ConfusionEntry entry = new ConfusionEntry(r, s);
                totalNum += confusionMatrix.get(entry);
            }
        }
        System.out.println("Total number of points: " + totalNum);
        int numTrueOutlierHits = 0;
        int numOutliers = 0;
        for(ResultType r : ResultType.values()) {
            for (ResultType s : ResultType.values()) {
                ConfusionEntry entry = new ConfusionEntry(r, s);
                long cnt = confusionMatrix.get(entry);
                if(s == ResultType.OUTLIER) {
                    numOutliers += cnt;
                    if(r == ResultType.OUTLIER) {
                       numTrueOutlierHits += cnt;
                    }
                }
                double pct = (100.0*cnt)/totalNum;
                System.out.println( entry + " => " + cnt + " (" + pct + "%)");
            }
        }
        double pctOutliersFound = (100.0*numTrueOutlierHits)/numOutliers;
        System.out.println("Percent of true outliers found: " + + pctOutliersFound + "%");
    }
}
