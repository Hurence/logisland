package com.caseystella.analytics.distribution;

import com.caseystella.analytics.DataPoint;
import com.caseystella.analytics.distribution.config.RotationConfig;
import com.caseystella.analytics.distribution.config.Type;
import com.caseystella.analytics.distribution.sampling.ExponentiallyBiasedAChao;
import com.caseystella.analytics.distribution.scaling.ScalingFunction;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.twitter.algebird.QTree;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import scala.Tuple2;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;

public class Distribution implements Measurable {


    public static class Context {
        private Distribution currentDistribution;
        private Distribution previousDistribution;
        private LinkedList<Distribution> chunks = new LinkedList<>();
        private ExponentiallyBiasedAChao<Double> reservoir;

        public Context(int reservoirSize, double decayRate) {
            if(reservoirSize > 0) {
                reservoir = new ExponentiallyBiasedAChao<>(reservoirSize, decayRate, new Random(0));
            }
            else {
                reservoir = null;
            }
        }
        public Distribution getPreviousDistribution() {
            return previousDistribution;
        }
        public Distribution getCurrentDistribution() {
            return currentDistribution;
        }
        public LinkedList<Distribution> getChunks() {
            return chunks;
        }
        public ExponentiallyBiasedAChao<Double> getSample() {
            return reservoir;
        }
        public long getAmount() {
            return currentDistribution == null?0L:currentDistribution.getAmount();
        }

        public void addDataPoint( DataPoint dp
                                , RotationConfig rotationPolicy
                                , RotationConfig chunkingPolicy
                                , ScalingFunction scalingFunction
                                , GlobalStatistics stats
                                )
        {
            if(currentDistribution == null) {
                currentDistribution = new Distribution(dp, scalingFunction, stats);
            }
            else {
                currentDistribution.addDataPoint(dp, scalingFunction);
            }
            //do I need to create a new chunk?
            boolean needNewChunk = chunks.size() == 0 || outOfPolicy(getCurrentChunk(), chunkingPolicy);
            if(needNewChunk) {
                addChunk(new Distribution(dp, scalingFunction, stats));
            }
            else {
                getCurrentChunk().addDataPoint(dp, scalingFunction);
            }
            if(needNewChunk) {
                //do I need to rotate now?
                boolean needsRotation = outOfPolicy(currentDistribution, rotationPolicy)
                                    && outOfPolicy(sketch(Iterables.limit(chunks, chunks.size() - 1)), rotationPolicy);
                if(needsRotation) {
                    rotate();
                }
            }
        }

        protected void addChunk(Distribution d) {
            chunks.addFirst(d);
        }

        protected void rotate() {
            chunks.removeLast();
            previousDistribution = currentDistribution;
            currentDistribution = Distribution.merge(chunks);
            if(reservoir != null) {
                reservoir.advancePeriod();
            }
        }

        private Distribution getCurrentChunk() {
            return chunks.getFirst();
        }

        private Measurable sketch(Iterable<Distribution> chunks) {
            long begin = Long.MAX_VALUE;
            long end = -1;
            long amount = 0;
            for(Distribution d : chunks) {
                begin = Math.min(begin, d.getBegin());
                end = Math.max(end, d.getEnd());
                amount += d.getAmount();
            }
            final long measurableBegin = begin;
            final long measurableEnd= end;
            final long measurableAmount = amount;
            return new Measurable() {
                @Override
                public long getAmount() {
                    return measurableAmount;
                }

                @Override
                public Long getBegin() {
                    return measurableBegin;
                }

                @Override
                public Long getEnd() {
                    return measurableEnd;
                }
            };
        }

        private boolean outOfPolicy(Measurable dist, RotationConfig policy) {
            if(policy.getType() == Type.BY_AMOUNT) {
                return dist.getAmount() >= policy.getAmount();
            }
            else if(policy.getType() == Type.BY_TIME) {
                return dist.getAmount() >= policy.getUnit().apply(dist);
            }
            else if(policy.getType() == Type.NEVER) {
                return false;
            }
            else {
                throw new IllegalStateException("Unsupported type: " + policy.getType());
            }
        }



    }
    QTree<Object> distribution;
    long begin = 0L;
    long end = 0L;
    long amount = 0L;
    double sum;
    GlobalStatistics globalStatistics;

    public Distribution(Distribution dist) {
        this(dist.distribution, dist.getBegin(), dist.getEnd(), dist.getAmount(), dist.getSum(), dist.getGlobalStatistics());
    }
    public Distribution(QTree<Object> distribution, long begin, long end, long amount, double sum, GlobalStatistics stats) {
        this.distribution = distribution;
        this.begin = begin;
        this.end = end;
        this.amount = amount;
        this.sum = sum;
        this.globalStatistics = stats;

    }
    public Distribution(DataPoint dp, ScalingFunction scalingFunction, GlobalStatistics stats) {
        this.begin = dp.getTimestamp();
        this.end = dp.getTimestamp();
        this.globalStatistics = stats;
        this.amount = 1L;
        this.sum = dp.getValue();
        this.distribution = DistributionUtils.createTree(ImmutableList.of(scalingFunction.scale(dp.getValue(), globalStatistics)));
    }

    public double getSum() {
        return sum;
    }

    public GlobalStatistics getGlobalStatistics() {
        return globalStatistics;
    }
    public void addDataPoint(DataPoint dp, ScalingFunction scalingFunction ) {
        this.end = Math.max(end, dp.getTimestamp());
        this.begin = Math.min(begin, dp.getTimestamp());
        this.sum += dp.getValue();
        this.amount++;

        this.distribution = DistributionUtils.merge(this.distribution, DistributionUtils.createTree(ImmutableList.of(scalingFunction.scale(dp.getValue(), globalStatistics))));
    }

    @Override
    public Long getBegin() {
        return begin;
    }

    @Override
    public Long getEnd() {
        return end;
    }

    @Override
    public long getAmount() {
        return amount;
    }

    public static double getMadScore(Iterable<Double> vals, Double val) {
        DescriptiveStatistics stats = new DescriptiveStatistics();
        DescriptiveStatistics medianStats = new DescriptiveStatistics();
        for(Double v : vals) {
            stats.addValue(v);
        }
        double median = stats.getPercentile(50);
        for(Double v : vals) {
            medianStats.addValue(Math.abs(v - median));
        }
        double mad = medianStats.getPercentile(50);
        return Math.abs(0.6745*(val - median)/mad);
    }

    public static double kullbackLeibler(Distribution p0, Distribution p1) {
        if(p0 == null || p1 == null ) {
            return 0;
        }
        double kl = 0;
        for(double i = 0.01;i < 1.0;i += 0.01) {
            double p_i = p0.getPercentile(i);
            kl += p_i*Math.log(p_i/p1.getPercentile(i));
        }
        kl /= Math.log(2);
        return kl;
    }

    public static Distribution merge(Iterable<Distribution> distributions) {
        QTree<Object> distribution = null;
        long begin = Long.MAX_VALUE;
        long end = -1l;
        long amount = 0l;
        double sum = 0;
        GlobalStatistics globalStats = null;
        for(Distribution d : distributions) {
            if(distribution == null) {
                distribution = d.distribution;
                globalStats = d.getGlobalStatistics();
            }
            else {
                distribution = DistributionUtils.merge(distribution, d.distribution);
            }
            begin = Math.min(begin, d.begin);
            end = Math.max(end, d.end);
            sum += d.sum;
            amount += d.amount;
        }
        return new Distribution(distribution, begin, end, amount, sum, globalStats);
    }

    public double getMean() {
        return sum / amount;
    }

    public ValueRange getPercentileRange(double percentile) {
        Tuple2<Object, Object> bounds = distribution.quantileBounds(percentile);
        if(Math.abs(percentile - 0.0) < 1e-6) {
            double min = ((Number)bounds._1()).doubleValue();
            return new ValueRange(min, min);
        }
        if(Math.abs(percentile - 1.0) < 1e-6) {
            double max = ((Number)bounds._2()).doubleValue();
            return new ValueRange(max, max);
        }
        double l = ((Number)bounds._1()).doubleValue();
        double r = ((Number)bounds._2()).doubleValue();
        return new ValueRange(l, r);
    }

    public double getPercentileRange(double percentile, Function<Range<Double>, Double> approximator) {
        return approximator.apply(getPercentileRange(percentile));
    }

    public double getPercentile(double percentile) {
       return getPercentileRange(percentile, RangeApproximator.MIDPOINT);
    }
    public double getMedian(Function<Range<Double>, Double> approximator) {
        return getPercentileRange(0.5, approximator);
    }

    public double getMedian() {
        return getMedian(RangeApproximator.MIDPOINT);
    }

}
