package com.caseystella.analytics.distribution.scaling;

import com.caseystella.analytics.distribution.GlobalStatistics;

public class DefaultScalingFunctions {
    public static class ShiftToPositive implements ScalingFunction {

        @Override
        public double scale(double val, GlobalStatistics globalStatistics) {
            if(globalStatistics.getMin() != null) {
                return val + Math.abs(globalStatistics.getMin());
            }
            else {
                throw new RuntimeException("Unable to shift to positive because min is not set.");
            }
        }
    }
    public static class SqueezeToUnit implements ScalingFunction {
        @Override
        public double scale(double val, GlobalStatistics globalStatistics) {
            if(globalStatistics.getMin() != null && globalStatistics.getMax() != null) {
                return (val - globalStatistics.getMin())/(globalStatistics.getMax() - globalStatistics.getMin());
            }
            throw new RuntimeException("Unable to squeeze to [0,1] because either min or max isn't specified");
        }
    }
    public static class FixedMeanUnitVariance implements ScalingFunction {

        @Override
        public double scale(double val, GlobalStatistics stats) {
            if(stats.getMin() != null && stats.getMax() != null && stats.getMean() != null && stats.getStddev() != null) {
                double projectedValue = (val - stats.getMean()) / stats.getStddev();
                double projectedMin = (stats.getMin() - stats.getMean())/stats.getStddev();
                //I KNOW that this isn't really E[X] = 0, sqrt(V[X]) = 1, but I have to scale to positive.
                return projectedValue + Math.abs(projectedMin);
            }
            throw new RuntimeException("Unable to scale to 0 mean, unit variance and then shift positive because we require min, max, mean and stddev to be globally known");
        }
    }
    public static class ZeroMeanUnitVariance implements ScalingFunction {

        @Override
        public double scale(double val, GlobalStatistics stats) {
            if(stats.getMin() != null && stats.getMax() != null && stats.getMean() != null && stats.getStddev() != null) {
                return (val - stats.getMean()) / stats.getStddev();
            }
            throw new RuntimeException("Unable to scale to 0 mean, unit variance and then shift positive because we require min, max, mean and stddev to be globally known");
        }
    }
}
