package com.caseystella.analytics.distribution;

import java.io.Serializable;

public class GlobalStatistics implements Serializable {
    Double mean;
    Double min;
    Double max;
    Double stddev;

    public Double getMean() {
        return mean;
    }

    public void setMean(double mean) {
        this.mean = mean;
    }

    public Double getMin() {
        return min;
    }

    public void setMin(double min) {
        this.min = min;
    }

    public Double getMax() {
        return max;
    }

    public void setMax(double max) {
        this.max = max;
    }

    public Double getStddev() {
        return stddev;
    }

    public void setStddev(double stddev) {
        this.stddev = stddev;
    }
}
