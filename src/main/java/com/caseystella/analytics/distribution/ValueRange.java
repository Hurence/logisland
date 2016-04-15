package com.caseystella.analytics.distribution;

public class ValueRange implements Range<Double> {
    double begin;
    double end;
    public ValueRange(double begin, double end) {
        this.begin = begin;
        this.end = end;
    }
    @Override
    public Double getBegin() {
        return begin;
    }

    @Override
    public Double getEnd() {
        return end;
    }
}
