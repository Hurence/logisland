package com.caseystella.analytics.distribution;

import com.google.common.base.Function;

import javax.annotation.Nullable;

/**
 * Created by cstella on 2/28/16.
 */
public enum RangeApproximator implements Function<Range<Double>, Double> {
    MIDPOINT(new Function<Range<Double>, Double>() {
        @Nullable
        @Override
        public Double apply(@Nullable Range<Double> doubleRange) {
            return (doubleRange.getEnd() + doubleRange.getBegin())/2;
        }
    })
    ;
    private Function<Range<Double>, Double> _func;
    RangeApproximator(Function<Range<Double>, Double> func) {
        _func = func;
    }
    @Nullable
    @Override
    public Double apply(@Nullable Range<Double> doubleRange) {
        return _func.apply(doubleRange);
    }
}
