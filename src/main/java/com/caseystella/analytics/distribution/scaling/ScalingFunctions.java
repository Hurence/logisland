package com.caseystella.analytics.distribution.scaling;

import com.caseystella.analytics.distribution.GlobalStatistics;

public enum ScalingFunctions implements ScalingFunction {
    NONE(new ScalingFunction() {

        @Override
        public double scale(double val, GlobalStatistics stats) {
            return val;
        }
    })
   ,SHIFT_TO_POSITIVE(new DefaultScalingFunctions.ShiftToPositive())

    ,SQUEEZE_TO_UNIT( new DefaultScalingFunctions.SqueezeToUnit())

    ,FIXED_MEAN_UNIT_VARIANCE(new DefaultScalingFunctions.FixedMeanUnitVariance())
    ,ZERO_MEAN_UNIT_VARIANCE(new DefaultScalingFunctions.ZeroMeanUnitVariance())
    ;
    private ScalingFunction _func;
    ScalingFunctions(ScalingFunction func) {
        _func = func;
    }

    @Override
    public double scale(double val, GlobalStatistics globalStatistics) {
        return _func.scale(val, globalStatistics);
    }
}
