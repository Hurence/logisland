package com.caseystella.analytics.distribution.scaling;

import com.caseystella.analytics.distribution.GlobalStatistics;

public interface ScalingFunction {
    double scale(double val, GlobalStatistics stats);
}
