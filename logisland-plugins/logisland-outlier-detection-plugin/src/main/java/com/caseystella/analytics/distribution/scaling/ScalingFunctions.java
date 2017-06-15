/**
 * Copyright (C) 2016 Hurence (support@hurence.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
