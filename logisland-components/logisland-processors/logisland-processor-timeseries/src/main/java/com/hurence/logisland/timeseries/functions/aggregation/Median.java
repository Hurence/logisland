/*
 * Copyright (C) 2016 QAware GmbH
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package com.hurence.logisland.timeseries.functions.aggregation;

import com.hurence.logisland.timeseries.MetricTimeSeries;
import com.hurence.logisland.timeseries.functions.ChronixAggregation;
import com.hurence.logisland.timeseries.functions.FunctionValueMap;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * The median aggregation
 *
 * @author bailett
 */
public final class Median implements ChronixAggregation<MetricTimeSeries> {

    /**
     * Computes the median value of timeseries.
     *
     * @param timeSeries the time series
     * @return The median value.
     */
    @Override
    public void execute(MetricTimeSeries timeSeries, FunctionValueMap functionValueMap) {

        // If it is empty, we return NaN
        if (timeSeries.size() <= 0) {
            functionValueMap.add(this, Double.NaN);
            return;
        }

        // Else calculate the analysis value
        double[] clonedSeries = timeSeries.getValuesAsArray();
        double median;

        if (clonedSeries.length % 2 == 0) {
            median = (clonedSeries[clonedSeries.length / 2]
                    + (double) clonedSeries[clonedSeries.length / 2 - 1]) / 2;
        }
        else {
            median = clonedSeries[clonedSeries.length / 2];
        }
        functionValueMap.add(this, median);



    }

    @Override
    public String getQueryName() {
        return "median";
    }

    @Override
    public String getTimeSeriesType() {
        return "metric";
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        return new EqualsBuilder()
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder()
                .toHashCode();
    }
}
