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
package com.hurence.logisland.timeseries.converter;

import com.hurence.logisland.timeseries.MetricTimeSeries;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.function.Function;

/**
 * Holds default group by and reduce functions for the chronix metric time series
 *
 * @author f.lautenschlager
 */
public final class ChronixTimeSeriesDefaults {

    /**
     * Default group by function for the metric time series class.
     * Groups time series on its name.
     */
    public static final Function<MetricTimeSeries, String> GROUP_BY = MetricTimeSeries::getName;
    /**
     * Default reduce function.
     * Attributes in both collected and reduced are merged using set holding both values.
     */
    public static final BinaryOperator<MetricTimeSeries> REDUCE = (collected, reduced) -> {
        collected.addAll(reduced.getTimestampsAsArray(), reduced.getValuesAsArray());

        // The collected attributes
        Map<String, Object> collectedAttributes = collected.getAttributesReference();

        // merge the attributes
        // we iterate over the copy
        for (HashMap.Entry<String, Object> entry : reduced.attributes().entrySet()) {
            Set<Object> set = new HashSet<>();

            // Attribute to add
            String attribute = entry.getKey();
            Object value = entry.getValue();

            if (collectedAttributes.containsKey(attribute)) {
                Object collectedValue = collectedAttributes.get(attribute);

                if (collectedValue instanceof Set<?>) {
                    set.addAll((Set<?>) collectedValue);
                } else {
                    set.add(collectedValue);
                }
            }

            set.add(value);
            collectedAttributes.put(attribute, set);
        }

        return collected;
    };

    private ChronixTimeSeriesDefaults() {
        //avoid instances
    }
}