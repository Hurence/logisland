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
package com.hurence.logisland.timeseries;



import com.hurence.logisland.timeseries.converter.common.IntList;
import com.hurence.logisland.timeseries.converter.common.LongList;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Stan Salvador (stansalvador@hotmail.com)
 * @author f.lautenschlager
 */
public class MultivariateTimeSeries {

    private final IntList labels;   // labels for each column
    private final LongList times;
    private final List<double[]> values;    // ArrayList of TSValues.. no time

    public MultivariateTimeSeries() {
        labels = new IntList();
        times = new LongList();
        values = new ArrayList<>();
    }

    public MultivariateTimeSeries(int numOfDimensions) {
        this();
        for (int x = 0; x <= numOfDimensions; x++)
            labels.add(x);
    }

    public int size() {
        return times.size();
    }

    public int numOfDimensions() {
        return labels.size() - 1;
    }

    public double getTimeAtNthPoint(int n) {
        return times.get(n);
    }

    public IntList getDimensions() {
        return labels;
    }

    public void setDimensions(IntList newLabels) {
        labels.clear();
        labels.addAll(newLabels);
    }

    public double[] getMeasurementVector(int pointIndex) {
        return values.get(pointIndex);
    }

    public void add(long time, double[] values) {
        if (labels.size() != values.length + 1)  // labels include a label for time
            throw new InternalError("ERROR:  The TSValues: " + values + " contains the wrong number of values. " + "expected:  " + labels.size() + ", " + "found: " + values.length);

        if ((this.size() > 0) && (time <= times.get(times.size() - 1)))
            throw new InternalError("ERROR:  The point being inserted at the " + "end of the time series does not have " + "the correct time sequence. ");

        times.add(time);
        this.values.add(values);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("labels", labels)
                .append("times", times)
                .append("values", values)
                .toString();
    }
}
