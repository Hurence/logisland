/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2016 Stan Salvador (stansalvador@hotmail.com), Philip Chan (pkc@cs.fit.edu), QAware GmbH
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
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
