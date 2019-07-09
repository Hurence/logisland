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


import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * @author Stan Salvador (stansalvador@hotmail.com)
 * @author f.lautenschlager
 */
public class PAA extends MultivariateTimeSeries {
    private int[] aggPtSize;
    private final int originalLength;


    public PAA(MultivariateTimeSeries ts, int shrunkSize) {
        // Initialize private data.
        this.originalLength = ts.size();
        this.aggPtSize = new int[shrunkSize];

        // Initialize the new aggregate time series.
        this.setDimensions(ts.getDimensions());

        // Determine the size of each sampled point. (may be a fraction)
        final double reducedPtSize = ts.size() / (double) shrunkSize;

        // Variables that keep track of the range of points being averaged into a single point.
        int ptToReadFrom = 0;
        int ptToReadTo;

        // Keep averaging ranges of points into aggregate points until all of the data is averaged.
        while (ptToReadFrom < ts.size()) {
            ptToReadTo = (int) Math.round(reducedPtSize * (this.size() + 1)) - 1;   // determine end of current range
            final int ptsToRead = ptToReadTo - ptToReadFrom + 1;

            // Keep track of the sum of all the values being averaged to create a single point.
            long timeSum = 0;
            final double[] measurementSums = new double[ts.numOfDimensions()];

            // Sum all of the values over the range ptToReadFrom...ptToReadFrom.
            for (int pt = ptToReadFrom; pt <= ptToReadTo; pt++) {
                final double[] currentPoint = ts.getMeasurementVector(pt);

                timeSum += ts.getTimeAtNthPoint(pt);

                for (int dim = 0; dim < ts.numOfDimensions(); dim++)
                    measurementSums[dim] += currentPoint[dim];
            }

            // Determine the average value over the range ptToReadFrom...ptToReadFrom.
            timeSum = timeSum / ptsToRead;
            for (int dim = 0; dim < ts.numOfDimensions(); dim++)
                measurementSums[dim] = measurementSums[dim] / ptsToRead;   // find the average of each measurement

            // Add the computed average value to the aggregate approximation.
            this.aggPtSize[super.size()] = ptsToRead;
            this.add(timeSum, measurementSums);

            ptToReadFrom = ptToReadTo + 1;    // next window of points to average startw where the last window ended
        }
    }

    public int aggregatePtSize(int ptIndex) {
        return aggPtSize[ptIndex];
    }

    public int originalSize() {
        return originalLength;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .appendSuper(super.toString())
                .append("aggPtSize", aggPtSize)
                .append("originalLength", originalLength)
                .toString();
    }
}
