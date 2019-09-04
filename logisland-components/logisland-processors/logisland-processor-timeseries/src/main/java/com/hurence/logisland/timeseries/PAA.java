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
