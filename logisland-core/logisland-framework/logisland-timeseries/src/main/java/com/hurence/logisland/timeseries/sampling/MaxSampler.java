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
package com.hurence.logisland.timeseries.sampling;

import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

import java.util.List;
import java.util.stream.Collectors;

public class MaxSampler<SAMPLED> extends AbstractSampler<SAMPLED> {

    public MaxSampler(TimeSerieHandler<SAMPLED> timeSerieHandler, int bucketSize) {
        super(timeSerieHandler, bucketSize);
    }

    /**
     * divide the points sequence into equally sized buckets
     * and compute average of each bucket
     *
     * @param inputRecords the input list
     * @return
     */
    @Override
    public List<SAMPLED> sample(List<SAMPLED> inputRecords) {
        final int realBucketSize = SamplingUtils.fitBucketSize(inputRecords, bucketSize);
        return SamplingUtils.grouped(inputRecords, realBucketSize)
                .map(bucket -> {
                    SummaryStatistics stats = new SummaryStatistics();
                    bucket.forEach(element -> {
                        final Double recordValue = timeSerieHandler.getTimeserieValue(element);
                        if (recordValue != null)
                            stats.addValue(recordValue);
                    });
                    final double maxValue = stats.getMax();
                    final long timestamp = timeSerieHandler.getTimeserieTimestamp(bucket.get(0));
                    final SAMPLED sampleElement = timeSerieHandler.createTimeserie(timestamp, maxValue);
                    return sampleElement;
                }).collect(Collectors.toList());
    }
}
