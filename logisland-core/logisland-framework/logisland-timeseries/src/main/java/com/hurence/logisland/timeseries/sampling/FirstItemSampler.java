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

import java.util.List;
import java.util.stream.Collectors;

public class FirstItemSampler<SAMPLED> implements Sampler<SAMPLED> {


    private int numBuckets;

    public FirstItemSampler(int numBuckets) {
        this.numBuckets = numBuckets;
    }

    /**
     * divide the points sequence into equally sized buckets
     * and select the first point of each bucket
     *
     * @param toBeSampled the input list
     * @return
     */
    @Override
    public List<SAMPLED> sample(List<SAMPLED> toBeSampled) {
        // simple downsample to numBucket data points
        final int bucketSize = SamplingUtils.fitBucketSize(toBeSampled, numBuckets);

        return SamplingUtils.grouped(toBeSampled, bucketSize)
                .map(bucket -> bucket.get(0))
                .collect(Collectors.toList());
    }
}
