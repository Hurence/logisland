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
import java.util.stream.Stream;

public class FirstItemSamplerWithSpecificBucketing<SAMPLED>  extends AbstractFirstItemSampler<SAMPLED> implements Sampler<SAMPLED> {

    private BucketingStrategy bucketingStrategy;

    public FirstItemSamplerWithSpecificBucketing(BucketingStrategy bucketingStrategy) {
        this.bucketingStrategy = bucketingStrategy;
    }

    @Override
    protected Stream<List<SAMPLED>> group(List<SAMPLED> toBeSampled) {
        return SamplingUtils.groupedWithStrictBucketStrategy(toBeSampled, bucketingStrategy);
    }
}
