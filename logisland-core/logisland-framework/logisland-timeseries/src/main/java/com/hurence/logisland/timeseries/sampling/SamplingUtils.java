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
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class SamplingUtils {

    /**
     *
     * @param rawData elements to bucket
     * @param bucketSize wanted size of buckets
     * @return 1 if bucketSize is lesser than 0 or lesser than the size of rawData
     *         else bucketSize
     */
    public static int fitBucketSize(List rawData, int bucketSize) {
        return fitBucketSize(rawData.size(), bucketSize);
    }

    private static int fitBucketSize(int numberOfPoint, int bucketSize) {
        if (bucketSize <= 0 || bucketSize > numberOfPoint) return 1;
        else return bucketSize;
    }

    /**
     * group a list of records into fixed size sublist
     *
     * @param source
     * @param bucketSize
     * @return
     */
    public static <E> Stream<List<E>> grouped(List<E> source, int bucketSize) throws IllegalArgumentException {
        if (bucketSize <= 0)
            throw new IllegalArgumentException("length = " + bucketSize);
        int size = source.size();
        if (size <= 0)
            return Stream.empty();
        int numberOfFullBucket = (size - 1) / bucketSize;
        return IntStream.range(0, numberOfFullBucket + 1).mapToObj(
                n -> source.subList(n * bucketSize, n == numberOfFullBucket ? size : (n + 1) * bucketSize)
        );
    }

    /**
     *
     * @param source
     * @param bucketingStrategy
     * @param <E>
     * @return a stream of bucket corresponding to bucketingStrategy
     * @throws IllegalArgumentException if size of input source does not fit in bucketingStrategy
     */
    public static <E> Stream<List<E>> groupedWithStrictBucketStrategy(List<E> source, BucketingStrategy bucketingStrategy) throws IllegalArgumentException {
        int size = source.size();
        if (bucketingStrategy.getTotalNumberOfPointSampled() != size)
            throw new IllegalArgumentException(
                    String.format("toBeSampled size expected to be %s but was %s",
                            bucketingStrategy.getTotalNumberOfPointSampled(),
                            size));
        return LongStream.range(1, bucketingStrategy.getTotalNumberOfBucket()).mapToObj(
                bucketNumber -> {
                    int from = bucketingStrategy.getStartPointOfBucket(bucketNumber);
                    int to = bucketingStrategy.getEndPointOfBucket(bucketNumber);
                    return source.subList(from, to);
                }
        );
    }


}
