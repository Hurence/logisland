/**
 * Copyright (C) 2016 Hurence (bailet.thomas@gmail.com)
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
package com.hurence.logisland.sampling;

import com.hurence.logisland.record.Record;

import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class SamplingUtils {

    public static int fitBucketSize(List<Record> rawData, int bucketSize) {
        if (bucketSize <= 0 || bucketSize >= rawData.size()) return 1;
        else return bucketSize;
    }

    /**
     * group a list of records into fixed size sublist
     *
     * @param source
     * @param length
     * @return
     */
    public static  Stream<List<Record>> grouped(List<Record> source, int length) {
        if (length <= 0)
            throw new IllegalArgumentException("length = " + length);
        int size = source.size();
        if (size <= 0)
            return Stream.empty();
        int fullChunks = (size - 1) / length;
        return IntStream.range(0, fullChunks + 1).mapToObj(
                n -> source.subList(n * length, n == fullChunks ? size : (n + 1) * length));
    }


}
