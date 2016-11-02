/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.sampling;

import com.hurence.logisland.record.Record;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class LTTBSampler extends AbstractSampler {

    private int threshold;

    public LTTBSampler(String valueFieldName, String timeFieldName, int threshold) {
        super(valueFieldName,timeFieldName);

        if (threshold <= 2) {
            throw new IllegalArgumentException("What am I supposed to do with that?");
        }

        this.threshold = threshold;
    }

    public Number[][] largestTriangleThreeBuckets(Number[][] data, Integer threshold) {
        Number[][] sampled = new Number[threshold][];
        if (data == null) {
            throw new NullPointerException("Cannot cope with a null data input array.");
        }
        if (threshold <= 2) {
            throw new RuntimeException("What am I supposed to do with that?");
        }
        if (data.length <= 2 || data.length <= threshold) {
            return data;
        }
        int sampled_index = 0;
        double every = (double) (data.length - 2) / (double) (threshold - 2);
        System.out.println(": " + every);
        int a = 0, next_a = 0;
        Number[] max_area_point = null;
        double max_area, area;

        sampled[sampled_index++] = data[a];

        for (int i = 0; i < threshold - 2; i++) {
            double avg_x = 0.0D, avg_y = 0.0D;
            int avg_range_start = (int) Math.floor((i + 1) * every) + 1;
            int avg_range_end = (int) Math.floor((i + 2) * every) + 1;
            avg_range_end = avg_range_end < data.length ? avg_range_end : data.length;
            int avg_range_length = (int) (avg_range_end - avg_range_start);
            while (avg_range_start < avg_range_end) {
                avg_x = avg_x + data[avg_range_start][0].doubleValue();
                avg_y += data[avg_range_start][1].doubleValue();
                avg_range_start++;
            }
            avg_x /= avg_range_length;
            avg_y /= avg_range_length;

            int range_offs = (int) Math.floor((i + 0) * every) + 1;
            int range_to = (int) Math.floor((i + 1) * every) + 1;

            double point_a_x = data[a][0].doubleValue();
            double point_a_y = data[a][1].doubleValue();

            max_area = area = -1;

            while (range_offs < range_to) {
                area = Math.abs(
                        (point_a_x - avg_x) * (data[range_offs][1].doubleValue() - point_a_y) -
                                (point_a_x - data[range_offs][0].doubleValue()) * (avg_y - point_a_y)
                ) * 0.5D;
                if (area > max_area) {
                    max_area = area;
                    max_area_point = data[range_offs];
                    next_a = range_offs;
                }
                range_offs++;
            }
            sampled[sampled_index++] = max_area_point;
            a = next_a;
        }

        sampled[sampled_index++] = data[data.length - 1];
        return sampled;
    }


    /**
     * Return a downsampled version of data.
     Parameters
     ----------
     data: list of lists/tuples
     data must be formated this way: [[x,y], [x,y], [x,y], ...]
     or: [(x,y), (x,y), (x,y), ...]
     threshold: int
     threshold must be >= 2 and <= to the len of data



     * @param inputRecords
     * @return data, but downsampled using threshold
     */
    @Override
    public List<Record> sample(List<Record> inputRecords) {
        if (inputRecords == null) {
            throw new NullPointerException("Cannot cope with a null data input array.");
        }

        ArrayList<Record> sampled = new ArrayList<>(threshold);
        for(int i=0; i<threshold; i++){
            sampled.add(null);
        }
        ArrayList<Record> data = new ArrayList<>(inputRecords);


        if (inputRecords.size() <= 2 || inputRecords.size() <= threshold) {
            return inputRecords;
        }
        int sampledIndex = 0;
        double every = (double) (inputRecords.size() - 2) / (double) (threshold - 2);
        System.out.println(": " + every);

        // Initially a is the first point in the triangle
        int a = 0, nextA = 0;
        Record maxAreaPoint = null;
        double maxArea, area;

        // Always add the first point
        sampled.set(0, inputRecords.get(0));

        for (int i = 0; i < threshold - 2; i++) {

            // Calculate point average for next bucket (containing c)
            double avgX = 0.0D, avgY = 0.0D;
            int avgRangeStart = (int) Math.floor((i + 1) * every) + 1;
            int avgRangeEnd = (int) Math.floor((i + 2) * every) + 1;
            avgRangeEnd = avgRangeEnd < inputRecords.size() ? avgRangeEnd : inputRecords.size();
            int avgRangeLength = (int) (avgRangeEnd - avgRangeStart);
            while (avgRangeStart < avgRangeEnd) {
                avgX = avgX + getRecordTime(inputRecords.get(avgRangeStart));
                avgY += getRecordValue(inputRecords.get(avgRangeStart));
                avgRangeStart++;
            }
            avgX /= avgRangeLength;
            avgY /= avgRangeLength;

            // Get the range for this bucket
            int rangeOffs = (int) Math.floor((i + 0) * every) + 1;
            int rangeTo = (int) Math.floor((i + 1) * every) + 1;

            double pointAX = getRecordTime(inputRecords.get(a));
            double pointAY = getRecordValue(inputRecords.get(a));

            maxArea = area = -1;

            while (rangeOffs < rangeTo) {
                area = Math.abs(
                        (pointAX - avgX) * (getRecordValue(inputRecords.get(rangeOffs)) - pointAY) -
                                (pointAX - getRecordTime(inputRecords.get(rangeOffs))) * (avgY - pointAY)
                ) * 0.5D;
                if (area > maxArea) {
                    maxArea = area;
                    maxAreaPoint = getTimeValueRecord(inputRecords.get(rangeOffs));
                    nextA = rangeOffs;
                }
                rangeOffs++;
            }
            sampled.set(sampledIndex++, maxAreaPoint);
            a = nextA;
        }

        sampled.set(sampledIndex++, data.get(inputRecords.size() - 1));
        return sampled
                .stream()
                .filter(record -> record != null)
                .collect(Collectors.toList());
    }
}
