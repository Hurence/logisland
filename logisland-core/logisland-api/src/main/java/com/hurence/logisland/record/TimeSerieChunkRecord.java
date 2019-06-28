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
package com.hurence.logisland.record;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.*;
import java.util.*;
import java.util.List;
import java.util.stream.Stream;

/**
 *
 */
public class TimeSerieChunkRecord extends TimeSerieRecord {

    private static Logger logger = LoggerFactory.getLogger(TimeSerieChunkRecord.class);
    public static String DEFAULT_CHUNK_RECORD_TYPE = RecordDictionary.METRIC;

    public TimeSerieChunkRecord() {
        super(DEFAULT_CHUNK_RECORD_TYPE);
    }

    public TimeSerieChunkRecord(String metricName) {
        this(DEFAULT_CHUNK_RECORD_TYPE, metricName);
    }

    public TimeSerieChunkRecord(String metricType, String metricName) {
        super(metricType);
        setMetricName(metricName);
    }

    @Override
    public String toString() {
        return super.toString();
    }

    @Override
    public boolean isInternField(String fieldName) {
        return super.isInternField(fieldName) ||
                FieldDictionary.CHUNK_FIELDS.contains(fieldName);
    }

    public Map<String, Object> getAttributes() {
        if (hasField(FieldDictionary.RECORD_CHUNK_META))
            return (Map<String, Object>) getField(FieldDictionary.RECORD_CHUNK_META).getRawValue();
        return null;
    }

    public void setAttributes(Map<String, Object> attributes) {
        setField(FieldDictionary.RECORD_CHUNK_META, FieldType.MAP, attributes);
    }

    public void addAttributes(String key, Object attribute) {
        if (!hasField(FieldDictionary.RECORD_CHUNK_META)) {
            setAttributes(new HashMap<String, Object>());
        }
        getAttributes().put(key,attribute);
    }

    //TODO needed ?
//    public Stream<Long> getTimestamps() {
//        return timestamps;
//    }
//
//    public void setTimestamps(List<Long> timestamps) {
//        this.timestamps = timestamps;
//    }
//
//    public Stream<Double> getValues() {
//        return values;
//    }
//
//    public void setValues(List<Double> values) {
//        this.values = values;
//    }

    public Stream<Point> getUnCompressedPoints() {
        Object rawValue = getField(FieldDictionary.RECORD_CHUNK_UNCOMPRESSED_POINTS).getRawValue();
        return ((List<Point>) rawValue).stream();
    }

    public void setUnCompressedPoints(List<Point> points) {
        setField(FieldDictionary.RECORD_CHUNK_UNCOMPRESSED_POINTS, FieldType.ARRAY, points);
    }

    public byte[] getCompressedPoints() {
        return getField(FieldDictionary.RECORD_CHUNK_COMPRESSED_POINTS).asBytes();
    }

    public void setCompressedPoints(byte[] points) {
        setField(FieldDictionary.RECORD_CHUNK_COMPRESSED_POINTS, FieldType.BYTES, points);
    }

    public long getEnd() {
        return getField(FieldDictionary.RECORD_CHUNK_END).asLong();
    }

    public void setEnd(long end) {
        setField(FieldDictionary.RECORD_CHUNK_END, FieldType.LONG, end);
    }

    public long getStart() {
        return getField(FieldDictionary.RECORD_CHUNK_START).asLong();
    }

    public void setStart(long start) {
        setField(FieldDictionary.RECORD_CHUNK_START, FieldType.LONG, start);
    }

    public long getMax() {
        return getField(FieldDictionary.RECORD_CHUNK_MAX).asLong();
    }

    public void setMax(long max) {
        setField(FieldDictionary.RECORD_CHUNK_MAX, FieldType.LONG, max);
    }

    public long getMin() {
        return getField(FieldDictionary.RECORD_CHUNK_MIN).asLong();
    }

    public void setMin(long min) {
        setField(FieldDictionary.RECORD_CHUNK_MIN, FieldType.LONG, min);
    }

    public long getAvg() {
        return getField(FieldDictionary.RECORD_CHUNK_AVG).asLong();
    }

    public void setAvg(long avg) {
        setField(FieldDictionary.RECORD_CHUNK_AVG, FieldType.LONG, avg);
    }

}
