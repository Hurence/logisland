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

/*******************************************************************************
 * Copyright (C) 2015 - Amit Kumar Mondal <admin@amitinside.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * EdcPosition is a data structure to capture a geo location. It can be
 * associated to an EdcPayload to geotag an EdcMessage before sending to the
 * Everyware Cloud. Refer to the description of each of the fields for more
 * information on the model of EdcPosition.
 */
public class ChunkRecord extends StandardRecord {//TODO change name to MetricTimeSeriesRecord ???

    private static Logger logger = LoggerFactory.getLogger(ChunkRecord.class);
    public static String DEFAULT_CHUNK_RECORD_TYPE = RecordDictionary.METRIC;

    private List<Long> timestamps;
    private List<Double> values;

    public ChunkRecord() {
        super(DEFAULT_CHUNK_RECORD_TYPE);
    }

    public ChunkRecord(String metricName) {
        this(DEFAULT_CHUNK_RECORD_TYPE, metricName);
    }

    public ChunkRecord(String metricType, String metricName) {
        super(metricType);
        setMetricName(metricName);
    }

    public Map<String, Object> getAttributes() {
        Object rawValue = getField(FieldDictionary.RECORD_CHUNK_META).getRawValue();
        if (rawValue != null && rawValue instanceof Map)
            return (Map<String, Object>) getField(FieldDictionary.RECORD_CHUNK_META).getRawValue();
        return null;
    }

    public void setAttributes(Map<String, Object> attributes) {
        setField(FieldDictionary.RECORD_CHUNK_META, FieldType.MAP, attributes);
    }

    public String getMetricName() {
        return getField(FieldDictionary.RECORD_NAME).asString();
    }

    public void setMetricName(String metricName) {
        setStringField(FieldDictionary.RECORD_NAME, metricName);
    }

    public String getMetricType() {
        return getType();
    }

    public void setMetricType(String metricType) {
        setType(metricType);
    }

//    public List<Long> getTimestamps() {
//        return timestamps;
//    }
//
//    public void setTimestamps(List<Long> timestamps) {
//        this.timestamps = timestamps;
//    }
//
//    public List<Double> getValues() {
//        return values;
//    }
//
//    public void setValues(List<Double> values) {
//        this.values = values;
//    }

    public byte[] getPoints() {
        return getField(FieldDictionary.RECORD_VALUE).asBytes();
    }

    public void setPoints(byte[] points) {
        setField(FieldDictionary.RECORD_VALUE, FieldType.BYTES, points);
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
