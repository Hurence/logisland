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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class TimeSeriePointRecord extends TimeSerieRecord {

    private static Logger logger = LoggerFactory.getLogger(TimeSeriePointRecord.class);

    public TimeSeriePointRecord() {
    }

    public TimeSeriePointRecord(String metricName) {
        super(metricName);
    }

    public TimeSeriePointRecord(String metricType, String metricName) {
        super(metricType, metricName);
    }

    @Override
    public String toString() {
        return super.toString();
    }

    @Override
    public boolean isInternField(String fieldName) {
        return super.isInternField(fieldName) ||
                FieldDictionary.TIMESERIE_POINT_FIELDS.contains(fieldName);
    }

//    public Map<String, Object> getAttributes() {
//        if (hasField(FieldDictionary.RECORD_CHUNK_META))
//            return (Map<String, Object>) getField(FieldDictionary.RECORD_CHUNK_META).getRawValue();
//        return null;
//    }
//
//    public void setAttributes(Map<String, Object> attributes) {
//        setField(FieldDictionary.RECORD_CHUNK_META, FieldType.MAP, attributes);
//    }
//
//    public void addAttributes(String key, Object attribute) {
//        if (!hasField(FieldDictionary.RECORD_CHUNK_META)) {
//            setAttributes(new HashMap<String, Object>());
//        }
//        getAttributes().put(key,attribute);
//    }

    public Long getTimestamp() {
        return getTime().getTime();
    }

    public void setTimestamp(Long timestamp) {
        setTime(timestamp);
    }

    public Double getValue() {
        return getField(FieldDictionary.RECORD_VALUE).asDouble();
    }

    public void setValue(Double value) {
        setField(FieldDictionary.RECORD_VALUE, FieldType.DOUBLE, value);
    }
}
