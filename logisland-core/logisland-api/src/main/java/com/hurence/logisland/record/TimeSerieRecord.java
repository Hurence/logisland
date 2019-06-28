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
import java.util.Map;

/**
 *
 */
public abstract class TimeSerieRecord extends StandardRecord {

    private static Logger logger = LoggerFactory.getLogger(TimeSerieRecord.class);
    public static String DEFAULT_TIME_SERIES_POINT_RECORD_TYPE = RecordDictionary.TIMESERIES;

    public TimeSerieRecord() {
        super(DEFAULT_TIME_SERIES_POINT_RECORD_TYPE);
    }

    public TimeSerieRecord(String metricName) {
        this(DEFAULT_TIME_SERIES_POINT_RECORD_TYPE, metricName);
    }

    public TimeSerieRecord(String metricType, String metricName) {
        super(metricType);
        setMetricName(metricName);
    }

    @Override
    public String toString() {
        return super.toString();
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
}
