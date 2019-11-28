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

import com.hurence.logisland.timeseries.MetricTimeSeries;

/**
 * Timeseries holder record
 */
public class TimeSeriesRecord extends StandardRecord {

    private MetricTimeSeries timeSeries;

    public TimeSeriesRecord(MetricTimeSeries timeSeries) {
        super(timeSeries.getType());
        this.timeSeries = timeSeries;

        setStringField(FieldDictionary.RECORD_NAME, timeSeries.getName());
        setField(FieldDictionary.CHUNK_START, FieldType.LONG, timeSeries.getStart());
        setField(FieldDictionary.CHUNK_END, FieldType.LONG, timeSeries.getEnd());
        setField(FieldDictionary.CHUNK_SIZE, FieldType.INT, timeSeries.getValues().size());
        setField(FieldDictionary.CHUNK_WINDOW_MS, FieldType.LONG, timeSeries.getEnd() - timeSeries.getStart());

        timeSeries.attributes().keySet().forEach(key -> {
            setStringField(key, String.valueOf(timeSeries.attribute(key)));
        });
    }

    public MetricTimeSeries getTimeSeries() {
        return timeSeries;
    }


}
