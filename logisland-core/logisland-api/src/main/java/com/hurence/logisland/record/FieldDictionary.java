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

import org.apache.commons.collections.ListUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FieldDictionary {

    public static final String RECORD_ERRORS = "record_errors";
    public static final String RECORD_TYPE = "record_type";
    public static final String RECORD_ID = "record_id";
    public static final String RECORD_TIME = "record_time";
    public static final String RECORD_PROCESSING_TIME = "record_processing_time";
    public static final String RECORD_DAYTIME = "record_daytime";
    public static final String RECORD_KEY = "record_key";
    public static final String RECORD_VALUE = "record_value";
    public static final String RECORD_NAME = "record_name";
    public static final String PROCESSOR_NAME = "processor_name";
    public static final String RECORD_BODY = "record_body";
    public static final String RECORD_COUNT = "record_count";
    public static final String RECORD_LAST_UPDATE_TIME = "record_last_update_time";

    public static final String RECORD_CHUNK_COMPRESSED_POINTS = "record_chunk_compressed_points";
    public static final String RECORD_CHUNK_UNCOMPRESSED_POINTS = "record_chunk_uncompressed_points";
    public static final String RECORD_CHUNK_SAX_POINTS = "record_chunk_sax_points";
    public static final String CHUNK_START = "chunk_start";
    public static final String CHUNK_END = "chunk_end";
    public static final String CHUNK_META = "chunk_attribute";
    public static final String CHUNK_MAX = "chunk_max";
    public static final String CHUNK_MIN = "chunk_min";
    public static final String CHUNK_FIRST_VALUE = "chunk_first";
    public static final String CHUNK_AVG = "chunk_avg";
    public static final String CHUNK_SAX = "chunk_sax";
    public static final String CHUNK_TREND = "chunk_trend";
    public static final String CHUNK_OUTLIER = "chunk_outlier";
    public static final String CHUNK_SIZE = "chunk_size";
    public static final String CHUNK_VALUE = "chunk_value";
    public static final String CHUNK_SIZE_BYTES ="chunk_size_bytes";
    public static final String CHUNK_SUM ="chunk_sum";
    public static final String CHUNK_WINDOW_MS = "chunk_window_ms";

    public static final String RECORD_TIMESERIE_POINT_TIMESTAMP = "record_timeserie_time";
    public static final String RECORD_TIMESERIE_POINT_VALUE = "record_timeserie_value";

    public static final String RECORD_POSITION = "record_position";
    public static final String RECORD_POSITION_LATITUDE = "record_position_latitude";
    public static final String RECORD_POSITION_LONGITUDE = "record_position_longitude";
    public static final String RECORD_POSITION_ALTITUDE = "record_position_altitude";
    public static final String RECORD_POSITION_HEADING = "record_position_heading";
    public static final String RECORD_POSITION_PRECISION = "record_position_precision";
    public static final String RECORD_POSITION_SATELLITES = "record_position_satellites";
    public static final String RECORD_POSITION_SPEED = "record_position_speed";
    public static final String RECORD_POSITION_STATUS = "record_position_status";
    public static final String RECORD_POSITION_TIMESTAMP = "record_position_timestamp";

    public static Boolean contains(String fieldName) {
        return asList().contains(fieldName);
    }

    /**
     * Technical fields. If accessed, do not modify!
     * Kept accessible for performance purpose (no duplicate on usage like when using asList())
     */
    public static final List<String> TECHNICAL_FIELDS = Arrays.asList(
            RECORD_ERRORS,
            RECORD_TYPE,
            RECORD_ID,
            RECORD_TIME,
            RECORD_DAYTIME,
            RECORD_KEY,
            RECORD_VALUE,
            RECORD_NAME,
            PROCESSOR_NAME,
            RECORD_BODY,
            RECORD_COUNT,
            RECORD_LAST_UPDATE_TIME
        );

    /**
     * Position fields. If accessed, do not modify!
     * Kept accessible for performance purpose (no duplicate on usage like when using asList())
     */
    public static final List<String> POSITION_FIELDS = Arrays.asList(
            RECORD_POSITION,
            RECORD_POSITION_LATITUDE,
            RECORD_POSITION_LONGITUDE,
            RECORD_POSITION_ALTITUDE,
            RECORD_POSITION_HEADING,
            RECORD_POSITION_PRECISION,
            RECORD_POSITION_SATELLITES,
            RECORD_POSITION_SPEED,
            RECORD_POSITION_STATUS,
            RECORD_POSITION_TIMESTAMP
    );

    public static final List<String> CHUNK_FIELDS = Arrays.asList(
            CHUNK_START,
            CHUNK_END,
            CHUNK_META,
            CHUNK_MAX,
            CHUNK_MIN,
            CHUNK_AVG,
            CHUNK_SAX
    );

    public static final List<String> TIMESERIE_POINT_FIELDS = Arrays.asList(
            RECORD_TIMESERIE_POINT_TIMESTAMP,
            RECORD_TIMESERIE_POINT_VALUE
    );
    /**
     * All fields. If accessed, do not modify!
     * Kept accessible for performance purpose (no duplicate on usage like when using asList())
     */
    public static final List<String> ALL = ListUtils.union(TECHNICAL_FIELDS, POSITION_FIELDS);

    public static List<String> asList() {
        return new ArrayList<String>(ALL);
    }
}
