/**
 * Copyright (C) 2016 Hurence (support@hurence.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.record;


import java.util.Arrays;
import java.util.List;

public class FieldDictionary {

    public static String RECORD_ERRORS = "record_errors";
    public static String RECORD_TYPE = "record_type";
    public static String RECORD_ID = "record_id";
    public static String RECORD_TIME = "record_time";
    public static String RECORD_DAYTIME = "record_daytime";
    public static String RECORD_KEY = "record_key";
    public static String RECORD_VALUE = "record_value";
    public static String RECORD_NAME = "record_name";
    public static String PROCESSOR_NAME = "processor_name";
    public static String RECORD_POSITION = "record_position";
    public static String RECORD_BODY = "record_body";
    public static String RECORD_COUNT = "record_count";


    public static String RECORD_POSITION_LATITUDE = "record_position_latitude";
    public static String RECORD_POSITION_LONGITUDE = "record_position_longitude";
    public static String RECORD_POSITION_ALTITUDE = "record_position_altitude";
    public static String RECORD_POSITION_HEADING = "record_position_heading";
    public static String RECORD_POSITION_PRECISION = "record_position_precision";
    public static String RECORD_POSITION_SATELLITES = "record_position_satellites";
    public static String RECORD_POSITION_SPEED = "record_position_speed";
    public static String RECORD_POSITION_STATUS = "record_position_status";
    public static String RECORD_POSITION_TIMESTAMP = "record_position_timestamp";

    public static Boolean contains(String fieldName) {
        return asList().contains(fieldName);
    }

    public static List<String> asList() {
        return Arrays.asList(
                RECORD_ERRORS,
                RECORD_TYPE,
                RECORD_ID,
                RECORD_TIME,
                RECORD_DAYTIME,
                RECORD_KEY,
                RECORD_VALUE,
                PROCESSOR_NAME,
                RECORD_POSITION,
                RECORD_BODY,
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
    }
}
