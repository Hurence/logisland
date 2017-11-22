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


import com.sun.org.apache.xpath.internal.operations.Bool;

import java.util.ArrayList;
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
    public static String RECORD_RAW_KEY = "record_raw_key";
    public static String RECORD_RAW_VALUE = "record_raw_value";
    public static String PROCESSOR_NAME = "processor_name";

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
            RECORD_RAW_KEY,
            RECORD_RAW_VALUE,
            PROCESSOR_NAME
        );
    }
}
