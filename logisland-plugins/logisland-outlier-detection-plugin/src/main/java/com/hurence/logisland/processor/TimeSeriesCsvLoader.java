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
package com.hurence.logisland.processor;

import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by mike on 15/04/16.
 */
public class TimeSeriesCsvLoader {
    private static final Logger logger = LoggerFactory.getLogger(TimeSeriesCsvLoader.class);
    private static String TIMESTAMP_KEY = "timestamp";
    private static String VALUE_KEY = "value";

    /**
     * CSV reader that waits for a 2 columns csv files with or without a header.
     * If less than 2 columns ==> exception, otherwise, the 3rd and following columns are ignored
     *
     * @param in
     * @param hasHeader
     * @param inputDatetimeFormat input date format
     * @return
     * @throws IOException
     * @throws IllegalArgumentException
     * @throws ArrayIndexOutOfBoundsException
     */
    public static List<Record> load(Reader in, boolean hasHeader, DateTimeFormatter inputDatetimeFormat)
            throws IOException {

        List<Record> records = new ArrayList<>();
        for (CSVRecord record : CSVFormat.DEFAULT.parse(in)) {
            try {
                if (!hasHeader) {
                    StandardRecord event = new StandardRecord("sensors");
                    event.setField(TIMESTAMP_KEY, FieldType.LONG, inputDatetimeFormat.withZone(DateTimeZone.UTC).parseDateTime(record.get(0)).getMillis());
                    event.setField(VALUE_KEY, FieldType.DOUBLE, Double.parseDouble(record.get(1)));

                    records.add(event);
                } else {
                    TIMESTAMP_KEY = record.get(0);
                    VALUE_KEY = record.get(1);
                }

                hasHeader = false;
            } catch (Exception e) {
                logger.error("Parsing error " + e.getMessage());
                throw new RuntimeException("parsing error", e);
            }
        }

        return records;
    }
}
