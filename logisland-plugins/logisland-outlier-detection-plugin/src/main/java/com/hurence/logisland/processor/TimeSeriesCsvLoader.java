package com.hurence.logisland.processor;

import com.hurence.logisland.event.Event;
import com.hurence.logisland.log.LogParserException;

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
     * @param in
     * @param hasHeader
     * @param inputDatetimeFormat input date format
     * @return
     * @throws IOException
     * @throws IllegalArgumentException
     * @throws ArrayIndexOutOfBoundsException
     */
    public static List<Event> load(Reader in, boolean hasHeader, DateTimeFormatter inputDatetimeFormat)
            throws IOException, LogParserException {

        List<Event> events = new ArrayList<>();
        for (CSVRecord record : CSVFormat.DEFAULT.parse(in)) {
            try {
                if (!hasHeader) {
                    Event event = new Event("sensors");
                    event.put(TIMESTAMP_KEY, "long", inputDatetimeFormat.withZone(DateTimeZone.UTC).parseDateTime(record.get(0)).getMillis());
                    event.put(VALUE_KEY, "double", Double.parseDouble(record.get(1)));

                    events.add(event);
                } else {
                    TIMESTAMP_KEY = record.get(0);
                    VALUE_KEY = record.get(1);
                }

                hasHeader = false;
            } catch (Exception e) {
                logger.error("Parsing error " + e.getMessage());
                throw new LogParserException("parsing error", e);
            }
        }

        return events;
    }
}
