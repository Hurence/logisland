package com.hurence.logisland.processor.webanalytics.util;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

public class DateUtils {

    /**
     * The legacy format used in Date.toString().
     */
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss zzz yyyy",
            Locale.ENGLISH);

    /**
     * Returns the epoch timestamp corresponding to the specified value parsed with the default formatter.
     *
     * @param string the value to parse with the default formatter.
     *
     * @return the epoch timestamp corresponding to the specified value parsed with the default formatter.
     */
    public static long toEpochMilli(final String string)
    {
        return LocalDateTime.parse(string, DATE_FORMAT)
                .atZone(ZoneId.systemDefault())
                .toInstant()
                .toEpochMilli();
    }

    /**
     * Returns the epoch timestamp corresponding to the specified value parsed with the default formatter.
     *
     * @param string the value to parse with the default formatter.
     *
     * @return the epoch timestamp corresponding to the specified value parsed with the default formatter.
     */
    public static long toEpochSecond(final String string)
    {
        return LocalDateTime.parse(string, DATE_FORMAT)
                .atZone(ZoneId.systemDefault())
                .toInstant()
                .getEpochSecond();
    }

    /**
     * Returns the provided epoch timestamp formatted with the default formatter.
     *
     * @param epoch the timestamp in milliseconds.
     *
     * @return the provided epoch timestamp formatted with the default formatter.
     */
    public static String toFormattedDate(final long epoch)
    {
        ZonedDateTime date = Instant.ofEpochMilli(epoch).atZone(ZoneId.systemDefault());
        String result = DATE_FORMAT.format(date);

        return result;
    }
}
