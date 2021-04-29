package com.hurence.logisland.processor.webanalytics.util;

import com.hurence.logisland.record.Field;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class Utils {

    /**
     * Return {@code true} if the provided field has a non-null value;  {@code false} otherwise.
     *
     * @param field the field to check.
     *
     * @return {@code true} if the provided field has a non-null value;  {@code false} otherwise.
     */
    public static boolean isFieldAssigned(final Field field)
    {
        return field!=null && field.getRawValue()!=null;
    }

    /**
     * Returns the name of the event index corresponding to the specified date such as
     * ${event-index-name}.${event-suffix}.
     * Eg. openanalytics-webevents.2018.01.31
     *
     * @param date the ZonedDateTime of the event to store in the index.
     * @return the name of the event index corresponding to the specified date.
     */
    public static String buildIndexName(final String indexPrefix,
                                        final DateTimeFormatter formatterSuffix,
                                        final ZonedDateTime date,
                                        final ZoneId zoneIdToUse) {
        return indexPrefix + formatterSuffix.format(date.withZoneSameInstant(zoneIdToUse));
    }

}
