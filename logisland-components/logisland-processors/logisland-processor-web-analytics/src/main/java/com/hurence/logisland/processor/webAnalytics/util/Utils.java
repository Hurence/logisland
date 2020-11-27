package com.hurence.logisland.processor.webAnalytics.util;

import com.hurence.logisland.record.Field;

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
}
