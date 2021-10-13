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
