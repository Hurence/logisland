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
package com.caseystella.analytics.distribution.config;

import com.caseystella.analytics.distribution.TimeRange;
import com.google.common.base.Function;
import org.joda.time.*;

import javax.annotation.Nullable;
import java.util.Date;

public enum Unit implements Function<TimeRange, Long> {

    MILLISECONDS(new SimpleConversion(1L)),
    SECONDS(new SimpleConversion(SimpleConversion.MS_IN_SECOND)),
    HOURS(new SimpleConversion(SimpleConversion.MS_IN_HOUR)),
    DAYS(new SimpleConversion(SimpleConversion.MS_IN_DAY)),
    MONTHS(new Function<TimeRange, Long>() {
        @Nullable
        @Override
        public Long apply(@Nullable TimeRange timeRange) {
            DateTime end = new DateTime(timeRange.getEnd());
            DateTime begin = new DateTime(timeRange.getBegin());
            Months months = Months.monthsBetween(begin, end);
            return (long)months.getMonths();
        }
    }),
    YEARS(new Function<TimeRange, Long>() {

        @Nullable
        @Override
        public Long apply(@Nullable TimeRange timeRange) {
            DateTime end = new DateTime(timeRange.getEnd());
            DateTime begin = new DateTime(timeRange.getBegin());
            Years years = Years.yearsBetween(begin, end);
            return (long)years.getYears();
        }
    }),
    POINTS(new Function<TimeRange, Long>() {
        @Nullable
        @Override
        public Long apply(@Nullable TimeRange timeRange) {
            return null;
        }
    });


    public static class SimpleConversion implements Function<TimeRange, Long>
    {
        public static final long MS_IN_SECOND = 1000;
        public static final long MS_IN_MINUTE = 60*MS_IN_SECOND;
        public static final long MS_IN_HOUR = 60*MS_IN_MINUTE;
        public static final long MS_IN_DAY = 24*MS_IN_HOUR;
        long conversion;
        public SimpleConversion(long conversion) {
            this.conversion = conversion;
        }

        @Nullable
        @Override
        public Long apply(@Nullable TimeRange timeRange) {
            return (timeRange.getEnd()- timeRange.getBegin())/conversion;
        }
    }

    private Function<TimeRange, Long> _func;

    Unit(Function<TimeRange, Long> _func)
    {
        this._func = _func;
    }

    public Long apply(TimeRange in) {
        return _func.apply(in);
    }
}
