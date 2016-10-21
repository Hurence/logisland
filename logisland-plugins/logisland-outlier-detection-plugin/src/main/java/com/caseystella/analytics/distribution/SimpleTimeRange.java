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
package com.caseystella.analytics.distribution;

public class SimpleTimeRange implements TimeRange{
    private long begin;
    private long end;
    public SimpleTimeRange(TimeRange tr) {
        this(tr.getBegin(), tr.getEnd());
    }

    public SimpleTimeRange(long begin, long end) {
        this.begin = begin;
        this.end = end;
    }

    @Override
    public Long getBegin() {
        return begin;
    }

    @Override
    public Long getEnd() {
        return end;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SimpleTimeRange that = (SimpleTimeRange) o;

        if (getBegin() != that.getBegin()) return false;
        return getEnd() == that.getEnd();

    }

    @Override
    public int hashCode() {
        int result = (int) (getBegin() ^ (getBegin() >>> 32));
        result = 31 * result + (int) (getEnd() ^ (getEnd() >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "(" + begin +
                "," + end +
                ')';
    }
}
