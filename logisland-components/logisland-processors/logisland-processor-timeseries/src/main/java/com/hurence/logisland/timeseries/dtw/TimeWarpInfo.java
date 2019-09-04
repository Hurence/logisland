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
package com.hurence.logisland.timeseries.dtw;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * @author Stan Salvador (stansalvador@hotmail.com)
 * @author f.lautenschlager
 */
public class TimeWarpInfo {
    private final double distance;
    private final WarpPath path;
    private final int base;

    /**
     * @param dist
     * @param wp
     * @param sizeI
     * @param sizeJ
     */
    public TimeWarpInfo(double dist, WarpPath wp, int sizeI, int sizeJ) {
        distance = dist;
        path = wp;
        base = sizeI + sizeJ;
    }

    /**
     * @return the distance between the two time series
     */
    public double getDistance() {
        return distance;
    }

    /**
     * Normalizes the distance based on the length of the time series.
     * The returned value is the average per-step distance.
     * A value of 0.0 indicates that the time series are equals.
     *
     * @return the normalized distance.
     */
    public double getNormalizedDistance() {
        return (distance / base);

    }

    /**
     * The resulting warp path
     *
     * @return the warp path
     */
    public WarpPath getPath() {
        return path;
    }


    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        TimeWarpInfo rhs = (TimeWarpInfo) obj;
        return new EqualsBuilder()
                .append(this.distance, rhs.distance)
                .append(this.path, rhs.path)
                .append(this.base, rhs.base)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder()
                .append(distance)
                .append(path)
                .append(base)
                .toHashCode();
    }


    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("distance", distance)
                .append("path", path)
                .append("base", base)
                .toString();
    }
}