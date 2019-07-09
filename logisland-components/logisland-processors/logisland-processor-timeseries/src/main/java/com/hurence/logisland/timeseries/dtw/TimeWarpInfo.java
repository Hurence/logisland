/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2016 Stan Salvador (stansalvador@hotmail.com), Philip Chan (pkc@cs.fit.edu), QAware GmbH
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
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