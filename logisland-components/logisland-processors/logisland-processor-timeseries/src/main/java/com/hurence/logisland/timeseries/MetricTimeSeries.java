/*
 * Copyright (C) 2016 QAware GmbH
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package com.hurence.logisland.timeseries;



import com.hurence.logisland.timeseries.converter.common.DoubleList;
import com.hurence.logisland.timeseries.converter.common.LongList;
import com.hurence.logisland.timeseries.dts.Point;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.io.Serializable;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

/**
 * A name time series that have at least the following fields:
 * - name,
 * - start and end,
 * - arbitrary attributes
 * and a list of name data points (timestamp, double value)
 *
 * @author f.lautenschlager
 */
public final class MetricTimeSeries implements Serializable {

    private static final long serialVersionUID = 5497398456431471102L;

    private String name;
    private String type;

    private LongList timestamps;
    private DoubleList values;

    private Map<String, Object> attributes = new HashMap<>();
    private long end;
    private long start;

    //Marks if the time series needs a sort
    //Used to avoid unnecessary sorts.
    private boolean needsSort = true;

    /**
     * Private constructor.
     * To instantiate a time series use the builder class.
     */
    private MetricTimeSeries() {
        timestamps = new LongList(500);
        values = new DoubleList(500);
    }

    /**
     * Sets the start and end end based on the
     */
    private void setStartAndEnd() {
        //When the time stamps are empty we do not set the start and end
        //An aggregation or analysis response does not have a data field per default.
        if (!timestamps.isEmpty()) {
            sort();
            start = timestamps.get(0);
            end = timestamps.get(size() - 1);
        }
    }

    /**
     * @return a copy of the timestamps
     */
    public LongList getTimestamps() {
        return timestamps.copy();
    }

    /**
     * In some cases if one just want to access all values,
     * that method is faster than {@see getTimestamps} due to no {@see LongList} initialization.
     *
     * @return a copy of the timestamps as array
     */
    public long[] getTimestampsAsArray() {
        return timestamps.toArray();
    }

    /**
     * @return a copy of the data points
     */
    public DoubleList getValues() {
        return values.copy();
    }

    /**
     * In some cases if one just want to access all values,
     * that method is faster than {@see getValues} due to no {@see DoubleList} initialization.
     *
     * @return a copy of the values as array
     */
    public double[] getValuesAsArray() {
        return values.toArray();
    }

    /**
     * Gets the data point at the index i
     *
     * @param i the index position of the value
     * @return the value
     */
    public double getValue(int i) {
        return values.get(i);
    }

    /**
     * Gets the timestamp at the given index
     *
     * @param i the index position of the time stamp
     * @return the timestamp as long
     */
    public long getTime(int i) {
        return timestamps.get(i);
    }

    /**
     * Sorts the time series values.
     */
    public void sort() {
        if (needsSort && timestamps.size() > 1) {

            LongList sortedTimes = new LongList(timestamps.size());
            DoubleList sortedValues = new DoubleList(values.size());

            points().sorted(Comparator.comparingLong(Point::getTimestamp)).forEachOrdered(p -> {
                sortedTimes.add(p.getTimestamp());
                sortedValues.add(p.getValue());
            });

            timestamps = sortedTimes;
            values = sortedValues;

            needsSort = false;
        }
    }

    /**
     * A stream over the points
     *
     * @return the points as points
     */
    public Stream<Point> points() {
        if (timestamps.isEmpty()) {
            return Stream.empty();
        }
        return Stream.iterate(of(0), pair -> of(pair.getIndex() + 1)).limit(timestamps.size());
    }

    private Point of(int index) {
        return new Point(index, timestamps.get(index), values.get(index));
    }


    /**
     * Sets the timestamps and values as data
     *
     * @param timestamps - the timestamps
     * @param values     - the values
     */
    private void setAll(LongList timestamps, DoubleList values) {
        this.timestamps = timestamps;
        this.values = values;

        needsSort = true;
    }

    /**
     * Adds all the given points to the time series
     *
     * @param timestamps the timestamps
     * @param values     the values
     */
    public final void addAll(LongList timestamps, DoubleList values) {
        for (int i = 0; i < timestamps.size(); i++) {
            add(timestamps.get(i), values.get(i));
        }
    }

    /**
     * @param timestamps the timestamps as long[]
     * @param values     the values as double[]
     */
    public final void addAll(long[] timestamps, double[] values) {
        this.timestamps.addAll(timestamps);
        this.values.addAll(values);

        needsSort = true;
    }

    /**
     * Adds a single timestamp and value
     *
     * @param timestamp the timestamp
     * @param value     the value
     */
    public final void add(long timestamp, double value) {
        this.timestamps.add(timestamp);
        this.values.add(value);

        needsSort = true;
    }

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * Adds an attribute to the time series
     *
     * @param key   the key
     * @param value the value
     */
    private void addAttribute(String key, Object value) {
        attributes.put(key, value);
    }

    /**
     * Get the attribute for the given key
     *
     * @param key the attribute key
     * @return the value as object
     */
    public Object attribute(String key) {
        return attributes.get(key);
    }

    /**
     * @return a copy of the attributes of this time series
     */
    public Map<String, Object> attributes() {
        return new HashMap<>(attributes);
    }

    /**
     * This method should be used with care as it delivers the reference.
     *
     * @return the attributes of this time series
     */
    @SuppressWarnings("all")
    public Map<String, Object> getAttributesReference() {
        return attributes;
    }

    /**
     * Clears the time series
     */
    public void clear() {
        timestamps.clear();
        values.clear();
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
        MetricTimeSeries rhs = (MetricTimeSeries) obj;
        return new EqualsBuilder()
                .append(this.getName(), rhs.getName())
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder()
                .append(getName())
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("name", name)
                .append("attributes", attributes)
                .toString();
    }


    /**
     * @return the start of the time series
     */
    public long getStart() {
        setStartAndEnd();
        return start;
    }

    /**
     * @return the end of the time series
     */
    public long getEnd() {
        setStartAndEnd();
        return end;
    }

    /**
     * @return the size
     */
    public int size() {
        return timestamps.size();
    }

    /**
     * @return empty if the time series contains no points
     */
    public boolean isEmpty() {
        return timestamps.size() == 0;
    }

    /**
     * @return the type
     */
    public String getType() {
        return type;
    }

    /**
     * The Builder class
     */
    public static final class Builder {

        /**
         * The time series object
         */
        private MetricTimeSeries metricTimeSeries;

        /**
         * Constructs a new Builder
         *
         * @param name of the time series
         * @param type of the time series
         */
        public Builder(String name, String type) {
            metricTimeSeries = new MetricTimeSeries();
            metricTimeSeries.name = name;
            metricTimeSeries.type = type;
        }


        /**
         * @return the filled time series
         */
        public MetricTimeSeries build() {
            return metricTimeSeries;
        }


        /**
         * Sets the time series data
         *
         * @param timestamps the time stamps
         * @param values     the values
         * @return the builder
         */
        public Builder points(LongList timestamps, DoubleList values) {
            if (timestamps != null && values != null) {
                metricTimeSeries.setAll(timestamps, values);
            }
            return this;
        }

        /**
         * Adds the given single data point to the time series
         *
         * @param timestamp the timestamp of the value
         * @param value     the belonging value
         * @return the builder
         */
        public Builder point(long timestamp, double value) {
            metricTimeSeries.timestamps.add(timestamp);
            metricTimeSeries.values.add(value);
            return this;
        }

        /**
         * Adds an attribute to the class
         *
         * @param key   the name of the attribute
         * @param value the value of the attribute
         * @return the builder
         */
        public Builder attribute(String key, Object value) {
            metricTimeSeries.addAttribute(key, value);
            return this;
        }

        /**
         * Sets the attributes for this time series
         *
         * @param attributes the time series attributes
         * @return the builder
         */
        public Builder attributes(Map<String, Object> attributes) {
            metricTimeSeries.attributes = attributes;
            return this;
        }

        /**
         * Sets the end of the time series
         *
         * @param end the end of the time series
         * @return the builder
         */
        public Builder end(long end) {
            metricTimeSeries.end = end;
            return this;
        }

        /**
         * Sets the start of the time series
         *
         * @param start the start of the time series
         * @return the builder
         */
        public Builder start(long start) {
            metricTimeSeries.start = start;
            return this;
        }
    }
}
