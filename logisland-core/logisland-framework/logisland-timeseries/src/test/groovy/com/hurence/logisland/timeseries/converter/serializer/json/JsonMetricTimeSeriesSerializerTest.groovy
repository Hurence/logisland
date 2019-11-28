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
package com.hurence.logisland.timeseries.converter.serializer.json

import com.hurence.logisland.timeseries.converter.common.DoubleList
import com.hurence.logisland.timeseries.converter.common.LongList
import com.hurence.logisland.timeseries.MetricTimeSeries
import spock.lang.Specification
import spock.lang.Unroll

import java.nio.charset.Charset

/**
 * Unit test for the time json metric series serializer
 * @author f.lautenschlager
 */
class JsonMetricTimeSeriesSerializerTest extends Specification {

    def "test serialize to and deserialize from json"() {
        given:
        def times = longList([0, 1, 2])
        def values = doubleList([4711d, 8564d, 1237d])
        def ts = new MetricTimeSeries.Builder("test", "metric").points(times, values).build()

        def serializer = new JsonMetricTimeSeriesSerializer()
        def start = 0l
        def end = 2l

        when:
        def json = serializer.toJson(ts)

        then:
        def builder = new MetricTimeSeries.Builder("test", "metric")
        serializer.fromJson(json, start, end, builder)
        def recoverdTs = builder.build();


        times.size() == 3
        recoverdTs.size() == 3

        recoverdTs.getTime(0) == 0l
        recoverdTs.getTime(1) == 1l
        recoverdTs.getTime(2) == 2l

        recoverdTs.getValue(0) == 4711d
        recoverdTs.getValue(1) == 8564d
        recoverdTs.getValue(2) == 1237d
    }

    @Unroll
    def "test serialize and deserialize from json with filter #start and #end expecting #size elements"() {
        given:
        def times = longList([0, 1, 2, 3, 4, 5])
        def values = doubleList([4711, 8564, 8564, 1237, 1237, 1237])
        def ts = new MetricTimeSeries.Builder("test", "metric").points(times, values).build()

        def serializer = new JsonMetricTimeSeriesSerializer()

        when:
        def builder = new MetricTimeSeries.Builder("test", "metric")

        def json = serializer.toJson(ts)
        serializer.fromJson(json, start, end, builder)
        then:
        builder.build().size() == size

        where:
        start << [0, 1, 1, 0]
        end << [0, 1, 3, 6]
        size << [0, 1, 3, 6]
    }

    def "test serialize to json with empty timestamps, values"() {
        given:
        def serializer = new JsonMetricTimeSeriesSerializer()
        def ts = new MetricTimeSeries.Builder("test", "metric").points(times, values).build()

        when:
        def json = serializer.toJson(ts)

        then:
        new String(json) == "[[],[]]"

        where:
        times << [null, longList([0l, 1l, 2l]) as LongList]
        values << [doubleList([0l, 1l, 2l]) as DoubleList, null]
    }

    def longList(ArrayList<Long> longs) {
        def times = new LongList()
        longs.each { l -> times.add(l) }

        times
    }

    def doubleList(ArrayList<Double> doubles) {
        def values = new DoubleList()
        doubles.each { l -> values.add(l) }

        values
    }

    def "test deserialize from empty json "() {
        given:
        def serializer = new JsonMetricTimeSeriesSerializer()
        def builder = new MetricTimeSeries.Builder("test", "metric")

        when:
        serializer.fromJson("[[],[]]".getBytes(Charset.forName("UTF-8")), 1, 2000, builder)
        def ts = builder.build()
        then:
        ts.size() == 0
    }
}
