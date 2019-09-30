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
package com.hurence.logisland.timeseries

import com.hurence.logisland.timeseries.converter.common.DoubleList
import com.hurence.logisland.timeseries.converter.common.LongList
import spock.lang.Specification

/**
 * Unit test for the name time series
 * @author f.lautenschlager
 */
class MetricTimeSeriesTest extends Specification {

    def "test create a metric time series and access its values"() {
        given:
        def times = new LongList()
        def values = new DoubleList()
        10.times {
            times.add(it as long)
            values.add(it * 10 as double)
        }
        def attributes = new HashMap<String, Object>()
        attributes.put("thread", 2 as long)

        when:
        def ts = new MetricTimeSeries.Builder("//CPU//Load","metric")
                .attributes(attributes)
                .attribute("host", "laptop")
                .attribute("avg", 2.23)
                .points(times, values)
                .point(10 as long, 100)
                .build()

        then:
        ts.start == 0
        ts.end == 10
        ts.name == "//CPU//Load"
        ts.type == "metric"
        ts.attributes().size() == 3
        ts.attributesReference.size() == 3
        ts.attribute("host") == "laptop"
        ts.attribute("avg") == 2.23
        ts.attribute("thread") == 2 as long
        ts.size() == 11
        ts.getTimestamps().size() == 11
        ts.getTime(0) == 0
        ts.getValues().size() == 11
        ts.getValue(0) == 0
        //check array copy
        ts.getTimestampsAsArray().length == 11
        ts.getValuesAsArray().length == 11
        !ts.isEmpty()
    }

    def "test points"() {
        given:
        def times = new LongList()
        def values = new DoubleList()
        10.times {
            times.add(100 - it as long)
            values.add(it * 10 as double)
        }
        def ts = new MetricTimeSeries.Builder("//CPU//Load","metric").points(times, values).build()

        when:

        def stream = ts.points()
        then:
        stream.count() == 10

    }

    def "test sort"() {
        given:
        def times = new LongList()
        def values = new DoubleList()
        10.times {
            times.add(100 - it as long)
            values.add(100 - it as double)
        }
        def ts = new MetricTimeSeries.Builder("//CPU//Load","metric").build()
        ts.addAll(times, values)

        when:
        ts.sort()

        then:
        ts.getValue(0) == 91
    }

    def "test sort on empty time series"() {
        given:
        def ts = new MetricTimeSeries.Builder("//CPU//Load","metric").build()

        when:
        ts.sort()

        then:
        0 * ts.points()
    }

    def "test clear time series"() {
        given:

        def times = new LongList()
        def values = new DoubleList()

        10.times {
            times.add(it as long)
            values.add(it * 10 as double)
        }

        def ts = new MetricTimeSeries.Builder("//CPU//Load","metric")
                .points(times, values)
                .build()

        when:
        ts.clear()

        then:
        ts.size() == 0
        ts.isEmpty()
        ts.getEnd() == 0
        ts.getStart() == 0
    }

    def "test to string"() {
        given:
        def ts = new MetricTimeSeries.Builder("//CPU//Load","metric").build()

        when:
        def string = ts.toString()

        then:
        string.contains("name")
    }

    def "test equals"() {
        given:
        def ts = new MetricTimeSeries.Builder("//CPU//Load","metric").build()

        when:
        def result = ts == other

        then:
        result == expected

        where:
        other << [null, 1, new MetricTimeSeries.Builder("//CPU//Load","metric").build()]
        expected << [false, false, true]
    }

    def "test equals same instance"() {
        given:
        def ts = new MetricTimeSeries.Builder("//CPU//Load","metric").build()

        expect:
        ts == ts
    }

    def "test hash code"() {
        given:
        def ts = new MetricTimeSeries.Builder("//CPU//Load","metric").build()
        def ts2 = new MetricTimeSeries.Builder("//CPU//Load","metric").build()
        def ts3 = new MetricTimeSeries.Builder("//CPU//Load//","metric").build()

        expect:
        ts.hashCode() == ts2.hashCode()
        ts.hashCode() != ts3.hashCode()
    }

    def "test empty points"() {
        expect:
        def ts = new MetricTimeSeries.Builder("","").build()
        ts.points().count() == 0l
        ts.isEmpty()
    }

    def "test add all as array"() {
        given:
        def times = []
        def values = []
        10.times {
            times.add(100 - it as long)
            values.add(100 - it as double)
        }
        def ts = new MetricTimeSeries.Builder("//CPU//Load","metric").build()
        ts.addAll(times as long[], values as double[])

        when:
        ts.sort()

        then:
        ts.getValue(0) == 91
    }

    def "test attribute reference"() {
        given:
        def ts = new MetricTimeSeries.Builder("//CPU//Load","metric")
                .attribute("added via builder", "oh dear")
                .build()
        when:
        ts.getAttributesReference().clear()

        then:
        ts.attributes().size() == 0
    }

}
