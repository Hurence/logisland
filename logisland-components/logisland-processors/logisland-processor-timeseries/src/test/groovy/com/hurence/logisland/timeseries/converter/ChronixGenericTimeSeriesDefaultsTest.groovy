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
package com.hurence.logisland.timeseries.converter

import com.hurence.logisland.timeseries.MetricTimeSeries
import com.hurence.logisland.timeseries.dts.Point
import spock.lang.Specification

import java.util.stream.Collectors

/**
 * Unit test for the reduce and group by defaults
 * @author f.lautenschlager
 */
class ChronixGenericTimeSeriesDefaultsTest extends Specification {

    def "test default group by"() {
        given:
        def metricTimeSeries = new MetricTimeSeries.Builder("name", "metric").build()

        when:
        def groupBy = ChronixTimeSeriesDefaults.GROUP_BY.apply(metricTimeSeries)

        then:
        groupBy == "name"
    }

    def "test default reduce"() {

        given:
        def ts1 = new MetricTimeSeries.Builder("name", "metric")
                .attribute("existsInBoth", 1)
                .attribute("onlyInTs1", "value only in ts1")
                .attribute("simpleList", ["one", "two", "three"])
                .point(1, 2)
                .point(3, 6)
                .point(5, 10)
                .build()
        def ts2 = new MetricTimeSeries.Builder("name", "metric")
                .attribute("existsInBoth", 2)
                .attribute("onlyInTs2", "ts1 would never see me")
                .attribute("simpleList", ["four", "five", "six"])
                .attribute("threads", [1, 2, 3])
                .point(2, 4)
                .point(4, 8)
                .point(6, 12)
                .build()
        when:
        def merged = ChronixTimeSeriesDefaults.REDUCE.apply(ts1, ts2)

        then:
        merged.size() == 6
        merged.sort()
        def list = merged.points().collect(Collectors.toList())
        list.containsAll([new Point(0, 1, 2), new Point(1, 2, 4), new Point(2, 3, 6)
                          , new Point(3, 4, 8), new Point(4, 5, 10), new Point(5, 6, 12)])
        merged.attributes().size() == 5
        merged.attribute("existsInBoth") as Set<Integer> == [1, 2] as Set<Integer>
        merged.attribute("onlyInTs1") as String == "value only in ts1"
        merged.attribute("onlyInTs2") as Set<String> == ["ts1 would never see me"] as Set<String>
        merged.attribute("threads") as Set == [[1, 2, 3]] as Set
        merged.attribute("simpleList") as Set == [["one", "two", "three"], ["four", "five", "six"]] as Set
    }

    def "test private constructor"() {
        when:
        ChronixTimeSeriesDefaults.newInstance()
        then:
        noExceptionThrown()
    }
}
