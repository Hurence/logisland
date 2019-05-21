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

import com.hurence.logisland.timeseries.GenericTimeSeries
import com.hurence.logisland.timeseries.dts.Pair
import spock.lang.Specification

/**
 * Unit test for the advanced time series converter
 * @author f.lautenschlager
 */
class GenericTimeSeriesConverterTest extends Specification {

    def "test to and from storage time series"() {
        given:
        def converter = new GenericTimeSeriesConverter()
        def points = [Pair.pairOf(1l, 1d),
                      Pair.pairOf(20l, 2d),
                      Pair.pairOf(50l, 3d)]
        def timeSeries = new GenericTimeSeries<Long, Double>(points.iterator())

        timeSeries.addAttribute("name", "myMetric")
        timeSeries.addAttribute("type", "metric")
        timeSeries.addAttribute("host", "NB-Prod-01")
        timeSeries.addAttribute("process", "timeSeriesConverterTest")
        timeSeries.addAttribute("length", points.size())
        timeSeries.addAttribute("maxValue", 3d)


        when:
        def binaryTimeSeries = converter.to(timeSeries)

        def reconvertedTimeSeries = converter.from(binaryTimeSeries, 1l, 50l);
        then:
        binaryTimeSeries != null
        reconvertedTimeSeries.size() == 4
        reconvertedTimeSeries.get(0).getSecond() == null;
        reconvertedTimeSeries.get(1).getSecond() == 1d
        reconvertedTimeSeries.get(2).getSecond() == 2d
        reconvertedTimeSeries.get(3).getSecond() == 3d

        reconvertedTimeSeries.getAttribute("host") == "NB-Prod-01"
        reconvertedTimeSeries.getAttribute("process") == "timeSeriesConverterTest"
        reconvertedTimeSeries.getAttribute("length") == 3I
        reconvertedTimeSeries.getAttribute("maxValue") == 3d
    }

    def "test from storage series with range query"() {
        given:
        def converter = new GenericTimeSeriesConverter()
        def points = [Pair.pairOf(1l, 1d),
                      Pair.pairOf(20l, 2d),
                      Pair.pairOf(50l, 3d),
                      Pair.pairOf(80l, 4d),
                      Pair.pairOf(115l, 5d),
                      Pair.pairOf(189l, 6d)]
        def timeSeries = new GenericTimeSeries<Long, Double>(points.iterator())

        timeSeries.addAttribute("name", "myMetric")
        timeSeries.addAttribute("type", "metric")

        when:
        def binaryTimeSeries = converter.to(timeSeries)

        def reconvertedTimeSeries = converter.from(binaryTimeSeries, 50l, 115l);
        then:
        binaryTimeSeries != null
        reconvertedTimeSeries.size() == 4
        reconvertedTimeSeries.get(0).getSecond() == null;
        reconvertedTimeSeries.get(1).getSecond() == 3d
        reconvertedTimeSeries.get(2).getSecond() == 4d
        reconvertedTimeSeries.get(3).getSecond() == 5d
    }

    def "test to with empty time series"() {

        given:
        def converter = new GenericTimeSeriesConverter()
        def points = []

        when:
        def binaryTimeSeries = converter.to(new GenericTimeSeries<Long, Double>(points.iterator()))

        then:
        binaryTimeSeries.points == new byte[0];
    }
}
