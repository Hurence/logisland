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
package com.hurence.logisland.timeseries.functions.encoding

import com.hurence.logisland.timeseries.MetricTimeSeries
import com.hurence.logisland.timeseries.converter.common.DoubleList
import com.hurence.logisland.timeseries.converter.common.LongList
import com.hurence.logisland.timeseries.functions.FunctionValueMap
import com.hurence.logisland.timeseries.functions.aggregation.Median
import spock.lang.Specification

/**
 * Unit test for the median aggregation
 * @author bailett
 */
class SaxTest extends Specification {
    def "test execute"() {
        given:
        MetricTimeSeries.Builder timeSeries = new MetricTimeSeries.Builder("Sax", "metric")

        LongList timestamps = new LongList()
        15.times {
            timestamps.add(it * 10)
        }

        DoubleList values = new DoubleList()
        values.add(2.02)
        values.add(2.33)
        values.add(2.99)
        values.add(6.85)
        values.add(9.20)
        values.add(8.80)
        values.add(7.50)
        values.add(6.00)
        values.add(5.85)
        values.add(3.85)
        values.add(4.85)
        values.add(3.85)
        values.add(2.22)
        values.add(1.45)
        values.add(1.34)

        timeSeries.points(timestamps, values)
        MetricTimeSeries ts = timeSeries.build()

        def analysisResult = new FunctionValueMap(0, 0, 0, 3)

        when:
        new Sax(["11", "0.01", "10"] as String[]).execute(ts, analysisResult)
        new Sax(["10", "0.01", "14"] as String[]).execute(ts, analysisResult)
        new Sax(["7", "0.01", "9"] as String[]).execute(ts, analysisResult)

        then:
        analysisResult.getEncodingValue(0) == "bcjkiheebb"
        analysisResult.getEncodingValue(1) == "bcdijjhgfeecbb"
        analysisResult.getEncodingValue(2) == "bcggfddba"
    }

    def "test for empty time series"() {
        given:
        def analysisResult = new FunctionValueMap(0, 0, 0, 1)

        when:
        new Sax().execute(new MetricTimeSeries.Builder("Empty", "metric").build(), analysisResult)

        then:
        analysisResult.getEncodingValue(0).isEmpty()
    }


    def "test arguments"() {
        expect:
        new Sax().getArguments().length == 3
    }

    def "test type"() {
        expect:
        new Sax().getQueryName() == "sax"
    }

    def "test equals and hash code"() {
        expect:
        def sax = new Sax()
        !sax.equals(null)
        !sax.equals(new Object())
        sax.equals(sax)
        sax.equals(new Sax())
        new Sax().hashCode() == new Sax().hashCode()
    }
}
