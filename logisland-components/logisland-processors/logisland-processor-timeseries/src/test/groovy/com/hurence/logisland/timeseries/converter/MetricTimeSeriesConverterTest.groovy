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
import spock.lang.Specification

import java.time.Instant

/**
 * Unit test for the name time series converter
 * @author f.lautenschlager
 */
class MetricTimeSeriesConverterTest extends Specification {

    def "test to and from compressed data"() {
        given:
        def ts = new MetricTimeSeries.Builder("\\Load\\avg", "metric").attribute("MyField", 4711)
        def start = Instant.now()

        100.times {
            ts.point(start.plusSeconds(it).toEpochMilli(), it * 2)
        }

        def converter = new MetricTimeSeriesConverter()

        when:
        def binaryTimeSeries = converter.to(ts.build())
        def tsReconverted = converter.from(binaryTimeSeries, start.toEpochMilli(), start.plusSeconds(20).toEpochMilli())

        then:
        tsReconverted.name == "\\Load\\avg"
        tsReconverted.type == "metric"
        tsReconverted.size() == 21
        tsReconverted.getValue(1) == 2
        tsReconverted.attribute("MyField") == 4711

    }

    def "test to and from aggregated value"() {
        given:
        def converter = new MetricTimeSeriesConverter()

        def binTs = new BinaryTimeSeries.Builder()
                .field("0_function_avg", 4711d)
                .name("\\Load\\avg")
                .type("metric")
                .start(0)
                .end(10)

        when:
        def tsReconverted = converter.from(binTs.build(), 0, 100)

        then:
        tsReconverted.name == "\\Load\\avg"
        tsReconverted.type == "metric"
        tsReconverted.size() == 0
        tsReconverted.attribute("0_function_avg") == 4711d
        tsReconverted.start == 0
        tsReconverted.end == 10
    }

    def "test to and from json data"() {
        given:
        def converter = new MetricTimeSeriesConverter()

        def binTs = new BinaryTimeSeries.Builder()
                .field("0_function_avg", 4711d)
                .field("dataAsJson", "[[0,1,2,3],[4711.0,4712.0,4713.0,4714.0]]")
                .name("\\Load\\avg")
                .type("metric")
                .start(0)
                .end(10)

        when:
        def tsReconverted = converter.from(binTs.build(), 0, 100)

        then:
        tsReconverted.name == "\\Load\\avg"
        tsReconverted.type == "metric"
        tsReconverted.size() == 4
        tsReconverted.getValue(3) == 4714d
        tsReconverted.start == 0
        tsReconverted.end == 3
    }

    def "test to and from json data with encoding exception"() {
        given:
        def converter = new MetricTimeSeriesConverter();

        def binTs = new BinaryTimeSeries.Builder()
                .field("0_function_avg", 4711d)
                .field("dataAsJson", new String("[[0,1,2,3],[4711.0,4712.0,4713.0,4714.0]]".getBytes("IBM420")))
                .name("\\Load\\avg")
                .type("metric")
                .start(0)
                .end(10)

        when:
        def tsReconverted = converter.from(binTs.build(), 0, 100)

        then:
        tsReconverted.name == "\\Load\\avg"
        tsReconverted.type == "metric"
        tsReconverted.size() == 0
        tsReconverted.start == 0
        tsReconverted.end == 0
    }
}
