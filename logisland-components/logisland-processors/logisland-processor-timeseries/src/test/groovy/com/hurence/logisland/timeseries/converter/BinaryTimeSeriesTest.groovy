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

import spock.lang.Specification

import java.time.Instant

/**
 * Tests the creation of a binary storage document
 * @author f.lautenschlager
 */
class BinaryTimeSeriesTest extends Specification {

    def "test creation of a binary storage document"() {

        given:
        def builder = new BinaryTimeSeries.Builder()
        def start = Instant.now().toEpochMilli()
        def end = Instant.now().plusSeconds(64000).toEpochMilli()

        when:
        def binaryTimeSeries = builder
                .id("6525-9662-2342")
                .start(start)
                .end(end)
                .data("The-Binary-Large-Object".getBytes())
                .type("String")
                .name("Name")
                .field("host", "myProductionHost")
                .field("size", 0)
                .build()

        then:
        binaryTimeSeries != null
        binaryTimeSeries.getFields().size() == 8

        binaryTimeSeries.getId() == "6525-9662-2342"
        binaryTimeSeries.getStart() == start
        binaryTimeSeries.getEnd() == end
        binaryTimeSeries.getPoints() == "The-Binary-Large-Object".getBytes()
        binaryTimeSeries.getType() == "String"
        binaryTimeSeries.getName() == "Name"
        binaryTimeSeries.get("host") == "myProductionHost"
        binaryTimeSeries.get("size") == 0

    }

    def "test creation of a binary storage document without start and end"() {

        given:
        def builder = new BinaryTimeSeries.Builder()

        when:
        def binaryTimeSeries = builder
                .id("6525-9662-2342")
                .data("The-Binary-Large-Object".getBytes())
                .type("String")
                .name("Name")
                .field("host", "myProductionHost")
                .field("size", 0)
                .build()

        then:
        binaryTimeSeries != null
        binaryTimeSeries.getFields().size() == 6

        binaryTimeSeries.getId() == "6525-9662-2342"
        binaryTimeSeries.getStart() == Long.MIN_VALUE
        binaryTimeSeries.getEnd() == Long.MAX_VALUE
        binaryTimeSeries.getPoints() == "The-Binary-Large-Object".getBytes()
        binaryTimeSeries.getType() == "String"
        binaryTimeSeries.getName() == "Name"
        binaryTimeSeries.get("host") == "myProductionHost"
        binaryTimeSeries.get("size") == 0

    }
}
