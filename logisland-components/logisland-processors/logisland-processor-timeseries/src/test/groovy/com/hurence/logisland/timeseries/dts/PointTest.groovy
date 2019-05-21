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
package com.hurence.logisland.timeseries.dts

import spock.lang.Specification

/**
 * Unit test for the point class
 */
class PointTest extends Specification {

    def "test point"() {
        when:
        def pair = new Point(0 as int, 1 as long, 2 as double)

        then:
        pair.index == 0
        pair.timestamp == 1l
        pair.value == 2d
    }

    def "test equals"() {
        when:
        def pair = new Point(0i, 1l, 2d)

        then:
        pair.equals(pair)
        pair.equals(other) == result

        where:
        other << [null, new Object(), new Point(1i, 1l, 2d), new Point(0i, 2l, 2d), new Point(0i, 1l, 3d), new Point(0i, 1l, 2d)]
        result << [false, false, false, false, false, true]
    }

    def "test hashCode"() {
        when:
        def pair = new Point(0i, 1l, 2d)

        then:
        (pair.hashCode() == other.hashCode()) == result


        where:
        other << [new Object(), new Point(1i, 1l, 2d), new Point(0i, 2l, 2d), new Point(0i, 1l, 3d), new Point(0i, 1l, 2d)]
        result << [false, false, false, false, true]
    }

    def "test to string"() {
        expect:
        new Point(0i, 1l, 2d).toString()
    }

}
