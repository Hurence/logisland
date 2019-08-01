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
package com.hurence.logisland.timeseries.dtw

import spock.lang.Specification

/**
 * Unit test for the warp path
 * @author f.lautenschlager
 */
class WarpPathTest extends Specification {
    def "test size"() {
        given:
        def warpPath = new WarpPath(10)

        when:
        warpPath.add(1, 1)
        warpPath.add(2, 4)
        warpPath.add(3, 8)

        def element = warpPath.get(1)

        then:
        warpPath.maxI() == 1
        warpPath.maxJ() == 1
        warpPath.minI() == 3
        warpPath.minJ() == 8
        warpPath.size() == 3

        element.col == 2
        element.row == 4
    }


    def "test toString"() {
        given:
        def warpPath = new WarpPath(10)
        when:
        def string = warpPath.toString()
        then:
        !string.isEmpty()
    }

    def "test equals"() {
        given:
        def warpPath = new WarpPath(10)

        when:
        def equals = warpPath.equals(others)
        then:
        equals == result
        warpPath.equals(warpPath)//always true

        where:
        others << [new Object(), null, new WarpPath(10)]
        result << [false, false, true]
    }

    def "test hashCode"() {
        given:
        def colMajorCell = new WarpPath(10)
        colMajorCell.add(1, 1)

        when:
        def hash = colMajorCell.hashCode()
        then:
        def other = new WarpPath(10)
        other.add(1, 1)

        hash == other.hashCode()
        hash != new WarpPath(10).hashCode()
    }
}
