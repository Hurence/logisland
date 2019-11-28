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
package com.hurence.logisland.timeseries.distance

import spock.lang.Specification

/**
 * Unit test for the euclidean distance function
 * @author f.lautenschlager
 */
class EuclideanDistanceTest extends Specification {
    def "test calcDistance"() {
        when:
        def result = new EuclideanDistance().calcDistance(firstValues, secondValues)

        then:
        result == expectedResult

        where:
        expectedResult << [2.23606797749979d, 0d]
        firstValues << [[2.0d, 4.5d] as double[], [1.0d, 2d] as double[]]
        secondValues << [[3.0d, 6.5d] as double[], [1.0d, 2d] as double[]]
    }
}
