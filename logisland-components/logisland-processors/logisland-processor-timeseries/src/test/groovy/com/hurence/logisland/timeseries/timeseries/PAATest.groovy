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
package com.hurence.logisland.timeseries.timeseries

import com.hurence.logisland.timeseries.MultivariateTimeSeries
import com.hurence.logisland.timeseries.PAA
import spock.lang.Specification

/**
 * Unit test for the PAA class
 * @author f.lautenschlager
 */
class PAATest extends Specification {

    def "test convert multivariate time series to paa representation"() {
        given:
        def originalTimeSeries = new MultivariateTimeSeries(1);
        originalTimeSeries.add(1, [1d] as double[])
        originalTimeSeries.add(2, [2d] as double[])
        originalTimeSeries.add(3, [3d] as double[])
        originalTimeSeries.add(4, [4d] as double[])

        when:
        def paa = new PAA(originalTimeSeries, 2)

        then:
        paa.size() == 2
        paa.originalSize() == 4
        paa.aggregatePtSize(1) == 2
    }
}
