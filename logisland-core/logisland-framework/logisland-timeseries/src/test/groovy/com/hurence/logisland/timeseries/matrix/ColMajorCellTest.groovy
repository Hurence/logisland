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
package com.hurence.logisland.timeseries.matrix

import spock.lang.Specification

/**
 * Unit test for the ColMajorCell
 * @author f.lautenschlager
 */
class ColMajorCellTest extends Specification {

    def "test getCol and getRow"() {
        given:
        def colMajorCell = new ColMajorCell(0, 0)

        when:
        def col = colMajorCell.getCol()
        def row = colMajorCell.getRow()

        then:
        col == 0
        row == 0
    }

    def "test toString"() {
        given:
        def colMajorCell = new ColMajorCell(0, 0)

        when:
        def string = colMajorCell.toString()
        then:
        string.contains("col")
        string.contains("row")

    }

    def "test equals"() {
        given:
        def colMajorCell = new ColMajorCell(0, 0)

        when:
        def equals = colMajorCell.equals(others)
        then:
        equals == result
        colMajorCell.equals(colMajorCell)//always true

        where:
        others << [new Object(), null, new ColMajorCell(0, 0)]
        result << [false, false, true]
    }

    def "test hashCode"() {
        given:
        def colMajorCell = new ColMajorCell(0, 0)

        when:
        def hash = colMajorCell.hashCode()
        then:
        hash == new ColMajorCell(0, 0).hashCode()
        hash != new ColMajorCell(0, 1).hashCode()
    }
}
