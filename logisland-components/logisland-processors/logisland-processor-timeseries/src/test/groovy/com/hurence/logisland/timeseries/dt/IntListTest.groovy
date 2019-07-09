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
package com.hurence.logisland.timeseries.dt

import com.hurence.logisland.timeseries.converter.common.IntList
import spock.lang.Specification
import spock.lang.Unroll

/**
 * Unit test for our IntList
 * @author f.lautenschlager
 */
class IntListTest extends Specification {
    def "test size"() {
        given:
        def list = new IntList()

        when:
        list.add(1)
        list.add(2)

        list.remove(2)
        then:
        list.size() == 1
    }

    def "test isEmpty"() {
        when:
        def list = insertedList

        then:
        list.isEmpty() == expected

        where:
        insertedList << [new IntList()]
        expected << [true]
    }

    def "test remove"() {
        given:
        def list = new IntList(20)

        when:
        list.add(1)
        list.add(2)
        list.add(3)
        list.add(4)

        list.remove(2)
        then:
        !list.isEmpty()
        list.size() == 3
        list.get(0) == 1

    }

    @Unroll
    def "test contains value: #values expected: #expected"() {
        given:
        def list = new IntList()

        when:
        list.add(3)
        list.add(4)
        list.add(5)

        then:
        def result = list.contains(values)
        result == expected

        where:
        values << [3, 4, 5, 6, 2]
        expected << [true, true, true, false, false]
    }

    @Unroll
    def "test indexOf value: #values index: #expected"() {
        given:
        def list = new IntList()

        when:
        list.add(3)
        list.add(4)
        list.add(4)
        list.add(5)

        then:
        def result = list.indexOf(values)
        result == expected

        where:
        values << [3, 4, 5, 6]
        expected << [0, 1, 3, -1]
    }

    @Unroll
    def "test lastIndexOf value: #values index: #expected"() {
        given:
        def list = new IntList()

        when:
        list.add(3)
        list.add(4)
        list.add(4)
        list.add(5)

        then:
        def result = list.lastIndexOf(values)
        result == expected

        where:
        values << [3, 4, 5, 6]
        expected << [0, 2, 3, -1]
    }

    def "test copy"() {
        given:
        def list = new IntList()

        when:
        list.add(3)
        list.add(4)
        list.add(4)
        list.add(5)

        then:
        def result = list.copy()
        result.equals(list)

    }

    def "test toArray"() {
        given:
        def list = new IntList()

        when:
        list.add(3)
        list.add(4)
        list.add(4)
        list.add(5)

        then:
        def result = list.toArray()
        result == [3, 4, 4, 5] as double[]
    }

    def "test get"() {
        given:
        def list = new IntList()

        when:
        list.add(3)
        list.add(4)
        list.add(4)
        list.add(5)

        then:
        def result = list.get(index)
        result == expected

        where:
        index << [3, 2, 1, 0]
        expected << [5, 4, 4, 3]

    }

    def "test set"() {
        given:
        def list = new IntList()

        when:
        list.add(3)
        list.add(4)
        list.add(4)
        list.add(5)

        then:
        def result = list.set(index, 4)
        result == expected
        list.set(index, 4) == 4

        where:
        index << [3, 2, 1, 0]
        expected << [5, 4, 4, 3]
    }


    def "test add"() {
        given:
        def list = new IntList()

        when:
        list.add(3)
        list.add(4)
        list.add(4)
        list.add(5)

        then:
        list.size() == 4
        list.contains(3)
        list.contains(4)
        list.contains(4)
        list.contains(5)
    }


    def "test add at index"() {
        given:
        def list = new IntList()

        when:
        list.add(3)
        list.add(4)
        list.add(4)
        list.add(5)

        list.add(3, 99)

        then:
        list.size() == 5
        list.get(3) == 99
    }

    def "test remove int"() {
        given:
        def list = new IntList()


        when:
        list.add(3)
        list.add(4)
        list.add(4)
        list.add(5)

        def firstRemove = list.remove(4)
        def secondRemove = list.remove(1)

        then:
        firstRemove
        !secondRemove
        list.size() == 3
        list.get(1) == 4
    }


    def "test remove at index"() {
        given:
        def list = new IntList()

        when:
        list.add(3)
        list.add(4)
        list.add(4)
        list.add(5)

        def result = list.removeAt(3)

        then:
        result == 5
        list.size() == 3
    }

    def "test clear"() {
        given:
        def list = new IntList()

        when:
        list.add(3)
        list.add(4)
        list.add(4)
        list.add(5)

        list.clear()
        then:
        list.size() == 0
    }

    def "test addAll"() {
        given:
        def list1 = new IntList()
        def list2 = new IntList()

        when:
        list1.add(1)
        list1.add(2)
        list1.add(3)

        list2.add(3)
        list2.add(4)
        list2.add(4)
        list2.add(5)

        def result = list1.addAll(list2)
        then:
        result
        list1.size() == 7
        list1.contains(3)
        list1.contains(4)
        list1.contains(5)

    }

    def "test addAll with empty list"() {
        given:
        def list1 = new IntList()
        def list2 = new IntList()

        when:
        list1.add(1)
        list1.add(2)
        list1.add(3)


        def result = list1.addAll(list2)
        then:
        !result
        list1.size() == 3
        list1.contains(1)
        list1.contains(2)
        list1.contains(3)
    }


    def "test addAll at index"() {
        given:
        def list1 = new IntList()
        def list2 = new IntList()

        when:
        list1.add(1)
        list1.add(2)
        list1.add(3)


        list2.add(3)
        list2.add(4)
        list2.add(4)
        list2.add(5)

        list2.removeAt(1)


        def result = list1.addAll(3, list2)
        then:
        result
        list1.size() == 6
        list1.get(0) == 1
        list1.get(1) == 2
        list1.get(2) == 3
        list1.get(3) == 3
        list1.get(4) == 4
        list1.get(5) == 5

    }

    def "test addAll of empty list at index"() {
        given:
        def list1 = new IntList(20)
        def list2 = new IntList(20)
        when:
        list1.add(1)
        list1.add(2)
        list1.add(3)

        def result = list1.addAll(0, list2)
        then:
        !result
        list1.size() == 3

    }

    def "test removeRange"() {
        given:
        def list = new IntList()

        when:
        list.add(1)
        list.add(2)
        list.add(3)
        list.add(3)
        list.add(4)
        list.add(4)
        list.add(5)


        list.removeRange(3, 6)

        then:
        list.size() == 4
        !list.contains(4)
    }

    def "test hash code"() {
        given:
        def list = new IntList()
        def list2 = new IntList()

        when:
        list.add(1)
        list.add(2)
        list.add(3)

        list2.add(1)
        list2.add(2)
        list2.add(3)


        then:
        list.hashCode() == list2.hashCode()

    }

    def "test to string"() {
        given:
        def list = new IntList()
        list.add(1)

        when:
        def toString = list.toString()

        then:
        toString != null
        toString.size() > 0
    }

    def "test equals"() {
        given:
        def list = new IntList()
        list.add(1)
        list.add(2)
        list.add(3)

        list.remove(1)

        when:
        def result = list.equals(other)
        def alwaysTrue = list.equals(list)

        then:
        alwaysTrue
        result == expected

        where:
        other << [null, new Integer(1), new IntList(20), otherList()]
        expected << [false, false, false, true]
    }

    def otherList() {
        def list = new IntList(50)
        list.add(1)
        list.add(2)
        list.add(3)

        list.remove(1)

        return list
    }

    def "test constructor"() {

        when:
        def list = new IntList(initialSize)

        then:
        list.ints.size() == initialSize

        where:
        initialSize << [5, 0]
    }

    def "test invalid initial size"() {
        when:
        new IntList(-1)
        then:
        thrown IllegalArgumentException
    }

    def "test index out of bound exceptions"() {

        when:
        c1.call()

        then:
        thrown IndexOutOfBoundsException

        where:
        c1 << [{ new IntList().get(0) } as Closure, { new IntList().add(-1, 0) } as Closure]

    }

    def "test grow"() {
        given:
        def list = new IntList(2)


        when:
        list.add(1)
        list.add(2)
        list.add(3)


        then:
        list.size() == 3
    }


}
