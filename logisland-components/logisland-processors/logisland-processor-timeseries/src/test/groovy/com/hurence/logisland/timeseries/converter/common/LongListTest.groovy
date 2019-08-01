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
package com.hurence.logisland.timeseries.converter.common

import spock.lang.Specification
import spock.lang.Unroll

/**
 * Unit test for the long list
 * @author f.lautenschlager
 */
class LongListTest extends Specification {
    def "test size"() {
        given:
        def list = new LongList()

        when:
        list.add(1l)
        list.add(2l)

        list.remove(2l)
        then:
        list.size() == 1
    }

    def "test isEmpty"() {
        when:
        def list = insertedList

        then:
        list.isEmpty() == expected

        where:
        insertedList << [new LongList()]
        expected << [true]
    }

    def "test remove"() {
        given:
        def list = new LongList(20)

        when:
        list.add(1l)
        list.add(2l)
        list.add(3l)
        list.add(4l)

        list.remove(2)
        then:
        !list.isEmpty()
        list.size() == 3
        list.get(0) == 1l

    }

    @Unroll
    def "test contains value: #values expected: #expected"() {
        given:
        def list = new LongList()

        when:
        list.add(3l)
        list.add(4l)
        list.add(5l)

        then:
        def result = list.contains(values)
        result == expected

        where:
        values << [3l, 4l, 5l, 6l, 2l]
        expected << [true, true, true, false, false]
    }

    @Unroll
    def "test indexOf value: #values index: #expected"() {
        given:
        def list = new LongList()

        when:
        list.add(3l)
        list.add(4l)
        list.add(4l)
        list.add(5l)

        then:
        def result = list.indexOf(values)
        result == expected

        where:
        values << [3l, 4l, 5l, 6l]
        expected << [0, 1, 3, -1]
    }

    @Unroll
    def "test lastIndexOf value: #values index: #expected"() {
        given:
        def list = new LongList()

        when:
        list.add(3l)
        list.add(4l)
        list.add(4l)
        list.add(5l)

        then:
        def result = list.lastIndexOf(values)
        result == expected

        where:
        values << [3l, 4l, 5l, 6l]
        expected << [0, 2, 3, -1]
    }

    def "test copy"() {
        given:
        def list = new LongList()

        when:
        list.add(3l)
        list.add(4l)
        list.add(4l)
        list.add(5l)

        then:
        def result = list.copy()
        result.equals(list)

    }

    def "test toArray"() {
        given:
        def list = new LongList()

        when:
        list.add(3l)
        list.add(4l)
        list.add(4l)
        list.add(5l)

        then:
        def result = list.toArray()
        result == [3, 4, 4, 5] as long[]
    }

    def "test get"() {
        given:
        def list = new LongList()

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
        expected << [5l, 4l, 4l, 3l]

    }

    def "test set"() {
        given:
        def list = new LongList()

        when:
        list.add(3l)
        list.add(4l)
        list.add(4l)
        list.add(5l)

        then:
        def result = list.set(index, 4)
        result == expected
        list.set(index, 4) == 4

        where:
        index << [3, 2, 1, 0]
        expected << [5l, 4l, 4l, 3l]
    }


    def "test add"() {
        given:
        def list = new LongList()

        when:
        list.add(3l)
        list.add(4l)
        list.add(4l)
        list.add(5l)

        then:
        list.size() == 4
        list.contains(3l)
        list.contains(4l)
        list.contains(4l)
        list.contains(5l)
    }


    def "test add at index"() {
        given:
        def list = new LongList()

        when:
        list.add(3l)
        list.add(4l)
        list.add(4l)
        list.add(5l)

        list.add(3, 99l)

        then:
        list.size() == 5
        list.get(3) == 99l
    }

    def "test remove double"() {
        given:
        def list = new LongList()


        when:
        list.add(3l)
        list.add(4l)
        list.add(4l)
        list.add(5l)

        def firstRemove = list.remove(4l)
        def secondRemove = list.remove(1l)

        then:
        firstRemove
        !secondRemove
        list.size() == 3
        list.get(1) == 4l
    }


    def "test remove at index"() {
        given:
        def list = new LongList()

        when:
        list.add(3l)
        list.add(4l)
        list.add(4l)
        list.add(5l)

        def result = list.remove(3)

        then:
        result == 5l
        list.size() == 3
    }

    def "test clear"() {
        given:
        def list = new LongList()

        when:
        list.add(3l)
        list.add(4l)
        list.add(4l)
        list.add(5l)

        list.clear()
        then:
        list.size() == 0
    }

    def "test addAll"() {
        given:
        def list1 = new LongList()
        def list2 = new LongList()

        when:
        list1.add(1l)
        list1.add(2l)
        list1.add(3l)


        list2.add(3l)
        list2.add(4l)
        list2.add(4l)
        list2.add(5l)

        def result = list1.addAll(list2)
        then:
        result
        list1.size() == 7
        list1.contains(3l)
        list1.contains(4l)
        list1.contains(5l)

    }

    def "test addAll with empty list"() {
        given:
        def list1 = new LongList()
        def list2 = new LongList()

        when:
        list1.add(1l)
        list1.add(2l)
        list1.add(3l)


        def result = list1.addAll(list2)
        then:
        !result
        list1.size() == 3
        list1.contains(1l)
        list1.contains(2l)
        list1.contains(3l)
    }

    def "test addAll with array"() {
        given:
        def list1 = new LongList()
        def list2 = [4l, 5l, 6l] as long[]

        when:
        list1.add(1l)
        list1.add(2l)
        list1.add(3l)


        def result = list1.addAll(list2)
        then:
        result
        list1.size() == 6
        list1.contains(1l)
        list1.contains(2l)
        list1.contains(3l)
        list1.contains(4l)
        list1.contains(5l)
        list1.contains(6l)
    }


    def "test addAll at index"() {
        given:
        def list1 = new LongList(20)
        def list2 = new LongList(20)

        when:
        list1.add(1l)
        list1.add(2l)
        list1.add(3l)


        list2.add(3l)
        list2.add(4l)
        list2.add(4l)
        list2.add(5l)

        list2.remove(1)

        def result = list1.addAll(3, list2)
        then:
        result
        list1.size() == 6
        list1.get(0) == 1l
        list1.get(1) == 2l
        list1.get(2) == 3l
        list1.get(3) == 3l
        list1.get(4) == 4l
        list1.get(5) == 5l
    }

    def "test addAll of empty list at index"() {
        given:
        def list1 = new LongList(20)
        def list2 = new LongList(20)
        when:
        list1.add(1l)
        list1.add(2l)
        list1.add(3l)

        def result = list1.addAll(0, list2)
        then:
        !result
        list1.size() == 3

    }

    def "test removeRange"() {
        given:
        def list = new LongList()

        when:
        list.add(1l)
        list.add(2l)
        list.add(3l)
        list.add(3l)
        list.add(4l)
        list.add(4l)
        list.add(5l)


        list.removeRange(3, 6)

        then:
        list.size() == 4
        !list.contains(4l)
    }

    def "test hash code"() {
        given:
        def list = new LongList()
        def list2 = new LongList()

        when:
        list.add(1l)
        list.add(2l)
        list.add(3l)

        list2.add(1l)
        list2.add(2l)
        list2.add(3l)


        then:
        list.hashCode() == list2.hashCode()

    }

    def "test to string"() {
        given:
        def list = new LongList()
        list.add(1l)

        when:
        def toString = list.toString()

        then:
        toString != null
        toString.size() > 0
    }

    def "test equals"() {
        given:
        def list = new LongList()
        list.add(1l)
        list.add(2l)
        list.add(3l)

        list.remove(1)

        when:
        def result = list.equals(other)
        def alwaysTrue = list.equals(list)

        then:
        alwaysTrue
        result == expected

        where:
        other << [null, new Integer(1), new LongList(20), otherList()]
        expected << [false, false, false, true]
    }

    def otherList() {
        def list = new LongList(50)
        list.add(1l)
        list.add(2l)
        list.add(3l)

        list.remove(1)

        return list
    }

    def "test constructor"() {

        when:
        def list = new LongList(initialSize)

        then:
        list.longs.size() == initialSize

        where:
        initialSize << [5, 0]
    }

    def "test invalid initial size"() {
        when:
        new LongList(-1)
        then:
        thrown IllegalArgumentException
    }

    def "test index out of bound exceptions"() {

        when:
        c1.call()

        then:
        thrown IndexOutOfBoundsException

        where:
        c1 << [{ new LongList().get(0) } as Closure, { new LongList().add(-1, 0) } as Closure]

    }

    def "test grow"() {
        given:
        def list = new LongList(2)

        when:
        list.add(1l)
        list.add(2l)
        list.add(3l)

        then:
        list.size() == 3
    }

    def "test array constructor"() {
        given:
        def list = new LongList(initialArray, lastValidValue)

        when:
        list.add(765l)

        then:
        list.size() == size

        where:
        initialArray << [[1, 2, 3] as long[], [] as long[]]
        lastValidValue << [3, 0]
        size << [4, 1]
    }

    def "test array constructor null array"() {
        when:
        new LongList(null, 0)
        then:
        thrown IllegalArgumentException
    }

    def "test array constructor negative size"() {
        when:
        new LongList([] as long[], -1)
        then:
        thrown IllegalArgumentException
    }
}
