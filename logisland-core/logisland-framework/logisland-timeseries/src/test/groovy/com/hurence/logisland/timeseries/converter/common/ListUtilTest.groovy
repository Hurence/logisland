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
 * Unit test for the list util class.
 * @author f.lautenschlager
 */
class ListUtilTest extends Specification {
    @Unroll
    def "test rangeCheck for size: #size, index: #index, exception thrown: #expected"() {
        when:
        boolean thrown = false
        try {
            ListUtil.rangeCheck(index, size)
        } catch (IndexOutOfBoundsException e) {
            thrown = true;
        }
        then:
        thrown == expected

        where:
        index << [1, 2]
        size << [2, 1]
        expected << [false, true]
    }

    @Unroll
    def "test rangeCheckAdd for size: #size, index: #index, exception thrown: #expected"() {
        when:
        boolean thrown = false
        try {
            ListUtil.rangeCheckForAdd(index, size)
        } catch (IndexOutOfBoundsException e) {
            thrown = true;
        }
        then:
        thrown == expected

        where:
        index << [1, 2, -1]
        size << [2, 1, 2]
        expected << [false, true, true]
    }

    @Unroll
    def "test calculateNewCapacity for oldCapacity: #oldCapacity with a minCapacity of: #minCapacity. Expecting: #expectedCapacity"() {
        when:
        def newCapacity = ListUtil.calculateNewCapacity(oldCapacity, minCapacity)
        then:
        newCapacity == expectedCapacity
        where:
        oldCapacity << [1, 2, Integer.MAX_VALUE - 8]
        minCapacity << [2, 1, Integer.MAX_VALUE - 7]
        expectedCapacity << [2, -1, 2147483647]
    }

    @Unroll
    def "test hugeCapacity for minCapacity: #minCapacity, expected: #expectedCapacity, exception thrown: #exception"() {
        when:
        def thrown = false
        def capacity = -1;
        try {
            capacity = ListUtil.hugeCapacity(minCapacity)
        } catch (OutOfMemoryError e) {
            thrown = true
        }

        then:
        capacity == expectedCapacity
        thrown == exception

        where:
        minCapacity << [1, -1]
        expectedCapacity << [2147483639, -1]
        exception << [false, true]
    }

    def "test private constructor"() {
        when:
        ListUtil.newInstance()
        then:
        noExceptionThrown()
    }
}
