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
package com.hurence.logisland.timeseries.converter.common;

/**
 * A class with common used list functions like grow or rangeCheck.
 *
 * @author f.lautenschlager
 */
public final class ListUtil {

    private ListUtil() {
        //private constructor
    }

    /**
     * The maximum size of array to allocate.
     * Some VMs reserve some header words in an array.
     * Attempts to allocate larger arrays may result in
     * OutOfMemoryError: Requested array size exceeds VM limit
     */
    public static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;


    /**
     * Checks if the given index is in range.  If not, throws an appropriate
     * runtime exception.  This method does *not* check if the index is
     * negative: It is always used immediately prior to an array access,
     * which throws an ArrayIndexOutOfBoundsException if index is negative.
     *
     * @param index to access an element
     * @param size  the size of the elements
     */
    public static void rangeCheck(int index, int size) {
        if (index >= size) {
            throw new IndexOutOfBoundsException(outOfBoundsMsg(index, size));
        }
    }


    /**
     * A version of rangeCheck used by add and addAll.
     *
     * @param index the accessed index
     * @param size  the size of the container
     */
    public static void rangeCheckForAdd(int index, int size) {
        if (index > size || index < 0) {
            throw new IndexOutOfBoundsException(outOfBoundsMsg(index, size));
        }
    }


    /**
     * Calculates a new  capacity based on the old capacity and th min capacity
     *
     * @param oldCapacity the old capacity of the container
     * @param minCapacity the min capacity of the container
     * @return the new calculated capacity
     */
    public static int calculateNewCapacity(int oldCapacity, int minCapacity) {
        if (minCapacity - oldCapacity > 0) {
            int newCapacity = oldCapacity + (oldCapacity >> 1);
            if (newCapacity - minCapacity < 0) {
                newCapacity = minCapacity;
            }
            if (newCapacity - MAX_ARRAY_SIZE > 0) {
                newCapacity = hugeCapacity(minCapacity);
            }
            return newCapacity;
        }
        return -1;
    }


    /**
     * @param minCapacity the minimum capacity
     * @return the maximum size
     */
    public static int hugeCapacity(int minCapacity) {
        if (minCapacity < 0) {
            // overflow
            throw new OutOfMemoryError();
        }
        return (minCapacity > MAX_ARRAY_SIZE) ? Integer.MAX_VALUE : MAX_ARRAY_SIZE;
    }


    /**
     * Constructs an IndexOutOfBoundsException detail message.
     * Of the many possible refactorings of the error handling code,
     * this "outlining" performs best with both server and client VMs.
     */
    private static String outOfBoundsMsg(int index, int size) {
        return "Index: " + index + ", Size: " + size;
    }
}
