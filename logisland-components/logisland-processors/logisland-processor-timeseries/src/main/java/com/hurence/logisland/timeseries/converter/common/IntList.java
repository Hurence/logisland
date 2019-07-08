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
package com.hurence.logisland.timeseries.converter.common;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.Arrays;

import static com.hurence.logisland.timeseries.converter.common.ListUtil.*;


/**
 * Implementation of a list with primitive ints.
 *
 * @author f.lautenschlager
 */
public class IntList {

    /**
     * Shared empty array instance used for empty instances.
     */
    private static final int[] EMPTY_ELEMENT_DATA = {};

    /**
     * Shared empty array instance used for default sized empty instances. We
     * distinguish this from EMPTY_ELEMENT_DATA to know how much to inflate when
     * first element is added.
     */
    private static final int[] DEFAULT_CAPACITY_EMPTY_ELEMENT_DATA = {};


    private int[] ints;
    private int size;

    /**
     * Constructs an empty list with the specified initial capacity.
     *
     * @param initialCapacity the initial capacity of the list
     * @throws IllegalArgumentException if the specified initial capacity
     *                                  is negative
     */
    public IntList(int initialCapacity) {
        if (initialCapacity > 0) {
            this.ints = new int[initialCapacity];
        } else if (initialCapacity == 0) {
            this.ints = EMPTY_ELEMENT_DATA;
        } else {
            throw new IllegalArgumentException("Illegal Capacity: " + initialCapacity);
        }
    }

    /**
     * Constructs an empty list with an initial capacity of ten.
     */
    public IntList() {
        this.ints = DEFAULT_CAPACITY_EMPTY_ELEMENT_DATA;
    }


    /**
     * Returns the number of elements in this list.
     *
     * @return the number of elements in this list
     */
    public int size() {
        return size;
    }

    /**
     * Returns <tt>true</tt> if this list contains no elements.
     *
     * @return <tt>true</tt> if this list contains no elements
     */
    public boolean isEmpty() {
        return size == 0;
    }

    /**
     * Returns <tt>true</tt> if this list contains the specified element.
     * More formally, returns <tt>true</tt> if and only if this list contains
     * at least one element <tt>e</tt> such that
     * <tt>(o==null&nbsp;?&nbsp;e==null&nbsp;:&nbsp;o.equals(e))</tt>.
     *
     * @param o element whose presence in this list is to be tested
     * @return <tt>true</tt> if this list contains the specified element
     */
    public boolean contains(long o) {
        return indexOf(o) >= 0;
    }

    /**
     * Returns the index of the first occurrence of the specified element
     * in this list, or -1 if this list does not contain the element.
     * More formally, returns the lowest index <tt>i</tt> such that
     * <tt>(o==null&nbsp;?&nbsp;get(i)==null&nbsp;:&nbsp;o.equals(get(i)))</tt>,
     * or -1 if there is no such index.
     *
     * @param o the long value
     * @return the index of the given long element
     */
    public int indexOf(long o) {

        for (int i = 0; i < size; i++) {
            if (o == ints[i]) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Returns the index of the last occurrence of the specified element
     * in this list, or -1 if this list does not contain the element.
     * More formally, returns the highest index <tt>i</tt> such that
     * <tt>(o==null&nbsp;?&nbsp;get(i)==null&nbsp;:&nbsp;o.equals(get(i)))</tt>,
     * or -1 if there is no such index.
     *
     * @param o the long value
     * @return the last index of the given long element
     */
    public int lastIndexOf(long o) {

        for (int i = size - 1; i >= 0; i--) {
            if (o == ints[i]) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Returns a shallow copy of this <tt>LongList</tt> instance.  (The
     * elements themselves are not copied.)
     *
     * @return a clone of this <tt>LongList</tt> instance
     */
    public IntList copy() {
        IntList v = new IntList(size);
        v.ints = Arrays.copyOf(ints, size);
        v.size = size;
        return v;
    }

    /**
     * Returns an array containing all of the elements in this list
     * in proper sequence (from first to last element).
     * <p>
     * <p>The returned array will be "safe" in that no references to it are
     * maintained by this list.  (In other words, this method must allocate
     * a new array).  The caller is thus free to modify the returned array.
     * <p>
     * <p>This method acts as bridge between array-based and collection-based
     * APIs.
     *
     * @return an array containing all of the elements in this list in
     * proper sequence
     */
    public int[] toArray() {
        return Arrays.copyOf(ints, size);
    }


    private void growIfNeeded(int newCapacity) {
        if (newCapacity != -1) {
            ints = Arrays.copyOf(ints, newCapacity);
        }
    }

    /**
     * Returns the element at the specified position in this list.
     *
     * @param index index of the element to return
     * @return the element at the specified position in this list
     * @throws IndexOutOfBoundsException
     */
    public int get(int index) {
        rangeCheck(index, size);
        return ints[index];
    }

    /**
     * Replaces the element at the specified position in this list with
     * the specified element.
     *
     * @param index   index of the element to replace
     * @param element element to be stored at the specified position
     * @return the element previously at the specified position
     * @throws IndexOutOfBoundsException
     */
    public int set(int index, int element) {
        rangeCheck(index, size);

        int oldValue = ints[index];
        ints[index] = element;
        return oldValue;
    }

    /**
     * Appends the specified element to the end of this list.
     *
     * @param e element to be appended to this list
     * @return <tt>true</tt> (as specified by Collection#add)
     */
    public boolean add(int e) {
        int newCapacity = calculateNewCapacity(ints.length, size + 1);
        growIfNeeded(newCapacity);

        ints[size++] = e;
        return true;
    }

    /**
     * Inserts the specified element at the specified position in this
     * list. Shifts the element currently at that position (if any) and
     * any subsequent elements to the right (adds one to their indices).
     *
     * @param index   index at which the specified element is to be inserted
     * @param element element to be inserted
     * @throws IndexOutOfBoundsException
     */
    public void add(int index, int element) {
        rangeCheckForAdd(index, size);

        int newCapacity = calculateNewCapacity(ints.length, size + 1);
        growIfNeeded(newCapacity);

        System.arraycopy(ints, index, ints, index + 1, size - index);
        ints[index] = element;
        size++;
    }

    /**
     * Removes the element at the specified position in this list.
     * Shifts any subsequent elements to the left (subtracts one from their
     * indices).
     *
     * @param index the index of the element to be removed
     * @return the element that was removed from the list
     * @throws IndexOutOfBoundsException
     */
    public long removeAt(int index) {
        rangeCheck(index, size);

        long oldValue = ints[index];

        int numMoved = size - index - 1;
        if (numMoved > 0) {
            System.arraycopy(ints, index + 1, ints, index, numMoved);
        }
        --size;

        return oldValue;
    }

    /**
     * Removes the first occurrence of the specified element from this list,
     * if it is present.  If the list does not contain the element, it is
     * unchanged.  More formally, removes the element with the lowest index
     * <tt>i</tt> such that
     * <tt>(o==null&nbsp;?&nbsp;get(i)==null&nbsp;:&nbsp;o.equals(get(i)))</tt>
     * (if such an element exists).  Returns <tt>true</tt> if this list
     * contained the specified element (or equivalently, if this list
     * changed as a result of the call).
     *
     * @param o element to be removed from this list, if present
     * @return <tt>true</tt> if this list contained the specified element
     */
    public boolean remove(int o) {

        for (int index = 0; index < size; index++) {
            if (o == ints[index]) {
                fastRemove(index);
                return true;
            }
        }

        return false;
    }

    private void fastRemove(int index) {
        int numMoved = size - index - 1;
        if (numMoved > 0) {
            System.arraycopy(ints, index + 1, ints, index, numMoved);
        }
        --size;
    }

    /**
     * Removes all of the elements from this list.  The list will
     * be empty after this call returns.
     */
    public void clear() {
        ints = DEFAULT_CAPACITY_EMPTY_ELEMENT_DATA;
        size = 0;
    }

    /**
     * Appends all of the elements in the specified collection to the end of
     * this list, in the order that they are returned by the
     * specified collection's Iterator.  The behavior of this operation is
     * undefined if the specified collection is modified while the operation
     * is in progress.  (This implies that the behavior of this call is
     * undefined if the specified collection is this list, and this
     * list is nonempty.)
     *
     * @param c collection containing elements to be added to this list
     * @return <tt>true</tt> if this list changed as a result of the call
     * @throws NullPointerException if the specified collection is null
     */
    public boolean addAll(IntList c) {
        int[] a = c.toArray();
        int numNew = a.length;

        int newCapacity = calculateNewCapacity(ints.length, size + numNew);
        growIfNeeded(newCapacity);

        System.arraycopy(a, 0, ints, size, numNew);
        size += numNew;
        return numNew != 0;
    }

    /**
     * Inserts all of the elements in the specified collection into this
     * list, starting at the specified position.  Shifts the element
     * currently at that position (if any) and any subsequent elements to
     * the right (increases their indices).  The new elements will appear
     * in the list in the order that they are returned by the
     * specified collection's iterator.
     *
     * @param index index at which to insert the first element from the
     *              specified collection
     * @param c     collection containing elements to be added to this list
     * @return <tt>true</tt> if this list changed as a result of the call
     * @throws IndexOutOfBoundsException
     * @throws NullPointerException      if the specified collection is null
     */
    public boolean addAll(int index, IntList c) {
        rangeCheckForAdd(index, size);

        int[] a = c.toArray();
        int numNew = a.length;

        int newCapacity = calculateNewCapacity(ints.length, size + numNew);
        growIfNeeded(newCapacity);

        int numMoved = size - index;
        if (numMoved > 0) {
            System.arraycopy(ints, index, ints, index + numNew, numMoved);
        }

        System.arraycopy(a, 0, ints, index, numNew);
        size += numNew;
        return numNew != 0;
    }

    /**
     * Removes from this list all of the elements whose index is between
     * {@code fromIndex}, inclusive, and {@code toIndex}, exclusive.
     * Shifts any succeeding elements to the left (reduces their index).
     * This call shortens the list by {@code (toIndex - fromIndex)} elements.
     * (If {@code toIndex==fromIndex}, this operation has no effect.)
     *
     * @throws IndexOutOfBoundsException if {@code fromIndex} or
     *                                   {@code toIndex} is out of range
     *                                   ({@code fromIndex < 0 ||
     *                                   fromIndex >= size() ||
     *                                   toIndex > size() ||
     *                                   toIndex < fromIndex})
     */
    public void removeRange(int fromIndex, int toIndex) {
        int numMoved = size - toIndex;
        System.arraycopy(ints, toIndex, ints, fromIndex, numMoved);

        size = size - (toIndex - fromIndex);
    }


    /**
     * Trims the capacity of this <tt>ArrayList</tt> instance to be the
     * list's current size.  An application can use this operation to minimize
     * the storage of an <tt>ArrayList</tt> instance.
     */
    private int[] trimToSize(int size, int[] elements) {
        int[] copy = Arrays.copyOf(elements, elements.length);
        if (size < elements.length) {
            copy = (size == 0) ? EMPTY_ELEMENT_DATA : Arrays.copyOf(elements, size);
        }
        return copy;
    }


    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj == this) {
            return true;
        }

        if (obj.getClass() != getClass()) {
            return false;
        }
        IntList rhs = (IntList) obj;

        int[] thisTrimmed = trimToSize(this.size, this.ints);
        int[] otherTrimmed = trimToSize(rhs.size, rhs.ints);

        return new EqualsBuilder()
                .append(thisTrimmed, otherTrimmed)
                .append(this.size, rhs.size)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder()
                .append(ints)
                .append(size)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("ints", trimToSize(this.size, ints))
                .append("size", size)
                .toString();
    }
}
