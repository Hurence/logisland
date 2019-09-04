/**
 * Copyright (C) 2016 Hurence (support@hurence.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.timeseries.converter.common;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.io.Serializable;
import java.util.Arrays;

import static com.hurence.logisland.timeseries.converter.common.ListUtil.*;

/**
 * Implementation of a list with primitive doubles.
 *
 * @author f.lautenschlager
 */
public class DoubleList implements Serializable {

    private static final long serialVersionUID = -1275724597860546074L;

    /**
     * Shared empty array instance used for empty instances.
     */
    private static final double[] EMPTY_ELEMENT_DATA = {};

    /**
     * Shared empty array instance used for default sized empty instances. We
     * distinguish this from EMPTY_ELEMENT_DATA to know how much to inflate when
     * first element is added.
     */
    private static final double[] DEFAULT_CAPACITY_EMPTY_ELEMENT_DATA = {};


    private double[] doubles;
    private int size;

    /**
     * Constructs an empty list with the specified initial capacity.
     *
     * @param initialCapacity the initial capacity of the list
     * @throws IllegalArgumentException if the specified initial capacity
     *                                  is negative
     */
    public DoubleList(int initialCapacity) {
        if (initialCapacity > 0) {
            this.doubles = new double[initialCapacity];
        } else if (initialCapacity == 0) {
            this.doubles = EMPTY_ELEMENT_DATA;
        } else {
            throw new IllegalArgumentException("Illegal Capacity: " + initialCapacity);
        }
    }

    /**
     * Constructs an empty list with an initial capacity of ten.
     */
    public DoubleList() {
        this.doubles = DEFAULT_CAPACITY_EMPTY_ELEMENT_DATA;
    }

    /**
     * Constructs a double list from the given values by simple assigning them.
     *
     * @param longs the values of the double list.
     * @param size  the index of the last value in the array.
     */
    @SuppressWarnings("all")
    public DoubleList(double[] longs, int size) {
        if (longs == null) {
            throw new IllegalArgumentException("Illegal initial array 'null'");
        }
        if (size < 0) {
            throw new IllegalArgumentException("Size if negative.");
        }

        this.doubles = longs;
        this.size = size;
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
    public boolean contains(double o) {
        return indexOf(o) >= 0;
    }

    /**
     * Returns the index of the first occurrence of the specified element
     * in this list, or -1 if this list does not contain the element.
     * More formally, returns the lowest index <tt>i</tt> such that
     * <tt>(o==null&nbsp;?&nbsp;get(i)==null&nbsp;:&nbsp;o.equals(get(i)))</tt>,
     * or -1 if there is no such index.
     *
     * @param o the double value
     * @return the index of the given double element
     */
    public int indexOf(double o) {

        for (int i = 0; i < size; i++) {
            if (o == doubles[i]) {
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
     * @param o the double value
     * @return the last index of the given double element
     */
    public int lastIndexOf(double o) {

        for (int i = size - 1; i >= 0; i--) {
            if (o == doubles[i]) {
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
    public DoubleList copy() {
        DoubleList v = new DoubleList(size);
        v.doubles = Arrays.copyOf(doubles, size);
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
    public double[] toArray() {
        return Arrays.copyOf(doubles, size);
    }


    private void growIfNeeded(int newCapacity) {
        if (newCapacity != -1) {
            doubles = Arrays.copyOf(doubles, newCapacity);
        }
    }

    /**
     * Returns the element at the specified position in this list.
     *
     * @param index index of the element to return
     * @return the element at the specified position in this list
     * @throws IndexOutOfBoundsException
     */
    public double get(int index) {
        rangeCheck(index, size);
        return doubles[index];
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
    public double set(int index, double element) {
        rangeCheck(index, size);

        double oldValue = doubles[index];
        doubles[index] = element;
        return oldValue;
    }

    /**
     * Appends the specified element to the end of this list.
     *
     * @param e element to be appended to this list
     * @return <tt>true</tt> (as specified by Collection#add)
     */
    public boolean add(double e) {
        int newCapacity = calculateNewCapacity(doubles.length, size + 1);
        growIfNeeded(newCapacity);

        doubles[size++] = e;
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
    public void add(int index, double element) {
        rangeCheckForAdd(index, size);

        int newCapacity = calculateNewCapacity(doubles.length, size + 1);
        growIfNeeded(newCapacity);

        System.arraycopy(doubles, index, doubles, index + 1, size - index);
        doubles[index] = element;
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
    public double remove(int index) {
        rangeCheck(index, size);

        double oldValue = doubles[index];

        int numMoved = size - index - 1;
        if (numMoved > 0) {
            System.arraycopy(doubles, index + 1, doubles, index, numMoved);
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
    public boolean remove(double o) {

        for (int index = 0; index < size; index++) {
            if (o == doubles[index]) {
                fastRemove(index);
                return true;
            }
        }

        return false;
    }

    private void fastRemove(int index) {
        int numMoved = size - index - 1;
        if (numMoved > 0) {
            System.arraycopy(doubles, index + 1, doubles, index, numMoved);
        }
        --size;
    }

    /**
     * Removes all of the elements from this list.  The list will
     * be empty after this call returns.
     */
    public void clear() {
        doubles = DEFAULT_CAPACITY_EMPTY_ELEMENT_DATA;
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
    public boolean addAll(DoubleList c) {
        double[] a = c.toArray();
        int numNew = a.length;

        int newCapacity = calculateNewCapacity(doubles.length, size + numNew);
        growIfNeeded(newCapacity);

        System.arraycopy(a, 0, doubles, size, numNew);
        size += numNew;
        return numNew != 0;
    }

    /**
     * Appends the long[] at the end of this long list.
     *
     * @param otherDoubles the other double[] that is appended
     * @return <tt>true</tt> if this list changed as a result of the call
     * @throws NullPointerException if the specified array is null
     */
    public boolean addAll(double[] otherDoubles) {
        int numNew = otherDoubles.length;

        int newCapacity = calculateNewCapacity(doubles.length, size + numNew);
        growIfNeeded(newCapacity);

        System.arraycopy(otherDoubles, 0, doubles, size, numNew);
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
    public boolean addAll(int index, DoubleList c) {
        rangeCheckForAdd(index, size);

        double[] a = c.toArray();
        int numNew = a.length;

        int newCapacity = calculateNewCapacity(doubles.length, size + numNew);
        growIfNeeded(newCapacity);

        int numMoved = size - index;
        if (numMoved > 0) {
            System.arraycopy(doubles, index, doubles, index + numNew, numMoved);
        }

        System.arraycopy(a, 0, doubles, index, numNew);
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
     * @param fromIndex from index
     * @param toIndex   to index
     * @throws IndexOutOfBoundsException if {@code fromIndex} or
     *                                   {@code toIndex} is out of range
     *                                   ({@code fromIndex < 0 ||
     *                                   fromIndex >= size() ||
     *                                   toIndex > size() ||
     *                                   toIndex < fromIndex})
     */
    public void removeRange(int fromIndex, int toIndex) {
        int numMoved = size - toIndex;
        System.arraycopy(doubles, toIndex, doubles, fromIndex, numMoved);

        size = size - (toIndex - fromIndex);
    }


    /**
     * Trims the capacity of this <tt>ArrayList</tt> instance to be the
     * list's current size. An application can use this operation to minimize
     * the storage of an <tt>ArrayList</tt> instance.
     *
     * @param size     current size
     * @param elements the current elements
     * @return the trimmed elements
     */
    private double[] trimToSize(int size, double[] elements) {
        double[] copy = Arrays.copyOf(elements, elements.length);
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
        DoubleList rhs = (DoubleList) obj;

        double[] thisTrimmed = trimToSize(this.size, this.doubles);
        double[] otherTrimmed = trimToSize(rhs.size, rhs.doubles);

        return new EqualsBuilder()
                .append(thisTrimmed, otherTrimmed)
                .append(this.size, rhs.size)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder()
                .append(doubles)
                .append(size)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("doubles", trimToSize(this.size, doubles))
                .append("size", size)
                .toString();
    }
}
