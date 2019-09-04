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
 * The long list implementation contains primitive longs and acts like an array list.
 * Parts are copied from array list.
 *
 * @author f.lautenschlager
 */
public class LongList implements Serializable {

    private static final long serialVersionUID = -8791366160708918410L;

    /**
     * Shared empty array instance used for empty instances.
     */
    private static final long[] EMPTY_ELEMENT_DATA = {};

    /**
     * Shared empty array instance used for default sized empty instances. We
     * distinguish this from EMPTY_ELEMENT_DATA to know how much to inflate when
     * first element is added.
     */
    private static final long[] DEFAULT_CAPACITY_EMPTY_ELEMENT_DATA = {};

    private long[] longs;
    private int size;

    /**
     * Constructs an empty list with the specified initial capacity.
     *
     * @param initialCapacity the initial capacity of the list
     * @throws IllegalArgumentException if the specified initial capacity
     *                                  is negative
     */
    public LongList(int initialCapacity) {
        if (initialCapacity > 0) {
            this.longs = new long[initialCapacity];
        } else if (initialCapacity == 0) {
            this.longs = EMPTY_ELEMENT_DATA;
        } else {
            throw new IllegalArgumentException("Illegal Capacity: " +
                    initialCapacity);
        }
    }

    /**
     * Constructs an empty list with an initial capacity of ten.
     */
    public LongList() {
        this.longs = DEFAULT_CAPACITY_EMPTY_ELEMENT_DATA;
    }

    /**
     * Constructs a long list from the given longs by simple assigning them.
     *
     * @param longs the values of the double list.
     * @param size  the index of the last value in the array.
     */
    @SuppressWarnings("all")
    public LongList(long[] longs, int size) {
        if (longs == null) {
            throw new IllegalArgumentException("Illegal initial array 'null'");
        }
        if (size < 0) {
            throw new IllegalArgumentException("Size if negative.");
        }
        this.longs = longs;
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
    public boolean contains(Long o) {
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
     * @return the index of the element or -1
     */
    public int indexOf(long o) {
        for (int i = 0; i < size; i++) {
            if (o == longs[i]) {
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
     * @return the last index of the element or -1
     */
    public int lastIndexOf(long o) {
        for (int i = size - 1; i >= 0; i--) {
            if (o == longs[i]) {
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
    public LongList copy() {
        LongList v = new LongList(size);
        v.longs = Arrays.copyOf(longs, size);
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
    public long[] toArray() {
        return Arrays.copyOf(longs, size);
    }

    /**
     * Returns the element at the specified position in this list.
     *
     * @param index index of the element to return
     * @return the element at the specified position in this list
     * @throws IndexOutOfBoundsException
     */
    public long get(int index) {
        rangeCheck(index, size);
        return longs[index];
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
    public long set(int index, long element) {
        rangeCheck(index, size);

        long oldValue = longs[index];
        longs[index] = element;
        return oldValue;
    }

    /**
     * Appends the specified element to the end of this list.
     *
     * @param e element to be appended to this list
     * @return <tt>true</tt> (as specified by Collection#add)
     */
    public boolean add(long e) {
        int newCapacity = calculateNewCapacity(longs.length, size + 1);
        growIfNeeded(newCapacity);

        longs[size++] = e;
        return true;
    }

    private void growIfNeeded(int newCapacity) {
        if (newCapacity != -1) {
            longs = Arrays.copyOf(longs, newCapacity);
        }
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
    public void add(int index, long element) {
        rangeCheckForAdd(index, size);

        int newCapacity = calculateNewCapacity(longs.length, size + 1);
        growIfNeeded(newCapacity);

        System.arraycopy(longs, index, longs, index + 1, size - index);
        longs[index] = element;
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
    public long remove(int index) {
        rangeCheck(index, size);

        long oldValue = longs[index];

        int numMoved = size - index - 1;
        if (numMoved > 0) {
            System.arraycopy(longs, index + 1, longs, index, numMoved);
        }
        --size;
        //we do not override the value

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
    public boolean remove(long o) {

        for (int index = 0; index < size; index++) {
            if (o == longs[index]) {
                fastRemove(index);
                return true;
            }
        }

        return false;
    }

    private void fastRemove(int index) {
        int numMoved = size - index - 1;
        if (numMoved > 0) {
            System.arraycopy(longs, index + 1, longs, index, numMoved);
        }
        --size;
        //we do not override the value.
    }

    /**
     * Removes all of the elements from this list.  The list will
     * be empty after this call returns.
     */
    public void clear() {
        longs = DEFAULT_CAPACITY_EMPTY_ELEMENT_DATA;
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
    public boolean addAll(LongList c) {
        long[] a = c.toArray();
        int numNew = a.length;
        int newCapacity = calculateNewCapacity(longs.length, size + numNew);
        growIfNeeded(newCapacity);

        System.arraycopy(a, 0, longs, size, numNew);
        size += numNew;
        return numNew != 0;
    }

    /**
     * Appends the long[] at the end of this long list.
     *
     * @param otherLongs the other long[] that is appended
     * @return <tt>true</tt> if this list changed as a result of the call
     * @throws NullPointerException if the specified array is null
     */
    public boolean addAll(long[] otherLongs) {
        int numNew = otherLongs.length;

        int newCapacity = calculateNewCapacity(longs.length, size + numNew);
        growIfNeeded(newCapacity);

        System.arraycopy(otherLongs, 0, longs, size, numNew);
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
    public boolean addAll(int index, LongList c) {
        rangeCheckForAdd(index, size);

        long[] a = c.toArray();
        int numNew = a.length;

        int newCapacity = calculateNewCapacity(longs.length, size + numNew);
        growIfNeeded(newCapacity);

        int numMoved = size - index;
        if (numMoved > 0) {
            System.arraycopy(longs, index, longs, index + numNew, numMoved);
        }

        System.arraycopy(a, 0, longs, index, numNew);
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
        System.arraycopy(longs, toIndex, longs, fromIndex, numMoved);
        size = size - (toIndex - fromIndex);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("longs", trimToSize(this.size, longs))
                .append("size", size)
                .toString();
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
        LongList rhs = (LongList) obj;

        long[] thisTrimmed = trimToSize(this.size, this.longs);
        long[] otherTrimmed = trimToSize(rhs.size, rhs.longs);

        return new EqualsBuilder()
                .append(thisTrimmed, otherTrimmed)
                .append(this.size, rhs.size)
                .isEquals();
    }

    /**
     * Trims the capacity of this <tt>ArrayList</tt> instance to be the
     * list's current size.  An application can use this operation to minimize
     * the storage of an <tt>ArrayList</tt> instance.
     */
    private long[] trimToSize(int size, long[] elements) {
        long[] copy = Arrays.copyOf(elements, elements.length);
        if (size < elements.length) {
            copy = (size == 0) ? EMPTY_ELEMENT_DATA : Arrays.copyOf(elements, size);
        }
        return copy;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder()
                .append(longs)
                .append(size)
                .toHashCode();
    }
}
