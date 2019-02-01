/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.std;

import com.questdb.std.str.CharSink;

import java.util.Arrays;

public class LongList implements Mutable {
    private static final int DEFAULT_ARRAY_SIZE = 16;
    private static final long DEFAULT_NO_ENTRY_VALUE = -1;
    /**
     * The maximum number of runs in merge sort.
     */
    private static final int MAX_RUN_COUNT = 67;
    /**
     * The maximum length of run in merge sort.
     */
    private static final int MAX_RUN_LENGTH = 33;
    /**
     * If the length of an array to be sorted is less than this
     * constant, Quicksort is used in preference to merge sort.
     */
    private static final int QUICKSORT_THRESHOLD = 286;
    /**
     * If the length of an array to be sorted is less than this
     * constant, insertion sort is used in preference to Quicksort.
     */
    private static final int INSERTION_SORT_THRESHOLD = 47;
    private final long noEntryValue;
    private long[] buffer;
    private int pos = 0;

    public LongList() {
        this(DEFAULT_ARRAY_SIZE);
    }

    public LongList(int capacity) {
        this(capacity, DEFAULT_NO_ENTRY_VALUE);
    }

    public LongList(int capacity, long noEntryValue) {
        this.buffer = new long[capacity];
        this.noEntryValue = noEntryValue;
    }

    public LongList(LongList other) {
        this.buffer = new long[other.size() < DEFAULT_ARRAY_SIZE ? DEFAULT_ARRAY_SIZE : other.size()];
        setPos(other.size());
        System.arraycopy(other.buffer, 0, this.buffer, 0, pos);
        this.noEntryValue = other.noEntryValue;
    }

    public void add(long value) {
        ensureCapacity(pos + 1);
        Unsafe.arrayPut(buffer, pos++, value);
    }

    public void add(LongList that) {
        int p = pos;
        int s = that.size();
        ensureCapacity(p + s);
        System.arraycopy(that.buffer, 0, this.buffer, p, s);
        pos += s;
    }

    public void add(int index, long element) {
        ensureCapacity(++pos);
        System.arraycopy(buffer, index, buffer, index + 1, pos - index - 1);
        Unsafe.arrayPut(buffer, index, element);
    }

    public void arrayCopy(int srcPos, int dstPos, int length) {
        System.arraycopy(buffer, srcPos, buffer, dstPos, length);
    }

    public int binarySearch(long v) {
        int low = 0;
        int high = pos;

        while (low < high) {

            if (high - low < 65) {
                return scanSearch(v);
            }

            int mid = (low + high - 1) >>> 1;
            long midVal = Unsafe.arrayGet(buffer, mid);

            if (midVal < v)
                low = mid + 1;
            else if (midVal > v)
                high = mid;
            else
                return mid;
        }
        return -(low + 1);
    }

    public void clear() {
        pos = 0;
    }

    public void ensureCapacity(int capacity) {
        if (capacity < 0) {
            throw new IllegalArgumentException("Negative capacity. Integer overflow may be?");
        }

        int l = buffer.length;
        if (capacity > l) {
            int newCap = Math.max(l << 1, capacity);
            long[] buf = new long[newCap];
            System.arraycopy(buffer, 0, buf, 0, l);
            this.buffer = buf;
        }
    }

    public void erase() {
        pos = 0;
        Arrays.fill(buffer, noEntryValue);
    }

    public void extendAndSet(int index, long value) {
        ensureCapacity(index + 1);
        if (index >= pos) {
            pos = index + 1;
        }
        Unsafe.arrayPut(buffer, index, value);
    }

    public void fill(int from, int to, long value) {
        Arrays.fill(buffer, from, to, value);
    }

    public long get(int index) {
        if (index < pos) {
            return Unsafe.arrayGet(buffer, index);
        }
        throw new ArrayIndexOutOfBoundsException(index);
    }

    public long getAndSetQuick(int index, long value) {
        long v = getQuick(index);
        Unsafe.arrayPut(buffer, index, value);
        return v;
    }

    /**
     * Returns last element of the list or null if list is empty.
     *
     * @return last element of the list
     */
    public long getLast() {
        if (pos > 0) {
            return Unsafe.arrayGet(buffer, pos - 1);
        }
        return noEntryValue;
    }

    /**
     * Returns element at the specified position. This method does not do
     * bounds check and may cause memory corruption if index is out of bounds.
     * Instead the responsibility to check bounds is placed on application code,
     * which is often the case anyway, for example in indexed for() loop.
     *
     * @param index of the element
     * @return element at the specified position.
     */
    public long getQuick(int index) {
        return Unsafe.arrayGet(buffer, index);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        long hashCode = 1;
        for (int i = 0, n = pos; i < n; i++) {
            long v = getQuick(i);
            hashCode = 31 * hashCode + (v == noEntryValue ? 0 : v);
        }
        return (int) hashCode;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object that) {
        return this == that || that instanceof LongList && equals((LongList) that);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        CharSink toStringBuilder = Misc.getThreadLocalBuilder();

        toStringBuilder.put('[');
        for (int i = 0, k = size(); i < k; i++) {
            if (i > 0) {
                toStringBuilder.put(',');
            }
            toStringBuilder.put(get(i));
        }
        toStringBuilder.put(']');
        return toStringBuilder.toString();
    }

    public void increment(int index) {
        Unsafe.arrayPut(buffer, index, Unsafe.arrayGet(buffer, index) + 1);
    }

    public boolean remove(long v) {
        int index = indexOf(v);
        if (index > -1) {
            removeIndex(index);
            return true;
        }
        return false;
    }

    public void removeIndex(int index) {
        if (pos < 1 || index >= pos) {
            return;
        }
        int move = pos - index - 1;
        if (move > 0) {
            System.arraycopy(buffer, index + 1, buffer, index, move);
        }
        Unsafe.arrayPut(buffer, --pos, noEntryValue);
    }

    public void seed(int capacity, long value) {
        ensureCapacity(capacity);
        pos = capacity;
        fill(0, capacity, value);
    }

    public void seed(int fromIndex, int count, long value) {
        int capacity = fromIndex + count;
        ensureCapacity(capacity);
        pos = capacity;
        Arrays.fill(buffer, fromIndex, capacity, value);
    }

    public void set(int index, long element) {
        if (index < pos) {
            Unsafe.arrayPut(buffer, index, element);
            return;
        }
        throw new ArrayIndexOutOfBoundsException(index);
    }

    final public void setPos(int pos) {
        ensureCapacity(pos);
        this.pos = pos;
    }

    public void setQuick(int index, long value) {
        Unsafe.arrayPut(buffer, index, value);
    }

    public void shuffle(Rnd rnd) {
        for (int i = 0, sz = size(); i < sz; i++) {
            swap(i, rnd.nextPositiveInt() & (sz - 1));
        }
    }

    public int size() {
        return pos;
    }

    /**
     * Sorts the specified array.
     */
    public void sort() {
        sort(0, size() - 1);
    }

    public LongList subset(int lo, int hi) {
        int _hi = hi > pos ? pos : hi;
        LongList that = new LongList(_hi - lo);
        System.arraycopy(this.buffer, lo, that.buffer, 0, _hi - lo);
        that.pos = _hi - lo;
        return that;
    }

    public void zero(int value) {
        Arrays.fill(buffer, 0, pos, value);
    }

    private boolean equals(LongList that) {
        if (this.pos == that.pos) {
            for (int i = 0, n = pos; i < n; i++) {
                long lhs = this.getQuick(i);
                if (lhs == noEntryValue) {
                    return that.getQuick(i) == noEntryValue;
                } else if (lhs == that.getQuick(i)) {
                    return true;
                }
            }
        }
        return false;
    }

    private int indexOf(long o) {
        for (int i = 0, n = pos; i < n; i++) {
            if (o == getQuick(i)) {
                return i;
            }
        }
        return -1;
    }

    private void let(int a, int b) {
        Unsafe.arrayPut(buffer, a, Unsafe.arrayGet(buffer, b));
    }

    private int scanSearch(long v) {
        int sz = size();
        for (int i = 0; i < sz; i++) {
            long f = getQuick(i);
            if (f == v) {
                return i;
            }
            if (f > v) {
                return -(i + 1);
            }
        }
        return -(sz + 1);
    }

    /**
     * Sorts the specified range of the array.
     *
     * @param left  the index of the first element, inclusive, to be sorted
     * @param right the index of the last element, inclusive, to be sorted
     */
    private void sort(int left, int right) {
        // Use Quicksort on small arrays
        if (right - left < QUICKSORT_THRESHOLD) {
            sort(left, right, true);
            return;
        }

        /*
         * Index run[i] is the start of i-th run
         * (ascending or descending sequence).
         */
        int[] run = new int[MAX_RUN_COUNT + 1];
        int count = 0;
        run[0] = left;

        // Check if the array is nearly sorted
        for (int k = left; k < right; run[count] = k) {
            if (getQuick(k) < getQuick(k + 1)) { // ascending
                //noinspection StatementWithEmptyBody
                while (++k <= right && getQuick(k - 1) <= getQuick(k)) ;
            } else if (getQuick(k) > getQuick(k + 1)) { // descending
                //noinspection StatementWithEmptyBody
                while (++k <= right && getQuick(k - 1) >= getQuick(k)) ;
                for (int lo = run[count] - 1, hi = k; ++lo < --hi; ) {
                    swap(lo, hi);
                }
            } else { // equal
                for (int m = MAX_RUN_LENGTH; ++k <= right && getQuick(k - 1) == getQuick(k); ) {
                    if (--m == 0) {
                        sort(left, right, true);
                        return;
                    }
                }
            }

            /*
             * The array is not highly structured,
             * use Quicksort instead of merge sort.
             */
            if (++count == MAX_RUN_COUNT) {
                sort(left, right, true);
                return;
            }
        }

        // Check special cases
        if (run[count] == right++) { // The last run contains one element
            run[++count] = right;
        } else if (count == 1) { // The array is already sorted
            return;
        }

        /*
         * Create temporary array, which is used for merging.
         * Implementation note: variable "right" is increased by 1.
         */

        LongList a;
        LongList b;

        //long[] b;
        byte odd = 0;
        //noinspection StatementWithEmptyBody
        for (int n = 1; (n <<= 1) < count; odd ^= 1) ;

        if (odd == 0) {
            b = this;
            a = new LongList(this.size());

            //noinspection StatementWithEmptyBody
            for (int i = left - 1; ++i < right; a.setQuick(i, b.getQuick(i))) ;
        } else {
            a = this;
            b = new LongList(this.size());
        }

        // Merging
        for (int last; count > 1; count = last) {
            for (int k = (last = 0) + 2; k <= count; k += 2) {
                int hi = run[k], mi = run[k - 1];
                for (int i = run[k - 2], p = i, q = mi; i < hi; ++i) {
                    if (q >= hi || p < mi && getQuick(p) <= getQuick(q)) {
                        b.setQuick(i, a.getQuick(p++));
                    } else {
                        b.setQuick(i, a.getQuick(q++));
                    }
                }
                run[++last] = hi;
            }
            if ((count & 1) != 0) {
                //noinspection StatementWithEmptyBody
                for (int i = right, lo = run[count - 1]; --i >= lo; b.setQuick(i, a.getQuick(i))) ;
                run[++last] = right;
            }
            LongList t = a;
            a = b;
            b = t;
        }
    }

    /**
     * Sorts the specified range of the array by Dual-Pivot Quicksort.
     *
     * @param left     the index of the first element, inclusive, to be sorted
     * @param right    the index of the last element, inclusive, to be sorted
     * @param leftmost indicates if this part is the leftmost in the range
     */
    private void sort(int left, int right, boolean leftmost) {
        int length = right - left + 1;

        // Use insertion sort on tiny arrays
        if (length < INSERTION_SORT_THRESHOLD) {
            if (leftmost) {
                /*
                 * Traditional (without sentinel) insertion sort,
                 * optimized for server VM, is used in case of
                 * the leftmost part.
                 */
                for (int i = left, j = i; i < right; j = ++i) {
                    long ai = getQuick(i + 1);
                    while (ai < getQuick(j)) {
                        let(j + 1, j);
                        if (j-- == left) {
                            break;
                        }
                    }
                    setQuick(j + 1, ai);
                }
            } else {
                /*
                 * Skip the longest ascending sequence.
                 */
                do {
                    if (left >= right) {
                        return;
                    }
                } while (getQuick(++left) >= getQuick(left - 1));

                /*
                 * Every element from adjoining part plays the role
                 * of sentinel, therefore this allows us to avoid the
                 * left range check on each iteration. Moreover, we use
                 * the more optimized algorithm, so called pair insertion
                 * sort, which is faster (in the context of Quicksort)
                 * than traditional implementation of insertion sort.
                 */
                for (int k = left; ++left <= right; k = ++left) {
                    long a1 = getQuick(k), a2 = getQuick(left);

                    if (a1 < a2) {
                        a2 = a1;
                        a1 = getQuick(left);
                    }
                    while (a1 < getQuick(--k)) {
                        let(k + 2, k);
                    }
                    setQuick(++k + 1, a1);

                    while (a2 < getQuick(--k)) {
                        let(k + 1, k);
                    }
                    setQuick(k + 1, a2);
                }
                long last = getQuick(right);

                while (last < getQuick(--right)) {
                    let(right + 1, right);
                }
                setQuick(right + 1, last);
            }
            return;
        }

        // Inexpensive approximation of length / 7
        int seventh = (length >> 3) + (length >> 6) + 1;

        /*
         * Sort five evenly spaced elements around (and including) the
         * center element in the range. These elements will be used for
         * pivot selection as described below. The choice for spacing
         * these elements was empirically determined to work well on
         * a wide variety of inputs.
         */
        int e3 = (left + right) >>> 1; // The midpoint
        int e2 = e3 - seventh;
        int e1 = e2 - seventh;
        int e4 = e3 + seventh;
        int e5 = e4 + seventh;

        // Sort these elements using insertion sort
        if (getQuick(e2) < getQuick(e1)) {
            swap(e2, e1);
        }

        if (getQuick(e3) < getQuick(e2)) {
            long t = getQuick(e3);
            let(e3, e2);
            setQuick(e2, t);
            if (t < getQuick(e1)) {
                let(e2, e1);
                setQuick(e1, t);
            }
        }
        if (getQuick(e4) < getQuick(e3)) {
            long t = getQuick(e4);
            let(e4, e3);
            setQuick(e3, t);
            if (t < getQuick(e2)) {
                let(e3, e2);
                setQuick(e2, t);
                if (t < getQuick(e1)) {
                    let(e2, e1);
                    setQuick(e1, t);
                }
            }
        }
        if (getQuick(e5) < getQuick(e4)) {
            long t = getQuick(e5);
            let(e5, e4);
            setQuick(e4, t);
            if (t < getQuick(e3)) {
                let(e4, e3);
                setQuick(e3, t);
                if (t < getQuick(e2)) {
                    let(e3, e2);
                    setQuick(e2, t);
                    if (t < getQuick(e1)) {
                        let(e2, e1);
                        setQuick(e1, t);
                    }
                }
            }
        }

        // Pointers
        int less = left;  // The index of the first element of center part
        int great = right; // The index before the first element of right part

        if (getQuick(e1) != getQuick(e2) && getQuick(e2) != getQuick(e3) && getQuick(e3) != getQuick(e4) && getQuick(e4) != getQuick(e5)) {
            /*
             * Use the second and fourth of the five sorted elements as pivots.
             * These values are inexpensive approximations of the first and
             * second terciles of the array. Note that pivot1 <= pivot2.
             */
            long pivot1 = getQuick(e2);
            long pivot2 = getQuick(e4);

            /*
             * The first and the last elements to be sorted are moved to the
             * locations formerly occupied by the pivots. When partitioning
             * is complete, the pivots are swapped back into their final
             * positions, and excluded from subsequent sorting.
             */
            let(e2, left);
            let(e4, right);

            /*
             * Skip elements, which are less or greater than pivot values.
             */
            //noinspection StatementWithEmptyBody
            while (getQuick(++less) < pivot1) ;
            //noinspection StatementWithEmptyBody
            while (getQuick(--great) > pivot2) ;

            /*
             * Partitioning:
             *
             *   left part           center part                   right part
             * +--------------------------------------------------------------+
             * |  < pivot1  |  pivot1 <= && <= pivot2  |    ?    |  > pivot2  |
             * +--------------------------------------------------------------+
             *               ^                          ^       ^
             *               |                          |       |
             *              less                        k     great
             *
             * Invariants:
             *
             *              all in (left, less)   < pivot1
             *    pivot1 <= all in [less, k)     <= pivot2
             *              all in (great, right) > pivot2
             *
             * Pointer k is the first index of ?-part.
             */
            outer:
            for (int k = less - 1; ++k <= great; ) {
                long ak = getQuick(k);
                if (ak < pivot1) { // Move a[k] to left part
                    let(k, less);
                    /*
                     * Here and below we use "a[i] = b; i++;" instead
                     * of "a[i++] = b;" due to performance issue.
                     */
                    setQuick(less, ak);
                    ++less;
                } else if (ak > pivot2) { // Move a[k] to right part
                    while (getQuick(great) > pivot2) {
                        if (great-- == k) {
                            break outer;
                        }
                    }
                    if (getQuick(great) < pivot1) { // a[great] <= pivot2
                        let(k, less);
                        let(less, great);
                        ++less;
                    } else { // pivot1 <= a[great] <= pivot2
                        let(k, great);
                    }
                    /*
                     * Here and below we use "a[i] = b; i--;" instead
                     * of "a[i--] = b;" due to performance issue.
                     */
                    setQuick(great, ak);
                    --great;
                }
            }

            // Swap pivots into their final positions
            let(left, less - 1);
            setQuick(less - 1, pivot1);
            let(right, great + 1);
            setQuick(great + 1, pivot2);

            // Sort left and right parts recursively, excluding known pivots
            sort(left, less - 2, leftmost);
            sort(great + 2, right, false);

            /*
             * If center part is too large (comprises > 4/7 of the array),
             * swap internal pivot values to ends.
             */
            if (less < e1 && e5 < great) {
                /*
                 * Skip elements, which are equal to pivot values.
                 */
                while (getQuick(less) == pivot1) {
                    ++less;
                }

                while (getQuick(great) == pivot2) {
                    --great;
                }

                /*
                 * Partitioning:
                 *
                 *   left part         center part                  right part
                 * +----------------------------------------------------------+
                 * | == pivot1 |  pivot1 < && < pivot2  |    ?    | == pivot2 |
                 * +----------------------------------------------------------+
                 *              ^                        ^       ^
                 *              |                        |       |
                 *             less                      k     great
                 *
                 * Invariants:
                 *
                 *              all in (*,  less) == pivot1
                 *     pivot1 < all in [less,  k)  < pivot2
                 *              all in (great, *) == pivot2
                 *
                 * Pointer k is the first index of ?-part.
                 */
                outer:
                for (int k = less - 1; ++k <= great; ) {
                    long ak = getQuick(k);
                    if (ak == pivot1) { // Move a[k] to left part
                        let(k, less);
                        setQuick(less, ak);
                        ++less;
                    } else if (ak == pivot2) { // Move a[k] to right part
                        while (getQuick(great) == pivot2) {
                            if (great-- == k) {
                                break outer;
                            }
                        }
                        if (getQuick(great) == pivot1) { // a[great] < pivot2
                            let(k, less);
                            /*
                             * Even though a[great] equals to pivot1, the
                             * assignment a[less] = pivot1 may be incorrect,
                             * if a[great] and pivot1 are floating-point zeros
                             * of different signs. Therefore in float and
                             * double sorting methods we have to use more
                             * accurate assignment a[less] = a[great].
                             */
                            setQuick(less, pivot1);
                            ++less;
                        } else { // pivot1 < a[great] < pivot2
                            let(k, great);
                        }
                        setQuick(great, ak);
                        --great;
                    }
                }
            }

            // Sort center part recursively
            sort(less, great, false);

        } else { // Partitioning with one pivot
            /*
             * Use the third of the five sorted elements as pivot.
             * This value is inexpensive approximation of the median.
             */
            long pivot = getQuick(e3);

            /*
             * Partitioning degenerates to the traditional 3-way
             * (or "Dutch National Flag") schema:
             *
             *   left part    center part              right part
             * +-------------------------------------------------+
             * |  < pivot  |   == pivot   |     ?    |  > pivot  |
             * +-------------------------------------------------+
             *              ^              ^        ^
             *              |              |        |
             *             less            k      great
             *
             * Invariants:
             *
             *   all in (left, less)   < pivot
             *   all in [less, k)     == pivot
             *   all in (great, right) > pivot
             *
             * Pointer k is the first index of ?-part.
             */
            for (int k = less; k <= great; ++k) {
                if (getQuick(k) == pivot) {
                    continue;
                }
                long ak = getQuick(k);
                if (ak < pivot) { // Move a[k] to left part
                    let(k, less);
                    setQuick(less, ak);
                    ++less;
                } else { // a[k] > pivot - Move a[k] to right part
                    while (getQuick(great) > pivot) {
                        --great;
                    }
                    if (getQuick(great) < pivot) { // a[great] <= pivot
                        let(k, less);
                        let(less, great);
                        ++less;
                    } else { // a[great] == pivot
                        /*
                         * Even though a[great] equals to pivot, the
                         * assignment a[k] = pivot may be incorrect,
                         * if a[great] and pivot are floating-point
                         * zeros of different signs. Therefore in float
                         * and double sorting methods we have to use
                         * more accurate assignment a[k] = a[great].
                         */
                        setQuick(k, pivot);
                    }
                    setQuick(great, ak);
                    --great;
                }
            }

            /*
             * Sort left and right parts recursively.
             * All elements from center part are equal
             * and, therefore, already sorted.
             */
            sort(left, less - 1, leftmost);
            sort(great + 1, right, false);
        }
    }

    private void swap(int a, int b) {
        long tmp = getQuick(a);
        setQuick(a, getQuick(b));
        setQuick(b, tmp);
    }
}