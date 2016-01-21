/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.nfsdb.collections;

import com.nfsdb.misc.Unsafe;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;

public class ObjList<T> implements Mutable {
    private static final int DEFAULT_ARRAY_SIZE = 16;
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

    private final Iter iterator = new Iter();
    private T[] buffer;
    private int pos = 0;
    private StringBuilder toStringBuilder;

    @SuppressWarnings("unchecked")
    public ObjList() {
        this.buffer = (T[]) new Object[DEFAULT_ARRAY_SIZE];
    }

    @SuppressWarnings("unchecked")
    public ObjList(int capacity) {
        this.buffer = (T[]) new Object[capacity < DEFAULT_ARRAY_SIZE ? DEFAULT_ARRAY_SIZE : capacity];
    }

    /**
     * {@inheritDoc}
     */
    public void add(T value) {
        ensureCapacity0(pos + 1);
        Unsafe.arrayPut(buffer, pos++, value);
    }

//    public void addAll(ObjList<T> that) {
//        int n = that.size();
//        ensureCapacity0(pos + n);
//        for (int i = 0; i < n; i++) {
//            Unsafe.arrayPut(buffer, pos++, that.getQuick(i));
//        }
//    }

    /**
     * {@inheritDoc}
     */
    public void clear() {
        pos = 0;
        Arrays.fill(buffer, null);
    }

    public void ensureCapacity(int capacity) {
        ensureCapacity0(capacity);
        pos = capacity;
    }

    public void extendAndSet(int index, T value) {
        ensureCapacity0(index + 1);
        if (index >= pos) {
            pos = index + 1;
        }
        Unsafe.arrayPut(buffer, index, value);
    }

    /**
     * {@inheritDoc}
     */
    public T get(int index) {
        if (index < pos) {
            return Unsafe.arrayGet(buffer, index);
        }
        throw new ArrayIndexOutOfBoundsException(index);
    }

    public T getAndSetQuick(int index, T value) {
        T v = Unsafe.arrayGet(buffer, index);
        Unsafe.arrayPut(buffer, index, value);
        return v;
    }

    /**
     * Returns last element of the list or null if list is empty.
     *
     * @return last element of the list
     */
    public T getLast() {
        if (pos > 0) {
            return Unsafe.arrayGet(buffer, pos - 1);
        }
        return null;
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
    public T getQuick(int index) {
        return Unsafe.arrayGet(buffer, index);
    }

    /**
     * Returns element at the specified position or null, if element index is
     * out of bounds. This is an alternative to throwing runtime exception or
     * doing preemptive check.
     *
     * @param index position of element
     * @return element at the specified position.
     */
    public T getQuiet(int index) {
        if (index < pos) {
            return Unsafe.arrayGet(buffer, index);
        }
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        int hashCode = 1;
        for (int i = 0, n = pos; i < n; i++) {
            T o = getQuick(i);
            hashCode = 31 * hashCode + (o == null ? 0 : o.hashCode());
        }
        return hashCode;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object that) {
        return this == that || that instanceof ObjList && equals((ObjList) that);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        if (toStringBuilder == null) {
            toStringBuilder = new StringBuilder();
        }

        toStringBuilder.setLength(0);
        toStringBuilder.append('[');
        for (int i = 0, k = size(); i < k; i++) {
            if (i > 0) {
                toStringBuilder.append(',');
            }
            toStringBuilder.append(getQuick(i));
        }
        toStringBuilder.append(']');
        return toStringBuilder.toString();
    }

    /**
     * {@inheritDoc}
     */
    @NotNull
    public Iterator<T> iterator() {
        iterator.n = 0;
        return iterator;
    }

    /**
     * {@inheritDoc}
     */
    public void remove(int index) {
        if (pos < 1 || index >= pos) {
            return;
        }
        int move = pos - index - 1;
        if (move > 0) {
            System.arraycopy(buffer, index + 1, buffer, index, move);
        }
        Unsafe.arrayPut(buffer, --pos, null);
    }

    /**
     * {@inheritDoc}
     */
    public boolean remove(Object o) {
        if (pos == 0) {
            return false;
        }
        int index = indexOf(o);
        if (index > -1) {
            remove(index);
            return true;
        }
        return false;
    }

    public void seed(int capacity, ObjectFactory<T> factory) {
        ensureCapacity0(capacity);
        pos = capacity;
        for (int i = 0; i < capacity; i++) {
            T o = Unsafe.arrayGet(buffer, i);
            if (o == null) {
                Unsafe.arrayPut(buffer, i, factory.newInstance());
            } else if (o instanceof Mutable) {
                ((Mutable) o).clear();
            }
        }
    }

    public void setAll(int capacity, T value) {
        ensureCapacity0(capacity);
        pos = 0;
        Arrays.fill(buffer, value);
    }

    public void setQuick(int index, T value) {
        Unsafe.arrayPut(buffer, index, value);
    }

    /**
     * {@inheritDoc}
     */
    public int size() {
        return pos;
    }

    public void sort(Comparator<T> cmp) {
        sort(0, size() - 1, cmp);
    }

    @SuppressWarnings("unchecked")
    private void ensureCapacity0(int capacity) {
        int l = buffer.length;
        if (capacity > l) {
            int newCap = Math.max(l << 1, capacity);
            T[] buf = (T[]) new Object[newCap];
            System.arraycopy(buffer, 0, buf, 0, l);
            this.buffer = buf;
        }
    }

    private boolean equals(ObjList that) {
        if (this.pos == that.pos) {
            for (int i = 0, n = pos; i < n; i++) {
                Object lhs = this.getQuick(i);
                if (lhs == null) {
                    return that.getQuick(i) == null;
                } else if (lhs.equals(that.getQuick(i))) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * {@inheritDoc}
     */
    private int indexOf(Object o) {
        if (o == null) {
            return indexOfNull();
        } else {
            for (int i = 0, n = pos; i < n; i++) {
                if (o.equals(getQuick(i))) {
                    return i;
                }
            }
            return -1;
        }
    }

    private int indexOfNull() {
        for (int i = 0, n = pos; i < n; i++) {
            if (null == getQuick(i)) {
                return i;
            }
        }
        return -1;
    }

    private void let(int a, int b) {
        Unsafe.arrayPut(buffer, a, Unsafe.arrayGet(buffer, b));
    }

    /**
     * Sorts the specified range of the array.
     *
     * @param left  the index of the first element, inclusive, to be sorted
     * @param right the index of the last element, inclusive, to be sorted
     */
    private void sort(int left, int right, Comparator<T> cmp) {
        // Use Quicksort on small arrays
        if (right - left < QUICKSORT_THRESHOLD) {
            sort(left, right, true, cmp);
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
            if (cmp.compare(getQuick(k), getQuick(k + 1)) < 0) { // ascending
                //noinspection StatementWithEmptyBody
                while (++k <= right && cmp.compare(getQuick(k - 1), getQuick(k)) <= 0) ;
            } else if (cmp.compare(getQuick(k), getQuick(k + 1)) > 0) { // descending
                //noinspection StatementWithEmptyBody
                while (++k <= right && cmp.compare(getQuick(k - 1), getQuick(k)) >= 0) ;
                for (int lo = run[count] - 1, hi = k; ++lo < --hi; ) {
                    swap(lo, hi);
                }
            } else { // equal
                for (int m = MAX_RUN_LENGTH; ++k <= right && getQuick(k - 1) == getQuick(k); ) {
                    if (--m == 0) {
                        sort(left, right, true, cmp);
                        return;
                    }
                }
            }

            /*
             * The array is not highly structured,
             * use Quicksort instead of merge sort.
             */
            if (++count == MAX_RUN_COUNT) {
                sort(left, right, true, cmp);
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

        ObjList<T> a;
        ObjList<T> b;

        //long[] b;
        byte odd = 0;
        //noinspection StatementWithEmptyBody
        for (int n = 1; (n <<= 1) < count; odd ^= 1) ;

        if (odd == 0) {
            b = this;
            a = new ObjList<>(this.size());

            //noinspection StatementWithEmptyBody
            for (int i = left - 1; ++i < right; a.setQuick(i, b.getQuick(i))) ;
        } else {
            a = this;
            b = new ObjList<>(this.size());
        }

        // Merging
        for (int last; count > 1; count = last) {
            for (int k = (last = 0) + 2; k <= count; k += 2) {
                int hi = run[k], mi = run[k - 1];
                for (int i = run[k - 2], p = i, q = mi; i < hi; ++i) {
                    if (q >= hi || p < mi && cmp.compare(getQuick(p), getQuick(q)) <= 0) {
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
            ObjList<T> t = a;
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
    @SuppressFBWarnings({"CC_CYCLOMATIC_COMPLEXITY"})
    private void sort(int left, int right, boolean leftmost, Comparator<T> cmp) {
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
                    T ai = getQuick(i + 1);
                    while (cmp.compare(ai, getQuick(j)) < 0) {
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
                } while (cmp.compare(getQuick(++left), getQuick(left - 1)) >= 0);

                /*
                 * Every element from adjoining part plays the role
                 * of sentinel, therefore this allows us to avoid the
                 * left range check on each iteration. Moreover, we use
                 * the more optimized algorithm, so called pair insertion
                 * sort, which is faster (in the context of Quicksort)
                 * than traditional implementation of insertion sort.
                 */
                for (int k = left; ++left <= right; k = ++left) {
                    T a1 = getQuick(k), a2 = getQuick(left);

                    if (cmp.compare(a1, a2) < 0) {
                        a2 = a1;
                        a1 = getQuick(left);
                    }
                    while (cmp.compare(a1, getQuick(--k)) < 0) {
                        let(k + 2, k);
                    }
                    setQuick(++k + 1, a1);

                    while (cmp.compare(a2, getQuick(--k)) < 0) {
                        let(k + 1, k);
                    }
                    setQuick(k + 1, a2);
                }
                T last = getQuick(right);

                while (cmp.compare(last, getQuick(--right)) < 0) {
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
        if (cmp.compare(getQuick(e2), getQuick(e1)) < 0) {
            swap(e2, e1);
        }

        if (cmp.compare(getQuick(e3), getQuick(e2)) < 0) {
            T t = getQuick(e3);
            let(e3, e2);
            setQuick(e2, t);
            if (cmp.compare(t, getQuick(e1)) < 0) {
                let(e2, e1);
                setQuick(e1, t);
            }
        }
        if (cmp.compare(getQuick(e4), getQuick(e3)) < 0) {
            T t = getQuick(e4);
            let(e4, e3);
            setQuick(e3, t);
            if (cmp.compare(t, getQuick(e2)) < 0) {
                let(e3, e2);
                setQuick(e2, t);
                if (cmp.compare(t, getQuick(e1)) < 0) {
                    let(e2, e1);
                    setQuick(e1, t);
                }
            }
        }
        if (cmp.compare(getQuick(e5), getQuick(e4)) < 0) {
            T t = getQuick(e5);
            let(e5, e4);
            setQuick(e4, t);
            if (cmp.compare(t, getQuick(e3)) < 0) {
                let(e4, e3);
                setQuick(e3, t);
                if (cmp.compare(t, getQuick(e2)) < 0) {
                    let(e3, e2);
                    setQuick(e2, t);
                    if (cmp.compare(t, getQuick(e1)) < 0) {
                        let(e2, e1);
                        setQuick(e1, t);
                    }
                }
            }
        }

        // Pointers
        int less = left;  // The index of the first element of center part
        int great = right; // The index before the first element of right part

        if (cmp.compare(getQuick(e1), getQuick(e2)) != 0
                && cmp.compare(getQuick(e2), getQuick(e3)) != 0
                && cmp.compare(getQuick(e3), getQuick(e4)) != 0
                && cmp.compare(getQuick(e4), getQuick(e5)) != 0) {
            /*
             * Use the second and fourth of the five sorted elements as pivots.
             * These values are inexpensive approximations of the first and
             * second terciles of the array. Note that pivot1 <= pivot2.
             */
            T pivot1 = getQuick(e2);
            T pivot2 = getQuick(e4);

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
            while (cmp.compare(getQuick(++less), pivot1) < 0) ;
            //noinspection StatementWithEmptyBody
            while (cmp.compare(getQuick(--great), pivot2) > 0) ;

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
                T ak = getQuick(k);
                if (cmp.compare(ak, pivot1) < 0) { // Move a[k] to left part
                    let(k, less);
                    /*
                     * Here and below we use "a[i] = b; i++;" instead
                     * of "a[i++] = b;" due to performance issue.
                     */
                    setQuick(less, ak);
                    ++less;
                } else if (cmp.compare(ak, pivot2) > 0) { // Move a[k] to right part
                    while (cmp.compare(getQuick(great), pivot2) > 0) {
                        if (great-- == k) {
                            break outer;
                        }
                    }
                    if (cmp.compare(getQuick(great), pivot1) < 0) { // a[great] <= pivot2
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
            sort(left, less - 2, leftmost, cmp);
            sort(great + 2, right, false, cmp);

            /*
             * If center part is too large (comprises > 4/7 of the array),
             * swap internal pivot values to ends.
             */
            if (less < e1 && e5 < great) {
                /*
                 * Skip elements, which are equal to pivot values.
                 */
                while (cmp.compare(getQuick(less), pivot1) == 0) {
                    ++less;
                }

                while (cmp.compare(getQuick(great), pivot2) == 0) {
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
                    T ak = getQuick(k);
                    if (cmp.compare(ak, pivot1) == 0) { // Move a[k] to left part
                        let(k, less);
                        setQuick(less, ak);
                        ++less;
                    } else if (cmp.compare(ak, pivot2) == 0) { // Move a[k] to right part
                        while (cmp.compare(getQuick(great), pivot2) == 0) {
                            if (great-- == k) {
                                break outer;
                            }
                        }
                        if (cmp.compare(getQuick(great), pivot1) == 0) { // a[great] < pivot2
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
            sort(less, great, false, cmp);

        } else { // Partitioning with one pivot
            /*
             * Use the third of the five sorted elements as pivot.
             * This value is inexpensive approximation of the median.
             */
            T pivot = getQuick(e3);

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
                if (cmp.compare(getQuick(k), pivot) == 0) {
                    continue;
                }
                T ak = getQuick(k);
                if (cmp.compare(ak, pivot) < 0) { // Move a[k] to left part
                    let(k, less);
                    setQuick(less, ak);
                    ++less;
                } else { // a[k] > pivot - Move a[k] to right part
                    while (cmp.compare(getQuick(great), pivot) > 0) {
                        --great;
                    }
                    if (cmp.compare(getQuick(great), pivot) < 0) { // a[great] <= pivot
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
            sort(left, less - 1, leftmost, cmp);
            sort(great + 1, right, false, cmp);
        }
    }

    private void swap(int a, int b) {
        T tmp = getQuick(a);
        setQuick(a, getQuick(b));
        setQuick(b, tmp);
    }

    private class Iter implements Iterator<T> {
        private int n;

        @Override
        public boolean hasNext() {
            return n < pos;
        }

        @Override
        public T next() {
            return getQuick(n++);
        }

        @Override
        public void remove() {

        }
    }

}