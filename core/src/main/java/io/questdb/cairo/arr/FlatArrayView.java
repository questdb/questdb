/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.cairo.arr;

import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Utf8Sequence;

public interface FlatArrayView {

    default long appendPlainDoubleValue(long addr, int offset, int length) {
        for (int i = offset, n = offset + length; i < n; i++) {
            Unsafe.getUnsafe().putDouble(addr, getDoubleAtAbsIndex(i));
            addr += Double.BYTES;
        }
        return addr;
    }

    /**
     * Appends a block of elements from this flat array to the supplied memory
     * block.
     *
     * @param offset the starting offset (in elements) into this array from which to append
     * @param length the number of elements to append
     */
    void appendToMemFlat(MemoryA mem, int offset, int length);

    /**
     * Computes the average of the block of elements in this flat array.
     *
     * @param offset the starting offset of the block (in elements)
     * @param length the number of elements in the block
     */
    default double avgDouble(int offset, int length) {
        double sum = 0d;
        int count = 0;
        for (int i = offset, n = offset + length; i < n; i++) {
            double v = getDoubleAtAbsIndex(i);
            if (!Double.isNaN(v)) {
                sum += v;
                count++;
            }
        }
        return sum / count;
    }

    /**
     * Performs a binary search on the block of elements in this flat array. The
     * elements must be sorted in the order specified by the {@code ascending} parameter.
     *
     * @param offset      the starting offset of the block (in elements)
     * @param length      the number of elements in the block
     * @param ascending   if true, the elements are expected to be sorted in the ascending order;
     *                    otherwise, they must be sorted in the descending order
     * @param forwardScan if true when array has multiple equal elements, return the position of the first equal element;
     *                    otherwise, return the position of the last equal element.
     * @return if zero or positive, it's the index of the found element. If negative, its absolute value
     * is the insertion point into the flat array of the element that wasn't found, plus one. In both cases,
     * the index is relative to the supplied offset.
     */
    default int binarySearchDouble(double value, int offset, int length, boolean ascending, boolean forwardScan) {
        int low = offset;
        int high = offset + length - 1;

        while (low <= high) {
            int mid = low + (high - low) / 2;
            double midVal = getDoubleAtAbsIndex(mid);
            if (Math.abs(midVal - value) <= Numbers.DOUBLE_TOLERANCE) {
                if (forwardScan) {
                    while (low < mid) {
                        int m = low + (mid - low) / 2;
                        if (Math.abs(getDoubleAtAbsIndex(m) - value) <= Numbers.DOUBLE_TOLERANCE) {
                            mid = m;
                        } else {
                            low = m + 1;
                        }
                    }
                    return low - offset;
                } else {
                    while (mid < high) {
                        int m = mid + (high - mid + 1) / 2;
                        if (Math.abs(getDoubleAtAbsIndex(m) - value) <= Numbers.DOUBLE_TOLERANCE) {
                            mid = m;
                        } else {
                            high = m - 1;
                        }
                    }
                    return mid - offset;
                }
            }
            if (ascending) {
                if (midVal < value) {
                    low = mid + 1;
                } else {
                    high = mid - 1;
                }
            } else {
                if (midVal > value) {
                    low = mid + 1;
                } else {
                    high = mid - 1;
                }
            }
        }

        return -(low - offset + 1);
    }

    /**
     * Counts the number of non-null numbers within a block of this
     * flat array of DOUBLE.
     *
     * @param offset the starting offset of the block (in elements)
     * @param length the number of elements in the block
     */
    default int countDouble(int offset, int length) {
        int count = 0;
        for (int i = offset, n = offset + length; i < n; i++) {
            if (Numbers.isFinite(getDoubleAtAbsIndex(i))) {
                count++;
            }
        }
        return count;
    }

    /**
     * Counts the number of non-null numbers within a block of this
     * flat array of LONG.
     *
     * @param offset the starting offset of the block (in elements)
     * @param length the number of elements in the block
     */
    default int countLong(int offset, int length) {
        int count = 0;
        for (int i = offset, n = offset + length; i < n; i++) {
            double v = getLongAtAbsIndex(i);
            if (v != Numbers.LONG_NULL) {
                count++;
            }
        }
        return count;
    }

    /**
     * Returns the double value at the provided absolute flat view index.
     */
    double getDoubleAtAbsIndex(int elemIndex);

    /**
     * Returns the long value at the provided absolute flat view index.
     */
    long getLongAtAbsIndex(int elemIndex);

    default Utf8Sequence getVarcharAt(int index) {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns the number of elements stored in this flat array.
     */
    int length();

    /**
     * Performs a linear search on the block of elements in this flat array.
     *
     * @param offset the starting offset of the block (in elements)
     * @param length the number of elements in the block
     */
    default int linearSearch(double value, int offset, int length) {
        for (int i = offset, n = offset + length; i < n; i++) {
            if (Math.abs(getDoubleAtAbsIndex(i) - value) <= Numbers.DOUBLE_TOLERANCE) {
                return i - offset;
            }
        }
        return Numbers.INT_NULL;
    }

    /**
     * Returns the maximum element in the selected range of this flat array.
     * If the range is empty, or it doesn't contain any finite elements, returns NaN.
     */
    default double maxDouble(int offset, int length) {
        double max = Double.NEGATIVE_INFINITY;
        for (int i = offset, n = offset + length; i < n; i++) {
            double v = getDoubleAtAbsIndex(i);
            if (Numbers.isFinite(v) && v > max) {
                max = v;
            }
        }
        return Numbers.isFinite(max) ? max : Double.NaN;
    }

    /**
     * Returns the minimum element in the selected range of this flat array.
     * If the range is empty, or it doesn't contain any finite elements, returns NaN.
     */
    default double minDouble(int offset, int length) {
        double min = Double.POSITIVE_INFINITY;
        for (int i = offset, n = offset + length; i < n; i++) {
            double v = getDoubleAtAbsIndex(i);
            if (Numbers.isFinite(v) && v < min) {
                min = v;
            }
        }
        return Numbers.isFinite(min) ? min : Double.NaN;
    }

    /**
     * Computes the average of the block of elements in this flat array.
     * Uses Kahan summation.
     *
     * @param offset the starting offset of the block (in elements)
     * @param length the number of elements in the block
     */
    default double sumDouble(int offset, int length) {
        double sum = Double.NaN;
        double compensation = 0d;
        for (int i = offset, n = offset + length; i < n; i++) {
            double v = getDoubleAtAbsIndex(i);
            if (Numbers.isFinite(v)) {
                if (compensation == 0d && Numbers.isNull(sum)) {
                    sum = 0d;
                }
                final double y = v - compensation;
                final double t = sum + y;
                compensation = t - sum - y;
                sum = t;
            }
        }
        return sum;
    }
}
