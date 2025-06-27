/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

public interface FlatArrayView {
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
            if (Numbers.isFinite(v)) {
                sum += v;
                count++;
            }
        }
        return count == 0 ? Double.NaN : sum / count;
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
     * @return if positive, it's the index of the found element. If negative, its absolute value
     * is the insertion point into the flat array of the element that wasn't found. In both cases,
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
                    do {
                        if (mid > low) {
                            mid--;
                        } else {
                            return mid - offset + 1;
                        }
                    } while (Math.abs(getDoubleAtAbsIndex(mid) - value) <= Numbers.DOUBLE_TOLERANCE);
                    return mid - offset + 2;
                } else {
                    do {
                        if (mid < high) {
                            mid++;
                        } else {
                            return mid - offset + 1;
                        }
                    } while (Math.abs(getDoubleAtAbsIndex(mid) - value) <= Numbers.DOUBLE_TOLERANCE);
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
     * Counts the number of finite numbers within a block of this flat array.
     *
     * @param offset the starting offset of the block (in elements)
     * @param length the number of elements in the block
     */
    default int countDouble(int offset, int length) {
        // Optimization idea: store the number of nulls (or a null bitmap) as metadata.
        // Then the array's count could be obtained directly from the metadata.
        // Similarly, knowing from the metadata that a set of data contains no nulls could
        // also offer optimizations for other aggregate operators (sum, avg etc.).
        int count = 0;
        for (int i = offset, n = offset + length; i < n; i++) {
            double v = getDoubleAtAbsIndex(i);
            if (Numbers.isFinite(v)) {
                count++;
            }
        }
        return count;
    }

    double getDoubleAtAbsIndex(int elemIndex);

    long getLongAtAbsIndex(int elemIndex);

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
                return i - offset + 1;
            }
        }
        return Numbers.INT_NULL;
    }

    /**
     * Computes the average of the block of elements in this flat array.
     *
     * @param offset the starting offset of the block (in elements)
     * @param length the number of elements in the block
     */
    default double sumDouble(int offset, int length) {
        double sum = 0d;
        for (int i = offset, n = offset + length; i < n; i++) {
            double v = getDoubleAtAbsIndex(i);
            if (Numbers.isFinite(v)) {
                sum += v;
            }
        }
        return sum;
    }
}
