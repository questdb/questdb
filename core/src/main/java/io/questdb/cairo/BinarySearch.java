/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.cairo;

public class BinarySearch {
    public static final int SCAN_UP = -1;
    public static final int SCAN_DOWN = 1;

    /**
     * Performs binary search on column of Long values.
     *
     * @param column        the column
     * @param value         the search value
     * @param low           the low boundary of the search, inclusive of value
     * @param high          the high boundary of the search inclusive of value
     * @param scanDirection logical direction in which column is searched. UP means we are looking for
     *                      the bottom boundary of the values that are lower or equal the search value. DOWN means
     *                      we are looking for upper boundary of the values that are greater or equal the search
     *                      value.
     * @return index in column where value is less or equal to the search value. If column contains
     * multiple exact matches the scanDirection determines whether top or bottom of these matches is returned.
     * When scan direction is DOWN - the last index of exact matches is returns, when UP - the first index
     */
    public static long find(ReadOnlyColumn column, long value, long low, long high, int scanDirection) {
        while (low < high) {
            long mid = (low + high) / 2;
            long midVal = column.getLong(mid * Long.BYTES);

            if (midVal < value) {
                if (low < mid) {
                    low = mid;
                } else {
                    final long hv = column.getLong(low * Long.BYTES);
                    if (hv <= value) {
                        return low;
                    }

                    if (column.getLong(high * Long.BYTES) <= value) {
                        return high;
                    }
                    return low;
                }
            } else if (midVal > value)
                high = mid;
            else {
                // In case of multiple equal values, find the first
                mid += scanDirection;
                while (mid > 0 && mid <= high && midVal == column.getLong(mid * Long.BYTES)) {
                    mid += scanDirection;
                }
                return mid - scanDirection;
            }
        }

        if (column.getLong(low * Long.BYTES) <= value) {
            return low;
        }
        return -1;
    }
}
