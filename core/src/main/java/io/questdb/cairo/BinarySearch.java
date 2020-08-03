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

    public static long findOrEmplace(ReadOnlyColumn column, long value, long low, long high, int scanDirection) {
        while (low < high) {
            long mid = (low + high - 1) >>> 1;
            long midVal = column.getLong(mid * 8);

            if (midVal < value)
                low = mid + 1;
            else if (midVal > value)
                high = mid;
            else {
                // In case of multiple equal values, find the first
                mid += scanDirection;
                while (mid > 0 && mid < high && midVal == column.getLong(mid * 8)) {
                    mid += scanDirection;
                }
                return mid - scanDirection;
            }
        }
        return -(low + 1);
    }

    public static long find(ReadOnlyColumn column, long value, long low, long high) {
        while (low < high) {
            long mid = (low + high - 1) >>> 1;
            long midVal = column.getLong(mid * 8);

            if (midVal < value)
                low = mid + 1;
            else if (midVal > value)
                high = mid;
            else {
                // In case of multiple equal values, find the first
                mid += 1;
                while (mid > 0 && mid < high && midVal == column.getLong(mid * 8)) {
                    mid += 1;
                }
                return mid - 1;
            }
        }
        return low;
    }
}
