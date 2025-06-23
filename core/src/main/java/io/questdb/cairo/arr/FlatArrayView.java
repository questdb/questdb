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
     * Appends the contents of this flat array to the supplied memory block.
     */
    void appendToMemFlat(MemoryA mem, int flatViewOffset, int flatViewLength);

    default double avgDouble(int flatViewOffset, int flatViewLength) {
        double sum = 0d;
        int count = 0;
        for (int i = flatViewOffset, n = flatViewOffset + flatViewLength; i < n; i++) {
            double v = getDoubleAtAbsIndex(i);
            if (Numbers.isFinite(v)) {
                sum += v;
                count++;
            }
        }
        return count == 0 ? Double.NaN : sum / count;
    }

    default int binarySearchDouble(double value, int flatViewOffset, int flatViewLength, boolean ascending) {
        int low = flatViewOffset;
        int high = flatViewOffset + flatViewLength - 1;

        while (low <= high) {
            int mid = low + (high - low) / 2;
            double midVal = getDoubleAtAbsIndex(mid);
            if (Math.abs(midVal - value) <= Numbers.DOUBLE_TOLERANCE) {
                do {
                    if (mid > low) {
                        mid--;
                    } else {
                        return mid - flatViewOffset + 1;
                    }
                } while (Math.abs(getDoubleAtAbsIndex(mid) - value) <= Numbers.DOUBLE_TOLERANCE);
                return mid - flatViewOffset + 2;
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

        return -(low - flatViewOffset + 1);
    }

    default int countDouble(int flatViewOffset, int flatViewLength) {
        int count = 0;
        for (int i = flatViewOffset, n = flatViewOffset + flatViewLength; i < n; i++) {
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

    default int linearSearch(double value, int flatViewOffset, int flatViewLength) {
        for (int i = flatViewOffset, n = flatViewOffset + flatViewLength; i < n; i++) {
            if (Math.abs(getDoubleAtAbsIndex(i) - value) <= Numbers.DOUBLE_TOLERANCE) {
                return i - flatViewOffset + 1;
            }
        }
        return 0;
    }

    default double sumDouble(int flatViewOffset, int flatViewLength) {
        double sum = 0d;
        for (int i = flatViewOffset, n = flatViewOffset + flatViewLength; i < n; i++) {
            double v = getDoubleAtAbsIndex(i);
            if (Numbers.isFinite(v)) {
                sum += v;
            }
        }
        return sum;
    }
}
