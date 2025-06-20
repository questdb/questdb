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

    double getDoubleAtAbsIndex(int elemIndex);

    long getLongAtAbsIndex(int elemIndex);

    /**
     * Returns the number of elements stored in this flat array.
     */
    int length();

    default double sumDouble(int flatViewOffset, int flatViewLength) {
        double sum = 0d;
        for (int i = 0; i < flatViewLength; i++) {
            double v = getDoubleAtAbsIndex(i + flatViewOffset);
            if (Numbers.isFinite(v)) {
                sum += v;
            }
        }
        return sum;
    }

    ;
}
