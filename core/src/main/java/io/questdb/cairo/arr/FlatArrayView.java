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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.std.Numbers;

public interface FlatArrayView {
    void appendToMemFlat(MemoryA mem);

    /**
     * Returns the type of elements stored in this flat view,
     * one of the {@link io.questdb.cairo.ColumnType} constants.
     */
    int elemType();

    default boolean flatEquals(FlatArrayView other) {
        int length = this.length();
        if (length != other.length()) {
            return false;
        }
        switch (elemType()) {
            case ColumnType.DOUBLE:
                for (int i = 0; i < length; i++) {
                    if (!Numbers.equals(getDouble(i), other.getDouble(i))) {
                        return false;
                    }
                }
                break;
            case ColumnType.LONG:
                for (int i = 0; i < length; i++) {
                    if (getLong(i) != other.getLong(i)) {
                        return false;
                    }
                }
                break;
            default:
                throw new UnsupportedOperationException("Only DOUBLE and LONG are supported");
        }
        return true;
    }

    double getDouble(int elemIndex);

    long getLong(int elemIndex);

    int length();
}
