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

package io.questdb.griffin.engine.groupby;

import io.questdb.cairo.ColumnType;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;

public final class LongNullUtils {
    public static final long[] LONG_NULLs = new long[ColumnType.MAX];

    static {
        long buffer = Unsafe.malloc(8);
        for (int i = 0, n = LONG_NULLs.length; i < n; i++) {
            switch (i) {
                case ColumnType.LONG:
                case ColumnType.TIMESTAMP:
                case ColumnType.DATE:
                    LONG_NULLs[i] = Numbers.LONG_NaN;
                    break;

                case ColumnType.INT:
                case ColumnType.SYMBOL:
                    LONG_NULLs[i] = Numbers.INT_NaN;
                    break;

                case ColumnType.FLOAT:
                    Unsafe.getUnsafe().putLong(buffer, 0L);
                    Unsafe.getUnsafe().putFloat(buffer, Float.NaN);
                    LONG_NULLs[i] = Unsafe.getUnsafe().getLong(buffer);
                    break;

                case ColumnType.DOUBLE:
                    Unsafe.getUnsafe().putLong(buffer, 0L);
                    Unsafe.getUnsafe().putDouble(buffer, Double.NaN);
                    LONG_NULLs[i] = Unsafe.getUnsafe().getLong(buffer);
                    break;

                default:
                    LONG_NULLs[i] = 0L;
                    break;
            }
        }

        Unsafe.free(buffer, 8);
    }
}
