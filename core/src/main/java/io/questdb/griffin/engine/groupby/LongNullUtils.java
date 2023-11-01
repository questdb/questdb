/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.cairo.GeoHashes;
import io.questdb.std.Numbers;

public final class LongNullUtils {
    private static final long[] LONG_NULLs = new long[ColumnType.NULL];

    public static long getLongNull(int type) {
        return LONG_NULLs[ColumnType.tagOf(type)];
    }

    static {
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
                    LONG_NULLs[i] = Float.floatToIntBits(Float.NaN);
                    break;

                case ColumnType.DOUBLE:
                    LONG_NULLs[i] = Double.doubleToLongBits(Double.NaN);
                    break;

                case ColumnType.GEOBYTE:
                case ColumnType.GEOSHORT:
                case ColumnType.GEOINT:
                case ColumnType.GEOLONG:
                    LONG_NULLs[i] = GeoHashes.NULL;
                    break;

                default:
                    LONG_NULLs[i] = 0L;
                    break;
            }
        }
    }
}
