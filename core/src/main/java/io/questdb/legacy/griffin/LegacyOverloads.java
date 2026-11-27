/*+*****************************************************************************
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


package io.questdb.legacy.griffin;

import io.questdb.std.Numbers;

import static io.questdb.cairo.ColumnType.*;

/**
 * Master's overload distances, which the legacy {@link FunctionParser} resolves overloads by. The shared
 * {@link io.questdb.cairo.ColumnType#overloadDistance} widens integers to LONG256 and no longer narrows LONG256 to
 * LONG, conversions the new planner's resolver inserts and the legacy parser does not.
 */
final class LegacyOverloads {
    private static final short OVERLOAD_PRIORITY_N = (short) Math.pow(2.0, Numbers.msb(NULL) + 1.0);
    private static final int[] OVERLOAD_PRIORITY_MATRIX = new int[OVERLOAD_PRIORITY_N * OVERLOAD_PRIORITY_N];

    private LegacyOverloads() {
    }

    static int overloadDistance(short from, short to) {
        final int fromTag = tagOf(from);
        final int toTag = tagOf(to);
        assert toTag > UNDEFINED : "Undefined not supported in overloads";
        return OVERLOAD_PRIORITY_MATRIX[OVERLOAD_PRIORITY_N * fromTag + toTag];
    }

    static {
        final short[][] overloadPriority = new short[][]{
                /* 0 UNDEFINED   */  {DOUBLE, FLOAT, STRING, VARCHAR, LONG, TIMESTAMP, DATE, INT, CHAR, SHORT, BYTE, BOOLEAN}
                /* 1  BOOLEAN    */, {BOOLEAN}
                /* 2  BYTE       */, {BYTE, SHORT, INT, LONG, FLOAT, DOUBLE, DECIMAL}
                /* 3  SHORT      */, {SHORT, INT, LONG, FLOAT, DOUBLE, CHAR, DECIMAL}
                /* 4  CHAR       */, {CHAR, STRING, VARCHAR, SHORT, INT, LONG, FLOAT, DOUBLE}
                /* 5  INT        */, {INT, LONG, FLOAT, DOUBLE, TIMESTAMP, DATE, DECIMAL}
                /* 6  LONG       */, {LONG, DOUBLE, TIMESTAMP, DATE, DECIMAL}
                /* 7  DATE       */, {DATE, TIMESTAMP, LONG, DOUBLE}
                /* 8  TIMESTAMP  */, {TIMESTAMP, LONG, DATE, DOUBLE}
                /* 9  FLOAT      */, {FLOAT, DOUBLE}
                /* 10 DOUBLE     */, {DOUBLE}
                /* 11 STRING     */, {STRING, VARCHAR, CHAR, DOUBLE, LONG, INT, FLOAT, SHORT, BYTE, TIMESTAMP, DATE, SYMBOL, IPv4}
                /* 12 SYMBOL     */, {SYMBOL, STRING, VARCHAR, CHAR, INT, TIMESTAMP}
                /* 13 LONG256    */, {LONG256, LONG}
                /* 14 GEOBYTE    */, {GEOBYTE, GEOSHORT, GEOINT, GEOLONG, GEOHASH}
                /* 15 GEOSHORT   */, {GEOSHORT, GEOINT, GEOLONG, GEOHASH}
                /* 16 GEOINT     */, {GEOINT, GEOLONG, GEOHASH}
                /* 17 GEOLONG    */, {GEOLONG, GEOHASH}
                /* 18 BINARY     */, {BINARY}
                /* 19 UUID       */, {UUID, STRING}
                /* 20 CURSOR     */, {CURSOR}
                /* 21 unused     */, {}
                /* 22 unused     */, {}
                /* 23 unused     */, {}
                /* 24 LONG128    */, {LONG128}
                /* 25 IPv4       */, {IPv4, STRING, VARCHAR}
                /* 26 VARCHAR    */, {VARCHAR, STRING, CHAR, DOUBLE, LONG, INT, FLOAT, SHORT, BYTE, TIMESTAMP, DATE, SYMBOL, IPv4}
                /* 27 ARRAY      */, {ARRAY}
                /* 28 DECIMAL8   */, {DECIMAL8, DECIMAL16, DECIMAL32, DECIMAL64, DECIMAL128, DECIMAL256, DECIMAL}
                /* 29 DECIMAL16  */, {DECIMAL16, DECIMAL32, DECIMAL64, DECIMAL128, DECIMAL256, DECIMAL}
                /* 30 DECIMAL32  */, {DECIMAL32, DECIMAL64, DECIMAL128, DECIMAL256, DECIMAL}
                /* 31 DECIMAL64  */, {DECIMAL64, DECIMAL128, DECIMAL256, DECIMAL}
                /* 32 DECIMAL128 */, {DECIMAL128, DECIMAL256, DECIMAL}
                /* 33 DECIMAL256 */, {DECIMAL256, DECIMAL}
                /* 34 DECIMAL    */, {}
                /* 35 unused     */, {}
                /* 36 unused     */, {}
                /* 37 unused     */, {}
                /* 38 unused     */, {}
                /* 39 INTERVAL   */, {INTERVAL, STRING}
                /* 40 VARCHAR_SLICE */, {VARCHAR, STRING, CHAR, DOUBLE, LONG, INT, FLOAT, SHORT, BYTE, TIMESTAMP, DATE, SYMBOL, IPv4}
                /* 41 NULL       */, {VARCHAR, STRING, DOUBLE, FLOAT, LONG, INT}
        };
        for (short fromTag = UNDEFINED; fromTag < NULL; fromTag++) {
            for (short toTag = BOOLEAN; toTag <= NULL; toTag++) {
                short value = OVERLOAD_NONE;
                short[] priority = overloadPriority[fromTag];
                for (short i = 0; i < priority.length; i++) {
                    if (priority[i] == toTag) {
                        value = i;
                        break;
                    }
                }
                OVERLOAD_PRIORITY_MATRIX[OVERLOAD_PRIORITY_N * fromTag + toTag] = value;
            }
        }
        // When null used as func arg, default to string as function factory arg to avoid weird behaviour
        OVERLOAD_PRIORITY_MATRIX[OVERLOAD_PRIORITY_N * NULL + STRING] = OVERLOAD_FULL;
        // Do the same for symbol -> avoids weird null behaviour
        OVERLOAD_PRIORITY_MATRIX[OVERLOAD_PRIORITY_N * NULL + SYMBOL] = OVERLOAD_FULL;
        // A NULL literal is a scalar, never a cursor (scalar sub-query). Without this a bare
        // `null` matches a CURSOR argument at distance 0, so `col <= null` (i.e. not(col > null))
        // binds to a `>(?C)` cursor-comparison factory and blows up calling getRecordCursorFactory()
        // on the NULL constant. Force no overload so scalar null-comparison factories are used.
        OVERLOAD_PRIORITY_MATRIX[OVERLOAD_PRIORITY_N * NULL + CURSOR] = OVERLOAD_NONE;
    }
}
