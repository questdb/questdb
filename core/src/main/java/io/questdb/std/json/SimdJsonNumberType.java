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

package io.questdb.std.json;

import org.jetbrains.annotations.TestOnly;

/**
 * Maps to the constants of the C++ `simdjson::ondemand::number_type` enum.
 */
public class SimdJsonNumberType {
    /**
     * An unset `SimdJsonNumberType`.
     */
    public static final int UNSET = 0;  // 0

    /**
     * a binary64 number
     */
    public static final int FLOATING_POINT_NUMBER = UNSET + 1;  // 1

    /**
     * a signed integer that fits in a 64-bit word using two's complement
     */
    public static final int SIGNED_INTEGER = FLOATING_POINT_NUMBER + 1;  // 2

    /**
     * a positive integer larger or equal to 2^63
     */
    public static final int UNSIGNED_INTEGER = SIGNED_INTEGER + 1;  // 3

    /**
     * a big integer that does not fit in a 64-bit word
     */
    public static final int BIG_INTEGER = UNSIGNED_INTEGER + 1;  // 4

    @TestOnly
    public static String nameOf(int numberType) {
        switch (numberType) {
            case UNSET:
                return "UNSET";
            case FLOATING_POINT_NUMBER:
                return "FLOATING_POINT_NUMBER";
            case SIGNED_INTEGER:
                return "SIGNED_INTEGER";
            case UNSIGNED_INTEGER:
                return "UNSIGNED_INTEGER";
            case BIG_INTEGER:
                return "BIG_INTEGER";
            default:
                return "UNKNOWN";
        }
    }
}
