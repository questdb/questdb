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

package io.questdb.cairo;

public class Decimals {
    public static final int MAX_SCALE = 76;
    private static final int[] PRECISION_SIZE_POW2 = {
            0, 0, 0, // precision 0-2 -> 1 byte
            1, 1, // precision 3-4 -> 2 byte
            2, 2, 2, 2, 2, // precision 5-9 -> 2 bytes
            3, 3, 3, 3, 3, 3, 3, 3, 3, // precision 10-18 -> 4 bytes
            4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4,
            4, 4, 4, 4, 4, 4, 4, 4, // precision 19-38 -> 8 bytes
            5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5,
            5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5,
            5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5,
            5, 5, // precision 39-76 -> 16 bytes
    };

    /**
     * Returns the required storage size in bytes (pow 2) for a decimal with given precision.
     * <p>
     * Storage sizes:
     * - Precision 1-2:   1 byte  (7 bits for magnitude + 1 sign bit)
     * - Precision 3-4:   2 bytes (15 bits for magnitude + 1 sign bit)
     * - Precision 5-9:   4 bytes (31 bits for magnitude + 1 sign bit)
     * - Precision 10-18: 8 bytes (63 bits for magnitude + 1 sign bit)
     * - Precision 19-38: 16 bytes (128-bit storage)
     * - Precision 39-76: 32 bytes (256-bit storage)
     *
     * @param precision the number of significant digits
     * @return the required storage size in bytes
     * @throws IllegalArgumentException if precision is invalid
     */
    public static int getStorageSizePow2(int precision) {
        if (precision < 1 || precision > MAX_SCALE) {
            throw new IllegalArgumentException("Invalid decimal precision: " + precision +
                    ". Must be between 1 and " + MAX_SCALE);
        }

        return PRECISION_SIZE_POW2[precision];
    }
}
