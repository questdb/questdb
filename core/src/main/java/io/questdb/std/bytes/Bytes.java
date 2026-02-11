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

package io.questdb.std.bytes;

public class Bytes {

    private Bytes() {
    }

    /**
     * Aligns the given size to 4 bytes.
     */
    public static long align4b(long size) {
        return (size + 3) & ~0x3;
    }

    /**
     * Aligns the given size to 8 bytes.
     */
    public static long align8b(long size) {
        return (size + 7) & ~0x7;
    }

    /**
     * Compute the size of a range, checking for overflow.
     */
    public static int checkedLoHiSize(long lo, long hi, int baseSize) {
        final long additional = hi - lo;
        if (additional < 0) {
            throw new IllegalArgumentException("lo > hi");
        }
        final long size = baseSize + additional;

        if (size > (long) Integer.MAX_VALUE) {
            throw new IllegalArgumentException("size exceeds 2GiB limit");
        }
        return (int) additional;
    }
}
