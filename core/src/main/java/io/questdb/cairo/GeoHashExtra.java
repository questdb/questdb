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

import io.questdb.std.Numbers;

public class GeoHashExtra {
    private static final int BITS_OFFSET = 8;

    public static int setBitsPrecision(int type, int bits) {
        assert bits >= 0 && bits < 61;
        return (type & ~(0xFF << BITS_OFFSET)) | (bits << BITS_OFFSET);
    }

    public static int getBitsPrecision(int type) {
        return (type >> BITS_OFFSET) & 0xFF;
    }

    public static int storageSizeInBits(int type) {
        int size = GeoHashExtra.getBitsPrecision(type);
        if (size == 0) {
            return 64; // variable length geohash
        }
        size = (size + Byte.SIZE - 1) & -Byte.SIZE; // round up to 8 bit
        return Numbers.ceilPow2(size); // next pow of 2
    }

    public static int storageSizeInPow2(int type) {
        return Numbers.msb(storageSizeInBits(type) / Byte.SIZE);
    }

}
