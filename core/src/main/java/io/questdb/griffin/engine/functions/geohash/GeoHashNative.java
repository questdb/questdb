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

package io.questdb.griffin.engine.functions.geohash;

import io.questdb.std.CharSequenceHashSet;
import io.questdb.std.DirectLongList;

public class GeoHashNative {
    static final int[] base32Indexes = {
            0, 1, 2, 3, 4, 5, 6, 7,        // 30-37, '0'..'7'
            8, 9, -1, -1, -1, -1, -1, -1,  // 38-2F, '8','9'
            -1, -1, 10, 11, 12, 13, 14, 15, // 40-47, 'B'..'G'
            16, -1, 17, 18, -1, 19, 20, -1, // 48-4F, 'H','J','K','M','N'
            21, 22, 23, 24, 25, 26, 27, 28, // 50-57, 'P'..'W'
            29, 30, 31, -1, -1, -1, -1, -1, // 58-5F, 'X'..'Z'
            -1, -1, 10, 11, 12, 13, 14, 15, // 60-67, 'b'..'g'
            16, -1, 17, 18, -1, 19, 20, -1, // 68-6F, 'h','j','k','m','n'
            21, 22, 23, 24, 25, 26, 27, 28, // 70-77, 'p'..'w'
            29, 30, 31                      // 78-7A, 'x'..'z'
    };

    private static final char[] base32 = {
            '0', '1', '2', '3', '4', '5', '6', '7',
            '8', '9', 'b', 'c', 'd', 'e', 'f', 'g',
            'h', 'j', 'k', 'm', 'n', 'p', 'q', 'r',
            's', 't', 'u', 'v', 'w', 'x', 'y', 'z'
    };

    public static long fromString(CharSequence hash) {
        long output = 0;
        for (int i = 0; i < hash.length(); ++i) {
            char c = hash.charAt(i);
            int idx = base32Indexes[(int) c - 48];
            if (idx < 0) {
                throw new IllegalArgumentException(hash.toString());
            }
            for (int bits = 4; bits >= 0; --bits) {
                output <<= 1;
                output |= ((idx >> bits) & 1) != 0 ? 1 : 0;
            }
        }
        return (((long) hash.length()) << 60L) + output;
    }

    public static long fromCoordinates(double lat, double lng, int bits) {
        if (lat < -90.0 || lat > 90.0) {
            throw new IllegalArgumentException("lat range is [-90, 90]");
        }
        if (lng < -180.0 || lng > 180.0) {
            throw new IllegalArgumentException("lat range is [-180, 180]");
        }
        if (bits % 5 != 0) {
            throw new IllegalArgumentException("bits range is [0, 60] and a multiple of 5");
        }
        double minLat = -90, maxLat = 90;
        double minLng = -180, maxLng = 180;
        long result = 0;
        for (int i = 0; i < bits; ++i) {
            if (i % 2 == 0) {
                double midpoint = (minLng + maxLng) / 2;
                if (lng < midpoint) {
                    result <<= 1;
                    maxLng = midpoint;
                } else {
                    result = result << 1 | 1;
                    minLng = midpoint;
                }
            } else {
                double midpoint = (minLat + maxLat) / 2;
                if (lat < midpoint) {
                    result <<= 1;
                    maxLat = midpoint;
                } else {
                    result = result << 1 | 1;
                    minLat = midpoint;
                }
            }
        }
        return ((bits / 5L) << 60L) + result;
    }

    public static long fromBitString(CharSequence bits) {
        if (bits.length() % 5 != 0) {
            throw new IllegalArgumentException("length must be a multiple of 5 from 0 to 60");
        }
        long result = 0;
        for (int i = 0; i < bits.length(); i++) {
            char c = bits.charAt(i);
            if (c == '1') {
                result = (result << 1) | 1;
            } else {
                result = result << 1;
            }
        }
        return ((bits.length() / 5L) << 60L) + result;
    }

    public static String toString(long hash, int precision) {
        if (precision < 0 || precision > 12) {
            throw new IllegalArgumentException("precision range is [0, 12]");
        }
        char[] chars = new char[precision];
        for (int i = precision - 1; i >= 0; i--) {
            chars[i] = base32[(int) (hash & 31)];
            hash >>= 5;
        }
        return new String(chars);
    }

    public static String toString(long hashz) {
        return toString(hashz, hashSize(hashz));
    }

    public static long bitmask(int count, int shift) {
        // e.g. 3, 4 -> 1110000
        return ((1L << count) - 1) << shift;
    }

    public static long toHash(long hashz) {
        return hashz & 0x0fffffffffffffffL;
    }

    public static int hashSize(long hashz) {
        return (int) (hashz >>> 60);
    }

    public static long toHashWithSize(long hash, int length) {
        return (((long) length) << 60L) + hash;
    }

    public static void fromStringToBits(final CharSequenceHashSet prefixes, final DirectLongList prefixesBits) {
        prefixesBits.clear();
        // skip first (search column name) element
        for (int i = 1, sz = prefixes.size(); i < sz; i++) {
            final CharSequence prefix = prefixes.get(i);
            final long hashz = fromString(prefix);
            final int bits = 5 * (int) (hashz >>> 60);
            final int shift = 8 * 5 - bits;
            final long norm = (hashz & 0x0fffffffffffffffL) << shift;
            final long mask = bitmask(bits, shift);

            prefixesBits.add(norm);
            prefixesBits.add(mask);
        }
    }

    public static native void latesByAndFilterPrefix(
            long keysMemory,
            long keysMemorySize,
            long valuesMemory,
            long valuesMemorySize,
            long argsMemory,
            long unIndexedNullCount,
            long maxValue,
            long minValue,
            int partitionIndex,
            int blockValueCountMod,
            long hashesAddress,
            int hashLength,
            long prefixesAddress,
            long prefixesCount
    );

    public static native long slideFoundBlocks(long argsAddress, long argsCount);

    public static native long iota(long address, long size, long init);

}
