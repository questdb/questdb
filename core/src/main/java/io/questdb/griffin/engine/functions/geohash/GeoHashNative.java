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

import io.questdb.cairo.GeoHashExtra;
import io.questdb.std.CharSequenceHashSet;
import io.questdb.std.DirectLongList;
import io.questdb.std.NumericException;
import io.questdb.std.str.CharSink;

public class GeoHashNative {
    public static final int MAX_STRING_LENGTH = 12;
    public static final int MAX_BITS_LENGTH = 60;
    private static final char MAX_CHAR = 'z' + 1;
    private static final char MIN_CHAR = '0' - 1;

    static final byte[] base32Indexes = {
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

    public static long fromString(CharSequence hash, int parseLen) throws NumericException {
        if (parseLen == 0 || parseLen > MAX_STRING_LENGTH || hash == null || parseLen > hash.length()) {
            throw NumericException.INSTANCE;
        }
        long output = 0;
        for (int i = 0; i < parseLen; ++i) {
            char c = hash.charAt(i);
            if (c > MIN_CHAR && c < MAX_CHAR) {
                byte idx = base32Indexes[(int) c - 48];
                if (idx >= 0) {
                    output = (output << 5) | idx;
                    continue;
                }
            }
            throw NumericException.INSTANCE;
        }
        return output;
    }

    public static long fromStringNl(CharSequence hash) throws NumericException {
        if (hash == null || hash.length() == 0) {
            return GeoHashExtra.NULL;
        }
        return fromString(hash, hash.length());
    }

    public static long fromCoordinates(double lat, double lng, int bits) throws NumericException {
        if (lat < -90.0 || lat > 90.0) {
            throw NumericException.INSTANCE;
        }
        if (lng < -180.0 || lng > 180.0) {
            throw NumericException.INSTANCE;
        }
        assert bits > 0 && bits <= GeoHashNative.MAX_BITS_LENGTH;
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
        return result;
    }

    public static long fromBitString(CharSequence bits) throws NumericException {
        if (bits.length() > 60) {
            throw NumericException.INSTANCE;
        }
        long result = 0;
        for (int i = 0; i < bits.length(); i++) {
            char c = bits.charAt(i);
            if (c == '1') {
                result = (result << 1) | 1;
            } else if (c == '0') {
                result = result << 1;
            } else {
                throw NumericException.INSTANCE;
            }
        }
        return result;
    }

    public static void toBitString(long hash, int bits, CharSink sink) {
        if (hash != GeoHashExtra.NULL) {
            assert bits > 0 && bits <= GeoHashNative.MAX_BITS_LENGTH;
            for (int i = bits - 1; i >= 0; --i) {
                sink.put(((hash >> i) & 1) == 1? '1' : '0');
            }
        }
    }

    public static void toString(long hash, int chars, CharSink sink) {
        if (hash != GeoHashExtra.NULL) {
            assert chars >0 && chars <= GeoHashNative.MAX_STRING_LENGTH;
            for (int i = chars - 1; i >= 0; --i) {
                sink.put(base32[(int) ((hash >> i * 5) & 0x1F)]);
            }
        }
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

    public static void fromStringToBits(final CharSequenceHashSet prefixes, int lo, int hi, final DirectLongList prefixesBits) {
        prefixesBits.clear();
        assert hi <= prefixes.size();
        for (int i = lo; i < hi; i++) {
            try {
                final CharSequence prefix = prefixes.get(i);
                final long hash = fromStringNl(prefix);
                if (hash == GeoHashExtra.NULL) {
                    continue;
                }
                final int bits = 5 * prefix.length();
                final int shift = 8 * 5 - bits;
                final long norm = hash << shift;
                long mask = bitmask(bits, shift);
                mask |= 1L << 63; // set the most significant bit to ignore null from prefix matching
                prefixesBits.add(norm);
                prefixesBits.add(mask);
            } catch (NumericException e) {
                // Skip invalid geo hashes
            }
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
