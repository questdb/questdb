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

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.std.*;
import io.questdb.std.str.CharSink;

public class GeoHashes {

    // geohash null value: -1
    // we use the highest bit of every storage size (byte, short, int, long)
    // to indicate null value. When a null value is cast down, nullity is
    // preserved, i.e. highest bit remains set:
    //     long nullLong = -1L;
    //     short nullShort = (short) nullLong;
    //     nullShort == nullLong;
    // in addition, -1 is the first negative non geohash value.
    public static final byte BYTE_NULL = -1;
    public static final short SHORT_NULL = -1;
    public static final int INT_NULL = -1;
    public static final long NULL = -1L;
    public static final int MAX_BITS_LENGTH = 60;
    public static final int MAX_STRING_LENGTH = 12;
    private static final int[] GEO_TYPE_SIZE_POW2 = new int[MAX_BITS_LENGTH + 1];

    static final byte[] base32Indexes = {
            0, 1, 2, 3, 4, 5, 6, 7,         // 30-37, '0'..'7'
            8, 9, -1, -1, -1, -1, -1, -1,   // 38-2F, '8','9'
            -1, -1, 10, 11, 12, 13, 14, 15, // 40-47, 'B'..'G'
            16, -1, 17, 18, -1, 19, 20, -1, // 48-4F, 'H','J','K','M','N'
            21, 22, 23, 24, 25, 26, 27, 28, // 50-57, 'P'..'W'
            29, 30, 31, -1, -1, -1, -1, -1, // 58-5F, 'X'..'Z'
            -1, -1, 10, 11, 12, 13, 14, 15, // 60-67, 'b'..'g'
            16, -1, 17, 18, -1, 19, 20, -1, // 68-6F, 'h','j','k','m','n'
            21, 22, 23, 24, 25, 26, 27, 28, // 70-77, 'p'..'w'
            29, 30, 31                      // 78-7A, 'x'..'z'
    };

    private static final int BITS_OFFSET = 8;
    private static final char[] base32 = {
            '0', '1', '2', '3', '4', '5', '6', '7',
            '8', '9', 'b', 'c', 'd', 'e', 'f', 'g',
            'h', 'j', 'k', 'm', 'n', 'p', 'q', 'r',
            's', 't', 'u', 'v', 'w', 'x', 'y', 'z'
    };

    static {
        for (int bits = 1; bits <= MAX_BITS_LENGTH; bits++) {
            GEO_TYPE_SIZE_POW2[bits] = Numbers.msb(Numbers.ceilPow2(((bits + Byte.SIZE) & -Byte.SIZE)) >> 3);
        }
    }

    public static boolean isValidChar(char ch) {
        int idx = ch - 48;
        return idx >= 0 && idx < base32Indexes.length && base32Indexes[idx] != -1;
    }

    public static boolean isValidChars(CharSequence tok, int start) {
        if (start < 0 || start > tok.length()) {
            throw new IndexOutOfBoundsException();
        }
        int idx;
        for (int i = start, n = tok.length(), excessIdx = base32Indexes.length; i < n; i++) {
            idx = tok.charAt(i) - 48;
            if (idx < 0 || idx >= excessIdx || base32Indexes[idx] == -1) {
                return false;
            }
        }
        return true;
    }

    public static boolean isValidBits(CharSequence tok, int start) {
        if (start < 0 || start > tok.length()) {
            throw new IndexOutOfBoundsException();
        }
        int idx;
        for (int i = start, n = tok.length(); i < n; i++) {
            idx = tok.charAt(i) - 48;
            if (idx < 0 || idx > 1) {
                return false;
            }
        }
        return true;
    }

    public static long bitmask(int count, int shift) {
        // e.g. 3, 4 -> 1110000
        return ((1L << count) - 1) << shift;
    }

    public static long fromBitString(CharSequence bits) throws NumericException {
        return fromBitString(bits, 0);
    }

    public static long fromBitString(CharSequence bits, int start) throws NumericException {
        if (start < 0 || bits.length() - start > MAX_BITS_LENGTH) {
            throw NumericException.INSTANCE;
        }
        long result = 0;
        for (int i = start, n = bits.length(); i < n; i++) {
            switch (bits.charAt(i)) {
                case '0':
                    result = result << 1;
                    break;
                case '1':
                    result = (result << 1) | 1;
                    break;
                default:
                    throw NumericException.INSTANCE;
            }
        }
        return result;
    }

    public static long fromCoordinates(double lat, double lng, int bits) throws NumericException {
        if (lat < -90.0 || lat > 90.0) {
            throw NumericException.INSTANCE;
        }
        if (lng < -180.0 || lng > 180.0) {
            throw NumericException.INSTANCE;
        }
        assert bits > 0 && bits <= MAX_BITS_LENGTH;
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

    public static long fromStringTruncatingNl(CharSequence hash, int start, int end, int bits) throws NumericException {
        if (start == end) {
            return NULL;
        }
        final int chars = Math.min(end - start, MAX_STRING_LENGTH);
        int actualBits = 5 * chars;
        if (actualBits < bits) {
            throw NumericException.INSTANCE;
        }
        return fromString(hash, start, start + chars) >>> (actualBits - bits);
    }

    public static long fromStringTruncatingNl(long lo, long hi, int bits) throws NumericException {
        if (lo == hi) {
            return NULL;
        }
        final int chars = Math.min((int) (hi - lo), MAX_STRING_LENGTH);
        int actualBits = 5 * chars;
        if (actualBits < bits) {
            throw NumericException.INSTANCE;
        }
        long geohash = 0;
        for (long p = lo, limit = p + chars; p < limit; p++) {
            geohash = appendChar(geohash, (char) Unsafe.getUnsafe().getByte(p)); // base32
        }
        return geohash >>> (actualBits - bits);
    }

    public static long fromStringNl(CharSequence geohash, int start, int length) throws NumericException {
        if (length <= 0 || geohash == null || geohash.length() == 0) {
            return GeoHashes.NULL;
        }
        return fromString(geohash, start, start + (length <= MAX_STRING_LENGTH ? length : MAX_STRING_LENGTH));
    }

    public static long fromString(CharSequence hash, int start, int end) throws NumericException {
        // no bounds/length/nullity checks
        long geohash = 0;
        for (int i = start; i < end; ++i) {
            geohash = appendChar(geohash, hash.charAt(i));
        }
        return geohash;
    }

    private static long appendChar(long geohash, char c) throws NumericException {
        if (c >= 48 && c < 123) { // 123 = base32Indexes.length + 48
            byte idx = base32Indexes[c - 48];
            if (idx >= 0) {
                return (geohash << 5) | idx;
            }
        }
        throw NumericException.INSTANCE;
    }

    public static void fromStringToBits(final CharSequenceHashSet prefixes, int columnType, final DirectLongList prefixesBits) {
        prefixesBits.clear();
        final int columnSize = ColumnType.sizeOf(columnType);
        final int columnBits = GeoHashes.getBitsPrecision(columnType);
        for (int i = 0, sz = prefixes.size(); i < sz; i++) {
            try {
                final CharSequence prefix = prefixes.get(i);
                if (prefix == null || prefix.length() == 0) {
                    continue;
                }
                final long hash = fromString(prefix, 0, prefix.length());
                final int bits = 5 * prefix.length();
                final int shift = columnBits - bits;
                long norm = hash << shift;
                long mask = bitmask(bits, shift);
                mask |= 1L << (columnSize * 8 - 1); // set the most significant bit to ignore null from prefix matching
                // if the prefix is more precise than hashes,
                // exclude it from matching
                if (bits > columnBits) {
                    norm = 0L;
                    mask = -1L;
                }
                prefixesBits.add(norm);
                prefixesBits.add(mask);
            } catch (NumericException e) {
                // Skip invalid geo hashes
            }
        }
    }

    public static int hashSize(long hashz) {
        return (int) (hashz >>> MAX_BITS_LENGTH);
    }

    public static long toHash(long hashz) {
        return hashz & 0x0fffffffffffffffL;
    }

    public static long toHashWithSize(long hash, int length) {
        return (((long) length) << 60L) + hash;
    }

    public static int getBitsPrecision(int type) {
        assert ColumnType.tagOf(type) == ColumnType.GEOHASH; // This maybe relaxed in the future
        return (byte) ((type >> BITS_OFFSET) & 0xFF);
    }

    public static int setBitsPrecision(int type, int bits) {
        return (type & ~(0xFF << 8)) | (Byte.toUnsignedInt((byte) bits) << 8);
    }

    public static int sizeOf(int columnType) {
        assert ColumnType.tagOf(columnType) == ColumnType.GEOHASH;
        int bits = getBitsPrecision(columnType);
        if (bits <= MAX_BITS_LENGTH && bits > 0) {
            return 1 << GEO_TYPE_SIZE_POW2[bits];
        }
        return -1; // Corrupt metadata
    }

    public static int pow2SizeOf(int columnType) {
        assert ColumnType.tagOf(columnType) == ColumnType.GEOHASH;
        int bits = getBitsPrecision(columnType);
        if (bits <= MAX_BITS_LENGTH) {
            return GEO_TYPE_SIZE_POW2[bits];
        }
        return -1; // Corrupt metadata
    }

    public static void toBitString(long hash, int bits, CharSink sink) {
        if (hash != NULL) {
            // Below assertion can happen if there is corrupt metadata
            // which should not happen in production code since reader and writer check table metadata
            assert bits > 0 && bits <= MAX_BITS_LENGTH;
            for (int i = bits - 1; i >= 0; --i) {
                sink.put(((hash >> i) & 1) == 1 ? '1' : '0');
            }
        }
    }

    public static void toString(long hash, int chars, CharSink sink) {
        if (hash != NULL) {
            // Below assertion can happen if there is corrupt metadata
            // which should not happen in production code since reader and writer check table metadata
            assert chars > 0 && chars <= MAX_STRING_LENGTH;
            for (int i = chars - 1; i >= 0; --i) {
                sink.put(base32[(int) ((hash >> i * 5) & 0x1F)]);
            }
        }
    }

    public static long getGeoLong(int type, Function func, Record rec) {
        switch (ColumnType.sizeOf(type)) {
            case Byte.BYTES:
                return func.getGeoHashByte(rec);
            case Short.BYTES:
                return func.getGeoHashShort(rec);
            case Integer.BYTES:
                return func.getGeoHashInt(rec);
            default:
                return func.getGeoHashLong(rec);
        }
    }
}
