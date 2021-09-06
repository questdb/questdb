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
import io.questdb.std.CharSequenceHashSet;
import io.questdb.std.DirectLongList;
import io.questdb.std.NumericException;
import io.questdb.std.Unsafe;
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
    public static final int MAX_STRING_LENGTH = 12;
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

    private static final char[] base32 = {
            '0', '1', '2', '3', '4', '5', '6', '7',
            '8', '9', 'b', 'c', 'd', 'e', 'f', 'g',
            'h', 'j', 'k', 'm', 'n', 'p', 'q', 'r',
            's', 't', 'u', 'v', 'w', 'x', 'y', 'z'
    };

    public static long bitmask(int count, int shift) {
        // e.g. 3, 4 -> 1110000
        return ((1L << count) - 1) << shift;
    }

    public static long fromStringNl(CharSequence geohash, int start, int length) throws NumericException {
        if (length <= 0 || geohash == null || geohash.length() == 0) {
            return GeoHashes.NULL;
        }
        return fromString(geohash, start, start + Math.min(length, MAX_STRING_LENGTH));
    }

    public static long fromBitStringNl(CharSequence bits, int start) throws NumericException {
        int len = bits.length();
        if (len - start <= 0) {
            return NULL;
        }
        return fromBitString(bits, start, Math.min(bits.length(), ColumnType.GEO_HASH_MAX_BITS_LENGTH + start));
    }

    public static long fromBitString(CharSequence bits, int start) throws NumericException {
        return fromBitString(bits, start, Math.min(bits.length(),  ColumnType.GEO_HASH_MAX_BITS_LENGTH + start));
    }

    private static long fromBitString(CharSequence bits, int start, int limit) throws NumericException {
        long result = 0;
        for (int i = start; i < limit; i++) {
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

    public static long fromCoordinatesUnsafe(double lat, double lng, int bits) {
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

    public static long fromCoordinates(double lat, double lng, int bits) throws NumericException {
        if (lat < -90.0 || lat > 90.0) {
            throw NumericException.INSTANCE;
        }
        if (lng < -180.0 || lng > 180.0) {
            throw NumericException.INSTANCE;
        }
        if (bits < 0 || bits > ColumnType.GEO_HASH_MAX_BITS_LENGTH) {
            throw NumericException.INSTANCE;
        }
        return fromCoordinatesUnsafe(lat, lng, bits);
    }

    public static long fromString(CharSequence hash) throws NumericException {
        return fromString(hash, 0, hash.length());
    }

    public static long fromString(CharSequence hash, int start, int end) throws NumericException {
        // no bounds/length/nullity checks
        long geohash = 0;
        for (int i = start; i < end; ++i) {
            geohash = appendChar(geohash, hash.charAt(i));
        }
        return geohash;
    }

    private static long appendChar(long hash, char c) throws NumericException {
        if (c >= 48 && c < 123) { // 123 = base32Indexes.length + 48
            byte idx = base32Indexes[c - 48];
            if (idx >= 0) {
                return (hash << 5) | idx;
            }
        }
        throw NumericException.INSTANCE;
    }


    public static boolean isValidChars(CharSequence tok, int start) {
        int idx;
        int len = tok.length();
        for (int i = start; i < len; i++) {
            idx = tok.charAt(i);
            if (idx < 48 || idx > 122 || base32Indexes[idx - 48] == -1) {
                return false;
            }
        }
        return start < len;
    }

    public static boolean isValidBits(CharSequence tok, int start) {
        int idx;
        int len = tok.length();
        for (int i = start; i < len; i++) {
            idx = tok.charAt(i);
            if (idx < '0' || idx > '1') {
                return false;
            }
        }
        return start < len;
    }

    public static void fromStringToBits(final CharSequenceHashSet prefixes, int columnType, final DirectLongList prefixesBits) {
        prefixesBits.clear();
        final int columnSize = ColumnType.sizeOf(columnType);
        final int columnBits = ColumnType.getGeoHashBits(columnType);
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

    public static long fromStringTruncatingNl(CharSequence hash, int start, int end, int toBits) throws NumericException {
        if (start == end) {
            return NULL;
        }
        final int chars = Math.min(end - start, MAX_STRING_LENGTH);
        int fromBits = 5 * chars;
        if (fromBits < toBits) {
            throw NumericException.INSTANCE;
        }
        return ColumnType.truncateGeoHashBits(fromString(hash, start, start + chars), fromBits, toBits);
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
        return ColumnType.truncateGeoHashBits(geohash, actualBits, bits);
    }

    public static long getGeoLong(int type, Function func, Record rec) {
        switch (ColumnType.tagOf(type)) {
            case ColumnType.GEOBYTE:
                return func.getGeoByte(rec);
            case ColumnType.GEOSHORT:
                return func.getGeoShort(rec);
            case ColumnType.GEOINT:
                return func.getGeoInt(rec);
            default:
                return func.getGeoLong(rec);
        }
    }

    public static void toBitString(long hash, int bits, CharSink sink) {
        if (hash != NULL) {
            appendBinaryStringUnsafe(hash, bits, sink);
        }
    }

    public static void appendBinaryStringUnsafe(long hash, int bits, CharSink sink) {
        // Below assertion can happen if there is corrupt metadata
        // which should not happen in production code since reader and writer check table metadata
        assert bits > 0 && bits <= ColumnType.GEO_HASH_MAX_BITS_LENGTH;
        for (int i = bits - 1; i >= 0; --i) {
            sink.put(((hash >> i) & 1) == 1 ? '1' : '0');
        }
    }

    public static void appendChars(long hash, int chars, CharSink sink) {
        if (hash != NULL) {
            appendCharsUnsafe(hash, chars, sink);
        }
    }

    public static void appendCharsUnsafe(long hash, int chars, CharSink sink) {
        // Below assertion can happen if there is corrupt metadata
        // which should not happen in production code since reader and writer check table metadata
        assert chars > 0 && chars <= MAX_STRING_LENGTH;
        for (int i = chars - 1; i >= 0; --i) {
            sink.put(base32[(int) ((hash >> i * 5) & 0x1F)]);
        }
    }
}
