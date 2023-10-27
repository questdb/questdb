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

package io.questdb.cairo;

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.Unsafe;
import io.questdb.std.str.CharSinkBase;

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
    public static final int INT_NULL = -1;
    public static final int MAX_STRING_LENGTH = 12;
    public static final long NULL = -1L;
    public static final short SHORT_NULL = -1;
    // we fill this array with BYTE_NULL (-1)
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
            29, 30, 31, -1, -1, -1, -1, -1  // 78-7A, 'x'..'z'
    };

    private static final char[] base32 = {
            '0', '1', '2', '3', '4', '5', '6', '7',
            '8', '9', 'b', 'c', 'd', 'e', 'f', 'g',
            'h', 'j', 'k', 'm', 'n', 'p', 'q', 'r',
            's', 't', 'u', 'v', 'w', 'x', 'y', 'z'
    };

    public static void addNormalizedGeoPrefix(long hash, int prefixType, int columnType, final LongList prefixes) throws NumericException {
        final int bits = ColumnType.getGeoHashBits(prefixType);
        final int columnSize = ColumnType.sizeOf(columnType);
        final int columnBits = ColumnType.getGeoHashBits(columnType);

        if (hash == NULL || bits > columnBits) {
            throw NumericException.INSTANCE;
        }

        final int shift = columnBits - bits;
        long norm = hash << shift;
        long mask = GeoHashes.bitmask(bits, shift);
        mask |= 1L << (columnSize * 8 - 1); // set the most significant bit to ignore null from prefix matching

        prefixes.add(norm);
        prefixes.add(mask);
    }

    public static void append(long hash, int bits, CharSinkBase<?> sink) {
        if (hash == GeoHashes.NULL) {
            sink.putAscii("null");
        } else {
            sink.putAscii('\"');
            if (bits < 0) {
                GeoHashes.appendCharsUnsafe(hash, -bits, sink);
            } else {
                GeoHashes.appendBinaryStringUnsafe(hash, bits, sink);
            }
            sink.putAscii('\"');
        }
    }


    public static void appendBinary(long hash, int bits, CharSinkBase<?> sink) {
        if (hash != NULL) {
            appendBinaryStringUnsafe(hash, bits, sink);
        }
    }

    public static void appendBinaryStringUnsafe(long hash, int bits, CharSinkBase<?> sink) {
        // Below assertion can happen if there is corrupt metadata
        // which should not happen in production code since reader and writer check table metadata
        assert bits > 0 && bits <= ColumnType.GEOLONG_MAX_BITS;
        for (int i = bits - 1; i >= 0; --i) {
            sink.putAscii(((hash >> i) & 1) == 1 ? '1' : '0');
        }
    }

    public static void appendChars(long hash, int chars, CharSinkBase<?> sink) {
        if (hash != NULL) {
            appendCharsUnsafe(hash, chars, sink);
        }
    }

    public static void appendCharsUnsafe(long hash, int chars, CharSinkBase<?> sink) {
        // Below assertion can happen if there is corrupt metadata
        // which should not happen in production code since reader and writer check table metadata
        assert chars > 0 && chars <= MAX_STRING_LENGTH;
        for (int i = chars - 1; i >= 0; --i) {
            sink.putAscii(base32[(int) ((hash >> i * 5) & 0x1F)]);
        }
    }

    public static long bitmask(int count, int shift) {
        // e.g. 3, 4 -> 1110000
        return ((1L << count) - 1) << shift;
    }

    public static byte encodeChar(char c) {
        // element at index 11 is -1
        return base32Indexes[c > 47 && c < 123 ? c - 48 : 11];
    }

    public static long fromBitString(CharSequence bits, int start) throws NumericException {
        return fromBitString(bits, start, Math.min(bits.length(), ColumnType.GEOLONG_MAX_BITS + start));
    }

    public static long fromBitStringNl(CharSequence bits, int start) throws NumericException {
        int len = bits.length();
        if (len - start <= 0) {
            return NULL;
        }
        return fromBitString(bits, start, Math.min(bits.length(), ColumnType.GEOLONG_MAX_BITS + start));
    }

    public static long fromCoordinatesDeg(double lat, double lon, int bits) throws NumericException {
        if (lat < -90.0 || lat > 90.0) {
            throw NumericException.INSTANCE;
        }
        if (lon < -180.0 || lon > 180.0) {
            throw NumericException.INSTANCE;
        }
        if (bits < 0 || bits > ColumnType.GEOLONG_MAX_BITS) {
            throw NumericException.INSTANCE;
        }
        return fromCoordinatesDegUnsafe(lat, lon, bits);
    }

    public static long fromCoordinatesDegUnsafe(double lat, double lon, int bits) {
        long latq = (long) Math.scalb((lat + 90.0) / 180.0, 32);
        long lngq = (long) Math.scalb((lon + 180.0) / 360.0, 32);
        return Numbers.interleaveBits(latq, lngq) >>> (64 - bits);
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

    public static long fromStringNl(CharSequence geohash, int start, int length) throws NumericException {
        if (length <= 0 || geohash == null || geohash.length() == 0) {
            return GeoHashes.NULL;
        }
        return fromString(geohash, start, start + Math.min(length, MAX_STRING_LENGTH));
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
        return widen(fromString(hash, start, start + chars), fromBits, toBits);
    }

    public static long fromStringTruncatingNl(long lo, long hi, int bits) throws NumericException {
        if (lo == hi) {
            return NULL;
        }
        final int chars = Math.min((int) (hi - lo), MAX_STRING_LENGTH);
        int actualBits = 5 * chars;
        if (actualBits < bits || bits == 0) {
            throw NumericException.INSTANCE;
        }
        long geohash = 0;
        for (long p = lo, limit = p + chars; p < limit; p++) {
            geohash = appendChar(geohash, (char) Unsafe.getUnsafe().getByte(p)); // base32
        }
        return widen(geohash, actualBits, bits);
    }

    public static int getBitFlags(int columnType) {
        if (!ColumnType.isGeoHash(columnType)) {
            return 0;
        }
        final int bits = ColumnType.getGeoHashBits(columnType);
        if (bits > 0 && bits % 5 == 0) {
            // It's 5 bit per char. If it's integer number of chars value to be serialized as chars
            return -bits / 5;
        }
        // Value to be serialized as bit array.
        return bits;
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

    public static boolean isValidChars(CharSequence tok, int start) {
        char c;
        int len = tok.length();
        for (int i = start; i < len; i++) {
            c = tok.charAt(i);
            if (encodeChar(c) == -1) {
                return false;
            }
        }
        return start < len;
    }

    public static long widen(long hash, int fromBits, int toBits) {
        return hash >> (fromBits - toBits);
    }

    private static long appendChar(long hash, char c) throws NumericException {
        final byte idx = encodeChar(c);
        if (idx > -1) {
            return (hash << 5) | idx;
        }
        throw NumericException.INSTANCE;
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
}
