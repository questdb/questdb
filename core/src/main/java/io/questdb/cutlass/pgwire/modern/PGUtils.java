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

package io.questdb.cutlass.pgwire.modern;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.sql.Record;
import io.questdb.cutlass.pgwire.BadProtocolException;
import io.questdb.std.BinarySequence;
import io.questdb.std.Chars;
import io.questdb.std.Long256;
import io.questdb.std.Long256Impl;
import io.questdb.std.Numbers;
import io.questdb.std.Uuid;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8s;

class PGUtils {
    private static final int MAX_BYTE_TEXT_LEN = String.valueOf(Byte.MIN_VALUE).length();
    private static final int MAX_CHAR_TEXT_LEN = 3;
    private static final int MAX_DATE_TEXT_LEN = 28; // "292278994-08-17 07:12:55.807"
    private static final int MAX_DOUBLE_TEXT_LEN = 24;
    private static final int MAX_FLOAT_TEXT_LEN = 16;
    private static final int MAX_GEOBYTE_TEXT_LEN = 8;
    private static final int MAX_GEOINT_TEXT_LEN = 32;
    private static final int MAX_GEOLONG_TEXT_LEN = 64;
    private static final int MAX_GEOSHORT_TEXT_LEN = 16;
    private static final int MAX_INT_TEXT_LEN = String.valueOf(Integer.MIN_VALUE).length();
    private static final int MAX_IPv4_TEXT_LEN = 15; // "255.255.255.255"
    private static final int MAX_LONG256_TEXT_LEN = 66; // "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
    private static final int MAX_LONG_TEXT_LEN = String.valueOf(Long.MIN_VALUE).length();
    private static final int MAX_SHORT_TEXT_LEN = String.valueOf(Short.MIN_VALUE).length();
    private static final int MAX_TIMESTAMP_TEXT_LEN = 28; // "294247-01-10 04:00:54.775807"
    private static final int MAX_UUID_TEXT_LEN = 36;

    private PGUtils() {
    }

    /**
     * Returns the size of the serialized value in bytes, or -1 if the type is not supported.
     *
     * @throws BadProtocolException if the binary value exceeds maxBlobSize
     */
    public static int calculateColumnBinSize(
            PGPipelineEntry pipelineEntry,
            Record record,
            int columnIndex,
            int typeTag,
            int bitFlags,
            long maxBlobSize
    ) throws BadProtocolException {
        switch (typeTag) {
            case ColumnType.NULL:
                return Integer.BYTES;
            case ColumnType.BOOLEAN:
                return Integer.BYTES + Byte.BYTES;
            case ColumnType.BYTE:
            case ColumnType.SHORT:
                return Integer.BYTES + Short.BYTES;
            case ColumnType.CHAR:
                final char charValue = record.getChar(columnIndex);
                return charValue == 0 ? Integer.BYTES : Integer.BYTES + Chars.charBytes(charValue);
            case ColumnType.IPv4:
                final int ipValue = record.getIPv4(columnIndex);
                return ipValue != Numbers.IPv4_NULL ? Integer.BYTES + Numbers.sinkSizeIPv4(ipValue) : Integer.BYTES;
            case ColumnType.INT:
                final int value = record.getInt(columnIndex);
                return value != Numbers.INT_NULL ? Integer.BYTES + Integer.BYTES : Integer.BYTES;
            case ColumnType.LONG:
                final long longValue = record.getLong(columnIndex);
                return longValue != Numbers.LONG_NULL ? Integer.BYTES + Long.BYTES : Integer.BYTES;
            case ColumnType.DATE:
                final long dateValue = record.getDate(columnIndex);
                return dateValue != Numbers.LONG_NULL ? Integer.BYTES + Long.BYTES : Integer.BYTES;
            case ColumnType.TIMESTAMP:
                final long tsValue = record.getTimestamp(columnIndex);
                return tsValue != Numbers.LONG_NULL ? Integer.BYTES + Long.BYTES : Integer.BYTES;
            case ColumnType.FLOAT:
                final float floatValue = record.getFloat(columnIndex);
                return Float.isNaN(floatValue) ? Integer.BYTES : Integer.BYTES + Float.BYTES;
            case ColumnType.DOUBLE:
                final double doubleValue = record.getDouble(columnIndex);
                return Double.isNaN(doubleValue) ? Integer.BYTES : Integer.BYTES + Double.BYTES;
            case ColumnType.UUID:
                final long lo = record.getLong128Lo(columnIndex);
                final long hi = record.getLong128Hi(columnIndex);
                return Uuid.isNull(lo, hi) ? Integer.BYTES : Integer.BYTES + Long.BYTES * 2;
            case ColumnType.LONG256:
                final Long256 long256Value = record.getLong256A(columnIndex);
                return Long256Impl.isNull(long256Value) ? Integer.BYTES : Integer.BYTES + Numbers.hexDigitsLong256(long256Value);
            case ColumnType.GEOBYTE:
                return geoHashBytes(record.getGeoByte(columnIndex), bitFlags);
            case ColumnType.GEOSHORT:
                return geoHashBytes(record.getGeoShort(columnIndex), bitFlags);
            case ColumnType.GEOINT:
                return geoHashBytes(record.getGeoInt(columnIndex), bitFlags);
            case ColumnType.GEOLONG:
                return geoHashBytes(record.getGeoLong(columnIndex), bitFlags);
            case ColumnType.VARCHAR:
                final Utf8Sequence vcValue = record.getVarcharA(columnIndex);
                return vcValue == null ? Integer.BYTES : Integer.BYTES + vcValue.size();
            case ColumnType.STRING:
                final CharSequence strValue = record.getStrA(columnIndex);
                return strValue == null ? Integer.BYTES : Integer.BYTES + Utf8s.utf8Bytes(strValue);
            case ColumnType.SYMBOL:
                final CharSequence symValue = record.getSymA(columnIndex);
                return symValue == null ? Integer.BYTES : Integer.BYTES + Utf8s.utf8Bytes(symValue);
            case ColumnType.BINARY:
                BinarySequence sequence = record.getBin(columnIndex);
                if (sequence == null) {
                    return Integer.BYTES;
                } else {
                    long blobSize = sequence.length();
                    if (blobSize < maxBlobSize) {
                        return Integer.BYTES + (int) blobSize;
                    } else {
                        throw BadProtocolException.instance(pipelineEntry)
                                .put("blob is too large [blobSize=").put(blobSize)
                                .put(", maxBlobSize=").put(maxBlobSize)
                                .put(", columnIndex=").put(columnIndex)
                                .put(']');
                    }
                }
            default:
                assert false : "unsupported type: " + typeTag;
                return -1;
        }
    }

    /**
     * Returns an upper estimate for the column value in text format.
     * For efficiency purposes, we don't bother with null checks for fixed-size types.
     * Example: a long value is estimated as 20 chars (Long.MIN_VALUE).
     */
    public static long estimateColumnTxtSize(
            Record record,
            int columnIndex,
            int typeTag
    ) {
        switch (typeTag) {
            case ColumnType.NULL:
                return Integer.BYTES;
            case ColumnType.BOOLEAN:
                return Integer.BYTES + Byte.BYTES;
            case ColumnType.BYTE:
                return Integer.BYTES + MAX_BYTE_TEXT_LEN;
            case ColumnType.SHORT:
                return Integer.BYTES + MAX_SHORT_TEXT_LEN;
            case ColumnType.CHAR:
                return Integer.BYTES + MAX_CHAR_TEXT_LEN;
            case ColumnType.IPv4:
                return Integer.BYTES + MAX_IPv4_TEXT_LEN;
            case ColumnType.INT:
                return Integer.BYTES + MAX_INT_TEXT_LEN;
            case ColumnType.LONG:
                return Integer.BYTES + MAX_LONG_TEXT_LEN;
            case ColumnType.DATE:
                return Integer.BYTES + MAX_DATE_TEXT_LEN;
            case ColumnType.TIMESTAMP:
                return Integer.BYTES + MAX_TIMESTAMP_TEXT_LEN;
            case ColumnType.FLOAT:
                return Integer.BYTES + MAX_FLOAT_TEXT_LEN;
            case ColumnType.DOUBLE:
                return Integer.BYTES + MAX_DOUBLE_TEXT_LEN;
            case ColumnType.UUID:
                return Integer.BYTES + MAX_UUID_TEXT_LEN;
            case ColumnType.LONG256:
                return Integer.BYTES + MAX_LONG256_TEXT_LEN;
            case ColumnType.GEOBYTE:
                return Integer.BYTES + MAX_GEOBYTE_TEXT_LEN;
            case ColumnType.GEOSHORT:
                return Integer.BYTES + MAX_GEOSHORT_TEXT_LEN;
            case ColumnType.GEOINT:
                return Integer.BYTES + MAX_GEOINT_TEXT_LEN;
            case ColumnType.GEOLONG:
                return Integer.BYTES + MAX_GEOLONG_TEXT_LEN;
            case ColumnType.VARCHAR:
                final Utf8Sequence vcValue = record.getVarcharA(columnIndex);
                return vcValue == null ? Integer.BYTES : Integer.BYTES + vcValue.size();
            case ColumnType.STRING:
                final CharSequence strValue = record.getStrA(columnIndex);
                // take rough upper estimate based on the string length
                return strValue == null ? Integer.BYTES : Integer.BYTES + 3L * strValue.length();
            case ColumnType.SYMBOL:
                final CharSequence symValue = record.getSymA(columnIndex);
                // take rough upper estimate based on the string length
                return symValue == null ? Integer.BYTES : Integer.BYTES + 3L * symValue.length();
            case ColumnType.BINARY:
                BinarySequence sequence = record.getBin(columnIndex);
                return sequence == null ? Integer.BYTES : Integer.BYTES + sequence.length();
            default:
                assert false : "unsupported type: " + typeTag;
                return -1;
        }
    }

    private static int geoHashBytes(long value, int bitFlags) {
        if (value == GeoHashes.NULL) {
            return Integer.BYTES;
        } else {
            assert bitFlags > 0;
            // chars or bits
            return Integer.BYTES + bitFlags;
        }
    }
}
