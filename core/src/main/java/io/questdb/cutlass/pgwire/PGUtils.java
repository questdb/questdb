/*+*****************************************************************************
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

package io.questdb.cutlass.pgwire;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.BinarySequence;
import io.questdb.std.Chars;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimal64;
import io.questdb.std.Decimals;
import io.questdb.std.Long256;
import io.questdb.std.Long256Impl;
import io.questdb.std.Numbers;
import io.questdb.std.Uuid;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8s;

public final class PGUtils {
    static final short NUMERIC_NEG = 0x4000;
    static final short NUMERIC_POS = 0x0000;
    private static final int MAX_BYTE_TEXT_LEN = String.valueOf(Byte.MIN_VALUE).length();
    private static final int MAX_CHAR_TEXT_LEN = 3;
    private static final int MAX_DATE_TEXT_LEN = 28; // "292278994-08-17 07:12:55.807"
    private static final int MAX_DOUBLE_TEXT_LEN = 24;
    private static final int MAX_FLOAT_TEXT_LEN = 16;
    private static final int MAX_GEOBYTE_TEXT_LEN = 8;
    private static final int MAX_GEOINT_TEXT_LEN = 32;
    private static final int MAX_GEOLONG_TEXT_LEN = 64;
    private static final int MAX_GEOSHORT_TEXT_LEN = 16;
    // Interval.toSink() renders "('<timestamp>', '<timestamp>')": 2 * MAX_TIMESTAMP_TEXT_LEN (31)
    // for the two ISO timestamps, plus punctuation. Spelled as a literal because
    // MAX_TIMESTAMP_TEXT_LEN is declared below and a simple name cannot be forward-referenced.
    private static final int MAX_INTERVAL_TEXT_LEN = 78;
    private static final int MAX_INT_TEXT_LEN = String.valueOf(Integer.MIN_VALUE).length();
    private static final int MAX_IPv4_TEXT_LEN = 15; // "255.255.255.255"
    private static final int MAX_LONG256_TEXT_LEN = 66; // "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
    private static final int MAX_LONG_TEXT_LEN = String.valueOf(Long.MIN_VALUE).length();
    private static final int MAX_SHORT_TEXT_LEN = String.valueOf(Short.MIN_VALUE).length();
    private static final int MAX_TIMESTAMP_TEXT_LEN = 31; // "294247-01-10 04:00:54.775807123"
    private static final int MAX_UUID_TEXT_LEN = 36;
    private static final int NULL_LITERAL_TEXT_LEN = 4; // "NULL", as ArrayTypeDriver.arrayToPgWire() writes it
    private static final int PG_NUMERIC_FIXED_BIN_SIZE = Integer.BYTES + 4 * Short.BYTES;

    private PGUtils() {
    }

    public static long calculateArrayColBinSizeIncludingHeader(ArrayView array, int notNullCount) {
        int nullCount = array.getCardinality() - notNullCount;
        return calculateArrayHeaderSize(array) + calculateArrayResumeColBinSize(notNullCount, nullCount);
    }

    // does NOT include array header size!
    public static long calculateArrayResumeColBinSize(int notNullCount, int nullCount) {
        return (long) notNullCount *
                (Integer.BYTES // element size
                        + Long.BYTES) + // element value
                (long) nullCount *
                        Integer.BYTES; // element size, zero for NULL value
    }

    /**
     * Returns the size of the serialized value in bytes, or -1 if the type is not supported.
     *
     * @throws PGMessageProcessingException if the binary value exceeds maxBlobSize
     */
    public static long calculateColumnBinSize(
            PGPipelineEntry pipelineEntry,
            SqlExecutionContext sqlExecutionContext,
            Record record,
            int columnIndex,
            int columnType,
            int geohashSize,
            long maxBlobSize,
            int resumePoint
    ) throws PGMessageProcessingException {
        final short typeTag = ColumnType.tagOf(columnType);
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
            case ColumnType.DECIMAL8:
                final byte decimal8 = record.getDecimal8(columnIndex);
                return decimal8 == Decimals.DECIMAL8_NULL
                        ? Integer.BYTES
                        : calculateDecimalBinSize(decimal8, ColumnType.getDecimalScale(columnType));
            case ColumnType.DECIMAL16:
                final short decimal16 = record.getDecimal16(columnIndex);
                return decimal16 == Decimals.DECIMAL16_NULL
                        ? Integer.BYTES
                        : calculateDecimalBinSize(decimal16, ColumnType.getDecimalScale(columnType));
            case ColumnType.DECIMAL32:
                final int decimal32 = record.getDecimal32(columnIndex);
                return decimal32 == Decimals.DECIMAL32_NULL
                        ? Integer.BYTES
                        : calculateDecimalBinSize(decimal32, ColumnType.getDecimalScale(columnType));
            case ColumnType.DECIMAL64:
                final long decimal64 = record.getDecimal64(columnIndex);
                return decimal64 == Decimals.DECIMAL64_NULL
                        ? Integer.BYTES
                        : calculateDecimalBinSize(decimal64, ColumnType.getDecimalScale(columnType));
            case ColumnType.DECIMAL128:
                final Decimal128 decimal128 = sqlExecutionContext.getDecimal128();
                record.getDecimal128(columnIndex, decimal128);
                return calculateDecimalBinSize(
                        decimal128,
                        ColumnType.getDecimalPrecision(columnType),
                        ColumnType.getDecimalScale(columnType)
                );
            case ColumnType.DECIMAL256:
                final Decimal256 decimal256 = sqlExecutionContext.getDecimal256();
                record.getDecimal256(columnIndex, decimal256);
                return calculateDecimalBinSize(
                        decimal256,
                        ColumnType.getDecimalPrecision(columnType),
                        ColumnType.getDecimalScale(columnType)
                );
            case ColumnType.UUID:
                final long lo = record.getLong128Lo(columnIndex);
                final long hi = record.getLong128Hi(columnIndex);
                return Uuid.isNull(lo, hi) ? Integer.BYTES : Integer.BYTES + Long.BYTES * 2;
            case ColumnType.LONG256:
                final Long256 long256Value = record.getLong256A(columnIndex);
                return Long256Impl.isNull(long256Value) ? Integer.BYTES : Integer.BYTES + Numbers.hexDigitsLong256(long256Value);
            case ColumnType.GEOBYTE:
                return geoHashBytes(record.getGeoByte(columnIndex), geohashSize);
            case ColumnType.GEOSHORT:
                return geoHashBytes(record.getGeoShort(columnIndex), geohashSize);
            case ColumnType.GEOINT:
                return geoHashBytes(record.getGeoInt(columnIndex), geohashSize);
            case ColumnType.GEOLONG:
                return geoHashBytes(record.getGeoLong(columnIndex), geohashSize);
            case ColumnType.VARCHAR:
                final Utf8Sequence vcValue = record.getVarcharA(columnIndex);
                if (vcValue == null) {
                    return Integer.BYTES;
                }
                // resumePoint == -1 means header not sent yet, include it
                // resumePoint >= 0 is the byte offset of already sent data
                int vcResumePoint = Math.max(0, resumePoint);
                int vcRemaining = vcValue.size() - vcResumePoint;
                return resumePoint == -1 ? Integer.BYTES + vcRemaining : vcRemaining;
            case ColumnType.ARRAY_STRING:
                // ARRAY_STRING goes out through outColString() under either format code
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
                        return Integer.BYTES + blobSize;
                    } else {
                        throw PGMessageProcessingException.instance(pipelineEntry)
                                .put("blob is too large [blobSize=").put(blobSize)
                                .put(", maxBlobSize=").put(maxBlobSize)
                                .put(", columnIndex=").put(columnIndex)
                                .put(']');
                    }
                }
            case ColumnType.ARRAY:
                ArrayView array = record.getArray(columnIndex, columnType);
                if (array.isNull()) {
                    return Integer.BYTES; // size field (will be -1 for NULL)
                }
                final short elemType = ColumnType.decodeArrayElementType(columnType);
                if (elemType != ColumnType.DOUBLE) {
                    // outColBinArr() only encodes DOUBLE elements, and the fixed-size arithmetic
                    // below assumes them too. Report "cannot size" so
                    // calculateRecordTailSize() rewinds the row instead of patching a wrong length,
                    // and let outColBinArr() reject the request with a message the client can act on.
                    return -1;
                }

                int actualResumePoint = Math.max(0, resumePoint);
                int remainingElements = array.getCardinality() - actualResumePoint; // includes nulls
                int notNullCount = PGUtils.countNotNull(array, actualResumePoint);

                // -1 = array header was not written yet -> we have to include it in our calculation
                long size = resumePoint == -1 ? calculateArrayHeaderSize(array) : 0;

                // add remaining elements
                size += calculateArrayResumeColBinSize(notNullCount, remainingElements - notNullCount);
                return size;
            case ColumnType.INTERVAL:
                // This method has to be EXACT, not an upper bound: calculateRecordTailSize() patches
                // its result into a DataRow length prefix. An interval's size depends on the rendered
                // timestamps, so it cannot be sized without doing the work. Report "cannot size" and
                // give up mid-record resume for this row; the whole-row rewind still delivers it.
                return -1;
            default:
                // never assert here: this runs inside outRecord()'s NoSpaceLeftInResponseBufferException
                // handler, where a thrown AssertionError would replace the in-flight exception and
                // derail the rewind. outRecord()'s default arm reports the unsupported type instead.
                return -1;
        }
    }

    public static int countNotNull(ArrayView array, int resumePoint) {
        if (array.isEmpty()) {
            return 0;
        }

        final int cardinality = array.getCardinality();
        final int skip = Math.max(0, resumePoint);
        if (skip >= cardinality) {
            return 0;
        }

        if (array.isVanilla()) {
            if (array.getElemType() != ColumnType.DOUBLE) {
                throw new AssertionError("Unsupported array element type: " + array.getElemType());
            }
            return array.flatView().countDouble(
                    array.getFlatViewOffset() + skip,
                    array.getFlatViewLength() - skip);
        } else {
            return countNotNullRecursive(array, 0, 0, skip, cardinality);
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
            int columnType
    ) {
        // matches calculateColumnBinSize(), which also derives the tag from the full column type
        final int typeTag = ColumnType.tagOf(columnType);
        return switch (typeTag) {
            case ColumnType.NULL -> Integer.BYTES;
            case ColumnType.BOOLEAN -> Integer.BYTES + Byte.BYTES;
            case ColumnType.BYTE -> Integer.BYTES + MAX_BYTE_TEXT_LEN;
            case ColumnType.SHORT -> Integer.BYTES + MAX_SHORT_TEXT_LEN;
            case ColumnType.CHAR -> Integer.BYTES + MAX_CHAR_TEXT_LEN;
            case ColumnType.IPv4 -> Integer.BYTES + MAX_IPv4_TEXT_LEN;
            case ColumnType.INT -> Integer.BYTES + MAX_INT_TEXT_LEN;
            case ColumnType.LONG -> Integer.BYTES + MAX_LONG_TEXT_LEN;
            case ColumnType.DATE -> Integer.BYTES + MAX_DATE_TEXT_LEN;
            case ColumnType.TIMESTAMP -> Integer.BYTES + MAX_TIMESTAMP_TEXT_LEN;
            case ColumnType.FLOAT -> Integer.BYTES + MAX_FLOAT_TEXT_LEN;
            case ColumnType.DOUBLE -> Integer.BYTES + MAX_DOUBLE_TEXT_LEN;
            // Reserve bytes for an optional sign, decimal point, and leading zero.
            case ColumnType.DECIMAL8,
                 ColumnType.DECIMAL16,
                 ColumnType.DECIMAL32,
                 ColumnType.DECIMAL64,
                 ColumnType.DECIMAL128,
                 ColumnType.DECIMAL256 -> Integer.BYTES + ColumnType.getDecimalPrecision(columnType) + 3L;
            case ColumnType.UUID -> Integer.BYTES + MAX_UUID_TEXT_LEN;
            case ColumnType.LONG256 -> Integer.BYTES + MAX_LONG256_TEXT_LEN;
            case ColumnType.GEOBYTE -> Integer.BYTES + MAX_GEOBYTE_TEXT_LEN;
            case ColumnType.GEOSHORT -> Integer.BYTES + MAX_GEOSHORT_TEXT_LEN;
            case ColumnType.GEOINT -> Integer.BYTES + MAX_GEOINT_TEXT_LEN;
            case ColumnType.GEOLONG -> Integer.BYTES + MAX_GEOLONG_TEXT_LEN;
            case ColumnType.VARCHAR -> {
                final Utf8Sequence vcValue = record.getVarcharA(columnIndex);
                yield vcValue == null ? Integer.BYTES : Integer.BYTES + vcValue.size();
            }
            case ColumnType.STRING -> {
                final CharSequence strValue = record.getStrA(columnIndex);
                // take a rough upper estimate based on the string length
                yield strValue == null ? Integer.BYTES : Integer.BYTES + 3L * strValue.length();
            }
            case ColumnType.SYMBOL -> {
                final CharSequence symValue = record.getSymA(columnIndex);
                // take a rough upper estimate based on the string length
                yield symValue == null ? Integer.BYTES : Integer.BYTES + 3L * symValue.length();
            }
            case ColumnType.BINARY -> {
                BinarySequence sequence = record.getBin(columnIndex);
                yield sequence == null ? Integer.BYTES : Integer.BYTES + sequence.length();
            }
            // ARRAY sits last, as it does in calculateColumnBinSize()
            case ColumnType.ARRAY -> {
                final ArrayView array = record.getArray(columnIndex, columnType);
                if (array.isNull()) {
                    yield Integer.BYTES;
                }
                yield Integer.BYTES + arrayTxtSize(array);
            }
            case ColumnType.INTERVAL -> Integer.BYTES + MAX_INTERVAL_TEXT_LEN;
            // NOTE: no ARRAY_STRING arm - txtAndBinSizesCanBeDifferent() reports it as same-sized
            // in both formats, so it is sized by calculateColumnBinSize() and never reaches here.
            default ->
                // an unknown type must not raise here: this runs inside outRecord()'s
                // NoSpaceLeftInResponseBufferException handler, where a thrown AssertionError
                // replaces the in-flight exception and derails the rewind. outRecord()'s own
                // default arm is what reports an unsupported type to the client.
                    -1;
        };
    }

    /**
     * Upper bound on the bytes {@code outColTxtArr()} writes for a non-null array, excluding the
     * length prefix. ArrayTypeDriver.arrayToText() emits one brace PAIR per node of the shape
     * tree, not one per element, so the brace count is driven by dimensionality: for shape
     * (d0..dk-1) it is {@code 1 + d0 + d0*d1 + ... + d0*..*dk-2}. Charging a fixed allowance per
     * element under-counts as soon as a trailing dimension is 1 (shape (2,1,1) needs 5 pairs for
     * 2 elements), so this walks the shape instead - at most {@link ColumnType#ARRAY_NDIMS_LIMIT}
     * steps, on the send-buffer-overflow path only. Commas telescope to exactly cardinality - 1.
     */
    private static long arrayTxtSize(ArrayView array) {
        if (array.isEmpty()) {
            // arrayToText() short-circuits an empty array to "{}" whatever its shape, so the node
            // walk below must not run: a shape like (100_000_000, 100_000_000, 0) would count ten
            // quadrillion phantom brace pairs
            return 2;
        }
        long nodes = 0;
        long levelNodes = 1;
        for (int d = 0, n = array.getDimCount(); d < n; d++) {
            nodes += levelNodes;
            levelNodes *= array.getDimLen(d);
        }
        final int cardinality = array.getCardinality();
        final long elements;
        if (array.getElemType() == ColumnType.VARCHAR) {
            // a varchar element has no width bound, so measure the elements we are going to write.
            if (array.isVanilla()) {
                long size = 0;
                for (int i = 0; i < cardinality; i++) {
                    final Utf8Sequence value = array.getVarchar(i);
                    size += value == null ? NULL_LITERAL_TEXT_LEN : value.size();
                }
                elements = size;
            } else {
                elements = varcharArrayTxtSize(array, 0, 0);
            }
        } else {
            // DOUBLE is the supported numeric element type; the "NULL" literal is shorter
            elements = (long) cardinality * MAX_DOUBLE_TEXT_LEN;
        }
        return 2 * nodes + (cardinality - 1L) + elements;
    }

    private static long varcharArrayTxtSize(ArrayView array, int dim, int flatIndex) {
        long size = 0;
        final int count = array.getDimLen(dim);
        final int stride = array.getStride(dim);
        if (dim < array.getDimCount() - 1) {
            for (int i = 0; i < count; i++) {
                size += varcharArrayTxtSize(array, dim + 1, flatIndex);
                flatIndex += stride;
            }
        } else {
            for (int i = 0; i < count; i++) {
                final Utf8Sequence value = array.getVarchar(flatIndex);
                size += value == null ? NULL_LITERAL_TEXT_LEN : value.size();
                flatIndex += stride;
            }
        }
        return size;
    }

    /**
     * Writes one PostgreSQL NUMERIC field: an int32 length prefix, the four int16 header words
     * (ndigits, weight, sign, dscale) and one int16 per base-10000 digit group. The number of bytes this
     * writes must match what {@link #calculateColumnBinSize} reports for the same value and column type,
     * which {@code PGUtilsTest} pins.
     */
    public static void outColBinDecimal(PGResponseSink utf8Sink, Decimal128 decimal128, int type) {
        if (decimal128.isNull()) {
            utf8Sink.setNullValue();
            return;
        }

        final int precision = ColumnType.getDecimalPrecision(type);
        final int scale = ColumnType.getDecimalScale(type);

        short sign = NUMERIC_POS;
        if (decimal128.isNegative()) {
            sign = NUMERIC_NEG;
            decimal128.negate();
        }

        // Based on https://github.com/postgres/postgres/blob/4246a977bad6e76c4276a0d52def8a3dced154bb/src/backend/utils/adt/numeric.c#L1142-L1165
        // Postgres binary format serialize decimals into an array of unsigned 4 digits (stored in 16-bit) integers.
        // Each array member encode the digit between 10^x and 10^(x+3), x being a multiple of 4. For example, 12.34 must
        // be encoded as an array of 2 shorts: [12, 3400].

        long startAddress = utf8Sink.getSendBufferPtr();
        utf8Sink.putNetworkInt(4 * Short.BYTES); // type size, defaults to a zero value, we will came back later to rewrite it

        utf8Sink.putNetworkShort((short) 0); // ndigits, same
        utf8Sink.putNetworkShort((short) 0); // weight, same
        utf8Sink.putNetworkShort(sign); // sign
        utf8Sink.putNetworkShort((short) scale); // dscale

        if (decimal128.isZero()) {
            return;
        }

        boolean writing = false;
        int digit = 0;
        int weight = 0;
        int pow = precision;

        // We start with the whole part of the decimal
        final int wholePartPrecision = precision - scale;
        for (int i = wholePartPrecision - 1; i >= 0; i--) {
            final int mul = decimal128.getDigitAtPowerOfTen(--pow);
            digit = digit * 10 + mul;
            decimal128.subtractPowerOfTenMultiple(pow, mul);
            if (i % 4 == 0 && (writing || digit != 0)) {
                if (!writing) {
                    writing = true;
                    weight = (i + 3) / 4;
                }
                utf8Sink.putNetworkShort((short) digit);
                digit = 0;
            }
        }

        // And then the decimal part
        for (int i = 0, n = (scale + 3) / 4; i < n; i++) {
            digit = 0;
            for (int j = 0; j < 4; j++) {
                final int mul = pow > 0 ? decimal128.getDigitAtPowerOfTen(--pow) : 0;
                digit = digit * 10 + mul;
                decimal128.subtractPowerOfTenMultiple(pow, mul);
            }
            if (writing || digit != 0) {
                if (!writing) {
                    writing = true;
                    weight = -i - 1;
                }
                utf8Sink.putNetworkShort((short) digit);
            }
        }

        // We now need to fix previous values
        long endAddress = utf8Sink.getSendBufferPtr();
        final int typeLen = (int) (endAddress - startAddress - Integer.BYTES);
        // Patching the whole type len
        utf8Sink.putNetworkInt(startAddress, typeLen);
        // Patching the number of digits (remove the static attributes first)
        utf8Sink.putNetworkShort(startAddress + Integer.BYTES, (short) ((typeLen - 4 * Short.BYTES) / Short.BYTES));
        // Patching the weight
        utf8Sink.putNetworkShort(startAddress + Integer.BYTES + Short.BYTES, (short) weight);
    }

    /**
     * @see #outColBinDecimal(PGResponseSink, Decimal128, int)
     */
    public static void outColBinDecimal(PGResponseSink utf8Sink, Decimal256 decimal256, int type) {
        if (decimal256.isNull()) {
            utf8Sink.setNullValue();
            return;
        }

        final int precision = ColumnType.getDecimalPrecision(type);
        final int scale = ColumnType.getDecimalScale(type);

        short sign = NUMERIC_POS;
        if (decimal256.isNegative()) {
            sign = NUMERIC_NEG;
            decimal256.negate();
        }

        // Based on https://github.com/postgres/postgres/blob/4246a977bad6e76c4276a0d52def8a3dced154bb/src/backend/utils/adt/numeric.c#L1142-L1165
        // Postgres binary format serialize decimals into an array of unsigned 4 digits (stored in 16-bit) integers.
        // Each array member encode the digit between 10^x and 10^(x+3), x being a multiple of 4. For example, 12.34 must
        // be encoded as an array of 2 shorts: [12, 3400].

        long startAddress = utf8Sink.getSendBufferPtr();
        utf8Sink.putNetworkInt(4 * Short.BYTES); // type size, defaults to a zero value, we will came back later to rewrite it

        utf8Sink.putNetworkShort((short) 0); // ndigits, same
        utf8Sink.putNetworkShort((short) 0); // weight, same
        utf8Sink.putNetworkShort(sign); // sign
        utf8Sink.putNetworkShort((short) scale); // dscale

        if (decimal256.isZero()) {
            return;
        }

        boolean writing = false;
        int digit = 0;
        int weight = 0;
        int pow = precision;

        // We start with the whole part of the decimal
        final int wholePartPrecision = precision - scale;
        for (int i = wholePartPrecision - 1; i >= 0; i--) {
            final int mul = decimal256.getDigitAtPowerOfTen(--pow);
            digit = digit * 10 + mul;
            decimal256.subtractPowerOfTenMultiple(pow, mul);
            if (i % 4 == 0 && (writing || digit != 0)) {
                if (!writing) {
                    writing = true;
                    weight = (i + 3) / 4;
                }
                utf8Sink.putNetworkShort((short) digit);
                digit = 0;
            }
        }

        // And then the decimal part
        for (int i = 0, n = (scale + 3) / 4; i < n; i++) {
            digit = 0;
            for (int j = 0; j < 4; j++) {
                final int mul = pow > 0 ? decimal256.getDigitAtPowerOfTen(--pow) : 0;
                digit = digit * 10 + mul;
                decimal256.subtractPowerOfTenMultiple(pow, mul);
            }
            if (writing || digit != 0) {
                if (!writing) {
                    writing = true;
                    weight = -i - 1;
                }
                utf8Sink.putNetworkShort((short) digit);
            }
        }

        // We now need to fix previous values
        long endAddress = utf8Sink.getSendBufferPtr();
        final int typeLen = (int) (endAddress - startAddress - Integer.BYTES);
        // Patching the whole type len
        utf8Sink.putNetworkInt(startAddress, typeLen);
        // Patching the number of digits (remove the static attributes first)
        utf8Sink.putNetworkShort(startAddress + Integer.BYTES, (short) ((typeLen - 4 * Short.BYTES) / Short.BYTES));
        // Patching the weight
        utf8Sink.putNetworkShort(startAddress + Integer.BYTES + Short.BYTES, (short) weight);
    }

    /**
     * @see #outColBinDecimal(PGResponseSink, Decimal128, int)
     */
    public static void outColBinDecimal(PGResponseSink utf8Sink, Decimal64 decimal64, int type) {
        if (decimal64.isNull()) {
            utf8Sink.setNullValue();
            return;
        }

        final int precision = ColumnType.getDecimalPrecision(type);
        final int scale = ColumnType.getDecimalScale(type);

        short sign = NUMERIC_POS;
        if (decimal64.isNegative()) {
            sign = NUMERIC_NEG;
            decimal64.negate();
        }

        // Based on https://github.com/postgres/postgres/blob/4246a977bad6e76c4276a0d52def8a3dced154bb/src/backend/utils/adt/numeric.c#L1142-L1165
        // Postgres binary format serialize decimals into an array of unsigned 4 digits (stored in 16-bit) integers.
        // Each array member encode the digit between 10^x and 10^(x+3), x being a multiple of 4. For example, 12.34 must
        // be encoded as an array of 2 shorts: [12, 3400].

        long startAddress = utf8Sink.getSendBufferPtr();
        utf8Sink.putNetworkInt(4 * Short.BYTES); // type size, defaults to a zero value, we will came back later to rewrite it

        utf8Sink.putNetworkShort((short) 0); // ndigits, same
        utf8Sink.putNetworkShort((short) 0); // weight, same
        utf8Sink.putNetworkShort(sign); // sign
        utf8Sink.putNetworkShort((short) scale); // dscale

        if (decimal64.isZero()) {
            return;
        }

        boolean writing = false;
        int digit = 0;
        int weight = 0;
        int pow = precision;

        // We start with the whole part of the decimal
        final int wholePartPrecision = precision - scale;
        for (int i = wholePartPrecision - 1; i >= 0; i--) {
            final int mul = decimal64.getDigitAtPowerOfTen(--pow);
            digit = digit * 10 + mul;
            decimal64.subtractPowerOfTenMultiple(pow, mul);
            if (i % 4 == 0 && (writing || digit != 0)) {
                if (!writing) {
                    writing = true;
                    weight = (i + 3) / 4;
                }
                utf8Sink.putNetworkShort((short) digit);
                digit = 0;
            }
        }

        // And then the decimal part
        for (int i = 0, n = (scale + 3) / 4; i < n; i++) {
            digit = 0;
            for (int j = 0; j < 4; j++) {
                final int mul = pow > 0 ? decimal64.getDigitAtPowerOfTen(--pow) : 0;
                digit = digit * 10 + mul;
                decimal64.subtractPowerOfTenMultiple(pow, mul);
            }
            if (writing || digit != 0) {
                if (!writing) {
                    writing = true;
                    weight = -i - 1;
                }
                utf8Sink.putNetworkShort((short) digit);
            }
        }

        // We now need to fix previous values
        long endAddress = utf8Sink.getSendBufferPtr();
        final int typeLen = (int) (endAddress - startAddress - Integer.BYTES);
        // Patching the whole type len
        utf8Sink.putNetworkInt(startAddress, typeLen);
        // Patching the number of digits (remove the static attributes first)
        utf8Sink.putNetworkShort(startAddress + Integer.BYTES, (short) ((typeLen - 4 * Short.BYTES) / Short.BYTES));
        // Patching the weight
        utf8Sink.putNetworkShort(startAddress + Integer.BYTES + Short.BYTES, (short) weight);
    }

    private static int calculateArrayHeaderSize(ArrayView array) {
        return Integer.BYTES // size field (stores the number returned from this method)
                + Integer.BYTES // dimension count
                + Integer.BYTES // "has nulls" flag
                + Integer.BYTES // component type
                + array.getDimCount() * (2 * Integer.BYTES); // dimension lengths
    }

    private static int calculateDecimalBinSize(long value, int scale) {
        if (value == 0) {
            return PG_NUMERIC_FIXED_BIN_SIZE;
        }

        if (value < 0) {
            value = -value;
        }
        int highestPower = 0;
        while (value >= 10) {
            value /= 10;
            highestPower++;
        }
        return calculateNonZeroDecimalBinSize(highestPower, scale);
    }

    private static int calculateDecimalBinSize(Decimal128 decimal, int precision, int scale) {
        if (decimal.isNull()) {
            return Integer.BYTES;
        }
        if (decimal.isZero()) {
            return PG_NUMERIC_FIXED_BIN_SIZE;
        }
        if (decimal.isNegative()) {
            decimal.negate();
        }

        int highestPower = precision - 1;
        while (decimal.getDigitAtPowerOfTen(highestPower) == 0) {
            highestPower--;
        }
        return calculateNonZeroDecimalBinSize(highestPower, scale);
    }

    private static int calculateDecimalBinSize(Decimal256 decimal, int precision, int scale) {
        if (decimal.isNull()) {
            return Integer.BYTES;
        }
        if (decimal.isZero()) {
            return PG_NUMERIC_FIXED_BIN_SIZE;
        }
        if (decimal.isNegative()) {
            decimal.negate();
        }

        int highestPower = precision - 1;
        while (decimal.getDigitAtPowerOfTen(highestPower) == 0) {
            highestPower--;
        }
        return calculateNonZeroDecimalBinSize(highestPower, scale);
    }

    private static int calculateNonZeroDecimalBinSize(int highestPower, int scale) {
        // PostgreSQL NUMERIC groups decimal digits in base 10000. The first group's weight comes from the
        // highest non-zero decimal digit, while the final group covers the end of the declared scale.
        final int firstWeight = Math.floorDiv(highestPower - scale, 4);
        final int digitCount = firstWeight + (scale + 3) / 4 + 1;
        assert digitCount > 0;
        return PG_NUMERIC_FIXED_BIN_SIZE + digitCount * Short.BYTES;
    }

    private static int countNotNullRecursive(
            ArrayView array,
            int dim,
            int flatIndex,
            int skip,
            int subtreeCardinality
    ) {
        int count = 0;
        final int dimLen = array.getDimLen(dim);
        final int stride = array.getStride(dim);
        final boolean atDeepestDim = dim == array.getDimCount() - 1;
        if (atDeepestDim) {
            if (array.getElemType() != ColumnType.DOUBLE) {
                throw new AssertionError("Unsupported array element type: " + array.getElemType());
            }
            flatIndex += skip * stride;
            for (int i = skip; i < dimLen; i++) {
                if (Numbers.isFinite(array.getDouble(flatIndex))) {
                    count++;
                }
                flatIndex += stride;
            }
        } else {
            final int childCardinality = subtreeCardinality / dimLen;
            final int firstChild = skip / childCardinality;
            final int childSkip = skip % childCardinality;
            flatIndex += firstChild * stride;
            for (int i = firstChild; i < dimLen; i++) {
                count += countNotNullRecursive(
                        array,
                        dim + 1,
                        flatIndex,
                        i == firstChild ? childSkip : 0,
                        childCardinality
                );
                flatIndex += stride;
            }
        }
        return count;
    }

    private static int geoHashBytes(long value, int size) {
        if (value == GeoHashes.NULL) {
            return Integer.BYTES;
        } else {
            assert size > 0;
            // chars or bits
            return Integer.BYTES + size;
        }
    }
}
