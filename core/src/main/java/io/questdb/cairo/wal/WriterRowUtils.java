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

package io.questdb.cairo.wal;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.TableWriter;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimals;
import io.questdb.std.NumericException;
import io.questdb.std.str.Utf8Sequence;

public class WriterRowUtils {

    private WriterRowUtils() {
    }

    /**
     * Puts decimal value into a row column. The Decimal should already have the right scale.
     *
     * @param index      column index
     * @param value      decimal value to be copied to the row column
     * @param columnType column type
     * @param row        row to be updated
     */
    public static void putDecimal(int index, Decimal256 value, int columnType, TableWriter.Row row) {
        if (value.isNull()) {
            putNullDecimal(row, index, columnType);
            return;
        }
        short tag = ColumnType.tagOf(columnType);
        if (!value.fitsInStorageSizePow2(tag - ColumnType.DECIMAL8)) {
            throw CairoException.nonCritical().put("value does not fit in column type ").put(ColumnType.nameOf(columnType));
        }
        putDecimal0(index, value, tag, row);
    }

    /**
     * Puts decimal value into a row column. The Decimal should already have the right scale and precision.
     *
     * @param index     column index
     * @param value     decimal value to be copied to the row column
     * @param columnTag column tag ({@code tagOf(type)})
     * @param row       row to be updated
     */
    public static void putDecimalQuick(int index, Decimal256 value, short columnTag, TableWriter.Row row) {
        if (value.isNull()) {
            putNullDecimal(row, index, columnTag);
            return;
        }
        putDecimal0(index, value, columnTag, row);
    }

    public static void putDecimalStr(int index, Decimal256 decimalSink, CharSequence decimalValue, int columnType, TableWriter.Row row) {
        if (decimalValue == null) {
            putNullDecimal(row, index, columnType);
            return;
        }

        final int precision = ColumnType.getDecimalPrecision(columnType);
        final int scale = ColumnType.getDecimalScale(columnType);
        try {
            decimalSink.ofString(decimalValue, precision, scale);
        } catch (NumericException e) {
            throw ImplicitCastException.inconvertibleValue(decimalValue, ColumnType.STRING, columnType);
        }
        putDecimal0(index, decimalSink, ColumnType.tagOf(columnType), row);
    }

    public static void putGeoHash(int index, long value, int columnType, TableWriter.Row row) {
        switch (ColumnType.tagOf(columnType)) {
            case ColumnType.GEOBYTE:
                row.putByte(index, (byte) value);
                break;
            case ColumnType.GEOSHORT:
                row.putShort(index, (short) value);
                break;
            case ColumnType.GEOINT:
                row.putInt(index, (int) value);
                break;
            default:
                row.putLong(index, value);
                break;
        }
    }

    public static void putGeoStr(int index, CharSequence hash, int type, TableWriter.Row row) {
        if (hash == null) {
            putGeoHash(index, GeoHashes.NULL, type, row);
            return;
        }

        final int hashLen = hash.length();
        final int typeBits = ColumnType.getGeoHashBits(type);
        final int charsRequired = (typeBits - 1) / 5 + 1;
        if (hashLen < charsRequired) {
            throw ImplicitCastException.inconvertibleValue(hash, ColumnType.STRING, type);
        }

        try {
            long val = GeoHashes.widen(
                    GeoHashes.fromString(hash, 0, charsRequired),
                    charsRequired * 5,
                    typeBits
            );
            putGeoHash(index, val, type, row);
        } catch (NumericException e) {
            throw ImplicitCastException.inconvertibleValue(hash, ColumnType.STRING, type);
        }
    }

    public static void putGeoVarchar(int index, Utf8Sequence hash, int type, TableWriter.Row row) {
        if (hash == null) {
            putGeoHash(index, GeoHashes.NULL, type, row);
            return;
        }

        final int hashLen = hash.size();
        final int typeBits = ColumnType.getGeoHashBits(type);
        final int bytesRequired = (typeBits - 1) / 5 + 1;
        if (hashLen < bytesRequired) {
            throw ImplicitCastException.inconvertibleValue(hash, ColumnType.VARCHAR, type);
        }

        try {
            long val = GeoHashes.widen(
                    GeoHashes.fromAscii(hash, 0, bytesRequired),
                    bytesRequired * 5,
                    typeBits
            );
            putGeoHash(index, val, type, row);
        } catch (NumericException e) {
            throw ImplicitCastException.inconvertibleValue(hash, ColumnType.VARCHAR, type);
        }
    }

    public static void putNullDecimal(TableWriter.Row row, int col, int toType) {
        putNullDecimal(row, col, ColumnType.tagOf(toType));
    }

    public static void putNullDecimal(TableWriter.Row row, int col, short toTag) {
        switch (toTag) {
            case ColumnType.DECIMAL8:
                row.putByte(col, Decimals.DECIMAL8_NULL);
                break;
            case ColumnType.DECIMAL16:
                row.putShort(col, Decimals.DECIMAL16_NULL);
                break;
            case ColumnType.DECIMAL32:
                row.putInt(col, Decimals.DECIMAL32_NULL);
                break;
            case ColumnType.DECIMAL64:
                row.putLong(col, Decimals.DECIMAL64_NULL);
                break;
            case ColumnType.DECIMAL128:
                row.putDecimal128(col, Decimals.DECIMAL128_HI_NULL, Decimals.DECIMAL128_LO_NULL);
                break;
            case ColumnType.DECIMAL256:
                row.putDecimal256(col, Decimals.DECIMAL256_HH_NULL, Decimals.DECIMAL256_HL_NULL, Decimals.DECIMAL256_LH_NULL, Decimals.DECIMAL256_LL_NULL);
                break;
        }
    }

    private static void putDecimal0(int index, Decimal256 value, int tag, TableWriter.Row row) {
        switch (tag) {
            case ColumnType.DECIMAL8:
                row.putByte(index, (byte) value.getLl());
                break;
            case ColumnType.DECIMAL16:
                row.putShort(index, (short) value.getLl());
                break;
            case ColumnType.DECIMAL32:
                row.putInt(index, (int) value.getLl());
                break;
            case ColumnType.DECIMAL64:
                row.putLong(index, value.getLl());
                break;
            case ColumnType.DECIMAL128:
                row.putDecimal128(index, value.getLh(), value.getLl());
                break;
            default:
                row.putDecimal256(index, value.getHh(), value.getHl(), value.getLh(), value.getLl());
                break;
        }
    }
}
