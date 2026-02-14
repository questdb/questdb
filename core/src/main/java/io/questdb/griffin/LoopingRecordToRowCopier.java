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

package io.questdb.griffin;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.ColumnFilter;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.DoubleArrayParser;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Transient;
import io.questdb.std.str.Utf8Sequence;

/**
 * Loop-based implementation of RecordToRowCopier for tables with many columns.
 * Unlike the bytecode-generated copiers, this uses simple loops and switch statements,
 * trading some performance for simplicity and avoiding bytecode size limitations.
 * <p>
 * This implementation is used when the number of columns exceeds the configured threshold
 * (default: 1000) to avoid hitting the JVM's 64KB method size limit.
 */
public class LoopingRecordToRowCopier implements RecordToRowCopier {
    private final DoubleArrayParser arrayParser; // null if not needed
    private final ColumnTypes fromTypes;
    private final int timestampIndex;
    private final ColumnFilter toColumnFilter;
    private final RecordMetadata toMetadata;

    public LoopingRecordToRowCopier(
            @Transient ColumnTypes fromTypes,
            @Transient RecordMetadata toMetadata,
            @Transient ColumnFilter toColumnFilter
    ) {
        // Deep copy all mutable objects to avoid retaining references
        this.fromTypes = copyColumnTypes(fromTypes);
        this.toMetadata = GenericRecordMetadata.copyOfNew(toMetadata);
        this.toColumnFilter = copyColumnFilter(toColumnFilter);
        this.timestampIndex = toMetadata.getTimestampIndex();

        // Only create array parser if needed (for STRING/VARCHAR → ARRAY conversions)
        this.arrayParser = isArrayParserRequired(this.fromTypes, this.toMetadata, this.toColumnFilter)
                ? new DoubleArrayParser()
                : null;
    }

    @Override
    public void copy(SqlExecutionContext context, Record record, TableWriter.Row row) {
        final int n = toColumnFilter.getColumnCount();
        final Decimal128 decimal128 = context.getDecimal128();
        final Decimal256 decimal256 = context.getDecimal256();

        for (int i = 0; i < n; i++) {
            final int toColumnIndex = toColumnFilter.getColumnIndexFactored(i);

            // Skip timestamp column (handled externally)
            if (toColumnIndex == timestampIndex) {
                continue;
            }

            final int toColumnType = toMetadata.getColumnType(toColumnIndex);
            final int fromColumnType = fromTypes.getColumnType(i);
            int fromColumnTypeTag = ColumnType.tagOf(fromColumnType);
            final int toColumnTypeTag = ColumnType.tagOf(toColumnType);
            final int toColumnWriterIndex = toMetadata.getWriterIndex(toColumnIndex);

            // Handle NULL type - treat as target type so getter returns null value
            if (fromColumnTypeTag == ColumnType.NULL) {
                fromColumnTypeTag = toColumnTypeTag;
            }

            // Get TimestampDriver when needed for conversions
            TimestampDriver timestampDriver = null;
            if (toColumnTypeTag == ColumnType.DATE && fromColumnTypeTag == ColumnType.TIMESTAMP) {
                timestampDriver = ColumnType.getTimestampDriver(fromColumnType);
            } else if (toColumnTypeTag == ColumnType.TIMESTAMP &&
                    (fromColumnTypeTag == ColumnType.DATE ||
                            fromColumnTypeTag == ColumnType.VARCHAR ||
                            fromColumnTypeTag == ColumnType.STRING ||
                            (fromColumnTypeTag == ColumnType.TIMESTAMP && fromColumnType != toColumnType))) {
                timestampDriver = ColumnType.getTimestampDriver(toColumnType);
            }

            // Copy the column value based on source and target types
            copyColumn(record, row, i, toColumnWriterIndex, fromColumnTypeTag, toColumnTypeTag,
                    fromColumnType, toColumnType, timestampDriver, decimal128, decimal256);
        }
    }

    private static ColumnFilter copyColumnFilter(ColumnFilter from) {
        ListColumnFilter copy = new ListColumnFilter();
        for (int i = 0, n = from.getColumnCount(); i < n; i++) {
            copy.add(from.getColumnIndex(i));
        }
        return copy;
    }

    private static ColumnTypes copyColumnTypes(ColumnTypes from) {
        ArrayColumnTypes copy = new ArrayColumnTypes();
        for (int i = 0, n = from.getColumnCount(); i < n; i++) {
            copy.add(from.getColumnType(i));
        }
        return copy;
    }

    private static boolean isArrayParserRequired(ColumnTypes from, RecordMetadata to, ColumnFilter toColumnFilter) {
        int n = toColumnFilter.getColumnCount();
        for (int i = 0; i < n; i++) {
            int toColumnIndex = toColumnFilter.getColumnIndexFactored(i);
            int toColumnType = to.getColumnType(toColumnIndex);
            int fromColumnType = from.getColumnType(i);
            if (ColumnType.tagOf(toColumnType) == ColumnType.ARRAY &&
                    (ColumnType.tagOf(fromColumnType) == ColumnType.STRING ||
                            ColumnType.tagOf(fromColumnType) == ColumnType.VARCHAR)) {
                return true;
            }
        }
        return false;
    }

    private void copyColumn(
            Record record,
            TableWriter.Row row,
            int fromColumnIndex,
            int toColumnWriterIndex,
            int fromColumnTypeTag,
            int toColumnTypeTag,
            int fromColumnType,
            int toColumnType,
            TimestampDriver timestampDriver,
            Decimal128 decimal128,
            Decimal256 decimal256
    ) {
        switch (fromColumnTypeTag) {
            case ColumnType.INT ->
                    copyFromInt(record, row, fromColumnIndex, toColumnWriterIndex, toColumnTypeTag, toColumnType, decimal256);
            case ColumnType.IPv4 -> copyFromIPv4(record, row, fromColumnIndex, toColumnWriterIndex);
            case ColumnType.LONG ->
                    copyFromLong(record, row, fromColumnIndex, toColumnWriterIndex, toColumnTypeTag, toColumnType, decimal256);
            case ColumnType.DATE ->
                    copyFromDate(record, row, fromColumnIndex, toColumnWriterIndex, toColumnTypeTag, timestampDriver);
            case ColumnType.TIMESTAMP ->
                    copyFromTimestamp(record, row, fromColumnIndex, toColumnWriterIndex, toColumnTypeTag, fromColumnType, toColumnType, timestampDriver);
            case ColumnType.BYTE ->
                    copyFromByte(record, row, fromColumnIndex, toColumnWriterIndex, toColumnTypeTag, toColumnType, decimal256);
            case ColumnType.SHORT ->
                    copyFromShort(record, row, fromColumnIndex, toColumnWriterIndex, toColumnTypeTag, toColumnType, decimal256);
            case ColumnType.BOOLEAN -> copyFromBoolean(record, row, fromColumnIndex, toColumnWriterIndex);
            case ColumnType.FLOAT -> copyFromFloat(record, row, fromColumnIndex, toColumnWriterIndex, toColumnTypeTag);
            case ColumnType.DOUBLE ->
                    copyFromDouble(record, row, fromColumnIndex, toColumnWriterIndex, toColumnTypeTag);
            case ColumnType.CHAR ->
                    copyFromChar(record, row, fromColumnIndex, toColumnWriterIndex, toColumnTypeTag, toColumnType);
            case ColumnType.SYMBOL ->
                    copyFromSymbol(record, row, fromColumnIndex, toColumnWriterIndex, toColumnTypeTag);
            case ColumnType.VARCHAR ->
                    copyFromVarchar(record, row, fromColumnIndex, toColumnWriterIndex, toColumnTypeTag, toColumnType, timestampDriver);
            case ColumnType.STRING ->
                    copyFromString(record, row, fromColumnIndex, toColumnWriterIndex, toColumnTypeTag, toColumnType, timestampDriver);
            case ColumnType.BINARY -> copyFromBinary(record, row, fromColumnIndex, toColumnWriterIndex);
            case ColumnType.LONG256 -> copyFromLong256(record, row, fromColumnIndex, toColumnWriterIndex);
            case ColumnType.GEOBYTE ->
                    copyFromGeoByte(record, row, fromColumnIndex, toColumnWriterIndex, fromColumnType, toColumnType);
            case ColumnType.GEOSHORT ->
                    copyFromGeoShort(record, row, fromColumnIndex, toColumnWriterIndex, fromColumnType, toColumnType, toColumnTypeTag);
            case ColumnType.GEOINT ->
                    copyFromGeoInt(record, row, fromColumnIndex, toColumnWriterIndex, fromColumnType, toColumnType, toColumnTypeTag);
            case ColumnType.GEOLONG ->
                    copyFromGeoLong(record, row, fromColumnIndex, toColumnWriterIndex, fromColumnType, toColumnType, toColumnTypeTag);
            case ColumnType.LONG128, ColumnType.UUID ->
                    copyFromUuid(record, row, fromColumnIndex, toColumnWriterIndex, toColumnTypeTag);
            case ColumnType.ARRAY ->
                    copyFromArray(record, row, fromColumnIndex, toColumnWriterIndex, fromColumnType, toColumnTypeTag);
            case ColumnType.DECIMAL8, ColumnType.DECIMAL16, ColumnType.DECIMAL32, ColumnType.DECIMAL64,
                 ColumnType.DECIMAL128, ColumnType.DECIMAL256 ->
                    copyFromDecimal(record, row, fromColumnIndex, toColumnWriterIndex, fromColumnTypeTag, fromColumnType, toColumnType, decimal128, decimal256);
            default -> {
            }
            // NULL type - do nothing, let NullSetters handle it
        }
    }

    private void copyFromArray(Record record, TableWriter.Row row, int fromIndex, int toIndex, int fromType, int toTypeTag) {
        if (toTypeTag == ColumnType.ARRAY) {
            ArrayView array = record.getArray(fromIndex, fromType);
            row.putArray(toIndex, array);
        }
    }

    private void copyFromBinary(Record record, TableWriter.Row row, int fromIndex, int toIndex) {
        row.putBin(toIndex, record.getBin(fromIndex));
    }

    private void copyFromBoolean(Record record, TableWriter.Row row, int fromIndex, int toIndex) {
        row.putBool(toIndex, record.getBool(fromIndex));
    }

    private void copyFromByte(Record record, TableWriter.Row row, int fromIndex, int toIndex, int toTypeTag, int toType, Decimal256 decimal256) {
        byte value = record.getByte(fromIndex);
        switch (toTypeTag) {
            case ColumnType.BOOLEAN, ColumnType.BYTE -> row.putByte(toIndex, value);
            case ColumnType.SHORT -> row.putShort(toIndex, value);
            case ColumnType.INT -> row.putInt(toIndex, value);
            case ColumnType.LONG -> row.putLong(toIndex, value);
            case ColumnType.DATE -> row.putDate(toIndex, value);
            case ColumnType.TIMESTAMP -> row.putTimestamp(toIndex, value);
            case ColumnType.FLOAT -> row.putFloat(toIndex, value);
            case ColumnType.DOUBLE -> row.putDouble(toIndex, value);
            default -> {
                if (ColumnType.isDecimalType(toTypeTag)) {
                    RecordToRowCopierUtils.transferByteToDecimal(row, toIndex, value, decimal256, toType);
                }
            }
        }
    }

    private void copyFromChar(Record record, TableWriter.Row row, int fromIndex, int toIndex, int toTypeTag, int toType) {
        char value = record.getChar(fromIndex);
        switch (toTypeTag) {
            case ColumnType.BYTE -> row.putByte(toIndex, SqlUtil.implicitCastCharAsByte(value, toType));
            case ColumnType.SHORT -> row.putShort(toIndex, SqlUtil.implicitCastCharAsByte(value, toType));
            case ColumnType.CHAR -> row.putChar(toIndex, value);
            case ColumnType.INT -> row.putInt(toIndex, SqlUtil.implicitCastCharAsByte(value, toType));
            case ColumnType.LONG -> row.putLong(toIndex, SqlUtil.implicitCastCharAsByte(value, toType));
            case ColumnType.DATE -> row.putDate(toIndex, SqlUtil.implicitCastCharAsByte(value, toType));
            case ColumnType.TIMESTAMP -> row.putTimestamp(toIndex, SqlUtil.implicitCastCharAsByte(value, toType));
            case ColumnType.FLOAT -> row.putFloat(toIndex, SqlUtil.implicitCastCharAsByte(value, toType));
            case ColumnType.DOUBLE -> row.putDouble(toIndex, SqlUtil.implicitCastCharAsByte(value, toType));
            case ColumnType.STRING -> row.putStr(toIndex, value);
            case ColumnType.VARCHAR -> row.putVarchar(toIndex, value);
            case ColumnType.SYMBOL -> row.putSym(toIndex, value);
            case ColumnType.GEOBYTE -> row.putByte(toIndex, SqlUtil.implicitCastCharAsGeoHash(value, toType));
            default -> throw new IllegalStateException("Unexpected value: " + toTypeTag);
        }
    }

    private void copyFromDate(Record record, TableWriter.Row row, int fromIndex, int toIndex, int toTypeTag, TimestampDriver timestampDriver) {
        long value = record.getDate(fromIndex);
        switch (toTypeTag) {
            case ColumnType.BYTE -> row.putByte(toIndex, SqlUtil.implicitCastLongAsByte(value));
            case ColumnType.SHORT -> row.putShort(toIndex, SqlUtil.implicitCastLongAsShort(value));
            case ColumnType.INT -> row.putInt(toIndex, SqlUtil.implicitCastLongAsInt(value));
            case ColumnType.LONG -> row.putLong(toIndex, value);
            case ColumnType.DATE -> row.putDate(toIndex, value);
            case ColumnType.TIMESTAMP -> row.putTimestamp(toIndex, timestampDriver.fromDate(value));
            case ColumnType.FLOAT -> row.putFloat(toIndex, SqlUtil.implicitCastLongAsFloat(value));
            case ColumnType.DOUBLE -> row.putDouble(toIndex, SqlUtil.implicitCastLongAsDouble(value));
            default -> throw new IllegalStateException("Unexpected value: " + toTypeTag);
        }
    }

    private void copyFromDecimal(Record record, TableWriter.Row row, int fromIndex, int toIndex, int fromTypeTag, int fromType, int toType, Decimal128 decimal128, Decimal256 decimal256) {
        switch (fromTypeTag) {
            case ColumnType.DECIMAL8 ->
                    RecordToRowCopierUtils.transferDecimal8(row, toIndex, decimal256, fromType, toType, record.getDecimal8(fromIndex));
            case ColumnType.DECIMAL16 ->
                    RecordToRowCopierUtils.transferDecimal16(row, toIndex, decimal256, fromType, toType, record.getDecimal16(fromIndex));
            case ColumnType.DECIMAL32 ->
                    RecordToRowCopierUtils.transferDecimal32(row, toIndex, decimal256, fromType, toType, record.getDecimal32(fromIndex));
            case ColumnType.DECIMAL64 ->
                    RecordToRowCopierUtils.transferDecimal64(row, toIndex, decimal256, fromType, toType, record.getDecimal64(fromIndex));
            case ColumnType.DECIMAL128 -> {
                record.getDecimal128(fromIndex, decimal128);
                RecordToRowCopierUtils.transferDecimal128(row, toIndex, decimal256, fromType, toType, decimal128);
            }
            case ColumnType.DECIMAL256 -> {
                record.getDecimal256(fromIndex, decimal256);
                RecordToRowCopierUtils.transferDecimal256(row, toIndex, decimal256, fromType, toType);
            }
            default -> throw new IllegalStateException("Unexpected value: " + fromTypeTag);
        }
    }

    private void copyFromDouble(Record record, TableWriter.Row row, int fromIndex, int toIndex, int toTypeTag) {
        double value = record.getDouble(fromIndex);
        switch (toTypeTag) {
            case ColumnType.BYTE -> row.putByte(toIndex, SqlUtil.implicitCastDoubleAsByte(value));
            case ColumnType.SHORT -> row.putShort(toIndex, SqlUtil.implicitCastDoubleAsShort(value));
            case ColumnType.INT -> row.putInt(toIndex, SqlUtil.implicitCastDoubleAsInt(value));
            case ColumnType.LONG -> row.putLong(toIndex, SqlUtil.implicitCastDoubleAsLong(value));
            case ColumnType.DATE -> row.putDate(toIndex, SqlUtil.implicitCastDoubleAsLong(value));
            case ColumnType.TIMESTAMP -> row.putTimestamp(toIndex, SqlUtil.implicitCastDoubleAsLong(value));
            case ColumnType.FLOAT -> row.putFloat(toIndex, SqlUtil.implicitCastDoubleAsFloat(value));
            case ColumnType.DOUBLE -> row.putDouble(toIndex, value);
            default -> throw new IllegalStateException("Unexpected value: " + toTypeTag);
        }
    }

    private void copyFromFloat(Record record, TableWriter.Row row, int fromIndex, int toIndex, int toTypeTag) {
        float value = record.getFloat(fromIndex);
        switch (toTypeTag) {
            case ColumnType.BYTE -> row.putByte(toIndex, SqlUtil.implicitCastFloatAsByte(value));
            case ColumnType.SHORT -> row.putShort(toIndex, SqlUtil.implicitCastFloatAsShort(value));
            case ColumnType.INT -> row.putInt(toIndex, SqlUtil.implicitCastFloatAsInt(value));
            case ColumnType.LONG -> row.putLong(toIndex, SqlUtil.implicitCastFloatAsLong(value));
            case ColumnType.DATE -> row.putDate(toIndex, SqlUtil.implicitCastFloatAsLong(value));
            case ColumnType.TIMESTAMP -> row.putTimestamp(toIndex, SqlUtil.implicitCastFloatAsLong(value));
            case ColumnType.FLOAT -> row.putFloat(toIndex, value);
            case ColumnType.DOUBLE -> row.putDouble(toIndex, SqlUtil.implicitCastFloatAsDouble(value));
            default -> throw new IllegalStateException("Unexpected value: " + toTypeTag);
        }
    }

    private void copyFromGeoByte(Record record, TableWriter.Row row, int fromIndex, int toIndex, int fromType, int toType) {
        byte value = record.getGeoByte(fromIndex);
        if (fromType != toType && fromType != ColumnType.NULL && fromType != ColumnType.GEOBYTE) {
            // Use sign extension (not zero extension) to preserve null sentinel (-1)
            long converted = SqlUtil.implicitCastGeoHashAsGeoHash(value, fromType, toType);
            value = (byte) converted;
        }
        row.putByte(toIndex, value);
    }

    private void copyFromGeoInt(Record record, TableWriter.Row row, int fromIndex, int toIndex, int fromType, int toType, int toTypeTag) {
        int value = record.getGeoInt(fromIndex);
        switch (toTypeTag) {
            case ColumnType.GEOBYTE ->
                    row.putByte(toIndex, (byte) SqlUtil.implicitCastGeoHashAsGeoHash(value, fromType, toType));
            case ColumnType.GEOSHORT ->
                    row.putShort(toIndex, (short) SqlUtil.implicitCastGeoHashAsGeoHash(value, fromType, toType));
            case ColumnType.GEOINT -> {
                long converted;
                if (fromType != toType && fromType != ColumnType.NULL && fromType != ColumnType.GEOINT) {
                    converted = SqlUtil.implicitCastGeoHashAsGeoHash(value, fromType, toType);
                    value = (int) converted;
                }
                row.putInt(toIndex, value);
            }
            default -> throw new IllegalStateException("Unexpected value: " + toTypeTag);
        }
    }

    private void copyFromGeoLong(Record record, TableWriter.Row row, int fromIndex, int toIndex, int fromType, int toType, int toTypeTag) {
        long value = record.getGeoLong(fromIndex);
        switch (toTypeTag) {
            case ColumnType.GEOBYTE ->
                    row.putByte(toIndex, (byte) SqlUtil.implicitCastGeoHashAsGeoHash(value, fromType, toType));
            case ColumnType.GEOSHORT ->
                    row.putShort(toIndex, (short) SqlUtil.implicitCastGeoHashAsGeoHash(value, fromType, toType));
            case ColumnType.GEOINT ->
                    row.putInt(toIndex, (int) SqlUtil.implicitCastGeoHashAsGeoHash(value, fromType, toType));
            case ColumnType.GEOLONG -> {
                if (fromType != toType && fromType != ColumnType.NULL && fromType != ColumnType.GEOLONG) {
                    value = SqlUtil.implicitCastGeoHashAsGeoHash(value, fromType, toType);
                }
                row.putLong(toIndex, value);
            }
            default -> throw new IllegalStateException("Unexpected value: " + toTypeTag);
        }
    }

    private void copyFromGeoShort(Record record, TableWriter.Row row, int fromIndex, int toIndex, int fromType, int toType, int toTypeTag) {
        short value = record.getGeoShort(fromIndex);
        if (toTypeTag == ColumnType.GEOBYTE) {
            long converted = SqlUtil.implicitCastGeoHashAsGeoHash(value, fromType, toType);
            row.putByte(toIndex, (byte) converted);
        } else if (fromType != toType && fromType != ColumnType.NULL && fromType != ColumnType.GEOSHORT) {
            long converted = SqlUtil.implicitCastGeoHashAsGeoHash(value, fromType, toType);
            row.putShort(toIndex, (short) converted);
        } else {
            row.putShort(toIndex, value);
        }
    }

    private void copyFromIPv4(Record record, TableWriter.Row row, int fromIndex, int toIndex) {
        row.putIPv4(toIndex, record.getIPv4(fromIndex));
    }

    private void copyFromInt(Record record, TableWriter.Row row, int fromIndex, int toIndex, int toTypeTag, int toType, Decimal256 decimal256) {
        int value = record.getInt(fromIndex);
        switch (toTypeTag) {
            case ColumnType.BYTE -> row.putByte(toIndex, SqlUtil.implicitCastIntAsByte(value));
            case ColumnType.SHORT -> row.putShort(toIndex, SqlUtil.implicitCastIntAsShort(value));
            case ColumnType.INT -> row.putInt(toIndex, value);
            case ColumnType.LONG -> row.putLong(toIndex, SqlUtil.implicitCastIntAsLong(value));
            case ColumnType.DATE -> row.putDate(toIndex, SqlUtil.implicitCastIntAsLong(value));
            case ColumnType.TIMESTAMP -> row.putTimestamp(toIndex, SqlUtil.implicitCastIntAsLong(value));
            case ColumnType.FLOAT -> row.putFloat(toIndex, SqlUtil.implicitCastIntAsFloat(value));
            case ColumnType.DOUBLE -> row.putDouble(toIndex, SqlUtil.implicitCastIntAsDouble(value));
            default -> {
                if (ColumnType.isDecimalType(toTypeTag)) {
                    RecordToRowCopierUtils.transferIntToDecimal(row, toIndex, value, decimal256, toType);
                }
            }
        }
    }

    private void copyFromLong(Record record, TableWriter.Row row, int fromIndex, int toIndex, int toTypeTag, int toType, Decimal256 decimal256) {
        long value = record.getLong(fromIndex);
        switch (toTypeTag) {
            case ColumnType.BYTE -> row.putByte(toIndex, SqlUtil.implicitCastLongAsByte(value));
            case ColumnType.SHORT -> row.putShort(toIndex, SqlUtil.implicitCastLongAsShort(value));
            case ColumnType.INT -> row.putInt(toIndex, SqlUtil.implicitCastLongAsInt(value));
            case ColumnType.LONG -> row.putLong(toIndex, value);
            case ColumnType.DATE -> row.putDate(toIndex, value);
            case ColumnType.TIMESTAMP -> row.putTimestamp(toIndex, value);
            case ColumnType.FLOAT -> row.putFloat(toIndex, SqlUtil.implicitCastLongAsFloat(value));
            case ColumnType.DOUBLE -> row.putDouble(toIndex, SqlUtil.implicitCastLongAsDouble(value));
            case ColumnType.VARCHAR -> RecordToRowCopierUtils.transferLongToVarcharCol(row, toIndex, value);
            case ColumnType.STRING -> RecordToRowCopierUtils.transferLongToStrCol(row, toIndex, value);
            default -> {
                if (ColumnType.isDecimalType(toTypeTag)) {
                    RecordToRowCopierUtils.transferLongToDecimal(row, toIndex, value, decimal256, toType);
                }
            }
        }
    }

    private void copyFromLong256(Record record, TableWriter.Row row, int fromIndex, int toIndex) {
        row.putLong256(toIndex, record.getLong256A(fromIndex));
    }

    private void copyFromShort(Record record, TableWriter.Row row, int fromIndex, int toIndex, int toTypeTag, int toType, Decimal256 decimal256) {
        short value = record.getShort(fromIndex);
        switch (toTypeTag) {
            case ColumnType.BYTE -> row.putByte(toIndex, SqlUtil.implicitCastShortAsByte(value));
            case ColumnType.SHORT -> row.putShort(toIndex, value);
            case ColumnType.INT -> row.putInt(toIndex, value);
            case ColumnType.LONG -> row.putLong(toIndex, value);
            case ColumnType.DATE -> row.putDate(toIndex, value);
            case ColumnType.TIMESTAMP -> row.putTimestamp(toIndex, value);
            case ColumnType.FLOAT -> row.putFloat(toIndex, value);
            case ColumnType.DOUBLE -> row.putDouble(toIndex, value);
            default -> {
                if (ColumnType.isDecimalType(toTypeTag)) {
                    RecordToRowCopierUtils.transferShortToDecimal(row, toIndex, value, decimal256, toType);
                }
            }
        }
    }

    private void copyFromString(Record record, TableWriter.Row row, int fromIndex, int toIndex, int toTypeTag, int toType, TimestampDriver timestampDriver) {
        CharSequence value = record.getStrA(fromIndex);
        switch (toTypeTag) {
            case ColumnType.ARRAY ->
                    RecordToRowCopierUtils.validateArrayDimensionsAndTransferCol(row, toIndex, arrayParser, value, toType);
            case ColumnType.BYTE -> row.putByte(toIndex, SqlUtil.implicitCastStrAsByte(value));
            case ColumnType.SHORT -> row.putShort(toIndex, SqlUtil.implicitCastStrAsShort(value));
            case ColumnType.CHAR -> row.putChar(toIndex, SqlUtil.implicitCastStrAsChar(value));
            case ColumnType.INT -> row.putInt(toIndex, SqlUtil.implicitCastStrAsInt(value));
            case ColumnType.IPv4 -> row.putIPv4(toIndex, SqlUtil.implicitCastStrAsIPv4(value));
            case ColumnType.LONG -> row.putLong(toIndex, SqlUtil.implicitCastStrAsLong(value));
            case ColumnType.FLOAT -> row.putFloat(toIndex, SqlUtil.implicitCastStrAsFloat(value));
            case ColumnType.DOUBLE -> row.putDouble(toIndex, SqlUtil.implicitCastStrAsDouble(value));
            case ColumnType.SYMBOL -> row.putSym(toIndex, value);
            case ColumnType.DATE -> row.putDate(toIndex, SqlUtil.implicitCastStrAsDate(value));
            case ColumnType.TIMESTAMP -> row.putTimestamp(toIndex, timestampDriver.implicitCast(value));
            case ColumnType.GEOBYTE, ColumnType.GEOSHORT, ColumnType.GEOINT, ColumnType.GEOLONG ->
                    row.putGeoStr(toIndex, value);
            case ColumnType.STRING -> row.putStr(toIndex, value);
            case ColumnType.VARCHAR -> RecordToRowCopierUtils.transferStrToVarcharCol(row, toIndex, value);
            case ColumnType.UUID -> row.putUuid(toIndex, value);
            case ColumnType.LONG256 -> row.putLong256(toIndex, SqlUtil.implicitCastStrAsLong256(value));
            case ColumnType.DECIMAL8, ColumnType.DECIMAL16, ColumnType.DECIMAL32, ColumnType.DECIMAL64,
                 ColumnType.DECIMAL128, ColumnType.DECIMAL256 -> row.putDecimalStr(toIndex, value);
            default -> throw new IllegalStateException("Unexpected value: " + toTypeTag);
        }
    }

    private void copyFromSymbol(Record record, TableWriter.Row row, int fromIndex, int toIndex, int toTypeTag) {
        CharSequence value = record.getSymA(fromIndex);
        switch (toTypeTag) {
            case ColumnType.SYMBOL -> row.putSym(toIndex, value);
            case ColumnType.STRING -> row.putStr(toIndex, value);
            case ColumnType.VARCHAR -> RecordToRowCopierUtils.transferStrToVarcharCol(row, toIndex, value);
            default -> throw new IllegalStateException("Unexpected value: " + toTypeTag);
        }
    }

    private void copyFromTimestamp(Record record, TableWriter.Row row, int fromIndex, int toIndex, int toTypeTag, int fromType, int toType, TimestampDriver timestampDriver) {
        long value = record.getTimestamp(fromIndex);
        switch (toTypeTag) {
            case ColumnType.BYTE -> row.putByte(toIndex, SqlUtil.implicitCastLongAsByte(value));
            case ColumnType.SHORT -> row.putShort(toIndex, SqlUtil.implicitCastLongAsShort(value));
            case ColumnType.INT -> row.putInt(toIndex, SqlUtil.implicitCastLongAsInt(value));
            case ColumnType.LONG -> row.putLong(toIndex, value);
            case ColumnType.FLOAT -> row.putFloat(toIndex, SqlUtil.implicitCastLongAsFloat(value));
            case ColumnType.DOUBLE -> row.putDouble(toIndex, SqlUtil.implicitCastLongAsDouble(value));
            case ColumnType.DATE -> row.putDate(toIndex, timestampDriver.toDate(value));
            case ColumnType.TIMESTAMP -> {
                if (fromType != toType && fromType != ColumnType.NULL) {
                    value = timestampDriver.from(value, fromType);
                }
                row.putTimestamp(toIndex, value);
            }
            default -> throw new IllegalStateException("Unexpected value: " + toTypeTag);
        }
    }

    private void copyFromUuid(Record record, TableWriter.Row row, int fromIndex, int toIndex, int toTypeTag) {
        long lo = record.getLong128Lo(fromIndex);
        long hi = record.getLong128Hi(fromIndex);
        switch (toTypeTag) {
            case ColumnType.LONG128, ColumnType.UUID -> row.putLong128(toIndex, lo, hi);
            case ColumnType.STRING -> RecordToRowCopierUtils.transferUuidToStrCol(row, toIndex, lo, hi);
            case ColumnType.VARCHAR -> RecordToRowCopierUtils.transferUuidToVarcharCol(row, toIndex, lo, hi);
            default -> throw new IllegalStateException("Unexpected value: " + toTypeTag);
        }
    }

    private void copyFromVarchar(Record record, TableWriter.Row row, int fromIndex, int toIndex, int toTypeTag, int toType, TimestampDriver timestampDriver) {
        Utf8Sequence value = record.getVarcharA(fromIndex);
        switch (toTypeTag) {
            case ColumnType.VARCHAR -> row.putVarchar(toIndex, value);
            case ColumnType.ARRAY ->
                    RecordToRowCopierUtils.validateArrayDimensionsAndTransferCol(row, toIndex, arrayParser, value, toType);
            case ColumnType.STRING -> RecordToRowCopierUtils.transferVarcharToStrCol(row, toIndex, value);
            case ColumnType.IPv4 -> row.putInt(toIndex, SqlUtil.implicitCastStrAsIPv4(value));
            case ColumnType.LONG -> row.putLong(toIndex, SqlUtil.implicitCastVarcharAsLong(value));
            case ColumnType.SHORT -> row.putShort(toIndex, SqlUtil.implicitCastVarcharAsShort(value));
            case ColumnType.INT -> row.putInt(toIndex, SqlUtil.implicitCastVarcharAsInt(value));
            case ColumnType.BYTE -> row.putByte(toIndex, SqlUtil.implicitCastVarcharAsByte(value));
            case ColumnType.CHAR -> row.putChar(toIndex, SqlUtil.implicitCastVarcharAsChar(value));
            case ColumnType.FLOAT -> row.putFloat(toIndex, SqlUtil.implicitCastVarcharAsFloat(value));
            case ColumnType.DOUBLE -> row.putDouble(toIndex, SqlUtil.implicitCastVarcharAsDouble(value));
            case ColumnType.UUID -> row.putUuidUtf8(toIndex, value);
            case ColumnType.TIMESTAMP -> row.putTimestamp(toIndex, timestampDriver.implicitCastVarchar(value));
            case ColumnType.SYMBOL -> RecordToRowCopierUtils.transferVarcharToSymbolCol(row, toIndex, value);
            case ColumnType.DATE -> RecordToRowCopierUtils.transferVarcharToDateCol(row, toIndex, value);
            case ColumnType.GEOBYTE, ColumnType.GEOSHORT, ColumnType.GEOINT, ColumnType.GEOLONG ->
                    row.putGeoVarchar(toIndex, value);
            case ColumnType.LONG256 -> row.putLong256Utf8(toIndex, value);
            default -> throw new IllegalStateException("Unexpected value: " + toTypeTag);
        }
    }
}
