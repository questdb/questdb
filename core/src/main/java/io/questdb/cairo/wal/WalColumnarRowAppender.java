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

package io.questdb.cairo.wal;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.StringTypeDriver;
import io.questdb.cairo.SymbolMapReader;
import io.questdb.cairo.VarcharTypeDriver;
import io.questdb.cairo.arr.ArrayTypeDriver;
import io.questdb.cairo.arr.DirectArray;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.cairo.vm.api.MemoryMA;
import io.questdb.cutlass.qwp.protocol.QwpArrayColumnCursor;
import io.questdb.cutlass.qwp.protocol.QwpBooleanColumnCursor;
import io.questdb.cutlass.qwp.protocol.QwpDecimalColumnCursor;
import io.questdb.cutlass.qwp.protocol.QwpFixedWidthColumnCursor;
import io.questdb.cutlass.qwp.protocol.QwpGeoHashColumnCursor;
import io.questdb.cutlass.qwp.protocol.QwpNullBitmap;
import io.questdb.cutlass.qwp.protocol.QwpParseException;
import io.questdb.cutlass.qwp.protocol.QwpStringColumnCursor;
import io.questdb.cutlass.qwp.protocol.QwpSymbolColumnCursor;
import io.questdb.cutlass.qwp.protocol.QwpTimestampColumnCursor;
import io.questdb.cutlass.line.tcp.ClientSymbolCache;
import io.questdb.cutlass.line.tcp.ConnectionSymbolCache;
import io.questdb.std.Decimal256;
import io.questdb.std.Misc;
import io.questdb.std.QuietCloseable;
import static io.questdb.cutlass.qwp.protocol.QwpConstants.TYPE_DECIMAL128;
import static io.questdb.cutlass.qwp.protocol.QwpConstants.TYPE_DECIMAL256;
import static io.questdb.cutlass.qwp.protocol.QwpConstants.TYPE_DECIMAL64;
import static io.questdb.cutlass.qwp.protocol.QwpConstants.TYPE_TIMESTAMP_NANOS;
import io.questdb.std.Decimals;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.Utf8s;

/**
 * Implementation of {@link ColumnarRowAppender} for WAL writers.
 * <p>
 * This class provides optimized bulk column writes, avoiding per-row overhead
 * when ingesting columnar data (like ILP v4).
 * <p>
 * <b>Thread Safety:</b> Not thread-safe. Each WalWriter should have its own instance.
 */
public class WalColumnarRowAppender implements ColumnarRowAppender, QuietCloseable {

    private final WalWriter walWriter;
    private final DirectArray reusableArray = new DirectArray();
    private int pendingRowCount;
    private long startRowId;
    private boolean inColumnarWrite;

    public WalColumnarRowAppender(WalWriter walWriter) {
        this.walWriter = walWriter;
    }

    @Override
    public void close() {
        reusableArray.close();
    }

    @Override
    public void beginColumnarWrite(int rowCount) {
        if (inColumnarWrite) {
            throw new IllegalStateException("Already in columnar write mode");
        }
        // Handle pending segment roll, similar to what newRow() does.
        // This is needed when columns are added while rollSegmentOnNextRow is true,
        // since addColumn defers opening column files until the segment is rolled.
        if (walWriter.rollSegmentOnNextRow) {
            walWriter.rollSegment();
            walWriter.rollSegmentOnNextRow = false;
        }
        this.pendingRowCount = rowCount;
        this.startRowId = walWriter.getSegmentRowCount();
        this.inColumnarWrite = true;
    }

    @Override
    public void putFixedColumn(int columnIndex, long valuesAddress, int valueCount,
                               int valueSize, long nullBitmapAddress, int rowCount) {
        checkInColumnarWrite();

        MemoryMA dataMem = walWriter.getDataColumn(columnIndex);
        int columnType = walWriter.getMetadata().getColumnType(columnIndex);

        if (nullBitmapAddress == 0) {
            // Fast path: no nulls, direct memory copy
            dataMem.putBlockOfBytes(valuesAddress, (long) valueCount * valueSize);
        } else {
            // Slow path: expand sparse to dense, inserting null sentinels
            int valueIdx = 0;
            for (int row = 0; row < rowCount; row++) {
                if (QwpNullBitmap.isNull(nullBitmapAddress, row)) {
                    writeNullSentinel(dataMem, columnType);
                } else {
                    // Copy value from packed array
                    dataMem.putBlockOfBytes(valuesAddress + (long) valueIdx * valueSize, valueSize);
                    valueIdx++;
                }
            }
        }

        walWriter.setRowValueNotNullColumnar(columnIndex, startRowId + rowCount - 1);
    }

    @Override
    public void putFixedColumnNarrowing(int columnIndex, long valuesAddress, int valueCount,
                                        int sourceValueSize, long nullBitmapAddress, int rowCount,
                                        int columnType) {
        checkInColumnarWrite();

        MemoryMA dataMem = walWriter.getDataColumn(columnIndex);
        final boolean hasNulls = nullBitmapAddress != 0;
        int valueIdx = 0;

        switch (ColumnType.tagOf(columnType)) {
            case ColumnType.BYTE:
                for (int row = 0; row < rowCount; row++) {
                    if (hasNulls && QwpNullBitmap.isNull(nullBitmapAddress, row)) {
                        dataMem.putByte((byte) 0);
                    } else {
                        dataMem.putByte((byte) Unsafe.getUnsafe().getLong(valuesAddress + (long) valueIdx * sourceValueSize));
                        valueIdx++;
                    }
                }
                break;
            case ColumnType.SHORT:
                for (int row = 0; row < rowCount; row++) {
                    if (hasNulls && QwpNullBitmap.isNull(nullBitmapAddress, row)) {
                        dataMem.putShort((short) 0);
                    } else {
                        dataMem.putShort((short) Unsafe.getUnsafe().getLong(valuesAddress + (long) valueIdx * sourceValueSize));
                        valueIdx++;
                    }
                }
                break;
            case ColumnType.INT:
                for (int row = 0; row < rowCount; row++) {
                    if (hasNulls && QwpNullBitmap.isNull(nullBitmapAddress, row)) {
                        dataMem.putInt(Numbers.INT_NULL);
                    } else {
                        dataMem.putInt((int) Unsafe.getUnsafe().getLong(valuesAddress + (long) valueIdx * sourceValueSize));
                        valueIdx++;
                    }
                }
                break;
            case ColumnType.FLOAT:
                for (int row = 0; row < rowCount; row++) {
                    if (hasNulls && QwpNullBitmap.isNull(nullBitmapAddress, row)) {
                        dataMem.putFloat(Float.NaN);
                    } else {
                        dataMem.putFloat((float) Unsafe.getUnsafe().getDouble(valuesAddress + (long) valueIdx * sourceValueSize));
                        valueIdx++;
                    }
                }
                break;
            default:
                throw new UnsupportedOperationException(
                        "Narrowing not supported for column type: " + ColumnType.nameOf(columnType));
        }

        walWriter.setRowValueNotNullColumnar(columnIndex, startRowId + rowCount - 1);
    }

    @Override
    public void putFixedToDecimal128Column(int columnIndex, QwpFixedWidthColumnCursor cursor,
                                            int rowCount, int columnType) {
        checkInColumnarWrite();
        MemoryMA dataMem = walWriter.getDataColumn(columnIndex);
        int columnScale = ColumnType.getDecimalScale(columnType);

        cursor.resetRowPosition();
        try {
            for (int row = 0; row < rowCount; row++) {
                cursor.advanceRow();
                if (cursor.isNull()) {
                    dataMem.putDecimal128(Decimals.DECIMAL128_HI_NULL, Decimals.DECIMAL128_LO_NULL);
                } else {
                    Decimal256 decimal = Misc.getThreadLocalDecimal256();
                    decimal.ofRaw(cursor.getLong());
                    decimal.setScale(0);
                    decimal.rescale(columnScale);
                    dataMem.putDecimal128(decimal.getLh(), decimal.getLl());
                }
            }
        } catch (QwpParseException e) {
            throw new RuntimeException("Failed to convert fixed column to DECIMAL128", e);
        }
        walWriter.setRowValueNotNullColumnar(columnIndex, startRowId + rowCount - 1);
    }

    @Override
    public void putFixedToDecimal256Column(int columnIndex, QwpFixedWidthColumnCursor cursor,
                                            int rowCount, int columnType) {
        checkInColumnarWrite();
        MemoryMA dataMem = walWriter.getDataColumn(columnIndex);
        int columnScale = ColumnType.getDecimalScale(columnType);

        cursor.resetRowPosition();
        try {
            for (int row = 0; row < rowCount; row++) {
                cursor.advanceRow();
                if (cursor.isNull()) {
                    dataMem.putDecimal256(Decimals.DECIMAL256_HH_NULL, Decimals.DECIMAL256_HL_NULL,
                            Decimals.DECIMAL256_LH_NULL, Decimals.DECIMAL256_LL_NULL);
                } else {
                    Decimal256 decimal = Misc.getThreadLocalDecimal256();
                    decimal.ofRaw(cursor.getLong());
                    decimal.setScale(0);
                    decimal.rescale(columnScale);
                    dataMem.putDecimal256(decimal.getHh(), decimal.getHl(), decimal.getLh(), decimal.getLl());
                }
            }
        } catch (QwpParseException e) {
            throw new RuntimeException("Failed to convert fixed column to DECIMAL256", e);
        }
        walWriter.setRowValueNotNullColumnar(columnIndex, startRowId + rowCount - 1);
    }

    @Override
    public void putFixedToDecimal64Column(int columnIndex, QwpFixedWidthColumnCursor cursor,
                                           int rowCount, int columnType) {
        checkInColumnarWrite();
        MemoryMA dataMem = walWriter.getDataColumn(columnIndex);
        int columnScale = ColumnType.getDecimalScale(columnType);

        cursor.resetRowPosition();
        try {
            for (int row = 0; row < rowCount; row++) {
                cursor.advanceRow();
                if (cursor.isNull()) {
                    dataMem.putLong(Decimals.DECIMAL64_NULL);
                } else {
                    Decimal256 decimal = Misc.getThreadLocalDecimal256();
                    decimal.ofRaw(cursor.getLong());
                    decimal.setScale(0);
                    decimal.rescale(columnScale);
                    dataMem.putLong(decimal.getLl());
                }
            }
        } catch (QwpParseException e) {
            throw new RuntimeException("Failed to convert fixed column to DECIMAL64", e);
        }
        walWriter.setRowValueNotNullColumnar(columnIndex, startRowId + rowCount - 1);
    }

    @Override
    public void putFixedToSmallDecimalColumn(int columnIndex, QwpFixedWidthColumnCursor cursor,
                                              int rowCount, int columnType) {
        checkInColumnarWrite();
        MemoryMA dataMem = walWriter.getDataColumn(columnIndex);
        int columnScale = ColumnType.getDecimalScale(columnType);

        cursor.resetRowPosition();
        try {
            for (int row = 0; row < rowCount; row++) {
                cursor.advanceRow();
                if (cursor.isNull()) {
                    writeNullSentinel(dataMem, columnType);
                } else {
                    Decimal256 decimal = Misc.getThreadLocalDecimal256();
                    decimal.ofRaw(cursor.getLong());
                    decimal.setScale(0);
                    decimal.rescale(columnScale);
                    switch (ColumnType.tagOf(columnType)) {
                        case ColumnType.DECIMAL8 -> dataMem.putByte((byte) decimal.getLl());
                        case ColumnType.DECIMAL16 -> dataMem.putShort((short) decimal.getLl());
                        case ColumnType.DECIMAL32 -> dataMem.putInt((int) decimal.getLl());
                        default -> throw new UnsupportedOperationException(
                                "Unsupported small decimal type: " + ColumnType.nameOf(columnType));
                    }
                }
            }
        } catch (QwpParseException e) {
            throw new RuntimeException("Failed to convert fixed column to small decimal", e);
        }
        walWriter.setRowValueNotNullColumnar(columnIndex, startRowId + rowCount - 1);
    }

    @Override
    public void putTimestampColumn(int columnIndex, long valuesAddress, int valueCount,
                                   long nullBitmapAddress, int rowCount, long startRowId) {
        checkInColumnarWrite();

        MemoryMA dataMem = walWriter.getDataColumn(columnIndex);
        int timestampIndex = walWriter.getMetadata().getTimestampIndex();
        boolean isDesignated = (columnIndex == timestampIndex);

        if (isDesignated) {
            // Designated timestamp: write 128-bit (timestamp, rowId) pairs
            int valueIdx = 0;
            for (int row = 0; row < rowCount; row++) {
                long timestamp;
                if (nullBitmapAddress != 0 && QwpNullBitmap.isNull(nullBitmapAddress, row)) {
                    timestamp = Numbers.LONG_NULL;
                } else {
                    timestamp = Unsafe.getUnsafe().getLong(valuesAddress + (long) valueIdx * 8);
                    valueIdx++;
                }
                dataMem.putLong128(timestamp, startRowId + row);
            }
        } else {
            // Non-designated timestamp: write as regular LONG column
            if (nullBitmapAddress == 0) {
                dataMem.putBlockOfBytes(valuesAddress, (long) valueCount * 8);
            } else {
                int valueIdx = 0;
                for (int row = 0; row < rowCount; row++) {
                    if (QwpNullBitmap.isNull(nullBitmapAddress, row)) {
                        dataMem.putLong(Numbers.LONG_NULL);
                    } else {
                        dataMem.putLong(Unsafe.getUnsafe().getLong(valuesAddress + (long) valueIdx * 8));
                        valueIdx++;
                    }
                }
            }
        }

        walWriter.setRowValueNotNullColumnar(columnIndex, this.startRowId + rowCount - 1);
    }

    @Override
    public void putCharColumn(int columnIndex, QwpStringColumnCursor cursor, int rowCount) {
        checkInColumnarWrite();

        MemoryMA dataMem = walWriter.getDataColumn(columnIndex);

        cursor.resetRowPosition();
        try {
            for (int row = 0; row < rowCount; row++) {
                cursor.advanceRow();
                if (cursor.isNull()) {
                    dataMem.putChar((char) 0);
                } else {
                    DirectUtf8Sequence value = cursor.getUtf8Value();
                    if (value.size() == 0) {
                        dataMem.putChar((char) 0);
                    } else if (value.size() == 1 && value.byteAt(0) > -1) {
                        // Single ASCII byte
                        dataMem.putChar((char) value.byteAt(0));
                    } else {
                        // Multi-byte UTF-8: decode first codepoint
                        int encodedResult = Utf8s.utf8CharDecode(value);
                        if (Numbers.decodeLowShort(encodedResult) > 0) {
                            dataMem.putChar((char) Numbers.decodeHighShort(encodedResult));
                        } else {
                            dataMem.putChar((char) 0);
                        }
                    }
                }
            }
        } catch (QwpParseException e) {
            throw new RuntimeException("Failed to parse CHAR column", e);
        }

        walWriter.setRowValueNotNullColumnar(columnIndex, startRowId + rowCount - 1);
    }

    @Override
    public void putVarcharColumn(int columnIndex, QwpStringColumnCursor cursor, int rowCount) {
        checkInColumnarWrite();

        MemoryMA dataMem = walWriter.getDataColumn(columnIndex);
        MemoryMA auxMem = walWriter.getAuxColumn(columnIndex);

        cursor.resetRowPosition();
        try {
            for (int row = 0; row < rowCount; row++) {
                cursor.advanceRow();
                if (cursor.isNull()) {
                    VarcharTypeDriver.appendValue(auxMem, dataMem, null);
                } else {
                    DirectUtf8Sequence value = cursor.getUtf8Value();
                    VarcharTypeDriver.appendValue(auxMem, dataMem, value);
                }
            }
        } catch (QwpParseException e) {
            throw new RuntimeException("Failed to parse VARCHAR column", e);
        }

        walWriter.setRowValueNotNullColumnar(columnIndex, startRowId + rowCount - 1);
    }

    @Override
    public void putStringColumn(int columnIndex, QwpStringColumnCursor cursor, int rowCount) {
        checkInColumnarWrite();

        MemoryMA dataMem = walWriter.getDataColumn(columnIndex);
        MemoryMA auxMem = walWriter.getAuxColumn(columnIndex);

        cursor.resetRowPosition();
        try {
            for (int row = 0; row < rowCount; row++) {
                cursor.advanceRow();
                if (cursor.isNull()) {
                    StringTypeDriver.INSTANCE.appendNull(auxMem, dataMem);
                } else {
                    DirectUtf8Sequence utf8Value = cursor.getUtf8Value();
                    // Convert UTF-8 to UTF-16 String for StringTypeDriver
                    String value = Utf8s.toString(utf8Value);
                    StringTypeDriver.appendValue(auxMem, dataMem, value);
                }
            }
        } catch (QwpParseException e) {
            throw new RuntimeException("Failed to parse STRING column", e);
        }

        walWriter.setRowValueNotNullColumnar(columnIndex, startRowId + rowCount - 1);
    }

    @Override
    public boolean putSymbolColumn(int columnIndex, QwpSymbolColumnCursor cursor, int rowCount) {
        return putSymbolColumn(columnIndex, cursor, rowCount, null, 0, 0);
    }

    @Override
    public boolean putSymbolColumn(int columnIndex, QwpSymbolColumnCursor cursor, int rowCount,
                                   ConnectionSymbolCache symbolCache, long tableId, int initialSymbolCount) {
        checkInColumnarWrite();

        MemoryMA dataMem = walWriter.getDataColumn(columnIndex);
        SymbolMapReader symbolMapReader = walWriter.getSymbolMapReader(columnIndex);

        if (symbolMapReader == null) {
            throw new UnsupportedOperationException();
        }

        // Get per-column cache from connection cache
        ClientSymbolCache columnCache = null;
        if (symbolCache != null) {
            columnCache = symbolCache.getCache(tableId, columnIndex);
            // Invalidate cache if watermark changed (segment rolled, symbols committed)
            columnCache.checkAndInvalidate(initialSymbolCount);
        }

        boolean deltaMode = cursor.isDeltaMode();

        cursor.resetRowPosition();
        try {
            for (int row = 0; row < rowCount; row++) {
                cursor.advanceRow();
                if (cursor.isNull()) {
                    dataMem.putInt(SymbolTable.VALUE_IS_NULL);
                    continue;
                }

                int symbolKey;
                int clientSymbolId = cursor.getSymbolIndex();

                // Cache lookup (only in delta mode - global IDs are stable)
                if (deltaMode && columnCache != null) {
                    symbolKey = columnCache.get(clientSymbolId);
                    if (symbolKey != ClientSymbolCache.NO_ENTRY) {
                        // Cache hit - skip string allocation
                        symbolCache.recordHit();
                        dataMem.putInt(symbolKey);
                        continue;
                    }
                }

                // Cache miss - resolve via string lookup
                if (deltaMode && symbolCache != null) {
                    symbolCache.recordMiss();
                }

                String symbolValue = cursor.getSymbolString();
                if (symbolValue == null) {
                    dataMem.putInt(SymbolTable.VALUE_IS_NULL);
                    continue;
                }

                symbolKey = walWriter.resolveSymbol(columnIndex, symbolValue, symbolMapReader);
                dataMem.putInt(symbolKey);

                // Cache if: delta mode + committed (stable) symbol
                if (deltaMode && columnCache != null && symbolKey < initialSymbolCount) {
                    columnCache.put(clientSymbolId, symbolKey);
                }
            }
        } catch (QwpParseException e) {
            throw new RuntimeException("Failed to parse SYMBOL column", e);
        }

        walWriter.setRowValueNotNullColumnar(columnIndex, startRowId + rowCount - 1);
        return true;
    }

    @Override
    public void putBooleanColumn(int columnIndex, QwpBooleanColumnCursor cursor, int rowCount) {
        checkInColumnarWrite();

        MemoryMA dataMem = walWriter.getDataColumn(columnIndex);

        cursor.resetRowPosition();
        try {
            for (int row = 0; row < rowCount; row++) {
                cursor.advanceRow();
                // Boolean in WAL is stored as byte (0 = false, 1 = true)
                // Null booleans are also stored as 0
                dataMem.putByte(cursor.isNull() ? (byte) 0 : (cursor.getValue() ? (byte) 1 : (byte) 0));
            }
        } catch (QwpParseException e) {
            throw new RuntimeException("Failed to parse BOOLEAN column", e);
        }

        walWriter.setRowValueNotNullColumnar(columnIndex, startRowId + rowCount - 1);
    }

    @Override
    public void putNullColumn(int columnIndex, int columnType, int rowCount) {
        checkInColumnarWrite();

        MemoryMA dataMem = walWriter.getDataColumn(columnIndex);

        for (int row = 0; row < rowCount; row++) {
            writeNullSentinel(dataMem, columnType);
        }

        // Mark as having been written (even though all nulls)
        walWriter.setRowValueNotNullColumnar(columnIndex, startRowId + rowCount - 1);
    }

    @Override
    public void putTimestampColumnWithConversion(int columnIndex, QwpTimestampColumnCursor cursor,
                                                  int rowCount, byte ilpType, int columnType,
                                                  boolean isDesignated, long startRowId) throws QwpParseException {
        checkInColumnarWrite();

        MemoryMA dataMem = walWriter.getDataColumn(columnIndex);

        // Determine conversion direction
        // TYPE_TIMESTAMP = microseconds, TYPE_TIMESTAMP_NANOS = nanoseconds
        // ColumnType.TIMESTAMP = microseconds, ColumnType.TIMESTAMP_NANO = nanoseconds
        boolean wireIsNanos = (ilpType == TYPE_TIMESTAMP_NANOS);
        boolean columnIsNanos = (columnType == ColumnType.TIMESTAMP_NANO);

        cursor.resetRowPosition();
        for (int row = 0; row < rowCount; row++) {
            cursor.advanceRow();

            long timestamp;
            if (cursor.isNull()) {
                timestamp = Numbers.LONG_NULL;
            } else {
                timestamp = cursor.getTimestamp();

                // Apply precision conversion
                if (wireIsNanos && !columnIsNanos) {
                    // Wire is nanos, column is micros: divide by 1000
                    timestamp = timestamp / 1000;
                } else if (!wireIsNanos && columnIsNanos) {
                    // Wire is micros, column is nanos: multiply by 1000
                    timestamp = timestamp * 1000;
                }
            }

            if (isDesignated) {
                // Designated timestamp: write 128-bit (timestamp, rowId) pairs
                dataMem.putLong128(timestamp, startRowId + row);
            } else {
                // Non-designated timestamp: write as regular LONG
                dataMem.putLong(timestamp);
            }
        }

        walWriter.setRowValueNotNullColumnar(columnIndex, this.startRowId + rowCount - 1);
    }

    @Override
    public void putGeoHashColumn(int columnIndex, QwpGeoHashColumnCursor cursor,
                                  int rowCount, int columnType) throws QwpParseException {
        checkInColumnarWrite();

        MemoryMA dataMem = walWriter.getDataColumn(columnIndex);

        cursor.resetRowPosition();
        for (int row = 0; row < rowCount; row++) {
            cursor.advanceRow();

            if (cursor.isNull()) {
                // Write appropriate null sentinel based on column type
                switch (ColumnType.tagOf(columnType)) {
                    case ColumnType.GEOBYTE:
                        dataMem.putByte(GeoHashes.BYTE_NULL);
                        break;
                    case ColumnType.GEOSHORT:
                        dataMem.putShort(GeoHashes.SHORT_NULL);
                        break;
                    case ColumnType.GEOINT:
                        dataMem.putInt(GeoHashes.INT_NULL);
                        break;
                    case ColumnType.GEOLONG:
                        dataMem.putLong(GeoHashes.NULL);
                        break;
                    default:
                        throw new UnsupportedOperationException("Invalid GeoHash column type: " + ColumnType.nameOf(columnType));
                }
            } else {
                long geohash = cursor.getGeoHash();
                // Write value with appropriate size based on column type
                switch (ColumnType.tagOf(columnType)) {
                    case ColumnType.GEOBYTE:
                        dataMem.putByte((byte) geohash);
                        break;
                    case ColumnType.GEOSHORT:
                        dataMem.putShort((short) geohash);
                        break;
                    case ColumnType.GEOINT:
                        dataMem.putInt((int) geohash);
                        break;
                    case ColumnType.GEOLONG:
                        dataMem.putLong(geohash);
                        break;
                    default:
                        throw new UnsupportedOperationException("Invalid GeoHash column type: " + ColumnType.nameOf(columnType));
                }
            }
        }

        walWriter.setRowValueNotNullColumnar(columnIndex, startRowId + rowCount - 1);
    }

    @Override
    public void putDecimal64Column(int columnIndex, QwpDecimalColumnCursor cursor,
                                    int rowCount, int columnType) throws QwpParseException {
        checkInColumnarWrite();

        MemoryMA dataMem = walWriter.getDataColumn(columnIndex);
        int wireScale = cursor.getScale() & 0xFF;
        int columnScale = ColumnType.getDecimalScale(columnType);
        boolean needsRescale = (wireScale != columnScale);

        cursor.resetRowPosition();
        for (int row = 0; row < rowCount; row++) {
            cursor.advanceRow();

            if (cursor.isNull()) {
                dataMem.putLong(Decimals.DECIMAL64_NULL);
            } else if (needsRescale) {
                Decimal256 decimal = Misc.getThreadLocalDecimal256();
                decimal.ofRaw(cursor.getDecimal64());
                decimal.setScale(wireScale);
                decimal.rescale(columnScale);
                dataMem.putLong(decimal.getLl());  // DECIMAL64 uses lowest 64 bits
            } else {
                dataMem.putLong(cursor.getDecimal64());
            }
        }

        walWriter.setRowValueNotNullColumnar(columnIndex, startRowId + rowCount - 1);
    }

    @Override
    public void putDecimal128Column(int columnIndex, QwpDecimalColumnCursor cursor,
                                     int rowCount, int columnType) throws QwpParseException {
        checkInColumnarWrite();

        MemoryMA dataMem = walWriter.getDataColumn(columnIndex);
        int wireScale = cursor.getScale() & 0xFF;
        int columnScale = ColumnType.getDecimalScale(columnType);
        boolean needsRescale = (wireScale != columnScale);

        cursor.resetRowPosition();
        for (int row = 0; row < rowCount; row++) {
            cursor.advanceRow();

            if (cursor.isNull()) {
                dataMem.putDecimal128(Decimals.DECIMAL128_HI_NULL, Decimals.DECIMAL128_LO_NULL);
            } else if (needsRescale) {
                Decimal256 decimal = Misc.getThreadLocalDecimal256();
                decimal.ofRaw(cursor.getDecimal128Hi(), cursor.getDecimal128Lo());
                decimal.setScale(wireScale);
                decimal.rescale(columnScale);
                dataMem.putDecimal128(decimal.getLh(), decimal.getLl());  // DECIMAL128 uses lower 128 bits (lh, ll)
            } else {
                dataMem.putDecimal128(cursor.getDecimal128Hi(), cursor.getDecimal128Lo());
            }
        }

        walWriter.setRowValueNotNullColumnar(columnIndex, startRowId + rowCount - 1);
    }

    @Override
    public void putDecimal256Column(int columnIndex, QwpDecimalColumnCursor cursor,
                                     int rowCount, int columnType) throws QwpParseException {
        checkInColumnarWrite();

        MemoryMA dataMem = walWriter.getDataColumn(columnIndex);
        int wireScale = cursor.getScale() & 0xFF;
        int columnScale = ColumnType.getDecimalScale(columnType);
        boolean needsRescale = (wireScale != columnScale);

        cursor.resetRowPosition();
        for (int row = 0; row < rowCount; row++) {
            cursor.advanceRow();

            if (cursor.isNull()) {
                dataMem.putDecimal256(Decimals.DECIMAL256_HH_NULL, Decimals.DECIMAL256_HL_NULL,
                        Decimals.DECIMAL256_LH_NULL, Decimals.DECIMAL256_LL_NULL);
            } else if (needsRescale) {
                Decimal256 decimal = Misc.getThreadLocalDecimal256();
                decimal.ofRaw(
                        cursor.getDecimal256Hh(),
                        cursor.getDecimal256Hl(),
                        cursor.getDecimal256Lh(),
                        cursor.getDecimal256Ll()
                );
                decimal.setScale(wireScale);
                decimal.rescale(columnScale);
                dataMem.putDecimal256(decimal.getHh(), decimal.getHl(), decimal.getLh(), decimal.getLl());
            } else {
                dataMem.putDecimal256(
                        cursor.getDecimal256Hh(),
                        cursor.getDecimal256Hl(),
                        cursor.getDecimal256Lh(),
                        cursor.getDecimal256Ll()
                );
            }
        }

        walWriter.setRowValueNotNullColumnar(columnIndex, startRowId + rowCount - 1);
    }

    @Override
    public void putDecimalToSmallDecimalColumn(int columnIndex, QwpDecimalColumnCursor cursor,
                                                int rowCount, int columnType) throws QwpParseException {
        checkInColumnarWrite();

        MemoryMA dataMem = walWriter.getDataColumn(columnIndex);
        int wireScale = cursor.getScale() & 0xFF;
        int columnScale = ColumnType.getDecimalScale(columnType);
        byte wireType = cursor.getTypeCode();

        cursor.resetRowPosition();
        for (int row = 0; row < rowCount; row++) {
            cursor.advanceRow();

            if (cursor.isNull()) {
                writeNullSentinel(dataMem, columnType);
            } else {
                Decimal256 decimal = Misc.getThreadLocalDecimal256();
                loadDecimalFromCursor(decimal, cursor, wireType);
                decimal.setScale(wireScale);
                decimal.rescale(columnScale);
                switch (ColumnType.tagOf(columnType)) {
                    case ColumnType.DECIMAL8 -> dataMem.putByte((byte) decimal.getLl());
                    case ColumnType.DECIMAL16 -> dataMem.putShort((short) decimal.getLl());
                    case ColumnType.DECIMAL32 -> dataMem.putInt((int) decimal.getLl());
                    default -> throw new UnsupportedOperationException(
                            "Unsupported small decimal type: " + ColumnType.nameOf(columnType));
                }
            }
        }

        walWriter.setRowValueNotNullColumnar(columnIndex, startRowId + rowCount - 1);
    }

    @Override
    public void putArrayColumn(int columnIndex, QwpArrayColumnCursor cursor,
                                int rowCount, int columnType) throws QwpParseException {
        checkInColumnarWrite();

        MemoryMA auxMem = walWriter.getAuxColumn(columnIndex);
        MemoryMA dataMem = walWriter.getDataColumn(columnIndex);

        cursor.resetRowPosition();
        for (int row = 0; row < rowCount; row++) {
            cursor.advanceRow();

            if (cursor.isNull()) {
                // Append null: write offset and zero size
                auxMem.putLong(dataMem.getAppendOffset());
                auxMem.putLong(0);  // size & padding = 0 indicates null
            } else {
                // Build array from cursor data
                int nDims = cursor.getNDims();
                int totalElements = cursor.getTotalElements();

                // Determine element type and create encoded type
                short elemType = cursor.isDoubleArray() ? ColumnType.DOUBLE : ColumnType.LONG;
                int encodedType = ColumnType.encodeArrayType(elemType, nDims);

                // Set up the reusable array with the correct type and shape
                reusableArray.setType(encodedType);
                for (int d = 0; d < nDims; d++) {
                    reusableArray.setDimLen(d, cursor.getDimSize(d));
                }
                reusableArray.applyShape();

                // Copy data from cursor to reusable array
                MemoryA arrayMem = reusableArray.startMemoryA();
                long srcAddr = cursor.getValuesAddress();
                if (cursor.isDoubleArray()) {
                    for (int i = 0; i < totalElements; i++) {
                        arrayMem.putDouble(io.questdb.std.Unsafe.getUnsafe().getDouble(srcAddr + (long) i * 8));
                    }
                } else {
                    for (int i = 0; i < totalElements; i++) {
                        arrayMem.putLong(io.questdb.std.Unsafe.getUnsafe().getLong(srcAddr + (long) i * 8));
                    }
                }

                ArrayTypeDriver.appendValue(auxMem, dataMem, reusableArray);
            }
        }

        walWriter.setRowValueNotNullColumnar(columnIndex, startRowId + rowCount - 1);
    }

    @Override
    public void endColumnarWrite(long minTimestamp, long maxTimestamp, boolean outOfOrder) {
        checkInColumnarWrite();

        // Update WAL writer state
        walWriter.finishColumnarWrite(pendingRowCount, minTimestamp, maxTimestamp, outOfOrder);

        inColumnarWrite = false;
        pendingRowCount = 0;
    }

    @Override
    public void cancelColumnarWrite() {
        if (!inColumnarWrite) {
            return;
        }

        // Reset append positions to before the write started
        walWriter.cancelColumnarWrite(startRowId);

        inColumnarWrite = false;
        pendingRowCount = 0;
    }

    private static void loadDecimalFromCursor(Decimal256 decimal, QwpDecimalColumnCursor cursor, byte wireType) {
        switch (wireType) {
            case TYPE_DECIMAL64 -> decimal.ofRaw(cursor.getDecimal64());
            case TYPE_DECIMAL128 -> decimal.ofRaw(cursor.getDecimal128Hi(), cursor.getDecimal128Lo());
            case TYPE_DECIMAL256 -> decimal.ofRaw(
                    cursor.getDecimal256Hh(), cursor.getDecimal256Hl(),
                    cursor.getDecimal256Lh(), cursor.getDecimal256Ll()
            );
            default -> throw new UnsupportedOperationException("Unknown decimal wire type: " + wireType);
        }
    }

    private void checkInColumnarWrite() {
        if (!inColumnarWrite) {
            throw new IllegalStateException("Not in columnar write mode. Call beginColumnarWrite() first.");
        }
    }

    /**
     * Writes the appropriate null sentinel value for the given column type.
     */
    private static void writeNullSentinel(MemoryMA dataMem, int columnType) {
        switch (ColumnType.tagOf(columnType)) {
            case ColumnType.BOOLEAN:
            case ColumnType.BYTE:
                dataMem.putByte((byte) 0);
                break;
            case ColumnType.SHORT:
            case ColumnType.CHAR:
                dataMem.putShort((short) 0);
                break;
            case ColumnType.INT:
                dataMem.putInt(Numbers.INT_NULL);
                break;
            case ColumnType.IPv4:
                dataMem.putInt(Numbers.IPv4_NULL);
                break;
            case ColumnType.SYMBOL:
                dataMem.putInt(SymbolTable.VALUE_IS_NULL);
                break;
            case ColumnType.FLOAT:
                dataMem.putFloat(Float.NaN);
                break;
            case ColumnType.LONG:
            case ColumnType.DATE:
            case ColumnType.TIMESTAMP:
                dataMem.putLong(Numbers.LONG_NULL);
                break;
            case ColumnType.DOUBLE:
                dataMem.putDouble(Double.NaN);
                break;
            case ColumnType.UUID:
            case ColumnType.LONG128:
                dataMem.putLong128(Numbers.LONG_NULL, Numbers.LONG_NULL);
                break;
            case ColumnType.LONG256:
                dataMem.putLong256(Numbers.LONG_NULL, Numbers.LONG_NULL, Numbers.LONG_NULL, Numbers.LONG_NULL);
                break;
            case ColumnType.GEOBYTE:
                dataMem.putByte(GeoHashes.BYTE_NULL);
                break;
            case ColumnType.GEOSHORT:
                dataMem.putShort(GeoHashes.SHORT_NULL);
                break;
            case ColumnType.GEOINT:
                dataMem.putInt(GeoHashes.INT_NULL);
                break;
            case ColumnType.GEOLONG:
                dataMem.putLong(GeoHashes.NULL);
                break;
            case ColumnType.DECIMAL8:
                dataMem.putByte(Decimals.DECIMAL8_NULL);
                break;
            case ColumnType.DECIMAL16:
                dataMem.putShort(Decimals.DECIMAL16_NULL);
                break;
            case ColumnType.DECIMAL32:
                dataMem.putInt(Decimals.DECIMAL32_NULL);
                break;
            case ColumnType.DECIMAL64:
                dataMem.putLong(Decimals.DECIMAL64_NULL);
                break;
            case ColumnType.DECIMAL128:
                dataMem.putDecimal128(Decimals.DECIMAL128_HI_NULL, Decimals.DECIMAL128_LO_NULL);
                break;
            case ColumnType.DECIMAL256:
                dataMem.putDecimal256(Decimals.DECIMAL256_HH_NULL, Decimals.DECIMAL256_HL_NULL,
                        Decimals.DECIMAL256_LH_NULL, Decimals.DECIMAL256_LL_NULL);
                break;
            default:
                throw new UnsupportedOperationException("Unsupported column type for null sentinel: "
                        + ColumnType.nameOf(columnType));
        }
    }
}
