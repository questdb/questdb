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
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.vm.api.MemoryMA;
import io.questdb.cutlass.ilpv4.protocol.IlpV4BooleanColumnCursor;
import io.questdb.cutlass.ilpv4.protocol.IlpV4NullBitmap;
import io.questdb.cutlass.ilpv4.protocol.IlpV4ParseException;
import io.questdb.cutlass.ilpv4.protocol.IlpV4StringColumnCursor;
import io.questdb.cutlass.ilpv4.protocol.IlpV4SymbolColumnCursor;
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
public class WalColumnarRowAppender implements ColumnarRowAppender {

    private final WalWriter walWriter;
    private int pendingRowCount;
    private long startRowId;
    private boolean inColumnarWrite;

    public WalColumnarRowAppender(WalWriter walWriter) {
        this.walWriter = walWriter;
    }

    @Override
    public void beginColumnarWrite(int rowCount) {
        if (inColumnarWrite) {
            throw new IllegalStateException("Already in columnar write mode");
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
                if (IlpV4NullBitmap.isNull(nullBitmapAddress, row)) {
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
                if (nullBitmapAddress != 0 && IlpV4NullBitmap.isNull(nullBitmapAddress, row)) {
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
                    if (IlpV4NullBitmap.isNull(nullBitmapAddress, row)) {
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
    public void putVarcharColumn(int columnIndex, IlpV4StringColumnCursor cursor, int rowCount) {
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
        } catch (IlpV4ParseException e) {
            throw new RuntimeException("Failed to parse VARCHAR column", e);
        }

        walWriter.setRowValueNotNullColumnar(columnIndex, startRowId + rowCount - 1);
    }

    @Override
    public void putStringColumn(int columnIndex, IlpV4StringColumnCursor cursor, int rowCount) {
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
        } catch (IlpV4ParseException e) {
            throw new RuntimeException("Failed to parse STRING column", e);
        }

        walWriter.setRowValueNotNullColumnar(columnIndex, startRowId + rowCount - 1);
    }

    @Override
    public boolean putSymbolColumn(int columnIndex, IlpV4SymbolColumnCursor cursor, int rowCount) {
        checkInColumnarWrite();

        MemoryMA dataMem = walWriter.getDataColumn(columnIndex);
        SymbolMapReader symbolMapReader = walWriter.getSymbolMapReader(columnIndex);

        if (symbolMapReader == null) {
            throw new UnsupportedOperationException();
        }

        cursor.resetRowPosition();
        try {
            for (int row = 0; row < rowCount; row++) {
                cursor.advanceRow();
                if (cursor.isNull()) {
                    dataMem.putInt(SymbolTable.VALUE_IS_NULL);
                } else {
                    String symbolValue = cursor.getSymbolString();
                    if (symbolValue == null) {
                        dataMem.putInt(SymbolTable.VALUE_IS_NULL);
                    } else {
                        int symbolKey = walWriter.resolveSymbol(columnIndex, symbolValue, symbolMapReader);
                        dataMem.putInt(symbolKey);
                    }
                }
            }
        } catch (IlpV4ParseException e) {
            throw new RuntimeException("Failed to parse SYMBOL column", e);
        }

        walWriter.setRowValueNotNullColumnar(columnIndex, startRowId + rowCount - 1);
        return true;
    }

    @Override
    public void putBooleanColumn(int columnIndex, IlpV4BooleanColumnCursor cursor, int rowCount) {
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
        } catch (IlpV4ParseException e) {
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
