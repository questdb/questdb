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

package io.questdb.cairo.wal;

import io.questdb.cairo.CairoException;
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
import io.questdb.cutlass.line.tcp.ClientSymbolCache;
import io.questdb.cutlass.line.tcp.ConnectionSymbolCache;
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
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimal64;
import io.questdb.std.Decimals;
import io.questdb.std.Long256Impl;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.Uuid;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.datetime.millitime.DateFormatUtils;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;

import static io.questdb.cutlass.qwp.protocol.QwpConstants.*;

/**
 * Implementation of {@link ColumnarRowAppender} for WAL writers.
 * <p>
 * This class provides optimized bulk column writes, avoiding per-row overhead
 * when ingesting columnar data (like QWP v1).
 * <p>
 * <b>Thread Safety:</b> Not thread-safe. Each WalWriter should have its own instance.
 */
//TODO [mtopolnik]: This class shouldn't have a dependency to the Wire protocol layer.
// This works for now because QWP is the only user of columnar appending. When introducing
// the second user, clean up the architecture.
public class WalColumnarRowAppender implements ColumnarRowAppender, QuietCloseable {

    private final Decimal256 decimal = new Decimal256();
    private final Decimal128 decimal128 = new Decimal128();
    private final Decimal64 decimal64 = new Decimal64();
    private final Long256Impl long256 = new Long256Impl();
    private final DirectArray reusableArray = new DirectArray();
    private final StringSink strSink = new StringSink();
    private final Utf8StringSink utf8Sink = new Utf8StringSink();
    private final WalWriter walWriter;
    private boolean inColumnarWrite;
    private int pendingRowCount;
    private long startRowId;

    public WalColumnarRowAppender(WalWriter walWriter) {
        this.walWriter = walWriter;
    }

    @Override
    public void beginColumnarWrite(int rowCount) {
        walWriter.checkDistressed();
        if (inColumnarWrite) {
            throw CairoException.nonCritical().put("already in columnar write mode");
        }
        // Handle pending segment roll, similar to what newRow() does.
        // This is needed when columns are added while rollSegmentOnNextRow is true,
        // since addColumn defers opening column files until the segment is rolled.
        try {
            if (walWriter.rollSegmentOnNextRow) {
                walWriter.rollSegment();
                walWriter.rollSegmentOnNextRow = false;
            }
        } catch (Throwable e) {
            walWriter.distressed = true;
            throw e;
        }
        this.pendingRowCount = rowCount;
        this.startRowId = walWriter.getSegmentRowCount();
        this.inColumnarWrite = true;
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

    @Override
    public void close() {
        reusableArray.close();
    }

    @Override
    public void endColumnarWrite(long minTimestamp, long maxTimestamp, boolean outOfOrder) {
        checkInColumnarWrite();

        // Update WAL writer state
        walWriter.finishColumnarWrite(pendingRowCount, minTimestamp, maxTimestamp, outOfOrder);

        inColumnarWrite = false;
        pendingRowCount = 0;
    }

    public boolean isInColumnarWrite() {
        return inColumnarWrite;
    }

    @Override
    public void putArrayColumn(
            int columnIndex,
            QwpArrayColumnCursor cursor,
            int rowCount,
            int columnType
    ) {
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

                // Bulk copy data from cursor to reusable array.
                // Wire format and storage format are both 8-byte little-endian,
                // so we can use a single memcpy instead of element-by-element.
                MemoryA arrayMem = reusableArray.startMemoryA();
                if (totalElements > 0) {
                    arrayMem.putBlockOfBytes(cursor.getValuesAddress(), (long) totalElements * 8);
                }

                ArrayTypeDriver.appendValue(auxMem, dataMem, reusableArray);
            }
        }

        walWriter.setRowValueNotNullColumnar(columnIndex, startRowId + rowCount - 1);
    }

    @Override
    public void putBooleanColumn(int columnIndex, QwpBooleanColumnCursor cursor, int rowCount) {
        checkInColumnarWrite();

        MemoryMA dataMem = walWriter.getDataColumn(columnIndex);

        cursor.resetRowPosition();
        for (int row = 0; row < rowCount; row++) {
            cursor.advanceRow();
            // Boolean in WAL is stored as byte (0 = false, 1 = true)
            // Null booleans are also stored as 0
            dataMem.putByte(cursor.isNull() ? (byte) 0 : (cursor.getValue() ? (byte) 1 : (byte) 0));
        }

        walWriter.setRowValueNotNullColumnar(columnIndex, startRowId + rowCount - 1);
    }

    @Override
    public void putBooleanToNumericColumn(int columnIndex, QwpBooleanColumnCursor cursor, int rowCount, int columnType) {
        checkInColumnarWrite();
        MemoryMA dataMem = walWriter.getDataColumn(columnIndex);

        cursor.resetRowPosition();
        switch (ColumnType.tagOf(columnType)) {
            case ColumnType.BYTE -> {
                for (int row = 0; row < rowCount; row++) {
                    cursor.advanceRow();
                    dataMem.putByte(cursor.isNull() ? (byte) 0 : (cursor.getValue() ? (byte) 1 : (byte) 0));
                }
            }
            case ColumnType.SHORT -> {
                for (int row = 0; row < rowCount; row++) {
                    cursor.advanceRow();
                    dataMem.putShort(cursor.isNull() ? (short) 0 : (cursor.getValue() ? (short) 1 : (short) 0));
                }
            }
            case ColumnType.INT -> {
                for (int row = 0; row < rowCount; row++) {
                    cursor.advanceRow();
                    dataMem.putInt(cursor.isNull() ? Numbers.INT_NULL : (cursor.getValue() ? 1 : 0));
                }
            }
            case ColumnType.LONG -> {
                for (int row = 0; row < rowCount; row++) {
                    cursor.advanceRow();
                    dataMem.putLong(cursor.isNull() ? Numbers.LONG_NULL : (cursor.getValue() ? 1L : 0L));
                }
            }
            case ColumnType.FLOAT -> {
                for (int row = 0; row < rowCount; row++) {
                    cursor.advanceRow();
                    dataMem.putFloat(cursor.isNull() ? Float.NaN : (cursor.getValue() ? 1f : 0f));
                }
            }
            case ColumnType.DOUBLE -> {
                for (int row = 0; row < rowCount; row++) {
                    cursor.advanceRow();
                    dataMem.putDouble(cursor.isNull() ? Double.NaN : (cursor.getValue() ? 1d : 0d));
                }
            }
            default -> throw CairoException.nonCritical()
                    .put("unsupported boolean-to-numeric target type: ").put(ColumnType.nameOf(columnType));
        }
        walWriter.setRowValueNotNullColumnar(columnIndex, startRowId + rowCount - 1);
    }

    @Override
    public void putBooleanToStringColumn(int columnIndex, QwpBooleanColumnCursor cursor, int rowCount) {
        checkInColumnarWrite();
        MemoryMA dataMem = walWriter.getDataColumn(columnIndex);
        MemoryMA auxMem = walWriter.getAuxColumn(columnIndex);

        cursor.resetRowPosition();
        for (int row = 0; row < rowCount; row++) {
            cursor.advanceRow();
            if (cursor.isNull()) {
                StringTypeDriver.INSTANCE.appendNull(auxMem, dataMem);
            } else {
                strSink.clear();
                strSink.put(Boolean.toString(cursor.getValue()));
                StringTypeDriver.appendValue(auxMem, dataMem, strSink);
            }
        }
        walWriter.setRowValueNotNullColumnar(columnIndex, startRowId + rowCount - 1);
    }

    @Override
    public void putBooleanToVarcharColumn(int columnIndex, QwpBooleanColumnCursor cursor, int rowCount) {
        checkInColumnarWrite();
        MemoryMA dataMem = walWriter.getDataColumn(columnIndex);
        MemoryMA auxMem = walWriter.getAuxColumn(columnIndex);

        cursor.resetRowPosition();
        for (int row = 0; row < rowCount; row++) {
            cursor.advanceRow();
            if (cursor.isNull()) {
                VarcharTypeDriver.appendValue(auxMem, dataMem, null);
            } else {
                utf8Sink.clear();
                utf8Sink.put(Boolean.toString(cursor.getValue()));
                VarcharTypeDriver.appendValue(auxMem, dataMem, utf8Sink);
            }
        }
        walWriter.setRowValueNotNullColumnar(columnIndex, startRowId + rowCount - 1);
    }

    @Override
    public void putCharColumn(int columnIndex, QwpStringColumnCursor cursor, int rowCount) throws QwpParseException {
        checkInColumnarWrite();

        MemoryMA dataMem = walWriter.getDataColumn(columnIndex);

        cursor.resetRowPosition();
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

        walWriter.setRowValueNotNullColumnar(columnIndex, startRowId + rowCount - 1);
    }

    @Override
    public void putDecimal128Column(
            int columnIndex,
            QwpDecimalColumnCursor cursor,
            int rowCount,
            int columnType
    ) {
        putDecimalToDecimalColumn(columnIndex, cursor, rowCount, columnType);
    }

    @Override
    public void putDecimal256Column(
            int columnIndex, QwpDecimalColumnCursor cursor, int rowCount, int columnType
    ) {
        putDecimalToDecimalColumn(columnIndex, cursor, rowCount, columnType);
    }

    @Override
    public void putDecimal64Column(
            int columnIndex, QwpDecimalColumnCursor cursor, int rowCount, int columnType
    ) {
        putDecimalToDecimalColumn(columnIndex, cursor, rowCount, columnType);
    }

    @Override
    public void putDecimalToSmallDecimalColumn(
            int columnIndex, QwpDecimalColumnCursor cursor, int rowCount, int columnType
    ) {
        putDecimalToDecimalColumn(columnIndex, cursor, rowCount, columnType);
    }

    @Override
    public void putDecimalToStringColumn(int columnIndex, QwpDecimalColumnCursor cursor, int rowCount) {
        checkInColumnarWrite();
        MemoryMA dataMem = walWriter.getDataColumn(columnIndex);
        MemoryMA auxMem = walWriter.getAuxColumn(columnIndex);
        int wireScale = cursor.getScale() & 0xFF;
        byte wireType = cursor.getTypeCode();

        cursor.resetRowPosition();
        switch (wireType) {
            case TYPE_DECIMAL64 -> {
                for (int row = 0; row < rowCount; row++) {
                    cursor.advanceRow();
                    if (cursor.isNull()) {
                        StringTypeDriver.INSTANCE.appendNull(auxMem, dataMem);
                    } else {
                        decimal.ofRaw(cursor.getDecimal64());
                        decimal.setScale(wireScale);
                        strSink.clear();
                        decimal.toSink(strSink);
                        StringTypeDriver.appendValue(auxMem, dataMem, strSink);
                    }
                }
            }
            case TYPE_DECIMAL128 -> {
                for (int row = 0; row < rowCount; row++) {
                    cursor.advanceRow();
                    if (cursor.isNull()) {
                        StringTypeDriver.INSTANCE.appendNull(auxMem, dataMem);
                    } else {
                        decimal.ofRaw(cursor.getDecimal128Hi(), cursor.getDecimal128Lo());
                        decimal.setScale(wireScale);
                        strSink.clear();
                        decimal.toSink(strSink);
                        StringTypeDriver.appendValue(auxMem, dataMem, strSink);
                    }
                }
            }
            case TYPE_DECIMAL256 -> {
                for (int row = 0; row < rowCount; row++) {
                    cursor.advanceRow();
                    if (cursor.isNull()) {
                        StringTypeDriver.INSTANCE.appendNull(auxMem, dataMem);
                    } else {
                        decimal.ofRaw(
                                cursor.getDecimal256Hh(), cursor.getDecimal256Hl(),
                                cursor.getDecimal256Lh(), cursor.getDecimal256Ll()
                        );
                        decimal.setScale(wireScale);
                        strSink.clear();
                        decimal.toSink(strSink);
                        StringTypeDriver.appendValue(auxMem, dataMem, strSink);
                    }
                }
            }
            default -> throw CairoException.nonCritical()
                    .put("unknown decimal wire type: ").put(wireType);
        }
        walWriter.setRowValueNotNullColumnar(columnIndex, startRowId + rowCount - 1);
    }

    @Override
    public void putDecimalToVarcharColumn(int columnIndex, QwpDecimalColumnCursor cursor, int rowCount) {
        checkInColumnarWrite();
        MemoryMA dataMem = walWriter.getDataColumn(columnIndex);
        MemoryMA auxMem = walWriter.getAuxColumn(columnIndex);
        int wireScale = cursor.getScale() & 0xFF;
        byte wireType = cursor.getTypeCode();

        cursor.resetRowPosition();
        switch (wireType) {
            case TYPE_DECIMAL64 -> {
                for (int row = 0; row < rowCount; row++) {
                    cursor.advanceRow();
                    if (cursor.isNull()) {
                        VarcharTypeDriver.appendValue(auxMem, dataMem, null);
                    } else {
                        decimal.ofRaw(cursor.getDecimal64());
                        decimal.setScale(wireScale);
                        utf8Sink.clear();
                        decimal.toSink(utf8Sink);
                        VarcharTypeDriver.appendValue(auxMem, dataMem, utf8Sink);
                    }
                }
            }
            case TYPE_DECIMAL128 -> {
                for (int row = 0; row < rowCount; row++) {
                    cursor.advanceRow();
                    if (cursor.isNull()) {
                        VarcharTypeDriver.appendValue(auxMem, dataMem, null);
                    } else {
                        decimal.ofRaw(cursor.getDecimal128Hi(), cursor.getDecimal128Lo());
                        decimal.setScale(wireScale);
                        utf8Sink.clear();
                        decimal.toSink(utf8Sink);
                        VarcharTypeDriver.appendValue(auxMem, dataMem, utf8Sink);
                    }
                }
            }
            case TYPE_DECIMAL256 -> {
                for (int row = 0; row < rowCount; row++) {
                    cursor.advanceRow();
                    if (cursor.isNull()) {
                        VarcharTypeDriver.appendValue(auxMem, dataMem, null);
                    } else {
                        decimal.ofRaw(
                                cursor.getDecimal256Hh(), cursor.getDecimal256Hl(),
                                cursor.getDecimal256Lh(), cursor.getDecimal256Ll()
                        );
                        decimal.setScale(wireScale);
                        utf8Sink.clear();
                        decimal.toSink(utf8Sink);
                        VarcharTypeDriver.appendValue(auxMem, dataMem, utf8Sink);
                    }
                }
            }
            default -> throw CairoException.nonCritical()
                    .put("unknown decimal wire type: ").put(wireType);
        }
        walWriter.setRowValueNotNullColumnar(columnIndex, startRowId + rowCount - 1);
    }

    @Override
    public void putFixedColumn(
            int columnIndex, long valuesAddress, int valueCount, int valueSize, long nullBitmapAddress, int rowCount
    ) {
        checkInColumnarWrite();

        MemoryMA dataMem = walWriter.getDataColumn(columnIndex);
        int columnType = walWriter.getMetadata().getColumnType(columnIndex);

        if (nullBitmapAddress == 0) {
            // Fast path: no nulls, direct memory copy
            dataMem.putBlockOfBytes(valuesAddress, (long) valueCount * valueSize);
        } else {
            // Expand sparse to dense, inserting null sentinels
            int valueIdx = 0;
            switch (ColumnType.tagOf(columnType)) {
                case ColumnType.BYTE -> {
                    for (int row = 0; row < rowCount; row++) {
                        if (QwpNullBitmap.isNull(nullBitmapAddress, row)) {
                            dataMem.putByte((byte) 0);
                        } else {
                            dataMem.putByte(Unsafe.getUnsafe().getByte(valuesAddress + valueIdx));
                            valueIdx++;
                        }
                    }
                }
                case ColumnType.SHORT, ColumnType.CHAR -> {
                    for (int row = 0; row < rowCount; row++) {
                        if (QwpNullBitmap.isNull(nullBitmapAddress, row)) {
                            dataMem.putShort((short) 0);
                        } else {
                            dataMem.putShort(Unsafe.getUnsafe().getShort(valuesAddress + (long) valueIdx * 2));
                            valueIdx++;
                        }
                    }
                }
                case ColumnType.INT -> {
                    for (int row = 0; row < rowCount; row++) {
                        if (QwpNullBitmap.isNull(nullBitmapAddress, row)) {
                            dataMem.putInt(Numbers.INT_NULL);
                        } else {
                            dataMem.putInt(Unsafe.getUnsafe().getInt(valuesAddress + (long) valueIdx * 4));
                            valueIdx++;
                        }
                    }
                }
                case ColumnType.FLOAT -> {
                    for (int row = 0; row < rowCount; row++) {
                        if (QwpNullBitmap.isNull(nullBitmapAddress, row)) {
                            dataMem.putFloat(Float.NaN);
                        } else {
                            dataMem.putFloat(Unsafe.getUnsafe().getFloat(valuesAddress + (long) valueIdx * 4));
                            valueIdx++;
                        }
                    }
                }
                case ColumnType.LONG, ColumnType.DATE, ColumnType.TIMESTAMP -> {
                    for (int row = 0; row < rowCount; row++) {
                        if (QwpNullBitmap.isNull(nullBitmapAddress, row)) {
                            dataMem.putLong(Numbers.LONG_NULL);
                        } else {
                            dataMem.putLong(Unsafe.getUnsafe().getLong(valuesAddress + (long) valueIdx * 8));
                            valueIdx++;
                        }
                    }
                }
                case ColumnType.DOUBLE -> {
                    for (int row = 0; row < rowCount; row++) {
                        if (QwpNullBitmap.isNull(nullBitmapAddress, row)) {
                            dataMem.putDouble(Double.NaN);
                        } else {
                            dataMem.putDouble(Unsafe.getUnsafe().getDouble(valuesAddress + (long) valueIdx * 8));
                            valueIdx++;
                        }
                    }
                }
                case ColumnType.UUID, ColumnType.LONG128 -> {
                    for (int row = 0; row < rowCount; row++) {
                        if (QwpNullBitmap.isNull(nullBitmapAddress, row)) {
                            dataMem.putLong128(Numbers.LONG_NULL, Numbers.LONG_NULL);
                        } else {
                            long addr = valuesAddress + (long) valueIdx * 16;
                            dataMem.putLong128(
                                    Unsafe.getUnsafe().getLong(addr),
                                    Unsafe.getUnsafe().getLong(addr + 8)
                            );
                            valueIdx++;
                        }
                    }
                }
                case ColumnType.LONG256 -> {
                    for (int row = 0; row < rowCount; row++) {
                        if (QwpNullBitmap.isNull(nullBitmapAddress, row)) {
                            dataMem.putLong256(Numbers.LONG_NULL, Numbers.LONG_NULL, Numbers.LONG_NULL, Numbers.LONG_NULL);
                        } else {
                            long addr = valuesAddress + (long) valueIdx * 32;
                            dataMem.putLong256(
                                    Unsafe.getUnsafe().getLong(addr),
                                    Unsafe.getUnsafe().getLong(addr + 8),
                                    Unsafe.getUnsafe().getLong(addr + 16),
                                    Unsafe.getUnsafe().getLong(addr + 24)
                            );
                            valueIdx++;
                        }
                    }
                }
                default -> throw CairoException.nonCritical()
                        .put("unsupported column type for direct copy: ").put(ColumnType.nameOf(columnType));
            }
        }

        walWriter.setRowValueNotNullColumnar(columnIndex, startRowId + rowCount - 1);
    }

    @Override
    public void putFixedOtherToStringColumn(int columnIndex, QwpFixedWidthColumnCursor cursor, int rowCount, byte ilpType) {
        checkInColumnarWrite();
        MemoryMA dataMem = walWriter.getDataColumn(columnIndex);
        MemoryMA auxMem = walWriter.getAuxColumn(columnIndex);

        cursor.resetRowPosition();
        for (int row = 0; row < rowCount; row++) {
            cursor.advanceRow();
            if (cursor.isNull()) {
                StringTypeDriver.INSTANCE.appendNull(auxMem, dataMem);
            } else {
                strSink.clear();
                formatFixedOtherValue(strSink, cursor, ilpType);
                StringTypeDriver.appendValue(auxMem, dataMem, strSink);
            }
        }
        walWriter.setRowValueNotNullColumnar(columnIndex, startRowId + rowCount - 1);
    }

    @Override
    public void putFixedOtherToVarcharColumn(int columnIndex, QwpFixedWidthColumnCursor cursor, int rowCount, byte ilpType) {
        checkInColumnarWrite();
        MemoryMA dataMem = walWriter.getDataColumn(columnIndex);
        MemoryMA auxMem = walWriter.getAuxColumn(columnIndex);

        cursor.resetRowPosition();
        for (int row = 0; row < rowCount; row++) {
            cursor.advanceRow();
            if (cursor.isNull()) {
                VarcharTypeDriver.appendValue(auxMem, dataMem, null);
            } else {
                utf8Sink.clear();
                formatFixedOtherValue(utf8Sink, cursor, ilpType);
                VarcharTypeDriver.appendValue(auxMem, dataMem, utf8Sink);
            }
        }
        walWriter.setRowValueNotNullColumnar(columnIndex, startRowId + rowCount - 1);
    }

    @Override
    public void putFixedToDecimal128Column(
            int columnIndex, QwpFixedWidthColumnCursor cursor, int rowCount, int columnType
    ) {
        checkInColumnarWrite();
        MemoryMA dataMem = walWriter.getDataColumn(columnIndex);
        int columnScale = ColumnType.getDecimalScale(columnType);

        cursor.resetRowPosition();
        for (int row = 0; row < rowCount; row++) {
            cursor.advanceRow();
            if (cursor.isNull()) {
                writeDecimalNullSentinel(dataMem, columnType);
            } else {
                decimal.ofRaw(cursor.getLong());
                decimal.setScale(0);
                if (columnScale != 0) {
                    rescaleDecimalValue(decimal, 0, columnScale, columnType, columnIndex);
                }
                validateAndWriteDecimalValue(dataMem, decimal, columnType, columnIndex);
            }
        }
        walWriter.setRowValueNotNullColumnar(columnIndex, startRowId + rowCount - 1);
    }

    @Override
    public void putFixedToDecimal256Column(
            int columnIndex, QwpFixedWidthColumnCursor cursor, int rowCount, int columnType
    ) {
        checkInColumnarWrite();
        MemoryMA dataMem = walWriter.getDataColumn(columnIndex);
        int columnScale = ColumnType.getDecimalScale(columnType);

        cursor.resetRowPosition();
        for (int row = 0; row < rowCount; row++) {
            cursor.advanceRow();
            if (cursor.isNull()) {
                writeDecimalNullSentinel(dataMem, columnType);
            } else {
                decimal.ofRaw(cursor.getLong());
                decimal.setScale(0);
                if (columnScale != 0) {
                    rescaleDecimalValue(decimal, 0, columnScale, columnType, columnIndex);
                }
                validateAndWriteDecimalValue(dataMem, decimal, columnType, columnIndex);
            }
        }
        walWriter.setRowValueNotNullColumnar(columnIndex, startRowId + rowCount - 1);
    }

    @Override
    public void putFixedToDecimal64Column(
            int columnIndex, QwpFixedWidthColumnCursor cursor, int rowCount, int columnType
    ) {
        checkInColumnarWrite();
        MemoryMA dataMem = walWriter.getDataColumn(columnIndex);
        int columnScale = ColumnType.getDecimalScale(columnType);

        cursor.resetRowPosition();
        for (int row = 0; row < rowCount; row++) {
            cursor.advanceRow();
            if (cursor.isNull()) {
                writeDecimalNullSentinel(dataMem, columnType);
            } else {
                decimal.ofRaw(cursor.getLong());
                decimal.setScale(0);
                if (columnScale != 0) {
                    rescaleDecimalValue(decimal, 0, columnScale, columnType, columnIndex);
                }
                validateAndWriteDecimalValue(dataMem, decimal, columnType, columnIndex);
            }
        }
        walWriter.setRowValueNotNullColumnar(columnIndex, startRowId + rowCount - 1);
    }

    @Override
    public void putFixedToSmallDecimalColumn(
            int columnIndex, QwpFixedWidthColumnCursor cursor, int rowCount, int columnType
    ) {
        checkInColumnarWrite();
        MemoryMA dataMem = walWriter.getDataColumn(columnIndex);
        int columnScale = ColumnType.getDecimalScale(columnType);

        cursor.resetRowPosition();
        switch (ColumnType.tagOf(columnType)) {
            case ColumnType.DECIMAL8, ColumnType.DECIMAL16, ColumnType.DECIMAL32 -> {
                for (int row = 0; row < rowCount; row++) {
                    cursor.advanceRow();
                    if (cursor.isNull()) {
                        writeDecimalNullSentinel(dataMem, columnType);
                    } else {
                        decimal.ofRaw(cursor.getLong());
                        decimal.setScale(0);
                        if (columnScale != 0) {
                            rescaleDecimalValue(decimal, 0, columnScale, columnType, columnIndex);
                        }
                        validateAndWriteDecimalValue(dataMem, decimal, columnType, columnIndex);
                    }
                }
            }
            default -> throw CairoException.nonCritical()
                    .put("unsupported small decimal type: ").put(ColumnType.nameOf(columnType));
        }
        walWriter.setRowValueNotNullColumnar(columnIndex, startRowId + rowCount - 1);
    }

    @Override
    public void putFixedToStringColumn(int columnIndex, QwpFixedWidthColumnCursor cursor, int rowCount) {
        checkInColumnarWrite();
        MemoryMA dataMem = walWriter.getDataColumn(columnIndex);
        MemoryMA auxMem = walWriter.getAuxColumn(columnIndex);

        cursor.resetRowPosition();
        for (int row = 0; row < rowCount; row++) {
            cursor.advanceRow();
            if (cursor.isNull()) {
                StringTypeDriver.INSTANCE.appendNull(auxMem, dataMem);
            } else {
                strSink.clear();
                Numbers.append(strSink, cursor.getLong());
                StringTypeDriver.appendValue(auxMem, dataMem, strSink);
            }
        }
        walWriter.setRowValueNotNullColumnar(columnIndex, startRowId + rowCount - 1);
    }

    @Override
    public void putFixedToSymbolColumn(int columnIndex, QwpFixedWidthColumnCursor cursor, int rowCount) {
        checkInColumnarWrite();
        MemoryMA dataMem = walWriter.getDataColumn(columnIndex);
        SymbolMapReader symbolMapReader = walWriter.getSymbolMapReader(columnIndex);

        cursor.resetRowPosition();
        for (int row = 0; row < rowCount; row++) {
            cursor.advanceRow();
            if (cursor.isNull()) {
                dataMem.putInt(SymbolTable.VALUE_IS_NULL);
                walWriter.markSymbolMapNull(columnIndex);
            } else {
                strSink.clear();
                Numbers.append(strSink, cursor.getLong());
                int symbolKey = walWriter.resolveSymbol(columnIndex, strSink, symbolMapReader);
                dataMem.putInt(symbolKey);
            }
        }
        walWriter.setRowValueNotNullColumnar(columnIndex, startRowId + rowCount - 1);
    }

    @Override
    public void putFixedToVarcharColumn(int columnIndex, QwpFixedWidthColumnCursor cursor, int rowCount) {
        checkInColumnarWrite();
        MemoryMA dataMem = walWriter.getDataColumn(columnIndex);
        MemoryMA auxMem = walWriter.getAuxColumn(columnIndex);

        cursor.resetRowPosition();
        for (int row = 0; row < rowCount; row++) {
            cursor.advanceRow();
            if (cursor.isNull()) {
                VarcharTypeDriver.appendValue(auxMem, dataMem, null);
            } else {
                utf8Sink.clear();
                Numbers.append(utf8Sink, cursor.getLong());
                VarcharTypeDriver.appendValue(auxMem, dataMem, utf8Sink);
            }
        }
        walWriter.setRowValueNotNullColumnar(columnIndex, startRowId + rowCount - 1);
    }

    @Override
    public void putFloatToDecimalColumn(
            int columnIndex,
            QwpFixedWidthColumnCursor cursor,
            int rowCount,
            int columnType
    ) {
        checkInColumnarWrite();
        MemoryMA dataMem = walWriter.getDataColumn(columnIndex);
        int columnPrecision = ColumnType.getDecimalPrecision(columnType);
        int columnScale = ColumnType.getDecimalScale(columnType);

        cursor.resetRowPosition();
        try {
            switch (ColumnType.tagOf(columnType)) {
                case ColumnType.DECIMAL8 ->
                        putFloatToDecimal8Loop(dataMem, cursor, rowCount, columnType, columnPrecision, columnScale, columnIndex);
                case ColumnType.DECIMAL16 ->
                        putFloatToDecimal16Loop(dataMem, cursor, rowCount, columnType, columnPrecision, columnScale, columnIndex);
                case ColumnType.DECIMAL32 ->
                        putFloatToDecimal32Loop(dataMem, cursor, rowCount, columnType, columnPrecision, columnScale, columnIndex);
                case ColumnType.DECIMAL64 ->
                        putFloatToDecimal64Loop(dataMem, cursor, rowCount, columnType, columnPrecision, columnScale, columnIndex);
                case ColumnType.DECIMAL128 ->
                        putFloatToDecimal128Loop(dataMem, cursor, rowCount, columnType, columnPrecision, columnScale, columnIndex);
                default ->
                        putFloatToDecimal256Loop(dataMem, cursor, rowCount, columnType, columnPrecision, columnScale, columnIndex);
            }
        } catch (QwpParseException e) {
            throw CairoException.nonCritical().put("failed to convert float column to decimal");
        }
        walWriter.setRowValueNotNullColumnar(columnIndex, startRowId + rowCount - 1);
    }

    @Override
    public void putFloatToNumericColumn(
            int columnIndex, QwpFixedWidthColumnCursor cursor, int rowCount, int columnType
    ) {
        checkInColumnarWrite();
        MemoryMA dataMem = walWriter.getDataColumn(columnIndex);

        cursor.resetRowPosition();
        switch (ColumnType.tagOf(columnType)) {
            case ColumnType.BYTE -> {
                for (int row = 0; row < rowCount; row++) {
                    cursor.advanceRow();
                    if (cursor.isNull()) {
                        dataMem.putByte((byte) 0);
                    } else {
                        long longValue = checkDoubleToInteger(cursor.getDouble(), columnIndex, columnType);
                        checkIntegerRange(longValue, Byte.MIN_VALUE, Byte.MAX_VALUE, columnIndex, columnType);
                        dataMem.putByte((byte) longValue);
                    }
                }
            }
            case ColumnType.SHORT -> {
                for (int row = 0; row < rowCount; row++) {
                    cursor.advanceRow();
                    if (cursor.isNull()) {
                        dataMem.putShort((short) 0);
                    } else {
                        long longValue = checkDoubleToInteger(cursor.getDouble(), columnIndex, columnType);
                        checkIntegerRange(longValue, Short.MIN_VALUE, Short.MAX_VALUE, columnIndex, columnType);
                        dataMem.putShort((short) longValue);
                    }
                }
            }
            case ColumnType.INT -> {
                for (int row = 0; row < rowCount; row++) {
                    cursor.advanceRow();
                    if (cursor.isNull()) {
                        dataMem.putInt(Numbers.INT_NULL);
                    } else {
                        long longValue = checkDoubleToInteger(cursor.getDouble(), columnIndex, columnType);
                        checkIntegerRange(longValue, Integer.MIN_VALUE, Integer.MAX_VALUE, columnIndex, columnType);
                        dataMem.putInt((int) longValue);
                    }
                }
            }
            case ColumnType.LONG, ColumnType.DATE, ColumnType.TIMESTAMP -> {
                for (int row = 0; row < rowCount; row++) {
                    cursor.advanceRow();
                    if (cursor.isNull()) {
                        dataMem.putLong(Numbers.LONG_NULL);
                    } else {
                        long longValue = checkDoubleToInteger(cursor.getDouble(), columnIndex, columnType);
                        dataMem.putLong(longValue);
                    }
                }
            }
            case ColumnType.FLOAT -> {
                for (int row = 0; row < rowCount; row++) {
                    cursor.advanceRow();
                    if (cursor.isNull()) {
                        dataMem.putFloat(Float.NaN);
                    } else {
                        dataMem.putFloat((float) cursor.getDouble());
                    }
                }
            }
            case ColumnType.DOUBLE -> {
                for (int row = 0; row < rowCount; row++) {
                    cursor.advanceRow();
                    if (cursor.isNull()) {
                        dataMem.putDouble(Double.NaN);
                    } else {
                        dataMem.putDouble(cursor.getDouble());
                    }
                }
            }
            default -> throw CairoException.nonCritical()
                    .put("unsupported target type: ").put(ColumnType.nameOf(columnType));
        }
        walWriter.setRowValueNotNullColumnar(columnIndex, startRowId + rowCount - 1);
    }

    @Override
    public void putFloatToStringColumn(int columnIndex, QwpFixedWidthColumnCursor cursor, int rowCount) {
        checkInColumnarWrite();
        MemoryMA dataMem = walWriter.getDataColumn(columnIndex);
        MemoryMA auxMem = walWriter.getAuxColumn(columnIndex);

        cursor.resetRowPosition();
        for (int row = 0; row < rowCount; row++) {
            cursor.advanceRow();
            if (cursor.isNull()) {
                StringTypeDriver.INSTANCE.appendNull(auxMem, dataMem);
            } else {
                strSink.clear();
                Numbers.append(strSink, cursor.getDouble());
                StringTypeDriver.appendValue(auxMem, dataMem, strSink);
            }
        }
        walWriter.setRowValueNotNullColumnar(columnIndex, startRowId + rowCount - 1);
    }

    @Override
    public void putFloatToSymbolColumn(int columnIndex, QwpFixedWidthColumnCursor cursor, int rowCount) {
        checkInColumnarWrite();
        MemoryMA dataMem = walWriter.getDataColumn(columnIndex);
        SymbolMapReader symbolMapReader = walWriter.getSymbolMapReader(columnIndex);

        cursor.resetRowPosition();
        for (int row = 0; row < rowCount; row++) {
            cursor.advanceRow();
            if (cursor.isNull()) {
                dataMem.putInt(SymbolTable.VALUE_IS_NULL);
                walWriter.markSymbolMapNull(columnIndex);
            } else {
                strSink.clear();
                Numbers.append(strSink, cursor.getDouble());
                int symbolKey = walWriter.resolveSymbol(columnIndex, strSink, symbolMapReader);
                dataMem.putInt(symbolKey);
            }
        }
        walWriter.setRowValueNotNullColumnar(columnIndex, startRowId + rowCount - 1);
    }

    @Override
    public void putFloatToVarcharColumn(int columnIndex, QwpFixedWidthColumnCursor cursor, int rowCount) {
        checkInColumnarWrite();
        MemoryMA dataMem = walWriter.getDataColumn(columnIndex);
        MemoryMA auxMem = walWriter.getAuxColumn(columnIndex);

        cursor.resetRowPosition();
        for (int row = 0; row < rowCount; row++) {
            cursor.advanceRow();
            if (cursor.isNull()) {
                VarcharTypeDriver.appendValue(auxMem, dataMem, null);
            } else {
                utf8Sink.clear();
                Numbers.append(utf8Sink, cursor.getDouble());
                VarcharTypeDriver.appendValue(auxMem, dataMem, utf8Sink);
            }
        }
        walWriter.setRowValueNotNullColumnar(columnIndex, startRowId + rowCount - 1);
    }

    @Override
    public void putGeoHashColumn(
            int columnIndex, QwpGeoHashColumnCursor cursor, int rowCount, int columnType
    ) {
        checkInColumnarWrite();

        MemoryMA dataMem = walWriter.getDataColumn(columnIndex);

        cursor.resetRowPosition();
        switch (ColumnType.tagOf(columnType)) {
            case ColumnType.GEOBYTE -> {
                for (int row = 0; row < rowCount; row++) {
                    cursor.advanceRow();
                    dataMem.putByte(cursor.isNull() ? GeoHashes.BYTE_NULL : (byte) cursor.getGeoHash());
                }
            }
            case ColumnType.GEOSHORT -> {
                for (int row = 0; row < rowCount; row++) {
                    cursor.advanceRow();
                    dataMem.putShort(cursor.isNull() ? GeoHashes.SHORT_NULL : (short) cursor.getGeoHash());
                }
            }
            case ColumnType.GEOINT -> {
                for (int row = 0; row < rowCount; row++) {
                    cursor.advanceRow();
                    dataMem.putInt(cursor.isNull() ? GeoHashes.INT_NULL : (int) cursor.getGeoHash());
                }
            }
            case ColumnType.GEOLONG -> {
                for (int row = 0; row < rowCount; row++) {
                    cursor.advanceRow();
                    dataMem.putLong(cursor.isNull() ? GeoHashes.NULL : cursor.getGeoHash());
                }
            }
            default -> throw CairoException.nonCritical()
                    .put("invalid GeoHash column type: ").put(ColumnType.nameOf(columnType));
        }

        walWriter.setRowValueNotNullColumnar(columnIndex, startRowId + rowCount - 1);
    }

    @Override
    public void putGeoHashToStringColumn(int columnIndex, QwpGeoHashColumnCursor cursor, int rowCount) {
        checkInColumnarWrite();
        MemoryMA dataMem = walWriter.getDataColumn(columnIndex);
        MemoryMA auxMem = walWriter.getAuxColumn(columnIndex);

        cursor.resetRowPosition();
        for (int row = 0; row < rowCount; row++) {
            cursor.advanceRow();
            if (cursor.isNull()) {
                StringTypeDriver.INSTANCE.appendNull(auxMem, dataMem);
            } else {
                strSink.clear();
                int bits = cursor.getPrecision();
                if (bits < 0) {
                    GeoHashes.appendCharsUnsafe(cursor.getGeoHash(), -bits, strSink);
                } else {
                    GeoHashes.appendBinaryStringUnsafe(cursor.getGeoHash(), bits, strSink);
                }
                StringTypeDriver.appendValue(auxMem, dataMem, strSink);
            }
        }
        walWriter.setRowValueNotNullColumnar(columnIndex, startRowId + rowCount - 1);
    }

    @Override
    public void putGeoHashToVarcharColumn(int columnIndex, QwpGeoHashColumnCursor cursor, int rowCount) {
        checkInColumnarWrite();
        MemoryMA dataMem = walWriter.getDataColumn(columnIndex);
        MemoryMA auxMem = walWriter.getAuxColumn(columnIndex);

        cursor.resetRowPosition();
        for (int row = 0; row < rowCount; row++) {
            cursor.advanceRow();
            if (cursor.isNull()) {
                VarcharTypeDriver.appendValue(auxMem, dataMem, null);
            } else {
                utf8Sink.clear();
                int bits = cursor.getPrecision();
                if (bits < 0) {
                    GeoHashes.appendCharsUnsafe(cursor.getGeoHash(), -bits, utf8Sink);
                } else {
                    GeoHashes.appendBinaryStringUnsafe(cursor.getGeoHash(), bits, utf8Sink);
                }
                VarcharTypeDriver.appendValue(auxMem, dataMem, utf8Sink);
            }
        }
        walWriter.setRowValueNotNullColumnar(columnIndex, startRowId + rowCount - 1);
    }

    @Override
    public void putIntegerToNumericColumn(
            int columnIndex, QwpFixedWidthColumnCursor cursor, int rowCount, int columnType
    ) {
        checkInColumnarWrite();
        MemoryMA dataMem = walWriter.getDataColumn(columnIndex);

        cursor.resetRowPosition();
        switch (ColumnType.tagOf(columnType)) {
            case ColumnType.BYTE -> {
                for (int row = 0; row < rowCount; row++) {
                    cursor.advanceRow();
                    if (cursor.isNull()) {
                        dataMem.putByte((byte) 0);
                    } else {
                        long value = cursor.getLong();
                        checkIntegerRange(value, Byte.MIN_VALUE, Byte.MAX_VALUE, columnIndex, columnType);
                        dataMem.putByte((byte) value);
                    }
                }
            }
            case ColumnType.SHORT -> {
                for (int row = 0; row < rowCount; row++) {
                    cursor.advanceRow();
                    if (cursor.isNull()) {
                        dataMem.putShort((short) 0);
                    } else {
                        long value = cursor.getLong();
                        checkIntegerRange(value, Short.MIN_VALUE, Short.MAX_VALUE, columnIndex, columnType);
                        dataMem.putShort((short) value);
                    }
                }
            }
            case ColumnType.INT -> {
                for (int row = 0; row < rowCount; row++) {
                    cursor.advanceRow();
                    if (cursor.isNull()) {
                        dataMem.putInt(Numbers.INT_NULL);
                    } else {
                        long value = cursor.getLong();
                        checkIntegerRange(value, Integer.MIN_VALUE, Integer.MAX_VALUE, columnIndex, columnType);
                        dataMem.putInt((int) value);
                    }
                }
            }
            case ColumnType.LONG, ColumnType.DATE, ColumnType.TIMESTAMP -> {
                for (int row = 0; row < rowCount; row++) {
                    cursor.advanceRow();
                    if (cursor.isNull()) {
                        dataMem.putLong(Numbers.LONG_NULL);
                    } else {
                        dataMem.putLong(cursor.getLong());
                    }
                }
            }
            case ColumnType.FLOAT -> {
                for (int row = 0; row < rowCount; row++) {
                    cursor.advanceRow();
                    if (cursor.isNull()) {
                        dataMem.putFloat(Float.NaN);
                    } else {
                        dataMem.putFloat((float) cursor.getLong());
                    }
                }
            }
            case ColumnType.DOUBLE -> {
                for (int row = 0; row < rowCount; row++) {
                    cursor.advanceRow();
                    if (cursor.isNull()) {
                        dataMem.putDouble(Double.NaN);
                    } else {
                        dataMem.putDouble((double) cursor.getLong());
                    }
                }
            }
            default -> throw CairoException.nonCritical()
                    .put("unsupported target type: ").put(ColumnType.nameOf(columnType));
        }
        walWriter.setRowValueNotNullColumnar(columnIndex, startRowId + rowCount - 1);
    }

    @Override
    public void putStringColumn(int columnIndex, QwpStringColumnCursor cursor, int rowCount) throws QwpParseException {
        checkInColumnarWrite();

        MemoryMA dataMem = walWriter.getDataColumn(columnIndex);
        MemoryMA auxMem = walWriter.getAuxColumn(columnIndex);

        cursor.resetRowPosition();
        for (int row = 0; row < rowCount; row++) {
            cursor.advanceRow();
            if (cursor.isNull()) {
                StringTypeDriver.INSTANCE.appendNull(auxMem, dataMem);
            } else {
                DirectUtf8Sequence utf8Value = cursor.getUtf8Value();
                strSink.clear();
                CharSequence value = Utf8s.directUtf8ToUtf16(utf8Value, strSink);
                StringTypeDriver.appendValue(auxMem, dataMem, value);
            }
        }

        walWriter.setRowValueNotNullColumnar(columnIndex, startRowId + rowCount - 1);
    }

    @Override
    public void putStringToBooleanColumn(int columnIndex, QwpStringColumnCursor cursor, int rowCount) throws QwpParseException {
        checkInColumnarWrite();
        MemoryMA dataMem = walWriter.getDataColumn(columnIndex);

        cursor.resetRowPosition();
        for (int row = 0; row < rowCount; row++) {
            cursor.advanceRow();
            if (cursor.isNull()) {
                dataMem.putByte((byte) 0);
            } else {
                DirectUtf8Sequence value = cursor.getUtf8Value();
                int size = value.size();
                if (size == 1) {
                    byte b = value.byteAt(0);
                    if (b == '1') {
                        dataMem.putByte((byte) 1);
                    } else if (b == '0') {
                        dataMem.putByte((byte) 0);
                    } else {
                        throw CairoException.nonCritical()
                                .put("cannot parse boolean from string [value=")
                                .put(value)
                                .put(", column=").put(walWriter.getMetadata().getColumnName(columnIndex))
                                .put(']');
                    }
                } else if (size == 4 && Utf8s.equalsIgnoreCaseAscii("true", value)) {
                    dataMem.putByte((byte) 1);
                } else if (size == 5 && Utf8s.equalsIgnoreCaseAscii("false", value)) {
                    dataMem.putByte((byte) 0);
                } else {
                    throw CairoException.nonCritical()
                            .put("cannot parse boolean from string [value=")
                            .put(value)
                            .put(", column=").put(walWriter.getMetadata().getColumnName(columnIndex))
                            .put(']');
                }
            }
        }
        walWriter.setRowValueNotNullColumnar(columnIndex, startRowId + rowCount - 1);
    }

    @Override
    public void putStringToDecimalColumn(int columnIndex, QwpStringColumnCursor cursor, int rowCount, int columnType) throws QwpParseException {
        checkInColumnarWrite();
        MemoryMA dataMem = walWriter.getDataColumn(columnIndex);
        int columnPrecision = ColumnType.getDecimalPrecision(columnType);
        int columnScale = ColumnType.getDecimalScale(columnType);

        cursor.resetRowPosition();
        switch (ColumnType.tagOf(columnType)) {
            case ColumnType.DECIMAL8 ->
                    putStringToDecimal8Loop(dataMem, cursor, rowCount, columnPrecision, columnScale, columnIndex);
            case ColumnType.DECIMAL16 ->
                    putStringToDecimal16Loop(dataMem, cursor, rowCount, columnPrecision, columnScale, columnIndex);
            case ColumnType.DECIMAL32 ->
                    putStringToDecimal32Loop(dataMem, cursor, rowCount, columnPrecision, columnScale, columnIndex);
            case ColumnType.DECIMAL64 ->
                    putStringToDecimal64Loop(dataMem, cursor, rowCount, columnPrecision, columnScale, columnIndex);
            case ColumnType.DECIMAL128 ->
                    putStringToDecimal128Loop(dataMem, cursor, rowCount, columnPrecision, columnScale, columnIndex);
            default -> putStringToDecimal256Loop(dataMem, cursor, rowCount, columnPrecision, columnScale, columnIndex);
        }
        walWriter.setRowValueNotNullColumnar(columnIndex, startRowId + rowCount - 1);
    }

    @Override
    public void putStringToGeoHashColumn(int columnIndex, QwpStringColumnCursor cursor, int rowCount, int columnType) throws QwpParseException {
        checkInColumnarWrite();
        MemoryMA dataMem = walWriter.getDataColumn(columnIndex);
        int typeBits = ColumnType.getGeoHashBits(columnType);

        cursor.resetRowPosition();
        switch (ColumnType.tagOf(columnType)) {
            case ColumnType.GEOBYTE -> {
                for (int row = 0; row < rowCount; row++) {
                    cursor.advanceRow();
                    if (cursor.isNull()) {
                        dataMem.putByte(GeoHashes.BYTE_NULL);
                    } else {
                        DirectUtf8Sequence value = cursor.getUtf8Value();
                        try {
                            long geohash = GeoHashes.fromStringTruncatingNl(value.asAsciiCharSequence(), 0, value.size(), typeBits);
                            dataMem.putByte((byte) geohash);
                        } catch (NumericException e) {
                            throw geoHashParseError(value, columnIndex);
                        }
                    }
                }
            }
            case ColumnType.GEOSHORT -> {
                for (int row = 0; row < rowCount; row++) {
                    cursor.advanceRow();
                    if (cursor.isNull()) {
                        dataMem.putShort(GeoHashes.SHORT_NULL);
                    } else {
                        DirectUtf8Sequence value = cursor.getUtf8Value();
                        try {
                            long geohash = GeoHashes.fromStringTruncatingNl(value.asAsciiCharSequence(), 0, value.size(), typeBits);
                            dataMem.putShort((short) geohash);
                        } catch (NumericException e) {
                            throw geoHashParseError(value, columnIndex);
                        }
                    }
                }
            }
            case ColumnType.GEOINT -> {
                for (int row = 0; row < rowCount; row++) {
                    cursor.advanceRow();
                    if (cursor.isNull()) {
                        dataMem.putInt(GeoHashes.INT_NULL);
                    } else {
                        DirectUtf8Sequence value = cursor.getUtf8Value();
                        try {
                            long geohash = GeoHashes.fromStringTruncatingNl(value.asAsciiCharSequence(), 0, value.size(), typeBits);
                            dataMem.putInt((int) geohash);
                        } catch (NumericException e) {
                            throw geoHashParseError(value, columnIndex);
                        }
                    }
                }
            }
            case ColumnType.GEOLONG -> {
                for (int row = 0; row < rowCount; row++) {
                    cursor.advanceRow();
                    if (cursor.isNull()) {
                        dataMem.putLong(GeoHashes.NULL);
                    } else {
                        DirectUtf8Sequence value = cursor.getUtf8Value();
                        try {
                            long geohash = GeoHashes.fromStringTruncatingNl(value.asAsciiCharSequence(), 0, value.size(), typeBits);
                            dataMem.putLong(geohash);
                        } catch (NumericException e) {
                            throw geoHashParseError(value, columnIndex);
                        }
                    }
                }
            }
            default -> throw CairoException.nonCritical()
                    .put("invalid GeoHash column type: ").put(ColumnType.nameOf(columnType));
        }
        walWriter.setRowValueNotNullColumnar(columnIndex, startRowId + rowCount - 1);
    }

    @Override
    public void putStringToLong256Column(int columnIndex, QwpStringColumnCursor cursor, int rowCount) throws QwpParseException {
        checkInColumnarWrite();
        MemoryMA dataMem = walWriter.getDataColumn(columnIndex);

        cursor.resetRowPosition();
        for (int row = 0; row < rowCount; row++) {
            cursor.advanceRow();
            if (cursor.isNull()) {
                dataMem.putLong256(Numbers.LONG_NULL, Numbers.LONG_NULL, Numbers.LONG_NULL, Numbers.LONG_NULL);
            } else {
                DirectUtf8Sequence value = cursor.getUtf8Value();
                Long256Impl result = Numbers.parseLong256(value, long256);
                if (Long256Impl.isNull(result)) {
                    throw CairoException.nonCritical()
                            .put("cannot parse long256 from string [value=")
                            .put(value)
                            .put(", column=").put(walWriter.getMetadata().getColumnName(columnIndex))
                            .put(']');
                }
                dataMem.putLong256(result.getLong0(), result.getLong1(), result.getLong2(), result.getLong3());
            }
        }
        walWriter.setRowValueNotNullColumnar(columnIndex, startRowId + rowCount - 1);
    }

    @Override
    public void putStringToNumericColumn(int columnIndex, QwpStringColumnCursor cursor, int rowCount, int columnType) throws QwpParseException {
        checkInColumnarWrite();
        MemoryMA dataMem = walWriter.getDataColumn(columnIndex);

        cursor.resetRowPosition();
        switch (ColumnType.tagOf(columnType)) {
            case ColumnType.BYTE -> {
                for (int row = 0; row < rowCount; row++) {
                    cursor.advanceRow();
                    if (cursor.isNull()) {
                        dataMem.putByte((byte) 0);
                    } else {
                        DirectUtf8Sequence value = cursor.getUtf8Value();
                        try {
                            int v = Numbers.parseInt(value);
                            checkIntegerRange(v, Byte.MIN_VALUE, Byte.MAX_VALUE, columnIndex, columnType);
                            dataMem.putByte((byte) v);
                        } catch (NumericException e) {
                            throw numericParseError(value, columnIndex, columnType);
                        }
                    }
                }
            }
            case ColumnType.SHORT -> {
                for (int row = 0; row < rowCount; row++) {
                    cursor.advanceRow();
                    if (cursor.isNull()) {
                        dataMem.putShort((short) 0);
                    } else {
                        DirectUtf8Sequence value = cursor.getUtf8Value();
                        try {
                            dataMem.putShort(Numbers.parseShort(value));
                        } catch (NumericException e) {
                            throw numericParseError(value, columnIndex, columnType);
                        }
                    }
                }
            }
            case ColumnType.INT -> {
                for (int row = 0; row < rowCount; row++) {
                    cursor.advanceRow();
                    if (cursor.isNull()) {
                        dataMem.putInt(Numbers.INT_NULL);
                    } else {
                        DirectUtf8Sequence value = cursor.getUtf8Value();
                        try {
                            dataMem.putInt(Numbers.parseInt(value));
                        } catch (NumericException e) {
                            throw numericParseError(value, columnIndex, columnType);
                        }
                    }
                }
            }
            case ColumnType.LONG -> {
                for (int row = 0; row < rowCount; row++) {
                    cursor.advanceRow();
                    if (cursor.isNull()) {
                        dataMem.putLong(Numbers.LONG_NULL);
                    } else {
                        DirectUtf8Sequence value = cursor.getUtf8Value();
                        try {
                            dataMem.putLong(Numbers.parseLong(value));
                        } catch (NumericException e) {
                            throw numericParseError(value, columnIndex, columnType);
                        }
                    }
                }
            }
            case ColumnType.FLOAT -> {
                for (int row = 0; row < rowCount; row++) {
                    cursor.advanceRow();
                    if (cursor.isNull()) {
                        dataMem.putFloat(Float.NaN);
                    } else {
                        DirectUtf8Sequence value = cursor.getUtf8Value();
                        try {
                            dataMem.putFloat(Numbers.parseFloat(value.ptr(), value.size()));
                        } catch (NumericException e) {
                            throw numericParseError(value, columnIndex, columnType);
                        }
                    }
                }
            }
            case ColumnType.DOUBLE -> {
                for (int row = 0; row < rowCount; row++) {
                    cursor.advanceRow();
                    if (cursor.isNull()) {
                        dataMem.putDouble(Double.NaN);
                    } else {
                        DirectUtf8Sequence value = cursor.getUtf8Value();
                        try {
                            dataMem.putDouble(Numbers.parseDouble(value.ptr(), value.size()));
                        } catch (NumericException e) {
                            throw numericParseError(value, columnIndex, columnType);
                        }
                    }
                }
            }
            case ColumnType.DATE -> {
                for (int row = 0; row < rowCount; row++) {
                    cursor.advanceRow();
                    if (cursor.isNull()) {
                        dataMem.putLong(Numbers.LONG_NULL);
                    } else {
                        DirectUtf8Sequence value = cursor.getUtf8Value();
                        try {
                            dataMem.putLong(DateFormatUtils.parseDate(value.asAsciiCharSequence()));
                        } catch (NumericException e) {
                            throw numericParseError(value, columnIndex, columnType);
                        }
                    }
                }
            }
            default -> throw CairoException.nonCritical()
                    .put("unsupported target type: ").put(ColumnType.nameOf(columnType));
        }
        walWriter.setRowValueNotNullColumnar(columnIndex, startRowId + rowCount - 1);
    }

    @Override
    public void putStringToSymbolColumn(int columnIndex, QwpStringColumnCursor cursor, int rowCount) throws QwpParseException {
        checkInColumnarWrite();
        MemoryMA dataMem = walWriter.getDataColumn(columnIndex);
        SymbolMapReader symbolMapReader = walWriter.getSymbolMapReader(columnIndex);

        cursor.resetRowPosition();
        for (int row = 0; row < rowCount; row++) {
            cursor.advanceRow();
            if (cursor.isNull()) {
                dataMem.putInt(SymbolTable.VALUE_IS_NULL);
                walWriter.markSymbolMapNull(columnIndex);
            } else {
                DirectUtf8Sequence value = cursor.getUtf8Value();
                strSink.clear();
                CharSequence symbolValue = Utf8s.directUtf8ToUtf16(value, strSink);
                int symbolKey = walWriter.resolveSymbol(columnIndex, symbolValue, symbolMapReader);
                dataMem.putInt(symbolKey);
            }
        }
        walWriter.setRowValueNotNullColumnar(columnIndex, startRowId + rowCount - 1);
    }

    @Override
    public void putStringToTimestampColumn(int columnIndex, QwpStringColumnCursor cursor, int rowCount, int columnType) throws QwpParseException {
        checkInColumnarWrite();
        MemoryMA dataMem = walWriter.getDataColumn(columnIndex);
        boolean columnIsNanos = (columnType == ColumnType.TIMESTAMP_NANO);

        cursor.resetRowPosition();
        for (int row = 0; row < rowCount; row++) {
            cursor.advanceRow();
            if (cursor.isNull()) {
                dataMem.putLong(Numbers.LONG_NULL);
            } else {
                DirectUtf8Sequence value = cursor.getUtf8Value();
                try {
                    long micros = MicrosFormatUtils.parseTimestamp(value.asAsciiCharSequence());
                    if (columnIsNanos) {
                        if (micros > Long.MAX_VALUE / 1000 || micros < Long.MIN_VALUE / 1000) {
                            throw CairoException.nonCritical()
                                    .put("timestamp overflow converting micros to nanos: ").put(micros);
                        }
                        dataMem.putLong(micros * 1000);
                    } else {
                        dataMem.putLong(micros);
                    }
                } catch (NumericException e) {
                    throw CairoException.nonCritical()
                            .put("cannot parse timestamp from string [value=")
                            .put(value)
                            .put(", column=").put(walWriter.getMetadata().getColumnName(columnIndex))
                            .put(']');
                }
            }
        }
        walWriter.setRowValueNotNullColumnar(columnIndex, startRowId + rowCount - 1);
    }

    @Override
    public void putStringToUuidColumn(int columnIndex, QwpStringColumnCursor cursor, int rowCount) throws QwpParseException {
        checkInColumnarWrite();
        MemoryMA dataMem = walWriter.getDataColumn(columnIndex);

        cursor.resetRowPosition();
        for (int row = 0; row < rowCount; row++) {
            cursor.advanceRow();
            if (cursor.isNull()) {
                dataMem.putLong128(Numbers.LONG_NULL, Numbers.LONG_NULL);
            } else {
                DirectUtf8Sequence value = cursor.getUtf8Value();
                try {
                    Uuid.checkDashesAndLength(value);
                    long lo = Uuid.parseLo(value, 0);
                    long hi = Uuid.parseHi(value, 0);
                    dataMem.putLong128(lo, hi);
                } catch (NumericException e) {
                    throw CairoException.nonCritical()
                            .put("cannot parse UUID from string [value=")
                            .put(value)
                            .put(", column=").put(walWriter.getMetadata().getColumnName(columnIndex))
                            .put(']');
                }
            }
        }
        walWriter.setRowValueNotNullColumnar(columnIndex, startRowId + rowCount - 1);
    }

    @Override
    public void putSymbolColumn(int columnIndex, QwpSymbolColumnCursor cursor, int rowCount) {
        putSymbolColumn(columnIndex, cursor, rowCount, null, 0, 0);
    }

    @Override
    public void putSymbolColumn(
            int columnIndex,
            QwpSymbolColumnCursor cursor,
            int rowCount,
            ConnectionSymbolCache symbolCache,
            long tableId,
            int initialSymbolCount
    ) {
        checkInColumnarWrite();

        MemoryMA dataMem = walWriter.getDataColumn(columnIndex);
        SymbolMapReader symbolMapReader = walWriter.getSymbolMapReader(columnIndex);

        if (symbolMapReader == null) {
            throw CairoException.nonCritical().put("symbol map reader is not available");
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
        if (deltaMode && columnCache != null) {
            for (int row = 0; row < rowCount; row++) {
                cursor.advanceRow();
                if (cursor.isNull()) {
                    dataMem.putInt(SymbolTable.VALUE_IS_NULL);
                    walWriter.markSymbolMapNull(columnIndex);
                    continue;
                }

                int clientSymbolId = cursor.getSymbolIndex();
                int symbolKey = columnCache.get(clientSymbolId);
                if (symbolKey != ClientSymbolCache.NO_ENTRY) {
                    symbolCache.recordHit();
                    dataMem.putInt(symbolKey);
                    continue;
                }

                symbolCache.recordMiss();
                CharSequence symbolValue = cursor.getSymbolCharSequence();
                if (symbolValue == null) {
                    dataMem.putInt(SymbolTable.VALUE_IS_NULL);
                    walWriter.markSymbolMapNull(columnIndex);
                    continue;
                }

                symbolKey = walWriter.resolveSymbol(columnIndex, symbolValue, symbolMapReader);
                dataMem.putInt(symbolKey);
                if (symbolKey < initialSymbolCount) {
                    columnCache.put(clientSymbolId, symbolKey);
                }
            }
        } else {
            for (int row = 0; row < rowCount; row++) {
                cursor.advanceRow();
                if (cursor.isNull()) {
                    dataMem.putInt(SymbolTable.VALUE_IS_NULL);
                    walWriter.markSymbolMapNull(columnIndex);
                    continue;
                }

                DirectUtf8Sequence utf8Value = cursor.getSymbolUtf8();
                if (utf8Value == null) {
                    dataMem.putInt(SymbolTable.VALUE_IS_NULL);
                    walWriter.markSymbolMapNull(columnIndex);
                    continue;
                }

                strSink.clear();
                CharSequence symbolValue = Utf8s.directUtf8ToUtf16(utf8Value, strSink);
                int symbolKey = walWriter.resolveSymbol(columnIndex, symbolValue, symbolMapReader);
                dataMem.putInt(symbolKey);
            }
        }

        walWriter.setRowValueNotNullColumnar(columnIndex, startRowId + rowCount - 1);
    }

    @Override
    public void putSymbolToStringColumn(int columnIndex, QwpSymbolColumnCursor cursor, int rowCount) {
        checkInColumnarWrite();
        MemoryMA dataMem = walWriter.getDataColumn(columnIndex);
        MemoryMA auxMem = walWriter.getAuxColumn(columnIndex);

        cursor.resetRowPosition();
        for (int row = 0; row < rowCount; row++) {
            cursor.advanceRow();
            if (cursor.isNull()) {
                StringTypeDriver.INSTANCE.appendNull(auxMem, dataMem);
            } else {
                CharSequence symbolValue = cursor.getSymbolCharSequence();
                if (symbolValue == null) {
                    StringTypeDriver.INSTANCE.appendNull(auxMem, dataMem);
                } else {
                    StringTypeDriver.appendValue(auxMem, dataMem, symbolValue);
                }
            }
        }
        walWriter.setRowValueNotNullColumnar(columnIndex, startRowId + rowCount - 1);
    }

    @Override
    public void putSymbolToVarcharColumn(int columnIndex, QwpSymbolColumnCursor cursor, int rowCount) {
        checkInColumnarWrite();
        MemoryMA dataMem = walWriter.getDataColumn(columnIndex);
        MemoryMA auxMem = walWriter.getAuxColumn(columnIndex);
        cursor.resetRowPosition();
        for (int row = 0; row < rowCount; row++) {
            cursor.advanceRow();
            if (cursor.isNull()) {
                VarcharTypeDriver.appendValue(auxMem, dataMem, null);
            } else {
                DirectUtf8Sequence utf8Value = cursor.getSymbolUtf8();
                if (utf8Value != null) {
                    VarcharTypeDriver.appendValue(auxMem, dataMem, utf8Value);
                } else {
                    // delta mode: getSymbolUtf8() returns null, fall back to CharSequence
                    CharSequence symbolValue = cursor.getSymbolCharSequence();
                    if (symbolValue == null) {
                        VarcharTypeDriver.appendValue(auxMem, dataMem, null);
                    } else {
                        this.utf8Sink.clear();
                        this.utf8Sink.put(symbolValue);
                        VarcharTypeDriver.appendValue(auxMem, dataMem, this.utf8Sink);
                    }
                }
            }
        }
        walWriter.setRowValueNotNullColumnar(columnIndex, startRowId + rowCount - 1);
    }

    @Override
    public void putTimestampColumn(
            int columnIndex,
            long valuesAddress,
            int valueCount,
            long nullBitmapAddress,
            int rowCount,
            long serverTimestampMicros
    ) {
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
                    // Null designated timestamp means server-assigned (atNow)
                    timestamp = serverTimestampMicros;
                } else {
                    timestamp = Unsafe.getUnsafe().getLong(valuesAddress + (long) valueIdx * 8);
                    valueIdx++;
                }
                walWriter.validateDesignatedTimestampBounds(timestamp);
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
    public void putTimestampColumnWithConversion(
            int columnIndex,
            QwpTimestampColumnCursor cursor,
            int rowCount,
            byte ilpType,
            int columnType,
            boolean isDesignated,
            long serverTimestamp
    ) {
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
            if (!cursor.isNull()) {
                timestamp = cursor.getTimestamp();

                // Apply precision conversion
                if (wireIsNanos && !columnIsNanos) {
                    // Wire is nanos, column is micros: divide by 1000
                    timestamp = timestamp / 1000;
                } else if (!wireIsNanos && columnIsNanos) {
                    // Wire is micros, column is nanos: multiply by 1000
                    if (timestamp > Long.MAX_VALUE / 1000 || timestamp < Long.MIN_VALUE / 1000) {
                        throw CairoException.nonCritical()
                                .put("timestamp overflow converting micros to nanos: ").put(timestamp);
                    }
                    timestamp = timestamp * 1000;
                }
            } else {
                // Null designated timestamp means server-assigned (atNow)
                timestamp = isDesignated ? serverTimestamp : Numbers.LONG_NULL;
            }

            if (isDesignated) {
                // Designated timestamp: write 128-bit (timestamp, rowId) pairs
                walWriter.validateDesignatedTimestampBounds(timestamp);
                dataMem.putLong128(timestamp, startRowId + row);
            } else {
                // Non-designated timestamp: write as regular LONG
                dataMem.putLong(timestamp);
            }
        }

        walWriter.setRowValueNotNullColumnar(columnIndex, this.startRowId + rowCount - 1);
    }

    @Override
    public void putTimestampToStringColumn(int columnIndex, QwpTimestampColumnCursor cursor, int rowCount, byte ilpType) {
        checkInColumnarWrite();
        MemoryMA dataMem = walWriter.getDataColumn(columnIndex);
        MemoryMA auxMem = walWriter.getAuxColumn(columnIndex);
        boolean wireIsNanos = (ilpType == TYPE_TIMESTAMP_NANOS);

        cursor.resetRowPosition();
        for (int row = 0; row < rowCount; row++) {
            cursor.advanceRow();
            if (cursor.isNull()) {
                StringTypeDriver.INSTANCE.appendNull(auxMem, dataMem);
            } else {
                long ts = cursor.getTimestamp();
                long micros = wireIsNanos ? ts / 1000 : ts;
                strSink.clear();
                MicrosFormatUtils.appendDateTime(strSink, micros);
                StringTypeDriver.appendValue(auxMem, dataMem, strSink);
            }
        }
        walWriter.setRowValueNotNullColumnar(columnIndex, startRowId + rowCount - 1);
    }

    @Override
    public void putTimestampToVarcharColumn(int columnIndex, QwpTimestampColumnCursor cursor, int rowCount, byte ilpType) {
        checkInColumnarWrite();
        MemoryMA dataMem = walWriter.getDataColumn(columnIndex);
        MemoryMA auxMem = walWriter.getAuxColumn(columnIndex);
        boolean wireIsNanos = (ilpType == TYPE_TIMESTAMP_NANOS);

        cursor.resetRowPosition();
        for (int row = 0; row < rowCount; row++) {
            cursor.advanceRow();
            if (cursor.isNull()) {
                VarcharTypeDriver.appendValue(auxMem, dataMem, null);
            } else {
                long ts = cursor.getTimestamp();
                long micros = wireIsNanos ? ts / 1000 : ts;
                utf8Sink.clear();
                MicrosFormatUtils.appendDateTime(utf8Sink, micros);
                VarcharTypeDriver.appendValue(auxMem, dataMem, utf8Sink);
            }
        }
        walWriter.setRowValueNotNullColumnar(columnIndex, startRowId + rowCount - 1);
    }

    @Override
    public void putVarcharColumn(int columnIndex, QwpStringColumnCursor cursor, int rowCount) throws QwpParseException {
        checkInColumnarWrite();

        MemoryMA dataMem = walWriter.getDataColumn(columnIndex);
        MemoryMA auxMem = walWriter.getAuxColumn(columnIndex);

        cursor.resetRowPosition();
        for (int row = 0; row < rowCount; row++) {
            cursor.advanceRow();
            if (cursor.isNull()) {
                VarcharTypeDriver.appendValue(auxMem, dataMem, null);
            } else {
                DirectUtf8Sequence value = cursor.getUtf8Value();
                VarcharTypeDriver.appendValue(auxMem, dataMem, value);
            }
        }

        walWriter.setRowValueNotNullColumnar(columnIndex, startRowId + rowCount - 1);
    }

    private static void formatFixedOtherValue(CharSink<?> sink, QwpFixedWidthColumnCursor cursor, byte ilpType) {
        switch (ilpType) {
            case TYPE_UUID -> Numbers.appendUuid(cursor.getUuidLo(), cursor.getUuidHi(), sink);
            case TYPE_LONG256 -> Numbers.appendLong256(
                    cursor.getLong256_0(), cursor.getLong256_1(),
                    cursor.getLong256_2(), cursor.getLong256_3(), sink);
            case TYPE_DATE -> DateFormatUtils.appendDateTime(sink, cursor.getDate());
            case TYPE_TIMESTAMP -> MicrosFormatUtils.appendDateTime(sink, cursor.getTimestamp());
            case TYPE_TIMESTAMP_NANOS -> MicrosFormatUtils.appendDateTime(sink, cursor.getTimestamp() / 1000);
            case TYPE_CHAR -> sink.putAscii((char) cursor.getShort());
            default -> throw CairoException.nonCritical()
                    .put("unsupported wire type for string conversion: ").put(ilpType);
        }
    }


    /**
     * Writes the appropriate null sentinel value for the given decimal column type.
     */
    private static void writeDecimalNullSentinel(MemoryMA dataMem, int columnType) {
        switch (ColumnType.tagOf(columnType)) {
            case ColumnType.DECIMAL8 -> dataMem.putByte(Decimals.DECIMAL8_NULL);
            case ColumnType.DECIMAL16 -> dataMem.putShort(Decimals.DECIMAL16_NULL);
            case ColumnType.DECIMAL32 -> dataMem.putInt(Decimals.DECIMAL32_NULL);
            case ColumnType.DECIMAL64 -> dataMem.putLong(Decimals.DECIMAL64_NULL);
            case ColumnType.DECIMAL128 ->
                    dataMem.putDecimal128(Decimals.DECIMAL128_HI_NULL, Decimals.DECIMAL128_LO_NULL);
            case ColumnType.DECIMAL256 ->
                    dataMem.putDecimal256(Decimals.DECIMAL256_HH_NULL, Decimals.DECIMAL256_HL_NULL,
                            Decimals.DECIMAL256_LH_NULL, Decimals.DECIMAL256_LL_NULL);
            default -> throw CairoException.nonCritical().put("unsupported decimal column type for null sentinel: ")
                    .put(ColumnType.nameOf(columnType));
        }
    }

    private long checkDoubleToInteger(double value, int columnIndex, int columnType) {
        long longValue = (long) value;
        if ((double) longValue != value) {
            throw CairoException.nonCritical()
                    .put("double value ").put(value)
                    .put(" loses precision when converted to ")
                    .put(ColumnType.nameOf(columnType))
                    .put(" [column=").put(walWriter.getMetadata().getColumnName(columnIndex))
                    .put(']');
        }
        return longValue;
    }

    private void checkInColumnarWrite() {
        if (!inColumnarWrite) {
            throw CairoException.nonCritical().put("not in columnar write mode");
        }
    }

    private void checkIntegerRange(long value, long min, long max, int columnIndex, int columnType) {
        if (value < min || value > max) {
            throw CairoException.nonCritical()
                    .put("integer value ").put(value)
                    .put(" out of range for ").put(ColumnType.nameOf(columnType))
                    .put(" [column=").put(walWriter.getMetadata().getColumnName(columnIndex))
                    .put(", min=").put(min)
                    .put(", max=").put(max)
                    .put(']');
        }
    }

    private CairoException doubleToDecimalConversionError(double value, int columnType, int columnIndex, int columnScale) {
        return CairoException.nonCritical()
                .put("double value ").put(value)
                .put(" cannot be converted to ")
                .put(ColumnType.nameOf(columnType))
                .put(" [column=").put(walWriter.getMetadata().getColumnName(columnIndex))
                .put(", scale=").put(columnScale)
                .put(']');
    }

    private CairoException geoHashParseError(DirectUtf8Sequence value, int columnIndex) {
        return CairoException.nonCritical()
                .put("cannot parse geohash from string [value=")
                .put(value)
                .put(", column=").put(walWriter.getMetadata().getColumnName(columnIndex))
                .put(']');
    }

    private CairoException numericParseError(DirectUtf8Sequence value, int columnIndex, int columnType) {
        return CairoException.nonCritical()
                .put("cannot parse ").put(ColumnType.nameOf(columnType))
                .put(" from string [value=").put(value)
                .put(", column=").put(walWriter.getMetadata().getColumnName(columnIndex))
                .put(']');
    }

    private void putDecimal128ToDecimalColumn(
            MemoryMA dataMem, QwpDecimalColumnCursor cursor, int rowCount,
            int wireScale, int columnScale, int columnType, int columnIndex
    ) {
        boolean needsRescale = wireScale != columnScale;
        for (int row = 0; row < rowCount; row++) {
            cursor.advanceRow();
            if (cursor.isNull()) {
                writeDecimalNullSentinel(dataMem, columnType);
            } else {
                decimal.ofRaw(cursor.getDecimal128Hi(), cursor.getDecimal128Lo());
                decimal.setScale(wireScale);
                if (needsRescale) {
                    rescaleDecimalValue(decimal, wireScale, columnScale, columnType, columnIndex);
                }
                validateAndWriteDecimalValue(dataMem, decimal, columnType, columnIndex);
            }
        }
    }

    private void putDecimal256ToDecimalColumn(
            MemoryMA dataMem, QwpDecimalColumnCursor cursor, int rowCount,
            int wireScale, int columnScale, int columnType, int columnIndex
    ) {
        boolean needsRescale = wireScale != columnScale;
        for (int row = 0; row < rowCount; row++) {
            cursor.advanceRow();
            if (cursor.isNull()) {
                writeDecimalNullSentinel(dataMem, columnType);
            } else {
                decimal.ofRaw(
                        cursor.getDecimal256Hh(), cursor.getDecimal256Hl(),
                        cursor.getDecimal256Lh(), cursor.getDecimal256Ll()
                );
                decimal.setScale(wireScale);
                if (needsRescale) {
                    rescaleDecimalValue(decimal, wireScale, columnScale, columnType, columnIndex);
                }
                validateAndWriteDecimalValue(dataMem, decimal, columnType, columnIndex);
            }
        }
    }

    private void putDecimal64ToDecimalColumn(
            MemoryMA dataMem,
            QwpDecimalColumnCursor cursor,
            int rowCount,
            int wireScale,
            int columnScale,
            int columnType,
            int columnIndex
    ) {
        boolean needsRescale = wireScale != columnScale;
        for (int row = 0; row < rowCount; row++) {
            cursor.advanceRow();
            if (cursor.isNull()) {
                writeDecimalNullSentinel(dataMem, columnType);
            } else {
                decimal.ofRaw(cursor.getDecimal64());
                decimal.setScale(wireScale);
                if (needsRescale) {
                    rescaleDecimalValue(decimal, wireScale, columnScale, columnType, columnIndex);
                }
                validateAndWriteDecimalValue(dataMem, decimal, columnType, columnIndex);
            }
        }
    }

    private void putDecimalToDecimalColumn(
            int columnIndex, QwpDecimalColumnCursor cursor, int rowCount, int columnType
    ) {
        checkInColumnarWrite();

        MemoryMA dataMem = walWriter.getDataColumn(columnIndex);
        int wireScale = cursor.getScale() & 0xFF;
        int columnScale = ColumnType.getDecimalScale(columnType);

        cursor.resetRowPosition();
        switch (cursor.getTypeCode()) {
            case TYPE_DECIMAL64 ->
                    putDecimal64ToDecimalColumn(dataMem, cursor, rowCount, wireScale, columnScale, columnType, columnIndex);
            case TYPE_DECIMAL128 ->
                    putDecimal128ToDecimalColumn(dataMem, cursor, rowCount, wireScale, columnScale, columnType, columnIndex);
            case TYPE_DECIMAL256 ->
                    putDecimal256ToDecimalColumn(dataMem, cursor, rowCount, wireScale, columnScale, columnType, columnIndex);
            default -> throw CairoException.nonCritical()
                    .put("unknown decimal wire type: ").put(cursor.getTypeCode());
        }

        walWriter.setRowValueNotNullColumnar(columnIndex, startRowId + rowCount - 1);
    }

    private void putFloatToDecimal128Loop(
            MemoryMA dataMem,
            QwpFixedWidthColumnCursor cursor,
            int rowCount,
            int columnType,
            int columnPrecision,
            int columnScale,
            int columnIndex
    ) throws QwpParseException {
        for (int row = 0; row < rowCount; row++) {
            cursor.advanceRow();
            if (cursor.isNull()) {
                dataMem.putDecimal128(Decimals.DECIMAL128_HI_NULL, Decimals.DECIMAL128_LO_NULL);
            } else {
                double value = cursor.getDouble();
                if (!Numbers.isFinite(value)) {
                    dataMem.putDecimal128(Decimals.DECIMAL128_HI_NULL, Decimals.DECIMAL128_LO_NULL);
                } else {
                    try {
                        Numbers.doubleToDecimal(value, decimal128, columnPrecision, columnScale, false);
                    } catch (NumericException e) {
                        throw doubleToDecimalConversionError(value, columnType, columnIndex, columnScale);
                    }
                    dataMem.putDecimal128(decimal128.getHigh(), decimal128.getLow());
                }
            }
        }
    }

    private void putFloatToDecimal16Loop(
            MemoryMA dataMem,
            QwpFixedWidthColumnCursor cursor,
            int rowCount,
            int columnType,
            int columnPrecision,
            int columnScale,
            int columnIndex
    ) throws QwpParseException {
        for (int row = 0; row < rowCount; row++) {
            cursor.advanceRow();
            if (cursor.isNull()) {
                dataMem.putShort(Decimals.DECIMAL16_NULL);
            } else {
                double value = cursor.getDouble();
                if (!Numbers.isFinite(value)) {
                    dataMem.putShort(Decimals.DECIMAL16_NULL);
                } else {
                    try {
                        Numbers.doubleToDecimal(value, decimal64, columnPrecision, columnScale, false);
                    } catch (NumericException e) {
                        throw doubleToDecimalConversionError(value, columnType, columnIndex, columnScale);
                    }
                    dataMem.putShort((short) decimal64.getValue());
                }
            }
        }
    }

    private void putFloatToDecimal256Loop(
            MemoryMA dataMem,
            QwpFixedWidthColumnCursor cursor,
            int rowCount,
            int columnType,
            int columnPrecision,
            int columnScale,
            int columnIndex
    ) throws QwpParseException {
        for (int row = 0; row < rowCount; row++) {
            cursor.advanceRow();
            if (cursor.isNull()) {
                dataMem.putDecimal256(Decimals.DECIMAL256_HH_NULL, Decimals.DECIMAL256_HL_NULL,
                        Decimals.DECIMAL256_LH_NULL, Decimals.DECIMAL256_LL_NULL);
            } else {
                double value = cursor.getDouble();
                if (!Numbers.isFinite(value)) {
                    dataMem.putDecimal256(Decimals.DECIMAL256_HH_NULL, Decimals.DECIMAL256_HL_NULL,
                            Decimals.DECIMAL256_LH_NULL, Decimals.DECIMAL256_LL_NULL);
                } else {
                    try {
                        Numbers.doubleToDecimal(value, decimal, columnPrecision, columnScale, false);
                    } catch (NumericException e) {
                        throw doubleToDecimalConversionError(value, columnType, columnIndex, columnScale);
                    }
                    dataMem.putDecimal256(decimal.getHh(), decimal.getHl(), decimal.getLh(), decimal.getLl());
                }
            }
        }
    }

    private void putFloatToDecimal32Loop(
            MemoryMA dataMem,
            QwpFixedWidthColumnCursor cursor,
            int rowCount,
            int columnType,
            int columnPrecision,
            int columnScale,
            int columnIndex
    ) throws QwpParseException {
        for (int row = 0; row < rowCount; row++) {
            cursor.advanceRow();
            if (cursor.isNull()) {
                dataMem.putInt(Decimals.DECIMAL32_NULL);
            } else {
                double value = cursor.getDouble();
                if (!Numbers.isFinite(value)) {
                    dataMem.putInt(Decimals.DECIMAL32_NULL);
                } else {
                    try {
                        Numbers.doubleToDecimal(value, decimal64, columnPrecision, columnScale, false);
                    } catch (NumericException e) {
                        throw doubleToDecimalConversionError(value, columnType, columnIndex, columnScale);
                    }
                    dataMem.putInt((int) decimal64.getValue());
                }
            }
        }
    }

    private void putFloatToDecimal64Loop(
            MemoryMA dataMem, QwpFixedWidthColumnCursor cursor, int rowCount,
            int columnType, int columnPrecision, int columnScale, int columnIndex
    ) throws QwpParseException {
        for (int row = 0; row < rowCount; row++) {
            cursor.advanceRow();
            if (cursor.isNull()) {
                dataMem.putLong(Decimals.DECIMAL64_NULL);
            } else {
                double value = cursor.getDouble();
                if (!Numbers.isFinite(value)) {
                    dataMem.putLong(Decimals.DECIMAL64_NULL);
                } else {
                    try {
                        Numbers.doubleToDecimal(value, decimal64, columnPrecision, columnScale, false);
                    } catch (NumericException e) {
                        throw doubleToDecimalConversionError(value, columnType, columnIndex, columnScale);
                    }
                    dataMem.putLong(decimal64.getValue());
                }
            }
        }
    }

    private void putFloatToDecimal8Loop(
            MemoryMA dataMem,
            QwpFixedWidthColumnCursor cursor,
            int rowCount,
            int columnType,
            int columnPrecision,
            int columnScale,
            int columnIndex
    ) throws QwpParseException {
        for (int row = 0; row < rowCount; row++) {
            cursor.advanceRow();
            if (cursor.isNull()) {
                dataMem.putByte(Decimals.DECIMAL8_NULL);
            } else {
                double value = cursor.getDouble();
                if (!Numbers.isFinite(value)) {
                    dataMem.putByte(Decimals.DECIMAL8_NULL);
                } else {
                    try {
                        Numbers.doubleToDecimal(value, decimal64, columnPrecision, columnScale, false);
                    } catch (NumericException e) {
                        throw doubleToDecimalConversionError(value, columnType, columnIndex, columnScale);
                    }
                    dataMem.putByte((byte) decimal64.getValue());
                }
            }
        }
    }

    private void putStringToDecimal128Loop(
            MemoryMA dataMem,
            QwpStringColumnCursor cursor,
            int rowCount,
            int columnPrecision,
            int columnScale,
            int columnIndex
    ) throws QwpParseException {
        for (int row = 0; row < rowCount; row++) {
            cursor.advanceRow();
            if (cursor.isNull()) {
                dataMem.putDecimal128(Decimals.DECIMAL128_HI_NULL, Decimals.DECIMAL128_LO_NULL);
            } else {
                DirectUtf8Sequence value = cursor.getUtf8Value();
                try {
                    decimal128.ofString(value.asAsciiCharSequence(), 0, value.size(),
                            columnPrecision, columnScale, false, false);
                } catch (NumericException e) {
                    throw stringToDecimalConversionError(value, columnIndex);
                }
                dataMem.putDecimal128(decimal128.getHigh(), decimal128.getLow());
            }
        }
    }

    private void putStringToDecimal16Loop(
            MemoryMA dataMem,
            QwpStringColumnCursor cursor,
            int rowCount,
            int columnPrecision,
            int columnScale,
            int columnIndex
    ) throws QwpParseException {
        for (int row = 0; row < rowCount; row++) {
            cursor.advanceRow();
            if (cursor.isNull()) {
                dataMem.putShort(Decimals.DECIMAL16_NULL);
            } else {
                DirectUtf8Sequence value = cursor.getUtf8Value();
                try {
                    decimal64.ofString(value.asAsciiCharSequence(), 0, value.size(),
                            columnPrecision, columnScale, false, false);
                } catch (NumericException e) {
                    throw stringToDecimalConversionError(value, columnIndex);
                }
                dataMem.putShort((short) decimal64.getValue());
            }
        }
    }

    private void putStringToDecimal256Loop(
            MemoryMA dataMem,
            QwpStringColumnCursor cursor,
            int rowCount,
            int columnPrecision,
            int columnScale,
            int columnIndex
    ) throws QwpParseException {
        for (int row = 0; row < rowCount; row++) {
            cursor.advanceRow();
            if (cursor.isNull()) {
                dataMem.putDecimal256(Decimals.DECIMAL256_HH_NULL, Decimals.DECIMAL256_HL_NULL,
                        Decimals.DECIMAL256_LH_NULL, Decimals.DECIMAL256_LL_NULL);
            } else {
                DirectUtf8Sequence value = cursor.getUtf8Value();
                try {
                    decimal.ofString(value.asAsciiCharSequence(), 0, value.size(),
                            columnPrecision, columnScale, false, false);
                } catch (NumericException e) {
                    throw stringToDecimalConversionError(value, columnIndex);
                }
                dataMem.putDecimal256(decimal.getHh(), decimal.getHl(), decimal.getLh(), decimal.getLl());
            }
        }
    }

    private void putStringToDecimal32Loop(
            MemoryMA dataMem,
            QwpStringColumnCursor cursor,
            int rowCount,
            int columnPrecision,
            int columnScale,
            int columnIndex
    ) throws QwpParseException {
        for (int row = 0; row < rowCount; row++) {
            cursor.advanceRow();
            if (cursor.isNull()) {
                dataMem.putInt(Decimals.DECIMAL32_NULL);
            } else {
                DirectUtf8Sequence value = cursor.getUtf8Value();
                try {
                    decimal64.ofString(value.asAsciiCharSequence(), 0, value.size(),
                            columnPrecision, columnScale, false, false);
                } catch (NumericException e) {
                    throw stringToDecimalConversionError(value, columnIndex);
                }
                dataMem.putInt((int) decimal64.getValue());
            }
        }
    }

    private void putStringToDecimal64Loop(
            MemoryMA dataMem,
            QwpStringColumnCursor cursor,
            int rowCount,
            int columnPrecision,
            int columnScale,
            int columnIndex
    ) throws QwpParseException {
        for (int row = 0; row < rowCount; row++) {
            cursor.advanceRow();
            if (cursor.isNull()) {
                dataMem.putLong(Decimals.DECIMAL64_NULL);
            } else {
                DirectUtf8Sequence value = cursor.getUtf8Value();
                try {
                    decimal64.ofString(value.asAsciiCharSequence(), 0, value.size(),
                            columnPrecision, columnScale, false, false);
                } catch (NumericException e) {
                    throw stringToDecimalConversionError(value, columnIndex);
                }
                dataMem.putLong(decimal64.getValue());
            }
        }
    }

    private void putStringToDecimal8Loop(
            MemoryMA dataMem,
            QwpStringColumnCursor cursor,
            int rowCount,
            int columnPrecision,
            int columnScale,
            int columnIndex
    ) throws QwpParseException {
        for (int row = 0; row < rowCount; row++) {
            cursor.advanceRow();
            if (cursor.isNull()) {
                dataMem.putByte(Decimals.DECIMAL8_NULL);
            } else {
                DirectUtf8Sequence value = cursor.getUtf8Value();
                try {
                    decimal64.ofString(value.asAsciiCharSequence(), 0, value.size(),
                            columnPrecision, columnScale, false, false);
                } catch (NumericException e) {
                    throw stringToDecimalConversionError(value, columnIndex);
                }
                dataMem.putByte((byte) decimal64.getValue());
            }
        }
    }

    private void rescaleDecimalValue(
            Decimal256 decimal,
            int sourceScale,
            int targetScale,
            int columnType,
            int columnIndex
    ) {
        try {
            decimal.rescale(targetScale);
        } catch (NumericException ignored) {
            if (sourceScale > targetScale) {
                throwDecimalPrecisionLoss(decimal, columnType, columnIndex);
            }
            throwDecimalOverflow(decimal, columnType, columnIndex);
        }
    }

    private CairoException stringToDecimalConversionError(DirectUtf8Sequence value, int columnIndex) {
        return CairoException.nonCritical()
                .put("cannot parse decimal from string [value=")
                .put(value)
                .put(", column=").put(walWriter.getMetadata().getColumnName(columnIndex))
                .put(']');
    }

    private void throwDecimalOverflow(Decimal256 decimal, int columnType, int columnIndex) {
        throw CairoException.nonCritical()
                .put("decimal value overflows ")
                .put(ColumnType.nameOf(columnType))
                .put(" [column=").put(walWriter.getMetadata().getColumnName(columnIndex))
                .put(", decimal=").put(decimal)
                .put(']');
    }

    private void throwDecimalPrecisionLoss(Decimal256 decimal, int columnType, int columnIndex) {
        throw CairoException.nonCritical()
                .put("decimal value causes precision loss converting to ")
                .put(ColumnType.nameOf(columnType))
                .put(" [column=").put(walWriter.getMetadata().getColumnName(columnIndex))
                .put(", decimal=").put(decimal)
                .put(']');
    }

    private void validateAndWriteDecimalValue(MemoryMA dataMem, Decimal256 decimal, int columnType, int columnIndex) {
        if (!decimal.comparePrecision(ColumnType.getDecimalPrecision(columnType))) {
            throwDecimalOverflow(decimal, columnType, columnIndex);
        }
        writeDecimalValue(dataMem, decimal, columnType, columnIndex);
    }

    private void writeDecimalValue(MemoryMA dataMem, Decimal256 decimal, int columnType, int columnIndex) {
        long ll = decimal.getLl();
        long lh = decimal.getLh();
        long hl = decimal.getHl();
        long hh = decimal.getHh();
        switch (ColumnType.tagOf(columnType)) {
            case ColumnType.DECIMAL8 -> {
                long sign = (ll < 0) ? -1L : 0L;
                if ((ll != (byte) ll) || lh != sign || hl != sign || hh != sign) {
                    throwDecimalOverflow(decimal, columnType, columnIndex);
                }
                dataMem.putByte((byte) ll);
            }
            case ColumnType.DECIMAL16 -> {
                long sign = (ll < 0) ? -1L : 0L;
                if ((ll != (short) ll) || lh != sign || hl != sign || hh != sign) {
                    throwDecimalOverflow(decimal, columnType, columnIndex);
                }
                dataMem.putShort((short) ll);
            }
            case ColumnType.DECIMAL32 -> {
                long sign = (ll < 0) ? -1L : 0L;
                if ((ll != (int) ll) || lh != sign || hl != sign || hh != sign) {
                    throwDecimalOverflow(decimal, columnType, columnIndex);
                }
                dataMem.putInt((int) ll);
            }
            case ColumnType.DECIMAL64 -> {
                long sign = (ll < 0) ? -1L : 0L;
                if (lh != sign || hl != sign || hh != sign) {
                    throwDecimalOverflow(decimal, columnType, columnIndex);
                }
                dataMem.putLong(ll);
            }
            case ColumnType.DECIMAL128 -> {
                long sign = (lh < 0) ? -1L : 0L;
                if (hl != sign || hh != sign) {
                    throwDecimalOverflow(decimal, columnType, columnIndex);
                }
                dataMem.putDecimal128(lh, ll);
            }
            case ColumnType.DECIMAL256 -> dataMem.putDecimal256(hh, hl, lh, ll);
            default -> throw CairoException.nonCritical()
                    .put("unsupported decimal type: ").put(ColumnType.nameOf(columnType));
        }
    }
}
