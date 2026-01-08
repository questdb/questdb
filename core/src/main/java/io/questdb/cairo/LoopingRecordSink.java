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

package io.questdb.cairo;

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.std.BitSet;
import io.questdb.std.BoolList;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.Nullable;

/**
 * Loop-based implementation of RecordSink for tables with many columns.
 * Unlike the bytecode-generated sinks, this uses simple loops and switch statements,
 * trading some performance for simplicity and avoiding bytecode size limitations.
 * <p>
 * This implementation is used when the estimated bytecode size exceeds the configured
 * threshold to avoid hitting the JVM's 64KB method size limit or performance cliffs
 * from methods that are too large to be fully optimized.
 */
public class LoopingRecordSink implements RecordSink {
    private final IntList columnIndices;
    private final IntList columnTypes;
    private final Decimal128 decimal128;
    private final Decimal256 decimal256;
    private final IntList skewedIndices;
    private final BoolList strAsVarchar;
    private final BoolList symAsString;
    private final BoolList timestampAsNanos;
    private ObjList<Function> keyFunctions;

    public LoopingRecordSink(
            ColumnTypes columnTypes,
            ColumnFilter columnFilter,
            @Nullable IntList skewIndex,
            @Nullable BitSet writeSymbolAsString,
            @Nullable BitSet writeStringAsVarchar,
            @Nullable BitSet writeTimestampAsNanos
    ) {
        int columnCount = columnFilter.getColumnCount();
        this.columnIndices = new IntList(columnCount);
        this.columnTypes = new IntList(columnCount);
        this.skewedIndices = new IntList(columnCount);
        this.symAsString = new BoolList(columnCount);
        this.strAsVarchar = new BoolList(columnCount);
        this.timestampAsNanos = new BoolList(columnCount);

        for (int i = 0; i < columnCount; i++) {
            final int index = columnFilter.getColumnIndex(i);
            final int factor = columnFilter.getIndexFactor(index);
            final int actualIndex = index * factor - 1;
            final int type = columnTypes.getColumnType(actualIndex);

            // For negative factor (skip columns), store negative type and skip other lookups
            // This aligns with single sink which checks factor < 0 before accessing skewIndex/BitSets
            if (factor < 0) {
                this.columnIndices.extendAndSet(i, actualIndex);
                this.columnTypes.extendAndSet(i, -ColumnType.tagOf(type));
                this.skewedIndices.extendAndSet(i, -1);  // sentinel, not used
                this.symAsString.extendAndSet(i, false);
                this.strAsVarchar.extendAndSet(i, false);
                this.timestampAsNanos.extendAndSet(i, false);
                continue;
            }

            this.columnIndices.extendAndSet(i, actualIndex);
            // Store full type (not just tag) to preserve ARRAY element type info
            this.columnTypes.extendAndSet(i, type);
            this.skewedIndices.extendAndSet(i, getSkewedIndex(actualIndex, skewIndex));
            this.symAsString.extendAndSet(i, writeSymbolAsString != null && writeSymbolAsString.get(actualIndex));
            this.strAsVarchar.extendAndSet(i, writeStringAsVarchar != null && writeStringAsVarchar.get(actualIndex));
            this.timestampAsNanos.extendAndSet(i, writeTimestampAsNanos != null && writeTimestampAsNanos.get(actualIndex));
        }

        // Always create decimal objects - they may be needed by keyFunctions even if no columns use them
        this.decimal128 = new Decimal128();
        this.decimal256 = new Decimal256();
    }

    @Override
    public void copy(Record r, RecordSinkSPI w) {
        // Copy column data
        for (int i = 0, n = columnIndices.size(); i < n; i++) {
            final int type = columnTypes.getQuick(i);
            final int skewedIdx = skewedIndices.getQuick(i);
            final boolean symStr = symAsString.get(i);
            final boolean strVar = strAsVarchar.get(i);
            final boolean tsNanos = timestampAsNanos.get(i);

            // Handle negative types (skip columns)
            if (type < 0) {
                int size = ColumnType.sizeOf(-type);
                if (size > 0) {
                    w.skip(size);
                }
                continue;
            }

            copyColumn(r, w, type, skewedIdx, symStr, strVar, tsNanos);
        }

        // Copy function keys
        if (keyFunctions != null) {
            for (int i = 0, n = keyFunctions.size(); i < n; i++) {
                final Function func = keyFunctions.getQuick(i);
                copyFunction(r, w, func);
            }
        }
    }

    @Override
    public void setFunctions(ObjList<Function> keyFunctions) {
        this.keyFunctions = keyFunctions;
    }

    private static int getSkewedIndex(int src, @Nullable IntList skewIndex) {
        if (skewIndex == null) {
            return src;
        }
        return skewIndex.getQuick(src);
    }

    /**
     * Copies a column value from the record to the sink.
     * <p>
     * Note: This method is intentionally separate from copyFunction() despite similar switch structure.
     * The key differences are:
     * <ul>
     *   <li>copyColumn reads from Record using column index (r.getInt(idx))</li>
     *   <li>copyColumn handles conversion flags (symStr, strVar, tsNanos) for column-specific behavior</li>
     *   <li>copyFunction reads from Function using record (func.getInt(r))</li>
     *   <li>copyFunction always writes symbols as strings, no timestamp conversion</li>
     * </ul>
     * Merging these would require either boxing primitives or complex abstractions that hurt performance.
     */
    private void copyColumn(Record r, RecordSinkSPI w, int type, int idx, boolean symStr, boolean strVar, boolean tsNanos) {
        switch (ColumnType.tagOf(type)) {
            case ColumnType.INT:
                w.putInt(r.getInt(idx));
                break;
            case ColumnType.IPv4:
                w.putIPv4(r.getIPv4(idx));
                break;
            case ColumnType.SYMBOL:
                if (symStr) {
                    if (strVar) {
                        w.putVarchar(r.getSymA(idx));
                    } else {
                        w.putStr(r.getSymA(idx));
                    }
                } else {
                    w.putInt(r.getInt(idx));
                }
                break;
            case ColumnType.LONG:
                w.putLong(r.getLong(idx));
                break;
            case ColumnType.DATE:
                w.putDate(r.getDate(idx));
                break;
            case ColumnType.TIMESTAMP:
                long ts = r.getTimestamp(idx);
                if (tsNanos) {
                    ts *= 1000L;
                }
                w.putTimestamp(ts);
                break;
            case ColumnType.BYTE:
                w.putByte(r.getByte(idx));
                break;
            case ColumnType.SHORT:
                w.putShort(r.getShort(idx));
                break;
            case ColumnType.CHAR:
                w.putChar(r.getChar(idx));
                break;
            case ColumnType.BOOLEAN:
                w.putBool(r.getBool(idx));
                break;
            case ColumnType.FLOAT:
                w.putFloat(r.getFloat(idx));
                break;
            case ColumnType.DOUBLE:
                w.putDouble(r.getDouble(idx));
                break;
            case ColumnType.STRING:
                if (strVar) {
                    w.putVarchar(r.getStrA(idx));
                } else {
                    w.putStr(r.getStrA(idx));
                }
                break;
            case ColumnType.VARCHAR:
                w.putVarchar(r.getVarcharA(idx));
                break;
            case ColumnType.BINARY:
                w.putBin(r.getBin(idx));
                break;
            case ColumnType.LONG256:
                w.putLong256(r.getLong256A(idx));
                break;
            case ColumnType.RECORD:
                w.putRecord(r.getRecord(idx));
                break;
            case ColumnType.GEOBYTE:
                w.putByte(r.getGeoByte(idx));
                break;
            case ColumnType.GEOSHORT:
                w.putShort(r.getGeoShort(idx));
                break;
            case ColumnType.GEOINT:
                w.putInt(r.getGeoInt(idx));
                break;
            case ColumnType.GEOLONG:
                w.putLong(r.getGeoLong(idx));
                break;
            case ColumnType.LONG128:
            case ColumnType.UUID:
                w.putLong128(r.getLong128Lo(idx), r.getLong128Hi(idx));
                break;
            case ColumnType.INTERVAL:
                w.putInterval(r.getInterval(idx));
                break;
            case ColumnType.ARRAY:
                // Pass full type (with element info) to getArray
                w.putArray(r.getArray(idx, type));
                break;
            case ColumnType.DECIMAL8:
                w.putByte(r.getDecimal8(idx));
                break;
            case ColumnType.DECIMAL16:
                w.putShort(r.getDecimal16(idx));
                break;
            case ColumnType.DECIMAL32:
                w.putInt(r.getDecimal32(idx));
                break;
            case ColumnType.DECIMAL64:
                w.putLong(r.getDecimal64(idx));
                break;
            case ColumnType.DECIMAL128:
                r.getDecimal128(idx, decimal128);
                w.putDecimal128(decimal128);
                break;
            case ColumnType.DECIMAL256:
                r.getDecimal256(idx, decimal256);
                w.putDecimal256(decimal256);
                break;
            case ColumnType.NULL:
                // ignore
                break;
            default:
                throw new IllegalArgumentException("Unexpected column type: " + ColumnType.nameOf(type));
        }
    }

    private void copyFunction(Record r, RecordSinkSPI w, Function func) {
        int type = ColumnType.tagOf(func.getType());
        switch (type) {
            case ColumnType.INT:
                w.putInt(func.getInt(r));
                break;
            case ColumnType.IPv4:
                w.putIPv4(func.getIPv4(r));
                break;
            case ColumnType.SYMBOL:
                w.putStr(func.getSymbol(r));
                break;
            case ColumnType.LONG:
                w.putLong(func.getLong(r));
                break;
            case ColumnType.DATE:
                w.putDate(func.getDate(r));
                break;
            case ColumnType.TIMESTAMP:
                w.putTimestamp(func.getTimestamp(r));
                break;
            case ColumnType.BYTE:
                w.putByte(func.getByte(r));
                break;
            case ColumnType.SHORT:
                w.putShort(func.getShort(r));
                break;
            case ColumnType.CHAR:
                w.putChar(func.getChar(r));
                break;
            case ColumnType.BOOLEAN:
                w.putBool(func.getBool(r));
                break;
            case ColumnType.FLOAT:
                w.putFloat(func.getFloat(r));
                break;
            case ColumnType.DOUBLE:
                w.putDouble(func.getDouble(r));
                break;
            case ColumnType.STRING:
                w.putStr(func.getStrA(r));
                break;
            case ColumnType.VARCHAR:
                w.putVarchar(func.getVarcharA(r));
                break;
            case ColumnType.BINARY:
                w.putBin(func.getBin(r));
                break;
            case ColumnType.LONG256:
                w.putLong256(func.getLong256A(r));
                break;
            case ColumnType.RECORD:
                w.putRecord(func.extendedOps().getRecord(r));
                break;
            case ColumnType.GEOBYTE:
                w.putByte(func.getGeoByte(r));
                break;
            case ColumnType.GEOSHORT:
                w.putShort(func.getGeoShort(r));
                break;
            case ColumnType.GEOINT:
                w.putInt(func.getGeoInt(r));
                break;
            case ColumnType.GEOLONG:
                w.putLong(func.getGeoLong(r));
                break;
            case ColumnType.LONG128:
            case ColumnType.UUID:
                w.putLong128(func.getLong128Lo(r), func.getLong128Hi(r));
                break;
            case ColumnType.INTERVAL:
                w.putInterval(func.getInterval(r));
                break;
            case ColumnType.ARRAY:
                w.putArray(func.getArray(r));
                break;
            case ColumnType.DECIMAL8:
                w.putByte(func.getDecimal8(r));
                break;
            case ColumnType.DECIMAL16:
                w.putShort(func.getDecimal16(r));
                break;
            case ColumnType.DECIMAL32:
                w.putInt(func.getDecimal32(r));
                break;
            case ColumnType.DECIMAL64:
                w.putLong(func.getDecimal64(r));
                break;
            case ColumnType.DECIMAL128:
                func.getDecimal128(r, decimal128);
                w.putDecimal128(decimal128);
                break;
            case ColumnType.DECIMAL256:
                func.getDecimal256(r, decimal256);
                w.putDecimal256(decimal256);
                break;
            default:
                throw new IllegalArgumentException("Unexpected function type: " + ColumnType.nameOf(type));
        }
    }
}
