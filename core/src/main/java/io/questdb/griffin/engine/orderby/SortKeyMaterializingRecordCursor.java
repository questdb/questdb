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

package io.questdb.griffin.engine.orderby;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.DelegatingRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.DirectLongLongHashMap;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;

class SortKeyMaterializingRecordCursor implements DelegatingRecordCursor {
    private static final long PAGE_SIZE = 8192;
    private final int[] bufferToColIndex;
    private final MemoryCARW[] buffers;
    private final int[] colSizes;
    private final int[] colToBufferIndex;
    private final int[] colTypes;
    private final Decimal128 decimal128A = new Decimal128();
    private final Decimal256 decimal256A = new Decimal256();
    private final MaterializedRecord recordA = new MaterializedRecord();
    private final MaterializedRecord recordB = new MaterializedRecord();
    private final DirectLongLongHashMap rowIdToOrdinal;
    private RecordCursor baseCursor;
    private boolean isOpen;
    private boolean materialized;
    private long nextOrdinal;

    SortKeyMaterializingRecordCursor(
            int columnCount,
            IntList materializedColIndices,
            IntList materializedColTypes,
            int maxPages
    ) {
        final int bufferCount = materializedColIndices.size();
        this.buffers = new MemoryCARW[bufferCount];
        this.colToBufferIndex = new int[columnCount];
        this.bufferToColIndex = new int[bufferCount];
        this.colTypes = new int[bufferCount];
        this.colSizes = new int[bufferCount];

        for (int i = 0; i < columnCount; i++) {
            colToBufferIndex[i] = -1;
        }

        this.rowIdToOrdinal = new DirectLongLongHashMap(64, 0.5, Long.MIN_VALUE, -1L, MemoryTag.NATIVE_TREE_CHAIN);
        try {
            for (int i = 0; i < bufferCount; i++) {
                final int colIndex = materializedColIndices.getQuick(i);
                final int colType = materializedColTypes.getQuick(i);
                buffers[i] = Vm.getCARWInstance(PAGE_SIZE, maxPages, MemoryTag.NATIVE_TREE_CHAIN);
                colToBufferIndex[colIndex] = i;
                bufferToColIndex[i] = colIndex;
                colTypes[i] = colType;
                colSizes[i] = ColumnType.sizeOf(colType);
            }
        } catch (Throwable th) {
            freeBuffers();
            throw th;
        }
        this.isOpen = true;
    }

    @Override
    public void close() {
        if (isOpen) {
            isOpen = false;
            for (int i = 0, n = buffers.length; i < n; i++) {
                buffers[i].truncate();
            }
            rowIdToOrdinal.restoreInitialCapacity();
            baseCursor = Misc.free(baseCursor);
        }
    }

    @Override
    public Record getRecord() {
        return recordA;
    }

    @Override
    public Record getRecordB() {
        return recordB;
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        return baseCursor.getSymbolTable(columnIndex);
    }

    @Override
    public boolean hasNext() {
        if (baseCursor.hasNext()) {
            final Record baseRecord = recordA.getBaseRecord();
            final long rowId = baseRecord.getRowId();
            long ordinal;
            if (materialized) {
                ordinal = rowIdToOrdinal.get(rowId);
            } else {
                ordinal = nextOrdinal++;
                rowIdToOrdinal.put(rowId, ordinal);
                for (int i = 0, n = buffers.length; i < n; i++) {
                    appendValue(baseRecord, i);
                }
            }
            recordA.setOrdinal(ordinal);
            return true;
        }
        materialized = true;
        return false;
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        return baseCursor.newSymbolTable(columnIndex);
    }

    @Override
    public void of(RecordCursor baseCursor, SqlExecutionContext executionContext) {
        this.baseCursor = baseCursor;
        isOpen = true;
        for (int i = 0, n = buffers.length; i < n; i++) {
            buffers[i].truncate();
        }
        rowIdToOrdinal.reopen();
        rowIdToOrdinal.clear();
        materialized = false;
        nextOrdinal = 0;
        recordA.of(baseCursor.getRecord(), colToBufferIndex, colSizes, buffers);
        recordB.of(baseCursor.getRecordB(), colToBufferIndex, colSizes, buffers);
    }

    @Override
    public long preComputedStateSize() {
        long size = 0;
        for (int i = 0, n = buffers.length; i < n; i++) {
            size += buffers[i].getAppendOffset();
        }
        return size;
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        baseCursor.recordAt(((MaterializedRecord) record).getBaseRecord(), atRowId);
        final long ordinal = rowIdToOrdinal.get(atRowId);
        ((MaterializedRecord) record).setOrdinal(ordinal);
    }

    @Override
    public long size() {
        return baseCursor.size();
    }

    @Override
    public void toTop() {
        baseCursor.toTop();
        if (!materialized) {
            // Interrupted first pass — reset partial materialization
            for (int i = 0, n = buffers.length; i < n; i++) {
                buffers[i].truncate();
            }
            rowIdToOrdinal.clear();
            nextOrdinal = 0;
        }
    }

    private void appendValue(Record record, int bufferIndex) {
        final int colIndex = bufferToColIndex[bufferIndex];
        final MemoryCARW buf = buffers[bufferIndex];
        switch (ColumnType.tagOf(colTypes[bufferIndex])) {
            case ColumnType.BOOLEAN -> buf.putBool(record.getBool(colIndex));
            case ColumnType.BYTE -> buf.putByte(record.getByte(colIndex));
            case ColumnType.SHORT -> buf.putShort(record.getShort(colIndex));
            case ColumnType.CHAR -> buf.putChar(record.getChar(colIndex));
            case ColumnType.INT -> buf.putInt(record.getInt(colIndex));
            case ColumnType.FLOAT -> buf.putFloat(record.getFloat(colIndex));
            case ColumnType.LONG, ColumnType.TIMESTAMP, ColumnType.DATE -> buf.putLong(record.getLong(colIndex));
            case ColumnType.DOUBLE -> buf.putDouble(record.getDouble(colIndex));
            case ColumnType.DECIMAL8 -> buf.putByte(record.getDecimal8(colIndex));
            case ColumnType.DECIMAL16 -> buf.putShort(record.getDecimal16(colIndex));
            case ColumnType.DECIMAL32 -> buf.putInt(record.getDecimal32(colIndex));
            case ColumnType.DECIMAL64 -> buf.putLong(record.getDecimal64(colIndex));
            case ColumnType.DECIMAL128 -> {
                record.getDecimal128(colIndex, decimal128A);
                buf.putDecimal128(decimal128A.getHigh(), decimal128A.getLow());
            }
            case ColumnType.DECIMAL256 -> {
                record.getDecimal256(colIndex, decimal256A);
                buf.putDecimal256(decimal256A.getHh(), decimal256A.getHl(), decimal256A.getLh(), decimal256A.getLl());
            }
            case ColumnType.GEOBYTE -> buf.putByte(record.getGeoByte(colIndex));
            case ColumnType.GEOSHORT -> buf.putShort(record.getGeoShort(colIndex));
            case ColumnType.GEOINT -> buf.putInt(record.getGeoInt(colIndex));
            case ColumnType.GEOLONG -> buf.putLong(record.getGeoLong(colIndex));
            default -> throw new UnsupportedOperationException(
                    "unsupported column type for materialization: " + ColumnType.nameOf(colTypes[bufferIndex])
            );
        }
    }

    void freeBuffers() {
        close();
        for (int i = 0, n = buffers.length; i < n; i++) {
            buffers[i] = Misc.free(buffers[i]);
        }
        rowIdToOrdinal.close();
    }
}
