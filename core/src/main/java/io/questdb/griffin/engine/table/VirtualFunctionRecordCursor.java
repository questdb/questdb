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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.ParquetDecodeHint;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordBlock;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.VirtualFunctionRecord;
import io.questdb.griffin.PriorityMetadata;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.functions.columns.ColumnFunction;
import io.questdb.griffin.engine.functions.memoization.MemoizerFunction;
import io.questdb.griffin.engine.groupby.GroupByBatchKernels;
import io.questdb.griffin.engine.groupby.GroupByUtils;
import io.questdb.std.DirectLongLongSortedList;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

// Final: peekRecordBlock() exposes the base's rows and the record's values as the rows hasNext()
// returns. A subclass that changed the rows in hasNext() or getRecord() would offer blocks that
// bypass it.
public final class VirtualFunctionRecordCursor implements RecordCursor {
    // the most rows of a block whose computed columns are evaluated column-wise in one go
    private static final int KERNEL_BLOCK_ROWS = 4096;
    private final VirtualFunctionRecord recordA;
    private final ObjList<Function> functions;
    private final int memoizerCount;
    private final ObjList<MemoizerFunction> memoizers;
    private final PriorityMetadata priorityMetadata;
    private final VirtualFunctionRecord recordB;
    private final boolean supportsRandomAccess;
    private final int virtualColumnReservedSlots;
    private RecordCursor baseCursor;
    // per column, the base column it reads unchanged, for block pass-through, or -1 for a column the
    // record computes; null until first asked
    private IntList blockBaseColumns;
    private VirtualBlock block;
    // whether no column computes a SYMBOL: see supportsRecordBlocks()
    private boolean blocksAllowed;
    // per column, the address of the current block's values of a column computed column-wise, or 0
    private long[] kernelAddresses;
    // evaluates the computed columns it has loops for over a block's rows, or null for none
    private GroupByBatchKernels kernels;
    // the query's, which the kernels' buffers are charged to
    private MemoryTracker memoryTracker;

    public VirtualFunctionRecordCursor(
            @NotNull PriorityMetadata priorityMetadata,
            @NotNull ObjList<Function> functions,
            @NotNull ObjList<MemoizerFunction> memoizers,
            boolean supportsRandomAccess,
            int virtualColumnReservedSlots
    ) {
        this.priorityMetadata = priorityMetadata;
        this.functions = functions;
        this.memoizers = memoizers;
        this.memoizerCount = memoizers.size();
        if (supportsRandomAccess) {
            this.recordA = new VirtualFunctionRecord(functions, virtualColumnReservedSlots);
            this.recordB = new VirtualFunctionRecord(functions, virtualColumnReservedSlots);
        } else {
            this.recordA = new VirtualFunctionRecord(functions, virtualColumnReservedSlots);
            this.recordB = null;
        }
        this.supportsRandomAccess = supportsRandomAccess;
        this.virtualColumnReservedSlots = virtualColumnReservedSlots;
    }

    @Override
    public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
        assert baseCursor != null;
        baseCursor.calculateSize(circuitBreaker, counter);
    }

    @Override
    public void close() {
        if (kernels != null) {
            // the buffers go with the cursor, and the tracker, which of() binds again; the next
            // block allocates them again
            kernels.clear();
        }
        memoryTracker = null;
        baseCursor = Misc.free(baseCursor);
        for (int i = 0, n = functions.size(); i < n; i++) {
            functions.getQuick(i).cursorClosed();
        }
    }

    @Override
    public void expectLimitedIteration() {
        baseCursor.expectLimitedIteration();
    }

    public int getLongTopKColumnIndex(int columnIndex) {
        if (!supportsRandomAccess) {
            return -1;
        }
        ColumnFunction columnFunction = ColumnFunction.unwrap(functions.getQuick(columnIndex));
        if (columnFunction == null) {
            return -1;
        }
        final int virtualColumnIndex = columnFunction.getColumnIndex();
        final int columnType = priorityMetadata.getColumnType(virtualColumnIndex);
        if (columnType == ColumnType.LONG || ColumnType.isTimestamp(columnType)) {
            return priorityMetadata.getBaseColumnIndex(virtualColumnIndex);
        }
        return -1;
    }

    @Override
    public Record getRecord() {
        return recordA;
    }

    @Override
    public Record getRecordB() {
        if (supportsRandomAccess) {
            return recordB;
        }
        throw new UnsupportedOperationException();
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        return (SymbolTable) functions.getQuick(columnIndex);
    }

    @Override
    public boolean hasNext() {
        final boolean result = baseCursor.hasNext();
        if (result) {
            clearMemos();
        }
        return result;
    }

    @Override
    public void longTopK(DirectLongLongSortedList list, int columnIndex) {
        final int baseColumnIndex = getLongTopKColumnIndex(columnIndex);
        assert baseColumnIndex != -1;
        baseCursor.longTopK(list, baseColumnIndex);
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        return ((SymbolFunction) functions.getQuick(columnIndex)).newSymbolTable();
    }

    public void of(RecordCursor cursor, @Nullable MemoryTracker memoryTracker) {
        this.memoryTracker = memoryTracker;
        if (kernels != null) {
            kernels.setMemoryTracker(memoryTracker);
        }
        baseCursor = cursor;
        recordA.of(baseCursor.getRecord());
        if (recordB != null) {
            recordB.of(baseCursor.getRecordB());
        }
        cursor.toTop();
    }

    /**
     * The base's block: a column that reads a base column unchanged exposes the base block's
     * memory for it; a column computed by arithmetic and casts over base columns, which
     * {@link GroupByBatchKernels#compileProjection} compiles, is evaluated column-wise over the
     * block's rows into a buffer the block exposes; every other column is computed through
     * {@link RecordBlock#getRecordAt}, on a record positioned at the base block's row, row by row.
     */
    @Override
    public RecordBlock peekRecordBlock(int maxRows) {
        if (blockBaseColumns == null) {
            mapBlockBaseColumns();
        }
        if (!blocksAllowed) {
            return null;
        }
        final GroupByBatchKernels kernels = this.kernels;
        final RecordBlock baseBlock = baseCursor.peekRecordBlock(kernels != null ? Math.min(maxRows, KERNEL_BLOCK_ROWS) : maxRows);
        if (baseBlock == null) {
            return null;
        }
        if (block == null) {
            block = new VirtualBlock();
        }
        block.base = baseBlock;
        if (kernels != null) {
            kernels.ofBlock(baseBlock, baseBlock.getRowCount());
            for (int i = 0, n = kernelAddresses.length; i < n; i++) {
                if (kernels.isKernel(i)) {
                    // null: a column it reads is not in the block's memory, so the row path
                    final GroupByBatchKernels.Args args = kernels.prepare(i);
                    kernelAddresses[i] = args != null ? args.address(0) : 0;
                }
            }
        }
        return block;
    }

    @Override
    public long preComputedStateSize() {
        return 0;
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        if (supportsRandomAccess) {
            assert baseCursor != null;
            baseCursor.recordAt(((VirtualFunctionRecord) record).getBaseRecord(), atRowId);
            clearMemos();
        } else {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public void setParquetDecodeHint(ParquetDecodeHint hint) {
        if (baseCursor != null) {
            baseCursor.setParquetDecodeHint(hint);
        }
    }

    @Override
    public void setRecordAtRows(@Nullable RowIdSource source) {
        if (baseCursor != null) {
            baseCursor.setRecordAtRows(source);
        }
    }

    @Override
    public long size() {
        assert baseCursor != null;
        return baseCursor.size();
    }

    @Override
    public void skipRecordBlock(int rowCount) {
        baseCursor.skipRecordBlock(rowCount);
    }

    @Override
    public void skipRows(Counter rowCount, long maxRowsAfterSkip) {
        assert baseCursor != null;
        baseCursor.skipRows(rowCount, maxRowsAfterSkip);
    }

    /**
     * The base's answer, unless a column computes a SYMBOL. The block fill reads SYMBOL columns
     * first, row by row, before the other columns, so a computed SYMBOL column would be evaluated
     * out of the row path's column order; the other computed columns are evaluated in it.
     */
    @Override
    public boolean supportsRecordBlocks() {
        if (blockBaseColumns == null) {
            mapBlockBaseColumns();
        }
        return blocksAllowed && baseCursor.supportsRecordBlocks();
    }

    @Override
    public void toTop() {
        assert baseCursor != null;
        baseCursor.toTop();
        GroupByUtils.toTop(functions);
    }

    private void clearMemos() {
        for (int i = 0; i < memoizerCount; i++) {
            memoizers.getQuick(i).clearMemo();
        }
    }

    private void mapBlockBaseColumns() {
        final int n = functions.size();
        final IntList baseColumns = new IntList(n);
        boolean allowed = true;
        for (int i = 0; i < n; i++) {
            final Function function = functions.getQuick(i);
            final ColumnFunction columnFunction = ColumnFunction.unwrap(function);
            int baseColumn = -1;
            if (columnFunction != null) {
                final int index = columnFunction.getColumnIndex();
                // a reference to a base column, read as the base's type
                if (index >= virtualColumnReservedSlots && priorityMetadata.getColumnType(index) == function.getType()) {
                    baseColumn = priorityMetadata.getBaseColumnIndex(index);
                }
            }
            if (baseColumn == -1 && ColumnType.tagOf(function.getType()) == ColumnType.SYMBOL) {
                allowed = false;
            }
            baseColumns.add(baseColumn);
        }
        blocksAllowed = allowed;
        blockBaseColumns = baseColumns;
        if (allowed) {
            kernels = GroupByBatchKernels.compileProjection(functions, virtualColumnReservedSlots, KERNEL_BLOCK_ROWS);
            if (kernels != null) {
                kernels.setMemoryTracker(memoryTracker);
                kernelAddresses = new long[n];
            }
        }
    }

    private class VirtualBlock implements RecordBlock {
        private RecordBlock base;
        // computes the columns over a base block record that is not the base cursor's own record
        private VirtualFunctionRecord record;

        @Override
        public long getColumnAddress(int columnIndex) {
            final int baseColumn = blockBaseColumns.getQuick(columnIndex);
            if (baseColumn != -1) {
                return base.getColumnAddress(baseColumn);
            }
            return kernels != null ? kernelAddresses[columnIndex] : 0;
        }

        @Override
        public long getColumnRowIndexesAddress(int columnIndex) {
            final int baseColumn = blockBaseColumns.getQuick(columnIndex);
            // a column computed column-wise holds one value per block row, in order
            return baseColumn != -1 ? base.getColumnRowIndexesAddress(baseColumn) : 0;
        }

        @Override
        public long getColumnStride(int columnIndex) {
            final int baseColumn = blockBaseColumns.getQuick(columnIndex);
            if (baseColumn != -1) {
                return base.getColumnStride(baseColumn);
            }
            return ColumnType.sizeOf(functions.getQuick(columnIndex).getType());
        }

        @Override
        public Record getRecordAt(int row) {
            final Record baseRecord = base.getRecordAt(row);
            // a new row: hasNext() would have cleared the memos
            clearMemos();
            if (baseRecord == recordA.getBaseRecord()) {
                return recordA;
            }
            if (record == null) {
                record = new VirtualFunctionRecord(functions, virtualColumnReservedSlots);
            }
            if (record.getBaseRecord() != baseRecord) {
                record.of(baseRecord);
            }
            return record;
        }

        @Override
        public int getRowCount() {
            return base.getRowCount();
        }

        @Override
        public long getRowIndexesAddress() {
            return base.getRowIndexesAddress();
        }
    }
}
