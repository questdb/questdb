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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordBlock;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.sql.VirtualFunctionRecord;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.PerWorkerLocks;
import io.questdb.griffin.engine.groupby.GroupByBatchKernels;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.util.concurrent.atomic.AtomicLong;

/**
 * The virtual columns of a projection over an Async Filter, computed by the thread that filters
 * a frame, for the frame's filtered rows, into the frame's task: the projection's own thread then
 * reads each value instead of computing it, see {@link ProjectedColumnFunction}.
 * <p>
 * <b>What is computed.</b> Columns of a fixed-size numeric type (boolean to double, date,
 * timestamp) whose expression is not a plain column. Each slot (the owner's, and one per worker)
 * computes them with a copy of the projection's functions of its own, compiled from the same
 * expressions and checked to be of the same classes and types: column-wise, over batches of the
 * frame's filtered rows, where {@link GroupByBatchKernels#compileProjection} compiles the column
 * (as the projection's own cursor computes a block of it), and row by row, over a
 * {@link VirtualFunctionRecord} of its own, for the others and for a batch whose columns the
 * kernels cannot read (a column top, a type cast). Either way a value is the one the projection's
 * own record gives for the row, bit for bit. Values are stored column after column, each a dense array of
 * its type, one value per filtered row in the rows' order, so that a block of the frame's rows
 * exposes them as plain columns, see {@link #getBlockAddress(int)}.
 * <p>
 * <b>When a frame is not computed.</b> A Parquet frame, a count-only task, or a frame on which a
 * copy threw: the task is then marked not projected, and the projection's functions compute that
 * frame's values row by row as before. So an error is raised only when the row is read, as
 * serially, and never for a row a LIMIT left unread.
 * <p>
 * <b>Which reads are served.</b> Only those through the projection cursor's own record, while the
 * forward Async Filter cursor stands on a projected frame; any other record (a record B, a
 * random-access read) and any other cursor (the negative-LIMIT one) compute as before.
 */
public class FrameProjection implements QuietCloseable {
    // the rows of a batch the kernels compute at once, as the projection's cursor's blocks
    private static final int KERNEL_BATCH_ROWS = 4096;
    // the output columns of the projection that are computed here, in stride order
    private final IntList columns;
    private final IntList columnTypes;
    // the sizes of the projection's base columns, which the kernels read, see ProjectionBlock
    private final IntList baseColumnSizes;
    private final ObjList<ProjectionBlock> slotBlocks;
    // per slot, the column-wise evaluator of the projection's copy, or null when it has none
    private final ObjList<GroupByBatchKernels> slotKernels;
    private final PerWorkerLocks locks;
    // the base record each slot's projection record reads: the frame record, or a selection over it
    private final ObjList<Record> slotBaseRecords;
    private final ObjList<ObjList<Function>> slotFunctions;
    private final ObjList<VirtualFunctionRecord> slotRecords;
    private final AtomicLong testProjectedFrames = new AtomicLong();
    // tests: column batches computed column-wise, and row by row, see getBatchCounts()
    private final AtomicLong testKernelBatches = new AtomicLong();
    private final AtomicLong testRowBatches = new AtomicLong();
    private final int reservedSlots;
    private final @Nullable IntList crossIndex;
    // the record whose reads are served, the projection cursor's own, see bind()
    private Record boundRecord;
    // the bytes of a filtered row's values, over all columns, and each column's size
    private final int rowBytes;
    private final int[] columnSizes;
    // the current frame's columns, 0 when the frame is not projected; see ofFrame()
    private final long[] columnAddresses;
    private long frameAddress;
    // the row of the current frame the projection cursor's record stands on, -1 for none
    private long rowPos = -1;
    // the first row of the block the forward cursor exposes, -1 for none; see ofBlock()
    private long blockPos = -1;

    /**
     * @param slotFunctions one copy of the projection's functions per slot, the owner's first
     * @param crossIndex    the base columns of a selection between the projection and the
     *                      filter, null when the projection reads the filter's columns directly
     */
    public FrameProjection(
            @NotNull CairoConfiguration configuration,
            @NotNull ObjList<ObjList<Function>> slotFunctions,
            @NotNull IntList columns,
            @NotNull IntList columnTypes,
            @NotNull IntList baseColumnTypes,
            int reservedSlots,
            @Nullable IntList crossIndex
    ) {
        this.columns = columns;
        this.columnTypes = columnTypes;
        this.reservedSlots = reservedSlots;
        this.crossIndex = crossIndex;
        this.slotFunctions = slotFunctions;
        final int n = columns.size();
        this.columnSizes = new int[n];
        this.columnAddresses = new long[n];
        int bytes = 0;
        for (int j = 0; j < n; j++) {
            columnSizes[j] = ColumnType.sizeOf(columnTypes.getQuick(j));
            bytes += columnSizes[j];
        }
        this.rowBytes = bytes;
        this.baseColumnSizes = new IntList(baseColumnTypes.size());
        for (int c = 0, m = baseColumnTypes.size(); c < m; c++) {
            final int type = baseColumnTypes.getQuick(c);
            baseColumnSizes.add(ColumnType.isVarSize(type) ? 0 : ColumnType.sizeOf(type));
        }
        final int slotCount = slotFunctions.size();
        this.slotRecords = new ObjList<>(slotCount);
        this.slotBaseRecords = new ObjList<>(slotCount);
        this.slotKernels = new ObjList<>(slotCount);
        this.slotBlocks = new ObjList<>(slotCount);
        try {
            for (int i = 0; i < slotCount; i++) {
                slotRecords.add(new VirtualFunctionRecord(slotFunctions.getQuick(i), reservedSlots));
                slotBaseRecords.add(crossIndex != null ? new SelectedRecord(crossIndex) : null);
                final GroupByBatchKernels kernels = GroupByBatchKernels.compileProjection(slotFunctions.getQuick(i), reservedSlots, KERNEL_BATCH_ROWS);
                slotKernels.add(kernels);
                slotBlocks.add(kernels != null ? new ProjectionBlock() : null);
            }
            // the workers' slots; the owner always has its own
            this.locks = new PerWorkerLocks(configuration, Math.max(1, slotCount - 1));
        } catch (Throwable th) {
            // the projection owns the copies from here on: free them, and the kernels compiled so far
            close();
            throw th;
        }
    }

    /**
     * Whether the projection can compute a column of this type.
     */
    public static boolean isSupportedType(int type) {
        return switch (ColumnType.tagOf(type)) {
            case ColumnType.BOOLEAN, ColumnType.BYTE, ColumnType.SHORT, ColumnType.CHAR, ColumnType.INT,
                 ColumnType.LONG, ColumnType.DATE, ColumnType.TIMESTAMP, ColumnType.FLOAT, ColumnType.DOUBLE -> true;
            default -> false;
        };
    }

    /**
     * Serves the reads through this record, the projection cursor's own; see the class doc.
     */
    public void bind(Record record) {
        boundRecord = record;
        ofFrame(0, 0);
    }

    /**
     * The execution ended: frees the kernels' buffers, which the next batch allocates again.
     */
    public void clear() {
        for (int i = 0, n = slotKernels.size(); i < n; i++) {
            final GroupByBatchKernels kernels = slotKernels.getQuick(i);
            if (kernels != null) {
                kernels.clear();
            }
        }
    }

    @Override
    public void close() {
        for (int i = 0, n = slotFunctions.size(); i < n; i++) {
            Misc.freeObjList(slotFunctions.getQuick(i));
        }
        Misc.freeObjList(slotKernels);
        boundRecord = null;
    }

    public int getColumnCount() {
        return columns.size();
    }

    public IntList getColumns() {
        return columns;
    }

    /**
     * Frames whose values a slot computed since the last {@link #resetTestCounter()}.
     */
    @TestOnly
    public long getProjectedFrameCount() {
        return testProjectedFrames.get();
    }

    /**
     * Initializes every slot's copies for an execution, as the filter's per-worker copies are.
     */
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        final MemoryTracker memoryTracker = executionContext.getMemoryTracker();
        for (int i = 0, n = slotKernels.size(); i < n; i++) {
            final GroupByBatchKernels kernels = slotKernels.getQuick(i);
            if (kernels != null) {
                kernels.setMemoryTracker(memoryTracker);
            }
        }
        final SymbolTableSource source = new BaseSymbolTableSource(symbolTableSource);
        final boolean current = executionContext.getCloneSymbolTables();
        executionContext.setCloneSymbolTables(true);
        try {
            for (int i = 0, n = slotFunctions.size(); i < n; i++) {
                Function.init(slotFunctions.getQuick(i), source, executionContext, null);
            }
        } finally {
            executionContext.setCloneSymbolTables(current);
        }
        ofFrame(0, 0);
    }

    /**
     * Whether the block the forward cursor exposes now is of a projected frame.
     */
    public boolean isBlockServed() {
        return blockPos > -1;
    }

    /**
     * Whether the value of a read through this record comes from the frame.
     */
    public boolean isServed(Record record) {
        return rowPos > -1 && record == boundRecord;
    }

    /**
     * The address of column {@code j}'s values from the block's first row on, one value of the
     * column's type per row; valid only when {@link #isBlockServed()}.
     */
    public long getBlockAddress(int j) {
        return columnAddresses[j] + blockPos * columnSizes[j];
    }

    public boolean getBool(int j) {
        return Unsafe.getByte(columnAddresses[j] + rowPos) != 0;
    }

    public byte getByte(int j) {
        return Unsafe.getByte(columnAddresses[j] + rowPos);
    }

    public char getChar(int j) {
        return Unsafe.getChar(columnAddresses[j] + (rowPos << 1));
    }

    public double getDouble(int j) {
        return Unsafe.getDouble(columnAddresses[j] + (rowPos << 3));
    }

    public float getFloat(int j) {
        return Unsafe.getFloat(columnAddresses[j] + (rowPos << 2));
    }

    public int getInt(int j) {
        return Unsafe.getInt(columnAddresses[j] + (rowPos << 2));
    }

    public long getLong(int j) {
        return Unsafe.getLong(columnAddresses[j] + (rowPos << 3));
    }

    public short getShort(int j) {
        return Unsafe.getShort(columnAddresses[j] + (rowPos << 1));
    }

    /**
     * The forward cursor exposes a block of the frame's rows from its {@code pos}-th filtered row.
     */
    public void ofBlock(long pos) {
        blockPos = frameAddress != 0 ? pos : -1;
    }

    /**
     * The forward Async Filter cursor moved to a frame: its values and filtered row count, or 0
     * when the frame is not projected.
     */
    public void ofFrame(long address, long rowCount) {
        frameAddress = address;
        rowPos = -1;
        blockPos = -1;
        long a = address;
        for (int j = 0, n = columnSizes.length; j < n; j++) {
            columnAddresses[j] = a;
            a += rowCount * columnSizes[j];
        }
    }

    /**
     * The forward Async Filter cursor's record moved to the frame's {@code pos}-th filtered row,
     * or, -1, elsewhere.
     */
    public void ofRow(long pos) {
        rowPos = frameAddress != 0 ? pos : -1;
    }

    /**
     * Computes the frame's values for its filtered rows into {@code sink}. Returns false, and
     * leaves the frame to be computed row by row, when a copy threw.
     *
     * @param owner whether the calling thread is the query's own, which has a slot of its own
     */
    public boolean project(
            int workerId,
            boolean owner,
            @NotNull PageFrameMemoryRecord frameRecord,
            @NotNull DirectLongList rows,
            @NotNull DirectLongList sink,
            @Nullable MemoryTracker memoryTracker,
            @NotNull SqlExecutionCircuitBreaker circuitBreaker
    ) {
        final int slot = workerId == -1 && owner ? -1 : locks.acquireSlot(workerId, circuitBreaker);
        try {
            final VirtualFunctionRecord record = slotRecords.getQuick(slot + 1);
            final Record base = slotBaseRecords.getQuick(slot + 1);
            if (base != null) {
                ((SelectedRecord) base).of(frameRecord);
                record.of(base);
            } else {
                record.of(frameRecord);
            }
            final int n = columns.size();
            final long rowCount = rows.size();
            final long words = (rowCount * rowBytes + Long.BYTES - 1) / Long.BYTES;
            sink.clear();
            ensureCapacity(sink, words, memoryTracker);
            final long dst = sink.getAddress();
            final GroupByBatchKernels kernels = slotKernels.getQuick(slot + 1);
            final ProjectionBlock block = slotBlocks.getQuick(slot + 1);
            for (long batchLo = 0; batchLo < rowCount; batchLo += KERNEL_BATCH_ROWS) {
                final int batchRows = (int) Math.min(KERNEL_BATCH_ROWS, rowCount - batchLo);
                if (kernels != null) {
                    block.of(frameRecord, rows, batchLo, batchRows);
                    kernels.ofBlock(block, batchRows);
                }
                long colDst = dst;
                for (int j = 0; j < n; j++) {
                    final int col = columns.getQuick(j);
                    final int size = columnSizes[j];
                    final GroupByBatchKernels.Args args = kernels != null && kernels.isKernel(col) ? kernels.prepare(col) : null;
                    if (args != null) {
                        // the column's values for the batch, as the projection's own block computes them
                        Unsafe.copyMemory(args.address(0), colDst + batchLo * size, (long) batchRows * size);
                        assert testKernelBatches.incrementAndGet() > 0;
                    } else {
                        assert testRowBatches.incrementAndGet() > 0;
                        final int type = columnTypes.getQuick(j);
                        for (long i = batchLo, hi = batchLo + batchRows; i < hi; i++) {
                            frameRecord.setRowIndex(rows.get(i));
                            write(record, col, type, colDst + i * size);
                        }
                    }
                    colDst += rowCount * size;
                }
            }
            sink.setPos(words);
            assert testProjectedFrames.incrementAndGet() > 0;
            return true;
        } catch (CairoException e) {
            if (e.isInterruption() || e.isCancellation() || e.isOutOfMemory()) {
                throw e;
            }
            // computed row by row by the projection's own thread, which raises it if it reads the row
            sink.clear();
            return false;
        } catch (RuntimeException e) {
            // e.g. an ImplicitCastException: the same, raised when the row is read
            sink.clear();
            return false;
        } finally {
            locks.releaseSlot(slot);
        }
    }

    /**
     * Column batches computed column-wise and row by row, since the last {@link #resetTestCounter()}.
     */
    @TestOnly
    public long[] getBatchCounts() {
        return new long[]{testKernelBatches.get(), testRowBatches.get()};
    }

    @TestOnly
    public void resetTestCounter() {
        testProjectedFrames.set(0);
        testKernelBatches.set(0);
        testRowBatches.set(0);
    }

    // Grows the frame's value list, charging the growth to the query's memory limit (a breach throws).
    // The list belongs to a reduce task of the shared queue, which may free or reuse it after the
    // query ends, so its frees are global-only and the charge is released when the query's tracker
    // is reconciled, as covered decode buffers' are, see MemoryTracker.addCoveredBytes().
    private static void ensureCapacity(DirectLongList sink, long words, @Nullable MemoryTracker memoryTracker) {
        final long capacityBefore = sink.getCapacity();
        if (memoryTracker == null || capacityBefore >= words) {
            sink.ensureCapacity(words);
            return;
        }
        sink.setMemoryTracker(memoryTracker);
        try {
            sink.ensureCapacity(words);
        } finally {
            sink.setMemoryTracker(null);
        }
        memoryTracker.addCoveredBytes((sink.getCapacity() - capacityBefore) * Long.BYTES);
    }

    private static void write(Record record, int col, int type, long address) {
        switch (ColumnType.tagOf(type)) {
            case ColumnType.BOOLEAN -> Unsafe.putByte(address, (byte) (record.getBool(col) ? 1 : 0));
            case ColumnType.BYTE -> Unsafe.putByte(address, record.getByte(col));
            case ColumnType.SHORT -> Unsafe.putShort(address, record.getShort(col));
            case ColumnType.CHAR -> Unsafe.putChar(address, record.getChar(col));
            case ColumnType.INT -> Unsafe.putInt(address, record.getInt(col));
            case ColumnType.LONG -> Unsafe.putLong(address, record.getLong(col));
            case ColumnType.DATE -> Unsafe.putLong(address, record.getDate(col));
            case ColumnType.TIMESTAMP -> Unsafe.putLong(address, record.getTimestamp(col));
            case ColumnType.FLOAT -> Unsafe.putFloat(address, record.getFloat(col));
            case ColumnType.DOUBLE -> Unsafe.putDouble(address, record.getDouble(col));
            default -> throw new UnsupportedOperationException();
        }
    }

    // A batch of a frame's filtered rows as the kernels read them: the frame's columns, through the
    // selection when there is one, at the rows' indexes. A column the frame does not hold in place
    // (a column top, a type cast, a var-size column) has no address: the kernels then leave the
    // batch to the row path.
    private class ProjectionBlock implements RecordBlock {
        private PageFrameMemoryRecord frameRecord;
        private int rowCount;
        private long rowsAddr;
        private DirectLongList rows;
        private long rowLo;

        @Override
        public long getColumnAddress(int columnIndex) {
            if (baseColumnSizes.getQuick(columnIndex) == 0) {
                return 0;
            }
            return frameRecord.getPageAddress(crossIndex != null ? crossIndex.getQuick(columnIndex) : columnIndex);
        }

        @Override
        public long getColumnStride(int columnIndex) {
            return baseColumnSizes.getQuick(columnIndex);
        }

        @Override
        public Record getRecordAt(int row) {
            frameRecord.setRowIndex(rows.get(rowLo + row));
            return frameRecord;
        }

        @Override
        public int getRowCount() {
            return rowCount;
        }

        @Override
        public long getRowIndexesAddress() {
            return rowsAddr;
        }

        void of(PageFrameMemoryRecord frameRecord, DirectLongList rows, long rowLo, int rowCount) {
            this.frameRecord = frameRecord;
            this.rows = rows;
            this.rowLo = rowLo;
            this.rowCount = rowCount;
            this.rowsAddr = rows.getAddress() + rowLo * Long.BYTES;
        }
    }

    // The base's symbol tables at the projection's column indexes: its own columns below the
    // reserved slots have none the copies may read (see SqlCodeGenerator, no symbol is computed or
    // read), the base's from there, through the selection when there is one.
    private class BaseSymbolTableSource implements SymbolTableSource {
        private final SymbolTableSource base;

        BaseSymbolTableSource(SymbolTableSource base) {
            this.base = base;
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            final int index = baseIndex(columnIndex);
            return index < 0 ? null : base.getSymbolTable(index);
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            final int index = baseIndex(columnIndex);
            return index < 0 ? null : base.newSymbolTable(index);
        }

        private int baseIndex(int columnIndex) {
            if (columnIndex < reservedSlots) {
                return -1;
            }
            final int index = columnIndex - reservedSlots;
            return crossIndex != null ? crossIndex.getQuick(index) : index;
        }
    }
}
