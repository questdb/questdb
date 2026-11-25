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

package io.questdb.cutlass.parquet;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.StringTypeDriver;
import io.questdb.cairo.SymbolMapReader;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.VarcharTypeDriver;
import io.questdb.cairo.arr.ArrayTypeDriver;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.sql.VirtualFunctionRecord;
import io.questdb.cairo.vm.MemoryCARWImpl;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.griffin.PriorityMetadata;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.columns.ColumnFunction;
import io.questdb.griffin.engine.functions.constants.NullConstant;
import io.questdb.griffin.engine.table.VirtualRecordCursorFactory;
import io.questdb.griffin.engine.table.parquet.ParquetDecoder;
import io.questdb.griffin.engine.table.parquet.RowGroupBuffers;
import io.questdb.std.BoolList;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.DirectIntList;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntList;
import io.questdb.std.Interval;
import io.questdb.std.Long256;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import static io.questdb.cairo.SymbolMapWriter.HEADER_SIZE;

/**
 * Materializes computed columns into native memory buffers for Parquet export.
 * Pass-through columns (simple column references) use page frame pointers directly (zero-copy).
 * Computed columns (expressions) are evaluated row-by-row and written to allocated buffers.
 * <p>
 * A page frame of a Parquet partition, or of {@code read_parquet()}, has no page addresses: its
 * row group is decoded into {@link RowGroupBuffers} in the native column layout, and the frame
 * is then read like a native one.
 * <p>
 * Also supports cursor-based mode (no page frame backing) where all columns are materialized
 * from a RecordCursor.
 */
public class HybridColumnMaterializer implements Mutable, QuietCloseable {
    private static final long DEFAULT_PAGE_SIZE = 1024 * 1024L;
    // Test-only observability for the reuse of decode buffers: decodeParquetFrame() counts the
    // RowGroupBuffers it allocates when decodeBufferPool has none to reuse. The export frees the
    // native memory of every buffer it releases and reuses the objects, so the count is the most
    // decoded frames an export holds at once. Tests read it on the thread that ran the export and
    // assert on deltas.
    private static long decodeBufferAllocationCount;

    /**
     * Returns the number of decode buffers that materializers allocated since class load. The
     * counter is monotonic: capture it before an export and assert on the delta afterwards.
     *
     * @return count of decode buffer allocations
     */
    @TestOnly
    public static long getDecodeBufferAllocationCountForTesting() {
        return decodeBufferAllocationCount;
    }

    private static void addColumnData(DirectLongList columnData, MemoryCARWImpl dataBuf, MemoryCARWImpl auxBuf, int columnType) {
        columnData.add(0L); // no col top
        columnData.add(dataBuf.addressOf(0));
        columnData.add(dataBuf.getAppendOffset());
        if (auxBuf != null) {
            columnData.add(auxBuf.addressOf(0));
            // For STRING columns, the aux buffer has N+1 entries. Discard the
            // last entry, which refers to the offset of the yet-unwritten string.
            long auxSize = auxBuf.getAppendOffset();
            if (ColumnType.tagOf(columnType) == ColumnType.STRING) {
                auxSize -= Long.BYTES;
            }
            columnData.add(auxSize);
        } else {
            columnData.add(0L);
            columnData.add(0L);
        }
        columnData.add(0L);
        columnData.add(0L);
    }

    private static boolean isTypePreservingParquetPassThrough(int sourceColumnType, int outputColumnType) {
        return sourceColumnType == outputColumnType
                && outputColumnType == toExportColumnType(outputColumnType);
    }

    // A bare NULL has no column type of its own: it exports as a STRING column of NULLs, the
    // type /exec reports for it. An export through a temp table does the same, see
    // CreateTableOperationImpl.
    private static int toExportColumnType(int columnType) {
        if (ColumnType.isSymbol(columnType) || ColumnType.isNull(columnType)) {
            return ColumnType.STRING;
        }
        if (columnType == ColumnType.VARCHAR_SLICE) {
            return ColumnType.VARCHAR;
        }
        return columnType;
    }

    private final GenericRecordMetadata adjustedMetadata = new GenericRecordMetadata();
    // Per computed col: aux memory (for var-size) or null (for fixed-size)
    private final ObjList<MemoryCARWImpl> auxBuffers = new ObjList<>();
    // Per output col: base col index, or -1 if computed
    private final IntList baseColumnMap = new IntList();
    // Reuse pool for MemoryCARWImpl instances released by releasePinnedBuffers()
    private final ObjList<MemoryCARWImpl> bufferPool = new ObjList<>();
    // Per output col: index into buffer lists, or -1 if pass-through
    private final IntList computedBufferIdx = new IntList();
    // Dense array of output column indices that are computed (not pass-through)
    private final IntList computedColumnIndices = new IntList();
    // Pre-computed per-column: Function instances, NullConstant.NULL for a NULL-typed one (page-frame path only)
    private final ObjList<Function> computedFunctions = new ObjList<>();
    // Pre-computed per-column: isSymbol(srcType) && outputType == STRING (page-frame path only)
    private final BoolList computedIsSymbolToString = new BoolList();
    // Pre-computed per-column: adjustedMetadata.getColumnType(computedColumnIndices[k])
    private final IntList computedOutputTypes = new IntList();
    // Pre-computed per-column: original column type before SYMBOL→STRING conversion (cursor path)
    private final IntList computedSourceTypes = new IntList();
    // Per computed col: data memory
    private final ObjList<MemoryCARWImpl> dataBuffers = new ObjList<>();
    // Reuse pool for RowGroupBuffers released by releasePinnedBuffers() and releaseFrameBuffers(),
    // which free their native memory: decodeParquetFrame() reopens a pooled buffer for the next frame
    private final ObjList<RowGroupBuffers> decodeBufferPool = new ObjList<>();
    // Per Parquet frame: the [parquetIdx, columnType] pairs of the base columns the file holds
    private final DirectIntList decodeColumns = new DirectIntList(16, MemoryTag.NATIVE_PARQUET_EXPORTER, true);
    // Per Parquet frame: the file column of each base column, see ParquetExportColumnResolver
    private final DirectIntList decodeParquetColumns = new DirectIntList(16, MemoryTag.NATIVE_PARQUET_EXPORTER, true);
    private final Decimal128 decimal128A = new Decimal128();
    private final Decimal256 decimal256A = new Decimal256();
    private final HybridSymbolTableSource hybridSymbolTableSource = new HybridSymbolTableSource();
    private final ReusablePageFrameMemory pageFrameMemory = new ReusablePageFrameMemory();
    private final PageFrameMemoryRecord pageFrameRecord = new PageFrameMemoryRecord();
    private final ParquetExportColumnResolver parquetColumnResolver = new ParquetExportColumnResolver();
    // Buffers that Rust still references (pending_partitions). Freed after row group flush.
    private final ObjList<MemoryCARWImpl> pinnedAuxBuffers = new ObjList<>();
    private final ObjList<MemoryCARWImpl> pinnedDataBuffers = new ObjList<>();
    private final ObjList<RowGroupBuffers> pinnedDecodeBuffers = new ObjList<>();
    private RecordMetadata baseMetadata;
    private int computedCount;
    // The decoded row group of the current Parquet frame; pinned when the next frame starts, unless
    // releaseFrameBuffers() returned it
    private @Nullable RowGroupBuffers decodeBuffers;
    private @Nullable MemoryTracker decodeMemoryTracker;
    private VirtualFunctionRecord functionRecord;
    private ObjList<Function> functions;
    private int outputColumnCount;

    /**
     * Builds column data by reading rows from a RecordCursor (cursor-based path, no page frame backing).
     *
     * @param cursor     the record cursor to read from
     * @param columnData output: 7 longs per column
     * @param batchSize  max rows per batch
     * @return number of rows materialized (0 when cursor exhausted)
     */
    public long buildColumnDataFromCursor(RecordCursor cursor, DirectLongList columnData, long batchSize) {
        Record record = cursor.getRecord();
        if (!cursor.hasNext()) {
            return 0;
        }

        resetBuffers();
        long rowCount = 0;
        do {
            for (int k = 0; k < computedCount; k++) {
                int bufIdx = computedBufferIdx.getQuick(computedColumnIndices.getQuick(k));
                MemoryCARWImpl dataBuf = dataBuffers.getQuick(bufIdx);
                MemoryCARWImpl auxBuf = auxBuffers.getQuick(bufIdx);
                writeColumnValue(record, computedColumnIndices.getQuick(k), computedSourceTypes.getQuick(k), dataBuf, auxBuf);
            }
            rowCount++;
        } while (rowCount < batchSize && cursor.hasNext());

        columnData.clear();
        for (int k = 0; k < computedCount; k++) {
            int bufIdx = computedBufferIdx.getQuick(computedColumnIndices.getQuick(k));
            MemoryCARWImpl dataBuf = dataBuffers.getQuick(bufIdx);
            MemoryCARWImpl auxBuf = auxBuffers.getQuick(bufIdx);
            addColumnData(columnData, dataBuf, auxBuf, computedOutputTypes.getQuick(k));
        }
        return rowCount;
    }

    /**
     * Builds the mixed 7-longs column data array for a page frame (page-frame-backed path).
     * Pass-through columns get their data from the page frame directly.
     * Computed columns get their data from materialization buffers.
     *
     * @param cursor     page frame cursor for symbol tables
     * @param frame      current page frame
     * @param columnData output: 7 longs per column
     * @return number of rows in this frame
     */
    public long buildColumnDataFromPageFrame(PageFrameCursor cursor, PageFrame frame, DirectLongList columnData) {
        CopyExportRequestTask.checkRawPageFrame(frame);
        final long frameRowCount = frame.getPartitionHi() - frame.getPartitionLo();
        pinDecodeBuffers();
        if (frame.getFormat() == PartitionFormat.PARQUET) {
            decodeParquetFrame(cursor, frame);
        } else {
            pageFrameMemory.of(frame);
        }
        pageFrameRecord.init(pageFrameMemory);
        pageFrameRecord.setRowIndex(0);
        materializeComputedColumns(frameRowCount);

        columnData.clear();
        for (int i = 0; i < outputColumnCount; i++) {
            final int baseColIdx = baseColumnMap.getQuick(i);
            final int adjustedType = adjustedMetadata.getColumnType(i);

            if (baseColIdx >= 0) {
                // Pass-through column: read directly from page frame, or from the decoded row
                // group of a Parquet frame.
                // Var-size columns may have an empty .d file when all values are inlined into
                // the aux vector (see FwdTableReaderPageFrameCursor for the producer contract);
                // use the aux address as the column-top detector to avoid materialising live
                // rows as NULL.
                final long pageAddress = pageFrameMemory.getPageAddress(baseColIdx);
                final long localColTop;
                if (ColumnType.isVarSize(adjustedType)) {
                    localColTop = pageFrameMemory.getAuxPageAddress(baseColIdx) > 0 ? 0 : frameRowCount;
                } else {
                    localColTop = pageAddress > 0 ? 0 : frameRowCount;
                }

                columnData.add(localColTop);
                columnData.add(pageAddress);
                columnData.add(pageFrameMemory.getPageSize(baseColIdx));
                if (ColumnType.isSymbol(adjustedType)) {
                    SymbolMapReader symbolMapReader = (SymbolMapReader) cursor.getSymbolTable(baseColIdx);
                    final MemoryR symbolValuesMem = symbolMapReader.getSymbolValuesColumn();
                    final MemoryR symbolOffsetsMem = symbolMapReader.getSymbolOffsetsColumn();
                    columnData.add(symbolValuesMem.addressOf(0));
                    columnData.add(symbolValuesMem.size());
                    columnData.add(symbolOffsetsMem.addressOf(HEADER_SIZE));
                    columnData.add(symbolMapReader.getSymbolCount());
                } else {
                    columnData.add(pageFrameMemory.getAuxPageAddress(baseColIdx));
                    columnData.add(pageFrameMemory.getAuxPageSize(baseColIdx));
                    columnData.add(0L);
                    columnData.add(0L);
                }
            } else {
                // Computed column: use materialization buffers
                int bufIdx = computedBufferIdx.getQuick(i);
                MemoryCARWImpl dataBuf = dataBuffers.getQuick(bufIdx);
                MemoryCARWImpl auxBuf = auxBuffers.getQuick(bufIdx);
                addColumnData(columnData, dataBuf, auxBuf, adjustedType);
            }
        }
        return frameRowCount;
    }

    /**
     * Resets state for reuse: frees data/aux buffers and nulls references,
     * but retains {@code pageFrameMemory}'s native allocations (DirectLongLists).
     * Use {@link #close()} to fully release all native memory including
     * {@code pageFrameMemory}.
     */
    @Override
    public void clear() {
        pinDecodeBuffers();
        releasePinnedBuffers();
        for (int i = 0, n = bufferPool.size(); i < n; i++) {
            Misc.free(bufferPool.getQuick(i));
        }
        bufferPool.clear();
        // Closing the decode buffers credits the memory tracker before its owner recycles it.
        Misc.freeObjListAndClear(decodeBufferPool);
        Misc.free(decodeColumns);
        Misc.free(decodeParquetColumns);
        decodeMemoryTracker = null;
        baseMetadata = null;
        for (int i = 0, n = dataBuffers.size(); i < n; i++) {
            Misc.free(dataBuffers.getQuick(i));
        }
        dataBuffers.clear();
        for (int i = 0, n = auxBuffers.size(); i < n; i++) {
            Misc.free(auxBuffers.getQuick(i));
        }
        auxBuffers.clear();
        pageFrameMemory.clear();
        baseColumnMap.clear();
        computedBufferIdx.clear();
        computedColumnIndices.clear();
        computedFunctions.clear();
        computedIsSymbolToString.clear();
        computedOutputTypes.clear();
        computedSourceTypes.clear();
        functions = null;
        functionRecord = null;
        adjustedMetadata.clear();
        computedCount = 0;
        outputColumnCount = 0;
    }

    @Override
    public void close() {
        clear();
        Misc.free(pageFrameMemory);
        pageFrameRecord.close();
    }

    public GenericRecordMetadata getAdjustedMetadata() {
        return adjustedMetadata;
    }

    public IntList getBaseColumnMap() {
        return baseColumnMap;
    }

    /**
     * Returns the current frame's buffers for the next frame to reuse: the decoded row group, freed,
     * to the pool, and the computed values, truncated in place. Call it once the writer holds none of
     * the frame's rows.
     */
    public void releaseFrameBuffers() {
        if (decodeBuffers != null) {
            releaseDecodeBuffers(decodeBuffers);
            decodeBuffers = null;
        }
        truncateComputedBuffers();
    }

    /**
     * Frees all pinned buffers that were being held for Rust's pending_partitions.
     * Call after a row group flush when all pinned partitions have been drained.
     */
    public void releasePinnedBuffers() {
        for (int i = 0, n = pinnedDataBuffers.size(); i < n; i++) {
            bufferPool.add(pinnedDataBuffers.getQuick(i));
        }
        pinnedDataBuffers.clear();
        for (int i = 0, n = pinnedDecodeBuffers.size(); i < n; i++) {
            releaseDecodeBuffers(pinnedDecodeBuffers.getQuick(i));
        }
        pinnedDecodeBuffers.clear();
        for (int i = 0, n = pinnedAuxBuffers.size(); i < n; i++) {
            MemoryCARWImpl buf = pinnedAuxBuffers.getQuick(i);
            if (buf != null) {
                bufferPool.add(buf);
            }
        }
        pinnedAuxBuffers.clear();
    }

    /**
     * Sets up for cursor-based export: all columns materialized from a RecordCursor
     * (no page frame backing). This is the fallback path used when the cursor-mode
     * factory is not a {@link io.questdb.griffin.engine.table.VirtualRecordCursorFactory}
     * (e.g. join factories, aggregation factories).
     */
    public void setUp(RecordMetadata metadata) {
        this.outputColumnCount = metadata.getColumnCount();
        this.baseColumnMap.setPos(outputColumnCount);
        this.computedBufferIdx.setPos(outputColumnCount);
        this.functions = null;

        adjustedMetadata.clear();
        computedCount = 0;

        for (int i = 0; i < outputColumnCount; i++) {
            int columnType = metadata.getColumnType(i);
            int parquetEncodingConfig = 0;
            if (columnType == toExportColumnType(columnType)) {
                parquetEncodingConfig = metadata.getColumnMetadata(i).getParquetEncodingConfig();
            }
            baseColumnMap.setQuick(i, -1); // all computed in full mode
            addComputedColumn(metadata, i, columnType, parquetEncodingConfig);
        }

        buildComputedColumnIndices();

        int tsIdx = metadata.getTimestampIndex();
        if (tsIdx >= 0) {
            adjustedMetadata.setTimestampIndex(tsIdx);
        }
    }

    /**
     * Sets up for cursor-based export of a virtual factory: all columns are materialized
     * from a RecordCursor, but true pass-through columns can still inherit parquet encoding
     * overrides when the exported type is unchanged.
     */
    public void setUpCursorBacked(VirtualRecordCursorFactory vf) {
        PriorityMetadata priorityMetadata = vf.getPriorityMetadata();
        ObjList<Function> vfFunctions = vf.getFunctions();
        RecordMetadata outputMeta = vf.getMetadata();
        RecordMetadata baseMeta = vf.getBaseFactory().getMetadata();
        this.outputColumnCount = outputMeta.getColumnCount();
        this.baseColumnMap.setPos(outputColumnCount);
        this.computedBufferIdx.setPos(outputColumnCount);
        this.functions = null;

        adjustedMetadata.clear();
        computedCount = 0;

        for (int i = 0; i < outputColumnCount; i++) {
            int columnType = outputMeta.getColumnType(i);
            int parquetEncodingConfig = 0;
            ColumnFunction cf = ColumnFunction.unwrap(vfFunctions.getQuick(i));
            if (cf != null) {
                int baseColIdx = priorityMetadata.getBaseColumnIndex(cf.getColumnIndex());
                if (baseColIdx >= 0) {
                    int sourceColumnType = baseMeta.getColumnType(baseColIdx);
                    if (isTypePreservingParquetPassThrough(sourceColumnType, columnType)) {
                        parquetEncodingConfig = baseMeta.getColumnMetadata(baseColIdx).getParquetEncodingConfig();
                    }
                }
            }

            baseColumnMap.setQuick(i, -1); // all columns are materialized in cursor mode
            addComputedColumn(outputMeta, i, columnType, parquetEncodingConfig);
        }

        buildComputedColumnIndices();

        int tsIdx = outputMeta.getTimestampIndex();
        if (tsIdx >= 0) {
            adjustedMetadata.setTimestampIndex(tsIdx);
        }
    }

    /**
     * Sets up for page-frame-backed export: classifies columns as pass-through or computed,
     * allocates buffers for computed columns only.
     * <p>
     * The caller must ensure there are no computed BINARY columns before calling this method.
     */
    public void setUpPageFrameBacked(
            VirtualRecordCursorFactory vf,
            PageFrameCursor pfc,
            SqlExecutionContext ctx
    ) throws SqlException {
        PriorityMetadata priorityMetadata = vf.getPriorityMetadata();
        this.functions = vf.getFunctions();
        RecordMetadata outputMeta = vf.getMetadata();
        RecordMetadata baseMeta = vf.getBaseFactory().getMetadata();
        this.baseMetadata = baseMeta;
        this.decodeMemoryTracker = ctx.getMemoryTracker();
        this.outputColumnCount = outputMeta.getColumnCount();
        this.baseColumnMap.setPos(outputColumnCount);
        this.computedBufferIdx.setPos(outputColumnCount);
        this.adjustedMetadata.clear();
        this.computedCount = 0;

        for (int i = 0; i < outputColumnCount; i++) {
            ColumnFunction cf = ColumnFunction.unwrap(functions.getQuick(i));
            int baseColIdx = -1;

            if (cf != null) {
                int joinSpaceIdx = cf.getColumnIndex();
                // The column index in function space includes virtualColumnReservedSlots offset.
                // For base columns, the index is >= virtualColumnReservedSlots.
                int priorityMetadataBaseIdx = priorityMetadata.getBaseColumnIndex(joinSpaceIdx);
                if (priorityMetadataBaseIdx >= 0) {
                    baseColIdx = priorityMetadataBaseIdx;
                }
            }

            int columnType = outputMeta.getColumnType(i);
            baseColumnMap.setQuick(i, baseColIdx);

            if (baseColIdx >= 0) {
                // Pass-through
                computedBufferIdx.setQuick(i, -1);
                TableColumnMetadata passThrough;
                if (ColumnType.isSymbol(columnType)) {
                    // SYMBOL columns require the extended constructor
                    passThrough = new TableColumnMetadata(
                            outputMeta.getColumnName(i),
                            columnType,
                            IndexType.NONE,
                            0,
                            true,
                            null
                    );
                } else {
                    passThrough = new TableColumnMetadata(
                            outputMeta.getColumnName(i),
                            columnType
                    );
                }
                // Forward the per-column parquet encoding config from the source
                // metadata so that user-specified PARQUET_ENCODING(...) overrides
                // survive the hybrid pass-through path. Computed columns keep the
                // default 0 because the encoding choice does not translate when
                // the column type changes (e.g. SYMBOL -> STRING).
                passThrough.setParquetEncodingConfig(
                        baseMeta.getColumnMetadata(baseColIdx).getParquetEncodingConfig()
                );
                adjustedMetadata.add(passThrough);
            } else {
                assert ColumnType.tagOf(columnType) != ColumnType.BINARY;
                addComputedColumn(outputMeta, i, columnType);
            }
        }

        buildComputedColumnIndices();

        // Pre-compute function references and symbol-to-string flags
        computedFunctions.setPos(computedCount);
        computedIsSymbolToString.setPos(computedCount);
        for (int k = 0; k < computedCount; k++) {
            int i = computedColumnIndices.getQuick(k);
            Function func = functions.getQuick(i);
            // A NULL-typed function, such as a CASE whose branches are all NULL, holds no value and
            // need not implement getters: NullCaseFunction throws. Read its column through
            // NullConstant, which returns the NULL of any type, as
            // SqlCodeGenerator.generateCastFunctions() does for a NULL-typed column.
            computedFunctions.setQuick(k, ColumnType.isNull(func.getType()) ? NullConstant.NULL : func);
            computedIsSymbolToString.setQuick(k, ColumnType.isSymbol(func.getType())
                    && computedOutputTypes.getQuick(k) == ColumnType.STRING);
        }

        // Set timestamp index
        int tsIdx = outputMeta.getTimestampIndex();
        if (tsIdx >= 0) {
            adjustedMetadata.setTimestampIndex(tsIdx);
        }

        // Set up function evaluation record
        int virtualColumnReservedSlots = priorityMetadata.getVirtualColumnReservedSlots();
        functionRecord = new VirtualFunctionRecord(functions, virtualColumnReservedSlots);
        hybridSymbolTableSource.of(pfc, virtualColumnReservedSlots);
        pageFrameRecord.of(hybridSymbolTableSource);

        // Init functions with symbol table source
        Function.init(functions, hybridSymbolTableSource, ctx, null);
        functionRecord.of(pageFrameRecord);
    }

    private void addComputedColumn(RecordMetadata metadata, int i, int columnType) {
        addComputedColumn(metadata, i, columnType, 0);
    }

    private void addComputedColumn(RecordMetadata metadata, int i, int columnType, int parquetEncodingConfig) {
        int adjustedType = toExportColumnType(columnType);
        computedBufferIdx.setQuick(i, computedCount);
        allocateBuffer(adjustedType);
        computedSourceTypes.add(columnType);
        computedCount++;
        TableColumnMetadata adjustedColumn = new TableColumnMetadata(metadata.getColumnName(i), adjustedType);
        adjustedColumn.setParquetEncodingConfig(parquetEncodingConfig);
        adjustedMetadata.add(adjustedColumn);
    }

    private void allocateBuffer(int columnType) {
        dataBuffers.add(obtainBuffer());
        auxBuffers.add(ColumnType.isVarSize(columnType) ? obtainBuffer() : null);
    }

    private void buildComputedColumnIndices() {
        computedColumnIndices.setPos(computedCount);
        computedOutputTypes.setPos(computedCount);
        int k = 0;
        for (int i = 0; i < outputColumnCount; i++) {
            if (baseColumnMap.getQuick(i) < 0) {
                computedColumnIndices.setQuick(k, i);
                computedOutputTypes.setQuick(k, adjustedMetadata.getColumnType(i));
                k++;
            }
        }
    }

    // Decodes the row group of a Parquet frame into fresh buffers and points pageFrameMemory at
    // them. The encoder reads the pass-through columns from these buffers, so they stay pinned
    // until the writer has written their last row.
    private void decodeParquetFrame(PageFrameCursor cursor, PageFrame frame) {
        final ParquetDecoder decoder = frame.getParquetDecoder();
        decodeParquetColumns.reopen();
        decodeParquetColumns.clear();
        parquetColumnResolver.resolve(cursor, baseMetadata, decoder, decodeParquetColumns);
        decodeColumns.reopen();
        decodeColumns.clear();
        for (int i = 0, n = (int) decodeParquetColumns.size(); i < n; i++) {
            final int parquetColumn = decodeParquetColumns.get(i);
            if (parquetColumn != ParquetExportColumnResolver.ABSENT) {
                // The query's column type, not the file's: read_parquet() reads a SYMBOL
                // column of a QuestDB file as VARCHAR. Materialized column types only (never
                // VARCHAR_SLICE): the buffers hold the native layout and outlive the decode
                // resource, released below.
                decodeColumns.add(parquetColumn);
                decodeColumns.add(baseMetadata.getColumnType(i));
            }
        }
        RowGroupBuffers buffers = decodeBufferPool.size() > 0 ? decodeBufferPool.popLast() : null;
        if (buffers == null) {
            buffers = new RowGroupBuffers(MemoryTag.NATIVE_PARQUET_EXPORTER, true);
            decodeBufferAllocationCount++;
            // The tracker binds at allocation, so set it before reopen().
            buffers.setMemoryTracker(decodeMemoryTracker);
        }
        decodeBuffers = buffers;
        buffers.reopen();
        decoder.decodeRowGroup(
                buffers,
                decodeColumns,
                frame.getParquetRowGroup(),
                frame.getParquetRowGroupLo(),
                frame.getParquetRowGroupHi()
        );
        decoder.releaseDecodeResource(decoder.takeDecodeResource());
        pageFrameMemory.of(buffers, decodeParquetColumns, frame.getPartitionLo());
    }

    private void materializeComputedColumns(long frameRowCount) {
        resetBuffers();
        for (long row = 0; row < frameRowCount; row++) {
            pageFrameRecord.setRowIndex(row);
            for (int k = 0; k < computedCount; k++) {
                int bufIdx = computedBufferIdx.getQuick(computedColumnIndices.getQuick(k));
                MemoryCARWImpl dataBuf = dataBuffers.getQuick(bufIdx);
                MemoryCARWImpl auxBuf = auxBuffers.getQuick(bufIdx);
                if (computedIsSymbolToString.get(k)) {
                    CharSequence sym = computedFunctions.getQuick(k).getSymbol(functionRecord.getInternalJoinRecord());
                    StringTypeDriver.appendValue(auxBuf, dataBuf, sym);
                } else {
                    writeComputedValue(computedFunctions.getQuick(k), functionRecord.getInternalJoinRecord(), computedOutputTypes.getQuick(k), dataBuf, auxBuf);
                }
            }
        }
    }

    private MemoryCARWImpl obtainBuffer() {
        int n = bufferPool.size();
        if (n > 0) {
            MemoryCARWImpl buf = bufferPool.getQuick(n - 1);
            bufferPool.setQuick(n - 1, null);
            bufferPool.setPos(n - 1);
            buf.truncate();
            return buf;
        }
        return new MemoryCARWImpl(DEFAULT_PAGE_SIZE, Integer.MAX_VALUE, MemoryTag.NATIVE_PARQUET_EXPORTER);
    }

    private void pinDecodeBuffers() {
        if (decodeBuffers != null) {
            pinnedDecodeBuffers.add(decodeBuffers);
            decodeBuffers = null;
        }
    }

    // Frees the native memory of a decoded row group that the writer no longer reads, and pools the
    // object for a later frame. A buffer would otherwise keep the capacity of the largest row group
    // it decoded, charged to the query's memory tracker, whatever frame it holds next.
    private void releaseDecodeBuffers(RowGroupBuffers buffers) {
        buffers.close();
        decodeBufferPool.add(buffers);
    }

    private void resetBuffers() {
        // Check if any buffer holds rows that Rust may still reference. A VARCHAR value of at most
        // 9 bytes, a NULL VARCHAR and a NULL array take bytes in the aux buffer alone.
        boolean hasRows = false;
        for (int i = 0; i < computedCount; i++) {
            final MemoryCARWImpl auxBuf = auxBuffers.getQuick(i);
            if (dataBuffers.getQuick(i).getAppendOffset() > 0 || (auxBuf != null && auxBuf.getAppendOffset() > 0)) {
                hasRows = true;
                break;
            }
        }

        if (hasRows) {
            // Pin current buffers (Rust still references them via pending_partitions)
            // and allocate fresh ones for the next frame.
            for (int i = 0; i < computedCount; i++) {
                pinnedDataBuffers.add(dataBuffers.getQuick(i));
                pinnedAuxBuffers.add(auxBuffers.getQuick(i));
            }
            dataBuffers.clear();
            auxBuffers.clear();
            int savedComputedCount = computedCount;
            computedCount = 0;
            for (int k = 0; k < savedComputedCount; k++) {
                computedBufferIdx.setQuick(computedColumnIndices.getQuick(k), computedCount);
                allocateBuffer(computedOutputTypes.getQuick(k));
                computedCount++;
            }
        } else {
            // First call or empty buffers: truncate in place.
            truncateComputedBuffers();
        }

        // Write the zero offset of the first string in a STRING data column.
        // StringTypeDriver will then append the offset of the first available byte
        // after each written string (start offset of the next, yet unwritten string).
        for (int k = 0; k < computedCount; k++) {
            if (ColumnType.tagOf(computedOutputTypes.getQuick(k)) == ColumnType.STRING) {
                auxBuffers.getQuick(computedBufferIdx.getQuick(computedColumnIndices.getQuick(k))).putLong(0);
            }
        }
    }

    private void truncateComputedBuffers() {
        for (int i = 0; i < computedCount; i++) {
            dataBuffers.getQuick(i).truncate();
            MemoryCARWImpl auxBuf = auxBuffers.getQuick(i);
            if (auxBuf != null) {
                auxBuf.truncate();
            }
        }
    }

    private void writeColumnValue(Record record, int col, int columnType, MemoryCARWImpl dataBuf, MemoryCARWImpl auxBuf) {
        switch (ColumnType.tagOf(columnType)) {
            case ColumnType.BOOLEAN -> dataBuf.putBool(record.getBool(col));
            case ColumnType.BYTE -> dataBuf.putByte(record.getByte(col));
            case ColumnType.SHORT -> dataBuf.putShort(record.getShort(col));
            case ColumnType.CHAR -> dataBuf.putChar(record.getChar(col));
            case ColumnType.INT -> dataBuf.putInt(record.getInt(col));
            case ColumnType.LONG -> dataBuf.putLong(record.getLong(col));
            case ColumnType.DATE -> dataBuf.putLong(record.getDate(col));
            case ColumnType.TIMESTAMP -> dataBuf.putLong(record.getTimestamp(col));
            case ColumnType.FLOAT -> dataBuf.putFloat(record.getFloat(col));
            case ColumnType.DOUBLE -> dataBuf.putDouble(record.getDouble(col));
            case ColumnType.STRING -> StringTypeDriver.appendValue(auxBuf, dataBuf, record.getStrA(col));
            case ColumnType.VARCHAR, ColumnType.VARCHAR_SLICE ->
                    VarcharTypeDriver.appendValue(auxBuf, dataBuf, record.getVarcharA(col));
            case ColumnType.SYMBOL -> {
                // Symbols get converted to STRING in adjusted metadata
                CharSequence sym = record.getSymA(col);
                StringTypeDriver.appendValue(auxBuf, dataBuf, sym);
            }
            // Exported as STRING. A row that a sort copies keeps no bytes for a NULL column, and
            // a getter would read the next column: write NULL without reading.
            case ColumnType.NULL -> StringTypeDriver.INSTANCE.appendNull(auxBuf, dataBuf);
            case ColumnType.LONG256 -> {
                Long256 val = record.getLong256A(col);
                dataBuf.putLong256(val);
            }
            case ColumnType.LONG128, ColumnType.UUID ->
                    dataBuf.putLong128(record.getLong128Lo(col), record.getLong128Hi(col));
            case ColumnType.IPv4 -> dataBuf.putInt(record.getIPv4(col));
            case ColumnType.GEOBYTE -> dataBuf.putByte(record.getGeoByte(col));
            case ColumnType.GEOSHORT -> dataBuf.putShort(record.getGeoShort(col));
            case ColumnType.GEOINT -> dataBuf.putInt(record.getGeoInt(col));
            case ColumnType.GEOLONG -> dataBuf.putLong(record.getGeoLong(col));
            case ColumnType.DECIMAL8 -> dataBuf.putByte(record.getDecimal8(col));
            case ColumnType.DECIMAL16 -> dataBuf.putShort(record.getDecimal16(col));
            case ColumnType.DECIMAL32 -> dataBuf.putInt(record.getDecimal32(col));
            case ColumnType.DECIMAL64 -> dataBuf.putLong(record.getDecimal64(col));
            case ColumnType.DECIMAL128 -> {
                record.getDecimal128(col, decimal128A);
                dataBuf.putDecimal128(decimal128A.getHigh(), decimal128A.getLow());
            }
            case ColumnType.DECIMAL256 -> {
                record.getDecimal256(col, decimal256A);
                dataBuf.putDecimal256(decimal256A.getHh(), decimal256A.getHl(), decimal256A.getLh(), decimal256A.getLl());
            }
            case ColumnType.INTERVAL -> {
                Interval iv = record.getInterval(col);
                dataBuf.putLong(iv.getLo());
                dataBuf.putLong(iv.getHi());
            }
            case ColumnType.ARRAY -> ArrayTypeDriver.appendValue(auxBuf, dataBuf, record.getArray(col, columnType));
            // Native layout: one aux offset per row, pointing at [length][bytes] in data.
            // A zero-copy export over custom (Delta) frames falls back here with BINARY.
            case ColumnType.BINARY -> {
                auxBuf.putLong(dataBuf.getAppendOffset());
                dataBuf.putBin(record.getBin(col));
            }
            default ->
                    throw new UnsupportedOperationException("unsupported column type: " + ColumnType.nameOf(columnType));
        }
    }

    private void writeComputedValue(Function func, Record record, int outputType, MemoryCARWImpl dataBuf, MemoryCARWImpl auxBuf) {
        switch (ColumnType.tagOf(outputType)) {
            case ColumnType.BOOLEAN -> dataBuf.putBool(func.getBool(record));
            case ColumnType.BYTE -> dataBuf.putByte(func.getByte(record));
            case ColumnType.SHORT -> dataBuf.putShort(func.getShort(record));
            case ColumnType.CHAR -> dataBuf.putChar(func.getChar(record));
            case ColumnType.INT -> dataBuf.putInt(func.getInt(record));
            case ColumnType.LONG -> dataBuf.putLong(func.getLong(record));
            case ColumnType.DATE -> dataBuf.putLong(func.getDate(record));
            case ColumnType.TIMESTAMP -> dataBuf.putLong(func.getTimestamp(record));
            case ColumnType.FLOAT -> dataBuf.putFloat(func.getFloat(record));
            case ColumnType.DOUBLE -> dataBuf.putDouble(func.getDouble(record));
            case ColumnType.STRING -> StringTypeDriver.appendValue(auxBuf, dataBuf, func.getStrA(record));
            case ColumnType.VARCHAR -> VarcharTypeDriver.appendValue(auxBuf, dataBuf, func.getVarcharA(record));
            case ColumnType.LONG256 -> {
                Long256 val = func.getLong256A(record);
                dataBuf.putLong256(val);
            }
            case ColumnType.LONG128, ColumnType.UUID ->
                    dataBuf.putLong128(func.getLong128Lo(record), func.getLong128Hi(record));
            case ColumnType.IPv4 -> dataBuf.putInt(func.getIPv4(record));
            case ColumnType.GEOBYTE -> dataBuf.putByte(func.getGeoByte(record));
            case ColumnType.GEOSHORT -> dataBuf.putShort(func.getGeoShort(record));
            case ColumnType.GEOINT -> dataBuf.putInt(func.getGeoInt(record));
            case ColumnType.GEOLONG -> dataBuf.putLong(func.getGeoLong(record));
            case ColumnType.DECIMAL8 -> dataBuf.putByte(func.getDecimal8(record));
            case ColumnType.DECIMAL16 -> dataBuf.putShort(func.getDecimal16(record));
            case ColumnType.DECIMAL32 -> dataBuf.putInt(func.getDecimal32(record));
            case ColumnType.DECIMAL64 -> dataBuf.putLong(func.getDecimal64(record));
            case ColumnType.DECIMAL128 -> {
                func.getDecimal128(record, decimal128A);
                dataBuf.putDecimal128(decimal128A.getHigh(), decimal128A.getLow());
            }
            case ColumnType.DECIMAL256 -> {
                func.getDecimal256(record, decimal256A);
                dataBuf.putDecimal256(decimal256A.getHh(), decimal256A.getHl(), decimal256A.getLh(), decimal256A.getLl());
            }
            case ColumnType.INTERVAL -> {
                Interval iv = func.getInterval(record);
                dataBuf.putLong(iv.getLo());
                dataBuf.putLong(iv.getHi());
            }
            case ColumnType.ARRAY -> ArrayTypeDriver.appendValue(auxBuf, dataBuf, func.getArray(record));
            // determineExportMode() routes queries with computed BINARY columns to
            // TEMP_TABLE mode, so this method never encounters BINARY.
            default ->
                    throw new UnsupportedOperationException("unsupported column type: " + ColumnType.nameOf(outputType));
        }
    }

    /**
     * Routes symbol table lookups: virtual columns → null, base columns → page frame cursor.
     */
    private static class HybridSymbolTableSource implements SymbolTableSource {
        private PageFrameCursor pageFrameCursor;
        private int virtualColumnReservedSlots;

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            if (columnIndex < virtualColumnReservedSlots) {
                return null;
            }
            return pageFrameCursor.getSymbolTable(columnIndex - virtualColumnReservedSlots);
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            if (columnIndex < virtualColumnReservedSlots) {
                return null;
            }
            return pageFrameCursor.newSymbolTable(columnIndex - virtualColumnReservedSlots);
        }

        void of(PageFrameCursor pageFrameCursor, int virtualColumnReservedSlots) {
            this.pageFrameCursor = pageFrameCursor;
            this.virtualColumnReservedSlots = virtualColumnReservedSlots;
        }
    }

}
