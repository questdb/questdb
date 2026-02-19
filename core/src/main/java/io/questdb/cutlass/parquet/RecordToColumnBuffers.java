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

package io.questdb.cutlass.parquet;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.StringTypeDriver;
import io.questdb.cairo.SymbolMapReader;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.VarcharTypeDriver;
import io.questdb.cairo.arr.ArrayTypeDriver;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.RecordCursorFactory;
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
import io.questdb.griffin.engine.QueryProgress;
import io.questdb.griffin.engine.functions.columns.ColumnFunction;
import io.questdb.griffin.engine.table.VirtualRecordCursorFactory;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntHashSet;
import io.questdb.std.Interval;
import io.questdb.std.Long256;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.str.Utf8Sequence;

import static io.questdb.cairo.SymbolMapWriter.HEADER_SIZE;

/**
 * Materializes computed columns into native memory buffers for Parquet export.
 * Pass-through columns (simple column references) use page frame pointers directly (zero-copy).
 * Computed columns (expressions) are evaluated row-by-row and written to allocated buffers.
 * <p>
 * Also supports cursor-based mode (no page frame backing) where all columns are materialized
 * from a RecordCursor.
 */
public class RecordToColumnBuffers implements Mutable, QuietCloseable {
    private static final long DEFAULT_PAGE_SIZE = 1024 * 1024L;
    // Per computed col: aux memory (for var-size) or null (for fixed-size)
    private final ObjList<MemoryCARWImpl> auxBuffers = new ObjList<>();
    // Per computed col: data memory
    private final ObjList<MemoryCARWImpl> dataBuffers = new ObjList<>();
    private final Decimal128 decimal128A = new Decimal128();
    private final Decimal256 decimal256A = new Decimal256();
    private final PageFrameMemoryRecord pageFrameRecord = new PageFrameMemoryRecord();
    private final ReusablePageFrameMemory pfMemory = new ReusablePageFrameMemory();
    // Buffers that Rust still references (pending_partitions). Freed after row group flush.
    private final ObjList<MemoryCARWImpl> pinnedAuxBuffers = new ObjList<>();
    private final ObjList<MemoryCARWImpl> pinnedDataBuffers = new ObjList<>();
    private GenericRecordMetadata adjustedMetadata;
    // Per output col: base col index, or -1 if computed
    private int[] baseColumnMap;
    // Per output col: index into buffer lists, or -1 if pass-through
    private int[] computedBufferIdx;
    private int computedCount;
    private VirtualFunctionRecord functionRecord;
    private ObjList<Function> functions;
    private int outputColumnCount;

    /**
     * Determines the export mode for a given RecordCursorFactory.
     * Shared by both HTTP and SQL export paths.
     */
    public static ParquetExportMode determineExportMode(RecordCursorFactory factory) {
        RecordCursorFactory unwrapped = unwrapFactory(factory);
        if (factory.supportsPageFrameCursor()) {
            return ParquetExportMode.DIRECT_PAGE_FRAME;
        }
        if (unwrapped instanceof VirtualRecordCursorFactory vf && vf.getBaseFactory().supportsPageFrameCursor()) {
            if (hasComputedBinaryColumn(vf)) {
                return ParquetExportMode.TEMP_TABLE;
            }
            return ParquetExportMode.PAGE_FRAME_BACKED;
        }
        RecordMetadata meta = factory.getMetadata();
        for (int i = 0, n = meta.getColumnCount(); i < n; i++) {
            if (ColumnType.tagOf(meta.getColumnType(i)) == ColumnType.BINARY) {
                return ParquetExportMode.TEMP_TABLE;
            }
        }
        return ParquetExportMode.CURSOR_BASED;
    }

    /**
     * Checks whether a VirtualRecordCursorFactory has any computed BINARY columns.
     * Computed BINARY columns cannot be materialized into buffers for Parquet export.
     */
    public static boolean hasComputedBinaryColumn(VirtualRecordCursorFactory vf) {
        PriorityMetadata pm = vf.getPriorityMetadata();
        ObjList<Function> functions = vf.getFunctions();
        RecordMetadata meta = vf.getMetadata();
        for (int i = 0, n = meta.getColumnCount(); i < n; i++) {
            if (ColumnType.tagOf(meta.getColumnType(i)) != ColumnType.BINARY) {
                continue;
            }
            Function func = functions.getQuick(i);
            if (func instanceof ColumnFunction cf) {
                if (pm.getBaseColumnIndex(cf.getColumnIndex()) >= 0) {
                    continue; // pass-through BINARY is fine
                }
            }
            return true;
        }
        return false;
    }

    /**
     * Unwraps a QueryProgress wrapper to access the underlying factory.
     */
    public static RecordCursorFactory unwrapFactory(RecordCursorFactory factory) {
        return factory instanceof QueryProgress qp ? qp.getBaseFactory() : factory;
    }

    /**
     * Builds column data by reading rows from a RecordCursor (cursor-based path, no page frame backing).
     *
     * @param cursor     the record cursor to read from
     * @param columnData output: 7 longs per column
     * @param batchSize  max rows per batch
     * @return number of rows materialized (0 when cursor exhausted)
     */
    public long buildColumnDataFromCursor(RecordCursor cursor, DirectLongList columnData, long batchSize) {
        resetBuffers();
        Record record = cursor.getRecord();
        long rowCount = 0;

        while (rowCount < batchSize && cursor.hasNext()) {
            for (int i = 0; i < outputColumnCount; i++) {
                int bufIdx = computedBufferIdx[i];
                MemoryCARWImpl dataBuf = dataBuffers.getQuick(bufIdx);
                MemoryCARWImpl auxBuf = auxBuffers.getQuick(bufIdx);
                int columnType = adjustedMetadata.getColumnType(i);
                writeColumnValue(record, i, columnType, dataBuf, auxBuf);
            }
            rowCount++;
        }

        if (rowCount == 0) {
            return 0;
        }

        columnData.clear();
        for (int i = 0; i < outputColumnCount; i++) {
            int bufIdx = computedBufferIdx[i];
            MemoryCARWImpl dataBuf = dataBuffers.getQuick(bufIdx);
            MemoryCARWImpl auxBuf = auxBuffers.getQuick(bufIdx);
            int columnType = adjustedMetadata.getColumnType(i);
            addColumnData(columnData, dataBuf, auxBuf, columnType);
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
        final long frameRowCount = frame.getPartitionHi() - frame.getPartitionLo();
        populatePageFrameRecord(frame);
        materializeComputedColumns(frameRowCount);

        columnData.clear();
        for (int i = 0; i < outputColumnCount; i++) {
            final int baseColIdx = baseColumnMap[i];
            final int adjustedType = adjustedMetadata.getColumnType(i);

            if (baseColIdx >= 0) {
                // Pass-through column: read directly from page frame
                long localColTop = frame.getPageAddress(baseColIdx) > 0 ? 0 : frameRowCount;
                long pageAddress = frame.getPageAddress(baseColIdx);

                columnData.add(localColTop);
                columnData.add(pageAddress);
                columnData.add(frame.getPageSize(baseColIdx));
                if (ColumnType.isSymbol(adjustedType)) {
                    SymbolMapReader symbolMapReader = (SymbolMapReader) cursor.getSymbolTable(baseColIdx);
                    final MemoryR symbolValuesMem = symbolMapReader.getSymbolValuesColumn();
                    final MemoryR symbolOffsetsMem = symbolMapReader.getSymbolOffsetsColumn();
                    columnData.add(symbolValuesMem.addressOf(0));
                    columnData.add(symbolValuesMem.size());
                    columnData.add(symbolOffsetsMem.addressOf(HEADER_SIZE));
                    columnData.add(symbolMapReader.getSymbolCount());
                } else {
                    columnData.add(frame.getAuxPageAddress(baseColIdx));
                    columnData.add(frame.getAuxPageSize(baseColIdx));
                    columnData.add(0L);
                    columnData.add(0L);
                }
            } else {
                // Computed column: use materialization buffers
                int bufIdx = computedBufferIdx[i];
                MemoryCARWImpl dataBuf = dataBuffers.getQuick(bufIdx);
                MemoryCARWImpl auxBuf = auxBuffers.getQuick(bufIdx);
                addColumnData(columnData, dataBuf, auxBuf, adjustedType);
            }
        }
        return frameRowCount;
    }

    /**
     * Resets state for reuse: frees data/aux buffers and nulls references,
     * but retains {@code pfMemory}'s native allocations (DirectLongLists).
     * Use {@link #close()} to fully release all native memory including
     * {@code pfMemory}.
     */
    @Override
    public void clear() {
        releasePinnedBuffers();
        for (int i = 0, n = dataBuffers.size(); i < n; i++) {
            Misc.free(dataBuffers.getQuick(i));
        }
        dataBuffers.clear();
        for (int i = 0, n = auxBuffers.size(); i < n; i++) {
            Misc.free(auxBuffers.getQuick(i));
        }
        auxBuffers.clear();
        pfMemory.clear();
        baseColumnMap = null;
        computedBufferIdx = null;
        functions = null;
        functionRecord = null;
        adjustedMetadata = null;
        computedCount = 0;
        outputColumnCount = 0;
    }

    @Override
    public void close() {
        clear();
        Misc.free(pfMemory);
        pageFrameRecord.close();
    }

    public GenericRecordMetadata getAdjustedMetadata() {
        return adjustedMetadata;
    }

    public int[] getBaseColumnMap() {
        return baseColumnMap;
    }

    /**
     * Frees all pinned buffers that were being held for Rust's pending_partitions.
     * Call after a row group flush when all pinned partitions have been drained.
     */
    public void releasePinnedBuffers() {
        for (int i = 0, n = pinnedDataBuffers.size(); i < n; i++) {
            Misc.free(pinnedDataBuffers.getQuick(i));
        }
        pinnedDataBuffers.clear();
        for (int i = 0, n = pinnedAuxBuffers.size(); i < n; i++) {
            Misc.free(pinnedAuxBuffers.getQuick(i));
        }
        pinnedAuxBuffers.clear();
    }

    /**
     * Sets up for cursor-based export: all columns materialized from a RecordCursor
     * (no page frame backing).
     */
    public void setUp(RecordMetadata metadata) {
        this.outputColumnCount = metadata.getColumnCount();
        this.baseColumnMap = new int[outputColumnCount];
        this.computedBufferIdx = new int[outputColumnCount];
        this.functions = null;

        adjustedMetadata = new GenericRecordMetadata();
        computedCount = 0;

        for (int i = 0; i < outputColumnCount; i++) {
            int columnType = metadata.getColumnType(i);
            baseColumnMap[i] = -1; // all computed in full mode
            addComputedColumn(metadata, i, columnType);
        }

        int tsIdx = metadata.getTimestampIndex();
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
        this.outputColumnCount = outputMeta.getColumnCount();
        this.baseColumnMap = new int[outputColumnCount];
        this.computedBufferIdx = new int[outputColumnCount];
        this.adjustedMetadata = new GenericRecordMetadata();
        this.computedCount = 0;

        for (int i = 0; i < outputColumnCount; i++) {
            Function func = functions.getQuick(i);
            int baseColIdx = -1;

            if (func instanceof ColumnFunction cf) {
                int joinSpaceIdx = cf.getColumnIndex();
                // The column index in function space includes virtualColumnReservedSlots offset.
                // For base columns, the index is >= virtualColumnReservedSlots.
                int pmBaseIdx = priorityMetadata.getBaseColumnIndex(joinSpaceIdx);
                if (pmBaseIdx >= 0) {
                    baseColIdx = pmBaseIdx;
                }
            }

            int columnType = outputMeta.getColumnType(i);
            baseColumnMap[i] = baseColIdx;

            if (baseColIdx >= 0) {
                // Pass-through
                computedBufferIdx[i] = -1;
                if (ColumnType.isSymbol(columnType)) {
                    // SYMBOL columns require the extended constructor
                    adjustedMetadata.add(new TableColumnMetadata(
                            outputMeta.getColumnName(i),
                            columnType,
                            false,
                            0,
                            true,
                            null
                    ));
                } else {
                    adjustedMetadata.add(new TableColumnMetadata(
                            outputMeta.getColumnName(i),
                            columnType
                    ));
                }
            } else {
                assert ColumnType.tagOf(columnType) != ColumnType.BINARY;
                addComputedColumn(outputMeta, i, columnType);
            }
        }

        // Set timestamp index
        int tsIdx = outputMeta.getTimestampIndex();
        if (tsIdx >= 0) {
            adjustedMetadata.setTimestampIndex(tsIdx);
        }

        // Set up function evaluation record
        int virtualColumnReservedSlots = priorityMetadata.getVirtualColumnReservedSlots();
        functionRecord = new VirtualFunctionRecord(functions, virtualColumnReservedSlots);
        HybridSymbolTableSource hybridSource = new HybridSymbolTableSource(pfc, virtualColumnReservedSlots);
        pageFrameRecord.of(hybridSource);

        // Init functions with symbol table source
        Function.init(functions, hybridSource, ctx, null);
        functionRecord.of(pageFrameRecord);
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

    private void addComputedColumn(RecordMetadata metadata, int i, int columnType) {
        int adjustedType = columnType;
        if (ColumnType.isSymbol(columnType)) {
            adjustedType = ColumnType.STRING;
        }

        computedBufferIdx[i] = computedCount;
        allocateBuffer(adjustedType);
        computedCount++;
        adjustedMetadata.add(new TableColumnMetadata(metadata.getColumnName(i), adjustedType));
    }

    private void allocateBuffer(int columnType) {
        dataBuffers.add(new MemoryCARWImpl(DEFAULT_PAGE_SIZE, Integer.MAX_VALUE, MemoryTag.NATIVE_PARQUET_EXPORTER));
        auxBuffers.add(ColumnType.isVarSize(columnType)
                ? new MemoryCARWImpl(DEFAULT_PAGE_SIZE, Integer.MAX_VALUE, MemoryTag.NATIVE_PARQUET_EXPORTER)
                : null
        );
    }

    private void materializeComputedColumns(long frameRowCount) {
        resetBuffers();
        for (long row = 0; row < frameRowCount; row++) {
            pageFrameRecord.setRowIndex(row);
            for (int i = 0; i < outputColumnCount; i++) {
                if (baseColumnMap[i] >= 0) {
                    continue; // this is a pass-through column
                }
                int bufIdx = computedBufferIdx[i];
                MemoryCARWImpl dataBuf = dataBuffers.getQuick(bufIdx);
                MemoryCARWImpl auxBuf = auxBuffers.getQuick(bufIdx);
                int outputType = adjustedMetadata.getColumnType(i);
                Function func = functions.getQuick(i);
                int srcType = func.getType();
                if (ColumnType.isSymbol(srcType) && outputType == ColumnType.STRING) {
                    CharSequence sym = func.getSymbol(functionRecord.getInternalJoinRecord());
                    StringTypeDriver.appendValue(auxBuf, dataBuf, sym);
                } else {
                    writeComputedValue(func, functionRecord.getInternalJoinRecord(), outputType, dataBuf, auxBuf);
                }
            }
        }
    }

    private void populatePageFrameRecord(PageFrame frame) {
        pfMemory.of(frame);
        pageFrameRecord.init(pfMemory);
        pageFrameRecord.setRowIndex(0);
    }

    private void resetBuffers() {
        // Check if any buffer has data that Rust may still reference.
        boolean hasData = false;
        for (int i = 0; i < computedCount; i++) {
            if (dataBuffers.getQuick(i).getAppendOffset() > 0) {
                hasData = true;
                break;
            }
        }

        if (hasData) {
            // Pin current buffers (Rust still references them via pending_partitions)
            // and allocate fresh ones for the next frame.
            for (int i = 0; i < computedCount; i++) {
                pinnedDataBuffers.add(dataBuffers.getQuick(i));
                pinnedAuxBuffers.add(auxBuffers.getQuick(i));
            }
            dataBuffers.clear();
            auxBuffers.clear();
            computedCount = 0;
            for (int i = 0; i < outputColumnCount; i++) {
                if (computedBufferIdx[i] >= 0) {
                    computedBufferIdx[i] = computedCount;
                    allocateBuffer(adjustedMetadata.getColumnType(i));
                    computedCount++;
                }
            }
        } else {
            // First call or empty buffers: truncate in place.
            for (int i = 0; i < computedCount; i++) {
                dataBuffers.getQuick(i).truncate();
                MemoryCARWImpl auxBuf = auxBuffers.getQuick(i);
                if (auxBuf != null) {
                    auxBuf.truncate();
                }
            }
        }

        // Write the zero offset of the first string in a STRING data column.
        // StringTypeDriver will then append the offset of the first available byte
        // after each written string (start offset of the next, yet unwritten string).
        for (int i = 0; i < outputColumnCount; i++) {
            int bufIdx = computedBufferIdx[i];
            if (bufIdx >= 0 && ColumnType.tagOf(adjustedMetadata.getColumnType(i)) == ColumnType.STRING) {
                auxBuffers.getQuick(bufIdx).putLong(0);
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
            case ColumnType.VARCHAR -> VarcharTypeDriver.appendValue(auxBuf, dataBuf, record.getVarcharA(col));
            case ColumnType.SYMBOL -> {
                // Symbols get converted to STRING in adjusted metadata
                CharSequence sym = record.getSymA(col);
                StringTypeDriver.appendValue(auxBuf, dataBuf, sym);
            }
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
            default ->
                    throw new UnsupportedOperationException("unsupported column type: " + ColumnType.nameOf(outputType));
        }
    }

    /**
     * Routes symbol table lookups: virtual columns → null, base columns → page frame cursor.
     */
    private record HybridSymbolTableSource(
            PageFrameCursor pageFrameCursor,
            int virtualColumnReservedSlots
    ) implements SymbolTableSource {

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
    }

    /**
     * Reusable PageFrameMemory backed by DirectLongLists. Updated in-place per frame
     * to avoid allocating a new object on every call (zero-GC on data path).
     */
    private static class ReusablePageFrameMemory implements PageFrameMemory, Mutable, QuietCloseable {
        private final DirectLongList auxPageAddresses = new DirectLongList(32, MemoryTag.NATIVE_PARQUET_EXPORTER);
        private final DirectLongList auxPageSizes = new DirectLongList(32, MemoryTag.NATIVE_PARQUET_EXPORTER);
        private final DirectLongList pageAddresses = new DirectLongList(32, MemoryTag.NATIVE_PARQUET_EXPORTER);
        private final DirectLongList pageSizes = new DirectLongList(32, MemoryTag.NATIVE_PARQUET_EXPORTER);
        private int columnCount;
        private boolean hasColumnTops;
        private long rowIdOffset;

        @Override
        public void clear() {
            pageAddresses.clear();
            auxPageAddresses.clear();
            pageSizes.clear();
            auxPageSizes.clear();
        }

        @Override
        public void close() {
            Misc.free(pageAddresses);
            Misc.free(auxPageAddresses);
            Misc.free(pageSizes);
            Misc.free(auxPageSizes);
        }

        @Override
        public long getAuxPageAddress(int columnIndex) {
            return auxPageAddresses.get(columnIndex);
        }

        @Override
        public DirectLongList getAuxPageAddresses() {
            return auxPageAddresses;
        }

        @Override
        public DirectLongList getAuxPageSizes() {
            return auxPageSizes;
        }

        @Override
        public int getColumnCount() {
            return columnCount;
        }

        @Override
        public int getColumnOffset() {
            return 0;
        }

        @Override
        public byte getFrameFormat() {
            return PartitionFormat.NATIVE;
        }

        @Override
        public int getFrameIndex() {
            return 0;
        }

        @Override
        public long getPageAddress(int columnIndex) {
            return pageAddresses.get(columnIndex);
        }

        @Override
        public DirectLongList getPageAddresses() {
            return pageAddresses;
        }

        @Override
        public long getPageSize(int columnIndex) {
            return pageSizes.get(columnIndex);
        }

        @Override
        public DirectLongList getPageSizes() {
            return pageSizes;
        }

        @Override
        public long getRowIdOffset() {
            return rowIdOffset;
        }

        @Override
        public boolean hasColumnTops() {
            return hasColumnTops;
        }

        public void of(PageFrame frame) {
            this.columnCount = frame.getColumnCount();
            this.rowIdOffset = frame.getPartitionLo();

            pageAddresses.clear();
            auxPageAddresses.clear();
            pageSizes.clear();
            auxPageSizes.clear();

            hasColumnTops = false;
            for (int col = 0; col < columnCount; col++) {
                long addr = frame.getPageAddress(col);
                pageAddresses.add(addr);
                pageSizes.add(frame.getPageSize(col));
                auxPageAddresses.add(frame.getAuxPageAddress(col));
                auxPageSizes.add(frame.getAuxPageSize(col));
                if (addr == 0) {
                    hasColumnTops = true;
                }
            }
        }

        @Override
        public boolean populateRemainingColumns(IntHashSet filterColumnIndexes, DirectLongList filteredRows, boolean fillWithNulls) {
            return false;
        }
    }
}
