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

package io.questdb.griffin.engine.functions.table;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.VarcharTypeDriver;
import io.questdb.cairo.sql.ColumnMapping;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.vm.MemoryCARWImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.table.ParquetRowGroupFilter;
import io.questdb.griffin.engine.table.PushdownFilterExtractor;
import io.questdb.griffin.engine.table.parquet.ParquetFileDecoder;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.BoolList;
import io.questdb.std.Chars;
import io.questdb.std.DirectLongList;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.datetime.millitime.DateFormatUtils;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8s;
import org.jetbrains.annotations.Nullable;

import static io.questdb.griffin.engine.functions.table.ReadParquetRecordCursor.canProjectMetadata;

/**
 * Page frame cursor for parallel read_parquet() over a hive-partitioned glob.
 * <p>
 * Walks the underlying glob cursor lazily, opening one {@link ParquetFileDecoder}
 * per matched file as it advances. Each emitted frame represents one row group of
 * one file; partition column values are materialised into per-file native buffers
 * and surfaced via the virtual page address hook on {@link PageFrame}.
 * <p>
 * Decoders are released through {@link #releaseOpenPartitions()} once the cursor
 * has moved past their file, so in-flight worker frames stay valid while older
 * files retire.
 * <p>
 * All inferred partition column types (INT, LONG, DATE, TIMESTAMP, DOUBLE,
 * VARCHAR) are supported. VARCHAR partition values are surfaced through
 * hand-encoded aux+data pages built in {@link #fillVarcharPartitionBuffer}.
 */
public class HivePartitionedReadParquetPageFrameCursor implements PageFrameCursor {
    private static final Log LOG = LogFactory.getLog(HivePartitionedReadParquetPageFrameCursor.class);
    // VARCHAR slice aux entry layout, mirrored from VarcharTypeDriver so the
    // partition virtual buffers can be hand-encoded. Each aux entry is 16 bytes:
    //   bytes 0-3: header = (size << 4) | (ascii ? 2 : 0) | (null ? 4 : 0)
    //   bytes 4-7: reserved (zero)
    //   bytes 8-15: absolute pointer to value bytes
    private static final int VARCHAR_AUX_ENTRY_BYTES = 16;
    private static final int VARCHAR_HEADER_FLAG_ASCII = 2;
    private final ColumnMapping columnMapping = new ColumnMapping();
    private final ObjList<ParquetFileDecoder> decoders = new ObjList<>();
    private final LongList fds = new LongList();
    private final FilesFacade ff;
    private final LongList fileAddrs = new LongList();
    private final LongList fileSizes = new LongList();
    private final LongList filterBufEnds = new LongList();
    private final ObjList<DirectLongList> filterLists = new ObjList<>();
    private final BoolList filterPrepared = new BoolList();
    private final ObjList<MemoryCARWImpl> filterValues = new ObjList<>();
    private final HivePartitionedPageFrame frame = new HivePartitionedPageFrame();
    private final RecordCursor globCursor;
    // Safety net for runaway-glob scenarios. The cursor cannot proactively close
    // older files because their frames may still be in flight on worker threads;
    // we rely on the consumer's releaseOpenPartitions calls to bound memory. If
    // a consumer never calls release (or it's heavily delayed), a glob over many
    // thousands of files could exhaust fds / address space. Fail fast when the
    // in-flight file count crosses this threshold. Configured via
    // cairo.sql.parquet.hive.max.open.files (default 4096).
    private final int maxConcurrentOpenFiles;
    private final int nonGlobRootLen;
    private final int parquetColumnCount;
    private final RecordMetadata parquetMetadata;
    // Per-file partition buffers, flat-indexed by fileIndex * partitionColumnCount + partitionCol.
    // For fixed-size types, partitionBufferAddrs/Sizes is the only buffer; aux is unused (0).
    // For VARCHAR, partitionBufferAddrs/Sizes is the data page and partitionAuxBufferAddrs/Sizes
    // is the aux page.
    private final LongList partitionAuxBufferAddrs = new LongList();
    private final LongList partitionAuxBufferSizes = new LongList();
    private final LongList partitionBufferAddrs = new LongList();
    private final LongList partitionBufferSizes = new LongList();
    private final IntList partitionBufferTypeWidths = new IntList();
    private final int partitionColumnCount;
    private final ObjList<String> partitionColumnNames;
    private final IntList partitionColumnTypes;
    private final @Nullable ObjList<PushdownFilterExtractor.PushdownFilterCondition> pushdownFilterConditions;
    private int currentFileIndex = -1;
    private long cumulativePartitionLo = 0;
    private SqlExecutionContext executionContext;
    // If releaseOpenPartitions retired any decoder, cheap toTop reuse is unsafe
    // (a released decoder is null in the list and we'd need to reopen — orders
    // of complexity beyond the simple advance-through-list path).
    private boolean isAnyFileReleased = false;
    private boolean isFilterConditionsInitialised = false;
    // Row counts are accumulated as files open during normal iteration so that
    // size() after a full scan does not need a separate probe walk.
    private boolean isTotalRowCountFinalised = false;
    private int lowestOpenFileIndex = 0;
    // Lazy: freed in close, re-allocated on next openNextFile so the cursor can
    // survive close-reopen cycles via the factory's caching pattern.
    private Path path;
    // Scratch arrays for per-file partition value parsing during file pruning and
    // VARCHAR buffer fill. Sized to partitionColumnCount; reused across files.
    private int[] prunePartitionByteHi;
    private int[] prunePartitionByteLo;
    private boolean[] prunePartitionPresent;
    private long[] prunePartitionValues;
    private long runningRowCount = 0;
    private long totalRowCount = -1;

    public HivePartitionedReadParquetPageFrameCursor(
            FilesFacade ff,
            RecordCursor globCursor,
            RecordMetadata parquetMetadata,
            int parquetColumnCount,
            ObjList<String> partitionColumnNames,
            IntList partitionColumnTypes,
            int nonGlobRootByteLen,
            int maxConcurrentOpenFiles,
            @Nullable ObjList<PushdownFilterExtractor.PushdownFilterCondition> pushdownFilterConditions
    ) {
        this.ff = ff;
        this.globCursor = globCursor;
        this.parquetMetadata = parquetMetadata;
        this.parquetColumnCount = parquetColumnCount;
        this.partitionColumnNames = partitionColumnNames;
        this.partitionColumnTypes = partitionColumnTypes;
        this.partitionColumnCount = partitionColumnNames.size();
        this.nonGlobRootLen = nonGlobRootByteLen;
        this.maxConcurrentOpenFiles = maxConcurrentOpenFiles;
        this.pushdownFilterConditions = pushdownFilterConditions;
        this.prunePartitionValues = new long[this.partitionColumnCount];
        this.prunePartitionPresent = new boolean[this.partitionColumnCount];
        this.prunePartitionByteLo = new int[this.partitionColumnCount];
        this.prunePartitionByteHi = new int[this.partitionColumnCount];
    }

    @Override
    public void calculateSize(RecordCursor.Counter counter) {
        // Eager total: walk metadata of every remaining file. Used by limit/skip planning.
        if (totalRowCount < 0) {
            computeTotalRowCount();
        }
        counter.add(totalRowCount - cumulativePartitionLo);
    }

    @Override
    public void close() {
        closeAllOpenFiles();
        // Do not free globCursor here: the factory reuses this cursor across
        // getCursor() calls, and the next of() will call globCursor.toTop() on it.
        // The globCursor's owner is the underlying globCursorFactory and gets
        // freed when that factory is closed.
        path = Misc.free(path);
        // The factory caches this cursor and re-calls of(ctx) on a fresh getCursor.
        // After close the globCursor handle is gone and decoder slots are nulled, so
        // any cheap-toTop reuse must fall back to a full reset. Clearing the lists
        // makes toTop's `decoders.size() > 0` reuse condition fail naturally; resetting
        // isTotalRowCountFinalised stops size() from returning a value backed by freed
        // state.
        decoders.clear();
        fds.clear();
        fileAddrs.clear();
        fileSizes.clear();
        filterLists.clear();
        filterValues.clear();
        filterBufEnds.clear();
        filterPrepared.clear();
        partitionBufferAddrs.clear();
        partitionBufferSizes.clear();
        partitionBufferTypeWidths.clear();
        partitionAuxBufferAddrs.clear();
        partitionAuxBufferSizes.clear();
        isTotalRowCountFinalised = false;
        runningRowCount = 0;
        totalRowCount = -1;
        isFilterConditionsInitialised = false;
        isAnyFileReleased = false;
        currentFileIndex = -1;
        lowestOpenFileIndex = 0;
        cumulativePartitionLo = 0;
    }

    @Override
    public ColumnMapping getColumnMapping() {
        return columnMapping;
    }

    @Override
    public long getRemainingRowsInInterval() {
        return 0;
    }

    @Override
    public StaticSymbolTable getSymbolTable(int columnIndex) {
        return null;
    }

    @Override
    public boolean isExternal() {
        return true;
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        return null;
    }

    @Override
    public @Nullable PageFrame next(long skipTarget) {
        while (true) {
            if (currentFileIndex >= 0) {
                ParquetFileDecoder decoder = decoders.getQuick(currentFileIndex);
                int rgCount = decoder.metadata().getRowGroupCount();
                while (++frame.rowGroupIndex < rgCount) {
                    if (filterPrepared.get(currentFileIndex) && ParquetRowGroupFilter.canSkipRowGroup(
                            frame.rowGroupIndex,
                            decoder,
                            filterLists.getQuick(currentFileIndex),
                            filterBufEnds.getQuick(currentFileIndex)
                    )) {
                        cumulativePartitionLo += decoder.metadata().getRowGroupSize(frame.rowGroupIndex);
                        continue;
                    }
                    final int rgSize = decoder.metadata().getRowGroupSize(frame.rowGroupIndex);
                    frame.partitionIndex = currentFileIndex;
                    frame.rowGroupSize = rgSize;
                    frame.partitionLo = cumulativePartitionLo;
                    cumulativePartitionLo += rgSize;
                    frame.partitionHi = cumulativePartitionLo;
                    frame.decoder = decoder;
                    return frame;
                }
            }
            if (!moveToNextFile()) {
                // Cursor exhausted: every file has now passed through openNextFile, so the
                // running total is the authoritative row count. Cache it and skip the
                // re-walk that computeTotalRowCount would otherwise do for a later size().
                if (!isTotalRowCountFinalised) {
                    totalRowCount = runningRowCount;
                    isTotalRowCountFinalised = true;
                }
                return null;
            }
        }
    }

    public void of(SqlExecutionContext executionContext) {
        this.executionContext = executionContext;
        toTop();
    }

    @Override
    public void releaseOpenPartitions() {
        // Close every file strictly below the currently-yielded one; their frames
        // are guaranteed to have been consumed before the cursor advanced.
        if (lowestOpenFileIndex < currentFileIndex) {
            isAnyFileReleased = true;
        }
        for (int i = lowestOpenFileIndex; i < currentFileIndex; i++) {
            closeFile(i);
        }
        lowestOpenFileIndex = currentFileIndex;
    }

    @Override
    public long size() {
        if (totalRowCount < 0) {
            computeTotalRowCount();
        }
        return totalRowCount;
    }

    @Override
    public boolean supportsSizeCalculation() {
        return true;
    }

    @Override
    public void toTop() {
        // Cheap path: the previous pass walked every file and no decoder was
        // retired by releaseOpenPartitions. The whole pool of open decoders +
        // filter prep + partition buffers is still valid — just rewind the
        // iteration cursors. moveToNextFile will walk the existing decoders
        // list instead of pulling fresh paths from the glob cursor. close()
        // resets the flag/list so a close-reopen cycle takes the full path.
        if (isTotalRowCountFinalised && !isAnyFileReleased && decoders.size() > 0) {
            currentFileIndex = -1;
            cumulativePartitionLo = 0;
            frame.rowGroupIndex = -1;
            return;
        }
        // Full reset: tear down and re-open from the glob.
        closeAllOpenFiles();
        decoders.clear();
        fds.clear();
        fileAddrs.clear();
        fileSizes.clear();
        filterLists.clear();
        filterValues.clear();
        filterBufEnds.clear();
        filterPrepared.clear();
        partitionBufferAddrs.clear();
        partitionBufferSizes.clear();
        partitionBufferTypeWidths.clear();
        partitionAuxBufferAddrs.clear();
        partitionAuxBufferSizes.clear();
        globCursor.toTop();
        currentFileIndex = -1;
        lowestOpenFileIndex = 0;
        cumulativePartitionLo = 0;
        isFilterConditionsInitialised = false;
        isAnyFileReleased = false;
        // Preserve the finalised row count across toTop calls: file row counts are
        // immutable, so once we've seen them once we can keep reusing the total.
        if (!isTotalRowCountFinalised) {
            totalRowCount = -1;
            runningRowCount = 0;
        }
        frame.rowGroupIndex = -1;
        buildColumnMapping();
    }

    private void allocatePartitionBuffersForFile(int fileIndex, Utf8Sequence filePath, ParquetFileDecoder decoder) {
        // Reserve slots in the flat per-file × per-col arrays.
        for (int c = 0; c < partitionColumnCount; c++) {
            partitionBufferAddrs.add(0);
            partitionBufferSizes.add(0);
            partitionBufferTypeWidths.add(0);
            partitionAuxBufferAddrs.add(0);
            partitionAuxBufferSizes.add(0);
        }
        if (partitionColumnCount == 0) {
            return;
        }
        final int rgCount = decoder.metadata().getRowGroupCount();
        int maxRowGroupSize = 0;
        for (int i = 0; i < rgCount; i++) {
            int rgSize = decoder.metadata().getRowGroupSize(i);
            if (rgSize > maxRowGroupSize) {
                maxRowGroupSize = rgSize;
            }
        }
        if (maxRowGroupSize == 0) {
            return;
        }
        // openNextFile parses partition values into prunePartition* before calling us,
        // so the byte ranges below come for free.
        for (int c = 0; c < partitionColumnCount; c++) {
            int type = partitionColumnTypes.getQuick(c);
            int slot = fileIndex * partitionColumnCount + c;
            if (ColumnType.tagOf(type) == ColumnType.VARCHAR) {
                final long auxSize = (long) maxRowGroupSize * VARCHAR_AUX_ENTRY_BYTES;
                final long auxAddr = Unsafe.malloc(auxSize, MemoryTag.NATIVE_DEFAULT);
                // Slice format: the aux entry holds an absolute pointer into the data
                // buffer plus a length. We allocate enough room for the value bytes
                // once (every aux entry will point at the same start). Null/empty
                // values get a 1-byte allocation so the data pointer is non-zero —
                // the virtual-page overlay only fires when the data address is non-zero.
                int valueBytes = 0;
                if (prunePartitionPresent[c]) {
                    valueBytes = prunePartitionByteHi[c] - prunePartitionByteLo[c];
                }
                final long dataSize = Math.max(valueBytes, 1);
                final long dataAddr = Unsafe.malloc(dataSize, MemoryTag.NATIVE_DEFAULT);
                partitionAuxBufferAddrs.setQuick(slot, auxAddr);
                partitionAuxBufferSizes.setQuick(slot, auxSize);
                partitionBufferAddrs.setQuick(slot, dataAddr);
                partitionBufferSizes.setQuick(slot, dataSize);
                partitionBufferTypeWidths.setQuick(slot, VARCHAR_AUX_ENTRY_BYTES);
            } else {
                int typeWidth = ColumnType.sizeOf(type);
                long bufferSize = (long) maxRowGroupSize * typeWidth;
                long bufferAddr = Unsafe.malloc(bufferSize, MemoryTag.NATIVE_DEFAULT);
                partitionBufferAddrs.setQuick(slot, bufferAddr);
                partitionBufferSizes.setQuick(slot, bufferSize);
                partitionBufferTypeWidths.setQuick(slot, typeWidth);
            }
        }
        // Populate buffers by parsing partition values from the file path.
        populatePartitionBuffersFromPath(fileIndex, filePath, maxRowGroupSize);
    }

    private void buildColumnMapping() {
        columnMapping.clear();
        // Parquet columns: writer index is the parquet position (external files have no field IDs).
        for (int i = 0; i < parquetColumnCount; i++) {
            columnMapping.addColumn(i, i);
        }
        // Partition virtual columns: writer index is a sentinel that won't match any
        // parquet column, so openParquet skips them and the virtual page overlay fills
        // their slots instead.
        for (int i = 0; i < partitionColumnCount; i++) {
            columnMapping.addColumn(parquetColumnCount + i, Integer.MAX_VALUE - i);
        }
    }

    private boolean canCompareTyped(int columnType) {
        // DOUBLE is intentionally excluded: equality on doubles is fraught (NaN
        // never compares equal, and decimal literals don't round-trip exactly
        // through Double). A spuriously-pruned file is a correctness bug; better
        // to let the row-level filter handle DOUBLE conditions.
        switch (ColumnType.tagOf(columnType)) {
            case ColumnType.INT:
            case ColumnType.LONG:
            case ColumnType.DATE:
            case ColumnType.TIMESTAMP:
                return true;
            default:
                return false;
        }
    }

    private void closeAllOpenFiles() {
        for (int i = lowestOpenFileIndex, n = decoders.size(); i < n; i++) {
            closeFile(i);
        }
        lowestOpenFileIndex = 0;
    }

    private void closeFile(int fileIndex) {
        if (fileIndex < 0 || fileIndex >= decoders.size()) {
            return;
        }
        ParquetFileDecoder decoder = decoders.getQuick(fileIndex);
        if (decoder != null) {
            Misc.free(decoder);
            decoders.setQuick(fileIndex, null);
        }
        long fd = fds.getQuick(fileIndex);
        if (fd != -1) {
            ff.close(fd);
            fds.setQuick(fileIndex, -1);
        }
        long addr = fileAddrs.getQuick(fileIndex);
        long size = fileSizes.getQuick(fileIndex);
        if (addr != 0) {
            ff.munmap(addr, size, MemoryTag.MMAP_PARQUET_PARTITION_DECODER);
            fileAddrs.setQuick(fileIndex, 0);
            fileSizes.setQuick(fileIndex, 0);
        }
        DirectLongList fl = filterLists.getQuick(fileIndex);
        if (fl != null) {
            Misc.free(fl);
            filterLists.setQuick(fileIndex, null);
        }
        MemoryCARWImpl fv = filterValues.getQuick(fileIndex);
        if (fv != null) {
            Misc.free(fv);
            filterValues.setQuick(fileIndex, null);
        }
        for (int c = 0; c < partitionColumnCount; c++) {
            int slot = fileIndex * partitionColumnCount + c;
            long pAddr = partitionBufferAddrs.getQuick(slot);
            long pSize = partitionBufferSizes.getQuick(slot);
            if (pAddr != 0) {
                Unsafe.free(pAddr, pSize, MemoryTag.NATIVE_DEFAULT);
                partitionBufferAddrs.setQuick(slot, 0);
                partitionBufferSizes.setQuick(slot, 0);
            }
            long pAuxAddr = partitionAuxBufferAddrs.getQuick(slot);
            long pAuxSize = partitionAuxBufferSizes.getQuick(slot);
            if (pAuxAddr != 0) {
                Unsafe.free(pAuxAddr, pAuxSize, MemoryTag.NATIVE_DEFAULT);
                partitionAuxBufferAddrs.setQuick(slot, 0);
                partitionAuxBufferSizes.setQuick(slot, 0);
            }
        }
    }

    private void computeTotalRowCount() {
        // Walk the glob cursor in metadata-only mode to total up rows. Reset on the way
        // out so the cursor's main iteration starts fresh.
        long total = 0;
        globCursor.toTop();
        try (Path tempPath = new Path(MemoryTag.NATIVE_PATH);
             ParquetFileDecoder probe = new ParquetFileDecoder()) {
            while (globCursor.hasNext()) {
                Utf8Sequence filePath = globCursor.getRecord().getVarcharA(0);
                tempPath.of(filePath);
                final long fd = TableUtils.openRO(ff, tempPath.$(), LOG);
                long addr = 0;
                long size = 0;
                try {
                    size = ff.length(fd);
                    addr = TableUtils.mapRO(ff, fd, size, MemoryTag.MMAP_PARQUET_PARTITION_DECODER);
                    probe.of(addr, size, MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
                    total += probe.metadata().getRowCount();
                } finally {
                    ff.close(fd);
                    if (addr != 0) {
                        ff.munmap(addr, size, MemoryTag.MMAP_PARQUET_PARTITION_DECODER);
                    }
                }
            }
        }
        totalRowCount = total;
        globCursor.toTop();
    }

    private void ensureFilterConditionsInitialised() {
        if (isFilterConditionsInitialised || pushdownFilterConditions == null) {
            return;
        }
        try {
            for (int i = 0, n = pushdownFilterConditions.size(); i < n; i++) {
                pushdownFilterConditions.getQuick(i).init(executionContext);
            }
        } catch (SqlException e) {
            throw CairoException.nonCritical().put("failed to init pushdown filter: ").put(e.getFlyweightMessage());
        }
        isFilterConditionsInitialised = true;
    }

    private void fillPartitionBuffer(long addr, int rowCount, int columnType, long longValue, boolean present) {
        if (!present) {
            switch (ColumnType.tagOf(columnType)) {
                case ColumnType.INT:
                    for (int i = 0; i < rowCount; i++) {
                        Unsafe.getUnsafe().putInt(addr + i * 4L, Numbers.INT_NULL);
                    }
                    break;
                case ColumnType.LONG:
                case ColumnType.DATE:
                case ColumnType.TIMESTAMP:
                    for (int i = 0; i < rowCount; i++) {
                        Unsafe.getUnsafe().putLong(addr + i * 8L, Numbers.LONG_NULL);
                    }
                    break;
                case ColumnType.DOUBLE:
                    final long nanBits = Double.doubleToRawLongBits(Double.NaN);
                    for (int i = 0; i < rowCount; i++) {
                        Unsafe.getUnsafe().putLong(addr + i * 8L, nanBits);
                    }
                    break;
                default:
                    throw CairoException.nonCritical().put("unsupported partition column type for parallel hive read [type=").put(ColumnType.nameOf(columnType)).put(']');
            }
            return;
        }
        switch (ColumnType.tagOf(columnType)) {
            case ColumnType.INT:
                final int intVal = (int) longValue;
                for (int i = 0; i < rowCount; i++) {
                    Unsafe.getUnsafe().putInt(addr + i * 4L, intVal);
                }
                break;
            case ColumnType.LONG:
            case ColumnType.DATE:
            case ColumnType.TIMESTAMP:
            case ColumnType.DOUBLE:
                for (int i = 0; i < rowCount; i++) {
                    Unsafe.getUnsafe().putLong(addr + i * 8L, longValue);
                }
                break;
            default:
                throw CairoException.nonCritical().put("unsupported partition column type for parallel hive read [type=").put(ColumnType.nameOf(columnType)).put(']');
        }
    }

    /**
     * Encodes the VARCHAR_SLICE aux+data pages for a constant partition value
     * across {@code rowCount} rows. PARQUET-format frames read VARCHAR columns
     * through {@link VarcharTypeDriver#getSliceValue}, which expects the slice
     * layout (not the full append-value layout that table partitions use):
     * <pre>
     *   bytes 0-3: header = (size &lt;&lt; 4) | (ascii ? 2 : 0) | (null ? 4 : 0)
     *   bytes 4-7: reserved (zero)
     *   bytes 8-15: absolute pointer to value bytes
     * </pre>
     * All aux entries are identical and point to the same start of
     * {@code dataAddr}; the value bytes are written once.
     */
    private void fillVarcharPartitionBuffer(
            long auxAddr,
            long dataAddr,
            int rowCount,
            @Nullable Utf8Sequence value,
            int valueLo,
            int valueHi
    ) {
        final long entryHi;
        final long entryLo;
        if (value == null) {
            entryLo = VarcharTypeDriver.VARCHAR_HEADER_FLAG_NULL & 0xFFFFFFFFL;
            entryHi = 0L;
        } else {
            final int valueSize = valueHi - valueLo;
            boolean ascii = true;
            for (int i = valueLo; i < valueHi; i++) {
                if (value.byteAt(i) < 0) {
                    ascii = false;
                    break;
                }
            }
            // Write value bytes once at the start of the data buffer; every aux
            // entry will point here.
            for (int i = 0; i < valueSize; i++) {
                Unsafe.getUnsafe().putByte(dataAddr + i, value.byteAt(valueLo + i));
            }
            final int header = (valueSize << 4) | (ascii ? VARCHAR_HEADER_FLAG_ASCII : 0);
            // bytes 0-3: header, bytes 4-7: reserved (zero)
            entryLo = ((long) header) & 0xFFFFFFFFL;
            entryHi = dataAddr;
        }
        for (int i = 0; i < rowCount; i++) {
            final long entryAddr = auxAddr + (long) i * VARCHAR_AUX_ENTRY_BYTES;
            Unsafe.getUnsafe().putLong(entryAddr, entryLo);
            Unsafe.getUnsafe().putLong(entryAddr + 8, entryHi);
        }
    }

    /**
     * Compares one value-function from a pushdown condition against the typed
     * partition value. Returns true if the function's value matches the
     * partition value under the partition column's type.
     * <p>
     * Returns false (i.e. keep the file) if the function's typed accessor
     * raises a known evaluation error - the row-level filter will still
     * exercise the condition. Errors of types we don't expect (e.g. OOM,
     * assertion failures) propagate so they're not silently swallowed.
     */
    private boolean functionMatchesPartition(Function f, int partitionType, long partitionValue) {
        try {
            switch (ColumnType.tagOf(partitionType)) {
                case ColumnType.INT:
                    return f.getInt(null) == (int) partitionValue;
                case ColumnType.LONG:
                    return f.getLong(null) == partitionValue;
                case ColumnType.DATE:
                    return f.getDate(null) == partitionValue;
                case ColumnType.TIMESTAMP:
                    return f.getTimestamp(null) == partitionValue;
                default:
                    return false;
            }
        } catch (CairoException | NumericException ignored) {
            return false;
        }
    }

    private int indexOfPartitionColumn(CharSequence name) {
        for (int i = 0; i < partitionColumnCount; i++) {
            if (Chars.equalsIgnoreCase(partitionColumnNames.getQuick(i), name)) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Returns true if pushdown filter conditions referencing partition columns
     * rule out every row of the current file, based on the partition values
     * already parsed into {@code prunePartition*}. Conservative: any unsupported
     * operator yields false (file kept). Conditions referencing parquet columns
     * are ignored here; row-group statistics handle those once the file is open.
     */
    private boolean isPrunedByParsedPartition() {
        if (pushdownFilterConditions == null || pushdownFilterConditions.size() == 0) {
            return false;
        }
        ensureFilterConditionsInitialised();

        for (int i = 0, n = pushdownFilterConditions.size(); i < n; i++) {
            PushdownFilterExtractor.PushdownFilterCondition cond = pushdownFilterConditions.getQuick(i);
            int partCol = indexOfPartitionColumn(cond.getColumnName());
            if (partCol < 0) {
                continue;
            }
            int op = cond.getOperationType();
            boolean present = prunePartitionPresent[partCol];

            if (op == PushdownFilterExtractor.OP_IS_NULL) {
                if (present) return true;
                continue;
            }
            if (op == PushdownFilterExtractor.OP_IS_NOT_NULL) {
                if (!present) return true;
                continue;
            }
            if (op != PushdownFilterExtractor.OP_EQ) {
                // Other ops (LT, GT, BETWEEN, NEQ) - skip conservatively. Could be added
                // later but the IN/=/IS NULL cases cover the common hive query shape.
                continue;
            }
            if (!present) {
                // OP_EQ against a NULL partition value never matches.
                return true;
            }
            final int type = partitionColumnTypes.getQuick(partCol);
            if (!canCompareTyped(type)) {
                continue;
            }
            final ObjList<Function> vals = cond.getValueFunctions();
            if (vals.size() == 0) {
                continue;
            }
            boolean anyMatch = false;
            for (int v = 0, vn = vals.size(); v < vn; v++) {
                if (functionMatchesPartition(vals.getQuick(v), type, prunePartitionValues[partCol])) {
                    anyMatch = true;
                    break;
                }
            }
            if (!anyMatch) {
                return true;
            }
        }
        return false;
    }

    /**
     * Advance to the next file. If a previous pass has already populated the decoder
     * pool (cheap toTop reuse), advance to the next live decoder in the list. Only
     * fall through to {@link #openNextFile()} when no pre-opened file remains, which
     * also handles the fresh-pass case where the list is empty.
     */
    private boolean moveToNextFile() {
        int next = currentFileIndex + 1;
        if (next < decoders.size() && decoders.getQuick(next) != null) {
            currentFileIndex = next;
            frame.rowGroupIndex = -1;
            return true;
        }
        return openNextFile();
    }

    private boolean openNextFile() {
        // Safety net against runaway globs where the consumer never calls
        // releaseOpenPartitions: bound the in-flight open file count.
        if (decoders.size() - lowestOpenFileIndex >= maxConcurrentOpenFiles) {
            throw CairoException.nonCritical()
                    .put("hive glob has too many concurrently open files (")
                    .put(decoders.size() - lowestOpenFileIndex)
                    .put("); narrow the glob, reduce the page frame reduce queue capacity, ")
                    .put("or raise cairo.sql.parquet.hive.max.open.files");
        }
        // Cheap pre-screen: parse partition values from the path and check if any
        // pushdown filter condition rejects them. If so, skip the file entirely
        // without opening an fd or mmaping the parquet content. Parsing once here
        // also primes the prunePartition* scratch arrays so allocatePartitionBuffersForFile
        // and the buffer fill below can reuse the result without re-walking the path.
        Utf8Sequence filePath;
        boolean isPruned;
        do {
            if (!globCursor.hasNext()) {
                return false;
            }
            filePath = globCursor.getRecord().getVarcharA(0);
            if (partitionColumnCount > 0) {
                parsePartitionValues(filePath, prunePartitionValues, prunePartitionPresent);
                isPruned = isPrunedByParsedPartition();
            } else {
                isPruned = false;
            }
        } while (isPruned);
        final int fileIndex = ++currentFileIndex;
        if (path == null) {
            path = new Path(MemoryTag.NATIVE_PATH);
        }
        path.of(filePath);
        final long fd = TableUtils.openRO(ff, path.$(), LOG);
        long addr = 0;
        long size = 0;
        ParquetFileDecoder decoder = null;
        try {
            size = ff.length(fd);
            addr = TableUtils.mapRO(ff, fd, size, MemoryTag.MMAP_PARQUET_PARTITION_DECODER);
            decoder = new ParquetFileDecoder();
            decoder.of(addr, size, MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
            if (parquetColumnCount > 0 && !canProjectMetadata(parquetMetadata, decoder, null, null)) {
                throw CairoException.nonCritical()
                        .put("parquet schema mismatch: file '")
                        .put(path)
                        .put("' is incompatible with the schema of the first matched file");
            }
        } catch (Throwable th) {
            if (addr != 0) {
                ff.munmap(addr, size, MemoryTag.MMAP_PARQUET_PARTITION_DECODER);
            }
            ff.close(fd);
            Misc.free(decoder);
            throw th;
        }
        decoders.add(decoder);
        fds.add(fd);
        fileAddrs.add(addr);
        fileSizes.add(size);
        filterLists.add(null);
        filterValues.add(null);
        filterBufEnds.add(0);
        filterPrepared.add(false);

        prepareFilterListForFile(fileIndex, decoder);
        allocatePartitionBuffersForFile(fileIndex, filePath, decoder);

        if (!isTotalRowCountFinalised) {
            runningRowCount += decoder.metadata().getRowCount();
        }

        frame.rowGroupIndex = -1;
        return true;
    }

    /**
     * Parses partition values from {@code filePath} into the supplied arrays.
     * Missing or unparseable values leave {@code present[i]} false.
     * Byte offsets into {@code filePath} are recorded for every matched segment,
     * regardless of whether typed parsing succeeded — VARCHAR buffer fill uses them
     * directly.
     */
    private void parsePartitionValues(Utf8Sequence filePath, long[] values, boolean[] present) {
        for (int c = 0; c < partitionColumnCount; c++) {
            values[c] = 0L;
            present[c] = false;
            prunePartitionByteLo[c] = -1;
            prunePartitionByteHi[c] = -1;
        }
        if (partitionColumnCount == 0) {
            return;
        }
        final int n = filePath.size();
        int segStart = Math.min(nonGlobRootLen, n);
        final StringSink sink = Misc.getThreadLocalSink();
        while (segStart < n) {
            int segEnd = segStart;
            while (segEnd < n) {
                byte b = filePath.byteAt(segEnd);
                if (b == '/' || b == Files.SEPARATOR) {
                    break;
                }
                segEnd++;
            }
            int eqIdx = -1;
            for (int i = segStart; i < segEnd; i++) {
                if (filePath.byteAt(i) == '=') {
                    eqIdx = i;
                    break;
                }
            }
            if (eqIdx > segStart && eqIdx < segEnd - 1) {
                final int keyLen = eqIdx - segStart;
                int matchedIdx = -1;
                for (int c = 0; c < partitionColumnCount; c++) {
                    String name = partitionColumnNames.getQuick(c);
                    if (name.length() == keyLen) {
                        boolean ok = true;
                        for (int j = 0; j < keyLen; j++) {
                            if (filePath.byteAt(segStart + j) != (byte) name.charAt(j)) {
                                ok = false;
                                break;
                            }
                        }
                        if (ok) {
                            matchedIdx = c;
                            break;
                        }
                    }
                }
                if (matchedIdx >= 0) {
                    prunePartitionByteLo[matchedIdx] = eqIdx + 1;
                    prunePartitionByteHi[matchedIdx] = segEnd;
                    final int type = partitionColumnTypes.getQuick(matchedIdx);
                    if (ColumnType.tagOf(type) == ColumnType.VARCHAR) {
                        // VARCHAR uses the raw byte range, no typed parse needed.
                        present[matchedIdx] = true;
                    } else {
                        sink.clear();
                        Utf8s.utf8ToUtf16(filePath, eqIdx + 1, segEnd, sink);
                        try {
                            values[matchedIdx] = parseTyped(sink, type);
                            present[matchedIdx] = true;
                        } catch (NumericException ignored) {
                            // leave as missing -> NULL fill
                        }
                    }
                }
            }
            if (segEnd >= n) {
                break;
            }
            segStart = segEnd + 1;
        }
    }

    private long parseTyped(CharSequence cs, int columnType) throws NumericException {
        switch (ColumnType.tagOf(columnType)) {
            case ColumnType.INT:
                return Numbers.parseInt(cs);
            case ColumnType.LONG:
                return Numbers.parseLong(cs);
            case ColumnType.DATE:
                return DateFormatUtils.parseDate(cs);
            case ColumnType.TIMESTAMP:
                return MicrosFormatUtils.parseTimestamp(cs);
            case ColumnType.DOUBLE:
                return Double.doubleToRawLongBits(Numbers.parseDouble(cs));
            default:
                throw NumericException.INSTANCE;
        }
    }

    private void populatePartitionBuffersFromPath(int fileIndex, Utf8Sequence filePath, int rowCount) {
        // openNextFile (via parsePartitionValues) primed the prunePartition* scratch
        // arrays for this file; reuse them rather than re-walking the path.
        for (int c = 0; c < partitionColumnCount; c++) {
            int slot = fileIndex * partitionColumnCount + c;
            long addr = partitionBufferAddrs.getQuick(slot);
            if (addr == 0) {
                continue;
            }
            int type = partitionColumnTypes.getQuick(c);
            if (ColumnType.tagOf(type) == ColumnType.VARCHAR) {
                long auxAddr = partitionAuxBufferAddrs.getQuick(slot);
                fillVarcharPartitionBuffer(
                        auxAddr,
                        addr,
                        rowCount,
                        prunePartitionPresent[c] ? filePath : null,
                        prunePartitionPresent[c] ? prunePartitionByteLo[c] : 0,
                        prunePartitionPresent[c] ? prunePartitionByteHi[c] : 0
                );
            } else {
                fillPartitionBuffer(addr, rowCount, type,
                        prunePartitionValues[c], prunePartitionPresent[c]);
            }
        }
    }

    private void prepareFilterListForFile(int fileIndex, ParquetFileDecoder decoder) {
        if (pushdownFilterConditions == null || pushdownFilterConditions.size() == 0) {
            return;
        }
        // Filter init runs at most once per cursor lifetime - see ensureFilterConditionsInitialised.
        ensureFilterConditionsInitialised();
        DirectLongList filterList = null;
        MemoryCARWImpl filterVals = null;
        boolean stashed = false;
        try {
            filterList = new DirectLongList(
                    (long) pushdownFilterConditions.size() * ParquetRowGroupFilter.LONGS_PER_FILTER,
                    MemoryTag.NATIVE_PARQUET_PARTITION_DECODER,
                    true
            );
            filterVals = new MemoryCARWImpl(
                    ParquetRowGroupFilter.FILTER_BUFFER_PAGE_SIZE,
                    ParquetRowGroupFilter.FILTER_BUFFER_MAX_PAGES,
                    MemoryTag.NATIVE_PARQUET_PARTITION_DECODER
            );
            boolean prepared = ParquetRowGroupFilter.prepareFilterList(
                    decoder.metadata(),
                    pushdownFilterConditions,
                    filterList,
                    filterVals
            );
            if (prepared) {
                filterLists.setQuick(fileIndex, filterList);
                filterValues.setQuick(fileIndex, filterVals);
                filterBufEnds.setQuick(fileIndex, filterVals.getAddress() + filterVals.getAppendOffset());
                filterPrepared.set(fileIndex, true);
                stashed = true;
            }
        } finally {
            // If prepareFilterList threw or the filter wasn't actually prepared, release
            // the local allocations - they were never stashed in the per-file lists, so
            // closeFile() would not see them.
            if (!stashed) {
                Misc.free(filterList);
                Misc.free(filterVals);
            }
        }
    }

    private class HivePartitionedPageFrame implements PageFrame {
        private ParquetFileDecoder decoder;
        private long partitionHi;
        private int partitionIndex;
        private long partitionLo;
        private int rowGroupIndex = -1;
        private int rowGroupSize;

        @Override
        public long getAuxPageAddress(int columnIndex) {
            return 0;
        }

        @Override
        public long getAuxPageSize(int columnIndex) {
            return 0;
        }

        @Override
        public int getColumnCount() {
            return columnMapping.getColumnCount();
        }

        @Override
        public byte getFormat() {
            return PartitionFormat.PARQUET;
        }

        @Override
        public io.questdb.cairo.idx.IndexReader getIndexReader(int columnIndex, int direction) {
            return null;
        }

        @Override
        public long getPageAddress(int columnIndex) {
            return 0;
        }

        @Override
        public long getPageSize(int columnIndex) {
            return 0;
        }

        @Override
        public ParquetFileDecoder getParquetDecoder() {
            return decoder;
        }

        @Override
        public int getParquetRowGroup() {
            return rowGroupIndex;
        }

        @Override
        public int getParquetRowGroupHi() {
            return rowGroupSize;
        }

        @Override
        public int getParquetRowGroupLo() {
            return 0;
        }

        @Override
        public long getPartitionHi() {
            return partitionHi;
        }

        @Override
        public int getPartitionIndex() {
            return partitionIndex;
        }

        @Override
        public long getPartitionLo() {
            return partitionLo;
        }

        @Override
        public long getVirtualAuxPageAddress(int columnIndex) {
            if (columnIndex < parquetColumnCount) {
                return 0;
            }
            int partCol = columnIndex - parquetColumnCount;
            return partitionAuxBufferAddrs.getQuick(partitionIndex * partitionColumnCount + partCol);
        }

        @Override
        public long getVirtualAuxPageSize(int columnIndex) {
            if (columnIndex < parquetColumnCount) {
                return 0;
            }
            int partCol = columnIndex - parquetColumnCount;
            if (partitionAuxBufferAddrs.getQuick(partitionIndex * partitionColumnCount + partCol) == 0) {
                return 0;
            }
            // For VARCHAR aux: rowGroupSize entries of VARCHAR_AUX_ENTRY_BYTES bytes each.
            return (long) rowGroupSize * VARCHAR_AUX_ENTRY_BYTES;
        }

        @Override
        public long getVirtualPageAddress(int columnIndex) {
            if (columnIndex < parquetColumnCount) {
                return 0;
            }
            int partCol = columnIndex - parquetColumnCount;
            return partitionBufferAddrs.getQuick(partitionIndex * partitionColumnCount + partCol);
        }

        @Override
        public long getVirtualPageSize(int columnIndex) {
            if (columnIndex < parquetColumnCount) {
                return 0;
            }
            int partCol = columnIndex - parquetColumnCount;
            final int slot = partitionIndex * partitionColumnCount + partCol;
            // For VARCHAR, the data page size is the allocated value byte count, independent
            // of row group size. For fixed-size types it scales with the row group.
            if (ColumnType.tagOf(partitionColumnTypes.getQuick(partCol)) == ColumnType.VARCHAR) {
                return partitionBufferSizes.getQuick(slot);
            }
            int typeWidth = partitionBufferTypeWidths.getQuick(slot);
            return (long) rowGroupSize * typeWidth;
        }
    }
}
