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
import io.questdb.cairo.sql.ColumnMapping;
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
import io.questdb.std.str.LPSZ;
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
 * Variable-length partition columns (VARCHAR) are not yet supported on the
 * parallel path; the factory falls back to the sequential record cursor when
 * any partition column is VARCHAR.
 */
public class HivePartitionedReadParquetPageFrameCursor implements PageFrameCursor {
    private static final Log LOG = LogFactory.getLog(HivePartitionedReadParquetPageFrameCursor.class);
    private final ColumnMapping columnMapping = new ColumnMapping();
    private final ObjList<ParquetFileDecoder> decoders = new ObjList<>();
    private final FilesFacade ff;
    private final LongList fileAddrs = new LongList();
    private final LongList fileSizes = new LongList();
    private final LongList fds = new LongList();
    private final LongList filterBufEnds = new LongList();
    private final ObjList<DirectLongList> filterLists = new ObjList<>();
    private final BoolList filterPrepared = new BoolList();
    private final ObjList<MemoryCARWImpl> filterValues = new ObjList<>();
    private final HivePartitionedPageFrame frame = new HivePartitionedPageFrame();
    private final RecordCursor globCursor;
    private final RecordMetadata parquetMetadata;
    private final int parquetColumnCount;
    // Per-file partition buffers, flat-indexed by fileIndex * partitionColumnCount + partitionCol.
    private final LongList partitionBufferAddrs = new LongList();
    private final IntList partitionBufferTypeWidths = new IntList();
    private final LongList partitionBufferSizes = new LongList();
    private final ObjList<String> partitionColumnNames;
    private final IntList partitionColumnTypes;
    private final int partitionColumnCount;
    private final int nonGlobRootLen;
    private final Path path = new Path(MemoryTag.NATIVE_PATH);
    private final @Nullable ObjList<PushdownFilterExtractor.PushdownFilterCondition> pushdownFilterConditions;
    private SqlExecutionContext executionContext;
    private int currentFileIndex = -1;
    private long cumulativePartitionLo = 0;
    // Row counts are accumulated as files open during normal iteration so that
    // size() after a full scan does not need a separate probe walk.
    private boolean isTotalRowCountFinalised = false;
    private int lowestOpenFileIndex = 0;
    private long runningRowCount = 0;
    private long totalRowCount = -1;

    public HivePartitionedReadParquetPageFrameCursor(
            FilesFacade ff,
            RecordCursor globCursor,
            RecordMetadata parquetMetadata,
            int parquetColumnCount,
            ObjList<String> partitionColumnNames,
            IntList partitionColumnTypes,
            CharSequence nonGlobRoot,
            @Nullable ObjList<PushdownFilterExtractor.PushdownFilterCondition> pushdownFilterConditions
    ) {
        this.ff = ff;
        this.globCursor = globCursor;
        this.parquetMetadata = parquetMetadata;
        this.parquetColumnCount = parquetColumnCount;
        this.partitionColumnNames = partitionColumnNames;
        this.partitionColumnTypes = partitionColumnTypes;
        this.partitionColumnCount = partitionColumnNames.size();
        this.nonGlobRootLen = nonGlobRoot.length();
        this.pushdownFilterConditions = pushdownFilterConditions;
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
        Misc.free(globCursor);
        Misc.free(path);
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
            if (!openNextFile()) {
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
        globCursor.toTop();
        currentFileIndex = -1;
        lowestOpenFileIndex = 0;
        cumulativePartitionLo = 0;
        // Preserve the finalised row count across toTop calls: file row counts are
        // immutable, so once we've seen them once we can keep reusing the total.
        if (!isTotalRowCountFinalised) {
            totalRowCount = -1;
            runningRowCount = 0;
        }
        frame.rowGroupIndex = -1;
        buildColumnMapping();
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

    private boolean openNextFile() {
        if (!globCursor.hasNext()) {
            return false;
        }
        final int fileIndex = ++currentFileIndex;
        final Utf8Sequence filePath = globCursor.getRecord().getVarcharA(0);
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

    private void allocatePartitionBuffersForFile(int fileIndex, Utf8Sequence filePath, ParquetFileDecoder decoder) {
        // Reserve slots in the flat per-file × per-col arrays.
        for (int c = 0; c < partitionColumnCount; c++) {
            partitionBufferAddrs.add(0);
            partitionBufferSizes.add(0);
            partitionBufferTypeWidths.add(0);
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
        for (int c = 0; c < partitionColumnCount; c++) {
            int type = partitionColumnTypes.getQuick(c);
            int typeWidth = ColumnType.sizeOf(type);
            long bufferSize = (long) maxRowGroupSize * typeWidth;
            long bufferAddr = Unsafe.malloc(bufferSize, MemoryTag.NATIVE_DEFAULT);
            int slot = fileIndex * partitionColumnCount + c;
            partitionBufferAddrs.setQuick(slot, bufferAddr);
            partitionBufferSizes.setQuick(slot, bufferSize);
            partitionBufferTypeWidths.setQuick(slot, typeWidth);
        }
        // Populate buffers by parsing partition values from the file path.
        populatePartitionBuffersFromPath(fileIndex, filePath, maxRowGroupSize);
    }

    private void populatePartitionBuffersFromPath(int fileIndex, Utf8Sequence filePath, int rowCount) {
        // Start every column marked as missing; flip to present when we find a matching segment.
        final boolean[] present = new boolean[partitionColumnCount];
        final long[] values = new long[partitionColumnCount];
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
                    sink.clear();
                    Utf8s.utf8ToUtf16(filePath, eqIdx + 1, segEnd, sink);
                    try {
                        values[matchedIdx] = parseTyped(sink, partitionColumnTypes.getQuick(matchedIdx));
                        present[matchedIdx] = true;
                    } catch (NumericException ignored) {
                        // leave as missing -> NULL fill
                    }
                }
            }
            if (segEnd >= n) {
                break;
            }
            segStart = segEnd + 1;
        }
        for (int c = 0; c < partitionColumnCount; c++) {
            int slot = fileIndex * partitionColumnCount + c;
            long addr = partitionBufferAddrs.getQuick(slot);
            if (addr != 0) {
                fillPartitionBuffer(addr, rowCount, partitionColumnTypes.getQuick(c), values[c], present[c]);
            }
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

    private void prepareFilterListForFile(int fileIndex, ParquetFileDecoder decoder) {
        if (pushdownFilterConditions == null || pushdownFilterConditions.size() == 0) {
            return;
        }
        try {
            for (int i = 0, sz = pushdownFilterConditions.size(); i < sz; i++) {
                pushdownFilterConditions.getQuick(i).init(executionContext);
            }
        } catch (SqlException e) {
            throw CairoException.nonCritical().put("failed to init pushdown filter: ").put(e.getFlyweightMessage());
        }
        DirectLongList filterList = new DirectLongList(
                (long) pushdownFilterConditions.size() * ParquetRowGroupFilter.LONGS_PER_FILTER,
                MemoryTag.NATIVE_PARQUET_PARTITION_DECODER,
                true
        );
        MemoryCARWImpl filterVals = new MemoryCARWImpl(
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
        } else {
            Misc.free(filterList);
            Misc.free(filterVals);
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
            int typeWidth = partitionBufferTypeWidths.getQuick(partitionIndex * partitionColumnCount + partCol);
            return (long) rowGroupSize * typeWidth;
        }
    }

    // Suppress unused field warning - kept to align lifetime with cursor and document the contract.
    @SuppressWarnings("unused")
    private void unusedSink() {
        // ensures imports above (LPSZ) are tracked
    }
}
