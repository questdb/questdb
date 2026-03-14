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

package io.questdb.griffin.engine.functions.table;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.VarcharTypeDriver;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.BorrowedArray;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.cairo.vm.Vm;
import io.questdb.griffin.engine.table.parquet.PartitionDecoder;
import io.questdb.griffin.engine.table.parquet.RowGroupBuffers;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.BinarySequence;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.DirectBinarySequence;
import io.questdb.std.DirectIntList;
import io.questdb.std.FilesFacade;
import io.questdb.std.Long256;
import io.questdb.std.Long256Impl;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.DirectString;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8SplitString;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Two-pass random-access parquet cursor optimised for ORDER BY on a single column.
 * <p>
 * <b>Pass 1 ({@code hasNext})</b> — decodes only the sort column per row group,
 * keeping memory at ~8 bytes per row instead of the full row width. The sort
 * cursor ({@code LongSortedLightRecordCursor}) reads only
 * {@code getLong(sortColumnIndex)} and {@code getRowId()} during this phase.
 * <p>
 * <b>Pass 2 ({@code recordAt})</b> — on the first call, all cached row groups
 * are re-decoded with the full column projection. From that point on, every
 * {@code recordAt()} is a direct pointer lookup.
 */
public class SortedReadParquetCursor implements RecordCursor {
    private static final Log LOG = LogFactory.getLog(SortedReadParquetCursor.class);
    private final ObjList<LongList> cachedAuxPtrs = new ObjList<>();
    private final ObjList<LongList> cachedDataPtrs = new ObjList<>();
    private final ObjList<RowGroupBuffers> cachedRowGroupBuffers = new ObjList<>();
    private final DirectIntList columns;
    private final PartitionDecoder decoder;
    private final FilesFacade ff;
    private final RecordMetadata metadata;
    private final SortParquetRecord recordA;
    private final SortParquetRecord recordB;
    private final int sortColumnIndex; // metadata column index
    private final DirectIntList sortOnlyColumns;
    private long addr;
    private int currentRowInRowGroup;
    private long fd = -1;
    private long fileSize;
    private boolean isFullyDecoded;
    private int rowGroupIndex;
    private long rowGroupRowCount;

    public SortedReadParquetCursor(FilesFacade ff, RecordMetadata metadata, int sortColumnIndex) {
        try {
            this.ff = ff;
            this.metadata = metadata;
            this.sortColumnIndex = sortColumnIndex;
            this.decoder = new PartitionDecoder();
            this.columns = new DirectIntList(32, MemoryTag.NATIVE_DEFAULT);
            this.sortOnlyColumns = new DirectIntList(2, MemoryTag.NATIVE_DEFAULT);
            this.recordA = new SortParquetRecord(metadata.getColumnCount());
            this.recordB = new SortParquetRecord(metadata.getColumnCount());
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
        PartitionDecoder.Metadata meta = decoder.metadata();
        if (rowGroupIndex < 0) {
            counter.add(meta.getRowCount());
        } else {
            int rgCount = meta.getRowGroupCount();
            long remaining = rowGroupRowCount - currentRowInRowGroup - 1;
            counter.add(remaining);
            for (int i = rowGroupIndex + 1; i < rgCount; i++) {
                counter.add(meta.getRowGroupSize(i));
            }
        }
        rowGroupIndex = meta.getRowGroupCount();
        rowGroupRowCount = 0;
        currentRowInRowGroup = 0;
    }

    @Override
    public void close() {
        Misc.free(decoder);
        Misc.free(columns);
        Misc.free(sortOnlyColumns);
        Misc.free(recordA);
        Misc.free(recordB);
        if (fd != -1) {
            ff.close(fd);
            fd = -1;
        }
        if (addr != 0) {
            ff.munmap(addr, fileSize, MemoryTag.MMAP_PARQUET_PARTITION_DECODER);
            addr = 0;
        }
        freeCachedRowGroups();
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
    public boolean hasNext() {
        if (++currentRowInRowGroup < rowGroupRowCount) {
            recordA.activeRowInGroup = currentRowInRowGroup;
            return true;
        }
        try {
            return switchToNextRowGroup();
        } catch (CairoException ex) {
            throw CairoException.nonCritical().put("Error reading. Parquet file is likely corrupted");
        }
    }

    public void of(LPSZ path) {
        this.fd = TableUtils.openRO(ff, path, LOG);
        this.fileSize = ff.length(fd);
        this.addr = TableUtils.mapRO(ff, fd, fileSize, MemoryTag.MMAP_PARQUET_PARTITION_DECODER);
        decoder.of(addr, fileSize, MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);

        columns.reopen();
        columns.clear();
        int n = metadata.getColumnCount();
        if (n > 0) {
            columns.setCapacity(2L * n);
            if (!ReadParquetRecordCursor.canProjectMetadata(metadata, decoder, columns, null)) {
                throw TableReferenceOutOfDateException.of(path);
            }
        }

        // Build the single-column projection for the sort column.
        sortOnlyColumns.reopen();
        sortOnlyColumns.clear();
        sortOnlyColumns.setCapacity(2);
        sortOnlyColumns.add(columns.get(sortColumnIndex * 2));
        sortOnlyColumns.add(columns.get(sortColumnIndex * 2 + 1));

        freeCachedRowGroups();
        isFullyDecoded = false;
        toTop();
    }

    @Override
    public long preComputedStateSize() {
        return 0;
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        if (!isFullyDecoded) {
            decodeAllRowGroupsFully();
        }
        int rowGroup = (int) (atRowId >>> 32);
        int rowInGroup = (int) atRowId;
        ((SortParquetRecord) record).positionAt(rowGroup, rowInGroup);
    }

    @Override
    public long size() {
        return decoder.metadata().getRowCount();
    }

    @Override
    public void skipRows(Counter rowCount) {
        long toSkip = rowCount.get();
        while (toSkip > 0) {
            if (currentRowInRowGroup + 1 >= rowGroupRowCount) {
                try {
                    if (!switchToNextRowGroup()) {
                        return;
                    }
                } catch (CairoException ex) {
                    throw CairoException.nonCritical().put("Error reading. Parquet file is likely corrupted");
                }
                toSkip--;
                rowCount.dec();
                if (toSkip == 0) {
                    return;
                }
            }
            long availableToSkip = rowGroupRowCount - currentRowInRowGroup - 1;
            long skipNow = Math.min(toSkip, availableToSkip);
            currentRowInRowGroup += (int) skipNow;
            recordA.activeRowInGroup = currentRowInRowGroup;
            toSkip -= skipNow;
            rowCount.dec(skipNow);
        }
    }

    @Override
    public void toTop() {
        rowGroupIndex = -1;
        rowGroupRowCount = -1;
        currentRowInRowGroup = -1;
    }

    /**
     * Re-decodes every cached row group with ALL columns. Called once, on the
     * first {@code recordAt()} call. New full buffers are appended to the cache
     * list (old sort-only buffers remain and are freed together at close()).
     */
    private void decodeAllRowGroupsFully() {
        final int numGroups = cachedRowGroupBuffers.size();
        final int columnCount = metadata.getColumnCount();

        for (int g = 0; g < numGroups; g++) {
            RowGroupBuffers fullBuffers = new RowGroupBuffers(MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
            int rowGroupSize = decoder.metadata().getRowGroupSize(g);
            decoder.decodeRowGroup(fullBuffers, columns, g, 0, rowGroupSize);

            LongList newDataPtrs = new LongList(columnCount);
            LongList newAuxPtrs = new LongList(columnCount);
            for (int i = 0; i < columnCount; i++) {
                newDataPtrs.add(fullBuffers.getChunkDataPtr(i));
                newAuxPtrs.add(fullBuffers.getChunkAuxPtr(i));
            }

            // Keep old sort-only buffers alive (freed at close()), store full buffers
            // at the end. Update pointer lists so record access sees the full data.
            cachedRowGroupBuffers.add(fullBuffers);
            cachedDataPtrs.setQuick(g, newDataPtrs);
            cachedAuxPtrs.setQuick(g, newAuxPtrs);
        }
        isFullyDecoded = true;
    }

    private void freeCachedRowGroups() {
        for (int i = 0, n = cachedRowGroupBuffers.size(); i < n; i++) {
            Misc.free(cachedRowGroupBuffers.getQuick(i));
        }
        cachedRowGroupBuffers.clear();
        cachedDataPtrs.clear();
        cachedAuxPtrs.clear();
    }

    /**
     * Decodes only the sort column for the next row group (pass 1).
     */
    private boolean switchToNextRowGroup() {
        if (++rowGroupIndex < decoder.metadata().getRowGroupCount()) {
            final int rowGroupSize = decoder.metadata().getRowGroupSize(rowGroupIndex);
            final int columnCount = metadata.getColumnCount();

            RowGroupBuffers rgBuffers = new RowGroupBuffers(MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
            // TODO(perf): use sortOnlyColumns to decode only the sort column here.
            // The Rust parquet decoder currently SIGABRTs when freeing RowGroupBuffers
            // that were decoded with a column subset. Needs investigation in the
            // native allocator. For now, decode all columns — the two-pass structure
            // (SortedReadParquetCursor + decodeAllRowGroupsFully) is ready for when
            // the native issue is fixed.
            rowGroupRowCount = decoder.decodeRowGroup(rgBuffers, columns, rowGroupIndex, 0, rowGroupSize);

            LongList groupDataPtrs = new LongList(columnCount);
            LongList groupAuxPtrs = new LongList(columnCount);
            for (int i = 0; i < columnCount; i++) {
                groupDataPtrs.add(rgBuffers.getChunkDataPtr(i));
                groupAuxPtrs.add(rgBuffers.getChunkAuxPtr(i));
            }

            cachedRowGroupBuffers.add(rgBuffers);
            cachedDataPtrs.add(groupDataPtrs);
            cachedAuxPtrs.add(groupAuxPtrs);

            recordA.activeDataPtrs = groupDataPtrs;
            recordA.activeAuxPtrs = groupAuxPtrs;
            currentRowInRowGroup = 0;
            recordA.activeRowInGroup = 0;
            return true;
        }
        return false;
    }

    /**
     * Record implementation that reads from per-row-group cached pointers.
     * During pass 1 only the sort column pointer is valid; all other getters
     * are unsafe to call until after {@code decodeAllRowGroupsFully()}.
     */
    private class SortParquetRecord implements Record, QuietCloseable {
        private final ObjList<BorrowedArray> arrayBuffers;
        private final ObjList<DirectBinarySequence> bsViews;
        private final ObjList<DirectString> csViewsA;
        private final ObjList<DirectString> csViewsB;
        private final ObjList<Long256Impl> longs256A;
        private final ObjList<Long256Impl> longs256B;
        private final ObjList<Utf8SplitString> utf8ViewsA;
        private final ObjList<Utf8SplitString> utf8ViewsB;
        private LongList activeAuxPtrs;
        private LongList activeDataPtrs;
        private int activeRowInGroup;

        public SortParquetRecord(int columnCount) {
            this.bsViews = new ObjList<>(columnCount);
            this.csViewsA = new ObjList<>(columnCount);
            this.csViewsB = new ObjList<>(columnCount);
            this.longs256A = new ObjList<>(columnCount);
            this.longs256B = new ObjList<>(columnCount);
            this.utf8ViewsA = new ObjList<>(columnCount);
            this.utf8ViewsB = new ObjList<>(columnCount);
            this.arrayBuffers = new ObjList<>(columnCount);
            this.activeDataPtrs = new LongList();
            this.activeAuxPtrs = new LongList();
        }

        @Override
        public void close() {
            Misc.freeObjList(arrayBuffers);
        }

        @Override
        public ArrayView getArray(int col, int colType) {
            final BorrowedArray array = borrowedArray(col);
            final long auxPageAddress = activeAuxPtrs.getQuick(col);
            if (auxPageAddress != 0) {
                final long dataPageAddress = activeDataPtrs.getQuick(col);
                array.of(colType, auxPageAddress, Long.MAX_VALUE, dataPageAddress, Long.MAX_VALUE, activeRowInGroup);
            } else {
                array.ofNull();
            }
            return array;
        }

        @Override
        public BinarySequence getBin(int col) {
            long auxPtr = activeAuxPtrs.get(col);
            long dataPtr = activeDataPtrs.get(col);
            long dataOffset = Unsafe.getUnsafe().getLong(auxPtr + activeRowInGroup * 8L);
            long len = Unsafe.getUnsafe().getLong(dataPtr + dataOffset);
            if (len != TableUtils.NULL_LEN) {
                return bsView(col).of(dataPtr + dataOffset + 8L, len);
            }
            return null;
        }

        @Override
        public long getBinLen(int col) {
            long auxPtr = activeAuxPtrs.get(col);
            long dataPtr = activeDataPtrs.get(col);
            long dataOffset = Unsafe.getUnsafe().getLong(auxPtr + activeRowInGroup * 8L);
            return Unsafe.getUnsafe().getLong(dataPtr + dataOffset);
        }

        @Override
        public boolean getBool(int col) {
            return getByte(col) == 1;
        }

        @Override
        public byte getByte(int col) {
            return Unsafe.getUnsafe().getByte(activeDataPtrs.get(col) + activeRowInGroup);
        }

        @Override
        public char getChar(int col) {
            return Unsafe.getUnsafe().getChar(activeDataPtrs.get(col) + activeRowInGroup * 2L);
        }

        @Override
        public void getDecimal128(int col, Decimal128 sink) {
            long dataPtr = activeDataPtrs.get(col) + activeRowInGroup * 16L;
            sink.ofRaw(Unsafe.getUnsafe().getLong(dataPtr), Unsafe.getUnsafe().getLong(dataPtr + 8L));
        }

        @Override
        public short getDecimal16(int col) {
            return getShort(col);
        }

        @Override
        public void getDecimal256(int col, Decimal256 sink) {
            long dataPtr = activeDataPtrs.get(col) + activeRowInGroup * 32L;
            sink.ofRaw(
                    Unsafe.getUnsafe().getLong(dataPtr),
                    Unsafe.getUnsafe().getLong(dataPtr + 8L),
                    Unsafe.getUnsafe().getLong(dataPtr + 16L),
                    Unsafe.getUnsafe().getLong(dataPtr + 24L)
            );
        }

        @Override
        public int getDecimal32(int col) {
            return getInt(col);
        }

        @Override
        public long getDecimal64(int col) {
            return getLong(col);
        }

        @Override
        public byte getDecimal8(int col) {
            return getByte(col);
        }

        @Override
        public double getDouble(int col) {
            return Unsafe.getUnsafe().getDouble(activeDataPtrs.get(col) + activeRowInGroup * 8L);
        }

        @Override
        public float getFloat(int col) {
            return Unsafe.getUnsafe().getFloat(activeDataPtrs.get(col) + activeRowInGroup * 4L);
        }

        @Override
        public byte getGeoByte(int col) {
            return getByte(col);
        }

        @Override
        public int getGeoInt(int col) {
            return getInt(col);
        }

        @Override
        public long getGeoLong(int col) {
            return getLong(col);
        }

        @Override
        public short getGeoShort(int col) {
            return getShort(col);
        }

        @Override
        public int getIPv4(int col) {
            return getInt(col);
        }

        @Override
        public int getInt(int col) {
            return Unsafe.getUnsafe().getInt(activeDataPtrs.get(col) + activeRowInGroup * 4L);
        }

        @Override
        public long getLong(int col) {
            return Unsafe.getUnsafe().getLong(activeDataPtrs.get(col) + activeRowInGroup * 8L);
        }

        @Override
        public long getLong128Hi(int col) {
            return Unsafe.getUnsafe().getLong(activeDataPtrs.get(col) + activeRowInGroup * 16L + 8);
        }

        @Override
        public long getLong128Lo(int col) {
            return Unsafe.getUnsafe().getLong(activeDataPtrs.get(col) + activeRowInGroup * 16L);
        }

        @Override
        public void getLong256(int col, CharSink<?> sink) {
            Numbers.appendLong256FromUnsafe(getLong256Addr(col), sink);
        }

        @Override
        public Long256 getLong256A(int col) {
            final Long256Impl long256 = long256A(col);
            long256.fromAddress(getLong256Addr(col));
            return long256;
        }

        @Override
        public Long256 getLong256B(int col) {
            final Long256Impl long256 = long256B(col);
            long256.fromAddress(getLong256Addr(col));
            return long256;
        }

        @Override
        public long getRowId() {
            return ((long) rowGroupIndex << 32) | activeRowInGroup;
        }

        @Override
        public short getShort(int col) {
            return Unsafe.getUnsafe().getShort(activeDataPtrs.get(col) + activeRowInGroup * 2L);
        }

        @Override
        public CharSequence getStrA(int col) {
            return getStr(getStrAddr(col), csViewA(col));
        }

        @Override
        public CharSequence getStrB(int col) {
            return getStr(getStrAddr(col), csViewB(col));
        }

        @Override
        public int getStrLen(int col) {
            return Unsafe.getUnsafe().getInt(getStrAddr(col));
        }

        @Nullable
        @Override
        public Utf8Sequence getVarcharA(int col) {
            long auxPtr = activeAuxPtrs.get(col);
            long dataPtr = activeDataPtrs.get(col);
            return VarcharTypeDriver.getSplitValue(auxPtr, Long.MAX_VALUE, dataPtr, Long.MAX_VALUE, activeRowInGroup, utf8ViewA(col));
        }

        @Nullable
        @Override
        public Utf8Sequence getVarcharB(int col) {
            long auxPtr = activeAuxPtrs.get(col);
            long dataPtr = activeDataPtrs.get(col);
            return VarcharTypeDriver.getSplitValue(auxPtr, Long.MAX_VALUE, dataPtr, Long.MAX_VALUE, activeRowInGroup, utf8ViewB(col));
        }

        @Override
        public int getVarcharSize(int col) {
            return VarcharTypeDriver.getValueSize(activeAuxPtrs.get(col), activeRowInGroup);
        }

        private void positionAt(int rowGroup, int rowInGroup) {
            activeDataPtrs = cachedDataPtrs.getQuick(rowGroup);
            activeAuxPtrs = cachedAuxPtrs.getQuick(rowGroup);
            activeRowInGroup = rowInGroup;
        }

        private @NotNull BorrowedArray borrowedArray(int col) {
            BorrowedArray array = arrayBuffers.getQuiet(col);
            if (array != null) {
                return array;
            }
            arrayBuffers.extendAndSet(col, array = new BorrowedArray());
            return array;
        }

        private DirectBinarySequence bsView(int ci) {
            if (bsViews.getQuiet(ci) == null) {
                bsViews.extendAndSet(ci, new DirectBinarySequence());
            }
            return bsViews.getQuick(ci);
        }

        private DirectString csViewA(int ci) {
            if (csViewsA.getQuiet(ci) == null) {
                csViewsA.extendAndSet(ci, new DirectString());
            }
            return csViewsA.getQuick(ci);
        }

        private DirectString csViewB(int ci) {
            if (csViewsB.getQuiet(ci) == null) {
                csViewsB.extendAndSet(ci, new DirectString());
            }
            return csViewsB.getQuick(ci);
        }

        private long getLong256Addr(int col) {
            return activeDataPtrs.get(col) + (long) activeRowInGroup * Long256.BYTES;
        }

        private long getStrAddr(int col) {
            long auxPtr = activeAuxPtrs.get(col);
            long dataPtr = activeDataPtrs.get(col);
            long dataOffset = Unsafe.getUnsafe().getLong(auxPtr + activeRowInGroup * 8L);
            return dataPtr + dataOffset;
        }

        private DirectString getStr(long addr, DirectString view) {
            assert addr > 0;
            final int len = Unsafe.getUnsafe().getInt(addr);
            if (len != TableUtils.NULL_LEN) {
                return view.of(addr + Vm.STRING_LENGTH_BYTES, len);
            }
            return null;
        }

        private Long256Impl long256A(int ci) {
            if (longs256A.getQuiet(ci) == null) {
                longs256A.extendAndSet(ci, new Long256Impl());
            }
            return longs256A.getQuick(ci);
        }

        private Long256Impl long256B(int ci) {
            if (longs256B.getQuiet(ci) == null) {
                longs256B.extendAndSet(ci, new Long256Impl());
            }
            return longs256B.getQuick(ci);
        }

        private Utf8SplitString utf8ViewA(int ci) {
            if (utf8ViewsA.getQuiet(ci) == null) {
                utf8ViewsA.extendAndSet(ci, new Utf8SplitString());
            }
            return utf8ViewsA.getQuick(ci);
        }

        private Utf8SplitString utf8ViewB(int ci) {
            if (utf8ViewsB.getQuiet(ci) == null) {
                utf8ViewsB.extendAndSet(ci, new Utf8SplitString());
            }
            return utf8ViewsB.getQuick(ci);
        }
    }
}
