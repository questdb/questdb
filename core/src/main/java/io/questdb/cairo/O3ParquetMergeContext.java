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

import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.engine.table.parquet.ParquetPartitionDecoder;
import io.questdb.griffin.engine.table.parquet.PartitionDescriptor;
import io.questdb.griffin.engine.table.parquet.PartitionUpdater;
import io.questdb.griffin.engine.table.parquet.RowGroupBuffers;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimal64;
import io.questdb.std.DirectIntList;
import io.questdb.std.IntIntHashMap;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8StringSink;

import java.io.Closeable;

public class O3ParquetMergeContext implements Closeable {
    // Initial size of the rebase aux arena (see getRebaseAuxMem). Grown on demand.
    private static final long REBASE_AUX_ARENA_PAGE_SIZE = 16 * 1024;
    private ObjList<O3ParquetMergeStrategy.MergeAction> actionsBuf;
    private IntList activeColIndices;
    private IntList activeToDecodeIdx;
    private DirectIntList bloomFilterColumns;
    private PartitionDescriptor chunkDescriptor;
    private LongList convertedPtrs;
    private Decimal128 decimal128Buf;
    private Decimal256 decimal256Buf;
    private Decimal64 decimal64Buf;
    // Non-owning descriptor for the O3-only writers (writeFreshParquetFromO3 +
    // copyO3ToRowGroup). Column pointers reference already-sorted/deduped O3
    // source buffers — and the merge index for the designated timestamp — so
    // nothing in this descriptor needs to be freed by the descriptor itself.
    private PartitionDescriptor freshPartitionDescriptor;
    private LongList gapO3Ranges;
    private LongList mergeDstBufs;
    private LongList nullBufs;
    private IntIntHashMap parquetColIdToIdx;
    private DirectIntList parquetColumns;
    private ParquetMetaFileReader parquetMetaReader;
    private ParquetPartitionDecoder partitionDecoder;
    private PartitionUpdater partitionUpdater;
    private MemoryCARW rebaseAuxMem;
    private LongList rgO3Ranges;
    private LongList rowGroupBounds;
    private RowGroupBuffers rowGroupBuffers;
    private LongList srcPtrs;
    private IntList tableToParquetIdx;
    private LongList tmpBufs;
    private StringSink utf16Sink;
    private Utf8StringSink utf8Sink;

    public O3ParquetMergeContext() {
        actionsBuf = new ObjList<>();
        activeColIndices = new IntList();
        activeToDecodeIdx = new IntList();
        bloomFilterColumns = new DirectIntList(16, MemoryTag.NATIVE_O3);
        chunkDescriptor = new PartitionDescriptor();
        convertedPtrs = new LongList();
        decimal128Buf = new Decimal128();
        decimal256Buf = new Decimal256();
        decimal64Buf = new Decimal64();
        freshPartitionDescriptor = new PartitionDescriptor();
        gapO3Ranges = new LongList();
        mergeDstBufs = new LongList();
        nullBufs = new LongList();
        parquetColumns = new DirectIntList(64, MemoryTag.NATIVE_O3);
        parquetColIdToIdx = new IntIntHashMap();
        parquetMetaReader = new ParquetMetaFileReader();
        partitionDecoder = new ParquetPartitionDecoder();
        partitionUpdater = new PartitionUpdater();
        rebaseAuxMem = Vm.getCARWInstance(REBASE_AUX_ARENA_PAGE_SIZE, Integer.MAX_VALUE, MemoryTag.NATIVE_O3);
        rgO3Ranges = new LongList();
        rowGroupBuffers = new RowGroupBuffers(MemoryTag.NATIVE_PARQUET_PARTITION_UPDATER);
        rowGroupBounds = new LongList();
        srcPtrs = new LongList();
        tableToParquetIdx = new IntList();
        tmpBufs = new LongList();
        utf16Sink = new StringSink();
        utf8Sink = new Utf8StringSink();
    }

    public void clear() {
        activeColIndices.clear();
        activeToDecodeIdx.clear();
        bloomFilterColumns.clear();
        chunkDescriptor.clear();
        convertedPtrs.clear();
        freshPartitionDescriptor.clear();
        gapO3Ranges.clear();
        mergeDstBufs.clear();
        nullBufs.clear();
        parquetColIdToIdx.clear();
        parquetColumns.clear();
        parquetMetaReader.clear();
        rgO3Ranges.clear();
        rowGroupBounds.clear();
        srcPtrs.clear();
        tableToParquetIdx.clear();
    }

    @Override
    public void close() {
        actionsBuf = null;
        activeColIndices = null;
        activeToDecodeIdx = null;
        bloomFilterColumns = Misc.free(bloomFilterColumns);
        chunkDescriptor = Misc.free(chunkDescriptor);
        // Holds pointer copies into rowGroupBuffers / nullBufs / tmpBufs, not owned
        // buffers, so it is dropped like srcPtrs (no freeNativePairs).
        convertedPtrs = null;
        decimal128Buf = null;
        decimal256Buf = null;
        decimal64Buf = null;
        freshPartitionDescriptor = Misc.free(freshPartitionDescriptor);
        gapO3Ranges = null;
        // Each list stores [addr, size, addr, size] per column; the per-row-group
        // finally blocks normally free these, but on abnormal shutdown (worker
        // thread death) the lists may still hold native pointers. Walk and free
        // before dropping the references.
        mergeDstBufs = freeNativePairs(mergeDstBufs);
        nullBufs = freeNativePairs(nullBufs);
        tmpBufs = freeNativePairs(tmpBufs);
        parquetColIdToIdx = null;
        parquetColumns = Misc.free(parquetColumns);
        if (parquetMetaReader != null) {
            // Reader does not own its mmap; clear() releases the lazily
            // allocated native handle and zeros all fields.
            parquetMetaReader.clear();
            parquetMetaReader = null;
        }
        partitionDecoder = Misc.free(partitionDecoder);
        partitionUpdater = Misc.free(partitionUpdater);
        rebaseAuxMem = Misc.free(rebaseAuxMem);
        rgO3Ranges = null;
        rowGroupBuffers = Misc.free(rowGroupBuffers);
        rowGroupBounds = null;
        srcPtrs = null;
        tableToParquetIdx = null;
        utf16Sink = null;
        utf8Sink = null;
    }

    public ObjList<O3ParquetMergeStrategy.MergeAction> getActionsBuf() {
        return actionsBuf;
    }

    public IntList getActiveColIndices(int columnCount) {
        activeColIndices.setPos(columnCount);
        return activeColIndices;
    }

    public IntList getActiveToDecodeIdx(int columnCount) {
        activeToDecodeIdx.setPos(columnCount);
        return activeToDecodeIdx;
    }

    public DirectIntList getBloomFilterColumns() {
        return bloomFilterColumns;
    }

    public PartitionDescriptor getChunkDescriptor() {
        return chunkDescriptor;
    }

    /**
     * Per-row-group result table for {@link O3PartitionJob#prepareParquetSourceColumn}, sized
     * for {@code colCount} columns. Layout: 4 longs per column =
     * {@code [dataPtr, dataSize, auxPtr, auxSize]} (auxPtr/auxSize are 0 for fixed-size columns).
     * Holds the target-typed source pointers a converted/null/raw decoded column resolves to;
     * the owned allocations behind them are tracked separately in the caller's free-list.
     */
    public LongList getConvertedPtrs(int colCount) {
        final int requiredLen = colCount * 4;
        convertedPtrs.setPos(requiredLen);
        convertedPtrs.fill(0, requiredLen, 0);
        return convertedPtrs;
    }

    public Decimal128 getDecimal128Buf() {
        return decimal128Buf;
    }

    public Decimal256 getDecimal256Buf() {
        return decimal256Buf;
    }

    public Decimal64 getDecimal64Buf() {
        return decimal64Buf;
    }

    public PartitionDescriptor getFreshPartitionDescriptor() {
        return freshPartitionDescriptor;
    }

    public LongList getGapO3Ranges() {
        return gapO3Ranges;
    }

    public LongList getMergeDstBufs(int colCount) {
        final int requiredLen = colCount * 4;
        mergeDstBufs.setPos(requiredLen);
        mergeDstBufs.fill(0, requiredLen, 0);
        return mergeDstBufs;
    }

    public LongList getNullBufs(int colCount) {
        final int requiredLen = colCount * 4;
        nullBufs.setPos(requiredLen);
        nullBufs.fill(0, requiredLen, 0);
        return nullBufs;
    }

    public IntIntHashMap getParquetColIdToIdx() {
        parquetColIdToIdx.clear();
        return parquetColIdToIdx;
    }

    public DirectIntList getParquetColumns() {
        return parquetColumns;
    }

    public ParquetMetaFileReader getParquetMetaReader() {
        return parquetMetaReader;
    }

    public ParquetPartitionDecoder getPartitionDecoder() {
        return partitionDecoder;
    }

    public PartitionUpdater getPartitionUpdater() {
        return partitionUpdater;
    }

    /**
     * Reusable scratch arena for window-relative rebased var-column aux vectors.
     * The caller sizes it once per apply (a later resize moves earlier slots),
     * then hands out base+offset slots. Freed in {@link #close()}.
     */
    public MemoryCARW getRebaseAuxMem() {
        return rebaseAuxMem;
    }

    public LongList getRgO3Ranges() {
        return rgO3Ranges;
    }

    public LongList getRowGroupBounds() {
        return rowGroupBounds;
    }

    public RowGroupBuffers getRowGroupBuffers() {
        return rowGroupBuffers;
    }

    public LongList getSrcPtrs(int colCount) {
        final int requiredLen = colCount * 2;
        srcPtrs.setPos(requiredLen);
        srcPtrs.fill(0, requiredLen, 0);
        return srcPtrs;
    }

    public IntList getTableToParquetIdx(int columnCount) {
        tableToParquetIdx.setAll(columnCount, -1);
        return tableToParquetIdx;
    }

    /**
     * Per-row-group temporary buffer table, sized for {@code activeColCount} columns.
     * Layout: 4 longs per column = {@code [auxAddr, auxSize, dataAddr, dataSize]}.
     * Returned list is zero-filled so callers can write-then-free without
     * worrying about stale entries from earlier row groups.
     */
    public LongList getTmpBufs(int activeColCount) {
        final int requiredLen = activeColCount * 4;
        tmpBufs.setPos(requiredLen);
        tmpBufs.fill(0, requiredLen, 0);
        return tmpBufs;
    }

    /**
     * Reusable UTF-16 sink for parquet rewrite / convert-to-native conversion
     * passes. Caller is responsible for calling {@code clear()} before use; the
     * loops in {@link O3PartitionJob#convertFixedColumnToString} and
     * {@link O3PartitionJob#convertVarColumnToFixed} clear once per row anyway.
     */
    public StringSink getUtf16Sink() {
        return utf16Sink;
    }

    /**
     * Reusable UTF-8 sink for parquet rewrite / convert-to-native conversion
     * passes. See {@link #getUtf16Sink()} for the clear-before-use contract.
     */
    public Utf8StringSink getUtf8Sink() {
        return utf8Sink;
    }

    /**
     * Releases the Rust-owned partition updater (file descriptors) held by
     * the context while keeping it pooled for reuse. Call after each
     * processParquetPartition() invocation.
     * <p>
     * Does not touch {@link ParquetMetaFileReader}: the caller owns the
     * {@code _pm} mapping and is responsible for the {@code clear() + munmap}
     * pair on the reader. See the lifecycle contract on
     * {@link ParquetMetaFileReader}.
     */
    public void releaseResources() {
        partitionUpdater.close();
    }

    /**
     * Walks a per-column buffer list with [addr, size, addr, size] stride and
     * frees any non-zero (addr, size) pair. Returns {@code null} so the caller
     * can drop the reference in one assignment.
     */
    private static LongList freeNativePairs(LongList list) {
        if (list == null) {
            return null;
        }
        final int n = list.size();
        for (int i = 0; i + 1 < n; i += 2) {
            long addr = list.getQuick(i);
            if (addr != 0) {
                Unsafe.free(addr, list.getQuick(i + 1), MemoryTag.NATIVE_O3);
            }
        }
        return null;
    }
}
