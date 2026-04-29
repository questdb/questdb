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

import io.questdb.griffin.engine.table.parquet.OwnedMemoryPartitionDescriptor;
import io.questdb.griffin.engine.table.parquet.ParquetPartitionDecoder;
import io.questdb.griffin.engine.table.parquet.PartitionDescriptor;
import io.questdb.griffin.engine.table.parquet.PartitionUpdater;
import io.questdb.griffin.engine.table.parquet.RowGroupBuffers;
import io.questdb.std.DirectIntList;
import io.questdb.std.IntIntHashMap;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;

import java.io.Closeable;

public class O3ParquetMergeContext implements Closeable {
    private ObjList<O3ParquetMergeStrategy.MergeAction> actionsBuf;
    private IntList activeColIndices;
    private IntList activeToDecodeIdx;
    private PartitionDescriptor chunkDescriptor;
    private LongList gapO3Ranges;
    private LongList mergeDstBufs;
    private LongList nullBufs;
    private IntIntHashMap parquetColIdToIdx;
    private DirectIntList parquetColumns;
    private ParquetMetaFileReader parquetMetaReader;
    private ParquetPartitionDecoder partitionDecoder;
    private OwnedMemoryPartitionDescriptor partitionDescriptor;
    private PartitionUpdater partitionUpdater;
    private LongList rgO3Ranges;
    private LongList rowGroupBounds;
    private RowGroupBuffers rowGroupBuffers;
    private LongList srcPtrs;
    private IntList tableToParquetIdx;

    public O3ParquetMergeContext() {
        actionsBuf = new ObjList<>();
        activeColIndices = new IntList();
        activeToDecodeIdx = new IntList();
        chunkDescriptor = new PartitionDescriptor();
        gapO3Ranges = new LongList();
        mergeDstBufs = new LongList();
        nullBufs = new LongList();
        parquetColumns = new DirectIntList(64, MemoryTag.NATIVE_O3);
        parquetColIdToIdx = new IntIntHashMap();
        parquetMetaReader = new ParquetMetaFileReader();
        partitionDecoder = ParquetPartitionDecoder.newInstance();
        partitionDescriptor = new OwnedMemoryPartitionDescriptor();
        partitionUpdater = new PartitionUpdater();
        rgO3Ranges = new LongList();
        rowGroupBuffers = new RowGroupBuffers(MemoryTag.NATIVE_PARQUET_PARTITION_UPDATER);
        rowGroupBounds = new LongList();
        srcPtrs = new LongList();
        tableToParquetIdx = new IntList();
    }

    public void clear() {
        activeColIndices.clear();
        activeToDecodeIdx.clear();
        chunkDescriptor.clear();
        gapO3Ranges.clear();
        mergeDstBufs.clear();
        nullBufs.clear();
        parquetColIdToIdx.clear();
        parquetColumns.clear();
        parquetMetaReader.clear();
        partitionDescriptor.clear();
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
        chunkDescriptor = Misc.free(chunkDescriptor);
        gapO3Ranges = null;
        mergeDstBufs = null;
        nullBufs = null;
        parquetColIdToIdx = null;
        parquetColumns = Misc.free(parquetColumns);
        if (parquetMetaReader != null) {
            // Reader does not own its mmap; clear() releases the lazily
            // allocated native handle and zeros all fields.
            parquetMetaReader.clear();
            parquetMetaReader = null;
        }
        partitionDecoder = Misc.free(partitionDecoder);
        partitionDescriptor = Misc.free(partitionDescriptor);
        partitionUpdater = Misc.free(partitionUpdater);
        rgO3Ranges = null;
        rowGroupBuffers = Misc.free(rowGroupBuffers);
        rowGroupBounds = null;
        srcPtrs = null;
        tableToParquetIdx = null;
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

    public PartitionDescriptor getChunkDescriptor() {
        return chunkDescriptor;
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

    public OwnedMemoryPartitionDescriptor getPartitionDescriptor() {
        return partitionDescriptor;
    }

    public PartitionUpdater getPartitionUpdater() {
        return partitionUpdater;
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
}
