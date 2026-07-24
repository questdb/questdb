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
import io.questdb.std.Unsafe;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8StringSink;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;

/**
 * Reusable scratch for parquet row-group decoding and type materialization.
 * Instances belong to a worker and are reset between partitions; callers must
 * not allocate one per row group or share one between concurrent workers.
 */
public class ParquetConversionContext implements Closeable {
    private IntList activeColIndices;
    private IntList activeToDecodeIdx;
    private PartitionDescriptor chunkDescriptor;
    private CairoConfiguration configuration;
    private LongList convertedPtrs;
    private Decimal128 decimal128Buf;
    private Decimal256 decimal256Buf;
    private Decimal64 decimal64Buf;
    private int lastDecodedColumnCount;
    private final int memoryTag;
    private IntIntHashMap parquetColIdToIdx;
    private DirectIntList parquetColumns;
    private ParquetMetaFileReader parquetMetaReader;
    private ParquetPartitionDecoder partitionDecoder;
    private PartitionUpdater partitionUpdater;
    private RowGroupBuffers rowGroupBuffers;
    private IntList tableToParquetIdx;
    private LongList tmpBufs;
    private StringSink utf16Sink;
    private Utf8StringSink utf8Sink;

    public ParquetConversionContext(int memoryTag) {
        this.memoryTag = memoryTag;
        activeColIndices = new IntList();
        activeToDecodeIdx = new IntList();
        chunkDescriptor = new PartitionDescriptor();
        convertedPtrs = new LongList();
        decimal128Buf = new Decimal128();
        decimal256Buf = new Decimal256();
        decimal64Buf = new Decimal64();
        parquetColIdToIdx = new IntIntHashMap();
        parquetColumns = new DirectIntList(64, memoryTag);
        parquetMetaReader = new ParquetMetaFileReader();
        partitionUpdater = new PartitionUpdater();
        rowGroupBuffers = new RowGroupBuffers(MemoryTag.NATIVE_PARQUET_PARTITION_UPDATER);
        tableToParquetIdx = new IntList();
        tmpBufs = new LongList();
        utf16Sink = new StringSink();
        utf8Sink = new Utf8StringSink();
    }

    public void clear() {
        activeColIndices.clear();
        activeToDecodeIdx.clear();
        chunkDescriptor.clear();
        convertedPtrs.clear();
        lastDecodedColumnCount = 0;
        parquetColIdToIdx.clear();
        parquetColumns.clear();
        parquetMetaReader.clear();
        tableToParquetIdx.clear();
    }

    @Override
    public void close() {
        activeColIndices = null;
        activeToDecodeIdx = null;
        chunkDescriptor = Misc.free(chunkDescriptor);
        configuration = null;
        // Holds pointer copies into rowGroupBuffers / tmpBufs, not owned buffers.
        convertedPtrs = null;
        decimal128Buf = null;
        decimal256Buf = null;
        decimal64Buf = null;
        parquetColIdToIdx = null;
        parquetColumns = Misc.free(parquetColumns);
        partitionDecoder = Misc.free(partitionDecoder);
        if (parquetMetaReader != null) {
            parquetMetaReader.clear();
            parquetMetaReader = null;
        }
        partitionUpdater = Misc.free(partitionUpdater);
        rowGroupBuffers = Misc.free(rowGroupBuffers);
        tableToParquetIdx = null;
        tmpBufs = freeNativePairsAndNull(tmpBufs);
        utf16Sink = null;
        utf8Sink = null;
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

    /**
     * Per-row-group target pointers, laid out as
     * {@code [dataPtr, dataSize, auxPtr, auxSize]} per active column.
     */
    public LongList getConvertedPtrs(int columnCount) {
        final int requiredLen = columnCount * 4;
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

    @TestOnly
    public int getLastDecodedColumnCount() {
        return lastDecodedColumnCount;
    }

    @TestOnly
    public long getLastDecodedRowGroupBytes() {
        return rowGroupBuffers.sumChunkBytes(0, lastDecodedColumnCount);
    }

    public int getMemoryTag() {
        return memoryTag;
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

    public ParquetPartitionDecoder getPartitionDecoder(CairoConfiguration configuration) {
        if (partitionDecoder == null || this.configuration != configuration) {
            partitionDecoder = Misc.free(partitionDecoder);
            partitionDecoder = configuration.newParquetPartitionDecoder();
            this.configuration = configuration;
        }
        return partitionDecoder;
    }

    public PartitionUpdater getPartitionUpdater() {
        return partitionUpdater;
    }

    public RowGroupBuffers getRowGroupBuffers() {
        return rowGroupBuffers;
    }

    public IntList getTableToParquetIdx(int columnCount) {
        tableToParquetIdx.setAll(columnCount, -1);
        return tableToParquetIdx;
    }

    /**
     * Per-row-group owned buffers, laid out as two order-independent
     * {@code [address, size]} pairs per active column.
     */
    public LongList getTmpBufs(int activeColumnCount) {
        final int requiredLen = activeColumnCount * 4;
        tmpBufs.setPos(requiredLen);
        tmpBufs.fill(0, requiredLen, 0);
        return tmpBufs;
    }

    public StringSink getUtf16Sink() {
        return utf16Sink;
    }

    public Utf8StringSink getUtf8Sink() {
        return utf8Sink;
    }

    /**
     * Releases native handles and file descriptors that borrow from the current
     * parquet mappings while retaining the Java scratch containers for reuse.
     */
    public void releaseResources() {
        if (partitionDecoder != null) {
            partitionDecoder.close();
        }
        if (partitionUpdater != null) {
            partitionUpdater.close();
        }
    }

    void setLastDecodedColumnCount(int lastDecodedColumnCount) {
        this.lastDecodedColumnCount = lastDecodedColumnCount;
    }

    protected final LongList freeNativePairsAndNull(LongList list) {
        freeNativePairs(list);
        return null;
    }

    void freeNativePairs(LongList list) {
        if (list != null) {
            final int n = list.size();
            for (int i = 0; i + 1 < n; i += 2) {
                final long address = list.getQuick(i);
                if (address != 0) {
                    Unsafe.free(address, list.getQuick(i + 1), memoryTag);
                    list.setQuick(i, 0);
                    list.setQuick(i + 1, 0);
                }
            }
        }
    }
}
