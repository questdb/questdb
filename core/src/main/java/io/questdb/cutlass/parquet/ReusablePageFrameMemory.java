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

import io.questdb.cairo.NullPolicy;
import io.questdb.cairo.sql.ColumnVectorDescriptor;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntHashSet;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;

/**
 * Reusable PageFrameMemory backed by DirectLongLists. Updated in-place per frame
 * to avoid allocating a new object on every call (zero-GC on data path).
 */
class ReusablePageFrameMemory implements PageFrameMemory, Mutable, QuietCloseable {
    private final DirectLongList auxPageAddresses = new DirectLongList(32, MemoryTag.NATIVE_PARQUET_EXPORTER);
    private final DirectLongList auxPageSizes = new DirectLongList(32, MemoryTag.NATIVE_PARQUET_EXPORTER);
    private final ObjList<NullPolicy> columnNullPolicies = new ObjList<>();
    private final ColumnVectorDescriptor columnVectors = new ColumnVectorDescriptor();
    private final DirectLongList nullCounts = new DirectLongList(32, MemoryTag.NATIVE_PARQUET_EXPORTER);
    private final DirectLongList pageAddresses = new DirectLongList(32, MemoryTag.NATIVE_PARQUET_EXPORTER);
    private final DirectLongList pageSizes = new DirectLongList(32, MemoryTag.NATIVE_PARQUET_EXPORTER);
    private final DirectLongList validityAddresses = new DirectLongList(32, MemoryTag.NATIVE_PARQUET_EXPORTER);
    private final DirectLongList validityBitOffsets = new DirectLongList(32, MemoryTag.NATIVE_PARQUET_EXPORTER);
    private int columnCount;
    private boolean hasColumnTops;
    private long rowIdOffset;

    @Override
    public void clear() {
        pageAddresses.clear();
        auxPageAddresses.clear();
        pageSizes.clear();
        auxPageSizes.clear();
        validityAddresses.clear();
        validityBitOffsets.clear();
        nullCounts.clear();
        columnVectors.clear();
    }

    @Override
    public void close() {
        Misc.free(pageAddresses);
        Misc.free(auxPageAddresses);
        Misc.free(pageSizes);
        Misc.free(auxPageSizes);
        Misc.free(validityAddresses);
        Misc.free(validityBitOffsets);
        Misc.free(nullCounts);
        columnVectors.clear();
    }

    @Override
    public int getColumnCount() {
        return columnCount;
    }

    @Override
    public ColumnVectorDescriptor getColumnVectorDescriptor() {
        return columnVectors;
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
    public PageFrameMemoryPool getPool() {
        // Not backed by a PageFrameMemoryPool; records bound from here always rebind.
        return null;
    }

    @Override
    public long getRowIdOffset() {
        return rowIdOffset;
    }

    @Override
    public NullPolicy getSourceColumnNullPolicy(int columnIndex) {
        return null;
    }

    @Override
    public int getSourceColumnType(int columnIndex) {
        return -1;
    }

    @Override
    public boolean hasColumnTops() {
        return hasColumnTops;
    }

    @Override
    public boolean hasColumnTypeCasts() {
        return false;
    }

    /**
     * Copies a frame of the page frame cursor whose metadata {@link #ofMetadata} took.
     */
    public void of(PageFrame frame) {
        this.columnCount = frame.getColumnCount();
        this.rowIdOffset = frame.getPartitionLo();

        pageAddresses.clear();
        auxPageAddresses.clear();
        pageSizes.clear();
        auxPageSizes.clear();
        validityAddresses.clear();
        validityBitOffsets.clear();
        nullCounts.clear();

        hasColumnTops = false;
        for (int col = 0; col < columnCount; col++) {
            long addr = frame.getDataAddress(col);
            pageAddresses.add(addr);
            pageSizes.add(frame.getDataSize(col));
            auxPageAddresses.add(frame.getAuxAddress(col));
            auxPageSizes.add(frame.getAuxSize(col));
            validityAddresses.add(frame.getValidityAddress(col));
            validityBitOffsets.add(frame.getValidityBitOffset(col));
            nullCounts.add(frame.getNullCount(col));
            if (addr == 0) {
                hasColumnTops = true;
            }
        }
        columnVectors.of(
                pageAddresses,
                pageSizes,
                auxPageAddresses,
                auxPageSizes,
                validityAddresses,
                validityBitOffsets,
                nullCounts,
                columnNullPolicies,
                0,
                columnCount
        );
    }

    /**
     * Takes the per-column NULL policies of the page frame cursor's metadata, once at setup.
     */
    public void ofMetadata(RecordMetadata metadata) {
        columnNullPolicies.clear();
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            columnNullPolicies.add(metadata.getColumnNullPolicy(i));
        }
    }

    @Override
    public boolean populateRemainingColumns(IntHashSet filterColumnIndexes, DirectLongList filteredRows, boolean fillWithNulls) {
        return false;
    }
}
