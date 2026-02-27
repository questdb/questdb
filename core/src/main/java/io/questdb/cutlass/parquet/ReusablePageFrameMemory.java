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

import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntHashSet;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.QuietCloseable;

/**
 * Reusable PageFrameMemory backed by DirectLongLists. Updated in-place per frame
 * to avoid allocating a new object on every call (zero-GC on data path).
 */
class ReusablePageFrameMemory implements PageFrameMemory, Mutable, QuietCloseable {
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
