/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.cairo.sql;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.std.*;

public class PageAddressCache implements Mutable {

    private final long cacheSizeThreshold;
    // Index remapping for variable size columns.
    private final IntList varSizeColumnIndexes = new IntList();
    // Index page addresses and page sizes are stored only for variable length columns.
    private LongList auxPageAddresses = new LongList();
    private int columnCount;
    private LongList pageAddresses = new LongList();
    private LongList pageRowIdOffsets = new LongList();
    private LongList pageSizes = new LongList();
    private int varSizeColumnCount;

    public PageAddressCache(CairoConfiguration configuration) {
        cacheSizeThreshold = configuration.getSqlJitPageAddressCacheThreshold() / Long.BYTES;
    }

    public void add(int frameIndex, @Transient PageFrame frame) {
        if (pageAddresses.size() >= columnCount * (frameIndex + 1)) {
            return; // The page frame is already cached
        }
        for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
            pageAddresses.add(frame.getPageAddress(columnIndex));
            int varSizeColumnIndex = varSizeColumnIndexes.getQuick(columnIndex);
            if (varSizeColumnIndex > -1) {
                auxPageAddresses.add(frame.getIndexPageAddress(columnIndex));
                pageSizes.add(frame.getPageSize(columnIndex));
            }
        }
        pageRowIdOffsets.add(Rows.toRowID(frame.getPartitionIndex(), frame.getPartitionLo()));
    }

    @Override
    public void clear() {
        varSizeColumnIndexes.clear();
        if (pageAddresses.size() < cacheSizeThreshold) {
            pageAddresses.clear();
            auxPageAddresses.clear();
            pageSizes.clear();
            pageRowIdOffsets.clear();
        } else {
            pageAddresses = new LongList();
            auxPageAddresses = new LongList();
            pageSizes = new LongList();
            pageRowIdOffsets = new LongList();
        }
    }

    public long getAuxPageAddress(int frameIndex, int columnIndex) {
        assert auxPageAddresses.size() >= varSizeColumnCount * (frameIndex + 1);
        int varSizeColumnIndex = varSizeColumnIndexes.getQuick(columnIndex);
        assert varSizeColumnIndex > -1;
        return auxPageAddresses.getQuick(varSizeColumnCount * frameIndex + varSizeColumnIndex);
    }

    public int getColumnCount() {
        return columnCount;
    }

    public long getPageAddress(int frameIndex, int columnIndex) {
        assert pageAddresses.size() >= columnCount * (frameIndex + 1);
        return pageAddresses.getQuick(columnCount * frameIndex + columnIndex);
    }

    public long getPageSize(int frameIndex, int columnIndex) {
        assert pageSizes.size() >= varSizeColumnCount * (frameIndex + 1);
        int varSizeColumnIndex = varSizeColumnIndexes.getQuick(columnIndex);
        assert varSizeColumnIndex > -1;
        return pageSizes.getQuick(varSizeColumnCount * frameIndex + varSizeColumnIndex);
    }

    public boolean hasColumnTops(int frameIndex) {
        assert pageAddresses.size() >= columnCount * (frameIndex + 1);
        for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
            int varSizeColumnIndex = varSizeColumnIndexes.getQuick(columnIndex);
            if (pageAddresses.getQuick(columnCount * frameIndex + columnIndex) == 0
                    // VARCHAR column that contains short strings will have zero data vector,
                    // so for such columns we also need to check that the aux (index) vector is zero.
                    && (varSizeColumnIndex == -1 || auxPageAddresses.getQuick(varSizeColumnCount * frameIndex + varSizeColumnIndex) == 0)) {
                return true;
            }
        }
        return false;
    }

    public boolean isVarSizeColumn(int columnIndex) {
        return varSizeColumnIndexes.getQuick(columnIndex) > -1;
    }

    public void of(@Transient RecordMetadata metadata) {
        this.columnCount = metadata.getColumnCount();
        this.varSizeColumnIndexes.setAll(columnCount, -1);
        this.varSizeColumnCount = 0;
        for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
            final int columnType = metadata.getColumnType(columnIndex);
            if (ColumnType.isVarSize(columnType)) {
                varSizeColumnIndexes.setQuick(columnIndex, varSizeColumnCount++);
            }
        }
    }

    public long toTableRowID(int frameIndex, long index) {
        return pageRowIdOffsets.get(frameIndex) + index;
    }
}
