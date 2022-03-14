/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.Mutable;
import io.questdb.std.Transient;

public class PageAddressCache implements Mutable {

    private final long cacheSizeThreshold;
    private int columnCount;
    private int varLenColumnCount;

    // Index remapping for variable length columns.
    private final IntList varLenColumnIndexes = new IntList();

    private LongList pageAddresses = new LongList();
    // Index page addresses and page sizes are stored only for variable length columns.
    private LongList indexPageAddresses = new LongList();
    private LongList pageSizes = new LongList();

    public PageAddressCache(CairoConfiguration configuration) {
        cacheSizeThreshold = configuration.getSqlJitPageAddressCacheThreshold() / Long.BYTES;
    }

    public void of(@Transient RecordMetadata metadata) {
        this.columnCount = metadata.getColumnCount();
        this.varLenColumnIndexes.setAll(columnCount, -1);
        this.varLenColumnCount = 0;
        for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
            final int columnType = metadata.getColumnType(columnIndex);
            if (ColumnType.isVariableLength(columnType)) {
                varLenColumnIndexes.setQuick(columnIndex, varLenColumnCount++);
            }
        }
    }

    @Override
    public void clear() {
        varLenColumnIndexes.clear();
        if (pageAddresses.size() > cacheSizeThreshold) {
            pageAddresses.clear();
            indexPageAddresses.clear();
            pageSizes.clear();
        } else {
            pageAddresses = new LongList();
            indexPageAddresses = new LongList();
            pageSizes = new LongList();
        }
    }

    public void add(int frameIndex, @Transient PageFrame frame) {
        if (pageAddresses.size() >= columnCount * (frameIndex + 1)) {
            return; // The page frame is already cached
        }
        for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
            pageAddresses.add(frame.getPageAddress(columnIndex));
            int varLenColumnIndex = varLenColumnIndexes.getQuick(columnIndex);
            if (varLenColumnIndex > -1) {
                indexPageAddresses.add(frame.getIndexPageAddress(columnIndex));
                pageSizes.add(frame.getPageSize(columnIndex));
            }
        }
    }

    public long getPageAddress(int frameIndex, int columnIndex) {
        assert pageAddresses.size() >= columnCount * (frameIndex + 1);
        return pageAddresses.getQuick(columnCount * frameIndex + columnIndex);
    }

    public long getIndexPageAddress(int frameIndex, int columnIndex) {
        assert indexPageAddresses.size() >= varLenColumnCount * (frameIndex + 1);
        int varLenColumnIndex = varLenColumnIndexes.getQuick(columnIndex);
        assert varLenColumnIndex > -1;
        return indexPageAddresses.getQuick(varLenColumnCount * frameIndex + varLenColumnIndex);
    }

    public long getPageSize(int frameIndex, int columnIndex) {
        assert pageSizes.size() >= varLenColumnCount * (frameIndex + 1);
        int varLenColumnIndex = varLenColumnIndexes.getQuick(columnIndex);
        assert varLenColumnIndex > -1;
        return pageSizes.getQuick(varLenColumnCount * frameIndex + varLenColumnIndex);
    }

    public boolean hasColumnTops(int frameIndex) {
        assert pageAddresses.size() >= columnCount * (frameIndex + 1);
        for (int columnIndex = 0, baseIndex = columnCount * frameIndex; columnIndex < columnCount; columnIndex++) {
            if (pageAddresses.getQuick(baseIndex + columnIndex) == 0) {
                return true;
            }
        }
        return false;
    }

    public int getColumnCount() {
        return columnCount;
    }
}
