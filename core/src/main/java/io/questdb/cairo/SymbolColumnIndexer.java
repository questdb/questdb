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

package io.questdb.cairo;

import io.questdb.cairo.vm.MemorySRImpl;
import io.questdb.cairo.vm.api.MemoryMA;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;

public class SymbolColumnIndexer implements ColumnIndexer, Mutable {

    private static final long SEQUENCE_OFFSET;
    private final BitmapIndexWriter writer = new BitmapIndexWriter();
    private final MemorySRImpl mem = new MemorySRImpl();
    private long columnTop;
    @SuppressWarnings({"unused", "FieldCanBeLocal", "FieldMayBeFinal"})
    private volatile long sequence = 0L;
    private volatile boolean distressed = false;

    @Override
    public void clear() {
        writer.clear();
    }

    @Override
    public void close() {
        Misc.free(writer);
        Misc.free(mem);
    }

    @Override
    public void distress() {
        distressed = true;
    }

    @Override
    public long getFd() {
        return mem.getFd();
    }

    @Override
    public long getSequence() {
        return sequence;
    }

    @Override
    public void refreshSourceAndIndex(long loRow, long hiRow) {
        mem.updateSize();
        index(mem, loRow, hiRow);
    }

    @Override
    public void index(MemoryR mem, long loRow, long hiRow) {
        // while we may have to read column starting with zero offset
        // index values have to be adjusted to partition-level row id
        writer.rollbackConditionally(loRow);
        for (long lo = Math.max(loRow,  columnTop); lo < hiRow; lo++) {
            writer.add(TableUtils.toIndexKey(mem.getInt((lo - columnTop) * Integer.BYTES)), lo);
        }
        writer.setMaxValue(hiRow - 1);
    }

    @Override
    public BitmapIndexWriter getWriter() {
        return writer;
    }

    @Override
    public boolean isDistressed() {
        return distressed;
    }

    @Override
    public void configureFollowerAndWriter(
            CairoConfiguration configuration,
            Path path,
            CharSequence name,
            long columnNameTxn,
            MemoryMA columnMem,
            long columnTop
    ) {
        this.columnTop = columnTop;
        try {
            this.writer.of(
                    configuration,
                    path,
                    name,
                    columnNameTxn,
                    configuration.getDataIndexKeyAppendPageSize(),
                    configuration.getDataIndexValueAppendPageSize()
            );
            this.mem.of(columnMem, MemoryTag.MMAP_INDEX_SLIDER);
        } catch (Throwable e) {
            this.close();
            throw e;
        }
    }

    @Override
    public void configureWriter(CairoConfiguration configuration, Path path, CharSequence name, long columnNameTxn, long columnTop) {
        this.columnTop = columnTop;
        try {
            this.writer.of(
                    configuration,
                    path,
                    name,
                    columnNameTxn,
                    configuration.getDataIndexKeyAppendPageSize(),
                    configuration.getDataIndexValueAppendPageSize()
            );
        } catch (Throwable e) {
            this.close();
            throw e;
        }
    }

    @Override
    public void closeSlider() {
        mem.close();
    }

    @Override
    public void rollback(long maxRow) {
        this.writer.rollbackValues(maxRow);
    }

    @Override
    public boolean tryLock(long expectedSequence) {
        return Unsafe.cas(this, SEQUENCE_OFFSET, expectedSequence, expectedSequence + 1);
    }

    static {
        SEQUENCE_OFFSET = Unsafe.getFieldOffset(SymbolColumnIndexer.class, "sequence");
    }
}
