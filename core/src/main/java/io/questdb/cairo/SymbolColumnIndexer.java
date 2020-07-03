/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

import io.questdb.std.Misc;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;

import java.io.Closeable;

class SymbolColumnIndexer implements ColumnIndexer, Closeable {

    private static final long SEQUENCE_OFFSET;

    static {
        SEQUENCE_OFFSET = Unsafe.getFieldOffset(SymbolColumnIndexer.class, "sequence");
    }

    private final BitmapIndexWriter writer = new BitmapIndexWriter();
    private final SlidingWindowMemory mem = new SlidingWindowMemory();
    private long columnTop;
    @SuppressWarnings({"unused", "FieldCanBeLocal", "FieldMayBeFinal"})
    private volatile long sequence = 0L;
    private volatile boolean distressed = false;

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
    public void index(VirtualMemory mem, long loRow, long hiRow) {
        // while we may have to read column starting with zero offset
        // index values have to be adjusted to partition-level row id
        for (long lo = loRow - columnTop; lo < hiRow; lo++) {
            writer.add(TableUtils.toIndexKey(mem.getInt(lo * Integer.BYTES)), lo + columnTop);
        }
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
            AppendMemory columnMem,
            long columnTop
    ) {
        this.columnTop = columnTop;
        try {
            this.writer.of(configuration, path, name);
            this.mem.of(columnMem);
        } catch (CairoException e) {
            this.close();
            throw e;
        }
    }

    @Override
    public void configureWriter(CairoConfiguration configuration, Path path, CharSequence name, long columnTop) {
        this.columnTop = columnTop;
        try {
            this.writer.of(configuration, path, name);
        } catch (CairoException e) {
            this.close();
            throw e;
        }
    }

    @Override
    public void rollback(long maxRow) {
        this.writer.rollbackValues(maxRow);
    }

    @Override
    public boolean tryLock(long expectedSequence) {
        return Unsafe.cas(this, SEQUENCE_OFFSET, expectedSequence, expectedSequence + 1);
    }
}
