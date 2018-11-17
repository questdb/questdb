/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.cairo;

import com.questdb.std.Misc;
import com.questdb.std.Unsafe;
import com.questdb.std.str.Path;

import java.io.Closeable;

class SymbolColumnIndexer implements ColumnIndexer, Closeable {

    private static final long SEQUENCE_OFFSET;
    private final BitmapIndexWriter writer = new BitmapIndexWriter();
    private final SlidingWindowMemory mem = new SlidingWindowMemory();
    private long columnTop;
    @SuppressWarnings({"unused", "FieldCanBeLocal"})
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
    public void index(long loRow, long hiRow) {
        mem.updateSize();
        // while we may have to read column starting with zero offset
        // index values have to be adjusted to partition-level row id
        for (long lo = loRow - columnTop; lo < hiRow; lo++) {
            writer.add(TableUtils.toIndexKey(mem.getInt(lo * 4)), lo + columnTop);
        }
    }

    @Override
    public boolean isDistressed() {
        return distressed;
    }

    @Override
    public void of(CairoConfiguration configuration, Path path, CharSequence name, AppendMemory mem1, AppendMemory mem2, long columnTop) {
        this.columnTop = columnTop;
        try {
            this.writer.of(configuration, path, name);
            this.mem.of(mem1);
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

    static {
        SEQUENCE_OFFSET = Unsafe.getFieldOffset(SymbolColumnIndexer.class, "sequence");
    }
}
