/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
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

package com.questdb.std;

import java.io.Closeable;

public class MemoryPages implements Closeable, Mutable {
    private final int pageSize;
    private final int mask;
    private final int bits;
    private final LongList pages = new LongList();
    private long cachePageHi;
    private long cachePageLo;

    public MemoryPages(int pageSize) {
        this.pageSize = Numbers.ceilPow2(pageSize);
        this.bits = Numbers.msb(this.pageSize);
        this.mask = this.pageSize - 1;
        allocate0(0);
    }

    public long addressOf(long offset) {
        return pages.getQuick((int) (offset >>> bits)) + (offset & mask);
    }

    public long allocate(long length) {
        return addressOf(allocateOffset(length));
    }

    public long allocateOffset(long length) {
        if (cachePageLo + length > cachePageHi) {
            allocate0((cachePageLo + length) >>> bits);
        }
        return (cachePageLo += length) - length;
    }

    public void clear() {
        cachePageLo = 0;
        cachePageHi = cachePageLo + pageSize;
    }

    @Override
    public void close() {
        for (int i = 0; i < pages.size(); i++) {
            long address = pages.getQuick(i);
            if (address != 0) {
                Unsafe.free(address, pageSize);
            }
        }
        pages.clear();
    }

    public int pageRemaining(long offset) {
        return pageSize - (int) (offset & mask);
    }

    public int pageSize() {
        return pageSize;
    }

    public long size() {
        return cachePageLo;
    }

    private void allocate0(long index) {
        if (index > Integer.MAX_VALUE) {
            throw new OutOfMemoryError();
        }

        if (index >= pages.size()) {
            pages.extendAndSet((int) index, Unsafe.malloc(pageSize));
        }

        cachePageLo = index << bits;
        cachePageHi = cachePageLo + pageSize;
    }
}