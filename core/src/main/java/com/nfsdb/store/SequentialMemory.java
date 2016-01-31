/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.nfsdb.store;

import com.nfsdb.misc.Numbers;
import com.nfsdb.misc.Unsafe;
import com.nfsdb.std.LongList;
import com.nfsdb.std.Mutable;

import java.io.Closeable;
import java.io.IOException;

public class SequentialMemory implements Closeable, Mutable {
    private final int pageSize;
    private final int mask;
    private final int bits;
    private final LongList pages;
    private long cachePageHi;
    private long cachePageLo;

    public SequentialMemory(int pageSize) {
        this.pageSize = Numbers.ceilPow2(pageSize);
        this.bits = Numbers.msb(this.pageSize);
        this.mask = this.pageSize - 1;
        pages = new LongList();
        allocateAddress(0);
    }

    public long addressOf(long offset) {
        return pages.getQuick((int) (offset >>> bits)) + (offset & mask);
    }

    public long allocate(long length) {
        if (cachePageLo + length > cachePageHi) {
            allocateAddress((cachePageLo + length) >>> bits);
        }
        return (cachePageLo += length) - length;
    }

    public void clear() {
        cachePageLo = 0;
        cachePageHi = cachePageLo + pageSize;
    }

    @Override
    public void close() throws IOException {
        for (int i = 0; i < pages.size(); i++) {
            long address = pages.getQuick(i);
            if (address != 0) {
                Unsafe.getUnsafe().freeMemory(address);
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

    private void allocateAddress(long index) {
        if (index > Integer.MAX_VALUE) {
            throw new OutOfMemoryError();
        }

        if (index <= pages.size()) {
            pages.extendAndSet((int) index, Unsafe.getUnsafe().allocateMemory(pageSize));
        }

        cachePageLo = index << bits;
        cachePageHi = cachePageLo + pageSize;
    }
}