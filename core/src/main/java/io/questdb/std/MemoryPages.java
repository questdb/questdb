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

package io.questdb.std;

import io.questdb.griffin.engine.LimitOverflowException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;

import java.io.Closeable;

public class MemoryPages implements Closeable, Mutable {

    private static final Log LOG = LogFactory.getLog(MemoryPages.class);

    private final long pageSize;
    private final long mask;
    private final int bits;
    private final LongList pages = new LongList();
    private long cachePageHi;
    private long cachePageLo;
    private final int maxPages;

    public MemoryPages(long pageSize, int maxPages) {
        this.pageSize = Numbers.ceilPow2(pageSize);
        this.bits = Numbers.msb(this.pageSize);
        this.mask = this.pageSize - 1;
        this.maxPages = maxPages;
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

    @Override
    public void clear() {
        cachePageLo = 0;
        cachePageHi = cachePageLo + pageSize;
    }

    @Override
    public void close() {
        for (int i = 0; i < pages.size(); i++) {
            long address = pages.getQuick(i);
            if (address != 0) {
                Unsafe.free(address, pageSize, MemoryTag.NATIVE_TREE_CHAIN);
            }
        }
        pages.clear();
    }

    public long size() {
        return cachePageLo;
    }

    private void allocate0(long index) {
        if (index > Integer.MAX_VALUE) {
            throw new OutOfMemoryError();
        }

        if (index > maxPages) {
            throw LimitOverflowException.instance().put("Maximum number of pages (").put(maxPages).put(") breached in MemoryPages");
        }

        if (index >= pages.size()) {
            pages.extendAndSet((int) index, Unsafe.malloc(pageSize, MemoryTag.NATIVE_TREE_CHAIN));
            LOG.debug().$("new page [size=").$(pageSize).$(']').$();
        }

        cachePageLo = index << bits;
        cachePageHi = cachePageLo + pageSize;
    }
}