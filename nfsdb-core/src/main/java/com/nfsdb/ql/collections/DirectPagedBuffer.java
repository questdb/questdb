/*
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
 */

package com.nfsdb.ql.collections;

import com.nfsdb.collections.DirectInputStream;
import com.nfsdb.collections.LongList;
import com.nfsdb.collections.Mutable;
import com.nfsdb.exceptions.JournalRuntimeException;
import com.nfsdb.utils.Numbers;
import com.nfsdb.utils.Unsafe;

import java.io.Closeable;
import java.io.IOException;

class DirectPagedBuffer implements Closeable, Mutable {
    private final int pageCapacity;
    private final int mask;
    private final int bits;
    private final LongList pages;
    private long cachePageHi;
    private long cachePageLo;

    public DirectPagedBuffer(int pageCapacity) {
        this.pageCapacity = Numbers.ceilPow2(pageCapacity);
        this.bits = Numbers.msb(this.pageCapacity);
        this.mask = this.pageCapacity - 1;
        pages = new LongList();
        allocateAddress(0);
    }

    public long address(long offset) {
        return pages.get((int) (offset >>> bits)) + (offset & mask);
    }

    public void append(DirectInputStream value) {
        long writeOffset = cachePageLo;
        long size = value.size();
        if (size < 0 || writeOffset < 0) {
            throw new OutOfMemoryError();
        }

        // Find last page needed.
        int pageIndex = (int) (writeOffset >>> bits);
        long pageOffset = writeOffset & mask;
        int finalPage = (int) (pageIndex + (size >>> bits) + (((size & mask) + pageOffset) > pageCapacity ? 1 : 0));

        // Allocate pages.
        if (finalPage >= pages.size()) {
            for (int i = pages.size(); i < finalPage + 1; i++) {
                pages.add(Unsafe.getUnsafe().allocateMemory(pageCapacity));
            }
        }

        do {
            long writeAddress = pages.getQuick(pageIndex);
            pageOffset = writeOffset & mask;
            writeAddress += pageOffset;
            long writeSize = Math.min(size, pageCapacity - pageOffset);
            value.copyTo(writeAddress, 0, writeSize);
            size -= writeSize;
            cachePageLo += writeSize;
            writeOffset = 0;
            pageIndex++;
        } while (size > 0);

        if (cachePageHi <= cachePageLo) {
            cachePageHi = ((cachePageLo >>> bits) << bits) + pageCapacity;
        }
    }

    public long calcOffset(long length) {
        if (cachePageLo + length > cachePageHi) {
            allocateAddress((cachePageLo + length) >>> bits);
        }
        return (cachePageLo += length) - length;
    }

    public long calcOffsetChecked(long length) {
        if (cachePageLo + length > cachePageHi) {
            allocateAddressChecked((cachePageLo + length) >>> bits, length);
        }
        return (cachePageLo += length) - length;
    }

    public void clear() {
        cachePageLo = 0;
        cachePageHi = cachePageLo + pageCapacity;
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
        return pageCapacity - (int) (offset & mask);
    }

    private void allocateAddress(long index) {
        if (index > Integer.MAX_VALUE) {
            throw new OutOfMemoryError();
        }
        cachePageLo = allocatePage((int) index);
        cachePageHi = cachePageLo + pageCapacity;
    }

    private void allocateAddressChecked(long index, long length) {
        if (length > pageCapacity) {
            throw new JournalRuntimeException("Failed to allocate page of length %d. Maximum page size %d", length, pageCapacity);
        }
        allocateAddress(index);
    }

    private long allocatePage(int index) {
        if (index <= pages.size()) {
            pages.extendAndSet(index, Unsafe.getUnsafe().allocateMemory(pageCapacity));
        }
        return index << bits;
    }
}
