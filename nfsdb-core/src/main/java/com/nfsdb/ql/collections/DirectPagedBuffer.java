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
import java.io.OutputStream;

class DirectPagedBuffer implements Closeable, Mutable {
    private final int pageCapacity;
    private final int mask;
    private final int bits;
    private LongList pages;
    private long cachePageHi;
    private long cachePageLo;

    public DirectPagedBuffer(int pageCapacity) {
        this.pageCapacity = Numbers.ceilPow2(pageCapacity);
        this.bits = 31 - Integer.numberOfLeadingZeros(this.pageCapacity);
        this.mask = this.pageCapacity - 1;
        pages = new LongList();
        allocateAddress();
    }

    public void append(DirectInputStream value) {
        long writeOffset = cachePageLo;
        long size = value.getLength();
        if (size < 0 || writeOffset < 0) {
            throw new IndexOutOfBoundsException();
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
            long writeAddress = pages.get(pageIndex);
            pageOffset = writeOffset & mask;
            writeAddress += pageOffset;
            long writeSize = Math.min(size, pageCapacity - pageOffset);
            value.copyTo(writeAddress, 0, writeSize);
            size -= writeSize;
            cachePageLo += writeSize;
            writeOffset = 0;
            pageIndex++;
        } while (size > 0);
    }

    public void clear() {
        cachePageLo = pages.get(0);
        cachePageHi = cachePageLo + pageCapacity;
    }

    @Override
    public void close() throws IOException {
        if (pages != null) {
            for (int i = 0; i < pages.size(); i++) {
                long address = pages.get(i);
                if (address != 0) {
                    Unsafe.getUnsafe().freeMemory(address);
                }
            }
            pages = null;
        }
    }

    public long getBlockLen(long offset) {
        return pageCapacity - (offset & mask);
    }

    public long getWriteOffsetQuick(long length) {
        if (cachePageLo + length > cachePageHi) {
            allocateAddress();
        }
        return (cachePageLo += length) - length;
    }

    public long getWriteOffsetWithChecks(long length) {
        if (cachePageLo + length > cachePageHi) {
            allocateAddressChecked(length);
        }
        return (cachePageLo += length) - length;
    }

    public long toAddress(long offset) {
        return pages.get((int) (offset >> bits)) + (offset & mask);
    }

    public long write(long writeTo, long readOffset, long size) {
        if (size < 0 || readOffset < 0) {
            throw new IndexOutOfBoundsException();
        }

        int pageIndex = (int) (readOffset >>> bits);
        long pageOffset = readOffset & mask;
        long readLen = 0;

        if (pageIndex >= pages.size()) {
            return readLen;
        }

        do {
            long readAddress = pages.get(pageIndex);
            if (readAddress == 0) {
                return readLen;
            }

            long toRead = Math.min(size, pageCapacity - pageOffset);
            Unsafe.getUnsafe().copyMemory(readAddress + pageOffset, writeTo, toRead);
            writeTo += toRead;
            readLen += toRead;
            size -= toRead;
            pageOffset = 0;
            pageIndex++;
        }
        while (size > 0);
        return readLen;
    }

    public void write(OutputStream stream, long offset, long len) throws IOException {
        long position = offset;
        long copied = 0;
        while (copied < len) {
            long address = toAddress(offset);
            long blockEndOffset = getBlockLen(offset);
            long copyEndOffset = Math.min(blockEndOffset, len - copied);
            len += copyEndOffset - position;

            while (position < copyEndOffset) {
                stream.write(Unsafe.getUnsafe().getByte(address + position++));
            }
        }
    }

    private void allocateAddress() {
        cachePageLo = allocatePage();
        cachePageHi = cachePageLo + pageCapacity;
    }

    private void allocateAddressChecked(long length) {
        if (length > pageCapacity) {
            throw new JournalRuntimeException("Failed to allocate page of length %d. Maximum page size %d", length, pageCapacity);
        }
        allocateAddress();
    }

    private long allocatePage() {
        pages.add(Unsafe.getUnsafe().allocateMemory(pageCapacity));
        return (pages.size() - 1) << bits;
    }
}
