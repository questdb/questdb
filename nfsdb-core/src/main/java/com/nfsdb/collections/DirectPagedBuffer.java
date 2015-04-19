/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
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

package com.nfsdb.collections;

import com.nfsdb.exceptions.JournalRuntimeException;
import com.nfsdb.utils.Numbers;
import com.nfsdb.utils.Unsafe;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;

public class DirectPagedBuffer implements Closeable {
    private final int pageCapacity;
    private final int mask;
    private final int bits;
    private DirectLongList pages;
    private long cachedPageOffsetHi;
    private long cachedPageOffset;

    public DirectPagedBuffer(int pageCapacity) {
        this.pageCapacity = Numbers.ceilPow2(pageCapacity);
        this.bits = 31 - Integer.numberOfLeadingZeros(this.pageCapacity);
        this.mask = this.pageCapacity - 1;
        pages = new DirectLongList(1);
        allocateAddress();
    }

    public void append(DirectInputStream value) {
        long writeOffset = cachedPageOffset;
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
            cachedPageOffset += writeSize;
            writeOffset = 0;
            pageIndex++;
        } while (size > 0);
    }

    public long getBlockLen(long offset) {
        return pageCapacity - (offset & mask);
    }

    public long getWriteOffsetQuick(long length) {
        if (cachedPageOffset + length <= cachedPageOffsetHi) {
            // Increment currentAddress and return original value
            return (cachedPageOffset += length) - length;
        } else {
            cachedPageOffset = allocateAddress();
            return (cachedPageOffset += length) - length;
        }
    }

    public long getWriteOffsetWithChecks(long length) {
        if (cachedPageOffset + length <= cachedPageOffsetHi) {
            return (cachedPageOffset += length) - length;
        } else {
            cachedPageOffset = allocateAddressChecked(length);
            return (cachedPageOffset += length) - length;
        }
    }

    public long toAddress(long offset) {
        return pages.get((int) (offset >> bits)) + (offset & mask);
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

    private long allocateAddressChecked(long length) {
        if (length > pageCapacity) {
            throw new JournalRuntimeException("Failed to allocate page of length %d. Maximum page size %d", length, pageCapacity);
        }
        return allocateAddress();
    }

    private long allocateAddress() {
        long cachedPageOffsetLo = allocatePage();
        cachedPageOffsetHi = cachedPageOffsetLo + pageCapacity;
        return cachedPageOffsetLo;
    }

    private long allocatePage() {
        pages.add(Unsafe.getUnsafe().allocateMemory(pageCapacity));
        return (pages.size() - 1) * pageCapacity;
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
            pages.close();
            pages = null;
        }
    }
}
