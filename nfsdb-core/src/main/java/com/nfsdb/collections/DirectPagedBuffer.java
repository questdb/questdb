package com.nfsdb.collections;
import com.nfsdb.column.DirectInputStream;
import com.nfsdb.exceptions.JournalRuntimeException;
import com.nfsdb.utils.Unsafe;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;

public class DirectPagedBuffer implements Closeable {
    private long pageCapacity;
    private DirectLongList pages;
    private long cachedPageOffsetHi;
    private long cachedPageOffset;

    public DirectPagedBuffer(long pageCapacity) {
        this.pageCapacity = pageCapacity;
        pages = new DirectLongList(1);
        allocateAddress();
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
        int pageIndex = (int) (offset / pageCapacity);
        int pageOffset = (int) (offset % pageCapacity);
        assert pageIndex < pages.size();
        assert pages.get(pageIndex) != 0;

        return pages.get(pageIndex) + pageOffset;
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
        long address = Unsafe.getUnsafe().allocateMemory(pageCapacity);
        pages.add(address);
        return (pages.size() - 1) * pageCapacity;
    }

    public void append(DirectInputStream value) {
        long writeOffset = cachedPageOffset;
        long size = value.getLength();
        if (size < 0 || writeOffset < 0) {
            throw new IndexOutOfBoundsException();
        }

        // Find last page needed.
        int pageIndex = (int) (writeOffset / pageCapacity);
        long pageOffset = writeOffset % pageCapacity;
        int finalPage = (int) (pageIndex + size / pageCapacity + ((size % pageCapacity + pageOffset) > pageCapacity ? 1 : 0));

        // Allocate pages.
        if (finalPage >= pages.size()) {
            for (int i = pages.size(); i < finalPage + 1; i++) {
                pages.add(Unsafe.getUnsafe().allocateMemory(pageCapacity));
            }
        }

        do {
            long writeAddress = pages.get(pageIndex);
            pageOffset = writeOffset % pageCapacity;
            writeAddress += pageOffset;
            long writeSize = Math.min(size, pageCapacity - pageOffset);
            value.copyTo(writeAddress, 0, writeSize);
            size -= writeSize;
            cachedPageOffset += writeSize;
            writeOffset = 0;
            pageIndex++;
        } while (size > 0);
    }

    public void read(OutputStream stream, long offset, long len) throws IOException {
        long position = offset;
        long copied = 0;
        while (copied < len) {
            long address = toAddress(offset);
            long blockEndOffset = getBlockLen(offset);
            long copyEndOffset = Math.min(blockEndOffset, len - copied);
            len += copyEndOffset - position;

            while(position < copyEndOffset) {
                stream.write(Unsafe.getUnsafe().getByte(address + position++));
            }
        }
    }

    public long read(long writeTo, long readOffset, long size) {
        if (size < 0 || readOffset < 0) {
            throw new IndexOutOfBoundsException();
        }

        int pageIndex = (int) (readOffset / pageCapacity);
        long pageOffset = readOffset % pageCapacity;
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

    @Override
    public synchronized void close() throws IOException {
        free();
    }

    private void free() {
        if (pages != null) {
            for (int i = 0; i < pages.size(); i++) {
                long address = pages.get(i);
                if (address != 0) {
                    Unsafe.getUnsafe().freeMemory(pages.get(i));
                }
            }
            pages.close();
            pages = null;
        }
    }

    @Override
     protected void finalize() throws Throwable {
        free();
        super.finalize();
    }

    public long getBlockLen(long offset) {
        return pageCapacity - offset % pageCapacity;
    }
}
