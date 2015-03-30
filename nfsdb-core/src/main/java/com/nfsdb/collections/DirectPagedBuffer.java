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
    private long cachedPageOffsetLo;
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
        cachedPageOffsetLo = allocatePage();
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
        for(int i = 0; i < pages.size(); i++) {
            long address = pages.get(i);
            if (address != 0) {
                Unsafe.getUnsafe().freeMemory(pages.get(i));
            }
        }
        pages.close();
    }

    @Override
     protected void finalize() throws Throwable {
        free();
        super.finalize();
    }

    public long getBlockLen(long offset) {
        return pageCapacity - offset % pageCapacity;
    }

/*
    public byte getByte(long readOffset) {
        int pageIndex = (int) (readOffset / pageCapacity);
        long pageOffset = readOffset % pageCapacity;
        if (pageIndex >= pages.size()) {
            throw new IndexOutOfBoundsException();
        }

        long readAddress = pages.get(pageIndex);
        if (readAddress == 0) {
            throw new IndexOutOfBoundsException();
        }

        return Unsafe.getUnsafe().getByte(readAddress + pageOffset);
    }


    public int readInt(long l) {
        this.read(localBufferAddress, l, 4);
        return Unsafe.getUnsafe().getInt(localBufferAddress);
    }

    public long readLong(long l) {
        this.read(localBufferAddress, l, 8);
        return Unsafe.getUnsafe().getLong(localBufferAddress);
    }

    public double readDouble(long l) {
        this.read(localBufferAddress, l, 8);
        return Unsafe.getUnsafe().getDouble(localBufferAddress);
    }

    public short readShort(long l) {
        this.read(localBufferAddress, l, 2);
        return Unsafe.getUnsafe().getShort(localBufferAddress);
    }

    public char readChar(long l) {
        this.read(localBufferAddress, l, 2);
        return Unsafe.getUnsafe().getChar(localBufferAddress);
    }

    public int writeByte(byte value, long offset) {
        Unsafe.getUnsafe().putByte(localBufferAddress, value);
        this.append(localBufferAddress, offset, 1);
        return 1;
    }

    public int writeDouble(double value, long offset) {
        Unsafe.getUnsafe().putDouble(localBufferAddress, value);
        this.append(localBufferAddress, offset, 8);
        return 8;
    }

    public int writeInt(int value, long offset) {
        Unsafe.getUnsafe().putInt(localBufferAddress, value);
        this.append(localBufferAddress, offset, 4);
        return 4;
    }

    public int writeLong(long value, long offset) {
        Unsafe.getUnsafe().putLong(localBufferAddress, value);
        this.append(localBufferAddress, offset, 8);
        return 8;
    }

    public int writeShort(short value, long offset) {
        Unsafe.getUnsafe().putShort(localBufferAddress, value);
        this.append(localBufferAddress, offset, 2);
        return 2;
    }

    public int writeString(CharSequence value, long offset) {
        int len = value == null ? -1 : value.length();
        this.writeInt(len, offset);
        if (len <= 0) {
            return 4;
        }

        offset += 2;
        for(int i = 0; i < value.length(); i++) {
            Unsafe.getUnsafe().putChar(localBufferAddress, value.charAt(i));
            this.append(localBufferAddress, offset += 2, 2);
        }
        return value.length() + 4;
    }

    public int writeBinary(DirectInputStream value, long offset) {
        long initialOffset = offset;
        long len = value.getLength();
        this.writeInt((int) value.getLength(), offset);
        if (len <= 0) {
            return 4;
        }
        offset -= 4;
        long copied;
        do {
            do {
                copied = value.read(localBufferAddress, 0, 8);
                this.append(localBufferAddress, offset += 8, copied);
            }
            while (copied == 8);
            offset -= 8 - copied;
        }
        while (copied > 0);
        return (int)(offset + 8 - initialOffset);
    }
    */
}
