package com.nfsdb.collections;

import com.nfsdb.column.DirectInputStream;
import com.nfsdb.utils.Unsafe;
import java.io.Closeable;
import java.io.IOException;

public class DirectLinkedBuffer implements Closeable {
    private static final long CACHE_LINE_SIZE = AbstractDirectList.CACHE_LINE_SIZE;
    private long pageCapacity;
    private DirectLongList buffers;
    private long localBufferAddress;

    public DirectLinkedBuffer() {
        this(CACHE_LINE_SIZE);
    }

    public DirectLinkedBuffer(long pageCapacity) {
        this.pageCapacity = pageCapacity;
        long address = Unsafe.getUnsafe().allocateMemory(pageCapacity);
        buffers = new DirectLongList(1);
        buffers.add(address);
        localBufferAddress = Unsafe.getUnsafe().allocateMemory(8);
    }

    public void write(long readFrom, long writeOffset, long size) {
        if (size < 0 || writeOffset < 0) {
            throw new IndexOutOfBoundsException();
        }

        int pageIndex = (int) (writeOffset / pageCapacity);
        long pageOffset = writeOffset % pageCapacity;
        int finalPage = (int) (pageIndex + size / pageCapacity + ((size % pageCapacity + pageOffset) > pageCapacity ? 1 : 0));
        if (finalPage >= buffers.size()) {
            for (int i = buffers.size(); i < finalPage + 1; i++) {
                buffers.add(0);
            }
        }

        do {
            long writeAddress = buffers.get(pageIndex);
            if (writeAddress == 0) {
                writeAddress = Unsafe.getUnsafe().allocateMemory(pageCapacity);
                buffers.set(pageIndex, writeAddress);
            }

            pageOffset = writeOffset % pageCapacity;
            writeAddress += pageOffset;
            long writeSize = Math.min(size, pageCapacity - pageOffset);
            Unsafe.getUnsafe().copyMemory(readFrom, writeAddress, writeSize);
            readFrom += writeSize;
            size -= writeSize;
            writeOffset = 0;
            pageIndex++;
        } while (size > 0);
    }

    public long read(long writeTo, long readOffset, long size) {
        if (size < 0 || readOffset < 0) {
            throw new IndexOutOfBoundsException();
        }

        int pageIndex = (int) (readOffset / pageCapacity);
        long pageOffset = readOffset % pageCapacity;
        long readLen = 0;

        if (pageIndex >= buffers.size()) {
            return readLen;
        }

        do {
            long readAddress = buffers.get(pageIndex);
            if (readAddress == 0) {
                return readLen;
            }

            long toRead = Math.min(size, pageCapacity - pageOffset);
            Unsafe.getUnsafe().copyMemory(readAddress + pageOffset, writeTo, toRead);
            writeTo += toRead;
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
        for(int i = 0; i < buffers.size(); i++) {
            long address = buffers.get(i);
            if (address != 0) {
                Unsafe.getUnsafe().freeMemory(buffers.get(i));
            }
        }
        buffers.close();
    }

    @Override
     protected void finalize() throws Throwable {
        free();
        super.finalize();
    }

    public byte getByte(long readOffset) {
        int pageIndex = (int) (readOffset / pageCapacity);
        long pageOffset = readOffset % pageCapacity;
        if (pageIndex >= buffers.size()) {
            throw new IndexOutOfBoundsException();
        }

        long readAddress = buffers.get(pageIndex);
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
        this.write(localBufferAddress, offset, 1);
        return 1;
    }

    public int writeDouble(double value, long offset) {
        Unsafe.getUnsafe().putDouble(localBufferAddress, value);
        this.write(localBufferAddress, offset, 8);
        return 8;
    }

    public int writeInt(int value, long offset) {
        Unsafe.getUnsafe().putInt(localBufferAddress, value);
        this.write(localBufferAddress, offset, 4);
        return 4;
    }

    public int writeLong(long value, long offset) {
        Unsafe.getUnsafe().putLong(localBufferAddress, value);
        this.write(localBufferAddress, offset, 8);
        return 8;
    }

    public int writeShort(short value, long offset) {
        Unsafe.getUnsafe().putShort(localBufferAddress, value);
        this.write(localBufferAddress, offset, 2);
        return 2;
    }

    public int writeString(CharSequence value, long offset) {
        this.writeInt(value.length(), offset);
        offset += 2;
        for(int i = 0; i < value.length(); i++) {
            Unsafe.getUnsafe().putChar(localBufferAddress, value.charAt(i));
            this.write(localBufferAddress, offset += 2, 2);
        }
        return value.length() + 4;
    }

    public int writeBinary(DirectInputStream value, long offset) {
        long initialOffset = offset;
        this.writeInt((int) value.getLength(), offset);
        offset -= 4;
        long copied;
        do {
            do {
                copied = value.copyTo(localBufferAddress, 0, 8);
                this.write(localBufferAddress, offset += 8, copied);
            }
            while (copied == 8);
            offset -= 8 - copied;
        }
        while (copied > 0);
        return (int)(offset + 8 - initialOffset);
    }
}
