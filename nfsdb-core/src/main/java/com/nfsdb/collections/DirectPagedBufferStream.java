package com.nfsdb.collections;

import com.nfsdb.column.DirectInputStream;
import com.nfsdb.utils.Unsafe;

import java.io.IOException;

public class DirectPagedBufferStream extends DirectInputStream {
    private final long length;
    private final DirectPagedBuffer buffer;
    private final long offset;
    private long blockStartAddress;
    private long blockEndOffset;
    private long blockStartOffset;
    private long position;

    public DirectPagedBufferStream(DirectPagedBuffer buffer, long offset, long length) {
        this.buffer = buffer;
        this.offset = offset;
        this.blockStartAddress = buffer.toAddress(offset);
        this.blockStartOffset = 0;
        this.length = length;
    }

    @Override
    public long getLength() {
        return (int) length - position;
    }

    @Override
    public long copyTo(long address, long start, long length) {
        long read = buffer.read(address, offset + position + start, Math.min(length, this.length));
        position += read + start;
        return read;
    }

    @Override
    public int read() throws IOException {
        if (position < length) {
            if (position < blockEndOffset) {
                return Unsafe.getUnsafe().getByte(blockStartAddress + offset + position++ - blockStartOffset);
            }
            return readFromNextBlock();
        }
        return -1;
    }

    private int readFromNextBlock() {
        blockStartOffset = offset + position;
        blockStartAddress = buffer.toAddress(blockStartOffset);
        long blockLen = buffer.getBlockLen(blockStartOffset);
        if (blockLen < 0) {
            return -1;
        }

        blockEndOffset += blockLen;
        assert position < blockEndOffset;
        return Unsafe.getUnsafe().getByte(blockStartAddress + offset + position++ - blockStartOffset);
    }
}
