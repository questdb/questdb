package com.nfsdb.collections;

import com.nfsdb.column.DirectInputStream;
import com.nfsdb.utils.Unsafe;

import java.io.IOException;

public class DirectAddressStream extends DirectInputStream {
    private final long address;
    private final long len;
    private long pos;

    public DirectAddressStream(long address, long len) {
        this.address = address;
        this.len = len;
    }

    @Override
    public long getLength() {
        return (int)len - pos;
    }

    @Override
    public long copyTo(long address, long start, long length) {
        long copyLen = Math.min(len - start - pos, length);
        Unsafe.getUnsafe().copyMemory(this.address + pos + start, address, copyLen);
        pos += start + copyLen;
        return copyLen;
    }

    @Override
    public int read() throws IOException {
        if (pos < len) {
            return Unsafe.getUnsafe().getByte(address + pos++);
        }
        return -1;
    }
}
