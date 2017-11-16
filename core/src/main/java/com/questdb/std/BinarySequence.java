package com.questdb.std;

public interface BinarySequence {

    byte byteAt(long index);

    default long copyTo(long address, long start, long length) {
        long size = length();
        long n = size < length ? size : length;
        for (long l = 0; l < n; l++) {
            Unsafe.getUnsafe().putByte(address + l, byteAt(start + l));
        }
        return n;
    }

    long length();
}
