/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.std;

public final class Zip {
    public static final long gzipHeader;
    public static final int gzipHeaderLen = 10;
    private final static int GZIP_MAGIC = 0x8b1f;


    private Zip() {
    }

    public static native int availIn(long z_streamp);

    public static native int availOut(long z_streamp);

    public static native int crc32(int crc, long address, int available);

    public static native int deflate(long z_streamp, long out, int available, boolean flush);

    // Deflate

    public static native void deflateEnd(long z_streamp);

    public static native long deflateInit();

    public static native void deflateReset(long z_stream);

    public static native int inflate(long z_streamp, long address, int available, boolean flush);

    // Inflate

    public static native void inflateEnd(long z_streamp);

    public static native long inflateInit(boolean nowrap);

    public static native int inflateReset(long z_streamp);

    public static native void setInput(long z_streamp, long address, int available);

    public static native int totalOut(long z_streamp);

    static {
        Os.init();
        gzipHeader = Unsafe.malloc(Numbers.ceilPow2(gzipHeaderLen));
        long p = gzipHeader;
        Unsafe.getUnsafe().setMemory(gzipHeader, gzipHeaderLen, (byte) 0);
        Unsafe.getUnsafe().putByte(p++, (byte) GZIP_MAGIC);
        Unsafe.getUnsafe().putByte(p++, (byte) (GZIP_MAGIC >> 8));
        Unsafe.getUnsafe().putByte(p, (byte) 8); // compression method
    }
}