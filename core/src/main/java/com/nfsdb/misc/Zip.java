/*******************************************************************************
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
 ******************************************************************************/

package com.nfsdb.misc;

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

    public static native int deflateReset(long z_stream);

    public static native int inflate(long z_streamp, long address, int available, boolean flush);

    // Inflate

    public static native void inflateEnd(long z_streamp);

    public static native long inflateInit(boolean nowrap);

    public static native int inflateReset(long z_streamp);

    public static native void setInput(long z_streamp, long address, int available);

    public static native int totalOut(long z_streamp);

    static {
        gzipHeader = Unsafe.getUnsafe().allocateMemory(Numbers.ceilPow2(gzipHeaderLen));
        long p = gzipHeader;
        Unsafe.getUnsafe().setMemory(gzipHeader, gzipHeaderLen, (byte) 0);
        Unsafe.getUnsafe().putByte(p++, (byte) GZIP_MAGIC);
        Unsafe.getUnsafe().putByte(p++, (byte) (GZIP_MAGIC >> 8));
        Unsafe.getUnsafe().putByte(p, (byte) 8); // compression method
    }
}