/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.std;

public final class Zip {
    // return codes from zlib version 1.2.8
    public static final int Z_OK = 0;
    public static final int Z_STREAM_END = 1;
    public static final int Z_BUF_ERROR = -5;
    public static final long gzipHeader;
    public static final int gzipHeaderLen = 10;
    private final static int GZIP_MAGIC = 0x8b1f;

    static {
        Os.init();
        gzipHeader = Unsafe.calloc(Numbers.ceilPow2(gzipHeaderLen), MemoryTag.NATIVE_DEFAULT);
        long p = gzipHeader;
        Unsafe.getUnsafe().putByte(p++, (byte) GZIP_MAGIC);
        Unsafe.getUnsafe().putByte(p++, (byte) (GZIP_MAGIC >> 8));
        Unsafe.getUnsafe().putByte(p, (byte) 8); // compression method
    }

    public static void init() {
        // Method used for testing to force invocation of static class methods and hence memory initialisation
    }

    private Zip() {
    }

    public static native int availIn(long z_streamp);

    public static native int availOut(long z_streamp);

    public static native int crc32(int crc, long address, int available);

    // Deflate

    public static native int deflate(long z_streamp, long out, int available, boolean flush);

    public static native void deflateEnd(long z_streamp);

    public static native long deflateInit();

    public static native void deflateReset(long z_stream);

    // Inflate

    public static native int inflate(long z_streamp, long address, int available, boolean flush);

    public static native void inflateEnd(long z_streamp);

    public static native long inflateInit(boolean nowrap);

    public static native int inflateReset(long z_streamp);

    public static native void setInput(long z_streamp, long address, int available);

    public static native int totalOut(long z_streamp);
}