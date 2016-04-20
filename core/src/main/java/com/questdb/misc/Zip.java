/*******************************************************************************
 * ___                  _   ____  ____
 * / _ \ _   _  ___  ___| |_|  _ \| __ )
 * | | | | | | |/ _ \/ __| __| | | |  _ \
 * | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 * \__\_\\__,_|\___||___/\__|____/|____/
 * <p>
 * Copyright (C) 2014-2016 Appsicle
 * <p>
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * <p>
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * <p>
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 ******************************************************************************/

package com.questdb.misc;

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
        gzipHeader = Unsafe.getUnsafe().allocateMemory(Numbers.ceilPow2(gzipHeaderLen));
        long p = gzipHeader;
        Unsafe.getUnsafe().setMemory(gzipHeader, gzipHeaderLen, (byte) 0);
        Unsafe.getUnsafe().putByte(p++, (byte) GZIP_MAGIC);
        Unsafe.getUnsafe().putByte(p++, (byte) (GZIP_MAGIC >> 8));
        Unsafe.getUnsafe().putByte(p, (byte) 8); // compression method
    }
}