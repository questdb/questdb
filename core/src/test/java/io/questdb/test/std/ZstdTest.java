/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

package io.questdb.test.std;

import io.questdb.std.MemoryTag;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.Zstd;
import org.junit.Assert;
import org.junit.Test;

public class ZstdTest {

    static {
        Os.init();
    }

    @Test
    public void testCompressThenDecompressHighlyCompressibleRuns() {
        // Runs of repeated bytes should compress dramatically.
        int len = 256 * 1024;
        long src = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < len; i++) {
                Unsafe.getUnsafe().putByte(src + i, (byte) (i / 1024));
            }
            roundTrip(src, len);
        } finally {
            Unsafe.free(src, len, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testCompressThenDecompressRandom() {
        int len = 64 * 1024;
        long src = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
        try {
            Rnd rnd = new Rnd();
            for (int i = 0; i < len; i++) {
                Unsafe.getUnsafe().putByte(src + i, (byte) rnd.nextInt());
            }
            roundTrip(src, len);
        } finally {
            Unsafe.free(src, len, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testLevelClampedByNative() {
        // Requesting a level outside [1, 22] must not crash; the native side
        // clamps to the legal range.
        long c0 = Zstd.createCCtx(-5);
        long c1 = Zstd.createCCtx(1000);
        Assert.assertNotEquals(0, c0);
        Assert.assertNotEquals(0, c1);
        Zstd.freeCCtx(c0);
        Zstd.freeCCtx(c1);
    }

    @Test
    public void testReuseContextAcrossManyBuffers() {
        long cctx = Zstd.createCCtx(3);
        long dctx = Zstd.createDCtx();
        int len = 8 * 1024;
        long src = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
        long comp = Unsafe.malloc(len + 128, MemoryTag.NATIVE_DEFAULT);
        long decomp = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
        try {
            Rnd rnd = new Rnd();
            for (int iter = 0; iter < 100; iter++) {
                for (int i = 0; i < len; i++) {
                    Unsafe.getUnsafe().putByte(src + i, (byte) (rnd.nextInt() & (iter < 50 ? 0x03 : 0xFF)));
                }
                long compLen = Zstd.compress(cctx, src, len, comp, len + 128);
                Assert.assertTrue("compressLen=" + compLen, compLen > 0);
                long decLen = Zstd.decompress(dctx, comp, compLen, decomp, len);
                Assert.assertEquals(len, decLen);
                for (int i = 0; i < len; i++) {
                    Assert.assertEquals(
                            "iter=" + iter + " pos=" + i,
                            Unsafe.getUnsafe().getByte(src + i),
                            Unsafe.getUnsafe().getByte(decomp + i)
                    );
                }
            }
        } finally {
            Unsafe.free(src, len, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(comp, len + 128, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(decomp, len, MemoryTag.NATIVE_DEFAULT);
            Zstd.freeCCtx(cctx);
            Zstd.freeDCtx(dctx);
        }
    }

    private void roundTrip(long srcAddr, int srcLen) {
        long cctx = Zstd.createCCtx(3);
        long dctx = Zstd.createDCtx();
        Assert.assertNotEquals(0, cctx);
        Assert.assertNotEquals(0, dctx);
        long dstCap = srcLen + 512; // zstd worst case bound
        long comp = Unsafe.malloc(dstCap, MemoryTag.NATIVE_DEFAULT);
        long decomp = Unsafe.malloc(srcLen, MemoryTag.NATIVE_DEFAULT);
        try {
            long compLen = Zstd.compress(cctx, srcAddr, srcLen, comp, dstCap);
            Assert.assertTrue("compress returned " + compLen, compLen > 0);
            Assert.assertTrue("compressed larger than src+overhead", compLen <= dstCap);

            long decLen = Zstd.decompress(dctx, comp, compLen, decomp, srcLen);
            Assert.assertEquals("decompressed length mismatch", srcLen, decLen);

            for (int i = 0; i < srcLen; i++) {
                Assert.assertEquals(
                        "byte mismatch at " + i,
                        Unsafe.getUnsafe().getByte(srcAddr + i),
                        Unsafe.getUnsafe().getByte(decomp + i)
                );
            }
        } finally {
            Unsafe.free(comp, dstCap, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(decomp, srcLen, MemoryTag.NATIVE_DEFAULT);
            Zstd.freeCCtx(cctx);
            Zstd.freeDCtx(dctx);
        }
    }
}
