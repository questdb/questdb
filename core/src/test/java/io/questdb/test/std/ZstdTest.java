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
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class ZstdTest {

    static {
        Os.init();
    }

    @Test
    public void testCompressThenDecompressHighlyCompressibleRuns() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Runs of repeated bytes should compress dramatically.
            int len = 256 * 1024;
            long src = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < len; i++) {
                    Unsafe.putByte(src + i, (byte) (i / 1024));
                }
                roundTrip(src, len);
            } finally {
                Unsafe.free(src, len, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testCompressThenDecompressRandom() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int len = 64 * 1024;
            long src = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
            try {
                Rnd rnd = new Rnd();
                for (int i = 0; i < len; i++) {
                    Unsafe.putByte(src + i, (byte) rnd.nextInt());
                }
                roundTrip(src, len);
            } finally {
                Unsafe.free(src, len, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testCompressTooSmallDestinationReturnsError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // A realistic payload compressed into a destination one byte wide cannot fit
            // even the zstd frame header. The native side must return a negative error
            // code, not silently truncate.
            int len = 4096;
            long src = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
            long dst = Unsafe.malloc(1, MemoryTag.NATIVE_DEFAULT);
            long cctx = Zstd.createCCtx(3);
            try {
                for (int i = 0; i < len; i++) {
                    Unsafe.putByte(src + i, (byte) i);
                }
                long result = Zstd.compress(cctx, src, len, dst, 1);
                Assert.assertTrue("expected negative error, got " + result, result < 0);
            } finally {
                Zstd.freeCCtx(cctx);
                Unsafe.free(src, len, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(dst, 1, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testDecompressCorruptedInputReturnsError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int len = 4096;
            long src = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
            long dst = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
            long dctx = Zstd.createDCtx();
            try {
                // Zeros are not a valid zstd frame header; the library must reject them.
                for (int i = 0; i < len; i++) {
                    Unsafe.putByte(src + i, (byte) 0);
                }
                long result = Zstd.decompress(dctx, src, len, dst, len);
                Assert.assertTrue("expected negative error, got " + result, result < 0);
            } finally {
                Zstd.freeDCtx(dctx);
                Unsafe.free(src, len, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(dst, len, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testDecompressIntoTooSmallDestinationReturnsError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int len = 4096;
            long src = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
            long comp = Unsafe.malloc(len + 128, MemoryTag.NATIVE_DEFAULT);
            long decomp = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            long cctx = Zstd.createCCtx(3);
            long dctx = Zstd.createDCtx();
            try {
                for (int i = 0; i < len; i++) {
                    Unsafe.putByte(src + i, (byte) (i & 0x3F));
                }
                long compLen = Zstd.compress(cctx, src, len, comp, len + 128);
                Assert.assertTrue(compLen > 0);
                long result = Zstd.decompress(dctx, comp, compLen, decomp, 16);
                Assert.assertTrue("expected negative error, got " + result, result < 0);
            } finally {
                Zstd.freeCCtx(cctx);
                Zstd.freeDCtx(dctx);
                Unsafe.free(src, len, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(comp, len + 128, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(decomp, 16, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testEmptyInputRoundTrip() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // zstd supports empty frames -- compress with srcLen == 0 and decompress back.
            long src = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            long comp = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
            long decomp = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            long cctx = Zstd.createCCtx(3);
            long dctx = Zstd.createDCtx();
            try {
                long compLen = Zstd.compress(cctx, src, 0, comp, 64);
                Assert.assertTrue("compress(empty) returned " + compLen, compLen > 0);
                long decLen = Zstd.decompress(dctx, comp, compLen, decomp, 16);
                Assert.assertEquals(0, decLen);
            } finally {
                Zstd.freeCCtx(cctx);
                Zstd.freeDCtx(dctx);
                Unsafe.free(src, 16, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(comp, 64, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(decomp, 16, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testFreeOnZeroPointerIsNoOp() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Matches the Rust early-return on ptr == 0. Must not crash and must not
            // leak (assertMemoryLeak asserts the latter).
            Zstd.freeCCtx(0);
            Zstd.freeDCtx(0);
        });
    }

    @Test
    public void testLevelClampedByNative() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Requesting a level outside [1, 22] must not crash; the native side
            // clamps to the legal range.
            long c0 = Zstd.createCCtx(-5);
            long c1 = Zstd.createCCtx(1000);
            Assert.assertNotEquals(0, c0);
            Assert.assertNotEquals(0, c1);
            Zstd.freeCCtx(c0);
            Zstd.freeCCtx(c1);
        });
    }

    @Test
    public void testReuseContextAcrossManyBuffers() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
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
                        Unsafe.putByte(src + i, (byte) (rnd.nextInt() & (iter < 50 ? 0x03 : 0xFF)));
                    }
                    long compLen = Zstd.compress(cctx, src, len, comp, len + 128);
                    Assert.assertTrue("compressLen=" + compLen, compLen > 0);
                    long decLen = Zstd.decompress(dctx, comp, compLen, decomp, len);
                    Assert.assertEquals(len, decLen);
                    for (int i = 0; i < len; i++) {
                        Assert.assertEquals(
                                "iter=" + iter + " pos=" + i,
                                Unsafe.getByte(src + i),
                                Unsafe.getByte(decomp + i)
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
        });
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
                        Unsafe.getByte(srcAddr + i),
                        Unsafe.getByte(decomp + i)
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
