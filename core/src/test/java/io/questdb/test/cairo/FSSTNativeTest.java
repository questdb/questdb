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

package io.questdb.test.cairo;

import io.questdb.cairo.idx.FSSTNative;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FSSTNativeTest extends AbstractCairoTest {

    @Test
    public void testDecoderReuseAcrossImports() throws Exception {
        assertMemoryLeak(() -> {
            byte[][] payloadA = {
                    "foo bar baz".getBytes(StandardCharsets.UTF_8),
                    "foo bar qux".getBytes(StandardCharsets.UTF_8),
                    "foo bar quux".getBytes(StandardCharsets.UTF_8),
                    "foo bar corge".getBytes(StandardCharsets.UTF_8),
            };
            byte[][] payloadB = {
                    "wholly different content alpha".getBytes(StandardCharsets.UTF_8),
                    "wholly different content beta".getBytes(StandardCharsets.UTF_8),
                    "wholly different content gamma".getBytes(StandardCharsets.UTF_8),
                    "wholly different content delta".getBytes(StandardCharsets.UTF_8),
            };

            CompressedBlock a = compress(payloadA);
            CompressedBlock b = compress(payloadB);
            long decoder = Unsafe.malloc(FSSTNative.DECODER_STRUCT_SIZE, MemoryTag.NATIVE_DEFAULT);
            try {
                assertTrue("import A", FSSTNative.importTable(decoder, a.tableAddr) > 0);
                assertDecodeMatches(decoder, a, payloadA);
                assertTrue("import B (in-place reuse)", FSSTNative.importTable(decoder, b.tableAddr) > 0);
                assertDecodeMatches(decoder, b, payloadB);
            } finally {
                Unsafe.free(decoder, FSSTNative.DECODER_STRUCT_SIZE, MemoryTag.NATIVE_DEFAULT);
                Misc.free(a);
                Misc.free(b);
            }
        });
    }

    @Test
    public void testDecompressTruncationReturnsError() throws Exception {
        assertMemoryLeak(() -> {
            byte[][] payload = {
                    "GET /api/v1/users/12345 HTTP/1.1 200 OK".getBytes(StandardCharsets.UTF_8),
                    "GET /api/v1/users/67890 HTTP/1.1 404 NotFound".getBytes(StandardCharsets.UTF_8),
                    "GET /api/v1/users/11111 HTTP/1.1 500 Error".getBytes(StandardCharsets.UTF_8),
            };
            CompressedBlock block = compress(payload);
            long decoder = Unsafe.malloc(FSSTNative.DECODER_STRUCT_SIZE, MemoryTag.NATIVE_DEFAULT);
            int decodedTotal = 0;
            for (byte[] p : payload) decodedTotal += p.length;
            long bigDstCap = decodedTotal + 64;
            long bigDst = Unsafe.malloc(bigDstCap, MemoryTag.NATIVE_DEFAULT);
            long dstOffs = Unsafe.malloc((long) (payload.length + 1) * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            long tinyDstCap = 4;
            long tinyDst = Unsafe.malloc(tinyDstCap, MemoryTag.NATIVE_DEFAULT);
            try {
                assertTrue("import", FSSTNative.importTable(decoder, block.tableAddr) > 0);
                long truncated = FSSTNative.decompressBlock(
                        decoder, block.cmpAddr, block.cmpOffsAddr, Long.BYTES, payload.length,
                        tinyDst, tinyDstCap, dstOffs);
                assertEquals("truncation must return -1", -1, truncated);
                long ok = FSSTNative.decompressBlock(
                        decoder, block.cmpAddr, block.cmpOffsAddr, Long.BYTES, payload.length,
                        bigDst, bigDstCap, dstOffs);
                assertEquals("retry with adequate capacity must succeed", decodedTotal, ok);
            } finally {
                Unsafe.free(tinyDst, tinyDstCap, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(dstOffs, (long) (payload.length + 1) * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(bigDst, bigDstCap, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(decoder, FSSTNative.DECODER_STRUCT_SIZE, MemoryTag.NATIVE_DEFAULT);
                Misc.free(block);
            }
        });
    }

    @Test
    public void testDecompressWithUint32Offsets() throws Exception {
        assertMemoryLeak(() -> {
            byte[][] payload = {
                    "the quick brown fox jumps over the lazy dog".getBytes(StandardCharsets.UTF_8),
                    "pack my box with five dozen liquor jugs".getBytes(StandardCharsets.UTF_8),
                    "how vexingly quick daft zebras jump".getBytes(StandardCharsets.UTF_8),
                    "sphinx of black quartz, judge my vow".getBytes(StandardCharsets.UTF_8),
            };
            CompressedBlock block = compress(payload);
            int count = payload.length;
            int decodedTotal = 0;
            for (byte[] p : payload) decodedTotal += p.length;
            long offsets32Bytes = (long) (count + 1) * Integer.BYTES;
            long offsets32 = Unsafe.malloc(offsets32Bytes, MemoryTag.NATIVE_DEFAULT);
            long dstOffsAddr = Unsafe.malloc((long) (count + 1) * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            long dstCap = decodedTotal + 64;
            long dstAddr = Unsafe.malloc(dstCap, MemoryTag.NATIVE_DEFAULT);
            long decoder = Unsafe.malloc(FSSTNative.DECODER_STRUCT_SIZE, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i <= count; i++) {
                    long off = Unsafe.getLong(block.cmpOffsAddr + (long) i * Long.BYTES);
                    Unsafe.putInt(offsets32 + (long) i * Integer.BYTES, (int) off);
                }
                assertTrue("import", FSSTNative.importTable(decoder, block.tableAddr) > 0);
                long decoded = FSSTNative.decompressBlock(
                        decoder, block.cmpAddr, offsets32, Integer.BYTES, count,
                        dstAddr, dstCap, dstOffsAddr);
                assertEquals("decoded total", decodedTotal, decoded);
                for (int i = 0; i < count; i++) {
                    long lo = Unsafe.getLong(dstOffsAddr + (long) i * Long.BYTES);
                    long hi = Unsafe.getLong(dstOffsAddr + (long) (i + 1) * Long.BYTES);
                    assertEquals("value " + i + " length", payload[i].length, (int) (hi - lo));
                    for (int j = 0; j < payload[i].length; j++) {
                        assertEquals("value " + i + " byte " + j,
                                payload[i][j], Unsafe.getByte(dstAddr + lo + j));
                    }
                }
            } finally {
                Unsafe.free(decoder, FSSTNative.DECODER_STRUCT_SIZE, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(dstAddr, dstCap, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(dstOffsAddr, (long) (count + 1) * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(offsets32, offsets32Bytes, MemoryTag.NATIVE_DEFAULT);
                Misc.free(block);
            }
        });
    }

    @Test
    public void testEmptyValueInBatchProducesZeroLengthOutput() throws Exception {
        assertMemoryLeak(() -> {
            byte[][] payload = {
                    "alpha".getBytes(StandardCharsets.UTF_8),
                    new byte[0],
                    "beta".getBytes(StandardCharsets.UTF_8),
                    new byte[0],
                    "gamma_payload_for_padding_so_training_does_not_underflow".getBytes(StandardCharsets.UTF_8),
            };
            roundtrip(payload);
        });
    }

    @Test
    public void testLargeBatchRoundtrip() throws Exception {
        assertMemoryLeak(() -> {
            Random rnd = new Random(42);
            byte[] alphabet = "the_quick_brown_fox_jumps_over_the_lazy_dog_0123456789".getBytes(StandardCharsets.UTF_8);
            byte[][] payload = new byte[2048][];
            for (int i = 0; i < payload.length; i++) {
                int len = 8 + rnd.nextInt(40);
                int start = rnd.nextInt(alphabet.length - len);
                byte[] v = new byte[len];
                System.arraycopy(alphabet, start, v, 0, len);
                payload[i] = v;
            }
            roundtrip(payload);
        });
    }

    @Test
    public void testNoLeakOnRepeatedCycles() throws Exception {
        assertMemoryLeak(() -> {
            byte[][] payload = {
                    "GET /api/v1/users/12345".getBytes(StandardCharsets.UTF_8),
                    "GET /api/v1/users/67890".getBytes(StandardCharsets.UTF_8),
                    "GET /api/v1/users/11111".getBytes(StandardCharsets.UTF_8),
            };
            for (int cycle = 0; cycle < 16; cycle++) {
                roundtrip(payload);
            }
        });
    }

    @Test
    public void testRoundtripPreservesBytes() throws Exception {
        assertMemoryLeak(() -> {
            String[] strings = {
                    "https://questdb.io/docs/reference/sql/select/?lang=en",
                    "https://questdb.io/docs/reference/sql/insert/?lang=en",
                    "https://questdb.io/docs/reference/sql/update/?lang=en",
                    "{\"user_id\":42,\"role\":\"admin\",\"status\":\"active\"}",
                    "{\"user_id\":17,\"role\":\"viewer\",\"status\":\"pending\"}",
                    "GET /api/v1/orders/67890 HTTP/1.1 404 NotFound",
            };
            byte[][] payload = new byte[strings.length][];
            for (int i = 0; i < strings.length; i++) {
                payload[i] = strings[i].getBytes(StandardCharsets.UTF_8);
            }
            roundtrip(payload);
        });
    }

    @Test
    public void testSingleValueBatch() throws Exception {
        assertMemoryLeak(() -> {
            byte[][] payload = {
                    "the only value in this batch needs to be long enough for training".getBytes(StandardCharsets.UTF_8)
            };
            roundtrip(payload);
        });
    }

    private static void assertDecodeMatches(long decoder, CompressedBlock block, byte[][] expected) {
        int count = expected.length;
        int decodedTotal = 0;
        for (byte[] p : expected) decodedTotal += p.length;
        long dstCap = decodedTotal + 64;
        long dstAddr = Unsafe.malloc(dstCap, MemoryTag.NATIVE_DEFAULT);
        long dstOffsAddr = Unsafe.malloc((long) (count + 1) * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        try {
            long decoded = FSSTNative.decompressBlock(
                    decoder, block.cmpAddr, block.cmpOffsAddr, Long.BYTES, count,
                    dstAddr, dstCap, dstOffsAddr);
            assertEquals("decoded total", decodedTotal, decoded);
            for (int i = 0; i < count; i++) {
                long lo = Unsafe.getLong(dstOffsAddr + (long) i * Long.BYTES);
                long hi = Unsafe.getLong(dstOffsAddr + (long) (i + 1) * Long.BYTES);
                assertEquals("value " + i + " length", expected[i].length, (int) (hi - lo));
                for (int j = 0; j < expected[i].length; j++) {
                    assertEquals("value " + i + " byte " + j,
                            expected[i][j], Unsafe.getByte(dstAddr + lo + j));
                }
            }
        } finally {
            Unsafe.free(dstOffsAddr, (long) (count + 1) * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(dstAddr, dstCap, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static CompressedBlock compress(byte[][] payload) {
        int count = payload.length;
        long total = 0;
        for (byte[] p : payload) total += p.length;
        assertTrue("payload must be non-empty", total > 0);
        long offsetsBytes = (long) (count + 1) * Long.BYTES;
        long cmpCap = total * 2 + 64;
        long batchScratchBytes = (long) count * FSSTNative.BATCH_SCRATCH_BYTES_PER_VALUE;

        long srcAddr = 0;
        long srcOffsAddr = 0;
        long cmpAddr = 0;
        long cmpOffsAddr = 0;
        long tableAddr = 0;
        long batchScratchAddr = 0;
        CompressedBlock result = null;
        try {
            srcAddr = Unsafe.malloc(total, MemoryTag.NATIVE_DEFAULT);
            srcOffsAddr = Unsafe.malloc(offsetsBytes, MemoryTag.NATIVE_DEFAULT);
            cmpAddr = Unsafe.malloc(cmpCap, MemoryTag.NATIVE_DEFAULT);
            cmpOffsAddr = Unsafe.malloc(offsetsBytes, MemoryTag.NATIVE_DEFAULT);
            tableAddr = Unsafe.malloc(FSSTNative.MAX_HEADER_SIZE, MemoryTag.NATIVE_DEFAULT);
            batchScratchAddr = Unsafe.malloc(batchScratchBytes, MemoryTag.NATIVE_DEFAULT);

            long pos = 0;
            for (int i = 0; i < count; i++) {
                Unsafe.putLong(srcOffsAddr + (long) i * Long.BYTES, pos);
                for (byte b : payload[i]) {
                    Unsafe.putByte(srcAddr + pos++, b);
                }
            }
            Unsafe.putLong(srcOffsAddr + (long) count * Long.BYTES, pos);

            long packed = FSSTNative.trainAndCompressBlock(
                    srcAddr, srcOffsAddr, count,
                    cmpAddr, cmpCap, cmpOffsAddr,
                    tableAddr, batchScratchAddr);
            assertTrue("train+compress", packed >= 0);
            result = new CompressedBlock(srcAddr, total, srcOffsAddr,
                    cmpAddr, cmpCap, cmpOffsAddr, offsetsBytes, tableAddr);
            return result;
        } finally {
            if (batchScratchAddr != 0) Unsafe.free(batchScratchAddr, batchScratchBytes, MemoryTag.NATIVE_DEFAULT);
            if (result == null) {
                if (tableAddr != 0) Unsafe.free(tableAddr, FSSTNative.MAX_HEADER_SIZE, MemoryTag.NATIVE_DEFAULT);
                if (cmpOffsAddr != 0) Unsafe.free(cmpOffsAddr, offsetsBytes, MemoryTag.NATIVE_DEFAULT);
                if (cmpAddr != 0) Unsafe.free(cmpAddr, cmpCap, MemoryTag.NATIVE_DEFAULT);
                if (srcOffsAddr != 0) Unsafe.free(srcOffsAddr, offsetsBytes, MemoryTag.NATIVE_DEFAULT);
                if (srcAddr != 0) Unsafe.free(srcAddr, total, MemoryTag.NATIVE_DEFAULT);
            }
        }
    }

    private static void roundtrip(byte[][] payload) {
        try (CompressedBlock block = compress(payload)) {
            long decoder = Unsafe.malloc(FSSTNative.DECODER_STRUCT_SIZE, MemoryTag.NATIVE_DEFAULT);
            try {
                assertTrue("table import must succeed", FSSTNative.importTable(decoder, block.tableAddr) > 0);
                assertDecodeMatches(decoder, block, payload);
            } finally {
                Unsafe.free(decoder, FSSTNative.DECODER_STRUCT_SIZE, MemoryTag.NATIVE_DEFAULT);
            }
        }
    }

    private record CompressedBlock(long srcAddr, long srcLen, long srcOffsAddr, long cmpAddr, long cmpCap,
                                   long cmpOffsAddr, long offsetsBytes, long tableAddr) implements QuietCloseable {

        @Override
        public void close() {
            Unsafe.free(tableAddr, FSSTNative.MAX_HEADER_SIZE, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(cmpOffsAddr, offsetsBytes, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(cmpAddr, cmpCap, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(srcOffsAddr, offsetsBytes, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(srcAddr, srcLen, MemoryTag.NATIVE_DEFAULT);
        }
    }
}
