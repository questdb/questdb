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

package io.questdb.test.cutlass.qwp;

import io.questdb.cairo.CairoException;
import io.questdb.cutlass.qwp.protocol.QwpBitReader;
import io.questdb.cutlass.qwp.protocol.QwpBitWriter;
import io.questdb.cutlass.qwp.protocol.QwpGorillaEncoder;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.MemoryTag;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.test.tools.TestUtils.assertMemoryLeak;

/**
 * The server's Gorilla timestamp encoder packs its bits a word at a time. Its bytes must be the
 * ones the bit-at-a-time encoding (each delta-of-delta's bucket prefix, then its payload, through
 * {@link QwpBitWriter}) produces, for every bucket, every boundary and any alignment.
 */
public class QwpEgressGorillaEncoderTest {
    private static final long[] EDGE_DODS = {
            0, 1, -1, 63, 64, -64, -65, 255, 256, -256, -257, 2047, 2048, -2048, -2049,
            Integer.MAX_VALUE, Integer.MIN_VALUE, Integer.MAX_VALUE - 1, Integer.MIN_VALUE + 1, 65_536, -1_000_000
    };
    private static final Log LOG = LogFactory.getLog(QwpEgressGorillaEncoderTest.class);

    @Test
    public void testEdgeDeltasOfDeltasAtEveryAlignment() throws Exception {
        assertMemoryLeak(() -> {
            // a run of zeros shifts the next value through every bit position of the accumulator
            for (long dod : EDGE_DODS) {
                for (int zeros = 0; zeros < 70; zeros++) {
                    final long[] dods = new long[zeros + 3];
                    dods[zeros] = dod;
                    dods[zeros + 1] = dod;
                    dods[zeros + 2] = 1;
                    assertSameAsReference(timestamps(1_700_000_000_000_000L, 1_000, dods));
                }
            }
        });
    }

    @Test
    public void testOverflowThrows() throws Exception {
        assertMemoryLeak(() -> {
            final long[] ts = timestamps(0, 1_000, new long[]{Integer.MAX_VALUE, Integer.MIN_VALUE, 5, 0, 77, 3000});
            final long src = put(ts);
            final int size = QwpGorillaEncoder.calculateEncodedSizeIfSupported(src, ts.length);
            final long dst = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
            try {
                final QwpGorillaEncoder encoder = new QwpGorillaEncoder();
                Assert.assertEquals(size, encoder.encodeTimestamps(dst, size, src, ts.length));
                try {
                    encoder.encodeTimestamps(dst, size - 1, src, ts.length);
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "Gorilla encoder buffer overflow");
                }
            } finally {
                Unsafe.free(src, ts.length * 8L, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(dst, size, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testRandomSeries() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);
            for (int i = 0; i < 2_000; i++) {
                final int count = rnd.nextInt(600);
                final long[] dods = new long[Math.max(0, count - 2)];
                for (int j = 0; j < dods.length; j++) {
                    dods[j] = switch (rnd.nextInt(6)) {
                        case 0, 1 -> 0;
                        case 2 -> rnd.nextInt(128) - 64;
                        case 3 -> rnd.nextInt(512) - 256;
                        case 4 -> rnd.nextInt(4096) - 2048;
                        default -> rnd.nextInt();
                    };
                }
                final long[] ts = timestamps(rnd.nextLong() >> 8, rnd.nextInt(1_000_000), dods);
                assertSameAsReference(count < 2 ? java.util.Arrays.copyOf(ts, count) : ts);
            }
        });
    }

    private static void assertSameAsReference(long[] ts) throws Exception {
        final long src = put(ts);
        final int size = QwpGorillaEncoder.calculateEncodedSizeIfSupported(src, ts.length);
        Assert.assertTrue(size >= 0);
        final long dst = Unsafe.malloc(size + 1, MemoryTag.NATIVE_DEFAULT);
        final long ref = Unsafe.malloc(size + 1, MemoryTag.NATIVE_DEFAULT);
        try {
            final int written = new QwpGorillaEncoder().encodeTimestamps(dst, size, src, ts.length);
            Assert.assertEquals(size, written);
            Assert.assertEquals(size, referenceEncode(ref, size, ts));
            for (int i = 0; i < size; i++) {
                Assert.assertEquals("byte " + i + " of " + size, Unsafe.getByte(ref + i), Unsafe.getByte(dst + i));
            }
            // and it decodes back
            if (ts.length > 2) {
                final QwpBitReader reader = new QwpBitReader();
                reader.reset(dst + 16, size - 16);
                long prevTs = ts[1];
                long prevDelta = ts[1] - ts[0];
                for (int i = 2; i < ts.length; i++) {
                    final long delta = prevDelta + decodeDoD(reader);
                    prevTs += delta;
                    prevDelta = delta;
                    Assert.assertEquals(ts[i], prevTs);
                }
            }
        } finally {
            Unsafe.free(src, Math.max(1, ts.length) * 8L, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(dst, size + 1, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(ref, size + 1, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static long decodeDoD(QwpBitReader reader) throws Exception {
        if (reader.readBit() == 0) {
            return 0;
        }
        if (reader.readBit() == 0) {
            return reader.readSigned(7);
        }
        if (reader.readBit() == 0) {
            return reader.readSigned(9);
        }
        if (reader.readBit() == 0) {
            return reader.readSigned(12);
        }
        return reader.readSigned(32);
    }

    private static long put(long[] ts) {
        final long address = Unsafe.malloc(Math.max(1, ts.length) * 8L, MemoryTag.NATIVE_DEFAULT);
        for (int i = 0; i < ts.length; i++) {
            Unsafe.putLong(address + i * 8L, ts[i]);
        }
        return address;
    }

    // the encoding one bit field at a time: two raw timestamps, then prefix and payload per value
    private static int referenceEncode(long dst, int capacity, long[] ts) {
        if (ts.length == 0) {
            return 0;
        }
        Unsafe.putLong(dst, ts[0]);
        if (ts.length == 1) {
            return 8;
        }
        Unsafe.putLong(dst + 8, ts[1]);
        final QwpBitWriter writer = new QwpBitWriter();
        writer.reset(dst + 16, capacity - 16);
        long prevDelta = ts[1] - ts[0];
        for (int i = 2; i < ts.length; i++) {
            final long delta = ts[i] - ts[i - 1];
            final long dod = delta - prevDelta;
            prevDelta = delta;
            switch (QwpGorillaEncoder.getBucket(dod)) {
                case 0 -> writer.writeBit(0);
                case 1 -> {
                    writer.writeBits(0b01, 2);
                    writer.writeSigned(dod, 7);
                }
                case 2 -> {
                    writer.writeBits(0b011, 3);
                    writer.writeSigned(dod, 9);
                }
                case 3 -> {
                    writer.writeBits(0b0111, 4);
                    writer.writeSigned(dod, 12);
                }
                default -> {
                    writer.writeBits(0b1111, 4);
                    writer.writeSigned(dod, 32);
                }
            }
        }
        return 16 + writer.finish();
    }

    // timestamps from a start, a first delta and the deltas of deltas after it
    private static long[] timestamps(long start, long firstDelta, long[] dods) {
        final long[] ts = new long[dods.length + 2];
        ts[0] = start;
        ts[1] = start + firstDelta;
        long delta = firstDelta;
        for (int i = 0; i < dods.length; i++) {
            delta += dods[i];
            ts[i + 2] = ts[i + 1] + delta;
        }
        return ts;
    }
}
