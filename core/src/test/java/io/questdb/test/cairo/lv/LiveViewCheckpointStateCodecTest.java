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

package io.questdb.test.cairo.lv;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.lv.LiveViewCheckpointStateCodec;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.MemoryTrackerWorkload;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class LiveViewCheckpointStateCodecTest extends AbstractCairoTest {

    @Test
    public void testAdaptiveFallbackAndSavingThreshold() throws Exception {
        assertMemoryLeak(() -> {
            try (LiveViewCheckpointStateCodec.Scratch scratch = new LiveViewCheckpointStateCodec.Scratch(null)) {
                final long timestampAddress = scratch.timestampsAddress();
                for (int i = 0; i < LiveViewCheckpointStateCodec.CHUNK_ROWS; i++) {
                    Unsafe.putLong(timestampAddress + (long) i * Long.BYTES, 1_000_000L + i * 1_000L);
                }
                Assert.assertEquals(
                        LiveViewCheckpointStateCodec.TIMESTAMP_DELTA_OF_DELTA_VARINT,
                        LiveViewCheckpointStateCodec.selectTimestampCodec(
                                timestampAddress,
                                LiveViewCheckpointStateCodec.CHUNK_ROWS
                        )
                );

                // A decreasing stream and a mathematically positive delta that
                // overflows signed long arithmetic must both select raw.
                put(timestampAddress, 10, 9);
                Assert.assertEquals(
                        LiveViewCheckpointStateCodec.TIMESTAMP_RAW_64,
                        LiveViewCheckpointStateCodec.selectTimestampCodec(timestampAddress, 2)
                );
                put(timestampAddress, Long.MIN_VALUE, Long.MAX_VALUE);
                Assert.assertEquals(
                        LiveViewCheckpointStateCodec.TIMESTAMP_RAW_64,
                        LiveViewCheckpointStateCodec.selectTimestampCodec(timestampAddress, 2)
                );

                final long doubleAddress = scratch.valuesAddress();
                final Rnd rnd = new Rnd(0x1234, 0x5678);
                for (int i = 0; i < LiveViewCheckpointStateCodec.CHUNK_ROWS; i++) {
                    Unsafe.putLong(doubleAddress + (long) i * Long.BYTES, rnd.nextLong());
                }
                Assert.assertEquals(
                        LiveViewCheckpointStateCodec.DOUBLE_RAW_64,
                        LiveViewCheckpointStateCodec.selectDoubleCodec(doubleAddress, LiveViewCheckpointStateCodec.CHUNK_ROWS)
                );

                for (int i = 0; i < 64; i++) {
                    Unsafe.putLong(doubleAddress + (long) i * Long.BYTES, Double.doubleToRawLongBits(42.5));
                }
                Assert.assertEquals(
                        LiveViewCheckpointStateCodec.DOUBLE_XOR,
                        LiveViewCheckpointStateCodec.selectDoubleCodec(doubleAddress, 64)
                );

                // Even a smaller encoded stream stays raw until it saves the
                // 16-byte floor required by the format.
                Assert.assertEquals(
                        LiveViewCheckpointStateCodec.DOUBLE_RAW_64,
                        LiveViewCheckpointStateCodec.selectDoubleCodec(doubleAddress, 2)
                );
            }
        });
    }

    @Test
    public void testAllDoubleBitPatternsRoundTripExactly() throws Exception {
        assertMemoryLeak(() -> {
            final long[] bits = {
                    0L,
                    Long.MIN_VALUE,
                    1L,
                    Long.MIN_VALUE | 1L,
                    0x000f_ffff_ffff_ffffL,
                    0x0010_0000_0000_0000L,
                    0x7fef_ffff_ffff_ffffL,
                    0xffef_ffff_ffff_ffffL,
                    0x7ff0_0000_0000_0000L,
                    0xfff0_0000_0000_0000L,
                    0x7ff8_0000_0000_0001L,
                    0x7ff0_0000_0000_0001L,
                    0xfff8_dead_beef_1234L,
                    0x3ff0_0000_0000_0000L,
                    0xbff0_0000_0000_0000L
            };
            assertDoubleRoundTrip(bits, LiveViewCheckpointStateCodec.DOUBLE_RAW_64);
            assertDoubleRoundTrip(bits, LiveViewCheckpointStateCodec.DOUBLE_XOR);
        });
    }

    @Test
    public void testBoundarySizesAndDuplicateTimestamps() throws Exception {
        assertMemoryLeak(() -> {
            final int[] sizes = {0, 1, 2, LiveViewCheckpointStateCodec.CHUNK_ROWS - 1, LiveViewCheckpointStateCodec.CHUNK_ROWS};
            for (int size : sizes) {
                final long[] timestamps = new long[size];
                final long[] doubles = new long[size];
                for (int i = 0; i < size; i++) {
                    timestamps[i] = 100 + (i / 3) * 7L;
                    doubles[i] = Double.doubleToRawLongBits(100.0 + (i / 5) * 0.25);
                }
                assertTimestampRoundTrip(timestamps, LiveViewCheckpointStateCodec.TIMESTAMP_RAW_64);
                assertTimestampRoundTrip(timestamps, LiveViewCheckpointStateCodec.TIMESTAMP_DELTA_OF_DELTA_VARINT);
                assertDoubleRoundTrip(doubles, LiveViewCheckpointStateCodec.DOUBLE_RAW_64);
                assertDoubleRoundTrip(doubles, LiveViewCheckpointStateCodec.DOUBLE_XOR);
            }
        });
    }

    @Test
    public void testCorruptDoubleStreamsAreRejected() throws Exception {
        assertMemoryLeak(() -> {
            try (LiveViewCheckpointStateCodec.Scratch scratch = new LiveViewCheckpointStateCodec.Scratch(null);
                 MemoryCARW encoded = Vm.getCARWInstance(4096, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
                final long target = scratch.valuesAddress();

                assertDoubleDecodeFails(encoded, target, 99, 0, "unknown double codec");
                assertDoubleDecodeFails(encoded, target, LiveViewCheckpointStateCodec.DOUBLE_RAW_64, -1, "row count out of bounds");
                assertDoubleDecodeFails(
                        encoded,
                        target,
                        LiveViewCheckpointStateCodec.DOUBLE_RAW_64,
                        LiveViewCheckpointStateCodec.CHUNK_ROWS + 1,
                        "row count out of bounds"
                );

                encoded.putLong(7);
                assertDoubleDecodeFails(encoded, target, LiveViewCheckpointStateCodec.DOUBLE_RAW_64, 2, "raw double page length");

                encoded.truncate();
                encoded.putLong(7);
                encoded.putByte((byte) 0x01); // nonzero control + reuse control
                assertDoubleDecodeFails(encoded, target, LiveViewCheckpointStateCodec.DOUBLE_XOR, 2, "missing window");

                encoded.truncate();
                encoded.putLong(7);
                encoded.putByte((byte) 0xff); // nonzero + new + leading=63
                encoded.putByte((byte) 0x02); // significant bits=2 => impossible window
                assertDoubleDecodeFails(encoded, target, LiveViewCheckpointStateCodec.DOUBLE_XOR, 2, "window out of bounds");

                final long[] values = new long[32];
                Arrays.fill(values, Double.doubleToRawLongBits(3.5));
                final int validLength = encodeDoubles(encoded, scratch, values, LiveViewCheckpointStateCodec.DOUBLE_XOR);
                encoded.putByte((byte) 0);
                assertDoubleDecodeFails(
                        encoded,
                        target,
                        LiveViewCheckpointStateCodec.DOUBLE_XOR,
                        values.length,
                        "trailing bytes"
                );

                encoded.jumpTo(validLength);
                final byte last = encoded.getByte(validLength - 1L);
                encoded.putByte(validLength - 1L, (byte) (last | 0x80));
                assertDoubleDecodeFails(
                        encoded,
                        target,
                        LiveViewCheckpointStateCodec.DOUBLE_XOR,
                        values.length,
                        "padding bits"
                );

                encoded.jumpTo(validLength - 1L);
                assertDoubleDecodeFails(
                        encoded,
                        target,
                        LiveViewCheckpointStateCodec.DOUBLE_XOR,
                        values.length,
                        "truncated double XOR"
                );

                try {
                    LiveViewCheckpointStateCodec.decodeDoubles(
                            encoded.addressOf(0),
                            validLength,
                            LiveViewCheckpointStateCodec.DOUBLE_XOR,
                            values.length,
                            target,
                            values.length - 1
                    );
                    Assert.fail("expected target capacity rejection");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "target capacity");
                }
            }
        });
    }

    @Test
    public void testCorruptTimestampStreamsAreRejected() throws Exception {
        assertMemoryLeak(() -> {
            try (LiveViewCheckpointStateCodec.Scratch scratch = new LiveViewCheckpointStateCodec.Scratch(null);
                 MemoryCARW encoded = Vm.getCARWInstance(4096, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
                final long target = scratch.timestampsAddress();

                assertTimestampDecodeFails(encoded, target, 99, 0, "unknown timestamp codec");
                assertTimestampDecodeFails(
                        encoded,
                        target,
                        LiveViewCheckpointStateCodec.TIMESTAMP_RAW_64,
                        -1,
                        "row count out of bounds"
                );

                encoded.putLong(7);
                assertTimestampDecodeFails(
                        encoded,
                        target,
                        LiveViewCheckpointStateCodec.TIMESTAMP_RAW_64,
                        2,
                        "raw timestamp page length"
                );

                encoded.truncate();
                encoded.putLong(7);
                encoded.putByte((byte) 0x80);
                assertTimestampDecodeFails(
                        encoded,
                        target,
                        LiveViewCheckpointStateCodec.TIMESTAMP_DELTA_OF_DELTA_VARINT,
                        2,
                        "truncated LEB128"
                );

                encoded.putByte((byte) 0);
                assertTimestampDecodeFails(
                        encoded,
                        target,
                        LiveViewCheckpointStateCodec.TIMESTAMP_DELTA_OF_DELTA_VARINT,
                        2,
                        "non-canonical LEB128"
                );

                // Padding a value out over several bytes is the same corruption as
                // padding it out over two, however many of the extra bytes carry
                // payload bits.
                encoded.truncate();
                encoded.putLong(7);
                encoded.putByte((byte) 0x81);
                encoded.putByte((byte) 0x82);
                encoded.putByte((byte) 0x00);
                assertTimestampDecodeFails(
                        encoded,
                        target,
                        LiveViewCheckpointStateCodec.TIMESTAMP_DELTA_OF_DELTA_VARINT,
                        2,
                        "non-canonical LEB128"
                );

                encoded.truncate();
                encoded.putLong(Long.MAX_VALUE);
                encoded.putByte((byte) 1);
                assertTimestampDecodeFails(
                        encoded,
                        target,
                        LiveViewCheckpointStateCodec.TIMESTAMP_DELTA_OF_DELTA_VARINT,
                        2,
                        "timestamp arithmetic overflow"
                );

                encoded.truncate();
                encoded.putLong(0);
                encoded.putByte((byte) 0); // first delta
                encoded.putByte((byte) 1); // ZigZag(-1), making the next delta negative
                assertTimestampDecodeFails(
                        encoded,
                        target,
                        LiveViewCheckpointStateCodec.TIMESTAMP_DELTA_OF_DELTA_VARINT,
                        3,
                        "sequence decreases"
                );

                encoded.truncate();
                encoded.putLong(0);
                encoded.putByte((byte) 1);
                encoded.putByte((byte) 0);
                assertTimestampDecodeFails(
                        encoded,
                        target,
                        LiveViewCheckpointStateCodec.TIMESTAMP_DELTA_OF_DELTA_VARINT,
                        2,
                        "trailing bytes"
                );

                encoded.truncate();
                encoded.putLong(0);
                for (int i = 0; i < 9; i++) {
                    encoded.putByte((byte) 0x80);
                }
                encoded.putByte((byte) 0x02);
                assertTimestampDecodeFails(
                        encoded,
                        target,
                        LiveViewCheckpointStateCodec.TIMESTAMP_DELTA_OF_DELTA_VARINT,
                        2,
                        "overflows 64 bits"
                );
            }
        });
    }

    @Test
    public void testRandomRoundTripProperty() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = new Rnd(0x9876_5432L, 0x1020_3040L);
            for (int iteration = 0; iteration < 200; iteration++) {
                final int size = rnd.nextInt(LiveViewCheckpointStateCodec.CHUNK_ROWS + 1);
                final long[] timestamps = new long[size];
                final long[] doubles = new long[size];
                long timestamp = rnd.nextLong() % 1_000_000_000L;
                for (int i = 0; i < size; i++) {
                    timestamp += rnd.nextPositiveInt() % 1000;
                    timestamps[i] = timestamp;
                    doubles[i] = rnd.nextLong();
                }
                assertTimestampRoundTrip(timestamps, LiveViewCheckpointStateCodec.TIMESTAMP_RAW_64);
                assertTimestampRoundTrip(timestamps, LiveViewCheckpointStateCodec.TIMESTAMP_DELTA_OF_DELTA_VARINT);
                assertDoubleRoundTrip(doubles, LiveViewCheckpointStateCodec.DOUBLE_RAW_64);
                assertDoubleRoundTrip(doubles, LiveViewCheckpointStateCodec.DOUBLE_XOR);
            }
        });
    }

    @Test
    public void testScratchIsLazyTrackedAndReused() throws Exception {
        assertMemoryLeak(() -> {
            final MemoryTracker tracker = engine.getMemoryTrackerProvider().acquire(
                    AllowAllSecurityContext.INSTANCE,
                    1,
                    MemoryTrackerWorkload.LIVE_VIEW_REFRESH
            );
            try (LiveViewCheckpointStateCodec.Scratch scratch = new LiveViewCheckpointStateCodec.Scratch(tracker)) {
                Assert.assertEquals(0, tracker.getUsed());
                final long timestamps = scratch.timestampsAddress();
                Assert.assertEquals((long) LiveViewCheckpointStateCodec.CHUNK_ROWS * Long.BYTES, tracker.getUsed());
                final long doubles = scratch.valuesAddress();
                Assert.assertEquals(2L * LiveViewCheckpointStateCodec.CHUNK_ROWS * Long.BYTES, tracker.getUsed());
                Assert.assertEquals(timestamps, scratch.timestampsAddress());
                Assert.assertEquals(doubles, scratch.valuesAddress());
                Assert.assertEquals(2L * LiveViewCheckpointStateCodec.CHUNK_ROWS * Long.BYTES, tracker.getUsed());
            }
            Assert.assertEquals(0, tracker.getUsed());
            tracker.close();
        });
    }

    @Test
    public void testTimestampExtremesAndMaximumDeltaRoundTrip() throws Exception {
        assertMemoryLeak(() -> {
            assertTimestampRoundTrip(
                    new long[]{Long.MIN_VALUE, -1},
                    LiveViewCheckpointStateCodec.TIMESTAMP_DELTA_OF_DELTA_VARINT
            );
            assertTimestampRoundTrip(
                    new long[]{Long.MAX_VALUE - 2, Long.MAX_VALUE - 1, Long.MAX_VALUE},
                    LiveViewCheckpointStateCodec.TIMESTAMP_DELTA_OF_DELTA_VARINT
            );
            assertTimestampRoundTrip(
                    new long[]{Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE + 1, Long.MIN_VALUE + 1},
                    LiveViewCheckpointStateCodec.TIMESTAMP_DELTA_OF_DELTA_VARINT
            );
            assertTimestampRoundTrip(
                    new long[]{Long.MIN_VALUE, 0, Long.MAX_VALUE},
                    LiveViewCheckpointStateCodec.TIMESTAMP_RAW_64
            );
            // Delta-of-delta 64 zigzags to 128, whose canonical encoding spends a
            // first byte carrying no payload bits at all. Only the terminator says
            // whether an encoding is canonical, so this must round-trip rather than
            // read as the padding the case above rejects.
            assertTimestampRoundTrip(
                    new long[]{0, 100, 264, 8_620},
                    LiveViewCheckpointStateCodec.TIMESTAMP_DELTA_OF_DELTA_VARINT
            );
        });
    }

    private static void assertDoubleDecodeFails(
            MemoryCARW encoded,
            long targetAddress,
            int codec,
            int rowCount,
            CharSequence message
    ) {
        try {
            LiveViewCheckpointStateCodec.decodeDoubles(
                    encoded.addressOf(0),
                    (int) encoded.getAppendOffset(),
                    codec,
                    rowCount,
                    targetAddress,
                    LiveViewCheckpointStateCodec.CHUNK_ROWS
            );
            Assert.fail("expected malformed double stream rejection");
        } catch (CairoException e) {
            Assert.assertEquals(CairoException.LV_CHECKPOINT_TIMELINE_INVALID, e.getErrno());
            TestUtils.assertContains(e.getFlyweightMessage(), message);
        }
    }

    private static void assertDoubleRoundTrip(long[] values, int codec) {
        try (LiveViewCheckpointStateCodec.Scratch source = new LiveViewCheckpointStateCodec.Scratch(null);
             LiveViewCheckpointStateCodec.Scratch target = new LiveViewCheckpointStateCodec.Scratch(null);
             MemoryCARW encoded = Vm.getCARWInstance(4096, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            final long sourceAddress = source.valuesAddress();
            final long targetAddress = target.valuesAddress();
            put(sourceAddress, values);
            final int written = LiveViewCheckpointStateCodec.encodeDoubles(encoded, sourceAddress, values.length, codec);
            Assert.assertEquals(encoded.getAppendOffset(), written);
            Assert.assertEquals(
                    written,
                    LiveViewCheckpointStateCodec.decodeDoubles(
                            encoded.addressOf(0),
                            written,
                            codec,
                            values.length,
                            targetAddress,
                            LiveViewCheckpointStateCodec.CHUNK_ROWS
                    )
            );
            assertBitsEqual(values, targetAddress);
        }
    }

    private static void assertTimestampDecodeFails(
            MemoryCARW encoded,
            long targetAddress,
            int codec,
            int rowCount,
            CharSequence message
    ) {
        try {
            LiveViewCheckpointStateCodec.decodeTimestamps(
                    encoded.addressOf(0),
                    (int) encoded.getAppendOffset(),
                    codec,
                    rowCount,
                    targetAddress,
                    LiveViewCheckpointStateCodec.CHUNK_ROWS
            );
            Assert.fail("expected malformed timestamp stream rejection");
        } catch (CairoException e) {
            Assert.assertEquals(CairoException.LV_CHECKPOINT_TIMELINE_INVALID, e.getErrno());
            TestUtils.assertContains(e.getFlyweightMessage(), message);
        }
    }

    private static void assertTimestampRoundTrip(long[] values, int codec) {
        try (LiveViewCheckpointStateCodec.Scratch source = new LiveViewCheckpointStateCodec.Scratch(null);
             LiveViewCheckpointStateCodec.Scratch target = new LiveViewCheckpointStateCodec.Scratch(null);
             MemoryCARW encoded = Vm.getCARWInstance(4096, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            final long sourceAddress = source.timestampsAddress();
            final long targetAddress = target.timestampsAddress();
            put(sourceAddress, values);
            final int written = LiveViewCheckpointStateCodec.encodeTimestamps(encoded, sourceAddress, values.length, codec);
            Assert.assertEquals(encoded.getAppendOffset(), written);
            Assert.assertEquals(
                    written,
                    LiveViewCheckpointStateCodec.decodeTimestamps(
                            encoded.addressOf(0),
                            written,
                            codec,
                            values.length,
                            targetAddress,
                            LiveViewCheckpointStateCodec.CHUNK_ROWS
                    )
            );
            assertBitsEqual(values, targetAddress);
        }
    }

    private static void assertBitsEqual(long[] expected, long actualAddress) {
        for (int i = 0; i < expected.length; i++) {
            Assert.assertEquals("row " + i, expected[i], Unsafe.getLong(actualAddress + (long) i * Long.BYTES));
        }
    }

    private static int encodeDoubles(
            MemoryCARW encoded,
            LiveViewCheckpointStateCodec.Scratch scratch,
            long[] values,
            int codec
    ) {
        encoded.truncate();
        final long sourceAddress = scratch.valuesAddress();
        put(sourceAddress, values);
        return LiveViewCheckpointStateCodec.encodeDoubles(encoded, sourceAddress, values.length, codec);
    }

    private static void put(long address, long... values) {
        for (int i = 0; i < values.length; i++) {
            Unsafe.putLong(address + (long) i * Long.BYTES, values[i]);
        }
    }
}
