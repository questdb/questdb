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
import io.questdb.cairo.idx.CoveringCompressor;
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

    private static final int CHUNK_ROWS = LiveViewCheckpointStateCodec.CHUNK_ROWS;
    // One page large enough to hold any encoded chunk contiguously, which is what
    // lets a test read an encoded page back through a single address.
    private static final int SINK_PAGE_SIZE = 1024 * 1024;

    @Test
    public void testAllMaxValueStridesKeepTheirBase() throws Exception {
        assertMemoryLeak(() -> {
            // A LONG value ring carries raw column payloads, so a frame whose rows
            // all hold Long.MAX_VALUE hands the plain-FoR encoder a stride whose
            // minimum equals the seed that encoder starts its scan from. The block
            // must store that minimum as its base rather than reset it: the page is
            // its 13-byte header alone, and every row has to decode back to
            // Long.MAX_VALUE instead of 0.
            final long[] allMax = new long[64];
            Arrays.fill(allMax, Long.MAX_VALUE);
            assertLongSelection(allMax, LiveViewCheckpointStateCodec.COVERING_LONG,
                    CoveringCompressor.LONG_HEADER_SIZE);

            // Two rows are the smallest page the covering block wins, and the
            // smallest one that can carry the clobbered base.
            assertLongSelection(new long[]{Long.MAX_VALUE, Long.MAX_VALUE},
                    LiveViewCheckpointStateCodec.COVERING_LONG, CoveringCompressor.LONG_HEADER_SIZE);

            // One row cannot pay for the header, so raw wins and the covering
            // encoder's output never reaches the page at all.
            assertLongSelection(new long[]{Long.MAX_VALUE},
                    LiveViewCheckpointStateCodec.RAW_64, Long.BYTES);

            // The same stride under the timestamp page's three-way selection. The
            // linear candidate fits a zero stride and zero residuals, but its
            // 29-byte header cannot beat the plain block's 13.
            assertTimestampSelection(allMax, LiveViewCheckpointStateCodec.COVERING_LONG,
                    CoveringCompressor.LONG_HEADER_SIZE);
        });
    }

    @Test
    public void testCorruptCoveringDoublePagesAreRejected() throws Exception {
        assertMemoryLeak(() -> {
            final long[] values = new long[256];
            for (int i = 0; i < values.length; i++) {
                values[i] = Double.doubleToRawLongBits(100.0 + i);
            }
            try (LiveViewCheckpointStateCodec.Scratch scratch = new LiveViewCheckpointStateCodec.Scratch(null);
                 MemoryCARW encoded = sink()) {
                final long source = scratch.valuesAddress();
                put(source, values);
                final int codec = LiveViewCheckpointStateCodec.encodeDoubles(encoded, scratch, source, values.length);
                Assert.assertEquals(LiveViewCheckpointStateCodec.COVERING_DOUBLE, codec);
                final int storedLength = (int) encoded.getAppendOffset();

                // Every rejection is the checked covering decoder's, relabelled by
                // the adapter rather than swallowed.
                assertDoubleDecodeFails(encoded, storedLength - 1, codec, values.length, "block length mismatch");
                assertDoubleDecodeFails(encoded, storedLength, codec, values.length - 1, "block count mismatch");
                assertDoubleDecodeFails(encoded, CoveringCompressor.DOUBLE_HEADER_SIZE - 1, codec, values.length,
                        "truncated block header");

                // An ALP exponent outside the power tables and an impossible bit
                // width are both caught before a single value is decoded.
                final byte exponent = encoded.getByte(4);
                encoded.putByte(4, (byte) 100);
                assertDoubleDecodeFails(encoded, storedLength, codec, values.length, "invalid ALP exponent");
                encoded.putByte(4, exponent);
                final byte bitWidth = encoded.getByte(6);
                encoded.putByte(6, (byte) 65);
                assertDoubleDecodeFails(encoded, storedLength, codec, values.length, "invalid bit width");
                encoded.putByte(6, bitWidth);

                // The page is intact again, so it must still decode exactly.
                try (LiveViewCheckpointStateCodec.Scratch target = new LiveViewCheckpointStateCodec.Scratch(null)) {
                    Assert.assertEquals(storedLength, LiveViewCheckpointStateCodec.decodeDoubles(
                            encoded.addressOf(0), storedLength, codec, values.length,
                            target.valuesAddress(), CHUNK_ROWS, target
                    ));
                    assertBitsEqual(values, target.valuesAddress());
                }
            }
        });
    }

    @Test
    public void testCorruptCoveringLongPagesAreRejected() throws Exception {
        assertMemoryLeak(() -> {
            final long[] values = new long[128];
            for (int i = 0; i < values.length; i++) {
                values[i] = 1_000_000L + i * 7L;
            }
            try (LiveViewCheckpointStateCodec.Scratch scratch = new LiveViewCheckpointStateCodec.Scratch(null);
                 MemoryCARW encoded = sink()) {
                final long source = scratch.timestampsAddress();
                put(source, values);
                final int codec = LiveViewCheckpointStateCodec.encodeTimestamps(encoded, scratch, source, values.length);
                Assert.assertEquals(LiveViewCheckpointStateCodec.COVERING_LONG, codec);
                final int storedLength = (int) encoded.getAppendOffset();

                assertLongDecodeFails(encoded, storedLength + 1, codec, values.length, "block length mismatch");
                assertLongDecodeFails(encoded, storedLength, codec, values.length + 1, "block count mismatch");
                assertLongDecodeFails(encoded, CoveringCompressor.LONG_HEADER_SIZE - 1, codec, values.length,
                        "truncated block header");

                // The top bit alone is not the linear-prediction flag, so the byte
                // reads as a plain block of impossible width.
                final byte flags = encoded.getByte(4);
                encoded.putByte(4, (byte) 0x80);
                assertLongDecodeFails(encoded, storedLength, codec, values.length, "invalid bit width");
                encoded.putByte(4, flags);

                try (LiveViewCheckpointStateCodec.Scratch target = new LiveViewCheckpointStateCodec.Scratch(null)) {
                    Assert.assertEquals(storedLength, LiveViewCheckpointStateCodec.decodeLongs(
                            encoded.addressOf(0), storedLength, codec, values.length,
                            target.timestampsAddress(), CHUNK_ROWS, target
                    ));
                    assertBitsEqual(values, target.timestampsAddress());
                }
            }
        });
    }

    @Test
    public void testDoubleBitPatternsRoundTripExactly() throws Exception {
        assertMemoryLeak(() -> {
            // ALP stores whatever it cannot transform bit-exactly as an exception,
            // so a NaN payload, an infinity and a signed zero must come back
            // verbatim whichever codec the page ends up under. Long.MAX_VALUE is
            // in the list because it is the encoder's own "not encodable" marker:
            // read as a double it is a NaN, so it leaves through the exception
            // list and never reaches the block's FoR base.
            final long[] bits = {
                    0L,
                    Long.MIN_VALUE,
                    Long.MAX_VALUE,
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
            assertDoubleRoundTrip(bits, -1);
            for (int size : new int[]{0, 1, 2, CHUNK_ROWS - 1, CHUNK_ROWS}) {
                final long[] prices = new long[size];
                for (int i = 0; i < size; i++) {
                    prices[i] = Double.doubleToRawLongBits(100.0 + (i % 997) * 0.01);
                }
                assertDoubleRoundTrip(prices, -1);
            }
        });
    }

    @Test
    public void testDoubleSelectionByExactSize() throws Exception {
        assertMemoryLeak(() -> {
            // A constant decimal double transforms to one repeated ALP word, so the
            // whole chunk costs its 19-byte header and no packed bits at all.
            final long[] constant = new long[CHUNK_ROWS];
            Arrays.fill(constant, Double.doubleToRawLongBits(42.5));
            assertDoubleSelection(constant, LiveViewCheckpointStateCodec.COVERING_DOUBLE, 19);

            // A decimal price series transforms to 18-bit ALP words with a handful
            // of values the transform cannot reproduce exactly: 19 header bytes,
            // 9216 packed bytes and 43 exceptions at 12 bytes each.
            final long[] prices = new long[CHUNK_ROWS];
            for (int i = 0; i < prices.length; i++) {
                prices[i] = Double.doubleToRawLongBits(100.0 + i * 0.01);
            }
            assertDoubleSelection(prices, LiveViewCheckpointStateCodec.COVERING_DOUBLE,
                    CoveringCompressor.DOUBLE_HEADER_SIZE + (CHUNK_ROWS * 18) / 8 + 43 * (Integer.BYTES + Double.BYTES));

            // A repeated NaN payload is what ALP cannot represent at all: every
            // value becomes an exception, which is longer than storing raw. This is
            // the regression the bespoke XOR codec used to win, and does not any
            // more.
            final long[] nans = new long[CHUNK_ROWS];
            Arrays.fill(nans, 0x7ff8_dead_beef_1234L);
            assertDoubleSelection(nans, LiveViewCheckpointStateCodec.RAW_64, CHUNK_ROWS * Long.BYTES);
        });
    }

    @Test
    public void testLongSelectionByExactSize() throws Exception {
        assertMemoryLeak(() -> {
            final long[] constant = new long[CHUNK_ROWS];
            Arrays.fill(constant, -12345L);
            assertLongSelection(constant, LiveViewCheckpointStateCodec.COVERING_LONG,
                    CoveringCompressor.LONG_HEADER_SIZE);

            final long[] narrow = new long[CHUNK_ROWS];
            for (int i = 0; i < narrow.length; i++) {
                narrow[i] = i % 1024;
            }
            assertLongSelection(narrow, LiveViewCheckpointStateCodec.COVERING_LONG,
                    CoveringCompressor.LONG_HEADER_SIZE + CHUNK_ROWS * 10 / 8);

            // A stream that spans the whole 64-bit range packs no narrower than it
            // already is, so the FoR header alone makes the block longer than raw.
            final long[] wide = new long[CHUNK_ROWS];
            for (int i = 0; i < wide.length; i++) {
                wide[i] = (i & 1) == 0 ? Long.MIN_VALUE + i : Long.MAX_VALUE - i;
            }
            assertLongSelection(wide, LiveViewCheckpointStateCodec.RAW_64, CHUNK_ROWS * Long.BYTES);
        });
    }

    @Test
    public void testRandomRoundTripProperty() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = new Rnd(0x9876_5432L, 0x1020_3040L);
            for (int iteration = 0; iteration < 100; iteration++) {
                final int size = rnd.nextInt(CHUNK_ROWS + 1);
                final long[] timestamps = new long[size];
                final long[] doubles = new long[size];
                final long[] longs = new long[size];
                long timestamp = rnd.nextLong() % 1_000_000_000L;
                for (int i = 0; i < size; i++) {
                    timestamp += rnd.nextPositiveInt() % 1000;
                    timestamps[i] = timestamp;
                    doubles[i] = rnd.nextLong();
                    longs[i] = rnd.nextLong() >> (rnd.nextPositiveInt() % 64);
                }
                assertTimestampRoundTrip(timestamps, -1);
                assertDoubleRoundTrip(doubles, -1);
                assertLongRoundTrip(longs, -1);
            }
        });
    }

    @Test
    public void testRawWinsSizeTies() throws Exception {
        assertMemoryLeak(() -> {
            // Two values spanning nine bits pack into exactly the 16 raw bytes they
            // came from: the tie must go to raw, which is what keeps a page from
            // ever exceeding its decoded payload.
            final long[] tied = {0, 511};
            Assert.assertEquals(
                    CoveringCompressor.LONG_HEADER_SIZE + 3,
                    tied.length * Long.BYTES
            );
            assertLongSelection(tied, LiveViewCheckpointStateCodec.RAW_64, tied.length * Long.BYTES);
            assertTimestampSelection(tied, LiveViewCheckpointStateCodec.RAW_64, tied.length * Long.BYTES);
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
            try (LiveViewCheckpointStateCodec.Scratch scratch = new LiveViewCheckpointStateCodec.Scratch(tracker);
                 MemoryCARW encoded = sink()) {
                Assert.assertEquals(0, tracker.getUsed());
                final long timestamps = scratch.timestampsAddress();
                Assert.assertEquals((long) CHUNK_ROWS * Long.BYTES, tracker.getUsed());
                final long values = scratch.valuesAddress();
                Assert.assertEquals(2L * CHUNK_ROWS * Long.BYTES, tracker.getUsed());
                Assert.assertEquals(timestamps, scratch.timestampsAddress());
                Assert.assertEquals(values, scratch.valuesAddress());
                Assert.assertEquals(2L * CHUNK_ROWS * Long.BYTES, tracker.getUsed());

                // The encoder region is the largest of them and opens only when a
                // page is actually encoded, so a restore-only reader never pays for
                // it. A second page reuses it rather than growing the charge.
                for (int i = 0; i < CHUNK_ROWS; i++) {
                    Unsafe.putLong(timestamps + (long) i * Long.BYTES, i * 1_000L);
                }
                LiveViewCheckpointStateCodec.encodeTimestamps(encoded, scratch, timestamps, CHUNK_ROWS);
                final long encodeScratch = tracker.getUsed();
                Assert.assertTrue(encodeScratch > 2L * CHUNK_ROWS * Long.BYTES);
                LiveViewCheckpointStateCodec.encodeDoubles(encoded, scratch, values, CHUNK_ROWS);
                Assert.assertEquals(encodeScratch, tracker.getUsed());
            }
            Assert.assertEquals(0, tracker.getUsed());
            tracker.close();
        });
    }

    @Test
    public void testTimestampSelectionByExactSize() throws Exception {
        assertMemoryLeak(() -> {
            // A regularly spaced full chunk is exactly what linear prediction is
            // for: the residuals are all zero, so the block is its header alone.
            final long[] regular = new long[CHUNK_ROWS];
            for (int i = 0; i < regular.length; i++) {
                regular[i] = 1_700_000_000_000_000L + i * 1_000L;
            }
            assertTimestampSelection(regular, LiveViewCheckpointStateCodec.COVERING_LONG,
                    CoveringCompressor.LONG_LINEAR_PRED_HEADER_SIZE);

            // Three rows are too few to pay the 29-byte linear header, so the
            // 13-byte plain header wins even though the residuals are still zero.
            assertTimestampSelection(new long[]{100, 200, 300}, LiveViewCheckpointStateCodec.COVERING_LONG,
                    CoveringCompressor.LONG_HEADER_SIZE + 3);

            // One row cannot pay for any header.
            assertTimestampSelection(new long[]{100}, LiveViewCheckpointStateCodec.RAW_64, Long.BYTES);

            // A stream spanning the full 64-bit range overflows the linear residual
            // width, and plain FoR cannot beat raw either.
            assertTimestampSelection(new long[]{Long.MIN_VALUE, Long.MAX_VALUE},
                    LiveViewCheckpointStateCodec.RAW_64, 2 * Long.BYTES);

            // Duplicate timestamps are legal in the ring, and a jittered cadence is
            // the common case: both still round-trip through whichever layout wins.
            final long[] jittered = new long[CHUNK_ROWS];
            long timestamp = 1_700_000_000_000_000L;
            for (int i = 0; i < jittered.length; i++) {
                timestamp += (i % 7) * 100L;
                jittered[i] = timestamp;
            }
            assertTimestampRoundTrip(jittered, LiveViewCheckpointStateCodec.COVERING_LONG);
        });
    }

    @Test
    public void testUnknownCodecTagsAndRawLengthsAreRejected() throws Exception {
        assertMemoryLeak(() -> {
            try (LiveViewCheckpointStateCodec.Scratch scratch = new LiveViewCheckpointStateCodec.Scratch(null);
                 MemoryCARW encoded = sink()) {
                final long target = scratch.valuesAddress();
                encoded.putLong(7);

                // Only the three format-1 tags exist, and a page kind accepts one
                // covering family, not both.
                assertDecodeFails(() -> LiveViewCheckpointStateCodec.decodeLongs(
                        encoded.addressOf(0), 8, 3, 1, target, CHUNK_ROWS, scratch
                ), "unknown long codec tag");
                assertDecodeFails(() -> LiveViewCheckpointStateCodec.decodeLongs(
                        encoded.addressOf(0), 8, LiveViewCheckpointStateCodec.COVERING_DOUBLE, 1, target, CHUNK_ROWS, scratch
                ), "unknown long codec tag");
                assertDecodeFails(() -> LiveViewCheckpointStateCodec.decodeDoubles(
                        encoded.addressOf(0), 8, LiveViewCheckpointStateCodec.COVERING_LONG, 1, target, CHUNK_ROWS, scratch
                ), "unknown double codec tag");

                // A raw page's stored length is fully determined by its row count.
                assertDecodeFails(() -> LiveViewCheckpointStateCodec.decodeLongs(
                        encoded.addressOf(0), 8, LiveViewCheckpointStateCodec.RAW_64, 2, target, CHUNK_ROWS, scratch
                ), "raw long page length mismatch");
                assertDecodeFails(() -> LiveViewCheckpointStateCodec.decodeDoubles(
                        encoded.addressOf(0), 8, LiveViewCheckpointStateCodec.RAW_64, 2, target, CHUNK_ROWS, scratch
                ), "raw double page length mismatch");

                assertDecodeFails(() -> LiveViewCheckpointStateCodec.decodeLongs(
                        encoded.addressOf(0), 8, LiveViewCheckpointStateCodec.RAW_64, -1, target, CHUNK_ROWS, scratch
                ), "row count out of bounds");
                assertDecodeFails(() -> LiveViewCheckpointStateCodec.decodeLongs(
                        encoded.addressOf(0), 8, LiveViewCheckpointStateCodec.RAW_64, CHUNK_ROWS + 1, target,
                        CHUNK_ROWS, scratch
                ), "row count out of bounds");
                assertDecodeFails(() -> LiveViewCheckpointStateCodec.decodeLongs(
                        encoded.addressOf(0), 8, LiveViewCheckpointStateCodec.RAW_64, 1, target, 0, scratch
                ), "decode target capacity too small");

                // The encoder rejects the same bounds before it writes anything.
                try {
                    LiveViewCheckpointStateCodec.encodeLongs(encoded, scratch, target, CHUNK_ROWS + 1);
                    Assert.fail("expected row count rejection");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "row count out of bounds");
                }
            }
        });
    }

    private static void assertBitsEqual(long[] expected, long actualAddress) {
        for (int i = 0; i < expected.length; i++) {
            Assert.assertEquals("row " + i, expected[i], Unsafe.getLong(actualAddress + (long) i * Long.BYTES));
        }
    }

    private static void assertDecodeFails(Decode decode, CharSequence message) {
        try {
            decode.run();
            Assert.fail("expected malformed page rejection");
        } catch (CairoException e) {
            Assert.assertEquals(CairoException.LV_CHECKPOINT_TIMELINE_INVALID, e.getErrno());
            TestUtils.assertContains(e.getFlyweightMessage(), message);
        }
    }

    private static void assertDoubleDecodeFails(
            MemoryCARW encoded,
            int storedLength,
            int codec,
            int rowCount,
            CharSequence message
    ) {
        try (LiveViewCheckpointStateCodec.Scratch target = new LiveViewCheckpointStateCodec.Scratch(null)) {
            assertDecodeFails(() -> LiveViewCheckpointStateCodec.decodeDoubles(
                    encoded.addressOf(0), storedLength, codec, rowCount,
                    target.valuesAddress(), CHUNK_ROWS, target
            ), message);
        }
    }

    /**
     * Round-trips a double page through whichever codec the adapter selects,
     * asserting the exact bits and the format's hard size invariant. Pass
     * {@code expectedCodec} as -1 when the selection is not the point of the test.
     */
    private static void assertDoubleRoundTrip(long[] values, int expectedCodec) {
        try (LiveViewCheckpointStateCodec.Scratch source = new LiveViewCheckpointStateCodec.Scratch(null);
             LiveViewCheckpointStateCodec.Scratch target = new LiveViewCheckpointStateCodec.Scratch(null);
             MemoryCARW encoded = sink()) {
            final long sourceAddress = source.valuesAddress();
            put(sourceAddress, values);
            final int codec = LiveViewCheckpointStateCodec.encodeDoubles(encoded, source, sourceAddress, values.length);
            final int storedLength = assertStoredLength(encoded, values.length, codec, expectedCodec);
            Assert.assertEquals(storedLength, LiveViewCheckpointStateCodec.decodeDoubles(
                    encoded.addressOf(0), storedLength, codec, values.length,
                    target.valuesAddress(), CHUNK_ROWS, target
            ));
            assertBitsEqual(values, target.valuesAddress());
        }
    }

    private static void assertDoubleSelection(long[] values, int expectedCodec, int expectedStoredLength) {
        try (LiveViewCheckpointStateCodec.Scratch scratch = new LiveViewCheckpointStateCodec.Scratch(null);
             MemoryCARW encoded = sink()) {
            final long sourceAddress = scratch.valuesAddress();
            put(sourceAddress, values);
            Assert.assertEquals(
                    expectedCodec,
                    LiveViewCheckpointStateCodec.encodeDoubles(encoded, scratch, sourceAddress, values.length)
            );
            Assert.assertEquals(expectedStoredLength, encoded.getAppendOffset());
        }
        assertDoubleRoundTrip(values, expectedCodec);
    }

    private static void assertLongDecodeFails(
            MemoryCARW encoded,
            int storedLength,
            int codec,
            int rowCount,
            CharSequence message
    ) {
        try (LiveViewCheckpointStateCodec.Scratch target = new LiveViewCheckpointStateCodec.Scratch(null)) {
            assertDecodeFails(() -> LiveViewCheckpointStateCodec.decodeLongs(
                    encoded.addressOf(0), storedLength, codec, rowCount,
                    target.timestampsAddress(), CHUNK_ROWS, target
            ), message);
        }
    }

    private static void assertLongRoundTrip(long[] values, int expectedCodec) {
        try (LiveViewCheckpointStateCodec.Scratch source = new LiveViewCheckpointStateCodec.Scratch(null);
             LiveViewCheckpointStateCodec.Scratch target = new LiveViewCheckpointStateCodec.Scratch(null);
             MemoryCARW encoded = sink()) {
            final long sourceAddress = source.valuesAddress();
            put(sourceAddress, values);
            final int codec = LiveViewCheckpointStateCodec.encodeLongs(encoded, source, sourceAddress, values.length);
            final int storedLength = assertStoredLength(encoded, values.length, codec, expectedCodec);
            Assert.assertEquals(storedLength, LiveViewCheckpointStateCodec.decodeLongs(
                    encoded.addressOf(0), storedLength, codec, values.length,
                    target.valuesAddress(), CHUNK_ROWS, target
            ));
            assertBitsEqual(values, target.valuesAddress());
        }
    }

    private static void assertLongSelection(long[] values, int expectedCodec, int expectedStoredLength) {
        try (LiveViewCheckpointStateCodec.Scratch scratch = new LiveViewCheckpointStateCodec.Scratch(null);
             MemoryCARW encoded = sink()) {
            final long sourceAddress = scratch.valuesAddress();
            put(sourceAddress, values);
            Assert.assertEquals(
                    expectedCodec,
                    LiveViewCheckpointStateCodec.encodeLongs(encoded, scratch, sourceAddress, values.length)
            );
            Assert.assertEquals(expectedStoredLength, encoded.getAppendOffset());
        }
        assertLongRoundTrip(values, expectedCodec);
    }

    private static int assertStoredLength(MemoryCARW encoded, int rowCount, int codec, int expectedCodec) {
        if (expectedCodec >= 0) {
            Assert.assertEquals(expectedCodec, codec);
        }
        final int storedLength = (int) encoded.getAppendOffset();
        // The invariant the whole selection exists to hold: a page is never larger
        // than the payload it decodes to.
        Assert.assertTrue(
                "storedLength=" + storedLength + ", decodedLength=" + rowCount * Long.BYTES,
                storedLength <= rowCount * Long.BYTES
        );
        return storedLength;
    }

    private static void assertTimestampRoundTrip(long[] values, int expectedCodec) {
        try (LiveViewCheckpointStateCodec.Scratch source = new LiveViewCheckpointStateCodec.Scratch(null);
             LiveViewCheckpointStateCodec.Scratch target = new LiveViewCheckpointStateCodec.Scratch(null);
             MemoryCARW encoded = sink()) {
            final long sourceAddress = source.timestampsAddress();
            put(sourceAddress, values);
            final int codec = LiveViewCheckpointStateCodec.encodeTimestamps(
                    encoded, source, sourceAddress, values.length
            );
            final int storedLength = assertStoredLength(encoded, values.length, codec, expectedCodec);
            Assert.assertEquals(storedLength, LiveViewCheckpointStateCodec.decodeLongs(
                    encoded.addressOf(0), storedLength, codec, values.length,
                    target.timestampsAddress(), CHUNK_ROWS, target
            ));
            assertBitsEqual(values, target.timestampsAddress());
        }
    }

    private static void assertTimestampSelection(long[] values, int expectedCodec, int expectedStoredLength) {
        try (LiveViewCheckpointStateCodec.Scratch scratch = new LiveViewCheckpointStateCodec.Scratch(null);
             MemoryCARW encoded = sink()) {
            final long sourceAddress = scratch.timestampsAddress();
            put(sourceAddress, values);
            Assert.assertEquals(
                    expectedCodec,
                    LiveViewCheckpointStateCodec.encodeTimestamps(encoded, scratch, sourceAddress, values.length)
            );
            Assert.assertEquals(expectedStoredLength, encoded.getAppendOffset());
        }
        assertTimestampRoundTrip(values, expectedCodec);
    }

    private static void put(long address, long... values) {
        for (int i = 0; i < values.length; i++) {
            Unsafe.putLong(address + (long) i * Long.BYTES, values[i]);
        }
    }

    private static MemoryCARW sink() {
        return Vm.getCARWInstance(SINK_PAGE_SIZE, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
    }

    @FunctionalInterface
    private interface Decode {
        void run();
    }
}
