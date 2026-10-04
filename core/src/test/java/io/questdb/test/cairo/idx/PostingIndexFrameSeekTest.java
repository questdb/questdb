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

package io.questdb.test.cairo.idx;

import io.questdb.cairo.idx.PostingIndexBwdReader;
import io.questdb.cairo.idx.PostingIndexFwdReader;
import io.questdb.cairo.idx.PostingIndexUtils;
import io.questdb.cairo.idx.PostingIndexWriter;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE;

/**
 * Row-id identity of the posting-index cursors when one key's list is read as a sequence of
 * page frames, i.e. one {@code getCursor(key, frameLo, frameHi)} per frame, which is how
 * {@code PageFrameRecordCursorImpl} drives the index. Every frame's output must equal the
 * oracle (the row ids written for the key, clipped to the frame), in both directions, for
 * EF / delta / adaptive encodings, ranked and legacy (unranked) EF blobs, sealed (dense),
 * unsealed (sparse) and mixed generation layouts, with and without a column top, for keys
 * that are large, small, bursty, periodic, absent from part of the partition, or absent.
 * <p>
 * Frame boundaries are chosen to land at, just before and just after values, on delta-block
 * edges (every 64th value of a key), between values, and in empty stretches; frames are
 * visited in scan order (ascending for forward, descending for backward, reusing the pooled
 * cursor as the page-frame cursor does) and in random order.
 */
public class PostingIndexFrameSeekTest extends AbstractCairoTest {
    private static final int KEY_ABSENT = 6 * 256 + 1;
    private static final int KEY_BURSTY = 3 * 256 + 1;
    private static final int KEY_HALF = 5 * 256 + 1;
    // Special keys live in their own dense strides (DENSE_STRIDE = 256) so a sealed gen stores
    // them as per-key EF/delta blobs instead of folding them into one flat stride with the
    // random filler keys of stride 0.
    private static final int KEY_LARGE = 256 + 1;
    private static final int KEY_LAST = 7 * 256 + 1;
    private static final int KEY_PERIODIC = 4 * 256 + 1;
    private static final int KEY_SMALL = 2 * 256 + 1;
    private static final int[] CHECKED_KEYS = {0, 9, 17, KEY_LARGE, KEY_SMALL, KEY_BURSTY, KEY_PERIODIC, KEY_HALF, KEY_ABSENT, KEY_LAST};
    private static final int FILLER_KEY_COUNT = 40;
    private static final int LAYOUT_MIXED = 2;
    private static final int LAYOUT_SEALED = 0;
    private static final int LAYOUT_SPARSE = 1;
    private static final int ROW_COUNT = 160_000;

    @Test
    public void testAdaptiveMixed() throws Exception {
        assertFrames(PostingIndexUtils.ENCODING_ADAPTIVE, true, LAYOUT_MIXED, 0);
    }

    @Test
    public void testAdaptiveSealedColumnTop() throws Exception {
        assertFrames(PostingIndexUtils.ENCODING_ADAPTIVE, true, LAYOUT_SEALED, 12_345);
    }

    @Test
    public void testDeltaMixedColumnTop() throws Exception {
        assertFrames(PostingIndexUtils.ENCODING_DELTA, true, LAYOUT_MIXED, 777);
    }

    @Test
    public void testDeltaSealed() throws Exception {
        assertFrames(PostingIndexUtils.ENCODING_DELTA, true, LAYOUT_SEALED, 0);
    }

    @Test
    public void testDeltaSparse() throws Exception {
        assertFrames(PostingIndexUtils.ENCODING_DELTA, true, LAYOUT_SPARSE, 0);
    }

    @Test
    public void testEfLegacyMixed() throws Exception {
        assertFrames(PostingIndexUtils.ENCODING_EF, false, LAYOUT_MIXED, 0);
    }

    @Test
    public void testEfLegacySealedColumnTop() throws Exception {
        assertFrames(PostingIndexUtils.ENCODING_EF, false, LAYOUT_SEALED, 4_096);
    }

    @Test
    public void testEfLegacySparse() throws Exception {
        assertFrames(PostingIndexUtils.ENCODING_EF, false, LAYOUT_SPARSE, 0);
    }

    @Test
    public void testEfLowerBoundWordMatchesBruteForce() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);
            final int maxCount = 20_000;
            final long src = Unsafe.malloc((long) maxCount * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            final long dstSize = PostingIndexUtils.computeMaxEncodedSize(maxCount);
            final long dst = Unsafe.malloc(dstSize, MemoryTag.NATIVE_DEFAULT);
            try (PostingIndexUtils.EncodeContext ctx = new PostingIndexUtils.EncodeContext()) {
                for (int iter = 0; iter < 60; iter++) {
                    final int count = 1 + rnd.nextInt(iter < 10 ? 70 : maxCount);
                    // Mix of tight and sparse gaps plus long consecutive runs, so the high vector
                    // has empty words, full words and high buckets spanning several words.
                    long v = rnd.nextInt(1000);
                    final int gapScale = 1 + rnd.nextInt(5_000);
                    final long[] values = new long[count];
                    for (int i = 0; i < count; i++) {
                        values[i] = v;
                        Unsafe.putLong(src + (long) i * Long.BYTES, v);
                        v += rnd.nextInt(10) < 3 ? 1 : 1 + rnd.nextInt(gapScale);
                    }
                    ctx.ensureCapacity(count);
                    final int size = PostingIndexUtils.encodeKeyEF(src, count, dst, ctx);
                    final int bitsL = Unsafe.getByte(dst + 8) & 0xFF;
                    final long highStart = dst + 17 + ((((long) count * bitsL + 63) >>> 6) << 3);
                    final long universe = values[count - 1] + 1;
                    for (int t = 0; t < 2_000; t++) {
                        final long target = switch (t % 4) {
                            case 0 -> values[rnd.nextInt(count)];
                            case 1 -> values[rnd.nextInt(count)] + 1;
                            case 2 -> values[rnd.nextInt(count)] - 1;
                            default -> rnd.nextLong(universe + 2);
                        };
                        int expected = 0;
                        while (expected < count && values[expected] < target) {
                            expected++;
                        }
                        final int ordinal = PostingIndexUtils.efLowerBound(dst, size, target);
                        Assert.assertEquals("count=" + count + ", target=" + target, expected, ordinal);
                        if (ordinal < 0 || ordinal >= count) {
                            continue;
                        }
                        final long packed = PostingIndexUtils.efLowerBoundWord(dst, target, ordinal);
                        final int word = (int) (packed >>> 32);
                        final int rank = (int) packed;
                        int bruteRank = 0;
                        for (int w = 0; w < word; w++) {
                            bruteRank += Long.bitCount(Unsafe.getLong(highStart + (long) w * Long.BYTES));
                        }
                        final String label = "count=" + count + ", target=" + target + ", ordinal=" + ordinal;
                        Assert.assertEquals(label, bruteRank, rank);
                        Assert.assertTrue(label, rank <= ordinal && ordinal - rank <= 64);
                        // the ordinal's own one bit is at or after the start word
                        final long ordinalBit = (values[ordinal] >>> bitsL) + ordinal;
                        Assert.assertTrue(label, ordinalBit >= (long) word * 64);
                    }
                }
            } finally {
                Unsafe.free(src, (long) maxCount * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(dst, dstSize, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testEfRankedMixedColumnTop() throws Exception {
        assertFrames(PostingIndexUtils.ENCODING_EF, true, LAYOUT_MIXED, 12_345);
    }

    @Test
    public void testEfRankedSealed() throws Exception {
        assertFrames(PostingIndexUtils.ENCODING_EF, true, LAYOUT_SEALED, 0);
    }

    @Test
    public void testEfRankedSealedColumnTop() throws Exception {
        assertFrames(PostingIndexUtils.ENCODING_EF, true, LAYOUT_SEALED, 1_000);
    }

    @Test
    public void testEfRankedSparse() throws Exception {
        assertFrames(PostingIndexUtils.ENCODING_EF, true, LAYOUT_SPARSE, 0);
    }

    @Test
    public void testEfRankedSparseColumnTop() throws Exception {
        assertFrames(PostingIndexUtils.ENCODING_EF, true, LAYOUT_SPARSE, 50_000);
    }

    private static void addBoundariesAround(LongList boundaries, LongList values, int step, int offsetInStep) {
        for (int i = offsetInStep; i < values.size(); i += step) {
            final long v = values.getQuick(i);
            boundaries.add(v - 1);
            boundaries.add(v);
            boundaries.add(v + 1);
        }
    }

    private static void assertFrame(
            String label,
            RowCursor cursor,
            LongList expected,
            long lo,
            long hi,
            boolean forward
    ) {
        try {
            // expected values in [lo, hi]: binary search for the range
            int from = lowerBound(expected, lo);
            int to = lowerBound(expected, hi + 1);
            if (forward) {
                for (int i = from; i < to; i++) {
                    Assert.assertTrue(label + " missing row " + expected.getQuick(i), cursor.hasNext());
                    Assert.assertEquals(label + " ordinal " + i, expected.getQuick(i), cursor.next() + lo);
                }
            } else {
                for (int i = to - 1; i >= from; i--) {
                    Assert.assertTrue(label + " missing row " + expected.getQuick(i), cursor.hasNext());
                    Assert.assertEquals(label + " ordinal " + i, expected.getQuick(i), cursor.next() + lo);
                }
            }
            if (cursor.hasNext()) {
                Assert.fail(label + " extra row " + (cursor.next() + lo));
            }
        } finally {
            Misc.free(cursor);
        }
    }

    private static LongList frameBoundaries(Rnd rnd, ObjList<LongList> oracle, int frameSize) {
        final LongList boundaries = new LongList();
        if (frameSize > 0) {
            for (long b = 0; b < ROW_COUNT; b += frameSize) {
                boundaries.add(b);
            }
        } else {
            // boundaries at, around and between values of the checked keys, on delta-block
            // edges (every 64th value, and the value before it), plus random cut points
            for (int k : CHECKED_KEYS) {
                final LongList values = oracle.getQuick(k);
                addBoundariesAround(boundaries, values, 64, 0);
                addBoundariesAround(boundaries, values, 64, 63);
                addBoundariesAround(boundaries, values, Math.max(1, values.size() / 40), rnd.nextInt(Math.max(1, values.size() / 40)));
                for (int i = 0; i + 1 < values.size(); i += 1 + rnd.nextInt(97)) {
                    final long a = values.getQuick(i);
                    final long b = values.getQuick(i + 1);
                    if (b - a > 2) {
                        boundaries.add(a + 1 + rnd.nextLong((b - a) - 1));
                    }
                }
            }
            for (int i = 0; i < 300; i++) {
                boundaries.add(rnd.nextLong(ROW_COUNT));
            }
            boundaries.add(0);
        }
        boundaries.add(ROW_COUNT);
        boundaries.sort();
        // dedupe and clamp to [0, ROW_COUNT]; drop sub-sampling so the frame count stays bounded
        final LongList out = new LongList();
        long prev = -1;
        final int keepOneIn = frameSize > 0 ? 1 : Math.max(1, boundaries.size() / 1_500);
        for (int i = 0, n = boundaries.size(); i < n; i++) {
            final long b = Math.max(0, Math.min(ROW_COUNT, boundaries.getQuick(i)));
            if (b != prev && (b == 0 || b == ROW_COUNT || i % keepOneIn == 0)) {
                out.add(b);
                prev = b;
            }
        }
        return out;
    }

    private static int keyOf(Rnd rnd, long row) {
        if ((row / 20_000) % 2 == 1 && row % 20_000 < 3_000) {
            return KEY_BURSTY;
        }
        if (row % 64 == 7) {
            return KEY_PERIODIC;
        }
        if (row == ROW_COUNT - 1) {
            return KEY_LAST;
        }
        final double r = rnd.nextDouble();
        if (r < 0.25) {
            return KEY_LARGE;
        }
        if (r < 0.2504) {
            return KEY_SMALL;
        }
        if (r < 0.30 && row < ROW_COUNT / 2) {
            return KEY_HALF;
        }
        if (r < 0.31) {
            return 0;
        }
        return 1 + rnd.nextInt(FILLER_KEY_COUNT);
    }

    private static int lowerBound(LongList values, long target) {
        int lo = 0;
        int hi = values.size();
        while (lo < hi) {
            final int mid = (lo + hi) >>> 1;
            if (values.getQuick(mid) < target) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo;
    }

    private void assertFrames(byte encoding, boolean ranked, int layout, long columnTop) throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);
            final ObjList<LongList> oracle = new ObjList<>();
            for (int k = 0; k <= KEY_LAST; k++) {
                oracle.add(new LongList());
            }
            for (long r = 0; r < columnTop; r++) {
                // rows before the column top read as NULL, i.e. key 0, without being indexed
                oracle.getQuick(0).add(r);
            }

            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                final String name = "seek_" + encoding + "_" + ranked + "_" + layout + "_" + columnTop;
                final boolean wasRanked = PostingIndexUtils.isEfRankTrailerEnabled;
                PostingIndexUtils.isEfRankTrailerEnabled = ranked;
                try (PostingIndexWriter writer = new PostingIndexWriter(configuration, encoding)) {
                    writer.of(path, name, COLUMN_NAME_TXN_NONE, true);
                    writer.setNextTxnAtSeal(0L);
                    final int commitEvery = 9_000 + rnd.nextInt(4_000);
                    final long sealAt = layout == LAYOUT_MIXED ? ROW_COUNT * 3L / 5 : -1;
                    for (long r = columnTop; r < ROW_COUNT; r++) {
                        final int key = keyOf(rnd, r);
                        writer.add(key, r);
                        oracle.getQuick(key).add(r);
                        if ((r + 1) % commitEvery == 0 || r == ROW_COUNT - 1) {
                            writer.setMaxValue(r);
                            writer.commit();
                        }
                        if (r == sealAt) {
                            writer.setMaxValue(r);
                            writer.commit();
                            writer.seal();
                        }
                    }
                    if (layout == LAYOUT_SEALED) {
                        writer.seal();
                    }
                } finally {
                    PostingIndexUtils.isEfRankTrailerEnabled = wasRanked;
                }

                path.trimTo(plen);
                try (
                        PostingIndexFwdReader fwd = new PostingIndexFwdReader(configuration, path, name, COLUMN_NAME_TXN_NONE, -1, columnTop);
                        PostingIndexBwdReader bwd = new PostingIndexBwdReader(configuration, path, name, COLUMN_NAME_TXN_NONE, -1, columnTop, null, null, 0)
                ) {
                    for (int frameSize : new int[]{0, 1_000, 4_093, 65_536 + 3, ROW_COUNT}) {
                        final LongList boundaries = frameBoundaries(rnd, oracle, frameSize);
                        final int frameCount = boundaries.size() - 1;
                        for (int key : CHECKED_KEYS) {
                            final LongList expected = oracle.getQuick(key);
                            final String label = "key=" + key + ", frameSize=" + frameSize + ", count=" + expected.size();
                            // forward, frames in scan order
                            for (int f = 0; f < frameCount; f++) {
                                final long lo = boundaries.getQuick(f);
                                final long hi = boundaries.getQuick(f + 1) - 1;
                                assertFrame(label + " fwd [" + lo + "," + hi + "]", fwd.getCursor(key, lo, hi), expected, lo, hi, true);
                            }
                            // backward, frames in reverse scan order
                            for (int f = frameCount - 1; f >= 0; f--) {
                                final long lo = boundaries.getQuick(f);
                                final long hi = boundaries.getQuick(f + 1) - 1;
                                assertFrame(label + " bwd [" + lo + "," + hi + "]", bwd.getCursor(key, lo, hi), expected, lo, hi, false);
                            }
                            // random frame order, plus single-row and inverted (empty) frames
                            for (int i = 0; i < 64; i++) {
                                final int f = rnd.nextInt(frameCount);
                                final long lo = boundaries.getQuick(f);
                                final long hi = boundaries.getQuick(f + 1) - 1;
                                assertFrame(label + " fwd-rnd [" + lo + "," + hi + "]", fwd.getCursor(key, lo, hi), expected, lo, hi, true);
                                assertFrame(label + " bwd-rnd [" + lo + "," + hi + "]", bwd.getCursor(key, lo, hi), expected, lo, hi, false);
                                if (expected.size() > 0) {
                                    final long v = expected.getQuick(rnd.nextInt(expected.size()));
                                    assertFrame(label + " fwd-one [" + v + "]", fwd.getCursor(key, v, v), expected, v, v, true);
                                    assertFrame(label + " bwd-one [" + v + "]", bwd.getCursor(key, v, v), expected, v, v, false);
                                    assertFrame(label + " fwd-tail [" + v + ",max]", fwd.getCursor(key, v, ROW_COUNT - 1), expected, v, ROW_COUNT - 1, true);
                                    assertFrame(label + " bwd-head [0," + v + "]", bwd.getCursor(key, 0, v), expected, 0, v, false);
                                }
                            }
                            // frames past the last row
                            assertFrame(label + " fwd-past", fwd.getCursor(key, ROW_COUNT, ROW_COUNT + 10), expected, ROW_COUNT, ROW_COUNT + 10, true);
                            assertFrame(label + " bwd-past", bwd.getCursor(key, ROW_COUNT, ROW_COUNT + 10), expected, ROW_COUNT, ROW_COUNT + 10, false);
                        }
                    }
                }
            }
        });
    }
}
