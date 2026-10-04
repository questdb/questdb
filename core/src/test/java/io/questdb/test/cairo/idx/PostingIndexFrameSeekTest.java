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
import io.questdb.cairo.vm.MemoryCMARWImpl;
import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.std.IntList;
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

import java.lang.reflect.Field;

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
 * cursor as the page-frame cursor does) and in random order. Single-row frames, inverted
 * (empty) frames and frames open to {@code Long.MAX_VALUE} are checked as well.
 * <p>
 * The class also pins the two primitives behind the seek against brute force, the forward
 * cursor's first-value guard (a blob already at or above the frame's lower bound skips the
 * seek), and the fallback when a high word contradicts the validated ordinal.
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
    public void testEfFirstValueMatchesBruteForce() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);
            final int maxCount = 5_000;
            final long src = Unsafe.malloc((long) maxCount * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            final long dstSize = PostingIndexUtils.computeMaxEncodedSize(maxCount);
            final long dst = Unsafe.malloc(dstSize, MemoryTag.NATIVE_DEFAULT);
            final boolean wasRanked = PostingIndexUtils.isEfRankTrailerEnabled;
            int checkpointPath = 0;
            int legacyUnknown = 0;
            try (PostingIndexUtils.EncodeContext ctx = new PostingIndexUtils.EncodeContext()) {
                for (int iter = 0; iter < 400; iter++) {
                    final boolean ranked = iter % 2 == 0;
                    PostingIndexUtils.isEfRankTrailerEnabled = ranked;
                    final int count = 1 + rnd.nextInt(iter < 40 ? 3 : maxCount);
                    // The first value ranges from 0 to far past the list's span, so the first one
                    // bit of the high vector falls in word 0, in the first checkpoint block, or
                    // many blocks later, as in every generation after the first of a live index.
                    long v = switch (iter % 4) {
                        case 0 -> rnd.nextInt(64);
                        case 1 -> rnd.nextInt(10_000);
                        default -> rnd.nextLong(1L << (10 + rnd.nextInt(30)));
                    };
                    final long first = v;
                    for (int i = 0; i < count; i++) {
                        Unsafe.putLong(src + (long) i * Long.BYTES, v);
                        v += rnd.nextInt(10) < 3 ? 1 : 1 + rnd.nextInt(1 + rnd.nextInt(300));
                    }
                    ctx.ensureCapacity(count);
                    final int size = PostingIndexUtils.encodeKeyEF(src, count, dst, ctx);
                    final int bitsL = Unsafe.getByte(dst + 8) & 0xFF;
                    final long firstHighWord = (first >>> bitsL) >>> 6;
                    final long actual = PostingIndexUtils.efFirstValue(dst, size);
                    final String label = "ranked=" + ranked + ", count=" + count + ", first=" + first + ", L=" + bitsL;
                    if (ranked || firstHighWord < 8) {
                        Assert.assertEquals(label, first, actual);
                        if (firstHighWord >= 8) {
                            checkpointPath++;
                        }
                    } else {
                        // a legacy blob has no checkpoints to narrow the scan past the first block
                        Assert.assertEquals(label, -1, actual);
                        legacyUnknown++;
                    }
                    Assert.assertEquals(label, javaFirstValue(dst), first);
                }
            } finally {
                PostingIndexUtils.isEfRankTrailerEnabled = wasRanked;
                Unsafe.free(src, (long) maxCount * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(dst, dstSize, MemoryTag.NATIVE_DEFAULT);
            }
            Assert.assertTrue("fixture must reach the checkpoint-narrowed scan", checkpointPath > 0);
            Assert.assertTrue("fixture must reach a legacy blob past the first block", legacyUnknown > 0);
        });
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
    public void testEfLowerBoundWordRejectsInconsistentHighWord() throws Exception {
        // The O(1) step reads the high word holding the target's bit position without validating
        // it. A word with more one bits below that position than the (validated) ordinal would
        // give a negative rank, and a decode from a negative ordinal addresses the low bits far
        // outside the blob. The step must reject such a word instead of returning that rank.
        assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);
            final int maxCount = 2_000;
            final long src = Unsafe.malloc((long) maxCount * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            final long dstSize = PostingIndexUtils.computeMaxEncodedSize(maxCount);
            final long dst = Unsafe.malloc(dstSize, MemoryTag.NATIVE_DEFAULT);
            final boolean wasRanked = PostingIndexUtils.isEfRankTrailerEnabled;
            PostingIndexUtils.isEfRankTrailerEnabled = true;
            int rejected = 0;
            try (PostingIndexUtils.EncodeContext ctx = new PostingIndexUtils.EncodeContext()) {
                for (int iter = 0; iter < 200; iter++) {
                    final int count = 64 + rnd.nextInt(maxCount - 64);
                    final long[] values = new long[count];
                    long v = rnd.nextInt(100);
                    final int gap = 2 + rnd.nextInt(200);
                    for (int i = 0; i < count; i++) {
                        values[i] = v;
                        Unsafe.putLong(src + (long) i * Long.BYTES, v);
                        v += 1 + rnd.nextInt(gap);
                    }
                    ctx.ensureCapacity(count);
                    final int size = PostingIndexUtils.encodeKeyEF(src, count, dst, ctx);
                    final int bitsL = Unsafe.getByte(dst + 8) & 0xFF;
                    final long highStart = dst + 17 + ((((long) count * bitsL + 63) >>> 6) << 3);
                    // a target among the first values, so the ordinal is small
                    final long target = values[rnd.nextInt(40)] + rnd.nextInt(2);
                    final int ordinal = PostingIndexUtils.efLowerBound(dst, size, target);
                    Assert.assertTrue(ordinal >= 0 && ordinal < count);
                    final long position = (target >>> bitsL) + ordinal;
                    final int bit = (int) (position & 63);
                    if (bit == 0) {
                        continue;
                    }
                    final long wordAddr = highStart + (position >>> 6) * Long.BYTES;
                    final long word = Unsafe.getLong(wordAddr);
                    final long below = -1L >>> (64 - bit);
                    Assert.assertTrue(PostingIndexUtils.efLowerBoundWord(dst, target, ordinal) >= 0);
                    // corrupt: set every bit below the position, only when that yields more one
                    // bits there than the ordinal, i.e. a negative rank
                    if (Long.bitCount(below) <= ordinal) {
                        continue;
                    }
                    Unsafe.putLong(wordAddr, word | below);
                    try {
                        Assert.assertEquals("count=" + count + ", target=" + target + ", ordinal=" + ordinal,
                                -1, PostingIndexUtils.efLowerBoundWord(dst, target, ordinal));
                        // the validated search itself catches this corruption, which is why a
                        // reader cannot reach the step with it (see the reader-level test)
                        Assert.assertEquals(-1, PostingIndexUtils.efLowerBound(dst, size, target));
                        rejected++;
                    } finally {
                        Unsafe.putLong(wordAddr, word);
                    }
                }
            } finally {
                PostingIndexUtils.isEfRankTrailerEnabled = wasRanked;
                Unsafe.free(src, (long) maxCount * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(dst, dstSize, MemoryTag.NATIVE_DEFAULT);
            }
            Assert.assertTrue("fixture must produce inconsistent words", rejected > 20);
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

    @Test
    public void testForwardFirstValueGuard() throws Exception {
        // A forward cursor skips the seek when the blob's first value is already >= minValue
        // (every generation after the one holding minValue), mirroring the backward cursor's
        // maxValue < u - 1 guard. The guard is observable when minValue's high part lies past
        // high word 0: the seek would then start the decode at that word, the guard at word 0.
        assertMemoryLeak(() -> {
            final ObjList<LongList> oracle = new ObjList<>();
            for (int k = 0; k < 5; k++) {
                oracle.add(new LongList());
            }
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                final String name = "seek_first_value_guard";
                final boolean wasRanked = PostingIndexUtils.isEfRankTrailerEnabled;
                PostingIndexUtils.isEfRankTrailerEnabled = true;
                try (PostingIndexWriter writer = new PostingIndexWriter(configuration, PostingIndexUtils.ENCODING_EF)) {
                    writer.of(path, name, COLUMN_NAME_TXN_NONE, true);
                    writer.setNextTxnAtSeal(0L);
                    // two sparse generations: key 2's first one bit is in the first checkpoint
                    // block (word 3), key 1 exists only in the second generation and key 4 in
                    // both, so their second-generation blobs start many blocks in
                    for (long r = 0; r < 60_000; r++) {
                        final int key;
                        if (r < 50_000) {
                            key = r >= 1_000 && r < 4_000 && r % 3 == 0 ? 2 : (r % 5 == 0 ? 4 : 3);
                        } else {
                            key = r % 5 == 0 ? 4 : 1;
                        }
                        writer.add(key, r);
                        oracle.getQuick(key).add(r);
                        if (r == 49_999 || r == 59_999) {
                            writer.setMaxValue(r);
                            writer.commit();
                        }
                    }
                } finally {
                    PostingIndexUtils.isEfRankTrailerEnabled = wasRanked;
                }

                path.trimTo(plen);
                try (PostingIndexFwdReader fwd = new PostingIndexFwdReader(configuration, path, name, COLUMN_NAME_TXN_NONE, -1, 0)) {
                    final MemoryMR valueMem = (MemoryMR) fieldObject(fwd, "valueMem");
                    int guardedObservable = 0;
                    int seekedObservable = 0;
                    for (int key = 1; key < 5; key++) {
                        final LongList values = oracle.getQuick(key);
                        final long first = values.getQuick(0);
                        final long mid = values.getQuick(values.size() / 2);
                        final long[] bounds = {1, first - 1, first, first + 1, first + 64, mid, mid + 1, values.getLast(), 49_999, 50_000, 50_001, 59_999};
                        for (long minValue : bounds) {
                            final String label = "key=" + key + ", minValue=" + minValue;
                            final RowCursor cursor = fwd.getCursor(key, minValue, Long.MAX_VALUE);
                            if (!(Boolean) fieldObject(cursor, "isEFMode")) {
                                // no generation of this key reaches minValue
                                assertFrame(label, cursor, values, minValue, Long.MAX_VALUE, true);
                                continue;
                            }
                            final long blobAddr = valueMem.addressOf(0) + (Long) fieldObject(cursor, "encodedOffset");
                            final long blobFirst = javaFirstValue(blobAddr);
                            final int bitsL = (Integer) fieldObject(cursor, "efL");
                            final int word = (Integer) fieldObject(cursor, "efHighWordIdx");
                            final int outputCount = (Integer) fieldObject(cursor, "efOutputCount");
                            final boolean observable = ((minValue >>> bitsL) >>> 6) > 0;
                            if (minValue <= blobFirst) {
                                Assert.assertEquals(label + " guarded blob must start at word 0", 0, word);
                                Assert.assertEquals(label + " guarded blob must start at ordinal 0", 0, outputCount);
                                if (observable) {
                                    guardedObservable++;
                                }
                            } else if (observable) {
                                Assert.assertTrue(label + " blob below minValue must seek", word > 0);
                                seekedObservable++;
                            }
                            assertFrame(label, cursor, values, minValue, Long.MAX_VALUE, true);
                        }
                    }
                    Assert.assertTrue("fixture must reach the guard where the seek would move", guardedObservable > 0);
                    Assert.assertTrue("fixture must reach the seek past word 0", seekedObservable > 0);
                }
            }
        });
    }

    @Test
    public void testForwardSeekCorruptHighWordMatchesUnrankedWalk() throws Exception {
        // A corrupt high word under a small ordinal, set to more one bits below the target's
        // position than the ordinal (the range that would give the O(1) step a negative rank).
        // The forward cursor must neither crash nor trust the word: for every bound in that range
        // it returns exactly what a from-zero decode of the corrupt blob (the unranked walk) gives.
        assertMemoryLeak(() -> {
            final int count = 400;
            final long[] values = new long[count];
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                final String name = "seek_corrupt_high_word";
                final boolean wasRanked = PostingIndexUtils.isEfRankTrailerEnabled;
                PostingIndexUtils.isEfRankTrailerEnabled = true;
                try (PostingIndexWriter writer = new PostingIndexWriter(configuration, PostingIndexUtils.ENCODING_EF)) {
                    writer.of(path, name, COLUMN_NAME_TXN_NONE, true);
                    writer.setNextTxnAtSeal(0L);
                    for (int i = 0; i < count; i++) {
                        values[i] = 7 + i * 50L;
                        writer.add(1, values[i]);
                    }
                    writer.setMaxValue(values[count - 1]);
                    writer.commit();
                } finally {
                    PostingIndexUtils.isEfRankTrailerEnabled = wasRanked;
                }

                final long encodedOffset;
                final long highOffset;
                final int bitsL;
                path.trimTo(plen);
                try (
                        PostingIndexFwdReader fwd = new PostingIndexFwdReader(configuration, path, name, COLUMN_NAME_TXN_NONE, 0, 0);
                        RowCursor cursor = fwd.getCursor(1, 0, Long.MAX_VALUE)
                ) {
                    Assert.assertTrue((Boolean) fieldObject(cursor, "isEFMode"));
                    encodedOffset = (Long) fieldObject(cursor, "encodedOffset");
                    highOffset = (Long) fieldObject(cursor, "efHighOffset");
                    bitsL = (Integer) fieldObject(cursor, "efL");
                    final long blob = fwd.getValueBaseAddress() + encodedOffset;
                    Assert.assertTrue(PostingIndexUtils.hasEfRankTrailer(blob, (Integer) fieldObject(cursor, "encodedSize")));
                }

                // bounds whose position (high(minValue) + ordinal) lies in high word 0, past the
                // zero bits of the earlier high buckets
                final LongList bounds = new LongList();
                final IntList ordinals = new IntList();
                for (long minValue = 1; minValue < values[62]; minValue++) {
                    int ordinal = 0;
                    while (values[ordinal] < minValue) {
                        ordinal++;
                    }
                    final long position = (minValue >>> bitsL) + ordinal;
                    if (position < 64 && (position & 63) > ordinal) {
                        bounds.add(minValue);
                        ordinals.add(ordinal);
                    }
                }
                Assert.assertTrue("fixture must have bounds past zero bits in word 0", bounds.size() > 10);

                // corrupt word 0: every bit below the largest such position becomes a one
                path.trimTo(plen);
                final long fileSize = configuration.getFilesFacade().length(
                        PostingIndexUtils.valueFileName(path, name, COLUMN_NAME_TXN_NONE, 0));
                path.trimTo(plen);
                long maxPosition = 0;
                for (int i = 0, n = bounds.size(); i < n; i++) {
                    maxPosition = Math.max(maxPosition, (bounds.getQuick(i) >>> bitsL) + ordinals.getQuick(i));
                }
                try (MemoryCMARWImpl mem = new MemoryCMARWImpl(
                        configuration.getFilesFacade(),
                        PostingIndexUtils.valueFileName(path, name, COLUMN_NAME_TXN_NONE, 0),
                        configuration.getFilesFacade().getPageSize(),
                        fileSize,
                        MemoryTag.MMAP_DEFAULT,
                        0
                )) {
                    // efHighOffset is the file offset of high word 0
                    final long word = mem.getLong(highOffset);
                    mem.putLong(highOffset, word | (-1L >>> (64 - maxPosition)));
                    Assert.assertNotEquals(word, mem.getLong(highOffset));
                }

                path.trimTo(plen);
                try (PostingIndexFwdReader fwd = new PostingIndexFwdReader(configuration, path, name, COLUMN_NAME_TXN_NONE, 0, 0)) {
                    final long blob = fwd.getValueBaseAddress() + encodedOffset;
                    final LongList walk = javaDecodeEf(blob);
                    final long corruptWord = Unsafe.getLong(blob + 17 + ((((long) count * bitsL + 63) >>> 6) << 3));
                    int negativeRank = 0;
                    for (int i = 0, n = bounds.size(); i < n; i++) {
                        final long minValue = bounds.getQuick(i);
                        final int ordinal = ordinals.getQuick(i);
                        final long position = (minValue >>> bitsL) + ordinal;
                        if (Long.bitCount(corruptWord & (-1L >>> (64 - position))) > ordinal) {
                            negativeRank++;
                        }
                        final String label = "minValue=" + minValue;
                        try (RowCursor cursor = fwd.getCursor(1, minValue, Long.MAX_VALUE)) {
                            for (int j = 0, m = walk.size(); j < m; j++) {
                                final long v = walk.getQuick(j);
                                if (v >= minValue) {
                                    Assert.assertTrue(label + " missing " + v, cursor.hasNext());
                                    Assert.assertEquals(label, v, cursor.next() + minValue);
                                }
                            }
                            Assert.assertFalse(label + " extra row", cursor.hasNext());
                        }
                    }
                    Assert.assertTrue("corruption must put bounds in the negative-rank range", negativeRank > 10);
                }
            }
        });
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
            // expected values in [lo, hi]: binary search for the range; an inverted frame is empty
            final int from = lowerBound(expected, lo);
            final int to = lo > hi ? from : (hi == Long.MAX_VALUE ? expected.size() : lowerBound(expected, hi + 1));
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

    private static Object fieldObject(Object instance, String fieldName) throws Exception {
        Class<?> clazz = instance.getClass();
        while (clazz != null) {
            try {
                final Field field = clazz.getDeclaredField(fieldName);
                field.setAccessible(true);
                return field.get(instance);
            } catch (NoSuchFieldException e) {
                clazz = clazz.getSuperclass();
            }
        }
        throw new NoSuchFieldException(fieldName);
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

    /**
     * Decodes an EF blob from ordinal 0 exactly as the forward cursor's decode does, trusting
     * every high word: the from-zero (unranked) walk.
     */
    private static LongList javaDecodeEf(long blob) {
        final int count = Unsafe.getInt(blob + 4);
        final int bitsL = Unsafe.getByte(blob + 8) & 0xFF;
        final long universe = Unsafe.getLong(blob + 9);
        final long lowStart = blob + 17;
        final long highStart = lowStart + ((((long) count * bitsL + 63) >>> 6) << 3);
        final long highWordCount = (count + (universe >>> bitsL) + 63) / 64;
        final LongList out = new LongList();
        int ordinal = 0;
        for (int w = 0; w < highWordCount && ordinal < count; w++) {
            long word = Unsafe.getLong(highStart + (long) w * Long.BYTES);
            while (word != 0 && ordinal < count) {
                final long high = (long) w * 64 + Long.numberOfTrailingZeros(word) - ordinal;
                out.add((high << bitsL) | readLow(lowStart, ordinal, bitsL));
                ordinal++;
                word &= word - 1;
            }
        }
        return out;
    }

    private static long javaFirstValue(long blob) {
        final int count = Unsafe.getInt(blob + 4);
        final int bitsL = Unsafe.getByte(blob + 8) & 0xFF;
        final long highStart = blob + 17 + ((((long) count * bitsL + 63) >>> 6) << 3);
        long position = 0;
        for (int w = 0; ; w++) {
            final long word = Unsafe.getLong(highStart + (long) w * Long.BYTES);
            if (word != 0) {
                position = (long) w * 64 + Long.numberOfTrailingZeros(word);
                break;
            }
        }
        return (position << bitsL) | readLow(blob + 17, 0, bitsL);
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

    private static long readLow(long lowStart, int ordinal, int bitsL) {
        if (bitsL == 0) {
            return 0;
        }
        final long bitPos = (long) ordinal * bitsL;
        final long addr = lowStart + ((bitPos >>> 6) << 3);
        final int offset = (int) (bitPos & 63);
        long low = Unsafe.getLong(addr) >>> offset;
        if (offset + bitsL > 64) {
            low |= Unsafe.getLong(addr + 8) << (64 - offset);
        }
        return low & ((1L << bitsL) - 1);
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
                            // random frame order, plus single-row, inverted (empty) and open-ended frames
                            for (int i = 0; i < 64; i++) {
                                final int f = rnd.nextInt(frameCount);
                                final long lo = boundaries.getQuick(f);
                                final long hi = boundaries.getQuick(f + 1) - 1;
                                assertFrame(label + " fwd-rnd [" + lo + "," + hi + "]", fwd.getCursor(key, lo, hi), expected, lo, hi, true);
                                assertFrame(label + " bwd-rnd [" + lo + "," + hi + "]", bwd.getCursor(key, lo, hi), expected, lo, hi, false);
                                if (lo < hi) {
                                    assertFrame(label + " fwd-inverted [" + hi + "," + lo + "]", fwd.getCursor(key, hi, lo), expected, hi, lo, true);
                                    assertFrame(label + " bwd-inverted [" + hi + "," + lo + "]", bwd.getCursor(key, hi, lo), expected, hi, lo, false);
                                }
                                assertFrame(label + " fwd-inverted [" + (lo + 1) + "," + lo + "]", fwd.getCursor(key, lo + 1, lo), expected, lo + 1, lo, true);
                                assertFrame(label + " bwd-inverted [" + (lo + 1) + "," + lo + "]", bwd.getCursor(key, lo + 1, lo), expected, lo + 1, lo, false);
                                assertFrame(label + " fwd-open [" + lo + ",MAX]", fwd.getCursor(key, lo, Long.MAX_VALUE), expected, lo, Long.MAX_VALUE, true);
                                assertFrame(label + " bwd-open [" + lo + ",MAX]", bwd.getCursor(key, lo, Long.MAX_VALUE), expected, lo, Long.MAX_VALUE, false);
                                if (expected.size() > 0) {
                                    final long v = expected.getQuick(rnd.nextInt(expected.size()));
                                    assertFrame(label + " fwd-one [" + v + "]", fwd.getCursor(key, v, v), expected, v, v, true);
                                    assertFrame(label + " bwd-one [" + v + "]", bwd.getCursor(key, v, v), expected, v, v, false);
                                    assertFrame(label + " fwd-tail [" + v + ",max]", fwd.getCursor(key, v, ROW_COUNT - 1), expected, v, ROW_COUNT - 1, true);
                                    assertFrame(label + " bwd-head [0," + v + "]", bwd.getCursor(key, 0, v), expected, 0, v, false);
                                }
                            }
                            // frames past the last row, and the whole list open-ended
                            assertFrame(label + " fwd-past", fwd.getCursor(key, ROW_COUNT, ROW_COUNT + 10), expected, ROW_COUNT, ROW_COUNT + 10, true);
                            assertFrame(label + " bwd-past", bwd.getCursor(key, ROW_COUNT, ROW_COUNT + 10), expected, ROW_COUNT, ROW_COUNT + 10, false);
                            assertFrame(label + " fwd-past-open", fwd.getCursor(key, ROW_COUNT, Long.MAX_VALUE), expected, ROW_COUNT, Long.MAX_VALUE, true);
                            assertFrame(label + " bwd-past-open", bwd.getCursor(key, ROW_COUNT, Long.MAX_VALUE), expected, ROW_COUNT, Long.MAX_VALUE, false);
                            assertFrame(label + " fwd-all-open", fwd.getCursor(key, 0, Long.MAX_VALUE), expected, 0, Long.MAX_VALUE, true);
                            assertFrame(label + " bwd-all-open", bwd.getCursor(key, 0, Long.MAX_VALUE), expected, 0, Long.MAX_VALUE, false);
                        }
                    }
                }
            }
        });
    }
}
