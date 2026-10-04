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
import io.questdb.cairo.vm.api.MemoryMR;
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
 * Edge cases of the posting-index frame seek, beyond the frame sweeps of
 * {@link PostingIndexFrameSeekTest}:
 * <ul>
 *     <li>the {@code efLowerBoundWord} contract checked exhaustively on small lists (one and two
 *     values, consecutive runs with L = 0, values on 64-bit boundaries, large buckets with huge
 *     gaps), for every target near the universe;</li>
 *     <li>unusual bounds: inverted, negative, {@code Long.MAX_VALUE}, past the end, at and around
 *     the first and last values, with row-id bases near 2^31, 3e9 and 2^40;</li>
 *     <li>a reader that stays open while the writer appends sparse generations (and seals), with
 *     fresh per-frame cursors and a cursor opened before the append;</li>
 *     <li>the exact blob extent ({@code encodedSize}): every EF blob the cursors visit has a
 *     reachable rank trailer, so no seek silently degrades to the linear one.</li>
 * </ul>
 */
public class PostingIndexFrameSeekEdgeCaseTest extends AbstractCairoTest {
    // keys 1..5, plus the null key 0
    private static final int KEYS = 6;
    private static final int ROWS = 40_000;

    @Test
    public void testEfLowerBoundWordExhaustiveSmallLists() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);
            final int maxCount = 600;
            final long src = Unsafe.malloc((long) maxCount * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            final long dstSize = PostingIndexUtils.computeMaxEncodedSize(maxCount);
            final long dst = Unsafe.malloc(dstSize, MemoryTag.NATIVE_DEFAULT);
            final boolean wasRanked = PostingIndexUtils.isEfRankTrailerEnabled;
            PostingIndexUtils.isEfRankTrailerEnabled = true;
            try (PostingIndexUtils.EncodeContext ctx = new PostingIndexUtils.EncodeContext()) {
                for (int iter = 0; iter < 3_000; iter++) {
                    final int shape = iter % 8;
                    final int count = switch (shape) {
                        case 0 -> 1;
                        case 1 -> 2;
                        default -> 1 + rnd.nextInt(maxCount);
                    };
                    final long[] values = new long[count];
                    long v = switch (rnd.nextInt(4)) {
                        case 0 -> 0;
                        case 1 -> rnd.nextInt(64);
                        case 2 -> 63 + rnd.nextInt(3);
                        default -> rnd.nextInt(5_000);
                    };
                    for (int i = 0; i < count; i++) {
                        values[i] = v;
                        Unsafe.putLong(src + (long) i * Long.BYTES, v);
                        v += switch (shape) {
                            case 2 -> 1; // consecutive -> L = 0
                            case 3 -> 1 + rnd.nextInt(2);
                            case 4 -> 64; // positions land on word boundaries
                            case 5 -> rnd.nextInt(10) < 8 ? 1 : 1 + rnd.nextInt(100_000); // big buckets + gaps
                            default -> 1 + rnd.nextInt(1 + rnd.nextInt(300));
                        };
                    }
                    ctx.ensureCapacity(count);
                    final int size = PostingIndexUtils.encodeKeyEF(src, count, dst, ctx);
                    final int bitsL = Unsafe.getByte(dst + 8) & 0xFF;
                    final long highStart = dst + 17 + ((((long) count * bitsL + 63) >>> 6) << 3);
                    final long universe = values[count - 1] + 1;
                    final long step = Math.max(1, universe / 4_000);
                    for (long target = 0; target <= universe + 1; target += (target < universe - 3_000 ? step : 1)) {
                        int expected = 0;
                        while (expected < count && values[expected] < target) {
                            expected++;
                        }
                        final int ordinal = PostingIndexUtils.efLowerBound(dst, size, target);
                        final String label = "iter=" + iter + " shape=" + shape + " count=" + count + " L=" + bitsL + " target=" + target;
                        Assert.assertEquals(label, expected, ordinal);
                        if (ordinal >= count) {
                            continue;
                        }
                        final long packed = PostingIndexUtils.efLowerBoundWord(dst, target, ordinal);
                        final int word = (int) (packed >>> 32);
                        final int rank = (int) packed;
                        int bruteRank = 0;
                        for (int w = 0; w < word; w++) {
                            bruteRank += Long.bitCount(Unsafe.getLong(highStart + (long) w * Long.BYTES));
                        }
                        Assert.assertEquals(label, bruteRank, rank);
                        Assert.assertTrue(label, rank >= 0 && rank <= ordinal);
                        // contract: ordinals rank..ordinal start in this word; rank-1 is in an earlier word
                        final long posRank = (values[rank] >>> bitsL) + rank;
                        // the start word may be an all-zero word before the ordinal (empty high buckets)
                        Assert.assertTrue(label, posRank >= (long) word * 64);
                        if (rank > 0) {
                            final long posPrev = (values[rank - 1] >>> bitsL) + rank - 1;
                            Assert.assertTrue(label, posPrev < (long) word * 64);
                        }
                        final long posOrd = (values[ordinal] >>> bitsL) + ordinal;
                        Assert.assertTrue(label, posOrd >= (long) word * 64);
                    }
                }
            } finally {
                PostingIndexUtils.isEfRankTrailerEnabled = wasRanked;
                Unsafe.free(src, (long) maxCount * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(dst, dstSize, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testInterleavedAppendAdaptive() throws Exception {
        assertInterleavedAppend(PostingIndexUtils.ENCODING_ADAPTIVE, true);
    }

    @Test
    public void testInterleavedAppendEfLegacy() throws Exception {
        assertInterleavedAppend(PostingIndexUtils.ENCODING_EF, false);
    }

    @Test
    public void testInterleavedAppendEfRanked() throws Exception {
        assertInterleavedAppend(PostingIndexUtils.ENCODING_EF, true);
    }

    @Test
    public void testRankedTrailerReachableMixed() throws Exception {
        assertRankedReachable(2);
    }

    @Test
    public void testRankedTrailerReachableSealed() throws Exception {
        assertRankedReachable(0);
    }

    @Test
    public void testRankedTrailerReachableSparse() throws Exception {
        assertRankedReachable(1);
    }

    @Test
    public void testUnusualBoundsAdaptiveMixedLarge() throws Exception {
        assertUnusualBounds(PostingIndexUtils.ENCODING_ADAPTIVE, true, 2, 1L << 40, 0);
    }

    @Test
    public void testUnusualBoundsDeltaSparse() throws Exception {
        assertUnusualBounds(PostingIndexUtils.ENCODING_DELTA, true, 1, 0, 333);
    }

    @Test
    public void testUnusualBoundsEfLegacyMixedLarge() throws Exception {
        assertUnusualBounds(PostingIndexUtils.ENCODING_EF, false, 2, 3_000_000_000L, 0);
    }

    @Test
    public void testUnusualBoundsEfLegacySealed() throws Exception {
        assertUnusualBounds(PostingIndexUtils.ENCODING_EF, false, 0, 0, 1_000);
    }

    @Test
    public void testUnusualBoundsEfRankedMixedLarge() throws Exception {
        assertUnusualBounds(PostingIndexUtils.ENCODING_EF, true, 2, (1L << 40) + 17, 0);
    }

    @Test
    public void testUnusualBoundsEfRankedSealed() throws Exception {
        assertUnusualBounds(PostingIndexUtils.ENCODING_EF, true, 0, 0, 1_000);
    }

    @Test
    public void testUnusualBoundsEfRankedSealedLarge() throws Exception {
        assertUnusualBounds(PostingIndexUtils.ENCODING_EF, true, 0, Integer.MAX_VALUE - 5_000L, 0);
    }

    @Test
    public void testUnusualBoundsEfRankedSparse() throws Exception {
        assertUnusualBounds(PostingIndexUtils.ENCODING_EF, true, 1, 0, 0);
    }

    private static void assertFrame(String label, RowCursor cursor, LongList expected, long lo, long hi, boolean forward) {
        try {
            if (lo > hi) {
                // the old reader returns nothing for an inverted range; the oracle is empty
                if (cursor.hasNext()) {
                    Assert.fail(label + " inverted range returned row " + (cursor.next() + lo));
                }
                return;
            }
            int from = lowerBound(expected, lo);
            int to = hi == Long.MAX_VALUE ? expected.size() : lowerBound(expected, hi + 1);
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

    private static Object field(Object o, String name) throws Exception {
        Class<?> c = o.getClass();
        while (c != null) {
            try {
                final Field f = c.getDeclaredField(name);
                f.setAccessible(true);
                return f.get(o);
            } catch (NoSuchFieldException e) {
                c = c.getSuperclass();
            }
        }
        throw new NoSuchFieldException(name);
    }

    private static int keyOf(Rnd rnd, long i) {
        if (i % 50 == 3) {
            return 5;
        }
        final int r = rnd.nextInt(100);
        if (r < 40) {
            return 1;
        }
        if (r < 41) {
            return 2;
        }
        if (r < 70) {
            return 3;
        }
        if (r < 72) {
            return 0;
        }
        return 4;
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

    private void assertInterleavedAppend(byte encoding, boolean ranked) throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);
            final ObjList<LongList> oracle = new ObjList<>();
            for (int k = 0; k < KEYS; k++) {
                oracle.add(new LongList());
            }
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                final String name = "seek_edge_append_" + encoding + "_" + ranked;
                final boolean wasRanked = PostingIndexUtils.isEfRankTrailerEnabled;
                PostingIndexUtils.isEfRankTrailerEnabled = ranked;
                try (PostingIndexWriter writer = new PostingIndexWriter(configuration, encoding)) {
                    writer.of(path, name, COLUMN_NAME_TXN_NONE, true);
                    writer.setNextTxnAtSeal(0L);
                    long row = 0;
                    for (int i = 0; i < 2_000; i++) {
                        final int key = keyOf(rnd, row);
                        writer.add(key, row);
                        oracle.getQuick(key).add(row);
                        row++;
                    }
                    writer.setMaxValue(row - 1);
                    writer.commit();
                    path.trimTo(plen);
                    try (
                            PostingIndexFwdReader fwd = new PostingIndexFwdReader(configuration, path, name, COLUMN_NAME_TXN_NONE, -1, 0);
                            PostingIndexBwdReader bwd = new PostingIndexBwdReader(configuration, path, name, COLUMN_NAME_TXN_NONE, -1, 0, null, null, 0)
                    ) {
                        for (int round = 0; round < 25; round++) {
                            // snapshot oracle for "cursor opened before the append"
                            final long snapshotMax = row - 1;
                            final int key0 = 1 + rnd.nextInt(KEYS - 1);
                            final long preLo = rnd.nextLong(row);
                            final RowCursor preFwd = fwd.getCursor(key0, preLo, Long.MAX_VALUE);
                            final LongList preExp = new LongList();
                            preExp.add(oracle.getQuick(key0));

                            final int n = 500 + rnd.nextInt(4_000);
                            for (int i = 0; i < n; i++) {
                                final int key = keyOf(rnd, row);
                                writer.add(key, row);
                                oracle.getQuick(key).add(row);
                                row++;
                            }
                            writer.setMaxValue(row - 1);
                            writer.commit();
                            if (round % 9 == 8) {
                                writer.seal();
                            }

                            // pre-opened cursor: must see exactly what was published when it was opened
                            // (or, if the reader semantics extend, at least be a consistent prefix)
                            try {
                                final int from = lowerBound(preExp, preLo);
                                int i = from;
                                while (preFwd.hasNext()) {
                                    final long v = preFwd.next() + preLo;
                                    Assert.assertTrue("pre-open cursor key=" + key0 + " extra row " + v + " (snapshotMax=" + snapshotMax + ")", i < preExp.size() || v > snapshotMax);
                                    if (i < preExp.size()) {
                                        Assert.assertEquals("pre-open cursor key=" + key0 + " ordinal " + i, preExp.getQuick(i), v);
                                    }
                                    i++;
                                }
                                Assert.assertTrue("pre-open cursor key=" + key0 + " missed rows: got to " + i + " of " + preExp.size(), i >= preExp.size());
                            } finally {
                                Misc.free(preFwd);
                            }

                            // fresh cursors over page-frame-like windows
                            final long frame = 997;
                            for (int key = 0; key < KEYS; key++) {
                                final LongList exp = oracle.getQuick(key);
                                for (long lo = 0; lo < row; lo += frame) {
                                    final long hi = Math.min(row, lo + frame) - 1;
                                    assertFrame("round=" + round + " key=" + key + " fwd [" + lo + "," + hi + "]", fwd.getCursor(key, lo, hi), exp, lo, hi, true);
                                }
                                for (long lo = ((row - 1) / frame) * frame; lo >= 0; lo -= frame) {
                                    final long hi = Math.min(row, lo + frame) - 1;
                                    assertFrame("round=" + round + " key=" + key + " bwd [" + lo + "," + hi + "]", bwd.getCursor(key, lo, hi), exp, lo, hi, false);
                                }
                            }
                        }
                    }
                } finally {
                    PostingIndexUtils.isEfRankTrailerEnabled = wasRanked;
                }
            }
        });
    }

    private void assertRankedReachable(int layout) throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                final String name = "seek_edge_reach_" + layout;
                final boolean wasRanked = PostingIndexUtils.isEfRankTrailerEnabled;
                PostingIndexUtils.isEfRankTrailerEnabled = true;
                try (PostingIndexWriter writer = new PostingIndexWriter(configuration, PostingIndexUtils.ENCODING_EF)) {
                    writer.of(path, name, COLUMN_NAME_TXN_NONE, true);
                    writer.setNextTxnAtSeal(0L);
                    for (int i = 0; i < ROWS; i++) {
                        writer.add(keyOf(rnd, i), i);
                        if ((i + 1) % 3_001 == 0 || i == ROWS - 1) {
                            writer.setMaxValue(i);
                            writer.commit();
                        }
                        if (layout == 2 && i == ROWS / 2) {
                            writer.setMaxValue(i);
                            writer.commit();
                            writer.seal();
                        }
                    }
                    if (layout == 0) {
                        writer.seal();
                    }
                } finally {
                    PostingIndexUtils.isEfRankTrailerEnabled = wasRanked;
                }
                path.trimTo(plen);
                try (
                        PostingIndexFwdReader fwd = new PostingIndexFwdReader(configuration, path, name, COLUMN_NAME_TXN_NONE, -1, 0);
                        PostingIndexBwdReader bwd = new PostingIndexBwdReader(configuration, path, name, COLUMN_NAME_TXN_NONE, -1, 0, null, null, 0)
                ) {
                    final MemoryMR fwdMem = (MemoryMR) field(fwd, "valueMem");
                    int efBlobs = 0;
                    int unrankedBlobs = 0;
                    int bwdEf = 0;
                    int bwdUnranked = 0;
                    final StringBuilder bad = new StringBuilder();
                    for (int key = 0; key < KEYS; key++) {
                        RowCursor c = fwd.getCursor(key, 1, Long.MAX_VALUE);
                        try {
                            long lastOff = -1;
                            do {
                                final boolean ef = (Boolean) field(c, "isEFMode");
                                final long off = (Long) field(c, "encodedOffset");
                                if (ef && off != lastOff) {
                                    lastOff = off;
                                    final int size = (Integer) field(c, "encodedSize");
                                    efBlobs++;
                                    if (!PostingIndexUtils.hasEfRankTrailer(fwdMem.addressOf(0) + off, size)) {
                                        unrankedBlobs++;
                                        bad.append(" fwd key=").append(key).append(" gen=").append(field(c, "currentGen")).append(" size=").append(size)
                                                .append(" prefix=").append(PostingIndexUtils.efPrefixSize(fwdMem.addressOf(0) + off));
                                    }
                                }
                            } while (c.hasNext());
                        } finally {
                            Misc.free(c);
                        }
                        c = bwd.getCursor(key, 0, ROWS - 2);
                        try {
                            int lastGen = Integer.MIN_VALUE;
                            do {
                                final boolean ef = (Boolean) field(c, "isEFMode");
                                final int gen = (Integer) field(c, "currentGen");
                                if (ef && gen != lastGen) {
                                    lastGen = gen;
                                    bwdEf++;
                                    if (!(Boolean) field(c, "isEFRanked")) {
                                        bwdUnranked++;
                                        bad.append(" bwd key=").append(key).append(" gen=").append(gen);
                                    }
                                }
                            } while (c.hasNext());
                        } finally {
                            Misc.free(c);
                        }
                    }
                    LOG.info().$("ranked trailer reach [layout=").$(layout).$(", fwdEf=").$(efBlobs).$(", bwdEf=").$(bwdEf).I$();
                    Assert.assertTrue("no EF blobs visited", efBlobs > 0 && bwdEf > 0);
                    Assert.assertEquals("unranked fwd/bwd blobs (silent fallback to the linear seek):" + bad, 0, unrankedBlobs + bwdUnranked);
                }
            }
        });
    }

    private void assertUnusualBounds(byte encoding, boolean ranked, int layout, long base, long columnTop) throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);
            final ObjList<LongList> oracle = new ObjList<>();
            for (int k = 0; k < KEYS; k++) {
                oracle.add(new LongList());
            }
            if (base == 0) {
                for (long r = 0; r < columnTop; r++) {
                    oracle.getQuick(0).add(r);
                }
            }
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                final String name = "seek_edge_" + encoding + "_" + ranked + "_" + layout + "_" + base;
                final boolean wasRanked = PostingIndexUtils.isEfRankTrailerEnabled;
                PostingIndexUtils.isEfRankTrailerEnabled = ranked;
                long lastRow = 0;
                try (PostingIndexWriter writer = new PostingIndexWriter(configuration, encoding)) {
                    writer.of(path, name, COLUMN_NAME_TXN_NONE, true);
                    writer.setNextTxnAtSeal(0L);
                    long row = base + columnTop;
                    for (int i = 0; i < ROWS; i++) {
                        // irregular gaps so large-base values exercise high L
                        row += base > 0 ? 1 + rnd.nextInt(1_000) : 1;
                        final int key = keyOf(rnd, i);
                        writer.add(key, row);
                        oracle.getQuick(key).add(row);
                        if ((i + 1) % 3_001 == 0 || i == ROWS - 1) {
                            writer.setMaxValue(row);
                            writer.commit();
                        }
                        if (layout == 2 && i == ROWS / 2) {
                            writer.setMaxValue(row);
                            writer.commit();
                            writer.seal();
                        }
                    }
                    lastRow = row;
                    if (layout == 0) {
                        writer.seal();
                    }
                } finally {
                    PostingIndexUtils.isEfRankTrailerEnabled = wasRanked;
                }
                path.trimTo(plen);
                final long top = base == 0 ? columnTop : 0;
                try (
                        PostingIndexFwdReader fwd = new PostingIndexFwdReader(configuration, path, name, COLUMN_NAME_TXN_NONE, -1, top);
                        PostingIndexBwdReader bwd = new PostingIndexBwdReader(configuration, path, name, COLUMN_NAME_TXN_NONE, -1, top, null, null, 0)
                ) {
                    for (int key = 0; key < KEYS; key++) {
                        final LongList exp = oracle.getQuick(key);
                        final LongList probes = new LongList();
                        probes.add(0);
                        probes.add(1);
                        probes.add(base);
                        probes.add(lastRow);
                        probes.add(lastRow + 1);
                        probes.add(lastRow - 1);
                        for (int i = 0; i < 40 && exp.size() > 0; i++) {
                            final long v = exp.getQuick(rnd.nextInt(exp.size()));
                            probes.add(v);
                            probes.add(v - 1);
                            probes.add(v + 1);
                        }
                        if (exp.size() > 0) {
                            probes.add(exp.getQuick(0));
                            probes.add(exp.getLast());
                            probes.add(exp.getLast() - 1);
                        }
                        final long[][] extra = {
                                {0, Long.MAX_VALUE}, {0, -1}, {-5, -1}, {1, 0},
                                {lastRow + 1, Long.MAX_VALUE}, {Long.MAX_VALUE - 1, Long.MAX_VALUE},
                        };
                        for (long[] b : extra) {
                            if (key == 0 && b[0] < 0) {
                                continue; // NullCursor emits negative row ids for lo < 0 (pre-existing, no caller)
                            }
                            final String label = "key=" + key + " [" + b[0] + "," + b[1] + "]";
                            assertFrame(label + " fwd", fwd.getCursor(key, b[0], b[1]), exp, b[0], b[1], true);
                            assertFrame(label + " bwd", bwd.getCursor(key, b[0], b[1]), exp, b[0], b[1], false);
                        }
                        for (int i = 0; i < probes.size(); i++) {
                            final long a = probes.getQuick(i);
                            if (key == 0 && a < 0) {
                                // value - 1 of the null key's row 0; see the NullCursor note above
                                continue;
                            }
                            for (int j = 0; j < probes.size(); j += 3) {
                                final long b = probes.getQuick(j);
                                final String label = "key=" + key + " [" + a + "," + b + "]";
                                assertFrame(label + " fwd", fwd.getCursor(key, a, b), exp, a, b, true);
                                assertFrame(label + " bwd", bwd.getCursor(key, a, b), exp, a, b, false);
                            }
                            assertFrame("key=" + key + " [" + a + ",MAX] fwd", fwd.getCursor(key, a, Long.MAX_VALUE), exp, a, Long.MAX_VALUE, true);
                            assertFrame("key=" + key + " [" + a + ",MAX] bwd", bwd.getCursor(key, a, Long.MAX_VALUE), exp, a, Long.MAX_VALUE, false);
                        }
                    }
                }
            }
        });
    }
}
