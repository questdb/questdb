/*******************************************************************************
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

import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointPageRef;
import io.questdb.cairo.lv.LiveViewCheckpointRowPositionDeltaReader;
import io.questdb.cairo.lv.LiveViewCheckpointRowPositionDeltaWriter;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineEntry;
import io.questdb.std.FilesFacade;
import io.questdb.std.Rnd;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Coverage for the persistent copy-on-write row-position difference/prefix-sum
 * B+ tree built by {@link LiveViewCheckpointRowPositionDeltaWriter} and
 * navigated by {@link LiveViewCheckpointRowPositionDeltaReader}. A localized O3
 * repair shifts every reused suffix root's cumulative {@code lvRowPosition}
 * with one suffix range-add (a difference-array point add); {@code prefixSum}
 * reads back the accumulated shift for any key. The tests assert prefix-sum and
 * effective-position correctness against a sorted-list oracle - suffix
 * semantics, composite-key ties, accumulation, negative deltas,
 * {@code Long.MAX_VALUE} keys - plus the structural sharing the copy-on-write
 * contract promises.
 * <p>
 * Small node capacities force many tree levels and repeated splits so the
 * assertions exercise internal navigation and subtree-sum aggregation rather than a
 * single fat leaf.
 */
public class LiveViewCheckpointRowPositionDeltaTest extends AbstractCairoTest {

    private static final String LV_DIR = "lv_rp_delta";

    @Before
    public void setUp() {
        super.setUp();
        try (Path path = new Path()) {
            final FilesFacade ff = configuration.getFilesFacade();
            checkpointsDir(path).concat(LiveViewCheckpointLayout.META_DIR_NAME).slash();
            ff.mkdirs(path, configuration.getMkDirMode());
        }
    }

    @Test
    public void testAccumulateAtSameKey() throws Exception {
        assertMemoryLeak(() -> {
            try (Harness h = new Harness(3, 3)) {
                h.suffixAdd(100, 5, 7);
                h.suffixAdd(100, 5, 4);  // accumulates: diff[(100,5)] = 11
                h.suffixAdd(100, 5, -3); // diff[(100,5)] = 8
                Assert.assertEquals(1, h.reader.size(h.root));
                h.assertPrefixSum(100, 5);
                h.assertPrefixSum(101, 0);
                h.assertIterateAll();
            }
        });
    }

    @Test
    public void testCompositeKeyTieSemantics() throws Exception {
        assertMemoryLeak(() -> {
            try (Harness h = new Harness(4, 4)) {
                // A repaired boundary and a suffix boundary can share a timestamp;
                // the breakpoint at the first suffix key (100, 5) must exclude
                // (100, 4) yet include (100, 5) and (100, 6).
                h.suffixAdd(100, 5, 7);
                h.assertPrefixSum(99, Long.MAX_VALUE); // 0
                h.assertPrefixSum(100, 4);             // 0
                h.assertPrefixSum(100, 5);             // 7
                h.assertPrefixSum(100, 6);             // 7
                h.assertPrefixSum(101, Long.MIN_VALUE);// 7
            }
        });
    }

    @Test
    public void testEffectivePosition() throws Exception {
        assertMemoryLeak(() -> {
            try (Harness h = new Harness(4, 4)) {
                // Five checkpoint boundaries with cumulative observed positions.
                // An O3 repair over [C, H) = [25, 45) touches boundaries at 30, 40;
                // its replacement adds 25 output rows, so every root at or above the
                // first suffix key (50, 5) shifts by +25.
                final LiveViewCheckpointTimelineEntry b10 = entry(10, 1, 100);
                final LiveViewCheckpointTimelineEntry b20 = entry(20, 2, 200);
                final LiveViewCheckpointTimelineEntry b30 = entry(30, 3, 300);
                final LiveViewCheckpointTimelineEntry b40 = entry(40, 4, 400);
                final LiveViewCheckpointTimelineEntry b50 = entry(50, 5, 500);

                // No deltas yet: effective == base.
                Assert.assertEquals(100, h.reader.effectivePosition(h.root, b10));
                Assert.assertEquals(500, h.reader.effectivePosition(h.root, b50));

                h.suffixAdd(50, 5, 25);

                // Prefix roots (< first suffix key) are untouched.
                Assert.assertEquals(100, h.reader.effectivePosition(h.root, b10));
                Assert.assertEquals(200, h.reader.effectivePosition(h.root, b20));
                Assert.assertEquals(300, h.reader.effectivePosition(h.root, b30));
                Assert.assertEquals(400, h.reader.effectivePosition(h.root, b40));
                // Suffix root shifts by the replacement's row-count delta, with no
                // leaf rewrite of its own timeline entry.
                Assert.assertEquals(525, h.reader.effectivePosition(h.root, b50));

                // A second, later O3 repair shifts (50, 5) again and a further suffix.
                h.suffixAdd(70, 7, -4);
                Assert.assertEquals(525, h.reader.effectivePosition(h.root, b50)); // below (70,7)
                final LiveViewCheckpointTimelineEntry b80 = entry(80, 8, 800);
                Assert.assertEquals(800 + 25 - 4, h.reader.effectivePosition(h.root, b80));
            }
        });
    }

    @Test
    public void testEmptyTree() throws Exception {
        assertMemoryLeak(() -> {
            try (Harness h = new Harness(4, 4)) {
                Assert.assertEquals(0, h.reader.size(h.root));
                Assert.assertEquals(0, h.reader.prefixSum(h.root, 0, 0));
                Assert.assertEquals(0, h.reader.prefixSum(h.root, Long.MAX_VALUE, Long.MAX_VALUE));
                Assert.assertEquals(0, h.reader.rootChildCount(h.root));
                final LiveViewCheckpointTimelineEntry e = entry(5, 5, 42);
                Assert.assertEquals(42, h.reader.effectivePosition(h.root, e));
                h.assertIterateAll();
            }
        });
    }

    @Test
    public void testLongMaxTimestampKey() throws Exception {
        assertMemoryLeak(() -> {
            try (Harness h = new Harness(3, 3)) {
                h.suffixAdd(Long.MAX_VALUE - 1, 1, 11);
                h.suffixAdd(Long.MAX_VALUE, 2, 22);
                h.suffixAdd(100, 3, 33);
                h.assertIterateAll();
                // Long.MAX_VALUE is a real key: a query strictly below it excludes it.
                h.assertPrefixSum(Long.MAX_VALUE - 1, 0);       // below both MAX keys
                h.assertPrefixSum(Long.MAX_VALUE - 1, 1);       // includes (MAX-1, 1)
                h.assertPrefixSum(Long.MAX_VALUE, 1);           // excludes (MAX, 2)
                h.assertPrefixSum(Long.MAX_VALUE, 2);           // includes all
                Assert.assertEquals(66, h.reader.prefixSum(h.root, Long.MAX_VALUE, Long.MAX_VALUE));
            }
        });
    }

    @Test
    public void testManyBreakpointsMultiLevel() throws Exception {
        assertMemoryLeak(() -> {
            try (Harness h = new Harness(3, 3)) {
                final int n = 60;
                for (int i = 0; i < n; i++) {
                    h.suffixAdd(i * 10L, i, (i % 5) - 2); // mix of positive/negative/zero deltas
                    // Every suffix-add copies only a spine of pages, never the whole tree.
                    Assert.assertTrue(
                            "suffixAdd wrote too many pages: " + h.writer.getLastSegmentPageCount(),
                            h.writer.getLastSegmentPageCount() <= 12
                    );
                }
                Assert.assertEquals(n, h.reader.size(h.root));
                h.assertIterateAll();
                // Prefix sum at every key boundary, strictly between keys, below min,
                // and above max.
                for (int i = -1; i <= n; i++) {
                    h.assertPrefixSum(i * 10L, Long.MIN_VALUE);
                    h.assertPrefixSum(i * 10L, i);
                    h.assertPrefixSum(i * 10L - 5, Long.MAX_VALUE);
                }
            }
        });
    }

    @Test
    public void testPruneBelowAccumulatesIntoExistingFloorBreakpoint() throws Exception {
        assertMemoryLeak(() -> {
            try (Harness h = new Harness(4, 4)) {
                h.suffixAdd(10, 1, 5);
                h.suffixAdd(20, 2, -3);
                h.suffixAdd(30, 3, 7);
                h.suffixAdd(40, 4, 11);
                // The floor key already carries a breakpoint, so the discarded
                // prefix accumulates into it rather than inserting beside it.
                h.assertPruneBelowPreservesSuffix(30, 3);
                Assert.assertEquals(2, h.reader.size(h.root));
                h.assertIterateAll();
                h.assertPrefixSum(30, 3); // 5 - 3 + 7
                h.assertPrefixSum(40, 4);
                Assert.assertEquals(9, h.reader.prefixSum(h.root, 30, 3));
            }
        });
    }

    @Test
    public void testPruneBelowEmptiesTreeWhenPrefixCancels() throws Exception {
        assertMemoryLeak(() -> {
            try (Harness h = new Harness(4, 4)) {
                h.suffixAdd(10, 1, 40);
                h.suffixAdd(20, 2, -40);
                // Both breakpoints are below the floor and cancel out, so there is
                // nothing to fold forward and the tree goes empty.
                Assert.assertTrue(h.pruneBelow(100, 9));
                Assert.assertTrue("an emptied index must publish a null root", h.root.isNull());
                Assert.assertEquals(0, h.reader.size(h.root));
                Assert.assertEquals(0, h.writer.getLastSegmentBytes());
                h.assertIterateAll();
                h.assertPrefixSum(100, 9);
                h.assertPrefixSum(Long.MAX_VALUE, Long.MAX_VALUE);
            }
        });
    }

    @Test
    public void testPruneBelowFoldsDiscardedPrefixIntoFloorKey() throws Exception {
        assertMemoryLeak(() -> {
            try (Harness h = new Harness(3, 3)) {
                final int n = 40;
                for (int i = 0; i < n; i++) {
                    h.suffixAdd(i * 10L, i, i + 1);
                }
                // Floor between two breakpoints, so the survivor set starts at a key
                // the index does not hold and the fold has to insert one.
                h.assertPruneBelowPreservesSuffix(205, 20);
                h.assertIterateAll();
                Assert.assertEquals(
                        "the folded breakpoint plus the survivors",
                        n - 21 + 1,
                        h.reader.size(h.root)
                );
                // Nothing below the floor is addressable any more.
                Assert.assertEquals(0, h.reader.prefixSum(h.root, 204, Long.MAX_VALUE));
            }
        });
    }

    @Test
    public void testPruneBelowNothingBelowFloorIsNoOp() throws Exception {
        assertMemoryLeak(() -> {
            try (Harness h = new Harness(3, 3)) {
                for (int i = 0; i < 20; i++) {
                    h.suffixAdd(i * 10L, i, i + 1);
                }
                final LiveViewCheckpointPageRef before = new LiveViewCheckpointPageRef();
                before.of(h.root.getSegmentId(), h.root.getOffset(), h.root.getLength());
                Assert.assertFalse(h.pruneBelow(0, 0));
                Assert.assertTrue("a no-op prune must reuse the root", sameRef(before, h.root));
                Assert.assertEquals(0, h.writer.getLastSegmentBytes());
                h.assertIterateAll();
            }
        });
    }

    @Test
    public void testPruneBelowRandomAgainstOracle() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = new Rnd(0x51ED_1234L, 0xDEC0DEL);
            try (Harness h = new Harness(3, 4)) {
                for (int i = 0; i < 300; i++) {
                    h.suffixAdd(rnd.nextLong(120) - 20, rnd.nextLong(3), rnd.nextLong(200) - 100);
                }
                // Retire a random prefix repeatedly. Each round asserts that every
                // key at or above the new floor still reports the prefix sum it
                // reported before the prune - which is the whole contract - on top
                // of the oracle comparison.
                for (int round = 0; round < 8 && !h.root.isNull(); round++) {
                    final long floorTs = rnd.nextLong(120) - 20;
                    final long floorId = rnd.nextLong(3);
                    h.assertPruneBelowPreservesSuffix(floorTs, floorId);
                    h.assertIterateAll();
                    for (int q = 0; q < 40; q++) {
                        h.assertPrefixSum(rnd.nextLong(160) - 40, rnd.nextLong(5) - 1);
                    }
                }
            }
        });
    }

    @Test
    public void testPruneBelowReusesSuffixSubtrees() throws Exception {
        assertMemoryLeak(() -> {
            try (Harness h = new Harness(3, 3)) {
                for (int i = 0; i < 40; i++) {
                    h.suffixAdd(i * 10L, i, i + 1);
                }
                final int childCount = h.reader.rootChildCount(h.root);
                Assert.assertTrue("expected a multi-level tree", childCount >= 2);
                final LiveViewCheckpointPageRef lastChild = new LiveViewCheckpointPageRef();
                h.reader.rootChildRef(h.root, childCount - 1, lastChild);

                // A shallow prune touches only the leftmost spine, so every other
                // subtree keeps its page reference and the sums above stay exact.
                h.assertPruneBelowPreservesSuffix(15, 0);
                final int wrote = h.writer.getLastSegmentPageCount();
                Assert.assertEquals("the root shape must be untouched", childCount, h.reader.rootChildCount(h.root));
                final LiveViewCheckpointPageRef after = new LiveViewCheckpointPageRef();
                h.reader.rootChildRef(h.root, childCount - 1, after);
                Assert.assertTrue("rightmost subtree must be reused", sameRef(lastChild, after));
                Assert.assertTrue("prune copied too many pages: " + wrote, wrote <= childCount + 4);
                h.assertIterateAll();
            }
        });
    }

    @Test
    public void testRandomAgainstOracle() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = new Rnd(0x51ED_1234L, 0xC0FFEEL);
            try (Harness h = new Harness(3, 4)) {
                final int n = 400;
                for (int i = 0; i < n; i++) {
                    // Random keys (with deliberate collisions to exercise accumulation)
                    // and signed deltas.
                    final long ts = rnd.nextLong(80) - 20;
                    final long id = rnd.nextLong(4);
                    final long delta = rnd.nextLong(2_000) - 1_000;
                    h.suffixAdd(ts, id, delta);
                }
                h.assertIterateAll();
                for (int q = 0; q < 400; q++) {
                    final long qts = rnd.nextLong(120) - 40;
                    final long qid = rnd.nextLong(6) - 1;
                    h.assertPrefixSum(qts, qid);
                }
                // Total == prefix sum at the top of the key space.
                Assert.assertEquals(h.oracleTotal(), h.reader.prefixSum(h.root, Long.MAX_VALUE, Long.MAX_VALUE));
            }
        });
    }

    @Test
    public void testReusesUntouchedSubtrees() throws Exception {
        assertMemoryLeak(() -> {
            try (Harness h = new Harness(3, 3)) {
                for (int i = 0; i < 40; i++) {
                    h.suffixAdd(i * 10L, i, i + 1);
                }
                final int childCount = h.reader.rootChildCount(h.root);
                Assert.assertTrue("expected a multi-level tree", childCount >= 2);

                // Capture the root's child references before the mutation.
                final LiveViewCheckpointPageRef[] before = new LiveViewCheckpointPageRef[childCount];
                for (int i = 0; i < childCount; i++) {
                    before[i] = new LiveViewCheckpointPageRef();
                    h.reader.rootChildRef(h.root, i, before[i]);
                }

                // Accumulate into the global-minimum key (0, 0): a pure spine copy into
                // the leftmost subtree, no structural change.
                h.suffixAdd(0, 0, 500);
                final int wrote = h.writer.getLastSegmentPageCount();

                Assert.assertEquals(childCount, h.reader.rootChildCount(h.root));
                final LiveViewCheckpointPageRef after = new LiveViewCheckpointPageRef();
                // Child 0 (the touched subtree) was rewritten...
                h.reader.rootChildRef(h.root, 0, after);
                Assert.assertFalse(sameRef(before[0], after));
                // ...while every other child subtree kept its exact page reference.
                for (int i = 1; i < childCount; i++) {
                    h.reader.rootChildRef(h.root, i, after);
                    Assert.assertTrue("child " + i + " must be reused", sameRef(before[i], after));
                }
                Assert.assertTrue("suffixAdd copied too many pages: " + wrote, wrote <= childCount + 4);
                h.assertIterateAll();
            }
        });
    }

    @Test
    public void testSingleSuffixAdd() throws Exception {
        assertMemoryLeak(() -> {
            try (Harness h = new Harness(4, 4)) {
                h.suffixAdd(200, 20, 15);
                Assert.assertEquals(1, h.reader.size(h.root));
                h.assertPrefixSum(199, Long.MAX_VALUE); // below breakpoint -> 0
                h.assertPrefixSum(200, 20);             // at breakpoint -> 15
                h.assertPrefixSum(1_000, 0);            // above -> 15
                h.assertIterateAll();
            }
        });
    }

    private static Path checkpointsDir(Path path) {
        path.of(configuration.getDbRoot()).concat(LV_DIR).concat("_checkpoints");
        return path;
    }

    private static int compareKey(long aTs, long aId, long bTs, long bId) {
        if (aTs != bTs) {
            return Long.compare(aTs, bTs);
        }
        return Long.compare(aId, bId);
    }

    private static LiveViewCheckpointTimelineEntry entry(long ts, long id, long basePosition) {
        final LiveViewCheckpointTimelineEntry e = new LiveViewCheckpointTimelineEntry();
        e.of(ts, id, 0, basePosition, 0);
        return e;
    }

    private static boolean sameRef(LiveViewCheckpointPageRef a, LiveViewCheckpointPageRef b) {
        return a.getSegmentId() == b.getSegmentId() && a.getOffset() == b.getOffset() && a.getLength() == b.getLength();
    }

    /**
     * Drives a row-position delta writer/reader against a sorted-list oracle keyed by
     * {@code (maxTimestamp, checkpointId)}, accumulating a signed diff per key.
     */
    private final class Harness implements AutoCloseable {
        // Each oracle row: {ts, id, diff}.
        final List<long[]> oracle = new ArrayList<>();
        final LiveViewCheckpointRowPositionDeltaReader reader;
        final LiveViewCheckpointPageRef root = new LiveViewCheckpointPageRef();
        final LiveViewCheckpointRowPositionDeltaWriter writer;
        private final Path dir = new Path();
        private final LiveViewCheckpointPageRef tmpRoot = new LiveViewCheckpointPageRef();
        private long nextSegmentId;

        Harness(int leafCapacity, int internalCapacity) {
            checkpointsDir(dir);
            writer = new LiveViewCheckpointRowPositionDeltaWriter(configuration, leafCapacity, internalCapacity);
            writer.of(dir);
            reader = new LiveViewCheckpointRowPositionDeltaReader(configuration);
            reader.of(dir);
        }

        void assertIterateAll() {
            final List<long[]> got = new ArrayList<>();
            reader.iterateAll(root, (ts, id, diff) -> got.add(new long[]{ts, id, diff}));
            Assert.assertEquals(oracle.size(), got.size());
            for (int i = 0; i < oracle.size(); i++) {
                Assert.assertArrayEquals("entry " + i + " mismatch", oracle.get(i), got.get(i));
            }
        }

        void assertPrefixSum(long ts, long id) {
            Assert.assertEquals(
                    "prefixSum(" + ts + ", " + id + ")",
                    oraclePrefixSum(ts, id),
                    reader.prefixSum(root, ts, id)
            );
        }

        /**
         * Snapshots what every key at or above the floor reports, prunes, and
         * asserts each of them still reports the same value. That equality is the
         * whole contract of the fold: a difference keyed to a retired boundary
         * still applies to every surviving one.
         */
        void assertPruneBelowPreservesSuffix(long floorTs, long floorId) {
            final List<long[]> survivors = new ArrayList<>();
            for (int i = 0; i < oracle.size(); i++) {
                final long[] e = oracle.get(i);
                if (compareKey(e[0], e[1], floorTs, floorId) >= 0) {
                    survivors.add(new long[]{e[0], e[1], reader.prefixSum(root, e[0], e[1])});
                }
            }
            pruneBelow(floorTs, floorId);
            for (int i = 0; i < survivors.size(); i++) {
                final long[] s = survivors.get(i);
                Assert.assertEquals(
                        "prefixSum(" + s[0] + ", " + s[1] + ") must survive the prune",
                        s[2],
                        reader.prefixSum(root, s[0], s[1])
                );
            }
        }

        @Override
        public void close() {
            writer.close();
            reader.close();
            dir.close();
        }

        long oracleTotal() {
            long sum = 0;
            for (int i = 0; i < oracle.size(); i++) {
                sum += oracle.get(i)[2];
            }
            return sum;
        }

        boolean pruneBelow(long floorTs, long floorId) {
            final boolean changed = writer.pruneBelow(root, floorTs, floorId, nextSegmentId++, tmpRoot);
            root.of(tmpRoot.getSegmentId(), tmpRoot.getOffset(), tmpRoot.getLength());
            if (changed) {
                long folded = 0;
                for (int i = oracle.size() - 1; i >= 0; i--) {
                    final long[] e = oracle.get(i);
                    if (compareKey(e[0], e[1], floorTs, floorId) < 0) {
                        folded += e[2];
                        oracle.remove(i);
                    }
                }
                if (folded != 0) {
                    oracleAdd(floorTs, floorId, folded);
                }
            }
            return changed;
        }

        void suffixAdd(long ts, long id, long delta) {
            writer.suffixAdd(root, ts, id, delta, nextSegmentId++, tmpRoot);
            root.of(tmpRoot.getSegmentId(), tmpRoot.getOffset(), tmpRoot.getLength());
            oracleAdd(ts, id, delta);
        }

        private void oracleAdd(long ts, long id, long delta) {
            int pos = 0;
            while (pos < oracle.size()) {
                final long[] o = oracle.get(pos);
                if (o[0] == ts && o[1] == id) {
                    o[2] += delta;
                    return;
                }
                if (o[0] > ts || (o[0] == ts && o[1] > id)) {
                    break;
                }
                pos++;
            }
            oracle.add(pos, new long[]{ts, id, delta});
        }

        private long oraclePrefixSum(long ts, long id) {
            long sum = 0;
            for (int i = 0; i < oracle.size(); i++) {
                final long[] e = oracle.get(i);
                if (compareKey(e[0], e[1], ts, id) <= 0) {
                    sum += e[2];
                } else {
                    break;
                }
            }
            return sum;
        }
    }
}
