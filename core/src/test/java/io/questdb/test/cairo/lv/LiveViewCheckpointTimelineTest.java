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
import io.questdb.cairo.lv.LiveViewCheckpointTimelineEntry;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineReader;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineWriter;
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
 * Coverage for the persistent copy-on-write timeline B+ tree (design sections 7,
 * 8.1) built by {@link LiveViewCheckpointTimelineWriter} and navigated by
 * {@link LiveViewCheckpointTimelineReader}. Each mutation copies only the search
 * path into a fresh metadata segment and reuses untouched subtrees, so the tests
 * assert both correctness against a sorted-list oracle - append, predecessor,
 * range {@code [C, H)}, point lookup, splice - and the structural sharing the
 * copy-on-write contract promises.
 * <p>
 * Small node capacities force many tree levels and repeated splits so the
 * assertions exercise internal navigation rather than a single fat leaf.
 */
public class LiveViewCheckpointTimelineTest extends AbstractCairoTest {

    private static final String LV_DIR = "lv_timeline";

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
    public void testDuplicateMaxTimestampDistinctIds() throws Exception {
        assertMemoryLeak(() -> {
            try (Harness h = new Harness(3, 3)) {
                h.append(10, 100);
                h.append(50, 500); // (50, 500)
                h.append(50, 501); // (50, 501)
                h.append(50, 502); // (50, 502)
                h.append(90, 900);

                // Strict predecessor skips the whole tie at 50.
                h.assertPredecessor(50);  // -> (10, 100)
                h.assertPredecessor(51);  // -> (50, 502)
                h.assertPredecessor(90);  // -> (50, 502)
                h.assertPredecessor(91);  // -> (90, 900)

                // A range that starts exactly on the tie must include all three.
                h.assertRange(50, 51);
                h.assertRange(50, 90);
                h.assertRange(51, 90);
                h.assertIterateAll();
            }
        });
    }

    @Test
    public void testEmptyTree() throws Exception {
        assertMemoryLeak(() -> {
            try (Harness h = new Harness(4, 4)) {
                Assert.assertEquals(0, h.reader.size(h.root));
                Assert.assertFalse(h.reader.predecessor(h.root, 100, h.out));
                h.assertRange(0, 1_000);
                h.assertIterateAll();
                Assert.assertFalse(h.reader.findExact(h.root, 5, 5, h.out));
                Assert.assertEquals(0, h.reader.rootChildCount(h.root));
            }
        });
    }

    @Test
    public void testInOrderAppendsMultiLevel() throws Exception {
        assertMemoryLeak(() -> {
            try (Harness h = new Harness(3, 3)) {
                final int n = 60;
                for (int i = 0; i < n; i++) {
                    h.append(i * 10L, i);
                    // Every append copies only a spine of pages, never the whole tree.
                    Assert.assertTrue(
                            "append wrote too many pages: " + h.writer.getLastSegmentPageCount(),
                            h.writer.getLastSegmentPageCount() <= 12
                    );
                }
                Assert.assertEquals(n, h.reader.size(h.root));
                h.assertIterateAll();
                // Predecessor at every edge and interior point, including one past the last key.
                for (int i = 0; i <= n; i++) {
                    h.assertPredecessor(i * 10L);      // exactly a key boundary
                    h.assertPredecessor(i * 10L - 5);  // strictly between keys / below min
                }
                // Point lookup hits every stored key and misses the gaps.
                for (int i = 0; i < n; i++) {
                    h.assertFindExact(i * 10L, i);
                }
                h.assertFindExactMissing(5, 5);
                h.assertFindExactMissing(600, 60);
                // Ranges at the edges and spanning several leaves.
                h.assertRange(Long.MIN_VALUE, Long.MAX_VALUE);
                h.assertRange(-100, 5);
                h.assertRange(0, 10);
                h.assertRange(95, 305);
                h.assertRange(590, 1_000);
                h.assertRange(1_000, 2_000);
            }
        });
    }

    @Test
    public void testLongMaxTimestampIsData() throws Exception {
        assertMemoryLeak(() -> {
            try (Harness h = new Harness(3, 3)) {
                h.append(Long.MAX_VALUE - 1, 1);
                h.append(Long.MAX_VALUE, 2);
                h.append(100, 3);
                h.assertIterateAll();
                h.assertFindExact(Long.MAX_VALUE, 2);
                // Long.MAX_VALUE is a valid designated timestamp: predecessor of it
                // is the entry strictly below, not "infinity".
                h.assertPredecessor(Long.MAX_VALUE);      // -> (Long.MAX_VALUE - 1, 1)
                h.assertRange(100, Long.MAX_VALUE);        // excludes Long.MAX_VALUE
            }
        });
    }

    @Test
    public void testRandomAppendsAndQueriesAgainstOracle() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = new Rnd(0x9E3779B9L, 0x7F4A7C15L);
            try (Harness h = new Harness(3, 4)) {
                final int n = 400;
                for (int i = 0; i < n; i++) {
                    // Random timestamps (with collisions) and a unique monotonic id.
                    h.append(rnd.nextLong(120) - 20, i);
                }
                Assert.assertEquals(n, h.reader.size(h.root));
                h.assertIterateAll();
                for (int q = 0; q < 300; q++) {
                    final long c = rnd.nextLong(160) - 40;
                    h.assertPredecessor(c);
                    final long lo = rnd.nextLong(160) - 40;
                    final long hi = lo + rnd.nextLong(60);
                    h.assertRange(lo, hi);
                }
                // Every stored key is found exactly.
                for (int i = 0; i < h.oracle.size(); i++) {
                    final long[] e = h.oracle.get(i);
                    h.assertFindExact(e[0], e[1]);
                }
            }
        });
    }

    @Test
    public void testSingleEntry() throws Exception {
        assertMemoryLeak(() -> {
            try (Harness h = new Harness(4, 4)) {
                h.append(42, 7);
                Assert.assertEquals(1, h.reader.size(h.root));
                Assert.assertFalse(h.reader.predecessor(h.root, 42, h.out)); // strict: nothing below 42
                h.assertPredecessor(43);
                h.assertFindExact(42, 7);
                h.assertFindExactMissing(42, 8);
                h.assertRange(42, 43);
                h.assertRange(0, 42); // excludes 42
                h.assertIterateAll();
            }
        });
    }

    @Test
    public void testSplicePreservesUnaffectedEntries() throws Exception {
        assertMemoryLeak(() -> {
            try (Harness h = new Harness(3, 3)) {
                final int n = 40;
                for (int i = 0; i < n; i++) {
                    h.append(i * 10L, i);
                }
                // Re-version a contiguous key range [150, 260): keys 15..25.
                h.splice(150, 260);
                Assert.assertEquals(n, h.reader.size(h.root));
                h.assertIterateAll();      // order + every payload matches the oracle
                for (int i = 0; i < n; i++) {
                    h.assertFindExact(i * 10L, i);
                }
            }
        });
    }

    @Test
    public void testSpliceReusesUntouchedSubtrees() throws Exception {
        assertMemoryLeak(() -> {
            try (Harness h = new Harness(3, 3)) {
                for (int i = 0; i < 40; i++) {
                    h.append(i * 10L, i);
                }
                final int childCount = h.reader.rootChildCount(h.root);
                Assert.assertTrue("expected a multi-level tree", childCount >= 2);

                // Capture the root's child references before the splice.
                final LiveViewCheckpointPageRef[] before = new LiveViewCheckpointPageRef[childCount];
                for (int i = 0; i < childCount; i++) {
                    before[i] = new LiveViewCheckpointPageRef();
                    h.reader.rootChildRef(h.root, i, before[i]);
                }

                // Splice a single key that lives in the leftmost subtree.
                h.splice(0, 5); // keys with ts in [0, 5) -> just (0, 0)
                final int spliceWrote = h.writer.getLastSegmentPageCount();

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
                // The splice copied only the spine to the one touched leaf, far fewer
                // pages than the whole tree.
                Assert.assertTrue("splice copied too many pages: " + spliceWrote, spliceWrote <= childCount + 4);
                h.assertIterateAll();
            }
        });
    }

    @Test
    public void testSpliceWholeTree() throws Exception {
        assertMemoryLeak(() -> {
            try (Harness h = new Harness(3, 3)) {
                final int n = 30;
                for (int i = 0; i < n; i++) {
                    h.append(i * 10L, i);
                }
                // Splice every entry.
                h.splice(Long.MIN_VALUE, Long.MAX_VALUE);
                Assert.assertEquals(n, h.reader.size(h.root));
                h.assertIterateAll();
            }
        });
    }

    private static Path checkpointsDir(Path path) {
        path.of(configuration.getDbRoot()).concat(LV_DIR).concat("_checkpoints");
        return path;
    }

    private static boolean sameRef(LiveViewCheckpointPageRef a, LiveViewCheckpointPageRef b) {
        return a.getSegmentId() == b.getSegmentId() && a.getOffset() == b.getOffset() && a.getLength() == b.getLength();
    }

    /**
     * Drives a timeline writer/reader against a sorted-list oracle. Payload values
     * are deterministic functions of the key so a mismatch is easy to localize;
     * splice rewrites them to a second deterministic function.
     */
    private final class Harness implements AutoCloseable {
        final LiveViewCheckpointTimelineEntry in = new LiveViewCheckpointTimelineEntry();
        // Each oracle row: {ts, id, createdLvSeqTxn, baseRowPosition, logicalStateBytes, rootSeg, rootOff, rootLen}.
        final List<long[]> oracle = new ArrayList<>();
        final LiveViewCheckpointTimelineEntry out = new LiveViewCheckpointTimelineEntry();
        final LiveViewCheckpointTimelineReader reader;
        final LiveViewCheckpointPageRef root = new LiveViewCheckpointPageRef();
        final LiveViewCheckpointTimelineWriter writer;
        private final Path dir = new Path();
        private final LiveViewCheckpointPageRef tmpRoot = new LiveViewCheckpointPageRef();
        private long nextSegmentId;

        Harness(int leafCapacity, int internalCapacity) {
            checkpointsDir(dir);
            writer = new LiveViewCheckpointTimelineWriter(configuration, leafCapacity, internalCapacity);
            writer.of(dir);
            reader = new LiveViewCheckpointTimelineReader(configuration);
            reader.of(dir);
        }

        void append(long ts, long id) {
            fill(in, ts, id, false);
            writer.append(root, in, nextSegmentId++, tmpRoot);
            root.of(tmpRoot.getSegmentId(), tmpRoot.getOffset(), tmpRoot.getLength());
            oracleInsert(ts, id, false);
        }

        void assertFindExact(long ts, long id) {
            Assert.assertTrue("expected to find (" + ts + ", " + id + ")", reader.findExact(root, ts, id, out));
            final long[] e = oracleFind(ts, id);
            Assert.assertNotNull(e);
            assertEntry(e, out);
        }

        void assertFindExactMissing(long ts, long id) {
            Assert.assertFalse(reader.findExact(root, ts, id, out));
        }

        void assertIterateAll() {
            final List<long[]> got = new ArrayList<>();
            reader.iterateAll(root, entry -> got.add(snapshot(entry)));
            Assert.assertEquals(oracle.size(), got.size());
            for (int i = 0; i < oracle.size(); i++) {
                assertRow(oracle.get(i), got.get(i));
            }
        }

        void assertPredecessor(long correctionTimestamp) {
            final long[] expected = oraclePredecessor(correctionTimestamp);
            final boolean found = reader.predecessor(root, correctionTimestamp, out);
            if (expected == null) {
                Assert.assertFalse("expected no predecessor for C=" + correctionTimestamp, found);
                return;
            }
            Assert.assertTrue("expected a predecessor for C=" + correctionTimestamp, found);
            assertEntry(expected, out);
        }

        void assertRange(long lo, long hi) {
            final List<long[]> got = new ArrayList<>();
            reader.range(root, lo, hi, entry -> got.add(snapshot(entry)));
            final List<long[]> expected = new ArrayList<>();
            for (int i = 0; i < oracle.size(); i++) {
                final long[] e = oracle.get(i);
                if (e[0] >= lo && e[0] < hi) {
                    expected.add(e);
                }
            }
            Assert.assertEquals("range [" + lo + ", " + hi + ") size", expected.size(), got.size());
            for (int i = 0; i < expected.size(); i++) {
                assertRow(expected.get(i), got.get(i));
            }
        }

        @Override
        public void close() {
            writer.close();
            reader.close();
            dir.close();
        }

        void splice(long lo, long hi) {
            final List<long[]> affected = new ArrayList<>();
            for (int i = 0; i < oracle.size(); i++) {
                final long[] e = oracle.get(i);
                if (e[0] >= lo && e[0] < hi) {
                    affected.add(e);
                }
            }
            final LiveViewCheckpointTimelineEntry[] reps = new LiveViewCheckpointTimelineEntry[affected.size()];
            for (int i = 0; i < affected.size(); i++) {
                final long[] e = affected.get(i);
                final LiveViewCheckpointTimelineEntry rep = new LiveViewCheckpointTimelineEntry();
                fill(rep, e[0], e[1], true); // rewrite payload to the "spliced" function
                reps[i] = rep;
                // Mirror the rewrite in the oracle.
                oracleInsert(e[0], e[1], true);
            }
            writer.splice(root, reps, reps.length, nextSegmentId++, tmpRoot);
            root.of(tmpRoot.getSegmentId(), tmpRoot.getOffset(), tmpRoot.getLength());
        }

        private void assertEntry(long[] expected, LiveViewCheckpointTimelineEntry actual) {
            assertRow(expected, snapshot(actual));
        }

        private void assertRow(long[] expected, long[] actual) {
            Assert.assertArrayEquals(
                    "entry (" + expected[0] + ", " + expected[1] + ") mismatch",
                    expected, actual
            );
        }

        private void fill(LiveViewCheckpointTimelineEntry e, long ts, long id, boolean spliced) {
            final long tag = spliced ? 1 : 0;
            e.of(ts, id, ts * 7 + id + tag * 3, ts + id * 3 + tag * 5, id * 11 + 5 + tag * 7);
            e.rootRef.of((spliced ? 2_000 : 1_000) + id, (ts & 0xFFFF) * 64L, 60);
        }

        private void oracleInsert(long ts, long id, boolean spliced) {
            final long[] row = new long[8];
            final long tag = spliced ? 1 : 0;
            row[0] = ts;
            row[1] = id;
            row[2] = ts * 7 + id + tag * 3;
            row[3] = ts + id * 3 + tag * 5;
            row[4] = id * 11 + 5 + tag * 7;
            row[5] = (spliced ? 2_000 : 1_000) + id;
            row[6] = (ts & 0xFFFF) * 64L;
            row[7] = 60;
            // Keep the list sorted by (ts, id); replace on exact key match (splice).
            int pos = 0;
            while (pos < oracle.size()) {
                final long[] o = oracle.get(pos);
                if (o[0] == ts && o[1] == id) {
                    oracle.set(pos, row);
                    return;
                }
                if (o[0] > ts || (o[0] == ts && o[1] > id)) {
                    break;
                }
                pos++;
            }
            oracle.add(pos, row);
        }

        private long[] oracleFind(long ts, long id) {
            for (int i = 0; i < oracle.size(); i++) {
                final long[] e = oracle.get(i);
                if (e[0] == ts && e[1] == id) {
                    return e;
                }
            }
            return null;
        }

        private long[] oraclePredecessor(long correctionTimestamp) {
            long[] best = null;
            for (int i = 0; i < oracle.size(); i++) {
                final long[] e = oracle.get(i);
                if (e[0] < correctionTimestamp) {
                    best = e;
                } else {
                    break;
                }
            }
            return best;
        }

        private long[] snapshot(LiveViewCheckpointTimelineEntry e) {
            return new long[]{
                    e.maxTimestamp,
                    e.checkpointId,
                    e.createdLvSeqTxn,
                    e.baseLvRowPosition,
                    e.logicalStateBytes,
                    e.rootRef.getSegmentId(),
                    e.rootRef.getOffset(),
                    e.rootRef.getLength()
            };
        }
    }
}
