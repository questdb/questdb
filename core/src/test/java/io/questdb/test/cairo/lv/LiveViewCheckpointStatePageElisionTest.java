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

import io.questdb.PropertyKey;
import io.questdb.cairo.lv.LiveViewCheckpointFunctionDirectory;
import io.questdb.cairo.lv.LiveViewCheckpointFunctionRoot;
import io.questdb.cairo.lv.LiveViewCheckpointGenerationPin;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointMetaStore;
import io.questdb.cairo.lv.LiveViewCheckpointPageRef;
import io.questdb.cairo.lv.LiveViewCheckpointPartitionMapReader;
import io.questdb.cairo.lv.LiveViewCheckpointRoot;
import io.questdb.cairo.lv.LiveViewCheckpointStatePageRef;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineReader;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.std.Chars;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Coverage for whole-state page elision: a partition whose encoded state has not
 * moved since the boundary below reuses that boundary's page instead of writing
 * a byte-identical copy.
 * <p>
 * The growth this closes is the seal-rate multiplier on cold keys. A seal freezes
 * every live key, not the keys the batch touched, so one row into one key used to
 * cost a fresh state page per key in {@code data/} plus a full partition-map
 * rewrite in {@code meta/} - and at a five-minute cadence that is 288 whole-state
 * images a day for a view whose key set never changes. Reusing the page makes the
 * map entry byte-identical too, which is what
 * {@code LiveViewCheckpointPartitionMapWriter}'s existing equal-put elision needs
 * to drop the put and leave the leaf and its ancestors alone.
 * <p>
 * The view is a bounded ROWS {@code sum}: whole-state per key, so no ring sharing
 * confuses the measurement, and finite in both directions, so an out-of-order
 * correction takes the localized repair whose capture the elision also has to
 * reach. Every case pairs its structural assertion with the from-base recompute
 * oracle at a zero fault count, so a reused page that did not in fact hold the
 * right bytes surfaces as a diff rather than as a saving.
 */
public class LiveViewCheckpointStatePageElisionTest extends AbstractLiveViewTest {

    // The one key the trickle feeds. Every other key is written once and then left
    // cold for the rest of the run.
    private static final String HOT_KEY = "k00";
    // Wide enough that a per-seal whole-map rewrite is unmistakable against a
    // single touched key.
    private static final int KEYS = 24;
    // In-order seals the repair case folds its correction back into.
    private static final int REPAIR_HISTORY_SEALS = 10;
    private static final int TRICKLE_SEALS = 6;
    private static final String VIEW_SQL = "SELECT ts, sym, sum(x) OVER (" +
            "PARTITION BY sym ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW" +
            ") AS s FROM base";

    @After
    public void resetClock() {
        setCurrentMicros(-1);
    }

    @Before
    public void setUpCadence() {
        // One logical boundary per commit, so a seal is exactly a commit and the
        // per-seal cost the case measures is not averaged over a cadence window.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setCurrentMicros(0);
    }

    @Test
    public void testColdPartitionsReuseTheirStatePagesAcrossSeals() throws Exception {
        assertMemoryLeak(() -> {
            createView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                commitEveryKey(job, 10);
                final LiveViewInstance instance = viewInstance();
                final long dataBytesAfterFirstSeal = dataDirBytes(instance);

                for (int seal = 1; seal <= TRICKLE_SEALS; seal++) {
                    commitHotKey(job, 10 + seal * 10, seal);
                }
                driveRefreshToQuiescence(job);
                assertViewMatchesRecompute();

                final List<Boundary> boundaries = boundaryPages(instance);
                Assert.assertEquals(
                        "one boundary per commit at this cadence",
                        TRICKLE_SEALS + 1,
                        boundaries.size()
                );
                for (int i = 0; i < boundaries.size(); i++) {
                    Assert.assertEquals(
                            "every key stays live for the whole run",
                            KEYS,
                            boundaries.get(i).pages.size()
                    );
                }

                // The point of the case: a seal writes the keys the batch moved, not
                // the keys the view holds. Each trickle commit feeds one key, so
                // exactly one of the KEYS entries may name a different page than the
                // boundary below it.
                for (int i = 1; i < boundaries.size(); i++) {
                    Assert.assertEquals(
                            "seal " + i + " must re-image only the key it touched",
                            1,
                            changedPageCount(boundaries.get(i - 1).pages, boundaries.get(i).pages)
                    );
                }

                // Stated as a total: KEYS pages for the first seal, one more per
                // trickle seal. Without elision it would be KEYS per seal.
                final Set<Page> distinct = new HashSet<>();
                for (Boundary boundary : boundaries) {
                    distinct.addAll(boundary.pages.values());
                }
                Assert.assertEquals(
                        "the run must write one page per key plus one per touched key",
                        KEYS + TRICKLE_SEALS,
                        distinct.size()
                );

                // A data segment carries payload bytes only, so what the trickle added
                // to data/ is exactly the pages it wrote - and nothing was written for
                // the keys it left alone.
                long trickleStateBytes = 0;
                for (Page page : distinct) {
                    if (!boundaries.get(0).pages.containsValue(page)) {
                        trickleStateBytes += page.storedLength;
                    }
                }
                Assert.assertEquals(
                        "data/ must grow by the touched key's pages and nothing else",
                        trickleStateBytes,
                        dataDirBytes(instance) - dataBytesAfterFirstSeal
                );
            }
        });
    }

    @Test
    public void testElidedStateRestartsFromTheSegmentThatHoldsIt() throws Exception {
        assertMemoryLeak(() -> {
            createView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                commitEveryKey(job, 10);
                for (int seal = 1; seal <= TRICKLE_SEALS; seal++) {
                    commitHotKey(job, 10 + seal * 10, seal);
                }
                driveRefreshToQuiescence(job);
                assertViewMatchesRecompute();

                // The head boundary names the first seal's segment for every cold key,
                // so the restore has to reach back past every segment written since.
                final List<Boundary> boundaries = boundaryPages(viewInstance());
                final Set<Long> headSegments = new HashSet<>();
                for (Page page : boundaries.get(boundaries.size() - 1).pages.values()) {
                    headSegments.add(page.segmentId);
                }
                Assert.assertEquals(
                        "the head must restore from the cold keys' original segment and the hot key's newest one",
                        2,
                        headSegments.size()
                );
            }

            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
                Assert.assertTrue(
                        "the view must restore its accumulators from the checkpoint timeline",
                        viewInstance().isCheckpointRestoreSucceeded()
                );
                driveRefreshToQuiescence(job);
                assertViewMatchesRecompute();
                // Keep ingesting after the restore: a state page restored from an old
                // segment has to carry the frame forward, not merely read back.
                for (int seal = TRICKLE_SEALS + 1; seal <= TRICKLE_SEALS + 3; seal++) {
                    commitHotKey(job, 10 + seal * 10, seal);
                }
                commitEveryKey(job, 10 + (TRICKLE_SEALS + 4) * 10);
                driveRefreshToQuiescence(job);
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testRepairCaptureSharesColdPartitionsAcrossBoundaries() throws Exception {
        assertMemoryLeak(() -> {
            createView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // Two commits over the whole key set, then a trickle into one key, so
                // the range a correction replays holds rows for every key at its floor
                // and rows for one key above it.
                commitEveryKey(job, 10);
                commitEveryKey(job, 20);
                for (int seal = 1; seal <= REPAIR_HISTORY_SEALS; seal++) {
                    commitHotKey(job, 20 + seal * 10, seal);
                }
                driveRefreshToQuiescence(job);
                assertViewMatchesRecompute();

                final LiveViewInstance instance = viewInstance();
                final Set<Page> before = allPages(boundaryPages(instance));
                final long repairedBefore = repairedRows(instance);

                // One out-of-order row inside the ROWS frame's look-behind, so the
                // repair replays a range that crosses several boundaries and freezes
                // each of them into one capture segment.
                commitHotKey(job, 25, 9_000);
                driveRefreshToQuiescence(job);
                Assert.assertTrue(
                        "the correction must be repaired rather than appended",
                        repairedRows(instance) > repairedBefore
                );
                assertViewMatchesRecompute();

                // The capture shares against the boundary it froze immediately before,
                // reading it out of its own still-unpublished segment. So a key the
                // replay carried but did not touch again costs the capture one page
                // however many boundaries it re-versions above it.
                final Map<Page, Set<String>> boundariesByCapturedPage = new HashMap<>();
                for (Boundary boundary : boundaryPages(instance)) {
                    for (Page page : boundary.pages.values()) {
                        if (!before.contains(page)) {
                            boundariesByCapturedPage
                                    .computeIfAbsent(page, ignore -> new HashSet<>())
                                    .add(boundary.key());
                        }
                    }
                }
                Assert.assertFalse(
                        "the repair must have written pages of its own",
                        boundariesByCapturedPage.isEmpty()
                );
                int mostSharedBy = 0;
                for (Set<String> boundaries : boundariesByCapturedPage.values()) {
                    mostSharedBy = Math.max(mostSharedBy, boundaries.size());
                }
                Assert.assertTrue(
                        "a captured page must be named by more than one re-versioned boundary, was " + mostSharedBy,
                        mostSharedBy > 1
                );
            }
        });
    }

    @Test
    public void testRingFunctionIsUnaffected() throws Exception {
        assertMemoryLeak(() -> {
            // The control. A RANGE frame is ring-shaped, so the freeze takes the
            // branch this change does not touch: the ring's entry payload carries an
            // advancing row count and is never byte-identical to the one below it,
            // and chunk sharing rather than page reuse is what keeps its seals cheap.
            // The same workload has to seal, restore and match the recompute exactly
            // as it did before.
            final String ringWindow = "sum(x) OVER (PARTITION BY sym ORDER BY ts "
                    + "RANGE BETWEEN '30' SECOND PRECEDING AND CURRENT ROW)";
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS "
                    + "SELECT ts, sym, " + ringWindow + " AS s FROM base");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                commitEveryKey(job, 10);
                for (int seal = 1; seal <= TRICKLE_SEALS; seal++) {
                    commitHotKey(job, 10 + seal * 10, seal);
                }
                driveRefreshToQuiescence(job);
                assertRingViewMatchesRecompute(ringWindow);
            }

            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
                Assert.assertTrue(
                        "the ring must restore from the checkpoint timeline",
                        viewInstance().isCheckpointRestoreSucceeded()
                );
                commitEveryKey(job, 10 + (TRICKLE_SEALS + 1) * 10);
                driveRefreshToQuiescence(job);
                assertRingViewMatchesRecompute(ringWindow);
            }
        });
    }

    // Every state page the timeline names, whatever boundary or key names it.
    private static Set<Page> allPages(List<Boundary> boundaries) {
        final Set<Page> out = new HashSet<>();
        for (Boundary boundary : boundaries) {
            out.addAll(boundary.pages.values());
        }
        return out;
    }

    private static Path checkpointsDir(LiveViewInstance instance) {
        return new Path().of(configuration.getDbRoot())
                .concat(instance.getLiveViewToken())
                .concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
    }

    // Entries naming a different page than the boundary below named for the same key.
    private static int changedPageCount(Map<String, Page> previous, Map<String, Page> current) {
        int changed = 0;
        for (Map.Entry<String, Page> entry : current.entrySet()) {
            if (!entry.getValue().equals(previous.get(entry.getKey()))) {
                changed++;
            }
        }
        return changed;
    }

    // Total bytes of the published data segments. A segment carries payload bytes
    // only, so this is exactly the state the view has written.
    private static long dataDirBytes(LiveViewInstance instance) {
        long bytes = 0;
        try (Path checkpointsDir = checkpointsDir(instance); Path dataDir = new Path()) {
            LiveViewCheckpointLayout.dataDirPath(dataDir, checkpointsDir);
            final File[] files = new File(dataDir.toString()).listFiles();
            if (files != null) {
                for (File file : files) {
                    if (Chars.startsWith(file.getName(), LiveViewCheckpointLayout.DATA_SEGMENT_PREFIX)
                            && !file.getName().endsWith(LiveViewCheckpointLayout.TMP_SUFFIX)) {
                        bytes += file.length();
                    }
                }
            }
        }
        return bytes;
    }

    private static String hex(byte[] key) {
        final StringBuilder sb = new StringBuilder(key.length * 2);
        for (byte b : key) {
            sb.append(Character.forDigit((b >> 4) & 0xf, 16)).append(Character.forDigit(b & 0xf, 16));
        }
        return sb.toString();
    }

    private static String timestamp(int secondOfDay) {
        return String.format(
                "2026-01-01T%02d:%02d:%02d.000000Z",
                secondOfDay / 3600,
                (secondOfDay % 3600) / 60,
                secondOfDay % 60
        );
    }

    private void assertRingViewMatchesRecompute(String ringWindow) throws Exception {
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(SELECT ts, sym, " + ringWindow + " AS s FROM base) ORDER BY 2, 1",
                "(lv) ORDER BY 2, 1",
                LOG,
                true
        );
        assertNoRefreshFaults("lv");
    }

    private void assertViewMatchesRecompute() throws Exception {
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(" + VIEW_SQL + ") ORDER BY 2, 1",
                "(lv) ORDER BY 2, 1",
                LOG,
                true
        );
        assertNoRefreshFaults("lv");
    }

    /**
     * One map per logical boundary, ascending, of encoded partition key to the
     * state page the boundary's function root names for it.
     */
    private List<Boundary> boundaryPages(LiveViewInstance instance) {
        final List<Boundary> out = new ArrayList<>();
        try (
                Path dir = checkpointsDir(instance);
                LiveViewCheckpointMetaStore metaStore = new LiveViewCheckpointMetaStore(configuration)
        ) {
            metaStore.of(dir);
            try (
                    LiveViewCheckpointGenerationPin pin = metaStore.pin();
                    LiveViewCheckpointTimelineReader timeline = new LiveViewCheckpointTimelineReader(configuration);
                    LiveViewCheckpointRoot root = new LiveViewCheckpointRoot(configuration);
                    LiveViewCheckpointFunctionDirectory functions = new LiveViewCheckpointFunctionDirectory(configuration);
                    LiveViewCheckpointFunctionRoot functionRoot = new LiveViewCheckpointFunctionRoot(configuration);
                    LiveViewCheckpointPartitionMapReader partitions = new LiveViewCheckpointPartitionMapReader(configuration)
            ) {
                timeline.of(dir);
                partitions.of(dir);
                final LiveViewCheckpointPageRef functionDirectoryRef = new LiveViewCheckpointPageRef();
                final LiveViewCheckpointPageRef functionRootRef = new LiveViewCheckpointPageRef();
                final LiveViewCheckpointPageRef partitionMapRoot = new LiveViewCheckpointPageRef();
                timeline.iterateAll(pin.getTimelineRootRef(), entry -> {
                    final Map<String, Page> pages = new HashMap<>();
                    root.of(dir, entry.rootRef);
                    root.getFunctionDirectoryRef(functionDirectoryRef);
                    functions.of(dir, functionDirectoryRef);
                    Assert.assertEquals("the view declares exactly one window function", 1, functions.size());
                    functions.getRootRef(0, functionRootRef);
                    functionRoot.of(dir, functionRootRef);
                    functionRoot.getPartitionMapRootRef(partitionMapRoot);
                    partitions.iterateAll(partitionMapRoot, partition -> {
                        Assert.assertEquals(
                                "a whole-state entry holds exactly one page",
                                1,
                                partition.getStatePageCount()
                        );
                        pages.put(hex(partition.getKey()), new Page(partition.getStatePageRef(0)));
                    });
                    out.add(new Boundary(entry.maxTimestamp, entry.checkpointId, pages));
                });
            }
        }
        return out;
    }

    // One row for every key, at one designated timestamp, plus a refresh turn.
    private void commitEveryKey(LiveViewRefreshJob job, int second) throws Exception {
        setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
        final StringBuilder sql = new StringBuilder("INSERT INTO base (ts, sym, x) VALUES ");
        final String rowTs = timestamp(second);
        for (int k = 0; k < KEYS; k++) {
            if (k > 0) {
                sql.append(", ");
            }
            sql.append("('").append(rowTs).append("', '").append(key(k)).append("', ").append(k + 1).append(')');
        }
        execute(sql.toString());
        drainWalQueue();
        drainJob(job);
        drainWalQueue();
    }

    // One row for the single hot key, plus a refresh turn. This is the trickle the
    // whole case is about: a seal the cadence owes to one key out of KEYS.
    private void commitHotKey(LiveViewRefreshJob job, int second, long x) throws Exception {
        setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
        execute("INSERT INTO base (ts, sym, x) VALUES ('" + timestamp(second) + "', '" + HOT_KEY + "', " + x + ")");
        drainWalQueue();
        drainJob(job);
        drainWalQueue();
    }

    private void createView() throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " + VIEW_SQL);
    }

    private String key(int index) {
        return String.format("k%02d", index);
    }

    private long repairedRows(LiveViewInstance instance) {
        return instance.getO3BoundaryReplayRows() + instance.getO3ResumeReplayRows();
    }

    private void restartCycle() {
        engine.getLiveViewRegistry().clear();
        engine.buildViewGraphs();
    }

    private LiveViewInstance viewInstance() {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull("live view 'lv' must be registered", instance);
        return instance;
    }

    /**
     * One logical boundary: its timeline identity plus the state page its
     * function root names for every live partition key.
     */
    private static final class Boundary {
        private final long checkpointId;
        private final long maxTimestamp;
        private final Map<String, Page> pages;

        private Boundary(long maxTimestamp, long checkpointId, Map<String, Page> pages) {
            this.maxTimestamp = maxTimestamp;
            this.checkpointId = checkpointId;
            this.pages = pages;
        }

        // A repair re-versions a boundary in place and can insert one of its own, so
        // the composite key rather than the position identifies it across a repair.
        private String key() {
            return maxTimestamp + ":" + checkpointId;
        }
    }

    /**
     * One state page reference, compared by identity of the bytes it names rather
     * than by the flyweight the reader hands out.
     */
    private static final class Page {
        private final long offset;
        private final long segmentId;
        private final int storedLength;

        private Page(LiveViewCheckpointStatePageRef ref) {
            this.segmentId = ref.getSegmentId();
            this.offset = ref.getOffset();
            this.storedLength = ref.getStoredLength();
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Page)) {
                return false;
            }
            final Page that = (Page) other;
            return segmentId == that.segmentId && offset == that.offset && storedLength == that.storedLength;
        }

        @Override
        public int hashCode() {
            return (int) (segmentId * 31 + offset);
        }

        @Override
        public String toString() {
            return segmentId + ":" + offset + ":" + storedLength;
        }
    }
}
