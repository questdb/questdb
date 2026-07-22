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
import io.questdb.cairo.lv.LiveViewCheckpointGenerationPin;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointMetaStore;
import io.questdb.cairo.lv.LiveViewCheckpointPageRef;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineEntry;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineReader;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineWriter;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Acceptance measurements for the versioned checkpoint timeline: physical
 * growth, checkpoint write cost, predecessor-lookup latency, out-of-order repair
 * scan bounds, and refresh lag, each measured against the acceptance criteria
 * the design pins.
 * <p>
 * Every case measures the dimension over a timeline that keeps growing under it
 * and asserts the law the measurement follows, so a regression that reintroduces
 * a dependence on view age fails here rather than in a benchmark nobody runs.
 * The four dimensions that meet their criteria are:
 * <ul>
 *     <li><b>write cost per seal</b> - a cadence seal writes exactly one
 *     complete frame image, and that figure is byte-identical whether 40 or 160
 *     roots precede it;</li>
 *     <li><b>lookup latency</b> - the timeline tree's height, its copy-on-write
 *     append cost and the wall clock of a predecessor lookup all grow
 *     logarithmically, measured at 1, 1K and 20K logical entries;</li>
 *     <li><b>repair scan bounds</b> - a historical correction reads exactly the
 *     base rows inside its proven dependency interval and not one row more, the
 *     same count over a history three times longer;</li>
 *     <li><b>refresh lag</b> - repeated historical corrections leave the view
 *     caught up with the base after every round, and the work each round
 *     publishes is constant as the timeline grows past a hundred entries.</li>
 * </ul>
 * Two measured shortfalls are asserted rather than omitted, so the gap is a
 * failing expectation the day it closes rather than a forgotten one - see
 * {@link #testSteadyStateGrowthWritesOneFrameImagePerRoot}, which records both:
 * a root re-encodes the whole frame instead of sharing pages with its
 * predecessor, and publication metadata grows with the segment count because the
 * segment directory is one page rewritten per publication.
 * <p>
 * Wall clock is asserted only where the operation under test dominates it - the
 * lookup case, which runs against nothing else. A checkpoint seal's own elapsed
 * time is not: the tests pin the clock, which makes {@code
 * head_checkpoint_write_micros} read zero, and a real timer around a commit
 * measures the WAL and SQL machinery around the seal rather than the seal. Bytes
 * and pages written are the write-cost proxy here.
 */
public class LiveViewCheckpointAcceptanceTest extends AbstractLiveViewTest {

    // Commits per round of the refresh-lag case: twelve in-order groups and then one
    // historical correction, which is thirteen published generations per round.
    private static final int LAG_COMMITS_PER_ROUND = 12;
    private static final int LAG_GENERATIONS_PER_ROUND = LAG_COMMITS_PER_ROUND + 1;
    private static final int LAG_ROUNDS = 10;
    // Round by which every correction's dependency interval is full on both sides,
    // so the rows it reads stop growing with the history under it.
    private static final int LAG_STEADY_ROUND = 5;
    // Logical entry counts the lookup case measures over. 20K entries is three levels
    // of the production 64-way tree, so the height/cost curve has three points on it.
    private static final int[] LOOKUP_ENTRY_COUNTS = {1, 1_000, 20_000};
    private static final int LOOKUP_QUERIES = 10_000;
    private static final String RANGE_30S_FRAME =
            "PARTITION BY sym ORDER BY ts RANGE BETWEEN '30' SECOND PRECEDING AND CURRENT ROW";
    private static final String RANGE_60S_FRAME =
            "PARTITION BY sym ORDER BY ts RANGE BETWEEN '60' SECOND PRECEDING AND CURRENT ROW";
    // Seals of the steady-state growth case, sampled every SAMPLE commits.
    private static final int SAMPLE = 40;
    private static final int SEALS = 4 * SAMPLE;

    @After
    public void resetClock() {
        setCurrentMicros(-1);
    }

    @Before
    public void setUpCadence() {
        // One logical root per commit: the densest cadence the view can seal, so a
        // growth or lookup measurement sees the most roots per committed row.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        // Pin the clock below the (2026) data so a START FROM NOW view resolves its
        // lower bound under every row it will ever see, corrections included.
        setCurrentMicros(0);
    }

    @Test
    public void testHistoricalRepairReadsOnlyItsDependencyInterval() throws Exception {
        assertMemoryLeak(() -> {
            // Two identical views over two bases, one carrying three times the history
            // of the other. The correction below lands at the same coordinate in both,
            // so any difference in what they read is a dependence on view age - which
            // is exactly what the old retained ring had, and what the finite dependency
            // interval replaces.
            createBaseAndView("short_base", "lv_short", RANGE_30S_FRAME);
            createBaseAndView("long_base", "lv_long", RANGE_30S_FRAME);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int commit = 1; commit <= 120; commit++) {
                    if (commit <= 40) {
                        commit(job, "short_base", commit * 10, commit);
                    }
                    commit(job, "long_base", commit * 10, commit);
                }
                driveRefreshToQuiescence(job);

                // The control the equality below rests on: the long view really does
                // carry three times the history, so a scan proportional to view age -
                // the boundary rebuild the retained ring fell back to - could not
                // produce the same figure for both.
                assertQuery("SELECT count() FROM short_base").noLeakCheck().noRandomAccess().expectSize()
                        .returns("count\n80\n");
                assertQuery("SELECT count() FROM long_base").noLeakCheck().noRandomAccess().expectSize()
                        .returns("count\n240\n");

                final long shortScan = correctAndMeasureScan(job, "short_base", "lv_short");
                final long longScan = correctAndMeasureScan(job, "long_base", "lv_long");

                // The interval is [L, H] = [C - 30s, C + 30s] around the correction at
                // second 33: the in-order groups at 10, 20, 30, 40, 50 and 60 seconds
                // (two rows each, one per key) plus the correction's own two rows.
                Assert.assertEquals("a correction must read exactly its dependency interval", 14, shortScan);
                Assert.assertEquals(
                        "the same correction over three times the history must read the same rows",
                        shortScan,
                        longScan
                );

                assertViewMatchesRecompute("lv_short", "short_base", RANGE_30S_FRAME);
                assertViewMatchesRecompute("lv_long", "long_base", RANGE_30S_FRAME);
            }
        });
    }

    @Test
    public void testPredecessorLookupAndPublicationStayLogarithmic() throws Exception {
        assertMemoryLeak(() -> {
            long previousNanosPerLookup = 0;
            int previousHeight = 0;
            for (int i = 0; i < LOOKUP_ENTRY_COUNTS.length; i++) {
                final int entryCount = LOOKUP_ENTRY_COUNTS[i];
                try (Path dir = new Path()) {
                    timelineDir(dir, entryCount);
                    try (
                            LiveViewCheckpointTimelineWriter writer = new LiveViewCheckpointTimelineWriter(configuration);
                            LiveViewCheckpointTimelineReader reader = new LiveViewCheckpointTimelineReader(configuration)
                    ) {
                        writer.of(dir);
                        reader.of(dir);
                        final LiveViewCheckpointPageRef root = new LiveViewCheckpointPageRef();
                        final LiveViewCheckpointPageRef newRoot = new LiveViewCheckpointPageRef();
                        final LiveViewCheckpointTimelineEntry entry = new LiveViewCheckpointTimelineEntry();
                        int maxAppendPages = 0;
                        for (int id = 0; id < entryCount; id++) {
                            entry.of(id * 10L, id, id, id * 2L, 64);
                            entry.rootRef.of(1_000 + id, 64L * id, 48);
                            writer.append(root, entry, id, newRoot);
                            root.of(newRoot.getSegmentId(), newRoot.getOffset(), newRoot.getLength());
                            maxAppendPages = Math.max(maxAppendPages, writer.getLastSegmentPageCount());
                        }

                        final int height = height(reader, root);
                        Assert.assertEquals(entryCount, reader.size(root));
                        // A 64-way tree reaches 20K entries in three levels even at the
                        // half-full occupancy repeated splits leave behind.
                        Assert.assertTrue("height at " + entryCount + " entries: " + height, height <= 3);
                        Assert.assertTrue(
                                "height must not shrink as entries are added: " + height,
                                height >= previousHeight
                        );
                        // An append copies the search path and, at worst, one split per
                        // level plus a new root.
                        Assert.assertTrue(
                                "append copied " + maxAppendPages + " pages at " + entryCount + " entries",
                                maxAppendPages <= 2 * height + 1
                        );

                        // The oldest boundary stays addressable and is still the
                        // predecessor of every later correction timestamp, which is what
                        // makes an old out-of-order row a search rather than a fallback.
                        final LiveViewCheckpointTimelineEntry out = new LiveViewCheckpointTimelineEntry();
                        Assert.assertTrue(reader.findExact(root, 0, 0, out));
                        Assert.assertTrue(reader.predecessor(root, 5, out));
                        Assert.assertEquals(0, out.checkpointId);
                        if (entryCount > 1) {
                            Assert.assertTrue(reader.predecessor(root, (entryCount - 1) * 10L, out));
                            Assert.assertEquals(entryCount - 2, out.checkpointId);
                        }

                        final long start = System.nanoTime();
                        for (int q = 0; q < LOOKUP_QUERIES; q++) {
                            reader.predecessor(root, (q % entryCount) * 10L + 5, out);
                        }
                        final long nanosPerLookup = (System.nanoTime() - start) / LOOKUP_QUERIES;
                        if (i > 0) {
                            // A structure linear in entry count would cost at least the
                            // entry-count ratio more per lookup; the tree must stay far
                            // under that. The bound is deliberately loose - this is the
                            // one wall clock the suite asserts, and it only has to
                            // separate logarithmic from linear.
                            final long linearRatio = entryCount / LOOKUP_ENTRY_COUNTS[i - 1];
                            Assert.assertTrue(
                                    "lookup cost grew " + nanosPerLookup + "ns from " + previousNanosPerLookup
                                            + "ns for " + linearRatio + "x the entries",
                                    nanosPerLookup < previousNanosPerLookup * linearRatio
                            );
                        }
                        previousNanosPerLookup = nanosPerLookup;
                        previousHeight = height;
                    }
                }
            }
        });
    }

    @Test
    public void testRefreshLagAndRepairCostStayFlatAsTheTimelineGrows() throws Exception {
        assertMemoryLeak(() -> {
            createBaseAndView("base", "lv", RANGE_30S_FRAME);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                long previousGeneration = 0;
                long steadyScanRows = -1;
                for (int round = 1; round <= LAG_ROUNDS; round++) {
                    for (int i = 0; i < LAG_COMMITS_PER_ROUND; i++) {
                        commit(job, "base", 10 * ((round - 1) * LAG_COMMITS_PER_ROUND + i + 1), round * 100L + i);
                    }
                    driveRefreshToQuiescence(job);

                    final LiveViewInstance instance = viewInstance("lv");
                    final long scanRowsBefore = instance.getO3ReplayScanRows();
                    // Three seconds above the group this round's ordinal names: deep in
                    // history, below the durable frontier, and colliding with no in-order
                    // group.
                    commit(job, "base", 10 * (round - 1) + 3, 9_000L + round);
                    driveRefreshToQuiescence(job);
                    final long scanRows = instance.getO3ReplayScanRows() - scanRowsBefore;
                    Assert.assertTrue("round " + round + " must repair its correction", scanRows > 0);

                    // The view is caught up with the base after the correction, however
                    // many roots the timeline now holds.
                    Assert.assertEquals("round " + round + " left the view lagging", 0, lagSeqTxn(instance));

                    final long generation = generation(instance);
                    if (round > 1) {
                        Assert.assertEquals(
                                "each round publishes the same work: " + LAG_COMMITS_PER_ROUND
                                        + " seals and one repair",
                                LAG_GENERATIONS_PER_ROUND,
                                generation - previousGeneration
                        );
                    }
                    // The first rounds have less history below their correction than its
                    // dependency interval spans, so the scan grows until the interval is
                    // full and is flat from there: it follows the interval, not the age.
                    if (round == LAG_STEADY_ROUND) {
                        steadyScanRows = scanRows;
                    } else if (round > LAG_STEADY_ROUND) {
                        Assert.assertEquals(
                                "round " + round + " scanned a different interval than round " + LAG_STEADY_ROUND,
                                steadyScanRows,
                                scanRows
                        );
                    }
                    previousGeneration = generation;
                }

                final LiveViewInstance instance = viewInstance("lv");
                // One entry per in-order seal: every round's correction converged, so its
                // publication re-versioned the interval it touched instead of appending a
                // boundary of its own.
                Assert.assertEquals(
                        "the corrections must have run over a timeline of this many entries",
                        LAG_ROUNDS * LAG_COMMITS_PER_ROUND,
                        timelineEntries(instance)
                );
                assertViewMatchesRecompute("lv", "base", RANGE_30S_FRAME);
            }
        });
    }

    @Test
    public void testSteadyStateGrowthWritesOneFrameImagePerRoot() throws Exception {
        assertMemoryLeak(() -> {
            // Rows every five seconds under a one-minute look-behind, so a frame holds
            // thirteen rows per key and two keys are live throughout: the steady-state
            // bounded window the storage claim is about.
            createBaseAndView("base", "lv", RANGE_60S_FRAME);
            final long[] dataBytes = new long[SEALS / SAMPLE + 1];
            final long[] metadataBytes = new long[SEALS / SAMPLE + 1];
            long logicalStateBytes = 0;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int commit = 1; commit <= SEALS; commit++) {
                    commit(job, "base", commit * 5, commit);
                    if (commit % SAMPLE == 0) {
                        driveRefreshToQuiescence(job);
                        final LiveViewInstance instance = viewInstance("lv");
                        final int sample = commit / SAMPLE;
                        try (LiveViewCheckpointMetaStore store = openStore(instance)) {
                            dataBytes[sample] = store.getSuperblock().dataBytes;
                            metadataBytes[sample] = store.getSuperblock().metadataBytes;
                        }
                        logicalStateBytes = instance.getHeadCheckpointStateBytes();
                    }
                }

                final LiveViewInstance instance = viewInstance("lv");
                Assert.assertEquals("one cadence seal per commit", SEALS, timelineEntries(instance));
                assertViewMatchesRecompute("lv", "base", RANGE_60S_FRAME);

                // Write cost per seal: byte-identical across the last two windows, so a
                // seal costs the live frame and nothing per root that precedes it.
                final long midDataPerSeal = (dataBytes[3] - dataBytes[2]) / SAMPLE;
                final long lateDataPerSeal = (dataBytes[4] - dataBytes[3]) / SAMPLE;
                Assert.assertEquals(
                        "a seal's data bytes must not depend on how many roots precede it",
                        midDataPerSeal,
                        lateDataPerSeal
                );
                Assert.assertTrue(
                        "a seal wrote " + lateDataPerSeal + " bytes over a " + logicalStateBytes + " byte frame",
                        lateDataPerSeal <= logicalStateBytes
                );

                // Recorded shortfall 1: a root re-encodes its whole frame rather than
                // sharing pages with the root before it, so the total is one complete
                // image per root - roots times frame - where the design's storage claim
                // is unique captured rows plus descriptors. The persistent-chunk layer
                // exists but the seal path freezes every partition's complete state
                // instead of going through it. Asserted rather than omitted, so closing
                // the gap fails here and gets recorded instead of passing unnoticed.
                Assert.assertTrue(
                        "a root writes the whole frame: " + lateDataPerSeal + " bytes per seal over a "
                                + logicalStateBytes + " byte frame",
                        10 * lateDataPerSeal >= 9 * logicalStateBytes
                );
                Assert.assertTrue(
                        "the timeline holds one complete image per root: " + dataBytes[4] + " bytes over "
                                + SEALS + " roots",
                        10 * dataBytes[4] >= 9 * SEALS * lateDataPerSeal
                );

                // Recorded shortfall 2: publication metadata grows with the number of
                // live segments, because the segment directory is a single page rewritten
                // in full on every publication while the timeline tree beside it copies
                // only its search path. Total metadata is therefore super-linear in
                // checkpoint count. Same reasoning as above: asserted, not omitted.
                final long earlyMetadataPerSeal = metadataBytes[1] / SAMPLE;
                final long lateMetadataPerSeal = (metadataBytes[4] - metadataBytes[3]) / SAMPLE;
                Assert.assertTrue(
                        "publication metadata per seal: " + earlyMetadataPerSeal + " bytes over the first "
                                + SAMPLE + " roots, " + lateMetadataPerSeal + " bytes over the last " + SAMPLE,
                        lateMetadataPerSeal >= 2 * earlyMetadataPerSeal
                );
            }
        });
    }

    private static Path checkpointsDir(LiveViewInstance instance) {
        return new Path().of(configuration.getDbRoot())
                .concat(instance.getLiveViewToken())
                .concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
    }

    /**
     * Number of metadata nodes a root-to-leaf descent visits, which is what a
     * predecessor or point lookup reads. The tree is balanced, so the leftmost
     * path measures every path.
     */
    private static int height(LiveViewCheckpointTimelineReader reader, LiveViewCheckpointPageRef root) {
        final LiveViewCheckpointPageRef node = new LiveViewCheckpointPageRef();
        final LiveViewCheckpointPageRef child = new LiveViewCheckpointPageRef();
        node.of(root.getSegmentId(), root.getOffset(), root.getLength());
        int height = 1;
        while (reader.rootChildCount(node) > 0) {
            reader.rootChildRef(node, 0, child);
            node.of(child.getSegmentId(), child.getOffset(), child.getLength());
            height++;
        }
        return height;
    }

    private static String timestamp(int secondOfDay) {
        return String.format(
                "2026-01-01T%02d:%02d:%02d.000000Z",
                secondOfDay / 3600,
                (secondOfDay % 3600) / 60,
                secondOfDay % 60
        );
    }

    private static String viewSql(String baseName, String windowFrame) {
        return "SELECT ts, sym, sum(x) OVER (" + windowFrame + ") AS s FROM " + baseName;
    }

    private void assertViewMatchesRecompute(String viewName, String baseName, String windowFrame) throws Exception {
        // A refresh fault self-heals into exactly this recompute, so the fault count
        // beside it is what says the view converged through the incremental and repair
        // paths rather than through a rebuild that would also have thrown away the
        // timeline every measurement above was taken over.
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(" + viewSql(baseName, windowFrame) + ") ORDER BY 2, 1",
                "(" + viewName + ") ORDER BY 2, 1",
                LOG,
                true
        );
        assertNoRefreshFaults(viewName);
    }

    // Commits one (sym, ts) group of two rows and gives the refresh job a turn on it.
    // The clock steps past the view's flush window first, so the group reaches disk
    // before the next commit rather than lingering as an unflushed lead.
    private void commit(LiveViewRefreshJob job, String baseName, int second, long value) throws Exception {
        setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
        final String rowTs = timestamp(second);
        execute("INSERT INTO " + baseName + " (ts, sym, x) VALUES "
                + "('" + rowTs + "', 'a', " + value + "), "
                + "('" + rowTs + "', 'b', " + (value + 1) + ")");
        drainWalQueue();
        drainJob(job);
        drainWalQueue();
    }

    /**
     * Commits one out-of-order row deep in the view's history and returns the base
     * rows its repair read. Fails if the row was appended rather than repaired,
     * which would leave the returned figure measuring ordinary cadence.
     */
    private long correctAndMeasureScan(LiveViewRefreshJob job, String baseName, String viewName) throws Exception {
        final LiveViewInstance instance = viewInstance(viewName);
        final long scanRowsBefore = instance.getO3ReplayScanRows();
        final long replayRowsBefore = instance.getO3BoundaryReplayRows() + instance.getO3ResumeReplayRows();
        commit(job, baseName, 33, 9_999);
        driveRefreshToQuiescence(job);
        Assert.assertTrue(
                "the row must be repaired rather than appended",
                instance.getO3BoundaryReplayRows() + instance.getO3ResumeReplayRows() > replayRowsBefore
        );
        return instance.getO3ReplayScanRows() - scanRowsBefore;
    }

    private void createBaseAndView(String baseName, String viewName, String windowFrame) throws Exception {
        execute("CREATE TABLE " + baseName + " (ts TIMESTAMP, sym SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("CREATE LIVE VIEW " + viewName + " FLUSH EVERY 100ms START FROM NOW AS "
                + viewSql(baseName, windowFrame));
    }

    private long generation(LiveViewInstance instance) {
        try (LiveViewCheckpointMetaStore store = openStore(instance)) {
            return store.getSuperblock().generation;
        }
    }

    private long lagSeqTxn(LiveViewInstance instance) {
        return engine.getTableSequencerAPI()
                .getTxnTracker(instance.getDefinition().getBaseTableToken())
                .getWriterTxn() - instance.getLastProcessedSeqTxn();
    }

    private LiveViewCheckpointMetaStore openStore(LiveViewInstance instance) {
        final LiveViewCheckpointMetaStore store = new LiveViewCheckpointMetaStore(configuration);
        try (Path dir = checkpointsDir(instance)) {
            store.of(dir);
        }
        return store;
    }

    private long timelineEntries(LiveViewInstance instance) {
        try (
                LiveViewCheckpointMetaStore store = openStore(instance);
                LiveViewCheckpointGenerationPin pin = store.pin();
                LiveViewCheckpointTimelineReader reader = new LiveViewCheckpointTimelineReader(configuration)
        ) {
            try (Path dir = checkpointsDir(instance)) {
                reader.of(dir);
            }
            return reader.size(pin.getTimelineRootRef());
        }
    }

    /**
     * Points {@code dir} at a scratch {@code _checkpoints} directory for a synthetic
     * timeline of {@code entryCount} entries, creating its metadata subdirectory.
     */
    private void timelineDir(Path dir, int entryCount) {
        dir.of(configuration.getDbRoot()).concat("lv_lookup_" + entryCount).concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
        try (Path meta = new Path()) {
            meta.of(dir).concat(LiveViewCheckpointLayout.META_DIR_NAME).slash();
            configuration.getFilesFacade().mkdirs(meta, configuration.getMkDirMode());
        }
    }

    private LiveViewInstance viewInstance(String viewName) {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance(viewName);
        Assert.assertNotNull("live view '" + viewName + "' must be registered", instance);
        return instance;
    }
}
