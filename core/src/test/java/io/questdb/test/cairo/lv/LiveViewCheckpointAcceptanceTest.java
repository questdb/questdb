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
import io.questdb.cairo.lv.LiveViewCheckpointPageCache;
import io.questdb.cairo.lv.LiveViewCheckpointPageCacheBudget;
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
 * The five dimensions that meet their criteria are:
 * <ul>
 *     <li><b>write cost per seal</b> - a cadence seal writes one frame image,
 *     packed by the semantic codecs to a fraction of the raw frame, and that
 *     figure is flat whether 40 or 160 roots precede it;</li>
 *     <li><b>structural sharing</b> - a frame wide enough for a chunk descriptor
 *     to pay for itself carries the previous root's chunk pages forward by
 *     reference and writes only the rows the batch appended;</li>
 *     <li><b>lookup latency</b> - the timeline tree's height, its copy-on-write
 *     append cost and the wall clock of a predecessor lookup all grow
 *     logarithmically, measured at 1, 1K and 20K logical entries;</li>
 *     <li><b>repair scan bounds</b> - a historical correction reads exactly the
 *     base rows inside its proven dependency interval and not one row more, the
 *     same count over a history three times longer;</li>
 *     <li><b>refresh lag</b> - repeated historical corrections leave the view
 *     caught up with the base after every round, and the work each round
 *     publishes is constant as the timeline grows past a hundred entries;</li>
 *     <li><b>publication metadata</b> - a seal's metadata cost is flat across the
 *     run, because every structure it touches copies only its search path.</li>
 * </ul>
 * <p>
 * Wall clock is asserted only where the operation under test dominates it - the
 * lookup case, which runs against nothing else. A checkpoint seal's own elapsed
 * time is not: the tests pin the clock, which makes {@code
 * checkpoint_last_write_micros} read zero, and a real timer around a commit
 * measures the WAL and SQL machinery around the seal rather than the seal. Bytes
 * and pages written are the write-cost proxy here.
 */
public class LiveViewCheckpointAcceptanceTest extends AbstractLiveViewTest {

    // Shape of the page cache differential. Every view carries CACHE_KEYS window
    // partitions, so one anchor restore reads a page pair per key and ten anchors
    // put a few hundred distinct pages in front of the admission hash.
    private static final int CACHE_COMMITS_PER_ROUND = 4;
    private static final int CACHE_KEYS = 8;
    private static final int CACHE_ROUNDS = 10;
    private static final int CACHE_VIEWS = 4;
    // Fraction of pages, by identity hash, each view admits. The fraction is what
    // an engine-wide cap resolves to per view, and it is settable where the cap is
    // fixed for the engine's life, so the differential drives it directly. The
    // fourth view is left alone for the self-tuner to size, which is the fraction a
    // deployment actually runs on.
    private static final double CACHE_SELF_TUNED = -1;
    private static final double[] CACHE_ADMISSION_FRACTIONS = {0, 0.5, 1, CACHE_SELF_TUNED};
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
    // Rows per key one commit of the dense-frame case adds, at one-second spacing.
    // Comfortably above LiveViewCheckpointRingSeal.MIN_SHARED_CHUNK_ROWS, so the
    // chunk each seal writes carries enough rows to be worth referencing later.
    private static final int DENSE_ROWS_PER_COMMIT = 200;
    private static final int DENSE_SAMPLE = 40;
    private static final int DENSE_SAMPLES = 3;
    // Twenty commits fill this frame, so twenty chunks per key live inside it.
    private static final String RANGE_DENSE_FRAME =
            "PARTITION BY sym ORDER BY ts RANGE BETWEEN '4000' SECOND PRECEDING AND CURRENT ROW";
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
    public void testViewContentIsIdenticalWhateverThePageCacheHolds() throws Exception {
        assertMemoryLeak(() -> {
            // Four identical views over four identical bases, refreshed through the
            // same commits, differing only in how much of their decoded checkpoint
            // state the page cache is allowed to keep: nothing, a hash-selected
            // half, everything, and whatever the self-tuner sizes the fourth to. A
            // cache that ever answered with the wrong page would show up here as one
            // view disagreeing with the others - and with the recompute all four are
            // measured against.
            for (int i = 0; i < CACHE_VIEWS; i++) {
                createBaseAndView(cacheBase(i), cacheView(i), RANGE_30S_FRAME);
            }
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                pinAdmissionFractions();
                int second = 0;
                for (int round = 1; round <= CACHE_ROUNDS; round++) {
                    for (int i = 0; i < CACHE_COMMITS_PER_ROUND; i++) {
                        second += 10;
                        commitEveryKey(job, second, round * 100L + i);
                        pinAdmissionFractions();
                    }
                    driveRefreshToQuiescence(job);
                    // Two corrections just below the head. Both resume from the
                    // newest boundary below them - the same root - so the second
                    // restore meets the pages the first one decoded, which is the
                    // repeat under sustained out-of-order ingestion the cache is
                    // for.
                    commitEveryKey(job, second - 5, 9_000L + round);
                    pinAdmissionFractions();
                    commitEveryKey(job, second - 3, 9_500L + round);
                    pinAdmissionFractions();
                    driveRefreshToQuiescence(job);
                }

                for (int i = 0; i < CACHE_VIEWS; i++) {
                    final LiveViewInstance instance = viewInstance(cacheView(i));
                    Assert.assertTrue(
                            cacheView(i) + " never resumed from an anchor checkpoint, so it never"
                                    + " restored one and the differential below proves nothing",
                            instance.getO3ResumeReplayRows() > 0
                    );
                    assertViewMatchesRecompute(cacheView(i), cacheBase(i), RANGE_30S_FRAME);
                }
                // And against each other, not only against their own recompute.
                for (int i = 1; i < CACHE_VIEWS; i++) {
                    TestUtils.assertSqlCursors(
                            engine,
                            sqlExecutionContext,
                            "(" + cacheView(0) + ") ORDER BY 2, 1",
                            "(" + cacheView(i) + ") ORDER BY 2, 1",
                            LOG,
                            true
                    );
                }

                final LiveViewCheckpointPageCache cold = pageCache(cacheView(0));
                final LiveViewCheckpointPageCache half = pageCache(cacheView(1));
                final LiveViewCheckpointPageCache warm = pageCache(cacheView(2));
                final LiveViewCheckpointPageCache tuned = pageCache(cacheView(3));
                // The four views really were served differently, so the equality
                // above is a differential rather than four runs of one path.
                Assert.assertTrue("no view restored a ring page", cold.getMisses() > 0);
                // Identical data through identical turns, so the four views probed
                // for exactly the same pages and differ only in what came back.
                Assert.assertEquals(cold.getMisses(), half.getHits() + half.getMisses());
                Assert.assertEquals(cold.getMisses(), warm.getHits() + warm.getMisses());
                Assert.assertEquals(cold.getMisses(), tuned.getHits() + tuned.getMisses());
                Assert.assertEquals("a view admitting nothing must serve nothing", 0, cold.getHits());
                Assert.assertEquals(0, cold.getPageCount());
                Assert.assertTrue("the half-admitting view served nothing", half.getHits() > 0);
                Assert.assertTrue(
                        "the half-admitting view held as much as the full one [half="
                                + half.getPageCount() + ", warm=" + warm.getPageCount() + ']',
                        half.getPageCount() < warm.getPageCount()
                );
                Assert.assertTrue(
                        "holding more pages served no more of them [half=" + half.getHits()
                                + ", warm=" + warm.getHits() + ']',
                        warm.getHits() > half.getHits()
                );
                // The self-tuned view measured its working set on the restore path -
                // which is the wiring, since only a restore that ran through
                // beginRestore/endRestore can have produced a figure at all - and the
                // engine-wide cap is far above what a view this size reads, so the
                // fraction saturates and it behaves as the fully-admitting view does.
                Assert.assertTrue(
                        "the self-tuned view measured no working set, so it never tuned",
                        tuned.getWorkingSetBytes() > 0
                );
                Assert.assertTrue(
                        "a cap far above the working set must leave the fraction at 1 [workingSet="
                                + tuned.getWorkingSetBytes() + ", capacity="
                                + engine.getLiveViewRegistry().getCheckpointPageCacheBudget().getCapacityBytes()
                                + ']',
                        tuned.getAdmissionFraction() == 1.0
                );
                Assert.assertEquals(warm.getHits(), tuned.getHits());
                Assert.assertEquals(warm.getPageCount(), tuned.getPageCount());
            }
        });
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
    public void testDenseFrameSealsShareChunksWithThePreviousRoot() throws Exception {
        assertMemoryLeak(() -> {
            // A frame wide enough for a chunk descriptor to pay for itself: 4000
            // rows per key, refilled 200 rows at a time, so twenty roots' worth of
            // chunks sit inside one frame and the seal has something to share.
            createBaseAndView("base", "lv", RANGE_DENSE_FRAME);
            final long[] dataBytes = new long[DENSE_SAMPLES + 1];
            long logicalStateBytes = 0;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int commit = 1; commit <= DENSE_SAMPLES * DENSE_SAMPLE; commit++) {
                    commitDense(job, "base", commit);
                    if (commit % DENSE_SAMPLE == 0) {
                        driveRefreshToQuiescence(job);
                        final LiveViewInstance instance = viewInstance("lv");
                        try (LiveViewCheckpointMetaStore store = openStore(instance)) {
                            dataBytes[commit / DENSE_SAMPLE] = store.getSuperblock().dataBytes;
                        }
                        logicalStateBytes = instance.getHeadCheckpointStateBytes();
                    }
                }

                final LiveViewInstance instance = viewInstance("lv");
                Assert.assertEquals(
                        "one cadence seal per commit",
                        DENSE_SAMPLES * DENSE_SAMPLE,
                        timelineEntries(instance)
                );
                assertViewMatchesRecompute("lv", "base", RANGE_DENSE_FRAME);

                // Steady state: the frame is full and every seal drops as many rows
                // off the head as it appends, so what a seal writes is the 200 rows
                // per key it appended - not the 4000 per key it holds.
                final long steadyPerSeal =
                        (dataBytes[DENSE_SAMPLES] - dataBytes[DENSE_SAMPLES - 1]) / DENSE_SAMPLE;
                Assert.assertTrue(
                        "a seal wrote " + steadyPerSeal + " bytes over a " + logicalStateBytes + " byte frame",
                        20 * steadyPerSeal < logicalStateBytes
                );
                // And the timeline holds one copy of each captured row rather than
                // one copy per root: 240 roots over a frame this size would be two
                // orders of magnitude more than what the rows themselves cost.
                Assert.assertTrue(
                        "the timeline holds " + dataBytes[DENSE_SAMPLES] + " bytes over "
                                + (DENSE_SAMPLES * DENSE_SAMPLE) + " roots of " + logicalStateBytes + " bytes each",
                        dataBytes[DENSE_SAMPLES] < DENSE_SAMPLES * DENSE_SAMPLE * logicalStateBytes / 10
                );
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

                // Write cost per seal: flat across the last two windows, so a seal
                // costs the live frame and nothing per root that precedes it. Not
                // byte-identical any more - the timestamp and double codecs pack the
                // frame, and how well they pack it moves by a byte or two as the
                // values grow.
                final long midDataPerSeal = (dataBytes[3] - dataBytes[2]) / SAMPLE;
                final long lateDataPerSeal = (dataBytes[4] - dataBytes[3]) / SAMPLE;
                Assert.assertTrue(
                        "a seal's data bytes must not depend on how many roots precede it: "
                                + midDataPerSeal + " then " + lateDataPerSeal,
                        Math.abs(midDataPerSeal - lateDataPerSeal) <= 2
                );
                // The semantic codecs are where this frame's win is: rows on a fixed
                // cadence carry no timestamp delta-of-delta and the values differ in
                // few bits, so a root's image costs a fraction of the raw frame.
                Assert.assertTrue(
                        "a seal wrote " + lateDataPerSeal + " bytes over a " + logicalStateBytes + " byte frame",
                        4 * lateDataPerSeal < logicalStateBytes
                );

                // This frame declines chunk sharing, and the total is therefore still
                // one image per root. That is the intended answer at this size, not a
                // gap: a chunk costs two 40-byte page references in every later root,
                // against the 16 raw bytes of the row it would save re-encoding, so a
                // thirteen-row frame refilled one row at a time would pay far more in
                // partition-entry metadata than it saved in payload.
                // LiveViewCheckpointRingSeal.chunkCap draws that line at
                // MIN_SHARED_CHUNK_ROWS rows per chunk, which leaves this frame with
                // one chunk - rebuilt per root - and lets a dense frame share almost
                // everything (testDenseFrameSealsShareChunksWithThePreviousRoot).
                Assert.assertTrue(
                        "the timeline holds one image per root: " + dataBytes[4] + " bytes over "
                                + SEALS + " roots",
                        10 * dataBytes[4] >= 9 * SEALS * lateDataPerSeal
                );

                // Publication metadata per seal: flat once the tree nodes fill,
                // because every structure a publication touches is copy-on-write.
                // The segment directory was the last one that was not - one page
                // holding every live segment, rewritten in full on every
                // publication, so a seal paid 32 bytes per catalogued segment and
                // that bill grew without bound. It is now a tree that copies its
                // search path, like the timeline beside it, and a seal pays a node
                // per level instead.
                //
                // The first window is the cheap one for both trees - their leaves
                // are still filling towards the 64-record capacity a copy rewrites
                // in full - which is why flatness is asserted from the second
                // window on. What is left growing beyond that is tree height, and
                // the next level costs one more node at 4096 entries.
                final long earlyMetadataPerSeal = metadataBytes[1] / SAMPLE;
                final long midMetadataPerSeal = (metadataBytes[2] - metadataBytes[1]) / SAMPLE;
                final long lateMetadataPerSeal = (metadataBytes[4] - metadataBytes[3]) / SAMPLE;
                Assert.assertTrue(
                        "publication metadata per seal: " + earlyMetadataPerSeal + " bytes over the first "
                                + SAMPLE + " roots, " + midMetadataPerSeal + " over the second " + SAMPLE
                                + ", " + lateMetadataPerSeal + " over the last " + SAMPLE,
                        5 * lateMetadataPerSeal <= 6 * midMetadataPerSeal
                );
            }
        });
    }

    private static String cacheBase(int index) {
        return "cache_base_" + index;
    }

    private static String cacheView(int index) {
        return "cache_lv_" + index;
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

    private static String timestamp(int second) {
        return String.format(
                "2026-01-%02dT%02d:%02d:%02d.000000Z",
                1 + second / 86_400,
                (second % 86_400) / 3600,
                (second % 3600) / 60,
                second % 60
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
     * Commits one row per key, at the same timestamp, into every base of the page
     * cache differential, and gives the refresh job a turn on all of them. The
     * clock steps once for the whole group, so the three views meet identical data
     * on identical deadlines and the only thing that separates them is what their
     * caches are allowed to keep.
     */
    private void commitEveryKey(LiveViewRefreshJob job, int second, long value) throws Exception {
        setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
        final String rowTs = timestamp(second);
        for (int view = 0; view < CACHE_VIEWS; view++) {
            final StringBuilder sql = new StringBuilder("INSERT INTO " + cacheBase(view) + " (ts, sym, x) VALUES ");
            for (int key = 0; key < CACHE_KEYS; key++) {
                if (key > 0) {
                    sql.append(", ");
                }
                sql.append("('").append(rowTs).append("', '")
                        .append((char) ('a' + key)).append("', ").append(value + key).append(')');
            }
            execute(sql.toString());
        }
        drainWalQueue();
        drainJob(job);
        drainWalQueue();
    }

    /**
     * Commits {@code DENSE_ROWS_PER_COMMIT} rows per key at one-second spacing,
     * ascending, and gives the refresh job a turn on them. Ascending matters: an
     * out-of-order commit routes the whole cycle through the repair path, which
     * would measure something other than the cadence seal.
     */
    private void commitDense(LiveViewRefreshJob job, String baseName, int commit) throws Exception {
        setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
        final StringBuilder sql = new StringBuilder("INSERT INTO " + baseName + " (ts, sym, x) VALUES ");
        final int firstSecond = (commit - 1) * DENSE_ROWS_PER_COMMIT;
        for (int i = 0; i < DENSE_ROWS_PER_COMMIT; i++) {
            final String rowTs = timestamp(firstSecond + i);
            if (i > 0) {
                sql.append(", ");
            }
            sql.append("('").append(rowTs).append("', 'a', ").append(firstSecond + i).append("), ")
                    .append("('").append(rowTs).append("', 'b', ").append(firstSecond + i + 1).append(')');
        }
        execute(sql.toString());
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

    private LiveViewCheckpointPageCache pageCache(String viewName) {
        final LiveViewCheckpointPageCache cache = viewInstance(viewName).getCheckpointPageCache();
        Assert.assertNotNull("live view '" + viewName + "' holds no page cache", cache);
        return cache;
    }

    /**
     * Re-applies each differential view's admission fraction. A cache is built on
     * the view's first restore and rebuilt cold if the view ever lets its refresh
     * state go, so the fraction is pinned after every commit rather than once: a
     * cache that came back at the default would quietly admit everything and turn
     * the differential into four runs of the same path.
     * <p>
     * The self-tuned view is the exception - nothing is applied to it, so its
     * fraction is whatever its own restores worked out.
     */
    private void pinAdmissionFractions() {
        final LiveViewCheckpointPageCacheBudget budget =
                engine.getLiveViewRegistry().getCheckpointPageCacheBudget();
        for (int i = 0; i < CACHE_VIEWS; i++) {
            final LiveViewCheckpointPageCache cache =
                    viewInstance(cacheView(i)).getOrCreateCheckpointPageCache(budget);
            Assert.assertNotNull("the engine-wide page cache budget must be enabled", cache);
            if (CACHE_ADMISSION_FRACTIONS[i] != CACHE_SELF_TUNED) {
                cache.setAdmissionFraction(CACHE_ADMISSION_FRACTIONS[i]);
            }
        }
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
