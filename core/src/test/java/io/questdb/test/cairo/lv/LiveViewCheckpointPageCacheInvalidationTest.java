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
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointPageCache;
import io.questdb.cairo.lv.LiveViewCheckpointPageCacheBudget;
import io.questdb.cairo.lv.LiveViewCheckpointRangeRingStateReader;
import io.questdb.cairo.lv.LiveViewCheckpointStateCodec;
import io.questdb.cairo.lv.LiveViewCheckpointStatePageRef;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.std.Chars;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

/**
 * Invalidation coverage for the decoded checkpoint page cache: what the cache
 * must forget, and when.
 * <p>
 * The cache keys a decoded state page on {@code (segmentId, offset, pageKind)}
 * and answers from it without reading the file again, so its whole safety
 * argument is that a data segment is immutable and its id is minted once. Two
 * things can break that. A transition that retires a timeline restarts the id
 * space at zero, and a stale entry would then answer for the file that replaced
 * it - that hazard is closed by moving the cache to a new epoch, which drops
 * everything it holds. A transition that merely deletes a segment cannot alias
 * anything, because the id is never re-minted, but it leaves entries nothing
 * will ever probe again holding slots against an engine-wide budget - those are
 * evicted by segment.
 * <p>
 * The oracle throughout is the same window recomputed from the base table with
 * the refresh fault count held at zero, so a cache that answered with the wrong
 * page surfaces as a diff rather than as a view that quietly self-healed. The
 * headline case runs that oracle as a differential: two identical views over
 * identical bases through identical commits, one caching everything and one
 * caching nothing, must agree with the recompute and with each other after every
 * transition the refresh worker can drive.
 */
public class LiveViewCheckpointPageCacheInvalidationTest extends AbstractLiveViewTest {

    // Fraction of pages, by identity hash, each of the two differential views
    // admits: everything, and nothing.
    private static final double[] ADMISSION_FRACTIONS = {1, 0};
    private static final int CACHED = 0;
    private static final int COLD = 1;
    private static final int COMMITS_PER_ROUND = 4;
    // One row per key per commit. Several keys keep several rings in front of the
    // cache, so a partition map holds a page pair per key per root.
    private static final int KEYS = 4;
    // Shape of the probe page seeded under every catalogued segment. The offset is
    // a terabyte in, which no data segment reaches, so a probe page shares an
    // identity with nothing a restore decoded.
    private static final int PROBE_PAGE_BYTES = 64;
    private static final long PROBE_PAGE_OFFSET = 1L << 40;
    private static final int ROUNDS = 8;
    private static final String RANGE_30S_FRAME =
            "PARTITION BY sym ORDER BY ts RANGE BETWEEN '30' SECOND PRECEDING AND CURRENT ROW";
    private static final int VIEWS = 2;

    @After
    public void resetClock() {
        setCurrentMicros(-1);
    }

    @Before
    public void setUpCadence() {
        // One logical root per commit: the densest cadence, so the timeline offers
        // the most boundaries for a repair to re-version and a compaction to drain.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        // Pin the clock below the (2026) data so a START FROM NOW view resolves its
        // lower bound under every row it will ever see, corrections included.
        setCurrentMicros(0);
    }

    @Test
    public void testDeletedSegmentsLeaveNoPageBehindInTheCache() throws Exception {
        // Compaction off - set here rather than left at the default, because the
        // sibling case turns it on and the two share a static override - and no
        // historical repair below, so the only thing that can reclaim a segment is
        // the purge sweep the next worker's first seal runs and this case measures
        // that hook and no other. Out-of-order resumes are what strand the segments:
        // each truncates the timeline tail it replayed over, and the roots that go
        // with it were the last to name some of what the cache had already decoded.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_COMPACTION_INTERVAL, 0);
        assertMemoryLeak(() -> {
            createBaseAndView(base(CACHED), view(CACHED));
            int second = 0;
            final LongList segmentsBefore;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                pinAdmissionFractions(1);
                second = buildHistory(job, second, 1, false);

                final LiveViewInstance instance = viewInstance(view(CACHED));
                segmentsBefore = dataSegmentIds(instance);
                final LiveViewCheckpointPageCache cache = pageCache(view(CACHED));
                Assert.assertTrue(
                        "the history must leave the cache holding pages to invalidate",
                        cache.getPageCount() > 0
                );
                seedProbePages(cache, segmentsBefore);
            }

            // A fresh worker brings a fresh timeline writer, so its first seal runs
            // the lifecycle reconciliation - the live view's segment GC - over a
            // timeline whose resumes have left segments no current root names.
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                pinAdmissionFractions(1);
                for (int i = 0; i < COMMITS_PER_ROUND; i++) {
                    second += 10;
                    commitEveryKey(job, second, 7_000L + i, 1);
                    pinAdmissionFractions(1);
                }
                driveRefreshToQuiescence(job);
            }

            final LiveViewInstance instance = viewInstance(view(CACHED));
            final LongList segmentsAfter = dataSegmentIds(instance);
            final LiveViewCheckpointPageCache cache = pageCache(view(CACHED));
            int deleted = 0;
            int survived = 0;
            for (int i = 0, n = segmentsBefore.size(); i < n; i++) {
                final long segmentId = segmentsBefore.getQuick(i);
                final boolean unlinked = segmentsAfter.indexOf(segmentId) < 0;
                // The probe page says what the sweep did to this segment's entries;
                // the page count says the same about the ones the restores put there.
                Assert.assertEquals(
                        "segment " + segmentId + (unlinked ? " was unlinked" : " is still on disk")
                                + " but the cache disagrees",
                        unlinked,
                        cache.probe(probeRef(segmentId)) == 0
                );
                if (unlinked) {
                    deleted++;
                    Assert.assertEquals(
                            "segment " + segmentId + " is gone from disk but its pages are still cached",
                            0,
                            cache.getSegmentPageCount(segmentId)
                    );
                } else {
                    survived++;
                }
            }
            Assert.assertTrue("the sweep unlinked nothing, so this case asserts nothing", deleted > 0);
            Assert.assertTrue(
                    "the sweep unlinked every segment, so it cannot have been scoped to what it deleted",
                    survived > 0
            );
            assertViewMatchesRecompute(view(CACHED), base(CACHED));
        });
    }

    @Test
    public void testRebuiltTimelineCannotServeAPageOfTheOneItReplaced() throws Exception {
        // The alias hazard, end to end. A reconciliation that meets a checkpoint
        // directory this build cannot read removes it whole, and the timeline the
        // next seal opens mints segment ids from zero again - so an entry the old
        // timeline left behind sits under an id the new one is about to hand to a
        // different file. Nothing about the entry looks wrong: a re-minted segment
        // lays pages of the same shape at the same offsets, so the codec, row count
        // and decoded length a probe validates all agree. Only the epoch tells them
        // apart.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_COMPACTION_INTERVAL, 0);
        assertMemoryLeak(() -> {
            createBaseAndView(base(CACHED), view(CACHED));
            int second = 0;
            final LongList segmentsBefore;
            final long epochBefore;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                pinAdmissionFractions(1);
                second = buildHistory(job, second, 1, false);

                final LiveViewInstance instance = viewInstance(view(CACHED));
                final LiveViewCheckpointPageCache cache = pageCache(view(CACHED));
                segmentsBefore = dataSegmentIds(instance);
                Assert.assertTrue("the history must leave a warm cache", cache.getPageCount() > 0);
                Assert.assertEquals("the history must mint segment ids from zero", 0, segmentsBefore.getQuick(0));
                epochBefore = cache.getEpoch();
                seedProbePages(cache, segmentsBefore);
                // A top-level name outside the current layout, of the kind an
                // earlier development build left: the next reconciliation reads the
                // whole directory as foreign and removes it.
                touchTopLevel(instance, "_ring");
            }

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                pinAdmissionFractions(1);
                for (int i = 0; i < COMMITS_PER_ROUND; i++) {
                    second += 10;
                    commitEveryKey(job, second, 6_000L + i, 1);
                    pinAdmissionFractions(1);
                }
                driveRefreshToQuiescence(job);

                final LiveViewInstance instance = viewInstance(view(CACHED));
                final LiveViewCheckpointPageCache cache = pageCache(view(CACHED));
                Assert.assertEquals(
                        "the rebuilt timeline must mint segment ids from zero again",
                        0,
                        dataSegmentIds(instance).getQuick(0)
                );
                Assert.assertTrue(
                        "the cache must have moved to a new epoch [before=" + epochBefore
                                + ", after=" + cache.getEpoch() + ']',
                        cache.getEpoch() > epochBefore
                );
                for (int i = 0, n = segmentsBefore.size(); i < n; i++) {
                    final long segmentId = segmentsBefore.getQuick(i);
                    Assert.assertEquals(
                            "segment " + segmentId + " belongs to the timeline that was removed,"
                                    + " but the cache still answers for it",
                            0,
                            cache.probe(probeRef(segmentId))
                    );
                    Assert.assertEquals(0, cache.getSegmentPageCount(segmentId));
                }

                // And the rebuilt timeline is usable: corrections below the head
                // resume from its roots, so the restores that follow read the
                // re-minted ids the stale entries were keyed on.
                commitEveryKey(job, second - 5, 6_500L, 1);
                pinAdmissionFractions(1);
                driveRefreshToQuiescence(job);
            }
            assertViewMatchesRecompute(view(CACHED), base(CACHED));
        });
    }

    @Test
    public void testViewIsIdenticalThroughEveryCacheInvalidatingTransition() throws Exception {
        // Attempt a compaction pass on every seal, so the drained-segment eviction
        // runs over this case too rather than behind a cadence it would wait out.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_COMPACTION_INTERVAL, 1);
        assertMemoryLeak(() -> {
            // Two identical views over two identical bases, driven through the same
            // commits, differing only in whether their decoded checkpoint state is
            // cached at all. Every transition below - an out-of-order resume that
            // truncates the timeline tail, a historical repair that splices it, a
            // compaction that drains segments, and a restart whose first seal purges
            // them - runs over both. A cache that kept a page it should have dropped
            // shows up as the two views disagreeing.
            for (int i = 0; i < VIEWS; i++) {
                createBaseAndView(base(i), view(i));
            }
            int second = 0;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                pinAdmissionFractions(VIEWS);
                second = buildHistory(job, second, VIEWS, true);

                final LiveViewInstance cached = viewInstance(view(CACHED));
                Assert.assertTrue(
                        "the history must resume from an anchor checkpoint, or nothing restored",
                        cached.getO3ResumeReplayRows() > 0
                );
                Assert.assertTrue(
                        "the history must publish a repair splice, or the epoch bump is untested",
                        cached.getCheckpointRepairRootsVersioned() > 0
                );
            }

            // Restart the worker: a fresh timeline writer reconciles the directory on
            // its first seal, which purges the segments the repairs and compactions
            // above left unreferenced.
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                pinAdmissionFractions(VIEWS);
                for (int i = 0; i < COMMITS_PER_ROUND; i++) {
                    second += 10;
                    commitEveryKey(job, second, 8_000L + i, VIEWS);
                    pinAdmissionFractions(VIEWS);
                }
                driveRefreshToQuiescence(job);
                commitEveryKey(job, second - 5, 8_500L, VIEWS);
                pinAdmissionFractions(VIEWS);
                driveRefreshToQuiescence(job);
            }

            for (int i = 0; i < VIEWS; i++) {
                assertViewMatchesRecompute(view(i), base(i));
            }
            TestUtils.assertSqlCursors(
                    engine,
                    sqlExecutionContext,
                    "(" + view(CACHED) + ") ORDER BY 2, 1",
                    "(" + view(COLD) + ") ORDER BY 2, 1",
                    LOG,
                    true
            );

            final LiveViewCheckpointPageCache cached = pageCache(view(CACHED));
            final LiveViewCheckpointPageCache cold = pageCache(view(COLD));
            // The two views really were served differently, so the equality above is
            // a differential rather than two runs of one path.
            Assert.assertTrue("no view restored a ring page", cold.getMisses() > 0);
            Assert.assertEquals("a view admitting nothing must serve nothing", 0, cold.getHits());
            Assert.assertEquals(cold.getMisses(), cached.getHits() + cached.getMisses());
            Assert.assertTrue(
                    "the caching view served nothing, so no invalidation of it was exercised",
                    cached.getHits() > 0
            );
            // And the invalidations really fired: a repair splice moves the cache to
            // a new epoch, which is the drop the alias hazard is closed with.
            Assert.assertTrue("no transition ever moved the cache to a new epoch", cached.getEpoch() > 0);
        });
    }

    private static String base(int index) {
        return "inv_base_" + index;
    }

    private static Path checkpointsDir(LiveViewInstance instance) {
        return new Path().of(configuration.getDbRoot())
                .concat(instance.getLiveViewToken())
                .concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
    }

    // The data segment ids with a final-name file on disk, ascending. A purge
    // unlinks the file and leaves the catalogue entry, so on-disk presence is what
    // says a segment is gone.
    private static LongList dataSegmentIds(LiveViewInstance instance) {
        final LongList ids = new LongList();
        try (Path checkpointsDir = checkpointsDir(instance); Path dataDir = new Path()) {
            LiveViewCheckpointLayout.dataDirPath(dataDir, checkpointsDir);
            final String[] names = new File(dataDir.toString()).list();
            if (names != null) {
                for (String name : names) {
                    if (name.endsWith(LiveViewCheckpointLayout.TMP_SUFFIX)
                            || !Chars.startsWith(name, LiveViewCheckpointLayout.DATA_SEGMENT_PREFIX)) {
                        continue;
                    }
                    try {
                        ids.add(Long.parseLong(name.substring(LiveViewCheckpointLayout.DATA_SEGMENT_PREFIX.length())));
                    } catch (NumberFormatException ignore) {
                        // A name that is not d.<number> is not a data segment we track.
                    }
                }
            }
        }
        ids.sort();
        return ids;
    }

    /**
     * A state page reference for {@code segmentId} at an offset no real segment
     * reaches, so a probe page seeded under it is measurable without colliding
     * with anything a restore decoded.
     */
    private static LiveViewCheckpointStatePageRef probeRef(long segmentId) {
        return new LiveViewCheckpointStatePageRef().of(
                segmentId,
                PROBE_PAGE_OFFSET,
                PROBE_PAGE_BYTES,
                PROBE_PAGE_BYTES,
                LiveViewCheckpointRangeRingStateReader.TIMESTAMP_PAGE_KIND,
                LiveViewCheckpointStateCodec.TIMESTAMP_RAW_64,
                PROBE_PAGE_BYTES / Long.BYTES,
                0
        );
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

    private static String view(int index) {
        return "inv_lv_" + index;
    }

    private static String viewSql(String baseName) {
        return "SELECT ts, sym, sum(x) OVER (" + RANGE_30S_FRAME + ") AS s FROM " + baseName;
    }

    private void assertViewMatchesRecompute(String viewName, String baseName) throws Exception {
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(" + viewSql(baseName) + ") ORDER BY 2, 1",
                "(" + viewName + ") ORDER BY 2, 1",
                LOG,
                true
        );
        assertNoRefreshFaults(viewName);
    }

    /**
     * Builds the history every case runs its invalidations over: in-order groups,
     * then two corrections just below the head - which resume from the newest
     * boundary under them and truncate the tail - and, when
     * {@code deepCorrections} asks for it, one deep correction per round, which
     * localizes and converges, so its repair publishes a range splice. Returns the
     * second the history ends at.
     */
    private int buildHistory(
            LiveViewRefreshJob job,
            int fromSecond,
            int views,
            boolean deepCorrections
    ) throws Exception {
        int second = fromSecond;
        for (int round = 1; round <= ROUNDS; round++) {
            for (int i = 0; i < COMMITS_PER_ROUND; i++) {
                second += 10;
                commitEveryKey(job, second, round * 100L + i, views);
                pinAdmissionFractions(views);
            }
            driveRefreshToQuiescence(job);
            // Two corrections just below the head. Both resume from the newest
            // boundary below them - the same root - so the second restore meets the
            // pages the first one decoded.
            commitEveryKey(job, second - 5, 9_000L + round, views);
            pinAdmissionFractions(views);
            commitEveryKey(job, second - 3, 9_500L + round, views);
            pinAdmissionFractions(views);
            driveRefreshToQuiescence(job);
            if (deepCorrections && round > 1) {
                // Deep in history, below the durable frontier and colliding with no
                // in-order group: a localized repair that converges and splices.
                commitEveryKey(job, 10 * (round - 1) + 3, 8_000L + round, views);
                pinAdmissionFractions(views);
                driveRefreshToQuiescence(job);
            }
        }
        return second;
    }

    /**
     * Commits one row per key, at the same timestamp, into the first {@code views}
     * bases and gives the refresh job a turn on all of them. The clock steps once
     * for the whole group, so the views meet identical data on identical deadlines.
     */
    private void commitEveryKey(LiveViewRefreshJob job, int second, long value, int views) throws Exception {
        setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
        final String rowTs = timestamp(second);
        for (int view = 0; view < views; view++) {
            final StringBuilder sql = new StringBuilder("INSERT INTO " + base(view) + " (ts, sym, x) VALUES ");
            for (int key = 0; key < KEYS; key++) {
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

    private void createBaseAndView(String baseName, String viewName) throws Exception {
        execute("CREATE TABLE " + baseName + " (ts TIMESTAMP, sym SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("CREATE LIVE VIEW " + viewName + " FLUSH EVERY 100ms START FROM NOW AS " + viewSql(baseName));
    }

    private LiveViewCheckpointPageCache pageCache(String viewName) {
        final LiveViewCheckpointPageCache cache = viewInstance(viewName).getCheckpointPageCache();
        Assert.assertNotNull("live view '" + viewName + "' holds no page cache", cache);
        return cache;
    }

    /**
     * Re-applies each view's admission fraction. A cache is built on the view's
     * first restore and rebuilt cold if the view ever lets its refresh state go,
     * so the fraction is pinned after every commit rather than once: a cache that
     * came back at the default would quietly admit everything and turn the
     * differential into two runs of the same path.
     */
    private void pinAdmissionFractions(int views) {
        final LiveViewCheckpointPageCacheBudget budget =
                engine.getLiveViewRegistry().getCheckpointPageCacheBudget();
        for (int i = 0; i < views; i++) {
            final LiveViewCheckpointPageCache cache =
                    viewInstance(view(i)).getOrCreateCheckpointPageCache(budget);
            Assert.assertNotNull("the engine-wide page cache budget must be enabled", cache);
            cache.setAdmissionFraction(ADMISSION_FRACTIONS[i]);
        }
    }

    /**
     * Puts one probe page into the view's cache for every catalogued data
     * segment. What a restore happens to have decoded varies with the workload;
     * a probe page under every segment does not, so the sweep's effect on the
     * cache is measurable segment by segment - the ones it unlinks must lose
     * theirs, the ones it keeps must not.
     */
    private void seedProbePages(LiveViewCheckpointPageCache cache, LongList segmentIds) {
        final long address = Unsafe.malloc(PROBE_PAGE_BYTES, MemoryTag.NATIVE_DEFAULT);
        try {
            Vect.memset(address, PROBE_PAGE_BYTES, 0);
            for (int i = 0, n = segmentIds.size(); i < n; i++) {
                Assert.assertTrue(
                        "the cache must take a probe page for segment " + segmentIds.getQuick(i),
                        cache.admit(probeRef(segmentIds.getQuick(i)), address)
                );
            }
        } finally {
            Unsafe.free(address, PROBE_PAGE_BYTES, MemoryTag.NATIVE_DEFAULT);
        }
    }

    /**
     * Creates an empty file at the top level of the view's checkpoint directory,
     * standing in for an artefact of a layout this build does not write.
     */
    private void touchTopLevel(LiveViewInstance instance, CharSequence name) {
        try (Path dir = checkpointsDir(instance); Path path = new Path()) {
            path.of(dir).concat(name);
            Assert.assertTrue(configuration.getFilesFacade().touch(path.$()));
        }
    }

    private LiveViewInstance viewInstance(String viewName) {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance(viewName);
        Assert.assertNotNull("live view '" + viewName + "' must be registered", instance);
        return instance;
    }
}
