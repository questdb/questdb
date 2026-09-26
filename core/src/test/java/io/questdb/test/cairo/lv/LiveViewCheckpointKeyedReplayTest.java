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

import com.sun.management.ThreadMXBean;
import io.questdb.PropertyKey;
import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.lv.LiveViewCheckpointGenerationPin;
import io.questdb.cairo.lv.LiveViewCheckpointKeyedReplay;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointMetaStore;
import io.questdb.cairo.lv.LiveViewCheckpointOutputKeyDomain;
import io.questdb.cairo.lv.LiveViewCheckpointPageRef;
import io.questdb.cairo.lv.LiveViewCheckpointPartitionMapReader;
import io.questdb.cairo.lv.LiveViewCheckpointRoot;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineReader;
import io.questdb.cairo.lv.LiveViewCheckpointWindowRoot;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.pool.PoolListener;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.std.AbstractIntHashSet;
import io.questdb.std.Chars;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.NumericException;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Coverage for the keyed repair of a closed anchor segment: the replay that follows only
 * the keys a correction touched, and the merge that supplies every other key's row from
 * the view's own stored output.
 * <p>
 * The property every case here rests on is that the two routes are indistinguishable in
 * what they publish. A whole-segment repair reads the segment, evaluates the window over
 * all of it and re-emits the lot; a keyed one reads the affected keys' rows through the
 * base's posting index, re-emits those, and copies the rest forward. The block carries the
 * segment's full row set either way, because {@code REPLACE_RANGE} deletes the range
 * wholesale - which is precisely why a replay emitting only its own keys' rows would drop
 * every other key's, and why the merge exists.
 * <p>
 * The one thing that is <b>not</b> the same is where an unaffected key's row comes from: a
 * whole-segment repair recomputes it from the base, a keyed one copies the stored one. So
 * a keyed repair stops being a from-base recompute of its range, which is the reason
 * {@code cairo.live.view.checkpoint.repair.keyed.replay.enabled} defaults to false and the
 * reason one case below pins the default.
 * <p>
 * The view is the same reported customer shape the pricing and per-segment repair cases
 * use: an anchored WINDOW carrying an unbounded cumulative sum per account, over a base
 * whose timestamps span several anchor days so closed segments exist at all.
 */
public class LiveViewCheckpointKeyedReplayTest extends AbstractLiveViewTest {
    private static final int ACCOUNTS = 8;
    private static final String ACCOUNT_PREFIX = "acct-";
    // A bound on the measured arms together that does not grow with the key count: the
    // tables a wide domain restarts from on every arm fit in it, one object per key of an
    // ARM_KEY_DOMAIN-wide domain does not.
    private static final long ARM_ALLOCATION_LIMIT_BYTES = 16 * 1024;
    private static final int ARM_KEY_DOMAIN = 4_096;
    private static final int ARM_ROUNDS = 4;
    // Keys few enough to stay far below the retained key count, for a case that spends the
    // retained key bytes on width alone.
    private static final int FEW_WIDE_KEYS = 8;
    private static final int ROWS_PER_ACCOUNT_PER_DAY = 10;
    // The default cairo.live.view.checkpoint.repair.scan.max.keys: the widest key domain a
    // keyed repair takes at the default budget.
    private static final int WIDE_KEY_DOMAIN = 100_000;

    @Test
    public void testABoundaryTheKeyedScanNeverCrossesKeepsItsOwnPosition() throws Exception {
        // Every cadence boundary records the count of live-view rows at or below it, and
        // the ladder is the only place that number lives - a wrong one leaves the runtime
        // serving correct rows and a from-base recompute agreeing with it, and the first
        // thing to read it is the resume that credits the view with the rows the root
        // claims.
        //
        // A whole-segment replay reads every row of the range it repairs, so a boundary its
        // cursor never crosses genuinely has no row between it and the last row read: the
        // position the replay ends on is that boundary's own. A KEYED replay reads nothing
        // of the kind. Its cursor follows the corrected keys alone, so a boundary above the
        // last of their rows still has every other key's rows between it and the end of the
        // segment - rows the merge accounts for rather than the replay loop. Freezing those
        // boundaries after the merge has been drained to the end credits each of them with
        // the whole segment.
        //
        // The correction here touches acct-1, whose rows in the repaired day both sit at or
        // below 01:00, while acct-2 carries five rows above it. So the keyed cursor stops
        // at 01:00 and every one of those five boundaries is frozen without being crossed -
        // which is the shape that separates each one's own position from the segment's
        // total. They share an hour, and so a partition, because the keyed read is priced
        // per index open per page frame and has to come out cheaper than reading the day.
        armKeyedReplay();
        assertMemoryLeak(() -> {
            // One row per commit at a one-row cadence, so the repaired day carries five
            // boundaries of its own rather than one.
            createView(row(2, 1, "acct-1"));
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                for (int minute = 10; minute <= 50; minute += 10) {
                    commit(row(2, 1, minute, 0, "acct-2"), job);
                }
                // The head, which closes the second day below it.
                commit(row(5, 1, "acct-1"), job);
                assertLadderCountsRowsAtOrBelowEachBoundary("before");

                // Below every row the day already holds, on the account whose rows stop at
                // 01:00.
                commit(row(2, 0, 30, 0, "acct-1"), job);

                Assert.assertEquals(
                        "the correction must be repaired by key, or the case covers nothing",
                        1,
                        job.keyedReplaySegmentCountForTest()
                );
                Assert.assertEquals(
                        "the merge must supply every row above the last key the replay followed",
                        5,
                        job.keyedReplayMergedRowsForTest()
                );
                assertLadderCountsRowsAtOrBelowEachBoundary("after");
                assertViewMatchesRecompute();
            }

            // The resume is what reads those positions back: it credits the view with the
            // rows the root it selects claims, so a ladder that over-counted leaves the
            // running total disagreeing with the table it was measured against.
            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(2, 0, 45, 0, "acct-2"), job);
                Assert.assertEquals(
                        "the view's own row counter must still describe its table",
                        count("select count() from lv"),
                        viewInstance().getLvRowsTotal()
                );
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testAKeyedRepairIntroducingAnAccountEmitsItsRowsAndKeepsTheRest() throws Exception {
        // A key the view has never stored has no row for the merge to drop, and the replay
        // is the only thing that can produce one. The case that would fail is a merge
        // treating "unresolved in the view's symbol map" as "keep every row of it".
        armKeyedReplay();
        assertMemoryLeak(() -> {
            createView(seedEightAccountsOverThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 1, "acct-1"), job);
                final String before = dumpUnaffectedRowsOnTheSecond("acct-9");

                commit(correction("acct-9"), job);

                Assert.assertEquals(
                        "the correction must be repaired by key",
                        1,
                        job.keyedReplaySegmentCountForTest()
                );
                Assert.assertEquals(
                        "every seeded row of the day belongs to a key the correction did not touch",
                        ACCOUNTS * ROWS_PER_ACCOUNT_PER_DAY,
                        job.keyedReplayMergedRowsForTest()
                );
                TestUtils.assertEquals(before, dumpUnaffectedRowsOnTheSecond("acct-9"));
                Assert.assertEquals(1, count("select count() from lv where account_id = 'acct-9'"));
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testACorrectionOnTheNullKeyIsRepairedByKey() throws Exception {
        // The null account is a partition key like any other: the base index names its
        // rows under a key of its own, and the view stores them under its own null key. A
        // merge that read "the correction's key does not resolve" as "keep every row of
        // it" would copy the null key's stale rows forward beside the replay's corrected
        // ones and double the day.
        armKeyedReplay();
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_REPLAY_MAX_ROWS, 1);
        assertMemoryLeak(() -> {
            createView(seedEightAccountsOverThreeDays() + ", " + seedNullAccountOverThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 1, "acct-1"), job);
                final LiveViewInstance instance = viewInstance();
                final long resumesBefore = instance.getCheckpointRepairResumes();
                final long rowsBefore = count("select count() from lv");

                commit(nullCorrection(), job);

                Assert.assertEquals(
                        "a correction on the null key must be repaired by key like any other",
                        1,
                        job.keyedReplaySegmentCountForTest()
                );
                Assert.assertTrue(
                        "the null-key merge state must survive a replay-budget park",
                        instance.getCheckpointRepairResumes() > resumesBefore
                );
                Assert.assertEquals(
                        "the named accounts' rows are the ones copied forward - the null key's are replayed",
                        ACCOUNTS * ROWS_PER_ACCOUNT_PER_DAY,
                        job.keyedReplayMergedRowsForTest()
                );
                Assert.assertEquals(rowsBefore + 1, count("select count() from lv"));
                Assert.assertEquals(
                        ROWS_PER_ACCOUNT_PER_DAY + 1,
                        count("select count() from lv where account_id is null"
                                + " and created_at >= '2026-01-02T00:00:00.000000Z'::timestamp"
                                + " and created_at < '2026-01-03T00:00:00.000000Z'::timestamp")
                );
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testASegmentBehindAParkedOneIsStillRepairedByItsKeys() throws Exception {
        // The two routes have to compose. The first segment is priced whole and parks;
        // the segment queued behind it is priced keyed. The key domain is collected by
        // the change-set decomposition into scratch that belongs to the classifying turn,
        // so a loop carrying only timestamps would hand the resuming turn nothing to arm
        // from and the queued segment would read whole.
        //
        // The shape is the reported one: an eight-account correction on the older day,
        // which the cost model prices whole and which a one-row replay budget parks, and a
        // one-account correction on the day above it, which it prices keyed.
        armKeyedReplay();
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_REPLAY_MAX_ROWS, 1);
        assertMemoryLeak(() -> {
            createView(seedEightAccountsOverThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 1, "acct-1"), job);

                commit(correctionOnEveryAccount(2) + ", " + row(3, 0, 30, 0, "acct-1"), job);

                Assert.assertTrue(
                        "a one-row replay budget must park the loop's first segment",
                        job.segmentYieldCountForTest() > 0
                );
                Assert.assertEquals(
                        "the eight-account day must be priced whole and the one-account day keyed",
                        1,
                        job.keyedScanCheaperCountForTest()
                );
                Assert.assertEquals(
                        "the segment behind the parked one must still be repaired by its keys",
                        1,
                        job.keyedReplaySegmentCountForTest()
                );
                Assert.assertEquals(
                        "and its merge must copy every unaffected account's row forward",
                        (ACCOUNTS - 1) * ROWS_PER_ACCOUNT_PER_DAY,
                        job.keyedReplayMergedRowsForTest()
                );
                Assert.assertEquals(
                        "both segments must be repaired, and each exactly once",
                        2,
                        job.segmentRepairCountForTest()
                );
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testAnApplyAheadCommitIsRepairedByItsOwnKeys() throws Exception {
        // The key domain is per segment, and the change-set decomposition collects it over
        // the whole range the repair re-materialises rather than over the commit the drain
        // broke on. ApplyWal2TableJob races past that trigger, so a segment whose only
        // correction arrived in the range behind it has to be repaired by the keys that
        // commit touched - and by no others. A domain inherited from the trigger would
        // recompute an account the ahead commit never corrected and copy the corrected
        // accounts' stale rows forward, which loses the correction outright and reports the
        // trigger's own merge width while doing it.
        armKeyedReplay();
        assertMemoryLeak(() -> {
            createView(seedEightAccountsOverThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 1, "acct-1"), job);
                final long rowsBefore = count("select count() from lv");

                // The trigger corrects one account on the second day. The commit apply raced
                // past it corrects two others on the third, and the drain never reads it.
                execute("insert into tx values " + correction("acct-1"));
                execute("insert into tx values " + row(3, 0, 30, 2, "acct-2")
                        + ", " + row(3, 0, 30, 3, "acct-3"));
                drainWalQueue();
                driveRefreshToQuiescence(job);

                Assert.assertEquals(
                        "both days must be repaired, and each by its own keys",
                        2,
                        job.keyedReplaySegmentCountForTest()
                );
                Assert.assertEquals(
                        "the second day copies seven accounts forward and the third six, which is the"
                                + " ahead commit's own two keys rather than the trigger's one",
                        (ACCOUNTS - 1 + ACCOUNTS - 2) * ROWS_PER_ACCOUNT_PER_DAY,
                        job.keyedReplayMergedRowsForTest()
                );
                Assert.assertEquals(2, job.segmentRepairCountForTest());
                Assert.assertEquals(rowsBefore + 3, count("select count() from lv"));
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testAnApplyAheadCommitIntroducingAnAccountIsRepairedByItsOwnKeys() throws Exception {
        // The change set keeps each key as the pinned base reader's symbol integer, which it
        // resolves while it walks the WAL. The WAL carries values the base had never seen
        // when the drain broke on its trigger - here an account first written by the commit
        // apply raced past it - and the walk has to find each one in the reader it pinned
        // for the repair. It can, because it walks no commit above that reader's own; a
        // value the reader could not name would leave the day's domain incomplete, and the
        // day would read whole rather than by key.
        armKeyedReplay();
        assertMemoryLeak(() -> {
            createView(seedEightAccountsOverThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 1, "acct-1"), job);
                final long rowsBefore = count("select count() from lv");

                execute("insert into tx values " + correction("acct-1"));
                execute("insert into tx values " + row(3, 0, 30, 2, "acct-new"));
                drainWalQueue();
                driveRefreshToQuiescence(job);

                Assert.assertEquals(
                        "both days must be repaired by key, the new account's included",
                        2,
                        job.keyedReplaySegmentCountForTest()
                );
                Assert.assertEquals(
                        "the second day copies seven accounts forward and the third all eight",
                        (ACCOUNTS - 1 + ACCOUNTS) * ROWS_PER_ACCOUNT_PER_DAY,
                        job.keyedReplayMergedRowsForTest()
                );
                Assert.assertEquals(rowsBefore + 2, count("select count() from lv"));
                Assert.assertEquals(1, count("select count() from lv where account_id = 'acct-new'"));
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testADedupReplacementAppliedBehindTheCleanRangeCheckIsRepairedWhole() throws Exception {
        // The replacement moves 2026-01-03T01:00:01 from acct-1 to acct-5 in the day above
        // the correction's.
        assertDedupReplacementBehindTheCleanRangeCheckIsRepairedWhole(
                false,
                "",
                "('2026-01-03T01:00:01.000000Z', 'acct-5', 1.0)",
                "2026-01-03T01:00:01.000000Z",
                """
                        created_at\taccount_id\tcumulative_sum
                        2026-01-03T01:00:01.000000Z\tacct-5\t1.0
                        """
        );
    }

    @Test
    public void testADedupReplacementAppliedBehindTheCleanRangeCheckIsRepairedWholeWhenPublishedSparsely() throws Exception {
        assertDedupReplacementBehindTheCleanRangeCheckIsRepairedWhole(
                true,
                "",
                "('2026-01-03T01:00:01.000000Z', 'acct-5', 1.0)",
                "2026-01-03T01:00:01.000000Z",
                """
                        created_at\taccount_id\tcumulative_sum
                        2026-01-03T01:00:01.000000Z\tacct-5\t1.0
                        """
        );
    }

    @Test
    public void testADedupReplacementInsideTheCorrectedSegmentIsRepairedWhole() throws Exception {
        // The replacement moves 2026-01-02T01:00:02 from acct-2 to acct-5 in the day the
        // correction itself lands in, so one segment carries both keys the walk saw and the
        // one it did not.
        assertDedupReplacementBehindTheCleanRangeCheckIsRepairedWhole(
                false,
                "",
                "('2026-01-02T01:00:02.000000Z', 'acct-5', 1.0)",
                "2026-01-02T01:00:02.000000Z",
                """
                        created_at\taccount_id\tcumulative_sum
                        2026-01-02T01:00:02.000000Z\tacct-5\t1.0
                        """
        );
    }

    @Test
    public void testADedupReplacementTheFilterRejectsIsRepairedWhole() throws Exception {
        // The replacement moves 2026-01-03T01:00:01 to acct-5 with an amount the view's WHERE
        // rejects, so the timestamp must leave the view altogether.
        assertDedupReplacementBehindTheCleanRangeCheckIsRepairedWhole(
                false,
                " where amount > 0",
                "('2026-01-03T01:00:01.000000Z', 'acct-5', -1.0)",
                "2026-01-03T01:00:01.000000Z",
                "created_at\taccount_id\tcumulative_sum\n"
        );
    }

    @Test
    public void testAFilteredViewOverAnAppendOnlyBaseIsStillRepairedByKey() throws Exception {
        // The counterpart of the dedup cases, and the reason their gate is the base's dedup
        // keys rather than the view's filter. Without dedup a classified change set only adds
        // base rows, so a stored row whose key the walk did not collect derives from base rows
        // nothing changed, and a recomputed key re-emits every pair it had stored plus whatever
        // the new rows add - the merge has no stale row to carry and a sparse upsert has no row
        // to delete. The correction carries one row the filter accepts and one it rejects, so
        // both kinds of key are in the domain, and the publication is the sparse one, which
        // is the one that could not remove a row if a filter ever required it to.
        armKeyedReplay();
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SPARSE_PUBLICATION_ENABLED, "true");
        assertMemoryLeak(() -> {
            createView(seedEightAccountsOverThreeDays(), true, "", " where amount > 0");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 1, "acct-1"), job);

                commit(correction("acct-1") + ", ('2026-01-02T00:30:01.000000Z', 'acct-2', -1.0)", job);

                Assert.assertEquals(
                        "a filtered view over an append-only base must still be repaired by key",
                        1,
                        job.keyedReplaySegmentCountForTest()
                );
                Assert.assertEquals(
                        "and published sparsely, or the case covers the merged route only",
                        1,
                        job.sparsePublicationCountForTest()
                );
                assertViewMatchesRecompute("lv", " where amount > 0");
            }
        });
    }

    @Test
    public void testAKeyedRepairAcrossAFusionSwitchNeedsNoConversionAndKeepsEveryKey() throws Exception {
        // A keyed repair's splice images the keys the correction touched and leaves every
        // other key's entry to the root it re-versions, so it may only build on a root the
        // running build can read. That used to make the fusion switch a format boundary:
        // roots sealed with it off were a different shape, the splice over them was declined
        // and the interval took the truncate instead.
        //
        // The switch is no longer a format boundary. Both settings seal a window root under
        // the same manifest, so the ten roots below need no conversion and the splice goes
        // through in both phases - which is what the decline counter asserts here. The guard
        // itself stays in production for the case that can still reach it: a timeline an
        // earlier build wrote, or a function whose state format has moved on.
        //
        // Per-segment repair is switched off for the last correction so that it resumes off
        // a sealed root rather than re-repairing the segment - the route that masked the
        // loss before the guard existed.
        armKeyedReplay();
        setProperty(PropertyKey.CAIRO_SQL_WINDOW_MAP_FUSION_ENABLED, "false");
        try {
            assertMemoryLeak(() -> {
                createView(row(2, 1, "acct-1"));
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    driveRefreshToQuiescence(job);
                    // Ten commits of eight accounts each on the second, one root per commit,
                    // all sealed with map fusion off.
                    for (int minute = 0; minute < 10; minute++) {
                        commit(eightAccountsOnTheSecond(minute), job);
                    }
                    // Closes the second below the head.
                    commit(row(3, 1, "acct-1"), job);
                }

                // The switch goes back on. The runtime fuses and the next refresh restores
                // off the very roots the unfused seals wrote, with nothing to convert.
                setProperty(PropertyKey.CAIRO_SQL_WINDOW_MAP_FUSION_ENABLED, (String) null);
                restartCycle();
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    driveRefreshToQuiescence(job);
                    assertRestoredFromTimeline("lv");
                    // Below every row the second holds, on one account: a keyed correction
                    // whose interval is exactly those ten roots.
                    commit(row(2, 0, 30, 0, "acct-1"), job);
                    Assert.assertEquals(
                            "roots sealed under the other fusion setting need no conversion, so the"
                                    + " splice must go through",
                            0,
                            job.keyDomainSpliceDeclineCountForTest()
                    );
                    assertViewMatchesRecompute();
                }

                // A resume off a spliced root is what reads the repair back. With the
                // per-segment route off, the correction below resumes from the newest root
                // beneath it instead of re-repairing the segment.
                setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_PER_SEGMENT_ENABLED, "false");
                restartCycle();
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    driveRefreshToQuiescence(job);
                    commit(row(2, 1, 5, 30, "acct-2"), job);
                    assertViewMatchesRecompute();
                    assertQuery("SELECT cumulative_sum FROM lv WHERE account_id = 'acct-2' AND created_at >= '2026-01-02T01:05:30' ORDER BY created_at")
                            .returns("cumulative_sum\n" +
                                    "7.0\n" +
                                    "8.0\n" +
                                    "9.0\n" +
                                    "10.0\n" +
                                    "11.0\n");
                    Assert.assertEquals(
                            "a later splice must go through as well",
                            0,
                            job.keyDomainSpliceDeclineCountForTest()
                    );
                }
            });
        } finally {
            setProperty(PropertyKey.CAIRO_SQL_WINDOW_MAP_FUSION_ENABLED, (String) null);
            setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_PER_SEGMENT_ENABLED, (String) null);
        }
    }

    @Test
    public void testAKeyedRepairParksOnItsReplayBudget() throws Exception {
        armKeyedReplay();
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_REPLAY_MAX_ROWS, 1);
        assertMemoryLeak(() -> {
            createView(seedEightAccountsOverThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 1, "acct-1"), job);
                final LiveViewInstance instance = viewInstance();
                final long resumesBefore = instance.getCheckpointRepairResumes();

                commit(correction("acct-1"), job);

                Assert.assertEquals(1, job.keyedReplaySegmentCountForTest());
                Assert.assertTrue(
                        "a closed-segment keyed repair must honor the configured replay budget",
                        instance.getCheckpointRepairResumes() > resumesBefore
                );
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testTwoParkedKeyedRepairsOwnAndReleaseIndependentReplayState() throws Exception {
        armKeyedReplay();
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_REPLAY_MAX_ROWS, 1);
        assertMemoryLeak(() -> {
            createView(seedEightAccountsOverThreeDays());
            execute("create live view lv2 flush every 100ms start from beginning as "
                    + "select created_at, account_id, sum(amount) over w as cumulative_sum "
                    + "from tx window w as (partition by account_id order by created_at anchor daily '00:00')");
            final LiveViewInstance first = viewInstance();
            final LiveViewInstance second = engine.getLiveViewRegistry().getViewInstance("lv2");
            Assert.assertNotNull("live view 'lv2' must be registered", second);

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 1, "acct-1"), job);

                execute("insert into tx values " + correction("acct-1"));
                drainWalQueue();
                Assert.assertTrue(job.processNotificationsForTest());

                Assert.assertNotNull("the first keyed repair must be parked", first.getSuspendedRepair());
                Assert.assertNotNull("the second keyed repair must be parked", second.getSuspendedRepair());
                Assert.assertNotNull(first.getSuspendedRepair().getKeyedReplay());
                Assert.assertNotNull(second.getSuspendedRepair().getKeyedReplay());
                Assert.assertNotSame(
                        "parked views must not share worker-global keyed replay state",
                        first.getSuspendedRepair().getKeyedReplay(),
                        second.getSuspendedRepair().getKeyedReplay()
                );
            }

            Assert.assertNull("worker close must discard the first parked repair", first.getSuspendedRepair());
            Assert.assertNull("worker close must discard the second parked repair", second.getSuspendedRepair());
            Assert.assertEquals("worker close must release parked readers", 0, engine.getBusyReaderCount());
            Assert.assertEquals("worker close must release parked writers", 0, engine.getBusyWriterCount());

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                assertViewMatchesRecompute();
                assertViewMatchesRecompute("lv2");
            }
        });
    }

    /**
     * A closed-segment keyed repair with map fusion off must keep the anchors of the keys
     * outside its correction domain. {@code acct-3}, which no correction touches, is the
     * distinguishing evidence: its cumulative sums must read 7.0 and 10.0, where the
     * defective baseline produced 1.0 and 4.0.
     */
    @Test
    public void testAKeyedRepairWithoutFusionKeepsUntouchedAccountsAccumulating() throws Exception {
        armKeyedReplay();
        // The two settings the reproduction turns on top of the armed route. Per-segment
        // repair is what produces one repaired root per minute boundary rather than one
        // for the day; fusion off is what used to select the second root format, and is
        // kept off across the restart below so the case never leaves the mode it is about.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_PER_SEGMENT_ENABLED, "true");
        setProperty(PropertyKey.CAIRO_SQL_WINDOW_MAP_FUSION_ENABLED, "false");
        assertMemoryLeak(() -> {
            createView("('2026-01-02T00:45:00.000000Z', 'acct-1', 1.0)");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");

                // Ten minute-boundary checkpoints, each one commit of eight accounts. One
                // commit per minute rather than one for the lot: the ladder is what the
                // repair re-versions, and a single commit would leave it one boundary deep.
                for (int minute = 0; minute < 10; minute++) {
                    commit(eightAccountsAtMinute(minute), job);
                }
                Assert.assertTrue(
                        "the ten minute commits must each seal a boundary of their own",
                        snapshotCheckpointLadder(viewInstance()).size() / 2 >= 10
                );

                // Closes the January 2 anchor segment, so the correction below lands in a
                // closed one and takes the keyed route rather than the open-segment resume.
                commit("('2026-01-03T01:00:00.000000Z', 'acct-1', 1.0)", job);

                // The first correction, below every row January 2 holds.
                commit("('2026-01-02T00:30:00.000000Z', 'acct-1', 1.0)", job);
                Assert.assertTrue(
                        "the correction must take the keyed route, or the case covers nothing",
                        job.keyedReplaySegmentCountForTest() > 0
                );
                assertViewMatchesRecompute();
                // The defect in its own terms, before any restart reads the roots back:
                // every repaired boundary must still name all eight accounts. A root that
                // kept only the corrected key would pass the comparison above - the live
                // runtime still holds the right state - and fail the restore below.
                assertEveryRepairedBoundaryKeepsAllAccounts();
            }

            // Per-segment repair off for the restart: the state has to come back off the
            // roots the repair published rather than be re-derived by another repair.
            setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_PER_SEGMENT_ENABLED, "false");
            restartCycle();
            Assert.assertFalse("the restored view must not be invalid", viewInstance().isInvalid());
            assertNoRefreshFaults("lv");

            // The second correction, on a different account. This is what read the
            // restored accumulators on the defective baseline: acct-3 had none left.
            //
            // It is also what forces the restore. The drain inside restartCycle has no base
            // transaction to apply, so the recompiled view does not rehydrate until a row
            // needs it - which is why the route witness is asserted here rather than
            // straight after the restart.
            try (LiveViewRefreshJob resumed = new LiveViewRefreshJob(0, engine, 1)) {
                commit("('2026-01-02T01:05:30.000000Z', 'acct-2', 1.0)", resumed);
                assertRestoredFromTimeline("lv");
                assertNoRefreshFaults("lv");
                assertViewMatchesRecompute();
                assertAccountSum("acct-3", "2026-01-02T01:06:00.000000Z", 7.0);
                assertAccountSum("acct-3", "2026-01-02T01:09:00.000000Z", 10.0);
            }
        });
    }

    @Test
    public void testAKeyedRepairIsNotTakenWhenTheWholeSegmentIsCheaper() throws Exception {
        // The route is armed and the gate is open; the cost model is what turns it down.
        // At the default index-open price a forty-row day is not worth seeking through, so
        // the segment reads whole - which is what every repair did before the route existed.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_KEYED_REPLAY_ENABLED, "true");
        assertMemoryLeak(() -> {
            createView(seedEightAccountsOverThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 1, "acct-1"), job);

                commit(correction("acct-1"), job);

                Assert.assertEquals(
                        "the whole-segment read is priced below the keyed one at this scale",
                        0,
                        job.keyedScanCheaperCountForTest()
                );
                Assert.assertEquals(0, job.keyedReplaySegmentCountForTest());
                Assert.assertEquals(0, job.keyedReplayMergedRowsForTest());
                Assert.assertEquals(1, job.segmentRepairCountForTest());
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testAKeyedRepairLeavesEveryUnaffectedKeysRowExactlyAsItStood() throws Exception {
        // The property item 4's designed publication could not hold, and the one the merge
        // exists for: a REPLACE_RANGE over the segment deletes every row in it, so a replay
        // emitting only the corrected account's rows would take the other three accounts'
        // rows out of the day with it.
        armKeyedReplay();
        assertMemoryLeak(() -> {
            createView(seedEightAccountsOverThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 1, "acct-1"), job);
                final long rowsBefore = count("select count() from lv");
                final String before = dumpUnaffectedRowsOnTheSecond("acct-1");

                commit(correction("acct-1"), job);

                Assert.assertEquals(
                        "the correction must be repaired by key",
                        1,
                        job.keyedReplaySegmentCountForTest()
                );
                Assert.assertEquals(
                        "the seven untouched accounts' rows must be copied forward, not recomputed",
                        (ACCOUNTS - 1) * ROWS_PER_ACCOUNT_PER_DAY,
                        job.keyedReplayMergedRowsForTest()
                );
                TestUtils.assertEquals(
                        "an unaffected key's stored rows must survive the replacement unchanged",
                        before,
                        dumpUnaffectedRowsOnTheSecond("acct-1")
                );
                Assert.assertEquals(
                        "the block carries the segment's whole row set, so the view gains one row",
                        rowsBefore + 1,
                        count("select count() from lv")
                );
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testAKeyedRepairSurvivesARestartThroughItsOwnCheckpointRoots() throws Exception {
        // A keyed replay's state describes its own keys and no others, so the roots it
        // re-versions have to take the replayed entry for those and leave every other key's
        // exactly as the old root wrote it. Nothing detects a root that got that wrong at
        // the seal or at read time - the runtime still holds the truth - so the restore is
        // what reads it back.
        armKeyedReplay();
        assertMemoryLeak(() -> {
            createView(seedEightAccountsOverThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 1, "acct-1"), job);

                commit(correction("acct-1"), job);
                Assert.assertEquals(1, job.keyedReplaySegmentCountForTest());
                assertViewMatchesRecompute();
            }

            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
                assertRestoredFromTimeline("lv");
                driveRefreshToQuiescence(job);
                // The rows that read the restored accumulators back: one for a key the
                // keyed replay described and one for a key it did not.
                commit(row(5, 5, "acct-1") + ", " + row(5, 6, "acct-3"), job);
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testAKeyedRepairsRowPositionsSurviveASecondCorrectionBelowThem() throws Exception {
        // The ladder's cumulative positions are the thing a merged block can get wrong: a
        // boundary's position is the count of rows at or below it, and the replay emits
        // only some of them. A second correction below the first is what reads those
        // positions back, because it plans against the roots the first repair published.
        armKeyedReplay();
        assertMemoryLeak(() -> {
            createView(seedEightAccountsOverThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 1, "acct-1"), job);

                commit(row(3, 0, 30, 0, "acct-2"), job);
                commit(correction("acct-1"), job);

                Assert.assertEquals(
                        "both corrections must be repaired by key",
                        2,
                        job.keyedReplaySegmentCountForTest()
                );
                assertViewMatchesRecompute();
            }

            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
                assertRestoredFromTimeline("lv");
                driveRefreshToQuiescence(job);
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testAKeyedRepairWhoseStoredRowsFailToCloseAfterItsCommitRetiresTheTimeline() throws Exception {
        // The keyed replay's unwind closes the merge's stored rows after the replacement has
        // committed. A close that fails there skips the publication tail, so the splice that
        // corrects the positions of the roots above the correction never publishes, while the
        // committed replacement has already moved the view's row count under them. The unwind
        // therefore retires the timeline. Without that retire the retry replans against the
        // stale roots: its replacement is idempotent, its splice publishes a zero position
        // delta and clears the repair marker, and every root stays one row short with nothing
        // left to flag it. The next restart then fails the restore's row-count check and
        // rebuilds from the applied base, which the restatement guard refuses once the base
        // has lost history, and the view stops refreshing.
        //
        // A cold keyed head miss in the open segment retires the timeline on its retry anyway,
        // so only a closed segment's keyed repair depends on this retire. StoredRowCloseFault
        // says how the case makes the close fail.
        armKeyedReplay();
        final LiveViewOpenSegmentKeyedReplayTest.StoredRowCloseFault fault =
                new LiveViewOpenSegmentKeyedReplayTest.StoredRowCloseFault();
        assertMemoryLeak(fault, () -> {
            createView(seedEightAccountsOverThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 1, "acct-1"), job);
                final LiveViewInstance instance = viewInstance();

                fault.holdReaderOf(instance);
                try {
                    // Six commits, each an hour partition of the view's table the held reader
                    // never saw.
                    for (int hour = 2; hour < 8; hour++) {
                        commit(row(5, hour, "acct-2"), job);
                    }
                    fault.installOn(instance);
                    commit(correction("acct-1"), job);
                } finally {
                    fault.returnHeldReader();
                }

                fault.assertCloseFailedOnTheRemap();
                Assert.assertTrue(
                        "the correction must have been repaired by key",
                        job.keyedReplaySegmentCountForTest() > 0
                );
                Assert.assertEquals(
                        "a closed segment must not take the cold keyed route",
                        0,
                        job.openSegmentColdKeyedReplayCountForTest()
                );
                Assert.assertEquals(
                        "the failed close must cost exactly one refresh fault",
                        1,
                        instance.getRefreshFaultCount()
                );
                Assert.assertEquals(
                        "the pinned base reader must be back in the pool",
                        0,
                        engine.getBusyReaderCount()
                );
                // The view's rows cannot show a stale root: the runtime serves the same output
                // either way, until a restore reads the root's position back.
                assertLadderCountsRowsAtOrBelowEachBoundary("after the retry");
            }

            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
                assertRestoredFromTimeline("lv");
                driveRefreshToQuiescence(job);
                // Rows that read the restored accumulators back: one for the corrected account
                // and one for an account the correction never touched.
                commit(row(5, 8, "acct-1") + ", " + row(5, 9, "acct-3"), job);
                assertViewMatchesRecompute();
                assertLadderCountsRowsAtOrBelowEachBoundary("after the restart");
            }
        });
    }

    @Test
    public void testAnUnindexedKeyLeavesEverySegmentReadingWhole() throws Exception {
        // Without an index there is nothing to name one key's rows with, so the route is
        // not offered at all - and the repair is exactly the one this view has today.
        armKeyedReplay();
        assertMemoryLeak(() -> {
            createView(seedEightAccountsOverThreeDays(), false);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 1, "acct-1"), job);

                commit(correction("acct-1"), job);

                Assert.assertEquals(0, job.keyedReplaySegmentCountForTest());
                Assert.assertEquals(0, job.keyedReplayMergedRowsForTest());
                Assert.assertEquals(1, job.segmentRepairCountForTest());
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testTheKeyedRouteCanBeDeclined() throws Exception {
        // The switch, in the direction that is now the non-default one. The pricing says the
        // keyed read is the smaller one, and declining the route leaves the segment reading
        // whole anyway - which is what an operator turns off when a copied-forward row not
        // being recomputed from the base is not a trade they want.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_KEYED_SCAN_INDEX_OPEN_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_KEYED_REPLAY_ENABLED, "false");
        assertMemoryLeak(() -> {
            createView(seedEightAccountsOverThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 1, "acct-1"), job);

                commit(correction("acct-1"), job);

                Assert.assertEquals(
                        "the pricing still runs, and still says the keyed read is smaller",
                        1,
                        job.keyedScanCheaperCountForTest()
                );
                Assert.assertEquals(
                        "and nothing takes it, because the switch declines it",
                        0,
                        job.keyedReplaySegmentCountForTest()
                );
                Assert.assertEquals(0, job.keyedReplayMergedRowsForTest());
                Assert.assertEquals(1, job.segmentRepairCountForTest());
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testTwoCorrectionsInOneClosedSegmentFollowBothKeys() throws Exception {
        // Two of four accounts corrected in one day: half the day is replayed and half is
        // copied forward, and the two halves have to add up to the day.
        armKeyedReplay();
        assertMemoryLeak(() -> {
            createView(seedEightAccountsOverThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 1, "acct-1"), job);
                final long rowsBefore = count("select count() from lv");

                commit(correction("acct-1") + ", " + row(2, 0, 31, 0, "acct-2"), job);

                Assert.assertEquals(1, job.keyedReplaySegmentCountForTest());
                Assert.assertEquals(
                        "the six untouched accounts' rows are the ones copied forward",
                        (ACCOUNTS - 2) * ROWS_PER_ACCOUNT_PER_DAY,
                        job.keyedReplayMergedRowsForTest()
                );
                Assert.assertEquals(rowsBefore + 2, count("select count() from lv"));
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testAKeyBudgetOfZeroIsUnlimited() throws Exception {
        // server.conf documents a key budget at or below zero as unlimited, and the rows
        // bound discovery reads the same key that way. The change set's key collection has
        // to agree: a budget of zero must not collect nothing and quietly lose the keyed
        // route, which keyed.replay.enabled=false is the switch for.
        assertAKeyBudgetAtOrBelowZeroIsUnlimited(0);
    }

    @Test
    public void testANegativeKeyBudgetIsUnlimited() throws Exception {
        assertAKeyBudgetAtOrBelowZeroIsUnlimited(-1);
    }

    @Test
    public void testANegativeKeyBudgetBelowTheIntRangeIsUnlimited() throws Exception {
        // -(2^32 - 1): an (int) narrowing keeps its low 32 bits, which read as a budget of
        // one key, and the correction below carries two.
        assertAKeyBudgetAtOrBelowZeroIsUnlimited(-4_294_967_295L);
    }

    @Test
    public void testANarrowRepairAfterAWideOneClearsTablesSizedForItsOwnKeys() throws Exception {
        // The cost contract of the worker's keyed replay scratch. A wide repair - a
        // correction touching a whole key budget's worth of accounts - grows the three key
        // tables to hold its domain, and the worker's one replay serves every later keyed
        // repair of every view on it. The refresh job clears it at least twice per repaired
        // segment, keyed or not, and a clear sweeps the whole table, so a table left at the
        // wide repair's size charges every one of those clears for the widest domain the
        // worker ever armed. A narrow repair that follows must work on tables no larger than
        // the ones a fresh worker would build for the same keys.
        assertMemoryLeak(() -> {
            final ArrayColumnTypes checkpointKeyTypes = new ArrayColumnTypes();
            checkpointKeyTypes.add(ColumnType.STRING);
            final AccountSymbolTable symbols = new AccountSymbolTable();
            final SymbolTableCursor storedRows = new SymbolTableCursor(symbols);
            final IntList wideKeys = new IntList();
            for (int key = 0; key < WIDE_KEY_DOMAIN; key++) {
                wideKeys.add(key);
            }
            final IntList narrowKeys = new IntList();
            narrowKeys.add(1);
            narrowKeys.add(2);
            try (
                    LiveViewCheckpointKeyedReplay replay = new LiveViewCheckpointKeyedReplay();
                    LiveViewCheckpointKeyedReplay fresh = new LiveViewCheckpointKeyedReplay()
            ) {
                Assert.assertTrue(replay.arm(0, symbols, checkpointKeyTypes, wideKeys, true));
                Assert.assertTrue(replay.bindStoredRows(storedRows, 0, 1));
                final int[] wideSlots = keyTableSlots(replay);
                for (int i = 0; i < wideSlots.length; i++) {
                    Assert.assertTrue(
                            "the wide repair must have grown table " + i + " past its keys, or the case covers nothing",
                            wideSlots[i] > WIDE_KEY_DOMAIN
                    );
                }
                // The refresh job's sequence around one segment: the clear in the finally of
                // the wide segment, then the clear ahead of the next segment's gate.
                replay.closeStoredRows();
                replay.clear();
                replay.clear();

                Assert.assertTrue(replay.arm(0, symbols, checkpointKeyTypes, narrowKeys, false));
                Assert.assertTrue(replay.bindStoredRows(storedRows, 0, 1));
                Assert.assertTrue(fresh.arm(0, symbols, checkpointKeyTypes, narrowKeys, false));
                Assert.assertTrue(fresh.bindStoredRows(storedRows, 0, 1));
                // All three at once, so a failure names every table still at the wide size.
                Assert.assertEquals(
                        "a narrow repair after a wide one must work on tables sized for its own keys"
                                + " [storedSymbolKeys, outputKeys]",
                        Arrays.toString(keyTableSlots(fresh)),
                        Arrays.toString(keyTableSlots(replay))
                );
                // Whatever the tables were, the narrow repair's domain is its own.
                Assert.assertEquals(2, replay.getOutputKeys().size());
                Assert.assertEquals(2, replay.getBaseSymbolKeys().size());
                Assert.assertEquals(1, replay.getBaseSymbolKeys().getQuick(0));
                Assert.assertEquals(2, replay.getBaseSymbolKeys().getQuick(1));
                replay.closeStoredRows();
                fresh.closeStoredRows();
            }
        });
    }

    @Test
    public void testArmingAWideDomainAllocatesNoHeapPerKey() throws Exception {
        // Arming resolves every key of Q into the encoding a checkpoint root keys by, once
        // per keyed segment repair and up to the scan key budget wide. A String or an
        // encoded array per member charges each of those repairs heap in proportion to the
        // domain, so the measured arms must come out the same size whatever the key count.
        assertMemoryLeak(() -> {
            final ArrayColumnTypes checkpointKeyTypes = new ArrayColumnTypes();
            checkpointKeyTypes.add(ColumnType.STRING);
            final PrecomputedAccountSymbolTable symbols = new PrecomputedAccountSymbolTable(ARM_KEY_DOMAIN);
            final IntList keys = new IntList();
            for (int key = 0; key < ARM_KEY_DOMAIN; key++) {
                keys.add(key);
            }
            try (
                    TestUtils.ThreadMetricsScope<ThreadMXBean> scope = TestUtils.threadAllocationScope();
                    LiveViewCheckpointKeyedReplay replay = new LiveViewCheckpointKeyedReplay()
            ) {
                final ThreadMXBean threadMXBean = scope.getBean();
                // Two warm-up arms, so the lists the domain reuses have reached its width
                // and the class paths are resolved before the measured window.
                Assert.assertTrue(replay.arm(0, symbols, checkpointKeyTypes, keys, true));
                Assert.assertTrue(replay.arm(0, symbols, checkpointKeyTypes, keys, true));

                final long threadId = Thread.currentThread().threadId();
                final long before = threadMXBean.getThreadAllocatedBytes(threadId);
                for (int round = 0; round < ARM_ROUNDS; round++) {
                    Assert.assertTrue(replay.arm(0, symbols, checkpointKeyTypes, keys, true));
                }
                final long allocated = threadMXBean.getThreadAllocatedBytes(threadId) - before;

                Assert.assertEquals("every key plus the null one", ARM_KEY_DOMAIN + 1, replay.getOutputKeys().size());
                Assert.assertTrue(
                        ARM_ROUNDS + " arms of a " + ARM_KEY_DOMAIN + "-key domain allocated " + allocated
                                + " heap bytes; arming must not allocate per key",
                        allocated < ARM_ALLOCATION_LIMIT_BYTES
                );
            }
        });
    }

    @Test
    public void testAStoredRowCloseThatFailsStillReleasesTheMerge() throws Exception {
        // The refresh job closes the merge's stored rows on its unwind, ahead of frees it must
        // not skip, and that close can fail: the cursor hands a pooled reader of the view's table
        // back, and a reader the table outgrew reloads its txn file as it goes passive. The merge
        // drops its hold on the cursor anyway, so it never points at a cursor that is half
        // closed, and the failure still reaches the caller.
        assertMemoryLeak(() -> {
            final ArrayColumnTypes checkpointKeyTypes = new ArrayColumnTypes();
            checkpointKeyTypes.add(ColumnType.STRING);
            final ListSymbolTable symbols = new ListSymbolTable("acct-1", "acct-2");
            final IntList keys = new IntList();
            keys.add(0);
            final SymbolTableCursor storedRows = new SymbolTableCursor(symbols) {
                @Override
                public void close() {
                    throw CairoException.critical(0).put("could not remap file");
                }
            };
            try (LiveViewCheckpointKeyedReplay replay = new LiveViewCheckpointKeyedReplay()) {
                Assert.assertTrue(replay.arm(0, symbols, checkpointKeyTypes, keys, false));
                Assert.assertTrue(replay.bindStoredRows(storedRows, 0, 1));
                try {
                    replay.closeStoredRows();
                    Assert.fail("the stored rows' close failure must reach the caller");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "could not remap file");
                }
                Assert.assertNull(
                        "the merge must not keep the cursor whose close failed",
                        fieldOf(LiveViewCheckpointKeyedReplay.class, "storedRowCursor", replay)
                );
            }
        });
    }

    @Test
    public void testBindingStoredRowsResolvesEveryKeyThroughTheViewsOwnMap() throws Exception {
        // The merge drops a stored row whose key the replay recomputes, and the view's rows
        // carry the view's own symbol ids rather than the base's. Binding therefore resolves
        // every value of Q in the view's map, and a value the view never stored is simply
        // absent there. Non-ASCII values have to resolve exactly like ASCII ones - a decode
        // that mangled a UTF-16 unit would silently keep a superseded row - and the null key
        // resolves to the null id on both sides.
        assertMemoryLeak(() -> {
            final ArrayColumnTypes checkpointKeyTypes = new ArrayColumnTypes();
            checkpointKeyTypes.add(ColumnType.STRING);
            final ListSymbolTable baseSymbols = new ListSymbolTable(
                    "acct-1", "東京", "zürich", "ÿĀ￿", "pair-😀", "", "only-in-base"
            );
            // The view's map names the same strings under other ids, in another order, and
            // does not name the base's last value at all.
            final ListSymbolTable storedSymbols = new ListSymbolTable(
                    "x", "pair-😀", "", "zürich", "ÿĀ￿", "東京", "acct-1"
            );
            final IntList keys = new IntList();
            for (int key = 0; key < 7; key++) {
                keys.add(key);
            }
            try (LiveViewCheckpointKeyedReplay replay = new LiveViewCheckpointKeyedReplay()) {
                for (int pass = 0; pass < 2; pass++) {
                    final boolean hasNullKey = pass == 0;
                    Assert.assertTrue(replay.arm(0, baseSymbols, checkpointKeyTypes, keys, hasNullKey));
                    Assert.assertTrue(replay.bindStoredRows(new SymbolTableCursor(storedSymbols), 0, 1));
                    final IntHashSet stored =
                            (IntHashSet) fieldOf(LiveViewCheckpointKeyedReplay.class, "storedSymbolKeys", replay);
                    Assert.assertEquals(hasNullKey ? 7 : 6, stored.size());
                    Assert.assertEquals(hasNullKey, stored.contains(SymbolTable.VALUE_IS_NULL));
                    for (int storedKey = 1; storedKey < 7; storedKey++) {
                        Assert.assertTrue(
                                "stored id " + storedKey + " (" + storedSymbols.valueOf(storedKey) + ") must resolve",
                                stored.contains(storedKey)
                        );
                    }
                    Assert.assertFalse("a value the base never carried must not resolve", stored.contains(0));
                    Assert.assertEquals(hasNullKey ? 8 : 7, replay.getOutputKeys().size());
                    replay.closeStoredRows();
                }
            }
        });
    }

    @Test
    public void testClearingAFewWideKeysGivesTheirStorageBack() throws Exception {
        // The worker's one keyed replay keeps Q's storage from repair to repair. A bound on
        // the key count alone would let a few wide keys pin an arena of any size on the
        // worker for good: a thousand 16,000-character keys take 32 MiB, and every later
        // repair within the count would clear that arena and keep it. A clear past the
        // retained key bytes therefore frees Q's storage, as a clear past the key count
        // does, and a domain of ordinary keys still keeps its storage for the next repair.
        assertMemoryLeak(() -> {
            final ArrayColumnTypes checkpointKeyTypes = new ArrayColumnTypes();
            checkpointKeyTypes.add(ColumnType.STRING);
            // Each key's UTF-16 image is a quarter of the byte bound, so together they pass
            // it twice over while the count stays far below the key bound.
            final String padding = "x".repeat((int) (LiveViewCheckpointOutputKeyDomain.MAX_RETAINED_KEY_BYTES / FEW_WIDE_KEYS));
            final ObjList<String> wideValues = new ObjList<>();
            final IntList wideKeys = new IntList();
            for (int key = 0; key < FEW_WIDE_KEYS; key++) {
                wideValues.add(ACCOUNT_PREFIX + key + '-' + padding);
                wideKeys.add(key);
            }
            final ListSymbolTable wideSymbols = new ListSymbolTable(wideValues);
            final ListSymbolTable narrowSymbols = new ListSymbolTable("acct-1", "acct-2");
            final IntList narrowKeys = new IntList();
            narrowKeys.add(0);
            narrowKeys.add(1);
            try (LiveViewCheckpointKeyedReplay replay = new LiveViewCheckpointKeyedReplay()) {
                final long baseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_IN_MEM);
                Assert.assertTrue(replay.arm(0, wideSymbols, checkpointKeyTypes, wideKeys, true));
                Assert.assertTrue(replay.bindStoredRows(new SymbolTableCursor(wideSymbols), 0, 1));
                Assert.assertEquals("every key plus the null one", FEW_WIDE_KEYS + 1, replay.getOutputKeys().size());
                Assert.assertTrue(
                        "the keys must pass the byte bound, or the case covers nothing",
                        Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_IN_MEM) - baseline
                                > LiveViewCheckpointOutputKeyDomain.MAX_RETAINED_KEY_BYTES
                );
                // The refresh job's order: the merge's cursor goes first, the clear follows.
                replay.closeStoredRows();
                replay.clear();
                Assert.assertEquals(
                        "a clear past the retained key bytes must free Q's storage",
                        baseline,
                        Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_IN_MEM)
                );
                Assert.assertEquals(0, replay.getOutputKeys().getSlotCount());

                // The next repair builds its domain afresh, and an ordinary one keeps its
                // storage across the clear: that reuse is what the bounds are there for.
                Assert.assertTrue(replay.arm(0, narrowSymbols, checkpointKeyTypes, narrowKeys, false));
                Assert.assertTrue(replay.bindStoredRows(new SymbolTableCursor(narrowSymbols), 0, 1));
                final IntHashSet stored =
                        (IntHashSet) fieldOf(LiveViewCheckpointKeyedReplay.class, "storedSymbolKeys", replay);
                Assert.assertEquals(2, stored.size());
                Assert.assertTrue(stored.contains(0));
                Assert.assertTrue(stored.contains(1));
                replay.closeStoredRows();
                final long narrowBytes = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_IN_MEM) - baseline;
                Assert.assertTrue(narrowBytes > 0);
                replay.clear();
                Assert.assertEquals(
                        "a clear within both bounds must keep Q's storage for the next repair",
                        narrowBytes,
                        Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_IN_MEM) - baseline
                );
                Assert.assertEquals(0, replay.getOutputKeys().size());
            }
        });
    }

    @Test
    public void testClearingAWideDomainAllocatesNoHeap() throws Exception {
        // The refresh job clears its keyed replay on cleanup chains, ahead of frees those
        // chains must not skip: the head-miss prologue's unwind clears it right before it
        // frees the staged timeline capture. Past the retained-key bound a clear starts the
        // key tables over, and an allocation there that fails - a heap OutOfMemoryError -
        // would unwind past the capture's free and strand its native memory. A clear
        // therefore drops the wide tables and allocates nothing, and the next arm allocates
        // the stored-key set its merge fills.
        assertMemoryLeak(() -> {
            final ArrayColumnTypes checkpointKeyTypes = new ArrayColumnTypes();
            checkpointKeyTypes.add(ColumnType.STRING);
            final PrecomputedAccountSymbolTable symbols = new PrecomputedAccountSymbolTable(ARM_KEY_DOMAIN);
            final SymbolTableCursor storedRows = new SymbolTableCursor(symbols);
            final IntList keys = new IntList();
            for (int key = 0; key < ARM_KEY_DOMAIN; key++) {
                keys.add(key);
            }
            try (
                    TestUtils.ThreadMetricsScope<ThreadMXBean> scope = TestUtils.threadAllocationScope();
                    LiveViewCheckpointKeyedReplay replay = new LiveViewCheckpointKeyedReplay()
            ) {
                final ThreadMXBean threadMXBean = scope.getBean();
                long minAllocated = Long.MAX_VALUE;
                for (int round = 0; round < ARM_ROUNDS; round++) {
                    // Both key tables past the bound, then the refresh job's order: the
                    // merge's cursor goes first and the clear follows it.
                    Assert.assertTrue(replay.arm(0, symbols, checkpointKeyTypes, keys, true));
                    Assert.assertTrue(replay.bindStoredRows(storedRows, 0, 1));
                    replay.closeStoredRows();
                    final long before = threadMXBean.getCurrentThreadAllocatedBytes();
                    replay.clear();
                    minAllocated = Math.min(minAllocated, threadMXBean.getCurrentThreadAllocatedBytes() - before);
                    Assert.assertFalse(replay.isArmed());
                    Assert.assertEquals(0, replay.getOutputKeys().size());
                }
                Assert.assertEquals(
                        "clearing a domain past the retained-key bound must allocate no heap",
                        0,
                        minAllocated
                );

                // A second clear finds nothing left to drop, and the next arm provides the
                // stored-key set its merge fills.
                replay.clear();
                final IntList narrowKeys = new IntList();
                narrowKeys.add(1);
                Assert.assertTrue(replay.arm(0, symbols, checkpointKeyTypes, narrowKeys, false));
                Assert.assertTrue(replay.bindStoredRows(storedRows, 0, 1));
                final IntHashSet stored =
                        (IntHashSet) fieldOf(LiveViewCheckpointKeyedReplay.class, "storedSymbolKeys", replay);
                Assert.assertEquals(1, stored.size());
                Assert.assertTrue(stored.contains(1));
                replay.closeStoredRows();
            }
        });
    }

    @Test
    public void testClosingAReplayWhoseStoredRowsFailToCloseStillFreesItsKeys() throws Exception {
        // A parked keyed repair's session owns a replay of its own, and discarding the session
        // closes that replay while its merge may still hold the stored rows. Their close hands
        // a pooled reader of the view's table back, and a reader close can fail for any remap
        // or I/O reason; this case injects one. The replay frees Q's native storage anyway,
        // since nothing else owns it, and the close failure still reaches the caller.
        assertMemoryLeak(() -> {
            final ArrayColumnTypes checkpointKeyTypes = new ArrayColumnTypes();
            checkpointKeyTypes.add(ColumnType.STRING);
            final ListSymbolTable symbols = new ListSymbolTable("acct-1", "acct-2");
            final IntList keys = new IntList();
            keys.add(0);
            keys.add(1);
            final AtomicInteger closeCount = new AtomicInteger();
            final SymbolTableCursor storedRows = new SymbolTableCursor(symbols) {
                @Override
                public void close() {
                    closeCount.incrementAndGet();
                    throw CairoException.critical(0).put("could not remap file");
                }
            };
            final LiveViewCheckpointKeyedReplay replay = new LiveViewCheckpointKeyedReplay();
            Assert.assertTrue(replay.arm(0, symbols, checkpointKeyTypes, keys, false));
            Assert.assertTrue(replay.bindStoredRows(storedRows, 0, 1));
            Assert.assertTrue(replay.getOutputKeys().getSlotCount() > 0);
            try {
                replay.close();
                Assert.fail("the stored rows' close failure must reach the caller");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "could not remap file");
            }
            Assert.assertEquals(1, closeCount.get());
            Assert.assertEquals(
                    "a close must free Q's storage even when the stored rows fail to close",
                    0,
                    replay.getOutputKeys().getSlotCount()
            );
            Assert.assertFalse(replay.isArmed());
            Assert.assertNull(
                    "the merge must not keep the cursor whose close failed",
                    fieldOf(LiveViewCheckpointKeyedReplay.class, "storedRowCursor", replay)
            );
            // The failed cursor is gone, so a second close has nothing left to fail on.
            replay.close();
            Assert.assertEquals(1, closeCount.get());
        });
    }

    /**
     * Turns the keyed route on, and prices one index open at one base row.
     * <p>
     * The default prices it at 256, which is what a real hourly-partitioned base against a
     * daily anchor segment is worth - and at forty rows a day it would (correctly) prefer
     * the whole segment whatever the key domain. What these cases are about is the replay
     * and its merge, so the setup term is priced at the scale the fixture actually has.
     */
    private void armKeyedReplay() {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_KEYED_SCAN_INDEX_OPEN_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_KEYED_REPLAY_ENABLED, "true");
        // The merged publication, which is what this class covers: the replay recomputes the
        // affected keys and copies every other key's stored row forward into the same
        // REPLACE_RANGE. A view carrying the sparse identity takes the other publication -
        // it commits only the recomputed rows and copies none - so the rows this class counts
        // would be reported as kept rather than merged. LiveViewSparsePublicationTest owns
        // that route.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SPARSE_PUBLICATION_ENABLED, "false");
    }

    /**
     * Corrects two of the eight accounts inside one closed day under {@code keyBudget}, and
     * holds the repair to the keyed route and the view to a from-base recompute.
     */
    private void assertAKeyBudgetAtOrBelowZeroIsUnlimited(long keyBudget) throws Exception {
        armKeyedReplay();
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SCAN_MAX_KEYS, keyBudget);
        assertMemoryLeak(() -> {
            createView(seedEightAccountsOverThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 1, "acct-1"), job);
                final long rowsBefore = count("select count() from lv");

                commit(correction("acct-1") + ", " + row(2, 0, 31, 0, "acct-2"), job);

                Assert.assertEquals(
                        "a key budget of " + keyBudget + " must leave the day repaired by key",
                        1,
                        job.keyedReplaySegmentCountForTest()
                );
                Assert.assertEquals(
                        "the six untouched accounts' rows are the ones copied forward",
                        (ACCOUNTS - 2) * ROWS_PER_ACCOUNT_PER_DAY,
                        job.keyedReplayMergedRowsForTest()
                );
                Assert.assertEquals(1, job.segmentRepairCountForTest());
                Assert.assertEquals(rowsBefore + 2, count("select count() from lv"));
                assertViewMatchesRecompute();
            }
        });
    }

    /**
     * Applies a key-changing replacement to a deduplicating base inside the window a refresh
     * turn opens between vouching for its drained range and pinning the reader its repair
     * plans against, and holds the view to a from-base recompute.
     * <p>
     * The turn takes the raw-WAL drain only because the apply signal proves nothing deduped
     * over the range it drains. The repair then pins whatever the apply has reached, and the
     * change-set walk classifies every commit up to that pin - including a replacement the
     * signal never vouched for. The walk reads the incoming row's key and not the key of the
     * row the replacement displaced, so a keyed replay recomputes the new key and copies the
     * displaced key's stale row forward, or leaves it standing under a sparse publication.
     * <p>
     * The injection is synchronous on the refresh thread: it fires on the turn's first
     * acquisition of the view's WAL writer, which the raw-WAL drain takes after the range
     * check and before the repair's pin. That the turn really took the raw-WAL drain is
     * asserted rather than assumed - a replacement applied before the check would fail it
     * and route the turn through the applied base, where no keyed route exists and the case
     * would cover nothing.
     */
    private void assertDedupReplacementBehindTheCleanRangeCheckIsRepairedWhole(
            boolean isSparse,
            String whereClause,
            String replacementRow,
            String replacedTimestamp,
            String expectedRowsAtReplacedTimestamp
    ) throws Exception {
        armKeyedReplay();
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SPARSE_PUBLICATION_ENABLED, isSparse ? "true" : "false");
        assertMemoryLeak(() -> {
            createView(seedEightAccountsOverThreeDays(), true, " dedup upsert keys(created_at)", whereClause);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 1, "acct-1"), job);
                final LiveViewInstance instance = viewInstance();

                // Applied before the turn, so the apply signal covers the range it drains.
                execute("insert into tx values " + correction("acct-1"));
                drainWalQueue();
                final long cleanCyclesBefore = instance.getDedupRawWalCleanCycles();

                final AtomicBoolean isInjectionArmed = new AtomicBoolean(true);
                final AtomicReference<Throwable> injectionError = new AtomicReference<>();
                engine.setPoolListener((factoryType, thread, tableToken, event, segment, position) -> {
                    if (factoryType == PoolListener.SRC_WAL_WRITER
                            && (event == PoolListener.EV_GET || event == PoolListener.EV_CREATE)
                            && "lv".equals(tableToken.getTableName())
                            && isInjectionArmed.compareAndSet(true, false)) {
                        try {
                            execute("insert into tx values " + replacementRow);
                            drainWalQueue();
                        } catch (Throwable t) {
                            injectionError.set(t);
                        }
                    }
                });
                try {
                    driveRefreshToQuiescence(job);
                } finally {
                    engine.setPoolListener(null);
                }
                if (injectionError.get() != null) {
                    throw new AssertionError("the injected replacement failed", injectionError.get());
                }
                Assert.assertFalse("the replacement must land inside the refresh turn", isInjectionArmed.get());
                Assert.assertTrue(
                        "the turn must take the raw-WAL drain, which puts the replacement behind its range check",
                        instance.getDedupRawWalCleanCycles() > cleanCyclesBefore
                );
                Assert.assertTrue(
                        "the correction must reach the per-segment repair, or the case covers nothing",
                        job.segmentRepairCountForTest() > 0
                );

                assertQuery("SELECT created_at, account_id, cumulative_sum FROM lv WHERE created_at = '" + replacedTimestamp + "'")
                        .noLeakCheck()
                        .timestamp("created_at")
                        .returns(expectedRowsAtReplacedTimestamp);
                assertViewMatchesRecompute("lv", whereClause);
                Assert.assertEquals(
                        "a deduplicating base must keep every closed segment on the whole-segment read",
                        0,
                        job.keyedReplaySegmentCountForTest()
                );
            }
        });
    }

    private void assertViewMatchesRecompute() throws Exception {
        assertViewMatchesRecompute("lv");
    }

    private void assertViewMatchesRecompute(String viewName) throws Exception {
        assertViewMatchesRecompute(viewName, "");
    }

    private void assertViewMatchesRecompute(String viewName, String whereClause) throws Exception {
        final String bucket = "timestamp_floor('1d', created_at, '1970-01-01T00:00:00.000000Z'::timestamp)";
        final String recompute = "select created_at, account_id, "
                + "sum(amount) over (partition by account_id, bucket order by created_at "
                + "rows between unbounded preceding and current row) as cumulative_sum "
                + "from (select created_at, account_id, amount, " + bucket + " as bucket from tx" + whereClause + ")";
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(" + recompute + ") order by 2, 1",
                "(" + viewName + ") order by 2, 1",
                LOG,
                true
        );
        assertNoRefreshFaults(viewName);
    }

    private void commit(String values, LiveViewRefreshJob job) throws Exception {
        execute("insert into tx values " + values);
        drainWalQueue();
        driveRefreshToQuiescence(job);
    }

    private long count(String sql) throws Exception {
        try (
                RecordCursorFactory factory = select(sql);
                RecordCursor cursor = factory.getCursor(sqlExecutionContext)
        ) {
            Assert.assertTrue(cursor.hasNext());
            return cursor.getRecord().getLong(0);
        }
    }

    private void createView(String seedRows) throws Exception {
        createView(seedRows, true);
    }

    private void createView(String seedRows, boolean isKeyIndexed) throws Exception {
        createView(seedRows, isKeyIndexed, "", "");
    }

    /**
     * @param dedupClause the base table's DEDUP clause, or empty for an append-only base
     * @param whereClause the view's WHERE clause, or empty for an unfiltered view
     */
    private void createView(String seedRows, boolean isKeyIndexed, String dedupClause, String whereClause) throws Exception {
        execute("create table tx (created_at timestamp, account_id symbol nocache"
                + (isKeyIndexed ? " index capacity 8" : "") + ", "
                + "amount double) timestamp(created_at) partition by hour wal" + dedupClause);
        execute("insert into tx values " + seedRows);
        drainWalQueue();
        execute("create live view lv flush every 100ms start from beginning as "
                + "select created_at, account_id, sum(amount) over w as cumulative_sum "
                + "from tx" + whereClause + " window w as (partition by account_id order by created_at anchor daily '00:00')");
    }

    /**
     * The view's stored rows for 2026-01-02 that the correction does not touch, as text.
     * This is the image a keyed repair must leave byte for byte where it found it.
     */
    private String dumpUnaffectedRowsOnTheSecond(String correctedAccount) throws Exception {
        return TestUtils.printSqlToString(
                engine,
                sqlExecutionContext,
                "select * from lv where created_at >= '2026-01-02T00:00:00.000000Z'::timestamp"
                        + " and created_at < '2026-01-03T00:00:00.000000Z'::timestamp"
                        + " and account_id != '" + correctedAccount + "' order by 2, 1",
                new StringSink()
        );
    }

    /**
     * Holds every timeline boundary to the number of live-view rows at or below its own
     * timestamp, read off the published ladder and off the table it describes.
     */
    private void assertLadderCountsRowsAtOrBelowEachBoundary(String stage) throws Exception {
        final LongList ladder = snapshotCheckpointLadder(viewInstance());
        Assert.assertTrue(stage + ": the view must have sealed a ladder to check", ladder.size() > 0);
        for (int i = 0, n = ladder.size() / 2; i < n; i++) {
            final long maxTimestamp = ladder.getQuick(i * 2);
            Assert.assertEquals(
                    stage + ": boundary " + i + " at " + maxTimestamp
                            + " must count the rows at or below it",
                    count("select count() from lv where created_at <= " + maxTimestamp + "::timestamp"),
                    ladder.getQuick(i * 2 + 1)
            );
        }
    }

    private void restartCycle() {
        engine.getLiveViewRegistry().clear();
        engine.buildViewGraphs();
    }

    /**
     * One row of {@code account} at {@code hour}:{@code minute} on 2026-01-{@code day}, as
     * an INSERT tuple. The day is what carries the case: with a daily anchor it is also the
     * segment.
     */
    private String row(int day, int hour, String account) {
        return row(day, hour, 0, 0, account);
    }

    private String row(int day, int hour, int minute, int second, String account) {
        return "('2026-01-" + String.format("%02d", day) + "T" + String.format("%02d", hour)
                + ":" + String.format("%02d", minute) + ":" + String.format("%02d", second)
                + ".000000Z', '" + account + "', 1.0)";
    }

    /**
     * The same correction on the null account, which both indexes name under a key of
     * their own.
     */
    private String nullCorrection() {
        return "('2026-01-02T00:30:00.000000Z', null, 1.0)";
    }

    /**
     * One correction of {@code account} on 2026-01-02, below every row that day already
     * holds - which is what puts the segment's stored rows inside the range the
     * replacement deletes, and so in front of the merge.
     */
    private String correction(String account) {
        return row(2, 0, 30, 0, account);
    }

    /**
     * One correction of every seeded account on 2026-01-{@code day}, at 00:30 like
     * {@link #correction}. Eight keys against eighty rows is what makes the cost model
     * price the day's whole-segment read below its keyed one, which is the only way to get
     * a segment that both reads whole and may therefore park.
     */
    private String correctionOnEveryAccount(int day) {
        final StringBuilder rows = new StringBuilder();
        for (int account = 1; account <= ACCOUNTS; account++) {
            if (rows.length() > 0) {
                rows.append(", ");
            }
            rows.append(row(day, 0, 30, account, "acct-" + account));
        }
        return rows.toString();
    }

    /**
     * Ten rows of each of eight accounts on each of 2026-01-02, 2026-01-03 and 2026-01-04,
     * all inside the 01:00 hour of their day.
     * <p>
     * Two things about the shape carry the cases. A correction touching one account leaves
     * seven eighths of the segment for the merge to copy forward, and the whole day sits in
     * one partition, so the per-key-per-frame setup does not dominate the comparison at
     * this scale the way it does on a real hourly-partitioned base. And every seeded row
     * sits ABOVE the correction {@link #correction} lands at, so the replacement's floor is
     * below the lot and the merge really has the day's rows in front of it - a correction
     * above them would leave the segment's stored rows outside the replaced range and the
     * merge with nothing to do.
     */
    /**
     * One row of each of the eight accounts at 01:{@code minute}:0a on 2026-01-02, account
     * {@code a} on second {@code a}, as one INSERT's tuples.
     */
    private String eightAccountsOnTheSecond(int minute) {
        final StringBuilder rows = new StringBuilder();
        for (int account = 1; account <= ACCOUNTS; account++) {
            if (rows.length() > 0) {
                rows.append(", ");
            }
            rows.append(row(2, 1, minute, account, "acct-" + account));
        }
        return rows.toString();
    }

    private String seedEightAccountsOverThreeDays() {
        final StringBuilder rows = new StringBuilder();
        for (int day = 2; day <= 4; day++) {
            for (int i = 0; i < ROWS_PER_ACCOUNT_PER_DAY; i++) {
                for (int account = 1; account <= ACCOUNTS; account++) {
                    if (rows.length() > 0) {
                        rows.append(", ");
                    }
                    final int offset = i * ACCOUNTS + account;
                    rows.append(row(day, 1, offset / 60, offset % 60, "acct-" + account));
                }
            }
        }
        return rows.toString();
    }

    /**
     * Ten rows of the null account on each of the three seeded days, in the 02:00 hour so
     * they sit above {@link #nullCorrection} and inside the same anchor segment.
     */
    private String seedNullAccountOverThreeDays() {
        final StringBuilder rows = new StringBuilder();
        for (int day = 2; day <= 4; day++) {
            for (int i = 0; i < ROWS_PER_ACCOUNT_PER_DAY; i++) {
                if (rows.length() > 0) {
                    rows.append(", ");
                }
                rows.append("('2026-01-0").append(day).append("T02:00:")
                        .append(String.format("%02d", i)).append(".000000Z', null, 1.0)");
            }
        }
        return rows.toString();
    }

    /**
     * One commit of eight accounts at {@code 2026-01-02T01:mm:00}, a unit amount each.
     */
    private static String eightAccountsAtMinute(int minute) {
        final StringBuilder rows = new StringBuilder();
        for (int account = 1; account <= ACCOUNTS; account++) {
            if (rows.length() > 0) {
                rows.append(", ");
            }
            rows.append("('2026-01-02T01:").append(String.format("%02d", minute))
                    .append(":00.000000Z', 'acct-").append(account).append("', 1.0)");
        }
        return rows.toString();
    }

    /**
     * Asserts one account's cumulative sum at one timestamp, read from the view itself.
     * <p>
     * The oracle comparison covers the whole result; this names the two rows the reported
     * defect actually moved, so the case still says what it is about when read on its own.
     */
    private void assertAccountSum(String account, String timestamp, double expected) throws Exception {
        try (
                RecordCursorFactory factory = select(
                        "select cumulative_sum from lv where account_id = '" + account
                                + "' and created_at = '" + timestamp + "'"
                );
                RecordCursor cursor = factory.getCursor(sqlExecutionContext)
        ) {
            Assert.assertTrue(account + " must have a row at " + timestamp, cursor.hasNext());
            Assert.assertEquals(
                    account + " at " + timestamp,
                    expected,
                    cursor.getRecord().getDouble(0),
                    1e-9
            );
            Assert.assertFalse("one row per account per timestamp", cursor.hasNext());
        }
    }

    /**
     * Asserts every checkpoint boundary the repair left behind names all eight accounts.
     * <p>
     * Read off the published roots rather than off the view: the live runtime holds the
     * corrected state either way, so only the entries on disk can say whether a key
     * outside the correction's output-key domain kept what the predecessor held for it.
     * That is the exact invariant the removed anchor-root builder broke - it treated a
     * keyed capture as a complete snapshot and removed every key the capture did not put.
     * <p>
     * All ten minute boundaries survive the correction as re-versioned roots, because the
     * keyed repair splices its key domain into them rather than retiring the interval. That
     * splice is only permitted over roots the running build can build on, which is what
     * {@code isKeyDomainSpliceable} decides - and deciding it from the storage plan rather
     * than the runtime one is what keeps it correct with map fusion off.
     */
    private void assertEveryRepairedBoundaryKeepsAllAccounts() {
        final LiveViewInstance instance = viewInstance();
        int boundariesRead = 0;
        try (
                Path dir = checkpointsDir(instance);
                LiveViewCheckpointMetaStore store = new LiveViewCheckpointMetaStore(configuration)
        ) {
            store.of(dir);
            Assert.assertTrue(store.isValid());
            try (
                    LiveViewCheckpointGenerationPin pin = store.pin();
                    LiveViewCheckpointTimelineReader timeline = new LiveViewCheckpointTimelineReader(configuration);
                    LiveViewCheckpointRoot root = new LiveViewCheckpointRoot(configuration);
                    LiveViewCheckpointWindowRoot windowRoot = new LiveViewCheckpointWindowRoot(configuration);
                    LiveViewCheckpointPartitionMapReader partitions =
                            new LiveViewCheckpointPartitionMapReader(configuration)
            ) {
                timeline.of(dir);
                partitions.of(dir);
                final LiveViewCheckpointPageRef stateRootRef = new LiveViewCheckpointPageRef();
                final LiveViewCheckpointPageRef mapRootRef = new LiveViewCheckpointPageRef();
                final int[] read = {0};
                // The boundaries inside the repaired anchor day, from the first commit that
                // carried all eight accounts. The seed boundary below it legitimately holds
                // one key - acct-1 was the only account that existed then - and the January 3
                // boundary above the day was never in the repaired interval.
                timeline.range(
                        pin.getTimelineRootRef(),
                        ts("2026-01-02T01:00:00.000000Z"),
                        ts("2026-01-03T00:00:00.000000Z"),
                        entry -> {
                            root.of(dir, entry.rootRef);
                            root.getStateRootRef(stateRootRef);
                            Assert.assertFalse(
                                    "an anchored boundary always has a state root",
                                    stateRootRef.isNull()
                            );
                            Assert.assertTrue(
                                    "every anchored boundary must carry a window root",
                                    windowRoot.ofIfWindowRoot(dir, stateRootRef)
                            );
                            windowRoot.getPartitionMapRootRef(mapRootRef);
                            Assert.assertEquals(
                                    "boundary at " + entry.maxTimestamp + " must keep every account,"
                                            + " including the ones the correction never touched",
                                    ACCOUNTS,
                                    partitions.size(mapRootRef)
                            );
                            read[0]++;
                        }
                );
                boundariesRead = read[0];
                Assert.assertEquals(
                        "all ten minute boundaries must survive the repair as re-versioned roots",
                        10,
                        boundariesRead
                );
            }
        }
    }

    private static Path checkpointsDir(LiveViewInstance instance) {
        return new Path().of(configuration.getDbRoot())
                .concat(instance.getLiveViewToken())
                .concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
    }

    private static Object fieldOf(Class<?> owner, String name, Object target) throws ReflectiveOperationException {
        final Field field = owner.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(target);
    }

    /**
     * The slot counts of the replay's two key tables - storedSymbolKeys and outputKeys -
     * which is what each clear of that table sweeps. Read afresh on every call so that an
     * assertion never stands on a table the replay has since replaced.
     */
    private static int[] keyTableSlots(LiveViewCheckpointKeyedReplay replay) throws ReflectiveOperationException {
        final Object storedSymbolKeys = fieldOf(LiveViewCheckpointKeyedReplay.class, "storedSymbolKeys", replay);
        return new int[]{
                ((int[]) fieldOf(AbstractIntHashSet.class, "keys", storedSymbolKeys)).length,
                replay.getOutputKeys().getSlotCount()
        };
    }

    private LiveViewInstance viewInstance() {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull("live view 'lv' must be registered", instance);
        return instance;
    }

    /**
     * A symbol map naming key {@code n} {@code acct-n}, for both the base reader a keyed
     * replay arms against and the view's own map its merge resolves the same values in.
     */
    private static final class AccountSymbolTable implements StaticSymbolTable {
        @Override
        public boolean containsNullValue() {
            return false;
        }

        @Override
        public int getSymbolCount() {
            return WIDE_KEY_DOMAIN;
        }

        @Override
        public int keyOf(CharSequence value) {
            if (value == null) {
                return SymbolTable.VALUE_IS_NULL;
            }
            try {
                final int key = Numbers.parseInt(value, ACCOUNT_PREFIX.length(), value.length());
                return key < WIDE_KEY_DOMAIN ? key : SymbolTable.VALUE_NOT_FOUND;
            } catch (NumericException e) {
                return SymbolTable.VALUE_NOT_FOUND;
            }
        }

        @Override
        public CharSequence valueBOf(int key) {
            return valueOf(key);
        }

        @Override
        public CharSequence valueOf(int key) {
            return key > -1 && key < WIDE_KEY_DOMAIN ? ACCOUNT_PREFIX + key : null;
        }
    }

    /**
     * A symbol map naming each key by the value at its index, and the null key by null.
     */
    private static final class ListSymbolTable implements StaticSymbolTable {
        private final ObjList<String> values = new ObjList<>();

        private ListSymbolTable(String... values) {
            for (String value : values) {
                this.values.add(value);
            }
        }

        private ListSymbolTable(ObjList<String> values) {
            this.values.addAll(values);
        }

        @Override
        public boolean containsNullValue() {
            return false;
        }

        @Override
        public int getSymbolCount() {
            return values.size();
        }

        @Override
        public int keyOf(CharSequence value) {
            if (value == null) {
                return SymbolTable.VALUE_IS_NULL;
            }
            for (int key = 0, n = values.size(); key < n; key++) {
                if (Chars.equals(values.getQuick(key), value)) {
                    return key;
                }
            }
            return SymbolTable.VALUE_NOT_FOUND;
        }

        @Override
        public CharSequence valueBOf(int key) {
            return valueOf(key);
        }

        @Override
        public CharSequence valueOf(int key) {
            return key > -1 && key < values.size() ? values.getQuick(key) : null;
        }
    }

    /**
     * {@link AccountSymbolTable} over a narrower domain whose values exist before the case
     * measures anything, so resolving a key allocates nothing on the symbol map's side.
     */
    private static final class PrecomputedAccountSymbolTable implements StaticSymbolTable {
        private final ObjList<String> values = new ObjList<>();

        private PrecomputedAccountSymbolTable(int keyCount) {
            for (int key = 0; key < keyCount; key++) {
                values.add(ACCOUNT_PREFIX + key);
            }
        }

        @Override
        public boolean containsNullValue() {
            return false;
        }

        @Override
        public int getSymbolCount() {
            return values.size();
        }

        @Override
        public int keyOf(CharSequence value) {
            if (value == null) {
                return SymbolTable.VALUE_IS_NULL;
            }
            try {
                final int key = Numbers.parseInt(value, ACCOUNT_PREFIX.length(), value.length());
                return key < values.size() ? key : SymbolTable.VALUE_NOT_FOUND;
            } catch (NumericException e) {
                return SymbolTable.VALUE_NOT_FOUND;
            }
        }

        @Override
        public CharSequence valueBOf(int key) {
            return valueOf(key);
        }

        @Override
        public CharSequence valueOf(int key) {
            return key > -1 && key < values.size() ? values.getQuick(key) : null;
        }
    }

    /**
     * The view's stored rows as far as binding a merge reads them: the key column's symbol
     * map, and no row.
     */
    private static class SymbolTableCursor implements RecordCursor {
        private final StaticSymbolTable symbols;

        private SymbolTableCursor(StaticSymbolTable symbols) {
            this.symbols = symbols;
        }

        @Override
        public void close() {
        }

        @Override
        public Record getRecord() {
            return null;
        }

        @Override
        public Record getRecordB() {
            return null;
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return symbols;
        }

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public long preComputedStateSize() {
            return 0;
        }

        @Override
        public void recordAt(Record record, long atRowId) {
        }

        @Override
        public long size() {
            return 0;
        }

        @Override
        public void toTop() {
        }
    }
}
