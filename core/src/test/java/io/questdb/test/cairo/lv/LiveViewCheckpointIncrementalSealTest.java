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
import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.RecordSinkSPI;
import io.questdb.cairo.SingleColumnType;
import io.questdb.cairo.lv.LiveViewCheckpointFunctionDirectory;
import io.questdb.cairo.lv.LiveViewCheckpointFunctionRoot;
import io.questdb.cairo.lv.LiveViewCheckpointGenerationPin;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointMetaStore;
import io.questdb.cairo.lv.LiveViewCheckpointPageRef;
import io.questdb.cairo.lv.LiveViewCheckpointPartitionMapReader;
import io.questdb.cairo.lv.LiveViewCheckpointRoot;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineEntry;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineReader;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineStoreReader;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineStoreWriter;
import io.questdb.cairo.lv.LiveViewCheckpointWindowRoot;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.lv.LiveViewWindow;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapRecord;
import io.questdb.cairo.map.MapRecordCursor;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.cairo.sql.WindowSPI;
import io.questdb.cairo.vm.MemoryCARWImpl;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.engine.QueryProgress;
import io.questdb.griffin.engine.functions.columns.LongColumn;
import io.questdb.griffin.engine.functions.constants.LongConstant;
import io.questdb.griffin.engine.functions.window.BasePartitionedWindowFunction;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.griffin.engine.window.WindowRecordCursorFactory;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import io.questdb.test.tools.LimitedMemoryTracker;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Coverage for the touched-key cadence seal: a seal that freezes the partitions the
 * batch moved rather than every partition the view holds.
 * <p>
 * The whole optimization rests on one invariant - a key whose state moved is in the
 * dirty set, and every path that can remove a key forces a complete scan instead.
 * An end-state comparison alone cannot see a break in it: the runtime keeps serving
 * correct results out of memory, and the omission only surfaces on the next restart.
 * Every case here therefore pairs the from-base recompute oracle with a direct
 * reading of the runtime's dirty set, so a key that stopped being tracked is a
 * failure at the seal rather than a failure a restart happens to expose.
 * <p>
 * A restore is the other way onto that path: reading the generation's head root back
 * leaves the runtime holding exactly what the root holds, which is the same position
 * a publication leaves it in, so the seal that follows may stay incremental. The two
 * restore cases below hold that to the same standard - the head root seeds the
 * baseline and any other root does not.
 * <p>
 * The view is the customer shape the optimization was written for: an anchored
 * WINDOW carrying an unbounded cumulative sum and count per account, which is
 * whole-state per key and therefore takes the incremental branch rather than the
 * ring one.
 */
public class LiveViewCheckpointIncrementalSealTest extends AbstractLiveViewTest {

    // Midnight. ANCHOR DAILY '00:00' is the only daily form the frontier sweep
    // accepts: it desugars into the two-argument timestamp_floor, and
    // LiveViewRefreshJob.isProvablyMonotoneAnchor takes that form and no other.
    private static final String MIDNIGHT_ANCHOR = "00:00";
    // Noon, so a bucket crossing lands in the middle of a day rather than on the
    // calendar boundary a careless oracle would agree with by accident.
    private static final String NOON_ANCHOR = "12:00";
    // Room for anything the window allocates, so a tracker a case has already tripped once
    // stops standing in the way of the retry that case is really about.
    private static final long ROOMY_TRACKER_LIMIT_BYTES = 64 * 1024 * 1024L;
    // Four accounts in one anchor bucket. Every sweep case starts here: the trigger
    // demands at least half the map be reclaimable, so three of these have to fall
    // behind the frontier before anything fires.
    private static final String SEED_FOUR_ACCOUNTS =
            "('2026-01-01T11:00:00.000000Z', 'acct-1', 10.0), "
                    + "('2026-01-01T11:00:01.000000Z', 'acct-2', 20.0), "
                    + "('2026-01-01T11:00:02.000000Z', 'acct-3', 30.0), "
                    + "('2026-01-01T11:00:03.000000Z', 'acct-4', 40.0)";
    // Enough accounts that a sweep's evicted set dwarfs what one cadence touches, which is
    // the only shape in which the dirty sets' retained capacity is observable at all.
    private static final int SWEEP_CAPACITY_ACCOUNTS = 2_000;

    @Test
    public void testARepeatedKeyEntersTheDirtySetOncePerCadence() throws Exception {
        // Four rows per boundary, as in testTheCadenceCounterWrappingStillNamesEveryTouchedKey
        // and testTouchedKeysAreTheOnlyDirtyStateBetweenSeals, so a commit smaller than that
        // refreshes without sealing and the marks a cadence made stay readable.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 4);
        assertMemoryLeak(() -> {
            createView(
                    NOON_ANCHOR,
                    "('2026-01-01T11:00:00.000000Z', 'acct-1', 10.0), "
                            + "('2026-01-01T11:00:01.000000Z', 'acct-2', 20.0), "
                            + "('2026-01-01T11:00:02.000000Z', 'acct-3', 30.0), "
                            + "('2026-01-01T11:00:03.000000Z', 'acct-4', 40.0)"
            );
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                assertDirtySetsClearedByPublish();

                // Three rows, one key, inside one cadence. The dirty set names the key
                // once, and the marks it costs are one rather than three: a repeat row
                // reads the cadence off the anchor value processRow has already loaded
                // and never serializes the key into the second map at all.
                final long marksBefore = anchorWindow().getCheckpointDirtyMarkCount();
                commit("('2026-01-01T11:00:04.000000Z', 'acct-1', 1.0), "
                        + "('2026-01-01T11:00:05.000000Z', 'acct-1', 2.0), "
                        + "('2026-01-01T11:00:06.000000Z', 'acct-1', 3.0)", job);
                Assert.assertEquals(1, anchorWindow().getCheckpointDirtyAnchorMapSize());
                Assert.assertEquals(
                        "a repeat row must not enter its key into the dirty set again",
                        1,
                        anchorWindow().getCheckpointDirtyMarkCount() - marksBefore
                );

                // The fourth row crosses the boundary and seals, which empties the dirty
                // set. The cadence has to move on with it: a key whose anchor entry still
                // carried the sealed cadence would skip its mark for good, and the next
                // incremental seal would publish a root missing it.
                commit("('2026-01-01T11:00:07.000000Z', 'acct-1', 4.0)", job);
                assertDirtySetsClearedByPublish();

                final long marksAfterSeal = anchorWindow().getCheckpointDirtyMarkCount();
                commit("('2026-01-01T11:00:08.000000Z', 'acct-1', 5.0)", job);
                Assert.assertEquals(
                        "the first row of a new cadence must enter its key again",
                        1,
                        anchorWindow().getCheckpointDirtyAnchorMapSize()
                );
                Assert.assertEquals(
                        1,
                        anchorWindow().getCheckpointDirtyMarkCount() - marksAfterSeal
                );
                assertViewMatchesRecompute(NOON_ANCHOR);
            }

            // ...and the key the second cadence named has to reach the durable root, which
            // is what says the skipped marks cost the seal nothing.
            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
                Assert.assertTrue(viewInstance().isCheckpointRestoreSucceeded());
                driveRefreshToQuiescence(job);
                assertViewMatchesRecompute(NOON_ANCHOR);
            }
        });
    }

    /**
     * The cadence stamp the anchor keeps is what every window function's own dirty set
     * now rides on, so a repeat row must cost the residual functions no key serialization
     * and no probe of their own either. What the assertions read is each function's set
     * rather than the anchor's - {@link #createUnfusedView} is what makes those sets the
     * live ones, because a fused group takes its key domain from the anchor entry instead.
     * <p>
     * The restart is the half that matters: a set that skipped a key its rows moved still
     * answers correctly out of memory, and only the root the next incremental seal
     * publishes shows the omission.
     */
    @Test
    public void testARepeatedKeyEntersEveryFunctionDirtySetOncePerCadence() throws Exception {
        // Four rows per boundary, as in testARepeatedKeyEntersTheDirtySetOncePerCadence, so
        // a commit smaller than that refreshes without sealing and the cadence's marks
        // stay readable.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 4);
        assertMemoryLeak(() -> {
            createUnfusedView(NOON_ANCHOR, SEED_FOUR_ACCOUNTS);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                assertDirtySetsClearedByPublish();

                // Three rows, one key, one cadence. The residual sum's dirty set names the
                // key once, and the anchor's mark count says the second and third rows
                // stopped at the stamp rather than reaching either map.
                final long marksBefore = anchorWindow().getCheckpointDirtyMarkCount();
                commit("('2026-01-01T11:00:04.000000Z', 'acct-1', 1.0), "
                        + "('2026-01-01T11:00:05.000000Z', 'acct-1', 2.0), "
                        + "('2026-01-01T11:00:06.000000Z', 'acct-1', 3.0)", job);
                assertFunctionDirtySize(1);
                Assert.assertEquals(
                        "a repeat row must not enter its key into any dirty set again",
                        1,
                        anchorWindow().getCheckpointDirtyMarkCount() - marksBefore
                );

                commit("('2026-01-01T11:00:07.000000Z', 'acct-1', 4.0)", job);
                assertDirtySetsClearedByPublish();
                assertUnfusedViewMatchesRecompute(NOON_ANCHOR);
            }

            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
                Assert.assertTrue(viewInstance().isCheckpointRestoreSucceeded());
                driveRefreshToQuiescence(job);
                assertUnfusedViewMatchesRecompute(NOON_ANCHOR);
            }
        });
    }

    /**
     * The one row inside a cadence that moves a function's state the most - the anchor
     * cross, which resets the accumulator to the new bucket - is also a row whose key was
     * already named earlier in that cadence, so it makes no mark of its own. The mark the
     * cadence's first row made has to still stand for it.
     * <p>
     * The restart is what holds it: the durable image has to be the post-cross
     * accumulator, and a key the seal never froze would come back as the pre-cross one.
     */
    @Test
    public void testAnAnchorCrossAfterTheCadenceMarkStillNamesTheKey() throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 4);
        assertMemoryLeak(() -> {
            createUnfusedView(NOON_ANCHOR, SEED_FOUR_ACCOUNTS);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                assertDirtySetsClearedByPublish();

                // First row of the cadence names acct-1 while it is still in the pre-noon
                // bucket. The second crosses noon on the same key in the same cadence:
                // resetPartition empties the accumulator and this row starts it again,
                // with no fresh mark anywhere.
                final long marksBefore = anchorWindow().getCheckpointDirtyMarkCount();
                commit("('2026-01-01T11:00:04.000000Z', 'acct-1', 1.0), "
                        + "('2026-01-01T12:00:00.000000Z', 'acct-1', 7.0)", job);
                assertFunctionDirtySize(1);
                Assert.assertEquals(
                        "an anchor cross on an already-named key must not mark it twice",
                        1,
                        anchorWindow().getCheckpointDirtyMarkCount() - marksBefore
                );

                // Seal the cadence, then read the restored accumulator back.
                commit("('2026-01-01T12:00:01.000000Z', 'acct-2', 2.0), "
                        + "('2026-01-01T12:00:02.000000Z', 'acct-3', 3.0)", job);
                assertDirtySetsClearedByPublish();
                assertUnfusedViewMatchesRecompute(NOON_ANCHOR);
            }

            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
                Assert.assertTrue(viewInstance().isCheckpointRestoreSucceeded());
                driveRefreshToQuiescence(job);
                assertUnfusedViewMatchesRecompute(NOON_ANCHOR);

                // A follow-up row in the restored bucket accumulates on top of what came
                // back, so a stale pre-cross image shows up here as an inflated sum.
                commit("('2026-01-01T12:30:00.000000Z', 'acct-1', 5.0)", job);
                assertUnfusedViewMatchesRecompute(NOON_ANCHOR);
            }
        });
    }

    /**
     * Eviction and revival inside one cadence, read off the residual function's own dirty
     * set rather than the anchor's. The sweep takes the key out of the anchor map, so the
     * revived row arrives as a new partition and raises the cadence flag again - which is
     * what turns the eviction marker the sweep left in the function's set back into an
     * upsert. A flag that stayed down would leave the seal emitting a removal for a key
     * the runtime holds.
     */
    @Test
    public void testASweptResidualKeyRevivedInOneCadenceIsNamedAgain() throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 4);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_PARTITION_COMPACT_THRESHOLD, 2);
        assertMemoryLeak(() -> {
            createUnfusedView(MIDNIGHT_ANCHOR, SEED_FOUR_ACCOUNTS);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                assertDirtySetsClearedByPublish();
                final long generation = publishedGeneration();
                final long sealFailuresBefore = viewInstance().getCheckpointSealFailures();

                commit("('2026-01-02T01:00:00.000000Z', 'acct-1', 1.0)", job);
                commit("('2026-01-03T01:00:00.000000Z', 'acct-1', 2.0)", job);
                Assert.assertEquals(1, anchorWindow().getCompactionCount());
                assertEvictionMarkerCount(3);

                // acct-2 is back inside the same cadence. The sweep dropped its anchor
                // entry, so this row is a first touch again and re-names the key in every
                // dirty set - the function's marker has to go back to 0 with the anchor's.
                commit("('2026-01-03T02:00:00.000000Z', 'acct-2', 3.0)", job);
                assertEvictionMarkerCount(2);
                assertIncrementalGateOpen(generation);

                commit("('2026-01-03T03:00:00.000000Z', 'acct-1', 4.0)", job);
                Assert.assertEquals(
                        "the re-created key must not have produced a duplicate mutation",
                        sealFailuresBefore,
                        viewInstance().getCheckpointSealFailures()
                );
                assertDirtySetsClearedByPublish();
                assertUnfusedViewMatchesRecompute(MIDNIGHT_ANCHOR);
            }

            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
                Assert.assertTrue(viewInstance().isCheckpointRestoreSucceeded());
                driveRefreshToQuiescence(job);
                assertUnfusedViewMatchesRecompute(MIDNIGHT_ANCHOR);

                commit("('2026-01-03T05:00:00.000000Z', 'acct-2', 6.0)", job);
                assertUnfusedViewMatchesRecompute(MIDNIGHT_ANCHOR);
            }
        });
    }

    /**
     * {@link #testTheCadenceCounterWrappingStillNamesEveryTouchedKey} over the residual
     * functions. The turn is the one arm that can leave a stamp matching a cadence no
     * dirty set holds, and the stamp now gates F + 1 sets rather than the anchor's alone,
     * so a missed re-raise takes every one of them out at once.
     */
    @Test
    public void testTheCadenceCounterWrappingStillNamesEveryFunctionKey() throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 4);
        assertMemoryLeak(() -> {
            createUnfusedView(NOON_ANCHOR, SEED_FOUR_ACCOUNTS);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                assertDirtySetsClearedByPublish();

                // Stamp acct-1 with the cadence the counter's turn lands back on.
                anchorWindow().setCheckpointDirtyEpoch((short) 1);
                commit("('2026-01-01T11:00:04.000000Z', 'acct-1', 1.0)", job);
                assertFunctionDirtySize(1);

                // Stand the counter one cadence below its turn and seal on OTHER keys, so
                // acct-1 carries a stale stamp rather than a fresh one.
                anchorWindow().setCheckpointDirtyEpoch(Short.MAX_VALUE);
                commit("('2026-01-01T11:00:05.000000Z', 'acct-2', 2.0), "
                        + "('2026-01-01T11:00:06.000000Z', 'acct-3', 3.0), "
                        + "('2026-01-01T11:00:07.000000Z', 'acct-4', 4.0)", job);
                assertDirtySetsClearedByPublish();

                commit("('2026-01-01T11:00:08.000000Z', 'acct-1', 5.0)", job);
                assertFunctionDirtySize(1);
                assertUnfusedViewMatchesRecompute(NOON_ANCHOR);
            }

            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
                Assert.assertTrue(viewInstance().isCheckpointRestoreSucceeded());
                driveRefreshToQuiescence(job);
                assertUnfusedViewMatchesRecompute(NOON_ANCHOR);
            }
        });
    }

    /**
     * The stamp is the whole of the flag's memory, so what happens between the anchor's
     * own mark and the last function's is the case the ordering exists for: a function
     * allocates its dirty set on first use through the per-view tracker, and that
     * allocation throws on a breach of the refresh memory limit.
     * <p>
     * A stamp written ahead of the loop would have the retry read the row as marked,
     * leaving the function that threw naming one key fewer than its rows moved - a set the
     * seal cannot tell from a complete one. Driving {@link LiveViewWindow#processRow}
     * directly is what puts a throw exactly there; a compiled view has no supported way to
     * fail one function's mark and not the other's.
     */
    @Test
    public void testAFailedFunctionMarkLeavesTheCadenceStampUnwritten() throws Exception {
        assertMemoryLeak(() -> {
            final LongKeyRecordStub record = new LongKeyRecordStub();
            final MarkCountingFunctionStub marking = new MarkCountingFunctionStub();
            final MarkCountingFunctionStub failing = new MarkCountingFunctionStub();
            final ObjList<WindowFunction> functions = new ObjList<>();
            functions.add(marking);
            functions.add(failing);
            record.value = 1;
            try (LiveViewWindow window = standaloneWindow(functions)) {
                failing.isFailing = true;
                try {
                    window.processRow(record);
                    Assert.fail("the failing function's mark must not be swallowed");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "mark refused");
                }
                Assert.assertEquals(1, marking.markedCount);
                Assert.assertEquals(1, window.getCheckpointDirtyAnchorMapSize());
                Assert.assertEquals(1, window.getCheckpointDirtyMarkCount());

                // The retry has to find the row unmarked, or the function that threw never
                // names the key at all.
                failing.isFailing = false;
                window.processRow(record);
                Assert.assertEquals(
                        "the retry must re-raise the flag for every function",
                        2,
                        marking.markedCount
                );
                Assert.assertEquals(1, failing.markedCount);
                Assert.assertEquals(2, window.getCheckpointDirtyMarkCount());

                // ...and only then does the stamp stand, which is the whole point of the
                // gate: a further row of the same cadence reaches no dirty map at all.
                window.processRow(record);
                Assert.assertEquals(2, marking.markedCount);
                Assert.assertEquals(1, failing.markedCount);
                Assert.assertEquals(2, window.getCheckpointDirtyMarkCount());
                Assert.assertEquals(1, window.getCheckpointDirtyAnchorMapSize());
            } finally {
                marking.reset();
                failing.reset();
            }
        });
    }

    /**
     * The stamp is a value slot, and a value slot is only what the last writer left in it.
     * A key that arrives new lands on whatever heap bytes its entry's offset held before -
     * OrderedMap.clear() rewinds the append pointer and zeroes the offsets, and leaves the
     * heap it wrote the entries into exactly as it found it - so the creating row has to
     * put the slot somewhere no cadence matches before anything that can throw runs.
     * <p>
     * Leave it, and the retry after a failed mark reads those bytes rather than
     * {@code isNew()}: they say "already dirty in this cadence", the flag comes back
     * false, and the function that threw finishes the cadence never having named the key.
     * The seal that follows freezes the keys the dirty sets name and leaves the root's
     * stale image of that one standing.
     * <p>
     * The head-miss replay ({@code LiveViewRefreshJob.clearWindowState}) is what empties
     * the anchor map over a live heap here, and the cadence counter is a SHORT, so it
     * comes back around to a value the abandoned bytes still carry -
     * {@link LiveViewWindow#setCheckpointDirtyEpoch(short)} stands in for the 32766 seals
     * that turn it over.
     */
    @Test
    public void testARetryFindsANewKeyUnmarkedWhenItsStampSlotHeldTheLiveCadence() throws Exception {
        assertMemoryLeak(() -> {
            final LongKeyRecordStub record = new LongKeyRecordStub();
            final MarkCountingFunctionStub marking = new MarkCountingFunctionStub();
            final MarkCountingFunctionStub failing = new MarkCountingFunctionStub();
            final ObjList<WindowFunction> functions = new ObjList<>();
            functions.add(marking);
            functions.add(failing);
            record.value = 1;
            try (LiveViewWindow window = standaloneWindow(
                    functions,
                    LongKeyRecordStub.PAIR_KEY_TYPES,
                    LongKeyRecordStub.PAIR_SINK
            )) {
                // The poisoning below rests on the anchor map keeping its heap across a
                // clear(), which is OrderedMap's contract and not the memsetting one every
                // unordered implementation holds to. Asserted rather than assumed: a
                // single-column key of this width lands on Unordered8Map at the default
                // cairo.sql.unordered.map.max.entry.size, and this case would pass on the
                // memset rather than on the fix.
                Assert.assertEquals("OrderedMap", window.getAnchorMapImplementation());

                // Cadence 1 stamps the key's slot at the head of the anchor map's heap.
                window.processRow(record);
                Assert.assertEquals(1, marking.markedCount);
                Assert.assertEquals(1, failing.markedCount);
                Assert.assertEquals(1, window.getCheckpointDirtyAnchorMapSize());

                // The replay rewinds every function and this window, which empties both
                // dirty sets and the anchor map - and leaves the stamp lying in the heap
                // the next entry at that offset will land on.
                marking.toTop();
                failing.toTop();
                window.toTop();
                window.setCheckpointDirtyEpoch((short) 1);
                Assert.assertEquals(0, window.getCheckpointDirtyAnchorMapSize());
                Assert.assertEquals(0, failing.getCheckpointDirtyPartitionMap().size());

                // The replay's first row creates the key again, onto those same bytes, and
                // the second function's mark throws before the stamp goes in.
                failing.isFailing = true;
                try {
                    window.processRow(record);
                    Assert.fail("the failing function's mark must not be swallowed");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "mark refused");
                }
                Assert.assertEquals(2, marking.markedCount);
                Assert.assertEquals(1, failing.markedCount);
                Assert.assertEquals(0, failing.getCheckpointDirtyPartitionMap().size());

                // The retry has to find the row unmarked. It reads the stamp slot rather
                // than isNew(), so a slot the creating row left holding the live cadence
                // has every set on this view finish the cadence one key short.
                failing.isFailing = false;
                window.processRow(record);
                Assert.assertEquals(
                        "the retry must re-enter the key into the dirty set of the function that threw",
                        1,
                        failing.getCheckpointDirtyPartitionMap().size()
                );
                Assert.assertEquals(2, failing.markedCount);
                Assert.assertEquals(3, marking.markedCount);
                Assert.assertEquals(1, marking.getCheckpointDirtyPartitionMap().size());
                Assert.assertEquals(1, window.getCheckpointDirtyAnchorMapSize());

                // ...and only then does the stamp stand: a further row of the same cadence
                // reaches no dirty map at all.
                window.processRow(record);
                Assert.assertEquals(2, failing.markedCount);
                Assert.assertEquals(3, marking.markedCount);
            } finally {
                marking.reset();
                failing.reset();
            }
        });
    }

    /**
     * The window's own dirty mark is the first thing after {@code createValue()} that can
     * throw: it builds the dirty set lazily under the per-view tracker, so a breach of
     * cairo.live.view.refresh.memory.limit.bytes raises on that allocation with the anchor
     * entry already published. Both flag slots have to stand by then, or the entry the
     * retry finds keeps whatever the heap region carried - a tombstone byte that has
     * {@code snapshot()} skip a live partition and then refuse the payload whose count it
     * no longer matches, and a stamp that has the retry read the key as marked.
     * <p>
     * This is what separates seeding the slots ahead of the mark from seeding them beside
     * the reset that follows it. The map arrives pre-poisoned for the same reason the case
     * above poisons one: a fresh mapping reads as zero and would pass either way.
     */
    @Test
    public void testAThrownWindowMarkLeavesNoStaleFlagOnTheKeyItCreated() throws Exception {
        assertMemoryLeak(() -> {
            final LongKeyRecordStub record = new LongKeyRecordStub();
            final MarkCountingFunctionStub marking = new MarkCountingFunctionStub();
            final ObjList<WindowFunction> functions = new ObjList<>();
            functions.add(marking);
            record.value = 1;
            try (
                    // One byte, so the dirty set breaches on its first malloc: the map that
                    // owns it closes and unwinds before it holds anything, and the anchor
                    // map - which this test supplies with no tracker bound, and the window
                    // owns from there - is untouched.
                    LimitedMemoryTracker tracker = new LimitedMemoryTracker(1);
                    LiveViewWindow window = standaloneWindow(
                            functions,
                            LongKeyRecordStub.PAIR_KEY_TYPES,
                            LongKeyRecordStub.PAIR_SINK,
                            poisonedAnchorMap(record),
                            tracker
                    );
                    MemoryCARWImpl sink = new MemoryCARWImpl(1024, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)
            ) {
                // The poison only lands on the next entry if the map keeps the heap it wrote
                // it into. Asserted rather than assumed - see the case above.
                Assert.assertEquals("OrderedMap", window.getAnchorMapImplementation());

                // The row publishes the entry onto the poisoned bytes, and the window's own
                // mark throws before any function is reached.
                try {
                    window.processRow(record);
                    Assert.fail("the tracker breach must not be swallowed");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                }
                Assert.assertEquals(0, marking.markedCount);
                Assert.assertEquals("the throw leaves the entry published", 1, window.getAnchorMapSize());
                Assert.assertEquals(0, window.getCheckpointDirtyAnchorMapSize());

                // The retry has room, and has to find the key both unmarked and alive.
                tracker.setLimit(ROOMY_TRACKER_LIMIT_BYTES);
                window.processRow(record);
                Assert.assertEquals(
                        "the retry must name the key its failed attempt never did",
                        1,
                        window.getCheckpointDirtyAnchorMapSize()
                );
                Assert.assertEquals(1, marking.markedCount);
                Assert.assertEquals(1, marking.getCheckpointDirtyPartitionMap().size());

                // The tombstone slot decides whether the partition reaches a root at all.
                window.snapshot(sink);
                window.restore(sink);
                Assert.assertEquals(
                        "the snapshot must carry the partition the failed row created",
                        1,
                        window.getAnchorMapSize()
                );
            } finally {
                marking.reset();
            }
        });
    }

    @Test
    public void testASweepInflatedDirtySetGivesItsCapacityBackOnPublish() throws Exception {
        // One boundary per row, so the seed's accounts arrive over many small cadences and
        // the dirty sets never grow to hold more than a couple of keys at a time. That is
        // the steady state the sweep then breaks: it puts every evicted key into those same
        // maps at once, and a plain clear() on publish would leave the peak resident for
        // the view's lifetime.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_PARTITION_COMPACT_THRESHOLD, 2);
        assertMemoryLeak(() -> {
            createViewWithGeneratedSeed(MIDNIGHT_ANCHOR, SWEEP_CAPACITY_ACCOUNTS);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                assertDirtySetsClearedByPublish();
                Assert.assertEquals(SWEEP_CAPACITY_ACCOUNTS, anchorWindow().getAnchorMapSize());
                final LongList capacityBefore = readDirtySetKeyCapacities();

                // Two bucket advances with one account following along, so the sweep drops
                // every other account into the dirty sets at once. At one boundary per row
                // the same cadence seals, which is where the capacity has to come back.
                commit("('2026-01-02T01:00:00.000000Z', 'acct-1', 1.0)", job);
                commit("('2026-01-03T01:00:00.000000Z', 'acct-1', 2.0)", job);
                Assert.assertEquals(1, anchorWindow().getCompactionCount());
                Assert.assertEquals(
                        SWEEP_CAPACITY_ACCOUNTS - 1,
                        anchorWindow().getCompactedPartitionCount()
                );
                assertDirtySetsClearedByPublish();

                final LongList capacityAfter = readDirtySetKeyCapacities();
                for (int i = 0, n = capacityBefore.size(); i < n; i++) {
                    Assert.assertTrue(
                            "dirty set " + i + " kept the sweep's capacity: before="
                                    + capacityBefore.getQuick(i) + " after=" + capacityAfter.getQuick(i),
                            capacityAfter.getQuick(i) <= capacityBefore.getQuick(i)
                    );
                }

                // Handing the backing back must not have cost the seal its baseline, nor
                // the view its results.
                assertViewMatchesRecompute(MIDNIGHT_ANCHOR);
            }
        });
    }

    @Test
    public void testASweepThenAFailedPublishKeepsItsRemovalsForTheRetry() throws Exception {
        // The two halves the pair beside this one covers separately: a seal that dies after
        // durable metadata, and a sweep that fills the dirty sets with removals. Together
        // they are the case where what the failed seal owed the root is not a handful of
        // touched keys but every key the sweep dropped - and the runtime's only record of
        // them is the eviction markers and the inflated capacity holding them. Clearing
        // either on the way out of a failure would leave the root naming partitions no map
        // has, and no later seal would go looking.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_PARTITION_COMPACT_THRESHOLD, 2);
        assertMemoryLeak(() -> {
            createViewWithGeneratedSeed(MIDNIGHT_ANCHOR, SWEEP_CAPACITY_ACCOUNTS);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                assertDirtySetsClearedByPublish();
                Assert.assertEquals(SWEEP_CAPACITY_ACCOUNTS, anchorWindow().getAnchorMapSize());

                // One account follows the frontier into the next bucket, leaving the other
                // 1999 a bucket behind. This one seals cleanly; the readings that the failure
                // must not disturb are taken after it, not before.
                commit("('2026-01-02T01:00:00.000000Z', 'acct-1', 1.0)", job);
                assertDirtySetsClearedByPublish();
                final LongList capacityBefore = readDirtySetKeyCapacities();
                final long baselineGeneration = anchorWindow().getCheckpointBaselineGeneration();
                final long sealFailuresBefore = viewInstance().getCheckpointSealFailures();

                // The second advance is what sweeps, and at one boundary per row the same
                // cadence seals - so the seal carrying 1999 removals is the one that dies.
                job.setCheckpointTimelineTestFailureStage(
                        LiveViewCheckpointTimelineStoreWriter.TEST_FAIL_AFTER_METADATA_PUBLISH
                );
                commit("('2026-01-03T01:00:00.000000Z', 'acct-1', 2.0)", job);

                Assert.assertEquals(1, anchorWindow().getCompactionCount());
                Assert.assertEquals(
                        SWEEP_CAPACITY_ACCOUNTS - 1,
                        anchorWindow().getCompactedPartitionCount()
                );
                Assert.assertEquals(1, anchorWindow().getAnchorMapSize());
                Assert.assertTrue(
                        "the injected failure must be counted as a failed seal",
                        viewInstance().getCheckpointSealFailures() > sealFailuresBefore
                );
                Assert.assertEquals(
                        "a failed publish must not adopt a new baseline",
                        baselineGeneration,
                        anchorWindow().getCheckpointBaselineGeneration()
                );
                // Every removal the failed seal was going to write is still marked, and the
                // capacity holding them is still standing - a shrink here would drop them.
                assertEvictionMarkerCount(SWEEP_CAPACITY_ACCOUNTS - 1);
                final LongList capacityDuring = readDirtySetKeyCapacities();
                boolean isInflated = false;
                for (int i = 0, n = capacityBefore.size(); i < n; i++) {
                    isInflated |= capacityDuring.getQuick(i) > capacityBefore.getQuick(i);
                }
                Assert.assertTrue("the sweep's removals must still be held", isInflated);

                // The retry is what publishes them, and only then does the capacity go back.
                job.setCheckpointTimelineTestFailureStage(0);
                commit("('2026-01-03T02:00:00.000000Z', 'acct-1', 3.0)", job);
                assertDirtySetsClearedByPublish();
                assertEvictionMarkerCount(0);
                Assert.assertTrue(
                        "the recovering seal must publish a new generation",
                        anchorWindow().getCheckpointBaselineGeneration() > baselineGeneration
                );
                final LongList capacityAfter = readDirtySetKeyCapacities();
                for (int i = 0, n = capacityBefore.size(); i < n; i++) {
                    Assert.assertTrue(
                            "dirty set " + i + " kept the sweep's capacity: before="
                                    + capacityBefore.getQuick(i) + " after=" + capacityAfter.getQuick(i),
                            capacityAfter.getQuick(i) <= capacityBefore.getQuick(i)
                    );
                }
                assertViewMatchesRecompute(MIDNIGHT_ANCHOR);
            }

            // The proof the removals reached the root rather than merely leaving the maps:
            // a restart reads the root and nothing else, and must find only the survivor.
            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
                Assert.assertTrue(viewInstance().isCheckpointRestoreSucceeded());
                Assert.assertEquals(1, anchorWindow().getAnchorMapSize());
                driveRefreshToQuiescence(job);
                assertViewMatchesRecompute(MIDNIGHT_ANCHOR);
            }
        });
    }

    /**
     * The sweep must never be what starts a function's dirty tracking. An unbounded-rows
     * avg or ksum whose own markPartitionAlive skipped
     * {@code markCheckpointPartitionDirty} named none of the keys its rows moved, so a
     * dirty set the sweep built out of eviction markers alone opened the incremental gate
     * on it: the seal froze the removals and nothing else, and every partition whose
     * accumulator had advanced between the two seals kept its stale durable image. The
     * runtime went on answering correctly out of memory, so only a restart showed it.
     * <p>
     * Both calls here are residual - an expression argument, as in
     * {@link #createUnfusedView} - so what the case reads is each function's own dirty
     * set and its own root rather than the window's fused entry.
     */
    @Test
    public void testASweptResidualAvgStillFreezesTheKeysItsRowsMoved() throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 4);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_PARTITION_COMPACT_THRESHOLD, 2);
        assertMemoryLeak(() -> {
            createUnfusedAvgView(MIDNIGHT_ANCHOR, SEED_FOUR_ACCOUNTS);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                // The whole case rests on both calls staying residual. Every per-function
                // assertion below falls back to the anchor's own numbers for a fused
                // group, so a plan that learned to accept an expression argument would
                // leave this passing against state it no longer describes.
                Assert.assertFalse(
                        "both calls must decline the fused plan",
                        isWindowStateFused()
                );
                assertDirtySetsClearedByPublish();
                assertHeadRootPartitionCount(4);
                final long generation = publishedGeneration();

                // Two bucket advances with only acct-1 following along, both under the
                // four-row boundary, so the second one sweeps without sealing.
                commit("('2026-01-02T01:00:00.000000Z', 'acct-1', 1.0)", job);
                commit("('2026-01-03T01:00:00.000000Z', 'acct-1', 2.0)", job);
                Assert.assertEquals(1, anchorWindow().getCompactionCount());
                Assert.assertEquals(
                        "only the account that followed the frontier survives",
                        1,
                        anchorWindow().getAnchorMapSize()
                );
                assertIncrementalGateOpen(generation);

                // Four keys rather than three: the survivor these two commits moved is
                // named alongside the three the sweep dropped. A dirty set the sweep
                // started on its own holds the evictions and nothing else.
                assertFunctionDirtySize(4);
                assertEvictionMarkerCount(3);

                // Two more rows into the survivor's live bucket - the accumulator advance
                // the seal owes the root - and the fourth row of the cadence seals.
                commit("('2026-01-03T02:00:00.000000Z', 'acct-1', 3.0), "
                        + "('2026-01-03T03:00:00.000000Z', 'acct-1', 4.0)", job);
                assertDirtySetsClearedByPublish();
                assertHeadRootPartitionCount(1);
                assertUnfusedAvgViewMatchesRecompute(MIDNIGHT_ANCHOR);
            }

            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
                Assert.assertTrue(viewInstance().isCheckpointRestoreSucceeded());
                driveRefreshToQuiescence(job);
                assertUnfusedAvgViewMatchesRecompute(MIDNIGHT_ANCHOR);

                // A row in the bucket the survivor is already accumulating in, so it reads
                // the restored accumulator rather than starting a fresh one. A root left
                // holding the pre-sweep image answers 7.5 here where the oracle says 3.5.
                commit("('2026-01-03T04:00:00.000000Z', 'acct-1', 5.0)", job);
                assertUnfusedAvgViewMatchesRecompute(MIDNIGHT_ANCHOR);
            }
        });
    }

    @Test
    public void testAnchorBucketCrossingCarriesTheNewAnchorValueThroughARestart() throws Exception {
        // One boundary per commit, so each step below is its own seal.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            createView(
                    NOON_ANCHOR,
                    "('2026-01-01T11:00:00.000000Z', 'acct-1', 10.0), "
                            + "('2026-01-01T11:00:01.000000Z', 'acct-2', 20.0), "
                            + "('2026-01-01T11:00:02.000000Z', 'acct-3', 30.0)"
            );
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                assertViewMatchesRecompute(NOON_ANCHOR);

                // Two of the three accounts cross into the 12:00 bucket, one does not.
                // The crossing rewrites their anchor value in place, which is the
                // incremental anchor path: a root that kept the old value would restore
                // a window that thinks it is still in the morning bucket.
                commit("('2026-01-01T12:00:00.000000Z', 'acct-1', 1.0), "
                        + "('2026-01-01T12:00:01.000000Z', 'acct-2', 2.0)", job);
                assertViewMatchesRecompute(NOON_ANCHOR);
                assertDirtySetsClearedByPublish();
            }

            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
                Assert.assertTrue(
                        "the view must restore its accumulators from the checkpoint timeline",
                        viewInstance().isCheckpointRestoreSucceeded()
                );
                driveRefreshToQuiescence(job);
                assertViewMatchesRecompute(NOON_ANCHOR);

                // The row that reads the restored anchor value back. It sits in the same
                // 12:00 bucket the crossing opened, so a stale anchor value would look
                // like a fresh crossing here and zero acct-1's accumulator - a diff
                // against the recompute rather than a silent saving.
                commit("('2026-01-01T13:00:00.000000Z', 'acct-1', 3.0), "
                        + "('2026-01-01T13:00:01.000000Z', 'acct-3', 4.0)", job);
                assertViewMatchesRecompute(NOON_ANCHOR);

                // And one more crossing, now on top of restored state.
                commit("('2026-01-02T12:00:00.000000Z', 'acct-1', 5.0), "
                        + "('2026-01-02T12:00:01.000000Z', 'acct-4', 6.0)", job);
                assertViewMatchesRecompute(NOON_ANCHOR);
                assertDirtySetsClearedByPublish();
            }
        });
    }

    @Test
    public void testDirtyKeyMissingWithoutASweepStillFails() throws Exception {
        // Four rows per boundary, so the three rows below leave the dirty sets standing
        // and the fourth is what seals.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 4);
        assertMemoryLeak(() -> {
            // What this holds to its contract is the per-function missing-state branch,
            // which a fused group has no state to reach: emptying a grouped function's
            // private map describes nothing the seal reads, because the accumulator is a
            // slice of the window's own entry. The branch still runs for every residual.
            createUnfusedView(MIDNIGHT_ANCHOR, SEED_FOUR_ACCOUNTS);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                assertDirtySetsClearedByPublish();
                final long sealFailuresBefore = viewInstance().getCheckpointSealFailures();

                commit("('2026-01-01T12:00:00.000000Z', 'acct-1', 1.0)", job);
                commit("('2026-01-01T12:00:01.000000Z', 'acct-2', 2.0)", job);
                commit("('2026-01-01T12:00:02.000000Z', 'acct-3', 3.0)", job);
                Assert.assertEquals(
                        "no anchor bucket was crossed, so nothing may have been swept",
                        0,
                        anchorWindow().getCompactionCount()
                );

                // State the seal is owed simply disappears, with no sweep anywhere in
                // the picture. That is a bookkeeping bug, not a removal, and relaxing
                // the seal's missing-value branch must not have turned it into one:
                // the root would silently stop naming three live accounts.
                clearFunctionStateMaps();
                commit("('2026-01-01T12:00:03.000000Z', 'acct-4', 4.0)", job);
                Assert.assertTrue(
                        "a dirty key with no live state and no eviction marker must fail the seal",
                        viewInstance().getCheckpointSealFailures() > sealFailuresBefore
                );
            }
        });
    }

    @Test
    public void testFailedPublishKeepsTheTouchedKeysForTheNextSeal() throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            createView(
                    NOON_ANCHOR,
                    "('2026-01-01T11:00:00.000000Z', 'acct-1', 10.0), "
                            + "('2026-01-01T11:00:01.000000Z', 'acct-2', 20.0), "
                            + "('2026-01-01T11:00:02.000000Z', 'acct-3', 30.0), "
                            + "('2026-01-01T11:00:03.000000Z', 'acct-4', 40.0)"
            );
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                assertDirtySetsClearedByPublish();
                final long baselineGeneration = anchorWindow().getCheckpointBaselineGeneration();
                final long sealFailuresBefore = viewInstance().getCheckpointSealFailures();

                // The seal reaches durable metadata and then dies, so it never calls
                // onCheckpointPersisted. The dirty sets are the runtime's only record
                // of what the failed seal was going to write; clearing them on the way
                // out would leave those keys unwritten by every later seal too.
                job.setCheckpointTimelineTestFailureStage(
                        LiveViewCheckpointTimelineStoreWriter.TEST_FAIL_AFTER_METADATA_PUBLISH
                );
                commit("('2026-01-01T11:00:04.000000Z', 'acct-1', 1.0)", job);
                Assert.assertTrue(
                        "the injected failure must be counted as a failed seal",
                        viewInstance().getCheckpointSealFailures() > sealFailuresBefore
                );
                Assert.assertEquals(
                        "a failed publish must not adopt a new baseline",
                        baselineGeneration,
                        anchorWindow().getCheckpointBaselineGeneration()
                );
                Assert.assertTrue(
                        "the failed seal's touched keys must still be dirty",
                        anchorWindow().getCheckpointDirtyAnchorMapSize() > 0
                );
                assertFunctionDirtySize(1);

                // A second key moves while the failure is still injected. Both keys are
                // now owed to the root.
                commit("('2026-01-01T11:00:05.000000Z', 'acct-2', 2.0)", job);
                assertFunctionDirtySize(2);
                Assert.assertEquals(2, anchorWindow().getCheckpointDirtyAnchorMapSize());

                job.setCheckpointTimelineTestFailureStage(0);
                commit("('2026-01-01T11:00:06.000000Z', 'acct-3', 3.0)", job);
                assertDirtySetsClearedByPublish();
                Assert.assertTrue(
                        "the recovering seal must publish a new generation",
                        anchorWindow().getCheckpointBaselineGeneration() > baselineGeneration
                );
                assertViewMatchesRecompute(NOON_ANCHOR);
            }

            // The proof that the three keys the failed seals owed were all written: a
            // restart reads the root and nothing else.
            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
                Assert.assertTrue(viewInstance().isCheckpointRestoreSucceeded());
                driveRefreshToQuiescence(job);
                assertViewMatchesRecompute(NOON_ANCHOR);
                commit("('2026-01-01T11:00:07.000000Z', 'acct-1', 7.0)", job);
                assertViewMatchesRecompute(NOON_ANCHOR);
            }
        });
    }

    /**
     * The end-state case at the tightest cadence there is - one boundary per row, so the
     * batch that sweeps is the batch that seals and nothing of the sweep's own state
     * outlives the publication. That makes this the one sweep case here that cannot
     * assert the incremental gate: the gate opens and closes inside a single refresh, and
     * the demoting full scan this change replaced arrives at the same root anyway. What
     * it holds is the end state - the root drops the evicted keys and a restart neither
     * resurrects them nor loses the survivor. {@link
     * #testFrontierSweepRecordsEvictionsAndKeepsTheSealIncremental} carries the gate.
     */
    @Test
    public void testFrontierCompactionDropsEvictedKeysFromTheRoot() throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        // Low enough that the sweep fires as soon as the anchor advances past a
        // bucket and the map is holding more than a couple of accounts.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_PARTITION_COMPACT_THRESHOLD, 2);
        assertMemoryLeak(() -> {
            createView(MIDNIGHT_ANCHOR, SEED_FOUR_ACCOUNTS);
            final long survivors;
            final LongList sealedLogicalBytes;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                assertViewMatchesRecompute(MIDNIGHT_ANCHOR);
                Assert.assertEquals(4, anchorWindow().getAnchorMapSize());
                Assert.assertEquals(0, anchorWindow().getCompactionCount());

                // One boundary per row, which is the tightest cadence the sweep can land
                // in: the batch that sweeps is the batch that seals. The bucket advances
                // with only acct-1 following along, and the second advance puts the other
                // three accounts a whole bucket behind the frontier - the eviction cutoff.
                commit("('2026-01-02T01:00:00.000000Z', 'acct-1', 1.0)", job);
                commit("('2026-01-03T01:00:00.000000Z', 'acct-1', 2.0)", job);
                commit("('2026-01-04T01:00:00.000000Z', 'acct-1', 3.0)", job);
                Assert.assertTrue(
                        "the frontier sweep must have run",
                        anchorWindow().getCompactionCount() > 0
                );
                survivors = anchorWindow().getAnchorMapSize();
                Assert.assertTrue(
                        "the sweep must have evicted the behind-frontier accounts, map size=" + survivors,
                        survivors < 4
                );
                // The recorded removals are what take the evicted accounts out of the
                // root; before, only the complete freeze the sweep forced could.
                assertHeadRootPartitionCount((int) survivors);
                assertViewMatchesRecompute(MIDNIGHT_ANCHOR);
                assertDirtySetsClearedByPublish();
                sealedLogicalBytes = readLogicalStateBytes();
            }

            // If the seal had kept the evicted accounts, they would come back to life
            // here carrying an accumulator no live row supports.
            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
                Assert.assertTrue(viewInstance().isCheckpointRestoreSucceeded());
                Assert.assertEquals(
                        "the restore must rehydrate the swept map, not the pre-sweep one",
                        survivors,
                        anchorWindow().getAnchorMapSize()
                );
                assertLogicalStateBytesEqual(sealedLogicalBytes);
                driveRefreshToQuiescence(job);
                assertViewMatchesRecompute(MIDNIGHT_ANCHOR);

                // An evicted account comes back. It starts a fresh bucket, so its
                // accumulator restarts - resurrected state from the root would show up
                // as an inflated sum.
                commit("('2026-01-04T01:00:01.000000Z', 'acct-2', 4.0)", job);
                assertViewMatchesRecompute(MIDNIGHT_ANCHOR);
            }
        });
    }

    @Test
    public void testFrontierSweepRecordsEvictionsAndKeepsTheSealIncremental() throws Exception {
        // Four rows per boundary, so the sweep lands mid-cadence and the state it leaves
        // behind is readable before the seal consumes it.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 4);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_PARTITION_COMPACT_THRESHOLD, 2);
        assertMemoryLeak(() -> {
            createView(MIDNIGHT_ANCHOR, SEED_FOUR_ACCOUNTS);
            final long survivors;
            final LongList sealedLogicalBytes;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                assertDirtySetsClearedByPublish();
                assertHeadRootPartitionCount(4);
                final long generation = publishedGeneration();

                // Two bucket advances with only acct-1 following along, both under the
                // four-row boundary, so the second one sweeps without sealing.
                commit("('2026-01-02T01:00:00.000000Z', 'acct-1', 1.0)", job);
                commit("('2026-01-03T01:00:00.000000Z', 'acct-1', 2.0)", job);
                Assert.assertEquals(1, anchorWindow().getCompactionCount());
                survivors = anchorWindow().getAnchorMapSize();
                Assert.assertEquals("only the account that followed the frontier survives", 1, survivors);

                // The claim under test: the sweep no longer pins the next seal to a
                // complete freeze of every live key of every function.
                assertIncrementalGateOpen(generation);

                // What replaced the demotion - the evicted keys are named, alongside the
                // one account the two commits touched.
                Assert.assertEquals(4, anchorWindow().getCheckpointDirtyAnchorMapSize());
                assertFunctionDirtySize(4);
                assertEvictionMarkerCount(3);

                // The fourth row seals, and the recorded removals are what take the three
                // evicted accounts out of the root.
                commit("('2026-01-03T02:00:00.000000Z', 'acct-1', 3.0), "
                        + "('2026-01-03T03:00:00.000000Z', 'acct-1', 4.0)", job);
                assertDirtySetsClearedByPublish();
                assertHeadRootPartitionCount((int) survivors);
                assertViewMatchesRecompute(MIDNIGHT_ANCHOR);
                sealedLogicalBytes = readLogicalStateBytes();
            }

            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
                Assert.assertTrue(viewInstance().isCheckpointRestoreSucceeded());
                Assert.assertEquals(survivors, anchorWindow().getAnchorMapSize());
                assertLogicalStateBytesEqual(sealedLogicalBytes);
                driveRefreshToQuiescence(job);
                assertViewMatchesRecompute(MIDNIGHT_ANCHOR);

                commit("('2026-01-03T04:00:00.000000Z', 'acct-2', 5.0)", job);
                assertViewMatchesRecompute(MIDNIGHT_ANCHOR);
            }
        });
    }

    @Test
    public void testKeyEvictedThenRecreatedInOneCadenceIsUpserted() throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 4);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_PARTITION_COMPACT_THRESHOLD, 2);
        assertMemoryLeak(() -> {
            createView(MIDNIGHT_ANCHOR, SEED_FOUR_ACCOUNTS);
            final LongList sealedLogicalBytes;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                assertDirtySetsClearedByPublish();
                final long generation = publishedGeneration();
                final long sealFailuresBefore = viewInstance().getCheckpointSealFailures();

                commit("('2026-01-02T01:00:00.000000Z', 'acct-1', 1.0)", job);
                commit("('2026-01-03T01:00:00.000000Z', 'acct-1', 2.0)", job);
                Assert.assertEquals(1, anchorWindow().getCompactionCount());
                assertEvictionMarkerCount(3);

                // acct-2 was swept two rows ago and is back inside the same cadence. Its
                // dirty entry now has to mean an upsert again: emitting both the removal
                // the sweep recorded and the put this row asks for names one key twice,
                // which the partition-map writer rejects outright.
                commit("('2026-01-03T02:00:00.000000Z', 'acct-2', 3.0)", job);
                assertEvictionMarkerCount(2);
                assertIncrementalGateOpen(generation);

                commit("('2026-01-03T03:00:00.000000Z', 'acct-1', 4.0)", job);
                Assert.assertEquals(
                        "the re-created key must not have produced a duplicate mutation",
                        sealFailuresBefore,
                        viewInstance().getCheckpointSealFailures()
                );
                assertDirtySetsClearedByPublish();
                assertHeadRootPartitionCount(2);
                assertViewMatchesRecompute(MIDNIGHT_ANCHOR);
                sealedLogicalBytes = readLogicalStateBytes();
            }

            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
                Assert.assertTrue(viewInstance().isCheckpointRestoreSucceeded());
                Assert.assertEquals(2, anchorWindow().getAnchorMapSize());
                assertLogicalStateBytesEqual(sealedLogicalBytes);
                driveRefreshToQuiescence(job);
                assertViewMatchesRecompute(MIDNIGHT_ANCHOR);

                // The re-created account's accumulator restarted with its new bucket, so a
                // resurrected pre-sweep image shows up here as an inflated sum.
                commit("('2026-01-03T05:00:00.000000Z', 'acct-2', 6.0)", job);
                assertViewMatchesRecompute(MIDNIGHT_ANCHOR);
            }
        });
    }

    @Test
    public void testKeyTouchedThenEvictedInOneCadenceIsRemoved() throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 4);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_PARTITION_COMPACT_THRESHOLD, 2);
        assertMemoryLeak(() -> {
            createView(MIDNIGHT_ANCHOR, SEED_FOUR_ACCOUNTS);
            final LongList sealedLogicalBytes;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                assertDirtySetsClearedByPublish();
                final long generation = publishedGeneration();

                // acct-2 is dirty before the cadence crosses a single bucket boundary, and
                // swept two boundaries later - inside the same cadence. Nothing bounds a
                // cadence to one bucket, so the dirty entry has to carry both facts and the
                // seal has to land on the removal.
                commit("('2026-01-01T12:00:00.000000Z', 'acct-2', 1.0)", job);
                commit("('2026-01-02T01:00:00.000000Z', 'acct-1', 2.0)", job);
                commit("('2026-01-03T01:00:00.000000Z', 'acct-1', 3.0)", job);
                Assert.assertEquals(1, anchorWindow().getCompactionCount());
                Assert.assertEquals(1, anchorWindow().getAnchorMapSize());
                assertEvictionMarkerCount(3);
                assertIncrementalGateOpen(generation);

                commit("('2026-01-03T02:00:00.000000Z', 'acct-1', 4.0)", job);
                assertDirtySetsClearedByPublish();
                assertHeadRootPartitionCount(1);
                assertViewMatchesRecompute(MIDNIGHT_ANCHOR);
                sealedLogicalBytes = readLogicalStateBytes();
            }

            // The accounting proof: a restore recomputes the logical size by walking the
            // root it reads, so a seal that subtracted the touched-then-evicted key twice
            // - or not at all - disagrees with it here.
            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
                Assert.assertTrue(viewInstance().isCheckpointRestoreSucceeded());
                Assert.assertEquals(1, anchorWindow().getAnchorMapSize());
                assertLogicalStateBytesEqual(sealedLogicalBytes);
                driveRefreshToQuiescence(job);
                assertViewMatchesRecompute(MIDNIGHT_ANCHOR);
            }
        });
    }

    @Test
    public void testRepeatedCorrectionsReplayABoundedRangeRatherThanAGrowingOne() throws Exception {
        // One boundary per commit, so the ladder below is dense enough for a correction
        // to land within a cadence of a boundary - if the repair before it left the
        // boundaries standing.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        // What this case measures is the range a resume replays, so it pins the resume to
        // one route. A keyed resume reads its own keys rather than every row above the
        // anchor, so a run that switched routes mid-way would compare two different
        // measurements; the route switch itself belongs to the open-segment pricing cases.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_OPEN_SEGMENT_KEYED_REPLAY_ENABLED, "false");
        assertMemoryLeak(() -> {
            createView(
                    NOON_ANCHOR,
                    "('2026-01-01T11:00:00.000000Z', 'acct-1', 1.0), "
                            + "('2026-01-01T11:00:01.000000Z', 'acct-2', 1.0)"
            );
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);

                // A ladder deep enough to hold a boundary under every correction below.
                for (int second = 10; second < 30; second++) {
                    commit(row("acct-1", second), job);
                }

                // Then a run of corrections at a fixed lateness - ten seconds under the
                // head, which is what makes this the case it is. Each late row sits below
                // the frontier the repair before it reached, so the only boundary that can
                // anchor it is one that repair had to decide the fate of.
                //
                // That decision is the whole point. A repair used to truncate every
                // boundary above its floor and seal exactly one new one, at the frontier -
                // which the next late row, being below the frontier by definition, could
                // not use. The newest usable anchor stayed pinned where the ladder ended
                // before the first correction, and the replayed range grew by one commit
                // per correction, without bound and regardless of how late the rows were.
                final LongList replayed = new LongList();
                long previous = viewInstance().getO3ResumeReplayRows();
                for (int i = 0; i < 6; i++) {
                    final int head = 30 + i * 2;
                    commit(row("acct-1", head), job);
                    commit(row("acct-2", head - 10), job);
                    final long now = viewInstance().getO3ResumeReplayRows();
                    Assert.assertTrue("correction " + i + " must repair through a resume", now > previous);
                    replayed.add(now - previous);
                    previous = now;
                }
                Assert.assertEquals(
                        "the route is pinned, so nothing here may resume by key",
                        0,
                        job.openSegmentKeyedResumeCountForTest()
                );

                // Bounded, not growing. Every correction resumes from a boundary the
                // repair before it re-versioned in place, so what it replays is its own
                // depth plus a cadence rather than everything the view has emitted since
                // the ladder was last intact. The run may drift downwards as corrections
                // climb out of the dense ladder; what it must never do is climb.
                for (int i = 1, n = replayed.size(); i < n; i++) {
                    Assert.assertTrue(
                            "repair " + i + " must not replay more than the first one did [replayed="
                                    + replayed + ']',
                            replayed.getQuick(i) <= replayed.getQuick(0)
                    );
                }
                assertViewMatchesRecompute(NOON_ANCHOR);
            }

            // The ladder those repairs kept is only worth keeping if it restores, so the
            // restart reads every root they re-versioned rather than the runtime's memory.
            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
                Assert.assertTrue(viewInstance().isCheckpointRestoreSucceeded());
                driveRefreshToQuiescence(job);
                assertViewMatchesRecompute(NOON_ANCHOR);
            }
        });
    }

    @Test
    public void testTheSealAboveASplicedLadderStaysIncremental() throws Exception {
        // One boundary per commit, so the repair below crosses two of them and still has
        // a frontier above the newest to seal.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            createView(
                    NOON_ANCHOR,
                    "('2026-01-01T11:00:00.000000Z', 'acct-1', 10.0), "
                            + "('2026-01-01T11:00:01.000000Z', 'acct-2', 20.0), "
                            + "('2026-01-01T11:00:02.000000Z', 'acct-3', 30.0), "
                            + "('2026-01-01T11:00:03.000000Z', 'acct-4', 40.0), "
                            + "('2026-01-01T11:00:04.000000Z', 'acct-5', 50.0), "
                            + "('2026-01-01T11:00:05.000000Z', 'acct-6', 60.0)"
            );
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row("acct-1", 10), job);
                commit(row("acct-2", 11), job);

                // One commit carrying both a late row and a forward one, so the replay
                // runs past the newest boundary it re-versions and the repair owes a seal
                // above the ladder it just spliced.
                commit(row("acct-1", 6) + ", " + row("acct-3", 20), job);

                // That seal stands on the newest spliced boundary and images only what
                // the replay moved above it - acct-3's single row - rather than the six
                // keys the domain holds.
                //
                // It is the batch-minimum window that decides which of those two it does.
                // A seal shares its predecessor's chunks only against a batch it can prove
                // sits strictly above that predecessor, and the replay's own minimum sits
                // well below it: the freeze cursor restarts the window at every boundary
                // it publishes, which is what a cadence seal gets from setHeadCheckpoint
                // and a truncating repair from the head it clears. Without the restart
                // this reads six.
                Assert.assertEquals(
                        "the seal above a spliced ladder must image only what the replay moved above it",
                        1,
                        anchorWindow().getCheckpointLastFreezeKeyCount()
                );
                assertHeadRootPartitionCount(6);
                assertViewMatchesRecompute(NOON_ANCHOR);
            }

            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
                Assert.assertTrue(viewInstance().isCheckpointRestoreSucceeded());
                driveRefreshToQuiescence(job);
                assertViewMatchesRecompute(NOON_ANCHOR);
                commit(row("acct-5", 30) + ", " + row("acct-6", 31), job);
                assertViewMatchesRecompute(NOON_ANCHOR);
            }
        });
    }

    @Test
    public void testResumeDeclinedTheChainFallsBackToTheTruncate() throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        // Zero declines the chain for every repair, whatever its depth. It is what a
        // correction reaching further back than the budget takes in the field, and the
        // case exists because that fallback still has to produce a correct view: the
        // repair truncates the ladder above its floor and seals one fresh head, which is
        // what every out-of-order repair did before the chain.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_MAX_CHAINED_BOUNDARIES, 0);
        assertMemoryLeak(() -> {
            createView(
                    NOON_ANCHOR,
                    "('2026-01-01T11:00:00.000000Z', 'acct-1', 10.0), "
                            + "('2026-01-01T11:00:01.000000Z', 'acct-2', 20.0), "
                            + "('2026-01-01T11:00:02.000000Z', 'acct-3', 30.0)"
            );
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit("('2026-01-01T11:00:10.000000Z', 'acct-1', 1.0)", job);
                commit("('2026-01-01T11:00:11.000000Z', 'acct-2', 2.0)", job);
                final LongList idsBefore = logicalCheckpointIds();

                final long resumeBefore = viewInstance().getO3ResumeReplayRows();
                commit("('2026-01-01T11:00:06.000000Z', 'acct-1', 5.0)", job);
                Assert.assertTrue(
                        "a late row with a boundary below it must repair through a resume",
                        viewInstance().getO3ResumeReplayRows() > resumeBefore
                );

                // The discriminator: a truncate drops the logical entries above its floor
                // where the chain would have re-versioned them in place.
                final LongList ids = logicalCheckpointIds();
                Assert.assertTrue(
                        "the declined chain must truncate rather than splice [before=" + idsBefore
                                + ", after=" + ids + ']',
                        ids.size() < idsBefore.size()
                );
                assertHeadRootPartitionCount(3);
                assertViewMatchesRecompute(NOON_ANCHOR);
            }

            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
                driveRefreshToQuiescence(job);
                assertViewMatchesRecompute(NOON_ANCHOR);
                commit("('2026-01-01T11:00:20.000000Z', 'acct-3', 3.0)", job);
                assertViewMatchesRecompute(NOON_ANCHOR);
            }
        });
    }

    @Test
    public void testResumeFromAnchorImagesOnlyTheKeysItsReplayTouched() throws Exception {
        // One boundary per commit, so the late row below has an anchor strictly under it
        // and the seal that closes the repair has a predecessor root to build on.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            createView(
                    NOON_ANCHOR,
                    "('2026-01-01T11:00:00.000000Z', 'acct-1', 10.0), "
                            + "('2026-01-01T11:00:01.000000Z', 'acct-2', 20.0), "
                            + "('2026-01-01T11:00:02.000000Z', 'acct-3', 30.0), "
                            + "('2026-01-01T11:00:03.000000Z', 'acct-4', 40.0), "
                            + "('2026-01-01T11:00:04.000000Z', 'acct-5', 50.0), "
                            + "('2026-01-01T11:00:05.000000Z', 'acct-6', 60.0)"
            );
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                assertHeadRootPartitionCount(6);

                // Two forward commits, so the timeline carries boundaries above the seed's
                // and the repair below has one to drop as well as one to keep.
                commit("('2026-01-01T11:00:10.000000Z', 'acct-1', 1.0)", job);
                commit("('2026-01-01T11:00:11.000000Z', 'acct-2', 2.0)", job);
                assertViewMatchesRecompute(NOON_ANCHOR);

                final long resumeBefore = viewInstance().getO3ResumeReplayRows();
                final long imagedBefore = anchorWindow().getCheckpointFreezeKeyCountTotal();
                final LongList idsBefore = logicalCheckpointIds();
                // A late row above the seed's boundary and below both commits. The repair
                // resumes from that boundary, so its replay reads the three rows above it -
                // which move acct-1 and acct-2 and no other account.
                commit("('2026-01-01T11:00:06.000000Z', 'acct-1', 5.0)", job);
                Assert.assertTrue(
                        "a late row with a boundary below it must repair through a resume",
                        viewInstance().getO3ResumeReplayRows() > resumeBefore
                );

                // The repair re-versions the two boundaries its replay crosses rather than
                // dropping them, and freezes each against the one below it: the first
                // images acct-1, whose two rows sit at or below it, and the second images
                // acct-2. Two key images for the whole repair.
                //
                // What this is written against is the freeze that walks the live domain
                // instead. Restoring the anchor without a baseline made every repair seal
                // do that once; re-versioning a ladder without chaining would make it do
                // that once per boundary. Either reads as six here, and as twenty million
                // per boundary in the workload this is written for.
                Assert.assertEquals(
                        "the repair must image the keys its replay touched, once, not the live domain",
                        2,
                        anchorWindow().getCheckpointFreezeKeyCountTotal() - imagedBefore
                );
                Assert.assertEquals(
                        "no boundary of the chain may fall back to a complete freeze",
                        1,
                        anchorWindow().getCheckpointLastFreezeKeyCount()
                );

                // The ladder survives the repair, in place: the next late row below the
                // frontier then resumes from a boundary within one cadence of itself
                // instead of from wherever the last in-order commit left one.
                final LongList ids = logicalCheckpointIds();
                Assert.assertEquals("the repair must drop no logical entry", idsBefore.size(), ids.size());
                for (int i = 0, n = ids.size(); i < n; i++) {
                    Assert.assertEquals("the ladder keeps its ids in place", idsBefore.getQuick(i), ids.getQuick(i));
                }

                // The other four accounts keep the entries the anchor root already holds,
                // so the root the repair published must still name all six.
                assertHeadRootPartitionCount(6);
                assertViewMatchesRecompute(NOON_ANCHOR);
            }

            // An incremental seal that dropped or staled an untouched key is invisible
            // while the runtime serves from memory; the restart is what reads the root.
            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
                Assert.assertTrue(viewInstance().isCheckpointRestoreSucceeded());
                driveRefreshToQuiescence(job);
                assertViewMatchesRecompute(NOON_ANCHOR);

                // Rows for accounts the repair seal left alone. A root that lost their
                // accumulators shows up here as a cumulative sum restarting from this row
                // rather than as a missing key.
                commit("('2026-01-01T11:00:20.000000Z', 'acct-3', 3.0), "
                        + "('2026-01-01T11:00:21.000000Z', 'acct-6', 6.0)", job);
                assertViewMatchesRecompute(NOON_ANCHOR);
            }
        });
    }

    @Test
    public void testRestoreFromANonHeadRootDoesNotAdoptAnIncrementalBaseline() throws Exception {
        // One boundary per commit, so the timeline below holds several of them and the
        // head has a predecessor to restore instead.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            createView(
                    NOON_ANCHOR,
                    "('2026-01-01T11:00:00.000000Z', 'acct-1', 10.0), "
                            + "('2026-01-01T11:00:01.000000Z', 'acct-2', 20.0)"
            );
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit("('2026-01-01T11:00:02.000000Z', 'acct-3', 30.0)", job);
                assertDirtySetsClearedByPublish();
            }

            final LiveViewCheckpointTimelineEntry head = new LiveViewCheckpointTimelineEntry();
            final LiveViewCheckpointTimelineEntry predecessor = new LiveViewCheckpointTimelineEntry();
            readHeadBoundaries(head, predecessor);

            // A cadence seal always builds on the timeline head, so state restored from
            // any other root does not describe the entries an incremental seal would
            // leave alone. Restoring the predecessor must therefore leave the runtime on
            // the full scan even though the generation is the one a seal would name.
            final long generation = restoreRoot(predecessor.maxTimestamp, predecessor.checkpointId);
            Assert.assertFalse(
                    "restoring a predecessor must leave the seal's own gate shut",
                    anchorWindow().canFreezeCheckpointIncrementally(generation)
            );
            assertPinnedToFullScan();

            // The same reader, the same runtime, the same generation - only the root
            // differs, and that is what decides it.
            Assert.assertEquals(generation, restoreRoot(head.maxTimestamp, head.checkpointId));
            assertIncrementalBaseline(generation);
        });
    }

    @Test
    public void testRestoreFromTheHeadRootLeavesTheNextSealIncremental() throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            createView(
                    NOON_ANCHOR,
                    "('2026-01-01T11:00:00.000000Z', 'acct-1', 10.0), "
                            + "('2026-01-01T11:00:01.000000Z', 'acct-2', 20.0), "
                            + "('2026-01-01T11:00:02.000000Z', 'acct-3', 30.0), "
                            + "('2026-01-01T11:00:03.000000Z', 'acct-4', 40.0)"
            );
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                assertDirtySetsClearedByPublish();
                assertHeadRootPartitionCount(4);
                assertViewMatchesRecompute(NOON_ANCHOR);
            }
            final long restoredGeneration = publishedGeneration();

            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
                Assert.assertTrue(viewInstance().isCheckpointRestoreSucceeded());
                // Reading a root back publishes nothing, so the generation the restore
                // stamped on the runtime is still the one the next seal builds on.
                Assert.assertEquals(
                        "a restore must not publish a generation of its own",
                        restoredGeneration,
                        publishedGeneration()
                );
                assertIncrementalBaseline(restoredGeneration);

                // The first seal after the resume therefore freezes the one key this
                // commit touches. The other three accounts keep the entries the restored
                // root already holds - so the root must still name all four, and their
                // accumulators must survive the next read-back.
                driveRefreshToQuiescence(job);
                commit("('2026-01-01T11:00:04.000000Z', 'acct-1', 1.0)", job);
                assertDirtySetsClearedByPublish();
                assertHeadRootPartitionCount(4);
                assertViewMatchesRecompute(NOON_ANCHOR);
            }

            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
                Assert.assertTrue(viewInstance().isCheckpointRestoreSucceeded());
                driveRefreshToQuiescence(job);
                assertViewMatchesRecompute(NOON_ANCHOR);

                // Rows for the accounts the incremental seal left alone. A root that
                // dropped or staled their entries shows up here as a restarted or
                // inflated accumulator rather than as a missing key.
                commit("('2026-01-01T11:00:05.000000Z', 'acct-2', 2.0), "
                        + "('2026-01-01T11:00:06.000000Z', 'acct-3', 3.0), "
                        + "('2026-01-01T11:00:07.000000Z', 'acct-4', 4.0)", job);
                assertViewMatchesRecompute(NOON_ANCHOR);
            }
        });
    }

    /**
     * The fail-safe the three-argument retention default exists for. A function that
     * keeps an incremental baseline, implements retention and never learns to record the
     * sweep's evictions would publish a root still naming keys the runtime dropped, so
     * the sweep's entry point refuses rather than delegates.
     */
    @Test
    public void testRetentionWithoutRemovalTrackingCannotStayIncremental() {
        final RetainingFunctionStub incremental = new RetainingFunctionStub(false);
        try {
            incremental.retainPartitions(null, null, false);
            Assert.fail("retention without removal tracking must not stay incremental");
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "without checkpoint removal tracking");
        }
        Assert.assertFalse(
                "the guard must refuse before the retention itself runs",
                incremental.isRetained
        );

        // Recording the removals is what opens the door.
        incremental.retainPartitions(null, null, true);
        Assert.assertTrue(incremental.isRetained);

        // A function already committed to a complete freeze needs no door: the freeze
        // walks its whole map and finds the dropped keys on its own.
        final RetainingFunctionStub fullScan = new RetainingFunctionStub(true);
        fullScan.retainPartitions(null, null, false);
        Assert.assertTrue(fullScan.isRetained);
    }

    @Test
    public void testTheCadenceCounterWrappingStillNamesEveryTouchedKey() throws Exception {
        // Four rows per boundary, as in testARepeatedKeyEntersTheDirtySetOncePerCadence and
        // testTouchedKeysAreTheOnlyDirtyStateBetweenSeals.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 4);
        assertMemoryLeak(() -> {
            createView(
                    NOON_ANCHOR,
                    "('2026-01-01T11:00:00.000000Z', 'acct-1', 10.0), "
                            + "('2026-01-01T11:00:01.000000Z', 'acct-2', 20.0), "
                            + "('2026-01-01T11:00:02.000000Z', 'acct-3', 30.0), "
                            + "('2026-01-01T11:00:03.000000Z', 'acct-4', 40.0)"
            );
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                assertDirtySetsClearedByPublish();

                // Stamp acct-1 with the cadence the counter's turn will land back on. The
                // counter is a SHORT and restarts at 1 when it turns over, so a key
                // stamped 1 and then left alone is the one the turn collides with - and a
                // key whose stamp matches skips its mark, which takes it out of the
                // incremental root with nothing to say so.
                anchorWindow().setCheckpointDirtyEpoch((short) 1);
                commit("('2026-01-01T11:00:04.000000Z', 'acct-1', 1.0)", job);
                Assert.assertEquals(1, anchorWindow().getCheckpointDirtyAnchorMapSize());

                // Stand the counter one cadence below its turn, which is what 32765 quiet
                // cadences would have done, and cross the boundary on OTHER keys so
                // acct-1's stamp is the stale one rather than a fresh one.
                anchorWindow().setCheckpointDirtyEpoch(Short.MAX_VALUE);
                commit("('2026-01-01T11:00:05.000000Z', 'acct-2', 2.0), "
                        + "('2026-01-01T11:00:06.000000Z', 'acct-3', 3.0), "
                        + "('2026-01-01T11:00:07.000000Z', 'acct-4', 4.0)", job);
                assertDirtySetsClearedByPublish();

                // The counter has turned back onto acct-1's stamp. Its next row must still
                // be named.
                final long marksAfterWrap = anchorWindow().getCheckpointDirtyMarkCount();
                commit("('2026-01-01T11:00:08.000000Z', 'acct-1', 5.0)", job);
                Assert.assertEquals(
                        "a key stamped with the cadence the turn lands on must be named again",
                        1,
                        anchorWindow().getCheckpointDirtyAnchorMapSize()
                );
                Assert.assertEquals(
                        1,
                        anchorWindow().getCheckpointDirtyMarkCount() - marksAfterWrap
                );
                assertViewMatchesRecompute(NOON_ANCHOR);
            }

            // The incremental root the wrapped cadence published has to hold both keys,
            // which is the consequence a skipped mark would have destroyed.
            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
                Assert.assertTrue(viewInstance().isCheckpointRestoreSucceeded());
                driveRefreshToQuiescence(job);
                assertViewMatchesRecompute(NOON_ANCHOR);
            }
        });
    }

    /**
     * The same fail-safe {@link #testRetentionWithoutRemovalTrackingCannotStayIncremental} holds,
     * one step earlier, at the hook the sweep calls per evicted key. A partitioned function that
     * offers everything but the marking - a tombstone slot and a scratch-map factory, with a
     * markPartitionAlive that names nothing - must have the hook decline for it, because the
     * dirty set it would otherwise build names none of the keys the function's own rows moved.
     */
    @Test
    public void testTheSweepCannotStartDirtyTrackingOnItsOwn() throws Exception {
        assertMemoryLeak(() -> {
            final LongKeyRecordStub record = new LongKeyRecordStub();
            final NonMarkingFunctionStub function = new NonMarkingFunctionStub();
            try {
                // The state a published seal leaves behind, and the only state an
                // incremental freeze can build on at all.
                function.onCheckpointPersisted(0, 7);
                Assert.assertFalse(function.isCheckpointFullScanRequired());

                record.value = 1;
                Assert.assertFalse(
                        "a function that names no touched key must decline the eviction hook",
                        function.markCheckpointPartitionEvicted(record, LongKeyRecordStub.SINK)
                );
                Assert.assertNull(
                        "declining must leave no dirty set for the seal to read",
                        function.getCheckpointDirtyPartitionMap()
                );

                // The same function once it does mark, which is what says the guard reads
                // the tracking rather than refusing outright.
                function.markDirty(record);
                Assert.assertEquals(1, function.getCheckpointDirtyPartitionMap().size());
                record.value = 2;
                Assert.assertTrue(
                        function.markCheckpointPartitionEvicted(record, LongKeyRecordStub.SINK)
                );
                Assert.assertEquals(2, function.getCheckpointDirtyPartitionMap().size());
            } finally {
                function.reset();
            }

            // reset() hands the dirty set back, so the tracking has to go with it: a rebound
            // cursor earns it again through the marking or not at all.
            Assert.assertNull(function.getCheckpointDirtyPartitionMap());
            Assert.assertFalse(
                    "reset() must take the tracking with the map it freed",
                    function.markCheckpointPartitionEvicted(record, LongKeyRecordStub.SINK)
            );
            function.reset();
        });
    }

    @Test
    public void testTombstonedTouchedKeyIsRemovedFromTheRoot() throws Exception {
        // Four rows per boundary, so the seed seals and the batch below is one boundary.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 4);
        assertMemoryLeak(() -> {
            // A residual function, because the removal branch this case holds to its
            // contract is the per-function one - see createUnfusedView.
            createUnfusedView(
                    NOON_ANCHOR,
                    "('2026-01-01T11:00:00.000000Z', 'acct-1', 10.0), "
                            + "('2026-01-01T11:00:01.000000Z', 'acct-2', 20.0), "
                            + "('2026-01-01T11:00:02.000000Z', 'acct-3', 30.0), "
                            + "('2026-01-01T11:00:03.000000Z', 'acct-4', 40.0)"
            );
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                assertDirtySetsClearedByPublish();
                assertHeadRootPartitionCount(4);

                // No supported path leaves a tombstone standing at a seal: resetPartition
                // is the only writer of the bit, and processRow cancels it through
                // markPartitionAlive on the very same row. The removal branch would
                // therefore never run, so poke the bits in directly and hold the branch
                // to its contract - it deletes the key from the root, which is the one
                // thing that must not happen by accident.
                tombstoneEveryPartition();
                commit("('2026-01-01T11:00:04.000000Z', 'acct-1', 1.0), "
                        + "('2026-01-01T11:00:05.000000Z', 'acct-2', 2.0), "
                        + "('2026-01-01T11:00:06.000000Z', 'acct-3', 3.0), "
                        + "('2026-01-01T11:00:07.000000Z', 'acct-4', 4.0)", job);
                assertDirtySetsClearedByPublish();
                assertHeadRootPartitionCount(0);
            }
        });
    }

    @Test
    public void testTouchedKeysAreTheOnlyDirtyStateBetweenSeals() throws Exception {
        // Four rows per boundary, so a commit smaller than that refreshes without
        // sealing and leaves the dirty sets readable mid-cadence.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 4);
        assertMemoryLeak(() -> {
            createView(
                    NOON_ANCHOR,
                    "('2026-01-01T11:00:00.000000Z', 'acct-1', 10.0), "
                            + "('2026-01-01T11:00:01.000000Z', 'acct-2', 20.0), "
                            + "('2026-01-01T11:00:02.000000Z', 'acct-3', 30.0), "
                            + "('2026-01-01T11:00:03.000000Z', 'acct-4', 40.0), "
                            + "('2026-01-01T11:00:04.000000Z', 'acct-5', 50.0), "
                            + "('2026-01-01T11:00:05.000000Z', 'acct-6', 60.0)"
            );
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                assertDirtySetsClearedByPublish();
                Assert.assertEquals(6, anchorWindow().getAnchorMapSize());

                // One row, one key, no seal. The dirty sets now name what the next seal
                // owes the root, and what they name is the batch rather than the view:
                // this assertion holds at six accounts and at six million.
                commit("('2026-01-01T11:00:06.000000Z', 'acct-1', 1.0)", job);
                Assert.assertEquals(
                        "the anchor dirty set must name the touched key and no other",
                        1,
                        anchorWindow().getCheckpointDirtyAnchorMapSize()
                );
                assertFunctionDirtySize(1);
                assertFunctionStateSize(6);

                // Two more rows over one further key. Still under the boundary, so the
                // dirty sets accumulate rather than reset, and a key touched twice is
                // still one entry.
                commit("('2026-01-01T11:00:07.000000Z', 'acct-2', 2.0), "
                        + "('2026-01-01T11:00:08.000000Z', 'acct-2', 3.0)", job);
                Assert.assertEquals(2, anchorWindow().getCheckpointDirtyAnchorMapSize());
                assertFunctionDirtySize(2);

                // The fourth row crosses the boundary and seals.
                commit("('2026-01-01T11:00:09.000000Z', 'acct-3', 4.0)", job);
                assertDirtySetsClearedByPublish();
                assertViewMatchesRecompute(NOON_ANCHOR);
            }

            final long restoredGeneration = publishedGeneration();
            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
                Assert.assertTrue(viewInstance().isCheckpointRestoreSucceeded());
                driveRefreshToQuiescence(job);
                assertViewMatchesRecompute(NOON_ANCHOR);
                // The restore rehydrated the head root, so the runtime holds exactly what
                // that root holds and the seal after it stays on the touched-key path.
                Assert.assertEquals(
                        restoredGeneration,
                        anchorWindow().getCheckpointBaselineGeneration()
                );
            }
        });
    }

    @Test
    public void testUnrelatedDirtyAnchorKeyMissingAfterASweepStillFails() throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 4);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_PARTITION_COMPACT_THRESHOLD, 2);
        assertMemoryLeak(() -> {
            createView(MIDNIGHT_ANCHOR, SEED_FOUR_ACCOUNTS);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                assertDirtySetsClearedByPublish();
                final long sealFailuresBefore = viewInstance().getCheckpointSealFailures();

                commit("('2026-01-02T01:00:00.000000Z', 'acct-1', 1.0)", job);
                commit("('2026-01-03T01:00:00.000000Z', 'acct-1', 2.0)", job);
                Assert.assertEquals(1, anchorWindow().getCompactionCount());
                assertEvictionMarkerCount(3);

                // One of the three keys the sweep dropped loses its provenance while the
                // other two keep theirs. A sweep-wide "something was evicted" flag would
                // wave this one through and publish a root missing an entry no sweep took
                // out; the per-key marker is what makes it a hard error.
                Assert.assertEquals(1, anchorWindow().clearCheckpointEvictionMarkers(1));
                Assert.assertEquals(2, anchorWindow().getCheckpointEvictionMarkerCount());
                commit("('2026-01-03T02:00:00.000000Z', 'acct-1', 3.0), "
                        + "('2026-01-03T03:00:00.000000Z', 'acct-1', 4.0)", job);
                Assert.assertTrue(
                        "a dirty anchor key missing without its own marker must fail the seal",
                        viewInstance().getCheckpointSealFailures() > sealFailuresBefore
                );
            }
        });
    }

    @Test
    public void testUnrelatedDirtyFunctionKeyMissingAfterASweepStillFails() throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 4);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_PARTITION_COMPACT_THRESHOLD, 2);
        assertMemoryLeak(() -> {
            // A residual function, because its own dirty map is what this case breaks -
            // see createUnfusedView.
            createUnfusedView(MIDNIGHT_ANCHOR, SEED_FOUR_ACCOUNTS);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                assertDirtySetsClearedByPublish();
                final long sealFailuresBefore = viewInstance().getCheckpointSealFailures();

                commit("('2026-01-02T01:00:00.000000Z', 'acct-1', 1.0)", job);
                commit("('2026-01-03T01:00:00.000000Z', 'acct-1', 2.0)", job);
                Assert.assertEquals(1, anchorWindow().getCompactionCount());

                // The same break, one channel over: the anchor keeps every marker it
                // recorded and freezes cleanly, so the raise has to come from the function.
                Assert.assertTrue(clearFunctionEvictionMarkers() > 0);
                Assert.assertEquals(
                        "the anchor's own markers must be untouched",
                        3,
                        anchorWindow().getCheckpointEvictionMarkerCount()
                );
                commit("('2026-01-03T02:00:00.000000Z', 'acct-1', 3.0), "
                        + "('2026-01-03T03:00:00.000000Z', 'acct-1', 4.0)", job);
                Assert.assertTrue(
                        "a dirty partition key missing without its own marker must fail the seal",
                        viewInstance().getCheckpointSealFailures() > sealFailuresBefore
                );
            }
        });
    }

    private static void assertPartitionCount(
            String what,
            int expected,
            LiveViewCheckpointPartitionMapReader partitions,
            LiveViewCheckpointPageRef partitionMapRoot
    ) {
        final int[] count = {0};
        partitions.iterateAll(partitionMapRoot, partition -> count[0]++);
        Assert.assertEquals(what + " partition count", expected, count[0]);
    }

    private static Path checkpointsDir(LiveViewInstance instance) {
        return new Path().of(configuration.getDbRoot())
                .concat(instance.getLiveViewToken())
                .concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
    }

    /**
     * The live compiled window functions of the view, so a case can read the dirty
     * set the seal will consume rather than infer it from what the seal wrote.
     */
    private static ObjList<WindowFunction> windowFunctions(LiveViewInstance instance) {
        RecordCursorFactory factory = instance.getCompiledFactory();
        while (factory != null) {
            if (factory instanceof WindowRecordCursorFactory windowFactory) {
                return windowFactory.getWindowFunctions();
            }
            if (factory instanceof QueryProgress) {
                factory = factory.getBaseFactory();
                continue;
            }
            break;
        }
        throw new IllegalStateException("compiled factory does not contain a WindowRecordCursorFactory");
    }

    private LiveViewWindow anchorWindow() {
        final LiveViewWindow window = viewInstance().getAnchorWindow();
        Assert.assertNotNull("the view must carry an anchored window", window);
        return window;
    }

    /**
     * An anchor map whose heap already carries a full set of value bytes at the offset the
     * first entry lands on - the state a live map leaves behind once {@code clear()} has
     * rewound the append pointer back over the entries it wrote. The stamp is the cadence
     * the counter starts on, which is what a stamped entry leaves; the tombstone byte is a
     * 1 the region can carry from anything the allocator recycled into it, since the window
     * writes that slot only as 0. The anchor value is one no row here computes, so the
     * partition still crosses into its own bucket.
     */
    private Map poisonedAnchorMap(Record record) {
        final Map map = MapFactory.createUnorderedMap(
                configuration,
                LongKeyRecordStub.PAIR_KEY_TYPES,
                LiveViewWindow.anchorMapValueTypes()
        );
        try {
            final MapKey key = map.withKey();
            key.put(record, LongKeyRecordStub.PAIR_SINK);
            final MapValue value = key.createValue();
            // The anchor value layout in slot order: anchor LONG, initialized BYTE,
            // tombstone BYTE, dirty-cadence SHORT. See LiveViewWindow.anchorMapValueTypes().
            value.putLong(0, 42L);
            value.putByte(1, (byte) 1);
            value.putByte(2, (byte) 1);
            value.putShort(3, (short) 1);
            map.clear();
            return map;
        } catch (Throwable th) {
            map.close();
            throw th;
        }
    }

    /**
     * An anchored window over a single LONG partition key, with a constant anchor so every
     * row of one key stays in one bucket and the frontier sweep never fires. Enough to
     * drive {@link LiveViewWindow#processRow} without a compiled live view, which is what
     * a case needs to fail one function's mark and not another's.
     */
    private LiveViewWindow standaloneWindow(ObjList<WindowFunction> functions) {
        return standaloneWindow(functions, new SingleColumnType(ColumnType.LONG), LongKeyRecordStub.SINK);
    }

    /**
     * The same window over a caller-chosen partition-key shape, which is what a case that
     * turns on the anchor map's implementation needs: MapFactory picks that implementation
     * off the key shape and the value width, so the key is the only handle a test has on
     * it. See {@link LongKeyRecordStub#PAIR_KEY_TYPES}.
     */
    private LiveViewWindow standaloneWindow(
            ObjList<WindowFunction> functions,
            ColumnTypes keyTypes,
            RecordSink keySink
    ) {
        return standaloneWindow(
                functions,
                keyTypes,
                keySink,
                MapFactory.createUnorderedMap(configuration, keyTypes, LiveViewWindow.anchorMapValueTypes()),
                null
        );
    }

    /**
     * The same window over a caller-supplied anchor map and per-view tracker, which is what
     * a case that seeds the map's heap or fails one of the window's own allocations needs.
     * The window takes ownership of the map and frees it on close, as it does the one the
     * other overloads hand it.
     */
    private LiveViewWindow standaloneWindow(
            ObjList<WindowFunction> functions,
            ColumnTypes keyTypes,
            RecordSink keySink,
            Map anchorMap,
            MemoryTracker memoryTracker
    ) {
        return new LiveViewWindow(
                configuration,
                "w",
                new LongConstant(1_000L),
                ColumnType.LONG,
                keyTypes,
                anchorMap,
                keySink,
                // The anchor is not monotone, so no sweep runs and the anchor-key sink is
                // never reached.
                keySink,
                // No fused group: these functions keep the private maps the cases read.
                null,
                null,
                null,
                functions,
                false,
                null,
                memoryTracker
        );
    }

    private void assertDirtySetsClearedByPublish() {
        Assert.assertEquals(
                "a published seal must clear the anchor dirty set",
                0,
                anchorWindow().getCheckpointDirtyAnchorMapSize()
        );
        assertFunctionDirtySize(0);
        Assert.assertNotEquals(
                "a published seal must leave a baseline generation behind",
                Numbers.LONG_NULL,
                anchorWindow().getCheckpointBaselineGeneration()
        );
        final long generation = publishedGeneration();
        Assert.assertTrue(
                "the seal must be able to freeze the next boundary incrementally",
                anchorWindow().canFreezeCheckpointIncrementally(generation)
        );
        final ObjList<WindowFunction> functions = windowFunctions(viewInstance());
        for (int i = 0, n = functions.size(); i < n; i++) {
            final WindowFunction function = functions.getQuick(i);
            if (function.getCheckpointDirtyPartitionMap() == null) {
                continue;
            }
            Assert.assertFalse(
                    "function " + i + " must not be pinned to a full scan after a publish",
                    function.isCheckpointFullScanRequired()
            );
            Assert.assertEquals(
                    "function " + i + " must hold the published generation as its baseline",
                    generation,
                    function.getCheckpointBaselineGeneration()
            );
        }
    }

    /**
     * Asserts the anchor window and every dirty-tracking window function carry exactly
     * {@code expected} eviction markers - the record the sweep leaves behind and the seal
     * turns into removals. A sweep that recorded nothing still leaves correct results in
     * memory, so without this the omission would only surface on a restart.
     * <p>
     * A fused group records nothing of its own: the sweep drops one entry carrying the
     * anchor value and every component together, so the anchor's marker above is the
     * whole record and the tracked-count guard applies only to the residual functions.
     */
    private void assertEvictionMarkerCount(int expected) {
        Assert.assertEquals(
                "anchor eviction marker count",
                expected,
                anchorWindow().getCheckpointEvictionMarkerCount()
        );
        final ObjList<WindowFunction> functions = windowFunctions(viewInstance());
        int tracked = 0;
        boolean fused = false;
        for (int i = 0, n = functions.size(); i < n; i++) {
            final WindowFunction function = functions.getQuick(i);
            if (function.isWindowStateOwned()) {
                fused = true;
                continue;
            }
            final Map dirty = function.getCheckpointDirtyPartitionMap();
            final int tombstoneIndex = function.getTombstoneValueIndex();
            if (dirty == null || tombstoneIndex < 0) {
                continue;
            }
            tracked++;
            final MapRecordCursor cursor = dirty.getCursor();
            final MapRecord record = dirty.getRecord();
            int marked = 0;
            while (cursor.hasNext()) {
                if (record.getValue().getByte(tombstoneIndex) == 1) {
                    marked++;
                }
            }
            Assert.assertEquals("function " + i + " eviction marker count", expected, marked);
        }
        Assert.assertTrue("no window function tracks dirty partitions", fused || tracked > 0);
    }

    /**
     * Asserts every function that keeps a dirty set of its own holds exactly
     * {@code expected} keys.
     * <p>
     * A function the window has fused keeps none: the group's touched keys are the
     * anchor's, marked once for the whole entry. The helper then asserts that one set
     * instead of passing vacuously, which is what the tracked-count guard is for - a
     * view where nothing at all tracked would mean the seal had lost its incremental
     * path rather than moved it.
     */
    private void assertFunctionDirtySize(long expected) {
        final ObjList<WindowFunction> functions = windowFunctions(viewInstance());
        int tracked = 0;
        boolean fused = false;
        for (int i = 0, n = functions.size(); i < n; i++) {
            final WindowFunction function = functions.getQuick(i);
            if (function.isWindowStateOwned()) {
                fused = true;
                continue;
            }
            final Map dirty = function.getCheckpointDirtyPartitionMap();
            if (dirty == null) {
                continue;
            }
            tracked++;
            Assert.assertEquals("function " + i + " dirty key count", expected, dirty.size());
        }
        if (fused) {
            Assert.assertEquals(
                    "the fused group's dirty keys are the anchor's",
                    expected,
                    anchorWindow().getCheckpointDirtyAnchorMapSize()
            );
            return;
        }
        Assert.assertTrue("no window function tracks dirty partitions", tracked > 0);
    }

    private void assertFunctionStateSize(long expected) {
        final ObjList<WindowFunction> functions = windowFunctions(viewInstance());
        for (int i = 0, n = functions.size(); i < n; i++) {
            final WindowFunction function = functions.getQuick(i);
            if (function.isWindowStateOwned()) {
                // Its accumulator is a slice of the window's own entry, so the live key
                // count for it is the window's.
                Assert.assertEquals(
                        "function " + i + " live key count",
                        expected,
                        anchorWindow().getAnchorMapSize()
                );
                continue;
            }
            final Map state = function.getPartitionMap();
            if (state == null) {
                continue;
            }
            Assert.assertEquals("function " + i + " live key count", expected, state.size());
        }
    }

    /**
     * Whether the view's anchored window has adopted a fused plan, and so owns the state
     * the grouped functions would otherwise each keep. The per-function assertions below
     * have nothing to read for such a function; the window's own are what carry them.
     */
    private boolean isWindowStateFused() {
        return anchorWindow().getCheckpointWindowStatePlan() != null;
    }

    /**
     * Asserts every per-partition state root at the head boundary names exactly
     * {@code expected} partitions.
     * <p>
     * This view's two calls fuse, so the head's state root is one window root holding
     * both of them and the function directory is empty. The legacy arm is kept because
     * the shape is a property of the compiled plan rather than of the assertion: a view
     * the plan declines still seals one root per function, and the count means the same
     * thing either way.
     */
    private void assertHeadRootPartitionCount(int expected) {
        final LiveViewInstance instance = viewInstance();
        try (
                Path dir = checkpointsDir(instance);
                LiveViewCheckpointMetaStore metaStore = new LiveViewCheckpointMetaStore(configuration)
        ) {
            metaStore.of(dir);
            Assert.assertTrue(metaStore.isValid());
            try (
                    LiveViewCheckpointGenerationPin pin = metaStore.pin();
                    LiveViewCheckpointTimelineReader timeline = new LiveViewCheckpointTimelineReader(configuration);
                    LiveViewCheckpointRoot root = new LiveViewCheckpointRoot(configuration);
                    LiveViewCheckpointFunctionDirectory functions = new LiveViewCheckpointFunctionDirectory(configuration);
                    LiveViewCheckpointFunctionRoot functionRoot = new LiveViewCheckpointFunctionRoot(configuration);
                    LiveViewCheckpointWindowRoot windowRoot = new LiveViewCheckpointWindowRoot(configuration);
                    LiveViewCheckpointPartitionMapReader partitions = new LiveViewCheckpointPartitionMapReader(configuration)
            ) {
                timeline.of(dir);
                partitions.of(dir);
                final LiveViewCheckpointTimelineEntry head = new LiveViewCheckpointTimelineEntry();
                Assert.assertTrue("the view must have sealed a boundary", timeline.last(pin.getTimelineRootRef(), head));
                final LiveViewCheckpointPageRef functionDirectoryRef = new LiveViewCheckpointPageRef();
                final LiveViewCheckpointPageRef functionRootRef = new LiveViewCheckpointPageRef();
                final LiveViewCheckpointPageRef partitionMapRoot = new LiveViewCheckpointPageRef();
                final LiveViewCheckpointPageRef stateRootRef = new LiveViewCheckpointPageRef();
                root.of(dir, head.rootRef);
                root.getStateRootRef(stateRootRef);
                int stateRoots = 0;
                if (!stateRootRef.isNull() && windowRoot.ofIfWindowRoot(dir, stateRootRef)) {
                    windowRoot.getPartitionMapRootRef(partitionMapRoot);
                    assertPartitionCount("window state root", expected, partitions, partitionMapRoot);
                    stateRoots++;
                }
                root.getFunctionDirectoryRef(functionDirectoryRef);
                functions.of(dir, functionDirectoryRef);
                for (int i = 0, n = functions.size(); i < n; i++) {
                    functions.getRootRef(i, functionRootRef);
                    functionRoot.of(dir, functionRootRef);
                    functionRoot.getPartitionMapRootRef(partitionMapRoot);
                    assertPartitionCount("function " + i + " root", expected, partitions, partitionMapRoot);
                    stateRoots++;
                }
                Assert.assertTrue("the view declares per-partition checkpoint state", stateRoots > 0);
            }
        }
    }

    /**
     * Asserts the anchor window and every partition-mapped window function hold
     * {@code generation} as their incremental baseline, carry no dirty keys and are off
     * the full scan. Unlike {@link #assertDirtySetsClearedByPublish()} it tolerates a
     * function whose dirty map is still null, which is the state a restart leaves
     * behind: the first row the resumed view processes is what creates it.
     */
    private void assertIncrementalBaseline(long generation) {
        Assert.assertEquals(
                "the anchor window must hold the restored root's generation as its baseline",
                generation,
                anchorWindow().getCheckpointBaselineGeneration()
        );
        Assert.assertFalse(
                "the anchor window must not be pinned to a full scan after a head restore",
                anchorWindow().isCheckpointFullScanRequired()
        );
        Assert.assertEquals(
                "a freshly restored anchor map must carry no dirty keys",
                0,
                anchorWindow().getCheckpointDirtyAnchorMapSize()
        );
        final ObjList<WindowFunction> functions = windowFunctions(viewInstance());
        int checked = 0;
        for (int i = 0, n = functions.size(); i < n; i++) {
            final WindowFunction function = functions.getQuick(i);
            if (!function.supportsCheckpointState()
                    || function.isWindowStateOwned()
                    || function.getPartitionMap() == null
                    || function.supportsCheckpointRingState()) {
                continue;
            }
            checked++;
            Assert.assertFalse(
                    "function " + i + " must not be pinned to a full scan after a head restore",
                    function.isCheckpointFullScanRequired()
            );
            Assert.assertEquals(
                    "function " + i + " must hold the restored root's generation as its baseline",
                    generation,
                    function.getCheckpointBaselineGeneration()
            );
            final Map dirty = function.getCheckpointDirtyPartitionMap();
            Assert.assertTrue(
                    "function " + i + " must carry no dirty keys",
                    dirty == null || dirty.size() == 0
            );
        }
        Assert.assertTrue("no window function carries partition state", isWindowStateFused() || checked > 0);
    }

    /**
     * Asserts the anchor window and every partition-mapped window function may still
     * freeze the next boundary on top of {@code generation}. Says nothing about what
     * they have dirty, which is what makes it usable mid-cadence where
     * {@link #assertIncrementalBaseline(long)} is not.
     */
    private void assertIncrementalGateOpen(long generation) {
        Assert.assertFalse(
                "the anchor window must not be pinned to a full scan",
                anchorWindow().isCheckpointFullScanRequired()
        );
        Assert.assertTrue(
                "the anchor window must be able to freeze the next boundary incrementally",
                anchorWindow().canFreezeCheckpointIncrementally(generation)
        );
        final ObjList<WindowFunction> functions = windowFunctions(viewInstance());
        int checked = 0;
        for (int i = 0, n = functions.size(); i < n; i++) {
            final WindowFunction function = functions.getQuick(i);
            if (function.isWindowStateOwned()
                    || function.getCheckpointDirtyPartitionMap() == null
                    || function.supportsCheckpointRingState()) {
                continue;
            }
            checked++;
            Assert.assertFalse(
                    "function " + i + " must not be pinned to a full scan",
                    function.isCheckpointFullScanRequired()
            );
            Assert.assertEquals(
                    "function " + i + " must still hold the published generation as its baseline",
                    generation,
                    function.getCheckpointBaselineGeneration()
            );
        }
        Assert.assertTrue("no window function tracks dirty partitions", isWindowStateFused() || checked > 0);
    }

    /**
     * Asserts the anchor window and every partition-mapped function charge what
     * {@code expected} recorded. Read after a restart it is the accounting oracle: the
     * restore recomputes the figure by walking the root it read, so a seal that
     * subtracted an evicted key twice - or never - disagrees here even though the root's
     * contents are right.
     */
    private void assertLogicalStateBytesEqual(LongList expected) {
        Assert.assertEquals(
                "logical state bytes must survive a restart",
                expected.toString(),
                readLogicalStateBytes().toString()
        );
    }

    /**
     * Asserts the anchor window and every partition-mapped window function still demand
     * a complete freeze, which is where a restore that cannot vouch for its root has to
     * leave them.
     */
    private void assertPinnedToFullScan() {
        Assert.assertTrue(
                "the anchor window must still demand a complete freeze",
                anchorWindow().isCheckpointFullScanRequired()
        );
        Assert.assertEquals(
                "the anchor window must hold no baseline generation",
                Numbers.LONG_NULL,
                anchorWindow().getCheckpointBaselineGeneration()
        );
        final ObjList<WindowFunction> functions = windowFunctions(viewInstance());
        int checked = 0;
        for (int i = 0, n = functions.size(); i < n; i++) {
            final WindowFunction function = functions.getQuick(i);
            if (!function.supportsCheckpointState()
                    || function.isWindowStateOwned()
                    || function.getPartitionMap() == null) {
                continue;
            }
            checked++;
            Assert.assertTrue(
                    "function " + i + " must still demand a complete freeze",
                    function.isCheckpointFullScanRequired()
            );
            Assert.assertEquals(
                    "function " + i + " must hold no baseline generation",
                    Numbers.LONG_NULL,
                    function.getCheckpointBaselineGeneration()
            );
        }
        Assert.assertTrue("no window function carries partition state", isWindowStateFused() || checked > 0);
    }

    /**
     * The {@link #createUnfusedAvgView} shape's oracle, built the same way
     * {@link #recompute(String)} builds the fused view's.
     */
    private void assertUnfusedAvgViewMatchesRecompute(String anchorTime) throws Exception {
        final String bucket = "timestamp_floor('1d', created_at, '1970-01-01T"
                + anchorTime + ":00.000000Z'::timestamp)";
        final String recompute = "select created_at, cod_acct_no, "
                + "avg(amt_txn + 0.0) over (partition by cod_acct_no, bucket order by created_at "
                + "rows between unbounded preceding and current row) as cumulative_avg, "
                + "ksum(amt_txn + 0.0) over (partition by cod_acct_no, bucket order by created_at "
                + "rows between unbounded preceding and current row) as cumulative_ksum "
                + "from (select created_at, cod_acct_no, amt_txn, " + bucket + " as bucket from tx)";
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(" + recompute + ") order by 2, 1",
                "(lv) order by 2, 1",
                LOG,
                true
        );
        assertNoRefreshFaults("lv");
    }

    /**
     * The {@link #createUnfusedView} shape's oracle, built the same way
     * {@link #recompute(String)} builds the fused view's.
     */
    private void assertUnfusedViewMatchesRecompute(String anchorTime) throws Exception {
        final String bucket = "timestamp_floor('1d', created_at, '1970-01-01T"
                + anchorTime + ":00.000000Z'::timestamp)";
        final String recompute = "select created_at, cod_acct_no, "
                + "sum(amt_txn + 0.0) over (partition by cod_acct_no, bucket order by created_at "
                + "rows between unbounded preceding and current row) as cumulative_sum "
                + "from (select created_at, cod_acct_no, amt_txn, " + bucket + " as bucket from tx)";
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(" + recompute + ") order by 2, 1",
                "(lv) order by 2, 1",
                LOG,
                true
        );
        assertNoRefreshFaults("lv");
    }

    private void assertViewMatchesRecompute(String anchorTime) throws Exception {
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(" + recompute(anchorTime) + ") order by 2, 1",
                "(lv) order by 2, 1",
                LOG,
                true
        );
        assertNoRefreshFaults("lv");
    }

    /**
     * Clears one eviction marker in each dirty-tracking function's dirty set, and returns
     * how many it cleared. The key stays absent from the function's live state and stays
     * in the dirty set, so what the seal sees is a dirty key with no provenance.
     */
    private int clearFunctionEvictionMarkers() {
        final ObjList<WindowFunction> functions = windowFunctions(viewInstance());
        int cleared = 0;
        for (int i = 0, n = functions.size(); i < n; i++) {
            final WindowFunction function = functions.getQuick(i);
            final Map dirty = function.getCheckpointDirtyPartitionMap();
            final int tombstoneIndex = function.getTombstoneValueIndex();
            if (dirty == null || tombstoneIndex < 0) {
                continue;
            }
            final MapRecordCursor cursor = dirty.getCursor();
            final MapRecord record = dirty.getRecord();
            while (cursor.hasNext()) {
                final MapValue value = record.getValue();
                if (value.getByte(tombstoneIndex) == 1) {
                    value.putByte(tombstoneIndex, (byte) 0);
                    cleared++;
                    break;
                }
            }
        }
        return cleared;
    }

    /**
     * Empties every window function's live partition map, leaving the dirty set naming
     * keys whose state is gone with no sweep anywhere in the picture. No production path
     * does this: the map is emptied only by paths that force a complete freeze first.
     */
    private void clearFunctionStateMaps() {
        final ObjList<WindowFunction> functions = windowFunctions(viewInstance());
        int cleared = 0;
        for (int i = 0, n = functions.size(); i < n; i++) {
            final Map state = functions.getQuick(i).getPartitionMap();
            if (state == null) {
                continue;
            }
            state.clear();
            cleared++;
        }
        Assert.assertTrue("no window function carries partition state", cleared > 0);
    }

    private void commit(String values, LiveViewRefreshJob job) throws Exception {
        execute("insert into tx values " + values);
        drainWalQueue();
        driveRefreshToQuiescence(job);
    }

    /**
     * The unfused view again, over the two accumulators whose own markPartitionAlive
     * used to skip the dirty marking. Both arguments are expressions, so the fused
     * window-state plan declines them and each function keeps the private map, dirty set
     * and root the case reads - see {@link #createUnfusedView}.
     */
    private void createUnfusedAvgView(String anchorTime, String seedRows) throws Exception {
        execute("create table tx (created_at timestamp, cod_acct_no symbol nocache index capacity 4, "
                + "amt_txn double) timestamp(created_at) partition by hour wal");
        execute("insert into tx values " + seedRows);
        drainWalQueue();
        execute("create live view lv flush every 100ms start from beginning as "
                + "select created_at, cod_acct_no, "
                + "avg(amt_txn + 0.0) over w as cumulative_avg, "
                + "ksum(amt_txn + 0.0) over w as cumulative_ksum "
                + "from tx window w as (partition by cod_acct_no order by created_at anchor daily '"
                + anchorTime + "')");
    }

    /**
     * The same view, over an argument the fused window-state plan declines: an
     * expression is not a direct column reference, and SQL text equality is not a proof
     * that two expressions are one accumulator.
     * <p>
     * A case uses this when what it holds to its contract is the <b>per-function</b>
     * removal or dirty-set branch. Those still run for every residual function - a ring
     * window, {@code count(*)}, an expression argument - but a fused group takes its key
     * domain and its removals from the anchor instead, so poking a function's own
     * tombstone or eviction bit there describes a state nothing in that path reads.
     */
    private void createUnfusedView(String anchorTime, String seedRows) throws Exception {
        execute("create table tx (created_at timestamp, cod_acct_no symbol nocache index capacity 4, "
                + "amt_txn double) timestamp(created_at) partition by hour wal");
        execute("insert into tx values " + seedRows);
        drainWalQueue();
        execute("create live view lv flush every 100ms start from beginning as "
                + "select created_at, cod_acct_no, "
                + "sum(amt_txn + 0.0) over w as cumulative_sum "
                + "from tx window w as (partition by cod_acct_no order by created_at anchor daily '"
                + anchorTime + "')");
    }

    private void createView(String anchorTime, String seedRows) throws Exception {
        execute("create table tx (created_at timestamp, cod_acct_no symbol nocache index capacity 4, "
                + "amt_txn double) timestamp(created_at) partition by hour wal");
        execute("insert into tx values " + seedRows);
        drainWalQueue();
        execute("create live view lv flush every 100ms start from beginning as "
                + "select created_at, cod_acct_no, "
                + "sum(amt_txn) over w as cumulative_sum, "
                + "count(cod_acct_no) over w as cumulative_count "
                + "from tx window w as (partition by cod_acct_no order by created_at anchor daily '"
                + anchorTime + "')");
    }

    /**
     * Seeds {@code accounts} distinct accounts into one anchor bucket through a generated
     * insert, rather than the literal row list {@link #createView} takes. The rows sit a
     * millisecond apart so the whole seed stays inside the 2026-01-01 bucket however many
     * accounts a case asks for.
     */
    private void createViewWithGeneratedSeed(String anchorTime, int accounts) throws Exception {
        execute("create table tx (created_at timestamp, cod_acct_no symbol nocache index capacity 4, "
                + "amt_txn double) timestamp(created_at) partition by hour wal");
        execute("INSERT INTO tx SELECT ('2026-01-01T11:00:00.000000Z'::timestamp + x * 1_000)::timestamp, "
                + "('acct-' || x)::symbol, x * 1.0 FROM long_sequence(" + accounts + ")");
        drainWalQueue();
        execute("create live view lv flush every 100ms start from beginning as "
                + "select created_at, cod_acct_no, "
                + "sum(amt_txn) over w as cumulative_sum, "
                + "count(cod_acct_no) over w as cumulative_count "
                + "from tx window w as (partition by cod_acct_no order by created_at anchor daily '"
                + anchorTime + "')");
    }

    /**
     * One row of {@code account} at {@code second} past 11:00, as an INSERT tuple. The
     * cases that drive a run of corrections read as a sequence of timestamps rather than
     * as string concatenation this way.
     */
    private String row(String account, int second) {
        return "('2026-01-01T11:00:" + String.format("%02d", second) + ".000000Z', '" + account + "', 1.0)";
    }

    /**
     * The checkpoint ids the published timeline holds, oldest first. What it reports on
     * is which publication a repair took: a splice re-versions a boundary's payload and
     * keeps its logical coordinate, so the list comes back unchanged, while a truncate
     * drops every entry above the repair floor.
     */
    private LongList logicalCheckpointIds() {
        final LongList ids = new LongList();
        final LiveViewInstance instance = viewInstance();
        try (
                Path dir = checkpointsDir(instance);
                LiveViewCheckpointMetaStore metaStore = new LiveViewCheckpointMetaStore(configuration)
        ) {
            metaStore.of(dir);
            Assert.assertTrue("the view must have published a generation", metaStore.isValid());
            try (
                    LiveViewCheckpointGenerationPin pin = metaStore.pin();
                    LiveViewCheckpointTimelineReader timeline = new LiveViewCheckpointTimelineReader(configuration)
            ) {
                timeline.of(dir);
                timeline.iterateAll(pin.getTimelineRootRef(), entry -> ids.add(entry.checkpointId));
            }
        }
        return ids;
    }

    private long publishedGeneration() {
        final LiveViewInstance instance = viewInstance();
        try (
                Path dir = checkpointsDir(instance);
                LiveViewCheckpointMetaStore metaStore = new LiveViewCheckpointMetaStore(configuration)
        ) {
            metaStore.of(dir);
            Assert.assertTrue("the view must have published a generation", metaStore.isValid());
            return metaStore.getSuperblock().generation;
        }
    }

    /**
     * The key capacity the anchor's dirty set and every dirty-tracking function's
     * currently retain, in a fixed order so two readings compare directly. Capacity, not
     * size: a publication empties these maps either way, and what the sweep leaves behind
     * is the backing they hold on to.
     */
    private LongList readDirtySetKeyCapacities() {
        final LongList out = new LongList();
        out.add(anchorWindow().getCheckpointDirtyAnchorMapKeyCapacity());
        final ObjList<WindowFunction> functions = windowFunctions(viewInstance());
        for (int i = 0, n = functions.size(); i < n; i++) {
            final WindowFunction function = functions.getQuick(i);
            if (function.isWindowStateOwned()) {
                continue;
            }
            final Map dirty = function.getCheckpointDirtyPartitionMap();
            if (dirty == null) {
                continue;
            }
            out.add(dirty.getKeyCapacity());
        }
        Assert.assertTrue("no window function tracks dirty partitions", isWindowStateFused() || out.size() > 1);
        return out;
    }

    /**
     * Copies the two newest logical boundaries of the published generation into
     * {@code headOut} and {@code predecessorOut}, so a case can name a root the
     * refresh job would never select on its own.
     */
    private void readHeadBoundaries(
            LiveViewCheckpointTimelineEntry headOut,
            LiveViewCheckpointTimelineEntry predecessorOut
    ) {
        final LiveViewInstance instance = viewInstance();
        try (
                Path dir = checkpointsDir(instance);
                LiveViewCheckpointMetaStore metaStore = new LiveViewCheckpointMetaStore(configuration);
                LiveViewCheckpointTimelineReader timeline = new LiveViewCheckpointTimelineReader(configuration)
        ) {
            metaStore.of(dir);
            Assert.assertTrue("the view must have published a generation", metaStore.isValid());
            timeline.of(dir);
            try (LiveViewCheckpointGenerationPin pin = metaStore.pin()) {
                Assert.assertTrue(
                        "the view must have sealed a boundary",
                        timeline.last(pin.getTimelineRootRef(), headOut)
                );
                Assert.assertTrue(
                        "the view must have sealed at least two boundaries",
                        timeline.predecessor(pin.getTimelineRootRef(), headOut.maxTimestamp, predecessorOut)
                );
            }
        }
    }

    /**
     * The logical byte counts the anchor window and every partition-mapped function
     * currently charge, in a fixed order so two readings compare directly.
     */
    private LongList readLogicalStateBytes() {
        final LongList out = new LongList();
        out.add(anchorWindow().getCheckpointLogicalStateBytes());
        final ObjList<WindowFunction> functions = windowFunctions(viewInstance());
        for (int i = 0, n = functions.size(); i < n; i++) {
            final WindowFunction function = functions.getQuick(i);
            if (function.isWindowStateOwned()
                    || function.getPartitionMap() == null
                    || function.supportsCheckpointRingState()) {
                continue;
            }
            out.add(function.getCheckpointLogicalStateBytes());
        }
        Assert.assertTrue("no window function carries partition state", isWindowStateFused() || out.size() > 1);
        return out;
    }

    /**
     * The oracle: the anchored view's semantics restated for the plain window engine.
     * {@code ANCHOR DAILY 'HH:MM'} desugars (SqlParser.desugarDailyAnchor) into
     * exactly this {@code timestamp_floor}, so folding that bucket into the PARTITION
     * BY and running an unbounded frame computes what the anchor computes. Unlike a
     * bare unbounded frame it stays correct across a bucket crossing, which is what
     * these cases are about.
     */
    private String recompute(String anchorTime) {
        final String bucket = "timestamp_floor('1d', created_at, '1970-01-01T"
                + anchorTime + ":00.000000Z'::timestamp)";
        return "select created_at, cod_acct_no, "
                + "sum(amt_txn) over (partition by cod_acct_no, bucket order by created_at "
                + "rows between unbounded preceding and current row) as cumulative_sum, "
                + "count(cod_acct_no) over (partition by cod_acct_no, bucket order by created_at "
                + "rows between unbounded preceding and current row) as cumulative_count "
                + "from (select created_at, cod_acct_no, amt_txn, " + bucket + " as bucket from tx)";
    }

    private void restartCycle() {
        engine.getLiveViewRegistry().clear();
        engine.buildViewGraphs();
    }

    /**
     * Rehydrates the live runtime from one exact checkpoint root, the way the restart
     * path does, and returns the generation the restore ran under.
     */
    private long restoreRoot(long maxTimestamp, long checkpointId) {
        final LiveViewInstance instance = viewInstance();
        try (
                Path dir = checkpointsDir(instance);
                LiveViewCheckpointTimelineStoreReader reader =
                        new LiveViewCheckpointTimelineStoreReader(configuration)
        ) {
            reader.of(dir);
            try {
                return reader.restore(
                        maxTimestamp,
                        checkpointId,
                        instance.getLiveViewToken().getTableId(),
                        windowFunctions(instance),
                        instance.getAnchorWindow()
                ).generation;
            } finally {
                reader.detach();
            }
        }
    }

    /**
     * Sets the tombstone bit on every live partition of every window function that
     * carries one. The counter stays where it is on purpose: markPartitionAlive
     * early-exits on a zero count, so the bits survive the rows that follow and reach
     * the seal, which is the state the runtime never produces on its own.
     */
    private void tombstoneEveryPartition() {
        final ObjList<WindowFunction> functions = windowFunctions(viewInstance());
        int tombstoned = 0;
        for (int i = 0, n = functions.size(); i < n; i++) {
            final WindowFunction function = functions.getQuick(i);
            final Map map = function.getPartitionMap();
            final int tombstoneIndex = function.getTombstoneValueIndex();
            if (map == null || tombstoneIndex < 0) {
                continue;
            }
            final MapRecordCursor cursor = map.getCursor();
            final MapRecord record = map.getRecord();
            while (cursor.hasNext()) {
                record.getValue().putByte(tombstoneIndex, (byte) 1);
                tombstoned++;
            }
        }
        Assert.assertTrue("no window function carries a tombstone slot", tombstoned > 0);
    }

    private LiveViewInstance viewInstance() {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull("live view 'lv' must be registered", instance);
        return instance;
    }

    /**
     * One LONG column, so the stub function's key sink has something to serialise and a
     * case can move the key between calls.
     */
    private static final class LongKeyRecordStub implements Record {
        /**
         * A two-column partition key, which is the shape that puts the anchor map on
         * OrderedMap: MapFactory falls back to it for every multi-column key whatever the
         * value width, and OrderedMap is the one implementation whose clear() leaves the
         * value bytes it wrote standing rather than memsetting the region. Both columns
         * read the stub's one field, so a key is still one long.
         */
        private static final ArrayColumnTypes PAIR_KEY_TYPES = new ArrayColumnTypes()
                .add(ColumnType.LONG)
                .add(ColumnType.LONG);
        private static final RecordSink PAIR_SINK = new RecordSink() {
            @Override
            public void copy(Record r, RecordSinkSPI w) {
                w.putLong(r.getLong(0));
                w.putLong(r.getLong(1));
            }

            @Override
            public void setFunctions(ObjList<Function> keyFunctions) {
            }
        };
        private static final RecordSink SINK = new RecordSink() {
            @Override
            public void copy(Record r, RecordSinkSPI w) {
                w.putLong(r.getLong(0));
            }

            @Override
            public void setFunctions(ObjList<Function> keyFunctions) {
            }
        };
        private long value;

        @Override
        public long getLong(int col) {
            return value;
        }
    }

    /**
     * A partitioned window function that marks exactly as the base class does and counts
     * the marks, with a switch that makes one refuse. The refusal stands in for the
     * per-view memory tracker tripping inside the dirty set's first allocation, which is
     * the realistic way a mark throws.
     */
    private static final class MarkCountingFunctionStub extends BasePartitionedWindowFunction {
        private static final ArrayColumnTypes KEY_TYPES = new ArrayColumnTypes().add(ColumnType.LONG);
        private static final ArrayColumnTypes VALUE_TYPES = new ArrayColumnTypes().add(ColumnType.BYTE);
        private boolean isFailing;
        private int markedCount;

        private MarkCountingFunctionStub() {
            super(null, new VirtualRecord(keyFunctions()), LongKeyRecordStub.SINK, null);
            this.tombstoneValueIndex = 0;
        }

        @Override
        public String getName() {
            return "mark_counting";
        }

        @Override
        public int getType() {
            return ColumnType.DOUBLE;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            throw new UnsupportedOperationException();
        }

        /**
         * Counts what the base class's gate lets through, and refuses when the switch is on.
         * The stub inherits {@link BasePartitionedWindowFunction#markPartitionAlive(Record, boolean)}
         * rather than reimplementing its {@code isFirstCadenceTouch} test, so every count this
         * case asserts on observes the production gate itself. The refusal sits at the entry to
         * {@code markCheckpointPartitionDirty}, the method that lazily allocates the dirty set
         * under the per-view tracker, so it throws out of the same call and leaves that set
         * untouched - what a tracker breach on the allocation leaves behind.
         */
        @Override
        protected void markCheckpointPartitionDirty(Record record) {
            if (isFailing) {
                throw CairoException.critical(0).put("mark refused");
            }
            super.markCheckpointPartitionDirty(record);
            markedCount++;
        }

        @Override
        protected Map newCompactionScratch() {
            return MapFactory.createUnorderedMap(configuration, KEY_TYPES, VALUE_TYPES);
        }

        private static ObjList<Function> keyFunctions() {
            final ObjList<Function> functions = new ObjList<>();
            functions.add(LongColumn.newInstance(0));
            return functions;
        }
    }

    /**
     * A partitioned window function carrying everything the frontier sweep needs except
     * the marking itself - a tombstone slot and a scratch-map factory, with a
     * markPartitionAlive that names no key. The shape two unbounded-rows accumulators
     * carried before the mark moved back to the base class.
     */
    private static final class NonMarkingFunctionStub extends BasePartitionedWindowFunction {
        private static final ArrayColumnTypes KEY_TYPES = new ArrayColumnTypes().add(ColumnType.LONG);
        private static final ArrayColumnTypes VALUE_TYPES = new ArrayColumnTypes().add(ColumnType.BYTE);

        private NonMarkingFunctionStub() {
            super(null, new VirtualRecord(keyFunctions()), LongKeyRecordStub.SINK, null);
            this.tombstoneValueIndex = 0;
        }

        @Override
        public String getName() {
            return "non_marking";
        }

        @Override
        public int getType() {
            return ColumnType.DOUBLE;
        }

        @Override
        public void markPartitionAlive(Record record, boolean isFirstCadenceTouch) {
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            throw new UnsupportedOperationException();
        }

        void markDirty(Record record) {
            markCheckpointPartitionDirty(record);
        }

        @Override
        protected Map newCompactionScratch() {
            return MapFactory.createUnorderedMap(configuration, KEY_TYPES, VALUE_TYPES);
        }

        private static ObjList<Function> keyFunctions() {
            final ObjList<Function> functions = new ObjList<>();
            functions.add(LongColumn.newInstance(0));
            return functions;
        }
    }

    /**
     * A window function that implements retention and nothing else of the sweep's
     * contract - no recording hook, no three-argument override. The shape a future
     * implementer produces by migrating half the contract.
     */
    private static final class RetainingFunctionStub implements WindowFunction {
        private final boolean isFullScanRequired;
        private boolean isRetained;

        private RetainingFunctionStub(boolean isFullScanRequired) {
            this.isFullScanRequired = isFullScanRequired;
        }

        @Override
        public int getType() {
            return ColumnType.DOUBLE;
        }

        @Override
        public boolean isCheckpointFullScanRequired() {
            return isFullScanRequired;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void reset() {
        }

        @Override
        public void retainPartitions(Map survivingKeys, RecordSink survivingKeySink) {
            isRetained = true;
        }

        @Override
        public void setColumnIndex(int columnIndex) {
        }

        @Override
        public void toPlan(PlanSink sink) {
        }
    }
}
