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

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointOutputUniqueness;
import io.questdb.cairo.lv.LiveViewCheckpointOutputUniqueness.GroupKeySet;
import io.questdb.cairo.lv.LiveViewCheckpointRepairSession;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineStoreWriter;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.window.WindowRecordCursorFactory;
import io.questdb.std.Chars;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Coverage for the segment yield: one anchor segment's repair may stop on the refresh
 * turn's budget and continue on a later turn, and the loop it belongs to continues with it.
 * <p>
 * A per-segment repair bounds what a deep correction reads and rewrites, but the bound is
 * the anchor period's own base rows - a whole day of them under {@code ANCHOR DAILY},
 * however few rows the correction carried. The loop that drives one owns a single pinned
 * base snapshot across every segment it takes, which is why it could not let a replay park:
 * a parked repair takes the snapshot with it and the rest of the loop would have nothing to
 * run against. So the loop position parks too.
 * <p>
 * Every case here holds two things at once. The from-base recompute oracle says the output
 * converged, and the counters say <em>how</em>: the replay parked at least once, and the
 * loop repaired each of its segments exactly once. The second half is what an end-state
 * comparison cannot see - a loop that dropped its position would leave the change
 * unconsumed with its segments already repaired, and the next drain would re-classify the
 * range and repair every one of them again, converging on the same rows for twice the work.
 * <p>
 * One case runs two refresh workers rather than one, because a production pool does: the
 * notification queue is not sharded, so a worker that did not park a loop can still be handed
 * a task for the view it is parked on. It must back off and leave the loop to its owner, and
 * the owner must still finish it afterwards.
 * <p>
 * The view is the reported customer shape the per-segment cases use: an
 * anchored WINDOW carrying an unbounded cumulative sum and count per account, over a base
 * whose timestamps span several anchor days so closed segments exist at all. The days are
 * seeded several rows deep on purpose - a one-row replay budget only spreads a repair
 * across turns if the segment holds more than one row to replay.
 */
public class LiveViewCheckpointSegmentYieldTest extends AbstractLiveViewTest {

    // The route a repair took beside the rows it replayed taking it, which is the pair a
    // park holds apart for as many turns as it lasts.
    private static final String REPAIR_READING =
            "SELECT checkpoint_repair_in_progress, checkpoint_repair_last_disposition,"
                    + " o3_resume_replay_rows, o3_boundary_replay_rows, o3_replay_scan_rows"
                    + " FROM live_views()";
    // A two-worker pool, which is what a default shared.worker.count gives a live view in
    // production. Two is enough: the guard under test compares the parked session's owner
    // against the worker running the turn, so one owner and one stranger cover it.
    private static final int REFRESH_WORKER_COUNT = 2;
    private static final String REPAIR_READING_HEADER =
            "checkpoint_repair_in_progress\tcheckpoint_repair_last_disposition\t"
                    + "o3_resume_replay_rows\to3_boundary_replay_rows\to3_replay_scan_rows\n";
    // The one key the injected set carries into the resumed turn, so the park that follows
    // it has a key to copy back and therefore an add() to fail on.
    private static final int SEEDED_GROUP_KEY = 7;

    @Test
    public void testACloseWhoseWriterFreeThrowsStillReleasesEverythingBelowIt() throws Exception {
        // close() is the last chance every resource a parked repair holds ever gets. Each of its
        // callers has already detached the session by the time the free runs - endRepairSession
        // clears instance.suspendedRepair first, and discardSuspendedRepair nulls the field before
        // it frees - so a handle the chain skips is held by nothing in the process and comes back
        // only on a restart.
        //
        // The chain's throw-prone statements sit at its head and everything that releases memory
        // sits below them: the pinned base snapshot, the descriptor's three Paths and its mapping,
        // the overlay's window-state copy and the carryover's buffers. Closing the live-view
        // WalWriter is the head statement, and it is not a no-op on the paths that reach here with
        // a parked session - handleRefreshFailure, a dropped or invalidated view, a base-schema
        // recompile, worker shutdown - because a parked repair's writer carries the uncommitted
        // replacement. WalWriter.close() rolls that back through real file IO and
        // WalWriterPool.WalWriterTenant.close() deliberately rethrows whatever the rollback raises.
        //
        // A disk that fails that rollback is the natural producer and is not reproducible here, so
        // the case drives the other throw the same pool really raises: AbstractMultiTenantPool
        // answers a second return of the same writer with "double close". Both leave close() part
        // way through the same chain, which is what the case pins - the reader, the descriptor and
        // the rest are released anyway.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE close_chain (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final TableToken token = engine.verifyTableName("close_chain");
            try (
                    LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1);
                    RecordCursorFactory baseFactory = select("SELECT ts, x FROM close_chain");
                    WindowRecordCursorFactory windowFactory = new WindowRecordCursorFactory(
                            baseFactory,
                            GenericRecordMetadata.copyOf(baseFactory.getMetadata()),
                            new ObjList<>()
                    )
            ) {
                // Deliberately not in the try-with-resources above: the park below hands both to
                // the session, which owns them from that point on, and closing either here as well
                // would be a second return of it - the very thing the case makes the writer take.
                final TableReader baseReader = engine.getReader(token);
                final WalWriter walWriter = engine.getWalWriter(token);
                final LiveViewCheckpointRepairSession session =
                        new LiveViewCheckpointRepairSession(engine.getConfiguration(), job, windowFactory);
                session.suspend(
                        baseReader,
                        walWriter,
                        null,
                        1_000,
                        0,
                        0,
                        0,
                        0,
                        Numbers.LONG_NULL,
                        Numbers.LONG_NULL,
                        new LiveViewCheckpointOutputUniqueness()
                );
                Assert.assertTrue("the fixture must have parked the session", session.isSuspended());

                // Returns the writer to the pool behind the session's back, which is what makes the
                // session's own return of it the second one.
                walWriter.close();

                boolean hasFired = false;
                try {
                    session.close();
                } catch (CairoException e) {
                    hasFired = Chars.contains(e.getFlyweightMessage(), "double close");
                    if (!hasFired) {
                        throw e;
                    }
                }
                // Without this the case is vacuous. A fixture that stopped producing the throw -
                // the pool learning to tolerate the second return, say - would leave close() with
                // nothing to survive and pass on a chain that was never interrupted.
                Assert.assertTrue(
                        "the writer free never threw, so the case pinned nothing",
                        hasFired
                );

                // The statement immediately below the thrower. A stranded reader pins the base
                // snapshot, and its pool slot, for the life of the process.
                Assert.assertNull(
                        "the pinned base snapshot must go back even though the writer free threw",
                        session.takeBaseReader()
                );
                // And the tail of the chain, past the descriptor discard and the three frees the
                // leak check measures - the descriptor's Paths and mapping, the overlay's buffer
                // and the carryover's states.
                Assert.assertFalse(
                        "a closed session must not still read as parked",
                        session.isSuspended()
                );
            }
        });
    }

    @Test
    public void testAClosingWorkerFreesItsWriterOnlyAfterAnInvalidationReleasesItsParkedCapture() throws Exception {
        // A capture the worker parked holds a scratch its timeline writer leased, and the writer's
        // close frees every scratch it owns, leased or not. Invalidation discards the parked capture
        // on the invalidating thread, under the view's refresh latch, and hands the scratch back on
        // its way out. The two meet at shutdown: a pool halt closes the live view workers while the
        // WAL apply workers, which invalidate a view whose base table changed under it, still run.
        // A worker close that freed its writer while that release is still running could free the
        // scratch's native memory at the same time as the release, and both would free the same
        // pointer. The close waits on each parked view's latch before it frees the writer, and that
        // wait is what keeps the two apart.
        //
        // The interleaving is pinned rather than hoped for. The invalidation stops inside the
        // release of the parked capture's scratch, still holding the view's latch. The worker close
        // starts only then, and the case waits until the close is spinning on that latch, which it
        // cannot leave while the release is held. A close that freed its writer before it came to
        // the latch has done so by that point, and the writer must still be open. The release
        // stops only once the scratch has let go of its memory, so a close in the wrong order
        // fails the assertion instead of freeing anything twice.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_REPLAY_MAX_ROWS, 1);
        assertMemoryLeak(() -> {
            final LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1);
            final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
            final Thread invalidator = new Thread(() -> {
                try {
                    engine.invalidateLiveViewsForBaseTable(engine.verifyTableName("tx"), "base table changed while its worker closes");
                } catch (Throwable th) {
                    errors.add(th);
                } finally {
                    Path.clearThreadLocals();
                }
            }, "lv-invalidator");
            final Thread closer = new Thread(() -> {
                try {
                    job.close();
                } catch (Throwable th) {
                    errors.add(th);
                } finally {
                    Path.clearThreadLocals();
                }
            }, "lv-worker-close");
            try {
                parkAStagedCapture(job);
                final LiveViewInstance instance = viewInstance();
                Assert.assertNotNull(instance.getSuspendedRepair());
                final LiveViewCheckpointTimelineStoreWriter writer = checkpointTimelineStoreWriter(job);
                Assert.assertEquals("the parked capture must hold its scratch lease", 1, writer.getLeasedRepairScratchCountForTest());
                final CountDownLatch invalidatorInRelease = new CountDownLatch(1);
                final CountDownLatch resumeInvalidator = new CountDownLatch(1);
                writer.setRepairScratchReleaseHookForTest(() -> {
                    if (Thread.currentThread() == invalidator) {
                        invalidatorInRelease.countDown();
                        try {
                            if (!resumeInvalidator.await(60, TimeUnit.SECONDS)) {
                                errors.add(new AssertionError("the invalidation was never resumed"));
                            }
                        } catch (InterruptedException e) {
                            errors.add(e);
                        }
                    }
                });
                invalidator.start();
                try {
                    Assert.assertTrue(
                            "the invalidation never reached the release of the parked capture's scratch",
                            invalidatorInRelease.await(60, TimeUnit.SECONDS)
                    );
                    final boolean isLatchFree = instance.tryLockForRefresh();
                    if (isLatchFree) {
                        instance.unlockAfterRefresh();
                    }
                    Assert.assertFalse("the invalidation must hold the view's refresh latch while it releases", isLatchFree);
                    Assert.assertFalse(writer.isClosedForTest());
                    closer.start();
                    // Nothing in the spin signals, so the case reads the close's stack until it
                    // shows the spin. The close cannot leave the spin while the release is held, so
                    // the wait ends on a state the close stays in, not on a guess at timing.
                    TestUtils.assertEventually(() -> Assert.assertTrue(
                            "the worker close never came to the latch the invalidation holds",
                            isSpinningOnARefreshLatch(closer.getStackTrace())
                    ));
                    Assert.assertFalse(
                            "the worker close must not free its writer while another thread still releases a scratch of it",
                            writer.isClosedForTest()
                    );
                    // The lease clears only once the release is over, so the assertion above ran
                    // while the release was still in flight.
                    Assert.assertEquals(1, writer.getLeasedRepairScratchCountForTest());
                } finally {
                    resumeInvalidator.countDown();
                    invalidator.join(60_000);
                    if (closer.getState() != Thread.State.NEW) {
                        closer.join(60_000);
                    }
                }
                Assert.assertFalse("the invalidation did not finish", invalidator.isAlive());
                Assert.assertFalse("the worker close did not finish", closer.isAlive());
                if (!errors.isEmpty()) {
                    throw new RuntimeException("a thread failed", errors.peek());
                }
                Assert.assertTrue(instance.isInvalid());
                Assert.assertNull("the invalidation must let go of the parked repair", instance.getSuspendedRepair());
                Assert.assertTrue("the worker close must free its writer once the release is over", writer.isClosedForTest());
                execute("DROP LIVE VIEW lv");
            } finally {
                if (closer.getState() == Thread.State.NEW) {
                    job.close();
                }
            }
        });
    }

    @Test
    public void testACompletedRepairWhoseCleanupFaultsReleasesItsRepairSession() throws Exception {
        // The sibling of the failed-park case below, one boolean away from it. The unwind cleanup
        // runs on EVERY exit of the head-miss replay, not only an unwinding one: a turn that
        // completed its replay reaches the same statements, and the release that used to sit behind
        // them was gated on the turn NOT having completed. So a throw from the cleanup of a
        // COMPLETED turn skipped the release, and skipped the publication tail's own release with
        // it - the throw leaves the executor before that tail is entered. Nothing else frees the
        // session: this turn never parked, so the instance is not holding it and
        // handleRefreshFailure's discardSuspendedRepair finds nothing.
        //
        // The correction is repaired with the checkpoint splice declined, which is what leaves the
        // turn holding a repair session and no staged capture. Without that the capture would leak
        // too - it belongs to the publication tail the throw skips - and the oracle could not tell
        // the two apart.
        //
        // The throw is injected; there is no reproducible natural producer. What the case pins is
        // the ordering, which holds for any throwable those statements raise.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_MAX_CHAINED_BOUNDARIES, 0);
        assertMemoryLeak(() -> {
            createView(seedThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 1, "acct-1") + ", " + row(5, 2, "acct-2"), job);

                execute("insert into tx values " + row(2, 5, "acct-1"));
                drainWalQueue();

                // The next cleanup chain this worker runs, which is the correction's own.
                job.setSimulateRepairCleanupFaultForTest(0);
                runOneRefreshPass(job);

                Assert.assertFalse(
                        "the injected cleanup fault never fired, so no repair cleanup ran and the"
                                + " case pinned nothing",
                        job.isRepairCleanupFaultArmedForTest()
                );
                Assert.assertNull(
                        "a repair that faulted must leave nothing parked on the view",
                        viewInstance().getSuspendedRepair()
                );
                Assert.assertEquals(
                        "the injected cleanup fault must cost exactly one refresh fault",
                        1L,
                        viewInstance().getRefreshFaultCount()
                );

                // The view still converges: the fault is recoverable and the recompute is what
                // recovers it. assertViewMatchesRecompute() is not used here - it also asserts zero
                // faults, and this case injects one on purpose.
                driveRefreshToQuiescence(job);
                TestUtils.assertSqlCursors(
                        engine,
                        sqlExecutionContext,
                        "(" + recompute() + ") order by 2, 1",
                        "(lv) order by 2, 1",
                        LOG,
                        true
                );
            }
            // assertMemoryLeak is the oracle for the session itself: one nothing released leaves its
            // descriptor's three Paths and its scratch overlay allocated, which shows up as a
            // native-memory difference no assertion above can see.
        });
    }

    @Test
    public void testACompletedRepairWhosePublicationTailFaultsReleasesItsRepairSession() throws Exception {
        // One chain further on than the case above. A completed head-miss turn runs two cleanup
        // chains, not one: the replay's own, and then the publication tail that settles the runtime,
        // retires the timeline, frees the staged capture and rewinds the keyed runtime. The tail
        // ended with the session release, so a throw from any statement ahead of it skipped the
        // release - and by then the replay cleanup above has already run to its end and released
        // nothing, so the tail is the turn's last chance to end the repair. The turn did not park,
        // so the instance is not holding the session either and handleRefreshFailure's
        // discardSuspendedRepair finds nothing to free.
        //
        // Same fixture as the case above, with the fault armed one chain later so it lands on the
        // tail rather than on the replay cleanup ahead of it. The checkpoint splice stays declined
        // for the same reason: without that the staged capture would leak on both sides of the fix
        // and the oracle could not tell it from the session.
        //
        // The throw is injected; there is no reproducible natural producer. What the case pins is
        // the ordering, which holds for any throwable those statements raise.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_MAX_CHAINED_BOUNDARIES, 0);
        assertMemoryLeak(() -> {
            createView(seedThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 1, "acct-1") + ", " + row(5, 2, "acct-2"), job);

                execute("insert into tx values " + row(2, 5, "acct-1"));
                drainWalQueue();

                // Let the correction's replay cleanup through, and fault the publication tail
                // behind it.
                job.setSimulateRepairCleanupFaultForTest(1);
                runOneRefreshPass(job);

                Assert.assertFalse(
                        "the injected cleanup fault never fired, so the publication tail never ran"
                                + " and the case pinned nothing",
                        job.isRepairCleanupFaultArmedForTest()
                );
                Assert.assertNull(
                        "a repair that faulted must leave nothing parked on the view",
                        viewInstance().getSuspendedRepair()
                );
                Assert.assertEquals(
                        "the injected cleanup fault must cost exactly one refresh fault",
                        1L,
                        viewInstance().getRefreshFaultCount()
                );

                // The view still converges: the fault is recoverable and the recompute is what
                // recovers it. assertViewMatchesRecompute() is not used here - it also asserts zero
                // faults, and this case injects one on purpose.
                driveRefreshToQuiescence(job);
                TestUtils.assertSqlCursors(
                        engine,
                        sqlExecutionContext,
                        "(" + recompute() + ") order by 2, 1",
                        "(lv) order by 2, 1",
                        LOG,
                        true
                );
            }
            // assertMemoryLeak is the oracle for the session itself: one nothing released leaves its
            // descriptor's three Paths and its scratch overlay allocated, which shows up as a
            // native-memory difference no assertion above can see.
        });
    }

    @Test
    public void testADropOnAnotherThreadWaitsOutTheDroppedViewsRefreshTurn() throws Exception {
        // DROP LIVE VIEW runs on a SQL thread, and the refresh worker can be inside a turn of the
        // very view it drops. The drop waits in its refresh fence until that turn lets go of the
        // view's latch, and the worker then carries on with the other view it serves through the
        // same timeline writer.
        //
        // The turn is pinned mid-seal rather than hoped for: the worker, on a thread of its own,
        // stops in the mkdirs its timeline writer runs right after binding the scratch it freezes
        // into. The drop starts only then, and the turn goes on only once the drop has marked the
        // view dropped, which it does before it waits for the latch.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        final SealPausingFilesFacade ff = new SealPausingFilesFacade();
        assertMemoryLeak(ff, () -> {
            createView(row(2, 1, "acct-1"));
            createSecondView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                final LiveViewInstance second = viewInstance("lv2");
                final long sealFailuresBefore = second.getCheckpointSealFailures();
                execute("INSERT INTO tx VALUES " + row(2, 2, "acct-1"));
                execute("INSERT INTO tx2 VALUES " + row(5, 2, "acct-9"));
                drainWalQueue();
                setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);

                final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
                final AtomicBoolean isDropReturned = new AtomicBoolean();
                final Thread worker = new Thread(() -> {
                    try {
                        job.processNotificationsForTest();
                    } catch (Throwable th) {
                        errors.add(th);
                    } finally {
                        Path.clearThreadLocals();
                    }
                }, "lv-refresh-worker");
                final Thread dropper = new Thread(() -> {
                    try (SqlExecutionContext dropContext = TestUtils.createSqlExecutionCtx(engine)) {
                        engine.execute("DROP LIVE VIEW lv2", dropContext);
                        isDropReturned.set(true);
                    } catch (Throwable th) {
                        errors.add(th);
                    } finally {
                        Path.clearThreadLocals();
                    }
                }, "lv-dropper");
                ff.arm(worker, checkpointMetaDir("lv2"));
                worker.start();
                try {
                    Assert.assertTrue("the refresh turn never reached the seal of lv2", ff.awaitPaused());
                    dropper.start();
                    awaitDropped(second, 60_000);
                    // The worker still holds the latch of lv2, so the drop cannot be past its fence.
                    Assert.assertFalse("the drop must wait out the refresh turn in flight", isDropReturned.get());
                    Assert.assertTrue("the drop must keep lv2 registered until the turn ends", engine.getLiveViewRegistry().hasView("lv2"));
                } finally {
                    ff.release();
                    worker.join(60_000);
                    if (dropper.getState() != Thread.State.NEW) {
                        dropper.join(60_000);
                    }
                }
                Assert.assertFalse("the refresh worker did not finish", worker.isAlive());
                Assert.assertFalse("the dropper did not finish", dropper.isAlive());
                if (!errors.isEmpty()) {
                    throw new RuntimeException("a thread failed", errors.peek());
                }
                Assert.assertTrue(isDropReturned.get());
                // The turn the drop waited out ran to its end, seal included: the drop retires the
                // checkpoint directory only once it is past its fence.
                Assert.assertEquals(
                        "the seal the drop waited out must not fail",
                        sealFailuresBefore,
                        second.getCheckpointSealFailures()
                );
                Assert.assertEquals(
                        "the seal the drop waited out must publish the boundary it was pinned in",
                        ts("2026-01-05T02:00:00.000000Z"),
                        second.getHeadCheckpointMaxTs()
                );
                Assert.assertFalse(engine.getLiveViewRegistry().hasView("lv2"));
                Assert.assertNull(engine.getTableTokenIfExists("lv2"));

                driveRefreshToQuiescence(job);
                assertNoRefreshFaults("lv");
                assertQuery("SELECT * FROM lv ORDER BY created_at")
                        .noLeakCheck()
                        .timestamp("created_at")
                        .expectSize()
                        .returns("""
                                created_at\taccount_id\tcumulative_sum\tcumulative_count
                                2026-01-02T01:00:00.000000Z\tacct-1\t1.0\t1
                                2026-01-02T02:00:00.000000Z\tacct-1\t2.0\t2
                                """);
            }
        });
    }

    @Test
    public void testADropThatLosesItsFreeToAClosingWorkerStillFreesTheView() throws Exception {
        // A closing worker discards the repairs it parked under each view's refresh latch, and a
        // DROP of one of those views can find that latch taken. DROP frees the view last, after
        // it has unregistered it, and like every other latch taker it leaves that free to the
        // latch holder when it cannot take the latch: the holder retries once it lets go. Nothing
        // else reaches a view that is gone from the registry, so the worker close has to run that
        // retry itself, or the view's compiled factory, its maps and its tier stay allocated for
        // good.
        //
        // The interleaving is pinned rather than hoped for. The drop stops in the first file check
        // of its timeline retire, which comes after its refresh fence and before it unregisters
        // the view. The worker close starts only then, takes the latch and stops inside the
        // discard of the parked capture. The drop then runs to its end and finds the latch taken
        // when it comes to free the view, and the worker close goes on only after that.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_REPLAY_MAX_ROWS, 1);
        final RetirePausingFilesFacade ff = new RetirePausingFilesFacade();
        assertMemoryLeak(ff, () -> {
            final LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1);
            final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
            final Thread closer = new Thread(() -> {
                try {
                    job.close();
                } catch (Throwable th) {
                    errors.add(th);
                } finally {
                    Path.clearThreadLocals();
                }
            }, "lv-worker-close");
            final Thread dropper = new Thread(() -> {
                try (SqlExecutionContext dropContext = TestUtils.createSqlExecutionCtx(engine)) {
                    engine.execute("DROP LIVE VIEW lv", dropContext);
                } catch (Throwable th) {
                    errors.add(th);
                } finally {
                    Path.clearThreadLocals();
                }
            }, "lv-dropper");
            try {
                parkAStagedCapture(job);
                final LiveViewInstance instance = viewInstance();
                Assert.assertNotNull(instance.getSuspendedRepair());
                Assert.assertNotNull("the view must hold a compiled factory for the drop to free", instance.getCompiledFactory());
                final CountDownLatch closerInDiscard = new CountDownLatch(1);
                final CountDownLatch resumeCloser = new CountDownLatch(1);
                checkpointTimelineStoreWriter(job).setRepairScratchReleaseHookForTest(() -> {
                    if (Thread.currentThread() == closer) {
                        closerInDiscard.countDown();
                        try {
                            if (!resumeCloser.await(60, TimeUnit.SECONDS)) {
                                errors.add(new AssertionError("the worker close was never resumed"));
                            }
                        } catch (InterruptedException e) {
                            errors.add(e);
                        }
                    }
                });
                ff.arm(dropper, checkpointTimelinePath("lv"));
                dropper.start();
                try {
                    Assert.assertTrue("the drop never reached its timeline retire", ff.awaitPaused());
                    Assert.assertTrue(instance.isDropped());
                    closer.start();
                    Assert.assertTrue("the worker close never reached its discard", closerInDiscard.await(60, TimeUnit.SECONDS));
                    ff.release();
                    dropper.join(60_000);
                    Assert.assertFalse("the dropper did not finish", dropper.isAlive());
                    // The drop is over, and the worker close still holds the latch, so the drop
                    // left the free to it.
                    Assert.assertFalse(engine.getLiveViewRegistry().hasView("lv"));
                    Assert.assertNotNull(
                            "the drop must have found the latch taken and left its free to the worker close",
                            instance.getCompiledFactory()
                    );
                } finally {
                    ff.abandon();
                    resumeCloser.countDown();
                    if (dropper.getState() != Thread.State.NEW) {
                        dropper.join(60_000);
                    }
                    if (closer.getState() != Thread.State.NEW) {
                        closer.join(60_000);
                    }
                }
                Assert.assertFalse("the worker close did not finish", closer.isAlive());
                if (!errors.isEmpty()) {
                    throw new RuntimeException("a thread failed", errors.peek());
                }
                Assert.assertNull("the worker close must let go of the repair", instance.getSuspendedRepair());
                Assert.assertNull(
                        "the worker close must free the dropped view whose latch it held",
                        instance.getCompiledFactory()
                );
            } finally {
                if (closer.getState() == Thread.State.NEW) {
                    job.close();
                }
            }
        });
    }

    @Test
    public void testAFailedParkWhoseCleanupAlsoFaultsReleasesItsRepairSession() throws Exception {
        // A park that fails hands its turn back to the executor's unwind, and that unwind runs a
        // list of cleanups - free the staged capture, retire the durable descriptor, drop the
        // boundary schedule, retire the timeline - before it releases the repair session. Every
        // one of them touches native memory or files, so any of them can throw; and a throw from
        // one used to skip the release, because the release sat in the same finally behind them.
        //
        // What that costs depends on whether the instance is already holding the session. A turn
        // that RESUMES a park is holding it, so handleRefreshFailure's discardSuspendedRepair
        // picks the skipped release up on the way out and nothing is lost. The FIRST turn of a
        // repair is not: it only attaches its session after a park succeeds, so a first turn whose
        // park failed leaves a session nothing points at - its descriptor mapping, its three Paths
        // and the pre-repair window state its overlay holds go with the turn, for the life of the
        // process. That is the case here, and assertMemoryLeak is its oracle.
        //
        // Both faults are injected; neither has a reproducible natural producer. The first stands
        // in for the OutOfMemoryError a real key-set growth raises inside suspend()'s copy, on the
        // read side so it can fail the FIRST park - the write side belongs to a session that does
        // not exist until the park creates it. The second stands in for a throw out of any of the
        // cleanup statements above the release; what the case pins is the ordering, which holds
        // for any throwable they raise.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_REPLAY_MAX_ROWS, 1);
        assertMemoryLeak(() -> {
            createView(seedThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                // Head rows, so the runtime's own segment is the fifth day and the three seeded
                // days are all closed below it.
                commit(row(5, 1, "acct-1") + ", " + row(5, 2, "acct-2"), job);

                execute("insert into tx values " + row(2, 5, "acct-1"));
                drainWalQueue();

                final ReadThrowingGroupKeySet injectedCopyFailure = failTheFirstUniquenessCopy(job);
                job.setSimulateRepairUnwindCleanupFaultForTest();
                runOneRefreshPass(job);

                Assert.assertTrue(
                        "the injected copy failure never fired, so no park failed and the case pinned nothing",
                        injectedCopyFailure.hasFired()
                );
                // The cleanup fault clears itself as it throws, so an armed hook here means the
                // unwind never reached the cleanup block - which would leave the case asserting
                // nothing about the release that follows it.
                Assert.assertFalse(
                        "the injected cleanup fault never fired, so the unwind never reached the cleanup"
                                + " and the case pinned nothing",
                        job.isRepairUnwindCleanupFaultArmedForTest()
                );
                Assert.assertNull(
                        "a park that failed must leave no repair parked on the view",
                        viewInstance().getSuspendedRepair()
                );
                // The turn unwound through handleRefreshFailure, which counts the fault and
                // recomputes the view from the applied base. Exactly one: the recompute must not
                // fault again.
                Assert.assertEquals(
                        "the injected park failure must cost exactly one refresh fault",
                        1L,
                        viewInstance().getRefreshFaultCount()
                );

                // The view still converges: the fault is recoverable and the recompute is what
                // recovers it. assertViewMatchesRecompute() is not used here - it also asserts
                // zero faults, and this case injects one on purpose.
                driveRefreshToQuiescence(job);
                TestUtils.assertSqlCursors(
                        engine,
                        sqlExecutionContext,
                        "(" + recompute() + ") order by 2, 1",
                        "(lv) order by 2, 1",
                        LOG,
                        true
                );
            }
            // assertMemoryLeak is the oracle for the session itself: one nothing released leaves
            // its descriptor's three Paths and its scratch overlay allocated, which shows up as a
            // native-memory difference no assertion above can see.
        });
    }

    @Test
    public void testAForeignWorkerLeavesAParkedSegmentLoopToItsOwner() throws Exception {
        // The multi-worker shape of the yield. Every other case in this class drives a single
        // refresh job, so a segment loop only ever parks and resumes on the same object. A
        // production pool runs more than one worker, and the notification queue is not
        // sharded - any worker can be handed a base-table task for a view another worker has
        // parked a repair on. That worker must leave it alone: the parked session holds one
        // pinned base reader, one uncommitted replacement and a replay standing half-way
        // through a segment, and a second worker driving them would publish over its own
        // pinned snapshot. Only the owner may continue it, and the loop position it parked
        // must survive the stranger's whole pass.
        //
        // Deterministic by construction rather than by timing: the two workers never run at
        // the same time. Every pass runs to completion before the next one starts, so the
        // interleaving under test - park on worker 0, a full pass on worker 1, resume on
        // worker 0 - is fixed by the call order. Worker 1's passes run on their own thread so
        // the identity under test is a real second worker, not a second object on this one,
        // and the test thread joins each pass before asserting anything about it.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_REPLAY_MAX_ROWS, 1);
        assertMemoryLeak(() -> {
            createView(seedThreeDays());
            final LiveViewInstance instance = viewInstance();
            // The idle fallback scan is sharded by table id (LiveViewRegistry.getShardedViews),
            // so pin which worker owns lv rather than assuming it. Worker 0 owning the shard is
            // what lets the shared drive helpers park the repair exactly as the single-worker
            // cases do; worker 1 then reaches the view only through the notification queue,
            // which is the production route a foreign worker takes to a parked view. Without
            // this assertion a fixture edit that shifted lv's table id would silently move the
            // shard and leave every assertion below vacuous.
            Assert.assertEquals(
                    "worker 0 must own the lv shard, or the drive helpers never park the repair",
                    0,
                    Math.floorMod(instance.getLiveViewToken().getTableId(), REFRESH_WORKER_COUNT)
            );
            try (
                    LiveViewRefreshJob owner = new LiveViewRefreshJob(0, REFRESH_WORKER_COUNT, engine, 1);
                    LiveViewRefreshJob foreign = new LiveViewRefreshJob(1, REFRESH_WORKER_COUNT, engine, 1)
            ) {
                driveRefreshToQuiescence(owner);
                commit(row(5, 1, "acct-1") + ", " + row(5, 2, "acct-2"), owner);

                // The control the back-off assertions are measured against. This is the same
                // notification route worker 1 takes below, over the same view, with nothing
                // parked: it reaches refreshInstance and advances the watermark. Without it a
                // "worker 1 changed nothing" assertion would also pass on a worker that never
                // got near the view - which is exactly what a sharded fallback scan would do.
                final long processedBeforeControl = instance.getLastProcessedSeqTxn();
                execute("insert into tx values " + row(5, 3, "acct-2"));
                Assert.assertTrue(
                        "the foreign worker must dequeue the base-table notification",
                        runOnePassOnItsOwnThread(foreign, "lv-foreign-refresh-control")
                );
                Assert.assertTrue(
                        "the foreign worker's notification route must reach this view and refresh it",
                        instance.getLastProcessedSeqTxn() > processedBeforeControl
                );
                drainWalQueue();
                driveRefreshToQuiescence(owner);

                // Park. Two closed segments under a one-row replay budget, so the first parks
                // with the second still queued behind it on the loop - the case
                // testTheSegmentBehindAParkedOneIsRepairedByTheResumingTurn covers on one
                // worker.
                execute("insert into tx values " + row(2, 5, "acct-1") + ", " + row(3, 5, "acct-1"));
                drainWalQueue();
                driveUntilParked(owner, "lv");
                final LiveViewCheckpointRepairSession parked = instance.getSuspendedRepair();
                Assert.assertNotNull("the segment repair must park before the foreign pass", parked);
                Assert.assertSame("the worker that parked the repair must own it", owner, parked.getOwner());
                Assert.assertTrue(
                        "a one-row replay budget must park the first segment of the loop",
                        owner.segmentYieldCountForTest() > 0
                );

                // The stranger's turn. Re-publishing the base head enqueues a task for tx
                // without adding a row, which is what a coalesced or re-published commit
                // notification looks like to a pool: worker 1 dequeues it and walks into the
                // same refreshInstance the control just proved it reaches.
                final long baseHead = baseHead();
                Assert.assertTrue(
                        "the parked view must still be lagging, or the foreign pass stops short of refreshInstance",
                        baseHead > instance.getLastProcessedSeqTxn()
                );
                final long processedBeforeForeign = instance.getLastProcessedSeqTxn();
                final long resumesBeforeForeign = instance.getCheckpointRepairResumes();
                final String durableBeforeForeign = viewContents();
                engine.getLiveViewStateStore()
                        .notifyBaseTableCommit(engine.verifyTableName("tx"), baseHead);
                Assert.assertTrue(
                        "the foreign worker must dequeue the base-table notification",
                        runOnePassOnItsOwnThread(foreign, "lv-foreign-refresh-parked")
                );

                Assert.assertSame(
                        "the foreign worker must leave the parked session where it found it",
                        parked,
                        instance.getSuspendedRepair()
                );
                Assert.assertSame(
                        "the foreign worker must not take ownership of another worker's repair",
                        owner,
                        parked.getOwner()
                );
                Assert.assertEquals(
                        "the foreign worker must not continue another worker's replay",
                        resumesBeforeForeign,
                        instance.getCheckpointRepairResumes()
                );
                Assert.assertEquals(
                        "the foreign worker must not repair a segment off a snapshot it does not hold",
                        0,
                        foreign.segmentRepairCountForTest()
                );
                Assert.assertEquals(0, foreign.segmentYieldCountForTest());
                Assert.assertEquals(
                        "a backed-off pass must not consume the change the parked loop still owes",
                        processedBeforeForeign,
                        instance.getLastProcessedSeqTxn()
                );
                TestUtils.assertEquals(
                        "a backed-off pass must leave the durable view exactly as it found it",
                        durableBeforeForeign,
                        viewContents()
                );
                // refreshInstance takes the refresh latch before it reads the session owner and
                // releases it in an outer finally, so a stranger's back-off must not lock the
                // owner out of its own resume. Taking the latch here says so directly, ahead of
                // the resume that would otherwise only say it indirectly - and it names the
                // latch if it ever does strand, instead of surfacing as a view that never
                // converges.
                Assert.assertTrue(
                        "the foreign back-off must not strand the refresh latch",
                        instance.tryLockForRefresh()
                );
                instance.unlockAfterRefresh();

                // Resume. The owner finishes the loop it parked, across the stranger's pass.
                driveRefreshToQuiescence(owner);
                Assert.assertNull(
                        "the owner must finish the repair it parked",
                        instance.getSuspendedRepair()
                );
                Assert.assertTrue(
                        "the owner, not the stranger, must be the worker that resumed",
                        instance.getCheckpointRepairResumes() > resumesBeforeForeign
                );
                Assert.assertEquals(
                        "both segments must be repaired, each exactly once, across the foreign pass",
                        2,
                        owner.segmentRepairCountForTest()
                );
                Assert.assertEquals(
                        "the foreign worker must have repaired nothing at all",
                        0,
                        foreign.segmentRepairCountForTest()
                );
                Assert.assertEquals(
                        "the loop's last segment must advance the watermark over the whole change",
                        baseHead,
                        instance.getLastProcessedSeqTxn()
                );
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testAParkThatFailsItsUniquenessCopyKeepsTheCallerOwningEverything() throws Exception {
        // A park hands suspend() the three resources the next turn continues from - the pinned
        // base reader, the writer carrying the uncommitted replacement and the staged capture -
        // and the caller only stops owning them once suspend() returns. LiveViewRefreshJob
        // raises walWriterRetained after the call, its finally closes the writer while that flag
        // is down, and the unwind then runs endRepairSession -> close(), which frees whatever
        // the session holds. So a suspend() that takes them and then throws has the writer
        // returned to the pool twice, and AbstractMultiTenantPool answers the second return with
        // "double close" - which masks the original fault and aborts close() before it frees the
        // descriptor, the overlay and the seal carryover.
        //
        // The one statement in suspend() that can throw is the uniqueness copy, whose
        // GroupKeySet.add() allocates when the parked timestamp group is wider than the set it
        // grew from. A real allocation failure is not reproducible, so the set injected below
        // stands in for one: what the case pins is the ordering, which holds for any throwable
        // the copy raises.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE park_owner (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final TableToken token = engine.verifyTableName("park_owner");

            // Two rows of one timestamp group, which is what leaves keys in the set the copy walks -
            // a group of one is held in a scalar and never touches it.
            final LiveViewCheckpointOutputUniqueness parked = new LiveViewCheckpointOutputUniqueness();
            parked.of(1);
            Assert.assertTrue(parked.observe(1_000, 7));
            Assert.assertTrue(parked.observe(1_000, 8));

            // A bare window factory over the base table, built here rather than taken off a
            // compiled view: the session only stores it and hands it back through
            // getWindowFactory(), and neither suspend() nor close() reads it, which is what
            // lets the case stand on a session rather than on a compiled view. It is built
            // at all because the constructor parameter is @NotNull, which an instrumented
            // build enforces at the call.
            try (
                    LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1);
                    TableReader baseReader = engine.getReader(token);
                    WalWriter walWriter = engine.getWalWriter(token);
                    RecordCursorFactory baseFactory = select("SELECT ts, x FROM park_owner");
                    WindowRecordCursorFactory windowFactory = new WindowRecordCursorFactory(
                            baseFactory,
                            GenericRecordMetadata.copyOf(baseFactory.getMetadata()),
                            new ObjList<>()
                    )
            ) {
                final LiveViewCheckpointRepairSession session =
                        new LiveViewCheckpointRepairSession(engine.getConfiguration(), job, windowFactory);
                try {
                    failTheUniquenessCopy(session);
                    try {
                        session.suspend(
                                baseReader,
                                walWriter,
                                null,
                                1_000,
                                0,
                                0,
                                0,
                                0,
                                Numbers.LONG_NULL,
                                Numbers.LONG_NULL,
                                parked
                        );
                        Assert.fail("the injected copy failure must have failed the park");
                    } catch (InjectedAllocationFailure expected) {
                        // The park failed, so none of it may have taken effect.
                    }

                    // The two that discriminate. Both are non-null once the pre-fix ordering has
                    // run its three handovers, and the pool answers the second close of either
                    // with "double close".
                    Assert.assertNull("the caller still owns the pinned reader", session.takeBaseReader());
                    Assert.assertNull("the caller still owns the writer", session.takeWalWriter());
                    // The two below hold under the pre-fix ordering too - it sets both after the
                    // copy - so they pin that a failed park leaves no half-parked bookkeeping
                    // behind rather than pinning the ordering itself. The capture is not asserted
                    // here: this case passes suspend() a null one, which makes any claim about it
                    // vacuous. testAParkWhoseSuspendFailsFreesTheStagedCapture drives the job's own
                    // park with a real staged capture and pins what happens to it.
                    Assert.assertFalse("a failed park must not read as suspended", session.isSuspended());
                    Assert.assertEquals("a failed park must not count as a turn", 0, session.getTurns());
                } finally {
                    session.close();
                }
            }
            // The try-with-resources closes the reader and the writer exactly once each. A session
            // that had taken them would have closed them first, and the pool refuses the second
            // return with "double close".
        });
    }

    @Test
    public void testAParkWhoseSuspendFailsFreesTheStagedCapture() throws Exception {
        // The caller-side half of the park's ownership contract, which only the job's own path
        // shows. suspend() takes the staged capture along with the reader and the writer, and
        // LiveViewRefreshJob clears its own timelineCapture only after the call returns. The
        // executor's cleanup for that capture is gated on !yielded - a repair that really parked
        // keeps it for its next turn - so a suspend() that throws leaves a turn that IS yielded
        // holding a capture nothing frees: the local still points at it and the session never
        // took it. The direct-call case above cannot observe that gate; it calls suspend() rather
        // than the job.
        //
        // What leaks is not bookkeeping. A RepairCapture owns two Paths of its own and a data
        // segment writer holding three more plus its mapping, and that is what assertMemoryLeak
        // catches here - the whole leak, before any boundary is frozen.
        //
        // The same close() is also the only thing that unlinks a staged d.<id>.tmp segment,
        // while the session's own close() discards the descriptor that named it and
        // LiveViewCheckpointRepairState.sweep reaches a staged segment only through a descriptor
        // that names it. This fixture does not reach that: the repair it parks re-versions no
        // logical boundary, so the capture never opens a data segment, and the .tmp assertion
        // below reads zero on both sides of the fix. The staged half is covered by
        // testAParkWhoseSuspendFailsUnlinksItsStagedSegment, whose correction sits low enough
        // in its segment for the replay to freeze a boundary before it parks.
        //
        // The failure is injected into the park that follows the first one, so the turn under
        // test is a genuine resumed turn: the detector the session hands back is disarmed and
        // seeded with one key, so the resumed turn's own copy carries that key forward untouched
        // and the next park's copy into the session hits the injected set. One shot, no timing.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_REPLAY_MAX_ROWS, 1);
        assertMemoryLeak(() -> {
            createView(seedThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                // Head rows, so the runtime's own segment is the fifth day and the three seeded
                // days are all closed below it.
                commit(row(5, 1, "acct-1") + ", " + row(5, 2, "acct-2"), job);

                execute("insert into tx values " + row(2, 5, "acct-1"));
                drainWalQueue();
                driveUntilParked(job, "lv");

                final LiveViewCheckpointRepairSession parked = viewInstance().getSuspendedRepair();
                Assert.assertNotNull("the repair must park for the case to reach a second park", parked);
                Assert.assertNotNull(
                        "the parked repair must hold a staged capture, or there is nothing to strand",
                        stagedCapture(parked)
                );

                final ThrowingGroupKeySet injected = failTheNextUniquenessCopy(parked);
                driveRefreshToQuiescence(job);

                Assert.assertTrue(
                        "the injected copy failure never fired, so the case pinned nothing",
                        injected.hasFired()
                );
                // The turn unwound through handleRefreshFailure, which counts the fault and
                // recomputes the view from the applied base. Exactly one: the recompute must not
                // fault again.
                Assert.assertEquals(
                        "the injected park failure must cost exactly one refresh fault",
                        1L,
                        viewInstance().getRefreshFaultCount()
                );
                Assert.assertNull(
                        "a park that failed must leave no repair parked on the view",
                        viewInstance().getSuspendedRepair()
                );
                // Zero on both sides of the fix in this fixture, for the reason stated above.
                // The shape where the reading discriminates - a park whose capture has already
                // frozen a boundary - is driven by
                // testAParkWhoseSuspendFailsUnlinksItsStagedSegment.
                Assert.assertEquals(
                        "no staged segment may outlive the repair that opened it",
                        0,
                        stagedTmpSegmentCount(viewInstance())
                );
                // The view still converges: the fault is recoverable and the recompute is what
                // recovers it. assertViewMatchesRecompute() is not used here - it also asserts
                // zero faults, and this case injects one on purpose.
                TestUtils.assertSqlCursors(
                        engine,
                        sqlExecutionContext,
                        "(" + recompute() + ") order by 2, 1",
                        "(lv) order by 2, 1",
                        LOG,
                        true
                );
            }
            // assertMemoryLeak is the oracle for the capture itself: a stranded one leaves its own
            // and its data segment writer's Paths allocated, which shows up as a NATIVE_PATH
            // difference no other assertion here can see.
        });
    }

    @Test
    public void testAParkedCaptureDroppedOnAnotherThreadLeavesAnotherViewsSealIntact() throws Exception {
        // A worker's timeline writer serves every view the worker seals, and a capture the worker
        // parks keeps one of the writer's repair scratches leased across turns. DROP closes that
        // capture on the dropping thread, under the dropped view's refresh latch - which the
        // worker does not take while it seals a different view. So the release of the parked
        // scratch can land in the middle of the worker's seal of another view, and it must leave
        // that seal alone.
        //
        // The interleaving is pinned rather than hoped for. The dropping thread stops inside the
        // release of the parked scratch, once the capture has let go of what it froze. The worker
        // then starts the seal of lv2 on the test thread and stops right after binding the scratch
        // it freezes into, in the mkdirs that precedes its freeze. The drop finishes only then,
        // and the seal goes on to freeze only once the drop has returned.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_REPLAY_MAX_ROWS, 1);
        final SealPausingFilesFacade ff = new SealPausingFilesFacade();
        assertMemoryLeak(ff, () -> {
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                createSecondView();
                parkAStagedCapture(job);
                final LiveViewCheckpointTimelineStoreWriter writer = checkpointTimelineStoreWriter(job);
                Assert.assertEquals("the parked capture must hold its scratch lease", 1, writer.getLeasedRepairScratchCountForTest());
                final LiveViewInstance second = viewInstance("lv2");
                final long sealFailuresBefore = second.getCheckpointSealFailures();
                execute("INSERT INTO tx2 VALUES " + row(5, 2, "acct-9"));
                drainWalQueue();
                setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);

                final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
                final CountDownLatch releasePaused = new CountDownLatch(1);
                final Thread dropper = new Thread(() -> {
                    try (SqlExecutionContext dropContext = TestUtils.createSqlExecutionCtx(engine)) {
                        engine.execute("DROP LIVE VIEW lv", dropContext);
                    } catch (Throwable th) {
                        errors.add(th);
                    } finally {
                        // The seal goes on only once the drop is over, whichever way it ended.
                        ff.release();
                        Path.clearThreadLocals();
                    }
                }, "lv-dropper");
                writer.setRepairScratchReleaseHookForTest(() -> {
                    if (Thread.currentThread() == dropper) {
                        releasePaused.countDown();
                        if (!ff.awaitPaused()) {
                            errors.add(new AssertionError("the seal of lv2 never paused"));
                        }
                    }
                });
                dropper.start();
                try {
                    Assert.assertTrue("the drop never released the parked capture's scratch", releasePaused.await(60, TimeUnit.SECONDS));
                    ff.arm(Thread.currentThread(), checkpointMetaDir("lv2"));
                    job.processNotificationsForTest();
                } finally {
                    ff.abandon();
                    dropper.join(60_000);
                    writer.setRepairScratchReleaseHookForTest(null);
                }
                Assert.assertFalse("the dropper did not finish", dropper.isAlive());
                if (!errors.isEmpty()) {
                    throw new RuntimeException("a thread failed", errors.peek());
                }
                Assert.assertTrue("the seal of lv2 never paused, so the case pinned nothing", ff.hasPaused());
                Assert.assertEquals(
                        "the drop of a parked capture must not fail the seal of another view on the same writer",
                        sealFailuresBefore,
                        second.getCheckpointSealFailures()
                );
                Assert.assertEquals(
                        "the seal of lv2 must publish the boundary it was pinned in",
                        ts("2026-01-05T02:00:00.000000Z"),
                        second.getHeadCheckpointMaxTs()
                );
                Assert.assertFalse(engine.getLiveViewRegistry().hasView("lv"));
                Assert.assertEquals("the drop must return the capture's scratch lease", 0, writer.getLeasedRepairScratchCountForTest());
                Assert.assertEquals("the drop must free the payloads the capture froze", 0, writer.getRetainedFrozenPayloadBytesForTest());

                driveRefreshToQuiescence(job);
                assertNoRefreshFaults("lv2");
                assertQuery("SELECT * FROM lv2 ORDER BY created_at")
                        .noLeakCheck()
                        .timestamp("created_at")
                        .expectSize()
                        .returns("""
                                created_at\taccount_id\tcumulative_sum\tcumulative_count
                                2026-01-05T01:00:00.000000Z\tacct-9\t1.0\t1
                                2026-01-05T02:00:00.000000Z\tacct-9\t2.0\t2
                                """);
            }
        });
    }

    @Test
    public void testAParkedCaptureDroppedOnAnotherThreadLeavesItsWritersActiveScratchAlone() throws Exception {
        // The writer's active scratch is the one its freeze reads, and each seal and each capture
        // on the worker binds it before the freeze reads it. A park leaves it naming the parked
        // capture's scratch. DROP releases that scratch on the dropping thread, while the worker
        // may be binding the scratch of a seal of another view. A release that cleared the active
        // scratch whenever it still named the released one could read it before the worker's bind
        // and write null after it, and the seal would then read null.
        //
        // No hook splits that check from that write, since both sit in one statement, so the case
        // pins the contract that rules the race out: a release on another thread leaves the
        // active scratch exactly as the park left it.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_REPLAY_MAX_ROWS, 1);
        assertMemoryLeak(() -> {
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                parkAStagedCapture(job);
                final LiveViewCheckpointRepairSession parked = viewInstance().getSuspendedRepair();
                Assert.assertNotNull(parked);
                final LiveViewCheckpointTimelineStoreWriter writer = checkpointTimelineStoreWriter(job);
                final Object parkedScratch = stagedCaptureScratch(parked);
                Assert.assertNotNull(parkedScratch);
                Assert.assertSame(
                        "the park must leave the writer's active scratch naming the parked capture's scratch",
                        parkedScratch,
                        writer.getActiveScratchForTest()
                );

                final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
                final Thread dropper = new Thread(() -> {
                    try (SqlExecutionContext dropContext = TestUtils.createSqlExecutionCtx(engine)) {
                        engine.execute("DROP LIVE VIEW lv", dropContext);
                    } catch (Throwable th) {
                        errors.add(th);
                    } finally {
                        Path.clearThreadLocals();
                    }
                }, "lv-dropper");
                dropper.start();
                dropper.join(60_000);
                Assert.assertFalse("the dropper did not finish", dropper.isAlive());
                if (!errors.isEmpty()) {
                    throw new RuntimeException("the drop failed", errors.peek());
                }
                Assert.assertFalse(engine.getLiveViewRegistry().hasView("lv"));
                Assert.assertEquals("the drop must return the capture's scratch lease", 0, writer.getLeasedRepairScratchCountForTest());
                Assert.assertSame(
                        "a release on another thread must leave the writer's active scratch alone",
                        parkedScratch,
                        writer.getActiveScratchForTest()
                );
                drainJob(job);
            }
        });
    }

    @Test
    public void testAParkedCaptureFreesItsPayloadsWhenItsViewIsDropped() throws Exception {
        // A parked capture holds the scratch its writer leased it, and with it the fused
        // payloads of every boundary it froze. DROP closes the parked session on the dropping
        // thread, under the view's refresh latch, and that close is the payloads' last owner:
        // the dropped view leaves the registry, so no later turn would free them.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_REPLAY_MAX_ROWS, 1);
        assertMemoryLeak(() -> {
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                parkAStagedCapture(job);
                final LiveViewCheckpointTimelineStoreWriter writer = checkpointTimelineStoreWriter(job);
                Assert.assertEquals("the parked capture must hold its scratch lease", 1, writer.getLeasedRepairScratchCountForTest());
                Assert.assertTrue(
                        "the parked capture must hold the payloads it froze",
                        writer.getRetainedFrozenPayloadBytesForTest() > 0
                );
                final LiveViewInstance instance = viewInstance();

                execute("DROP LIVE VIEW lv");

                // Before the worker runs again, let alone closes.
                Assert.assertTrue(instance.isDropped());
                Assert.assertNull("the drop must let go of the parked repair", instance.getSuspendedRepair());
                Assert.assertEquals("the drop must return the capture's scratch lease", 0, writer.getLeasedRepairScratchCountForTest());
                Assert.assertEquals(
                        "the drop must free the payloads the capture froze",
                        0,
                        writer.getRetainedFrozenPayloadBytesForTest()
                );
                drainJob(job);
            }
        });
    }

    @Test
    public void testAParkedCaptureFreesItsPayloadsWhenItsWorkerCloses() throws Exception {
        // A closing worker abandons the repair it parked before it frees its writer, so the
        // capture's own close hands its scratch - and the payloads it froze - back to a writer
        // that is still whole, and the writer's close then meets a scratch that holds nothing.
        // Neither may free the payloads twice: the leak check is what says they went exactly once.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_REPLAY_MAX_ROWS, 1);
        assertMemoryLeak(() -> {
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                parkAStagedCapture(job);
                Assert.assertTrue(
                        "the parked capture must hold the payloads it froze",
                        checkpointTimelineStoreWriter(job).getRetainedFrozenPayloadBytesForTest() > 0
                );
            }
            Assert.assertNull("a closing worker must let go of its repair", viewInstance().getSuspendedRepair());
        });
    }

    @Test
    public void testAParkedCaptureInvalidatedWhileItsWorkerClosesHasOneCloser() throws Exception {
        // A closing worker abandons the repairs it parked, and it is not the only one that can:
        // invalidation discards a parked repair on the invalidating thread, under the view's
        // refresh latch. The two meet at shutdown - a pool halt closes the live view workers while
        // the WAL apply workers, which invalidate a view whose base table changed under it, still
        // run. The worker has to take the same latch, or both close one session at once: two
        // returns of its pinned reader and its live-view WAL writer, and two frees of its native
        // state.
        //
        // The worker's close is pinned inside its discard - on the release of the parked
        // capture's scratch - and the invalidation runs only then. It must find the repair still
        // being discarded by its owner, and defer to it. The invalidation leaves the free of the
        // view's runtime state to the latch holder too, and a closed worker runs no later turn,
        // so the close has to free that state itself once it lets go of the latch.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_REPLAY_MAX_ROWS, 1);
        assertMemoryLeak(() -> {
            final LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1);
            final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
            final Thread closer = new Thread(() -> {
                try {
                    job.close();
                } catch (Throwable th) {
                    errors.add(th);
                } finally {
                    Path.clearThreadLocals();
                }
            }, "lv-worker-close");
            try {
                parkAStagedCapture(job);
                final LiveViewInstance instance = viewInstance();
                final LiveViewCheckpointRepairSession parked = instance.getSuspendedRepair();
                Assert.assertNotNull(parked);
                Assert.assertNotNull("the view must hold a compiled factory for the invalidation to free", instance.getCompiledFactory());
                final CountDownLatch closerInDiscard = new CountDownLatch(1);
                final CountDownLatch resumeCloser = new CountDownLatch(1);
                checkpointTimelineStoreWriter(job).setRepairScratchReleaseHookForTest(() -> {
                    if (Thread.currentThread() == closer) {
                        closerInDiscard.countDown();
                        try {
                            if (!resumeCloser.await(60, TimeUnit.SECONDS)) {
                                errors.add(new AssertionError("the worker close was never resumed"));
                            }
                        } catch (InterruptedException e) {
                            errors.add(e);
                        }
                    }
                });
                closer.start();
                try {
                    Assert.assertTrue("the worker close never reached its discard", closerInDiscard.await(60, TimeUnit.SECONDS));
                    engine.invalidateLiveViewsForBaseTable(engine.verifyTableName("tx"), "base table changed while its worker closes");
                    Assert.assertTrue(instance.isInvalid());
                    Assert.assertSame(
                            "the invalidation must leave a repair its owner is discarding to the owner",
                            parked,
                            instance.getSuspendedRepair()
                    );
                    Assert.assertNotNull(
                            "the invalidation must have found the latch taken and left its free to the worker close",
                            instance.getCompiledFactory()
                    );
                } finally {
                    resumeCloser.countDown();
                    closer.join(60_000);
                }
                Assert.assertFalse("the worker close did not finish", closer.isAlive());
                if (!errors.isEmpty()) {
                    throw new RuntimeException("the worker close failed", errors.peek());
                }
                Assert.assertNull("a closing worker must let go of its repair", instance.getSuspendedRepair());
                Assert.assertNull(
                        "the worker close must free the runtime state of the view invalidated while it held the latch",
                        instance.getCompiledFactory()
                );
                execute("DROP LIVE VIEW lv");
            } finally {
                if (closer.getState() == Thread.State.NEW) {
                    job.close();
                }
            }
        });
    }

    @Test
    public void testAParkWhoseSuspendFailsUnlinksItsStagedSegment() throws Exception {
        // The case the sibling above could not reach: a park that fails while the capture it
        // is handing over already holds an open data segment on disk.
        //
        // A capture opens d.<id>.tmp on the first boundary it freezes, and its close() is the
        // only thing that unlinks one no splice published. The session's own close() discards
        // the descriptor that names the segment, and LiveViewCheckpointRepairState.sweep
        // reaches a staged segment only through a descriptor that still names it - so a
        // capture the failed park stranded leaves the file with nothing in this process
        // pointing at it. The next restart still collects it, because
        // LiveViewCheckpointLifecycle.reconcile's orphan pass removes every .tmp under data/
        // and meta/ whatever named it, so what a strand costs a running process is the file
        // plus the capture's own descriptor and mapping until then. The executor's cleanup
        // for the capture is gated on !yielded, which is exactly what the failed park clears,
        // and this pins that gate from the file's side.
        //
        // Reaching a staged park needs boundaries INSIDE the repaired segment and above the
        // correction: the sibling's correction lands at the top of its segment, so its repair
        // re-versions no root and its capture never opens a segment at all. Here the segment's
        // rows are committed one at a time under a one-row checkpoint cadence, so each seals a
        // boundary, and the correction lands low enough that six of them sit above it. Under
        // the one-row replay budget the replay then freezes one boundary per turn, so the park
        // the failure is injected into is holding a real staged segment.
        //
        // The reading is taken on the turn that failed, not at quiescence: the recovery
        // recompute replans the correction and its own repair stages a segment of its own, and
        // a later turn's legitimate .tmp would mask - or impersonate - the stranded one.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_REPLAY_MAX_ROWS, 1);
        assertMemoryLeak(() -> {
            createView(row(2, 1, "acct-1"));
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                // One commit per row, so the cadence seals a boundary at each of them.
                for (int hour = 2; hour <= 8; hour++) {
                    commit(row(2, hour, "acct-1"), job);
                }
                // The day above, which closes the day-2 segment so its repair converges below
                // the runtime frontier and carries a finite high bound.
                for (int hour = 1; hour <= 2; hour++) {
                    commit(row(3, hour, "acct-1"), job);
                }
                // Head rows, so the runtime's own segment is neither of the two below it.
                commit(row(5, 1, "acct-1"), job);

                // Low in the day-2 segment, with six sealed boundaries above it.
                execute("insert into tx values " + row(2, 3, "acct-1"));
                drainWalQueue();
                final LiveViewCheckpointRepairSession parked = driveUntilStagedPark(job, "lv");
                Assert.assertEquals(
                        "the parked repair must hold the one segment its capture staged",
                        1,
                        stagedTmpSegmentCount(viewInstance())
                );

                final ThrowingGroupKeySet injected = failTheNextUniquenessCopy(parked);
                runOneRefreshPass(job);

                Assert.assertTrue(
                        "the injected copy failure never fired, so the case pinned nothing",
                        injected.hasFired()
                );
                Assert.assertEquals(
                        "the injected park failure must cost exactly one refresh fault",
                        1L,
                        viewInstance().getRefreshFaultCount()
                );
                Assert.assertNull(
                        "a park that failed must leave no repair parked on the view",
                        viewInstance().getSuspendedRepair()
                );
                // The assertion the case exists for. The turn that failed its park owns the
                // staged segment on the way out, and nothing else can name it afterwards.
                Assert.assertEquals(
                        "a failed park must unlink the segment its capture staged",
                        0,
                        stagedTmpSegmentCount(viewInstance())
                );

                // The view still converges: the fault is recoverable and the recompute is what
                // recovers it. assertViewMatchesRecompute() is not used here - it also asserts
                // zero faults, and this case injects one on purpose.
                driveRefreshToQuiescence(job);
                Assert.assertEquals(
                        "the recovery must strand no staged segment either",
                        0,
                        stagedTmpSegmentCount(viewInstance())
                );
                TestUtils.assertSqlCursors(
                        engine,
                        sqlExecutionContext,
                        "(" + recompute() + ") order by 2, 1",
                        "(lv) order by 2, 1",
                        LOG,
                        true
                );
            }
            // assertMemoryLeak is the second oracle: a stranded capture leaves its own Paths
            // and the file descriptor its data segment writer holds on the staged segment.
        });
    }

    @Test
    public void testAParkedSegmentRepairNamesNoDispositionAheadOfItsCounters() throws Exception {
        // What a reader of live_views() must not be shown: a repair's route beside counters
        // that carry none of its rows. The disposition is settled when the segment repair is
        // planned and the o3_* counters move only when its replay finishes, so the two are
        // published from different moments. A park holds those moments apart for whole
        // refresh turns, which is what makes the gap readable without racing the worker.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_REPLAY_MAX_ROWS, 1);
        assertMemoryLeak(() -> {
            createView(seedThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                // Head rows, so the runtime's own segment is the fifth day and the three
                // seeded days are all closed below it. Forward-only work runs no repair, so
                // the pair is NULL and every counter is still zero here.
                commit(row(5, 1, "acct-1") + ", " + row(5, 2, "acct-2"), job);

                execute("insert into tx values " + row(2, 5, "acct-1"));
                drainWalQueue();
                driveUntilParked(job, "lv");

                // Mid-repair. checkpoint_repair_in_progress is the column that says a repair
                // is in flight; the disposition names the last one that ran, and this one has
                // not replayed a row yet.
                assertQuery(REPAIR_READING)
                        .noLeakCheck().noRandomAccess()
                        .returns(REPAIR_READING_HEADER + "true\t\t0\t0\t0\n");

                driveRefreshToQuiescence(job);
                Assert.assertNull(viewInstance().getSuspendedRepair());
                // And when it lands, the route and its cost land together.
                assertQuery(REPAIR_READING)
                        .noLeakCheck().noRandomAccess()
                        .returns(REPAIR_READING_HEADER + "false\tlocalized rebuild\t0\t1\t6\n");
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testASegmentRepairYieldsOnItsTurnBudgetAndResumes() throws Exception {
        // The base case: one closed segment, nothing at the head, and a replay budget that
        // cannot carry the segment in one turn. The repair has to cross turns and still
        // publish once, over that segment alone.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_REPLAY_MAX_ROWS, 1);
        assertMemoryLeak(() -> {
            createView(seedThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                // Head rows, so the runtime's own segment is the fifth day and the three
                // seeded days are all closed below it.
                commit(row(5, 1, "acct-1") + ", " + row(5, 2, "acct-2"), job);

                final long baseHead = commit(row(2, 5, "acct-1"), job);

                Assert.assertTrue(
                        "a one-row replay budget must take the segment repair across several turns",
                        job.segmentYieldCountForTest() > 0
                );
                Assert.assertEquals(
                        "the parked segment must publish exactly once",
                        1,
                        job.segmentRepairCountForTest()
                );
                Assert.assertEquals(
                        "the loop's last segment must advance the watermark over the whole change",
                        baseHead,
                        viewInstance().getLastProcessedSeqTxn()
                );
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testAYieldedLoopStillRepairsItsResidual() throws Exception {
        // A change set with both halves: a correction in a closed segment and rows at the
        // head. The segment parks, and the residual behind it is the runtime's own
        // correction - the turn that finishes the segment owes it too. Dropping it would
        // leave the change unconsumed with the segment already repaired, which the next
        // drain would notice by repairing that segment a second time.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_REPLAY_MAX_ROWS, 1);
        assertMemoryLeak(() -> {
            createView(seedThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 1, "acct-1") + ", " + row(5, 2, "acct-2"), job);

                // The production shape of a deep commit: rows at the frontier beside rows in
                // one old segment.
                final long baseHead = commit(row(2, 5, "acct-1") + ", " + row(5, 3, "acct-2"), job);

                Assert.assertTrue(
                        "a one-row replay budget must take the segment repair across several turns",
                        job.segmentYieldCountForTest() > 0
                );
                Assert.assertEquals(
                        "the closed segment must be repaired once, not once per re-classification",
                        1,
                        job.segmentRepairCountForTest()
                );
                Assert.assertEquals(
                        "the residual must run on the turn that finished the loop, consuming the change",
                        baseHead,
                        viewInstance().getLastProcessedSeqTxn()
                );
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testDecliningTheIsolatedRuntimeUnderAParkedLoopLosesNoCorrection() throws Exception {
        // The runtime a parked repair is standing in can drift out from under it, and an
        // operator declining the isolated runtime mid-repair is the deterministic way to
        // make it. The candidate goes; the correction must not, because the loop never
        // advanced the watermark over it.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_REPLAY_MAX_ROWS, 1);
        assertMemoryLeak(() -> {
            createView(seedThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 1, "acct-1") + ", " + row(5, 2, "acct-2"), job);

                execute("insert into tx values " + row(2, 5, "acct-1") + ", " + row(3, 5, "acct-1"));
                drainWalQueue();
                driveUntilParked(job, "lv");

                setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_ISOLATED_RUNTIME_ENABLED, "false");
                driveRefreshToQuiescence(job);

                Assert.assertNull(
                        "the drifted candidate must be discarded rather than continued",
                        viewInstance().getSuspendedRepair()
                );
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testDecliningTheSegmentYieldKeepsASegmentReplayInOneTurn() throws Exception {
        // The escape hatch, and the control column a measurement runs against: the same
        // correction under the same budget, on the route every segment repair took before
        // the yield existed.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_REPLAY_MAX_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SEGMENT_YIELD_ENABLED, "false");
        assertMemoryLeak(() -> {
            createView(seedThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 1, "acct-1") + ", " + row(5, 2, "acct-2"), job);
                commit(row(2, 5, "acct-1"), job);

                Assert.assertEquals(
                        "a declined yield must carry the whole segment inside one turn",
                        0,
                        job.segmentYieldCountForTest()
                );
                Assert.assertEquals(1, job.segmentRepairCountForTest());
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testTheSegmentBehindAParkedOneIsRepairedByTheResumingTurn() throws Exception {
        // The loop position itself. Two closed segments and nothing at the head: the first
        // parks, and the turn that finishes it owes the second against the same pinned
        // snapshot. A repair count of two is what says the loop carried on rather than
        // being re-derived - a dropped position would leave the change unconsumed and the
        // next drain would repair the first segment again.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_REPLAY_MAX_ROWS, 1);
        assertMemoryLeak(() -> {
            createView(seedThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 1, "acct-1") + ", " + row(5, 2, "acct-2"), job);

                final long baseHead = commit(row(2, 5, "acct-1") + ", " + row(3, 5, "acct-1"), job);

                Assert.assertTrue(
                        "a one-row replay budget must park the first segment of the loop",
                        job.segmentYieldCountForTest() > 0
                );
                Assert.assertEquals(
                        "both segments must be repaired, and each exactly once",
                        2,
                        job.segmentRepairCountForTest()
                );
                Assert.assertEquals(
                        "the loop's last segment must advance the watermark over the whole change",
                        baseHead,
                        viewInstance().getLastProcessedSeqTxn()
                );
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testTheViewIsUnchangedWhileASegmentRepairIsParked() throws Exception {
        // What the yield must not cost: a reader seeing a half-repaired segment. The
        // replacement stays uncommitted in the writer the session holds and no generation
        // names the roots it has staged, so the durable view is the pre-repair one for
        // every turn the repair takes.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_REPLAY_MAX_ROWS, 1);
        assertMemoryLeak(() -> {
            createView(seedThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 1, "acct-1") + ", " + row(5, 2, "acct-2"), job);

                final String beforeRepair = viewContents();
                execute("insert into tx values " + row(2, 5, "acct-1"));
                drainWalQueue();
                driveUntilParked(job, "lv");

                TestUtils.assertEquals(
                        "a parked repair must leave the durable view exactly as it found it",
                        beforeRepair,
                        viewContents()
                );

                driveRefreshToQuiescence(job);
                Assert.assertNull(viewInstance().getSuspendedRepair());
                assertViewMatchesRecompute();
            }
        });
    }

    /**
     * Poisons the worker's OWN uniqueness detector - the source {@code suspend()} copies from -
     * so the copy fails on the first park of the next repair, before any session has been
     * attached to the view.
     * <p>
     * {@code failTheNextUniquenessCopy} cannot reach that park: it poisons the destination, which
     * belongs to the session, and no session exists until a park has created one. The source
     * belongs to the worker and exists before the repair starts, which is what makes a FIRST park
     * failable at all.
     *
     * @return the injected set, which records whether it actually fired
     */
    private static ReadThrowingGroupKeySet failTheFirstUniquenessCopy(LiveViewRefreshJob job) throws Exception {
        final Field jobUniquenessField = LiveViewRefreshJob.class.getDeclaredField("outputUniqueness");
        jobUniquenessField.setAccessible(true);
        final LiveViewCheckpointOutputUniqueness uniqueness =
                (LiveViewCheckpointOutputUniqueness) jobUniquenessField.get(job);
        final ReadThrowingGroupKeySet injected = new ReadThrowingGroupKeySet();
        final Field groupKeysField =
                LiveViewCheckpointOutputUniqueness.class.getDeclaredField("groupKeys");
        groupKeysField.setAccessible(true);
        groupKeysField.set(uniqueness, injected);
        return injected;
    }

    /**
     * Makes the next park of an already-parked {@code session} fail inside suspend()'s uniqueness
     * copy, once, while leaving the resume that precedes it able to run.
     * <p>
     * The copy's only fallible statement is {@link GroupKeySet#add(int)} on the session's own key
     * set, and it runs once per key the resuming turn's detector holds - so the injection needs
     * that detector to hold at least one key by the time the turn parks. Two edits arrange it
     * without depending on the data: the session's detector is disarmed, which the resume copies
     * over and which makes the replay observe nothing and touch no key set; and the session's key
     * set is replaced by one seeded with a single key, which the resume therefore carries forward
     * untouched into the detector the next park copies back.
     *
     * @return the injected set, which records whether it actually fired
     */
    private static ThrowingGroupKeySet failTheNextUniquenessCopy(LiveViewCheckpointRepairSession session)
            throws Exception {
        final LiveViewCheckpointOutputUniqueness uniqueness = session.getOutputUniqueness();
        final Field keyColumnIndexField =
                LiveViewCheckpointOutputUniqueness.class.getDeclaredField("keyColumnIndex");
        keyColumnIndexField.setAccessible(true);
        keyColumnIndexField.setInt(uniqueness, LiveViewCheckpointOutputUniqueness.NO_KEY_COLUMN);
        final ThrowingGroupKeySet injected = new ThrowingGroupKeySet(SEEDED_GROUP_KEY);
        final Field groupKeysField =
                LiveViewCheckpointOutputUniqueness.class.getDeclaredField("groupKeys");
        groupKeysField.setAccessible(true);
        groupKeysField.set(uniqueness, injected);
        return injected;
    }

    /**
     * Replaces the scratch key set inside {@code session}'s own uniqueness detector with one that
     * fails on the first key {@code copyFrom} adds - which is what an allocation failure inside
     * {@link GroupKeySet#add(int)} looks like from suspend()'s side.
     */
    private static void failTheUniquenessCopy(LiveViewCheckpointRepairSession session) throws Exception {
        final LiveViewCheckpointOutputUniqueness uniqueness = session.getOutputUniqueness();
        final Field groupKeysField =
                LiveViewCheckpointOutputUniqueness.class.getDeclaredField("groupKeys");
        groupKeysField.setAccessible(true);
        groupKeysField.set(uniqueness, new ThrowingGroupKeySet());
    }

    private static boolean isFrame(StackTraceElement frame, Class<?> frameClass, String methodName) {
        return frameClass.getName().equals(frame.getClassName()) && methodName.equals(frame.getMethodName());
    }

    /**
     * Whether {@code frames}, the stack of a thread in {@link LiveViewRefreshJob#close()}, show
     * that close spinning on a view's refresh latch: trying the latch, or pausing between two
     * tries, straight from the close.
     */
    private static boolean isSpinningOnARefreshLatch(StackTraceElement[] frames) {
        for (int i = 1, n = frames.length; i < n; i++) {
            if (isFrame(frames[i], LiveViewRefreshJob.class, "close")) {
                return isFrame(frames[i - 1], LiveViewInstance.class, "tryLockForRefresh")
                        || isFrame(frames[i - 1], Os.class, "pause");
            }
        }
        return false;
    }

    /**
     * The staged capture {@code session} is holding, read straight off the field rather than
     * through {@code takeCapture()}, which would take it away from the parked repair.
     */
    private static Object stagedCapture(LiveViewCheckpointRepairSession session) throws Exception {
        final Field captureField = LiveViewCheckpointRepairSession.class.getDeclaredField("capture");
        captureField.setAccessible(true);
        return captureField.get(session);
    }

    /**
     * The freeze scratch the staged capture of {@code session} leased from its writer, read off
     * the capture's field for an identity comparison.
     */
    private static Object stagedCaptureScratch(LiveViewCheckpointRepairSession session) throws Exception {
        final Object capture = stagedCapture(session);
        Assert.assertNotNull("the parked repair must hold a staged capture", capture);
        final Field scratchField = LiveViewCheckpointTimelineStoreWriter.RepairCapture.class.getDeclaredField("scratch");
        scratchField.setAccessible(true);
        return scratchField.get(capture);
    }

    private void assertViewMatchesRecompute() throws Exception {
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(" + recompute() + ") order by 2, 1",
                "(lv) order by 2, 1",
                LOG,
                true
        );
        assertNoRefreshFaults("lv");
    }

    /**
     * The base table's sequencer head, which is what a refresh worker's fallback scan compares
     * a view's watermark against.
     */
    private long baseHead() {
        return engine.getTableSequencerAPI()
                .getTxnTracker(engine.verifyTableName("tx"))
                .getWriterTxn();
    }

    /**
     * The checkpoint metadata directory of the named view exactly as its timeline writer's seal
     * creates it, trailing separator included.
     */
    private String checkpointMetaDir(String viewName) {
        try (
                Path checkpointsDir = new Path().of(engine.getConfiguration().getDbRoot())
                        .concat(engine.verifyTableName(viewName))
                        .concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
                Path metaDir = new Path()
        ) {
            return LiveViewCheckpointLayout.metaDirPath(metaDir, checkpointsDir).slash().toString();
        }
    }

    /**
     * The timeline file of the named view exactly as a DROP's timeline retire checks for it.
     */
    private String checkpointTimelinePath(String viewName) {
        try (
                Path checkpointsDir = new Path().of(engine.getConfiguration().getDbRoot())
                        .concat(engine.verifyTableName(viewName))
                        .concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
                Path timeline = new Path()
        ) {
            return LiveViewCheckpointLayout.timelinePath(timeline, checkpointsDir).toString();
        }
    }

    /**
     * Inserts {@code values}, drains, and drives the view to quiescence.
     *
     * @return the base table's sequencer head after the commit
     */
    private long commit(String values, LiveViewRefreshJob job) throws Exception {
        execute("insert into tx values " + values);
        drainWalQueue();
        driveRefreshToQuiescence(job);
        return engine.getTableSequencerAPI()
                .getTxnTracker(engine.verifyTableName("tx"))
                .getWriterTxn();
    }

    private void createView(String seedRows) throws Exception {
        execute("create table tx (created_at timestamp, account_id symbol nocache index capacity 4, "
                + "amount double) timestamp(created_at) partition by hour wal");
        execute("insert into tx values " + seedRows);
        drainWalQueue();
        execute("create live view lv flush every 100ms start from beginning as "
                + "select created_at, account_id, "
                + "sum(amount) over w as cumulative_sum, "
                + "count(account_id) over w as cumulative_count "
                + "from tx window w as (partition by account_id order by created_at anchor daily '00:00')");
    }

    /**
     * A second view of the same shape, over a base table of its own, so a refresh worker serves
     * two views through one timeline writer and a change to one base leaves the other idle.
     */
    private void createSecondView() throws Exception {
        execute("CREATE TABLE tx2 (created_at TIMESTAMP, account_id SYMBOL, amount DOUBLE)"
                + " TIMESTAMP(created_at) PARTITION BY HOUR WAL");
        execute("INSERT INTO tx2 VALUES " + row(5, 1, "acct-9"));
        drainWalQueue();
        execute("CREATE LIVE VIEW lv2 FLUSH EVERY 100ms START FROM BEGINNING AS "
                + "SELECT created_at, account_id, "
                + "sum(amount) OVER w AS cumulative_sum, "
                + "count(account_id) OVER w AS cumulative_count "
                + "FROM tx2 WINDOW w AS (PARTITION BY account_id ORDER BY created_at ANCHOR DAILY '00:00')");
    }

    /**
     * Drives one refresh pass at a time until the named view has a repair parked on it whose
     * capture has already frozen a boundary - and therefore opened its {@code d.<id>.tmp} data
     * segment. Fails if no park ever holds one, which would leave every assertion about a
     * staged segment vacuous.
     *
     * @return the parked session, holding a capture with a staged segment
     */
    private LiveViewCheckpointRepairSession driveUntilStagedPark(LiveViewRefreshJob job, String viewName) {
        for (int pass = 0; pass < REFRESH_QUIESCENCE_PASSES; pass++) {
            setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
            drainWalQueue();
            job.processNotificationsForTest();
            drainWalQueue();
            final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance(viewName);
            Assert.assertNotNull("live view '" + viewName + "' must be registered", instance);
            final LiveViewCheckpointRepairSession parked = instance.getSuspendedRepair();
            if (parked != null && parked.getCapturedBoundaries() > 0) {
                return parked;
            }
        }
        Assert.fail("the repair on '" + viewName + "' never parked holding a staged segment");
        return null;
    }

    /**
     * The staged park {@link #testAParkWhoseSuspendFailsUnlinksItsStagedSegment} reaches: six
     * sealed boundaries above a correction low in a closed segment, replayed under a one-row
     * budget, so the repair parks with its capture holding a frozen boundary.
     */
    private void parkAStagedCapture(LiveViewRefreshJob job) throws Exception {
        createView(row(2, 1, "acct-1"));
        driveRefreshToQuiescence(job);
        for (int hour = 2; hour <= 8; hour++) {
            commit(row(2, hour, "acct-1"), job);
        }
        for (int hour = 1; hour <= 2; hour++) {
            commit(row(3, hour, "acct-1"), job);
        }
        commit(row(5, 1, "acct-1"), job);
        execute("INSERT INTO tx VALUES " + row(2, 3, "acct-1"));
        drainWalQueue();
        driveUntilStagedPark(job, "lv");
    }

    /**
     * The from-base oracle: the same accumulators partitioned by account and anchor day.
     */
    private String recompute() {
        final String bucket = "timestamp_floor('1d', created_at, '1970-01-01T00:00:00.000000Z'::timestamp)";
        return "select created_at, account_id, "
                + "sum(amount) over (partition by account_id, bucket order by created_at "
                + "rows between unbounded preceding and current row) as cumulative_sum, "
                + "count(account_id) over (partition by account_id, bucket order by created_at "
                + "rows between unbounded preceding and current row) as cumulative_count "
                + "from (select created_at, account_id, amount, " + bucket + " as bucket from tx)";
    }

    /**
     * One row of {@code account} at {@code hour} on 2026-01-{@code day}, as an INSERT tuple.
     * The day is what carries the case: with a daily anchor it is also the segment.
     */
    private String row(int day, int hour, String account) {
        return "('2026-01-" + String.format("%02d", day) + "T" + String.format("%02d", hour)
                + ":00:00.000000Z', '" + account + "', 1.0)";
    }

    /**
     * Runs one refresh pass of {@code job} on a thread of its own and waits for it to finish,
     * so the pass carries a worker identity - and a carrier thread - that is not the test's.
     * <p>
     * The join is what keeps the case deterministic: the two workers never overlap, so the
     * interleaving under test is fixed by the order of these calls rather than by timing. It
     * is a safety bound, not the coordination - the assertion is that the thread finished, and
     * a pass that hangs fails here naming the worker instead of timing the whole class out.
     *
     * @return what the pass reported through {@code Job.run()}
     */
    private boolean runOnePassOnItsOwnThread(LiveViewRefreshJob job, String threadName) throws Exception {
        final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
        final AtomicBoolean didWork = new AtomicBoolean();
        final Thread worker = new Thread(() -> {
            try {
                didWork.set(job.processNotificationsForTest());
            } catch (Throwable t) {
                errors.add(t);
            } finally {
                Path.clearThreadLocals();
            }
        }, threadName);
        worker.start();
        worker.join(60_000);
        Assert.assertFalse("refresh pass on " + threadName + " did not finish", worker.isAlive());
        if (!errors.isEmpty()) {
            throw new RuntimeException("refresh pass on " + threadName + " failed", errors.peek());
        }
        return didWork.get();
    }

    /**
     * Advances the clock and runs exactly one refresh pass, so a caller can read the state one
     * turn left behind rather than the state the turns after it converged on.
     */
    private void runOneRefreshPass(LiveViewRefreshJob job) {
        setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
        drainWalQueue();
        job.processNotificationsForTest();
        drainWalQueue();
    }

    /**
     * Two accounts on each of 2026-01-02, 2026-01-03 and 2026-01-04, four rows deep for the
     * first of them. The depth is deliberate: a one-row replay budget only spreads a
     * segment repair across turns if the segment holds more than one row to replay.
     */
    private String seedThreeDays() {
        final StringBuilder rows = new StringBuilder();
        for (int day = 2; day <= 4; day++) {
            for (int hour = 1; hour <= 4; hour++) {
                if (rows.length() > 0) {
                    rows.append(", ");
                }
                rows.append(row(day, hour, "acct-1"));
            }
            rows.append(", ").append(row(day, 1, "acct-2"));
        }
        return rows.toString();
    }

    /**
     * Temporary data segments under the named view's checkpoint directory. A capture stages one
     * as {@code d.<id>.tmp} and its close() unlinks it when no splice published it, so at
     * quiescence - no repair in flight - every file this counts is an orphan.
     * <p>
     * A directory that cannot be listed counts as zero, exactly as it does in
     * {@code LiveViewCheckpointStatePageElisionTest.dataDirBytes()} over the same directory:
     * the caller asks whether this view stranded a staged segment, and a data directory the
     * view never created holds no staged segment to strand. So the reading says only that
     * nothing is stranded - never that anything was staged. A caller that needs the other half
     * asserts a non-zero reading first, which
     * testAParkWhoseSuspendFailsUnlinksItsStagedSegment does before it fails its park.
     */
    private int stagedTmpSegmentCount(LiveViewInstance instance) {
        int count = 0;
        try (
                Path checkpointsDir = new Path().of(engine.getConfiguration().getDbRoot())
                        .concat(instance.getLiveViewToken())
                        .concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
                Path dataDir = new Path()
        ) {
            LiveViewCheckpointLayout.dataDirPath(dataDir, checkpointsDir);
            final File[] files = new File(dataDir.toString()).listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.getName().endsWith(LiveViewCheckpointLayout.TMP_SUFFIX)) {
                        count++;
                    }
                }
            }
        }
        return count;
    }

    private String viewContents() throws Exception {
        final StringSink out = new StringSink();
        printSql("lv order by 2, 1", out);
        return out.toString();
    }

    private LiveViewInstance viewInstance() {
        return viewInstance("lv");
    }

    private LiveViewInstance viewInstance(String viewName) {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance(viewName);
        Assert.assertNotNull("live view '" + viewName + "' must be registered", instance);
        return instance;
    }

    /**
     * Stands in for the {@link OutOfMemoryError} a real key-set growth raises. The case is about
     * suspend() unwinding without having taken anything, which holds for any throwable the copy
     * can raise.
     */
    private static final class InjectedAllocationFailure extends RuntimeException {
        private InjectedAllocationFailure() {
            super("injected allocation failure");
        }
    }

    /**
     * The read side of {@link ThrowingGroupKeySet}: it fails the walk {@code copyFrom} makes over
     * the SOURCE set rather than the add it makes into the destination. Same fault - an
     * allocation failure inside the copy - reached from the one side a test can poison before the
     * first park exists.
     * <p>
     * {@code size()} reports one key while armed rather than the set's own count, because the
     * detector empties the set once per repair and again at every timestamp group it closes, so
     * no real seed survives to the park and {@code copyFrom} would walk nothing. Reporting one
     * entry is what carries the walk into {@code get()}. Both behaviours are one-shot, so the
     * recovery that follows the fault runs against an ordinary set.
     */
    private static final class ReadThrowingGroupKeySet extends GroupKeySet {
        private boolean hasFired;
        private boolean isArmed = true;

        @Override
        public int get(int index) {
            if (isArmed) {
                isArmed = false;
                hasFired = true;
                throw new InjectedAllocationFailure();
            }
            return super.get(index);
        }

        @Override
        public int size() {
            return isArmed ? 1 : super.size();
        }

        private boolean hasFired() {
            return hasFired;
        }
    }

    /**
     * Stops one thread, once, in the exists check a DROP's timeline retire runs on the dropped
     * view's timeline file - after the drop's refresh fence and before it unregisters and frees
     * the view - until the case lets it go.
     */
    private static final class RetirePausingFilesFacade extends TestFilesFacadeImpl {
        private final CountDownLatch paused = new CountDownLatch(1);
        private final CountDownLatch resumed = new CountDownLatch(1);
        private volatile boolean hasPaused;
        private volatile String pausePath;
        private volatile Thread pauseThread;

        @Override
        public boolean exists(LPSZ path) {
            if (Thread.currentThread() == pauseThread && Utf8s.equalsAscii(pausePath, path)) {
                pauseThread = null;
                hasPaused = true;
                paused.countDown();
                try {
                    if (!resumed.await(60, TimeUnit.SECONDS)) {
                        throw new AssertionError("the paused drop was never resumed");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError("interrupted while the drop was paused", e);
                }
            }
            return super.exists(path);
        }

        /**
         * Lets every waiter go, the paused drop and anyone waiting for it to pause, whether it
         * paused or not. For a finally block: nothing the case set up may stay parked.
         */
        private void abandon() {
            pauseThread = null;
            paused.countDown();
            resumed.countDown();
        }

        private void arm(Thread thread, String path) {
            pausePath = path;
            pauseThread = thread;
        }

        private boolean awaitPaused() {
            try {
                return paused.await(60, TimeUnit.SECONDS) && hasPaused;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }

        private void release() {
            pauseThread = null;
            resumed.countDown();
        }
    }

    /**
     * Stops one thread, once, in the mkdirs a timeline writer runs on a view's checkpoint metadata
     * directory at the start of a seal - after the seal has bound the scratch it freezes into and
     * before the freeze reads it - until the case lets it go.
     */
    private static final class SealPausingFilesFacade extends TestFilesFacadeImpl {
        private final CountDownLatch paused = new CountDownLatch(1);
        private final CountDownLatch resumed = new CountDownLatch(1);
        private volatile boolean hasPaused;
        private volatile String pausePath;
        private volatile Thread pauseThread;

        @Override
        public int mkdirs(Path path, int mode) {
            if (Thread.currentThread() == pauseThread && path.toString().equals(pausePath)) {
                pauseThread = null;
                hasPaused = true;
                paused.countDown();
                try {
                    if (!resumed.await(60, TimeUnit.SECONDS)) {
                        throw new AssertionError("the paused seal was never resumed");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError("interrupted while the seal was paused", e);
                }
            }
            return super.mkdirs(path, mode);
        }

        /**
         * Lets every waiter go, the paused seal and anyone waiting for it to pause, whether it
         * paused or not. For a finally block: nothing the case set up may stay parked.
         */
        private void abandon() {
            pauseThread = null;
            paused.countDown();
            resumed.countDown();
        }

        private void arm(Thread thread, String path) {
            pausePath = path;
            pauseThread = thread;
        }

        private boolean awaitPaused() {
            try {
                return paused.await(60, TimeUnit.SECONDS) && hasPaused;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }

        private boolean hasPaused() {
            return hasPaused;
        }

        private void release() {
            pauseThread = null;
            resumed.countDown();
        }
    }

    private static final class ThrowingGroupKeySet extends GroupKeySet {
        private boolean hasFired;
        private boolean isArmed = true;

        private ThrowingGroupKeySet() {
        }

        /**
         * One that carries {@code seedKey} into the copy that reads it before it fails the copy
         * that writes it. The seed goes in through {@code super.add} with the injection off, so
         * the set is a real one-key set to every reader.
         */
        private ThrowingGroupKeySet(int seedKey) {
            isArmed = false;
            super.add(seedKey);
            isArmed = true;
        }

        @Override
        public boolean add(int key) {
            if (isArmed) {
                hasFired = true;
                throw new InjectedAllocationFailure();
            }
            return super.add(key);
        }

        private boolean hasFired() {
            return hasFired;
        }
    }
}
