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
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.lv.LiveViewCheckpointGenerationPin;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointLifecycle;
import io.questdb.cairo.lv.LiveViewCheckpointMetaStore;
import io.questdb.cairo.lv.LiveViewCheckpointRecoveryPhase;
import io.questdb.cairo.lv.LiveViewCheckpointRowPositionDeltaReader;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineReader;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineStoreWriter;
import io.questdb.cairo.lv.LiveViewCompiledPlan;
import io.questdb.cairo.lv.LiveViewInMemoryTier;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.lv.LiveViewState;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.QueryProgress;
import io.questdb.griffin.engine.lv.LiveViewRecordCursor;
import io.questdb.griffin.engine.lv.LiveViewRecordCursorFactory;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.mp.Job;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;

import java.io.File;
import java.lang.reflect.Field;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Shared driver helpers for the live view tests. Every test in this package advances a live view by
 * hand-driving a {@link LiveViewRefreshJob} rather than running a worker pool, so each of them needs
 * the same three loops. They used to be copy-pasted into ten test classes, where the seed bound
 * had drifted to three different values (500 / 1000 / 2000).
 * <p>
 * The important part is not the dedup: it is that {@link #driveRefreshToQuiescence} and
 * {@link #driveSeedToCompletion} now <b>fail</b> when they exhaust their pass budget. The
 * copy-pasted versions fell out of the loop silently and let the test carry on against a view that
 * had never converged, so a refresh that stalled surfaced hundreds of lines later as an unexplained
 * row-content mismatch - or, if the stalled view happened to hold the right rows already, as a pass.
 */
public abstract class AbstractLiveViewTest extends AbstractCairoTest {

    // Longer than the 100ms FLUSH EVERY the tests declare, so one pass always crosses a flush
    // deadline rather than leaving the view waiting on the clock.
    protected static final long CLOCK_ADVANCE_MICROS = 250_000;
    protected static final int REFRESH_QUIESCENCE_PASSES = 512;
    // The refresh-retry backoff LiveViewRefreshJob documents: after a faulting turn no worker
    // re-drives the view for the first wait, each consecutive faulting turn doubles it, and no wait
    // exceeds the cap. Mirrored here, rather than read from the job, so a change to the schedule
    // shows up as a failing expectation instead of passing silently.
    protected static final long REFRESH_RETRY_BACKOFF_BASE_MICROS = 100_000;
    protected static final long REFRESH_RETRY_BACKOFF_MAX_MICROS = 5_000_000;
    // How long a paced writer waits for the refresh driver's next tick before giving up on
    // it. Generous: it bounds a stalled driver, it does not pace anything itself.
    protected static final long REFRESH_TICK_WAIT_NANOS = 30_000_000_000L;
    // Bounds drainJobThroughRetryBackoff. A streak ends within this many waits under any budget the
    // tests set: the default count budget allows 5 faulting turns, and the 60s default duration
    // budget about 18 at the capped wait.
    protected static final int RETRY_BACKOFF_WAITS = 64;
    // Enough passes for the slowest seed in the suite (the parquet base test, which used 2000
    // when the helper was still copy-pasted). The loop exits as soon as the view leaves the
    // SEEDING state, so a generous bound costs nothing on the tests that converge quickly.
    protected static final int SEED_COMPLETION_PASSES = 2000;
    // What assertTierReadConsistent compared the routed read with.
    protected static final int TIER_READ_RECOMPUTE = 1;
    protected static final int TIER_READ_SKIPPED = 0;
    protected static final int TIER_READ_TABLE_ONLY = 2;


    /**
     * Appends one {@code (ts, x)} row through a WalWriter without committing, so the caller
     * decides the commit mode. A plain INSERT cannot produce a REPLACE_RANGE commit; only the
     * WalWriter API can, so any test that needs one builds its rows this way. Assumes the base
     * table's second column is a LONG named x.
     */
    protected static void appendRow(WalWriter walWriter, long ts, long x) {
        TableWriter.Row row = walWriter.newRow(ts);
        row.putLong(1, x);
        row.append();
    }

    /**
     * Waits for the refresh driver to complete one more tick than it had when called.
     * Bounded: a driver that has stopped ticking must fail the run loudly rather than
     * silently drop the paced writer's intended refresh/writer overlap. The caller runs on
     * a writer thread whose try/catch routes any throw into the shared errors queue, so a
     * timeout surfaces as a test failure instead of a vacuous pass (or a hang).
     */
    protected static void awaitRefreshTick(AtomicLong refreshTicks, ConcurrentLinkedQueue<Throwable> errors) {
        final long seen = refreshTicks.get();
        final long deadlineNanos = System.nanoTime() + REFRESH_TICK_WAIT_NANOS;
        while (refreshTicks.get() == seen && errors.isEmpty()) {
            if (System.nanoTime() >= deadlineNanos) {
                throw new AssertionError("refresh driver did not tick within "
                        + (REFRESH_TICK_WAIT_NANOS / 1_000_000_000L)
                        + "s; the paced writer lost its intended refresh/writer overlap");
            }
            Os.pause();
        }
    }

    /**
     * Runs the job until it reports no more work, up to a bounded burst. Unlike the two drive*
     * helpers this is not an await - the caller loops over it - so exhausting the bound is not a
     * failure.
     * <p>
     * It leaves the clock alone, so a view backing off after a faulting turn stays put: a caller
     * that means to drive such a view on uses {@link #drainJobThroughRetryBackoff}.
     */
    protected static boolean drainJob(Job job) {
        boolean any = false;
        for (int i = 0; i < 64 && job.run(); i++) {
            any = true;
        }
        return any;
    }

    /**
     * {@link #drainJob} that does not stop at a refresh-retry backoff: whenever the burst ends with
     * a view waiting out the backoff a faulting turn armed, it moves the simulated clock to that
     * view's deadline and drains again. So a fault that clears ends with the view refreshed, and one
     * that does not ends with the view invalidated once a flush-retry budget runs out, as a worker
     * would leave it; nothing else moves the clock. Fails if the waits do not end within
     * {@link #RETRY_BACKOFF_WAITS}.
     */
    protected static boolean drainJobThroughRetryBackoff(Job job) {
        boolean any = drainJob(job);
        for (int i = 0; i < RETRY_BACKOFF_WAITS; i++) {
            final long retryUs = nextRefreshRetryMicros();
            if (retryUs == Numbers.LONG_NULL) {
                return any;
            }
            setCurrentMicros(retryUs);
            any |= drainJob(job);
        }
        Assert.fail("live view refresh still backing off after " + RETRY_BACKOFF_WAITS + " waits");
        return any;
    }

    /**
     * The earliest refresh-retry deadline still ahead of the engine's clock among the registered
     * views that can refresh, or {@link Numbers#LONG_NULL} when none is backing off after a
     * faulting turn. A deadline the clock already passed is not a wait: the view is due, and
     * whether the job drives it is the job's answer.
     */
    protected static long nextRefreshRetryMicros() {
        final ObjList<LiveViewInstance> views = new ObjList<>();
        engine.getLiveViewRegistry().getViews(views);
        final long nowUs = engine.getConfiguration().getMicrosecondClock().getTicks();
        long nextUs = Numbers.LONG_NULL;
        for (int i = 0, n = views.size(); i < n; i++) {
            final LiveViewInstance instance = views.getQuick(i);
            if (instance.isStub() || instance.isDropped() || instance.isInvalid()) {
                continue;
            }
            final long retryUs = instance.getRefreshRetryNotBeforeUs();
            if (retryUs != Numbers.LONG_NULL && retryUs > nowUs && (nextUs == Numbers.LONG_NULL || retryUs < nextUs)) {
                nextUs = retryUs;
            }
        }
        return nextUs;
    }

    /**
     * How long after the first faulting turn of a streak its {@code turn}-th (counted from 1) runs,
     * when every turn runs at the deadline the one before it armed.
     */
    protected static long refreshRetryStreakMicros(int turn) {
        long offsetUs = 0;
        for (int i = 0; i < turn - 1; i++) {
            offsetUs += Math.min(REFRESH_RETRY_BACKOFF_MAX_MICROS, REFRESH_RETRY_BACKOFF_BASE_MICROS << Math.min(i, 32));
        }
        return offsetUs;
    }

    /**
     * How many faulting turns a streak charged to the duration budget alone runs, each at the
     * deadline the one before it armed, before the budget of {@code maxDurationMicros} runs out:
     * the turn that finds the budget spent is the last.
     */
    protected static int refreshRetryTurnsUntilDurationExhausts(long maxDurationMicros) {
        int turn = 1;
        while (refreshRetryStreakMicros(turn) < maxDurationMicros) {
            turn++;
        }
        return turn;
    }

    /**
     * Confirms {@code SELECT * FROM viewName} routes through the in-memory tier (Mode B), not
     * disk-only: opens the inner {@link LiveViewRecordCursor}, drains it, and asserts the fence
     * engaged and the tier served rows. Single-threaded, after quiescence.
     */
    protected static void assertModeBEngaged(String viewName) throws SqlException {
        try (RecordCursorFactory factory = select("SELECT * FROM " + viewName)) {
            final RecordCursorFactory lvFactory = liveViewFactoryOf(factory);
            try (LiveViewRecordCursor cursor = (LiveViewRecordCursor) lvFactory.getCursor(sqlExecutionContext)) {
                println(lvFactory.getMetadata(), cursor, new StringSink());
                Assert.assertTrue("read-back must route through Mode B", cursor.isRoutingEligible());
                Assert.assertTrue("Mode B must serve in-mem rows", cursor.inMemRowsServed() > 0);
            }
        }
    }

    /**
     * Runs {@code sql} with the tier on (Mode B) and again with the fence forced off
     * (disk-only, see {@link #printDiskOnly}), and asserts the two outputs are byte-identical.
     * Mirrors the differential oracle in {@code LiveViewInMemReadTest}; single-threaded only.
     */
    protected static void assertModeBMatchesDiskOnly(String viewName, String sql) throws SqlException {
        final StringSink modeB = new StringSink();
        printSql(sql, modeB);
        final StringSink diskOnly = new StringSink();
        printDiskOnly(viewName, sql, diskOnly);
        Assert.assertEquals("Mode B vs disk-only mismatch for: " + sql, diskOnly.toString(), modeB.toString());
    }

    /**
     * Asserts the view's SYMBOL column {@code keyColumn} names each row by one label only. A
     * filter on the symbol resolves a label to an id through the view's own symbol table, a
     * filter on its text compares labels, and a grouped count keys on the id, so all three
     * agree unless the table lists one label under two ids, or an id the rows carry under no
     * label. Needs no recompute, so it holds at any point a reader can open the view, an
     * un-flushed lead or a recovery's discarded ids included.
     *
     * @param keys every label the run's rows carry, NULL aside
     */
    /**
     * Asserts that {@code instance}'s restart recovery restored its window state from a published
     * timeline root, and that nothing rebuilt or reset on the way there. Static so a test that
     * runs a real server, and so reaches the instance through that server's engine, asserts the
     * same witness.
     * <p>
     * {@code isCheckpointRestoreSucceeded()} alone cannot carry this assertion. Both restart routes
     * report it: {@code tryRestoreFromTimeline} catches every {@link Throwable} and falls through to
     * the applied-base rebuild, which recomputes the whole window, produces correct rows, faults no
     * refresh cycle and fails no seal. A recompute oracle, the fault count and the seal-failure
     * count are all green over that fallback, so a test that asserts only the flag proves the view
     * has state - not that the roots it published are the state it came back on.
     */
    protected static void assertRestoredFromTimeline(String viewName, LiveViewInstance instance) {
        assertRestoreRoute(viewName, LiveViewCheckpointRecoveryPhase.ROUTE_TIMELINE_RESTORE, instance);
        Assert.assertEquals(
                "live view '" + viewName + "' restored, but only after rebuilding from the applied base first",
                0L,
                instance.getCheckpointRebuildAttempts()
        );
        Assert.assertEquals(
                "live view '" + viewName + "' restored, but retired the timeline it restored from;"
                        + " the roots the previous process published are gone",
                0L,
                instance.getCheckpointTimelineResets()
        );
        Assert.assertTrue(
                "live view '" + viewName + "' restored without naming the root it came back on",
                instance.getCheckpointRestoreCheckpointId() != Numbers.LONG_NULL
                        && instance.getCheckpointRestoreGeneration() != Numbers.LONG_NULL
        );
    }

    protected static void assertSymbolLabelsConsistent(String viewName, String keyColumn, ObjList<String> keys) throws SqlException {
        final StringSink bySymbol = new StringSink();
        final StringSink byText = new StringSink();
        for (int i = 0, n = keys.size(); i < n; i++) {
            final String key = keys.getQuick(i);
            bySymbol.clear();
            byText.clear();
            printSql("SELECT count() FROM " + viewName + " WHERE " + keyColumn + " = '" + key + "'", bySymbol);
            printSql("SELECT count() FROM " + viewName + " WHERE " + keyColumn + "::varchar = '" + key + "'", byText);
            TestUtils.assertEquals("view '" + viewName + "' filters key '" + key + "' by id unlike by label", byText, bySymbol);
        }
        bySymbol.clear();
        byText.clear();
        printSql("SELECT count() FROM " + viewName + " WHERE " + keyColumn + " IS NULL", bySymbol);
        printSql("SELECT count() FROM " + viewName + " WHERE " + keyColumn + "::varchar IS NULL", byText);
        TestUtils.assertEquals("view '" + viewName + "' filters the NULL key by id unlike by label", byText, bySymbol);
        bySymbol.clear();
        byText.clear();
        printSql("SELECT " + keyColumn + ", count() FROM " + viewName + " ORDER BY 1", bySymbol);
        printSql("SELECT " + keyColumn + "::varchar " + keyColumn + ", count() FROM " + viewName + " ORDER BY 1", byText);
        TestUtils.assertEquals("view '" + viewName + "' groups its key by id unlike by label", byText, bySymbol);
    }

    /**
     * Asserts what a read through the view's in-memory tier returns, at any point of a run: an
     * un-flushed lead resident, a repair parked, a view WAL block committed but not applied.
     * Returns which of the checks below ran.
     * <ul>
     *     <li>While the view's WAL holds a block its table has not applied, reads stay on the
     *     table as it stands, so the routed read must equal the forced disk-only read
     *     ({@link #TIER_READ_TABLE_ONLY}). A read that served a lead on top of the table
     *     without the pending block's rows equals neither that nor the recompute.</li>
     *     <li>Once the view has refreshed every commit the base applied, with no repair parked
     *     and no retry pending, the routed read - the table plus any un-flushed lead - must
     *     already equal the recompute ({@link #TIER_READ_RECOMPUTE}).</li>
     *     <li>Otherwise the view stands at a base commit the test cannot recompute, and nothing
     *     is checked ({@link #TIER_READ_SKIPPED}). So is a view with no tier, whose un-flushed
     *     rows no reader sees.</li>
     * </ul>
     *
     * @param orderBy an ORDER BY list naming every output column, so tied rows sort alike
     */
    protected static int assertTierReadConsistent(
            String viewName,
            String baseName,
            String oracleSql,
            String orderBy
    ) throws SqlException {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance(viewName);
        Assert.assertNotNull("live view '" + viewName + "' is not registered", instance);
        if (instance.getInMemoryTier() == null || instance.isInvalid()) {
            return TIER_READ_SKIPPED;
        }
        final String viewRead = "(" + viewName + ") ORDER BY " + orderBy;
        final TableToken viewToken = instance.getLiveViewToken();
        final long viewWalTxn = engine.getTableSequencerAPI().lastTxn(viewToken);
        final long viewAppliedTxn = engine.getTableSequencerAPI().getTxnTracker(viewToken).getWriterTxn();
        if (viewWalTxn > viewAppliedTxn) {
            final StringSink routed = new StringSink();
            printSql(viewRead, routed);
            final StringSink tableOnly = new StringSink();
            printDiskOnly(viewName, viewRead, tableOnly);
            TestUtils.assertEquals(
                    "view '" + viewName + "' served a read past a WAL block its table has not applied [viewWalTxn="
                            + viewWalTxn + ", viewAppliedTxn=" + viewAppliedTxn + ']',
                    tableOnly,
                    routed
            );
            return TIER_READ_TABLE_ONLY;
        }
        final long baseSeqTxn;
        try (TableReader reader = getReader(baseName)) {
            baseSeqTxn = reader.getSeqTxn();
        }
        if (instance.getRefreshReachedSeqTxn() != baseSeqTxn
                || instance.getSuspendedRepair() != null
                || instance.getRefreshRetryNotBeforeUs() != Numbers.LONG_NULL) {
            return TIER_READ_SKIPPED;
        }
        try {
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "(" + oracleSql + ") ORDER BY " + orderBy, viewRead, LOG, true);
        } catch (AssertionError e) {
            throw new AssertionError("view '" + viewName + "' read through its tier at base seqTxn " + baseSeqTxn
                    + " [leadRows=" + instance.getLeadRowCount()
                    + ", lastProcessedSeqTxn=" + instance.getLastProcessedSeqTxn() + "]: " + e.getMessage(), e);
        }
        return TIER_READ_RECOMPUTE;
    }

    /**
     * Asserts a keyed {@code WINDOW JOIN} from the view onto its base returns what the same
     * join from a plain table copy of the view returns, on the sync and the parallel plan,
     * each with the JIT filter on and off. The join maps each base key to the view's key for
     * it through the view's symbol table, so a table that lists a label under an id no row
     * carries leaves rows without a match - NULL aggregates - where the copy, with a symbol
     * table of its own, matches them.
     */
    protected static void assertViewWindowJoinMatchesCopy(
            String viewName,
            String baseName,
            String keyColumn,
            String baseValueColumn,
            String filter
    ) throws SqlException {
        final String copyName = viewName + "_wj_copy";
        execute("DROP TABLE IF EXISTS " + copyName);
        execute("CREATE TABLE " + copyName + " AS (SELECT * FROM " + viewName + ") TIMESTAMP(ts) PARTITION BY DAY");
        final boolean wasParallel = sqlExecutionContext.isParallelWindowJoinEnabled();
        final int jitMode = sqlExecutionContext.getJitMode();
        try {
            for (int mode = 0; mode < 4; mode++) {
                sqlExecutionContext.setParallelWindowJoinEnabled((mode & 1) != 0);
                sqlExecutionContext.setJitMode((mode & 2) != 0 ? SqlJitMode.JIT_MODE_DISABLED : SqlJitMode.JIT_MODE_ENABLED);
                try {
                    TestUtils.assertSqlCursors(
                            engine,
                            sqlExecutionContext,
                            windowJoinSql(copyName, baseName, keyColumn, baseValueColumn, filter),
                            windowJoinSql(viewName, baseName, keyColumn, baseValueColumn, filter),
                            LOG,
                            true
                    );
                } catch (AssertionError e) {
                    throw new AssertionError("WINDOW JOIN from view '" + viewName + "' differs from its copy [parallel="
                            + ((mode & 1) != 0) + ", jit=" + ((mode & 2) == 0) + "]: " + e.getMessage(), e);
                }
            }
        } finally {
            sqlExecutionContext.setParallelWindowJoinEnabled(wasParallel);
            sqlExecutionContext.setJitMode(jitMode);
            execute("DROP TABLE " + copyName);
        }
    }

    /**
     * Prints {@code sql} with the seqTxn fence forced off - both slot stamps moved to a value
     * the disk reader never reports - so the view's reads fall back to the disk-only path and
     * serve its table as it stands. Restores the stamps afterwards; single-threaded only.
     */
    protected static void printDiskOnly(String viewName, String sql, StringSink sink) throws SqlException {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance(viewName);
        Assert.assertNotNull("live view '" + viewName + "' is not registered", instance);
        final LiveViewInMemoryTier tier = instance.getInMemoryTier();
        Assert.assertNotNull("live view '" + viewName + "' has no in-memory tier", tier);
        final long s0 = tier.getSlot(0).lvSeqTxn();
        final long s1 = tier.getSlot(1).lvSeqTxn();
        tier.getSlot(0).setLvSeqTxn(fenceMismatch(s0));
        tier.getSlot(1).setLvSeqTxn(fenceMismatch(s1));
        try {
            printSql(sql, sink);
        } finally {
            tier.getSlot(0).setLvSeqTxn(s0);
            tier.getSlot(1).setLvSeqTxn(s1);
        }
    }

    /**
     * Waits until {@code instance} has been marked dropped, then returns. This is the deterministic
     * fence a concurrent {@code engine.dropLiveView} crosses: {@code markDroppedAndAwaitCheckpoint}
     * publishes {@code dropped = true} (a volatile) BEFORE it commits to its blocking branch - the
     * {@code waitForUnfrozen} park under a checkpoint freeze, or the {@code fenceRefresh} spin behind
     * a held refresh latch. Keying off the view's own published drop state - rather than probing the
     * dropper thread's stack for a private method name - makes the fence immune to method renames,
     * JIT frame materialization and scheduling; the {@code deadlineNanos} guard is only a safety net,
     * never the load-bearing timing. Once this returns the dropper cannot complete the drop until the
     * blocker (freeze / latch) is released, so callers may assert the drop is still in progress.
     */
    protected static void awaitDropped(LiveViewInstance instance, long timeoutMs) {
        final long deadlineNanos = System.nanoTime() + timeoutMs * 1_000_000L;
        while (!instance.isDropped()) {
            if (System.nanoTime() >= deadlineNanos) {
                Assert.fail("live view was never marked dropped within " + timeoutMs + "ms");
            }
            Os.pause();
        }
    }

    /**
     * The refresh job's private checkpoint timeline writer, for the tests that read what a seal
     * or a repair left behind in it.
     */
    protected static LiveViewCheckpointTimelineStoreWriter checkpointTimelineStoreWriter(
            LiveViewRefreshJob job
    ) throws ReflectiveOperationException {
        final Field field = LiveViewRefreshJob.class.getDeclaredField("checkpointTimelineStoreWriter");
        field.setAccessible(true);
        final LiveViewCheckpointTimelineStoreWriter writer = (LiveViewCheckpointTimelineStoreWriter) field.get(job);
        Assert.assertNotNull("the job must have sealed through its timeline writer", writer);
        return writer;
    }

    /**
     * The test hooks installed on {@code job}, installing them first if the job runs none.
     */
    protected static LiveViewTestRefreshHooks hooks(LiveViewRefreshJob job) {
        return LiveViewTestRefreshHooks.install(job);
    }

    /**
     * The LiveViewRecordCursorFactory under any wrapper the plan put on top of it.
     */
    protected static RecordCursorFactory liveViewFactoryOf(RecordCursorFactory factory) {
        RecordCursorFactory f = factory;
        while (f != null && !(f instanceof LiveViewRecordCursorFactory)) {
            f = f.getBaseFactory();
        }
        Assert.assertNotNull("expected a LiveViewRecordCursorFactory in the plan", f);
        return f;
    }

    /**
     * Parses a microsecond timestamp literal, for the tests that hand raw timestamps to a
     * WalWriter or compare against a live view's persisted START FROM boundary.
     */
    protected static long ts(String timestamp) {
        return MicrosTimestampDriver.floor(timestamp);
    }

    /**
     * Returns the view's window function list, which is how a test reaches a non-anchored
     * window - {@code LiveViewInstance.getAnchorWindow()} does not surface one.
     * <p>
     * Reads the decomposed plan rather than walking the factory tree here. A SELECT that
     * wraps its window function in an expression puts a projection above the window
     * factory, so a hand-rolled walk that peels only {@code QueryProgress} stops one node
     * short and reports the view has no window at all.
     */
    protected static ObjList<WindowFunction> unwrapWindowFunctions(LiveViewInstance instance) {
        final LiveViewCompiledPlan plan = instance.getCompiledPlan();
        if (plan == null) {
            throw new IllegalStateException("live view has no compiled plan; refresh it first");
        }
        return plan.getWindowFactory().getWindowFunctions();
    }

    /**
     * Whether the engine's {@code MillisecondClock} follows the simulated microsecond clock. Probed
     * rather than declared: moves {@code currentMicros} to a stamp no wall clock reads, asks the
     * engine's own configuration for the time, then puts the clock back. A derived clock answers
     * with the stamp; a pinned real one answers with wall time.
     */
    private static void assertRestoreRoute(String viewName, int expected, LiveViewInstance instance) {
        Assert.assertEquals(
                "live view '" + viewName + "' took the wrong restart recovery route",
                LiveViewCheckpointRecoveryPhase.routeName(expected),
                LiveViewCheckpointRecoveryPhase.routeName(instance.getCheckpointRestoreRoute())
        );
    }

    // A keyed WINDOW JOIN from `left` onto the base, sorted on every output column so the
    // view and its copy compare row for row whatever order their tied rows scan in.
    private static String windowJoinSql(String left, String baseName, String keyColumn, String baseValueColumn, String filter) {
        return "SELECT * FROM (SELECT t.ts, t." + keyColumn + ", count() AS n, sum(p." + baseValueColumn + ") AS s FROM " + left + " t"
                + " WINDOW JOIN " + baseName + " p ON t." + keyColumn + " = p." + keyColumn
                + " RANGE BETWEEN 1 HOUR PRECEDING AND 1 HOUR FOLLOWING EXCLUDE PREVAILING"
                + (filter != null ? " WHERE " + filter : "")
                + ") ORDER BY 1, 2, 3, 4";
    }

    // Maps a slot stamp to a value the disk reader can never report, which forces the fence
    // off so the read serves disk-only. An unstamped slot never passes the fence, so it stays
    // unstamped: any number it took instead could be the disk reader's own seqTxn.
    private static long fenceMismatch(long seqTxn) {
        return seqTxn == Numbers.LONG_NULL ? Numbers.LONG_NULL : seqTxn + 1_000_000;
    }

    private static boolean isEngineMillisecondClockDerivedFromTestClock() {
        // 2100-01-01T00:00:00Z in micros, far enough from now that no real clock reads it.
        final long probeMicros = 4_102_444_800_000_000L;
        final long savedMicros = currentMicros;
        try {
            setCurrentMicros(probeMicros);
            return engine.getConfiguration().getMillisecondClock().getTicks() == probeMicros / 1000;
        } finally {
            setCurrentMicros(savedMicros);
        }
    }

    @Before
    @Override
    public void setUp() {
        super.setUp();
        Assert.assertEquals(
                "isMillisecondClockSimulated() disagrees with the MillisecondClock the engine actually runs on; "
                        + "override it to match, because setUp() sizes the engine's spin deadlines from this answer",
                isMillisecondClockSimulated(),
                isEngineMillisecondClockDerivedFromTestClock()
        );
        if (isMillisecondClockSimulated()) {
            // These tests hand-drive the clock, and CairoTestConfiguration derives the millisecond clock
            // the engine's spin deadlines run on from that same simulated clock. So a soak whose refresh
            // driver advances the clock by CLOCK_ADVANCE_MICROS per tick, on its own thread, fast-forwards
            // every concurrent reader's deadline with it: a reader that loses one benign race in
            // TableReader.readTxnSlow - the writer commits between its txn read and its scoreboard acquire -
            // re-checks a deadline the driver has already blown by tens of simulated seconds and throws
            // "Transaction read timeout" milliseconds after entering the loop. Raising the budget past any
            // span such a test can simulate takes the simulated clock out of the spin loops. It costs no
            // real liveness cover THERE: the clock is simulated, so the deadline never measured real time
            // in the first place, and AbstractCairoTest's 20-minute JUnit timeout rule still fails a
            // genuinely stuck reader.
            //
            // isMillisecondClockSimulated() scopes the raise, because that argument is false for a
            // subclass that pins the production wall clock instead: there the deadline measures real
            // time, so raising it would trade a 5s reader-side "Transaction read timeout" naming the
            // stalled table for the 20-minute class timeout - 240x coarser, with no reader diagnostic.
            spinLockTimeout = 365L * 24 * 60 * 60 * 1000; // a simulated year
        }
    }

    /**
     * Runs {@code test}, a test method whose body runs under the plain {@code assertMemoryLeak},
     * with {@code cairo.commit.mode} set to {@code commitMode}, then asserts that every kind of
     * checkpoint file in {@code fileKinds} (see {@link LiveViewCheckpointDiskFullFault#kindOf})
     * was synced with the flag that mode calls for, and never with the other. The test
     * configuration defaults to nosync, which skips the sync branch of every checkpoint writer.
     */
    protected void assertCheckpointFilesSyncInCommitMode(
            String commitMode,
            ObjList<String> fileKinds,
            TestUtils.LeakProneCode test
    ) throws Exception {
        final boolean isAsync = "async".equals(commitMode);
        Assert.assertTrue("not a syncing commit mode: " + commitMode, isAsync || "sync".equals(commitMode));
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, commitMode);
        final LiveViewCheckpointDiskFullFault syncs = new LiveViewCheckpointDiskFullFault();
        ff = syncs;
        test.run();
        for (int i = 0, n = fileKinds.size(); i < n; i++) {
            final String kind = fileKinds.getQuick(i);
            Assert.assertTrue(
                    "the " + commitMode + " commit mode must sync " + kind,
                    syncs.getSyncCount(kind, isAsync) > 0
            );
            Assert.assertEquals(
                    "the " + commitMode + " commit mode must not sync " + kind + " with the other flag",
                    0,
                    syncs.getSyncCount(kind, !isAsync)
            );
        }
    }

    /**
     * Asserts the named view refreshed without a single faulting cycle.
     * <p>
     * Any test whose oracle compares the live view against a from-base recompute of its own SELECT
     * needs this. A refresh fault does not fail the cycle: {@code handleRefreshFailure} recomputes
     * the window from the applied base, calls {@code recordRefreshSuccess()} and returns, so the view
     * converges on the recompute - which is precisely what such an oracle compares it against. The
     * comparison therefore passes whether the incremental path worked or faulted on every commit and
     * rebuilt itself each time. Only the fault count can tell those apart.
     */
    protected void assertNoRefreshFaults(String viewName) {
        LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance(viewName);
        Assert.assertNotNull("live view '" + viewName + "' is not registered", instance);
        Assert.assertEquals(
                "live view '" + viewName + "' refresh cycles must not fault: a fault self-heals into a"
                        + " full recompute from the applied base, so a recompute-based oracle passes either way",
                0L,
                instance.getRefreshFaultCount()
        );
    }

    /**
     * Asserts the named view's restart recovery rebuilt its whole window from the applied base
     * rather than restoring a published root, and that the rebuild retired the timeline on its
     * way through. The mirror image of {@link #assertRestoredFromTimeline(String)}: a case that
     * means to exercise the fallback needs this, because a restore that quietly succeeded would
     * satisfy the same rows.
     */
    protected void assertRebuiltFromAppliedBase(String viewName) {
        final LiveViewInstance instance = restoreWitnessInstance(viewName);
        assertRestoreRoute(viewName, LiveViewCheckpointRecoveryPhase.ROUTE_FALLBACK_REBUILD, instance);
        Assert.assertTrue(
                "live view '" + viewName + "' took the rebuild route without starting a rebuild",
                instance.getCheckpointRebuildAttempts() > 0
        );
    }

    /**
     * Asserts the named view, carried over from an older checkpoint format, rebuilt its whole
     * window from the applied base on the upgrade route, and that the rebuild retired the older
     * directory: nothing is pending any more. Distinct from
     * {@link #assertRebuiltFromAppliedBase(String)}, whose route a view that merely lost its
     * timeline takes, with the restatement guard armed.
     */
    protected void assertUpgradeRebuilt(String viewName) {
        final LiveViewInstance instance = restoreWitnessInstance(viewName);
        assertRestoreRoute(viewName, LiveViewCheckpointRecoveryPhase.ROUTE_UPGRADE_REBUILD, instance);
        Assert.assertTrue(
                "live view '" + viewName + "' took the upgrade route without starting a rebuild",
                instance.getCheckpointRebuildAttempts() > 0
        );
        Assert.assertTrue(instance.isCheckpointRestoreSucceeded());
        Assert.assertFalse(
                "live view '" + viewName + "' rebuilt, but its older-format timeline is still pending retirement",
                instance.isCheckpointUpgradeRebuildPending()
        );
        Assert.assertFalse(instance.isCheckpointRecoveryBlocked());
        Assert.assertFalse(instance.isInvalid());
    }

    /**
     * Asserts the named view's restart recovery restored its window state from a published timeline
     * root, and that nothing rebuilt or reset on the way there. See
     * {@link #assertRestoredFromTimeline(String, LiveViewInstance)}.
     */
    protected void assertRestoredFromTimeline(String viewName) {
        assertRestoredFromTimeline(viewName, restoreWitnessInstance(viewName));
    }

    /**
     * Moves the simulated clock to where the next refresh pass should run: the earliest deadline a
     * view backing off after a faulting turn waits for, or {@link #CLOCK_ADVANCE_MICROS} on when no
     * view is backing off.
     */
    protected static void advanceClockToNextRefreshPass() {
        final long retryUs = nextRefreshRetryMicros();
        setCurrentMicros(retryUs != Numbers.LONG_NULL ? retryUs : currentMicros + CLOCK_ADVANCE_MICROS);
    }

    /**
     * Advances the clock and drives the refresh job until it makes no further progress. Fails if the
     * job is still finding work after {@link #REFRESH_QUIESCENCE_PASSES} passes, which means the view
     * never converged and any assertion the caller makes next would be reading a half-refreshed view.
     * <p>
     * A view waiting out a refresh-retry backoff is not quiescent: the pass after it runs at the
     * view's deadline ({@link #advanceClockToNextRefreshPass}), so a fault that clears converges and
     * one that does not runs a flush-retry budget out, as a worker would leave it.
     */
    protected void driveRefreshToQuiescence(LiveViewRefreshJob job) {
        for (int i = 0; i < REFRESH_QUIESCENCE_PASSES; i++) {
            advanceClockToNextRefreshPass();
            drainWalQueue();
            boolean progressed = drainJob(job);
            drainWalQueue();
            if (!progressed && nextRefreshRetryMicros() == Numbers.LONG_NULL) {
                return;
            }
        }
        Assert.fail("live view refresh did not quiesce within " + REFRESH_QUIESCENCE_PASSES
                + " passes; the refresh job is still finding work");
    }

    /**
     * Drives the refresh job until the named view leaves the SEEDING state, then drains the WAL
     * queue so the seeded rows are visible to a reader. Fails if the view is still seeding
     * after {@link #SEED_COMPLETION_PASSES} passes.
     */
    protected void driveSeedToCompletion(LiveViewRefreshJob job, String viewName) {
        boolean completed = false;
        for (int i = 0; i < SEED_COMPLETION_PASSES; i++) {
            LiveViewInstance inst = engine.getLiveViewRegistry().getViewInstance(viewName);
            if (inst == null
                    || inst.getStateReader().getSeedState() != LiveViewState.SEED_STATE_SEEDING) {
                completed = true;
                break;
            }
            // The clock stays put while the sweep runs, unless a faulting seed turn left the view
            // backing off: the sweep resumes at the view's deadline, not on a clock that never
            // reaches it.
            drainJobThroughRetryBackoff(job);
        }
        drainWalQueue();
        if (!completed) {
            Assert.fail("seed of live view '" + viewName + "' did not complete within "
                    + SEED_COMPLETION_PASSES + " passes; the view is still SEEDING");
        }
    }

    /**
     * Drives one refresh pass at a time until the named view has a repair parked on it, so a
     * caller can read the durable state a reader would see mid-repair, or what the parked
     * session itself holds before a later turn resumes or discards it. Fails if the repair
     * never parks, which would leave every assertion after it vacuous.
     */
    protected void driveUntilParked(LiveViewRefreshJob job, String viewName) {
        for (int pass = 0; pass < REFRESH_QUIESCENCE_PASSES; pass++) {
            setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
            drainWalQueue();
            job.processNotificationsForTest();
            drainWalQueue();
            final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance(viewName);
            Assert.assertNotNull("live view '" + viewName + "' must be registered", instance);
            if (instance.getSuspendedRepair() != null) {
                return;
            }
        }
        Assert.fail("the repair on '" + viewName + "' never parked on its turn budget");
    }

    /**
     * Whether the millisecond clock this test's engine reads - the clock the storage engine measures
     * every spin deadline against - is the simulated one the tests hand-drive through
     * {@code setCurrentMicros}, rather than the production wall clock.
     * <p>
     * True by default, because {@link io.questdb.test.cairo.CairoTestConfiguration} derives its
     * millisecond clock from the same microsecond clock {@code setCurrentMicros} moves. A subclass
     * that installs a real {@code MillisecondClock} of its own must override this and return false:
     * {@link #setUp} then leaves it {@link io.questdb.test.AbstractCairoTest#DEFAULT_SPIN_LOCK_TIMEOUT},
     * which on a real clock is genuine liveness cover rather than an artifact of the simulated one.
     * <p>
     * It describes the wiring, not every instant: the derived clock falls through to real wall time
     * while {@code currentMicros == -1}, so a simulated suite still reads real time until its first
     * {@code setCurrentMicros}.
     * <p>
     * {@link #setUp} asserts the answer against the engine's actual clock rather than trusting it,
     * so a subclass that pins a real clock and forgets to override this fails immediately instead of
     * silently inheriting a simulated-year {@code spinLockTimeout} - which is the defect the hook
     * exists to prevent.
     */
    protected boolean isMillisecondClockSimulated() {
        return true;
    }

    /**
     * Every logical timeline entry of {@code instance} as {@code (maxTimestamp, effective row
     * position)}, ascending - the ladder a resume reads to decide how many live-view rows the
     * root it selects stands on.
     * <p>
     * The position is the effective one, so it carries the suffix corrections later repairs
     * published into {@code LiveViewCheckpointRowPositionDelta} rather than only what the
     * entry itself stores.
     */
    protected LongList snapshotCheckpointLadder(LiveViewInstance instance) {
        final LongList ladder = new LongList();
        try (
                Path checkpointsDir = new Path().of(engine.getConfiguration().getDbRoot())
                        .concat(instance.getLiveViewToken())
                        .concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
                LiveViewCheckpointMetaStore store = new LiveViewCheckpointMetaStore(engine.getConfiguration());
                LiveViewCheckpointTimelineReader reader =
                        new LiveViewCheckpointTimelineReader(engine.getConfiguration());
                LiveViewCheckpointRowPositionDeltaReader deltaReader =
                        new LiveViewCheckpointRowPositionDeltaReader(engine.getConfiguration())
        ) {
            store.of(checkpointsDir);
            reader.of(checkpointsDir);
            deltaReader.of(checkpointsDir);
            try (LiveViewCheckpointGenerationPin pin = store.pin()) {
                reader.iterateAll(pin.getTimelineRootRef(), entry -> {
                    ladder.add(entry.maxTimestamp);
                    ladder.add(deltaReader.effectivePosition(pin.getRowPositionDeltaRootRef(), entry));
                });
            }
        }
        return ladder;
    }

    /**
     * Removes every WAL directory of the base table, which is what a restore that captured the
     * applied table and not its WAL leaves behind.
     */
    protected void removeBaseWal(String baseTableName) {
        final TableToken baseToken = engine.verifyTableName(baseTableName);
        final File baseDir = new File(engine.getConfiguration().getDbRoot(), baseToken.getDirName());
        final File[] walDirs = baseDir.listFiles(f -> f.isDirectory() && f.getName().startsWith(WalUtils.WAL_NAME_BASE));
        Assert.assertNotNull(walDirs);
        Assert.assertTrue("the base must have a WAL to lose", walDirs.length > 0);
        for (File walDir : walDirs) {
            try (Path p = new Path()) {
                p.of(walDir.getAbsolutePath());
                Assert.assertTrue("could not remove " + walDir, engine.getConfiguration().getFilesFacade().rmdir(p));
            }
        }
    }

    /**
     * Retires the checkpoint timeline the seed sweep has been sealing into, so a following restart
     * has no resume source and the sweep has to re-run from offset 0 behind its skip-write floor. A
     * no-op when the view has no timeline (nothing has been sealed yet, or the sweep completed and
     * retired it).
     */
    protected void retireSeedCheckpointTimeline(LiveViewInstance instance) {
        try (Path p = new Path()) {
            p.of(engine.getConfiguration().getDbRoot())
                    .concat(instance.getLiveViewToken())
                    .concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
            LiveViewCheckpointLifecycle.retireTimeline(engine.getConfiguration(), p, null, true);
        }
    }

    private LiveViewInstance restoreWitnessInstance(String viewName) {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance(viewName);
        Assert.assertNotNull("live view '" + viewName + "' is not registered", instance);
        Assert.assertTrue(
                "live view '" + viewName + "' has not run its restart recovery attempt yet, so it"
                        + " witnesses no route: drive refresh to quiescence first",
                instance.isCheckpointRestoreAttempted()
        );
        return instance;
    }
}
