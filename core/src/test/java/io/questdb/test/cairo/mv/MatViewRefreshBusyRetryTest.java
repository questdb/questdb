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

package io.questdb.test.cairo.mv;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.EntryUnavailableException;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.mv.MatViewState;
import io.questdb.cairo.mv.MatViewTimerJob;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Verifies that a transient "table busy" error while refreshing a materialized view does NOT
 * invalidate the view. Instead, the refresh is deferred and re-driven by {@link MatViewTimerJob}
 * once the backoff elapses, after which the view catches up. The transient error is injected two
 * ways: the base table reader pool being exhausted (getReader throws) and the view's own WAL writer
 * pool being exhausted (getWalWriter throws), the latter covering both the base-commit (dependent
 * views) and the timer-driven single-view refresh paths.
 * <p>
 * Also verifies the boundaries of the defer-instead-of-invalidate behaviour: once the consecutive
 * deferral count exceeds the configured limit the view IS invalidated (to release base-table WAL
 * retention), a successful refresh resets that counter, and a deferred intermediate view in a
 * dependent-view chain does not invalidate its grandchild.
 */
public class MatViewRefreshBusyRetryTest extends AbstractCairoTest {

    private static final AtomicBoolean failBaseReader = new AtomicBoolean(false);
    // Make the base read fail with a retriable out-of-memory error, simulating an OOM that survived
    // the in-call interval step reduction during a materialized view refresh.
    private static final AtomicBoolean failBaseReaderOom = new AtomicBoolean(false);
    // Stage 1 of the m3 scenario: make the base read fail NON-retriably so the dependent-view loop's
    // inner catch routes to refreshFailState (which marks the view invalid in-memory).
    private static final AtomicBoolean failBaseReaderNonRetriable = new AtomicBoolean(false);
    // Stage 2 of the m3 scenario: make WalWriter.resetMatViewState throw a RETRIABLE error so the
    // failure inside refreshFailState propagates to the dependent-view loop's outer catch.
    private static final AtomicBoolean failViewResetMatViewState = new AtomicBoolean(false);
    private static final AtomicBoolean failViewWalWriter = new AtomicBoolean(false);
    private static volatile String baseTableName;
    private static volatile String viewWalWriterName;

    @BeforeClass
    public static void setUpStatic() throws Exception {
        // Inject an engine whose getReader() throws EntryUnavailableException for the base table,
        // simulating reader pool exhaustion during a materialized view refresh.
        AbstractCairoTest.engineFactory = conf -> new CairoEngine(conf) {
            @Override
            public TableReader getReader(TableToken tableToken) {
                final String prefix = baseTableName;
                if (prefix != null && tableToken.getTableName().startsWith(prefix)) {
                    if (failBaseReaderNonRetriable.get()) {
                        // A NON-retriable refresh failure: the inner catch in the dependent-view loop
                        // calls refreshFailState, which marks the view invalid in-memory.
                        throw CairoException.nonCritical().put("injected non-retriable refresh failure");
                    }
                    if (failBaseReader.get()) {
                        throw EntryUnavailableException.instance("pool size exceeded");
                    }
                    if (failBaseReaderOom.get()) {
                        throw CairoException.nonCritical().put("injected out-of-memory").setOutOfMemory(true);
                    }
                }
                return super.getReader(tableToken);
            }

            // Inject a view whose getWalWriter() throws EntryUnavailableException, simulating the
            // view's own WAL writer pool being exhausted while its refresh tries to acquire a writer.
            @Override
            public WalWriter getWalWriter(TableToken tableToken) {
                final String prefix = viewWalWriterName;
                if (prefix != null && tableToken.getTableName().startsWith(prefix)) {
                    if (failViewWalWriter.get()) {
                        throw EntryUnavailableException.instance("wal writer pool size exceeded");
                    }
                    if (failViewResetMatViewState.get()) {
                        // A real WAL writer whose resetMatViewState() throws a RETRIABLE error,
                        // simulating the WAL write inside refreshFailState failing transiently AFTER
                        // refreshFail() already marked the view invalid in-memory.
                        return new WalWriter(
                                getConfiguration(),
                                tableToken,
                                getTableSequencerAPI(),
                                getDdlListener(tableToken),
                                getWalDirectoryPolicy(),
                                getWalLocker(),
                                getRecentWriteTracker(),
                                getTelemetryWal()
                        ) {
                            @Override
                            public void resetMatViewState(
                                    long lastRefreshBaseTxn,
                                    long lastRefreshTimestamp,
                                    boolean invalid,
                                    CharSequence invalidationReason,
                                    long lastPeriodHi,
                                    LongList refreshIntervals,
                                    long refreshIntervalsBaseTxn
                            ) {
                                throw EntryUnavailableException.instance("wal writer busy during resetMatViewState");
                            }
                        };
                    }
                }
                return super.getWalWriter(tableToken);
            }
        };
        AbstractCairoTest.setUpStatic();
    }

    @Before
    @Override
    public void setUp() {
        super.setUp();
        // Reset all fault-injection switches so they never leak between tests regardless of order.
        failBaseReader.set(false);
        failBaseReaderOom.set(false);
        failBaseReaderNonRetriable.set(false);
        failViewResetMatViewState.set(false);
        failViewWalWriter.set(false);
        baseTableName = null;
        viewWalWriterName = null;
    }

    @Test
    public void testDeferredRefreshWaitsForBackoffDeadline() throws Exception {
        // 1s backoff so the deferred refresh is not eligible immediately.
        setProperty(PropertyKey.CAIRO_MAT_VIEW_REFRESH_BUSY_RETRY_TIMEOUT, 1000);
        failBaseReader.set(false);
        failViewWalWriter.set(false);
        baseTableName = "base_price";
        assertMemoryLeak(() -> {
            try {
                execute(
                        "create table base_price (" +
                                "sym varchar, price double, ts timestamp" +
                                ") timestamp(ts) partition by DAY WAL;"
                );
                execute("insert into base_price values('a', 1.0, '2020-01-01T00:00:00.000000Z')");
                drainWalAndMatViewQueues();

                execute(
                        "create materialized view price_1h as (" +
                                "  select ts, avg(price) as avg_price from base_price sample by 1h" +
                                ") partition by day"
                );
                drainWalAndMatViewQueues();
                assertViewStatus("valid");
                assertViewRowCount(1);

                // Second bucket lands in the base table.
                execute("insert into base_price values('a', 2.0, '2020-01-01T01:00:00.000000Z')");
                drainWalQueue();

                // Pin the clock so the retry deadline (now + 1s) is deterministic.
                final long t0 = 1_700_000_000_000_000L;
                setCurrentMicros(t0);

                // Refresh hits "table busy" and gets deferred with a 1s backoff.
                failBaseReader.set(true);
                drainMatViewQueue(engine);
                assertNoInvalidViews();
                assertViewStatus("retrying");
                assertViewRowCount(1);

                // A single long-lived timer job, mirroring production: the RETRY task lowers its
                // watermark once and the same instance re-drives the view when it comes due.
                final MatViewTimerJob timerJob = new MatViewTimerJob(engine);

                // Clear the failure, but the backoff has NOT elapsed yet (now == t0 < t0 + 1s):
                // the watermark gate must keep the deferred refresh on hold.
                failBaseReader.set(false);
                drainMatViewTimerQueue(timerJob);
                drainWalAndMatViewQueues();
                assertViewStatus("retrying"); // backoff not elapsed: still deferred
                assertViewRowCount(1); // still behind

                // Advance past the backoff deadline; the same timer job now re-drives the refresh.
                setCurrentMicros(t0 + 1_000_000L);
                drainMatViewTimerQueue(timerJob);
                drainWalAndMatViewQueues();
                assertViewStatus("valid");
                assertViewRowCount(2);
            } finally {
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testReaderPoolExhaustionDefersRefreshInsteadOfInvalidating() throws Exception {
        // 0 backoff so the timer sweep re-drives the deferred refresh on the very next tick.
        setProperty(PropertyKey.CAIRO_MAT_VIEW_REFRESH_BUSY_RETRY_TIMEOUT, 0);
        failBaseReader.set(false);
        failViewWalWriter.set(false);
        baseTableName = "base_price";
        assertMemoryLeak(() -> {
            execute(
                    "create table base_price (" +
                            "sym varchar, price double, ts timestamp" +
                            ") timestamp(ts) partition by DAY WAL;"
            );

            // First bucket (hour 0).
            execute("insert into base_price values('a', 1.0, '2020-01-01T00:00:00.000000Z')");
            drainWalAndMatViewQueues();

            execute(
                    "create materialized view price_1h as (" +
                            "  select ts, avg(price) as avg_price from base_price sample by 1h" +
                            ") partition by day"
            );
            drainWalAndMatViewQueues();

            // Baseline: view is valid and holds exactly one bucket.
            assertViewStatus("valid");
            assertViewRowCount(1);

            // Second bucket (hour 1) lands in the base table.
            execute("insert into base_price values('a', 2.0, '2020-01-01T01:00:00.000000Z')");
            drainWalQueue();

            // Now make the base table reader unavailable and try to refresh.
            failBaseReader.set(true);
            drainMatViewQueue(engine);

            // The transient failure must NOT invalidate the view (it reports "retrying"), and the
            // view must not have caught up yet (still one bucket).
            assertNoInvalidViews();
            assertViewStatus("retrying");
            assertViewRowCount(1);

            // Clear the transient failure and let the timer job re-drive the deferred refresh.
            failBaseReader.set(false);
            drainMatViewTimerQueue(new MatViewTimerJob(engine));
            drainWalAndMatViewQueues();

            // The view stayed valid throughout and has now caught up to both buckets.
            assertViewStatus("valid");
            assertViewRowCount(2);
        });
    }

    @Test
    public void testStaggeredDeadlinesRedriveInDeadlineOrder() throws Exception {
        // Several views arm retries at DISTINCT deadlines. The timer job must re-drive each view
        // exactly when its own deadline elapses - it pops only the due entries from its retry heap,
        // in deadline order, rather than re-driving all pending retries on the first due tick.
        setProperty(PropertyKey.CAIRO_MAT_VIEW_REFRESH_BUSY_RETRY_TIMEOUT, 1000);
        failBaseReader.set(false);
        failViewWalWriter.set(false);
        baseTableName = "base_"; // prefix-match: fail readers for every base_* table
        final int views = 3;
        assertMemoryLeak(() -> {
            try {
                for (int i = 0; i < views; i++) {
                    execute(
                            "create table base_" + i + " (sym varchar, price double, ts timestamp) " +
                                    "timestamp(ts) partition by DAY WAL;"
                    );
                    execute("insert into base_" + i + " values('a', 1.0, '2020-01-01T00:00:00.000000Z')");
                }
                drainWalAndMatViewQueues();
                for (int i = 0; i < views; i++) {
                    execute(
                            "create materialized view v_" + i + " as (" +
                                    "  select ts, avg(price) as avg_price from base_" + i + " sample by 1h" +
                                    ") partition by day"
                    );
                }
                drainWalAndMatViewQueues();
                for (int i = 0; i < views; i++) {
                    assertViewStatus("v_" + i, "valid");
                    assertViewRowCount("v_" + i, 1);
                }

                // Pin the clock so every retry deadline (now + 1s) is deterministic.
                final long t0 = 1_700_000_000_000_000L;

                // Arm each view's retry at its own deadline: commit + refresh one view at a time, at
                // clocks 100ms apart, so v_i's backoff elapses at d_i = t0 + i*100ms + 1s.
                for (int i = 0; i < views; i++) {
                    failBaseReader.set(false);
                    execute("insert into base_" + i + " values('a', 2.0, '2020-01-01T01:00:00.000000Z')");
                    drainWalQueue(); // commit only base_i -> enqueues only v_i for refresh
                    setCurrentMicros(t0 + i * 100_000L);
                    failBaseReader.set(true);
                    drainMatViewQueue(engine); // v_i refresh hits "table busy" -> arms retry at d_i
                }
                assertNoInvalidViews();
                for (int i = 0; i < views; i++) {
                    assertViewStatus("v_" + i, "retrying");
                    assertViewRowCount("v_" + i, 1);
                }

                // Clear the transient failure. A single long-lived timer job mirrors production: its
                // retry heap holds all three (deadline, view) entries at once.
                failBaseReader.set(false);
                final MatViewTimerJob timerJob = new MatViewTimerJob(engine);

                // Before the soonest deadline: the heap drains the RETRY tasks but pops nothing.
                setCurrentMicros(t0 + 900_000L);
                drainMatViewTimerQueue(timerJob);
                drainWalAndMatViewQueues();
                for (int i = 0; i < views; i++) {
                    assertViewStatus("v_" + i, "retrying");
                    assertViewRowCount("v_" + i, 1);
                }

                // Cross each deadline in turn; exactly one more view catches up each time, proving
                // the heap pops due entries only, in deadline order.
                for (int due = 0; due < views; due++) {
                    setCurrentMicros(t0 + 1_000_000L + due * 100_000L); // d_due
                    drainMatViewTimerQueue(timerJob);
                    drainWalAndMatViewQueues();
                    for (int i = 0; i < views; i++) {
                        if (i <= due) {
                            assertViewStatus("v_" + i, "valid");
                            assertViewRowCount("v_" + i, 2);
                        } else {
                            assertViewStatus("v_" + i, "retrying");
                            assertViewRowCount("v_" + i, 1);
                        }
                    }
                }
            } finally {
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testDeferredIntermediateViewDoesNotInvalidateGrandchild() throws Exception {
        // 0 backoff so the timer sweep re-drives the deferred refresh on the very next tick.
        setProperty(PropertyKey.CAIRO_MAT_VIEW_REFRESH_BUSY_RETRY_TIMEOUT, 0);
        failBaseReader.set(false);
        failViewWalWriter.set(false);
        // Fail only the base table reader, so the intermediate view (which reads the base) is
        // deferred while the grandchild (which reads the intermediate view) is left untouched.
        baseTableName = "base_price";
        assertMemoryLeak(() -> {
            execute(
                    "create table base_price (" +
                            "sym varchar, price double, ts timestamp" +
                            ") timestamp(ts) partition by DAY WAL;"
            );
            execute("insert into base_price values('a', 1.0, '2020-01-01T00:00:00.000000Z')");
            drainWalAndMatViewQueues();

            // A dependent-view chain: base_price -> price_1h (intermediate) -> price_1d (grandchild).
            execute(
                    "create materialized view price_1h as (" +
                            "  select ts, avg(price) as avg_price from base_price sample by 1h" +
                            ") partition by day"
            );
            drainWalAndMatViewQueues();
            execute(
                    "create materialized view price_1d as (" +
                            "  select ts, avg(avg_price) as avg_price from price_1h sample by 1d" +
                            ") partition by day"
            );
            drainWalAndMatViewQueues();
            assertViewStatus("price_1h", "valid");
            assertViewStatus("price_1d", "valid");
            assertViewRowCount("price_1h", 1);
            assertViewRowCount("price_1d", 1);

            // A second base bucket lands. The intermediate view's refresh hits "table busy".
            execute("insert into base_price values('a', 2.0, '2020-01-01T01:00:00.000000Z')");
            drainWalQueue();
            failBaseReader.set(true);
            drainMatViewQueue(engine);

            // The intermediate is deferred (retrying), not invalidated. Because it never commits, the
            // grandchild is never enqueued for refresh: it must stay valid and stale, NOT invalidated.
            // The intermediate is deferred (retrying); the grandchild is not invalidated (proven by
            // assertNoInvalidViews) and stays stale.
            assertNoInvalidViews();
            assertViewStatus("price_1h", "retrying");
            assertViewRowCount("price_1h", 1);
            // Grandchild folded only the first bucket (avg of {1.0}); prove it did NOT advance.
            assertQuery("select avg_price from price_1d")
                    .noLeakCheck()
                    .expectSize()
                    .returns("avg_price\n1.0\n");

            // Clear the failure; the timer re-drives the intermediate and the chain catches up.
            failBaseReader.set(false);
            drainMatViewTimerQueue(new MatViewTimerJob(engine));
            for (int i = 0; i < 3; i++) {
                drainWalAndMatViewQueues();
            }
            assertNoInvalidViews();
            assertViewRowCount("price_1h", 2);
            // Grandchild now reflects both hourly buckets: avg(1.0, 2.0) = 1.5.
            assertViewRowCount("price_1d", 1); // both in the same day bucket
            assertQuery("select avg_price from price_1d")
                    .noLeakCheck()
                    .expectSize()
                    .returns("avg_price\n1.5\n");
        });
    }

    @Test
    public void testReaderPoolExhaustionInvalidatesAfterRetryLimitExceeded() throws Exception {
        // 0 backoff and a low cap so a few re-drives exhaust the retry limit.
        setProperty(PropertyKey.CAIRO_MAT_VIEW_REFRESH_BUSY_RETRY_TIMEOUT, 0);
        setProperty(PropertyKey.CAIRO_MAT_VIEW_REFRESH_BUSY_RETRY_LIMIT, 2);
        failBaseReader.set(false);
        failViewWalWriter.set(false);
        baseTableName = "base_price";
        assertMemoryLeak(() -> {
            execute(
                    "create table base_price (" +
                            "sym varchar, price double, ts timestamp" +
                            ") timestamp(ts) partition by DAY WAL;"
            );
            execute("insert into base_price values('a', 1.0, '2020-01-01T00:00:00.000000Z')");
            drainWalAndMatViewQueues();

            execute(
                    "create materialized view price_1h as (" +
                            "  select ts, avg(price) as avg_price from base_price sample by 1h" +
                            ") partition by day"
            );
            drainWalAndMatViewQueues();
            assertViewStatus("valid");
            assertViewRowCount(1);

            execute("insert into base_price values('a', 2.0, '2020-01-01T01:00:00.000000Z')");

            // Keep the base table reader unavailable across every re-drive. Each deferral bumps the
            // retry counter; once it exceeds the limit (2), the view is invalidated to release the
            // base table's WAL retention. This is the EntryUnavailableException ("table busy") cap
            // path, distinct from the OOM cap covered by MatViewRefreshRetryTest.
            failBaseReader.set(true);
            drainWalAndMatViewQueues();
            assertViewStatus("retrying");
            assertViewRowCount(1);

            final MatViewTimerJob timerJob = new MatViewTimerJob(engine);
            for (int i = 0; i < 5; i++) {
                drainMatViewTimerQueue(timerJob);
                drainWalAndMatViewQueues();
            }
            assertViewStatus("invalid");
            assertViewRowCount(1);

            // With the reader available again, an explicit full refresh recovers the view.
            failBaseReader.set(false);
            execute("refresh materialized view price_1h full;");
            drainWalAndMatViewQueues();
            assertViewStatus("valid");
            assertViewRowCount(2);
        });
    }

    @Test
    public void testRetryCounterResetsAfterSuccessfulRefresh() throws Exception {
        // 0 backoff and a cap of 2 so the reset is observable: a successful refresh between two
        // deferral runs must restore the full retry budget.
        setProperty(PropertyKey.CAIRO_MAT_VIEW_REFRESH_BUSY_RETRY_TIMEOUT, 0);
        setProperty(PropertyKey.CAIRO_MAT_VIEW_REFRESH_BUSY_RETRY_LIMIT, 2);
        failBaseReader.set(false);
        failViewWalWriter.set(false);
        baseTableName = "base_price";
        assertMemoryLeak(() -> {
            execute(
                    "create table base_price (" +
                            "sym varchar, price double, ts timestamp" +
                            ") timestamp(ts) partition by DAY WAL;"
            );
            execute("insert into base_price values('a', 1.0, '2020-01-01T00:00:00.000000Z')");
            drainWalAndMatViewQueues();

            execute(
                    "create materialized view price_1h as (" +
                            "  select ts, avg(price) as avg_price from base_price sample by 1h" +
                            ") partition by day"
            );
            drainWalAndMatViewQueues();
            assertViewStatus("valid");
            assertViewRowCount(1);

            // Round 1: defer once, then let the refresh succeed. The success must reset the counter.
            execute("insert into base_price values('a', 2.0, '2020-01-01T01:00:00.000000Z')");
            failBaseReader.set(true);
            drainWalAndMatViewQueues();
            assertViewStatus("retrying");
            failBaseReader.set(false);
            drainMatViewTimerQueue(new MatViewTimerJob(engine));
            drainWalAndMatViewQueues();
            assertViewStatus("valid");
            assertViewRowCount(2);

            // Round 2: with the counter reset, the cap (2) must again allow two fresh deferrals before
            // invalidation. Had the counter leaked from round 1, the view would invalidate a deferral
            // sooner (i.e. it would already be "invalid" at the assertion below).
            execute("insert into base_price values('a', 3.0, '2020-01-01T02:00:00.000000Z')");
            failBaseReader.set(true);
            drainWalAndMatViewQueues(); // fresh attempt 1
            assertViewStatus("retrying");
            final MatViewTimerJob timerJob = new MatViewTimerJob(engine);
            drainMatViewTimerQueue(timerJob);
            drainWalAndMatViewQueues(); // fresh attempt 2 - still under the cap, proving the reset
            assertViewStatus("retrying");
            drainMatViewTimerQueue(timerJob);
            drainWalAndMatViewQueues(); // attempt 3 exceeds the cap -> invalidate
            assertViewStatus("invalid");
        });
    }

    @Test
    public void testWalWriterPoolExhaustionDefersDependentViewRefresh() throws Exception {
        // Exercises the base-commit (dependent views) refresh path's outer catch: getWalWriter()
        // for the view throws, simulating the view's own WAL writer pool being exhausted.
        setProperty(PropertyKey.CAIRO_MAT_VIEW_REFRESH_BUSY_RETRY_TIMEOUT, 0);
        failBaseReader.set(false);
        failViewWalWriter.set(false);
        viewWalWriterName = "price_1h";
        assertMemoryLeak(() -> {
            execute(
                    "create table base_price (" +
                            "sym varchar, price double, ts timestamp" +
                            ") timestamp(ts) partition by DAY WAL;"
            );
            execute("insert into base_price values('a', 1.0, '2020-01-01T00:00:00.000000Z')");
            drainWalAndMatViewQueues();

            execute(
                    "create materialized view price_1h as (" +
                            "  select ts, avg(price) as avg_price from base_price sample by 1h" +
                            ") partition by day"
            );
            drainWalAndMatViewQueues();
            assertViewStatus("valid");
            assertViewRowCount(1);

            execute("insert into base_price values('a', 2.0, '2020-01-01T01:00:00.000000Z')");
            drainWalQueue();

            // The view's WAL writer pool is exhausted as the base-commit-driven refresh tries to
            // acquire a writer. The outer catch must defer (not invalidate) the refresh.
            failViewWalWriter.set(true);
            drainMatViewQueue(engine);
            assertNoInvalidViews();
            assertViewStatus("retrying");
            assertViewRowCount(1);

            // Clear the failure; the timer re-drives the deferred refresh and the view catches up.
            failViewWalWriter.set(false);
            drainMatViewTimerQueue(new MatViewTimerJob(engine));
            drainWalAndMatViewQueues();
            assertViewStatus("valid");
            assertViewRowCount(2);
        });
    }

    @Test
    public void testWalWriterPoolExhaustionDefersTimerDrivenRefresh() throws Exception {
        // Exercises the single-view refresh path's outer catch (the timer-driven re-drive calls
        // refreshIncremental(view)): getWalWriter() for the view throws on that path too.
        setProperty(PropertyKey.CAIRO_MAT_VIEW_REFRESH_BUSY_RETRY_TIMEOUT, 0);
        failBaseReader.set(false);
        failViewWalWriter.set(false);
        viewWalWriterName = "price_1h";
        assertMemoryLeak(() -> {
            execute(
                    "create table base_price (" +
                            "sym varchar, price double, ts timestamp" +
                            ") timestamp(ts) partition by DAY WAL;"
            );
            execute("insert into base_price values('a', 1.0, '2020-01-01T00:00:00.000000Z')");
            drainWalAndMatViewQueues();

            execute(
                    "create materialized view price_1h as (" +
                            "  select ts, avg(price) as avg_price from base_price sample by 1h" +
                            ") partition by day"
            );
            drainWalAndMatViewQueues();
            assertViewStatus("valid");
            assertViewRowCount(1);

            execute("insert into base_price values('a', 2.0, '2020-01-01T01:00:00.000000Z')");
            drainWalQueue();

            // First deferral via the base-commit (dependent views) path.
            failViewWalWriter.set(true);
            drainMatViewQueue(engine);
            assertViewStatus("retrying");
            assertViewRowCount(1);

            // The timer re-drives via refreshIncremental(view); the WAL writer pool is still
            // exhausted, so the single-view path's outer catch defers again instead of invalidating.
            final MatViewTimerJob timerJob = new MatViewTimerJob(engine);
            drainMatViewTimerQueue(timerJob);
            drainMatViewQueue(engine);
            assertNoInvalidViews();
            assertViewStatus("retrying");
            assertViewRowCount(1);

            // Clear the failure; the next re-drive catches the view up.
            failViewWalWriter.set(false);
            drainMatViewTimerQueue(timerJob);
            drainWalAndMatViewQueues();
            assertViewStatus("valid");
            assertViewRowCount(2);
        });
    }

    @Test
    public void testResetStateRetriableErrorDoesNotScheduleRetryOnInvalidView() throws Exception {
        // m3 regression - stale-invalid retry scheduling window. When the WAL write inside
        // refreshFailState (WalWriter.resetMatViewState) throws a RETRIABLE error AFTER refreshFail()
        // has already marked the view invalid in-memory, the failure propagates to the dependent-view
        // loop's OUTER catch, whose tryScheduleRetry() then arms a refresh retry on an already-invalid
        // view. The retry is later dropped by the timer job's isInvalid() guard, so it is harmless,
        // but an invalid view must not carry a pending retry deadline.
        //
        // RED on current code (a retry is armed on the invalid view); GREEN once the outer-catch
        // tryScheduleRetry is gated on !viewState.isInvalid().
        setProperty(PropertyKey.CAIRO_MAT_VIEW_REFRESH_BUSY_RETRY_TIMEOUT, 1000);
        setProperty(PropertyKey.CAIRO_MAT_VIEW_REFRESH_BUSY_RETRY_LIMIT, 10);
        failBaseReader.set(false);
        failViewWalWriter.set(false);
        failBaseReaderNonRetriable.set(false);
        failViewResetMatViewState.set(false);
        baseTableName = "base_price";
        viewWalWriterName = "price_1h";
        assertMemoryLeak(() -> {
            execute(
                    "create table base_price (" +
                            "sym varchar, price double, ts timestamp" +
                            ") timestamp(ts) partition by DAY WAL;"
            );
            execute("insert into base_price values('a', 1.0, '2020-01-01T00:00:00.000000Z')");
            drainWalAndMatViewQueues();

            execute(
                    "create materialized view price_1h as (" +
                            "  select ts, avg(price) as avg_price from base_price sample by 1h" +
                            ") partition by day"
            );
            drainWalAndMatViewQueues();
            assertViewStatus("valid");
            assertViewRowCount(1);

            // A second bucket lands; committing it enqueues the dependent-view refresh.
            execute("insert into base_price values('a', 2.0, '2020-01-01T01:00:00.000000Z')");
            drainWalQueue();

            // Stage 1: the base read fails NON-retriably -> refreshIncremental0 throws -> the inner
            //          catch calls refreshFailState, which marks the view invalid in-memory.
            // Stage 2: resetMatViewState (the WAL write inside refreshFailState) throws a RETRIABLE
            //          error -> propagates to the dependent-view loop's outer catch.
            failBaseReaderNonRetriable.set(true);
            failViewResetMatViewState.set(true);
            drainMatViewQueue(engine);

            // The view is invalid (refreshFail ran). It must NOT carry a pending refresh retry.
            final TableToken viewToken = engine.getTableTokenIfExists("price_1h");
            Assert.assertNotNull(viewToken);
            final MatViewState state = engine.getMatViewStateStore().getViewState(viewToken);
            Assert.assertNotNull(state);
            Assert.assertTrue("view must be invalid after the refresh failure", state.isInvalid());
            Assert.assertEquals(
                    "an invalid view must not carry a pending refresh-retry deadline",
                    Numbers.LONG_NULL,
                    state.getRefreshRetryAfterMicros()
            );
            Assert.assertEquals(
                    "an invalid view must not carry a bumped refresh-retry count",
                    0,
                    state.getRefreshRetryCount()
            );
        });
    }

    @Test
    public void testResetStateRetriableErrorDoesNotScheduleRetryOnInvalidViewSingleViewPath() throws Exception {
        // Same m3 stale-invalid retry window as the dependent-view test, but on the single-view
        // (timer/enqueue-driven) refresh path: refreshIncremental(view)'s inner catch calls
        // refreshFailState (marking the view invalid in-memory), whose WAL write throws a RETRIABLE
        // error that propagates to that method's OUTER catch. The outer-catch tryScheduleRetry must
        // not arm a retry on the now-invalid view.
        setProperty(PropertyKey.CAIRO_MAT_VIEW_REFRESH_BUSY_RETRY_TIMEOUT, 1000);
        setProperty(PropertyKey.CAIRO_MAT_VIEW_REFRESH_BUSY_RETRY_LIMIT, 10);
        baseTableName = "base_price";
        viewWalWriterName = "price_1h";
        assertMemoryLeak(() -> {
            execute(
                    "create table base_price (" +
                            "sym varchar, price double, ts timestamp" +
                            ") timestamp(ts) partition by DAY WAL;"
            );
            execute("insert into base_price values('a', 1.0, '2020-01-01T00:00:00.000000Z')");
            drainWalAndMatViewQueues();

            execute(
                    "create materialized view price_1h as (" +
                            "  select ts, avg(price) as avg_price from base_price sample by 1h" +
                            ") partition by day"
            );
            drainWalAndMatViewQueues();
            assertViewStatus("valid");
            assertViewRowCount(1);

            // A second bucket lands so the view has work to refresh, but commit it WITHOUT driving the
            // mat view queue, so no dependent-view refresh runs yet.
            execute("insert into base_price values('a', 2.0, '2020-01-01T01:00:00.000000Z')");
            drainWalQueue();

            // Drive the SINGLE-VIEW path directly by enqueuing a view-targeted incremental refresh.
            final TableToken viewToken = engine.getTableTokenIfExists("price_1h");
            Assert.assertNotNull(viewToken);
            failBaseReaderNonRetriable.set(true);
            failViewResetMatViewState.set(true);
            engine.getMatViewStateStore().enqueueIncrementalRefresh(viewToken);
            drainMatViewQueue(engine);

            // The view is invalid (refreshFail ran). It must NOT carry a pending refresh retry.
            final MatViewState state = engine.getMatViewStateStore().getViewState(viewToken);
            Assert.assertNotNull(state);
            Assert.assertTrue("view must be invalid after the refresh failure", state.isInvalid());
            Assert.assertEquals(
                    "an invalid view must not carry a pending refresh-retry deadline",
                    Numbers.LONG_NULL,
                    state.getRefreshRetryAfterMicros()
            );
            Assert.assertEquals(
                    "an invalid view must not carry a bumped refresh-retry count",
                    0,
                    state.getRefreshRetryCount()
            );
        });
    }

    @Test
    public void testPeriodRangeRefreshOutOfMemoryDefersInsteadOfInvalidating() throws Exception {
        // A period mat view's scheduled refresh runs through the range path. An out-of-memory error
        // there must defer (not invalidate), exactly like the incremental path.
        assertPeriodRangeRefreshTransientErrorDefers(failBaseReaderOom);
    }

    @Test
    public void testPeriodRangeRefreshReaderPoolExhaustionDefersInsteadOfInvalidating() throws Exception {
        // A period mat view's scheduled refresh runs through the range path (the period timer enqueues
        // a range refresh). A transient "base table busy" error there must defer (not invalidate).
        assertPeriodRangeRefreshTransientErrorDefers(failBaseReader);
    }

    private void assertPeriodRangeRefreshTransientErrorDefers(AtomicBoolean faultSwitch) throws Exception {
        // 0 backoff so the timer sweep re-drives the deferred refresh on the very next tick.
        setProperty(PropertyKey.CAIRO_MAT_VIEW_REFRESH_BUSY_RETRY_TIMEOUT, 0);
        baseTableName = "base_price";
        assertMemoryLeak(() -> {
            execute(
                    "create table base_price (" +
                            "sym varchar, price double, ts timestamp" +
                            ") timestamp(ts) partition by DAY WAL;"
            );
            // Anchor the clock so period P1 (2020-01-01) is already complete at creation, while
            // period P2 (2020-01-02) is still open. An immediate period view registers ONLY a period
            // timer, so the sole timer-driven refresh path is the range path.
            currentMicros = parseFloorPartialTimestamp("2020-01-02T00:00:00.000000Z");
            execute(
                    "create materialized view price_1h refresh immediate period (length 1d) as (" +
                            "  select ts, avg(price) as avg_price from base_price sample by 1h" +
                            ") partition by day"
            );
            // One row in P1 and one row in P2. P2's row stays unrefreshed until P2 closes.
            execute(
                    "insert into base_price values('a', 1.0, '2020-01-01T00:00:00.000000Z')" +
                            ",('a', 2.0, '2020-01-02T12:00:00.000000Z')"
            );

            // Baseline: drain everything so P1 is refreshed (via the immediate incremental path) and
            // the view is valid with no pending tasks.
            final MatViewTimerJob timerJob = new MatViewTimerJob(engine);
            drainMatViewTimerQueue(timerJob);
            drainWalAndMatViewQueues();
            assertViewStatus("valid");
            assertViewRowCount(1);

            // P2 closes. No new base data, so the only refresh trigger is the period timer, i.e. the
            // range path. Inject the transient error and let the range refresh run.
            currentMicros = parseFloorPartialTimestamp("2020-01-03T00:00:01.000000Z");
            faultSwitch.set(true);
            drainMatViewTimerQueue(timerJob); // period timer -> enqueueRangeRefresh(P2)
            drainMatViewQueue(engine);        // rangeRefresh -> getReader throws -> defer
            // Apply any state transaction the refresh wrote. Without the fix the range path calls
            // refreshFailState here, so this applies the invalidation and assertNoInvalidViews() below
            // fails -- demonstrating the bug. With the fix nothing is written (the refresh is deferred).
            drainWalQueue();

            // The transient failure must NOT invalidate the period view (it reports "retrying"), and
            // the view must not have caught up yet (still P1's single bucket).
            assertNoInvalidViews();
            assertViewStatus("retrying");
            assertViewRowCount(1);

            // Clear the failure; the timer re-drives the deferred refresh (as an incremental refresh,
            // which re-includes the now-complete P2) and the view catches up.
            faultSwitch.set(false);
            drainMatViewTimerQueue(timerJob);
            drainWalAndMatViewQueues();
            assertViewStatus("valid");
            assertViewRowCount(2);
        });
    }

    private void assertNoInvalidViews() throws Exception {
        assertQuery("select count() from materialized_views where view_status = 'invalid'")
                .noLeakCheck()
                .expectSize()
                .noRandomAccess()
                .returns("count\n0\n");
    }

    private void assertViewRowCount(long expected) throws Exception {
        assertViewRowCount("price_1h", expected);
    }

    private void assertViewRowCount(String viewName, long expected) throws Exception {
        assertQuery("select count() from " + viewName)
                .noLeakCheck()
                .expectSize()
                .noRandomAccess()
                .returns("count\n" + expected + "\n");
    }

    private void assertViewStatus(String viewName, String status) throws Exception {
        assertQuery("select view_status from materialized_views where view_name = '" + viewName + "'")
                .noLeakCheck()
                .noRandomAccess()
                .returns("view_status\n" + status + "\n");
    }

    private void assertViewStatus(String status) throws Exception {
        assertQuery("select view_name, view_status from materialized_views")
                .noLeakCheck()
                .noRandomAccess()
                .returns("view_name\tview_status\nprice_1h\t" + status + "\n");
    }
}
