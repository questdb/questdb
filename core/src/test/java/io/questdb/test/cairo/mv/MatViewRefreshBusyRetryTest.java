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
import io.questdb.cairo.EntryUnavailableException;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.mv.MatViewTimerJob;
import io.questdb.test.AbstractCairoTest;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Verifies that a transient "table busy" error (e.g. base table reader pool exhausted) while
 * refreshing a materialized view does NOT invalidate the view. Instead, the refresh is deferred and
 * re-driven by {@link MatViewTimerJob} once the backoff elapses, after which the view catches up.
 */
public class MatViewRefreshBusyRetryTest extends AbstractCairoTest {

    private static final AtomicBoolean failBaseReader = new AtomicBoolean(false);
    private static volatile String baseTableName;

    @BeforeClass
    public static void setUpStatic() throws Exception {
        // Inject an engine whose getReader() throws EntryUnavailableException for the base table,
        // simulating reader pool exhaustion during a materialized view refresh.
        AbstractCairoTest.engineFactory = conf -> new CairoEngine(conf) {
            @Override
            public TableReader getReader(TableToken tableToken) {
                final String prefix = baseTableName;
                if (failBaseReader.get() && prefix != null && tableToken.getTableName().startsWith(prefix)) {
                    throw EntryUnavailableException.instance("pool size exceeded");
                }
                return super.getReader(tableToken);
            }
        };
        AbstractCairoTest.setUpStatic();
    }

    @Test
    public void testDeferredRefreshWaitsForBackoffDeadline() throws Exception {
        // 1s backoff so the deferred refresh is not eligible immediately.
        setProperty(PropertyKey.CAIRO_MAT_VIEW_REFRESH_BUSY_RETRY_TIMEOUT, 1000);
        failBaseReader.set(false);
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
