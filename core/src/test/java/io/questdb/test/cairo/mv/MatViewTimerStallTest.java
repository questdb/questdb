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
import io.questdb.cairo.TableToken;
import io.questdb.cairo.mv.MatViewRefreshJob;
import io.questdb.cairo.mv.MatViewState;
import io.questdb.cairo.mv.MatViewTimerJob;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.std.str.Path;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Regression for #7416: a timer-driven (REFRESH EVERY) materialized view can silently stall when
 * an in-flight refresh parks after taking the view latch. The timer job must log while the latch
 * is held and the view is behind, avoid hot-spinning intervals updates, and re-drive when a prior
 * task returned before tryLock without bumping refreshSeq.
 */
public class MatViewTimerStallTest extends AbstractCairoTest {

    private static final AtomicBoolean wedgeArmed = new AtomicBoolean(false);
    private static final AtomicBoolean wedgeConsumed = new AtomicBoolean(false);
    private static volatile CountDownLatch wedgeEntered;
    private static volatile CountDownLatch wedgeRelease;
    private static volatile String wedgedViewName;

    @BeforeClass
    public static void setUpStatic() throws Exception {
        // Park the first getWalWriter() for the wedged view while armed. refreshIncremental takes
        // the view latch before getWalWriter, so the latch stays held for the whole park.
        AbstractCairoTest.engineFactory = conf -> new CairoEngine(conf) {
            @Override
            public WalWriter getWalWriter(TableToken tableToken) {
                final String name = wedgedViewName;
                if (name != null
                        && tableToken.getTableName().equals(name)
                        && wedgeArmed.get()
                        && wedgeConsumed.compareAndSet(false, true)) {
                    wedgeEntered.countDown();
                    try {
                        wedgeRelease.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
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
        wedgeArmed.set(false);
        wedgeConsumed.set(false);
        wedgeEntered = new CountDownLatch(1);
        wedgeRelease = new CountDownLatch(1);
        wedgedViewName = null;
        setProperty(PropertyKey.CAIRO_MAT_VIEW_REFRESH_BUSY_RETRY_TIMEOUT, 0);
        // Keep the intervals timer out of the simulated window so a wedged latch cannot interact
        // with UPDATE_REFRESH_INTERVALS during the stall assertions.
        setProperty(PropertyKey.CAIRO_MAT_VIEW_REFRESH_INTERVALS_UPDATE_PERIOD, 86_400_000);
    }

    @Test
    public void testWedgedRefreshKeepsSiblingHealthyAndRecovers() throws Exception {
        wedgedViewName = "v_wedged";
        assertMemoryLeak(() -> {
            Thread wedgeThread = null;
            try {
                currentMicros = parseFloorPartialTimestamp("2024-01-01T00:00:00.000000Z");
                execute(
                        "create table base_price (sym varchar, price double, ts timestamp) " +
                                "timestamp(ts) partition by DAY WAL"
                );
                execute("insert into base_price values('a', 1.0, '2024-01-01T00:00:00.000000Z')");
                drainWalQueue();

                execute(
                        "create materialized view v_wedged refresh every 1m as (" +
                                "select ts, avg(price) as avg_price from base_price sample by 1h) partition by day"
                );
                execute(
                        "create materialized view v_ok refresh every 1m as (" +
                                "select ts, avg(price) as avg_price from base_price sample by 1h) partition by day"
                );

                final MatViewTimerJob timerJob = new MatViewTimerJob(engine);

                currentMicros += Micros.MINUTE_MICROS;
                drainMatViewTimerQueue(timerJob);
                drainWalAndMatViewQueues();
                assertQuery("""
                        select view_name, view_status, refresh_base_table_txn from materialized_views order by view_name
                        """)
                        .noLeakCheck()
                        .returns("""
                                view_name\tview_status\trefresh_base_table_txn
                                v_ok\tvalid\t1
                                v_wedged\tvalid\t1
                                """);

                execute("insert into base_price values('a', 2.0, '2024-01-01T01:00:00.000000Z')");
                drainWalQueue();

                wedgeArmed.set(true);
                currentMicros += Micros.MINUTE_MICROS;
                drainMatViewTimerQueue(timerJob);

                wedgeThread = new Thread(() -> {
                    try (MatViewRefreshJob job = createMatViewRefreshJob(engine)) {
                        drainMatViewQueue(job);
                    } finally {
                        Path.clearThreadLocals();
                    }
                }, "wedged-refresh");
                wedgeThread.start();
                Assert.assertTrue("refresh never reached the wedge", wedgeEntered.await(30, TimeUnit.SECONDS));

                final TableToken wedgedToken = engine.verifyTableName("v_wedged");
                final MatViewState wedgedState = engine.getMatViewStateStore().getViewState(wedgedToken);
                Assert.assertNotNull(wedgedState);
                Assert.assertTrue(wedgedState.isLocked());
                Assert.assertTrue(wedgedState.getRefreshLockTimestampUs() > 0);

                for (int hour = 2; hour <= 4; hour++) {
                    execute("insert into base_price values('a', " + (hour + 1) + ".0, '2024-01-01T0" + hour + ":00:00.000000Z')");
                    drainWalQueue();
                    currentMicros += Micros.MINUTE_MICROS;
                    drainMatViewTimerQueue(timerJob);
                    // Sibling can still refresh; wedged view must not be re-enqueued into a hot spin.
                    drainWalAndMatViewQueues();
                }

                assertQuery("""
                        select view_name, view_status, refresh_base_table_txn,
                        base_table_txn > refresh_base_table_txn as is_behind
                        from materialized_views order by view_name
                        """)
                        .noLeakCheck()
                        .returns("""
                                view_name\tview_status\trefresh_base_table_txn\tis_behind
                                v_ok\tvalid\t5\tfalse
                                v_wedged\tvalid\t1\ttrue
                                """);

                wedgeRelease.countDown();
                wedgeThread.join(TimeUnit.SECONDS.toMillis(30));
                Assert.assertFalse("wedged refresh thread did not finish", wedgeThread.isAlive());
                wedgeThread = null;
                drainWalAndMatViewQueues();

                currentMicros += Micros.MINUTE_MICROS;
                drainMatViewTimerQueue(timerJob);
                drainWalAndMatViewQueues();

                assertQuery("""
                        select view_name, view_status, refresh_base_table_txn,
                        base_table_txn > refresh_base_table_txn as is_behind
                        from materialized_views order by view_name
                        """)
                        .noLeakCheck()
                        .returns("""
                                view_name\tview_status\trefresh_base_table_txn\tis_behind
                                v_ok\tvalid\t5\tfalse
                                v_wedged\tvalid\t5\tfalse
                                """);
            } finally {
                if (wedgeThread != null) {
                    wedgeRelease.countDown();
                    wedgeThread.join(TimeUnit.SECONDS.toMillis(30));
                }
                wedgeArmed.set(false);
                wedgedViewName = null;
                currentMicros = -1;
            }
        });
    }

    @Test
    public void testBlockedRefreshIsRedrivenAfterUnblock() throws Exception {
        assertMemoryLeak(() -> {
            try {
                currentMicros = parseFloorPartialTimestamp("2024-01-01T00:00:00.000000Z");
                execute(
                        "create table base_price (sym varchar, price double, ts timestamp) " +
                                "timestamp(ts) partition by DAY WAL"
                );
                execute("insert into base_price values('a', 1.0, '2024-01-01T00:00:00.000000Z')");
                drainWalQueue();

                execute(
                        "create materialized view v_blocked refresh every 1m as (" +
                                "select ts, avg(price) as avg_price from base_price sample by 1h) partition by day"
                );

                final MatViewTimerJob timerJob = new MatViewTimerJob(engine);
                currentMicros += Micros.MINUTE_MICROS;
                drainMatViewTimerQueue(timerJob);
                drainWalAndMatViewQueues();

                assertQuery("""
                        select view_name, view_status, refresh_base_table_txn from materialized_views
                        """)
                        .noLeakCheck()
                        .noRandomAccess()
                        .returns("""
                                view_name\tview_status\trefresh_base_table_txn
                                v_blocked\tvalid\t1
                                """);

                setProperty(PropertyKey.CAIRO_MAT_VIEW_REFRESH_BLOCK_LIST, "v_blocked");

                execute("insert into base_price values('a', 2.0, '2024-01-01T01:00:00.000000Z')");
                drainWalQueue();

                // Timer enqueues; refresh returns before tryLock without bumping refreshSeq.
                currentMicros += Micros.MINUTE_MICROS;
                drainMatViewTimerQueue(timerJob);
                drainWalAndMatViewQueues();

                execute("insert into base_price values('a', 3.0, '2024-01-01T02:00:00.000000Z')");
                drainWalQueue();
                currentMicros += Micros.MINUTE_MICROS;
                drainMatViewTimerQueue(timerJob);
                drainWalAndMatViewQueues();

                assertQuery("""
                        select view_name, view_status, refresh_base_table_txn,
                        base_table_txn > refresh_base_table_txn as is_behind
                        from materialized_views
                        """)
                        .noLeakCheck()
                        .noRandomAccess()
                        .returns("""
                                view_name\tview_status\trefresh_base_table_txn\tis_behind
                                v_blocked\tvalid\t1\ttrue
                                """);

                setProperty(PropertyKey.CAIRO_MAT_VIEW_REFRESH_BLOCK_LIST, "");

                // Without the !locked re-drive, knownSeq == refreshSeq would leave the view stalled
                // after unblock. The timer must enqueue again while the view is behind.
                currentMicros += Micros.MINUTE_MICROS;
                drainMatViewTimerQueue(timerJob);
                drainWalAndMatViewQueues();

                assertQuery("""
                        select view_name, view_status, refresh_base_table_txn,
                        base_table_txn > refresh_base_table_txn as is_behind
                        from materialized_views
                        """)
                        .noLeakCheck()
                        .noRandomAccess()
                        .returns("""
                                view_name\tview_status\trefresh_base_table_txn\tis_behind
                                v_blocked\tvalid\t3\tfalse
                                """);
            } finally {
                currentMicros = -1;
                setProperty(PropertyKey.CAIRO_MAT_VIEW_REFRESH_BLOCK_LIST, "");
            }
        });
    }
}
