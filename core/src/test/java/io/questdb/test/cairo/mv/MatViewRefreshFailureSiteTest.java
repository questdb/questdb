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
import io.questdb.cairo.TableToken;
import io.questdb.cairo.mv.MatViewRefreshJob;
import io.questdb.cairo.mv.MatViewState;
import io.questdb.cairo.mv.MatViewTimerJob;
import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A refresh fails at three sites inside the refresh job's insert-as-select: the compile of the view's SQL, the run of
 * the query and the copy of its rows (a value that does not cast to its view column, or any other runtime error), and
 * the recompiles that schema changes keep making stale until the retries run out. A user's
 * {@code REFRESH ... RANGE} that fails at any of them leaves the view valid, with its rows, refresh position and
 * view WAL untouched, reports the error in {@code materialized_views().last_refresh_error}, is not retried, and the
 * next RANGE that succeeds clears the error. Incremental, FULL and period-timer refreshes keep their behaviour at the
 * same sites: a compile or runtime error invalidates the view with that error, and running out of retries defers an
 * incremental or period refresh, which then succeeds, and invalidates a FULL refresh until its redelivery rebuilds
 * the view.
 * <p>
 * The view joins a WAL base with a non-WAL table, whose schema changes apply at once and make a compiled plan stale
 * at its next cursor; a seam applies them once the refresh holds the view lock, after the FULL refresh has compiled
 * its plan. Expected rows come from the base rows each test inserts: a 1h bucket holds the last price and the last
 * amount / qty ratio of its rows, named by the joined table.
 */
@RunWith(Parameterized.class)
public class MatViewRefreshFailureSiteTest extends AbstractCairoTest {
    private static final String DAY_1_ROWS = """
            ts\tname\tprice\tratio
            2024-01-01T00:00:00.000000Z\talpha\t1.0\t2.0000
            2024-01-01T01:00:00.000000Z\tbeta\t2.0\t2.0000
            """;
    // DAY_1_ROWS and the day 2 row ('a', 5.0, 6.00 / 2.00); a quotient of two DECIMAL(10,2) values has scale 4
    private static final String DAY_1_AND_2_ROWS = DAY_1_ROWS + "2024-01-02T00:00:00.000000Z\talpha\t5.0\t3.0000\n";
    private static final String FULL_OUT_OF_RETRIES_ERROR = "base table is under heavy DDL changes during full refresh";
    private static final String RANGE_OUT_OF_RETRIES_ERROR = "base table is under heavy DDL changes during range refresh";
    private static final String VIEW_SQL = "SELECT b.ts, d.name, last(b.price) AS price, last(b.amount / b.qty) AS ratio FROM base b JOIN dim d ON (sym) SAMPLE BY 1h";
    private final RefreshKind kind;

    public MatViewRefreshFailureSiteTest(RefreshKind kind) {
        this.kind = kind;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {RefreshKind.USER_RANGE}, {RefreshKind.INCREMENTAL}, {RefreshKind.FULL}, {RefreshKind.PERIOD}
        });
    }

    @Test
    public void testCastError() throws Exception {
        // The base column changes type, and a new row holds a value that does not cast back to the view column. The
        // compile accepts the type pair; the copy of the row fails.
        final String error = "inconvertible value: `abc` [VARCHAR -> DOUBLE] [from=price, to=price]";
        assertMemoryLeak(() -> {
            try {
                final MatViewTimerJob timerJob = createViewWithDayOneRows();
                execute("ALTER TABLE base ALTER COLUMN price TYPE VARCHAR");
                insertDayTwoRow("('a', 'abc', 6.00m, 2.00m, '2024-01-02T00:10:00.000000Z')");

                runFailingRefresh(timerJob, null, null);
                assertFailureOutcome(error);

                if (kind == RefreshKind.USER_RANGE) {
                    execute("UPDATE base SET price = '5.0' WHERE price = 'abc'");
                    assertRerunRangeSucceeds();
                }
            } finally {
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testCompileError() throws Exception {
        // The joined table drops a column the view reads once the refresh holds the view lock, so the plan the
        // refresh starts with is stale, and its recompile, inside the insert-as-select, fails.
        final String error = "[" + VIEW_SQL.indexOf("d.name") + "]: Invalid column: d.name";
        assertMemoryLeak(() -> {
            try {
                final MatViewTimerJob timerJob = createViewWithDayOneRows();
                insertDayTwoRow("('a', 5.0, 6.00m, 2.00m, '2024-01-02T00:10:00.000000Z')");

                final AtomicInteger compileCount = new AtomicInteger();
                runFailingRefresh(timerJob, "ALTER TABLE dim DROP COLUMN name", compileCount);
                Assert.assertEquals("the insert-as-select compiles the view's SQL once", 1, compileCount.get());
                assertFailureOutcome(error);

                if (kind == RefreshKind.USER_RANGE) {
                    execute("ALTER TABLE dim ADD COLUMN name STRING");
                    execute("UPDATE dim SET name = CASE WHEN sym = 'a' THEN 'alpha' ELSE 'beta' END");
                    assertRerunRangeSucceeds();
                }
            } finally {
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testOutOfRecompileRetries() throws Exception {
        // With no recompile retries, a schema change of the joined table once the refresh holds the view lock makes
        // the refresh's plan stale at its first cursor, and the retries run out.
        setProperty(PropertyKey.CAIRO_MAT_VIEW_MAX_REFRESH_RETRIES, 0);
        assertMemoryLeak(() -> {
            try {
                final MatViewTimerJob timerJob = createViewWithDayOneRows();
                insertDayTwoRow("('a', 5.0, 6.00m, 2.00m, '2024-01-02T00:10:00.000000Z')");

                final AtomicReference<String> reasonAtUnlock = new AtomicReference<>();
                if (kind == RefreshKind.FULL) {
                    // The redelivered FULL refresh rebuilds the view, so the invalidation shows at the failed
                    // refresh's release of the view lock.
                    final MatViewState state = viewState();
                    state.setOnBeforeUnlockForTesting(() -> reasonAtUnlock.set(state.isInvalid() ? state.getInvalidationReason() : "valid"));
                }
                runFailingRefresh(timerJob, "ALTER TABLE dim ADD COLUMN extra INT", null);
                switch (kind) {
                    case USER_RANGE -> {
                        assertFailureOutcome(RANGE_OUT_OF_RETRIES_ERROR);
                        // an incremental refresh that succeeds is no RANGE, so the error stays
                        execute("REFRESH MATERIALIZED VIEW mv INCREMENTAL");
                        drainWalAndMatViewQueues();
                        assertViewState("valid", "", RANGE_OUT_OF_RETRIES_ERROR);
                        assertViewRows(DAY_1_AND_2_ROWS);
                        assertRerunRangeSucceeds();
                    }
                    case INCREMENTAL, PERIOD -> {
                        // the refresh is deferred and runs again, with a fresh plan
                        drainWalAndMatViewQueues();
                        assertViewState("valid", "", null);
                        assertViewRows(DAY_1_AND_2_ROWS);
                    }
                    case FULL -> {
                        Assert.assertEquals(FULL_OUT_OF_RETRIES_ERROR, reasonAtUnlock.get());
                        // the FULL refresh waits for the view's WAL to apply its invalidation, then rebuilds the view
                        drainWalAndMatViewQueues();
                        drainWalAndMatViewQueues();
                        assertViewState("valid", "", null);
                        assertViewRows(DAY_1_AND_2_ROWS);
                    }
                }
            } finally {
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testRuntimeError() throws Exception {
        // A new row divides by a zero DECIMAL, which fails the query while the refresh reads its cursor.
        final String error = "[-1]: '/' operation failed: Division by zero";
        assertMemoryLeak(() -> {
            try {
                final MatViewTimerJob timerJob = createViewWithDayOneRows();
                insertDayTwoRow("('a', 5.0, 6.00m, 0.00m, '2024-01-02T00:10:00.000000Z')");

                runFailingRefresh(timerJob, null, null);
                assertFailureOutcome(error);

                if (kind == RefreshKind.USER_RANGE) {
                    execute("UPDATE base SET qty = 2.00m WHERE qty = 0.00m");
                    assertRerunRangeSucceeds();
                }
            } finally {
                setCurrentMicros(-1);
            }
        });
    }

    // A failed user RANGE leaves the view as it was and reports the error; any other refresh invalidates the view with
    // the error, after a FULL refresh has truncated it.
    private void assertFailureOutcome(String error) throws Exception {
        drainWalQueue();
        if (kind == RefreshKind.USER_RANGE) {
            assertViewState("valid", "", error);
            assertViewRows(DAY_1_ROWS);
            Assert.assertTrue("a failed user RANGE is not retried", engine.getMatViewStateStore().isRefreshQueueEmpty());
        } else {
            assertViewState("invalid", error, null);
            assertViewRows(kind == RefreshKind.FULL ? "ts\tname\tprice\tratio\n" : DAY_1_ROWS);
        }
    }

    // The next user RANGE over the same range succeeds and clears the error of the failed one.
    private void assertRerunRangeSucceeds() throws Exception {
        drainWalQueue();
        execute("REFRESH MATERIALIZED VIEW mv RANGE FROM '2024-01-02' TO '2024-01-03'");
        drainWalAndMatViewQueues();
        assertViewState("valid", "", null);
        assertViewRows(DAY_1_AND_2_ROWS);
    }

    private void assertViewRows(String expected) throws Exception {
        assertQuery("mv ORDER BY ts, name").timestamp("ts").noLeakCheck().expectSize().returns(expected);
    }

    // view_status reads "refreshing" after a successful user RANGE, which records no refresh finish time.
    private void assertViewState(String status, String reason, String error) throws Exception {
        final boolean isRangeSucceeded = kind == RefreshKind.USER_RANGE && error == null;
        assertQuery("SELECT view_status, invalidation_reason, last_refresh_error FROM materialized_views() WHERE view_name = 'mv'")
                .noLeakCheck()
                .noRandomAccess()
                .returns("view_status\tinvalidation_reason\tlast_refresh_error\n"
                        + (isRangeSucceeded ? "refreshing" : status) + '\t' + reason + '\t' + (error != null ? error : "") + '\n');
        Assert.assertEquals(error, viewState().getLastRefreshError());
    }

    // A view with the day 1 rows, refreshed once, so that its compiled plan is cached. A period view refreshes
    // complete days on its own; a user RANGE, an incremental and a FULL refresh run on a manual view.
    private MatViewTimerJob createViewWithDayOneRows() throws Exception {
        execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, amount DECIMAL(10,2), qty DECIMAL(10,2), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("CREATE TABLE dim (sym SYMBOL, name STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
        execute("""
                INSERT INTO dim VALUES
                ('a', 'alpha', '2024-01-01T00:00:00.000000Z'),
                ('b', 'beta', '2024-01-01T00:00:00.000000Z')
                """);
        if (kind == RefreshKind.PERIOD) {
            // the first day is complete, the second is not
            setCurrentMicros(parseFloorPartialTimestamp("2024-01-02T00:00:00.000000Z"));
        }
        execute("CREATE MATERIALIZED VIEW mv WITH BASE base REFRESH "
                + (kind == RefreshKind.PERIOD ? "IMMEDIATE PERIOD (LENGTH 1d)" : "MANUAL")
                + " AS (" + VIEW_SQL + ") PARTITION BY DAY");
        execute("""
                INSERT INTO base VALUES
                ('a', 1.0, 3.00m, 1.50m, '2024-01-01T00:10:00.000000Z'),
                ('b', 2.0, 4.00m, 2.00m, '2024-01-01T01:10:00.000000Z')
                """);
        final MatViewTimerJob timerJob = new MatViewTimerJob(engine);
        drainMatViewTimerQueue(timerJob);
        drainWalQueue();
        if (kind != RefreshKind.PERIOD) {
            execute("REFRESH MATERIALIZED VIEW mv INCREMENTAL");
        }
        drainWalAndMatViewQueues();
        assertViewRows(DAY_1_ROWS);
        Assert.assertNull(viewState().getLastRefreshError());
        return timerJob;
    }

    // A period view's refresh of a new row waits for the row's day to complete.
    private void insertDayTwoRow(String row) throws Exception {
        execute("INSERT INTO base VALUES " + row);
        drainWalAndMatViewQueues();
        assertViewRows(DAY_1_ROWS);
    }

    // Requests the refresh of the kind under test and runs it. The seam applies the schema change, when given, once
    // the refresh holds the view lock, and counts the compiles of the view's SQL inside the insert-as-select.
    private void runFailingRefresh(MatViewTimerJob timerJob, String schemaChangeSql, AtomicInteger compileCount) throws Exception {
        final TableToken viewToken = viewToken();
        final long viewSeqTxn = engine.getTableSequencerAPI().getTxnTracker(viewToken).getSeqTxn();
        final long refreshBaseTxn = viewState().getLastRefreshBaseTxn();
        final AtomicBoolean isSchemaChanged = new AtomicBoolean();
        try (MatViewRefreshJob job = createMatViewRefreshJob()) {
            if (schemaChangeSql != null) {
                job.setOnHoldingLockForTesting(() -> {
                    if (isSchemaChanged.compareAndSet(false, true)) {
                        try {
                            execute(schemaChangeSql);
                        } catch (SqlException e) {
                            throw new RuntimeException(e);
                        }
                    }
                });
            }
            if (compileCount != null) {
                job.setOnBeforeCompileForTesting(compileCount::incrementAndGet);
            }
            switch (kind) {
                case USER_RANGE -> execute("REFRESH MATERIALIZED VIEW mv RANGE FROM '2024-01-02' TO '2024-01-03'");
                case INCREMENTAL -> execute("REFRESH MATERIALIZED VIEW mv INCREMENTAL");
                case FULL -> execute("REFRESH MATERIALIZED VIEW mv FULL");
                case PERIOD -> {
                    // the second day completes, and the period timer refreshes it
                    setCurrentMicros(parseFloorPartialTimestamp("2024-01-03T00:00:00.000000Z"));
                    drainMatViewTimerQueue(timerJob);
                }
            }
            drainMatViewQueue(job);
        }
        Assert.assertEquals(schemaChangeSql != null, isSchemaChanged.get());
        if (kind == RefreshKind.USER_RANGE) {
            // RANGE never moves the refresh position, and the failed one writes nothing to the view's WAL
            Assert.assertEquals(viewSeqTxn, engine.getTableSequencerAPI().getTxnTracker(viewToken).getSeqTxn());
            Assert.assertEquals(refreshBaseTxn, viewState().getLastRefreshBaseTxn());
        }
    }

    private MatViewState viewState() {
        return engine.getMatViewStateStore().getViewState(viewToken());
    }

    private TableToken viewToken() {
        return engine.verifyTableName("mv");
    }

    enum RefreshKind {
        USER_RANGE, INCREMENTAL, FULL, PERIOD
    }
}
