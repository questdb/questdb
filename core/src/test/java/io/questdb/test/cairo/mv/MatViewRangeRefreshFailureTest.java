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

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.mv.MatViewRefreshJob;
import io.questdb.cairo.mv.MatViewRefreshTask;
import io.questdb.cairo.mv.MatViewState;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * A user's {@code REFRESH ... RANGE} that fails leaves the view valid, with its rows and refresh position, and
 * {@code materialized_views().last_refresh_error} reports its error until the next user RANGE succeeds. The error
 * lives in memory only. A missing or non-WAL base still invalidates the view, since the view cannot work then.
 * {@link MatViewRefreshFailureSiteTest} covers each failure site of the refresh's insert-as-select.
 * <p>
 * Expected rows come from the base rows each test inserts: a bucket of the 1h view holds the last price of its
 * symbol's base rows within the hour.
 */
public class MatViewRangeRefreshFailureTest extends AbstractCairoTest {
    private static final String RANGE_SQL = "REFRESH MATERIALIZED VIEW price_1h RANGE FROM '2024-01-01' TO '2024-01-02'";
    private static final String VIEW_ROWS = """
            ts\tsym\tprice
            2024-01-01T00:00:00.000000Z\ta\t1.0
            2024-01-01T01:00:00.000000Z\tb\t2.0
            """;
    private static final String VIEW_SQL = "SELECT ts, sym, last(price) AS price FROM base SAMPLE BY 1h";
    // the error of a compile of VIEW_SQL once the base has no price column
    private static final String DROPPED_PRICE_ERROR = "[" + VIEW_SQL.indexOf("price") + "]: Invalid column: price";

    @Test
    public void testCompileErrorWithoutCachedPlanKeepsViewValid() throws Exception {
        // After a restart the view has no compiled plan, so the RANGE compiles the view's SQL before it looks for the
        // range's rows, and fails there.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("MANUAL");
            restart();
            execute("ALTER TABLE base DROP COLUMN price");
            drainWalQueue();

            final AtomicInteger insertAsSelectCompileCount = new AtomicInteger();
            final long viewSeqTxn = viewSeqTxn();
            execute(RANGE_SQL);
            try (MatViewRefreshJob job = createMatViewRefreshJob()) {
                job.setOnBeforeCompileForTesting(insertAsSelectCompileCount::incrementAndGet);
                drainMatViewQueue(job);
            }
            drainWalQueue();
            Assert.assertEquals("the compile ran before the insert-as-select", 0, insertAsSelectCompileCount.get());
            assertViewState("valid", "", DROPPED_PRICE_ERROR);
            Assert.assertEquals(viewSeqTxn, viewSeqTxn());
            assertViewRows(VIEW_ROWS);
        });
    }

    @Test
    public void testEmptyRangeClearsError() throws Exception {
        // A RANGE over a range without base rows has nothing to recompute; it succeeds all the same.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("MANUAL");
            execute("ALTER TABLE base ALTER COLUMN price TYPE VARCHAR");
            execute("INSERT INTO base VALUES ('a', 'abc', 0.0, '2024-01-01T02:10:00.000000Z')");
            drainWalQueue();
            execute(RANGE_SQL);
            drainWalAndMatViewQueues();
            assertViewState("valid", "", "inconvertible value: `abc` [VARCHAR -> DOUBLE] [from=price, to=price]");

            final long viewSeqTxn = viewSeqTxn();
            execute("REFRESH MATERIALIZED VIEW price_1h RANGE FROM '2023-01-01' TO '2023-01-02'");
            drainWalAndMatViewQueues();
            assertViewState("valid", "", null);
            Assert.assertEquals(viewSeqTxn, viewSeqTxn());
            assertViewRows(VIEW_ROWS);
        });
    }

    @Test
    public void testErrorIsKeptInMemoryOnly() throws Exception {
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            execute("ALTER TABLE base DROP COLUMN price");
            drainWalAndMatViewQueues();
            execute(RANGE_SQL);
            drainWalAndMatViewQueues();
            assertViewState("valid", "", DROPPED_PRICE_ERROR);

            restart();
            assertViewState("valid", "", null);
            assertViewRows(VIEW_ROWS);
        });
    }

    @Test
    public void testErrorIsNullWithoutViewState() throws Exception {
        // A node with materialized views disabled keeps no view state, so it knows no RANGE error.
        assertMemoryLeak(() -> {
            final String root = temp.newFolder().getAbsolutePath();
            try (CairoEngine first = new CairoEngine(new DefaultTestCairoConfiguration(root))) {
                first.load();
                first.execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                first.execute("CREATE MATERIALIZED VIEW price_1h AS (" + VIEW_SQL + ") PARTITION BY DAY");
                try (SqlExecutionContextImpl context = TestUtils.createSqlExecutionCtx(first, 1)) {
                    first.execute("INSERT INTO base VALUES ('a', 1.0, '2024-01-01T00:10:00.000000Z')", context);
                }
                drainWalAndMatViewQueues(first);
            }
            final DefaultTestCairoConfiguration matViewsDisabled = new DefaultTestCairoConfiguration(root) {
                @Override
                public boolean isMatViewEnabled() {
                    return false;
                }
            };
            try (
                    CairoEngine second = new CairoEngine(matViewsDisabled);
                    SqlExecutionContextImpl context = TestUtils.createSqlExecutionCtx(second, 1)
            ) {
                second.load();
                second.buildViewGraphs();
                Assert.assertNull(second.getMatViewStateStore().getViewState(second.verifyTableName("price_1h")));
                assertQuery("SELECT view_name, view_status, last_refresh_error, last_refresh_error IS NULL AS no_error FROM materialized_views()")
                        .withEngine(second)
                        .withContext(context)
                        .noLeakCheck()
                        .noRandomAccess()
                        .returns("view_name\tview_status\tlast_refresh_error\tno_error\nprice_1h\tvalid\t\ttrue\n");
            }
        });
    }

    @Test
    public void testExpiryPolicyConflictKeepsViewValid() throws Exception {
        // The view joins a materialized view that takes an EXPIRE ROWS policy, which the view's SQL cannot read
        // through. A user RANGE reports it and leaves the view valid; the incremental refresh that a base insert
        // triggers still invalidates the view with it.
        final String error = "cannot materialize view 'mv': source materialized view 'v' has an active EXPIRE ROWS policy";
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE base2 (sym SYMBOL, weight DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE MATERIALIZED VIEW v AS (SELECT * FROM base2)");
            execute("""
                    CREATE MATERIALIZED VIEW mv WITH BASE base AS
                    (SELECT base.ts, base.sym, last(base.price) AS price, first(v.weight) AS weight FROM base JOIN v ON (sym) SAMPLE BY 1h)""");
            execute("INSERT INTO base2 VALUES ('a', 0.5, '2024-01-01T00:00:00.000000Z')");
            drainWalAndMatViewQueues();
            execute("INSERT INTO base VALUES ('a', 1.0, '2024-01-01T00:10:00.000000Z')");
            drainWalAndMatViewQueues();
            final String rows = """
                    ts\tsym\tprice\tweight
                    2024-01-01T00:00:00.000000Z\ta\t1.0\t0.5
                    """;
            assertQuery("mv").timestamp("ts").noLeakCheck().expectSize().returns(rows);

            execute("ALTER MATERIALIZED VIEW v SET EXPIRE ROWS WHEN weight < 0.1");
            drainWalAndMatViewQueues();
            execute("REFRESH MATERIALIZED VIEW mv RANGE FROM '2024-01-01' TO '2024-01-02'");
            drainWalAndMatViewQueues();
            assertViewState("mv", "valid", "", error);
            assertQuery("mv").timestamp("ts").noLeakCheck().expectSize().returns(rows);

            execute("INSERT INTO base VALUES ('a', 2.0, '2024-01-01T00:20:00.000000Z')");
            drainWalAndMatViewQueues();
            assertViewState("mv", "invalid", error, error);
        });
    }

    @Test
    public void testFailedRangeAfterBaseDropColumnKeepsViewValid() throws Exception {
        // The base drops the column the view reads. The view stays valid, since it has nothing to recompute, and a
        // RANGE that recomputes rows fails to compile its SQL. Once the base has the column again, the next RANGE
        // succeeds and clears the error; it reads the re-added column, which holds no values for the old rows.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            execute("ALTER TABLE base DROP COLUMN price");
            drainWalAndMatViewQueues();
            assertViewState("valid", "", null);
            final long refreshBaseTxn = viewState().getLastRefreshBaseTxn();
            final long viewSeqTxn = viewSeqTxn();

            execute(RANGE_SQL);
            drainWalAndMatViewQueues();
            assertViewState("valid", "", DROPPED_PRICE_ERROR);
            assertViewRows(VIEW_ROWS);
            Assert.assertEquals(refreshBaseTxn, viewState().getLastRefreshBaseTxn());
            Assert.assertEquals(viewSeqTxn, viewSeqTxn());

            // a repeated RANGE fails the same way
            execute(RANGE_SQL);
            drainWalAndMatViewQueues();
            assertViewState("valid", "", DROPPED_PRICE_ERROR);

            execute("ALTER TABLE base ADD COLUMN price DOUBLE");
            drainWalAndMatViewQueues();
            execute(RANGE_SQL);
            drainWalAndMatViewQueues();
            // a RANGE records no refresh finish time, so the view reports it refreshing
            assertViewState("refreshing", "", null);
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\tnull
                    2024-01-01T01:00:00.000000Z\tb\tnull
                    """);
        });
    }

    @Test
    public void testMissingBaseInvalidates() throws Exception {
        // The base is dropped and its invalidation of the view is lost, as in a crash: the RANGE finds no base.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("MANUAL");
            dropBaseLosingInvalidation();
            execute(RANGE_SQL);
            drainWalAndMatViewQueues();
            assertViewState("invalid", "[" + CairoException.TABLE_DOES_NOT_EXIST + "]: table does not exist [table=base]", null);
            assertViewRows(VIEW_ROWS);
        });
    }

    @Test
    public void testNonWalBaseInvalidates() throws Exception {
        // The base is re-created as a non-WAL table and the drop's invalidation of the view is lost, as in a crash.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("MANUAL");
            dropBaseLosingInvalidation();
            execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute(RANGE_SQL);
            drainWalAndMatViewQueues();
            assertViewState("invalid", "base table is not a WAL table", null);
            assertViewRows(VIEW_ROWS);
        });
    }

    private void assertViewRows(String expected) throws Exception {
        assertQuery("price_1h ORDER BY ts, sym").timestamp("ts").noLeakCheck().expectSize().returns(expected);
    }

    private void assertViewState(String status, String reason, String error) throws Exception {
        assertViewState("price_1h", status, reason, error);
    }

    private void assertViewState(String viewName, String status, String reason, String error) throws Exception {
        assertQuery("SELECT view_status, invalidation_reason, last_refresh_error, last_refresh_error IS NULL AS no_error "
                + "FROM materialized_views() WHERE view_name = '" + viewName + "'")
                .noLeakCheck()
                .noRandomAccess()
                .returns("view_status\tinvalidation_reason\tlast_refresh_error\tno_error\n"
                        + status + '\t' + reason + '\t' + (error != null ? error : "") + '\t' + (error == null) + '\n');
        Assert.assertEquals(error, engine.getMatViewStateStore().getViewState(engine.verifyTableName(viewName)).getLastRefreshError());
    }

    private void createBaseWithRefreshedView(String refreshType) throws Exception {
        execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, amount DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("""
                INSERT INTO base VALUES
                ('a', 1.0, 1.0, '2024-01-01T00:10:00.000000Z'),
                ('b', 2.0, 2.0, '2024-01-01T01:10:00.000000Z')
                """);
        execute("CREATE MATERIALIZED VIEW price_1h REFRESH " + refreshType + " AS (" + VIEW_SQL + ") PARTITION BY DAY");
        if (!"IMMEDIATE".equals(refreshType)) {
            drainWalQueue();
            execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
        }
        drainWalAndMatViewQueues();
        assertViewRows(VIEW_ROWS);
        assertViewState("valid", "", null);
    }

    // Drops the base table and the invalidation of the view that the drop queued, as a crash would.
    private void dropBaseLosingInvalidation() throws SqlException {
        execute("DROP TABLE base");
        final MatViewRefreshTask task = new MatViewRefreshTask();
        Assert.assertTrue(engine.getMatViewStateStore().tryDequeueRefreshTask(task));
        Assert.assertEquals(MatViewRefreshTask.INVALIDATE, task.operation);
        Assert.assertFalse(engine.getMatViewStateStore().tryDequeueRefreshTask(task));
        Assert.assertFalse(viewState().isInvalid());
    }

    // A restart of the view states: the store reloads them from disk, as at startup.
    private void restart() {
        engine.getMatViewStateStore().clear();
        engine.hydrateMatViewStateStore();
    }

    private long viewSeqTxn() {
        return engine.getTableSequencerAPI().getTxnTracker(viewToken()).getSeqTxn();
    }

    private MatViewState viewState() {
        return engine.getMatViewStateStore().getViewState(viewToken());
    }

    private TableToken viewToken() {
        return engine.verifyTableName("price_1h");
    }
}
