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
import io.questdb.TelemetryJob;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.mv.MatViewBacklog;
import io.questdb.cairo.mv.MatViewBareNameGuard;
import io.questdb.cairo.mv.MatViewRefreshJob;
import io.questdb.cairo.mv.MatViewRefreshTask;
import io.questdb.cairo.mv.MatViewState;
import io.questdb.cairo.mv.MatViewStateStoreImpl;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.griffin.DefaultSqlExecutionCircuitBreakerConfiguration;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.SqlParser;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import io.questdb.std.Os;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * {@code REFRESH MATERIALIZED VIEW v RESUME}: an invalid view becomes valid again with its rows kept. In this
 * version RESUME jumps to the base table's latest applied transaction and clears the view's cached refresh
 * intervals; the rows the base received while the view was invalid reach it only through {@code REFRESH ... RANGE}.
 * The class also covers the FULL refresh guards that come with RESUME: the statement refuses a base the view cannot
 * refresh from, the refresh job checks the SQL and the base before it truncates, and a FULL on an invalid view waits
 * for the view's own WAL.
 * <p>
 * Expected rows come from the base rows each test inserts: a bucket of the 1h view holds the last price of its
 * symbol's base rows within the hour.
 */
public class MatViewResumeTest extends AbstractCairoTest {
    // the rows of the bare count view rebuilt while the base has a column named count that holds no value
    private static final String BARE_COUNT_VIEW_COLUMN_ROWS = """
            ts\tsym\tcount\tprice
            2024-01-01T00:00:00.000000Z\ta\tnull\t1.0
            2024-01-01T01:00:00.000000Z\tb\tnull\t2.0
            """;
    private static final String BARE_COUNT_VIEW_ROWS = """
            ts\tsym\tcount\tprice
            2024-01-01T00:00:00.000000Z\ta\t1\t1.0
            2024-01-01T01:00:00.000000Z\tb\t1\t2.0
            """;
    // the SQL of a view created before the explicit-name rule, which calls count without parentheses
    private static final String BARE_COUNT_VIEW_SQL = "SELECT ts, sym, count, last(price) AS price FROM base SAMPLE BY 1h";
    private static final String CALL_COUNT_VIEW_SQL = "SELECT ts, sym, count() AS count, last(price) AS price FROM base SAMPLE BY 1h";
    // the refusal of RESUME for a SQL that reads count as a column once the base dropped it
    private static final String COUNT_COMPILE_REFUSAL = "materialized view SQL does not compile, fix it with ALTER MATERIALIZED VIEW ... AS first "
            + "[view=price_1h, error=count is not a column; write count() to call the function]";
    // the rows of the view that reads the base column count bare, see createCountColumnBaseWithView()
    private static final String COUNT_COLUMN_VIEW_ROWS = """
            ts\tsym\tcount\tprice
            2024-01-01T00:00:00.000000Z\ta\t7\t1.0
            2024-01-01T01:00:00.000000Z\tb\t8\t2.0
            """;
    // two rows of the hour 02:00 for the base of the bare count view, once it has a column named count
    private static final String INSERT_C_ROWS = "INSERT INTO base (sym, price, amount, ts) VALUES "
            + "('c', 5.0, 5.0, '2024-01-01T02:10:00.000000Z'), ('c', 6.0, 6.0, '2024-01-01T02:20:00.000000Z')";
    // Math.PI as a view row prints it
    private static final String PI_TEXT = "3.141592653589793";
    private static final String PLAIN_RESUME_SQL = "REFRESH MATERIALIZED VIEW price_1h RESUME";
    private static final String RESUME_SQL = "REFRESH MATERIALIZED VIEW price_1h RESUME SKIP BACKLOG";
    private static final int VIEW_NAME_POSITION = "REFRESH MATERIALIZED VIEW ".length();
    private static final String VIEW_ROWS = """
            ts\tsym\tprice
            2024-01-01T00:00:00.000000Z\ta\t1.0
            2024-01-01T01:00:00.000000Z\tb\t2.0
            """;
    private static final String VIEW_SQL = "SELECT ts, sym, last(price) AS price FROM base SAMPLE BY 1h";

    @Test
    public void testBareCountRefusedWithParenthesesHint() throws Exception {
        // RESUME reads bare names as columns. A view created before the explicit-name rule that calls count without
        // parentheses refuses with the hint, also after it missed, while invalid, two base column changes that the
        // bare-name guard watches: a column renamed to count and dropped again.
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRows();
            SqlParser.setMatViewExplicitNamesCheckSkipped(true);
            try {
                execute("CREATE MATERIALIZED VIEW price_1h AS (SELECT ts, sym, count FROM base SAMPLE BY 1h) PARTITION BY DAY");
            } finally {
                SqlParser.setMatViewExplicitNamesCheckSkipped(false);
            }
            drainWalAndMatViewQueues();
            final String rows = """
                    ts\tsym\tcount
                    2024-01-01T00:00:00.000000Z\ta\t1
                    2024-01-01T01:00:00.000000Z\tb\t1
                    """;
            assertViewRows(rows);
            invalidate("test invalidation");

            assertExceptionNoLeakCheck(RESUME_SQL, VIEW_NAME_POSITION, "count is not a column; write count() to call the function");
            assertViewStatus("invalid", "test invalidation");

            execute("ALTER TABLE base RENAME COLUMN amount TO count");
            drainWalAndMatViewQueues();
            execute("ALTER TABLE base DROP COLUMN count");
            drainWalAndMatViewQueues();
            assertViewStatus("invalid", "test invalidation");
            assertExceptionNoLeakCheck(RESUME_SQL, VIEW_NAME_POSITION, "count is not a column; write count() to call the function");
            assertViewStatus("invalid", "test invalidation");
            assertViewRows(rows);
        });
    }

    @Test
    public void testBaseReplacedUnderValidViewInvalidatesAtBaseCommitRefresh() throws Exception {
        // The base table is dropped and re-created under the same name, and the invalidation the drop sends is lost,
        // as in a crash. The refresh that the new table's commit triggers finds another table id under the base's
        // name and invalidates the view without writing rows.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            final int oldBaseTableId = engine.verifyTableName("base").getTableId();
            Assert.assertEquals(oldBaseTableId, viewState().getBaseTableId());

            replaceBaseLosingInvalidation();
            final long viewSeqTxn = viewSeqTxn();
            drainWalAndMatViewQueues();

            assertViewStatus("invalid", MatViewRefreshJob.BASE_TABLE_CHANGED_REASON);
            assertViewRows(VIEW_ROWS);
            // the invalidation is the one view transaction the refresh wrote
            Assert.assertEquals(viewSeqTxn + 1, viewSeqTxn());
            Assert.assertEquals(oldBaseTableId, viewState().getBaseTableId());
        });
    }

    @Test
    public void testBaseReplacedUnderValidViewInvalidatesAtQueuedIncrementalRefresh() throws Exception {
        // As above, through a queued incremental refresh of the view: REFRESH ... INCREMENTAL of a MANUAL view.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("MANUAL");
            replaceBaseLosingInvalidation();
            // the base commit of a MANUAL view refreshes nothing
            drainWalAndMatViewQueues();
            Assert.assertFalse(viewState().isInvalid());
            final long viewSeqTxn = viewSeqTxn();
            execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
            drainWalAndMatViewQueues();

            assertViewStatus("invalid", MatViewRefreshJob.BASE_TABLE_CHANGED_REASON);
            assertViewRows(VIEW_ROWS);
            Assert.assertEquals(viewSeqTxn + 1, viewSeqTxn());

            // RESUME positions the view on the new table.
            assertResumeSucceeds();
            assertViewStatus("valid", "");
            Assert.assertEquals(engine.verifyTableName("base").getTableId(), viewState().getBaseTableId());
        });
    }

    @Test
    public void testBaseReplacementWhileResumeHoldsLatch() throws Exception {
        // The base table is dropped and re-created while RESUME holds the view latch. RESUME positions the view on the
        // table that has the base's name when it resolves it, the new one, and the drop's invalidation, which arrives
        // while RESUME holds the latch, invalidates the view again once RESUME returns. A second RESUME recovers it on
        // the new table.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            invalidate("test invalidation");
            viewState().setOnResumeHoldingLockForTesting(() -> {
                try {
                    execute("DROP TABLE base");
                    createBase();
                    execute("INSERT INTO base VALUES ('n', 5.0, 5.0, '2024-01-01T00:40:00.000000Z')");
                    drainWalQueue();
                    drainMatViewQueue(engine);
                } catch (SqlException e) {
                    throw new RuntimeException(e);
                }
            });
            execute(RESUME_SQL);
            Assert.assertEquals(engine.verifyTableName("base").getTableId(), viewState().getBaseTableId());
            drainWalAndMatViewQueues();
            assertViewStatus("invalid", "base table is dropped or renamed");

            assertResumeSucceeds();
            assertViewStatus("valid", "");
            Assert.assertEquals(engine.verifyTableName("base").getTableId(), viewState().getBaseTableId());
            assertViewRows(VIEW_ROWS);
            // the RANGE recomputes the buckets the new table holds rows in; the view keeps the old table's other rows
            execute("REFRESH MATERIALIZED VIEW price_1h RANGE FROM '2024-01-01' TO '2024-01-02'");
            drainWalAndMatViewQueues();
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\tn\t5.0
                    2024-01-01T01:00:00.000000Z\tb\t2.0
                    """);
        });
    }

    @Test
    public void testEnqueueFailureAfterStateWriteKeepsStatementSuccessful() throws Exception {
        // Once RESUME has persisted the view state, a failure to enqueue the IMMEDIATE view's incremental refresh is
        // logged: the statement succeeds and the view is valid.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            invalidate("test invalidation");
            final MatViewStateStoreImpl store = (MatViewStateStoreImpl) engine.getMatViewStateStore();
            final AtomicBoolean isFired = new AtomicBoolean();
            store.setOnTaskQueueAppendForTesting(() -> {
                if (isFired.compareAndSet(false, true)) {
                    throw new OutOfMemoryError("test enqueue failure");
                }
            });
            try {
                execute(RESUME_SQL);
            } finally {
                store.setOnTaskQueueAppendForTesting(null);
            }
            Assert.assertTrue(isFired.get());
            Assert.assertFalse(viewState().isInvalid());
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            assertViewRows(VIEW_ROWS);
        });
    }

    @Test
    public void testFullAfterBaseColumnTypeWithoutCastKeepsRows() throws Exception {
        // A base ALTER COLUMN TYPE to a type with no copier conversion to the view column fails FULL before its
        // truncate: the view keeps its rows and goes invalid with the cast error.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            execute("ALTER TABLE base ALTER COLUMN price TYPE BOOLEAN");
            drainWalAndMatViewQueues();
            execute("REFRESH MATERIALIZED VIEW price_1h FULL");
            drainWalAndMatViewQueues();
            assertViewStatus("invalid", "[-1]: inconvertible types: BOOLEAN -> DOUBLE [from=price, to=price]");
            assertViewRows(VIEW_ROWS);
        });
    }

    @Test
    public void testFullOnDroppedReadColumnKeepsRows() throws Exception {
        // FULL compiles the view's SQL before it truncates. A SQL that reads a dropped base column fails the compile,
        // so the view keeps its rows and goes invalid with the compile error.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            execute("ALTER TABLE base DROP COLUMN price");
            drainWalAndMatViewQueues();
            execute("REFRESH MATERIALIZED VIEW price_1h FULL");
            drainWalAndMatViewQueues();
            assertViewStatus("invalid", "[21]: Invalid column: price");
            assertViewRows(VIEW_ROWS);
        });
    }

    @Test
    public void testFullParkedOnUnappliedViewWalRunsOnceApplied() throws Exception {
        // FULL on an invalid view waits while the view's own WAL has unapplied transactions: the refresh job parks
        // its request, and the WAL apply job redelivers it once it has applied the view's WAL.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            execute("INSERT INTO base VALUES ('c', 3.0, 3.0, '2024-01-01T02:10:00.000000Z')");
            drainWalQueue();
            // the invalidation event stays in the view's WAL, unapplied
            engine.getMatViewStateStore().enqueueInvalidate(viewToken(), "test invalidation");
            drainMatViewQueue(engine);
            Assert.assertTrue(viewState().isInvalid());
            Assert.assertTrue(hasUnappliedViewWal());

            execute("REFRESH MATERIALIZED VIEW price_1h FULL");
            drainMatViewQueue(engine);
            // parked: still invalid, the request kept on the view
            Assert.assertTrue(viewState().isInvalid());
            Assert.assertTrue(viewState().hasPendingFullRefreshOwnerForTesting());

            drainWalQueue();
            Assert.assertFalse(hasUnappliedViewWal());
            drainWalAndMatViewQueues();
            Assert.assertFalse(viewState().hasPendingFullRefreshOwnerForTesting());
            assertViewStatus("valid", "");
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    2024-01-01T01:00:00.000000Z\tb\t2.0
                    2024-01-01T02:00:00.000000Z\tc\t3.0
                    """);
        });
    }

    @Test
    public void testFullParkedWhileRedeliveryArrivesUnderLatch() throws Exception {
        // The WAL apply job applies the view's WAL and redelivers the parked FULL while the parked FULL still holds
        // the view latch. The redelivered task cannot take the latch, and the parked FULL suppressed its own wake, so
        // the FULL checks the view's WAL again after its unlock and redelivers itself.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            engine.getMatViewStateStore().enqueueInvalidate(viewToken(), "test invalidation");
            drainMatViewQueue(engine);
            Assert.assertTrue(hasUnappliedViewWal());
            execute("REFRESH MATERIALIZED VIEW price_1h FULL");

            final AtomicBoolean isSeamFired = new AtomicBoolean();
            viewState().setOnBeforeUnlockForTesting(() -> {
                isSeamFired.set(true);
                // the apply job applies the view's WAL and redelivers the owner, and another refresh job takes the
                // redelivered task while the parked FULL still holds the latch
                drainWalQueue();
                try (MatViewRefreshJob otherJob = new MatViewRefreshJob(1, engine, 0)) {
                    otherJob.run();
                }
            });
            drainMatViewQueue(engine);
            Assert.assertTrue(isSeamFired.get());
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            assertViewRows(VIEW_ROWS);
        });
    }

    @Test
    public void testFullParkedOwnerClearedBeforeRecheckIsNotRedelivered() throws Exception {
        // The parked FULL checks the view's WAL again after its unlock, and redelivers only an owner that is still
        // pending: one that something else consumed in between stays consumed.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            engine.getMatViewStateStore().enqueueInvalidate(viewToken(), "test invalidation");
            drainMatViewQueue(engine);
            execute("REFRESH MATERIALIZED VIEW price_1h FULL");
            viewState().setOnBeforeUnlockForTesting(() -> {
                drainWalQueue();
                viewState().clearPendingInvalidationForTesting();
            });
            drainMatViewQueue(engine);
            Assert.assertFalse(hasUnappliedViewWal());
            Assert.assertFalse(viewState().hasPendingFullRefreshOwnerForTesting());
            drainWalAndMatViewQueues();
            assertViewStatus("invalid", "test invalidation");
            assertViewRows(VIEW_ROWS);
        });
    }

    @Test
    public void testFullParkedRedeliveryEnqueueFailureRetries() throws Exception {
        // When the parked FULL cannot enqueue its own redelivery after the unlock, the store's retry scan delivers it.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            engine.getMatViewStateStore().enqueueInvalidate(viewToken(), "test invalidation");
            drainMatViewQueue(engine);
            execute("REFRESH MATERIALIZED VIEW price_1h FULL");
            final MatViewStateStoreImpl store = (MatViewStateStoreImpl) engine.getMatViewStateStore();
            final AtomicBoolean isFailed = new AtomicBoolean();
            viewState().setOnBeforeUnlockForTesting(() -> {
                // the WAL apply job applies the view's WAL; its own redelivery of the owner is lost, as in a crash
                drainWalQueue();
                final MatViewRefreshTask task = new MatViewRefreshTask();
                while (store.tryDequeueRefreshTask(task)) {
                    Assert.assertEquals(MatViewRefreshTask.FULL_REFRESH, task.operation);
                }
                store.setOnTaskQueueAppendForTesting(() -> {
                    if (isFailed.compareAndSet(false, true)) {
                        throw new OutOfMemoryError("test enqueue failure");
                    }
                });
            });
            try {
                drainMatViewQueue(engine);
            } finally {
                store.setOnTaskQueueAppendForTesting(null);
            }
            Assert.assertTrue(isFailed.get());
            Assert.assertTrue(viewState().hasPendingFullRefreshOwnerForTesting());
            drainWalAndMatViewQueues();
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            Assert.assertFalse(viewState().hasPendingFullRefreshOwnerForTesting());
        });
    }

    @Test
    public void testFullParkedWalApplyRedeliveryFailureRetries() throws Exception {
        // When the WAL apply job cannot redeliver the parked FULL, the refresh job's retry scan delivers it.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            engine.getMatViewStateStore().enqueueInvalidate(viewToken(), "test invalidation");
            drainMatViewQueue(engine);
            execute("REFRESH MATERIALIZED VIEW price_1h FULL");
            drainMatViewQueue(engine);
            Assert.assertTrue(viewState().hasPendingFullRefreshOwnerForTesting());

            final MatViewStateStoreImpl store = (MatViewStateStoreImpl) engine.getMatViewStateStore();
            final AtomicBoolean isFailed = new AtomicBoolean();
            store.setOnTaskQueueAppendForTesting(() -> {
                if (isFailed.compareAndSet(false, true)) {
                    throw new OutOfMemoryError("test enqueue failure");
                }
            });
            try {
                drainWalQueue();
            } finally {
                store.setOnTaskQueueAppendForTesting(null);
            }
            Assert.assertTrue(isFailed.get());
            Assert.assertTrue(viewState().isInvalid());
            drainWalAndMatViewQueues();
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            Assert.assertFalse(viewState().hasPendingFullRefreshOwnerForTesting());
        });
    }

    @Test
    public void testFullRefusesNonWalBaseAtStatement() throws Exception {
        // FULL refuses, at the view name, a base the view cannot refresh from; the view keeps its rows.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            execute("DROP TABLE base");
            execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, amount DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            drainWalAndMatViewQueues();
            assertExceptionNoLeakCheck(
                    "REFRESH MATERIALIZED VIEW price_1h FULL",
                    VIEW_NAME_POSITION,
                    "base table is not a WAL table or a materialized view [view=price_1h, baseTable=base]"
            );
            assertExceptionNoLeakCheck(RESUME_SQL, VIEW_NAME_POSITION, "base table is not a WAL table or a materialized view [view=price_1h, baseTable=base]");
            drainWalAndMatViewQueues();
            assertViewStatus("invalid", "base table is dropped or renamed");
            assertViewRows(VIEW_ROWS);
        });
    }

    @Test
    public void testFullRefusesOtherTimestampTypeAtStatement() throws Exception {
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            execute("DROP TABLE base");
            execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, amount DOUBLE, ts TIMESTAMP_NS) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalAndMatViewQueues();
            final String message = "base table designated timestamp type does not match the view's [view=price_1h, baseTable=base, viewType=TIMESTAMP, baseTableType=TIMESTAMP_NS]";
            assertExceptionNoLeakCheck("REFRESH MATERIALIZED VIEW price_1h FULL", VIEW_NAME_POSITION, message);
            assertExceptionNoLeakCheck(RESUME_SQL, VIEW_NAME_POSITION, message);
            assertViewStatus("invalid", "base table is dropped or renamed");
            assertViewRows(VIEW_ROWS);
        });
    }

    @Test
    public void testFullRefreshJobRefusesBaseItCannotRefreshFrom() throws Exception {
        // The FULL refresh job checks the base again before it truncates. A FULL that reaches the job past the
        // statement, as one queued before the base changed, fails the refresh with the view's rows in place.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            execute("DROP TABLE base");
            execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, amount DOUBLE, ts TIMESTAMP_NS) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalAndMatViewQueues();
            engine.getMatViewStateStore().enqueueFullRefresh(viewToken());
            drainWalAndMatViewQueues();
            assertViewStatus("invalid", "base table designated timestamp type does not match the view's [view=price_1h, baseTable=base, "
                    + "viewType=TIMESTAMP, baseTableType=TIMESTAMP_NS]; move the view to another base table with ALTER MATERIALIZED VIEW ... "
                    + "WITH BASE, or re-create the view");
            assertViewRows(VIEW_ROWS);

            execute("DROP TABLE base");
            execute("CREATE TABLE other (sym SYMBOL, price DOUBLE, amount DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE VIEW base AS (SELECT * FROM other)");
            drainWalAndViewQueues();
            final String kindRefusal = "base table is not a WAL table or a materialized view [view=price_1h, baseTable=base]";
            assertExceptionNoLeakCheck("REFRESH MATERIALIZED VIEW price_1h FULL", VIEW_NAME_POSITION, kindRefusal);
            assertExceptionNoLeakCheck(RESUME_SQL, VIEW_NAME_POSITION, kindRefusal);
            engine.getMatViewStateStore().enqueueFullRefresh(viewToken());
            drainWalAndMatViewQueues();
            assertViewStatus("invalid", kindRefusal + "; move the view to another base table with ALTER MATERIALIZED VIEW ... WITH BASE, or re-create the view");
            assertViewRows(VIEW_ROWS);
        });
    }

    @Test
    public void testFullAndResumeRefuseLiveViewBase() throws Exception {
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            execute("DROP TABLE base");
            execute("CREATE TABLE other (sym SYMBOL, price DOUBLE, amount DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW base FLUSH EVERY 100ms START FROM NOW AS SELECT ts, sym, price, amount, "
                    + "count(*) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM other");
            drainWalAndMatViewQueues();
            final String kindRefusal = "base table is not a WAL table or a materialized view [view=price_1h, baseTable=base]";
            assertExceptionNoLeakCheck("REFRESH MATERIALIZED VIEW price_1h FULL", VIEW_NAME_POSITION, kindRefusal);
            assertViewRows(VIEW_ROWS);
        });
    }

    @Test
    public void testFullRequestWhileResumeHoldsLatch() throws Exception {
        // A FULL refresh requested while RESUME holds the latch makes RESUME refuse: FULL recovers the view, and the
        // release of the latch delivers it.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            invalidate("test invalidation");
            execute("INSERT INTO base VALUES ('c', 3.0, 3.0, '2024-01-01T02:10:00.000000Z')");
            drainWalQueue();
            viewState().setOnResumeHoldingLockForTesting(() -> {
                try {
                    execute("REFRESH MATERIALIZED VIEW price_1h FULL");
                } catch (SqlException e) {
                    throw new RuntimeException(e);
                }
                drainMatViewQueue(engine);
            });
            assertExceptionNoLeakCheck(RESUME_SQL, VIEW_NAME_POSITION, "materialized view full refresh is pending, it recovers the view [view=price_1h]");
            Assert.assertFalse(viewState().isLocked());
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    2024-01-01T01:00:00.000000Z\tb\t2.0
                    2024-01-01T02:00:00.000000Z\tc\t3.0
                    """);
        });
    }

    @Test
    public void testIncrementalAndRangeRefusedOnInvalidView() throws Exception {
        // INCREMENTAL and RANGE on an invalid view fail at the view name; FULL and STATS keep working.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("MANUAL");
            invalidate("test invalidation");
            final String message = "materialized view is invalid, run REFRESH MATERIALIZED VIEW price_1h RESUME first [reason=test invalidation]";
            assertExceptionNoLeakCheck("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL", VIEW_NAME_POSITION, message);
            assertExceptionNoLeakCheck("REFRESH MATERIALIZED VIEW price_1h RANGE FROM '2024-01-01' TO '2024-01-02'", VIEW_NAME_POSITION, message);
            execute("REFRESH MATERIALIZED VIEW price_1h STATS");
            execute("REFRESH MATERIALIZED VIEW price_1h FULL");
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
            drainWalAndMatViewQueues();
            assertViewRows(VIEW_ROWS);
        });
    }

    @Test
    public void testInvalidationWhileResumeHoldsLatchInvalidatesAgain() throws Exception {
        // An invalidation published while RESUME holds the latch is kept, and invalidates the view again once RESUME
        // has made it valid.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            invalidate("test invalidation");
            viewState().setOnResumeHoldingLockForTesting(() -> {
                engine.getMatViewStateStore().enqueueInvalidate(viewToken(), "late invalidation");
                drainMatViewQueue(engine);
                Assert.assertEquals("late invalidation", viewState().getPendingInvalidationReason());
            });
            execute(RESUME_SQL);
            drainWalAndMatViewQueues();
            assertViewStatus("invalid", "late invalidation");
            Assert.assertFalse(viewState().hasPendingInvalidationReason());
            assertViewRows(VIEW_ROWS);
        });
    }

    @Test
    public void testInvalidationWhileRefusedResumeHoldsLatchIsDropped() throws Exception {
        // A RESUME that refuses drops the invalidations published while it held the latch: the view stays invalid
        // with its reason, and no pending reason is left.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            invalidate("test invalidation");
            viewState().setOnResumeHoldingLockForTesting(() -> {
                engine.getMatViewStateStore().enqueueInvalidate(viewToken(), "late invalidation");
                drainMatViewQueue(engine);
                Assert.assertEquals("late invalidation", viewState().getPendingInvalidationReason());
                try {
                    // the compile that follows fails, so RESUME refuses
                    execute("ALTER TABLE base DROP COLUMN price");
                } catch (SqlException e) {
                    throw new RuntimeException(e);
                }
                drainWalQueue();
            });
            assertExceptionNoLeakCheck(RESUME_SQL, VIEW_NAME_POSITION, "materialized view SQL does not compile, fix it with ALTER MATERIALIZED VIEW ... AS first [view=price_1h, error=Invalid column: price]");
            Assert.assertFalse(viewState().hasPendingInvalidationReason());
            Assert.assertFalse(viewState().isResuming());
            drainWalAndMatViewQueues();
            assertViewStatus("invalid", "test invalidation");
            assertViewRows(VIEW_ROWS);
        });
    }

    @Test
    public void testInvalidationKeptByFailedFullDoesNotInvalidateResumedView() throws Exception {
        // A FULL refresh recovers an invalid view, so an invalidation that reaches the view while the FULL holds the
        // latch stays pending. Here the base table's drop sends it, and the FULL then fails on the missing base, which
        // leaves the view invalid. RESUME, once the base is re-created, resolves the new base after that drop and
        // writes the view's state from it, so the drop's invalidation predates everything RESUME read, as an
        // invalidation published before a FULL fixes its base reader predates the rebuild, which consumes it. The
        // resumed view stays valid and follows the new base.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            invalidate("test invalidation");
            execute("REFRESH MATERIALIZED VIEW price_1h FULL");
            execute("DROP TABLE base");
            final AtomicBoolean isPublishedUnderFull = new AtomicBoolean();
            final AtomicBoolean hasFullFailed = new AtomicBoolean();
            try (
                    MatViewRefreshJob fullJob = new MatViewRefreshJob(0, engine, 0);
                    MatViewRefreshJob invalidationJob = new MatViewRefreshJob(1, engine, 0)
            ) {
                invalidationJob.setOnInvalidationPublishedForTesting(() -> isPublishedUnderFull.set(viewState().isLocked()));
                // the FULL found the base missing and holds the latch: the drop's invalidation arrives now
                fullJob.setOnFullRefreshTerminalFailureForTesting(() -> {
                    hasFullFailed.set(true);
                    drainMatViewQueue(invalidationJob);
                });
                drainWalQueue();
                drainMatViewQueue(fullJob);
            }
            Assert.assertTrue("the FULL must fail on the missing base", hasFullFailed.get());
            Assert.assertTrue("the drop's invalidation must reach the view while the FULL holds the latch", isPublishedUnderFull.get());
            Assert.assertFalse(viewState().isLocked());
            drainWalAndMatViewQueues();
            assertViewStatus("invalid", "[-105]: table does not exist [table=base]");
            assertViewRows(VIEW_ROWS);

            createBase();
            execute("INSERT INTO base VALUES ('c', 3.0, 3.0, '2024-01-01T02:10:00.000000Z')");
            drainWalAndMatViewQueues();
            assertViewStatus("invalid", "[-105]: table does not exist [table=base]");

            assertResumeSucceeds();
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            Assert.assertFalse(viewState().hasPendingInvalidationReason());
            Assert.assertEquals(engine.verifyTableName("base").getTableId(), viewState().getBaseTableId());
            // RESUME SKIP BACKLOG jumped past c's row; the next base row reaches the view
            execute("INSERT INTO base VALUES ('d', 4.0, 4.0, '2024-01-01T03:10:00.000000Z')");
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            assertViewRows(VIEW_ROWS + "2024-01-01T03:00:00.000000Z\td\t4.0\n");
        });
    }

    @Test
    public void testResumeDropsEarlierGuardInvalidationOfChangeAppliedBeforeLatch() throws Exception {
        // A bare-name guard invalidation that was pending when RESUME took the latch, as a FULL refresh that then failed
        // leaves it, stands for a base column change, here the ADD COLUMN count that turned the bare count of the view's
        // SQL from a call into a column. ALTER ... AS has made the SQL call count() since, so the change switches no
        // name of the SQL that RESUME compiles. The change had applied when RESUME took the latch, so RESUME's compile
        // saw it: RESUME read past the invalidation, and the resumed view stays valid.
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRows();
            createBareCountView();
            invalidate("test invalidation");
            execute("ALTER TABLE base ADD COLUMN count LONG");
            drainWalQueue();
            final TableToken baseToken = engine.verifyTableName("base");
            // the base's insert is txn 1, the ADD COLUMN txn 2
            Assert.assertEquals(2, appliedTxn(baseToken));
            execute("ALTER MATERIALIZED VIEW price_1h AS (" + CALL_COUNT_VIEW_SQL + ")");
            drainWalQueue();
            viewState().markAsPendingInvalidationForTesting(MatViewBareNameGuard.invalidationReason("count"), baseToken, 2, false);
            Assert.assertTrue(viewState().hasPendingInvalidationBaseTableChangeForTesting());

            assertResumeSucceeds();
            Assert.assertFalse(viewState().hasPendingInvalidationReason());
            Assert.assertFalse(viewState().hasPendingInvalidationBaseTableChangeForTesting());
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            // the view follows the base and counts the rows of the new bucket, whatever the column holds
            execute("INSERT INTO base (sym, price, amount, ts, count) VALUES ('c', 3.0, 3.0, '2024-01-01T02:10:00.000000Z', 7)");
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            assertViewRows(BARE_COUNT_VIEW_ROWS + "2024-01-01T02:00:00.000000Z\tc\t1\t3.0\n");
        });
    }

    @Test
    public void testResumeDropsEarlierGuardInvalidationOfReplacedBase() throws Exception {
        // A bare-name guard invalidation that was pending when RESUME took the latch stands for a column change of a
        // table that the view no longer follows: the base was dropped, and while RESUME holds the latch a new table
        // takes its name, which RESUME positions the view on. RESUME read past the invalidation, whether or not the
        // change applied, and the resumed view stays valid.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            final TableToken oldBaseToken = engine.verifyTableName("base");
            execute("DROP TABLE base");
            drainWalAndMatViewQueues();
            assertViewStatus("invalid", "base table is dropped or renamed");
            // the dropped table applied its txn 1 only
            viewState().markAsPendingInvalidationForTesting(MatViewBareNameGuard.invalidationReason("version"), oldBaseToken, 2, false);
            viewState().setOnResumeHoldingLockForTesting(() -> {
                try {
                    createBase();
                    execute("INSERT INTO base VALUES ('c', 3.0, 3.0, '2024-01-01T02:10:00.000000Z')");
                    drainWalQueue();
                } catch (SqlException e) {
                    throw new RuntimeException(e);
                }
            });

            assertResumeSucceeds();
            Assert.assertFalse(viewState().hasPendingInvalidationReason());
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            Assert.assertEquals(engine.verifyTableName("base").getTableId(), viewState().getBaseTableId());
            execute("INSERT INTO base VALUES ('d', 4.0, 4.0, '2024-01-01T03:10:00.000000Z')");
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            assertViewRows(VIEW_ROWS + "2024-01-01T03:00:00.000000Z\td\t4.0\n");
        });
    }

    @Test
    public void testResumeKeepsEarlierGuardInvalidationOfChangeNotAppliedAtLatch() throws Exception {
        // A bare-name guard invalidation that was pending when RESUME took the latch stands for a DROP of the base
        // column version, which the view's SQL reads bare. The guard publishes before the change applies, and here the
        // DROP had not applied when RESUME took the latch, so RESUME's compile still read version as the column, while
        // the view's refreshes read it as a call of version() once the DROP applies. The invalidation stays pending, and
        // invalidates the resumed view again; the view keeps its rows.
        assertMemoryLeak(() -> {
            createBase();
            execute("ALTER TABLE base ADD COLUMN version INT");
            execute("""
                    INSERT INTO base (sym, price, amount, version, ts) VALUES
                    ('a', 1.0, 1.0, 1, '2024-01-01T00:10:00.000000Z'),
                    ('b', 2.0, 2.0, 2, '2024-01-01T01:10:00.000000Z')
                    """);
            drainWalQueue();
            execute("CREATE MATERIALIZED VIEW price_1h AS (SELECT ts, sym, version, last(price) AS price FROM base SAMPLE BY 1h) PARTITION BY DAY");
            drainWalAndMatViewQueues();
            final String rows = """
                    ts\tsym\tversion\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1\t1.0
                    2024-01-01T01:00:00.000000Z\tb\t2\t2.0
                    """;
            assertViewRows(rows);
            invalidate("test invalidation");
            execute("ALTER TABLE base DROP COLUMN version");
            final TableToken baseToken = engine.verifyTableName("base");
            // the ADD COLUMN is txn 1, the insert txn 2 and the DROP, not applied yet, txn 3
            Assert.assertEquals(2, appliedTxn(baseToken));
            final String reason = MatViewBareNameGuard.invalidationReason("version");
            viewState().markAsPendingInvalidationForTesting(reason, baseToken, 3, false);

            execute(RESUME_SQL);
            Assert.assertFalse(viewState().isInvalid());
            Assert.assertEquals(reason, viewState().getPendingInvalidationReason());
            Assert.assertEquals(3, viewState().getPendingInvalidationBaseTxnForTesting());
            drainWalAndMatViewQueues();
            assertViewStatus("invalid", reason);
            Assert.assertFalse(viewState().hasPendingInvalidationReason());
            assertViewRows(rows);
        });
    }

    @Test
    public void testResumeKeepsEarlierGuardInvalidationThroughFailedFull() throws Exception {
        // A bare-name guard invalidation pending on an invalid view stays what it is while a FULL refresh is requested
        // on top of it and then fails: the FULL's compile fails on a dropped base column, so the FULL consumes nothing,
        // and its owner leaves the marker. The user re-adds the column and resumes the view. The change the guard
        // invalidation stands for has not applied when RESUME takes the latch, so it stays pending and invalidates the
        // resumed view again; the view keeps its rows throughout.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            invalidate("test invalidation");
            final TableToken baseToken = engine.verifyTableName("base");
            // the insert is the base's txn 1; the DROP and ADD COLUMN below are txns 2 and 3, and the change of the
            // guard invalidation txn 4, which has not applied when RESUME takes the latch
            final String reason = MatViewBareNameGuard.invalidationReason("version");
            viewState().markAsPendingInvalidationForTesting(reason, baseToken, 4, false);
            execute("ALTER TABLE base DROP COLUMN price");
            drainWalQueue();
            // the refresh job publishes the FULL's owner on the guard invalidation's marker, and clears it once the
            // FULL has failed
            execute("REFRESH MATERIALIZED VIEW price_1h FULL");
            drainMatViewQueue(engine);
            Assert.assertFalse(viewState().hasPendingFullRefreshOwnerForTesting());
            Assert.assertEquals(reason, viewState().getPendingInvalidationReason());
            Assert.assertEquals(4, viewState().getPendingInvalidationBaseTxnForTesting());
            drainWalAndMatViewQueues();
            assertViewStatus("invalid", "[" + VIEW_SQL.indexOf("price") + "]: Invalid column: price");
            assertViewRows(VIEW_ROWS);
            execute("ALTER TABLE base ADD COLUMN price DOUBLE");
            drainWalQueue();
            Assert.assertEquals(3, appliedTxn(baseToken));

            execute(RESUME_SQL);
            Assert.assertFalse(viewState().isInvalid());
            Assert.assertEquals(reason, viewState().getPendingInvalidationReason());
            Assert.assertEquals(4, viewState().getPendingInvalidationBaseTxnForTesting());
            drainWalAndMatViewQueues();
            assertViewStatus("invalid", reason);
            Assert.assertFalse(viewState().hasPendingInvalidationReason());
            assertViewRows(VIEW_ROWS);
        });
    }

    @Test
    public void testResumeKeepsEarlierGuardInvalidationOfTableRenamedToBaseUnderLatch() throws Exception {
        // A bare-name guard invalidation that was pending when RESUME took the latch stands for a column change of
        // another table, which, while RESUME holds the latch, takes the base table's name after a drop of the base.
        // RESUME positions the view on that table. The change's txn is below the transaction the dropped base had
        // applied, but that transaction says nothing about the other table, so RESUME cannot tell that its compile saw
        // the change: the invalidation stays pending. The drop's invalidation, delivered after RESUME, invalidates the
        // view again.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            execute("INSERT INTO base VALUES ('c', 3.0, 3.0, '2024-01-01T02:10:00.000000Z')");
            execute("CREATE TABLE other (sym SYMBOL, price DOUBLE, amount DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO other VALUES ('o', 9.0, 9.0, '2024-01-01T00:30:00.000000Z')");
            drainWalAndMatViewQueues();
            invalidate("test invalidation");
            final TableToken baseToken = engine.verifyTableName("base");
            final TableToken otherToken = engine.verifyTableName("other");
            Assert.assertEquals(2, appliedTxn(baseToken));
            Assert.assertEquals(1, appliedTxn(otherToken));
            final String reason = MatViewBareNameGuard.invalidationReason("version");
            viewState().markAsPendingInvalidationForTesting(reason, otherToken, 1, false);
            viewState().setOnResumeHoldingLockForTesting(() -> {
                try {
                    execute("DROP TABLE base");
                    execute("RENAME TABLE other TO base");
                    drainWalQueue();
                } catch (SqlException e) {
                    throw new RuntimeException(e);
                }
            });

            execute(RESUME_SQL);
            Assert.assertFalse(viewState().isInvalid());
            Assert.assertEquals(otherToken.getTableId(), viewState().getBaseTableId());
            Assert.assertEquals(reason, viewState().getPendingInvalidationReason());
            Assert.assertEquals(1, viewState().getPendingInvalidationBaseTxnForTesting());
            drainWalAndMatViewQueues();
            assertViewStatus("invalid", "base table is dropped or renamed");
            Assert.assertFalse(viewState().hasPendingInvalidationReason());
            assertViewRows(VIEW_ROWS + "2024-01-01T02:00:00.000000Z\tc\t3.0\n");
        });
    }

    @Test
    public void testResumeKeepsEarlierInvalidationThatLostItsBaseTableChange() throws Exception {
        // A bare-name guard invalidation for a base column change that had applied, which RESUME would read past, merged
        // with a later invalidation without a base table change before RESUME took the latch. The merge keeps the later
        // reason and loses the change's table and txn, so RESUME cannot tell that its compile saw the change: the
        // invalidation stays pending, and invalidates the resumed view again.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            invalidate("test invalidation");
            final TableToken baseToken = engine.verifyTableName("base");
            Assert.assertEquals(1, appliedTxn(baseToken));
            viewState().markAsPendingInvalidationForTesting(MatViewBareNameGuard.invalidationReason("version"), baseToken, 1, false);
            Assert.assertTrue(viewState().hasPendingInvalidationBaseTableChangeForTesting());
            viewState().markAsPendingInvalidation("forced invalidation");
            Assert.assertNull(viewState().getPendingInvalidationBaseTableTokenForTesting());
            Assert.assertTrue(viewState().hasPendingInvalidationBaseTableChangeForTesting());

            execute(RESUME_SQL);
            Assert.assertFalse(viewState().isInvalid());
            Assert.assertEquals("forced invalidation", viewState().getPendingInvalidationReason());
            drainWalAndMatViewQueues();
            assertViewStatus("invalid", "forced invalidation");
            Assert.assertFalse(viewState().hasPendingInvalidationReason());
            assertViewRows(VIEW_ROWS);
        });
    }

    @Test
    public void testResumeKeepsInvalidationPublishedAfterEarlierOne() throws Exception {
        // RESUME read past the invalidation that was pending when it took the latch, but not past one published while
        // it holds the latch, which replaces the earlier one: the later invalidation stays pending, and invalidates
        // the resumed view again.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            invalidate("test invalidation");
            viewState().markAsPendingInvalidation("earlier invalidation");
            viewState().setOnResumeHoldingLockForTesting(() -> {
                engine.getMatViewStateStore().enqueueInvalidate(viewToken(), "late invalidation");
                drainMatViewQueue(engine);
                Assert.assertEquals("late invalidation", viewState().getPendingInvalidationReason());
                Assert.assertFalse(viewState().hasPendingInvalidationBaseTableChangeForTesting());
            });

            execute(RESUME_SQL);
            Assert.assertFalse(viewState().isInvalid());
            Assert.assertEquals("late invalidation", viewState().getPendingInvalidationReason());
            drainWalAndMatViewQueues();
            assertViewStatus("invalid", "late invalidation");
            Assert.assertFalse(viewState().hasPendingInvalidationReason());
            assertViewRows(VIEW_ROWS);
        });
    }

    @Test
    public void testResumeWaitingAgainForViewWalKeepsInvalidationForNextPass() throws Exception {
        // An invalidation published while RESUME holds the latch stays pending when RESUME releases the latch to wait
        // for a view WAL transaction that arrived meanwhile: RESUME's next pass takes the latch with it pending. Here
        // it is one that RESUME cannot read past, a bare-name guard invalidation merged with a later one, so it
        // invalidates the view again once RESUME's next pass has made the view valid.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            invalidate("test invalidation");
            final MatViewState state = viewState();
            final TableToken baseToken = engine.verifyTableName("base");
            final CountDownLatch applyGate = new CountDownLatch(1);
            final AtomicReference<Throwable> applierError = new AtomicReference<>();
            final Thread applier = new Thread(() -> {
                try {
                    applyGate.await();
                    drainWalQueue();
                } catch (Throwable th) {
                    applierError.set(th);
                } finally {
                    Path.clearThreadLocals();
                }
            });
            final AtomicBoolean isReleasedForWait = new AtomicBoolean();
            state.setOnResumeHoldingLockForTesting(() -> {
                state.markAsPendingInvalidationForTesting(MatViewBareNameGuard.invalidationReason("version"), baseToken, 1, false);
                state.markAsPendingInvalidation("late invalidation");
                try {
                    execute("ALTER MATERIALIZED VIEW price_1h ADD COLUMN extra DOUBLE");
                } catch (SqlException e) {
                    throw new RuntimeException(e);
                }
                // RESUME finds the view's new transaction unapplied and releases the latch to wait for it
                state.setOnBeforeUnlockForTesting(() -> {
                    isReleasedForWait.set(true);
                    applyGate.countDown();
                });
            });
            applier.start();
            try {
                execute(RESUME_SQL);
            } finally {
                applyGate.countDown();
                applier.join();
            }
            Assert.assertNull(applierError.get());
            Assert.assertTrue(isReleasedForWait.get());
            Assert.assertFalse(state.isInvalid());
            Assert.assertEquals("late invalidation", state.getPendingInvalidationReason());
            drainWalAndMatViewQueues();
            assertViewStatus("invalid", "late invalidation");
            Assert.assertFalse(state.hasPendingInvalidationReason());
            assertViewRows("""
                    ts\tsym\tprice\textra
                    2024-01-01T00:00:00.000000Z\ta\t1.0\tnull
                    2024-01-01T01:00:00.000000Z\tb\t2.0\tnull
                    """);
        });
    }

    @Test
    public void testBareNameGuardWhileResumeHoldsLatchInvalidatesAgain() throws Exception {
        // The bare-name guard's invalidation counts too: a base ADD COLUMN count that applies while RESUME holds the
        // latch switches the bare count of the view's SQL from a call to a column. The guard keeps its invalidation,
        // which invalidates the view again once RESUME returns. RESUME SKIP BACKLOG moved the view's refresh position
        // to the ADD COLUMN's txn without computing a row, so the next RESUME refuses on the switch, and after a
        // restart, which loses the switch, on the guard's reason that the view's state keeps.
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRows();
            SqlParser.setMatViewExplicitNamesCheckSkipped(true);
            try {
                execute("CREATE MATERIALIZED VIEW price_1h AS (SELECT ts, sym, count, last(price) AS price FROM base SAMPLE BY 1h) PARTITION BY DAY");
            } finally {
                SqlParser.setMatViewExplicitNamesCheckSkipped(false);
            }
            drainWalAndMatViewQueues();
            invalidate("test invalidation");
            viewState().setOnResumeHoldingLockForTesting(() -> {
                try {
                    execute("ALTER TABLE base ADD COLUMN count LONG");
                } catch (SqlException e) {
                    throw new RuntimeException(e);
                }
                drainWalQueue();
            });
            execute(RESUME_SQL);
            assertRefreshBaseTxn(2);
            drainWalAndMatViewQueues();
            final String reason = MatViewBareNameGuard.invalidationReason("count");
            assertViewStatus("invalid", reason);
            assertViewRows("""
                    ts\tsym\tcount\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1\t1.0
                    2024-01-01T01:00:00.000000Z\tb\t1\t2.0
                    """);

            assertSwitchRefused(PLAIN_RESUME_SQL, "count", reason, BARE_COUNT_VIEW_ROWS);
            assertSwitchRefused(RESUME_SQL, "count", reason, BARE_COUNT_VIEW_ROWS);
            restart();
            assertSwitchRefused(PLAIN_RESUME_SQL, "count", reason, BARE_COUNT_VIEW_ROWS);
            assertSwitchRefused(RESUME_SQL, "count", reason, BARE_COUNT_VIEW_ROWS);
        });
    }

    @Test
    public void testBareNameGuardWhilePlainResumeHoldsLatchRefusesNextResume() throws Exception {
        // As above, through plain RESUME, which keeps the view's refresh position: the guard keeps its invalidation, and
        // the next RESUME refuses on the switch.
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRows();
            createBareCountView();
            invalidate("test invalidation");
            viewState().setOnResumeHoldingLockForTesting(() -> {
                try {
                    execute("ALTER TABLE base ADD COLUMN count LONG");
                } catch (SqlException e) {
                    throw new RuntimeException(e);
                }
                drainWalQueue();
            });
            execute(PLAIN_RESUME_SQL);
            assertRefreshBaseTxn(1);
            drainWalAndMatViewQueues();
            final String reason = MatViewBareNameGuard.invalidationReason("count");
            assertViewStatus("invalid", reason);
            assertViewRows(BARE_COUNT_VIEW_ROWS);
            assertSwitchRefused(PLAIN_RESUME_SQL, "count", reason, BARE_COUNT_VIEW_ROWS);
            assertSwitchRefused(RESUME_SQL, "count", reason, BARE_COUNT_VIEW_ROWS);
        });
    }

    @Test
    public void testCoverageOfReplacedBaseCoversNothingOnNewBase() throws Exception {
        // A FULL refresh covers the bare-name switches of the table it rebuilt the view from, and no other table's. The
        // view reads the base column count bare. Dropping it switches the bare count to a call; FULL rebuilds the view
        // under that reading, and the base gains the column again, which no FULL covers. The base is then replaced by
        // a table with a column named count, which RESUME SKIP BACKLOG positions the view on: RESUME does not judge
        // the rows of another table, and the switches of the old table do not stop RESUME on the new one. Finally the
        // new table drops its count while the view is invalid. RESUME runs after the bare-name guard recorded that
        // switch and before the drop applies, so its compile reads count as the column: the FULL of the old table, at
        // a txn above the new table's switch, does not cover it, and RESUME refuses.
        assertMemoryLeak(() -> {
            createCountColumnBaseWithView();
            execute("ALTER TABLE base DROP COLUMN count");
            drainWalAndMatViewQueues();
            assertViewStatus("invalid", MatViewBareNameGuard.invalidationReason("count"));
            // two more base txns, so that the FULL's snapshot, txn 4, is above the txn of the new table's DROP COLUMN
            execute("INSERT INTO base (sym, price, amount, ts) VALUES ('e', 5.0, 5.0, '2024-01-01T05:10:00.000000Z')");
            execute("INSERT INTO base (sym, price, amount, ts) VALUES ('f', 6.0, 6.0, '2024-01-01T06:10:00.000000Z')");
            drainWalQueue();
            execute("REFRESH MATERIALIZED VIEW price_1h FULL");
            drainWalAndMatViewQueues();
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            final String rebuiltRows = """
                    ts\tsym\tcount\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1\t1.0
                    2024-01-01T01:00:00.000000Z\tb\t1\t2.0
                    2024-01-01T05:00:00.000000Z\te\t1\t5.0
                    2024-01-01T06:00:00.000000Z\tf\t1\t6.0
                    """;
            assertViewRows(rebuiltRows);
            Assert.assertEquals(4, appliedTxn(engine.verifyTableName("base")));
            // a switch of the old table that no FULL covers
            execute("ALTER TABLE base ADD COLUMN count LONG");
            drainWalAndMatViewQueues();
            assertViewStatus("invalid", MatViewBareNameGuard.invalidationReason("count"));

            final int oldBaseTableId = engine.verifyTableName("base").getTableId();
            execute("DROP TABLE base");
            execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, amount DOUBLE, count LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base VALUES ('c', 3.0, 3.0, 9, '2024-01-01T02:10:00.000000Z')");
            drainWalAndMatViewQueues();
            final TableToken newBaseToken = engine.verifyTableName("base");
            Assert.assertNotEquals(oldBaseTableId, newBaseToken.getTableId());
            assertResumeSucceeds();
            Assert.assertEquals(newBaseToken.getTableId(), viewState().getBaseTableId());
            execute("INSERT INTO base VALUES ('d', 4.0, 4.0, 5, '2024-01-01T03:10:00.000000Z')");
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            final String newBaseRows = """
                    ts\tsym\tcount\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1\t1.0
                    2024-01-01T01:00:00.000000Z\tb\t1\t2.0
                    2024-01-01T03:00:00.000000Z\td\t5\t4.0
                    2024-01-01T05:00:00.000000Z\te\t1\t5.0
                    2024-01-01T06:00:00.000000Z\tf\t1\t6.0
                    """;
            assertViewRows(newBaseRows);

            // the old table's switch belongs to rows of another table
            invalidate("test invalidation");
            execute(PLAIN_RESUME_SQL);
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            Assert.assertEquals(2, appliedTxn(newBaseToken));

            invalidate("test invalidation");
            // the new table's DROP COLUMN is its txn 3, below the old table's FULL at txn 4
            assertSwitchRefusedWhileGuardedDropPends("count", "test invalidation");
            Assert.assertEquals(3, appliedTxn(newBaseToken));
            assertViewRows(newBaseRows);
        });
    }

    @Test
    public void testGuardBetweenResumeFlagAndSwitchReadRefusesResume() throws Exception {
        // RESUME sets the view's resuming flag before it reads the bare-name switches, and the bare-name guard records a
        // switch before it reads the flag. Here the guard runs between RESUME's two steps: the base gains a column named
        // count, which the invalid older view calls bare. The guard sees RESUME and keeps its invalidation for the
        // release of RESUME's latch, and RESUME sees the switch and refuses, which drops that invalidation: the view
        // stays invalid with its reason.
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRows();
            createBareCountView();
            invalidate("test invalidation");
            final AtomicReference<String> guardReason = new AtomicReference<>();
            viewState().setOnResumeFlagSetForTesting(() -> {
                try {
                    execute("ALTER TABLE base ADD COLUMN count LONG");
                } catch (SqlException e) {
                    throw new RuntimeException(e);
                }
                drainWalQueue();
                guardReason.set(viewState().getPendingInvalidationReason());
            });
            assertSwitchRefused(PLAIN_RESUME_SQL, "count", "test invalidation", BARE_COUNT_VIEW_ROWS);
            Assert.assertEquals(MatViewBareNameGuard.invalidationReason("count"), guardReason.get());
            Assert.assertEquals(2, appliedTxn(engine.verifyTableName("base")));
        });
    }

    @Test
    public void testGuardRecordsEveryMatchedName() throws Exception {
        // An older view calls count without parentheses and reads the base column pi bare. While the view is invalid,
        // the base renames pi to count, which switches both names: pi from a column to a call of pi(), and count from
        // a call to a column. RESUME refuses on its compile while the SQL reads pi as a column, and on the switch of
        // count once ALTER ... AS calls pi(): the guard records every name a change switches, not only the first.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, amount DOUBLE, pi DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO base VALUES
                    ('a', 1.0, 1.0, 5.0, '2024-01-01T00:10:00.000000Z'),
                    ('b', 2.0, 2.0, 6.0, '2024-01-01T01:10:00.000000Z')
                    """);
            drainWalQueue();
            createOlderView("SELECT ts, sym, count, pi, last(price) AS price FROM base SAMPLE BY 1h");
            final String rows = """
                    ts\tsym\tcount\tpi\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1\t5.0\t1.0
                    2024-01-01T01:00:00.000000Z\tb\t1\t6.0\t2.0
                    """;
            assertViewRows(rows);
            invalidate("test invalidation");
            execute("ALTER TABLE base RENAME COLUMN pi TO count");
            drainWalAndMatViewQueues();
            assertViewStatus("invalid", "test invalidation");
            assertExceptionNoLeakCheck(
                    PLAIN_RESUME_SQL,
                    VIEW_NAME_POSITION,
                    "materialized view SQL does not compile, fix it with ALTER MATERIALIZED VIEW ... AS first [view=price_1h, error=pi is not a column; write pi() to call the function]"
            );

            execute("ALTER MATERIALIZED VIEW price_1h AS (SELECT ts, sym, count, pi() AS pi, last(price) AS price FROM base SAMPLE BY 1h)");
            drainWalQueue();
            assertSwitchRefused(PLAIN_RESUME_SQL, "count", "test invalidation", rows);

            execute("ALTER MATERIALIZED VIEW price_1h AS (SELECT ts, sym, count() AS count, pi() AS pi, last(price) AS price FROM base SAMPLE BY 1h)");
            drainWalQueue();
            execute(PLAIN_RESUME_SQL);
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            execute("INSERT INTO base (sym, price, amount, ts) VALUES ('c', 3.0, 3.0, '2024-01-01T02:10:00.000000Z')");
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            assertViewRows(rows + "2024-01-01T02:00:00.000000Z\tc\t1\t" + PI_TEXT + "\t3.0\n");
        });
    }

    @Test
    public void testGuardsOfTwoBasesRecordingAtOnceKeepEachSwitch() throws Exception {
        // Around ALTER ... WITH BASE, the bare-name guards of two base tables can record on one view at once: the guard
        // of the table the view leaves fetched the view before the move, and the guard of the table it moves to fetches
        // it after. The view reads the column count of base bare, and ALTER ... WITH BASE moves it to base2 with a SQL
        // that reads the count of base2 bare. The guard of base2's DROP COLUMN count builds its copy of the view's
        // switches and holds it unpublished while ALTER ... WITH BASE moves the view back to base, and the guard of
        // base's DROP COLUMN count then waits for the lock until the first copy is published, and copies it: neither
        // guard drops the other's switch. RESUME SKIP BACKLOG runs before base's drop applies, so its compile reads count
        // as the column of base, the table of the view's rows, and it refuses on the switch that the second guard
        // recorded.
        final CountDownLatch applyPaused = new CountDownLatch(1);
        final CountDownLatch applyRelease = new CountDownLatch(1);
        final AtomicReference<String> pauseSuffix = new AtomicReference<>();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public boolean removeQuiet(LPSZ name) {
                final String suffix = pauseSuffix.get();
                if (suffix != null && Utf8s.endsWithAscii(name, suffix) && pauseSuffix.compareAndSet(suffix, null)) {
                    applyPaused.countDown();
                    try {
                        Assert.assertTrue(applyRelease.await(60, TimeUnit.SECONDS));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                return super.removeQuiet(name);
            }
        };
        assertMemoryLeak(ff, () -> {
            createCountColumnBaseWithView();
            execute("CREATE TABLE base2 (sym SYMBOL, price DOUBLE, count LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base2 VALUES ('x', 9.0, 9, '2024-01-01T00:30:00.000000Z')");
            drainWalAndMatViewQueues();
            invalidate("test invalidation");
            execute("ALTER MATERIALIZED VIEW price_1h WITH BASE base2 AS (SELECT ts, sym, count, last(price) AS price FROM base2 SAMPLE BY 1h)");
            drainWalAndMatViewQueues();

            final CountDownLatch copied = new CountDownLatch(1);
            final CountDownLatch publish = new CountDownLatch(1);
            viewState().setOnBareNameSwitchCopiedForTesting(() -> {
                copied.countDown();
                try {
                    Assert.assertTrue(publish.await(60, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
            execute("ALTER TABLE base2 DROP COLUMN count");
            final AtomicReference<Throwable> base2ApplierError = new AtomicReference<>();
            final Thread base2Applier = startWalApplier(base2ApplierError);
            try {
                Assert.assertTrue(copied.await(30, TimeUnit.SECONDS));
                execute("ALTER MATERIALIZED VIEW price_1h WITH BASE base AS (" + BARE_COUNT_VIEW_SQL + ")");
                drainWalQueue();
                pauseSuffix.set(engine.verifyTableName("base").getDirName() + Files.SEPARATOR + TableUtils.META_SWAP_FILE_NAME);
                execute("ALTER TABLE base DROP COLUMN count");
                final AtomicReference<Throwable> baseApplierError = new AtomicReference<>();
                final Thread baseApplier = startWalApplier(baseApplierError);
                try {
                    awaitBareNameSwitchLockOrLatch(baseApplier, applyPaused);
                    publish.countDown();
                    Assert.assertTrue(applyPaused.await(30, TimeUnit.SECONDS));
                    // base2's guard is done, so nothing else runs on the view while RESUME does
                    base2Applier.join();
                    Assert.assertNull(base2ApplierError.get());
                    assertSwitchRefusedInPlace(RESUME_SQL, "count");
                } finally {
                    publish.countDown();
                    applyRelease.countDown();
                    baseApplier.join();
                }
                Assert.assertNull(baseApplierError.get());
            } finally {
                publish.countDown();
                base2Applier.join();
            }
            drainWalAndMatViewQueues();
            assertViewStatus("invalid", MatViewBacklog.LOST_BASE_TABLE_CHANGED);
            assertViewRows(COUNT_COLUMN_VIEW_ROWS);
        });
    }

    @Test
    public void testResumeAcceptedAfterFullFailedPastTruncate() throws Exception {
        // A FULL refresh that fails after its truncate leaves the view with a "never refreshed" position, so the view's
        // next refresh is a first build that recomputes all its rows under one reading. RESUME accepts it, though the
        // bare-name switch that the FULL followed is covered by no FULL, and the first build reads count as the new
        // column in every bucket.
        setProperty(PropertyKey.CAIRO_MAT_VIEW_ROWS_PER_QUERY_ESTIMATE, 1);
        setProperty(PropertyKey.CAIRO_MAT_VIEW_INSERT_AS_SELECT_BATCH_SIZE, 1);
        assertMemoryLeak(() -> {
            createBase();
            execute("""
                    INSERT INTO base VALUES
                    ('a', 1.0, 1.0, '2024-01-01T00:10:00.000000Z'),
                    ('b', 2.0, 2.0, '2024-01-02T00:10:00.000000Z'),
                    ('c', 3.0, 3.0, '2024-01-03T00:10:00.000000Z')
                    """);
            drainWalQueue();
            createOlderView(BARE_COUNT_VIEW_SQL);
            assertViewRows("""
                    ts\tsym\tcount\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1\t1.0
                    2024-01-02T00:00:00.000000Z\tb\t1\t2.0
                    2024-01-03T00:00:00.000000Z\tc\t1\t3.0
                    """);
            execute("ALTER TABLE base ADD COLUMN count LONG");
            drainWalAndMatViewQueues();
            assertViewStatus("invalid", MatViewBareNameGuard.invalidationReason("count"));

            execute("REFRESH MATERIALIZED VIEW price_1h FULL");
            final AtomicLong commitCount = new AtomicLong();
            try (MatViewRefreshJob job = new MatViewRefreshJob(0, engine, 0)) {
                job.setOnBatchCommittedForTesting(() -> {
                    if (commitCount.incrementAndGet() == 2) {
                        throw CairoException.nonCritical().put("injected failure after the truncate");
                    }
                });
                drainWalQueue();
                drainMatViewQueue(job);
            }
            drainWalQueue();
            assertViewStatus("invalid", "[-1]: injected failure after the truncate");
            assertRefreshBaseTxn(-1);
            assertViewRows("""
                    ts\tsym\tcount\tprice
                    2024-01-01T00:00:00.000000Z\ta\tnull\t1.0
                    2024-01-02T00:00:00.000000Z\tb\tnull\t2.0
                    """);

            assertResumeSucceeds();
            assertRefreshBaseTxn(-1);
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            assertViewRows("""
                    ts\tsym\tcount\tprice
                    2024-01-01T00:00:00.000000Z\ta\tnull\t1.0
                    2024-01-02T00:00:00.000000Z\tb\tnull\t2.0
                    2024-01-03T00:00:00.000000Z\tc\tnull\t3.0
                    """);
        });
    }

    @Test
    public void testResumeAcceptedOnceFullCoversSwitch() throws Exception {
        // The bare-name guard invalidates the valid older view when the base gains a column named count, the base's
        // txn 2. RESUME refuses until a FULL refresh rebuilds the view from a base snapshot that includes the change,
        // here at txn 2 itself, under the column reading. A later RESUME accepts the view, which keeps reading count
        // as the column.
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRows();
            createBareCountView();
            execute("ALTER TABLE base ADD COLUMN count LONG");
            drainWalAndMatViewQueues();
            final String reason = MatViewBareNameGuard.invalidationReason("count");
            assertViewStatus("invalid", reason);
            Assert.assertEquals(2, appliedTxn(engine.verifyTableName("base")));
            assertSwitchRefused(PLAIN_RESUME_SQL, "count", reason, BARE_COUNT_VIEW_ROWS);

            execute("REFRESH MATERIALIZED VIEW price_1h FULL");
            drainWalAndMatViewQueues();
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            assertViewRows(BARE_COUNT_VIEW_COLUMN_ROWS);
            assertRefreshBaseTxn(2);

            invalidate("test invalidation");
            execute(PLAIN_RESUME_SQL);
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            execute("INSERT INTO base (sym, price, amount, ts) VALUES ('c', 3.0, 3.0, '2024-01-01T02:10:00.000000Z')");
            drainWalAndMatViewQueues();
            assertViewRows(BARE_COUNT_VIEW_COLUMN_ROWS + "2024-01-01T02:00:00.000000Z\tc\tnull\t3.0\n");
        });
    }

    @Test
    public void testResumeIgnoresBareNameInsideRegularView() throws Exception {
        // A bare name inside a regular view that the view's SQL reads is not the view's own: the bare-name guard does
        // not watch it, and RESUME does not refuse on it. The regular view reads pi as the base's new column from then
        // on, the documented unguarded case.
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRows();
            execute("CREATE VIEW rv AS (SELECT ts, sym, price, pi AS p FROM base)");
            drainWalAndViewQueues();
            execute("CREATE MATERIALIZED VIEW price_1h WITH BASE base AS (SELECT ts, sym, last(price) AS price, last(p) AS p FROM rv SAMPLE BY 1h) PARTITION BY DAY");
            drainWalAndMatViewQueues();
            final String rows = "ts\tsym\tprice\tp\n"
                    + "2024-01-01T00:00:00.000000Z\ta\t1.0\t" + PI_TEXT + "\n"
                    + "2024-01-01T01:00:00.000000Z\tb\t2.0\t" + PI_TEXT + "\n";
            assertViewRows(rows);
            invalidate("test invalidation");
            execute("ALTER TABLE base ADD COLUMN pi DOUBLE");
            drainWalAndMatViewQueues();
            assertViewStatus("invalid", "test invalidation");

            execute(PLAIN_RESUME_SQL);
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            execute("INSERT INTO base (sym, price, amount, ts) VALUES ('c', 5.0, 5.0, '2024-01-01T02:10:00.000000Z')");
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            assertViewRows(rows + "2024-01-01T02:00:00.000000Z\tc\t5.0\tnull\n");
        });
    }

    @Test
    public void testResumeOfNeverRefreshedViewAfterSwitch() throws Exception {
        // A view that never completed a refresh keeps no rows that its next refresh does not recompute, so RESUME
        // accepts it after a bare-name switch. Its first build then reads count as the new column. Only a FULL refresh
        // records the base snapshot it rebuilt the view from, so after that first build RESUME refuses until a FULL.
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRows();
            createOlderView("REFRESH MANUAL DEFERRED ", BARE_COUNT_VIEW_SQL);
            execute("ALTER TABLE base ADD COLUMN count LONG");
            drainWalAndMatViewQueues();
            // the guard's invalidation declines a view that never completed a refresh
            assertViewStatus("valid", "");
            invalidate("test invalidation");
            execute(PLAIN_RESUME_SQL);
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            assertRefreshBaseTxn(-1);
            assertViewRows("ts\tsym\tcount\tprice\n");

            execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
            drainWalAndMatViewQueues();
            assertViewRows(BARE_COUNT_VIEW_COLUMN_ROWS);
            assertRefreshBaseTxn(2);
            invalidate("test invalidation");
            assertSwitchRefused(PLAIN_RESUME_SQL, "count", "test invalidation", BARE_COUNT_VIEW_COLUMN_ROWS);

            execute("REFRESH MATERIALIZED VIEW price_1h FULL");
            drainWalAndMatViewQueues();
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            assertViewRows(BARE_COUNT_VIEW_COLUMN_ROWS);
            invalidate("test invalidation");
            execute(PLAIN_RESUME_SQL);
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
        });
    }

    @Test
    public void testResumeRefusesBareCallSwitchedByIngestedColumnWhileInvalid() throws Exception {
        // The column of the switch comes from the WAL writer API that ILP's column auto-create uses.
        assertMemoryLeak(() -> assertBareCallSwitchedWhileInvalidRefused(() -> {
            try (WalWriter walWriter = engine.getWalWriter(engine.verifyTableName("base"))) {
                walWriter.addColumn("count", ColumnType.LONG, sqlExecutionContext.getSecurityContext());
            }
        }));
    }

    @Test
    public void testResumeRefusesBareCallSwitchedByRenameWhileInvalid() throws Exception {
        // A base column renamed to count switches the bare count too; the view's LONG column takes the DOUBLE column.
        assertMemoryLeak(() -> assertBareCallSwitchedWhileInvalidRefused(() -> execute("ALTER TABLE base RENAME COLUMN amount TO count")));
    }

    @Test
    public void testResumeRefusesBareCallSwitchedToColumnWhileInvalid() throws Exception {
        // An older view calls count without parentheses. While the view is invalid, the base gains a column named
        // count, which turns the bare count of the view's SQL from a call into a column: RESUME's compile, which reads
        // bare names as columns, reads the new column, while the view's rows hold the row count. Plain RESUME and
        // RESUME SKIP BACKLOG refuse before plain RESUME brings the backlog up, so a refusal writes no view state.
        // ALTER ... AS writing count() recovers the view, and plain RESUME catches up on the rows the base received
        // meanwhile.
        assertMemoryLeak(() -> assertBareCallSwitchedWhileInvalidRefusedThenRecovered("IMMEDIATE"));
    }

    @Test
    public void testResumeRefusesBareCallSwitchedToColumnWhileInvalidManualView() throws Exception {
        // As above, for a MANUAL view, which catches up through REFRESH ... INCREMENTAL.
        assertMemoryLeak(() -> assertBareCallSwitchedWhileInvalidRefusedThenRecovered("MANUAL"));
    }

    @Test
    public void testResumeRefusesBareCallSwitchedToIntColumnWhileInvalid() throws Exception {
        // The view's LONG column takes the INT column, so the column mapping passes, and the switch decides.
        assertMemoryLeak(() -> assertBareCallSwitchedWhileInvalidRefused(() -> execute("ALTER TABLE base ADD COLUMN count INT")));
    }

    @Test
    public void testResumeRefusesBareNameThatAlterKeepsBare() throws Exception {
        // ALTER ... AS that keeps count bare reads the new column on purpose, but RESUME cannot tell that from the old
        // reading and refuses with the guard's advice: write count() to call the function or qualify the column. A
        // qualified column resumes, and the view's new rows read the column.
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRows();
            createBareCountView();
            invalidate("test invalidation");
            execute("ALTER TABLE base ADD COLUMN count LONG");
            execute(INSERT_C_ROWS);
            drainWalAndMatViewQueues();
            assertSwitchRefused(PLAIN_RESUME_SQL, "count", "test invalidation", BARE_COUNT_VIEW_ROWS);

            execute("ALTER MATERIALIZED VIEW price_1h AS (SELECT ts, sym, count, max(price) AS price FROM base SAMPLE BY 1h)");
            drainWalQueue();
            assertSwitchRefused(PLAIN_RESUME_SQL, "count", "test invalidation", BARE_COUNT_VIEW_ROWS);

            execute("ALTER MATERIALIZED VIEW price_1h AS (SELECT ts, sym, base.count AS count, last(price) AS price FROM base SAMPLE BY 1h)");
            drainWalQueue();
            execute(PLAIN_RESUME_SQL);
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            assertViewRows(BARE_COUNT_VIEW_ROWS + "2024-01-01T02:00:00.000000Z\tc\tnull\t6.0\n");
        });
    }

    @Test
    public void testResumeRefusesEachSwitchedName() throws Exception {
        // An older view calls count and pi without parentheses, and the base gains a column under each name while the
        // view is invalid. RESUME refuses on count, then, once ALTER ... AS calls count(), on pi, and accepts the view
        // once the SQL calls both.
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRows();
            createOlderView("SELECT ts, sym, count, pi, last(price) AS price FROM base SAMPLE BY 1h");
            final String rows = "ts\tsym\tcount\tpi\tprice\n"
                    + "2024-01-01T00:00:00.000000Z\ta\t1\t" + PI_TEXT + "\t1.0\n"
                    + "2024-01-01T01:00:00.000000Z\tb\t1\t" + PI_TEXT + "\t2.0\n";
            assertViewRows(rows);
            invalidate("test invalidation");
            execute("ALTER TABLE base ADD COLUMN count LONG");
            execute("ALTER TABLE base ADD COLUMN pi DOUBLE");
            drainWalAndMatViewQueues();
            assertSwitchRefused(PLAIN_RESUME_SQL, "count", "test invalidation", rows);

            execute("ALTER MATERIALIZED VIEW price_1h AS (SELECT ts, sym, count() AS count, pi, last(price) AS price FROM base SAMPLE BY 1h)");
            drainWalQueue();
            assertSwitchRefused(PLAIN_RESUME_SQL, "pi", "test invalidation", rows);

            execute("ALTER MATERIALIZED VIEW price_1h AS (SELECT ts, sym, count() AS count, pi() AS pi, last(price) AS price FROM base SAMPLE BY 1h)");
            drainWalQueue();
            execute(PLAIN_RESUME_SQL);
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            assertViewRows(rows);
        });
    }

    @Test
    public void testResumeRefusesGuardReasonAfterRestart() throws Exception {
        // The bare-name guard invalidates the valid older view when the base gains a column named count. A restart
        // loses the switch the guard recorded in memory, but the view's state keeps the guard's reason, on which RESUME
        // refuses. ALTER ... AS writing count() recovers the view.
        // A view that was already invalid for another reason when the switch came keeps that reason, so a restart
        // before RESUME loses the switch: RESUME then accepts the view, the documented loss of the guard's
        // notification, which no test asserts.
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRows();
            createBareCountView();
            execute("ALTER TABLE base ADD COLUMN count LONG");
            drainWalAndMatViewQueues();
            final String reason = MatViewBareNameGuard.invalidationReason("count");
            assertViewStatus("invalid", reason);
            restart();
            assertViewStatus("invalid", reason);
            assertSwitchRefused(PLAIN_RESUME_SQL, "count", reason, BARE_COUNT_VIEW_ROWS);
            assertSwitchRefused(RESUME_SQL, "count", reason, BARE_COUNT_VIEW_ROWS);

            execute("ALTER MATERIALIZED VIEW price_1h AS (" + CALL_COUNT_VIEW_SQL + ")");
            drainWalQueue();
            execute(PLAIN_RESUME_SQL);
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            execute(INSERT_C_ROWS);
            drainWalAndMatViewQueues();
            assertViewRows(BARE_COUNT_VIEW_ROWS + "2024-01-01T02:00:00.000000Z\tc\t2\t6.0\n");
        });
    }

    @Test
    public void testResumeRefusesSwitchAfterLastFull() throws Exception {
        // A FULL refresh covers the switches up to the base snapshot it rebuilt the view from, here txn 1, and not the
        // guarded ADD COLUMN count of txn 2 that comes after it, while the view is invalid.
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRows();
            createBareCountView();
            execute("REFRESH MATERIALIZED VIEW price_1h FULL");
            drainWalAndMatViewQueues();
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            assertViewRows(BARE_COUNT_VIEW_ROWS);
            invalidate("test invalidation");
            execute("ALTER TABLE base ADD COLUMN count LONG");
            drainWalAndMatViewQueues();
            Assert.assertEquals(2, appliedTxn(engine.verifyTableName("base")));
            assertViewStatus("invalid", "test invalidation");
            assertSwitchRefused(PLAIN_RESUME_SQL, "count", "test invalidation", BARE_COUNT_VIEW_ROWS);
        });
    }

    @Test
    public void testResumeRefusesSwitchBackAfterFull() throws Exception {
        // The view reads the base column count bare. Dropping it, the base's txn 2, switches count to a call, and
        // FULL rebuilds the view from txn 2 under that reading. The base then gains the column again, txn 3, while the
        // view is invalid: the switch of count now stands at txn 3, which that FULL does not cover. A second FULL
        // rebuilds the view under the column reading, after which RESUME accepts it.
        assertMemoryLeak(() -> {
            createCountColumnBaseWithView();
            execute("ALTER TABLE base DROP COLUMN count");
            drainWalAndMatViewQueues();
            assertViewStatus("invalid", MatViewBareNameGuard.invalidationReason("count"));
            execute("REFRESH MATERIALIZED VIEW price_1h FULL");
            drainWalAndMatViewQueues();
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            assertViewRows(BARE_COUNT_VIEW_ROWS);

            invalidate("test invalidation");
            execute("ALTER TABLE base ADD COLUMN count LONG");
            drainWalAndMatViewQueues();
            Assert.assertEquals(3, appliedTxn(engine.verifyTableName("base")));
            assertViewStatus("invalid", "test invalidation");
            assertSwitchRefused(PLAIN_RESUME_SQL, "count", "test invalidation", BARE_COUNT_VIEW_ROWS);

            execute("REFRESH MATERIALIZED VIEW price_1h FULL");
            drainWalAndMatViewQueues();
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            assertViewRows(BARE_COUNT_VIEW_COLUMN_ROWS);
            invalidate("test invalidation");
            execute(PLAIN_RESUME_SQL);
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
        });
    }

    @Test
    public void testResumeRefusesSwitchJumpedOverAfterFailedFull() throws Exception {
        // The base gains a column named count while RESUME SKIP BACKLOG holds the latch: the guard invalidates the
        // resumed view again, and the jump moved its refresh position to the ADD COLUMN's txn without computing a row.
        // A FULL that fails before its truncate, on a base whose readers are locked, replaces the guard's reason and
        // keeps the position. RESUME still refuses on the switch, which only a FULL that succeeds covers.
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRows();
            createBareCountView();
            invalidate("test invalidation");
            viewState().setOnResumeHoldingLockForTesting(() -> {
                try {
                    execute("ALTER TABLE base ADD COLUMN count LONG");
                } catch (SqlException e) {
                    throw new RuntimeException(e);
                }
                drainWalQueue();
            });
            execute(RESUME_SQL);
            assertRefreshBaseTxn(2);
            drainWalAndMatViewQueues();
            assertViewStatus("invalid", MatViewBareNameGuard.invalidationReason("count"));

            execute("REFRESH MATERIALIZED VIEW price_1h FULL");
            final TableToken baseToken = engine.verifyTableName("base");
            Assert.assertTrue(engine.lockReaders(baseToken));
            try {
                drainMatViewQueue(engine);
            } finally {
                engine.unlockReaders(baseToken);
            }
            drainWalAndMatViewQueues();
            final String failedFullReason = "[-1]: table busy [reason=unknown]";
            assertViewStatus("invalid", failedFullReason);
            assertRefreshBaseTxn(2);
            assertViewRows(BARE_COUNT_VIEW_ROWS);
            assertSwitchRefused(PLAIN_RESUME_SQL, "count", failedFullReason, BARE_COUNT_VIEW_ROWS);
            assertSwitchRefused(RESUME_SQL, "count", failedFullReason, BARE_COUNT_VIEW_ROWS);

            execute("REFRESH MATERIALIZED VIEW price_1h FULL");
            drainWalAndMatViewQueues();
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            assertViewRows(BARE_COUNT_VIEW_COLUMN_ROWS);
            invalidate("test invalidation");
            execute(PLAIN_RESUME_SQL);
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
        });
    }

    @Test
    public void testResumeRefusesSwitchOfBaseRecordedBeforeAnotherBase() throws Exception {
        // A view keeps the bare-name switches of each base table it followed. Its base gains a column named count
        // while the view is invalid; ALTER ... WITH BASE then moves the view to another table, whose own count the
        // new SQL reads bare and which the table then renames away, and back to the first table. RESUME SKIP BACKLOG
        // on the first table refuses on its switch, which the other table's later switch does not replace.
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRows();
            createBareCountView();
            execute("CREATE TABLE base2 (sym SYMBOL, price DOUBLE, count LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base2 VALUES ('x', 9.0, 9, '2024-01-01T00:30:00.000000Z')");
            drainWalAndMatViewQueues();
            invalidate("test invalidation");
            execute("ALTER TABLE base ADD COLUMN count LONG");
            drainWalAndMatViewQueues();
            execute("ALTER MATERIALIZED VIEW price_1h WITH BASE base2 AS (SELECT ts, sym, count, last(price) AS price FROM base2 SAMPLE BY 1h)");
            drainWalAndMatViewQueues();
            execute("ALTER TABLE base2 RENAME COLUMN count TO cnt");
            drainWalAndMatViewQueues();
            execute("ALTER MATERIALIZED VIEW price_1h WITH BASE base AS (" + BARE_COUNT_VIEW_SQL + ")");
            drainWalAndMatViewQueues();
            assertViewStatus("invalid", MatViewBacklog.LOST_BASE_TABLE_CHANGED);
            assertSwitchRefused(RESUME_SQL, "count", MatViewBacklog.LOST_BASE_TABLE_CHANGED, BARE_COUNT_VIEW_ROWS);
        });
    }

    @Test
    public void testResumeRefusesSwitchOfParentViewColumn() throws Exception {
        // An older view over a parent materialized view calls count without parentheses. While the view is invalid,
        // the parent gains a view column named count, which switches the bare count: RESUME refuses. Once the parent
        // drops the column again, RESUME refuses on its compile, which reads count as a column.
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRows();
            execute("CREATE MATERIALIZED VIEW parent AS (" + VIEW_SQL + ") PARTITION BY DAY");
            drainWalAndMatViewQueues();
            createOlderView("SELECT ts, sym, count, last(price) AS price FROM parent SAMPLE BY 1h");
            assertViewRows(BARE_COUNT_VIEW_ROWS);
            invalidate("test invalidation");
            execute("ALTER MATERIALIZED VIEW parent ADD COLUMN count LONG");
            drainWalAndMatViewQueues();
            assertViewStatus("invalid", "test invalidation");
            assertSwitchRefused(PLAIN_RESUME_SQL, "count", "test invalidation", BARE_COUNT_VIEW_ROWS);

            execute("ALTER MATERIALIZED VIEW parent DROP COLUMN count");
            drainWalAndMatViewQueues();
            assertExceptionNoLeakCheck(PLAIN_RESUME_SQL, VIEW_NAME_POSITION, COUNT_COMPILE_REFUSAL);
            assertViewStatus("invalid", "test invalidation");
            assertViewRows(BARE_COUNT_VIEW_ROWS);
        });
    }

    @Test
    public void testResumeRefusesSwitchOfSkippedChange() throws Exception {
        // The view reads the base column count bare, and the base drops it while the view is invalid. The bare-name
        // guard records the switch before the change applies, but the apply fails and suspends the base's WAL, and
        // RESUME WAL FROM TXN skips the change: count stays a column. RESUME cannot tell a skipped change from one that
        // applied, and refuses; a FULL refresh recovers the view.
        final AtomicReference<String> failSuffix = new AtomicReference<>();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public boolean removeQuiet(LPSZ name) {
                final String suffix = failSuffix.get();
                if (suffix != null && Utf8s.containsAscii(name, suffix)) {
                    return false;
                }
                return super.removeQuiet(name);
            }
        };
        assertMemoryLeak(ff, () -> {
            createCountColumnBaseWithView();
            invalidate("test invalidation");
            final TableToken baseToken = engine.verifyTableName("base");
            failSuffix.set(baseToken.getDirName() + Files.SEPARATOR + TableUtils.META_SWAP_FILE_NAME);
            execute("ALTER TABLE base DROP COLUMN count");
            drainWalAndMatViewQueues();
            failSuffix.set(null);
            Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(baseToken));
            Assert.assertEquals(1, appliedTxn(baseToken));
            // the drop is the base's txn 2, which the resume skips
            execute("ALTER TABLE base RESUME WAL FROM TXN 3");
            drainWalAndMatViewQueues();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(baseToken));
            Assert.assertEquals(2, appliedTxn(baseToken));
            assertViewStatus("invalid", "test invalidation");
            assertSwitchRefused(PLAIN_RESUME_SQL, "count", "test invalidation", COUNT_COLUMN_VIEW_ROWS);

            // the FULL reads count as the column the base kept
            execute("REFRESH MATERIALIZED VIEW price_1h FULL");
            drainWalAndMatViewQueues();
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            assertViewRows(COUNT_COLUMN_VIEW_ROWS);
            invalidate("test invalidation");
            execute(PLAIN_RESUME_SQL);
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
        });
    }

    @Test
    public void testResumeRefusesSwitchOfViewInvalidatedBeforeThisVersion() throws Exception {
        // A view invalidated before this version keeps no base table id, so its backlog counts as lost. While it is
        // invalid, the base gains a column named count: RESUME SKIP BACKLOG refuses on the switch, and so does plain
        // RESUME, before it finds the backlog lost. Once ALTER ... AS calls count(), plain RESUME refuses on the lost
        // backlog, and RESUME SKIP BACKLOG accepts the view.
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRows();
            createBareCountView();
            try (WalWriter walWriter = engine.getWalWriter(viewToken())) {
                // the view state event of a previous version: no base table id and no backlog-lost marker
                walWriter.resetMatViewState(1, 0, true, "legacy invalidation", Numbers.LONG_NULL, null, -1);
            }
            drainWalQueue();
            restart();
            assertViewStatus("invalid", "legacy invalidation");
            Assert.assertEquals(MatViewState.BASE_TABLE_ID_UNKNOWN, viewState().getBaseTableId());
            execute("ALTER TABLE base ADD COLUMN count LONG");
            drainWalAndMatViewQueues();
            assertViewStatus("invalid", "legacy invalidation");
            assertSwitchRefused(RESUME_SQL, "count", "legacy invalidation", BARE_COUNT_VIEW_ROWS);
            assertSwitchRefused(PLAIN_RESUME_SQL, "count", "legacy invalidation", BARE_COUNT_VIEW_ROWS);

            execute("ALTER MATERIALIZED VIEW price_1h AS (" + CALL_COUNT_VIEW_SQL + ")");
            drainWalQueue();
            assertExceptionNoLeakCheck(
                    PLAIN_RESUME_SQL,
                    VIEW_NAME_POSITION,
                    "materialized view backlog is lost [reason=no base table id]; RESUME cannot catch up on base changes made while "
                            + "the view was invalid. Run REFRESH MATERIALIZED VIEW price_1h RESUME SKIP BACKLOG, then REFRESH ... RANGE over the gap"
            );
            execute(RESUME_SQL);
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            execute(INSERT_C_ROWS);
            drainWalAndMatViewQueues();
            assertViewRows(BARE_COUNT_VIEW_ROWS + "2024-01-01T02:00:00.000000Z\tc\t2\t6.0\n");
        });
    }

    @Test
    public void testResumeRefusesSwitchOnEmptyBase() throws Exception {
        // The switch decides, not the base's rows: RESUME refuses also once the base is empty.
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRows();
            createBareCountView();
            invalidate("test invalidation");
            execute("ALTER TABLE base ADD COLUMN count LONG");
            execute("TRUNCATE TABLE base");
            drainWalAndMatViewQueues();
            assertSwitchRefused(PLAIN_RESUME_SQL, "count", "test invalidation", BARE_COUNT_VIEW_ROWS);
            assertSwitchRefused(RESUME_SQL, "count", "test invalidation", BARE_COUNT_VIEW_ROWS);
        });
    }

    @Test
    public void testResumeRefusesSwitchRecordedWhileWaitingAgain() throws Exception {
        // RESUME reads the bare-name switches on each pass under the latch. A view WAL transaction that arrives after
        // RESUME's wait makes RESUME release the latch and wait again, and meanwhile the base gains a column named
        // count, which the invalid older view calls bare. The bare-name guard finds the view invalid and not resuming,
        // and keeps the view's reason; RESUME's next pass sees the switch and refuses.
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRows();
            createBareCountView();
            invalidate("test invalidation");
            final MatViewState state = viewState();
            final AtomicLong viewSeqTxnAfterAddColumn = new AtomicLong(-1);
            final AtomicBoolean isResumingWhileWaiting = new AtomicBoolean(true);
            state.setOnResumeWalAppliedForTesting(() -> {
                try {
                    execute("ALTER MATERIALIZED VIEW price_1h ADD COLUMN extra DOUBLE");
                } catch (SqlException e) {
                    throw new RuntimeException(e);
                }
                viewSeqTxnAfterAddColumn.set(viewSeqTxn());
                // RESUME takes the latch, finds the view's new transaction unapplied and waits for it again
                state.setOnResumeWalWaitForTesting(() -> {
                    isResumingWhileWaiting.set(state.isResuming());
                    try {
                        execute("ALTER TABLE base ADD COLUMN count LONG");
                    } catch (SqlException e) {
                        throw new RuntimeException(e);
                    }
                    drainWalQueue();
                });
            });
            assertExceptionNoLeakCheck(PLAIN_RESUME_SQL, VIEW_NAME_POSITION, switchedRefusal("count"));
            Assert.assertFalse(isResumingWhileWaiting.get());
            Assert.assertEquals(2, appliedTxn(engine.verifyTableName("base")));
            Assert.assertTrue(state.isInvalid());
            Assert.assertFalse(state.hasPendingInvalidationReason());
            Assert.assertFalse(state.isLocked());
            Assert.assertFalse(state.isResuming());
            drainWalAndMatViewQueues();
            // the refusal wrote no view transaction
            Assert.assertEquals(viewSeqTxnAfterAddColumn.get(), viewSeqTxn());
            assertViewStatus("invalid", "test invalidation");
            assertViewRows("""
                    ts\tsym\tcount\tprice\textra
                    2024-01-01T00:00:00.000000Z\ta\t1\t1.0\tnull
                    2024-01-01T01:00:00.000000Z\tb\t1\t2.0\tnull
                    """);
        });
    }

    @Test
    public void testResumeRefusesSwitchedBarePi() throws Exception {
        // The refusal is not count's: an older view calls pi without parentheses, and a base column named pi turns the
        // bare pi of its SQL from a call into a column while the view is invalid.
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRows();
            createOlderView("SELECT ts, sym, pi, last(price) AS price FROM base SAMPLE BY 1h");
            final String rows = "ts\tsym\tpi\tprice\n"
                    + "2024-01-01T00:00:00.000000Z\ta\t" + PI_TEXT + "\t1.0\n"
                    + "2024-01-01T01:00:00.000000Z\tb\t" + PI_TEXT + "\t2.0\n";
            assertViewRows(rows);
            invalidate("test invalidation");
            execute("ALTER TABLE base ADD COLUMN pi DOUBLE");
            drainWalAndMatViewQueues();
            assertViewStatus("invalid", "test invalidation");
            assertSwitchRefused(PLAIN_RESUME_SQL, "pi", "test invalidation", rows);
        });
    }

    @Test
    public void testResumeRefusesSwitchedNameInNestedModel() throws Exception {
        // A bare name in a nested select of the view's own SQL counts as one of its top-level select list.
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRows();
            createOlderView("SELECT ts, sym, last(p) AS p FROM (SELECT ts, sym, pi AS p FROM base) SAMPLE BY 1h");
            final String rows = "ts\tsym\tp\n"
                    + "2024-01-01T00:00:00.000000Z\ta\t" + PI_TEXT + "\n"
                    + "2024-01-01T01:00:00.000000Z\tb\t" + PI_TEXT + "\n";
            assertViewRows(rows);
            invalidate("test invalidation");
            execute("ALTER TABLE base ADD COLUMN pi DOUBLE");
            drainWalAndMatViewQueues();
            assertSwitchRefused(PLAIN_RESUME_SQL, "pi", "test invalidation", rows);
        });
    }

    @Test
    public void testResumeRefusesSwitchedNameInSqlSpelling() throws Exception {
        // Names compare without regard to case: the SQL writes COUNT, the base gains count, and the refusal names the
        // bare name as the SQL writes it.
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRows();
            createOlderView("SELECT ts, sym, COUNT, last(price) AS price FROM base SAMPLE BY 1h");
            final String rows = """
                    ts\tsym\tCOUNT\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1\t1.0
                    2024-01-01T01:00:00.000000Z\tb\t1\t2.0
                    """;
            assertViewRows(rows);
            invalidate("test invalidation");
            execute("ALTER TABLE base ADD COLUMN count LONG");
            drainWalAndMatViewQueues();
            assertSwitchRefused(PLAIN_RESUME_SQL, "COUNT", "test invalidation", rows);
        });
    }

    @Test
    public void testResumeRefusesSwitchedNameRespelledByAlter() throws Exception {
        // Names compare without regard to case also when ALTER ... AS spells the switched name another way. The older
        // view's SQL writes COUNT, and the bare-name guard invalidates the valid view when the base gains count, with
        // a reason that names COUNT. ALTER ... AS then writes count: RESUME refuses on the recorded switch, naming the
        // bare name as the SQL it compiled writes it, and after a restart, which loses the switch, on the guard's
        // reason.
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRows();
            createOlderView("SELECT ts, sym, COUNT, last(price) AS price FROM base SAMPLE BY 1h");
            final String rows = """
                    ts\tsym\tCOUNT\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1\t1.0
                    2024-01-01T01:00:00.000000Z\tb\t1\t2.0
                    """;
            assertViewRows(rows);
            execute("ALTER TABLE base ADD COLUMN count LONG");
            drainWalAndMatViewQueues();
            final String reason = MatViewBareNameGuard.invalidationReason("COUNT");
            assertViewStatus("invalid", reason);

            execute("ALTER MATERIALIZED VIEW price_1h AS (" + BARE_COUNT_VIEW_SQL + ")");
            drainWalQueue();
            assertSwitchRefused(PLAIN_RESUME_SQL, "count", reason, rows);
            restart();
            assertViewStatus("invalid", reason);
            assertSwitchRefused(PLAIN_RESUME_SQL, "count", reason, rows);
        });
    }

    @Test
    public void testResumeRefusesSwitchedQuotedName() throws Exception {
        // A quoted bare name is the same unqualified column reference.
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRows();
            createOlderView("SELECT ts, sym, \"count\", last(price) AS price FROM base SAMPLE BY 1h");
            assertViewRows(BARE_COUNT_VIEW_ROWS);
            invalidate("test invalidation");
            execute("ALTER TABLE base ADD COLUMN count LONG");
            drainWalAndMatViewQueues();
            assertSwitchRefused(PLAIN_RESUME_SQL, "count", "test invalidation", BARE_COUNT_VIEW_ROWS);
        });
    }

    @Test
    public void testResumeRefusesWhileGuardedDropPending() throws Exception {
        // The view reads the base column count bare. While the view is invalid, the base drops the column: the
        // bare-name guard runs before the drop applies, and the drop's apply pauses before the change is visible.
        // RESUME meanwhile compiles count as the column, which the view's refreshes would read as a call once the drop
        // applies, and refuses on the switch. Once the drop has applied, RESUME refuses on its compile, and ALTER ... AS
        // without count recovers the view.
        final CountDownLatch applyPaused = new CountDownLatch(1);
        final CountDownLatch applyRelease = new CountDownLatch(1);
        final AtomicReference<String> pauseSuffix = new AtomicReference<>();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public boolean removeQuiet(LPSZ name) {
                final String suffix = pauseSuffix.get();
                if (suffix != null && Utf8s.endsWithAscii(name, suffix) && pauseSuffix.compareAndSet(suffix, null)) {
                    applyPaused.countDown();
                    try {
                        Assert.assertTrue(applyRelease.await(60, TimeUnit.SECONDS));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                return super.removeQuiet(name);
            }
        };
        assertMemoryLeak(ff, () -> {
            createCountColumnBaseWithView();
            invalidate("test invalidation");
            pauseSuffix.set(engine.verifyTableName("base").getDirName() + Files.SEPARATOR + TableUtils.META_SWAP_FILE_NAME);
            execute("ALTER TABLE base DROP COLUMN count");
            final AtomicReference<Throwable> applierError = new AtomicReference<>();
            final Thread applier = startWalApplier(applierError);
            try {
                Assert.assertTrue(applyPaused.await(30, TimeUnit.SECONDS));
                assertSwitchRefusedInPlace(PLAIN_RESUME_SQL, "count");
                assertSwitchRefusedInPlace(RESUME_SQL, "count");
            } finally {
                applyRelease.countDown();
                applier.join();
            }
            Assert.assertNull(applierError.get());
            drainWalAndMatViewQueues();
            assertViewStatus("invalid", "test invalidation");
            assertViewRows(COUNT_COLUMN_VIEW_ROWS);
            assertExceptionNoLeakCheck(RESUME_SQL, VIEW_NAME_POSITION, COUNT_COMPILE_REFUSAL);

            execute("ALTER MATERIALIZED VIEW price_1h AS (" + VIEW_SQL + ")");
            drainWalQueue();
            assertResumeSucceeds();
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            assertViewRows(COUNT_COLUMN_VIEW_ROWS);
        });
    }

    @Test
    public void testResumeBetweenGuardRecordAndGateRefuses() throws Exception {
        // The bare-name guard records a switch before it reads the view's resuming flag, and RESUME sets the flag
        // before it reads the switches: a RESUME that runs between the guard's record and its read sees the switch.
        // Here the view reads the base column count bare, and the base drops it while the view is invalid; RESUME runs
        // in that window, before the drop applies, so its compile reads count as the column, and it refuses on the
        // switch. The guard then finds the view invalid and not resuming, and keeps its reason.
        assertMemoryLeak(() -> {
            createCountColumnBaseWithView();
            invalidate("test invalidation");
            assertSwitchRefusedWhileGuardedDropPends("count", "test invalidation");
            Assert.assertFalse(viewState().hasPendingInvalidationReason());
            assertViewRows(COUNT_COLUMN_VIEW_ROWS);
            assertExceptionNoLeakCheck(RESUME_SQL, VIEW_NAME_POSITION, COUNT_COMPILE_REFUSAL);
            assertViewStatus("invalid", "test invalidation");
        });
    }

    @Test
    public void testResumeSkipBacklogAcceptsReplacedBaseAfterSwitch() throws Exception {
        // The bare-name guard invalidates the valid older view when the base gains a column named count. The base is
        // then replaced by a table with a column named count: the view's rows belong to the old table, so RESUME SKIP
        // BACKLOG positions the view on the new one, whose rows the view reads count as the column in.
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRows();
            createBareCountView();
            execute("ALTER TABLE base ADD COLUMN count LONG");
            drainWalAndMatViewQueues();
            final String reason = MatViewBareNameGuard.invalidationReason("count");
            assertViewStatus("invalid", reason);
            execute("DROP TABLE base");
            execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, amount DOUBLE, count LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base VALUES ('c', 3.0, 3.0, 9, '2024-01-01T02:10:00.000000Z')");
            drainWalAndMatViewQueues();
            // the drop's invalidation reaches an invalid view, which keeps its reason
            assertViewStatus("invalid", reason);

            assertResumeSucceeds();
            Assert.assertEquals(engine.verifyTableName("base").getTableId(), viewState().getBaseTableId());
            execute("INSERT INTO base VALUES ('d', 4.0, 4.0, 5, '2024-01-01T03:10:00.000000Z')");
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            assertViewRows(BARE_COUNT_VIEW_ROWS + "2024-01-01T03:00:00.000000Z\td\t5\t4.0\n");
        });
    }

    @Test
    public void testBareNameGuardWhileFullParkedOnViewWal() throws Exception {
        // A FULL parked on an invalid view's unapplied WAL is pending, so it recovers the view: a guarded base column
        // change that applies meanwhile publishes the guard's invalidation instead of leaving the view's old reason.
        // The FULL then rebuilds from a base snapshot that includes the change, which covers that invalidation, and
        // reads the bare count as the new column, as FULL does: count becomes a grouping key, NULL in the old rows.
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRows();
            SqlParser.setMatViewExplicitNamesCheckSkipped(true);
            try {
                execute("CREATE MATERIALIZED VIEW price_1h AS (SELECT ts, sym, count, last(price) AS price FROM base SAMPLE BY 1h) PARTITION BY DAY");
            } finally {
                SqlParser.setMatViewExplicitNamesCheckSkipped(false);
            }
            drainWalAndMatViewQueues();
            assertViewRows("""
                    ts\tsym\tcount\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1\t1.0
                    2024-01-01T01:00:00.000000Z\tb\t1\t2.0
                    """);
            // the invalidation event stays in the view's WAL, unapplied, so the FULL parks
            engine.getMatViewStateStore().enqueueInvalidate(viewToken(), "test invalidation");
            drainMatViewQueue(engine);
            Assert.assertTrue(hasUnappliedViewWal());
            execute("REFRESH MATERIALIZED VIEW price_1h FULL");
            drainMatViewQueue(engine);
            Assert.assertTrue(viewState().isInvalid());
            Assert.assertTrue(viewState().hasPendingFullRefreshOwnerForTesting());

            execute("ALTER TABLE base ADD COLUMN count LONG");
            drainWalQueue();
            Assert.assertEquals(MatViewBareNameGuard.invalidationReason("count"), viewState().getPendingInvalidationReason());

            drainWalAndMatViewQueues();
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            Assert.assertFalse(viewState().hasPendingInvalidationReason());
            Assert.assertFalse(viewState().hasPendingFullRefreshOwnerForTesting());
            final String rebuiltRows = """
                    ts\tsym\tcount\tprice
                    2024-01-01T00:00:00.000000Z\ta\tnull\t1.0
                    2024-01-01T01:00:00.000000Z\tb\tnull\t2.0
                    """;
            assertViewRows(rebuiltRows);

            // the guard keeps watching the rebuilt view: dropping the column switches the bare count back to a call
            execute("ALTER TABLE base DROP COLUMN count");
            drainWalAndMatViewQueues();
            assertViewStatus("invalid", MatViewBareNameGuard.invalidationReason("count"));
            assertViewRows(rebuiltRows);
        });
    }

    @Test
    public void testMappingColumnsAfterResume() throws Exception {
        // RESUME compiles the view's SQL and reports the column mapping it builds in materialized_views(): a view
        // column the SQL does not produce receives NULL.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            execute("ALTER MATERIALIZED VIEW price_1h ADD COLUMN extra DOUBLE");
            drainWalAndMatViewQueues();
            invalidate("test invalidation");
            restart();
            assertMappingColumns(null, null);
            execute(RESUME_SQL);
            assertMappingColumns("extra", "");
        });
    }

    @Test
    public void testNeverRefreshedViewResumesWithoutPosition() throws Exception {
        // A view that never completed a refresh keeps its "never refreshed" position, so its next refresh builds it.
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRows();
            execute("CREATE MATERIALIZED VIEW price_1h REFRESH MANUAL DEFERRED AS (" + VIEW_SQL + ") PARTITION BY DAY");
            drainWalAndMatViewQueues();
            invalidate("test invalidation");
            assertResumeSucceeds();
            assertViewStatus("valid", "");
            assertRefreshBaseTxn(-1);
            assertViewRows("ts\tsym\tprice\n");
            execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
            drainWalAndMatViewQueues();
            assertViewRows(VIEW_ROWS);
            assertRefreshBaseTxn(1);
        });
    }

    @Test
    public void testPeriodViewMovesLastPeriodToCurrentBoundary() throws Exception {
        // RESUME jumps a period view's last refreshed period to the current period boundary: the periods the view
        // missed while invalid are left to REFRESH ... RANGE.
        assertMemoryLeak(() -> {
            currentMicros = parseFloorPartialTimestamp("2024-01-01T00:00:00.000000Z");
            createBase();
            execute("CREATE MATERIALIZED VIEW price_1h REFRESH IMMEDIATE PERIOD (LENGTH 1d) AS (" + VIEW_SQL + ") PARTITION BY DAY");
            insertBaseRows();
            currentMicros = parseFloorPartialTimestamp("2024-01-02T01:00:00.000000Z");
            drainWalAndMatViewQueues();
            assertViewRows(VIEW_ROWS);
            Assert.assertEquals(parseFloorPartialTimestamp("2024-01-02T00:00:00.000000Z"), viewState().getLastPeriodHi());
            invalidate("test invalidation");

            currentMicros = parseFloorPartialTimestamp("2024-01-05T12:00:00.000000Z");
            assertResumeSucceeds();
            Assert.assertEquals(parseFloorPartialTimestamp("2024-01-05T00:00:00.000000Z"), viewState().getLastPeriodHi());
            assertViewStatus("valid", "");
            assertViewRows(VIEW_ROWS);
        });
    }

    @Test
    public void testPeriodViewWithTimeZoneMovesLastPeriodToCurrentBoundary() throws Exception {
        // The period boundary of a view whose periods follow a time zone is the local midnight: 23:00 UTC in January
        // in Berlin.
        assertMemoryLeak(() -> {
            currentMicros = parseFloorPartialTimestamp("2023-12-31T23:00:00.000000Z");
            createBase();
            execute("CREATE MATERIALIZED VIEW price_1h REFRESH IMMEDIATE PERIOD (LENGTH 1d TIME ZONE 'Europe/Berlin') AS ("
                    + VIEW_SQL + ") PARTITION BY DAY");
            insertBaseRows();
            currentMicros = parseFloorPartialTimestamp("2024-01-02T01:00:00.000000Z");
            drainWalAndMatViewQueues();
            assertViewRows(VIEW_ROWS);
            Assert.assertEquals(parseFloorPartialTimestamp("2024-01-01T23:00:00.000000Z"), viewState().getLastPeriodHi());
            invalidate("test invalidation");

            currentMicros = parseFloorPartialTimestamp("2024-01-05T12:00:00.000000Z");
            assertResumeSucceeds();
            Assert.assertEquals(parseFloorPartialTimestamp("2024-01-04T23:00:00.000000Z"), viewState().getLastPeriodHi());
            assertViewStatus("valid", "");
        });
    }

    @Test
    public void testResumeAfterFailedFullRebuildsRemovedPartitionRows() throws Exception {
        // A FULL that fails after its truncate leaves the view with the rows it rebuilt and a "never refreshed"
        // position. RESUME keeps that position, so the view's first build after it skips the partition clip: once
        // the base dropped an interior partition that the FULL had rebuilt, the first build removes the view's rows
        // there, as a second FULL would.
        setProperty(PropertyKey.CAIRO_MAT_VIEW_ROWS_PER_QUERY_ESTIMATE, 1);
        setProperty(PropertyKey.CAIRO_MAT_VIEW_INSERT_AS_SELECT_BATCH_SIZE, 1);
        assertMemoryLeak(() -> {
            createBase();
            execute("""
                    INSERT INTO base VALUES
                    ('a', 1.0, 1.0, '2024-01-01T00:10:00.000000Z'),
                    ('b', 2.0, 2.0, '2024-01-02T00:10:00.000000Z'),
                    ('c', 3.0, 3.0, '2024-01-03T00:10:00.000000Z')
                    """);
            drainWalQueue();
            createView("IMMEDIATE");
            drainWalAndMatViewQueues();
            final String allRows = """
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    2024-01-02T00:00:00.000000Z\tb\t2.0
                    2024-01-03T00:00:00.000000Z\tc\t3.0
                    """;
            assertViewRows(allRows);

            execute("REFRESH MATERIALIZED VIEW price_1h FULL");
            final AtomicLong commitCount = new AtomicLong();
            try (MatViewRefreshJob job = new MatViewRefreshJob(0, engine, 0)) {
                job.setOnBatchCommittedForTesting(() -> {
                    if (commitCount.incrementAndGet() == 2) {
                        throw CairoException.nonCritical().put("injected failure after the truncate");
                    }
                });
                drainWalQueue();
                drainMatViewQueue(job);
            }
            drainWalQueue();
            assertViewStatus("invalid", "[-1]: injected failure after the truncate");
            assertRefreshBaseTxn(-1);
            // the FULL rebuilt the first two days before it failed
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    2024-01-02T00:00:00.000000Z\tb\t2.0
                    """);

            execute("ALTER TABLE base DROP PARTITION LIST '2024-01-02'");
            drainWalAndMatViewQueues();
            assertResumeSucceeds();
            assertRefreshBaseTxn(-1);
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    2024-01-03T00:00:00.000000Z\tc\t3.0
                    """);
        });
    }

    @Test
    public void testResumeClearsRefreshIntervalCache() throws Exception {
        // RESUME jumps, so it clears the view's cached refresh intervals, also across a restart.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("MANUAL");
            execute("INSERT INTO base VALUES ('c', 3.0, 3.0, '2024-01-01T02:10:00.000000Z')");
            drainWalQueue();
            engine.getMatViewStateStore().enqueueUpdateRefreshIntervals(viewToken());
            drainWalAndMatViewQueues();
            Assert.assertEquals(2, viewState().getRefreshIntervals().size());
            invalidate("test invalidation");
            Assert.assertEquals(2, viewState().getRefreshIntervals().size());

            assertResumeSucceeds();
            assertCacheEmpty();
            drainWalAndMatViewQueues();
            restart();
            assertCacheEmpty();
            assertViewStatus("valid", "");
            // the gap is left to RANGE
            execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
            drainWalAndMatViewQueues();
            assertViewRows(VIEW_ROWS);
        });
    }

    @Test
    public void testResumeKeepsRowsAndJumpsToAppliedBaseTxn() throws Exception {
        // RESUME keeps the view's rows and positions it at the base's latest applied transaction. The rows the base
        // received while the view was invalid reach it through REFRESH ... RANGE, and later rows through the
        // incremental refresh as usual.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            invalidate("test invalidation");
            execute("INSERT INTO base VALUES ('a', 3.0, 3.0, '2024-01-01T00:20:00.000000Z')");
            execute("INSERT INTO base VALUES ('c', 4.0, 4.0, '2024-01-02T00:10:00.000000Z')");
            drainWalAndMatViewQueues();
            assertViewStatus("invalid", "test invalidation");
            assertRefreshBaseTxn(1);

            assertResumeSucceeds();
            assertViewStatus("valid", "");
            assertRefreshBaseTxn(3);
            drainWalAndMatViewQueues();
            assertViewRows(VIEW_ROWS);
            assertRefreshBaseTxn(3);

            execute("REFRESH MATERIALIZED VIEW price_1h RANGE FROM '2024-01-01' TO '2024-01-03'");
            drainWalAndMatViewQueues();
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t3.0
                    2024-01-01T01:00:00.000000Z\tb\t2.0
                    2024-01-02T00:00:00.000000Z\tc\t4.0
                    """);

            execute("INSERT INTO base VALUES ('a', 5.0, 5.0, '2024-01-02T00:30:00.000000Z')");
            drainWalAndMatViewQueues();
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t3.0
                    2024-01-01T01:00:00.000000Z\tb\t2.0
                    2024-01-02T00:00:00.000000Z\ta\t5.0
                    2024-01-02T00:00:00.000000Z\tc\t4.0
                    """);
            assertViewStatus("valid", "");
            assertRefreshBaseTxn(4);
        });
    }

    @Test
    public void testResumeOnValidViewDoesNothing() throws Exception {
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            final long viewSeqTxn = viewSeqTxn();
            execute(RESUME_SQL);
            Assert.assertEquals(viewSeqTxn, viewSeqTxn());
            assertViewStatus("valid", "");
            assertRefreshBaseTxn(1);
        });
    }

    @Test
    public void testResumeOnSuspendedViewWalRefuses() throws Exception {
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            invalidate("test invalidation");
            execute("ALTER MATERIALIZED VIEW price_1h SUSPEND WAL");
            assertExceptionNoLeakCheck(RESUME_SQL, VIEW_NAME_POSITION, "materialized view WAL is suspended, run ALTER MATERIALIZED VIEW price_1h RESUME WAL first");
            execute("ALTER MATERIALIZED VIEW price_1h RESUME WAL");
            drainWalAndMatViewQueues();
            assertResumeSucceeds();
            assertViewStatus("valid", "");
        });
    }

    @Test
    public void testResumeRefusesAtOnceWhileFullPending() throws Exception {
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            invalidate("test invalidation");
            viewState().markAsPendingFullRefreshForTesting();
            assertExceptionNoLeakCheck(RESUME_SQL, VIEW_NAME_POSITION, "materialized view full refresh is pending, it recovers the view [view=price_1h]");
            Assert.assertFalse(viewState().isLocked());
            assertViewStatus("invalid", "test invalidation");
        });
    }

    @Test
    public void testResumeRefusesMissingBase() throws Exception {
        // A dropped base fails RESUME's compile; the view keeps its rows.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            execute("DROP TABLE base");
            drainWalAndMatViewQueues();
            assertViewStatus("invalid", "base table is dropped or renamed");
            assertExceptionNoLeakCheck(RESUME_SQL, VIEW_NAME_POSITION, "materialized view SQL does not compile, fix it with ALTER MATERIALIZED VIEW ... AS first [view=price_1h, error=table does not exist [table=base]]");
            assertViewRows(VIEW_ROWS);
        });
    }

    @Test
    public void testResumeRefusesMissingBaseWhenSqlReadsAnotherTable() throws Exception {
        // RESUME resolves the base table by the name the view's definition holds. A SQL that compiles without it,
        // reading another table, still refuses when that name resolves to nothing.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            execute("CREATE TABLE other (sym SYMBOL, price DOUBLE, amount DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            invalidate("test invalidation");
            viewState().getViewDefinition().setMatViewSqlForTesting("SELECT ts, sym, last(price) AS price FROM other SAMPLE BY 1h");
            execute("DROP TABLE base");
            drainWalAndMatViewQueues();
            assertExceptionNoLeakCheck(RESUME_SQL, VIEW_NAME_POSITION, "base table does not exist [view=price_1h, baseTable=base]");
            Assert.assertFalse(viewState().isLocked());
            Assert.assertTrue(viewState().isInvalid());
        });
    }

    @Test
    public void testResumeRefusesViewWithoutState() throws Exception {
        // A view whose state the store does not hold, or holds as dropped, is not ready for RESUME.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            invalidate("test invalidation");
            engine.getMatViewStateStore().removeViewState(viewToken());
            assertExceptionNoLeakCheck(RESUME_SQL, VIEW_NAME_POSITION, "materialized view does not exist or is not ready for refresh");
            // INCREMENTAL finds no state to refuse on, a base invalidation no state to invalidate, and the WAL apply of
            // a view transaction no state to redeliver parked work of
            execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
            engine.getMatViewStateStore().enqueueInvalidateDependentViews(engine.verifyTableName("base"), "test invalidation");
            final long viewSeqTxn = viewSeqTxn();
            execute("ALTER MATERIALIZED VIEW price_1h ADD COLUMN extra DOUBLE");
            Assert.assertEquals(viewSeqTxn + 1, viewSeqTxn());
            drainWalAndMatViewQueues();
            Assert.assertEquals(viewSeqTxn + 1, engine.getTableSequencerAPI().getTxnTracker(viewToken()).getWriterTxn());
            engine.hydrateMatViewStateStore();
            Assert.assertTrue(viewState().isInvalid());

            viewState().markAsDropped();
            assertExceptionNoLeakCheck(RESUME_SQL, VIEW_NAME_POSITION, "materialized view does not exist or is not ready for refresh");
        });
    }

    @Test
    public void testResumeStopsWhenViewDroppedUnderLatch() throws Exception {
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            invalidate("test invalidation");
            final MatViewState state = viewState();
            state.setOnResumeHoldingLockForTesting(state::markAsDropped);
            assertExceptionNoLeakCheck(RESUME_SQL, VIEW_NAME_POSITION, "materialized view does not exist [view=price_1h]");
            Assert.assertFalse(state.isLocked());
            Assert.assertTrue(state.isInvalid());
        });
    }

    @Test
    public void testResumeStopsWhenViewDroppedWhileWaitingForWal() throws Exception {
        // RESUME waits for the view's WAL, and stops when the view is dropped meanwhile.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            invalidate("test invalidation");
            execute("ALTER MATERIALIZED VIEW price_1h ADD COLUMN extra DOUBLE");
            Assert.assertTrue(hasUnappliedViewWal());
            final AtomicBoolean isWaitSeen = new AtomicBoolean();
            viewState().setOnResumeWalWaitForTesting(() -> {
                isWaitSeen.set(true);
                try {
                    execute("DROP MATERIALIZED VIEW price_1h");
                } catch (SqlException e) {
                    throw new RuntimeException(e);
                }
            });
            assertExceptionNoLeakCheck(RESUME_SQL, VIEW_NAME_POSITION, "materialized view does not exist [view=price_1h]");
            Assert.assertTrue(isWaitSeen.get());
        });
    }

    @Test
    public void testResumeWaitForWalHonoursTimeout() throws Exception {
        // The wait for the view's WAL honours the statement timeout.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            invalidate("test invalidation");
            execute("ALTER MATERIALIZED VIEW price_1h ADD COLUMN extra DOUBLE");
            try (SqlExecutionContextImpl context = TestUtils.createSqlExecutionCtx(engine, 1);
                 NetworkSqlExecutionCircuitBreaker circuitBreaker = newCircuitBreaker()) {
                context.with(circuitBreaker);
                circuitBreaker.resetTimer();
                viewState().setOnResumeWalWaitForTesting(() -> circuitBreaker.setTimeout(SqlExecutionCircuitBreaker.TIMEOUT_FAIL_ON_FIRST_CHECK));
                try {
                    execute(RESUME_SQL, context);
                    Assert.fail();
                } catch (CairoException e) {
                    Assert.assertEquals(SqlExecutionCircuitBreaker.STATE_TIMEOUT, e.getInterruptionReason());
                    TestUtils.assertContains(e.getFlyweightMessage(), "timeout, query aborted");
                }
            }
            Assert.assertFalse(viewState().isLocked());
            drainWalAndMatViewQueues();
            assertViewStatus("invalid", "test invalidation");
        });
    }

    @Test
    public void testResumeOnViewThatBecameValidUnderLatchDoesNothing() throws Exception {
        // A view that another holder made valid before RESUME took the latch, as a FULL refresh would, needs no RESUME.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            invalidate("test invalidation");
            final MatViewState state = viewState();
            final long viewSeqTxn = viewSeqTxn();
            state.setOnResumeHoldingLockForTesting(state::markAsValid);
            execute(RESUME_SQL);
            Assert.assertEquals(viewSeqTxn, viewSeqTxn());
            Assert.assertFalse(state.isLocked());
            Assert.assertFalse(state.isResuming());
            assertRefreshBaseTxn(1);
        });
    }

    @Test
    public void testResumeRefusesUnmappedTimestamp() throws Exception {
        // RESUME checks the column mapping as refresh builds it: a SQL that no longer produces the view's designated
        // timestamp under its name refuses.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            invalidate("test invalidation");
            viewState().getViewDefinition().setMatViewSqlForTesting("SELECT ts AS ts2, sym, last(price) AS price FROM base SAMPLE BY 1h");
            assertExceptionNoLeakCheck(RESUME_SQL, VIEW_NAME_POSITION, "materialized view SQL does not match the view's columns, fix it with ALTER MATERIALIZED VIEW ... AS first [view=price_1h, error=materialized view query does not produce the designated timestamp column [column=ts]]");
            Assert.assertNull(viewState().getColumnMappingSummary());
            assertViewStatus("invalid", "test invalidation");
        });
    }

    @Test
    public void testResumeRefusesWithRetryLaterOnTimeout() throws Exception {
        // RESUME waits for another holder of the view latch within the statement timeout, and refuses with "retry
        // later" when the timeout expires first.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            invalidate("test invalidation");
            final MatViewState state = viewState();
            try (SqlExecutionContextImpl context = TestUtils.createSqlExecutionCtx(engine, 1);
                 NetworkSqlExecutionCircuitBreaker circuitBreaker = newCircuitBreaker()) {
                context.with(circuitBreaker);
                circuitBreaker.resetTimer();
                Assert.assertTrue(state.tryLock());
                try {
                    state.setOnResumeLatchBusyForTesting(() -> circuitBreaker.setTimeout(SqlExecutionCircuitBreaker.TIMEOUT_FAIL_ON_FIRST_CHECK));
                    try {
                        execute(RESUME_SQL, context);
                        Assert.fail();
                    } catch (SqlException e) {
                        Assert.assertEquals(VIEW_NAME_POSITION, e.getPosition());
                        TestUtils.assertContains(e.getFlyweightMessage(), "materialized view is busy, retry later [view=price_1h]");
                    }
                } finally {
                    MatViewRefreshJob.finalizeAndUnlock(engine, engine.getMatViewStateStore(), viewToken(), state, false);
                }
            }
            assertViewStatus("invalid", "test invalidation");
            assertResumeSucceeds();
        });
    }

    @Test
    public void testResumeStopsWhenCancelledWhileWaitingForLatch() throws Exception {
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            invalidate("test invalidation");
            final MatViewState state = viewState();
            try (SqlExecutionContextImpl context = TestUtils.createSqlExecutionCtx(engine, 1);
                 NetworkSqlExecutionCircuitBreaker circuitBreaker = newCircuitBreaker()) {
                context.with(circuitBreaker);
                circuitBreaker.resetTimer();
                Assert.assertTrue(state.tryLock());
                try {
                    state.setOnResumeLatchBusyForTesting(circuitBreaker::cancel);
                    try {
                        execute(RESUME_SQL, context);
                        Assert.fail();
                    } catch (CairoException e) {
                        Assert.assertTrue(e.isCancellation() || e.getInterruptionReason() == SqlExecutionCircuitBreaker.STATE_CANCELLED);
                        TestUtils.assertContains(e.getFlyweightMessage(), "cancelled by user");
                    }
                } finally {
                    MatViewRefreshJob.finalizeAndUnlock(engine, engine.getMatViewStateStore(), viewToken(), state, false);
                }
            }
            assertViewStatus("invalid", "test invalidation");
        });
    }

    @Test
    public void testResumeTelemetryEvent() throws Exception {
        // RESUME writes a MAT_VIEW_RESUME event that holds the view's refresh position after it and the mode.
        assertMemoryLeak(() -> {
            try (TelemetryJob telemetryJob = new TelemetryJob(engine)) {
                // The telemetry queue may hold the other tests' events, and throttles an event of the same view table
                // id within an interval of the test clock: flush them and move the clock past them.
                telemetryJob.runSerially();
                currentMicros = parseFloorPartialTimestamp("2099-01-01T00:00:00.000000Z");
                createBaseWithRefreshedView("IMMEDIATE");
                invalidate("test invalidation");
                execute("INSERT INTO base VALUES ('c', 3.0, 3.0, '2024-01-01T02:10:00.000000Z')");
                drainWalQueue();
                assertResumeSucceeds();
                telemetryJob.runSerially();
                assertQuery("SELECT event, base_table_txn, invalidation_reason FROM sys.telemetry_mat_view WHERE event = 205 AND view_table_id = "
                        + viewToken().getTableId() + " AND created >= '2099-01-01'")
                        .noLeakCheck()
                        .returns("""
                                event\tbase_table_txn\tinvalidation_reason
                                205\t2\tskip backlog
                                """);
            }
        });
    }

    @Test
    public void testResumePollsLatchUntilHolderReleases() throws Exception {
        // RESUME polls a busy latch, checking the statement's timeout and cancellation between attempts, until the
        // holder releases it.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            invalidate("test invalidation");
            final MatViewState state = viewState();
            final AtomicInteger checks = new AtomicInteger();
            try (SqlExecutionContextImpl context = TestUtils.createSqlExecutionCtx(engine, 1);
                 NetworkSqlExecutionCircuitBreaker circuitBreaker = new NetworkSqlExecutionCircuitBreaker(engine, new DefaultSqlExecutionCircuitBreakerConfiguration()) {
                     @Override
                     public void statefulThrowExceptionIfTrippedNoThrottle() {
                         super.statefulThrowExceptionIfTrippedNoThrottle();
                         if (checks.incrementAndGet() == 2) {
                             // the holder finishes after RESUME polled the latch twice
                             MatViewRefreshJob.finalizeAndUnlock(engine, engine.getMatViewStateStore(), viewToken(), state, false);
                         }
                     }

                     @Override
                     protected boolean testConnection(long fd) {
                         return false;
                     }
                 }) {
                context.with(circuitBreaker);
                Assert.assertTrue(state.tryLock());
                execute(RESUME_SQL, context);
            }
            Assert.assertTrue(checks.get() >= 2);
            Assert.assertFalse(state.isLocked());
            Assert.assertFalse(state.isInvalid());
        });
    }

    @Test
    public void testResumeReleaseWakeFailureKeepsStatementSuccessful() throws Exception {
        // The release of the latch wakes an invalidation published while RESUME held it. When that wake fails to
        // enqueue, RESUME logs it and succeeds, and the store's retry scan delivers the invalidation.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            invalidate("test invalidation");
            final MatViewStateStoreImpl store = (MatViewStateStoreImpl) engine.getMatViewStateStore();
            final AtomicBoolean isFailed = new AtomicBoolean();
            viewState().setOnResumeHoldingLockForTesting(() -> {
                store.enqueueInvalidate(viewToken(), "late invalidation");
                drainMatViewQueue(engine);
                store.setOnTaskQueueAppendForTesting(() -> {
                    if (isFailed.compareAndSet(false, true)) {
                        throw new OutOfMemoryError("test enqueue failure");
                    }
                });
            });
            try {
                execute(RESUME_SQL);
            } finally {
                store.setOnTaskQueueAppendForTesting(null);
            }
            Assert.assertTrue(isFailed.get());
            Assert.assertFalse(viewState().isLocked());
            drainWalAndMatViewQueues();
            drainWalAndMatViewQueues();
            assertViewStatus("invalid", "late invalidation");
        });
    }

    @Test
    public void testResumeOfOlderViewWithGeneratedNamesCopiesByPosition() throws Exception {
        // A view created before the explicit-name rule, whose SQL relies on generated names, copies by position;
        // RESUME checks its mapping the same way and reports it as leaving nothing out.
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRows();
            SqlParser.setMatViewExplicitNamesCheckSkipped(true);
            try {
                execute("CREATE MATERIALIZED VIEW price_1h AS (SELECT ts, sym, last(price) FROM base SAMPLE BY 1h) PARTITION BY DAY");
            } finally {
                SqlParser.setMatViewExplicitNamesCheckSkipped(false);
            }
            drainWalAndMatViewQueues();
            invalidate("test invalidation");
            assertResumeSucceeds();
            assertMappingColumns("", "");
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
        });
    }

    @Test
    public void testResumePassthroughView() throws Exception {
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRows();
            execute("CREATE MATERIALIZED VIEW base_copy AS (SELECT * FROM base) PARTITION BY DAY");
            drainWalAndMatViewQueues();
            final TableToken copyToken = engine.verifyTableName("base_copy");
            engine.getMatViewStateStore().enqueueInvalidate(copyToken, "test invalidation");
            drainWalAndMatViewQueues();
            execute("REFRESH MATERIALIZED VIEW base_copy RESUME SKIP BACKLOG");
            Assert.assertFalse(engine.getMatViewStateStore().getViewState(copyToken).isInvalid());
            execute("INSERT INTO base VALUES ('c', 3.0, 3.0, '2024-01-01T02:10:00.000000Z')");
            drainWalAndMatViewQueues();
            assertQuery("base_copy ORDER BY ts").timestamp("ts").noLeakCheck().expectSize().returns("""
                    sym\tprice\tamount\tts
                    a\t1.0\t1.0\t2024-01-01T00:10:00.000000Z
                    b\t2.0\t2.0\t2024-01-01T01:10:00.000000Z
                    c\t3.0\t3.0\t2024-01-01T02:10:00.000000Z
                    """);

            // The view's column check compiles the SQL with the passthrough reading, not RESUME's: the resumed view
            // takes a view ADD COLUMN, which the SQL does not produce, so the new column reads NULL.
            execute("ALTER MATERIALIZED VIEW base_copy ADD COLUMN extra DOUBLE");
            execute("INSERT INTO base VALUES ('d', 4.0, 4.0, '2024-01-01T03:10:00.000000Z')");
            drainWalAndMatViewQueues();
            Assert.assertFalse(engine.getMatViewStateStore().getViewState(copyToken).isInvalid());
            assertQuery("base_copy ORDER BY ts").timestamp("ts").noLeakCheck().expectSize().returns("""
                    sym\tprice\tamount\tts\textra
                    a\t1.0\t1.0\t2024-01-01T00:10:00.000000Z\tnull
                    b\t2.0\t2.0\t2024-01-01T01:10:00.000000Z\tnull
                    c\t3.0\t3.0\t2024-01-01T02:10:00.000000Z\tnull
                    d\t4.0\t4.0\t2024-01-01T03:10:00.000000Z\tnull
                    """);
        });
    }

    @Test
    public void testFullOnMissingBaseIsLeftToRefreshJob() throws Exception {
        // FULL refuses no missing base at the statement: the refresh job finds it missing and invalidates the view.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            execute("DROP TABLE base");
            execute("REFRESH MATERIALIZED VIEW price_1h FULL");
            drainWalAndMatViewQueues();
            drainWalAndMatViewQueues();
            Assert.assertTrue(viewState().isInvalid());
            assertViewRows(VIEW_ROWS);
        });
    }

    @Test
    public void testResumeWaitsForLatchHolder() throws Exception {
        // RESUME issued while another holder has the view latch waits and succeeds once the holder releases it.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            invalidate("test invalidation");
            final MatViewState state = viewState();
            Assert.assertTrue(state.tryLock());
            final AtomicBoolean isWaitSeen = new AtomicBoolean();
            state.setOnResumeLatchBusyForTesting(() -> {
                isWaitSeen.set(true);
                // the holder finishes its short task and releases the latch while RESUME waits
                MatViewRefreshJob.finalizeAndUnlock(engine, engine.getMatViewStateStore(), viewToken(), state, false);
            });
            execute(RESUME_SQL);
            Assert.assertTrue(isWaitSeen.get());
            Assert.assertFalse(state.isLocked());
            drainWalQueue();
            assertViewStatus("valid", "");
        });
    }

    @Test
    public void testResumeWaitsForViewAddColumn() throws Exception {
        // RESUME right after a view ADD COLUMN waits until the view's WAL applies it, and succeeds.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            invalidate("test invalidation");
            execute("ALTER MATERIALIZED VIEW price_1h ADD COLUMN extra DOUBLE");
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(viewToken());
            final long addColumnSeqTxn = tracker.getSeqTxn();
            Assert.assertTrue(tracker.getWriterTxn() < addColumnSeqTxn);

            final AtomicLong writerTxnAfterWait = new AtomicLong(-1);
            viewState().setOnResumeWalAppliedForTesting(() -> writerTxnAfterWait.set(tracker.getWriterTxn()));
            final AtomicReference<Throwable> applierError = new AtomicReference<>();
            final AtomicReference<Thread> applier = new AtomicReference<>();
            // RESUME finds the WAL not applied and waits; the WAL apply job runs meanwhile
            viewState().setOnResumeWalWaitForTesting(() -> {
                final Thread thread = new Thread(() -> {
                    try {
                        drainWalQueue();
                    } catch (Throwable th) {
                        applierError.set(th);
                    } finally {
                        Path.clearThreadLocals();
                    }
                });
                applier.set(thread);
                thread.start();
            });
            try {
                execute(RESUME_SQL);
            } finally {
                if (applier.get() != null) {
                    applier.get().join();
                }
            }
            Assert.assertNotNull(applier.get());
            Assert.assertNull(applierError.get());
            Assert.assertTrue(writerTxnAfterWait.get() >= addColumnSeqTxn);
            drainWalQueue();
            assertViewStatus("valid", "");
            assertMappingColumns("extra", "");
        });
    }

    @Test
    public void testViewWalTransactionBetweenWaitAndLatch() throws Exception {
        // A view WAL transaction that arrives after RESUME's WAL wait and before it takes the latch makes RESUME
        // release the latch, wait for that transaction to apply, and succeed.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            invalidate("test invalidation");
            final MatViewState state = viewState();
            final CountDownLatch applyGate = new CountDownLatch(1);
            final AtomicReference<Throwable> applierError = new AtomicReference<>();
            final Thread applier = new Thread(() -> {
                try {
                    applyGate.await();
                    drainWalQueue();
                } catch (Throwable th) {
                    applierError.set(th);
                } finally {
                    Path.clearThreadLocals();
                }
            });
            final AtomicBoolean isReleasedForWait = new AtomicBoolean();
            state.setOnResumeWalAppliedForTesting(() -> {
                try {
                    execute("ALTER MATERIALIZED VIEW price_1h ADD COLUMN extra DOUBLE");
                } catch (SqlException e) {
                    throw new RuntimeException(e);
                }
                // RESUME takes the latch, finds the new transaction unapplied and releases the latch to wait for it
                state.setOnBeforeUnlockForTesting(() -> {
                    isReleasedForWait.set(true);
                    applyGate.countDown();
                });
            });
            applier.start();
            try {
                execute(RESUME_SQL);
            } finally {
                applyGate.countDown();
                applier.join();
            }
            Assert.assertNull(applierError.get());
            Assert.assertTrue(isReleasedForWait.get());
            drainWalQueue();
            assertViewStatus("valid", "");
            assertMappingColumns("extra", "");
        });
    }

    // Waits until the thread waits for the lock under which MatViewState.recordBareNameSwitch() copies and publishes the
    // view's bare-name switches, or until the latch is counted down, as when the thread got past the lock at once.
    private static void awaitBareNameSwitchLockOrLatch(Thread thread, CountDownLatch latch) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (latch.getCount() > 0) {
            final Thread.State state = thread.getState();
            if (state == Thread.State.BLOCKED) {
                final StackTraceElement[] frames = thread.getStackTrace();
                if (frames.length > 0 && "recordBareNameSwitch".equals(frames[0].getMethodName())) {
                    return;
                }
            }
            Assert.assertNotEquals("the thread ended", Thread.State.TERMINATED, state);
            Assert.assertTrue("the thread neither waits for the lock nor got past it [state=" + state + ']', System.nanoTime() < deadline);
            Os.pause();
        }
    }

    // Applies the WAL on another thread, so that the test thread can run a statement while the apply pauses.
    private static Thread startWalApplier(AtomicReference<Throwable> applierError) {
        final Thread applier = new Thread(() -> {
            try {
                drainWalQueue();
            } catch (Throwable th) {
                applierError.set(th);
            } finally {
                Path.clearThreadLocals();
            }
        });
        applier.start();
        return applier;
    }

    // The refusal of RESUME for a bare name of the view's SQL whose meaning a base column change switched.
    private static String switchedRefusal(String bareName) {
        return "materialized view SQL reads a bare name whose meaning a base column change switched, fix it with ALTER "
                + "MATERIALIZED VIEW ... AS first [view=price_1h, error=" + MatViewBareNameGuard.invalidationReason(bareName) + ']';
    }

    private static NetworkSqlExecutionCircuitBreaker newCircuitBreaker() {
        return new NetworkSqlExecutionCircuitBreaker(engine, new DefaultSqlExecutionCircuitBreakerConfiguration() {
            @Override
            public int getCircuitBreakerThrottle() {
                return 0;
            }
        }) {
            @Override
            protected boolean testConnection(long fd) {
                return false;
            }
        };
    }

    // The last sequencer txn that the table has applied.
    private static long appliedTxn(TableToken tableToken) {
        return engine.getTableSequencerAPI().getTxnTracker(tableToken).getWriterTxn();
    }

    private void assertCacheEmpty() {
        final LongList intervals = viewState().getRefreshIntervals();
        Assert.assertEquals(0, intervals.size());
        Assert.assertEquals(-1, viewState().getRefreshIntervalsBaseTxn());
    }

    private void assertMappingColumns(String unmapped, String ignored) throws Exception {
        assertQuery("SELECT unmapped_view_columns, ignored_query_columns FROM materialized_views() WHERE view_name = 'price_1h'")
                .noLeakCheck()
                .noRandomAccess()
                .returns("unmapped_view_columns\tignored_query_columns\n"
                        + (unmapped != null ? unmapped : "") + "\t" + (ignored != null ? ignored : "") + "\n");
        if (unmapped == null) {
            Assert.assertNull(viewState().getColumnMappingSummary());
        }
    }

    private void assertRefreshBaseTxn(long expected) {
        Assert.assertEquals(expected, viewState().getLastRefreshBaseTxn());
    }

    // RESUME returns once the view state event is in the view's WAL; materialized_views() reports the state the
    // view's WAL applied, so the WAL applies it before the caller looks.
    private void assertResumeSucceeds() throws SqlException {
        execute(RESUME_SQL);
        Assert.assertFalse(viewState().isInvalid());
        Assert.assertFalse(viewState().isLocked());
        Assert.assertFalse(viewState().isResuming());
        drainWalQueue();
    }

    private void assertViewRows(String expected) throws Exception {
        assertQuery("price_1h ORDER BY ts, sym").timestamp("ts").noLeakCheck().expectSize().returns(expected);
    }

    // An older view calls count without parentheses; while the view is invalid, the change gives the base a column
    // named count. Plain RESUME and RESUME SKIP BACKLOG refuse on the switch.
    private void assertBareCallSwitchedWhileInvalidRefused(TestUtils.LeakProneCode change) throws Exception {
        createBase();
        insertBaseRows();
        createBareCountView();
        invalidate("test invalidation");
        change.run();
        drainWalAndMatViewQueues();
        assertViewStatus("invalid", "test invalidation");
        assertSwitchRefused(PLAIN_RESUME_SQL, "count", "test invalidation", BARE_COUNT_VIEW_ROWS);
        assertSwitchRefused(RESUME_SQL, "count", "test invalidation", BARE_COUNT_VIEW_ROWS);
    }

    // The base of an older view that calls count without parentheses gains a column named count, and then two rows of
    // the hour 02:00, while the view is invalid. RESUME refuses on the switch until ALTER ... AS calls count(), and
    // plain RESUME then catches the view up on the two rows.
    private void assertBareCallSwitchedWhileInvalidRefusedThenRecovered(String refreshType) throws Exception {
        final boolean isManual = "MANUAL".equals(refreshType);
        createBase();
        insertBaseRows();
        createOlderView("REFRESH " + refreshType + " ", BARE_COUNT_VIEW_SQL);
        if (isManual) {
            execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
            drainWalAndMatViewQueues();
        }
        assertViewRows(BARE_COUNT_VIEW_ROWS);
        invalidate("test invalidation");
        execute("ALTER TABLE base ADD COLUMN count LONG");
        execute(INSERT_C_ROWS);
        drainWalAndMatViewQueues();
        assertViewStatus("invalid", "test invalidation");
        assertSwitchRefused(PLAIN_RESUME_SQL, "count", "test invalidation", BARE_COUNT_VIEW_ROWS);
        assertSwitchRefused(RESUME_SQL, "count", "test invalidation", BARE_COUNT_VIEW_ROWS);

        execute("ALTER MATERIALIZED VIEW price_1h AS (" + CALL_COUNT_VIEW_SQL + ")");
        drainWalQueue();
        execute(PLAIN_RESUME_SQL);
        drainWalAndMatViewQueues();
        if (isManual) {
            execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
            drainWalAndMatViewQueues();
        }
        assertViewStatus("valid", "");
        assertViewRows(BARE_COUNT_VIEW_ROWS + "2024-01-01T02:00:00.000000Z\tc\t2\t6.0\n");
        // the view holds what its SQL gives on the base
        assertSqlCursors(CALL_COUNT_VIEW_SQL + " ORDER BY ts, sym", "price_1h ORDER BY ts, sym");
    }

    // RESUME refuses on the switch of the bare name, and leaves the view as it was: its status, reason and rows, with
    // no view transaction, no pending invalidation, the latch free and the backlog-lost marker kept.
    private void assertSwitchRefused(String resumeSql, String bareName, String reason, String rows) throws Exception {
        assertSwitchRefusedInPlace(resumeSql, bareName);
        final long viewSeqTxn = viewSeqTxn();
        drainWalAndMatViewQueues();
        Assert.assertEquals(viewSeqTxn, viewSeqTxn());
        assertViewStatus("invalid", reason);
        assertViewRows(rows);
    }

    // The part of assertSwitchRefused() that runs no job, for a refusal while another thread applies the WAL.
    private void assertSwitchRefusedInPlace(String resumeSql, String bareName) throws Exception {
        final long viewSeqTxn = viewSeqTxn();
        final String backlogLostReason = viewState().getBacklogLostReason();
        assertExceptionNoLeakCheck(resumeSql, VIEW_NAME_POSITION, switchedRefusal(bareName));
        Assert.assertTrue(viewState().isInvalid());
        Assert.assertFalse(viewState().hasPendingInvalidationReason());
        Assert.assertFalse(viewState().isLocked());
        Assert.assertFalse(viewState().isResuming());
        Assert.assertEquals(backlogLostReason, viewState().getBacklogLostReason());
        Assert.assertEquals(viewSeqTxn, viewSeqTxn());
    }

    // The base drops its column count, which the invalid view reads bare, and RESUME SKIP BACKLOG runs after the
    // bare-name guard recorded the switch and before the guard reads the view's flags, on the test thread while
    // another thread applies the drop: RESUME's compile still reads count as the column, and RESUME refuses on the
    // switch. The guard then finds the view invalid and keeps its reason.
    private void assertSwitchRefusedWhileGuardedDropPends(String bareName, String reason) throws Exception {
        final CountDownLatch recorded = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        viewState().setOnBareNameSwitchRecordedForTesting(() -> {
            recorded.countDown();
            try {
                Assert.assertTrue(release.await(60, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        execute("ALTER TABLE base DROP COLUMN count");
        final AtomicReference<Throwable> applierError = new AtomicReference<>();
        final Thread applier = startWalApplier(applierError);
        try {
            Assert.assertTrue(recorded.await(30, TimeUnit.SECONDS));
            assertSwitchRefusedInPlace(RESUME_SQL, bareName);
        } finally {
            release.countDown();
            applier.join();
        }
        Assert.assertNull(applierError.get());
        drainWalAndMatViewQueues();
        assertViewStatus("invalid", reason);
    }

    private void assertViewStatus(String status, String reason) throws Exception {
        assertQuery("SELECT view_status, invalidation_reason FROM materialized_views() WHERE view_name = 'price_1h'")
                .noLeakCheck()
                .noRandomAccess()
                .returns("view_status\tinvalidation_reason\n" + status + "\t" + reason + "\n");
    }

    // A view, created before the explicit-name rule, whose SQL calls count without parentheses, refreshed from the
    // base's two rows.
    private void createBareCountView() throws Exception {
        createOlderView(BARE_COUNT_VIEW_SQL);
        assertViewRows(BARE_COUNT_VIEW_ROWS);
    }

    // A base with a column named count, and a view created on this version whose SQL reads the column bare, refreshed
    // from the base's two rows at base txn 1.
    private void createCountColumnBaseWithView() throws Exception {
        execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, amount DOUBLE, count LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("""
                INSERT INTO base VALUES
                ('a', 1.0, 1.0, 7, '2024-01-01T00:10:00.000000Z'),
                ('b', 2.0, 2.0, 8, '2024-01-01T01:10:00.000000Z')
                """);
        drainWalQueue();
        execute("CREATE MATERIALIZED VIEW price_1h AS (" + BARE_COUNT_VIEW_SQL + ") PARTITION BY DAY");
        drainWalAndMatViewQueues();
        assertViewRows(COUNT_COLUMN_VIEW_ROWS);
        assertRefreshBaseTxn(1);
    }

    // A view as the versions before the explicit-name rule store it: a bare name that matches no column calls the
    // zero-argument function of that name.
    private void createOlderView(String viewSql) throws Exception {
        createOlderView("", viewSql);
    }

    private void createOlderView(String refreshClause, String viewSql) throws Exception {
        SqlParser.setMatViewExplicitNamesCheckSkipped(true);
        try {
            execute("CREATE MATERIALIZED VIEW price_1h " + refreshClause + "AS (" + viewSql + ") PARTITION BY DAY");
        } finally {
            SqlParser.setMatViewExplicitNamesCheckSkipped(false);
        }
        drainWalAndMatViewQueues();
    }

    private void createBase() throws SqlException {
        execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, amount DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
    }

    // A base with two rows and a view refreshed from them at base txn 1.
    private void createBaseWithRefreshedView(String refreshType) throws Exception {
        createBase();
        insertBaseRows();
        createView(refreshType);
        if (!"IMMEDIATE".equals(refreshType)) {
            drainWalQueue();
            execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
        }
        drainWalAndMatViewQueues();
        assertViewRows(VIEW_ROWS);
        assertViewStatus("valid", "");
        assertRefreshBaseTxn(1);
    }

    private void createView(String refreshType) throws SqlException {
        execute("CREATE MATERIALIZED VIEW price_1h REFRESH " + refreshType + " AS (" + VIEW_SQL + ") PARTITION BY DAY");
    }

    private boolean hasUnappliedViewWal() {
        final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(viewToken());
        return tracker.getWriterTxn() < tracker.getSeqTxn();
    }

    private void insertBaseRows() throws SqlException {
        execute("""
                INSERT INTO base VALUES
                ('a', 1.0, 1.0, '2024-01-01T00:10:00.000000Z'),
                ('b', 2.0, 2.0, '2024-01-01T01:10:00.000000Z')
                """);
    }

    private void invalidate(String reason) {
        engine.getMatViewStateStore().enqueueInvalidate(viewToken(), reason);
        drainWalAndMatViewQueues();
        Assert.assertTrue(viewState().isInvalid());
    }

    // Drops the base table and re-creates it under the same name with a row, and drops the invalidation the drop
    // queued, as a crash would. The new table's commit notification stays queued.
    private void replaceBaseLosingInvalidation() throws SqlException {
        execute("DROP TABLE base");
        final MatViewRefreshTask task = new MatViewRefreshTask();
        Assert.assertTrue(engine.getMatViewStateStore().tryDequeueRefreshTask(task));
        Assert.assertEquals(MatViewRefreshTask.INVALIDATE, task.operation);
        Assert.assertFalse(engine.getMatViewStateStore().tryDequeueRefreshTask(task));
        createBase();
        execute("INSERT INTO base VALUES ('y', 8.0, 8.0, '2024-01-01T00:20:00.000000Z')");
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
