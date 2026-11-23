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
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.DatabaseCheckpointAgent;
import io.questdb.cairo.ErrorTag;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.file.BlockFileReader;
import io.questdb.cairo.mv.MatViewBacklog;
import io.questdb.cairo.mv.MatViewDefinition;
import io.questdb.cairo.mv.MatViewRefreshJob;
import io.questdb.cairo.mv.MatViewRefreshTask;
import io.questdb.cairo.mv.MatViewState;
import io.questdb.cairo.mv.MatViewStateReader;
import io.questdb.cairo.mv.MatViewStateStoreImpl;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.wal.WalEventCursor;
import io.questdb.cairo.wal.WalEventReader;
import io.questdb.cairo.wal.WalTxnType;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.cairo.wal.seq.TableTransactionLogFile;
import io.questdb.cairo.wal.seq.TableTransactionLogV1;
import io.questdb.cairo.wal.seq.TransactionLogCursor;
import io.questdb.griffin.DefaultSqlExecutionCircuitBreakerConfiguration;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.Files;
import io.questdb.std.LongList;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.LogCapture;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * {@code ALTER MATERIALIZED VIEW v [WITH BASE b] AS (<select>)}: replaces a view's SQL, and with WITH BASE naming
 * another table its base table, keeping the view's rows. Without a base change the view stays valid and its next
 * refresh uses the new SQL; a base change leaves the view invalid, with its backlog lost, until
 * {@code RESUME SKIP BACKLOG} positions it on the new base table.
 * <p>
 * Expected rows come from the base rows each test inserts: a bucket of the 1h view holds the last price, or the
 * expression the new SQL computes, of its symbol's base rows within the hour.
 */
public class AlterMatViewAsTest extends AbstractCairoTest {
    // the error of the rule ALTER MATERIALIZED VIEW ... AS applies to an aggregating view's designated timestamp
    private static final String AGGREGATING_TIMESTAMP_ERROR = "aggregating materialized view timestamp must bucket the base table designated timestamp without transformation";
    // the rows of the view av of createAggregatingView()
    private static final String AGGREGATING_VIEW_ROWS = """
            ts\tsym\tprice
            2024-01-01T10:00:00.000000Z\ta\t1.0
            2024-01-01T11:00:00.000000Z\tb\t2.0
            """;
    private static final String ALTER_PREFIX = "ALTER MATERIALIZED VIEW price_1h ";
    private static final String BASE2_SQL = "SELECT ts, sym, last(price) AS price FROM base2 SAMPLE BY 1h";
    // the start of the refusal of a view ALTER that waits for the definition event of an earlier ALTER ... AS while the
    // apply of the view's WAL is suspended
    private static final String DEFINITION_NOT_APPLIED = "materialized view WAL is suspended with an earlier ALTER MATERIALIZED VIEW ... AS not applied, ";
    // column names of the base of the positional copy tests, see createLongNameBase()
    private static final String L64 = "long_col_" + "w".repeat(55);
    private static final String L65 = "long_col_" + "y".repeat(56);
    private static final String M65 = "more_col_" + "z".repeat(56);
    // the hourly maximum and minimum of the price in the base rows of createLongNameBase() and insertSecondHour()
    private static final String MIN_MAX_SQL = "SELECT ts, max(price) mx, min(price) mn FROM base SAMPLE BY 1h";
    // the error of the rule CREATE MATERIALIZED VIEW applies to a passthrough view's designated timestamp
    private static final String PASSTHROUGH_TIMESTAMP_ERROR = "passthrough materialized view timestamp must reference the base table designated timestamp without transformation";
    // the rows of the view pv of createPassthroughBase()
    private static final String PASSTHROUGH_VIEW_ROWS = """
            ts\tsym\tprice
            2024-01-01T10:00:00.000000Z\ta\t1.0
            2024-01-01T11:00:00.000000Z\tb\t2.0
            """;
    private static final String POSITIONAL_COPY_HINT = "; the query relies on generated column names, so refresh copies its columns by position: "
            + "keep each view column at its position, or give the query's columns explicit names";
    // a REFRESH ... RANGE over the two hours of createBaseWithRefreshedView()
    private static final String RANGE_SQL = "REFRESH MATERIALIZED VIEW price_1h RANGE FROM '2024-01-01T00:00:00.000000Z' TO '2024-01-01T01:59:59.999999Z'";
    // a SQL whose rows tell it from VIEW_SQL: ten times the hourly maximum price
    private static final String TEN_TIMES_MAX_SQL = "SELECT ts, sym, max(price) * 10 AS price FROM base SAMPLE BY 1h";
    private static final String RESUME_SKIP_SQL = "REFRESH MATERIALIZED VIEW price_1h RESUME SKIP BACKLOG";
    private static final String RESUME_SQL = "REFRESH MATERIALIZED VIEW price_1h RESUME";
    private static final int VIEW_NAME_POSITION = ALTER_PREFIX.indexOf("price_1h");
    private static final int REFRESH_VIEW_NAME_POSITION = "REFRESH MATERIALIZED VIEW ".length();
    private static final String VIEW_ROWS = """
            ts\tsym\tprice
            2024-01-01T00:00:00.000000Z\ta\t1.0
            2024-01-01T01:00:00.000000Z\tb\t2.0
            """;
    private static final String VIEW_SQL = "SELECT ts, sym, last(price) AS price FROM base SAMPLE BY 1h";
    // a SQL that also produces the column x, the average price of the hour, which the view lacks
    private static final String X_SQL = "SELECT ts, sym, last(price) AS price, avg(price) AS x FROM base SAMPLE BY 1h";

    @After
    public void releaseCheckpoint() throws Exception {
        // a test that failed while it held a checkpoint must not fail the tests that follow it
        engine.checkpointRelease();
    }

    @After
    public void tearDownSeams() {
        DatabaseCheckpointAgent.setOnMatViewDefinitionCheckpointedForTesting(null);
        DatabaseCheckpointAgent.setOnBeforeMatViewIntervalMergeForTesting(null);
    }

    @Test
    public void testAlterTelemetryEvent() throws Exception {
        // ALTER ... AS writes a MAT_VIEW_ALTER event with the view's refresh position, and the text "base change" for
        // a base change, empty otherwise.
        assertMemoryLeak(() -> {
            try (TelemetryJob telemetryJob = new TelemetryJob(engine)) {
                telemetryJob.runSerially();
                currentMicros = parseFloorPartialTimestamp("2099-01-01T00:00:00.000000Z");
                createBaseWithRefreshedView("IMMEDIATE");
                createBase2WithRow();
                execute(ALTER_PREFIX + "AS (SELECT ts, sym, max(price) AS price FROM base SAMPLE BY 1h)");
                telemetryJob.runSerially();
                // the telemetry throttles events of the same view within an interval of the test clock
                currentMicros += 3_600_000_000L;
                // the base change waits until the view's WAL applies the first statement's definition event
                drainWalQueue();
                execute(ALTER_PREFIX + "WITH BASE base2 AS (" + BASE2_SQL + ")");
                telemetryJob.runSerially();
                assertQuery("SELECT event, base_table_txn, invalidation_reason FROM sys.telemetry_mat_view WHERE event = 206 AND view_table_id = "
                        + viewToken().getTableId() + " AND created >= '2099-01-01' ORDER BY created")
                        .noLeakCheck()
                        .returns("""
                                event\tbase_table_txn\tinvalidation_reason
                                206\t1\t
                                206\t1\tbase change
                                """);
            }
        });
    }

    @Test
    public void testAlterWithoutBaseChangeKeepsRowsAndPosition() throws Exception {
        // Without a base change the view stays valid with its rows and refresh position, and the refresh that follows
        // the next base commit recomputes the buckets it touches with the new SQL.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            final long viewSeqTxn = viewSeqTxn();
            execute(ALTER_PREFIX + "AS (SELECT ts, sym, max(price) * 10 AS price FROM base SAMPLE BY 1h)");
            // one definition event, no view state event
            Assert.assertEquals(viewSeqTxn + 1, viewSeqTxn());
            Assert.assertFalse(viewState().isInvalid());
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            assertViewRows(VIEW_ROWS);
            assertRefreshBaseTxn(1);
            Assert.assertEquals("SELECT ts, sym, max(price) * 10 AS price FROM base SAMPLE BY 1h", viewDefinition().getMatViewSql());
            Assert.assertEquals("base", viewDefinition().getBaseTableName());
            execute("INSERT INTO base VALUES ('a', 3.0, 1.0, '2024-01-01T00:20:00.000000Z')");
            drainWalAndMatViewQueues();
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t30.0
                    2024-01-01T01:00:00.000000Z\tb\t2.0
                    """);
            assertRefreshBaseTxn(2);
            // a restart loads the new definition
            restartEngine((restarted, context) -> Assert.assertEquals(
                    "SELECT ts, sym, max(price) * 10 AS price FROM base SAMPLE BY 1h",
                    restarted.getDependentViewGraph().getViewDefinition(restarted.verifyTableName("price_1h")).getMatViewSql()
            ));
        });
    }

    @Test
    public void testBackToBackBaseRevertFollowsSecondStatement() throws Exception {
        // A second ALTER ... WITH BASE right after a first one waits until the view's WAL applies the first, and compares
        // its base table with the first one's, as when the WAL applies between the statements: moving the view back to
        // its old base table is a base change, and the view ends on the old base table with the second SQL.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            createBase2WithRow();
            final long viewSeqTxn = viewSeqTxn();
            execute(ALTER_PREFIX + "WITH BASE base2 AS (" + BASE2_SQL + ")");
            final AtomicBoolean isWaitSeen = new AtomicBoolean();
            viewState().setOnDefinitionWaitForTesting(() -> {
                // the second statement waits before the view's WAL applies the first
                Assert.assertEquals("base", viewDefinition().getBaseTableName());
                isWaitSeen.set(true);
                drainWalQueue();
            });
            final String secondSql = "SELECT ts, sym, max(price) * 10 AS price FROM base SAMPLE BY 1h";
            execute(ALTER_PREFIX + "WITH BASE base AS (" + secondSql + ")");
            Assert.assertTrue(isWaitSeen.get());
            // each statement writes its invalidation, then its definition event
            Assert.assertEquals(viewSeqTxn + 4, viewSeqTxn());
            try (WalEventReader eventReader = new WalEventReader(configuration); Path path = new Path()) {
                Assert.assertEquals(WalTxnType.MAT_VIEW_INVALIDATE, openViewEvent(eventReader, path, viewSeqTxn + 3).getType());
                final WalEventCursor definition = openViewEvent(eventReader, path, viewSeqTxn + 4);
                Assert.assertEquals(WalTxnType.MAT_VIEW_DEFINITION, definition.getType());
                TestUtils.assertEquals("base", definition.getMatViewDefinitionInfo().getBaseTableName());
                TestUtils.assertEquals(secondSql, definition.getMatViewDefinitionInfo().getMatViewSql());
                Assert.assertTrue(definition.getMatViewDefinitionInfo().isBaseTableChange());
            }
            drainWalAndMatViewQueues();
            Assert.assertEquals("base", viewDefinition().getBaseTableName());
            Assert.assertEquals(secondSql, viewDefinition().getMatViewSql());
            assertViewStatus("invalid", "base table changed");
            Assert.assertEquals(
                    "CREATE MATERIALIZED VIEW 'price_1h' WITH BASE 'base' REFRESH IMMEDIATE AS (\n" + secondSql + "\n) PARTITION BY DAY;\n",
                    queryShowCreate("price_1h")
            );
            assertDependents("base", "price_1h");
            assertDependents("base2");

            execute(RESUME_SKIP_SQL);
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            Assert.assertEquals(engine.verifyTableName("base").getTableId(), viewState().getBaseTableId());
            // the second SQL computes max(price) * 10 of the new bucket
            execute("INSERT INTO base VALUES ('c', 3.0, 1.0, '2024-01-01T02:10:00.000000Z')");
            drainWalAndMatViewQueues();
            final String rows = VIEW_ROWS + "2024-01-01T02:00:00.000000Z\tc\t30.0\n";
            assertViewRows(rows);
            // base2's commits no longer reach the view
            execute("INSERT INTO base2 VALUES ('d', 7.0, 1.0, '2024-01-03T00:10:00.000000Z')");
            drainWalAndMatViewQueues();
            assertViewRows(rows);
        });
    }

    @Test
    public void testBackToBackBaseRevertThenFullRebuildsFromSecondBase() throws Exception {
        // A FULL refresh right after two ALTER ... WITH BASE statements, the second of which moves the view back to its
        // old base table, rebuilds the view from the old base table with the second SQL, as for a single move there.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            createBase2WithRow();
            execute(ALTER_PREFIX + "WITH BASE base2 AS (" + BASE2_SQL + ")");
            final AtomicBoolean isAlterWaitSeen = armSeamDrain();
            execute(ALTER_PREFIX + "WITH BASE base AS (SELECT ts, sym, max(price) * 10 AS price FROM base SAMPLE BY 1h)");
            Assert.assertTrue(isAlterWaitSeen.get());
            final AtomicBoolean isRefreshWaitSeen = armSeamDrain();
            execute("REFRESH MATERIALIZED VIEW price_1h FULL");
            Assert.assertTrue(isRefreshWaitSeen.get());
            drainMatViewQueue(engine);
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            Assert.assertEquals("base", viewDefinition().getBaseTableName());
            // max(price) * 10 of each bucket of base, and no row of base2's 2024-01-02
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t10.0
                    2024-01-01T01:00:00.000000Z\tb\t20.0
                    """);
        });
    }

    @Test
    public void testBaseChangeDefinitionWriteFailure() throws Exception {
        // A base change whose definition event fails to write after the invalidation leaves the view invalid on its old
        // base table, with the latch and the WAL writer released, and the statement runs again.
        setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 1);
        final AtomicBoolean isFailArmed = new AtomicBoolean();
        final StringSink failingPart = new StringSink();
        assertMemoryLeak(newSeqPartFailingFacade(isFailArmed, failingPart, true), () -> {
            createBaseWithRefreshedView("IMMEDIATE");
            createBase2WithRow();
            final long viewSeqTxn = viewSeqTxn();
            // the invalidation takes the view's next transaction, the definition event the one after it
            armViewTxnFailure(isFailArmed, failingPart, 1);
            try {
                execute(ALTER_PREFIX + "WITH BASE base2 AS (" + BASE2_SQL + ")");
                Assert.fail("the definition event write must fail");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "could not open");
            }
            Assert.assertFalse(isFailArmed.get());
            Assert.assertFalse(viewState().isLocked());
            Assert.assertTrue(viewState().isInvalid());
            Assert.assertEquals(viewSeqTxn + 1, viewSeqTxn());
            drainWalAndMatViewQueues();
            assertViewStatus("invalid", "base table changed");
            Assert.assertEquals("base", viewDefinition().getBaseTableName());
            assertViewRows(VIEW_ROWS);
            Assert.assertEquals(-1, viewState().getPendingDefinitionSeqTxn());

            execute(ALTER_PREFIX + "WITH BASE base2 AS (" + BASE2_SQL + ")");
            drainWalAndMatViewQueues();
            Assert.assertEquals("base2", viewDefinition().getBaseTableName());
            assertViewStatus("invalid", "base table changed");
            execute(RESUME_SKIP_SQL);
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            Assert.assertEquals(engine.verifyTableName("base2").getTableId(), viewState().getBaseTableId());
        });
    }

    @Test
    public void testBaseChangeOnViewInvalidOnlyInMemoryWritesInvalidation() throws Exception {
        // A refresh failure whose view state event fails to write leaves the view invalid in memory only. A base change
        // still writes its invalidation, so that the view's persisted state never says valid next to the new base.
        setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 1);
        final AtomicBoolean isFailArmed = new AtomicBoolean();
        final StringSink failingPart = new StringSink();
        assertMemoryLeak(newSeqPartFailingFacade(isFailArmed, failingPart, false), () -> {
            createBaseWithRefreshedView("IMMEDIATE");
            createBase2WithRow();
            execute("ALTER TABLE base DROP COLUMN price");
            drainWalAndMatViewQueues();
            Assert.assertFalse(viewState().isInvalid());
            execute("INSERT INTO base (sym, amount, ts) VALUES ('c', 3.0, '2024-01-01T02:10:00.000000Z')");
            drainWalQueue();
            final long viewSeqTxn = viewSeqTxn();
            // the refresh fails to compile the view's SQL, and every write of its invalidation fails
            armViewTxnFailure(isFailArmed, failingPart, 0);
            drainMatViewQueue(engine);
            isFailArmed.set(false);
            Assert.assertTrue(viewState().isInvalid());
            Assert.assertEquals(viewSeqTxn, viewSeqTxn());
            // the state on disk is still valid
            restartStates();
            Assert.assertFalse(viewState().isInvalid());
            viewState().markAsInvalid("in memory only");

            execute(ALTER_PREFIX + "WITH BASE base2 AS (" + BASE2_SQL + ")");
            Assert.assertEquals(viewSeqTxn + 2, viewSeqTxn());
            drainWalQueue();
            restartStates();
            Assert.assertTrue(viewState().isInvalid());
            Assert.assertEquals(MatViewBacklog.LOST_BASE_TABLE_CHANGED, viewState().getBacklogLostReason());
            assertViewStatus("invalid", "base table changed");
        });
    }

    @Test
    public void testBaseChangeRefusesAtOnceWhileFullRefreshPending() throws Exception {
        // A FULL refresh parked on an invalid view, which waits for the view's WAL, makes a base change refuse at once.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            createBase2WithRow();
            engine.getMatViewStateStore().enqueueInvalidate(viewToken(), "test invalidation");
            drainMatViewQueue(engine);
            Assert.assertTrue(viewState().isInvalid());
            // the invalidation event is not applied yet, so the FULL refresh parks
            execute("REFRESH MATERIALIZED VIEW price_1h FULL");
            drainMatViewQueue(engine);
            final long viewSeqTxn = viewSeqTxn();
            assertExceptionNoLeakCheck(
                    ALTER_PREFIX + "WITH BASE base2 AS (" + BASE2_SQL + ")",
                    VIEW_NAME_POSITION,
                    "materialized view full refresh is pending, retry later [view=price_1h]"
            );
            Assert.assertEquals(viewSeqTxn, viewSeqTxn());
            Assert.assertFalse(viewState().isLocked());
            // the FULL refresh runs once the view's WAL applies, and the base change then goes through
            drainWalAndMatViewQueues();
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            execute(ALTER_PREFIX + "WITH BASE base2 AS (" + BASE2_SQL + ")");
            drainWalAndMatViewQueues();
            assertViewStatus("invalid", "base table changed");
        });
    }

    @Test
    public void testBaseChangeRefusesRetryLaterOnTimeout() throws Exception {
        // A base change waits for another holder of the view latch within the statement timeout, and refuses with
        // "retry later" when the timeout expires first, writing nothing.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            createBase2WithRow();
            final MatViewState state = viewState();
            final long viewSeqTxn = viewSeqTxn();
            try (SqlExecutionContextImpl context = TestUtils.createSqlExecutionCtx(engine, 1);
                 NetworkSqlExecutionCircuitBreaker circuitBreaker = newCircuitBreaker()) {
                context.with(circuitBreaker);
                circuitBreaker.resetTimer();
                Assert.assertTrue(state.tryLock());
                try {
                    state.setOnResumeLatchBusyForTesting(() -> circuitBreaker.setTimeout(SqlExecutionCircuitBreaker.TIMEOUT_FAIL_ON_FIRST_CHECK));
                    try {
                        execute(ALTER_PREFIX + "WITH BASE base2 AS (" + BASE2_SQL + ")", context);
                        Assert.fail();
                    } catch (SqlException e) {
                        Assert.assertEquals(VIEW_NAME_POSITION, e.getPosition());
                        TestUtils.assertContains(e.getFlyweightMessage(), "materialized view is busy, retry later [view=price_1h]");
                    }
                } finally {
                    MatViewRefreshJob.finalizeAndUnlock(engine, engine.getMatViewStateStore(), viewToken(), state, false);
                }
            }
            Assert.assertEquals(viewSeqTxn, viewSeqTxn());
            Assert.assertFalse(state.isInvalid());
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            Assert.assertEquals("base", viewDefinition().getBaseTableName());
        });
    }

    @Test
    public void testBaseChangeThenResumeSkipBacklog() throws Exception {
        // A base change leaves the view invalid with its rows, and its backlog lost: plain RESUME refuses, RESUME SKIP
        // BACKLOG positions the view on the new base table, whose later commits reach the view, and the old base
        // table's do not.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            createBase2WithRow();
            final int base2Id = engine.verifyTableName("base2").getTableId();
            final long viewSeqTxn = viewSeqTxn();

            execute(ALTER_PREFIX + "WITH BASE base2 AS (" + BASE2_SQL + ")");
            // the invalidation, then the definition event
            Assert.assertEquals(viewSeqTxn + 2, viewSeqTxn());
            Assert.assertTrue(viewState().isInvalid());
            Assert.assertEquals(MatViewBacklog.LOST_BASE_TABLE_CHANGED, viewState().getBacklogLostReason());
            Assert.assertFalse(viewState().isLocked());
            // a restart before the view's WAL applies the two events loads the invalidation, which the definition
            // event follows in the view's WAL
            restartStates();
            Assert.assertTrue(viewState().isInvalid());
            Assert.assertEquals("base table changed", viewState().getInvalidationReasonForReport());
            Assert.assertEquals(MatViewBacklog.LOST_BASE_TABLE_CHANGED, viewState().getBacklogLostReason());
            drainWalAndMatViewQueues();
            assertViewStatus("invalid", "base table changed");
            Assert.assertEquals("base2", viewDefinition().getBaseTableName());
            Assert.assertEquals(BASE2_SQL, viewDefinition().getMatViewSql());
            assertViewRows(VIEW_ROWS);
            // the dependency graph moved the view's edge
            assertDependents("base");
            assertDependents("base2", "price_1h");

            assertResumeRefused("base table changed");
            execute(RESUME_SKIP_SQL);
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            Assert.assertEquals(base2Id, viewState().getBaseTableId());
            Assert.assertEquals(engine.getTableSequencerAPI().getTxnTracker(engine.verifyTableName("base2")).getWriterTxn(), viewState().getLastRefreshBaseTxn());
            // the row base2 held at RESUME is the gap
            assertViewRows(VIEW_ROWS);
            execute("INSERT INTO base2 VALUES ('d', 7.0, 1.0, '2024-01-03T00:10:00.000000Z')");
            drainWalAndMatViewQueues();
            final String rows = """
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    2024-01-01T01:00:00.000000Z\tb\t2.0
                    2024-01-03T00:00:00.000000Z\td\t7.0
                    """;
            assertViewRows(rows);
            execute("INSERT INTO base VALUES ('e', 9.0, 1.0, '2024-01-04T00:10:00.000000Z')");
            drainWalAndMatViewQueues();
            assertViewRows(rows);
            assertViewStatus("valid", "");
        });
    }

    @Test
    public void testBaseChangeThenResumeSkipBacklogAndRangeSurviveRestart() throws Exception {
        // After a base change, RESUME SKIP BACKLOG positions the view on the new base table, and a RANGE refresh commits
        // above RESUME's view state event. A restart before the view's WAL applies them loads the view valid on the new
        // base, with no backlog-lost marker, and the new base's next commit reaches the view.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            createBase2WithRow();
            final int base2Id = engine.verifyTableName("base2").getTableId();
            execute(ALTER_PREFIX + "WITH BASE base2 AS (" + BASE2_SQL + ")");
            drainWalAndMatViewQueues();
            assertViewStatus("invalid", "base table changed");
            execute(RESUME_SKIP_SQL);
            execute("REFRESH MATERIALIZED VIEW price_1h RANGE FROM '2024-01-02' TO '2024-01-03'");
            // the refresh job runs the RANGE, the WAL apply job does not
            drainMatViewQueue(engine);
            Assert.assertTrue(viewSeqTxn() > engine.getTableSequencerAPI().getTxnTracker(viewToken()).getWriterTxn());

            restartStates();
            Assert.assertFalse(viewState().isInvalid());
            Assert.assertNull(viewState().getBacklogLostReason());
            Assert.assertEquals(base2Id, viewState().getBaseTableId());
            // RESUME jumped to base2's applied transaction, the insert of c
            assertRefreshBaseTxn(1);
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            execute("INSERT INTO base2 VALUES ('d', 7.0, 1.0, '2024-01-03T00:10:00.000000Z')");
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            assertRefreshBaseTxn(2);
            // the old base's rows stay, the RANGE recomputed c's day from base2, and d reached the view
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    2024-01-01T01:00:00.000000Z\tb\t2.0
                    2024-01-02T00:00:00.000000Z\tc\t5.0
                    2024-01-03T00:00:00.000000Z\td\t7.0
                    """);
        });
    }

    @Test
    public void testBaseChangeWaitsForRefreshHoldingLatch() throws Exception {
        // A base change issued while a refresh holds the view latch waits for the refresh to finish, then goes through.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            createBase2WithRow();
            execute("INSERT INTO base VALUES ('c', 3.0, 1.0, '2024-01-01T02:10:00.000000Z')");
            drainWalQueue();
            final CountDownLatch isWaiting = new CountDownLatch(1);
            viewState().setOnResumeLatchBusyForTesting(isWaiting::countDown);
            final AtomicReference<Throwable> alterError = new AtomicReference<>();
            final Thread alter = new Thread(() -> {
                try (SqlExecutionContext context = TestUtils.createSqlExecutionCtx(engine)) {
                    engine.execute(ALTER_PREFIX + "WITH BASE base2 AS (" + BASE2_SQL + ")", context);
                } catch (Throwable th) {
                    alterError.set(th);
                }
            });
            try (MatViewRefreshJob job = new MatViewRefreshJob(0, engine, 0)) {
                job.setOnHoldingLockForTesting(() -> {
                    alter.start();
                    await(isWaiting);
                });
                drainMatViewQueue(job);
            }
            alter.join();
            Assert.assertNull(alterError.get());
            drainWalAndMatViewQueues();
            assertViewStatus("invalid", "base table changed");
            Assert.assertEquals("base2", viewDefinition().getBaseTableName());
            // the refresh committed before the base change
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    2024-01-01T01:00:00.000000Z\tb\t2.0
                    2024-01-01T02:00:00.000000Z\tc\t3.0
                    """);
        });
    }

    @Test
    public void testBaseChangeWaitsWhileResumeHoldsLatch() throws Exception {
        // On an invalid view, a base change waits while RESUME holds the latch: RESUME's view state event, positioned
        // on the old base table, lands before the base change's invalidation, never after its definition event.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            createBase2WithRow();
            invalidate("test invalidation");
            final MatViewState state = viewState();
            final CountDownLatch isWaiting = new CountDownLatch(1);
            final AtomicReference<Throwable> alterError = new AtomicReference<>();
            final Thread alter = new Thread(() -> {
                try (SqlExecutionContext context = TestUtils.createSqlExecutionCtx(engine)) {
                    engine.execute(ALTER_PREFIX + "WITH BASE base2 AS (" + BASE2_SQL + ")", context);
                } catch (Throwable th) {
                    alterError.set(th);
                }
            });
            state.setOnResumeLatchBusyForTesting(isWaiting::countDown);
            state.setOnResumeHoldingLockForTesting(() -> {
                alter.start();
                await(isWaiting);
            });
            execute(RESUME_SKIP_SQL);
            alter.join();
            Assert.assertNull(alterError.get());
            Assert.assertTrue(state.isInvalid());
            drainWalAndMatViewQueues();
            assertViewStatus("invalid", "base table changed");
            Assert.assertEquals("base2", viewDefinition().getBaseTableName());
            Assert.assertEquals(MatViewBacklog.LOST_BASE_TABLE_CHANGED, state.getBacklogLostReason());
            assertViewRows(VIEW_ROWS);
        });
    }

    @Test
    public void testAlterSqlForms() throws Exception {
        // The AS block takes the forms of CREATE's: a SELECT without parentheses, a SELECT with WITH clauses, and *.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            execute(ALTER_PREFIX + "AS SELECT ts, sym, max(price) AS price FROM base SAMPLE BY 1h");
            drainWalQueue();
            Assert.assertEquals("SELECT ts, sym, max(price) AS price FROM base SAMPLE BY 1h", viewDefinition().getMatViewSql());
            final String cteSql = "WITH c AS (SELECT ts, sym, price * 2 AS price FROM base) SELECT ts, sym, max(price) AS price FROM c SAMPLE BY 1h";
            execute(ALTER_PREFIX + "AS (" + cteSql + ")");
            drainWalQueue();
            Assert.assertEquals(cteSql, viewDefinition().getMatViewSql());
            execute("INSERT INTO base VALUES ('a', 3.0, 1.0, '2024-01-01T00:20:00.000000Z')");
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t6.0
                    2024-01-01T01:00:00.000000Z\tb\t2.0
                    """);

            execute("CREATE MATERIALIZED VIEW base_copy AS (SELECT ts, sym, price FROM base) PARTITION BY DAY");
            drainWalAndMatViewQueues();
            execute("ALTER MATERIALIZED VIEW base_copy AS (SELECT * FROM base)");
            assertQuery("SELECT unmapped_view_columns, ignored_query_columns FROM materialized_views() WHERE view_name = 'base_copy'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("unmapped_view_columns\tignored_query_columns\n\tamount\n");
            drainWalAndMatViewQueues();
            Assert.assertEquals("SELECT * FROM base", engine.getDependentViewGraph().getViewDefinition(engine.verifyTableName("base_copy")).getMatViewSql());
        });
    }

    @Test
    public void testBaseResolution() throws Exception {
        // Without WITH BASE the base table stays, and a SQL that no longer reads it fails. WITH BASE naming the current
        // base table, in any case, is no base change.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            createBase2WithRow();
            final String otherTable = ALTER_PREFIX + "AS (" + BASE2_SQL + ")";
            assertExceptionNoLeakCheck(
                    otherTable,
                    otherTable.indexOf("SELECT"),
                    "materialized view query does not read its base table, name the new base table with WITH BASE [baseTable=base]"
            );

            final long viewSeqTxn = viewSeqTxn();
            execute(ALTER_PREFIX + "WITH BASE BASE AS (SELECT ts, sym, max(price) AS price FROM base SAMPLE BY 1h)");
            Assert.assertEquals(viewSeqTxn + 1, viewSeqTxn());
            Assert.assertFalse(viewState().isInvalid());
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            Assert.assertEquals("base", viewDefinition().getBaseTableName());
            Assert.assertEquals("SELECT ts, sym, max(price) AS price FROM base SAMPLE BY 1h", viewDefinition().getMatViewSql());
            assertViewRows(VIEW_ROWS);
            assertRefreshBaseTxn(1);
        });
    }

    @Test
    public void testCheckpointCancelledAtDefinitionRetry() throws Exception {
        // A cancel at the definition retry of the pass that writes a never-refreshed view again, after a second ALTER,
        // fails CHECKPOINT CREATE with the cancel. That pass wrote the view into the checkpoint, not into the live view,
        // which keeps no state file and stays valid, and the next checkpoint succeeds.
        Assume.assumeTrue(Os.type != Os.WINDOWS);
        assertMemoryLeak(() -> {
            createBaseWithNeverRefreshedView();
            final TableToken viewToken = viewToken();
            final AtomicBoolean isRetrySeamRun = new AtomicBoolean();
            try (NetworkSqlExecutionCircuitBreaker circuitBreaker = newCircuitBreaker()) {
                circuitBreaker.resetTimer();
                final Runnable retrySeam = () -> {
                    applyAlter(ALTER_PREFIX + "SET REFRESH LIMIT 2 HOURS");
                    circuitBreaker.cancel();
                    isRetrySeamRun.set(true);
                };
                DatabaseCheckpointAgent.setOnMatViewDefinitionCheckpointedForTesting(() -> {
                    applyAlter(ALTER_PREFIX + "SET REFRESH LIMIT 1 HOUR");
                    // the seam runs once: arm it again for the pass that writes the view again
                    DatabaseCheckpointAgent.setOnMatViewDefinitionCheckpointedForTesting(retrySeam);
                });
                try {
                    engine.checkpointCreate(circuitBreaker, false);
                    Assert.fail("the cancel must fail the checkpoint");
                } catch (CairoException e) {
                    final StringSink expected = new StringSink();
                    expected.put("error creating checkpoint [table=").put(viewToken).put(", error=cancelled by user]");
                    TestUtils.assertEquals(expected, e.getFlyweightMessage());
                }
            }
            Assert.assertTrue(isRetrySeamRun.get());
            assertNoLiveViewState(viewToken);
            // the pass that the cancel stopped wrote the definition of the first ALTER into the checkpoint
            Assert.assertEquals(1, readCheckpointedDefinition(viewToken).getRefreshLimitHoursOrMonths());
            Assert.assertEquals(2, viewDefinition().getRefreshLimitHoursOrMonths());
            assertViewStatus("valid", "");
            execute("CHECKPOINT CREATE");
            Assert.assertEquals(2, readCheckpointedDefinition(viewToken).getRefreshLimitHoursOrMonths());
            execute("CHECKPOINT RELEASE");
            assertNoLiveViewState(viewToken);
        });
    }

    @Test
    public void testCheckpointDefinitionAppliedBeforeTableCopy() throws Exception {
        // A definition event that applies after CHECKPOINT CREATE wrote the view's definition and before it copied the
        // view's table makes the checkpoint write the view again, so a restore has the new definition.
        Assume.assumeTrue(Os.type != Os.WINDOWS);
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            final String newSql = "SELECT ts, sym, max(price) AS price FROM base SAMPLE BY 1h";
            final AtomicBoolean isSeamRun = new AtomicBoolean();
            DatabaseCheckpointAgent.setOnMatViewDefinitionCheckpointedForTesting(() -> {
                try {
                    execute(ALTER_PREFIX + "AS (" + newSql + ")");
                } catch (SqlException e) {
                    throw new AssertionError(e);
                }
                drainWalQueue();
                isSeamRun.set(true);
            });
            execute("CHECKPOINT CREATE");
            Assert.assertTrue(isSeamRun.get());
            restoreCheckpoint();
            Assert.assertEquals(newSql, viewDefinition().getMatViewSql());
            execute("CHECKPOINT RELEASE");
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            assertViewRows(VIEW_ROWS);
        });
    }

    @Test
    public void testCheckpointDefinitionAppliedBeforeTableCopyOfNeverRefreshedView() throws Exception {
        // A definition event that applies after CHECKPOINT CREATE wrote the definition of a view that has never
        // refreshed, and so has no state file, makes the checkpoint write the view again, into the checkpoint: the live
        // view keeps the files the ALTER left, with no state file, and stays valid, the checkpoint holds the altered
        // definition and no state, and the next checkpoint succeeds. The cases cover each kind of ALTER that replaces
        // the definition (SET REFRESH LIMIT, SET REFRESH EVERY and AS), a full and an incremental checkpoint, and a
        // view name of multi-byte characters.
        Assume.assumeTrue(Os.type != Os.WINDOWS);
        final String newSql = "SELECT ts, sym, max(price) AS price FROM base SAMPLE BY 1h";
        final ObjList<CheckpointAlterCase> cases = new ObjList<>();
        cases.add(new CheckpointAlterCase("price_1h", "SET REFRESH LIMIT 1 HOUR", false, definition -> {
            Assert.assertEquals(MatViewDefinition.REFRESH_TYPE_MANUAL, definition.getRefreshType());
            Assert.assertEquals(1, definition.getRefreshLimitHoursOrMonths());
            Assert.assertEquals(VIEW_SQL, definition.getMatViewSql());
        }));
        cases.add(new CheckpointAlterCase("price_1h", "SET REFRESH EVERY 1h", false, definition -> {
            Assert.assertEquals(MatViewDefinition.REFRESH_TYPE_TIMER, definition.getRefreshType());
            Assert.assertEquals(1, definition.getTimerInterval());
            Assert.assertEquals('h', definition.getTimerUnit());
            Assert.assertEquals(VIEW_SQL, definition.getMatViewSql());
        }));
        cases.add(new CheckpointAlterCase("price_1h", "AS (" + newSql + ")", false, definition -> {
            Assert.assertEquals(MatViewDefinition.REFRESH_TYPE_MANUAL, definition.getRefreshType());
            Assert.assertEquals(newSql, definition.getMatViewSql());
        }));
        cases.add(new CheckpointAlterCase("price_1h", "SET REFRESH LIMIT 1 HOUR", true, definition -> {
            Assert.assertEquals(MatViewDefinition.REFRESH_TYPE_MANUAL, definition.getRefreshType());
            Assert.assertEquals(1, definition.getRefreshLimitHoursOrMonths());
        }));
        cases.add(new CheckpointAlterCase("цена_1ч", "AS (" + newSql + ")", false, definition -> {
            Assert.assertEquals(MatViewDefinition.REFRESH_TYPE_MANUAL, definition.getRefreshType());
            Assert.assertEquals(newSql, definition.getMatViewSql());
        }));
        assertMemoryLeak(() -> {
            createBase("base");
            execute("INSERT INTO base VALUES ('a', 1.0, 1.0, '2024-01-01T00:10:00.000000Z')");
            drainWalQueue();
            for (int i = 0, n = cases.size(); i < n; i++) {
                final CheckpointAlterCase alterCase = cases.getQuick(i);
                try {
                    final String viewName = alterCase.viewName();
                    createNeverRefreshedView(viewName, VIEW_SQL);
                    final TableToken viewToken = engine.verifyTableName(viewName);
                    final StringSink liveFilesAfterAlter = new StringSink();
                    DatabaseCheckpointAgent.setOnMatViewDefinitionCheckpointedForTesting(() -> {
                        applyAlter("ALTER MATERIALIZED VIEW \"" + viewName + "\" " + alterCase.alterTail());
                        liveFilesAfterAlter.put(listLiveViewDir(viewToken));
                    });
                    if (alterCase.isIncremental()) {
                        engine.checkpointCreate(sqlExecutionContext.getCircuitBreaker(), true);
                    } else {
                        execute("CHECKPOINT CREATE");
                    }
                    Assert.assertTrue("the seam must run", liveFilesAfterAlter.length() > 0);
                    // the checkpoint writes nothing into the live view
                    TestUtils.assertEquals(liveFilesAfterAlter, listLiveViewDir(viewToken));
                    assertNoLiveViewState(viewToken);
                    Assert.assertFalse(isCheckpointFilePresent(viewToken, MatViewState.MAT_VIEW_STATE_FILE_NAME));
                    alterCase.definitionCheck().accept(readCheckpointedDefinition(viewToken));
                    assertViewStatus(viewName, "valid", "");
                    execute("CHECKPOINT RELEASE");
                    execute("CHECKPOINT CREATE");
                    execute("CHECKPOINT RELEASE");
                    // the checkpoint of the next case copies one view, the one its seam alters
                    execute("DROP MATERIALIZED VIEW \"" + viewName + "\"");
                    drainWalQueue();
                } catch (Throwable th) {
                    throw new AssertionError(alterCase.describe(), th);
                }
            }
        });
    }

    @Test
    public void testCheckpointDefinitionRetryAfterUnreadableViewState() throws Exception {
        // A view state file that CHECKPOINT CREATE finds but cannot read makes it copy no state. When an ALTER then
        // makes it write the view again, it writes into the checkpoint, not into the live view.
        Assume.assumeTrue(Os.type != Os.WINDOWS);
        final AtomicReference<Utf8String> unreadableStateFile = new AtomicReference<>();
        // reports the file present while it is set, and BlockFileReader then finds it too small to read
        final TestFilesFacadeImpl unreadableStateFacade = new TestFilesFacadeImpl() {
            @Override
            public boolean exists(LPSZ path) {
                final Utf8String file = unreadableStateFile.get();
                return (file != null && Utf8s.equals(file, path)) || super.exists(path);
            }
        };
        assertMemoryLeak(unreadableStateFacade, () -> {
            createBaseWithNeverRefreshedView();
            final TableToken viewToken = viewToken();
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(viewToken).concat(MatViewState.MAT_VIEW_STATE_FILE_NAME);
                unreadableStateFile.set(new Utf8String(path.toString()));
            }
            final AtomicBoolean isSeamRun = new AtomicBoolean();
            DatabaseCheckpointAgent.setOnMatViewDefinitionCheckpointedForTesting(() -> {
                unreadableStateFile.set(null);
                applyAlter(ALTER_PREFIX + "SET REFRESH LIMIT 1 HOUR");
                isSeamRun.set(true);
            });
            try {
                execute("CHECKPOINT CREATE");
            } finally {
                unreadableStateFile.set(null);
            }
            Assert.assertTrue(isSeamRun.get());
            assertNoLiveViewState(viewToken);
            Assert.assertFalse(isCheckpointFilePresent(viewToken, MatViewState.MAT_VIEW_STATE_FILE_NAME));
            Assert.assertEquals(1, readCheckpointedDefinition(viewToken).getRefreshLimitHoursOrMonths());
            assertViewStatus("valid", "");
            execute("CHECKPOINT RELEASE");
        });
    }

    @Test
    public void testCheckpointDefinitionRetryAfterViewStateAppears() throws Exception {
        // A view that has no state file when CHECKPOINT CREATE writes its definition, and has one when an ALTER makes
        // the checkpoint write the view again, since a refresh ran in between: the checkpoint holds the altered
        // definition and the refreshed state, and the live state file stays as the refresh wrote it.
        Assume.assumeTrue(Os.type != Os.WINDOWS);
        assertMemoryLeak(() -> {
            createBaseWithNeverRefreshedView();
            final TableToken viewToken = viewToken();
            final String newSql = "SELECT ts, sym, max(price) AS price FROM base SAMPLE BY 1h";
            final AtomicBoolean isSeamRun = new AtomicBoolean();
            DatabaseCheckpointAgent.setOnMatViewDefinitionCheckpointedForTesting(() -> {
                // REFRESH waits for the definition event of the ALTER to apply
                applyAlter(ALTER_PREFIX + "AS (" + newSql + ")");
                try {
                    execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
                } catch (SqlException e) {
                    throw new AssertionError(e);
                }
                drainWalAndMatViewQueues();
                isSeamRun.set(true);
            });
            execute("CHECKPOINT CREATE");
            Assert.assertTrue(isSeamRun.get());
            // the refresh caught up with the base's one commit
            Assert.assertEquals(1, readLiveState(viewToken).getLastRefreshBaseTxn());
            Assert.assertEquals(1, readCheckpointedState(viewToken).getLastRefreshBaseTxn());
            Assert.assertEquals(newSql, readCheckpointedDefinition(viewToken).getMatViewSql());
            assertViewStatus("valid", "");
            execute("CHECKPOINT RELEASE");
        });
    }

    @Test
    public void testCheckpointDefinitionRetryKeepsLiveViewAcrossRestart() throws Exception {
        // After CHECKPOINT CREATE wrote a never-refreshed view again for an ALTER, a restart loads the view with the
        // altered definition and no refresh, valid, and takes a checkpoint.
        Assume.assumeTrue(Os.type != Os.WINDOWS);
        assertMemoryLeak(() -> {
            createBaseWithNeverRefreshedView();
            final AtomicBoolean isSeamRun = new AtomicBoolean();
            DatabaseCheckpointAgent.setOnMatViewDefinitionCheckpointedForTesting(() -> {
                applyAlter(ALTER_PREFIX + "SET REFRESH LIMIT 1 HOUR");
                isSeamRun.set(true);
            });
            execute("CHECKPOINT CREATE");
            Assert.assertTrue(isSeamRun.get());
            execute("CHECKPOINT RELEASE");
            restartEngine((restarted, context) -> {
                final TableToken viewToken = restarted.verifyTableName("price_1h");
                Assert.assertEquals(1, restarted.getDependentViewGraph().getViewDefinition(viewToken).getRefreshLimitHoursOrMonths());
                final MatViewState state = restarted.getMatViewStateStore().getViewState(viewToken);
                Assert.assertFalse(state.isInvalid());
                Assert.assertEquals(-1, state.getLastRefreshBaseTxn());
                assertQuery("SELECT view_name, view_status, invalidation_reason FROM materialized_views()")
                        .withEngine(restarted)
                        .withContext(context)
                        .noLeakCheck()
                        .noRandomAccess()
                        .returns("""
                                view_name\tview_status\tinvalidation_reason
                                price_1h\tvalid\t
                                """);
                restarted.execute("CHECKPOINT CREATE", context);
                restarted.execute("CHECKPOINT RELEASE", context);
            });
        });
    }

    @Test
    public void testCheckpointIntervalMergeUsesCheckpointedBaseTable() throws Exception {
        // An incremental checkpoint merges the gap intervals of the base table that the checkpointed definition names
        // into the view's checkpointed state, also when a base change applies between the table copy and the merge.
        Assume.assumeTrue(Os.type != Os.WINDOWS);
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("MANUAL");
            // another view, which a DROP removes before the merge, takes no merge
            execute("CREATE MATERIALIZED VIEW other_1h REFRESH MANUAL AS (" + VIEW_SQL + ") PARTITION BY DAY");
            drainWalQueue();
            execute("REFRESH MATERIALIZED VIEW other_1h INCREMENTAL");
            drainWalAndMatViewQueues();
            final TableToken otherToken = engine.verifyTableName("other_1h");
            createBase2WithRow();
            for (int i = 0; i < 4; i++) {
                execute("INSERT INTO base2 VALUES ('z', " + i + ", 1.0, '2024-02-0" + (i + 1) + "T00:10:00.000000Z')");
            }
            // the gap of the view's base table: base txns 2 and 3
            execute("INSERT INTO base VALUES ('a', 3.0, 1.0, '2024-01-01T00:20:00.000000Z')");
            execute("INSERT INTO base VALUES ('c', 4.0, 1.0, '2024-01-02T00:10:00.000000Z')");
            drainWalAndMatViewQueues();
            final long baseSeqTxn = engine.getTableSequencerAPI().getTxnTracker(engine.verifyTableName("base")).getSeqTxn();
            Assert.assertEquals(3, baseSeqTxn);
            Assert.assertEquals(5, engine.getTableSequencerAPI().getTxnTracker(engine.verifyTableName("base2")).getSeqTxn());

            DatabaseCheckpointAgent.setOnBeforeMatViewIntervalMergeForTesting(() -> {
                try {
                    execute(ALTER_PREFIX + "WITH BASE base2 AS (" + BASE2_SQL + ")");
                    execute("DROP MATERIALIZED VIEW other_1h");
                } catch (SqlException e) {
                    throw new AssertionError(e);
                }
                drainWalQueue();
            });
            engine.checkpointCreate(sqlExecutionContext.getCircuitBreaker(), true);
            try {
                Assert.assertEquals(-1, readCheckpointedState(otherToken).getRefreshIntervalsBaseTxn());
                Assert.assertEquals("base2", viewDefinition().getBaseTableName());
                final MatViewStateReader stateReader = readCheckpointedState();
                Assert.assertFalse(stateReader.isInvalid());
                Assert.assertEquals(1, stateReader.getLastRefreshBaseTxn());
                Assert.assertEquals(baseSeqTxn, stateReader.getRefreshIntervalsBaseTxn());
                Assert.assertEquals(engine.verifyTableName("base").getTableId(), stateReader.getBaseTableId());
                final LongList expected = new LongList();
                expected.add(parseFloorPartialTimestamp("2024-01-01T00:20:00.000000Z"), parseFloorPartialTimestamp("2024-01-01T00:20:00.000000Z"));
                expected.add(parseFloorPartialTimestamp("2024-01-02T00:10:00.000000Z"), parseFloorPartialTimestamp("2024-01-02T00:10:00.000000Z"));
                TestUtils.assertEquals(expected, stateReader.getRefreshIntervals());
            } finally {
                execute("CHECKPOINT RELEASE");
            }
        });
    }

    @Test
    public void testCheckpointRestoreOfNeverRefreshedViewAlteredBeforeTableCopy() throws Exception {
        // A checkpoint of a never-refreshed view whose SQL an ALTER replaced before the checkpoint copied the view's
        // table holds no state, and restores the new SQL and a view that has never refreshed, whose first refresh
        // builds it with the new SQL. The base table has a one-character name: a view definition misread as a view
        // state then parses without an error, so only what the checkpoint holds shows a write into the wrong file.
        Assume.assumeTrue(Os.type != Os.WINDOWS);
        assertMemoryLeak(() -> {
            createBase("b");
            execute("""
                    INSERT INTO b VALUES
                    ('a', 1.0, 1.0, '2024-01-01T00:10:00.000000Z'),
                    ('a', 3.0, 3.0, '2024-01-01T00:20:00.000000Z'),
                    ('b', 2.0, 2.0, '2024-01-01T01:10:00.000000Z')
                    """);
            drainWalQueue();
            createNeverRefreshedView("price_1h", "SELECT ts, sym, last(price) AS price FROM b SAMPLE BY 1h");
            final TableToken viewToken = viewToken();
            final String newSql = "SELECT ts, sym, max(price) AS price FROM b SAMPLE BY 1h";
            final AtomicBoolean isSeamRun = new AtomicBoolean();
            DatabaseCheckpointAgent.setOnMatViewDefinitionCheckpointedForTesting(() -> {
                applyAlter(ALTER_PREFIX + "AS (" + newSql + ")");
                isSeamRun.set(true);
            });
            execute("CHECKPOINT CREATE");
            Assert.assertTrue(isSeamRun.get());
            Assert.assertFalse(isCheckpointFilePresent(viewToken, MatViewState.MAT_VIEW_STATE_FILE_NAME));
            // nothing refreshes the view before the restore, so it has no live state file either
            assertNoLiveViewState(viewToken);
            restoreCheckpoint();
            Assert.assertEquals(newSql, viewDefinition().getMatViewSql());
            Assert.assertFalse(viewState().isInvalid());
            assertRefreshBaseTxn(-1);
            execute("CHECKPOINT RELEASE");
            execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            // the maximum price of each symbol within the hour
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t3.0
                    2024-01-01T01:00:00.000000Z\tb\t2.0
                    """);
            assertSqlCursors(newSql + " ORDER BY ts, sym", "price_1h ORDER BY ts, sym");
        });
    }

    @Test
    public void testFullAfterDroppedBaseColumnSucceedsOnceAlterFixesSql() throws Exception {
        // A view whose SQL reads a dropped base column goes invalid, and FULL keeps it invalid with its rows; once
        // ALTER ... AS fixes the SQL, FULL rebuilds it, leaving the column the SQL no longer produces NULL.
        assertMemoryLeak(() -> {
            createBase("base");
            execute("""
                    INSERT INTO base VALUES
                    ('a', 1.0, 10.0, '2024-01-01T00:10:00.000000Z'),
                    ('b', 2.0, 20.0, '2024-01-01T01:10:00.000000Z')
                    """);
            execute("CREATE MATERIALIZED VIEW price_1h AS (SELECT ts, sym, last(price) AS price, last(amount) AS amount FROM base SAMPLE BY 1h) PARTITION BY DAY");
            drainWalAndMatViewQueues();
            final String rows = """
                    ts\tsym\tprice\tamount
                    2024-01-01T00:00:00.000000Z\ta\t1.0\t10.0
                    2024-01-01T01:00:00.000000Z\tb\t2.0\t20.0
                    """;
            assertViewRows(rows);
            execute("ALTER TABLE base DROP COLUMN amount");
            execute("INSERT INTO base VALUES ('c', 3.0, '2024-01-01T02:10:00.000000Z')");
            drainWalAndMatViewQueues();
            assertViewStatus("invalid", "[43]: Invalid column: amount");
            execute("REFRESH MATERIALIZED VIEW price_1h FULL");
            drainWalAndMatViewQueues();
            drainWalAndMatViewQueues();
            assertViewStatus("invalid", "[43]: Invalid column: amount");
            assertViewRows(rows);

            execute(ALTER_PREFIX + "AS (" + VIEW_SQL + ")");
            final AtomicBoolean isWaitSeen = new AtomicBoolean();
            viewState().setOnDefinitionWaitForTesting(() -> {
                isWaitSeen.set(true);
                drainWalQueue();
            });
            execute("REFRESH MATERIALIZED VIEW price_1h FULL");
            Assert.assertTrue(isWaitSeen.get());
            drainWalAndMatViewQueues();
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            assertViewRows("""
                    ts\tsym\tprice\tamount
                    2024-01-01T00:00:00.000000Z\ta\t1.0\tnull
                    2024-01-01T01:00:00.000000Z\tb\t2.0\tnull
                    2024-01-01T02:00:00.000000Z\tc\t3.0\tnull
                    """);
            assertMappingColumns("amount", "");
        });
    }

    @Test
    public void testFullRequestedBeforeBaseChangeAppliesRebuildsFromNewBase() throws Exception {
        // A FULL refresh that reaches the refresh job before the base change's definition event applies parks on the
        // invalid view, and runs once the view's WAL applies: it rebuilds the view from the new base table. A FULL
        // statement issued right after the base change waits for the definition the same way.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            createBase2WithRow();
            execute(ALTER_PREFIX + "WITH BASE base2 AS (" + BASE2_SQL + ")");
            engine.getMatViewStateStore().enqueueFullRefresh(viewToken());
            drainMatViewQueue(engine);
            // parked: nothing changed yet
            Assert.assertEquals("base", viewDefinition().getBaseTableName());
            assertViewRows(VIEW_ROWS);
            drainWalAndMatViewQueues();
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            Assert.assertEquals(engine.verifyTableName("base2").getTableId(), viewState().getBaseTableId());
            final String base2Rows = """
                    ts\tsym\tprice
                    2024-01-02T00:00:00.000000Z\tc\t5.0
                    """;
            assertViewRows(base2Rows);

            // the statement: back to the first base table, and FULL right after it
            execute(ALTER_PREFIX + "WITH BASE base AS (" + VIEW_SQL + ")");
            final AtomicBoolean isWaitSeen = new AtomicBoolean();
            viewState().setOnDefinitionWaitForTesting(() -> {
                Assert.assertEquals("base2", viewDefinition().getBaseTableName());
                isWaitSeen.set(true);
                drainWalQueue();
            });
            execute("REFRESH MATERIALIZED VIEW price_1h FULL");
            Assert.assertTrue(isWaitSeen.get());
            Assert.assertEquals("base", viewDefinition().getBaseTableName());
            drainWalAndMatViewQueues();
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            assertViewRows(VIEW_ROWS);
        });
    }

    @Test
    public void testFullRightAfterAlterRebuildsWithNewSql() throws Exception {
        // A FULL refresh issued right after ALTER ... AS on a valid view waits for the definition event to apply, so
        // the rebuild runs the new SQL even when the refresh job runs before the WAL apply job.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            execute(ALTER_PREFIX + "AS (SELECT ts, sym, max(price) * 10 AS price FROM base SAMPLE BY 1h)");
            final AtomicBoolean isWaitSeen = new AtomicBoolean();
            viewState().setOnDefinitionWaitForTesting(() -> {
                isWaitSeen.set(true);
                drainWalQueue();
            });
            execute("REFRESH MATERIALIZED VIEW price_1h FULL");
            Assert.assertTrue(isWaitSeen.get());
            drainMatViewQueue(engine);
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t10.0
                    2024-01-01T01:00:00.000000Z\tb\t20.0
                    """);
        });
    }

    @Test
    public void testGuardInvalidatedOlderViewRecoversWithResume() throws Exception {
        // An older view calls count without parentheses; a base column named count makes the bare-name guard
        // invalidate it. ALTER ... AS writes the parentheses, and plain RESUME catches up on the rows the base
        // received while the view was invalid.
        assertMemoryLeak(() -> {
            createBase("base");
            executeWithoutExplicitNamesCheck("CREATE MATERIALIZED VIEW counts AS (SELECT ts, sym, count FROM base SAMPLE BY 1h) PARTITION BY DAY");
            execute("""
                    INSERT INTO base VALUES
                    ('a', 1.0, 1.0, '2024-01-01T00:10:00.000000Z'),
                    ('b', 2.0, 2.0, '2024-01-01T01:10:00.000000Z')
                    """);
            drainWalAndMatViewQueues();
            execute("ALTER TABLE base ADD COLUMN count LONG");
            drainWalAndMatViewQueues();
            assertQuery("SELECT view_status FROM materialized_views() WHERE view_name = 'counts'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("view_status\ninvalid\n");
            execute("INSERT INTO base (sym, price, amount, count, ts) VALUES ('a', 3.0, 3.0, 42, '2024-01-01T00:20:00.000000Z'), ('c', 4.0, 4.0, 43, '2024-01-01T02:10:00.000000Z')");
            drainWalAndMatViewQueues();

            // RESUME reads bare names as columns, so it refuses the stored SQL
            assertExceptionNoLeakCheck(
                    "REFRESH MATERIALIZED VIEW counts RESUME",
                    REFRESH_VIEW_NAME_POSITION,
                    "materialized view SQL does not compile, fix it with ALTER MATERIALIZED VIEW ... AS first"
            );
            execute("ALTER MATERIALIZED VIEW counts AS (SELECT ts, sym, count() AS count FROM base SAMPLE BY 1h)");
            drainWalQueue();
            execute("REFRESH MATERIALIZED VIEW counts RESUME");
            drainWalAndMatViewQueues();
            assertQuery("SELECT view_status, invalidation_reason FROM materialized_views() WHERE view_name = 'counts'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("view_status\tinvalidation_reason\nvalid\t\n");
            assertQuery("counts ORDER BY ts, sym").timestamp("ts").noLeakCheck().expectSize().returns("""
                    ts\tsym\tcount
                    2024-01-01T00:00:00.000000Z\ta\t2
                    2024-01-01T01:00:00.000000Z\tb\t1
                    2024-01-01T02:00:00.000000Z\tc\t1
                    """);
        });
    }

    @Test
    public void testLoopFromConcurrentBaseChanges() throws Exception {
        // Two base changes that each pass the loop check, because the dependency graph changes only on apply, close a
        // loop. Both views stay invalid, RESUME and FULL refuse them, and CHECKPOINT CREATE and a restart survive the
        // loop.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            execute("CREATE MATERIALIZED VIEW other_1h AS (" + VIEW_SQL + ") PARTITION BY DAY");
            drainWalAndMatViewQueues();
            execute(ALTER_PREFIX + "WITH BASE other_1h AS (SELECT ts, sym, last(price) AS price FROM other_1h SAMPLE BY 1h)");
            execute("ALTER MATERIALIZED VIEW other_1h WITH BASE price_1h AS (SELECT ts, sym, last(price) AS price FROM price_1h SAMPLE BY 1h)");
            drainWalAndMatViewQueues();
            Assert.assertTrue(engine.getDependentViewGraph().isOnDependencyLoop(viewDefinition()));
            assertViewStatus("invalid", "base table changed");
            assertQuery("SELECT view_status, invalidation_reason FROM materialized_views() WHERE view_name = 'other_1h'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("view_status\tinvalidation_reason\ninvalid\tbase table changed\n");

            final String loopRefusal = "materialized view is on a dependency loop [view=price_1h, baseTable=other_1h]; move the view to another base table with ALTER MATERIALIZED VIEW ... WITH BASE";
            assertExceptionNoLeakCheck(RESUME_SKIP_SQL, REFRESH_VIEW_NAME_POSITION, loopRefusal);
            assertExceptionNoLeakCheck("REFRESH MATERIALIZED VIEW price_1h FULL", REFRESH_VIEW_NAME_POSITION, loopRefusal);
            Assert.assertFalse(viewState().isLocked());

            // the checkpoint orders the tables of the loop without looping
            if (Os.type != Os.WINDOWS) {
                execute("CHECKPOINT CREATE");
                execute("CHECKPOINT RELEASE");
            }

            restartEngine((restarted, context) -> {
                final TableToken viewToken = restarted.verifyTableName("price_1h");
                final MatViewDefinition definition = restarted.getDependentViewGraph().getViewDefinition(viewToken);
                Assert.assertNotNull(definition);
                Assert.assertNotNull(restarted.getDependentViewGraph().getViewDefinition(restarted.verifyTableName("other_1h")));
                Assert.assertTrue(restarted.getDependentViewGraph().isOnDependencyLoop(definition));
                drainWalAndMatViewQueues(restarted);
                Assert.assertTrue(restarted.getMatViewStateStore().getViewState(viewToken).isInvalid());
                Assert.assertTrue(restarted.getMatViewStateStore().getViewState(restarted.verifyTableName("other_1h")).isInvalid());
                try {
                    restarted.execute(RESUME_SKIP_SQL, context);
                    Assert.fail("RESUME must refuse a view on a loop");
                } catch (SqlException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), loopRefusal);
                }

                // moving one view off the loop lets both recover
                restarted.execute("ALTER MATERIALIZED VIEW price_1h WITH BASE base AS (" + VIEW_SQL + ")", context);
                drainWalAndMatViewQueues(restarted);
                Assert.assertFalse(restarted.getDependentViewGraph().isOnDependencyLoop(restarted.getDependentViewGraph().getViewDefinition(viewToken)));
                restarted.execute(RESUME_SKIP_SQL, context);
                restarted.execute("REFRESH MATERIALIZED VIEW other_1h RESUME SKIP BACKLOG", context);
                drainWalAndMatViewQueues(restarted);
                Assert.assertFalse(restarted.getMatViewStateStore().getViewState(viewToken).isInvalid());
                Assert.assertFalse(restarted.getMatViewStateStore().getViewState(restarted.verifyTableName("other_1h")).isInvalid());
            });
        });
    }

    @Test
    public void testLiveViewBaseIsRefused() throws Exception {
        // A live view cannot be a base table, as for CREATE MATERIALIZED VIEW.
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            execute("CREATE LIVE VIEW base_lv FLUSH EVERY 100ms START FROM NOW AS "
                    + "SELECT ts, sym, sum(price) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS price FROM base");
            assertAlterFails(
                    "WITH BASE base_lv AS (SELECT ts, sym, last(price) AS price FROM base_lv SAMPLE BY 1h)",
                    "base_lv",
                    "live views are not allowed as base tables [name=base_lv]"
            );
            assertViewStatus("valid", "");
        });
    }

    @Test
    public void testMappingColumnsRightAfterAlter() throws Exception {
        // The validation of ALTER ... AS builds the column mapping, which materialized_views() reports as soon as the
        // statement returns, before its definition event applies.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            assertMappingColumns("", "");
            execute(ALTER_PREFIX + "AS (SELECT ts, sym, last(price) AS price, last(amount) AS extra FROM base SAMPLE BY 1h)");
            assertMappingColumns("", "extra");
            // the ADD COLUMN waits until the view's WAL applies the definition event
            drainWalQueue();
            execute(ALTER_PREFIX + "ADD COLUMN amount DOUBLE");
            execute(ALTER_PREFIX + "AS (SELECT ts, sym, last(price) AS price FROM base SAMPLE BY 1h)");
            assertMappingColumns("amount", "");
            drainWalAndMatViewQueues();
            assertMappingColumns("amount", "");
        });
    }

    @Test
    public void testOlderViewWithBareCountConvertsWithShowCreateSql() throws Exception {
        // A valid older view that calls count without parentheses takes the SQL that SHOW CREATE prints, with the
        // parentheses written, stays valid, and a base column named count no longer switches its reading.
        assertMemoryLeak(() -> {
            createBase("base");
            executeWithoutExplicitNamesCheck("CREATE MATERIALIZED VIEW counts AS (SELECT ts, sym, count FROM base SAMPLE BY 1h) PARTITION BY DAY");
            execute("""
                    INSERT INTO base VALUES
                    ('a', 1.0, 1.0, '2024-01-01T00:10:00.000000Z'),
                    ('b', 2.0, 2.0, '2024-01-01T01:10:00.000000Z')
                    """);
            drainWalAndMatViewQueues();
            final String showCreate = queryShowCreate("counts");
            final int lo = showCreate.indexOf("AS (\n") + "AS (\n".length();
            final int hi = showCreate.indexOf("\n)", lo);
            final String sql = showCreate.substring(lo, hi);
            Assert.assertEquals("SELECT ts, sym, count() AS count FROM base SAMPLE BY 1h", sql);

            execute("ALTER MATERIALIZED VIEW counts AS (" + sql + ")");
            drainWalAndMatViewQueues();
            final TableToken countsToken = engine.verifyTableName("counts");
            Assert.assertEquals(sql, engine.getDependentViewGraph().getViewDefinition(countsToken).getMatViewSql());
            execute("ALTER TABLE base ADD COLUMN count LONG");
            execute("INSERT INTO base (sym, price, amount, count, ts) VALUES ('a', 3.0, 3.0, 42, '2024-01-01T00:20:00.000000Z')");
            drainWalAndMatViewQueues();
            assertQuery("SELECT view_status, invalidation_reason FROM materialized_views() WHERE view_name = 'counts'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("view_status\tinvalidation_reason\nvalid\t\n");
            assertQuery("counts ORDER BY ts, sym").timestamp("ts").noLeakCheck().expectSize().returns("""
                    ts\tsym\tcount
                    2024-01-01T00:00:00.000000Z\ta\t2
                    2024-01-01T01:00:00.000000Z\tb\t1
                    """);
        });
    }

    @Test
    public void testPlainAsAfterPendingBaseChangeReadsNewBase() throws Exception {
        // A plain ALTER ... AS right after ALTER ... WITH BASE waits until the view's WAL applies the base change, so its
        // SQL reads the new base table, as when the WAL applies between the statements, and it is no base change.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            createBase2WithRow();
            execute(ALTER_PREFIX + "WITH BASE base2 AS (" + BASE2_SQL + ")");
            final long baseChangeSeqTxn = viewSeqTxn();
            final AtomicBoolean isWaitSeen = armSeamDrain();
            final String maxSql = "SELECT ts, sym, max(price) AS price FROM base2 SAMPLE BY 1h";
            execute(ALTER_PREFIX + "AS (" + maxSql + ")");
            Assert.assertTrue(isWaitSeen.get());
            // the definition event alone, on base2
            Assert.assertEquals(baseChangeSeqTxn + 1, viewSeqTxn());
            try (WalEventReader eventReader = new WalEventReader(configuration); Path path = new Path()) {
                final WalEventCursor definition = openViewEvent(eventReader, path, baseChangeSeqTxn + 1);
                Assert.assertEquals(WalTxnType.MAT_VIEW_DEFINITION, definition.getType());
                TestUtils.assertEquals("base2", definition.getMatViewDefinitionInfo().getBaseTableName());
                TestUtils.assertEquals(maxSql, definition.getMatViewDefinitionInfo().getMatViewSql());
                Assert.assertFalse(definition.getMatViewDefinitionInfo().isBaseTableChange());
            }
            drainWalAndMatViewQueues();
            Assert.assertEquals("base2", viewDefinition().getBaseTableName());
            Assert.assertEquals(maxSql, viewDefinition().getMatViewSql());
            assertViewStatus("invalid", "base table changed");
            execute(RESUME_SKIP_SQL);
            drainWalAndMatViewQueues();
            // the bucket takes the maximum price, 9.0, not the last, 8.0
            execute("""
                    INSERT INTO base2 VALUES
                    ('d', 7.0, 1.0, '2024-01-03T00:10:00.000000Z'),
                    ('d', 9.0, 1.0, '2024-01-03T00:20:00.000000Z'),
                    ('d', 8.0, 1.0, '2024-01-03T00:30:00.000000Z')
                    """);
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            assertViewRows(VIEW_ROWS + "2024-01-03T00:00:00.000000Z\td\t9.0\n");
        });
    }

    @Test
    public void testPlainAsOnOldBaseAfterPendingBaseChangeRefused() throws Exception {
        // A plain ALTER ... AS right after ALTER ... WITH BASE waits until the view's WAL applies the base change, so a
        // SQL that still reads the old base table is refused, as when the WAL applies between the statements, instead
        // of being written and then skipped as stale when it applies.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            createBase2WithRow();
            execute(ALTER_PREFIX + "WITH BASE base2 AS (" + BASE2_SQL + ")");
            final long viewSeqTxn = viewSeqTxn();
            final AtomicBoolean isWaitSeen = armSeamDrain();
            final String sql = ALTER_PREFIX + "AS (SELECT ts, sym, max(price) AS price FROM base SAMPLE BY 1h)";
            try {
                execute(sql);
                Assert.fail("a SQL that reads the old base table must be refused");
            } catch (SqlException e) {
                TestUtils.assertEquals(
                        "materialized view query does not read its base table, name the new base table with WITH BASE [baseTable=base2]",
                        e.getFlyweightMessage()
                );
                Assert.assertEquals(sql.indexOf("SELECT"), e.getPosition());
            } finally {
                viewState().setOnDefinitionWaitForTesting(null);
            }
            Assert.assertTrue(isWaitSeen.get());
            Assert.assertEquals(viewSeqTxn, viewSeqTxn());
            Assert.assertFalse(viewState().isLocked());
            drainWalAndMatViewQueues();
            Assert.assertEquals("base2", viewDefinition().getBaseTableName());
            Assert.assertEquals(BASE2_SQL, viewDefinition().getMatViewSql());
            assertViewStatus("invalid", "base table changed");
        });
    }

    @Test
    public void testPlainResumeAcceptedOnNeverRefreshedView() throws Exception {
        // A view that never completed a refresh has no backlog: after a base change, plain RESUME accepts it, and its
        // next refresh builds it from the new base table.
        assertMemoryLeak(() -> {
            createBase("base");
            execute("INSERT INTO base VALUES ('a', 1.0, 1.0, '2024-01-01T00:10:00.000000Z')");
            execute("CREATE MATERIALIZED VIEW price_1h REFRESH MANUAL DEFERRED AS (" + VIEW_SQL + ") PARTITION BY DAY");
            createBase2WithRow();
            drainWalAndMatViewQueues();
            assertRefreshBaseTxn(-1);
            execute(ALTER_PREFIX + "WITH BASE base2 AS (" + BASE2_SQL + ")");
            Assert.assertNull(viewState().getBacklogLostReason());
            drainWalAndMatViewQueues();
            assertViewStatus("invalid", "base table changed");
            execute(RESUME_SQL);
            drainWalQueue();
            assertViewStatus("valid", "");
            execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
            drainWalAndMatViewQueues();
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-02T00:00:00.000000Z\tc\t5.0
                    """);
        });
    }

    @Test
    public void testPlainResumeRefusedAfterFollowingRenamedBase() throws Exception {
        assertPlainResumeRefusedAfterFollowingRenamedBase(false);
    }

    @Test
    public void testPlainResumeRefusedAfterFollowingRenamedBaseWithCacheUpdates() throws Exception {
        assertPlainResumeRefusedAfterFollowingRenamedBase(true);
    }

    @Test
    public void testRestartAfterAlterAsAbovePurgedTxnLogParts() throws Exception {
        // With the view's transaction log in parts of one transaction, the WAL purge deletes the parts below the one of
        // the view's applied seqTxn, an applied view ADD COLUMN. A restart before the view's WAL applies an ALTER ... AS
        // takes the state from _mv.s and reads no deleted part; with WITH BASE, it loads the base change's
        // invalidation, which lies below the definition.
        setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 1);
        assertMemoryLeak(() -> {
            for (int round = 0; round < 2; round++) {
                final boolean isBaseChange = round == 1;
                createBaseWithRefreshedView("IMMEDIATE");
                if (isBaseChange) {
                    createBase2WithRow();
                }
                execute(ALTER_PREFIX + "ADD COLUMN extra DOUBLE");
                drainWalQueue();
                final long appliedSeqTxn = engine.getTableSequencerAPI().getTxnTracker(viewToken()).getWriterTxn();
                Assert.assertEquals(3, appliedSeqTxn);
                MatViewTxnLogParts.purge(engine, viewToken());
                Assert.assertFalse(MatViewTxnLogParts.exists(engine, viewToken(), 0));
                Assert.assertFalse(MatViewTxnLogParts.exists(engine, viewToken(), 1));
                Assert.assertTrue(MatViewTxnLogParts.exists(engine, viewToken(), 2));
                if (isBaseChange) {
                    execute(ALTER_PREFIX + "WITH BASE base2 AS (" + BASE2_SQL + ")");
                } else {
                    execute(ALTER_PREFIX + "AS (" + VIEW_SQL + ")");
                }
                Assert.assertEquals(appliedSeqTxn, engine.getTableSequencerAPI().getTxnTracker(viewToken()).getWriterTxn());

                restartStates();
                if (isBaseChange) {
                    Assert.assertTrue(viewState().isInvalid());
                    Assert.assertEquals("base table changed", viewState().getInvalidationReasonForReport());
                    Assert.assertEquals(MatViewBacklog.LOST_BASE_TABLE_CHANGED, viewState().getBacklogLostReason());
                    drainWalAndMatViewQueues();
                    assertViewStatus("invalid", "base table changed");
                } else {
                    Assert.assertFalse(viewState().isInvalid());
                    assertRefreshBaseTxn(1);
                    Assert.assertEquals(engine.verifyTableName("base").getTableId(), viewState().getBaseTableId());
                    drainWalAndMatViewQueues();
                    execute("INSERT INTO base VALUES ('c', 3.0, 3.0, '2024-01-01T02:10:00.000000Z')");
                    drainWalAndMatViewQueues();
                    assertViewStatus("valid", "");
                    assertRefreshBaseTxn(2);
                    assertQuery("SELECT ts, sym, price, extra FROM price_1h ORDER BY ts, sym").timestamp("ts").noLeakCheck().expectSize().returns("""
                            ts\tsym\tprice\textra
                            2024-01-01T00:00:00.000000Z\ta\t1.0\tnull
                            2024-01-01T01:00:00.000000Z\tb\t2.0\tnull
                            2024-01-01T02:00:00.000000Z\tc\t3.0\tnull
                            """);
                }
                execute("DROP MATERIALIZED VIEW price_1h");
                execute("DROP TABLE base");
                execute("DROP TABLE IF EXISTS base2");
                drainWalAndMatViewQueues();
            }
        });
    }

    @Test
    public void testRangeAcceptedOnSuspendedViewWalOnceDefinitionApplied() throws Exception {
        // REFRESH ... RANGE waits only for a definition event that the view's WAL has not applied: once it applied, a
        // later suspension of the view's WAL refuses no RANGE, which then recomputes with the new SQL.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("MANUAL");
            execute(ALTER_PREFIX + "AS (" + TEN_TIMES_MAX_SQL + ")");
            drainWalQueue();
            execute(ALTER_PREFIX + "SUSPEND WAL");
            armNoWaitGuard();
            try {
                execute(RANGE_SQL);
            } finally {
                viewState().setOnDefinitionWaitForTesting(null);
            }
            execute(ALTER_PREFIX + "RESUME WAL");
            drainWalAndMatViewQueues();
            Assert.assertNull(viewState().getLastRefreshError());
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t10.0
                    2024-01-01T01:00:00.000000Z\tb\t20.0
                    """);
        });
    }

    @Test
    public void testRangeAcceptedOnSuspendedViewWalWithNothingPending() throws Exception {
        // REFRESH ... RANGE on a view that ran no ALTER ... AS has no definition to wait for: a suspended view WAL refuses
        // nothing.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("MANUAL");
            execute("INSERT INTO base VALUES ('a', 3.0, 3.0, '2024-01-01T00:20:00.000000Z')");
            drainWalQueue();
            Assert.assertEquals(-1, viewState().getPendingDefinitionSeqTxn());
            execute(ALTER_PREFIX + "SUSPEND WAL");
            armNoWaitGuard();
            try {
                execute(RANGE_SQL);
            } finally {
                viewState().setOnDefinitionWaitForTesting(null);
            }
            execute(ALTER_PREFIX + "RESUME WAL");
            drainWalAndMatViewQueues();
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t3.0
                    2024-01-01T01:00:00.000000Z\tb\t2.0
                    """);
        });
    }

    @Test
    public void testRangeRefusalOrderWithPendingBaseChange() throws Exception {
        // REFRESH ... RANGE after a base change whose definition event waits in a suspended view WAL refuses on the WAL
        // first, and, once RESUME WAL let the event apply, on the view it left invalid: each refusal names its remedy.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            createBase2WithRow();
            execute(ALTER_PREFIX + "WITH BASE base2 AS (" + BASE2_SQL + ")");
            execute(ALTER_PREFIX + "SUSPEND WAL");
            armNoWaitGuard();
            try {
                assertAlterRefused(
                        "price_1h",
                        RANGE_SQL,
                        REFRESH_VIEW_NAME_POSITION,
                        DEFINITION_NOT_APPLIED + "run ALTER MATERIALIZED VIEW price_1h RESUME WAL first",
                        true
                );
            } finally {
                viewState().setOnDefinitionWaitForTesting(null);
            }
            execute(ALTER_PREFIX + "RESUME WAL");
            drainWalAndMatViewQueues();
            Assert.assertEquals("base2", viewDefinition().getBaseTableName());
            assertAlterRefused(
                    "price_1h",
                    RANGE_SQL,
                    REFRESH_VIEW_NAME_POSITION,
                    "materialized view is invalid, run REFRESH MATERIALIZED VIEW price_1h RESUME first [reason=base table changed]",
                    true
            );
        });
    }

    @Test
    public void testRangeRefusedAfterDefinitionApplyFailureUntilSkipped() throws Exception {
        // A definition event that fails to apply suspends the view's WAL, and REFRESH ... RANGE refuses until RESUME WAL
        // FROM TXN skips the event. RESUME WAL FROM TXN leaves the view's applied seqTxn to the WAL apply, so the next
        // RANGE waits for it, and then recomputes with the SQL the view kept.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("MANUAL");
            execute(ALTER_PREFIX + "AS (" + TEN_TIMES_MAX_SQL + ")");
            final long definitionSeqTxn = viewSeqTxn();
            final MatViewDefinition definition = viewDefinition();
            engine.getDependentViewGraph().removeView(viewToken());
            try {
                drainWalQueue();
                Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(viewToken()));
            } finally {
                engine.getDependentViewGraph().addView(definition);
            }
            armNoWaitGuard();
            try {
                assertAlterRefused(
                        "price_1h",
                        RANGE_SQL,
                        REFRESH_VIEW_NAME_POSITION,
                        DEFINITION_NOT_APPLIED + "run ALTER MATERIALIZED VIEW price_1h RESUME WAL first"
                );
            } finally {
                viewState().setOnDefinitionWaitForTesting(null);
            }
            Assert.assertTrue(engine.getMatViewStateStore().isRefreshQueueEmpty());
            execute(ALTER_PREFIX + "RESUME WAL FROM TXN " + (definitionSeqTxn + 1));
            execute("INSERT INTO base VALUES ('a', 3.0, 3.0, '2024-01-01T00:20:00.000000Z')");
            final AtomicBoolean isWaitSeen = armSeamDrain();
            execute(RANGE_SQL);
            Assert.assertTrue(isWaitSeen.get());
            Assert.assertEquals(VIEW_SQL, viewDefinition().getMatViewSql());
            drainWalAndMatViewQueues();
            Assert.assertNull(viewState().getLastRefreshError());
            // the skipped event never applies: the view keeps its SQL, the last price of the hour
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t3.0
                    2024-01-01T01:00:00.000000Z\tb\t2.0
                    """);
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(viewToken()));
        });
    }

    @Test
    public void testRangeRefusedWhenViewWalSuspendedWhileWaiting() throws Exception {
        // A REFRESH ... RANGE that waits for the definition event of an ALTER ... AS refuses once the view's WAL is
        // suspended, and enqueues nothing.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("MANUAL");
            execute(ALTER_PREFIX + "AS (" + TEN_TIMES_MAX_SQL + ")");
            final AtomicBoolean isWaitSeen = new AtomicBoolean();
            viewState().setOnDefinitionWaitForTesting(() -> {
                isWaitSeen.set(true);
                try {
                    execute(ALTER_PREFIX + "SUSPEND WAL");
                } catch (SqlException e) {
                    throw new AssertionError(e);
                }
            });
            final long viewSeqTxn = viewSeqTxn();
            try {
                execute(RANGE_SQL);
                Assert.fail("the RANGE must refuse once the view's WAL is suspended");
            } catch (SqlException e) {
                TestUtils.assertEquals(DEFINITION_NOT_APPLIED + "run ALTER MATERIALIZED VIEW price_1h RESUME WAL first", e.getFlyweightMessage());
                Assert.assertEquals(REFRESH_VIEW_NAME_POSITION, e.getPosition());
            }
            Assert.assertTrue(isWaitSeen.get());
            Assert.assertEquals(viewSeqTxn, viewSeqTxn());
            Assert.assertTrue(engine.getMatViewStateStore().isRefreshQueueEmpty());
            execute(ALTER_PREFIX + "RESUME WAL");
            drainWalAndMatViewQueues();
            Assert.assertEquals(TEN_TIMES_MAX_SQL, viewDefinition().getMatViewSql());
            assertViewRows(VIEW_ROWS);
        });
    }

    @Test
    public void testRangeRefusedWhileViewListedInSuspendedTables() throws Exception {
        // The cairo.wal.apply.suspended.tables list stops the apply of the view's WAL without the suspended flag of its
        // tracker: a REFRESH ... RANGE that would wait for the definition event the list holds back refuses at once, and
        // names the list entry. Once the list no longer holds the view, the event applies, and the RANGE recomputes
        // with the new SQL.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("MANUAL");
            execute(ALTER_PREFIX + "AS (" + TEN_TIMES_MAX_SQL + ")");
            final String dirName = viewToken().getDirName();
            setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_TABLES, dirName);
            armNoWaitGuard();
            try {
                Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(viewToken()));
                assertAlterRefused(
                        "price_1h",
                        RANGE_SQL,
                        REFRESH_VIEW_NAME_POSITION,
                        DEFINITION_NOT_APPLIED + "remove " + dirName
                                + " from cairo.wal.apply.suspended.tables and run ALTER MATERIALIZED VIEW price_1h RESUME WAL first"
                );
            } finally {
                viewState().setOnDefinitionWaitForTesting(null);
                setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_TABLES, null);
            }
            Assert.assertTrue(engine.getMatViewStateStore().isRefreshQueueEmpty());
            drainWalQueue();
            Assert.assertEquals(TEN_TIMES_MAX_SQL, viewDefinition().getMatViewSql());
            execute(RANGE_SQL);
            drainWalAndMatViewQueues();
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t10.0
                    2024-01-01T01:00:00.000000Z\tb\t20.0
                    """);
        });
    }

    @Test
    public void testRangeRefusedWhileViewWalSuspendedWithPendingDefinition() throws Exception {
        // REFRESH ... RANGE issued while the definition event of an earlier ALTER ... AS waits in a suspended view WAL
        // refuses at once, at the view name, as a view ALTER does, and enqueues nothing: its bounds cannot wait in the
        // refresh job. So it does after ALTER ... SUSPEND WAL, and with the hard suspension flag alone. A statement that
        // does not parse fails on its parse error first. INCREMENTAL and FULL go on to the refresh job, which waits for
        // the event there. Once RESUME WAL lets the event apply, the RANGE waits for it and recomputes with the new SQL.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("MANUAL");
            execute(ALTER_PREFIX + "AS (" + TEN_TIMES_MAX_SQL + ")");
            execute(ALTER_PREFIX + "SUSPEND WAL");
            final String refusal = DEFINITION_NOT_APPLIED + "run ALTER MATERIALIZED VIEW price_1h RESUME WAL first";
            final String rangeTail = " RANGE FROM '2024-01-01T00:00:00.000000Z' TO '2024-01-01T01:59:59.999999Z'";
            armNoWaitGuard();
            try {
                assertAlterRefused("price_1h", RANGE_SQL, REFRESH_VIEW_NAME_POSITION, refusal);
                // the view name quoted, and in upper case
                assertAlterRefused("price_1h", "REFRESH MATERIALIZED VIEW 'price_1h'" + rangeTail, REFRESH_VIEW_NAME_POSITION, refusal);
                assertAlterRefused("price_1h", "REFRESH MATERIALIZED VIEW PRICE_1H" + rangeTail, REFRESH_VIEW_NAME_POSITION, refusal);
                // parse errors come first
                final String badFrom = "REFRESH MATERIALIZED VIEW price_1h RANGE FROM 'x' TO '2024-01-02'";
                assertAlterRefused("price_1h", badFrom, badFrom.indexOf("'x'"), "invalid FROM timestamp value");
                final String badTo = "REFRESH MATERIALIZED VIEW price_1h RANGE FROM '2024-01-02' TO '2024-01-01'";
                assertAlterRefused("price_1h", badTo, badTo.indexOf("'2024-01-01'"), "TO timestamp must not be earlier than FROM timestamp");
                assertAlterRefused("price_1h", RANGE_SQL + " x", RANGE_SQL.length() + 1, "unexpected token [x] while trying to refresh materialized view");
                Assert.assertTrue(engine.getMatViewStateStore().isRefreshQueueEmpty());
                // INCREMENTAL and FULL do not wait while the view's WAL is suspended
                execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
                execute("REFRESH MATERIALIZED VIEW price_1h FULL");
                execute(ALTER_PREFIX + "RESUME WAL");
                engine.addWalApplySuspended(viewToken());
                Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(viewToken()));
                assertAlterRefused("price_1h", RANGE_SQL, REFRESH_VIEW_NAME_POSITION, refusal);
                engine.removeWalApplySuspended(viewToken());
            } finally {
                viewState().setOnDefinitionWaitForTesting(null);
            }
            Assert.assertEquals(VIEW_SQL, viewDefinition().getMatViewSql());
            final AtomicBoolean isWaitSeen = armSeamDrain();
            execute("INSERT INTO base VALUES ('a', 3.0, 3.0, '2024-01-01T00:20:00.000000Z')");
            execute(RANGE_SQL);
            Assert.assertTrue(isWaitSeen.get());
            Assert.assertEquals(TEN_TIMES_MAX_SQL, viewDefinition().getMatViewSql());
            drainWalAndMatViewQueues();
            Assert.assertNull(viewState().getLastRefreshError());
            // ten times the maximum price of the hour: a 3.0, b 2.0
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t30.0
                    2024-01-01T01:00:00.000000Z\tb\t20.0
                    """);
        });
    }

    @Test
    public void testRangeRightAfterAlterRecomputesWithNewSql() throws Exception {
        // A RANGE refresh issued right after ALTER ... AS waits for the definition event to apply, so it recomputes
        // its range with the new SQL even when the refresh job runs before the WAL apply job.
        assertMemoryLeak(() -> {
            currentMicros = parseFloorPartialTimestamp("2024-01-05T00:00:00.000000Z");
            createBaseWithRefreshedView("IMMEDIATE");
            execute(ALTER_PREFIX + "AS (SELECT ts, sym, max(price) * 10 AS price FROM base SAMPLE BY 1h)");
            final AtomicBoolean isWaitSeen = new AtomicBoolean();
            viewState().setOnDefinitionWaitForTesting(() -> {
                isWaitSeen.set(true);
                drainWalQueue();
            });
            execute("REFRESH MATERIALIZED VIEW price_1h RANGE FROM '2024-01-01T00:00:00.000000Z' TO '2024-01-01T00:59:59.999999Z'");
            Assert.assertTrue(isWaitSeen.get());
            drainMatViewQueue(engine);
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t10.0
                    2024-01-01T01:00:00.000000Z\tb\t2.0
                    """);
        });
    }

    @Test
    public void testReadOnlyAndValidationOnly() throws Exception {
        // A read-only context refuses the statement, and a validation-only compile writes nothing.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            final long viewSeqTxn = viewSeqTxn();
            final SqlExecutionContextImpl context = (SqlExecutionContextImpl) sqlExecutionContext;
            context.setValidationOnly(true);
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                compiler.compile(ALTER_PREFIX + "AS (SELECT ts, sym, max(price) AS price FROM base SAMPLE BY 1h)", context);
            } finally {
                context.setValidationOnly(false);
            }
            Assert.assertEquals(viewSeqTxn, viewSeqTxn());
            Assert.assertEquals(VIEW_SQL, viewDefinition().getMatViewSql());

            try (SqlExecutionContextImpl readOnlyContext = TestUtils.createSqlExecutionCtx(engine, 1)) {
                readOnlyContext.with(io.questdb.cairo.security.ReadOnlySecurityContext.INSTANCE);
                engine.execute(ALTER_PREFIX + "AS (SELECT ts, sym, max(price) AS price FROM base SAMPLE BY 1h)", readOnlyContext);
                Assert.fail("a read-only context must refuse the statement");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "permission denied");
            }
            Assert.assertEquals(viewSeqTxn, viewSeqTxn());
        });
    }

    @Test
    public void testRefreshDoesNotWaitWithoutPendingAlterOrOnSuspendedWal() throws Exception {
        // REFRESH waits only for a pending ALTER ... AS whose definition event the view's WAL has not applied. While the
        // view's WAL is suspended short of the event, INCREMENTAL does not wait, and leaves the wait to the refresh job,
        // while RANGE refuses: its bounds cannot wait in the refresh job.
        assertMemoryLeak(() -> {
            currentMicros = parseFloorPartialTimestamp("2024-01-05T00:00:00.000000Z");
            createBaseWithRefreshedView("IMMEDIATE");
            final AtomicBoolean isWaitSeen = new AtomicBoolean();
            viewState().setOnDefinitionWaitForTesting(() -> isWaitSeen.set(true));
            execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
            Assert.assertFalse(isWaitSeen.get());

            execute(ALTER_PREFIX + "AS (SELECT ts, sym, max(price) AS price FROM base SAMPLE BY 1h)");
            drainWalQueue();
            execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
            Assert.assertFalse(isWaitSeen.get());

            execute(ALTER_PREFIX + "AS (SELECT ts, sym, min(price) AS price FROM base SAMPLE BY 1h)");
            // REFRESH ... STATS runs no SQL, so it does not wait
            execute("REFRESH MATERIALIZED VIEW price_1h STATS");
            Assert.assertFalse(isWaitSeen.get());
            engine.getTableSequencerAPI().suspendTable(viewToken(), ErrorTag.NONE, "test suspension");
            execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
            assertAlterRefused(
                    "price_1h",
                    "REFRESH MATERIALIZED VIEW price_1h RANGE FROM '2024-01-01' TO '2024-01-02'",
                    REFRESH_VIEW_NAME_POSITION,
                    DEFINITION_NOT_APPLIED + "run ALTER MATERIALIZED VIEW price_1h RESUME WAL first"
            );
            Assert.assertFalse(isWaitSeen.get());
            Assert.assertEquals("SELECT ts, sym, max(price) AS price FROM base SAMPLE BY 1h", viewDefinition().getMatViewSql());
            execute("ALTER MATERIALIZED VIEW price_1h RESUME WAL");
            drainWalAndMatViewQueues();
            Assert.assertEquals("SELECT ts, sym, min(price) AS price FROM base SAMPLE BY 1h", viewDefinition().getMatViewSql());
            assertViewStatus("valid", "");
        });
    }

    @Test
    public void testReorderedMissingAndExtraColumns() throws Exception {
        // The view's columns map by name: a SQL that reorders them, stops producing one, or produces one the view
        // lacks keeps the view valid; the missing column gets NULL and the extra one is ignored.
        assertMemoryLeak(() -> {
            createBase("base");
            execute("""
                    INSERT INTO base VALUES
                    ('a', 1.0, 10.0, '2024-01-01T00:10:00.000000Z'),
                    ('b', 2.0, 20.0, '2024-01-01T01:10:00.000000Z')
                    """);
            execute("CREATE MATERIALIZED VIEW price_1h AS (SELECT ts, sym, last(price) AS price, last(amount) AS amount FROM base SAMPLE BY 1h) PARTITION BY DAY");
            drainWalAndMatViewQueues();

            // reordered
            execute(ALTER_PREFIX + "AS (SELECT ts, last(amount) AS amount, sym, last(price) AS price FROM base SAMPLE BY 1h)");
            assertMappingColumns("", "");
            drainWalQueue();
            execute("INSERT INTO base VALUES ('c', 3.0, 30.0, '2024-01-01T02:10:00.000000Z')");
            drainWalAndMatViewQueues();
            // missing amount, extra total, at a position of a view column the SQL still produces
            execute(ALTER_PREFIX + "AS (SELECT ts, sum(amount) AS total, sym, last(price) AS price FROM base SAMPLE BY 1h)");
            assertMappingColumns("amount", "total");
            drainWalQueue();
            execute("INSERT INTO base VALUES ('d', 4.0, 40.0, '2024-01-01T03:10:00.000000Z')");
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            assertViewRows("""
                    ts\tsym\tprice\tamount
                    2024-01-01T00:00:00.000000Z\ta\t1.0\t10.0
                    2024-01-01T01:00:00.000000Z\tb\t2.0\t20.0
                    2024-01-01T02:00:00.000000Z\tc\t3.0\t30.0
                    2024-01-01T03:00:00.000000Z\td\t4.0\tnull
                    """);
        });
    }

    @Test
    public void testResumeAfterBaseChangeWaitsForDefinition() throws Exception {
        // A RESUME issued right after a base change waits until the view's WAL applies the definition event, and then
        // positions the view on the new base table.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            createBase2WithRow();
            invalidate("test invalidation");
            execute(ALTER_PREFIX + "WITH BASE base2 AS (" + BASE2_SQL + ")");
            final AtomicBoolean isWaitSeen = new AtomicBoolean();
            viewState().setOnResumeWalWaitForTesting(() -> {
                Assert.assertEquals("base", viewDefinition().getBaseTableName());
                isWaitSeen.set(true);
                drainWalQueue();
            });
            execute(RESUME_SKIP_SQL);
            Assert.assertTrue(isWaitSeen.get());
            Assert.assertFalse(viewState().isInvalid());
            Assert.assertEquals(engine.verifyTableName("base2").getTableId(), viewState().getBaseTableId());
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
        });
    }

    @Test
    public void testResumeRightAfterAlterWaitsAndSucceeds() throws Exception {
        // RESUME right after ALTER ... AS waits for the definition event and compiles the new SQL: here the new SQL
        // produces the column the view gained, so the mapping leaves nothing out.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            execute(ALTER_PREFIX + "ADD COLUMN amount DOUBLE");
            drainWalQueue();
            invalidate("test invalidation");
            execute(ALTER_PREFIX + "AS (SELECT ts, sym, last(price) AS price, last(amount) AS amount FROM base SAMPLE BY 1h)");
            final AtomicBoolean isWaitSeen = new AtomicBoolean();
            viewState().setOnResumeWalWaitForTesting(() -> {
                isWaitSeen.set(true);
                drainWalQueue();
            });
            execute(RESUME_SKIP_SQL);
            Assert.assertTrue(isWaitSeen.get());
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            assertMappingColumns("", "");
            execute("INSERT INTO base VALUES ('c', 3.0, 30.0, '2024-01-01T02:10:00.000000Z')");
            drainWalAndMatViewQueues();
            assertViewRows("""
                    ts\tsym\tprice\tamount
                    2024-01-01T00:00:00.000000Z\ta\t1.0\tnull
                    2024-01-01T01:00:00.000000Z\tb\t2.0\tnull
                    2024-01-01T02:00:00.000000Z\tc\t3.0\t30.0
                    """);
        });
    }

    @Test
    public void testStaleAlterWithoutBaseIsSkippedOnApply() throws Exception {
        // An ALTER ... AS without WITH BASE validates against the view's base table, and a base change and a RESUME on
        // the new base table get into the view's WAL before its event does. The apply skips the stale event, so the
        // view stays on the new base table.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            createBase2WithRow();
            viewState().setOnAlterValidatedForTesting(() -> {
                try {
                    execute(ALTER_PREFIX + "WITH BASE base2 AS (" + BASE2_SQL + ")");
                    drainWalQueue();
                    execute(RESUME_SKIP_SQL);
                    drainWalQueue();
                } catch (SqlException e) {
                    throw new AssertionError(e);
                }
            });
            execute(ALTER_PREFIX + "AS (SELECT ts, sym, max(price) AS price FROM base SAMPLE BY 1h)");
            drainWalAndMatViewQueues();
            Assert.assertEquals("base2", viewDefinition().getBaseTableName());
            Assert.assertEquals(BASE2_SQL, viewDefinition().getMatViewSql());
            assertViewStatus("valid", "");
            Assert.assertEquals(engine.verifyTableName("base2").getTableId(), viewState().getBaseTableId());
            execute("INSERT INTO base2 VALUES ('d', 7.0, 1.0, '2024-01-03T00:10:00.000000Z')");
            drainWalAndMatViewQueues();
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    2024-01-01T01:00:00.000000Z\tb\t2.0
                    2024-01-03T00:00:00.000000Z\td\t7.0
                    """);
        });
    }

    @Test
    public void testValidationRulesAtTheirPositions() throws Exception {
        // Each rule of ALTER ... AS fails the statement at the offending token and leaves the view as it was.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            createBase2WithRow();
            execute("CREATE TABLE plain (sym SYMBOL, price DOUBLE, amount DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("CREATE TABLE base_ns (sym SYMBOL, price DOUBLE, amount DOUBLE, ts TIMESTAMP_NS) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE VIEW base_view AS (SELECT * FROM base)");
            execute("CREATE MATERIALIZED VIEW child AS (SELECT ts, sym, last(price) AS price FROM price_1h SAMPLE BY 1d) PARTITION BY DAY");
            execute(ALTER_PREFIX + "ADD COLUMN cnt LONG");
            drainWalAndMatViewQueues();
            final long viewSeqTxn = viewSeqTxn();

            assertAlterFails(
                    "AS (SELECT ts, sym, last(price) AS price FROM base SAMPLE BY 2h)",
                    "2h",
                    "materialized view query must keep the view's sampling interval [interval=1h, queryInterval=2h]"
            );
            assertAlterFails(
                    "AS (SELECT ts, sym, last(price) AS price FROM base SAMPLE BY 1h ALIGN TO CALENDAR TIME ZONE 'Europe/London')",
                    "'Europe/London'",
                    "materialized view query must keep the view's SAMPLE BY time zone and offset [timeZone=, offset=00:00]"
            );
            assertAlterFails(
                    "AS (SELECT ts AS ts2, sym, last(price) AS price FROM base SAMPLE BY 1h)",
                    "SELECT",
                    "materialized view query must produce the view's designated timestamp as its designated timestamp [column=ts, type=TIMESTAMP]"
            );
            assertAlterFails(
                    "AS (SELECT ts, sym, last(price) AS price, last(sym) AS cnt FROM base SAMPLE BY 1h)",
                    "last(sym)",
                    "inconvertible types: SYMBOL -> LONG [from=cnt, to=cnt]"
            );
            assertAlterFails(
                    "AS (SELECT ts, sym, last(price) AS px FROM base SAMPLE BY 1h)",
                    "last(price)",
                    "materialized view column does not match the query column at the same position [view column=price, query column=px]; "
                            + "to keep the view column, alias the query column to its name; to replace it, run ALTER MATERIALIZED VIEW price_1h ADD COLUMN for the new column first"
            );
            assertAlterFails(
                    "AS (SELECT ts, sym, last(price) FROM base SAMPLE BY 1h)",
                    "last(price)",
                    "column 'last' requires an explicit alias. Use: last AS your_column_name"
            );
            assertAlterFails(
                    "AS (SELECT ts, sym, count FROM base SAMPLE BY 1h)",
                    "count FROM",
                    "count is not a column; write count() to call the function"
            );
            assertAlterFails(
                    "AS (SELECT ts, sym, last(price) AS price FROM base WHERE ts < now() SAMPLE BY 1h)",
                    "now()",
                    "non-deterministic function cannot be used in materialized view: now"
            );
            assertAlterFails(
                    "AS (SELECT ts, sym, last(price) AS price FROM base SAMPLE BY 1h FILL(PREV))",
                    "PREV",
                    "FILL on base table is not supported for materialized views: base"
            );
            assertAlterFails(
                    "WITH BASE base2 AS (" + VIEW_SQL + ")",
                    "base2",
                    "base table is not referenced in materialized view query: base2"
            );
            assertAlterFails(
                    "WITH BASE base_view AS (SELECT ts, sym, last(price) AS price FROM base_view SAMPLE BY 1h)",
                    "base_view",
                    "base table should be a physical table, cannot be a view: base_view"
            );
            assertAlterFails(
                    "WITH BASE plain AS (SELECT ts, sym, last(price) AS price FROM plain SAMPLE BY 1h)",
                    "plain",
                    "base table has to be WAL enabled"
            );
            assertAlterFails(
                    "WITH BASE base_ns AS (SELECT ts, sym, last(price) AS price FROM base_ns SAMPLE BY 1h)",
                    "base_ns",
                    "base table designated timestamp type does not match the view's [view=price_1h, baseTable=base_ns, viewType=TIMESTAMP, baseTableType=TIMESTAMP_NS]; "
                            + "move the view to another base table with ALTER MATERIALIZED VIEW ... WITH BASE, or re-create the view"
            );
            assertAlterFails(
                    "WITH BASE child AS (SELECT ts, sym, last(price) AS price FROM child SAMPLE BY 1h)",
                    "child",
                    "circular dependency detected for materialized view [view=price_1h, baseTable=child]"
            );
            assertAlterFails(
                    "WITH BASE price_1h AS (SELECT ts, sym, last(price) AS price FROM price_1h SAMPLE BY 1h)",
                    "price_1h AS",
                    "circular dependency detected for materialized view [view=price_1h, baseTable=price_1h]"
            );
            assertAlterFails("WITH BASE nope AS (" + VIEW_SQL + ")", "nope", "table does not exist [table=nope]");
            assertAlterFails("WITH BASE select AS (" + VIEW_SQL + ")", "select AS", "table and column names that are SQL keywords have to be enclosed in double quotes, such as \"select\"");
            assertExceptionNoLeakCheck("ALTER MATERIALIZED VIEW price_1h", "ALTER MATERIALIZED VIEW price_1h".length(),
                    SqlCompilerImpl.ALTER_MAT_VIEW_EXPECTED_TOKEN_DESCR + " expected");
            assertAlterFails("WITH", null, "'base' expected");
            assertAlterFails("WITH TABLE base2", "TABLE", "'base' expected");
            assertAlterFails("WITH BASE", null, "base table name expected");
            assertAlterFails("WITH BASE base2", null, "'as' expected");
            assertAlterFails("WITH BASE base2 SELECT", "SELECT", "'as' expected");
            assertAlterFails("AS", null, "'(' or 'with' or 'select' expected");
            assertAlterFails("AS (" + VIEW_SQL + ") extra", "extra", "unexpected token [extra]");

            Assert.assertEquals(viewSeqTxn, viewSeqTxn());
            Assert.assertEquals(VIEW_SQL, viewDefinition().getMatViewSql());
            Assert.assertFalse(viewState().isLocked());
            assertViewStatus("valid", "");
        });
    }

    @Test
    public void testViewAlterAcceptedOnSuspendedViewWalOnceDefinitionApplied() throws Exception {
        // A view ALTER waits only for a definition event that the view's WAL has not applied: once it applied, a later
        // suspension of the view's WAL, by an apply error, by ALTER ... SUSPEND WAL or by the
        // cairo.wal.apply.suspended.tables list, stops none of them, although the node keeps the event's seqTxn.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            execute(ALTER_PREFIX + "AS (" + X_SQL + ")");
            final long definitionSeqTxn = viewState().getPendingDefinitionSeqTxn();
            Assert.assertEquals(viewSeqTxn(), definitionSeqTxn);
            drainWalQueue();
            Assert.assertEquals(X_SQL, viewDefinition().getMatViewSql());
            armNoWaitGuard();
            final String lastSql = "SELECT ts, sym, max(price) AS price FROM base SAMPLE BY 1h";
            try {
                // the suspension an apply error leaves
                engine.getTableSequencerAPI().suspendTable(viewToken(), ErrorTag.NONE, "test suspension");
                execute(ALTER_PREFIX + "ADD COLUMN extra INT");
                execute(ALTER_PREFIX + "RESUME WAL");
                execute(ALTER_PREFIX + "SUSPEND WAL");
                execute(ALTER_PREFIX + "ADD COLUMN extra2 INT");
                execute(ALTER_PREFIX + "RESUME WAL");
                Assert.assertEquals(definitionSeqTxn, viewState().getPendingDefinitionSeqTxn());
                setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_TABLES, viewToken().getDirName());
                execute(ALTER_PREFIX + "DROP COLUMN extra2");
                execute(ALTER_PREFIX + "AS (" + lastSql + ")");
                Assert.assertEquals(viewSeqTxn(), viewState().getPendingDefinitionSeqTxn());
            } finally {
                viewState().setOnDefinitionWaitForTesting(null);
                setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_TABLES, null);
            }
            drainWalAndMatViewQueues();
            // ALTER ... AS adds no view column
            assertViewColumns("price_1h", "ts", "sym", "price", "extra");
            Assert.assertEquals(lastSql, viewDefinition().getMatViewSql());
            assertViewStatus("valid", "");
        });
    }

    @Test
    public void testViewAlterAcceptedOnSuspendedViewWalWithNothingPending() throws Exception {
        // A view ALTER on a view that ran no ALTER ... AS has no definition to wait for: a suspended or listed view WAL
        // refuses nothing.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            Assert.assertEquals(-1, viewState().getPendingDefinitionSeqTxn());
            execute(ALTER_PREFIX + "SUSPEND WAL");
            armNoWaitGuard();
            try {
                execute(ALTER_PREFIX + "ADD COLUMN extra INT");
                setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_TABLES, viewToken().getDirName());
                execute(ALTER_PREFIX + "ADD COLUMN extra2 INT");
            } finally {
                viewState().setOnDefinitionWaitForTesting(null);
                setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_TABLES, null);
            }
            execute(ALTER_PREFIX + "RESUME WAL");
            drainWalAndMatViewQueues();
            assertViewColumns("price_1h", "ts", "sym", "price", "extra", "extra2");
        });
    }

    @Test
    public void testViewAlterAddColumnIfNotExistsOfExistingColumnDoesNotWait() throws Exception {
        // ADD COLUMN IF NOT EXISTS of a column the view has changes nothing, so it checks no SQL and does not wait for
        // the definition event of an earlier ALTER ... AS: it neither refuses while the apply of the view's WAL is
        // suspended short of the event, nor waits for the event once RESUME WAL lifts the suspension.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            execute(ALTER_PREFIX + "AS (" + X_SQL + ")");
            final long viewSeqTxn = viewSeqTxn();
            Assert.assertEquals(viewSeqTxn, viewState().getPendingDefinitionSeqTxn());
            execute(ALTER_PREFIX + "SUSPEND WAL");
            armNoWaitGuard();
            try {
                execute(ALTER_PREFIX + "ADD COLUMN IF NOT EXISTS price DOUBLE");
                execute(ALTER_PREFIX + "RESUME WAL");
                execute(ALTER_PREFIX + "ADD COLUMN IF NOT EXISTS price DOUBLE");
            } finally {
                viewState().setOnDefinitionWaitForTesting(null);
            }
            // neither statement wrote to the view's WAL, whose apply has not reached the definition event
            Assert.assertEquals(viewSeqTxn, viewSeqTxn());
            Assert.assertTrue(engine.getTableSequencerAPI().getTxnTracker(viewToken()).getWriterTxn() < viewSeqTxn);
            Assert.assertEquals(VIEW_SQL, viewDefinition().getMatViewSql());
            drainWalAndMatViewQueues();
            Assert.assertEquals(X_SQL, viewDefinition().getMatViewSql());
            assertViewColumns("price_1h", "ts", "sym", "price");
            assertViewStatus("valid", "");
        });
    }

    @Test
    public void testViewAlterAfterDefinitionApplyFailureRefusedUntilSkipped() throws Exception {
        // A definition event that fails to apply suspends the view's WAL, and a view ALTER refuses until RESUME WAL FROM
        // TXN skips the event. RESUME WAL FROM TXN leaves the view's applied seqTxn to the WAL apply, so the next view
        // ALTER waits for it, and then checks the SQL the view kept.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            execute(ALTER_PREFIX + "AS (" + X_SQL + ")");
            final long definitionSeqTxn = viewSeqTxn();
            final MatViewDefinition definition = viewDefinition();
            engine.getDependentViewGraph().removeView(viewToken());
            try {
                drainWalQueue();
                Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(viewToken()));
            } finally {
                engine.getDependentViewGraph().addView(definition);
            }
            Assert.assertEquals(definitionSeqTxn - 1, engine.getTableSequencerAPI().getTxnTracker(viewToken()).getWriterTxn());
            armNoWaitGuard();
            try {
                assertAlterRefused(
                        "price_1h",
                        ALTER_PREFIX + "ADD COLUMN x CHAR",
                        VIEW_NAME_POSITION,
                        DEFINITION_NOT_APPLIED + "run ALTER MATERIALIZED VIEW price_1h RESUME WAL first"
                );
            } finally {
                viewState().setOnDefinitionWaitForTesting(null);
            }
            execute(ALTER_PREFIX + "RESUME WAL FROM TXN " + (definitionSeqTxn + 1));
            Assert.assertEquals(definitionSeqTxn - 1, engine.getTableSequencerAPI().getTxnTracker(viewToken()).getWriterTxn());
            final AtomicBoolean isWaitSeen = armSeamDrain();
            // the skipped event never applies: the view keeps its SQL, which does not produce x
            execute(ALTER_PREFIX + "ADD COLUMN x CHAR");
            Assert.assertTrue(isWaitSeen.get());
            Assert.assertEquals(VIEW_SQL, viewDefinition().getMatViewSql());
            drainWalAndMatViewQueues();
            assertViewColumns("price_1h", "ts", "sym", "price", "x");
            Assert.assertEquals(VIEW_SQL, viewDefinition().getMatViewSql());
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(viewToken()));
        });
    }

    @Test
    public void testViewAlterAuthorizesBeforeWaiting() throws Exception {
        // A caller who may not alter the view gets the authorization error at once while an ALTER ... AS waits in the
        // view's WAL: the statement authorizes before it waits for the definition event.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            createBase2WithRow();
            execute(ALTER_PREFIX + "AS (" + X_SQL + ")");
            final long viewSeqTxn = viewSeqTxn();
            armNoWaitGuard();
            try (SqlExecutionContextImpl readOnlyContext = TestUtils.createSqlExecutionCtx(engine, 1)) {
                readOnlyContext.with(io.questdb.cairo.security.ReadOnlySecurityContext.INSTANCE);
                for (String tail : new String[]{
                        "ADD COLUMN x DOUBLE",
                        "DROP COLUMN price",
                        "AS (" + VIEW_SQL + ")",
                        "WITH BASE base2 AS (" + BASE2_SQL + ")"
                }) {
                    try {
                        engine.execute(ALTER_PREFIX + tail, readOnlyContext);
                        Assert.fail("a read-only context must refuse the statement: " + tail);
                    } catch (CairoException e) {
                        Assert.assertTrue(e.isAuthorizationError());
                        TestUtils.assertEquals("Write permission denied", e.getFlyweightMessage());
                    }
                }
            } finally {
                viewState().setOnDefinitionWaitForTesting(null);
            }
            Assert.assertEquals(viewSeqTxn, viewSeqTxn());
            Assert.assertFalse(viewState().isLocked());
            drainWalAndMatViewQueues();
            Assert.assertEquals(X_SQL, viewDefinition().getMatViewSql());
            assertViewColumns("price_1h", "ts", "sym", "price");
        });
    }

    @Test
    public void testViewAlterRefusedWhileViewListedInSuspendedTables() throws Exception {
        assertViewAlterRefusedWhileViewListed(false);
    }

    @Test
    public void testViewAlterRefusedWhileViewListedInSuspendedTablesWithWriteDenied() throws Exception {
        assertViewAlterRefusedWhileViewListed(true);
    }

    @Test
    public void testViewAlterRefusedWhileViewWalSuspendedWithoutSuspendedTablesList() throws Exception {
        // An engine whose configuration keeps no cairo.wal.apply.suspended.tables list at all, as an embedded engine's
        // may not, refuses a view ALTER while the definition event of an earlier ALTER ... AS waits in a suspended view
        // WAL, and names RESUME WAL alone.
        assertMemoryLeak(() -> {
            final DefaultTestCairoConfiguration conf = new DefaultTestCairoConfiguration(temp.newFolder().getAbsolutePath());
            Assert.assertNull(conf.getWalApplySuspendedTables());
            try (
                    CairoEngine embedded = new CairoEngine(conf);
                    SqlExecutionContext context = TestUtils.createSqlExecutionCtx(embedded)
            ) {
                embedded.load();
                embedded.execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, amount DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL", context);
                embedded.execute("INSERT INTO base VALUES ('a', 1.0, 1.0, '2024-01-01T00:10:00.000000Z')", context);
                embedded.execute("CREATE MATERIALIZED VIEW price_1h AS (" + VIEW_SQL + ") PARTITION BY DAY", context);
                drainWalAndMatViewQueues(embedded);
                embedded.execute(ALTER_PREFIX + "AS (" + X_SQL + ")", context);
                embedded.execute(ALTER_PREFIX + "SUSPEND WAL", context);
                final TableToken viewToken = embedded.verifyTableName("price_1h");
                final MatViewState state = embedded.getMatViewStateStore().getViewState(viewToken);
                final long viewSeqTxn = embedded.getTableSequencerAPI().lastTxn(viewToken);
                state.setOnDefinitionWaitForTesting(() -> {
                    throw new AssertionError("the statement waited for the pending definition event");
                });
                try {
                    embedded.execute(ALTER_PREFIX + "ADD COLUMN x CHAR", context);
                    Assert.fail("the statement must refuse a suspended view WAL with a pending definition event");
                } catch (SqlException e) {
                    TestUtils.assertEquals(DEFINITION_NOT_APPLIED + "run ALTER MATERIALIZED VIEW price_1h RESUME WAL first", e.getFlyweightMessage());
                    Assert.assertEquals(VIEW_NAME_POSITION, e.getPosition());
                } finally {
                    state.setOnDefinitionWaitForTesting(null);
                }
                Assert.assertEquals(viewSeqTxn, embedded.getTableSequencerAPI().lastTxn(viewToken));
                embedded.execute(ALTER_PREFIX + "RESUME WAL", context);
                drainWalAndMatViewQueues(embedded);
                Assert.assertEquals(X_SQL, embedded.getDependentViewGraph().getViewDefinition(viewToken).getMatViewSql());
            }
        });
    }

    @Test
    public void testViewAlterRefusedWhileViewWalSuspendedWithPendingDefinition() throws Exception {
        // A view ADD COLUMN, DROP COLUMN or ALTER ... AS issued while the definition event of an earlier ALTER ... AS
        // waits in a suspended view WAL refuses at once, at the view name, and names RESUME WAL: the definition its own
        // WAL entry would apply after is unknown until the WAL applies. So it does after ALTER ... SUSPEND WAL, and with
        // the hard suspension flag alone, which a SUSPEND WAL whose suspension failed leaves. ADD and DROP check the
        // view's columns first, and the statements that do not check the view's SQL run as before. Once RESUME WAL lets
        // the definition apply, ADD COLUMN checks the new SQL.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            createBase2WithRow();
            execute(ALTER_PREFIX + "AS (" + X_SQL + ")");
            execute(ALTER_PREFIX + "SUSPEND WAL");
            final String refusal = DEFINITION_NOT_APPLIED + "run ALTER MATERIALIZED VIEW price_1h RESUME WAL first";
            armNoWaitGuard();
            try {
                assertDefinitionWaitRefused(refusal);
                assertAlterFails("ADD COLUMN price DOUBLE", "price DOUBLE", "column 'price' already exists");
                assertAlterFails("DROP COLUMN nosuch", "nosuch", "Invalid column: nosuch");
                execute(ALTER_PREFIX + "SET REFRESH LIMIT 10 DAYS");
                execute(ALTER_PREFIX + "RESUME WAL");
                engine.addWalApplySuspended(viewToken());
                Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(viewToken()));
                assertDefinitionWaitRefused(refusal);
                execute(ALTER_PREFIX + "RESUME WAL");
            } finally {
                viewState().setOnDefinitionWaitForTesting(null);
            }
            drainWalQueue();
            Assert.assertEquals(X_SQL, viewDefinition().getMatViewSql());
            final String addChar = ALTER_PREFIX + "ADD COLUMN x CHAR";
            assertAlterRefused("price_1h", addChar, addChar.indexOf("x CHAR"), "inconvertible types: DOUBLE -> CHAR [from=x, to=x]");
            execute(ALTER_PREFIX + "ADD COLUMN x DOUBLE");
            drainWalAndMatViewQueues();
            assertViewColumns("price_1h", "ts", "sym", "price", "x");
            assertViewStatus("valid", "");
        });
    }

    @Test
    public void testViewAlterRefusesViewDroppedAfterDefinitionApplied() throws Exception {
        // A view DROP COLUMN that waited for the definition event of an ALTER ... AS finds the view dropped once the
        // event applied.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            execute(ALTER_PREFIX + "AS (" + X_SQL + ")");
            final TableToken viewToken = viewToken();
            final AtomicBoolean isWaitSeen = new AtomicBoolean();
            viewState().setOnDefinitionWaitForTesting(() -> {
                isWaitSeen.set(true);
                drainWalQueue();
                try {
                    execute("DROP MATERIALIZED VIEW price_1h");
                } catch (SqlException e) {
                    throw new AssertionError(e);
                }
            });
            try {
                execute(ALTER_PREFIX + "DROP COLUMN price");
                Assert.fail("the view is dropped");
            } catch (SqlException e) {
                TestUtils.assertEquals("materialized view does not exist [view=price_1h]", e.getFlyweightMessage());
                Assert.assertEquals(VIEW_NAME_POSITION, e.getPosition());
            }
            Assert.assertTrue(isWaitSeen.get());
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(viewToken);
            Assert.assertTrue(tracker.isDropped());
            // the definition applied before the drop
            Assert.assertTrue(tracker.getWriterTxn() >= tracker.getSeqTxn());
            drainWalAndMatViewQueues();
            Assert.assertNull(engine.getTableTokenIfExists("price_1h"));
        });
    }

    @Test
    public void testViewAlterStopsWaitingForDroppedView() throws Exception {
        // A view ADD COLUMN that waits for the definition event of an ALTER ... AS stops once the view is dropped, and
        // writes nothing.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            execute(ALTER_PREFIX + "AS (" + X_SQL + ")");
            final TableToken viewToken = viewToken();
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(viewToken);
            final AtomicLong droppedSeqTxn = new AtomicLong(-1);
            viewState().setOnDefinitionWaitForTesting(() -> {
                try {
                    execute("DROP MATERIALIZED VIEW price_1h");
                } catch (SqlException e) {
                    throw new AssertionError(e);
                }
                droppedSeqTxn.set(tracker.getSeqTxn());
            });
            try {
                execute(ALTER_PREFIX + "ADD COLUMN extra INT");
                Assert.fail("the view is dropped");
            } catch (SqlException e) {
                TestUtils.assertEquals("materialized view does not exist [view=price_1h]", e.getFlyweightMessage());
                Assert.assertEquals(VIEW_NAME_POSITION, e.getPosition());
            }
            Assert.assertTrue(droppedSeqTxn.get() > -1);
            Assert.assertTrue(tracker.isDropped());
            // the definition event never applied, and the statement wrote nothing after the drop
            Assert.assertTrue(tracker.getWriterTxn() < tracker.getSeqTxn());
            Assert.assertEquals(droppedSeqTxn.get(), tracker.getSeqTxn());
            drainWalAndMatViewQueues();
            Assert.assertNull(engine.getTableTokenIfExists("price_1h"));
        });
    }

    @Test
    public void testViewAlterWaitsUntilDefinitionApplies() throws Exception {
        // A view ADD COLUMN polls the view's WAL until another thread applies the definition event of the ALTER ... AS
        // before it, and then checks the new SQL, which produces the column.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            execute(ALTER_PREFIX + "AS (" + X_SQL + ")");
            final AtomicReference<Throwable> applierError = new AtomicReference<>();
            final Thread applier = new Thread(() -> {
                try {
                    drainWalQueue();
                } catch (Throwable th) {
                    applierError.set(th);
                } finally {
                    Path.clearThreadLocals();
                }
            });
            viewState().setOnDefinitionWaitForTesting(applier::start);
            try {
                execute(ALTER_PREFIX + "ADD COLUMN x DOUBLE");
                // the statement returned once the definition event applied
                Assert.assertEquals(X_SQL, viewDefinition().getMatViewSql());
            } finally {
                viewState().setOnDefinitionWaitForTesting(null);
                applier.join();
            }
            Assert.assertNull(applierError.get());
            // the new SQL produces x, so the mapping leaves nothing out
            assertMappingColumns("", "");
            drainWalQueue();
            execute("INSERT INTO base VALUES ('a', 3.0, 1.0, '2024-01-01T00:20:00.000000Z')");
            drainWalAndMatViewQueues();
            // the refresh recomputes the 00:00 bucket only: avg(1.0, 3.0) = 2.0
            assertQuery("price_1h ORDER BY ts, sym").timestamp("ts").noLeakCheck().expectSize().returns("""
                    ts\tsym\tprice\tx
                    2024-01-01T00:00:00.000000Z\ta\t3.0\t2.0
                    2024-01-01T01:00:00.000000Z\tb\t2.0\tnull
                    """);
            assertViewStatus("valid", "");
        });
    }

    @Test
    public void testViewWithoutSamplingIntervalKeepsPassthroughShape() throws Exception {
        // A non-aggregating view takes only a non-aggregating SQL without SAMPLE BY, and an aggregating view one with
        // its sampling interval, which GROUP BY timestamp_floor() may give.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            execute("CREATE MATERIALIZED VIEW base_copy AS (SELECT ts, sym, price FROM base) PARTITION BY DAY");
            drainWalAndMatViewQueues();
            final String withSampleBy = "ALTER MATERIALIZED VIEW base_copy AS (SELECT ts, sym, last(price) AS price FROM base SAMPLE BY 1h)";
            assertExceptionNoLeakCheck(withSampleBy, withSampleBy.indexOf("1h"), "materialized view without a sampling interval takes no SAMPLE BY or GROUP BY timestamp_floor() [view=base_copy]");
            final String aggregating = "ALTER MATERIALIZED VIEW base_copy AS (SELECT ts, sym, max(price) AS price FROM base)";
            assertExceptionNoLeakCheck(aggregating, aggregating.indexOf("SELECT"), "non-aggregating materialized view takes only a non-aggregating query over its base table [view=base_copy]");
            // CREATE, which tells passthrough queries with the same check, refuses such a query too
            final String createAggregating = "CREATE MATERIALIZED VIEW base_max AS (SELECT ts, sym, max(price) AS price FROM base) PARTITION BY DAY";
            assertExceptionNoLeakCheck(createAggregating, createAggregating.indexOf("SELECT"), "TIMESTAMP column is not present in select list");
            final String withTimeZone = "ALTER MATERIALIZED VIEW base_copy AS (SELECT ts, sym, last(price) AS price FROM base SAMPLE BY 1h ALIGN TO CALENDAR TIME ZONE 'Europe/London')";
            assertExceptionNoLeakCheck(withTimeZone, withTimeZone.indexOf("1h"), "materialized view without a sampling interval takes no SAMPLE BY or GROUP BY timestamp_floor() [view=base_copy]");
            // a SHOW names the base table but produces none of its rows
            final String show = "ALTER MATERIALIZED VIEW base_copy AS (SHOW COLUMNS FROM base)";
            assertExceptionNoLeakCheck(show, show.indexOf("SHOW"), "non-aggregating materialized view takes only a non-aggregating query over its base table [view=base_copy]");
            // a bare table name is a passthrough query too, with no SAMPLE BY time zone or offset
            execute("ALTER MATERIALIZED VIEW base_copy AS (base)");
            drainWalQueue();
            Assert.assertEquals("base", engine.getDependentViewGraph().getViewDefinition(engine.verifyTableName("base_copy")).getMatViewSql());
            final String noInterval = ALTER_PREFIX + "AS (SELECT ts, sym, price FROM base)";
            assertExceptionNoLeakCheck(noInterval, noInterval.indexOf("SELECT"), "materialized view query must keep the view's sampling interval, use SAMPLE BY or GROUP BY timestamp_floor() [interval=1h]");

            execute("ALTER MATERIALIZED VIEW base_copy AS (SELECT ts, sym, price * 2 AS price FROM base)");
            execute(ALTER_PREFIX + "AS (SELECT timestamp_floor('1h', ts) AS ts, sym, last(price) AS price FROM base ORDER BY ts)");
            drainWalQueue();
            execute("INSERT INTO base VALUES ('c', 3.0, 1.0, '2024-01-01T02:10:00.000000Z')");
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    2024-01-01T01:00:00.000000Z\tb\t2.0
                    2024-01-01T02:00:00.000000Z\tc\t3.0
                    """);
            assertQuery("base_copy ORDER BY ts").timestamp("ts").noLeakCheck().expectSize().returns("""
                    ts\tsym\tprice
                    2024-01-01T00:10:00.000000Z\ta\t1.0
                    2024-01-01T01:10:00.000000Z\tb\t2.0
                    2024-01-01T02:10:00.000000Z\tc\t6.0
                    """);
        });
    }

    @Test
    public void testAggregatingAlterKeepsBucketedBaseTimestamp() throws Exception {
        // ALTER ... AS takes, as CREATE does, a SQL whose designated timestamp buckets the base table's designated
        // timestamp, by SAMPLE BY or timestamp_floor(), under any spelling, table alias, alias chain, subquery, CTE or
        // regular view, as the master of a join, under a subquery's own alias too, with filters and other columns. A FULL
        // refresh and the incremental refresh of a later insert then give the SQL's rows over the base: one row a round,
        // an hour later each round.
        assertMemoryLeak(() -> {
            createAggregatingView();
            createPassthroughRegularViews();
            final String[] accepted = {
                    "SELECT ts, sym, max(price) AS price FROM BASE SAMPLE BY 1h",
                    "SELECT TS, sym, sum(price) AS price FROM base SAMPLE BY 1h",
                    "SELECT b.ts AS ts, b.sym, sum(b.price) AS price FROM base b SAMPLE BY 1h",
                    "SELECT base.ts, sym, sum(price) AS price FROM base TIMESTAMP(ts) WHERE price > 0 SAMPLE BY 1h",
                    "SELECT ts, sym, sum(price) AS price FROM (SELECT * FROM base WHERE price > 0) SAMPLE BY 1h",
                    "SELECT x AS ts, sym, sum(price) AS price FROM (SELECT ts AS x, sym, price FROM base) TIMESTAMP(x) SAMPLE BY 1h",
                    "WITH c AS (SELECT ts, sym, price FROM base) SELECT ts, sym, sum(price) AS price FROM c SAMPLE BY 1h",
                    "SELECT ts, sym, sum(price) AS price FROM rv_ok SAMPLE BY 1h",
                    "SELECT * FROM (SELECT ts, sym, sum(price) AS price FROM base SAMPLE BY 1h)",
                    "SELECT timestamp_floor('1h', ts) AS ts, sym, sum(price) AS price FROM base ORDER BY ts",
                    "SELECT timestamp_floor('1h', ts, '2024-01-01T00:00:00.000000Z') AS ts, sym, sum(price) AS price FROM base ORDER BY ts",
                    "SELECT timestamp_floor('1h', x) AS ts, sym, sum(price) AS price FROM (SELECT ts AS x, sym, price FROM base) TIMESTAMP(x) ORDER BY ts",
                    "SELECT b.ts, b.sym, sum(b.price) AS price FROM base b JOIN other o ON (sym) SAMPLE BY 1h",
                    "SELECT b.ts, b.sym, sum(b.price) AS price FROM (SELECT * FROM base WHERE price > 0) b JOIN other o ON (sym) SAMPLE BY 1h",
                    "SELECT b.ts, b.sym, sum(b.price + o.x) AS price FROM (base WHERE price > 0) b ASOF JOIN other o ON (sym) SAMPLE BY 1h",
                    "SELECT ts, sym, sum(price) AS price, max(ts2) AS last_ts2 FROM base SAMPLE BY 1h",
                    "SELECT ts, sym, sum(price) AS price FROM base SAMPLE BY 1h"
            };
            final StringSink expected = new StringSink();
            expected.put(AGGREGATING_VIEW_ROWS);
            for (int i = 0; i < accepted.length; i++) {
                final String query = accepted[i];
                execute("CREATE MATERIALIZED VIEW parity WITH BASE base AS (" + query + ") PARTITION BY DAY");
                execute("DROP MATERIALIZED VIEW parity");

                execute("ALTER MATERIALIZED VIEW av AS (" + query + ")");
                assertMappingColumns("av", "", query.contains("last_ts2") ? "last_ts2" : "");
                drainWalQueue();
                Assert.assertEquals(query, engine.getDependentViewGraph().getViewDefinition(engine.verifyTableName("av")).getMatViewSql());
                execute("REFRESH MATERIALIZED VIEW av FULL");
                drainWalAndMatViewQueues();
                final String hour = String.format("2024-01-%02dT%02d:00:00.000000Z", 1 + (12 + i) / 24, (12 + i) % 24);
                execute("INSERT INTO base VALUES ('" + hour + "', '" + hour + "', 'c', " + (12 + i) + ".0)");
                drainWalAndMatViewQueues();
                assertViewStatus("av", "valid", "");
                assertSqlCursors("SELECT ts, sym, price FROM (" + query + ") ORDER BY ts, sym", "SELECT ts, sym, price FROM av ORDER BY ts, sym");
                assertQuery("SELECT count() FROM av").noLeakCheck().noRandomAccess().expectSize().returns("count\n" + (3 + i) + "\n");
                expected.put(hour).put("\tc\t").put(12 + i).put(".0\n");
            }
            // the last SQL sums the price of each symbol by the hour
            assertQuery("av").timestamp("ts").noLeakCheck().expectSize().returns(expected);
        });
    }

    @Test
    public void testAggregatingAlterOnTimeZoneNanosAndViewBase() throws Exception {
        // The rule holds under a SAMPLE BY time zone and offset, for a nanosecond designated timestamp, and over a
        // materialized view as the base table, whose designated timestamp is the view's.
        assertMemoryLeak(() -> {
            createAggregatingView();
            execute("CREATE MATERIALIZED VIEW tz_1h AS (SELECT ts, sym, sum(price) AS price FROM base SAMPLE BY 1h ALIGN TO CALENDAR TIME ZONE 'Europe/London' WITH OFFSET '00:15') PARTITION BY DAY");
            execute("CREATE TABLE base_ns (ts TIMESTAMP_NS, ts2 TIMESTAMP_NS, sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base_ns VALUES ('2024-01-01T10:00:00.000000001Z', '2024-01-01T10:30:00.000000001Z', 'a', 1.0)");
            drainWalQueue();
            execute("CREATE MATERIALIZED VIEW nv AS (SELECT ts, sym, sum(price) AS price FROM base_ns SAMPLE BY 1h) PARTITION BY DAY");
            execute("CREATE MATERIALIZED VIEW av_1d AS (SELECT ts, sym, sum(price) AS price FROM av SAMPLE BY 1d) PARTITION BY DAY");
            drainWalAndMatViewQueues();
            drainWalAndMatViewQueues();

            final String grid = " SAMPLE BY 1h ALIGN TO CALENDAR TIME ZONE 'Europe/London' WITH OFFSET '00:15'";
            assertAggregatingAlterRefused("tz_1h", "SELECT ts2 AS ts, sym, sum(price) AS price FROM base TIMESTAMP(ts2)" + grid, "ts2 AS ts");
            execute("ALTER MATERIALIZED VIEW tz_1h AS (SELECT ts, sym, max(price) AS price FROM base" + grid + ")");
            assertAggregatingAlterRefused("nv", "SELECT ts2 AS ts, sym, sum(price) AS price FROM base_ns TIMESTAMP(ts2) SAMPLE BY 1h", "ts2 AS ts");
            execute("ALTER MATERIALIZED VIEW nv AS (SELECT ts, sym, max(price) AS price FROM base_ns SAMPLE BY 1h)");
            assertAggregatingAlterRefused("av_1d", "SELECT ts, sym, sum(price) AS price FROM (SELECT dateadd('h', 1, ts) AS ts, sym, price FROM av) TIMESTAMP(ts) SAMPLE BY 1d", "ts, sym, sum");
            execute("ALTER MATERIALIZED VIEW av_1d AS (SELECT ts, max(price) AS price, sym FROM av SAMPLE BY 1d)");
            drainWalQueue();

            execute("INSERT INTO base VALUES ('2024-01-01T12:00:00.000000Z', '2024-01-01T12:30:00.000000Z', 'c', 3.0), ('2024-01-01T12:05:00.000000Z', '2024-01-01T12:35:00.000000Z', 'c', 4.0)");
            execute("INSERT INTO base_ns VALUES ('2024-01-01T11:00:00.000000002Z', '2024-01-01T11:30:00.000000002Z', 'b', 2.0), ('2024-01-01T11:30:00.000000003Z', '2024-01-01T11:35:00.000000003Z', 'b', 5.0)");
            drainWalAndMatViewQueues();
            drainWalAndMatViewQueues();
            for (String view : new String[]{"av", "tz_1h", "nv", "av_1d"}) {
                assertViewStatus(view, "valid", "");
            }
            // London keeps UTC in January, and the offset starts the buckets at a quarter past the hour
            assertQuery("tz_1h").timestamp("ts").noLeakCheck().expectSize().returns("""
                    ts\tsym\tprice
                    2024-01-01T09:15:00.000000Z\ta\t1.0
                    2024-01-01T10:15:00.000000Z\tb\t2.0
                    2024-01-01T11:15:00.000000Z\tc\t4.0
                    """);
            assertQuery("nv").timestamp("ts").noLeakCheck().expectSize().returns("""
                    ts\tsym\tprice
                    2024-01-01T10:00:00.000000000Z\ta\t1.0
                    2024-01-01T11:00:00.000000000Z\tb\t5.0
                    """);
            // av sums the two rows of c; av_1d takes the largest hourly sum of each symbol a day
            assertQuery("av_1d ORDER BY sym").noLeakCheck().expectSize().returns("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    2024-01-01T00:00:00.000000Z\tb\t2.0
                    2024-01-01T00:00:00.000000Z\tc\t7.0
                    """);
        });
    }

    @Test
    public void testAggregatingAlterRefusesTimestampNotBucketingBase() throws Exception {
        // An aggregating view recomputes, at each refresh, the buckets of the base table's intervals of its designated
        // timestamp, so ALTER ... AS refuses a SQL whose designated timestamp buckets anything else: another column under
        // TIMESTAMP(ts2), a shifted timestamp, either through a subquery, a CTE, a regular view or the master of a join,
        // or the timestamp of a joined table. The error points at the query column, or at the SQL under a wildcard.
        // CREATE refuses such a SQL with the same error (questdb/questdb#6052). The view keeps refreshing with its SQL.
        assertMemoryLeak(() -> {
            createAggregatingView();
            createPassthroughRegularViews();
            final String[][] refused = {
                    {"SELECT ts2 AS ts, sym, sum(price) AS price FROM base TIMESTAMP(ts2) SAMPLE BY 1h", "ts2 AS ts"},
                    {"SELECT ts2 AS ts, ts AS ts2, sym, sum(price) AS price FROM base TIMESTAMP(ts2) SAMPLE BY 1h", "ts2 AS ts,"},
                    {"SELECT timestamp_floor('1h', ts2) AS ts, sym, sum(price) AS price FROM base TIMESTAMP(ts2) ORDER BY ts", "timestamp_floor"},
                    {"SELECT timestamp_floor('1h', ts2, '2024-01-01T00:00:00.000000Z') AS ts, sym, sum(price) AS price FROM base TIMESTAMP(ts2) ORDER BY ts", "timestamp_floor"},
                    {"SELECT timestamp_floor('1h', dateadd('m', 15, ts)) AS ts, sym, sum(price) AS price FROM base ORDER BY ts", "timestamp_floor"},
                    {"SELECT ts, sym, sum(price) AS price FROM (SELECT ts2 AS ts, sym, price FROM base) TIMESTAMP(ts) SAMPLE BY 1h", "ts, sym, sum"},
                    {"SELECT ts, sym, sum(price) AS price FROM (SELECT dateadd('m', 15, ts) AS ts, sym, price FROM base) TIMESTAMP(ts) SAMPLE BY 1h", "ts, sym, sum"},
                    {"WITH c AS (SELECT ts2 AS ts, sym, price FROM base TIMESTAMP(ts2)) SELECT ts, sym, sum(price) AS price FROM c SAMPLE BY 1h", "ts, sym, sum"},
                    {"SELECT ts, sym, sum(price) AS price FROM rv SAMPLE BY 1h", "ts, sym, sum"},
                    {"SELECT * FROM (SELECT ts2 AS ts, sym, sum(price) AS price FROM base TIMESTAMP(ts2) SAMPLE BY 1h)", null},
                    {"SELECT b.ts, b.sym, sum(b.price) AS price FROM (SELECT ts2 AS ts, sym, price FROM base TIMESTAMP(ts2)) b JOIN other o ON (sym) SAMPLE BY 1h", "b.ts"},
                    {"SELECT o.ts, o.sym, sum(b.price) AS price FROM other o JOIN base b ON (sym) SAMPLE BY 1h", "o.ts"}
            };
            for (String[] shape : refused) {
                assertAggregatingAlterRefused("av", shape[0], shape[1]);
                // CREATE points at the wildcard that carries the timestamp up, ALTER at the SQL's start
                assertCreateRefusesAggregatingTimestamp(shape[0], shape[1] != null ? shape[1] : "*");
            }
            execute("INSERT INTO base VALUES ('2024-01-01T12:00:00.000000Z', '2024-01-01T12:30:00.000000Z', 'c', 3.0)");
            drainWalAndMatViewQueues();
            assertViewStatus("av", "valid", "");
            assertQuery("av").timestamp("ts").noLeakCheck().expectSize().returns(AGGREGATING_VIEW_ROWS + "2024-01-01T12:00:00.000000Z\tc\t3.0\n");
        });
    }

    @Test
    public void testAggregatingAlterWithBaseChecksNewBaseTimestamp() throws Exception {
        // WITH BASE checks the SQL against the new base table's designated timestamp, event_time here: the column named
        // as the old base's timestamp is refused before the view goes invalid, and event_time under the view's timestamp
        // name is taken.
        assertMemoryLeak(() -> {
            createAggregatingView();
            execute("CREATE TABLE base3 (ts TIMESTAMP, event_time TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(event_time) PARTITION BY DAY WAL");
            execute("INSERT INTO base3 VALUES ('2024-01-02T10:00:00.000000Z', '2024-01-02T10:30:00.000000Z', 'c', 3.0)");
            drainWalQueue();
            final String head = "ALTER MATERIALIZED VIEW av WITH BASE base3 AS (";
            assertAggregatingAlterRefused("av", head, "SELECT ts, sym, sum(price) AS price FROM base3 TIMESTAMP(ts) SAMPLE BY 1h", "ts,");
            assertAggregatingAlterRefused("av", head, "SELECT timestamp_floor('1h', ts) AS ts, sym, sum(price) AS price FROM base3 TIMESTAMP(ts) ORDER BY ts", "timestamp_floor");
            Assert.assertNull(engine.getMatViewStateStore().getViewState(engine.verifyTableName("av")).getBacklogLostReason());
            assertDependents("base3");

            execute(head + "SELECT event_time AS ts, sym, sum(price) AS price FROM base3 SAMPLE BY 1h)");
            drainWalAndMatViewQueues();
            assertViewStatus("av", "invalid", "base table changed");
            execute("REFRESH MATERIALIZED VIEW av RESUME SKIP BACKLOG");
            drainWalAndMatViewQueues();
            execute("INSERT INTO base3 VALUES ('2024-01-02T11:00:00.000000Z', '2024-01-02T11:30:00.000000Z', 'd', 4.0)");
            drainWalAndMatViewQueues();
            assertViewStatus("av", "valid", "");
            // the old base's rows stay, and the row base3 held before RESUME SKIP BACKLOG falls into the gap
            assertQuery("av").timestamp("ts").noLeakCheck().expectSize().returns(AGGREGATING_VIEW_ROWS + "2024-01-02T11:00:00.000000Z\td\t4.0\n");
        });
    }

    @Test
    public void testAggregatingTimestampCheckOrder() throws Exception {
        // The rule comes after the designated timestamp and the sampling interval checks, which keep their errors, and
        // before the column checks, so a SQL that breaks the rule and a column cast fails on the rule. A timestamp the
        // query takes from a joined table, or from a column that SAMPLE BY does not bucket, is not its designated
        // timestamp. A malformed timestamp_floor() inside the SQL fails its compile.
        assertMemoryLeak(() -> {
            createAggregatingView();
            final String head = "ALTER MATERIALIZED VIEW av AS (";
            final String[] notDesignated = {
                    "SELECT ts2 AS ts, sym, sum(price) AS price FROM base SAMPLE BY 1h",
                    "SELECT o.ts AS ts, b.sym, sum(b.price) AS price FROM base b JOIN other o ON (sym) SAMPLE BY 1h",
                    "SELECT o.ts, b.sym, sum(b.price) AS price FROM (SELECT * FROM base) b ASOF JOIN (SELECT * FROM other) o ON (sym) SAMPLE BY 1h"
            };
            for (String sql : notDesignated) {
                assertAlterRefused("av", head + sql + ')', head.length() + "SELECT ".length(),
                        "materialized view query must produce the view's designated timestamp as its designated timestamp [column=ts, type=TIMESTAMP]");
            }
            final String malformedFloor = "SELECT timestamp_floor('1h', x) AS ts, sym, sum(price) AS price FROM (SELECT timestamp_floor('1h', ts, null, '00:00') AS x, sym, price FROM base) TIMESTAMP(x) ORDER BY ts";
            assertAlterRefused("av", head + malformedFloor + ')', head.length() + malformedFloor.lastIndexOf("timestamp_floor"),
                    "there is no matching function `timestamp_floor` with the argument types: (STRING, TIMESTAMP, NULL, STRING)");
            final String otherInterval = "SELECT ts2 AS ts, sym, sum(price) AS price FROM base TIMESTAMP(ts2) SAMPLE BY 2h";
            assertAlterRefused("av", head + otherInterval + ')', head.length() + otherInterval.indexOf("2h"),
                    "materialized view query must keep the view's sampling interval [interval=1h, queryInterval=2h]");
            assertAggregatingAlterRefused("av", "SELECT ts2 AS ts, sym, first(sym) AS price FROM base TIMESTAMP(ts2) SAMPLE BY 1h", "ts2 AS ts");
            final String inconvertible = "SELECT ts, sym, first(sym) AS price FROM base SAMPLE BY 1h";
            assertAlterRefused("av", head + inconvertible + ')', head.length() + inconvertible.indexOf("first"),
                    "inconvertible types: SYMBOL -> DOUBLE [from=price, to=price]");
        });
    }

    @Test
    public void testAlterOnDroppedViewStateIsRefused() throws Exception {
        // A view whose state a concurrent DROP marked dropped refuses the statement once it has validated the SQL.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            final long viewSeqTxn = viewSeqTxn();
            final MatViewState state = viewState();
            state.markAsDropped();
            assertExceptionNoLeakCheck(
                    ALTER_PREFIX + "AS (SELECT ts, sym, max(price) AS price FROM base SAMPLE BY 1h)",
                    VIEW_NAME_POSITION,
                    "materialized view does not exist or is not ready for refresh"
            );
            Assert.assertEquals(viewSeqTxn, viewSeqTxn());
            Assert.assertFalse(state.isLocked());
            // as the DROP would, free the dropped state's cached factory
            state.tryCloseIfDropped();
        });
    }

    @Test
    public void testAlterWithMatViewsDisabledIsRefused() throws Exception {
        // A node that refreshes no views keeps no view states: it refuses ALTER ... AS, and REFRESH and view ADD COLUMN
        // have no definition to wait for.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            setProperty(PropertyKey.CAIRO_MAT_VIEW_ENABLED, "false");
            restartEngine((restarted, context) -> {
                final TableToken viewToken = restarted.verifyTableName("price_1h");
                final long viewSeqTxn = restarted.getTableSequencerAPI().getTxnTracker(viewToken).getSeqTxn();
                try {
                    restarted.execute(ALTER_PREFIX + "AS (SELECT ts, sym, max(price) AS price FROM base SAMPLE BY 1h)", context);
                    Assert.fail("a node without view states must refuse the statement");
                } catch (SqlException e) {
                    Assert.assertEquals(VIEW_NAME_POSITION, e.getPosition());
                    TestUtils.assertContains(e.getFlyweightMessage(), "materialized view does not exist or is not ready for refresh");
                }
                Assert.assertEquals(viewSeqTxn, restarted.getTableSequencerAPI().getTxnTracker(viewToken).getSeqTxn());
                restarted.execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL", context);
                final long lastTxn = restarted.getTableSequencerAPI().lastTxn(viewToken);
                restarted.execute(ALTER_PREFIX + "ADD COLUMN extra INT", context);
                Assert.assertEquals(lastTxn + 1, restarted.getTableSequencerAPI().lastTxn(viewToken));
            });
        });
    }

    @Test
    public void testBaseChangeLatchHolderLeavesFullRefreshPendingOrDropsView() throws Exception {
        // A base change that waited for the latch re-checks under it: a FULL refresh that became pending meanwhile, or
        // a DROP of the view, refuses it, writing nothing.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            createBase2WithRow();
            final MatViewState state = viewState();
            final long viewSeqTxn = viewSeqTxn();
            Assert.assertTrue(state.tryLock());
            state.setOnResumeLatchBusyForTesting(() -> {
                state.markAsPendingFullRefreshForTesting();
                state.unlock();
            });
            assertExceptionNoLeakCheck(
                    ALTER_PREFIX + "WITH BASE base2 AS (" + BASE2_SQL + ")",
                    VIEW_NAME_POSITION,
                    "materialized view full refresh is pending, retry later [view=price_1h]"
            );
            Assert.assertEquals(viewSeqTxn, viewSeqTxn());
            Assert.assertFalse(state.isLocked());
            // the release redelivered the pending FULL refresh, which rebuilds the view
            drainWalAndMatViewQueues();
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            assertViewRows(VIEW_ROWS);

            Assert.assertTrue(state.tryLock());
            state.setOnResumeLatchBusyForTesting(() -> {
                state.markAsDropped();
                state.unlock();
            });
            final String sql = ALTER_PREFIX + "WITH BASE base2 AS (" + BASE2_SQL + ")";
            assertExceptionNoLeakCheck(sql, VIEW_NAME_POSITION, "materialized view does not exist [view=price_1h]");
            Assert.assertFalse(state.isLocked());
            state.tryCloseIfDropped();
        });
    }

    @Test
    public void testBaseChangeRedeliveryFailureKeepsStatementSuccessful() throws Exception {
        // The release of the latch after a base change redelivers the work that waited for it. A failure to enqueue it
        // is logged: the base change succeeded, and the store's retry scan delivers the work.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            createBase2WithRow();
            final MatViewState state = viewState();
            final MatViewStateStoreImpl store = (MatViewStateStoreImpl) engine.getMatViewStateStore();
            final AtomicBoolean isFired = new AtomicBoolean();
            Assert.assertTrue(state.tryLock());
            state.setOnResumeLatchBusyForTesting(() -> {
                // an incremental refresh that loses the latch leaves its request for the holder's release
                engine.getMatViewStateStore().enqueueIncrementalRefresh(viewToken());
                drainMatViewQueue(engine);
                store.setOnTaskQueueAppendForTesting(() -> {
                    if (isFired.compareAndSet(false, true)) {
                        throw CairoException.nonCritical().put("test enqueue failure");
                    }
                });
                state.unlock();
            });
            try {
                execute(ALTER_PREFIX + "WITH BASE base2 AS (" + BASE2_SQL + ")");
            } finally {
                store.setOnTaskQueueAppendForTesting(null);
            }
            Assert.assertTrue(isFired.get());
            Assert.assertFalse(state.isLocked());
            drainWalAndMatViewQueues();
            assertViewStatus("invalid", "base table changed");
            Assert.assertEquals("base2", viewDefinition().getBaseTableName());
        });
    }

    @Test
    public void testBaseDroppedWithoutWithBaseIsRefused() throws Exception {
        // Without WITH BASE the view keeps its base table, which has to exist.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            execute("DROP TABLE base");
            drainWalAndMatViewQueues();
            final String sql = ALTER_PREFIX + "AS (" + VIEW_SQL + ")";
            assertExceptionNoLeakCheck(
                    sql,
                    sql.indexOf("FROM base") + "FROM ".length(),
                    "base table does not exist, name the new base table with WITH BASE [baseTable=base]"
            );
            createBase2WithRow();
            execute(ALTER_PREFIX + "WITH BASE base2 AS (" + BASE2_SQL + ")");
            drainWalAndMatViewQueues();
            execute(RESUME_SKIP_SQL);
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
        });
    }

    @Test
    public void testBaseReadThroughRegularViewIsRefusedAtViewReference() throws Exception {
        // The new SQL reads the base table only through regular views, which the parse expands into their bodies. A
        // body's positions point into the regular view's own SQL, so the statement reports a base table it cannot use
        // at the SQL's reference to the outermost regular view that reads it, and leaves the view as it was. The
        // reference sits in the top query, in a join, in a join behind a table function, and in front of a regular view
        // over another.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            execute("CREATE TABLE other (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            // a body longer than the statements below, so that a position taken from it points past their end
            execute("""
                    CREATE VIEW rv AS (
                        SELECT ts, sym, price, price AS p01, price AS p02, price AS p03, price AS p04, price AS p05,
                            price AS p06, price AS p07, price AS p08, price AS p09, price AS p10, price AS p11
                        FROM base
                    )""");
            execute("CREATE VIEW rv2 AS (SELECT * FROM rv)");
            final String sql = ALTER_PREFIX + "AS (SELECT ts, sym, last(price) AS price FROM rv SAMPLE BY 1h)";
            final String joinSql = ALTER_PREFIX + "AS (SELECT b.ts, b.sym, last(b.price) AS price FROM rv b "
                    + "JOIN other o ON (sym) SAMPLE BY 1h)";
            final String functionSql = ALTER_PREFIX + "AS (SELECT b.ts, b.sym, last(b.price) AS price FROM long_sequence(1) l "
                    + "CROSS JOIN rv b SAMPLE BY 1h)";
            final String nestedSql = ALTER_PREFIX + "AS (SELECT ts, sym, last(price) AS price FROM rv2 SAMPLE BY 1h)";

            execute("DROP TABLE base");
            drainWalAndMatViewQueues();
            final String missingError = "base table does not exist, name the new base table with WITH BASE [baseTable=base]";
            assertExceptionNoLeakCheck(sql, sql.indexOf("FROM rv") + "FROM ".length(), missingError);
            assertExceptionNoLeakCheck(joinSql, joinSql.indexOf("rv b"), missingError);
            assertExceptionNoLeakCheck(functionSql, functionSql.indexOf("rv b"), missingError);
            assertExceptionNoLeakCheck(nestedSql, nestedSql.indexOf("FROM rv2") + "FROM ".length(), missingError);

            execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            drainWalAndMatViewQueues();
            assertExceptionNoLeakCheck(sql, sql.indexOf("FROM rv") + "FROM ".length(), "base table has to be WAL enabled");
            assertExceptionNoLeakCheck(nestedSql, nestedSql.indexOf("FROM rv2") + "FROM ".length(), "base table has to be WAL enabled");

            Assert.assertEquals(VIEW_SQL, viewDefinition().getMatViewSql());
            Assert.assertEquals("base", viewDefinition().getBaseTableName());
            assertViewRows(VIEW_ROWS);
        });
    }

    @Test
    public void testBaseRecreatedWithoutWithBaseIsRefusedAtBaseReference() throws Exception {
        // Without WITH BASE the view keeps its base table's name. When a table the view cannot use took the name, the
        // statement fails at the SQL's first reference to the base table, as CREATE does, and leaves the view as it
        // was. The base table sits in the top query, in a join, and in a subquery joined to a UNION.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            execute("CREATE TABLE other (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final String sql = ALTER_PREFIX + "AS (" + VIEW_SQL + ")";
            final int position = sql.indexOf("FROM base") + "FROM ".length();
            final String joinSql = ALTER_PREFIX + "AS (SELECT o.ts, b.sym, last(b.price) AS price FROM other o "
                    + "JOIN base b ON (sym) SAMPLE BY 1h)";
            final String unionSql = ALTER_PREFIX + "AS (SELECT b.ts, b.sym, last(b.price) AS price FROM (SELECT * FROM base) b "
                    + "JOIN (SELECT sym FROM other UNION ALL SELECT sym FROM other) u ON (sym) SAMPLE BY 1h)";

            execute("DROP TABLE base");
            execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            drainWalAndMatViewQueues();
            assertExceptionNoLeakCheck(sql, position, "base table has to be WAL enabled");
            assertExceptionNoLeakCheck(joinSql, joinSql.indexOf("base b"), "base table has to be WAL enabled");
            assertExceptionNoLeakCheck(unionSql, unionSql.indexOf("FROM base") + "FROM ".length(), "base table has to be WAL enabled");

            execute("DROP TABLE base");
            execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, ts TIMESTAMP_NS) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalAndMatViewQueues();
            assertExceptionNoLeakCheck(
                    sql,
                    position,
                    "base table designated timestamp type does not match the view's [view=price_1h, baseTable=base, viewType=TIMESTAMP, baseTableType=TIMESTAMP_NS]; "
                            + "move the view to another base table with ALTER MATERIALIZED VIEW ... WITH BASE, or re-create the view"
            );

            Assert.assertEquals(VIEW_SQL, viewDefinition().getMatViewSql());
            Assert.assertEquals("base", viewDefinition().getBaseTableName());
            assertViewRows(VIEW_ROWS);
        });
    }

    @Test
    public void testBaseReplacedByRegularViewWithoutWithBaseIsRefusedAtViewReference() throws Exception {
        // Without WITH BASE the view keeps its base table's name. When a regular view took the name, the parse expands
        // it, so the SQL reads the view's own tables; the statement refuses the view as CREATE does, at the SQL's first
        // reference to it, and leaves the view as it was. The reference sits in the top query, in a join, in a subquery
        // joined to a UNION, and in front of a join to another regular view. Only a reference that the search does not
        // walk to, one in a WHERE subquery, gets the error at the start of the SQL. The regular view's name differs in
        // case from the base table name the view keeps.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            execute("CREATE TABLE other (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final String sql = ALTER_PREFIX + "AS (" + VIEW_SQL + ")";
            final String joinSql = ALTER_PREFIX + "AS (SELECT o.ts, b.sym, last(b.price) AS price FROM other o "
                    + "JOIN base b ON (sym) SAMPLE BY 1h)";
            final String unionSql = ALTER_PREFIX + "AS (SELECT b.ts, b.sym, last(b.price) AS price FROM (SELECT * FROM base) b "
                    + "JOIN (SELECT sym FROM other UNION ALL SELECT sym FROM other) u ON (sym) SAMPLE BY 1h)";
            final String otherViewSql = ALTER_PREFIX + "AS (SELECT b.ts, b.sym, last(b.price) AS price FROM base b "
                    + "JOIN other_view o ON (sym) SAMPLE BY 1h)";
            final String whereSql = ALTER_PREFIX + "AS (SELECT ts, sym, last(price) AS price FROM other "
                    + "WHERE sym IN (SELECT sym FROM base) SAMPLE BY 1h)";
            final String unionBranchSql = ALTER_PREFIX + "AS (SELECT ts, sym, last(price) AS price FROM "
                    + "(SELECT * FROM other UNION ALL SELECT * FROM base) SAMPLE BY 1h)";
            // a SQL that does not read the base table, while the base table is a table and once it is gone
            final String otherSql = ALTER_PREFIX + "AS (SELECT ts, sym, last(price) AS price FROM other SAMPLE BY 1h)";
            final String notReadError = "materialized view query does not read its base table, name the new base table with WITH BASE [baseTable=base]";
            assertExceptionNoLeakCheck(otherSql, otherSql.indexOf("SELECT"), notReadError);

            execute("DROP TABLE base");
            drainWalAndMatViewQueues();
            assertExceptionNoLeakCheck(otherSql, otherSql.indexOf("SELECT"), notReadError);
            execute("CREATE VIEW BASE AS (SELECT * FROM other)");
            execute("CREATE VIEW other_view AS (SELECT * FROM other)");
            drainWalAndMatViewQueues();
            final String error = "base table should be a physical table, cannot be a view: base";
            assertExceptionNoLeakCheck(sql, sql.indexOf("FROM base") + "FROM ".length(), error);
            assertExceptionNoLeakCheck(joinSql, joinSql.indexOf("base b"), error);
            assertExceptionNoLeakCheck(unionSql, unionSql.indexOf("FROM base") + "FROM ".length(), error);
            assertExceptionNoLeakCheck(otherViewSql, otherViewSql.indexOf("base b"), error);
            assertExceptionNoLeakCheck(whereSql, whereSql.indexOf("SELECT"), error);
            assertExceptionNoLeakCheck(unionBranchSql, unionBranchSql.indexOf("FROM base") + "FROM ".length(), error);
            assertExceptionNoLeakCheck(otherSql, otherSql.indexOf("SELECT"), notReadError);
            // WITH BASE naming the regular view is refused at the name after WITH BASE
            final String withBaseSql = ALTER_PREFIX + "WITH BASE base AS (" + VIEW_SQL + ")";
            assertExceptionNoLeakCheck(withBaseSql, withBaseSql.indexOf("base AS"), "base table should be a physical table, cannot be a view: BASE");

            Assert.assertEquals(VIEW_SQL, viewDefinition().getMatViewSql());
            Assert.assertEquals("base", viewDefinition().getBaseTableName());
            assertViewRows(VIEW_ROWS);
        });
    }

    @Test
    public void testDefinitionApplyFailureSuspendsViewWal() throws Exception {
        // A definition event that cannot apply, here because the view's definition left the dependency graph, suspends
        // the view's WAL, so that no node goes on without the definition.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            execute(ALTER_PREFIX + "AS (SELECT ts, sym, max(price) AS price FROM base SAMPLE BY 1h)");
            final MatViewDefinition definition = viewDefinition();
            engine.getDependentViewGraph().removeView(viewToken());
            try {
                drainWalQueue();
                Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(viewToken()));
            } finally {
                engine.getDependentViewGraph().addView(definition);
            }
        });
    }

    @Test
    public void testDefinitionEventContent() throws Exception {
        // A base change writes the invalidation, with the backlog-lost marker, then the definition event with the new
        // base table name, the SQL text and the base change flag; a statement without a base change writes the
        // definition event alone, with the current base table name.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            createBase2WithRow();
            final long viewSeqTxn = viewSeqTxn();
            execute(ALTER_PREFIX + "WITH BASE base2 AS (" + BASE2_SQL + ")");
            // the second statement waits until the view's WAL applies the first statement's events
            final AtomicBoolean isWaitSeen = new AtomicBoolean();
            viewState().setOnDefinitionWaitForTesting(() -> {
                isWaitSeen.set(true);
                drainWalQueue();
            });
            execute(ALTER_PREFIX + "WITH BASE base2 AS (SELECT ts, sym, max(price) AS price FROM base2 SAMPLE BY 1h)");
            Assert.assertTrue(isWaitSeen.get());
            Assert.assertEquals(viewSeqTxn + 3, viewSeqTxn());
            try (WalEventReader eventReader = new WalEventReader(configuration); Path path = new Path()) {
                final WalEventCursor invalidation = openViewEvent(eventReader, path, viewSeqTxn + 1);
                Assert.assertEquals(WalTxnType.MAT_VIEW_INVALIDATE, invalidation.getType());
                Assert.assertTrue(invalidation.getMatViewInvalidationInfo().isInvalid());
                TestUtils.assertEquals("base table changed", invalidation.getMatViewInvalidationInfo().getInvalidationReason());
                TestUtils.assertEquals(MatViewBacklog.LOST_BASE_TABLE_CHANGED, invalidation.getMatViewInvalidationInfo().getBacklogLostReason());
                Assert.assertEquals(1, invalidation.getMatViewInvalidationInfo().getLastRefreshBaseTableTxn());
                try {
                    invalidation.getMatViewDefinitionInfo();
                    Assert.fail("an invalidation event carries no definition");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "WAL event type is not MAT_VIEW_DEFINITION, type=4");
                }

                WalEventCursor definition = openViewEvent(eventReader, path, viewSeqTxn + 2);
                Assert.assertEquals(WalTxnType.MAT_VIEW_DEFINITION, definition.getType());
                TestUtils.assertEquals("base2", definition.getMatViewDefinitionInfo().getBaseTableName());
                TestUtils.assertEquals(BASE2_SQL, definition.getMatViewDefinitionInfo().getMatViewSql());
                Assert.assertTrue(definition.getMatViewDefinitionInfo().isBaseTableChange());

                // the second statement names base2, the base table of the definition it waited for: no base change, so
                // the definition event alone
                definition = openViewEvent(eventReader, path, viewSeqTxn + 3);
                Assert.assertEquals(WalTxnType.MAT_VIEW_DEFINITION, definition.getType());
                TestUtils.assertEquals("base2", definition.getMatViewDefinitionInfo().getBaseTableName());
                TestUtils.assertEquals("SELECT ts, sym, max(price) AS price FROM base2 SAMPLE BY 1h", definition.getMatViewDefinitionInfo().getMatViewSql());
                Assert.assertFalse(definition.getMatViewDefinitionInfo().isBaseTableChange());
            }
            drainWalAndMatViewQueues();
            final long appliedSeqTxn = viewSeqTxn();
            execute(ALTER_PREFIX + "AS (SELECT ts, sym, min(price) AS price FROM base2 SAMPLE BY 1h)");
            Assert.assertEquals(appliedSeqTxn + 1, viewSeqTxn());
            try (WalEventReader eventReader = new WalEventReader(configuration); Path path = new Path()) {
                final WalEventCursor definition = openViewEvent(eventReader, path, appliedSeqTxn + 1);
                TestUtils.assertEquals("base2", definition.getMatViewDefinitionInfo().getBaseTableName());
                TestUtils.assertEquals("SELECT ts, sym, min(price) AS price FROM base2 SAMPLE BY 1h", definition.getMatViewDefinitionInfo().getMatViewSql());
                Assert.assertFalse(definition.getMatViewDefinitionInfo().isBaseTableChange());
            }
            Assert.assertEquals(appliedSeqTxn + 1, viewState().getPendingDefinitionSeqTxn());
        });
    }

    @Test
    public void testDefinitionEventUnderAdaptiveCommitWindow() throws Exception {
        // Under the ADAPTIVE commit mode with a group window, the definition event joins the pending durable batch.
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, "10ms");
        assertDefinitionEventApplies();
    }

    @Test
    public void testDefinitionEventUnderAdaptiveCommitWithoutWindow() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, 0);
        assertDefinitionEventApplies();
    }

    @Test
    public void testDefinitionEventUnderNosyncCommitMode() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        assertDefinitionEventApplies();
    }

    @Test
    public void testDroppedViewColumnKeepsPositions() throws Exception {
        // The same-position check reads the view's columns in their order with the dropped ones left out, and a query
        // column named like a dropped view column is ignored.
        assertMemoryLeak(() -> {
            createBase("base");
            execute("INSERT INTO base VALUES ('a', 1.0, 10.0, '2024-01-01T00:10:00.000000Z')");
            execute("CREATE MATERIALIZED VIEW price_1h AS (SELECT ts, sym, last(amount) AS amount, last(price) AS price FROM base SAMPLE BY 1h) PARTITION BY DAY");
            drainWalAndMatViewQueues();
            execute(ALTER_PREFIX + "DROP COLUMN amount");
            // the view's columns are now ts, sym and price, and amount is ignored
            execute(ALTER_PREFIX + "AS (SELECT ts, sym, last(price) AS price, last(amount) AS amount FROM base SAMPLE BY 1h)");
            assertMappingColumns("", "amount");
            drainWalQueue();
            execute("INSERT INTO base VALUES ('b', 2.0, 20.0, '2024-01-01T01:10:00.000000Z')");
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    2024-01-01T01:00:00.000000Z\tb\t2.0
                    """);
        });
    }

    @Test
    public void testFullRefreshJobRefusesViewOnLoop() throws Exception {
        // A FULL refresh that reaches the refresh job for a view on a loop, as one requested before the loop closed,
        // keeps the view's rows and leaves it invalid with the loop's reason.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            execute("CREATE MATERIALIZED VIEW other_1h AS (" + VIEW_SQL + ") PARTITION BY DAY");
            drainWalAndMatViewQueues();
            execute(ALTER_PREFIX + "WITH BASE other_1h AS (SELECT ts, sym, last(price) AS price FROM other_1h SAMPLE BY 1h)");
            execute("ALTER MATERIALIZED VIEW other_1h WITH BASE price_1h AS (SELECT ts, sym, last(price) AS price FROM price_1h SAMPLE BY 1h)");
            drainWalAndMatViewQueues();
            engine.getMatViewStateStore().enqueueFullRefresh(viewToken());
            drainWalAndMatViewQueues();
            drainWalAndMatViewQueues();
            assertViewStatus("invalid", "materialized view is on a dependency loop [view=price_1h, baseTable=other_1h]; "
                    + "move the view to another base table with ALTER MATERIALIZED VIEW ... WITH BASE");
            assertViewRows(VIEW_ROWS);
        });
    }

    @Test
    public void testGeneratedColumnNamesKeepPositionalCopy() throws Exception {
        // An unaliased reference to a column name longer than 64 characters relies on a generated name, so the view
        // copies the new SQL's columns by position, which must fit the view's one to one.
        assertMemoryLeak(() -> {
            final String longName = "c".repeat(65);
            execute("CREATE TABLE base (sym SYMBOL, " + longName + " DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base VALUES ('a', 1.0, '2024-01-01T00:10:00.000000Z')");
            execute("CREATE MATERIALIZED VIEW price_1h AS (SELECT ts, " + longName + ", count() AS cnt FROM base SAMPLE BY 1h) PARTITION BY DAY");
            drainWalAndMatViewQueues();
            final String tooMany = ALTER_PREFIX + "AS (SELECT ts, " + longName + ", count() AS cnt, max(" + longName + ") AS hi FROM base SAMPLE BY 1h)";
            assertExceptionNoLeakCheck(
                    tooMany,
                    tooMany.indexOf("SELECT"),
                    "materialized view query does not match view schema [view=price_1h, queryColumnCount=4, viewColumnCount=3]"
            );
            execute(ALTER_PREFIX + "AS (SELECT ts, " + longName + ", sum(" + longName + ")::LONG AS cnt FROM base SAMPLE BY 1h)");
            assertMappingColumns("", "");
            drainWalQueue();
            execute("INSERT INTO base VALUES ('b', 5.0, '2024-01-01T00:20:00.000000Z')");
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            assertQuery("SELECT count() n, sum(cnt) total FROM price_1h").noLeakCheck().noRandomAccess().expectSize().returns("""
                    n\ttotal
                    2\t6
                    """);
        });
    }

    @Test
    public void testPassthroughAlterKeepsUntransformedTimestamp() throws Exception {
        // ALTER ... AS takes, as CREATE does, a SQL whose designated timestamp is the base table's designated timestamp
        // as it is, under any spelling, table alias, alias chain, CTE or regular view, with filters and other columns.
        // The view's FULL and incremental refreshes then give the SQL's rows over the base. SHOW CREATE of a view with
        // such a SQL recreates it.
        assertMemoryLeak(() -> {
            createPassthroughBase();
            createPassthroughRegularViews();
            final String[] accepted = {
                    "base",
                    "SELECT * FROM base",
                    "SELECT b.ts, sym, price * 2 AS price FROM base b",
                    "SELECT TS, sym, price FROM base",
                    "SELECT x AS ts, sym, price FROM (SELECT ts AS x, sym, price FROM base)",
                    "WITH c AS (SELECT ts, sym, price FROM base) SELECT * FROM c",
                    "SELECT * FROM rv_ok",
                    "SELECT ts, sym, price FROM base TIMESTAMP(ts) WHERE price > 0",
                    "SELECT ts2 AS other, ts, sym, price FROM base"
            };
            final String[] ignored = {"ts2", "ts2", "", "", "", "", "", "", "other"};
            for (int i = 0; i < accepted.length; i++) {
                final String query = accepted[i];
                execute("CREATE MATERIALIZED VIEW parity AS (" + query + ") PARTITION BY DAY");
                execute("DROP MATERIALIZED VIEW parity");

                execute("ALTER MATERIALIZED VIEW pv AS (" + query + ")");
                assertMappingColumns("pv", "", ignored[i]);
                drainWalQueue();
                Assert.assertEquals(query, engine.getDependentViewGraph().getViewDefinition(engine.verifyTableName("pv")).getMatViewSql());
                execute("REFRESH MATERIALIZED VIEW pv FULL");
                drainWalAndMatViewQueues();
                // one row a round, at 12:00 and an hour later each round
                final int hour = 12 + i;
                execute("INSERT INTO base VALUES ('2024-01-01T" + hour + ":00:00.000000Z', '2024-01-01T" + hour + ":30:00.000000Z', 'c', " + hour + ".0)");
                drainWalAndMatViewQueues();
                assertViewStatus("pv", "valid", "");
                assertSqlCursors("SELECT ts, sym, price FROM (" + query + ")", "pv");
            }

            final String aliasChain = accepted[4];
            execute("ALTER MATERIALIZED VIEW pv AS (" + aliasChain + ")");
            drainWalAndMatViewQueues();
            final String ddl = queryShowCreate("pv").trim();
            execute("DROP MATERIALIZED VIEW pv");
            drainWalAndMatViewQueues();
            execute(ddl);
            drainWalAndMatViewQueues();
            // SHOW CREATE prints the SQL on lines of its own, which the recreated view keeps
            Assert.assertEquals(aliasChain, engine.getDependentViewGraph().getViewDefinition(engine.verifyTableName("pv")).getMatViewSql().trim());
            assertViewStatus("pv", "valid", "");
            assertSqlCursors(aliasChain, "pv");
        });
    }

    @Test
    public void testPassthroughAlterMatchesBaseNameInAnyCase() throws Exception {
        // The SQL may name the view's base table in another letter case: the rule finds the base table's designated
        // timestamp through it as the statement's base check finds the table.
        assertMemoryLeak(() -> {
            createPassthroughBase();
            assertPassthroughAlterRefused("pv", "SELECT dateadd('m', 15, ts) AS ts, sym, price FROM BASE", "dateadd");
            final String query = "SELECT ts, sym, price FROM BASE";
            execute("ALTER MATERIALIZED VIEW pv AS (" + query + ")");
            drainWalQueue();
            Assert.assertEquals(query, engine.getDependentViewGraph().getViewDefinition(engine.verifyTableName("pv")).getMatViewSql());
            execute("INSERT INTO base VALUES ('2024-01-01T12:00:00.000000Z', '2024-01-01T12:30:00.000000Z', 'c', 3.0)");
            drainWalAndMatViewQueues();
            assertViewStatus("pv", "valid", "");
            assertQuery("pv").timestamp("ts").noLeakCheck().expectSize().returns(PASSTHROUGH_VIEW_ROWS + "2024-01-01T12:00:00.000000Z\tc\t3.0\n");
        });
    }

    @Test
    public void testPassthroughAlterOnRecreatedBaseChecksItsTimestamp() throws Exception {
        // A base dropped and created again under its name with another designated timestamp, ts2, leaves the view
        // invalid. ALTER ... AS checks the fix against the new table: its old timestamp column ts under TIMESTAMP(ts) is
        // refused, ts2 under the view's timestamp name is taken, and RESUME SKIP BACKLOG makes the view follow it.
        assertMemoryLeak(() -> {
            createPassthroughBase();
            execute("DROP TABLE base");
            execute("CREATE TABLE base (ts TIMESTAMP, ts2 TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts2) PARTITION BY DAY WAL");
            execute("INSERT INTO base VALUES ('2024-01-02T10:00:00.000000Z', '2024-01-02T10:30:00.000000Z', 'c', 3.0)");
            drainWalAndMatViewQueues();
            Assert.assertTrue(engine.getMatViewStateStore().getViewState(engine.verifyTableName("pv")).isInvalid());
            assertPassthroughAlterRefused("pv", "ALTER MATERIALIZED VIEW pv AS (", "SELECT ts, sym, price FROM base TIMESTAMP(ts)", "ts,", true);
            final String query = "SELECT ts2 AS ts, sym, price FROM base";
            execute("ALTER MATERIALIZED VIEW pv AS (" + query + ")");
            drainWalQueue();
            execute("REFRESH MATERIALIZED VIEW pv RESUME SKIP BACKLOG");
            drainWalAndMatViewQueues();
            execute("INSERT INTO base VALUES ('2024-01-02T11:00:00.000000Z', '2024-01-02T11:30:00.000000Z', 'd', 4.0)");
            drainWalAndMatViewQueues();
            assertViewStatus("pv", "valid", "");
            Assert.assertEquals(query, engine.getDependentViewGraph().getViewDefinition(engine.verifyTableName("pv")).getMatViewSql());
            // the old base's rows stay, and the row inserted before RESUME SKIP BACKLOG falls into the gap
            assertQuery("pv").timestamp("ts").noLeakCheck().expectSize().returns(PASSTHROUGH_VIEW_ROWS + "2024-01-02T11:30:00.000000Z\td\t4.0\n");
        });
    }

    @Test
    public void testPassthroughAlterOnRenamedTimestampAndViewBase() throws Exception {
        // The rule follows the view's own timestamp name, quoted or not, a quoted base table name, and a materialized
        // view as the base table, whose designated timestamp is the view's.
        assertMemoryLeak(() -> {
            createPassthroughBase();
            execute("CREATE TABLE \"Base Q\" (ts TIMESTAMP, ts2 TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO \"Base Q\" VALUES ('2024-01-01T10:00:00.000000Z', '2024-01-01T10:30:00.000000Z', 'a', 1.0)");
            drainWalQueue();
            execute("CREATE MATERIALIZED VIEW ev AS (SELECT ts AS event_ts, sym, price FROM base) TIMESTAMP(event_ts) PARTITION BY DAY");
            execute("CREATE MATERIALIZED VIEW dq AS (SELECT ts AS \"my ts\", sym, price FROM base) TIMESTAMP(\"my ts\") PARTITION BY DAY");
            execute("CREATE MATERIALIZED VIEW qv AS (SELECT ts, sym, price FROM \"Base Q\") PARTITION BY DAY");
            execute("CREATE MATERIALIZED VIEW price_1h AS (SELECT ts, sym, last(price) AS price FROM base SAMPLE BY 1h) PARTITION BY DAY");
            drainWalAndMatViewQueues();
            execute("CREATE MATERIALIZED VIEW pv2 AS (SELECT ts, sym, price FROM price_1h) PARTITION BY DAY");
            drainWalAndMatViewQueues();

            assertPassthroughAlterRefused("ev", "SELECT dateadd('m', 15, ts) AS event_ts, sym, price FROM base", "dateadd");
            assertPassthroughAlterRefused("ev", "SELECT ts2 AS event_ts, sym, price FROM base TIMESTAMP(ts2)", "ts2");
            // the designated timestamp event_ts passes the base's through; the view ignores the query column ts
            execute("ALTER MATERIALIZED VIEW ev AS (SELECT ts2 AS ts, ts AS event_ts, sym, price FROM base)");
            assertMappingColumns("ev", "", "ts");
            assertPassthroughAlterRefused("dq", "SELECT dateadd('m', 15, ts) AS \"my ts\", sym, price FROM base", "dateadd");
            execute("ALTER MATERIALIZED VIEW dq AS (SELECT ts AS \"my ts\", sym, price FROM base WHERE price > 0)");
            assertPassthroughAlterRefused("qv", "SELECT dateadd('m', 15, ts) AS ts, sym, price FROM \"Base Q\"", "dateadd");
            execute("ALTER MATERIALIZED VIEW qv AS (SELECT ts, sym, price FROM \"Base Q\" WHERE price > 0)");
            assertPassthroughAlterRefused("pv2", "SELECT dateadd('h', 1, ts) AS ts, sym, price FROM price_1h", "dateadd");
            execute("ALTER MATERIALIZED VIEW pv2 AS (SELECT ts, sym, price * 2 AS price FROM price_1h)");
            drainWalQueue();

            execute("INSERT INTO base VALUES ('2024-01-01T12:00:00.000000Z', '2024-01-01T12:30:00.000000Z', 'c', 3.0)");
            execute("INSERT INTO \"Base Q\" VALUES ('2024-01-01T11:00:00.000000Z', '2024-01-01T11:30:00.000000Z', 'b', 2.0)");
            drainWalAndMatViewQueues();
            drainWalAndMatViewQueues();
            for (String view : new String[]{"ev", "dq", "qv", "pv2"}) {
                assertViewStatus(view, "valid", "");
            }
            final String rows = """
                    2024-01-01T10:00:00.000000Z\ta\t1.0
                    2024-01-01T11:00:00.000000Z\tb\t2.0
                    2024-01-01T12:00:00.000000Z\tc\t3.0
                    """;
            assertQuery("ev").timestamp("event_ts").noLeakCheck().expectSize().returns("event_ts\tsym\tprice\n" + rows);
            assertQuery("dq").timestamp("my ts").noLeakCheck().expectSize().returns("my ts\tsym\tprice\n" + rows);
            assertQuery("qv").timestamp("ts").noLeakCheck().expectSize().returns(PASSTHROUGH_VIEW_ROWS);
            // the base insert refreshed the 12:00 bucket of price_1h, which pv2 doubles from then on
            assertQuery("pv2").timestamp("ts").noLeakCheck().expectSize().returns(PASSTHROUGH_VIEW_ROWS + "2024-01-01T12:00:00.000000Z\tc\t6.0\n");
        });
    }

    @Test
    public void testPassthroughAlterRefusesTransformedNanosTimestamp() throws Exception {
        // The rule holds for a nanosecond designated timestamp too.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base_ns (ts TIMESTAMP_NS, sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base_ns VALUES ('2024-01-01T10:00:00.000000001Z', 'a', 1.0)");
            drainWalQueue();
            execute("CREATE MATERIALIZED VIEW nv AS (SELECT ts, sym, price FROM base_ns) PARTITION BY DAY");
            drainWalAndMatViewQueues();
            assertPassthroughAlterRefused("nv", "SELECT dateadd('m', 15, ts) AS ts, sym, price FROM base_ns", "dateadd");
            execute("ALTER MATERIALIZED VIEW nv AS (SELECT ts, sym, price FROM base_ns WHERE price > 0)");
            drainWalQueue();
            execute("INSERT INTO base_ns VALUES ('2024-01-01T11:00:00.000000002Z', 'b', 2.0), ('2024-01-01T12:00:00.000000003Z', 'c', -1.0)");
            drainWalAndMatViewQueues();
            assertViewStatus("nv", "valid", "");
            assertQuery("nv").timestamp("ts").noLeakCheck().expectSize().returns("""
                    ts\tsym\tprice
                    2024-01-01T10:00:00.000000001Z\ta\t1.0
                    2024-01-01T11:00:00.000000002Z\tb\t2.0
                    """);
        });
    }

    @Test
    public void testPassthroughAlterRefusesTransformedTimestamp() throws Exception {
        // A non-aggregating view replaces the base table's intervals of its designated timestamp at each refresh, so
        // ALTER ... AS refuses, as CREATE does, a SQL whose designated timestamp is not the base table's designated
        // timestamp as it is: shifted, another column, or either through a subquery, a CTE or a regular view. The error
        // points at the query column, or at the SQL under a wildcard. The view keeps refreshing with its SQL.
        assertMemoryLeak(() -> {
            createPassthroughBase();
            createPassthroughRegularViews();
            final String[][] refused = {
                    {"SELECT dateadd('m', 15, ts) AS ts, sym, price FROM base", "dateadd"},
                    {"SELECT dateadd('m', -15, ts) AS ts, sym, price FROM base", "dateadd"},
                    {"SELECT ts2 AS ts, sym, price FROM base TIMESTAMP(ts2)", "ts2 AS ts"},
                    {"SELECT ts2 AS ts, ts AS ts2, sym, price FROM base TIMESTAMP(ts2)", "ts2 AS ts"},
                    {"SELECT x AS ts, sym, price FROM (SELECT dateadd('m', 15, ts) AS x, sym, price FROM base)", "x AS ts"},
                    {"SELECT ts, sym, price FROM (SELECT ts2 AS ts, sym, price FROM base) TIMESTAMP(ts)", "ts,"},
                    {"SELECT * FROM (SELECT dateadd('m', 15, ts) AS ts, sym, price FROM base)", null},
                    {"WITH s AS (SELECT dateadd('m', 15, ts) AS ts, sym, price FROM base) SELECT * FROM s", null},
                    {"SELECT * FROM rv", null}
            };
            for (String[] shape : refused) {
                assertPassthroughAlterRefused("pv", shape[0], shape[1]);
                assertCreateRefusesPassthroughTimestamp(shape[0]);
            }
            execute("INSERT INTO base VALUES ('2024-01-01T12:00:00.000000Z', '2024-01-01T12:30:00.000000Z', 'c', 3.0)");
            drainWalAndMatViewQueues();
            assertViewStatus("pv", "valid", "");
            assertQuery("pv").timestamp("ts").noLeakCheck().expectSize().returns(PASSTHROUGH_VIEW_ROWS + "2024-01-01T12:00:00.000000Z\tc\t3.0\n");
        });
    }

    @Test
    public void testPassthroughAlterWithBaseChecksNewBaseTimestamp() throws Exception {
        // WITH BASE checks the SQL against the new base table's designated timestamp, event_time here: the column named
        // as the old base's timestamp, or a shifted event_time, is refused before the view goes invalid, and event_time
        // under the view's timestamp name is taken.
        assertMemoryLeak(() -> {
            createPassthroughBase();
            execute("CREATE TABLE base3 (ts TIMESTAMP, event_time TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(event_time) PARTITION BY DAY WAL");
            execute("INSERT INTO base3 VALUES ('2024-01-02T10:00:00.000000Z', '2024-01-02T10:30:00.000000Z', 'c', 3.0)");
            drainWalQueue();
            final String head = "ALTER MATERIALIZED VIEW pv WITH BASE base3 AS (";
            assertPassthroughAlterRefused("pv", head, "SELECT ts, sym, price FROM base3 TIMESTAMP(ts)", "ts,", false);
            assertPassthroughAlterRefused("pv", head, "SELECT dateadd('m', 15, event_time) AS ts, sym, price FROM base3", "dateadd", false);
            Assert.assertNull(engine.getMatViewStateStore().getViewState(engine.verifyTableName("pv")).getBacklogLostReason());
            assertDependents("base", "pv");
            assertDependents("base3");

            execute(head + "SELECT event_time AS ts, sym, price FROM base3)");
            drainWalAndMatViewQueues();
            assertViewStatus("pv", "invalid", "base table changed");
            execute("REFRESH MATERIALIZED VIEW pv RESUME SKIP BACKLOG");
            drainWalAndMatViewQueues();
            execute("INSERT INTO base3 VALUES ('2024-01-02T11:00:00.000000Z', '2024-01-02T11:30:00.000000Z', 'd', 4.0)");
            drainWalAndMatViewQueues();
            assertViewStatus("pv", "valid", "");
            // the old base's rows stay, and the row base3 held before RESUME SKIP BACKLOG falls into the gap
            assertQuery("pv").timestamp("ts").noLeakCheck().expectSize().returns(PASSTHROUGH_VIEW_ROWS + "2024-01-02T11:30:00.000000Z\td\t4.0\n");
        });
    }

    @Test
    public void testPassthroughTimestampCheckOrder() throws Exception {
        // The rule comes after the designated timestamp check, so a SQL without the view's designated timestamp keeps
        // that error, and before the column checks, so a shifted timestamp fails on the rule first. An aggregating view
        // buckets the base's designated timestamp, which the passthrough rule does not apply to.
        assertMemoryLeak(() -> {
            createPassthroughBase();
            execute("CREATE MATERIALIZED VIEW price_1h AS (SELECT ts, sym, last(price) AS price FROM base SAMPLE BY 1h) PARTITION BY DAY");
            drainWalAndMatViewQueues();
            final String noDesignated = "materialized view query must produce the view's designated timestamp as its designated timestamp [column=ts, type=TIMESTAMP]";
            final String head = "ALTER MATERIALIZED VIEW pv AS (";
            final String[][] notDesignated = {
                    {"SELECT ts2 AS ts, sym, price FROM base", "ts2"},
                    {"SELECT null::TIMESTAMP AS ts, sym, price FROM base", "::"},
                    {"SELECT ts::TIMESTAMP_NS AS ts, sym, price FROM base", "::"},
                    {"SELECT ts AS ts0, sym, price FROM base", "SELECT"}
            };
            for (String[] shape : notDesignated) {
                assertAlterRefused("pv", head + shape[0] + ')', head.length() + shape[0].indexOf(shape[1]), noDesignated);
            }
            assertPassthroughAlterRefused("pv", "SELECT dateadd('m', 15, ts) AS ts, sym, price AS px FROM base", "dateadd");

            execute("ALTER MATERIALIZED VIEW price_1h AS (SELECT timestamp_floor('1h', ts) AS ts, sym, last(price) AS price FROM base ORDER BY ts)");
            drainWalQueue();
            execute("INSERT INTO base VALUES ('2024-01-01T12:10:00.000000Z', '2024-01-01T12:30:00.000000Z', 'c', 3.0)");
            drainWalAndMatViewQueues();
            assertViewStatus("price_1h", "valid", "");
            assertQuery("price_1h").timestamp("ts").noLeakCheck().expectSize().returns(PASSTHROUGH_VIEW_ROWS + "2024-01-01T12:00:00.000000Z\tc\t3.0\n");
        });
    }

    @Test
    public void testPositionalAlterAfterByNameReorder() throws Exception {
        // A by-name ALTER that reordered the SQL leaves the view's columns in their order, so a later SQL that drops
        // the long reference's alias, and so copies by position, must follow the view's order, not the previous SQL's.
        assertMemoryLeak(() -> {
            createLongNameBase("base");
            execute("CREATE MATERIALIZED VIEW pv AS (SELECT ts, " + L65 + ", max(price) AS mx, min(price) AS mn FROM base SAMPLE BY 1h) PARTITION BY DAY");
            drainWalAndMatViewQueues();
            execute("ALTER MATERIALIZED VIEW pv AS (SELECT ts, " + L65 + " AS " + L65 + ", min(price) AS mn, max(price) AS mx FROM base SAMPLE BY 1h)");
            drainWalAndMatViewQueues();
            final String sqlOrder = "ALTER MATERIALIZED VIEW pv AS (SELECT ts, " + L65 + ", min(price) AS mn, max(price) AS mx FROM base SAMPLE BY 1h)";
            assertPositionalAlterRefused("pv", sqlOrder, "min(price)", "mx", "mn");
            execute("ALTER MATERIALIZED VIEW pv AS (SELECT ts, " + L65 + ", max(price) AS mx, min(price) AS mn FROM base SAMPLE BY 1h)");
            drainWalQueue();
            insertSecondHour("base");
            assertViewStatus("pv", "valid", "");
            assertMinMaxRows("pv", """
                    2024-01-01T00:00:00.000000Z\t9.0\t1.0
                    2024-01-01T01:00:00.000000Z\t6.0\t2.0
                    """);
            assertSqlCursors(MIN_MAX_SQL, "SELECT ts, mx, mn FROM pv");
        });
    }

    @Test
    public void testPositionalAlterAfterViewAddColumn() throws Exception {
        // A view column added while the SQL named its columns sits at the end, where a SQL that copies by position
        // must produce it; the rows computed before read NULL there until a refresh recomputes them.
        assertMemoryLeak(() -> {
            createLongNameBase("base");
            execute("CREATE MATERIALIZED VIEW av AS (SELECT ts, " + L65 + " AS " + L65 + ", max(price) AS mx FROM base SAMPLE BY 1h) PARTITION BY DAY");
            drainWalAndMatViewQueues();
            execute("ALTER MATERIALIZED VIEW av ADD COLUMN mn DOUBLE");
            drainWalAndMatViewQueues();
            final String moved = "ALTER MATERIALIZED VIEW av AS (SELECT ts, " + L65 + ", min(price) AS mn, max(price) AS mx FROM base SAMPLE BY 1h)";
            assertPositionalAlterRefused("av", moved, "min(price)", "mx", "mn");
            execute("ALTER MATERIALIZED VIEW av AS (SELECT ts, " + L65 + ", max(price) AS mx, min(price) AS mn FROM base SAMPLE BY 1h)");
            assertMappingColumns("av", "", "");
            drainWalQueue();
            insertSecondHour("base");
            assertViewStatus("av", "valid", "");
            assertMinMaxRows("av", """
                    2024-01-01T00:00:00.000000Z\t9.0\tnull
                    2024-01-01T01:00:00.000000Z\t6.0\t2.0
                    """);
        });
    }

    @Test
    public void testPositionalAlterChecksNamesBeforeCasts() throws Exception {
        // A moved column fails on its name before its type meets the view column at its position; with the names in
        // place, a type that does not cast to the view column's still fails with the cast error.
        assertMemoryLeak(() -> {
            createLongNameBase("base");
            execute("CREATE MATERIALIZED VIEW bv AS (SELECT ts, " + L65 + ", max(price) AS mx, last(ok) AS flag FROM base SAMPLE BY 1h) PARTITION BY DAY");
            drainWalAndMatViewQueues();
            final String moved = "ALTER MATERIALIZED VIEW bv AS (SELECT ts, " + L65 + ", last(ok) AS flag, max(price) AS mx FROM base SAMPLE BY 1h)";
            assertPositionalAlterRefused("bv", moved, "last(ok)", "mx", "flag");
            final String typesSwapped = "ALTER MATERIALIZED VIEW bv AS (SELECT ts, " + L65 + ", last(ok) AS mx, max(price) AS flag FROM base SAMPLE BY 1h)";
            assertAlterRefused("bv", typesSwapped, typesSwapped.indexOf("SELECT"), "inconvertible types: BOOLEAN -> DOUBLE [from=mx, to=mx]");
            insertSecondHour("base");
            assertViewStatus("bv", "valid", "");
            assertQuery("SELECT ts, mx, flag FROM bv ORDER BY ts").timestamp("ts").noLeakCheck().expectSize().returns("""
                    ts\tmx\tflag
                    2024-01-01T00:00:00.000000Z\t9.0\tfalse
                    2024-01-01T01:00:00.000000Z\t6.0\tfalse
                    """);
        });
    }

    @Test
    public void testPositionalAlterKeepingPositionsIsAccepted() throws Exception {
        // A SQL that copies by position and keeps every view column's name at its position, in any letter case, is
        // taken as it is: the view's columns keep their names, and the next refresh computes with the new SQL.
        assertMemoryLeak(() -> {
            createLongNameBase("base");
            execute("CREATE MATERIALIZED VIEW pv AS (SELECT ts, " + L65 + ", max(price) AS mx, min(price) AS mn FROM base SAMPLE BY 1h) PARTITION BY DAY");
            drainWalAndMatViewQueues();
            final long viewSeqTxn = engine.getTableSequencerAPI().getTxnTracker(engine.verifyTableName("pv")).getSeqTxn();
            final String newSql = "SELECT ts, " + L65 + ", max(price) * 2 AS MX, min(price) AS Mn FROM base SAMPLE BY 1h";
            execute("ALTER MATERIALIZED VIEW pv AS (" + newSql + ")");
            Assert.assertEquals(viewSeqTxn + 1, engine.getTableSequencerAPI().getTxnTracker(engine.verifyTableName("pv")).getSeqTxn());
            assertMappingColumns("pv", "", "");
            drainWalQueue();
            Assert.assertEquals(newSql, engine.getDependentViewGraph().getViewDefinition(engine.verifyTableName("pv")).getMatViewSql());
            insertSecondHour("base");
            assertViewStatus("pv", "valid", "");
            assertQuery("pv ORDER BY ts").timestamp("ts").noLeakCheck().expectSize().returns(
                    "ts\t" + L65 + "\tmx\tmn\n"
                            + "2024-01-01T00:00:00.000000Z\ta\t9.0\t1.0\n"
                            + "2024-01-01T01:00:00.000000Z\ta\t12.0\t2.0\n"
            );
        });
    }

    @Test
    public void testPositionalAlterRefusesDriftedGeneratedName() throws Exception {
        // CREATE under the default alias rules names an unaliased reference to a 65-character column after its first
        // 64 characters. Under the legacy rules the same reference takes the full name, which is not the view
        // column's at its position; the alias of the view column's name keeps it.
        setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED, "true");
        assertMemoryLeak(() -> {
            final String l65Cut = L65.substring(0, 64);
            createLongNameBase("base");
            execute("CREATE MATERIALIZED VIEW pv AS (SELECT ts, " + L65 + ", max(price) AS mx, min(price) AS mn FROM base SAMPLE BY 1h) PARTITION BY DAY");
            drainWalAndMatViewQueues();
            assertViewColumns("pv", "ts", l65Cut, "mx", "mn");
            setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED, "false");
            final String drifted = "ALTER MATERIALIZED VIEW pv AS (SELECT ts, " + L65 + ", max(price) * 2 AS mx, min(price) AS mn FROM base SAMPLE BY 1h)";
            assertPositionalAlterRefused("pv", drifted, L65, l65Cut, L65);
            execute("ALTER MATERIALIZED VIEW pv AS (SELECT ts, " + L65 + " AS " + l65Cut + ", max(price) * 2 AS mx, min(price) AS mn FROM base SAMPLE BY 1h)");
            assertMappingColumns("pv", "", "");
            drainWalQueue();
            insertSecondHour("base");
            assertViewStatus("pv", "valid", "");
            assertMinMaxRows("pv", """
                    2024-01-01T00:00:00.000000Z\t9.0\t1.0
                    2024-01-01T01:00:00.000000Z\t12.0\t2.0
                    """);
        });
    }

    @Test
    public void testPositionalAlterRefusesMovedColumn() throws Exception {
        // An unaliased reference to a column name longer than 64 characters makes the SQL copy by position, so a
        // moved view column would take another column's values: refused. With the long reference aliased, the same
        // order maps by name. A reference to a 64-character name keeps its name, so its SQL maps by name too.
        assertMemoryLeak(() -> {
            createLongNameBase("base");
            execute("CREATE MATERIALIZED VIEW pv AS (SELECT ts, " + L65 + ", max(price) AS mx, min(price) AS mn FROM base SAMPLE BY 1h) PARTITION BY DAY");
            execute("CREATE MATERIALIZED VIEW pv64 AS (SELECT ts, " + L64 + ", max(price) AS mx, min(price) AS mn FROM base SAMPLE BY 1h) PARTITION BY DAY");
            drainWalAndMatViewQueues();
            final String moved = "ALTER MATERIALIZED VIEW pv AS (SELECT ts, " + L65 + ", min(price) AS mn, max(price) AS mx FROM base SAMPLE BY 1h)";
            assertPositionalAlterRefused("pv", moved, "min(price)", "mx", "mn");

            execute("ALTER MATERIALIZED VIEW pv AS (SELECT ts, " + L65 + " AS " + L65 + ", min(price) AS mn, max(price) AS mx FROM base SAMPLE BY 1h)");
            assertMappingColumns("pv", "", "");
            execute("ALTER MATERIALIZED VIEW pv64 AS (SELECT ts, " + L64 + ", min(price) AS mn, max(price) AS mx FROM base SAMPLE BY 1h)");
            assertMappingColumns("pv64", "", "");
            drainWalQueue();
            insertSecondHour("base");
            for (String view : new String[]{"pv", "pv64"}) {
                assertViewStatus(view, "valid", "");
                assertMinMaxRows(view, """
                        2024-01-01T00:00:00.000000Z\t9.0\t1.0
                        2024-01-01T01:00:00.000000Z\t6.0\t2.0
                        """);
                assertSqlCursors(MIN_MAX_SQL, "SELECT ts, mx, mn FROM " + view);
            }
        });
    }

    @Test
    public void testPositionalAlterRefusesMovedColumnFromSubqueryOrCte() throws Exception {
        // An unaliased long reference anywhere in the SQL's text makes it copy by position, in a subquery or a CTE
        // too, even when its column never reaches the output; the error points at the outer SELECT's column.
        assertMemoryLeak(() -> {
            createLongNameBase("base");
            execute("CREATE MATERIALIZED VIEW sv AS (SELECT ts, max(price) AS mx, min(price) AS mn FROM base SAMPLE BY 1h) PARTITION BY DAY");
            drainWalAndMatViewQueues();
            final String subquery = "ALTER MATERIALIZED VIEW sv AS (SELECT ts, min(price) AS mn, max(price) AS mx FROM (SELECT ts, " + L65 + ", price FROM base) SAMPLE BY 1h)";
            assertPositionalAlterRefused("sv", subquery, "min(price)", "mx", "mn");
            final String cte = "ALTER MATERIALIZED VIEW sv AS (WITH q AS (SELECT ts, " + L65 + ", price FROM base) SELECT ts, min(price) AS mn, max(price) AS mx FROM q SAMPLE BY 1h)";
            assertPositionalAlterRefused("sv", cte, "min(price)", "mx", "mn");
            execute("ALTER MATERIALIZED VIEW sv AS (SELECT ts, min(price) AS mn, max(price) AS mx FROM (SELECT ts, " + L65 + " AS k, price FROM base) SAMPLE BY 1h)");
            assertMappingColumns("sv", "", "");
            drainWalQueue();
            insertSecondHour("base");
            assertViewStatus("sv", "valid", "");
            assertMinMaxRows("sv", """
                    2024-01-01T00:00:00.000000Z\t9.0\t1.0
                    2024-01-01T01:00:00.000000Z\t6.0\t2.0
                    """);
            assertSqlCursors(MIN_MAX_SQL, "SELECT ts, mx, mn FROM sv");
        });
    }

    @Test
    public void testPositionalAlterRefusesMovedColumnUnderWildcard() throws Exception {
        // A top-level wildcard records no column position, so the error points at the SQL. Refresh, which checks such
        // a SQL's names, never meets the moved column.
        assertMemoryLeak(() -> {
            createLongNameBase("base");
            execute("CREATE MATERIALIZED VIEW wv AS (SELECT * FROM (SELECT ts, " + L65 + ", max(price) AS mx, min(price) AS mn FROM base SAMPLE BY 1h)) PARTITION BY DAY");
            drainWalAndMatViewQueues();
            final String moved = "ALTER MATERIALIZED VIEW wv AS (SELECT * FROM (SELECT ts, " + L65 + ", min(price) AS mn, max(price) AS mx FROM base SAMPLE BY 1h))";
            assertPositionalAlterRefused("wv", moved, "SELECT", "mx", "mn");
            insertSecondHour("base");
            assertViewStatus("wv", "valid", "");
            assertMinMaxRows("wv", """
                    2024-01-01T00:00:00.000000Z\t9.0\t1.0
                    2024-01-01T01:00:00.000000Z\t6.0\t2.0
                    """);
        });
    }

    @Test
    public void testPositionalAlterRefusesMovedTimestamp() throws Exception {
        // The designated timestamp moved out of its position is a moved column like any other.
        assertMemoryLeak(() -> {
            createLongNameBase("base");
            execute("CREATE MATERIALIZED VIEW pv AS (SELECT ts, " + L65 + ", max(price) AS mx, min(price) AS mn FROM base SAMPLE BY 1h) PARTITION BY DAY");
            drainWalAndMatViewQueues();
            final String moved = "ALTER MATERIALIZED VIEW pv AS (SELECT " + L65 + ", ts, max(price) AS mx, min(price) AS mn FROM base SAMPLE BY 1h)";
            assertPositionalAlterRefused("pv", moved, L65, "ts", L65);
            insertSecondHour("base");
            assertViewStatus("pv", "valid", "");
            assertMinMaxRows("pv", """
                    2024-01-01T00:00:00.000000Z\t9.0\t1.0
                    2024-01-01T01:00:00.000000Z\t6.0\t2.0
                    """);
        });
    }

    @Test
    public void testPositionalAlterRefusesRenamedColumn() throws Exception {
        // A query column the view lacks at a view column's position replaces the view column: a renamed column, a long
        // reference that lost its alias, or another long column in its place.
        assertMemoryLeak(() -> {
            createLongNameBase("base");
            execute("CREATE MATERIALIZED VIEW pv AS (SELECT ts, " + L65 + ", max(price) AS mx, min(price) AS mn FROM base SAMPLE BY 1h) PARTITION BY DAY");
            execute("CREATE MATERIALIZED VIEW nv AS (SELECT ts, " + L65 + " AS s, max(price) AS mx, min(price) AS mn FROM base SAMPLE BY 1h) PARTITION BY DAY");
            drainWalAndMatViewQueues();
            final String renamed = "ALTER MATERIALIZED VIEW pv AS (SELECT ts, " + L65 + ", avg(price) AS av, min(price) AS mn FROM base SAMPLE BY 1h)";
            assertPositionalAlterRefused("pv", renamed, "avg(price)", "mx", "av");
            final String aliasLost = "ALTER MATERIALIZED VIEW nv AS (SELECT ts, " + L65 + ", max(price) AS mx, min(price) AS mn FROM base SAMPLE BY 1h)";
            assertPositionalAlterRefused("nv", aliasLost, L65, "s", L65);
            final String otherColumn = "ALTER MATERIALIZED VIEW nv AS (SELECT ts, " + M65 + ", max(price) AS mx, min(price) AS mn FROM base SAMPLE BY 1h)";
            assertPositionalAlterRefused("nv", otherColumn, M65, "s", M65);
            execute("ALTER MATERIALIZED VIEW nv AS (SELECT ts, " + L65 + " AS s, max(price) AS mx, min(price) AS mn FROM base SAMPLE BY 1h)");
            assertMappingColumns("nv", "", "");
            drainWalQueue();
            insertSecondHour("base");
            for (String view : new String[]{"pv", "nv"}) {
                assertViewStatus(view, "valid", "");
                assertMinMaxRows(view, """
                        2024-01-01T00:00:00.000000Z\t9.0\t1.0
                        2024-01-01T01:00:00.000000Z\t6.0\t2.0
                        """);
            }
        });
    }

    @Test
    public void testPositionalAlterRefusesRenamedLastColumn() throws Exception {
        // The check reaches the last view column: a query column the view lacks there replaces it, though every column
        // before it keeps its name and place. The view keeps computing its minimum, not the refused SQL's average.
        assertMemoryLeak(() -> {
            createLongNameBase("base");
            execute("CREATE MATERIALIZED VIEW pv AS (SELECT ts, " + L65 + ", max(price) AS mx, min(price) AS mn FROM base SAMPLE BY 1h) PARTITION BY DAY");
            drainWalAndMatViewQueues();
            final String renamedLast = "ALTER MATERIALIZED VIEW pv AS (SELECT ts, " + L65 + ", max(price) AS mx, avg(price) AS av FROM base SAMPLE BY 1h)";
            assertPositionalAlterRefused("pv", renamedLast, "avg(price)", "mn", "av");
            insertSecondHour("base");
            assertViewStatus("pv", "valid", "");
            assertMinMaxRows("pv", """
                    2024-01-01T00:00:00.000000Z\t9.0\t1.0
                    2024-01-01T01:00:00.000000Z\t6.0\t2.0
                    """);
        });
    }

    @Test
    public void testPositionalAlterUnderServerDefaultAliasRules() throws Exception {
        // Under the default alias rules an unaliased reference to a 65-character column takes its first 64 characters
        // as its name, which the view's column has; the moved and the renamed column are refused as under the legacy
        // rules, and a SQL that keeps the names in place is taken.
        setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED, "true");
        assertMemoryLeak(() -> {
            final String l65Cut = L65.substring(0, 64);
            createLongNameBase("base");
            execute("CREATE MATERIALIZED VIEW pv AS (SELECT ts, " + L65 + ", max(price) AS mx, min(price) AS mn FROM base SAMPLE BY 1h) PARTITION BY DAY");
            execute("CREATE MATERIALIZED VIEW nv AS (SELECT ts, " + L65 + " AS s, max(price) AS mx, min(price) AS mn FROM base SAMPLE BY 1h) PARTITION BY DAY");
            drainWalAndMatViewQueues();
            assertViewColumns("pv", "ts", l65Cut, "mx", "mn");
            final String moved = "ALTER MATERIALIZED VIEW pv AS (SELECT ts, " + L65 + ", min(price) AS mn, max(price) AS mx FROM base SAMPLE BY 1h)";
            assertPositionalAlterRefused("pv", moved, "min(price)", "mx", "mn");
            final String aliasLost = "ALTER MATERIALIZED VIEW nv AS (SELECT ts, " + L65 + ", max(price) AS mx, min(price) AS mn FROM base SAMPLE BY 1h)";
            assertPositionalAlterRefused("nv", aliasLost, L65, "s", l65Cut);
            execute("ALTER MATERIALIZED VIEW pv AS (SELECT ts, " + L65 + ", max(price) * 2 AS mx, min(price) AS mn FROM base SAMPLE BY 1h)");
            assertMappingColumns("pv", "", "");
            drainWalQueue();
            insertSecondHour("base");
            assertViewStatus("pv", "valid", "");
            assertMinMaxRows("pv", """
                    2024-01-01T00:00:00.000000Z\t9.0\t1.0
                    2024-01-01T01:00:00.000000Z\t12.0\t2.0
                    """);
        });
    }

    @Test
    public void testPositionalAlterUnderSmallGeneratedNameSize() throws Exception {
        // Under a generated name size of 16, an unaliased reference to a 20-character column takes its first 16
        // characters as its name, so the SQL copies by position, as for a name over 64 characters.
        setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED, "true");
        setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_GENERATED_MAX_SIZE, 16);
        assertMemoryLeak(() -> {
            final String c20 = "c".repeat(20);
            execute("CREATE TABLE base (ts TIMESTAMP, " + c20 + " SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base VALUES ('2024-01-01T00:10:00.000000Z', 'a', 1.0), ('2024-01-01T00:20:00.000000Z', 'a', 9.0)");
            drainWalQueue();
            execute("CREATE MATERIALIZED VIEW zv AS (SELECT ts, " + c20 + ", max(price) AS mx, min(price) AS mn FROM base SAMPLE BY 1h) PARTITION BY DAY");
            drainWalAndMatViewQueues();
            assertViewColumns("zv", "ts", c20.substring(0, 16), "mx", "mn");
            final String moved = "ALTER MATERIALIZED VIEW zv AS (SELECT ts, " + c20 + ", min(price) AS mn, max(price) AS mx FROM base SAMPLE BY 1h)";
            assertPositionalAlterRefused("zv", moved, "min(price)", "mx", "mn");
            execute("ALTER MATERIALIZED VIEW zv AS (SELECT ts, " + c20 + ", max(price) * 2 AS mx, min(price) AS mn FROM base SAMPLE BY 1h)");
            drainWalQueue();
            execute("INSERT INTO base VALUES ('2024-01-01T01:10:00.000000Z', 'a', 2.0), ('2024-01-01T01:20:00.000000Z', 'a', 6.0)");
            drainWalAndMatViewQueues();
            // the generated name size cuts the names of unaliased columns, the materialized_views() ones too
            assertQuery("SELECT view_status status, invalidation_reason reason, unmapped_view_columns unmapped, ignored_query_columns ignored "
                    + "FROM materialized_views() WHERE view_name = 'zv'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("status\treason\tunmapped\tignored\nvalid\t\t\t\n");
            assertMinMaxRows("zv", """
                    2024-01-01T00:00:00.000000Z\t9.0\t1.0
                    2024-01-01T01:00:00.000000Z\t12.0\t2.0
                    """);
        });
    }

    @Test
    public void testPositionalAlterWithBaseRefusedKeepsBase() throws Exception {
        // A base change whose SQL moves a view column fails before it invalidates the view, which keeps its base
        // table.
        assertMemoryLeak(() -> {
            createLongNameBase("base");
            createLongNameBase("base2");
            execute("CREATE MATERIALIZED VIEW pv AS (SELECT ts, " + L65 + ", max(price) AS mx, min(price) AS mn FROM base SAMPLE BY 1h) PARTITION BY DAY");
            drainWalAndMatViewQueues();
            final String moved = "ALTER MATERIALIZED VIEW pv WITH BASE base2 AS (SELECT ts, " + L65 + ", min(price) AS mn, max(price) AS mx FROM base2 SAMPLE BY 1h)";
            assertPositionalAlterRefused("pv", moved, "min(price)", "mx", "mn");
            Assert.assertNull(engine.getMatViewStateStore().getViewState(engine.verifyTableName("pv")).getBacklogLostReason());
            assertDependents("base", "pv");
            assertDependents("base2");
            insertSecondHour("base");
            assertViewStatus("pv", "valid", "");
            assertMinMaxRows("pv", """
                    2024-01-01T00:00:00.000000Z\t9.0\t1.0
                    2024-01-01T01:00:00.000000Z\t6.0\t2.0
                    """);
        });
    }

    @Test
    public void testPositionalPassthroughAlterRefusesMovedColumn() throws Exception {
        // A non-aggregating view copies by position the same way.
        assertMemoryLeak(() -> {
            createLongNameBase("base");
            execute("CREATE MATERIALIZED VIEW ptv AS (SELECT ts, " + L65 + ", price, amount FROM base) PARTITION BY DAY");
            drainWalAndMatViewQueues();
            final String moved = "ALTER MATERIALIZED VIEW ptv AS (SELECT ts, " + L65 + ", amount, price FROM base)";
            assertPositionalAlterRefused("ptv", moved, "amount", "price", "amount");
            insertSecondHour("base");
            assertViewStatus("ptv", "valid", "");
            assertQuery("SELECT ts, price, amount FROM ptv").timestamp("ts").noLeakCheck().expectSize().returns("""
                    ts\tprice\tamount
                    2024-01-01T00:10:00.000000Z\t1.0\t100.0
                    2024-01-01T00:20:00.000000Z\t9.0\t900.0
                    2024-01-01T01:10:00.000000Z\t2.0\t200.0
                    2024-01-01T01:20:00.000000Z\t6.0\t600.0
                    """);
        });
    }

    @Test
    public void testPositionalPassthroughAlterRefusesTransformedTimestamp() throws Exception {
        // A SQL that copies by position meets the passthrough timestamp rule before its column checks, which a shifted
        // timestamp under the view's name and at its position would pass. A SQL that passes the base's timestamp
        // through goes on to them: a type with no cast to the view column's at its position fails, and one that keeps
        // the view's columns at their positions is taken.
        assertMemoryLeak(() -> {
            createLongNameBase("base");
            execute("CREATE MATERIALIZED VIEW ptv AS (SELECT ts, " + L65 + ", price, amount FROM base) PARTITION BY DAY");
            drainWalAndMatViewQueues();
            assertPassthroughAlterRefused("ptv", "SELECT dateadd('m', 15, ts) AS ts, " + L65 + ", price, amount FROM base", "dateadd");
            final String noCast = "ALTER MATERIALIZED VIEW ptv AS (SELECT ts, " + L65 + ", ok AS price, amount FROM base)";
            assertAlterRefused("ptv", noCast, noCast.indexOf("SELECT"), "inconvertible types: BOOLEAN -> DOUBLE [from=price, to=price]");
            insertSecondHour("base");
            assertViewStatus("ptv", "valid", "");
            assertQuery("SELECT ts, price, amount FROM ptv").timestamp("ts").noLeakCheck().expectSize().returns("""
                    ts\tprice\tamount
                    2024-01-01T00:10:00.000000Z\t1.0\t100.0
                    2024-01-01T00:20:00.000000Z\t9.0\t900.0
                    2024-01-01T01:10:00.000000Z\t2.0\t200.0
                    2024-01-01T01:20:00.000000Z\t6.0\t600.0
                    """);

            execute("ALTER MATERIALIZED VIEW ptv AS (SELECT ts, " + L65 + ", price * 2 AS price, amount FROM base)");
            assertMappingColumns("ptv", "", "");
            drainWalQueue();
            execute("REFRESH MATERIALIZED VIEW ptv FULL");
            drainWalAndMatViewQueues();
            assertViewStatus("ptv", "valid", "");
            assertQuery("SELECT ts, price, amount FROM ptv").timestamp("ts").noLeakCheck().expectSize().returns("""
                    ts\tprice\tamount
                    2024-01-01T00:10:00.000000Z\t2.0\t100.0
                    2024-01-01T00:20:00.000000Z\t18.0\t900.0
                    2024-01-01T01:10:00.000000Z\t4.0\t200.0
                    2024-01-01T01:20:00.000000Z\t12.0\t600.0
                    """);
        });
    }

    @Test
    public void testRebaseRegistersViewOnLoop() throws Exception {
        // A rebase of a view on a loop registers the rebased view with its edge, on the loop.
        setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_WRITE_DENIED, "true");
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            execute("CREATE MATERIALIZED VIEW other_1h AS (" + VIEW_SQL + ") PARTITION BY DAY");
            drainWalAndMatViewQueues();
            execute(ALTER_PREFIX + "WITH BASE other_1h AS (SELECT ts, sym, last(price) AS price FROM other_1h SAMPLE BY 1h)");
            execute("ALTER MATERIALIZED VIEW other_1h WITH BASE price_1h AS (SELECT ts, sym, last(price) AS price FROM price_1h SAMPLE BY 1h)");
            drainWalAndMatViewQueues();
            final TableToken oldView = viewToken();
            execute(ALTER_PREFIX + "SUSPEND WAL");
            execute(ALTER_PREFIX + "REBASE WAL");
            drainWalQueue();
            final TableToken newView = viewToken();
            Assert.assertNotEquals(oldView.getDirName(), newView.getDirName());
            final MatViewDefinition definition = engine.getDependentViewGraph().getViewDefinition(newView);
            Assert.assertNotNull(definition);
            Assert.assertEquals("other_1h", definition.getBaseTableName());
            Assert.assertTrue(engine.getDependentViewGraph().isOnDependencyLoop(definition));
            Assert.assertNotNull(engine.getMatViewStateStore().getViewState(newView));
        });
    }

    @Test
    public void testRangeStopsWaitingForDroppedView() throws Exception {
        // A REFRESH ... RANGE that waits for a pending definition refuses once the view is dropped, and enqueues nothing.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("MANUAL");
            execute(ALTER_PREFIX + "AS (" + TEN_TIMES_MAX_SQL + ")");
            final TableToken viewToken = viewToken();
            final AtomicBoolean isWaitSeen = new AtomicBoolean();
            viewState().setOnDefinitionWaitForTesting(() -> {
                isWaitSeen.set(true);
                try {
                    execute("DROP MATERIALIZED VIEW price_1h");
                } catch (SqlException e) {
                    throw new AssertionError(e);
                }
            });
            try {
                execute(RANGE_SQL);
                Assert.fail("the view is dropped");
            } catch (SqlException e) {
                TestUtils.assertEquals("materialized view does not exist [view=price_1h]", e.getFlyweightMessage());
                Assert.assertEquals(REFRESH_VIEW_NAME_POSITION, e.getPosition());
            }
            Assert.assertTrue(isWaitSeen.get());
            Assert.assertTrue(engine.getTableSequencerAPI().getTxnTracker(viewToken).isDropped());
            // the drop queued the invalidation of the views over the dropped view, and the RANGE queued nothing
            final MatViewRefreshTask task = new MatViewRefreshTask();
            Assert.assertTrue(engine.getMatViewStateStore().tryDequeueRefreshTask(task));
            Assert.assertEquals(MatViewRefreshTask.INVALIDATE, task.operation);
            Assert.assertFalse(engine.getMatViewStateStore().tryDequeueRefreshTask(task));
            drainWalAndMatViewQueues();
            Assert.assertNull(engine.getTableTokenIfExists("price_1h"));
        });
    }

    @Test
    public void testRefreshStopsWaitingForDroppedView() throws Exception {
        // A REFRESH that waits for a pending definition stops waiting once the view is dropped.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            execute(ALTER_PREFIX + "AS (SELECT ts, sym, max(price) AS price FROM base SAMPLE BY 1h)");
            final TableToken viewToken = viewToken();
            final AtomicBoolean isWaitSeen = new AtomicBoolean();
            viewState().setOnDefinitionWaitForTesting(() -> {
                isWaitSeen.set(true);
                try {
                    execute("DROP MATERIALIZED VIEW price_1h");
                } catch (SqlException e) {
                    throw new AssertionError(e);
                }
            });
            // the FULL refresh then finds the view gone
            try {
                execute("REFRESH MATERIALIZED VIEW price_1h FULL");
                Assert.fail("the view is dropped");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "table does not exist [table=price_1h]");
            }
            Assert.assertTrue(isWaitSeen.get());
            Assert.assertTrue(engine.getTableSequencerAPI().getTxnTracker(viewToken).isDropped());
            drainWalAndMatViewQueues();
        });
    }

    @Test
    public void testRefreshWaitsUntilDefinitionApplies() throws Exception {
        // REFRESH polls the view's WAL until another thread applies the definition event.
        assertMemoryLeak(() -> {
            currentMicros = parseFloorPartialTimestamp("2024-01-05T00:00:00.000000Z");
            createBaseWithRefreshedView("IMMEDIATE");
            execute(ALTER_PREFIX + "AS (SELECT ts, sym, max(price) * 10 AS price FROM base SAMPLE BY 1h)");
            final AtomicReference<Throwable> applierError = new AtomicReference<>();
            final Thread applier = new Thread(() -> {
                try {
                    drainWalQueue();
                } catch (Throwable th) {
                    applierError.set(th);
                } finally {
                    Path.clearThreadLocals();
                }
            });
            viewState().setOnDefinitionWaitForTesting(applier::start);
            execute("REFRESH MATERIALIZED VIEW price_1h RANGE FROM '2024-01-01T01:00:00.000000Z' TO '2024-01-01T01:59:59.999999Z'");
            applier.join();
            Assert.assertNull(applierError.get());
            Assert.assertEquals("SELECT ts, sym, max(price) * 10 AS price FROM base SAMPLE BY 1h", viewDefinition().getMatViewSql());
            drainMatViewQueue(engine);
            drainWalAndMatViewQueues();
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    2024-01-01T01:00:00.000000Z\tb\t20.0
                    """);
        });
    }

    @Test
    public void testRestartAfterAppliedDefinitionAndSqlEvent() throws Exception {
        // A restart whose newest view transaction is an applied definition event, or a SQL event after it, loads the
        // view's state from its state file.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            execute(ALTER_PREFIX + "AS (SELECT ts, sym, max(price) AS price FROM base SAMPLE BY 1h)");
            drainWalQueue();
            restartStates();
            Assert.assertFalse(viewState().isInvalid());
            assertRefreshBaseTxn(1);
            execute(ALTER_PREFIX + "SET REFRESH LIMIT 10 DAYS");
            drainWalQueue();
            restartStates();
            Assert.assertFalse(viewState().isInvalid());
            assertRefreshBaseTxn(1);
            Assert.assertEquals(engine.verifyTableName("base").getTableId(), viewState().getBaseTableId());
        });
    }

    @Test
    public void testValidationOfGridAndTimestamp() throws Exception {
        // The SAMPLE BY interval, its unit, time zone and offset of the view's, and its designated timestamp under its
        // type and as the query's designated timestamp.
        assertMemoryLeak(() -> {
            createBase("base");
            execute("INSERT INTO base VALUES ('a', 1.0, 1.0, '2024-01-01T00:10:00.000000Z')");
            execute("CREATE MATERIALIZED VIEW price_1h AS (SELECT ts, sym, last(price) AS price FROM base SAMPLE BY 1h ALIGN TO CALENDAR TIME ZONE 'Europe/London' WITH OFFSET '00:15') PARTITION BY DAY");
            drainWalAndMatViewQueues();
            final String grid = " ALIGN TO CALENDAR TIME ZONE 'Europe/London' WITH OFFSET '00:15')";
            assertAlterFails("AS (SELECT ts, sym, last(price) AS price FROM base SAMPLE BY 60m" + grid, "60m",
                    "materialized view query must keep the view's sampling interval [interval=1h, queryInterval=60m]");
            assertAlterFails("AS (SELECT ts, sym, last(price) AS price FROM base SAMPLE BY 1d" + grid, "1d",
                    "materialized view query must keep the view's sampling interval [interval=1h, queryInterval=1d]");
            assertAlterFails("AS (SELECT ts, sym, last(price) AS price FROM base SAMPLE BY 1h ALIGN TO CALENDAR TIME ZONE 'Europe/London' WITH OFFSET '00:30')", "'00:30'",
                    "materialized view query must keep the view's SAMPLE BY time zone and offset [timeZone=Europe/London, offset=00:15]");
            assertAlterFails("AS (SELECT ts, sym, last(price) AS price FROM base SAMPLE BY 1h ALIGN TO CALENDAR TIME ZONE 'Europe/Paris' WITH OFFSET '00:15')", "'Europe/Paris'",
                    "materialized view query must keep the view's SAMPLE BY time zone and offset [timeZone=Europe/London, offset=00:15]");
            assertAlterFails("AS (SELECT timestamp_floor('1h', ts) AS ts, sym, last(price) AS price FROM base)", "SELECT",
                    "materialized view query must keep the view's SAMPLE BY time zone and offset [timeZone=Europe/London, offset=00:15]");
            // the same time zone in another case, and a statement that ends with a semicolon
            execute(ALTER_PREFIX + "AS (SELECT ts, sym, max(price) AS price FROM base SAMPLE BY 1h ALIGN TO CALENDAR TIME ZONE 'europe/london' WITH OFFSET '00:15');");
            drainWalAndMatViewQueues();
            Assert.assertEquals("SELECT ts, sym, max(price) AS price FROM base SAMPLE BY 1h ALIGN TO CALENDAR TIME ZONE 'europe/london' WITH OFFSET '00:15'", viewDefinition().getMatViewSql());

            execute("CREATE MATERIALIZED VIEW floor_1h AS (SELECT timestamp_floor('1h', ts) AS ts, sym, last(price) AS price FROM base ORDER BY ts) PARTITION BY DAY");
            drainWalAndMatViewQueues();
            final String noDesignated = "ALTER MATERIALIZED VIEW floor_1h AS (SELECT timestamp_floor('1h', ts) AS ts, sym, last(price) AS price FROM base)";
            assertExceptionNoLeakCheck(noDesignated, noDesignated.indexOf("timestamp_floor"),
                    "materialized view query must produce the view's designated timestamp as its designated timestamp [column=ts, type=TIMESTAMP]");
            final String nanos = "ALTER MATERIALIZED VIEW floor_1h AS (SELECT timestamp_floor('1h', ts::TIMESTAMP_NS) AS ts, sym, last(price) AS price FROM base ORDER BY ts)";
            assertExceptionNoLeakCheck(nanos, nanos.indexOf("timestamp_floor"),
                    "materialized view query must produce the view's designated timestamp as its designated timestamp [column=ts, type=TIMESTAMP]");
            final String withTimeZone = "ALTER MATERIALIZED VIEW floor_1h AS (SELECT ts, sym, last(price) AS price FROM base SAMPLE BY 1h ALIGN TO CALENDAR TIME ZONE 'Europe/London')";
            assertExceptionNoLeakCheck(withTimeZone, withTimeZone.indexOf("'Europe/London'"),
                    "materialized view query must keep the view's SAMPLE BY time zone and offset [timeZone=, offset=00:00]");
        });
    }

    @Test
    public void testBootLoadOpensNoSequencerForViewWithNothingUnapplied() throws Exception {
        // The boot load reads the header of the view's transaction log first, and opens the view's sequencer only when
        // the header counts a transaction above the view's applied seqTxn. NOSYNC: under ADAPTIVE the engine's recovery
        // pass opens the sequencer of every table with a save point before the load.
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        final AtomicBoolean isCounting = new AtomicBoolean();
        final AtomicInteger txnLogOpens = new AtomicInteger();
        final StringSink txnLogFile = new StringSink();
        final TestFilesFacadeImpl ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (isCounting.get() && txnLogFile.length() > 0 && Utf8s.endsWithAscii(name, txnLogFile)) {
                    txnLogOpens.incrementAndGet();
                }
                return super.openRW(name, opts);
            }
        };
        assertMemoryLeak(ff, () -> {
            createBaseWithRefreshedView("MANUAL");
            txnLogFile.put(viewToken().getDirName()).put(Files.SEPARATOR).put(WalUtils.SEQ_DIR).put(Files.SEPARATOR).put(WalUtils.TXNLOG_FILE_NAME);
            Assert.assertEquals(engine.getTableSequencerAPI().lastTxn(viewToken()), engine.getTableSequencerAPI().getTxnTracker(viewToken()).getWriterTxn());
            restartEngineCountingBoot(isCounting, (restarted, context) -> {
                Assert.assertEquals(0, txnLogOpens.get());
                Assert.assertEquals(-1, viewState(restarted).getPendingDefinitionSeqTxn());
            });
            execute(ALTER_PREFIX + "SET REFRESH LIMIT 1 HOUR");
            final long lastTxn = engine.getTableSequencerAPI().lastTxn(viewToken());
            Assert.assertEquals(lastTxn - 1, engine.getTableSequencerAPI().getTxnTracker(viewToken()).getWriterTxn());
            restartEngineCountingBoot(isCounting, (restarted, context) -> {
                Assert.assertEquals(1, txnLogOpens.get());
                Assert.assertEquals(-1, viewState(restarted).getPendingDefinitionSeqTxn());
            });
        });
    }

    @Test
    public void testBootLoadWithBrokenViewTxnLogKeepsLoadOutcome() throws Exception {
        // A view whose transaction log the boot load cannot read loads as before: a view on a dropped base gets its
        // invalidation, one on a present base its default state, and the view records no pending definition. The load
        // logs why, except for a log shorter than the count in its header, whose short read leaves nothing to log, and
        // the next view loads its pending definition. NOSYNC: under ADAPTIVE the engine's recovery pass opens the log
        // first, and fails the boot.
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        assertMemoryLeak(() -> {
            final String seedError = "could not read pending definition of materialized view, recording none \\[view=price_1h~";
            for (int round = 0; round < 6; round++) {
                final boolean isBaseDropped = round > 2;
                final int brokenLog = round % 3;
                createBase("base_o");
                execute("INSERT INTO base_o VALUES ('o', 1.0, 1.0, '2024-01-01T00:10:00.000000Z')");
                execute("CREATE MATERIALIZED VIEW other_1h AS (SELECT ts, sym, last(price) AS price FROM base_o SAMPLE BY 1h) PARTITION BY DAY");
                createBaseWithRefreshedView("IMMEDIATE");
                if (isBaseDropped) {
                    execute("DROP TABLE base");
                    drainWalAndMatViewQueues();
                }
                execute("ALTER MATERIALIZED VIEW other_1h AS (SELECT ts, sym, max(price) AS price FROM base_o SAMPLE BY 1h)");
                final long otherDefinitionSeqTxn = engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("other_1h"));
                final TableToken viewToken = viewToken();
                // a transaction above the view's applied seqTxn, so that the header of its log counts one
                execute(ALTER_PREFIX + "SET REFRESH LIMIT 1 HOUR");
                Assert.assertEquals(
                        engine.getTableSequencerAPI().getTxnTracker(viewToken).getWriterTxn() + 1,
                        engine.getTableSequencerAPI().lastTxn(viewToken)
                );
                releaseAllForLogEdit();
                final java.nio.file.Path txnLog = new File(txnLogPath(viewToken)).toPath();
                final byte[] txnLogBytes = java.nio.file.Files.readAllBytes(txnLog);
                switch (brokenLog) {
                    case 0 -> {
                        // shorter than its header
                        try (RandomAccessFile f = new RandomAccessFile(txnLog.toFile(), "rw")) {
                            f.setLength(10);
                        }
                    }
                    case 1 -> {
                        // of an unknown format version
                        try (RandomAccessFile f = new RandomAccessFile(txnLog.toFile(), "rw")) {
                            f.seek(TableTransactionLogFile.TX_LOG_STRUCTURE_VERSION_OFFSET);
                            f.write(new byte[]{7, 0, 0, 0});
                        }
                    }
                    default -> java.nio.file.Files.delete(txnLog);
                }
                final int finalRound = round;
                final LogCapture logCapture = new LogCapture();
                logCapture.start();
                try {
                    restartEngine((restarted, context) -> {
                        logCapture.drain();
                        Assert.assertEquals("round=" + finalRound, brokenLog == 0 ? 0 : 1, logCapture.countLoggedRE(seedError));
                        final MatViewState state = viewState(restarted);
                        Assert.assertEquals(-1, state.getPendingDefinitionSeqTxn());
                        Assert.assertFalse(state.isInvalid());
                        final TableToken otherToken = restarted.verifyTableName("other_1h");
                        Assert.assertEquals(otherDefinitionSeqTxn, restarted.getMatViewStateStore().getViewState(otherToken).getPendingDefinitionSeqTxn());
                        // the refresh job would open the broken log, so the test reads the tasks from the queue
                        TestUtils.assertEquals(
                                "round=" + finalRound,
                                isBaseDropped ? "invalidate base table does not exist\n" : "",
                                queuedViewTasks(restarted)
                        );
                    });
                } finally {
                    logCapture.stop();
                }
                // the static engine opens the view's log again
                java.nio.file.Files.write(txnLog, txnLogBytes);
                execute("DROP MATERIALIZED VIEW other_1h");
                execute("DROP MATERIALIZED VIEW price_1h");
                execute("DROP TABLE base_o");
                execute("DROP TABLE IF EXISTS base");
                drainWalAndMatViewQueues();
            }
        });
    }

    @Test
    public void testBootLoadWithoutAssertionsRecordsPendingDefinition() throws Exception {
        // A JVM without -ea, as the Windows service starts QuestDB, boots on views whose WAL holds an ALTER ... AS that
        // it has not applied, in a transaction log of one file (V1) and in parts (V2), as the last transaction and below
        // an unapplied invalidation: each view loads the definition event as pending, so the walk does not depend on
        // assertions to position its cursor.
        assertMemoryLeak(() -> {
            createBase("base");
            execute("INSERT INTO base VALUES ('a', 1.0, 1.0, '2024-01-01T00:10:00.000000Z')");
            drainWalQueue();
            final String[] viewNames = {"v1_last", "v1_below", "v2_last", "v2_below"};
            final long[] definitionSeqTxns = new long[viewNames.length];
            for (int i = 0; i < viewNames.length; i++) {
                setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, i < 2 ? 0 : 2);
                execute("CREATE MATERIALIZED VIEW " + viewNames[i] + " AS (" + VIEW_SQL + ") PARTITION BY DAY");
            }
            drainWalAndMatViewQueues();
            for (int i = 0; i < viewNames.length; i++) {
                final TableToken viewToken = engine.verifyTableName(viewNames[i]);
                try (TransactionLogCursor cursor = engine.getTableSequencerAPI().getCursor(viewToken, 0)) {
                    Assert.assertEquals(i < 2 ? WalUtils.WAL_SEQUENCER_FORMAT_VERSION_V1 : WalUtils.WAL_SEQUENCER_FORMAT_VERSION_V2, cursor.getVersion());
                }
                execute("ALTER MATERIALIZED VIEW " + viewNames[i] + " AS (" + TEN_TIMES_MAX_SQL + ")");
                definitionSeqTxns[i] = engine.getTableSequencerAPI().lastTxn(viewToken);
                if (i % 2 == 1) {
                    engine.getMatViewStateStore().enqueueInvalidate(viewToken, "test invalidation");
                    drainMatViewQueue(engine);
                    Assert.assertEquals(definitionSeqTxns[i] + 1, engine.getTableSequencerAPI().lastTxn(viewToken));
                }
                Assert.assertEquals(definitionSeqTxns[i] - 1, engine.getTableSequencerAPI().getTxnTracker(viewToken).getWriterTxn());
            }
            releaseAllForLogEdit();
            final StringSink expected = new StringSink();
            for (int i = 0; i < viewNames.length; i++) {
                expected.put("pending definition ").put(viewNames[i]).put('=').put(definitionSeqTxns[i]).put('\n');
            }
            // the child's println ends a line with \r\n on Windows and \n elsewhere, and lines() strips either
            final StringSink printed = new StringSink();
            runBootLoadMain(viewNames).lines()
                    .filter(line -> line.startsWith("pending definition "))
                    .forEach(line -> printed.put(line).put('\n'));
            TestUtils.assertEquals(expected, printed);
            drainWalAndMatViewQueues();
            for (String viewName : viewNames) {
                Assert.assertEquals(TEN_TIMES_MAX_SQL, engine.getDependentViewGraph().getViewDefinition(engine.verifyTableName(viewName)).getMatViewSql());
            }
        });
    }

    @Test
    public void testReloadRecordsNoPendingDefinition() throws Exception {
        // A reload of the view states, as a promote runs it while the WAL apply runs, records no pending definition:
        // the WAL apply could apply the event before the reloaded store is in place, which would leave it pending.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            execute(ALTER_PREFIX + "AS (" + X_SQL + ")");
            Assert.assertTrue(viewState().isDefinitionPending());
            restartStates();
            Assert.assertEquals(-1, viewState().getPendingDefinitionSeqTxn());
            Assert.assertFalse(viewState().isDefinitionPending());
        });
    }

    @Test
    public void testRestartKeepsBackToBackBaseRevertFollowingSecondStatement() throws Exception {
        // A restart between two ALTER ... WITH BASE statements, before the view's WAL applies the first: the second waits
        // for the first, compares its base table with the first one's, and moves the view back to its old base table.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            createBase2WithRow();
            execute(ALTER_PREFIX + "WITH BASE base2 AS (" + BASE2_SQL + ")");
            final long definitionSeqTxn = viewState().getPendingDefinitionSeqTxn();
            restartEngine((restarted, context) -> {
                Assert.assertEquals(definitionSeqTxn, viewState(restarted).getPendingDefinitionSeqTxn());
                final AtomicBoolean isWaitSeen = armSeamDrain(restarted);
                restarted.execute(ALTER_PREFIX + "WITH BASE base AS (" + TEN_TIMES_MAX_SQL + ")", context);
                Assert.assertTrue(isWaitSeen.get());
                drainWalAndMatViewQueues(restarted);
                Assert.assertEquals("base", viewDefinition(restarted).getBaseTableName());
                Assert.assertEquals(TEN_TIMES_MAX_SQL, viewDefinition(restarted).getMatViewSql());
                assertViewStatus(restarted, context, "invalid", "base table changed");
                restarted.execute(RESUME_SKIP_SQL, context);
                drainWalAndMatViewQueues(restarted);
                assertViewStatus(restarted, context, "valid", "");
                Assert.assertEquals(restarted.verifyTableName("base").getTableId(), viewState(restarted).getBaseTableId());
            });
        });
    }

    @Test
    public void testRestartKeepsRefreshWaitingForPendingDefinition() throws Exception {
        // A restart before the view's WAL applies an ALTER ... AS loads its definition event as pending: REFRESH ... FULL
        // waits until the WAL applies it, and rebuilds the view with the new SQL.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            final String newSql = "SELECT ts, sym, last(price) * 10 AS price FROM base SAMPLE BY 1h";
            execute(ALTER_PREFIX + "AS (" + newSql + ")");
            final long definitionSeqTxn = viewState().getPendingDefinitionSeqTxn();
            restartEngine((restarted, context) -> {
                Assert.assertEquals(definitionSeqTxn, viewState(restarted).getPendingDefinitionSeqTxn());
                final AtomicBoolean isWaitSeen = armSeamDrain(restarted);
                restarted.execute("REFRESH MATERIALIZED VIEW price_1h FULL", context);
                Assert.assertTrue(isWaitSeen.get());
                drainWalAndMatViewQueues(restarted);
                try (SqlCompiler compiler = restarted.getSqlCompiler()) {
                    TestUtils.assertSqlCursors(compiler, context, newSql, "price_1h", LOG);
                }
                // ten times the last price of each bucket
                assertQuery("price_1h ORDER BY ts, sym").withEngine(restarted).withContext(context).timestamp("ts").noLeakCheck().expectSize().returns("""
                        ts\tsym\tprice
                        2024-01-01T00:00:00.000000Z\ta\t10.0
                        2024-01-01T01:00:00.000000Z\tb\t20.0
                        """);
                assertQuery("SELECT view_sql, view_status FROM materialized_views() WHERE view_name = 'price_1h'")
                        .withEngine(restarted)
                        .withContext(context)
                        .noLeakCheck()
                        .noRandomAccess()
                        .returns("view_sql\tview_status\n" + newSql + "\tvalid\n");
            });
        });
    }

    @Test
    public void testRestartKeepsViewAlterRefusedWhileListedViewWalShortOfDefinition() throws Exception {
        // A view in cairo.wal.apply.suspended.tables across a restart, whose WAL holds an ALTER ... AS it has not
        // applied: the restart loads the definition event as pending, so the view ALTERs that check the view's SQL and
        // REFRESH ... RANGE refuse with the list hint, before the WAL apply runs and after, until the list no longer
        // holds the view and its WAL applies the event.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            execute(ALTER_PREFIX + "AS (" + X_SQL + ")");
            final long definitionSeqTxn = viewState().getPendingDefinitionSeqTxn();
            final String dirName = viewToken().getDirName();
            final String refusal = DEFINITION_NOT_APPLIED + "remove " + dirName
                    + " from cairo.wal.apply.suspended.tables and run ALTER MATERIALIZED VIEW price_1h RESUME WAL first";
            setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_TABLES, dirName);
            try {
                restartEngine((restarted, context) -> {
                    Assert.assertEquals(definitionSeqTxn, viewState(restarted).getPendingDefinitionSeqTxn());
                    for (int round = 0; round < 2; round++) {
                        if (round == 1) {
                            drainWalQueue(restarted);
                        }
                        armNoWaitGuard(restarted);
                        try {
                            for (String tail : new String[]{
                                    "ADD COLUMN x CHAR",
                                    "DROP COLUMN price",
                                    "AS (" + VIEW_SQL + ")",
                                    "WITH BASE base AS (" + VIEW_SQL + ")"
                            }) {
                                assertRefused(restarted, context, ALTER_PREFIX + tail, VIEW_NAME_POSITION, refusal);
                            }
                            assertRefused(restarted, context, RANGE_SQL, REFRESH_VIEW_NAME_POSITION, refusal);
                        } finally {
                            viewState(restarted).setOnDefinitionWaitForTesting(null);
                        }
                        // the list holds the view, so its WAL does not apply
                        Assert.assertEquals(VIEW_SQL, viewDefinition(restarted).getMatViewSql());
                    }
                    setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_TABLES, null);
                    drainWalQueue(restarted);
                    Assert.assertEquals(X_SQL, viewDefinition(restarted).getMatViewSql());
                    final String addChar = ALTER_PREFIX + "ADD COLUMN x CHAR";
                    assertRefused(restarted, context, addChar, addChar.indexOf("x CHAR"), "inconvertible types: DOUBLE -> CHAR [from=x, to=x]");
                });
            } finally {
                setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_TABLES, null);
            }
        });
    }

    @Test
    public void testRestartKeepsViewAlterWaitingForPendingDefinition() throws Exception {
        // A restart before the view's WAL applies an ALTER ... AS loads its definition event as pending: a view ADD
        // COLUMN waits until the WAL applies it and checks itself against the new SQL, as on the node before the
        // restart, and a view ALTER after that does not wait.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            execute(ALTER_PREFIX + "AS (" + X_SQL + ")");
            final long definitionSeqTxn = viewState().getPendingDefinitionSeqTxn();
            Assert.assertEquals(engine.getTableSequencerAPI().lastTxn(viewToken()), definitionSeqTxn);
            restartEngine((restarted, context) -> {
                Assert.assertEquals(definitionSeqTxn, viewState(restarted).getPendingDefinitionSeqTxn());
                Assert.assertTrue(viewState(restarted).isDefinitionPending());
                final AtomicBoolean isWaitSeen = armSeamDrain(restarted);
                final String addChar = ALTER_PREFIX + "ADD COLUMN x CHAR";
                assertRefused(restarted, context, addChar, addChar.indexOf("x CHAR"), "inconvertible types: DOUBLE -> CHAR [from=x, to=x]");
                Assert.assertTrue(isWaitSeen.get());
                Assert.assertEquals(X_SQL, viewDefinition(restarted).getMatViewSql());
                armNoWaitGuard(restarted);
                try {
                    restarted.execute(ALTER_PREFIX + "ADD COLUMN extra INT", context);
                } finally {
                    viewState(restarted).setOnDefinitionWaitForTesting(null);
                }
                Assert.assertEquals(definitionSeqTxn + 1, restarted.getTableSequencerAPI().lastTxn(restarted.verifyTableName("price_1h")));
            });
        });
    }

    @Test
    public void testRestartRecordsDefinitionBelowTornTxnLogTail() throws Exception {
        // A committed definition event below a torn one: the restart loads the committed one as pending, and a view ADD
        // COLUMN waits for it and checks itself against its SQL.
        setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 0);
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("MANUAL");
            final long committedSeqTxn;
            try (WalWriter writer = engine.getWalWriter(viewToken())) {
                committedSeqTxn = writer.appendMatViewDefinition("base", X_SQL, false);
            }
            execute(ALTER_PREFIX + "AS (" + TEN_TIMES_MAX_SQL + ")");
            final long tornSeqTxn = viewState().getPendingDefinitionSeqTxn();
            Assert.assertEquals(committedSeqTxn + 1, tornSeqTxn);
            tearTxnLogRecord(tornSeqTxn);
            restartEngine((restarted, context) -> {
                Assert.assertEquals(committedSeqTxn, viewState(restarted).getPendingDefinitionSeqTxn());
                Assert.assertEquals(committedSeqTxn, restarted.getTableSequencerAPI().lastTxn(restarted.verifyTableName("price_1h")));
                final AtomicBoolean isWaitSeen = armSeamDrain(restarted);
                final String addChar = ALTER_PREFIX + "ADD COLUMN x CHAR";
                assertRefused(restarted, context, addChar, addChar.indexOf("x CHAR"), "inconvertible types: DOUBLE -> CHAR [from=x, to=x]");
                Assert.assertTrue(isWaitSeen.get());
                Assert.assertEquals(X_SQL, viewDefinition(restarted).getMatViewSql());
            });
        });
    }

    @Test
    public void testRestartRecordsNewestOfSeveralPendingDefinitions() throws Exception {
        // Two definition events that the view's WAL has not applied: the restart loads the newer one as pending, so a
        // refresh defers until the WAL applies both, and runs with the newer SQL.
        final AtomicBoolean isFailArmed = new AtomicBoolean();
        final AtomicInteger definitionOpens = new AtomicInteger();
        assertMemoryLeak(newDefinitionWriteFailingFacade(isFailArmed, definitionOpens, 2), () -> {
            createBaseWithRefreshedView("IMMEDIATE");
            final long firstSeqTxn;
            final long secondSeqTxn;
            try (WalWriter writer = engine.getWalWriter(viewToken())) {
                firstSeqTxn = writer.appendMatViewDefinition("base", X_SQL, false);
                secondSeqTxn = writer.appendMatViewDefinition("base", TEN_TIMES_MAX_SQL, false);
            }
            restartEngine((restarted, context) -> {
                final TableToken viewToken = restarted.verifyTableName("price_1h");
                final MatViewState state = viewState(restarted);
                Assert.assertEquals(secondSeqTxn, state.getPendingDefinitionSeqTxn());
                // the WAL apply applies the first event, and fails on the second
                isFailArmed.set(true);
                try {
                    drainWalQueue(restarted);
                } finally {
                    isFailArmed.set(false);
                }
                Assert.assertEquals(2, definitionOpens.get());
                Assert.assertEquals(firstSeqTxn, restarted.getTableSequencerAPI().getTxnTracker(viewToken).getWriterTxn());
                Assert.assertEquals(firstSeqTxn, state.getAppliedDefinitionSeqTxnForTesting());
                Assert.assertTrue(state.isDefinitionPending());
                restarted.execute("INSERT INTO base VALUES ('c', 3.0, 1.0, '2024-01-01T02:10:00.000000Z')", context);
                drainWalQueue(restarted);
                final long viewSeqTxn = restarted.getTableSequencerAPI().lastTxn(viewToken);
                drainMatViewQueue(restarted);
                // the refresh deferred and wrote nothing
                Assert.assertEquals(viewSeqTxn, restarted.getTableSequencerAPI().lastTxn(viewToken));
                Assert.assertTrue(state.hasDeferredDefinitionRefreshForTesting());
                restarted.execute(ALTER_PREFIX + "RESUME WAL", context);
                drainWalAndMatViewQueues(restarted);
                Assert.assertEquals(secondSeqTxn, state.getAppliedDefinitionSeqTxnForTesting());
                Assert.assertEquals(TEN_TIMES_MAX_SQL, viewDefinition(restarted).getMatViewSql());
                // ten times the maximum price of the new bucket
                assertQuery("price_1h ORDER BY ts, sym").withEngine(restarted).withContext(context).timestamp("ts").noLeakCheck().expectSize().returns(
                        VIEW_ROWS + "2024-01-01T02:00:00.000000Z\tc\t30.0\n"
                );
            });
        });
    }

    @Test
    public void testRestartRecordsNothingForAppliedDefinition() throws Exception {
        // An ALTER ... AS that the view's WAL applied, below a transaction it has not: the restart loads no pending
        // definition, so a view ALTER does not wait and a refresh does not defer.
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            execute(ALTER_PREFIX + "AS (" + TEN_TIMES_MAX_SQL + ")");
            final long definitionSeqTxn = viewState().getPendingDefinitionSeqTxn();
            drainWalQueue();
            Assert.assertEquals(definitionSeqTxn, engine.getTableSequencerAPI().getTxnTracker(viewToken()).getWriterTxn());
            execute(ALTER_PREFIX + "SET REFRESH LIMIT 10 DAYS");
            restartEngine((restarted, context) -> {
                Assert.assertEquals(-1, viewState(restarted).getPendingDefinitionSeqTxn());
                Assert.assertFalse(viewState(restarted).isDefinitionPending());
                armNoWaitGuard(restarted);
                try {
                    restarted.execute(ALTER_PREFIX + "ADD COLUMN extra INT", context);
                } finally {
                    viewState(restarted).setOnDefinitionWaitForTesting(null);
                }
                restarted.execute("INSERT INTO base VALUES ('c', 3.0, 1.0, '2024-01-01T02:10:00.000000Z')", context);
                drainWalAndMatViewQueues(restarted);
                Assert.assertFalse(viewState(restarted).hasDeferredDefinitionRefreshForTesting());
                Assert.assertEquals(2, viewState(restarted).getLastRefreshBaseTxn());
                assertQuery("SELECT ts, sym, price FROM price_1h ORDER BY ts, sym").withEngine(restarted).withContext(context).timestamp("ts").noLeakCheck().expectSize().returns(
                        VIEW_ROWS + "2024-01-01T02:00:00.000000Z\tc\t30.0\n"
                );
            });
        });
    }

    @Test
    public void testRestartRecordsNothingFromTornTxnLogTailAdaptive() throws Exception {
        assertRestartRecordsNothingFromTornTxnLogTail("adaptive");
    }

    @Test
    public void testRestartRecordsNothingFromTornTxnLogTailNosync() throws Exception {
        assertRestartRecordsNothingFromTornTxnLogTail("nosync");
    }

    @Test
    public void testRestartRecordsNothingFromTornTxnLogTailSync() throws Exception {
        assertRestartRecordsNothingFromTornTxnLogTail("sync");
    }

    @Test
    public void testRestartRecordsNothingFromV1TxnLogShorterThanItsHeader() throws Exception {
        // A power loss before the definition event's record reached the disk whole: the log's header counts it, but the
        // file ends inside it. The sequencer's open drops it, so the restart records no pending definition.
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "sync");
        setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 0);
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("MANUAL");
            execute(ALTER_PREFIX + "AS (" + X_SQL + ")");
            final long definitionSeqTxn = viewState().getPendingDefinitionSeqTxn();
            Assert.assertEquals(definitionSeqTxn, engine.getTableSequencerAPI().lastTxn(viewToken()));
            releaseAllForLogEdit();
            try (RandomAccessFile f = new RandomAccessFile(txnLogPath(viewToken()), "rw")) {
                writeLong(f, TableTransactionLogV1.UNSYNCED_FROM_TXN_OFFSET_64, definitionSeqTxn);
                Assert.assertEquals(definitionSeqTxn, readLong(f, TableTransactionLogFile.MAX_TXN_OFFSET_64));
                f.setLength(TableTransactionLogFile.HEADER_SIZE + (definitionSeqTxn - 1) * TableTransactionLogV1.RECORD_SIZE + TableTransactionLogV1.RECORD_SIZE / 2);
            }
            restartEngine((restarted, context) -> {
                Assert.assertEquals(-1, viewState(restarted).getPendingDefinitionSeqTxn());
                Assert.assertEquals(definitionSeqTxn - 1, restarted.getTableSequencerAPI().lastTxn(restarted.verifyTableName("price_1h")));
                armNoWaitGuard(restarted);
                try {
                    restarted.execute(ALTER_PREFIX + "ADD COLUMN x CHAR", context);
                } finally {
                    viewState(restarted).setOnDefinitionWaitForTesting(null);
                }
                drainWalQueue(restarted);
                Assert.assertEquals(VIEW_SQL, viewDefinition(restarted).getMatViewSql());
            });
        });
    }

    @Test
    public void testRestartRecordsPendingBaseChangeOfViewWithDroppedBase() throws Exception {
        assertRestartRecordsPendingBaseChangeOfViewWithoutWalBase(false);
    }

    @Test
    public void testRestartRecordsPendingBaseChangeOfViewWithNonWalBase() throws Exception {
        assertRestartRecordsPendingBaseChangeOfViewWithoutWalBase(true);
    }

    @Test
    public void testRestartRecordsPendingDefinitionOfNeverRefreshedView() throws Exception {
        // A view that never refreshed has no state to load, and its load records the pending definition all the same: a
        // view ADD COLUMN waits for it and checks itself against the new SQL.
        assertMemoryLeak(() -> {
            createBaseWithNeverRefreshedView();
            execute(ALTER_PREFIX + "AS (" + X_SQL + ")");
            final long definitionSeqTxn = viewState().getPendingDefinitionSeqTxn();
            restartEngine((restarted, context) -> {
                Assert.assertEquals(definitionSeqTxn, viewState(restarted).getPendingDefinitionSeqTxn());
                Assert.assertEquals(-1, viewState(restarted).getLastRefreshBaseTxn());
                final AtomicBoolean isWaitSeen = armSeamDrain(restarted);
                final String addChar = ALTER_PREFIX + "ADD COLUMN x CHAR";
                assertRefused(restarted, context, addChar, addChar.indexOf("x CHAR"), "inconvertible types: DOUBLE -> CHAR [from=x, to=x]");
                Assert.assertTrue(isWaitSeen.get());
                Assert.assertEquals(X_SQL, viewDefinition(restarted).getMatViewSql());
            });
        });
    }

    @Test
    public void testRestartRefusesViewAlterWhileRecordedDefinitionFailsToApply() throws Exception {
        // The definition event that the restart loads as pending fails to apply, which suspends the view's WAL: a view
        // ALTER refuses until RESUME WAL lets the event apply, and then checks itself against the new SQL.
        final AtomicBoolean isFailArmed = new AtomicBoolean();
        final AtomicInteger definitionOpens = new AtomicInteger();
        assertMemoryLeak(newDefinitionWriteFailingFacade(isFailArmed, definitionOpens, 1), () -> {
            createBaseWithRefreshedView("IMMEDIATE");
            execute(ALTER_PREFIX + "AS (" + X_SQL + ")");
            final long definitionSeqTxn = viewState().getPendingDefinitionSeqTxn();
            restartEngine((restarted, context) -> {
                final TableToken viewToken = restarted.verifyTableName("price_1h");
                Assert.assertEquals(definitionSeqTxn, viewState(restarted).getPendingDefinitionSeqTxn());
                isFailArmed.set(true);
                try {
                    drainWalQueue(restarted);
                } finally {
                    isFailArmed.set(false);
                }
                Assert.assertEquals(1, definitionOpens.get());
                Assert.assertTrue(restarted.getTableSequencerAPI().isSuspended(viewToken));
                Assert.assertEquals(definitionSeqTxn - 1, restarted.getTableSequencerAPI().getTxnTracker(viewToken).getWriterTxn());
                Assert.assertEquals(-1, viewState(restarted).getAppliedDefinitionSeqTxnForTesting());
                armNoWaitGuard(restarted);
                try {
                    assertRefused(
                            restarted,
                            context,
                            ALTER_PREFIX + "ADD COLUMN x CHAR",
                            VIEW_NAME_POSITION,
                            DEFINITION_NOT_APPLIED + "run ALTER MATERIALIZED VIEW price_1h RESUME WAL first"
                    );
                } finally {
                    viewState(restarted).setOnDefinitionWaitForTesting(null);
                }
                restarted.execute(ALTER_PREFIX + "RESUME WAL", context);
                drainWalQueue(restarted);
                Assert.assertEquals(definitionSeqTxn, viewState(restarted).getAppliedDefinitionSeqTxnForTesting());
                Assert.assertEquals(X_SQL, viewDefinition(restarted).getMatViewSql());
                final String addChar = ALTER_PREFIX + "ADD COLUMN x CHAR";
                assertRefused(restarted, context, addChar, addChar.indexOf("x CHAR"), "inconvertible types: DOUBLE -> CHAR [from=x, to=x]");
            });
        });
    }

    private static void assertDefinitionEventApplies() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, amount DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base VALUES ('a', 1.0, 1.0, '2024-01-01T00:10:00.000000Z')");
            execute("CREATE MATERIALIZED VIEW price_1h AS (" + VIEW_SQL + ") PARTITION BY DAY");
            drainWalAndMatViewQueues();
            execute("CREATE TABLE base2 (sym SYMBOL, price DOUBLE, amount DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute(ALTER_PREFIX + "AS (SELECT ts, sym, max(price) AS price FROM base SAMPLE BY 1h)");
            // the base change waits until the view's WAL applies the first statement's definition event
            drainWalQueue();
            execute(ALTER_PREFIX + "WITH BASE base2 AS (" + BASE2_SQL + ")");
            drainWalAndMatViewQueues();
            final TableToken viewToken = engine.verifyTableName("price_1h");
            final MatViewDefinition definition = engine.getDependentViewGraph().getViewDefinition(viewToken);
            Assert.assertEquals("base2", definition.getBaseTableName());
            Assert.assertTrue(engine.getMatViewStateStore().getViewState(viewToken).isInvalid());
        });
    }

    private static WalEventCursor openViewEvent(WalEventReader eventReader, Path path, long seqTxn) {
        final TableToken viewToken = engine.verifyTableName("price_1h");
        try (TransactionLogCursor cursor = engine.getTableSequencerAPI().getCursor(viewToken, seqTxn - 1)) {
            Assert.assertTrue(cursor.hasNext());
            Assert.assertEquals(seqTxn, cursor.getTxn());
            path.of(configuration.getDbRoot()).concat(viewToken).concat(WalUtils.WAL_NAME_BASE).put(cursor.getWalId()).slash().put(cursor.getSegmentId());
            return eventReader.of(path, cursor.getSegmentTxn());
        }
    }

    // Runs an ALTER from a checkpoint seam and applies the view's WAL, so that the checkpoint sees the new definition.
    private static void applyAlter(String sql) {
        try {
            execute(sql);
        } catch (SqlException e) {
            throw new AssertionError(e);
        }
        drainWalQueue();
    }

    private static void armViewTxnFailure(AtomicBoolean isFailArmed, StringSink failingPart, int txnsAhead) {
        // With one transaction per sequencer part, each transaction opens a part of its own, numbered from 0.
        failingPart.clear();
        final TableToken viewToken = engine.verifyTableName("price_1h");
        final long seqTxn = engine.getTableSequencerAPI().getTxnTracker(viewToken).getSeqTxn();
        failingPart.put(viewToken.getDirName()).put(Files.SEPARATOR).put(WalUtils.SEQ_DIR).put(Files.SEPARATOR)
                .put(WalUtils.TXNLOG_PARTS_DIR).put(Files.SEPARATOR).put(seqTxn + txnsAhead);
        isFailArmed.set(true);
    }

    // CREATE MATERIALIZED VIEW refuses the query with the error of the aggregating timestamp rule at the token of the
    // query, which must be unique, and creates no view.
    private static void assertCreateRefusesAggregatingTimestamp(String query, String token) {
        final String head = "CREATE MATERIALIZED VIEW parity WITH BASE base AS (";
        final int tokenIndex = query.indexOf(token);
        Assert.assertTrue(tokenIndex > -1);
        Assert.assertEquals("the token must be unique", tokenIndex, query.lastIndexOf(token));
        try {
            execute(head + query + ") PARTITION BY DAY");
            Assert.fail("CREATE must refuse: " + query);
        } catch (SqlException e) {
            TestUtils.assertEquals(AGGREGATING_TIMESTAMP_ERROR, e.getFlyweightMessage());
            Assert.assertEquals(query, head.length() + tokenIndex, e.getPosition());
        }
        Assert.assertNull(engine.getTableTokenIfExists("parity"));
    }

    // CREATE MATERIALIZED VIEW refuses the query as the SQL of a passthrough view, with the error of the passthrough
    // timestamp rule, at a position of its own.
    private static void assertCreateRefusesPassthroughTimestamp(String query) {
        try {
            execute("CREATE MATERIALIZED VIEW parity AS (" + query + ") PARTITION BY DAY");
            Assert.fail("CREATE must refuse: " + query);
        } catch (SqlException e) {
            TestUtils.assertEquals(PASSTHROUGH_TIMESTAMP_ERROR, e.getFlyweightMessage());
        }
    }

    private static void assertNoLiveViewState(TableToken viewToken) {
        try (Path path = new Path()) {
            Assert.assertFalse(TableUtils.isMatViewStateFileExists(configuration, path, viewToken.getDirName()));
        }
    }

    private static void assertDependents(String baseTableName, String... expected) {
        final io.questdb.std.ObjList<TableToken> sink = new io.questdb.std.ObjList<>();
        engine.getDependentViewGraph().getDependentViews(engine.verifyTableName(baseTableName), sink);
        Assert.assertEquals(expected.length, sink.size());
        for (int i = 0; i < expected.length; i++) {
            Assert.assertEquals(expected[i], sink.getQuick(i).getTableName());
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            Assert.assertTrue("latch timeout", latch.await(30, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    private static boolean isCheckpointFilePresent(TableToken viewToken, String fileName) {
        try (Path path = new Path()) {
            path.of(configuration.getCheckpointRoot()).concat(configuration.getDbDirectory()).concat(viewToken).concat(fileName);
            return TestFilesFacadeImpl.INSTANCE.exists(path.$());
        }
    }

    // The names in the view's live table directory, sorted.
    private static String listLiveViewDir(TableToken viewToken) {
        final TreeSet<String> names = new TreeSet<>();
        final StringSink name = new StringSink();
        try (Path path = new Path()) {
            path.of(configuration.getDbRoot()).concat(viewToken);
            TestFilesFacadeImpl.INSTANCE.iterateDir(path.$(), (pUtf8NameZ, type) -> {
                if (Files.notDots(pUtf8NameZ)) {
                    name.clear();
                    Utf8s.utf8ToUtf16Z(pUtf8NameZ, name);
                    names.add(name.toString());
                }
            });
        }
        return String.join(",", names);
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

    // Fails, while armed, the opening of the part of the view's sequencer that failingPart names; a one-shot failure
    // disarms itself.
    private static TestFilesFacadeImpl newSeqPartFailingFacade(AtomicBoolean isFailArmed, StringSink failingPart, boolean isOneShot) {
        return new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (isFailArmed.get() && failingPart.length() > 0 && Utf8s.endsWithAscii(name, failingPart)) {
                    if (isOneShot) {
                        isFailArmed.set(false);
                    }
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };
    }

    // The definition that the checkpoint holds for the view.
    private static MatViewDefinition readCheckpointedDefinition(TableToken viewToken) {
        try (Path path = new Path(); BlockFileReader reader = new BlockFileReader(configuration)) {
            path.of(configuration.getCheckpointRoot()).concat(configuration.getDbDirectory());
            final MatViewDefinition definition = new MatViewDefinition();
            MatViewDefinition.readFrom(engine, definition, reader, path, path.size(), viewToken);
            return definition;
        }
    }

    // Arms the definition wait seam of the view price_1h on the engine to fail the statement that reaches it, see
    // armNoWaitGuard() above. The caller disarms it.
    private static void armNoWaitGuard(CairoEngine e) {
        viewState(e).setOnDefinitionWaitForTesting(() -> {
            throw new AssertionError("the statement waited for the pending definition event");
        });
    }

    // Arms the definition wait seam of the view price_1h on the engine to apply the engine's WAL of every table, and
    // returns the flag the seam sets when it runs.
    private static AtomicBoolean armSeamDrain(CairoEngine e) {
        final AtomicBoolean isWaitSeen = new AtomicBoolean();
        viewState(e).setOnDefinitionWaitForTesting(() -> {
            isWaitSeen.set(true);
            drainWalQueue(e);
        });
        return isWaitSeen;
    }

    // Fails the statement on the engine with the message at the position, and asserts that it wrote nothing to the WAL
    // of the view price_1h.
    private static void assertRefused(CairoEngine e, SqlExecutionContext context, String sql, int position, String message) {
        Assert.assertTrue(position > -1);
        final TableToken viewToken = e.verifyTableName("price_1h");
        final long lastTxn = e.getTableSequencerAPI().lastTxn(viewToken);
        try {
            e.execute(sql, context);
            Assert.fail("the statement must fail: " + sql);
        } catch (SqlException ex) {
            TestUtils.assertEquals(message, ex.getFlyweightMessage());
            Assert.assertEquals(sql, position, ex.getPosition());
        }
        Assert.assertEquals(lastTxn, e.getTableSequencerAPI().lastTxn(viewToken));
    }

    // Fails, while armed, the open for write of the definition file _mv of the view price_1h, which the WAL apply job
    // writes as it applies a definition event, at the given count of such opens.
    private static TestFilesFacadeImpl newDefinitionWriteFailingFacade(AtomicBoolean isFailArmed, AtomicInteger definitionOpens, int failingOpen) {
        return new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (isFailArmed.get()
                        && Utf8s.endsWithAscii(name, MatViewDefinition.MAT_VIEW_DEFINITION_FILE_NAME)
                        && Utf8s.containsAscii(name, "price_1h")
                        && definitionOpens.incrementAndGet() == failingOpen) {
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };
    }

    // The refresh tasks of the view price_1h in the engine's queue, one line each, in queue order: the operation and the
    // invalidation reason, if any. The tasks go back on the queue in their order.
    private static String queuedViewTasks(CairoEngine e) {
        final ObjList<MatViewRefreshTask> tasks = new ObjList<>();
        final StringSink sink = new StringSink();
        final TableToken viewToken = e.verifyTableName("price_1h");
        while (true) {
            final MatViewRefreshTask task = new MatViewRefreshTask();
            if (!e.getMatViewStateStore().tryDequeueRefreshTask(task)) {
                break;
            }
            tasks.add(task);
            if (viewToken.equals(task.matViewToken)) {
                sink.put(MatViewRefreshTask.getRefreshOperationName(task.operation));
                if (task.invalidationReason != null) {
                    sink.put(' ').put(task.invalidationReason);
                }
                sink.put('\n');
            }
        }
        for (int i = 0, n = tasks.size(); i < n; i++) {
            e.getMatViewStateStore().reenqueueRefreshTask(tasks.getQuick(i));
        }
        return sink.toString();
    }

    private static long readLong(RandomAccessFile f, long offset) throws IOException {
        final byte[] bytes = new byte[Long.BYTES];
        f.seek(offset);
        f.readFully(bytes);
        return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getLong();
    }

    // Releases the engine's writers and sequencers, so that a test can edit a transaction log as a crash left it, and the
    // next engine opens it again.
    private static void releaseAllForLogEdit() {
        engine.releaseInactive();
        engine.releaseAllWriters();
        engine.releaseAllWalWriters();
        engine.releaseAllViewWalWriters();
        engine.getTableSequencerAPI().releaseAll();
    }

    // Runs MatViewBootLoadMain in a JVM without -ea, as the Windows service starts QuestDB, on the data directory and
    // the views, and returns its output once it exited with 0.
    private static String runBootLoadMain(String... viewNames) throws Exception {
        File javaExecutable = new File(new File(System.getProperty("java.home"), "bin"), "java");
        if (!javaExecutable.exists()) {
            javaExecutable = new File(javaExecutable.getPath() + ".exe");
        }
        final String classPath = Paths.get(MatViewBootLoadMain.class.getProtectionDomain().getCodeSource().getLocation().toURI())
                + File.pathSeparator
                + Paths.get(CairoEngine.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        final List<String> command = new ArrayList<>(List.of(
                javaExecutable.getAbsolutePath(),
                "--enable-native-access=ALL-UNNAMED",
                "--add-exports=java.base/jdk.internal.vm=ALL-UNNAMED",
                "--add-opens=java.base/java.lang=ALL-UNNAMED",
                "--add-opens=java.base/java.nio=ALL-UNNAMED",
                "--sun-misc-unsafe-memory-access=allow",
                "-cp",
                classPath,
                MatViewBootLoadMain.class.getName(),
                configuration.getDbRoot()
        ));
        command.addAll(List.of(viewNames));
        // the data directory of a server of this version, so that the child runs no table migrations
        final File upgradeFile = new File(configuration.getDbRoot(), TableUtils.UPGRADE_FILE_NAME);
        final boolean isUpgradeFileWritten = !upgradeFile.exists();
        if (isUpgradeFileWritten) {
            try (RandomAccessFile f = new RandomAccessFile(upgradeFile, "rw")) {
                f.write(ByteBuffer.allocate(2 * Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN)
                        .putInt(ColumnType.VERSION)
                        .putInt(ColumnType.MIGRATION_VERSION)
                        .array());
            }
        }
        final File outputFile = File.createTempFile("mat-view-boot-load", ".out");
        try {
            final Process process = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(outputFile).start();
            try {
                process.getOutputStream().close();
                if (!process.waitFor(120, TimeUnit.SECONDS)) {
                    Assert.fail("the boot load process timed out:\n" + java.nio.file.Files.readString(outputFile.toPath(), StandardCharsets.UTF_8));
                }
                final String output = java.nio.file.Files.readString(outputFile.toPath(), StandardCharsets.UTF_8);
                Assert.assertEquals(output, 0, process.exitValue());
                return output;
            } finally {
                if (process.isAlive()) {
                    process.destroyForcibly().onExit().join();
                }
            }
        } finally {
            Assert.assertTrue(outputFile.delete());
            if (isUpgradeFileWritten) {
                Assert.assertTrue(upgradeFile.delete());
            }
        }
    }

    private static String txnLogPath(TableToken viewToken) {
        return configuration.getDbRoot() + Files.SEPARATOR + viewToken.getDirName() + Files.SEPARATOR + WalUtils.SEQ_DIR
                + Files.SEPARATOR + WalUtils.TXNLOG_FILE_NAME;
    }

    private static MatViewDefinition viewDefinition(CairoEngine e) {
        return e.getDependentViewGraph().getViewDefinition(e.verifyTableName("price_1h"));
    }

    private static MatViewState viewState(CairoEngine e) {
        return e.getMatViewStateStore().getViewState(e.verifyTableName("price_1h"));
    }

    private static void writeLong(RandomAccessFile f, long offset, long value) throws IOException {
        f.seek(offset);
        f.write(ByteBuffer.allocate(Long.BYTES).order(ByteOrder.LITTLE_ENDIAN).putLong(value).array());
    }

    // Tears the record of the seqTxn, the last one, in the V1 transaction log of the view price_1h, as a power loss
    // before the flush of its last bytes completed does: its commit timestamp, written last, is zero, and the
    // flushed-through hint names it. The record's event stays intact.
    private static void tearTxnLogRecord(long seqTxn) throws IOException {
        final String path = txnLogPath(engine.verifyTableName("price_1h"));
        releaseAllForLogEdit();
        final long recordOffset = TableTransactionLogFile.HEADER_SIZE + (seqTxn - 1) * TableTransactionLogV1.RECORD_SIZE;
        try (RandomAccessFile f = new RandomAccessFile(path, "rw")) {
            Assert.assertEquals(seqTxn, readLong(f, TableTransactionLogFile.MAX_TXN_OFFSET_64));
            writeLong(f, recordOffset + TableTransactionLogFile.TX_LOG_COMMIT_TIMESTAMP_OFFSET, 0);
            writeLong(f, TableTransactionLogV1.UNSYNCED_FROM_TXN_OFFSET_64, seqTxn);
        }
    }

    // A restart of the view states: the store reloads them from disk, as at startup.
    private static void restartStates() {
        engine.getMatViewStateStore().clear();
        engine.hydrateMatViewStateStore();
    }

    // Arms the definition wait seam of the view price_1h to fail the statement that reaches it: a statement that must
    // not wait for a definition event fails instead of polling forever under the test's circuit breaker, which never
    // trips. The seam stays armed when no statement reaches it, so the caller disarms it.
    private void armNoWaitGuard() {
        viewState().setOnDefinitionWaitForTesting(() -> {
            throw new AssertionError("the statement waited for the pending definition event");
        });
    }

    // Arms the definition wait seam of the view price_1h to apply the WAL of every table, and returns the flag the seam
    // sets when it runs.
    private AtomicBoolean armSeamDrain() {
        final AtomicBoolean isWaitSeen = new AtomicBoolean();
        viewState().setOnDefinitionWaitForTesting(() -> {
            isWaitSeen.set(true);
            drainWalQueue();
        });
        return isWaitSeen;
    }

    // Fails ALTER MATERIALIZED VIEW ... AS (query) on the valid view with the aggregating timestamp rule, see
    // assertAlterRefused(), at the token of the query, or at the query's start when the token is null.
    private void assertAggregatingAlterRefused(String viewName, String query, String token) throws Exception {
        assertAggregatingAlterRefused(viewName, "ALTER MATERIALIZED VIEW " + viewName + " AS (", query, token);
    }

    // As assertAggregatingAlterRefused() above, for the statement that starts with head.
    private void assertAggregatingAlterRefused(String viewName, String head, String query, String token) throws Exception {
        final int tokenIndex = token != null ? query.indexOf(token) : 0;
        Assert.assertTrue(tokenIndex > -1);
        Assert.assertTrue("the token must be unique", token == null || query.lastIndexOf(token) == tokenIndex);
        assertAlterRefused(viewName, head + query + ')', head.length() + tokenIndex, AGGREGATING_TIMESTAMP_ERROR);
    }

    private void assertAlterFails(String tail, String token, String message) throws Exception {
        final String sql = ALTER_PREFIX + tail;
        final int position = token != null ? sql.indexOf(token) : sql.length();
        Assert.assertTrue(position > -1);
        assertExceptionNoLeakCheck(sql, position, message);
    }

    // Fails the statement twice, with the message at the position, and asserts that it left the view as it was: no
    // view transaction, so the same rows, the same SQL and base table, valid, and its latch free.
    private void assertAlterRefused(String viewName, String sql, int position, String message) throws Exception {
        assertAlterRefused(viewName, sql, position, message, false);
    }

    // As assertAlterRefused() above, on a view that is invalid, or valid, before and after the statement.
    private void assertAlterRefused(String viewName, String sql, int position, String message, boolean isInvalid) throws Exception {
        Assert.assertTrue(position > -1);
        final TableToken viewToken = engine.verifyTableName(viewName);
        final MatViewState state = engine.getMatViewStateStore().getViewState(viewToken);
        final long viewSeqTxn = engine.getTableSequencerAPI().getTxnTracker(viewToken).getSeqTxn();
        final MatViewDefinition definition = engine.getDependentViewGraph().getViewDefinition(viewToken);
        for (int i = 0; i < 2; i++) {
            try {
                execute(sql);
                Assert.fail("the statement must fail: " + sql);
            } catch (SqlException e) {
                TestUtils.assertEquals(message, e.getFlyweightMessage());
                Assert.assertEquals(position, e.getPosition());
            }
        }
        Assert.assertEquals(viewSeqTxn, engine.getTableSequencerAPI().getTxnTracker(viewToken).getSeqTxn());
        final MatViewDefinition definitionAfter = engine.getDependentViewGraph().getViewDefinition(viewToken);
        Assert.assertEquals(definition.getMatViewSql(), definitionAfter.getMatViewSql());
        Assert.assertEquals(definition.getBaseTableName(), definitionAfter.getBaseTableName());
        Assert.assertEquals(isInvalid, state.isInvalid());
        Assert.assertFalse(state.isLocked());
    }

    // Fails, with the refusal at the view name, each form of view ALTER that checks the view's SQL: ADD COLUMN, DROP
    // COLUMN, ALTER ... AS and ALTER ... WITH BASE ... AS, see assertAlterRefused(), on the valid view price_1h.
    private void assertDefinitionWaitRefused(String refusal) throws Exception {
        for (String tail : new String[]{
                "ADD COLUMN x CHAR",
                "DROP COLUMN price",
                "AS (" + VIEW_SQL + ")",
                "WITH BASE base2 AS (" + BASE2_SQL + ")"
        }) {
            assertAlterRefused("price_1h", ALTER_PREFIX + tail, VIEW_NAME_POSITION, refusal);
        }
    }

    private void assertMappingColumns(String unmapped, String ignored) throws Exception {
        assertMappingColumns("price_1h", unmapped, ignored);
    }

    private void assertMappingColumns(String viewName, String unmapped, String ignored) throws Exception {
        assertQuery("SELECT unmapped_view_columns, ignored_query_columns FROM materialized_views() WHERE view_name = '" + viewName + "'")
                .noLeakCheck()
                .noRandomAccess()
                .returns("unmapped_view_columns\tignored_query_columns\n" + unmapped + "\t" + ignored + "\n");
    }

    // The rows of a view with the mx and mn columns.
    private void assertMinMaxRows(String viewName, String expected) throws Exception {
        assertQuery("SELECT ts, mx, mn FROM " + viewName + " ORDER BY ts").timestamp("ts").noLeakCheck().expectSize().returns("ts\tmx\tmn\n" + expected);
    }

    // Fails ALTER MATERIALIZED VIEW ... AS (query) on the valid view with the passthrough timestamp rule, see
    // assertAlterRefused(), at the token of the query, or at the query's start when the token is null.
    private void assertPassthroughAlterRefused(String viewName, String query, String token) throws Exception {
        assertPassthroughAlterRefused(viewName, "ALTER MATERIALIZED VIEW " + viewName + " AS (", query, token, false);
    }

    // As assertPassthroughAlterRefused() above, for the statement that starts with head, on a view that is invalid, or
    // valid, before and after the statement.
    private void assertPassthroughAlterRefused(String viewName, String head, String query, String token, boolean isInvalid) throws Exception {
        final int tokenIndex = token != null ? query.indexOf(token) : 0;
        Assert.assertTrue(tokenIndex > -1);
        assertAlterRefused(viewName, head + query + ')', head.length() + tokenIndex, PASSTHROUGH_TIMESTAMP_ERROR, isInvalid);
    }

    // A base renamed under a valid view invalidates it; ALTER ... WITH BASE follows the renamed table, whose table id
    // is the old one, and plain RESUME still refuses with "base table changed", with or without backlog cache updates
    // before and after the base change.
    private void assertPlainResumeRefusedAfterFollowingRenamedBase(boolean isUpdatingCache) throws Exception {
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            final int baseId = engine.verifyTableName("base").getTableId();
            execute("RENAME TABLE base TO base_renamed");
            drainWalAndMatViewQueues();
            Assert.assertTrue(viewState().isInvalid());
            execute("INSERT INTO base_renamed VALUES ('c', 3.0, 1.0, '2024-01-01T02:10:00.000000Z')");
            drainWalAndMatViewQueues();
            if (isUpdatingCache) {
                updateBacklog();
            }
            execute(ALTER_PREFIX + "WITH BASE base_renamed AS (SELECT ts, sym, last(price) AS price FROM base_renamed SAMPLE BY 1h)");
            drainWalAndMatViewQueues();
            Assert.assertEquals(MatViewBacklog.LOST_BASE_TABLE_CHANGED, viewState().getBacklogLostReason());
            if (isUpdatingCache) {
                final long viewSeqTxn = viewSeqTxn();
                updateBacklog();
                // a lost backlog takes no cache update
                Assert.assertEquals(viewSeqTxn, viewSeqTxn());
                Assert.assertEquals(MatViewBacklog.LOST_BASE_TABLE_CHANGED, viewState().getBacklogLostReason());
            }
            assertViewStatus("invalid", "base table changed");
            assertResumeRefused("base table changed");
            execute(RESUME_SKIP_SQL);
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            Assert.assertEquals(baseId, viewState().getBaseTableId());
            assertViewRows(VIEW_ROWS);
            execute("INSERT INTO base_renamed VALUES ('d', 4.0, 1.0, '2024-01-01T03:10:00.000000Z')");
            drainWalAndMatViewQueues();
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    2024-01-01T01:00:00.000000Z\tb\t2.0
                    2024-01-01T03:00:00.000000Z\td\t4.0
                    """);
        });
    }

    // Fails an ALTER ... AS whose SQL copies by position, see assertAlterRefused(), with the query column at the view
    // column's position, at the token. No positional test drops a view column first: the view's table keeps a dropped
    // column's slot, which positional copy meets at refresh.
    private void assertPositionalAlterRefused(String viewName, String sql, String token, String viewColumn, String queryColumn) throws Exception {
        assertAlterRefused(
                viewName,
                sql,
                sql.indexOf(token),
                "materialized view column does not match the query column at the same position [view column=" + viewColumn
                        + ", query column=" + queryColumn + ']' + POSITIONAL_COPY_HINT
        );
    }

    private void assertRefreshBaseTxn(long expected) {
        Assert.assertEquals(expected, viewState().getLastRefreshBaseTxn());
    }

    private void assertResumeRefused(String cause) throws Exception {
        final long viewSeqTxn = viewSeqTxn();
        assertExceptionNoLeakCheck(
                RESUME_SQL,
                REFRESH_VIEW_NAME_POSITION,
                "materialized view backlog is lost [reason=" + cause + "]; RESUME cannot catch up on base changes made while"
                        + " the view was invalid. Run REFRESH MATERIALIZED VIEW price_1h RESUME SKIP BACKLOG, then REFRESH ... RANGE over the gap"
        );
        Assert.assertEquals(viewSeqTxn, viewSeqTxn());
        Assert.assertTrue(viewState().isInvalid());
        Assert.assertFalse(viewState().isLocked());
    }

    // The cairo.wal.apply.suspended.tables list stops the apply of the view's WAL without the suspended flag of its
    // tracker, and RESUME WAL does not lift it. A view ALTER that waits for an earlier ALTER ... AS whose definition
    // event the WAL of the listed view holds refuses at once and names the list entry: before ALTER ... SUSPEND WAL, under
    // it and after RESUME WAL. Under cairo.wal.apply.suspended.write.denied the refusal comes before the write refusal
    // of a listed table. Once the list no longer holds the view, its WAL applies the definition without RESUME WAL, and
    // ADD COLUMN checks the new SQL.
    private void assertViewAlterRefusedWhileViewListed(boolean isWriteDenied) throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_WRITE_DENIED, Boolean.toString(isWriteDenied));
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            createBase2WithRow();
            // the list does not hold the view yet, so a write denial does not refuse the statement
            execute(ALTER_PREFIX + "AS (" + X_SQL + ")");
            final String dirName = viewToken().getDirName();
            final String refusal = DEFINITION_NOT_APPLIED + "remove " + dirName
                    + " from cairo.wal.apply.suspended.tables and run ALTER MATERIALIZED VIEW price_1h RESUME WAL first";
            setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_TABLES, dirName);
            armNoWaitGuard();
            try {
                Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(viewToken()));
                assertDefinitionWaitRefused(refusal);
                if (!isWriteDenied) {
                    execute(ALTER_PREFIX + "SUSPEND WAL");
                    assertDefinitionWaitRefused(refusal);
                    execute(ALTER_PREFIX + "RESUME WAL");
                    drainWalQueue();
                    // the list still holds the view, so its WAL does not apply
                    Assert.assertEquals(VIEW_SQL, viewDefinition().getMatViewSql());
                    assertDefinitionWaitRefused(refusal);
                }
            } finally {
                viewState().setOnDefinitionWaitForTesting(null);
                setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_TABLES, null);
            }
            drainWalQueue();
            Assert.assertEquals(X_SQL, viewDefinition().getMatViewSql());
            final String addChar = ALTER_PREFIX + "ADD COLUMN x CHAR";
            assertAlterRefused("price_1h", addChar, addChar.indexOf("x CHAR"), "inconvertible types: DOUBLE -> CHAR [from=x, to=x]");
            execute(ALTER_PREFIX + "ADD COLUMN x DOUBLE");
            drainWalAndMatViewQueues();
            assertViewColumns("price_1h", "ts", "sym", "price", "x");
            assertViewStatus("valid", "");
        });
    }

    // A base change whose definition event the view's WAL has not applied, of a view whose base table was dropped, or
    // created again without WAL: the restart invalidates the view as before and loads the definition event as pending,
    // so an ALTER ... AS on the new base table waits for it, and checks itself against the new base table.
    private void assertRestartRecordsPendingBaseChangeOfViewWithoutWalBase(boolean isNonWalBase) throws Exception {
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            createBase2WithRow();
            execute("DROP TABLE base");
            if (isNonWalBase) {
                execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, amount DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            }
            drainWalAndMatViewQueues();
            execute(ALTER_PREFIX + "WITH BASE base2 AS (" + BASE2_SQL + ")");
            final long definitionSeqTxn = viewState().getPendingDefinitionSeqTxn();
            restartEngine((restarted, context) -> {
                Assert.assertEquals(
                        "invalidate " + (isNonWalBase ? "base table is not WAL table" : "base table does not exist") + '\n',
                        queuedViewTasks(restarted)
                );
                Assert.assertEquals(definitionSeqTxn, viewState(restarted).getPendingDefinitionSeqTxn());
                final AtomicBoolean isWaitSeen = armSeamDrain(restarted);
                final String newSql = "SELECT ts, sym, max(price) AS price FROM base2 SAMPLE BY 1h";
                restarted.execute(ALTER_PREFIX + "AS (" + newSql + ")", context);
                Assert.assertTrue(isWaitSeen.get());
                drainWalAndMatViewQueues(restarted);
                Assert.assertEquals("base2", viewDefinition(restarted).getBaseTableName());
                Assert.assertEquals(newSql, viewDefinition(restarted).getMatViewSql());
            });
        });
    }

    // MANUAL view, V1 log: ALTER ... AS writes its definition event as the last record n, and a power loss tears the
    // record before the flush of its last bytes completed: its commit timestamp, written last, is zero, and the
    // flushed-through hint names it, while its event stays intact. The sequencer's open drops record n and a later
    // commit takes its seqTxn, so the restart records no pending definition: a view ADD COLUMN and REFRESH ... FULL do
    // not wait, and the view keeps its SQL. Under ADAPTIVE the setup takes a save point, so the engine's recovery pass
    // opens the log, and drops record n, before the load.
    private void assertRestartRecordsNothingFromTornTxnLogTail(String commitMode) throws Exception {
        final boolean isAdaptive = "adaptive".equals(commitMode);
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, commitMode);
        setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 0);
        if (isAdaptive) {
            setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, "0");
            // a durable epoch for the setup
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 0);
        }
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("MANUAL");
            if (isAdaptive) {
                // the statement below applies lazily, above the setup's save point
                setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
            }
            execute(ALTER_PREFIX + "AS (" + X_SQL + ")");
            final long tornSeqTxn = viewState().getPendingDefinitionSeqTxn();
            Assert.assertEquals(tornSeqTxn, engine.getTableSequencerAPI().lastTxn(viewToken()));
            tearTxnLogRecord(tornSeqTxn);
            final String txnLogPath = txnLogPath(viewToken());
            restartEngine(restarted -> {
                if (isAdaptive) {
                    // the recovery pass of the engine's start opened the log, which dropped the torn record
                    try (RandomAccessFile f = new RandomAccessFile(txnLogPath, "r")) {
                        Assert.assertEquals(tornSeqTxn - 1, readLong(f, TableTransactionLogFile.MAX_TXN_OFFSET_64));
                    } catch (IOException e) {
                        throw new AssertionError(e);
                    }
                }
            }, (restarted, context) -> {
                final TableToken viewToken = restarted.verifyTableName("price_1h");
                Assert.assertEquals(-1, viewState(restarted).getPendingDefinitionSeqTxn());
                Assert.assertEquals(tornSeqTxn - 1, restarted.getTableSequencerAPI().lastTxn(viewToken));
                armNoWaitGuard(restarted);
                try {
                    restarted.execute(ALTER_PREFIX + "ADD COLUMN x CHAR", context);
                    // the commit takes the seqTxn of the torn record
                    Assert.assertEquals(tornSeqTxn, restarted.getTableSequencerAPI().lastTxn(viewToken));
                    restarted.execute("REFRESH MATERIALIZED VIEW price_1h FULL", context);
                } finally {
                    viewState(restarted).setOnDefinitionWaitForTesting(null);
                }
                drainWalAndMatViewQueues(restarted);
                Assert.assertEquals(VIEW_SQL, viewDefinition(restarted).getMatViewSql());
                assertQuery("SELECT ts, sym, price FROM price_1h ORDER BY ts, sym").withEngine(restarted).withContext(context).timestamp("ts").noLeakCheck().expectSize().returns(VIEW_ROWS);
                assertViewStatus(restarted, context, "valid", "");
            });
        });
    }

    private void assertViewStatus(CairoEngine e, SqlExecutionContext context, String status, String reason) throws Exception {
        assertQuery("SELECT view_status, invalidation_reason FROM materialized_views() WHERE view_name = 'price_1h'")
                .withEngine(e)
                .withContext(context)
                .noLeakCheck()
                .noRandomAccess()
                .returns("view_status\tinvalidation_reason\n" + status + "\t" + reason + "\n");
    }

    private void assertViewColumns(String viewName, String... expected) throws Exception {
        final StringBuilder sb = new StringBuilder("column\n");
        for (String column : expected) {
            sb.append(column).append('\n');
        }
        assertQuery("SELECT \"column\" FROM table_columns('" + viewName + "')").noLeakCheck().noRandomAccess().returns(sb.toString());
    }

    private void assertViewRows(String expected) throws Exception {
        assertQuery("price_1h ORDER BY ts, sym").timestamp("ts").noLeakCheck().expectSize().returns(expected);
    }

    private void assertViewStatus(String status, String reason) throws Exception {
        assertViewStatus("price_1h", status, reason);
    }

    private void assertViewStatus(String viewName, String status, String reason) throws Exception {
        assertQuery("SELECT view_status, invalidation_reason FROM materialized_views() WHERE view_name = '" + viewName + "'")
                .noLeakCheck()
                .noRandomAccess()
                .returns("view_status\tinvalidation_reason\n" + status + "\t" + reason + "\n");
    }

    private void createBase(String name) throws Exception {
        execute("CREATE TABLE " + name + " (sym SYMBOL, price DOUBLE, amount DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
    }

    // The view av over the base of createPassthroughBase(), which sums the price of each symbol by the hour of the base's
    // designated timestamp ts, see AGGREGATING_VIEW_ROWS, and the table other, with a row for each symbol of the tests.
    private void createAggregatingView() throws Exception {
        createPassthroughBase();
        execute("CREATE TABLE other (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("""
                INSERT INTO other VALUES
                ('2024-01-01T00:00:00.000000Z', 'a', 10.0),
                ('2024-01-01T00:00:00.000000Z', 'b', 20.0),
                ('2024-01-01T00:00:00.000000Z', 'c', 30.0)
                """);
        drainWalQueue();
        execute("CREATE MATERIALIZED VIEW av AS (SELECT ts, sym, sum(price) AS price FROM base SAMPLE BY 1h) PARTITION BY DAY");
        drainWalAndMatViewQueues();
        assertQuery("av").timestamp("ts").noLeakCheck().expectSize().returns(AGGREGATING_VIEW_ROWS);
    }

    // A second base table with one row, base txn 1.
    private void createBase2WithRow() throws Exception {
        createBase("base2");
        execute("INSERT INTO base2 VALUES ('c', 5.0, 1.0, '2024-01-02T00:10:00.000000Z')");
        drainWalAndMatViewQueues();
    }

    // A base with two rows and a view refreshed from them at base txn 1.
    private void createBaseWithRefreshedView(String refreshType) throws Exception {
        createBase("base");
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
        assertViewStatus("valid", "");
        assertRefreshBaseTxn(1);
    }

    // A base with one row, base txn 1, and the view price_1h over it, see createNeverRefreshedView().
    private void createBaseWithNeverRefreshedView() throws Exception {
        createBase("base");
        execute("INSERT INTO base VALUES ('a', 1.0, 1.0, '2024-01-01T00:10:00.000000Z')");
        drainWalQueue();
        createNeverRefreshedView("price_1h", VIEW_SQL);
    }

    // A view that has never refreshed: REFRESH MANUAL DEFERRED takes no refresh at creation, so the view has no state
    // file.
    private void createNeverRefreshedView(String viewName, String sql) throws Exception {
        execute("CREATE MATERIALIZED VIEW \"" + viewName + "\" REFRESH MANUAL DEFERRED AS (" + sql + ") PARTITION BY DAY");
        drainWalAndMatViewQueues();
        assertNoLiveViewState(engine.verifyTableName(viewName));
    }

    // The base of the positional copy tests: SYMBOL columns of 65, 65 and 64 characters, each holding one value, and
    // two rows in the first hour, whose price has the maximum 9.0 and the minimum 1.0.
    private void createLongNameBase(String name) throws Exception {
        execute("CREATE TABLE " + name + " (ts TIMESTAMP, " + L65 + " SYMBOL, " + M65 + " SYMBOL, " + L64
                + " SYMBOL, price DOUBLE, amount DOUBLE, ok BOOLEAN) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("INSERT INTO " + name + " VALUES " + """
                ('2024-01-01T00:10:00.000000Z', 'a', 'b', 'c', 1.0, 100.0, true),
                ('2024-01-01T00:20:00.000000Z', 'a', 'b', 'c', 9.0, 900.0, false)
                """);
        drainWalQueue();
    }

    // The base of the passthrough timestamp tests, whose ts2 is a TIMESTAMP column besides the designated timestamp
    // ts, with rows at 10:00 and 11:00, and the passthrough view pv that mirrors it, see PASSTHROUGH_VIEW_ROWS.
    private void createPassthroughBase() throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, ts2 TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("""
                INSERT INTO base VALUES
                ('2024-01-01T10:00:00.000000Z', '2024-01-01T10:30:00.000000Z', 'a', 1.0),
                ('2024-01-01T11:00:00.000000Z', '2024-01-01T10:45:00.000000Z', 'b', 2.0)
                """);
        drainWalQueue();
        execute("CREATE MATERIALIZED VIEW pv AS (SELECT ts, sym, price FROM base) PARTITION BY DAY");
        drainWalAndMatViewQueues();
        assertQuery("pv").timestamp("ts").noLeakCheck().expectSize().returns(PASSTHROUGH_VIEW_ROWS);
    }

    // Regular views over the base of createPassthroughBase(): rv names ts2 ts and takes it as its designated timestamp,
    // rv_ok passes ts through.
    private void createPassthroughRegularViews() throws Exception {
        execute("CREATE VIEW rv AS (SELECT ts2 AS ts, sym, price FROM base TIMESTAMP(ts2))");
        execute("CREATE VIEW rv_ok AS (SELECT ts, sym, price FROM base)");
    }

    // Two rows in the second hour of the base of createLongNameBase(), whose price has the maximum 6.0 and the minimum
    // 2.0, and the refreshes they trigger.
    private void insertSecondHour(String name) throws Exception {
        execute("INSERT INTO " + name + " VALUES " + """
                ('2024-01-01T01:10:00.000000Z', 'a', 'b', 'c', 2.0, 200.0, true),
                ('2024-01-01T01:20:00.000000Z', 'a', 'b', 'c', 6.0, 600.0, false)
                """);
        drainWalAndMatViewQueues();
    }

    private void invalidate(String reason) {
        engine.getMatViewStateStore().enqueueInvalidate(viewToken(), reason);
        drainWalAndMatViewQueues();
        Assert.assertTrue(viewState().isInvalid());
    }

    private String queryShowCreate(String viewName) throws Exception {
        final StringSink sink = new StringSink();
        printSql("SHOW CREATE MATERIALIZED VIEW " + viewName, sink);
        // drop the header line
        final String text = sink.toString();
        return text.substring(text.indexOf('\n') + 1);
    }

    private MatViewStateReader readCheckpointedState() {
        return readCheckpointedState(viewToken());
    }

    private MatViewStateReader readCheckpointedState(TableToken viewToken) {
        try (Path path = new Path(); BlockFileReader reader = new BlockFileReader(configuration)) {
            path.of(configuration.getCheckpointRoot()).concat(configuration.getDbDirectory()).concat(viewToken)
                    .concat(MatViewState.MAT_VIEW_STATE_FILE_NAME);
            reader.of(path.$());
            return new MatViewStateReader().of(reader, viewToken);
        }
    }

    private MatViewStateReader readLiveState(TableToken viewToken) {
        try (Path path = new Path(); BlockFileReader reader = new BlockFileReader(configuration)) {
            reader.of(path.of(configuration.getDbRoot()).concat(viewToken).concat(MatViewState.MAT_VIEW_STATE_FILE_NAME).$());
            return new MatViewStateReader().of(reader, viewToken);
        }
    }

    // Stops the engine and starts another one on the same data directory, which loads the views from disk.
    private void restartEngine(RestartedEngineCheck check) throws Exception {
        restartEngine(restarted -> {
        }, check);
    }

    // As restartEngine() above, running the action on the new engine before it loads the tables and views.
    private void restartEngine(Consumer<CairoEngine> beforeLoad, RestartedEngineCheck check) throws Exception {
        engine.releaseInactive();
        engine.releaseAllWriters();
        try (
                CairoEngine restarted = new CairoEngine(configuration);
                SqlExecutionContext restartedContext = TestUtils.createSqlExecutionCtx(restarted)
        ) {
            beforeLoad.accept(restarted);
            restarted.load();
            restarted.buildViewGraphs();
            check.run(restarted, restartedContext);
        }
    }

    // As restartEngine() above, with the flag set while the new engine loads the tables and views.
    private void restartEngineCountingBoot(AtomicBoolean isCounting, RestartedEngineCheck check) throws Exception {
        try {
            restartEngine(restarted -> isCounting.set(true), (restarted, context) -> {
                isCounting.set(false);
                check.run(restarted, context);
            });
        } finally {
            isCounting.set(false);
        }
    }

    // Restores the checkpoint the test took into the data directory, as a restart with the restore trigger does.
    private void restoreCheckpoint() {
        engine.clear();
        engine.closeNameRegistry();
        try (Path triggerPath = new Path()) {
            triggerPath.of(configuration.getDbRoot()).parent().concat(TableUtils.RESTORE_FROM_CHECKPOINT_TRIGGER_FILE_NAME);
            Assert.assertTrue(Files.touch(triggerPath.$()));
        }
        engine.checkpointRecover();
        engine.reloadTableNames();
        engine.getMetadataCache().onStartupAsyncHydrator();
        engine.buildViewGraphs();
    }

    // Runs a backlog cache update, as the refresh intervals update timer enqueues it.
    private void updateBacklog() {
        engine.getMatViewStateStore().enqueueUpdateRefreshIntervals(viewToken());
        drainWalAndMatViewQueues();
    }

    private MatViewDefinition viewDefinition() {
        return engine.getDependentViewGraph().getViewDefinition(viewToken());
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

    @FunctionalInterface
    private interface RestartedEngineCheck {
        void run(CairoEngine restarted, SqlExecutionContext context) throws Exception;
    }

    // An ALTER MATERIALIZED VIEW that applies while CHECKPOINT CREATE copies the view, see
    // testCheckpointDefinitionAppliedBeforeTableCopyOfNeverRefreshedView(), and the check of the definition that the
    // checkpoint holds after it.
    private record CheckpointAlterCase(
            String viewName,
            String alterTail,
            boolean isIncremental,
            Consumer<MatViewDefinition> definitionCheck
    ) {
        String describe() {
            return "view=" + viewName + ", alter=" + alterTail + ", incremental=" + isIncremental;
        }
    }
}
