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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.O3PartitionJob;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.mv.MatViewRefreshJob;
import io.questdb.cairo.mv.MatViewState;
import io.questdb.cairo.mv.MatViewTimerJob;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.cairo.wal.WalPurgeJob;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.griffin.DefaultSqlExecutionCircuitBreakerConfiguration;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.continuation.FiberRuntime;
import io.questdb.mp.continuation.FiberRuntimeState;
import io.questdb.mp.continuation.FiberTask;
import io.questdb.mp.continuation.LaunchResult;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The view latch protocol under concurrency: statement threads issue RESUME, RESUME SKIP BACKLOG, ALTER
 * MATERIALIZED VIEW ... AS with and without WITH BASE, REFRESH FULL, INCREMENTAL and RANGE, view ADD and DROP COLUMN,
 * view DROP PARTITION, SET and DROP EXPIRE of EXPIRE ROWS policies that hide no row, base TRUNCATE, DETACH and ATTACH, UPDATE, base column DDL, base drop and re-create, forced
 * invalidations and backlog updates at random, while an ingest thread inserts into the base tables, and free-running
 * refresh workers, a WAL apply worker, the timer job and the WAL purge job run on their own threads. The view is
 * IMMEDIATE, and each round picks microsecond or nanosecond timestamps.
 * <p>
 * A round picks at random whether the statement threads run their statements that take no lock of this test as fiber
 * tasks on one fiber runtime, which fewer carrier threads drain than there are statement threads: a statement that
 * held its carrier while it waits for the view latch, the view's WAL or a new definition would stall the others, and a
 * parked statement resumes on any carrier. Such a round first parks a RESUME on the view latch, which it holds until
 * the runtime's drain returns with the RESUME unfinished.
 * <p>
 * Every statement must return within its timeout with success or one of the errors this test lists, see
 * {@link #ALLOWED_ERRORS}. Once the threads stop and the queues drain, no view latch is held, no RESUME is in
 * progress, no FULL refresh is parked, no WAL is suspended, and the view, moved back to its first SQL and base table,
 * resumed and refreshed with a RANGE over the base's days, equals a twin view with the same SQL that a FULL refresh
 * rebuilds from the base.
 * <p>
 * The budget: questdb.fuzz.matview.concurrent.statements statements per statement thread, and
 * questdb.fuzz.matview.concurrent.rounds independent rounds.
 */
public class MatViewConcurrentOperationsFuzzTest extends AbstractCairoTest {
    // Errors a statement may return under the race, matched as substrings of the error message. Each is a refusal or
    // a missing object that the statement reports by design.
    private static final String[] ALLOWED_ERRORS = {
            // RESUME and ALTER ... WITH BASE: a latch holder that outlives the statement timeout, a pending FULL
            "materialized view is busy, retry later",
            "materialized view full refresh is pending",
            // RESUME: the view's SQL or base cannot take the view back, or the backlog is lost
            "materialized view SQL does not compile",
            "materialized view SQL does not match the view's columns",
            "materialized view backlog is lost",
            "base table does not exist",
            // INCREMENTAL and RANGE on an invalid view
            "materialized view is invalid, run REFRESH MATERIALIZED VIEW mv RESUME first",
            // ALTER ... AS: a SQL that reads another table than the view's base
            "materialized view query does not read its base table",
            // a statement that reads or writes a base column that a base column change dropped or renamed
            "Invalid column",
            // a statement on the base while a replacement drops and re-creates it: the table is gone, being dropped,
            // or its name is taken by the table being created
            "table does not exist",
            "table already exists",
            "table is dropped",
            "table name is reserved",
            // an INSERT whose WAL writer opens the base while a DROP TABLE removes its files: the _txn file, or the _cv
            // file, which the WAL writer opens right after it to configure the base's symbol tables
            "could not open txn file",
            "/_cv]",
            // view and base column DDL that another statement thread has already made, or made moot
            "already exists",
            "duplicate column",
            "cannot remove, column does not exist",
            "does not exist in table",
            "type is already",
    };
    private static final int DAY_COUNT = 4;
    private static final long DAY_LO = 1_704_067_200_000_000L; // 2024-01-01
    private static final long HANG_BOUND_MILLIS = TimeUnit.MINUTES.toMillis(5);
    private static final Log LOG = LogFactory.getLog(MatViewConcurrentOperationsFuzzTest.class);
    private static final long STATEMENT_TIMEOUT_MILLIS = 30_000;
    private static final String VIEW_COLUMNS = "ts, sym, price, qty";
    private static final String VIEW_SQL = "SELECT ts, sym, last(price) AS price, sum(qty) AS qty FROM base SAMPLE BY 1h";
    private static final String VIEW_SQL_BASE2 = "SELECT ts, sym, last(price) AS price, sum(qty) AS qty FROM base2 SAMPLE BY 1h";
    private static final String VIEW_SQL_MAX = "SELECT ts, sym, max(price) AS price, sum(qty) AS qty FROM base SAMPLE BY 1h";
    // Serializes the statements that change the base's columns or replace it with a DETACH and ATTACH pair and with
    // UPDATE.
    private final ReentrantLock baseLayoutLock = new ReentrantLock();
    // Keeps TRUNCATE of the base out of a DETACH and ATTACH pair, see questdb/questdb#8355. Taken after baseLayoutLock.
    private final ReentrantLock detachedPartitionLock = new ReentrantLock();
    // Whether the base table, since it was created, dropped a column or changed a column's type, which leaves a deleted
    // column whose name a live column may carry. Guarded by baseLayoutLock.
    private boolean hasDeletedBaseColumn;
    // The designated timestamp type of the round's base tables and views.
    private String timestampType;

    @Test
    public void testConcurrentViewAndBaseOperations() throws Exception {
        assertRounds(false);
    }

    @Test
    public void testConcurrentViewAndBaseOperationsOnFibers() throws Exception {
        assertRounds(true);
    }

    private static void appendDay(StringSink sink, int day) {
        sink.put('\'');
        MicrosFormatUtils.appendDateTimeUSec(sink, DAY_LO + day * Micros.DAY_MICROS);
        sink.put('\'');
    }

    private static void assertNotSuspended(String tableName) {
        final TableToken token = engine.getTableTokenIfExists(tableName);
        if (token != null) {
            Assert.assertFalse(tableName + " WAL is suspended", engine.getTableSequencerAPI().isSuspended(token));
        }
    }

    // Waits until the WAL apply worker applied the base's committed transactions, or suspended the base.
    private static void awaitBaseWalApplied() {
        final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(engine.verifyTableName("base"));
        final long seqTxn = tracker.getSeqTxn();
        final long deadline = System.currentTimeMillis() + HANG_BOUND_MILLIS;
        while (tracker.getWriterTxn() < seqTxn && !tracker.isSuspended()) {
            Assert.assertTrue("base WAL was not applied", System.currentTimeMillis() < deadline);
            Os.pause();
        }
    }

    private static void closeRuntime(FiberRuntime runtime) {
        runtime.beginQuiesce();
        final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(HANG_BOUND_MILLIS);
        while (runtime.state() != FiberRuntimeState.CLOSED && System.nanoTime() < deadline) {
            runtime.drain(8);
        }
        Assert.assertTrue(runtime.awaitClosed(deadline));
        runtime.closeAfterDrained();
    }

    private static void countError(CharSequence message, ConcurrentHashMap<String, AtomicInteger> errorCounts, ConcurrentHashMap<String, String> unexpectedErrors) {
        final String text = message.toString();
        errorCounts.computeIfAbsent(text, k -> new AtomicInteger()).incrementAndGet();
        if (!isAllowed(message)) {
            LOG.error().$("fuzz: unexpected error: ").$(text).$();
            unexpectedErrors.put(text, text);
        }
    }

    private static void drainAll() {
        for (int i = 0; i < 4; i++) {
            drainWalAndMatViewQueues();
        }
    }

    // Runs the statement on the calling thread, or as a fiber task on the round's runtime, which the carriers drain.
    private static void executeStatement(CharSequence sql, SqlExecutionContextImpl context, @Nullable FiberRuntime runtime) throws SqlException {
        if (runtime == null) {
            engine.execute(sql, context);
            return;
        }
        final StatementTask task = new StatementTask(sql, context);
        Assert.assertSame(LaunchResult.LAUNCHED, runtime.launch(task));
        task.awaitAndRethrow();
    }

    private static void insertRows(String tableName, Rnd rnd, int rowCount, SqlExecutionContextImpl context) throws SqlException {
        final StringSink sql = new StringSink();
        sql.put("INSERT INTO ").put(tableName).put(" (sym, price, qty, ts) VALUES ");
        for (int i = 0; i < rowCount; i++) {
            if (i > 0) {
                sql.put(", ");
            }
            sql.put("('s").put(rnd.nextInt(3)).put("', ").put(rnd.nextInt(1000)).put(".5, ").put(rnd.nextInt(100)).put(", '");
            MicrosFormatUtils.appendDateTimeUSec(sql, DAY_LO + rnd.nextLong(DAY_COUNT * Micros.DAY_MICROS));
            sql.put("')");
        }
        engine.execute(sql, context);
    }

    private static boolean isAllowed(CharSequence message) {
        for (String allowed : ALLOWED_ERRORS) {
            if (Chars.contains(message, allowed)) {
                return true;
            }
        }
        return false;
    }

    // Turns the detached partition of the base for the day into one that ATTACH PARTITION takes, and returns whether
    // the base had it detached.
    private static boolean makeAttachable(int day) {
        final TableToken baseToken = engine.verifyTableName("base");
        final StringSink partition = new StringSink();
        MicrosFormatUtils.appendDateTimeUSec(partition, DAY_LO + day * Micros.DAY_MICROS);
        partition.clear(10);
        try (Path path = new Path(); Path other = new Path()) {
            path.of(configuration.getDbRoot()).concat(baseToken).concat(partition).put(TableUtils.DETACHED_DIR_MARKER).$();
            if (!Files.exists(path.$())) {
                return false;
            }
            other.of(configuration.getDbRoot()).concat(baseToken).concat(partition).put(configuration.getAttachPartitionSuffix()).$();
            Assert.assertTrue(Files.rename(path.$(), other.$()) > -1);
            return true;
        }
    }

    private static NetworkSqlExecutionCircuitBreaker newCircuitBreaker() {
        return new NetworkSqlExecutionCircuitBreaker(engine, new DefaultSqlExecutionCircuitBreakerConfiguration() {
            @Override
            public int getCircuitBreakerThrottle() {
                return 0;
            }

            @Override
            public long getQueryTimeout() {
                return STATEMENT_TIMEOUT_MILLIS;
            }
        }) {
            @Override
            protected boolean testConnection(long fd) {
                return false;
            }
        };
    }

    // Rows in every day, at the first and the last microsecond of the days too, so that a RANGE over the days, which
    // reads from the base's minimum to its maximum timestamp, recomputes all of them, and the partition clip keeps no
    // view row of a day the base lacks.
    private static void seedDays(String tableName, Rnd rnd, SqlExecutionContextImpl context) throws SqlException {
        final StringSink sql = new StringSink();
        sql.put("INSERT INTO ").put(tableName).put(" (sym, price, qty, ts) VALUES ");
        for (int day = 0; day <= DAY_COUNT; day++) {
            final long ts = day == DAY_COUNT ? DAY_LO + DAY_COUNT * Micros.DAY_MICROS - 1 : DAY_LO + day * Micros.DAY_MICROS + (day == 0 ? 0 : rnd.nextLong(Micros.DAY_MICROS));
            if (day > 0) {
                sql.put(", ");
            }
            sql.put("('s").put(rnd.nextInt(3)).put("', ").put(rnd.nextInt(1000)).put(".5, ").put(rnd.nextInt(100)).put(", '");
            MicrosFormatUtils.appendDateTimeUSec(sql, ts);
            sql.put("')");
        }
        engine.execute(sql, context);
    }

    private static MatViewState viewState(String viewName) {
        return engine.getMatViewStateStore().getViewState(engine.verifyTableName(viewName));
    }

    private void assertRounds(boolean isFiberForced) throws Exception {
        final int rounds = Integer.getInteger("questdb.fuzz.matview.concurrent.rounds", 1);
        final int statements = Integer.getInteger("questdb.fuzz.matview.concurrent.statements", -1);
        // short WAL segments, so that the WAL purges find closed segments, and a short backlog update period, so that
        // the timer job updates the invalid view's backlog during the run
        setProperty(PropertyKey.CAIRO_WAL_SEGMENT_ROLLOVER_ROW_COUNT, 8);
        setProperty(PropertyKey.CAIRO_MAT_VIEW_REFRESH_INTERVALS_UPDATE_PERIOD, "10ms");
        assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);
            for (int round = 0; round < rounds; round++) {
                LOG.info().$("fuzz: round ").$(round).$();
                runRound(rnd, statements > 0 ? statements : 30 + rnd.nextInt(30), isFiberForced);
            }
        });
    }

    // Moves the view back to its first SQL and base table, makes it valid and backfills it with a RANGE over the base's
    // days, then asserts it against the twin, which FULL rebuilds from the base.
    private void assertViewMatchesTwin(Rnd rnd, SqlExecutionContextImpl context) throws Exception {
        repairBase(rnd, context);
        engine.execute("ALTER MATERIALIZED VIEW mv WITH BASE base AS (" + VIEW_SQL + ")", context);
        drainAll();
        final MatViewState state = viewState("mv");
        if (state.isInvalid()) {
            engine.execute("REFRESH MATERIALIZED VIEW mv RESUME SKIP BACKLOG", context);
            drainAll();
        }
        Assert.assertFalse("view is invalid: " + state.getInvalidationReasonForReport(), state.isInvalid());
        seedDays("base", rnd, context);
        drainAll();
        final StringSink sql = new StringSink();
        sql.put("REFRESH MATERIALIZED VIEW mv RANGE FROM ");
        appendDay(sql, 0);
        sql.put(" TO ");
        appendDay(sql, DAY_COUNT);
        engine.execute(sql, context);
        drainAll();
        Assert.assertFalse("view is invalid: " + state.getInvalidationReasonForReport(), state.isInvalid());

        engine.execute("REFRESH MATERIALIZED VIEW twin FULL", context);
        drainAll();
        Assert.assertFalse(viewState("twin").isInvalid());
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            TestUtils.assertSqlCursors(
                    compiler,
                    context,
                    "SELECT " + VIEW_COLUMNS + " FROM twin ORDER BY ts, sym",
                    "SELECT " + VIEW_COLUMNS + " FROM mv ORDER BY ts, sym",
                    LOG
            );
        }
    }

    private void createBase(String tableName, Rnd rnd, SqlExecutionContextImpl context) throws SqlException {
        engine.execute("CREATE TABLE " + tableName + " (sym SYMBOL, price DOUBLE, qty LONG, ts " + timestampType + ") TIMESTAMP(ts) PARTITION BY DAY WAL", context);
        seedDays(tableName, rnd, context);
    }

    // Parks a RESUME of the invalidated view on the view latch, which the test holds, before any carrier starts: the
    // drain on the test thread returns while RESUME waits. Once the test frees the latch, RESUME completes.
    private void parkFirstResume(FiberRuntime runtime) throws Exception {
        final TableToken viewToken = engine.verifyTableName("mv");
        engine.getMatViewStateStore().enqueueInvalidate(viewToken, "fuzz invalidation");
        drainAll();
        final MatViewState state = viewState("mv");
        Assert.assertTrue(state.isInvalid());
        Assert.assertTrue(state.tryLock());
        boolean isLatchHeld = true;
        try (
                SqlExecutionContextImpl context = TestUtils.createSqlExecutionCtx(engine, 1);
                NetworkSqlExecutionCircuitBreaker circuitBreaker = newCircuitBreaker()
        ) {
            context.with(circuitBreaker);
            circuitBreaker.resetTimer();
            final StatementTask task = new StatementTask("REFRESH MATERIALIZED VIEW mv RESUME", context);
            Assert.assertSame(LaunchResult.LAUNCHED, runtime.launch(task));
            Assert.assertEquals(1, runtime.drain(1));
            final int parkedCount = task.isDone() ? 0 : 1;
            isLatchHeld = false;
            MatViewRefreshJob.finalizeAndUnlock(engine, engine.getMatViewStateStore(), viewToken, state, false);
            final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(HANG_BOUND_MILLIS);
            while (!task.isDone() && System.nanoTime() < deadline) {
                runtime.drain(1);
                Os.pause();
            }
            Assert.assertEquals("the round's first RESUME did not park", 1, parkedCount);
            task.awaitAndRethrow();
            Assert.assertFalse(state.isInvalid());
        } finally {
            if (isLatchHeld) {
                MatViewRefreshJob.finalizeAndUnlock(engine, engine.getMatViewStateStore(), viewToken, state, false);
            }
        }
    }

    // The base's qty column as the view's SQL reads it: present, named qty, of type LONG.
    private void repairBase(Rnd rnd, SqlExecutionContextImpl context) throws SqlException {
        if (engine.getTableTokenIfExists("base") == null) {
            createBase("base", rnd, context);
            drainAll();
            return;
        }
        final int qtyType;
        final boolean hasQty2;
        try (TableReader reader = engine.getReader("base")) {
            final int qtyIndex = reader.getMetadata().getColumnIndexQuiet("qty");
            qtyType = qtyIndex > -1 ? reader.getMetadata().getColumnType(qtyIndex) : ColumnType.UNDEFINED;
            hasQty2 = reader.getMetadata().getColumnIndexQuiet("qty2") > -1;
        }
        if (qtyType == ColumnType.UNDEFINED) {
            engine.execute(hasQty2 ? "ALTER TABLE base RENAME COLUMN qty2 TO qty" : "ALTER TABLE base ADD COLUMN qty LONG", context);
        } else if (qtyType != ColumnType.LONG) {
            engine.execute("ALTER TABLE base ALTER COLUMN qty TYPE LONG", context);
        }
        drainAll();
    }

    private void runRound(Rnd rnd, int statementCount, boolean isFiberForced) throws Exception {
        timestampType = rnd.nextBoolean() ? "TIMESTAMP" : "TIMESTAMP_NS";
        LOG.info().$("fuzz: timestamp type ").$(timestampType).$();
        // The record copier the refreshes write rows with: picked by the schema's size (0), or forced to the
        // single-method, chunked or looping flavour. Production uses the looping copier for wide views when chunked
        // copiers are disabled or fail.
        final int copierType = rnd.nextInt(4);
        LOG.info().$("fuzz: copier type ").$(copierType).$();
        setProperty(PropertyKey.DEBUG_CAIRO_COPIER_TYPE, copierType);
        try (SqlExecutionContextImpl context = TestUtils.createSqlExecutionCtx(engine, 1)) {
            createBase("base", rnd, context);
            createBase("base2", rnd, context);
            engine.execute("CREATE MATERIALIZED VIEW mv AS (" + VIEW_SQL + ") PARTITION BY DAY", context);
            engine.execute("CREATE MATERIALIZED VIEW twin REFRESH MANUAL DEFERRED AS (" + VIEW_SQL + ") PARTITION BY DAY", context);
            drainAll();

            final AtomicBoolean isStopped = new AtomicBoolean();
            final AtomicInteger workerErrors = new AtomicInteger();
            final ConcurrentHashMap<String, AtomicInteger> errorCounts = new ConcurrentHashMap<>();
            final ConcurrentHashMap<String, String> unexpectedErrors = new ConcurrentHashMap<>();
            final ObjList<Thread> workers = new ObjList<>();
            final ObjList<Thread> clients = new ObjList<>();
            final int refreshWorkerCount = 1 + rnd.nextInt(3);
            final int statementThreadCount = 2 + rnd.nextInt(2);
            final boolean isFiberRound = rnd.nextBoolean() || isFiberForced;
            final int carrierCount = isFiberRound ? 1 + rnd.nextInt(statementThreadCount - 1) : 0;
            LOG.info().$("fuzz: statement threads ").$(statementThreadCount).$(", fiber carriers ").$(carrierCount).$();
            final FiberRuntime runtime = isFiberRound ? new FiberRuntime(statementThreadCount + 1) : null;
            final CyclicBarrier startBarrier = new CyclicBarrier(statementThreadCount + 1);
            try {
                if (runtime != null) {
                    Assert.assertTrue(carrierCount < statementThreadCount);
                    parkFirstResume(runtime);
                    for (int i = 0; i < carrierCount; i++) {
                        workers.add(startCarrier(i, runtime, isStopped, workerErrors));
                    }
                }
                for (int i = 0; i < refreshWorkerCount; i++) {
                    workers.add(startRefreshWorker(i, isStopped, workerErrors, rnd.nextLong(), rnd.nextLong()));
                }
                workers.add(startApplyWorker(isStopped, workerErrors, rnd.nextLong(), rnd.nextLong()));
                for (int i = 0; i < statementThreadCount; i++) {
                    clients.add(startStatementThread(i, statementCount, startBarrier, errorCounts, unexpectedErrors, runtime, rnd.nextLong(), rnd.nextLong()));
                }
                clients.add(startIngestThread(statementCount, startBarrier, errorCounts, unexpectedErrors, rnd.nextLong(), rnd.nextLong()));
                for (int i = 0, n = clients.size(); i < n; i++) {
                    final Thread client = clients.getQuick(i);
                    client.join(HANG_BOUND_MILLIS);
                    Assert.assertFalse("statement thread did not finish: " + client.getName(), client.isAlive());
                }
            } finally {
                isStopped.set(true);
                for (int i = 0, n = clients.size(); i < n; i++) {
                    clients.getQuick(i).join(HANG_BOUND_MILLIS);
                }
                for (int i = 0, n = workers.size(); i < n; i++) {
                    workers.getQuick(i).join();
                }
                if (runtime != null) {
                    closeRuntime(runtime);
                }
            }

            final StringSink errorReport = new StringSink();
            for (Map.Entry<String, AtomicInteger> e : new TreeMap<>(errorCounts).entrySet()) {
                errorReport.put('\n').put(e.getValue().get()).put(" x ").put(e.getKey());
            }
            LOG.info().$("fuzz: statement errors").$(errorReport).$();
            Assert.assertEquals("unexpected statement errors: " + unexpectedErrors.values(), 0, unexpectedErrors.size());
            Assert.assertEquals("a worker failed, see the log", 0, workerErrors.get());

            drainAll();
            final MatViewState state = viewState("mv");
            Assert.assertFalse("view latch is held", state.isLocked());
            Assert.assertFalse("view is resuming", state.isResuming());
            Assert.assertFalse("a FULL refresh is parked", state.hasPendingFullRefreshOwnerForTesting());
            assertNotSuspended("base");
            assertNotSuspended("base2");
            assertNotSuspended("mv");
            assertNotSuspended("twin");

            assertViewMatchesTwin(rnd, context);

            engine.execute("DROP MATERIALIZED VIEW mv", context);
            engine.execute("DROP MATERIALIZED VIEW twin", context);
            engine.execute("DROP TABLE base", context);
            engine.execute("DROP TABLE base2", context);
            drainAll();
        }
    }

    // Runs one statement or action, weighted towards the statements that take or wait for the view latch.
    private void runStatement(Rnd rnd, SqlExecutionContextImpl context, StringSink sql, @Nullable FiberRuntime runtime) throws SqlException {
        sql.clear();
        switch (rnd.nextInt(28)) {
            case 0, 1, 2 -> sql.put("REFRESH MATERIALIZED VIEW mv RESUME");
            case 3, 4 -> sql.put("REFRESH MATERIALIZED VIEW mv RESUME SKIP BACKLOG");
            case 5, 6 -> sql.put("ALTER MATERIALIZED VIEW mv AS (").put(rnd.nextBoolean() ? VIEW_SQL : VIEW_SQL_MAX).put(')');
            case 7, 8 -> {
                if (rnd.nextBoolean()) {
                    sql.put("ALTER MATERIALIZED VIEW mv WITH BASE base2 AS (").put(VIEW_SQL_BASE2).put(')');
                } else {
                    sql.put("ALTER MATERIALIZED VIEW mv WITH BASE base AS (").put(VIEW_SQL).put(')');
                }
            }
            case 9 -> sql.put("REFRESH MATERIALIZED VIEW mv FULL");
            case 10 -> sql.put("REFRESH MATERIALIZED VIEW mv INCREMENTAL");
            case 11, 12 -> {
                final int day = rnd.nextInt(DAY_COUNT);
                sql.put("REFRESH MATERIALIZED VIEW mv RANGE FROM ");
                appendDay(sql, day);
                sql.put(" TO ");
                appendDay(sql, day + 1 + rnd.nextInt(DAY_COUNT - day));
            }
            case 13, 14 -> sql.put(rnd.nextBoolean() ? "ALTER MATERIALIZED VIEW mv ADD COLUMN extra DOUBLE" : "ALTER MATERIALIZED VIEW mv DROP COLUMN extra");
            case 15 -> {
                sql.put(rnd.nextBoolean() ? "ALTER MATERIALIZED VIEW mv DROP PARTITION LIST " : "ALTER MATERIALIZED VIEW mv DROP PARTITION WHERE ts < ");
                appendDay(sql, rnd.nextInt(DAY_COUNT));
            }
            case 16 -> {
                // Skips the sequence that questdb/questdb#8355 breaks until master fixes it: ATTACH PARTITION of a
                // partition that DETACH PARTITION produced before a TRUNCATE reads a base column added after the
                // partition was written as if the partition held it, and every refresh that reads the partition then
                // fails. So TRUNCATE does not run between a DETACH and its ATTACH. Remove the lock with the fix.
                sql.put("TRUNCATE TABLE base");
                detachedPartitionLock.lock();
                try {
                    LOG.info().$("fuzz: ").$(sql).$();
                    engine.execute(sql, context);
                } finally {
                    detachedPartitionLock.unlock();
                }
                return;
            }
            case 17, 18 -> {
                // DETACH and ATTACH of one day, with no base column change in between: ATTACH checks the partition's
                // columns against the table's, and a mismatch suspends the base table's WAL
                final int day = rnd.nextInt(DAY_COUNT);
                baseLayoutLock.lock();
                try {
                    if (engine.getTableTokenIfExists("base") == null) {
                        return;
                    }
                    // Skips the sequence that questdb/questdb#8258 breaks until master fixes it: ATTACH PARTITION of a
                    // partition that DETACH PARTITION produced fails, and suspends the base table's WAL, once a live
                    // column carries the name of a deleted one, as after ALTER COLUMN TYPE or DROP COLUMN followed by
                    // ADD COLUMN. Remove the skip with the fix.
                    if (hasDeletedBaseColumn) {
                        LOG.info().$("fuzz: skipping DETACH and ATTACH PARTITION after a base column was deleted, see questdb/questdb#8258").$();
                        return;
                    }
                    detachedPartitionLock.lock();
                    try {
                        sql.put("ALTER TABLE base DETACH PARTITION LIST ");
                        appendDay(sql, day);
                        LOG.info().$("fuzz: ").$(sql).$();
                        engine.execute(sql, context);
                        awaitBaseWalApplied();
                        if (!makeAttachable(day)) {
                            return;
                        }
                        sql.clear();
                        sql.put("ALTER TABLE base ATTACH PARTITION LIST ");
                        appendDay(sql, day);
                        LOG.info().$("fuzz: ").$(sql).$();
                        engine.execute(sql, context);
                    } finally {
                        detachedPartitionLock.unlock();
                    }
                } finally {
                    baseLayoutLock.unlock();
                }
                return;
            }
            case 19 -> {
                final int day = rnd.nextInt(DAY_COUNT);
                sql.put("UPDATE base SET qty = qty + 1 WHERE ts >= ");
                appendDay(sql, day);
                sql.put(" AND ts < ");
                appendDay(sql, day + 1);
                // Skips the race that questdb/questdb#8351 breaks until master fixes it: an UPDATE that took its WAL
                // writer before a concurrent DROP or RENAME COLUMN of qty commits still reaches the base's WAL after
                // it, and its apply then fails and suspends the base table's WAL, so base column changes do not race
                // it. Remove the serialization with the fix.
                baseLayoutLock.lock();
                try {
                    LOG.info().$("fuzz: ").$(sql).$();
                    engine.execute(sql, context);
                } finally {
                    baseLayoutLock.unlock();
                }
                return;
            }
            case 20, 21 -> {
                switch (rnd.nextInt(6)) {
                    case 0 -> sql.put("ALTER TABLE base DROP COLUMN qty");
                    case 1 -> sql.put("ALTER TABLE base ADD COLUMN qty LONG");
                    case 2 -> sql.put("ALTER TABLE base RENAME COLUMN qty TO qty2");
                    case 3 -> sql.put("ALTER TABLE base RENAME COLUMN qty2 TO qty");
                    case 4 -> sql.put("ALTER TABLE base ALTER COLUMN qty TYPE INT");
                    default -> sql.put("ALTER TABLE base ALTER COLUMN qty TYPE LONG");
                }
                baseLayoutLock.lock();
                try {
                    LOG.info().$("fuzz: ").$(sql).$();
                    engine.execute(sql, context);
                    hasDeletedBaseColumn |= Chars.contains(sql, "DROP COLUMN") || Chars.contains(sql, " TYPE ");
                } finally {
                    baseLayoutLock.unlock();
                }
                return;
            }
            case 22 -> {
                LOG.info().$("fuzz: replacing base table").$();
                baseLayoutLock.lock();
                try {
                    try {
                        engine.execute("DROP TABLE base", context);
                    } catch (SqlException | CairoException e) {
                        if (!Chars.contains(e.getFlyweightMessage(), "does not exist")) {
                            throw e;
                        }
                    }
                    createBase("base", rnd, context);
                    hasDeletedBaseColumn = false;
                } finally {
                    baseLayoutLock.unlock();
                }
                return;
            }
            case 23 -> {
                engine.getMatViewStateStore().enqueueInvalidate(engine.verifyTableName("mv"), "fuzz invalidation");
                return;
            }
            case 24 -> {
                engine.getMatViewStateStore().enqueueUpdateRefreshIntervals(engine.verifyTableName("mv"));
                return;
            }
            // An EXPIRE ROWS policy that hides no row and reads only columns no statement drops from the view, so every
            // read of the view, the final comparison's included, goes through the policy's rewrite while the twin
            // stays its oracle. A policy that reads extra would race its view DROP COLUMN, which design section 11
            // rule 8 lists as a known limitation (questdb-enterprise#1479, #1480).
            case 25 -> sql.put("ALTER MATERIALIZED VIEW mv SET EXPIRE ROWS ").put(switch (rnd.nextInt(5)) {
                case 0 -> "WHEN price < -1";
                case 1 -> "WHEN qty < min(qty) OVER ()";
                case 2 -> "KEEP HIGHEST ON price PARTITION BY ts, sym";
                case 3 -> "KEEP 2 LOWEST ON qty PARTITION BY ts, sym";
                default -> "KEEP LATEST PARTITION BY sym, ts, price";
            });
            case 26 -> sql.put("ALTER MATERIALIZED VIEW mv DROP EXPIRE");
            default -> {
                // a base table that a replacement dropped and then failed to re-create comes back
                if (engine.getTableTokenIfExists("base") == null) {
                    baseLayoutLock.lock();
                    try {
                        if (engine.getTableTokenIfExists("base") == null) {
                            createBase("base", rnd, context);
                            hasDeletedBaseColumn = false;
                        }
                    } finally {
                        baseLayoutLock.unlock();
                    }
                    return;
                }
                sql.put("REFRESH MATERIALIZED VIEW mv RESUME");
            }
        }
        LOG.info().$("fuzz: ").$(sql).$();
        executeStatement(sql, context, runtime);
    }

    private Thread startApplyWorker(AtomicBoolean isStopped, AtomicInteger workerErrors, long seed0, long seed1) {
        final Thread th = new Thread(() -> {
            final Rnd rnd = new Rnd(seed0, seed1);
            final MatViewTimerJob timerJob = new MatViewTimerJob(engine);
            try (
                    ApplyWal2TableJob walApplyJob = createWalApplyJob();
                    WalPurgeJob purgeJob = new WalPurgeJob(engine)
            ) {
                while (!isStopped.get()) {
                    drainWalQueue(walApplyJob, engine);
                    timerJob.run();
                    if (rnd.nextInt(10) == 0) {
                        purgeJob.drain(0);
                    }
                    Os.sleep(rnd.nextInt(5));
                }
                drainWalQueue(walApplyJob, engine);
            } catch (Throwable th1) {
                LOG.error().$("WAL apply worker failed: ").$(th1).$();
                workerErrors.incrementAndGet();
            } finally {
                Path.clearThreadLocals();
                Misc.free(O3PartitionJob.THREAD_LOCAL_CLEANER);
            }
        }, "wal-apply-worker");
        th.start();
        return th;
    }

    private Thread startCarrier(int carrierId, FiberRuntime runtime, AtomicBoolean isStopped, AtomicInteger workerErrors) {
        final Thread th = new Thread(() -> {
            runtime.initializeCarrier();
            try {
                while (!isStopped.get()) {
                    if (runtime.drain(8) == 0) {
                        Os.pause();
                    }
                }
            } catch (Throwable th1) {
                LOG.error().$("fiber carrier failed: ").$(th1).$();
                workerErrors.incrementAndGet();
            } finally {
                Path.clearThreadLocals();
                Misc.free(O3PartitionJob.THREAD_LOCAL_CLEANER);
            }
        }, "fiber-carrier-" + carrierId);
        th.start();
        return th;
    }

    private Thread startIngestThread(
            int statementCount,
            CyclicBarrier startBarrier,
            ConcurrentHashMap<String, AtomicInteger> errorCounts,
            ConcurrentHashMap<String, String> unexpectedErrors,
            long seed0,
            long seed1
    ) {
        final Thread th = new Thread(() -> {
            final Rnd rnd = new Rnd(seed0, seed1);
            try (SqlExecutionContextImpl context = TestUtils.createSqlExecutionCtx(engine, 1)) {
                startBarrier.await();
                for (int i = 0; i < statementCount * 2; i++) {
                    try {
                        insertRows(rnd.nextInt(5) == 0 ? "base2" : "base", rnd, 1 + rnd.nextInt(5), context);
                    } catch (SqlException | CairoException e) {
                        countError(e.getFlyweightMessage(), errorCounts, unexpectedErrors);
                    }
                    Os.sleep(rnd.nextInt(3));
                }
            } catch (Throwable th1) {
                LOG.error().$("ingest thread failed: ").$(th1).$();
                unexpectedErrors.put("ingest: " + th1, th1.toString());
            } finally {
                Path.clearThreadLocals();
                Misc.free(O3PartitionJob.THREAD_LOCAL_CLEANER);
            }
        }, "ingest");
        th.start();
        return th;
    }

    private Thread startRefreshWorker(int workerId, AtomicBoolean isStopped, AtomicInteger workerErrors, long seed0, long seed1) {
        final Thread th = new Thread(() -> {
            final Rnd rnd = new Rnd(seed0, seed1);
            try (MatViewRefreshJob refreshJob = new MatViewRefreshJob(workerId, engine, 0)) {
                while (!isStopped.get()) {
                    if (!refreshJob.run()) {
                        Os.sleep(rnd.nextInt(5));
                    }
                }
            } catch (Throwable th1) {
                LOG.error().$("refresh worker failed: ").$(th1).$();
                workerErrors.incrementAndGet();
            } finally {
                Path.clearThreadLocals();
                Misc.free(O3PartitionJob.THREAD_LOCAL_CLEANER);
            }
        }, "refresh-worker-" + workerId);
        th.start();
        return th;
    }

    private Thread startStatementThread(
            int threadId,
            int statementCount,
            CyclicBarrier startBarrier,
            ConcurrentHashMap<String, AtomicInteger> errorCounts,
            ConcurrentHashMap<String, String> unexpectedErrors,
            @Nullable FiberRuntime runtime,
            long seed0,
            long seed1
    ) {
        final Thread th = new Thread(() -> {
            final Rnd rnd = new Rnd(seed0, seed1);
            final StringSink sql = new StringSink();
            try (
                    SqlExecutionContextImpl context = TestUtils.createSqlExecutionCtx(engine, 1);
                    NetworkSqlExecutionCircuitBreaker circuitBreaker = newCircuitBreaker()
            ) {
                context.with(circuitBreaker);
                startBarrier.await();
                for (int i = 0; i < statementCount; i++) {
                    circuitBreaker.resetTimer();
                    try {
                        runStatement(rnd, context, sql, runtime);
                    } catch (SqlException | CairoException e) {
                        countError(e.getFlyweightMessage(), errorCounts, unexpectedErrors);
                    }
                    Os.sleep(rnd.nextInt(3));
                }
            } catch (Throwable th1) {
                LOG.error().$("statement thread failed [sql=").$(sql).$(", ex=").$(th1).I$();
                unexpectedErrors.put("statement " + threadId + ": " + th1, sql.toString());
            } finally {
                Path.clearThreadLocals();
                Misc.free(O3PartitionJob.THREAD_LOCAL_CLEANER);
            }
        }, "statement-" + threadId);
        th.start();
        return th;
    }

    // A statement that a statement thread runs on the round's fiber runtime; the thread waits for its outcome.
    private static final class StatementTask extends FiberTask {
        private final SqlExecutionContextImpl context;
        private final CountDownLatch doneLatch = new CountDownLatch(1);
        private final String sql;
        private volatile Throwable error;

        private StatementTask(CharSequence sql, SqlExecutionContextImpl context) {
            this.context = context;
            this.sql = sql.toString();
        }

        @Override
        protected boolean runStep() {
            try {
                engine.execute(sql, context);
            } catch (Throwable th) {
                error = th;
            } finally {
                doneLatch.countDown();
            }
            return true;
        }

        private void awaitAndRethrow() throws SqlException {
            try {
                Assert.assertTrue("fiber statement did not finish: " + sql, doneLatch.await(HANG_BOUND_MILLIS, TimeUnit.MILLISECONDS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
            final Throwable th = error;
            if (th instanceof SqlException e) {
                throw e;
            }
            if (th instanceof CairoException e) {
                throw e;
            }
            if (th != null) {
                throw new AssertionError("fiber statement failed: " + sql, th);
            }
        }
    }
}
