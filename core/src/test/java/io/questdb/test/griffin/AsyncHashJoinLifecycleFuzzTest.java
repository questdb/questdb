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

package io.questdb.test.griffin;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.cairo.sql.BindVariableService;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.griffin.engine.table.AsyncHashJoinRecordCursorFactory;
import io.questdb.griffin.engine.table.HashJoinTupleRing;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPoolMode;
import io.questdb.std.Chars;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.griffin.fuzz.CancelAfterChecksCircuitBreaker;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

import static io.questdb.test.griffin.ParallelHashJoinTestUtils.*;

/**
 * Compiled factories of the parallel hash join through a random sequence of executions and of
 * changes to their tables, on thread workers and on fiber workers, the way a query cache uses a
 * factory: the query fuzzer's tables stay as they are once it has built them, and the tests of the
 * join's lifecycle walk fixed scripts.
 * <p>
 * A run keeps one factory per query: joins that never swap their inputs, joins and stacks that
 * swap them when the left table is the smaller, stages that walk their matches under an ON residual
 * and a post-join filter, a stolen probe filter over a SYMBOL key, a pushed-down build ON filter,
 * a backward probe, and LIMITs that reach the join from a sub-query, from the end, and from a bind
 * variable that each execution sets. An execution reads every row, or stops part of the way and
 * closes, counts the rest after some rows, rewinds, is cancelled after some rows or at one of its
 * circuit breaker's checks, or runs under a per-query memory limit. Between executions, the tables
 * gain rows in order and out of order, lose them to TRUNCATE and DROP PARTITION, convert
 * partitions to Parquet and back, and gain columns, which makes the factories stale.
 * <p>
 * Every execution takes its builds, its orientation, its probe frames, its window, its tuple layout
 * and its SYMBOL key translation from the tables as it finds them, so its rows are those of the
 * ordinary plan
 * compiled then, in order, and once its cursor closes, whatever ended it, the join holds no slot,
 * no ring memory and no reader. The first executions of a run follow a script, so that every run
 * swaps its inputs, reads Parquet probe frames, joins an empty build, cancels, breaches a memory
 * limit and compiles a stale query again, whatever its seeds; the rest are drawn.
 * {@code -Dquestdb.fuzz.join.lifecycle.executions=N} sets how many, 200 by default, and
 * {@code -Dfuzz.s0} and {@code -Dfuzz.s1} replay a run's seeds.
 */
public class AsyncHashJoinLifecycleFuzzTest extends AbstractCairoTest {
    // The query whose LIMIT reads the bind variable $1, which each of its executions sets.
    private static final int BIND_LIMIT_QUERY = 9;
    private static final int DEFAULT_EXECUTIONS = 200;
    // The rows that a bulk insert adds to d, whose keys meet no row of t: more than t ever holds, so
    // that d is the larger table until it is truncated, and the joins of two table scans swap.
    private static final int D_BULK_ROWS = 2_500;
    // The most rows of d whose keys meet rows of t, past which an insert truncates d instead.
    private static final int D_MATCHING_MAX_ROWS = 300;
    // The most columns that d gains.
    private static final int D_MAX_ADDED_COLUMNS = 6;
    // The rows that the seed of d inserts, see MUTATION_D_SEED.
    private static final int D_SEED_ROWS = 150;
    // The executions of a run after its scripted ones.
    private static final String EXECUTIONS_PROP = "questdb.fuzz.join.lifecycle.executions";
    private static final int E_MAX_ROWS = 60;
    private static final int HOUR_DROPPED = 2;
    private static final int HOUR_NATIVE = 0;
    private static final int HOUR_PARQUET = 1;
    private static final Log LOG = LogFactory.getLog(AsyncHashJoinLifecycleFuzzTest.class);
    // The hourly partitions of t's first day that the run converts, drops and inserts into out of
    // order: all but the last one that the seed rows fill, which is the active one until rows
    // follow it.
    private static final int MODEL_HOURS = 7;
    private static final int MUTATION_D_ADD_COLUMN = 0;
    private static final int MUTATION_D_INSERT_BULK = 1;
    private static final int MUTATION_D_INSERT_MATCHING = 2;
    private static final int MUTATION_D_SEED = 3;
    private static final int MUTATION_D_TRUNCATE = 4;
    private static final int MUTATION_E_INSERT = 5;
    private static final int MUTATION_E_TRUNCATE = 6;
    private static final int MUTATION_KINDS = 12;
    private static final int MUTATION_T_APPEND = 7;
    private static final int MUTATION_T_DROP_PARTITION = 8;
    private static final int MUTATION_T_INSERT_OUT_OF_ORDER = 9;
    private static final int MUTATION_T_TO_NATIVE = 10;
    private static final int MUTATION_T_TO_PARQUET = 11;
    private static final int OP_CANCEL_AFTER_CHECKS = 0;
    private static final int OP_CANCEL_AFTER_ROWS = 1;
    private static final int OP_COUNT_AFTER_ROWS = 2;
    private static final int OP_COUNT_THEN_READ = 3;
    private static final int OP_KINDS = 8;
    private static final int OP_MEMORY_LIMIT = 4;
    private static final String[] OP_NAMES = {
            "cancel after some checks",
            "cancel after some rows",
            "count after some rows",
            "count, then read",
            "memory limit",
            "read",
            "read part of the way",
            "toTop()",
    };
    private static final int OP_READ = 5;
    private static final int OP_READ_PART = 6;
    private static final int OP_TO_TOP = 7;
    private static final String[] QUERIES = {
            // A LEFT join, which never swaps, with SYMBOL, STRING and VARCHAR columns of both inputs.
            "SELECT t.k, t.v, t.sym, t.vc, d.w, d.s, d.str FROM t LEFT JOIN d ON t.k = d.k",
            // An INNER join of two table scans, which swaps them when t is the smaller.
            "SELECT t.k, t.v, d.w, d.str FROM t JOIN d ON t.k = d.k",
            // A stack over such a join, whose later stage translates a SYMBOL key.
            "SELECT t.v, t.sym, d.w, e.ev FROM t JOIN d ON t.k = d.k JOIN e ON t.sym = e.sym",
            // Stages that walk their matches: an ON residual over both inputs, and a post-join filter.
            "SELECT t.v, d.w, e.ev FROM t LEFT JOIN d ON t.k = d.k AND d.w > t.v % 7 LEFT JOIN e ON t.sym = e.sym WHERE e.ev IS NULL OR e.ev % 3 <> 0",
            // A probe filter that the join steals, over a SYMBOL key.
            "SELECT t.v, t.sym, d.w, d.s FROM t JOIN d ON t.sym = d.s WHERE t.v % 3 = 0",
            // A LIMIT that reaches a stack through a sub-query, and one that counts from the end.
            "SELECT * FROM (SELECT t.v, d.w, e.ev FROM t LEFT JOIN d ON t.k = d.k LEFT JOIN e ON t.k = e.k) LIMIT 37",
            "SELECT t.v, d.w, e.ev FROM t LEFT JOIN d ON t.k = d.k LEFT JOIN e ON t.k = e.k LIMIT -11",
            // An ON residual over the build alone, which the build runs over its rows, and an interval.
            "SELECT t.v, t.vc, d.w, d.str FROM t LEFT JOIN d ON t.sym = d.s AND d.str LIKE 'i%' WHERE t.ts > '2024-01-01T03'",
            // A backward probe, whose order rules the swap out.
            "SELECT t.ts, t.v, d.w FROM t JOIN d ON t.k = d.k ORDER BY t.ts DESC",
            // A LIMIT that each execution binds.
            "SELECT t.v, d.w FROM t LEFT JOIN d ON t.k = d.k LIMIT $1",
    };
    // The most rows that the run adds to t's seed rows, past which an insert starts t over.
    private static final int T_MAX_ADDED_ROWS = 900;
    private static final int WORKER_COUNT = 4;

    @Test
    public void testLifecycleOnFiberWorkers() throws Exception {
        run(WorkerPoolMode.FIBER_HOST);
    }

    @Test
    public void testLifecycleOnThreadWorkers() throws Exception {
        run(WorkerPoolMode.LEGACY);
    }

    private static void run(WorkerPoolMode mode) throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        // What an execution's shape follows: the tuples of a task, the reduce queue, the ring's
        // budget, the builds' rounds, the Parquet row groups, the frames, the first task's rows and
        // the stepper warm-up, which a run reaches in the middle of an execution when it is short.
        final int taskOutputMaxRows = 1 + rnd.nextInt(64);
        final int queueCapacity = 4 << rnd.nextInt(3);
        final long ringMaxSize = rnd.nextBoolean() ? 192L << rnd.nextInt(8) : -1;
        final boolean isBuiltInRounds = rnd.nextBoolean();
        final int rowGroupSize = 16 << rnd.nextInt(3);
        final int frameRows = 8 << rnd.nextInt(5);
        final long firstTaskRows = 1 + rnd.nextInt(100);
        final long stepperWarmUpRows = rnd.nextInt(3) == 0 ? 0 : rnd.nextInt(20_000);
        // The bits of a narrow tuple slot: the server's 32, or a few, which the tables outgrow and
        // shrink back under, so that the executions of a factory switch between the layouts, and
        // the check of each narrowed value fails the run on a wrong choice.
        final int narrowSlotBits = rnd.nextBoolean() ? Integer.SIZE : 8 + rnd.nextInt(8);
        final int executions = Integer.getInteger(EXECUTIONS_PROP, DEFAULT_EXECUTIONS);
        LOG.info().$("lifecycle fuzz [mode=").$(mode.name())
                .$(", executions=").$(executions)
                .$(", taskOutputMaxRows=").$(taskOutputMaxRows)
                .$(", queueCapacity=").$(queueCapacity)
                .$(", ringMaxSize=").$(ringMaxSize)
                .$(", isBuiltInRounds=").$(isBuiltInRounds)
                .$(", rowGroupSize=").$(rowGroupSize)
                .$(", frameRows=").$(frameRows)
                .$(", firstTaskRows=").$(firstTaskRows)
                .$(", stepperWarmUpRows=").$(stepperWarmUpRows)
                .$(", narrowSlotBits=").$(narrowSlotBits)
                .I$();
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, taskOutputMaxRows);
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_REDUCE_QUEUE_CAPACITY, queueCapacity);
        if (ringMaxSize > -1) {
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TUPLE_RING_MAX_SIZE, ringMaxSize);
        }
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, isBuiltInRounds ? 0 : Long.MAX_VALUE);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_ROWS_PER_PARTITION, 16);
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, rowGroupSize);
        AsyncHashJoinRecordCursorFactory.setFirstTaskRowsForTesting(firstTaskRows);
        AsyncHashJoinRecordCursorFactory.setStepperWarmUpRowsForTesting(stepperWarmUpRows);
        HashJoinTupleRing.setNarrowSlotBitsForTesting(narrowSlotBits);
        try {
            final BindVariableService bindVariableService = new BindVariableServiceImpl(configuration);
            assertMemoryLeak(() -> executeOnWorkers(mode, WORKER_COUNT, bindVariableService, configuration, LOG, (db, context) -> {
                context.changePageFrameSizes(frameRows, frameRows);
                context.setParallelHashJoinEnabled(true);
                final Lifecycle lifecycle = new Lifecycle(db, context, bindVariableService, rnd, mode);
                try {
                    lifecycle.createTables();
                    lifecycle.runScript();
                    for (int i = 0; i < executions; i++) {
                        if (rnd.nextInt(4) == 0) {
                            lifecycle.mutate(rnd.nextInt(MUTATION_KINDS));
                        }
                        lifecycle.execute(rnd.nextInt(QUERIES.length), rnd.nextInt(OP_KINDS));
                    }
                    lifecycle.readEveryFactory();
                    lifecycle.logCounts();
                } finally {
                    lifecycle.close();
                }
            }));
        } finally {
            AsyncHashJoinRecordCursorFactory.setFirstTaskRowsForTesting(AsyncHashJoinRecordCursorFactory.FIRST_TASK_ROWS);
            HashJoinTupleRing.setNarrowSlotBitsForTesting(Integer.SIZE);
            restoreSteppers();
        }
    }

    // One run's tables, as the run changed them, its factories and what its executions met.
    private static final class Lifecycle {
        private final BindVariableService bindVariableService;
        private final SqlExecutionContextImpl context;
        // The breaker of the context, which an execution that cancels replaces while it runs.
        private final SqlExecutionCircuitBreaker contextBreaker;
        private final CairoEngine db;
        // Each query's rows under the ordinary plan, null once the tables changed.
        private final ObjList<ObjList<String>> expected = new ObjList<>();
        // Each query's factory, null until the query first runs.
        private final ObjList<RecordCursorFactory> factories = new ObjList<>();
        // The state of each hourly partition of t's first day that the run changes.
        private final int[] hourStates = new int[MODEL_HOURS];
        private final WorkerPoolMode mode;
        private final Rnd rnd;
        private final StringSink sink = new StringSink();
        private int cancelledExecutions;
        private int dAddedColumns;
        private int dMatchingRows;
        private int eRows;
        private int executionCount;
        private boolean hasDBulkRows;
        private int limitedExecutions;
        // The executions that joined into narrow tuple slots, and into wide ones.
        private int narrowExecutions;
        // The timestamp of the next row that an insert appends to t, in microseconds: the day after
        // the seed rows' at first.
        private long nextTimestamp;
        private int parquetExecutions;
        private int staleFactories;
        private int swappedExecutions;
        private int tAddedRows;
        private int wideExecutions;

        private Lifecycle(CairoEngine db, SqlExecutionContextImpl context, BindVariableService bindVariableService, Rnd rnd, WorkerPoolMode mode) {
            this.db = db;
            this.context = context;
            this.contextBreaker = context.getCircuitBreaker();
            this.bindVariableService = bindVariableService;
            this.rnd = rnd;
            this.mode = mode;
            expected.setPos(QUERIES.length);
            factories.setPos(QUERIES.length);
        }

        // Once an execution's cursor closed, whatever ended it, its joins hold nothing. A stale
        // factory whose new compile failed, under a memory limit or a cancellation, is gone.
        private void assertReleased(int query, String label) {
            final RecordCursorFactory factory = factories.getQuick(query);
            final ObjList<AsyncHashJoinRecordCursorFactory> joins = factory != null ? findAll(factory, context) : new ObjList<>();
            Assert.assertTrue(label, factory == null || joins.size() > 0);
            for (int i = 0, n = joins.size(); i < n; i++) {
                final AsyncHashJoinRecordCursorFactory join = joins.getQuick(i);
                Assert.assertEquals(label, 0, join.getAtom().getRing().getAllocatedBytes());
                Assert.assertEquals(label, 0, join.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
                Assert.assertEquals(label, 0, join.getBuildRoundAtom().getPerWorkerLocks().getAcquiredSlotCount());
            }
            Assert.assertEquals(label, 0, db.getBusyReaderCount());
        }

        // Cancels the query before it returns its first row: the join checks its breaker before it
        // builds, so the cancellation always lands.
        private void cancelBeforeTheFirstRow(int query) throws Exception {
            final String label = label(query, "cancel before the first row");
            final ObjList<String> rows = expectedRows(query);
            final AtomicBooleanCircuitBreaker breaker = new AtomicBooleanCircuitBreaker(db, 0);
            context.with(breaker);
            try (RecordCursor cursor = openCursor(query, label)) {
                breaker.cancel();
                cursor.hasNext();
                Assert.fail(label + ": the cancelled query returned");
            } catch (CairoException e) {
                Assert.assertTrue(label + ": " + e.getFlyweightMessage(), e.isCancellation());
                cancelledExecutions++;
            } finally {
                context.with(contextBreaker);
            }
            assertReleased(query, label);
            readAll(query, rows, label + ", then read");
        }

        private void close() {
            Misc.freeObjList(factories);
        }

        private void compile(int query, String label) throws SqlException {
            final RecordCursorFactory factory = db.select(QUERIES[query], context);
            factories.setQuick(query, factory);
            Assert.assertNotNull(label, find(factory));
        }

        private void createTables() throws Exception {
            db.execute("CREATE TABLE t (k INT, v LONG, sym SYMBOL, vc VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR BYPASS WAL", context);
            db.execute("CREATE TABLE d (k INT, w LONG, s SYMBOL, str STRING)", context);
            db.execute("CREATE TABLE e (sym SYMBOL, k INT, ev LONG)", context);
            // The LIMIT's bind variable is a LONG from the first compile on.
            bindVariableService.clear();
            bindVariableService.setLong(0, 5);
            seedT();
            mutate(MUTATION_D_SEED);
            mutate(MUTATION_E_INSERT);
        }

        // Runs one execution of the query's factory, which compiles on first use and again once it
        // is stale, and compares what it returns with the ordinary plan's rows.
        private void execute(int query, int op) throws Exception {
            executionCount++;
            if (query == BIND_LIMIT_QUERY) {
                // A LIMIT of some rows, of none, of more rows than the join has, or from the end.
                bindVariableService.setLong(0, switch (rnd.nextInt(4)) {
                    case 0 -> 0;
                    case 1 -> 1_000_000;
                    case 2 -> -(1 + rnd.nextInt(20));
                    default -> 1 + rnd.nextInt(50);
                });
            }
            final ObjList<String> rows = expectedRows(query);
            final int rowCount = rows.size();
            final String label = label(query, OP_NAMES[op]);
            // The rows that the execution reads before it does what the op names.
            final int readFirst = rnd.nextInt(rowCount + 1);
            final CancelAfterChecksCircuitBreaker checksBreaker = op == OP_CANCEL_AFTER_CHECKS
                    ? new CancelAfterChecksCircuitBreaker(db, (1L << rnd.nextInt(12)) - 1)
                    : null;
            final AtomicBooleanCircuitBreaker rowsBreaker = op == OP_CANCEL_AFTER_ROWS ? new AtomicBooleanCircuitBreaker(db, 0) : null;
            if (checksBreaker != null) {
                context.with(checksBreaker);
            } else if (rowsBreaker != null) {
                context.with(rowsBreaker);
            }
            if (op == OP_MEMORY_LIMIT) {
                setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 256L << rnd.nextInt(16));
            }
            boolean isFailed = false;
            try (RecordCursor cursor = openCursor(query, label)) {
                final RecordMetadata metadata = factories.getQuick(query).getMetadata();
                switch (op) {
                    case OP_CANCEL_AFTER_CHECKS, OP_MEMORY_LIMIT, OP_READ ->
                            readToEnd(cursor, metadata, rows, 0, label);
                    case OP_CANCEL_AFTER_ROWS -> {
                        read(cursor, metadata, rows, 0, readFirst, label);
                        rowsBreaker.cancel();
                        readToEnd(cursor, metadata, rows, readFirst, label + ", cancelled after " + readFirst + " rows");
                    }
                    case OP_COUNT_AFTER_ROWS -> {
                        read(cursor, metadata, rows, 0, readFirst, label);
                        final RecordCursor.Counter counter = new RecordCursor.Counter();
                        cursor.calculateSize(context.getCircuitBreaker(), counter);
                        Assert.assertEquals(label + ", count after " + readFirst + " rows", rowCount - readFirst, counter.get());
                        Assert.assertFalse(label, cursor.hasNext());
                        cursor.toTop();
                        readToEnd(cursor, metadata, rows, 0, label + ", after a count and toTop()");
                    }
                    case OP_COUNT_THEN_READ -> {
                        final RecordCursor.Counter counter = new RecordCursor.Counter();
                        cursor.calculateSize(context.getCircuitBreaker(), counter);
                        Assert.assertEquals(label + ", count", rowCount, counter.get());
                        cursor.toTop();
                        read(cursor, metadata, rows, 0, readFirst, label + ", after a count");
                    }
                    case OP_READ_PART -> read(cursor, metadata, rows, 0, readFirst, label);
                    case OP_TO_TOP -> {
                        read(cursor, metadata, rows, 0, readFirst, label);
                        cursor.toTop();
                        read(cursor, metadata, rows, 0, rnd.nextInt(rowCount + 1), label + ", after toTop()");
                        cursor.toTop();
                        readToEnd(cursor, metadata, rows, 0, label + ", after a second toTop()");
                    }
                    default -> throw new AssertionError(op);
                }
                final AsyncHashJoinRecordCursorFactory join = find(factories.getQuick(query));
                if (join.getAtom().isSwapped()) {
                    swappedExecutions++;
                }
                if (join.getAtom().getParquetProbeFrameCount() > 0) {
                    parquetExecutions++;
                }
                // An execution that built and bound its ring picked a tuple layout.
                final HashJoinTupleRing ring = join.getAtom().getRing();
                if (ring.getTupleBytes() > 0) {
                    if (ring.isNarrow()) {
                        narrowExecutions++;
                    } else {
                        wideExecutions++;
                    }
                }
            } catch (CairoException e) {
                // Only a cancelled execution may report a cancellation, and only one under a memory
                // limit a breach of it; either may also return every row.
                isFailed = true;
                if (op == OP_CANCEL_AFTER_CHECKS || op == OP_CANCEL_AFTER_ROWS) {
                    if (!e.isCancellation()) {
                        throw new AssertionError(label, e);
                    }
                    cancelledExecutions++;
                } else if (op == OP_MEMORY_LIMIT) {
                    if (!e.isOutOfMemory()) {
                        throw new AssertionError(label, e);
                    }
                    limitedExecutions++;
                } else {
                    throw new AssertionError(label, e);
                }
            } finally {
                context.with(contextBreaker);
                if (op == OP_MEMORY_LIMIT) {
                    setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 0);
                }
            }
            assertReleased(query, label);
            if (isFailed) {
                // The factory reads its rows right after the execution that failed.
                readAll(query, rows, label + ", then read");
            }
        }

        // The query's rows under the ordinary plan, one line each, over the tables as they stand.
        private ObjList<String> expectedRows(int query) throws Exception {
            // The bind variable's value changes the rows of the query that reads it.
            if (expected.getQuick(query) == null || query == BIND_LIMIT_QUERY) {
                final String text = ordinaryRows(QUERIES[query], context);
                final ObjList<String> rows = new ObjList<>();
                for (int lo = 0, n = text.length(); lo < n; ) {
                    final int hi = text.indexOf('\n', lo);
                    rows.add(text.substring(lo, hi));
                    lo = hi + 1;
                }
                expected.setQuick(query, rows);
            }
            return expected.getQuick(query);
        }

        // Fails the query under a memory limit that the join's first tracked allocations breach.
        private void failUnderMemoryLimit(int query, long limit) throws Exception {
            final String label = label(query, "memory limit of " + limit + " bytes");
            final ObjList<String> rows = expectedRows(query);
            setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, limit);
            try (RecordCursor cursor = openCursor(query, label)) {
                cursor.hasNext();
                Assert.fail(label + ": the query fit the limit");
            } catch (CairoException e) {
                Assert.assertTrue(label + ": " + e.getFlyweightMessage(), e.isOutOfMemory());
                limitedExecutions++;
            } finally {
                setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 0);
            }
            assertReleased(query, label);
            readAll(query, rows, label + ", then read");
        }

        private String hour(int hour) {
            return "2024-01-01T0" + hour;
        }

        private void hourToParquet(int hour) throws Exception {
            Assert.assertEquals(HOUR_NATIVE, hourStates[hour]);
            db.execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '" + hour(hour) + "'", context);
            hourStates[hour] = HOUR_PARQUET;
            expected.setAll(QUERIES.length, null);
        }

        private String label(int query, String what) {
            return "execution " + executionCount + " [" + mode.name() + "], " + what + ": " + QUERIES[query];
        }

        private void logCounts() {
            LOG.info().$("lifecycle fuzz done [mode=").$(mode.name())
                    .$(", executions=").$(executionCount)
                    .$(", swapped=").$(swappedExecutions)
                    .$(", overParquetProbeFrames=").$(parquetExecutions)
                    .$(", cancelled=").$(cancelledExecutions)
                    .$(", overMemoryLimit=").$(limitedExecutions)
                    .$(", staleFactories=").$(staleFactories)
                    .$(", narrow=").$(narrowExecutions)
                    .$(", wide=").$(wideExecutions)
                    .I$();
        }

        // Changes the tables, or does nothing when they do not allow the change: an hour that is
        // gone converts to nothing. Whatever it does, the queries' rows are to compute again.
        private void mutate(int mutation) throws Exception {
            switch (mutation) {
                case MUTATION_D_ADD_COLUMN -> {
                    if (dAddedColumns < D_MAX_ADDED_COLUMNS) {
                        db.execute("ALTER TABLE d ADD COLUMN c" + dAddedColumns++ + " INT", context);
                    }
                }
                case MUTATION_D_INSERT_BULK -> {
                    if (!hasDBulkRows) {
                        db.execute("INSERT INTO d (k, w, s, str) SELECT (1_000 + x % 50)::INT, x, ('z' || (x % 5))::SYMBOL, 'bulk' FROM long_sequence(" + D_BULK_ROWS + ")", context);
                        hasDBulkRows = true;
                    }
                }
                case MUTATION_D_INSERT_MATCHING -> {
                    final int rows = 1 + rnd.nextInt(40);
                    if (dMatchingRows + rows > D_MATCHING_MAX_ROWS) {
                        truncateD();
                    } else {
                        db.execute("INSERT INTO d (k, w, s, str) SELECT (x % 60)::INT, 10_000 + x, ('s' || (x % 20))::SYMBOL, 'ins' || x FROM long_sequence(" + rows + ")", context);
                        dMatchingRows += rows;
                    }
                }
                case MUTATION_D_SEED -> {
                    if (dMatchingRows + D_SEED_ROWS > D_MATCHING_MAX_ROWS) {
                        truncateD();
                    }
                    // D_SEED_ROWS rows: three of most keys, NULL keys, and NULL and empty strings.
                    db.execute("""
                            INSERT INTO d (k, w, s, str)
                            SELECT
                                CASE WHEN x % 17 = 0 THEN NULL ELSE (x % 50)::INT END,
                                x,
                                ('s' || (x % 12))::SYMBOL,
                                CASE WHEN x % 4 = 0 THEN 'i' || x WHEN x % 7 = 0 THEN NULL WHEN x % 9 = 0 THEN '' ELSE 'b' || x END
                            FROM long_sequence(150)
                            """, context);
                    dMatchingRows += D_SEED_ROWS;
                }
                case MUTATION_D_TRUNCATE -> truncateD();
                case MUTATION_E_INSERT -> {
                    final int rows = 1 + rnd.nextInt(30);
                    if (eRows + rows > E_MAX_ROWS) {
                        truncateE();
                    } else {
                        db.execute("INSERT INTO e (sym, k, ev) SELECT ('s' || (x % 7))::SYMBOL, (x % 30)::INT, x FROM long_sequence(" + rows + ")", context);
                        eRows += rows;
                    }
                }
                case MUTATION_E_TRUNCATE -> truncateE();
                case MUTATION_T_APPEND -> {
                    final int rows = 1 + rnd.nextInt(200);
                    if (tAddedRows + rows > T_MAX_ADDED_ROWS) {
                        // t starts over, with every partition native.
                        db.execute("TRUNCATE TABLE t", context);
                        seedT();
                    } else {
                        // New symbols, and rows of new partitions.
                        db.execute("INSERT INTO t (k, v, sym, vc, ts) SELECT (x % 45)::INT, 100_000 + x, ('n' || (x % 5))::SYMBOL, ('a' || x)::VARCHAR, "
                                + "timestamp_sequence(" + nextTimestamp + "::TIMESTAMP, 20_000_000) FROM long_sequence(" + rows + ")", context);
                        nextTimestamp += rows * 20_000_000L;
                        tAddedRows += rows;
                    }
                }
                case MUTATION_T_DROP_PARTITION -> {
                    final int hour = rnd.nextInt(MODEL_HOURS);
                    if (hourStates[hour] != HOUR_DROPPED) {
                        db.execute("ALTER TABLE t DROP PARTITION LIST '" + hour(hour) + "'", context);
                        hourStates[hour] = HOUR_DROPPED;
                    }
                }
                case MUTATION_T_INSERT_OUT_OF_ORDER -> {
                    // Into a native partition of the first day, or one that the run dropped, which
                    // the rows bring back.
                    final int hour = rnd.nextInt(MODEL_HOURS);
                    final int rows = 1 + rnd.nextInt(50);
                    if (hourStates[hour] != HOUR_PARQUET && tAddedRows + rows <= T_MAX_ADDED_ROWS) {
                        db.execute("INSERT INTO t (k, v, sym, vc, ts) SELECT (x % 45)::INT, 200_000 + x, ('o' || (x % 3))::SYMBOL, NULL, "
                                + "timestamp_sequence('" + hour(hour) + ":00:01', 7_000_000) FROM long_sequence(" + rows + ")", context);
                        hourStates[hour] = HOUR_NATIVE;
                        tAddedRows += rows;
                    }
                }
                case MUTATION_T_TO_NATIVE -> {
                    final int hour = rnd.nextInt(MODEL_HOURS);
                    if (hourStates[hour] == HOUR_PARQUET) {
                        db.execute("ALTER TABLE t CONVERT PARTITION TO NATIVE LIST '" + hour(hour) + "'", context);
                        hourStates[hour] = HOUR_NATIVE;
                    }
                }
                case MUTATION_T_TO_PARQUET -> {
                    final int hour = rnd.nextInt(MODEL_HOURS);
                    if (hourStates[hour] == HOUR_NATIVE) {
                        db.execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '" + hour(hour) + "'", context);
                        hourStates[hour] = HOUR_PARQUET;
                    }
                }
                default -> throw new AssertionError(mutation);
            }
            expected.setAll(QUERIES.length, null);
        }

        // Opens a cursor of the query's factory, as a server opens a cached query: it compiles the
        // query on first use, and again when the factory is stale. One open in four comes under the
        // rows that a client reads first.
        private RecordCursor openCursor(int query, String label) throws SqlException {
            if (factories.getQuick(query) == null) {
                compile(query, label);
            }
            context.setFirstReadRows(switch (rnd.nextInt(8)) {
                case 0 -> 1;
                case 1 -> 1 + rnd.nextInt(1_000);
                default -> Long.MAX_VALUE;
            });
            try {
                try {
                    return factories.getQuick(query).getCursor(context);
                } catch (TableReferenceOutOfDateException e) {
                    staleFactories++;
                    factories.setQuick(query, Misc.free(factories.getQuick(query)));
                    compile(query, label);
                    return factories.getQuick(query).getCursor(context);
                }
            } finally {
                context.setFirstReadRows(Long.MAX_VALUE);
            }
        }

        // Reads rows of the cursor, which stands before the row of the given index, and returns the
        // index of the row that follows them.
        private int read(RecordCursor cursor, RecordMetadata metadata, ObjList<String> rows, int from, int count, String label) {
            final int to = (int) Math.min(rows.size(), (long) from + count);
            for (int i = from; i < to; i++) {
                Assert.assertTrue(label + ": the cursor ended at row " + i + " of " + rows.size(), cursor.hasNext());
                sink.clear();
                CursorPrinter.println(cursor.getRecord(), metadata, sink, true);
                // Without the line's end.
                sink.clear(sink.length() - 1);
                if (!Chars.equals(rows.getQuick(i), sink)) {
                    Assert.fail(label + ": row " + i + "\nexpected: " + rows.getQuick(i) + "\nactual:   " + sink);
                }
            }
            return to;
        }

        // Reads every row of the query's factory.
        private void readAll(int query, ObjList<String> rows, String label) throws SqlException {
            try (RecordCursor cursor = openCursor(query, label)) {
                readToEnd(cursor, factories.getQuick(query).getMetadata(), rows, 0, label);
            }
            assertReleased(query, label);
        }

        // Every factory that the run compiled still reads its query's rows.
        private void readEveryFactory() throws Exception {
            for (int query = 0; query < QUERIES.length; query++) {
                if (factories.getQuick(query) != null) {
                    executionCount++;
                    readAll(query, expectedRows(query), label(query, "last read"));
                }
            }
        }

        private void readToEnd(RecordCursor cursor, RecordMetadata metadata, ObjList<String> rows, int from, String label) {
            Assert.assertEquals(label, rows.size(), read(cursor, metadata, rows, from, Integer.MAX_VALUE, label));
            Assert.assertFalse(label + ": a row past the last one", cursor.hasNext());
        }

        // The first executions of every run, whatever its seeds: each query in the query's
        // orientation, a cancellation and a breach of the memory limit that always land, joins that
        // swap their inputs, Parquet probe frames, stale factories and an empty build.
        private void runScript() throws Exception {
            for (int query = 0; query < QUERIES.length; query++) {
                execute(query, OP_READ);
            }
            Assert.assertEquals(0, swappedExecutions);
            cancelBeforeTheFirstRow(3);
            failUnderMemoryLimit(0, 256);
            // d outgrows t: the joins of the two table scans, and the stack over one, swap.
            mutate(MUTATION_D_INSERT_BULK);
            execute(1, OP_READ);
            execute(2, OP_READ);
            Assert.assertEquals(2, swappedExecutions);
            // The swapped stack again, part of the way, cancelled and counted.
            cancelBeforeTheFirstRow(2);
            execute(2, OP_READ_PART);
            execute(2, OP_COUNT_AFTER_ROWS);
            // Two of t's partitions convert, which the LEFT join's probe then reads.
            hourToParquet(0);
            hourToParquet(3);
            Assert.assertEquals(0, parquetExecutions);
            execute(0, OP_READ);
            Assert.assertEquals(1, parquetExecutions);
            execute(4, OP_TO_TOP);
            execute(7, OP_COUNT_AFTER_ROWS);
            // A new column of d leaves every factory stale, as the first Parquet partition of t left
            // those whose scans of t push a filter down.
            final int staleBefore = staleFactories;
            mutate(MUTATION_D_ADD_COLUMN);
            execute(1, OP_READ);
            execute(3, OP_READ_PART);
            Assert.assertEquals(staleBefore + 2, staleFactories);
            // An empty build: the INNER joins return no row, and the LEFT join every row of t.
            mutate(MUTATION_D_TRUNCATE);
            Assert.assertEquals(0, expectedRows(1).size());
            execute(1, OP_READ);
            execute(2, OP_COUNT_THEN_READ);
            execute(0, OP_READ);
            mutate(MUTATION_D_SEED);
            Assert.assertTrue(cancelledExecutions >= 2);
            Assert.assertTrue(limitedExecutions >= 1);
        }

        // The 900 seed rows of t, half a minute apart from the start of 2024-01-01: seven hourly
        // partitions and half of an eighth, with NULL keys and NULL and empty VARCHAR values.
        private void seedT() throws Exception {
            db.execute("""
                    INSERT INTO t (k, v, sym, vc, ts)
                    SELECT
                        CASE WHEN x % 13 = 0 THEN NULL ELSE (x % 40)::INT END,
                        x,
                        ('s' || (x % 9))::SYMBOL,
                        CASE WHEN x % 11 = 0 THEN NULL WHEN x % 10 = 0 THEN '' ELSE 'v' || (x * 7_919 % 1_000) END::VARCHAR,
                        timestamp_sequence('2024-01-01', 30_000_000)
                    FROM long_sequence(900)
                    """, context);
            Arrays.fill(hourStates, HOUR_NATIVE);
            // 2024-01-02T00:00:00.000000Z
            nextTimestamp = 1_704_153_600_000_000L;
            tAddedRows = 0;
        }

        private void truncateD() throws Exception {
            db.execute("TRUNCATE TABLE d", context);
            dMatchingRows = 0;
            hasDBulkRows = false;
        }

        private void truncateE() throws Exception {
            db.execute("TRUNCATE TABLE e", context);
            eRows = 0;
        }
    }
}
