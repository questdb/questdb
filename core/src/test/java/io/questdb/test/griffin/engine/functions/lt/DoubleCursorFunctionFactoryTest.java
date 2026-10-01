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

package io.questdb.test.griffin.engine.functions.lt;

import io.questdb.PropertyKey;
import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoConfigurationWrapper;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.StatefulAtom;
import io.questdb.cairo.sql.async.UnorderedPageFrameSequence;
import io.questdb.cairo.vm.MemoryCARWImpl;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.groupby.CountLongConstGroupByFunction;
import io.questdb.griffin.engine.functions.test.TestCloseCounterFunctionFactory;
import io.questdb.griffin.engine.functions.test.TestFaultFunctionFactory;
import io.questdb.griffin.engine.functions.test.TestTimestampCounterFactory;
import io.questdb.griffin.engine.functions.test.TestWorkerCloneFunctionFactory;
import io.questdb.griffin.engine.join.JoinRecordMetadata;
import io.questdb.griffin.engine.table.AsyncHorizonJoinNotKeyedRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncHorizonJoinResources;
import io.questdb.griffin.engine.table.AsyncMultiHorizonJoinNotKeyedRecordCursorFactory;
import io.questdb.griffin.engine.table.ConcurrentTimeFrameCursor;
import io.questdb.griffin.engine.table.HorizonJoinSlaveState;
import io.questdb.jit.CompiledFilter;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for the {@code double < (sub-query)} / {@code double > (sub-query)} operators, where the
 * right-hand side is a scalar sub-query (cursor) executed once per query execution.
 *
 * @see io.questdb.griffin.engine.functions.lt.LtDoubleCursorFunctionFactory
 * @see io.questdb.griffin.engine.functions.lt.GtDoubleCursorFunctionFactory
 */
public class DoubleCursorFunctionFactoryTest extends AbstractCursorFunctionFactoryTest {

    @Override
    @Before
    public void setUp() {
        // exercise the parallel horizon join paths; horizon joins scan the master with small frames
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HORIZON_JOIN_ENABLED, "true");
        setProperty(PropertyKey.CAIRO_SMALL_SQL_PAGE_FRAME_MAX_ROWS, 1000);
        super.setUp();
    }

    @Test
    public void testAsyncGroupByExpressionKeyExecutesCursorOnce() throws Exception {
        // The cursor comparison as a GROUP BY expression key runs on the parallel Async Group By path,
        // where per-worker clones of the key function are initialized with the owner's state donated
        // up front. The scalar sub-query must execute exactly once per query - not once per worker -
        // and every worker must observe the same threshold. test_timestamp_counter() increments once
        // per row the sub-query cursor reads, so the counter equals the number of RHS executions.
        runWithPool((compiler, ctx) -> {
            execute(compiler, "create table src (ts timestamp)", ctx);
            execute(compiler, "insert into src values (50000)", ctx);
            execute(
                    compiler,
                    "create table t as (" +
                            "  select x::double price, timestamp_sequence(0, 1000000) ts" +
                            "  from long_sequence(100000)" +
                            ") timestamp(ts) partition by day",
                    ctx
            );

            final String query = "select price::string::double > (select test_timestamp_counter(ts)::long from src) k, count() c " +
                    "from t group by k order by k";

            // the non-thread-safe left operand must still run on the parallel group by
            assertQuery(query)
                    .withContext(ctx)
                    .noLeakCheck()
                    .assertsPlanContaining("Async Group By workers: 4");

            // threshold = 50000 -> 1..50000 false, 50001..100000 true
            assertQuery(query)
                    .withContext(ctx)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            k\tc
                            false\t50000
                            true\t50000
                            """);

            // one explicitly compiled execution pins the exact sub-query execution count,
            // decoupled from how many times the assertion battery above opens cursors
            TestTimestampCounterFactory.COUNTER.set(0);
            try (RecordCursorFactory factory = compiler.compile(query, ctx).getRecordCursorFactory()) {
                try (RecordCursor cursor = factory.getCursor(ctx)) {
                    TestUtils.assertCursor(
                            "k\tc\nfalse\t50000\ntrue\t50000\n",
                            cursor,
                            factory.getMetadata(),
                            true,
                            sink
                    );
                }
            }
            Assert.assertEquals(1, TestTimestampCounterFactory.COUNTER.get());
        });
    }

    @Test
    public void testKeyedAggregateArgumentExecutesCursorOnce() throws Exception {
        // An aggregate whose argument contains a cursor comparison reaches the KEYED parallel
        // group by path (AsyncGroupByAtom) when the projection carries only the aggregate and the
        // key comes from the SAMPLE BY rewrite (a timestamp_floor() key absent from the
        // projection). The scalar sub-query inside the aggregate argument must execute exactly
        // once per query - not once per worker clone - and every worker must observe the same
        // threshold, or nondeterministic sub-queries produce internally inconsistent bucket
        // aggregates within a single execution. test_timestamp_counter() increments once per row
        // the sub-query cursor reads, so the counter equals the number of RHS executions.
        runWithPool((compiler, ctx) -> {
            execute(compiler, "create table src (ts timestamp)", ctx);
            execute(compiler, "insert into src values (5000)", ctx);
            execute(
                    compiler,
                    "create table t as (" +
                            "  select x::double price, timestamp_sequence(0, 1000000) ts" +
                            "  from long_sequence(10000)" +
                            ") timestamp(ts) partition by day",
                    ctx
            );

            final String query = "select sum((price::string::double > (select test_timestamp_counter(ts)::long from src))::int) s " +
                    "from t sample by 1h";

            // the non-thread-safe aggregate argument must still run on the keyed parallel group by
            assertQuery(query)
                    .withContext(ctx)
                    .noLeakCheck()
                    .assertsPlanContaining("Async Group By workers: 4");

            // threshold = 5000; 1h buckets hold prices 1..3600, 3601..7200, 7201..10000,
            // so prices above the threshold count 0, 2200 and 2800 per bucket
            assertQuery(query)
                    .withContext(ctx)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            s
                            0
                            2200
                            2800
                            """);

            // one explicitly compiled execution pins the exact sub-query execution count,
            // decoupled from how many times the assertion battery above opens cursors
            TestTimestampCounterFactory.COUNTER.set(0);
            try (RecordCursorFactory factory = compiler.compile(query, ctx).getRecordCursorFactory()) {
                try (RecordCursor cursor = factory.getCursor(ctx)) {
                    TestUtils.assertCursor("s\n0\n2200\n2800\n", cursor, factory.getMetadata(), true, sink);
                }
            }
            Assert.assertEquals(1, TestTimestampCounterFactory.COUNTER.get());
        });
    }

    @Test
    public void testNotKeyedAggregateArgumentExecutesCursorOnce() throws Exception {
        // An aggregate whose argument contains a cursor comparison runs on the not-keyed parallel
        // group by path (AsyncGroupByNotKeyedAtom) with per-worker clones of the group-by functions.
        // The scalar sub-query inside the aggregate argument must execute exactly once per query -
        // not once per worker - and every worker clone must observe the same threshold.
        // test_timestamp_counter() increments once per row the sub-query cursor reads, so the counter
        // equals the number of RHS executions. (The keyed variant, reached through the SAMPLE BY
        // rewrite, is covered by testKeyedAggregateArgumentExecutesCursorOnce.)
        runWithPool((compiler, ctx) -> {
            execute(compiler, "create table src (ts timestamp)", ctx);
            execute(compiler, "insert into src values (5000)", ctx);
            execute(
                    compiler,
                    "create table t as (" +
                            "  select x::double price, timestamp_sequence(0, 1000000) ts" +
                            "  from long_sequence(10000)" +
                            ") timestamp(ts) partition by day",
                    ctx
            );

            final String query = "select sum(case when price::string::double > (select test_timestamp_counter(ts)::long from src) then 1 else 0 end) s from t";

            // the non-thread-safe aggregate argument must still run on the parallel group by
            assertQuery(query)
                    .withContext(ctx)
                    .noLeakCheck()
                    .assertsPlanContaining("Async Group By workers: 4");

            // threshold = 5000 -> x in 5001..10000 -> 5000 rows
            assertQuery(query)
                    .withContext(ctx)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            s
                            5000
                            """);

            // one explicitly compiled execution pins the exact sub-query execution count
            TestTimestampCounterFactory.COUNTER.set(0);
            try (RecordCursorFactory factory = compiler.compile(query, ctx).getRecordCursorFactory()) {
                try (RecordCursor cursor = factory.getCursor(ctx)) {
                    TestUtils.assertCursor("s\n5000\n", cursor, factory.getMetadata(), true, sink);
                }
            }
            Assert.assertEquals(1, TestTimestampCounterFactory.COUNTER.get());
        });
    }

    @Test
    public void testLateralConsumersExecuteScalarSubQueryOnce() throws Exception {
        // Lateral decorrelation opens one shared cursor per dependent over the not-keyed group
        // by. Each shared consumer initializes its own clone list of the group-by functions; the
        // owner's already-initialized scalar state must be donated to those clones before they
        // initialize, or every shared consumer re-executes the scalar sub-query. With two
        // lateral consumers the sub-query must still execute exactly once per query, on both the
        // parallel (AsyncGroupByNotKeyedRecordCursorFactory) and the serial
        // (GroupByNotKeyedRecordCursorFactory) paths.
        runWithPool((compiler, ctx) -> {
            execute(compiler, "create table src (ts timestamp)", ctx);
            execute(compiler, "insert into src values (5000)", ctx);
            execute(
                    compiler,
                    "create table t as (" +
                            "  select x::double price, timestamp_sequence(0, 1000000) ts" +
                            "  from long_sequence(10000)" +
                            ") timestamp(ts) partition by day",
                    ctx
            );
            execute(compiler, "create table rates (min_amount double, rate double)", ctx);
            execute(compiler, "insert into rates values (1000.0, 0.1)", ctx);

            final String query = "SELECT o.s, sub1.rate, sub2.rate r2 " +
                    "FROM (SELECT sum(case when price::string::double > (SELECT test_timestamp_counter(ts)::long FROM src) then 1 else 0 end) s FROM t) o " +
                    "JOIN LATERAL (SELECT rate FROM rates WHERE min_amount <= o.s) sub1 " +
                    "JOIN LATERAL (SELECT rate FROM rates WHERE min_amount <= o.s) sub2";

            // the aggregate must still run on the parallel group by
            assertQuery(query)
                    .withContext(ctx)
                    .noLeakCheck()
                    .assertsPlanContaining("Async Group By workers: 4");

            // threshold = 5000 -> price in 5001..10000 -> s = 5000; both laterals match 0.1
            final String expected = "s\trate\tr2\n5000\t0.1\t0.1\n";

            // parallel path: owner initializes once, workers and both shared consumers inherit
            TestTimestampCounterFactory.COUNTER.set(0);
            try (RecordCursorFactory factory = compiler.compile(query, ctx).getRecordCursorFactory()) {
                try (RecordCursor cursor = factory.getCursor(ctx)) {
                    TestUtils.assertCursor(expected, cursor, factory.getMetadata(), true, sink);
                }
            }
            Assert.assertEquals("parallel: scalar sub-query must execute exactly once", 1, TestTimestampCounterFactory.COUNTER.get());

            // serial path: same contract on GroupByNotKeyedRecordCursorFactory's shared cursors
            ctx.setParallelGroupByEnabled(false);
            try {
                TestTimestampCounterFactory.COUNTER.set(0);
                try (RecordCursorFactory factory = compiler.compile(query, ctx).getRecordCursorFactory()) {
                    try (RecordCursor cursor = factory.getCursor(ctx)) {
                        TestUtils.assertCursor(expected, cursor, factory.getMetadata(), true, sink);
                    }
                }
                Assert.assertEquals("serial: scalar sub-query must execute exactly once", 1, TestTimestampCounterFactory.COUNTER.get());
            } finally {
                ctx.setParallelGroupByEnabled(true);
            }
        });
    }

    @Test
    public void testScalarOwnerInitFailureKeepsCachedFactoryReusable() throws Exception {
        // A shared consumer can open the serial not-keyed group by before its primary consumer.
        // Fail while the owner's scalar RHS initializes: getSharedCursor must close the partial
        // owner open before it reaches the single shared-clone donation. Reopening the same cached
        // factory must execute a fresh scalar RHS and donate that value to the one shared clone.
        runWithPool((compiler, ctx) -> {
            execute(compiler, "create table src (ts timestamp)", ctx);
            execute(compiler, "insert into src values (5000)", ctx);
            execute(
                    compiler,
                    "create table t as (" +
                            "  select x::double price, timestamp_sequence(0, 1000000) ts" +
                            "  from long_sequence(10000)" +
                            ") timestamp(ts) partition by day",
                    ctx
            );
            execute(compiler, "create table rates (min_amount double, rate double)", ctx);
            execute(compiler, "insert into rates values (1000.0, 0.1)", ctx);

            final String query = "SELECT o.s, sub1.rate " +
                    "FROM (SELECT sum((test_fault() AND price::string::double > " +
                    "(SELECT test_timestamp_counter(ts)::long FROM src WHERE test_fault()))::int) s FROM t) o " +
                    "JOIN LATERAL (SELECT rate FROM rates WHERE min_amount <= o.s) sub1";
            ctx.setParallelGroupByEnabled(false);
            try (RecordCursorFactory factory = compiler.compile(query, ctx).getRecordCursorFactory()) {
                // The outer test_fault initializes first and later counts the one owner-to-shared
                // donation. The second test_fault is in the scalar RHS and fails next.
                TestFaultFunctionFactory.armToFailAfterInits(1);
                try {
                    factory.getCursor(ctx).close();
                    Assert.fail("injected scalar RHS init failure expected");
                } catch (Throwable e) {
                    TestUtils.assertContains(e.getMessage(), "test_fault: injected init failure");
                } finally {
                    TestFaultFunctionFactory.disarm();
                }
                Assert.assertEquals(1, TestFaultFunctionFactory.faultsTriggered());
                Assert.assertEquals(
                        "owner state must not be donated before scalar RHS initialization succeeds",
                        0,
                        TestFaultFunctionFactory.offersFromInitProbe()
                );

                TestTimestampCounterFactory.COUNTER.set(0);
                try (RecordCursor cursor = factory.getCursor(ctx)) {
                    TestUtils.assertCursor("s\trate\n5000\t0.1\n", cursor, factory.getMetadata(), true, sink);
                }
                Assert.assertEquals("scalar sub-query must re-execute once after a failed owner open", 1, TestTimestampCounterFactory.COUNTER.get());
                Assert.assertEquals(
                        "owner state must be donated to exactly one shared clone",
                        1,
                        TestFaultFunctionFactory.offersFromInitProbe()
                );
            } finally {
                ctx.setParallelGroupByEnabled(true);
            }
        });
    }

    @Test
    public void testSharedCursorInitFailureKeepsCachedFactoryReusable() throws Exception {
        // When a shared consumer opens the base cursor first (it sits on the build side of the
        // enclosing join), getSharedCursor initializes the owner functions before donating
        // state. If the consumer's own clone initialization then throws, the catch must fully
        // close the primary cursor so the next execution of the cached factory re-initializes
        // the owner functions; otherwise it serves the previous execution's stale scalar state.
        runWithPool((compiler, ctx) -> {
            execute(compiler, "create table src (ts timestamp)", ctx);
            execute(compiler, "insert into src values (5000)", ctx);
            execute(
                    compiler,
                    "create table t as (" +
                            "  select x::double price, timestamp_sequence(0, 1000000) ts" +
                            "  from long_sequence(10000)" +
                            ") timestamp(ts) partition by day",
                    ctx
            );
            execute(compiler, "create table rates (min_amount double, rate double)", ctx);
            execute(compiler, "insert into rates values (1000.0, 0.1)", ctx);

            final String query = "SELECT o.s, sub1.rate " +
                    "FROM (SELECT sum(case when test_fault() and price::string::double > (SELECT test_timestamp_counter(ts)::long FROM src) then 1 else 0 end) s FROM t) o " +
                    "JOIN LATERAL (SELECT rate FROM rates WHERE min_amount <= o.s) sub1";
            ctx.setParallelGroupByEnabled(false);
            try (RecordCursorFactory factory = compiler.compile(query, ctx).getRecordCursorFactory()) {
                // the owner list initializes test_fault() once inside getSharedCursor; the
                // shared consumer's clone init runs next and fails
                TestFaultFunctionFactory.armToFailAfterInits(1);
                try {
                    factory.getCursor(ctx).close();
                    Assert.fail("injected init failure expected");
                } catch (Throwable e) {
                    TestUtils.assertContains(e.getMessage(), "test_fault: injected init failure");
                } finally {
                    TestFaultFunctionFactory.disarm();
                }
                // the cached factory must re-initialize the owner functions on the next
                // execution: the scalar sub-query executes exactly once more and the results
                // are fresh
                TestTimestampCounterFactory.COUNTER.set(0);
                try (RecordCursor cursor = factory.getCursor(ctx)) {
                    TestUtils.assertCursor("s\trate\n5000\t0.1\n", cursor, factory.getMetadata(), true, sink);
                }
                Assert.assertEquals("scalar sub-query must re-execute after a failed open", 1, TestTimestampCounterFactory.COUNTER.get());
            } finally {
                ctx.setParallelGroupByEnabled(true);
            }
        });
    }

    @Test
    public void testHorizonJoinAggregateArgumentExecutesCursorOnce() throws Exception {
        // A not-keyed single-slave HORIZON JOIN whose aggregate argument contains a cursor
        // comparison runs on the parallel path (BaseAsyncHorizonJoinAtom) with per-worker clones of
        // the group-by functions. The scalar sub-query must execute exactly once per query - not
        // once per worker - and every worker clone must observe the same threshold.
        runWithPool((compiler, ctx) -> {
            createHorizonJoinTables(compiler, ctx);

            final String query = "SELECT avg((t.qty::string::double > (SELECT test_timestamp_counter(ts)::long FROM src))::int) a " +
                    "FROM trades t HORIZON JOIN prices p ON (t.sym = p.sym) LIST (0) AS h";

            // the non-thread-safe aggregate argument must still run on the parallel horizon join
            assertQuery(query)
                    .withContext(ctx)
                    .noLeakCheck()
                    .assertsPlanContaining("Async Horizon Join workers: 4");

            // threshold = 5000 -> qty in 5001..10000 -> 5000 of 10000 rows -> avg 0.5
            assertQuery(query)
                    .withContext(ctx)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            a
                            0.5
                            """);

            // one explicitly compiled execution pins the exact sub-query execution count
            TestTimestampCounterFactory.COUNTER.set(0);
            try (RecordCursorFactory factory = compiler.compile(query, ctx).getRecordCursorFactory()) {
                try (RecordCursor cursor = factory.getCursor(ctx)) {
                    TestUtils.assertCursor("a\n0.5\n", cursor, factory.getMetadata(), true, sink);
                }
            }
            Assert.assertEquals(1, TestTimestampCounterFactory.COUNTER.get());
        });
    }

    @Test
    public void testHorizonJoinExpressionKeyExecutesCursorOnce() throws Exception {
        // The cursor comparison as a GROUP BY expression key of a single-slave HORIZON JOIN runs on
        // the parallel path (AsyncHorizonJoinAtom), where per-worker clones of the key function are
        // initialized with the owner's state donated up front. The scalar sub-query must execute
        // exactly once per query - not once per worker - and every worker must observe the same
        // threshold. test_timestamp_counter() increments once per row the sub-query cursor reads, so
        // the counter equals the number of RHS executions.
        runWithPool((compiler, ctx) -> {
            createHorizonJoinTables(compiler, ctx);

            final String query = "SELECT t.qty::string::double > (SELECT test_timestamp_counter(ts)::long FROM src) k, avg(p.price) a " +
                    "FROM trades t HORIZON JOIN prices p ON (t.sym = p.sym) LIST (0) AS h " +
                    "GROUP BY k ORDER BY k";

            // the non-thread-safe key must still run on the parallel horizon join
            assertQuery(query)
                    .withContext(ctx)
                    .noLeakCheck()
                    .assertsPlanContaining("Async Horizon Join workers: 4");

            // threshold = 5000 -> both key groups see the single price 100.0
            assertQuery(query)
                    .withContext(ctx)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            k\ta
                            false\t100.0
                            true\t100.0
                            """);

            // one explicitly compiled execution pins the exact sub-query execution count
            TestTimestampCounterFactory.COUNTER.set(0);
            try (RecordCursorFactory factory = compiler.compile(query, ctx).getRecordCursorFactory()) {
                try (RecordCursor cursor = factory.getCursor(ctx)) {
                    TestUtils.assertCursor("k\ta\nfalse\t100.0\ntrue\t100.0\n", cursor, factory.getMetadata(), true, sink);
                }
            }
            Assert.assertEquals(1, TestTimestampCounterFactory.COUNTER.get());
        });
    }

    @Test
    public void testHorizonJoinAggregatesCompileOncePerWorker() throws Exception {
        // The single-slave HORIZON join compiles the per-worker projection clones (which include
        // the GROUP_BY-flagged aggregate slots) and must reuse those aggregate clones instead of
        // freeing them and parsing every aggregate a second time per worker.
        // test_close_counter('x') inside the aggregate argument counts one creation per compile,
        // so the expected count is exactly one owner compile plus one per worker clone.
        runWithPool((compiler, ctx) -> {
            createHorizonJoinTables(compiler, ctx);
            final String query = "SELECT avg((t.qty::string::double > (SELECT max(price) FROM prices))::int * length(test_close_counter('x'))) a " +
                    "FROM trades t HORIZON JOIN prices p ON (t.sym = p.sym) LIST (0) AS h";
            assertQuery(query)
                    .withContext(ctx)
                    .noLeakCheck()
                    .assertsPlanContaining("Async Horizon Join workers: 4");
            TestCloseCounterFunctionFactory.reset();
            compiler.compile(query, ctx).getRecordCursorFactory().close();
            Assert.assertEquals(
                    "each aggregate must compile once for the owner and once per worker clone",
                    5,
                    TestCloseCounterFunctionFactory.created()
            );
        });
    }

    @Test
    public void testMultiHorizonJoinAggregatesCompileOncePerWorker() throws Exception {
        // The keyed multi-slave HORIZON join compiles the per-worker projection clones (which
        // include the GROUP_BY-flagged aggregate slots) and must reuse those aggregate clones
        // instead of freeing them and parsing every aggregate a second time per worker.
        runWithPool((compiler, ctx) -> {
            createHorizonJoinTables(compiler, ctx);
            final String query = "SELECT t.qty::string::double > (SELECT test_timestamp_counter(ts)::long FROM src) k, " +
                    "avg((t.qty + 1)::string::double * length(test_close_counter('x'))) a, avg(a2.ask) b " +
                    "FROM trades t HORIZON JOIN prices p ON (t.sym = p.sym) HORIZON JOIN asks a2 ON (t.sym = a2.sym) LIST (0) AS h " +
                    "GROUP BY k ORDER BY k";
            assertQuery(query)
                    .withContext(ctx)
                    .noLeakCheck()
                    .assertsPlanContaining("Async Multi Horizon Join workers: 4");
            TestCloseCounterFunctionFactory.reset();
            compiler.compile(query, ctx).getRecordCursorFactory().close();
            Assert.assertEquals(
                    "each aggregate must compile once for the owner and once per worker clone",
                    5,
                    TestCloseCounterFunctionFactory.created()
            );
        });
    }

    @Test
    public void testHorizonJoinConstructorRollbackPreservesPrimaryAndContinues() throws Exception {
        assertMemoryLeak(() -> assertHorizonConstructorRollback(false));
    }

    @Test
    public void testHorizonJoinEarlyMetadataRollbackOwnsDetachedResources() throws Exception {
        assertMemoryLeak(this::assertHorizonEarlyMetadataRollback);
    }

    @Test
    public void testHorizonJoinCompileRollbackPreservesPrimaryAndContinues() throws Exception {
        // The second worker projection compile fails after the owner and one worker function exist.
        // Both functions throw on close. The worker helper owns the first clone, while the horizon
        // generator owns the owner function and the outer join generator still owns both table
        // factories. Rollback must retain the compile failure, append both cleanup failures in
        // close order, and continue through the later factories.
        runWithPool((compiler, ctx) -> {
            createHorizonJoinTables(compiler, ctx);
            final String query = "SELECT t.qty > (SELECT max(price) FROM prices) k, " +
                    "avg(case when test_fault() then p.price else p.price end) a " +
                    "FROM trades t HORIZON JOIN prices p ON (t.sym = p.sym) LIST (0) AS h " +
                    "GROUP BY k";
            assertHorizonCompileRollback(compiler, ctx, query);
        });
    }

    @Test
    public void testMultiHorizonJoinConstructorRollbackPreservesPrimaryAndContinues() throws Exception {
        assertMemoryLeak(() -> assertHorizonConstructorRollback(true));
    }

    @Test
    public void testMultiHorizonJoinAggregateArgumentExecutesCursorOnce() throws Exception {
        // Same contract as the single-slave variant, on the multi-slave parallel path
        // (BaseAsyncMultiHorizonJoinAtom): the scalar sub-query inside an aggregate argument must
        // execute exactly once per query even with 4 workers holding group-by function clones.
        runWithPool((compiler, ctx) -> {
            createHorizonJoinTables(compiler, ctx);

            final String query = "SELECT avg((t.qty::string::double > (SELECT test_timestamp_counter(ts)::long FROM src))::int) a, avg(a2.ask) b " +
                    "FROM trades t HORIZON JOIN prices p ON (t.sym = p.sym) HORIZON JOIN asks a2 ON (t.sym = a2.sym) LIST (0) AS h";

            // the non-thread-safe aggregate argument must still run on the parallel horizon join
            assertQuery(query)
                    .withContext(ctx)
                    .noLeakCheck()
                    .assertsPlanContaining("Async Multi Horizon Join workers: 4");

            // threshold = 5000 -> avg 0.5; the single ask 200.0 matches every trade
            assertQuery(query)
                    .withContext(ctx)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            a\tb
                            0.5\t200.0
                            """);

            // one explicitly compiled execution pins the exact sub-query execution count
            TestTimestampCounterFactory.COUNTER.set(0);
            try (RecordCursorFactory factory = compiler.compile(query, ctx).getRecordCursorFactory()) {
                try (RecordCursor cursor = factory.getCursor(ctx)) {
                    TestUtils.assertCursor("a\tb\n0.5\t200.0\n", cursor, factory.getMetadata(), true, sink);
                }
            }
            Assert.assertEquals(1, TestTimestampCounterFactory.COUNTER.get());
        });
    }

    @Test
    public void testMultiHorizonJoinCompileRollbackPreservesPrimaryAndContinues() throws Exception {
        // Multi-slave counterpart: compilation fails only after every HorizonJoinSlaveState owns a
        // slave factory. A throwing worker cleanup and then a throwing owner cleanup must not mask
        // the compile failure or prevent teardown of the later slave states and factories.
        runWithPool((compiler, ctx) -> {
            createHorizonJoinTables(compiler, ctx);
            final String query = "SELECT t.qty > (SELECT max(price) FROM prices) k, " +
                    "avg(case when test_fault() then p.price else p.price end) a, avg(a2.ask) b " +
                    "FROM trades t HORIZON JOIN prices p ON (t.sym = p.sym) " +
                    "HORIZON JOIN asks a2 ON (t.sym = a2.sym) LIST (0) AS h GROUP BY k";
            assertHorizonCompileRollback(compiler, ctx, query);
        });
    }

    @Test
    public void testMultiHorizonJoinPartialStateTransferRollback() throws Exception {
        assertMemoryLeak(this::assertMultiHorizonPartialStateTransferRollback);
    }

    @Test
    public void testMultiHorizonJoinPendingRollbackPreservesValidationPrimary() throws Exception {
        runWithPool((compiler, ctx) -> {
            createHorizonJoinTables(compiler, ctx);
            TestFaultFunctionFactory.armCloseFailures();
            try {
                compiler.compile(
                        "SELECT count() FROM trades t " +
                                "HORIZON JOIN (SELECT * FROM prices WHERE test_fault()) p ON (t.sym = p.sym) " +
                                "HORIZON JOIN (SELECT * FROM asks WHERE test_fault()) a ON (t.sym = a.sym)",
                        ctx
                );
                Assert.fail("missing horizon offset validation expected");
            } catch (Throwable failure) {
                Assert.assertTrue(failure instanceof SqlException);
                Assert.assertEquals(29, ((SqlException) failure).getPosition());
                TestUtils.assertContains(failure.getMessage(), "HORIZON JOIN requires offset configuration");
                Assert.assertEquals(TestFaultFunctionFactory.created(), TestFaultFunctionFactory.closeCalls());
                Assert.assertEquals(10, TestFaultFunctionFactory.created());
                Assert.assertArrayEquals(
                        new Throwable[]{
                                TestFaultFunctionFactory.closeFailure(1),
                                TestFaultFunctionFactory.closeFailure(6)
                        },
                        failure.getSuppressed()
                );
                Assert.assertArrayEquals(
                        new Throwable[]{
                                TestFaultFunctionFactory.closeFailure(2),
                                TestFaultFunctionFactory.closeFailure(3),
                                TestFaultFunctionFactory.closeFailure(4),
                                TestFaultFunctionFactory.closeFailure(0)
                        },
                        failure.getSuppressed()[0].getSuppressed()
                );
                Assert.assertArrayEquals(
                        new Throwable[]{
                                TestFaultFunctionFactory.closeFailure(7),
                                TestFaultFunctionFactory.closeFailure(8),
                                TestFaultFunctionFactory.closeFailure(9),
                                TestFaultFunctionFactory.closeFailure(5)
                        },
                        failure.getSuppressed()[1].getSuppressed()
                );
                Assert.assertEquals(0, engine.getBusyReaderCount());
            } finally {
                TestFaultFunctionFactory.disarm();
            }
        });
    }

    @Test
    public void testMultiHorizonJoinExpressionKeyExecutesCursorOnce() throws Exception {
        // Same contract as the single-slave variant, on the multi-slave parallel path
        // (AsyncMultiHorizonJoinAtom): the scalar sub-query inside a GROUP BY expression key must
        // execute exactly once per query even with 4 workers holding key function clones.
        runWithPool((compiler, ctx) -> {
            createHorizonJoinTables(compiler, ctx);

            final String query = "SELECT t.qty::string::double > (SELECT test_timestamp_counter(ts)::long FROM src) k, avg(p.price) a, avg(a2.ask) b " +
                    "FROM trades t HORIZON JOIN prices p ON (t.sym = p.sym) HORIZON JOIN asks a2 ON (t.sym = a2.sym) LIST (0) AS h " +
                    "GROUP BY k ORDER BY k";

            // the non-thread-safe key must still run on the parallel horizon join
            assertQuery(query)
                    .withContext(ctx)
                    .noLeakCheck()
                    .assertsPlanContaining("Async Multi Horizon Join workers: 4");

            // threshold = 5000 -> both key groups see the single price 100.0 and ask 200.0
            assertQuery(query)
                    .withContext(ctx)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            k\ta\tb
                            false\t100.0\t200.0
                            true\t100.0\t200.0
                            """);

            // one explicitly compiled execution pins the exact sub-query execution count
            TestTimestampCounterFactory.COUNTER.set(0);
            try (RecordCursorFactory factory = compiler.compile(query, ctx).getRecordCursorFactory()) {
                try (RecordCursor cursor = factory.getCursor(ctx)) {
                    TestUtils.assertCursor("k\ta\tb\nfalse\t100.0\t200.0\ntrue\t100.0\t200.0\n", cursor, factory.getMetadata(), true, sink);
                }
            }
            Assert.assertEquals(1, TestTimestampCounterFactory.COUNTER.get());
        });
    }

    @Test
    public void testWindowJoinAggregateArgumentExecutesCursorOnce() throws Exception {
        // A WINDOW JOIN whose aggregate argument contains a cursor comparison runs on the parallel
        // path (AsyncWindowJoinAtom) with per-worker clones of the group-by functions. The scalar
        // sub-query must execute exactly once per query execution - not once per worker - and
        // every worker clone must observe the same threshold. test_timestamp_counter() increments
        // once per row the sub-query cursor reads, so the counter equals the number of RHS
        // executions.
        runWithPool((compiler, ctx) -> {
            execute(compiler, "create table src (ts timestamp)", ctx);
            execute(compiler, "insert into src values (5000)", ctx);
            // master and slave rows align 10 seconds apart, so a one-second window holds exactly
            // the aligned slave row and nothing else
            execute(
                    compiler,
                    "create table trades as (" +
                            "  select x::double qty, timestamp_sequence(10_000_000, 10_000_000) ts" +
                            "  from long_sequence(10_000)" +
                            ") timestamp(ts) partition by day",
                    ctx
            );
            execute(
                    compiler,
                    "create table prices as (" +
                            "  select x::double price, timestamp_sequence(10_000_000, 10_000_000) ts" +
                            "  from long_sequence(10_000)" +
                            ") timestamp(ts) partition by day",
                    ctx
            );

            final String query = "SELECT sum(s) t FROM (" +
                    "  SELECT sum((p.price::string::double > (SELECT test_timestamp_counter(ts)::long FROM src))::int) s " +
                    "  FROM trades t " +
                    "  WINDOW JOIN prices p RANGE BETWEEN 1 seconds PRECEDING AND 1 seconds FOLLOWING EXCLUDE PREVAILING" +
                    ")";

            // the non-thread-safe aggregate argument must still run on the parallel window join
            assertQuery(query)
                    .withContext(ctx)
                    .noLeakCheck()
                    .assertsPlanContaining("Async Window Join workers: 4");

            // threshold = 5000 -> prices 5001..10000 cross it -> 5000 of 10000 windows contribute 1
            assertQuery(query)
                    .withContext(ctx)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            t
                            5000
                            """);

            // one explicitly compiled execution pins the exact sub-query execution count
            TestTimestampCounterFactory.COUNTER.set(0);
            try (RecordCursorFactory factory = compiler.compile(query, ctx).getRecordCursorFactory()) {
                try (RecordCursor cursor = factory.getCursor(ctx)) {
                    TestUtils.assertCursor("t\n5000\n", cursor, factory.getMetadata(), true, sink);
                }
                Assert.assertEquals(1, TestTimestampCounterFactory.COUNTER.get());

                // change the RHS and re-execute the same compiled factory: the cached scalar must
                // refresh, again with a single sub-query execution
                execute(compiler, "update src set ts = 9000", ctx);
                try (RecordCursor cursor = factory.getCursor(ctx)) {
                    TestUtils.assertCursor("t\n1000\n", cursor, factory.getMetadata(), true, sink);
                }
                Assert.assertEquals(2, TestTimestampCounterFactory.COUNTER.get());
            }
        });
    }

    @Test
    public void testSampleByFillLinearCursorComparisonKey() throws Exception {
        // regression: compiling the scalar sub-query of a cursor-comparison key must not corrupt
        // generateSampleBy's projection scratch state, and the execution plan must render across
        // the nested sub-query plan of the key
        assertMemoryLeak(() -> {
            execute("create table t as (" +
                    "select x::double price, x::double qty, timestamp_sequence(0, 60000000) ts" +
                    " from long_sequence(10)" +
                    ") timestamp(ts) partition by day");
            final String query = "select price > (select avg(price) from t) k, sum(qty) s, ts from t sample by 1h fill(linear)";
            assertQuery(query)
                    .noLeakCheck()
                    .assertsPlanContaining("Sample By");
            // avg(price) = 5.5 -> k=false sums 1..5, k=true sums 6..10; single 1h bucket
            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            k\ts\tts
                            false\t15.0\t1970-01-01T00:00:00.000000Z
                            true\t40.0\t1970-01-01T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testWorkerGroupByCloneCompilationFailureFreesPartialClones() throws Exception {
        // compileWorkerGroupByFunctionsConditionally compiles one clone list per worker. When a
        // later clone's compilation throws, the helper must free the already-compiled clones, or
        // their resources leak. alloc() places tracked native memory in the aggregate argument
        // and the armed test_fault() makes a later worker clone's compilation throw after the
        // owner and at least one clone compiled successfully. The plain parallel not-keyed GROUP
        // BY reaches this helper as its only clone pass, so the armed compile count is
        // deterministic (a HORIZON JOIN would clone the projection first and absorb the fault
        // there instead).
        runWithPool((compiler, ctx) -> {
            execute(
                    compiler,
                    "create table t as (" +
                            "  select x::double qty, timestamp_sequence(0, 1000000) ts" +
                            "  from long_sequence(10000)" +
                            ") timestamp(ts) partition by day",
                    ctx
            );
            final String query = "SELECT avg(qty + alloc(32) + (case when test_fault() then 1.0 else 2.0 end)) a FROM t";
            // owner assembly compiles test_fault() once, then each of the 4 worker clones
            // compiles it once more; fail on the second clone
            TestFaultFunctionFactory.armToFailAfterCompiles(2);
            try {
                compiler.compile(query, ctx);
                Assert.fail("compilation should have failed with the injected fault");
            } catch (Throwable e) {
                TestUtils.assertContains(e.getMessage(), "test_fault: injected compile failure");
            } finally {
                TestFaultFunctionFactory.disarm();
            }
        });
    }

    @Test
    public void testWorkerKeyCloneCompilationFailureFreesPartialClones() throws Exception {
        // compilePerWorkerInnerProjectionFunctions compiles one projection clone list per worker.
        // When a later clone's compilation throws, the helper must free the already-compiled
        // clones, or their resources - including the cursor-comparison key's nested sub-query
        // factory - leak. alloc() places tracked native memory in the key expression and the
        // armed test_fault() makes a later worker clone's compilation throw after the owner and
        // at least one clone compiled successfully.
        runWithPool((compiler, ctx) -> {
            createHorizonJoinTables(compiler, ctx);
            final String query = "SELECT t.qty + alloc(32) + (case when test_fault() then 1.0 else 2.0 end) > (SELECT max(price) FROM prices) k, avg(p.price) a " +
                    "FROM trades t HORIZON JOIN prices p ON (t.sym = p.sym) LIST (0) AS h " +
                    "GROUP BY k ORDER BY k";
            // owner assembly compiles test_fault() once, then each of the 4 worker clones
            // compiles it once more; fail on the second clone
            TestFaultFunctionFactory.armToFailAfterCompiles(2);
            try {
                compiler.compile(query, ctx);
                Assert.fail("compilation should have failed with the injected fault");
            } catch (Throwable e) {
                TestUtils.assertContains(e.getMessage(), "test_fault: injected compile failure");
            } finally {
                TestFaultFunctionFactory.disarm();
            }
        });
    }

    @Test
    public void testSpecialValueRawBitMatrix() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE double_values (id INT, v DOUBLE)");
            execute("CREATE TABLE double_scalars (id INT, v DOUBLE)");
            final long[] bits = {
                    0xfff0_0000_0000_0000L, // 1: -Infinity
                    0xbff0_0000_0000_0000L, // 2: -1.0
                    0x8000_0000_0000_0000L, // 3: -0.0
                    0x0000_0000_0000_0000L, // 4: +0.0
                    0x3ff0_0000_0000_0000L, // 5: +1.0
                    0x7ff0_0000_0000_0000L, // 6: +Infinity
                    0x7ff8_0000_0000_0000L, // 7: canonical DOUBLE NULL NaN
                    0x7ff8_0000_0000_0001L  // 8: noncanonical NaN payload
            };
            try (
                    TableWriter valueWriter = getWriter("double_values");
                    TableWriter scalarWriter = getWriter("double_scalars")
            ) {
                for (int i = 0; i < bits.length; i++) {
                    final double value = Double.longBitsToDouble(bits[i]);
                    TableWriter.Row row = valueWriter.newRow();
                    row.putInt(0, i + 1);
                    row.putDouble(1, value);
                    row.append();
                    row = scalarWriter.newRow();
                    row.putInt(0, i + 1);
                    row.putDouble(1, value);
                    row.append();
                }
                valueWriter.commit();
                scalarWriter.commit();
            }

            final int initialJitMode = sqlExecutionContext.getJitMode();
            try {
                for (int jitMode : new int[]{SqlJitMode.JIT_MODE_DISABLED, SqlJitMode.JIT_MODE_ENABLED}) {
                    sqlExecutionContext.setJitMode(jitMode);
                    // Expected ID lists are ordered as v > scalar, v >= scalar, v < scalar,
                    // v <= scalar, scalar > v, scalar >= v, scalar < v, scalar <= v.
                    assertSpecialValueMatrix(4,
                            "5,6", "3,4,5,6", "1,2", "1,2,3,4",
                            "1,2", "1,2,3,4", "5,6", "3,4,5,6"
                    );
                    assertSpecialValueMatrix(6,
                            "", "1,6,7,8", "2,3,4,5", "1,2,3,4,5,6,7,8",
                            "2,3,4,5", "1,2,3,4,5,6,7,8", "", "1,6,7,8"
                    );
                    assertSpecialValueMatrix(7,
                            "", "1,6,7,8", "", "1,6,7,8",
                            "", "1,6,7,8", "", "1,6,7,8"
                    );
                    assertSpecialValueMatrix(8,
                            "", "1,6,7,8", "", "1,6,7,8",
                            "", "1,6,7,8", "", "1,6,7,8"
                    );
                }
            } finally {
                sqlExecutionContext.setJitMode(initialJitMode);
            }
        });
    }

    @Test
    public void testToleranceEqualityAtScalarBoundary() throws Exception {
        // Doubles within Numbers.DOUBLE_TOLERANCE (1e-10) of the cached scalar are equal under
        // QuestDB's double semantics: the strict comparisons must exclude them and the negated
        // inclusive forms must include them, on both the Gt and Lt cursor functions. Values just
        // outside the tolerance follow the primitive ordering.
        runWithPool((compiler, ctx) -> {
            execute(compiler, "create table t (price double)", ctx);
            // 1.0 and the two 5e-11 neighbours are tolerance-equal to the scalar; the two 2e-10
            // neighbours are strictly greater/less
            execute(compiler, "insert into t values (1.0), (1.00000000005), (0.99999999995), (1.0000000002), (0.9999999998)", ctx);
            execute(compiler, "create table s (v double)", ctx);
            execute(compiler, "insert into s values (1.0)", ctx);

            assertQuery("select count() c from t where price > (select v from s)")
                    .withContext(ctx)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("c\n1\n");
            assertQuery("select count() c from t where price <= (select v from s)")
                    .withContext(ctx)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("c\n4\n");
            assertQuery("select count() c from t where price < (select v from s)")
                    .withContext(ctx)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("c\n1\n");
            assertQuery("select count() c from t where price >= (select v from s)")
                    .withContext(ctx)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("c\n4\n");
        });
    }

    @Test
    public void testWorkerFilterCloneCompilationFailureFreesProjectionClones() throws Exception {
        // The keyed parallel GROUP BY compiles the per-worker projection clones before it clones
        // a non-thread-safe filter. When a later filter clone's compilation throws, the
        // generator catch must free the completed projection clones - including the
        // cursor-comparison key's nested sub-query factory and its tracked native memory. The
        // helper that compiled the projection clones has already returned, so only a
        // catch-visible owner in generateSelectGroupBy can reach them. alloc() places tracked
        // native memory in each projection clone and the armed test_fault() makes a later
        // worker filter clone's compilation throw after every projection clone compiled.
        runWithPool((compiler, ctx) -> {
            execute(
                    compiler,
                    "create table t as (" +
                            "  select x::double price, x::double qty, timestamp_sequence(0, 1000000) ts" +
                            "  from long_sequence(10000)" +
                            ") timestamp(ts) partition by day",
                    ctx
            );
            final String query = "SELECT qty + alloc(32) > (SELECT max(price) FROM t) k, avg(price) a " +
                    "FROM t WHERE test_fault() GROUP BY k";
            // test_fault() compiles five times before the keyed group-by clones the stolen
            // filter: once for the owner filter and four times for the async filter factory's
            // worker clones (generateFilter0 builds those before the group-by steals the
            // filter). The keyed group-by then compiles the per-worker projection clones and
            // re-clones the filter once per worker; fail on the second of those, after every
            // projection clone compiled
            TestFaultFunctionFactory.armToFailAfterCompiles(6);
            try {
                compiler.compile(query, ctx);
                Assert.fail("compilation should have failed with the injected fault");
            } catch (Throwable e) {
                TestUtils.assertContains(e.getMessage(), "test_fault: injected compile failure");
            } finally {
                TestFaultFunctionFactory.disarm();
            }
        });
    }

    @Test
    public void testGenerateFillFailureFreesAdoptedGroupByFactory() throws Exception {
        // generateFill runs after the group-by factory constructor has adopted the projection
        // functions and the base factory. When fill-value parsing throws, generateFill's catch
        // must free the adopted factory - closing every projection clone and the base - and
        // the generator catch must not double-close the base (idempotent factory close).
        // alloc() places tracked native memory in the owner and per-worker aggregate clones so
        // assertMemoryLeak() sees any abandoned clone.
        runWithPool((compiler, ctx) -> {
            execute(
                    compiler,
                    "create table t as (" +
                            "  select x::double price, timestamp_sequence(0, 1000000) ts" +
                            "  from long_sequence(10000)" +
                            ") timestamp(ts) partition by day",
                    ctx
            );
            try {
                compiler.compile(
                        "select ts, sum(price + alloc(32)) s from t sample by 1d fill(no_such_column)",
                        ctx
                );
                Assert.fail("fill-value parse failure expected");
            } catch (Throwable e) {
                TestUtils.assertContains(e.getMessage(), "Invalid column");
            }
        });
    }

    @Test
    public void testSampleByKeyedFromToFailureFreesAssembledFunctions() throws Exception {
        // generateSampleBy assembles the group-by and projection functions (including any
        // resource-bearing scalar sub-query keys and aggregate arguments) before rejecting the
        // unsupported keyed FROM/TO combination. When guardAgainstFromToWithKeyedSampleBy throws,
        // the catch must free the assembled owner lists. alloc() places tracked native memory in
        // both the key and the aggregate argument so assertMemoryLeak() sees the leak.
        assertMemoryLeak(() -> {
            execute("create table t as (" +
                    "select x::double price, x::double qty, timestamp_sequence(0, 60000000) ts" +
                    " from long_sequence(10)" +
                    ") timestamp(ts) partition by day");
            assertQuery("select price + alloc(32) > (select avg(price) from t) k, sum(qty + alloc(32)) s " +
                    "from t sample by 1h from dateadd('h', 0, '1970-01-01'::timestamp) fill(prev)")
                    .fails(-1, "FROM-TO intervals are not supported for keyed SAMPLE BY queries");
        });
    }

    @Test
    public void testWindowJoinCursorComparisonProjection() throws Exception {
        // regression: compiling the scalar sub-query of a cursor-comparison projection must not
        // corrupt the WINDOW JOIN aggregation scratch state in generateJoins, and the execution
        // plan must render across the nested sub-query plan
        assertMemoryLeak(() -> {
            execute("create table trades as (" +
                    "select 'A'::symbol sym, x::double price, timestamp_sequence(1000000, 1000000) ts" +
                    " from long_sequence(4)" +
                    ") timestamp(ts) partition by day");
            execute("create table prices (ts timestamp, sym symbol, price double) timestamp(ts)");
            execute("insert into prices values (0, 'A', 2.0)");
            final String query = "select t.price > (select avg(p2.price) from prices p2) k, sum(p.price) w " +
                    "from trades t window join prices p range between 100 seconds preceding and 1 seconds following";
            assertQuery(query)
                    .noLeakCheck()
                    .assertsPlanContaining("Window Join");
            // avg = 2.0 -> k = price > 2.0; every window sees the single price 2.0
            assertQuery(query)
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            k\tw
                            false\t2.0
                            false\t2.0
                            true\t2.0
                            true\t2.0
                            """);
        });
    }

    @Test
    public void testHorizonJoinPostAssemblyFailureFreesKeyFunctions() throws Exception {
        // A keyed HORIZON JOIN with a cursor-comparison key assembles the projection (including
        // the resource-bearing scalar sub-query key) before the single-threaded path validates
        // that the master supports random access. When that validation throws, the generator's
        // catch must free the extracted owner key functions, or the whole key chain - including
        // the nested sub-query factory - leaks. alloc() places tracked native memory inside the
        // key chain so assertMemoryLeak() sees the leak. The UNION ALL master supports neither
        // page frames (downgrading to the single-threaded path) nor random access (triggering the
        // post-assembly failure), while timestamp(ts) re-designates the timestamp so the
        // pre-assembly master validation passes.
        assertMemoryLeak(() -> {
            execute("create table trades (sym symbol, qty double, ts timestamp) timestamp(ts) partition by day");
            execute("create table prices (ts timestamp, sym symbol, price double) timestamp(ts)");
            assertQuery("select t.qty + alloc(32) > (select max(price) from prices) k, avg(p.price) a " +
                    "from ((select * from trades union all select * from trades) timestamp(ts)) t " +
                    "horizon join prices p on (t.sym = p.sym) list (0) as h " +
                    "group by k")
                    .fails(-1, "left-hand side of HORIZON JOIN can only be a table with an optional filter");
        });
    }

    @Test
    public void testMultiHorizonJoinPostAssemblyFailureFreesKeyFunctions() throws Exception {
        // Multi-slave counterpart of testHorizonJoinPostAssemblyFailureFreesKeyFunctions: the
        // multi-HORIZON generator assembles the projection (including the cursor-comparison key)
        // before the single-threaded path validates that the master supports random access. The
        // catch must free the extracted owner key functions. alloc() places tracked native memory
        // inside the key chain so assertMemoryLeak() sees the leak.
        assertMemoryLeak(() -> {
            execute("create table trades (sym symbol, qty double, ts timestamp) timestamp(ts) partition by day");
            execute("create table prices (ts timestamp, sym symbol, price double) timestamp(ts)");
            execute("create table asks (ts timestamp, sym symbol, ask double) timestamp(ts)");
            assertQuery("select t.qty + alloc(32) > (select max(price) from prices) k, avg(p.price) a, avg(a2.ask) b " +
                    "from ((select * from trades union all select * from trades) timestamp(ts)) t " +
                    "horizon join prices p on (t.sym = p.sym) horizon join asks a2 on (t.sym = a2.sym) list (0) as h " +
                    "group by k")
                    .fails(-1, "left-hand side of HORIZON JOIN can only be a table with an optional filter");
        });
    }

    @Test
    public void testHorizonJoinAggregateSubQueryKey() throws Exception {
        // regression: a cursor-comparison GROUP BY key whose scalar sub-query itself aggregates
        // recursively re-enters group-by generation while generateHorizonJoinFactory holds its
        // projection scratch state; the factory must stay keyed and produce correct groups
        assertMemoryLeak(() -> {
            execute("create table trades as (" +
                    "select 'A'::symbol sym, x::double qty, timestamp_sequence(1000000, 1000000) ts" +
                    " from long_sequence(10)" +
                    ") timestamp(ts) partition by day");
            execute("create table prices (ts timestamp, sym symbol, price double) timestamp(ts)");
            execute("insert into prices values (0, 'A', 5.0)");
            final String query = "select t.qty > (select avg(price) from prices) k, avg(p.price) a " +
                    "from trades t horizon join prices p on (t.sym = p.sym) list (0) as h " +
                    "group by k order by k";
            assertQuery(query)
                    .noLeakCheck()
                    .assertsPlanContaining("Horizon Join");
            // avg(prices.price) = 5.0 -> qty 1..5 false, 6..10 true; both groups see price 5.0
            assertQuery(query)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            k\ta
                            false\t5.0
                            true\t5.0
                            """);
        });
    }

    @Test
    public void testMultiHorizonJoinAggregateSubQueryKey() throws Exception {
        // multi-slave counterpart of testHorizonJoinAggregateSubQueryKey, covering
        // generateMultiHorizonJoinFactory's projection scratch state
        assertMemoryLeak(() -> {
            execute("create table trades as (" +
                    "select 'A'::symbol sym, x::double qty, timestamp_sequence(1000000, 1000000) ts" +
                    " from long_sequence(10)" +
                    ") timestamp(ts) partition by day");
            execute("create table prices (ts timestamp, sym symbol, price double) timestamp(ts)");
            execute("insert into prices values (0, 'A', 5.0)");
            execute("create table asks (ts timestamp, sym symbol, ask double) timestamp(ts)");
            execute("insert into asks values (0, 'A', 7.0)");
            final String query = "select t.qty > (select avg(price) from prices) k, avg(p.price) a, avg(a2.ask) b " +
                    "from trades t horizon join prices p on (t.sym = p.sym) horizon join asks a2 on (t.sym = a2.sym) list (0) as h " +
                    "group by k order by k";
            assertQuery(query)
                    .noLeakCheck()
                    .assertsPlanContaining("Multi Horizon Join");
            // avg(prices.price) = 5.0 -> qty 1..5 false, 6..10 true; both groups see 5.0 and 7.0
            assertQuery(query)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            k\ta\tb
                            false\t5.0\t7.0
                            true\t5.0\t7.0
                            """);
        });
    }

    @Test
    public void testGroupByCursorComparisonKey() throws Exception {
        // regression: compiling the sub-query of a cursor comparison used as a GROUP BY key must not
        // corrupt the outer projection scratch state of the code generator (used to throw
        // ArrayIndexOutOfBoundsException from extractVirtualFunctionsFromProjection)
        assertMemoryLeak(() -> {
            execute("create table t as (select x::double d from long_sequence(10000))");
            // avg(d) = 5000.5 -> two groups of 5000 rows each
            assertQuery("select d > (select avg(d) from t) b, count() c from t group by b order by b")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            b\tc
                            false\t5000
                            true\t5000
                            """);
            assertQuery("select d < (select avg(d) from t) b, count() c from t group by b order by b")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            b\tc
                            false\t5000
                            true\t5000
                            """);
        });
    }

    @Test
    public void testCombinedWithIntervalFilter() throws Exception {
        // mirrors the motivating example: an interval filter plus a scalar sub-query predicate,
        // where the sub-query itself carries the same interval filter
        assertMemoryLeak(() -> {
            execute(
                    "create table trades as (" +
                            "  select x::double price, timestamp_sequence('2024-01-01', 60000000) ts" +
                            "  from long_sequence(100)" +
                            ") timestamp(ts) partition by day"
            );
            // prices 1..100 within 2024-01-01, avg = 50.5 -> price > 50.5 -> 50 rows
            assertQuery("select count() c from trades " +
                    "where ts in '2024-01-01' and price > (select avg(price) from trades where ts in '2024-01-01')")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("c\n50\n");
        });
    }

    @Test
    public void testBareLiteralNullComparison() throws Exception {
        // End-to-end guard for the ColumnType NULL->CURSOR overload fix: a bare `null` literal is a scalar,
        // never a cursor. `price <= null` (i.e. not(price > null)) must compile to a scalar null-comparison
        // instead of binding to the `>(?C)` cursor-comparison factory and blowing up on
        // getRecordCursorFactory() of the NULL constant. The existing null tests all use (select null...),
        // a different code path, so this pins the bare-literal end-to-end path.
        assertBareNullBehavior("double", "price");
    }

    @Test
    public void testBareNullLeftOperandComparison() throws Exception {
        // A bare NULL literal on the left of a numeric scalar sub-query comparison must compile
        // and follow QuestDB's NULL comparison convention (see LtNullComparisonTest): the strict
        // forms match no rows when either side is NULL; the inclusive forms match only when both
        // sides are NULL, i.e. when the scalar is NULL or the sub-query yields no rows.
        assertMemoryLeak(() -> {
            execute("create table t as (select x::int i from long_sequence(3))");

            // non-null scalar: no direction matches
            assertQuery("select i from t where null > (select max(i) from t)")
                    .noLeakCheck()
                    .returns("i\n");
            assertQuery("select i from t where null < (select max(i) from t)")
                    .noLeakCheck()
                    .returns("i\n");
            assertQuery("select i from t where null >= (select max(i) from t)")
                    .noLeakCheck()
                    .returns("i\n");
            assertQuery("select i from t where null <= (select max(i) from t)")
                    .noLeakCheck()
                    .returns("i\n");

            // NULL scalar: null equals null -> only the inclusive forms match, and they match
            // every row because the left operand is always NULL
            assertQuery("select i from t where null > (select null::int)")
                    .noLeakCheck()
                    .returns("i\n");
            assertQuery("select i from t where null < (select null::int)")
                    .noLeakCheck()
                    .returns("i\n");
            assertQuery("select i from t where null >= (select null::int)")
                    .noLeakCheck()
                    .returns("""
                            i
                            1
                            2
                            3
                            """);
            assertQuery("select i from t where null <= (select null::int)")
                    .noLeakCheck()
                    .returns("""
                            i
                            1
                            2
                            3
                            """);

            // empty sub-query: the cached scalar is NULL, same outcome as the NULL scalar
            assertQuery("select i from t where null > (select i from t where i < 0)")
                    .noLeakCheck()
                    .returns("i\n");
            assertQuery("select i from t where null < (select i from t where i < 0)")
                    .noLeakCheck()
                    .returns("i\n");
            assertQuery("select i from t where null >= (select i from t where i < 0)")
                    .noLeakCheck()
                    .returns("""
                            i
                            1
                            2
                            3
                            """);
            assertQuery("select i from t where null <= (select i from t where i < 0)")
                    .noLeakCheck()
                    .returns("""
                            i
                            1
                            2
                            3
                            """);
        });
    }

    @Test
    public void testCursorOnLeftIsSupportedViaSwap() throws Exception {
        // (select ...) > col is supported: the optimizer swaps the operands so the cursor becomes the
        // right-hand scalar sub-query. Pin the correctness of the swapped comparison so it can never
        // silently degrade into an internal failure.
        assertMemoryLeak(() -> {
            execute("create table t as (select x::double price from long_sequence(10))");
            // (avg = 5.5) > price -> price < 5.5 -> 1..5
            assertQuery("select price from t where (select avg(price) from t) > price")
                    .noLeakCheck()
                    .returns("price\n1.0\n2.0\n3.0\n4.0\n5.0\n");
            // (avg = 5.5) < price -> price > 5.5 -> 6..10
            assertQuery("select price from t where (select avg(price) from t) < price")
                    .noLeakCheck()
                    .returns("price\n6.0\n7.0\n8.0\n9.0\n10.0\n");
        });
    }

    @Test
    public void testCursorVsCursorFailsCleanly() throws Exception {
        // Comparing two scalar sub-queries has no supporting factory; it must surface a clean
        // "no matching operator" error rather than an internal failure.
        assertMemoryLeak(() -> {
            execute("create table t as (select x::double price from long_sequence(10))");
            assertQuery("select price from t where (select max(price) from t) > (select min(price) from t)")
                    .fails(53, "there is no matching operator `>` with the argument types: CURSOR > CURSOR");
            assertQuery("select price from t where (select max(price) from t) < (select min(price) from t)")
                    .fails(53, "there is no matching operator `<` with the argument types: CURSOR < CURSOR");
        });
    }

    @Test
    public void testWalTableCursorPredicate() throws Exception {
        // Correctness of the cursor-scalar predicate on a WAL table (non-async, non-parallel execution
        // path) - the correctness/plan tests elsewhere run only under the parallel async-filter path.
        assertMemoryLeak(() -> {
            execute("create table w (price double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into w select x::double, timestamp_sequence(0, 1000000) from long_sequence(10)");
            drainWalQueue();
            // avg(price) = 5.5 -> price > 5.5 -> 6..10
            assertQuery("select price from w where price > (select avg(price) from w)")
                    .noLeakCheck()
                    .returns("price\n6.0\n7.0\n8.0\n9.0\n10.0\n");
            // negated: price <= 5.5 -> 1..5
            assertQuery("select price from w where price <= (select avg(price) from w)")
                    .noLeakCheck()
                    .returns("price\n1.0\n2.0\n3.0\n4.0\n5.0\n");
        });
    }

    @Test
    public void testEmptyCursorSelectsNoRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select x::double price from long_sequence(10))");
            String empty = "price\n";
            assertQuery("select price from t where price > (select avg(price) from t where 1 <> 1)")
                    .noLeakCheck()
                    .returns(empty);
            assertQuery("select price from t where price < (select avg(price) from t where 1 <> 1)")
                    .noLeakCheck()
                    .returns(empty);
            // negated operator over an empty cursor (value == NaN) must also match no rows
            assertQuery("select price from t where price >= (select avg(price) from t where 1 <> 1)")
                    .noLeakCheck()
                    .returns(empty);
        });
    }

    @Test
    public void testErrorMultipleColumns() throws Exception {
        // the < and > factories duplicate the validation code, so both must be asserted
        assertMemoryLeak(() -> {
            execute("create table t as (select x::double price from long_sequence(10))");
            assertQuery("select price from t where price > (select avg(price), 1 x from t)")
                    .fails(35, "select must provide exactly one column");
            assertQuery("select price from t where price < (select avg(price), 1 x from t)")
                    .fails(35, "select must provide exactly one column");
        });
    }

    @Test
    public void testErrorNonNumericCursorColumn() throws Exception {
        // the < and > factories duplicate the validation code, so both must be asserted
        assertMemoryLeak(() -> {
            execute("create table t as (select x::double price from long_sequence(10))");
            assertQuery("select price from t where price > (select 'abc' from t)")
                    .fails(35, "cannot compare DOUBLE and STRING");
            assertQuery("select price from t where price < (select 'abc' from t)")
                    .fails(35, "cannot compare DOUBLE and STRING");
        });
    }

    @Test
    public void testTypedNumericCursorScalars() throws Exception {
        // pins the BYTE/SHORT/INT readers of the cursor scalar plus the typed-NULL branches
        assertMemoryLeak(() -> {
            execute("create table t as (select x::double price from long_sequence(10))");
            // BYTE cursor scalar
            assertQuery("select price from t where price > (select 5::byte)")
                    .noLeakCheck()
                    .returns("price\n6.0\n7.0\n8.0\n9.0\n10.0\n");
            assertQuery("select price from t where price < (select 5::byte)")
                    .noLeakCheck()
                    .returns("price\n1.0\n2.0\n3.0\n4.0\n");
            // SHORT cursor scalar, boundary via the negated operator (price == 5.0 matches <=)
            assertQuery("select price from t where price > (select 5::short)")
                    .noLeakCheck()
                    .returns("price\n6.0\n7.0\n8.0\n9.0\n10.0\n");
            assertQuery("select price from t where price <= (select 5::short)")
                    .noLeakCheck()
                    .returns("price\n1.0\n2.0\n3.0\n4.0\n5.0\n");
            // INT cursor scalar, boundary via the negated operator (price == 5.0 matches >=)
            assertQuery("select price from t where price < (select 5::int)")
                    .noLeakCheck()
                    .returns("price\n1.0\n2.0\n3.0\n4.0\n");
            assertQuery("select price from t where price >= (select 5::int)")
                    .noLeakCheck()
                    .returns("price\n5.0\n6.0\n7.0\n8.0\n9.0\n10.0\n");
            // typed INT/LONG NULL scalars must map to NaN and match no rows for every operator
            assertQuery("select price from t where price > (select null::int)")
                    .noLeakCheck()
                    .returns("price\n");
            assertQuery("select price from t where price < (select null::int)")
                    .noLeakCheck()
                    .returns("price\n");
            assertQuery("select price from t where price >= (select null::long)")
                    .noLeakCheck()
                    .returns("price\n");
            assertQuery("select price from t where price <= (select null::long)")
                    .noLeakCheck()
                    .returns("price\n");
        });
    }

    @Test
    public void testWorkerCloneEvaluatesFrameWithDonatedCursorState() throws Exception {
        runWithPool((compiler, ctx) -> {
            execute(compiler, "CREATE TABLE src (ts TIMESTAMP)", ctx);
            execute(compiler, "INSERT INTO src VALUES (5000)", ctx);
            execute(
                    compiler,
                    "CREATE TABLE t AS (" +
                            "  SELECT x::double price, timestamp_sequence(0, 1000000) ts" +
                            "  FROM long_sequence(10000)" +
                            ") TIMESTAMP(ts) PARTITION BY DAY",
                    ctx
            );

            final String query = "SELECT sum(test_worker_clone(" +
                    "price::string::double > (SELECT test_timestamp_counter(ts)::long FROM src), price" +
                    ")::int) s FROM t";
            TestTimestampCounterFactory.COUNTER.set(0);
            TestWorkerCloneFunctionFactory.arm(5000);
            try {
                try (RecordCursorFactory factory = compiler.compile(query, ctx).getRecordCursorFactory()) {
                    Assert.assertEquals("owner plus four worker instances", 5, TestWorkerCloneFunctionFactory.created());
                    try (RecordCursor cursor = factory.getCursor(ctx)) {
                        TestUtils.assertCursor("s\n5000\n", cursor, factory.getMetadata(), true, sink);
                    }
                }

                Assert.assertEquals("scalar cursor must execute once", 1, TestTimestampCounterFactory.COUNTER.get());
                Assert.assertEquals("worker clones must observe the donated threshold", 0, TestWorkerCloneFunctionFactory.mismatches());
                Assert.assertTrue("a non-owner thread must evaluate a worker clone", TestWorkerCloneFunctionFactory.workerEvaluations() > 0);
                int totalEvaluations = 0;
                int evaluatedClones = 0;
                for (int i = 0, n = TestWorkerCloneFunctionFactory.created(); i < n; i++) {
                    final int evaluations = TestWorkerCloneFunctionFactory.evaluations(i);
                    totalEvaluations += evaluations;
                    if (i > 0 && evaluations > 0) {
                        evaluatedClones++;
                    }
                }
                Assert.assertEquals("every input row must be evaluated exactly once", 10_000, totalEvaluations);
                Assert.assertTrue("at least one worker clone must evaluate a real frame", evaluatedClones > 0);
            } finally {
                // Unblock only this run on compile, cursor, assertion, or worker-pool failure.
                TestWorkerCloneFunctionFactory.disarm();
            }
        });
    }

    @Test
    public void testWorkerStateSharedExecutesCursorOnceAndRefreshes() throws Exception {
        // a LONG cursor scalar against a DOUBLE left operand -> the double comparison mode of the
        // worker-state contract
        assertWorkerStateSharedBehavior("double", "price", "long");
    }

    @Test
    public void testMultiRowCursorFails() throws Exception {
        // a scalar sub-query yielding more than one row is an error, reported at the sub-query position
        assertMemoryLeak(() -> {
            execute("create table t as (select x::double price from long_sequence(10))");
            assertQuery("select price from t where price > (select x::double from long_sequence(2))")
                    .fails(35, "scalar sub-query returned more than one row");
            assertQuery("select price from t where price < (select x::double from long_sequence(2))")
                    .fails(35, "scalar sub-query returned more than one row");
        });
    }

    @Test
    public void testFloatBindVariableLeftOperand() throws Exception {
        // A FLOAT-typed function (here a bind variable) reaches the cursor-comparison factory as a
        // raw FLOAT - unlike a FLOAT column, which the optimizer widens to DOUBLE before the factory
        // ever sees it. This pins the FLOAT left-operand support in the factory guard: the value is
        // widened to double losslessly via Function#getDouble.
        assertMemoryLeak(() -> {
            execute("create table t as (select x::double price from long_sequence(10))");
            bindVariableService.clear();
            // avg(price) = 5.5; :thr == avg -> strict comparisons select no rows on either side
            bindVariableService.setFloat("thr", 5.5f);
            assertQuery("select price from t where :thr > (select avg(price) from t)")
                    .noLeakCheck()
                    .returns("price\n");
            assertQuery("select price from t where :thr < (select avg(price) from t)")
                    .noLeakCheck()
                    .returns("price\n");
            // :thr > avg -> the constant predicate is true, so every row matches
            bindVariableService.setFloat("thr", 9.5f);
            assertQuery("select price from t where :thr > (select avg(price) from t)")
                    .noLeakCheck()
                    .returns("""
                            price
                            1.0
                            2.0
                            3.0
                            4.0
                            5.0
                            6.0
                            7.0
                            8.0
                            9.0
                            10.0
                            """);
        });
    }

    @Test
    public void testFloatColumnLeftOperand() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select x::float price from long_sequence(10))");
            // avg(price) = 5.5; a FLOAT column is widened to DOUBLE up front, comparison stays exact
            assertQuery("select price from t where price > (select avg(price) from t)")
                    .noLeakCheck()
                    .returns("price\n6.0\n7.0\n8.0\n9.0\n10.0\n");
            assertQuery("select price from t where price < (select avg(price) from t)")
                    .noLeakCheck()
                    .returns("price\n1.0\n2.0\n3.0\n4.0\n5.0\n");
        });
    }

    @Test
    public void testFloatCursorColumnAndBothSides() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select x::double price, x::float fprice from long_sequence(10))");
            // FLOAT cursor scalar on the right (read via Record#getFloat): min(fprice) = 1.0
            assertQuery("select price from t where price > (select min(fprice) from t)")
                    .noLeakCheck()
                    .returns("price\n2.0\n3.0\n4.0\n5.0\n6.0\n7.0\n8.0\n9.0\n10.0\n");
            // FLOAT on both sides: FLOAT column left (widened) vs FLOAT cursor scalar right
            assertQuery("select fprice from t where fprice < (select max(fprice) from t)")
                    .noLeakCheck()
                    .returns("fprice\n1.0\n2.0\n3.0\n4.0\n5.0\n6.0\n7.0\n8.0\n9.0\n");
        });
    }

    @Test
    public void testGreaterThan() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select x::double price from long_sequence(10))");
            // avg(price) = 5.5
            assertQuery("select price from t where price > (select avg(price) from t)")
                    .noLeakCheck()
                    .returns("""
                            price
                            6.0
                            7.0
                            8.0
                            9.0
                            10.0
                            """);
        });
    }

    @Test
    public void testGreaterThanOrEqualNegated() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select x::double price from long_sequence(10))");
            // avg(price) = 5.5 -> price >= 5.5 == price > 5.5 for integer prices
            assertQuery("select price from t where price >= (select avg(price) from t)")
                    .noLeakCheck()
                    .returns("""
                            price
                            6.0
                            7.0
                            8.0
                            9.0
                            10.0
                            """);
        });
    }

    @Test
    public void testLessThan() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select x::double price from long_sequence(10))");
            // avg(price) = 5.5
            assertQuery("select price from t where price < (select avg(price) from t)")
                    .noLeakCheck()
                    .returns("""
                            price
                            1.0
                            2.0
                            3.0
                            4.0
                            5.0
                            """);
        });
    }

    @Test
    public void testLessThanOrEqualNegated() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select x::double price from long_sequence(10))");
            // avg(price) = 5.5 -> price <= 5.5 == price < 5.5 for integer prices
            assertQuery("select price from t where price <= (select avg(price) from t)")
                    .noLeakCheck()
                    .returns("""
                            price
                            1.0
                            2.0
                            3.0
                            4.0
                            5.0
                            """);
        });
    }

    @Test
    public void testLongCursorColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select x::double price from long_sequence(10))");
            // sum(x) over 1..3 = 6 -> prices 7..10 cross the LONG threshold, 1..6 do not; a
            // broken LONG reader (e.g. one that always yields NaN) matches no rows and fails here
            assertQuery("select price from t where price > (select sum(x) from long_sequence(3))")
                    .noLeakCheck()
                    .returns("""
                            price
                            7.0
                            8.0
                            9.0
                            10.0
                            """);
            // the negated inclusive form across the same LONG threshold selects the complement
            assertQuery("select price from t where price <= (select sum(x) from long_sequence(3))")
                    .noLeakCheck()
                    .returns("""
                            price
                            1.0
                            2.0
                            3.0
                            4.0
                            5.0
                            6.0
                            """);
            // sum(x) over 1..10 = 55, so no price > 55
            assertQuery("select price from t where price > (select sum(x) from long_sequence(10))")
                    .noLeakCheck()
                    .returns("price\n");
            // count() = 10, price > 10 -> none; price > 9.x
            assertQuery("select price from t where price > (select count() from t)")
                    .noLeakCheck()
                    .returns("price\n");
        });
    }

    @Test
    public void testNullCursorSelectsNoRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select x::double price from long_sequence(10))");
            String empty = "price\n";
            assertQuery("select price from t where price > (select null::double)")
                    .noLeakCheck()
                    .returns(empty);
            assertQuery("select price from t where price < (select null::double)")
                    .noLeakCheck()
                    .returns(empty);
            assertQuery("select price from t where price > (select null)")
                    .noLeakCheck()
                    .returns(empty);
            // negated operators exercise the hand-rolled null-under-negation branch (value == NaN)
            assertQuery("select price from t where price >= (select null::double)")
                    .noLeakCheck()
                    .returns(empty);
            assertQuery("select price from t where price <= (select null::double)")
                    .noLeakCheck()
                    .returns(empty);
        });
    }

    @Test
    public void testNullLeftColumn() throws Exception {
        // long_sequence never yields null cells, so the null LEFT-column path needs an explicit null.
        // A null left value must never match a non-null cursor scalar (any operator), and must follow
        // QuestDB's null == null convention against a null cursor: >= and <= match, strict > / < do not.
        assertMemoryLeak(() -> {
            execute("create table t (id int, price double)");
            execute("insert into t values (1, null), (2, 5.0), (3, 8.0)");
            // null-left (id 1) is excluded for every operator against a non-null cursor
            assertQuery("select id from t where price > (select min(price) from t)") // > 5
                    .noLeakCheck()
                    .returns("id\n3\n");
            assertQuery("select id from t where price < (select max(price) from t)") // < 8
                    .noLeakCheck()
                    .returns("id\n2\n");
            assertQuery("select id from t where price >= (select max(price) from t)") // >= 8
                    .noLeakCheck()
                    .returns("id\n3\n");
            assertQuery("select id from t where price <= (select min(price) from t)") // <= 5
                    .noLeakCheck()
                    .returns("id\n2\n");
            // null == null: a null left value matches a null cursor for >= and <= only
            assertQuery("select id from t where price >= (select null::double)")
                    .noLeakCheck()
                    .returns("id\n1\n");
            assertQuery("select id from t where price <= (select null::double)")
                    .noLeakCheck()
                    .returns("id\n1\n");
            assertQuery("select id from t where price > (select null::double)")
                    .noLeakCheck()
                    .returns("id\n");
            assertQuery("select id from t where price < (select null::double)")
                    .noLeakCheck()
                    .returns("id\n");
        });
    }

    @Test
    public void testParallelGroupByWithCursorPredicate() throws Exception {
        runWithPool((compiler, ctx) -> {
            execute(
                    compiler,
                    "create table trades as (" +
                            "  select (x % 10)::int grp, x::double price, timestamp_sequence(0, 1000000) ts" +
                            "  from long_sequence(100000)" +
                            ") timestamp(ts) partition by day",
                    ctx
            );

            // plan: the cursor predicate is pushed into the parallel (async) group by filter
            assertQuery("select grp, count() c from trades where price > (select avg(price) from trades) order by grp")
                    .withContext(ctx)
                    .noLeakCheck()
                    .assertsPlan("""
                            Encode sort light
                              keys: [grp]
                                Async Group By workers: 4
                                  keys: [grp]
                                  values: [count(*)]
                                  filter: price [thread-safe] > cursor\s
                                    Async Group By workers: 4
                                      vectorized: true
                                      values: [avg(price)]
                                      filter: null
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: trades
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: trades
                            """);

            // avg(price) = 50000.5 -> x in 50001..100000 -> 5000 rows per grp
            assertQuery("select grp, count() c from trades where price > (select avg(price) from trades) order by grp")
                    .withContext(ctx)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            grp\tc
                            0\t5000
                            1\t5000
                            2\t5000
                            3\t5000
                            4\t5000
                            5\t5000
                            6\t5000
                            7\t5000
                            8\t5000
                            9\t5000
                            """);
        });
    }

    @Test
    public void testPlanAsyncFilterStateShared() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select x::double price from long_sequence(10))");
            // non-thread-safe left operand forces per-worker clones and state sharing
            assertQuery("select price from t where price::string::double > (select avg(price) from t)")
                    .noLeakCheck()
                    .assertsPlan("""
                            Async Filter workers: 1
                              filter: price::string::double > cursor\s
                                Async Group By workers: 1
                                  vectorized: true
                                  values: [avg(price)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: t [state-shared]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                            """);
        });
    }

    @Test
    public void testPlanNestedListBaseMetadataRestored() throws Exception {
        // The outer GROUP BY values list renders with base metadata. Its first aggregate contains
        // a scalar sub-query whose GROUP BY values use the nested ObjList optAttr overload. That
        // overload must restore the ambient mode before the later vwap element renders its columns.
        assertMemoryLeak(() -> {
            execute("create table tab (price double, qty double, ts timestamp) timestamp(ts) partition by day");
            assertQuery("select avg((qty > (select avg(price) from tab))::int) a, vwap(price, qty) v from tab")
                    .assertsPlan("""
                            Async Group By workers: 1
                              vectorized: false
                              values: [avg(qty [thread-safe] > cursor\s
                                Async Group By workers: 1
                                  vectorized: true
                                  values: [avg(price)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: tab::int),vwap(price,qty)]
                              filter: null
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: tab
                            """);
        });
    }

    @Test
    public void testPlanNestedSingleValueBaseMetadataRestored() throws Exception {
        // The values list of a GROUP BY plan renders with base metadata. The first aggregate's
        // argument contains a scalar sub-query whose own async-filter plan goes through the
        // single-Plannable optAttr(name, value, useBaseMetadata) overload; that nested render
        // must restore the ambient base-metadata mode, or the second aggregate's column names
        // render from the wrong metadata. The sibling ObjList overload is covered elsewhere;
        // this pins the single-value overload.
        assertMemoryLeak(() -> {
            execute("create table tab (price double, qty double, ts timestamp) timestamp(ts) partition by day");
            // the string cast keeps the nested filter off the JIT path so the plan is stable
            // across platforms
            assertQuery("select avg((qty > (select avg(price) from tab where price::string::double > 0))::int) a, vwap(price, qty) v from tab")
                    .assertsPlan("""
                            Async Group By workers: 1
                              vectorized: false
                              values: [avg(qty [thread-safe] > cursor\s
                                Async Group By workers: 1
                                  vectorized: false
                                  values: [avg(price)]
                                  filter: 0<price::string::double
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: tab::int),vwap(price,qty)]
                              filter: null
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: tab
                            """);
        });
    }

    @Test
    public void testNestedPageFrameSequenceConstructorRollbackPreservesPrimary() throws Exception {
        assertMemoryLeak(() -> {
            final RuntimeException primary = new RuntimeException("sequence constructor primary");
            final RuntimeException clearFailure = new RuntimeException("atom clear");
            final RuntimeException closeFailure = new RuntimeException("atom close");
            final CountingStatefulAtom atom = new CountingStatefulAtom(clearFailure, closeFailure);
            final CairoConfiguration throwingConfiguration = new CairoConfigurationWrapper(configuration) {
                @Override
                public @NotNull MillisecondClock getMillisecondClock() {
                    throw primary;
                }
            };

            try {
                new UnorderedPageFrameSequence<>(
                        engine,
                        throwingConfiguration,
                        engine.getMessageBus(),
                        atom,
                        (_, _, _, _, _, _) -> {
                        },
                        1
                );
                Assert.fail("sequence construction should have failed");
            } catch (Throwable failure) {
                Assert.assertSame(primary, failure);
                Assert.assertArrayEquals(new Throwable[]{clearFailure}, failure.getSuppressed());
                Assert.assertArrayEquals(new Throwable[]{closeFailure}, clearFailure.getSuppressed());
            }
            Assert.assertEquals(1, atom.clearCount);
            Assert.assertEquals(1, atom.closeCount);
        });
    }

    @Test
    public void testPlanAsyncFilterThreadSafe() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select x::double price from long_sequence(10))");
            assertQuery("select price from t where price > (select avg(price) from t)")
                    .noLeakCheck()
                    .assertsPlan("""
                            Async Filter workers: 1
                              filter: price [thread-safe] > cursor\s
                                Async Group By workers: 1
                                  vectorized: true
                                  values: [avg(price)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: t
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                            """);
        });
    }

    @Test
    public void testParallelAsyncFilterAndKeyedSumLessThan() throws Exception {
        runWithPool((compiler, ctx) -> {
            execute(
                    compiler,
                    "create table trades as (" +
                            "  select (x % 10)::int grp, x::double price, timestamp_sequence(0, 1000000) ts" +
                            "  from long_sequence(100000)" +
                            ") timestamp(ts) partition by day",
                    ctx
            );

            // (1) a plain filter with the cursor predicate must run on the async filter concurrently
            assertQuery("select ts, price from trades where price < (select avg(price) from trades)")
                    .withContext(ctx)
                    .noLeakCheck()
                    .assertsPlan("""
                            Async Filter workers: 4
                              filter: price [thread-safe] < cursor\s
                                Async Group By workers: 4
                                  vectorized: true
                                  values: [avg(price)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: trades
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: trades
                            """);

            // filter correctness: avg(price) = 50000.5 -> price in 1..50000 -> 50000 rows
            assertQuery("select count() c from trades where price < (select avg(price) from trades)")
                    .withContext(ctx)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("c\n50000\n");

            // (2) a keyed aggregate (sum) over the same predicate must stay parallel (Async Group By workers: 4)
            assertQuery("select grp, sum(price) s from trades where price < (select avg(price) from trades) order by grp")
                    .withContext(ctx)
                    .noLeakCheck()
                    .assertsPlan("""
                            Encode sort light
                              keys: [grp]
                                Async Group By workers: 4
                                  keys: [grp]
                                  values: [sum(price)]
                                  filter: price [thread-safe] < cursor\s
                                    Async Group By workers: 4
                                      vectorized: true
                                      values: [avg(price)]
                                      filter: null
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: trades
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: trades
                            """);

            assertQuery("select grp, sum(price) s from trades where price < (select avg(price) from trades) order by grp")
                    .withContext(ctx)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            grp	s
                            0	1.25025E8
                            1	1.2498E8
                            2	1.24985E8
                            3	1.2499E8
                            4	1.24995E8
                            5	1.25E8
                            6	1.25005E8
                            7	1.2501E8
                            8	1.25015E8
                            9	1.2502E8
                            """);
        });
    }

    private static void assertHorizonCompileRollback(
            SqlCompiler compiler,
            SqlExecutionContext ctx,
            String query
    ) {
        TestFaultFunctionFactory.armCloseFailures();
        // Owner assembly compiles once; fail on the second worker clone after one worker succeeds.
        TestFaultFunctionFactory.armToFailAfterCompiles(2);
        try {
            compiler.compile(query, ctx);
            Assert.fail("compilation should have failed with the injected fault");
        } catch (Throwable failure) {
            Assert.assertSame(TestFaultFunctionFactory.lastCompileFailure(), failure);
            Assert.assertEquals(TestFaultFunctionFactory.created(), TestFaultFunctionFactory.closeCalls());
            Assert.assertEquals(2, TestFaultFunctionFactory.created());
            Assert.assertEquals(2, TestFaultFunctionFactory.closeFailureCount());
            final Throwable[] suppressed = failure.getSuppressed();
            Assert.assertEquals(2, suppressed.length);
            // Failures are assigned at function creation: owner first, then the successful worker clone.
            // The worker compiler rolls its clone back before the horizon generator closes the owner.
            Assert.assertSame(TestFaultFunctionFactory.closeFailure(1), suppressed[0]);
            Assert.assertSame(TestFaultFunctionFactory.closeFailure(0), suppressed[1]);
            Assert.assertEquals("all master and slave factories must release their readers", 0, engine.getBusyReaderCount());
        } finally {
            TestFaultFunctionFactory.disarm();
        }
    }

    private void assertHorizonConstructorRollback(boolean isMulti) {
        final RuntimeException primary = new RuntimeException("constructor primary");
        final RuntimeException ownerFilterFailure = new RuntimeException("owner filter close");
        final RuntimeException workerFilterFailure = new RuntimeException("worker filter close");
        final RuntimeException masterFailure = new RuntimeException("master close");
        final RuntimeException slaveFailure = new RuntimeException("slave close");
        final RuntimeException groupByFailure = new RuntimeException("group by close");

        final CountingFactory masterFactory = new CountingFactory(timestampMetadata(), null, masterFailure);
        final CountingFactory firstSlaveFactory = new CountingFactory(timestampMetadata(), primary, slaveFailure);
        final CountingFactory secondSlaveFactory = new CountingFactory(timestampMetadata(), null, null);
        final CountingBooleanFunction ownerFilter = new CountingBooleanFunction(ownerFilterFailure);
        final CountingBooleanFunction workerFilter0 = new CountingBooleanFunction(workerFilterFailure);
        final CountingBooleanFunction workerFilter1 = new CountingBooleanFunction(null);
        final ObjList<Function> perWorkerFilters = new ObjList<>();
        perWorkerFilters.add(workerFilter0);
        perWorkerFilters.add(workerFilter1);
        final CountingGroupByFunction groupByFunction = new CountingGroupByFunction(groupByFailure);
        final ObjList<GroupByFunction> groupByFunctions = new ObjList<>();
        groupByFunctions.add(groupByFunction);
        final JoinRecordMetadata horizonMetadata = new JoinRecordMetadata(configuration, 0);
        final GenericRecordMetadata outputMetadata = new GenericRecordMetadata();
        final AsyncHorizonJoinResources resources = new AsyncHorizonJoinResources(
                null,
                null,
                null,
                null,
                null,
                ownerFilter,
                null,
                perWorkerFilters
        );

        try {
            if (isMulti) {
                final ObjList<HorizonJoinSlaveState> slaveStates = new ObjList<>();
                slaveStates.add(new HorizonJoinSlaveState(firstSlaveFactory, 1, 1, null, 1, null, null));
                slaveStates.add(new HorizonJoinSlaveState(secondSlaveFactory, 1, 1, null, 1, null, null));
                new AsyncMultiHorizonJoinNotKeyedRecordCursorFactory(
                        configuration,
                        new BytecodeAssembler(),
                        engine,
                        engine.getMessageBus(),
                        outputMetadata,
                        horizonMetadata,
                        masterFactory,
                        slaveStates,
                        null,
                        new Class[2],
                        new Class[2],
                        new long[]{0},
                        0,
                        groupByFunctions,
                        1,
                        new int[0],
                        new int[0],
                        resources,
                        2
                );
            } else {
                new AsyncHorizonJoinNotKeyedRecordCursorFactory(
                        configuration,
                        new BytecodeAssembler(),
                        engine,
                        engine.getMessageBus(),
                        outputMetadata,
                        horizonMetadata,
                        masterFactory,
                        firstSlaveFactory,
                        new long[]{0},
                        0,
                        groupByFunctions,
                        1,
                        null,
                        null,
                        null,
                        1,
                        null,
                        null,
                        new int[0],
                        new int[0],
                        resources,
                        2
                );
            }
            Assert.fail("constructor should have failed");
        } catch (Throwable failure) {
            Assert.assertSame(primary, failure);
            Assert.assertEquals(2, failure.getSuppressed().length);
            Assert.assertSame(ownerFilterFailure, failure.getSuppressed()[0]);
            Assert.assertArrayEquals(new Throwable[]{workerFilterFailure}, ownerFilterFailure.getSuppressed());
            Assert.assertSame(masterFailure, failure.getSuppressed()[1]);
            Assert.assertSame(slaveFailure, masterFailure.getSuppressed()[0]);
            Assert.assertSame(groupByFailure, masterFailure.getSuppressed()[1]);
        }

        Assert.assertEquals(1, ownerFilter.closeCount);
        Assert.assertEquals(1, workerFilter0.closeCount);
        Assert.assertEquals(1, workerFilter1.closeCount);
        Assert.assertEquals(1, groupByFunction.closeCount);
        Assert.assertEquals(1, masterFactory.closeCount);
        Assert.assertEquals(1, firstSlaveFactory.closeCount);
        Assert.assertEquals(isMulti ? 1 : 0, secondSlaveFactory.closeCount);
    }

    private void assertHorizonEarlyMetadataRollback() {
        final RuntimeException primary = new RuntimeException("metadata primary");
        final RuntimeException masterFailure = new RuntimeException("metadata master close");
        final RuntimeException slaveFailure = new RuntimeException("metadata slave close");
        final RuntimeException ownerGroupFailure = new RuntimeException("metadata owner group close");
        final RuntimeException workerGroupFailure = new RuntimeException("metadata worker group close");
        final RuntimeException workerKeyFailure = new RuntimeException("metadata worker key close");
        final RuntimeException compiledFilterFailure = new RuntimeException("metadata compiled filter close");
        final RuntimeException bindMemoryFailure = new RuntimeException("metadata bind memory close");
        final RuntimeException bindFunctionFailure = new RuntimeException("metadata bind function close");
        final RuntimeException ownerFilterFailure = new RuntimeException("metadata owner filter close");
        final RuntimeException workerFilterFailure = new RuntimeException("metadata worker filter close");

        final CountingFactory masterFactory = new CountingFactory(timestampMetadata(), null, masterFailure, primary);
        final CountingFactory slaveFactory = new CountingFactory(timestampMetadata(), null, slaveFailure);
        final CountingGroupByFunction ownerGroup = new CountingGroupByFunction(ownerGroupFailure);
        final CountingGroupByFunction workerGroup = new CountingGroupByFunction(workerGroupFailure);
        final CountingBooleanFunction workerKey = new CountingBooleanFunction(workerKeyFailure);
        final CountingCompiledFilter compiledFilter = new CountingCompiledFilter(compiledFilterFailure);
        final CountingMemory bindMemory = new CountingMemory(bindMemoryFailure);
        final CountingBooleanFunction bindFunction = new CountingBooleanFunction(bindFunctionFailure);
        final CountingBooleanFunction ownerFilter = new CountingBooleanFunction(ownerFilterFailure);
        final CountingBooleanFunction workerFilter = new CountingBooleanFunction(workerFilterFailure);
        final ObjList<GroupByFunction> ownerGroups = new ObjList<>();
        ownerGroups.add(ownerGroup);
        final ObjList<GroupByFunction> workerGroups = new ObjList<>();
        workerGroups.add(workerGroup);
        final ObjList<ObjList<GroupByFunction>> perWorkerGroups = new ObjList<>();
        perWorkerGroups.add(workerGroups);
        final ObjList<Function> workerKeys = new ObjList<>();
        workerKeys.add(workerKey);
        final ObjList<ObjList<Function>> perWorkerKeys = new ObjList<>();
        perWorkerKeys.add(workerKeys);
        final ObjList<Function> bindFunctions = new ObjList<>();
        bindFunctions.add(bindFunction);
        final ObjList<Function> workerFilters = new ObjList<>();
        workerFilters.add(workerFilter);
        final AsyncHorizonJoinResources resources = new AsyncHorizonJoinResources(
                perWorkerGroups,
                perWorkerKeys,
                compiledFilter,
                bindMemory,
                bindFunctions,
                ownerFilter,
                null,
                workerFilters
        );

        try {
            new AsyncHorizonJoinNotKeyedRecordCursorFactory(
                    configuration,
                    new BytecodeAssembler(),
                    engine,
                    engine.getMessageBus(),
                    new GenericRecordMetadata(),
                    new JoinRecordMetadata(configuration, 0),
                    masterFactory,
                    slaveFactory,
                    new long[]{0},
                    0,
                    ownerGroups,
                    1,
                    null,
                    null,
                    null,
                    1,
                    null,
                    null,
                    new int[0],
                    new int[0],
                    resources,
                    1
            );
            Assert.fail("metadata failure expected");
        } catch (Throwable failure) {
            Assert.assertSame(primary, failure);
            Assert.assertArrayEquals(new Throwable[]{masterFailure}, failure.getSuppressed());
            Assert.assertArrayEquals(
                    new Throwable[]{slaveFailure, workerGroupFailure, ownerGroupFailure},
                    masterFailure.getSuppressed()
            );
            Assert.assertArrayEquals(
                    new Throwable[]{
                            workerKeyFailure,
                            compiledFilterFailure,
                            bindMemoryFailure,
                            bindFunctionFailure,
                            ownerFilterFailure,
                            workerFilterFailure
                    },
                    workerGroupFailure.getSuppressed()
            );
        }

        Assert.assertEquals(1, masterFactory.closeCount);
        Assert.assertEquals(1, slaveFactory.closeCount);
        Assert.assertEquals(1, ownerGroup.closeCount);
        Assert.assertEquals(1, workerGroup.closeCount);
        Assert.assertEquals(1, workerKey.closeCount);
        Assert.assertEquals(1, compiledFilter.closeCount);
        Assert.assertEquals(1, bindMemory.closeCount);
        Assert.assertEquals(1, bindFunction.closeCount);
        Assert.assertEquals(1, ownerFilter.closeCount);
        Assert.assertEquals(1, workerFilter.closeCount);
    }

    private void assertMultiHorizonPartialStateTransferRollback() {
        final RuntimeException primary = new RuntimeException("state conversion primary");
        final RuntimeException masterFailure = new RuntimeException("state master close");
        final RuntimeException firstSlaveFailure = new RuntimeException("state first slave close");
        final RuntimeException secondSlaveFailure = new RuntimeException("state second slave close");
        final RuntimeException thirdSlaveFailure = new RuntimeException("state third slave close");
        final CountingFactory masterFactory = new CountingFactory(timestampMetadata(), null, masterFailure);
        final CountingFactory firstSlave = new CountingFactory(timestampMetadata(), null, firstSlaveFailure);
        final CountingFactory secondSlave = new CountingFactory(timestampMetadata(), null, secondSlaveFailure);
        final CountingFactory thirdSlave = new CountingFactory(timestampMetadata(), null, thirdSlaveFailure);
        final ObjList<HorizonJoinSlaveState> states = new ObjList<>();
        states.add(new CountingSlaveState(firstSlave, null, Integer.MAX_VALUE));
        states.add(new CountingSlaveState(secondSlave, primary, 4));
        states.add(new CountingSlaveState(thirdSlave, null, Integer.MAX_VALUE));
        final ObjList<GroupByFunction> groups = new ObjList<>();
        groups.add(new CountingGroupByFunction(null));
        final AsyncHorizonJoinResources resources = new AsyncHorizonJoinResources(
                null, null, null, null, null, null, null, null
        );

        try {
            new AsyncMultiHorizonJoinNotKeyedRecordCursorFactory(
                    configuration,
                    new BytecodeAssembler(),
                    engine,
                    engine.getMessageBus(),
                    new GenericRecordMetadata(),
                    new JoinRecordMetadata(configuration, 0),
                    masterFactory,
                    states,
                    null,
                    new Class[3],
                    new Class[3],
                    new long[]{0},
                    0,
                    groups,
                    1,
                    new int[0],
                    new int[0],
                    resources,
                    2
            );
            Assert.fail("state conversion failure expected");
        } catch (Throwable failure) {
            Assert.assertSame(primary, failure);
            Assert.assertArrayEquals(new Throwable[]{masterFailure}, failure.getSuppressed());
            Assert.assertArrayEquals(
                    new Throwable[]{firstSlaveFailure, secondSlaveFailure, thirdSlaveFailure},
                    masterFailure.getSuppressed()
            );
        }

        Assert.assertEquals(1, masterFactory.closeCount);
        Assert.assertEquals(1, firstSlave.closeCount);
        Assert.assertEquals(1, secondSlave.closeCount);
        Assert.assertEquals(1, thirdSlave.closeCount);
    }

    private void assertSpecialValueMatrix(int scalarId, String... expectedIdLists) throws Exception {
        final String scalar = "(SELECT v FROM double_scalars WHERE id = " + scalarId + ")";
        final String[] predicates = {
                "v > " + scalar,
                "v >= " + scalar,
                "v < " + scalar,
                "v <= " + scalar,
                scalar + " > v",
                scalar + " >= v",
                scalar + " < v",
                scalar + " <= v"
        };
        Assert.assertEquals(predicates.length, expectedIdLists.length);
        for (int i = 0; i < predicates.length; i++) {
            final String expected = expectedIdLists[i].isEmpty()
                    ? "id\n"
                    : "id\n" + expectedIdLists[i].replace(',', '\n') + '\n';
            assertQuery("SELECT id FROM double_values WHERE " + predicates[i] + " ORDER BY id")
                    .noLeakCheck()
                    .returns(expected);
        }
    }

    private void createHorizonJoinTables(SqlCompiler compiler, SqlExecutionContext ctx) throws Exception {
        execute(compiler, "create table src (ts timestamp)", ctx);
        execute(compiler, "insert into src values (5000)", ctx);
        execute(
                compiler,
                "create table trades as (" +
                        "  select 'A'::symbol sym, x::double qty, timestamp_sequence(1000000, 1000000) ts" +
                        "  from long_sequence(10000)" +
                        ") timestamp(ts) partition by day",
                ctx
        );
        execute(compiler, "create table prices (ts timestamp, sym symbol, price double) timestamp(ts)", ctx);
        execute(compiler, "insert into prices values (0, 'A', 100.0)", ctx);
        execute(compiler, "create table asks (ts timestamp, sym symbol, ask double) timestamp(ts)", ctx);
        execute(compiler, "insert into asks values (0, 'A', 200.0)", ctx);
    }

    private static GenericRecordMetadata timestampMetadata() {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("ts", ColumnType.TIMESTAMP));
        metadata.setTimestampIndex(0);
        return metadata;
    }

    private static class CountingBooleanFunction extends BooleanFunction {
        private final RuntimeException closeFailure;
        private int closeCount;

        private CountingBooleanFunction(RuntimeException closeFailure) {
            this.closeFailure = closeFailure;
        }

        @Override
        public void close() {
            closeCount++;
            if (closeFailure != null) {
                throw closeFailure;
            }
        }

        @Override
        public boolean getBool(io.questdb.cairo.sql.Record rec) {
            return true;
        }
    }

    private static class CountingCompiledFilter extends CompiledFilter {
        private final RuntimeException closeFailure;
        private int closeCount;

        private CountingCompiledFilter(RuntimeException closeFailure) {
            this.closeFailure = closeFailure;
        }

        @Override
        public void close() {
            closeCount++;
            throw closeFailure;
        }
    }

    private static class CountingFactory extends AbstractRecordCursorFactory {
        private final RuntimeException closeFailure;
        private final RuntimeException cursorFailure;
        private final RuntimeException metadataFailure;
        private int closeCount;

        private CountingFactory(RecordMetadata metadata, RuntimeException cursorFailure, RuntimeException closeFailure) {
            this(metadata, cursorFailure, closeFailure, null);
        }

        private CountingFactory(
                RecordMetadata metadata,
                RuntimeException cursorFailure,
                RuntimeException closeFailure,
                RuntimeException metadataFailure
        ) {
            super(metadata);
            this.cursorFailure = cursorFailure;
            this.closeFailure = closeFailure;
            this.metadataFailure = metadataFailure;
        }

        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) {
            throw new UnsupportedOperationException();
        }

        @Override
        public RecordMetadata getMetadata() {
            if (metadataFailure != null) {
                throw metadataFailure;
            }
            return super.getMetadata();
        }

        @Override
        public ConcurrentTimeFrameCursor newTimeFrameCursor() {
            if (cursorFailure != null) {
                throw cursorFailure;
            }
            return null;
        }

        @Override
        public boolean recordCursorSupportsRandomAccess() {
            return true;
        }

        @Override
        public boolean supportsTimeFrameCursor() {
            return true;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.type("counting");
        }

        @Override
        protected void _close() {
            closeCount++;
            if (closeFailure != null) {
                throw closeFailure;
            }
        }
    }

    private static class CountingGroupByFunction extends CountLongConstGroupByFunction {
        private final RuntimeException closeFailure;
        private int closeCount;

        private CountingGroupByFunction(RuntimeException closeFailure) {
            this.closeFailure = closeFailure;
        }

        @Override
        public void close() {
            closeCount++;
            if (closeFailure != null) {
                throw closeFailure;
            }
        }
    }

    private static class CountingMemory extends MemoryCARWImpl {
        private final RuntimeException closeFailure;
        private int closeCount;

        private CountingMemory(RuntimeException closeFailure) {
            super(64, 1, MemoryTag.NATIVE_DEFAULT);
            this.closeFailure = closeFailure;
        }

        @Override
        public void close() {
            closeCount++;
            super.close();
            throw closeFailure;
        }
    }

    private static class CountingSlaveState extends HorizonJoinSlaveState {
        private final RuntimeException failure;
        private final int failureCall;
        private int getFactoryCalls;

        private CountingSlaveState(RecordCursorFactory factory, RuntimeException failure, int failureCall) {
            super(factory, 1, 1, null, 1, null, null);
            this.failure = failure;
            this.failureCall = failureCall;
        }

        @Override
        public RecordCursorFactory getFactory() {
            if (++getFactoryCalls == failureCall) {
                throw failure;
            }
            return super.getFactory();
        }
    }

    private static class CountingStatefulAtom implements StatefulAtom {
        private final RuntimeException clearFailure;
        private final RuntimeException closeFailure;
        private int clearCount;
        private int closeCount;

        private CountingStatefulAtom(RuntimeException clearFailure, RuntimeException closeFailure) {
            this.clearFailure = clearFailure;
            this.closeFailure = closeFailure;
        }

        @Override
        public void clear() {
            clearCount++;
            throw clearFailure;
        }

        @Override
        public void close() {
            closeCount++;
            throw closeFailure;
        }
    }

}
