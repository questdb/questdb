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

package io.questdb.test.griffin.engine.functions.bool;

import io.questdb.PropertyKey;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.test.TestCloseCounterFunctionFactory;
import io.questdb.griffin.engine.functions.test.TestRuntimeConstTimestampCounterFactory;
import io.questdb.griffin.engine.functions.test.TestTimestampCounterFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Boundary matrix for the scalar-sub-query cursor BETWEEN overloads
 * ({@code between(NCC)} / {@code between(NCN)} / {@code between(NNC)} in
 * {@link io.questdb.griffin.engine.functions.bool.BetweenTimestampCursorFunctionFactory}) and the
 * designated-timestamp runtime-interval intrinsic these bounds also drive.
 *
 * <p>{@link BetweenTimestampCursorFunctionFactoryTest} already pins the happy path plus an empty /
 * NULL LOWER bound, reversed bounds under BETWEEN, and cross-precision results. This suite fills the
 * remaining boundary cells the finding called out: a NULL / empty UPPER bound, BOTH bounds NULL /
 * empty, the NOT BETWEEN negation of every empty-endpoint case, reversed endpoints under NOT
 * BETWEEN, a NULL endpoint under mixed precision, and the shared-sub-query LONG_NULL case. It also
 * pins the interval-shape (an empty endpoint collapses the designated scan to {@code intervals: []})
 * and the execution contract (the cursor bound is read exactly once and closed exactly once, with
 * the owner's cached bounds shared to per-worker filter clones instead of per-clone re-reads).
 *
 * <p>Semantics under test, verified against production, mirror {@code between(NNN)}: a NULL or empty
 * endpoint makes BETWEEN false for every row (empty result), so NOT BETWEEN is true for every row
 * (all rows), and reversed runtime endpoints normalize via min/max before the comparison.
 */
public class BetweenTimestampCursorBoundaryTest extends AbstractCairoTest {

    @Override
    @Before
    public void setUp() {
        // enables test_timestamp_counter() / test_close_counter() used to pin one-eval / one-close
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
        super.setUp();
    }

    @Test
    public void testBothEndpointsNullOrEmpty() throws Exception {
        // both bounds NULL (or empty): BETWEEN is false for every row, NOT BETWEEN is true for every
        // row - identical to a single NULL endpoint and to between(NNN)
        assertMemoryLeak(() -> {
            createBaseTables();
            execute("CREATE TABLE b_null (lo TIMESTAMP, hi TIMESTAMP)");
            execute("INSERT INTO b_null VALUES (null, null)");

            // both empty sub-queries
            assertQuery("SELECT x FROM t WHERE ts2 BETWEEN (SELECT lo FROM b WHERE 1 <> 1) AND (SELECT hi FROM b WHERE 1 <> 1)")
                    .returns("x\n");
            // both NULL values
            assertQuery("SELECT x FROM t WHERE ts2 BETWEEN (SELECT lo FROM b_null) AND (SELECT hi FROM b_null)")
                    .returns("x\n");
            // NOT BETWEEN negation: every row matches
            assertQuery("SELECT x FROM t WHERE ts2 NOT BETWEEN (SELECT lo FROM b WHERE 1 <> 1) AND (SELECT hi FROM b WHERE 1 <> 1)")
                    .returns("""
                            x
                            0
                            1
                            2
                            3
                            4
                            """);
            assertQuery("SELECT x FROM t WHERE ts2 NOT BETWEEN (SELECT lo FROM b_null) AND (SELECT hi FROM b_null)")
                    .returns("""
                            x
                            0
                            1
                            2
                            3
                            4
                            """);
            // projection: NULL bound is false, never NULL
            assertQuery("SELECT ts2 BETWEEN (SELECT lo FROM b_null) AND (SELECT hi FROM b_null) f FROM t")
                    .expectSize()
                    .returns("""
                            f
                            false
                            false
                            false
                            false
                            false
                            """);
        });
    }

    @Test
    public void testDesignatedTimestampEmptyEndpointCollapsesInterval() throws Exception {
        // On the designated timestamp the cursor BETWEEN drives the runtime interval intrinsic, not
        // the between() function. An empty / NULL endpoint must collapse the runtime interval to the
        // empty set (intervals: []), not leave a full-domain residual scan. Both LOWER and UPPER
        // empty endpoints collapse it, matching the empty-result of the between() function path.
        assertMemoryLeak(() -> {
            createBaseTables();
            execute("CREATE TABLE b_null (lo TIMESTAMP, hi TIMESTAMP)");
            execute("INSERT INTO b_null VALUES (null, '2020-01-01T03:00:00.000000Z')");

            // empty UPPER endpoint
            assertQuery("SELECT x FROM t WHERE ts BETWEEN (SELECT lo FROM b) AND (SELECT hi FROM b WHERE 1 <> 1)")
                    .withPlanContaining("Interval forward scan on: t", "intervals: []")
                    .returns("x\n");
            // empty LOWER endpoint
            assertQuery("SELECT x FROM t WHERE ts BETWEEN (SELECT lo FROM b WHERE 1 <> 1) AND (SELECT hi FROM b)")
                    .withPlanContaining("Interval forward scan on: t", "intervals: []")
                    .returns("x\n");
            // NULL LOWER value collapses it just like an empty cursor
            assertQuery("SELECT x FROM t WHERE ts BETWEEN (SELECT lo FROM b_null) AND (SELECT hi FROM b_null)")
                    .withPlanContaining("Interval forward scan on: t", "intervals: []")
                    .returns("x\n");
        });
    }

    @Test
    public void testDesignatedTimestampNotBetweenNullEndpointImposesNoInterval() throws Exception {
        // NOT BETWEEN with a NULL / empty endpoint is the negation of an all-false predicate, so it
        // matches every row and imposes NO interval restriction - the opposite of the BETWEEN
        // collapse above. The scan must not prune to intervals: [].
        assertMemoryLeak(() -> {
            createBaseTables();
            assertQuery("SELECT x FROM t WHERE ts NOT BETWEEN (SELECT lo FROM b WHERE 1 <> 1) AND (SELECT hi FROM b)")
                    .withPlanNotContaining("intervals: []")
                    .returns("""
                            x
                            0
                            1
                            2
                            3
                            4
                            """);
            assertQuery("SELECT x FROM t WHERE ts NOT BETWEEN (SELECT lo FROM b) AND (SELECT hi FROM b WHERE 1 <> 1)")
                    .withPlanNotContaining("intervals: []")
                    .returns("""
                            x
                            0
                            1
                            2
                            3
                            4
                            """);
        });
    }

    @Test
    public void testDualCursorBoundClosedExactlyOnce() throws Exception {
        // one-close: the compiled cursor bound factory chain (including the sub-query projection that
        // holds the bound value function) must be closed exactly once when the outer factory closes.
        // test_close_counter() counts close() on the runtime-constant bound value, so created ==
        // closeCalls and multiClosed == 0 proves exact-once teardown across both cursor bounds.
        assertMemoryLeak(() -> {
            createBaseTables();
            TestCloseCounterFunctionFactory.reset();
            final String loBound = "2020-01-01T01:00:00.000000Z";
            final String hiBound = "2020-01-01T03:00:00.000000Z";
            final String sql = "SELECT x FROM t WHERE ts2 BETWEEN " +
                    "(SELECT test_close_counter('" + loBound + "') FROM b) AND " +
                    "(SELECT test_close_counter('" + hiBound + "') FROM b)";

            try (RecordCursorFactory factory = select(sql)) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    assertCursor("x\n1\n2\n3\n", cursor, factory.getMetadata(), true);
                }
            }

            Assert.assertTrue(TestCloseCounterFunctionFactory.created(loBound) > 0);
            Assert.assertTrue(TestCloseCounterFunctionFactory.created(hiBound) > 0);
            Assert.assertEquals(
                    TestCloseCounterFunctionFactory.created(loBound),
                    TestCloseCounterFunctionFactory.closeCalls(loBound)
            );
            Assert.assertEquals(
                    TestCloseCounterFunctionFactory.created(hiBound),
                    TestCloseCounterFunctionFactory.closeCalls(hiBound)
            );
            Assert.assertEquals(0, TestCloseCounterFunctionFactory.multiClosed());
        });
    }

    @Test
    public void testDualCursorBoundEvaluatedExactlyOnce() throws Exception {
        // one-evaluation: each cursor bound is read once per execution during init(), not once per
        // row and not once per filter worker clone. test_timestamp_counter() increments once per row
        // the bound sub-query cursor reads, so a single execution reading both bounds counts 2.
        assertMemoryLeak(() -> {
            createBaseTables();
            final String sql = "SELECT x FROM t WHERE ts2 BETWEEN " +
                    "(SELECT test_timestamp_counter(lo) FROM b) AND (SELECT test_timestamp_counter(hi) FROM b)";

            TestTimestampCounterFactory.COUNTER.set(0);
            try (RecordCursorFactory factory = select(sql)) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    assertCursor("x\n1\n2\n3\n", cursor, factory.getMetadata(), true);
                }
            }
            Assert.assertEquals(
                    "both cursor bounds must be read exactly once per execution",
                    2,
                    TestTimestampCounterFactory.COUNTER.get()
            );
        });
    }

    @Test
    public void testFoldedScalarEndpointSharesState() throws Exception {
        // a folded (constant / runtime-constant) non-cursor endpoint must still donate its cached
        // bound epochs to per-worker filter clones. The ::varchar::timestamp round-trip makes the
        // left operand non-thread-safe, forcing the async filter to clone the between() predicate;
        // opening the cursor donates the owner's normalized epochs (offerStateTo), surfaced as
        // [state-shared]. Covers both mixed signatures (constant LOWER + cursor UPPER, and
        // cursor LOWER + constant UPPER).
        assertMemoryLeak(() -> {
            createBaseTables();
            // between(NNC): constant LOWER, cursor UPPER
            assertQuery("SELECT x FROM t WHERE ts2::varchar::timestamp BETWEEN '2020-01-01T01:00:00.000000Z' AND (SELECT hi FROM b)")
                    .withPlanContaining("[state-shared]")
                    .returns("""
                            x
                            1
                            2
                            3
                            """);
            // between(NCN): cursor LOWER, constant UPPER
            assertQuery("SELECT x FROM t WHERE ts2::varchar::timestamp BETWEEN (SELECT lo FROM b) AND '2020-01-01T03:00:00.000000Z'")
                    .withPlanContaining("[state-shared]")
                    .returns("""
                            x
                            1
                            2
                            3
                            """);
        });
    }

    @Test
    public void testHiEndpointNullOrEmpty() throws Exception {
        // a NULL or empty UPPER bound makes BETWEEN false for every row (empty result) and NOT
        // BETWEEN true for every row - the symmetric counterpart to the LOWER-bound cases already
        // covered in BetweenTimestampCursorFunctionFactoryTest
        assertMemoryLeak(() -> {
            createBaseTables();
            execute("CREATE TABLE b_null (lo TIMESTAMP, hi TIMESTAMP)");
            execute("INSERT INTO b_null VALUES ('2020-01-01T01:00:00.000000Z', null)");

            // empty UPPER sub-query
            assertQuery("SELECT x FROM t WHERE ts2 BETWEEN (SELECT lo FROM b) AND (SELECT hi FROM b WHERE 1 <> 1)")
                    .withPlanContaining("between")
                    .returns("x\n");
            // NULL UPPER value
            assertQuery("SELECT x FROM t WHERE ts2 BETWEEN (SELECT lo FROM b_null) AND (SELECT hi FROM b_null)")
                    .returns("x\n");
            // explicit (SELECT null) UPPER bound
            assertQuery("SELECT x FROM t WHERE ts2 BETWEEN (SELECT lo FROM b) AND (SELECT null)")
                    .returns("x\n");
            // NOT BETWEEN negation: every row matches
            assertQuery("SELECT x FROM t WHERE ts2 NOT BETWEEN (SELECT lo FROM b) AND (SELECT hi FROM b WHERE 1 <> 1)")
                    .returns("""
                            x
                            0
                            1
                            2
                            3
                            4
                            """);
            assertQuery("SELECT x FROM t WHERE ts2 NOT BETWEEN (SELECT lo FROM b_null) AND (SELECT hi FROM b_null)")
                    .returns("""
                            x
                            0
                            1
                            2
                            3
                            4
                            """);
        });
    }

    @Test
    public void testMixedPrecisionNullEndpoint() throws Exception {
        // a NULL / empty endpoint under mixed precision (nanosecond bound cursor, microsecond left
        // column, and vice versa) still makes BETWEEN false for every row and NOT BETWEEN true,
        // regardless of the endpoint's native precision
        assertMemoryLeak(() -> {
            createBaseTables();
            execute("CREATE TABLE b_ns (lo TIMESTAMP_NS, hi TIMESTAMP_NS)");
            execute("INSERT INTO b_ns VALUES ('2020-01-01T01:00:00.000000000Z', '2020-01-01T03:00:00.000000000Z')");

            // micro left column, empty nanosecond UPPER bound
            assertQuery("SELECT x FROM t WHERE ts2 BETWEEN (SELECT lo FROM b_ns) AND (SELECT hi FROM b_ns WHERE 1 <> 1)")
                    .returns("x\n");
            // micro left column, empty nanosecond LOWER bound
            assertQuery("SELECT x FROM t WHERE ts2 BETWEEN (SELECT lo FROM b_ns WHERE 1 <> 1) AND (SELECT hi FROM b_ns)")
                    .returns("x\n");
            // NOT BETWEEN negation with the empty nanosecond bound: every row matches
            assertQuery("SELECT x FROM t WHERE ts2 NOT BETWEEN (SELECT lo FROM b_ns) AND (SELECT hi FROM b_ns WHERE 1 <> 1)")
                    .returns("""
                            x
                            0
                            1
                            2
                            3
                            4
                            """);
        });
    }

    @Test
    public void testNonCursorEndpointReadOncePerExecution() throws Exception {
        // the non-cursor endpoint of a mixed cursor BETWEEN is invariant across rows when it is a
        // constant or runtime-constant; it must be read once per execution during init(), not once
        // per outer row. test_rt_const_ts_counter() is a runtime-constant TIMESTAMP leaf that counts
        // every getTimestamp() call, so a single execution reading the folded endpoint once counts 1
        // (the per-row implementation counted one per matched/scanned row instead).
        assertMemoryLeak(() -> {
            createBaseTables();

            // between(NNC): constant LOWER endpoint, cursor UPPER bound
            final String hiSql = "SELECT x FROM t WHERE ts2 BETWEEN " +
                    "test_rt_const_ts_counter('2020-01-01T01:00:00.000000Z') AND (SELECT hi FROM b)";
            TestRuntimeConstTimestampCounterFactory.COUNTER.set(0);
            try (RecordCursorFactory factory = select(hiSql)) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    assertCursor("x\n1\n2\n3\n", cursor, factory.getMetadata(), true);
                }
            }
            Assert.assertEquals(
                    "the constant LOWER endpoint must be read once per execution, not once per row",
                    1,
                    TestRuntimeConstTimestampCounterFactory.COUNTER.get()
            );

            // between(NCN): cursor LOWER bound, constant UPPER endpoint
            final String loSql = "SELECT x FROM t WHERE ts2 BETWEEN " +
                    "(SELECT lo FROM b) AND test_rt_const_ts_counter('2020-01-01T03:00:00.000000Z')";
            TestRuntimeConstTimestampCounterFactory.COUNTER.set(0);
            try (RecordCursorFactory factory = select(loSql)) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    assertCursor("x\n1\n2\n3\n", cursor, factory.getMetadata(), true);
                }
            }
            Assert.assertEquals(
                    "the constant UPPER endpoint must be read once per execution, not once per row",
                    1,
                    TestRuntimeConstTimestampCounterFactory.COUNTER.get()
            );
        });
    }

    @Test
    public void testNonThreadSafeArgSharesState() throws Exception {
        // a non-thread-safe left operand (the ::varchar::timestamp round-trip) forces the async
        // filter to compile a per-worker clone of the between() predicate even at the default
        // sharedQueryWorkerCount = 1; opening the cursor donates the owner's cached bounds to the
        // clone (offerStateTo), which the plan surfaces as [state-shared]. Without this marker the
        // clone protocol never ran and the one-evaluation contract above would be vacuous.
        assertMemoryLeak(() -> {
            createBaseTables();
            assertQuery("SELECT x FROM t WHERE ts2::varchar::timestamp BETWEEN (SELECT lo FROM b) AND (SELECT hi FROM b)")
                    .withPlanContaining("[state-shared]")
                    .returns("""
                            x
                            1
                            2
                            3
                            """);
        });
    }

    @Test
    public void testReversedEndpointsNotBetween() throws Exception {
        // reversed runtime endpoints (lo > hi) normalize via min/max before the comparison, so NOT
        // BETWEEN excludes the normalized [01:00, 03:00] range and keeps only the rows outside it.
        // The between() function path (ts2) and the designated interval path (ts) must agree, and
        // both must match the between(NNN) control with the same constants.
        assertMemoryLeak(() -> {
            createBaseTables();
            // function path: non-designated column
            assertQuery("SELECT x FROM t WHERE ts2 NOT BETWEEN (SELECT hi FROM b) AND (SELECT lo FROM b)")
                    .returns("""
                            x
                            0
                            4
                            """);
            // designated column
            assertQuery("SELECT x FROM t WHERE ts NOT BETWEEN (SELECT hi FROM b) AND (SELECT lo FROM b)")
                    .returns("""
                            x
                            0
                            4
                            """);
            // between(NNN) control with the reversed constants
            assertQuery("SELECT x FROM t WHERE ts2 NOT BETWEEN '2020-01-01T03:00:00.000000Z' AND '2020-01-01T01:00:00.000000Z'")
                    .returns("""
                            x
                            0
                            4
                            """);
        });
    }

    @Test
    public void testSharedSubQueryLongNullBound() throws Exception {
        // the same NULL-yielding sub-query supplies BOTH endpoints. Each bound resolves to LONG_NULL
        // independently, so BETWEEN is false for every row (empty) and NOT BETWEEN is true for every
        // row - the shared sub-query must not corrupt either bound's LONG_NULL sentinel.
        assertMemoryLeak(() -> {
            createBaseTables();
            execute("CREATE TABLE b_null (lo TIMESTAMP, hi TIMESTAMP)");
            execute("INSERT INTO b_null VALUES (null, null)");

            assertQuery("SELECT x FROM t WHERE ts2 BETWEEN (SELECT lo FROM b_null) AND (SELECT lo FROM b_null)")
                    .returns("x\n");
            assertQuery("SELECT x FROM t WHERE ts2 NOT BETWEEN (SELECT lo FROM b_null) AND (SELECT lo FROM b_null)")
                    .returns("""
                            x
                            0
                            1
                            2
                            3
                            4
                            """);
            // designated interval path collapses to the empty set as well
            assertQuery("SELECT x FROM t WHERE ts BETWEEN (SELECT lo FROM b_null) AND (SELECT lo FROM b_null)")
                    .withPlanContaining("Interval forward scan on: t", "intervals: []")
                    .returns("x\n");
        });
    }

    @Test
    public void testWorkerStateSharedReadsBoundsOncePerExecution() throws Exception {
        // The parallel async-filter path clones a non-thread-safe between() predicate per worker
        // and donates the owner's cached bound epochs to every clone (offerStateTo/stateInherited),
        // so the two bound sub-queries execute exactly once per query - not once per worker clone.
        // test_timestamp_counter() increments once per row a bound sub-query cursor reads: one
        // execution reading both bounds counts 2; a broken protocol would count 2 + 2 * cloneCount.
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED, "true");
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 1000);
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_SHARD_COUNT, 4);
        assertMemoryLeak(() -> {
            try (WorkerPool pool = new WorkerPool(() -> 4)) {
                TestUtils.execute(pool, (_, compiler, ctx) -> {
                    execute(compiler, "create table src (lo timestamp, hi timestamp)", ctx);
                    execute(compiler, "insert into src values (2500000000, 7499000000)", ctx);
                    execute(
                            compiler,
                            "create table big_t as (" +
                                    "  select timestamp_sequence(0, 1000000) ts," +
                                    "         timestamp_sequence(0, 1000000) ts2" +
                                    "  from long_sequence(10000)" +
                                    ") timestamp(ts) partition by day",
                            ctx
                    );

                    // ts2 (non-designated) keeps the interval intrinsic out; the
                    // ::varchar::timestamp round-trip makes the arg non-thread-safe,
                    // forcing per-worker clones of the between() function
                    TestTimestampCounterFactory.COUNTER.set(0);
                    try (RecordCursorFactory factory = compiler.compile(
                            "select count() c from big_t where ts2::varchar::timestamp between " +
                                    "(select test_timestamp_counter(lo) from src) and " +
                                    "(select test_timestamp_counter(hi) from src)",
                            ctx
                    ).getRecordCursorFactory()) {
                        // [2500s, 7499s] -> 5000 rows across 10 page frames; correct
                        // classification proves every clone observed the owner's epochs
                        try (RecordCursor cursor = factory.getCursor(ctx)) {
                            TestUtils.assertCursor("c\n5000\n", cursor, factory.getMetadata(), true, sink);
                        }
                        Assert.assertEquals(
                                "both bounds must be read exactly once per execution, not per worker clone",
                                2,
                                TestTimestampCounterFactory.COUNTER.get()
                        );

                        // re-executing the same compiled factory refreshes the cached bounds
                        execute(compiler, "update src set lo = 9000000000, hi = 9999000000", ctx);
                        try (RecordCursor cursor = factory.getCursor(ctx)) {
                            TestUtils.assertCursor("c\n1000\n", cursor, factory.getMetadata(), true, sink);
                        }
                        Assert.assertEquals(4, TestTimestampCounterFactory.COUNTER.get());
                    }
                }, configuration, LOG);
            }
        });
    }

    private void createBaseTables() throws SqlException {
        execute("CREATE TABLE t (ts TIMESTAMP, ts2 TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY");
        execute("""
                INSERT INTO t VALUES
                  ('2020-01-01T00:00:00.000000Z', '2020-01-01T00:00:00.000000Z', 0),
                  ('2020-01-01T01:00:00.000000Z', '2020-01-01T01:00:00.000000Z', 1),
                  ('2020-01-01T02:00:00.000000Z', '2020-01-01T02:00:00.000000Z', 2),
                  ('2020-01-01T03:00:00.000000Z', '2020-01-01T03:00:00.000000Z', 3),
                  ('2020-01-01T04:00:00.000000Z', '2020-01-01T04:00:00.000000Z', 4)
                """);
        execute("CREATE TABLE b (lo TIMESTAMP, hi TIMESTAMP)");
        execute("INSERT INTO b VALUES ('2020-01-01T01:00:00.000000Z', '2020-01-01T03:00:00.000000Z')");
    }
}
