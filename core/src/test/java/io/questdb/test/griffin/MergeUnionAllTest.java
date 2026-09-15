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

package io.questdb.test.griffin;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.union.MergeUnionAllRecordCursorFactory;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class MergeUnionAllTest extends AbstractCairoTest {

    @Test
    public void testCalculateSizeHonorsCircuitBreakerAfterHasNext() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a (px double, ts timestamp) timestamp(ts) partition by day");
            execute("create table b (px double, ts timestamp) timestamp(ts) partition by day");
            execute("insert into a values (10.0, 1), (30.0, 3)");
            execute("insert into b values (20.0, 2), (40.0, 4)");
            try (RecordCursorFactory factory = select(
                    "((select * from a where px > 0.0) union all (select * from b where px > 0.0)) order by ts")) {
                Assert.assertTrue(factory.getBaseFactory() instanceof MergeUnionAllRecordCursorFactory);
                final SqlExecutionContextImpl context = (SqlExecutionContextImpl) sqlExecutionContext;
                final SqlExecutionCircuitBreaker original = context.getCircuitBreaker();
                final AtomicBooleanCircuitBreaker breaker = new AtomicBooleanCircuitBreaker(engine);
                context.with(breaker);
                try (RecordCursor cursor = factory.getCursor(context)) {
                    Assert.assertTrue(cursor.hasNext());
                    breaker.cancel();
                    final RecordCursor.Counter counter = new RecordCursor.Counter();
                    try {
                        cursor.calculateSize(breaker, counter);
                        Assert.fail("expected query cancellation");
                    } catch (CairoException e) {
                        Assert.assertTrue(e.isCancellation());
                        Assert.assertEquals(1, counter.get());
                    }
                    breaker.reset();
                    cursor.calculateSize(breaker, counter);
                    Assert.assertEquals(3, counter.get());
                    Assert.assertFalse(cursor.hasNext());
                } finally {
                    context.with(original);
                }
            }
        });
    }

    @Test
    public void testCalculateSizeWithEmptyBranch() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a (px DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE b (px DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO a VALUES (10.0, 1), (30.0, 3)");
            try (RecordCursorFactory factory = select(
                    "((SELECT * FROM a WHERE px > 0.0) UNION ALL (SELECT * FROM b WHERE px > 0.0)) ORDER BY ts")) {
                Assert.assertTrue(factory.getBaseFactory() instanceof MergeUnionAllRecordCursorFactory);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    final RecordCursor.Counter startedCounter = new RecordCursor.Counter();
                    cursor.calculateSize(sqlExecutionContext.getCircuitBreaker(), startedCounter);
                    Assert.assertEquals(1, startedCounter.get());
                    Assert.assertFalse(cursor.hasNext());

                    cursor.toTop();
                    final RecordCursor.Counter unstartedCounter = new RecordCursor.Counter();
                    cursor.calculateSize(sqlExecutionContext.getCircuitBreaker(), unstartedCounter);
                    Assert.assertEquals(2, unstartedCounter.get());
                    Assert.assertFalse(cursor.hasNext());
                }
            }
        });
    }

    @Test
    public void testCastRequiredMergeWidensNonTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a (px int, ts timestamp) timestamp(ts) partition by day");
            execute("create table b (px double, ts timestamp) timestamp(ts) partition by day");
            execute("insert into a values (1, 1), (3, 3)");
            execute("insert into b values (2.5, 2), (4.5, 4)");
            assertQuery("select px from ((select * from a) union all (select * from b)) order by ts")
                    .withPlanContaining("Union All Merge")
                    .withPlanNotContaining("Encode sort")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            px
                            1.0
                            2.5
                            3.0
                            4.5
                            """);
            assertQuery("select px from ((select * from a) union all (select * from b)) order by ts desc")
                    .withPlanContaining("Union All Merge")
                    .withPlanNotContaining("Encode sort")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            px
                            4.5
                            3.0
                            2.5
                            1.0
                            """);
        });
    }

    @Test
    public void testChainedUnionAllDescMergesFully() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a (px double, ts timestamp) timestamp(ts) partition by day");
            execute("create table b (px double, ts timestamp) timestamp(ts) partition by day");
            execute("create table c (px double, ts timestamp) timestamp(ts) partition by day");
            execute("insert into a values (10.0, 1), (40.0, 4)");
            execute("insert into b values (20.0, 2), (50.0, 5)");
            execute("insert into c values (30.0, 3), (60.0, 6)");
            assertQuery("select px from ((select * from a) union all (select * from b) union all (select * from c)) order by ts desc")
                    .withPlanContaining("Union All Merge")
                    .withPlanNotContaining("Encode sort")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            px
                            60.0
                            50.0
                            40.0
                            30.0
                            20.0
                            10.0
                            """);
        });
    }

    @Test
    public void testChainedUnionAllMergesFully() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a (px double, ts timestamp) timestamp(ts) partition by day");
            execute("create table b (px double, ts timestamp) timestamp(ts) partition by day");
            execute("create table c (px double, ts timestamp) timestamp(ts) partition by day");
            execute("insert into a values (10.0, 1), (40.0, 4)");
            execute("insert into b values (20.0, 2), (50.0, 5)");
            execute("insert into c values (30.0, 3), (60.0, 6)");
            assertQuery("select px from ((select * from a) union all (select * from b) union all (select * from c)) order by ts")
                    .withPlanContaining("Union All Merge")
                    .withPlanNotContaining("Encode sort")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            px
                            10.0
                            20.0
                            30.0
                            40.0
                            50.0
                            60.0
                            """);
        });
    }

    @Test
    public void testDescTiesPreserveABeforeB() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a (px double, ts timestamp) timestamp(ts) partition by day");
            execute("create table b (px double, ts timestamp) timestamp(ts) partition by day");
            execute("insert into a values (30.0, 3), (21.0, 2)");
            execute("insert into b values (22.0, 2), (10.0, 1)");
            assertQuery("select px from ((select * from a) union all (select * from b)) order by ts desc")
                    .withPlanContaining("Union All Merge", "order: [ts desc]")
                    .withPlanNotContaining("Encode sort")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            px
                            30.0
                            21.0
                            22.0
                            10.0
                            """);
        });
    }

    @Test
    public void testDescWindowOverUnionStreams() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a (px double, ts timestamp) timestamp(ts) partition by day");
            execute("create table b (px double, ts timestamp) timestamp(ts) partition by day");
            execute("insert into a values (10.0, 1), (30.0, 3)");
            execute("insert into b values (20.0, 2), (40.0, 4)");
            assertQuery("select px, first_value(px) over (order by ts desc) " +
                    "from ((select * from a order by ts desc) union all (select * from b order by ts desc))")
                    .withPlanContaining("Window", "Union All Merge", "order: [ts desc]", "Frame backward scan on: a", "Frame backward scan on: b")
                    .withPlanNotContaining("CachedWindow")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            px\tfirst_value
                            40.0\t40.0
                            30.0\t40.0
                            20.0\t40.0
                            10.0\t40.0
                            """);
        });
    }

    @Test
    public void testEmptyBranchMerges() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a (px double, ts timestamp) timestamp(ts) partition by day");
            execute("create table b (px double, ts timestamp) timestamp(ts) partition by day");
            execute("insert into a values (10.0, 1), (30.0, 3)");

            // first branch is empty
            assertQuery("select px from ((select * from b) union all (select * from a)) order by ts desc")
                    .withPlanContaining("Union All Merge")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            px
                            30.0
                            10.0
                            """);

            // second branch is empty
            assertQuery("select px from ((select * from a) union all (select * from b)) order by ts")
                    .withPlanContaining("Union All Merge")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            px
                            10.0
                            30.0
                            """);
        });
    }

    @Test
    public void testExplicitOrderByTsMergesAndElidesSort() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a (px double, ts timestamp) timestamp(ts) partition by day");
            execute("create table b (px double, ts timestamp) timestamp(ts) partition by day");
            execute("insert into a values (10.0, 1), (30.0, 3), (50.0, 5)");
            execute("insert into b values (20.0, 2), (40.0, 4)");
            assertQuery("select px from ((select * from a) union all (select * from b)) order by ts")
                    .withPlanContaining("Union All Merge", "order: [ts asc]")
                    .withPlanNotContaining("Encode sort")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            px
                            10.0
                            20.0
                            30.0
                            40.0
                            50.0
                            """);
        });
    }

    @Test
    public void testFilteredBranchStillMerges() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a (px double, ts timestamp) timestamp(ts) partition by day");
            execute("create table b (px double, ts timestamp) timestamp(ts) partition by day");
            execute("insert into a values (10.0, 1), (30.0, 3), (50.0, 5)");
            execute("insert into b values (20.0, 2), (40.0, 4)");
            assertQuery("select px from ((select * from a where px > 15.0) union all (select * from b)) order by ts")
                    .withPlanContaining("Union All Merge")
                    .withPlanNotContaining("Encode sort")
                    .noRandomAccess()
                    .sizeMayVary()
                    .returns("""
                            px
                            20.0
                            30.0
                            40.0
                            50.0
                            """);
        });
    }

    @Test
    public void testInnerUnionAsBranchMergesFully() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a (px double, ts timestamp) timestamp(ts) partition by day");
            execute("create table b (px double, ts timestamp) timestamp(ts) partition by day");
            execute("create table c (px double, ts timestamp) timestamp(ts) partition by day");
            execute("insert into a values (10.0, 1), (40.0, 4)");
            execute("insert into b values (20.0, 2), (50.0, 5)");
            execute("insert into c values (30.0, 3), (60.0, 6)");
            assertQuery("select px from ((select * from a) union all (select * from ((select * from b) union all (select * from c)))) order by ts desc")
                    .withPlanContaining("Union All Merge")
                    .withPlanNotContaining("Encode sort")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            px
                            60.0
                            50.0
                            40.0
                            30.0
                            20.0
                            10.0
                            """);
        });
    }

    @Test
    public void testLimitInBranchPreservesResultSet() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a (px double, ts timestamp) timestamp(ts) partition by day");
            execute("create table b (px double, ts timestamp) timestamp(ts) partition by day");
            execute("insert into a values (10.0, 1), (40.0, 4)");
            execute("insert into b values (20.0, 2), (30.0, 3), (50.0, 5), (60.0, 6)");
            assertQuery("select px from ((select * from a) union all (select * from b limit 3)) order by ts desc")
                    .noRandomAccess()
                    .sizeMayVary()
                    .returns("""
                            px
                            50.0
                            40.0
                            30.0
                            20.0
                            10.0
                            """);
        });
    }

    @Test
    public void testMergePreservesDesignatedTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a (px double, ts timestamp) timestamp(ts) partition by day");
            execute("create table b (px double, ts timestamp) timestamp(ts) partition by day");
            execute("insert into a values (10.0, 1), (30.0, 3)");
            execute("insert into b values (20.0, 2), (40.0, 4)");
            assertQuery("select ts, px from ((select * from a) union all (select * from b)) order by ts")
                    .withPlanContaining("Union All Merge")
                    .timestamp("ts")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            ts\tpx
                            1970-01-01T00:00:00.000001Z\t10.0
                            1970-01-01T00:00:00.000002Z\t20.0
                            1970-01-01T00:00:00.000003Z\t30.0
                            1970-01-01T00:00:00.000004Z\t40.0
                            """);
        });
    }

    @Test
    public void testMixedChainInnerMergesOuterConcats() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a (px double, ts timestamp) timestamp(ts) partition by day");
            execute("create table b (px double, ts timestamp) timestamp(ts) partition by day");
            execute("create table c (px double, ts timestamp_ns) timestamp(ts) partition by day");
            execute("insert into a values (10.0, 1), (40.0, 4)");
            execute("insert into b values (20.0, 2)");
            execute("insert into c values (30.0, 3000)");
            assertQuery("select px from (((select * from a) union all (select * from b)) union all (select * from c)) order by ts")
                    .withPlanContaining("Union All Merge", "Encode sort")
                    .expectSize()
                    .returns("""
                            px
                            10.0
                            20.0
                            30.0
                            40.0
                            """);
        });
    }

    @Test
    public void testMultiKeyWindowOverDoesNotMerge() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a (px double, ts timestamp) timestamp(ts) partition by day");
            execute("create table b (px double, ts timestamp) timestamp(ts) partition by day");
            assertQuery("select first_value(px) over (order by ts, px) from ((select * from a) union all (select * from b))")
                    .assertsPlanNotContaining("Union All Merge");
        });
    }

    @Test
    public void testMultipleWindowsDifferentOrdersDoNotMerge() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a (px double, ts timestamp) timestamp(ts) partition by day");
            execute("create table b (px double, ts timestamp) timestamp(ts) partition by day");
            assertQuery("select first_value(px) over (order by ts), first_value(px) over (order by ts desc) " +
                    "from ((select * from a) union all (select * from b))")
                    .assertsPlanNotContaining("Union All Merge");
        });
    }

    // An outer merge flattens a nested merge only when the nested one reaches it directly. An opaque
    // wrapper - a computed projection, a LIMIT, a window - retains the nested merge instead and executes
    // it as an ordinary branch, so the nested merge has to be executable on its own.
    @Test
    public void testNestedMergeBehindOpaqueWrapperExecutes() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE union_wrapped AS (
                        SELECT x - 1 AS x, x::TIMESTAMP ts FROM long_sequence(6)
                    ) TIMESTAMP(ts)
                    """);

            final String projectionWrapped = """
                    SELECT x FROM (
                        (SELECT x, ts FROM union_wrapped WHERE x = 0)
                        UNION ALL
                        (
                            SELECT x + 0 AS x, ts FROM (
                                (SELECT x, ts FROM union_wrapped WHERE x = 1)
                                UNION ALL
                                (SELECT x, ts FROM union_wrapped WHERE x = 2)
                            ) ORDER BY ts
                        )
                    ) ORDER BY ts
                    """;
            assertPlanShape(projectionWrapped, 2, 0);
            assertQuery(projectionWrapped).noRandomAccess().returns("x\n0\n1\n2\n");

            final String projectionWrappedOnTheLeft = """
                    SELECT x FROM (
                        (
                            SELECT x + 0 AS x, ts FROM (
                                (SELECT x, ts FROM union_wrapped WHERE x = 1)
                                UNION ALL
                                (SELECT x, ts FROM union_wrapped WHERE x = 2)
                            ) ORDER BY ts
                        )
                        UNION ALL
                        (SELECT x, ts FROM union_wrapped WHERE x = 3)
                    ) ORDER BY ts
                    """;
            assertPlanShape(projectionWrappedOnTheLeft, 2, 0);
            assertQuery(projectionWrappedOnTheLeft).noRandomAccess().returns("x\n1\n2\n3\n");

            final String limitWrapped = """
                    SELECT x FROM (
                        (SELECT x, ts FROM union_wrapped WHERE x = 0)
                        UNION ALL
                        (
                            SELECT x, ts FROM (
                                (SELECT x, ts FROM union_wrapped WHERE x = 1)
                                UNION ALL
                                (SELECT x, ts FROM union_wrapped WHERE x = 2)
                            ) ORDER BY ts LIMIT 5
                        )
                    ) ORDER BY ts
                    """;
            assertPlanShape(limitWrapped, 2, 0);
            assertQuery(limitWrapped).noRandomAccess().returns("x\n0\n1\n2\n");

            final String windowWrapped = """
                    SELECT x FROM (
                        (SELECT x, ts FROM union_wrapped WHERE x = 0)
                        UNION ALL
                        (
                            SELECT sum(x) OVER (ORDER BY ts) AS x, ts FROM (
                                (SELECT x, ts FROM union_wrapped WHERE x = 1)
                                UNION ALL
                                (SELECT x, ts FROM union_wrapped WHERE x = 2)
                            ) ORDER BY ts
                        )
                    ) ORDER BY ts
                    """;
            assertPlanShape(windowWrapped, 2, 0);
            assertQuery(windowWrapped).noRandomAccess().returns("x\n0.0\n1.0\n3.0\n");

            // the outer union does not merge here at all - the nested merge is still the outer union's
            // operand, so it is still generated without a cursor
            final String groupByWrappedUnderConcat = """
                    SELECT x FROM (
                        (SELECT x, ts FROM union_wrapped WHERE x = 0)
                        UNION ALL
                        (
                            SELECT count() AS x, max(ts) AS ts FROM (
                                (SELECT x, ts FROM union_wrapped WHERE x = 1)
                                UNION ALL
                                (SELECT x, ts FROM union_wrapped WHERE x = 2)
                                ORDER BY ts
                            )
                        )
                    )
                    """;
            assertPlanShape(groupByWrappedUnderConcat, 1, 0);
            assertQuery(groupByWrappedUnderConcat).noRandomAccess().returns("x\n0\n2\n");
        });
    }

    @Test
    public void testPlainUnionWithoutOrderStaysConcat() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a (px double, ts timestamp) timestamp(ts) partition by day");
            execute("create table b (px double, ts timestamp) timestamp(ts) partition by day");
            execute("insert into a values (10.0, 1), (30.0, 3)");
            execute("insert into b values (20.0, 2)");
            assertQuery("(select px from a) union all (select px from b)")
                    .withPlanContaining("Union All")
                    .withPlanNotContaining("Union All Merge")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            px
                            10.0
                            30.0
                            20.0
                            """);
        });
    }

    // A column that is SYMBOL on every branch is widened to STRING inside the union and cast back
    // to SYMBOL above it. The merge path must join that chain like the plain concat path does,
    // otherwise an ordered UNION ALL reports STRING where the unordered one reports SYMBOL.
    @Test
    public void testSymbolColumnsAreReSymbolisedOnMergePath() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a (sym symbol, px double, ts timestamp) timestamp(ts) partition by day");
            execute("create table b (sym symbol, px double, ts timestamp) timestamp(ts) partition by day");
            execute("insert into a values ('x', 10.0, 1), ('y', 20.0, 3)");
            execute("insert into b values ('x', 30.0, 2), ('z', 40.0, 4)");

            assertQuery("select * from ((select * from a) union all (select * from b)) order by ts")
                    .withPlanContaining("Union All Merge")
                    .withPlanNotContaining("Encode sort")
                    .timestamp("ts")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            sym\tpx\tts
                            x\t10.0\t1970-01-01T00:00:00.000001Z
                            x\t30.0\t1970-01-01T00:00:00.000002Z
                            y\t20.0\t1970-01-01T00:00:00.000003Z
                            z\t40.0\t1970-01-01T00:00:00.000004Z
                            """);

            // the merge path must report the same column type as the plain concat path
            assertQuery("select typeOf(sym) t from ((select * from a) union all (select * from b)) order by ts limit 1")
                    .noRandomAccess()
                    .expectSize()
                    .returns("t\nSYMBOL\n");
            assertQuery("select typeOf(sym) t from (select sym from a union all select sym from b) limit 1")
                    .noRandomAccess()
                    .expectSize()
                    .returns("t\nSYMBOL\n");
        });
    }

    @Test
    public void testNestedSymbolMergeBehindOpaqueWrapperReportsSymbol() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE sym_wrapped AS (
                        SELECT x - 1 AS x, ('s' || x)::SYMBOL sym, ('t' || x)::SYMBOL sym2, x::TIMESTAMP ts
                        FROM long_sequence(4)
                    ) TIMESTAMP(ts)
                    """);
            final String b0 = "(SELECT x, sym, sym2, ts FROM sym_wrapped WHERE x = 0)";
            final String b1 = "(SELECT x, sym, sym2, ts FROM sym_wrapped WHERE x = 1)";
            final String b2 = "(SELECT x, sym, sym2, ts FROM sym_wrapped WHERE x = 2)";

            final String projectionWrapped = "SELECT * FROM (" + b0
                    + " UNION ALL (SELECT x + 0 AS x, sym, sym2, ts FROM (" + b1 + " UNION ALL " + b2
                    + ") ORDER BY ts)) ORDER BY ts";
            assertMergedSymbolTypes(projectionWrapped, "SYMBOL\tSYMBOL\n", 2);

            final String projectionWrappedOnTheLeft = "SELECT * FROM ((SELECT x + 0 AS x, sym, sym2, ts FROM ("
                    + b0 + " UNION ALL " + b1 + ") ORDER BY ts) UNION ALL " + b2 + ") ORDER BY ts";
            assertMergedSymbolTypes(projectionWrappedOnTheLeft, "SYMBOL\tSYMBOL\n", 2);

            final String limitWrapped = "SELECT * FROM (" + b0 + " UNION ALL (SELECT * FROM (" + b1
                    + " UNION ALL " + b2 + ") ORDER BY ts LIMIT 5)) ORDER BY ts";
            assertMergedSymbolTypes(limitWrapped, "SYMBOL\tSYMBOL\n", 2);
        });
    }

    // A UNION ALL segment that an outer merge still holds pending exposes the segment's internal
    // STRING widening rather than SYMBOL. The outer segment has to pick that pending segment's SYMBOL
    // candidates up, otherwise a right-nested or balanced all-SYMBOL union reports STRING where the
    // flat and left-associated forms - and the unordered concat form - report SYMBOL.
    @Test
    public void testNestedSymbolSegmentsReportSymbol() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE sym_nested AS (
                        SELECT x - 1 AS x, ('s' || x)::SYMBOL sym, ('t' || x)::SYMBOL sym2, x::TIMESTAMP ts
                        FROM long_sequence(4)
                    ) TIMESTAMP(ts)
                    """);
            final String b0 = "(SELECT x, sym, sym2, ts FROM sym_nested WHERE x = 0)";
            final String b1 = "(SELECT x, sym, sym2, ts FROM sym_nested WHERE x = 1)";
            final String b2 = "(SELECT x, sym, sym2, ts FROM sym_nested WHERE x = 2)";
            final String b3 = "(SELECT x, sym, sym2, ts FROM sym_nested WHERE x = 3)";
            // one branch reads sym2 as STRING, so sym2 is genuinely STRING while sym stays SYMBOL
            final String b1MixedSym2 = "(SELECT x, sym, sym2::STRING sym2, ts FROM sym_nested WHERE x = 1)";

            final String rightNested = "SELECT * FROM (" + b0 + " UNION ALL (" + b1 + " UNION ALL " + b2 + ")) ORDER BY ts";
            assertMergedSymbolTypes(rightNested, "SYMBOL\tSYMBOL\n");
            assertQuery(rightNested)
                    .withPlanContaining("Union All Merge")
                    .withPlanNotContaining("Encode sort")
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns("""
                            x\tsym\tsym2\tts
                            0\ts1\tt1\t1970-01-01T00:00:00.000001Z
                            1\ts2\tt2\t1970-01-01T00:00:00.000002Z
                            2\ts3\tt3\t1970-01-01T00:00:00.000003Z
                            """);

            final String rightNestedDesc = "SELECT * FROM (" + b0 + " UNION ALL (" + b1 + " UNION ALL " + b2 + ")) ORDER BY ts DESC";
            assertMergedSymbolTypes(rightNestedDesc, "SYMBOL\tSYMBOL\n");

            final String rightNestedDeeper = "SELECT * FROM (" + b0 + " UNION ALL (" + b1
                    + " UNION ALL (" + b2 + " UNION ALL " + b3 + "))) ORDER BY ts";
            assertMergedSymbolTypes(rightNestedDeeper, "SYMBOL\tSYMBOL\n");

            final String balanced = "SELECT * FROM ((" + b0 + " UNION ALL " + b1 + ") UNION ALL ("
                    + b2 + " UNION ALL " + b3 + ")) ORDER BY ts";
            assertMergedSymbolTypes(balanced, "SYMBOL\tSYMBOL\n");

            final String rightNestedWithTail = "SELECT * FROM (" + b0 + " UNION ALL (" + b1 + " UNION ALL " + b2
                    + ") UNION ALL " + b3 + ") ORDER BY ts";
            assertMergedSymbolTypes(rightNestedWithTail, "SYMBOL\tSYMBOL\n");

            final String leftAssociated = "SELECT * FROM ((" + b0 + " UNION ALL " + b1 + ") UNION ALL " + b2 + ") ORDER BY ts";
            assertMergedSymbolTypes(leftAssociated, "SYMBOL\tSYMBOL\n");

            // a column that is not SYMBOL on every branch stays STRING, whichever side widens it
            final String rightNestedMixed = "SELECT * FROM (" + b0 + " UNION ALL (" + b1MixedSym2 + " UNION ALL " + b2 + ")) ORDER BY ts";
            assertMergedSymbolTypes(rightNestedMixed, "SYMBOL\tSTRING\n");
            assertQuery(rightNestedMixed)
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns("""
                            x\tsym\tsym2\tts
                            0\ts1\tt1\t1970-01-01T00:00:00.000001Z
                            1\ts2\tt2\t1970-01-01T00:00:00.000002Z
                            2\ts3\tt3\t1970-01-01T00:00:00.000003Z
                            """);
        });
    }

    @Test
    public void testTiesPreserveABeforeB() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a (px double, ts timestamp) timestamp(ts) partition by day");
            execute("create table b (px double, ts timestamp) timestamp(ts) partition by day");
            execute("insert into a values (10.0, 1), (21.0, 2)");
            execute("insert into b values (22.0, 2), (30.0, 3)");
            assertQuery("select px from ((select * from a) union all (select * from b)) order by ts")
                    .withPlanContaining("Union All Merge")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            px
                            10.0
                            21.0
                            22.0
                            30.0
                            """);
        });
    }

    @Test
    public void testTsTypeMismatchFallsBackToConcat() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a (px double, ts timestamp) timestamp(ts) partition by day");
            execute("create table b (px double, ts timestamp_ns) timestamp(ts) partition by day");
            execute("insert into a values (10.0, 1)");
            execute("insert into b values (20.0, 2)");
            assertQuery("select px from ((select * from a) union all (select * from b)) order by ts")
                    .withPlanNotContaining("Union All Merge")
                    .expectSize()
                    .returns("""
                            px
                            20.0
                            10.0
                            """);
        });
    }

    @Test
    public void testTwoWindowsSameOrderMerge() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a (px double, ts timestamp) timestamp(ts) partition by day");
            execute("create table b (px double, ts timestamp) timestamp(ts) partition by day");
            execute("insert into a values (10.0, 1), (30.0, 3)");
            execute("insert into b values (20.0, 2), (40.0, 4)");
            assertQuery("select first_value(px) over (order by ts), sum(px) over (order by ts) " +
                    "from ((select * from a) union all (select * from b))")
                    .withPlanContaining("Window", "Union All Merge")
                    .withPlanNotContaining("CachedWindow")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            first_value\tsum
                            10.0\t10.0
                            10.0\t30.0
                            10.0\t60.0
                            10.0\t100.0
                            """);
        });
    }

    @Test
    public void testInnerBoundedOperandDoesNotUncapOuterUnion() throws Exception {
        assertMemoryLeak(() -> {
            createLongUnionTable();
            final StringBuilder query = new StringBuilder(
                    "SELECT x FROM ((SELECT x, ts FROM union_long ORDER BY ts LIMIT 1)"
            );
            for (int i = 1; i < 48; i++) {
                query.append(" UNION ALL ");
                appendFilteredUnionBranch(query, i);
            }
            query.append(") ORDER BY ts");

            assertPlanShape(query.toString(), 1, 0);
            Assert.assertTrue(getPlanSink(query).getSink().toString().contains("branches: 48"));
            assertQuery(query).noRandomAccess().returns(buildExpectedX(48, false, 0));
        });
    }

    @Test
    public void testLongOuterLimitsUseNWayMerge() throws Exception {
        assertMemoryLeak(() -> {
            createLongUnionTable();
            final String orderedQuery = buildFilteredUnionQuery(48, false, 0);

            final String positiveLimit = "SELECT x FROM (" + orderedQuery + ") LIMIT 3";
            assertPlanShape(positiveLimit, 1, 0);
            assertQuery(positiveLimit).noRandomAccess().returns(buildExpectedX(48, false, 3));

            final String negativeLimit = "SELECT x FROM (" + orderedQuery + ") LIMIT -3";
            assertPlanShape(negativeLimit, 1, 0);
            assertQuery(negativeLimit).noRandomAccess().expectSize().returns("x\n45\n46\n47\n");
        });
    }

    @Test
    public void testLongChainUsesNWayMerge() throws Exception {
        assertMemoryLeak(() -> {
            createLongUnionTable();

            final String query16 = buildFilteredUnionQuery(16, false, 0);
            assertPlanShape(query16, 1, 0);

            final String query17 = buildFilteredUnionQuery(17, false, 0);
            assertPlanShape(query17, 1, 0);
            assertQuery(query17).noRandomAccess().returns(buildExpectedX(17, false, 0));
            assertQuery(buildFilteredUnionQuery(17, true, 0)).noRandomAccess().returns(buildExpectedX(17, true, 0));

            assertPlanShape(buildFilteredUnionQuery(48, false, 0), 1, 0);

            final String limitQuery = buildFilteredUnionQuery(48, false, 3);
            assertPlanShape(limitQuery, 1, 0);
            assertQuery(limitQuery).noRandomAccess().returns(buildExpectedX(48, false, 3));
        });
    }

    @Test
    public void testLongParenthesizedChainsFlattenIntoNWayMerge() throws Exception {
        assertMemoryLeak(() -> {
            createLongUnionTable();

            final String leftDeep16 = buildParenthesizedUnionQuery(16, false, false);
            assertPlanShape(leftDeep16, 1, 0);
            assertQuery(leftDeep16).noRandomAccess().returns(buildExpectedX(16, false, 0));

            final String leftDeep17 = buildParenthesizedUnionQuery(17, false, false);
            assertPlanShape(leftDeep17, 1, 0);
            assertQuery(leftDeep17).noRandomAccess().returns(buildExpectedX(17, false, 0));

            final String leftDeep17Desc = buildParenthesizedUnionQuery(17, true, false);
            assertPlanShape(leftDeep17Desc, 1, 0);
            assertQuery(leftDeep17Desc).noRandomAccess().returns(buildExpectedX(17, true, 0));

            final String leftDeepSymbol17 = buildParenthesizedSymbolUnionQuery(17);
            assertPlanShape(leftDeepSymbol17, 1, 0);
            assertQuery(leftDeepSymbol17).noRandomAccess().returns(buildExpectedX(17, false, 0));

            final String balanced32 = buildParenthesizedUnionQuery(32, false, true);
            assertPlanShape(balanced32, 1, 0);
            assertQuery(balanced32).noRandomAccess().returns(buildExpectedX(32, false, 0));

            final String balanced32Desc = buildParenthesizedUnionQuery(32, true, true);
            assertPlanShape(balanced32Desc, 1, 0);
            assertQuery(balanced32Desc).noRandomAccess().returns(buildExpectedX(32, true, 0));
        });
    }

    @Test
    public void testLongParenthesizedChainsStayFlat() throws Exception {
        assertMemoryLeak(() -> {
            createLongUnionTable();

            final String leftDeep33 = buildParenthesizedUnionQuery(33, false, false);
            assertPlanShape(leftDeep33, 1, 0);
            assertQuery(leftDeep33).noRandomAccess().returns(buildExpectedX(33, false, 0));

            final String leftDeep48Desc = buildParenthesizedUnionQuery(48, true, false);
            assertPlanShape(leftDeep48Desc, 1, 0);
            assertQuery(leftDeep48Desc).noRandomAccess().returns(buildExpectedX(48, true, 0));

            final String leftDeepSymbol33 = buildParenthesizedSymbolUnionQuery(33);
            assertPlanShape(leftDeepSymbol33, 1, 0);
            assertQuery(leftDeepSymbol33).noRandomAccess().returns(buildExpectedX(33, false, 0));
        });
    }

    @Test
    public void testLongPositiveFullConsumptionLimitsUseNWayMerge() throws Exception {
        assertMemoryLeak(() -> {
            createLongUnionTable();
            final String query = buildFilteredUnionQuery(48, false, 0);

            final String positiveLiteral = query + " LIMIT 48";
            assertPlanShape(positiveLiteral, 1, 0);
            assertQuery(positiveLiteral).noRandomAccess().returns(buildExpectedX(48, false, 0));

            final String hugePositiveLiteral = query + " LIMIT 9223372036854775807";
            assertPlanShape(hugePositiveLiteral, 1, 0);
            assertQuery(hugePositiveLiteral).noRandomAccess().returns(buildExpectedX(48, false, 0));

            final String positiveRange = query + " LIMIT 0,48";
            assertPlanShape(positiveRange, 1, 0);
            assertQuery(positiveRange).noRandomAccess().returns(buildExpectedX(48, false, 0));

            final String reversedPositiveRange = query + " LIMIT 48,0";
            assertPlanShape(reversedPositiveRange, 1, 0);
            assertQuery(reversedPositiveRange).noRandomAccess().returns(buildExpectedX(48, false, 0));

            final String smallPositiveRange = query + " LIMIT 0,3";
            assertPlanShape(smallPositiveRange, 1, 0);
            assertQuery(smallPositiveRange).noRandomAccess().returns(buildExpectedX(48, false, 3));
        });
    }

    @Test
    public void testLongFixedPrefixesUseNWayMerge() throws Exception {
        assertMemoryLeak(() -> {
            createLongUnionTable();
            final String query = buildFilteredUnionQuery(48, false, 0);

            final String singleRowLimit = query + " LIMIT 1";
            assertPlanShape(singleRowLimit, 1, 0);
            assertQuery(singleRowLimit).noRandomAccess().returns("x\n0\n");

            final String singleRowRange = query + " LIMIT 0,1";
            assertPlanShape(singleRowRange, 1, 0);
            assertQuery(singleRowRange).noRandomAccess().returns("x\n0\n");

            final String reversedSingleRowRange = query + " LIMIT 1,0";
            assertPlanShape(reversedSingleRowRange, 1, 0);
            assertQuery(reversedSingleRowRange).noRandomAccess().returns("x\n0\n");

            final String descendingSingleRowLimit = buildFilteredUnionQuery(48, true, 0) + " LIMIT 1";
            assertPlanShape(descendingSingleRowLimit, 1, 0);
            assertQuery(descendingSingleRowLimit).noRandomAccess().returns("x\n47\n");

            final String leftDeepSingleRowLimit = buildParenthesizedUnionQuery(17, false, false) + " LIMIT 1";
            assertPlanShape(leftDeepSingleRowLimit, 1, 0);
            assertQuery(leftDeepSingleRowLimit).noRandomAccess().returns("x\n0\n");

            final String symbolSingleRowLimit = buildParenthesizedSymbolUnionQuery(17) + " LIMIT 1";
            assertPlanShape(symbolSingleRowLimit, 1, 0);
            assertQuery(symbolSingleRowLimit).noRandomAccess().returns("x\n0\n");

            final String zeroLimit = query + " LIMIT 0";
            assertPlanShape(zeroLimit, 1, 0);
            assertQuery(zeroLimit).noRandomAccess().returns("x\n");

            final String twoRowLimit = query + " LIMIT 2";
            assertPlanShape(twoRowLimit, 1, 0);
            assertQuery(twoRowLimit).noRandomAccess().returns("x\n0\n1\n");

            final String twoRowRange = query + " LIMIT 0,2";
            assertPlanShape(twoRowRange, 1, 0);
            assertQuery(twoRowRange).noRandomAccess().returns("x\n0\n1\n");

            final String threeRowLimit = query + " LIMIT 3";
            assertPlanShape(threeRowLimit, 1, 0);
            assertQuery(threeRowLimit).noRandomAccess().returns("x\n0\n1\n2\n");
        });
    }

    @Test
    public void testLongRuntimeLimitsUseNWayMerge() throws Exception {
        assertMemoryLeak(() -> {
            createLongUnionTable();
            final String query = buildFilteredUnionQuery(48, false, 0) + " LIMIT :lim";

            bindVariableService.setLong("lim", 3);
            assertPlanShape(query, 1, 0);
            assertQuery(query).noRandomAccess().returns(buildExpectedX(48, false, 3));

            bindVariableService.setLong("lim", -3);
            assertPlanShape(query, 1, 0);
            assertQuery(query).noRandomAccess().expectSize().returns("x\n45\n46\n47\n");
        });
    }

    @Test
    public void testLongTailLimitsUseNWayMerge() throws Exception {
        assertMemoryLeak(() -> {
            createLongUnionTable();

            final String positiveLimit = buildFilteredUnionQuery(48, false, 3);
            assertPlanShape(positiveLimit, 1, 0);
            assertQuery(positiveLimit).noRandomAccess().returns(buildExpectedX(48, false, 3));

            final String negativeLimit = buildFilteredUnionQuery(48, false, 0) + " LIMIT -3";
            assertPlanShape(negativeLimit, 1, 0);
            assertQuery(negativeLimit).noRandomAccess().expectSize().returns("x\n45\n46\n47\n");

            final String negativeLimitDesc = buildFilteredUnionQuery(48, true, 0) + " LIMIT -3";
            assertPlanShape(negativeLimitDesc, 1, 0);
            assertQuery(negativeLimitDesc).noRandomAccess().expectSize().returns("x\n2\n1\n0\n");

            final String negativeRange = buildFilteredUnionQuery(48, false, 0) + " LIMIT -5,-2";
            assertPlanShape(negativeRange, 1, 0);
            assertQuery(negativeRange).noRandomAccess().expectSize().returns("x\n43\n44\n45\n");
        });
    }

    @Test
    public void testLongNWayCastsUseFinalMetadataDirectly() throws Exception {
        assertMemoryLeak(() -> {
            createLongUnionTable();
            final StringBuilder query = new StringBuilder("SELECT value FROM (");
            for (int i = 0; i < 48; i++) {
                if (i > 0) {
                    query.append(" UNION ALL ");
                }
                query.append("(SELECT x::")
                        .append((i & 1) == 0 ? "int" : "double")
                        .append(" value, ts FROM union_long WHERE x = ")
                        .append(i)
                        .append(')');
            }
            query.append(") ORDER BY ts LIMIT 3");

            assertPlanShape(query.toString(), 1, 0);
            Assert.assertTrue(getPlanSink(query).getSink().toString().contains("branches: 48"));
            assertQuery(query).noRandomAccess().returns("value\n0.0\n1.0\n2.0\n");
        });
    }

    @Test
    public void testLongNWayCompileStructureIsFlat() throws Exception {
        assertMemoryLeak(() -> {
            createLongUnionTable();
            final String query = buildFilteredUnionQuery(64, false, 1);
            final String plan = getPlanSink(query).getSink().toString();
            Assert.assertEquals(1, countOccurrences(plan, "Union All Merge"));
            Assert.assertEquals(0, countOccurrences(plan, "Encode sort"));
            Assert.assertTrue(plan.contains("branches: 64"));
        });
    }

    @Test
    public void testLongNWayTiesPreserveBranchOrder() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE union_nulls AS (
                        SELECT x - 1 AS x, 1::timestamp ts
                        FROM long_sequence(48)
                    ) TIMESTAMP(ts)
                    """);
            final StringBuilder query = new StringBuilder("SELECT x FROM (");
            for (int i = 0; i < 48; i++) {
                if (i > 0) {
                    query.append(" UNION ALL ");
                }
                query.append("(SELECT x, ts FROM union_nulls WHERE x = ").append(i).append(')');
            }
            query.append(") ORDER BY ts LIMIT 3");

            assertPlanShape(query.toString(), 1, 0);
            assertQuery(query).noRandomAccess().returns("x\n0\n1\n2\n");
        });
    }

    @Test
    public void testWindowOrderByNonTimestampDoesNotMerge() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a (px double, ts timestamp) timestamp(ts) partition by day");
            execute("create table b (px double, ts timestamp) timestamp(ts) partition by day");
            assertQuery("select first_value(px) over (partition by ts order by px) from ((select * from a) union all (select * from b))")
                    .assertsPlanNotContaining("Union All Merge");
        });
    }

    @Test
    public void testWindowOverSymbolUnionStreams() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a (sym symbol, px double, ts timestamp) timestamp(ts) partition by day");
            execute("create table b (sym symbol, px double, ts timestamp) timestamp(ts) partition by day");
            execute("insert into a values ('x', 10.0, 1), ('y', 20.0, 3)");
            execute("insert into b values ('x', 30.0, 2), ('z', 40.0, 4)");
            assertQuery("select sym, px, first_value(px) over (partition by sym order by ts) " +
                    "from ((select * from a) union all (select * from b))")
                    .withPlanContaining("Window", "Union All Merge")
                    .withPlanNotContaining("CachedWindow")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            sym\tpx\tfirst_value
                            x\t10.0\t10.0
                            x\t30.0\t10.0
                            y\t20.0\t20.0
                            z\t40.0\t40.0
                            """);
        });
    }

    // Asserts the column types an all-SYMBOL union segment exposes, and that keeping the SYMBOL
    // candidates has not cost the segment its single flattened merge.
    private void assertMergedSymbolTypes(String query, String expectedTypes) throws Exception {
        assertMergedSymbolTypes(query, expectedTypes, 1);
    }

    private void assertMergedSymbolTypes(String query, String expectedTypes, int mergeCount) throws Exception {
        assertPlanShape(query, mergeCount, 0);
        assertQuery("SELECT typeOf(sym) sym_type, typeOf(sym2) sym2_type FROM (" + query + ") LIMIT 1")
                .noRandomAccess()
                .returns("sym_type\tsym2_type\n" + expectedTypes);
    }

    private void assertPlanShape(String query, int mergeCount, int sortCount) throws Exception {
        final String plan = getPlanSink(query).getSink().toString();
        Assert.assertEquals(mergeCount, countOccurrences(plan, "Union All Merge"));
        Assert.assertEquals(sortCount, countOccurrences(plan, "Encode sort"));
    }

    private String buildExpectedX(int branchCount, boolean isDescending, int limit) {
        final int rowCount = limit > 0 ? limit : branchCount;
        final StringBuilder expected = new StringBuilder("x\n");
        for (int i = 0; i < rowCount; i++) {
            expected.append(isDescending ? branchCount - i - 1 : i).append('\n');
        }
        return expected.toString();
    }

    private String buildFilteredUnionQuery(int branchCount, boolean isDescending, int limit) {
        final StringBuilder query = new StringBuilder("SELECT x FROM (");
        for (int i = 0; i < branchCount; i++) {
            if (i > 0) {
                query.append(" UNION ALL ");
            }
            appendFilteredUnionBranch(query, i);
        }
        query.append(") ORDER BY ts");
        if (isDescending) {
            query.append(" DESC");
        }
        if (limit > 0) {
            query.append(" LIMIT ").append(limit);
        }
        return query.toString();
    }

    private String buildParenthesizedUnionExpression(int lo, int hi, boolean isBalanced) {
        if (hi - lo == 1) {
            final StringBuilder branch = new StringBuilder();
            appendFilteredUnionBranch(branch, lo);
            return branch.toString();
        }
        final int mid = isBalanced ? (lo + hi) / 2 : hi - 1;
        return "(" + buildParenthesizedUnionExpression(lo, mid, isBalanced)
                + " UNION ALL " + buildParenthesizedUnionExpression(mid, hi, isBalanced) + ")";
    }

    private String buildParenthesizedUnionQuery(int branchCount, boolean isDescending, boolean isBalanced) {
        return "SELECT x FROM (" + buildParenthesizedUnionExpression(0, branchCount, isBalanced)
                + ") ORDER BY ts" + (isDescending ? " DESC" : "");
    }

    private String buildParenthesizedSymbolUnionQuery(int branchCount) {
        String expression = "(SELECT x, sym, ts FROM union_long WHERE x = 0)";
        for (int i = 1; i < branchCount; i++) {
            expression = "(" + expression + " UNION ALL (SELECT x, sym, ts FROM union_long WHERE x = " + i + "))";
        }
        return "SELECT x FROM (" + expression + ") ORDER BY ts";
    }

    private int countOccurrences(String value, String term) {
        int count = 0;
        int offset = 0;
        while ((offset = value.indexOf(term, offset)) > -1) {
            count++;
            offset += term.length();
        }
        return count;
    }

    private void createLongUnionTable() throws Exception {
        execute("CREATE TABLE union_long AS (" +
                "SELECT x - 1 AS x, 's'::symbol sym, x::timestamp ts FROM long_sequence(48)" +
                ") TIMESTAMP(ts)");
    }

    private void appendFilteredUnionBranch(StringBuilder query, int x) {
        query.append("(SELECT x, ts FROM union_long WHERE x = ").append(x).append(')');
    }
}
