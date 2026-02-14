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

import io.questdb.cairo.ColumnType;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.model.ExecutionModel;
import io.questdb.std.str.Sinkable;
import org.junit.Assert;
import org.junit.Test;

public class FilterPushdownIntoUnionTest extends AbstractSqlParserTest {

    @Test
    public void testFilterPushdownBlockedByLatestOnInUnionBranch() throws Exception {
        // Verify that a timestamp filter is NOT pushed into a UNION ALL branch
        // that has LATEST ON. Pushing a filter before LATEST ON would narrow the
        // scan window, changing which row is considered "latest" for each partition.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_sym (ts TIMESTAMP, sym SYMBOL, x LONG) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t_sym VALUES
                        ('2024-01-01T00:00:00.000000Z', 'A', 1),
                        ('2024-01-02T00:00:00.000000Z', 'A', 2),
                        ('2024-01-03T00:00:00.000000Z', 'A', 3)
                    """);

            execute("CREATE TABLE t_plain (ts TIMESTAMP, sym SYMBOL, x LONG) TIMESTAMP(ts)");
            execute("INSERT INTO t_plain VALUES ('2024-01-02T00:00:00.000000Z', 'C', 99)");

            // Correct semantics:
            //   1. LATEST ON: latest for A = Jan 3 (3)
            //   2. t_plain: Jan 2 (C, 99)
            //   3. UNION ALL: (Jan 3, A, 3), (Jan 2, C, 99)
            //   4. WHERE ts <= Jan 2: A's row filtered out → only (Jan 2, C, 99)
            //
            // Buggy semantics (if filter pushed before LATEST ON):
            //   1. WHERE ts <= Jan 2 then LATEST ON: latest for A within [<=Jan 2]
            //      is Jan 2 (2), which passes the outer WHERE too
            //   2. t_plain: Jan 2 (C, 99)
            //   3. Both rows survive → wrong!
            assertQueryNoLeakCheck(
                    """
                            ts\tsym\tx
                            2024-01-02T00:00:00.000000Z\tC\t99
                            """,
                    """
                            SELECT * FROM (
                                SELECT ts, sym, x FROM t_sym LATEST ON ts PARTITION BY sym
                                UNION ALL
                                SELECT ts, sym, x FROM t_plain
                            ) WHERE ts <= '2024-01-02'""",
                    null,
                    false
            );
        });
    }

    @Test
    public void testFilterPushdownBlockedByLimitInUnionBranch() throws Exception {
        // Verify that a timestamp filter is NOT pushed past a LIMIT inside a
        // UNION ALL branch. Pushing a filter before LIMIT would change which
        // rows are selected.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (ts TIMESTAMP, x LONG) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t1 VALUES
                        ('2024-01-01T00:00:00.000000Z', 1),
                        ('2024-01-02T00:00:00.000000Z', 2),
                        ('2024-01-03T00:00:00.000000Z', 3),
                        ('2024-01-04T00:00:00.000000Z', 4)
                    """);

            execute("CREATE TABLE t2 (ts TIMESTAMP, x LONG) TIMESTAMP(ts)");
            execute("INSERT INTO t2 VALUES ('2024-01-03T00:00:00.000000Z', 30)");

            // Correct semantics:
            //   1. Branch 1 inner: LIMIT 2 → Jan 1 (1), Jan 2 (2)
            //   2. Branch 2: Jan 3 (30)
            //   3. UNION ALL: (Jan 1, 1), (Jan 2, 2), (Jan 3, 30)
            //   4. WHERE ts >= Jan 3: only (Jan 3, 30) survives
            //
            // Buggy semantics (if filter pushed before LIMIT):
            //   1. Branch 1: WHERE ts >= Jan 3 then LIMIT 2 → Jan 3 (3), Jan 4 (4)
            //   2. Branch 2: Jan 3 (30)
            //   3. All 3 rows pass outer WHERE → wrong!
            assertQueryNoLeakCheck(
                    """
                            ts\tx
                            2024-01-03T00:00:00.000000Z\t30
                            """,
                    """
                            SELECT * FROM (
                                SELECT * FROM (SELECT ts, x FROM t1 LIMIT 2)
                                UNION ALL
                                SELECT ts, x FROM t2
                            ) WHERE ts >= '2024-01-03'""",
                    null,
                    false
            );
        });
    }

    @Test
    public void testFilterPushdownBlockedByLimitOnLastUnionBranch() throws Exception {
        // When LIMIT is on the last branch of UNION ALL, it semantically applies
        // to the whole union result. A timestamp filter must not be pushed into
        // any branch because it would change which rows enter the LIMIT window.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (ts TIMESTAMP, x LONG) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t1 VALUES
                        ('2024-01-01T00:00:00.000000Z', 1),
                        ('2024-01-02T00:00:00.000000Z', 2),
                        ('2024-01-03T00:00:00.000000Z', 3)
                    """);

            execute("CREATE TABLE t2 (ts TIMESTAMP, x LONG) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t2 VALUES
                        ('2024-01-04T00:00:00.000000Z', 4),
                        ('2024-01-05T00:00:00.000000Z', 5)
                    """);

            // Correct semantics:
            //   1. UNION ALL: (Jan 1,1), (Jan 2,2), (Jan 3,3), (Jan 4,4), (Jan 5,5)
            //   2. LIMIT 3: (Jan 1,1), (Jan 2,2), (Jan 3,3)
            //   3. WHERE ts >= Jan 3: only (Jan 3, 3)
            //
            // Buggy semantics (if filter pushed into branches before LIMIT):
            //   1. t1 WHERE ts >= Jan 3: (Jan 3, 3)
            //   2. t2 WHERE ts >= Jan 3: (Jan 4, 4), (Jan 5, 5)
            //   3. UNION ALL: (Jan 3,3), (Jan 4,4), (Jan 5,5)
            //   4. LIMIT 3: all 3
            //   5. WHERE ts >= Jan 3: all 3 → wrong!
            assertQueryNoLeakCheck(
                    """
                            ts\tx
                            2024-01-03T00:00:00.000000Z\t3
                            """,
                    """
                            SELECT * FROM (
                                SELECT ts, x FROM t1
                                UNION ALL
                                SELECT ts, x FROM t2
                                LIMIT 3
                            ) WHERE ts >= '2024-01-03'""",
                    null,
                    false
            );
        });
    }

    @Test
    public void testFilterPushdownShouldNotChangeSampleByInUnionBranch() throws Exception {
        // Regression test: pushing a WHERE filter into a UNION branch that has
        // SAMPLE BY must not change aggregation semantics. The filter should be
        // applied AFTER the SAMPLE BY aggregation (like HAVING), not before it
        // (as a scan-level WHERE that changes which rows are aggregated).
        assertMemoryLeak(() -> {
            execute("create table t1 (ts timestamp, x double) timestamp(ts)");
            execute("insert into t1 values ('2024-01-01T00:00:00.000000Z', 9.0)");

            execute("create table t2 (ts timestamp, x double) timestamp(ts)");
            execute("insert into t2 values ('2024-01-01T00:00:00.000000Z', 3.0)");
            execute("insert into t2 values ('2024-01-01T00:00:01.000000Z', 4.0)");
            execute("insert into t2 values ('2024-01-01T00:00:02.000000Z', 8.0)");

            // Correct semantics:
            //   1. t1 branch: (ts=0, x=9.0)
            //   2. t2 SAMPLE BY branch: avg(3.0, 4.0, 8.0) = 5.0
            //   3. UNION ALL: (9.0), (5.0)
            //   4. WHERE x > 5: only (9.0) — avg 5.0 is not > 5
            //
            // Buggy semantics (if filter pushed as pre-aggregation WHERE):
            //   1. t1 branch with WHERE x>5: (ts=0, x=9.0)
            //   2. t2 with WHERE x>5 then SAMPLE BY: only x=8.0 survives filter,
            //      avg(8.0) = 8.0
            //   3. UNION ALL: (9.0), (8.0)
            //   4. WHERE x > 5: both kept — wrong! t2 bucket should not appear
            assertQueryNoLeakCheck(
                    """
                            ts\tx
                            2024-01-01T00:00:00.000000Z\t9.0
                            """,
                    "select * from (" +
                            "select ts, x from t1 " +
                            "union all " +
                            "select ts, avg(x) x from t2 sample by 1h align to first observation" +
                            ") where x > 5",
                    null,
                    false
            );
        });
    }

    @Test
    public void testPushDownTimestampFilterThroughUnion() throws SqlException {
        assertQuery(
                "select-choose ts from (" +
                        /**/ "select-choose [ts1 ts] ts1 ts from (" +
                        /**/   "select [ts1] from t1 timestamp (ts1) where ts1 in '2025-12-01T01;2h'" +
                        /**/ ") union select-choose [ts2 ts] ts2 ts from (" +
                        /**/   "select [ts2] from t2 timestamp (ts2) where ts2 in '2025-12-01T01;2h'" +
                        /**/ ")" +
                        ")",
                "select ts from (select ts1 ts from t1 union select ts2 ts from t2) where ts in '2025-12-01T01;2h'",
                modelOf("t1").timestamp("ts1"),
                modelOf("t2").timestamp("ts2")
        );
    }

    @Test
    public void testPushDownTimestampFilterThroughUnionAll() throws SqlException {
        assertQuery(
                "select-choose ts from (" +
                        /**/ "select-choose [ts1 ts] ts1 ts from (" +
                        /**/   "select [ts1] from t1 timestamp (ts1) where ts1 in '2025-12-01T01;2h'" +
                        /**/ ") union all select-choose [ts2 ts] ts2 ts from (" +
                        /**/   "select [ts2] from t2 timestamp (ts2) where ts2 in '2025-12-01T01;2h'" +
                        /**/ ")" +
                        ")",
                "select ts from (select ts1 ts from t1 union all select ts2 ts from t2) where ts in '2025-12-01T01;2h'",
                modelOf("t1").timestamp("ts1"),
                modelOf("t2").timestamp("ts2")
        );
    }

    @Test
    public void testPushDownTimestampFilterThroughUnionAllCte() throws SqlException {
        // Two separate CTEs combined with UNION ALL in the main query.
        // The timestamp filter must be pushed into both CTE branches.
        createModelsAndRun(
                () -> {
                    try (SqlCompiler compiler = engine.getSqlCompiler()) {
                        // First verify that a single CTE wrapping the whole UNION works
                        // (structurally identical to the nested subquery form)
                        sink.clear();
                        ExecutionModel singleCteModel = compiler.generateExecutionModel(
                                "WITH u AS (SELECT ts1 ts FROM t1 UNION ALL SELECT ts2 ts FROM t2) " +
                                        "SELECT ts FROM u WHERE ts in '2025-12-01T01;2h'",
                                sqlExecutionContext);
                        ((Sinkable) singleCteModel).toSink(sink);
                        String singleCtePlan = sink.toString();

                        Assert.assertTrue(
                                "single CTE: timestamp filter not pushed into first branch: " + singleCtePlan,
                                singleCtePlan.contains("where ts1 in '2025-12-01T01;2h'"));
                        Assert.assertTrue(
                                "single CTE: timestamp filter not pushed into second branch: " + singleCtePlan,
                                singleCtePlan.contains("where ts2 in '2025-12-01T01;2h'"));

                        // Now verify that two separate CTEs combined with UNION ALL also push down
                        sink.clear();
                        ExecutionModel twoCteModel = compiler.generateExecutionModel(
                                "WITH l AS (SELECT ts1 ts FROM t1), " +
                                        "r AS (SELECT ts2 ts FROM t2) " +
                                        "SELECT ts FROM (l UNION ALL r) " +
                                        "WHERE ts in '2025-12-01T01;2h'",
                                sqlExecutionContext);
                        ((Sinkable) twoCteModel).toSink(sink);
                        String twoCtePlan = sink.toString();

                        Assert.assertTrue(
                                "two CTEs: timestamp filter not pushed into first CTE branch: " + twoCtePlan,
                                twoCtePlan.contains("where ts1 in '2025-12-01T01;2h'"));
                        Assert.assertTrue(
                                "two CTEs: timestamp filter not pushed into second CTE branch: " + twoCtePlan,
                                twoCtePlan.contains("where ts2 in '2025-12-01T01;2h'"));
                    }
                },
                modelOf("t1").timestamp("ts1"),
                modelOf("t2").timestamp("ts2")
        );
    }

    @Test
    public void testPushDownTimestampFilterThroughUnionAllMismatchedAliases() throws SqlException {
        // The "ts" alias maps to the TIMESTAMP column in branch 1 (position 1),
        // but to the SYMBOL column in branch 2 (position 2). The filter should be
        // remapped by position so that branch 2 filters on ts2 (the TIMESTAMP at
        // position 1), not on sym2 which carries the "ts" alias.
        assertQuery(
                "select-choose ts from (" +
                        /**/ "select-choose [ts1 ts] name1, ts1 ts, sym1 from (" +
                        /**/   "select [ts1] from t1 timestamp (ts1) where ts1 in '2025-12-01T01;2h'" +
                        /**/ ") union all select-choose [ts2] name2, ts2, sym2 ts from (" +
                        /**/   "select [ts2] from t2 timestamp (ts2) where ts2 in '2025-12-01T01;2h'" +
                        /**/ ")" +
                        ")",
                """
                        SELECT ts FROM (
                            SELECT name1, ts1 ts, sym1 FROM t1
                            UNION ALL
                            SELECT name2, ts2, sym2 ts FROM t2
                        ) WHERE ts IN '2025-12-01T01;2h'
                        """,
                modelOf("t1").col("name1", ColumnType.VARCHAR).timestamp("ts1").col("sym1", ColumnType.SYMBOL),
                modelOf("t2").col("name2", ColumnType.VARCHAR).timestamp("ts2").col("sym2", ColumnType.SYMBOL)
        );
    }

    @Test
    public void testPushDownTimestampFilterThroughUnionAllNonPushableBranch() throws SqlException {
        // One branch aliases the column to a non-literal expression (ts2+1).
        // The filter is pushed to both branches (since the parent's alias map sees it as a literal),
        // but on the non-literal branch it stays at the select-virtual level and cannot be pushed further.
        assertQuery(
                "select-choose ts from (" +
                        /**/ "select-choose [ts1 ts] ts1 ts from (" +
                        /**/   "select [ts1] from t1 timestamp (ts1) where ts1 in '2025-12-01T01;2h'" +
                        /**/ ") union all select-virtual [ts2 + 1 ts] ts2 + 1 ts from (" +
                        /**/   "select [ts2] from t2 timestamp (ts2)" +
                        /**/ ") where ts in '2025-12-01T01;2h'" +
                        ")",
                "select ts from (select ts1 ts from t1 union all select ts2+1 ts from t2) where ts in '2025-12-01T01;2h'",
                modelOf("t1").timestamp("ts1"),
                modelOf("t2").timestamp("ts2")
        );
    }

    @Test
    public void testPushDownTimestampFilterThroughUnionAllThreeBranches() throws SqlException {
        assertQuery(
                "select-choose ts from (" +
                        /**/ "select-choose [ts1 ts] ts1 ts from (" +
                        /**/   "select [ts1] from t1 timestamp (ts1) where ts1 in '2025-12-01T01;2h'" +
                        /**/ ") union all select-choose [ts2 ts] ts2 ts from (" +
                        /**/   "select [ts2] from t2 timestamp (ts2) where ts2 in '2025-12-01T01;2h'" +
                        /**/ ") union all select-choose [ts3 ts] ts3 ts from (" +
                        /**/   "select [ts3] from t3 timestamp (ts3) where ts3 in '2025-12-01T01;2h'" +
                        /**/ ")" +
                        ")",
                "select ts from (select ts1 ts from t1 union all select ts2 ts from t2 union all select ts3 ts from t3) where ts in '2025-12-01T01;2h'",
                modelOf("t1").timestamp("ts1"),
                modelOf("t2").timestamp("ts2"),
                modelOf("t3").timestamp("ts3")
        );
    }

    @Test
    public void testPushFilterThroughUnionAllExprInAllBranches() throws SqlException {
        // Non-timestamp filter is not pushed into union branches.
        assertQuery(
                "select-choose c from (" +
                        /**/ "select [c] from (" +
                        /**/   "select-virtual [x + y c] x + y c from (" +
                        /**/     "select [y, x] from t1" +
                        /**/   ") union all select-virtual [x + y c] x + y c from (" +
                        /**/     "select [y, x] from t2" +
                        /**/   ")" +
                        /**/ ") _xQdbA1 where c > 5" +
                        ")",
                "select c from (select x + y c from t1 union all select x + y c from t2) where c > 5",
                modelOf("t1").col("x", ColumnType.INT).col("y", ColumnType.INT),
                modelOf("t2").col("x", ColumnType.INT).col("y", ColumnType.INT)
        );
    }

    @Test
    public void testPushFilterThroughUnionAllMixedPushability() throws SqlException {
        // Two filters: ts is the designated timestamp (pushed into branches for partition pruning),
        // c is a non-timestamp expression (stays at parent level only).
        assertQuery(
                "select-choose ts, c from (" +
                        /**/ "select [ts, c] from (" +
                        /**/   "select-virtual [x + y c, ts] ts, x + y c from (" +
                        /**/     "select-choose [y, x, ts1 ts] ts1 ts, y, x from (" +
                        /**/       "select [y, x, ts1] from t1 timestamp (ts1) where ts1 in '2025-12-01T01;2h'" +
                        /**/     ")" +
                        /**/   ")" +
                        /**/   " union all select-virtual [x + y c, ts] ts, x + y c from (" +
                        /**/     "select-choose [y, x, ts2 ts] ts2 ts, y, x from (" +
                        /**/       "select [y, x, ts2] from t2 timestamp (ts2) where ts2 in '2025-12-01T01;2h'" +
                        /**/     ")" +
                        /**/   ")" +
                        /**/ ") _xQdbA1 where c > 5" +
                        ")",
                "select ts, c from (select ts1 ts, x + y c from t1 union all select ts2 ts, x + y c from t2) where ts in '2025-12-01T01;2h' and c > 5",
                modelOf("t1").timestamp("ts1").col("x", ColumnType.INT).col("y", ColumnType.INT),
                modelOf("t2").timestamp("ts2").col("x", ColumnType.INT).col("y", ColumnType.INT)
        );
    }

    @Test
    public void testPushFilterThroughUnionAllPartialPushdownToBranch1of2() throws SqlException {
        // SAMPLE BY on the second branch blocks pushdown into that branch.
        // Filter is pushed into the first branch; parent filter must be retained.
        createModelsAndRun(
                () -> {
                    try (SqlCompiler compiler = engine.getSqlCompiler()) {
                        sink.clear();
                        ExecutionModel model = compiler.generateExecutionModel(
                                """
                                        SELECT ts, x FROM (
                                            SELECT ts1 ts, x1 x FROM t1
                                            UNION ALL
                                            SELECT ts2 ts, sum(x2) x FROM t2 SAMPLE BY 1h
                                        ) WHERE ts IN '2025-12-01T01;30m'
                                        """,
                                sqlExecutionContext);
                        ((Sinkable) model).toSink(sink);
                        String plan = sink.toString();

                        Assert.assertTrue(
                                "first branch must have timestamp filter pushed: " + plan,
                                plan.contains("where ts1 in '2025-12-01T01;30m'"));
                        Assert.assertFalse(
                                "SAMPLE BY branch must NOT have timestamp filter pushed: " + plan,
                                plan.contains("where ts2 in '2025-12-01T01;30m'"));
                        Assert.assertTrue(
                                "parent filter must be retained for partial pushdown: " + plan,
                                plan.contains("where ts in '2025-12-01T01;30m'"));
                    }
                },
                modelOf("t1").timestamp("ts1").col("x1", ColumnType.DOUBLE),
                modelOf("t2").timestamp("ts2").col("x2", ColumnType.DOUBLE)
        );
    }

    @Test
    public void testPushFilterThroughUnionAllPartialPushdownToBranch2of2() throws SqlException {
        // One branch has SAMPLE BY which blocks timestamp filter pushdown.
        // Filter is pushed into the other branch; parent filter must be retained.
        createModelsAndRun(
                () -> {
                    try (SqlCompiler compiler = engine.getSqlCompiler()) {
                        sink.clear();
                        ExecutionModel model = compiler.generateExecutionModel(
                                """
                                        SELECT ts, x FROM (
                                            SELECT ts1 ts, sum(x1) x FROM t1 SAMPLE BY 1h
                                            UNION ALL
                                            SELECT ts2 ts, x2 x FROM t2
                                        ) WHERE ts IN '2025-12-01T01;30m'
                                        """,
                                sqlExecutionContext);
                        ((Sinkable) model).toSink(sink);
                        String plan = sink.toString();

                        Assert.assertFalse(
                                "SAMPLE BY branch must NOT have timestamp filter pushed: " + plan,
                                plan.contains("where ts1 in '2025-12-01T01;30m'"));
                        Assert.assertTrue(
                                "second branch must have timestamp filter pushed: " + plan,
                                plan.contains("where ts2 in '2025-12-01T01;30m'"));
                        Assert.assertTrue(
                                "parent filter must be retained for partial pushdown: " + plan,
                                plan.contains("where ts in '2025-12-01T01;30m'"));
                    }
                },
                modelOf("t1").timestamp("ts1").col("x1", ColumnType.DOUBLE),
                modelOf("t2").timestamp("ts2").col("x2", ColumnType.DOUBLE)
        );
    }

    @Test
    public void testPushFilterThroughUnionAllPartialPushdownToBranches1and2of3() throws SqlException {
        // SAMPLE BY on the third of three branches blocks pushdown into that branch.
        // Filter is pushed into branches 1 and 2; parent filter must be retained.
        createModelsAndRun(
                () -> {
                    try (SqlCompiler compiler = engine.getSqlCompiler()) {
                        sink.clear();
                        ExecutionModel model = compiler.generateExecutionModel(
                                """
                                        SELECT ts, x FROM (
                                            SELECT ts1 ts, x1 x FROM t1
                                            UNION ALL
                                            SELECT ts2 ts, x2 x FROM t2
                                            UNION ALL
                                            SELECT ts3 ts, sum(x3) x FROM t3 SAMPLE BY 1h
                                        ) WHERE ts IN '2025-12-01T01;30m'
                                        """,
                                sqlExecutionContext);
                        ((Sinkable) model).toSink(sink);
                        String plan = sink.toString();

                        Assert.assertTrue(
                                "first branch must have timestamp filter pushed: " + plan,
                                plan.contains("where ts1 in '2025-12-01T01;30m'"));
                        Assert.assertTrue(
                                "second branch must have timestamp filter pushed: " + plan,
                                plan.contains("where ts2 in '2025-12-01T01;30m'"));
                        Assert.assertFalse(
                                "SAMPLE BY branch must NOT have timestamp filter pushed: " + plan,
                                plan.contains("where ts3 in '2025-12-01T01;30m'"));
                        Assert.assertTrue(
                                "parent filter must be retained for partial pushdown: " + plan,
                                plan.contains("where ts in '2025-12-01T01;30m'"));
                    }
                },
                modelOf("t1").timestamp("ts1").col("x1", ColumnType.DOUBLE),
                modelOf("t2").timestamp("ts2").col("x2", ColumnType.DOUBLE),
                modelOf("t3").timestamp("ts3").col("x3", ColumnType.DOUBLE)
        );
    }

    @Test
    public void testPushFilterThroughUnionAllPartialPushdownToBranches1and3of3() throws SqlException {
        // SAMPLE BY on the second of three branches blocks pushdown into that branch.
        // Filter is pushed into branches 1 and 3; parent filter must be retained.
        createModelsAndRun(
                () -> {
                    try (SqlCompiler compiler = engine.getSqlCompiler()) {
                        sink.clear();
                        ExecutionModel model = compiler.generateExecutionModel(
                                """
                                        SELECT ts, x FROM (
                                            SELECT ts1 ts, x1 x FROM t1
                                            UNION ALL
                                            SELECT ts2 ts, sum(x2) x FROM t2 SAMPLE BY 1h
                                            UNION ALL
                                            SELECT ts3 ts, x3 x FROM t3
                                        ) WHERE ts IN '2025-12-01T01;30m'
                                        """,
                                sqlExecutionContext);
                        ((Sinkable) model).toSink(sink);
                        String plan = sink.toString();

                        Assert.assertTrue(
                                "first branch must have timestamp filter pushed: " + plan,
                                plan.contains("where ts1 in '2025-12-01T01;30m'"));
                        Assert.assertFalse(
                                "SAMPLE BY branch must NOT have timestamp filter pushed: " + plan,
                                plan.contains("where ts2 in '2025-12-01T01;30m'"));
                        Assert.assertTrue(
                                "third branch must have timestamp filter pushed: " + plan,
                                plan.contains("where ts3 in '2025-12-01T01;30m'"));
                        Assert.assertTrue(
                                "parent filter must be retained for partial pushdown: " + plan,
                                plan.contains("where ts in '2025-12-01T01;30m'"));
                    }
                },
                modelOf("t1").timestamp("ts1").col("x1", ColumnType.DOUBLE),
                modelOf("t2").timestamp("ts2").col("x2", ColumnType.DOUBLE),
                modelOf("t3").timestamp("ts3").col("x3", ColumnType.DOUBLE)
        );
    }

    @Test
    public void testPushFilterThroughUnionAllSameTable() throws SqlException {
        // Both branches reference the same table; filter should be pushed to both independently.
        assertQuery(
                "select-choose ts from (" +
                        /**/ "select-choose [ts] ts from (" +
                        /**/   "select [ts] from t1 timestamp (ts) where ts in '2025-12-01T01;2h'" +
                        /**/ ") union all select-choose [ts] ts from (" +
                        /**/   "select [ts] from t1 timestamp (ts) where ts in '2025-12-01T01;2h'" +
                        /**/ ")" +
                        ")",
                "select ts from (select ts from t1 union all select ts from t1) where ts in '2025-12-01T01;2h'",
                modelOf("t1").timestamp("ts")
        );
    }

    @Test
    public void testPushFilterWithMultipleColumnsThroughUnionAll() throws SqlException {
        // Filter references two columns (ts and v); not pushed because it references a non-timestamp column.
        assertQuery(
                "select-choose ts, v from (" +
                        /**/ "select [ts, v] from (" +
                        /**/   "select-choose [val1 v, ts1 ts] ts1 ts, val1 v from (" +
                        /**/     "select [val1, ts1] from t1" +
                        /**/   ") union all select-choose [val2 v, ts2 ts] ts2 ts, val2 v from (" +
                        /**/     "select [val2, ts2] from t2" +
                        /**/   ")" +
                        /**/ ") _xQdbA1 where ts > v" +
                        ")",
                "select ts, v from (select ts1 ts, val1 v from t1 union all select ts2 ts, val2 v from t2) where ts > v",
                modelOf("t1").col("ts1", ColumnType.TIMESTAMP).col("val1", ColumnType.INT),
                modelOf("t2").col("ts2", ColumnType.TIMESTAMP).col("val2", ColumnType.INT)
        );
    }
}
