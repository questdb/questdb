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

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class FilterPushdownIntoUnionTest extends AbstractCairoTest {

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
            assertQuery("""
                    SELECT * FROM (
                        SELECT ts, sym, x FROM t_sym LATEST ON ts PARTITION BY sym
                        UNION ALL
                        SELECT ts, sym, x FROM t_plain
                    ) WHERE ts <= '2024-01-02'""")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            ts\tsym\tx
                            2024-01-02T00:00:00.000000Z\tC\t99
                            """);
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
            assertQuery("""
                    SELECT * FROM (
                        SELECT * FROM (SELECT ts, x FROM t1 LIMIT 2)
                        UNION ALL
                        SELECT ts, x FROM t2
                    ) WHERE ts >= '2024-01-03'""")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            ts\tx
                            2024-01-03T00:00:00.000000Z\t30
                            """);
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
            assertQuery("""
                    SELECT * FROM (
                        SELECT ts, x FROM t1
                        UNION ALL
                        SELECT ts, x FROM t2
                        LIMIT 3
                    ) WHERE ts >= '2024-01-03'""")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            ts\tx
                            2024-01-03T00:00:00.000000Z\t3
                            """);
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
            assertQuery("select * from (" +
                    "select ts, x from t1 " +
                    "union all " +
                    "select ts, avg(x) x from t2 sample by 1h align to first observation" +
                    ") where x > 5")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            ts\tx
                            2024-01-01T00:00:00.000000Z\t9.0
                            """);
        });
    }

    @Test
    public void testPushDownTimestampFilterThroughUnion() throws Exception {
        assertQuery("SELECT ts FROM (SELECT ts1 ts FROM t1 UNION SELECT ts2 ts FROM t2) WHERE ts IN '2025-12-01T01;2h'")
                .ddl("CREATE TABLE t1 (ts1 TIMESTAMP) TIMESTAMP(ts1)", "CREATE TABLE t2 (ts2 TIMESTAMP) TIMESTAMP(ts2)")
                .assertsLogicalPlan("""
                        Project
                          columns: [ts]
                          Union
                            Project
                              columns: [ts1 AS ts]
                              Filter
                                predicate: in(ts1, '2025-12-01T01;2h')
                                Scan
                                  table: t1
                                  columns: [ts1]
                            Project
                              columns: [ts2 AS ts]
                              Filter
                                predicate: in(ts2, '2025-12-01T01;2h')
                                Scan
                                  table: t2
                                  columns: [ts2]
                        """);
    }

    @Test
    public void testPushDownTimestampFilterThroughUnionAll() throws Exception {
        assertQuery("SELECT ts FROM (SELECT ts1 ts FROM t1 UNION ALL SELECT ts2 ts FROM t2) WHERE ts IN '2025-12-01T01;2h'")
                .ddl("CREATE TABLE t1 (ts1 TIMESTAMP) TIMESTAMP(ts1)", "CREATE TABLE t2 (ts2 TIMESTAMP) TIMESTAMP(ts2)")
                .assertsLogicalPlan("""
                        Project
                          columns: [ts]
                          Union All
                            Project
                              columns: [ts1 AS ts]
                              Filter
                                predicate: in(ts1, '2025-12-01T01;2h')
                                Scan
                                  table: t1
                                  columns: [ts1]
                            Project
                              columns: [ts2 AS ts]
                              Filter
                                predicate: in(ts2, '2025-12-01T01;2h')
                                Scan
                                  table: t2
                                  columns: [ts2]
                        """);
    }

    @Test
    public void testPushDownTimestampFilterThroughUnionAllCte() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (ts1 TIMESTAMP) TIMESTAMP(ts1)");
            execute("CREATE TABLE t2 (ts2 TIMESTAMP) TIMESTAMP(ts2)");
            assertQuery("""
                    WITH u AS (SELECT ts1 ts FROM t1 UNION ALL SELECT ts2 ts FROM t2)
                    SELECT ts FROM u WHERE ts IN '2025-12-01T01;2h'
                    """)
                    .noLeakCheck()
                    .assertsLogicalPlan("""
                            Project
                              columns: [ts]
                              Union All
                                Project
                                  columns: [ts1 AS ts]
                                  Filter
                                    predicate: in(ts1, '2025-12-01T01;2h')
                                    Scan
                                      table: t1
                                      columns: [ts1]
                                Project
                                  columns: [ts2 AS ts]
                                  Filter
                                    predicate: in(ts2, '2025-12-01T01;2h')
                                    Scan
                                      table: t2
                                      columns: [ts2]
                            """);
            assertQuery("""
                    WITH l AS (SELECT ts1 ts FROM t1),
                    r AS (SELECT ts2 ts FROM t2)
                    SELECT ts FROM (l UNION ALL r)
                    WHERE ts IN '2025-12-01T01;2h'
                    """)
                    .noLeakCheck()
                    .assertsLogicalPlan("""
                            Project
                              columns: [ts]
                              Union All
                                Project
                                  columns: [ts1 AS ts]
                                  Filter
                                    predicate: in(ts1, '2025-12-01T01;2h')
                                    Scan
                                      table: t1
                                      columns: [ts1]
                                Project
                                  columns: [ts2 AS ts]
                                  Filter
                                    predicate: in(ts2, '2025-12-01T01;2h')
                                    Scan
                                      table: t2
                                      columns: [ts2]
                            """);
        });
    }

    @Test
    public void testPushDownTimestampFilterThroughUnionAllMismatchedAliases() throws Exception {
        assertQuery("""
                SELECT ts FROM (
                    SELECT name1, ts1 ts, sym1 FROM t1
                    UNION ALL
                    SELECT name2, ts2, sym2 ts FROM t2
                ) WHERE ts IN '2025-12-01T01;2h'
                """)
                .ddl("CREATE TABLE t1 (name1 VARCHAR, ts1 TIMESTAMP, sym1 SYMBOL) TIMESTAMP(ts1)", "CREATE TABLE t2 (name2 VARCHAR, ts2 TIMESTAMP, sym2 SYMBOL) TIMESTAMP(ts2)")
                .assertsLogicalPlan("""
                        Project
                          columns: [ts]
                          Union All
                            Project
                              columns: [ts1 AS ts]
                              Filter
                                predicate: in(ts1, '2025-12-01T01;2h')
                                Scan
                                  table: t1
                                  columns: [ts1]
                            Project
                              columns: [ts2]
                              Filter
                                predicate: in(ts2, '2025-12-01T01;2h')
                                Scan
                                  table: t2
                                  columns: [ts2]
                        """);
    }

    @Test
    public void testPushDownTimestampFilterThroughUnionAllNonPushableBranch() throws Exception {
        assertQuery("SELECT ts FROM (SELECT ts1 ts FROM t1 UNION ALL SELECT ts2 + 1 ts FROM t2) WHERE ts IN '2025-12-01T01;2h'")
                .ddl("CREATE TABLE t1 (ts1 TIMESTAMP) TIMESTAMP(ts1)", "CREATE TABLE t2 (ts2 TIMESTAMP) TIMESTAMP(ts2)")
                .assertsLogicalPlan("""
                        Project
                          columns: [ts]
                          Union All
                            Project
                              columns: [ts1 AS ts]
                              Filter
                                predicate: in(ts1, '2025-12-01T01;2h')
                                Scan
                                  table: t1
                                  columns: [ts1]
                            Filter
                              predicate: in(ts, '2025-12-01T01;2h')
                              Project
                                columns: [ts2 + 1 AS ts]
                                Scan
                                  table: t2
                                  columns: [ts2]
                        """);
    }

    @Test
    public void testPushDownTimestampFilterThroughUnionAllThreeBranches() throws Exception {
        assertQuery("SELECT ts FROM (SELECT ts1 ts FROM t1 UNION ALL SELECT ts2 ts FROM t2 UNION ALL SELECT ts3 ts FROM t3) WHERE ts IN '2025-12-01T01;2h'")
                .ddl("CREATE TABLE t1 (ts1 TIMESTAMP) TIMESTAMP(ts1)", "CREATE TABLE t2 (ts2 TIMESTAMP) TIMESTAMP(ts2)", "CREATE TABLE t3 (ts3 TIMESTAMP) TIMESTAMP(ts3)")
                .assertsLogicalPlan("""
                        Project
                          columns: [ts]
                          Union All
                            Union All
                              Project
                                columns: [ts1 AS ts]
                                Filter
                                  predicate: in(ts1, '2025-12-01T01;2h')
                                  Scan
                                    table: t1
                                    columns: [ts1]
                              Project
                                columns: [ts2 AS ts]
                                Filter
                                  predicate: in(ts2, '2025-12-01T01;2h')
                                  Scan
                                    table: t2
                                    columns: [ts2]
                            Project
                              columns: [ts3 AS ts]
                              Filter
                                predicate: in(ts3, '2025-12-01T01;2h')
                                Scan
                                  table: t3
                                  columns: [ts3]
                        """);
    }

    @Test
    public void testPushFilterThroughUnionAllExprInAllBranches() throws Exception {
        assertQuery("SELECT c FROM (SELECT x + y c FROM t1 UNION ALL SELECT x + y c FROM t2) WHERE c > 5")
                .ddl("CREATE TABLE t1 (x INT, y INT)", "CREATE TABLE t2 (x INT, y INT)")
                .assertsLogicalPlan("""
                        Project
                          columns: [c]
                          Filter
                            predicate: c > 5
                            Union All
                              Project
                                columns: [x + y AS c]
                                Scan
                                  table: t1
                                  columns: [x, y]
                              Project
                                columns: [x + y AS c]
                                Scan
                                  table: t2
                                  columns: [x, y]
                        """);
    }

    @Test
    public void testPushFilterThroughUnionAllMixedPushability() throws Exception {
        assertQuery("SELECT ts, c FROM (SELECT ts1 ts, x + y c FROM t1 UNION ALL SELECT ts2 ts, x + y c FROM t2) WHERE ts IN '2025-12-01T01;2h' AND c > 5")
                .ddl("CREATE TABLE t1 (ts1 TIMESTAMP, x INT, y INT) TIMESTAMP(ts1)", "CREATE TABLE t2 (ts2 TIMESTAMP, x INT, y INT) TIMESTAMP(ts2)")
                .assertsLogicalPlan("""
                        Project
                          columns: [ts, c]
                          Filter
                            predicate: c > 5
                            Union All
                              Project
                                columns: [ts1 AS ts, x + y AS c]
                                Filter
                                  predicate: in(ts1, '2025-12-01T01;2h')
                                  Scan
                                    table: t1
                                    columns: [ts1, x, y]
                              Project
                                columns: [ts2 AS ts, x + y AS c]
                                Filter
                                  predicate: in(ts2, '2025-12-01T01;2h')
                                  Scan
                                    table: t2
                                    columns: [ts2, x, y]
                        """);
    }

    @Test
    public void testPushFilterThroughUnionAllPartialPushdownToBranch1of2() throws Exception {
        assertQuery("""
                SELECT ts, x FROM (
                    SELECT ts1 ts, x1 x FROM t1
                    UNION ALL
                    SELECT ts2 ts, sum(x2) x FROM t2 SAMPLE BY 1h
                ) WHERE ts IN '2025-12-01T01;30m'
                """)
                .ddl("CREATE TABLE t1 (ts1 TIMESTAMP, x1 DOUBLE) TIMESTAMP(ts1)", "CREATE TABLE t2 (ts2 TIMESTAMP, x2 DOUBLE) TIMESTAMP(ts2)")
                .assertsLogicalPlan("""
                        Project
                          columns: [ts, x]
                          Union All
                            Project
                              columns: [ts1 AS ts, x1 AS x]
                              Filter
                                predicate: in(ts1, '2025-12-01T01;30m')
                                Scan
                                  table: t1
                                  columns: [ts1, x1]
                            Sort
                              keys: [ts]
                              Project
                                columns: [ts, x]
                                Filter
                                  predicate: in(ts, '2025-12-01T01;30m')
                                  Aggregate
                                    keys: [timestamp_floor_utc('1h', ts2, null, '00:00', null) AS ts]
                                    values: [sum(x2) AS x]
                                    Scan
                                      table: t2
                                      columns: [ts2, x2]
                        """);
    }

    @Test
    public void testPushFilterThroughUnionAllPartialPushdownToBranch2of2() throws Exception {
        assertQuery("""
                SELECT ts, x FROM (
                    SELECT ts1 ts, sum(x1) x FROM t1 SAMPLE BY 1h
                    UNION ALL
                    SELECT ts2 ts, x2 x FROM t2
                ) WHERE ts IN '2025-12-01T01;30m'
                """)
                .ddl("CREATE TABLE t1 (ts1 TIMESTAMP, x1 DOUBLE) TIMESTAMP(ts1)", "CREATE TABLE t2 (ts2 TIMESTAMP, x2 DOUBLE) TIMESTAMP(ts2)")
                .assertsLogicalPlan("""
                        Project
                          columns: [ts, x]
                          Union All
                            Sort
                              keys: [ts]
                              Project
                                columns: [ts, x]
                                Filter
                                  predicate: in(ts, '2025-12-01T01;30m')
                                  Aggregate
                                    keys: [timestamp_floor_utc('1h', ts1, null, '00:00', null) AS ts]
                                    values: [sum(x1) AS x]
                                    Scan
                                      table: t1
                                      columns: [ts1, x1]
                            Project
                              columns: [ts2 AS ts, x2 AS x]
                              Filter
                                predicate: in(ts2, '2025-12-01T01;30m')
                                Scan
                                  table: t2
                                  columns: [ts2, x2]
                        """);
    }

    @Test
    public void testPushFilterThroughUnionAllPartialPushdownToBranches1and2of3() throws Exception {
        assertQuery("""
                SELECT ts, x FROM (
                    SELECT ts1 ts, x1 x FROM t1
                    UNION ALL
                    SELECT ts2 ts, x2 x FROM t2
                    UNION ALL
                    SELECT ts3 ts, sum(x3) x FROM t3 SAMPLE BY 1h
                ) WHERE ts IN '2025-12-01T01;30m'
                """)
                .ddl("CREATE TABLE t1 (ts1 TIMESTAMP, x1 DOUBLE) TIMESTAMP(ts1)", "CREATE TABLE t2 (ts2 TIMESTAMP, x2 DOUBLE) TIMESTAMP(ts2)", "CREATE TABLE t3 (ts3 TIMESTAMP, x3 DOUBLE) TIMESTAMP(ts3)")
                .assertsLogicalPlan("""
                        Project
                          columns: [ts, x]
                          Union All
                            Union All
                              Project
                                columns: [ts1 AS ts, x1 AS x]
                                Filter
                                  predicate: in(ts1, '2025-12-01T01;30m')
                                  Scan
                                    table: t1
                                    columns: [ts1, x1]
                              Project
                                columns: [ts2 AS ts, x2 AS x]
                                Filter
                                  predicate: in(ts2, '2025-12-01T01;30m')
                                  Scan
                                    table: t2
                                    columns: [ts2, x2]
                            Sort
                              keys: [ts]
                              Project
                                columns: [ts, x]
                                Filter
                                  predicate: in(ts, '2025-12-01T01;30m')
                                  Aggregate
                                    keys: [timestamp_floor_utc('1h', ts3, null, '00:00', null) AS ts]
                                    values: [sum(x3) AS x]
                                    Scan
                                      table: t3
                                      columns: [ts3, x3]
                        """);
    }

    @Test
    public void testPushFilterThroughUnionAllPartialPushdownToBranches1and3of3() throws Exception {
        assertQuery("""
                SELECT ts, x FROM (
                    SELECT ts1 ts, x1 x FROM t1
                    UNION ALL
                    SELECT ts2 ts, sum(x2) x FROM t2 SAMPLE BY 1h
                    UNION ALL
                    SELECT ts3 ts, x3 x FROM t3
                ) WHERE ts IN '2025-12-01T01;30m'
                """)
                .ddl("CREATE TABLE t1 (ts1 TIMESTAMP, x1 DOUBLE) TIMESTAMP(ts1)", "CREATE TABLE t2 (ts2 TIMESTAMP, x2 DOUBLE) TIMESTAMP(ts2)", "CREATE TABLE t3 (ts3 TIMESTAMP, x3 DOUBLE) TIMESTAMP(ts3)")
                .assertsLogicalPlan("""
                        Project
                          columns: [ts, x]
                          Union All
                            Union All
                              Project
                                columns: [ts1 AS ts, x1 AS x]
                                Filter
                                  predicate: in(ts1, '2025-12-01T01;30m')
                                  Scan
                                    table: t1
                                    columns: [ts1, x1]
                              Sort
                                keys: [ts]
                                Project
                                  columns: [ts, x]
                                  Filter
                                    predicate: in(ts, '2025-12-01T01;30m')
                                    Aggregate
                                      keys: [timestamp_floor_utc('1h', ts2, null, '00:00', null) AS ts]
                                      values: [sum(x2) AS x]
                                      Scan
                                        table: t2
                                        columns: [ts2, x2]
                            Project
                              columns: [ts3 AS ts, x3 AS x]
                              Filter
                                predicate: in(ts3, '2025-12-01T01;30m')
                                Scan
                                  table: t3
                                  columns: [ts3, x3]
                        """);
    }

    @Test
    public void testPushFilterThroughUnionAllSameTable() throws Exception {
        assertQuery("SELECT ts FROM (SELECT ts FROM t1 UNION ALL SELECT ts FROM t1) WHERE ts IN '2025-12-01T01;2h'")
                .ddl("CREATE TABLE t1 (ts TIMESTAMP) TIMESTAMP(ts)")
                .assertsLogicalPlan("""
                        Project
                          columns: [ts]
                          Union All
                            Project
                              columns: [ts]
                              Filter
                                predicate: in(ts, '2025-12-01T01;2h')
                                Scan
                                  table: t1
                                  columns: [ts]
                            Project
                              columns: [ts]
                              Filter
                                predicate: in(ts, '2025-12-01T01;2h')
                                Scan
                                  table: t1
                                  columns: [ts]
                        """);
    }

    @Test
    public void testPushFilterWithMultipleColumnsThroughUnionAll() throws Exception {
        assertQuery("SELECT ts, v FROM (SELECT ts1 ts, val1 v FROM t1 UNION ALL SELECT ts2 ts, val2 v FROM t2) WHERE ts > v")
                .ddl("CREATE TABLE t1 (ts1 TIMESTAMP, val1 INT)", "CREATE TABLE t2 (ts2 TIMESTAMP, val2 INT)")
                .assertsLogicalPlan("""
                        Project
                          columns: [ts, v]
                          Filter
                            predicate: ts > v
                            Union All
                              Project
                                columns: [ts1 AS ts, val1 AS v]
                                Scan
                                  table: t1
                                  columns: [ts1, val1]
                              Project
                                columns: [ts2 AS ts, val2 AS v]
                                Scan
                                  table: t2
                                  columns: [ts2, val2]
                        """);
    }
}
