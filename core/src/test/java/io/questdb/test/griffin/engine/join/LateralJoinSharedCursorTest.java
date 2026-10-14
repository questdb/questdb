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

package io.questdb.test.griffin.engine.join;

import io.questdb.PropertyKey;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.functions.test.TestLatchedCounterFunctionFactory;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.QueryAssertion;
import io.questdb.test.tools.MutationStep;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class LateralJoinSharedCursorTest extends AbstractCairoTest {
    private final boolean enableParallelGroupBy;

    public LateralJoinSharedCursorTest(boolean enableParallelGroupBy) {
        this.enableParallelGroupBy = enableParallelGroupBy;
    }

    @Parameterized.Parameters(name = "parallel={0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {false},
                {true},
        });
    }

    @Before
    public void setUp() {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED, String.valueOf(enableParallelGroupBy));
        super.setUp();
    }

    @Test
    public void testAsyncKeyedGroupByOuter() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (category SYMBOL, region SYMBOL, amount DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE rates (min_amount DOUBLE, rate DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    ('A', 'US', 10.0, '2024-01-01T00:00:00.000000Z'),
                    ('A', 'US', 20.0, '2024-01-01T01:00:00.000000Z'),
                    ('B', 'EU',  5.0, '2024-01-01T02:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO rates VALUES
                    (10.0, 0.1, '2024-01-01T00:00:00.000000Z'),
                    (25.0, 0.2, '2024-01-01T00:00:01.000000Z')
                    """);

            assertQuery("""
                    SELECT o.category, o.region, o.total, sub.rate
                    FROM (
                        SELECT category, region, sum(amount) AS total
                        FROM orders
                        GROUP BY category, region
                    ) o
                    JOIN LATERAL (
                        SELECT rate FROM rates WHERE min_amount <= o.total
                    ) sub
                    ORDER BY o.category, o.region, sub.rate
                    """)
                    .withPlan(enableParallelGroupBy ? """
                            Encode sort
                              keys: [category, region, rate]
                                SelectedRecord
                                    Hash Join
                                      condition: sub.__qdb_outer_ref__0_total=o.total
                                        Async Group By workers: 1
                                          keys: [category,region]
                                          values: [sum(amount)]
                                          filter: null
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: orders
                                        Hash
                                            SelectedRecord
                                                Filter filter: __qdb_outer_ref__0.__qdb_outer_ref__0_total>=rates.min_amount
                                                    Cross Join
                                                        PageFrame
                                                            Row forward scan
                                                            Frame forward scan on: rates
                                                        GroupBy vectorized: false
                                                          keys: [__qdb_outer_ref__0_total]
                                                            SelectedRecord
                                                                (Shared)
                                                                    Async Group By workers: 1
                                                                      keys: [category,region]
                                                                      values: [sum(amount)]
                                                                      filter: null
                                                                        PageFrame
                                                                            Row forward scan
                                                                            Frame forward scan on: orders
                            """ : """
                            Encode sort
                              keys: [category, region, rate]
                                SelectedRecord
                                    Hash Join
                                      condition: sub.__qdb_outer_ref__0_total=o.total
                                        GroupBy vectorized: false
                                          keys: [category,region]
                                          values: [sum(amount)]
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: orders
                                        Hash
                                            SelectedRecord
                                                Filter filter: __qdb_outer_ref__0.__qdb_outer_ref__0_total>=rates.min_amount
                                                    Cross Join
                                                        PageFrame
                                                            Row forward scan
                                                            Frame forward scan on: rates
                                                        GroupBy vectorized: false
                                                          keys: [__qdb_outer_ref__0_total]
                                                            SelectedRecord
                                                                (Shared)
                                                                    GroupBy vectorized: false
                                                                      keys: [category,region]
                                                                      values: [sum(amount)]
                                                                        PageFrame
                                                                            Row forward scan
                                                                            Frame forward scan on: orders
                            """)
                    .expectSize()
                    .returns("""
                            category\tregion\ttotal\trate
                            A\tUS\t30.0\t0.1
                            A\tUS\t30.0\t0.2
                            """);
        });
    }

    @Test
    public void testAsyncKeyedGroupByOuterSharded() throws Exception {
        Assume.assumeTrue(enableParallelGroupBy);
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, "1");
            execute("CREATE TABLE orders (id INT, region SYMBOL, amount DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("CREATE TABLE rates (min_amount DOUBLE, rate DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");

            StringBuilder sb = new StringBuilder("INSERT INTO orders VALUES ");
            for (int hour = 0; hour < 4; hour++) {
                for (int i = 0; i < 50; i++) {
                    if (hour > 0 || i > 0) {
                        sb.append(',');
                    }
                    sb.append("(").append(i).append(", 'R").append(i % 5).append("', ")
                            .append(i * 10.0).append(", '2024-01-01T0").append(hour)
                            .append(":00:0").append(i % 10).append(".000000Z')");
                }
            }
            execute(sb.toString());
            execute("""
                    INSERT INTO rates VALUES
                    (1000.0, 0.1, '2024-01-01T00:00:00.000000Z')
                    """);

            assertQuery("""
                    SELECT o.id, o.region, o.total, sub.rate
                    FROM (
                        SELECT id, region, sum(amount) AS total
                        FROM orders
                        GROUP BY id, region
                    ) o
                    JOIN LATERAL (
                        SELECT rate FROM rates WHERE min_amount <= o.total
                    ) sub
                    WHERE o.total > 1900
                    ORDER BY o.id
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id	region	total	rate
                            48	R3	1920.0	0.1
                            49	R4	1960.0	0.1
                            """);
        });
    }

    @Test
    public void testAsyncNotKeyedGroupByOuter() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (x DOUBLE, y DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE rates (min_val DOUBLE, rate DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1.0, 2.0, '2024-01-01T00:00:00.000000Z'),
                    (2.0, 4.0, '2024-01-01T01:00:00.000000Z'),
                    (3.0, 6.0, '2024-01-01T02:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO rates VALUES
                    (0.5, 0.1, '2024-01-01T00:00:00.000000Z')
                    """);

            assertQuery("""
                    SELECT o.correlation, sub.rate
                    FROM (SELECT corr(x, y) AS correlation FROM orders) o
                    JOIN LATERAL (
                        SELECT rate FROM rates WHERE min_val <= o.correlation
                    ) sub
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            correlation\trate
                            1.0\t0.1
                            """);
        });
    }

    @Test
    public void testExpressionKeyGroupByOuter() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, amount DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE rates (min_amount DOUBLE, rate DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, 10.0, '2024-01-01T00:00:00.000000Z'),
                    (2, 20.0, '2024-01-01T01:00:00.000000Z'),
                    (3,  5.0, '2024-01-01T02:00:00.000000Z'),
                    (4, 15.0, '2024-01-01T03:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO rates VALUES
                    (10.0, 0.1, '2024-01-01T00:00:00.000000Z'),
                    (25.0, 0.2, '2024-01-01T00:00:01.000000Z')
                    """);

            assertQuery("""
                    SELECT o.bucket, o.total, sub.rate
                    FROM (SELECT id % 2 AS bucket, sum(amount) AS total FROM orders GROUP BY id % 2) o
                    JOIN LATERAL (
                        SELECT rate FROM rates WHERE min_amount <= o.total
                    ) sub
                    ORDER BY o.bucket, sub.rate
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            bucket\ttotal\trate
                            0\t35.0\t0.1
                            0\t35.0\t0.2
                            1\t15.0\t0.1
                            """);
        });
    }

    @Test
    public void testJoinGateBindVariableOverSharedGroupBy() throws Exception {
        // A bind variable in an INNER join ON clause gates the GROUP BY of the joined table when a RIGHT join
        // follows it, and the LATERAL reads the same GROUP BY through a shared cursor. The gate opens the GROUP BY
        // in every execution of one factory, also while the bind variable is false, so the shared cursor reads
        // the state of the current execution: the first execution with false read a primary cursor that was never
        // opened, and an execution with false after one with true read the state that the previous execution
        // closed. The threshold makes the parallel GROUP BY shard its maps, which then left no owner map to read.
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, "1");
            createGateTables();
            execute("CREATE TABLE t64 AS (SELECT (x % 64)::INT k, x::INT v FROM long_sequence(128))");
            final String query = """
                    SELECT a.id, o.n, l.mv, d.id did
                    FROM a
                    JOIN (SELECT k, count() n FROM t64 GROUP BY k) o ON a.k = o.k AND $1
                    CROSS JOIN LATERAL (SELECT max(u.v) mv FROM u WHERE u.k <= o.k) l
                    RIGHT JOIN d ON d.k = a.k
                    ORDER BY did, a.id
                    """;
            bindVariableService.clear();
            bindVariableService.setBoolean(0, false);
            try (RecordCursorFactory factory = select(query)) {
                for (boolean flag : new boolean[]{false, true, false, true}) {
                    bindVariableService.setBoolean(0, flag);
                    new QueryAssertion(engine, factory)
                            .withContext(sqlExecutionContext)
                            .returns(flag
                                    ? """
                                    id\tn\tmv\tdid
                                    1\t2\t100\t1
                                    2\t2\t200\t2
                                    null\tnull\tnull\t3
                                    """
                                    : """
                                    id\tn\tmv\tdid
                                    null\tnull\tnull\t1
                                    null\tnull\tnull\t2
                                    null\tnull\tnull\t3
                                    """
                            );
                }
            }
        });
    }

    @Test
    public void testJoinGateConstantFalseOverSharedGroupBy() throws Exception {
        // A constant false ON conjunct of an INNER join empties the joined table when a RIGHT join follows it: the
        // code generator frees the GROUP BY of the table. The LATERAL read that GROUP BY through a shared cursor,
        // and read the freed factory; EXPLAIN failed too. The LATERAL now builds its own copy of the GROUP BY.
        assertMemoryLeak(() -> {
            createGateTables();
            final String groupBy = enableParallelGroupBy ? "GroupBy vectorized: true workers: 1" : "GroupBy vectorized: false";
            for (String conjunct : new String[]{"1 = 0", "false"}) {
                final String query = """
                        SELECT a.id, o.n, l.mv, d.id did
                        FROM a
                        JOIN (SELECT k, count() n FROM t GROUP BY k) o ON a.k = o.k AND %s
                        CROSS JOIN LATERAL (SELECT max(u.v) mv FROM u WHERE u.k <= o.k) l
                        RIGHT JOIN d ON d.k = a.k
                        ORDER BY did, a.id
                        """.formatted(conjunct);
                assertQuery(query)
                        .noLeakCheck()
                        .withPlan("""
                                Encode sort
                                  keys: [did, id]
                                    SelectedRecord
                                        Hash Right Outer Join Light
                                          condition: d.k=a.k
                                            Hash Left Outer Join Light
                                              condition: l.__qdb_outer_ref__0_k=o.k
                                                Hash Join Light
                                                  condition: o.k=a.k
                                                    PageFrame
                                                        Row forward scan
                                                        Frame forward scan on: a
                                                    Hash
                                                        Empty table
                                                Hash
                                                    GroupBy vectorized: false
                                                      keys: [__qdb_outer_ref__0_k]
                                                      values: [max(v)]
                                                        SelectedRecord
                                                            Filter filter: __qdb_outer_ref__0.__qdb_outer_ref__0_k>=u.k
                                                                Cross Join
                                                                    PageFrame
                                                                        Row forward scan
                                                                        Frame forward scan on: u
                                                                    GroupBy vectorized: false
                                                                      keys: [__qdb_outer_ref__0_k]
                                                                        SelectedRecord
                                                                            %s
                                                                              keys: [k]
                                                                              values: [count(*)]
                                                                                PageFrame
                                                                                    Row forward scan
                                                                                    Frame forward scan on: t
                                            Hash
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: d
                                """.formatted(groupBy))
                        .returns("""
                                id\tn\tmv\tdid
                                null\tnull\tnull\t1
                                null\tnull\tnull\t2
                                null\tnull\tnull\t3
                                """);
                assertQuery("""
                        SELECT count() FROM a
                        JOIN (SELECT k, count() n FROM t GROUP BY k) o ON a.k = o.k AND %s
                        CROSS JOIN LATERAL (SELECT max(u.v) mv FROM u WHERE u.k <= o.k) l
                        RIGHT JOIN d ON d.k = a.k
                        WHERE a.id IS NOT NULL
                        """.formatted(conjunct))
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                count
                                0
                                """);
            }
        });
    }

    @Test
    public void testJoinGateFalseLeavesLateralUnread() throws Exception {
        // While the gate of the INNER join is false, the INNER join knows that it has no rows, so the LEFT join
        // that aligns the LATERAL with it reads no row of the LATERAL, see RecordCursor.isKnownEmpty(). The
        // filter in the LATERAL counts the pairs of a row of u and a key of t that the correlation keeps:
        // 1 + 2 + 3 for the keys 1, 2 and 3 of t.
        assertMemoryLeak(() -> {
            createGateTables();
            final String query = """
                    SELECT a.id, l.mv, d.id did
                    FROM a
                    JOIN (SELECT k, count() n FROM t GROUP BY k) o ON a.k = o.k AND $1
                    CROSS JOIN LATERAL (SELECT max(u.v) mv FROM u WHERE u.k <= o.k AND test_latched_counter()) l
                    RIGHT JOIN d ON d.k = a.k
                    """;
            bindVariableService.clear();
            bindVariableService.setBoolean(0, false);
            final String groupBy = enableParallelGroupBy ? "GroupBy vectorized: true workers: 1" : "GroupBy vectorized: false";
            assertQuery(query)
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Hash Right Outer Join Light
                                  condition: d.k=a.k
                                    Hash Left Outer Join Light
                                      condition: l.__qdb_outer_ref__0_k=o.k
                                        Hash Join Light
                                          condition: o.k=a.k
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: a
                                            Hash
                                                Filter filter: $0::boolean
                                                    %1$s
                                                      keys: [k]
                                                      values: [count(*)]
                                                        PageFrame
                                                            Row forward scan
                                                            Frame forward scan on: t
                                        Hash
                                            GroupBy vectorized: false
                                              keys: [__qdb_outer_ref__0_k]
                                              values: [max(v)]
                                                SelectedRecord
                                                    Filter filter: (__qdb_outer_ref__0.__qdb_outer_ref__0_k>=u.k and test_latched_counter())
                                                        Cross Join
                                                            PageFrame
                                                                Row forward scan
                                                                Frame forward scan on: u
                                                            GroupBy vectorized: false
                                                              keys: [__qdb_outer_ref__0_k]
                                                                SelectedRecord
                                                                    (Shared)
                                                                        %1$s
                                                                          keys: [k]
                                                                          values: [count(*)]
                                                                            PageFrame
                                                                                Row forward scan
                                                                                Frame forward scan on: t
                                    Hash
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: d
                            """.formatted(groupBy));
            try (RecordCursorFactory factory = select(query)) {
                for (boolean flag : new boolean[]{false, true, false}) {
                    bindVariableService.setBoolean(0, flag);
                    TestLatchedCounterFunctionFactory.reset(null);
                    int rowCount = 0;
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        while (cursor.hasNext()) {
                            rowCount++;
                        }
                    }
                    Assert.assertEquals("the RIGHT join returns every row of d", 3, rowCount);
                    Assert.assertEquals("the rows that the LATERAL reads", flag ? 6 : 0, TestLatchedCounterFunctionFactory.getCount());
                }
            }
        });
    }

    @Test
    public void testJoinGateNotKeyedSharedGroupBy() throws Exception {
        // The gate of a not keyed GROUP BY: in parallel, the shared cursor read a primary cursor that was never
        // opened. A serial GROUP BY opened its base for the shared cursor itself and kept it open after the
        // execution, so the next execution with true read the exhausted base and returned NULL keys instead of
        // the joined rows.
        assertMemoryLeak(() -> {
            createGateTables();
            execute("CREATE TABLE d2 (id INT, k INT)");
            execute("INSERT INTO d2 VALUES (1, 1), (2, 3), (3, 5)");
            final String query = """
                    SELECT a.id, o.k, l.mv, d2.id did
                    FROM a
                    JOIN (SELECT max(k) k, first(v) f FROM t) o ON a.k = o.k AND $1
                    CROSS JOIN LATERAL (SELECT max(u.v) mv FROM u WHERE u.k <= o.k) l
                    RIGHT JOIN d2 ON d2.k = a.k
                    ORDER BY did, a.id
                    """;
            bindVariableService.clear();
            bindVariableService.setBoolean(0, false);
            try (RecordCursorFactory factory = select(query)) {
                for (boolean flag : new boolean[]{false, true, false, true}) {
                    bindVariableService.setBoolean(0, flag);
                    new QueryAssertion(engine, factory)
                            .withContext(sqlExecutionContext)
                            .returns(flag
                                    ? """
                                    id\tk\tmv\tdid
                                    null\tnull\tnull\t1
                                    3\t3\t300\t2
                                    null\tnull\tnull\t3
                                    """
                                    : """
                                    id\tk\tmv\tdid
                                    null\tnull\tnull\t1
                                    null\tnull\tnull\t2
                                    null\tnull\tnull\t3
                                    """
                            );
                    // the execution closed the base of the GROUP BY, which the factory outlives
                    Assert.assertEquals(0, engine.getBusyReaderCount());
                }
            }
        });
    }

    @Test
    public void testJoinGateRuntimeConstantOverSharedGroupBy() throws Exception {
        // A runtime constant in an INNER join ON clause gates the GROUP BY of the joined table when a RIGHT or FULL
        // join follows it, and the LATERAL reads the same GROUP BY through a shared cursor. While the constant was
        // false, the gate did not open the GROUP BY, and the shared cursor failed with a NullPointerException.
        assertMemoryLeak(() -> {
            createGateTables();
            final String groupBy = enableParallelGroupBy ? "GroupBy vectorized: true workers: 1" : "GroupBy vectorized: false";
            final String query = """
                    SELECT a.id, o.n, l.mv, d.id did
                    FROM a
                    JOIN (SELECT k, count() n FROM t GROUP BY k) o ON a.k %s o.k AND now() %s '2000-01-01'
                    CROSS JOIN LATERAL (SELECT max(u.v) mv FROM u WHERE u.k <= o.k) l
                    %s JOIN d ON d.k = a.k
                    ORDER BY did, a.id, o.k
                    """;
            final String expectedFalse = """
                    id\tn\tmv\tdid
                    null\tnull\tnull\t1
                    null\tnull\tnull\t2
                    null\tnull\tnull\t3
                    """;
            assertQuery(query.formatted("=", "<", "RIGHT"))
                    .noLeakCheck()
                    .withPlan("""
                            SelectedRecord
                                Encode sort
                                  keys: [did, id, k]
                                    SelectedRecord
                                        Hash Right Outer Join Light
                                          condition: d.k=a.k
                                            Hash Left Outer Join Light
                                              condition: l.__qdb_outer_ref__0_k=o.k
                                                Hash Join Light
                                                  condition: o.k=a.k
                                                    PageFrame
                                                        Row forward scan
                                                        Frame forward scan on: a
                                                    Hash
                                                        Filter filter: now()<2000-01-01T00:00:00.000000Z
                                                            %1$s
                                                              keys: [k]
                                                              values: [count(*)]
                                                                PageFrame
                                                                    Row forward scan
                                                                    Frame forward scan on: t
                                                Hash
                                                    GroupBy vectorized: false
                                                      keys: [__qdb_outer_ref__0_k]
                                                      values: [max(v)]
                                                        SelectedRecord
                                                            Filter filter: __qdb_outer_ref__0.__qdb_outer_ref__0_k>=u.k
                                                                Cross Join
                                                                    PageFrame
                                                                        Row forward scan
                                                                        Frame forward scan on: u
                                                                    GroupBy vectorized: false
                                                                      keys: [__qdb_outer_ref__0_k]
                                                                        SelectedRecord
                                                                            (Shared)
                                                                                %1$s
                                                                                  keys: [k]
                                                                                  values: [count(*)]
                                                                                    PageFrame
                                                                                        Row forward scan
                                                                                        Frame forward scan on: t
                                            Hash
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: d
                            """.formatted(groupBy))
                    .returns(expectedFalse);
            assertQuery(query.formatted("=", ">", "RIGHT"))
                    .noLeakCheck()
                    .returns("""
                            id\tn\tmv\tdid
                            1\t2\t100\t1
                            2\t2\t200\t2
                            null\tnull\tnull\t3
                            """);
            assertQuery(query.formatted("=", "<", "FULL")).noLeakCheck().returns(expectedFalse);
            assertQuery(query.formatted("=", ">", "FULL"))
                    .noLeakCheck()
                    .returns("""
                            id\tn\tmv\tdid
                            3\t1\t300\tnull
                            1\t2\t100\t1
                            2\t2\t200\t2
                            null\tnull\tnull\t3
                            """);
            // a nested loop INNER join
            assertQuery(query.formatted("<=", "<", "RIGHT")).noLeakCheck().returns(expectedFalse);
            assertQuery(query.formatted("<=", ">", "RIGHT"))
                    .noLeakCheck()
                    .returns("""
                            id\tn\tmv\tdid
                            1\t2\t100\t1
                            1\t2\t200\t1
                            1\t1\t300\t1
                            2\t2\t200\t2
                            2\t1\t300\t2
                            null\tnull\tnull\t3
                            """);
        });
    }

    @Test
    public void testKeyedGroupByOuter() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (category SYMBOL, amount DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE rates (min_amount DOUBLE, rate DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    ('A', 10.0, '2024-01-01T00:00:00.000000Z'),
                    ('A', 20.0, '2024-01-01T01:00:00.000000Z'),
                    ('B',  5.0, '2024-01-01T02:00:00.000000Z'),
                    ('B',  3.0, '2024-01-01T03:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO rates VALUES
                    (10.0, 0.1, '2024-01-01T00:00:00.000000Z'),
                    (25.0, 0.2, '2024-01-01T00:00:01.000000Z')
                    """);

            assertQuery("""
                    SELECT o.category, o.total, sub.rate
                    FROM (SELECT category, sum(amount) AS total FROM orders GROUP BY category) o
                    JOIN LATERAL (
                        SELECT rate FROM rates WHERE min_amount <= o.total
                    ) sub
                    ORDER BY o.category, sub.rate
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            category\ttotal\trate
                            A\t30.0\t0.1
                            A\t30.0\t0.2
                            """);
        });
    }

    @Test
    public void testKeyedGroupByOuterAggregateBeforeKey() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (category SYMBOL, amount DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE rates (min_amount DOUBLE, rate DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    ('A', 10.0, '2024-01-01T00:00:00.000000Z'),
                    ('A', 20.0, '2024-01-01T01:00:00.000000Z'),
                    ('B',  5.0, '2024-01-01T02:00:00.000000Z'),
                    ('B',  3.0, '2024-01-01T03:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO rates VALUES
                    (10.0, 0.1, '2024-01-01T00:00:00.000000Z'),
                    (25.0, 0.2, '2024-01-01T00:00:01.000000Z')
                    """);

            assertQuery("""
                    SELECT o.total, o.category, sub.rate
                    FROM (SELECT sum(amount) AS total, category FROM orders GROUP BY category) o
                    JOIN LATERAL (
                        SELECT rate FROM rates WHERE min_amount <= o.total
                    ) sub
                    ORDER BY o.category, sub.rate
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            total\tcategory\trate
                            30.0\tA\t0.1
                            30.0\tA\t0.2
                            """);
        });
    }

    @Test
    public void testKeyedGroupByOuterEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (category SYMBOL, amount DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE rates (min_amount DOUBLE, rate DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO rates VALUES
                    (10.0, 0.1, '2024-01-01T00:00:00.000000Z')
                    """);

            assertQuery("""
                    SELECT o.category, o.total, sub.rate
                    FROM (SELECT category, sum(amount) AS total FROM orders GROUP BY category) o
                    JOIN LATERAL (
                        SELECT rate FROM rates WHERE min_amount <= o.total
                    ) sub
                    ORDER BY o.category, sub.rate
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            category\ttotal\trate
                            """);
        });
    }

    @Test
    public void testKeyedGroupByOuterLeftJoin() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (category SYMBOL, amount DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE rates (min_amount DOUBLE, rate DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    ('A', 10.0, '2024-01-01T00:00:00.000000Z'),
                    ('A', 20.0, '2024-01-01T01:00:00.000000Z'),
                    ('B',  3.0, '2024-01-01T02:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO rates VALUES
                    (10.0, 0.1, '2024-01-01T00:00:00.000000Z'),
                    (25.0, 0.2, '2024-01-01T00:00:01.000000Z')
                    """);

            assertQuery("""
                    SELECT o.category, o.total, sub.rate
                    FROM (SELECT category, sum(amount) AS total FROM orders GROUP BY category) o
                    LEFT JOIN LATERAL (
                        SELECT rate FROM rates WHERE min_amount <= o.total
                    ) sub
                    ORDER BY o.category, sub.rate
                    """)
                    .noLeakCheck()
                    .returns("""
                            category\ttotal\trate
                            A\t30.0\t0.1
                            A\t30.0\t0.2
                            B\t3.0\tnull
                            """);
        });
    }

    @Test
    public void testKeyedGroupByOuterNullAggregatedColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (category SYMBOL, amount DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE rates (min_amount DOUBLE, rate DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    ('A', 10.0, '2024-01-01T00:00:00.000000Z'),
                    ('A', null, '2024-01-01T01:00:00.000000Z'),
                    ('B', null, '2024-01-01T02:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO rates VALUES
                    (0.0, 0.1, '2024-01-01T00:00:00.000000Z'),
                    (5.0, 0.2, '2024-01-01T00:00:01.000000Z')
                    """);

            assertQuery("""
                    SELECT o.category, o.total, sub.rate
                    FROM (SELECT category, sum(amount) AS total FROM orders GROUP BY category) o
                    JOIN LATERAL (
                        SELECT rate FROM rates WHERE min_amount <= o.total
                    ) sub
                    ORDER BY o.category, sub.rate
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            category\ttotal\trate
                            A\t10.0\t0.1
                            A\t10.0\t0.2
                            """);
        });
    }

    @Test
    public void testKeyedStringAggGroupByOuter() throws Exception {
        Assume.assumeFalse(enableParallelGroupBy);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (category SYMBOL, item STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE rates (min_len INT, rate DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    ('A', 'apple', '2024-01-01T00:00:00.000000Z'),
                    ('A', 'avocado', '2024-01-01T01:00:00.000000Z'),
                    ('B', 'banana', '2024-01-01T02:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO rates VALUES
                    (5, 0.1, '2024-01-01T00:00:00.000000Z'),
                    (10, 0.2, '2024-01-01T00:00:01.000000Z')
                    """);

            assertQuery("""
                    SELECT o.category, o.items, sub.rate
                    FROM (
                        SELECT category, string_agg(item, ',') AS items
                        FROM orders
                        GROUP BY category
                    ) o
                    JOIN LATERAL (
                        SELECT rate FROM rates WHERE min_len <= length(o.items)
                    ) sub
                    ORDER BY o.category, sub.rate
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            category\titems\trate
                            A\tapple,avocado\t0.1
                            A\tapple,avocado\t0.2
                            B\tbanana\t0.1
                            """);
        });
    }

    @Test
    public void testLeftJoinConstantFalseOverSharedGroupBy() throws Exception {
        // A constant false ON conjunct of a LEFT join frees the joined GROUP BY, which the LATERAL read through a
        // shared cursor. The LATERAL now builds its own copy of the GROUP BY.
        assertMemoryLeak(() -> {
            createGateTables();
            final String groupBy = enableParallelGroupBy ? "GroupBy vectorized: true workers: 1" : "GroupBy vectorized: false";
            assertQuery("""
                    SELECT a.id, o.n, l.mv
                    FROM a
                    LEFT JOIN (SELECT k, count() n FROM t GROUP BY k) o ON a.k = o.k AND 1 = 0
                    CROSS JOIN LATERAL (SELECT max(u.v) mv FROM u WHERE u.k <= o.k) l
                    ORDER BY a.id
                    """)
                    .noLeakCheck()
                    .withPlan("""
                            Encode sort
                              keys: [id]
                                SelectedRecord
                                    Hash Left Outer Join Light
                                      condition: l.__qdb_outer_ref__0_k=o.k
                                        Hash Left Outer Join Light
                                          condition: o.k=a.k
                                          filter: false
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: a
                                            Hash
                                                Empty table
                                        Hash
                                            GroupBy vectorized: false
                                              keys: [__qdb_outer_ref__0_k]
                                              values: [max(v)]
                                                SelectedRecord
                                                    Filter filter: __qdb_outer_ref__0.__qdb_outer_ref__0_k>=u.k
                                                        Cross Join
                                                            PageFrame
                                                                Row forward scan
                                                                Frame forward scan on: u
                                                            GroupBy vectorized: false
                                                              keys: [__qdb_outer_ref__0_k]
                                                                SelectedRecord
                                                                    %s
                                                                      keys: [k]
                                                                      values: [count(*)]
                                                                        PageFrame
                                                                            Row forward scan
                                                                            Frame forward scan on: t
                            """.formatted(groupBy))
                    .returns("""
                            id\tn\tmv
                            1\tnull\tnull
                            2\tnull\tnull
                            3\tnull\tnull
                            4\tnull\tnull
                            """);
        });
    }

    @Test
    public void testMultipleLateralJoinsSharingOuter() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (category SYMBOL, amount DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE rates (min_amount DOUBLE, rate DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE discounts (min_amount DOUBLE, discount DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    ('A', 10.0, '2024-01-01T00:00:00.000000Z'),
                    ('A', 20.0, '2024-01-01T01:00:00.000000Z'),
                    ('B',  5.0, '2024-01-01T02:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO rates VALUES
                    (10.0, 0.1, '2024-01-01T00:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO discounts VALUES
                    (10.0, 0.05, '2024-01-01T00:00:00.000000Z')
                    """);

            assertQuery("""
                    SELECT o.category, o.total, r.rate, d.discount
                    FROM (SELECT category, sum(amount) AS total FROM orders GROUP BY category) o
                    JOIN LATERAL (
                        SELECT rate FROM rates WHERE min_amount <= o.total
                    ) r
                    JOIN LATERAL (
                        SELECT discount FROM discounts WHERE min_amount <= o.total
                    ) d
                    ORDER BY o.category
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            category\ttotal\trate\tdiscount
                            A\t30.0\t0.1\t0.05
                            """);
        });
    }

    @Test
    public void testNotKeyedGroupByOuter() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, amount DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE rates (min_amount DOUBLE, rate DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, 10.0, '2024-01-01T00:00:00.000000Z'),
                    (2, 20.0, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO rates VALUES
                    (10.0, 0.1, '2024-01-01T00:00:00.000000Z'),
                    (25.0, 0.2, '2024-01-01T00:00:01.000000Z')
                    """);

            assertQuery("""
                    SELECT o.total, sub.rate
                    FROM (SELECT sum(amount) AS total FROM orders) o
                    JOIN LATERAL (
                        SELECT rate FROM rates WHERE min_amount <= o.total
                    ) sub
                    ORDER BY sub.rate
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            total\trate
                            30.0\t0.1
                            30.0\t0.2
                            """);
        });
    }

    @Test
    public void testNotKeyedGroupByOuterLeftJoin() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (amount DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE rates (min_amount DOUBLE, rate DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1.0, '2024-01-01T00:00:00.000000Z')
                    """);

            assertQuery("""
                    SELECT o.total, sub.rate
                    FROM (SELECT sum(amount) AS total FROM orders) o
                    LEFT JOIN LATERAL (
                        SELECT rate FROM rates WHERE min_amount <= o.total
                    ) sub
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            total\trate
                            1.0\tnull
                            """);
        });
    }

    @Test
    public void testNotKeyedGroupByOuterLeftJoinCountArithmetic() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (amount DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE rates (min_amount DOUBLE, rate DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1.0, '2024-01-01T00:00:00.000000Z')
                    """);

            assertQuery("""
                    SELECT o.total, sub.arithmetic
                    FROM (SELECT sum(amount) AS total FROM orders) o
                    LEFT JOIN LATERAL (
                        SELECT count(*) + 2 AS arithmetic
                        FROM rates
                        WHERE min_amount = o.total
                    ) sub
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            total\tarithmetic
                            1.0\t2
                            """);

            execute("""
                    INSERT INTO rates VALUES
                    (1.0, 0.1, '2024-01-01T00:00:00.000000Z'),
                    (1.0, 0.2, '2024-01-01T00:00:01.000000Z')
                    """);

            assertQuery("""
                    SELECT o.total, sub.arithmetic
                    FROM (SELECT sum(amount) AS total FROM orders) o
                    LEFT JOIN LATERAL (
                        SELECT count(*) + 2 AS arithmetic
                        FROM rates
                        WHERE min_amount = o.total
                    ) sub
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            total\tarithmetic
                            1.0\t4
                            """);
        });
    }

    @Test
    public void testNotKeyedStringAggGroupByOuter() throws Exception {
        Assume.assumeFalse(enableParallelGroupBy);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (item STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE rates (min_len INT, rate DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    ('apple', '2024-01-01T00:00:00.000000Z'),
                    ('avocado', '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO rates VALUES
                    (5, 0.1, '2024-01-01T00:00:00.000000Z'),
                    (10, 0.2, '2024-01-01T00:00:01.000000Z')
                    """);

            assertQuery("""
                    SELECT o.all_items, sub.rate
                    FROM (SELECT string_agg(item, ',') AS all_items FROM orders) o
                    JOIN LATERAL (
                        SELECT rate FROM rates WHERE min_len <= length(o.all_items)
                    ) sub
                    ORDER BY sub.rate
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            all_items\trate
                            apple,avocado\t0.1
                            apple,avocado\t0.2
                            """);
        });
    }

    @Test
    public void testRightJoinConstantFalseBeforeLateral() throws Exception {
        // A constant false ON conjunct of a RIGHT join frees the joins before it, which hold the GROUP BY that a
        // LATERAL written after the RIGHT join read through a shared cursor. The LATERAL now builds its own copy.
        assertMemoryLeak(() -> {
            createGateTables();
            final String groupBy = enableParallelGroupBy ? "GroupBy vectorized: true workers: 1" : "GroupBy vectorized: false";
            assertQuery("""
                    SELECT a.id, d.id did, l.mv
                    FROM a
                    JOIN (SELECT k, count() n FROM t GROUP BY k) o ON a.k = o.k
                    RIGHT JOIN d ON d.k = a.k AND 1 = 0
                    CROSS JOIN LATERAL (SELECT max(u.v) mv FROM u WHERE u.k <= o.k) l
                    ORDER BY did
                    """)
                    .noLeakCheck()
                    .withPlan("""
                            Encode sort
                              keys: [did]
                                SelectedRecord
                                    Hash Left Outer Join Light
                                      condition: l.__qdb_outer_ref__0_k=o.k
                                        Hash Right Outer Join Light
                                          condition: d.k=a.k
                                          filter: false
                                            Empty table
                                            Hash
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: d
                                        Hash
                                            GroupBy vectorized: false
                                              keys: [__qdb_outer_ref__0_k]
                                              values: [max(v)]
                                                SelectedRecord
                                                    Filter filter: __qdb_outer_ref__0.__qdb_outer_ref__0_k>=u.k
                                                        Cross Join
                                                            PageFrame
                                                                Row forward scan
                                                                Frame forward scan on: u
                                                            GroupBy vectorized: false
                                                              keys: [__qdb_outer_ref__0_k]
                                                                SelectedRecord
                                                                    %s
                                                                      keys: [k]
                                                                      values: [count(*)]
                                                                        PageFrame
                                                                            Row forward scan
                                                                            Frame forward scan on: t
                            """.formatted(groupBy))
                    .returns("""
                            id\tdid\tmv
                            null\t1\tnull
                            null\t2\tnull
                            null\t3\tnull
                            """);
        });
    }

    @Test
    public void testSharedApproxPercentile() throws Exception {
        Assume.assumeFalse(enableParallelGroupBy);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE items (lval LONG, dval DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE rates (min_val DOUBLE, rate DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO items VALUES
                    (10, 10.0, '2024-01-01T00:00:00.000000Z'),
                    (20, 20.0, '2024-01-01T01:00:00.000000Z'),
                    (30, 30.0, '2024-01-01T02:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO rates VALUES
                    (10.0, 0.1, '2024-01-01T00:00:00.000000Z'),
                    (25.0, 0.2, '2024-01-01T00:00:01.000000Z')
                    """);
            // approx_percentile(lval, 0.5) ≈ 20, approx_percentile(dval, 0.5) ≈ 20.0
            // approx_percentile(lval, 0.5, 3) ≈ 20, approx_percentile(dval, 0.5, 3) ≈ 20.0
            assertQuery("""
                    SELECT o.p_long, o.p_double, o.p_long3, o.p_double3, sub.rate
                    FROM (
                        SELECT approx_percentile(lval, 0.5) AS p_long,
                               approx_percentile(dval, 0.5) AS p_double,
                               approx_percentile(lval, 0.5, 3) AS p_long3,
                               approx_percentile(dval, 0.5, 3) AS p_double3
                        FROM items
                    ) o
                    JOIN LATERAL (
                        SELECT rate FROM rates WHERE min_val <= o.p_double
                    ) sub
                    ORDER BY sub.rate
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            p_long	p_double	p_long3	p_double3	rate
                            20.0	20.5	20.0	20.0078125	0.1
                            """);
        });
    }

    @Test
    public void testSharedArrayAgg() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE items (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE rates (min_count INT, rate DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO items VALUES
                    (1.0, '2024-01-01T00:00:00.000000Z'),
                    (2.0, '2024-01-01T01:00:00.000000Z'),
                    (3.0, '2024-01-01T02:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO rates VALUES
                    (1, 0.1, '2024-01-01T00:00:00.000000Z'),
                    (3, 0.2, '2024-01-01T00:00:01.000000Z')
                    """);
            // The outer query reads o.arr both for projection and through
            // array_count(o.arr) in the lateral filter. The lateral subquery
            // runs as a SharedRecordCursorFactory backed by the inner Group By,
            // so the shared GroupByFunction flyweight must serve both reads
            // from the same MapValue without corrupting the rendered array.
            assertQuery("""
                    SELECT o.arr, sub.rate
                    FROM (SELECT array_agg(val) AS arr FROM items) o
                    JOIN LATERAL (
                        SELECT rate FROM rates WHERE min_count <= array_count(o.arr)
                    ) sub
                    ORDER BY sub.rate
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            arr\trate
                            [1.0,2.0,3.0]\t0.1
                            [1.0,2.0,3.0]\t0.2
                            """);
        });
    }

    @Test
    public void testSharedArrayAggKeyed() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE items (category SYMBOL, val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE rates (min_count INT, rate DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO items VALUES
                    ('A',  1.0, '2024-01-01T00:00:00.000000Z'),
                    ('A',  2.0, '2024-01-01T01:00:00.000000Z'),
                    ('A',  3.0, '2024-01-01T02:00:00.000000Z'),
                    ('B', 10.0, '2024-01-01T03:00:00.000000Z'),
                    ('B', 20.0, '2024-01-01T04:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO rates VALUES
                    (2, 0.1, '2024-01-01T00:00:00.000000Z'),
                    (3, 0.2, '2024-01-01T00:00:01.000000Z')
                    """);
            // Keyed variant: each group's array is rendered through the shared
            // cursor's GroupByFunction flyweight, then re-read in the lateral
            // subquery's filter. The per-instance render cache holds one slot,
            // so iterating across categories cache-misses on every group and
            // verifies that each render is computed correctly from a clean
            // build buffer.
            assertQuery("""
                    SELECT o.category, o.arr, sub.rate
                    FROM (
                        SELECT category, array_agg(val) AS arr
                        FROM items
                        GROUP BY category
                    ) o
                    JOIN LATERAL (
                        SELECT rate FROM rates WHERE min_count <= array_count(o.arr)
                    ) sub
                    ORDER BY o.category, sub.rate
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            category\tarr\trate
                            A\t[1.0,2.0,3.0]\t0.1
                            A\t[1.0,2.0,3.0]\t0.2
                            B\t[10.0,20.0]\t0.1
                            """);
        });
    }

    @Test
    public void testSharedArrayAggReusedFactory() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE items (category SYMBOL, val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE rates (min_count INT, rate DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO rates VALUES
                    (1, 0.1, '2024-01-01T00:00:00.000000Z'),
                    (5, 0.2, '2024-01-01T00:00:01.000000Z')
                    """);
            // Hold a single factory across multiple getCursor() calls and grow the
            // input table between calls. The shared GroupByFunction instance for the
            // keyed group-by survives across executions; AbstractArrayAggDoubleGroupByFunction
            // overrides cursorClosed() to reset cachedSrcPtr to 0 on every instance
            // (primary and shared). Without that reset, build-buffer address recycling
            // between executions could short-circuit getArray() to a stale cachedSrcPtr
            // match and return stale rendered bytes from a previous run. Different
            // counts per iteration ensure that any stale render would leak visibly.
            final String query = """
                    SELECT o.category, o.arr, sub.rate
                    FROM (
                        SELECT category, array_agg(val) AS arr
                        FROM items
                        GROUP BY category
                    ) o
                    JOIN LATERAL (
                        SELECT rate FROM rates WHERE min_count <= array_count(o.arr)
                    ) sub
                    ORDER BY o.category, sub.rate
                    """;
            final int ROW_CNT = 5;
            final ObjList<MutationStep> steps = new ObjList<>();
            for (int i = 1; i <= ROW_CNT; i++) {
                final StringBuilder arr = new StringBuilder("[");
                for (int j = 1; j <= i; j++) {
                    if (j > 1) arr.append(',');
                    arr.append(j).append(".0");
                }
                arr.append(']');
                final String expected;
                if (i == ROW_CNT) {
                    expected = "category\tarr\trate\nA\t" + arr + "\t0.1\nA\t" + arr + "\t0.2\n";
                } else {
                    expected = "category\tarr\trate\nA\t" + arr + "\t0.1\n";
                }
                steps.add(MutationStep.of(
                        "INSERT INTO items VALUES ('A', " + i + ".0, '2024-01-01T0" + i + ":00:00.000000Z')",
                        expected));
            }
            assertQuery(query)
                    .noLeakCheck()
                    .expectSize()
                    .mutateStepwise(steps);
        });
    }

    @Test
    public void testSharedCountDistinct() throws Exception {
        Assume.assumeFalse(enableParallelGroupBy);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE items (name STRING, vname VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE rates (min_cnt INT, rate DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO items VALUES
                    ('a', 'x', '2024-01-01T00:00:00.000000Z'),
                    ('b', 'x', '2024-01-01T01:00:00.000000Z'),
                    ('a', 'y', '2024-01-01T02:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO rates VALUES
                    (1, 0.1, '2024-01-01T00:00:00.000000Z'),
                    (2, 0.2, '2024-01-01T00:00:01.000000Z')
                    """);
            assertQuery("""
                    SELECT o.cd_str, o.cd_var, sub.rate
                    FROM (
                        SELECT count_distinct(name) AS cd_str, count_distinct(vname) AS cd_var
                        FROM items
                    ) o
                    JOIN LATERAL (
                        SELECT rate FROM rates WHERE min_cnt <= o.cd_str
                    ) sub
                    ORDER BY sub.rate
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            cd_str\tcd_var\trate
                            2\t2\t0.1
                            2\t2\t0.2
                            """);
        });
    }

    @Test
    public void testSharedCursorCrossedColumnOrder() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (category SYMBOL, amount DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE rates (min_amount DOUBLE, rate DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    ('A', 10.0, '2024-01-01T00:00:00.000000Z'),
                    ('A', 20.0, '2024-01-01T01:00:00.000000Z'),
                    ('B',  5.0, '2024-01-01T02:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO rates VALUES
                    (10.0, 0.1, '2024-01-01T00:00:00.000000Z'),
                    (25.0, 0.2, '2024-01-01T00:00:01.000000Z')
                    """);

            assertQuery("""
                    SELECT o.total, o.category, sub.rate
                    FROM (select total,category  from (SELECT category, sum(amount) AS total FROM orders GROUP BY category)) o
                    JOIN LATERAL (
                        SELECT rate FROM rates WHERE min_amount <= o.total
                    ) sub
                    ORDER BY o.category, sub.rate
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            total\tcategory\trate
                            30.0\tA\t0.1
                            30.0\tA\t0.2
                            """);

            assertQuery("""
                    SELECT o.total, o.category, sub.rate
                    FROM (select total, category, category as category1  from (SELECT category, sum(amount) AS total FROM orders GROUP BY category)) o
                    JOIN LATERAL (
                        SELECT rate FROM rates WHERE min_amount <= o.total
                    ) sub
                    ORDER BY o.category, sub.rate
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            total\tcategory\trate
                            30.0\tA\t0.1
                            30.0\tA\t0.2
                            """);
        });
    }

    @Test
    public void testSharedCursorLongTopK() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (category SYMBOL, id LONG, amount DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE rates (min_id LONG, rate DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    ('A', 1, 10.0, '2024-01-01T00:00:00.000000Z'),
                    ('A', 2, 20.0, '2024-01-01T01:00:00.000000Z'),
                    ('B', 3,  5.0, '2024-01-01T02:00:00.000000Z'),
                    ('B', 4,  3.0, '2024-01-01T03:00:00.000000Z'),
                    ('C', 5, 50.0, '2024-01-01T04:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO rates VALUES
                    (1, 0.1, '2024-01-01T00:00:00.000000Z'),
                    (3, 0.2, '2024-01-01T00:00:01.000000Z')
                    """);

            assertQuery("""
                    SELECT o.category, o.max_id, sub.rate
                    FROM (
                        SELECT category, max(id) AS max_id
                        FROM orders
                        GROUP BY category
                    ) o
                    JOIN LATERAL (
                        SELECT rate FROM rates WHERE min_id <= o.max_id
                    ) sub
                    ORDER BY o.max_id DESC
                    LIMIT 2
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            category\tmax_id\trate
                            C\t5\t0.1
                            C\t5\t0.2
                            """);
        });
    }

    @Test
    public void testSharedCursorSymbolTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (category SYMBOL, status SYMBOL, amount DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE rates (min_amount DOUBLE, rate DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    ('A', 'open', 10.0, '2024-01-01T00:00:00.000000Z'),
                    ('A', 'open', 20.0, '2024-01-01T01:00:00.000000Z'),
                    ('B', 'closed', 5.0, '2024-01-01T02:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO rates VALUES
                    (10.0, 0.1, '2024-01-01T00:00:00.000000Z'),
                    (25.0, 0.2, '2024-01-01T00:00:01.000000Z')
                    """);

            assertQuery("""
                    SELECT o.category, o.status, o.total, sub.rate
                    FROM (
                        SELECT category, last(status) AS status, sum(amount) AS total
                        FROM orders
                        GROUP BY category
                    ) o
                    JOIN LATERAL (
                        SELECT rate FROM rates WHERE min_amount <= o.total
                    ) sub
                    ORDER BY o.category, sub.rate
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            category\tstatus\ttotal\trate
                            A\topen\t30.0\t0.1
                            A\topen\t30.0\t0.2
                            """);
        });
    }

    @Test
    public void testSharedCursorUnordered8Map() throws Exception {
        Assume.assumeFalse(enableParallelGroupBy);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id LONG, item STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE rates (min_len INT, rate DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, 'apple', '2024-01-01T00:00:00.000000Z'),
                    (1, 'avocado', '2024-01-01T01:00:00.000000Z'),
                    (2, 'banana', '2024-01-01T02:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO rates VALUES
                    (5, 0.1, '2024-01-01T00:00:00.000000Z'),
                    (10, 0.2, '2024-01-01T00:00:01.000000Z')
                    """);

            String query = """
                    SELECT o.id, o.items, sub.rate
                    FROM (
                        SELECT id, string_agg(item, ',') AS items
                        FROM orders
                        GROUP BY id
                    ) o
                    JOIN LATERAL (
                        SELECT rate FROM rates WHERE min_len <= length(o.items)
                    ) sub
                    ORDER BY o.id, sub.rate
                    """;

            assertQuery(query)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\titems\trate
                            1\tapple,avocado\t0.1
                            1\tapple,avocado\t0.2
                            2\tbanana\t0.1
                            """);

            execute("INSERT INTO orders VALUES (0, 'cherry', '2024-01-01T03:00:00.000000Z')");
            assertQuery(query)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\titems\trate
                            0\tcherry\t0.1
                            1\tapple,avocado\t0.1
                            1\tapple,avocado\t0.2
                            2\tbanana\t0.1
                            """);
        });
    }

    @Test
    public void testSharedCursorUnorderedVarcharMap() throws Exception {
        Assume.assumeFalse(enableParallelGroupBy);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (name VARCHAR, item STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE rates (min_len INT, rate DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    ('alice', 'apple', '2024-01-01T00:00:00.000000Z'),
                    ('alice', 'avocado', '2024-01-01T01:00:00.000000Z'),
                    ('bob', 'banana', '2024-01-01T02:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO rates VALUES
                    (5, 0.1, '2024-01-01T00:00:00.000000Z'),
                    (10, 0.2, '2024-01-01T00:00:01.000000Z')
                    """);

            assertQuery("""
                    SELECT o.name, o.items, sub.rate
                    FROM (
                        SELECT name, string_agg(item, ',') AS items
                        FROM orders
                        GROUP BY name
                    ) o
                    JOIN LATERAL (
                        SELECT rate FROM rates WHERE min_len <= length(o.items)
                    ) sub
                    ORDER BY o.name, sub.rate
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            name\titems\trate
                            alice\tapple,avocado\t0.1
                            alice\tapple,avocado\t0.2
                            bob\tbanana\t0.1
                            """);
        });
    }

    @Test
    public void testSharedStringAggVarchar() throws Exception {
        Assume.assumeFalse(enableParallelGroupBy);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE items (name VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE rates (min_len INT, rate DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO items VALUES
                    ('abc', '2024-01-01T00:00:00.000000Z'),
                    ('defgh', '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO rates VALUES
                    (5, 0.1, '2024-01-01T00:00:00.000000Z')
                    """);
            assertQuery("""
                    SELECT o.all_names, sub.rate
                    FROM (SELECT string_agg(name, ',') AS all_names FROM items) o
                    JOIN LATERAL (
                        SELECT rate FROM rates WHERE min_len <= length(o.all_names)
                    ) sub
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            all_names\trate
                            abc,defgh\t0.1
                            """);
        });
    }

    @Test
    public void testSharedStringDistinctAgg() throws Exception {
        Assume.assumeFalse(enableParallelGroupBy);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE items (name STRING, vname VARCHAR, sname SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE rates (min_len INT, rate DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO items VALUES
                    ('a', 'x', 'p', '2024-01-01T00:00:00.000000Z'),
                    ('b', 'y', 'q', '2024-01-01T01:00:00.000000Z'),
                    ('a', 'x', 'p', '2024-01-01T02:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO rates VALUES
                    (1, 0.1, '2024-01-01T00:00:00.000000Z'),
                    (3, 0.2, '2024-01-01T00:00:01.000000Z')
                    """);
            assertQuery("""
                    SELECT o.sd_str, o.sd_var, o.sd_sym, sub.rate
                    FROM (
                        SELECT string_distinct_agg(name, ',') AS sd_str,
                               string_distinct_agg(vname, ',') AS sd_var,
                               string_distinct_agg(sname, ',') AS sd_sym
                        FROM items
                    ) o
                    JOIN LATERAL (
                        SELECT rate FROM rates WHERE min_len <= length(o.sd_str)
                    ) sub
                    ORDER BY sub.rate
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            sd_str\tsd_var\tsd_sym\trate
                            a,b\tx,y\tp,q\t0.1
                            a,b\tx,y\tp,q\t0.2
                            """);
        });
    }

    @Test
    public void testVectorizedKeyedGroupByOuter() throws Exception {
        Assume.assumeFalse(enableParallelGroupBy);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (group_id INT, qty LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE rates (min_qty LONG, rate DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, 100, '2024-01-01T00:00:00.000000Z'),
                    (1, 200, '2024-01-01T01:00:00.000000Z'),
                    (2,  50, '2024-01-01T02:00:00.000000Z'),
                    (2,  30, '2024-01-01T03:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO rates VALUES
                    (100, 0.1, '2024-01-01T00:00:00.000000Z'),
                    (250, 0.2, '2024-01-01T00:00:01.000000Z')
                    """);

            assertQuery("""
                    SELECT o.group_id, o.total, sub.rate
                    FROM (SELECT group_id, sum(qty) AS total FROM orders GROUP BY group_id) o
                    JOIN LATERAL (
                        SELECT rate FROM rates WHERE min_qty <= o.total
                    ) sub
                    ORDER BY o.group_id, sub.rate
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            group_id\ttotal\trate
                            1\t300\t0.1
                            1\t300\t0.2
                            """);
        });
    }

    @Test
    public void testVectorizedNotKeyedGroupByOuter() throws Exception {
        Assume.assumeFalse(enableParallelGroupBy);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (qty LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE rates (min_qty LONG, rate DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (100, '2024-01-01T00:00:00.000000Z'),
                    (200, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO rates VALUES
                    (100, 0.1, '2024-01-01T00:00:00.000000Z'),
                    (250, 0.2, '2024-01-01T00:00:01.000000Z')
                    """);

            assertQuery("""
                    SELECT o.total, sub.rate
                    FROM (SELECT sum(qty) AS total FROM orders) o
                    JOIN LATERAL (
                        SELECT rate FROM rates WHERE min_qty <= o.total
                    ) sub
                    ORDER BY sub.rate
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            total\trate
                            300\t0.1
                            300\t0.2
                            """);
        });
    }

    // a INNER joins the GROUP BY of t, the LATERAL reads u for each key of t, and d is the side that a later
    // RIGHT or FULL join keeps
    private static void createGateTables() throws Exception {
        execute("CREATE TABLE a (id INT, k INT)");
        execute("INSERT INTO a VALUES (1, 1), (2, 2), (3, 3), (4, 4)");
        execute("CREATE TABLE t (k INT, v INT)");
        execute("INSERT INTO t VALUES (1, 10), (1, 11), (2, 20), (2, 21), (3, 30)");
        execute("CREATE TABLE u (k INT, v INT)");
        execute("INSERT INTO u VALUES (1, 100), (2, 200), (3, 300)");
        execute("CREATE TABLE d (id INT, k INT)");
        execute("INSERT INTO d VALUES (1, 1), (2, 2), (3, 5)");
    }
}
