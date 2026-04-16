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
import io.questdb.test.AbstractCairoTest;
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
            execute("CREATE TABLE orders (category SYMBOL, region SYMBOL, amount DOUBLE, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE rates (min_amount DOUBLE, rate DOUBLE, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
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

            assertQueryAndPlan(
                    """
                            category\tregion\ttotal\trate
                            A\tUS\t30.0\t0.1
                            A\tUS\t30.0\t0.2
                            """,
                    enableParallelGroupBy ? """
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
                            """,
                    """
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
                            """,
                    null, true, true
            );
        });
    }

    @Test
    public void testAsyncKeyedGroupByOuterSharded() throws Exception {
        Assume.assumeTrue(enableParallelGroupBy);
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, "1");
            execute("CREATE TABLE orders (id INT, region SYMBOL, amount DOUBLE, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("CREATE TABLE rates (min_amount DOUBLE, rate DOUBLE, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");

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

            assertQueryNoLeakCheck(
                    """
                            id	region	total	rate
                            48	R3	1920.0	0.1
                            49	R4	1960.0	0.1
                            """,
                    """
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
                            """,
                    null, true, true
            );
        });
    }

    @Test
    public void testAsyncNotKeyedGroupByOuter() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (x DOUBLE, y DOUBLE, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE rates (min_val DOUBLE, rate DOUBLE, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
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

            assertQueryNoLeakCheck(
                    """
                            correlation\trate
                            1.0\t0.1
                            """,
                    """
                            SELECT o.correlation, sub.rate
                            FROM (SELECT corr(x, y) AS correlation FROM orders) o
                            JOIN LATERAL (
                                SELECT rate FROM rates WHERE min_val <= o.correlation
                            ) sub
                            """,
                    null, false, true
            );
        });
    }

    @Test
    public void testExpressionKeyGroupByOuter() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, amount DOUBLE, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE rates (min_amount DOUBLE, rate DOUBLE, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
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

            assertQueryNoLeakCheck(
                    """
                            bucket\ttotal\trate
                            0\t35.0\t0.1
                            0\t35.0\t0.2
                            1\t15.0\t0.1
                            """,
                    """
                            SELECT o.bucket, o.total, sub.rate
                            FROM (SELECT id % 2 AS bucket, sum(amount) AS total FROM orders GROUP BY id % 2) o
                            JOIN LATERAL (
                                SELECT rate FROM rates WHERE min_amount <= o.total
                            ) sub
                            ORDER BY o.bucket, sub.rate
                            """,
                    null, true, true
            );
        });
    }

    @Test
    public void testKeyedGroupByOuter() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (category SYMBOL, amount DOUBLE, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE rates (min_amount DOUBLE, rate DOUBLE, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
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

            assertQueryNoLeakCheck(
                    """
                            category\ttotal\trate
                            A\t30.0\t0.1
                            A\t30.0\t0.2
                            """,
                    """
                            SELECT o.category, o.total, sub.rate
                            FROM (SELECT category, sum(amount) AS total FROM orders GROUP BY category) o
                            JOIN LATERAL (
                                SELECT rate FROM rates WHERE min_amount <= o.total
                            ) sub
                            ORDER BY o.category, sub.rate
                            """,
                    null, true, true
            );
        });
    }

    @Test
    public void testKeyedGroupByOuterAggregateBeforeKey() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (category SYMBOL, amount DOUBLE, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE rates (min_amount DOUBLE, rate DOUBLE, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
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

            assertQueryNoLeakCheck(
                    """
                            total\tcategory\trate
                            30.0\tA\t0.1
                            30.0\tA\t0.2
                            """,
                    """
                            SELECT o.total, o.category, sub.rate
                            FROM (SELECT sum(amount) AS total, category FROM orders GROUP BY category) o
                            JOIN LATERAL (
                                SELECT rate FROM rates WHERE min_amount <= o.total
                            ) sub
                            ORDER BY o.category, sub.rate
                            """,
                    null, true, true
            );
        });
    }

    @Test
    public void testKeyedGroupByOuterEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (category SYMBOL, amount DOUBLE, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE rates (min_amount DOUBLE, rate DOUBLE, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO rates VALUES
                    (10.0, 0.1, '2024-01-01T00:00:00.000000Z')
                    """);

            assertQueryNoLeakCheck(
                    """
                            category\ttotal\trate
                            """,
                    """
                            SELECT o.category, o.total, sub.rate
                            FROM (SELECT category, sum(amount) AS total FROM orders GROUP BY category) o
                            JOIN LATERAL (
                                SELECT rate FROM rates WHERE min_amount <= o.total
                            ) sub
                            ORDER BY o.category, sub.rate
                            """,
                    null, true, true
            );
        });
    }

    @Test
    public void testKeyedGroupByOuterLeftJoin() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (category SYMBOL, amount DOUBLE, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE rates (min_amount DOUBLE, rate DOUBLE, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
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

            assertQueryNoLeakCheck(
                    """
                            category\ttotal\trate
                            A\t30.0\t0.1
                            A\t30.0\t0.2
                            B\t3.0\tnull
                            """,
                    """
                            SELECT o.category, o.total, sub.rate
                            FROM (SELECT category, sum(amount) AS total FROM orders GROUP BY category) o
                            LEFT JOIN LATERAL (
                                SELECT rate FROM rates WHERE min_amount <= o.total
                            ) sub
                            ORDER BY o.category, sub.rate
                            """,
                    null, true, false
            );
        });
    }

    @Test
    public void testKeyedGroupByOuterNullAggregatedColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (category SYMBOL, amount DOUBLE, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE rates (min_amount DOUBLE, rate DOUBLE, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
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

            assertQueryNoLeakCheck(
                    """
                            category\ttotal\trate
                            A\t10.0\t0.1
                            A\t10.0\t0.2
                            """,
                    """
                            SELECT o.category, o.total, sub.rate
                            FROM (SELECT category, sum(amount) AS total FROM orders GROUP BY category) o
                            JOIN LATERAL (
                                SELECT rate FROM rates WHERE min_amount <= o.total
                            ) sub
                            ORDER BY o.category, sub.rate
                            """,
                    null, true, true
            );
        });
    }

    @Test
    public void testKeyedStringAggGroupByOuter() throws Exception {
        Assume.assumeFalse(enableParallelGroupBy);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (category SYMBOL, item STRING, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE rates (min_len INT, rate DOUBLE, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
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

            assertQueryNoLeakCheck(
                    """
                            category\titems\trate
                            A\tapple,avocado\t0.1
                            A\tapple,avocado\t0.2
                            B\tbanana\t0.1
                            """,
                    """
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
                            """,
                    null, true, true
            );
        });
    }

    @Test
    public void testMultipleLateralJoinsSharingOuter() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (category SYMBOL, amount DOUBLE, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE rates (min_amount DOUBLE, rate DOUBLE, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE discounts (min_amount DOUBLE, discount DOUBLE, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
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

            assertQueryNoLeakCheck(
                    """
                            category\ttotal\trate\tdiscount
                            A\t30.0\t0.1\t0.05
                            """,
                    """
                            SELECT o.category, o.total, r.rate, d.discount
                            FROM (SELECT category, sum(amount) AS total FROM orders GROUP BY category) o
                            JOIN LATERAL (
                                SELECT rate FROM rates WHERE min_amount <= o.total
                            ) r
                            JOIN LATERAL (
                                SELECT discount FROM discounts WHERE min_amount <= o.total
                            ) d
                            ORDER BY o.category
                            """,
                    null, true, true
            );
        });
    }

    @Test
    public void testNotKeyedGroupByOuter() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, amount DOUBLE, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE rates (min_amount DOUBLE, rate DOUBLE, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
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

            assertQueryNoLeakCheck(
                    """
                            total\trate
                            30.0\t0.1
                            30.0\t0.2
                            """,
                    """
                            SELECT o.total, sub.rate
                            FROM (SELECT sum(amount) AS total FROM orders) o
                            JOIN LATERAL (
                                SELECT rate FROM rates WHERE min_amount <= o.total
                            ) sub
                            ORDER BY sub.rate
                            """,
                    null, true, true
            );
        });
    }

    @Test
    public void testNotKeyedGroupByOuterLeftJoin() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (amount DOUBLE, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE rates (min_amount DOUBLE, rate DOUBLE, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1.0, '2024-01-01T00:00:00.000000Z')
                    """);

            assertQueryNoLeakCheck(
                    """
                            total\trate
                            1.0\tnull
                            """,
                    """
                            SELECT o.total, sub.rate
                            FROM (SELECT sum(amount) AS total FROM orders) o
                            LEFT JOIN LATERAL (
                                SELECT rate FROM rates WHERE min_amount <= o.total
                            ) sub
                            """,
                    null, false, false
            );
        });
    }

    @Test
    public void testNotKeyedStringAggGroupByOuter() throws Exception {
        Assume.assumeFalse(enableParallelGroupBy);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (item STRING, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE rates (min_len INT, rate DOUBLE, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
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

            assertQueryNoLeakCheck(
                    """
                            all_items\trate
                            apple,avocado\t0.1
                            apple,avocado\t0.2
                            """,
                    """
                            SELECT o.all_items, sub.rate
                            FROM (SELECT string_agg(item, ',') AS all_items FROM orders) o
                            JOIN LATERAL (
                                SELECT rate FROM rates WHERE min_len <= length(o.all_items)
                            ) sub
                            ORDER BY sub.rate
                            """,
                    null, true, true
            );
        });
    }

    @Test
    public void testSharedApproxPercentile() throws Exception {
        Assume.assumeFalse(enableParallelGroupBy);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE items (lval LONG, dval DOUBLE, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE rates (min_val DOUBLE, rate DOUBLE, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
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
            assertQueryNoLeakCheck(
                    """
                            p_long	p_double	p_long3	p_double3	rate
                            20.0	20.5	20.0	20.0078125	0.1
                            """,
                    """
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
                            """,
                    null, true, true
            );
        });
    }

    @Test
    public void testSharedCountDistinct() throws Exception {
        Assume.assumeFalse(enableParallelGroupBy);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE items (name STRING, vname VARCHAR, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE rates (min_cnt INT, rate DOUBLE, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
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
            assertQueryNoLeakCheck(
                    """
                            cd_str\tcd_var\trate
                            2\t2\t0.1
                            2\t2\t0.2
                            """,
                    """
                            SELECT o.cd_str, o.cd_var, sub.rate
                            FROM (
                                SELECT count_distinct(name) AS cd_str, count_distinct(vname) AS cd_var
                                FROM items
                            ) o
                            JOIN LATERAL (
                                SELECT rate FROM rates WHERE min_cnt <= o.cd_str
                            ) sub
                            ORDER BY sub.rate
                            """,
                    null, true, true
            );
        });
    }

    @Test
    public void testSharedCursorCrossedColumnOrder() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (category SYMBOL, amount DOUBLE, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE rates (min_amount DOUBLE, rate DOUBLE, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
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

            assertQueryNoLeakCheck(
                    """
                            total\tcategory\trate
                            30.0\tA\t0.1
                            30.0\tA\t0.2
                            """,
                    """
                            SELECT o.total, o.category, sub.rate
                            FROM (select total,category  from (SELECT category, sum(amount) AS total FROM orders GROUP BY category)) o
                            JOIN LATERAL (
                                SELECT rate FROM rates WHERE min_amount <= o.total
                            ) sub
                            ORDER BY o.category, sub.rate
                            """,
                    null, true, true
            );

            assertQueryNoLeakCheck(
                    """
                            total\tcategory\trate
                            30.0\tA\t0.1
                            30.0\tA\t0.2
                            """,
                    """
                            SELECT o.total, o.category, sub.rate
                            FROM (select total, category, category as category1  from (SELECT category, sum(amount) AS total FROM orders GROUP BY category)) o
                            JOIN LATERAL (
                                SELECT rate FROM rates WHERE min_amount <= o.total
                            ) sub
                            ORDER BY o.category, sub.rate
                            """,
                    null, true, true
            );
        });
    }

    @Test
    public void testSharedCursorLongTopK() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (category SYMBOL, id LONG, amount DOUBLE, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE rates (min_id LONG, rate DOUBLE, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
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

            assertQueryNoLeakCheck(
                    """
                            category\tmax_id\trate
                            C\t5\t0.1
                            C\t5\t0.2
                            """,
                    """
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
                            """,
                    null, true, true
            );
        });
    }

    @Test
    public void testSharedCursorSymbolTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (category SYMBOL, status SYMBOL, amount DOUBLE, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE rates (min_amount DOUBLE, rate DOUBLE, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
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

            assertQueryNoLeakCheck(
                    """
                            category\tstatus\ttotal\trate
                            A\topen\t30.0\t0.1
                            A\topen\t30.0\t0.2
                            """,
                    """
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
                            """,
                    null, true, true
            );
        });
    }

    @Test
    public void testSharedCursorUnordered8Map() throws Exception {
        Assume.assumeFalse(enableParallelGroupBy);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id LONG, item STRING, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE rates (min_len INT, rate DOUBLE, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
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

            assertQueryNoLeakCheck(
                    """
                            id\titems\trate
                            1\tapple,avocado\t0.1
                            1\tapple,avocado\t0.2
                            2\tbanana\t0.1
                            """,
                    query, null, true, true
            );

            execute("INSERT INTO orders VALUES (0, 'cherry', '2024-01-01T03:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            id\titems\trate
                            0\tcherry\t0.1
                            1\tapple,avocado\t0.1
                            1\tapple,avocado\t0.2
                            2\tbanana\t0.1
                            """,
                    query, null, true, true
            );
        });
    }

    @Test
    public void testSharedCursorUnorderedVarcharMap() throws Exception {
        Assume.assumeFalse(enableParallelGroupBy);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (name VARCHAR, item STRING, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE rates (min_len INT, rate DOUBLE, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
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

            assertQueryNoLeakCheck(
                    """
                            name\titems\trate
                            alice\tapple,avocado\t0.1
                            alice\tapple,avocado\t0.2
                            bob\tbanana\t0.1
                            """,
                    """
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
                            """,
                    null, true, true
            );
        });
    }

    @Test
    public void testSharedStringAggVarchar() throws Exception {
        Assume.assumeFalse(enableParallelGroupBy);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE items (name VARCHAR, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE rates (min_len INT, rate DOUBLE, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO items VALUES
                    ('abc', '2024-01-01T00:00:00.000000Z'),
                    ('defgh', '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO rates VALUES
                    (5, 0.1, '2024-01-01T00:00:00.000000Z')
                    """);
            assertQueryNoLeakCheck(
                    """
                            all_names\trate
                            abc,defgh\t0.1
                            """,
                    """
                            SELECT o.all_names, sub.rate
                            FROM (SELECT string_agg(name, ',') AS all_names FROM items) o
                            JOIN LATERAL (
                                SELECT rate FROM rates WHERE min_len <= length(o.all_names)
                            ) sub
                            """,
                    null, false, true
            );
        });
    }

    @Test
    public void testSharedStringDistinctAgg() throws Exception {
        Assume.assumeFalse(enableParallelGroupBy);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE items (name STRING, vname VARCHAR, sname SYMBOL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE rates (min_len INT, rate DOUBLE, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
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
            assertQueryNoLeakCheck(
                    """
                            sd_str\tsd_var\tsd_sym\trate
                            a,b\tx,y\tp,q\t0.1
                            a,b\tx,y\tp,q\t0.2
                            """,
                    """
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
                            """,
                    null, true, true
            );
        });
    }

    @Test
    public void testVectorizedKeyedGroupByOuter() throws Exception {
        Assume.assumeFalse(enableParallelGroupBy);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (group_id INT, qty LONG, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE rates (min_qty LONG, rate DOUBLE, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
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

            assertQueryNoLeakCheck(
                    """
                            group_id\ttotal\trate
                            1\t300\t0.1
                            1\t300\t0.2
                            """,
                    """
                            SELECT o.group_id, o.total, sub.rate
                            FROM (SELECT group_id, sum(qty) AS total FROM orders GROUP BY group_id) o
                            JOIN LATERAL (
                                SELECT rate FROM rates WHERE min_qty <= o.total
                            ) sub
                            ORDER BY o.group_id, sub.rate
                            """,
                    null, true, true
            );
        });
    }

    @Test
    public void testVectorizedNotKeyedGroupByOuter() throws Exception {
        Assume.assumeFalse(enableParallelGroupBy);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (qty LONG, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE rates (min_qty LONG, rate DOUBLE, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
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

            assertQueryNoLeakCheck(
                    """
                            total\trate
                            300\t0.1
                            300\t0.2
                            """,
                    """
                            SELECT o.total, sub.rate
                            FROM (SELECT sum(qty) AS total FROM orders) o
                            JOIN LATERAL (
                                SELECT rate FROM rates WHERE min_qty <= o.total
                            ) sub
                            ORDER BY sub.rate
                            """,
                    null, true, true
            );
        });
    }
}
