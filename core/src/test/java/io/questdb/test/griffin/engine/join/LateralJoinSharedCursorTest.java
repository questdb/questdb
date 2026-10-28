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
import io.questdb.griffin.SqlException;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.MutationStep;
import io.questdb.test.tools.TestUtils;
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
    private static final String DISCARDED_BY_EMPTIED_MASTER = """
            SELECT o.id, l.v FROM o JOIN LATERAL (
                SELECT c.id v FROM (SELECT a.id, a.x FROM a WHERE a.x = o.x AND a.id > o.id) aa RIGHT JOIN c ON c.id = aa.id AND false
                UNION ALL
                SELECT b.id v FROM b WHERE b.x = o.x AND b.id > o.id
            ) l ORDER BY 1, 2
            """;
    private static final String DISCARDED_BY_EMPTIED_SLAVE = """
            SELECT o.id, l.v FROM o JOIN LATERAL (
                SELECT c.id v FROM c LEFT JOIN (SELECT a.id, a.x FROM a WHERE a.x = o.x AND a.id > o.id) aa ON c.id = aa.id AND false
                UNION ALL
                SELECT b.id v FROM b WHERE b.x = o.x AND b.id > o.id
            ) l ORDER BY 1, 2
            """;
    private static final String DISCARDED_BY_EMPTY_WINDOW_JOIN = """
            SELECT o.id, l.v FROM o JOIN LATERAL (
                SELECT tt.id v, sum(p.v) s FROM (SELECT * FROM t WHERE t.x = o.x AND t.id > o.id) tt
                WINDOW JOIN p ON (tt.x = p.x) RANGE BETWEEN 1 minute PRECEDING AND 1 minute FOLLOWING WHERE false
                UNION ALL
                SELECT b.id v, 0.0 s FROM b WHERE b.x = o.x AND b.id > o.id
            ) l ORDER BY 1, 2
            """;
    private static final String DISCARDED_BY_FILTER = """
            SELECT o.id, l.v FROM o JOIN LATERAL (
                SELECT v FROM (SELECT a.id v FROM a WHERE a.x = o.x AND a.id > o.id) WHERE false
                UNION ALL
                SELECT b.id v FROM b WHERE b.x = o.x AND b.id > o.id
            ) l ORDER BY 1, 2
            """;
    private static final String DISCARDED_BY_JOIN_FILTER = """
            SELECT o.id, l.v FROM o JOIN LATERAL (
                SELECT a.id v FROM a JOIN c ON a.id = c.id WHERE a.x = o.x AND a.id > o.id AND false
                UNION ALL
                SELECT b.id v FROM b WHERE b.x = o.x AND b.id > o.id
            ) l ORDER BY 1, 2
            """;
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
                                                Filter filter: __qdb_outer_ref__0.__qdb_outer_ref__0_total>=min_amount
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
                                                Filter filter: __qdb_outer_ref__0.__qdb_outer_ref__0_total>=min_amount
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
    public void testSharedMultiApproxPercentile() throws Exception {
        // The lateral subquery reads o.p through a shared copy of the GROUP BY functions. The
        // histograms live on the heap of the computing instance, so the shared copy must adopt them
        // in initSharedFrom(); otherwise o.p[1] reads as null and the join drops every row.
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
            // p_double and p_long use the default precision, p_double4 and p_long4 the packed histograms
            assertQuery("""
                    SELECT o.p_double, o.p_double4, o.p_long, o.p_long4, sub.rate
                    FROM (
                        SELECT approx_percentile(dval, ARRAY[0.5, 0.9]) AS p_double,
                               approx_percentile(dval, ARRAY[0.5, 0.9], 4) AS p_double4,
                               approx_percentile(lval, ARRAY[0.5, 0.9]) AS p_long,
                               approx_percentile(lval, ARRAY[0.5, 0.9], 4) AS p_long4
                        FROM items
                    ) o
                    JOIN LATERAL (
                        SELECT rate FROM rates
                        WHERE min_val <= o.p_double[1] AND min_val <= o.p_double4[1]
                          AND min_val <= o.p_long[1] AND min_val <= o.p_long4[1]
                    ) sub
                    ORDER BY sub.rate
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            p_double\tp_double4\tp_long\tp_long4\trate
                            [20.5,30.5]\t[20.00048828125,30.00048828125]\t[20.0,30.0]\t[20.0,30.0]\t0.1
                            """);
        });
    }

    @Test
    public void testSharedPercentile() throws Exception {
        // percentile_disc() and percentile_cont() keep their values in the GROUP BY allocator, which
        // the shared copy reads through the map value it shares with the computing instance
        assertMemoryLeak(() -> {
            execute("CREATE TABLE items (lval LONG, dval DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE rates (min_val DOUBLE, rate DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO items VALUES
                    (10, 10.0, '2024-01-01T00:00:00.000000Z'),
                    (20, 20.0, '2024-01-01T01:00:00.000000Z'),
                    (30, 30.0, '2024-01-01T02:00:00.000000Z'),
                    (40, 40.0, '2024-01-01T03:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO rates VALUES
                    (10.0, 0.1, '2024-01-01T00:00:00.000000Z'),
                    (25.0, 0.2, '2024-01-01T00:00:01.000000Z')
                    """);
            assertQuery("""
                    SELECT o.pd, o.pc, o.pl, o.pdm, o.pcm, sub.rate
                    FROM (
                        SELECT percentile_disc(dval, 0.5) AS pd,
                               percentile_cont(dval, 0.5) AS pc,
                               percentile_disc(lval, 0.5) AS pl,
                               percentile_disc(dval, ARRAY[0.25, 0.75]) AS pdm,
                               percentile_cont(dval, ARRAY[0.25, 0.75]) AS pcm
                        FROM items
                    ) o
                    JOIN LATERAL (
                        SELECT rate FROM rates
                        WHERE min_val <= o.pd AND min_val < o.pc AND min_val <= o.pl
                          AND min_val <= o.pdm[1] AND min_val < o.pcm[1]
                    ) sub
                    ORDER BY sub.rate
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            pd\tpc\tpl\tpdm\tpcm\trate
                            20.0\t25.0\t20\t[10.0,30.0]\t[17.5,32.5]\t0.1
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
    public void testSharedCursorSymbolKeyedDomain() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (sym SYMBOL, id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE items (sym SYMBOL, id INT)");
            execute("""
                    INSERT INTO trades VALUES
                    ('a', 1, '2024-01-01T00:00:00.000000Z'),
                    ('b', 2, '2024-01-01T01:00:00.000000Z'),
                    ('a', 3, '2024-01-02T00:00:00.000000Z')
                    """);
            execute("INSERT INTO items VALUES ('a', 1), ('a', 2), ('a', 3), ('b', 1), ('b', 2)");
            assertQuery("""
                    SELECT g.s, g.c, l.id
                    FROM (SELECT sym s, count() c FROM trades GROUP BY sym) g
                    CROSS JOIN LATERAL (
                        SELECT i.id FROM items i WHERE i.sym = g.s AND i.id > g.c ORDER BY i.id LIMIT 1
                    ) l
                    ORDER BY 1
                    """)
                    .noLeakCheck()
                    .withPlanContaining("(Shared)")
                    .returns("""
                            s\tc\tid
                            a\t2\t3
                            b\t1\t2
                            """);
            assertQuery("""
                    SELECT g.sym, g.c, l.id
                    FROM (SELECT sym, count() c FROM trades GROUP BY sym) g
                    CROSS JOIN LATERAL (
                        SELECT i.id FROM items i WHERE i.sym = g.sym AND i.id > g.c ORDER BY i.id LIMIT 1
                    ) l
                    ORDER BY 1
                    """)
                    .noLeakCheck()
                    .returns("""
                            sym\tc\tid
                            a\t2\t3
                            b\t1\t2
                            """);
            assertQuery("""
                    SELECT g.ts, g.sym, g.c, l.id
                    FROM (SELECT ts, sym, count() c FROM trades SAMPLE BY 1d) g
                    CROSS JOIN LATERAL (
                        SELECT i.id FROM items i WHERE i.sym = g.sym AND i.id > g.c ORDER BY i.id LIMIT 1
                    ) l
                    ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tsym\tc\tid
                            2024-01-01T00:00:00.000000Z\ta\t1\t2
                            2024-01-01T00:00:00.000000Z\tb\t1\t2
                            2024-01-02T00:00:00.000000Z\ta\t1\t2
                            """);
            assertQuery("""
                    SELECT g.sym, g.c, l.id
                    FROM (SELECT sym, first(id) c FROM trades GROUP BY sym) g
                    CROSS JOIN LATERAL (
                        SELECT i.id FROM items i WHERE i.sym = g.sym AND i.id > g.c ORDER BY i.id LIMIT 1
                    ) l
                    ORDER BY 1
                    """)
                    .noLeakCheck()
                    .returns("""
                            sym\tc\tid
                            a\t1\t2
                            """);
            assertQuery("""
                    SELECT g.k, g.s, l.id
                    FROM (SELECT id k, last(sym) s FROM trades GROUP BY id) g
                    CROSS JOIN LATERAL (
                        SELECT i.id FROM items i WHERE i.sym = g.s AND i.id > g.k ORDER BY i.id LIMIT 1
                    ) l
                    ORDER BY 1
                    """)
                    .noLeakCheck()
                    .returns("""
                            k\ts\tid
                            1\ta\t2
                            """);
            assertQuery("""
                    SELECT g.s, g.c, l.id
                    FROM (SELECT last(sym) s, count() c FROM trades) g
                    CROSS JOIN LATERAL (
                        SELECT i.id FROM items i WHERE i.sym = g.s AND i.id >= g.c ORDER BY i.id LIMIT 1
                    ) l
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            s\tc\tid
                            a\t3\t3
                            """);
        });
    }

    @Test
    public void testSharedCursorSymbolKeyedDomainConsumerOpenFails() throws Exception {
        assertMemoryLeak(() -> {
            assertConsumerOpenFails("SELECT sym, count() c FROM trades GROUP BY sym", """
                    sym\tc\tid
                    a\t1\t1
                    b\t1\t3
                    c\t1\t4
                    """);
            assertConsumerOpenFails("SELECT sym, first(id) c FROM trades GROUP BY sym", """
                    sym\tc\tid
                    a\t1\t1
                    b\t2\t3
                    c\t1\t4
                    """);
            assertConsumerOpenFails("SELECT last(sym) sym, count() c FROM trades", """
                    sym\tc\tid
                    c\t3\t4
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
    public void testSharedDomainConsumerOpenFails() throws Exception {
        assertMemoryLeak(() -> {
            createSharedDomainTables();
            assertSharedDomainOpenFails("""
                    SELECT o.id, l.v FROM o JOIN LATERAL (
                        SELECT a.id v FROM a WHERE a.x = o.x AND a.id > o.id
                        UNION ALL
                        SELECT b.id v FROM b WHERE b.x = o.x AND b.id > o.id AND b.s ~ $1
                    ) l ORDER BY 1, 2
                    """, "INSERT INTO o VALUES (4, 1, 'p')", "p|q", """
                    id\tv
                    1\t10
                    1\t21
                    1\t23
                    2\t12
                    3\t11
                    3\t24
                    4\t10
                    4\t21
                    4\t23
                    """);
            execute("DROP TABLE o");
            execute("DROP TABLE a");
            execute("DROP TABLE b");
            createRightJoinTables();
            assertRightJoinOpenFails("""
                    SELECT o.id, l.aid, l.bid, l.cid FROM o JOIN LATERAL (
                        SELECT a.id aid, b.id bid, c.id cid
                        FROM a JOIN b ON a.k = b.x AND b.s = o.s AND a.t ~ $1
                        RIGHT JOIN c ON b.k = c.k
                        WHERE c.id > o.id
                    ) l ORDER BY 1, 2, 3, 4
                    """, "u|v");
        });
    }

    @Test
    public void testSharedDomainCopied() throws Exception {
        assertMemoryLeak(() -> {
            createSharedDomainTables();
            execute("CREATE TABLE c (id INT, x INT)");
            execute("INSERT INTO c VALUES (30, 1), (31, 2)");
            assertQuery("""
                    SELECT m.id, m.v, l2.w FROM (
                        SELECT o.id, o.x, l.v FROM o JOIN LATERAL (
                            SELECT a.id v FROM a WHERE a.x = o.x AND a.id > o.id
                            UNION ALL
                            SELECT b.id v FROM b WHERE b.x = o.x AND b.id > o.id
                        ) l
                    ) m JOIN LATERAL (SELECT count() w FROM c WHERE c.x = m.x AND c.id > m.v) l2
                    ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .withPlanContaining(" ".repeat(72) + "SelectedRecord\n" + " ".repeat(76) + "(Shared)")
                    .returns("""
                            id\tv\tw
                            1\t10\t1
                            1\t21\t1
                            1\t23\t1
                            2\t12\t1
                            3\t11\t0
                            3\t24\t0
                            """);
        });
    }

    @Test
    public void testSharedDomainDiscarded() throws Exception {
        assertMemoryLeak(() -> {
            createDiscardTables();
            final String bRows = """
                    id\tv
                    1\t21
                    1\t23
                    3\t24
                    """;
            final String cAndBRows = """
                    id\tv
                    1\t21
                    1\t23
                    1\t30
                    1\t31
                    2\t30
                    2\t31
                    3\t24
                    3\t30
                    3\t31
                    """;
            assertDiscardedDomain(DISCARDED_BY_FILTER, "(Shared)", bRows);
            assertDiscardedDomain(DISCARDED_BY_JOIN_FILTER, "(Shared)", bRows);
            assertDiscardedDomain(DISCARDED_BY_EMPTIED_SLAVE, "(Shared)", cAndBRows);
            assertDiscardedDomain(DISCARDED_BY_EMPTIED_MASTER, "(Shared)", cAndBRows);
            assertDiscardedDomain(DISCARDED_BY_EMPTY_WINDOW_JOIN, "(Shared)", bRows);
        });
    }

    @Test
    public void testSharedDomainDiscardedOpenFails() throws Exception {
        assertMemoryLeak(() -> {
            createSharedDomainTables();
            assertSharedDomainOpenFails("""
                    SELECT o.id, l.v FROM (SELECT x, min(id) id FROM o WHERE s ~ $1 GROUP BY x) o JOIN LATERAL (
                        SELECT v FROM (SELECT a.id v FROM a WHERE a.x = o.x AND a.id > o.id) WHERE false
                        UNION ALL
                        SELECT b.id v FROM b WHERE b.x = o.x AND b.id > o.id
                    ) l ORDER BY 1, 2
                    """, "INSERT INTO o VALUES (4, 5, 'q')", "p|q", """
                    id\tv
                    1\t21
                    1\t23
                    3\t24
                    4\t20
                    """);
        });
    }

    @Test
    public void testSharedDomainDiscardedSharedSource() throws Exception {
        assertMemoryLeak(() -> {
            createDiscardTables();
            final String bRows = """
                    id\tv
                    1\t21
                    1\t21
                    1\t23
                    1\t23
                    3\t24
                    3\t24
                    """;
            final String sharedRead = " ".repeat(40) + "(Shared)\n" + " ".repeat(44) + "GroupBy vectorized: false\n"
                    + " ".repeat(46) + "keys: [__qdb_outer_ref__0_x,__qdb_outer_ref__0_id]";
            final String cAndBRows = """
                    id\tv
                    1\t21
                    1\t21
                    1\t23
                    1\t23
                    1\t30
                    1\t31
                    2\t30
                    2\t31
                    3\t24
                    3\t24
                    3\t30
                    3\t31
                    """;
            assertDiscardedDomain("""
                    SELECT o.id, l.v FROM (SELECT x, min(id) id FROM o GROUP BY x) o JOIN LATERAL (
                        SELECT v FROM (SELECT a.id v FROM a WHERE a.x = o.x AND a.id > o.id) WHERE false
                        UNION ALL
                        SELECT b.id v FROM b WHERE b.x = o.x AND b.id > o.id
                        UNION ALL
                        SELECT b.id v FROM b WHERE b.x = o.x AND b.id > o.id
                    ) l ORDER BY 1, 2
                    """, sharedRead, bRows);
            assertDiscardedDomain("""
                    SELECT o.id, l.v FROM (SELECT x, min(id) id FROM o GROUP BY x) o JOIN LATERAL (
                        SELECT a.id v FROM a JOIN c ON a.id = c.id WHERE a.x = o.x AND a.id > o.id AND false
                        UNION ALL
                        SELECT b.id v FROM b WHERE b.x = o.x AND b.id > o.id
                        UNION ALL
                        SELECT b.id v FROM b WHERE b.x = o.x AND b.id > o.id
                    ) l ORDER BY 1, 2
                    """, sharedRead, bRows);
            assertDiscardedDomain("""
                    SELECT o.id, l.v FROM (SELECT x, min(id) id FROM o GROUP BY x) o JOIN LATERAL (
                        SELECT c.id v FROM c LEFT JOIN (SELECT a.id, a.x FROM a WHERE a.x = o.x AND a.id > o.id) aa ON c.id = aa.id AND false
                        UNION ALL
                        SELECT b.id v FROM b WHERE b.x = o.x AND b.id > o.id
                        UNION ALL
                        SELECT b.id v FROM b WHERE b.x = o.x AND b.id > o.id
                    ) l ORDER BY 1, 2
                    """, sharedRead, cAndBRows);
            assertDiscardedDomain("""
                    SELECT o.id, l.v FROM (SELECT x, min(id) id FROM o GROUP BY x) o JOIN LATERAL (
                        SELECT c.id v FROM (SELECT a.id, a.x FROM a WHERE a.x = o.x AND a.id > o.id) aa RIGHT JOIN c ON c.id = aa.id AND false
                        UNION ALL
                        SELECT b.id v FROM b WHERE b.x = o.x AND b.id > o.id
                        UNION ALL
                        SELECT b.id v FROM b WHERE b.x = o.x AND b.id > o.id
                    ) l ORDER BY 1, 2
                    """, sharedRead, cAndBRows);
        });
    }

    @Test
    public void testSharedDomainFullJoin() throws Exception {
        assertMemoryLeak(() -> {
            createRightJoinTables();
            assertQuery("""
                    SELECT o.id, l.aid, l.bid, l.cid FROM o JOIN LATERAL (
                        SELECT a.id aid, b.id bid, c.id cid
                        FROM a RIGHT JOIN b ON a.k = b.k AND b.id > o.id
                        FULL JOIN c ON b.k = c.k AND c.s = o.s
                    ) l ORDER BY 1, 2, 3, 4
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .withPlanContaining("(Shared)")
                    .returns("""
                            id\taid\tbid\tcid
                            1\tnull\tnull\t31
                            1\tnull\tnull\t32
                            1\t10\t20\t30
                            1\t11\t21\tnull
                            2\tnull\tnull\t30
                            2\tnull\tnull\t32
                            2\t10\t20\tnull
                            2\t11\t21\t31
                            """);
        });
    }

    @Test
    public void testSharedDomainIntKeys() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, x INT)");
            execute("CREATE TABLE a (id INT, x INT)");
            execute("CREATE TABLE b (id INT, k INT)");
            execute("INSERT INTO o VALUES (1, 1), (2, NULL), (3, 3)");
            execute("INSERT INTO a VALUES (10, 1), (11, 3), (12, NULL), (0, 1)");
            execute("INSERT INTO b VALUES (20, 5), (21, 1), (22, 7), (23, 1), (24, 3)");
            assertSharedDomainRows();
        });
    }

    @Test
    public void testSharedDomainNarrowed() throws Exception {
        assertMemoryLeak(() -> {
            createSharedDomainTables();
            execute("CREATE TABLE c (id INT, x INT)");
            execute("INSERT INTO c VALUES (30, 1), (31, 2)");
            assertQuery("""
                    SELECT o.id, l.v FROM o JOIN LATERAL (
                        SELECT a.id v FROM a WHERE a.x = o.x AND a.id > o.id
                        UNION ALL
                        SELECT b.id v FROM b WHERE b.x = o.x AND b.id > o.id AND o.id > 1
                        UNION ALL
                        SELECT c.id v FROM c WHERE c.x = o.x AND c.id > o.id
                    ) l ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .withPlanContaining("(Shared)")
                    .returns("""
                            id\tv
                            1\t10
                            1\t30
                            2\t12
                            2\t31
                            3\t11
                            3\t24
                            """);
            assertQuery("""
                    SELECT o.id, l.v FROM o JOIN LATERAL (
                        SELECT a.id v FROM a WHERE a.x = o.x AND a.id > o.id
                        UNION ALL
                        SELECT b.id v FROM b WHERE b.x = o.x AND b.id > o.id
                        UNION ALL
                        SELECT c.id v FROM c WHERE c.x = o.x AND c.x = 1 AND c.id > o.id
                    ) l ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .withPlanContaining("(Shared)")
                    .returns("""
                            id\tv
                            1\t10
                            1\t21
                            1\t23
                            1\t30
                            2\t12
                            3\t11
                            3\t24
                            """);
            assertQuery("""
                    SELECT o.id, l.v FROM o JOIN LATERAL (
                        SELECT a.id v FROM a WHERE a.x = o.x AND a.id > o.id AND o.id > 1
                        UNION ALL
                        SELECT b.id v FROM b WHERE b.x = o.x AND b.id > o.id
                    ) l ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .withPlanContaining("(Shared)")
                    .returns("""
                            id\tv
                            1\t21
                            1\t23
                            2\t12
                            3\t11
                            3\t24
                            """);
        });
    }

    @Test
    public void testSharedDomainNarrowedEqually() throws Exception {
        assertMemoryLeak(() -> {
            createSharedDomainTables();
            assertQuery("""
                    SELECT o.id, l.v FROM o JOIN LATERAL (
                        SELECT a.id v FROM a WHERE a.x = o.x AND a.id > o.id AND o.id > 1
                        UNION ALL
                        SELECT b.id v FROM b WHERE b.x = o.x AND b.id > o.id AND o.id > 1
                    ) l ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .withPlanContaining("(Shared)")
                    .returns("""
                            id\tv
                            2\t12
                            3\t11
                            3\t24
                            """);
        });
    }

    @Test
    public void testSharedDomainOuterJoinFalseResidual() throws Exception {
        assertMemoryLeak(() -> {
            createTradesItemsTables();
            final String outerGroupBy = enableParallelGroupBy ? "GroupBy vectorized: true workers: 1" : "GroupBy vectorized: false";
            assertQuery("""
                    SELECT g.s, l.id FROM (SELECT sym s, count() c FROM trades GROUP BY sym) g
                    CROSS JOIN LATERAL (
                        SELECT i.id FROM items i LEFT JOIN items j ON j.id = i.id AND false WHERE i.sym = g.s AND i.id > g.c
                        UNION ALL
                        SELECT k.id FROM items k WHERE k.sym = g.s AND k.id > g.c
                    ) l ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .withPlan("""
                            Encode sort
                              keys: [s, id]
                                SelectedRecord
                                    Hash Join
                                      condition: l.__qdb_outer_ref__0_s=g.s and l.__qdb_outer_ref__0_c=g.c
                                        SelectedRecord
                                            %1$s
                                              keys: [sym]
                                              values: [count(*)]
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: trades
                                        Hash
                                            Union All
                                                SelectedRecord
                                                    Hash Left Outer Join Light
                                                      condition: j.id=i.id
                                                      filter: false
                                                        Filter filter: __qdb_outer_ref__0.__qdb_outer_ref__0_c<id
                                                            Hash Join Light
                                                              condition: __qdb_outer_ref__0_s=sym
                                                              symbolKeyJoin: true
                                                                PageFrame
                                                                    Row forward scan
                                                                    Frame forward scan on: items
                                                                Hash
                                                                    GroupBy vectorized: false
                                                                      keys: [__qdb_outer_ref__0_s,__qdb_outer_ref__0_c]
                                                                        SelectedRecord
                                                                            (Shared)
                                                                                SelectedRecord
                                                                                    %1$s
                                                                                      keys: [sym]
                                                                                      values: [count(*)]
                                                                                        PageFrame
                                                                                            Row forward scan
                                                                                            Frame forward scan on: trades
                                                        Hash
                                                            Empty table
                                                SelectedRecord
                                                    Filter filter: __qdb_outer_ref__0_1.__qdb_outer_ref__0_c<id
                                                        Hash Join Light
                                                          condition: __qdb_outer_ref__0_s=sym
                                                          symbolKeyJoin: true
                                                            PageFrame
                                                                Row forward scan
                                                                Frame forward scan on: items
                                                            Hash
                                                                SelectedRecord
                                                                    (Shared)
                                                                        GroupBy vectorized: false
                                                                          keys: [__qdb_outer_ref__0_s,__qdb_outer_ref__0_c]
                                                                            SelectedRecord
                                                                                (Shared)
                                                                                    SelectedRecord
                                                                                        %1$s
                                                                                          keys: [sym]
                                                                                          values: [count(*)]
                                                                                            PageFrame
                                                                                                Row forward scan
                                                                                                Frame forward scan on: trades
                            """.formatted(outerGroupBy))
                    .returns("""
                            s\tid
                            a\t3
                            a\t3
                            b\t2
                            b\t2
                            """);
        });
    }

    @Test
    public void testSharedDomainOuterJoinFalseResidualNested() throws Exception {
        assertMemoryLeak(() -> {
            createTradesItemsTables();
            assertQuery("""
                    SELECT m.s, m.id, l2.w FROM (
                        SELECT g.s, l.id FROM (SELECT sym s, count() c FROM trades GROUP BY sym) g
                        CROSS JOIN LATERAL (
                            SELECT i.id FROM items i WHERE i.sym = g.s AND i.id > g.c
                            UNION ALL
                            SELECT k.id FROM items k WHERE k.sym = g.s AND k.id > g.c
                        ) l
                    ) m CROSS JOIN LATERAL (
                        SELECT i.id w FROM items i LEFT JOIN items j ON j.id = i.id AND false WHERE i.sym = m.s AND i.id < m.id
                        UNION ALL
                        SELECT k.id w FROM items k WHERE k.sym = m.s AND k.id < m.id
                    ) l2 ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .withPlanContaining(" ".repeat(40) + "(Shared)\n" + " ".repeat(44) + "GroupBy vectorized: false\n"
                            + " ".repeat(46) + "keys: [__qdb_outer_ref__1_s,__qdb_outer_ref__1_id]")
                    .returns("""
                            s\tid\tw
                            a\t3\t1
                            a\t3\t1
                            a\t3\t1
                            a\t3\t1
                            a\t3\t2
                            a\t3\t2
                            a\t3\t2
                            a\t3\t2
                            b\t2\t1
                            b\t2\t1
                            b\t2\t1
                            b\t2\t1
                            """);
        });
    }

    @Test
    public void testSharedDomainPrimaryOpenFails() throws Exception {
        assertMemoryLeak(() -> {
            createSharedDomainTables();
            assertSharedDomainOpenFails("""
                    SELECT o.id, l.v FROM (SELECT id, x FROM o WHERE s ~ $1) o JOIN LATERAL (
                        SELECT a.id v FROM a WHERE a.x = o.x AND a.id > o.id
                        UNION ALL
                        SELECT b.id v FROM b WHERE b.x = o.x AND b.id > o.id
                    ) l ORDER BY 1, 2
                    """, "INSERT INTO o VALUES (4, 1, 'p')", "p|q", """
                    id\tv
                    1\t10
                    1\t21
                    1\t23
                    2\t12
                    3\t11
                    3\t24
                    4\t10
                    4\t21
                    4\t23
                    """);
            execute("DROP TABLE o");
            execute("DROP TABLE a");
            execute("DROP TABLE b");
            createRightJoinTables();
            assertRightJoinOpenFails("""
                    SELECT o.id, l.aid, l.bid, l.cid FROM (SELECT id, k, s FROM o WHERE s ~ $1) o JOIN LATERAL (
                        SELECT a.id aid, b.id bid, c.id cid
                        FROM a JOIN b ON a.k = b.x AND b.s = o.s
                        RIGHT JOIN c ON b.k = c.k
                        WHERE c.id > o.id
                    ) l ORDER BY 1, 2, 3, 4
                    """, "x|y");
        });
    }

    @Test
    public void testSharedDomainReadBeforeDomain() throws Exception {
        assertMemoryLeak(() -> {
            createSharedDomainTables();
            assertQuery("""
                    SELECT o.id, l.v FROM o JOIN LATERAL (
                        SELECT t.v FROM (
                            SELECT a.id v FROM a WHERE a.x = o.x
                            UNION ALL
                            SELECT b.id v FROM b WHERE b.id > o.id
                        ) t
                        JOIN (SELECT b.id w FROM b WHERE b.x <= o.x AND b.id > o.id) u ON u.w = t.v
                    ) l ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .withPlan(enableParallelGroupBy ? """
                            Encode sort
                              keys: [id, v]
                                SelectedRecord
                                    Hash Join
                                      condition: l.__qdb_outer_ref__0_x=o.x and l.__qdb_outer_ref__0_id=o.id
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: o
                                        Hash
                                            SelectedRecord
                                                Hash Join
                                                  condition: u.w=t.v and __qdb_outer_ref__0_x=__qdb_outer_ref__0_x and __qdb_outer_ref__0_id=__qdb_outer_ref__0_id
                                                    Union All
                                                        SelectedRecord
                                                            Hash Join Light
                                                              condition: __qdb_outer_ref__0_x=x
                                                                PageFrame
                                                                    Row forward scan
                                                                    Frame forward scan on: a
                                                                Hash
                                                                    Async Group By workers: 1
                                                                      keys: [__qdb_outer_ref__0_x,__qdb_outer_ref__0_id]
                                                                      filter: null
                                                                        PageFrame
                                                                            Row forward scan
                                                                            Frame forward scan on: o
                                                        SelectedRecord
                                                            Hash Join Light
                                                              condition: __qdb_outer_ref__0_id=__qdb_outer_ref__0_id
                                                                SelectedRecord
                                                                    Filter filter: __qdb_outer_ref__0_1.__qdb_outer_ref__0_id<id
                                                                        Cross Join
                                                                            PageFrame
                                                                                Row forward scan
                                                                                Frame forward scan on: b
                                                                            GroupBy vectorized: true workers: 1
                                                                              keys: [id]
                                                                              values: [count(*)]
                                                                                PageFrame
                                                                                    Row forward scan
                                                                                    Frame forward scan on: o
                                                                Hash
                                                                    SelectedRecord
                                                                        (Shared)
                                                                            Async Group By workers: 1
                                                                              keys: [__qdb_outer_ref__0_x,__qdb_outer_ref__0_id]
                                                                              filter: null
                                                                                PageFrame
                                                                                    Row forward scan
                                                                                    Frame forward scan on: o
                                                    Hash
                                                        SelectedRecord
                                                            Filter filter: (__qdb_outer_ref__0_3.__qdb_outer_ref__0_x>=x and __qdb_outer_ref__0_3.__qdb_outer_ref__0_id<id)
                                                                Cross Join
                                                                    PageFrame
                                                                        Row forward scan
                                                                        Frame forward scan on: b
                                                                    SelectedRecord
                                                                        (Shared)
                                                                            Async Group By workers: 1
                                                                              keys: [__qdb_outer_ref__0_x,__qdb_outer_ref__0_id]
                                                                              filter: null
                                                                                PageFrame
                                                                                    Row forward scan
                                                                                    Frame forward scan on: o
                            """ : """
                            Encode sort
                              keys: [id, v]
                                SelectedRecord
                                    Hash Join
                                      condition: l.__qdb_outer_ref__0_x=o.x and l.__qdb_outer_ref__0_id=o.id
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: o
                                        Hash
                                            SelectedRecord
                                                Hash Join
                                                  condition: u.w=t.v and __qdb_outer_ref__0_x=__qdb_outer_ref__0_x and __qdb_outer_ref__0_id=__qdb_outer_ref__0_id
                                                    Union All
                                                        SelectedRecord
                                                            Hash Join Light
                                                              condition: __qdb_outer_ref__0_x=x
                                                                PageFrame
                                                                    Row forward scan
                                                                    Frame forward scan on: a
                                                                Hash
                                                                    GroupBy vectorized: false
                                                                      keys: [__qdb_outer_ref__0_x,__qdb_outer_ref__0_id]
                                                                        PageFrame
                                                                            Row forward scan
                                                                            Frame forward scan on: o
                                                        SelectedRecord
                                                            Hash Join Light
                                                              condition: __qdb_outer_ref__0_id=__qdb_outer_ref__0_id
                                                                SelectedRecord
                                                                    Filter filter: __qdb_outer_ref__0_1.__qdb_outer_ref__0_id<id
                                                                        Cross Join
                                                                            PageFrame
                                                                                Row forward scan
                                                                                Frame forward scan on: b
                                                                            GroupBy vectorized: false
                                                                              keys: [__qdb_outer_ref__0_id]
                                                                                PageFrame
                                                                                    Row forward scan
                                                                                    Frame forward scan on: o
                                                                Hash
                                                                    SelectedRecord
                                                                        (Shared)
                                                                            GroupBy vectorized: false
                                                                              keys: [__qdb_outer_ref__0_x,__qdb_outer_ref__0_id]
                                                                                PageFrame
                                                                                    Row forward scan
                                                                                    Frame forward scan on: o
                                                    Hash
                                                        SelectedRecord
                                                            Filter filter: (__qdb_outer_ref__0_3.__qdb_outer_ref__0_x>=x and __qdb_outer_ref__0_3.__qdb_outer_ref__0_id<id)
                                                                Cross Join
                                                                    PageFrame
                                                                        Row forward scan
                                                                        Frame forward scan on: b
                                                                    SelectedRecord
                                                                        (Shared)
                                                                            GroupBy vectorized: false
                                                                              keys: [__qdb_outer_ref__0_x,__qdb_outer_ref__0_id]
                                                                                PageFrame
                                                                                    Row forward scan
                                                                                    Frame forward scan on: o
                            """)
                    .returns("""
                            id\tv
                            1\t21
                            1\t23
                            2\t21
                            2\t23
                            3\t21
                            3\t23
                            3\t24
                            """);
        });
    }

    @Test
    public void testSharedDomainRegenerated() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE p (id INT, s SYMBOL INDEX TYPE POSTING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE pa (id INT, s SYMBOL)");
            execute("CREATE TABLE pb (id INT, s SYMBOL)");
            execute("INSERT INTO p VALUES (1, 'a', '2024-01-01'), (2, 'b', '2024-01-01'), (3, 'a', '2024-01-02')");
            execute("INSERT INTO pa VALUES (10, 'a'), (11, 'b'), (12, 'c')");
            execute("INSERT INTO pb VALUES (20, 'a'), (21, 'a'), (22, 'b')");
            assertQuery("""
                    SELECT o.id, l.v FROM p o JOIN LATERAL (
                        SELECT pa.id v FROM pa WHERE pa.s = o.s
                        UNION ALL
                        SELECT pb.id v FROM pb WHERE pb.s = o.s
                    ) l ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .withPlan("""
                            Encode sort
                              keys: [id, v]
                                SelectedRecord
                                    Hash Join
                                      condition: l.__qdb_outer_ref__0_s=o.s
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: p
                                        Hash
                                            Union All
                                                SelectedRecord
                                                    Hash Join
                                                      condition: __qdb_outer_ref__0_s=s
                                                      symbolKeyJoin: true
                                                        PageFrame
                                                            Row forward scan
                                                            Frame forward scan on: pa
                                                        Hash
                                                            PostingIndex op: distinct on: __qdb_outer_ref__0_s
                                                                Frame forward scan on: p
                                                SelectedRecord
                                                    Hash Join
                                                      condition: __qdb_outer_ref__0_s=s
                                                      symbolKeyJoin: true
                                                        PageFrame
                                                            Row forward scan
                                                            Frame forward scan on: pb
                                                        Hash
                                                            SelectedRecord
                                                                PostingIndex op: distinct on: __qdb_outer_ref__0_s
                                                                    Frame forward scan on: p
                            """)
                    .returns("""
                            id\tv
                            1\t10
                            1\t20
                            1\t21
                            2\t11
                            2\t22
                            3\t10
                            3\t20
                            3\t21
                            """);
        });
    }

    @Test
    public void testSharedDomainReorderedOuterColumns() throws Exception {
        assertMemoryLeak(() -> {
            createSharedDomainTables();
            assertQuery("""
                    SELECT o.id, l.v FROM o JOIN LATERAL (
                        SELECT a.id v FROM a WHERE a.id > o.id AND a.x = o.x
                        UNION ALL
                        SELECT b.id v FROM b WHERE b.x = o.x AND b.id > o.id
                    ) l ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .withPlanContaining("(Shared)")
                    .returns("""
                            id\tv
                            1\t10
                            1\t21
                            1\t23
                            2\t12
                            3\t11
                            3\t24
                            """);
        });
    }

    @Test
    public void testSharedDomainRightJoin() throws Exception {
        assertMemoryLeak(() -> {
            createRightJoinTables();
            assertQuery("""
                    SELECT o.id, l.aid, l.bid, l.cid FROM o JOIN LATERAL (
                        SELECT a.id aid, b.id bid, c.id cid
                        FROM a JOIN b ON a.k = b.x AND b.s = o.s
                        RIGHT JOIN c ON b.k = c.k
                        WHERE a.k <= o.k
                    ) l ORDER BY 1, 2, 3, 4
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .withPlanContaining("(Shared)")
                    .returns("""
                            id\taid\tbid\tcid
                            1\t10\t20\t30
                            2\t11\t21\t31
                            """);
            assertQuery("""
                    SELECT o.id, l.aid, l.bid, l.cid FROM o JOIN LATERAL (
                        SELECT a.id aid, b.id bid, c.id cid
                        FROM a JOIN b ON a.k = b.x AND b.s = o.s
                        RIGHT JOIN c ON b.k = c.k
                        WHERE c.id > o.id
                    ) l ORDER BY 1, 2, 3, 4
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .withPlanContaining("(Shared)")
                    .returns("""
                            id\taid\tbid\tcid
                            1\tnull\tnull\t31
                            1\tnull\tnull\t32
                            1\t10\t20\t30
                            2\tnull\tnull\t30
                            2\tnull\tnull\t32
                            2\t11\t21\t31
                            """);
            execute("INSERT INTO o VALUES (3, NULL, NULL)");
            assertQuery("""
                    SELECT o.id, l.aid, l.bid, l.cid FROM o JOIN LATERAL (
                        SELECT a.id aid, b.id bid, c.id cid
                        FROM a JOIN b ON a.k = b.x AND b.s = o.s
                        RIGHT JOIN c ON b.k = c.k
                        WHERE a.k <= o.k
                    ) l ORDER BY 1, 2, 3, 4
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .withPlanContaining("(Shared)")
                    .returns("""
                            id\taid\tbid\tcid
                            1\t10\t20\t30
                            2\t11\t21\t31
                            3\tnull\tnull\t30
                            3\tnull\tnull\t31
                            3\tnull\tnull\t32
                            """);
        });
    }

    @Test
    public void testSharedDomainSetOperations() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, x INT)");
            execute("CREATE TABLE a (id INT, x INT)");
            execute("CREATE TABLE b (id INT, x INT)");
            execute("INSERT INTO o VALUES (1, 1), (2, 2), (3, 3)");
            execute("INSERT INTO a VALUES (10, 1), (11, 3), (12, 2), (0, 1), (20, 1)");
            execute("INSERT INTO b VALUES (20, 1), (21, 1), (11, 3), (12, 5)");
            assertSetOperation("UNION", """
                    id\tv
                    1\t10
                    1\t20
                    1\t21
                    2\t12
                    3\t11
                    """);
            assertSetOperation("INTERSECT", """
                    id\tv
                    1\t20
                    3\t11
                    """);
            assertSetOperation("EXCEPT", """
                    id\tv
                    1\t10
                    2\t12
                    """);
        });
    }

    @Test
    public void testSharedDomainSymbolKeys() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, x SYMBOL)");
            execute("CREATE TABLE a (id INT, x SYMBOL)");
            execute("CREATE TABLE b (id INT, k SYMBOL)");
            execute("INSERT INTO o VALUES (1, '1'), (2, NULL), (3, '3')");
            execute("INSERT INTO a VALUES (10, '1'), (11, '3'), (12, NULL), (0, '1')");
            execute("INSERT INTO b VALUES (20, '5'), (21, '1'), (22, '7'), (23, '1'), (24, '3')");
            assertSharedDomainRows();
        });
    }

    @Test
    public void testSharedDomainSymbolKeysThreeBranches() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, x SYMBOL)");
            execute("CREATE TABLE a (id INT, x SYMBOL)");
            execute("CREATE TABLE b (id INT, k SYMBOL)");
            execute("CREATE TABLE c (id INT, k SYMBOL)");
            execute("INSERT INTO o VALUES (1, '1'), (2, NULL), (3, '3')");
            execute("INSERT INTO a VALUES (10, '1'), (11, '3'), (12, NULL), (0, '1')");
            execute("INSERT INTO b VALUES (20, '5'), (21, '1'), (22, '7'), (23, '1'), (24, '3')");
            execute("INSERT INTO c VALUES (30, '1'), (31, NULL), (32, '3'), (1, '3')");
            assertQuery("""
                    SELECT o.id, l.v FROM o JOIN LATERAL (
                        SELECT a.id v FROM a WHERE a.x = o.x AND a.id > o.id
                        UNION ALL
                        SELECT b.id v FROM b WHERE b.k = o.x AND b.id > o.id
                        UNION ALL
                        SELECT c.id v FROM c WHERE c.k = o.x AND c.id > o.id
                    ) l ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .withPlanContaining("(Shared)")
                    .returns("""
                            id\tv
                            1\t10
                            1\t21
                            1\t23
                            1\t30
                            2\t12
                            2\t31
                            3\t11
                            3\t24
                            3\t32
                            """);
        });
    }

    @Test
    public void testSharedDomainThreeBranches() throws Exception {
        assertMemoryLeak(() -> {
            createSharedDomainTables();
            execute("CREATE TABLE c (id INT, x INT)");
            execute("INSERT INTO c VALUES (30, 1), (31, 2)");
            assertQuery("""
                    SELECT o.id, l.v FROM o JOIN LATERAL (
                        SELECT a.id v FROM a WHERE a.x = o.x AND a.id > o.id
                        UNION ALL
                        SELECT b.id v FROM b WHERE b.x = o.x AND b.id > o.id
                        UNION ALL
                        SELECT c.id v FROM c WHERE c.x = o.x AND c.id > o.id
                    ) l ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .withPlanContaining("(Shared)")
                    .returns("""
                            id\tv
                            1\t10
                            1\t21
                            1\t23
                            1\t30
                            2\t12
                            2\t31
                            3\t11
                            3\t24
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

    private static void createDiscardTables() throws SqlException {
        createSharedDomainTables();
        execute("CREATE TABLE c (id INT, x INT)");
        execute("CREATE TABLE t (id INT, x INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("CREATE TABLE p (x INT, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("INSERT INTO c VALUES (30, 1), (31, 2)");
        execute("""
                INSERT INTO t VALUES
                (10, 1, '2024-01-01T00:00:00.000000Z'),
                (11, 3, '2024-01-01T00:01:00.000000Z'),
                (12, 2, '2024-01-01T00:02:00.000000Z')
                """);
        execute("INSERT INTO p VALUES (1, 1.5, '2024-01-01T00:00:30.000000Z'), (2, 2.5, '2024-01-01T00:01:30.000000Z')");
    }

    private static void createRightJoinTables() throws SqlException {
        execute("CREATE TABLE o (id INT, k INT, s SYMBOL)");
        execute("CREATE TABLE a (id INT, k INT, t VARCHAR)");
        execute("CREATE TABLE b (id INT, k INT, x INT, s SYMBOL)");
        execute("CREATE TABLE c (id INT, k INT, s SYMBOL)");
        execute("INSERT INTO o VALUES (1, 1, 'x'), (2, 2, 'y')");
        execute("INSERT INTO a VALUES (10, 1, 'u'), (11, 2, 'v')");
        execute("INSERT INTO b VALUES (20, 1, 1, 'x'), (21, 2, 2, 'y')");
        execute("INSERT INTO c VALUES (30, 1, 'x'), (31, 2, 'y'), (32, 3, 'x')");
    }

    private static void createSharedDomainTables() throws SqlException {
        execute("CREATE TABLE o (id INT, x INT, s SYMBOL)");
        execute("CREATE TABLE a (id INT, x INT)");
        execute("CREATE TABLE b (id INT, x INT, s SYMBOL)");
        execute("INSERT INTO o VALUES (1, 1, 'p'), (2, 2, 'q'), (3, 3, 'p')");
        execute("INSERT INTO a VALUES (10, 1), (11, 3), (12, 2), (0, 1)");
        execute("INSERT INTO b VALUES (20, 5, 'p'), (21, 1, 'p'), (22, 7, 'q'), (23, 1, 'q'), (24, 3, 'p')");
    }

    private static void createTradesItemsTables() throws SqlException {
        execute("CREATE TABLE trades (sym SYMBOL, id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("CREATE TABLE items (sym SYMBOL, id INT)");
        execute("""
                INSERT INTO trades VALUES
                ('a', 1, '2024-01-01T00:00:00.000000Z'),
                ('b', 2, '2024-01-01T01:00:00.000000Z'),
                ('a', 3, '2024-01-02T00:00:00.000000Z')
                """);
        execute("INSERT INTO items VALUES ('a', 1), ('a', 2), ('a', 3), ('b', 1), ('b', 2)");
    }

    private void assertConsumerOpenFails(String source, String expected) throws Exception {
        execute("CREATE TABLE trades (sym SYMBOL, id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("CREATE TABLE items (sym SYMBOL, id INT)");
        execute("""
                INSERT INTO trades VALUES
                ('a', 1, '2024-01-01T00:00:00.000000Z'),
                ('b', 2, '2024-01-01T01:00:00.000000Z')
                """);
        execute("INSERT INTO items VALUES ('a', 1), ('a', 2), ('b', 3), ('c', 4)");
        final String sql = "SELECT g.sym, g.c, l.id FROM (" + source + ") g CROSS JOIN LATERAL ("
                + "SELECT i.id FROM items i WHERE i.sym = g.sym AND i.id >= g.c AND i.sym ~ $1 ORDER BY i.id LIMIT 1"
                + ") l ORDER BY 1";
        bindVariableService.clear();
        bindVariableService.setStr(0, "(");
        try (RecordCursorFactory factory = select(sql)) {
            try (RecordCursor ignored = factory.getCursor(sqlExecutionContext)) {
                Assert.fail("expected an invalid regex");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "Unclosed group");
            }
            execute("INSERT INTO trades VALUES ('c', 1, '2024-01-02T00:00:00.000000Z')");
            bindVariableService.setStr(0, "a|b|c");
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                TestUtils.assertCursor(expected, cursor, factory.getMetadata(), true, sink);
            }
        }
        execute("DROP TABLE trades");
        execute("DROP TABLE items");
    }

    private void assertDiscardedDomain(String sql, CharSequence sharedRead, String expected) throws Exception {
        assertQuery(sql)
                .noLeakCheck()
                .expectSize()
                .withPlanContaining("Empty table", sharedRead)
                .returns(expected);
    }

    private void assertRightJoinOpenFails(String sql, String pattern) throws Exception {
        assertSharedDomainOpenFails(sql, "INSERT INTO o VALUES (3, 1, 'x')", pattern, """
                id\taid\tbid\tcid
                1\tnull\tnull\t31
                1\tnull\tnull\t32
                1\t10\t20\t30
                2\tnull\tnull\t30
                2\tnull\tnull\t32
                2\t11\t21\t31
                3\tnull\tnull\t31
                3\tnull\tnull\t32
                3\t10\t20\t30
                """);
    }

    private void assertSetOperation(String operation, String expected) throws Exception {
        assertQuery("SELECT o.id, l.v FROM o JOIN LATERAL (SELECT a.id v FROM a WHERE a.x = o.x AND a.id > o.id "
                + operation + " SELECT b.id v FROM b WHERE b.x = o.x AND b.id > o.id) l ORDER BY 1, 2")
                .noLeakCheck()
                .expectSize()
                .withPlanContaining("(Shared)")
                .returns(expected);
    }

    private void assertSharedDomainOpenFails(String sql, String insert, String pattern, String expected) throws Exception {
        bindVariableService.clear();
        bindVariableService.setStr(0, "(");
        try (RecordCursorFactory factory = select(sql)) {
            TestUtils.assertContains(planText(factory), "(Shared)");
            try (RecordCursor ignored = factory.getCursor(sqlExecutionContext)) {
                Assert.fail("expected an invalid regex");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "Unclosed group");
            }
            execute(insert);
            bindVariableService.setStr(0, pattern);
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                TestUtils.assertCursor(expected, cursor, factory.getMetadata(), true, sink);
            }
        }
    }

    private void assertSharedDomainRows() throws Exception {
        assertQuery("""
                SELECT o.id, l.v FROM o JOIN LATERAL (
                    SELECT a.id v FROM a WHERE a.x = o.x AND a.id > o.id
                    UNION ALL
                    SELECT b.id v FROM b WHERE b.k = o.x AND b.id > o.id
                ) l ORDER BY 1, 2
                """)
                .noLeakCheck()
                .expectSize()
                .withPlanContaining("(Shared)")
                .returns("""
                        id\tv
                        1\t10
                        1\t21
                        1\t23
                        2\t12
                        3\t11
                        3\t24
                        """);
    }
}
