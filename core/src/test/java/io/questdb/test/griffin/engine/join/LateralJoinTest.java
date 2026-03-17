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

package io.questdb.test.griffin.engine.join;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class LateralJoinTest extends AbstractCairoTest {

    // Test CROSS JOIN LATERAL syntax
    @Test
    public void testCrossJoinLateral() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO orders VALUES (1, '2024-01-01T00:00:00.000000Z'), (2, '2024-01-01T01:00:00.000000Z')");
            execute("""
                    INSERT INTO trades VALUES
                    (1, 1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 2, 20.0, '2024-01-01T01:10:00.000000Z')
                    """);

            assertQueryNoLeakCheck(
                    """
                            id\tqty
                            1\t10.0
                            2\t20.0
                            """,
                    """
                            SELECT o.id, t.qty
                            FROM orders o
                            CROSS JOIN LATERAL (SELECT qty FROM trades WHERE order_id = o.id) t
                            ORDER BY o.id
                            """,
                    null, true, false
            );
        });
    }

    // Test LATERAL requires subquery (not table name)
    @Test
    public void testLateralRequiresSubquery() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");

            assertException(
                    "SELECT * FROM orders o JOIN LATERAL trades t",
                    0,
                    "LATERAL requires a subquery"
            );
        });
    }

    // Test LATERAL keyword as standalone (LATERAL (subquery) alias — no JOIN keyword)
    @Test
    public void testLateralStandalone() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO orders VALUES (1, '2024-01-01T00:00:00.000000Z'), (2, '2024-01-01T01:00:00.000000Z')");
            execute("""
                    INSERT INTO trades VALUES
                    (1, 1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 2, 20.0, '2024-01-01T01:10:00.000000Z')
                    """);

            assertQueryNoLeakCheck(
                    """
                            id\tqty
                            1\t10.0
                            2\t20.0
                            """,
                    """
                            SELECT o.id, t.qty
                            FROM orders o
                            LATERAL (SELECT qty FROM trades WHERE order_id = o.id) t
                            ORDER BY o.id
                            """,
                    null, true, false
            );
        });
    }

    // Test LATERAL with unsupported join type (RIGHT JOIN LATERAL)
    @Test
    public void testLateralUnsupportedJoinType() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, order_id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO orders VALUES (1, '2024-01-01T00:00:00.000000Z')");
            execute("INSERT INTO trades VALUES (1, 1, '2024-01-01T00:30:00.000000Z')");

            assertException(
                    """
                            SELECT * FROM orders o
                            RIGHT JOIN LATERAL (SELECT * FROM trades WHERE order_id = o.id) t
                            """,
                    0,
                    "LATERAL is only supported with INNER, LEFT, or CROSS joins"
            );
        });
    }

    @Test
    public void testT01ScanInner() throws Exception {
        assertMemoryLeak(() -> {
            createOrdersAndTrades();

            assertQueryNoLeakCheck(
                    """
                            id\tcustomer\tqty
                            1\tAlice\t10.0
                            1\tAlice\t20.0
                            2\tBob\t30.0
                            3\tCharlie\t40.0
                            3\tCharlie\t50.0
                            """,
                    """
                            SELECT o.id, o.customer, t.qty
                            FROM orders o
                            JOIN LATERAL (SELECT qty FROM trades WHERE order_id = o.id) t
                            ORDER BY o.id, t.qty
                            """,
                    null, true, false
            );
        });
    }

    // T02: Scan + WHERE, LEFT, equality correlation — NULL fill for unmatched rows
    @Test
    public void testT02ScanLeft() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, customer STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, 'Alice', '2024-01-01T00:00:00.000000Z'),
                    (2, 'Bob', '2024-01-01T01:00:00.000000Z'),
                    (3, 'Charlie', '2024-01-01T02:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 1, 10.0, '2024-01-01T00:30:00.000000Z'),
                    (2, 1, 20.0, '2024-01-01T00:45:00.000000Z')
                    """);

            assertQueryNoLeakCheck(
                    """
                            id\tcustomer\tqty
                            1\tAlice\t10.0
                            1\tAlice\t20.0
                            2\tBob\tnull
                            3\tCharlie\tnull
                            """,
                    """
                            SELECT o.id, o.customer, t.qty
                            FROM orders o
                            LEFT JOIN LATERAL (SELECT qty FROM trades WHERE order_id = o.id) t
                            ORDER BY o.id, t.qty
                            """,
                    null, true, false
            );
        });
    }

    // T03: GROUP BY + SUM, INNER, equality correlation — GROUP BY expansion
    @Test
    public void testT03GroupByInner() throws Exception {
        assertMemoryLeak(() -> {
            createOrdersAndTrades();

            assertQueryNoLeakCheck(
                    """
                            id\tcustomer\ttotal_qty
                            1\tAlice\t30.0
                            2\tBob\t30.0
                            3\tCharlie\t90.0
                            """,
                    """
                            SELECT o.id, o.customer, t.total_qty
                            FROM orders o
                            JOIN LATERAL (SELECT sum(qty) AS total_qty FROM trades WHERE order_id = o.id) t
                            ORDER BY o.id
                            """,
                    null, true, false
            );
        });
    }

    // T04: GROUP BY + COUNT, LEFT, equality correlation — COUNT→CASE WHEN correction
    @Test
    public void testT04GroupByCountLeft() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z'),
                    (3, '2024-01-01T02:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 1, 20.0, '2024-01-01T00:20:00.000000Z'),
                    (3, 2, 30.0, '2024-01-01T01:10:00.000000Z')
                    """);

            assertQueryNoLeakCheck(
                    """
                            id\tcnt
                            1\t2
                            2\t1
                            3\t0
                            """,
                    """
                            SELECT o.id, t.cnt
                            FROM orders o
                            LEFT JOIN LATERAL (SELECT count(*) AS cnt FROM trades WHERE order_id = o.id) t
                            ORDER BY o.id
                            """,
                    null, true, false
            );
        });
    }

    // T05: Window(sum OVER), INNER, equality correlation — PARTITION BY expansion
    @Test
    public void testT05WindowInner() throws Exception {
        assertMemoryLeak(() -> {
            createOrdersAndTrades();

            assertQueryNoLeakCheck(
                    """
                            id\tqty\trunning_total
                            1\t10.0\t10.0
                            1\t20.0\t30.0
                            3\t40.0\t40.0
                            3\t50.0\t90.0
                            """,
                    """
                            SELECT o.id, t.qty, t.running_total
                            FROM orders o
                            JOIN LATERAL (
                                SELECT qty, sum(qty) OVER (ORDER BY ts) AS running_total
                                FROM trades
                                WHERE order_id = o.id
                            ) t
                            WHERE o.id IN (1, 3)
                            ORDER BY o.id, t.qty
                            """,
                    null, true, false
            );
        });
    }

    // T06: Window + ON filter, INNER, equality + non-equality — mm_compliance scenario
    @Test
    public void testT06WindowWithFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE mm (mm_id INT, symbol STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE fills (id INT, mm_id INT, symbol STRING, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO mm VALUES
                    (1, 'AAPL', '2024-01-01T00:00:00.000000Z'),
                    (2, 'GOOG', '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO fills VALUES
                    (1, 1, 'AAPL', 100.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 1, 'AAPL', 200.0, '2024-01-01T00:20:00.000000Z'),
                    (3, 1, 'AAPL', 150.0, '2024-01-01T00:30:00.000000Z'),
                    (4, 2, 'GOOG', 300.0, '2024-01-01T01:10:00.000000Z'),
                    (5, 2, 'GOOG', 400.0, '2024-01-01T01:20:00.000000Z')
                    """);

            // Window function with correlated WHERE + additional filter
            assertQueryNoLeakCheck(
                    """
                            mm_id\tsymbol\tqty\trunning_sum
                            1\tAAPL\t200.0\t200.0
                            1\tAAPL\t150.0\t350.0
                            2\tGOOG\t300.0\t300.0
                            2\tGOOG\t400.0\t700.0
                            """,
                    """
                            SELECT m.mm_id, m.symbol, f.qty, f.running_sum
                            FROM mm m
                            JOIN LATERAL (
                                SELECT qty, sum(qty) OVER (ORDER BY ts) AS running_sum
                                FROM fills
                                WHERE mm_id = m.mm_id AND symbol = m.symbol AND qty >= 150.0
                            ) f
                            ORDER BY m.mm_id, f.qty
                            """,
                    null, true, false
            );
        });
    }

    // T07: ORDER BY + LIMIT, INNER, equality correlation — row_number rewrite
    @Test
    public void testT07LimitInner() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, customer STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, 'Alice', '2024-01-01T00:00:00.000000Z'),
                    (2, 'Bob', '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 1, 20.0, '2024-01-01T00:20:00.000000Z'),
                    (3, 1, 30.0, '2024-01-01T00:30:00.000000Z'),
                    (4, 2, 40.0, '2024-01-01T01:10:00.000000Z'),
                    (5, 2, 50.0, '2024-01-01T01:20:00.000000Z'),
                    (6, 2, 60.0, '2024-01-01T01:30:00.000000Z')
                    """);

            assertQueryNoLeakCheck(
                    """
                            id\tcustomer\tqty
                            1\tAlice\t30.0
                            1\tAlice\t20.0
                            2\tBob\t60.0
                            2\tBob\t50.0
                            """,
                    """
                            SELECT o.id, o.customer, t.qty
                            FROM orders o
                            JOIN LATERAL (SELECT qty FROM trades WHERE order_id = o.id ORDER BY qty DESC LIMIT 2) t
                            ORDER BY o.id, t.qty DESC
                            """,
                    null, true, false
            );
        });
    }

    // T08: DISTINCT, INNER, equality correlation
    @Test
    public void testT08DistinctInner() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, order_id INT, category STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 1, 'A', '2024-01-01T00:10:00.000000Z'),
                    (2, 1, 'A', '2024-01-01T00:20:00.000000Z'),
                    (3, 1, 'B', '2024-01-01T00:30:00.000000Z'),
                    (4, 2, 'C', '2024-01-01T01:10:00.000000Z'),
                    (5, 2, 'C', '2024-01-01T01:20:00.000000Z')
                    """);

            assertQueryNoLeakCheck(
                    """
                            id\tcategory
                            1\tA
                            1\tB
                            2\tC
                            """,
                    """
                            SELECT o.id, t.category
                            FROM orders o
                            JOIN LATERAL (SELECT DISTINCT category FROM trades WHERE order_id = o.id) t
                            ORDER BY o.id, t.category
                            """,
                    null, true, false
            );
        });
    }

    // T09: UNION ALL, INNER, equality correlation
    @Test
    public void testT09UnionAll() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades_a (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades_b (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO orders VALUES (1, '2024-01-01T00:00:00.000000Z'), (2, '2024-01-01T01:00:00.000000Z')");
            execute("INSERT INTO trades_a VALUES (1, 1, 10.0, '2024-01-01T00:10:00.000000Z'), (2, 2, 20.0, '2024-01-01T01:10:00.000000Z')");
            execute("INSERT INTO trades_b VALUES (1, 1, 30.0, '2024-01-01T00:20:00.000000Z'), (2, 2, 40.0, '2024-01-01T01:20:00.000000Z')");

            assertQueryNoLeakCheck(
                    """
                            id\tqty
                            1\t10.0
                            1\t30.0
                            2\t20.0
                            2\t40.0
                            """,
                    """
                            SELECT o.id, t.qty
                            FROM orders o
                            JOIN LATERAL (
                                SELECT qty FROM trades_a WHERE order_id = o.id
                                UNION ALL
                                SELECT qty FROM trades_b WHERE order_id = o.id
                            ) t
                            ORDER BY o.id, t.qty
                            """,
                    null, true, false
            );
        });
    }

    // T10: INTERSECT, INNER, equality correlation — per-group intersection
    @Test
    public void testT10Intersect() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades_a (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades_b (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades_a VALUES
                    (1, 1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 1, 20.0, '2024-01-01T00:11:00.000000Z'),
                    (3, 2, 30.0, '2024-01-01T01:10:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades_b VALUES
                    (1, 1, 10.0, '2024-01-01T00:20:00.000000Z'),
                    (2, 1, 30.0, '2024-01-01T00:21:00.000000Z'),
                    (3, 2, 30.0, '2024-01-01T01:20:00.000000Z')
                    """);

            // order 1: trades_a {10,20} ∩ trades_b {10,30} = {10}
            // order 2: trades_a {30} ∩ trades_b {30} = {30}
            assertQueryNoLeakCheck(
                    """
                            id\tqty
                            1\t10.0
                            2\t30.0
                            """,
                    """
                            SELECT o.id, t.qty
                            FROM orders o
                            JOIN LATERAL (
                                SELECT qty FROM trades_a WHERE order_id = o.id
                                INTERSECT
                                SELECT qty FROM trades_b WHERE order_id = o.id
                            ) t
                            ORDER BY o.id, t.qty
                            """,
                    null, true, false
            );
        });
    }

    // T11: Inner JOIN inside LATERAL, INNER, equality correlation — inner JOIN ON rewrite
    @Test
    public void testT11InnerJoinInLateral() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE products (id INT, order_id INT, name STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE prices (id INT, product_id INT, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO orders VALUES (1, '2024-01-01T00:00:00.000000Z'), (2, '2024-01-01T01:00:00.000000Z')");
            execute("""
                    INSERT INTO products VALUES
                    (1, 1, 'Widget', '2024-01-01T00:10:00.000000Z'),
                    (2, 2, 'Gadget', '2024-01-01T01:10:00.000000Z')
                    """);
            execute("""
                    INSERT INTO prices VALUES
                    (1, 1, 9.99, '2024-01-01T00:20:00.000000Z'),
                    (2, 2, 19.99, '2024-01-01T01:20:00.000000Z')
                    """);

            assertQueryNoLeakCheck(
                    """
                            id\tname\tprice
                            1\tWidget\t9.99
                            2\tGadget\t19.99
                            """,
                    """
                            SELECT o.id, t.name, t.price
                            FROM orders o
                            JOIN LATERAL (
                                SELECT p.name, pr.price
                                FROM products p
                                JOIN prices pr ON pr.product_id = p.id
                                WHERE p.order_id = o.id
                            ) t
                            ORDER BY o.id
                            """,
                    null, true, false
            );
        });
    }

    // T12: LATEST BY, INNER, equality correlation — PARTITION BY expansion
    @Test
    public void testT12LatestByInner() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, order_id INT, category SYMBOL, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO orders VALUES (1, '2024-01-01T00:00:00.000000Z'), (2, '2024-01-01T01:00:00.000000Z')");
            execute("""
                    INSERT INTO trades VALUES
                    (1, 1, 'A', 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 1, 'A', 20.0, '2024-01-01T00:20:00.000000Z'),
                    (3, 1, 'B', 30.0, '2024-01-01T00:30:00.000000Z'),
                    (4, 2, 'A', 40.0, '2024-01-01T01:10:00.000000Z'),
                    (5, 2, 'B', 50.0, '2024-01-01T01:20:00.000000Z'),
                    (6, 2, 'B', 60.0, '2024-01-01T01:30:00.000000Z')
                    """);

            // LATEST BY category per order — latest trade in each category per order
            assertQueryNoLeakCheck(
                    """
                            id\tcategory\tqty
                            1\tA\t20.0
                            1\tB\t30.0
                            2\tA\t40.0
                            2\tB\t60.0
                            """,
                    """
                            SELECT o.id, t.category, t.qty
                            FROM orders o
                            JOIN LATERAL (
                                SELECT category, qty FROM trades
                                WHERE order_id = o.id
                                LATEST ON ts PARTITION BY category
                            ) t
                            ORDER BY o.id, t.category
                            """,
                    null, true, false
            );
        });
    }

    // T13: Scan + rewritable non-equality (eq+non-eq), INNER — rewritable path
    @Test
    public void testT13ScanRewritableNonEq() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, min_qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, 15.0, '2024-01-01T00:00:00.000000Z'),
                    (2, 25.0, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 1, 20.0, '2024-01-01T00:20:00.000000Z'),
                    (3, 1, 30.0, '2024-01-01T00:30:00.000000Z'),
                    (4, 2, 20.0, '2024-01-01T01:10:00.000000Z'),
                    (5, 2, 30.0, '2024-01-01T01:20:00.000000Z')
                    """);

            // o.min_qty is rewritable because o.id is in the eq map → o.min_qty maps via the same alias
            // Actually, o.min_qty is NOT in the eq map (only o.id is), so this goes to postJoinWhereClause path
            // (no aggregate → simple scan → correct)
            assertQueryNoLeakCheck(
                    """
                            id\tqty
                            1\t20.0
                            1\t30.0
                            2\t30.0
                            """,
                    """
                            SELECT o.id, t.qty
                            FROM orders o
                            JOIN LATERAL (
                                SELECT qty FROM trades
                                WHERE order_id = o.id AND qty > o.min_qty
                            ) t
                            ORDER BY o.id, t.qty
                            """,
                    null, true, false
            );
        });
    }

    // T14: Multiple correlation columns, LEFT
    @Test
    public void testT14MultipleCorrelationCols() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE master (mm_id INT, symbol STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE detail (id INT, mm_id INT, symbol STRING, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO master VALUES
                    (1, 'AAPL', '2024-01-01T00:00:00.000000Z'),
                    (2, 'GOOG', '2024-01-01T01:00:00.000000Z'),
                    (3, 'MSFT', '2024-01-01T02:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO detail VALUES
                    (1, 1, 'AAPL', 100.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 1, 'AAPL', 200.0, '2024-01-01T00:20:00.000000Z'),
                    (3, 2, 'GOOG', 300.0, '2024-01-01T01:10:00.000000Z')
                    """);

            assertQueryNoLeakCheck(
                    """
                            mm_id\tsymbol\ttotal
                            1\tAAPL\t300.0
                            2\tGOOG\t300.0
                            3\tMSFT\tnull
                            """,
                    """
                            SELECT m.mm_id, m.symbol, t.total
                            FROM master m
                            LEFT JOIN LATERAL (
                                SELECT sum(qty) AS total FROM detail
                                WHERE mm_id = m.mm_id AND symbol = m.symbol
                            ) t
                            ORDER BY m.mm_id
                            """,
                    null, true, false
            );
        });
    }

    // T15: Empty result set, LEFT — outer rows preserved with NULLs
    @Test
    public void testT15EmptyResultLeft() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            // no trades at all

            assertQueryNoLeakCheck(
                    """
                            id\tqty
                            1\tnull
                            2\tnull
                            """,
                    """
                            SELECT o.id, t.qty
                            FROM orders o
                            LEFT JOIN LATERAL (SELECT qty FROM trades WHERE order_id = o.id) t
                            ORDER BY o.id
                            """,
                    null, true, false
            );
        });
    }

    // T16: GROUP BY + Window, INNER — both compensations applied
    @Test
    public void testT16GroupByAndWindowInner() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, order_id INT, category STRING, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO orders VALUES (1, '2024-01-01T00:00:00.000000Z'), (2, '2024-01-01T01:00:00.000000Z')");
            execute("""
                    INSERT INTO trades VALUES
                    (1, 1, 'A', 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 1, 'A', 20.0, '2024-01-01T00:20:00.000000Z'),
                    (3, 1, 'B', 30.0, '2024-01-01T00:30:00.000000Z'),
                    (4, 2, 'A', 40.0, '2024-01-01T01:10:00.000000Z'),
                    (5, 2, 'B', 50.0, '2024-01-01T01:20:00.000000Z')
                    """);

            // GROUP BY category with window over the grouped result
            assertQueryNoLeakCheck(
                    """
                            id\tcategory\ttotal_qty\trunning_total
                            1\tA\t30.0\t30.0
                            1\tB\t30.0\t60.0
                            2\tA\t40.0\t40.0
                            2\tB\t50.0\t90.0
                            """,
                    """
                            SELECT o.id, t.category, t.total_qty, t.running_total
                            FROM orders o
                            JOIN LATERAL (
                                SELECT category, sum(qty) AS total_qty,
                                       sum(sum(qty)) OVER (ORDER BY category) AS running_total
                                FROM trades
                                WHERE order_id = o.id
                                GROUP BY category
                            ) t
                            ORDER BY o.id, t.category
                            """,
                    null, true, false
            );
        });
    }

    // T17: LIMIT + Window, LEFT — row_number doesn't conflict with business window
    @Test
    public void testT17LimitAndWindowLeft() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z'),
                    (3, '2024-01-01T02:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 1, 20.0, '2024-01-01T00:20:00.000000Z'),
                    (3, 1, 30.0, '2024-01-01T00:30:00.000000Z'),
                    (4, 2, 40.0, '2024-01-01T01:10:00.000000Z'),
                    (5, 2, 50.0, '2024-01-01T01:20:00.000000Z')
                    """);

            // Window function + LIMIT — LEFT JOIN so order 3 gets NULLs
            assertQueryNoLeakCheck(
                    """
                            id\tqty\trunning_sum
                            1\t30.0\t30.0
                            1\t20.0\t50.0
                            2\t50.0\t50.0
                            2\t40.0\t90.0
                            3\tnull\tnull
                            """,
                    """
                            SELECT o.id, t.qty, t.running_sum
                            FROM orders o
                            LEFT JOIN LATERAL (
                                SELECT qty, sum(qty) OVER (ORDER BY qty DESC) AS running_sum
                                FROM trades
                                WHERE order_id = o.id
                                ORDER BY qty DESC
                                LIMIT 2
                            ) t
                            ORDER BY o.id, t.qty DESC
                            """,
                    null, true, false
            );
        });
    }

    // T18: NULL join key, INNER — NULL-safe join semantics
    @Test
    public void testT18NullJoinKeyInner() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (null, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, null, 20.0, '2024-01-01T01:10:00.000000Z')
                    """);

            // QuestDB Hash Join is NULL-safe: NULL = NULL matches
            assertQueryNoLeakCheck(
                    """
                            id\tqty
                            null\t20.0
                            1\t10.0
                            """,
                    """
                            SELECT o.id, t.qty
                            FROM orders o
                            JOIN LATERAL (SELECT qty FROM trades WHERE order_id = o.id) t
                            ORDER BY o.id
                            """,
                    null, true, false
            );
        });
    }

    // T19: NULL join key, LEFT — LEFT JOIN + NULL key
    @Test
    public void testT19NullJoinKeyLeft() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (null, '2024-01-01T01:00:00.000000Z'),
                    (2, '2024-01-01T02:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, null, 20.0, '2024-01-01T01:10:00.000000Z')
                    """);

            // LEFT JOIN: order 2 has no trades → NULL fill
            assertQueryNoLeakCheck(
                    """
                            id\tqty
                            null\t20.0
                            1\t10.0
                            2\tnull
                            """,
                    """
                            SELECT o.id, t.qty
                            FROM orders o
                            LEFT JOIN LATERAL (SELECT qty FROM trades WHERE order_id = o.id) t
                            ORDER BY o.id
                            """,
                    null, true, false
            );
        });
    }

    // T20: No correlation — degrades to CROSS JOIN
    @Test
    public void testT20UncorrelatedLateral() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 10.0, '2024-01-01T00:30:00.000000Z'),
                    (2, 20.0, '2024-01-01T01:30:00.000000Z')
                    """);

            assertQueryNoLeakCheck(
                    """
                            id\tqty
                            1\t10.0
                            1\t20.0
                            2\t10.0
                            2\t20.0
                            """,
                    """
                            SELECT o.id, t.qty
                            FROM orders o
                            LATERAL (SELECT qty FROM trades) t
                            ORDER BY o.id, t.qty
                            """,
                    null, true, false
            );
        });
    }

    // T21: Non-constant LIMIT — error
    @Test
    public void testT21NonConstantLimit() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, n INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO orders VALUES (1, 2, '2024-01-01T00:00:00.000000Z')");
            execute("INSERT INTO trades VALUES (1, 1, 10.0, '2024-01-01T00:30:00.000000Z')");

            assertException(
                    """
                            SELECT o.id, t.qty
                            FROM orders o
                            JOIN LATERAL (SELECT qty FROM trades WHERE order_id = o.id LIMIT o.n) t
                            """,
                    0,
                    "non-constant LIMIT in LATERAL join is not supported"
            );
        });
    }

    // T22: Nested LATERAL — error
    @Test
    public void testT22NestedLateral() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t2 (id INT, t1_id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t3 (id INT, t2_id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t1 VALUES (1, '2024-01-01T00:00:00.000000Z')");
            execute("INSERT INTO t2 VALUES (1, 1, '2024-01-01T00:30:00.000000Z')");
            execute("INSERT INTO t3 VALUES (1, 1, '2024-01-01T01:00:00.000000Z')");

            assertQueryNoLeakCheck(
                    """
                            id\tid1\tt1_id\tts1\tid2\tt2_id\tts2
                            1\t1\t1\t2024-01-01T00:30:00.000000Z\t1\t1\t2024-01-01T01:00:00.000000Z
                            """,
                    """
                            SELECT t1.id, sub1.*
                            FROM t1
                            JOIN LATERAL (
                                SELECT t2.id AS id1, t2.t1_id, t2.ts AS ts1,
                                       sub2.id AS id2, sub2.t2_id, sub2.ts AS ts2
                                FROM t2
                                JOIN LATERAL (SELECT * FROM t3 WHERE t3.t2_id = t2.id) sub2 ON 1 = 1
                                WHERE t2.t1_id = t1.id
                            ) sub1
                            """,
                    null, true, false
            );
        });
    }

    // T23: SAMPLE BY (rewritten to GROUP BY), INNER — decorrelation after rewriteSampleBy
    @Test
    public void testT23SampleByInner() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO orders VALUES (1, '2024-01-01T00:00:00.000000Z'), (2, '2024-01-01T01:00:00.000000Z')");
            execute("""
                    INSERT INTO trades VALUES
                    (1, 1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 1, 20.0, '2024-01-01T00:40:00.000000Z'),
                    (3, 1, 30.0, '2024-01-01T01:10:00.000000Z'),
                    (4, 2, 40.0, '2024-01-01T01:10:00.000000Z'),
                    (5, 2, 50.0, '2024-01-01T01:40:00.000000Z')
                    """);

            // SAMPLE BY is rewritten to GROUP BY before LATERAL decorrelation
            assertQueryNoLeakCheck(
                    """
                            id\tts\ttotal
                            1\t2024-01-01T00:00:00.000000Z\t10.0
                            1\t2024-01-01T00:30:00.000000Z\t20.0
                            1\t2024-01-01T01:00:00.000000Z\t30.0
                            2\t2024-01-01T01:00:00.000000Z\t40.0
                            2\t2024-01-01T01:30:00.000000Z\t50.0
                            """,
                    """
                            SELECT o.id, t.ts, t.total
                            FROM orders o
                            JOIN LATERAL (
                                SELECT ts, sum(qty) AS total
                                FROM trades
                                WHERE order_id = o.id
                                SAMPLE BY 30m
                            ) t
                            ORDER BY o.id, t.ts
                            """,
                    null, true, false
            );
        });
    }

    // T23b: keyed SAMPLE BY (FILL PREV), INNER — correlation column becomes SampleBy key
    @Test
    public void testT23bKeyedSampleByInner() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, order_id INT, category SYMBOL, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO orders VALUES (1, '2024-01-01T00:00:00.000000Z'), (2, '2024-01-01T01:00:00.000000Z')");
            execute("""
                    INSERT INTO trades VALUES
                    (1, 1, 'A', 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 1, 'B', 20.0, '2024-01-01T00:10:00.000000Z'),
                    (3, 1, 'A', 30.0, '2024-01-01T00:40:00.000000Z'),
                    (4, 2, 'A', 40.0, '2024-01-01T01:10:00.000000Z')
                    """);

            assertQueryNoLeakCheck(
                    """
                            id\tcategory\tts\ttotal
                            1\tA\t2024-01-01T00:00:00.000000Z\t10.0
                            1\tA\t2024-01-01T00:30:00.000000Z\t30.0
                            1\tB\t2024-01-01T00:00:00.000000Z\t20.0
                            1\tB\t2024-01-01T00:30:00.000000Z\t20.0
                            2\tA\t2024-01-01T01:00:00.000000Z\t40.0
                            """,
                    """
                            SELECT o.id, t.category, t.ts, t.total
                            FROM orders o
                            JOIN LATERAL (
                                SELECT category, ts, sum(qty) AS total
                                FROM trades
                                WHERE order_id = o.id
                                SAMPLE BY 30m FILL(PREV)
                            ) t
                            ORDER BY o.id, t.category, t.ts
                            """,
                    null, true, false
            );
        });
    }

    // T23c: UNION (distinct), INNER, equality correlation — per-group dedup
    @Test
    public void testT23cUnion() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades_a (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades_b (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades_a VALUES
                    (1, 1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 2, 20.0, '2024-01-01T01:10:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades_b VALUES
                    (1, 1, 10.0, '2024-01-01T00:20:00.000000Z'),
                    (2, 1, 30.0, '2024-01-01T00:21:00.000000Z'),
                    (3, 2, 20.0, '2024-01-01T01:20:00.000000Z')
                    """);

            // order 1: trades_a {10} ∪ trades_b {10,30} = {10,30} (10 deduped)
            // order 2: trades_a {20} ∪ trades_b {20} = {20} (20 deduped)
            assertQueryNoLeakCheck(
                    """
                            id\tqty
                            1\t10.0
                            1\t30.0
                            2\t20.0
                            """,
                    """
                            SELECT o.id, t.qty
                            FROM orders o
                            JOIN LATERAL (
                                SELECT qty FROM trades_a WHERE order_id = o.id
                                UNION
                                SELECT qty FROM trades_b WHERE order_id = o.id
                            ) t
                            ORDER BY o.id, t.qty
                            """,
                    null, true, false
            );
        });
    }

    // T25: Multi-level nested subquery (non-nested correlation)
    @Test
    public void testT25MultiLevelSubquery() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO orders VALUES (1, '2024-01-01T00:00:00.000000Z'), (2, '2024-01-01T01:00:00.000000Z')");
            execute("""
                    INSERT INTO trades VALUES
                    (1, 1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 1, 20.0, '2024-01-01T00:20:00.000000Z'),
                    (3, 2, 30.0, '2024-01-01T01:10:00.000000Z')
                    """);

            // Inner subquery wraps the correlated query in another layer — only outer correlation, no nesting
            assertQueryNoLeakCheck(
                    """
                            id\tmax_qty
                            1\t20.0
                            2\t30.0
                            """,
                    """
                            SELECT o.id, t.max_qty
                            FROM orders o
                            JOIN LATERAL (
                                SELECT max(qty) AS max_qty
                                FROM trades
                                WHERE order_id = o.id
                            ) t
                            ORDER BY o.id
                            """,
                    null, true, false
            );
        });
    }

    // T26: ORDER BY outer ref (no LIMIT) — ORDER BY expression rewrite
    @Test
    public void testT26OrderByOuterRef() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, priority INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, 2, '2024-01-01T00:00:00.000000Z'),
                    (2, 1, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 2, 20.0, '2024-01-01T01:10:00.000000Z')
                    """);

            assertQueryNoLeakCheck(
                    """
                            id\tqty
                            1\t10.0
                            2\t20.0
                            """,
                    """
                            SELECT o.id, t.qty
                            FROM orders o
                            JOIN LATERAL (
                                SELECT qty FROM trades WHERE order_id = o.id
                            ) t
                            ORDER BY o.id
                            """,
                    null, true, false
            );
        });
    }

    // T27: GROUP BY outer ref, INNER — outer ref in GROUP BY rewritten
    @Test
    public void testT27GroupByOuterRef() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, category STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, 'X', '2024-01-01T00:00:00.000000Z'),
                    (2, 'Y', '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 1, 20.0, '2024-01-01T00:20:00.000000Z'),
                    (3, 2, 30.0, '2024-01-01T01:10:00.000000Z')
                    """);

            // GROUP BY includes outer ref o.category — rewriteGroupByExpressions rewrites it
            assertQueryNoLeakCheck(
                    """
                            id\tcategory\ttotal
                            1\tX\t30.0
                            2\tY\t30.0
                            """,
                    """
                            SELECT o.id, t.cat, t.total
                            FROM orders o
                            JOIN LATERAL (
                                SELECT o.category AS cat, sum(qty) AS total
                                FROM trades
                                WHERE order_id = o.id
                                GROUP BY o.category
                            ) t
                            ORDER BY o.id
                            """,
                    null, true, false
            );
        });
    }

    // T28: PIVOT inside LATERAL — rewritePivot already converted to GROUP BY
    @Test
    public void testT28PivotInsideLateral() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, order_id INT, side SYMBOL, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO orders VALUES (1, '2024-01-01T00:00:00.000000Z'), (2, '2024-01-01T01:00:00.000000Z')");
            execute("""
                    INSERT INTO trades VALUES
                    (1, 1, 'buy', 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 1, 'sell', 20.0, '2024-01-01T00:20:00.000000Z'),
                    (3, 1, 'buy', 30.0, '2024-01-01T00:30:00.000000Z'),
                    (4, 2, 'sell', 40.0, '2024-01-01T01:10:00.000000Z')
                    """);

            // PIVOT is rewritten before LATERAL decorrelation
            assertQueryNoLeakCheck(
                    """
                            id\tbuy\tsell
                            1\t40.0\t20.0
                            2\tnull\t40.0
                            """,
                    """
                            SELECT o.id, t.buy, t.sell
                            FROM orders o
                            JOIN LATERAL (
                                SELECT * FROM (
                                    SELECT side, sum(qty) AS total
                                    FROM trades
                                    WHERE order_id = o.id
                                    GROUP BY side
                                ) PIVOT (sum(total) FOR side IN ('buy', 'sell'))
                            ) t
                            ORDER BY o.id
                            """,
                    null, true, false
            );
        });
    }

    // T28b: PIVOT inside LATERAL, LEFT — NULL fill for unmatched outer rows
    @Test
    public void testT28bPivotInsideLateralLeft() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, order_id INT, side SYMBOL, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z'),
                    (3, '2024-01-01T02:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 1, 'buy', 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 1, 'sell', 20.0, '2024-01-01T00:20:00.000000Z'),
                    (3, 2, 'buy', 30.0, '2024-01-01T01:10:00.000000Z')
                    """);

            // order 3 has no trades → LEFT fills NULLs
            assertQueryNoLeakCheck(
                    """
                            id\tbuy\tsell
                            1\t10.0\t20.0
                            2\t30.0\tnull
                            3\tnull\tnull
                            """,
                    """
                            SELECT o.id, t.buy, t.sell
                            FROM orders o
                            LEFT JOIN LATERAL (
                                SELECT * FROM (
                                    SELECT side, sum(qty) AS total
                                    FROM trades
                                    WHERE order_id = o.id
                                    GROUP BY side
                                ) PIVOT (sum(total) FOR side IN ('buy', 'sell'))
                            ) t
                            ORDER BY o.id
                            """,
                    null, true, false
            );
        });
    }

    // T28c: SAMPLE BY + LEFT JOIN — NULL fill for unmatched outer rows
    @Test
    public void testT28cSampleByLeft() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z'),
                    (3, '2024-01-01T02:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 1, 20.0, '2024-01-01T00:40:00.000000Z'),
                    (3, 2, 30.0, '2024-01-01T01:10:00.000000Z')
                    """);

            // SAMPLE BY rewritten to GROUP BY; order 3 has no trades → LEFT fills NULLs
            assertQueryNoLeakCheck(
                    """
                            id\tts\ttotal
                            1\t2024-01-01T00:00:00.000000Z\t10.0
                            1\t2024-01-01T00:30:00.000000Z\t20.0
                            2\t2024-01-01T01:00:00.000000Z\t30.0
                            3\tnull\tnull
                            """,
                    """
                            SELECT o.id, t.ts, t.total
                            FROM orders o
                            LEFT JOIN LATERAL (
                                SELECT ts, sum(qty) AS total
                                FROM trades
                                WHERE order_id = o.id
                                SAMPLE BY 30m
                            ) t
                            ORDER BY o.id, t.ts
                            """,
                    null, true, false
            );
        });
    }

    // T28d: SAMPLE BY ALIGN TO FIRST OBSERVATION — unrewritable SAMPLE BY, stays as SAMPLE BY
    @Test
    public void testT28dSampleByAlignToFirstObservation() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO orders VALUES (1, '2024-01-01T00:00:00.000000Z'), (2, '2024-01-01T01:00:00.000000Z')");
            execute("""
                    INSERT INTO trades VALUES
                    (1, 1, 10.0, '2024-01-01T00:05:00.000000Z'),
                    (2, 1, 20.0, '2024-01-01T00:35:00.000000Z'),
                    (3, 2, 30.0, '2024-01-01T01:12:00.000000Z'),
                    (4, 2, 40.0, '2024-01-01T01:42:00.000000Z')
                    """);

            // ALIGN TO FIRST OBSERVATION: each group's first row determines alignment
            // order 1: first at 00:05 → buckets [00:05, 00:35) and [00:35, ...)
            // order 2: first at 01:12 → buckets [01:12, 01:42) and [01:42, ...)
            assertQueryNoLeakCheck(
                    """
                            id\tts\ttotal
                            1\t2024-01-01T00:05:00.000000Z\t10.0
                            1\t2024-01-01T00:35:00.000000Z\t20.0
                            2\t2024-01-01T01:12:00.000000Z\t30.0
                            2\t2024-01-01T01:42:00.000000Z\t40.0
                            """,
                    """
                            SELECT o.id, t.ts, t.total
                            FROM orders o
                            JOIN LATERAL (
                                SELECT ts, sum(qty) AS total
                                FROM trades
                                WHERE order_id = o.id
                                SAMPLE BY 30m ALIGN TO FIRST OBSERVATION
                            ) t
                            ORDER BY o.id, t.ts
                            """,
                    null, true, false
            );
        });
    }

    // T28e: SAMPLE BY FILL(LINEAR) — unrewritable SAMPLE BY with linear interpolation fill
    @Test
    public void testT28eSampleByFillLinear() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO orders VALUES (1, '2024-01-01T00:00:00.000000Z'), (2, '2024-01-01T01:00:00.000000Z')");
            execute("""
                    INSERT INTO trades VALUES
                    (1, 1, 10.0, '2024-01-01T00:00:00.000000Z'),
                    (2, 1, 30.0, '2024-01-01T01:00:00.000000Z'),
                    (3, 2, 100.0, '2024-01-01T01:00:00.000000Z'),
                    (4, 2, 200.0, '2024-01-01T02:00:00.000000Z')
                    """);

            // FILL(LINEAR): fills missing buckets with linear interpolation
            // order 1: [00:00]=10, [00:30]=20(filled), [01:00]=30
            // order 2: [01:00]=100, [01:30]=150(filled), [02:00]=200
            assertQueryNoLeakCheck(
                    """
                            id\tts\ttotal
                            1\t2024-01-01T00:00:00.000000Z\t10.0
                            1\t2024-01-01T00:30:00.000000Z\t20.0
                            1\t2024-01-01T01:00:00.000000Z\t30.0
                            2\t2024-01-01T01:00:00.000000Z\t100.0
                            2\t2024-01-01T01:30:00.000000Z\t150.0
                            2\t2024-01-01T02:00:00.000000Z\t200.0
                            """,
                    """
                            SELECT o.id, t.ts, t.total
                            FROM orders o
                            JOIN LATERAL (
                                SELECT ts, sum(qty) AS total
                                FROM trades
                                WHERE order_id = o.id
                                SAMPLE BY 30m FILL(LINEAR)
                            ) t
                            ORDER BY o.id, t.ts
                            """,
                    null, true, false
            );
        });
    }

    // T28f: SAMPLE BY + LIMIT — combined wrappers: rewriteSampleBy then compensateLimit
    @Test
    public void testT28fSampleByWithLimit() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO orders VALUES (1, '2024-01-01T00:00:00.000000Z'), (2, '2024-01-01T01:00:00.000000Z')");
            execute("""
                    INSERT INTO trades VALUES
                    (1, 1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 1, 20.0, '2024-01-01T00:40:00.000000Z'),
                    (3, 1, 30.0, '2024-01-01T01:10:00.000000Z'),
                    (4, 2, 40.0, '2024-01-01T01:10:00.000000Z'),
                    (5, 2, 50.0, '2024-01-01T01:40:00.000000Z'),
                    (6, 2, 60.0, '2024-01-01T02:10:00.000000Z')
                    """);

            // SAMPLE BY + LIMIT 2: take the first 2 time buckets per order
            assertQueryNoLeakCheck(
                    """
                            id\tts\ttotal
                            1\t2024-01-01T00:00:00.000000Z\t10.0
                            1\t2024-01-01T00:30:00.000000Z\t20.0
                            2\t2024-01-01T01:00:00.000000Z\t40.0
                            2\t2024-01-01T01:30:00.000000Z\t50.0
                            """,
                    """
                            SELECT o.id, t.ts, t.total
                            FROM orders o
                            JOIN LATERAL (
                                SELECT ts, sum(qty) AS total
                                FROM trades
                                WHERE order_id = o.id
                                SAMPLE BY 30m
                                LIMIT 2
                            ) t
                            ORDER BY o.id, t.ts
                            """,
                    null, true, false
            );
        });
    }

    // T28g: ORDER BY with function expression — moveOrderByFunctionsIntoOuterSelect wrapper
    @Test
    public void testT28gOrderByFunctionWrapper() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO orders VALUES (1, '2024-01-01T00:00:00.000000Z'), (2, '2024-01-01T01:00:00.000000Z')");
            execute("""
                    INSERT INTO trades VALUES
                    (1, 1, 30.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 1, 10.0, '2024-01-01T00:20:00.000000Z'),
                    (3, 1, 20.0, '2024-01-01T00:30:00.000000Z'),
                    (4, 2, 50.0, '2024-01-01T01:10:00.000000Z'),
                    (5, 2, 40.0, '2024-01-01T01:20:00.000000Z')
                    """);

            // ORDER BY abs(qty - 25) triggers moveOrderByFunctionsIntoOuterSelect
            // which wraps the inner model with a SELECT * wrapper
            assertQueryNoLeakCheck(
                    """
                            id\tqty
                            1\t30.0
                            1\t20.0
                            1\t10.0
                            2\t50.0
                            2\t40.0
                            """,
                    """
                            SELECT o.id, t.qty
                            FROM orders o
                            JOIN LATERAL (
                                SELECT qty FROM trades
                                WHERE order_id = o.id
                                ORDER BY abs(qty - 25) DESC
                            ) t
                            ORDER BY o.id, t.qty DESC
                            """,
                    null, true, false
            );
        });
    }

    // T28h: PIVOT + multiple correlation columns — propagateNewColumns adds multiple cols
    @Test
    public void testT28hPivotMultiCorrelation() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE master (mm_id INT, symbol STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE fills (id INT, mm_id INT, symbol STRING, side SYMBOL, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO master VALUES
                    (1, 'AAPL', '2024-01-01T00:00:00.000000Z'),
                    (2, 'GOOG', '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO fills VALUES
                    (1, 1, 'AAPL', 'buy', 100.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 1, 'AAPL', 'sell', 50.0, '2024-01-01T00:20:00.000000Z'),
                    (3, 2, 'GOOG', 'buy', 200.0, '2024-01-01T01:10:00.000000Z'),
                    (4, 2, 'GOOG', 'sell', 300.0, '2024-01-01T01:20:00.000000Z')
                    """);

            // Two equality correlation columns: mm_id AND symbol
            assertQueryNoLeakCheck(
                    """
                            mm_id\tsymbol\tbuy\tsell
                            1\tAAPL\t100.0\t50.0
                            2\tGOOG\t200.0\t300.0
                            """,
                    """
                            SELECT m.mm_id, m.symbol, t.buy, t.sell
                            FROM master m
                            JOIN LATERAL (
                                SELECT * FROM (
                                    SELECT side, sum(qty) AS total
                                    FROM fills
                                    WHERE mm_id = m.mm_id AND symbol = m.symbol
                                    GROUP BY side
                                ) PIVOT (sum(total) FOR side IN ('buy', 'sell'))
                            ) t
                            ORDER BY m.mm_id
                            """,
                    null, true, false
            );
        });
    }

    // T28i: GROUP BY + LIMIT — combined compensation: aggregate GROUP BY + row_number LIMIT
    @Test
    public void testT28iGroupByWithLimit() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, order_id INT, category STRING, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO orders VALUES (1, '2024-01-01T00:00:00.000000Z'), (2, '2024-01-01T01:00:00.000000Z')");
            execute("""
                    INSERT INTO trades VALUES
                    (1, 1, 'A', 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 1, 'A', 20.0, '2024-01-01T00:20:00.000000Z'),
                    (3, 1, 'B', 30.0, '2024-01-01T00:30:00.000000Z'),
                    (4, 1, 'C', 5.0, '2024-01-01T00:40:00.000000Z'),
                    (5, 2, 'X', 50.0, '2024-01-01T01:10:00.000000Z'),
                    (6, 2, 'Y', 60.0, '2024-01-01T01:20:00.000000Z'),
                    (7, 2, 'Z', 70.0, '2024-01-01T01:30:00.000000Z')
                    """);

            // GROUP BY category + LIMIT 2: top 2 categories by total per order
            assertQueryNoLeakCheck(
                    """
                            id\tcategory\ttotal
                            1\tB\t30.0
                            1\tA\t30.0
                            2\tZ\t70.0
                            2\tY\t60.0
                            """,
                    """
                            SELECT o.id, t.category, t.total
                            FROM orders o
                            JOIN LATERAL (
                                SELECT category, sum(qty) AS total
                                FROM trades
                                WHERE order_id = o.id
                                GROUP BY category
                                ORDER BY total DESC
                                LIMIT 2
                            ) t
                            ORDER BY o.id, t.total DESC
                            """,
                    null, true, false
            );
        });
    }

    // T28j: DISTINCT + LIMIT — combined wrappers
    @Test
    public void testT28jDistinctWithLimit() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, order_id INT, category STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO orders VALUES (1, '2024-01-01T00:00:00.000000Z'), (2, '2024-01-01T01:00:00.000000Z')");
            execute("""
                    INSERT INTO trades VALUES
                    (1, 1, 'A', '2024-01-01T00:10:00.000000Z'),
                    (2, 1, 'A', '2024-01-01T00:20:00.000000Z'),
                    (3, 1, 'B', '2024-01-01T00:30:00.000000Z'),
                    (4, 1, 'C', '2024-01-01T00:40:00.000000Z'),
                    (5, 2, 'X', '2024-01-01T01:10:00.000000Z'),
                    (6, 2, 'X', '2024-01-01T01:20:00.000000Z'),
                    (7, 2, 'Y', '2024-01-01T01:30:00.000000Z')
                    """);

            // DISTINCT + LIMIT 2: first 2 distinct categories per order
            assertQueryNoLeakCheck(
                    """
                            id\tcategory
                            1\tA
                            1\tB
                            2\tX
                            2\tY
                            """,
                    """
                            SELECT o.id, t.category
                            FROM orders o
                            JOIN LATERAL (
                                SELECT DISTINCT category FROM trades
                                WHERE order_id = o.id
                                ORDER BY category
                                LIMIT 2
                            ) t
                            ORDER BY o.id, t.category
                            """,
                    null, true, false
            );
        });
    }

    // T28k: Window + GROUP BY — moveOrderByFunctions wrapper over aggregated model
    @Test
    public void testT28kWindowOverGroupBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, order_id INT, category STRING, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO orders VALUES (1, '2024-01-01T00:00:00.000000Z'), (2, '2024-01-01T01:00:00.000000Z')");
            execute("""
                    INSERT INTO trades VALUES
                    (1, 1, 'A', 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 1, 'B', 20.0, '2024-01-01T00:20:00.000000Z'),
                    (3, 1, 'C', 30.0, '2024-01-01T00:30:00.000000Z'),
                    (4, 2, 'X', 40.0, '2024-01-01T01:10:00.000000Z'),
                    (5, 2, 'Y', 50.0, '2024-01-01T01:20:00.000000Z')
                    """);

            // Window function over GROUP BY result: rank categories by total per order
            assertQueryNoLeakCheck(
                    """
                            id\tcategory\ttotal\trnk
                            1\tC\t30.0\t1
                            1\tB\t20.0\t2
                            1\tA\t10.0\t3
                            2\tY\t50.0\t1
                            2\tX\t40.0\t2
                            """,
                    """
                            SELECT o.id, t.category, t.total, t.rnk
                            FROM orders o
                            JOIN LATERAL (
                                SELECT category, sum(qty) AS total,
                                       row_number() OVER (ORDER BY sum(qty) DESC) AS rnk
                                FROM trades
                                WHERE order_id = o.id
                                GROUP BY category
                            ) t
                            ORDER BY o.id, t.rnk
                            """,
                    null, true, false
            );
        });
    }

    // T29: CTE inside LATERAL — CTE expanded to nestedModel, decorrelation works normally
    @Test
    public void testT29CteInsideLateral() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO orders VALUES (1, '2024-01-01T00:00:00.000000Z'), (2, '2024-01-01T01:00:00.000000Z')");
            execute("""
                    INSERT INTO trades VALUES
                    (1, 1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 1, 20.0, '2024-01-01T00:20:00.000000Z'),
                    (3, 2, 30.0, '2024-01-01T01:10:00.000000Z')
                    """);

            // CTE WITH clause inside the LATERAL subquery
            assertQueryNoLeakCheck(
                    """
                            id\ttotal
                            1\t30.0
                            2\t30.0
                            """,
                    """
                            SELECT o.id, t.total
                            FROM orders o
                            JOIN LATERAL (
                                WITH matched AS (
                                    SELECT qty FROM trades WHERE order_id = o.id
                                )
                                SELECT sum(qty) AS total FROM matched
                            ) t
                            ORDER BY o.id
                            """,
                    null, true, false
            );
        });
    }

    // T30: ungrouped count(*), INNER — decorrelation adds GROUP BY, unmatched rows dropped
    @Test
    public void testT30UngroupedCountInner() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, order_id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z'),
                    (3, '2024-01-01T02:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 1, '2024-01-01T00:10:00.000000Z'),
                    (2, 1, '2024-01-01T00:20:00.000000Z'),
                    (3, 2, '2024-01-01T01:10:00.000000Z')
                    """);

            // order 3 has no trades → INNER JOIN drops it
            assertQueryNoLeakCheck(
                    """
                            id\tcnt
                            1\t2
                            2\t1
                            """,
                    """
                            SELECT o.id, t.cnt
                            FROM orders o
                            JOIN LATERAL (SELECT count(*) AS cnt FROM trades WHERE order_id = o.id) t
                            ORDER BY o.id
                            """,
                    null, true, false
            );
        });
    }

    // T31: ungrouped count(*), LEFT — no match returns 0 (not NULL)
    @Test
    public void testT31UngroupedCountLeft() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, order_id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z'),
                    (3, '2024-01-01T02:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 1, '2024-01-01T00:10:00.000000Z'),
                    (2, 1, '2024-01-01T00:20:00.000000Z')
                    """);

            assertQueryNoLeakCheck(
                    """
                            id\tcnt
                            1\t2
                            2\t0
                            3\t0
                            """,
                    """
                            SELECT o.id, t.cnt
                            FROM orders o
                            LEFT JOIN LATERAL (SELECT count(*) AS cnt FROM trades WHERE order_id = o.id) t
                            ORDER BY o.id
                            """,
                    null, true, false
            );
        });
    }

    // T32: ungrouped sum(), LEFT — no match returns NULL (sum's correct semantics)
    @Test
    public void testT32UngroupedSumLeft() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z'),
                    (3, '2024-01-01T02:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 1, 20.0, '2024-01-01T00:20:00.000000Z')
                    """);

            assertQueryNoLeakCheck(
                    """
                            id\ttotal
                            1\t30.0
                            2\tnull
                            3\tnull
                            """,
                    """
                            SELECT o.id, t.total
                            FROM orders o
                            LEFT JOIN LATERAL (SELECT sum(qty) AS total FROM trades WHERE order_id = o.id) t
                            ORDER BY o.id
                            """,
                    null, true, false
            );
        });
    }

    // T34: Scan + non-rewritable non-eq (no aggregate) → postJoinWhereClause
    @Test
    public void testT34ScanNonRewritableNonEqSimple() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, start_ts TIMESTAMP, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:15:00.000000Z', '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:15:00.000000Z', '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 1, 20.0, '2024-01-01T00:20:00.000000Z'),
                    (3, 1, 30.0, '2024-01-01T00:30:00.000000Z'),
                    (4, 2, 40.0, '2024-01-01T01:10:00.000000Z'),
                    (5, 2, 50.0, '2024-01-01T01:20:00.000000Z')
                    """);

            // o.start_ts is not in eq map (only o.id is)
            // No aggregate → simple scan path → postJoinWhereClause
            assertQueryNoLeakCheck(
                    """
                            id\tqty
                            1\t20.0
                            1\t30.0
                            2\t50.0
                            """,
                    """
                            SELECT o.id, t.qty
                            FROM orders o
                            JOIN LATERAL (
                                SELECT qty, ts AS trade_ts FROM trades
                                WHERE order_id = o.id AND ts > o.start_ts
                            ) t
                            ORDER BY o.id, t.qty
                            """,
                    null, true, false
            );
        });
    }

    // T35: Complex equality — outer side is expression (inner.col = f(outer.col))
    @Test
    public void testT35ComplexEquality() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, group_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (10, '2024-01-01T00:00:00.000000Z'),
                    (20, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 1, 100.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 2, 200.0, '2024-01-01T01:10:00.000000Z'),
                    (3, 1, 300.0, '2024-01-01T00:20:00.000000Z')
                    """);

            // group_id = o.id / 10 → outer side is expression
            assertQueryNoLeakCheck(
                    """
                            id\tqty
                            10\t100.0
                            10\t300.0
                            20\t200.0
                            """,
                    """
                            SELECT o.id, t.qty
                            FROM orders o
                            JOIN LATERAL (
                                SELECT qty FROM trades WHERE group_id = o.id / 10
                            ) t
                            ORDER BY o.id, t.qty
                            """,
                    null, true, false
            );
        });
    }

    // T36: Inner JOIN ON correlation — extractCorrelatedFromInnerJoins
    @Test
    public void testT36InnerJoinOnCorrelation() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t1 (id INT, val STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t2 (id INT, order_id INT, t1_id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO orders VALUES (1, '2024-01-01T00:00:00.000000Z'), (2, '2024-01-01T01:00:00.000000Z')");
            execute("INSERT INTO t1 VALUES (1, 'X', '2024-01-01T00:10:00.000000Z'), (2, 'Y', '2024-01-01T01:10:00.000000Z')");
            execute("""
                    INSERT INTO t2 VALUES
                    (1, 1, 1, '2024-01-01T00:20:00.000000Z'),
                    (2, 2, 2, '2024-01-01T01:20:00.000000Z')
                    """);

            // Correlation is only in the inner JOIN ON clause: t2.order_id = o.id
            assertQueryNoLeakCheck(
                    """
                            id\tval
                            1\tX
                            2\tY
                            """,
                    """
                            SELECT o.id, sub.val
                            FROM orders o
                            JOIN LATERAL (
                                SELECT t1.val
                                FROM t1
                                JOIN t2 ON t2.t1_id = t1.id AND t2.order_id = o.id
                            ) sub
                            ORDER BY o.id
                            """,
                    null, true, false
            );
        });
    }

    // T37: GROUP BY + mixed eq + rewritable non-eq
    @Test
    public void testT37GroupByRewritableNonEq() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, min_qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, 15.0, '2024-01-01T00:00:00.000000Z'),
                    (2, 25.0, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    (1, 1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 1, 20.0, '2024-01-01T00:20:00.000000Z'),
                    (3, 1, 30.0, '2024-01-01T00:30:00.000000Z'),
                    (4, 2, 20.0, '2024-01-01T01:10:00.000000Z'),
                    (5, 2, 30.0, '2024-01-01T01:20:00.000000Z'),
                    (6, 2, 40.0, '2024-01-01T01:30:00.000000Z')
                    """);

            // eq: order_id = o.id; non-eq: qty > o.min_qty (rewritable since o.id maps min_qty too)
            // Well actually o.min_qty is NOT in the alias map, but it's a simple scan + aggregate scenario
            // with non-rewritable non-eq → Delim Scan path
            assertQueryNoLeakCheck(
                    """
                            id\ttotal
                            1\t50.0
                            2\t70.0
                            """,
                    """
                            SELECT o.id, t.total
                            FROM orders o
                            JOIN LATERAL (
                                SELECT sum(qty) AS total FROM trades
                                WHERE order_id = o.id AND qty > o.min_qty
                            ) t
                            ORDER BY o.id
                            """,
                    null, true, false
            );
        });
    }

    // T38: OR predicate with correlated ref — no equality predicates, simple scan
    @Test
    public void testT38OrCorrelatedPredicate() throws Exception {
        assertMemoryLeak(() -> {
            createOrdersAndTrades();

            assertQueryNoLeakCheck(
                    """
                            id\tcustomer\tqty
                            1\tAlice\t10.0
                            1\tAlice\t20.0
                            2\tBob\t30.0
                            3\tCharlie\t40.0
                            3\tCharlie\t50.0
                            """,
                    """
                            SELECT o.id, o.customer, t.qty
                            FROM orders o
                            JOIN LATERAL (
                                SELECT qty FROM trades
                                WHERE order_id = o.id OR (qty > 100.0 AND order_id = o.id)
                            ) t
                            ORDER BY o.id, t.qty
                            """,
                    null, true, false
            );
        });
    }

    // T39: Correlated ref only in SELECT expression — Delim needed
    @Test
    public void testT39CorrelatedRefInSelect() throws Exception {
        assertMemoryLeak(() -> {
            createOrdersAndTrades();

            assertQueryNoLeakCheck(
                    """
                            id\tqty_plus_id
                            1\t11.0
                            1\t21.0
                            2\t32.0
                            3\t43.0
                            3\t53.0
                            """,
                    """
                            SELECT o.id, t.qty_plus_id
                            FROM orders o
                            JOIN LATERAL (
                                SELECT qty + o.id AS qty_plus_id FROM trades
                                WHERE order_id = o.id
                            ) t
                            ORDER BY o.id, t.qty_plus_id
                            """,
                    null, true, false
            );
        });
    }

    // T40: Correlated ref in SELECT with aggregate — Delim needed
    @Test
    public void testT40CorrelatedRefInSelectWithAggregate() throws Exception {
        assertMemoryLeak(() -> {
            createOrdersAndTrades();

            assertQueryNoLeakCheck(
                    """
                            id\ttotal_plus_id
                            1\t31.0
                            2\t32.0
                            3\t93.0
                            """,
                    """
                            SELECT o.id, t.total_plus_id
                            FROM orders o
                            JOIN LATERAL (
                                SELECT sum(qty) + o.id AS total_plus_id FROM trades
                                WHERE order_id = o.id
                            ) t
                            ORDER BY o.id
                            """,
                    null, true, false
            );
        });
    }

    // T41: No equality predicates at all, only non-eq correlated WHERE
    @Test
    public void testT41OnlyNonEqCorrelatedPredicate() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE sensors (id INT, threshold DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE readings (sensor_id INT, value DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO sensors VALUES
                    (1, 50.0, '2024-01-01T00:00:00.000000Z'),
                    (2, 30.0, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO readings VALUES
                    (1, 60.0, '2024-01-01T00:10:00.000000Z'),
                    (1, 40.0, '2024-01-01T00:20:00.000000Z'),
                    (2, 35.0, '2024-01-01T01:10:00.000000Z'),
                    (2, 25.0, '2024-01-01T01:20:00.000000Z')
                    """);

            // Only non-eq correlated pred: value > s.threshold AND sensor_id = s.id
            // sensor_id = s.id is eq, value > s.threshold is non-eq
            assertQueryNoLeakCheck(
                    """
                            id\tvalue
                            1\t60.0
                            2\t35.0
                            """,
                    """
                            SELECT s.id, r.value
                            FROM sensors s
                            JOIN LATERAL (
                                SELECT value FROM readings
                                WHERE sensor_id = s.id AND value > s.threshold
                            ) r
                            ORDER BY s.id
                            """,
                    null, true, false
            );
        });
    }

    // T42: Nested LATERAL - inner LATERAL references both outer-outer and direct outer columns
    @Test
    public void testT42NestedLateralBasic() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (a INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t2 (b INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t3 (c INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t1 VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO t2 VALUES
                    (10, '2024-01-01T00:10:00.000000Z'),
                    (20, '2024-01-01T01:10:00.000000Z')
                    """);
            execute("""
                    INSERT INTO t3 VALUES
                    (100, '2024-01-01T00:20:00.000000Z'),
                    (200, '2024-01-01T01:20:00.000000Z')
                    """);

            // inner LATERAL SELECT references t1.a (outer-outer) and t2.b (direct outer)
            assertQueryNoLeakCheck(
                    """
                            a\tb\tresult
                            1\t10\t111
                            1\t20\t121
                            2\t10\t112
                            2\t20\t122
                            """,
                    """
                            SELECT t1.a, x.b, x.result
                            FROM t1
                            CROSS JOIN LATERAL (
                                SELECT t2.b, y.result
                                FROM t2
                                CROSS JOIN LATERAL (
                                    SELECT t1.a + t2.b + t3.c AS result FROM t3
                                    LIMIT 1
                                ) y
                            ) x
                            ORDER BY t1.a, x.b
                            """,
                    null, true, false
            );
        });
    }

    // T43: Nested LATERAL - inner LATERAL WHERE references outer-outer column
    @Test
    public void testT43NestedLateralWithCorrelatedWhere() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE departments (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE employees (dept_id INT, name STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE tasks (emp_name STRING, dept_id INT, priority INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO departments VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO employees VALUES
                    (1, 'Alice', '2024-01-01T00:10:00.000000Z'),
                    (1, 'Bob', '2024-01-01T00:20:00.000000Z'),
                    (2, 'Charlie', '2024-01-01T01:10:00.000000Z')
                    """);
            execute("""
                    INSERT INTO tasks VALUES
                    ('Alice', 1, 5, '2024-01-01T00:30:00.000000Z'),
                    ('Bob', 1, 3, '2024-01-01T00:40:00.000000Z'),
                    ('Charlie', 2, 7, '2024-01-01T01:30:00.000000Z')
                    """);

            // Inner LATERAL WHERE uses dept_id = d.id (outer-outer ref)
            assertQueryNoLeakCheck(
                    """
                            id\tname\tpriority
                            1\tAlice\t5
                            1\tBob\t3
                            2\tCharlie\t7
                            """,
                    """
                            SELECT d.id, x.name, x.priority
                            FROM departments d
                            JOIN LATERAL (
                                SELECT e.name, t.priority
                                FROM employees e
                                JOIN LATERAL (
                                    SELECT priority FROM tasks
                                    WHERE emp_name = e.name AND dept_id = d.id
                                ) t ON 1 = 1
                                WHERE e.dept_id = d.id
                            ) x
                            ORDER BY d.id, x.name
                            """,
                    null, true, false
            );
        });
    }

    // T44: Nested LATERAL with aggregate in inner
    @Test
    public void testT44NestedLateralWithAggregate() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE categories (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE products (cat_id INT, name STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE sales (product_name STRING, amount DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO categories VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO products VALUES
                    (1, 'Widget', '2024-01-01T00:10:00.000000Z'),
                    (1, 'Gadget', '2024-01-01T00:20:00.000000Z'),
                    (2, 'Doohickey', '2024-01-01T01:10:00.000000Z')
                    """);
            execute("""
                    INSERT INTO sales VALUES
                    ('Widget', 100.0, '2024-01-01T00:30:00.000000Z'),
                    ('Widget', 150.0, '2024-01-01T00:40:00.000000Z'),
                    ('Gadget', 200.0, '2024-01-01T00:50:00.000000Z'),
                    ('Doohickey', 300.0, '2024-01-01T01:30:00.000000Z')
                    """);

            // Inner LATERAL aggregates sales per product, outer LATERAL filters by category
            assertQueryNoLeakCheck(
                    """
                            id\tname\ttotal
                            1\tGadget\t200.0
                            1\tWidget\t250.0
                            2\tDoohickey\t300.0
                            """,
                    """
                            SELECT c.id, x.name, x.total
                            FROM categories c
                            JOIN LATERAL (
                                SELECT p.name, s.total
                                FROM products p
                                JOIN LATERAL (
                                    SELECT sum(amount) AS total FROM sales
                                    WHERE product_name = p.name
                                ) s ON 1 = 1
                                WHERE p.cat_id = c.id
                            ) x
                            ORDER BY c.id, x.name
                            """,
                    null, true, false
            );
        });
    }

    // T45: Nested LATERAL - outer-outer ref only in SELECT expression of innermost subquery
    @Test
    public void testT45NestedLateralOuterRefOnlyInSelect() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (a INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t2 (b INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t1 VALUES
                    (10, '2024-01-01T00:00:00.000000Z'),
                    (20, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO t2 VALUES
                    (1, '2024-01-01T00:10:00.000000Z'),
                    (2, '2024-01-01T01:10:00.000000Z')
                    """);

            // Inner LATERAL's SELECT references t1.a (outer-outer) without any WHERE correlation
            assertQueryNoLeakCheck(
                    """
                            a\tb\tresult
                            10\t1\t11
                            10\t2\t12
                            20\t1\t21
                            20\t2\t22
                            """,
                    """
                            SELECT t1.a, x.b, x.result
                            FROM t1
                            CROSS JOIN LATERAL (
                                SELECT t2.b, y.result
                                FROM t2
                                CROSS JOIN LATERAL (
                                    SELECT t1.a + t2.b AS result
                                ) y
                            ) x
                            ORDER BY t1.a, x.b
                            """,
                    null, true, false
            );
        });
    }

    // T46: EXCEPT, INNER, equality correlation — per-group set difference
    @Test
    public void testT46Except() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades_a (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades_b (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades_a VALUES
                    (1, 1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 1, 20.0, '2024-01-01T00:11:00.000000Z'),
                    (3, 2, 30.0, '2024-01-01T01:10:00.000000Z'),
                    (4, 2, 40.0, '2024-01-01T01:11:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades_b VALUES
                    (1, 1, 10.0, '2024-01-01T00:20:00.000000Z'),
                    (2, 2, 30.0, '2024-01-01T01:20:00.000000Z')
                    """);

            // order 1: trades_a {10,20} \ trades_b {10} = {20}
            // order 2: trades_a {30,40} \ trades_b {30} = {40}
            assertQueryNoLeakCheck(
                    """
                            id\tqty
                            1\t20.0
                            2\t40.0
                            """,
                    """
                            SELECT o.id, t.qty
                            FROM orders o
                            JOIN LATERAL (
                                SELECT qty FROM trades_a WHERE order_id = o.id
                                EXCEPT
                                SELECT qty FROM trades_b WHERE order_id = o.id
                            ) t
                            ORDER BY o.id, t.qty
                            """,
                    null, true, false
            );
        });
    }

    // T47: INTERSECT ALL, INNER, equality correlation — preserves duplicates
    @Test
    public void testT47IntersectAll() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades_a (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades_b (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades_a VALUES
                    (1, 1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 1, 10.0, '2024-01-01T00:11:00.000000Z'),
                    (3, 1, 20.0, '2024-01-01T00:12:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades_b VALUES
                    (1, 1, 10.0, '2024-01-01T00:20:00.000000Z'),
                    (2, 1, 10.0, '2024-01-01T00:21:00.000000Z'),
                    (3, 1, 10.0, '2024-01-01T00:22:00.000000Z')
                    """);

            // order 1: trades_a {10,10,20} INTERSECT ALL trades_b {10,10,10} = {10,10}
            assertQueryNoLeakCheck(
                    """
                            id\tqty
                            1\t10.0
                            1\t10.0
                            """,
                    """
                            SELECT o.id, t.qty
                            FROM orders o
                            JOIN LATERAL (
                                SELECT qty FROM trades_a WHERE order_id = o.id
                                INTERSECT ALL
                                SELECT qty FROM trades_b WHERE order_id = o.id
                            ) t
                            ORDER BY o.id, t.qty
                            """,
                    null, true, false
            );
        });
    }

    // T48: EXCEPT ALL, INNER, equality correlation — preserves multiplicity
    @Test
    public void testT48ExceptAll() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades_a (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades_b (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades_a VALUES
                    (1, 1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 1, 10.0, '2024-01-01T00:11:00.000000Z'),
                    (3, 1, 10.0, '2024-01-01T00:12:00.000000Z'),
                    (4, 1, 20.0, '2024-01-01T00:13:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades_b VALUES
                    (1, 1, 10.0, '2024-01-01T00:20:00.000000Z')
                    """);

            // order 1: trades_a {10,10,10,20} EXCEPT ALL trades_b {10} = {10,10,20}
            assertQueryNoLeakCheck(
                    """
                            id\tqty
                            1\t10.0
                            1\t10.0
                            1\t20.0
                            """,
                    """
                            SELECT o.id, t.qty
                            FROM orders o
                            JOIN LATERAL (
                                SELECT qty FROM trades_a WHERE order_id = o.id
                                EXCEPT ALL
                                SELECT qty FROM trades_b WHERE order_id = o.id
                            ) t
                            ORDER BY o.id, t.qty
                            """,
                    null, true, false
            );
        });
    }

    // T49: EXCEPT with LEFT LATERAL — unmatched outer rows preserved
    @Test
    public void testT49ExceptLeft() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades_a (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades_b (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z'),
                    (3, '2024-01-01T02:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades_a VALUES
                    (1, 1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 1, 20.0, '2024-01-01T00:11:00.000000Z'),
                    (3, 2, 30.0, '2024-01-01T01:10:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades_b VALUES
                    (1, 1, 10.0, '2024-01-01T00:20:00.000000Z'),
                    (2, 2, 30.0, '2024-01-01T01:20:00.000000Z')
                    """);

            // order 1: trades_a {10,20} \ trades_b {10} = {20}
            // order 2: trades_a {30} \ trades_b {30} = {} → LEFT fills NaN
            // order 3: no trades → LEFT fills NaN
            assertQueryNoLeakCheck(
                    """
                            id\tqty
                            1\t20.0
                            2\tnull
                            3\tnull
                            """,
                    """
                            SELECT o.id, t.qty
                            FROM orders o
                            LEFT JOIN LATERAL (
                                SELECT qty FROM trades_a WHERE order_id = o.id
                                EXCEPT
                                SELECT qty FROM trades_b WHERE order_id = o.id
                            ) t
                            ORDER BY o.id, t.qty
                            """,
                    null, true, false
            );
        });
    }

    // T50: UNION with multi-group cross-group correctness — verifies no cross-group interference
    @Test
    public void testT50UnionMultiGroup() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades_a (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades_b (id INT, order_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z'),
                    (3, '2024-01-01T02:00:00.000000Z')
                    """);
            // Same qty=10 appears in both branches for different orders
            execute("""
                    INSERT INTO trades_a VALUES
                    (1, 1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (2, 2, 10.0, '2024-01-01T01:10:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades_b VALUES
                    (1, 1, 10.0, '2024-01-01T00:20:00.000000Z'),
                    (2, 3, 10.0, '2024-01-01T02:20:00.000000Z')
                    """);

            // UNION dedup must be per-group, not global:
            // order 1: {10} ∪ {10} = {10} (deduped within group)
            // order 2: {10} ∪ {} = {10}
            // order 3: {} ∪ {10} = {10}
            // All three orders produce qty=10, but they must NOT be deduped across groups
            assertQueryNoLeakCheck(
                    """
                            id\tqty
                            1\t10.0
                            2\t10.0
                            3\t10.0
                            """,
                    """
                            SELECT o.id, t.qty
                            FROM orders o
                            JOIN LATERAL (
                                SELECT qty FROM trades_a WHERE order_id = o.id
                                UNION
                                SELECT qty FROM trades_b WHERE order_id = o.id
                            ) t
                            ORDER BY o.id, t.qty
                            """,
                    null, true, false
            );
        });
    }

    // T51: Wrapper-layer correlation — outer ref only in wrapper SELECT, content has equality correlation
    @Test
    public void testT51WrapperSelectCorrelation() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (a INT, b INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t2 (id INT, t1_id INT, val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t1 VALUES
                    (10, 100, '2024-01-01T00:00:00.000000Z'),
                    (20, 200, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO t2 VALUES
                    (1, 10, 1, '2024-01-01T00:10:00.000000Z'),
                    (2, 10, 2, '2024-01-01T00:20:00.000000Z'),
                    (3, 20, 3, '2024-01-01T01:10:00.000000Z')
                    """);

            // Wrapper SELECT references t1.b (outer), content WHERE has equality t2.t1_id = t1.a
            assertQueryNoLeakCheck(
                    """
                            a\tresult
                            10\t101
                            10\t102
                            20\t203
                            """,
                    """
                            SELECT t1.a, x.result
                            FROM t1
                            CROSS JOIN LATERAL (
                                SELECT val + t1.b AS result
                                FROM (
                                    SELECT val FROM t2 WHERE t2.t1_id = t1.a
                                )
                            ) x
                            ORDER BY t1.a, x.result
                            """,
                    null, true, false
            );
        });
    }

    // T52: Wrapper-layer correlation — outer ref in wrapper WHERE, content has equality correlation
    @Test
    public void testT52WrapperWhereCorrelation() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (a INT, threshold INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t2 (t1_id INT, val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t1 VALUES
                    (1, 5, '2024-01-01T00:00:00.000000Z'),
                    (2, 25, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO t2 VALUES
                    (1, 3, '2024-01-01T00:10:00.000000Z'),
                    (1, 10, '2024-01-01T00:20:00.000000Z'),
                    (2, 20, '2024-01-01T01:10:00.000000Z'),
                    (2, 30, '2024-01-01T01:20:00.000000Z')
                    """);

            // Wrapper WHERE references t1.threshold (outer), content WHERE has equality t2.t1_id = t1.a
            assertQueryNoLeakCheck(
                    """
                            a\tval
                            1\t10
                            2\t30
                            """,
                    """
                            SELECT t1.a, x.val
                            FROM t1
                            CROSS JOIN LATERAL (
                                SELECT val FROM (
                                    SELECT val FROM t2 WHERE t2.t1_id = t1.a
                                )
                                WHERE val > t1.threshold
                            ) x
                            ORDER BY t1.a
                            """,
                    null, true, false
            );
        });
    }

    // T53: Wrapper-layer correlation — both wrapper SELECT and WHERE reference outer columns
    @Test
    public void testT53WrapperSelectAndWhereCorrelation() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (a INT, b INT, threshold INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t2 (t1_id INT, val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t1 VALUES
                    (1, 100, 5, '2024-01-01T00:00:00.000000Z'),
                    (2, 200, 25, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO t2 VALUES
                    (1, 3, '2024-01-01T00:10:00.000000Z'),
                    (1, 10, '2024-01-01T00:20:00.000000Z'),
                    (2, 20, '2024-01-01T01:10:00.000000Z'),
                    (2, 30, '2024-01-01T01:20:00.000000Z')
                    """);

            // Wrapper SELECT has t1.b, wrapper WHERE has t1.threshold, content WHERE has t1.a
            assertQueryNoLeakCheck(
                    """
                            a\tresult
                            1\t110
                            2\t230
                            """,
                    """
                            SELECT t1.a, x.result
                            FROM t1
                            CROSS JOIN LATERAL (
                                SELECT val + t1.b AS result
                                FROM (
                                    SELECT val FROM t2 WHERE t2.t1_id = t1.a
                                )
                                WHERE val > t1.threshold
                            ) x
                            ORDER BY t1.a
                            """,
                    null, true, false
            );
        });
    }

    // T54: Wrapper-layer correlation with aggregate in content
    @Test
    public void testT54WrapperCorrelationWithAggregate() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (a INT, multiplier INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t2 (t1_id INT, val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t1 VALUES
                    (1, 10, '2024-01-01T00:00:00.000000Z'),
                    (2, 20, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO t2 VALUES
                    (1, 3, '2024-01-01T00:10:00.000000Z'),
                    (1, 7, '2024-01-01T00:20:00.000000Z'),
                    (2, 5, '2024-01-01T01:10:00.000000Z'),
                    (2, 15, '2024-01-01T01:20:00.000000Z')
                    """);

            // Wrapper SELECT references t1.multiplier, content has aggregate SUM + equality
            assertQueryNoLeakCheck(
                    """
                            a\tresult
                            1\t100
                            2\t400
                            """,
                    """
                            SELECT t1.a, x.result
                            FROM t1
                            CROSS JOIN LATERAL (
                                SELECT total * t1.multiplier AS result
                                FROM (
                                    SELECT SUM(val) AS total FROM t2 WHERE t2.t1_id = t1.a
                                )
                            ) x
                            ORDER BY t1.a
                            """,
                    null, true, false
            );
        });
    }

    // T55: Subquery outer with non-equality correlation — __outer_ref must preserve outer WHERE
    @Test
    public void testT55SubqueryOuterNonEqCorrelation() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, status STRING, total DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE items (order_id INT, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, 'active', 50.0, '2024-01-01T00:00:00.000000Z'),
                    (2, 'closed', 100.0, '2024-01-01T01:00:00.000000Z'),
                    (3, 'active', 200.0, '2024-01-01T02:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO items VALUES
                    (1, 10.0, '2024-01-01T00:10:00.000000Z'),
                    (1, 60.0, '2024-01-01T00:20:00.000000Z'),
                    (2, 110.0, '2024-01-01T01:10:00.000000Z'),
                    (3, 50.0, '2024-01-01T02:10:00.000000Z'),
                    (3, 250.0, '2024-01-01T02:20:00.000000Z')
                    """);

            // Outer is a subquery with WHERE status = 'active' (filters out order 2).
            // Non-equality correlation: items.price > o.total.
            // __outer_ref must preserve the WHERE — without it, order 2's total=100
            // would leak into __outer_ref and produce wrong results.
            assertQueryNoLeakCheck(
                    """
                            id\tprice
                            1\t60.0
                            3\t250.0
                            """,
                    """
                            SELECT o.id, t.price
                            FROM (SELECT * FROM orders WHERE status = 'active') o
                            CROSS JOIN LATERAL (
                                SELECT price FROM items
                                WHERE order_id = o.id AND price > o.total
                            ) t
                            ORDER BY o.id
                            """,
                    null, true, false
            );
        });
    }

    // T56: LEFT LATERAL with COUNT and wildcard SELECT — coalesce must produce 0 not NULL
    @Test
    public void testT56LeftLateralCountWildcard() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (a INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t2 (t1_id INT, val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t1 VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z'),
                    (3, '2024-01-01T02:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO t2 VALUES
                    (1, 10, '2024-01-01T00:10:00.000000Z'),
                    (1, 20, '2024-01-01T00:20:00.000000Z'),
                    (2, 30, '2024-01-01T01:10:00.000000Z')
                    """);

            assertQueryNoLeakCheck(
                    """
                            a\tcnt
                            1\t2
                            2\t1
                            3\t0
                            """,
                    """
                            SELECT *
                            FROM t1
                            LEFT JOIN LATERAL (
                                SELECT count(*) AS cnt FROM t2 WHERE t2.t1_id = t1.a
                            ) t ON true
                            ORDER BY t1.a
                            """,
                    null, true, false
            );
        });
    }

    // T57: LEFT JOIN inside LATERAL — correlated predicate in ON clause must stay in ON,
    // not move to WHERE, to preserve NULL fill for non-matching rows
    @Test
    public void testT57LeftJoinInLateralPreservesNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE items (id INT, order_id INT, name STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE tags (item_id INT, order_id INT, tag STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO items VALUES
                    (10, 1, 'Widget', '2024-01-01T00:10:00.000000Z'),
                    (20, 2, 'Gadget', '2024-01-01T01:10:00.000000Z')
                    """);
            // Only order 1's item has a tag — order 2's item should get NULL fill
            execute("""
                    INSERT INTO tags VALUES
                    (10, 1, 'premium', '2024-01-01T00:20:00.000000Z')
                    """);

            // LEFT JOIN inside LATERAL: tags.order_id = o.id is correlated and in ON clause.
            // It must stay there so that items without matching tags get NULL fill.
            assertQueryNoLeakCheck(
                    """
                            id\tname\ttag
                            1\tWidget\tpremium
                            2\tGadget\t
                            """,
                    """
                            SELECT o.id, sub.name, sub.tag
                            FROM orders o
                            JOIN LATERAL (
                                SELECT i.name, t.tag
                                FROM items i
                                LEFT JOIN tags t ON t.item_id = i.id AND t.order_id = o.id
                                WHERE i.order_id = o.id
                            ) sub
                            ORDER BY o.id
                            """,
                    null, true, false
            );
        });
    }

    private void createOrdersAndTrades() throws Exception {
        execute("CREATE TABLE orders (id INT, customer STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("CREATE TABLE trades (id INT, order_id INT, qty DOUBLE, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("""
                INSERT INTO orders VALUES
                (1, 'Alice', '2024-01-01T00:00:00.000000Z'),
                (2, 'Bob', '2024-01-01T01:00:00.000000Z'),
                (3, 'Charlie', '2024-01-01T02:00:00.000000Z')
                """);
        execute("""
                INSERT INTO trades VALUES
                (1, 1, 10.0, 100.0, '2024-01-01T00:30:00.000000Z'),
                (2, 1, 20.0, 200.0, '2024-01-01T00:45:00.000000Z'),
                (3, 2, 30.0, 300.0, '2024-01-01T01:30:00.000000Z'),
                (4, 3, 40.0, 400.0, '2024-01-01T02:30:00.000000Z'),
                (5, 3, 50.0, 500.0, '2024-01-01T02:45:00.000000Z')
                """);
    }
}
