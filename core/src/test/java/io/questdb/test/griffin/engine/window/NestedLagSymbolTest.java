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

package io.questdb.test.griffin.engine.window;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class NestedLagSymbolTest extends AbstractCairoTest {

    @Test
    public void testLagLeadOverBooleanCastKeepsMissingNeighborNull() throws Exception {
        // BOOLEAN has no NULL, so active::SYMBOL only ever mints keys 0 and 1. lag()/lead() mint
        // VALUE_IS_NULL for a missing neighbor and resolve it through the cast's symbol table,
        // which must answer NULL rather than 'false'.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE flags (seq LONG, device SYMBOL, active BOOLEAN)");
            execute("""
                    INSERT INTO flags VALUES
                    (1, 'pumpA', true),
                    (2, 'pumpB', false),
                    (3, 'pumpA', false),
                    (4, 'pumpB', true),
                    (5, 'pumpC', true),
                    (6, 'pumpA', true)
                    """);

            // streaming window
            assertQuery("SELECT seq, LAG(active::SYMBOL) OVER (PARTITION BY device) previous FROM flags")
                    .expectSize()
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            seq\tprevious
                            1\t
                            2\t
                            3\ttrue
                            4\tfalse
                            5\t
                            6\tfalse
                            """);
            assertQuery("""
                    SELECT seq FROM (
                        SELECT seq, LAG(active::SYMBOL) OVER (PARTITION BY device) previous FROM flags
                    ) WHERE previous IS NULL
                    """)
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            seq
                            1
                            2
                            5
                            """);
            // a downstream group-by keys on the window column and resolves the int key through
            // the table the cast hands out from newSymbolTable(), not through valueOf()
            assertQuery("""
                    SELECT previous, count() FROM (
                        SELECT LAG(active::SYMBOL) OVER (PARTITION BY device) previous FROM flags
                    ) ORDER BY previous
                    """)
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            previous\tcount
                            \t3
                            false\t2
                            true\t1
                            """);
            // cached window
            assertQuery("""
                    SELECT seq, device, coalesce(previous, 'none') previous, coalesce(next, 'none') next FROM (
                        SELECT
                            seq,
                            device,
                            LAG(active::SYMBOL) OVER (PARTITION BY device ORDER BY seq) previous,
                            LEAD(active::SYMBOL) OVER (PARTITION BY device ORDER BY seq) next
                        FROM flags
                    ) WHERE previous IS NULL OR next IS NULL
                    """)
                    .noLeakCheck()
                    .returns("""
                            seq\tdevice\tprevious\tnext
                            1\tpumpA\tnone\tfalse
                            2\tpumpB\tnone\ttrue
                            4\tpumpB\tfalse\tnone
                            5\tpumpC\tnone\tnone
                            6\tpumpA\tfalse\tnone
                            """);
        });
    }

    @Test
    public void testLagLeadOverSymbolIgnoreNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE symbols (" +
                            "  sym SYMBOL," +
                            "  ts TIMESTAMP" +
                            ") TIMESTAMP(ts) PARTITION BY DAY"
            );
            execute(
                    "INSERT INTO symbols VALUES" +
                            " ('a', '2024-01-01T00:00:00.000000Z')," +
                            " (null, '2024-01-01T00:01:00.000000Z')," +
                            " ('b', '2024-01-01T00:02:00.000000Z')," +
                            " (null, '2024-01-01T00:03:00.000000Z')," +
                            " ('c', '2024-01-01T00:04:00.000000Z')"
            );

            assertQuery(
                    "SELECT sym," +
                            "   LAG(sym, 1) ignore nulls OVER (ORDER BY ts) AS prev_sym," +
                            "   LEAD(sym, 1) ignore nulls OVER (ORDER BY ts) AS next_sym" +
                            " FROM symbols"
            ).expectSize().returns(
                    "sym\tprev_sym\tnext_sym\n" +
                            "a\t\tb\n" +
                            "\ta\tb\n" +
                            "b\ta\tc\n" +
                            "\tb\tc\n" +
                            "c\tb\t\n"
            );
        });
    }

    @Test
    public void testLagLeadSymbolDownstreamFilter() throws Exception {
        // The window column reports a static symbol table when its argument is a table column,
        // so the outer filter resolves its constants to int keys. 'nope' is absent from the
        // symbol table and must match nothing.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, k SYMBOL, a SYMBOL, p DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:00', 'k1', 'a1', 3),
                    ('2024-01-01T01:00:00', 'k2', NULL, 2),
                    ('2024-01-02T02:00:00', 'k1', 'a3', 1),
                    ('2024-01-02T03:00:00', 'k2', 'a4', 0)
                    """);

            // streaming window
            assertQuery("SELECT ts, x FROM (SELECT ts, lag(a) OVER () x FROM t) WHERE x IN ('a1', 'a3', 'nope')")
                    .timestamp("ts")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            ts\tx
                            2024-01-01T01:00:00.000000Z\ta1
                            2024-01-02T03:00:00.000000Z\ta3
                            """);
            assertQuery("SELECT ts, x FROM (SELECT ts, lag(a) OVER (PARTITION BY k) x FROM t) WHERE x != 'a1'")
                    .timestamp("ts")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            ts\tx
                            2024-01-01T00:00:00.000000Z\t
                            2024-01-01T01:00:00.000000Z\t
                            2024-01-02T03:00:00.000000Z\t
                            """);
            // cached window
            assertQuery("SELECT ts, x FROM (SELECT ts, lead(a) OVER (ORDER BY p) x FROM t) WHERE x = 'a3' OR x = 'nope'")
                    .timestamp("ts")
                    .noLeakCheck()
                    .returns("""
                            ts\tx
                            2024-01-02T03:00:00.000000Z\ta3
                            """);
        });
    }

    @Test
    public void testLagLeadSymbolDownstreamJoin() throws Exception {
        // dim assigns different int keys to the same symbol values, and holds a value absent
        // from t, so the join must map keys between the two static symbol tables by value.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, k SYMBOL, a SYMBOL, p DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:00', 'k1', 'a1', 3),
                    ('2024-01-01T01:00:00', 'k2', NULL, 2),
                    ('2024-01-02T02:00:00', 'k1', 'a3', 1),
                    ('2024-01-02T03:00:00', 'k2', 'a4', 0)
                    """);
            execute("CREATE TABLE dim (a SYMBOL, v INT)");
            execute("INSERT INTO dim VALUES ('zz', 0), ('a4', 4), ('a3', 3), ('a1', 1)");

            // streaming window
            assertQuery("SELECT w.ts, w.x, dim.v FROM (SELECT ts, lag(a) OVER () x FROM t) w JOIN dim ON w.x = dim.a")
                    .timestamp("ts")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            ts\tx\tv
                            2024-01-01T01:00:00.000000Z\ta1\t1
                            2024-01-02T03:00:00.000000Z\ta3\t3
                            """);
            // cached window
            assertQuery("SELECT w.ts, w.x, dim.v FROM (SELECT ts, lead(a) OVER (ORDER BY p) x FROM t) w LEFT JOIN dim ON w.x = dim.a")
                    .timestamp("ts")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            ts\tx\tv
                            2024-01-01T00:00:00.000000Z\t\tnull
                            2024-01-01T01:00:00.000000Z\ta1\t1
                            2024-01-02T02:00:00.000000Z\t\tnull
                            2024-01-02T03:00:00.000000Z\ta3\t3
                            """);
        });
    }

    @Test
    public void testLagLeadSymbolNonLightCachedWindow() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, false);
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE balances (" +
                            "  sym SYMBOL," +
                            "  quantity DOUBLE," +
                            "  ts TIMESTAMP" +
                            ") TIMESTAMP(ts) PARTITION BY DAY"
            );
            execute(
                    "INSERT INTO balances VALUES" +
                            " ('a', 1.0, '2024-01-01T00:00:00.000000Z')," +
                            " ('b', 2.0, '2024-01-01T00:01:00.000000Z')," +
                            " ('c', 3.0, '2024-01-01T00:02:00.000000Z')"
            );

            assertQuery(
                    "SELECT sym," +
                            "   LAG(sym, 1) OVER (ORDER BY ts) AS prev_sym," +
                            "   LEAD(sym, 1) OVER (ORDER BY ts) AS next_sym" +
                            " FROM balances"
            ).expectSize().returns(
                    "sym\tprev_sym\tnext_sym\n" +
                            "a\t\tb\n" +
                            "b\ta\tc\n" +
                            "c\tb\t\n"
            );
        });
    }

    @Test
    public void testLagLeadSymbolNullDefault() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE symbols (" +
                            "  sym SYMBOL," +
                            "  ts TIMESTAMP" +
                            ") TIMESTAMP(ts) PARTITION BY DAY"
            );
            execute(
                    "INSERT INTO symbols VALUES" +
                            " ('a', '2024-01-01T00:00:00.000000Z')," +
                            " ('b', '2024-01-01T00:01:00.000000Z')," +
                            " ('c', '2024-01-01T00:02:00.000000Z')"
            );

            assertQuery(
                    "SELECT sym," +
                            "   LAG(sym, 1, null) OVER (ORDER BY ts) AS prev_sym," +
                            "   LEAD(sym, 1, null) OVER (ORDER BY ts) AS next_sym" +
                            " FROM symbols"
            ).expectSize().returns(
                    "sym\tprev_sym\tnext_sym\n" +
                            "a\t\tb\n" +
                            "b\ta\tc\n" +
                            "c\tb\t\n"
            );
        });
    }

    @Test
    public void testLagLeadSymbolNullEquality() throws Exception {
        // Neither source column stores a NULL, so both dictionaries report
        // containsNullValue() == false, yet lag()/lead() mint a NULL for the missing neighbor.
        // NULL = NULL has to hold regardless, in line with the STRING comparison.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (id INT, grp SYMBOL, left_sym SYMBOL, right_sym SYMBOL)");
            execute("""
                    INSERT INTO t VALUES
                    (1, 'g1', 'a', 'x'),
                    (2, 'g1', 'b', 'z'),
                    (3, 'g1', 'c', 'a'),
                    (4, 'g2', 'd', 'd')
                    """);

            // streaming window
            assertQuery("""
                    SELECT id, a, b, a = b eq, a != b ne, a::STRING = b::STRING str_eq FROM (
                        SELECT id, LAG(left_sym) OVER (PARTITION BY id) a, LEAD(right_sym) OVER (PARTITION BY id) b FROM t
                    )
                    """)
                    .expectSize()
                    .returns("""
                            id\ta\tb\teq\tne\tstr_eq
                            1\t\t\ttrue\tfalse\ttrue
                            2\t\t\ttrue\tfalse\ttrue
                            3\t\t\ttrue\tfalse\ttrue
                            4\t\t\ttrue\tfalse\ttrue
                            """);
            // cached window: id 2 pairs 'a' with 'a', id 4 has neither neighbor
            assertQuery("""
                    SELECT id, a, b FROM (
                        SELECT
                            id,
                            LAG(left_sym) OVER (PARTITION BY grp ORDER BY id) a,
                            LEAD(right_sym) OVER (PARTITION BY grp ORDER BY id) b
                        FROM t
                    ) WHERE a = b
                    """)
                    .returns("""
                            id\ta\tb
                            2\ta\ta
                            4\t\t
                            """);
        });
    }

    @Test
    public void testLagLeadSymbolOverPartitionRepeatedCursorsStayUnderQueryMemoryLimit() throws Exception {
        // Each cursor run must release what it charged: a leaked or asymmetric charge would
        // accumulate across the runs and breach the limit, or drive the counter negative.
        node1.setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 4 * 1024 * 1024L);
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE tab AS (
                              SELECT x % 10 AS k, rnd_symbol('a', 'b', 'c') AS sym, x::TIMESTAMP AS ts
                              FROM long_sequence(1_000)
                            ) TIMESTAMP(ts) PARTITION BY DAY
                            """
            );
            for (String query : new String[]{
                    "SELECT lag(sym) OVER (PARTITION BY k) FROM tab",
                    "SELECT lead(sym) OVER (PARTITION BY k) FROM tab"
            }) {
                try (RecordCursorFactory factory = select(query)) {
                    for (int i = 0; i < 20; i++) {
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            long rows = 0;
                            while (cursor.hasNext()) {
                                rows++;
                            }
                            Assert.assertEquals(1_000, rows);
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testLagLeadSymbolStaticSymbolTableMetadata() throws Exception {
        // The window column shares its argument's symbol table: static for a table column,
        // dynamic for a cast, which builds its dictionary as rows arrive.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, k SYMBOL, a SYMBOL, p DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:00', 'k1', 'a1', 3),
                    ('2024-01-01T01:00:00', 'k2', NULL, 2),
                    ('2024-01-02T02:00:00', 'k1', 'a3', 1),
                    ('2024-01-02T03:00:00', 'k2', 'a4', 0)
                    """);

            for (String over : new String[]{"OVER ()", "OVER (PARTITION BY k)", "OVER (PARTITION BY k ORDER BY p)"}) {
                for (String function : new String[]{"lag", "lead"}) {
                    try (RecordCursorFactory factory = select("SELECT " + function + "(a) " + over + " x FROM t")) {
                        Assert.assertTrue(function + " " + over, factory.getMetadata().isSymbolTableStatic(0));
                    }
                    try (RecordCursorFactory factory = select("SELECT " + function + "(a::STRING::SYMBOL) " + over + " x FROM t")) {
                        Assert.assertFalse(function + " " + over, factory.getMetadata().isSymbolTableStatic(0));
                    }
                }
            }
        });
    }

    @Test
    public void testLagOffsetOverSymbol() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE balances (" +
                            "  sym SYMBOL," +
                            "  quantity DOUBLE," +
                            "  ts TIMESTAMP" +
                            ") TIMESTAMP(ts) PARTITION BY DAY"
            );
            execute(
                    "INSERT INTO balances VALUES" +
                            " ('a', 1.0, '2024-01-01T00:00:00.000000Z')," +
                            " ('a', 2.0, '2024-01-01T00:01:00.000000Z')," +
                            " ('a', 3.0, '2024-01-01T00:02:00.000000Z')"
            );

            assertQuery(
                    "SELECT sym," +
                            "   LAG(sym, 1) OVER (PARTITION BY sym ORDER BY ts) AS prev_sym," +
                            "   LAG(sym, 2) OVER (PARTITION BY sym ORDER BY ts) AS prev_prev_sym" +
                            " FROM balances"
            ).noRandomAccess().expectSize().returns(
                    "sym\tprev_sym\tprev_prev_sym\n" +
                            "a\t\t\n" +
                            "a\ta\t\n" +
                            "a\ta\ta\n"
            );
        });
    }

    @Test
    public void testLagSymbolOverPartitionChargesQueryMemoryLimit() throws Exception {
        // lag() over a high-cardinality partition key grows the function's partition map and
        // ring buffer. The streaming window factory holds no other growing state, so the breach
        // proves the SYMBOL function charges both to the per-query MemoryTracker.
        node1.setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 256 * 1024L);
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE tab AS (
                              SELECT x AS k, rnd_symbol('a', 'b', 'c') AS sym, x::TIMESTAMP AS ts
                              FROM long_sequence(100_000)
                            ) TIMESTAMP(ts) PARTITION BY DAY
                            """
            );
            try (
                    RecordCursorFactory factory = select("SELECT lag(sym) OVER (PARTITION BY k) FROM tab");
                    RecordCursor cursor = factory.getCursor(sqlExecutionContext)
            ) {
                while (cursor.hasNext()) {
                    // drain until breach
                }
                Assert.fail("expected per-query memory breach");
            } catch (CairoException e) {
                Assert.assertTrue("expected isOutOfMemory(), got: " + e.getFlyweightMessage(), e.isOutOfMemory());
                TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
            }
        });
    }

    @Test
    public void testLeadOffsetOverSymbol() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE balances (" +
                            "  sym SYMBOL," +
                            "  quantity DOUBLE," +
                            "  ts TIMESTAMP" +
                            ") TIMESTAMP(ts) PARTITION BY DAY"
            );
            execute(
                    "INSERT INTO balances VALUES" +
                            " ('a', 1.0, '2024-01-01T00:00:00.000000Z')," +
                            " ('a', 2.0, '2024-01-01T00:01:00.000000Z')," +
                            " ('a', 3.0, '2024-01-01T00:02:00.000000Z')"
            );

            assertQuery(
                    "SELECT sym," +
                            "   LEAD(sym, 1) OVER (PARTITION BY sym ORDER BY ts) AS next_sym," +
                            "   LEAD(sym, 2) OVER (PARTITION BY sym ORDER BY ts) AS next_next_sym" +
                            " FROM balances"
            ).expectSize().returns(
                    "sym\tnext_sym\tnext_next_sym\n" +
                            "a\ta\ta\n" +
                            "a\ta\t\n" +
                            "a\t\t\n"
            );
        });
    }

    @Test
    public void testNestedLagOverSymbol() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE balances (" +
                            "  sym SYMBOL," +
                            "  quantity DOUBLE," +
                            "  ts TIMESTAMP" +
                            ") TIMESTAMP(ts) PARTITION BY DAY"
            );
            execute(
                    "INSERT INTO balances VALUES" +
                            " ('a', 1.0, '2024-01-01T00:00:00.000000Z')," +
                            " ('a', 2.0, '2024-01-01T00:01:00.000000Z')," +
                            " ('a', 3.0, '2024-01-01T00:02:00.000000Z')"
            );

            assertQuery(
                    "WITH step1 AS (" +
                            "  SELECT ts, sym," +
                            "    LAG(sym) OVER (PARTITION BY sym ORDER BY ts) AS prev_sym" +
                            "  FROM balances" +
                            ")" +
                            " SELECT sym, prev_sym," +
                            "   LAG(prev_sym) OVER (PARTITION BY sym ORDER BY ts) AS prev_prev_sym" +
                            " FROM step1"
            ).noRandomAccess().expectSize().returns(
                    "sym\tprev_sym\tprev_prev_sym\n" +
                            "a\t\t\n" +
                            "a\ta\t\n" +
                            "a\ta\ta\n"
            );
        });
    }

    @Test
    public void testRejectsNonNullSymbolDefault() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE symbols (" +
                            "  sym SYMBOL," +
                            "  ts TIMESTAMP" +
                            ") TIMESTAMP(ts) PARTITION BY DAY"
            );

            assertQuery("select lag(sym, 1, 'x') over () from symbols")
                    .noLeakCheck()
                    .fails(19, "non-null default value is not supported for symbol lag");

            assertQuery("select lead(sym, 1, 'x') over () from symbols")
                    .noLeakCheck()
                    .fails(20, "non-null default value is not supported for symbol lead");
        });
    }

    @Test
    public void testTimestampValueFunctionsRejectSymbolArgument() throws Exception {
        // These functions have no SYMBOL variant. The function parser used to resolve them to
        // the TIMESTAMP factory through the implicit SYMBOL -> TIMESTAMP cast, which produced a
        // window column typed SYMBOL backed by a long.
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE symbols (" +
                            "  sym SYMBOL," +
                            "  ts TIMESTAMP" +
                            ") TIMESTAMP(ts) PARTITION BY DAY"
            );
            execute("INSERT INTO symbols VALUES ('a', '2024-01-01T00:00:00.000000Z')");

            assertQuery("SELECT first_value(sym) OVER () FROM symbols")
                    .noLeakCheck()
                    .fails(19, "there is no matching window function `first_value` with the argument type: SYMBOL");
            assertQuery("SELECT last_value(sym) OVER () FROM symbols")
                    .noLeakCheck()
                    .fails(18, "there is no matching window function `last_value` with the argument type: SYMBOL");
            assertQuery("SELECT max(sym) OVER (PARTITION BY sym) FROM symbols")
                    .noLeakCheck()
                    .fails(11, "there is no matching window function `max` with the argument type: SYMBOL");
            assertQuery("SELECT min(sym) OVER (PARTITION BY sym ORDER BY ts) FROM symbols")
                    .noLeakCheck()
                    .fails(11, "there is no matching window function `min` with the argument type: SYMBOL");
            assertQuery("SELECT nth_value(sym, 1) OVER () FROM symbols")
                    .noLeakCheck()
                    .fails(17, "there is no matching window function `nth_value` with the argument type: SYMBOL");

            // an explicit cast remains available
            assertQuery("SELECT max(ts::STRING::SYMBOL::TIMESTAMP) OVER () AS m FROM symbols").expectSize().returns(
                    "m\n" +
                            "2024-01-01T00:00:00.000000Z\n"
            );
        });
    }
}
