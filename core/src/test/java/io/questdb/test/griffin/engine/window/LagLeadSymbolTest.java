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
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class LagLeadSymbolTest extends AbstractCairoTest {

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
    public void testLagLeadOverDynamicSymbolCast() throws Exception {
        // Casts to SYMBOL from VARCHAR and from numeric or temporal types mint dictionary keys
        // while the scan runs, so a copy of the dictionary taken at init() is empty. lag()/lead() must resolve keys against the
        // live dictionary.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, x LONG, i INT, d DOUBLE, v VARCHAR) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:00.000000Z', 1, 10, 1.5, 'a'),
                    ('2024-01-01T00:00:01.000000Z', 2, 20, 2.5, 'b'),
                    ('2024-01-01T00:00:02.000000Z', 3, 30, 3.5, NULL),
                    ('2024-01-01T00:00:03.000000Z', 4, 40, 4.5, 'b'),
                    ('2024-01-01T00:00:04.000000Z', 5, 50, 5.5, 'c')
                    """);

            final String expectedUnpartitioned = """
                    x\tlag_x\tlead_x\tlag_v\tlead_v
                    1\t\t2\t\tb
                    2\t1\t3\ta\t
                    3\t2\t4\tb\tb
                    4\t3\t5\t\tc
                    5\t4\t\tb\t
                    """;
            assertQuery("""
                    SELECT x,
                        LAG(x::SYMBOL) OVER () lag_x,
                        LEAD(x::SYMBOL) OVER () lead_x,
                        LAG(v::SYMBOL) OVER () lag_v,
                        LEAD(v::SYMBOL) OVER () lead_v
                    FROM t
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns(expectedUnpartitioned);
            assertQuery("""
                    SELECT x,
                        LAG(x::SYMBOL) OVER (ORDER BY ts) lag_x,
                        LEAD(x::SYMBOL) OVER (ORDER BY ts) lead_x,
                        LAG(v::SYMBOL) OVER (ORDER BY ts) lag_v,
                        LEAD(v::SYMBOL) OVER (ORDER BY ts) lead_v
                    FROM t
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns(expectedUnpartitioned);

            final String expectedPartitioned = """
                    x\tlag_x\tlead_x\tlag_v\tlead_v
                    1\t\t3\t\t
                    2\t\t4\t\tb
                    3\t1\t5\ta\tc
                    4\t2\t\tb\t
                    5\t3\t\t\t
                    """;
            assertQuery("""
                    SELECT x,
                        LAG(x::SYMBOL) OVER (PARTITION BY x % 2) lag_x,
                        LEAD(x::SYMBOL) OVER (PARTITION BY x % 2) lead_x,
                        LAG(v::SYMBOL) OVER (PARTITION BY x % 2) lag_v,
                        LEAD(v::SYMBOL) OVER (PARTITION BY x % 2) lead_v
                    FROM t
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns(expectedPartitioned);
            assertQuery("""
                    SELECT x,
                        LAG(x::SYMBOL) OVER (PARTITION BY x % 2 ORDER BY ts) lag_x,
                        LEAD(x::SYMBOL) OVER (PARTITION BY x % 2 ORDER BY ts) lead_x,
                        LAG(v::SYMBOL) OVER (PARTITION BY x % 2 ORDER BY ts) lag_v,
                        LEAD(v::SYMBOL) OVER (PARTITION BY x % 2 ORDER BY ts) lead_v
                    FROM t
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns(expectedPartitioned);

            // the cast lives in a subquery, so the window reads a SYMBOL column whose dictionary
            // still grows during the scan
            assertQuery("SELECT k, LAG(k) OVER () prev FROM (SELECT x::SYMBOL k FROM t)")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            k\tprev
                            1\t
                            2\t1
                            3\t2
                            4\t3
                            5\t4
                            """);

            // streaming partitioned window over the INT and DOUBLE casts, which hand out
            // their snapshot from separate newSymbolTable() implementations
            assertQuery("""
                    SELECT x,
                        LAG(i::SYMBOL) OVER (PARTITION BY x % 2) lag_i,
                        LAG(d::SYMBOL) OVER (PARTITION BY x % 2) lag_d
                    FROM t
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            x\tlag_i\tlag_d
                            1\t\t
                            2\t\t
                            3\t10\t1.5
                            4\t20\t2.5
                            5\t30\t3.5
                            """);
            assertQuery("SELECT x, LAG(ts::SYMBOL) OVER () lag_ts FROM t")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            x\tlag_ts
                            1\t
                            2\t1704067200000000
                            3\t1704067201000000
                            4\t1704067202000000
                            5\t1704067203000000
                            """);

            // IGNORE NULLS skips the NULL that v::SYMBOL mints for the third row
            assertQuery("""
                    SELECT x,
                        LAG(v::SYMBOL) IGNORE NULLS OVER () lag_v,
                        LAG(v::SYMBOL) IGNORE NULLS OVER (PARTITION BY x % 2) lag_v_part
                    FROM t
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            x\tlag_v\tlag_v_part
                            1\t\t
                            2\ta\t
                            3\tb\ta
                            4\tb\tb
                            5\tb\ta
                            """);

            // downstream consumers resolve the window column through the table that the window
            // cursor forwards from newSymbolTable(), or through the window function itself
            assertQuery("SELECT x, LAG(p) OVER () pp FROM (SELECT x, LAG(x::SYMBOL) OVER () p FROM t)")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            x\tpp
                            1\t
                            2\t
                            3\t1
                            4\t2
                            5\t3
                            """);
            assertQuery("SELECT x, LEAD(p) OVER () np FROM (SELECT x, LEAD(x::SYMBOL) OVER () p FROM t)")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            x\tnp
                            1\t3
                            2\t4
                            3\t5
                            4\t
                            5\t
                            """);
            assertQuery("SELECT p, count() FROM (SELECT LAG(v::SYMBOL) OVER () p FROM t) ORDER BY p")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            p\tcount
                            \t2
                            a\t1
                            b\t2
                            """);
            assertQuery("SELECT x FROM (SELECT x, LAG(v::SYMBOL) OVER () p FROM t) WHERE p = 'b'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            x
                            3
                            5
                            """);
        });
    }

    @Test
    public void testLagLeadOverSymbolIgnoreNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE symbols (sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO symbols VALUES
                    ('a', '2024-01-01T00:00:00.000000Z'),
                    (NULL, '2024-01-01T00:01:00.000000Z'),
                    ('b', '2024-01-01T00:02:00.000000Z'),
                    (NULL, '2024-01-01T00:03:00.000000Z'),
                    ('c', '2024-01-01T00:04:00.000000Z')
                    """);

            assertQuery("""
                    SELECT sym,
                        LAG(sym, 1) IGNORE NULLS OVER (ORDER BY ts) AS prev_sym,
                        LEAD(sym, 1) IGNORE NULLS OVER (ORDER BY ts) AS next_sym
                    FROM symbols
                    """)
                    .expectSize()
                    .returns("""
                            sym\tprev_sym\tnext_sym
                            a\t\tb
                            \ta\tb
                            b\ta\tc
                            \tb\tc
                            c\tb\t
                            """);
        });
    }

    @Test
    public void testLagLeadOverSymbolIgnoreNullsOffsetTwo() throws Exception {
        // an offset above one keeps several ring slots; skipped NULLs must not advance the ring
        assertMemoryLeak(() -> {
            execute("CREATE TABLE symbols (sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO symbols VALUES
                    (NULL, '2024-01-01T00:00:00.000000Z'),
                    ('a', '2024-01-01T00:01:00.000000Z'),
                    (NULL, '2024-01-01T00:02:00.000000Z'),
                    ('b', '2024-01-01T00:03:00.000000Z'),
                    ('c', '2024-01-01T00:04:00.000000Z'),
                    (NULL, '2024-01-01T00:05:00.000000Z'),
                    ('d', '2024-01-01T00:06:00.000000Z')
                    """);

            assertQuery("""
                    SELECT sym,
                        LAG(sym, 2) IGNORE NULLS OVER (ORDER BY ts) AS prev_sym,
                        LEAD(sym, 2) IGNORE NULLS OVER (ORDER BY ts) AS next_sym
                    FROM symbols
                    """)
                    .expectSize()
                    .returns("""
                            sym\tprev_sym\tnext_sym
                            \t\tb
                            a\t\tc
                            \t\tc
                            b\t\td
                            c\ta\t
                            \tb\t
                            d\tb\t
                            """);
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
    public void testLagLeadSymbolEqualsNocacheHashCollision() throws Exception {
        // "Aa" and "BB" share a hash bucket. Symbol equality resolves the left key through the
        // reader's valueOf() and looks it up with keyOf() on the same reader; keyOf() must scan
        // the bucket through its own view, or the first candidate overwrites the left value and
        // "BB" matches the key of "Aa".
        assertMemoryLeak(() -> {
            execute("CREATE TABLE n (ts TIMESTAMP, s SYMBOL NOCACHE) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO n VALUES
                    ('2024-01-01T00:00:00', 'Aa'),
                    ('2024-01-01T01:00:00', 'BB'),
                    ('2024-01-01T02:00:00', 'Aa'),
                    ('2024-01-01T03:00:00', 'BB')
                    """);

            assertQuery("SELECT s, s = s eq FROM n")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            s\teq
                            Aa\ttrue
                            BB\ttrue
                            Aa\ttrue
                            BB\ttrue
                            """);
            // streaming window
            assertQuery("""
                    SELECT ts, s, l1, l2, l1 = l2 eq FROM (
                        SELECT ts, s, lag(s) OVER () l1, lag(s, 2) OVER () l2 FROM n
                    )
                    """)
                    .timestamp("ts")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            ts\ts\tl1\tl2\teq
                            2024-01-01T00:00:00.000000Z\tAa\t\t\ttrue
                            2024-01-01T01:00:00.000000Z\tBB\tAa\t\tfalse
                            2024-01-01T02:00:00.000000Z\tAa\tBB\tAa\tfalse
                            2024-01-01T03:00:00.000000Z\tBB\tAa\tBB\tfalse
                            """);
            // cached window
            assertQuery("""
                    SELECT ts, s, l1, l2, l1 = l2 eq FROM (
                        SELECT ts, s, lag(s) OVER (ORDER BY ts DESC) l1, lag(s, 2) OVER (ORDER BY ts DESC) l2 FROM n
                    ) ORDER BY ts
                    """)
                    .timestamp("ts")
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            ts\ts\tl1\tl2\teq
                            2024-01-01T00:00:00.000000Z\tAa\tBB\tAa\tfalse
                            2024-01-01T01:00:00.000000Z\tBB\tAa\tBB\tfalse
                            2024-01-01T02:00:00.000000Z\tAa\tBB\t\tfalse
                            2024-01-01T03:00:00.000000Z\tBB\t\t\ttrue
                            """);
        });
    }

    @Test
    public void testLagLeadSymbolEqualsOverDynamicSymbol() throws Exception {
        // rnd_symbol() has no static dictionary, so lag() resolves values through the argument's
        // valueOf()/valueBOf(). Both lag() columns share the argument's single A/B buffer pair,
        // and trim() reads its operand through the A view, so lag() must copy values that it
        // resolves through the argument into buffers it owns, or distinct values compare equal.
        // The seeded long_sequence() keeps the generated values stable across cursor re-reads.
        assertMemoryLeak(() -> {
            final String template = """
                    SELECT ts, l1::VARCHAR l1, l2::VARCHAR l2, l1 = l2 eq, l1 = trim(l2) eq_trim FROM (
                        SELECT ts,
                            lag(s, 1) OVER (ORDER BY ts#DIR#) l1,
                            lag(s, 2) OVER (ORDER BY ts#DIR#) l2
                        FROM (SELECT timestamp_sequence(0, 1_000) ts, rnd_symbol(4, 4, 4, 0) s FROM long_sequence(8, 42, 42))
                    )
                    """;
            assertQuery(template.replace("#DIR#", ""))
                    .expectSize()
                    .withPlanContaining("Window\n")
                    .returns("""
                            ts\tl1\tl2\teq\teq_trim
                            1970-01-01T00:00:00.000000Z\t\t\ttrue\ttrue
                            1970-01-01T00:00:00.001000Z\tTJOI\t\tfalse\tfalse
                            1970-01-01T00:00:00.002000Z\tRWNN\tTJOI\tfalse\tfalse
                            1970-01-01T00:00:00.003000Z\tTJOI\tRWNN\tfalse\tfalse
                            1970-01-01T00:00:00.004000Z\tCSVG\tTJOI\tfalse\tfalse
                            1970-01-01T00:00:00.005000Z\tFJWK\tCSVG\tfalse\tfalse
                            1970-01-01T00:00:00.006000Z\tTJOI\tFJWK\tfalse\tfalse
                            1970-01-01T00:00:00.007000Z\tTJOI\tTJOI\ttrue\ttrue
                            """);
            assertQuery(template.replace("#DIR#", " DESC"))
                    .expectSize()
                    .withPlanContaining("CachedWindow\n")
                    .returns("""
                            ts\tl1\tl2\teq\teq_trim
                            1970-01-01T00:00:00.000000Z\tRWNN\tTJOI\tfalse\tfalse
                            1970-01-01T00:00:00.001000Z\tTJOI\tCSVG\tfalse\tfalse
                            1970-01-01T00:00:00.002000Z\tCSVG\tFJWK\tfalse\tfalse
                            1970-01-01T00:00:00.003000Z\tFJWK\tTJOI\tfalse\tfalse
                            1970-01-01T00:00:00.004000Z\tTJOI\tTJOI\ttrue\ttrue
                            1970-01-01T00:00:00.005000Z\tTJOI\tCSVG\tfalse\tfalse
                            1970-01-01T00:00:00.006000Z\tCSVG\t\tfalse\tfalse
                            1970-01-01T00:00:00.007000Z\t\t\ttrue\ttrue
                            """);
        });
    }

    @Test
    public void testLagLeadSymbolEqualsSameTableValue() throws Exception {
        // BaseSymbolWindowFunction.init() gives lag()/lead() a private view of the argument's
        // dictionary. A non-cached dictionary keeps one A/B flyweight pair per view, so resolving
        // through the argument's own view would let the other side of = overwrite the window
        // value, and every row would compare equal. Comparing int keys never resolves a value, so
        // use sources that force the comparison onto resolved values: a UNION, a NOCACHE column
        // read through ::string, and a ::symbol cast over a STRING column (a CastToSymbolTable,
        // which the window reads through the argument).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, a SYMBOL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t2 (ts TIMESTAMP, a SYMBOL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE n (ts TIMESTAMP, a SYMBOL NOCACHE) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE s (ts TIMESTAMP, a STRING) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:00', 'a1'),
                    ('2024-01-01T01:00:00', 'a2'),
                    ('2024-01-01T02:00:00', 'a3'),
                    ('2024-01-01T03:00:00', 'a4')
                    """);
            execute("""
                    INSERT INTO t2 VALUES
                    ('2024-01-01T04:00:00', 'a4'),
                    ('2024-01-01T05:00:00', 'a1'),
                    ('2024-01-01T06:00:00', 'a2'),
                    ('2024-01-01T07:00:00', 'a2')
                    """);
            execute("INSERT INTO n SELECT * FROM (SELECT ts, a FROM t UNION ALL SELECT ts, a FROM t2)");
            execute("INSERT INTO s SELECT ts, a::string FROM n");

            // cached window over a UNION: symbol = symbol from the same record
            assertQuery("""
                    SELECT ts, a, ls FROM (
                        SELECT ts, a, lead(a) OVER (ORDER BY ts DESC) ls
                        FROM (SELECT ts, a FROM t UNION ALL SELECT ts, a FROM t2)
                    ) WHERE ls = a ORDER BY ts
                    """)
                    .timestamp("ts")
                    .noLeakCheck()
                    .returns("""
                            ts\ta\tls
                            2024-01-01T04:00:00.000000Z\ta4\ta4
                            2024-01-01T07:00:00.000000Z\ta2\ta2
                            """);
            assertQuery("""
                    SELECT ts, a, ls FROM (
                        SELECT ts, a, lead(a) OVER (ORDER BY ts DESC) ls
                        FROM (SELECT ts, a FROM t UNION ALL SELECT ts, a FROM t2)
                    ) WHERE ls != a ORDER BY ts
                    """)
                    .timestamp("ts")
                    .noLeakCheck()
                    .returns("""
                            ts\ta\tls
                            2024-01-01T00:00:00.000000Z\ta1\t
                            2024-01-01T01:00:00.000000Z\ta2\ta1
                            2024-01-01T02:00:00.000000Z\ta3\ta2
                            2024-01-01T03:00:00.000000Z\ta4\ta3
                            2024-01-01T05:00:00.000000Z\ta1\ta4
                            2024-01-01T06:00:00.000000Z\ta2\ta1
                            """);
            // streaming window over a UNION: two window columns resolved through the same table
            assertQuery("""
                    SELECT ts, a, l1, l2 FROM (
                        SELECT ts, a, lag(a) OVER () l1, lag(a, 2) OVER () l2
                        FROM (SELECT ts, a FROM t UNION ALL SELECT ts, a FROM t2)
                    ) WHERE l1 = l2
                    """)
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            ts\ta\tl1\tl2
                            2024-01-01T00:00:00.000000Z\ta1\t\t
                            2024-01-01T05:00:00.000000Z\ta1\ta4\ta4
                            """);
            // NOCACHE column: the reader hands out mapped views, and ::string bypasses the key path
            assertQuery("SELECT ts, a, x FROM (SELECT ts, a, lag(a) OVER () x FROM n) WHERE x = a::string")
                    .timestamp("ts")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            ts\ta\tx
                            2024-01-01T04:00:00.000000Z\ta4\ta4
                            2024-01-01T07:00:00.000000Z\ta2\ta2
                            """);
            // ::symbol over a STRING column: the cast function owns the dictionary
            assertQuery("""
                    SELECT ts, k, x FROM (
                        SELECT ts, k, lag(k) OVER () x FROM (SELECT ts, a::symbol k FROM s)
                    ) WHERE x = k
                    """)
                    .timestamp("ts")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            ts\tk\tx
                            2024-01-01T04:00:00.000000Z\ta4\ta4
                            2024-01-01T07:00:00.000000Z\ta2\ta2
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
    public void testLagLeadSymbolIsolatedFromSourceDictionaryView() throws Exception {
        // A NOCACHE dictionary keeps one A/B pair of flyweights per view. The window column
        // resolves through its own view, so a wrapper that reads the column through slot A
        // (trim, lower, concat) cannot overwrite the value the source column just returned.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE n (ts TIMESTAMP, a SYMBOL NOCACHE) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO n VALUES
                    ('2024-01-01T00:00:00', 'a1'),
                    ('2024-01-01T01:00:00', 'b1'),
                    ('2024-01-01T02:00:00', 'a1'),
                    ('2024-01-01T03:00:00', 'a1')
                    """);
            for (String wrapper : List.of("trim(x)", "lower(x)", "concat(x, '')", "x::string")) {
                assertQuery("SELECT ts FROM (SELECT ts, a, lag(a) OVER () x FROM n) WHERE a = " + wrapper)
                        .timestamp("ts")
                        .noRandomAccess()
                        .noLeakCheck()
                        .returns("""
                                ts
                                2024-01-01T03:00:00.000000Z
                                """);
                assertQuery("SELECT ts FROM (SELECT ts, a, lag(a) OVER () x FROM n) WHERE a::string = " + wrapper)
                        .timestamp("ts")
                        .noRandomAccess()
                        .noLeakCheck()
                        .returns("""
                                ts
                                2024-01-01T03:00:00.000000Z
                                """);
            }
            assertQuery("""
                    SELECT ts, starts_with(a::string, trim(x)) sw, nullif(x, trim(a::string)) nf, lpad(a::string, 4, trim(x)) lp
                    FROM (SELECT ts, a, lag(a) OVER () x FROM n)
                    """)
                    .timestamp("ts")
                    .expectSize()
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            ts\tsw\tnf\tlp
                            2024-01-01T00:00:00.000000Z\tfalse\t\t
                            2024-01-01T01:00:00.000000Z\tfalse\ta1\ta1b1
                            2024-01-01T02:00:00.000000Z\tfalse\tb1\tb1a1
                            2024-01-01T03:00:00.000000Z\ttrue\t\ta1a1
                            """);
            // two window columns over one dictionary, neither aliases the other; the first row
            // has no neighbor on either side and NULL = NULL holds
            assertQuery("""
                    SELECT ts FROM (SELECT ts, lag(a) OVER () x, lag(a, 2) OVER () y FROM n) WHERE trim(x) = y
                    """)
                    .timestamp("ts")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            ts
                            2024-01-01T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testLagLeadSymbolNonLightCachedWindow() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, false);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE balances (sym SYMBOL, quantity DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO balances VALUES
                    ('a', 1.0, '2024-01-01T00:00:00.000000Z'),
                    ('b', 2.0, '2024-01-01T00:01:00.000000Z'),
                    ('c', 3.0, '2024-01-01T00:02:00.000000Z')
                    """);

            assertQuery("""
                    SELECT sym,
                        LAG(sym, 1) OVER (ORDER BY ts) AS prev_sym,
                        LEAD(sym, 1) OVER (ORDER BY ts) AS next_sym
                    FROM balances
                    """)
                    .expectSize()
                    .withPlanContaining("CachedWindow\n")
                    .returns("""
                            sym\tprev_sym\tnext_sym
                            a\t\tb
                            b\ta\tc
                            c\tb\t
                            """);
        });
    }

    @Test
    public void testLagLeadSymbolNullDefault() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE symbols (sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO symbols VALUES
                    ('a', '2024-01-01T00:00:00.000000Z'),
                    ('b', '2024-01-01T00:01:00.000000Z'),
                    ('c', '2024-01-01T00:02:00.000000Z')
                    """);

            assertQuery("""
                    SELECT sym,
                        LAG(sym, 1, NULL) OVER (ORDER BY ts) AS prev_sym,
                        LEAD(sym, 1, NULL) OVER (ORDER BY ts) AS next_sym
                    FROM symbols
                    """)
                    .expectSize()
                    .returns("""
                            sym\tprev_sym\tnext_sym
                            a\t\tb
                            b\ta\tc
                            c\tb\t
                            """);
        });
    }

    @Test
    public void testLagLeadSymbolNullEquality() throws Exception {
        // lag()/lead() synthesize NULL for missing neighbors. These SYMBOL values must compare
        // like STRING values, including NULL = NULL.
        // EqSymFunctionFactoryTest.testNullFromOuterJoinMatchesNull guards NULL comparison when
        // the source dictionary reports no NULL.
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
    public void testLagLeadSymbolOverPartitionExpression() throws Exception {
        assertMemoryLeak(() -> {
            createPartitionedSymbols();
            for (String function : List.of("lag", "lead")) {
                String expected = function.equals("lag")
                        ? """
                        id\tneighbor
                        1\t
                        2\t
                        3\ta
                        4\tx
                        5\t
                        6\t
                        7\tb
                        8\ty
                        9\tc
                        10\tz
                        """
                        : """
                        id\tneighbor
                        1\t
                        2\t
                        3\tb
                        4\ty
                        5\tc
                        6\tz
                        7\td
                        8\tw
                        9\t
                        10\t
                        """;
                for (String orderBy : List.of("", " ORDER BY id")) {
                    boolean isCached = function.equals("lead") || !orderBy.isEmpty();
                    assertQuery("SELECT id, " + function + "(sym) OVER (PARTITION BY grp::SYMBOL" + orderBy + ") neighbor FROM partitioned_symbols")
                            .noLeakCheck()
                            .expectSize()
                            .supportsRandomAccess(isCached)
                            .withPlanContaining(isCached ? "CachedWindowLight" : "Window\n")
                            .returns(expected);
                }
            }
        });
    }

    @Test
    public void testLagLeadSymbolOverPartitionIgnoreNulls() throws Exception {
        assertMemoryLeak(() -> {
            createPartitionedSymbols();
            for (String function : List.of("lag", "lead")) {
                assertQuery("SELECT id, " +
                        function + "(sym) IGNORE NULLS OVER (PARTITION BY grp) skipped, " +
                        function + "(sym) RESPECT NULLS OVER (PARTITION BY grp) kept FROM partitioned_symbols")
                        .noLeakCheck()
                        .expectSize()
                        .supportsRandomAccess(function.equals("lead"))
                        .withPlanContaining(function.equals("lead") ? "CachedWindowLight" : "Window\n")
                        .returns(function.equals("lag")
                                ? """
                                id\tskipped\tkept
                                1\t\t
                                2\t\t
                                3\ta\ta
                                4\tx\tx
                                5\ta\t
                                6\tx\t
                                7\tb\tb
                                8\ty\ty
                                9\tc\tc
                                10\tz\tz
                                """
                                : """
                                id\tskipped\tkept
                                1\tb\t
                                2\ty\t
                                3\tb\tb
                                4\ty\ty
                                5\tc\tc
                                6\tz\tz
                                7\td\td
                                8\tw\tw
                                9\t\t
                                10\t\t
                                """);
            }
        });
    }

    @Test
    public void testLagLeadSymbolOverPartitionIgnoreNullsOffsetTwo() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE symbols (id INT, grp SYMBOL, sym SYMBOL)");
            // Group A starts and ends with NULL to exercise initialization in both scan directions.
            // Interior NULLs must not advance the multi-slot rings in either group.
            execute("""
                    INSERT INTO symbols VALUES
                    (1, 'A', NULL),
                    (2, 'B', 'v21'),
                    (3, 'A', 'v11'),
                    (4, 'B', NULL),
                    (5, 'A', NULL),
                    (6, 'B', 'v22'),
                    (7, 'A', 'v12'),
                    (8, 'B', 'v23'),
                    (9, 'A', 'v13'),
                    (10, 'B', NULL),
                    (11, 'A', NULL),
                    (12, 'B', 'v24')
                    """);

            assertQuery("SELECT id, LAG(sym, 2) IGNORE NULLS OVER (PARTITION BY grp) AS prev_sym FROM symbols")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .withPlanContaining("Window\n")
                    .returns("""
                            id\tprev_sym
                            1\t
                            2\t
                            3\t
                            4\t
                            5\t
                            6\t
                            7\t
                            8\tv21
                            9\tv11
                            10\tv22
                            11\tv12
                            12\tv22
                            """);
            assertQuery("""
                    SELECT id, grp, sym,
                        LAG(sym, 2) IGNORE NULLS OVER (PARTITION BY grp) AS prev_sym,
                        LEAD(sym, 2) IGNORE NULLS OVER (PARTITION BY grp) AS next_sym
                    FROM symbols
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .withPlanContaining("CachedWindowLight")
                    .returns("""
                            id\tgrp\tsym\tprev_sym\tnext_sym
                            1\tA\t\t\tv12
                            2\tB\tv21\t\tv23
                            3\tA\tv11\t\tv13
                            4\tB\t\t\tv23
                            5\tA\t\t\tv13
                            6\tB\tv22\t\tv24
                            7\tA\tv12\t\t
                            8\tB\tv23\tv21\t
                            9\tA\tv13\tv11\t
                            10\tB\t\tv22\t
                            11\tA\t\tv12\t
                            12\tB\tv24\tv22\t
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
            createPartitionedSymbols();
            assertQuery("""
                    SELECT id,
                        LAG(sym, 1) OVER (PARTITION BY grp ORDER BY ts) prev_sym,
                        LAG(sym, 2) OVER (PARTITION BY grp ORDER BY ts) prev_prev_sym
                    FROM partitioned_symbols
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .withPlanContaining("Window\n")
                    .returns("""
                            id\tprev_sym\tprev_prev_sym
                            1\t\t
                            2\t\t
                            3\ta\t
                            4\tx\t
                            5\t\ta
                            6\t\tx
                            7\tb\t
                            8\ty\t
                            9\tc\tb
                            10\tz\ty
                            """);
            assertQuery("SELECT id, LAG(sym, 2) OVER () prev_sym FROM partitioned_symbols")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .withPlanContaining("Window\n")
                    .returns("""
                            id\tprev_sym
                            1\t
                            2\t
                            3\ta
                            4\tx
                            5\t
                            6\t
                            7\tb
                            8\ty
                            9\tc
                            10\tz
                            """);
        });
    }

    @Test
    public void testLagSymbolOverPartitionChargesQueryMemoryLimit() throws Exception {
        // lag() over a high-cardinality partition key grows the function's partition map and
        // ring buffer. Either allocation can breach this limit; the allocation accounting tests
        // below separately guard against losing either tracker attachment.
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
    public void testLagSymbolOverPartitionTracksMapAndRingAllocations() throws Exception {
        assertPartitionTracksMapAndRingAllocations("lag");
    }

    @Test
    public void testLagSymbolZeroOffset() throws Exception {
        assertZeroOffset("lag");
    }

    @Test
    public void testLeadOffsetOverSymbol() throws Exception {
        assertMemoryLeak(() -> {
            createPartitionedSymbols();
            assertQuery("""
                    SELECT id,
                        LEAD(sym, 1) OVER (PARTITION BY grp ORDER BY ts) next_sym,
                        LEAD(sym, 2) OVER (PARTITION BY grp ORDER BY ts) next_next_sym
                    FROM partitioned_symbols
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .withPlanContaining("CachedWindowLight")
                    .returns("""
                            id\tnext_sym\tnext_next_sym
                            1\t\tb
                            2\t\ty
                            3\tb\tc
                            4\ty\tz
                            5\tc\td
                            6\tz\tw
                            7\td\t
                            8\tw\t
                            9\t\t
                            10\t\t
                            """);
            assertQuery("SELECT id, LEAD(sym, 2) OVER () next_sym FROM partitioned_symbols")
                    .noLeakCheck()
                    .expectSize()
                    .withPlanContaining("CachedWindowLight")
                    .returns("""
                            id\tnext_sym
                            1\t
                            2\t
                            3\tb
                            4\ty
                            5\tc
                            6\tz
                            7\td
                            8\tw
                            9\t
                            10\t
                            """);
        });
    }

    @Test
    public void testLeadSymbolOverPartitionTracksMapAndRingAllocations() throws Exception {
        assertPartitionTracksMapAndRingAllocations("lead");
    }

    @Test
    public void testLeadSymbolZeroOffset() throws Exception {
        assertZeroOffset("lead");
    }

    @Test
    public void testNestedLagOverRndSymbol() throws Exception {
        // rnd_symbol(count, lo, hi, nullRate) hands out no symbol table, so the inner lag() copies
        // the values it resolves through its argument into buffers it owns. The outer window
        // resolves its keys through the inner function, which must not overwrite the p value
        // that p = trim(pp) already holds. The generated values carry no whitespace, so eq_trim
        // must match eq on every row. The seeded long_sequence() and the cached windows keep the
        // generated values stable across cursor re-reads.
        assertMemoryLeak(() -> {
            final String template = """
                    SELECT ts, p, pp, p = pp eq, p = trim(pp) eq_trim FROM (
                        SELECT ts, p, #OUTER# pp FROM (
                            SELECT ts, lag(s) OVER (ORDER BY ts) p
                            FROM (SELECT timestamp_sequence(0, 1_000) ts, rnd_symbol(4, 4, 4, 0) s FROM long_sequence(8, 42, 42))
                        )
                    )
                    """;
            final String expected = """
                    ts\tp\tpp\teq\teq_trim
                    1970-01-01T00:00:00.000000Z\t\t\ttrue\ttrue
                    1970-01-01T00:00:00.001000Z\tTJOI\t\tfalse\tfalse
                    1970-01-01T00:00:00.002000Z\tRWNN\tTJOI\tfalse\tfalse
                    1970-01-01T00:00:00.003000Z\tTJOI\tRWNN\tfalse\tfalse
                    1970-01-01T00:00:00.004000Z\tCSVG\tTJOI\tfalse\tfalse
                    1970-01-01T00:00:00.005000Z\tFJWK\tCSVG\tfalse\tfalse
                    1970-01-01T00:00:00.006000Z\tTJOI\tFJWK\tfalse\tfalse
                    1970-01-01T00:00:00.007000Z\tTJOI\tTJOI\ttrue\ttrue
                    """;
            assertQuery(template.replace("#OUTER#", "lag(p) OVER (ORDER BY ts)"))
                    .noLeakCheck()
                    .expectSize()
                    .withPlanContaining("CachedWindow\n")
                    .returns(expected);
            // lead() over a descending order reads the same neighbor as lag() over the ascending one
            assertQuery(template.replace("#OUTER#", "lead(p) OVER (ORDER BY ts DESC)"))
                    .noLeakCheck()
                    .expectSize()
                    .withPlanContaining("CachedWindow\n")
                    .returns(expected);
        });
    }

    @Test
    public void testNestedLagOverSymbol() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE balances (sym SYMBOL, quantity DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO balances VALUES
                    ('a', 1.0, '2024-01-01T00:00:00.000000Z'),
                    ('a', 2.0, '2024-01-01T00:01:00.000000Z'),
                    ('a', 3.0, '2024-01-01T00:02:00.000000Z')
                    """);

            assertQuery("""
                    WITH step1 AS (
                        SELECT ts, sym, LAG(sym) OVER (PARTITION BY sym ORDER BY ts) AS prev_sym
                        FROM balances
                    )
                    SELECT sym, prev_sym, LAG(prev_sym) OVER (PARTITION BY sym ORDER BY ts) AS prev_prev_sym
                    FROM step1
                    """)
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            sym\tprev_sym\tprev_prev_sym
                            a\t\t
                            a\ta\t
                            a\ta\ta
                            """);
        });
    }

    @Test
    public void testNestedLagOverSymbolCast() throws Exception {
        // p = trim(pp) reads p, then resolves pp through the outer lag(), which resolves its key
        // through the inner lag(). trim() reads pp through the A view, as the comparison read p,
        // so resolving pp must not overwrite the p value the comparison already holds.
        assertMemoryLeak(() -> {
            createNestedLagTable();
            assertQuery("""
                    SELECT id, p, pp FROM (
                        SELECT id, p, lag(p) OVER () pp
                        FROM (SELECT id, lag(v::SYMBOL) OVER () p FROM t)
                    ) WHERE p = trim(pp)
                    """)
                    .noRandomAccess()
                    .returns("""
                            id\tp\tpp
                            1\t\t
                            5\tbb\t bb
                            """);
        });
    }

    @Test
    public void testNestedLagOverSymbolCastCachedWindow() throws Exception {
        assertNestedLagOverSymbolCastCachedWindow("CachedWindowLight\n");
    }

    @Test
    public void testNestedLagOverSymbolCastFromEachType() throws Exception {
        // Every cast to SYMBOL that hands out a CastToSymbolTable takes the VARCHAR path of
        // testNestedLagOverSymbolCast. Each column holds a, b, b, c: row 3 compares b against a
        // and must not match, row 4 compares b against b and must.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (id LONG, bo BOOLEAN, bt BYTE, sh SHORT, ch CHAR, i INT, l LONG, f FLOAT, d DOUBLE, dt DATE, ts TIMESTAMP, v VARCHAR)");
            execute("""
                    INSERT INTO t VALUES
                    (1, true, 1, 1, 'a', 1, 1, 1.5, 1.5, '2024-01-01', '2024-01-01', 'a'),
                    (2, false, 2, 2, 'b', 2, 2, 2.5, 2.5, '2024-01-02', '2024-01-02', 'b'),
                    (3, false, 2, 2, 'b', 2, 2, 2.5, 2.5, '2024-01-02', '2024-01-02', 'b'),
                    (4, true, 3, 3, 'c', 3, 3, 3.5, 3.5, '2024-01-03', '2024-01-03', 'c')
                    """);
            // the column, then the text its cast gives b
            final String[][] sources = {
                    {"bo", "false"},
                    {"bt", "2"},
                    {"sh", "2"},
                    {"ch", "b"},
                    {"i", "2"},
                    {"l", "2"},
                    {"f", "2.5"},
                    {"d", "2.5"},
                    {"dt", "1704153600000"},
                    {"ts", "1704153600000000"},
                    {"v", "b"}
            };
            for (String[] source : sources) {
                assertQuery("""
                        SELECT id, p, pp FROM (
                            SELECT id, p, lag(p) OVER () pp
                            FROM (SELECT id, lag(#COLUMN#::SYMBOL) OVER () p FROM t)
                        ) WHERE p = trim(pp)
                        """.replace("#COLUMN#", source[0]))
                        .noLeakCheck()
                        .noRandomAccess()
                        .returns("""
                                id\tp\tpp
                                1\t\t
                                4\t#B#\t#B#
                                """.replace("#B#", source[1]));
            }
        });
    }

    @Test
    public void testNestedLagOverSymbolCastNonLightCachedWindow() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, false);
        assertNestedLagOverSymbolCastCachedWindow("CachedWindow\n");
    }

    @Test
    public void testNestedLagOverSymbolCastPartitioned() throws Exception {
        // g holds a single value, so every PARTITION BY arrangement returns the rows of
        // testNestedLagOverSymbolCast.
        assertMemoryLeak(() -> {
            createNestedLagTable();
            final String template = """
                    SELECT id, p, pp FROM (
                        SELECT id, p, #OUTER# pp
                        FROM (SELECT id, g, #INNER# p FROM t)
                    ) WHERE p = trim(pp)
                    """;
            final String expected = """
                    id\tp\tpp
                    1\t\t
                    5\tbb\t bb
                    """;
            // partitioned outer window
            assertQuery(template.replace("#OUTER#", "lag(p) OVER (PARTITION BY g)").replace("#INNER#", "lag(v::SYMBOL) OVER ()"))
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns(expected);
            // partitioned inner window
            assertQuery(template.replace("#OUTER#", "lag(p) OVER ()").replace("#INNER#", "lag(v::SYMBOL) OVER (PARTITION BY g)"))
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns(expected);
            // both windows partitioned and streaming
            assertQuery(template.replace("#OUTER#", "lag(p) OVER (PARTITION BY g)").replace("#INNER#", "lag(v::SYMBOL) OVER (PARTITION BY g)"))
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns(expected);
            // both windows partitioned and cached: lead() over a descending order reads the
            // same neighbor as lag() over the ascending one
            assertQuery(template.replace("#OUTER#", "lead(p) OVER (PARTITION BY g ORDER BY id DESC)").replace("#INNER#", "lead(v::SYMBOL) OVER (PARTITION BY g ORDER BY id DESC)"))
                    .noLeakCheck()
                    .withPlanContaining("CachedWindowLight\n")
                    .returns(expected);
        });
    }

    @Test
    public void testNestedLagOverSymbolCastStringFunctions() throws Exception {
        // trim(), lower() and upper() read pp through the A view that the comparison already
        // read p through, and nullif() and starts_with() read both arguments through it. From
        // row 3 on, each row makes exactly one comparison true, and nullif() must return p.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (id LONG, v VARCHAR)");
            execute("""
                    INSERT INTO t VALUES
                    (1, ' bb'),
                    (2, 'bb'),
                    (3, 'BB'),
                    (4, 'bb'),
                    (5, 'bbc'),
                    (6, 'x')
                    """);
            assertQuery("""
                    SELECT id, p, pp,
                        p = trim(pp) eq_trim,
                        p = lower(pp) eq_lower,
                        p = upper(pp) eq_upper,
                        nullif(p, pp) nullif_pp,
                        starts_with(p, pp) starts_with_pp
                    FROM (
                        SELECT id, p, lag(p) OVER () pp
                        FROM (SELECT id, lag(v::SYMBOL) OVER () p FROM t)
                    )
                    """)
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            id\tp\tpp\teq_trim\teq_lower\teq_upper\tnullif_pp\tstarts_with_pp
                            1\t\t\ttrue\ttrue\ttrue\t\tfalse
                            2\t bb\t\tfalse\tfalse\tfalse\t bb\tfalse
                            3\tbb\t bb\ttrue\tfalse\tfalse\tbb\tfalse
                            4\tBB\tbb\tfalse\tfalse\ttrue\tBB\tfalse
                            5\tbb\tBB\tfalse\ttrue\tfalse\tbb\tfalse
                            6\tbbc\tbb\tfalse\tfalse\tfalse\tbbc\ttrue
                            """);
        });
    }

    @Test
    public void testNestedLagOverSymbolWithOwnSymbolTable() throws Exception {
        // A SYMBOL column, STRING::SYMBOL and the list form of rnd_symbol() hand lag() a symbol
        // table of its own, so the outer lag() resolves pp without touching the p value that
        // p = trim(pp) already holds.
        assertMemoryLeak(() -> {
            createNestedLagTable();
            execute("CREATE TABLE s AS (SELECT id, v::SYMBOL sym FROM t)");
            final String expected = """
                    id\tp\tpp
                    1\t\t
                    5\tbb\t bb
                    """;
            assertQuery("""
                    SELECT id, p, pp FROM (
                        SELECT id, p, lag(p) OVER () pp
                        FROM (SELECT id, lag(sym) OVER () p FROM s)
                    ) WHERE p = trim(pp)
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns(expected);
            assertQuery("""
                    SELECT id, p, pp FROM (
                        SELECT id, p, lag(p) OVER () pp
                        FROM (SELECT id, lag(v::STRING::SYMBOL) OVER () p FROM t)
                    ) WHERE p = trim(pp)
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns(expected);
            // the seeded long_sequence() and the cached windows keep the generated values stable
            // across cursor re-reads
            assertQuery("""
                    SELECT ts, p, pp, p = pp eq, p = trim(pp) eq_trim FROM (
                        SELECT ts, p, lag(p) OVER (ORDER BY ts) pp FROM (
                            SELECT ts, lag(s) OVER (ORDER BY ts) p
                            FROM (SELECT timestamp_sequence(0, 1_000) ts, rnd_symbol('a', 'b', 'c') s FROM long_sequence(8, 42, 42))
                        )
                    )
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            ts\tp\tpp\teq\teq_trim
                            1970-01-01T00:00:00.000000Z\t\t\ttrue\ttrue
                            1970-01-01T00:00:00.001000Z\ta\t\tfalse\tfalse
                            1970-01-01T00:00:00.002000Z\tc\ta\tfalse\tfalse
                            1970-01-01T00:00:00.003000Z\tb\tc\tfalse\tfalse
                            1970-01-01T00:00:00.004000Z\tc\tb\tfalse\tfalse
                            1970-01-01T00:00:00.005000Z\tb\tc\tfalse\tfalse
                            1970-01-01T00:00:00.006000Z\tc\tb\tfalse\tfalse
                            1970-01-01T00:00:00.007000Z\tc\tc\ttrue\ttrue
                            """);
        });
    }

    @Test
    public void testRejectsNonNullSymbolDefault() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE symbols (sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");

            assertQuery("SELECT lag(sym, 1, 'x') OVER () FROM symbols")
                    .noLeakCheck()
                    .fails(19, "non-null default value is not supported for symbol lag");

            assertQuery("SELECT lead(sym, 1, 'x') OVER () FROM symbols")
                    .noLeakCheck()
                    .fails(20, "non-null default value is not supported for symbol lead");
        });
    }

    private void assertNestedLagOverSymbolCastCachedWindow(String cachedWindowPlan) throws Exception {
        // lead() over a descending order reads the same neighbor as lag() over the ascending one,
        // so each ordered arrangement returns the rows of testNestedLagOverSymbolCast.
        assertMemoryLeak(() -> {
            createNestedLagTable();
            final String template = """
                    SELECT id, p, pp FROM (
                        SELECT id, p, #OUTER# pp
                        FROM (SELECT id, #INNER# p FROM t)
                    ) WHERE p = trim(pp)
                    """;
            final String expected = """
                    id\tp\tpp
                    1\t\t
                    5\tbb\t bb
                    """;
            // cached outer window over a streaming inner one: the streaming window offers no
            // random access, so the outer window never takes the light path
            assertQuery(template.replace("#OUTER#", "lead(p) OVER (ORDER BY id DESC)").replace("#INNER#", "lag(v::SYMBOL) OVER ()"))
                    .noLeakCheck()
                    .withPlanContaining("CachedWindow\n")
                    .returns(expected);
            // both windows cached
            assertQuery(template.replace("#OUTER#", "lead(p) OVER (ORDER BY id DESC)").replace("#INNER#", "lead(v::SYMBOL) OVER (ORDER BY id DESC)"))
                    .noLeakCheck()
                    .withPlanContaining(cachedWindowPlan)
                    .returns(expected);
            // streaming outer window over a cached inner one
            assertQuery(template.replace("#OUTER#", "lag(p) OVER ()").replace("#INNER#", "lead(v::SYMBOL) OVER (ORDER BY id DESC)"))
                    .noLeakCheck()
                    .noRandomAccess()
                    .withPlanContaining(cachedWindowPlan)
                    .returns(expected);
            // unordered lead() over lead() reads the following rows: only the last row, where
            // both sides are NULL, matches
            assertQuery(template.replace("#OUTER#", "lead(p) OVER ()").replace("#INNER#", "lead(v::SYMBOL) OVER ()"))
                    .noLeakCheck()
                    .withPlanContaining(cachedWindowPlan)
                    .returns("""
                            id\tp\tpp
                            5\t\t
                            """);
        });
    }

    private void assertPartitionTracksMapAndRingAllocations(String function) throws Exception {
        node1.setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, false);
        node1.setProperty(PropertyKey.CAIRO_SQL_WINDOW_STORE_PAGE_SIZE, 4 * 1024L);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab AS (SELECT x AS k, 'a'::SYMBOL AS sym FROM long_sequence(10_000))");
            String query = "SELECT " + function + "(sym) OVER (PARTITION BY k) FROM tab";
            assertQuery(query).noLeakCheck().assertsPlanContaining("lag".equals(function) ? "Window\n" : "CachedWindow\n");
            try (RecordCursorFactory factory = select(query)) {
                long mapBaseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_UNORDERED_MAP);
                long ringBaseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_CIRCULAR_BUFFER);
                long chainBaseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_RECORD_CHAIN);
                MemoryTracker tracker;
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    tracker = sqlExecutionContext.getMemoryTracker();
                    Assert.assertNotNull(tracker);
                    long rows = 0;
                    while (cursor.hasNext()) {
                        rows++;
                    }
                    Assert.assertEquals(10_000, rows);
                    long mapBytes = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_UNORDERED_MAP) - mapBaseline;
                    long ringBytes = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_CIRCULAR_BUFFER) - ringBaseline;
                    long chainBytes = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_RECORD_CHAIN) - chainBaseline;
                    Assert.assertTrue("partition map allocated", mapBytes > 0);
                    Assert.assertTrue("partition rings grew beyond one page", ringBytes > 4 * 1024);
                    // Account for lead's cached rows explicitly: their charge must not hide an
                    // untracked map or ring. The streaming lag cursor has no record chain.
                    Assert.assertEquals("map and ring allocations charged", mapBytes + ringBytes, tracker.getUsed() - chainBytes);
                }
                Assert.assertEquals("query memory released", 0, tracker.getUsed());
                Assert.assertEquals(mapBaseline, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_UNORDERED_MAP));
                Assert.assertEquals(ringBaseline, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_CIRCULAR_BUFFER));
                Assert.assertEquals(chainBaseline, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_RECORD_CHAIN));
            }
        });
    }

    private void assertZeroOffset(String function) throws Exception {
        assertMemoryLeak(() -> {
            createPartitionedSymbols();
            for (String over : List.of("", "PARTITION BY grp", "PARTITION BY grp ORDER BY id DESC")) {
                for (String nullTreatment : List.of("", "IGNORE NULLS")) {
                    assertQuery("SELECT id, " + function + "(sym, 0) " + nullTreatment + " OVER (" + over + ") current_sym FROM partitioned_symbols ORDER BY id")
                            .noLeakCheck()
                            .expectSize()
                            .withPlanContaining(over.contains("ORDER BY") ? "CachedWindowLight" : "Window\n")
                            .returns("""
                                    id\tcurrent_sym
                                    1\ta
                                    2\tx
                                    3\t
                                    4\t
                                    5\tb
                                    6\ty
                                    7\tc
                                    8\tz
                                    9\td
                                    10\tw
                                    """);
                }
            }
        });
    }

    private void createNestedLagTable() throws Exception {
        execute("CREATE TABLE t (id LONG, v VARCHAR, g SYMBOL)");
        execute("""
                INSERT INTO t VALUES
                (1, 'x', 'g'),
                (2, 'long value 0123456789', 'g'),
                (3, ' bb', 'g'),
                (4, 'bb', 'g'),
                (5, 'a', 'g')
                """);
    }

    private void createPartitionedSymbols() throws Exception {
        execute("CREATE TABLE partitioned_symbols (id INT, grp STRING, sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        // Interleaved groups, distinct values and NULLs make wrong keys and ring indices visible.
        execute("""
                INSERT INTO partitioned_symbols VALUES
                (1, 'a', 'a', '2024-01-01T00:00:01'),
                (2, 'b', 'x', '2024-01-01T00:00:02'),
                (3, 'a', NULL, '2024-01-01T00:00:03'),
                (4, 'b', NULL, '2024-01-01T00:00:04'),
                (5, 'a', 'b', '2024-01-01T00:00:05'),
                (6, 'b', 'y', '2024-01-01T00:00:06'),
                (7, 'a', 'c', '2024-01-01T00:00:07'),
                (8, 'b', 'z', '2024-01-01T00:00:08'),
                (9, 'a', 'd', '2024-01-01T00:00:09'),
                (10, 'b', 'w', '2024-01-01T00:00:10')
                """);
    }
}
