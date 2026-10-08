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

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.mp.WorkerPool;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.QueryAssertion;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * cairo.sql.distinct.groupby.key.pruning.enabled: a group by sub-query read by a DISTINCT (or a GROUP BY
 * without aggregates) that selects only some of its keys groups by those keys alone (TAQ idx 29). Each
 * query runs with the pruning off first; with it on, the output must be the same rows. The queries order
 * their output, because the order of DISTINCT values is unspecified and the pruned plan changes it.
 */
public class DistinctGroupByKeyPruningTest extends AbstractCairoTest {
    private static final String A = "SELECT timestamp_floor('10m', ts) AS minute, sym, avg(bid) AS v FROM quote WHERE sym IN ('A', 'B', 'C', 'Z')";
    private static final String[] PRUNED = {
            // TAQ idx 29: m and s
            "WITH a AS (" + A + ") SELECT DISTINCT sym FROM a ORDER BY sym",
            "WITH a AS (" + A + ") SELECT DISTINCT minute FROM a ORDER BY minute",
            // a GROUP BY without aggregates as the consumer, an expression of a key
            "WITH a AS (" + A + ") SELECT sym FROM a GROUP BY sym ORDER BY sym",
            "WITH a AS (" + A + ") SELECT DISTINCT lower(sym) AS l FROM a ORDER BY l",
            // an explicit inner DISTINCT, NULL keys
            "SELECT DISTINCT sym FROM (SELECT DISTINCT sym, bsize FROM quote) ORDER BY sym",
            // nested implicit group bys
            "SELECT DISTINCT sym FROM (SELECT sym, b FROM (SELECT sym, bsize % 3 AS b, max(bid) mx FROM quote)) ORDER BY sym",
            // TAQ idx 29 in full
            "WITH a AS (" + A + "), m AS (SELECT DISTINCT minute FROM a), s AS (SELECT DISTINCT sym FROM a), "
                    + "g AS (SELECT m.minute, s.sym, a.v FROM m CROSS JOIN s LEFT JOIN a ON a.minute = m.minute AND a.sym = s.sym), "
                    + "ff AS (SELECT minute, sym, last_value(v) IGNORE NULLS OVER (PARTITION BY sym ORDER BY minute ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS v FROM g) "
                    + "SELECT * FROM ff PIVOT (first(v) FOR sym IN ('A', 'B', 'C', 'Z') GROUP BY minute) ORDER BY minute",
    };
    private static final String[] NOT_PRUNED = {
            // a filter reads the other key, so it stays
            "WITH a AS (" + A + ") SELECT DISTINCT sym FROM a WHERE minute > '1970-01-01T01:00:00' ORDER BY sym",
            // an explicit GROUP BY clause is left as written
            "SELECT DISTINCT sym FROM (SELECT sym, b, count() c FROM (SELECT sym, bsize % 3 AS b, max(bid) mx FROM quote GROUP BY sym, b) GROUP BY sym, b) ORDER BY sym",
            // the consumer counts groups
            "WITH a AS (" + A + ") SELECT sym, count() FROM a ORDER BY sym",
            "WITH a AS (" + A + ") SELECT sym, count_distinct(minute) FROM a ORDER BY sym",
            // the consumer reads an aggregate
            "WITH a AS (" + A + ") SELECT DISTINCT sym, v > 50 AS hi FROM a ORDER BY sym, hi",
            // LIMIT on the group by: which groups survive depends on all keys
            "SELECT DISTINCT sym FROM (SELECT timestamp_floor('10m', ts) AS minute, sym, avg(bid) v FROM quote ORDER BY minute, sym LIMIT 30) ORDER BY sym",
            // a window function between
            "SELECT DISTINCT sym, r FROM (SELECT sym, row_number() OVER (ORDER BY sym, minute) AS r FROM (" + A + ")) ORDER BY sym, r",
            // SAMPLE BY with FILL makes rows of its own
            "SELECT DISTINCT sym FROM (SELECT ts, sym, avg(bid) FROM quote WHERE sym IN ('A', 'B') SAMPLE BY 30m FILL(NULL)) ORDER BY sym",
            // a filter on an aggregate, directly, through a pass-through and through CASE
            "WITH a AS (" + A + ") SELECT DISTINCT sym FROM a WHERE v > 50 ORDER BY sym",
            "WITH a AS (" + A + ") SELECT DISTINCT sym FROM (SELECT sym, v FROM a) WHERE v > 50 ORDER BY sym",
            "WITH a AS (" + A + ") SELECT DISTINCT sym FROM (SELECT sym, CASE WHEN v > 50 THEN 1 ELSE 0 END AS f FROM a) WHERE f = 1 ORDER BY sym",
            // a consumer grouping by both keys and selecting one: one row per (sym, minute), duplicates kept
            "WITH a AS (" + A + ") SELECT sym FROM a GROUP BY sym, minute ORDER BY sym",
    };
    // the plan may or may not change; the rows must not
    private static final String[] SAME_ROWS = {
            // a pruned DISTINCT as the outer side of a JOIN LATERAL
            "SELECT o.sym, sub.n FROM (WITH a AS (" + A + ") SELECT DISTINCT sym FROM a) o JOIN LATERAL (SELECT count() n FROM quote q WHERE q.sym = o.sym) sub ORDER BY o.sym",
            // the group by itself as the outer side, every key read through the outer reference
            "SELECT o.minute, o.sym, sub.n FROM (" + A + ") o JOIN LATERAL (SELECT DISTINCT bsize % 2 AS n FROM quote q WHERE q.sym = o.sym) sub ORDER BY o.minute, o.sym, sub.n",
    };

    @Test
    public void testExplain() throws Exception {
        assertMemoryLeak(() -> {
            createQuote(engine, sqlExecutionContext);
            final String query = "WITH a AS (" + A + ") SELECT DISTINCT sym FROM a";
            setProperty(PropertyKey.CAIRO_SQL_DISTINCT_GROUPBY_KEY_PRUNING_ENABLED, "false");
            assertPlan(
                    query,
                    """
                            GroupBy vectorized: false
                              keys: [sym]
                                Async JIT Group By workers: 1
                                  keys: [sym,minute]
                                  keyFunctions: [timestamp_floor('minute',ts)]
                                  filter: sym in [A,B,C,Z]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: quote
                            """
            );
            // a LIMIT on the group by itself: which groups survive depends on every key
            final String limited = "SELECT DISTINCT sym FROM (" + A + " LIMIT 30)";
            final String limitedPlan = plan(engine, sqlExecutionContext, limited);
            setProperty(PropertyKey.CAIRO_SQL_DISTINCT_GROUPBY_KEY_PRUNING_ENABLED, "true");
            Assert.assertTrue(limitedPlan, limitedPlan.contains("keys: [sym,minute]"));
            TestUtils.assertEquals(limitedPlan, plan(engine, sqlExecutionContext, limited));
            assertPlan(
                    query,
                    """
                            GroupBy vectorized: false
                              keys: [sym]
                                Async JIT Group By workers: 1
                                  keys: [sym]
                                  filter: sym in [A,B,C,Z]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: quote
                            """
            );
        });
    }

    @Test
    public void testPrunedMatchesUnpruned() throws Exception {
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, ctx) -> {
                        createQuote(engine, ctx);
                        for (String query : PRUNED) {
                            assertMatchesUnpruned(engine, ctx, query, true);
                        }
                        for (String query : NOT_PRUNED) {
                            assertMatchesUnpruned(engine, ctx, query, false);
                        }
                        for (String query : SAME_ROWS) {
                            assertMatchesUnpruned(engine, ctx, query, null);
                        }
                    },
                    configuration,
                    LOG
            );
        });
    }

    private static void assertMatchesUnpruned(CairoEngine engine, SqlExecutionContext ctx, String query, Boolean pruned) throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_DISTINCT_GROUPBY_KEY_PRUNING_ENABLED, "false");
        final String unprunedPlan = plan(engine, ctx, query);
        final StringSink expected = new StringSink();
        TestUtils.printSql(engine, ctx, query, expected);
        setProperty(PropertyKey.CAIRO_SQL_DISTINCT_GROUPBY_KEY_PRUNING_ENABLED, "true");
        final String prunedPlan = plan(engine, ctx, query);
        if (Boolean.TRUE.equals(pruned)) {
            Assert.assertNotEquals(query + '\n' + prunedPlan, unprunedPlan, prunedPlan);
        } else if (Boolean.FALSE.equals(pruned)) {
            Assert.assertEquals(query, unprunedPlan, prunedPlan);
        }
        Assert.assertTrue(query + '\n' + expected, expected.toString().split("\n").length > 2);
        new QueryAssertion(engine, ctx, () -> {
        }, query)
                .noLeakCheck()
                .sizeMayVary()
                .inferRandomAccess()
                .inferTimestamp()
                .returns(expected.toString());
    }

    private static void assertPlan(String query, String expected) throws SqlException {
        TestUtils.assertEquals("QUERY PLAN\n" + expected, plan(engine, sqlExecutionContext, query));
    }

    private static void createQuote(CairoEngine engine, SqlExecutionContext ctx) throws SqlException {
        engine.execute("CREATE TABLE quote (ts TIMESTAMP, sym SYMBOL, bid DOUBLE, bsize INT) TIMESTAMP(ts) PARTITION BY HOUR", ctx);
        engine.execute(
                """
                        INSERT INTO quote SELECT
                            (x * 3_000_000)::timestamp,
                            CASE WHEN x % 17 = 0 THEN NULL ELSE rnd_symbol('A', 'B', 'C', 'D', 'E') END,
                            rnd_int(1, 400, 0) * 0.25,
                            rnd_int(1, 100, 0)
                        FROM long_sequence(6_000)
                        """,
                ctx
        );
    }

    private static String plan(CairoEngine engine, SqlExecutionContext ctx, String query) throws SqlException {
        final StringSink sink = new StringSink();
        engine.print("EXPLAIN " + query, sink, ctx);
        return sink.toString();
    }
}
