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
 * cairo.sql.pivot.fuse.source.enabled: a PIVOT over a plain projection subquery aggregates the
 * subquery's table directly, so its inner GROUP BY runs as a parallel GROUP BY (TAQ idx 26, 27).
 * Each query runs with the fusion off first; that output, column names and order included, is the
 * expected output with the fusion on. The measures are sums of quarters, which add exactly in any order,
 * so the comparison is exact even though the fused GROUP BY merges per-worker partial sums.
 */
public class PivotFuseSourceTest extends AbstractCairoTest {
    private static final String SYMS = "('A','B','C','D','E','zz')";
    private static final String[] FUSED = {
            // TAQ idx 26/27: the IN list from an unordered SELECT DISTINCT, ORDER BY the group key
            "SELECT * FROM (SELECT timestamp_floor('10m', ts) AS minute, sym, (bsize * bid + asize * ask) / (bsize + asize) AS liq FROM quote WHERE sym IN " + SYMS + ") "
                    + "PIVOT (avg(liq) FOR sym IN (SELECT DISTINCT sym FROM quote WHERE sym IN " + SYMS + ") GROUP BY minute) ORDER BY minute",
            // a literal IN list, several aggregates, count(*) and sum (zero on empty)
            "SELECT * FROM (SELECT timestamp_floor('1h', ts) AS h, sym, bid * 2 AS b2, ask - bid AS spread, bsize FROM quote) "
                    + "PIVOT (sum(b2), avg(spread) AS sp, count(*) AS n, max(bsize) FOR sym IN ('A', 'B', 'C') GROUP BY h) ORDER BY h",
            // NULL symbols, NULL measures, a FOR value without rows
            "SELECT * FROM (SELECT timestamp_floor('1h', ts) AS h, sym, CASE WHEN bsize % 5 = 0 THEN NULL ELSE bid END AS v, "
                    + "CASE WHEN bsize % 7 = 0 THEN NULL ELSE ask END AS w FROM quote) "
                    + "PIVOT (avg(v), first(w) AS f FOR sym IN ('A', NULL, 'nope') GROUP BY h) ORDER BY h",
            "SELECT * FROM (SELECT ts, sym, bid FROM quote) PIVOT (sum(bid) FOR sym IN (NULL, 'A') GROUP BY timestamp_floor('2h', ts)) ORDER BY 1",
            // two FOR columns and two GROUP BY keys
            "SELECT * FROM (SELECT timestamp_floor('2h', ts) AS h, sym, bsize, bid > 50 AS big, ask AS a FROM quote WHERE bid > 2) "
                    + "PIVOT (sum(a) FOR sym IN ('A', 'B') bsize IN (10, 60, 61) GROUP BY h, big) ORDER BY h, big",
            // a WHERE between the subquery and PIVOT, references qualified by the source alias
            "SELECT * FROM (SELECT timestamp_floor('1h', ts) AS h, sym, bid + ask AS mid2, bid FROM quote) q WHERE q.bid > 10 "
                    + "PIVOT (sum(mid2), count() AS n FOR sym IN ('A', 'C') GROUP BY h) ORDER BY h",
            // a CTE source
            "WITH src AS (SELECT timestamp_floor('1h', ts) AS h, sym, bid * bsize AS notional FROM quote WHERE bsize < 90) "
                    + "SELECT * FROM src PIVOT (sum(notional) FOR sym IN ('A', 'B', 'D') GROUP BY h) ORDER BY h",
            // order-sensitive aggregates
            "SELECT * FROM (SELECT timestamp_floor('1h', ts) AS h, sym, bid, ts AS t FROM quote) "
                    + "PIVOT (first(bid) AS fb, last(bid) AS lb, min(t) AS mt FOR sym IN ('B', 'E') GROUP BY h) ORDER BY h",
    };
    private static final String[] NOT_FUSED = {
            // an aggregating subquery (the Manual Opt form of idx 26) is already parallel inside
            "SELECT * FROM (SELECT timestamp_floor('1h', ts) AS h, sym, avg(bid) AS v FROM quote GROUP BY h, sym) "
                    + "PIVOT (avg(v) FOR sym IN ('A', 'B') GROUP BY h) ORDER BY h",
            // a window function in the subquery
            "SELECT * FROM (SELECT timestamp_floor('1h', ts) AS h, sym, bid - lag(bid) OVER (PARTITION BY sym ORDER BY ts) AS d FROM quote) "
                    + "PIVOT (sum(d) FOR sym IN ('A', 'B') GROUP BY h) ORDER BY h",
            // a projected expression referenced twice
            "SELECT * FROM (SELECT timestamp_floor('1h', ts) AS h, sym, bid * 2 AS b2 FROM quote) "
                    + "PIVOT (sum(b2), max(b2) AS mx FOR sym IN ('A', 'B') GROUP BY h) ORDER BY h",
            // LIMIT and ORDER BY in the subquery
            "SELECT * FROM (SELECT timestamp_floor('1h', ts) AS h, sym, bid FROM quote ORDER BY ts LIMIT 5000) "
                    + "PIVOT (sum(bid) FOR sym IN ('A', 'B') GROUP BY h) ORDER BY h",
            // a computed FOR column: referenced by the inner GROUP BY and by the pushed-down IN filter
            "SELECT * FROM (SELECT timestamp_floor('1h', ts) AS h, coalesce(sym, 'none') AS s, bid FROM quote) "
                    + "PIVOT (avg(bid) FOR s IN ('A', 'none', 'nope') GROUP BY h) ORDER BY h",
            "SELECT * FROM (SELECT timestamp_floor('2h', ts) AS h, sym, bsize % 2 AS odd, bsize > 50 AS big, ask AS a FROM quote WHERE bid > 2) "
                    + "PIVOT (sum(a) FOR sym IN ('A', 'B') odd IN (0, 1) GROUP BY h, big) ORDER BY h, big",
            // columns qualified by the FROM's alias
            "SELECT * FROM (SELECT timestamp_floor('1h', q.ts) AS h, q.sym AS sym, q.bid AS bid FROM quote q) "
                    + "PIVOT (sum(bid) FOR sym IN ('A', 'B') GROUP BY h) ORDER BY h",
            // a projected column the PIVOT does not use
            "SELECT * FROM (SELECT timestamp_floor('1h', ts) AS h, sym, bid, ask * 2 AS unused FROM quote) "
                    + "PIVOT (sum(bid) FOR sym IN ('A', 'B') GROUP BY h) ORDER BY h",
            // a table source is parallel already
            "SELECT * FROM quote PIVOT (sum(bid) FOR sym IN ('A', 'B') GROUP BY bsize) ORDER BY bsize",
    };

    @Test
    public void testExplain() throws Exception {
        assertMemoryLeak(() -> {
            createQuote(engine, sqlExecutionContext);
            final String query = "SELECT * FROM (SELECT timestamp_floor('10m', ts) AS minute, sym, (bsize * bid + asize * ask) / (bsize + asize) AS liq "
                    + "FROM quote WHERE sym IN ('A', 'B')) PIVOT (avg(liq) FOR sym IN ('A', 'B') GROUP BY minute) ORDER BY minute";
            setProperty(PropertyKey.CAIRO_SQL_PIVOT_FUSE_SOURCE_ENABLED, "false");
            assertPlan(
                    query,
                    """
                            Encode sort light
                              keys: [minute]
                                GroupBy vectorized: false
                                  keys: [minute]
                                  values: [first_not_null(case([avg(liq),NaN,sym,switch(sym,'A',avg(liq),NaN)])),first_not_null(case([avg(liq),NaN,sym,switch(sym,'B',avg(liq),NaN)]))]
                                    GroupBy vectorized: false
                                      keys: [minute,sym]
                                      values: [avg(liq)]
                                        VirtualRecord
                                          functions: [timestamp_floor('minute',ts),bsize*bid+asize*ask/bsize+asize,sym]
                                            Async JIT Filter workers: 1
                                              filter: (sym in [A,B] and sym in [A,B])
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: quote
                            """
            );
            setProperty(PropertyKey.CAIRO_SQL_PIVOT_FUSE_SOURCE_ENABLED, "true");
            assertPlan(
                    query,
                    """
                            Encode sort light
                              keys: [minute]
                                GroupBy vectorized: false
                                  keys: [minute]
                                  values: [first_not_null(case([avg(liq),NaN,sym,switch(sym,'A',avg(liq),NaN)])),first_not_null(case([avg(liq),NaN,sym,switch(sym,'B',avg(liq),NaN)]))]
                                    Async JIT Group By workers: 1
                                      keys: [minute,sym]
                                      keyFunctions: [timestamp_floor('minute',ts)]
                                      values: [avg(bsize*bid+asize*ask/bsize+asize)]
                                      batchKernels: true
                                      filter: (sym in [A,B] and sym in [A,B])
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: quote
                            """
            );
        });
    }

    @Test
    public void testFusedMatchesUnfused() throws Exception {
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, ctx) -> {
                        createQuote(engine, ctx);
                        for (String query : FUSED) {
                            assertMatchesUnfused(engine, ctx, query, true);
                        }
                        for (String query : NOT_FUSED) {
                            assertMatchesUnfused(engine, ctx, query, false);
                        }
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testHintsStillApply() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE q (ts TIMESTAMP, sym SYMBOL INDEX, bid DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO q SELECT (x * 60_000_000)::timestamp, rnd_symbol('A', 'B', 'C'), x * 0.25 FROM long_sequence(500)");
            final String query = "SELECT /*+ no_index */ * FROM (SELECT timestamp_floor('1h', ts) AS h, sym, bid * 2 AS b2 FROM q WHERE sym IN ('A', 'B')) "
                    + "PIVOT (sum(b2) FOR sym IN ('A', 'B') GROUP BY h) ORDER BY h";
            for (String enabled : new String[]{"false", "true"}) {
                setProperty(PropertyKey.CAIRO_SQL_PIVOT_FUSE_SOURCE_ENABLED, enabled);
                final String plan = plan(engine, sqlExecutionContext, query);
                Assert.assertFalse(plan, plan.contains("Index"));
                Assert.assertTrue(plan, plan.contains("Frame forward scan on: q"));
            }
            assertMatchesUnfused(engine, sqlExecutionContext, query, true);
        });
    }

    @Test
    public void testUnknownColumnErrorUnchanged() throws Exception {
        assertMemoryLeak(() -> {
            createQuote(engine, sqlExecutionContext);
            final String query = "SELECT * FROM (SELECT timestamp_floor('1h', ts) AS h, sym, bid FROM quote) PIVOT (sum(ask) FOR sym IN ('A') GROUP BY h)";
            String unfused = null;
            for (String enabled : new String[]{"false", "true"}) {
                setProperty(PropertyKey.CAIRO_SQL_PIVOT_FUSE_SOURCE_ENABLED, enabled);
                try {
                    plan(engine, sqlExecutionContext, query);
                    Assert.fail();
                } catch (SqlException e) {
                    if (unfused == null) {
                        unfused = e.getMessage();
                    } else {
                        Assert.assertEquals(unfused, e.getMessage());
                    }
                }
            }
        });
    }

    private static void assertMatchesUnfused(CairoEngine engine, SqlExecutionContext ctx, String query, boolean fused) throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PIVOT_FUSE_SOURCE_ENABLED, "false");
        final String unfusedPlan = plan(engine, ctx, query);
        final StringSink expected = new StringSink();
        TestUtils.printSql(engine, ctx, query, expected);
        setProperty(PropertyKey.CAIRO_SQL_PIVOT_FUSE_SOURCE_ENABLED, "true");
        final String fusedPlan = plan(engine, ctx, query);
        if (fused) {
            // the pivot's inner GROUP BY reads the table: no projection between them
            Assert.assertTrue(query + '\n' + fusedPlan, fusedPlan.matches("(?s).*Async (JIT )?Group By workers: \\d+\\n\\s+keys: .*"));
        } else {
            Assert.assertEquals(query, unfusedPlan, fusedPlan);
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
        engine.execute("CREATE TABLE quote (ts TIMESTAMP, sym SYMBOL, bid DOUBLE, ask DOUBLE, bsize INT, asize INT) TIMESTAMP(ts) PARTITION BY HOUR", ctx);
        // quarters only (sums are exact in any order); bsize = asize keeps the TAQ liquidity-weighted
        // mid exact too: (b * bid + b * ask) / 2b = (bid + ask) / 2
        engine.execute(
                """
                        INSERT INTO quote SELECT
                            (x * 3_000_000)::timestamp,
                            CASE WHEN x % 17 = 0 THEN NULL ELSE rnd_symbol('A', 'B', 'C', 'D', 'E', 'F') END,
                            rnd_int(1, 400, 0) * 0.25,
                            rnd_int(1, 400, 0) * 0.25,
                            rnd_int(1, 100, 0) AS b,
                            0
                        FROM long_sequence(12_000)
                        """,
                ctx
        );
        engine.execute("UPDATE quote SET asize = bsize", ctx);
    }

    private static String plan(CairoEngine engine, SqlExecutionContext ctx, String query) throws SqlException {
        final StringSink sink = new StringSink();
        engine.print("EXPLAIN " + query, sink, ctx);
        return sink.toString();
    }
}
