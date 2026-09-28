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

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.functions.test.TestFaultFunctionFactory;
import io.questdb.griffin.engine.table.HorizonJoinProjectionRecordCursorFactory;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.QueryAssertion;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class HorizonJoinProjectionTest extends AbstractCairoTest {
    @Override
    public void setUp() {
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
        super.setUp();
    }

    @Test
    public void testAggregateSubQueryKeepsUnselectedGroupingKeys() throws Exception {
        assertMemoryLeak(() -> {
            createTradesAndQuotes();
            for (boolean isParallel : new boolean[]{false, true}) {
                sqlExecutionContext.setParallelHorizonJoinEnabled(isParallel);
                assertQuery("""
                        SELECT n FROM (
                            SELECT t.sym, count() AS n FROM TaqTrade t
                            HORIZON JOIN TaqQuote q ON (sym) LIST (1s, 5s) AS h
                        ) ORDER BY n
                        """).expectSize().returns("n\n2\n4\n6\n");
                assertQuery("""
                        SELECT count() FROM (
                            SELECT t.sym, h.offset, avg(q.bid) FROM TaqTrade t
                            HORIZON JOIN TaqQuote q ON (sym) LIST (1s, 5s) AS h
                        )
                        """).noRandomAccess().expectSize().returns("count\n6\n");
                assertQuery("""
                        SELECT count() FROM (
                            SELECT count() FROM TaqTrade t
                            HORIZON JOIN TaqQuote q ON (sym) LIST (1s, 5s) AS h
                        )
                        """).noRandomAccess().expectSize().returns("count\n1\n");
            }
        });
    }

    @Test
    public void testConstantProjectionAndEarlyClose() throws Exception {
        assertMemoryLeak(() -> {
            createTradesAndQuotes();
            assertQuery("""
                    SELECT alloc(1024) AS n FROM TaqTrade t
                    HORIZON JOIN TaqQuote q ON (sym) LIST (1s, 5s) AS h LIMIT 2
                    """).noRandomAccess().sizeMayVary().returns("n\n42\n42\n");
            assertQuery("""
                    SELECT count() FROM (
                        SELECT 1 AS n FROM TaqTrade t
                        HORIZON JOIN TaqQuote q ON (sym) LIST (1s, 5s) AS h
                    )
                    """).noRandomAccess().expectSize().returns("count\n12\n");
        });
    }

    @Test
    public void testDistinctAndExplicitGroupingStillCollapseRows() throws Exception {
        assertMemoryLeak(() -> {
            createTradesAndQuotes();
            for (String projection : List.of(
                    "SELECT DISTINCT t.sym, h.offset",
                    "SELECT t.sym, h.offset"
            )) {
                String groupBy = projection.contains("DISTINCT") ? "" : " GROUP BY t.sym, h.offset";
                assertQuery("SELECT offset / 1_000_000 AS seconds, count() AS n FROM ("
                        + projection + " FROM TaqTrade t HORIZON JOIN TaqQuote q ON (sym) LIST (1s, 5s) AS h"
                        + groupBy + ") GROUP BY seconds ORDER BY seconds")
                        .expectSize().returns("seconds\tn\n1\t3\n5\t3\n");
            }
        });
    }

    @Test
    public void testEmptyInputs() throws Exception {
        assertMemoryLeak(() -> {
            createTradesAndQuotes();
            execute("TRUNCATE TABLE TaqQuote");
            assertQuery("""
                    SELECT count() AS n, count(bid) AS matches FROM (
                        SELECT q.bid FROM TaqTrade t
                        HORIZON JOIN TaqQuote q ON (sym) LIST (1s, 5s) AS h
                    )
                    """).noRandomAccess().expectSize().returns("n\tmatches\n12\t0\n");
            execute("TRUNCATE TABLE TaqTrade");
            assertQuery("""
                    SELECT count() FROM (
                        SELECT h.offset FROM TaqTrade t
                        HORIZON JOIN TaqQuote q ON (sym) LIST (1s, 5s) AS h
                    )
                    """).noRandomAccess().expectSize().returns("count\n0\n");
        });
    }

    @Test
    public void testMultiSlaveProjectionAndNullSymbolKeys() throws Exception {
        assertMemoryLeak(() -> {
            createTradesAndQuotes();
            assertQuery("""
                    SELECT s, count() AS n, sum(bid + ask) AS total FROM (
                        SELECT q.sym AS s, q.bid, r.ask FROM TaqTrade t
                        HORIZON JOIN TaqQuote q ON (t.sym = q.sym)
                        HORIZON JOIN TaqQuote r ON (t.sym = r.sym) LIST (1s, 5s) AS h
                    ) GROUP BY s ORDER BY s
                    """).expectSize().returns("s\tn\ttotal\n\t4\tnull\nA\t6\t164.0\nB\t2\t92.0\n");
            assertQuery("""
                    SELECT s, count() AS n FROM (
                        SELECT q.sym AS s FROM TaqTrade t
                        HORIZON JOIN TaqQuote q ON (sym) LIST (1s, 5s) AS h
                    ) GROUP BY s ORDER BY s
                    """).expectSize().returns("s\tn\n\t4\nA\t6\nB\t2\n");
        });
    }

    @Test
    public void testProjectionFactoryReopensAfterFunctionFailure() throws Exception {
        assertMemoryLeak(() -> {
            createTradesAndQuotes();
            try (RecordCursorFactory factory = select("""
                    SELECT alloc(1024) AS n, test_fault() AS is_ok FROM TaqTrade t
                    HORIZON JOIN TaqQuote q ON (sym) LIST (1s, 5s) AS h LIMIT 2
                    """)) {
                TestUtils.assertFactoryInTree(factory, HorizonJoinProjectionRecordCursorFactory.class);
                TestFaultFunctionFactory.armToFailAfterInits(0);
                try {
                    factory.getCursor(sqlExecutionContext).close();
                    Assert.fail("expected injected init failure");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "test_fault: injected init failure");
                } finally {
                    TestFaultFunctionFactory.disarm();
                }
                TestFaultFunctionFactory.armToFailAfter(1);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    while (cursor.hasNext()) {
                        cursor.getRecord().getBool(1);
                    }
                    Assert.fail("expected injected read failure");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "test_fault: injected failure");
                } finally {
                    TestFaultFunctionFactory.disarm();
                }
                new QueryAssertion(engine, factory).withContext(sqlExecutionContext)
                        .noRandomAccess().sizeMayVary().returns("n\tis_ok\n42\ttrue\n42\ttrue\n");
            }
        });
    }

    @Test
    public void testProjectedMarkoutPreservesTrades() throws Exception {
        assertMemoryLeak(() -> {
            createTradesAndQuotes();
            for (boolean isParallel : new boolean[]{false, true}) {
                sqlExecutionContext.setParallelHorizonJoinEnabled(isParallel);
                // Keep the direct aggregation as the oracle, then use the report's projection shape.
                String expected = """
                        seconds\tavgMarkoutAll\tnTotal
                        1\t2.5\t6
                        5\t4.5\t6
                        10\t4.5\t6
                        30\t4.5\t6
                        60\t4.5\t6
                        """;
                assertQuery("""
                        SELECT offset / 1_000_000 AS seconds,
                               avg((bid + ask) / 2.0 - t.price) AS avgMarkoutAll, count() AS nTotal
                        FROM (SELECT * FROM TaqTrade WHERE sym IN ('A', 'B', 'C')) t
                        HORIZON JOIN TaqQuote ON (sym) LIST (1s, 5s, 10s, 30s, 60s) AS h
                        GROUP BY seconds
                        ORDER BY seconds
                        """).expectSize().returns(expected);
                assertQuery("""
                        SELECT offset / 1_000_000 AS seconds,
                               avg((bid + ask) / 2.0 - price) AS avgMarkoutAll, count() AS nTotal
                        FROM (
                            SELECT price, bid, ask, offset
                            FROM (SELECT * FROM TaqTrade WHERE sym IN ('A', 'B', 'C')) t
                            HORIZON JOIN TaqQuote ON (sym) LIST (1s, 5s, 10s, 30s, 60s) AS h
                        )
                        GROUP BY seconds
                        ORDER BY seconds
                        """).expectSize().returns(expected);
            }
        });
    }

    @Test
    public void testProjectionColumnListDoesNotChangeCounts() throws Exception {
        assertMemoryLeak(() -> {
            createTradesAndQuotes();
            for (boolean isParallel : new boolean[]{false, true}) {
                sqlExecutionContext.setParallelHorizonJoinEnabled(isParallel);
                for (String projection : List.of("h.offset", "t.sym, h.offset", "t.price, h.offset", "q.bid, h.offset", "t.price, q.bid, q.ask, h.offset")) {
                    assertQuery("SELECT offset / 1_000_000 AS seconds, count() AS n FROM (SELECT "
                            + projection + " FROM TaqTrade t HORIZON JOIN TaqQuote q ON (sym) LIST (1s, 5s) AS h)"
                            + " GROUP BY seconds ORDER BY seconds")
                            .expectSize().returns("seconds\tn\n1\t6\n5\t6\n");
                    assertQuery("SELECT count() FROM (SELECT " + projection
                            + " FROM TaqTrade t HORIZON JOIN TaqQuote q ON (sym) LIST (1s, 5s) AS h)")
                            .noRandomAccess().expectSize().returns("count\n12\n");
                }
            }
        });
    }

    @Test
    public void testProjectionIndexedFiftySymbolList() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE trades AS (
                        SELECT timestamp_sequence('2026-01-01', 1_000) ts,
                               (x % 50)::SYMBOL sym, 10.0 price
                        FROM long_sequence(10_000)
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("ALTER TABLE trades ALTER COLUMN sym ADD INDEX");
            execute("""
                    CREATE TABLE quotes AS (
                        SELECT '2026-01-01'::TIMESTAMP ts, (x % 50)::SYMBOL sym, 10.0 bid, 12.0 ask
                        FROM long_sequence(50)
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            StringSink symbols = new StringSink();
            for (int i = 0; i < 50; i++) {
                if (i > 0) {
                    symbols.put(',');
                }
                symbols.put('\'').put(i).put('\'');
            }
            for (boolean isParallel : new boolean[]{false, true}) {
                sqlExecutionContext.setParallelHorizonJoinEnabled(isParallel);
                assertQuery("SELECT offset / 1_000_000 AS seconds, count() AS n, avg((bid + ask) / 2.0 - price) AS markout FROM ("
                        + "SELECT price, bid, ask, offset FROM (SELECT * FROM trades WHERE sym IN (" + symbols + ")) t "
                        + "HORIZON JOIN quotes ON (sym) LIST (1s, 5s, 10s, 30s, 60s) AS h) GROUP BY seconds ORDER BY seconds")
                        .expectSize().withPlanContaining("Horizon Join Projection").returns("""
                                seconds\tn\tmarkout
                                1\t10000\t1.0
                                5\t10000\t1.0
                                10\t10000\t1.0
                                30\t10000\t1.0
                                60\t10000\t1.0
                                """);
            }
        });
    }

    @Test
    public void testProjectionMasterFilterAndNegativeOffsets() throws Exception {
        assertMemoryLeak(() -> {
            createTradesAndQuotes();
            assertQuery("""
                    SELECT offset / 1_000_000 AS seconds, count() AS n, count(bid) AS matches FROM (
                        SELECT h.offset, q.bid FROM TaqTrade t
                        HORIZON JOIN TaqQuote q ON (sym) RANGE FROM -3s TO 1s STEP 2s AS h
                        WHERE concat(t.sym, '') = 'A'
                    ) GROUP BY seconds ORDER BY seconds
                    """).expectSize().returns("seconds\tn\tmatches\n-3\t3\t0\n-1\t3\t1\n1\t3\t3\n");
        });
    }

    @Test
    public void testProjectionMixedTimestampTypes() throws Exception {
        assertMemoryLeak(() -> {
            createTradesAndQuotes();
            execute("CREATE TABLE nanoTrades AS (SELECT ts::TIMESTAMP_NS ts, sym, price FROM TaqTrade) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE nanoQuotes AS (SELECT ts::TIMESTAMP_NS ts, sym, bid, ask FROM TaqQuote) TIMESTAMP(ts) PARTITION BY DAY");
            for (String trades : List.of("TaqTrade", "nanoTrades")) {
                for (String quotes : List.of("TaqQuote", "nanoQuotes")) {
                    long divisor = trades.equals("TaqTrade") ? 1_000_000L : 1_000_000_000L;
                    assertQuery("SELECT offset / " + divisor + " AS seconds, count() AS n, sum(bid) AS total FROM ("
                            + "SELECT h.offset, q.bid FROM " + trades + " t HORIZON JOIN " + quotes
                            + " q ON (sym) LIST (1s, 5s) AS h) GROUP BY seconds ORDER BY seconds")
                            .expectSize().returns("seconds\tn\ttotal\n1\t6\t56.0\n5\t6\t64.0\n");
                }
            }
        });
    }

    @Test
    public void testProjectionParquetAndVariableColumns() throws Exception {
        assertMemoryLeak(() -> {
            createTradesAndQuotes();
            execute("ALTER TABLE TaqQuote ADD COLUMN description STRING, label VARCHAR");
            execute("UPDATE TaqQuote SET description = sym::STRING, label = sym::VARCHAR");
            execute("ALTER TABLE TaqTrade CONVERT PARTITION TO PARQUET WHERE ts >= 0");
            execute("ALTER TABLE TaqQuote CONVERT PARTITION TO PARQUET WHERE ts >= 0");
            assertQuery("""
                    SELECT s, description, label, count() AS n FROM (
                        SELECT q.sym AS s, q.description, q.label FROM TaqTrade t
                        HORIZON JOIN TaqQuote q ON (sym) LIST (1s, 5s) AS h
                    ) GROUP BY s, description, label ORDER BY s
                    """).expectSize().returns("s\tdescription\tlabel\tn\n\t\t\t4\nA\tA\tA\t6\nB\tB\tB\t2\n");
        });
    }

    @Test
    public void testProjectionSortDoesNotAssumeMasterTimestampOrder() throws Exception {
        assertMemoryLeak(() -> {
            createTradesAndQuotes();
            assertQuery("""
                    SELECT ts FROM (
                        SELECT t.ts FROM TaqTrade t
                        HORIZON JOIN TaqQuote q ON (sym) LIST (1s, 5s) AS h
                    ) ORDER BY ts LIMIT 4
                    """).expectSize().timestamp("ts").returns("""
                    ts
                    2026-01-01T00:00:00.000000Z
                    2026-01-01T00:00:00.000000Z
                    2026-01-01T00:00:00.000000Z
                    2026-01-01T00:00:00.000000Z
                    """);
        });
    }

    @Test
    public void testSingleSymbolAndUnkeyedProjection() throws Exception {
        assertMemoryLeak(() -> {
            createTradesAndQuotes();
            assertQuery("""
                    SELECT count() FROM (
                        SELECT t.price FROM (SELECT * FROM TaqTrade WHERE sym = 'A') t
                        HORIZON JOIN TaqQuote q ON (sym) LIST (1s, 5s) AS h
                    )
                    """).noRandomAccess().expectSize().returns("count\n6\n");
            assertQuery("""
                    SELECT count() FROM (
                        SELECT q.bid FROM TaqTrade t
                        HORIZON JOIN TaqQuote q LIST (1s, 5s) AS h
                    )
                    """).noRandomAccess().expectSize().returns("count\n12\n");
        });
    }

    private void createTradesAndQuotes() throws Exception {
        execute("CREATE TABLE TaqTrade (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
        execute("CREATE TABLE TaqQuote (ts TIMESTAMP, sym SYMBOL, bid DOUBLE, ask DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
        // Exact duplicate trades, repeated prices, and a symbol with no quotes must all retain multiplicity.
        execute("""
                INSERT INTO TaqTrade VALUES
                    ('2026-01-01T00:00:00Z', 'A', 10),
                    ('2026-01-01T00:00:00Z', 'A', 10),
                    ('2026-01-01T00:00:02Z', 'A', 10),
                    ('2026-01-01T00:00:02Z', 'B', 20),
                    ('2026-01-01T00:00:02Z', 'C', 30),
                    ('2026-01-01T00:00:02Z', 'C', 30)
                """);
        execute("""
                INSERT INTO TaqQuote VALUES
                    ('2026-01-01T00:00:00Z', 'A', 10, 12),
                    ('2026-01-01T00:00:00Z', 'B', 20, 22),
                    ('2026-01-01T00:00:03Z', 'A', 14, 16),
                    ('2026-01-01T00:00:03Z', 'B', 22, 24)
                """);
    }
}
