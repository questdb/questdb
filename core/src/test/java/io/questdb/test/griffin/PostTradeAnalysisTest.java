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
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.TestTimestampType;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

/**
 * This test focuses on post-trade analysis queries, mostly using HORIZON JOIN and WINDOW JOIN.
 */
public class PostTradeAnalysisTest extends AbstractCairoTest {
    private static final Log LOG = LogFactory.getLog(PostTradeAnalysisTest.class);
    private final TestTimestampType leftTableTimestampType;
    private final boolean parallelHorizonJoinEnabled;
    private final TestTimestampType rightTableTimestampType;

    public PostTradeAnalysisTest() {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        this.leftTableTimestampType = TestUtils.getTimestampType(rnd);
        this.rightTableTimestampType = TestUtils.getTimestampType(rnd);
        this.parallelHorizonJoinEnabled = rnd.nextBoolean();
    }

    @Override
    public void setUp() {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HORIZON_JOIN_ENABLED, String.valueOf(parallelHorizonJoinEnabled));
        super.setUp();
    }

    @Test
    public void testBasicMarkoutCurveByCounterparty() throws Exception {
        // Basic Markout Curve — By Counterparty
        // "Which counterparties are sending me toxic flow?"
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "CREATE TABLE fx_trades (ts #TIMESTAMP, symbol SYMBOL, counterparty SYMBOL, side SYMBOL, price DOUBLE, quantity DOUBLE) TIMESTAMP(ts) PARTITION BY HOUR",
                    leftTableTimestampType.getTypeName()
            );
            executeWithRewriteTimestamp(
                    "CREATE TABLE market_data (ts #TIMESTAMP, symbol SYMBOL, bids DOUBLE[][], asks DOUBLE[][]) TIMESTAMP(ts) PARTITION BY HOUR",
                    rightTableTimestampType.getTypeName()
            );

            execute(
                    """
                            INSERT INTO fx_trades VALUES
                                ('2024-01-01T00:00:01.000000Z', 'EURUSD', 'CITADEL', 'buy',  1.1005, 100.0),
                                ('2024-01-01T00:00:02.000000Z', 'EURUSD', 'CITADEL', 'sell', 1.0995, 200.0),
                                ('2024-01-01T00:00:03.000000Z', 'EURUSD', 'JUMP',    'buy',  1.1005, 150.0)
                            """
            );

            // Market data evolves: mid moves from 1.1000 -> 1.1010 -> 1.1020
            execute(
                    """
                            INSERT INTO market_data VALUES
                                ('2024-01-01T00:00:00.500000Z', 'EURUSD', ARRAY[[1.0990, 100.0]], ARRAY[[1.1010, 100.0]]),
                                ('2024-01-01T00:00:02.500000Z', 'EURUSD', ARRAY[[1.1000, 100.0]], ARRAY[[1.1020, 100.0]]),
                                ('2024-01-01T00:00:06.500000Z', 'EURUSD', ARRAY[[1.1010, 100.0]], ARRAY[[1.1030, 100.0]])
                            """
            );

            String sql = "SELECT" +
                    "    f.counterparty," +
                    "    h.offset / " + getSecondsDivisor() + " AS sec_offs," +
                    "    avg(" +
                    "        CASE WHEN f.side = 'buy' THEN 1 ELSE -1 END" +
                    "        * ((m.bids[1][1] + m.asks[1][1]) / 2 - f.price)" +
                    "        / f.price * 10_000" +
                    "    ) AS markout_bps," +
                    "    count(*) AS n_trades " +
                    "FROM fx_trades f " +
                    "HORIZON JOIN market_data m ON (f.symbol = m.symbol)" +
                    "    LIST (0s, 1s, 5s) AS h " +
                    "GROUP BY f.counterparty, sec_offs " +
                    "ORDER BY f.counterparty, h.offset";

            assertQueryNoLeakCheck(
                    """
                            counterparty\tsec_offs\tmarkout_bps\tn_trades
                            CITADEL\t0\t-4.545455484598749\t2
                            CITADEL\t1\t-9.092977085325852\t2
                            CITADEL\t5\t-9.097109317584579\t2
                            JUMP\t0\t4.543389368468377\t1
                            JUMP\t1\t4.543389368468377\t1
                            JUMP\t5\t13.630168105405133\t1
                            """,
                    sql,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testBuySideOnlyMarkoutCurve() throws Exception {
        // Markout Curve — Buy Side Only
        // "What is my markout curve for buy trades by counterparty?"
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "CREATE TABLE fx_trades (ts #TIMESTAMP, symbol SYMBOL, counterparty SYMBOL, side SYMBOL, price DOUBLE, quantity DOUBLE) TIMESTAMP(ts) PARTITION BY HOUR",
                    leftTableTimestampType.getTypeName()
            );
            executeWithRewriteTimestamp(
                    "CREATE TABLE market_data (ts #TIMESTAMP, symbol SYMBOL, best_bid DOUBLE, best_ask DOUBLE) TIMESTAMP(ts) PARTITION BY HOUR",
                    rightTableTimestampType.getTypeName()
            );

            execute(
                    """
                            INSERT INTO fx_trades VALUES
                                ('2024-01-01T00:00:01.000000Z', 'EURUSD', 'CITADEL', 'BUY',  1.1005, 100.0),
                                ('2024-01-01T00:00:02.000000Z', 'EURUSD', 'CITADEL', 'SELL', 1.0995, 200.0),
                                ('2024-01-01T00:00:03.000000Z', 'EURUSD', 'JUMP',    'BUY',  1.1005, 150.0)
                            """
            );

            execute(
                    """
                            INSERT INTO market_data VALUES
                                ('2024-01-01T00:00:00.500000Z', 'EURUSD', 1.0990, 1.1010),
                                ('2024-01-01T00:00:02.500000Z', 'EURUSD', 1.1000, 1.1020),
                                ('2024-01-01T00:00:06.500000Z', 'EURUSD', 1.1010, 1.1030)
                            """
            );

            String sql = "SELECT" +
                    "    t.symbol," +
                    "    t.counterparty," +
                    "    h.offset / " + getSecondsDivisor() + " AS sec_offs," +
                    "    count() AS n," +
                    "    avg((m.best_bid + m.best_ask) / 2 - t.price) / t.price * 10_000 AS avg_markout_bps," +
                    "    sum(((m.best_bid + m.best_ask) / 2 - t.price) * t.quantity) AS total_pnl " +
                    "FROM fx_trades t " +
                    "HORIZON JOIN market_data m ON (symbol)" +
                    "    RANGE FROM 0s TO 2s STEP 1s AS h " +
                    "WHERE t.side = 'BUY' " +
                    "GROUP BY t.symbol, t.counterparty, sec_offs, t.price " +
                    "ORDER BY t.symbol, t.counterparty, h.offset";

            assertQueryNoLeakCheck(
                    """
                            symbol\tcounterparty\tsec_offs\tn\tavg_markout_bps\ttotal_pnl
                            EURUSD\tCITADEL\t0\t1\t-4.543389368468377\t-0.04999999999999449
                            EURUSD\tCITADEL\t1\t1\t-4.543389368468377\t-0.04999999999999449
                            EURUSD\tCITADEL\t2\t1\t4.543389368468377\t0.04999999999999449
                            EURUSD\tJUMP\t0\t1\t4.543389368468377\t0.07499999999999174
                            EURUSD\tJUMP\t1\t1\t4.543389368468377\t0.07499999999999174
                            EURUSD\tJUMP\t2\t1\t4.543389368468377\t0.07499999999999174
                            """,
                    sql,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testISDecompositionBySymbol() throws Exception {
        // IS (Implementation Shortfall) Decomposition — By Symbol
        // Uses HORIZON JOIN + PIVOT to compute effective spread, permanent and temporary
        // market impact components from trade and market data.
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "CREATE TABLE fx_trades (ts #TIMESTAMP, symbol SYMBOL, side SYMBOL, price DOUBLE, quantity DOUBLE) TIMESTAMP(ts) PARTITION BY HOUR",
                    leftTableTimestampType.getTypeName()
            );
            executeWithRewriteTimestamp(
                    "CREATE TABLE market_data (ts #TIMESTAMP, symbol SYMBOL, bids DOUBLE[][], asks DOUBLE[][]) TIMESTAMP(ts) PARTITION BY HOUR",
                    rightTableTimestampType.getTypeName()
            );

            // Trade prices differ from mids to produce non-zero spread
            execute(
                    """
                            INSERT INTO fx_trades VALUES
                                ('2024-01-01T00:00:01.000000Z', 'EURUSD', 'buy',  1.1005, 100.0),
                                ('2024-01-01T00:00:02.000000Z', 'EURUSD', 'sell', 1.0995, 200.0),
                                ('2024-01-01T00:00:03.000000Z', 'GBPUSD', 'buy',  1.2705, 150.0)
                            """
            );

            // Market data: at fill time and 30s later with shifted quotes
            execute(
                    """
                            INSERT INTO market_data VALUES
                                ('2024-01-01T00:00:00.500000Z', 'EURUSD', ARRAY[[1.0990, 100.0]], ARRAY[[1.1010, 100.0]]),
                                ('2024-01-01T00:00:00.500000Z', 'GBPUSD', ARRAY[[1.2690, 100.0]], ARRAY[[1.2710, 100.0]]),
                                ('2024-01-01T00:00:31.000000Z', 'EURUSD', ARRAY[[1.1000, 100.0]], ARRAY[[1.1020, 100.0]]),
                                ('2024-01-01T00:00:31.000000Z', 'GBPUSD', ARRAY[[1.2700, 100.0]], ARRAY[[1.2720, 100.0]])
                            """
            );

            // Offset for 30s depends on the left table's timestamp type
            long offset30s = leftTableTimestampType == TestTimestampType.MICRO
                    ? 30_000_000L
                    : 30_000_000_000L;

            String sql = "WITH markouts AS (" +
                    "    SELECT" +
                    "        f.symbol," +
                    "        f.price," +
                    "        f.quantity," +
                    "        f.side," +
                    "        h.offset," +
                    "        (m.bids[1][1] + m.asks[1][1]) / 2 AS mid" +
                    "    FROM fx_trades f" +
                    "    HORIZON JOIN market_data m ON (f.symbol = m.symbol)" +
                    "        LIST (0s, 30s) AS h" +
                    ")," +
                    "pivoted AS (" +
                    "    SELECT * FROM markouts" +
                    "    PIVOT (" +
                    "        avg(mid) AS mid," +
                    "        avg(price) AS px," +
                    "        sum(quantity) AS vol" +
                    "        FOR offset IN (" +
                    "            0          AS at_fill," +
                    "            " + offset30s + " AS at_30s" +
                    "        )" +
                    "        GROUP BY symbol, side" +
                    "    )" +
                    ")" +
                    "SELECT" +
                    "    symbol," +
                    "    side," +
                    "    at_fill_vol AS total_volume," +
                    "    CASE WHEN side = 'buy' THEN 1 ELSE -1 END" +
                    "        * (at_fill_px - at_fill_mid) / at_fill_mid * 10_000 AS effective_spread_bps," +
                    "    CASE WHEN side = 'buy' THEN 1 ELSE -1 END" +
                    "        * (at_30s_mid - at_fill_mid) / at_fill_mid * 10_000 AS permanent_bps," +
                    "    CASE WHEN side = 'buy' THEN 1 ELSE -1 END" +
                    "        * (at_fill_px - at_30s_mid) / at_fill_mid * 10_000 AS temporary_bps " +
                    "FROM pivoted " +
                    "ORDER BY symbol, side";

            assertQueryNoLeakCheck(
                    """
                            symbol\tside\ttotal_volume\teffective_spread_bps\tpermanent_bps\ttemporary_bps
                            EURUSD\tbuy\t100.0\t4.545454545454045\t9.09090909090809\t-4.545454545454045
                            EURUSD\tsell\t200.0\t4.545454545456064\t-9.09090909090809\t13.636363636364152
                            GBPUSD\tbuy\t150.0\t3.9370078740153143\t7.8740157480306285\t-3.9370078740153143
                            """,
                    sql,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testLastLookAnalysis() throws Exception {
        // FX-Specific: Last-Look Analysis
        // Measures price movement during the last-look window per counterparty.
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "CREATE TABLE fx_trades (ts #TIMESTAMP, symbol SYMBOL, counterparty SYMBOL, side SYMBOL, passive BOOLEAN, price DOUBLE, quantity DOUBLE) TIMESTAMP(ts) PARTITION BY HOUR",
                    leftTableTimestampType.getTypeName()
            );
            executeWithRewriteTimestamp(
                    "CREATE TABLE market_data (ts #TIMESTAMP, symbol SYMBOL, bids DOUBLE[][], asks DOUBLE[][]) TIMESTAMP(ts) PARTITION BY HOUR",
                    rightTableTimestampType.getTypeName()
            );

            // Only passive trades are included in last-look analysis
            execute(
                    """
                            INSERT INTO fx_trades VALUES
                                ('2024-01-01T00:00:01.000000Z', 'EURUSD', 'CITADEL', 'buy',  true,  1.1005, 100.0),
                                ('2024-01-01T00:00:02.000000Z', 'EURUSD', 'JUMP',    'sell', true,  1.0995, 200.0),
                                ('2024-01-01T00:00:03.000000Z', 'EURUSD', 'CITADEL', 'buy',  false, 1.1010, 50.0)
                            """
            );

            execute(
                    """
                            INSERT INTO market_data VALUES
                                ('2024-01-01T00:00:00.500000Z', 'EURUSD', ARRAY[[1.0990, 100.0]], ARRAY[[1.1010, 100.0]]),
                                ('2024-01-01T00:00:02.500000Z', 'EURUSD', ARRAY[[1.1000, 100.0]], ARRAY[[1.1020, 100.0]]),
                                ('2024-01-01T00:00:04.500000Z', 'EURUSD', ARRAY[[1.1010, 100.0]], ARRAY[[1.1030, 100.0]])
                            """
            );

            String sql = "SELECT" +
                    "    f.counterparty," +
                    "    h.offset / " + getSecondsDivisor() + " AS sec_offs," +
                    "    avg(" +
                    "        CASE WHEN f.side = 'buy' THEN 1 ELSE -1 END" +
                    "        * ((m.bids[1][1] + m.asks[1][1]) / 2 - f.price)" +
                    "        / f.price * 10_000" +
                    "    ) AS markout_bps," +
                    "    count(*) AS n_trades " +
                    "FROM fx_trades f " +
                    "HORIZON JOIN market_data m ON (f.symbol = m.symbol)" +
                    "    RANGE FROM 0s TO 2s STEP 1s AS h " +
                    "WHERE f.passive = true" +
                    "  AND f.symbol = 'EURUSD' " +
                    "GROUP BY f.counterparty, sec_offs " +
                    "ORDER BY f.counterparty, h.offset";

            assertQueryNoLeakCheck(
                    """
                            counterparty\tsec_offs\tmarkout_bps\tn_trades
                            CITADEL\t0\t-4.543389368468377\t1
                            CITADEL\t1\t-4.543389368468377\t1
                            CITADEL\t2\t4.543389368468377\t1
                            JUMP\t0\t-4.547521600729122\t1
                            JUMP\t1\t-13.642564802183328\t1
                            JUMP\t2\t-13.642564802183328\t1
                            """,
                    sql,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testPassiveVsAggressiveMarkoutCurve() throws Exception {
        // Passive vs. Aggressive — Full Markout Curve
        // "How different is my markout profile when I'm making vs. taking?"
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "CREATE TABLE fx_trades (ts #TIMESTAMP, symbol SYMBOL, side SYMBOL, passive BOOLEAN, price DOUBLE, quantity DOUBLE) TIMESTAMP(ts) PARTITION BY HOUR",
                    leftTableTimestampType.getTypeName()
            );
            executeWithRewriteTimestamp(
                    "CREATE TABLE market_data (ts #TIMESTAMP, symbol SYMBOL, bids DOUBLE[][], asks DOUBLE[][]) TIMESTAMP(ts) PARTITION BY HOUR",
                    rightTableTimestampType.getTypeName()
            );

            execute(
                    """
                            INSERT INTO fx_trades VALUES
                                ('2024-01-01T00:00:01.000000Z', 'EURUSD', 'buy',  true,  1.1005, 100.0),
                                ('2024-01-01T00:00:02.000000Z', 'EURUSD', 'sell', false, 1.0995, 200.0)
                            """
            );

            execute(
                    """
                            INSERT INTO market_data VALUES
                                ('2024-01-01T00:00:00.500000Z', 'EURUSD', ARRAY[[1.0990, 100.0]], ARRAY[[1.1010, 100.0]]),
                                ('2024-01-01T00:00:02.500000Z', 'EURUSD', ARRAY[[1.1000, 100.0]], ARRAY[[1.1020, 100.0]]),
                                ('2024-01-01T00:00:04.500000Z', 'EURUSD', ARRAY[[1.1010, 100.0]], ARRAY[[1.1030, 100.0]])
                            """
            );

            String sql = "SELECT" +
                    "    f.symbol," +
                    "    f.passive," +
                    "    h.offset / " + getSecondsDivisor() + " AS sec_offs," +
                    "    avg(" +
                    "        CASE WHEN f.side = 'buy' THEN 1 ELSE -1 END" +
                    "        * ((m.bids[1][1] + m.asks[1][1]) / 2 - f.price)" +
                    "        / f.price * 10_000" +
                    "    ) AS markout_bps," +
                    "    sum(f.quantity) AS total_volume," +
                    "    count(*) AS n_trades " +
                    "FROM fx_trades f " +
                    "HORIZON JOIN market_data m ON (f.symbol = m.symbol)" +
                    "    RANGE FROM 0s TO 2s STEP 1s AS h " +
                    "GROUP BY f.symbol, f.passive, sec_offs " +
                    "ORDER BY f.symbol, f.passive, h.offset";

            assertQueryNoLeakCheck(
                    """
                            symbol\tpassive\tsec_offs\tmarkout_bps\ttotal_volume\tn_trades
                            EURUSD\tfalse\t0\t-4.547521600729122\t200.0\t1
                            EURUSD\tfalse\t1\t-13.642564802183328\t200.0\t1
                            EURUSD\tfalse\t2\t-13.642564802183328\t200.0\t1
                            EURUSD\ttrue\t0\t-4.543389368468377\t100.0\t1
                            EURUSD\ttrue\t1\t-4.543389368468377\t100.0\t1
                            EURUSD\ttrue\t2\t4.543389368468377\t100.0\t1
                            """,
                    sql,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testRealizedSpreadByECN() throws Exception {
        // Realized Spread — By ECN
        // "Which venues am I making money on as a market maker?"
        // Uses WINDOW JOIN to look up mid at fill time and 5s later.
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "CREATE TABLE fx_trades (ts #TIMESTAMP, symbol SYMBOL, ecn SYMBOL, side SYMBOL, passive BOOLEAN, price DOUBLE, quantity DOUBLE) TIMESTAMP(ts) PARTITION BY HOUR",
                    leftTableTimestampType.getTypeName()
            );
            executeWithRewriteTimestamp(
                    "CREATE TABLE market_data (ts #TIMESTAMP, symbol SYMBOL, bids DOUBLE[][], asks DOUBLE[][]) TIMESTAMP(ts) PARTITION BY HOUR",
                    rightTableTimestampType.getTypeName()
            );

            // Only passive (maker) fills
            execute(
                    """
                            INSERT INTO fx_trades VALUES
                                ('2024-01-01T00:00:01.000000Z', 'EURUSD', 'HOTSPOT', 'buy',  true, 1.1005, 100.0),
                                ('2024-01-01T00:00:02.000000Z', 'EURUSD', 'EBS',     'sell', true, 1.0995, 200.0)
                            """
            );

            execute(
                    """
                            INSERT INTO market_data VALUES
                                ('2024-01-01T00:00:00.500000Z', 'EURUSD', ARRAY[[1.0990, 100.0]], ARRAY[[1.1010, 100.0]]),
                                ('2024-01-01T00:00:06.500000Z', 'EURUSD', ARRAY[[1.1000, 100.0]], ARRAY[[1.1020, 100.0]])
                            """
            );

            String sql = """
                    WITH trades_with_mid AS (
                        SELECT
                            f.ecn,
                            f.symbol,
                            f.side,
                            f.price,
                            f.quantity,
                            first((m0.bids[1][1] + m0.asks[1][1]) / 2) AS mid_at_fill,
                            first((m5.bids[1][1] + m5.asks[1][1]) / 2) AS mid_at_5s
                        FROM fx_trades f
                        WINDOW JOIN market_data m0 ON (f.symbol = m0.symbol)
                            RANGE BETWEEN 0 seconds PRECEDING AND 0 seconds PRECEDING
                            INCLUDE PREVAILING
                        WINDOW JOIN market_data m5 ON (f.symbol = m5.symbol)
                            RANGE BETWEEN 5 seconds FOLLOWING AND 5 seconds FOLLOWING
                            INCLUDE PREVAILING
                        WHERE f.passive = true
                    )
                    SELECT
                        ecn,
                        symbol,
                        count(*) AS n_trades,
                        sum(quantity) AS total_volume,
                        avg(2 * CASE WHEN side = 'buy' THEN 1 ELSE -1 END
                            * (price - mid_at_fill) / mid_at_fill * 10_000)     AS effective_spread_bps,
                        avg(2 * CASE WHEN side = 'buy' THEN 1 ELSE -1 END
                            * (price - mid_at_5s) / mid_at_fill * 10_000)       AS realized_spread_bps,
                        avg(2 * CASE WHEN side = 'buy' THEN 1 ELSE -1 END
                            * (mid_at_5s - mid_at_fill) / mid_at_fill * 10_000) AS adverse_selection_bps
                    FROM trades_with_mid
                    GROUP BY ecn, symbol
                    ORDER BY ecn, symbol
                    """;

            assertQueryNoLeakCheck(
                    """
                            ecn\tsymbol\tn_trades\ttotal_volume\teffective_spread_bps\trealized_spread_bps\tadverse_selection_bps
                            EBS\tEURUSD\t1\t200.0\t9.090909090912128\t27.272727272728304\t-18.18181818181618
                            HOTSPOT\tEURUSD\t1\t100.0\t9.09090909090809\t9.09090909090809\t0.0
                            """,
                    sql,
                    null,
                    true,
                    true
            );
        });
    }

    private long getSecondsDivisor() {
        return leftTableTimestampType == TestTimestampType.MICRO ? 1_000_000L : 1_000_000_000L;
    }
}
