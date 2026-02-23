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
    public void testAggregateSlippageByEcnCounterpartyPassive() throws Exception {
        // Aggregate slippage by ECN, counterparty, and passive/aggressive
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "CREATE TABLE fx_trades (ts #TIMESTAMP, symbol SYMBOL, ecn SYMBOL, counterparty SYMBOL, side SYMBOL, passive BOOLEAN, price DOUBLE, quantity DOUBLE) TIMESTAMP(ts) PARTITION BY HOUR",
                    leftTableTimestampType.getTypeName()
            );
            executeWithRewriteTimestamp(
                    "CREATE TABLE market_data (ts #TIMESTAMP, symbol SYMBOL, bids DOUBLE[][], asks DOUBLE[][]) TIMESTAMP(ts) PARTITION BY HOUR",
                    rightTableTimestampType.getTypeName()
            );

            execute(
                    """
                            INSERT INTO fx_trades VALUES
                                ('2024-01-01T00:00:01.000000Z', 'EURUSD', 'HOTSPOT', 'CITADEL', 'buy',  true,  1.1005, 100.0),
                                ('2024-01-01T00:00:02.000000Z', 'EURUSD', 'EBS',     'JUMP',    'sell', false, 1.0995, 200.0),
                                ('2024-01-01T00:00:03.000000Z', 'EURUSD', 'HOTSPOT', 'CITADEL', 'buy',  true,  1.1010, 150.0),
                                ('2024-01-01T00:00:04.000000Z', 'EURUSD', 'HOTSPOT', 'JUMP',    'buy',  false, 1.1015, 100.0)
                            """
            );

            execute(
                    """
                            INSERT INTO market_data VALUES
                                ('2024-01-01T00:00:00.500000Z', 'EURUSD', ARRAY[[1.0990, 100.0]], ARRAY[[1.1010, 100.0]]),
                                ('2024-01-01T00:00:01.500000Z', 'EURUSD', ARRAY[[1.0995, 100.0]], ARRAY[[1.1015, 100.0]]),
                                ('2024-01-01T00:00:02.500000Z', 'EURUSD', ARRAY[[1.1000, 100.0]], ARRAY[[1.1020, 100.0]]),
                                ('2024-01-01T00:00:03.500000Z', 'EURUSD', ARRAY[[1.1005, 100.0]], ARRAY[[1.1025, 100.0]])
                            """
            );

            String sql = """
                    SELECT
                        t.symbol,
                        t.ecn,
                        t.counterparty,
                        t.passive,
                        count() AS trade_count,
                        sum(t.quantity) AS total_qty,
                        avg(
                            CASE t.side
                                WHEN 'buy'  THEN (t.price - (m.bids[1][1] + m.asks[1][1]) / 2)
                                                 / ((m.bids[1][1] + m.asks[1][1]) / 2) * 10_000
                                WHEN 'sell' THEN ((m.bids[1][1] + m.asks[1][1]) / 2 - t.price)
                                                 / ((m.bids[1][1] + m.asks[1][1]) / 2) * 10_000
                            END
                        ) AS avg_slippage_vs_mid_bps,
                        avg(
                            CASE t.side
                                WHEN 'buy'  THEN (t.price - m.asks[1][1]) / m.asks[1][1] * 10_000
                                WHEN 'sell' THEN (m.bids[1][1] - t.price) / m.bids[1][1] * 10_000
                            END
                        ) AS avg_slippage_vs_tob_bps,
                        avg(
                            (m.asks[1][1] - m.bids[1][1])
                            / ((m.bids[1][1] + m.asks[1][1]) / 2) * 10_000
                        ) AS avg_spread_bps
                    FROM fx_trades t
                    ASOF JOIN market_data m ON (symbol)
                    GROUP BY t.symbol, t.ecn, t.counterparty, t.passive
                    ORDER BY avg_slippage_vs_mid_bps DESC
                    """;

            assertQueryNoLeakCheck(
                    """
                            symbol\tecn\tcounterparty\tpassive\ttrade_count\ttotal_qty\tavg_slippage_vs_mid_bps\tavg_slippage_vs_tob_bps\tavg_spread_bps
                            EURUSD\tEBS\tJUMP\tfalse\t1\t200.0\t9.086778736936756\t0.0\t18.17355747387553
                            EURUSD\tHOTSPOT\tCITADEL\ttrue\t2\t250.0\t2.2727272727270225\t-6.807868115275761\t18.173561225332357
                            EURUSD\tHOTSPOT\tJUMP\tfalse\t1\t100.0\t-2.0158384468908877E-12\t-9.070294784581515\t18.157058556513856
                            """,
                    sql,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testBasicMarkoutCurveByCounterparty() throws Exception {
        // Counterparty Toxicity Markout (buy side)
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

            execute(
                    """
                            INSERT INTO market_data VALUES
                                ('2024-01-01T00:00:00.500000Z', 'EURUSD', ARRAY[[1.0990, 100.0]], ARRAY[[1.1010, 100.0]]),
                                ('2024-01-01T00:00:02.500000Z', 'EURUSD', ARRAY[[1.1000, 100.0]], ARRAY[[1.1020, 100.0]]),
                                ('2024-01-01T00:00:06.500000Z', 'EURUSD', ARRAY[[1.1010, 100.0]], ARRAY[[1.1030, 100.0]]),
                                ('2024-01-01T00:00:15.000000Z', 'EURUSD', ARRAY[[1.1020, 100.0]], ARRAY[[1.1040, 100.0]]),
                                ('2024-01-01T00:00:35.000000Z', 'EURUSD', ARRAY[[1.1030, 100.0]], ARRAY[[1.1050, 100.0]]),
                                ('2024-01-01T00:01:05.000000Z', 'EURUSD', ARRAY[[1.1040, 100.0]], ARRAY[[1.1060, 100.0]]),
                                ('2024-01-01T00:06:05.000000Z', 'EURUSD', ARRAY[[1.1050, 100.0]], ARRAY[[1.1070, 100.0]])
                            """
            );

            String sql = "SELECT" +
                    "    t.symbol," +
                    "    t.counterparty," +
                    "    h.offset / " + getSecondsDivisor() + " AS horizon_sec," +
                    "    count() AS n," +
                    "    avg(((m.bids[1][1] + m.asks[1][1]) / 2 - t.price) / t.price * 10_000) AS avg_markout_bps," +
                    "    sum(t.quantity) AS total_volume " +
                    "FROM fx_trades t " +
                    "HORIZON JOIN market_data m ON (symbol)" +
                    "    LIST (0, 1s, 5s, 10s, 30s, 1m, 5m) AS h " +
                    "WHERE t.side = 'buy' " +
                    "GROUP BY t.symbol, t.counterparty, horizon_sec " +
                    "ORDER BY t.symbol, t.counterparty, h.offset";

            assertQueryNoLeakCheck(
                    """
                            symbol\tcounterparty\thorizon_sec\tn\tavg_markout_bps\ttotal_volume
                            EURUSD\tCITADEL\t0\t1\t-4.543389368468377\t100.0
                            EURUSD\tCITADEL\t1\t1\t-4.543389368468377\t100.0
                            EURUSD\tCITADEL\t5\t1\t4.543389368468377\t100.0
                            EURUSD\tCITADEL\t10\t1\t13.630168105405133\t100.0
                            EURUSD\tCITADEL\t30\t1\t22.71694684234592\t100.0
                            EURUSD\tCITADEL\t60\t1\t31.803725579282677\t100.0
                            EURUSD\tCITADEL\t300\t1\t40.890504316219435\t100.0
                            EURUSD\tJUMP\t0\t1\t4.543389368468377\t150.0
                            EURUSD\tJUMP\t1\t1\t4.543389368468377\t150.0
                            EURUSD\tJUMP\t5\t1\t13.630168105405133\t150.0
                            EURUSD\tJUMP\t10\t1\t13.630168105405133\t150.0
                            EURUSD\tJUMP\t30\t1\t22.71694684234592\t150.0
                            EURUSD\tJUMP\t60\t1\t31.803725579282677\t150.0
                            EURUSD\tJUMP\t300\t1\t40.890504316219435\t150.0
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
        // "What does the buy-side markout curve look like across all trades?"
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

            execute(
                    """
                            INSERT INTO market_data VALUES
                                ('2024-01-01T00:00:00.500000Z', 'EURUSD', ARRAY[[1.0990, 100.0]], ARRAY[[1.1010, 100.0]]),
                                ('2024-01-01T00:00:02.500000Z', 'EURUSD', ARRAY[[1.1000, 100.0]], ARRAY[[1.1020, 100.0]]),
                                ('2024-01-01T00:00:06.500000Z', 'EURUSD', ARRAY[[1.1010, 100.0]], ARRAY[[1.1030, 100.0]]),
                                ('2024-01-01T00:00:15.000000Z', 'EURUSD', ARRAY[[1.1020, 100.0]], ARRAY[[1.1040, 100.0]]),
                                ('2024-01-01T00:00:35.000000Z', 'EURUSD', ARRAY[[1.1030, 100.0]], ARRAY[[1.1050, 100.0]])
                            """
            );

            String sql = "SELECT" +
                    "    t.symbol," +
                    "    h.offset / " + getSecondsDivisor() + " AS horizon_sec," +
                    "    count() AS n," +
                    "    avg(((m.bids[1][1] + m.asks[1][1]) / 2 - t.price) / t.price * 10_000) AS avg_markout_bps," +
                    "    sum(((m.bids[1][1] + m.asks[1][1]) / 2 - t.price) * t.quantity) AS total_pnl " +
                    "FROM fx_trades t " +
                    "HORIZON JOIN market_data m ON (symbol)" +
                    "    RANGE FROM 0s TO 10s STEP 5s AS h " +
                    "WHERE t.side = 'buy' " +
                    "GROUP BY t.symbol, horizon_sec " +
                    "ORDER BY t.symbol, h.offset";

            assertQueryNoLeakCheck(
                    """
                            symbol\thorizon_sec\tn\tavg_markout_bps\ttotal_pnl
                            EURUSD\t0\t2\t0.0\t0.024999999999997247
                            EURUSD\t5\t2\t9.086778736936754\t0.2749999999999697
                            EURUSD\t10\t2\t13.630168105405133\t0.3749999999999587
                            """,
                    sql,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testCompositeToxicityScore() throws Exception {
        // Composite toxicity score per ECN (buy side)
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "CREATE TABLE fx_trades (ts #TIMESTAMP, symbol SYMBOL, ecn SYMBOL, side SYMBOL, price DOUBLE, quantity DOUBLE) TIMESTAMP(ts) PARTITION BY HOUR",
                    leftTableTimestampType.getTypeName()
            );
            executeWithRewriteTimestamp(
                    "CREATE TABLE market_data (ts #TIMESTAMP, symbol SYMBOL, bids DOUBLE[][], asks DOUBLE[][]) TIMESTAMP(ts) PARTITION BY HOUR",
                    rightTableTimestampType.getTypeName()
            );

            execute(
                    """
                            INSERT INTO fx_trades VALUES
                                ('2024-01-01T00:00:01.000000Z', 'EURUSD', 'HOTSPOT', 'buy',  1.1005, 100.0),
                                ('2024-01-01T00:00:02.000000Z', 'EURUSD', 'EBS',     'buy',  1.1003, 150.0),
                                ('2024-01-01T00:00:03.000000Z', 'EURUSD', 'HOTSPOT', 'buy',  1.1008, 200.0)
                            """
            );

            execute(
                    """
                            INSERT INTO market_data VALUES
                                ('2024-01-01T00:00:00.500000Z', 'EURUSD', ARRAY[[1.0990, 100.0]], ARRAY[[1.1010, 100.0]]),
                                ('2024-01-01T00:00:02.500000Z', 'EURUSD', ARRAY[[1.1000, 100.0]], ARRAY[[1.1020, 100.0]]),
                                ('2024-01-01T00:00:06.500000Z', 'EURUSD', ARRAY[[1.1010, 100.0]], ARRAY[[1.1030, 100.0]]),
                                ('2024-01-01T00:00:10.000000Z', 'EURUSD', ARRAY[[1.1020, 100.0]], ARRAY[[1.1040, 100.0]])
                            """
            );

            String sql = "SELECT" +
                    "    t.symbol," +
                    "    t.ecn," +
                    "    count() AS fill_count," +
                    "    sum(t.quantity) AS total_volume," +
                    "    sum(((m.bids[1][1] + m.asks[1][1]) / 2 - t.price)" +
                    "        / t.price * 10_000 * t.quantity)" +
                    "        / sum(t.quantity) AS vw_markout_5s_bps," +
                    "    avg(CASE" +
                    "        WHEN (m.bids[1][1] + m.asks[1][1]) / 2 < t.price THEN 1.0" +
                    "        ELSE 0.0" +
                    "    END) AS adverse_fill_ratio " +
                    "FROM fx_trades t " +
                    "HORIZON JOIN market_data m ON (symbol)" +
                    "    LIST (5s) AS h " +
                    "WHERE t.side = 'buy' " +
                    "GROUP BY t.symbol, t.ecn " +
                    "ORDER BY t.symbol, vw_markout_5s_bps";

            assertQueryNoLeakCheck(
                    """
                            symbol\tecn\tfill_count\ttotal_volume\tvw_markout_5s_bps\tadverse_fill_ratio
                            EURUSD\tHOTSPOT\t2\t300.0\t8.78190498328711\t0.0
                            EURUSD\tEBS\t1\t150.0\t15.450331727708921\t0.0
                            """,
                    sql,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testCostBySizeBucketPivoted() throws Exception {
        // Cost by size bucket — pivoted (buy side)
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "CREATE TABLE fx_trades (ts #TIMESTAMP, symbol SYMBOL, side SYMBOL, price DOUBLE, quantity DOUBLE) TIMESTAMP(ts) PARTITION BY HOUR",
                    leftTableTimestampType.getTypeName()
            );
            executeWithRewriteTimestamp(
                    "CREATE TABLE market_data (ts #TIMESTAMP, symbol SYMBOL, bids DOUBLE[][], asks DOUBLE[][]) TIMESTAMP(ts) PARTITION BY HOUR",
                    rightTableTimestampType.getTypeName()
            );

            execute(
                    """
                            INSERT INTO fx_trades VALUES
                                ('2024-01-01T00:00:01.000000Z', 'EURUSD', 'buy', 1.1005, 50.0),
                                ('2024-01-01T00:00:02.000000Z', 'EURUSD', 'buy', 1.1008, 500.0),
                                ('2024-01-01T00:00:03.000000Z', 'EURUSD', 'buy', 1.1003, 5000.0)
                            """
            );

            execute(
                    """
                            INSERT INTO market_data VALUES
                                ('2024-01-01T00:00:00.500000Z', 'EURUSD', ARRAY[[1.0990, 100.0]], ARRAY[[1.1010, 100.0]]),
                                ('2024-01-01T00:00:02.500000Z', 'EURUSD', ARRAY[[1.1000, 100.0]], ARRAY[[1.1020, 100.0]]),
                                ('2024-01-01T00:00:06.500000Z', 'EURUSD', ARRAY[[1.1010, 100.0]], ARRAY[[1.1030, 100.0]])
                            """
            );

            long offset5s = leftTableTimestampType == TestTimestampType.MICRO ? 5_000_000L : 5_000_000_000L;

            String sql = "WITH fills AS (" +
                    "    SELECT" +
                    "        t.symbol," +
                    "        t.price," +
                    "        t.quantity," +
                    "        h.offset," +
                    "        (m.bids[1][1] + m.asks[1][1]) / 2 AS mid," +
                    "        m.asks[1][1] - m.bids[1][1] AS spread," +
                    "        CASE" +
                    "            WHEN t.quantity < 100    THEN 'S'" +
                    "            WHEN t.quantity < 1000   THEN 'M'" +
                    "            ELSE 'L'" +
                    "        END AS size_bucket" +
                    "    FROM fx_trades t" +
                    "    HORIZON JOIN market_data m ON (symbol)" +
                    "        LIST (0, 5s) AS h" +
                    "    WHERE t.side = 'buy'" +
                    ")" +
                    "SELECT * FROM fills " +
                    "PIVOT (" +
                    "    count() AS n," +
                    "    avg((mid - price) / price * 10_000) AS markout_bps," +
                    "    avg(spread / mid * 10_000) AS spread_bps" +
                    "    FOR offset IN (0 AS at_fill, " + offset5s + " AS t_5s)" +
                    "    GROUP BY symbol, size_bucket" +
                    ")" +
                    "ORDER BY symbol, size_bucket";

            assertQueryNoLeakCheck(
                    """
                            symbol\tsize_bucket\tat_fill_n\tat_fill_markout_bps\tat_fill_spread_bps\tt_5s_n\tt_5s_markout_bps\tt_5s_spread_bps
                            EURUSD\tL\t1\t6.361901299644851\t18.16530426884652\t1\t15.450331727708921\t18.148820326678784
                            EURUSD\tM\t1\t-7.267441860464316\t18.181818181818198\t1\t10.901162790696473\t18.148820326678784
                            EURUSD\tS\t1\t-4.543389368468377\t18.181818181818198\t1\t4.543389368468377\t18.16530426884652
                            """,
                    sql,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testEcnFillQualityScorecard() throws Exception {
        // ECN fill quality scorecard (buy side)
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "CREATE TABLE fx_trades (ts #TIMESTAMP, symbol SYMBOL, ecn SYMBOL, side SYMBOL, passive BOOLEAN, price DOUBLE, quantity DOUBLE) TIMESTAMP(ts) PARTITION BY HOUR",
                    leftTableTimestampType.getTypeName()
            );
            executeWithRewriteTimestamp(
                    "CREATE TABLE market_data (ts #TIMESTAMP, symbol SYMBOL, bids DOUBLE[][], asks DOUBLE[][]) TIMESTAMP(ts) PARTITION BY HOUR",
                    rightTableTimestampType.getTypeName()
            );

            execute(
                    """
                            INSERT INTO fx_trades VALUES
                                ('2024-01-01T00:00:01.000000Z', 'EURUSD', 'HOTSPOT', 'buy',  true,  1.1005, 100.0),
                                ('2024-01-01T00:00:02.000000Z', 'EURUSD', 'HOTSPOT', 'buy',  false, 1.1008, 200.0),
                                ('2024-01-01T00:00:03.000000Z', 'EURUSD', 'EBS',     'buy',  true,  1.1003, 150.0),
                                ('2024-01-01T00:00:04.000000Z', 'EURUSD', 'EBS',     'sell', true,  1.0995, 100.0)
                            """
            );

            execute(
                    """
                            INSERT INTO market_data VALUES
                                ('2024-01-01T00:00:00.500000Z', 'EURUSD', ARRAY[[1.0990, 100.0]], ARRAY[[1.1010, 100.0]]),
                                ('2024-01-01T00:00:01.500000Z', 'EURUSD', ARRAY[[1.0995, 100.0]], ARRAY[[1.1015, 100.0]]),
                                ('2024-01-01T00:00:02.500000Z', 'EURUSD', ARRAY[[1.1000, 100.0]], ARRAY[[1.1020, 100.0]]),
                                ('2024-01-01T00:00:03.500000Z', 'EURUSD', ARRAY[[1.1005, 100.0]], ARRAY[[1.1025, 100.0]])
                            """
            );

            String sql = """
                    SELECT
                        t.symbol,
                        t.ecn,
                        count() AS fill_count,
                        sum(t.quantity) AS total_volume,
                        avg(t.quantity) AS avg_fill_size,
                        avg((m.asks[1][1] - m.bids[1][1])
                            / ((m.bids[1][1] + m.asks[1][1]) / 2) * 10_000) AS avg_spread_bps,
                        avg(((m.bids[1][1] + m.asks[1][1]) / 2 - t.price)
                            / t.price * 10_000) AS avg_slippage_bps,
                        avg((m.asks[1][1] - t.price)
                            / t.price * 10_000) AS avg_slippage_vs_ask_bps,
                        avg(CASE WHEN t.passive THEN 1.0 ELSE 0.0 END) AS passive_ratio
                    FROM fx_trades t
                    ASOF JOIN market_data m ON (symbol)
                    WHERE t.side = 'buy'
                    GROUP BY t.symbol, t.ecn
                    ORDER BY t.symbol, avg_slippage_bps
                    """;

            assertQueryNoLeakCheck(
                    """
                            symbol\tecn\tfill_count\ttotal_volume\tavg_fill_size\tavg_spread_bps\tavg_slippage_bps\tavg_slippage_vs_ask_bps\tpassive_ratio
                            EURUSD\tHOTSPOT\t2\t300.0\t150.0\t18.177687827846864\t-3.634340033072256\t5.451200498187327\t0.5
                            EURUSD\tEBS\t1\t150.0\t150.0\t18.16530426884652\t6.361901299644851\t15.45033172771094\t1.0
                            """,
                    sql,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testEcnMarkoutCurves() throws Exception {
        // ECN markout curves side by side (buy side)
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "CREATE TABLE fx_trades (ts #TIMESTAMP, symbol SYMBOL, ecn SYMBOL, side SYMBOL, price DOUBLE, quantity DOUBLE) TIMESTAMP(ts) PARTITION BY HOUR",
                    leftTableTimestampType.getTypeName()
            );
            executeWithRewriteTimestamp(
                    "CREATE TABLE market_data (ts #TIMESTAMP, symbol SYMBOL, bids DOUBLE[][], asks DOUBLE[][]) TIMESTAMP(ts) PARTITION BY HOUR",
                    rightTableTimestampType.getTypeName()
            );

            execute(
                    """
                            INSERT INTO fx_trades VALUES
                                ('2024-01-01T00:00:01.000000Z', 'EURUSD', 'HOTSPOT', 'buy',  1.1005, 100.0),
                                ('2024-01-01T00:00:02.000000Z', 'EURUSD', 'EBS',     'buy',  1.1003, 150.0),
                                ('2024-01-01T00:00:03.000000Z', 'EURUSD', 'HOTSPOT', 'sell', 1.0995, 200.0)
                            """
            );

            execute(
                    """
                            INSERT INTO market_data VALUES
                                ('2024-01-01T00:00:00.500000Z', 'EURUSD', ARRAY[[1.0990, 100.0]], ARRAY[[1.1010, 100.0]]),
                                ('2024-01-01T00:00:02.500000Z', 'EURUSD', ARRAY[[1.1000, 100.0]], ARRAY[[1.1020, 100.0]]),
                                ('2024-01-01T00:00:06.500000Z', 'EURUSD', ARRAY[[1.1010, 100.0]], ARRAY[[1.1030, 100.0]])
                            """
            );

            String sql = "SELECT" +
                    "    t.symbol," +
                    "    t.ecn," +
                    "    h.offset / " + getSecondsDivisor() + " AS horizon_sec," +
                    "    count() AS n," +
                    "    avg(((m.bids[1][1] + m.asks[1][1]) / 2 - t.price)" +
                    "        / t.price * 10_000) AS avg_markout_bps," +
                    "    sum(((m.bids[1][1] + m.asks[1][1]) / 2 - t.price)" +
                    "        * t.quantity) AS total_pnl " +
                    "FROM fx_trades t " +
                    "HORIZON JOIN market_data m ON (symbol)" +
                    "    RANGE FROM 0s TO 5s STEP 5s AS h " +
                    "WHERE t.side = 'buy' " +
                    "GROUP BY t.symbol, t.ecn, horizon_sec " +
                    "ORDER BY t.symbol, t.ecn, h.offset";

            assertQueryNoLeakCheck(
                    """
                            symbol\tecn\thorizon_sec\tn\tavg_markout_bps\ttotal_pnl
                            EURUSD\tEBS\t0\t1\t-2.7265291284192212\t-0.044999999999995044
                            EURUSD\tEBS\t5\t1\t15.450331727708921\t0.2549999999999719
                            EURUSD\tHOTSPOT\t0\t1\t-4.543389368468377\t-0.04999999999999449
                            EURUSD\tHOTSPOT\t5\t1\t4.543389368468377\t0.04999999999999449
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
        // FX Last-Look Detection
        // Millisecond-granularity markout to detect last-look behavior per counterparty.
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "CREATE TABLE fx_trades (ts #TIMESTAMP, symbol SYMBOL, counterparty SYMBOL, side SYMBOL, passive BOOLEAN, price DOUBLE, quantity DOUBLE) TIMESTAMP(ts) PARTITION BY HOUR",
                    leftTableTimestampType.getTypeName()
            );
            executeWithRewriteTimestamp(
                    "CREATE TABLE market_data (ts #TIMESTAMP, symbol SYMBOL, bids DOUBLE[][], asks DOUBLE[][]) TIMESTAMP(ts) PARTITION BY HOUR",
                    rightTableTimestampType.getTypeName()
            );

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
                                ('2024-01-01T00:00:01.050000Z', 'EURUSD', ARRAY[[1.0992, 100.0]], ARRAY[[1.1012, 100.0]]),
                                ('2024-01-01T00:00:02.050000Z', 'EURUSD', ARRAY[[1.0998, 100.0]], ARRAY[[1.1018, 100.0]]),
                                ('2024-01-01T00:00:02.500000Z', 'EURUSD', ARRAY[[1.1000, 100.0]], ARRAY[[1.1020, 100.0]]),
                                ('2024-01-01T00:00:04.500000Z', 'EURUSD', ARRAY[[1.1010, 100.0]], ARRAY[[1.1030, 100.0]])
                            """
            );

            String sql = "SELECT" +
                    "    t.symbol," +
                    "    t.counterparty," +
                    "    t.passive," +
                    "    h.offset / " + getMillisecondsDivisor() + " AS horizon_ms," +
                    "    count() AS n," +
                    "    avg(" +
                    "        CASE WHEN t.side = 'buy' THEN 1 ELSE -1 END" +
                    "        * ((m.bids[1][1] + m.asks[1][1]) / 2 - t.price)" +
                    "        / t.price * 10_000" +
                    "    ) AS avg_markout_bps " +
                    "FROM fx_trades t " +
                    "HORIZON JOIN market_data m ON (symbol)" +
                    "    LIST (0, 1T, 5T, 10T, 50T, 100T, 500T, 1000T, 5000T) AS h " +
                    "GROUP BY t.symbol, t.counterparty, t.passive, horizon_ms " +
                    "ORDER BY t.symbol, t.counterparty, t.passive, h.offset";

            assertQueryNoLeakCheck(
                    """
                            symbol\tcounterparty\tpassive\thorizon_ms\tn\tavg_markout_bps
                            EURUSD\tCITADEL\tfalse\t0\t1\t0.0
                            EURUSD\tCITADEL\tfalse\t1\t1\t0.0
                            EURUSD\tCITADEL\tfalse\t5\t1\t0.0
                            EURUSD\tCITADEL\tfalse\t10\t1\t0.0
                            EURUSD\tCITADEL\tfalse\t50\t1\t0.0
                            EURUSD\tCITADEL\tfalse\t100\t1\t0.0
                            EURUSD\tCITADEL\tfalse\t500\t1\t0.0
                            EURUSD\tCITADEL\tfalse\t1000\t1\t0.0
                            EURUSD\tCITADEL\tfalse\t5000\t1\t9.082652134422252
                            EURUSD\tCITADEL\ttrue\t0\t1\t-4.543389368468377
                            EURUSD\tCITADEL\ttrue\t1\t1\t-4.543389368468377
                            EURUSD\tCITADEL\ttrue\t5\t1\t-4.543389368468377
                            EURUSD\tCITADEL\ttrue\t10\t1\t-4.543389368468377
                            EURUSD\tCITADEL\ttrue\t50\t1\t-2.7260336210810263
                            EURUSD\tCITADEL\ttrue\t100\t1\t-2.7260336210810263
                            EURUSD\tCITADEL\ttrue\t500\t1\t-2.7260336210810263
                            EURUSD\tCITADEL\ttrue\t1000\t1\t-2.7260336210810263
                            EURUSD\tCITADEL\ttrue\t5000\t1\t13.630168105405133
                            EURUSD\tJUMP\ttrue\t0\t1\t-6.366530241019964
                            EURUSD\tJUMP\ttrue\t1\t1\t-6.366530241019964
                            EURUSD\tJUMP\ttrue\t5\t1\t-6.366530241019964
                            EURUSD\tJUMP\ttrue\t10\t1\t-6.366530241019964
                            EURUSD\tJUMP\ttrue\t50\t1\t-11.823556161892487
                            EURUSD\tJUMP\ttrue\t100\t1\t-11.823556161892487
                            EURUSD\tJUMP\ttrue\t500\t1\t-13.642564802183328
                            EURUSD\tJUMP\ttrue\t1000\t1\t-13.642564802183328
                            EURUSD\tJUMP\ttrue\t5000\t1\t-22.737608003637533
                            """,
                    sql,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testMarkoutAtKeyHorizons() throws Exception {
        // Markout at non-uniform key horizons using LIST
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "CREATE TABLE fx_trades (ts #TIMESTAMP, symbol SYMBOL, ecn SYMBOL, passive BOOLEAN, side SYMBOL, price DOUBLE, quantity DOUBLE) TIMESTAMP(ts) PARTITION BY HOUR",
                    leftTableTimestampType.getTypeName()
            );
            executeWithRewriteTimestamp(
                    "CREATE TABLE market_data (ts #TIMESTAMP, symbol SYMBOL, bids DOUBLE[][], asks DOUBLE[][]) TIMESTAMP(ts) PARTITION BY HOUR",
                    rightTableTimestampType.getTypeName()
            );

            execute(
                    """
                            INSERT INTO fx_trades VALUES
                                ('2024-01-01T00:00:01.000000Z', 'EURUSD', 'HOTSPOT', true,  'buy',  1.1005, 100.0),
                                ('2024-01-01T00:00:02.000000Z', 'EURUSD', 'EBS',     false, 'sell', 1.0995, 200.0),
                                ('2024-01-01T00:00:03.000000Z', 'EURUSD', 'HOTSPOT', true,  'buy',  1.1008, 150.0)
                            """
            );

            execute(
                    """
                            INSERT INTO market_data VALUES
                                ('2024-01-01T00:00:00.500000Z', 'EURUSD', ARRAY[[1.0990, 100.0]], ARRAY[[1.1010, 100.0]]),
                                ('2024-01-01T00:00:02.500000Z', 'EURUSD', ARRAY[[1.1000, 100.0]], ARRAY[[1.1020, 100.0]]),
                                ('2024-01-01T00:00:06.500000Z', 'EURUSD', ARRAY[[1.1010, 100.0]], ARRAY[[1.1030, 100.0]])
                            """
            );

            String sql = "SELECT" +
                    "    t.ecn," +
                    "    t.passive," +
                    "    h.offset / " + getSecondsDivisor() + " AS horizon_sec," +
                    "    count() AS n," +
                    "    round(avg(" +
                    "        CASE t.side" +
                    "            WHEN 'buy'  THEN ((m.bids[1][1] + m.asks[1][1]) / 2 - t.price)" +
                    "                             / t.price * 10_000" +
                    "            WHEN 'sell' THEN (t.price - (m.bids[1][1] + m.asks[1][1]) / 2)" +
                    "                             / t.price * 10_000" +
                    "        END" +
                    "    ), 3) AS avg_markout_bps " +
                    "FROM fx_trades t " +
                    "HORIZON JOIN market_data m ON (symbol)" +
                    "    LIST (0, 1s, 5s) AS h " +
                    "GROUP BY t.ecn, t.passive, horizon_sec " +
                    "ORDER BY t.ecn, t.passive, h.offset";

            assertQueryNoLeakCheck(
                    """
                            ecn\tpassive\thorizon_sec\tn\tavg_markout_bps
                            EBS\tfalse\t0\t1\t-4.548
                            EBS\tfalse\t1\t1\t-13.643
                            EBS\tfalse\t5\t1\t-22.738
                            HOTSPOT\ttrue\t0\t2\t-1.363
                            HOTSPOT\ttrue\t1\t2\t-1.363
                            HOTSPOT\ttrue\t5\t2\t7.722
                            """,
                    sql,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testMarkoutCurveByVenueAndCounterparty() throws Exception {
        // Post-trade markout curve by venue and counterparty (both sides)
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "CREATE TABLE fx_trades (ts #TIMESTAMP, symbol SYMBOL, ecn SYMBOL, counterparty SYMBOL, passive BOOLEAN, side SYMBOL, price DOUBLE, quantity DOUBLE) TIMESTAMP(ts) PARTITION BY HOUR",
                    leftTableTimestampType.getTypeName()
            );
            executeWithRewriteTimestamp(
                    "CREATE TABLE market_data (ts #TIMESTAMP, symbol SYMBOL, bids DOUBLE[][], asks DOUBLE[][]) TIMESTAMP(ts) PARTITION BY HOUR",
                    rightTableTimestampType.getTypeName()
            );

            execute(
                    """
                            INSERT INTO fx_trades VALUES
                                ('2024-01-01T00:00:01.000000Z', 'EURUSD', 'HOTSPOT', 'CITADEL', true,  'buy',  1.1005, 100.0),
                                ('2024-01-01T00:00:02.000000Z', 'EURUSD', 'EBS',     'JUMP',    false, 'sell', 1.0995, 200.0),
                                ('2024-01-01T00:00:03.000000Z', 'EURUSD', 'HOTSPOT', 'CITADEL', true,  'buy',  1.1008, 150.0)
                            """
            );

            execute(
                    """
                            INSERT INTO market_data VALUES
                                ('2024-01-01T00:00:00.500000Z', 'EURUSD', ARRAY[[1.0990, 100.0]], ARRAY[[1.1010, 100.0]]),
                                ('2024-01-01T00:00:02.500000Z', 'EURUSD', ARRAY[[1.1000, 100.0]], ARRAY[[1.1020, 100.0]]),
                                ('2024-01-01T00:00:06.500000Z', 'EURUSD', ARRAY[[1.1010, 100.0]], ARRAY[[1.1030, 100.0]])
                            """
            );

            String sql = "SELECT" +
                    "    t.symbol," +
                    "    t.ecn," +
                    "    t.counterparty," +
                    "    t.passive," +
                    "    h.offset / " + getSecondsDivisor() + " AS horizon_sec," +
                    "    count() AS n," +
                    "    avg(" +
                    "        CASE t.side" +
                    "            WHEN 'buy'  THEN ((m.bids[1][1] + m.asks[1][1]) / 2 - t.price)" +
                    "                             / t.price * 10_000" +
                    "            WHEN 'sell' THEN (t.price - (m.bids[1][1] + m.asks[1][1]) / 2)" +
                    "                             / t.price * 10_000" +
                    "        END" +
                    "    ) AS avg_markout_bps," +
                    "    sum(" +
                    "        CASE t.side" +
                    "            WHEN 'buy'  THEN ((m.bids[1][1] + m.asks[1][1]) / 2 - t.price)" +
                    "                             * t.quantity" +
                    "            WHEN 'sell' THEN (t.price - (m.bids[1][1] + m.asks[1][1]) / 2)" +
                    "                             * t.quantity" +
                    "        END" +
                    "    ) AS total_pnl " +
                    "FROM fx_trades t " +
                    "HORIZON JOIN market_data m ON (symbol)" +
                    "    RANGE FROM 0s TO 5s STEP 1s AS h " +
                    "GROUP BY t.symbol, t.ecn, t.counterparty, t.passive, horizon_sec " +
                    "ORDER BY t.symbol, t.ecn, t.counterparty, t.passive, h.offset";

            assertQueryNoLeakCheck(
                    """
                            symbol\tecn\tcounterparty\tpassive\thorizon_sec\tn\tavg_markout_bps\ttotal_pnl
                            EURUSD\tEBS\tJUMP\tfalse\t0\t1\t-4.547521600729122\t-0.1000000000000334
                            EURUSD\tEBS\tJUMP\tfalse\t1\t1\t-13.642564802183328\t-0.30000000000001137
                            EURUSD\tEBS\tJUMP\tfalse\t2\t1\t-13.642564802183328\t-0.30000000000001137
                            EURUSD\tEBS\tJUMP\tfalse\t3\t1\t-13.642564802183328\t-0.30000000000001137
                            EURUSD\tEBS\tJUMP\tfalse\t4\t1\t-13.642564802183328\t-0.30000000000001137
                            EURUSD\tEBS\tJUMP\tfalse\t5\t1\t-22.737608003637533\t-0.49999999999998934
                            EURUSD\tHOTSPOT\tCITADEL\ttrue\t0\t2\t-1.363264451676149\t-0.019999999999997797
                            EURUSD\tHOTSPOT\tCITADEL\ttrue\t1\t2\t-1.363264451676149\t-0.019999999999997797
                            EURUSD\tHOTSPOT\tCITADEL\ttrue\t2\t2\t3.180124916792228\t0.07999999999999119
                            EURUSD\tHOTSPOT\tCITADEL\ttrue\t3\t2\t3.180124916792228\t0.07999999999999119
                            EURUSD\tHOTSPOT\tCITADEL\ttrue\t4\t2\t7.722276079582425\t0.22999999999997467
                            EURUSD\tHOTSPOT\tCITADEL\ttrue\t5\t2\t7.722276079582425\t0.22999999999997467
                            """,
                    sql,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testOrderLevelISDecomposition() throws Exception {
        // Order-level IS decomposition into permanent and temporary impact
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "CREATE TABLE fx_trades (ts #TIMESTAMP, order_id SYMBOL, symbol SYMBOL, side SYMBOL, price DOUBLE, quantity DOUBLE) TIMESTAMP(ts) PARTITION BY HOUR",
                    leftTableTimestampType.getTypeName()
            );
            executeWithRewriteTimestamp(
                    "CREATE TABLE market_data (ts #TIMESTAMP, symbol SYMBOL, bids DOUBLE[][], asks DOUBLE[][]) TIMESTAMP(ts) PARTITION BY HOUR",
                    rightTableTimestampType.getTypeName()
            );

            execute(
                    """
                            INSERT INTO fx_trades VALUES
                                ('2024-01-01T00:00:01.000000Z', 'ORD1', 'EURUSD', 'buy',  1.1005, 100.0),
                                ('2024-01-01T00:00:02.000000Z', 'ORD1', 'EURUSD', 'buy',  1.1010, 150.0),
                                ('2024-01-01T00:00:03.000000Z', 'ORD2', 'EURUSD', 'sell', 1.0995, 200.0)
                            """
            );

            execute(
                    """
                            INSERT INTO market_data VALUES
                                ('2024-01-01T00:00:00.500000Z', 'EURUSD', ARRAY[[1.0990, 100.0]], ARRAY[[1.1010, 100.0]]),
                                ('2024-01-01T00:00:02.500000Z', 'EURUSD', ARRAY[[1.1000, 100.0]], ARRAY[[1.1020, 100.0]]),
                                ('2024-01-01T00:00:06.500000Z', 'EURUSD', ARRAY[[1.1010, 100.0]], ARRAY[[1.1030, 100.0]])
                            """
            );

            long offset5s = leftTableTimestampType == TestTimestampType.MICRO ? 5_000_000L : 5_000_000_000L;

            String sql = "WITH order_markouts AS (" +
                    "    SELECT" +
                    "        f.order_id," +
                    "        f.symbol," +
                    "        f.side," +
                    "        h.offset," +
                    "        sum((m.bids[1][1] + m.asks[1][1]) / 2 * f.quantity)" +
                    "            / sum(f.quantity) AS weighted_mid," +
                    "        sum(f.price * f.quantity) / sum(f.quantity) AS avg_exec_price," +
                    "        sum(f.quantity) AS total_qty" +
                    "    FROM fx_trades f" +
                    "    HORIZON JOIN market_data m ON (f.symbol = m.symbol)" +
                    "        LIST (0s, 5s) AS h" +
                    ")," +
                    "pivoted AS (" +
                    "    SELECT * FROM order_markouts" +
                    "    PIVOT (" +
                    "        first(weighted_mid) AS mid" +
                    "        FOR offset IN (" +
                    "            0          AS at_fill," +
                    "            " + offset5s + " AS at_5s" +
                    "        )" +
                    "        GROUP BY order_id, symbol, side, avg_exec_price, total_qty" +
                    "    )" +
                    ")" +
                    "SELECT" +
                    "    order_id," +
                    "    symbol," +
                    "    side," +
                    "    total_qty," +
                    "    CASE WHEN side = 'buy' THEN 1 ELSE -1 END" +
                    "        * (avg_exec_price - at_fill_mid)" +
                    "        / at_fill_mid * 10_000 AS total_is_bps," +
                    "    CASE WHEN side = 'buy' THEN 1 ELSE -1 END" +
                    "        * (at_5s_mid - at_fill_mid)" +
                    "        / at_fill_mid * 10_000 AS permanent_bps," +
                    "    CASE WHEN side = 'buy' THEN 1 ELSE -1 END" +
                    "        * (avg_exec_price - at_5s_mid)" +
                    "        / at_fill_mid * 10_000 AS temporary_bps " +
                    "FROM pivoted " +
                    "ORDER BY total_is_bps DESC";

            assertQueryNoLeakCheck(
                    """
                            order_id\tsymbol\tside\ttotal_qty\ttotal_is_bps\tpermanent_bps\ttemporary_bps
                            ORD2\tEURUSD\tsell\t200.0\t13.623978201635394\t-9.082652134422252\t22.706630336057646
                            ORD1\tEURUSD\tbuy\t250.0\t7.272727272726471\t14.545454545452943\t-7.272727272726471
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
        // Passive vs. Aggressive Markout with Half-Spread Baseline (buy side)
        // "How different is my markout profile when I'm making vs. taking?"
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "CREATE TABLE fx_trades (ts #TIMESTAMP, symbol SYMBOL, ecn SYMBOL, side SYMBOL, passive BOOLEAN, price DOUBLE, quantity DOUBLE) TIMESTAMP(ts) PARTITION BY HOUR",
                    leftTableTimestampType.getTypeName()
            );
            executeWithRewriteTimestamp(
                    "CREATE TABLE market_data (ts #TIMESTAMP, symbol SYMBOL, bids DOUBLE[][], asks DOUBLE[][]) TIMESTAMP(ts) PARTITION BY HOUR",
                    rightTableTimestampType.getTypeName()
            );

            execute(
                    """
                            INSERT INTO fx_trades VALUES
                                ('2024-01-01T00:00:01.000000Z', 'EURUSD', 'HOTSPOT', 'buy',  true,  1.1005, 100.0),
                                ('2024-01-01T00:00:02.000000Z', 'EURUSD', 'HOTSPOT', 'buy',  false, 1.1008, 200.0),
                                ('2024-01-01T00:00:03.000000Z', 'EURUSD', 'EBS',     'buy',  true,  1.1003, 150.0)
                            """
            );

            execute(
                    """
                            INSERT INTO market_data VALUES
                                ('2024-01-01T00:00:00.500000Z', 'EURUSD', ARRAY[[1.0990, 100.0]], ARRAY[[1.1010, 100.0]]),
                                ('2024-01-01T00:00:02.500000Z', 'EURUSD', ARRAY[[1.1000, 100.0]], ARRAY[[1.1020, 100.0]]),
                                ('2024-01-01T00:00:06.500000Z', 'EURUSD', ARRAY[[1.1010, 100.0]], ARRAY[[1.1030, 100.0]])
                            """
            );

            String sql = "SELECT" +
                    "    t.symbol," +
                    "    t.ecn," +
                    "    t.passive," +
                    "    h.offset / " + getSecondsDivisor() + " AS horizon_sec," +
                    "    count() AS n," +
                    "    avg(((m.bids[1][1] + m.asks[1][1]) / 2 - t.price)" +
                    "        / t.price * 10_000) AS avg_markout_bps," +
                    "    avg((m.asks[1][1] - m.bids[1][1])" +
                    "        / ((m.bids[1][1] + m.asks[1][1]) / 2) * 10_000) / 2 AS avg_half_spread_bps " +
                    "FROM fx_trades t " +
                    "HORIZON JOIN market_data m ON (symbol)" +
                    "    RANGE FROM 0s TO 5s STEP 1s AS h " +
                    "WHERE t.side = 'buy' " +
                    "GROUP BY t.symbol, t.ecn, t.passive, horizon_sec " +
                    "ORDER BY t.symbol, t.ecn, t.passive, h.offset";

            assertQueryNoLeakCheck(
                    """
                            symbol\tecn\tpassive\thorizon_sec\tn\tavg_markout_bps\tavg_half_spread_bps
                            EURUSD\tEBS\ttrue\t0\t1\t6.361901299644851\t9.08265213442326
                            EURUSD\tEBS\ttrue\t1\t1\t6.361901299644851\t9.08265213442326
                            EURUSD\tEBS\ttrue\t2\t1\t6.361901299644851\t9.08265213442326
                            EURUSD\tEBS\ttrue\t3\t1\t6.361901299644851\t9.08265213442326
                            EURUSD\tEBS\ttrue\t4\t1\t15.450331727708921\t9.074410163339392
                            EURUSD\tEBS\ttrue\t5\t1\t15.450331727708921\t9.074410163339392
                            EURUSD\tHOTSPOT\tfalse\t0\t1\t-7.267441860464316\t9.090909090909099
                            EURUSD\tHOTSPOT\tfalse\t1\t1\t1.816860465116079\t9.08265213442326
                            EURUSD\tHOTSPOT\tfalse\t2\t1\t1.816860465116079\t9.08265213442326
                            EURUSD\tHOTSPOT\tfalse\t3\t1\t1.816860465116079\t9.08265213442326
                            EURUSD\tHOTSPOT\tfalse\t4\t1\t1.816860465116079\t9.08265213442326
                            EURUSD\tHOTSPOT\tfalse\t5\t1\t10.901162790696473\t9.074410163339392
                            EURUSD\tHOTSPOT\ttrue\t0\t1\t-4.543389368468377\t9.090909090909099
                            EURUSD\tHOTSPOT\ttrue\t1\t1\t-4.543389368468377\t9.090909090909099
                            EURUSD\tHOTSPOT\ttrue\t2\t1\t4.543389368468377\t9.08265213442326
                            EURUSD\tHOTSPOT\ttrue\t3\t1\t4.543389368468377\t9.08265213442326
                            EURUSD\tHOTSPOT\ttrue\t4\t1\t4.543389368468377\t9.08265213442326
                            EURUSD\tHOTSPOT\ttrue\t5\t1\t4.543389368468377\t9.08265213442326
                            """,
                    sql,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testPivotedEcnScorecard() throws Exception {
        // Pivoted ECN scorecard (buy side)
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "CREATE TABLE fx_trades (ts #TIMESTAMP, symbol SYMBOL, ecn SYMBOL, side SYMBOL, price DOUBLE, quantity DOUBLE) TIMESTAMP(ts) PARTITION BY HOUR",
                    leftTableTimestampType.getTypeName()
            );
            executeWithRewriteTimestamp(
                    "CREATE TABLE market_data (ts #TIMESTAMP, symbol SYMBOL, bids DOUBLE[][], asks DOUBLE[][]) TIMESTAMP(ts) PARTITION BY HOUR",
                    rightTableTimestampType.getTypeName()
            );

            execute(
                    """
                            INSERT INTO fx_trades VALUES
                                ('2024-01-01T00:00:01.000000Z', 'EURUSD', 'HOTSPOT', 'buy',  1.1005, 100.0),
                                ('2024-01-01T00:00:02.000000Z', 'EURUSD', 'EBS',     'buy',  1.1003, 150.0),
                                ('2024-01-01T00:00:03.000000Z', 'EURUSD', 'HOTSPOT', 'buy',  1.1008, 200.0)
                            """
            );

            execute(
                    """
                            INSERT INTO market_data VALUES
                                ('2024-01-01T00:00:00.500000Z', 'EURUSD', ARRAY[[1.0990, 100.0]], ARRAY[[1.1010, 100.0]]),
                                ('2024-01-01T00:00:02.500000Z', 'EURUSD', ARRAY[[1.1000, 100.0]], ARRAY[[1.1020, 100.0]]),
                                ('2024-01-01T00:00:06.500000Z', 'EURUSD', ARRAY[[1.1010, 100.0]], ARRAY[[1.1030, 100.0]])
                            """
            );

            long offset5s = leftTableTimestampType == TestTimestampType.MICRO ? 5_000_000L : 5_000_000_000L;

            String sql = "WITH markouts AS (" +
                    "    SELECT" +
                    "        t.symbol," +
                    "        t.ecn," +
                    "        t.price," +
                    "        t.quantity," +
                    "        h.offset," +
                    "        m.bids[1][1] AS best_bid," +
                    "        m.asks[1][1] AS best_ask" +
                    "    FROM fx_trades t" +
                    "    HORIZON JOIN market_data m ON (symbol)" +
                    "        LIST (0, 5s) AS h" +
                    "    WHERE t.side = 'buy'" +
                    ")" +
                    "SELECT * FROM markouts " +
                    "PIVOT (" +
                    "    count() AS fills," +
                    "    avg(quantity) AS avg_size," +
                    "    sum(quantity) AS volume," +
                    "    avg(((best_bid + best_ask) / 2 - price) / price * 10_000) AS markout_bps" +
                    "    FOR offset IN (0 AS at_fill, " + offset5s + " AS t_5s)" +
                    "    GROUP BY symbol, ecn" +
                    ")" +
                    "ORDER BY t_5s_markout_bps";

            assertQueryNoLeakCheck(
                    """
                            symbol\tecn\tat_fill_fills\tat_fill_avg_size\tat_fill_volume\tat_fill_markout_bps\tt_5s_fills\tt_5s_avg_size\tt_5s_volume\tt_5s_markout_bps
                            EURUSD\tHOTSPOT\t2\t150.0\t300.0\t-1.363264451676149\t2\t150.0\t300.0\t7.722276079582425
                            EURUSD\tEBS\t1\t150.0\t150.0\t-2.7265291284192212\t1\t150.0\t150.0\t15.450331727708921
                            """,
                    sql,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testPreAndPostTradeMarkout() throws Exception {
        // Price movement around trade events using negative offsets
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "CREATE TABLE fx_trades (ts #TIMESTAMP, symbol SYMBOL, side SYMBOL, price DOUBLE, quantity DOUBLE) TIMESTAMP(ts) PARTITION BY HOUR",
                    leftTableTimestampType.getTypeName()
            );
            executeWithRewriteTimestamp(
                    "CREATE TABLE market_data (ts #TIMESTAMP, symbol SYMBOL, bids DOUBLE[][], asks DOUBLE[][]) TIMESTAMP(ts) PARTITION BY HOUR",
                    rightTableTimestampType.getTypeName()
            );

            execute(
                    """
                            INSERT INTO fx_trades VALUES
                                ('2024-01-01T00:00:05.000000Z', 'EURUSD', 'buy',  1.1005, 100.0),
                                ('2024-01-01T00:00:06.000000Z', 'EURUSD', 'sell', 1.0995, 200.0)
                            """
            );

            execute(
                    """
                            INSERT INTO market_data VALUES
                                ('2024-01-01T00:00:01.000000Z', 'EURUSD', ARRAY[[1.0985, 100.0]], ARRAY[[1.1005, 100.0]]),
                                ('2024-01-01T00:00:03.000000Z', 'EURUSD', ARRAY[[1.0990, 100.0]], ARRAY[[1.1010, 100.0]]),
                                ('2024-01-01T00:00:04.500000Z', 'EURUSD', ARRAY[[1.0995, 100.0]], ARRAY[[1.1015, 100.0]]),
                                ('2024-01-01T00:00:06.500000Z', 'EURUSD', ARRAY[[1.1000, 100.0]], ARRAY[[1.1020, 100.0]]),
                                ('2024-01-01T00:00:08.000000Z', 'EURUSD', ARRAY[[1.1005, 100.0]], ARRAY[[1.1025, 100.0]])
                            """
            );

            String sql = "SELECT" +
                    "    h.offset / " + getSecondsDivisor() + " AS horizon_sec," +
                    "    count() AS n," +
                    "    round(avg(" +
                    "        CASE t.side" +
                    "            WHEN 'buy'  THEN ((m.bids[1][1] + m.asks[1][1]) / 2 - t.price)" +
                    "                             / t.price * 10_000" +
                    "            WHEN 'sell' THEN (t.price - (m.bids[1][1] + m.asks[1][1]) / 2)" +
                    "                             / t.price * 10_000" +
                    "        END" +
                    "    ), 3) AS avg_markout_bps " +
                    "FROM fx_trades t " +
                    "HORIZON JOIN market_data m ON (symbol)" +
                    "    RANGE FROM -2s TO 2s STEP 1s AS h " +
                    "GROUP BY horizon_sec " +
                    "ORDER BY h.offset";

            assertQueryNoLeakCheck(
                    """
                            horizon_sec\tn\tavg_markout_bps
                            -2\t2\t-4.545
                            -1\t2\t-6.819
                            0\t2\t-4.548
                            1\t2\t-6.821
                            2\t2\t-6.823
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

    @Test
    public void testSlippagePerFill() throws Exception {
        // Per-fill slippage against mid and top-of-book
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "CREATE TABLE fx_trades (ts #TIMESTAMP, symbol SYMBOL, ecn SYMBOL, counterparty SYMBOL, side SYMBOL, passive BOOLEAN, price DOUBLE, quantity DOUBLE) TIMESTAMP(ts) PARTITION BY HOUR",
                    leftTableTimestampType.getTypeName()
            );
            executeWithRewriteTimestamp(
                    "CREATE TABLE market_data (ts #TIMESTAMP, symbol SYMBOL, bids DOUBLE[][], asks DOUBLE[][]) TIMESTAMP(ts) PARTITION BY HOUR",
                    rightTableTimestampType.getTypeName()
            );

            execute(
                    """
                            INSERT INTO fx_trades VALUES
                                ('2024-01-01T00:00:01.000000Z', 'EURUSD', 'HOTSPOT', 'CITADEL', 'buy',  true,  1.1005, 100.0),
                                ('2024-01-01T00:00:02.000000Z', 'EURUSD', 'EBS',     'JUMP',    'sell', false, 1.0995, 200.0),
                                ('2024-01-01T00:00:03.000000Z', 'EURUSD', 'HOTSPOT', 'CITADEL', 'buy',  false, 1.1010, 150.0)
                            """
            );

            execute(
                    """
                            INSERT INTO market_data VALUES
                                ('2024-01-01T00:00:00.500000Z', 'EURUSD', ARRAY[[1.0990, 100.0]], ARRAY[[1.1010, 100.0]]),
                                ('2024-01-01T00:00:01.500000Z', 'EURUSD', ARRAY[[1.0995, 100.0]], ARRAY[[1.1015, 100.0]]),
                                ('2024-01-01T00:00:02.500000Z', 'EURUSD', ARRAY[[1.1000, 100.0]], ARRAY[[1.1020, 100.0]])
                            """
            );

            String sql = """
                    SELECT
                        t.ts,
                        t.symbol,
                        t.ecn,
                        t.counterparty,
                        t.side,
                        t.passive,
                        t.price,
                        t.quantity,
                        m.bids[1][1] AS best_bid,
                        m.asks[1][1] AS best_ask,
                        (m.bids[1][1] + m.asks[1][1]) / 2 AS mid,
                        (m.asks[1][1] - m.bids[1][1]) AS spread,
                        CASE t.side
                            WHEN 'buy'  THEN (t.price - (m.bids[1][1] + m.asks[1][1]) / 2)
                                             / ((m.bids[1][1] + m.asks[1][1]) / 2) * 10_000
                            WHEN 'sell' THEN ((m.bids[1][1] + m.asks[1][1]) / 2 - t.price)
                                             / ((m.bids[1][1] + m.asks[1][1]) / 2) * 10_000
                        END AS slippage_bps,
                        CASE t.side
                            WHEN 'buy'  THEN (t.price - m.asks[1][1]) / m.asks[1][1] * 10_000
                            WHEN 'sell' THEN (m.bids[1][1] - t.price) / m.bids[1][1] * 10_000
                        END AS slippage_vs_tob_bps
                    FROM fx_trades t
                    ASOF JOIN market_data m ON (symbol)
                    ORDER BY t.ts
                    """;

            assertQueryNoLeakCheck(
                    replaceExpectedTimestamp(
                            """
                                    ts\tsymbol\tecn\tcounterparty\tside\tpassive\tprice\tquantity\tbest_bid\tbest_ask\tmid\tspread\tslippage_bps\tslippage_vs_tob_bps
                                    2024-01-01T00:00:01.000000Z\tEURUSD\tHOTSPOT\tCITADEL\tbuy\ttrue\t1.1005\t100.0\t1.099\t1.101\t1.1\t0.0020000000000000018\t4.545454545454045\t-4.541326067211126
                                    2024-01-01T00:00:02.000000Z\tEURUSD\tEBS\tJUMP\tsell\tfalse\t1.0995\t200.0\t1.0995\t1.1015\t1.1004999999999998\t0.0020000000000000018\t9.086778736936756\t0.0
                                    2024-01-01T00:00:03.000000Z\tEURUSD\tHOTSPOT\tCITADEL\tbuy\tfalse\t1.101\t150.0\t1.1\t1.102\t1.101\t0.0020000000000000018\t0.0\t-9.074410163340398
                                    """
                    ),
                    sql,
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testTotalISPerOrder() throws Exception {
        // Total implementation shortfall per order
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "CREATE TABLE fx_trades (ts #TIMESTAMP, order_id SYMBOL, symbol SYMBOL, side SYMBOL, price DOUBLE, quantity DOUBLE) TIMESTAMP(ts) PARTITION BY HOUR",
                    leftTableTimestampType.getTypeName()
            );
            executeWithRewriteTimestamp(
                    "CREATE TABLE market_data (ts #TIMESTAMP, symbol SYMBOL, bids DOUBLE[][], asks DOUBLE[][]) TIMESTAMP(ts) PARTITION BY HOUR",
                    rightTableTimestampType.getTypeName()
            );

            execute(
                    """
                            INSERT INTO fx_trades VALUES
                                ('2024-01-01T00:00:01.000000Z', 'ORD1', 'EURUSD', 'buy',  1.1005, 100.0),
                                ('2024-01-01T00:00:02.000000Z', 'ORD1', 'EURUSD', 'buy',  1.1010, 150.0),
                                ('2024-01-01T00:00:03.000000Z', 'ORD2', 'EURUSD', 'sell', 1.0995, 200.0),
                                ('2024-01-01T00:00:04.000000Z', 'ORD2', 'EURUSD', 'sell', 1.0990, 100.0)
                            """
            );

            execute(
                    """
                            INSERT INTO market_data VALUES
                                ('2024-01-01T00:00:00.500000Z', 'EURUSD', ARRAY[[1.0990, 100.0]], ARRAY[[1.1010, 100.0]]),
                                ('2024-01-01T00:00:01.500000Z', 'EURUSD', ARRAY[[1.0995, 100.0]], ARRAY[[1.1015, 100.0]]),
                                ('2024-01-01T00:00:02.500000Z', 'EURUSD', ARRAY[[1.1000, 100.0]], ARRAY[[1.1020, 100.0]]),
                                ('2024-01-01T00:00:03.500000Z', 'EURUSD', ARRAY[[1.1005, 100.0]], ARRAY[[1.1025, 100.0]])
                            """
            );

            String sql = """
                    WITH fills_enriched AS (
                        SELECT
                            f.order_id,
                            f.symbol,
                            f.side,
                            f.price,
                            f.quantity,
                            f.ts,
                            (m.bids[1][1] + m.asks[1][1]) / 2 AS mid_at_fill
                        FROM fx_trades f
                        ASOF JOIN market_data m ON (symbol)
                    ),
                    order_summary AS (
                        SELECT
                            order_id,
                            symbol,
                            side,
                            first(mid_at_fill) AS arrival_mid,
                            sum(price * quantity) / sum(quantity) AS avg_exec_price,
                            sum(quantity) AS total_qty,
                            count() AS n_fills,
                            min(ts) AS first_fill_ts,
                            max(ts) AS last_fill_ts
                        FROM fills_enriched
                        GROUP BY order_id, symbol, side
                    )
                    SELECT
                        order_id,
                        symbol,
                        side,
                        n_fills,
                        total_qty,
                        CASE WHEN side = 'buy' THEN 1 ELSE -1 END
                            * (avg_exec_price - arrival_mid)
                            / arrival_mid * 10_000 AS total_is_bps
                    FROM order_summary
                    ORDER BY total_is_bps DESC
                    """;

            assertQueryNoLeakCheck(
                    """
                            order_id\tsymbol\tside\tn_fills\ttotal_qty\ttotal_is_bps
                            ORD2\tEURUSD\tsell\t2\t300.0\t15.137753557372436
                            ORD1\tEURUSD\tbuy\t2\t250.0\t7.272727272726471
                            """,
                    sql,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testVpin() throws Exception {
        // VPIN — Volume-Synchronized Probability of Informed Trading
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "CREATE TABLE fx_trades (ts #TIMESTAMP, symbol SYMBOL, side SYMBOL, price DOUBLE, quantity DOUBLE) TIMESTAMP(ts) PARTITION BY HOUR",
                    leftTableTimestampType.getTypeName()
            );

            execute(
                    """
                            INSERT INTO fx_trades VALUES
                                ('2024-01-01T00:00:01.000000Z', 'EURUSD', 'buy',  1.1001, 30.0),
                                ('2024-01-01T00:00:02.000000Z', 'EURUSD', 'buy',  1.1002, 40.0),
                                ('2024-01-01T00:00:03.000000Z', 'EURUSD', 'sell', 1.1000, 50.0),
                                ('2024-01-01T00:00:04.000000Z', 'EURUSD', 'sell', 1.0999, 30.0),
                                ('2024-01-01T00:00:05.000000Z', 'EURUSD', 'buy',  1.1003, 60.0),
                                ('2024-01-01T00:00:06.000000Z', 'EURUSD', 'sell', 1.0998, 20.0),
                                ('2024-01-01T00:00:07.000000Z', 'EURUSD', 'buy',  1.1004, 70.0),
                                ('2024-01-01T00:00:08.000000Z', 'EURUSD', 'sell', 1.0997, 40.0),
                                ('2024-01-01T00:00:09.000000Z', 'EURUSD', 'buy',  1.1005, 50.0),
                                ('2024-01-01T00:00:10.000000Z', 'EURUSD', 'sell', 1.0996, 30.0),
                                ('2024-01-01T00:00:11.000000Z', 'EURUSD', 'buy',  1.1006, 60.0),
                                ('2024-01-01T00:00:12.000000Z', 'EURUSD', 'sell', 1.0995, 20.0)
                            """
            );

            String sql = """
                    WITH bucketed AS (
                        SELECT
                            t.ts,
                            t.symbol,
                            t.side,
                            t.price,
                            t.quantity,
                            floor(
                                sum(t.quantity) OVER (PARTITION BY symbol ORDER BY ts)
                                / 100
                            ) AS vol_bucket
                        FROM fx_trades t
                        WHERE t.symbol = 'EURUSD'
                    ),
                    bucket_stats AS (
                        SELECT
                            symbol,
                            vol_bucket,
                            min(ts) AS bucket_start,
                            max(ts) AS bucket_end,
                            count() AS trade_count,
                            sum(quantity) AS total_vol,
                            sum(CASE WHEN side = 'buy' THEN quantity ELSE 0.0 END) AS buy_vol,
                            sum(CASE WHEN side = 'sell' THEN quantity ELSE 0.0 END) AS sell_vol,
                            abs(
                                sum(CASE WHEN side = 'buy' THEN quantity ELSE 0.0 END)
                                - sum(CASE WHEN side = 'sell' THEN quantity ELSE 0.0 END)
                            ) / sum(quantity) AS bucket_imbalance
                        FROM bucketed
                        GROUP BY symbol, vol_bucket
                    )
                    SELECT
                        symbol,
                        vol_bucket,
                        total_vol,
                        buy_vol,
                        sell_vol,
                        bucket_imbalance,
                        avg(bucket_imbalance) OVER (
                            PARTITION BY symbol
                            ORDER BY vol_bucket
                            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
                        ) AS vpin
                    FROM bucket_stats
                    ORDER BY vol_bucket
                    """;

            assertQueryNoLeakCheck(
                    """
                            symbol\tvol_bucket\ttotal_vol\tbuy_vol\tsell_vol\tbucket_imbalance\tvpin
                            EURUSD\t0.0\t70.0\t70.0\t0.0\t1.0\t1.0
                            EURUSD\t1.0\t80.0\t0.0\t80.0\t1.0\t1.0
                            EURUSD\t2.0\t80.0\t60.0\t20.0\t0.5\t0.8333333333333334
                            EURUSD\t3.0\t160.0\t120.0\t40.0\t0.5\t0.6666666666666666
                            EURUSD\t4.0\t90.0\t60.0\t30.0\t0.3333333333333333\t0.4444444444444444
                            EURUSD\t5.0\t20.0\t0.0\t20.0\t1.0\t0.611111111111111
                            """,
                    sql,
                    null,
                    true,
                    true
            );
        });
    }

    private long getMillisecondsDivisor() {
        return leftTableTimestampType == TestTimestampType.MICRO ? 1_000L : 1_000_000L;
    }

    private long getSecondsDivisor() {
        return leftTableTimestampType == TestTimestampType.MICRO ? 1_000_000L : 1_000_000_000L;
    }

    private String replaceExpectedTimestamp(String expected) {
        return leftTableTimestampType == TestTimestampType.MICRO ? expected : expected.replace(".000000Z", ".000000000Z");
    }
}
