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
 * This test focuses on post-trade analysis use case for HORIZON JOIN.
 */
public class HorizonJoinPostTradeAnalysisTest extends AbstractCairoTest {
    private static final Log LOG = LogFactory.getLog(HorizonJoinPostTradeAnalysisTest.class);
    private final TestTimestampType leftTableTimestampType;
    private final boolean parallelHorizonJoinEnabled;
    private final TestTimestampType rightTableTimestampType;

    public HorizonJoinPostTradeAnalysisTest() {
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
                    "        * (at_fill_px - at_fill_mid) / at_fill_mid * 10000 AS effective_spread_bps," +
                    "    CASE WHEN side = 'buy' THEN 1 ELSE -1 END" +
                    "        * (at_30s_mid - at_fill_mid) / at_fill_mid * 10000 AS permanent_bps," +
                    "    CASE WHEN side = 'buy' THEN 1 ELSE -1 END" +
                    "        * (at_fill_px - at_30s_mid) / at_fill_mid * 10000 AS temporary_bps " +
                    "FROM pivoted " +
                    "ORDER BY symbol, side";

            // at_fill: EURUSD mid=(1.0990+1.1010)/2=1.1000, GBPUSD mid=(1.2690+1.2710)/2=1.2700
            // at_30s:  EURUSD mid=(1.1000+1.1020)/2=1.1010, GBPUSD mid=(1.2700+1.2720)/2=1.2710
            //
            // EURUSD buy:  px=1.1005, fill_mid=1.1000, 30s_mid=1.1010
            //   effective = 1*(1.1005-1.1000)/1.1000*10000 = 0.4545...
            //   permanent = 1*(1.1010-1.1000)/1.1000*10000 = 0.9090...
            //   temporary = 1*(1.1005-1.1010)/1.1000*10000 = -0.4545...
            //
            // EURUSD sell: px=1.0995, fill_mid=1.1000, 30s_mid=1.1010
            //   effective = -1*(1.0995-1.1000)/1.1000*10000 = 0.4545...
            //   permanent = -1*(1.1010-1.1000)/1.1000*10000 = -0.9090...
            //   temporary = -1*(1.0995-1.1010)/1.1000*10000 = 1.3636...
            //
            // GBPUSD buy:  px=1.2705, fill_mid=1.2700, 30s_mid=1.2710
            //   effective = 1*(1.2705-1.2700)/1.2700*10000 = 0.3937...
            //   permanent = 1*(1.2710-1.2700)/1.2700*10000 = 0.7874...
            //   temporary = 1*(1.2705-1.2710)/1.2700*10000 = -0.3937...
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
}
