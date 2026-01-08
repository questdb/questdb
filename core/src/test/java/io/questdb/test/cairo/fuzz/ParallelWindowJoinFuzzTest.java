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

package io.questdb.test.cairo.fuzz;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.sql.BindVariableService;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.mp.WorkerPool;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.Test;

// This is not a fuzz test in traditional sense, but it's multithreaded, and we want to run it
// in CI frequently along with other fuzz tests.
public class ParallelWindowJoinFuzzTest extends AbstractCairoTest {
    private static final int PAGE_FRAME_COUNT = 4; // also used to set queue size, so must be a power of 2
    private static final int PAGE_FRAME_MAX_ROWS = 100;
    private static final int ROW_COUNT = 10 * PAGE_FRAME_COUNT * PAGE_FRAME_MAX_ROWS;
    private final boolean enableParallelWindowJoin;

    public ParallelWindowJoinFuzzTest() {
        this.enableParallelWindowJoin = TestUtils.generateRandom(LOG).nextBoolean();
        LOG.info().$("parallel window join enabled: ").$(enableParallelWindowJoin).$();
    }

    @Override
    @Before
    public void setUp() {
        // Async window join uses small page frames.
        setProperty(PropertyKey.CAIRO_SMALL_SQL_PAGE_FRAME_MAX_ROWS, PAGE_FRAME_MAX_ROWS);
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, PAGE_FRAME_MAX_ROWS);
        // We intentionally use small values for shard count and reduce
        // queue capacity to exhibit various edge cases.
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_SHARD_COUNT, 2);
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_REDUCE_QUEUE_CAPACITY, PAGE_FRAME_COUNT);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_JOIN_ENABLED, String.valueOf(enableParallelWindowJoin));
        super.setUp();
    }

    @Test
    public void testParallelWindowJoinFiltered() throws Exception {
        testParallelWindowJoin(
                "SELECT avg(price) avg_price, max(max_sym) max_sym " +
                        "FROM (" +
                        "  SELECT t.price price, max(p.sym) max_sym " +
                        "  FROM trades t " +
                        "  WINDOW JOIN prices p ON t.sym = p.sym " +
                        "  RANGE BETWEEN 2 seconds PRECEDING AND 2 seconds FOLLOWING " +
                        "  WHERE t.side = 'sell' " +
                        ")",
                """
                        avg_price\tmax_sym
                        19.98409342766\tsym95
                        """,
                "SELECT avg(price) avg_price, max(max_sym) max_sym " +
                        "FROM (" +
                        "  SELECT t.price price, max(p.sym) max_sym " +
                        "  FROM trades t " +
                        "  WINDOW JOIN prices p ON t.sym = p.sym " +
                        "  RANGE BETWEEN 2 seconds PRECEDING AND 2 seconds FOLLOWING EXCLUDE PREVAILING " +
                        "  WHERE t.side = 'sell' " +
                        ")",
                """
                        avg_price\tmax_sym
                        19.98409342766\tsym9
                        """
        );
    }

    @Test
    public void testParallelWindowJoinFiltered2() throws Exception {
        // tests thread-unsafe filter
        testParallelWindowJoin(
                "SELECT avg(price) avg_price, max(max_bid) max_bid, min(min_bid) min_bid " +
                        "FROM (" +
                        "  SELECT t.price price, max(p.bid) max_bid, min(p.bid) min_bid " +
                        "  FROM trades t " +
                        "  WINDOW JOIN prices p ON concat(p.sym, '_00') = 'sym11_00' " +
                        "  RANGE BETWEEN 1 second PRECEDING AND 1 second FOLLOWING " +
                        "  WHERE concat(t.side, '_00') = 'sell_00' " +
                        ")",
                """
                        avg_price\tmax_bid\tmin_bid
                        19.98409342766\t14.509523668084656\t5.600641833789625
                        """,
                "SELECT avg(price) avg_price, max(max_bid) max_bid, min(min_bid) min_bid " +
                        "FROM (" +
                        "  SELECT t.price price, max(p.bid) max_bid, min(p.bid) min_bid " +
                        "  FROM trades t " +
                        "  WINDOW JOIN prices p ON concat(p.sym, '_00') = 'sym11_00' " +
                        "  RANGE BETWEEN 1 second PRECEDING AND 1 second FOLLOWING EXCLUDE PREVAILING " +
                        "  WHERE concat(t.side, '_00') = 'sell_00' " +
                        ")",
                """
                        avg_price\tmax_bid\tmin_bid
                        19.98409342766\t6.190214242777591\t5.600641833789625
                        """
        );
    }

    @Test
    public void testParallelWindowJoinOnSymbol() throws Exception {
        // since aggregate functions have an expression with master's and slave's columns
        // as the argument, vectorized execution should not kick in
        testParallelWindowJoin(
                "SELECT max(max_bid) max_bid, min(min_bid) min_bid " +
                        "FROM (" +
                        "  SELECT max(t.price + p.bid) max_bid, min(t.price + p.bid) min_bid " +
                        "  FROM trades t " +
                        "  WINDOW JOIN prices p ON t.sym = p.sym " +
                        "  RANGE BETWEEN 1 second PRECEDING AND 1 second FOLLOWING " +
                        ")",
                """
                        max_bid\tmin_bid
                        44.953688102866025\t15.166874474084109
                        """,
                "SELECT max(max_bid) max_bid, min(min_bid) min_bid " +
                        "FROM (" +
                        "  SELECT max(t.price + p.bid) max_bid, min(t.price + p.bid) min_bid " +
                        "  FROM trades t " +
                        "  WINDOW JOIN prices p ON t.sym = p.sym " +
                        "  RANGE BETWEEN 1 second PRECEDING AND 1 second FOLLOWING EXCLUDE PREVAILING " +
                        ")",
                """
                        max_bid\tmin_bid
                        44.953688102866025\t15.166874474084109
                        """
        );
    }

    @Test
    public void testParallelWindowJoinOnSymbolFiltered() throws Exception {
        testParallelWindowJoin(
                "SELECT avg(price) avg_price, max(max_bid_str) max_bid_str " +
                        "FROM (" +
                        "  SELECT t.price price, max(p.bid::string) max_bid_str " +
                        "  FROM trades t " +
                        "  WINDOW JOIN prices p ON t.sym = p.sym " +
                        "  RANGE BETWEEN 1 second PRECEDING AND 1 second FOLLOWING " +
                        "  WHERE t.side = 'sell' " +
                        ")",
                """
                        avg_price\tmax_bid_str
                        19.98409342766\t9.996687893222216
                        """,
                "SELECT avg(price) avg_price, max(max_bid_str) max_bid_str " +
                        "FROM (" +
                        "  SELECT t.price price, max(p.bid::string) max_bid_str " +
                        "  FROM trades t " +
                        "  WINDOW JOIN prices p ON t.sym = p.sym " +
                        "  RANGE BETWEEN 1 second PRECEDING AND 1 second FOLLOWING EXCLUDE PREVAILING " +
                        "  WHERE t.side = 'sell' " +
                        ")",
                """
                        avg_price\tmax_bid_str
                        19.98409342766\t9.996687893222216
                        """
        );
    }

    @Test
    public void testParallelWindowJoinOnSymbolFiltered2() throws Exception {
        // tests thread-unsafe filter
        testParallelWindowJoin(
                "SELECT avg(price) avg_price, max(max_bid) max_bid, min(min_bid) min_bid " +
                        "FROM (" +
                        "  SELECT t.price price, max(p.bid) max_bid, min(p.bid) min_bid " +
                        "  FROM trades t " +
                        "  WINDOW JOIN prices p ON t.sym = p.sym AND concat(p.sym, '_00') = 'sym0_00' " +
                        "  RANGE BETWEEN 1 second PRECEDING AND 1 second FOLLOWING " +
                        "  WHERE concat(t.side, '_00') = 'sell_00' " +
                        ")",
                """
                        avg_price\tmax_bid\tmin_bid
                        19.98409342766\t14.982510448352535\t5.010953919128168
                        """,
                "SELECT avg(price) avg_price, max(max_bid) max_bid, min(min_bid) min_bid " +
                        "FROM (" +
                        "  SELECT t.price price, max(p.bid) max_bid, min(p.bid) min_bid " +
                        "  FROM trades t " +
                        "  WINDOW JOIN prices p ON t.sym = p.sym AND concat(p.sym, '_00') = 'sym0_00' " +
                        "  RANGE BETWEEN 1 second PRECEDING AND 1 second FOLLOWING EXCLUDE PREVAILING " +
                        "  WHERE concat(t.side, '_00') = 'sell_00' " +
                        ")",
                """
                        avg_price\tmax_bid\tmin_bid
                        19.98409342766\t14.982510448352535\t5.010953919128168
                        """
        );
    }

    @Test
    public void testParallelWindowJoinOnSymbolFiltered3() throws Exception {
        // since aggregate functions have an expression with master's and slave's columns
        // as the argument, vectorized execution should not kick in
        testParallelWindowJoin(
                "SELECT max(max_bid) max_bid, min(min_bid) min_bid " +
                        "FROM (" +
                        "  SELECT max(t.price + p.bid) max_bid, min(t.price + p.bid) min_bid " +
                        "  FROM trades t " +
                        "  WINDOW JOIN prices p ON t.sym = p.sym " +
                        "  RANGE BETWEEN 1 second PRECEDING AND 1 second FOLLOWING " +
                        "  WHERE t.side = 'sell' " +
                        ")",
                """
                        max_bid\tmin_bid
                        44.92088394101361\t15.171816406042234
                        """,
                "SELECT max(max_bid) max_bid, min(min_bid) min_bid " +
                        "FROM (" +
                        "  SELECT max(t.price + p.bid) max_bid, min(t.price + p.bid) min_bid " +
                        "  FROM trades t " +
                        "  WINDOW JOIN prices p ON t.sym = p.sym " +
                        "  RANGE BETWEEN 1 second PRECEDING AND 1 second FOLLOWING EXCLUDE PREVAILING " +
                        "  WHERE t.side = 'sell' " +
                        ")",
                """
                        max_bid\tmin_bid
                        44.92088394101361\t15.171816406042234
                        """
        );
    }

    @Test
    public void testParallelWindowJoinOnSymbolFilteredVectorized() throws Exception {
        testParallelWindowJoin(
                "SELECT avg(price) avg_price, max(max_bid) max_bid, min(min_bid) min_bid " +
                        "FROM (" +
                        "  SELECT t.price price, max(p.bid) max_bid, min(p.bid) min_bid " +
                        "  FROM trades t " +
                        "  WINDOW JOIN prices p ON t.sym = p.sym " +
                        "  RANGE BETWEEN 1 second PRECEDING AND 1 second FOLLOWING " +
                        "  WHERE t.side = 'sell' " +
                        ")",
                """
                        avg_price\tmax_bid\tmin_bid
                        19.98409342766\t14.982510448352535\t5.010953919128168
                        """,
                "SELECT avg(price) avg_price, max(max_bid) max_bid, min(min_bid) min_bid " +
                        "FROM (" +
                        "  SELECT t.price price, max(p.bid) max_bid, min(p.bid) min_bid " +
                        "  FROM trades t " +
                        "  WINDOW JOIN prices p ON t.sym = p.sym " +
                        "  RANGE BETWEEN 1 second PRECEDING AND 1 second FOLLOWING EXCLUDE PREVAILING " +
                        "  WHERE t.side = 'sell' " +
                        ")",
                """
                        avg_price\tmax_bid\tmin_bid
                        19.98409342766\t14.982510448352535\t5.010953919128168
                        """
        );
    }

    @Test
    public void testParallelWindowJoinOnSymbolFilteredWithBindVariables() throws Exception {
        testParallelWindowJoin(
                (sqlExecutionContext) -> {
                    BindVariableService bindVariableService = sqlExecutionContext.getBindVariableService();
                    bindVariableService.clear();
                    bindVariableService.setStr("side", "sell");
                },
                "SELECT avg(price) avg_price, max(max_bid_str) max_bid_str " +
                        "FROM (" +
                        "  SELECT t.price price, max(p.bid::string) max_bid_str " +
                        "  FROM trades t " +
                        "  WINDOW JOIN prices p ON t.sym = p.sym " +
                        "  RANGE BETWEEN 1 second PRECEDING AND 1 second FOLLOWING " +
                        "  WHERE t.side = :side " +
                        ")",
                """
                        avg_price\tmax_bid_str
                        19.98409342766\t9.996687893222216
                        """,
                "SELECT avg(price) avg_price, max(max_bid_str) max_bid_str " +
                        "FROM (" +
                        "  SELECT t.price price, max(p.bid::string) max_bid_str " +
                        "  FROM trades t " +
                        "  WINDOW JOIN prices p ON t.sym = p.sym " +
                        "  RANGE BETWEEN 1 second PRECEDING AND 1 second FOLLOWING EXCLUDE PREVAILING " +
                        "  WHERE t.side = :side " +
                        ")",
                """
                        avg_price\tmax_bid_str
                        19.98409342766\t9.996687893222216
                        """
        );
    }

    @Test
    public void testParallelWindowJoinOnSymbolFilteredWithBindVariablesVectorized() throws Exception {
        testParallelWindowJoin(
                (sqlExecutionContext) -> {
                    BindVariableService bindVariableService = sqlExecutionContext.getBindVariableService();
                    bindVariableService.clear();
                    bindVariableService.setStr("side", "sell");
                },
                "SELECT avg(price) avg_price, max(max_bid) max_bid, min(min_bid) min_bid " +
                        "FROM (" +
                        "  SELECT t.price price, max(p.bid) max_bid, min(p.bid) min_bid " +
                        "  FROM trades t " +
                        "  WINDOW JOIN prices p ON t.sym = p.sym " +
                        "  RANGE BETWEEN 1 second PRECEDING AND 1 second FOLLOWING " +
                        "  WHERE t.side = :side " +
                        ")",
                """
                        avg_price\tmax_bid\tmin_bid
                        19.98409342766\t14.982510448352535\t5.010953919128168
                        """,
                "SELECT avg(price) avg_price, max(max_bid) max_bid, min(min_bid) min_bid " +
                        "FROM (" +
                        "  SELECT t.price price, max(p.bid) max_bid, min(p.bid) min_bid " +
                        "  FROM trades t " +
                        "  WINDOW JOIN prices p ON t.sym = p.sym " +
                        "  RANGE BETWEEN 1 second PRECEDING AND 1 second FOLLOWING EXCLUDE PREVAILING " +
                        "  WHERE t.side = :side " +
                        ")",
                """
                        avg_price\tmax_bid\tmin_bid
                        19.98409342766\t14.982510448352535\t5.010953919128168
                        """
        );
    }

    @Test
    public void testParallelWindowJoinOnSymbolThreadUnsafeVectorized() throws Exception {
        // covers case when we need to clone group by functions per-worker since their args aren't thread-safe
        testParallelWindowJoin(
                "SELECT avg(price) avg_price, max(max_bid) max_bid " +
                        "FROM (" +
                        "  SELECT t.price price, max(cast(concat(p.bid, '0') as double)) max_bid " +
                        "  FROM trades t " +
                        "  WINDOW JOIN prices p ON t.sym = p.sym " +
                        "  RANGE BETWEEN 1 second PRECEDING AND 1 second FOLLOWING " +
                        ")",
                """
                        avg_price\tmax_bid
                        19.958398885587915\t14.982510448352535
                        """,
                "SELECT avg(price) avg_price, max(max_bid) max_bid " +
                        "FROM (" +
                        "  SELECT t.price price, max(cast(concat(p.bid, '0') as double)) max_bid " +
                        "  FROM trades t " +
                        "  WINDOW JOIN prices p ON t.sym = p.sym " +
                        "  RANGE BETWEEN 1 second PRECEDING AND 1 second FOLLOWING EXCLUDE PREVAILING " +
                        ")",
                """
                        avg_price\tmax_bid
                        19.958398885587915\t14.982510448352535
                        """
        );
    }

    @Test
    public void testParallelWindowJoinOnSymbolVectorized() throws Exception {
        testParallelWindowJoin(
                "SELECT avg(price) avg_price, max(max_bid) max_bid, min(min_bid) min_bid " +
                        "FROM (" +
                        "  SELECT t.price price, max(p.bid) max_bid, min(p.bid) min_bid " +
                        "  FROM trades t " +
                        "  WINDOW JOIN prices p ON t.sym = p.sym " +
                        "  RANGE BETWEEN 1 second PRECEDING AND 1 second FOLLOWING " +
                        ")",
                """
                        avg_price\tmax_bid\tmin_bid
                        19.958398885587915\t14.982510448352535\t5.010953919128168
                        """,
                "SELECT avg(price) avg_price, max(max_bid) max_bid, min(min_bid) min_bid " +
                        "FROM (" +
                        "  SELECT t.price price, max(p.bid) max_bid, min(p.bid) min_bid " +
                        "  FROM trades t " +
                        "  WINDOW JOIN prices p ON t.sym = p.sym " +
                        "  RANGE BETWEEN 1 second PRECEDING AND 1 second FOLLOWING EXCLUDE PREVAILING " +
                        ")",
                """
                        avg_price\tmax_bid\tmin_bid
                        19.958398885587915\t14.982510448352535\t5.010953919128168
                        """
        );
    }

    @Test
    public void testParallelWindowJoinVectorized() throws Exception {
        testParallelWindowJoin(
                "SELECT avg(price) avg_price, max(max_bid) max_bid, min(min_bid) min_bid " +
                        "FROM (" +
                        "  SELECT t.price price, max(p.bid) max_bid, min(p.bid) min_bid " +
                        "  FROM trades t " +
                        "  WINDOW JOIN prices p " +
                        "  RANGE BETWEEN 100 milliseconds PRECEDING AND 100 milliseconds FOLLOWING " +
                        ")",
                """
                        avg_price\tmax_bid\tmin_bid
                        19.958398885587915\t14.982510448352535\t5.010953919128168
                        """,
                "SELECT avg(price) avg_price, max(max_bid) max_bid, min(min_bid) min_bid " +
                        "FROM (" +
                        "  SELECT t.price price, max(p.bid) max_bid, min(p.bid) min_bid " +
                        "  FROM trades t " +
                        "  WINDOW JOIN prices p " +
                        "  RANGE BETWEEN 100 milliseconds PRECEDING AND 100 milliseconds FOLLOWING EXCLUDE PREVAILING " +
                        ")",
                """
                        avg_price\tmax_bid\tmin_bid
                        19.958398885587915\t14.982510448352535\t5.010953919128168
                        """
        );
    }

    private void testParallelWindowJoin(String... queriesAndExpectedResults) throws Exception {
        testParallelWindowJoin(null, queriesAndExpectedResults);
    }

    private void testParallelWindowJoin(BindVariablesInitializer initializer, String... queriesAndExpectedResults) throws Exception {
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        if (initializer != null) {
                            initializer.init(sqlExecutionContext);
                        }

                        engine.execute(
                                """
                                        CREATE TABLE IF NOT EXISTS trades (
                                                ts TIMESTAMP,
                                                sym SYMBOL CAPACITY 2048,
                                                side SYMBOL CAPACITY 4,
                                                price DOUBLE,
                                                amount DOUBLE
                                        ) TIMESTAMP(ts) PARTITION BY DAY;
                                        """,
                                sqlExecutionContext
                        );
                        engine.execute(
                                "INSERT INTO trades" +
                                        "  SELECT " +
                                        "      '2020-01-01T00:05'::timestamp + (10000*x) + rnd_long(-200, 200, 0) as ts, " +
                                        "      rnd_symbol_zipf(100, 2.0) AS sym, " +
                                        "      rnd_symbol('buy', 'sell') as side, " +
                                        "      rnd_double() * 20 + 10 AS price, " +
                                        "      rnd_double() * 20 + 10 AS amount " +
                                        "  FROM long_sequence(" + ROW_COUNT + ");",
                                sqlExecutionContext
                        );
                        engine.execute(
                                """
                                        CREATE TABLE prices (
                                            ts TIMESTAMP,
                                            sym SYMBOL CAPACITY 1024,
                                            bid DOUBLE,
                                            ask DOUBLE
                                        ) TIMESTAMP(ts) PARTITION BY DAY;
                                        """,
                                sqlExecutionContext
                        );
                        engine.execute(
                                "INSERT INTO prices " +
                                        "  SELECT " +
                                        "      '2020-01-01'::timestamp + (60000*x) + rnd_long(-200, 200, 0) as ts, " +
                                        "      rnd_symbol_zipf(100, 2.0) as sym, " +
                                        "      rnd_double() * 10.0 + 5.0 as bid, " +
                                        "      rnd_double() * 10.0 + 5.0 as ask " +
                                        "  FROM long_sequence(" + 10 * ROW_COUNT + ");",
                                sqlExecutionContext
                        );

                        assertQueries(engine, sqlExecutionContext, queriesAndExpectedResults);
                    },
                    configuration,
                    LOG
            );
        });
    }

    static void assertQueries(CairoEngine engine, SqlExecutionContext sqlExecutionContext, String... queriesAndExpectedResults) throws SqlException {
        for (int i = 0, n = queriesAndExpectedResults.length; i < n; i += 2) {
            final String query = queriesAndExpectedResults[i];
            final String expected = queriesAndExpectedResults[i + 1];
            TestUtils.assertSql(
                    engine,
                    sqlExecutionContext,
                    query,
                    sink,
                    expected
            );
        }
    }

    private interface BindVariablesInitializer {
        void init(SqlExecutionContext sqlExecutionContext) throws SqlException;
    }
}
