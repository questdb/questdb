/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.TableWriter;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.mp.WorkerPool;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

// This is not a fuzz test in traditional sense, but it's multithreaded, and we want to run it
// in CI frequently along with other fuzz tests.
public class ParallelWindowJoinFuzzTest extends AbstractCairoTest {
    private static final CharSequence[] AGGREGATE_FUNCTIONS = new CharSequence[]{
//            "avg", "sum", "first", "last" - uncomment when https://github.com/questdb/questdb/issues/6405 is solved
            "count", "max", "min"
    };
    private static final CharSequence[] EQ_OPERATORS = new CharSequence[]{
            "<=", ">=", "<>", "!=", "<", ">",
    };
    private static final int FUZZ_ITERATIONS = 1;
    private static final int PAGE_FRAME_COUNT = 4; // also used to set queue size, so must be a power of 2
    private static final int PAGE_FRAME_MAX_ROWS = 100;
    private static final int ROW_COUNT = 10 * PAGE_FRAME_COUNT * PAGE_FRAME_MAX_ROWS;
    private static final boolean RUN_ALL_PERMUTATIONS = true;
    private static final int RUN_N_PERMUTATIONS = 10;
    private final boolean enableParallelWindowJoin;

    public ParallelWindowJoinFuzzTest() {
        this.enableParallelWindowJoin = TestUtils.generateRandom(LOG).nextBoolean();
        LOG.info().$("parallel window join enabled: ").$(enableParallelWindowJoin).$();
    }

    @Override
    @Before
    public void setUp() {
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, PAGE_FRAME_MAX_ROWS);
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, PAGE_FRAME_MAX_ROWS);
        // We intentionally use small values for shard count and reduce
        // queue capacity to exhibit various edge cases.
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_SHARD_COUNT, 2);
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_REDUCE_QUEUE_CAPACITY, PAGE_FRAME_COUNT);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_JOIN_ENABLED, String.valueOf(enableParallelWindowJoin));
        super.setUp();
    }

    @Test
    public void testFuzz() throws Exception {
        for (int i = 0; i < FUZZ_ITERATIONS; i++) {
            assertFuzz();

            execute("DROP TABLE trades");
            execute("DROP TABLE prices");
        }
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
                        """
        );
    }

    private void assertFuzz() throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);

        assertMemoryLeak(() -> {
            CharSequence[] symbols = new CharSequence[rnd.nextInt(100) + 1];
            for (int i = 0; i < symbols.length; i++) {
                symbols[i] = "sym" + i;
            }

            long avgTradeSpread = generateTradeSpead(rnd);
            int tradeSize = rnd.nextInt(25) + 1;
            var aggregatableColumns = prepareFuzzTables(rnd, tradeSize, avgTradeSpread, symbols);
            var aggregates = prepareFuzzAggregations(rnd, aggregatableColumns);

            final Object[][] allOpts = new Object[][]{
                    // left table - ts filter
                    {false, true},
                    // left table - symbol filter
                    {false, true},
                    // left table - value filter
                    {false, true},
                    // left table - limit
                    {false, /*true*/}, // TODO: Fix WindowJoinTest#testMasterFilterLimit before enabling this
                    // symbol eq
                    {false, true},
                    // join filter
                    {false, true}
            };

            final Object[][] allPermutations = TestUtils.cartesianProduct(allOpts);
            final Object[][] permutations;
            if (RUN_ALL_PERMUTATIONS) {
                permutations = allPermutations;
            } else {
                List<Object[]> allPermutationsList = Arrays.asList(allPermutations);
                Collections.shuffle(allPermutationsList);
                permutations = Arrays.copyOf(allPermutations, RUN_N_PERMUTATIONS);
            }

            for (int perm = 0, n = permutations.length; perm < n; perm++) {
                Object[] permutation = permutations[perm];
                boolean filterTs = (boolean) permutation[0];
                boolean filterSymbol = (boolean) permutation[1];
                boolean filterValue = (boolean) permutation[2];
                boolean limit = (boolean) permutation[3];
                boolean symbolEq = (boolean) permutation[4];
                boolean joinFiltered = (boolean) permutation[5];

                var leftTable = generateFuzzTradeTable(rnd, symbols, tradeSize, avgTradeSpread, filterTs, filterSymbol, filterValue, limit);
                long preceding = rnd.nextInt(symbols.length * 2) * avgTradeSpread;
                long following = rnd.nextInt(symbols.length * 2) * avgTradeSpread;

                sink.clear();
                if (symbolEq) {
                    sink.put("(t.sym = p.sym)");
                }
                if (joinFiltered) {
                    for (int i = 0, o = rnd.nextInt(3) + 1; i < o; i++) {
                        if (!sink.isEmpty()) {
                            if (rnd.nextBoolean()) {
                                sink.put(" AND ");
                            } else {
                                sink.put(" OR ");
                            }
                        }
                        var col = aggregatableColumns[rnd.nextInt(aggregatableColumns.length)];
                        sink.put("t.price").put(EQ_OPERATORS[rnd.nextInt(EQ_OPERATORS.length)]).put(col);
                    }
                }
                var joinFilter = sink.toString();

                assertFuzzExecute(leftTable, joinFilter, preceding, following, aggregates, aggregatableColumns);
            }
        });
    }

    private void assertFuzzExecute(
            CharSequence leftTable,
            CharSequence joinFilter,
            long preceding,
            long following,
            CharSequence aggregates,
            CharSequence[] aggregatableColumns
    ) throws SqlException {
        sink.clear();
        sink
                .put("SELECT t.sym, t.price, t.ts, ")
                .put(aggregates)
                .put(" FROM ");
        var select = sink.toString();


        //region window-join query
        sink.clear();
        sink
                .put(select)
                .put(leftTable)
                .put(" WINDOW JOIN prices p");
        if (!joinFilter.isEmpty()) {
            sink.put(" ON ").put(joinFilter);
        }
        sink.put(" RANGE BETWEEN ")
                .put(preceding)
                .put(" microseconds PRECEDING AND ")
                .put(following)
                .put(" microseconds FOLLOWING ORDER BY t.ts, t.sym");

        var windowQuery = sink.toString();
        //endregion

        //region oracle - left-join query
        sink.clear();
        sink.put(select);
        // We need to use a sub-query to ensure that slaves are processed in the correct order (timestamp)
        sink.put("(SELECT t.sym, t.price, t.ts, ");
        for (int i = 0, n = aggregatableColumns.length; i < n; i++) {
            sink.put(aggregatableColumns[i]).put(", ");
        }
        sink
                .put("p.ts FROM ")
                .put(leftTable)
                .put(" LEFT JOIN prices p ON p.ts >= dateadd('u', -")
                .put(preceding)
                .put(", t.ts) AND p.ts <= dateadd('u', ")
                .put(following)
                .put(", t.ts)");
        if (!joinFilter.isEmpty()) {
            sink.put(" AND (").put(joinFilter).put(')');
        }
        sink.put(" ORDER BY t.ts, p.ts) t ORDER BY t.ts, t.sym");

        var leftJoinQuery = sink.toString();
        //endregion

        final StringSink actualSink = new StringSink();
        printSql(windowQuery, actualSink);

        final StringSink expectedSink = new StringSink();
        printSql(leftJoinQuery, expectedSink);

        TestUtils.assertEquals(expectedSink, actualSink);
    }

    private CharSequence generateFuzzTradeTable(
            Rnd rnd,
            CharSequence[] symbols,
            int tradeSize,
            long avgTradeSpread,
            boolean filterTs,
            boolean filterSymbol,
            boolean filterValue,
            boolean limited
    ) {
        sink.clear();
        if (filterTs) {
            sink.put(" WHERE ");
            final long startUs = MicrosTimestampDriver.INSTANCE.parseFloorLiteral("2020-01-01T00:00:00.000000Z");
            final long ts = startUs + avgTradeSpread * (rnd.nextPositiveLong() % tradeSize);
            if (rnd.nextBoolean()) {
                sink.put("ts >= ").put(ts);
            } else {
                sink.put("ts < ").put(ts);
            }
        }
        if (filterSymbol) {
            if (!sink.isEmpty()) {
                sink.put(" AND ");
            } else {
                sink.put(" WHERE ");
            }
            if (rnd.nextBoolean()) {
                sink.put("sym = '").put(symbols[rnd.nextPositiveInt() % symbols.length]).put("'");
            } else {
                sink.put("sym IN (");
                for (int i = 0, n = rnd.nextInt(3) + 2; i < n; i++) {
                    sink.put("'").put(symbols[rnd.nextPositiveInt() % symbols.length]).put("'");
                    if (i < n - 1) {
                        sink.put(", ");
                    }
                }
                sink.put(")");
            }
        }
        if (filterValue) {
            if (!sink.isEmpty()) {
                sink.put(" AND ");
            } else {
                sink.put(" WHERE ");
            }
            sink.put("price >= ").put(rnd.nextDouble() * 100);
        }

        if (limited) {
            sink.put(" LIMIT ");
            int lo = 0;
            if (rnd.nextBoolean()) {
                lo = rnd.nextInt(tradeSize);
                sink.put(lo).put(", ");
            }
            sink.put(lo + rnd.nextInt(tradeSize - lo));
        }

        if (sink.isEmpty()) {
            return "trades t";
        }

        return "(trades" + sink + ") t";
    }

    private long generateTradeSpead(Rnd rnd) {
        return switch (rnd.nextInt(3)) {
            // Small average spread (1ms-10ms)
            case 0 -> 1000 + rnd.nextLong(9000);
            // Medium average spread (10ms-250ms)
            case 1 -> 10_000 + rnd.nextLong(240_000);
            // Large average spread (250ms-10s)
            case 2 -> 250_000 + rnd.nextLong(9_750_000);
            // Very large average spread (30s-1h)
            default -> 30_000_000 + rnd.nextLong(3_570_000_000L);
        };
    }

    private CharSequence prepareFuzzAggregations(Rnd rnd, CharSequence[] aggregatableColumns) {
        var aggregates = new StringBuilder();
        for (int i = 0, n = 1 + rnd.nextInt(6); i < n; i++) {
            if (i > 0) {
                aggregates.append(", ");
            }
            aggregates.append(AGGREGATE_FUNCTIONS[rnd.nextInt(AGGREGATE_FUNCTIONS.length)])
                    .append('(')
                    .append(aggregatableColumns[rnd.nextInt(aggregatableColumns.length)])
                    .append(") agg")
                    .append(i);
        }
        return aggregates;
    }

    private CharSequence[] prepareFuzzTables(
            Rnd rnd,
            int tradeSize,
            long avgTradeSpread,
            CharSequence[] symbols
    ) throws SqlException {
        final CharSequence[] columnTypes = new CharSequence[]{"double", "float", "long"};
        execute("""
                create table trades (
                    sym symbol,
                    price double,
                    ts timestamp
                ) timestamp(ts) partition by day bypass wal;""");

        var nAggregatableColumns = rnd.nextPositiveInt() % 3 + 1;
        var aggregatableColumns = new CharSequence[nAggregatableColumns];
        var aggregatedColumnTypes = new int[nAggregatableColumns];
        StringBuilder columnsCreation = new StringBuilder();
        for (int i = 0; i < aggregatableColumns.length; i++) {
            aggregatableColumns[i] = "val" + i;
            var columnType = rnd.nextPositiveInt() % columnTypes.length;
            aggregatedColumnTypes[i] = columnType;
            columnsCreation
                    .append(aggregatableColumns[i])
                    .append(" ")
                    .append(columnTypes[columnType])
                    .append(",\n");
        }

        execute("""
                create table prices (
                    sym symbol,
                """ + columnsCreation + """
                    ts timestamp
                ) timestamp(ts) partition by day bypass wal;""");

        // We fill 2 tables: trades and prices.
        // - Some symbols will have more entries than others -> symbolFrequencies (avg 100)
        // - Some symbols will have a bigger prices per trade ratio than others -> priceFrequencies (avg 100)
        // - Some prices have duplicate entries (same symbol-timestamp pair)

        int[] symbolFrequencies = new int[symbols.length];
        int symbolFrequencySum = 0;
        for (int i = 0; i < symbolFrequencies.length; i++) {
            int c = rnd.nextPositiveInt() % 100;
            switch (c <= 2 ? 0 : c <= 4 ? 1 : 2) {
                // Very rare
                case 0 -> symbolFrequencySum += rnd.nextPositiveInt() % 5;
                // A lot
                case 1 -> symbolFrequencySum += 100 + rnd.nextPositiveInt() % 5000;
                // Average
                case 2 -> symbolFrequencySum += 50 + rnd.nextPositiveInt() % 100;
            }
            symbolFrequencies[i] = symbolFrequencySum;
        }

        int[] priceFrequencies = new int[symbols.length];
        int priceFrequencySum = 0;
        for (int i = 0; i < priceFrequencies.length; i++) {
            int c = rnd.nextPositiveInt() % 100;
            priceFrequencySum += switch (c <= 2 ? 0 : c <= 4 ? 1 : 2) {
                // Very rare
                case 0 -> rnd.nextPositiveInt() % 5;
                // A lot
                case 1 -> 100 + rnd.nextPositiveInt() % 5000;
                // Average
                default -> 50 + rnd.nextPositiveInt() % 100;
            } * symbolFrequencies[i];
            priceFrequencies[i] = priceFrequencySum;
        }

        long tradeStart = MicrosTimestampDriver.INSTANCE.fromSeconds(rnd.nextLong(4000000000L));

        long ts = tradeStart;
        try (TableWriter w = newOffPoolWriter("trades")) {
            for (int i = 0; i < tradeSize; i++) {
                ts += rnd.nextPositiveLong() % (avgTradeSpread << 1);
                CharSequence symbol = null;
                int symbolIdx = rnd.nextInt(symbolFrequencySum);
                for (int j = 0; j < symbolFrequencies.length; j++) {
                    if (symbolIdx < symbolFrequencies[j]) {
                        symbol = symbols[j];
                        break;
                    }
                }

                TableWriter.Row r = w.newRow(ts);
                r.putSym(0, symbol);
                r.putDouble(1, rnd.nextDouble() * 100);
                r.append();
            }
            w.commit();
        }

        // Keep some trades without matching price symbols
        if (rnd.nextBoolean()) {
            symbols[rnd.nextPositiveInt() % symbols.length] = "sym_no_price";
        }

        int avgPricePerTradeRatio = Math.max(priceFrequencySum / (symbolFrequencySum * symbols.length), 1);
        int priceSize = tradeSize * avgPricePerTradeRatio;
        long avgPriceSpread = avgTradeSpread / avgPricePerTradeRatio;
        ts = tradeStart - avgPriceSpread * rnd.nextInt(symbols.length * 10);
        try (TableWriter w = newOffPoolWriter("prices")) {
            CharSequence symbol = null;
            for (int i = 0; i < priceSize; i++) {
                if (i == 0 || rnd.nextInt(100) <= 98) {
                    ts += rnd.nextLong(Math.max(avgPriceSpread << 1, 1));
                    int symbolIdx = rnd.nextInt(priceFrequencySum);
                    for (int j = 0; j < priceFrequencies.length; j++) {
                        if (symbolIdx < priceFrequencies[j]) {
                            symbol = symbols[j];
                            break;
                        }
                    }
                }

                TableWriter.Row r = w.newRow(ts);
                r.putSym(0, symbol);
                for (int j = 0; j < nAggregatableColumns; j++) {
                    switch (aggregatedColumnTypes[j]) {
                        case 0 -> r.putDouble(j + 1, rnd.nextDouble() * 100);
                        case 1 -> r.putFloat(j + 1, rnd.nextFloat() * 100);
                        case 2 -> r.putLong(j + 1, rnd.nextLong());
                        default -> throw new IllegalStateException("Unexpected value: " + aggregatedColumnTypes[j]);
                    }
                }
                r.append();
            }
            w.commit();
        }

        return aggregatableColumns;
    }

    private void testParallelWindowJoin(String... queriesAndExpectedResults) throws Exception {
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
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
}
