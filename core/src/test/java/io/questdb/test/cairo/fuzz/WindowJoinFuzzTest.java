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

import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.TableWriter;
import io.questdb.griffin.SqlException;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

// This is not a fuzz test in traditional sense, but it's multithreaded, and we want to run it
// in CI frequently along with other fuzz tests.
public class WindowJoinFuzzTest extends AbstractCairoTest {
    private static final CharSequence[] AGGREGATE_FUNCTIONS = new CharSequence[]{
            // "first", "last", - uncomment when https://github.com/questdb/questdb/pull/6414 is merged
            "count", "max", "min", "avg", "sum"
    };
    private static final CharSequence[] EQ_OPERATORS = new CharSequence[]{
            "<=", ">=", "<>", "!=", "<", ">",
    };
    private static final int FUZZ_ITERATIONS = 1;
    private static final boolean RUN_ALL_PERMUTATIONS = true;
    private static final int RUN_N_PERMUTATIONS = 10;

    @Test
    public void testFuzz() throws Exception {
        for (int i = 0; i < FUZZ_ITERATIONS; i++) {
            assertFuzz();

            execute("DROP TABLE trades");
            execute("DROP TABLE prices");
        }
    }

    private void assertFuzz() throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);

        assertMemoryLeak(() -> {
            CharSequence[] symbols = new CharSequence[rnd.nextInt(100) + 1];
            for (int i = 0; i < symbols.length; i++) {
                symbols[i] = "sym" + i;
            }

            long avgTradeSpread = generateTradeSpread(rnd);
            int tradeSize = rnd.nextInt(25) + 1;
            var aggregatedColumns = prepareFuzzTables(rnd, tradeSize, avgTradeSpread, symbols);
            var aggregates = prepareFuzzAggregations(rnd, aggregatedColumns);

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

            for (Object[] permutation : permutations) {
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
                        var col = aggregatedColumns[rnd.nextInt(aggregatedColumns.length)];
                        sink.put("t.price").put(EQ_OPERATORS[rnd.nextInt(EQ_OPERATORS.length)]).put(col);
                    }
                }
                var joinFilter = sink.toString();

                assertFuzzExecute(leftTable, joinFilter, preceding, following, aggregates, aggregatedColumns);
            }
        });
    }

    private void assertFuzzExecute(
            CharSequence leftTable,
            CharSequence joinFilter,
            long preceding,
            long following,
            CharSequence aggregates,
            CharSequence[] aggregatedColumns
    ) throws SqlException {
        sink.clear();
        sink
                .put("SELECT t.sym, t.price, t.ts, ")
                .put(aggregates)
                .put(" FROM ");
        var select = sink.toString();

        // region window-join query
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
        // endregion

        // region oracle - left-join query
        sink.clear();
        sink.put(select);
        // We need to use a sub-query to ensure that slaves are processed in the correct order (timestamp)
        sink.put("(SELECT t.sym, t.price, t.ts, ");
        for (int i = 0, n = aggregatedColumns.length; i < n; i++) {
            sink.put(aggregatedColumns[i]).put(", ");
        }
        sink
                .put("p.ts, p.id FROM ")
                .put(leftTable)
                .put(" LEFT JOIN prices p ON p.ts >= dateadd('u', -")
                .put(preceding)
                .put(", t.ts) AND p.ts <= dateadd('u', ")
                .put(following)
                .put(", t.ts)");
        if (!joinFilter.isEmpty()) {
            sink.put(" AND (").put(joinFilter).put(')');
        }
        sink.put(" ORDER BY t.ts, p.id) t ORDER BY t.ts, t.sym");

        var leftJoinQuery = sink.toString();
        // endregion

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

    private long generateTradeSpread(Rnd rnd) {
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

    private CharSequence prepareFuzzAggregations(Rnd rnd, CharSequence[] aggregatedColumns) {
        var aggregates = new StringBuilder();
        for (int i = 0, n = 1 + rnd.nextInt(6); i < n; i++) {
            if (i > 0) {
                aggregates.append(", ");
            }
            final CharSequence func = AGGREGATE_FUNCTIONS[rnd.nextInt(AGGREGATE_FUNCTIONS.length)];
            aggregates.append(func)
                    .append('(')
                    .append(aggregatedColumns[rnd.nextInt(aggregatedColumns.length)])
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
        execute(
                """
                        create table trades (
                            sym symbol,
                            price double,
                            ts timestamp
                        ) timestamp(ts) partition by day bypass wal;
                        """
        );

        var nAggregatedColumns = rnd.nextPositiveInt() % 3 + 1;
        var aggregatedColumns = new CharSequence[nAggregatedColumns];
        var aggregatedColumnTypes = new int[nAggregatedColumns];
        StringBuilder columnsCreation = new StringBuilder();
        for (int i = 0; i < aggregatedColumns.length; i++) {
            aggregatedColumns[i] = "val" + i;
            var columnType = rnd.nextPositiveInt() % columnTypes.length;
            aggregatedColumnTypes[i] = columnType;
            columnsCreation
                    .append(aggregatedColumns[i])
                    .append(" ")
                    .append(columnTypes[columnType])
                    .append(",\n");
        }

        execute(
                """
                        create table prices (
                            id int,
                            sym symbol,
                        """ + columnsCreation + """
                            ts timestamp
                        ) timestamp(ts) partition by day bypass wal;
                        """
        );

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
                r.putInt(0, i);
                r.putSym(1, symbol);
                for (int j = 0; j < nAggregatedColumns; j++) {
                    switch (aggregatedColumnTypes[j]) {
                        case 0 -> r.putDouble(j + 2, rnd.nextLong(100));
                        case 1 -> r.putFloat(j + 2, rnd.nextLong(100));
                        case 2 -> r.putLong(j + 2, rnd.nextLong(100));
                        default -> throw new IllegalStateException("Unexpected value: " + aggregatedColumnTypes[j]);
                    }
                }
                r.append();
            }
            w.commit();
        }

        return aggregatedColumns;
    }
}
