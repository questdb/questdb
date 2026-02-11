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
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.TableWriter;
import io.questdb.griffin.SqlException;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class HorizonJoinFuzzTest extends AbstractCairoTest {
    private static final String[] AGGREGATE_FUNCTIONS = new String[]{
            "first", "last", "count", "max", "min", "avg", "sum"
    };
    private static final boolean RUN_ALL_PERMUTATIONS = true;
    private static final int RUN_N_PERMUTATIONS = 10;
    private Rnd rnd;

    @Override
    @Before
    public void setUp() {
        rnd = TestUtils.generateRandom(LOG);
        final boolean enableParallelHorizonJoin = rnd.nextBoolean();
        LOG.info().$("parallel horizon join enabled: ").$(enableParallelHorizonJoin).$();
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HORIZON_JOIN_ENABLED, String.valueOf(enableParallelHorizonJoin));
        // async horizon join uses small page frames
        setProperty(PropertyKey.CAIRO_SMALL_SQL_PAGE_FRAME_MAX_ROWS, 100);
        setProperty(PropertyKey.CAIRO_SQL_ASOF_JOIN_LOOKAHEAD, 5);
        super.setUp();
    }

    @Test
    public void testHorizonJoinFuzz() throws Exception {
        assertMemoryLeak(() -> {
            CharSequence[] symbols = new CharSequence[rnd.nextInt(20) + 4];
            for (int i = 0; i < symbols.length; i++) {
                symbols[i] = "sym" + i;
            }

            long avgTradeSpread = generateTradeSpread(rnd);
            int tradeSize = rnd.nextInt(100) + 1;
            int duplicatePercentage = 30 + rnd.nextInt(71);
            var aggregatedColumns = prepareFuzzTables(rnd, tradeSize, avgTradeSpread, duplicatePercentage, symbols);
            var aggregates = prepareFuzzAggregations(rnd, aggregatedColumns);
            String horizonAggregates = aggregates.toString().replace("(val", "(p.val");

            final Object[][] allOpts = new Object[][]{
                    // left table - ts filter
                    {false, true},
                    // left table - symbol filter
                    {false, true},
                    // left table - value filter
                    {false, true},
                    // left table - limit
                    {false, true},
                    // symbol eq (ON clause key)
                    {false, true},
                    // group by sym (keyed aggregation)
                    {false, true},
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
                boolean groupBySym = (boolean) permutation[5];

                // groupBySym requires symbolEq for the ASOF key
                if (groupBySym) {
                    symbolEq = true;
                }

                var tradesInner = generateFuzzTradeTable(rnd, symbols, tradeSize, avgTradeSpread, filterTs, filterSymbol, filterValue, limit);

                // Generate offsets (randomly choose RANGE or LIST)
                boolean useList = rnd.nextBoolean();
                long[] offsetsMicros;
                String horizonClause;
                if (useList) {
                    Set<Long> uniqueOffsets = new LinkedHashSet<>();
                    int count = 1 + rnd.nextInt(5);
                    while (uniqueOffsets.size() < count) {
                        int sec = -5 + rnd.nextInt(11);
                        uniqueOffsets.add(sec * 1_000_000L);
                    }
                    offsetsMicros = uniqueOffsets.stream().mapToLong(Long::longValue).sorted().toArray();
                    StringBuilder clause = new StringBuilder("LIST (");
                    for (int i = 0; i < offsetsMicros.length; i++) {
                        if (i > 0) {
                            clause.append(", ");
                        }
                        clause.append(offsetsMicros[i] / 1_000_000L).append('s');
                    }
                    clause.append(") AS h");
                    horizonClause = clause.toString();
                } else {
                    int fromSec = -(rnd.nextInt(6));
                    int stepSec = 1 + rnd.nextInt(3);
                    int numSteps = 1 + rnd.nextInt(5);
                    int toSec = fromSec + stepSec * numSteps;
                    int count = numSteps + 1;
                    offsetsMicros = new long[count];
                    for (int i = 0; i < count; i++) {
                        offsetsMicros[i] = (fromSec + (long) stepSec * i) * 1_000_000L;
                    }
                    horizonClause = "RANGE FROM " + fromSec + "s TO " + toSec + "s STEP " + stepSec + "s AS h";
                }

                assertFuzzExecute(
                        tradesInner, symbolEq, groupBySym,
                        horizonAggregates, aggregates.toString(),
                        aggregatedColumns, horizonClause, offsetsMicros
                );
            }
        });
    }

    private void assertFuzzExecute(
            CharSequence tradesInner,
            boolean symbolEq,
            boolean groupBySym,
            CharSequence horizonAggregates,
            CharSequence referenceAggregates,
            CharSequence[] aggregatedColumns,
            CharSequence horizonClause,
            long[] offsetsMicros
    ) throws SqlException {
        // Build HORIZON JOIN query
        sink.clear();
        sink
                .put("SELECT h.offset AS h_offset");
        if (groupBySym) {
            sink.put(", t.sym");
        }
        sink
                .put(", ").put(horizonAggregates)
                .put(" FROM ").put(tradesInner).put(" AS t")
                .put(" HORIZON JOIN prices AS p");
        if (symbolEq) {
            sink.put(" ON (t.sym = p.sym)");
        }
        sink
                .put(' ').put(horizonClause)
                .put(" ORDER BY h_offset");
        if (groupBySym) {
            sink.put(", t.sym");
        }
        var horizonQuery = sink.toString();

        // Build reference query: UNION ALL of ASOF JOINs per offset, then GROUP BY.
        // HORIZON JOIN at offset O is equivalent to ASOF JOIN where master timestamps
        // are shifted forward by O microseconds.
        sink.clear();
        sink
                .put("SELECT h_offset");
        if (groupBySym) {
            sink.put(", sym");
        }
        sink
                .put(", ").put(referenceAggregates)
                .put(" FROM (");

        for (int i = 0; i < offsetsMicros.length; i++) {
            if (i > 0) {
                sink.put(" UNION ALL ");
            }
            sink
                    .put("SELECT cast(").put(offsetsMicros[i]).put(" AS long) AS h_offset");
            if (groupBySym) {
                sink.put(", t.sym");
            }
            for (CharSequence col : aggregatedColumns) {
                sink.put(", p.").put(col);
            }
            sink
                    .put(" FROM (SELECT * FROM (SELECT dateadd('u', ")
                    .put(offsetsMicros[i])
                    .put(", ts) AS ts, id, sym, price FROM ")
                    .put(tradesInner)
                    .put(") TIMESTAMP(ts)) t ASOF JOIN prices p");
            if (symbolEq) {
                sink.put(" ON (t.sym = p.sym)");
            }
        }

        sink.put(") GROUP BY h_offset");
        if (groupBySym) {
            sink.put(", sym");
        }
        sink.put(" ORDER BY h_offset");
        if (groupBySym) {
            sink.put(", sym");
        }
        var referenceQuery = sink.toString();

        // Execute and compare
        final StringSink actualSink = new StringSink();
        printSql(horizonQuery, actualSink);

        final StringSink expectedSink = new StringSink();
        printSql(referenceQuery, expectedSink);

        try {
            TestUtils.assertEquals(expectedSink, actualSink);
        } catch (AssertionError e) {
            LOG.error().$("HORIZON JOIN query: ").$(horizonQuery).$();
            LOG.error().$("Reference query: ").$(referenceQuery).$();
            throw e;
        }
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
            return "trades";
        }

        return "(trades" + sink + ")";
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
            int duplicatePercentage,
            CharSequence[] symbols
    ) throws SqlException {
        final CharSequence[] columnTypes = new CharSequence[]{"double", "float", "long"};
        execute(
                """
                        create table trades (
                            id int,
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
            int symbolFrequency = switch (c <= 2 ? 0 : c <= 4 ? 1 : 2) {
                // Very rare
                case 0 -> rnd.nextPositiveInt() % 5;
                // A lot
                case 1 -> 100 + rnd.nextPositiveInt() % 5000;
                // Average
                default -> 50 + rnd.nextPositiveInt() % 100;
            };
            symbolFrequencySum += symbolFrequency;
            symbolFrequencies[i] = symbolFrequencySum;
        }

        int[] priceFrequencies = new int[symbols.length];
        int priceFrequencySum = 0;
        for (int i = 0; i < priceFrequencies.length; i++) {
            int c = rnd.nextPositiveInt() % 100;
            int priceFrequency = switch (c <= 2 ? 0 : c <= 4 ? 1 : 2) {
                // Very rare
                case 0 -> rnd.nextPositiveInt() % 5;
                // A lot
                case 1 -> 100 + rnd.nextPositiveInt() % 5000;
                // Average
                default -> 50 + rnd.nextPositiveInt() % 100;
            } * symbolFrequencies[i];
            priceFrequencySum += priceFrequency;
            priceFrequencies[i] = priceFrequencySum;
        }

        long tradeStart = MicrosTimestampDriver.INSTANCE.fromSeconds(rnd.nextLong(4000000000L));

        long ts = tradeStart;
        try (TableWriter w = newOffPoolWriter("trades")) {
            CharSequence symbol = null;
            for (int i = 0; i < tradeSize; i++) {
                if (i == 0 || rnd.nextInt(100) < 100 - duplicatePercentage) {
                    ts += rnd.nextPositiveLong() % (avgTradeSpread << 1);
                    int symbolIdx = rnd.nextInt(symbolFrequencySum);
                    for (int j = 0; j < symbolFrequencies.length; j++) {
                        if (symbolIdx < symbolFrequencies[j]) {
                            symbol = symbols[j];
                            break;
                        }
                    }
                }

                TableWriter.Row r = w.newRow(ts);
                r.putInt(0, i);
                r.putSym(1, symbol);
                r.putDouble(2, rnd.nextDouble() * 100);
                r.append();
            }
            w.commit();
        }

        // Keep some trades without matching price symbols
        if (rnd.nextBoolean()) {
            symbols[rnd.nextPositiveInt() % symbols.length] = "sym_no_price";
        }

        int avgPricePerTradeRatio = Math.min(Math.max(priceFrequencySum / (symbolFrequencySum * symbols.length), 1), 50);
        int priceSize = tradeSize * avgPricePerTradeRatio;
        long avgPriceSpread = avgTradeSpread / avgPricePerTradeRatio;
        ts = tradeStart - avgPriceSpread * rnd.nextLong(symbols.length * 10L);
        try (TableWriter w = newOffPoolWriter("prices")) {
            CharSequence symbol = null;
            for (int i = 0; i < priceSize; i++) {
                if (i == 0 || rnd.nextInt(100) < 100 - duplicatePercentage) {
                    ts += (avgPriceSpread / symbols.length) + rnd.nextLong(1000);
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
