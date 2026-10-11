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

package io.questdb.test.griffin.engine.join;

import io.questdb.cairo.CairoException;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Compares a time-series join after INNER and CROSS joins with the same query that sorts those joins
 * by the timestamp of the table that the query selects from, in a sub-query. A time-series join reads
 * the designated timestamp of the FROM table, whatever order the optimiser runs the joins before it in,
 * and the sort gives the sub-query that timestamp in any order. The generated joins let the optimiser
 * move a join key onto the FROM table: the FROM table is CROSS joined with the next table, and a later
 * INNER join is keyed on both. Keys compare SYMBOL, VARCHAR and computed columns, so the optimiser may
 * group keys that compare one column with columns of different types. The data holds NULL keys and
 * rows that do not match.
 */
public class TimeSeriesJoinOrderFuzzTest extends AbstractCairoTest {
    // the key columns of every table: SYMBOL s, VARCHAR v, INT k and LONG l
    private static final String KEY_COLUMNS = "svkl";
    private static final Log LOG = LogFactory.getLog(TimeSeriesJoinOrderFuzzTest.class);
    private static final int QUERY_COUNT = 500;
    private static final int TABLE_COUNT = 5;

    @Test
    public void testTimeSeriesJoinReadsFromTableTimestamp() throws Exception {
        assertFlatJoinReturnsRowsOfReference(false);
    }

    @Test
    public void testTimeSeriesJoinReadsFromTableTimestampFullFat() throws Exception {
        assertFlatJoinReturnsRowsOfReference(true);
    }

    private static boolean isFullFatValueTypeError(Throwable e, boolean fullFatJoins) {
        // a full-fat join stores the non-key columns of the table it hashes, which cannot be VARCHAR
        return fullFatJoins && e.getMessage() != null && e.getMessage().contains("is of unsupported type");
    }

    private void assertFlatJoinReturnsRowsOfReference(boolean fullFatJoins) throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);
            createTables();
            final StringSink flatRows = new StringSink();
            final StringSink referenceRows = new StringSink();
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                compiler.setFullFatJoins(fullFatJoins);
                for (int i = 0; i < QUERY_COUNT; i++) {
                    final String[] queries = generateQueries(rnd);
                    final String flat = queries[0];
                    final String reference = queries[1];
                    try {
                        TestUtils.printSql(compiler, sqlExecutionContext, reference, referenceRows);
                    } catch (SqlException | CairoException e) {
                        // the reference itself is not supported, e.g. a key of two different types
                        continue;
                    }
                    try {
                        TestUtils.printSql(compiler, sqlExecutionContext, flat, flatRows);
                    } catch (SqlException | CairoException e) {
                        if (isFullFatValueTypeError(e, fullFatJoins)) {
                            continue;
                        }
                        throw new AssertionError("flat query failed, although its reference runs [flat=" + flat
                                + ", reference=" + reference + ", error=" + e.getMessage() + ']', e);
                    }
                    Assert.assertEquals("flat=" + flat + ", reference=" + reference, sortedRows(referenceRows), sortedRows(flatRows));
                }
            }
        });
    }

    // a key between two tables, on columns of kinds that SqlCodeGenerator can join on
    private static String key(Rnd rnd, String me, String other) {
        // the pairs of a SYMBOL or VARCHAR column with another, and of INT and LONG with their own type
        final String pair = switch (rnd.nextInt(8)) {
            case 0, 1 -> "ss";
            case 2, 3 -> "sv";
            case 4 -> "vs";
            case 5 -> "vv";
            case 6 -> "kk";
            default -> "ll";
        };
        return me + '.' + pair.charAt(0) + " = " + other + '.' + pair.charAt(1);
    }

    private static String[] generateQueries(Rnd rnd) {
        final List<String> aliases = new ArrayList<>();
        final StringBuilder joins = new StringBuilder();
        final StringBuilder conditions = new StringBuilder();
        final boolean isCommaJoin = rnd.nextInt(5) == 0;
        joins.append(table(rnd)).append(" a0");
        aliases.add("a0");
        joins.append(isCommaJoin ? ", " : " CROSS JOIN ").append(table(rnd)).append(" a1");
        aliases.add("a1");
        final int innerJoinCount = 1 + rnd.nextInt(3);
        for (int i = 2, n = 2 + innerJoinCount; i < n; i++) {
            final String me = "a" + i;
            final StringBuilder on = new StringBuilder();
            on.append(key(rnd, me, aliases.get(1 + rnd.nextInt(aliases.size() - 1))));
            // the last INNER join reads the FROM table, so the optimiser may move that key onto it
            if (i == n - 1 || rnd.nextInt(3) == 0) {
                on.append(" AND ").append(key(rnd, me, "a0"));
            }
            if (rnd.nextInt(5) == 0) {
                on.append(" AND ").append(key(rnd, me, aliases.get(rnd.nextInt(aliases.size()))));
            }
            if (isCommaJoin) {
                joins.append(", ").append(table(rnd)).append(' ').append(me);
                if (!conditions.isEmpty()) {
                    conditions.append(" AND ");
                }
                conditions.append(on);
            } else {
                joins.append(" JOIN ").append(table(rnd)).append(' ').append(me).append(" ON ").append(on);
            }
            aliases.add(me);
        }

        // the reference sorts the joins above by the timestamp of the FROM table, in a sub-query
        final StringBuilder subQueryColumns = new StringBuilder();
        for (int i = 0, n = aliases.size(); i < n; i++) {
            final String alias = aliases.get(i);
            for (int c = 0, m = KEY_COLUMNS.length(); c < m; c++) {
                final char column = KEY_COLUMNS.charAt(c);
                subQueryColumns.append(i == 0 && c == 0 ? "" : ", ")
                        .append(alias).append('.').append(column).append(' ').append(alias).append('_').append(column);
            }
            subQueryColumns.append(", ").append(alias).append(".ts ").append(alias).append("_ts");
        }
        final String where = conditions.isEmpty() ? "" : " WHERE " + conditions;
        final String subQuery = "(SELECT " + subQueryColumns + " FROM " + joins + where + " ORDER BY a0_ts) x";

        final String timeSeriesJoin = rnd.nextBoolean() ? " ASOF JOIN " : " LT JOIN ";
        final String timeSeriesTable = table(rnd);
        String on = "";
        String referenceOn = "";
        final int onShape = rnd.nextInt(3);
        if (onShape > 0) {
            final String column = onShape == 1 ? "s" : "k";
            final String alias = aliases.get(rnd.nextInt(aliases.size()));
            on = " ON (q." + column + " = " + alias + '.' + column + ')';
            referenceOn = " ON (q." + column + " = x." + alias + '_' + column + ')';
        }
        final String projected = "a0.k, a0.ts, " + aliases.get(aliases.size() - 1) + ".s, q.k, q.ts";
        final String referenceProjected = "x.a0_k, x.a0_ts, x." + aliases.get(aliases.size() - 1) + "_s, q.k, q.ts";
        final String flat = "SELECT " + projected + " FROM " + joins + timeSeriesJoin + timeSeriesTable + " q" + on + where;
        final String reference = "SELECT " + referenceProjected + " FROM " + subQuery + timeSeriesJoin + timeSeriesTable + " q" + referenceOn;
        return new String[]{flat, reference};
    }

    private static String sortedRows(StringSink sink) {
        final String[] lines = sink.toString().split("\n");
        // the header differs: the sub-query renames the columns
        final String[] rows = Arrays.copyOfRange(lines, Math.min(1, lines.length), lines.length);
        Arrays.sort(rows);
        return String.join("\n", rows);
    }

    // a table, or a sub-query over one whose v column SqlOptimiser cannot type
    private static String table(Rnd rnd) {
        final int t = rnd.nextInt(TABLE_COUNT);
        if (rnd.nextInt(3) == 0) {
            return "(SELECT s, concat(s, '')::varchar v, k, l, ts FROM t" + t + ")";
        }
        return "t" + t;
    }

    private void createTables() throws SqlException {
        for (int t = 0; t < TABLE_COUNT; t++) {
            execute("CREATE TABLE t" + t + " (s SYMBOL, v VARCHAR, k INT, l LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            // NULL keys, values that the tables share and rows that do not match
            execute("INSERT INTO t" + t + " SELECT"
                    + " CASE WHEN (x + " + t + ") % 5 = 0 THEN NULL ELSE 'v' || ((x + " + t + ") % 3) END,"
                    + " CASE WHEN (x * " + (t + 1) + ") % 7 = 0 THEN NULL ELSE 'v' || ((x * " + (t + 2) + ") % 3) END,"
                    + " CASE WHEN (x + " + t + ") % 4 = 0 THEN NULL ELSE ((x + " + t + ") % 3)::int END,"
                    + " ((x * " + (t + 1) + ") % 3)::long,"
                    + " timestamp_sequence(" + (t * 700_000) + ", 1_000_000) FROM long_sequence(" + (4 + t % 4) + ")");
        }
    }
}
