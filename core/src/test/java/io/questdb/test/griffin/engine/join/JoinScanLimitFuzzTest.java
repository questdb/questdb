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

import io.questdb.PropertyKey;
import io.questdb.cairo.SqlJitMode;
import io.questdb.griffin.SqlException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Chars;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.griffin.fuzz.FuzzConfig;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Compares a LIMIT over joins with the rows of the same query without the LIMIT. The queries join two to
 * four tables with INNER, LEFT, CROSS, ASOF and LT joins, and always filter the first table, whose filter a
 * LIMIT over the joins reaches as advice; they may also filter a joined table, or two tables at once, which
 * filters after the joins. LIMIT n returns the first n rows of the query, LIMIT -n the last n rows, and
 * LIMIT lo, hi the rows from lo to hi, in the order of the query. A filter of the first table that stopped at
 * the LIMIT returned too few rows wherever a join or a filter after it dropped rows, see JoinScanLimitTest.
 * The tables hold NULL keys, rows that match nothing, and up to eight rows each, so the queries return fewer
 * rows than a LIMIT asks for as well as more. A quarter of the LIMITs take their bounds from bind variables.
 * The test skips a query that fails without the LIMIT for a known reason, see isKnownFailure(), and fails on
 * any other error. RightFullJoinPrefixFuzzTest covers the RIGHT and FULL joins.
 */
public class JoinScanLimitFuzzTest extends AbstractCairoTest {
    // queries that the coverage test draws from a fixed seed
    private static final int CORPUS_SIZE = 1_000;
    private static final Pattern LIMIT_FIRST_ROWS = Pattern.compile(" LIMIT \\d+$");
    private static final Pattern LIMIT_LAST_ROWS = Pattern.compile(" LIMIT -\\d+$");
    private static final Pattern LIMIT_RANGE = Pattern.compile(" LIMIT \\d+, \\d+$");
    private static final Log LOG = LogFactory.getLog(JoinScanLimitFuzzTest.class);
    // -Dquestdb.fuzz.queries=N runs N queries per test
    private static final int QUERY_COUNT = Integer.getInteger(FuzzConfig.QUERIES_PROP, 300);
    private static final int TABLE_COUNT = 6;

    @Test
    public void testLimitReturnsRowsOfQuery() throws Exception {
        assertLimitsReturnRowsOfQuery();
    }

    @Test
    public void testLimitReturnsRowsOfQueryOverOneRowPageFrames() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS, 1);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 1);
        assertLimitsReturnRowsOfQuery();
    }

    @Test
    public void testQueriesCoverJoinAndLimitShapes() {
        // The shapes that the LIMIT advice depends on: INNER joins, which drop rows, LEFT and time-series
        // joins, filters after the joins, levels whose filter of the first table takes the LIMIT, and the
        // three LIMIT forms.
        final Rnd rnd = new Rnd(42, 4242);
        int innerCount = 0;
        int leftCount = 0;
        int timeSeriesCount = 0;
        int filterAfterJoinCount = 0;
        int scanLimitCount = 0;
        int firstRowsCount = 0;
        int lastRowsCount = 0;
        int rangeCount = 0;
        for (int i = 0; i < CORPUS_SIZE; i++) {
            final Query query = generateQuery(rnd);
            final String limit = generateLimit(rnd);
            innerCount += query.hasInnerJoin ? 1 : 0;
            leftCount += query.hasLeftJoin ? 1 : 0;
            timeSeriesCount += query.hasTimeSeriesJoin ? 1 : 0;
            filterAfterJoinCount += query.hasFilterAfterJoin ? 1 : 0;
            scanLimitCount += !query.hasInnerJoin && !query.hasFilterAfterJoin ? 1 : 0;
            if (LIMIT_FIRST_ROWS.matcher(limit).find()) {
                firstRowsCount++;
            } else if (LIMIT_LAST_ROWS.matcher(limit).find()) {
                lastRowsCount++;
            } else if (LIMIT_RANGE.matcher(limit).find()) {
                rangeCount++;
            }
        }
        final String counts = "inner=" + innerCount + ", left=" + leftCount + ", timeSeries=" + timeSeriesCount
                + ", filterAfterJoin=" + filterAfterJoinCount + ", scanLimit=" + scanLimitCount
                + ", firstRows=" + firstRowsCount + ", lastRows=" + lastRowsCount + ", range=" + rangeCount
                + " of " + CORPUS_SIZE;
        Assert.assertEquals(counts, CORPUS_SIZE, firstRowsCount + lastRowsCount + rangeCount);
        Assert.assertTrue(counts, innerCount * 4 >= CORPUS_SIZE);
        Assert.assertTrue(counts, leftCount * 4 >= CORPUS_SIZE);
        Assert.assertTrue(counts, timeSeriesCount * 4 >= CORPUS_SIZE);
        Assert.assertTrue(counts, filterAfterJoinCount * 10 >= CORPUS_SIZE);
        Assert.assertTrue(counts, scanLimitCount * 10 >= CORPUS_SIZE);
        Assert.assertTrue(counts, firstRowsCount * 5 >= CORPUS_SIZE);
        Assert.assertTrue(counts, lastRowsCount * 5 >= CORPUS_SIZE);
        Assert.assertTrue(counts, rangeCount * 5 >= CORPUS_SIZE);
    }

    // Appends an ON condition of the table at index joined over a table joined before it: mostly an equality
    // of the INT columns, sometimes a comparison, or an OR, which makes the join a nested loop.
    private static void appendJoinCondition(Rnd rnd, StringBuilder sql, int[] tables, int joined) {
        final int left = tables[rnd.nextInt(joined)];
        final int right = tables[joined];
        switch (rnd.nextInt(8)) {
            case 0 -> sql.append('c').append(left).append(" < c").append(right);
            case 1 -> sql.append('c').append(left).append(" = c").append(right).append(" OR c").append(left).append(" = c").append(right).append(" + 2");
            case 2 -> sql.append('c').append(left).append(" = c").append(right).append(" AND v").append(right).append(" > 1");
            default -> sql.append('c').append(left).append(" = c").append(right);
        }
    }

    // Appends a predicate on one table: comparisons of the INT and DOUBLE columns, IS [NOT] NULL, and
    // predicates on the SYMBOL column, which is indexed in the even tables.
    private static void appendTablePredicate(Rnd rnd, StringBuilder sql, int table) {
        switch (rnd.nextInt(8)) {
            case 0 -> sql.append('c').append(table).append(" > 0");
            case 1 -> sql.append('c').append(table).append(" IS NOT NULL");
            case 2 -> sql.append('v').append(table).append(" > 1");
            case 3 -> sql.append('s').append(table).append(" = 'a'");
            case 4 -> sql.append('s').append(table).append(" IN ('a', 'b')");
            case 5 -> sql.append('s').append(table).append(" LIKE 'a%'");
            case 6 -> sql.append('c').append(table).append(" != 1");
            default -> sql.append('v').append(table).append(" IS NULL");
        }
    }

    // Sets the bounds of a LIMIT of generateLimit() as bind variables, and returns the LIMIT that reads them.
    private static String bindLimit(String limit) throws SqlException {
        final String bounds = limit.substring(" LIMIT ".length());
        final int comma = bounds.indexOf(',');
        bindVariableService.clear();
        if (comma == -1) {
            bindVariableService.setLong(0, Long.parseLong(bounds));
            return " LIMIT $1";
        }
        bindVariableService.setLong(0, Long.parseLong(bounds.substring(0, comma)));
        bindVariableService.setLong(1, Long.parseLong(bounds.substring(comma + 2)));
        return " LIMIT $1, $2";
    }

    // Returns LIMIT n, LIMIT -n or LIMIT lo, hi, with n from 1 to 6 and 0 <= lo < hi <= 6.
    private static String generateLimit(Rnd rnd) {
        return switch (rnd.nextInt(3)) {
            case 0 -> " LIMIT " + (1 + rnd.nextInt(6));
            case 1 -> " LIMIT -" + (1 + rnd.nextInt(6));
            default -> {
                final int lo = rnd.nextInt(6);
                yield " LIMIT " + lo + ", " + (lo + 1 + rnd.nextInt(6 - lo));
            }
        };
    }

    // Returns a query that joins two to four of the tables, in an order that rnd picks, and filters the first
    // table. A third of the queries filter a joined table as well, which filters after a LEFT, ASOF or LT
    // join, and a sixth filter on two tables at once, which filters after the joins.
    private static Query generateQuery(Rnd rnd) {
        final int modelCount = 2 + rnd.nextInt(3);
        final int[] tables = new int[TABLE_COUNT];
        for (int i = 0; i < TABLE_COUNT; i++) {
            tables[i] = i;
        }
        for (int i = TABLE_COUNT - 1; i > 0; i--) {
            final int j = rnd.nextInt(i + 1);
            final int tmp = tables[i];
            tables[i] = tables[j];
            tables[j] = tmp;
        }
        final StringBuilder sql = new StringBuilder("SELECT * FROM t").append(tables[0]);
        // whether the join of each table keeps every row it reads: LEFT, ASOF, LT
        final boolean[] isOuterJoined = new boolean[modelCount];
        boolean hasInnerJoin = false;
        boolean hasLeftJoin = false;
        boolean hasTimeSeriesJoin = false;
        for (int joined = 1; joined < modelCount; joined++) {
            final int r = rnd.nextInt(100);
            if (r < 30) {
                sql.append(" JOIN t").append(tables[joined]).append(" ON ");
                appendJoinCondition(rnd, sql, tables, joined);
                hasInnerJoin = true;
            } else if (r < 55) {
                sql.append(" LEFT JOIN t").append(tables[joined]).append(" ON ");
                appendJoinCondition(rnd, sql, tables, joined);
                isOuterJoined[joined] = true;
                hasLeftJoin = true;
            } else if (r < 70) {
                sql.append(" CROSS JOIN t").append(tables[joined]);
            } else {
                sql.append(r < 85 ? " ASOF JOIN t" : " LT JOIN t").append(tables[joined]);
                if (rnd.nextBoolean()) {
                    sql.append(" ON t").append(tables[0]).append(".k = t").append(tables[joined]).append(".k");
                }
                isOuterJoined[joined] = true;
                hasTimeSeriesJoin = true;
            }
        }
        sql.append(" WHERE ");
        appendTablePredicate(rnd, sql, tables[0]);
        boolean hasFilterAfterJoin = false;
        final int r = rnd.nextInt(6);
        if (r < 2) {
            final int joined = 1 + rnd.nextInt(modelCount - 1);
            sql.append(" AND ");
            appendTablePredicate(rnd, sql, tables[joined]);
            hasFilterAfterJoin = isOuterJoined[joined];
        } else if (r == 2) {
            final int left = rnd.nextInt(modelCount);
            final int right = rnd.nextInt(modelCount);
            sql.append(" AND c").append(tables[left]).append(" = c").append(tables[right]).append(" + 1");
            // a conjunct on two tables filters after the joins, and one on a single table after its join
            hasFilterAfterJoin = left != right || isOuterJoined[left];
        }
        return new Query(sql.toString(), hasInnerJoin, hasLeftJoin, hasTimeSeriesJoin, hasFilterAfterJoin);
    }

    // #7700: a LEFT join whose ON clause reads a table that a CROSS join, or an INNER join that runs as one,
    // joined before it may fail with "Invalid column", with or without a LIMIT.
    private static boolean isKnownFailure(String sql, SqlException e) {
        return Chars.startsWith(e.getFlyweightMessage(), "Invalid column: ") && sql.contains(" LEFT JOIN ");
    }

    // the rows that a LIMIT of generateLimit() keeps of the rows of the query, in their order
    private static List<String> limitedRows(String limit, List<String> rows) {
        final String bounds = limit.substring(" LIMIT ".length());
        final int comma = bounds.indexOf(',');
        final int size = rows.size();
        if (comma == -1) {
            final int n = Integer.parseInt(bounds);
            return n >= 0 ? rows.subList(0, Math.min(n, size)) : rows.subList(Math.max(0, size + n), size);
        }
        final int lo = Integer.parseInt(bounds.substring(0, comma));
        final int hi = Integer.parseInt(bounds.substring(comma + 2));
        return rows.subList(Math.min(lo, size), Math.min(hi, size));
    }

    private static List<String> rowList(StringSink sink) {
        final String[] lines = sink.toString().split("\n");
        // the first line is the header
        return new ArrayList<>(Arrays.asList(lines).subList(Math.min(1, lines.length), lines.length));
    }

    private void assertLimitsReturnRowsOfQuery() throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(() -> {
            createTables();
            final StringSink rows = new StringSink();
            final StringSink limitRows = new StringSink();
            final int callerJitMode = sqlExecutionContext.getJitMode();
            final boolean callerParallel = sqlExecutionContext.isParallelFilterEnabled();
            int checkedCount = 0;
            try {
                for (int i = 0; i < QUERY_COUNT; i++) {
                    final String sql = generateQuery(rnd).sql;
                    final String limit = generateLimit(rnd);
                    final boolean isJit = rnd.nextBoolean();
                    // the parallel filter is the only filter that takes the LIMIT, so most queries use it
                    final boolean isParallel = rnd.nextInt(4) > 0;
                    final boolean isBound = rnd.nextInt(4) == 0;
                    sqlExecutionContext.setJitMode(isJit ? SqlJitMode.JIT_MODE_ENABLED : SqlJitMode.JIT_MODE_DISABLED);
                    sqlExecutionContext.setParallelFilterEnabled(isParallel);
                    final String message = "sql=" + sql + limit + ", jit=" + isJit + ", parallel=" + isParallel + ", bound=" + isBound;
                    try {
                        rows.clear();
                        printSql(sql, rows);
                    } catch (SqlException e) {
                        if (isKnownFailure(sql, e)) {
                            continue;
                        }
                        throw new AssertionError("query failed [" + message + ", error=" + e + ']', e);
                    }
                    try {
                        limitRows.clear();
                        printSql(sql + (isBound ? bindLimit(limit) : limit), limitRows);
                    } catch (Throwable e) {
                        throw new AssertionError("query with LIMIT failed, although the query runs [" + message + ", error=" + e + ']', e);
                    }
                    Assert.assertEquals(
                            message,
                            String.join("\n", limitedRows(limit, rowList(rows))),
                            String.join("\n", rowList(limitRows))
                    );
                    checkedCount++;
                }
            } finally {
                sqlExecutionContext.setJitMode(callerJitMode);
                sqlExecutionContext.setParallelFilterEnabled(callerParallel);
            }
            // almost every query runs; a change that made them fail with an error that the loop skips would
            // check few LIMIT queries and pass
            Assert.assertTrue("only " + checkedCount + " of " + QUERY_COUNT + " queries were checked", checkedCount * 10 >= QUERY_COUNT * 9);
        });
    }

    private void createTables() throws SqlException {
        for (int t = 0; t < TABLE_COUNT; t++) {
            // the SYMBOL column is indexed in the even tables
            execute("CREATE TABLE t" + t + " (c" + t + " INT, k INT, v" + t + " DOUBLE, s" + t + " SYMBOL" + (t % 2 == 0 ? " INDEX" : "")
                    + ", ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            // NULL join keys, keys that match nothing, and rows over two days
            execute("INSERT INTO t" + t + " SELECT CASE WHEN (x + " + t + ") % 4 = 0 THEN NULL ELSE (x * " + (t + 2) + ") % 5 END, x % 3,"
                    + " CASE WHEN (x + " + t + ") % 5 = 0 THEN NULL ELSE ((x * " + (t + 1) + ") % 4) * 1.5 END,"
                    + " CASE WHEN (x + " + t + ") % 4 = 1 THEN NULL WHEN (x * " + (t + 1) + ") % 3 = 0 THEN 'a' WHEN (x * " + (t + 1) + ") % 3 = 1 THEN 'b' ELSE 'c' END,"
                    + " timestamp_sequence('2024-01-01T20:00:00.000000Z'::TIMESTAMP + " + (t * 1_000_000) + ", 3_600_000_000)"
                    + " FROM long_sequence(" + (3 + (t * 2) % 6) + ")");
        }
    }

    // a query of generateQuery() and its shape
    private record Query(String sql, boolean hasInnerJoin, boolean hasLeftJoin, boolean hasTimeSeriesJoin, boolean hasFilterAfterJoin) {
    }
}
