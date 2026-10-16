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

import io.questdb.griffin.SqlException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Chars;
import io.questdb.std.Numbers;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.regex.Pattern;

/**
 * Compares a query that ends with a RIGHT or FULL join with the same query written with sub-queries.
 * SQL joins are left-associative, so "t0 J1 t1 ... RIGHT JOIN tn" means "(t0 J1 t1 ...) RIGHT JOIN tn":
 * the RIGHT/FULL join returns each unmatched row of tn once, with every column of the tables before
 * it NULL. The optimiser cannot reorder across a sub-query, so a sub-query form is the reference for
 * the rows of the flat form, which the optimiser reorders. The nested form wraps every join, so it
 * evaluates the joins in the order written; it cannot express a time-series join after an outer join,
 * which loses the designated timestamp. The prefix form wraps only the tables before the last join,
 * so the optimiser plans the prefix as written, time-series joins included. The queries use standard
 * SQL: every ON clause reads tables joined before it, and every name resolves to one table. The data
 * holds NULL keys and rows that do not match. A reference may fail only where it cannot express its
 * query, see isUnsupportedReference(); any other failure of either query fails the test.
 * The implied-key queries tie, in the ON clause of an INNER join, the joined table to the table of the
 * join before it and to an earlier table, as in "JOIN t2 ON c0 = c2 AND c1 = c2", which implies
 * c0 = c1 for the rows that the INNER join keeps, and filter on one or both tables of the implied key.
 * SqlOptimiser hashes the outer join of t1 on such a key only when later conjuncts drop the rows that
 * the outer join NULL-extends, see SqlOptimiser.isNullExtensionRejected().
 * The WHERE-heavy queries always filter, with one or two conjuncts on a table, constants and INT bind
 * variables, and sometimes LATEST ON on the first table, which the references put in a sub-query of that
 * table. A conjunct on a table before the last join filters after it, and SqlOptimiser may also filter the
 * scan of the table with copies of the conjuncts, see SqlOptimiser.addScanFilterCopyCandidate(). The
 * LIMIT-fenced reference runs the flat joins without WHERE in a sub-query that LIMIT keeps whole, and
 * filters its rows, so it takes no such copy.
 */
public class RightFullJoinPrefixFuzzTest extends AbstractCairoTest {
    // a FULL join whose slave a later INNER join ties to an earlier table, with WHERE on the slave only
    private static final Pattern FULL_JOIN_SLAVE_ONLY_REJECTED = Pattern.compile(
            "FULL JOIN t(\\d) ON [^J]*\\d JOIN t(\\d) ON c\\d = c\\2 AND c\\1 = c\\2 [^W]*WHERE c\\1 > 0$"
    );
    // queries that the generator coverage test draws from a fixed seed
    private static final int IMPLIED_KEY_CORPUS_SIZE = 1_000;
    private static final int IMPLIED_KEY_QUERY_COUNT = 300;
    // index of the LIMIT-fenced reference query in the array that generateQueries() returns
    private static final int LIMIT_FENCED = 3;
    private static final Log LOG = LogFactory.getLog(RightFullJoinPrefixFuzzTest.class);
    // index of the reference query in the array that generateQueries() returns
    private static final int NESTED = 1;
    private static final int NESTED_QUERY_COUNT = 300;
    private static final int PREFIX_SUB_QUERY = 2;
    // the reordering that the prefix form checks needs four joins of particular types to line up
    private static final int PREFIX_SUB_QUERY_COUNT = 1_000;
    private static final int TABLE_COUNT = 10;
    // values of the INT bind variables of the WHERE-heavy queries
    private static final int[] WHERE_BIND_VALUES = {Numbers.INT_NULL, 0, 1, 2};
    // queries that the WHERE shape coverage test draws from a fixed seed
    private static final int WHERE_CORPUS_SIZE = 1_000;
    // a WHERE with two conjuncts
    private static final Pattern WHERE_CONJUNCT_PAIR = Pattern.compile(" WHERE .* AND ");
    private static final int WHERE_QUERY_COUNT = 300;
    // a WHERE predicate on a SYMBOL column
    private static final Pattern WHERE_SYMBOL_PREDICATE = Pattern.compile(" s\\d (LIKE|~|IN|NOT IN) ");

    @Test
    public void testFlatJoinKeyedThroughLaterInnerJoinReturnsRowsOfNestedJoin() throws Exception {
        assertFlatJoinReturnsRowsOf(NESTED, IMPLIED_KEY_QUERY_COUNT, true, false);
    }

    @Test
    public void testFlatJoinReturnsRowsOfNestedJoin() throws Exception {
        assertFlatJoinReturnsRowsOf(NESTED, NESTED_QUERY_COUNT, false, false);
    }

    @Test
    public void testFlatJoinReturnsRowsOfPrefixSubQuery() throws Exception {
        assertFlatJoinReturnsRowsOf(PREFIX_SUB_QUERY, PREFIX_SUB_QUERY_COUNT, false, false);
    }

    @Test
    public void testFlatJoinWhereReturnsRowsOfLimitFencedQuery() throws Exception {
        assertFlatJoinReturnsRowsOf(LIMIT_FENCED, WHERE_QUERY_COUNT, false, true);
    }

    @Test
    public void testFlatJoinWhereReturnsRowsOfNestedJoin() throws Exception {
        assertFlatJoinReturnsRowsOf(NESTED, WHERE_QUERY_COUNT, false, true);
    }

    @Test
    public void testImpliedKeyQueriesRejectOnlySlaveOfFullJoin() {
        // Without its check of the master side, SqlOptimiser keyed such a FULL join and returned the rows
        // that it NULL-extends on the master side, which the INNER join keeps through NULL = NULL.
        final Rnd rnd = new Rnd(42, 4242);
        int matchCount = 0;
        for (int i = 0; i < IMPLIED_KEY_CORPUS_SIZE; i++) {
            if (FULL_JOIN_SLAVE_ONLY_REJECTED.matcher(generateQueries(rnd, true, true, false)[0]).find()) {
                matchCount++;
            }
        }
        Assert.assertTrue("only " + matchCount + " of " + IMPLIED_KEY_CORPUS_SIZE + " queries", matchCount * 25 >= IMPLIED_KEY_CORPUS_SIZE);
    }

    @Test
    public void testWhereQueriesCoverScanFilterShapes() {
        // The shapes where a copy of a WHERE conjunct could change the rows: LATEST ON on the table, symbol
        // predicates that the NULL probe parses, bind variables whose value may be NULL, and several conjuncts
        // on one table, which a NULL-rejecting conjunct takes along.
        final Rnd rnd = new Rnd(42, 4242);
        int latestOnCount = 0;
        int symbolCount = 0;
        int bindCount = 0;
        int conjunctPairCount = 0;
        for (int i = 0; i < WHERE_CORPUS_SIZE; i++) {
            final String flat = generateQueries(rnd, false, false, true)[0];
            if (flat.contains(" LATEST ON ts PARTITION BY k")) {
                latestOnCount++;
            }
            if (WHERE_SYMBOL_PREDICATE.matcher(flat).find()) {
                symbolCount++;
            }
            if (flat.contains("$1")) {
                bindCount++;
            }
            if (WHERE_CONJUNCT_PAIR.matcher(flat).find()) {
                conjunctPairCount++;
            }
        }
        final String counts = "latestOn=" + latestOnCount + ", symbol=" + symbolCount + ", bind=" + bindCount
                + ", conjunctPair=" + conjunctPairCount + " of " + WHERE_CORPUS_SIZE;
        Assert.assertTrue(counts, latestOnCount * 10 >= WHERE_CORPUS_SIZE);
        Assert.assertTrue(counts, symbolCount * 10 >= WHERE_CORPUS_SIZE);
        Assert.assertTrue(counts, bindCount * 10 >= WHERE_CORPUS_SIZE);
        Assert.assertTrue(counts, conjunctPairCount * 10 >= WHERE_CORPUS_SIZE);
    }

    private static void appendCondition(Rnd rnd, StringBuilder sql, int[] tables, int joined, int left, boolean isOuterJoinHeavy) {
        final char column = rnd.nextInt(10) < 7 ? 'c' : 'v';
        final int shape = rnd.nextInt(100);
        if (shape < (isOuterJoinHeavy ? 45 : 75)) {
            sql.append(column).append(tables[left]).append(" = ").append(column).append(tables[joined]);
        } else if (shape < (isOuterJoinHeavy ? 75 : 90)) {
            sql.append(column).append(tables[left]).append(" < ").append(column).append(tables[joined]);
        } else {
            sql.append(column).append(tables[joined]).append(" > 0");
        }
    }

    // Appends a WHERE predicate on the table, of a kind that rnd picks: comparisons of the INT column with
    // constants and INT bind variables, IS [NOT] NULL, a function over it, predicates on the SYMBOL column,
    // and an equality with the INT column of another table of the query.
    private static void appendWherePredicate(Rnd rnd, StringBuilder where, int table, int[] tables, int modelCount) {
        final String c = "c" + table;
        final String s = "s" + table;
        switch (rnd.nextInt(16)) {
            case 0 -> where.append(c).append(" > 0");
            case 1 -> where.append(c).append(" IS NULL");
            case 2 -> where.append(c).append(" IS NOT NULL");
            case 3 -> where.append(c).append(" = c").append(tables[rnd.nextInt(modelCount)]);
            case 4 -> where.append(c).append(" IN (0, 1)");
            case 5 -> where.append(c).append(" BETWEEN 0 AND 1");
            case 6 -> where.append(c).append(" != 1");
            case 7 -> where.append("coalesce(").append(c).append(", 0) = 0");
            case 8 -> where.append(s).append(" LIKE 'a%'");
            case 9 -> where.append(s).append(" ~ 'a'");
            case 10 -> where.append(s).append(" IN ('a', 'b')");
            case 11 -> where.append(s).append(" NOT IN ('a')");
            case 12 -> where.append(c).append(" > $1");
            case 13 -> where.append(c).append(" >= $1");
            case 14 -> where.append(c).append(" >= $1 AND ").append(c).append(" < $2");
            default -> where.append(c).append(" = $1");
        }
    }

    // Returns {flat, nested, prefix sub-query, LIMIT-fenced}: the same joins flat, as sub-queries nested in
    // the order written, with the tables before the last join in a sub-query, and flat without WHERE in a
    // sub-query that LIMIT keeps whole, with the WHERE outside. With isOuterJoinHeavy, the
    // queries favour outer, non-equi and time-series joins, which the optimiser reorders around the
    // RIGHT/FULL joins. With isImpliedKeyHeavy, the queries write only INNER and outer joins, every INNER
    // join after the second table ties the table of the join before it to an earlier table, and WHERE
    // filters on one or both tables of the last such tie. With isWhereHeavy, every query filters with one
    // or two predicates on a table, and a quarter of them put LATEST ON on the first table: the flat form
    // as a trailing LATEST ON, which applies to the first table, and the nested and prefix forms as a
    // sub-query of that table.
    private static String[] generateQueries(Rnd rnd, boolean isOuterJoinHeavy, boolean isImpliedKeyHeavy, boolean isWhereHeavy) {
        final int modelCount = isOuterJoinHeavy ? 4 + rnd.nextInt(5) : 3 + rnd.nextInt(6);
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
        final boolean isLatestOn = isWhereHeavy && rnd.nextInt(4) == 0;
        final String firstTable = isLatestOn
                ? "(SELECT * FROM t" + tables[0] + " LATEST ON ts PARTITION BY k) t" + tables[0]
                : "t" + tables[0];
        final StringBuilder flat = new StringBuilder("SELECT * FROM t").append(tables[0]);
        // the joins of the flat form after its first table
        final StringBuilder joins = new StringBuilder();
        String nested = "SELECT * FROM " + firstTable;
        String prefixSubQuery = null;
        int impliedKeyMaster = -1;
        int impliedKeySlave = -1;
        for (int joined = 1; joined < modelCount; joined++) {
            final boolean isLast = joined == modelCount - 1;
            final String joinType;
            if (isLast) {
                joinType = rnd.nextInt(3) == 0 ? "FULL JOIN" : "RIGHT JOIN";
            } else if (isImpliedKeyHeavy) {
                joinType = switch (rnd.nextInt(4)) {
                    case 0 -> "JOIN";
                    case 1 -> "LEFT JOIN";
                    case 2 -> "RIGHT JOIN";
                    default -> "FULL JOIN";
                };
            } else {
                final int r = rnd.nextInt(100);
                if (r < (isOuterJoinHeavy ? 15 : 40)) {
                    joinType = "JOIN";
                } else if (r < (isOuterJoinHeavy ? 40 : 58)) {
                    joinType = "LEFT JOIN";
                } else if (r < (isOuterJoinHeavy ? 45 : 70)) {
                    joinType = "CROSS JOIN";
                } else if (r < (isOuterJoinHeavy ? 60 : 80)) {
                    joinType = "RIGHT JOIN";
                } else if (r < (isOuterJoinHeavy ? 70 : 88)) {
                    joinType = "FULL JOIN";
                } else if (r < (isOuterJoinHeavy ? 88 : 95)) {
                    joinType = "ASOF JOIN";
                } else {
                    joinType = "LT JOIN";
                }
            }
            final StringBuilder join = new StringBuilder().append(' ').append(joinType).append(" t").append(tables[joined]);
            if (!joinType.equals("CROSS JOIN") && !joinType.equals("ASOF JOIN") && !joinType.equals("LT JOIN")) {
                join.append(" ON ");
                if (isImpliedKeyHeavy && joinType.equals("JOIN") && joined > 1) {
                    impliedKeyMaster = tables[rnd.nextInt(joined - 1)];
                    impliedKeySlave = tables[joined - 1];
                    join.append('c').append(impliedKeyMaster).append(" = c").append(tables[joined])
                            .append(" AND c").append(impliedKeySlave).append(" = c").append(tables[joined]);
                } else {
                    appendCondition(rnd, join, tables, joined, rnd.nextInt(joined), isOuterJoinHeavy);
                    if (rnd.nextInt(3) == 0) {
                        join.append(" AND ");
                        appendCondition(rnd, join, tables, joined, rnd.nextInt(joined), isOuterJoinHeavy);
                    }
                }
            }
            if (isLast) {
                prefixSubQuery = "SELECT * FROM (SELECT * FROM " + firstTable + joins + ") p" + join;
            }
            flat.append(join);
            joins.append(join);
            nested = "SELECT * FROM (" + nested + ") n" + joined + join;
        }
        final String latestOn = isLatestOn ? " LATEST ON ts PARTITION BY k" : "";
        String limitFenced = "SELECT * FROM (" + flat + latestOn + " LIMIT 1_000_000_000) f";
        String where = null;
        if (impliedKeySlave != -1) {
            where = switch (rnd.nextInt(3)) {
                case 0 -> " WHERE c" + impliedKeySlave + " > 0";
                case 1 -> " WHERE c" + impliedKeyMaster + " > 0";
                default -> " WHERE c" + impliedKeyMaster + " > 0 AND c" + impliedKeySlave + " > 0";
            };
        } else if (isWhereHeavy) {
            final int table = tables[rnd.nextInt(modelCount)];
            final StringBuilder predicates = new StringBuilder(" WHERE ");
            appendWherePredicate(rnd, predicates, table, tables, modelCount);
            if (rnd.nextInt(3) == 0) {
                predicates.append(" AND ");
                appendWherePredicate(rnd, predicates, table, tables, modelCount);
            }
            where = predicates.toString();
        } else if (rnd.nextInt(10) < 3) {
            final int table = tables[rnd.nextInt(modelCount)];
            where = switch (rnd.nextInt(4)) {
                case 0 -> " WHERE c" + table + " > 0";
                case 1 -> " WHERE c" + table + " IS NULL";
                case 2 -> " WHERE c" + table + " IS NOT NULL";
                default -> " WHERE c" + table + " = c" + tables[rnd.nextInt(modelCount)];
            };
        }
        if (where != null) {
            flat.append(where);
            nested = "SELECT * FROM (" + nested + ") n" + where;
            prefixSubQuery += where;
            limitFenced += where;
        }
        flat.append(latestOn);
        return new String[]{flat.toString(), nested, prefixSubQuery, limitFenced};
    }

    // an ASOF or LT join written after a RIGHT or FULL join, so that its left side has no designated
    // timestamp
    private static boolean hasOuterJoinBeforeTimeSeriesJoin(String sql) {
        final int timeSeriesJoin = Math.max(sql.lastIndexOf(" ASOF JOIN "), sql.lastIndexOf(" LT JOIN "));
        final int rightJoin = sql.indexOf(" RIGHT JOIN ");
        final int fullJoin = sql.indexOf(" FULL JOIN ");
        return (rightJoin != -1 && rightJoin < timeSeriesJoin) || (fullJoin != -1 && fullJoin < timeSeriesJoin);
    }

    // A reference fails only where it cannot express its query. A time-series join after a RIGHT or
    // FULL join has no designated timestamp on its left side. The prefix form also runs into #7700:
    // SqlOptimiser may run a table that no join key ties to the others after an outer join whose ON
    // clause reads it, and the valid query fails with "Invalid column". The nested form joins two
    // models per level, so each ON clause reads only the models that its own join combines.
    private static boolean isUnsupportedReference(String reference, int referenceIndex, SqlException e) {
        final CharSequence message = e.getFlyweightMessage();
        if (Chars.equals(message, "TIMESTAMP column is required but not provided")
                || Chars.equals(message, "left side of time series join has no timestamp")) {
            return hasOuterJoinBeforeTimeSeriesJoin(reference);
        }
        return referenceIndex == PREFIX_SUB_QUERY && Chars.startsWith(message, "Invalid column: ");
    }

    private static String sortedRows(StringSink sink) {
        final String[] lines = sink.toString().split("\n");
        // the header differs: a sub-query renames the columns that several tables share
        final String[] rows = Arrays.copyOfRange(lines, Math.min(1, lines.length), lines.length);
        Arrays.sort(rows);
        return String.join("\n", rows);
    }

    private void assertFlatJoinReturnsRowsOf(int referenceIndex, int queryCount, boolean isImpliedKeyHeavy, boolean isWhereHeavy) throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);
            createTables();
            final StringSink flatRows = new StringSink();
            final StringSink referenceRows = new StringSink();
            int checkedCount = 0;
            int runCount = 0;
            for (int i = 0; i < queryCount; i++) {
                final String[] queries = generateQueries(rnd, referenceIndex == PREFIX_SUB_QUERY || isImpliedKeyHeavy, isImpliedKeyHeavy, isWhereHeavy);
                final String flat = queries[0];
                final String reference = queries[referenceIndex];
                // a query with bind variables runs with two sets of values
                final boolean hasBindVariables = flat.contains("$1");
                for (int run = 0, runs = hasBindVariables ? 2 : 1; run < runs; run++) {
                    runCount++;
                    String binds = "";
                    if (hasBindVariables) {
                        final int first = WHERE_BIND_VALUES[rnd.nextInt(WHERE_BIND_VALUES.length)];
                        final int second = WHERE_BIND_VALUES[rnd.nextInt(WHERE_BIND_VALUES.length)];
                        bindVariableService.clear();
                        bindVariableService.setInt(0, first);
                        if (flat.contains("$2")) {
                            bindVariableService.setInt(1, second);
                        }
                        binds = ", $1=" + first + ", $2=" + second;
                    }
                    try {
                        referenceRows.clear();
                        printSql(reference, referenceRows);
                    } catch (Throwable e) {
                        if (e instanceof SqlException sqlException && isUnsupportedReference(reference, referenceIndex, sqlException)) {
                            continue;
                        }
                        throw new AssertionError("reference failed [reference=" + reference + ", flat=" + flat + binds + ", error=" + e + ']', e);
                    }
                    try {
                        flatRows.clear();
                        printSql(flat, flatRows);
                    } catch (Throwable e) {
                        throw new AssertionError("flat query failed, although its reference runs [flat=" + flat
                                + ", reference=" + reference + binds + ", error=" + e + ']', e);
                    }
                    Assert.assertEquals("flat=" + flat + ", reference=" + reference + binds, sortedRows(referenceRows), sortedRows(flatRows));
                    checkedCount++;
                }
            }
            // Over 80% of the references run. A change that made most of them fail with an error
            // that isUnsupportedReference() accepts would check few rows and pass.
            Assert.assertTrue("only " + checkedCount + " of " + runCount + " queries were checked", checkedCount * 2 >= runCount);
        });
    }

    private void createTables() throws SqlException {
        for (int t = 0; t < TABLE_COUNT; t++) {
            // the SYMBOL column is indexed in the even tables
            execute("CREATE TABLE t" + t + " (c" + t + " INT, k INT, v" + t + " DOUBLE, s" + t + " SYMBOL" + (t % 2 == 0 ? " INDEX" : "")
                    + ", ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            // NULL join keys, and a row count that leaves rows unmatched
            execute("INSERT INTO t" + t + " SELECT CASE WHEN (x + " + t + ") % 4 = 0 THEN NULL ELSE (x + " + t + ") % 3 END, x % 3,"
                    + " CASE WHEN (x + " + t + ") % 5 = 0 THEN NULL ELSE ((x * " + (t + 1) + ") % 4) * 1.5 END,"
                    + " CASE WHEN (x + " + t + ") % 4 = 1 THEN NULL WHEN (x * " + (t + 1) + ") % 3 = 0 THEN 'a' WHEN (x * " + (t + 1) + ") % 3 = 1 THEN 'b' ELSE 'c' END,"
                    + " timestamp_sequence(" + (t * 1000) + ", 1000000) FROM long_sequence(" + (3 + t % 4) + ")");
        }
    }
}
