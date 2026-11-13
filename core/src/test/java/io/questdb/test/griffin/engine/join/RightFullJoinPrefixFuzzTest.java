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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
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
 * An ON clause may also hold a conjunct cX = cY + cZ or cX = cY - cZ over earlier tables, mostly in an INNER
 * join after a RIGHT or FULL join that NULL-extends those tables. SqlOptimiser must keep such a conjunct at its
 * INNER join, after the outer join, see SqlOptimiser.addModelOnPredicateFromCollectedIndexes().
 * The implied-key queries tie, in the ON clause of an INNER join, the joined table to the table of the
 * join before it and to an earlier table, as in "JOIN t2 ON c0 = c2 AND c1 = c2", which implies
 * c0 = c1 for the rows that the INNER join keeps, and filter on one or both tables of the implied key.
 * SqlOptimiser hashes the outer join of t1 on such a key only when later conjuncts drop the rows that
 * the outer join NULL-extends, see SqlOptimiser.isNullExtensionRejected().
 * The WHERE-heavy queries always filter, with one or two conjuncts on a table, constants and INT bind
 * variables, and sometimes LATEST ON on the first table, which the references put in a sub-query of that
 * table. A conjunct on a table before the last join filters after it, and SqlOptimiser may also filter the
 * scan of the table with copies of the conjuncts, see SqlOptimiser.addScanFilterCopyCandidate(), and, when a
 * later join matches on the table, the rows of an earlier RIGHT or FULL join, see
 * SqlOptimiser.addNullExtensionFilters(). The LIMIT-fenced reference runs the flat joins without WHERE in a
 * sub-query that LIMIT keeps whole, and filters its rows, so it takes no such copy. The LIMIT queries end the WHERE-heavy flat form with a LIMIT,
 * which may return any rows of the query but no fewer than it asks for, so a filter of a table before the
 * RIGHT or FULL join must not stop at the LIMIT.
 */
public class RightFullJoinPrefixFuzzTest extends AbstractCairoTest {
    // a join of the flat form: its type and the number of its table
    private static final Pattern FLAT_JOIN = Pattern.compile(" (JOIN|LEFT JOIN|CROSS JOIN|RIGHT JOIN|FULL JOIN|ASOF JOIN|LT JOIN) t(\\d)");
    // a FULL join whose slave a later INNER join ties to an earlier table, with WHERE on the slave only
    private static final Pattern FULL_JOIN_SLAVE_ONLY_REJECTED = Pattern.compile(
            "FULL JOIN t(\\d) ON [^J]*\\d JOIN t(\\d) ON c\\d = c\\2 AND c\\1 = c\\2 [^W]*WHERE c\\1 > 0$"
    );
    // queries that the generator coverage test draws from a fixed seed
    private static final int IMPLIED_KEY_CORPUS_SIZE = 1_000;
    private static final int IMPLIED_KEY_QUERY_COUNT = 300;
    // LIMIT shapes that the LIMIT coverage test draws from a fixed seed
    private static final int LIMIT_CORPUS_SIZE = 1_000;
    // index of the LIMIT-fenced reference query in the array that generateQueries() returns
    private static final int LIMIT_FENCED = 3;
    // LIMIT n, LIMIT -n and LIMIT lo, hi, see appendLimit()
    private static final Pattern LIMIT_FIRST_ROWS = Pattern.compile(" LIMIT \\d+$");
    private static final Pattern LIMIT_LAST_ROWS = Pattern.compile(" LIMIT -\\d+$");
    private static final int LIMIT_QUERY_COUNT = 300;
    private static final Pattern LIMIT_RANGE = Pattern.compile(" LIMIT \\d+, \\d+$");
    private static final Log LOG = LogFactory.getLog(RightFullJoinPrefixFuzzTest.class);
    // queries that the multi-table conjunct coverage test draws from a fixed seed
    private static final int MULTI_TABLE_CORPUS_SIZE = 1_000;
    // cY + cZ = cX or cY - cZ = cX, see appendMultiTableCondition()
    private static final Pattern MULTI_TABLE_SUM_ON_LEFT = Pattern.compile("c(\\d) [+-] c(\\d) = c(\\d)");
    // cX = cY + cZ or cX = cY - cZ, see appendMultiTableCondition()
    private static final Pattern MULTI_TABLE_SUM_ON_RIGHT = Pattern.compile("c(\\d) = c(\\d) [+-] c(\\d)");
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
    // a WHERE whose first predicate reads the first table, see appendWherePredicate()
    private static final Pattern WHERE_ON_FIRST_TABLE = Pattern.compile("^SELECT \\* FROM t(\\d) .* WHERE (coalesce\\()?[cs]\\1[ ,]");
    // the table that the first predicate of a WHERE reads, see appendWherePredicate()
    private static final Pattern WHERE_TABLE = Pattern.compile(" WHERE (coalesce\\()?[cs](\\d)[ ,]");
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
    public void testFlatJoinWhereWithLimitReturnsRowsOfFlatJoinWhere() throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        // The scan ahead of a RIGHT or FULL join under a LIMIT n or a LIMIT lo, hi either starts with
        // one-row page frames and grows them, or reads each partition of the small tables as one frame.
        final int rampStartRows = rnd.nextInt(2);
        LOG.info().$("page frame ramp-up [startRows=").$(rampStartRows).I$();
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_RAMP_START_ROWS, rampStartRows);
        assertMemoryLeak(() -> {
            createTables();
            final StringSink rows = new StringSink();
            final StringSink limitRows = new StringSink();
            int checkedCount = 0;
            for (int i = 0; i < LIMIT_QUERY_COUNT; i++) {
                final String flat = generateQueries(rnd, false, false, true, rnd.nextBoolean())[0];
                final String limit = generateLimit(rnd);
                String binds = "";
                if (flat.contains("$1")) {
                    final int first = WHERE_BIND_VALUES[rnd.nextInt(WHERE_BIND_VALUES.length)];
                    final int second = WHERE_BIND_VALUES[rnd.nextInt(WHERE_BIND_VALUES.length)];
                    bindVariableService.clear();
                    bindVariableService.setInt(0, first);
                    if (flat.contains("$2")) {
                        bindVariableService.setInt(1, second);
                    }
                    binds = ", $1=" + first + ", $2=" + second;
                }
                final String message = "flat=" + flat + limit + binds;
                try {
                    rows.clear();
                    printSql(flat, rows);
                } catch (Throwable e) {
                    // the flat form can write a time-series join after an outer join, see isUnsupportedReference()
                    if (e instanceof SqlException sqlException && isUnsupportedReference(flat, NESTED, sqlException)) {
                        continue;
                    }
                    throw new AssertionError("query failed [" + message + ", error=" + e + ']', e);
                }
                try {
                    limitRows.clear();
                    printSql(flat + limit, limitRows);
                } catch (Throwable e) {
                    throw new AssertionError("query with LIMIT failed, although the query runs [" + message + ", error=" + e + ']', e);
                }
                final List<String> all = rowList(rows);
                final List<String> limited = rowList(limitRows);
                Assert.assertEquals(message, limitedRowCount(limit, all.size()), limited.size());
                // any rows of the query, each no more often than the query returns it
                for (int j = 0, n = limited.size(); j < n; j++) {
                    Assert.assertTrue(message + ", row=" + limited.get(j), all.remove(limited.get(j)));
                }
                checkedCount++;
            }
            // most flat forms run; a change that made them fail with an error that isUnsupportedReference()
            // accepts would check few LIMIT queries and pass
            Assert.assertTrue("only " + checkedCount + " of " + LIMIT_QUERY_COUNT + " queries were checked", checkedCount * 2 >= LIMIT_QUERY_COUNT);
        });
    }

    @Test
    public void testImpliedKeyQueriesRejectOnlySlaveOfFullJoin() {
        // Without its check of the master side, SqlOptimiser keyed such a FULL join and returned the rows
        // that it NULL-extends on the master side, which the INNER join keeps through NULL = NULL.
        final Rnd rnd = new Rnd(42, 4242);
        int matchCount = 0;
        for (int i = 0; i < IMPLIED_KEY_CORPUS_SIZE; i++) {
            if (FULL_JOIN_SLAVE_ONLY_REJECTED.matcher(generateQueries(rnd, true, true, false, false)[0]).find()) {
                matchCount++;
            }
        }
        Assert.assertTrue("only " + matchCount + " of " + IMPLIED_KEY_CORPUS_SIZE + " queries", matchCount * 25 >= IMPLIED_KEY_CORPUS_SIZE);
    }

    @Test
    public void testLimitQueriesCoverLimitShapes() {
        // LIMIT n and LIMIT lo, hi read the first rows of the joins, LIMIT -n reads the last rows, and half
        // of the queries filter the first table, whose filter the LIMIT reaches as advice
        final Rnd rnd = new Rnd(42, 4242);
        int firstRowsCount = 0;
        int lastRowsCount = 0;
        int rangeCount = 0;
        int firstTableWhereCount = 0;
        for (int i = 0; i < LIMIT_CORPUS_SIZE; i++) {
            final String flat = generateQueries(rnd, false, false, true, rnd.nextBoolean())[0];
            final String limit = generateLimit(rnd);
            if (LIMIT_FIRST_ROWS.matcher(limit).find()) {
                firstRowsCount++;
            } else if (LIMIT_LAST_ROWS.matcher(limit).find()) {
                lastRowsCount++;
            } else if (LIMIT_RANGE.matcher(limit).find()) {
                rangeCount++;
            }
            if (WHERE_ON_FIRST_TABLE.matcher(flat).find()) {
                firstTableWhereCount++;
            }
        }
        final String counts = "firstRows=" + firstRowsCount + ", lastRows=" + lastRowsCount + ", range=" + rangeCount
                + ", firstTableWhere=" + firstTableWhereCount + " of " + LIMIT_CORPUS_SIZE;
        Assert.assertEquals(counts, LIMIT_CORPUS_SIZE, firstRowsCount + lastRowsCount + rangeCount);
        Assert.assertTrue(counts, firstRowsCount * 5 >= LIMIT_CORPUS_SIZE);
        Assert.assertTrue(counts, lastRowsCount * 5 >= LIMIT_CORPUS_SIZE);
        Assert.assertTrue(counts, rangeCount * 5 >= LIMIT_CORPUS_SIZE);
        Assert.assertTrue(counts, firstTableWhereCount * 5 >= LIMIT_CORPUS_SIZE * 2);
    }

    @Test
    public void testQueriesCoverMultiTableConjunctsAfterOuterJoin() {
        // Without the multi-table branches of SqlOptimiser.addModelOnPredicateFromCollectedIndexes(), such a
        // conjunct of an INNER join filtered below the RIGHT or FULL join, which then NULL-extended the rows
        // that the conjunct dropped.
        final Rnd rnd = new Rnd(42, 4242);
        int sumOnLeftCount = 0;
        int sumOnRightCount = 0;
        for (int i = 0; i < MULTI_TABLE_CORPUS_SIZE; i++) {
            final String flat = generateQueries(rnd, false, false, false, false)[0];
            if (hasMultiTableConjunctAfterOuterJoin(flat, MULTI_TABLE_SUM_ON_LEFT)) {
                sumOnLeftCount++;
            }
            if (hasMultiTableConjunctAfterOuterJoin(flat, MULTI_TABLE_SUM_ON_RIGHT)) {
                sumOnRightCount++;
            }
        }
        final String counts = "sumOnLeft=" + sumOnLeftCount + ", sumOnRight=" + sumOnRightCount + " of " + MULTI_TABLE_CORPUS_SIZE;
        Assert.assertTrue(counts, sumOnLeftCount * 25 >= MULTI_TABLE_CORPUS_SIZE);
        Assert.assertTrue(counts, sumOnRightCount * 25 >= MULTI_TABLE_CORPUS_SIZE);
    }

    @Test
    public void testWhereQueriesCoverScanFilterShapes() {
        // The shapes where a copy of a WHERE conjunct could change the rows: LATEST ON on the table, symbol
        // predicates that the NULL probe parses, bind variables whose value may be NULL, several conjuncts
        // on one table, which a NULL-rejecting conjunct takes along, and a join that matches on the table
        // after a RIGHT or FULL join, after which a copy also filters. A twentieth of the queries suffices
        // for the last: it needs a RIGHT or FULL join, a later ON clause and a WHERE that line up.
        final Rnd rnd = new Rnd(42, 4242);
        int latestOnCount = 0;
        int symbolCount = 0;
        int bindCount = 0;
        int conjunctPairCount = 0;
        int nullExtensionMatchCount = 0;
        for (int i = 0; i < WHERE_CORPUS_SIZE; i++) {
            final String flat = generateQueries(rnd, false, false, true, false)[0];
            if (hasJoinOnWhereTableAfterOuterJoin(flat)) {
                nullExtensionMatchCount++;
            }
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
                + ", conjunctPair=" + conjunctPairCount + ", nullExtensionMatch=" + nullExtensionMatchCount
                + " of " + WHERE_CORPUS_SIZE;
        Assert.assertTrue(counts, latestOnCount * 10 >= WHERE_CORPUS_SIZE);
        Assert.assertTrue(counts, symbolCount * 10 >= WHERE_CORPUS_SIZE);
        Assert.assertTrue(counts, bindCount * 10 >= WHERE_CORPUS_SIZE);
        Assert.assertTrue(counts, conjunctPairCount * 10 >= WHERE_CORPUS_SIZE);
        Assert.assertTrue(counts, nullExtensionMatchCount * 20 >= WHERE_CORPUS_SIZE);
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

    // Appends cX = cY + cZ or cX = cY - cZ, with the sum on either side of =, over the tables before the
    // one at index joined, Y and Z distinct. X may be Y or Z, so the conjunct reads two or three tables.
    private static void appendMultiTableCondition(Rnd rnd, StringBuilder sql, int[] tables, int joined) {
        final int x = tables[rnd.nextInt(joined)];
        final int yIndex = rnd.nextInt(joined);
        int zIndex = rnd.nextInt(joined - 1);
        if (zIndex >= yIndex) {
            zIndex++;
        }
        final String sum = "c" + tables[yIndex] + (rnd.nextBoolean() ? " + c" : " - c") + tables[zIndex];
        if (rnd.nextBoolean()) {
            sql.append('c').append(x).append(" = ").append(sum);
        } else {
            sql.append(sum).append(" = c").append(x);
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

    // Returns LIMIT n, LIMIT -n or LIMIT lo, hi, with n from 1 to 4 and 0 <= lo < hi <= 4. The tables hold up to
    // six rows each, so the queries return fewer rows than the LIMIT asks for as well as more.
    private static String generateLimit(Rnd rnd) {
        return switch (rnd.nextInt(3)) {
            case 0 -> " LIMIT " + (1 + rnd.nextInt(4));
            case 1 -> " LIMIT -" + (1 + rnd.nextInt(4));
            default -> {
                final int lo = rnd.nextInt(4);
                yield " LIMIT " + lo + ", " + (lo + 1 + rnd.nextInt(4 - lo));
            }
        };
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
    // sub-query of that table. With isFirstTableWhere, the WHERE of a WHERE-heavy query filters the first
    // table, whose filter a LIMIT over the joins reaches as advice.
    private static String[] generateQueries(
            Rnd rnd,
            boolean isOuterJoinHeavy,
            boolean isImpliedKeyHeavy,
            boolean isWhereHeavy,
            boolean isFirstTableWhere
    ) {
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
        // index of the last RIGHT or FULL join so far, which NULL-extends the tables before it
        int outerJoined = -1;
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
                    // an INNER join after a RIGHT or FULL join mostly gets a conjunct over the tables that the
                    // outer join NULL-extends, which must filter after the outer join; any join after the second
                    // table may get one over all earlier tables
                    if (joinType.equals("JOIN") && outerJoined >= 2 && rnd.nextInt(3) > 0) {
                        join.append(" AND ");
                        appendMultiTableCondition(rnd, join, tables, outerJoined);
                    } else if (joined >= 2 && rnd.nextInt(3) == 0) {
                        join.append(" AND ");
                        appendMultiTableCondition(rnd, join, tables, joined);
                    }
                }
            }
            if (joinType.equals("RIGHT JOIN") || joinType.equals("FULL JOIN")) {
                outerJoined = joined;
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
            final int table = isFirstTableWhere ? tables[0] : tables[rnd.nextInt(modelCount)];
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

    // True when the WHERE of the flat query filters the first table, or a table of an INNER or CROSS join, with a
    // predicate that is false on the NULL row of the table, a RIGHT or FULL join NULL-extends the table, and the
    // ON clause of a later join reads the table. Copies of the predicate filter the scan of the table, and the
    // later join matches the rows that the RIGHT or FULL join NULL-extends on NULL, so a copy also filters
    // after that join, see SqlOptimiser.addNullExtensionFilters().
    private static boolean hasJoinOnWhereTableAfterOuterJoin(String flat) {
        final Matcher whereMatcher = WHERE_TABLE.matcher(flat);
        if (!whereMatcher.find()) {
            return false;
        }
        final String table = whereMatcher.group(2);
        // the predicates of appendWherePredicate() that are false on NULL for every value of $1 and $2
        final Pattern nullRejectingPredicate = Pattern.compile("[cs]" + table + " (> 0|IS NOT NULL|IN \\(|BETWEEN|LIKE|~|> \\$1|< \\$2)");
        if (!nullRejectingPredicate.matcher(flat).region(whereMatcher.start(), flat.length()).find()) {
            return false;
        }
        final Pattern tableColumn = Pattern.compile("[cv]" + table + "(?!\\d)");
        final List<String> joinTypes = new ArrayList<>();
        final List<String> joinedTables = new ArrayList<>();
        final List<String> onClauses = new ArrayList<>();
        final Matcher joinMatcher = FLAT_JOIN.matcher(flat).region(0, whereMatcher.start());
        int onStart = -1;
        while (joinMatcher.find()) {
            if (onStart != -1) {
                onClauses.add(flat.substring(onStart, joinMatcher.start()));
            }
            joinTypes.add(joinMatcher.group(1));
            joinedTables.add(joinMatcher.group(2));
            onStart = joinMatcher.end();
        }
        if (onStart != -1) {
            onClauses.add(flat.substring(onStart, whereMatcher.start()));
        }
        boolean isTableJoined = flat.startsWith("SELECT * FROM t" + table + ' ');
        // a RIGHT or FULL join after the table runs before the join that the loop reads
        boolean isNullExtended = false;
        for (int i = 0, n = joinTypes.size(); i < n; i++) {
            if (isNullExtended && tableColumn.matcher(onClauses.get(i)).find()) {
                return true;
            }
            final String joinType = joinTypes.get(i);
            if (joinedTables.get(i).equals(table)) {
                if (!joinType.equals("JOIN") && !joinType.equals("CROSS JOIN")) {
                    // the WHERE filters the right side of an outer or time-series join after it
                    return false;
                }
                isTableJoined = true;
            } else {
                isNullExtended |= isTableJoined && (joinType.equals("RIGHT JOIN") || joinType.equals("FULL JOIN"));
            }
        }
        return false;
    }

    // True when an INNER join of the flat query has a conjunct that matches the pattern, and a RIGHT or FULL
    // join sits after every table that the conjunct reads and before the INNER join, which NULL-extends
    // those tables, so the conjunct must filter after it.
    private static boolean hasMultiTableConjunctAfterOuterJoin(String flat, Pattern conjunct) {
        // position in the join order of each table of the query, the first table at 0
        final int[] tablePositions = new int[TABLE_COUNT];
        final List<String> joinTypes = new ArrayList<>();
        final List<String> onClauses = new ArrayList<>();
        tablePositions[flat.charAt("SELECT * FROM t".length()) - '0'] = 0;
        joinTypes.add("FROM");
        onClauses.add("");
        final int whereIndex = flat.indexOf(" WHERE ");
        final int end = whereIndex == -1 ? flat.length() : whereIndex;
        final Matcher joinMatcher = FLAT_JOIN.matcher(flat).region(0, end);
        int onStart = -1;
        while (joinMatcher.find()) {
            if (onStart != -1) {
                onClauses.add(flat.substring(onStart, joinMatcher.start()));
            }
            tablePositions[joinMatcher.group(2).charAt(0) - '0'] = joinTypes.size();
            joinTypes.add(joinMatcher.group(1));
            onStart = joinMatcher.end();
        }
        if (onStart != -1) {
            onClauses.add(flat.substring(onStart, end));
        }
        for (int join = 2, n = joinTypes.size(); join < n; join++) {
            if (!joinTypes.get(join).equals("JOIN")) {
                continue;
            }
            final Matcher conjunctMatcher = conjunct.matcher(onClauses.get(join));
            if (!conjunctMatcher.find()) {
                continue;
            }
            int lastRead = 0;
            for (int group = 1; group <= 3; group++) {
                lastRead = Math.max(lastRead, tablePositions[conjunctMatcher.group(group).charAt(0) - '0']);
            }
            for (int outerJoin = lastRead + 1; outerJoin < join; outerJoin++) {
                final String joinType = joinTypes.get(outerJoin);
                if (joinType.equals("RIGHT JOIN") || joinType.equals("FULL JOIN")) {
                    return true;
                }
            }
        }
        return false;
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

    // the number of rows that a LIMIT of generateLimit() keeps of rowCount rows
    private static int limitedRowCount(String limit, int rowCount) {
        final String bounds = limit.substring(" LIMIT ".length());
        final int comma = bounds.indexOf(',');
        if (comma == -1) {
            return Math.min(Math.abs(Integer.parseInt(bounds)), rowCount);
        }
        final int lo = Integer.parseInt(bounds.substring(0, comma));
        final int hi = Integer.parseInt(bounds.substring(comma + 2));
        return Math.max(0, Math.min(hi, rowCount) - lo);
    }

    private static List<String> rowList(StringSink sink) {
        final String[] lines = sink.toString().split("\n");
        // the first line is the header
        return new ArrayList<>(Arrays.asList(lines).subList(Math.min(1, lines.length), lines.length));
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
                final String[] queries = generateQueries(rnd, referenceIndex == PREFIX_SUB_QUERY || isImpliedKeyHeavy, isImpliedKeyHeavy, isWhereHeavy, false);
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
