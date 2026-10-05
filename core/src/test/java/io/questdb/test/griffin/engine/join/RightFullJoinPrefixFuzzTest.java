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
import io.questdb.griffin.SqlException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

/**
 * Compares a RIGHT or FULL join with the same query written as nested sub-queries. SQL joins are
 * left-associative, so "t0 J1 t1 ... RIGHT JOIN tn" means "(...((t0 J1 t1) J2 t2)...) RIGHT JOIN tn":
 * the RIGHT/FULL join returns each unmatched row of tn once, with every column of the tables before
 * it NULL. The nested form evaluates the joins in the order written, and the optimiser cannot reorder
 * across a sub-query, so it is the reference for the rows of the flat form, which the optimiser
 * reorders. The queries use standard SQL only: every ON clause reads tables joined before it, and
 * every name resolves to one table. The data holds NULL keys and rows that do not match.
 */
public class RightFullJoinPrefixFuzzTest extends AbstractCairoTest {
    private static final Log LOG = LogFactory.getLog(RightFullJoinPrefixFuzzTest.class);
    private static final int QUERY_COUNT = 300;
    private static final int TABLE_COUNT = 10;

    @Test
    public void testFlatJoinReturnsRowsOfNestedJoin() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);
            createTables();
            final StringSink flatRows = new StringSink();
            final StringSink nestedRows = new StringSink();
            for (int i = 0; i < QUERY_COUNT; i++) {
                final String[] queries = generateQueries(rnd);
                final String flat = queries[0];
                final String nested = queries[1];
                try {
                    nestedRows.clear();
                    printSql(nested, nestedRows);
                } catch (SqlException | CairoException e) {
                    // the reference itself is not supported, e.g. a time-series join after an
                    // outer join, which drops the designated timestamp
                    continue;
                }
                try {
                    flatRows.clear();
                    printSql(flat, flatRows);
                } catch (SqlException | CairoException e) {
                    throw new AssertionError("flat query failed, although its nested form runs [flat=" + flat
                            + ", nested=" + nested + ", error=" + e.getMessage() + ']', e);
                }
                Assert.assertEquals("flat=" + flat + ", nested=" + nested, sortedRows(nestedRows), sortedRows(flatRows));
            }
        });
    }

    private static void appendCondition(Rnd rnd, StringBuilder sql, int[] tables, int joined, int left) {
        final char column = rnd.nextInt(10) < 7 ? 'c' : 'v';
        final int shape = rnd.nextInt(100);
        if (shape < 75) {
            sql.append(column).append(tables[left]).append(" = ").append(column).append(tables[joined]);
        } else if (shape < 90) {
            sql.append(column).append(tables[left]).append(" < ").append(column).append(tables[joined]);
        } else {
            sql.append(column).append(tables[joined]).append(" > 0");
        }
    }

    // returns {flat, nested}: the same joins, flat and as sub-queries nested in the order written
    private static String[] generateQueries(Rnd rnd) {
        final int modelCount = 3 + rnd.nextInt(6);
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
        final StringBuilder flat = new StringBuilder("SELECT * FROM t").append(tables[0]);
        String nested = "SELECT * FROM t" + tables[0];
        for (int joined = 1; joined < modelCount; joined++) {
            final boolean isLast = joined == modelCount - 1;
            final String joinType;
            if (isLast) {
                joinType = rnd.nextInt(3) == 0 ? "FULL JOIN" : "RIGHT JOIN";
            } else {
                final int r = rnd.nextInt(100);
                if (r < 40) {
                    joinType = "JOIN";
                } else if (r < 58) {
                    joinType = "LEFT JOIN";
                } else if (r < 70) {
                    joinType = "CROSS JOIN";
                } else if (r < 80) {
                    joinType = "RIGHT JOIN";
                } else if (r < 88) {
                    joinType = "FULL JOIN";
                } else if (r < 95) {
                    joinType = "ASOF JOIN";
                } else {
                    joinType = "LT JOIN";
                }
            }
            final StringBuilder join = new StringBuilder().append(' ').append(joinType).append(" t").append(tables[joined]);
            if (!joinType.equals("CROSS JOIN") && !joinType.equals("ASOF JOIN") && !joinType.equals("LT JOIN")) {
                join.append(" ON ");
                appendCondition(rnd, join, tables, joined, rnd.nextInt(joined));
                if (rnd.nextInt(3) == 0) {
                    join.append(" AND ");
                    appendCondition(rnd, join, tables, joined, rnd.nextInt(joined));
                }
            }
            flat.append(join);
            nested = "SELECT * FROM (" + nested + ") n" + joined + join;
        }
        if (rnd.nextInt(10) < 3) {
            final int table = tables[rnd.nextInt(modelCount)];
            final String where = switch (rnd.nextInt(4)) {
                case 0 -> " WHERE c" + table + " > 0";
                case 1 -> " WHERE c" + table + " IS NULL";
                case 2 -> " WHERE c" + table + " IS NOT NULL";
                default -> " WHERE c" + table + " = c" + tables[rnd.nextInt(modelCount)];
            };
            flat.append(where);
            nested = "SELECT * FROM (" + nested + ") n" + where;
        }
        return new String[]{flat.toString(), nested};
    }

    private static String sortedRows(StringSink sink) {
        final String[] lines = sink.toString().split("\n");
        // the header differs: a sub-query renames the columns that several tables share
        final String[] rows = Arrays.copyOfRange(lines, Math.min(1, lines.length), lines.length);
        Arrays.sort(rows);
        return String.join("\n", rows);
    }

    private void createTables() throws SqlException {
        for (int t = 0; t < TABLE_COUNT; t++) {
            execute("CREATE TABLE t" + t + " (c" + t + " INT, k INT, v" + t + " DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            // NULL join keys, and a row count that leaves rows unmatched
            execute("INSERT INTO t" + t + " SELECT CASE WHEN (x + " + t + ") % 4 = 0 THEN NULL ELSE (x + " + t + ") % 3 END, x % 3,"
                    + " CASE WHEN (x + " + t + ") % 5 = 0 THEN NULL ELSE ((x * " + (t + 1) + ") % 4) * 1.5 END,"
                    + " timestamp_sequence(" + (t * 1000) + ", 1000000) FROM long_sequence(" + (3 + t % 4) + ")");
        }
    }
}
