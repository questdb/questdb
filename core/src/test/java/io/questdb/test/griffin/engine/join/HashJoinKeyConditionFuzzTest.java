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

import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Chars;
import io.questdb.std.Numbers;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.griffin.fuzz.FuzzConfig;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.regex.Pattern;

/**
 * Fuzzes the conditions after INNER hash joins that SqlCodeGenerator moves into the joins, see
 * {@link HashJoinKeyConditionTest}. Each query joins two or three tables on one or two key pairs of random
 * columns, of one type, or of two types whose equal values the join matches: SYMBOL, STRING and VARCHAR, or
 * TIMESTAMP and TIMESTAMP_NS. Its WHERE holds random conjuncts over the joined table, the key column of the
 * table it joins, and columns of the earlier tables that are no key, with constants, an INT bind variable,
 * BETWEEN, CASE, OR, functions, extract() and a scalar sub-query. The tables have random sizes, so a light
 * hash join hashes either side, and hold NULL keys and keys that one side lacks.
 * <p>
 * Full-fat hash joins, which copy their slave and keep every condition in the filter after the join, give the
 * reference rows. The light hash joins must return them, again after toTop(), and calculateSize() must count
 * them. The test counts the queries whose plan moves a condition into a join and those whose plan keeps one
 * after a join, so that a generator that stopped making either fails.
 */
public class HashJoinKeyConditionFuzzTest extends AbstractCairoTest {
    // values of the INT bind variable
    private static final int[] BIND_VALUES = {Numbers.INT_NULL, 0, 2};
    private static final int DEFAULT_QUERY_COUNT = 300;
    private static final String[] KEY_COLUMNS = {"i", "l", "sy", "st", "vc", "ts", "tn", "d"};
    private static final Log LOG = LogFactory.getLog(HashJoinKeyConditionFuzzTest.class);
    // pairs of key columns of two types whose equal values the join matches
    private static final String[][] MIXED_KEY_PAIRS = {{"sy", "st"}, {"sy", "vc"}, {"st", "vc"}, {"ts", "tn"}};
    // a light hash join that evaluates a condition, see HashJoinLightRecordCursorFactory.toPlan()
    private static final Pattern MOVED_CONDITION = Pattern.compile("Hash Join Light\n\\s+condition: [^\n]*\n(\\s+symbolKeyJoin: true\n)?\\s+filter: ");
    private static final int QUERIES_PER_TABLE_SET = 20;
    private static final int TABLE_COUNT = 3;

    @Test
    public void testLightJoinsReturnRowsOfFullFatJoins() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);
            final int queryCount = Integer.getInteger(FuzzConfig.QUERIES_PROP, DEFAULT_QUERY_COUNT);
            final StringSink sink = new StringSink();
            final StringSink secondPass = new StringSink();
            // the statements that made the tables of the current query, which a failure reports
            String tables = null;
            int movedCount = 0;
            int keptCount = 0;
            try (SqlCompiler fullFatCompiler = engine.getSqlCompiler()) {
                fullFatCompiler.setFullFatJoins(true);
                for (int q = 0; q < queryCount; q++) {
                    if (q % QUERIES_PER_TABLE_SET == 0) {
                        tables = createTables(rnd);
                    }
                    final String sql = generateQuery(rnd);
                    final boolean hasBindVariable = sql.contains("$1");
                    if (hasBindVariable) {
                        bindVariableService.clear();
                        bindVariableService.setInt(0, BIND_VALUES[rnd.nextInt(BIND_VALUES.length)]);
                    }
                    printSql("EXPLAIN " + sql, sink);
                    if (MOVED_CONDITION.matcher(sink).find()) {
                        movedCount++;
                    }
                    if (Chars.contains(sink, "Filter filter: ")) {
                        keptCount++;
                    }
                    // a query with a bind variable runs with two of its values
                    for (int run = 0, runs = hasBindVariable ? 2 : 1; run < runs; run++) {
                        String context = ", tables=" + tables;
                        if (hasBindVariable) {
                            final int value = BIND_VALUES[rnd.nextInt(BIND_VALUES.length)];
                            bindVariableService.setInt(0, value);
                            context = ", $1=" + (value == Numbers.INT_NULL ? "NULL" : value) + context;
                        }
                        final String expected;
                        try (RecordCursorFactory factory = fullFatCompiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
                             RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            CursorPrinter.println(cursor, factory.getMetadata(), sink);
                            expected = sortedRows(sink);
                        } catch (Throwable e) {
                            throw new AssertionError("full-fat join failed [sql=" + sql + context + ", error=" + e + ']', e);
                        }
                        try (RecordCursorFactory factory = select(sql); RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            CursorPrinter.println(cursor, factory.getMetadata(), sink);
                            Assert.assertEquals("sql=" + sql + context, expected, sortedRows(sink));
                            cursor.toTop();
                            CursorPrinter.println(cursor, factory.getMetadata(), secondPass);
                            TestUtils.assertEquals("second pass, sql=" + sql + context, sink, secondPass);
                            cursor.toTop();
                            final RecordCursor.Counter counter = new RecordCursor.Counter();
                            cursor.calculateSize(sqlExecutionContext.getCircuitBreaker(), counter);
                            Assert.assertEquals("calculateSize(), sql=" + sql + context, rowCount(sink), counter.get());
                        } catch (SqlException e) {
                            throw new AssertionError("light join failed [sql=" + sql + context + ", error=" + e + ']', e);
                        }
                    }
                }
            }
            final String counts = "moved=" + movedCount + ", kept=" + keptCount + " of " + queryCount;
            LOG.info().$(counts).$();
            Assert.assertTrue(counts, movedCount * 4 >= queryCount);
            Assert.assertTrue(counts, keptCount * 4 >= queryCount);
        });
    }

    // Appends a conjunct over the slave's value column v, the master key column, and the v column of an earlier
    // table that is no key, of a kind that rnd picks for the type of the master key column
    private static void appendConjunct(Rnd rnd, StringBuilder where, int slave, String masterKey, String masterKeyColumn, int other) {
        final String v = "t" + slave + ".v";
        final String otherV = "t" + other + ".v";
        switch (masterKeyColumn) {
            case "i", "l" -> {
                switch (rnd.nextInt(10)) {
                    case 0 -> where.append(v).append(" > ").append(masterKey);
                    case 1 -> where.append(v).append(" + ").append(masterKey).append(" < 6");
                    case 2 ->
                            where.append(v).append(" BETWEEN ").append(masterKey).append(" AND ").append(masterKey).append(" + 3");
                    case 3 ->
                            where.append("CASE WHEN ").append(v).append(" > ").append(masterKey).append(" THEN ").append(v).append(" < 7 ELSE false END");
                    case 4 -> where.append("coalesce(").append(v).append(", 0) != ").append(masterKey);
                    case 5 -> where.append(v).append(" > ").append(masterKey).append(" + $1");
                    case 6 ->
                            where.append('(').append(v).append(" > ").append(masterKey).append(" OR ").append(v).append(" IS NULL)");
                    case 7 -> where.append(v).append(" + ").append(masterKey).append(" > (SELECT count() FROM t0)");
                    case 8 -> where.append(v).append(" > ").append(otherV);
                    default -> where.append(v).append(" + ").append(otherV).append(" > ").append(masterKey);
                }
            }
            case "sy", "st", "vc" -> {
                switch (rnd.nextInt(7)) {
                    case 0 -> where.append("length(").append(masterKey).append(") + ").append(v).append(" > 3");
                    case 1 -> where.append('(').append(masterKey).append(" = 'a' OR ").append(v).append(" > 5)");
                    case 2 -> where.append(v).append(" > length(").append(masterKey).append(") * 4");
                    case 3 -> where.append('(').append(masterKey).append(" IN ('a', '') OR ").append(v).append(" < 3)");
                    case 4 ->
                            where.append("CASE WHEN ").append(masterKey).append(" = 'b' THEN ").append(v).append(" > 2 ELSE ").append(v).append(" < 6 END");
                    case 5 -> where.append(v).append(" > ").append(otherV);
                    default ->
                            where.append("length(").append(masterKey).append(") + ").append(otherV).append(" < ").append(v);
                }
            }
            case "ts", "tn" -> {
                switch (rnd.nextInt(5)) {
                    case 0 -> where.append("second(").append(masterKey).append(") + ").append(v).append(" > 4");
                    // v is microseconds on a TIMESTAMP and nanoseconds on a TIMESTAMP_NS
                    case 1 ->
                            where.append(masterKey).append(" + ").append(v).append(" > '2024-01-01T00:00:02.000003Z'");
                    case 2 -> where.append("extract(second from ").append(masterKey).append(") < ").append(v);
                    case 3 ->
                            where.append('(').append(masterKey).append(" > '2024-01-01T00:00:01Z' OR ").append(v).append(" > 6)");
                    default -> where.append(v).append(" > ").append(otherV);
                }
            }
            default -> {
                if (rnd.nextBoolean()) {
                    where.append(v).append(" > ").append(masterKey);
                } else {
                    where.append(masterKey).append(" + ").append(v).append(" > 3");
                }
            }
        }
    }

    // Drops and creates the tables, each with 0 to 12 rows of random keys and values, and returns the statements
    // that create them
    private static String createTables(Rnd rnd) throws SqlException {
        final StringBuilder statements = new StringBuilder();
        for (int t = 0; t < TABLE_COUNT; t++) {
            execute("DROP TABLE IF EXISTS t" + t);
            final String create = "CREATE TABLE t" + t + " (i INT, l LONG, sy SYMBOL, st STRING, vc VARCHAR, ts TIMESTAMP, tn TIMESTAMP_NS, d DOUBLE, v INT)";
            execute(create);
            statements.append(create).append(";\n");
            final int rowCount = rnd.nextInt(13);
            if (rowCount == 0) {
                continue;
            }
            final StringBuilder insert = new StringBuilder("INSERT INTO t").append(t).append(" VALUES ");
            for (int r = 0; r < rowCount; r++) {
                if (r > 0) {
                    insert.append(", ");
                }
                insert.append('(');
                for (String column : KEY_COLUMNS) {
                    insert.append(keyValue(rnd, column)).append(", ");
                }
                insert.append(rnd.nextInt(6) == 0 ? "NULL" : String.valueOf(rnd.nextInt(10))).append(')');
            }
            execute(insert);
            statements.append(insert).append(";\n");
        }
        return statements.toString();
    }

    // A query that joins two or three tables, each table after the first on one or two key pairs to an earlier
    // table, with one or two conjuncts per join
    private static String generateQuery(Rnd rnd) {
        final int tableCount = 2 + rnd.nextInt(TABLE_COUNT - 1);
        final StringBuilder sql = new StringBuilder("SELECT * FROM t0");
        final StringBuilder where = new StringBuilder();
        for (int slave = 1; slave < tableCount; slave++) {
            final int master = rnd.nextInt(slave);
            final String[] key = pickKeyPair(rnd);
            final String masterKey = "t" + master + "." + key[1];
            sql.append(" JOIN t").append(slave).append(" ON t").append(slave).append('.').append(key[0]).append(" = ").append(masterKey);
            if (rnd.nextInt(4) == 0) {
                final String[] secondKey = pickKeyPair(rnd);
                sql.append(" AND t").append(slave).append('.').append(secondKey[0]).append(" = t").append(master).append('.').append(secondKey[1]);
            }
            for (int c = 0, n = 1 + rnd.nextInt(2); c < n; c++) {
                if (!where.isEmpty()) {
                    where.append(" AND ");
                }
                appendConjunct(rnd, where, slave, masterKey, key[1], rnd.nextInt(slave));
            }
        }
        return sql.append(" WHERE ").append(where).toString();
    }

    // A value of the key column, or NULL. The tables share the values, so that every key type matches rows.
    private static String keyValue(Rnd rnd, String column) {
        if (rnd.nextInt(5) == 0) {
            return "NULL";
        }
        final int value = rnd.nextInt(3);
        return switch (column) {
            case "i", "l" -> String.valueOf(value);
            case "sy", "st", "vc" -> value == 0 ? "''" : value == 1 ? "'a'" : "'b'";
            case "ts", "tn" -> "'2024-01-01T00:00:0" + (value + 1) + "Z'";
            default -> value + ".5";
        };
    }

    // A pair of key columns, slave first: mostly of one type
    private static String[] pickKeyPair(Rnd rnd) {
        if (rnd.nextInt(10) < 7) {
            final String column = KEY_COLUMNS[rnd.nextInt(KEY_COLUMNS.length)];
            return new String[]{column, column};
        }
        final String[] pair = MIXED_KEY_PAIRS[rnd.nextInt(MIXED_KEY_PAIRS.length)];
        return rnd.nextBoolean() ? pair : new String[]{pair[1], pair[0]};
    }

    // the lines of the rows, without the header
    private static long rowCount(CharSequence rows) {
        long count = -1;
        for (int i = 0, n = rows.length(); i < n; i++) {
            if (rows.charAt(i) == '\n') {
                count++;
            }
        }
        return count;
    }

    private static String sortedRows(CharSequence rows) {
        final String[] lines = rows.toString().split("\n");
        Arrays.sort(lines, 1, lines.length);
        return String.join("\n", lines);
    }
}
