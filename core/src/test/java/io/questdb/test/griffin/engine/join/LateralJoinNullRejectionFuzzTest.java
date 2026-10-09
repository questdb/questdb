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
import io.questdb.std.Chars;
import io.questdb.std.Numbers;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Fuzzes the filters after a RIGHT or FULL join in a correlated LATERAL body, see
 * {@link LateralJoinNullRejectionTest}, on random data: NULL and duplicate outer keys, random
 * typed values, random combinations of filters, random bind variable values, and scalar
 * sub-queries with a WHERE of their own. Each query must return the rows that the body returns per
 * outer row, or fail with the LateralJoinRewriter error that rejects the filter.
 */
public class LateralJoinNullRejectionFuzzTest extends AbstractCairoTest {
    private static final String ERROR_SUFFIX = "is not supported in a correlated lateral sub-query";
    // name, type, values of the column and constants of the filters, bind variable type and values
    private static final FuzzColumn[] COLUMNS = {
            new FuzzColumn("bo", "BOOLEAN", new String[]{"true", "false"}, new String[]{"true", "false", "'false'", "NULL"}, null, null),
            new FuzzColumn("bt", "BYTE", new String[]{"0", "1", "10", "-1"}, new String[]{"0", "1", "10", "-1", "5", "NULL", "5 + 5"}, null, null),
            new FuzzColumn("sh", "SHORT", new String[]{"0", "1", "10", "-1"}, new String[]{"0", "1", "10", "-1", "-5", "NULL", "10::short"}, "short", new Object[]{(short) 0, (short) 1, (short) 10}),
            new FuzzColumn("ch", "CHAR", new String[]{"'a'", "'b'", "'z'"}, new String[]{"'a'", "'b'", "'z'", "''", "NULL"}, null, null),
            new FuzzColumn("i", "INT", new String[]{"0", "1", "10", "-5"}, new String[]{"0", "1", "10", "-5", "NULL", "NULL::int", "-2_147_483_648", "5 + 5"}, "int", new Object[]{null, 0, 1, 10}),
            new FuzzColumn("l", "LONG", new String[]{"0", "1", "10", "-5"}, new String[]{"0", "1", "10", "-5", "NULL", "NULL::long", "CAST(NULL AS LONG)"}, "long", new Object[]{null, 0L, 10L}),
            new FuzzColumn("fl", "FLOAT", new String[]{"0.0", "1.5", "10.0", "'NaN'", "'Infinity'"}, new String[]{"0.0", "1.5", "10.0", "NaN", "'NaN'", "'Infinity'", "'-Infinity'", "NULL", "NULL::float"}, null, null),
            new FuzzColumn("d", "DOUBLE", new String[]{"0.0", "1.5", "10.0", "NaN", "'Infinity'", "'-Infinity'"}, new String[]{"0.0", "1.5", "10.0", "NaN", "'NaN'", "'Infinity'", "1e400", "NULL", "NULL::double", "-1.5"}, "double", new Object[]{null, 0.0, 10.0, Double.POSITIVE_INFINITY}),
            new FuzzColumn("dc", "DECIMAL(18,2)", new String[]{"0::DECIMAL(18,2)", "10::DECIMAL(18,2)", "-1.5::DECIMAL(18,2)"}, new String[]{"0", "10", "-1", "NULL", "10::DECIMAL(18,2)"}, null, null),
            new FuzzColumn("dt", "DATE", new String[]{"'2024-01-01'", "'1970-01-01'"}, new String[]{"'2024-01-01'", "'1970-01-01'", "''", "NULL", "NULL::date"}, null, null),
            new FuzzColumn("t2", "TIMESTAMP", new String[]{"'2024-01-01T00:00:00.000000Z'", "0::timestamp"}, new String[]{"'2024-01-01T00:00:00.000000Z'", "0::timestamp", "''", "NULL", "NULL::timestamp"}, "timestamp", new Object[]{null, 0L, 1_704_067_200_000_000L}),
            new FuzzColumn("st", "STRING", new String[]{"''", "'a'", "'abc'"}, new String[]{"''", "'a'", "'abc'", "NULL", "NULL::string"}, "str", new Object[]{null, "", "abc"}),
            new FuzzColumn("vc", "VARCHAR", new String[]{"''", "'a'", "'abc'"}, new String[]{"''", "'a'", "'abc'", "NULL"}, "str", new Object[]{null, "a"}),
            new FuzzColumn("sy", "SYMBOL", new String[]{"''", "'a'", "'abc'"}, new String[]{"''", "'a'", "'abc'", "NULL"}, "str", new Object[]{null, "abc"}),
            new FuzzColumn("ip", "IPv4", new String[]{"'0.0.0.1'", "'1.1.1.1'", "'2.2.2.2'"}, new String[]{"'0.0.0.0'", "'1.1.1.1'", "'2.2.2.2'", "NULL"}, null, null),
            new FuzzColumn("u", "UUID", new String[]{"'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'"}, new String[]{"'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'", "'00000000-0000-0000-0000-000000000000'", "NULL"}, null, null),
    };
    private static final String[] COMPARISONS = {"=", "!=", "<", "<=", ">", ">="};
    // correlated conjuncts: terminateHere() moves some into the outer-ref join's ON clause, and
    // t.x = o.k lets the rewriter remove the outer-ref join
    private static final String[] CORRELATED = {"t.id > {K}", "t.id >= {K}", "t.x = {K}", "t.id != {K}"};
    private static final String[] LATERAL_JOINS = {"JOIN LATERAL", "CROSS JOIN LATERAL", "LEFT JOIN LATERAL"};
    private static final int QUERIES_PER_ROUND = 40;
    private static final Shape[] SHAPES = {
            new Shape("{T} t RIGHT JOIN refunds r ON t.x = r.k AND r.k = {K} WHERE {P}", false, false),
            new Shape("{T} t RIGHT JOIN refunds r ON r.k = {K} AND t.x = r.k WHERE {P}", false, false),
            new Shape("{T} t JOIN xs x ON x.k = {K} RIGHT JOIN refunds r ON t.x = r.k WHERE {P}", false, false),
            new Shape("{T} t JOIN xs x ON x.k = t.x AND x.k = {K} RIGHT JOIN refunds r ON t.x = r.k WHERE {P}", false, false),
            new Shape("{T} t JOIN xs x ON x.k = {K} FULL JOIN refunds r ON t.x = r.k WHERE {P}", false, false),
            new Shape("{T} t RIGHT JOIN refunds r ON t.x = r.k WHERE t.x = {K} AND {P}", true, false),
            // the rewriter fails on this shape, but must never return other rows than the reference
            new Shape("{T} t FULL JOIN refunds r ON t.x = r.k AND r.k = {K} WHERE {P}", false, true)
    };

    @Test
    public void testFuzzDenseNulls() throws Exception {
        fuzz(12, 0.5, 6, 8);
    }

    @Test
    public void testFuzzFewRows() throws Exception {
        fuzz(9, 0.3, 2, 3);
    }

    @Test
    public void testFuzzSparseNulls() throws Exception {
        fuzz(18, 0.1, 6, 10);
    }

    // passes every column through, or computes it with a cast to its own type, whose type the
    // rewriter does not know
    private static String projection(boolean isCast) {
        final StringBuilder sb = new StringBuilder("(SELECT id, x");
        for (FuzzColumn column : COLUMNS) {
            sb.append(", ");
            if (isCast) {
                sb.append("CAST(").append(column.name).append(" AS ").append(column.type).append(") ");
            }
            sb.append(column.name);
        }
        return sb.append(" FROM trades)").toString();
    }

    private static String randomValue(Rnd rnd, FuzzColumn column, double nullRate) {
        return rnd.nextDouble() < nullRate ? "NULL" : column.values[rnd.nextInt(column.values.length)];
    }

    private static String randomKey(Rnd rnd, double nullRate) {
        return rnd.nextDouble() < nullRate ? "NULL" : Integer.toString(rnd.nextInt(4));
    }

    // A scalar sub-query that reads the column, which the plan runs once per execution. The code
    // generator generates it for the filter and again for the NULL check of the filter, and both
    // must read the value that the sub-query returns on its own: with its WHERE, its LIMIT, its
    // LATEST ON and the WHERE of a nested sub-query.
    private static String randomScalarSubQuery(Rnd rnd, FuzzColumn column) {
        final String key = Integer.toString(rnd.nextInt(4));
        return switch (rnd.nextInt(5)) {
            case 0 -> "(SELECT " + column.name + " FROM trades WHERE id = " + (10 + rnd.nextInt(10)) + " LIMIT 1)";
            case 1 -> "(SELECT " + column.name + " FROM trades WHERE x = " + key + " ORDER BY ts DESC LIMIT 1)";
            case 2 -> "(SELECT max(" + column.name + ") FROM trades WHERE x = " + key + ")";
            case 3 -> "(SELECT " + column.name + " FROM trades WHERE x = " + key + " LATEST ON ts PARTITION BY x)";
            default ->
                    "(SELECT " + column.name + " FROM trades WHERE id = (SELECT max(id) FROM trades WHERE x = " + key + "))";
        };
    }

    private static String randomTable(Rnd rnd) {
        return switch (rnd.nextInt(8)) {
            case 0 -> "(SELECT * FROM trades)";
            case 1 -> "(SELECT * FROM (SELECT * FROM trades))";
            case 2 -> "(SELECT DISTINCT * FROM trades)";
            case 3 -> projection(false);
            case 4 -> projection(true);
            case 5 -> "(SELECT * FROM trades UNION ALL SELECT * FROM trades WHERE id < 0)";
            case 6 -> "(SELECT * FROM trades WHERE id IS NOT NULL OR id IS NULL)";
            default -> "trades";
        };
    }

    private static List<String> rowsOf(CharSequence printed) {
        final List<String> rows = new ArrayList<>(Arrays.asList(printed.toString().split("\n")));
        // the header
        rows.removeFirst();
        rows.removeIf(String::isEmpty);
        return rows;
    }

    private static void setBindVariable(String type, Object value) throws SqlException {
        switch (type) {
            case "short" -> bindVariableService.setShort(0, (Short) value);
            case "int" -> bindVariableService.setInt(0, value == null ? Numbers.INT_NULL : (Integer) value);
            case "long" -> bindVariableService.setLong(0, value == null ? Numbers.LONG_NULL : (Long) value);
            case "double" -> bindVariableService.setDouble(0, value == null ? Double.NaN : (Double) value);
            case "timestamp" -> bindVariableService.setTimestamp(0, value == null ? Numbers.LONG_NULL : (Long) value);
            default -> bindVariableService.setStr(0, (String) value);
        }
    }

    private void assertRandomQuery(Rnd rnd, int outerRowCount, List<String> failures) throws SqlException {
        bindVariableService.clear();
        final Bind bind = new Bind();
        final StringBuilder filter = new StringBuilder(randomPredicate(rnd, bind));
        for (int i = 0, n = rnd.nextInt(3); i < n; i++) {
            filter.append(rnd.nextInt(4) == 0 ? " OR " : " AND ").append(randomPredicate(rnd, bind));
        }
        final Shape shape = SHAPES[rnd.nextInt(SHAPES.length)];
        // The rewriter drops a correlated comparison next to WHERE t.x = o.k, whatever the join,
        // which is a separate bug, so the shape with that key gets no correlated conjunct.
        if (!shape.hasWhereKey && rnd.nextInt(3) == 0) {
            filter.insert(0, CORRELATED[rnd.nextInt(CORRELATED.length)] + " AND ");
        }
        if (bind.type != null) {
            setBindVariable(bind.type, bind.value);
        }
        final String table = randomTable(rnd);
        final String lateralJoin = LATERAL_JOINS[rnd.nextInt(LATERAL_JOINS.length)];
        final String body = shape.body.replace("{T}", table).replace("{P}", filter);
        final String sql = "SELECT o.id, l.tid, l.rid FROM orders o " + lateralJoin
                + " (SELECT t.id tid, r.id rid FROM " + body.replace("{K}", "o.k") + ") l";

        // per outer row: the body with the outer key as a literal
        final List<String> expected = new ArrayList<>();
        sink.clear();
        printSql("SELECT id, k FROM orders");
        final List<String> outerRows = rowsOf(sink);
        Assert.assertEquals(outerRowCount, outerRows.size());
        for (int i = 0, n = outerRows.size(); i < n; i++) {
            final String[] outer = outerRows.get(i).split("\t");
            final String key = outer[1].equals("null") ? "NULL" : outer[1];
            try {
                sink.clear();
                printSql("SELECT t.id tid, r.id rid FROM " + body.replace("{K}", key));
            } catch (Throwable e) {
                // the filter does not apply to the column type
                return;
            }
            final List<String> bodyRows = rowsOf(sink);
            for (int j = 0, m = bodyRows.size(); j < m; j++) {
                expected.add(outer[0] + '\t' + bodyRows.get(j));
            }
            if (bodyRows.isEmpty() && lateralJoin.startsWith("LEFT")) {
                expected.add(outer[0] + "\tnull\tnull");
            }
        }
        Collections.sort(expected);

        try {
            sink.clear();
            printSql(sql);
            final List<String> actual = rowsOf(sink);
            Collections.sort(actual);
            if (!actual.equals(expected)) {
                failures.add("WRONG ROWS " + actual + ", expected " + expected + (bind.type != null ? " $1=" + bind.value : "") + "\n    " + sql);
            }
        } catch (SqlException e) {
            if (!shape.isAnyErrorAllowed && !Chars.contains(e.getFlyweightMessage(), ERROR_SUFFIX)) {
                failures.add("failed with [" + e.getPosition() + "] " + e.getFlyweightMessage() + "\n    " + sql);
            }
        } catch (Throwable e) {
            failures.add("failed with " + e + "\n    " + sql);
        }
    }

    private void createTables(Rnd rnd, double nullRate, int maxOuterRows, int maxRows) throws SqlException {
        final int outerRowCount = 1 + rnd.nextInt(maxOuterRows);
        final StringBuilder sb = new StringBuilder("INSERT INTO orders VALUES ");
        for (int i = 0; i < outerRowCount; i++) {
            sb.append(i > 0 ? ", (" : "(").append(i + 1).append(", ").append(randomKey(rnd, nullRate)).append(", ").append(i).append("::timestamp)");
        }
        execute("CREATE TABLE orders (id INT, k INT, ts TIMESTAMP) TIMESTAMP(ts)");
        execute(sb.toString());

        execute("CREATE TABLE refunds (id INT, k INT, ts TIMESTAMP) TIMESTAMP(ts)");
        final int refundCount = rnd.nextInt(maxRows);
        if (refundCount > 0) {
            sb.setLength(0);
            sb.append("INSERT INTO refunds VALUES ");
            for (int i = 0; i < refundCount; i++) {
                sb.append(i > 0 ? ", (" : "(").append(100 + i).append(", ").append(randomKey(rnd, nullRate)).append(", ").append(i).append("::timestamp)");
            }
            execute(sb.toString());
        }

        execute("CREATE TABLE xs (k INT, v INT)");
        final int xsCount = rnd.nextInt(5);
        if (xsCount > 0) {
            sb.setLength(0);
            sb.append("INSERT INTO xs VALUES ");
            for (int i = 0; i < xsCount; i++) {
                sb.append(i > 0 ? ", (" : "(").append(randomKey(rnd, nullRate)).append(", ").append(i).append(')');
            }
            execute(sb.toString());
        }

        sb.setLength(0);
        sb.append("CREATE TABLE trades (id INT, x INT");
        for (FuzzColumn column : COLUMNS) {
            sb.append(", ").append(column.name).append(' ').append(column.type);
        }
        execute(sb.append(", ts TIMESTAMP) TIMESTAMP(ts)").toString());
        final int tradeCount = rnd.nextInt(maxRows);
        if (tradeCount > 0) {
            sb.setLength(0);
            sb.append("INSERT INTO trades VALUES ");
            for (int i = 0; i < tradeCount; i++) {
                sb.append(i > 0 ? ", (" : "(").append(rnd.nextDouble() < nullRate / 4 ? "NULL" : Integer.toString(10 + i))
                        .append(", ").append(randomKey(rnd, nullRate));
                for (FuzzColumn column : COLUMNS) {
                    sb.append(", ").append(randomValue(rnd, column, nullRate));
                }
                sb.append(", ").append(i).append("::timestamp)");
            }
            execute(sb.toString());
        }
    }

    private void fuzz(int rounds, double nullRate, int maxOuterRows, int maxRows) throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);
            final List<String> failures = new ArrayList<>();
            int queryCount = 0;
            for (int round = 0; round < rounds; round++) {
                createTables(rnd, nullRate, maxOuterRows, maxRows);
                sink.clear();
                printSql("SELECT count() FROM orders");
                final int outerRowCount = Integer.parseInt(rowsOf(sink).getFirst());
                for (int q = 0; q < QUERIES_PER_ROUND; q++) {
                    assertRandomQuery(rnd, outerRowCount, failures);
                    queryCount++;
                }
                execute("DROP TABLE orders");
                execute("DROP TABLE refunds");
                execute("DROP TABLE xs");
                execute("DROP TABLE trades");
            }
            bindVariableService.clear();
            if (!failures.isEmpty()) {
                Assert.fail(failures.size() + " of " + queryCount + " queries failed:\n" + String.join("\n", failures.subList(0, Math.min(20, failures.size()))));
            }
        });
    }

    private String randomPredicate(Rnd rnd, Bind bind) {
        final FuzzColumn column = COLUMNS[rnd.nextInt(COLUMNS.length)];
        final String ref = "t." + column.name;
        final String[] constants = column.constants;
        switch (rnd.nextInt(10)) {
            case 0:
                return ref + (rnd.nextBoolean() ? " IS NULL" : " IS NOT NULL");
            case 1: {
                final StringBuilder sb = new StringBuilder(ref).append(rnd.nextInt(4) == 0 ? " NOT IN (" : " IN (");
                for (int i = 0, n = 1 + rnd.nextInt(3); i < n; i++) {
                    sb.append(i > 0 ? ", " : "").append(constants[rnd.nextInt(constants.length)]);
                }
                return sb.append(')').toString();
            }
            case 2:
                return ref + (rnd.nextInt(4) == 0 ? " NOT BETWEEN " : " BETWEEN ")
                        + constants[rnd.nextInt(constants.length)] + " AND " + constants[rnd.nextInt(constants.length)];
            default: {
                String operand = constants[rnd.nextInt(constants.length)];
                if (column.bindType != null && bind.type == null && rnd.nextInt(4) == 0) {
                    bind.type = column.bindType;
                    bind.value = column.bindValues[rnd.nextInt(column.bindValues.length)];
                    operand = "$1";
                } else if (rnd.nextInt(4) == 0) {
                    operand = randomScalarSubQuery(rnd, column);
                }
                final String op = COMPARISONS[rnd.nextInt(COMPARISONS.length)];
                return rnd.nextBoolean() ? ref + ' ' + op + ' ' + operand : operand + ' ' + op + ' ' + ref;
            }
        }
    }

    private static class Bind {
        String type;
        Object value;
    }

    private record FuzzColumn(String name, String type, String[] values, String[] constants, String bindType,
                              Object[] bindValues) {
    }

    private record Shape(String body, boolean hasWhereKey, boolean isAnyErrorAllowed) {
    }
}
