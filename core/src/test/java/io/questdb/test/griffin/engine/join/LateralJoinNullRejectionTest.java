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

import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.std.Chars;
import io.questdb.std.Numbers;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * LateralJoinRewriter decorrelates a LATERAL body whose RIGHT or FULL join follows an ON clause that
 * reads the outer row only when a filter after the join drops every row that the join NULL-extends on
 * its master side: the decorrelated join gives those rows a NULL outer key and loses them. The tests
 * check that decision for every combination of the master column type, the filter, the sub-query
 * form that passes the column through, and the join shape.
 * <p>
 * Each query runs against a reference that evaluates the body once per outer row, with the outer key
 * as a literal, and combines the results with UNION ALL. A query must return the reference rows, or
 * fail with the rewriter's error at the RIGHT or FULL keyword. On top of that, the tests pin the
 * design: a filter that keeps NULL-extended rows fails, and so does one that the rewriter cannot
 * prove safe, while a provably safe filter returns rows.
 * <p>
 * The data gives every outer row NULL-extended rows. Trade 10 holds a value in each typed column,
 * and trade 11 holds what the RIGHT join puts in NULL-extended rows: NULL, or 0 or false for BYTE,
 * SHORT and BOOLEAN. A filter therefore keeps NULL-extended rows exactly when the reference holds a
 * row with a NULL trade id.
 */
public class LateralJoinNullRejectionTest extends AbstractCairoTest {
    private static final String[] BIND_INT = {
            "{C} >= $1", "{C} <= $1", "$1 <= {C}", "{C} > $1", "{C} < $1", "$1 < {C}",
            "{C} = $1", "{C} != $1", "{C} IN ($1, 10)", "{C} BETWEEN $1 AND $2"
    };
    private static final String[] BIND_SCALAR = {"{C} >= $1", "{C} <= $1", "{C} > $1", "{C} < $1", "{C} = $1", "{C} BETWEEN $1 AND $2"};
    private static final String[] BIND_SHORT = {"{C} > $1", "{C} >= $1", "{C} = $1"};
    private static final String[] BIND_STR = {"{C} >= $1", "{C} > $1", "{C} = $1", "{C} IN ($1)"};
    private static final String[] BOOLEAN = {
            "{C} = false", "false = {C}", "{C} = true", "{C} != true", "{C} != false", "{C} IN (true)",
            "{C} IN (false)", "{C} IN (false, true)", "{C} = 'false'", "{C} = 'true'", "{C} = NULL::boolean"
    };
    private static final String[] CHAR = {
            "{C} = 'a'", "'a' = {C}", "{C} != 'a'", "{C} < 'z'", "{C} > 'a'", "{C} <= 'z'", "{C} >= 'a'",
            "{C} >= 'b'", "{C} IN ('a', 'b')", "{C} IN ('b')", "{C} <= NULL", "{C} >= NULL", "{C} = ''",
            "{C} = NULL::char"
    };
    private static final String ERROR_SUFFIX = "is not supported in a correlated lateral sub-query";
    private static final String[] FLOATING = {
            "{C} = 'NaN'", "'NaN' = {C}", "{C} = NaN", "{C} <= 'NaN'", "{C} >= 'NaN'", "{C} < 'NaN'",
            "{C} > 'NaN'", "{C} != 'NaN'", "{C} = 'Infinity'", "{C} = '-Infinity'", "{C} <= 'Infinity'",
            "{C} = 1e400", "{C} IN ('NaN', 10.0)", "{C} IN (NaN, 10.0)", "{C} IN (10.0, 11.0)", "{C} = -1.5",
            "{C} > -1.5", "{C} < -1.5", "{C} >= NULL::double", "{C} = NULL::double", "{C} = 10.0",
            "{C} >= 10.0", "{C} BETWEEN 5.5 AND 15.5"
    };
    private static final String[] GEO_1 = {"{C} = #u", "{C} != #u", "{C} IN (#u)", "{C} = 'u'"};
    private static final String[] GEO_2 = {"{C} = #u3", "{C} != #u3", "{C} IN (#u3)", "{C} = 'u3'"};
    private static final String[] GEO_4 = {"{C} = #u33d", "{C} != #u33d", "{C} IN (#u33d)", "{C} = 'u33d'"};
    private static final String[] GEO_8 = {"{C} = #u33dc0cp", "{C} != #u33dc0cp", "{C} IN (#u33dc0cp)", "{C} = 'u33dc0cp'"};
    private static final Pattern INTEGER_COMPARISON = Pattern.compile("^\\{C} (=|<|<=|>|>=) (-?[0-9][0-9_]*L?)$");
    private static final Pattern INTEGER_COMPARISON_FLIPPED = Pattern.compile("^(-?[0-9][0-9_]*L?) (=|<|<=|>|>=) \\{C}$");
    private static final Pattern INTEGER_IN = Pattern.compile("^\\{C} IN \\((-?[0-9][0-9_]*L?(, -?[0-9][0-9_]*L?)*)\\)$");
    private static final String[] IPV4 = {
            "{C} = '1.1.1.1'", "{C} = '0.0.0.0'", "{C} != '0.0.0.0'", "{C} <= '0.0.0.0'", "{C} >= '0.0.0.0'",
            "{C} > '0.0.0.1'", "{C} < '2.2.2.2'", "{C} IN ('0.0.0.0', '1.1.1.1')", "{C} IN ('1.1.1.1')",
            "{C} = NULL::ipv4", "{C} > NULL"
    };
    private static final Pattern LESS_OR_GREATER = Pattern.compile("^\\{C} [<>] .+$|^.+ [<>] \\{C}$");
    private static final String[] LONG256 = {"{C} = 0x01", "{C} != 0x01", "{C} < 0x05", "{C} > 0x00", "{C} >= 0x00", "{C} <= 0x05", "{C} IN (0x01)"};
    private static final String[] NULL_CHECKS = {"{C} IS NULL", "{C} IS NOT NULL", "{C} = NULL", "{C} != NULL", "NULL = {C}", "{C} IN (NULL)"};
    private static final String[] NUMERIC = {
            "{C} = 10", "10 = {C}", "{C} = 0", "{C} != 0", "{C} != 10", "{C} < 11", "11 > {C}", "{C} > 5",
            "5 < {C}", "{C} > -5", "{C} < -5", "{C} <= 10", "{C} >= 0", "{C} >= 5", "{C} <= NULL",
            "{C} >= NULL", "{C} < NULL", "{C} > NULL", "{C} IN (10)", "{C} IN (0)", "{C} IN (10, 11)",
            "{C} IN (0, 10)", "{C} IN (NULL, 10)", "{C} NOT IN (10)", "{C} BETWEEN 5 AND 15",
            "{C} BETWEEN 15 AND 5", "{C} BETWEEN -5 AND 5", "{C} NOT BETWEEN 5 AND 15", "{C} >= 5 + 5",
            "{C} = 10::int", "{C} >= NULL::int", "{C} <= NULL::long", "{C} = NULL::int",
            "{C} = -2_147_483_648", "{C} <= -2_147_483_648", "{C} < -2_147_483_648", "{C} > -2_147_483_648"
    };
    private static final String[] OUTER_KEYS = {"1", "2", "3"};
    private static final String[] STRING = {
            "{C} = 'abc'", "'abc' = {C}", "{C} != 'abc'", "{C} < 'zzz'", "{C} > ''", "{C} <= 'zzz'",
            "{C} >= ''", "{C} = ''", "{C} IN ('a', 'abc')", "{C} IN ('abc')", "{C} IN (NULL, 'abc')",
            "{C} >= NULL", "{C} <= NULL", "{C} LIKE 'a%'", "{C} ILIKE 'A%'", "{C} ~ 'a'",
            "{C} NOT IN ('x')", "{C} = NULL::string"
    };
    private static final String[] TIME = {
            "{C} = '2024-01-01'", "{C} >= '2024-01-01'", "{C} > '2023-01-01'", "{C} < '2025-01-01'",
            "{C} <= '2025-01-01'", "{C} < now()", "{C} <= now()", "{C} > dateadd('y', -100, now())",
            "{C} >= dateadd('y', -100, now())", "{C} BETWEEN '2023-01-01' AND '2025-01-01'",
            "{C} BETWEEN dateadd('y', -100, now()) AND now()", "{C} >= ''", "{C} = ''", "{C} <= NULL",
            "{C} IN ('2024-01-01')", "{C} >= NULL::timestamp", "{C} > 0"
    };
    private static final String[] UUID = {
            "{C} = 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'", "{C} != 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'",
            "{C} IN ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11')", "{C} IN (NULL, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11')",
            "{C} = '00000000-0000-0000-0000-000000000000'", "{C} = NULL::uuid"
    };
    // name, type, value of trade 10, value of trade 11, which is what a RIGHT join puts in a
    // NULL-extended row, filters, bind variable type and filters with bind variables
    private static final Column[] COLUMNS = {
            new Column("bo", "BOOLEAN", "true", "false", cat(NULL_CHECKS, BOOLEAN), null, null),
            new Column("bt", "BYTE", "10", "0", cat(NULL_CHECKS, NUMERIC), null, null),
            new Column("sh", "SHORT", "10", "0", cat(NULL_CHECKS, NUMERIC), "short", BIND_SHORT),
            new Column("ch", "CHAR", "'a'", "NULL", cat(NULL_CHECKS, CHAR), null, null),
            new Column("i", "INT", "10", "NULL", cat(NULL_CHECKS, NUMERIC), "int", BIND_INT),
            new Column("l", "LONG", "10", "NULL", cat(NULL_CHECKS, NUMERIC), "long", BIND_SCALAR),
            new Column("fl", "FLOAT", "10.0", "NULL", cat(NULL_CHECKS, cat(NUMERIC, FLOATING)), "float", BIND_SCALAR),
            new Column("d", "DOUBLE", "10.0", "NULL", cat(NULL_CHECKS, cat(NUMERIC, FLOATING)), "double", BIND_SCALAR),
            new Column("d8", "DECIMAL(2,0)", "10::DECIMAL(2,0)", "NULL", cat(NULL_CHECKS, NUMERIC), null, null),
            new Column("d16", "DECIMAL(4,1)", "10::DECIMAL(4,1)", "NULL", cat(NULL_CHECKS, NUMERIC), null, null),
            new Column("d32", "DECIMAL(9,2)", "10::DECIMAL(9,2)", "NULL", cat(NULL_CHECKS, NUMERIC), null, null),
            new Column("d64", "DECIMAL(18,2)", "10::DECIMAL(18,2)", "NULL", cat(NULL_CHECKS, NUMERIC), null, null),
            new Column("d128", "DECIMAL(38,4)", "10::DECIMAL(38,4)", "NULL", cat(NULL_CHECKS, NUMERIC), null, null),
            new Column("d256", "DECIMAL(60,4)", "10::DECIMAL(60,4)", "NULL", cat(NULL_CHECKS, NUMERIC), null, null),
            new Column("dt", "DATE", "'2024-01-01'", "NULL", cat(NULL_CHECKS, TIME), "date", BIND_SCALAR),
            new Column("t2", "TIMESTAMP", "'2024-01-01T00:00:00.000000Z'", "NULL", cat(NULL_CHECKS, TIME), "timestamp", BIND_SCALAR),
            new Column("tn", "TIMESTAMP_NS", "'2024-01-01T00:00:00.000000000Z'", "NULL", cat(NULL_CHECKS, TIME), "timestamp_ns", BIND_SCALAR),
            new Column("st", "STRING", "'abc'", "NULL", cat(NULL_CHECKS, STRING), "str", BIND_STR),
            new Column("vc", "VARCHAR", "'abc'", "NULL", cat(NULL_CHECKS, STRING), "str", BIND_STR),
            new Column("sy", "SYMBOL", "'abc'", "NULL", cat(NULL_CHECKS, STRING), "str", BIND_STR),
            new Column("ip", "IPv4", "'1.1.1.1'", "NULL", cat(NULL_CHECKS, IPV4), null, null),
            new Column("u", "UUID", "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'", "NULL", cat(NULL_CHECKS, UUID), null, null),
            new Column("h", "LONG256", "0x01", "NULL", cat(NULL_CHECKS, LONG256), null, null),
            new Column("g1", "GEOHASH(1c)", "#u", "NULL", cat(NULL_CHECKS, GEO_1), null, null),
            new Column("g2", "GEOHASH(2c)", "#u3", "NULL", cat(NULL_CHECKS, GEO_2), null, null),
            new Column("g4", "GEOHASH(4c)", "#u33d", "NULL", cat(NULL_CHECKS, GEO_4), null, null),
            new Column("g8", "GEOHASH(8c)", "#u33dc0cp", "NULL", cat(NULL_CHECKS, GEO_8), null, null),
            new Column("arr", "DOUBLE[]", "ARRAY[1.0, 2.0]", "NULL", cat(NULL_CHECKS, new String[]{"{C} = ARRAY[1.0, 2.0]", "{C} != ARRAY[1.0, 2.0]"}), null, null),
    };

    // A bind variable keeps its value across executions of a cached plan, so a filter that the
    // rewriter accepts with a bind variable, < > or BETWEEN on a type with NULL, must hold for
    // every value of the same compiled query. A filter whose acceptance would depend on the value,
    // such as >=, fails whatever value the variable holds at compile time, see the matrix tests.
    @Test
    public void testBindVariableValuesShareThePlan() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String[] filters = {"t.i > $1", "t.i < $1", "$1 < t.i", "t.i BETWEEN $1 AND $2", "t.l > $1", "t.d < $1", "t.t2 > $1", "t.st > $1"};
            final String[] bindTypes = {"int", "int", "int", "int", "long", "double", "timestamp", "str"};
            for (Shape shape : new Shape[]{Shape.RIGHT_ON, Shape.INNER_ON_RIGHT, Shape.INNER_ON_FULL}) {
                for (int f = 0; f < filters.length; f++) {
                    final String sql = lateralSql(shape, Form.TABLE, filters[f]);
                    bindVariableService.clear();
                    setBindVariables(bindTypes[f], BindValues.LOW);
                    try (RecordCursorFactory factory = select(sql)) {
                        for (BindValues values : new BindValues[]{BindValues.LOW, BindValues.NULL, BindValues.HIGH, BindValues.LOW}) {
                            setBindVariables(bindTypes[f], values);
                            final List<String> actual;
                            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                                final StringSink rows = new StringSink();
                                println(factory.getMetadata(), cursor, rows);
                                actual = sortedRows(rows);
                            }
                            final Outcome reference = run(referenceSql(shape, filters[f]));
                            Assert.assertNull(reference.error);
                            Assert.assertEquals(shape + " " + filters[f] + " " + values, reference.rows, actual);
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testFullJoinAfterCorrelatedInnerJoin() throws Exception {
        assertShape(Shape.INNER_ON_FULL);
    }

    // The outer-ref join of a FULL join whose ON reads the outer row cannot be decorrelated in
    // general, so the test only checks that no such query returns rows other than the reference.
    @Test
    public void testFullJoinWithCorrelatedOn() throws Exception {
        assertShape(Shape.FULL_ON);
    }

    @Test
    public void testRightJoinAfterCorrelatedInnerJoin() throws Exception {
        assertShape(Shape.INNER_ON_RIGHT);
    }

    @Test
    public void testRightJoinWithCorrelatedOn() throws Exception {
        assertShape(Shape.RIGHT_ON);
    }

    // WHERE t.x = o.k lets the rewriter key the rows by t.x and remove the outer-ref join
    @Test
    public void testRightJoinWithCorrelatedWhereKey() throws Exception {
        assertShape(Shape.WHERE_KEY_RIGHT);
    }

    private static String[] cat(String[] a, String[] b) {
        final String[] r = Arrays.copyOf(a, a.length + b.length);
        System.arraycopy(b, 0, r, a.length, b.length);
        return r;
    }

    private static boolean compare(String op, long l, long r) {
        return switch (op) {
            case "=" -> l == r;
            case "<" -> l < r;
            case "<=" -> l <= r;
            case ">" -> l > r;
            default -> l >= r;
        };
    }

    private static void createTables() throws SqlException {
        execute("CREATE TABLE orders (id INT, k INT, ts TIMESTAMP) TIMESTAMP(ts)");
        execute("INSERT INTO orders VALUES (1, 1, 1::timestamp), (2, 2, 2::timestamp), (3, 3, 3::timestamp)");
        execute("CREATE TABLE refunds (id INT, k INT, ts TIMESTAMP) TIMESTAMP(ts)");
        execute("INSERT INTO refunds VALUES (100, 1, 1::timestamp), (101, 2, 2::timestamp), (102, 3, 3::timestamp)");
        execute("CREATE TABLE xs (k INT, v INT)");
        execute("INSERT INTO xs VALUES (1, 100), (2, 200), (3, 300)");
        final StringBuilder ddl = new StringBuilder("CREATE TABLE trades (id INT, x INT");
        final StringBuilder valueRow = new StringBuilder("(10, 1");
        final StringBuilder fillRow = new StringBuilder("(11, 2");
        for (Column column : COLUMNS) {
            ddl.append(", ").append(column.name).append(' ').append(column.type);
            valueRow.append(", ").append(column.value);
            fillRow.append(", ").append(column.fill);
        }
        execute(ddl.append(", ts TIMESTAMP) TIMESTAMP(ts)").toString());
        execute("INSERT INTO trades VALUES " + valueRow.append(", 1::timestamp)") + ", " + fillRow.append(", 2::timestamp)"));
    }

    private static Expected expected(Shape shape, Form form, Column column, String template, boolean keepsNullExtendedRows) {
        if (shape.mode == Mode.WHERE_KEY) {
            return Expected.ROWS;
        }
        if (shape.mode == Mode.ANY) {
            return Expected.ROWS_OR_ERROR;
        }
        if (keepsNullExtendedRows || isUnsupported(template)) {
            return Expected.ERROR;
        }
        if (!form.isTypeKnown) {
            // a column of unknown type may hold NULL, or 0 for a BYTE or SHORT
            return isBetween(template) || isNullAndZeroRejectingInteger(template) ? Expected.ROWS : Expected.ERROR;
        }
        if (!template.contains("$") && !template.contains("now()")) {
            return Expected.ROWS;
        }
        // the value of a bind variable or now() may be NULL in another execution of the plan
        return (isBetween(template) || LESS_OR_GREATER.matcher(template).matches()) && column.hasNull() ? Expected.ROWS : Expected.ERROR;
    }

    private static String instantiate(String template, String columnRef) {
        return template.replace("{C}", columnRef);
    }

    private static boolean isBetween(String template) {
        return template.contains(" BETWEEN ") && !template.contains(" NOT BETWEEN ");
    }

    // Mirrors the rule for a column of unknown type: integer constants, other than Integer.MIN_VALUE,
    // that the comparison or IN rejects for 0 as well as for NULL
    private static boolean isNullAndZeroRejectingInteger(String template) {
        Matcher m = INTEGER_COMPARISON.matcher(template);
        if (m.matches()) {
            return isNullAndZeroRejecting(m.group(1), m.group(2));
        }
        m = INTEGER_COMPARISON_FLIPPED.matcher(template);
        if (m.matches()) {
            final String op = m.group(2);
            final String flipped = switch (op) {
                case "<" -> ">";
                case "<=" -> ">=";
                case ">" -> "<";
                case ">=" -> "<=";
                default -> op;
            };
            return isNullAndZeroRejecting(flipped, m.group(1));
        }
        m = INTEGER_IN.matcher(template);
        if (m.matches()) {
            for (String value : m.group(1).split(", ")) {
                final long v = parseLong(value);
                if (v == 0 || v == Integer.MIN_VALUE) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    private static boolean isNullAndZeroRejecting(String op, String value) {
        final long v = parseLong(value);
        return v != Integer.MIN_VALUE && !compare(op, 0, v);
    }

    private static boolean isRewriterError(Throwable error, int position) {
        return error instanceof SqlException e
                && Chars.contains(e.getFlyweightMessage(), ERROR_SUFFIX)
                && e.getPosition() == position;
    }

    // the rewriter only proves comparisons, IN and BETWEEN
    private static boolean isUnsupported(String template) {
        return template.contains(" NOT IN ")
                || template.contains(" NOT BETWEEN ")
                || template.contains("LIKE ")
                || template.contains(" ~ ");
    }

    private static String lateralSql(Shape shape, Form form, Column column, String template) {
        final String filter = instantiate(template, form.columnRef.replace("{c}", column.name));
        final String table = form.table.replace("{c}", column.name).replace("{t}", column.type);
        final String body = shape.body.replace("{T}", table).replace("{K}", "o.k").replace("{P}", filter);
        return form.prefix.replace("{c}", column.name)
                + "SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (SELECT t.id tid, r.id rid FROM " + body + ") l";
    }

    private static String lateralSql(Shape shape, Form form, String filter) {
        final String body = shape.body.replace("{T}", form.table).replace("{K}", "o.k").replace("{P}", filter);
        return "SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (SELECT t.id tid, r.id rid FROM " + body + ") l";
    }

    private static long parseLong(String value) {
        try {
            return Numbers.parseLong(value);
        } catch (Exception e) {
            throw new AssertionError(value, e);
        }
    }

    // the body once per outer row, with the outer key as a literal, over the table itself
    private static String referenceSql(Shape shape, String filter) {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < OUTER_KEYS.length; i++) {
            if (i > 0) {
                sb.append(" UNION ALL ");
            }
            final String body = shape.body.replace("{T}", "trades").replace("{K}", OUTER_KEYS[i]).replace("{P}", filter);
            sb.append("SELECT ").append(i + 1).append(" id, tid, rid FROM (SELECT t.id tid, r.id rid FROM ").append(body).append(')');
        }
        return sb.toString();
    }

    private static Outcome run(String sql) {
        try {
            sink.clear();
            printSql(sql);
            return new Outcome(sortedRows(sink), null);
        } catch (Throwable e) {
            return new Outcome(null, e);
        }
    }

    private static void setBindVariables(String type, BindValues values) throws SqlException {
        for (int i = 0; i < 2; i++) {
            final boolean isNull = values == BindValues.NULL;
            final boolean isLow = (values == BindValues.LOW) == (i == 0);
            switch (type) {
                case "short" -> bindVariableService.setShort(i, (short) (isNull ? 0 : isLow ? 5 : 15));
                case "int" -> bindVariableService.setInt(i, isNull ? Numbers.INT_NULL : isLow ? 5 : 15);
                case "long" -> bindVariableService.setLong(i, isNull ? Numbers.LONG_NULL : isLow ? 5 : 15);
                case "float" -> bindVariableService.setFloat(i, isNull ? Float.NaN : isLow ? 5 : 15);
                case "double" -> bindVariableService.setDouble(i, isNull ? Double.NaN : isLow ? 5 : 15);
                // 2023-01-01 and 2025-01-01
                case "date" -> bindVariableService.setDate(i, isNull ? Numbers.LONG_NULL : isLow ? 1_672_531_200_000L : 1_735_689_600_000L);
                case "timestamp" -> bindVariableService.setTimestamp(i, isNull ? Numbers.LONG_NULL : isLow ? 1_672_531_200_000_000L : 1_735_689_600_000_000L);
                case "timestamp_ns" -> bindVariableService.setTimestampNano(i, isNull ? Numbers.LONG_NULL : isLow ? 1_672_531_200_000_000_000L : 1_735_689_600_000_000_000L);
                default -> bindVariableService.setStr(i, isNull ? null : isLow ? "abb" : "abd");
            }
        }
    }

    private static List<String> sortedRows(CharSequence printed) {
        final List<String> rows = new ArrayList<>(Arrays.asList(printed.toString().split("\n")));
        // the header
        rows.removeFirst();
        rows.removeIf(String::isEmpty);
        Collections.sort(rows);
        return rows;
    }

    private static String summary(Outcome outcome) {
        if (outcome.error == null) {
            return outcome.rows.size() + " rows " + outcome.rows;
        }
        final String message = outcome.error instanceof SqlException e
                ? "[" + e.getPosition() + "] " + e.getFlyweightMessage()
                : String.valueOf(outcome.error.getMessage());
        return outcome.error.getClass().getSimpleName() + ": " + message;
    }

    private void assertCase(
            Shape shape,
            Form form,
            Column column,
            String template,
            BindValues bindValues,
            Outcome reference,
            List<String> failures
    ) {
        final String sql = lateralSql(shape, form, column, template);
        final Outcome actual = run(sql);
        final String header = shape + " " + form + " " + column.type + " [" + template + "]" + (bindValues != null ? " " + bindValues : "");
        if (reference.error != null) {
            if (actual.error == null) {
                failures.add(header + ": returned " + summary(actual) + ", the reference failed with " + summary(reference) + "\n    " + sql);
            }
            return;
        }
        if (actual.error == null && !actual.rows.equals(reference.rows)) {
            failures.add(header + ": WRONG ROWS " + summary(actual) + ", expected " + summary(reference) + "\n    " + sql);
            return;
        }
        boolean keepsNullExtendedRows = false;
        for (int i = 0, n = reference.rows.size(); i < n; i++) {
            keepsNullExtendedRows |= reference.rows.get(i).split("\t")[1].equals("null");
        }
        final Expected expected = expected(shape, form, column, template, keepsNullExtendedRows);
        final int position = sql.indexOf(shape.joinKeyword);
        switch (expected) {
            case ROWS -> {
                if (actual.error != null) {
                    failures.add(header + ": failed with " + summary(actual) + ", expected " + summary(reference) + "\n    " + sql);
                }
            }
            case ERROR -> {
                if (actual.error == null) {
                    failures.add(header + ": returned " + summary(actual) + ", expected the rewriter to reject the filter\n    " + sql);
                } else if (!isRewriterError(actual.error, position)) {
                    failures.add(header + ": failed with " + summary(actual) + ", expected [" + position + "] ... " + ERROR_SUFFIX + "\n    " + sql);
                }
            }
            default -> {
                if (actual.error != null && !(actual.error instanceof SqlException)) {
                    failures.add(header + ": failed with " + summary(actual) + "\n    " + sql);
                }
            }
        }
    }

    private void assertShape(Shape shape) throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final List<String> failures = new ArrayList<>();
            int caseCount = 0;
            for (Column column : COLUMNS) {
                final List<Form> forms = new ArrayList<>();
                for (Form form : Form.values()) {
                    // GROUP BY, DISTINCT and the like do not accept every column type
                    final String table = form.table.replace("{c}", column.name).replace("{t}", column.type);
                    if (run(form.prefix.replace("{c}", column.name) + "SELECT * FROM " + table).error == null) {
                        forms.add(form);
                    }
                }
                Assert.assertTrue(column.type, forms.contains(Form.TABLE));
                for (String template : column.filters) {
                    caseCount += assertTemplate(shape, column, forms, template, null, failures);
                }
                if (column.bindType != null) {
                    for (String template : column.bindFilters) {
                        for (BindValues values : BindValues.values()) {
                            // SHORT has no NULL
                            if (values != BindValues.NULL || !column.bindType.equals("short")) {
                                caseCount += assertTemplate(shape, column, forms, template, values, failures);
                            }
                        }
                    }
                }
            }
            bindVariableService.clear();
            if (!failures.isEmpty()) {
                Assert.fail(failures.size() + " of " + caseCount + " cases failed:\n" + String.join("\n", failures.subList(0, Math.min(40, failures.size()))));
            }
        });
    }

    private int assertTemplate(
            Shape shape,
            Column column,
            List<Form> forms,
            String template,
            BindValues bindValues,
            List<String> failures
    ) throws SqlException {
        bindVariableService.clear();
        if (bindValues != null) {
            setBindVariables(column.bindType, bindValues);
        }
        final Outcome reference = run(referenceSql(shape, instantiate(template, "t." + column.name)));
        for (int i = 0, n = forms.size(); i < n; i++) {
            assertCase(shape, forms.get(i), column, template, bindValues, reference, failures);
        }
        return forms.size();
    }

    private enum BindValues {
        LOW, NULL, HIGH
    }

    private enum Expected {
        ROWS, ERROR, ROWS_OR_ERROR
    }

    // how the sub-query passes the column through; the rewriter knows the type of a column that a
    // sub-query renames, but not of one that it computes or that a set operation may widen
    private enum Form {
        TABLE("", "trades", "t.{c}", true),
        WILDCARD("", "(SELECT * FROM trades)", "t.{c}", true),
        PROJECTION("", "(SELECT id, x, {c} FROM trades)", "t.{c}", true),
        RENAME("", "(SELECT id, x, {c} AS renamed FROM trades)", "t.renamed", true),
        QUALIFIED("", "(SELECT tr.id, tr.x, tr.{c} FROM trades tr)", "t.{c}", true),
        NESTED("", "(SELECT * FROM (SELECT id, x, {c} FROM trades))", "t.{c}", true),
        CTE("WITH tt AS (SELECT id, x, {c} FROM trades) ", "tt", "t.{c}", true),
        GROUP_BY("", "(SELECT id, x, {c} FROM trades GROUP BY id, x, {c})", "t.{c}", true),
        DISTINCT("", "(SELECT DISTINCT id, x, {c} FROM trades)", "t.{c}", true),
        // the parser drops the alias of a column cast with ::GEOHASH(1c), so the form spells the cast out
        CAST("", "(SELECT id, x, CAST({c} AS {t}) {c} FROM trades)", "t.{c}", false),
        UNION("", "(SELECT id, x, {c} FROM trades UNION ALL SELECT id, x, {c} FROM trades WHERE id < 0)", "t.{c}", false);

        final String columnRef;
        final boolean isTypeKnown;
        final String prefix;
        final String table;

        Form(String prefix, String table, String columnRef, boolean isTypeKnown) {
            this.prefix = prefix;
            this.table = table;
            this.columnRef = columnRef;
            this.isTypeKnown = isTypeKnown;
        }
    }

    private enum Mode {
        // the outer-ref join stays ahead of the join, which needs a filter that drops the rows it
        // NULL-extends
        PINNED,
        // the rewriter removes the outer-ref join, and the query returns the reference rows
        WHERE_KEY,
        // any SqlException is acceptable, but never rows other than the reference
        ANY
    }

    private enum Shape {
        RIGHT_ON("{T} t RIGHT JOIN refunds r ON t.x = r.k AND r.k = {K} WHERE {P}", "RIGHT JOIN refunds", Mode.PINNED),
        INNER_ON_RIGHT("{T} t JOIN xs x ON x.k = {K} RIGHT JOIN refunds r ON t.x = r.k WHERE {P}", "RIGHT JOIN refunds", Mode.PINNED),
        INNER_ON_FULL("{T} t JOIN xs x ON x.k = {K} FULL JOIN refunds r ON t.x = r.k WHERE {P}", "FULL JOIN refunds", Mode.PINNED),
        WHERE_KEY_RIGHT("{T} t RIGHT JOIN refunds r ON t.x = r.k WHERE t.x = {K} AND {P}", "RIGHT JOIN refunds", Mode.WHERE_KEY),
        FULL_ON("{T} t FULL JOIN refunds r ON t.x = r.k AND r.k = {K} WHERE {P}", "FULL JOIN refunds", Mode.ANY);

        final String body;
        final String joinKeyword;
        final Mode mode;

        Shape(String body, String joinKeyword, Mode mode) {
            this.body = body;
            this.joinKeyword = joinKeyword;
            this.mode = mode;
        }
    }

    private record Column(String name, String type, String value, String fill, String[] filters, String bindType, String[] bindFilters) {
        // BOOLEAN, BYTE and SHORT have no NULL: a RIGHT join puts false or 0 in them
        boolean hasNull() {
            return !type.equals("BOOLEAN") && !type.equals("BYTE") && !type.equals("SHORT");
        }
    }

    private record Outcome(List<String> rows, Throwable error) {
    }
}
