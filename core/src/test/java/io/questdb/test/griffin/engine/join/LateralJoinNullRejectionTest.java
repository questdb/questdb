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
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.table.LateralNullCheckRecordCursorFactory;
import io.questdb.std.Numbers;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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
 * fail with the rewriter's error at the RIGHT or FULL keyword, or, for a bind variable that is NULL
 * when the query runs, with the error that the plan raises at that variable. On top of that, the tests pin the
 * design: a filter that keeps NULL-extended rows fails, and so does one that is not provably safe, while
 * a provably safe filter returns rows. That holds for every sub-query form: the rewriter decides on the
 * type of a table column and of a sub-query column that passes one through, and the code generator on
 * the type of any other sub-query column, such as a computed one, which only it knows.
 * <p>
 * A filter that does not compile for its column type, such as BETWEEN on a FLOAT, fails the
 * reference. The filter must then fail with the same error on the table alone, which shows that the
 * join did not reject a valid filter. The query must fail with that error too, or, while the
 * outer-ref join stays, with the rewriter's error, which the rewriter raises before the code
 * generator compiles the filter. Any exception other than a SqlException fails the test at once.
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
    private static final String NULL_VALUE_SUFFIX = "is not supported in a correlated lateral sub-query when this value is NULL";
    // the error of the rewriter for a filter that may keep the rows that a RIGHT or FULL join NULL-extends
    private static final String OUTER_COLUMN_ERROR = "outer column reference in an ON clause at or before a RIGHT or FULL join " + ERROR_SUFFIX;
    private static final String[] OUTER_KEYS = {"1", "2", "3"};
    // = <= and >= against a bind variable or now(), whose value the plan checks for NULL once per execution
    private static final Pattern RUNTIME_CHECKED = Pattern.compile("^\\{C} (=|<=|>=) (\\$1|.*now\\(\\).*)$|^\\$1 (=|<=|>=) \\{C}$");
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

    // A cached plan runs with every value of its bind variables, so a filter that the rewriter
    // accepts with a bind variable must hold for every value of the same compiled query. < > and
    // BETWEEN on a type with NULL drop the NULL-extended rows whatever the value. = <= and >= drop
    // them only while the value is not NULL, so the plan checks the value once per execution and
    // fails for NULL, and returns the reference rows again for the next non-NULL value.
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
                            final String header = shape + " " + filters[f] + " " + values;
                            final List<String> actual;
                            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                                final StringSink rows = new StringSink();
                                println(factory.getMetadata(), cursor, rows);
                                actual = sortedRows(rows);
                            }
                            final Outcome reference = run(referenceSql(shape, filters[f]), header);
                            Assert.assertNull(header, reference.error);
                            Assert.assertEquals(header, reference.rows, actual);
                        }
                    }
                }
            }
            final String[] checkedFilters = {"t.i >= $1", "t.i <= $1", "$1 <= t.i", "t.i = $1", "t.l >= $1", "t.d <= $1", "t.t2 >= $1", "t.st >= $1", "t.sy = $1"};
            final String[] checkedBindTypes = {"int", "int", "int", "int", "long", "double", "timestamp", "str", "str"};
            for (Shape shape : new Shape[]{Shape.RIGHT_ON, Shape.INNER_ON_RIGHT, Shape.INNER_ON_FULL}) {
                for (int f = 0; f < checkedFilters.length; f++) {
                    final String sql = lateralSql(shape, Form.TABLE, checkedFilters[f]);
                    bindVariableService.clear();
                    setBindVariables(checkedBindTypes[f], BindValues.LOW);
                    try (RecordCursorFactory factory = select(sql)) {
                        for (BindValues values : new BindValues[]{BindValues.LOW, BindValues.NULL, BindValues.HIGH, BindValues.LOW}) {
                            setBindVariables(checkedBindTypes[f], values);
                            final String header = shape + " " + checkedFilters[f] + " " + values;
                            if (values == BindValues.NULL) {
                                try (RecordCursor ignored = factory.getCursor(sqlExecutionContext)) {
                                    Assert.fail(header + ": expected the NULL value to fail the execution");
                                } catch (SqlException e) {
                                    TestUtils.assertContains(header, e.getFlyweightMessage(), NULL_VALUE_SUFFIX);
                                    Assert.assertEquals(header, sql.indexOf("$1"), e.getPosition());
                                }
                                continue;
                            }
                            final List<String> actual;
                            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                                final StringSink rows = new StringSink();
                                println(factory.getMetadata(), cursor, rows);
                                actual = sortedRows(rows);
                            }
                            final Outcome reference = run(referenceSql(shape, checkedFilters[f]), header);
                            Assert.assertNull(header, reference.error);
                            Assert.assertEquals(header, reference.rows, actual);
                        }
                    }
                }
            }
        });
    }

    // checkReference() takes the failure of a reference for the failure of its filter only when the
    // filter fails with the same error on the table alone
    @Test
    public void testCheckOfFailedReference() {
        final String typeError = "there is no matching operator `between` with the argument type: FLOAT";
        final Outcome reference = new Outcome(null, typeError, 124);
        final List<String> failures = new ArrayList<>();
        checkReference("H", "S", reference, new Outcome(null, typeError, 35), failures);
        Assert.assertEquals(List.of(), failures);
        checkReference("H", "S", reference, new Outcome(List.of("10"), null, -1), failures);
        checkReference("H", "S", reference, new Outcome(null, "Invalid column: t.fl", 30), failures);
        Assert.assertEquals(
                List.of(
                        "H: the reference failed with [124] " + typeError + ", and the filter on the table alone returned 1 rows [10]\n    S",
                        "H: the reference failed with [124] " + typeError + ", and the filter on the table alone failed with [30] Invalid column: t.fl\n    S"
                ),
                failures
        );
    }

    // When the filter does not compile, a query fails with the error of the filter, or, while the
    // outer-ref join stays, with the rewriter's error at the RIGHT or FULL keyword. Mode.ANY takes
    // any error, but never rows.
    @Test
    public void testCheckOfQueryAfterFailedReference() {
        final Column column = column("FLOAT");
        final String template = "{C} BETWEEN 5.5 AND 15.5";
        final String typeError = "there is no matching operator `between` with the argument type: FLOAT";
        final Outcome reference = new Outcome(null, typeError, 124);
        for (Shape shape : Shape.values()) {
            final String sql = lateralSql(shape, Form.TABLE, column, template);
            final int keyword = sql.indexOf(shape.joinKeyword);
            final boolean isAnyErrorAccepted = shape.mode == Mode.ANY;
            assertCheck(shape, column, template, sql, reference, new Outcome(List.of(), null, -1), false);
            assertCheck(shape, column, template, sql, reference, new Outcome(null, typeError, 150), true);
            assertCheck(shape, column, template, sql, reference, new Outcome(null, OUTER_COLUMN_ERROR, keyword), shape.mode != Mode.WHERE_KEY);
            assertCheck(shape, column, template, sql, reference, new Outcome(null, OUTER_COLUMN_ERROR, keyword + 1), isAnyErrorAccepted);
            assertCheck(shape, column, template, sql, reference, new Outcome(null, "Invalid column: t.x", 117), isAnyErrorAccepted);
        }
    }

    // When the reference returns rows, a query returns the same rows, or fails with the rewriter's
    // error at the RIGHT keyword when the filter keeps the rows that the join NULL-extends, or with the
    // error that the plan raises at a bind variable that is NULL. Mode.ANY takes any error.
    @Test
    public void testCheckOfQueryAfterReferenceRows() {
        final Column column = column("INT");
        final Outcome matchedRows = new Outcome(List.of("1\t10\t100"), null, -1);
        final Outcome nullExtendedRows = new Outcome(List.of("1\tnull\t101"), null, -1);
        String sql = lateralSql(Shape.RIGHT_ON, Form.TABLE, column, "{C} = 10");
        final int keyword = sql.indexOf(Shape.RIGHT_ON.joinKeyword);
        final Outcome rewriterError = new Outcome(null, OUTER_COLUMN_ERROR, keyword);
        // a filter that drops the NULL-extended rows
        assertCheck(Shape.RIGHT_ON, column, "{C} = 10", sql, matchedRows, matchedRows, true);
        assertCheck(Shape.RIGHT_ON, column, "{C} = 10", sql, matchedRows, nullExtendedRows, false);
        assertCheck(Shape.RIGHT_ON, column, "{C} = 10", sql, matchedRows, rewriterError, false);
        // a filter that keeps them
        sql = lateralSql(Shape.RIGHT_ON, Form.TABLE, column, "{C} IS NULL");
        assertCheck(Shape.RIGHT_ON, column, "{C} IS NULL", sql, nullExtendedRows, nullExtendedRows, false);
        assertCheck(Shape.RIGHT_ON, column, "{C} IS NULL", sql, nullExtendedRows, rewriterError, true);
        assertCheck(Shape.RIGHT_ON, column, "{C} IS NULL", sql, nullExtendedRows, new Outcome(null, rewriterError.error, keyword + 1), false);
        // a bind variable value that is NULL
        sql = lateralSql(Shape.RIGHT_ON, Form.TABLE, column, "{C} >= $1");
        final String nullValueError = LateralNullCheckRecordCursorFactory.NULL_VALUE_ERROR;
        assertCheck(Shape.RIGHT_ON, column, "{C} >= $1", sql, nullExtendedRows, new Outcome(null, nullValueError, sql.indexOf("$1")), true);
        assertCheck(Shape.RIGHT_ON, column, "{C} >= $1", sql, nullExtendedRows, new Outcome(null, nullValueError, sql.indexOf("$1") + 1), false);
        // Mode.ANY
        sql = lateralSql(Shape.FULL_ON, Form.TABLE, column, "{C} = 10");
        assertCheck(Shape.FULL_ON, column, "{C} = 10", sql, matchedRows, new Outcome(null, "Invalid column: t.x", 117), true);
        assertCheck(Shape.FULL_ON, column, "{C} = 10", sql, matchedRows, nullExtendedRows, false);
    }

    // The engine throws only a SqlException for a query that it rejects. run() fails the test at once
    // on any other exception, instead of taking it for the error that a case expects.
    @Test
    public void testCrashFailsTheTest() throws Exception {
        // enables the npe() test function, which throws a NullPointerException
        node1.setProperty(PropertyKey.DEV_MODE_ENABLED, true);
        assertMemoryLeak(() -> {
            createTables();
            final AssertionError error = Assert.assertThrows(
                    AssertionError.class,
                    () -> assertTemplate(Shape.RIGHT_ON, column("INT"), "npe() AND {C} = 10", null, new ArrayList<>())
            );
            TestUtils.assertContains(error.getMessage(), "RIGHT_ON INT [npe() AND {C} = 10]: failed with java.lang.NullPointerException");
            TestUtils.assertContains(error.getMessage(), referenceSql(Shape.RIGHT_ON, "npe() AND t.i = 10"));
            Assert.assertTrue(error.getCause() instanceof NullPointerException);
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

    // A filter that runs on the table alone fails the reference, because the join finds id in both of
    // its tables. assertTemplate() reports it, instead of taking it for a filter that does not compile.
    @Test
    public void testReferenceThatTheJoinRejects() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final List<String> failures = new ArrayList<>();
            assertTemplate(Shape.RIGHT_ON, column("INT"), "id = 10", null, failures);
            final String sql = referenceSql(Shape.RIGHT_ON, "id = 10");
            // every query fails with the error of the reference, which the cases take for that of the filter
            Assert.assertEquals(
                    List.of(
                            "RIGHT_ON INT [id = 10]: the reference failed with [" + sql.indexOf("id = 10") + "] Ambiguous column [name=id], "
                                    + "and the filter on the table alone returned 1 rows [10]\n    " + sql
                    ),
                    failures
            );
        });
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

    private static void assertCheck(
            Shape shape,
            Column column,
            String template,
            String sql,
            Outcome reference,
            Outcome actual,
            boolean isAccepted
    ) {
        final List<String> failures = new ArrayList<>();
        checkCase(shape, Form.TABLE, column, template, "H", sql, reference, actual, failures);
        Assert.assertEquals(shape + " " + template + ", reference " + summary(reference) + ", query " + summary(actual) + ": " + failures, isAccepted ? 0 : 1, failures.size());
    }

    private static String[] cat(String[] a, String[] b) {
        final String[] r = Arrays.copyOf(a, a.length + b.length);
        System.arraycopy(b, 0, r, a.length, b.length);
        return r;
    }

    // When the filter does not compile, see checkReference(), the query fails with its error, or, while
    // the outer-ref join stays, with the rewriter's error, which the rewriter raises before the code
    // generator compiles the filter; Mode.ANY takes any error. Otherwise the query returns the rows of
    // the reference, or fails as expected() decides.
    private static void checkCase(
            Shape shape,
            Form form,
            Column column,
            String template,
            String header,
            String sql,
            Outcome reference,
            Outcome actual,
            List<String> failures
    ) {
        final int position = sql.indexOf(shape.joinKeyword);
        if (reference.error != null) {
            if (actual.error == null) {
                failures.add(header + ": returned " + summary(actual) + ", the reference failed with " + summary(reference) + "\n    " + sql);
            } else if (shape.mode != Mode.ANY
                    && !hasSameMessage(actual, reference)
                    && (shape.mode == Mode.WHERE_KEY || !isRewriterError(actual, position))) {
                failures.add(header + ": failed with " + summary(actual) + ", expected the error of the filter, " + summary(reference)
                        + (shape.mode == Mode.PINNED ? ", or [" + position + "] ... " + ERROR_SUFFIX : "") + "\n    " + sql);
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
        // ROWS_OR_ERROR takes any SqlException, see Mode.ANY
        final Expected expected = expected(shape, form, column, template, keepsNullExtendedRows);
        switch (expected) {
            case ROWS -> {
                if (actual.error != null) {
                    failures.add(header + ": failed with " + summary(actual) + ", expected " + summary(reference) + "\n    " + sql);
                }
            }
            case ERROR -> {
                if (actual.error == null) {
                    failures.add(header + ": returned " + summary(actual) + ", expected the rewriter to reject the filter\n    " + sql);
                } else if (!isRewriterError(actual, position) && !isNullValueError(actual, sql, template, column)) {
                    failures.add(header + ": failed with " + summary(actual) + ", expected [" + position + "] ... " + ERROR_SUFFIX + "\n    " + sql);
                }
            }
        }
    }

    // A reference fails only because its filter does not compile for the column type, so the filter
    // fails on the table alone with the same error. If it runs there, the join rejected a valid filter,
    // which checkCase() would take for the error of the filter.
    private static void checkReference(String header, String sql, Outcome reference, Outcome alone, List<String> failures) {
        if (alone.error == null || !hasSameMessage(alone, reference)) {
            failures.add(header + ": the reference failed with " + summary(reference) + ", and the filter on the table alone "
                    + (alone.error == null ? "returned " : "failed with ") + summary(alone) + "\n    " + sql);
        }
    }

    private static Column column(String type) {
        for (Column column : COLUMNS) {
            if (column.type.equals(type)) {
                return column;
            }
        }
        throw new AssertionError("no column of type " + type);
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
        if (!template.contains("$") && !template.contains("now()")) {
            return Expected.ROWS;
        }
        // the value of a bind variable or now() is known only when the query runs: < > and BETWEEN
        // are false for a NULL column whatever the value, and = <= and >= are while the value is
        // not NULL, which the plan checks once per execution
        return (isBetween(template) || LESS_OR_GREATER.matcher(template).matches() || RUNTIME_CHECKED.matcher(template).matches())
                && column.hasNull() ? Expected.ROWS : Expected.ERROR;
    }

    // the messages only, as the positions differ between the queries that share the filter
    private static boolean hasSameMessage(Outcome a, Outcome b) {
        return a.error.equals(b.error);
    }

    private static String instantiate(String template, String columnRef) {
        return template.replace("{C}", columnRef);
    }

    private static boolean isBetween(String template) {
        return template.contains(" BETWEEN ") && !template.contains(" NOT BETWEEN ");
    }

    // the error of a plan that checks, when the query runs, that a bind variable is not NULL
    private static boolean isNullValueError(Outcome outcome, String sql, String template, Column column) {
        return column.hasNull()
                && template.contains("$1")
                && RUNTIME_CHECKED.matcher(template).matches()
                && outcome.error.contains(NULL_VALUE_SUFFIX)
                && outcome.position == sql.indexOf("$1");
    }

    private static boolean isRewriterError(Outcome outcome, int position) {
        return outcome.error.contains(ERROR_SUFFIX) && outcome.position == position;
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

    // Returns the rows of the query, or the message and the position of the SqlException that rejects
    // it, copied, as SqlException reuses one instance per thread unless assertions are on. Any other
    // exception, such as a NullPointerException or an AssertionError of the engine, fails the test at
    // once: no case expects one, and a case that expects an error would take it for one.
    private static Outcome run(String sql, String header) {
        try {
            sink.clear();
            printSql(sql);
            return new Outcome(sortedRows(sink), null, -1);
        } catch (SqlException e) {
            return new Outcome(null, e.getFlyweightMessage().toString(), e.getPosition());
        } catch (Throwable e) {
            throw new AssertionError(header + ": failed with " + e + "\n    " + sql, e);
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
                case "date" ->
                        bindVariableService.setDate(i, isNull ? Numbers.LONG_NULL : isLow ? 1_672_531_200_000L : 1_735_689_600_000L);
                case "timestamp" ->
                        bindVariableService.setTimestamp(i, isNull ? Numbers.LONG_NULL : isLow ? 1_672_531_200_000_000L : 1_735_689_600_000_000L);
                case "timestamp_ns" ->
                        bindVariableService.setTimestampNano(i, isNull ? Numbers.LONG_NULL : isLow ? 1_672_531_200_000_000_000L : 1_735_689_600_000_000_000L);
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
        return "[" + outcome.position + "] " + outcome.error;
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
        final String header = shape + " " + form + " " + column.type + " [" + template + "]" + (bindValues != null ? " " + bindValues : "");
        checkCase(shape, form, column, template, header, sql, reference, run(sql, header), failures);
    }

    private void assertShape(Shape shape) throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final List<String> failures = new ArrayList<>();
            int caseCount = 0;
            for (Column column : COLUMNS) {
                for (String template : column.filters) {
                    caseCount += assertTemplate(shape, column, template, null, failures);
                }
                if (column.bindType != null) {
                    for (String template : column.bindFilters) {
                        for (BindValues values : BindValues.values()) {
                            // SHORT has no NULL
                            if (values != BindValues.NULL || !column.bindType.equals("short")) {
                                caseCount += assertTemplate(shape, column, template, values, failures);
                            }
                        }
                    }
                }
            }
            bindVariableService.clear();
            if (!failures.isEmpty()) {
                Assert.fail(failures.size() + " failures in " + caseCount + " cases:\n" + String.join("\n", failures.subList(0, Math.min(40, failures.size()))));
            }
        });
    }

    private int assertTemplate(
            Shape shape,
            Column column,
            String template,
            BindValues bindValues,
            List<String> failures
    ) throws SqlException {
        bindVariableService.clear();
        if (bindValues != null) {
            setBindVariables(column.bindType, bindValues);
        }
        final String filter = instantiate(template, "t." + column.name);
        final String sql = referenceSql(shape, filter);
        final String header = shape + " " + column.type + " [" + template + "]" + (bindValues != null ? " " + bindValues : "");
        final Outcome reference = run(sql, header);
        if (reference.error != null) {
            checkReference(header, sql, reference, run("SELECT id FROM trades t WHERE " + filter, header), failures);
        }
        // every form accepts every column type, so a form that stops accepting one fails its cases
        final Form[] forms = Form.values();
        for (Form form : forms) {
            assertCase(shape, form, column, template, bindValues, reference, failures);
        }
        return forms.length;
    }

    private enum BindValues {
        LOW, NULL, HIGH
    }

    private enum Expected {
        ROWS, ERROR, ROWS_OR_ERROR
    }

    // how the sub-query passes the column through; the rewriter knows the type of a column that a
    // sub-query renames, while only the code generator knows the type of one that it computes, that
    // a set operation may widen or that a join may take from any of its tables
    private enum Form {
        TABLE("", "trades", "t.{c}"),
        WILDCARD("", "(SELECT * FROM trades)", "t.{c}"),
        PROJECTION("", "(SELECT id, x, {c} FROM trades)", "t.{c}"),
        RENAME("", "(SELECT id, x, {c} AS renamed FROM trades)", "t.renamed"),
        QUALIFIED("", "(SELECT tr.id, tr.x, tr.{c} FROM trades tr)", "t.{c}"),
        NESTED("", "(SELECT * FROM (SELECT id, x, {c} FROM trades))", "t.{c}"),
        CTE("WITH tt AS (SELECT id, x, {c} FROM trades) ", "tt", "t.{c}"),
        GROUP_BY("", "(SELECT id, x, {c} FROM trades GROUP BY id, x, {c})", "t.{c}"),
        DISTINCT("", "(SELECT DISTINCT id, x, {c} FROM trades)", "t.{c}"),
        // the parser drops the alias of a column cast with ::GEOHASH(1c), so the form spells the cast out
        CAST("", "(SELECT id, x, CAST({c} AS {t}) {c} FROM trades)", "t.{c}"),
        UNION("", "(SELECT id, x, {c} FROM trades UNION ALL SELECT id, x, {c} FROM trades WHERE id < 0)", "t.{c}"),
        JOIN("", "(SELECT tr.id, tr.x, tr.{c} FROM trades tr CROSS JOIN long_sequence(1) u)", "t.{c}");

        final String columnRef;
        final String prefix;
        final String table;

        Form(String prefix, String table, String columnRef) {
            this.prefix = prefix;
            this.table = table;
            this.columnRef = columnRef;
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

    private record Column(String name, String type, String value, String fill, String[] filters, String bindType,
                          String[] bindFilters) {
        // BOOLEAN, BYTE and SHORT have no NULL: a RIGHT join puts false or 0 in them
        boolean hasNull() {
            return !type.equals("BOOLEAN") && !type.equals("BYTE") && !type.equals("SHORT");
        }
    }

    // the rows of a query, or the message and the position of the error that rejects it
    private record Outcome(List<String> rows, String error, int position) {
    }
}
