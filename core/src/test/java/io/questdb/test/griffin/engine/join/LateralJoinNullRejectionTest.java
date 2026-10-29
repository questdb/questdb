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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.TextPlanSink;
import io.questdb.std.Decimals;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.BindVarTuple;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * LateralJoinRewriter decorrelates a LATERAL body whose RIGHT or FULL join follows an ON clause that
 * reads the outer row by joining the outer row to the side of each such join that keeps its unmatched
 * rows, so that the rows the join NULL-extends on its master side carry their outer row. The tests
 * check the rows of the decorrelated query for every combination of the master column type, the
 * filter, which may keep or drop those rows, the sub-query form that passes the column through, and
 * the join shape.
 * <p>
 * Each query runs against a reference that evaluates the body once per outer row, with the outer key
 * as a literal, and combines the results with UNION ALL. A query must return the reference rows, under
 * every value of its bind variables and of its sub-queries.
 * <p>
 * A filter that does not compile for its column type, such as BETWEEN on a FLOAT, fails the
 * reference. The filter must then fail with the same error on the table alone, which shows that the
 * join did not reject a valid filter, and the query must fail with that error too. Any exception other
 * than a SqlException fails the test at once.
 * <p>
 * The data gives every outer row NULL-extended rows. Trade 10 holds a value in each typed column,
 * and trade 11 holds what the RIGHT join puts in NULL-extended rows: NULL, or 0 or false for BYTE,
 * SHORT and BOOLEAN. A filter therefore keeps NULL-extended rows exactly when the reference holds a
 * row with a NULL trade id.
 * <p>
 * Where a filter drops those rows only while a bind variable or a scalar sub-query is not NULL, one
 * plan serves every value, so the tests also assert that the plan keeps the RIGHT or FULL join and runs
 * the filter after it, and that the filter runs each of its sub-queries once.
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
    // over the data of createTables(), each outer row with its refund and trades 10 and 11
    private static final String BOTH_TRADES_PER_OUTER_ROW = """
            id\ttid\trid
            1\t10\t100
            1\t11\t100
            2\t10\t101
            2\t11\t101
            3\t10\t102
            3\t11\t102
            """;
    private static final String[] CHAR = {
            "{C} = 'a'", "'a' = {C}", "{C} != 'a'", "{C} < 'z'", "{C} > 'a'", "{C} <= 'z'", "{C} >= 'a'",
            "{C} >= 'b'", "{C} IN ('a', 'b')", "{C} IN ('b')", "{C} <= NULL", "{C} >= NULL", "{C} = ''",
            "{C} = NULL::char"
    };
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
    private static final String[] LATERAL_JOINS = {"JOIN LATERAL", "CROSS JOIN LATERAL", "LEFT JOIN LATERAL"};
    private static final String[] LONG256 = {"{C} = 0x01", "{C} != 0x01", "{C} < 0x05", "{C} > 0x00", "{C} >= 0x00", "{C} <= 0x05", "{C} IN (0x01)"};
    // Over the trades of createAnyConjunctTables(), passes i, l, sy, ip and v through, whose types the
    // rewriter knows, and computes sid, bid, cl, ci and ok, whose types only the code generator knows.
    // Trade 10 has sid 10 and ok true. Trade 11 has i NULL, sid 11, cl 10, ci NULL and ok true. A row
    // that a RIGHT join NULL-extends has sid 0, bid 0 and ok false, as SHORT, BYTE and BOOLEAN have no
    // NULL, and cl and ci NULL.
    private static final String MIXED_SELECT = "SELECT id, x, i, l, sy, ip, v, CAST(id AS SHORT) sid, CAST(id AS BYTE) bid,"
            + " CAST(l AS LONG) cl, CAST(i AS INT) ci, id > 0 AS ok FROM trades";
    private static final String MIXED_TABLE = "(" + MIXED_SELECT + ")";
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
    // over the data of createTables(), each outer row with the refunds of the other outer rows, which a
    // RIGHT join on r.k = o.k NULL-extends
    private static final String OTHER_REFUNDS_PER_OUTER_ROW = """
            id\ttid\trid
            1\tnull\t101
            1\tnull\t102
            2\tnull\t100
            2\tnull\t102
            3\tnull\t100
            3\tnull\t101
            """;
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
    // over the data of createTables(), each outer row with its refund and trade 10
    private static final String TRADE_10_PER_OUTER_ROW = """
            id\ttid\trid
            1\t10\t100
            2\t10\t101
            3\t10\t102
            """;
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

    // A cached plan runs with every value of its bind variables, so a filter with a bind variable must
    // return the rows of the reference for every value of the same compiled query, NULL included: < >
    // and BETWEEN on a type with NULL drop the NULL-extended rows whatever the value, while = <= and >=
    // keep them for a NULL value, as NULL = NULL, NULL <= NULL and NULL >= NULL are true.
    @Test
    public void testBindVariableValuesShareThePlan() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertBindVariableValuesShareThePlan(
                    new String[]{"t.i > $1", "t.i < $1", "$1 < t.i", "t.i BETWEEN $1 AND $2", "t.l > $1", "t.d < $1", "t.t2 > $1", "t.st > $1"},
                    new String[]{"int", "int", "int", "int", "long", "double", "timestamp", "str"}
            );
            assertBindVariableValuesShareThePlan(
                    new String[]{"t.i >= $1", "t.i <= $1", "$1 <= t.i", "t.i = $1", "t.l >= $1", "t.d <= $1", "t.t2 >= $1", "t.st >= $1", "t.sy = $1"},
                    new String[]{"int", "int", "int", "int", "long", "double", "timestamp", "str", "str"}
            );
            bindVariableService.clear();
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

    // When the filter does not compile, a query fails with the error of the filter, at a position of
    // its own
    @Test
    public void testCheckOfQueryAfterFailedReference() {
        final String typeError = "there is no matching operator `between` with the argument type: FLOAT";
        final Outcome reference = new Outcome(null, typeError, 124);
        for (Shape shape : Shape.values()) {
            final String sql = lateralSql(shape, Form.TABLE, column("FLOAT"), "{C} BETWEEN 5.5 AND 15.5");
            assertCheck(sql, reference, new Outcome(List.of(), null, -1), false);
            assertCheck(sql, reference, new Outcome(null, typeError, 150), true);
            assertCheck(sql, reference, new Outcome(null, "Invalid column: t.x", 117), false);
        }
    }

    // When the reference returns rows, a query returns the same rows, whether the filter keeps the rows
    // that the join NULL-extends or not, and does not fail
    @Test
    public void testCheckOfQueryAfterReferenceRows() {
        final Outcome matchedRows = new Outcome(List.of("1\t10\t100"), null, -1);
        final Outcome nullExtendedRows = new Outcome(List.of("1\tnull\t101"), null, -1);
        for (Shape shape : new Shape[]{Shape.RIGHT_ON, Shape.FULL_ON}) {
            final String sql = lateralSql(shape, Form.TABLE, column("INT"), "{C} = 10");
            assertCheck(sql, matchedRows, matchedRows, true);
            assertCheck(sql, matchedRows, nullExtendedRows, false);
            assertCheck(sql, matchedRows, new Outcome(List.of(), null, -1), false);
            assertCheck(sql, nullExtendedRows, nullExtendedRows, true);
            assertCheck(sql, nullExtendedRows, matchedRows, false);
            assertCheck(sql, nullExtendedRows, new Outcome(null, "Invalid column: t.x", 117), false);
        }
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

    // A parallel filter hands the crash of a worker to the caller as a CairoException, so run() must
    // fail the test on a CairoException too, not take it for the error of a query that the engine rejects
    @Test
    public void testCrashInParallelFilterFailsTheTest() throws Exception {
        // enables the npe() test function, which throws a NullPointerException
        node1.setProperty(PropertyKey.DEV_MODE_ENABLED, true);
        assertMemoryLeak(() -> {
            createTables();
            final String sql = "SELECT id FROM trades WHERE npe()";
            assertQuery(sql).noLeakCheck().assertsPlanContaining("Async Filter");
            final AssertionError error = Assert.assertThrows(AssertionError.class, () -> run(sql, "H"));
            TestUtils.assertContains(error.getMessage(), "H: failed with " + CairoException.class.getName());
            Assert.assertTrue(error.getCause() instanceof CairoException);
        });
    }

    @Test
    public void testFullJoinAfterCorrelatedInnerJoin() throws Exception {
        assertShape(Shape.INNER_ON_FULL);
    }

    // The FULL join also NULL-extends the trades that match no refund. They keep their outer row, so the
    // filters that decide for a RIGHT join decide for a FULL join too.
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

    // With a correlated sub-query as the slave of the RIGHT join, WHERE t.x = o.k lets the rewriter
    // key the rows by t.x. Per outer row, outer key 1 matches refund 100 and trade 10, key 2 refund
    // 101 and trade 11, whose ci NULL != 0 is true, and key 3 refund 102, which no trade matches, and
    // t.x = 3 drops the NULL-extended row.
    @Test
    public void testRightJoinRepairNextToAComputedColumn() throws Exception {
        assertMemoryLeak(() -> {
            createAnyConjunctTables();
            final String body = MIXED_TABLE + " t RIGHT JOIN (SELECT id, k FROM refunds WHERE k = {K}) r ON t.x = r.k"
                    + " WHERE t.x = {K} AND t.ci != 0";
            // no bind variables
            assertSteps(body, rowsStep(bindVariableService::clear, "1\t10\t100\n2\t11\t101"));
            assertQuery(lateralSql(body)).noLeakCheck().assertsPlanContaining("condition: l.x=o.k");
        });
    }

    // A scalar sub-query nested in a filter, below a CASE, takes one value per execution, which decides
    // per execution whether the filter keeps the rows that the RIGHT join NULL-extends, as NULL >= NULL
    // and NULL = NULL are true. Per outer row, refund k matches both trades, so the CASE value 0 keeps
    // the six rows of both trades, and 10 with = the three of trade 10, while a NULL value keeps the
    // two refunds per outer row that no trade matches. The sub-queries make the CASE NULL in some
    // executions and not in others, and each execution returns the rows of its value. The CASE reads
    // its sub-queries through comparisons and AND, over INT and over a BOOLEAN sub-query, through an IN
    // over a STRING sub-query, and through a BETWEEN over two TIMESTAMP sub-queries, over the table and
    // over a UNION ALL of it.
    @Test
    public void testRightJoinWithANestedRandomSubQuery() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String random = "(SELECT rnd_int(0, 1, 0) FROM long_sequence(1))";
            // the filter, the filter with the CASE value when it is not NULL, its rows, the filter with
            // the NULL value of the CASE, and the number of sub-queries that the filter runs
            final String[][] filters = {
                    {"t.id >= CASE WHEN " + random + " = 0 THEN NULL::int ELSE 0 END", "t.id >= 0", BOTH_TRADES_PER_OUTER_ROW, "t.id >= NULL::int", "1"},
                    {"t.id = CASE WHEN " + random + " = 0 THEN NULL::int ELSE 10 END", "t.id = 10", TRADE_10_PER_OUTER_ROW, "t.id = NULL::int", "1"},
                    {
                            "t.id >= CASE WHEN (SELECT rnd_int(0, 1, 0) = 0 FROM long_sequence(1)) AND true THEN NULL::int ELSE 0 END",
                            "t.id >= 0",
                            BOTH_TRADES_PER_OUTER_ROW,
                            "t.id >= NULL::int",
                            "1"
                    },
                    {"t.id >= CASE WHEN " + random + " = 0 AND " + random + " = 0 THEN NULL::int ELSE 0 END", "t.id >= 0", BOTH_TRADES_PER_OUTER_ROW, "t.id >= NULL::int", "2"},
                    {
                            "t.id >= CASE WHEN '0'::symbol IN (SELECT rnd_int(0, 1, 0)::string FROM long_sequence(1)) THEN NULL::int ELSE 0 END",
                            "t.id >= 0",
                            BOTH_TRADES_PER_OUTER_ROW,
                            "t.id >= NULL::int",
                            "1"
                    },
                    // the refunds have the timestamps 1 to 3, so 1 is between the bounds while the
                    // lower bound is 0, not 2; one function reads both sub-queries
                    {
                            "t.ts >= CASE WHEN 1::timestamp BETWEEN (SELECT CASE WHEN rnd_int(0, 1, 0) = 0 THEN 2::timestamp ELSE 0::timestamp END"
                                    + " FROM long_sequence(1)) AND (SELECT max(ts) FROM refunds) THEN 1::timestamp END",
                            "t.ts >= 1::timestamp",
                            BOTH_TRADES_PER_OUTER_ROW,
                            "t.ts >= NULL::timestamp",
                            "2"
                    }
            };
            final String[] tables = {"trades", "(SELECT id, x, ts FROM trades UNION ALL SELECT id, x, ts FROM trades WHERE id < 0)"};
            final Rnd rnd = TestUtils.generateRandom(LOG);
            for (String table : tables) {
                for (String[] filter : filters) {
                    final String body = table + " t RIGHT JOIN refunds r ON r.k = {K} WHERE " + filter[0];
                    assertReference("", "trades t RIGHT JOIN refunds r ON r.k = {K} WHERE " + filter[1], filter[2]);
                    assertReference("", "trades t RIGHT JOIN refunds r ON r.k = {K} WHERE " + filter[3], OTHER_REFUNDS_PER_OUTER_ROW);
                    assertRowsOfEitherValue(lateralSql(body), filter[2], OTHER_REFUNDS_PER_OUTER_ROW, rnd);
                    // the plan keeps the RIGHT join, and the filter after it runs each sub-query once
                    final String plan = planOf(lateralSql(body));
                    TestUtils.assertContains(plan, "Nested Loop Right Join");
                    Assert.assertEquals(plan, Integer.parseInt(filter[4]), countOf(plan, "cursor"));
                }
            }
        });
    }

    @Test
    public void testRightJoinWithCorrelatedOn() throws Exception {
        assertShape(Shape.RIGHT_ON);
    }

    // A filter on refunds, which the RIGHT join keeps whole, keeps the rows that the join NULL-extends on
    // trades, with a comparison against a constant or a bind variable, and with IN next to WHERE t.x = o.k,
    // which lets the rewriter key the rows by t.x and drops the NULL-extended rows. Per outer row,
    // refunds 101 and 102 match no trade for outer key 1, and r.id > 0 keeps them.
    @Test
    public void testRightJoinWithCorrelatedOnAndRefundsFilter() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String rows = """
                    id\ttid\trid
                    1\tnull\t101
                    1\tnull\t102
                    1\t10\t100
                    2\tnull\t100
                    2\tnull\t102
                    2\t11\t101
                    3\tnull\t100
                    3\tnull\t101
                    3\tnull\t102
                    """;
            assertReference("", Shape.RIGHT_ON.body.replace("{T}", "trades").replace("{P}", "r.id > 0"), rows);
            final String body = "SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (SELECT t.id tid, r.id rid "
                    + "FROM trades t RIGHT JOIN refunds r ON t.x = r.k AND r.k = o.k WHERE ";
            assertQuery(body + "r.id > 0) l ORDER BY 1, 2, 3").noLeakCheck().sizeMayVary().returns(rows);
            assertQuery(body + "t.x = o.k AND r.k IN (1, 2)) l ORDER BY 1, 2, 3").noLeakCheck().sizeMayVary().returns("""
                    id\ttid\trid
                    1\t10\t100
                    2\t11\t101
                    """);
            assertReference("", Shape.RIGHT_ON.body.replace("{T}", "trades").replace("{P}", "t.x = {K} AND r.k IN (1, 2)"), """
                    id\ttid\trid
                    1\t10\t100
                    2\t11\t101
                    """);
            bindVariableService.clear();
            bindVariableService.setInt(0, 0);
            assertQuery(body + "r.id >= $1) l ORDER BY 1, 2, 3").noLeakCheck().sizeMayVary().returns(rows);
            bindVariableService.clear();
        });
    }

    // WHERE t.x = o.k lets the rewriter key the rows by t.x, and drops the rows that the RIGHT join
    // NULL-extends, whose t.x is NULL, whatever the other conjunct. Outer key 1 matches trade 10, key 2
    // trade 11, which holds NULLs, and key 3 no trade. A NULL value makes < and > false, and keeps
    // trade 11 for <= and >=, as NULL >= NULL is true.
    @Test
    public void testRightJoinWithCorrelatedOnAndWhereKey() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String body = "SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (SELECT t.id tid, r.id rid "
                    + "FROM trades t RIGHT JOIN refunds r ON t.x = r.k AND r.k = o.k WHERE t.x = o.k AND ";
            final String trade10 = """
                    id\ttid\trid
                    1\t10\t100
                    """;
            final String noRows = "id\ttid\trid\n";

            assertQuery(body + "t.l BETWEEN 1 AND 10) l").noLeakCheck().noRandomAccess().expectSize().returns(trade10);
            assertQuery(body + "t.st IN ('abc')) l").noLeakCheck().noRandomAccess().expectSize().returns(trade10);
            assertQuery(body + "t.g4 = #u33d) l").noLeakCheck().noRandomAccess().expectSize().returns(trade10);

            bindVariableService.clear();
            bindVariableService.setLong(0, 5);
            assertQuery(body + "t.l > $1) l").noLeakCheck().noRandomAccess().expectSize().returns(trade10);
            bindVariableService.setLong(0, Numbers.LONG_NULL);
            assertQuery(body + "t.l > $1) l").noLeakCheck().noRandomAccess().expectSize().returns(noRows);

            bindVariableService.clear();
            bindVariableService.setInt(0, 0);
            assertQuery(body + "t.i >= $1) l").noLeakCheck().noRandomAccess().expectSize().returns(trade10);
            bindVariableService.setInt(0, Numbers.INT_NULL);
            assertQuery(body + "t.i >= $1) l").noLeakCheck().noRandomAccess().expectSize().returns("""
                    id\ttid\trid
                    2\t11\t101
                    """);
            assertReference("", "trades t RIGHT JOIN refunds r ON t.x = r.k AND r.k = {K} WHERE t.x = {K} AND t.i >= NULL::int", """
                    id\ttid\trid
                    2\t11\t101
                    """);
            bindVariableService.clear();

            assertQuery(body + "t.d <= (SELECT max(d) FROM trades)) l").noLeakCheck().noRandomAccess().expectSize().returns(trade10);
            assertQuery(body + "t.d < (SELECT max(d) FROM trades WHERE id = 99)) l").noLeakCheck().noRandomAccess().expectSize().returns(noRows);
        });
    }

    // WHERE t.x = o.k lets the rewriter key the rows by t.x and remove the outer-ref join
    @Test
    public void testRightJoinWithCorrelatedWhereKey() throws Exception {
        assertShape(Shape.WHERE_KEY_RIGHT);
    }

    // Two RIGHT joins over a UNION ALL table, whose columns the rewriter does not type: max(l) is 10, so
    // t.l >= 10 drops the rows that both joins NULL-extend on trades, and r.id >= NULL then drops every
    // row. In a chain over the mixed table, the join with xs has refunds alone on its master side, and
    // the join with ys reads t.cl, a computed column. min(id) is 100, and t.cl >= NULL drops every row.
    // Per outer row, outer key 1 matches refund 100 and trade 10, key 2 refund 101 and trade 11, and
    // key 3 refund 102, which no trade matches.
    @Test
    public void testRightJoinsWithANullValueNextToAComputedColumn() throws Exception {
        assertMemoryLeak(() -> {
            createAnyConjunctTables();
            final String union = "(SELECT id, x, i, l FROM trades UNION ALL SELECT id, x, i, l FROM trades WHERE id < 0)";
            final String twoRightJoins = union + " t RIGHT JOIN refunds r ON r.k = {K} RIGHT JOIN xs x ON x.k = r.k"
                    + " WHERE r.id >= $1 AND t.l >= (SELECT max(l) FROM trades)";
            assertSteps(
                    twoRightJoins,
                    rowsStep(() -> bindVariableService.setInt(0, Numbers.INT_NULL), ""),
                    rowsStep(() -> bindVariableService.setInt(0, 101), "2\t10\t101\n2\t11\t101\n3\t10\t102\n3\t11\t102")
            );
            // the plan keeps both RIGHT joins, and the filter after them runs the sub-query
            assertQuery(lateralSql(twoRightJoins)).noLeakCheck().assertsPlanContaining(
                    "Filter filter: (t.l [thread-safe] >= cursor",
                    "Frame forward scan on: trades and r.id>=$0::int)",
                    "Hash Right Outer Join",
                    "Nested Loop Right Join"
            );

            execute("CREATE TABLE ys (k INT)");
            execute("INSERT INTO ys VALUES (1), (2), (3)");
            final String chain = "refunds r RIGHT JOIN xs x ON x.k = r.k AND r.k = {K} JOIN " + MIXED_TABLE + " t ON t.x = x.k"
                    + " RIGHT JOIN ys y ON y.k = t.x WHERE r.id >= (SELECT min(id) FROM refunds) AND t.cl >= $1";
            assertSteps(
                    chain,
                    rowsStep(() -> bindVariableService.setLong(0, 10), "1\t10\t100\n2\t11\t101"),
                    rowsStep(() -> bindVariableService.setLong(0, Numbers.LONG_NULL), "")
            );
            assertQuery(lateralSql(chain)).noLeakCheck().assertsPlanContaining(
                    "Filter filter: (r.id [thread-safe] >= cursor",
                    "Frame forward scan on: refunds and t.cl>=$0::long)"
            );
            // both RIGHT joins, and one run of the sub-query
            final String chainPlan = planOf(lateralSql(chain));
            Assert.assertEquals(chainPlan, 2, countOf(chainPlan, "Hash Right Outer Join"));
            Assert.assertEquals(chainPlan, 1, countOf(chainPlan, "cursor"));
            bindVariableService.clear();
        });
    }

    // Over a sub-query column of t, whose type the rewriter does not know, both RIGHT joins read
    // t.id >= (SELECT ...), whose sub-query is NULL in some executions and 0 in others, so each
    // execution returns the rows of the filter with 0 or with NULL. Per outer row, refund k matches both
    // trades, and t.id >= 0 drops the rows that both joins NULL-extend, while t.id >= NULL keeps them:
    // the refunds of the other outer rows.
    @Test
    public void testRightJoinsWithOneRandomSubQuery() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String subQuery = "(SELECT CASE WHEN rnd_int(0, 1, 0) = 0 THEN NULL::int ELSE 0 END FROM long_sequence(1))";
            final String body = "(SELECT id, x FROM trades UNION ALL SELECT id, x FROM trades WHERE id < 0) t"
                    + " RIGHT JOIN refunds r ON r.k = {K} RIGHT JOIN xs x ON x.k = r.k WHERE t.id >= " + subQuery;
            assertReference("", body.replace(subQuery, "0"), BOTH_TRADES_PER_OUTER_ROW);
            assertReference("", body.replace(subQuery, "NULL::int"), OTHER_REFUNDS_PER_OUTER_ROW);
            assertRowsOfEitherValue(lateralSql(body), BOTH_TRADES_PER_OUTER_ROW, OTHER_REFUNDS_PER_OUTER_ROW, TestUtils.generateRandom(LOG));
            final String sql = lateralSql(body);
            final String plan = planOf(sql);
            // the plan keeps both RIGHT joins, and the filter after them runs the sub-query once
            TestUtils.assertContains(plan, "Filter filter: t.id [thread-safe] >= cursor");
            TestUtils.assertContains(plan, "Hash Right Outer Join");
            TestUtils.assertContains(plan, "Nested Loop Right Join");
            Assert.assertEquals(plan, 1, countOf(plan, "cursor"));
        });
    }

    // Both RIGHT joins read t.id >= (SELECT ...), whose sub-query is NULL in some executions and 0 in
    // others, next to r.id >= $1, so each execution returns the rows of the filter with 0 or with NULL,
    // over the table and over a sub-query column of t, whose type the rewriter does not know. Per outer
    // row, refund k matches both trades, and r.id >= 100 keeps every refund: t.id >= 0 drops the rows
    // that the joins NULL-extend, while t.id >= NULL keeps them, the refunds of the other outer rows.
    @Test
    public void testRightJoinsShareARandomSubQuery() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String subQuery = "(SELECT CASE WHEN rnd_int(0, 1, 0) = 0 THEN NULL::int ELSE 0 END FROM long_sequence(1))";
            final String[] tables = {"trades", "(SELECT id, x FROM trades UNION ALL SELECT id, x FROM trades WHERE id < 0)"};
            final Rnd rnd = TestUtils.generateRandom(LOG);
            for (String table : tables) {
                final String body = table + " t RIGHT JOIN refunds r ON r.k = {K} RIGHT JOIN xs x ON x.k = r.k WHERE t.id >= " + subQuery + " AND r.id >= $1";
                bindVariableService.clear();
                bindVariableService.setInt(0, 100);
                assertReference("", body.replace(subQuery, "0"), BOTH_TRADES_PER_OUTER_ROW);
                assertReference("", body.replace(subQuery, "NULL::int"), OTHER_REFUNDS_PER_OUTER_ROW);
                assertRowsOfEitherValue(lateralSql(body), BOTH_TRADES_PER_OUTER_ROW, OTHER_REFUNDS_PER_OUTER_ROW, rnd);
                final String plan = planOf(lateralSql(body));
                // the plan keeps both RIGHT joins, and the filter after them runs the sub-query once
                TestUtils.assertContains(plan, "Filter filter: (t.id [thread-safe] >= cursor");
                TestUtils.assertContains(plan, "Hash Right Outer Join");
                TestUtils.assertContains(plan, "Nested Loop Right Join");
                Assert.assertEquals(plan, 1, countOf(plan, "cursor"));
            }
            bindVariableService.clear();
        });
    }

    // Two RIGHT joins with a conjunct on each side of the first one, over scalar sub-queries, nested
    // sub-queries and bind variables. Per outer row, only outer key 3 matches refund 102, the largest
    // id, which matches both trades, the smallest of them 10. A filter that read the value of the other
    // conjunct, r.id >= 10 or t.id >= 102, would return refunds 100 and 101 too, or no row. With
    // r.id >= $1 and $1 NULL, r.id >= NULL drops every row; with $2 NULL, t.id >= NULL keeps refund 102
    // for outer keys 1 and 2, which the first RIGHT join NULL-extends there.
    @Test
    public void testRightJoinsWithTwoNullableConjuncts() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String body = "trades t RIGHT JOIN refunds r ON r.k = {K} RIGHT JOIN xs x ON x.k = r.k WHERE ";
            final String key3 = """
                    id\ttid\trid
                    3\t10\t102
                    3\t11\t102
                    """;

            // the plan of each filter keeps both RIGHT joins, and runs both conjuncts after them
            final String subQueries = body + "(SELECT max(id) FROM refunds) <= r.id AND t.id >= (SELECT min(id) FROM trades)";
            assertReference("", subQueries, key3);
            assertQuery(lateralSql(subQueries)).noLeakCheck()
                    .withPlanContaining(
                            "Filter filter: (t.id [thread-safe] >= cursor",
                            "Frame forward scan on: trades and r.id [thread-safe] >= cursor",
                            "Frame forward scan on: refunds)",
                            "Hash Right Outer Join",
                            "Nested Loop Right Join"
                    )
                    .expectSize()
                    .returns(key3);

            // sub-queries of different column types that look alike
            final String timestampSubQuery = body + "r.ts >= (SELECT max(ts) FROM refunds) AND t.id >= (SELECT min(id) FROM trades)";
            assertReference("", timestampSubQuery, key3);
            assertQuery(lateralSql(timestampSubQuery)).noLeakCheck()
                    .withPlanContaining(
                            "Filter filter: (t.id [thread-safe] >= cursor",
                            "Frame forward scan on: trades and r.ts [thread-safe] >= cursor",
                            "Frame backward scan on: refunds)",
                            "Hash Right Outer Join",
                            "Nested Loop Right Join"
                    )
                    .expectSize()
                    .returns(key3);

            // sub-queries nested in a CASE
            final String nestedSubQueries = body + "r.id >= CASE WHEN (SELECT max(id) FROM refunds) > 0 THEN 102 END"
                    + " AND t.id >= CASE WHEN (SELECT min(id) FROM trades) > 0 THEN 10 END";
            assertReference("", nestedSubQueries, key3);
            assertQuery(lateralSql(nestedSubQueries)).noLeakCheck()
                    .withPlanContaining(
                            "Filter filter: (t.id>=case([0 [thread-safe] < cursor",
                            "Frame forward scan on: trades,10]) and r.id>=case([0 [thread-safe] < cursor",
                            "Frame forward scan on: refunds,102]))",
                            "Hash Right Outer Join",
                            "Nested Loop Right Join"
                    )
                    .expectSize()
                    .returns(key3);

            final String bindVariables = body + "r.id >= $1 AND t.id >= $2";
            bindVariableService.clear();
            bindVariableService.setInt(0, 102);
            bindVariableService.setInt(1, 10);
            assertReference("", bindVariables, key3);
            assertQuery(lateralSql(bindVariables)).noLeakCheck()
                    .withPlanContaining("Filter filter: (t.id>=$1::int and r.id>=$0::int)", "Hash Right Outer Join", "Nested Loop Right Join")
                    .expectSize()
                    .returns(key3);
            bindVariableService.setInt(0, Numbers.INT_NULL);
            final String noRows = "id\ttid\trid\n";
            assertReference("", bindVariables, noRows);
            assertQuery(lateralSql(bindVariables)).noLeakCheck().expectSize().returns(noRows);
            bindVariableService.setInt(0, 102);
            bindVariableService.setInt(1, Numbers.INT_NULL);
            final String refund102 = """
                    id\ttid\trid
                    1\tnull\t102
                    2\tnull\t102
                    """;
            assertReference("", bindVariables, refund102);
            assertQuery(lateralSql(bindVariables)).noLeakCheck().expectSize().returns(refund102);
            bindVariableService.setInt(0, Numbers.INT_NULL);
            assertReference("", bindVariables, noRows);
            assertQuery(lateralSql(bindVariables)).noLeakCheck().expectSize().returns(noRows);

            final String oneConjunct = body + "t.id >= $1";
            bindVariableService.clear();
            bindVariableService.setInt(0, 10);
            final String allKeys = """
                    id\ttid\trid
                    1\t10\t100
                    1\t11\t100
                    2\t10\t101
                    2\t11\t101
                    3\t10\t102
                    3\t11\t102
                    """;
            assertReference("", oneConjunct, allKeys);
            assertQuery(lateralSql(oneConjunct)).noLeakCheck()
                    .withPlanContaining("Filter filter: t.id>=$0::int", "Hash Right Outer Join", "Nested Loop Right Join")
                    .expectSize()
                    .returns(allKeys);
            bindVariableService.clear();
        });
    }

    // A NULL value next to a conjunct on a computed column of MIXED_TABLE, whose type the rewriter does
    // not know. != 0 and != false are false on the 0 and false that the RIGHT join puts in SHORT, BYTE
    // and BOOLEAN, and so is > 5 on the NULL of LONG, so those conjuncts drop the rows that the join
    // NULL-extends, and t.i = NULL keeps trade 11, as NULL = NULL is true. With t.cl >= $2 or a
    // sub-query that is NULL too, NULL >= NULL keeps the NULL-extended rows. On INT, NULL != 0 is
    // true, and BYTE has no NULL, so neither t.ci != 0 nor t.bid >= $2 drops them. One compiled query
    // runs with each value.
    @Test
    public void testNullValueNextToAComputedColumn() throws Exception {
        assertMemoryLeak(() -> {
            createAnyConjunctTables();
            final String body = MIXED_TABLE + " t RIGHT JOIN refunds r ON t.x = r.k AND r.k = {K} WHERE ";
            for (String computed : new String[]{"t.sid != 0", "t.bid != 0", "t.ok != false", "t.cl > 5"}) {
                final String filter = body + "t.i = $1 AND " + computed;
                assertSteps(
                        filter,
                        rowsStep(() -> bindVariableService.setInt(0, Numbers.INT_NULL), "2\t11\t101"),
                        // the NULL of INT
                        rowsStep(() -> bindVariableService.setInt(0, -2_147_483_648), "2\t11\t101"),
                        rowsStep(() -> bindVariableService.setInt(0, 10), "1\t10\t100"),
                        rowsStep(() -> bindVariableService.setInt(0, 0), "")
                );
            }

            final String twoNullableConjuncts = body + "t.i = $1 AND t.cl >= $2";
            assertSteps(
                    twoNullableConjuncts,
                    rowsStep(() -> bindIntLong(Numbers.INT_NULL, 10), "2\t11\t101"),
                    rowsStep(() -> bindIntLong(10, Numbers.LONG_NULL), ""),
                    referenceStep(() -> bindIntLong(Numbers.INT_NULL, Numbers.LONG_NULL)),
                    rowsStep(() -> bindIntLong(10, 10), "1\t10\t100")
            );
            // the plan keeps the RIGHT join and runs the filter after it
            assertQuery(lateralSql(twoNullableConjuncts)).noLeakCheck().assertsPlanContaining(
                    "Filter filter: (t.i=$0::int and t.cl>=$1::long)",
                    "Hash Right Outer Join"
            );
            final String reversedConjuncts = body + "t.cl >= $2 AND t.i = $1";
            // the step with both values NULL first, so that assertSteps() also compiles the query with them
            assertSteps(
                    reversedConjuncts,
                    referenceStep(() -> bindIntLong(Numbers.INT_NULL, Numbers.LONG_NULL)),
                    rowsStep(() -> bindIntLong(Numbers.INT_NULL, 10), "2\t11\t101")
            );
            assertQuery(lateralSql(reversedConjuncts)).noLeakCheck().assertsPlanContaining(
                    "Filter filter: (t.cl>=$1::long and t.i=$0::int)",
                    "Hash Right Outer Join"
            );

            assertSteps(
                    body + "t.i = $1 AND t.cl >= (SELECT max(l) FROM trades)",
                    rowsStep(() -> bindVariableService.setInt(0, Numbers.INT_NULL), "2\t11\t101"),
                    rowsStep(() -> bindVariableService.setInt(0, 10), "1\t10\t100")
            );
            assertSteps(
                    body + "t.i = $1 AND t.cl >= (SELECT max(l) FROM trades WHERE id < 0)",
                    referenceStep(() -> bindVariableService.setInt(0, Numbers.INT_NULL)),
                    rowsStep(() -> bindVariableService.setInt(0, 10), "")
            );

            final String intColumn = body + "t.i = $1 AND t.ci != 0";
            assertSteps(
                    intColumn,
                    referenceStep(() -> bindVariableService.setInt(0, Numbers.INT_NULL)),
                    rowsStep(() -> bindVariableService.setInt(0, 10), "1\t10\t100")
            );
            assertQuery(lateralSql(intColumn)).noLeakCheck().assertsPlanContaining("Filter filter: (t.i=$0::int and t.ci!=0)", "Hash Right Outer Join");
            final String byteColumn = body + "t.i = $1 AND t.bid >= $2";
            assertSteps(
                    byteColumn,
                    referenceStep(() -> bindIntLong(Numbers.INT_NULL, 0)),
                    rowsStep(() -> bindIntLong(10, 0), "1\t10\t100")
            );
            assertQuery(lateralSql(byteColumn)).noLeakCheck().assertsPlanContaining("Filter filter: (t.i=$0::int and t.bid>=$1::long)", "Hash Right Outer Join");

            // each type of the column of known type, next to a computed SHORT column
            assertSteps(
                    body + "t.sy = $1 AND t.sid != 0",
                    rowsStep(() -> bindVariableService.setStr(0, null), "2\t11\t101"),
                    rowsStep(() -> bindVariableService.setStr(0, "abc"), "1\t10\t100")
            );
            assertSteps(
                    body + "t.ip = $1 AND t.sid != 0",
                    rowsStep(() -> bindVariableService.setStr(0, "0.0.0.0"), "2\t11\t101"),
                    rowsStep(() -> bindVariableService.setStr(0, "1.1.1.1"), "1\t10\t100")
            );
            assertSteps(
                    body + "t.v = $1 AND t.sid != 0",
                    rowsStep(() -> bindVariableService.setStr(0, null), "2\t11\t101"),
                    rowsStep(() -> bindVariableService.setStr(0, ""), "")
            );
            bindVariableService.clear();
        });
    }

    // A compiled query reads new data: the sub-query is NULL while no trade has x = 3, so with $1 NULL
    // the filter keeps the rows that the RIGHT join NULL-extends. Trade 12 makes it 10, which drops
    // them, and its i, 5, does not equal NULL. TRUNCATE makes the sub-query NULL again.
    @Test
    public void testNullValueNextToAComputedColumnAfterDataChange() throws Exception {
        assertNullValueAfterDataChange(MIXED_TABLE, "t.cl");
    }

    // A NULL value next to a conjunct on a computed column, in each place of the body: a later INNER
    // join's ON clause, a WHERE that lets the rewriter key the rows by t.x, also next to a correlated
    // sub-query, and in each LATERAL join and a CTE.
    @Test
    public void testNullValueNextToAComputedColumnOnEachPath() throws Exception {
        assertMemoryLeak(() -> {
            createAnyConjunctTables();
            // the body, and its rows when $1 is NULL and when it is 10
            final String[][] shapes = {
                    {"{T} t RIGHT JOIN refunds r ON t.x = r.k AND r.k = {K} WHERE t.i = $1 AND t.sid != 0", "2\t11\t101", "1\t10\t100"},
                    {
                            "{T} t RIGHT JOIN refunds r ON r.k = {K} WHERE t.i = $1 AND t.sid != 0",
                            "1\t11\t100\n2\t11\t101\n3\t11\t102",
                            "1\t10\t100\n2\t10\t101\n3\t10\t102"
                    },
                    {
                            "{T} t JOIN xs x ON x.k = {K} RIGHT JOIN refunds r ON t.x = r.k WHERE t.i = $1 AND t.sid != 0",
                            "1\t11\t101\n2\t11\t101\n3\t11\t101",
                            "1\t10\t100\n2\t10\t100\n3\t10\t100"
                    },
                    {
                            "{T} t JOIN xs x ON x.k = {K} FULL JOIN refunds r ON t.x = r.k WHERE t.i = $1 AND t.sid != 0",
                            "1\t11\t101\n2\t11\t101\n3\t11\t101",
                            "1\t10\t100\n2\t10\t100\n3\t10\t100"
                    },
                    {
                            "{T} t RIGHT JOIN refunds r ON t.x = r.k AND r.k = {K} JOIN xs x ON x.k = r.k AND t.i >= $1 WHERE t.sid != 0",
                            "2\t11\t101",
                            "1\t10\t100"
                    },
                    {
                            "{T} t RIGHT JOIN refunds r ON t.x = r.k AND r.k = {K} WHERE t.x = {K} AND t.i <= $1 AND t.sid != 0",
                            "2\t11\t101",
                            "1\t10\t100"
                    },
                    // a correlated sub-query as the slave of a FULL join with a RIGHT join after it
                    {
                            "{T} t FULL JOIN (SELECT id, k FROM refunds WHERE k = {K}) r ON t.x = r.k RIGHT JOIN xs x ON x.k = t.x"
                                    + " WHERE t.x = {K} AND t.i <= $1 AND t.sid != 0",
                            "2\t11\t101",
                            "1\t10\t100"
                    }
            };
            // the CTE prefix, then the table
            final String[][] tables = {{"", MIXED_TABLE}, {"WITH mt AS (" + MIXED_SELECT + ") ", "mt"}};
            for (String lateralJoin : LATERAL_JOINS) {
                for (String[] table : tables) {
                    for (String[] shape : shapes) {
                        final String body = shape[0].replace("{T}", table[1]);
                        assertSteps(
                                table[0],
                                lateralJoin,
                                body,
                                body,
                                rowsStep(() -> bindVariableService.setInt(0, Numbers.INT_NULL), shape[1]),
                                rowsStep(() -> bindVariableService.setInt(0, 10), shape[2])
                        );
                    }
                    // a correlated sub-query as the slave of the FULL join: the FULL join takes the outer
                    // value from either of its sides, which the plan matches to the outer row in a filter
                    // after the lateral join, so the cursor does not know its size
                    final String fullJoin = table[1] + " t FULL JOIN (SELECT id, k FROM refunds WHERE k = {K}) r ON t.x = r.k"
                            + " WHERE t.x = {K} AND t.i <= $1 AND t.sid != 0";
                    assertSteps(
                            table[0],
                            lateralJoin,
                            fullJoin,
                            fullJoin,
                            false,
                            rowsStep(() -> bindVariableService.setInt(0, Numbers.INT_NULL), "2\t11\t101"),
                            rowsStep(() -> bindVariableService.setInt(0, 10), "1\t10\t100")
                    );

                    final String innerOn = table[1] + " t RIGHT JOIN refunds r ON t.x = r.k AND r.k = {K}"
                            + " JOIN xs x ON x.k = r.k AND t.i >= $1 WHERE t.cl >= $2";
                    assertSteps(
                            table[0],
                            lateralJoin,
                            innerOn,
                            innerOn,
                            rowsStep(() -> bindIntLong(Numbers.INT_NULL, 10), "2\t11\t101"),
                            referenceStep(() -> bindIntLong(Numbers.INT_NULL, Numbers.LONG_NULL)),
                            rowsStep(() -> bindIntLong(10, Numbers.LONG_NULL), "")
                    );
                    // the plan keeps the RIGHT join and runs both conjuncts after it
                    assertQuery(table[0] + lateralSql(innerOn)).noLeakCheck().assertsPlanContaining(
                            "Filter filter: t.i>=$0::int",
                            "Filter filter: t.cl>=$1::long",
                            "Hash Right Outer Join"
                    );

                    final String whereKeyEquality = table[1] + " t RIGHT JOIN refunds r ON t.x = r.k AND r.k = {K}"
                            + " WHERE t.x = {K} AND t.i = $1 AND t.cl >= $2";
                    assertSteps(
                            table[0],
                            lateralJoin,
                            whereKeyEquality,
                            whereKeyEquality,
                            referenceStep(() -> bindIntLong(10, Numbers.LONG_NULL)),
                            rowsStep(() -> bindIntLong(10, 10), "1\t10\t100"),
                            rowsStep(() -> bindIntLong(Numbers.INT_NULL, 10), "2\t11\t101")
                    );
                    assertQuery(table[0] + lateralSql(whereKeyEquality)).noLeakCheck().assertsPlanContaining(
                            "Filter filter: (t.i=$0::int and t.cl>=$1::long and r.__qdb_outer_ref__1_k=t.x)",
                            "Hash Right Outer Join"
                    );

                    // NULL != 0 is true on INT, and t.x = o.k drops the NULL-extended rows
                    final String whereKey = table[1] + " t RIGHT JOIN refunds r ON t.x = r.k AND r.k = {K} WHERE t.x = {K} AND t.ci != 0";
                    assertSteps(table[0], lateralJoin, whereKey, whereKey, rowsStep(bindVariableService::clear, "1\t10\t100\n2\t11\t101"));
                }
            }
            bindVariableService.clear();
        });
    }

    // Without trades, t.i = NULL AND t.cl >= (SELECT max(l) FROM trades) keeps the rows that the RIGHT
    // join NULL-extends, as the sub-query is NULL too, while t.sid != 0 drops them, as SHORT has no
    // NULL. Without refunds, the RIGHT join has no row.
    @Test
    public void testNullValueNextToAComputedColumnOnEmptyTables() throws Exception {
        assertMemoryLeak(() -> {
            createAnyConjunctTables();
            final String body = MIXED_TABLE + " t RIGHT JOIN refunds r ON t.x = r.k AND r.k = {K} WHERE ";
            execute("TRUNCATE TABLE trades");
            assertSteps(
                    body + "t.i = $1 AND t.cl >= (SELECT max(l) FROM trades)",
                    referenceStep(() -> bindVariableService.setInt(0, Numbers.INT_NULL))
            );
            assertSteps(
                    body + "t.i = $1 AND t.sid != 0",
                    rowsStep(() -> bindVariableService.setInt(0, Numbers.INT_NULL), "")
            );
            execute("INSERT INTO trades (id, x, i, l, ts) VALUES (10, 1, 10, 10, 1::timestamp), (11, 2, NULL, 10, 2::timestamp)");
            execute("TRUNCATE TABLE refunds");
            assertSteps(
                    body + "t.i = $1 AND t.sid != 0",
                    rowsStep(() -> bindVariableService.setInt(0, Numbers.INT_NULL), "")
            );
            bindVariableService.clear();
        });
    }

    // WHERE t.x = o.k lets the rewriter key the rows by t.x, which drops the rows that the RIGHT join
    // NULL-extends. NULL != 0 is true on INT. With $1 10, trade 11 has i NULL, and NULL <= 10 is false,
    // while with $1 NULL, NULL <= NULL keeps it.
    @Test
    public void testNullValueNextToAComputedColumnOnWhereKey() throws Exception {
        assertMemoryLeak(() -> {
            createAnyConjunctTables();
            final String body = MIXED_TABLE + " t RIGHT JOIN refunds r ON t.x = r.k AND r.k = {K}"
                    + " WHERE t.x = {K} AND t.i <= $1 AND t.ci != 0";
            assertSteps(
                    body,
                    rowsStep(() -> bindVariableService.setInt(0, 10), "1\t10\t100"),
                    referenceStep(() -> bindVariableService.setInt(0, Numbers.INT_NULL))
            );
            bindVariableService.clear();
            bindVariableService.setInt(0, 10);
            // the plan keeps the RIGHT join, and the filter after it holds t.i <= $1 as $1 >= t.i
            assertQuery(lateralSql(body)).noLeakCheck().assertsPlanContaining(
                    "Filter filter: ($0::int>=t.i and t.ci!=0 and r.__qdb_outer_ref__1_k=t.x)",
                    "Hash Right Outer Join"
            );
            bindVariableService.clear();
        });
    }

    // A sub-query that fails fails the query with the error that the filter gives on the table alone,
    // also when t.i = $1 is true. The next query runs.
    @Test
    public void testNullValueNextToAComputedColumnWithFailingSubQuery() throws Exception {
        assertNullValueWithFailingSubQuery(MIXED_TABLE, "t.cl");
    }

    // A scalar sub-query nested in a filter, below a CASE without ELSE, which is NULL when its WHEN is
    // false, as it is on a sub-query that finds no row: a NULL value keeps the rows that the RIGHT join
    // NULL-extends, as NULL >= NULL is true. Per outer row, refund k matches both trades: t.id >= 10
    // keeps both. Trade 10 has l 10 and trade 11 l NULL, so t.l >= 10 keeps trade 10. The trades have
    // the timestamps 1 and 2, and the refunds 1 to 3, so t.ts >= 1 keeps both trades.
    // The plan keeps the RIGHT join, and the filter after it runs each sub-query once.
    @Test
    public void testNullValueOfANestedSubQuery() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String body = "trades t RIGHT JOIN refunds r ON r.k = {K} WHERE ";
            // trade 11 has l NULL, and NULL >= NULL keeps it next to the refunds of no trade
            final String otherRefundsAndTrade11 = """
                    id\ttid\trid
                    1\tnull\t101
                    1\tnull\t102
                    1\t11\t100
                    2\tnull\t100
                    2\tnull\t102
                    2\t11\t101
                    3\tnull\t100
                    3\tnull\t101
                    3\t11\t102
                    """;
            // the filter, the filter whose sub-queries find no row, its rows, the rows of the latter, the
            // filter as the plan prints it up to its first sub-query, and the number of its sub-queries
            final String[][] cases = {
                    {
                            "t.id >= CASE WHEN (SELECT max(id) FROM refunds) = 102 THEN 10 END",
                            "t.id >= CASE WHEN (SELECT max(id) FROM refunds WHERE id < 0) = 102 THEN 10 END",
                            BOTH_TRADES_PER_OUTER_ROW,
                            OTHER_REFUNDS_PER_OUTER_ROW,
                            "t.id>=case([102 [thread-safe] = cursor",
                            "1"
                    },
                    {
                            "t.l >= CASE WHEN (SELECT max(l) FROM trades) > 0 THEN 10::long END",
                            "t.l >= CASE WHEN (SELECT max(l) FROM trades WHERE id < 0) > 0 THEN 10::long END",
                            TRADE_10_PER_OUTER_ROW,
                            otherRefundsAndTrade11,
                            "t.l>=case([0 [thread-safe] < cursor",
                            "1"
                    },
                    {
                            "t.ts >= CASE WHEN (SELECT max(ts) FROM refunds) > 0::timestamp THEN 1::timestamp END",
                            "t.ts >= CASE WHEN (SELECT max(ts) FROM refunds WHERE id < 0) > 0::timestamp THEN 1::timestamp END",
                            BOTH_TRADES_PER_OUTER_ROW,
                            OTHER_REFUNDS_PER_OUTER_ROW,
                            "t.ts>=case([1970-01-01T00:00:00.000000Z [thread-safe] < cursor",
                            "1"
                    },
                    {
                            "t.ts >= CASE WHEN (SELECT max(ts) FROM refunds) = 3::timestamp THEN 1::timestamp END",
                            "t.ts >= CASE WHEN (SELECT max(ts) FROM refunds WHERE id < 0) = 3::timestamp THEN 1::timestamp END",
                            BOTH_TRADES_PER_OUTER_ROW,
                            OTHER_REFUNDS_PER_OUTER_ROW,
                            "t.ts>=case([1970-01-01T00:00:00.000003Z=cursor",
                            "1"
                    },
                    // one function reads both sub-queries
                    {
                            "t.ts >= CASE WHEN 2::timestamp BETWEEN (SELECT min(ts) FROM refunds) AND (SELECT max(ts) FROM refunds) THEN 1::timestamp END",
                            "t.ts >= CASE WHEN 2::timestamp BETWEEN (SELECT min(ts) FROM refunds) AND (SELECT max(ts) FROM refunds WHERE id < 0) THEN 1::timestamp END",
                            BOTH_TRADES_PER_OUTER_ROW,
                            OTHER_REFUNDS_PER_OUTER_ROW,
                            "t.ts>=case([1970-01-01T00:00:00.000002Z between cursor",
                            "2"
                    },
                    // a sub-query with a sub-query in its WHERE
                    {
                            "t.id >= CASE WHEN (SELECT id FROM trades WHERE id = (SELECT max(id) FROM trades)) = 11 THEN 10 ELSE NULL::int END",
                            "t.id >= CASE WHEN (SELECT id FROM trades WHERE id = (SELECT max(id) FROM trades WHERE id < 0)) = 11 THEN 10 ELSE NULL::int END",
                            BOTH_TRADES_PER_OUTER_ROW,
                            OTHER_REFUNDS_PER_OUTER_ROW,
                            "t.id>=case([11 [thread-safe] = cursor",
                            "2"
                    }
            };
            for (String[] c : cases) {
                assertReference("", body + c[0], c[2]);
                final String sql = lateralSql(body + c[0]);
                assertQuery(sql).noLeakCheck()
                        .withPlanContaining("Filter filter: " + c[4], "Nested Loop Right Join")
                        .expectSize()
                        .returns(c[2]);
                final String plan = planOf(sql);
                Assert.assertEquals(plan, Integer.parseInt(c[5]), countOf(plan, "cursor"));
                assertReference("", body + c[1], c[3]);
                assertQuery(lateralSql(body + c[1])).noLeakCheck().sizeMayVary().returns(c[3]);
            }

            // each LATERAL join, next to a bind variable. t.i = NULL keeps trade 11, as NULL = NULL is
            // true, and 10 >= NULL is false.
            final String nested = "t.id >= CASE WHEN (SELECT max(id) FROM refunds) = 102 THEN 10 END";
            final String nullNested = "t.id >= CASE WHEN (SELECT max(id) FROM refunds WHERE id < 0) = 102 THEN 10 END";
            for (String lateralJoin : LATERAL_JOINS) {
                assertSteps("", lateralJoin, body + nested, body + nested, rowsStep(bindVariableService::clear, "1\t10\t100\n1\t11\t100\n2\t10\t101\n2\t11\t101\n3\t10\t102\n3\t11\t102"));
                assertSteps("", lateralJoin, body + nullNested, body + nullNested, rowsStep(bindVariableService::clear, "1\tnull\t101\n1\tnull\t102\n2\tnull\t100\n2\tnull\t102\n3\tnull\t100\n3\tnull\t101"));
                final String withBind = body + "t.i = $1 AND " + nested;
                assertSteps(
                        "",
                        lateralJoin,
                        withBind,
                        withBind,
                        rowsStep(() -> bindVariableService.setInt(0, Numbers.INT_NULL), "1\t11\t100\n2\t11\t101\n3\t11\t102"),
                        rowsStep(() -> bindVariableService.setInt(0, 10), "1\t10\t100\n2\t10\t101\n3\t10\t102")
                );
                final String withBindAndNull = body + "t.i = $1 AND " + nullNested;
                assertSteps(
                        "",
                        lateralJoin,
                        withBindAndNull,
                        withBindAndNull,
                        rowsStep(() -> bindVariableService.setInt(0, Numbers.INT_NULL), "1\tnull\t101\n1\tnull\t102\n2\tnull\t100\n2\tnull\t102\n3\tnull\t100\n3\tnull\t101"),
                        rowsStep(() -> bindVariableService.setInt(0, 10), "")
                );
            }
            bindVariableService.clear();
            bindVariableService.setInt(0, 10);
            assertQuery(lateralSql(body + "t.i = $1 AND " + nested)).noLeakCheck()
                    .withPlanContaining("Filter filter: (t.i=$0::int and t.id>=case([102 [thread-safe] = cursor", "Nested Loop Right Join")
                    .expectSize()
                    .returns(TRADE_10_PER_OUTER_ROW);
            bindVariableService.clear();
        });
    }

    // A compiled query reads new data. Refund 103 makes max(id) 103, so the CASE becomes NULL, which
    // keeps the rows that the RIGHT join NULL-extends: per outer row, the three refunds of no trade,
    // 103 included, as it matches no outer key. Without trades, max(id) over trades is NULL, so a CASE
    // without ELSE is NULL, and every refund is a NULL-extended row of each outer row, while the ELSE
    // value 11 drops them all. Before, t.id >= 10 keeps both trades per outer row.
    @Test
    public void testNullValueOfANestedSubQueryAfterDataChange() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String body = "trades t RIGHT JOIN refunds r ON r.k = {K} WHERE ";
            final String refundsBody = body + "t.id >= CASE WHEN (SELECT max(id) FROM refunds) > 102 THEN NULL::int ELSE 10 END";
            final String tradesBody = body + "t.id >= CASE WHEN (SELECT max(id) FROM trades) > 0 THEN 10 END";
            final String elseBody = body + "t.id >= CASE WHEN (SELECT max(id) FROM trades) > 0 THEN 10 ELSE 11 END";
            final String refundsSql = lateralSql(refundsBody);
            final String tradesSql = lateralSql(tradesBody);
            final String elseSql = lateralSql(elseBody);
            assertReference("", refundsBody, BOTH_TRADES_PER_OUTER_ROW);
            assertReference("", tradesBody, BOTH_TRADES_PER_OUTER_ROW);
            assertReference("", elseBody, BOTH_TRADES_PER_OUTER_ROW);
            assertQuery(refundsSql).noLeakCheck()
                    .withPlanContaining("Filter filter: t.id>=case([102 [thread-safe] < cursor", "Nested Loop Right Join")
                    .expectSize()
                    .returns(BOTH_TRADES_PER_OUTER_ROW);
            assertQuery(tradesSql).noLeakCheck()
                    .withPlanContaining("Filter filter: t.id>=case([0 [thread-safe] < cursor", "Nested Loop Right Join")
                    .expectSize()
                    .returns(BOTH_TRADES_PER_OUTER_ROW);
            try (
                    RecordCursorFactory refunds = select(refundsSql);
                    RecordCursorFactory trades = select(tradesSql);
                    RecordCursorFactory otherwise = select(elseSql)
            ) {
                assertFactory(refunds).withContext(sqlExecutionContext).expectSize().returns(BOTH_TRADES_PER_OUTER_ROW);
                assertFactory(trades).withContext(sqlExecutionContext).expectSize().returns(BOTH_TRADES_PER_OUTER_ROW);
                assertFactory(otherwise).withContext(sqlExecutionContext).expectSize().returns(BOTH_TRADES_PER_OUTER_ROW);

                execute("INSERT INTO refunds (id, k, ts) VALUES (103, 4, 4::timestamp)");
                final String otherRefunds = """
                        id\ttid\trid
                        1\tnull\t101
                        1\tnull\t102
                        1\tnull\t103
                        2\tnull\t100
                        2\tnull\t102
                        2\tnull\t103
                        3\tnull\t100
                        3\tnull\t101
                        3\tnull\t103
                        """;
                assertReference("", refundsBody, otherRefunds);
                assertFactory(refunds).withContext(sqlExecutionContext).expectSize().returns(otherRefunds);
                assertReference("", tradesBody, BOTH_TRADES_PER_OUTER_ROW);
                assertFactory(trades).withContext(sqlExecutionContext).expectSize().returns(BOTH_TRADES_PER_OUTER_ROW);

                execute("TRUNCATE TABLE trades");
                final String allRefunds = """
                        id\ttid\trid
                        1\tnull\t100
                        1\tnull\t101
                        1\tnull\t102
                        1\tnull\t103
                        2\tnull\t100
                        2\tnull\t101
                        2\tnull\t102
                        2\tnull\t103
                        3\tnull\t100
                        3\tnull\t101
                        3\tnull\t102
                        3\tnull\t103
                        """;
                assertReference("", tradesBody, allRefunds);
                assertFactory(trades).withContext(sqlExecutionContext).expectSize().returns(allRefunds);
                assertReference("", elseBody, "id\ttid\trid\n");
                assertFactory(otherwise).withContext(sqlExecutionContext).expectSize().returns("id\ttid\trid\n");
            }
        });
    }

    // A nested sub-query that fails fails the query with the error of the sub-query, which the filter
    // gives on the table alone. The same compiled query runs again once the sub-query finds one trade,
    // 10, and t.id >= 10 then keeps trade 10 per outer row.
    @Test
    public void testNullValueOfANestedSubQueryWithFailingSubQuery() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String filter = "t.id >= CASE WHEN (SELECT id FROM trades) = 10 THEN 10 END";
            final Outcome alone = run("SELECT id FROM trades t WHERE " + filter, "alone");
            Assert.assertEquals("scalar sub-query returned more than one row", alone.error);
            final String body = "trades t RIGHT JOIN refunds r ON r.k = {K} WHERE " + filter;
            final String sql = lateralSql(body);
            try (RecordCursorFactory factory = select(sql)) {
                Assert.assertEquals(alone.error, runFactory(factory, sql).error);
                execute("TRUNCATE TABLE trades");
                execute("INSERT INTO trades (id, x, ts) VALUES (10, 1, 1::timestamp)");
                assertFactory(factory).withContext(sqlExecutionContext).expectSize().returns(TRADE_10_PER_OUTER_ROW);
                assertReference("", body, TRADE_10_PER_OUTER_ROW);
            }
            assertQuery(sql).noLeakCheck()
                    .withPlanContaining("Filter filter: t.id>=case([10 [thread-safe] = cursor", "Nested Loop Right Join")
                    .expectSize()
                    .returns(TRADE_10_PER_OUTER_ROW);
        });
    }

    // Two ANDed conjuncts that keep the rows a RIGHT or FULL join NULL-extends only while their value
    // is NULL, so the filter drops those rows while any of the values is not NULL, and keeps them when
    // all of them are NULL. In the data of createAnyConjunctTables(), trade 11 holds NULL in i and 10
    // in l. With $1 NULL and $2 10, t.i = $1 and t.i <= $1 keep trade 11, as NULL = NULL and
    // NULL <= NULL are true, and t.l >= $2 drops every NULL-extended row, as NULL >= 10 is false. With
    // $2 NULL, t.l >= $2 drops every row with a value, and t.i = 10 drops the NULL-extended ones. One
    // compiled query runs with each pair of values.
    @Test
    public void testNullValueOfAnyConjunct() throws Exception {
        assertMemoryLeak(() -> {
            createAnyConjunctTables();
            // the body, and its rows when $1 is NULL and $2 10, and when both are 10
            final String[][] shapes = {
                    {"trades t RIGHT JOIN refunds r ON t.x = r.k AND r.k = {K} WHERE {P}", "2\t11\t101", "1\t10\t100"},
                    {
                            "trades t JOIN xs x ON x.k = {K} RIGHT JOIN refunds r ON t.x = r.k WHERE {P}",
                            "1\t11\t101\n2\t11\t101\n3\t11\t101",
                            "1\t10\t100\n2\t10\t100\n3\t10\t100"
                    },
                    {
                            "trades t JOIN xs x ON x.k = {K} FULL JOIN refunds r ON t.x = r.k WHERE {P}",
                            "1\t11\t101\n2\t11\t101\n3\t11\t101",
                            "1\t10\t100\n2\t10\t100\n3\t10\t100"
                    },
                    {
                            "trades t RIGHT JOIN refunds r ON r.k = {K} WHERE {P}",
                            "1\t11\t100\n2\t11\t101\n3\t11\t102",
                            "1\t10\t100\n2\t10\t101\n3\t10\t102"
                    }
            };
            for (String[] shape : shapes) {
                for (String filter : new String[]{"t.i = $1 AND t.l >= $2", "t.i <= $1 AND t.l >= $2"}) {
                    final String body = shape[0].replace("{P}", filter);
                    for (String lateralJoin : LATERAL_JOINS) {
                        assertSteps(
                                "",
                                lateralJoin,
                                body,
                                body,
                                rowsStep(() -> bindIntLong(10, 10), shape[2]),
                                rowsStep(() -> bindIntLong(Numbers.INT_NULL, 10), shape[1]),
                                rowsStep(() -> bindIntLong(10, Numbers.LONG_NULL), ""),
                                referenceStep(() -> bindIntLong(Numbers.INT_NULL, Numbers.LONG_NULL)),
                                // the NULL of INT
                                rowsStep(() -> bindIntLong(-2_147_483_648, 10), shape[1]),
                                rowsStep(() -> bindIntLong(10, 10), shape[2])
                        );
                    }
                }
            }
            bindIntLong(10, 10);
            // one plan serves every value, so it keeps the RIGHT join and runs the filter after it
            assertQuery(lateralSql("trades t RIGHT JOIN refunds r ON t.x = r.k AND r.k = {K} WHERE t.i = $1 AND t.l >= $2")).noLeakCheck().assertsPlanContaining(
                    "Filter filter: (t.i=$0::int and t.l>=$1::long)",
                    "Hash Right Outer Join"
            );
            bindVariableService.clear();
        });
    }

    // A compiled query reads new data: the sub-query is NULL while no trade has x = 3, so with $1 NULL
    // the filter keeps the rows that the RIGHT join NULL-extends. Trade 12 makes it 10, which drops
    // them, and its i, 5, does not equal NULL. TRUNCATE makes the sub-query NULL again.
    @Test
    public void testNullValueOfAnyConjunctAfterDataChange() throws Exception {
        assertNullValueAfterDataChange("trades", "t.l");
    }

    // A sub-query that fails fails the query with the error of the sub-query, which the filter gives on
    // the table alone, also when the conjunct before it is true. The next query runs.
    @Test
    public void testNullValueOfAnyConjunctWithFailingSubQuery() throws Exception {
        assertNullValueWithFailingSubQuery("trades", "t.l");
    }

    // A NULL value of each type with NULL: a text bind variable may convert to the NULL of the column
    // type, as '0.0.0.0' does for IPv4, while '' and the zero UUID are no NULL. SHORT has no NULL, so
    // t.sh >= 0 holds for the 0 that the RIGHT join puts in it. Constants, and two sub-queries that look
    // alike.
    @Test
    public void testNullValueOfAnyConjunctOfEachType() throws Exception {
        assertMemoryLeak(() -> {
            createAnyConjunctTables();
            final String body = "trades t RIGHT JOIN refunds r ON t.x = r.k AND r.k = {K} WHERE ";
            assertSteps(
                    body + "t.i = $1 AND t.l >= (SELECT max(l) FROM trades)",
                    rowsStep(() -> bindVariableService.setInt(0, Numbers.INT_NULL), "2\t11\t101"),
                    rowsStep(() -> bindVariableService.setInt(0, 10), "1\t10\t100")
            );
            assertSteps(
                    body + "t.i = $1 AND t.l >= (SELECT max(l) FROM trades WHERE id < 0)",
                    referenceStep(() -> bindVariableService.setInt(0, Numbers.INT_NULL)),
                    rowsStep(() -> bindVariableService.setInt(0, 10), "")
            );
            assertSteps(
                    body + "t.sy = $1 AND t.d >= $2",
                    rowsStep(() -> bindStrDouble(null, 10.0), "2\t11\t101"),
                    referenceStep(() -> bindStrDouble(null, Double.NaN)),
                    rowsStep(() -> bindStrDouble("abc", Double.NaN), ""),
                    rowsStep(() -> bindStrDouble("abc", 10.0), "1\t10\t100")
            );
            assertSteps(
                    body + "t.ip = $1 AND t.l >= $2",
                    rowsStep(() -> bindStrLong("0.0.0.0", 10), "2\t11\t101"),
                    referenceStep(() -> bindStrLong("0.0.0.0", Numbers.LONG_NULL)),
                    rowsStep(() -> bindStrLong("1.1.1.1", 10), "1\t10\t100")
            );
            // the column, and the binds of $1 to a value that is not NULL and to NULL
            record EqualityCase(String column, BindSetter value, BindSetter nullValue) {
            }
            final int decimalType = ColumnType.getDecimalType(18, 2);
            final int geoHashType = ColumnType.getGeoHashTypeWithBits(20);
            final EqualityCase[] equalityCases = {
                    new EqualityCase("t.v", () -> bindVariableService.setStr(0, ""), () -> bindVariableService.setStr(0, null)),
                    new EqualityCase("t.st", () -> bindVariableService.setStr(0, ""), () -> bindVariableService.setStr(0, null)),
                    new EqualityCase(
                            "t.u",
                            () -> bindVariableService.setStr(0, "00000000-0000-0000-0000-000000000000"),
                            () -> bindVariableService.setStr(0, null)
                    ),
                    // 10.00
                    new EqualityCase(
                            "t.dc",
                            () -> bindVariableService.setDecimal(0, 0, 0, 0, 1000, decimalType),
                            () -> bindVariableService.setDecimal(0, Decimals.DECIMAL256_HH_NULL, Decimals.DECIMAL256_HL_NULL, Decimals.DECIMAL256_LH_NULL, Decimals.DECIMAL256_LL_NULL, decimalType)
                    ),
                    // 0000
                    new EqualityCase(
                            "t.g",
                            () -> bindVariableService.setGeoHash(0, 0, geoHashType),
                            () -> bindVariableService.setGeoHash(0, GeoHashes.NULL, geoHashType)
                    )
            };
            for (EqualityCase equalityCase : equalityCases) {
                assertSteps(
                        body + equalityCase.column + " = $1 AND t.l >= $2",
                        rowsStep(() -> {
                            equalityCase.value.set();
                            bindVariableService.setLong(1, Numbers.LONG_NULL);
                        }, ""),
                        referenceStep(() -> {
                            equalityCase.nullValue.set();
                            bindVariableService.setLong(1, Numbers.LONG_NULL);
                        }),
                        rowsStep(() -> {
                            equalityCase.nullValue.set();
                            bindVariableService.setLong(1, 10);
                        }, "2\t11\t101")
                );
            }
            assertSteps(
                    body + "t.tn = $1 AND (SELECT tn FROM trades WHERE x = 1 LATEST ON ts PARTITION BY x) >= t.tn",
                    rowsStep(() -> bindVariableService.setTimestampNano(0, Numbers.LONG_NULL), ""),
                    rowsStep(() -> bindVariableService.setTimestampNano(0, 1_704_067_200_000_000_000L), "1\t10\t100")
            );
            final String shortSql = body + "t.sh >= $1 AND t.i >= $2";
            assertSteps(
                    shortSql,
                    referenceStep(() -> bindShortInt((short) 0, Numbers.INT_NULL)),
                    rowsStep(() -> bindShortInt((short) 0, 10), "1\t10\t100")
            );
            assertQuery(lateralSql(shortSql)).noLeakCheck().assertsPlanContaining("Filter filter: (t.sh>=$0::short and t.i>=$1::int)", "Hash Right Outer Join");

            bindVariableService.clear();
            final String constants = body + "t.i = NULL AND t.l >= 10";
            assertSteps(constants, rowsStep(() -> {
            }, "2\t11\t101"));
            // t.l >= 10 drops the NULL-extended rows, so the plan joins trades with an INNER join, which
            // runs both conjuncts in the scan of trades
            assertQuery(lateralSql(constants)).noLeakCheck().assertsPlanContaining("filter: (i=null and l>=10)");

            // max(id) over no refund is NULL, and NULL >= NULL is true, so only t.id >= 10 drops the
            // NULL-extended rows, whichever side of the comparison the sub-query is on. A CASE
            // over sub-queries is NULL when the sub-queries find no trade, and 0 when 1::timestamp
            // is between the timestamps of the trades.
            final String[] subQueryFilters = {
                    "t.i >= (SELECT max(id) FROM refunds WHERE id < 0) AND t.id >= (SELECT min(id) FROM trades)",
                    "(SELECT max(id) FROM refunds WHERE id < 0) <= t.i AND (SELECT min(id) FROM trades) <= t.id",
                    "t.i >= CASE WHEN 1::timestamp BETWEEN (SELECT min(ts) FROM trades WHERE id < 0) AND (SELECT max(ts) FROM trades WHERE id < 0) THEN 0 END"
                            + " AND t.id >= CASE WHEN 1::timestamp BETWEEN (SELECT min(ts) FROM trades) AND (SELECT max(ts) FROM trades) THEN 0 END"
            };
            for (String subQueryFilter : subQueryFilters) {
                final String subQueries = body + subQueryFilter;
                assertSteps(subQueries, rowsStep(() -> {
                }, "2\t11\t101"));
                // the plan keeps the RIGHT join, and the filter after it runs each sub-query once
                final String plan = planOf(lateralSql(subQueries));
                TestUtils.assertContains(plan, "Hash Right Outer Join");
                Assert.assertEquals(plan, countOf(subQueryFilter, "(SELECT"), countOf(plan, "cursor"));
            }

            // both conjuncts read one bind variable
            final String sameBindVariable = body + "t.i >= $1 AND t.id >= $1";
            assertSteps(
                    sameBindVariable,
                    referenceStep(() -> bindVariableService.setInt(0, Numbers.INT_NULL)),
                    rowsStep(() -> bindVariableService.setInt(0, 10), "1\t10\t100")
            );
            assertQuery(lateralSql(sameBindVariable)).noLeakCheck().assertsPlanContaining("Filter filter: (t.i>=$0::int and t.id>=$0::int)", "Hash Right Outer Join");
            bindVariableService.clear();
        });
    }

    // A NULL value in each place of the body: a later INNER join's ON clause, a WHERE that lets the
    // rewriter key the rows by t.x, and a sub-query column, whose type the rewriter knows or does not.
    @Test
    public void testNullValueOfAnyConjunctOnEachPath() throws Exception {
        assertMemoryLeak(() -> {
            createAnyConjunctTables();
            for (String lateralJoin : LATERAL_JOINS) {
                final String innerOn = "trades t RIGHT JOIN refunds r ON t.x = r.k AND r.k = {K} JOIN xs x ON x.k = r.k AND t.i >= $1 WHERE t.l >= $2";
                assertSteps(
                        "",
                        lateralJoin,
                        innerOn,
                        innerOn,
                        rowsStep(() -> bindIntLong(Numbers.INT_NULL, 10), "2\t11\t101"),
                        referenceStep(() -> bindIntLong(Numbers.INT_NULL, Numbers.LONG_NULL)),
                        rowsStep(() -> bindIntLong(10, Numbers.LONG_NULL), "")
                );
                final String whereKey = "trades t RIGHT JOIN refunds r ON t.x = r.k AND r.k = {K} WHERE t.x = {K} AND t.i <= $1 AND t.l >= $2";
                assertSteps(
                        "",
                        lateralJoin,
                        whereKey,
                        whereKey,
                        rowsStep(() -> bindIntLong(Numbers.INT_NULL, 10), "2\t11\t101"),
                        referenceStep(() -> bindIntLong(Numbers.INT_NULL, Numbers.LONG_NULL)),
                        rowsStep(() -> bindIntLong(10, 10), "1\t10\t100")
                );
                // the CTE prefix, then the table
                final String[][] tables = {
                        {"", "(SELECT * FROM (SELECT id, x, i, l FROM trades))"},
                        {"WITH tt AS (SELECT id, x, i, l FROM trades) ", "tt"},
                        {"", "(SELECT id, x, i, l FROM trades GROUP BY id, x, i, l)"},
                        {"", "(SELECT DISTINCT id, x, i, l FROM trades)"},
                        {"", "(SELECT CAST(id AS INT) id, x, CAST(i AS INT) i, CAST(l AS LONG) l FROM trades)"},
                        {"", "(SELECT id, x, i, l FROM trades UNION ALL SELECT id, x, i, l FROM trades WHERE id < 0)"},
                        {"", "(SELECT tr.id, tr.x, tr.i, tr.l FROM trades tr CROSS JOIN long_sequence(1) u)"}
                };
                final String body = "{T} t RIGHT JOIN refunds r ON t.x = r.k AND r.k = {K} WHERE t.i = $1 AND t.l >= $2";
                // both conjuncts read one bind variable
                final String sameBindVariable = "{T} t RIGHT JOIN refunds r ON t.x = r.k AND r.k = {K} WHERE t.i >= $1 AND t.id >= $1";
                for (String[] table : tables) {
                    final String sameBindVariableBody = sameBindVariable.replace("{T}", table[1]);
                    assertSteps(
                            table[0],
                            lateralJoin,
                            sameBindVariableBody,
                            sameBindVariable.replace("{T}", "trades"),
                            referenceStep(() -> bindVariableService.setInt(0, Numbers.INT_NULL)),
                            rowsStep(() -> bindVariableService.setInt(0, 10), "1\t10\t100")
                    );
                    // the plan keeps the RIGHT join and runs the filter after it
                    assertQuery(table[0] + lateralSql(sameBindVariableBody)).noLeakCheck().assertsPlanContaining(
                            "Filter filter: (t.i>=$0::int and t.id>=$0::int)",
                            "Hash Right Outer Join"
                    );
                    assertSteps(
                            table[0],
                            lateralJoin,
                            body.replace("{T}", table[1]),
                            body.replace("{T}", "trades"),
                            rowsStep(() -> bindIntLong(Numbers.INT_NULL, 10), "2\t11\t101"),
                            referenceStep(() -> bindIntLong(Numbers.INT_NULL, Numbers.LONG_NULL)),
                            rowsStep(() -> bindIntLong(10, Numbers.LONG_NULL), ""),
                            rowsStep(() -> bindIntLong(10, 10), "1\t10\t100")
                    );
                }
            }
            bindVariableService.clear();
        });
    }

    // Without trades, the sub-query on refunds is 102, which drops every NULL-extended row. Without
    // refunds, the RIGHT join has no row. Neither query returns a row, although $1 is NULL.
    @Test
    public void testNullValueOfAnyConjunctOnEmptyTables() throws Exception {
        assertMemoryLeak(() -> {
            createAnyConjunctTables();
            final String body = "trades t RIGHT JOIN refunds r ON t.x = r.k AND r.k = {K} WHERE ";
            execute("TRUNCATE TABLE trades");
            assertSteps(
                    body + "t.i = $1 AND t.l >= (SELECT max(id) FROM refunds)",
                    rowsStep(() -> bindVariableService.setInt(0, Numbers.INT_NULL), "")
            );
            execute("INSERT INTO trades (id, x, i, l, ts) VALUES (10, 1, 10, 10, 1::timestamp), (11, 2, NULL, 10, 2::timestamp)");
            execute("TRUNCATE TABLE refunds");
            assertSteps(
                    body + "t.i = $1 AND t.l >= $2",
                    rowsStep(() -> bindIntLong(Numbers.INT_NULL, 10), "")
            );
            bindVariableService.clear();
        });
    }

    // Over a sub-query column of t, whose type the rewriter does not know, two sub-queries that look
    // alike. max(id) over no refund is NULL, and NULL >= NULL is true, so only t.id >= 10 drops the
    // NULL-extended rows, and a query that read the first sub-query for both would keep them.
    @Test
    public void testNullValueOfLookAlikeSubQueriesOverAComputedTable() throws Exception {
        assertMemoryLeak(() -> {
            createAnyConjunctTables();
            final String body = "{T} t RIGHT JOIN refunds r ON t.x = r.k AND r.k = {K}"
                    + " WHERE t.i >= (SELECT max(id) FROM refunds WHERE id < 0) AND t.id >= (SELECT min(id) FROM trades)";
            final String computedBody = body.replace("{T}", "(SELECT id, x, i, l FROM trades UNION ALL SELECT id, x, i, l FROM trades WHERE id < 0)");
            assertSteps("", "JOIN LATERAL", computedBody, body.replace("{T}", "trades"), rowsStep(() -> {
            }, "2\t11\t101"));
            // the plan keeps the RIGHT join, and the filter after it runs each sub-query once
            final String plan = planOf(lateralSql(computedBody));
            TestUtils.assertContains(plan, "Hash Right Outer Join");
            Assert.assertEquals(plan, 2, countOf(plan, "cursor"));
        });
    }

    private static void assertCheck(String sql, Outcome reference, Outcome actual, boolean isAccepted) {
        final List<String> failures = new ArrayList<>();
        checkCase("H", sql, reference, actual, failures);
        Assert.assertEquals("reference " + summary(reference) + ", query " + summary(actual) + ": " + failures, isAccepted ? 0 : 1, failures.size());
    }

    // Compiles the query once, with the random generator rnd, and runs it 60 times. A sub-query of the
    // filter takes a random value in each execution, and each execution returns the rows of one of its
    // values: valueRows or nullRows. Both must occur.
    private static void assertRowsOfEitherValue(String sql, String valueRows, String nullRows, Rnd rnd) throws SqlException {
        final List<String> expectedValueRows = sortedRows(valueRows);
        final List<String> expectedNullRows = sortedRows(nullRows);
        Assert.assertNotEquals(sql, expectedValueRows, expectedNullRows);
        sqlExecutionContext.setRandom(rnd);
        int valueCount = 0;
        int nullCount = 0;
        try (RecordCursorFactory factory = select(sql)) {
            for (int i = 0; i < 60; i++) {
                final Outcome outcome = runFactory(factory, sql);
                if (expectedValueRows.equals(outcome.rows)) {
                    valueCount++;
                } else {
                    Assert.assertEquals(sql, new Outcome(expectedNullRows, null, -1), outcome);
                    nullCount++;
                }
            }
        }
        Assert.assertTrue(sql + ": " + valueCount + " executions returned the rows of the value, " + nullCount + " those of NULL", valueCount > 0 && nullCount > 0);
    }

    private static void bindIntLong(int value1, long value2) throws SqlException {
        bindVariableService.setInt(0, value1);
        bindVariableService.setLong(1, value2);
    }

    private static void bindShortInt(short value1, int value2) throws SqlException {
        bindVariableService.setShort(0, value1);
        bindVariableService.setInt(1, value2);
    }

    private static void bindStrDouble(String value1, double value2) throws SqlException {
        bindVariableService.setStr(0, value1);
        bindVariableService.setDouble(1, value2);
    }

    private static void bindStrLong(String value1, long value2) throws SqlException {
        bindVariableService.setStr(0, value1);
        bindVariableService.setLong(1, value2);
    }

    private static String[] cat(String[] a, String[] b) {
        final String[] r = Arrays.copyOf(a, a.length + b.length);
        System.arraycopy(b, 0, r, a.length, b.length);
        return r;
    }

    // The query returns the rows of the reference, or, when the filter does not compile, see
    // checkReference(), fails with its error, as the reference does
    private static void checkCase(String header, String sql, Outcome reference, Outcome actual, List<String> failures) {
        if (reference.error != null) {
            if (actual.error == null) {
                failures.add(header + ": returned " + summary(actual) + ", the reference failed with " + summary(reference) + "\n    " + sql);
            } else if (!hasSameMessage(actual, reference)) {
                failures.add(header + ": failed with " + summary(actual) + ", expected the error of the filter, " + summary(reference) + "\n    " + sql);
            }
            return;
        }
        if (actual.error != null) {
            failures.add(header + ": failed with " + summary(actual) + ", expected " + summary(reference) + "\n    " + sql);
        } else if (!actual.rows.equals(reference.rows)) {
            failures.add(header + ": WRONG ROWS " + summary(actual) + ", expected " + summary(reference) + "\n    " + sql);
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

    private static int countOf(String text, String fragment) {
        int count = 0;
        for (int i = text.indexOf(fragment); i >= 0; i = text.indexOf(fragment, i + 1)) {
            count++;
        }
        return count;
    }

    // Trade 10 holds a value in each column. Trade 11 holds NULL in the columns that the first
    // conjunct of the filters of testNullValueOfAnyConjunct() reads, 0 in sh, which has no
    // NULL, and 10 in l and d, which their second conjunct reads.
    private static void createAnyConjunctTables() throws SqlException {
        createOuterTables();
        execute("""
                CREATE TABLE trades (
                    id INT, x INT, i INT, l LONG, d DOUBLE, sh SHORT, sy SYMBOL, ip IPv4, tn TIMESTAMP_NS,
                    v VARCHAR, st STRING, u UUID, dc DECIMAL(18,2), g GEOHASH(4c), ts TIMESTAMP
                ) TIMESTAMP(ts)""");
        execute("""
                INSERT INTO trades VALUES
                (10, 1, 10, 10, 10.0, 10, 'abc', '1.1.1.1', '2024-01-01T00:00:00.000000000Z', 'abc', 'abc',
                    'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 10::DECIMAL(18,2), #u33d, 1::timestamp),
                (11, 2, NULL, 10, 10.0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 2::timestamp)""");
    }

    private static void createOuterTables() throws SqlException {
        execute("CREATE TABLE orders (id INT, k INT, ts TIMESTAMP) TIMESTAMP(ts)");
        execute("INSERT INTO orders VALUES (1, 1, 1::timestamp), (2, 2, 2::timestamp), (3, 3, 3::timestamp)");
        execute("CREATE TABLE refunds (id INT, k INT, ts TIMESTAMP) TIMESTAMP(ts)");
        execute("INSERT INTO refunds VALUES (100, 1, 1::timestamp), (101, 2, 2::timestamp), (102, 3, 3::timestamp)");
        execute("CREATE TABLE xs (k INT, v INT)");
        execute("INSERT INTO xs VALUES (1, 100), (2, 200), (3, 300)");
    }

    private static void createTables() throws SqlException {
        createOuterTables();
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

    // the messages only, as the positions differ between the queries that share the filter
    private static boolean hasSameMessage(Outcome a, Outcome b) {
        return a.error.equals(b.error);
    }

    private static String instantiate(String template, String columnRef) {
        return template.replace("{C}", columnRef);
    }

    // the body with the outer key o.k in place of {K}, ordered for a stable result
    private static String lateralSql(String body) {
        return "SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (SELECT t.id tid, r.id rid FROM "
                + body.replace("{K}", "o.k") + ") l ORDER BY 1, 2, 3";
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
        return referenceSql(shape.body.replace("{T}", "trades").replace("{P}", filter));
    }

    // the body once per outer row, with the outer key in place of {K}
    private static String referenceSql(String body) {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < OUTER_KEYS.length; i++) {
            if (i > 0) {
                sb.append(" UNION ALL ");
            }
            sb.append("SELECT ").append(i + 1).append(" id, tid, rid FROM (SELECT t.id tid, r.id rid FROM ")
                    .append(body.replace("{K}", OUTER_KEYS[i])).append(')');
        }
        return sb.toString();
    }

    // Returns the rows of the query, or the message and the position of the SqlException that rejects
    // it, copied, as SqlException reuses one instance per thread unless assertions are on. Any other
    // exception fails the test at once: a NullPointerException or an AssertionError of the engine, and
    // a CairoException too, as a parallel filter hands the crash of a worker to the caller as one. No
    // case expects one, and a case that expects an error would take it for one.
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

    // The plan of the query, one line per row, without running it. Plan checks go through
    // assertQuery(sql).assertsPlanContaining(...) unless they count a fragment, which the builder
    // cannot assert.
    private static String planOf(String sql) throws SqlException {
        try (RecordCursorFactory factory = select(sql)) {
            return planOf(factory);
        }
    }

    private static String planOf(RecordCursorFactory factory) {
        final TextPlanSink plan = new TextPlanSink();
        plan.of(factory, sqlExecutionContext);
        final StringBuilder sb = new StringBuilder();
        for (int i = 1, n = plan.getLineCount(); i <= n; i++) {
            sb.append(plan.getLine(i)).append('\n');
        }
        return sb.toString();
    }

    // Returns the rows of one execution of the factory, or the message and the position of the
    // SqlException that fails it; see run()
    private static Outcome runFactory(RecordCursorFactory factory, String header) {
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            final StringSink rows = new StringSink();
            println(factory.getMetadata(), cursor, rows);
            return new Outcome(sortedRows(rows), null, -1);
        } catch (SqlException e) {
            return new Outcome(null, e.getFlyweightMessage().toString(), e.getPosition());
        } catch (Throwable e) {
            throw new AssertionError(header + ": failed with " + e, e);
        }
    }

    // a step that returns the rows of the reference under its bind values
    private static Step referenceStep(BindSetter bind) {
        return new Step(bind, null);
    }

    private static Step rowsStep(BindSetter bind, String rows) {
        return new Step(bind, rows);
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

    // Compiles the query of each shape and filter once and runs it with low, NULL, high and low bind
    // values again. Each execution returns the rows of the reference under the same values.
    private void assertBindVariableValuesShareThePlan(String[] filters, String[] bindTypes) throws Exception {
        for (Shape shape : new Shape[]{Shape.RIGHT_ON, Shape.INNER_ON_RIGHT, Shape.INNER_ON_FULL}) {
            for (int f = 0; f < filters.length; f++) {
                final String sql = lateralSql(shape, Form.TABLE, filters[f]) + " ORDER BY 1, 2, 3";
                final String bindType = bindTypes[f];
                final ObjList<BindVarTuple> cases = new ObjList<>();
                for (BindValues values : new BindValues[]{BindValues.LOW, BindValues.NULL, BindValues.HIGH, BindValues.LOW}) {
                    final String description = shape + " " + filters[f] + " " + values;
                    bindVariableService.clear();
                    setBindVariables(bindType, values);
                    sink.clear();
                    printSql("SELECT * FROM (" + referenceSql(shape, filters[f]) + ") ORDER BY 1, 2, 3");
                    cases.add(BindVarTuple.ok(description, sink.toString(), service -> setBindVariables(bindType, values)));
                }
                assertQuery(sql).noLeakCheck().expectSize().assertBinds(cases);
            }
        }
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
        checkCase(header, sql, reference, run(sql, header), failures);
    }

    // Asserts the rows of the reference, the body once per outer row, ordered by the outer row and the ids
    private void assertReference(String prefix, String body, String expected) throws Exception {
        assertQuery(prefix + "SELECT * FROM (" + referenceSql(body) + ") ORDER BY 1, 2, 3").noLeakCheck().returns(expected);
    }

    // The body of testNullValueNextToAComputedColumnAfterDataChange() and
    // testNullValueOfAnyConjunctAfterDataChange() over the table and its column
    private void assertNullValueAfterDataChange(String table, String column) throws Exception {
        assertMemoryLeak(() -> {
            createAnyConjunctTables();
            final String body = table + " t RIGHT JOIN refunds r ON t.x = r.k AND r.k = {K}"
                    + " WHERE t.i = $1 AND " + column + " >= (SELECT max(l) FROM trades WHERE x = 3)";
            final String sql = lateralSql(body);
            bindVariableService.clear();
            bindVariableService.setInt(0, Numbers.INT_NULL);
            try (RecordCursorFactory factory = select(sql)) {
                // no trade has x = 3, so the sub-query is NULL, and NULL = NULL and NULL >= NULL keep the
                // rows that the RIGHT join NULL-extends, while trade 11 has l 10
                final String nullExtended = """
                        id\ttid\trid
                        1\tnull\t101
                        1\tnull\t102
                        2\tnull\t100
                        2\tnull\t102
                        3\tnull\t100
                        3\tnull\t101
                        3\tnull\t102
                        """;
                assertReference("", body, nullExtended);
                assertFactory(factory).withContext(sqlExecutionContext).expectSize().returns(nullExtended);
                execute("INSERT INTO trades (id, x, i, l, ts) VALUES (12, 3, 5, 10, 3::timestamp)");
                assertReference("", body, """
                        id\ttid\trid
                        2\t11\t101
                        """);
                assertFactory(factory).withContext(sqlExecutionContext).expectSize().returns("id\ttid\trid\n2\t11\t101\n");
                execute("TRUNCATE TABLE trades");
                // the RIGHT join NULL-extends every refund, and the sub-query is NULL again
                final String allNullExtended = """
                        id\ttid\trid
                        1\tnull\t100
                        1\tnull\t101
                        1\tnull\t102
                        2\tnull\t100
                        2\tnull\t101
                        2\tnull\t102
                        3\tnull\t100
                        3\tnull\t101
                        3\tnull\t102
                        """;
                assertReference("", body, allNullExtended);
                assertFactory(factory).withContext(sqlExecutionContext).expectSize().returns(allNullExtended);
            }
            bindVariableService.clear();
        });
    }

    // The body of testNullValueNextToAComputedColumnWithFailingSubQuery() and
    // testNullValueOfAnyConjunctWithFailingSubQuery() over the table and its column
    private void assertNullValueWithFailingSubQuery(String table, String column) throws Exception {
        assertMemoryLeak(() -> {
            createAnyConjunctTables();
            bindVariableService.clear();
            bindVariableService.setInt(0, Numbers.INT_NULL);
            final Outcome alone = run("SELECT id FROM trades t WHERE t.l >= (SELECT l FROM trades)", "alone");
            Assert.assertNotNull(alone.error);
            final String body = table + " t RIGHT JOIN refunds r ON t.x = r.k AND r.k = {K} WHERE t.i = $1 AND " + column;
            final String sql = lateralSql(body + " >= (SELECT l FROM trades)");
            try (RecordCursorFactory factory = select(sql)) {
                Assert.assertEquals(alone.error, runFactory(factory, sql).error);
            }
            assertSteps(body + " >= $2", rowsStep(() -> bindIntLong(Numbers.INT_NULL, 10), "2\t11\t101"));
            bindVariableService.clear();
        });
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

    private void assertSteps(String body, Step... steps) throws Exception {
        assertSteps("", "JOIN LATERAL", body, body, steps);
    }

    private void assertSteps(String prefix, String lateralJoin, String body, String referenceBody, Step... steps) throws Exception {
        assertSteps(prefix, lateralJoin, body, referenceBody, !lateralJoin.startsWith("LEFT"), steps);
    }

    // Compiles the query once, with the bind values of the first step, and runs it with the bind values
    // of each step in turn, each step clearing the bind values and setting its own. A step returns the
    // rows of the reference, the body once per outer row over referenceBody: its own rows, which the
    // reference returns too, or, for a step of referenceStep(), whatever the reference returns. LEFT
    // JOIN LATERAL adds a NULL row for each outer row without one; assertBinds() runs the full
    // returns() checks on them, and expects the cursor to know its size when isSizeKnown is set. The
    // plan of LEFT JOIN LATERAL does not know it, nor does a filter after the lateral join.
    private void assertSteps(String prefix, String lateralJoin, String body, String referenceBody, boolean isSizeKnown, Step... steps) throws Exception {
        final String sql = prefix + "SELECT o.id, l.tid, l.rid FROM orders o " + lateralJoin
                + " (SELECT t.id tid, r.id rid FROM " + body.replace("{K}", "o.k") + ") l ORDER BY 1, 2, 3";
        final boolean isLeft = lateralJoin.startsWith("LEFT");
        final ObjList<BindVarTuple> cases = new ObjList<>();
        for (int i = 0; i < steps.length; i++) {
            final Step step = steps[i];
            final String description = "step " + i + ": " + sql;
            bindVariableService.clear();
            step.bind.set();
            final String stepRows;
            if (step.rows != null) {
                assertReference(prefix, referenceBody, "id\ttid\trid\n" + step.rows + (step.rows.isEmpty() ? "" : "\n"));
                stepRows = step.rows;
            } else {
                sink.clear();
                printSql(prefix + "SELECT * FROM (" + referenceSql(referenceBody) + ") ORDER BY 1, 2, 3");
                // the rows in the order of the query, which puts NULL first
                final List<String> referenceRows = new ArrayList<>(Arrays.asList(sink.toString().split("\n")));
                referenceRows.removeFirst();
                referenceRows.removeIf(String::isEmpty);
                stepRows = String.join("\n", referenceRows);
            }
            final StringBuilder expected = new StringBuilder("id\ttid\trid\n");
            final List<String> rows = stepRows.isEmpty() ? List.of() : List.of(stepRows.split("\n"));
            for (int k = 0; k < OUTER_KEYS.length; k++) {
                final String outerId = (k + 1) + "\t";
                boolean hasRow = false;
                for (String row : rows) {
                    if (row.startsWith(outerId)) {
                        expected.append(row).append('\n');
                        hasRow = true;
                    }
                }
                if (!hasRow && isLeft) {
                    expected.append(outerId).append("null\tnull\n");
                }
            }
            cases.add(BindVarTuple.ok(description, expected.toString(), service -> step.bind.set()));
        }
        bindVariableService.clear();
        steps[0].bind.set();
        assertQuery(sql).noLeakCheck().expectSize(isSizeKnown).assertBinds(cases);
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

    @FunctionalInterface
    private interface BindSetter {
        void set() throws SqlException;
    }

    private enum BindValues {
        LOW, NULL, HIGH
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

    private enum Shape {
        RIGHT_ON("{T} t RIGHT JOIN refunds r ON t.x = r.k AND r.k = {K} WHERE {P}"),
        INNER_ON_RIGHT("{T} t JOIN xs x ON x.k = {K} RIGHT JOIN refunds r ON t.x = r.k WHERE {P}"),
        INNER_ON_FULL("{T} t JOIN xs x ON x.k = {K} FULL JOIN refunds r ON t.x = r.k WHERE {P}"),
        WHERE_KEY_RIGHT("{T} t RIGHT JOIN refunds r ON t.x = r.k WHERE t.x = {K} AND {P}"),
        FULL_ON("{T} t FULL JOIN refunds r ON t.x = r.k AND r.k = {K} WHERE {P}");

        final String body;

        Shape(String body) {
            this.body = body;
        }
    }

    private record Column(String name, String type, String value, String fill, String[] filters, String bindType,
                          String[] bindFilters) {
    }

    // the rows of a query, or the message and the position of the error that rejects it
    private record Outcome(List<String> rows, String error, int position) {
    }

    // the bind values of one execution, and its rows for JOIN LATERAL, one per line in ORDER BY 1, 2, 3
    // order, or null for the rows of the reference
    private record Step(BindSetter bind, String rows) {
    }
}
