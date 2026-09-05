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


package io.questdb.test.griffin;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.std.Numbers;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Pins the exact message and position of every user-reachable SUBSAMPLE diagnostic, for the SUBSAMPLE
 * clause and for the window-function form. A caret ({@code ^}) in the SQL marks the expected error
 * position; {@link #assertError} strips it before compiling.
 */
public class SubsampleErrorMessageTest extends AbstractCairoTest {

    private static final String CLAUSE_ORDER_HINT = "SUBSAMPLE must be placed after the WHERE, LATEST ON, SAMPLE BY, GROUP BY and WINDOW clauses";
    private static final String EXPRESSION_VALUE = "SUBSAMPLE value argument must be a column name; alias the expression in the SELECT list and reference the alias";
    private static final String GAP_CONSTANT = "gap threshold must be a string constant such as '1h'";
    private static final String GAP_FORMAT = "; expected a number followed by a unit, such as '30s', '5m', '1h' or '1d'";
    private static final String HIDDEN_TIMESTAMP = "SUBSAMPLE requires a designated timestamp column; the SELECT list must include it unchanged";
    private static final String NO_TIMESTAMP = "SUBSAMPLE requires a designated timestamp column; the query source has no designated timestamp";
    private static final String ROW_LIMIT = "SUBSAMPLE input exceeds maximum of 3 rows (raise cairo.sql.subsample.max.rows)";
    private static final String UNKNOWN_METHOD = "unknown subsample method: foo. Supported methods: lttb, m4, minmax, uniform, cadence, sdt";

    @Test
    public void testClauseOrder() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertError("SELECT ts, v FROM t ORDER BY ts ^SUBSAMPLE lttb(v, 3)", "SUBSAMPLE must be placed before ORDER BY and LIMIT");
            assertError("SELECT ts, v FROM t LIMIT 10 ^SUBSAMPLE lttb(v, 3)", "SUBSAMPLE must be placed before ORDER BY and LIMIT");
            assertError("SELECT ts, v FROM t LIMIT 1, 10 ^SUBSAMPLE lttb(v, 3)", "SUBSAMPLE must be placed before ORDER BY and LIMIT");
            assertError("SELECT ts, v FROM t ORDER BY ts LIMIT 10 ^SUBSAMPLE lttb(v, 3)", "SUBSAMPLE must be placed before ORDER BY and LIMIT");
            assertError("SELECT ts, v FROM t SUBSAMPLE lttb(v, 3) ^SUBSAMPLE m4(v, 3)", "duplicate SUBSAMPLE clause");
            assertError("SELECT ts, v FROM t SUBSAMPLE lttb(v, 3) ORDER BY ts ^SUBSAMPLE m4(v, 3)", "duplicate SUBSAMPLE clause");
            assertError("SELECT ts, v FROM t SUBSAMPLE lttb(v, 3) ^WHERE v > 0", "unexpected token [WHERE] - " + CLAUSE_ORDER_HINT);
            assertError("SELECT ts, v FROM t SUBSAMPLE lttb(v, 3) ^LATEST ON ts PARTITION BY k", "unexpected token [LATEST] - " + CLAUSE_ORDER_HINT);
            assertError("SELECT ts, v FROM t SUBSAMPLE lttb(v, 3) ^SAMPLE BY 1h", "unexpected token [SAMPLE] - " + CLAUSE_ORDER_HINT);
            assertError("SELECT ts, v FROM t SUBSAMPLE lttb(v, 3) ^GROUP BY ts, v", "unexpected token [GROUP] - " + CLAUSE_ORDER_HINT);
            assertError("SELECT ts, v FROM t SUBSAMPLE lttb(v, 3) ^WINDOW w AS (ORDER BY ts)", "unexpected token [WINDOW] - " + CLAUSE_ORDER_HINT);
            assertError("SELECT * FROM (SELECT ts, v FROM t SUBSAMPLE lttb(v, 3) ^WHERE v > 0)", "unexpected token [WHERE] - " + CLAUSE_ORDER_HINT);
            assertError("SELECT ts, v FROM t ^SUBSAMPLE lttb(v, 3) UNION SELECT ts, v FROM t", "unexpected token 'subsample'");
        });
    }

    @Test
    public void testDesignatedTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertError("SELECT ts, v FROM nots ^SUBSAMPLE lttb(v, 3)", NO_TIMESTAMP);
            assertError("SELECT ts, v FROM nots ^SUBSAMPLE m4(v, 3)", NO_TIMESTAMP);
            assertError("SELECT ts, v FROM nots ^SUBSAMPLE minmax(v, 3)", NO_TIMESTAMP);
            assertError("SELECT ts, v FROM nots ^SUBSAMPLE uniform(2)", NO_TIMESTAMP);
            assertError("SELECT ts, v FROM nots ^SUBSAMPLE cadence(2)", NO_TIMESTAMP);
            assertError("SELECT ts, v FROM nots SUBSAMPLE ^sdt(v, 0.5)", NO_TIMESTAMP);
            assertError("SELECT v FROM (SELECT v, ts FROM nots) ^SUBSAMPLE minmax(v, 3)", NO_TIMESTAMP);
            assertError("SELECT v FROM t ^SUBSAMPLE lttb(v, 3)", HIDDEN_TIMESTAMP);
            assertError("SELECT v, ts::LONG ts FROM t ^SUBSAMPLE uniform(2)", HIDDEN_TIMESTAMP);
            assertError("SELECT v, ts + 1 AS ts FROM t ^SUBSAMPLE m4(v, 3)", HIDDEN_TIMESTAMP);
            assertError("SELECT v FROM (SELECT v, ts FROM t) ^SUBSAMPLE minmax(v, 3)", HIDDEN_TIMESTAMP);
            assertError("SELECT a.v FROM t a ASOF JOIN t b ^SUBSAMPLE lttb(v, 3)", HIDDEN_TIMESTAMP);
            assertError("SELECT v FROM t SUBSAMPLE ^sdt(v, 0.5)", HIDDEN_TIMESTAMP);
        });
    }

    @Test
    public void testGapThreshold() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertError("SELECT ts, v FROM t SUBSAMPLE lttb(v, 3, ^5)", GAP_CONSTANT);
            assertError("SELECT ts, v FROM t SUBSAMPLE lttb(v, 3, ^concat('1', 'h'))", GAP_CONSTANT);
            assertError("SELECT ts, v FROM t SUBSAMPLE lttb(v, 3, ^null)", GAP_CONSTANT);
            assertError("SELECT ts, v FROM t SUBSAMPLE lttb(v, 3, ^'bad')", "invalid gap threshold 'bad'" + GAP_FORMAT);
            assertError("SELECT ts, v FROM t SUBSAMPLE lttb(v, 3, ^'5')", "invalid gap threshold '5'" + GAP_FORMAT);
            assertError("SELECT ts, v FROM t SUBSAMPLE lttb(v, 3, ^'5hh')", "invalid gap threshold '5hh'" + GAP_FORMAT);
            assertError("SELECT ts, v FROM t SUBSAMPLE lttb(v, 3, ^'')", "invalid gap threshold ''" + GAP_FORMAT);
            assertError("SELECT ts, v FROM t SUBSAMPLE lttb(v, 3, ^'-1h')", "invalid gap threshold '-1h'" + GAP_FORMAT);
            assertError("SELECT ts, v FROM t SUBSAMPLE lttb(v, 3, ^'0h')", "gap threshold must be greater than zero");
            assertError("SELECT ts, v FROM t SUBSAMPLE lttb(v, 3, '5^w')", "unsupported interval unit: w. Supported: s, m, h, d");
            assertError("SELECT ts, v FROM t SUBSAMPLE lttb(v, 3, ^'99999999999999999999h')", "gap threshold overflow");
            assertError("SELECT ts, v FROM t SUBSAMPLE lttb(v, 3, ^'9999999999999999d')", "gap threshold overflow");
            // the window-function form reports through the same parser
            assertError("SELECT ts, v, lttb(ts, v, 3, ^'bad') OVER (ORDER BY ts) FROM t", "invalid gap threshold 'bad'" + GAP_FORMAT);
            assertError("SELECT ts, v, lttb(ts, v, 3, '5^w') OVER (ORDER BY ts) FROM t", "unsupported interval unit: w. Supported: s, m, h, d");
            assertError("SELECT ts, v, lttb(ts, v, 3, ^'0h') OVER (ORDER BY ts) FROM t", "gap threshold must be greater than zero");
        });
    }

    @Test
    public void testMethodNameAndArity() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertError("SELECT ts, v FROM t SUBSAMPLE ^foo(v, 3)", UNKNOWN_METHOD);
            // an unknown method is reported even when the timestamp is also missing
            assertError("SELECT ts, v FROM nots SUBSAMPLE ^foo(v, 3)", UNKNOWN_METHOD);
            assertError("SELECT ts, v FROM t SUBSAMPLE ^lttb(v)", "lttb() requires at least 2 arguments: column and target points");
            assertError("SELECT ts, v FROM t SUBSAMPLE lttb(v, 3, '1h', ^9)", "lttb() accepts at most 3 arguments: column, target points, and optional gap threshold");
            assertError("SELECT ts, v FROM t SUBSAMPLE ^m4(v)", "m4() requires at least 2 arguments: column and target points");
            assertError("SELECT ts, v FROM t SUBSAMPLE m4(v, 3, ^4)", "m4() accepts exactly 2 arguments: column and target points");
            assertError("SELECT ts, v FROM t SUBSAMPLE ^minmax(v)", "minmax() requires at least 2 arguments: column and target points");
            assertError("SELECT ts, v FROM t SUBSAMPLE minmax(v, 3, ^4)", "minmax() accepts exactly 2 arguments: column and target points");
            assertError("SELECT ts, v FROM t SUBSAMPLE ^uniform(3, 4)", "uniform() requires exactly 1 argument: target points");
            assertError("SELECT ts, v FROM t SUBSAMPLE ^cadence(2, 1, 0)", "cadence() requires 1 or 2 arguments: stride and optional seed");
            assertError("SELECT ts, v FROM t SUBSAMPLE ^sdt(v)", "sdt() requires exactly 2 arguments: column and compdev");
            assertError("SELECT ts, v FROM t SUBSAMPLE ^sdt(v, 0.5, 1)", "sdt() requires exactly 2 arguments: column and compdev");
        });
    }

    @Test
    public void testRowLimit() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_SUBSAMPLE_MAX_ROWS, 3);
        assertMemoryLeak(() -> {
            createTables();
            assertError("SELECT ts, v FROM t SUBSAMPLE ^lttb(v, 2)", ROW_LIMIT);
            assertError("SELECT ts, v FROM t SUBSAMPLE ^m4(v, 2)", ROW_LIMIT);
            assertError("SELECT ts, v FROM t SUBSAMPLE ^minmax(v, 2)", ROW_LIMIT);
            assertError("SELECT ts, v FROM t SUBSAMPLE ^uniform(2)", ROW_LIMIT);
            assertError("SELECT ts, v FROM t SUBSAMPLE ^cadence(2)", ROW_LIMIT);
        });
    }

    @Test
    public void testSyntax() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertError("SELECT ts, v FROM t SUBSAMPLE^", "subsample method name expected");
            assertError("SELECT ts, v FROM t SUBSAMPLE ^(v, 3)", "subsample method name expected");
            assertError("SELECT ts, v FROM t SUBSAMPLE ^'lttb'(v, 3)", "subsample method name expected");
            assertError("SELECT ts, v FROM t SUBSAMPLE ^;", "subsample method name expected");
            assertError("SELECT ts, v FROM t SUBSAMPLE lttb^", "'(' expected after subsample method name");
            assertError("SELECT ts, v FROM t SUBSAMPLE lttb ^v, 3)", "'(' expected after subsample method name");
            assertError("SELECT ts, v FROM t SUBSAMPLE lttb(^", "expression expected");
            assertError("SELECT ts, v FROM t SUBSAMPLE lttb(^)", "expression expected");
            assertError("SELECT ts, v FROM t SUBSAMPLE lttb(v, ^)", "expression expected");
            assertError("SELECT ts, v FROM t SUBSAMPLE lttb(v ^3)", "',' or ')' expected");
            assertError("SELECT ts, v FROM t SUBSAMPLE lttb(v, 3^", "',' or ')' expected");
        });
    }

    @Test
    public void testTargetStrideAndSeed() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertError("SELECT ts, v FROM t SUBSAMPLE lttb(v, ^1)", "target points must be at least 2");
            assertError("SELECT ts, v FROM t SUBSAMPLE lttb(v, ^-3)", "target points must be at least 2");
            assertError("SELECT ts, v FROM t SUBSAMPLE lttb(v, ^3.5)", "integer expected for target point count");
            assertError("SELECT ts, v FROM t SUBSAMPLE lttb(v, ^'abc')", "integer expected for target point count");
            assertError("SELECT ts, v FROM t SUBSAMPLE lttb(v, ^true)", "integer expected for target point count");
            assertError("SELECT ts, v FROM t SUBSAMPLE lttb(v, ^null)", "target point count must be set");
            assertError("SELECT ts, v FROM t SUBSAMPLE lttb(v, ^3000000000)", "target points exceeds maximum of 2147483647");
            assertError("SELECT ts, v FROM t SUBSAMPLE lttb(v, ^v)", "target point count must be a constant or bind variable");
            assertError("SELECT ts, v FROM t SUBSAMPLE lttb(v, ^rnd_int())", "target point count must be a constant or bind variable");
            assertError("SELECT ts, v FROM t SUBSAMPLE m4(v, ^0)", "target points must be at least 2");
            assertError("SELECT ts, v FROM t SUBSAMPLE minmax(v, ^1)", "target points must be at least 2");
            assertError("SELECT ts, v FROM t SUBSAMPLE minmax(v, ^'x')", "integer expected for target point count");
            assertError("SELECT ts, v FROM t SUBSAMPLE uniform(^1)", "target points must be at least 2");
            assertError("SELECT ts, v FROM t SUBSAMPLE uniform(^'x')", "integer expected for target point count");
            assertError("SELECT ts, v FROM t SUBSAMPLE uniform(^null)", "target point count must be set");
            assertError("SELECT ts, v FROM t SUBSAMPLE uniform(^v)", "target point count must be a constant or bind variable");
            assertError("SELECT ts, v FROM t SUBSAMPLE cadence(^0)", "stride must be at least 1");
            assertError("SELECT ts, v FROM t SUBSAMPLE cadence(^'x')", "integer expected for stride");
            assertError("SELECT ts, v FROM t SUBSAMPLE cadence(^null)", "stride must be set");
            assertError("SELECT ts, v FROM t SUBSAMPLE cadence(^3000000000)", "stride exceeds maximum of 2147483647");
            assertError("SELECT ts, v FROM t SUBSAMPLE cadence(^v)", "stride must be a constant or bind variable");
            assertError("SELECT ts, v FROM t SUBSAMPLE cadence(2, ^'x')", "integer or NULL expected for seed");
            assertError("SELECT ts, v FROM t SUBSAMPLE cadence(2, ^v)", "seed must be a constant, bind variable, or NULL");
            assertError("SELECT ts, v FROM t SUBSAMPLE sdt(v, ^-1)", "SUBSAMPLE sdt requires a constant, non-negative finite compdev");
            assertError("SELECT ts, v FROM t SUBSAMPLE sdt(v, ^'x')", "SUBSAMPLE sdt requires a constant, non-negative finite compdev");
            assertError("SELECT ts, v FROM t SUBSAMPLE sdt(v, ^v)", "SUBSAMPLE sdt requires a constant, non-negative finite compdev");
            // a bind variable migrates and is range-checked when the query runs
            bindVariableService.setInt(0, 1);
            assertError("SELECT ts, v FROM t SUBSAMPLE lttb(v, ^$1)", "target points must be at least 2");
            assertError("SELECT ts, v FROM t SUBSAMPLE uniform(^$1)", "target points must be at least 2");
            bindVariableService.setInt(0, 0);
            assertError("SELECT ts, v FROM t SUBSAMPLE cadence(^$1)", "stride must be at least 1");
            bindVariableService.setInt(0, Numbers.INT_NULL);
            assertError("SELECT ts, v FROM t SUBSAMPLE m4(v, ^$1)", "target point count must be set");
            assertError("SELECT ts, v FROM t SUBSAMPLE cadence(^$1)", "stride must be set");
        });
    }

    @Test
    public void testValueColumn() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertError("SELECT ts, v FROM t SUBSAMPLE lttb(^x, 3)", "column not found in SELECT list: x");
            assertError("SELECT v AS price, ts FROM t SUBSAMPLE lttb(^v, 3)", "column not found in SELECT list: v");
            assertError("SELECT ts, v FROM t SUBSAMPLE m4(^x, 3)", "column not found in SELECT list: x");
            assertError("SELECT ts, v FROM t SUBSAMPLE minmax(^x, 3)", "column not found in SELECT list: x");
            assertError("SELECT ts, v FROM t SUBSAMPLE sdt(^x, 0.5)", "column not found in SELECT list: x");
            assertError("SELECT ts, v FROM t SUBSAMPLE lttb(^t.v, 3)", "qualified column names are not supported in SUBSAMPLE arguments; use the unqualified SELECT list name");
            assertError("SELECT ts, v FROM t SUBSAMPLE lttb(^1, 3)", "SUBSAMPLE value argument must be a column name, not a constant");
            assertError("SELECT ts, v FROM t SUBSAMPLE sdt(^1, 0.5)", "SUBSAMPLE value argument must be a column name, not a constant");
            assertError("SELECT ts, v FROM t SUBSAMPLE lttb(^$1, 3)", "SUBSAMPLE value argument must be a column name, not a bind variable");
            assertError("SELECT ts, v FROM t SUBSAMPLE lttb(v ^* 2, 3)", EXPRESSION_VALUE);
            assertError("SELECT ts, v FROM t SUBSAMPLE m4(^abs(v), 3)", EXPRESSION_VALUE);
            assertError("SELECT ts, v FROM t SUBSAMPLE sdt(^abs(v), 0.5)", EXPRESSION_VALUE);
            assertError("SELECT ts, s FROM t SUBSAMPLE lttb(^s, 3)", "numeric column expected, got: STRING");
            assertError("SELECT ts, k FROM t SUBSAMPLE minmax(^k, 3)", "numeric column expected, got: SYMBOL");
            assertError("SELECT ts, ts AS v2 FROM t SUBSAMPLE m4(^v2, 3)", "numeric column expected, got: TIMESTAMP");
        });
    }

    @Test
    public void testWindowFunctionFormArguments() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertError("SELECT ts, v, lttb(ts, v, v^::long) OVER (ORDER BY ts) FROM t", "target must be a constant or bind variable");
            assertError("SELECT ts, v, m4(ts, v, v^::long) OVER (ORDER BY ts) FROM t", "target must be a constant or bind variable");
            assertError("SELECT ts, v, minmax(ts, v, v^::long) OVER (ORDER BY ts) FROM t", "target must be a constant or bind variable");
            assertError("SELECT ts, v, uniform(v^::long) OVER (ORDER BY ts) FROM t", "target must be a constant or bind variable");
            assertError("SELECT ts, v, lttb(ts, v, ^'x') OVER (ORDER BY ts) FROM t", "integer expected for target point count");
            assertError("SELECT ts, v, m4(ts, v, ^'x') OVER (ORDER BY ts) FROM t", "integer expected for target point count");
            assertError("SELECT ts, v, uniform(^'x') OVER (ORDER BY ts) FROM t", "integer expected for target point count");
            assertError("SELECT ts, v, m4(ts, v, ^1) OVER (ORDER BY ts) FROM t", "target points must be at least 2");
            assertError("SELECT ts, v, uniform(^1) OVER (ORDER BY ts) FROM t", "target points must be at least 2");
            assertError("SELECT ts, v, cadence(^0) OVER (ORDER BY ts) FROM t", "stride must be at least 1");
            assertError("SELECT ts, v, cadence(^'x') OVER (ORDER BY ts) FROM t", "integer expected for stride");
            assertError("SELECT ts, v, cadence(v^::long) OVER (ORDER BY ts) FROM t", "stride must be a constant or bind variable");
            assertError("SELECT ts, v, cadence(2, ^'x') OVER (ORDER BY ts) FROM t", "integer or NULL expected for seed");
            assertError("SELECT ts, v, cadence(2, v^::long) OVER (ORDER BY ts) FROM t", "seed must be a constant, bind variable, or NULL");
            // the signature pins compdev to a constant, so overload resolution rejects a column (and a bind
            // variable) before the factory's own "constant expected" check can run
            assertError(
                    "SELECT ts, v, sdt(ts, v, ^v) OVER (ORDER BY ts) FROM t",
                    "argument type mismatch for function `sdt` at #3 expected: DOUBLE constant, actual: DOUBLE"
            );
            assertError("SELECT ts, v, sdt(ts, v, ^-1) OVER (ORDER BY ts) FROM t", "compdev must be a non-negative finite constant");
        });
    }

    @Test
    public void testWindowFunctionFormOrdering() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String[][] calls = {
                    {"lttb", "lttb(ts, v, 3)"},
                    {"m4", "m4(ts, v, 3)"},
                    {"minmax", "minmax(ts, v, 3)"},
                    {"uniform", "uniform(3)"},
                    {"cadence", "cadence(2)"},
                    {"sdt", "sdt(ts, v, 0.5)"},
            };
            for (String[] call : calls) {
                final String name = call[0];
                final String fn = call[1];
                assertError("SELECT ts, v, ^" + fn + " OVER () FROM t", name + "() requires ORDER BY");
                if (!"uniform".equals(name) && !"cadence".equals(name)) {
                    // position-based methods accept either direction; value-based methods need ascending time
                    assertError("SELECT ts, v, ^" + fn + " OVER (ORDER BY ts DESC) FROM t", name + "() requires ascending ORDER BY");
                }
                assertError(
                        "SELECT ts, v, ^" + fn + " OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t",
                        name + "() does not support framing; remove ROWS/RANGE clause"
                );
                if (!"sdt".equals(name)) {
                    assertError("SELECT ts, v, ^" + fn + " OVER (PARTITION BY k ORDER BY ts) FROM t", name + "() does not support PARTITION BY");
                }
            }
            // ordering by a non-timestamp column is rejected when the rows arrive out of timestamp order
            assertError("SELECT ts, v, ^lttb(ts, v, 3) OVER (ORDER BY v) FROM t", "lttb() requires the timestamp argument in ascending ORDER BY order");
            assertError("SELECT ts, v, ^m4(ts, v, 3) OVER (ORDER BY v) FROM t", "m4() requires the timestamp argument in ascending ORDER BY order");
            assertError("SELECT ts, v, ^minmax(ts, v, 3) OVER (ORDER BY v) FROM t", "minmax() requires the timestamp argument in ascending ORDER BY order");
        });
    }

    /**
     * Compiles and drains {@code sqlWithCaret} (with the caret removed) and asserts that it fails with
     * exactly {@code expectedMessage} at the caret position. Draining matters: some diagnostics surface
     * only while rows are consumed.
     */
    private static void assertError(String sqlWithCaret, String expectedMessage) throws Exception {
        final int caret = sqlWithCaret.indexOf('^');
        Assert.assertTrue("caret marker missing in: " + sqlWithCaret, caret > -1);
        final String sql = sqlWithCaret.substring(0, caret) + sqlWithCaret.substring(caret + 1);
        try (RecordCursorFactory factory = select(sql); RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            //noinspection StatementWithEmptyBody
            while (cursor.hasNext()) {
            }
            Assert.fail("expected the statement to fail: " + sql);
        } catch (SqlException | CairoException e) {
            Assert.assertEquals("message for: " + sql, expectedMessage, e.getFlyweightMessage().toString());
            Assert.assertEquals("position for: " + sql, caret, e.getPosition());
        }
    }

    private static void createTables() throws SqlException {
        execute("CREATE TABLE t (v DOUBLE, s STRING, k SYMBOL, ts TIMESTAMP) TIMESTAMP(ts)");
        execute("""
                INSERT INTO t VALUES
                (1.0, 'a', 'x', '2024-01-01T00:00:00Z'),
                (5.0, 'b', 'y', '2024-01-01T00:10:00Z'),
                (2.0, 'c', 'x', '2024-01-01T00:20:00Z'),
                (9.0, 'd', 'y', '2024-01-01T00:30:00Z'),
                (3.0, 'e', 'x', '2024-01-01T00:40:00Z')
                """);
        execute("CREATE TABLE nots (v DOUBLE, ts TIMESTAMP)");
        execute("INSERT INTO nots VALUES (1.0, '2024-01-01T00:00:00Z'), (2.0, '2024-01-01T00:10:00Z')");
    }
}
