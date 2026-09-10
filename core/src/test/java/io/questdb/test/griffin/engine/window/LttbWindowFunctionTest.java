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

package io.questdb.test.griffin.engine.window;

import io.questdb.griffin.SqlException;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.BindVarTuple;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class LttbWindowFunctionTest extends AbstractCairoTest {

    @Test
    public void testGapImplicitTimestampUnitsWithNanoOrder() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE nano_order (id INT, ts TIMESTAMP_NS, v DOUBLE) TIMESTAMP(ts)");
            execute("INSERT INTO nano_order VALUES (1, 0, 1), (2, 1_000_000_000, 2), (3, 2_000_000_000, 3), " +
                    "(4, 10_000_000_000, 4), (5, 11_000_000_000, 5), (6, 12_000_000_000, 6)");
            final ObjList<String> expressions = new ObjList<>();
            expressions.add("ts::TIMESTAMP::LONG");
            expressions.add("ts::DATE");
            expressions.add("ts::STRING");
            for (int cache = 0; cache < 2; cache++) {
                final boolean isLight = cache == 1;
                setProperty(io.questdb.PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, Boolean.toString(isLight));
                for (int i = 0; i < expressions.size(); i++) {
                    assertQuery("SELECT id, lttb(" + expressions.getQuick(i) + ", v, 2, '1s') OVER (ORDER BY ts) keep FROM nano_order")
                            .noLeakCheck().expectSize().columnType(0, io.questdb.cairo.ColumnType.INT)
                            .columnType(1, io.questdb.cairo.ColumnType.BOOLEAN)
                            .withPlanContaining(isLight ? "CachedWindowLight" : "CachedWindow\n", "unorderedFunctions")
                            .returns("id\tkeep\n1\ttrue\n2\tfalse\n3\ttrue\n4\ttrue\n5\tfalse\n6\ttrue\n");
                }
            }
        });
    }

    @Test
    public void testGapImplicitTimestampBoundaries() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE boundary_input (id INT, t LONG, v DOUBLE)");
            for (int cache = 0; cache < 2; cache++) {
                setProperty(io.questdb.PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, cache == 1 ? "true" : "false");
                // LONG uses micros, DATE has 1ms input resolution, and text preserves individual nanos.
                for (int type = 0; type < 3; type++) {
                    final long second = type == 2 ? 1_000_000_000L : 1_000_000L;
                    final long unit = type == 1 ? 1000 : 1;
                    final String expression = type == 0 ? "t" : type == 1 ? "(t / 1000)::DATE" : "t::TIMESTAMP_NS::STRING";
                    for (int boundary = -1; boundary <= 1; boundary++) {
                        final long next = second + boundary * unit;
                        execute("TRUNCATE TABLE boundary_input");
                        execute("INSERT INTO boundary_input VALUES (1, " + (-2 * second) + ", 1), (2, " + (-second) +
                                ", 2), (3, 0, 3), (4, " + next + ", 4), (5, " + (next + second) + ", 5), (6, " + (next + 2 * second) + ", 6)");
                        final boolean hasSplit = boundary == 1;
                        assertQuery("SELECT id, lttb(" + expression + ", v, 2, '1s') OVER (ORDER BY id) keep FROM boundary_input")
                                .noLeakCheck().expectSize().returns("id\tkeep\n1\ttrue\n2\tfalse\n3\t" + hasSplit +
                                        "\n4\t" + hasSplit + "\n5\tfalse\n6\ttrue\n");
                    }
                }
                execute("TRUNCATE TABLE boundary_input");
                execute("INSERT INTO boundary_input VALUES (1, -2_000_000, 1), (2, -1_000_000, 2), (3, 0, 3), " +
                        "(4, 10_000_000, 4), (5, 11_000_000, 5), (6, 12_000_000, 6)");
                final ObjList<String> expressions = implicitTimestampExpressions();
                for (int i = 0; i < expressions.size(); i++) {
                    assertQuery("SELECT id, lttb(" + expressions.getQuick(i) + ", v, 2, '1h') OVER (ORDER BY id) keep FROM boundary_input")
                            .noLeakCheck().expectSize().returns("id\tkeep\n1\ttrue\n2\tfalse\n3\tfalse\n4\tfalse\n5\tfalse\n6\ttrue\n");
                }
            }
        });
    }

    @Test
    public void testGapImplicitTimestampNullsAndSmallInputs() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE small_input (id INT, t LONG, v DOUBLE)");
            final ObjList<String> expressions = implicitTimestampExpressions();
            for (int cache = 0; cache < 2; cache++) {
                setProperty(io.questdb.PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, cache == 1 ? "true" : "false");
                for (int i = 0; i < expressions.size(); i++) {
                    final String expression = expressions.getQuick(i);
                    execute("TRUNCATE TABLE small_input");
                    assertSmallInput(expression, 2, "id\tkeep\n");
                    execute("INSERT INTO small_input VALUES (1, NULL, 1), (2, 0, NULL), (3, NULL, NULL)");
                    assertSmallInput(expression, 2, "id\tkeep\n1\tfalse\n2\tfalse\n3\tfalse\n");
                    execute("TRUNCATE TABLE small_input");
                    execute("INSERT INTO small_input VALUES (1, 0, 1)");
                    assertSmallInput(expression, 2, "id\tkeep\n1\ttrue\n");
                    execute("INSERT INTO small_input VALUES (2, 1_000_000, 2)");
                    assertSmallInput(expression, 2, "id\tkeep\n1\ttrue\n2\ttrue\n");
                    assertSmallInput(expression, 8, "id\tkeep\n1\ttrue\n2\ttrue\n");
                    execute("INSERT INTO small_input VALUES (3, NULL, 3), (4, 2_000_000, NULL)");
                    assertSmallInput(expression, 2, "id\tkeep\n1\ttrue\n2\ttrue\n3\tfalse\n4\tfalse\n");
                }
            }
        });
    }

    @Test
    public void testGapTimestampConstantsAndBinds() throws Exception {
        assertMemoryLeak(() -> {
            final String expected = "x\tkeep\n1\ttrue\n2\tfalse\n3\tfalse\n4\ttrue\n";
            final ObjList<String> constants = new ObjList<>();
            constants.add("'1970-01-01T00:00:00.000001Z'");
            constants.add("'1970-01-01T00:00:00.000000001Z'");
            constants.add("1::TIMESTAMP");
            constants.add("1::TIMESTAMP_NS");
            for (int cache = 0; cache < 2; cache++) {
                setProperty(io.questdb.PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, cache == 1 ? "true" : "false");
                for (int i = 0; i < constants.size(); i++) {
                    assertQuery("SELECT x, lttb(" + constants.getQuick(i) + ", x::DOUBLE, 2, '1s') OVER (ORDER BY x) keep FROM long_sequence(4)")
                            .noLeakCheck().expectSize().columnType(0, io.questdb.cairo.ColumnType.LONG)
                            .columnType(1, io.questdb.cairo.ColumnType.BOOLEAN).returns(expected);
                }
                for (int type = 0; type < 6; type++) {
                    final int bindType = type;
                    final ObjList<BindVarTuple> cases = new ObjList<>();
                    cases.add(BindVarTuple.ok("typed timestamp getter", expected, binds -> setTimestampBind(binds, bindType, false)));
                    cases.add(BindVarTuple.ok("typed NULL", "x\tkeep\n1\tfalse\n2\tfalse\n3\tfalse\n4\tfalse\n",
                            binds -> setTimestampBind(binds, bindType, true)));
                    cases.add(BindVarTuple.ok("reopen typed timestamp getter", expected, binds -> setTimestampBind(binds, bindType, false)));
                    assertQuery("SELECT x, lttb($1, x::DOUBLE, 2, '1s') OVER (ORDER BY x) keep FROM long_sequence(4)")
                            .noLeakCheck().expectSize().assertBinds(cases);
                }
            }
        });
    }

    @Test
    public void testGapImplicitTimestampFailureThenReuse() throws Exception {
        assertMemoryLeak(() -> {
            for (int cache = 0; cache < 2; cache++) {
                setProperty(io.questdb.PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, cache == 1 ? "true" : "false");
                for (int type = 4; type < 6; type++) {
                    final int bindType = type;
                    for (int cast = 0; cast < 2; cast++) {
                        final ObjList<BindVarTuple> cases = new ObjList<>();
                        final String expected = "x\tkeep\n1\ttrue\n2\tfalse\n3\tfalse\n4\ttrue\n";
                        cases.add(BindVarTuple.ok("valid text", expected, binds -> setTimestampBind(binds, bindType, false)));
                        final io.questdb.test.tools.BindVariableTestSetter malformed = binds -> {
                            if (bindType == 4) {
                                binds.setStr(0, "not-a-timestamp");
                            } else {
                                binds.setVarchar(0, new io.questdb.std.str.Utf8String("not-a-timestamp"));
                            }
                        };
                        // Explicit text casts return NULL on parse errors; implicit timestamp getters throw.
                        // Pin both existing contracts while reopening the same factory after the bad value.
                        cases.add(cast == 0
                                ? BindVarTuple.fails("malformed timestamp", "inconvertible value", malformed)
                                : BindVarTuple.ok("explicit malformed timestamp is NULL",
                                        "x\tkeep\n1\tfalse\n2\tfalse\n3\tfalse\n4\tfalse\n", malformed));
                        cases.add(BindVarTuple.ok("valid after failure", expected, binds -> setTimestampBind(binds, bindType, false)));
                        cases.add(BindVarTuple.ok("NULL after failure", "x\tkeep\n1\tfalse\n2\tfalse\n3\tfalse\n4\tfalse\n",
                                binds -> setTimestampBind(binds, bindType, true)));
                        assertQuery("SELECT x, lttb($1" + (cast == 0 ? "" : "::TIMESTAMP_NS") +
                                ", x::DOUBLE, 2, '1s') OVER (ORDER BY x) keep FROM long_sequence(4)")
                                .noLeakCheck().expectSize().assertBinds(cases);
                    }
                }
                for (int type = 0; type < 2; type++) {
                    final String sql = "SELECT x, lttb(x" + (type == 0 ? "" : "::DATE") +
                            ", x::DOUBLE, $1, '1h') OVER (ORDER BY x) keep FROM long_sequence(4)";
                    final ObjList<BindVarTuple> cases = new ObjList<>();
                    cases.add(BindVarTuple.ok("all", "x\tkeep\n1\ttrue\n2\ttrue\n3\ttrue\n4\ttrue\n", binds -> binds.setLong(0, 8)));
                    cases.add(BindVarTuple.fails("invalid target", sql.indexOf("$1"), "target points must be at least 2", binds -> binds.setLong(0, 1)));
                    cases.add(BindVarTuple.ok("reuse", "x\tkeep\n1\ttrue\n2\tfalse\n3\tfalse\n4\ttrue\n", binds -> binds.setLong(0, 2)));
                    assertQuery(sql).noLeakCheck().expectSize().assertBinds(cases);
                }
            }
        });
    }

    @Test
    public void testGapImplicitTimestampOrdering() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE order_input (id INT, ts TIMESTAMP, v DOUBLE, reverse_id INT) TIMESTAMP(ts)");
            execute("INSERT INTO order_input VALUES (1, 0, 1, 4), (2, 0, 2, 3), (3, 1_000_000, 3, 2), (4, 1_000_000, 4, 1)");
            final ObjList<String> expressions = new ObjList<>();
            expressions.add("ts::LONG");
            expressions.add("ts::DATE");
            expressions.add("ts::STRING");
            for (int cache = 0; cache < 2; cache++) {
                setProperty(io.questdb.PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, cache == 1 ? "true" : "false");
                for (int i = 0; i < expressions.size(); i++) {
                    final String prefix = "SELECT id, lttb(" + expressions.getQuick(i) + ", v, 2, '1s') OVER (";
                    final ObjList<String> orders = new ObjList<>();
                    orders.add("ts");
                    orders.add("id");
                    for (int order = 0; order < orders.size(); order++) {
                        assertQuery(prefix + "ORDER BY " + orders.getQuick(order) + " DESC) keep FROM order_input")
                                .noLeakCheck().fails(11, "requires ascending ORDER BY");
                        assertQuery(prefix + "ORDER BY " + orders.getQuick(order) + ") keep FROM order_input")
                                .noLeakCheck().expectSize().returns("id\tkeep\n1\ttrue\n2\tfalse\n3\tfalse\n4\ttrue\n");
                    }
                    assertQuery(prefix + "PARTITION BY id ORDER BY ts) keep FROM order_input")
                            .noLeakCheck().fails(11, "does not support PARTITION BY");
                    assertQuery(prefix + "ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) keep FROM order_input")
                            .noLeakCheck().fails(11, "does not support framing");
                    assertQuery(prefix + "ORDER BY reverse_id) keep FROM order_input")
                            .noLeakCheck().fails(11, "requires the timestamp argument in ascending ORDER BY order");
                    assertQuery(prefix + "ORDER BY ts) keep FROM order_input")
                            .noLeakCheck().expectSize().returns("id\tkeep\n1\ttrue\n2\tfalse\n3\tfalse\n4\ttrue\n");
                }
            }
        });
    }

    @Test
    public void testGapTimestampConversionDiagnosticsThenReuse() throws Exception {
        assertMemoryLeak(() -> {
            final String suffix = ") OVER (ORDER BY x) keep FROM long_sequence(4)";
            final String valid = "SELECT x, lttb(x, x::DOUBLE, 2, '1s'" + suffix;
            for (int cache = 0; cache < 2; cache++) {
                setProperty(io.questdb.PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, cache == 1 ? "true" : "false");
                final ObjList<String> unsupported = new ObjList<>();
                unsupported.add("true");
                unsupported.add("x::BYTE");
                unsupported.add("x::SHORT");
                unsupported.add("x::FLOAT");
                unsupported.add("x::DOUBLE");
                unsupported.add("x::CHAR");
                for (int i = 0; i < unsupported.size(); i++) {
                    assertQuery("SELECT x, lttb(" + unsupported.getQuick(i) + ", x::DOUBLE, 2, '1s'" + suffix)
                            .noLeakCheck().fails(10, "there is no matching function `lttb`");
                    assertQuery(valid).noLeakCheck().expectSize().returns("x\tkeep\n1\ttrue\n2\tfalse\n3\tfalse\n4\ttrue\n");
                }
                assertQuery("SELECT x, lttb(NULL, x::DOUBLE, 2, '1s'" + suffix)
                        .noLeakCheck().fails(10, "does not support an untyped NULL argument; cast it to a concrete type");
                assertQuery(valid).noLeakCheck().expectSize().returns("x\tkeep\n1\ttrue\n2\tfalse\n3\tfalse\n4\ttrue\n");
                final String overflow = "SELECT x, lttb(x, x::DOUBLE, 2, '9223372036855s'" + suffix;
                assertQuery(overflow).noLeakCheck().fails(overflow.indexOf("'"), "gap threshold overflow");
                assertQuery(valid).noLeakCheck().expectSize().returns("x\tkeep\n1\ttrue\n2\tfalse\n3\tfalse\n4\ttrue\n");
                final String competing = "SELECT x, lttb(x, x::DOUBLE, 1, '0s'" + suffix;
                assertQuery(competing).noLeakCheck().fails(competing.indexOf("1,"), "target points must be at least 2");
                assertQuery(valid).noLeakCheck().expectSize().returns("x\tkeep\n1\ttrue\n2\tfalse\n3\tfalse\n4\ttrue\n");
            }
        });
    }

    private void assertSmallInput(String expression, int target, String expected) throws Exception {
        assertQuery("SELECT id, lttb(" + expression + ", v, " + target + ", '1s') OVER (ORDER BY id) keep FROM small_input")
                .noLeakCheck().expectSize().columnType(0, io.questdb.cairo.ColumnType.INT)
                .columnType(1, io.questdb.cairo.ColumnType.BOOLEAN).returns(expected);
    }

    private static ObjList<String> implicitTimestampExpressions() {
        final ObjList<String> expressions = new ObjList<>();
        expressions.add("t");
        expressions.add("(t / 1000)::DATE");
        expressions.add("t::INT");
        expressions.add("t::TIMESTAMP::STRING");
        expressions.add("t::TIMESTAMP::VARCHAR");
        expressions.add("t::TIMESTAMP::STRING::SYMBOL");
        return expressions;
    }

    private static void setTimestampBind(io.questdb.cairo.sql.BindVariableService binds, int type, boolean isNull) throws SqlException {
        final long value = isNull ? io.questdb.std.Numbers.LONG_NULL : 1;
        switch (type) {
            case 0 -> binds.setLong(0, value);
            case 1 -> binds.setDate(0, value);
            case 2 -> binds.setTimestamp(0, value);
            case 3 -> binds.setTimestampNano(0, value);
            case 4 -> binds.setStr(0, isNull ? null : "1970-01-01T00:00:00.000000001Z");
            case 5 -> binds.setVarchar(0, isNull ? null : new io.questdb.std.str.Utf8String("1970-01-01T00:00:00.000000001Z"));
            default -> throw new AssertionError(type);
        }
    }

    @Test
    public void testGapImplicitLongTimestamp() throws Exception {
        assertTimestampConversion("ts::LONG", true);
    }

    @Test
    public void testGapImplicitDateTimestamp() throws Exception {
        assertTimestampConversion("ts::DATE", true);
    }

    @Test
    public void testGapImplicitIntTimestamp() throws Exception {
        assertTimestampConversion("ts::LONG::INT", true);
    }

    @Test
    public void testGapImplicitStringTimestamp() throws Exception {
        assertTimestampConversion("ts::STRING", true);
    }

    @Test
    public void testGapImplicitVarcharTimestamp() throws Exception {
        assertTimestampConversion("ts::VARCHAR", true);
    }

    @Test
    public void testGapImplicitSymbolTimestamp() throws Exception {
        assertTimestampConversion("ts::STRING::SYMBOL", true);
    }

    @Test
    public void testGapExplicitTimestampConversionControls() throws Exception {
        assertTimestampConversion("ts::LONG::TIMESTAMP", true);
        assertTimestampConversion("ts::DATE::TIMESTAMP", true);
        assertTimestampConversion("ts::TIMESTAMP_NS", true);
        assertTimestampConversion("ts::STRING::TIMESTAMP_NS", true);
    }

    @Test
    public void testNoGapImplicitTimestampConversionControls() throws Exception {
        assertTimestampConversion("ts::LONG", false);
        assertTimestampConversion("ts::DATE", false);
        assertTimestampConversion("ts::LONG::INT", false);
        assertTimestampConversion("ts::STRING", false);
        assertTimestampConversion("ts::VARCHAR", false);
        assertTimestampConversion("ts::STRING::SYMBOL", false);
    }

    private void assertTimestampConversion(String timestampExpression, boolean hasGap) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE conversion_input (id INT, ts TIMESTAMP, v DOUBLE) TIMESTAMP(ts)");
            execute("INSERT INTO conversion_input VALUES (1, 0, 1), (2, 1_000_000, 2), (3, 2_000_000, 3), " +
                    "(4, 10_000_000, 4), (5, 11_000_000, 5), (6, 12_000_000, 6)");
            for (int cache = 0; cache < 2; cache++) {
                final boolean isLight = cache == 1;
                setProperty(io.questdb.PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, Boolean.toString(isLight));
                for (int order = 0; order < 2; order++) {
                    final String orderColumn = order == 0 ? "ts" : "id";
                    final String sql = "SELECT id, lttb(" + timestampExpression + ", v, 2" +
                            (hasGap ? ", '1s'" : "") + ") OVER (ORDER BY " + orderColumn + ") keep FROM conversion_input";
                    // Adjacent points exactly 1s apart stay together; only the 8s hole splits.
                    // A DATE getter returns micros, whereas STRING/VARCHAR/SYMBOL getters return nanos.
                    assertQuery(sql).noLeakCheck().expectSize()
                            .withPlanContaining(isLight ? "CachedWindowLight" : "CachedWindow\n",
                                    order == 0 ? "unorderedFunctions" : "orderedFunctions")
                            .returns("id\tkeep\n1\ttrue\n2\tfalse\n3\t" + hasGap +
                                    "\n4\t" + hasGap + "\n5\tfalse\n6\ttrue\n");
                }
            }
            execute("DROP TABLE conversion_input");
        });
    }

    @Test
    public void testImplicitTimestampNullControls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE nullable_input (id INT, t LONG, v DOUBLE)");
            execute("INSERT INTO nullable_input VALUES (1, NULL, 1), (2, 0, NULL), (3, 1_000_000, 3), (4, 2_000_000, 4)");
            final ObjList<String> expressions = new ObjList<>();
            expressions.add("t");
            expressions.add("(t / 1000)::DATE");
            expressions.add("t::INT");
            expressions.add("t::TIMESTAMP::STRING");
            expressions.add("t::TIMESTAMP::VARCHAR");
            expressions.add("t::TIMESTAMP::STRING::SYMBOL");
            for (int cache = 0; cache < 2; cache++) {
                setProperty(io.questdb.PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, cache == 1 ? "true" : "false");
                for (int i = 0; i < expressions.size(); i++) {
                    final String expression = expressions.getQuick(i);
                    assertQuery("SELECT id, lttb(" + expression + ", v, 2) OVER (ORDER BY id) keep FROM nullable_input")
                            .noLeakCheck().expectSize().returns("id\tkeep\n1\tfalse\n2\tfalse\n3\ttrue\n4\ttrue\n");
                    assertQuery("SELECT id, lttb(" + expression + "::TIMESTAMP, v, 2, '1s') OVER (ORDER BY id) keep FROM nullable_input")
                            .noLeakCheck().expectSize().returns("id\tkeep\n1\tfalse\n2\tfalse\n3\ttrue\n4\ttrue\n");
                }
            }
        });
    }

    @Test
    public void testTimestampConversionErrorThenReuseControls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE error_input (id INT, t LONG, v DOUBLE)");
            execute("INSERT INTO error_input VALUES (1, 0, 1), (2, 1_000_000, 2), (3, 2_000_000, 3), (4, 3_000_000, 4)");
            for (int cache = 0; cache < 2; cache++) {
                setProperty(io.questdb.PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, cache == 1 ? "true" : "false");
                final String prefix = "SELECT id, lttb(t, v, 2, ";
                assertQuery(prefix + "'0s') OVER (ORDER BY id) FROM error_input")
                        .noLeakCheck().fails(prefix.length(), "gap threshold must be greater than zero");
                assertQuery(prefix + "NULL::STRING) OVER (ORDER BY id) FROM error_input")
                        .noLeakCheck().fails(prefix.length() + 4, "gap threshold must be a string constant");
                assertQuery(prefix + "'1q') OVER (ORDER BY id) FROM error_input")
                        .noLeakCheck().fails(prefix.length() + 2, "unsupported interval unit: q");
                assertQuery("SELECT id, lttb(true, v, 2, '1s') OVER (ORDER BY id) FROM error_input")
                        .noLeakCheck().fails(11, "there is no matching function `lttb`");
                final ObjList<BindVarTuple> cases = new ObjList<>();
                cases.add(BindVarTuple.ok("keep all", "id\tkeep\n1\ttrue\n2\ttrue\n3\ttrue\n4\ttrue\n",
                        binds -> binds.setLong(0, 8)));
                final String sql = "SELECT id, lttb(t::DATE::TIMESTAMP, v, $1, '1h') OVER (ORDER BY id) keep FROM error_input";
                cases.add(BindVarTuple.fails("invalid target", sql.indexOf("$1"), "target points must be at least 2",
                        binds -> binds.setLong(0, 1)));
                cases.add(BindVarTuple.ok("reuse after failure", "id\tkeep\n1\ttrue\n2\tfalse\n3\tfalse\n4\ttrue\n",
                        binds -> binds.setLong(0, 2)));
                assertQuery(sql).noLeakCheck().expectSize().assertBinds(cases);
            }
        });
    }

    @Test
    public void testGapLongSequenceTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            final String expected = "x\tlttb\n1\ttrue\n2\ttrue\n3\tfalse\n4\tfalse\n5\tfalse\n" +
                    "6\tfalse\n7\tfalse\n8\tfalse\n9\tfalse\n10\ttrue\n";
            assertQuery("SELECT x, lttb(x::TIMESTAMP, x::DOUBLE, 3, '1h') OVER (ORDER BY x) FROM long_sequence(10)")
                    .noLeakCheck().expectSize().returns(expected);
            assertQuery("SELECT x, lttb(x, x::DOUBLE, 3) OVER (ORDER BY x) FROM long_sequence(10)")
                    .noLeakCheck().expectSize().returns(expected);
            assertQuery("SELECT x, lttb(x, x::DOUBLE, 3, '1h') OVER (ORDER BY x) FROM long_sequence(10)")
                    .noLeakCheck().expectSize().returns(expected);
        });
    }

    @Test
    public void testGapDateSequenceTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            final String expected = "x\tlttb\n1\ttrue\n2\ttrue\n3\tfalse\n4\tfalse\n5\tfalse\n" +
                    "6\tfalse\n7\tfalse\n8\tfalse\n9\tfalse\n10\ttrue\n";
            assertQuery("SELECT x, lttb(x::DATE::TIMESTAMP, x::DOUBLE, 3, '1h') OVER (ORDER BY x) FROM long_sequence(10)")
                    .noLeakCheck().expectSize().returns(expected);
            assertQuery("SELECT x, lttb(x::DATE, x::DOUBLE, 3) OVER (ORDER BY x) FROM long_sequence(10)")
                    .noLeakCheck().expectSize().returns(expected);
            assertQuery("SELECT x, lttb(x::DATE, x::DOUBLE, 3, '1h') OVER (ORDER BY x) FROM long_sequence(10)")
                    .noLeakCheck().expectSize().returns(expected);
        });
    }

    @Test
    public void testBindVariableTarget() throws Exception {
        // lttb(ts, value, target) accepts a runtime-constant (bind-variable) target, read PER-EXECUTION:
        // the SAME compiled factory produces keep-all vs downsampled keep-sets as $1 is re-bound between
        // executions, and a runtime out-of-range target throws at cursor-open (not compile).
        final ObjList<BindVarTuple> cases = new ObjList<>();
        // $1 = 8 over 6 rows: count(6) <= target(8) -> keep all (selectAll short-circuit).
        cases.add(BindVarTuple.ok(
                "target 8 (keep all)",
                """
                        ts\tv\tkeep
                        1970-01-01T00:00:00.000001Z\t10.0\ttrue
                        1970-01-01T00:00:00.000002Z\t20.0\ttrue
                        1970-01-01T00:00:00.000003Z\t30.0\ttrue
                        1970-01-01T00:00:00.000004Z\t40.0\ttrue
                        1970-01-01T00:00:00.000005Z\t50.0\ttrue
                        1970-01-01T00:00:00.000006Z\t60.0\ttrue
                        """,
                bindVariableService -> bindVariableService.setLong(0, 8)
        ));
        // Re-bind $1 = 2 on the same compiled factory: count(6) > 2 -> LTTB with no interior buckets
        // keeps only first (row 1) and last (row 6). A different result from the keep-all case above
        // proves the target is read at execution, not frozen at compile.
        cases.add(BindVarTuple.ok(
                "target 2 (re-bind, first+last only)",
                """
                        ts\tv\tkeep
                        1970-01-01T00:00:00.000001Z\t10.0\ttrue
                        1970-01-01T00:00:00.000002Z\t20.0\tfalse
                        1970-01-01T00:00:00.000003Z\t30.0\tfalse
                        1970-01-01T00:00:00.000004Z\t40.0\tfalse
                        1970-01-01T00:00:00.000005Z\t50.0\tfalse
                        1970-01-01T00:00:00.000006Z\t60.0\ttrue
                        """,
                bindVariableService -> bindVariableService.setLong(0, 2)
        ));
        // Re-bind $1 = 1: out-of-range detected at cursor-open (range validation moved from
        // newInstance to per-execution init), same message/position as a constant would produce.
        cases.add(BindVarTuple.fails(
                "target 1 (runtime out of range)",
                26,
                "target points must be at least 2",
                bindVariableService -> bindVariableService.setLong(0, 1)
        ));

        assertQuery("select ts, v, lttb(ts, v, $1) over (order by ts) keep from t")
                .ddl("create table t (ts timestamp, v double) timestamp(ts)",
                        "insert into t select x::timestamp, x*10 from long_sequence(6)")
                .timestamp("ts")
                .expectSize()
                .assertBinds(cases);
    }

    @Test
    public void testConstantTargetOutOfRangeFailsAtCompileTime() throws Exception {
        // Fix 2: a constant target's range is validated at newInstance (compile time), matching the
        // pre-bind-var-support factory and the legacy SUBSAMPLE cursor's own constant handling - not
        // deferred to cursor-open. select(...) below only compiles the query (it never calls
        // factory.getCursor(...)), so a thrown SqlException here proves the failure happened during
        // compilation, not execution.
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v double) timestamp(ts)");
            try {
                select("select ts, v, lttb(ts, v, 1) over (order by ts) keep from t");
                Assert.fail("expected compilation to fail for an out-of-range constant target");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "target points must be at least 2");
                Assert.assertEquals(26, e.getPosition());
            }
        });
    }

    @Test
    public void testKeepsAllWhenFewRows() throws Exception {
        // n=3, target=8 -> count <= target: the base's keepAll short-circuit fires before
        // LttbAlgorithm.select ever runs, so all rows are kept regardless of triangle areas.
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v double) timestamp(ts)");
            execute("insert into t values (1::timestamp,10.0),(2::timestamp,20.0),(3::timestamp,30.0)");
            assertQuery("select ts, v, lttb(ts, v, 8) over (order by ts) keep from t")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tv\tkeep
                            1970-01-01T00:00:00.000001Z\t10.0\ttrue
                            1970-01-01T00:00:00.000002Z\t20.0\ttrue
                            1970-01-01T00:00:00.000003Z\t30.0\ttrue
                            """);
        });
    }

    @Test
    public void testKeepsAllRowsWhenCountAtTargetEvenIfBucketingWouldDrop() throws Exception {
        // Distinguishing case for the count <= target keep-all short-circuit: 4 monotonically
        // increasing rows with target=4. Plain LTTB bucketing (numBuckets=2 -> first, one point per
        // interior bucket, last) would likely keep all 4 anyway here, but the point of this test is
        // that bufferCount(4) <= target(4) takes the selectAll() path unconditionally, matching the
        // captured legacy selectAll behavior - not that LTTB's own math happens to agree.
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v double) timestamp(ts)");
            execute("insert into t values (1::timestamp,10.0),(2::timestamp,20.0),(3::timestamp,30.0),(4::timestamp,40.0)");
            assertQuery("select ts, v, lttb(ts, v, 4) over (order by ts) keep from t")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tv\tkeep
                            1970-01-01T00:00:00.000001Z\t10.0\ttrue
                            1970-01-01T00:00:00.000002Z\t20.0\ttrue
                            1970-01-01T00:00:00.000003Z\t30.0\ttrue
                            1970-01-01T00:00:00.000004Z\t40.0\ttrue
                            """);
            // Captured legacy behavior: the clause keeps all rows at the exact target.
            assertQuery("select ts, v from t SUBSAMPLE lttb(v, 4)")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tv
                            1970-01-01T00:00:00.000001Z\t10.0
                            1970-01-01T00:00:00.000002Z\t20.0
                            1970-01-01T00:00:00.000003Z\t30.0
                            1970-01-01T00:00:00.000004Z\t40.0
                            """);
        });
    }

    @Test
    public void testKeepsNanoTimestampPrecision() throws Exception {
        // double ulp near a 2024 nanosecond epoch (~1.7e18) is 256ns, so LttbAlgorithm must
        // compute triangle areas from long timestamp differences rather than absolute epochs
        // converted to double. Candidates sit 1ns apart with exact areas 4, 400, 8: the
        // algorithm must keep id 2, not fall back to the first candidate on an all-zero tie.
        assertMemoryLeak(() -> {
            execute("create table t (id int, v double, ts timestamp_ns) timestamp(ts)");
            execute("""
                    insert into t values
                    (0, 0.0, '2024-01-01T00:00:00.000000000Z'),
                    (1, 1.0, '2024-01-01T00:00:00.000000001Z'),
                    (2, 100.0, '2024-01-01T00:00:00.000000002Z'),
                    (3, 2.0, '2024-01-01T00:00:00.000000003Z'),
                    (4, 0.0, '2024-01-01T00:00:00.000000004Z')
                    """);
            assertQuery("select id, lttb(ts, v, 3) over (order by ts) keep from t")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tkeep
                            0\ttrue
                            1\tfalse
                            2\ttrue
                            3\tfalse
                            4\ttrue
                            """);
        });
    }

    @Test
    public void testMatchesLttbAlgorithmOnTenPoints() throws Exception {
        // Same dataset as SubsampleTest.testLttbBasic (10 points -> target 5): first and last are
        // always kept, plus one point per interior bucket chosen by largest triangle area. Expected
        // keep flags filled from the captured legacy golden for the same dataset.
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v double) timestamp(ts)");
            execute("""
                    insert into t values
                    ('2024-01-01T00:00:00.000000Z'::timestamp, 10.0),
                    ('2024-01-01T01:00:00.000000Z'::timestamp, 20.0),
                    ('2024-01-01T02:00:00.000000Z'::timestamp, 50.0),
                    ('2024-01-01T03:00:00.000000Z'::timestamp, 30.0),
                    ('2024-01-01T04:00:00.000000Z'::timestamp, 15.0),
                    ('2024-01-01T05:00:00.000000Z'::timestamp, 45.0),
                    ('2024-01-01T06:00:00.000000Z'::timestamp, 25.0),
                    ('2024-01-01T07:00:00.000000Z'::timestamp, 35.0),
                    ('2024-01-01T08:00:00.000000Z'::timestamp, 5.0),
                    ('2024-01-01T09:00:00.000000Z'::timestamp, 40.0)
                    """);
            assertQuery("select ts, v from (select ts, v, lttb(ts, v, 5) over (order by ts) keep from t) where keep")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize(false)
                    .returns("""
                            ts\tv
                            2024-01-01T00:00:00.000000Z\t10.0
                            2024-01-01T02:00:00.000000Z\t50.0
                            2024-01-01T04:00:00.000000Z\t15.0
                            2024-01-01T08:00:00.000000Z\t5.0
                            2024-01-01T09:00:00.000000Z\t40.0
                            """);

            // Captured legacy golden, asserted through the sole window-only clause path.
            assertQuery("select ts, v from t SUBSAMPLE lttb(v, 5)")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tv
                            2024-01-01T00:00:00.000000Z\t10.0
                            2024-01-01T02:00:00.000000Z\t50.0
                            2024-01-01T04:00:00.000000Z\t15.0
                            2024-01-01T08:00:00.000000Z\t5.0
                            2024-01-01T09:00:00.000000Z\t40.0
                            """);
        });
    }

    @Test
    public void testGapPreservingSplitsSegments() throws Exception {
        // Same dataset as SubsampleTest.testLttbGapPreserving: an 4.5h gap between 00:30 and 05:00
        // with threshold '1h' splits the data into two 4-row segments, each budgeted 2 of the 4 target
        // points (proportional split) -> first+last of each segment.
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v double) timestamp(ts)");
            execute("""
                    insert into t values
                    ('2024-01-01T00:00:00.000000Z'::timestamp, 10.0),
                    ('2024-01-01T00:10:00.000000Z'::timestamp, 20.0),
                    ('2024-01-01T00:20:00.000000Z'::timestamp, 30.0),
                    ('2024-01-01T00:30:00.000000Z'::timestamp, 40.0),
                    ('2024-01-01T05:00:00.000000Z'::timestamp, 50.0),
                    ('2024-01-01T05:10:00.000000Z'::timestamp, 60.0),
                    ('2024-01-01T05:20:00.000000Z'::timestamp, 70.0),
                    ('2024-01-01T05:30:00.000000Z'::timestamp, 80.0)
                    """);
            assertQuery("select ts, v, lttb(ts, v, 4, '1h') over (order by ts) keep from t")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tv\tkeep
                            2024-01-01T00:00:00.000000Z\t10.0\ttrue
                            2024-01-01T00:10:00.000000Z\t20.0\tfalse
                            2024-01-01T00:20:00.000000Z\t30.0\tfalse
                            2024-01-01T00:30:00.000000Z\t40.0\ttrue
                            2024-01-01T05:00:00.000000Z\t50.0\ttrue
                            2024-01-01T05:10:00.000000Z\t60.0\tfalse
                            2024-01-01T05:20:00.000000Z\t70.0\tfalse
                            2024-01-01T05:30:00.000000Z\t80.0\ttrue
                            """);

            // Captured legacy gap-preserving golden, asserted through the window-only clause path.
            assertQuery("select ts, v from t SUBSAMPLE lttb(v, 4, '1h')")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tv
                            2024-01-01T00:00:00.000000Z\t10.0
                            2024-01-01T00:30:00.000000Z\t40.0
                            2024-01-01T05:00:00.000000Z\t50.0
                            2024-01-01T05:30:00.000000Z\t80.0
                            """);
        });
    }

    @Test
    public void testGapPreservingNoGapsFallsBackToPlainLttb() throws Exception {
        // Same dataset as SubsampleTest.testLttbGapPreservingNoGaps: no gap exceeds the '2h'
        // threshold, so one segment covers all 5 rows - same result as plain lttb(ts, v, 2).
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v double) timestamp(ts)");
            execute("""
                    insert into t values
                    ('2024-01-01T00:00:00.000000Z'::timestamp, 10.0),
                    ('2024-01-01T01:00:00.000000Z'::timestamp, 50.0),
                    ('2024-01-01T02:00:00.000000Z'::timestamp, 20.0),
                    ('2024-01-01T03:00:00.000000Z'::timestamp, 30.0),
                    ('2024-01-01T04:00:00.000000Z'::timestamp, 40.0)
                    """);
            assertQuery("select ts, v, lttb(ts, v, 2, '2h') over (order by ts) keep from t")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tv\tkeep
                            2024-01-01T00:00:00.000000Z\t10.0\ttrue
                            2024-01-01T01:00:00.000000Z\t50.0\tfalse
                            2024-01-01T02:00:00.000000Z\t20.0\tfalse
                            2024-01-01T03:00:00.000000Z\t30.0\tfalse
                            2024-01-01T04:00:00.000000Z\t40.0\ttrue
                            """);
        });
    }

    @Test
    public void testFiltersNullAndNaNRows() throws Exception {
        // A NULL/NaN value must be dropped before bucketing, exactly like m4/minmax: the old SUBSAMPLE
        // SUBSAMPLE drops NULL ts / null-or-NaN value rows
        // before ever handing the buffer to the algorithm. Non-null count(3) <= target(4) here, so this
        // also exercises the keepAll short-circuit on top of the null filter (matches
        // M4WindowFunctionTest.testFiltersNullAndNaNRows's shape, function swapped to lttb).
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v double) timestamp(ts)");
            execute("""
                    insert into t values
                    (1::timestamp, null),
                    (2::timestamp, 5.0),
                    (3::timestamp, 100.0),
                    (4::timestamp, 1.0)
                    """);
            assertQuery("select ts, v, lttb(ts, v, 4) over (order by ts) keep from t")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tv\tkeep
                            1970-01-01T00:00:00.000001Z\tnull\tfalse
                            1970-01-01T00:00:00.000002Z\t5.0\ttrue
                            1970-01-01T00:00:00.000003Z\t100.0\ttrue
                            1970-01-01T00:00:00.000004Z\t1.0\ttrue
                            """);

            // Captured legacy NULL-filter golden, asserted through the window-only clause path.
            assertQuery("select ts, v from t SUBSAMPLE lttb(v, 4)")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tv
                            1970-01-01T00:00:00.000002Z\t5.0
                            1970-01-01T00:00:00.000003Z\t100.0
                            1970-01-01T00:00:00.000004Z\t1.0
                            """);
        });
    }

    @Test
    public void testRejectsNonNumericValue() throws Exception {
        // SYMBOL is not implicitly castable to DOUBLE, so the overload resolver itself rejects this
        // before newInstance() ever runs, like M4WindowFunctionTest.testRejectsNonNumericValue - but
        // unlike m4 (a single 3-arg signature), lttb has two candidate signatures (3-arg and 4-arg
        // gap overload); with neither matching, FunctionParser can't pin the mismatch to one specific
        // argument and falls back to its generic "no matching function" diagnostic instead of the
        // single-candidate "argument type mismatch" message.
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, s symbol) timestamp(ts)");
            assertQuery("select ts, lttb(ts, s, 8) over (order by ts) from t")
                    .noLeakCheck()
                    .fails(11, "there is no matching function `lttb` with the argument types: (TIMESTAMP, SYMBOL, INT)");
        });
    }

    @Test
    public void testRejectsNonNumericValueThatOverloadResolutionLetsThrough() throws Exception {
        // CHAR is implicitly widenable to DOUBLE per ColumnType's overload rules, so it reaches
        // newInstance() and exercises our manual numeric-type guard and its SUBSAMPLE-cursor-matching
        // message - matches M4WindowFunctionTest's equivalent test.
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, c char) timestamp(ts)");
            assertQuery("select ts, lttb(ts, c, 8) over (order by ts) from t")
                    .noLeakCheck()
                    .fails(20, "numeric column expected, got: CHAR");
        });
    }

    @Test
    public void testRejectsNonConstantTarget() throws Exception {
        // lttb's target is uppercase 'L' in the signature ("lttb(NDL)", widened from the former
        // constant-only 'l' so a bind-variable target can reach newInstance), so - exactly like m4 -
        // a non-constant column target reaches newInstance0 and gets the friendly accept-check message
        // rather than FunctionParser's generic "no matching function" diagnostic.
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v double) timestamp(ts)");
            assertQuery("select ts, lttb(ts, v, v::long) over (order by ts) from t")
                    .noLeakCheck()
                    .fails(24, "target must be a constant or bind variable");
        });
    }

    @Test
    public void testRejectsTargetBelowTwo() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v double) timestamp(ts)");
            assertQuery("select ts, lttb(ts, v, 1) over (order by ts) from t")
                    .noLeakCheck()
                    .fails(23, "target points must be at least 2");
        });
    }

    @Test
    public void testGapRejectsInvalidUnit() throws Exception {
        // Matches SubsampleTest.testLttbGapInvalidUnit's message; the gap arg is constant-enforced by
        // the signature ("lttb(NDls)"), so a malformed constant string reaches newInstance and this
        // factory's own parseGapThreshold reproduces the old cursor's error exactly.
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v double) timestamp(ts)");
            assertQuery("select ts, lttb(ts, v, 5, '1M') over (order by ts) from t")
                    .noLeakCheck()
                    .fails(28, "unsupported interval unit");
        });
    }

    @Test
    public void testExplainPlan() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v double) timestamp(ts)");
            assertQuery("select ts, lttb(ts, v, 8) over (order by ts) from t")
                    .noLeakCheck()
                    .assertsPlan("CachedWindowLight\n" +
                            """
                                      unorderedFunctions: [lttb(ts,v,8) over (order by [ts])]
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: t
                                    """);
        });
    }

    @Test
    public void testExplainPlanWithBindVariableAndGap() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v DOUBLE) TIMESTAMP(ts)");
            bindVariableService.setLong(0, 8);
            assertQuery("SELECT ts, lttb(ts, v, $1, '1h') OVER (ORDER BY ts) FROM t")
                    .noLeakCheck()
                    .assertsPlan("CachedWindowLight\n" +
                            """
                                      unorderedFunctions: [lttb(ts,v,$0::long,'1h') over (order by [ts])]
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: t
                                    """);
        });
    }

    @Test
    public void testExplainPlanWithGap() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v double) timestamp(ts)");
            final ObjList<String> gaps = new ObjList<>();
            gaps.add("1h");
            gaps.add("30s");
            for (int i = 0; i < gaps.size(); i++) {
                final String gap = gaps.getQuick(i);
                assertQuery("SELECT ts, lttb(ts, v, 8, '" + gap + "') OVER (ORDER BY ts) FROM t")
                        .noLeakCheck()
                        .assertsPlan("CachedWindowLight\n" +
                                "  unorderedFunctions: [lttb(ts,v,8,'" + gap + "') over (order by [ts])]\n" +
                                """
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: t
                                        """);
            }
        });
    }

    @Test
    public void testLongBeyondDoublePrecisionGeometryUnchanged() throws Exception {
        // F2-M4-LONG preservation control (green pre-fix, must stay green): lttb shares
        // BucketSelectWindowFunction's buffer with m4/minmax, but its selection contract is
        // geometric (largest triangle area, computed in double by design), NOT exact extremum
        // comparison. LONG values distinct only beyond double precision (2^53 vs 2^53 + 1)
        // therefore tie at area 0 and the first candidate of the interior bucket wins - a
        // documented consequence of the double-domain area heuristic. This pins that behavior:
        // the integral-exactness repair to minmax/m4 must leave lttb's selection bit-identical.
        // n=5, m=3, plain LTTB path: first (row 1) and last (row 5) pinned; interior bucket
        // [rows 2..4] all collapse to the same double, every area is 0, first candidate row 2 wins.
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v long) timestamp(ts)");
            execute("""
                    insert into t values
                    (1::timestamp, 9_007_199_254_740_992),
                    (2::timestamp, 9_007_199_254_740_993),
                    (3::timestamp, 9_007_199_254_740_992),
                    (4::timestamp, 9_007_199_254_740_992),
                    (5::timestamp, 9_007_199_254_740_992)
                    """);
            assertQuery("select ts, v, lttb(ts, v, 3) over (order by ts) keep from t")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tv\tkeep
                            1970-01-01T00:00:00.000001Z\t9007199254740992\ttrue
                            1970-01-01T00:00:00.000002Z\t9007199254740993\ttrue
                            1970-01-01T00:00:00.000003Z\t9007199254740992\tfalse
                            1970-01-01T00:00:00.000004Z\t9007199254740992\tfalse
                            1970-01-01T00:00:00.000005Z\t9007199254740992\ttrue
                            """);
        });
    }
}
