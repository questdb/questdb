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
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.BindVariableService;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.FunctionFactoryDescriptor;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class SubsampleSdtDiagnosticTest extends AbstractCairoTest {
    private static final String PREFIX = "SELECT ts, v FROM t SUBSAMPLE ";
    private static final String ROWS = """
            ts\tv
            2024-01-01T00:00:00.000000Z\t1.0
            2024-01-01T00:00:01.000000Z\t2.0
            """;
    private static final String SHAPE = "SUBSAMPLE sdt requires a constant, non-negative finite compdev";

    private static void assertError(String suffixWithCaret, String message) throws Exception {
        int caret = suffixWithCaret.indexOf('^');
        Assert.assertTrue(caret >= 0);
        String sql = PREFIX + suffixWithCaret.replace("^", "");
        try (RecordCursorFactory ignored = select(sql)) {
            Assert.fail("expected failure: " + sql);
        } catch (SqlException e) {
            Assert.assertEquals(sql, message, e.getFlyweightMessage().toString());
            Assert.assertEquals(sql, PREFIX.length() + caret, e.getPosition());
        }
    }

    private static void assertMatchesUniformError(String expression) throws Exception {
        String uniform = PREFIX + "uniform(" + expression + ")";
        String message;
        int expressionPosition;
        try (RecordCursorFactory ignored = select(uniform)) {
            throw new AssertionError("expected parser error: " + uniform);
        } catch (SqlException e) {
            message = e.getFlyweightMessage().toString();
            expressionPosition = e.getPosition() - (PREFIX.length() + "uniform(".length());
        }
        Assert.assertTrue(expressionPosition >= 0 && expressionPosition < expression.length());
        assertError("sdt(v, " + expression.substring(0, expressionPosition) + "^" + expression.substring(expressionPosition) + ")", message);
    }

    private static void createTable() throws SqlException {
        execute("CREATE TABLE t (ts TIMESTAMP, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
        execute("INSERT INTO t VALUES ('2024-01-01T00:00:00Z', 1.0), ('2024-01-01T00:00:01Z', 2.0)");
    }

    @Test
    public void testUnknownFunction() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertError("sdt(v, ^nosuchfn(1))", "unknown function name: nosuchfn(INT)");
        });
    }

    @Test
    public void testNestedUnknownFunction() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertError("sdt(v, abs(^nosuchfn(1)))", "unknown function name: nosuchfn(INT)");
        });
    }

    @Test
    public void testWrongArity() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertMatchesUniformError("abs(1, 2)");
        });
    }

    @Test
    public void testWrongType() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertMatchesUniformError("abs(true)");
        });
    }

    @Test
    public void testInvalidCast() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertMatchesUniformError("abs('not-a-uuid'::UUID)");
        });
    }

    @Test
    public void testControlCombinedInvalidFunctionFirst() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            // The outer column independently violates SDT's constant-only shape, even if
            // FunctionParser encounters the unknown function first.
            assertError("sdt(v, v ^+ nosuchfn(1))", SHAPE);
        });
    }

    @Test
    public void testBoundWrongTypeAtBindPosition() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            bindVariableService.setBoolean("flag", true);
            // A bind-position diagnostic can come from overload resolution, not bind lookup.
            assertMatchesUniformError("sin(:flag)");
        });
    }

    @Test
    public void testControlDeclaredBindSubqueryError() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            String declaration = "DECLARE @dev := :missing ";
            String expression = "@dev + (SELECT @dev)";
            String uniform = declaration + PREFIX + "uniform(" + expression + ")";
            String message;
            int position;
            try (RecordCursorFactory ignored = select(uniform)) {
                throw new AssertionError("expected undefined bind");
            } catch (SqlException e) {
                message = e.getFlyweightMessage().toString();
                position = e.getPosition();
                Assert.assertEquals(declaration.indexOf(":missing"), position);
                Assert.assertEquals("undefined bind variable: :missing", message);
            }
            String sdt = declaration + PREFIX + "sdt(v, " + expression + ")";
            try (RecordCursorFactory ignored = select(sdt)) {
                Assert.fail("expected undefined bind");
            } catch (SqlException e) {
                // DECLARE reuses the same definition position in both scopes. The outer
                // missing bind independently makes this an invalid SDT compdev shape.
                Assert.assertEquals(SHAPE, e.getFlyweightMessage().toString());
                Assert.assertEquals(sdt.indexOf('+'), e.getPosition());
            }
        });
    }

    @Test
    public void testDeclaredBindOnlyInsideSubqueryError() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            String sql = "DECLARE @dev := :missing " + PREFIX + "sdt(v, (SELECT @dev))";
            try (RecordCursorFactory ignored = select(sql)) {
                Assert.fail("expected undefined bind");
            } catch (SqlException e) {
                Assert.assertEquals("undefined bind variable: :missing", e.getFlyweightMessage().toString());
                Assert.assertEquals(sql.indexOf(":missing"), e.getPosition());
            }
        });
    }

    @Test
    public void testControlOuterReferenceAfterSubquery() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertError("sdt(v, v ^+ (SELECT 1))", SHAPE);
            assertError("sdt(v, :missing ^+ (SELECT 1))", SHAPE);
            assertError("sdt(v, (SELECT 1) ^+ v)", SHAPE);
            assertError("sdt(v, (SELECT 1) ^+ :missing)", SHAPE);
            assertValid("0.5");
        });
    }

    @Test
    public void testMissingBindVariableService() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            ((SqlExecutionContextImpl) sqlExecutionContext).with((BindVariableService) null);
            try {
                try (RecordCursorFactory ignored = select(PREFIX + "sdt(v, :missing)")) {
                    Assert.fail("expected configuration failure");
                } catch (SqlException e) {
                    Assert.assertEquals("bind variable service is not provided", e.getFlyweightMessage().toString());
                    Assert.assertEquals(0, e.getPosition());
                }
            } finally {
                ((SqlExecutionContextImpl) sqlExecutionContext).with(bindVariableService);
                assertValid("0.5");
            }
        });
    }

    @Test
    public void testIndexedBindTypeInferenceError() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertMatchesUniformError("coalesce(1.0, $1)");
        });
    }

    @Test
    public void testControlCombinedInvalidNamedBindFunctionFirst() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertError("sdt(v, :missing ^+ nosuchfn(1))", SHAPE);
            assertError("sdt(v, $0 ^+ nosuchfn(1))", SHAPE);
        });
    }

    @Test
    public void testNestedVariadicUnknownFunction() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertError("sdt(v, coalesce(1, ^nosuchfn(1), 2))", "unknown function name: nosuchfn(INT)");
        });
    }

    @Test
    public void testSubqueryError() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertMatchesUniformError("(SELECT nosuchfn(1))");
        });
    }

    @Test
    public void testControlSubqueryColumnError() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertMatchesUniformError("(SELECT missing FROM t)");
        });
    }

    @Test
    public void testFailureFollowedByCompilerReuse() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                for (int i = 0; i < 3; i++) {
                    String sql = PREFIX + "sdt(v, abs(nosuchfn(1)))";
                    try (RecordCursorFactory ignored = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                        Assert.fail("expected compilation failure");
                    } catch (SqlException e) {
                        Assert.assertEquals("unknown function name: nosuchfn(INT)", e.getFlyweightMessage().toString());
                        Assert.assertEquals(sql.indexOf("nosuchfn"), e.getPosition());
                    } finally {
                        try (RecordCursorFactory factory = compiler.compile(PREFIX + "sdt(v, 0.5)", sqlExecutionContext).getRecordCursorFactory()) {
                            Assert.assertNotNull(factory);
                        }
                    }
                }
            }
            assertValid("0.5");
        });
    }

    @Test
    public void testUnexpectedRuntimeException() throws Exception {
        assertUnexpectedFailure(false, true);
    }

    @Test
    public void testUnexpectedError() throws Exception {
        assertUnexpectedFailure(true, true);
    }

    @Test
    public void testControlRuntimeExceptionThroughUniform() throws Exception {
        assertUnexpectedFailure(false, false);
    }

    @Test
    public void testControlErrorThroughUniform() throws Exception {
        assertUnexpectedFailure(true, false);
    }

    @Test
    public void testControlShapeAndReuse() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertError("sdt(v, ^v)", SHAPE);
            assertError("sdt(v, ^abs(v))", SHAPE);
            assertError("sdt(v, ^coalesce(1, v, 2))", SHAPE);
            assertError("sdt(v, ^abs(:missing))", SHAPE);
            assertError("sdt(v, ^sin(:missing))", SHAPE);
            assertError("sdt(v, ^:missing)", SHAPE);
            assertError("sdt(v, ^$1)", SHAPE);
            assertError("sdt(v, ^abs($1))", SHAPE);
            assertError("sdt(v, ^abs($0))", SHAPE);
            assertError("sdt(v, ^abs($abc))", SHAPE);
            assertError("sdt(v, (^SELECT 1))", SHAPE);
            assertError("sdt(v, ^abs($2147483648))", SHAPE);
            assertError("sdt(v, nosuchfn(1) ^+ v)", SHAPE);
            assertError("sdt(v, nosuchfn(1) ^+ :missing)", SHAPE);
            bindVariableService.setDouble(0, 0.5);
            bindVariableService.setDouble("dev", 0.5);
            assertError("sdt(v, ^$1)", SHAPE);
            assertError("sdt(v, ^abs($1))", SHAPE);
            assertError("sdt(v, ^:dev)", SHAPE);
            assertError("sdt(v, ^coalesce(1.0, :dev, 2.0))", SHAPE);
            assertValid("0.5");
        });
    }

    @Test
    public void testControlNumericBoundsAndNull() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertError("sdt(v, ^NULL)", SHAPE);
            assertError("sdt(v, NULL^::INT)", SHAPE);
            assertError("sdt(v, NULL^::LONG)", SHAPE);
            assertError("sdt(v, NULL^::DOUBLE)", SHAPE);
            assertError("sdt(v, ^-1)", SHAPE);
            assertError("sdt(v, ^'x')", SHAPE);
            assertError("sdt(v, ^true)", SHAPE);
            assertError("sdt(v, ^rnd_double())", SHAPE);
            assertError("sdt(v, 'NaN'^::DOUBLE)", SHAPE);
            assertError("sdt(v, 'Infinity'^::DOUBLE)", SHAPE);
            assertError("sdt(v, 1.0^/0.0)", SHAPE);
            assertError("sdt(v, 'bad'^::DOUBLE)", SHAPE);
            assertValid("0");
            assertValid("-0.0");
            assertValid("1e-322");
            assertValid("1.7976931348623157e308");
            assertValid("abs(-0.5)");
            assertValid("1::BYTE");
            assertValid("1::SHORT");
            assertValid("1::LONG");
            assertValid("1::FLOAT");
        });
    }

    @Test
    public void testControlConstantFoldedBinds() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            bindVariableService.setBoolean(0, true);
            bindVariableService.setBoolean("flag", true);
            // AND discards the runtime bind when its other operand is constant false.
            assertValid("(false AND $1)::INT");
            assertValid("(:flag AND false)::INT");
            assertValid("(true OR :flag)::INT");
            // An undefined indexed bind can infer BOOLEAN before AND folds it away.
            assertValid("(false AND $2)::INT");
        });
    }

    @Test
    public void testControlLegacyPrecedenceFoldedBinds() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_LEGACY_OPERATOR_PRECEDENCE, "true");
        testControlConstantFoldedBinds();
    }

    @Test
    public void testLegacyPrecedenceNestedError() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_LEGACY_OPERATOR_PRECEDENCE, "true");
        testNestedUnknownFunction();
    }

    private void assertUnexpectedFailure(boolean isError, boolean isSdt) throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            final RuntimeException runtimeFailure = new IllegalStateException("sdt diagnostic runtime probe");
            final Error errorFailure = new AssertionError("sdt diagnostic error probe");
            final int[] closeCount = {0};
            final String name = "sdt_diagnostic_failure";
            ObjList<FunctionFactoryDescriptor> descriptors = new ObjList<>();
            descriptors.add(new FunctionFactoryDescriptor(new FunctionFactory() {
                @Override
                public String getSignature() {
                    return name + "()";
                }

                @Override
                public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext executionContext) {
                    return new DoubleFunction() {
                        @Override
                        public void close() {
                            closeCount[0]++;
                        }

                        @Override
                        public double getDouble(Record rec) {
                            if (isError) {
                                throw errorFailure;
                            }
                            throw runtimeFailure;
                        }

                        @Override
                        public boolean isConstant() {
                            return true;
                        }
                    };
                }
            }));
            final var factories = engine.getFunctionFactoryCache().getFactories();
            Assert.assertNull(factories.get(name));
            factories.put(name, descriptors);
            try {
                try (RecordCursorFactory ignored = select(PREFIX + (isSdt ? "sdt(v, " : "uniform(") + name + "())")) {
                    Assert.fail("expected failure");
                } catch (Throwable actual) {
                    Assert.assertSame(isError ? errorFailure : runtimeFailure, actual);
                } finally {
                    Assert.assertEquals(1, closeCount[0]);
                }
            } finally {
                factories.remove(name);
                assertValid("0.5");
            }
        });
    }

    private void assertValid(String expression) throws Exception {
        assertQuery(PREFIX + "sdt(v, " + expression + ")").timestamp("ts").returns(ROWS);
    }
}
