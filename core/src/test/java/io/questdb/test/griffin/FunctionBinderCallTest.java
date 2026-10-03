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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionParser;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.ConstantExpression;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.TypeExpression;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class FunctionBinderCallTest extends AbstractCairoTest {

    @Test
    public void testAggregateCallsMatchSqlText() throws Exception {
        assertEquivalent(
                "sum(i)",
                "sum(l)",
                "sum(b)",
                "sum(sh)",
                "count(i)",
                "count(b)",
                "count()"
        );
    }

    @Test
    public void testArithmeticOverAggregateOutputsMatchesSqlText() throws Exception {
        assertEquivalent(
                "s * 3",
                "s * 3_000_000_000",
                "s * 2.5",
                "s + c * 3",
                "s - c * 3",
                "s + c * -3",
                "d + 2",
                "2 * 3"
        );
    }

    @Test
    public void testBoundCallInstantiatesForAnotherLayout() throws Exception {
        assertMemoryLeak(() -> {
            final OutputSchema reordered = new OutputSchema().add(11, "c", ColumnType.LONG, true)
                    .add(10, "s", ColumnType.LONG, true);
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
                 FunctionBindingHarness binder = new FunctionBindingHarness(new FunctionParser(configuration, engine.getFunctionFactoryCache()))) {
                final OutputSchema input = input();
                final BoundExpression expression = bindViaApi(binder, compiler.parseExpression("s + c * 3"), input);
                try (Function function = binder.instantiate(expression, reordered, sqlExecutionContext)) {
                    binder.clear();
                    final Record record = new Record() {
                        @Override
                        public long getLong(int columnIndex) {
                            return columnIndex == 0 ? 5 : 100;
                        }
                    };
                    Assert.assertEquals(115, function.getLong(record));
                }
            }
        });
    }

    @Test
    public void testDecimalCastUsesFloatLiteralSpelling() throws Exception {
        assertMemoryLeak(() -> {
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
                 FunctionBindingHarness binder = new FunctionBindingHarness(new FunctionParser(configuration, engine.getFunctionFactoryCache()))) {
                final OutputSchema input = input();
                final ExpressionNode node = compiler.parseExpression("0.1::decimal(5,2)");
                final BoundExpression expected = binder.bind(node, input, null, sqlExecutionContext);
                final ObjList<BoundExpression> args = new ObjList<>();
                args.add(binder.bind(node.lhs, input, null, sqlExecutionContext));
                args.add(new TypeExpression().of(ColumnType.getDecimalType(5, 2), node.rhs.position));
                assertSameBinding(expected, binder.bindCall(node.token, node.position, args, input, sqlExecutionContext));
                Assert.assertTrue(expected instanceof ConstantExpression);
                Assert.assertEquals(10, ((ConstantExpression) expected).getLongValue());
            }
        });
    }

    @Test
    public void testErrorsMatchSqlText() throws Exception {
        assertMemoryLeak(() -> {
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
                 FunctionBindingHarness binder = new FunctionBindingHarness(new FunctionParser(configuration, engine.getFunctionFactoryCache()))) {
                final OutputSchema input = input();
                final String[] texts = {"bo * 3", "sum(bo)", "nosuch(i)", "s + count(i)"};
                final String[] messages = {
                        "there is no matching operator `*` with the argument types: BOOLEAN * INT",
                        "there is no matching function `sum` with the argument types: (BOOLEAN)",
                        "unknown function name: nosuch(INT)",
                        "Aggregate function cannot be passed as an argument"
                };
                final int[] positions = {3, 0, 0, 4};
                for (int i = 0; i < texts.length; i++) {
                    final ExpressionNode node = compiler.parseExpression(texts[i]);
                    final SqlException expected = bindingError(binder, node, input, false);
                    final SqlException actual = bindingError(binder, node, input, true);
                    TestUtils.assertContains(expected.getFlyweightMessage(), messages[i]);
                    TestUtils.assertEquals(expected.getFlyweightMessage(), actual.getFlyweightMessage());
                    Assert.assertEquals(positions[i], expected.getPosition());
                    Assert.assertEquals(expected.getPosition(), actual.getPosition());
                    binder.clear();
                }
            }
        });
    }

    @Test
    public void testFilterComparisonErrors() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t (
                        aboolean BOOLEAN, along LONG, anint INT, ageolong GEOHASH(12c), auuid UUID, ts TIMESTAMP, ts_ns TIMESTAMP_NS
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            assertException(
                    "SELECT * FROM t WHERE aboolean = 0",
                    31,
                    "there is no matching operator `=` with the argument types: BOOLEAN = INT"
            );
            assertException(
                    "SELECT * FROM t WHERE along = true",
                    28,
                    "there is no matching operator `=` with the argument types: LONG = BOOLEAN"
            );
            assertException(
                    "SELECT * FROM t WHERE ageolong = 0",
                    31,
                    "there is no matching operator `=` with the argument types: GEOHASH(12c) = INT"
            );
            assertException(
                    "SELECT * FROM t WHERE auuid = anint",
                    28,
                    "there is no matching operator `=` with the argument types: UUID = INT"
            );
            assertException("SELECT * FROM t WHERE along = 0x123", 30, "invalid constant: 0x123");
            assertException("SELECT * FROM t WHERE ts = ''", 27, "invalid timestamp");
            assertException("SELECT * FROM t WHERE ts_ns = ''", 30, "Invalid date [str=]");
        });
    }

    @Test
    public void testNullComparisonsMatchSqlText() throws Exception {
        assertEquivalent(
                "l != null",
                "i != null",
                "d != null",
                "ts != null",
                "str != null",
                "sym != null",
                "ts > '2024-01-01'"
        );
    }

    private static void assertEquivalent(String... texts) throws Exception {
        assertMemoryLeak(() -> {
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
                 FunctionBindingHarness binder = new FunctionBindingHarness(new FunctionParser(configuration, engine.getFunctionFactoryCache()))) {
                final OutputSchema input = input();
                for (String text : texts) {
                    final ExpressionNode node = compiler.parseExpression(text);
                    final BoundExpression expected = binder.isGroupBy(node.token)
                            ? binder.bindAggregate(node, input, null, sqlExecutionContext)
                            : binder.bind(node, input, null, sqlExecutionContext);
                    final BoundExpression actual = bindViaApi(binder, node, input);
                    try {
                        assertSameBinding(expected, actual);
                    } catch (AssertionError e) {
                        throw new AssertionError(text, e);
                    }
                    binder.clear();
                }
            }
        });
    }

    private static void assertSameBinding(BoundExpression expected, BoundExpression actual) {
        Assert.assertSame(expected.getClass(), actual.getClass());
        Assert.assertEquals(ColumnType.nameOf(expected.getDataType()), ColumnType.nameOf(actual.getDataType()));
        Assert.assertEquals(expected.getDataType(), actual.getDataType());
        Assert.assertEquals(expected.getFunctionFlags(), actual.getFunctionFlags());
        Assert.assertEquals(expected.getPosition(), actual.getPosition());
        switch (expected) {
            case FunctionExpression call -> {
                final FunctionExpression other = (FunctionExpression) actual;
                Assert.assertSame(call.getOverload(), other.getOverload());
                Assert.assertEquals(call.isSetOperation(), other.isSetOperation());
                Assert.assertEquals(call.isProjectedOffset(), other.isProjectedOffset());
                Assert.assertEquals(call.getArgumentCount(), other.getArgumentCount());
                for (int i = 0, n = call.getArgumentCount(); i < n; i++) {
                    Assert.assertEquals(call.getArgumentPosition(i), other.getArgumentPosition(i));
                    assertSameBinding(call.argumentAt(i), other.argumentAt(i));
                }
            }
            case ConstantExpression constant -> {
                final ConstantExpression other = (ConstantExpression) actual;
                Assert.assertEquals(constant.isLiteral(), other.isLiteral());
                Assert.assertEquals(constant.getLongValue(), other.getLongValue());
                Assert.assertEquals(constant.getDecimalLh(), other.getDecimalLh());
                Assert.assertEquals(constant.getSource() == null, other.getSource() == null);
                if (constant.getSource() != null) {
                    assertSameBinding(constant.getSource(), other.getSource());
                }
            }
            case ColumnExpression column -> {
                final ColumnExpression other = (ColumnExpression) actual;
                Assert.assertEquals(column.getColumnId(), other.getColumnId());
                Assert.assertEquals(column.isDirectReference(), other.isDirectReference());
                Assert.assertEquals(column.isCast(), other.isCast());
            }
            default -> {
            }
        }
    }

    /**
     * Rebuilds the bound tree bottom-up through bindCall(), binding only leaves from SQL text.
     */
    private static BoundExpression bindViaApi(FunctionBindingHarness binder, ExpressionNode node, OutputSchema input) throws SqlException {
        if (node.type != ExpressionNode.FUNCTION && node.type != ExpressionNode.OPERATION
                && node.type != ExpressionNode.SET_OPERATION) {
            return binder.bind(node, input, null, sqlExecutionContext);
        }
        final ObjList<BoundExpression> args = new ObjList<>();
        if (node.paramCount < 3) {
            if (node.paramCount == 2) {
                args.add(bindViaApi(binder, node.lhs, input));
            }
            if (node.paramCount > 0) {
                args.add(bindViaApi(binder, node.rhs, input));
            }
        } else {
            for (int i = node.paramCount - 1; i >= 0; i--) {
                args.add(bindViaApi(binder, node.args.getQuick(i), input));
            }
        }
        return binder.bindCall(node.token, node.position, args, input, sqlExecutionContext);
    }

    private static SqlException bindingError(FunctionBindingHarness binder, ExpressionNode node, OutputSchema input, boolean isViaApi) {
        try {
            if (isViaApi) {
                bindViaApi(binder, node, input);
            } else if (binder.isGroupBy(node.token)) {
                binder.bindAggregate(node, input, null, sqlExecutionContext);
            } else {
                binder.bind(node, input, null, sqlExecutionContext);
            }
            throw new AssertionError("binding error expected: " + node);
        } catch (SqlException e) {
            return e;
        }
    }

    private static OutputSchema input() {
        return new OutputSchema()
                .add(1, "i", ColumnType.INT, true)
                .add(2, "l", ColumnType.LONG, true)
                .add(3, "b", ColumnType.BYTE, true)
                .add(4, "sh", ColumnType.SHORT, true)
                .add(10, "s", ColumnType.LONG, true)
                .add(11, "c", ColumnType.LONG, true)
                .add(12, "d", ColumnType.getDecimalType(10, 2), true)
                .add(13, "ts", ColumnType.TIMESTAMP_MICRO, true)
                .add(14, "str", ColumnType.STRING, true)
                .add(15, "bo", ColumnType.BOOLEAN, true)
                .add(16, "sym", ColumnType.SYMBOL, true);
    }
}
