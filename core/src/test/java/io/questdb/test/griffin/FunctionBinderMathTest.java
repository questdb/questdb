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
import io.questdb.griffin.FunctionFactoryDescriptor;
import io.questdb.griffin.FunctionParser;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ConstantExpression;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class FunctionBinderMathTest extends AbstractCairoTest {
    @Test
    public void testConstantsFoldAndKeepTheirFullResultType() throws Exception {
        assertMemoryLeak(() -> {
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
            final OutputSchema input = new OutputSchema();
            try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                final ObjList<ExpressionNode> calls = new ObjList<>(
                        function("pi", null, null),
                        function("degrees", null, constant("0.5")),
                        function("radians", null, constant("90")),
                        function("round", null, constant("null"))
                );
                final double[] expected = {Math.PI, Math.toDegrees(0.5), Math.toRadians(90), Double.NaN};
                for (int i = 0; i < calls.size(); i++) {
                    final BoundExpression expression = binder.bind(calls.getQuick(i), input, null, sqlExecutionContext);
                    Assert.assertTrue(expression instanceof ConstantExpression);
                    Assert.assertEquals(ColumnType.DOUBLE, expression.getDataType());
                    try (Function function = binder.instantiate(expression, input, sqlExecutionContext)) {
                        binder.clear();
                        Assert.assertTrue(function.isConstant());
                        Assert.assertEquals(expected[i], function.getDouble(null), 0.0);
                    }
                }
            }
        });
    }

    @Test
    public void testNestedMathBindsUnconstructedAndBuildsPerLayout() throws Exception {
        assertMemoryLeak(() -> {
            final int[] constructions = {0};
            final ObjList<Function> constructed = new ObjList<>();
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache()) {
                @Override
                public Function createFunction(FunctionFactoryDescriptor overload, int position, CharSequence name,
                                               ObjList<Function> args, IntList positions, SqlExecutionContext context) throws SqlException {
                    constructions[0]++;
                    final Function function = super.createFunction(overload, position, name, args, positions, context);
                    constructed.add(function);
                    return function;
                }
            };
            final OutputSchema original = new OutputSchema();
            for (int i = 0; i < 48; i++) {
                original.add(i, "unused" + i, ColumnType.INT, true);
            }
            original.add(70, "x", ColumnType.DOUBLE, true).add(80, "y", ColumnType.DOUBLE, true);
            final OutputSchema firstLayout = new OutputSchema().add(80, "y", ColumnType.DOUBLE, true)
                    .add(70, "x", ColumnType.DOUBLE, true);
            final OutputSchema secondLayout = new OutputSchema().add(70, "x", ColumnType.DOUBLE, true)
                    .add(80, "y", ColumnType.DOUBLE, true);
            try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                final FunctionExpression expression = (FunctionExpression) binder.bind(
                        function("atan2", function("sin", null, literal("y")), function("cos", null, literal("x"))),
                        original, null, sqlExecutionContext
                );
                Assert.assertEquals(0, constructions[0]);
                TestUtils.assertEquals("atan2(DD)", expression.getSignature());
                try (Function first = binder.instantiate(expression, firstLayout, sqlExecutionContext)) {
                    Assert.assertSame(constructed.getQuick(2), first);
                    Assert.assertEquals(3, constructions[0]);
                    try (Function second = binder.instantiate(expression, secondLayout, sqlExecutionContext)) {
                        Assert.assertNotSame(first, second);
                        Assert.assertEquals(6, constructions[0]);
                        Assert.assertFalse(first.isConstant());
                        Assert.assertFalse(first.isNonDeterministic());
                        Assert.assertTrue(first.isThreadSafe());
                        binder.clear();
                        parser.clear();
                        original.clear();
                        firstLayout.clear();
                        secondLayout.clear();
                        final double expected = Math.atan2(StrictMath.sin(0.75), StrictMath.cos(0.25));
                        Assert.assertEquals(expected, first.getDouble(pair(0.75, 0.25)), 0.0);
                        Assert.assertEquals(expected, second.getDouble(pair(0.25, 0.75)), 0.0);
                        Assert.assertEquals(Math.atan2(StrictMath.sin(-2), StrictMath.cos(1)), first.getDouble(pair(-2, 1)), 0.0);
                        Assert.assertEquals(expected, second.getDouble(pair(0.25, 0.75)), 0.0);
                    }
                }
            }
        });
    }

    @Test
    public void testSignKeepsDifferentFloatAndDoubleZeroSemantics() throws Exception {
        assertMemoryLeak(() -> {
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
            final OutputSchema input = new OutputSchema().add(1, "unused", ColumnType.INT, true)
                    .add(8, "f", ColumnType.FLOAT, true).add(9, "d", ColumnType.DOUBLE, true);
            final OutputSchema pruned = new OutputSchema().add(9, "d", ColumnType.DOUBLE, true)
                    .add(8, "f", ColumnType.FLOAT, true);
            try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                final FunctionExpression floatSign = (FunctionExpression) binder.bind(function("sign", null, literal("f")), input, null, sqlExecutionContext);
                final FunctionExpression doubleSign = (FunctionExpression) binder.bind(function("sign", null, literal("d")), input, null, sqlExecutionContext);
                TestUtils.assertEquals("sign(F)", floatSign.getSignature());
                TestUtils.assertEquals("sign(D)", doubleSign.getSignature());
                try (Function first = binder.instantiate(floatSign, pruned, sqlExecutionContext);
                     Function second = binder.instantiate(doubleSign, pruned, sqlExecutionContext)) {
                    binder.clear();
                    final Record record = new Record() {
                        @Override
                        public double getDouble(int columnIndex) {
                            Assert.assertEquals(0, columnIndex);
                            return -0.0;
                        }

                        @Override
                        public float getFloat(int columnIndex) {
                            Assert.assertEquals(1, columnIndex);
                            return -0.0f;
                        }
                    };
                    Assert.assertEquals(Float.floatToRawIntBits(-0.0f), Float.floatToRawIntBits(first.getFloat(record)));
                    Assert.assertEquals(Double.doubleToRawLongBits(0.0), Double.doubleToRawLongBits(second.getDouble(record)));
                }
            }
        });
    }

    private static ExpressionNode constant(String token) {
        return ExpressionNode.FACTORY.newInstance().of(ExpressionNode.CONSTANT, token, 0, 0);
    }

    private static ExpressionNode function(String name, ExpressionNode left, ExpressionNode right) {
        final ExpressionNode node = ExpressionNode.FACTORY.newInstance().of(ExpressionNode.FUNCTION, name, 0, 0);
        node.lhs = left;
        node.rhs = right;
        node.paramCount = left != null ? 2 : right != null ? 1 : 0;
        return node;
    }

    private static ExpressionNode literal(String token) {
        return ExpressionNode.FACTORY.newInstance().of(ExpressionNode.LITERAL, token, 0, 0);
    }

    private static Record pair(double first, double second) {
        return new Record() {
            @Override
            public double getDouble(int columnIndex) {
                Assert.assertTrue(columnIndex == 0 || columnIndex == 1);
                return columnIndex == 0 ? first : second;
            }
        };
    }
}
