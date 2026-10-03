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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionParser;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.constants.ArrayConstant;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.std.Misc;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class FunctionBinderArrayTest extends AbstractCairoTest {
    @Test
    public void testArrayColumnRelocatesWithFullTypeAndSurvivesCompilerClose() throws Exception {
        assertMemoryLeak(() -> {
            for (int dimensions = 1; dimensions <= 2; dimensions++) {
                final int type = ColumnType.encodeArrayType(ColumnType.DOUBLE, dimensions);
                final OutputSchema input = new OutputSchema().add(1, "unused", ColumnType.INT, true)
                        .add(70, "a", type, true);
                final OutputSchema pruned = new OutputSchema().add(70, "renamed", type, true);
                final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
                Function first = null;
                Function second = null;
                try {
                    try (FunctionBindingHarness binder = new FunctionBindingHarness(parser)) {
                        final BoundExpression expression = binder.bind(literal("a"), input, null, sqlExecutionContext);
                        first = binder.instantiate(expression, pruned, sqlExecutionContext);
                        second = binder.instantiate(expression, input, sqlExecutionContext);
                        Assert.assertNotSame(first, second);
                        binder.clear();
                    }
                    parser.clear();
                    input.clear();
                    pruned.clear();
                    try (ArrayConstant value = dimensions == 1 ? new ArrayConstant(new double[]{2, 4})
                            : new ArrayConstant(new double[][]{{2, 4}, {6, 8}})) {
                        for (int pass = 0; pass < 2; pass++) {
                            first.init(null, sqlExecutionContext);
                            second.init(null, sqlExecutionContext);
                            Assert.assertEquals(type, first.getType());
                            Assert.assertSame(value.getArray(null), first.getArray(record(0, type, value)));
                            Assert.assertSame(value.getArray(null), second.getArray(record(1, type, value)));
                            first.cursorClosed();
                            second.cursorClosed();
                        }
                    }
                } finally {
                    Misc.free(first);
                    Misc.free(second);
                }
            }
        });
    }

    @Test
    public void testConstantNestedAndEmptyArraysKeepIndependentOwnership() throws Exception {
        assertMemoryLeak(() -> {
            final OutputSchema input = new OutputSchema();
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
            try (FunctionBindingHarness binder = new FunctionBindingHarness(parser)) {
                final BoundExpression expression = binder.bind(array(array(constant("1"), constant("null")),
                        array(constant("3"), constant("4"))), input, null, sqlExecutionContext);
                Assert.assertTrue(expression instanceof FunctionExpression);
                Assert.assertTrue((expression.getFunctionFlags() & BoundExpression.CONSTANT) != 0);
                Assert.assertEquals(ColumnType.encodeArrayType(ColumnType.DOUBLE, 2), expression.getDataType());
                try (Function first = binder.instantiate(expression, input, sqlExecutionContext);
                     Function second = binder.instantiate(expression, input, sqlExecutionContext)) {
                    Assert.assertNotSame(first, second);
                    binder.clear();
                    parser.clear();
                    first.init(null, sqlExecutionContext);
                    second.init(null, sqlExecutionContext);
                    assertNested(first.getArray(null));
                    assertNested(second.getArray(null));
                    Assert.assertNotSame(first.getArray(null), second.getArray(null));
                }
                final BoundExpression empty = binder.bind(array(), input, null, sqlExecutionContext);
                try (Function first = binder.instantiate(empty, input, sqlExecutionContext);
                     Function second = binder.instantiate(empty, input, sqlExecutionContext)) {
                    Assert.assertTrue(first.getArray(null).isEmpty());
                    Assert.assertTrue(second.getArray(null).isEmpty());
                    Assert.assertEquals(first.getType(), second.getType());
                }
            }
        });
    }

    @Test
    public void testRowConstructorsRebuildAfterPruningAndParameterRebind() throws Exception {
        assertMemoryLeak(() -> {
            final OutputSchema input = new OutputSchema().add(1, "unused", ColumnType.INT, true)
                    .add(70, "d", ColumnType.DOUBLE, true);
            final OutputSchema pruned = new OutputSchema().add(70, "d", ColumnType.DOUBLE, true);
            bindVariableService.setDouble(0, 7);
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
            try (FunctionBindingHarness binder = new FunctionBindingHarness(parser)) {
                final BoundExpression expression = binder.bind(array(literal("d"), variable("$1")), input, null, sqlExecutionContext);
                Assert.assertEquals(0, expression.getFunctionFlags() & BoundExpression.CONSTANT);
                try (Function first = binder.instantiate(expression, pruned, sqlExecutionContext);
                     Function second = binder.instantiate(expression, input, sqlExecutionContext)) {
                    binder.clear();
                    parser.clear();
                    for (int pass = 0; pass < 2; pass++) {
                        bindVariableService.setDouble(0, 7 + pass);
                        first.init(null, sqlExecutionContext);
                        second.init(null, sqlExecutionContext);
                        final ArrayView a = first.getArray(doubleRecord(0, 2 + pass));
                        Assert.assertEquals(2 + pass, a.getDouble(0), 0);
                        Assert.assertEquals(7 + pass, a.getDouble(1), 0);
                        final ArrayView b = second.getArray(doubleRecord(1, 4 + pass));
                        Assert.assertEquals(4 + pass, b.getDouble(0), 0);
                        Assert.assertEquals(7 + pass, b.getDouble(1), 0);
                        first.cursorClosed();
                        second.cursorClosed();
                    }
                }
            }
        });
    }

    @Test
    public void testValidationFailureReleasesConstantArraysAndCompilerRecovers() throws Exception {
        assertMemoryLeak(() -> {
            final OutputSchema input = new OutputSchema();
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
            try (FunctionBindingHarness binder = new FunctionBindingHarness(parser)) {
                for (int pass = 0; pass < 3; pass++) {
                    try {
                        binder.bind(array(array(constant("1"), constant("2")), constant("3")), input, null, sqlExecutionContext);
                        Assert.fail();
                    } catch (SqlException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "mixed array and non-array elements");
                    }
                    binder.clear();
                    parser.clear();
                    final BoundExpression expression = binder.bind(array(constant("1"), constant("2")), input, null, sqlExecutionContext);
                    try (Function function = binder.instantiate(expression, input, sqlExecutionContext)) {
                        Assert.assertEquals(2, function.getArray(null).getDouble(1), 0);
                    }
                    binder.clear();
                    parser.clear();
                }
            }
        });
    }

    @Test
    public void testConstantAccessReconstructsNativeColumnIndexesThroughScalarParents() throws Exception {
        assertMemoryLeak(() -> {
            for (int dimensions = 1; dimensions <= 2; dimensions++) {
                final int type = ColumnType.encodeArrayType(ColumnType.DOUBLE, dimensions);
                final OutputSchema input = new OutputSchema().add(1, "unused", ColumnType.INT, true)
                        .add(70, "a", type, true);
                final OutputSchema pruned = new OutputSchema().add(70, "a", type, true);
                final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
                Function first = null;
                Function second = null;
                try {
                    try (FunctionBindingHarness binder = new FunctionBindingHarness(parser)) {
                        final ExpressionNode access = dimensions == 1 ? access(literal("a"), constant("2"))
                                : access(access(literal("a"), constant("2")), constant("1"));
                        final BoundExpression expression = binder.bind(binary("+", access, constant("1.0")), input, null, sqlExecutionContext);
                        first = binder.instantiate(expression, pruned, sqlExecutionContext);
                        second = binder.instantiate(expression, input, sqlExecutionContext);
                        Assert.assertNotSame(first, second);
                        binder.clear();
                    }
                    parser.clear();
                    try (ArrayConstant value = dimensions == 1 ? new ArrayConstant(new double[]{10, 20})
                            : new ArrayConstant(new double[][]{{10, 20}, {30, 40}})) {
                        final int[] calls = {0};
                        final double expected = dimensions == 1 ? 21 : 31;
                        Assert.assertEquals(expected, first.getDouble(fastRecord(0, type, value, calls)), 0);
                        Assert.assertEquals(expected, second.getDouble(fastRecord(1, type, value, calls)), 0);
                        Assert.assertEquals(2, calls[0]);
                    }
                } finally {
                    Misc.free(first);
                    Misc.free(second);
                }
            }
        });
    }

    @Test
    public void testSlicesDynamicBoundsAndNegativeIndexesOwnIndependentViews() throws Exception {
        assertMemoryLeak(() -> {
            final int type = ColumnType.encodeArrayType(ColumnType.DOUBLE, 1);
            final OutputSchema input = new OutputSchema().add(70, "a", type, true);
            bindVariableService.setInt(0, 2);
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
            try (ArrayConstant value = new ArrayConstant(new double[]{10, 20, 30, 40});
                 FunctionBindingHarness binder = new FunctionBindingHarness(parser)) {
                final BoundExpression slice = binder.bind(access(literal("a"),
                        binary(":", variable("$1"), constant("4"))), input, null, sqlExecutionContext);
                final BoundExpression negative = binder.bind(access(literal("a"), constant("-1")), input, null, sqlExecutionContext);
                final BoundExpression open = binder.bind(access(literal("a"), unary(":", constant("3"))), input, null, sqlExecutionContext);
                try (Function first = binder.instantiate(slice, input, sqlExecutionContext);
                     Function second = binder.instantiate(slice, input, sqlExecutionContext);
                     Function last = binder.instantiate(negative, input, sqlExecutionContext);
                     Function tail = binder.instantiate(open, input, sqlExecutionContext)) {
                    binder.clear();
                    parser.clear();
                    final Record record = record(0, type, value);
                    for (int pass = 0; pass < 2; pass++) {
                        bindVariableService.setInt(0, 2 + pass);
                        first.init(null, sqlExecutionContext);
                        second.init(null, sqlExecutionContext);
                        final ArrayView a = first.getArray(record);
                        final ArrayView b = second.getArray(record);
                        Assert.assertNotSame(a, b);
                        Assert.assertEquals(2 - pass, a.getFlatViewLength());
                        Assert.assertEquals(20 + pass * 10, a.getDouble(0), 0);
                        Assert.assertEquals(a.getDouble(0), b.getDouble(0), 0);
                        Assert.assertEquals(40, last.getDouble(record), 0);
                        Assert.assertEquals(2, tail.getArray(record).getFlatViewLength());
                        first.cursorClosed();
                        second.cursorClosed();
                    }
                    bindVariableService.setInt(0, 0);
                    first.init(null, sqlExecutionContext);
                    final CairoException error = Assert.assertThrows(CairoException.class, () -> first.getArray(record));
                    TestUtils.assertContains(error.getFlyweightMessage(), "array slice bounds must be non-zero");
                }
            }
        });
    }

    @Test
    public void testNullIndexReleasesNativeArrayAndWrongDimensionalityRecovers() throws Exception {
        assertMemoryLeak(() -> {
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
            final OutputSchema input = new OutputSchema();
            try (FunctionBindingHarness binder = new FunctionBindingHarness(parser)) {
                final BoundExpression expression = binder.bind(access(array(constant("1"), constant("2")),
                        binary("cast", constant("null"), constant("long"))), input, null, sqlExecutionContext);
                try (Function function = binder.instantiate(expression, input, sqlExecutionContext)) {
                    Assert.assertTrue(Double.isNaN(function.getDouble(null)));
                }
                binder.clear();
                parser.clear();
                try {
                    binder.bind(access(array(constant("1"), constant("2")), constant("1"), constant("1")),
                            input, null, sqlExecutionContext);
                    Assert.fail();
                } catch (SqlException error) {
                    TestUtils.assertContains(error.getFlyweightMessage(), "too many array access arguments");
                }
                binder.clear();
                parser.clear();
                final BoundExpression valid = binder.bind(access(array(constant("1"), constant("2")), constant("-1")), input, null, sqlExecutionContext);
                try (Function function = binder.instantiate(valid, input, sqlExecutionContext)) {
                    Assert.assertEquals(2, function.getDouble(null), 0);
                }
            }
        });
    }

    private static ExpressionNode access(ExpressionNode array, ExpressionNode index) {
        final ExpressionNode node = binary("[]", array, index);
        node.type = ExpressionNode.ARRAY_ACCESS;
        return node;
    }

    private static ExpressionNode access(ExpressionNode array, ExpressionNode first, ExpressionNode second) {
        final ExpressionNode node = ExpressionNode.FACTORY.newInstance().of(ExpressionNode.ARRAY_ACCESS, "[]", 0, 1);
        node.paramCount = 3;
        node.args.add(second);
        node.args.add(first);
        node.args.add(array);
        return node;
    }

    private static ExpressionNode binary(String name, ExpressionNode left, ExpressionNode right) {
        final ExpressionNode node = ExpressionNode.FACTORY.newInstance().of(ExpressionNode.FUNCTION, name, 0, 1);
        node.paramCount = 2;
        node.lhs = left;
        node.rhs = right;
        return node;
    }

    private static Record fastRecord(int expectedIndex, int expectedType, ArrayConstant value, int[] calls) {
        return new Record() {
            @Override
            public ArrayView getArray(int columnIndex, int columnType) {
                throw new AssertionError("native array column access expected");
            }

            @Override
            public double getArrayDouble1d2d(int columnIndex, int columnType, int i, int j) {
                Assert.assertEquals(expectedIndex, columnIndex);
                Assert.assertEquals(expectedType, columnType);
                calls[0]++;
                final ArrayView array = value.getArray(null);
                return array.getDouble(array.getDimCount() == 1 ? i : i * array.getStride(0) + j);
            }
        };
    }

    private static ExpressionNode unary(String name, ExpressionNode argument) {
        final ExpressionNode node = ExpressionNode.FACTORY.newInstance().of(ExpressionNode.FUNCTION, name, 0, 1);
        node.paramCount = 1;
        node.rhs = argument;
        return node;
    }

    private static ExpressionNode array() {
        return ExpressionNode.FACTORY.newInstance().of(ExpressionNode.ARRAY_CONSTRUCTOR, "array", 0, 0);
    }

    private static ExpressionNode array(ExpressionNode left, ExpressionNode right) {
        final ExpressionNode node = array();
        node.paramCount = 2;
        node.lhs = left;
        node.rhs = right;
        return node;
    }

    private static void assertNested(ArrayView array) {
        Assert.assertEquals(2, array.getDimCount());
        Assert.assertEquals(2, array.getDimLen(0));
        Assert.assertEquals(2, array.getDimLen(1));
        Assert.assertEquals(1, array.getDouble(0), 0);
        Assert.assertTrue(Double.isNaN(array.getDouble(1)));
        Assert.assertEquals(3, array.getDouble(2), 0);
        Assert.assertEquals(4, array.getDouble(3), 0);
    }

    private static ExpressionNode constant(String value) {
        return ExpressionNode.FACTORY.newInstance().of(ExpressionNode.CONSTANT, value, 0, 1);
    }

    private static Record doubleRecord(int expectedIndex, double value) {
        return new Record() {
            @Override
            public double getDouble(int columnIndex) {
                Assert.assertEquals(expectedIndex, columnIndex);
                return value;
            }
        };
    }

    private static ExpressionNode literal(String value) {
        return ExpressionNode.FACTORY.newInstance().of(ExpressionNode.LITERAL, value, 0, 1);
    }

    private static Record record(int expectedIndex, int expectedType, ArrayConstant value) {
        return new Record() {
            @Override
            public ArrayView getArray(int columnIndex, int columnType) {
                Assert.assertEquals(expectedIndex, columnIndex);
                Assert.assertEquals(expectedType, columnType);
                return value.getArray(null);
            }
        };
    }

    private static ExpressionNode variable(String value) {
        return ExpressionNode.FACTORY.newInstance().of(ExpressionNode.BIND_VARIABLE, value, 0, 1);
    }
}
