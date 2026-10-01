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
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionBinder;
import io.questdb.griffin.FunctionParser;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.constants.ArrayConstant;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class FunctionBinderArrayScalarTest extends AbstractCairoTest {
    @Test
    public void testReducersRelocateAndReconstructIndependentWorkers() throws Exception {
        assertMemoryLeak(() -> {
            final ObjList<String> names = new ObjList<>();
            names.add("array_sum");
            names.add("array_avg");
            names.add("array_min");
            names.add("array_max");
            names.add("array_count");
            names.add("array_stddev");
            names.add("array_stddev_samp");
            names.add("array_stddev_pop");
            final int type = ColumnType.encodeArrayType(ColumnType.DOUBLE, 2);
            final OutputSchema input = new OutputSchema().add(1, "unused", ColumnType.INT, true).add(70, "a", type, true);
            final OutputSchema pruned = new OutputSchema().add(70, "renamed", type, true);
            try (ArrayConstant value = new ArrayConstant(new double[][]{{1, 2}, {3, 4}})) {
                for (int i = 0; i < names.size(); i++) {
                    final String name = names.getQuick(i);
                    final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
                    Function owner = null;
                    Function worker = null;
                    try {
                        try (FunctionBinder binder = new FunctionBinder(parser)) {
                            final BoundExpression expression = binder.bind(unary(name, literal("a")), input, null, sqlExecutionContext);
                            owner = binder.instantiate(expression, pruned, sqlExecutionContext);
                            worker = binder.instantiate(expression, input, sqlExecutionContext);
                            Assert.assertNotSame(owner, worker);
                            Assert.assertFalse(owner.isThreadSafe());
                            binder.clear();
                        }
                        parser.clear();
                        owner.init(null, sqlExecutionContext);
                        worker.init(null, sqlExecutionContext);
                        final double expected = switch (name) {
                            case "array_sum" -> 10;
                            case "array_avg" -> 2.5;
                            case "array_min" -> 1;
                            case "array_max", "array_count" -> 4;
                            case "array_stddev_pop" -> Math.sqrt(1.25);
                            default -> Math.sqrt(5.0 / 3);
                        };
                        Assert.assertEquals(name, expected, numericValue(owner, record(0, type, value.getArray(null))), 1e-12);
                        Assert.assertEquals(name, expected, numericValue(worker, record(1, type, value.getArray(null))), 1e-12);
                        final double nullValue = numericValue(owner, record(0, type, ArrayConstant.NULL));
                        Assert.assertTrue(name, name.equals("array_count") ? nullValue == 0 : Double.isNaN(nullValue));
                        Assert.assertEquals(name, expected, numericValue(worker, record(1, type, value.getArray(null))), 1e-12);
                    } finally {
                        Misc.free(owner);
                        Misc.free(worker);
                    }
                }
            }
        });
    }

    @Test
    public void testDimensionLengthCapturesFinalIndexesThroughScalarParents() throws Exception {
        assertMemoryLeak(() -> {
            final int type = ColumnType.encodeArrayType(ColumnType.DOUBLE, 2);
            final OutputSchema input = new OutputSchema().add(1, "unused", ColumnType.INT, true).add(70, "a", type, true);
            final OutputSchema pruned = new OutputSchema().add(70, "a", type, true);
            for (boolean isRuntimeDimension : new boolean[]{false, true}) {
                bindVariableService.setInt(0, 2);
                final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
                Function owner = null;
                Function worker = null;
                try {
                    try (FunctionBinder binder = new FunctionBinder(parser)) {
                        final ExpressionNode dimension = isRuntimeDimension ? variable("$1") : constant("2");
                        final BoundExpression expression = binder.bind(binary("+",
                                binary("dim_length", literal("a"), dimension), constant("1")), input, null, sqlExecutionContext);
                        owner = binder.instantiate(expression, pruned, sqlExecutionContext);
                        worker = binder.instantiate(expression, input, sqlExecutionContext);
                        binder.clear();
                    }
                    parser.clear();
                    for (int pass = 0; pass < 2; pass++) {
                        bindVariableService.setInt(0, 2 - pass);
                        owner.init(null, sqlExecutionContext);
                        worker.init(null, sqlExecutionContext);
                        final int expectedDim = isRuntimeDimension ? 2 - pass : 2;
                        final int[] calls = {0};
                        Assert.assertEquals(5 + expectedDim, owner.getInt(shapeRecord(0, type, expectedDim, calls)));
                        Assert.assertEquals(5 + expectedDim, worker.getInt(shapeRecord(1, type, expectedDim, calls)));
                        Assert.assertEquals(2, calls[0]);
                        owner.cursorClosed();
                        worker.cursorClosed();
                    }
                    if (isRuntimeDimension) {
                        bindVariableService.setInt(0, Numbers.INT_NULL);
                        owner.init(null, sqlExecutionContext);
                        final int[] calls = {0};
                        Assert.assertEquals(Numbers.INT_NULL, owner.getInt(shapeRecord(0, type, 0, calls)));
                        Assert.assertEquals(0, calls[0]);
                    }
                } finally {
                    Misc.free(owner);
                    Misc.free(worker);
                }
            }
        });
    }

    @Test
    public void testTypedEmptyArraysReconstructAfterTypeAssignmentAndCompilerReset() throws Exception {
        assertMemoryLeak(() -> {
            for (int dimensions = 1; dimensions <= 2; dimensions++) {
                final int type = ColumnType.encodeArrayType(ColumnType.DOUBLE, dimensions);
                for (boolean isDimensionLength : new boolean[]{false, true}) {
                    bindVariableService.setInt(0, dimensions);
                    final OutputSchema input = new OutputSchema();
                    final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
                    Function owner = null;
                    Function worker = null;
                    try {
                        try (FunctionBinder binder = new FunctionBinder(parser)) {
                            final ExpressionNode empty = ExpressionNode.FACTORY.newInstance()
                                    .of(ExpressionNode.ARRAY_CONSTRUCTOR, "array", 0, 1);
                            final ExpressionNode cast = binary("cast", empty, constant(dimensions == 1 ? "double[]" : "double[][]"));
                            final BoundExpression expression = binder.bind(isDimensionLength
                                    ? binary("dim_length", cast, variable("$1")) : cast, input, null, sqlExecutionContext);
                            owner = binder.instantiate(expression, input, sqlExecutionContext);
                            worker = binder.instantiate(expression, input, sqlExecutionContext);
                            Assert.assertNotSame(owner, worker);
                            binder.clear();
                            final BoundExpression recovery = binder.bind(array(constant("1"), constant("2")), input, null, sqlExecutionContext);
                            try (Function value = binder.instantiate(recovery, input, sqlExecutionContext)) {
                                Assert.assertEquals(2, value.getArray(null).getDouble(1), 0);
                            }
                        }
                        parser.clear();
                        for (int pass = 0; pass < 3; pass++) {
                            bindVariableService.setInt(0, pass == 2 ? Numbers.INT_NULL : pass == 0 ? dimensions : 1);
                            owner.init(null, sqlExecutionContext);
                            worker.init(null, sqlExecutionContext);
                            if (isDimensionLength) {
                                final int expected = pass == 2 ? Numbers.INT_NULL : 0;
                                Assert.assertEquals(expected, owner.getInt(null));
                                Assert.assertEquals(expected, worker.getInt(null));
                            } else {
                                Assert.assertEquals(type, owner.getType());
                                Assert.assertEquals(type, worker.getType());
                                final ArrayView first = owner.getArray(null);
                                final ArrayView second = worker.getArray(null);
                                Assert.assertNotSame(first, second);
                                Assert.assertFalse(first.isNull());
                                Assert.assertFalse(second.isNull());
                                Assert.assertEquals(dimensions, first.getDimCount());
                                Assert.assertEquals(dimensions, second.getDimCount());
                                Assert.assertTrue(first.isEmpty());
                                Assert.assertTrue(second.isEmpty());
                            }
                            owner.cursorClosed();
                            worker.cursorClosed();
                        }
                        owner = Misc.free(owner);
                        bindVariableService.setInt(0, dimensions);
                        worker.init(null, sqlExecutionContext);
                        if (isDimensionLength) {
                            Assert.assertEquals(0, worker.getInt(null));
                        } else {
                            Assert.assertEquals(type, worker.getType());
                            Assert.assertTrue(worker.getArray(null).isEmpty());
                        }
                    } finally {
                        Misc.free(owner);
                        Misc.free(worker);
                    }
                }
            }
        });
    }

    @Test
    public void testSortAndReverseOwnIndependentNativeViews() throws Exception {
        assertMemoryLeak(() -> {
            final int type = ColumnType.encodeArrayType(ColumnType.DOUBLE, 1);
            final OutputSchema input = new OutputSchema().add(1, "unused", ColumnType.INT, true).add(70, "a", type, true);
            final OutputSchema pruned = new OutputSchema().add(70, "a", type, true);
            try (ArrayConstant value = new ArrayConstant(new double[]{3, Double.NaN, 1, 2});
                 ArrayConstant other = new ArrayConstant(new double[]{7, 8})) {
                for (int variant = 0; variant < 4; variant++) {
                    final ExpressionNode node = switch (variant) {
                        case 0 -> unary("array_reverse", literal("a"));
                        case 1 -> unary("array_sort", literal("a"));
                        case 2 -> binary("array_sort", literal("a"), constant("true"));
                        default -> ternary("array_sort", literal("a"), constant("false"), constant("true"));
                    };
                    final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
                    Function owner = null;
                    Function worker = null;
                    try {
                        try (FunctionBinder binder = new FunctionBinder(parser)) {
                            final BoundExpression expression = binder.bind(node, input, null, sqlExecutionContext);
                            owner = binder.instantiate(expression, pruned, sqlExecutionContext);
                            worker = binder.instantiate(expression, input, sqlExecutionContext);
                            Assert.assertNotSame(owner, worker);
                            Assert.assertFalse(owner.isThreadSafe());
                            binder.clear();
                        }
                        parser.clear();
                        for (int pass = 0; pass < 2; pass++) {
                            owner.init(null, sqlExecutionContext);
                            worker.init(null, sqlExecutionContext);
                            final ArrayView a = owner.getArray(record(0, type, value.getArray(null)));
                            final ArrayView b = worker.getArray(record(1, type, other.getArray(null)));
                            Assert.assertNotSame(a, b);
                            Assert.assertEquals(4, a.getFlatViewLength());
                            Assert.assertEquals(2, b.getFlatViewLength());
                            final double[] expected = switch (variant) {
                                case 0 -> new double[]{2, 1, Double.NaN, 3};
                                case 1 -> new double[]{1, 2, 3, Double.NaN};
                                case 2 -> new double[]{Double.NaN, 3, 2, 1};
                                default -> new double[]{Double.NaN, 1, 2, 3};
                            };
                            for (int i = 0; i < expected.length; i++) {
                                Assert.assertEquals(expected[i], a.getDouble(i), 0);
                            }
                            Assert.assertEquals(variant == 0 || variant == 2 ? 8 : 7, b.getDouble(0), 0);
                            owner.cursorClosed();
                            worker.cursorClosed();
                        }
                    } finally {
                        Misc.free(owner);
                        Misc.free(worker);
                    }
                }
            }
        });
    }

    @Test
    public void testArrayParametersRebindWithoutSharingWorkerViews() throws Exception {
        assertMemoryLeak(() -> {
            try (ArrayConstant first = new ArrayConstant(new double[]{3, 1});
                 ArrayConstant second = new ArrayConstant(new double[]{9, 2, 4});
                 FunctionBinder binder = new FunctionBinder(new FunctionParser(configuration, engine.getFunctionFactoryCache()))) {
                bindVariableService.setArray(0, first.getArray(null));
                final OutputSchema input = new OutputSchema();
                final BoundExpression expression = binder.bind(unary("array_sort", variable("$1")), input, null, sqlExecutionContext);
                try (Function owner = binder.instantiate(expression, input, sqlExecutionContext);
                     Function worker = binder.instantiate(expression, input, sqlExecutionContext)) {
                    binder.clear();
                    owner.init(null, sqlExecutionContext);
                    worker.init(null, sqlExecutionContext);
                    Assert.assertEquals(1, owner.getArray(null).getDouble(0), 0);
                    Assert.assertEquals(1, worker.getArray(null).getDouble(0), 0);
                    owner.cursorClosed();
                    worker.cursorClosed();
                    bindVariableService.setArray(0, second.getArray(null));
                    owner.init(null, sqlExecutionContext);
                    worker.init(null, sqlExecutionContext);
                    Assert.assertEquals(3, owner.getArray(null).getFlatViewLength());
                    Assert.assertEquals(2, worker.getArray(null).getDouble(0), 0);
                    Assert.assertNotSame(owner.getArray(null), worker.getArray(null));
                }
            } finally {
                bindVariableService.clear();
            }
        });
    }

    @Test
    public void testConstantArrayDiscardAndFailureReleaseNativeChildren() throws Exception {
        assertMemoryLeak(() -> {
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
            final OutputSchema input = new OutputSchema();
            try (FunctionBinder binder = new FunctionBinder(parser)) {
                for (int pass = 0; pass < 3; pass++) {
                    final BoundExpression expression = binder.bind(binary("dim_length", array(constant("1"), constant("2")),
                            binary("cast", constant("null"), constant("int"))), input, null, sqlExecutionContext);
                    try (Function result = binder.instantiate(expression, input, sqlExecutionContext)) {
                        Assert.assertEquals(Numbers.INT_NULL, result.getInt(null));
                    }
                    binder.clear();
                    parser.clear();
                    final SqlException error = Assert.assertThrows(SqlException.class, () -> binder.bind(
                            binary("dim_length", unary("array_sort", array(constant("2"), constant("1"))), constant("2")),
                            input, null, sqlExecutionContext));
                    TestUtils.assertContains(error.getFlyweightMessage(), "array dimension out of bounds");
                    binder.clear();
                    parser.clear();
                }
            }
        });
    }

    private static ExpressionNode array(ExpressionNode left, ExpressionNode right) {
        final ExpressionNode node = binary("array", left, right);
        node.type = ExpressionNode.ARRAY_CONSTRUCTOR;
        return node;
    }

    private static ExpressionNode binary(String name, ExpressionNode left, ExpressionNode right) {
        final ExpressionNode node = ExpressionNode.FACTORY.newInstance().of(ExpressionNode.FUNCTION, name, 0, 1);
        node.paramCount = 2;
        node.lhs = left;
        node.rhs = right;
        return node;
    }

    private static ExpressionNode constant(String value) {
        return ExpressionNode.FACTORY.newInstance().of(ExpressionNode.CONSTANT, value, 0, 1);
    }

    private static ExpressionNode literal(String value) {
        return ExpressionNode.FACTORY.newInstance().of(ExpressionNode.LITERAL, value, 0, 1);
    }

    private static double numericValue(Function function, Record record) {
        return function.getType() == ColumnType.INT ? function.getInt(record) : function.getDouble(record);
    }

    private static Record record(int index, int type, ArrayView value) {
        return new Record() {
            @Override
            public ArrayView getArray(int columnIndex, int columnType) {
                Assert.assertEquals(index, columnIndex);
                Assert.assertEquals(type, columnType);
                return value;
            }
        };
    }

    private static Record shapeRecord(int index, int type, int dimension, int[] calls) {
        return new Record() {
            @Override
            public ArrayView getArray(int columnIndex, int columnType) {
                throw new AssertionError("dimension length must use the native shape getter");
            }

            @Override
            public int getArrayDimLen(int columnIndex, int columnType, int dim) {
                Assert.assertEquals(index, columnIndex);
                Assert.assertEquals(type, columnType);
                Assert.assertEquals(dimension, dim);
                calls[0]++;
                return 4 + dim;
            }
        };
    }

    private static ExpressionNode ternary(String name, ExpressionNode first, ExpressionNode second, ExpressionNode third) {
        final ExpressionNode node = ExpressionNode.FACTORY.newInstance().of(ExpressionNode.FUNCTION, name, 0, 1);
        node.paramCount = 3;
        node.args.add(third);
        node.args.add(second);
        node.args.add(first);
        return node;
    }

    private static ExpressionNode unary(String name, ExpressionNode argument) {
        final ExpressionNode node = ExpressionNode.FACTORY.newInstance().of(ExpressionNode.FUNCTION, name, 0, 1);
        node.paramCount = 1;
        node.rhs = argument;
        return node;
    }

    private static ExpressionNode variable(String value) {
        return ExpressionNode.FACTORY.newInstance().of(ExpressionNode.BIND_VARIABLE, value, 0, 1);
    }
}
