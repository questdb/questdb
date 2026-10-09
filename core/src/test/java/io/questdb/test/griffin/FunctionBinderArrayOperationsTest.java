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

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactoryDescriptor;
import io.questdb.griffin.FunctionParser;
import io.questdb.griffin.FunctionResolver;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.columns.ColumnFunction;
import io.questdb.griffin.engine.functions.constants.ArrayConstant;
import io.questdb.griffin.engine.groupby.FastGroupByAllocator;
import io.questdb.griffin.engine.groupby.SimpleMapValue;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.std.Misc;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class FunctionBinderArrayOperationsTest extends AbstractCairoTest {
    @Test
    public void testArrayAggregateScalarContextAndDimensionRestrictionsRemain() throws Exception {
        assertMemoryLeak(() -> {
            try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, new FunctionParser(configuration, engine.getFunctionFactoryCache()))) {
                final int arrayType = ColumnType.encodeArrayType(ColumnType.DOUBLE, 1);
                for (int variant = 0; variant < 5; variant++) {
                    final String name = switch (variant) {
                        case 0 -> "array_agg";
                        case 1 -> "first";
                        case 2 -> "last";
                        case 3 -> "first_not_null";
                        default -> "last_not_null";
                    };
                    final SqlException error = Assert.assertThrows(SqlException.class,
                            () -> binder.bind(unary(name, literal()), schema(arrayType, false), null, sqlExecutionContext));
                    TestUtils.assertContains(error.getFlyweightMessage(), "aggregate functions are not allowed in this context");
                    binder.clear();
                }
                final SqlException dimensions = Assert.assertThrows(SqlException.class,
                        () -> binder.bindAggregate(unary("array_agg", literal()),
                                schema(ColumnType.encodeArrayType(ColumnType.DOUBLE, 2), false), null, sqlExecutionContext));
                TestUtils.assertContains(dimensions.getFlyweightMessage(), "array is not one-dimensional");
                binder.clear();
                final SqlException weak = Assert.assertThrows(SqlException.class,
                        () -> binder.bindAggregate(unary("array_agg", literal()),
                                schema(ColumnType.encodeArrayTypeWithWeakDims(ColumnType.DOUBLE, true), false), null, sqlExecutionContext));
                TestUtils.assertContains(weak.getFlyweightMessage(), "array bind variable argument is not supported");
            }
        });
    }

    @Test
    public void testArrayAggregatesRebuildFinalLayoutsAndOwnWorkerState() throws Exception {
        assertMemoryLeak(() -> {
            for (int variant = 0; variant < 6; variant++) {
                final String name = switch (variant) {
                    case 0, 1 -> "array_agg";
                    case 2 -> "first";
                    case 3 -> "last";
                    case 4 -> "first_not_null";
                    default -> "last_not_null";
                };
                final int inputType = variant == 0 ? ColumnType.DOUBLE : ColumnType.encodeArrayType(ColumnType.DOUBLE, 1);
                final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
                try (ArrayConstant a = new ArrayConstant(new double[]{1, 2});
                     ArrayConstant b = new ArrayConstant(new double[]{3, 4});
                     ArrayConstant w = new ArrayConstant(new double[]{9, 10});
                     FastGroupByAllocator firstAllocator = new FastGroupByAllocator(1024, 4096);
                     FastGroupByAllocator secondAllocator = new FastGroupByAllocator(1024, 4096);
                     SimpleMapValue firstValue = new SimpleMapValue(5);
                     SimpleMapValue secondValue = new SimpleMapValue(5)) {
                    Function owner = null;
                    Function worker = null;
                    try {
                        try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                            final FunctionExpression expression = binder.bindAggregate(unary(name, literal()), schema(inputType, true), null, sqlExecutionContext);
                            Assert.assertTrue(expression.getOverload().isOrderSensitiveAggregate());
                            owner = binder.instantiateAggregate(expression, schema(inputType, false), metadata(inputType, false), sqlExecutionContext);
                            worker = binder.instantiateAggregate(expression, schema(inputType, true), metadata(inputType, true), sqlExecutionContext);
                            Assert.assertNotSame(owner, worker);
                            Assert.assertEquals(0, ((ColumnFunction) ((UnaryFunction) owner).getArg()).getColumnIndex());
                            Assert.assertEquals(1, ((ColumnFunction) ((UnaryFunction) worker).getArg()).getColumnIndex());
                            binder.clear();
                        }
                        parser.clear();
                        final GroupByFunction first = (GroupByFunction) owner;
                        final GroupByFunction second = (GroupByFunction) worker;
                        first.initValueTypes(new ArrayColumnTypes());
                        second.initValueTypes(new ArrayColumnTypes());
                        first.setAllocator(firstAllocator);
                        second.setAllocator(secondAllocator);
                        owner.init(null, sqlExecutionContext);
                        worker.init(null, sqlExecutionContext);
                        first.computeFirst(firstValue, record(0, inputType, ArrayConstant.NULL, Double.NaN), 0);
                        first.computeNext(firstValue, record(0, inputType, a.getArray(null), 1), 10);
                        first.computeNext(firstValue, record(0, inputType, b.getArray(null), 2), 20);
                        first.computeNext(firstValue, record(0, inputType, ArrayConstant.NULL, Double.NaN), 30);
                        second.computeFirst(secondValue, record(1, inputType, w.getArray(null), 9), 5);
                        if (variant == 2 || variant == 3) {
                            Assert.assertTrue(owner.getArray(firstValue).isNull());
                        } else {
                            final double[] expected = switch (variant) {
                                case 0 -> new double[]{Double.NaN, 1, 2, Double.NaN};
                                case 1 -> new double[]{1, 2, 3, 4};
                                case 4 -> new double[]{1, 2};
                                default -> new double[]{3, 4};
                            };
                            assertValues(owner.getArray(firstValue), expected);
                        }
                        assertValues(worker.getArray(secondValue), variant == 0 ? new double[]{9} : new double[]{9, 10});
                        first.setNull(firstValue);
                        Assert.assertTrue(owner.getArray(firstValue).isNull());
                        owner = Misc.free(owner);
                        assertValues(worker.getArray(secondValue), variant == 0 ? new double[]{9} : new double[]{9, 10});
                    } finally {
                        Misc.free(owner);
                        Misc.free(worker);
                    }
                }
            }
        });
    }

    @Test
    public void testDimensionMismatchEqualityReleasesNativeConstantChildren() throws Exception {
        assertMemoryLeak(() -> {
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
            try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                for (int pass = 0; pass < 3; pass++) {
                    final ExpressionNode first = unary("array_sort", array(constant("2"), constant("1")));
                    final ExpressionNode second = array(array(constant("1"), constant("2")), array(constant("3"), constant("4")));
                    final BoundExpression expression = binder.bind(binary("=", first, second), new OutputSchema(), null, sqlExecutionContext);
                    try (Function result = binder.instantiate(expression, new OutputSchema(), sqlExecutionContext)) {
                        Assert.assertFalse(result.getBool(null));
                    }
                    binder.clear();
                    parser.clear();
                }
            }
        });
    }

    @Test
    public void testElementWiseAggregateRetainsIndependentFinalLayouts() throws Exception {
        assertMemoryLeak(() -> {
            final int type = ColumnType.encodeArrayType(ColumnType.DOUBLE, 2);
            for (String name : new String[]{"array_elem_sum", "array_elem_avg", "array_elem_min", "array_elem_max"}) {
                final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
                try (ArrayConstant firstInput = new ArrayConstant(new double[][]{{1, 2}, {3, 4}});
                     ArrayConstant secondInput = new ArrayConstant(new double[][]{{5, 6}, {7, 8}});
                     FastGroupByAllocator firstAllocator = new FastGroupByAllocator(1024, 4096);
                     FastGroupByAllocator secondAllocator = new FastGroupByAllocator(1024, 4096);
                     SimpleMapValue firstValue = new SimpleMapValue(5);
                     SimpleMapValue secondValue = new SimpleMapValue(5)) {
                    Function owner = null;
                    Function worker = null;
                    try {
                        try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                            final FunctionExpression expression = (FunctionExpression) binder.bindGroupByExpression(unary(name, literal()),
                                    schema(type, true), null, sqlExecutionContext);
                            Assert.assertTrue(expression.isAggregate());
                            Assert.assertFalse(expression.getOverload().isOrderSensitiveAggregate());
                            owner = binder.instantiateAggregate(expression, schema(type, false), metadata(type, false), sqlExecutionContext);
                            worker = binder.instantiateAggregate(expression, schema(type, true), metadata(type, true), sqlExecutionContext);
                            binder.clear();
                        }
                        parser.clear();
                        final GroupByFunction first = (GroupByFunction) owner;
                        final GroupByFunction second = (GroupByFunction) worker;
                        first.initValueTypes(new ArrayColumnTypes());
                        second.initValueTypes(new ArrayColumnTypes());
                        first.setAllocator(firstAllocator);
                        second.setAllocator(secondAllocator);
                        owner.init(null, sqlExecutionContext);
                        worker.init(null, sqlExecutionContext);
                        first.computeFirst(firstValue, record(0, type, ArrayConstant.NULL, 0), 0);
                        first.computeNext(firstValue, record(0, type, firstInput.getArray(null), 0), 1);
                        first.computeNext(firstValue, record(0, type, secondInput.getArray(null), 0), 2);
                        second.computeFirst(secondValue, record(1, type, secondInput.getArray(null), 0), 0);
                        assertValues(owner.getArray(firstValue), switch (name) {
                            case "array_elem_sum" -> new double[]{6, 8, 10, 12};
                            case "array_elem_avg" -> new double[]{3, 4, 5, 6};
                            case "array_elem_min" -> new double[]{1, 2, 3, 4};
                            default -> new double[]{5, 6, 7, 8};
                        });
                        owner = Misc.free(owner);
                        assertValues(worker.getArray(secondValue), new double[]{5, 6, 7, 8});
                        second.setNull(secondValue);
                        Assert.assertTrue(worker.getArray(secondValue).isNull());
                    } finally {
                        Misc.free(owner);
                        Misc.free(worker);
                    }
                }
            }
        });
    }

    @Test
    public void testElementWiseParametersReopenAndInvalidVariadicArgumentsReleaseChildren() throws Exception {
        assertMemoryLeak(() -> {
            try (ArrayConstant first = new ArrayConstant(new double[]{1, 2});
                 ArrayConstant second = new ArrayConstant(new double[]{3, 4, 5})) {
                for (String name : new String[]{"array_elem_sum", "array_elem_avg", "array_elem_min", "array_elem_max"}) {
                    final OutputSchema input = new OutputSchema();
                    try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, new FunctionParser(configuration, engine.getFunctionFactoryCache()))) {
                        final SqlException error = Assert.assertThrows(SqlException.class,
                                () -> binder.bindGroupByExpression(binary(name,
                                        unary("array_sort", array(constant("2"), constant("1"))), constant("1")), input, null, sqlExecutionContext));
                        TestUtils.assertContains(error.getFlyweightMessage(), "expected DOUBLE[] argument");
                        bindVariableService.setArray(0, first.getArray(null));
                        final BoundExpression expression = binder.bind(binary(name, variable("$1"), variable("$1")), input, null, sqlExecutionContext);
                        try (Function owner = binder.instantiate(expression, input, sqlExecutionContext);
                             Function worker = binder.instantiate(expression, input, sqlExecutionContext)) {
                            binder.clear();
                            for (int pass = 0; pass < 2; pass++) {
                                bindVariableService.setArray(0, pass == 0 ? first.getArray(null) : second.getArray(null));
                                owner.init(null, sqlExecutionContext);
                                worker.init(null, sqlExecutionContext);
                                final double[] expected = pass == 0 ? new double[]{1, 2} : new double[]{3, 4, 5};
                                if (name.equals("array_elem_sum")) {
                                    for (int i = 0; i < expected.length; i++) {
                                        expected[i] *= 2;
                                    }
                                }
                                assertValues(owner.getArray(null), expected);
                                assertValues(worker.getArray(null), expected);
                                Assert.assertNotSame(owner.getArray(null), worker.getArray(null));
                                owner.cursorClosed();
                                worker.cursorClosed();
                            }
                        }
                    }
                }
            } finally {
                bindVariableService.clear();
            }
        });
    }

    @Test
    public void testElementWiseScalarRelocatesAndRebuildsIndependentWorkerViews() throws Exception {
        assertMemoryLeak(() -> {
            for (int dimensions = 1; dimensions <= 2; dimensions++) {
                final int type = ColumnType.encodeArrayType(ColumnType.DOUBLE, dimensions);
                try (ArrayConstant value = dimensions == 1 ? new ArrayConstant(new double[]{1, 2})
                        : new ArrayConstant(new double[][]{{1, 2}, {3, 4}})) {
                    for (String name : new String[]{"array_elem_sum", "array_elem_avg", "array_elem_min", "array_elem_max"}) {
                        final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
                        Function owner = null;
                        Function worker = null;
                        try {
                            try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                                final FunctionExpression expression = (FunctionExpression) binder.bindGroupByExpression(
                                        ternary(name, literal(), literal(), literal()), schema(type, true), null, sqlExecutionContext);
                                Assert.assertFalse(expression.isAggregate());
                                Assert.assertTrue(expression.getOverload().isArrayElementWiseScalar());
                                owner = binder.instantiate(expression, schema(type, false), sqlExecutionContext);
                                worker = binder.instantiate(expression, schema(type, true), sqlExecutionContext);
                                Assert.assertNotSame(owner, worker);
                                binder.clear();
                            }
                            parser.clear();
                            for (int pass = 0; pass < 2; pass++) {
                                owner.init(null, sqlExecutionContext);
                                worker.init(null, sqlExecutionContext);
                                final ArrayView first = owner.getArray(record(0, type, value.getArray(null), 0));
                                final ArrayView second = worker.getArray(record(1, type, value.getArray(null), 0));
                                Assert.assertNotSame(first, second);
                                Assert.assertEquals(type, owner.getType());
                                final double[] expected = dimensions == 1 ? new double[]{1, 2} : new double[]{1, 2, 3, 4};
                                if (name.equals("array_elem_sum")) {
                                    for (int i = 0; i < expected.length; i++) {
                                        expected[i] *= 3;
                                    }
                                }
                                assertValues(first, expected);
                                assertValues(second, expected);
                                Assert.assertTrue(owner.getArray(record(0, type, ArrayConstant.NULL, 0)).isNull());
                                assertValues(worker.getArray(record(1, type, value.getArray(null), 0)), expected);
                                owner.cursorClosed();
                                worker.cursorClosed();
                            }
                        } finally {
                            Misc.free(owner);
                            Misc.free(worker);
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testGroupByCandidateSelectsOnceAndKeepsOtherContextsStrict() throws Exception {
        assertMemoryLeak(() -> {
            final int[] constructions = {0};
            final FunctionParser parser = new FunctionParser(configuration, new FunctionResolver(configuration, engine.getFunctionFactoryCache()) {
                @Override
                public Function createFunction(FunctionFactoryDescriptor overload, int position, CharSequence name,
                                               ObjList<Function> args, IntList positions, SqlExecutionContext context) throws SqlException {
                    constructions[0]++;
                    return super.createFunction(overload, position, name, args, positions, context);
                }
            });
            final OutputSchema input = schema(ColumnType.encodeArrayType(ColumnType.DOUBLE, 1), false);
            try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                for (String name : new String[]{"array_elem_sum", "array_elem_avg", "array_elem_min", "array_elem_max"}) {
                    constructions[0] = 0;
                    Assert.assertThrows(IllegalStateException.class, () -> binder.bindAggregate(binary(name, literal(), literal()), input, null, sqlExecutionContext));
                    Assert.assertThrows(SqlException.class, () -> binder.bind(unary(name, literal()), input, null, sqlExecutionContext));
                    Assert.assertThrows(SqlException.class, () -> binder.bindGroupByExpression(
                            binary(name, unary(name, literal()), literal()), input, null, sqlExecutionContext));
                    Assert.assertEquals(0, constructions[0]);
                    Assert.assertFalse(((FunctionExpression) binder.bindGroupByExpression(binary(name, literal(), literal()), input, null, sqlExecutionContext)).isAggregate());
                    Assert.assertEquals(1, constructions[0]);
                    Assert.assertTrue(((FunctionExpression) binder.bindGroupByExpression(unary(name, literal()), input, null, sqlExecutionContext)).isAggregate());
                    Assert.assertEquals(2, constructions[0]);
                    binder.clear();
                }
            }
        });
    }

    @Test
    public void testScalarRegistrationsRelocateAndRebuildIndependentWorkers() throws Exception {
        assertMemoryLeak(() -> {
            final int type = ColumnType.encodeArrayType(ColumnType.DOUBLE, 1);
            try (ArrayConstant value = new ArrayConstant(new double[]{1.25, 2.5, 3.75})) {
                for (int variant = 0; variant < 14; variant++) {
                    bindVariableService.setDouble(0, 2.5);
                    bindVariableService.setInt(1, 1);
                    bindVariableService.setBoolean(2, true);
                    final ExpressionNode node = switch (variant) {
                        case 0 -> binary("array_position", literal(), constant("2.5"));
                        case 1 -> binary("array_position", literal(), variable("$1"));
                        case 2 -> binary("insertion_point", literal(), variable("$1"));
                        case 3 -> ternary("insertion_point", literal(), variable("$1"), variable("$3"));
                        case 4 -> unary("flatten", literal());
                        case 5 -> unary("transpose", literal());
                        case 6 -> unary("array_cum_sum", literal());
                        case 7 -> binary("shift", literal(), variable("$2"));
                        case 8 -> ternary("shift", literal(), variable("$2"), variable("$1"));
                        case 9 -> binary("round", literal(), constant("1"));
                        case 10 -> binary("round", literal(), variable("$2"));
                        case 11 -> binary("=", literal(), literal());
                        case 12 -> binary("!=", literal(), literal());
                        default -> binary("<>", literal(), literal());
                    };
                    final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
                    Function owner = null;
                    Function worker = null;
                    try {
                        try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                            final BoundExpression expression = binder.bind(node, schema(type, true), null, sqlExecutionContext);
                            owner = binder.instantiate(expression, schema(type, false), sqlExecutionContext);
                            worker = binder.instantiate(expression, schema(type, true), sqlExecutionContext);
                            Assert.assertNotSame(owner, worker);
                            binder.clear();
                        }
                        parser.clear();
                        for (int pass = 0; pass < 2; pass++) {
                            owner.init(null, sqlExecutionContext);
                            worker.init(null, sqlExecutionContext);
                            final Record a = record(0, type, value.getArray(null), 0);
                            final Record b = record(1, type, value.getArray(null), 0);
                            if (variant < 4) {
                                final int expected = variant == 2 ? 3 : 2;
                                Assert.assertEquals(expected, owner.getInt(a));
                                Assert.assertEquals(expected, worker.getInt(b));
                            } else if (variant >= 11) {
                                Assert.assertEquals(variant == 11, owner.getBool(a));
                                Assert.assertEquals(variant == 11, worker.getBool(b));
                            } else {
                                final double[] expected = switch (variant) {
                                    case 6 -> new double[]{1.25, 3.75, 7.5};
                                    case 7 -> new double[]{Double.NaN, 1.25, 2.5};
                                    case 8 -> new double[]{2.5, 1.25, 2.5};
                                    case 9, 10 -> new double[]{1.3, 2.5, 3.8};
                                    default -> new double[]{1.25, 2.5, 3.75};
                                };
                                final ArrayView first = owner.getArray(a);
                                final ArrayView second = worker.getArray(b);
                                Assert.assertNotSame(first, second);
                                assertValues(first, expected);
                                assertValues(second, expected);
                                Assert.assertFalse(owner.isThreadSafe());
                            }
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

    private static ExpressionNode array(ExpressionNode left, ExpressionNode right) {
        final ExpressionNode node = binary("array", left, right);
        node.type = ExpressionNode.ARRAY_CONSTRUCTOR;
        return node;
    }

    private static void assertValues(ArrayView actual, double[] expected) {
        Assert.assertFalse(actual.isNull());
        Assert.assertEquals(expected.length, actual.getCardinality());
        for (int i = 0; i < expected.length; i++) {
            Assert.assertEquals(expected[i], actual.getDouble(i), 1e-12);
        }
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

    private static ExpressionNode literal() {
        return ExpressionNode.FACTORY.newInstance().of(ExpressionNode.LITERAL, "v", 0, 1);
    }

    private static GenericRecordMetadata metadata(int type, boolean isFull) {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        if (isFull) {
            metadata.add(new TableColumnMetadata("unused", ColumnType.INT));
        }
        metadata.add(new TableColumnMetadata("v", type));
        return metadata;
    }

    private static Record record(int index, int type, ArrayView array, double value) {
        return new Record() {
            @Override
            public ArrayView getArray(int columnIndex, int columnType) {
                Assert.assertEquals(index, columnIndex);
                Assert.assertEquals(type, columnType);
                return array;
            }

            @Override
            public double getDouble(int columnIndex) {
                Assert.assertEquals(index, columnIndex);
                return value;
            }
        };
    }

    private static OutputSchema schema(int type, boolean isFull) {
        final OutputSchema schema = new OutputSchema();
        if (isFull) {
            schema.add(1, "unused", ColumnType.INT, true);
        }
        return schema.add(70, "v", type, true);
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
