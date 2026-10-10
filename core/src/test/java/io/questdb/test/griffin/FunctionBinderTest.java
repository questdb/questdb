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
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.FunctionFactoryCache;
import io.questdb.griffin.FunctionFactoryDescriptor;
import io.questdb.griffin.FunctionParser;
import io.questdb.griffin.FunctionResolver;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.IntFunction;
import io.questdb.griffin.engine.functions.TernaryFunction;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.bool.OrFunctionFactory;
import io.questdb.griffin.engine.functions.columns.BindableColumn;
import io.questdb.griffin.engine.functions.columns.ColumnFunction;
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.griffin.engine.functions.constants.IntConstant;
import io.questdb.griffin.engine.functions.lt.LtLongFunctionFactory;
import io.questdb.griffin.engine.functions.math.AddIntFunctionFactory;
import io.questdb.griffin.engine.functions.str.ConcatFunctionFactory;
import io.questdb.griffin.engine.functions.str.SubStringFunctionFactory;
import io.questdb.griffin.engine.groupby.FlyweightPackedMapValue;
import io.questdb.griffin.engine.groupby.SimpleMapValue;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.BindVariableExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.ConstantExpression;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.ScanPlan;
import io.questdb.griffin.plan.logical.TypeExpression;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class FunctionBinderTest extends AbstractCairoTest {
    @Test
    public void testAdditionalConsumerRebuildsSelectedAliasesWithIndependentLeaves() throws Exception {
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
            final OutputSchema original = new OutputSchema().add(1, "unused", ColumnType.INT, true)
                    .add(7, "value", ColumnType.INT, true);
            final OutputSchema pruned = new OutputSchema().add(7, "value", ColumnType.INT, true);
            try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                final BoundExpression expression = binder.bind(binary("<=", 6,
                                binary("+", 3, literal("value", 0), constant("1", 5)), constant("4", 9)),
                        original, "t", sqlExecutionContext);
                try (
                        Function first = binder.instantiate(expression, original, sqlExecutionContext);
                        Function second = binder.instantiate(expression, pruned, sqlExecutionContext)
                ) {
                    Assert.assertEquals(4, constructions[0]);
                    Assert.assertNotSame(first, second);
                    binder.clear();
                    Assert.assertTrue(first.getBool(new Record() {
                        @Override
                        public int getInt(int columnIndex) {
                            Assert.assertEquals(1, columnIndex);
                            return 3;
                        }
                    }));
                    Assert.assertTrue(second.getBool(intRecord(3)));
                    Assert.assertFalse(second.getBool(intRecord(4)));
                }
            }
        });
    }

    @Test
    public void testAdoptsOnceAndSurvivesCompilerStorageReuse() throws Exception {
        assertMemoryLeak(() -> {
            final int[] constructions = {0};
            final ObjList<Function> constructed = new ObjList<>();
            final FunctionParser parser = new FunctionParser(configuration, new FunctionResolver(configuration, engine.getFunctionFactoryCache()) {
                @Override
                public Function createFunction(FunctionFactoryDescriptor overload, int position, CharSequence name,
                                               ObjList<Function> args, IntList positions, SqlExecutionContext context) throws SqlException {
                    constructions[0]++;
                    final Function function = super.createFunction(overload, position, name, args, positions, context);
                    constructed.add(function);
                    return function;
                }
            });
            final OutputSchema original = new OutputSchema().add(10, "unused", ColumnType.INT, true)
                    .add(27, "i", ColumnType.INT, true);
            final OutputSchema pruned = new OutputSchema().add(27, "i", ColumnType.INT, true);
            try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                final BoundExpression expression = binder.bind(binary("=", 5,
                                binary("&", 2, literal("t.i", 0), constant("7", 4)), constant("3", 7)),
                        original, "t", sqlExecutionContext);
                Assert.assertEquals(2, constructions[0]);
                final FunctionExpression call = (FunctionExpression) expression;
                TestUtils.assertEquals("=(II)", call.getSignature());
                Assert.assertEquals(2, call.getArgumentPosition(0));
                Assert.assertEquals(7, call.getArgumentPosition(1));
                Assert.assertEquals(27, ((ColumnExpression) ((FunctionExpression) call.argumentAt(0)).argumentAt(0)).getColumnId());
                try (Function function = binder.instantiate(expression, pruned)) {
                    Assert.assertSame(constructed.getQuick(1), function);
                    Assert.assertEquals(2, constructions[0]);
                    binder.clear();
                    original.clear();
                    pruned.clear();
                    parser.clear();
                    Assert.assertTrue(function.getBool(intRecord(3)));
                    Assert.assertFalse(function.getBool(intRecord(4)));
                }
            }
        });
    }

    @Test
    public void testAggregateContextRejectsNestedAndScalarRootsBeforeConstruction() throws Exception {
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
            final OutputSchema input = new OutputSchema().add(27, "i", ColumnType.INT, true);
            try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                try {
                    binder.bindAggregate(unary("sum", 0, unary("max", 4, literal("i", 8))), input, null, sqlExecutionContext);
                    Assert.fail("nested aggregate admitted");
                } catch (SqlException e) {
                    Assert.assertEquals(4, e.getPosition());
                    TestUtils.assertContains(e.getFlyweightMessage(), "Aggregate function cannot be passed as an argument");
                }
                Assert.assertThrows(AssertionError.class,
                        () -> binder.bindAggregate(unary("abs", 2, literal("i", 6)), input, null, sqlExecutionContext));
                Assert.assertThrows(AssertionError.class, () -> binder.bindAggregate(binary("cast", 2, literal("i", 7), constant("int", 12)),
                        input, null, sqlExecutionContext));
                try {
                    binder.bind(unary("sum", 3, literal("i", 7)), input, null, sqlExecutionContext);
                    Assert.fail("aggregate admitted in scalar context");
                } catch (SqlException e) {
                    Assert.assertEquals(3, e.getPosition());
                }
                Assert.assertEquals(0, constructions[0]);
                final FunctionExpression retry = binder.bindAggregate(unary("sum", 3, literal("i", 7)), input, null, sqlExecutionContext);
                Assert.assertTrue(retry.isAggregate());
                Assert.assertEquals(1, constructions[0]);
            }
        });
    }

    @Test
    public void testAggregateFinalLayoutReconstructionCapturesPrunedColumnIndex() throws Exception {
        assertMemoryLeak(() -> {
            final ObjList<Function> constructed = new ObjList<>();
            final FunctionParser parser = new FunctionParser(configuration, new FunctionResolver(configuration, engine.getFunctionFactoryCache()) {
                @Override
                public Function createFunction(FunctionFactoryDescriptor overload, int position, CharSequence name,
                                               ObjList<Function> args, IntList positions, SqlExecutionContext context) throws SqlException {
                    final Function function = super.createFunction(overload, position, name, args, positions, context);
                    constructed.add(function);
                    return function;
                }
            });
            final OutputSchema original = new OutputSchema().add(10, "unused", ColumnType.INT, true)
                    .add(27, "i", ColumnType.INT, true);
            final OutputSchema pruned = new OutputSchema().add(27, "i", ColumnType.INT, true);
            final GenericRecordMetadata metadata = new GenericRecordMetadata();
            metadata.add(new TableColumnMetadata("physical_name", ColumnType.INT));
            final GenericRecordMetadata originalMetadata = new GenericRecordMetadata();
            originalMetadata.add(new TableColumnMetadata("unused", ColumnType.INT));
            originalMetadata.add(new TableColumnMetadata("physical_name", ColumnType.INT));
            final int[] pageLookups = {0, 0};
            try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                final FunctionExpression expression = binder.bindAggregate(unary("sum", 0, literal("i", 4)),
                        original, "t", sqlExecutionContext);
                Assert.assertEquals(1, constructed.size());
                Assert.assertFalse(((UnaryFunction) constructed.getQuick(0)).getArg() instanceof ColumnFunction);
                try (
                        Function first = binder.instantiateAggregate(expression, pruned, metadata, sqlExecutionContext);
                        Function second = binder.instantiateAggregate(expression, original, originalMetadata, sqlExecutionContext);
                        PageFrameMemoryRecord firstFrame = new PageFrameMemoryRecord() {
                            @Override
                            public long getPageAddress(int columnIndex) {
                                Assert.assertEquals(0, columnIndex);
                                pageLookups[0]++;
                                return 0;
                            }
                        };
                        PageFrameMemoryRecord secondFrame = new PageFrameMemoryRecord() {
                            @Override
                            public long getPageAddress(int columnIndex) {
                                Assert.assertEquals(1, columnIndex);
                                pageLookups[1]++;
                                return 0;
                            }
                        };
                        SimpleMapValue values = new SimpleMapValue(2)
                ) {
                    Assert.assertEquals(3, constructed.size());
                    Assert.assertSame(constructed.getQuick(1), first);
                    Assert.assertSame(constructed.getQuick(2), second);
                    Assert.assertEquals(0, ((ColumnFunction) ((UnaryFunction) first).getArg()).getColumnIndex());
                    Assert.assertEquals(1, ((ColumnFunction) ((UnaryFunction) second).getArg()).getColumnIndex());
                    try {
                        binder.instantiate(expression, pruned);
                        Assert.fail("unused preparation still owned after final-layout reconstruction");
                    } catch (IllegalStateException e) {
                        TestUtils.assertContains(e.getMessage(), "bound function is not owned");
                    }
                    final GroupByFunction firstAggregate = (GroupByFunction) first;
                    final GroupByFunction secondAggregate = (GroupByFunction) second;
                    final ArrayColumnTypes valueTypes = new ArrayColumnTypes();
                    firstAggregate.initValueTypes(valueTypes);
                    secondAggregate.initValueTypes(valueTypes);
                    final FlyweightPackedMapValue packed = new FlyweightPackedMapValue(valueTypes);
                    // Even an empty batch consults the constructor-captured index.
                    firstAggregate.computeKeyedBatch(firstFrame, packed, 0, 0, 0, 0);
                    secondAggregate.computeKeyedBatch(secondFrame, packed, 0, 0, 0, 0);
                    Assert.assertEquals(1, pageLookups[0]);
                    Assert.assertEquals(1, pageLookups[1]);
                    binder.clear();
                    firstAggregate.computeFirst(values, numericRecord(ColumnType.INT, 0, 2), 0);
                    secondAggregate.computeFirst(values, numericRecord(ColumnType.INT, 1, 6), 0);
                    Assert.assertEquals(2, first.getLong(values));
                    Assert.assertEquals(6, second.getLong(values));
                }
            }
        });
    }

    @Test
    public void testAggregateOutputSubstitutionSkipsOnlySelectedOccurrences() throws Exception {
        assertMemoryLeak(() -> {
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
            final OutputSchema input = new OutputSchema().add(41, "result", ColumnType.LONG, true);
            final ExpressionNode aggregate = unary("sum", 4, literal("unavailable", 8));
            final ObjList<ExpressionNode> nodes = new ObjList<>();
            final ObjList<ColumnExpression> replacements = new ObjList<>();
            nodes.add(aggregate);
            replacements.add(new ColumnExpression().of(41, ColumnType.LONG, 99));
            try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                final BoundExpression root = binder.bind(aggregate, input, null, nodes, replacements, sqlExecutionContext);
                Assert.assertEquals(4, root.getPosition());
                Assert.assertEquals(41, ((ColumnExpression) root).getColumnId());
                final FunctionExpression expression = (FunctionExpression) binder.bind(
                        binary("+", 0, aggregate, constant("1", 20)), input, null, nodes, replacements, sqlExecutionContext);
                Assert.assertEquals(4, expression.getArgumentPosition(0));
                Assert.assertEquals(41, ((ColumnExpression) expression.argumentAt(0)).getColumnId());
                try (Function function = binder.instantiate(expression, input, sqlExecutionContext)) {
                    Assert.assertEquals(8, function.getLong(numericRecord(ColumnType.LONG, 0, 7)));
                }
                try {
                    binder.bind(binary("+", 0, aggregate, unary("sum", 23, literal("result", 27))),
                            input, null, nodes, replacements, sqlExecutionContext);
                    Assert.fail("unselected aggregate occurrence replaced");
                } catch (SqlException e) {
                    Assert.assertEquals(23, e.getPosition());
                }
                // Failure must release the borrowed lists as well as partial roots.
                try {
                    binder.bind(aggregate, input, null, sqlExecutionContext);
                    Assert.fail("replacement escaped its bind call");
                } catch (SqlException e) {
                    Assert.assertEquals(8, e.getPosition());
                    TestUtils.assertContains(e.getFlyweightMessage(), "Invalid column");
                }
                final BoundExpression retry = binder.bind(aggregate, input, null, nodes, replacements, sqlExecutionContext);
                try (Function function = binder.instantiate(retry, input, sqlExecutionContext)) {
                    nodes.clear();
                    replacements.clear();
                    binder.clear();
                    Assert.assertEquals(7, function.getLong(numericRecord(ColumnType.LONG, 0, 7)));
                }
            }
        });
    }

    @Test
    public void testAggregateScalarArgumentsAndNullFolding() throws Exception {
        assertMemoryLeak(() -> {
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
            final OutputSchema input = new OutputSchema().add(27, "i", ColumnType.INT, true);
            try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                final FunctionExpression expression = binder.bindAggregate(unary("sum", 0,
                        binary("+", 6, literal("i", 4), constant("1", 8))), input, "t", sqlExecutionContext);
                Assert.assertTrue(expression.isAggregate());
                final FunctionExpression argument = (FunctionExpression) expression.argumentAt(0);
                Assert.assertFalse(argument.isAggregate());
                TestUtils.assertEquals("+(II)", argument.getSignature());
                try (
                        Function first = binder.instantiate(expression, input, sqlExecutionContext);
                        Function second = binder.instantiate(expression, input, sqlExecutionContext);
                        SimpleMapValue values = new SimpleMapValue(2)
                ) {
                    final GroupByFunction firstAggregate = (GroupByFunction) first;
                    final GroupByFunction secondAggregate = (GroupByFunction) second;
                    firstAggregate.initValueIndex(0);
                    secondAggregate.initValueIndex(1);
                    firstAggregate.computeFirst(values, intRecord(2), 0);
                    firstAggregate.computeNext(values, intRecord(6), 1);
                    secondAggregate.computeFirst(values, intRecord(9), 0);
                    Assert.assertEquals(10, first.getLong(values));
                    Assert.assertEquals(10, second.getLong(values));
                }
                final FunctionExpression folded = binder.bindAggregate(unary("sum", 0,
                        binary("+", 6, literal("i", 4), constant("NULL", 8))), input, "t", sqlExecutionContext);
                Assert.assertTrue(folded.argumentAt(0) instanceof ConstantExpression);
                final OutputSchema empty = new OutputSchema();
                try (
                        Function first = binder.instantiate(folded, empty, sqlExecutionContext);
                        Function second = binder.instantiate(folded, empty, sqlExecutionContext);
                        SimpleMapValue values = new SimpleMapValue(2)
                ) {
                    final GroupByFunction firstAggregate = (GroupByFunction) first;
                    final GroupByFunction secondAggregate = (GroupByFunction) second;
                    firstAggregate.initValueIndex(0);
                    secondAggregate.initValueIndex(1);
                    binder.clear();
                    firstAggregate.computeFirst(values, null, 0);
                    secondAggregate.computeFirst(values, null, 0);
                    Assert.assertEquals(Numbers.LONG_NULL, first.getLong(values));
                    Assert.assertEquals(Numbers.LONG_NULL, second.getLong(values));
                }
            }
        });
    }

    @Test
    public void testAmbiguousAndUnknownQualifiedColumnsFailAtReferencePosition() throws Exception {
        assertMemoryLeak(() -> {
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
            final OutputSchema input = new OutputSchema()
                    .add(7, "id", ColumnType.INT, null, true, "left")
                    .add(12, "ID", ColumnType.INT, null, true, "right")
                    .add(13, "unique", ColumnType.INT, true);
            try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                assertBindingError(binder, input, "id", "left", "Ambiguous column [name=id]");
                assertBindingError(binder, input, "other.id", "other", "Invalid table name or alias");
                assertBindingError(binder, input, "left.missing", "left", "Invalid column: left.missing");
                assertBindingError(binder, input, "left.unique", "left", "Invalid column: left.unique");
                Assert.assertEquals(12, ((ColumnExpression) binder.bind(literal("right.id", 9), input, null, sqlExecutionContext)).getColumnId());
                Assert.assertEquals(13, ((ColumnExpression) binder.bind(literal("unique", 9), input, null, sqlExecutionContext)).getColumnId());
                input.clear();
                input.add(7, "id", ColumnType.INT, true).add(12, "id", ColumnType.INT, true);
                assertBindingError(binder, input, "id", null, "Ambiguous column [name=id]");
            }
        });
    }

    @Test
    public void testCaseInferenceFailureClosesPreviouslyConstructedNativeIn() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.clear();
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
            final OutputSchema input = new OutputSchema().add(7, "v", ColumnType.LONG, true);
            final ObjList<ExpressionNode> in = new ObjList<>();
            in.add(literal("v", 10));
            in.add(constant("1", 15));
            in.add(constant("2", 18));
            in.add(constant("3", 21));
            final ObjList<ExpressionNode> arguments = new ObjList<>();
            arguments.add(call("in", 12, in));
            arguments.add(constant("1", 29));
            arguments.add(parameter("$1", 36));
            try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                try {
                    binder.bind(call("case", 0, arguments), input, "t", sqlExecutionContext);
                    Assert.fail();
                } catch (SqlException e) {
                    Assert.assertEquals(36, e.getPosition());
                    TestUtils.assertContains(e.getFlyweightMessage(), "CASE values cannot be bind variables");
                }
                binder.clear();
                arguments.remove(2);
                final BoundExpression recovered = binder.bind(call("case", 0, arguments), input, "t", sqlExecutionContext);
                try (Function function = binder.instantiate(recovered, input, sqlExecutionContext)) {
                    Assert.assertEquals(1, function.getInt(numericRecord(ColumnType.LONG, 0, 2)));
                    Assert.assertEquals(Numbers.INT_NULL, function.getInt(numericRecord(ColumnType.LONG, 0, 4)));
                }
            }
        });
    }

    @Test
    public void testCaseWideningPreservesNativeBranchesAndRebuilds() throws Exception {
        assertMemoryLeak(() -> {
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
            final OutputSchema input = new OutputSchema().add(1, "unused", ColumnType.INT, true)
                    .add(5, "flag", ColumnType.BOOLEAN, true).add(7, "i", ColumnType.INT, true).add(8, "l", ColumnType.LONG, true);
            final OutputSchema reordered = new OutputSchema().add(8, "l", ColumnType.LONG, true)
                    .add(5, "flag", ColumnType.BOOLEAN, true).add(7, "i", ColumnType.INT, true);
            final ObjList<ExpressionNode> arguments = new ObjList<>();
            arguments.add(literal("flag", 5));
            arguments.add(literal("i", 15));
            arguments.add(literal("l", 22));
            try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                final FunctionExpression expression = (FunctionExpression) binder.bind(call("case", 0, arguments), input, "t", sqlExecutionContext);
                TestUtils.assertEquals("case(V)", expression.getSignature());
                Assert.assertEquals(ColumnType.LONG, expression.getDataType());
                Assert.assertEquals(ColumnType.INT, expression.argumentAt(1).getDataType());
                Assert.assertEquals(15, expression.getArgumentPosition(1));
                try (Function first = binder.instantiate(expression, reordered, sqlExecutionContext);
                     Function second = binder.instantiate(expression, reordered, sqlExecutionContext)) {
                    binder.clear();
                    for (boolean flag : new boolean[]{true, false}) {
                        final Record record = new Record() {
                            @Override
                            public boolean getBool(int index) {
                                Assert.assertEquals(1, index);
                                return flag;
                            }

                            @Override
                            public int getInt(int index) {
                                Assert.assertTrue(flag);
                                Assert.assertEquals(2, index);
                                return 7;
                            }

                            @Override
                            public long getLong(int index) {
                                Assert.assertFalse(flag);
                                Assert.assertEquals(0, index);
                                return 9_000_000_000L;
                            }
                        };
                        Assert.assertEquals(flag ? 7 : 9_000_000_000L, first.getLong(record));
                        Assert.assertEquals(flag ? 7 : 9_000_000_000L, second.getLong(record));
                    }
                }
            }
        });
    }

    @Test
    public void testCastCapturesTypeArgumentAndSameTypeReturn() throws Exception {
        assertMemoryLeak(() -> {
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
            final OutputSchema input = new OutputSchema().add(7, "i", ColumnType.INT, true);
            try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                final FunctionExpression cast = (FunctionExpression) binder.bind(
                        binary("cast", 0, literal("i", 5), constant("long", 10)), input, "t", sqlExecutionContext);
                TestUtils.assertEquals("cast(Il)", cast.getSignature());
                Assert.assertTrue(cast.argumentAt(1) instanceof TypeExpression);
                Assert.assertEquals(ColumnType.LONG, cast.argumentAt(1).getDataType());
                try (Function function = binder.instantiate(cast, input, sqlExecutionContext)) {
                    Assert.assertEquals(3L, function.getLong(intRecord(3)));
                    Assert.assertEquals(Long.MIN_VALUE, function.getLong(intRecord(Integer.MIN_VALUE)));
                }
                final BoundExpression identity = binder.bind(
                        binary("cast", 0, literal("i", 5), constant("int", 10)), input, "t", sqlExecutionContext);
                Assert.assertTrue(identity instanceof ColumnExpression);
                Assert.assertFalse(((ColumnExpression) identity).isDirectReference());
                Assert.assertEquals(0, identity.getPosition());
                try (Function function = binder.instantiate(identity, input)) {
                    Assert.assertEquals(4, function.getInt(intRecord(4)));
                }
                for (String type : new String[]{"int", "long", "float", "double"}) {
                    final BoundExpression nullCast = binder.bind(binary("cast", 0,
                            constant("null", 5), constant(type, 13)), input, "t", sqlExecutionContext);
                    Assert.assertEquals(ColumnType.typeOf(type), nullCast.getDataType());
                    try (Function function = binder.instantiate(nullCast, input, sqlExecutionContext)) {
                        Assert.assertTrue(function.isNullConstant());
                    }
                }
            }
        });
    }

    @Test
    public void testConjunctParameterInfersBooleanBeforeComposition() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.clear();
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
            final OutputSchema input = new OutputSchema();
            try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                final BoundExpression parameter = binder.bindPredicate(parameter("$1", 7), input, null,
                        null, ColumnType.BOOLEAN, sqlExecutionContext);
                Assert.assertEquals(ColumnType.BOOLEAN, parameter.getDataType());
                Assert.assertEquals(ColumnType.BOOLEAN, bindVariableService.getFunction(0).getType());
                Assert.assertTrue((parameter.getFunctionFlags() & BoundExpression.RUNTIME_CONSTANT) != 0);
                try (Function first = binder.instantiate(parameter, input, sqlExecutionContext);
                     Function second = binder.instantiate(parameter, input, sqlExecutionContext)) {
                    bindVariableService.setBoolean(0, true);
                    first.init(null, sqlExecutionContext);
                    second.init(null, sqlExecutionContext);
                    Assert.assertTrue(first.getBool(null));
                    Assert.assertTrue(second.getBool(null));
                    binder.clear();
                    bindVariableService.setBoolean(0, false);
                    first.init(null, sqlExecutionContext);
                    second.init(null, sqlExecutionContext);
                    Assert.assertFalse(first.getBool(null));
                    Assert.assertFalse(second.getBool(null));
                }
            }
        });
    }

    @Test
    public void testConstantArgumentsLeaveCallsUnconstructedUntilGeneration() throws Exception {
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
            final OutputSchema input = new OutputSchema().add(7, "i", ColumnType.INT, true);
            try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                final FunctionExpression expression = (FunctionExpression) binder.bind(binary("=", 5,
                                binary("+", 2, literal("i", 0), binary("*", 9, constant("2", 8), constant("3", 10))), constant("10", 12)),
                        input, "t", sqlExecutionContext);
                Assert.assertEquals(1, constructions[0]);
                Assert.assertTrue(((FunctionExpression) expression.argumentAt(0)).argumentAt(1) instanceof ConstantExpression);
                final BoundExpression nullSum = binder.bind(binary("+", 2, literal("i", 0), constant("null", 4)), input, "t", sqlExecutionContext);
                Assert.assertTrue(nullSum instanceof ConstantExpression);
                Assert.assertEquals(2, constructions[0]);
                try (Function function = binder.instantiate(expression, input, sqlExecutionContext)) {
                    Assert.assertEquals(4, constructions[0]);
                    Assert.assertTrue(function.getBool(intRecord(4)));
                    Assert.assertFalse(function.getBool(intRecord(5)));
                }
            }
        });
    }

    @Test
    public void testCountAggregateBindingOwnsOnePreparationAndIndependentReconstruction() throws Exception {
        assertMemoryLeak(() -> {
            final ObjList<Function> constructed = new ObjList<>();
            final FunctionParser parser = new FunctionParser(configuration, new FunctionResolver(configuration, engine.getFunctionFactoryCache()) {
                @Override
                public Function createFunction(FunctionFactoryDescriptor overload, int position, CharSequence name,
                                               ObjList<Function> args, IntList positions, SqlExecutionContext context) throws SqlException {
                    final Function function = super.createFunction(overload, position, name, args, positions, context);
                    constructed.add(function);
                    return function;
                }
            });
            final OutputSchema input = new OutputSchema();
            final ExpressionNode count = ExpressionNode.FACTORY.newInstance().of(ExpressionNode.FUNCTION, "count", 0, 7);
            try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                try {
                    binder.bind(count, input, null, sqlExecutionContext);
                    Assert.fail("aggregate admitted in scalar context");
                } catch (SqlException e) {
                    Assert.assertEquals(7, e.getPosition());
                    TestUtils.assertContains(e.getFlyweightMessage(), "aggregate functions are not allowed in this context");
                }
                Assert.assertEquals(0, constructed.size());
                final FunctionExpression expression = binder.bindAggregate(count, input, null, sqlExecutionContext);
                Assert.assertEquals(1, constructed.size());
                Assert.assertEquals(0, expression.getArgumentCount());
                Assert.assertEquals(ColumnType.LONG, expression.getDataType());
                Assert.assertEquals(7, expression.getPosition());
                TestUtils.assertEquals("count()", expression.getSignature());
                Assert.assertTrue(expression.isAggregate());
                Assert.assertFalse(expression.getOverload().isRelocatableScalar());
                try (
                        Function first = binder.instantiateAggregate(expression, input, new GenericRecordMetadata(), sqlExecutionContext);
                        Function second = binder.instantiateAggregate(expression, input, new GenericRecordMetadata(), sqlExecutionContext)
                ) {
                    Assert.assertEquals(2, constructed.size());
                    Assert.assertSame(constructed.getQuick(0), first);
                    Assert.assertSame(constructed.getQuick(1), second);
                    final GroupByFunction firstCount = (GroupByFunction) first;
                    final GroupByFunction secondCount = (GroupByFunction) second;
                    firstCount.initValueIndex(3);
                    secondCount.initValueIndex(9);
                    binder.clear();
                    Assert.assertEquals(3, firstCount.getValueIndex());
                    Assert.assertEquals(9, secondCount.getValueIndex());
                    Assert.assertEquals(42, first.getLong(new Record() {
                        @Override
                        public long getLong(int index) {
                            Assert.assertEquals(3, index);
                            return 42;
                        }
                    }));
                }
                final ExpressionNode invalidAggregate = ExpressionNode.FACTORY.newInstance().of(ExpressionNode.FUNCTION, "ksum", 0, 11);
                try {
                    binder.bindAggregate(invalidAggregate, input, null, sqlExecutionContext);
                    Assert.fail("aggregate with invalid argument admitted");
                } catch (SqlException e) {
                    Assert.assertEquals(11, e.getPosition());
                }
                Assert.assertEquals(2, constructed.size());
                final FunctionExpression retry = binder.bindAggregate(count, input, null, sqlExecutionContext);
                Assert.assertEquals(3, constructed.size());
                Assert.assertEquals(0, retry.getArgumentCount());
                // Leave the final preparation unclaimed: compiler cleanup owns it.
            }
        });
    }

    @Test
    public void testDateAndCharComparisonsUseTheirNativeGetters() throws Exception {
        assertMemoryLeak(() -> {
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
            final OutputSchema dates = new OutputSchema().add(1, "unused", ColumnType.INT, true)
                    .add(8, "d", ColumnType.DATE, true);
            final OutputSchema prunedDates = new OutputSchema().add(8, "d", ColumnType.DATE, true);
            try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                final FunctionExpression date = (FunctionExpression) binder.bind(binary("=", 2,
                        literal("d", 0), constant("'1970-01-01'", 4)), dates, "t", sqlExecutionContext);
                Assert.assertEquals(ColumnType.DATE, date.argumentAt(1).getDataType());
                try (Function first = binder.instantiate(date, prunedDates, sqlExecutionContext);
                     Function second = binder.instantiate(date, prunedDates, sqlExecutionContext)) {
                    Assert.assertTrue(first.getBool(numericRecord(ColumnType.DATE, 0, 0)));
                    Assert.assertTrue(second.getBool(numericRecord(ColumnType.DATE, 0, 0)));
                    Assert.assertFalse(second.getBool(numericRecord(ColumnType.DATE, 0, 1)));
                }
                final OutputSchema chars = new OutputSchema().add(8, "c", ColumnType.CHAR, true);
                bindVariableService.clear();
                bindVariableService.setChar(0, 'b');
                for (String op : new String[]{"=", "!=", "<", ">="}) {
                    final BoundExpression expression = binder.bind(binary(op, 2,
                            literal("c", 0), parameter("$1", 5)), chars, "t", sqlExecutionContext);
                    try (Function first = binder.instantiate(expression, chars, sqlExecutionContext);
                         Function second = binder.instantiate(expression, chars, sqlExecutionContext)) {
                        first.init(null, sqlExecutionContext);
                        second.init(null, sqlExecutionContext);
                        final boolean expected = op.equals("!=") || op.equals("<");
                        Assert.assertEquals(expected, first.getBool(numericRecord(ColumnType.CHAR, 0, 'a')));
                        Assert.assertEquals(expected, second.getBool(numericRecord(ColumnType.CHAR, 0, 'a')));
                    }
                }
                for (String type : new String[]{"byte", "short", "char", "date", "ipv4"}) {
                    final BoundExpression constant = binder.bind(binary("cast", 0,
                            constant("null", 5), constant(type, 13)), chars, "t", sqlExecutionContext);
                    Assert.assertEquals(ColumnType.typeOf(type), constant.getDataType());
                    try (Function first = binder.instantiate(constant, chars, sqlExecutionContext);
                         Function second = binder.instantiate(constant, chars, sqlExecutionContext)) {
                        Assert.assertEquals(first.getType(), second.getType());
                    }
                }
            }
        });
    }

    @Test
    public void testDeferredCallAdoptsBindTimeConstantOnceAndClosesItOnEveryPath() throws Exception {
        assertMemoryLeak(() -> {
            final int[] constructions = {0};
            final int[] closes = {0};
            final java.util.ArrayList<FunctionFactory> factories = new java.util.ArrayList<>();
            factories.add(new SubStringFunctionFactory());
            factories.add(new ConcatFunctionFactory());
            factories.add(new FunctionFactory() {
                @Override
                public String getSignature() {
                    return "counted_len()";
                }

                @Override
                public Function newInstance(int position, ObjList<Function> args, IntList argPositions,
                                            io.questdb.cairo.CairoConfiguration configuration, SqlExecutionContext context) {
                    constructions[0]++;
                    return new CountingIntConstant(2, closes);
                }
            });
            final FunctionParser parser = new FunctionParser(configuration, new FunctionFactoryCache(configuration, factories));
            final OutputSchema input = new OutputSchema().add(0, "s", ColumnType.STRING, true)
                    .add(1, "i", ColumnType.INT, true).add(2, "x", ColumnType.STRING, true);
            final OutputSchema withoutX = new OutputSchema().add(0, "s", ColumnType.STRING, true)
                    .add(1, "i", ColumnType.INT, true);
            try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                final BoundExpression substring = binder.bind(countedSubstring(), input, "t", sqlExecutionContext);
                Assert.assertTrue(substring instanceof FunctionExpression);
                Assert.assertEquals(1, constructions[0]);
                Assert.assertEquals(0, closes[0]);
                try (
                        Function first = binder.instantiate(substring, withoutX, sqlExecutionContext);
                        Function second = binder.instantiate(substring, withoutX, sqlExecutionContext)
                ) {
                    Assert.assertTrue(((TernaryFunction) first).getRight() instanceof CountingIntConstant);
                    Assert.assertFalse(((TernaryFunction) second).getRight() instanceof CountingIntConstant);
                    Assert.assertEquals(1, constructions[0]);
                    binder.clear();
                    Assert.assertEquals(0, closes[0]);
                    TestUtils.assertEquals("bc", first.getStrA(textAndIntRecord("abcdef", 2)));
                    TestUtils.assertEquals("bc", second.getStrA(textAndIntRecord("abcdef", 2)));
                }
                Assert.assertEquals(1, closes[0]);

                // A call that is never generated leaves its constant to the prepared functions.
                binder.bind(countedSubstring(), input, "t", sqlExecutionContext);
                binder.clear();
                Assert.assertEquals(2, constructions[0]);
                Assert.assertEquals(2, closes[0]);

                // Binding fails after the call handed its constant over.
                try {
                    final ExpressionNode negative = call("substring", 40, new ObjList<>(literal("s", 50), literal("i", 52), constant("-1", 54)));
                    binder.bind(call("concat", 0, new ObjList<>(negative, countedSubstring())), input, "t", sqlExecutionContext);
                    Assert.fail();
                } catch (SqlException e) {
                    Assert.assertEquals(40, e.getPosition());
                    TestUtils.assertContains(e.getFlyweightMessage(), "negative substring length is not allowed");
                }
                Assert.assertEquals(3, constructions[0]);
                Assert.assertEquals(3, closes[0]);
                binder.clear();
                Assert.assertEquals(3, closes[0]);

                // Generation fails after the call adopted its constant.
                final BoundExpression concat = binder.bind(call("concat", 0, new ObjList<>(countedSubstring(), literal("x", 30))),
                        input, "t", sqlExecutionContext);
                try {
                    binder.instantiate(concat, withoutX, sqlExecutionContext);
                    Assert.fail();
                } catch (IllegalStateException e) {
                    Assert.assertEquals("bound function input has changed", e.getMessage());
                }
                Assert.assertEquals(4, constructions[0]);
                Assert.assertEquals(4, closes[0]);
                binder.clear();
                Assert.assertEquals(4, closes[0]);
            }
        });
    }

    @Test
    public void testDeferredCallRaisesConstantArgumentErrorInConstructionOrder() throws Exception {
        assertMemoryLeak(() -> {
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
            final OutputSchema input = new OutputSchema().add(7, "ts", ColumnType.TIMESTAMP_MICRO, true);
            try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                final ExpressionNode dateAdd = call("dateadd", 10, new ObjList<>(constant("'x'", 18), constant("1", 23), literal("ts", 26)));
                try {
                    binder.bind(binary("=", 8, literal("missing", 0), dateAdd), input, "t", sqlExecutionContext);
                    Assert.fail();
                } catch (SqlException e) {
                    Assert.assertEquals(18, e.getPosition());
                    TestUtils.assertEquals("invalid time period [unit=x]", e.getFlyweightMessage());
                }
                try {
                    binder.bind(call("timestamp_floor", 0, new ObjList<>(constant("'0d'", 16), literal("ts", 22))), input, "t", sqlExecutionContext);
                    Assert.fail();
                } catch (SqlException e) {
                    Assert.assertEquals(16, e.getPosition());
                    TestUtils.assertEquals("invalid unit '0d'", e.getFlyweightMessage());
                }
            }
        });
    }

    @Test
    public void testFailingParentClosesConstructedChildOnceAndBinderRecovers() throws Exception {
        assertMemoryLeak(() -> {
            final int[] closes = {0};
            final int[] constructions = {0};
            final java.util.ArrayList<FunctionFactory> factories = new java.util.ArrayList<>();
            factories.add(new AddIntFunctionFactory());
            factories.add(new FunctionFactory() {
                @Override
                public String getSignature() {
                    return "failing(I)";
                }

                @Override
                public Function newInstance(int position, ObjList<Function> args, IntList argPositions,
                                            io.questdb.cairo.CairoConfiguration configuration, SqlExecutionContext context) throws SqlException {
                    throw SqlException.$(position, "parent rejected");
                }
            });
            final FunctionParser parser = new FunctionParser(configuration, new FunctionResolver(configuration, new FunctionFactoryCache(configuration, factories)) {
                @Override
                public Function createFunction(FunctionFactoryDescriptor overload, int position, CharSequence name,
                                               ObjList<Function> args, IntList positions, SqlExecutionContext context) throws SqlException {
                    final Function function = super.createFunction(overload, position, name, args, positions, context);
                    constructions[0]++;
                    return new CountingIntFunction(function, closes);
                }
            });
            final OutputSchema input = new OutputSchema().add(0, "i", ColumnType.INT, true);
            try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                final ExpressionNode parent = ExpressionNode.FACTORY.newInstance().of(ExpressionNode.FUNCTION, "failing", 0, 0);
                parent.paramCount = 1;
                parent.rhs = binary("+", 12, literal("i", 10), constant("1", 14));
                try {
                    binder.bind(parent, input, "t", sqlExecutionContext);
                    Assert.fail();
                } catch (SqlException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "parent rejected");
                }
                Assert.assertEquals(1, constructions[0]);
                Assert.assertEquals(1, closes[0]);
                binder.clear();
                Assert.assertEquals(1, closes[0]);
                final BoundExpression expression = binder.bind(binary("+", 2, literal("i", 0), constant("2", 4)),
                        input, "t", sqlExecutionContext);
                try (Function function = binder.instantiate(expression, input, sqlExecutionContext)) {
                    Assert.assertEquals(5, function.getInt(intRecord(3)));
                }
                Assert.assertEquals(2, constructions[0]);
                Assert.assertEquals(2, closes[0]);
            }
        });
    }

    @Test
    public void testFloatingConstantsAndNullFoldsSnapshotActualTypes() throws Exception {
        assertMemoryLeak(() -> {
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
            try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                final OutputSchema empty = new OutputSchema();
                final ConstantExpression sum = (ConstantExpression) binder.bind(
                        binary("+", 4, constant("1.25", 0), constant("2.25", 6)), empty, "t", sqlExecutionContext);
                Assert.assertEquals(ColumnType.DOUBLE, sum.getDataType());
                Assert.assertEquals(3.5, sum.getDoubleValue(), 0.0);
                try (Function function = binder.instantiate(sum, empty)) {
                    Assert.assertEquals(sum.getDoubleValue(), function.getDouble(null), 0.0);
                }
                for (int type : new int[]{ColumnType.LONG, ColumnType.FLOAT, ColumnType.DOUBLE}) {
                    final OutputSchema input = new OutputSchema().add(8, "discarded", type, true);
                    final ConstantExpression folded = (ConstantExpression) binder.bind(
                            binary("+", 10, literal("discarded", 0), constant("null", 12)), input, "t", sqlExecutionContext);
                    Assert.assertEquals(type, folded.getDataType());
                    try (Function function = binder.instantiate(folded, empty)) {
                        Assert.assertTrue(function.isNullConstant());
                        if (type == ColumnType.FLOAT) {
                            Assert.assertTrue(Float.isNaN(folded.getFloatValue()));
                        } else if (type == ColumnType.DOUBLE) {
                            Assert.assertTrue(Double.isNaN(folded.getDoubleValue()));
                        } else {
                            Assert.assertEquals(Long.MIN_VALUE, folded.getLongValue());
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testFoldClosesAndForgetsTheLeavesOfTheDroppedOperand() throws Exception {
        assertMemoryLeak(() -> {
            final ObjList<Function> folded = new ObjList<>();
            final java.util.ArrayList<FunctionFactory> factories = new java.util.ArrayList<>();
            factories.add(new OrFunctionFactory());
            factories.add(new LtLongFunctionFactory());
            factories.add(new FunctionFactory() {
                @Override
                public String getSignature() {
                    return "fold_false(L)";
                }

                @Override
                public Function newInstance(int position, ObjList<Function> args, IntList argPositions,
                                            io.questdb.cairo.CairoConfiguration configuration, SqlExecutionContext context) {
                    // Folds without closing its operand, as a fold that drops an operand may.
                    folded.add(args.getQuick(0));
                    return BooleanConstant.FALSE;
                }
            });
            final FunctionParser parser = new FunctionParser(configuration, new FunctionFactoryCache(configuration, factories));
            final OutputSchema input = new OutputSchema().add(1, "cnt", ColumnType.LONG, true)
                    .add(7, "v", ColumnType.LONG, true);
            final OutputSchema withoutCnt = new OutputSchema().add(7, "v", ColumnType.LONG, true);
            try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                final ExpressionNode dropped = ExpressionNode.FACTORY.newInstance().of(ExpressionNode.FUNCTION, "fold_false", 0, 0);
                dropped.paramCount = 1;
                dropped.rhs = literal("cnt", 11);
                final BoundExpression predicate = binder.bindPredicate(binary("or", 17, dropped,
                        binary("<", 22, literal("v", 20), constant("5", 24))), input, "t", sqlExecutionContext);
                Assert.assertTrue(predicate instanceof FunctionExpression call && "<".contentEquals(call.getName()));
                Assert.assertEquals(1, folded.size());
                Assert.assertTrue(folded.getQuick(0) instanceof BindableColumn);
                Assert.assertFalse(((BindableColumn) folded.getQuick(0)).isOpen());
                try (Function function = binder.instantiate(predicate, withoutCnt)) {
                    Assert.assertFalse(((BindableColumn) folded.getQuick(0)).isOpen());
                    binder.clear();
                    Assert.assertTrue(function.getBool(numericRecord(ColumnType.LONG, 0, 4)));
                    Assert.assertFalse(function.getBool(numericRecord(ColumnType.LONG, 0, 6)));
                }
            }
        });
    }

    @Test
    public void testGeneratedAliasPreservesRegistrationArgumentOrder() throws Exception {
        assertMemoryLeak(() -> {
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
            final OutputSchema input = new OutputSchema().add(7, "i", ColumnType.INT, true);
            try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                final FunctionExpression expression = (FunctionExpression) binder.bind(
                        binary("<=", 3, literal("i", 0), constant("4", 6)), input, "t", sqlExecutionContext);
                TestUtils.assertEquals("<=(II)", expression.getSignature());
                Assert.assertEquals(0, expression.getArgumentPosition(0));
                Assert.assertEquals(6, expression.getArgumentPosition(1));
                Assert.assertTrue(expression.argumentAt(0) instanceof ColumnExpression);
                try (Function function = binder.instantiate(expression, input, sqlExecutionContext)) {
                    Assert.assertTrue(function.getBool(intRecord(3)));
                    Assert.assertTrue(function.getBool(intRecord(4)));
                    Assert.assertFalse(function.getBool(intRecord(5)));
                }
            }
        });
    }

    @Test
    public void testGroupKeySubstitutionPreventsReassociationAcrossBoundary() throws Exception {
        assertMemoryLeak(() -> {
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
            final OutputSchema input = new OutputSchema().add(41, "group_key", ColumnType.BOOLEAN, true);
            final ExpressionNode key = binary("and", 5, literal("unavailable", 0), constant("true", 9));
            final ExpressionNode root = binary("and", 14, key, constant("false", 18));
            final ObjList<ExpressionNode> nodes = new ObjList<>();
            final ObjList<ColumnExpression> replacements = new ObjList<>();
            nodes.add(key);
            replacements.add(new ColumnExpression().of(41, ColumnType.BOOLEAN, 5));
            try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                final BoundExpression expression = binder.bind(root, input, null, nodes, replacements, sqlExecutionContext);
                Assert.assertTrue(expression instanceof ConstantExpression);
                try (Function function = binder.instantiate(expression, new OutputSchema(), sqlExecutionContext)) {
                    Assert.assertFalse(function.getBool(null));
                }
            }
        });
    }

    @Test
    public void testImplicitIPv4CastCapturesSelectedDescriptorAndIndependentBuffers() throws Exception {
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
            final OutputSchema input = new OutputSchema().add(1, "unused", ColumnType.INT, true)
                    .add(8, "ip", ColumnType.IPv4, true);
            final OutputSchema pruned = new OutputSchema().add(8, "ip", ColumnType.IPv4, true);
            try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                final FunctionExpression expression = (FunctionExpression) binder.bind(unary("length", 0, literal("ip", 7)),
                        input, "t", sqlExecutionContext);
                final FunctionExpression cast = (FunctionExpression) expression.argumentAt(0);
                TestUtils.assertEquals("cast(Xs)", cast.getSignature());
                Assert.assertEquals(ColumnType.IPv4, cast.argumentAt(0).getDataType());
                Assert.assertEquals(8, ((ColumnExpression) cast.argumentAt(0)).getColumnId());
                Assert.assertTrue(cast.argumentAt(1) instanceof TypeExpression);
                Assert.assertEquals(ColumnType.STRING, cast.argumentAt(1).getDataType());
                Assert.assertEquals(7, cast.getArgumentPosition(0));
                Assert.assertEquals(1, constructions[0]);
                try (Function first = binder.instantiate(expression, pruned, sqlExecutionContext);
                     Function second = binder.instantiate(expression, pruned, sqlExecutionContext);
                     Function text = binder.instantiate(cast, pruned, sqlExecutionContext)) {
                    Assert.assertEquals(4, constructions[0]);
                    binder.clear();
                    final Record ip = numericRecord(ColumnType.IPv4, 0, 0x01020304);
                    Assert.assertEquals(7, first.getInt(ip));
                    Assert.assertEquals(7, second.getInt(ip));
                    TestUtils.assertEquals("1.2.3.4", text.getStrA(ip));
                    TestUtils.assertEquals("5.6.7.8", text.getStrB(numericRecord(ColumnType.IPv4, 0, 0x05060708)));
                    Assert.assertEquals(-1, second.getInt(numericRecord(ColumnType.IPv4, 0, Numbers.IPv4_NULL)));
                    Assert.assertFalse(first.isThreadSafe());
                    Assert.assertFalse(second.isThreadSafe());
                }
            }
        });
    }

    @Test
    public void testInconvertibleConstantFoldReportsCallPosition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE k (l LONG)");
            execute("INSERT INTO k VALUES (1), (2), (3)");
            assertQuery("SELECT 'a' > 2").noLeakCheck().fails(11, "inconvertible value: a [CHAR -> INT]");
            assertQuery("SELECT 'a' > (2)").noLeakCheck().fails(11, "inconvertible value: a [CHAR -> INT]");
            assertQuery("SELECT 'a' + 2").noLeakCheck().fails(11, "inconvertible value: a [CHAR -> INT]");
            assertQuery("SELECT * FROM k WHERE 'a' > 2").noLeakCheck().fails(26, "inconvertible value: a [CHAR -> INT]");
            assertQuery("SELECT '1a' > 2").noLeakCheck().fails(12, "inconvertible value: `1a` [STRING -> INT]");
            assertQuery("SELECT NOT ('a' > 2)").noLeakCheck().fails(16, "inconvertible value: a [CHAR -> INT]");
            assertQuery("SELECT l FROM k WHERE '2' < l").noLeakCheck().returns("l\n3\n");
        });
    }

    @Test
    public void testLongNullComparisonKeepsSelectedSymbolOverload() throws Exception {
        assertMemoryLeak(() -> {
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
            final OutputSchema input = new OutputSchema().add(7, "l", ColumnType.LONG, true);
            try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                final FunctionExpression expression = (FunctionExpression) binder.bind(
                        binary("=", 2, literal("l", 0), constant("null", 4)), input, "t", sqlExecutionContext);
                TestUtils.assertEquals("=(LK)", expression.getSignature());
                try (Function function = binder.instantiate(expression, input)) {
                    Assert.assertTrue(function.getBool(new Record() {
                        @Override
                        public long getLong(int columnIndex) {
                            return Long.MIN_VALUE;
                        }
                    }));
                }
            }
        });
    }

    @Test
    public void testNullComparisonKeepsResolverSelectedConstantStringOverload() throws Exception {
        assertMemoryLeak(() -> {
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
            final OutputSchema input = new OutputSchema().add(7, "i", ColumnType.INT, true);
            try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                final FunctionExpression expression = (FunctionExpression) binder.bind(
                        binary("=", 3, literal("i", 0), constant("null", 5)), input, "t", sqlExecutionContext);
                TestUtils.assertEquals("=(Is)", expression.getSignature());
                try (Function function = binder.instantiate(expression, input, sqlExecutionContext)) {
                    Assert.assertTrue(function.getBool(intRecord(Integer.MIN_VALUE)));
                    Assert.assertFalse(function.getBool(intRecord(1)));
                }
            }
        });
    }

    @Test
    public void testNullFoldDropsClosedLeafDependency() throws Exception {
        assertMemoryLeak(() -> {
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
            final OutputSchema input = new OutputSchema().add(2, "discarded", ColumnType.INT, true)
                    .add(8, "kept", ColumnType.INT, true);
            try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                final FunctionExpression expression = (FunctionExpression) binder.bind(binary("=", 15,
                                binary("+", 10, literal("discarded", 0), constant("null", 12)), literal("kept", 17)),
                        input, "t", sqlExecutionContext);
                Assert.assertTrue(expression.argumentAt(0) instanceof ConstantExpression);
                Assert.assertEquals(ColumnType.INT, expression.argumentAt(0).getDataType());
                final OutputSchema pruned = new OutputSchema().add(8, "kept", ColumnType.INT, true);
                try (Function function = binder.instantiate(expression, pruned, sqlExecutionContext)) {
                    Assert.assertTrue(function.getBool(intRecord(Integer.MIN_VALUE)));
                    Assert.assertFalse(function.getBool(intRecord(1)));
                }
            }
        });
    }

    @Test
    public void testParameterInferenceAndRuntimeConstantReinitialization() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.clear();
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
            final OutputSchema input = new OutputSchema().add(7, "i", ColumnType.INT, true);
            try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                final FunctionExpression expression = (FunctionExpression) binder.bind(binary("=", 2,
                        literal("i", 0), binary("+", 7, parameter("$1", 4), constant("1", 9))), input, "t", sqlExecutionContext);
                final FunctionExpression runtimeConstant = (FunctionExpression) expression.argumentAt(1);
                final BindVariableExpression parameter = (BindVariableExpression) runtimeConstant.argumentAt(0);
                Assert.assertEquals(ColumnType.INT, parameter.getDataType());
                Assert.assertEquals(ColumnType.INT, bindVariableService.getFunction(0).getType());
                Assert.assertTrue((runtimeConstant.getFunctionFlags() & BoundExpression.RUNTIME_CONSTANT) != 0);
                try (Function function = binder.instantiate(expression, input)) {
                    binder.clear();
                    bindVariableService.setInt(0, 2);
                    function.init(null, sqlExecutionContext);
                    Assert.assertTrue(function.getBool(intRecord(3)));
                    bindVariableService.setInt(0, 8);
                    Assert.assertTrue(function.getBool(intRecord(3)));
                    function.init(null, sqlExecutionContext);
                    Assert.assertFalse(function.getBool(intRecord(3)));
                    Assert.assertTrue(function.getBool(intRecord(9)));
                }
            }
        });
    }

    @Test
    public void testPrimitiveAggregateFamiliesRelocateAndRebuildIndependentState() throws Exception {
        assertMemoryLeak(() -> {
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
            final ObjList<String> names = new ObjList<>();
            names.add("sum");
            names.add("min");
            names.add("max");
            names.add("avg");
            names.add("count");
            for (int n = 0; n < names.size(); n++) {
                final String name = names.getQuick(n);
                for (int type : new int[]{ColumnType.INT, ColumnType.LONG, ColumnType.FLOAT, ColumnType.DOUBLE}) {
                    final OutputSchema original = new OutputSchema().add(10, "unused", ColumnType.INT, true)
                            .add(27, "v", type, true);
                    final OutputSchema pruned = new OutputSchema().add(27, "v", type, true);
                    final GenericRecordMetadata originalMetadata = new GenericRecordMetadata();
                    originalMetadata.add(new TableColumnMetadata("unused", ColumnType.INT));
                    originalMetadata.add(new TableColumnMetadata("v", type));
                    final GenericRecordMetadata prunedMetadata = new GenericRecordMetadata();
                    prunedMetadata.add(new TableColumnMetadata("v", type));
                    try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                        final FunctionExpression expression = binder.bindAggregate(
                                unary(name, 7, literal("v", 11)), original, "t", sqlExecutionContext);
                        Assert.assertTrue(expression.isAggregate());
                        Assert.assertFalse(expression.getOverload().isRelocatableScalar());
                        Assert.assertEquals(1, expression.getArgumentCount());
                        Assert.assertEquals(type, expression.argumentAt(0).getDataType());
                        Assert.assertEquals(27, ((ColumnExpression) expression.argumentAt(0)).getColumnId());
                        Assert.assertEquals(11, expression.getArgumentPosition(0));
                        try (
                                Function first = binder.instantiateAggregate(expression, pruned, prunedMetadata, sqlExecutionContext);
                                Function second = binder.instantiateAggregate(expression, original, originalMetadata, sqlExecutionContext);
                                SimpleMapValue values = new SimpleMapValue(5)
                        ) {
                            Assert.assertNotSame(first, second);
                            final GroupByFunction firstAggregate = (GroupByFunction) first;
                            final GroupByFunction secondAggregate = (GroupByFunction) second;
                            firstAggregate.initValueIndex(0);
                            secondAggregate.initValueIndex(3);
                            binder.clear();
                            firstAggregate.computeFirst(values, numericRecord(type, 0, Double.NaN), 0);
                            firstAggregate.computeNext(values, numericRecord(type, 0, 2), 1);
                            firstAggregate.computeNext(values, numericRecord(type, 0, 6), 2);
                            secondAggregate.computeFirst(values, numericRecord(type, 1, 6), 0);
                            secondAggregate.computeNext(values, numericRecord(type, 1, Double.NaN), 1);
                            secondAggregate.computeNext(values, numericRecord(type, 1, 2), 2);
                            final double expected = switch (name) {
                                case "sum" -> 8;
                                case "max" -> 6;
                                case "avg" -> 4;
                                default -> 2;
                            };
                            Assert.assertEquals(name, expected, first.getDouble(values), 0);
                            Assert.assertEquals(name, expected, second.getDouble(values), 0);
                            firstAggregate.setEmpty(values);
                            if ("count".equals(name)) {
                                Assert.assertEquals(0, first.getLong(values));
                            } else {
                                Assert.assertTrue(name, Double.isNaN(first.getDouble(values)));
                            }
                            Assert.assertEquals(name, expected, second.getDouble(values), 0);
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testProjectionRemapPreservesDirectReferenceEligibility() throws Exception {
        assertMemoryLeak(() -> {
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
            final ScanPlan scan = new ScanPlan();
            scan.getOutput().add(7, "value", ColumnType.INT, true);
            final ProjectPlan projection = new ProjectPlan().of(scan, 0);
            projection.getOutput().add(20, "alias", ColumnType.INT, true);
            try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                final ColumnExpression castInput = (ColumnExpression) binder.bind(
                        binary("cast", 0, literal("value", 5), constant("int", 14)), scan.getOutput(), null, sqlExecutionContext);
                projection.getExpressions().add(castInput);
                final ColumnExpression direct = (ColumnExpression) binder.bind(literal("alias", 3), projection.getOutput(), null, sqlExecutionContext);
                Assert.assertTrue(direct.isDirectReference());
                final ColumnExpression mapped = (ColumnExpression) binder.remapColumns(direct, projection);
                Assert.assertFalse(mapped.isDirectReference());
                Assert.assertTrue(direct.isDirectReference());
                Assert.assertEquals(7, mapped.getColumnId());
                try (Function function = binder.instantiate(mapped, scan.getOutput())) {
                    Assert.assertEquals(12, function.getInt(intRecord(12)));
                }

                projection.getExpressions().setQuick(0, new ColumnExpression().of(7, ColumnType.INT, 0));
                final ColumnExpression castOutput = (ColumnExpression) binder.bind(
                        binary("cast", 0, literal("alias", 5), constant("int", 14)), projection.getOutput(), null, sqlExecutionContext);
                final ColumnExpression mappedCast = (ColumnExpression) binder.remapColumns(castOutput, projection);
                Assert.assertFalse(mappedCast.isDirectReference());
                try (Function function = binder.instantiate(mappedCast, scan.getOutput())) {
                    Assert.assertEquals(13, function.getInt(intRecord(13)));
                }
                binder.clear();
                final ColumnExpression reused = (ColumnExpression) binder.bind(literal("value", 0), scan.getOutput(), null, sqlExecutionContext);
                Assert.assertTrue(reused.isDirectReference());
            }
        });
    }

    @Test
    public void testProjectionRemapRetainsOneConstructionAndImmutableDescription() throws Exception {
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
            final ScanPlan scan = new ScanPlan();
            scan.getOutput().add(7, "value", ColumnType.INT, true);
            final ProjectPlan projection = new ProjectPlan().of(scan, 0);
            projection.getOutput().add(20, "alias", ColumnType.INT, true);
            projection.getExpressions().add(new ColumnExpression().of(7, ColumnType.INT, 5));
            try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                final FunctionExpression original = (FunctionExpression) binder.bind(binary("<=", 6,
                                binary("&", 3, literal("alias", 0), constant("7", 5)), constant("3", 9)),
                        projection.getOutput(), "t", sqlExecutionContext);
                Assert.assertEquals(2, constructions[0]);
                final FunctionExpression replacement = (FunctionExpression) binder.remapColumns(original, projection);
                Assert.assertNotSame(original, replacement);
                Assert.assertEquals(20, ((ColumnExpression) ((FunctionExpression) original.argumentAt(0)).argumentAt(0)).getColumnId());
                Assert.assertEquals(7, ((ColumnExpression) ((FunctionExpression) replacement.argumentAt(0)).argumentAt(0)).getColumnId());
                try (Function function = binder.instantiate(replacement, scan.getOutput(), sqlExecutionContext)) {
                    Assert.assertEquals(2, constructions[0]);
                    binder.clear();
                    Assert.assertTrue(function.getBool(intRecord(3)));
                    Assert.assertFalse(function.getBool(intRecord(4)));
                }
            }
        });
    }

    @Test
    public void testQualifiedColumnsRetainIdsThroughLayoutsAndCompilerReuse() throws Exception {
        assertMemoryLeak(() -> {
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
            final OutputSchema input = new OutputSchema()
                    .add(1, "unused", ColumnType.INT, null, true, "l")
                    .add(7, "id", ColumnType.INT, null, true, "l")
                    .add(12, "id", ColumnType.INT, null, true, "r");
            final OutputSchema reordered = new OutputSchema()
                    .add(12, "right_id", ColumnType.INT, true)
                    .add(7, "left_id", ColumnType.INT, true);
            try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                final FunctionExpression expression = (FunctionExpression) binder.bind(
                        binary("<", 5, literal("L.ID", 0), literal("r.id", 7)), input, "ignored", sqlExecutionContext);
                Assert.assertEquals(7, ((ColumnExpression) expression.argumentAt(0)).getColumnId());
                Assert.assertEquals(12, ((ColumnExpression) expression.argumentAt(1)).getColumnId());
                try (Function retained = binder.instantiate(expression, reordered, sqlExecutionContext);
                     Function rebuilt = binder.instantiate(expression, input, sqlExecutionContext)) {
                    binder.clear();
                    parser.clear();
                    input.clear();
                    reordered.clear();
                    Assert.assertTrue(retained.getBool(new Record() {
                        @Override
                        public int getInt(int columnIndex) {
                            Assert.assertTrue(columnIndex == 0 || columnIndex == 1);
                            return columnIndex == 0 ? 10 : 3;
                        }
                    }));
                    Assert.assertTrue(rebuilt.getBool(new Record() {
                        @Override
                        public int getInt(int columnIndex) {
                            Assert.assertTrue(columnIndex == 1 || columnIndex == 2);
                            return columnIndex == 1 ? 3 : 10;
                        }
                    }));
                }
            }
        });
    }

    @Test
    public void testQualifiedProtectedNamesAndSingleSourceFallback() throws Exception {
        assertMemoryLeak(() -> {
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
            final OutputSchema input = new OutputSchema()
                    .add(7, "a.b", ColumnType.INT, null, true, "source.dot")
                    .add(12, "in", ColumnType.INT, null, true, "source.dot")
                    .add(13, "\"a,b\"", ColumnType.INT, null, true, "source.dot");
            try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                Assert.assertEquals(7, ((ColumnExpression) binder.bind(literal("\"SOURCE.DOT\".\"A.B\"", 9), input, null, sqlExecutionContext)).getColumnId());
                Assert.assertEquals(7, ((ColumnExpression) binder.bind(literal("\"a.b\"", 9), input, null, sqlExecutionContext)).getColumnId());
                Assert.assertEquals(12, ((ColumnExpression) binder.bind(literal("\"source.dot\".\"in\"", 9), input, null, sqlExecutionContext)).getColumnId());
                Assert.assertEquals(13, ((ColumnExpression) binder.bind(literal("\"source.dot\".\"a,b\"", 9), input, null, sqlExecutionContext)).getColumnId());
                input.clear();
                // A derived projection exposes its immediate alias, not an inner source qualifier.
                input.add(22, "a.b", ColumnType.INT, true);
                Assert.assertEquals(22, ((ColumnExpression) binder.bind(literal("outer.\"a.b\"", 9), input, "outer", sqlExecutionContext)).getColumnId());
                assertBindingError(binder, input, "inner.\"a.b\"", "outer", "Invalid table name or alias");
                assertBindingError(binder, input, "outer.\"a.b\"", null, "Invalid table name or alias");
            }
        });
    }

    @Test
    public void testRebuiltParentAdoptsPreparedChildrenOnceAndClosesFailedPartialGraph() throws Exception {
        assertMemoryLeak(() -> {
            final ObjList<Function> constructed = new ObjList<>();
            final FunctionParser parser = new FunctionParser(configuration, new FunctionResolver(configuration, engine.getFunctionFactoryCache()) {
                @Override
                public Function createFunction(FunctionFactoryDescriptor overload, int position, CharSequence name,
                                               ObjList<Function> args, IntList positions, SqlExecutionContext context) throws SqlException {
                    final Function function = super.createFunction(overload, position, name, args, positions, context);
                    constructed.add(function);
                    return function;
                }
            });
            final OutputSchema input = new OutputSchema().add(1, "unused", ColumnType.LONG, true)
                    .add(7, "v", ColumnType.LONG, true);
            final OutputSchema pruned = new OutputSchema().add(7, "v", ColumnType.LONG, true);
            final ObjList<ExpressionNode> inArguments = new ObjList<>(literal("v", 0), constant("1", 5), constant("2", 7), parameter("$1", 9));
            bindVariableService.clear();
            bindVariableService.setLong(0, 1);
            try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                final BoundExpression left = binder.bind(call("in", 2, inArguments), input, null, sqlExecutionContext);
                final BoundExpression right = binder.bind(call("in", 15, new ObjList<>(literal("v", 13), constant("1", 18), constant("2", 20), constant("3", 22), parameter("$1", 24))),
                        input, null, sqlExecutionContext);
                final FunctionExpression combined = andDescription(parser, left, right);
                final ScanPlan scan = new ScanPlan();
                scan.getOutput().add(27, "v", ColumnType.LONG, true);
                final ProjectPlan projection = new ProjectPlan().of(scan, 0);
                projection.getOutput().add(7, "v", ColumnType.LONG, true);
                projection.getExpressions().add(new ColumnExpression().of(27, ColumnType.LONG, 0));
                final BoundExpression remapped = binder.remapColumns(combined, projection);
                Assert.assertEquals(7, ((ColumnExpression) ((FunctionExpression) combined.argumentAt(0)).argumentAt(0)).getColumnId());
                Assert.assertEquals(2, constructed.size());
                try (Function first = binder.instantiate(remapped, scan.getOutput(), sqlExecutionContext)) {
                    Assert.assertEquals(3, constructed.size());
                    Assert.assertSame(constructed.getQuick(0), ((BinaryFunction) first).getLeft());
                    Assert.assertSame(constructed.getQuick(1), ((BinaryFunction) first).getRight());
                    try (Function second = binder.instantiate(combined, input, sqlExecutionContext)) {
                        Assert.assertEquals(6, constructed.size());
                        binder.clear();
                        first.init(null, sqlExecutionContext);
                        second.init(null, sqlExecutionContext);
                        Assert.assertTrue(first.getBool(numericRecord(ColumnType.LONG, 0, 1)));
                        Assert.assertTrue(second.getBool(numericRecord(ColumnType.LONG, 1, 2)));
                        Assert.assertFalse(first.getBool(numericRecord(ColumnType.LONG, 0, 3)));
                    }
                }
                // The first native IN root transfers to the new parent before a
                // later missing-column error. That frame must close it exactly once.
                final BoundExpression nativeLeft = binder.bind(call("in", 2, inArguments), input, null, sqlExecutionContext);
                final BoundExpression missingRight = binder.bind(binary(">", 15,
                        literal("unused", 13), constant("0", 20)), input, null, sqlExecutionContext);
                try {
                    binder.instantiate(andDescription(parser, nativeLeft, missingRight), pruned, sqlExecutionContext);
                    Assert.fail("missing bound column accepted");
                } catch (IllegalStateException e) {
                    Assert.assertEquals("bound function input has changed", e.getMessage());
                }
                // Reconstructing the consumed description is independent of both
                // the closed native root and the still-owned right preparation.
                try (Function recovered = binder.instantiate(nativeLeft, pruned, sqlExecutionContext)) {
                    binder.clear();
                    recovered.init(null, sqlExecutionContext);
                    Assert.assertTrue(recovered.getBool(numericRecord(ColumnType.LONG, 0, 2)));
                }
            }
        });
    }

    @Test
    public void testRebuiltRuntimeConstantHasIndependentInitialization() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.clear();
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
            final OutputSchema input = new OutputSchema().add(7, "i", ColumnType.INT, true);
            try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                final BoundExpression expression = binder.bind(binary("=", 2, literal("i", 0),
                                binary("cast", 4, binary("+", 8, parameter("$1", 6), constant("1", 10)), constant("long", 15))),
                        input, "t", sqlExecutionContext);
                try (
                        Function first = binder.instantiate(expression, input, sqlExecutionContext);
                        Function second = binder.instantiate(expression, input, sqlExecutionContext)
                ) {
                    binder.clear();
                    bindVariableService.setInt(0, 2);
                    first.init(null, sqlExecutionContext);
                    bindVariableService.setInt(0, 8);
                    second.init(null, sqlExecutionContext);
                    Assert.assertTrue(first.getBool(intRecord(3)));
                    Assert.assertTrue(second.getBool(intRecord(9)));
                    Assert.assertFalse(second.getBool(intRecord(3)));
                }
            }
        });
    }

    @Test
    public void testReconstructionFailureClosesOnlyNewPartialGraph() throws Exception {
        assertMemoryLeak(() -> {
            final int[] closes = {0};
            final FunctionParser parser = new FunctionParser(configuration, new FunctionResolver(configuration, engine.getFunctionFactoryCache()) {
                @Override
                public Function createFunction(FunctionFactoryDescriptor overload, int position, CharSequence name,
                                               ObjList<Function> args, IntList positions, SqlExecutionContext context) throws SqlException {
                    final Function function = super.createFunction(overload, position, name, args, positions, context);
                    return overload.getFactory().getClass() == AddIntFunctionFactory.class
                            ? new CountingIntFunction(function, closes) : function;
                }
            });
            final OutputSchema input = new OutputSchema().add(7, "i", ColumnType.INT, true)
                    .add(8, "j", ColumnType.INT, true);
            final OutputSchema missing = new OutputSchema().add(7, "i", ColumnType.INT, true);
            try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                final BoundExpression expression = binder.bind(binary("<", 4,
                        binary("+", 2, literal("i", 0), constant("1", 3)), literal("j", 6)), input, "t", sqlExecutionContext);
                try (Function first = binder.instantiate(expression, input, sqlExecutionContext)) {
                    try (Function ignored = binder.instantiate(expression, missing, sqlExecutionContext)) {
                        Assert.fail("missing input must fail reconstruction");
                    } catch (IllegalStateException e) {
                        Assert.assertEquals("bound function input has changed", e.getMessage());
                    }
                    Assert.assertEquals(1, closes[0]);
                    Assert.assertTrue(first.getBool(new Record() {
                        @Override
                        public int getInt(int columnIndex) {
                            return columnIndex == 0 ? 2 : 5;
                        }
                    }));
                    try (Function rebuilt = binder.instantiate(expression, input, sqlExecutionContext)) {
                        Assert.assertTrue(rebuilt.getBool(new Record() {
                            @Override
                            public int getInt(int columnIndex) {
                                return columnIndex == 0 ? 2 : 5;
                            }
                        }));
                    }
                    Assert.assertEquals(2, closes[0]);
                }
                Assert.assertEquals(3, closes[0]);
            }
        });
    }

    @Test
    public void testRemainingPrimitiveLeavesAndAbsReconstruction() throws Exception {
        assertMemoryLeak(() -> {
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
            for (int type : new int[]{ColumnType.BYTE, ColumnType.SHORT, ColumnType.INT, ColumnType.LONG, ColumnType.FLOAT, ColumnType.DOUBLE}) {
                final OutputSchema input = new OutputSchema().add(1, "unused", ColumnType.INT, true)
                        .add(9, "value", type, true);
                final OutputSchema pruned = new OutputSchema().add(9, "value", type, true);
                try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                    final BoundExpression expression = binder.bind(unary("abs", 0, literal("value", 4)), input, "t", sqlExecutionContext);
                    try (Function first = binder.instantiate(expression, pruned, sqlExecutionContext);
                         Function second = binder.instantiate(expression, pruned, sqlExecutionContext)) {
                        binder.clear();
                        Assert.assertEquals(7, first.getDouble(numericRecord(type, 0, -7)), 0);
                        Assert.assertEquals(7, second.getDouble(numericRecord(type, 0, -7)), 0);
                    }
                    if (type == ColumnType.BYTE || type == ColumnType.SHORT) {
                        final BoundExpression negative = binder.bind(unary("-", 0, literal("value", 1)), input, "t", sqlExecutionContext);
                        try (Function first = binder.instantiate(negative, pruned, sqlExecutionContext);
                             Function second = binder.instantiate(negative, pruned, sqlExecutionContext)) {
                            Assert.assertEquals(3, first.getDouble(numericRecord(type, 0, -3)), 0);
                            Assert.assertEquals(3, second.getDouble(numericRecord(type, 0, -3)), 0);
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testRootParameterDefaultsAndCastInference() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.clear();
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
            final OutputSchema empty = new OutputSchema();
            try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                final BoundExpression projection = binder.bind(parameter("$1", 0), empty, "t", ColumnType.STRING, sqlExecutionContext);
                Assert.assertEquals(ColumnType.STRING, projection.getDataType());
                Assert.assertTrue(((BindVariableExpression) projection).isDirectReference());
                try (Function function = binder.instantiate(projection, empty)) {
                    bindVariableService.setStr(0, "first");
                    function.init(null, sqlExecutionContext);
                    TestUtils.assertEquals("first", function.getStrA(null));
                }
                final BoundExpression cast = binder.bind(binary("cast", 0,
                        parameter("$2", 5), constant("double", 11)), empty, "t", sqlExecutionContext);
                Assert.assertTrue(cast instanceof BindVariableExpression);
                Assert.assertFalse(((BindVariableExpression) cast).isDirectReference());
                Assert.assertEquals(ColumnType.DOUBLE, cast.getDataType());
                Assert.assertEquals(ColumnType.DOUBLE, bindVariableService.getFunction(1).getType());
                try (Function function = binder.instantiate(cast, empty)) {
                    bindVariableService.setDouble(1, 2.5);
                    function.init(null, sqlExecutionContext);
                    Assert.assertEquals(2.5, function.getDouble(null), 0.0);
                }
            }
        });
    }

    @Test
    public void testSharedParserExpressionsRemainUnchangedAcrossBindingsAndFailures() throws Exception {
        assertMemoryLeak(() -> {
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
            final ExpressionNode column = literal("i", 5);
            final ExpressionNode one = constant("1", 9);
            final ExpressionNode declared = binary("+", 7, column, one);
            final ExpressionNode first = binary("+", 12, declared, constant("2", 14));
            final ExpressionNode second = binary("+", 20, declared, constant("3", 22));
            final OutputSchema ints = new OutputSchema().add(7, "i", ColumnType.INT, true);
            final OutputSchema longs = new OutputSchema().add(12, "i", ColumnType.LONG, true);
            try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                try {
                    binder.bind(first, new OutputSchema(), null, sqlExecutionContext);
                    Assert.fail();
                } catch (SqlException e) {
                    Assert.assertEquals(5, e.getPosition());
                    TestUtils.assertEquals("Invalid column: i", e.getFlyweightMessage());
                }
                Assert.assertSame(declared, first.lhs);
                Assert.assertSame(column, declared.lhs);
                Assert.assertSame(one, declared.rhs);
                Assert.assertFalse(one.isConstantExpression);
                final BoundExpression intExpression = binder.bind(first, ints, null, sqlExecutionContext);
                final BoundExpression longExpression = binder.bind(second, longs, null, sqlExecutionContext);
                Assert.assertEquals(ColumnType.INT, intExpression.getDataType());
                Assert.assertEquals(ColumnType.LONG, longExpression.getDataType());
                Assert.assertSame(declared, first.lhs);
                Assert.assertSame(declared, second.lhs);
                Assert.assertSame(column, declared.lhs);
                Assert.assertSame(one, declared.rhs);
                Assert.assertFalse(one.isConstantExpression);
                try (Function intFunction = binder.instantiate(intExpression, ints, sqlExecutionContext);
                     Function longFunction = binder.instantiate(longExpression, longs, sqlExecutionContext)) {
                    binder.clear();
                    parser.clear();
                    Assert.assertEquals(6, intFunction.getInt(intRecord(3)));
                    Assert.assertEquals(8, longFunction.getLong(numericRecord(ColumnType.LONG, 0, 4)));
                }
            }
        });
    }

    @Test
    public void testStringReconstructionUsesDecodedValueAndIndependentRecordBuffers() throws Exception {
        assertMemoryLeak(() -> {
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
            final OutputSchema input = new OutputSchema().add(7, "a", ColumnType.STRING, true)
                    .add(8, "b", ColumnType.STRING, true);
            try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                final BoundExpression decoded = binder.bind(constant("'''quoted'''", 0), input, "t", sqlExecutionContext);
                final BoundExpression comparison = binder.bind(binary("=", 2, literal("a", 0), literal("b", 4)),
                        input, "t", sqlExecutionContext);
                final BoundExpression literalComparison = binder.bind(binary("=", 2, literal("a", 0), constant("'''quoted'''", 4)),
                        input, "t", sqlExecutionContext);
                try (
                        Function decodedFirst = binder.instantiate(decoded, input, sqlExecutionContext);
                        Function decodedSecond = binder.instantiate(decoded, input, sqlExecutionContext);
                        Function first = binder.instantiate(comparison, input, sqlExecutionContext);
                        Function second = binder.instantiate(comparison, input, sqlExecutionContext);
                        Function literalFirst = binder.instantiate(literalComparison, input, sqlExecutionContext);
                        Function literalSecond = binder.instantiate(literalComparison, input, sqlExecutionContext)
                ) {
                    binder.clear();
                    TestUtils.assertEquals("'quoted'", decodedFirst.getStrA(null));
                    TestUtils.assertEquals("'quoted'", decodedSecond.getStrB(null));
                    final Record record = new Record() {
                        @Override
                        public CharSequence getStrA(int columnIndex) {
                            Assert.assertEquals(0, columnIndex);
                            return "'quoted'";
                        }

                        @Override
                        public CharSequence getStrB(int columnIndex) {
                            Assert.assertEquals(1, columnIndex);
                            return "'quoted'";
                        }
                    };
                    Assert.assertTrue(first.getBool(record));
                    Assert.assertTrue(second.getBool(record));
                    Assert.assertTrue(literalFirst.getBool(record));
                    Assert.assertTrue(literalSecond.getBool(record));
                    Assert.assertFalse(first.isThreadSafe());
                    Assert.assertFalse(second.isThreadSafe());
                }
            }
        });
    }

    @Test
    public void testStringTransformConstantsPreserveDecodedValuesAndNulls() throws Exception {
        assertMemoryLeak(() -> {
            final ObjList<String> names = new ObjList<>("lower", "upper", "to_lowercase", "to_uppercase", "trim", "ltrim", "rtrim");
            final ObjList<String> expected = new ObjList<>("  'abc'  ", "  'ABC'  ", "  'abc'  ", "  'ABC'  ", "'AbC'", "'AbC'  ", "  'AbC'");
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
            for (int i = 0; i < names.size(); i++) {
                try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                    final OutputSchema input = new OutputSchema();
                    final BoundExpression expression = binder.bind(unary(names.getQuick(i), 0, constant("'  ''AbC''  '", 6)),
                            input, null, sqlExecutionContext);
                    Assert.assertTrue(expression instanceof ConstantExpression);
                    TestUtils.assertEquals(expected.getQuick(i), ((ConstantExpression) expression).getStrValue());
                    try (Function function = binder.instantiate(expression, input)) {
                        TestUtils.assertEquals(expected.getQuick(i), function.getStrA(null));
                    }
                    final BoundExpression nullExpression = binder.bind(unary(names.getQuick(i), 0,
                            binary("cast", 6, constant("null", 11), constant("string", 19))), input, null, sqlExecutionContext);
                    Assert.assertEquals(ColumnType.STRING, nullExpression.getDataType());
                    try (Function function = binder.instantiate(nullExpression, input)) {
                        Assert.assertNull(function.getStrA(null));
                    }
                }
            }
        });
    }

    @Test
    public void testStringTransformRuntimeParametersReinitialiseIndependently() throws Exception {
        assertMemoryLeak(() -> {
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
            final OutputSchema input = new OutputSchema();
            bindVariableService.setStr(0, "  MiXeD  ");
            try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                final BoundExpression expression = binder.bind(unary("lower", 0, unary("trim", 6, parameter("$1", 11))),
                        input, null, sqlExecutionContext);
                Assert.assertTrue((expression.getFunctionFlags() & BoundExpression.RUNTIME_CONSTANT) != 0);
                try (Function retained = binder.instantiate(expression, input);
                     Function rebuilt = binder.instantiate(expression, input, sqlExecutionContext)) {
                    retained.init(null, sqlExecutionContext);
                    rebuilt.init(null, sqlExecutionContext);
                    final CharSequence retainedValue = retained.getStrA(null);
                    TestUtils.assertEquals("mixed", retainedValue);
                    TestUtils.assertEquals("mixed", rebuilt.getStrA(null));
                    bindVariableService.setStr(0, "  NEXT  ");
                    rebuilt.init(null, sqlExecutionContext);
                    TestUtils.assertEquals("next", rebuilt.getStrB(null));
                    TestUtils.assertEquals("mixed", retainedValue);
                    retained.init(null, sqlExecutionContext);
                    TestUtils.assertEquals("next", retained.getStrA(null));
                    bindVariableService.setStr(0, null);
                    retained.init(null, sqlExecutionContext);
                    rebuilt.init(null, sqlExecutionContext);
                    Assert.assertNull(retained.getStrA(null));
                    Assert.assertNull(rebuilt.getStrB(null));
                }
            }
        });
    }

    @Test
    public void testStringTransformsBindUnconstructedAndBuildIndependentBuffers() throws Exception {
        assertMemoryLeak(() -> {
            final ObjList<String> names = new ObjList<>("lower", "upper", "to_lowercase", "to_uppercase", "trim", "ltrim", "rtrim");
            final ObjList<String> expected = new ObjList<>("  mixed  ", "  MIXED  ", "  mixed  ", "  MIXED  ", "MiXeD", "MiXeD  ", "  MiXeD");
            final int[] constructions = {0};
            final FunctionParser parser = new FunctionParser(configuration, new FunctionResolver(configuration, engine.getFunctionFactoryCache()) {
                @Override
                public Function createFunction(FunctionFactoryDescriptor overload, int position, CharSequence name,
                                               ObjList<Function> args, IntList positions, SqlExecutionContext context) throws SqlException {
                    constructions[0]++;
                    return super.createFunction(overload, position, name, args, positions, context);
                }
            });
            final OutputSchema input = new OutputSchema().add(1, "unused", ColumnType.LONG, true)
                    .add(7, "label", ColumnType.STRING, true);
            final OutputSchema pruned = new OutputSchema().add(7, "label", ColumnType.STRING, true);
            for (int i = 0; i < names.size(); i++) {
                try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                    constructions[0] = 0;
                    final FunctionExpression expression = (FunctionExpression) binder.bind(
                            unary(names.getQuick(i), 0, literal("label", 9)), input, null, sqlExecutionContext);
                    TestUtils.assertEquals(names.getQuick(i), expression.getName());
                    Assert.assertEquals(0, constructions[0]);
                    try (Function retained = binder.instantiate(expression, pruned, sqlExecutionContext)) {
                        Assert.assertEquals(1, constructions[0]);
                        try (Function rebuilt = binder.instantiate(expression, input, sqlExecutionContext)) {
                            Assert.assertEquals(2, constructions[0]);
                            Assert.assertFalse(retained.isThreadSafe());
                            Assert.assertFalse(rebuilt.isThreadSafe());
                            final CharSequence first = retained.getStrA(stringRecord(0, "  MiXeD  "));
                            final CharSequence second = retained.getStrB(stringRecord(0, "  MiXeD  "));
                            TestUtils.assertEquals(expected.getQuick(i), first);
                            TestUtils.assertEquals(expected.getQuick(i), second);
                            Assert.assertNotSame(first, second);
                            rebuilt.getStrA(stringRecord(1, "different"));
                            TestUtils.assertEquals(expected.getQuick(i), first);
                            TestUtils.assertEquals(expected.getQuick(i), second);
                            Assert.assertEquals(expected.getQuick(i).length(), retained.getStrLen(stringRecord(0, "  MiXeD  ")));
                            binder.clear();
                            parser.clear();
                            Assert.assertNull(retained.getStrA(stringRecord(0, null)));
                            Assert.assertNull(rebuilt.getStrB(stringRecord(1, null)));
                            Assert.assertEquals(-1, rebuilt.getStrLen(stringRecord(1, null)));
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testSwitchClosesDiscardedNativeBranchesDuringBindingAndReconstruction() throws Exception {
        assertMemoryLeak(() -> {
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
            final OutputSchema input = new OutputSchema().add(7, "v", ColumnType.LONG, true)
                    .add(8, "flag", ColumnType.BOOLEAN, true).add(9, "label", ColumnType.STRING, true);
            final ObjList<ExpressionNode> in = new ObjList<>();
            in.add(literal("v", 80));
            in.add(constant("1", 85));
            in.add(constant("2", 88));
            in.add(constant("3", 91));
            final ObjList<ExpressionNode> arguments = new ObjList<>();
            arguments.add(literal("flag", 5));
            arguments.add(constant("true", 15));
            arguments.add(constant("true", 25));
            arguments.add(constant("false", 35));
            arguments.add(constant("false", 45));
            arguments.add(call("in", 82, in));
            try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                BoundExpression expression = binder.bind(call("switch", 0, arguments), input, "t", sqlExecutionContext);
                final Record record = new Record() {
                    @Override
                    public boolean getBool(int index) {
                        Assert.assertEquals(1, index);
                        return true;
                    }

                    @Override
                    public CharSequence getStrA(int index) {
                        Assert.assertEquals(2, index);
                        return null;
                    }
                };
                try (Function first = binder.instantiate(expression, input, sqlExecutionContext);
                     Function worker = binder.instantiate(expression, input, sqlExecutionContext)) {
                    Assert.assertTrue(first.getBool(record));
                    Assert.assertTrue(worker.getBool(record));
                }
                binder.clear();
                arguments.clear();
                arguments.add(literal("label", 5));
                arguments.add(constant("null", 15));
                arguments.add(call("in", 82, in));
                arguments.add(constant("null", 35));
                arguments.add(constant("true", 45));
                expression = binder.bind(call("switch", 0, arguments), input, "t", sqlExecutionContext);
                try (Function first = binder.instantiate(expression, input, sqlExecutionContext);
                     Function worker = binder.instantiate(expression, input, sqlExecutionContext)) {
                    binder.clear();
                    Assert.assertTrue(first.getBool(record));
                    Assert.assertTrue(worker.getBool(record));
                }
            }
        });
    }

    @Test
    public void testSwitchRetainsKeyAndBranchesAndLastConstantNullBranch() throws Exception {
        assertMemoryLeak(() -> {
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
            final OutputSchema input = new OutputSchema().add(7, "v", ColumnType.LONG, true);
            final ObjList<ExpressionNode> arguments = new ObjList<>();
            arguments.add(literal("v", 5));
            arguments.add(constant("1", 12));
            arguments.add(constant("10", 19));
            arguments.add(constant("2", 27));
            arguments.add(binary("+", 35, literal("v", 33), constant("1", 37)));
            arguments.add(constant("null", 45));
            try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                final FunctionExpression expression = (FunctionExpression) binder.bind(call("switch", 0, arguments), input, "t", sqlExecutionContext);
                TestUtils.assertEquals("switch(V)", expression.getSignature());
                Assert.assertEquals(ColumnType.LONG, expression.getDataType());
                try (Function first = binder.instantiate(expression, input, sqlExecutionContext);
                     Function second = binder.instantiate(expression, input, sqlExecutionContext)) {
                    Assert.assertEquals(10, first.getLong(numericRecord(ColumnType.LONG, 0, 1)));
                    Assert.assertEquals(3, second.getLong(numericRecord(ColumnType.LONG, 0, 2)));
                    Assert.assertEquals(Numbers.LONG_NULL, first.getLong(numericRecord(ColumnType.LONG, 0, 3)));
                }
                arguments.clear();
                arguments.add(literal("label", 5));
                arguments.add(constant("null", 15));
                arguments.add(constant("1", 25));
                arguments.add(constant("null", 32));
                arguments.add(constant("2", 42));
                final OutputSchema strings = new OutputSchema().add(9, "label", ColumnType.STRING, true);
                final BoundExpression lastNull = binder.bind(call("switch", 0, arguments), strings, "t", sqlExecutionContext);
                try (Function first = binder.instantiate(lastNull, strings, sqlExecutionContext);
                     Function second = binder.instantiate(lastNull, strings, sqlExecutionContext)) {
                    binder.clear();
                    final Record record = new Record() {
                        @Override
                        public CharSequence getStrA(int index) {
                            Assert.assertEquals(0, index);
                            return null;
                        }
                    };
                    Assert.assertEquals(2, first.getInt(record));
                    Assert.assertEquals(2, second.getInt(record));
                }
            }
        });
    }

    @Test
    public void testTimestampLiteralCanonicalisationSurvivesReconstruction() throws Exception {
        assertMemoryLeak(() -> {
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
            final OutputSchema original = new OutputSchema().add(1, "unused", ColumnType.INT, true)
                    .add(7, "ts", ColumnType.TIMESTAMP_MICRO, true);
            final OutputSchema pruned = new OutputSchema().add(7, "ts", ColumnType.TIMESTAMP_MICRO, true);
            try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                final FunctionExpression expression = (FunctionExpression) binder.bind(binary("<", 3,
                                literal("ts", 0), constant("'1970-01-01T00:00:00.000001001Z'", 5)),
                        original, "t", sqlExecutionContext);
                final ConstantExpression timestamp = (ConstantExpression) expression.argumentAt(1);
                Assert.assertEquals(ColumnType.TIMESTAMP_MICRO, timestamp.getDataType());
                Assert.assertEquals(2, timestamp.getLongValue());
                Assert.assertEquals(ColumnType.TIMESTAMP_MICRO, expression.argumentAt(0).getDataType());
                try (
                        Function first = binder.instantiate(expression, original, sqlExecutionContext);
                        Function second = binder.instantiate(expression, pruned, sqlExecutionContext);
                        Function rebuiltConstant = binder.instantiate(timestamp, pruned, sqlExecutionContext)
                ) {
                    Assert.assertEquals(ColumnType.TIMESTAMP_MICRO, rebuiltConstant.getType());
                    Assert.assertEquals(2, rebuiltConstant.getTimestamp(null));
                    Assert.assertEquals(first.isConstant(), second.isConstant());
                    Assert.assertEquals(first.isRuntimeConstant(), second.isRuntimeConstant());
                    Assert.assertEquals(first.isNonDeterministic(), second.isNonDeterministic());
                    Assert.assertEquals(first.isStableWithinExecution(), second.isStableWithinExecution());
                    binder.clear();
                    Assert.assertTrue(first.getBool(timestampRecord(1, 1)));
                    Assert.assertTrue(second.getBool(timestampRecord(0, 1)));
                    Assert.assertFalse(second.getBool(timestampRecord(0, 2)));
                    Assert.assertFalse(second.getBool(timestampRecord(0, Long.MIN_VALUE)));
                }
                original.setTimestampIndex(1);
                final FunctionExpression predicate = (FunctionExpression) binder.bindPredicate(binary("<", 3,
                                literal("ts", 0), constant("'1970-01-01T00:00:00.000001001Z'", 5)),
                        original, "t", sqlExecutionContext);
                Assert.assertEquals(ColumnType.TIMESTAMP_MICRO, predicate.argumentAt(1).getDataType());
                Assert.assertEquals(2, ((ConstantExpression) predicate.argumentAt(1)).getLongValue());
            }
        });
    }

    @Test
    public void testTimestampRuntimeParameterRebuildAndFullColumnType() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.clear();
            bindVariableService.setTimestampNano(0, 1001);
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
            final OutputSchema input = new OutputSchema().add(7, "ts", ColumnType.TIMESTAMP_NANO, true);
            try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                final FunctionExpression expression = (FunctionExpression) binder.bind(
                        binary("=", 3, literal("ts", 0), parameter("$1", 5)), input, "t", sqlExecutionContext);
                Assert.assertEquals(ColumnType.TIMESTAMP_NANO, expression.argumentAt(0).getDataType());
                Assert.assertEquals(ColumnType.TIMESTAMP_NANO, expression.argumentAt(1).getDataType());
                try (
                        Function first = binder.instantiate(expression, input, sqlExecutionContext);
                        Function second = binder.instantiate(expression, input, sqlExecutionContext);
                        Function column = binder.instantiate(expression.argumentAt(0), input, sqlExecutionContext)
                ) {
                    Assert.assertEquals(ColumnType.TIMESTAMP_NANO, column.getType());
                    binder.clear();
                    first.init(null, sqlExecutionContext);
                    bindVariableService.setTimestampNano(0, 2002);
                    second.init(null, sqlExecutionContext);
                    Assert.assertTrue(first.getBool(timestampRecord(0, 1001)));
                    Assert.assertTrue(second.getBool(timestampRecord(0, 2002)));
                    Assert.assertFalse(second.getBool(timestampRecord(0, 1001)));
                    Assert.assertEquals(1234, column.getTimestamp(timestampRecord(0, 1234)));
                }
            }
        });
    }

    @Test
    public void testTypedNumericLeavesRelocateAndPreserveWidening() throws Exception {
        assertMemoryLeak(() -> {
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
            final int[] types = {ColumnType.LONG, ColumnType.FLOAT, ColumnType.DOUBLE};
            final String[] signatures = {"+(LL)", "+(FF)", "+(DD)"};
            for (int i = 0; i < types.length; i++) {
                final int type = types[i];
                final OutputSchema input = new OutputSchema().add(1, "discarded", ColumnType.INT, true)
                        .add(8, "value", type, true);
                final OutputSchema pruned = new OutputSchema().add(8, "value", type, true);
                try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                    final FunctionExpression expression = (FunctionExpression) binder.bind(
                            binary("+", 6, literal("value", 0), constant("1", 8)), input, "t", sqlExecutionContext);
                    Assert.assertEquals(type, expression.getDataType());
                    TestUtils.assertEquals(signatures[i], expression.getSignature());
                    Assert.assertEquals(ColumnType.INT, expression.argumentAt(1).getDataType());
                    try (Function function = binder.instantiate(expression, pruned, sqlExecutionContext)) {
                        binder.clear();
                        input.clear();
                        pruned.clear();
                        Assert.assertEquals(3.0, function.getDouble(new Record() {
                            @Override
                            public double getDouble(int columnIndex) {
                                Assert.assertEquals(0, columnIndex);
                                return 2.0;
                            }

                            @Override
                            public float getFloat(int columnIndex) {
                                Assert.assertEquals(0, columnIndex);
                                return 2.0f;
                            }

                            @Override
                            public long getLong(int columnIndex) {
                                Assert.assertEquals(0, columnIndex);
                                return 2L;
                            }
                        }), 0.0);
                    }
                }
            }
        });
    }

    @Test
    public void testVariadicInDynamicValuesAndNulls() throws Exception {
        assertMemoryLeak(() -> {
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
            for (int type : new int[]{ColumnType.LONG, ColumnType.DOUBLE}) {
                final OutputSchema input = new OutputSchema().add(7, "v", type, true);
                final ObjList<ExpressionNode> arguments = new ObjList<>();
                arguments.add(literal("v", 0));
                arguments.add(binary("+", 6, literal("v", 4), constant("1", 8)));
                arguments.add(constant("null", 11));
                arguments.add(constant("2", 17));
                try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                    final BoundExpression expression = binder.bind(call("in", 2, arguments), input, "t", sqlExecutionContext);
                    try (Function first = binder.instantiate(expression, input, sqlExecutionContext);
                         Function second = binder.instantiate(expression, input, sqlExecutionContext)) {
                        binder.clear();
                        first.init(null, sqlExecutionContext);
                        second.init(null, sqlExecutionContext);
                        Assert.assertTrue(first.getBool(numericRecord(type, 0, 2)));
                        Assert.assertTrue(second.getBool(numericRecord(type, 0, Double.NaN)));
                        Assert.assertFalse(second.getBool(numericRecord(type, 0, 3)));
                    }
                }
            }
        });
    }

    @Test
    public void testVariadicInPreservesEveryArgumentAndRebuildsNativeSets() throws Exception {
        assertMemoryLeak(() -> {
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
            final OutputSchema input = new OutputSchema().add(1, "unused", ColumnType.INT, true)
                    .add(8, "v", ColumnType.LONG, true);
            final OutputSchema pruned = new OutputSchema().add(8, "v", ColumnType.LONG, true);
            for (int size : new int[]{1, 2, 3, 40}) {
                final ObjList<ExpressionNode> arguments = new ObjList<>();
                arguments.add(literal("v", 0));
                for (int i = 1; i <= size; i++) {
                    arguments.add(constant(Integer.toString(i), i * 4));
                }
                try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                    final FunctionExpression expression = (FunctionExpression) binder.bind(call("in", 2, arguments), input, "t", sqlExecutionContext);
                    Assert.assertEquals(size + 1, expression.getArgumentCount());
                    TestUtils.assertEquals("in(LV)", expression.getSignature());
                    for (int i = 1; i <= size; i++) {
                        Assert.assertEquals(i * 4, expression.getArgumentPosition(i));
                    }
                    try (Function first = binder.instantiate(expression, pruned, sqlExecutionContext);
                         Function second = binder.instantiate(expression, pruned, sqlExecutionContext)) {
                        binder.clear();
                        Assert.assertTrue(first.getBool(numericRecord(ColumnType.LONG, 0, size)));
                        Assert.assertTrue(second.getBool(numericRecord(ColumnType.LONG, 0, size)));
                        Assert.assertFalse(first.getBool(numericRecord(ColumnType.LONG, 0, size + 1)));
                        Assert.assertFalse(second.getBool(numericRecord(ColumnType.LONG, 0, size + 1)));
                    }
                }
            }
        });
    }

    @Test
    public void testVariadicInRuntimeConstantsAndFailureRecovery() throws Exception {
        assertMemoryLeak(() -> {
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
            for (int type : new int[]{ColumnType.LONG, ColumnType.DOUBLE}) {
                bindVariableService.clear();
                final OutputSchema input = new OutputSchema().add(7, "v", type, true);
                final ObjList<ExpressionNode> arguments = new ObjList<>();
                arguments.add(literal("v", 0));
                arguments.add(constant("1", 5));
                arguments.add(parameter("$1", 8));
                arguments.add(constant("3", 12));
                try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                    final FunctionExpression expression = (FunctionExpression) binder.bind(call("in", 2, arguments), input, "t", sqlExecutionContext);
                    Assert.assertEquals(ColumnType.STRING, expression.argumentAt(2).getDataType());
                    try (Function first = binder.instantiate(expression, input, sqlExecutionContext);
                         Function second = binder.instantiate(expression, input, sqlExecutionContext)) {
                        bindVariableService.setStr(0, "2");
                        first.init(null, sqlExecutionContext);
                        second.init(null, sqlExecutionContext);
                        Assert.assertTrue(first.getBool(numericRecord(type, 0, 2)));
                        Assert.assertTrue(second.getBool(numericRecord(type, 0, 2)));
                        bindVariableService.setStr(0, "4");
                        second.init(null, sqlExecutionContext);
                        Assert.assertTrue(first.getBool(numericRecord(type, 0, 2)));
                        Assert.assertFalse(second.getBool(numericRecord(type, 0, 2)));
                        Assert.assertTrue(second.getBool(numericRecord(type, 0, 4)));
                    }
                    arguments.setQuick(2, constant("'bad'", 8));
                    try {
                        binder.bind(call("in", 2, arguments), input, "t", sqlExecutionContext);
                        Assert.fail();
                    } catch (SqlException e) {
                        Assert.assertEquals(8, e.getPosition());
                        TestUtils.assertContains(e.getFlyweightMessage(), "invalid");
                    }
                    binder.clear();
                    try (Function recovered = binder.instantiate(binder.bind(literal("v", 0), input, "t", sqlExecutionContext), input)) {
                        Assert.assertEquals(9, recovered.getDouble(numericRecord(type, 0, 9)), 0);
                    }
                }
            }
        });
    }

    private static FunctionExpression andDescription(FunctionParser parser, BoundExpression left, BoundExpression right) {
        final IntList positions = new IntList();
        positions.add(left.getPosition());
        positions.add(right.getPosition());
        return new FunctionExpression().of(
                parser.getFunctionFactoryCache().getOverloadList("and").getQuick(0),
                new ObjList<>(left, right), positions, ColumnType.BOOLEAN, left.getFunctionFlags() | right.getFunctionFlags(), 11);
    }

    private static void assertBindingError(FunctionBindingHarness binder, OutputSchema input, String name, String alias, String message) throws SqlException {
        try {
            binder.bind(literal(name, 17), input, alias, sqlExecutionContext);
            Assert.fail();
        } catch (SqlException e) {
            Assert.assertEquals(17, e.getPosition());
            TestUtils.assertEquals(message, e.getFlyweightMessage());
        }
    }

    private static ExpressionNode binary(String token, int position, ExpressionNode lhs, ExpressionNode rhs) {
        final ExpressionNode expression = ExpressionNode.FACTORY.newInstance().of(
                "cast".equals(token) ? ExpressionNode.FUNCTION : ExpressionNode.OPERATION, token, 0, position);
        expression.paramCount = 2;
        expression.lhs = lhs;
        expression.rhs = rhs;
        return expression;
    }

    private static ExpressionNode call(String name, int position, ObjList<ExpressionNode> arguments) {
        final ExpressionNode expression = ExpressionNode.FACTORY.newInstance().of(ExpressionNode.FUNCTION, name, 0, position);
        expression.paramCount = arguments.size();
        if (arguments.size() < 3) {
            expression.rhs = arguments.getLast();
            if (arguments.size() == 2) {
                expression.lhs = arguments.getQuick(0);
            }
        } else {
            for (int i = arguments.size() - 1; i >= 0; i--) {
                expression.args.add(arguments.getQuick(i));
            }
        }
        return expression;
    }

    private static ExpressionNode constant(String token, int position) {
        return ExpressionNode.FACTORY.newInstance().of(ExpressionNode.CONSTANT, token, 0, position);
    }

    private static ExpressionNode countedSubstring() {
        final ExpressionNode length = ExpressionNode.FACTORY.newInstance().of(ExpressionNode.FUNCTION, "counted_len", 0, 20);
        return call("substring", 7, new ObjList<>(literal("s", 17), literal("i", 19), length));
    }

    private static Record intRecord(int value) {
        return new Record() {
            @Override
            public int getInt(int columnIndex) {
                Assert.assertEquals(0, columnIndex);
                return value;
            }
        };
    }

    private static ExpressionNode literal(String token, int position) {
        return ExpressionNode.FACTORY.newInstance().of(ExpressionNode.LITERAL, token, 0, position);
    }

    private static Record numericRecord(int type, int index, double value) {
        return new Record() {
            @Override
            public byte getByte(int columnIndex) {
                Assert.assertEquals(ColumnType.BYTE, type);
                Assert.assertEquals(index, columnIndex);
                return (byte) value;
            }

            @Override
            public short getShort(int columnIndex) {
                Assert.assertEquals(ColumnType.SHORT, type);
                Assert.assertEquals(index, columnIndex);
                return (short) value;
            }

            @Override
            public char getChar(int columnIndex) {
                Assert.assertEquals(ColumnType.CHAR, type);
                Assert.assertEquals(index, columnIndex);
                return (char) value;
            }

            @Override
            public long getDate(int columnIndex) {
                Assert.assertEquals(ColumnType.DATE, type);
                Assert.assertEquals(index, columnIndex);
                return (long) value;
            }

            @Override
            public int getIPv4(int columnIndex) {
                Assert.assertEquals(ColumnType.IPv4, type);
                Assert.assertEquals(index, columnIndex);
                return (int) value;
            }

            @Override
            public double getDouble(int columnIndex) {
                Assert.assertEquals(ColumnType.DOUBLE, type);
                Assert.assertEquals(index, columnIndex);
                return value;
            }

            @Override
            public float getFloat(int columnIndex) {
                Assert.assertEquals(ColumnType.FLOAT, type);
                Assert.assertEquals(index, columnIndex);
                return (float) value;
            }

            @Override
            public int getInt(int columnIndex) {
                Assert.assertEquals(ColumnType.INT, type);
                Assert.assertEquals(index, columnIndex);
                return Double.isNaN(value) ? Numbers.INT_NULL : (int) value;
            }

            @Override
            public long getLong(int columnIndex) {
                Assert.assertEquals(ColumnType.LONG, type);
                Assert.assertEquals(index, columnIndex);
                return Double.isNaN(value) ? Numbers.LONG_NULL : (long) value;
            }
        };
    }

    private static ExpressionNode parameter(String token, int position) {
        return ExpressionNode.FACTORY.newInstance().of(ExpressionNode.BIND_VARIABLE, token, 0, position);
    }

    private static Record stringRecord(int index, String value) {
        return new Record() {
            @Override
            public CharSequence getStrA(int columnIndex) {
                Assert.assertEquals(index, columnIndex);
                return value;
            }

            @Override
            public CharSequence getStrB(int columnIndex) {
                Assert.assertEquals(index, columnIndex);
                return value;
            }

            @Override
            public int getStrLen(int columnIndex) {
                Assert.assertEquals(index, columnIndex);
                return value == null ? -1 : value.length();
            }
        };
    }

    private static Record textAndIntRecord(String text, int value) {
        return new Record() {
            @Override
            public int getInt(int col) {
                Assert.assertEquals(1, col);
                return value;
            }

            @Override
            public CharSequence getStrA(int col) {
                Assert.assertEquals(0, col);
                return text;
            }

            @Override
            public CharSequence getStrB(int col) {
                return getStrA(col);
            }

            @Override
            public int getStrLen(int col) {
                return getStrA(col).length();
            }
        };
    }

    private static Record timestampRecord(int index, long value) {
        return new Record() {
            @Override
            public long getTimestamp(int columnIndex) {
                Assert.assertEquals(index, columnIndex);
                return value;
            }
        };
    }

    private static ExpressionNode unary(String token, int position, ExpressionNode argument) {
        final ExpressionNode expression = ExpressionNode.FACTORY.newInstance().of(ExpressionNode.FUNCTION, token, 0, position);
        expression.paramCount = 1;
        expression.rhs = argument;
        return expression;
    }

    private static class CountingIntConstant extends IntConstant {
        private final int[] closes;

        private CountingIntConstant(int value, int[] closes) {
            super(value);
            this.closes = closes;
        }

        @Override
        public void close() {
            closes[0]++;
        }
    }

    private static class CountingIntFunction extends IntFunction implements UnaryFunction {
        private final Function arg;
        private final int[] closes;

        private CountingIntFunction(Function arg, int[] closes) {
            this.arg = arg;
            this.closes = closes;
        }

        @Override
        public void close() {
            closes[0]++;
            arg.close();
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public int getInt(Record rec) {
            return arg.getInt(rec);
        }
    }
}
