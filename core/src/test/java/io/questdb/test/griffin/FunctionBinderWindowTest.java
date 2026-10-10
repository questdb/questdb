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
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.griffin.FunctionParser;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.griffin.engine.functions.constants.IntConstant;
import io.questdb.griffin.engine.functions.window.LeadDoubleFunctionFactory;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.WindowExpression;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.test.griffin.FunctionBindingHarness.parser;

public class FunctionBinderWindowTest extends AbstractCairoTest {
    @Test
    public void testContextAndWindowRootAreRequired() throws Exception {
        assertMemoryLeak(() -> {
            final OutputSchema input = schema(false);
            try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser(engine, new ObjList<>()))) {
                try {
                    binder.bindWindow(call("row_number"), input, null, sqlExecutionContext);
                    Assert.fail("missing window context accepted");
                } catch (SqlException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "window");
                }
                configure(null, false, WindowExpression.FRAMING_RANGE, Long.MIN_VALUE, 0);
                try {
                    try {
                        binder.bind(call("row_number"), input, null, sqlExecutionContext);
                        Assert.fail("scalar window call accepted");
                    } catch (SqlException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "window function called in non-window context");
                    }
                    try {
                        binder.bindWindow(call("sum", call("row_number")), input, null, sqlExecutionContext);
                        Assert.fail("nested window accepted");
                    } catch (SqlException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "window function called in non-window context");
                    }
                    binder.bindWindow(call("row_number"), input, null, sqlExecutionContext);
                } finally {
                    sqlExecutionContext.clearWindowContext();
                }
            }
        });
    }

    @Test
    public void testCurrentRowFramesReleaseNativePartitionExpressions() throws Exception {
        assertMemoryLeak(() -> {
            final OutputSchema input = schema(false);
            input.add(28, "i", ColumnType.INT, true);
            final GenericRecordMetadata metadata = metadata(false);
            metadata.add(new TableColumnMetadata("i", ColumnType.INT));
            final ObjList<String> names = new ObjList<>("first_value", "last_value", "nth_value", "sum", "ksum", "avg", "min", "max",
                    "count", "stddev_pop", "var_samp", "corr", "covar_pop");
            final FunctionParser parser = parser(engine, new ObjList<>());
            try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                for (int i = 0; i < names.size(); i++) {
                    final String name = names.getQuick(i);
                    final ExpressionNode window = name.equals("nth_value")
                            ? call(name, literal("v"), constant("1"))
                            : name.equals("corr") || name.equals("covar_pop")
                              ? call(name, literal("v"), literal("v")) : call(name, literal("v"));
                    for (boolean ignoreNulls : new boolean[]{false, true}) {
                        if (ignoreNulls && !name.equals("first_value") && !name.equals("last_value") && !name.equals("nth_value")) {
                            continue;
                        }
                        final BoundExpression partition = binder.bind(call("in", literal("i"), constant("1"), constant("2")),
                                input, null, sqlExecutionContext);
                        final ObjList<Function> prototypePartition = new ObjList<>();
                        prototypePartition.add(binder.instantiate(partition, input, sqlExecutionContext));
                        try {
                            configure(new VirtualRecord(prototypePartition), ignoreNulls, WindowExpression.FRAMING_ROWS, 0, 0);
                            final FunctionExpression expression = binder.bindWindow(window, input, null, sqlExecutionContext);
                            if (!name.equals("last_value") || ignoreNulls) {
                                Assert.assertNull(prototypePartition.getQuick(0));
                            }
                            final ObjList<Function> runtimePartition = new ObjList<>();
                            runtimePartition.add(binder.instantiate(partition, input, metadata, sqlExecutionContext));
                            try {
                                configure(new VirtualRecord(runtimePartition), ignoreNulls, WindowExpression.FRAMING_ROWS, 0, 0);
                                try (WindowFunction function = binder.instantiateWindow(expression, input, metadata, sqlExecutionContext)) {
                                    if (!name.equals("last_value") || ignoreNulls) {
                                        Assert.assertNull(runtimePartition.getQuick(0));
                                    }
                                    Assert.assertEquals(expression.getDataType(), function.getType());
                                }
                            } finally {
                                Misc.freeObjList(runtimePartition);
                            }
                        } finally {
                            sqlExecutionContext.clearWindowContext();
                            Misc.freeObjList(prototypePartition);
                            binder.clear();
                            parser.clear();
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testIndependentFinalLayoutsSurviveCompilerReset() throws Exception {
        assertMemoryLeak(() -> {
            final ObjList<Function> constructions = new ObjList<>();
            final FunctionParser parser = parser(engine, constructions);
            final OutputSchema full = schema(true);
            final OutputSchema pruned = schema(false);
            try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                configure(null, false, WindowExpression.FRAMING_ROWS, Long.MIN_VALUE, 0);
                try {
                    final ExpressionNode ast = call("sum", literal("v"));
                    ast.windowExpression = WindowExpression.FACTORY.newInstance();
                    final FunctionExpression expression = binder.bindWindow(ast, full, null, sqlExecutionContext);
                    Assert.assertTrue(expression.isWindow());
                    Assert.assertFalse(expression.isAggregate());
                    Assert.assertEquals(1, constructions.size());
                    try (WindowFunction owner = binder.instantiateWindow(expression, pruned, metadata(false), sqlExecutionContext);
                         WindowFunction worker = binder.instantiateWindow(expression, full, metadata(true), sqlExecutionContext)) {
                        Assert.assertEquals(3, constructions.size());
                        Assert.assertNotSame(constructions.getQuick(0), owner);
                        Assert.assertNotSame(owner, worker);
                        binder.clear();
                        parser.clear();
                        full.clear();
                        pruned.clear();
                        for (int pass = 0; pass < 2; pass++) {
                            owner.init(null, sqlExecutionContext);
                            worker.init(null, sqlExecutionContext);
                            owner.toTop();
                            worker.toTop();
                            owner.computeNext(record(0, 2));
                            owner.computeNext(record(0, 3));
                            worker.computeNext(record(1, 7));
                            Assert.assertEquals(5, owner.getDouble(null), 0);
                            Assert.assertEquals(7, worker.getDouble(null), 0);
                            owner.cursorClosed();
                            worker.cursorClosed();
                        }
                    }
                } finally {
                    sqlExecutionContext.clearWindowContext();
                }
            }
        });
    }

    @Test
    public void testRankingValueAndStatisticalFamiliesReconstructSelectedFunctions() throws Exception {
        assertMemoryLeak(() -> {
            final ObjList<ExpressionNode> calls = new ObjList<>();
            final ObjList<String> zeroArgument = new ObjList<>("row_number", "rank", "dense_rank", "percent_rank", "cume_dist", "count");
            for (int i = 0; i < zeroArgument.size(); i++) {
                calls.add(call(zeroArgument.getQuick(i)));
            }
            final ObjList<String> unary = new ObjList<>("sum", "ksum", "avg", "min", "max", "count", "first_value", "last_value",
                    "stddev", "stddev_pop", "stddev_samp", "variance", "var_pop", "var_samp", "lead", "lag");
            for (int i = 0; i < unary.size(); i++) {
                calls.add(call(unary.getQuick(i), literal("v")));
            }
            calls.add(call("ntile", constant("2")));
            calls.add(call("nth_value", literal("v"), constant("2")));
            calls.add(call("corr", literal("v"), literal("v")));
            calls.add(call("covar_pop", literal("v"), literal("v")));
            calls.add(call("covar_samp", literal("v"), literal("v")));
            final FunctionParser parser = parser(engine, new ObjList<>());
            final OutputSchema input = schema(false);
            input.add(28, "ts", ColumnType.TIMESTAMP_MICRO, true);
            input.setTimestampIndex(1);
            final GenericRecordMetadata metadata = metadata(false);
            metadata.add(new TableColumnMetadata("ts", ColumnType.TIMESTAMP_MICRO));
            metadata.setTimestampIndex(1);
            try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                for (int i = 0; i < calls.size(); i++) {
                    sqlExecutionContext.configureWindowContext(null, null, new ArrayColumnTypes(), true,
                            RecordCursorFactory.SCAN_DIRECTION_FORWARD, 0, WindowExpression.FRAMING_RANGE,
                            Long.MIN_VALUE, (char) 0, 0, 0, 0, (char) 0, 0, 0, WindowExpression.EXCLUDE_NO_OTHERS, 0,
                            1, ColumnType.TIMESTAMP_MICRO, false, 0);
                    try {
                        final FunctionExpression expression = binder.bindWindow(calls.getQuick(i), input, null, sqlExecutionContext);
                        try (WindowFunction first = binder.instantiateWindow(expression, input, metadata, sqlExecutionContext);
                             WindowFunction second = binder.instantiateWindow(expression, input, metadata, sqlExecutionContext)) {
                            Assert.assertNotSame(first, second);
                            Assert.assertEquals(expression.getDataType(), first.getType());
                            Assert.assertEquals(first.getPassCount(), second.getPassCount());
                        }
                    } finally {
                        sqlExecutionContext.clearWindowContext();
                        binder.clear();
                        parser.clear();
                    }
                }
            }
        });
    }

    @Test
    public void testZeroOffsetLeadClosesDiscardedDefaultBeforeReturnAndOnCloseFailure() throws Exception {
        assertMemoryLeak(() -> {
            for (boolean failClose : new boolean[]{false, true}) {
                final CountingDouble value = new CountingDouble(false);
                final CountingDouble unused = new CountingDouble(failClose);
                final ObjList<Function> args = new ObjList<>(value, IntConstant.ZERO, unused);
                final IntList positions = new IntList();
                positions.add(1);
                positions.add(2);
                positions.add(3);
                configure(null, false, WindowExpression.FRAMING_RANGE, Long.MIN_VALUE, 0);
                try {
                    try (Function function = new LeadDoubleFunctionFactory().newInstance(0, args, positions, configuration, sqlExecutionContext)) {
                        args.clear();
                        Assert.assertFalse(failClose);
                        Assert.assertEquals(1, unused.closeCount);
                        Assert.assertEquals(0, value.closeCount);
                    } catch (IllegalStateException e) {
                        Assert.assertTrue(failClose);
                        Assert.assertEquals("close default", e.getMessage());
                        Misc.freeObjList(args, e);
                    }
                    Assert.assertEquals(1, unused.closeCount);
                    Assert.assertEquals(1, value.closeCount);
                } finally {
                    sqlExecutionContext.clearWindowContext();
                }
            }
        });
    }

    private static ExpressionNode call(String name) {
        return call(name, new ObjList<>());
    }

    private static ExpressionNode call(String name, ExpressionNode argument) {
        return call(name, new ObjList<>(argument));
    }

    private static ExpressionNode call(String name, ExpressionNode first, ExpressionNode second) {
        return call(name, new ObjList<>(first, second));
    }

    private static ExpressionNode call(String name, ExpressionNode first, ExpressionNode second, ExpressionNode third) {
        return call(name, new ObjList<>(first, second, third));
    }

    private static ExpressionNode call(String name, ObjList<ExpressionNode> args) {
        final ExpressionNode node = ExpressionNode.FACTORY.newInstance().of(ExpressionNode.FUNCTION, name, 0, 0);
        node.paramCount = args.size();
        if (args.size() == 1) {
            node.rhs = args.getQuick(0);
        } else if (args.size() == 2) {
            node.lhs = args.getQuick(0);
            node.rhs = args.getQuick(1);
        } else {
            for (int i = args.size() - 1; i >= 0; i--) {
                node.args.add(args.getQuick(i));
            }
        }
        return node;
    }

    private static ExpressionNode constant(String value) {
        return ExpressionNode.FACTORY.newInstance().of(ExpressionNode.CONSTANT, value, 0, 3);
    }

    private static ExpressionNode literal(String name) {
        return ExpressionNode.FACTORY.newInstance().of(ExpressionNode.LITERAL, name, 0, 1);
    }

    private static GenericRecordMetadata metadata(boolean isFull) {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        if (isFull) {
            metadata.add(new TableColumnMetadata("unused", ColumnType.INT));
        }
        metadata.add(new TableColumnMetadata("physical", ColumnType.DOUBLE));
        return metadata;
    }

    private static Record record(int index, double value) {
        return new Record() {
            @Override
            public double getDouble(int col) {
                Assert.assertEquals(index, col);
                return value;
            }
        };
    }

    private static OutputSchema schema(boolean isFull) {
        final OutputSchema schema = new OutputSchema();
        if (isFull) {
            schema.add(10, "unused", ColumnType.INT, true);
        }
        return schema.add(27, "v", ColumnType.DOUBLE, true);
    }

    private void configure(VirtualRecord partition, boolean ignoreNulls, int framingMode, long lo, long hi) throws SqlException {
        final ArrayColumnTypes keyTypes = new ArrayColumnTypes();
        if (partition != null) {
            keyTypes.add(ColumnType.BOOLEAN);
        }
        sqlExecutionContext.configureWindowContext(partition, null, keyTypes, true,
                RecordCursorFactory.SCAN_DIRECTION_OTHER, 0, framingMode,
                lo, (char) 0, 0, 0, hi, (char) 0, 0, 0, WindowExpression.EXCLUDE_NO_OTHERS, 0,
                -1, ColumnType.UNDEFINED, ignoreNulls, 0);
    }

    private static class CountingDouble extends DoubleFunction {
        private final boolean failClose;
        private int closeCount;

        private CountingDouble(boolean failClose) {
            this.failClose = failClose;
        }

        @Override
        public void close() {
            closeCount++;
            if (failClose) {
                throw new IllegalStateException("close default");
            }
        }

        @Override
        public double getDouble(Record rec) {
            return 1;
        }
    }
}
