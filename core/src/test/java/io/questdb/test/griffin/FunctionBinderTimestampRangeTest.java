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
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.griffin.engine.functions.bool.BetweenTimestampFunctionFactory;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ConstantExpression;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.std.Chars;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class FunctionBinderTimestampRangeTest extends AbstractCairoTest {
    @Test
    public void testSqlParserSetPredicatesBindWithoutAstRewriting() throws Exception {
        assertMemoryLeak(() -> {
            final OutputSchema input = new OutputSchema().add(70, "ts", ColumnType.TIMESTAMP_MICRO, true);
            input.setTimestampIndex(0);
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
                 FunctionBindingHarness binder = new FunctionBindingHarness(engine, new FunctionParser(configuration, engine.getFunctionFactoryCache()))) {
                for (String sql : new String[]{"ts BETWEEN '1970-01-01' AND '1970-01-02'",
                        "ts NOT BETWEEN '1970-01-01' AND '1970-01-02'",
                        "ts IN '1970-01-01'", "ts NOT IN '1970-01-01'",
                        "ts IN ('1970-01-01','1970-01-02')", "ts NOT IN ('1970-01-01','1970-01-02')"}) {
                    final ExpressionNode node = compiler.parseExpression(sql);
                    final boolean negated = sql.contains("NOT");
                    final ExpressionNode predicate = negated ? node.rhs : node;
                    final boolean between = sql.contains("BETWEEN");
                    final boolean list = sql.contains("(");
                    TestUtils.assertEquals(between ? "between" : "in", predicate.token);
                    Assert.assertEquals(sql, list ? ExpressionNode.FUNCTION : ExpressionNode.SET_OPERATION, predicate.type);
                    Assert.assertEquals(sql, between || list ? 3 : 2, predicate.paramCount);
                    final BoundExpression expression = binder.bindPredicate(node, input, null, sqlExecutionContext);
                    try (Function function = binder.instantiate(expression, input, sqlExecutionContext)) {
                        compiler.clear();
                        binder.clear();
                        Assert.assertEquals(!negated, function.getBool(record(0, 0)));
                    }
                }
            }
        });
    }

    @Test
    public void testBareNumericNativeInIsATimestampPoint() throws Exception {
        assertMemoryLeak(() -> {
            final OutputSchema input = new OutputSchema().add(70, "ts", ColumnType.TIMESTAMP_MICRO, true);
            input.setTimestampIndex(0);
            try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, new FunctionParser(configuration, engine.getFunctionFactoryCache()))) {
                for (int variant = 0; variant < 4; variant++) {
                    final ExpressionNode predicate = call("in", literal("ts"), variant == 0 ? cast(constant("0"), "long") : constant("0"));
                    final BoundExpression expression = variant < 2 ? binder.bindPredicate(predicate, input, null, sqlExecutionContext)
                            : variant == 2 ? binder.bind(predicate, input, null, sqlExecutionContext)
                              : binder.bindPredicate(predicate, input, null, new IntHashSet(), sqlExecutionContext);
                    try (Function function = binder.instantiate(expression, input, sqlExecutionContext)) {
                        Assert.assertTrue(function.getBool(record(0, 0)));
                        Assert.assertFalse(function.getBool(record(0, 1)));
                    }
                    binder.clear();
                }
            }
        });
    }

    @Test
    public void testBetweenLiteralsCanonicaliseToColumnPrecision() throws Exception {
        assertMemoryLeak(() -> {
            final OutputSchema input = new OutputSchema().add(70, "ts", ColumnType.TIMESTAMP_MICRO, true);
            input.setTimestampIndex(0);
            try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, new FunctionParser(configuration, engine.getFunctionFactoryCache()))) {
                for (int variant = 0; variant < 6; variant++) {
                    final boolean explicit = variant == 4;
                    ExpressionNode predicate = call("between", literal("ts"),
                            explicit ? cast(text("1969-12-31T23:59:59.999999999Z"), "timestamp_ns")
                                    : text("1969-12-31T23:59:59.999999999Z"),
                            variant == 5 ? literal("ts") : text("1970-01-01T00:00:00.000000001Z"));
                    if (variant == 1) {
                        predicate = call("not", predicate);
                    } else if (variant == 2) {
                        predicate = call("or", predicate, constant("false"));
                    }
                    FunctionExpression bound = (FunctionExpression) (variant == 3
                            ? binder.bind(predicate, input, null, sqlExecutionContext)
                            : binder.bindPredicate(predicate, input, null, sqlExecutionContext));
                    if (variant == 1) {
                        bound = (FunctionExpression) bound.argumentAt(0);
                    }
                    final ConstantExpression lo = (ConstantExpression) bound.argumentAt(1);
                    if (variant == 5) {
                        // A bound beside a column keeps its precision: it rounds only when both bounds are constant.
                        Assert.assertEquals(ColumnType.TIMESTAMP_NANO, lo.getDataType());
                        Assert.assertEquals(-1, lo.getLongValue());
                        TestUtils.assertEquals("1969-12-31T23:59:59.999999999Z", lo.getTimestampText());
                    } else {
                        Assert.assertEquals(ColumnType.TIMESTAMP_MICRO, lo.getDataType());
                        Assert.assertEquals(0, lo.getLongValue());
                        Assert.assertNull(lo.getTimestampText());
                    }
                    binder.clear();
                }
            }
        });
    }

    @Test
    public void testBetweenRuntimeBoundsRebuildAcrossLayoutsAndPrecisions() throws Exception {
        assertMemoryLeak(() -> {
            for (int type : new int[]{ColumnType.TIMESTAMP_MICRO, ColumnType.TIMESTAMP_NANO}) {
                bindVariableService.clear();
                final int boundType = type == ColumnType.TIMESTAMP_MICRO ? ColumnType.TIMESTAMP_NANO : ColumnType.TIMESTAMP_MICRO;
                final long bound = boundType == ColumnType.TIMESTAMP_NANO ? 1001 : 1;
                final long limit = type == ColumnType.TIMESTAMP_NANO ? 1000 : 1;
                bindVariableService.setTimestampWithType(0, boundType, bound);
                bindVariableService.setTimestampWithType(1, boundType, -bound);
                final OutputSchema original = wideSchema(type);
                final OutputSchema pruned = new OutputSchema().add(70, "ts", type, true);
                final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
                try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                    final BoundExpression expression = binder.bindPredicate(call("between", literal("ts"), parameter("$1"), parameter("$2")),
                            original, null, sqlExecutionContext);
                    try (Function owner = binder.instantiate(expression, pruned, sqlExecutionContext);
                         Function worker = binder.instantiate(expression, original, sqlExecutionContext)) {
                        Assert.assertNotSame(owner, worker);
                        binder.clear();
                        parser.clear();
                        original.clear();
                        pruned.clear();
                        owner.init(null, sqlExecutionContext);
                        worker.init(null, sqlExecutionContext);
                        for (long value : new long[]{-limit - 1, -limit, 0, limit, limit + 1, Numbers.LONG_NULL}) {
                            final boolean expected = value >= -limit && value <= limit;
                            Assert.assertEquals(expected, owner.getBool(record(0, value)));
                            Assert.assertEquals(expected, worker.getBool(record(48, value)));
                        }
                        bindVariableService.setTimestampWithType(0, boundType, Numbers.LONG_NULL);
                        owner.init(null, sqlExecutionContext);
                        worker.init(null, sqlExecutionContext);
                        Assert.assertFalse(owner.getBool(record(0, 0)));
                        Assert.assertFalse(worker.getBool(record(48, 0)));
                    }
                }
            }
        });
    }

    @Test
    public void testInPointListDropsValuesTheColumnCannotHold() throws Exception {
        assertMemoryLeak(() -> {
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
            final OutputSchema original = wideSchema(ColumnType.TIMESTAMP_MICRO);
            final OutputSchema pruned = new OutputSchema().add(70, "ts", ColumnType.TIMESTAMP_MICRO, true);
            try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                final FunctionExpression expression = (FunctionExpression) binder.bindPredicate(call("in", literal("ts"),
                                text("1969-12-31T23:59:59.999999999Z"),
                                cast(text("1969-12-31T23:59:59.999999999Z"), "timestamp_ns"), constant("null")),
                        original, null, sqlExecutionContext);
                Assert.assertEquals(2, expression.getArgumentCount());
                Assert.assertEquals(ColumnType.NULL, expression.argumentAt(1).getDataType());
                try (Function owner = binder.instantiate(expression, pruned, sqlExecutionContext);
                     Function worker = binder.instantiate(expression, original, sqlExecutionContext)) {
                    binder.clear();
                    parser.clear();
                    original.clear();
                    pruned.clear();
                    for (long value : new long[]{-2, -1, 0, 1, Numbers.LONG_NULL}) {
                        final boolean expected = value == Numbers.LONG_NULL;
                        Assert.assertEquals(expected, owner.getBool(record(0, value)));
                        Assert.assertEquals(expected, worker.getBool(record(48, value)));
                    }
                }
            }
        });
    }

    @Test
    public void testInRuntimeIntervalWorkersReinitializeIndependently() throws Exception {
        assertMemoryLeak(() -> {
            for (int type : new int[]{ColumnType.TIMESTAMP_MICRO, ColumnType.TIMESTAMP_NANO}) {
                bindVariableService.clear();
                bindVariableService.setStr(0, "1970-01-01");
                final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
                final OutputSchema original = wideSchema(type);
                final OutputSchema pruned = new OutputSchema().add(70, "ts", type, true);
                try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                    final BoundExpression expression = binder.bindPredicate(call("in", literal("ts"), parameter("$1")),
                            original, null, sqlExecutionContext);
                    try (Function owner = binder.instantiate(expression, pruned, sqlExecutionContext);
                         Function worker = binder.instantiate(expression, original, sqlExecutionContext)) {
                        Assert.assertNotSame(owner, worker);
                        Assert.assertFalse(owner.isThreadSafe());
                        Assert.assertFalse(worker.isThreadSafe());
                        binder.clear();
                        parser.clear();
                        original.clear();
                        pruned.clear();
                        owner.init(null, sqlExecutionContext);
                        worker.init(null, sqlExecutionContext);
                        Assert.assertTrue(owner.getBool(record(0, 0)));
                        Assert.assertTrue(worker.getBool(record(48, 0)));
                        bindVariableService.setStr(0, "1970-01-02");
                        owner.init(null, sqlExecutionContext);
                        Assert.assertFalse(owner.getBool(record(0, 0)));
                        Assert.assertTrue(worker.getBool(record(48, 0)));
                        worker.init(null, sqlExecutionContext);
                        Assert.assertFalse(worker.getBool(record(48, 0)));
                        Assert.assertFalse(owner.getBool(record(0, Numbers.LONG_NULL)));
                    }
                }
            }
        });
    }

    @Test
    public void testInRuntimePointListsKeepIndependentCachedValues() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setTimestampNano(0, 1000);
            bindVariableService.setStr(1, "1969-12-31T23:59:59.999999999Z");
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
            final OutputSchema original = wideSchema(ColumnType.TIMESTAMP_MICRO);
            final OutputSchema pruned = new OutputSchema().add(70, "ts", ColumnType.TIMESTAMP_MICRO, true);
            try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                final BoundExpression expression = binder.bindPredicate(call("in", literal("ts"), parameter("$1"), parameter("$2"), constant("null")),
                        original, null, sqlExecutionContext);
                try (Function owner = binder.instantiate(expression, pruned, sqlExecutionContext);
                     Function worker = binder.instantiate(expression, original, sqlExecutionContext)) {
                    binder.clear();
                    parser.clear();
                    original.clear();
                    pruned.clear();
                    owner.init(null, sqlExecutionContext);
                    worker.init(null, sqlExecutionContext);
                    for (long value : new long[]{-1, 0, 1, Numbers.LONG_NULL}) {
                        final boolean expected = value == 1 || value == Numbers.LONG_NULL;
                        Assert.assertEquals(expected, owner.getBool(record(0, value)));
                        Assert.assertEquals(expected, worker.getBool(record(48, value)));
                    }
                    bindVariableService.setTimestampNano(0, 2000);
                    owner.init(null, sqlExecutionContext);
                    Assert.assertFalse(owner.getBool(record(0, 1)));
                    Assert.assertTrue(owner.getBool(record(0, 2)));
                    Assert.assertTrue(worker.getBool(record(48, 1)));
                    worker.init(null, sqlExecutionContext);
                    Assert.assertFalse(worker.getBool(record(48, 1)));
                    Assert.assertTrue(worker.getBool(record(48, 2)));
                }
            }
        });
    }

    @Test
    public void testNullBetweenClosesDiscardedNativeOperand() throws Exception {
        assertMemoryLeak(() -> {
            final long memoryBefore = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS);
            final boolean[] allocated = {false};
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache()) {
                @Override
                public Function createFunction(FunctionFactoryDescriptor overload, int position, CharSequence name,
                                               ObjList<Function> args, IntList positions, SqlExecutionContext context) throws SqlException {
                    final Function result = super.createFunction(overload, position, name, args, positions, context);
                    if (Chars.equals(name, "in")) {
                        allocated[0] = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS) > memoryBefore;
                    }
                    return result;
                }
            };
            final OutputSchema input = new OutputSchema().add(70, "id", ColumnType.LONG, true);
            try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                final ExpressionNode value = cast(cast(call("in", literal("id"), constant("1"), constant("2"), constant("3")), "long"), "timestamp");
                final BoundExpression expression = binder.bind(call("between", value, constant("null"), cast(constant("1"), "timestamp")),
                        input, null, sqlExecutionContext);
                Assert.assertTrue(allocated[0]);
                Assert.assertTrue(expression instanceof ConstantExpression);
                Assert.assertEquals(0, ((ConstantExpression) expression).getLongValue());
                Assert.assertEquals(memoryBefore, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS));
            }
        });
    }

    @Test
    public void testNullBetweenCleanupClosesEveryArgumentOnceAfterFailure() throws Exception {
        assertMemoryLeak(() -> {
            for (boolean timestampKey : new boolean[]{false, true}) {
                final int[] closes = new int[3];
                final RuntimeException first = new RuntimeException("first close");
                final RuntimeException second = new RuntimeException("second close");
                final ObjList<Function> args = new ObjList<>();
                args.add(timestampKey ? trackedTimestamp(0, false, closes, 0, first) : new LongFunction() {
                    @Override
                    public void close() {
                        closes[0]++;
                        throw first;
                    }

                    @Override
                    public long getLong(Record rec) {
                        return 0;
                    }
                });
                args.add(trackedTimestamp(Numbers.LONG_NULL, true, closes, 1, second));
                args.add(trackedTimestamp(1, true, closes, 2, null));
                final RuntimeException failure = Assert.assertThrows(RuntimeException.class,
                        () -> new BetweenTimestampFunctionFactory().newInstance(0, args, new IntList(), configuration, sqlExecutionContext));
                Assert.assertSame(first, failure);
                Assert.assertArrayEquals(new Throwable[]{second}, failure.getSuppressed());
                Assert.assertArrayEquals(new int[]{1, 1, 1}, closes);
                for (int i = 0; i < 3; i++) {
                    Assert.assertNull(args.getQuick(i));
                }
            }
        });
    }

    @Test
    public void testTextAndIntervalBoundsBindOverNanoTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            final OutputSchema input = new OutputSchema().add(70, "ts", ColumnType.TIMESTAMP_NANO, true);
            try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, new FunctionParser(configuration, engine.getFunctionFactoryCache()))) {
                for (ExpressionNode predicate : new ExpressionNode[]{call("in", literal("ts"), text("2020-01")),
                        call("between", literal("ts"), text("2020-01"), text("2020-02")),
                        call("in", literal("ts"), call("interval", cast(text("2020-01-01"), "timestamp"), cast(text("2020-01-02"), "timestamp")))}) {
                    Assert.assertEquals(ColumnType.BOOLEAN,
                            binder.bindPredicate(predicate, input, null, new IntHashSet(), sqlExecutionContext).getDataType());
                    binder.clear();
                }
            }
        });
    }

    private static ExpressionNode call(String name, ExpressionNode... args) {
        final int type = "between".equals(name) || "in".equals(name) && args.length == 2
                ? ExpressionNode.SET_OPERATION : ExpressionNode.FUNCTION;
        final ExpressionNode result = ExpressionNode.FACTORY.newInstance().of(type, name, 0, 0);
        result.paramCount = args.length;
        if (args.length > 2) {
            for (int i = args.length - 1; i >= 0; i--) {
                result.args.add(args[i]);
            }
        } else if (args.length == 2) {
            result.lhs = args[0];
            result.rhs = args[1];
        } else if (args.length == 1) {
            result.rhs = args[0];
        }
        return result;
    }

    private static ExpressionNode cast(ExpressionNode value, String type) {
        return call("cast", value, constant(type));
    }

    private static ExpressionNode constant(String token) {
        return ExpressionNode.FACTORY.newInstance().of(ExpressionNode.CONSTANT, token, 0, 0);
    }

    private static ExpressionNode literal(String token) {
        return ExpressionNode.FACTORY.newInstance().of(ExpressionNode.LITERAL, token, 0, 0);
    }

    private static ExpressionNode parameter(String token) {
        return ExpressionNode.FACTORY.newInstance().of(ExpressionNode.BIND_VARIABLE, token, 0, 0);
    }

    private static Record record(int expectedIndex, long value) {
        return new Record() {
            @Override
            public long getTimestamp(int columnIndex) {
                Assert.assertEquals(expectedIndex, columnIndex);
                return value;
            }
        };
    }

    private static ExpressionNode text(String value) {
        return constant("'" + value + "'");
    }

    private static Function trackedTimestamp(long value, boolean constant, int[] closes, int index, RuntimeException failure) {
        return new TimestampFunction(ColumnType.TIMESTAMP_MICRO) {
            @Override
            public void close() {
                closes[index]++;
                if (failure != null) {
                    throw failure;
                }
            }

            @Override
            public long getTimestamp(Record rec) {
                return value;
            }

            @Override
            public boolean isConstant() {
                return constant;
            }
        };
    }

    private static OutputSchema wideSchema(int type) {
        final OutputSchema schema = new OutputSchema();
        for (int i = 0; i < 48; i++) {
            schema.add(i, "unused" + i, ColumnType.INT, true);
        }
        schema.add(70, "ts", type, true);
        schema.setTimestampIndex(48);
        return schema;
    }
}
