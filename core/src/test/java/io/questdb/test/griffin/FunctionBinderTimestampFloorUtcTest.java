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
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactoryDescriptor;
import io.questdb.griffin.FunctionParser;
import io.questdb.griffin.FunctionResolver;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.griffin.engine.functions.constants.StrConstant;
import io.questdb.griffin.engine.functions.constants.TimestampConstant;
import io.questdb.griffin.engine.functions.date.TimestampFloorFromOffsetUtcFunctionFactory;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.test.griffin.FunctionBindingHarness.record;

public class FunctionBinderTimestampFloorUtcTest extends AbstractCairoTest {
    @Test
    public void testConstantFoldingKeepsOriginAndFullPrecision() throws Exception {
        assertMemoryLeak(() -> {
            for (int type : new int[]{ColumnType.TIMESTAMP_MICRO, ColumnType.TIMESTAMP_NANO}) {
                final String typeName = type == ColumnType.TIMESTAMP_MICRO ? "timestamp" : "timestamp_ns";
                final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
                final OutputSchema empty = new OutputSchema();
                try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                    final BoundExpression expression = binder.bind(floor(constant("'1s'"),
                            cast(constant("123456789L"), typeName), cast(constant("123L"), typeName),
                            constant("null"), constant("null")), empty, null, sqlExecutionContext);
                    Assert.assertEquals(type, expression.getDataType());
                    final long scale = type == ColumnType.TIMESTAMP_MICRO ? 1_000_000L : 1_000_000_000L;
                    final long expected = (123_456_789L - 123) / scale * scale + 123;
                    try (Function first = binder.instantiate(expression, empty, sqlExecutionContext);
                         Function second = binder.instantiate(expression, empty, sqlExecutionContext)) {
                        Assert.assertTrue(first.isConstant());
                        Assert.assertEquals(type, first.getType());
                        Assert.assertEquals(expected, first.getTimestamp(null));
                        Assert.assertEquals(expected, second.getTimestamp(null));
                    }
                }
            }
        });
    }

    @Test
    public void testConstantTimezoneBranchesKeepUtcBucketsAndNulls() throws Exception {
        assertMemoryLeak(() -> {
            for (int type : new int[]{ColumnType.TIMESTAMP_MICRO, ColumnType.TIMESTAMP_NANO}) {
                assertFloor(type, "1d", "00:00", "+05:30", "2024-01-15T01:00:00Z", "2024-01-14T18:30:00Z");
                assertFloor(type, "1h", "00:00", "Europe/Berlin", "2021-10-31T00:30:00Z", "2021-10-31T00:00:00Z");
                assertFloor(type, "1h", "00:00", "Europe/Berlin", "2021-10-31T01:30:00Z", "2021-10-31T01:00:00Z");
                assertFloor(type, "1h", "00:00", "Pacific/Chatham", "2024-01-16T01:05:00Z", "2024-01-16T00:15:00Z");
            }
        });
    }

    @Test
    public void testFinalLayoutsBuildOnInstantiationAndSurviveCompilerClose() throws Exception {
        assertMemoryLeak(() -> {
            for (int type : new int[]{ColumnType.TIMESTAMP_MICRO, ColumnType.TIMESTAMP_NANO}) {
                final ObjList<Function> constructions = new ObjList<>();
                final FunctionParser parser = new FunctionParser(configuration, new FunctionResolver(configuration, engine.getFunctionFactoryCache()) {
                    @Override
                    public Function createFunction(FunctionFactoryDescriptor overload, int position, CharSequence name,
                                                   ObjList<Function> args, IntList positions, SqlExecutionContext context) throws SqlException {
                        final Function function = super.createFunction(overload, position, name, args, positions, context);
                        constructions.add(function);
                        return function;
                    }
                });
                final OutputSchema input = new OutputSchema().add(1, "unused", ColumnType.INT, true)
                        .add(70, "ts", type, true);
                final OutputSchema pruned = new OutputSchema().add(70, "renamed", type, true);
                Function first = null;
                Function second = null;
                try {
                    try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                        final FunctionExpression expression = (FunctionExpression) binder.bind(
                                floor(constant("'15m'"), literal("ts"), constant("null"),
                                        constant("'+00:05'"), constant("null")), input, null, sqlExecutionContext);
                        Assert.assertEquals(type, expression.getDataType());
                        TestUtils.assertEquals("timestamp_floor_utc(sNnSS)", expression.getSignature());
                        Assert.assertEquals(0, constructions.size());
                        first = binder.instantiate(expression, pruned, sqlExecutionContext);
                        second = binder.instantiate(expression, input, sqlExecutionContext);
                        Assert.assertSame(constructions.getQuick(0), first);
                        Assert.assertNotSame(first, second);
                        Assert.assertEquals(2, constructions.size());
                        binder.clear();
                    }
                    parser.clear();
                    input.clear();
                    pruned.clear();
                    final long secondScale = type == ColumnType.TIMESTAMP_MICRO ? 1_000_000L : 1_000_000_000L;
                    for (int pass = 0; pass < 2; pass++) {
                        first.init(null, sqlExecutionContext);
                        second.init(null, sqlExecutionContext);
                        Assert.assertEquals(4_800 * secondScale, first.getTimestamp(record(0, 4_921 * secondScale + 123)));
                        Assert.assertEquals(4_800 * secondScale, second.getTimestamp(record(1, 4_921 * secondScale + 123)));
                        Assert.assertEquals(Numbers.LONG_NULL, first.getTimestamp(record(0, Numbers.LONG_NULL)));
                        first.cursorClosed();
                        second.cursorClosed();
                    }
                } finally {
                    Misc.free(first);
                    Misc.free(second);
                }
            }
        });
    }

    @Test
    public void testInvalidTimezoneClosesArgumentsOnceAndKeepsPrimaryError() throws Exception {
        assertMemoryLeak(() -> {
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
            final FunctionFactoryDescriptor descriptor = new FunctionFactoryDescriptor(new TimestampFloorFromOffsetUtcFunctionFactory());
            Assert.assertTrue(descriptor.isRelocatableScalar());
            Assert.assertFalse(new FunctionFactoryDescriptor(new TimestampFloorFromOffsetUtcFunctionFactory() {
            }).isRelocatableScalar());
            for (boolean failClose : new boolean[]{false, true}) {
                final CountingTimestamp timestamp = new CountingTimestamp(failClose);
                final ObjList<Function> args = new ObjList<>(new StrConstant("1h"), timestamp,
                        TimestampConstant.TIMESTAMP_MICRO_NULL, new StrConstant("00:00"), new StrConstant("Invalid/Timezone"));
                final IntList positions = new IntList();
                for (int i = 0; i < args.size(); i++) {
                    positions.add(i + 1);
                }
                try {
                    parser.getFunctionResolver().createFunction(descriptor, 0, "timestamp_floor_utc", args, positions, sqlExecutionContext);
                    Assert.fail("invalid timezone accepted");
                } catch (SqlException e) {
                    Assert.assertEquals(5, e.getPosition());
                    TestUtils.assertContains(e.getFlyweightMessage(), "invalid timezone: Invalid/Timezone");
                    Assert.assertEquals(1, timestamp.closeCount);
                    Assert.assertEquals(failClose ? 1 : 0, e.getSuppressed().length);
                    if (failClose) {
                        Assert.assertEquals("close timestamp", e.getSuppressed()[0].getMessage());
                    }
                }
            }
        });
    }

    @Test
    public void testRuntimeOffsetAndTimezoneRebindIndependentFunctions() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setStr("offset", "00:00");
            bindVariableService.setStr("tz", "Europe/Berlin");
            final OutputSchema input = new OutputSchema().add(70, "ts", ColumnType.TIMESTAMP_NANO, true);
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
            try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                final FunctionExpression expression = (FunctionExpression) binder.bind(
                        floor(constant("'1h'"), literal("ts"), constant("null"), variable(":offset"), variable(":tz")),
                        input, null, sqlExecutionContext);
                try (Function first = binder.instantiate(expression, input, sqlExecutionContext);
                     Function second = binder.instantiate(expression, input, sqlExecutionContext)) {
                    binder.clear();
                    parser.clear();
                    input.clear();
                    final TimestampDriver driver = ColumnType.getTimestampDriver(ColumnType.TIMESTAMP_NANO);
                    final long value = driver.parseFloorLiteral("2024-01-15T01:40:00Z");
                    first.init(null, sqlExecutionContext);
                    second.init(null, sqlExecutionContext);
                    final long expected = driver.parseFloorLiteral("2024-01-15T01:00:00Z");
                    Assert.assertEquals(expected, first.getTimestamp(record(0, value)));
                    Assert.assertEquals(expected, second.getTimestamp(record(0, value)));
                    first.cursorClosed();
                    second.cursorClosed();
                    bindVariableService.setStr("offset", "+00:15");
                    bindVariableService.setStr("tz", "+05:30");
                    first.init(null, sqlExecutionContext);
                    second.init(null, sqlExecutionContext);
                    final long rebound = driver.parseFloorLiteral("2024-01-15T00:45:00Z");
                    Assert.assertEquals(rebound, first.getTimestamp(record(0, value)));
                    Assert.assertEquals(rebound, second.getTimestamp(record(0, value)));
                }
            }
        });
    }

    private static ExpressionNode cast(ExpressionNode value, String type) {
        final ExpressionNode node = ExpressionNode.FACTORY.newInstance().of(ExpressionNode.FUNCTION, "cast", 0, 0);
        node.lhs = value;
        node.rhs = constant(type);
        node.paramCount = 2;
        return node;
    }

    private static ExpressionNode constant(String value) {
        return ExpressionNode.FACTORY.newInstance().of(ExpressionNode.CONSTANT, value, 0, 1);
    }

    private static ExpressionNode floor(ExpressionNode unit, ExpressionNode timestamp, ExpressionNode origin,
                                        ExpressionNode offset, ExpressionNode timezone) {
        final ExpressionNode node = ExpressionNode.FACTORY.newInstance().of(ExpressionNode.FUNCTION, "timestamp_floor_utc", 0, 0);
        node.paramCount = 5;
        node.args.add(timezone);
        node.args.add(offset);
        node.args.add(origin);
        node.args.add(timestamp);
        node.args.add(unit);
        return node;
    }

    private static ExpressionNode literal(String value) {
        return ExpressionNode.FACTORY.newInstance().of(ExpressionNode.LITERAL, value, 0, 1);
    }

    private static ExpressionNode variable(String value) {
        return ExpressionNode.FACTORY.newInstance().of(ExpressionNode.BIND_VARIABLE, value, 0, 1);
    }

    private void assertFloor(int type, String unit, String offset, String timezone, String value, String expected) throws Exception {
        final TimestampDriver driver = ColumnType.getTimestampDriver(type);
        final OutputSchema input = new OutputSchema().add(70, "ts", type, true);
        final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
        try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
            final FunctionExpression expression = (FunctionExpression) binder.bind(
                    floor(constant("'" + unit + "'"), literal("ts"), constant("null"),
                            constant("'" + offset + "'"), constant("'" + timezone + "'")), input, null, sqlExecutionContext);
            try (Function first = binder.instantiate(expression, input, sqlExecutionContext);
                 Function second = binder.instantiate(expression, input, sqlExecutionContext)) {
                Assert.assertEquals(type, first.getType());
                first.init(null, sqlExecutionContext);
                second.init(null, sqlExecutionContext);
                final long timestamp = driver.parseFloorLiteral(value);
                final long result = driver.parseFloorLiteral(expected);
                Assert.assertEquals(result, first.getTimestamp(record(0, timestamp)));
                Assert.assertEquals(result, second.getTimestamp(record(0, timestamp)));
                Assert.assertEquals(Numbers.LONG_NULL, first.getTimestamp(record(0, Numbers.LONG_NULL)));
            }
        }
    }

    private static class CountingTimestamp extends TimestampFunction {
        private final boolean failClose;
        private int closeCount;

        private CountingTimestamp(boolean failClose) {
            super(ColumnType.TIMESTAMP_MICRO);
            this.failClose = failClose;
        }

        @Override
        public void close() {
            closeCount++;
            if (failClose) {
                throw new IllegalStateException("close timestamp");
            }
        }

        @Override
        public long getTimestamp(Record rec) {
            throw new AssertionError("must not evaluate timestamp argument");
        }
    }
}
