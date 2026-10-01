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
import io.questdb.griffin.FunctionBinder;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.FunctionFactoryDescriptor;
import io.questdb.griffin.FunctionParser;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.griffin.engine.functions.date.ToTimezoneTimestampFunctionFactory;
import io.questdb.griffin.engine.functions.date.ToUTCTimestampFunctionFactory;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class FunctionBinderTimezoneTest extends AbstractCairoTest {
    @Test
    public void testConstantAndRowTimezonesSurviveColumnRelocation() throws Exception {
        assertMemoryLeak(() -> {
            for (int type : new int[]{ColumnType.TIMESTAMP_MICRO, ColumnType.TIMESTAMP_NANO}) {
                final long hour = type == ColumnType.TIMESTAMP_MICRO ? 3_600_000_000L : 3_600_000_000_000L;
                final long winter = ColumnType.getTimestampDriver(type).parseFloorLiteral("2024-01-15T12:00:00Z");
                final long summer = ColumnType.getTimestampDriver(type).parseFloorLiteral("2024-07-15T12:00:00Z");
                final OutputSchema original = new OutputSchema().add(1, "unused", ColumnType.INT, true)
                        .add(70, "ts", type, true).add(71, "zone", ColumnType.STRING, true);
                final OutputSchema pruned = new OutputSchema().add(70, "ts", type, true)
                        .add(71, "zone", ColumnType.STRING, true);
                final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
                try (FunctionBinder binder = new FunctionBinder(parser)) {
                    for (String name : new String[]{"to_utc", "to_timezone"}) {
                        final int direction = name.equals("to_utc") ? -1 : 1;
                        for (boolean columnZone : new boolean[]{false, true}) {
                            final BoundExpression expression = binder.bind(call(name, literal("ts"),
                                    columnZone ? literal("zone") : constant("'Europe/Berlin'")), original, null, sqlExecutionContext);
                            try (Function first = binder.instantiate(expression, pruned, sqlExecutionContext);
                                 Function second = binder.instantiate(expression, original, sqlExecutionContext)) {
                                Assert.assertEquals(type, first.getType());
                                Assert.assertNotSame(first, second);
                                binder.clear();
                                parser.clear();
                                first.init(null, sqlExecutionContext);
                                second.init(null, sqlExecutionContext);
                                Assert.assertEquals(winter + direction * hour, first.getTimestamp(record(0, winter, "Europe/Berlin")));
                                Assert.assertEquals(summer + direction * 2 * hour, second.getTimestamp(record(1, summer, "Europe/Berlin")));
                                if (columnZone) {
                                    Assert.assertEquals(winter + direction * 5 * hour / 2, first.getTimestamp(record(0, winter, "+02:30")));
                                    Assert.assertEquals(winter, first.getTimestamp(record(0, winter, "Invalid/Zone")));
                                    Assert.assertEquals(winter, first.getTimestamp(record(0, winter, null)));
                                }
                                first.cursorClosed();
                                second.cursorClosed();
                            }
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testRuntimeTimezoneRebindsAfterCompilerClose() throws Exception {
        assertMemoryLeak(() -> {
            for (int type : new int[]{ColumnType.TIMESTAMP_MICRO, ColumnType.TIMESTAMP_NANO}) {
                final long hour = type == ColumnType.TIMESTAMP_MICRO ? 3_600_000_000L : 3_600_000_000_000L;
                final long timestamp = ColumnType.getTimestampDriver(type).parseFloorLiteral("2024-07-15T12:00:00Z");
                for (String name : new String[]{"to_utc", "to_timezone"}) {
                    final int direction = name.equals("to_utc") ? -1 : 1;
                    final OutputSchema input = new OutputSchema().add(70, "ts", type, true);
                    final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
                    bindVariableService.setStr(0, "Europe/Berlin");
                    final Function retained;
                    final Function worker;
                    try (FunctionBinder binder = new FunctionBinder(parser)) {
                        final BoundExpression expression = binder.bind(call(name, literal("ts"),
                                ExpressionNode.FACTORY.newInstance().of(ExpressionNode.BIND_VARIABLE, "$1", 0, 1)), input, null, sqlExecutionContext);
                        retained = binder.instantiate(expression, input, sqlExecutionContext);
                        worker = binder.instantiate(expression, input, sqlExecutionContext);
                    }
                    try (retained; worker) {
                        parser.clear();
                        input.clear();
                        for (String zone : new String[]{"Europe/Berlin", "+02:30", "-03:00", "Europe/Berlin"}) {
                            bindVariableService.setStr(0, zone);
                            final long offset = zone.equals("Europe/Berlin") ? 2 * hour : zone.equals("+02:30") ? 5 * hour / 2 : -3 * hour;
                            retained.init(null, sqlExecutionContext);
                            worker.init(null, sqlExecutionContext);
                            Assert.assertEquals(timestamp + direction * offset, retained.getTimestamp(record(0, timestamp, null)));
                            Assert.assertEquals(timestamp + direction * offset, worker.getTimestamp(record(0, timestamp, null)));
                            retained.cursorClosed();
                            worker.cursorClosed();
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testConstantFoldingPreservesTimestampPrecision() throws Exception {
        assertMemoryLeak(() -> {
            final OutputSchema input = new OutputSchema();
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
            try (FunctionBinder binder = new FunctionBinder(parser)) {
                for (int type : new int[]{ColumnType.TIMESTAMP_MICRO, ColumnType.TIMESTAMP_NANO}) {
                    final long hour = type == ColumnType.TIMESTAMP_MICRO ? 3_600_000_000L : 3_600_000_000_000L;
                    for (String name : new String[]{"to_utc", "to_timezone"}) {
                        final ExpressionNode timestamp = call("cast", constant("123456789L"),
                                constant(type == ColumnType.TIMESTAMP_MICRO ? "timestamp" : "timestamp_ns"));
                        final BoundExpression expression = binder.bind(call(name, timestamp, constant("'+01:00'")), input, null, sqlExecutionContext);
                        try (Function result = binder.instantiate(expression, input, sqlExecutionContext)) {
                            Assert.assertTrue(result.isConstant());
                            Assert.assertEquals(type, result.getType());
                            Assert.assertEquals(123_456_789L + (name.equals("to_utc") ? -hour : hour), result.getTimestamp(null));
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testSelectedConstructionOwnsDiscardedTimezoneAndPreservesErrors() throws Exception {
        assertMemoryLeak(() -> {
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
            for (FunctionFactory factory : new FunctionFactory[]{new ToUTCTimestampFunctionFactory(), new ToTimezoneTimestampFunctionFactory()}) {
                final FunctionFactoryDescriptor descriptor = new FunctionFactoryDescriptor(factory);
                Assert.assertTrue(descriptor.isRelocatableScalar());
                for (String zone : new String[]{"Europe/Berlin", "+02:30", "Invalid/Zone", null}) {
                    for (boolean failClose : new boolean[]{false, true}) {
                        final CountingTimestamp timestamp = new CountingTimestamp();
                        final CountingZone timezone = new CountingZone(zone, failClose);
                        final IntList positions = new IntList();
                        positions.add(1);
                        positions.add(17);
                        final boolean valid = zone != null && !zone.equals("Invalid/Zone");
                        try (Function ignored = parser.createFunction(descriptor, 0, descriptor.getName(),
                                new ObjList<>(timestamp, timezone), positions, sqlExecutionContext)) {
                            Assert.assertTrue(valid && !failClose);
                            Assert.assertEquals(0, timestamp.closeCount);
                            Assert.assertEquals(1, timezone.closeCount);
                        } catch (SqlException e) {
                            if (valid) {
                                Assert.assertTrue(failClose);
                                TestUtils.assertContains(e.getFlyweightMessage(), "timezone close");
                            } else {
                                Assert.assertEquals(17, e.getPosition());
                                TestUtils.assertContains(e.getFlyweightMessage(), zone == null ? "timezone must not be null" : "invalid timezone: Invalid/Zone");
                                Assert.assertEquals(failClose ? 1 : 0, e.getSuppressed().length);
                            }
                        }
                        Assert.assertEquals(1, timestamp.closeCount);
                        Assert.assertEquals(1, timezone.closeCount);
                    }
                }
            }
        });
    }

    private static ExpressionNode call(String name, ExpressionNode left, ExpressionNode right) {
        final ExpressionNode node = ExpressionNode.FACTORY.newInstance().of(ExpressionNode.FUNCTION, name, 0, 0);
        node.lhs = left;
        node.rhs = right;
        node.paramCount = 2;
        return node;
    }

    private static ExpressionNode constant(String value) {
        return ExpressionNode.FACTORY.newInstance().of(ExpressionNode.CONSTANT, value, 0, 1);
    }

    private static ExpressionNode literal(String value) {
        return ExpressionNode.FACTORY.newInstance().of(ExpressionNode.LITERAL, value, 0, 1);
    }

    private static Record record(int timestampIndex, long timestamp, String timezone) {
        return new Record() {
            @Override
            public CharSequence getStrA(int columnIndex) {
                Assert.assertEquals(timestampIndex + 1, columnIndex);
                return timezone;
            }

            @Override
            public long getTimestamp(int columnIndex) {
                Assert.assertEquals(timestampIndex, columnIndex);
                return timestamp;
            }
        };
    }

    private static class CountingTimestamp extends TimestampFunction {
        private int closeCount;

        private CountingTimestamp() {
            super(ColumnType.TIMESTAMP_MICRO);
        }

        @Override
        public void close() {
            closeCount++;
        }

        @Override
        public long getTimestamp(Record rec) {
            throw new AssertionError("construction must not evaluate the timestamp");
        }
    }

    private static class CountingZone extends StrFunction {
        private final boolean failClose;
        private final String value;
        private int closeCount;

        private CountingZone(String value, boolean failClose) {
            this.value = value;
            this.failClose = failClose;
        }

        @Override
        public void close() {
            closeCount++;
            if (failClose) {
                throw new IllegalStateException("timezone close");
            }
        }

        @Override
        public CharSequence getStrA(Record rec) {
            return value;
        }

        @Override
        public CharSequence getStrB(Record rec) {
            return value;
        }

        @Override
        public boolean isConstant() {
            return true;
        }
    }
}
