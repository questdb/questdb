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
import io.questdb.griffin.FunctionFactoryDescriptor;
import io.questdb.griffin.FunctionParser;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.ConstantExpression;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class FunctionBinderTemporalCastTest extends AbstractCairoTest {
    @Test
    public void testTimestampSubtractionRebuildsWithFullPrecisionAndNullSentinel() throws Exception {
        assertMemoryLeak(() -> {
            for (int type : new int[]{ColumnType.TIMESTAMP_MICRO, ColumnType.TIMESTAMP_NANO}) {
                final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
                final OutputSchema original = wideSchema(type);
                final OutputSchema pruned = new OutputSchema().add(70, "value", type, true);
                try (FunctionBinder binder = new FunctionBinder(parser)) {
                    final BoundExpression expression = binder.bind(binary("-", literal("value"), constant("1L")),
                            original, null, sqlExecutionContext);
                    try (Function owner = binder.instantiate(expression, pruned, sqlExecutionContext);
                         Function worker = binder.instantiate(expression, original, sqlExecutionContext)) {
                        Assert.assertNotSame(owner, worker);
                        binder.clear();
                        parser.clear();
                        original.clear();
                        pruned.clear();
                        Assert.assertEquals(type, owner.getType());
                        Assert.assertEquals(type, worker.getType());
                        for (long value : new long[]{0, -1, Numbers.LONG_NULL, Long.MIN_VALUE + 1, Long.MAX_VALUE}) {
                            final long expected = value == Numbers.LONG_NULL ? Numbers.LONG_NULL : value - 1;
                            Assert.assertEquals(expected, owner.getTimestamp(record(0, type, value)));
                            Assert.assertEquals(expected, worker.getTimestamp(record(48, type, value)));
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testIdentityTimestampCastAdmitsOnlyConstantRangePrecisionBounds() throws Exception {
        assertMemoryLeak(() -> {
            final OutputSchema input = new OutputSchema().add(70, "value", ColumnType.TIMESTAMP_MICRO, true);
            input.setTimestampIndex(0);
            try (FunctionBinder binder = new FunctionBinder(new FunctionParser(configuration, engine.getFunctionFactoryCache()))) {
                for (String operator : new String[]{"=", "<", "<=", ">", ">="}) {
                    final ExpressionNode bound = cast(constant("'1970-01-01T00:00:00.000000001Z'"), "timestamp_ns");
                    final FunctionExpression expression = (FunctionExpression) binder.bindPredicate(binary(operator,
                            cast(literal("value"), "timestamp"), bound), input, null, sqlExecutionContext);
                    Assert.assertFalse(((ColumnExpression) expression.argumentAt(0)).isDirectReference());
                    final ConstantExpression timestamp = (ConstantExpression) expression.argumentAt(1);
                    Assert.assertEquals(ColumnType.TIMESTAMP_NANO, timestamp.getDataType());
                    Assert.assertEquals(1, timestamp.getLongValue());
                    Assert.assertNull(timestamp.getTimestampText());
                    binder.clear();
                }
                for (int variant = 0; variant < 4; variant++) {
                    final ExpressionNode bound = variant == 3
                            ? ExpressionNode.FACTORY.newInstance().of(ExpressionNode.BIND_VARIABLE, "$1", 0, 0)
                            : cast(constant("'1970-01-01T00:00:00.000000001Z'"), "timestamp_ns");
                    ExpressionNode comparison = binary(variant == 1 ? "!=" : "<",
                            variant == 0 ? literal("value") : cast(literal("value"), "timestamp"), bound);
                    if (variant == 2) {
                        comparison = binary("or", comparison, constant("false"));
                    }
                    bindVariableService.setTimestampNano(0, 1);
                    Assert.assertEquals(ColumnType.BOOLEAN, binder.bindPredicate(comparison, input, null, sqlExecutionContext).getDataType());
                    binder.clear();
                }
            }
        });
    }

    @Test
    public void testDateToTimestampRetainsTargetPrecisionAcrossLayouts() throws Exception {
        assertMemoryLeak(() -> {
            for (int type : new int[]{ColumnType.TIMESTAMP_MICRO, ColumnType.TIMESTAMP_NANO}) {
                final ObjList<Function> constructed = new ObjList<>();
                final FunctionParser parser = parser(constructed);
                final OutputSchema original = wideSchema(ColumnType.DATE);
                final OutputSchema firstLayout = new OutputSchema().add(70, "value", ColumnType.DATE, true);
                final OutputSchema workerLayout = new OutputSchema().add(80, "unused", ColumnType.INT, true)
                        .add(70, "value", ColumnType.DATE, true);
                try (FunctionBinder binder = new FunctionBinder(parser)) {
                    final ExpressionNode node = cast(literal("value"), type == ColumnType.TIMESTAMP_MICRO ? "timestamp" : "timestamp_ns");
                    final FunctionExpression expression = (FunctionExpression) binder.bind(node, original, null, sqlExecutionContext);
                    Assert.assertEquals(type, expression.getDataType());
                    Assert.assertEquals(type, expression.argumentAt(1).getDataType());
                    TestUtils.assertEquals("cast(Mn)", expression.getSignature());
                    Assert.assertEquals(1, constructed.size());
                    node.clear();
                    try (Function owner = binder.instantiate(expression, firstLayout, sqlExecutionContext);
                         Function worker = binder.instantiate(expression, workerLayout, sqlExecutionContext)) {
                        Assert.assertSame(constructed.getQuick(0), owner);
                        Assert.assertNotSame(owner, worker);
                        Assert.assertEquals(2, constructed.size());
                        Assert.assertEquals(type, worker.getType());
                        binder.clear();
                        parser.clear();
                        original.clear();
                        firstLayout.clear();
                        workerLayout.clear();
                        final long scale = type == ColumnType.TIMESTAMP_MICRO ? 1_000 : 1_000_000;
                        Assert.assertEquals(123 * scale, owner.getTimestamp(record(0, ColumnType.DATE, 123)));
                        Assert.assertEquals(-123 * scale, worker.getTimestamp(record(1, ColumnType.DATE, -123)));
                        Assert.assertEquals(Numbers.LONG_NULL, owner.getTimestamp(record(0, ColumnType.DATE, Numbers.LONG_NULL)));
                        Assert.assertEquals(Numbers.LONG_NULL, worker.getTimestamp(record(1, ColumnType.DATE, Numbers.LONG_NULL)));
                    }
                }
            }
        });
    }

    @Test
    public void testTimestampPrecisionCastsRebuildAfterColumnPruning() throws Exception {
        assertMemoryLeak(() -> {
            for (int sourceType : new int[]{ColumnType.TIMESTAMP_MICRO, ColumnType.TIMESTAMP_NANO}) {
                for (int targetType : new int[]{ColumnType.TIMESTAMP_MICRO, ColumnType.TIMESTAMP_NANO}) {
                    final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
                    final OutputSchema original = wideSchema(sourceType);
                    final OutputSchema pruned = new OutputSchema().add(70, "value", sourceType, true);
                    try (FunctionBinder binder = new FunctionBinder(parser)) {
                        final BoundExpression expression = binder.bind(cast(literal("value"),
                                targetType == ColumnType.TIMESTAMP_MICRO ? "timestamp" : "timestamp_ns"), original, null, sqlExecutionContext);
                        try (Function owner = binder.instantiate(expression, pruned, sqlExecutionContext);
                             Function worker = binder.instantiate(expression, original, sqlExecutionContext)) {
                            binder.clear();
                            parser.clear();
                            original.clear();
                            pruned.clear();
                            Assert.assertEquals(targetType, owner.getType());
                            Assert.assertEquals(targetType, worker.getType());
                            for (long value : new long[]{123456789, -123456789, Numbers.LONG_NULL}) {
                                final long expected = ColumnType.getTimestampDriver(targetType).from(value, sourceType);
                                Assert.assertEquals(expected, owner.getTimestamp(record(0, sourceType, value)));
                                Assert.assertEquals(expected, worker.getTimestamp(record(48, sourceType, value)));
                            }
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testFailedIPv4ConstantClosesEarlierNativeArgument() throws Exception {
        assertMemoryLeak(() -> {
            final long memoryBefore = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS);
            final boolean[] hasNativeChild = {false};
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache()) {
                @Override
                public Function createFunction(FunctionFactoryDescriptor overload, int position, CharSequence name,
                                               ObjList<Function> args, IntList positions, SqlExecutionContext context) throws SqlException {
                    final Function function = super.createFunction(overload, position, name, args, positions, context);
                    if (Chars.equals(name, "in")) {
                        hasNativeChild[0] = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS) > memoryBefore;
                    }
                    return function;
                }
            };
            final OutputSchema input = new OutputSchema().add(70, "value", ColumnType.LONG, true);
            final ExpressionNode membership = ExpressionNode.FACTORY.newInstance().of(ExpressionNode.FUNCTION, "in", 0, 5);
            membership.args.add(constant("4"));
            membership.args.add(constant("3"));
            membership.args.add(constant("2"));
            membership.args.add(literal("value"));
            membership.paramCount = 4;
            final ExpressionNode bad = cast(constant("'not-an-ip'"), "ipv4");
            final ExpressionNode root = ExpressionNode.FACTORY.newInstance().of(ExpressionNode.FUNCTION, "concat", 0, 0);
            root.lhs = bad;
            root.rhs = cast(membership, "timestamp_ns");
            root.paramCount = 2;
            try (FunctionBinder binder = new FunctionBinder(parser)) {
                try {
                    binder.bind(root, input, null, sqlExecutionContext);
                    Assert.fail();
                } catch (SqlException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "invalid IPv4 constant");
                }
                Assert.assertTrue(hasNativeChild[0]);
                Assert.assertEquals(memoryBefore, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS));
                final BoundExpression recovered = binder.bind(cast(literal("value"), "date"), input, null, sqlExecutionContext);
                try (Function function = binder.instantiate(recovered, input, sqlExecutionContext)) {
                    Assert.assertEquals(123, function.getDate(record(0, ColumnType.LONG, 123)));
                }
            }
            Assert.assertEquals(memoryBefore, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS));
        });
    }

    @Test
    public void testTimestampAndIPv4FormattingRebuildsPrivateWorkerBuffers() throws Exception {
        assertMemoryLeak(() -> {
            final int[] types = {ColumnType.TIMESTAMP_MICRO, ColumnType.TIMESTAMP_NANO, ColumnType.IPv4};
            final ObjList<String> firstValues = new ObjList<>("1970-01-01T00:00:00.123456Z", "1970-01-01T00:00:00.000123456Z", "0.1.226.64");
            final ObjList<String> workerValues = new ObjList<>("1970-01-01T00:00:00.654321Z", "1970-01-01T00:00:00.000654321Z", "0.9.251.241");
            for (int i = 0; i < types.length; i++) {
                final int type = types[i];
                final ObjList<Function> constructed = new ObjList<>();
                final FunctionParser parser = parser(constructed);
                final OutputSchema original = wideSchema(type);
                final OutputSchema firstLayout = new OutputSchema().add(70, "value", type, true);
                final OutputSchema workerLayout = new OutputSchema().add(80, "unused", ColumnType.INT, true)
                        .add(70, "value", type, true);
                try (FunctionBinder binder = new FunctionBinder(parser)) {
                    final FunctionExpression expression = (FunctionExpression) binder.bind(cast(literal("value"), "varchar"), original, null, sqlExecutionContext);
                    Assert.assertEquals(type, expression.argumentAt(0).getDataType());
                    try (Function owner = binder.instantiate(expression, firstLayout, sqlExecutionContext);
                         Function worker = binder.instantiate(expression, workerLayout, sqlExecutionContext)) {
                        Assert.assertSame(constructed.getQuick(0), owner);
                        Assert.assertNotSame(owner, worker);
                        Assert.assertEquals(2, constructed.size());
                        Assert.assertFalse(owner.isThreadSafe());
                        Assert.assertFalse(worker.isThreadSafe());
                        binder.clear();
                        parser.clear();
                        original.clear();
                        firstLayout.clear();
                        workerLayout.clear();
                        final Utf8Sequence first = owner.getVarcharA(record(0, type, 123456));
                        final Utf8Sequence other = worker.getVarcharA(record(1, type, 654321));
                        owner.getVarcharB(record(0, type, 1));
                        TestUtils.assertEquals(firstValues.getQuick(i), Utf8s.toString(first));
                        TestUtils.assertEquals(workerValues.getQuick(i), Utf8s.toString(other));
                        final long nullValue = type == ColumnType.IPv4 ? Numbers.IPv4_NULL : Numbers.LONG_NULL;
                        Assert.assertNull(owner.getVarcharA(record(0, type, nullValue)));
                        TestUtils.assertEquals(workerValues.getQuick(i), Utf8s.toString(other));
                    }
                }
            }
        });
    }

    private static ExpressionNode binary(String name, ExpressionNode left, ExpressionNode right) {
        final ExpressionNode node = ExpressionNode.FACTORY.newInstance().of(ExpressionNode.FUNCTION, name, 0, 0);
        node.lhs = left;
        node.rhs = right;
        node.paramCount = 2;
        return node;
    }

    private static ExpressionNode cast(ExpressionNode value, String type) {
        return binary("cast", value, constant(type));
    }

    private static ExpressionNode constant(String value) {
        return ExpressionNode.FACTORY.newInstance().of(ExpressionNode.CONSTANT, value, 0, 0);
    }

    private static ExpressionNode literal(String value) {
        return ExpressionNode.FACTORY.newInstance().of(ExpressionNode.LITERAL, value, 0, 0);
    }

    private static Record record(int expectedIndex, int expectedType, long value) {
        return new Record() {
            @Override
            public long getDate(int columnIndex) {
                Assert.assertEquals(expectedIndex, columnIndex);
                Assert.assertEquals(ColumnType.DATE, expectedType);
                return value;
            }

            @Override
            public int getIPv4(int columnIndex) {
                Assert.assertEquals(expectedIndex, columnIndex);
                Assert.assertEquals(ColumnType.IPv4, expectedType);
                return (int) value;
            }

            @Override
            public long getLong(int columnIndex) {
                Assert.assertEquals(expectedIndex, columnIndex);
                Assert.assertEquals(ColumnType.LONG, expectedType);
                return value;
            }

            @Override
            public long getTimestamp(int columnIndex) {
                Assert.assertEquals(expectedIndex, columnIndex);
                Assert.assertTrue(ColumnType.isTimestamp(expectedType));
                return value;
            }
        };
    }

    private static OutputSchema wideSchema(int type) {
        final OutputSchema result = new OutputSchema();
        for (int i = 0; i < 48; i++) {
            result.add(i, "unused" + i, ColumnType.INT, true);
        }
        return result.add(70, "value", type, true);
    }

    private FunctionParser parser(ObjList<Function> constructed) {
        return new FunctionParser(configuration, engine.getFunctionFactoryCache()) {
            @Override
            public Function createFunction(FunctionFactoryDescriptor overload, int position, CharSequence name,
                                           ObjList<Function> args, IntList positions, SqlExecutionContext context) throws SqlException {
                final Function function = super.createFunction(overload, position, name, args, positions, context);
                constructed.add(function);
                return function;
            }
        };
    }
}
