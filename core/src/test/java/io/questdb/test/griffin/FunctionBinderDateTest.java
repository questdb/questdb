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
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class FunctionBinderDateTest extends AbstractCairoTest {
    @Test
    public void testNativeResolutionTruncationAdoptsArgumentAndRebuildsWithoutAst() throws Exception {
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
            original.add(70, "nt", ColumnType.TIMESTAMP_NANO, true);
            final OutputSchema firstLayout = new OutputSchema().add(70, "nt", ColumnType.TIMESTAMP_NANO, true);
            final OutputSchema secondLayout = new OutputSchema().add(80, "unused", ColumnType.INT, true)
                    .add(70, "nt", ColumnType.TIMESTAMP_NANO, true);
            try (FunctionBindingHarness binder = new FunctionBindingHarness(parser)) {
                final FunctionExpression expression = (FunctionExpression) binder.bind(
                        function("date_trunc", constant("'nanosecond'"), literal("nt")), original, null, sqlExecutionContext);
                Assert.assertEquals(1, constructions[0]);
                Assert.assertEquals(ColumnType.TIMESTAMP_NANO, expression.getDataType());
                TestUtils.assertEquals("date_trunc(sN)", expression.getSignature());
                try (Function first = binder.instantiate(expression, firstLayout, sqlExecutionContext);
                     Function second = binder.instantiate(expression, secondLayout, sqlExecutionContext)) {
                    Assert.assertSame(constructed.getQuick(0), first);
                    Assert.assertNotSame(first, second);
                    Assert.assertEquals(2, constructions[0]);
                    Assert.assertEquals(first.getType(), second.getType());
                    Assert.assertTrue(first.isThreadSafe());
                    binder.clear();
                    parser.clear();
                    original.clear();
                    firstLayout.clear();
                    secondLayout.clear();
                    Assert.assertEquals(123_456_789L, first.getTimestamp(timestampRecord(0, 123_456_789L)));
                    Assert.assertEquals(987_654_321L, second.getTimestamp(timestampRecord(1, 987_654_321L)));
                    Assert.assertEquals(Numbers.LONG_NULL, first.getTimestamp(timestampRecord(0, Numbers.LONG_NULL)));
                }
            }
        });
    }

    @Test
    public void testFormatterWorkersKeepSeparateBuffersAndFullPrecision() throws Exception {
        assertMemoryLeak(() -> {
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
            final OutputSchema original = new OutputSchema().add(1, "unused", ColumnType.INT, true)
                    .add(70, "nt", ColumnType.TIMESTAMP_NANO, true);
            final OutputSchema firstLayout = new OutputSchema().add(70, "nt", ColumnType.TIMESTAMP_NANO, true);
            final OutputSchema secondLayout = new OutputSchema().add(80, "unused", ColumnType.INT, true)
                    .add(70, "nt", ColumnType.TIMESTAMP_NANO, true);
            try (FunctionBindingHarness binder = new FunctionBindingHarness(parser)) {
                final FunctionExpression expression = (FunctionExpression) binder.bind(
                        function("to_str", literal("nt"), constant("'yyyy-MM-dd HH:mm:ss.SSSUUUNNN'")),
                        original, null, sqlExecutionContext);
                try (Function first = binder.instantiate(expression, firstLayout, sqlExecutionContext);
                     Function second = binder.instantiate(expression, secondLayout, sqlExecutionContext)) {
                    Assert.assertFalse(first.isThreadSafe());
                    Assert.assertFalse(second.isThreadSafe());
                    binder.clear();
                    parser.clear();
                    original.clear();
                    firstLayout.clear();
                    secondLayout.clear();
                    final CharSequence firstA = first.getStrA(timestampRecord(0, 123_456_789L));
                    final CharSequence firstB = first.getStrB(timestampRecord(0, 987_654_321L));
                    final CharSequence secondA = second.getStrA(timestampRecord(1, 1_000_000_001L));
                    TestUtils.assertEquals("1970-01-01 00:00:00.123456789", firstA);
                    TestUtils.assertEquals("1970-01-01 00:00:00.987654321", firstB);
                    TestUtils.assertEquals("1970-01-01 00:00:01.000000001", secondA);
                    Assert.assertNull(first.getStrA(timestampRecord(0, Numbers.LONG_NULL)));
                    Assert.assertEquals(-1, second.getStrLen(timestampRecord(1, Numbers.LONG_NULL)));
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
        node.paramCount = 2;
        return node;
    }

    private static ExpressionNode literal(String token) {
        return ExpressionNode.FACTORY.newInstance().of(ExpressionNode.LITERAL, token, 0, 0);
    }

    private static Record timestampRecord(int expectedIndex, long value) {
        return new Record() {
            @Override
            public long getTimestamp(int columnIndex) {
                Assert.assertEquals(expectedIndex, columnIndex);
                return value;
            }
        };
    }
}
