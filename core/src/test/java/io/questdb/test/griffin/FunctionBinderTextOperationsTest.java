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
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactoryDescriptor;
import io.questdb.griffin.FunctionParser;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.plan.logical.ConstantExpression;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class FunctionBinderTextOperationsTest extends AbstractCairoTest {
    @Test
    public void testVarcharBindsUnconstructedAndBuildsIndependentNativeBuffers() throws Exception {
        assertMemoryLeak(() -> {
            final ObjList<Function> constructed = new ObjList<>();
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache()) {
                @Override
                public Function createFunction(FunctionFactoryDescriptor overload, int position, CharSequence name,
                                               ObjList<Function> args, IntList positions, SqlExecutionContext context) throws SqlException {
                    final Function result = super.createFunction(overload, position, name, args, positions, context);
                    constructed.add(result);
                    return result;
                }
            };
            final OutputSchema original = new OutputSchema();
            for (int i = 0; i < 48; i++) {
                original.add(i, "unused" + i, ColumnType.INT, true);
            }
            original.add(70, "v", ColumnType.VARCHAR, true);
            final OutputSchema firstLayout = new OutputSchema().add(70, "v", ColumnType.VARCHAR, true);
            final OutputSchema secondLayout = new OutputSchema().add(80, "unused", ColumnType.INT, true)
                    .add(70, "v", ColumnType.VARCHAR, true);
            try (FunctionBindingHarness binder = new FunctionBindingHarness(parser)) {
                final ExpressionNode source = unary("trim", literal("v"));
                final FunctionExpression expression = (FunctionExpression) binder.bind(source, original, null, sqlExecutionContext);
                Assert.assertEquals(0, constructed.size());
                Assert.assertEquals(ColumnType.VARCHAR, expression.getDataType());
                Assert.assertEquals("trim(Ø)", expression.getSignature());
                source.clear();
                try (Function first = binder.instantiate(expression, firstLayout, sqlExecutionContext);
                     Function second = binder.instantiate(expression, secondLayout, sqlExecutionContext)) {
                    Assert.assertSame(constructed.getQuick(0), first);
                    Assert.assertNotSame(first, second);
                    Assert.assertEquals(2, constructed.size());
                    Assert.assertFalse(first.isThreadSafe());
                    Assert.assertFalse(second.isThreadSafe());
                    binder.clear();
                    parser.clear();
                    original.clear();
                    firstLayout.clear();
                    secondLayout.clear();
                    final Utf8Sequence a = first.getVarcharA(record(0, "  hé中  "));
                    final Utf8Sequence b = first.getVarcharB(record(0, "  other  "));
                    final Utf8Sequence worker = second.getVarcharA(record(1, "  worker  "));
                    Assert.assertEquals("hé中", Utf8s.toString(a));
                    Assert.assertEquals("other", Utf8s.toString(b));
                    Assert.assertEquals("worker", Utf8s.toString(worker));
                    Assert.assertNull(first.getVarcharA(record(0, null)));
                    Assert.assertEquals(TableUtils.NULL_LEN, second.getVarcharSize(record(1, null)));
                }
            }
        });
    }

    @Test
    public void testVarcharConstantsOwnBytesAndDoNotUnquoteOnReconstruction() throws Exception {
        assertMemoryLeak(() -> {
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
            final OutputSchema input = new OutputSchema();
            final Utf8StringSink source = new Utf8StringSink();
            source.put("'hé中'");
            final ConstantExpression expression = new ConstantExpression().ofVarchar(source, 0);
            source.clear();
            source.put("changed");
            try (FunctionBindingHarness binder = new FunctionBindingHarness(parser);
                 Function first = binder.instantiate(expression, input, sqlExecutionContext);
                 Function second = binder.instantiate(expression, input, sqlExecutionContext)) {
                Assert.assertNotSame(first, second);
                Assert.assertEquals(ColumnType.VARCHAR, first.getType());
                Assert.assertTrue(first.isConstant());
                expression.clear();
                binder.clear();
                parser.clear();
                Assert.assertEquals("'hé中'", Utf8s.toString(first.getVarcharA(null)));
                Assert.assertEquals("'hé中'", second.getStrB(null).toString());
            }
            try (FunctionBindingHarness binder = new FunctionBindingHarness(parser);
                 Function value = binder.instantiate(new ConstantExpression().ofVarchar(null, 0), input, sqlExecutionContext)) {
                Assert.assertNull(value.getVarcharA(null));
                Assert.assertTrue(value.isNullConstant());
            }
        });
    }

    @Test
    public void testVarcharLeafKeepsByteLengthAndUtf16Getters() throws Exception {
        assertMemoryLeak(() -> {
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
            final OutputSchema input = new OutputSchema().add(4, "v", ColumnType.VARCHAR, true);
            try (FunctionBindingHarness binder = new FunctionBindingHarness(parser)) {
                final FunctionExpression bytes = (FunctionExpression) binder.bind(unary("length_bytes", literal("v")), input, null, sqlExecutionContext);
                final FunctionExpression lower = (FunctionExpression) binder.bind(unary("lower", literal("v")), input, null, sqlExecutionContext);
                try (Function length = binder.instantiate(bytes, input, sqlExecutionContext);
                     Function caseConverted = binder.instantiate(lower, input, sqlExecutionContext)) {
                    Assert.assertEquals(6, length.getInt(record(0, "Aé中")));
                    Assert.assertEquals(ColumnType.STRING, caseConverted.getType());
                    Assert.assertEquals("aé中", caseConverted.getStrA(record(0, "Aé中")).toString());
                    Assert.assertEquals(TableUtils.NULL_LEN, length.getInt(record(0, null)));
                }
            }
        });
    }

    private static ExpressionNode literal(String token) {
        return ExpressionNode.FACTORY.newInstance().of(ExpressionNode.LITERAL, token, 0, 0);
    }

    private static Record record(int expectedIndex, String value) {
        final Utf8String bytes = value != null ? new Utf8String(value) : null;
        return new Record() {
            @Override
            public Utf8Sequence getVarcharA(int columnIndex) {
                Assert.assertEquals(expectedIndex, columnIndex);
                return bytes;
            }

            @Override
            public Utf8Sequence getVarcharB(int columnIndex) {
                Assert.assertEquals(expectedIndex, columnIndex);
                return bytes;
            }

            @Override
            public int getVarcharSize(int columnIndex) {
                Assert.assertEquals(expectedIndex, columnIndex);
                return bytes != null ? bytes.size() : TableUtils.NULL_LEN;
            }
        };
    }

    private static ExpressionNode unary(String name, ExpressionNode argument) {
        final ExpressionNode node = ExpressionNode.FACTORY.newInstance().of(ExpressionNode.FUNCTION, name, 0, 0);
        node.rhs = argument;
        node.paramCount = 1;
        return node;
    }
}
