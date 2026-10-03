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
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.FunctionFactoryDescriptor;
import io.questdb.griffin.FunctionParser;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.constants.ConstantFunction;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.plan.logical.BoundExpression;
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
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class FunctionBinderPrimitiveCastTest extends AbstractCairoTest {
    @Test
    public void testConstantFoldingAndFailedParentReleaseNativeChildren() throws Exception {
        assertMemoryLeak(() -> {
            final long memoryBefore = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS);
            final boolean[] hasNativeChild = {false};
            final int[] foldedCastCalls = {0};
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache()) {
                @Override
                public Function createFunction(FunctionFactoryDescriptor overload, int position, CharSequence name,
                                               ObjList<Function> args, IntList positions, SqlExecutionContext context) throws SqlException {
                    if (Chars.equals(name, "cast") && args.getQuick(0).getType() == ColumnType.BOOLEAN
                            && args.getQuick(1).getType() == ColumnType.STRING) {
                        Assert.assertTrue(args.getQuick(0) instanceof ConstantFunction);
                        Assert.assertEquals(memoryBefore, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS));
                        foldedCastCalls[0]++;
                    }
                    final Function result = super.createFunction(overload, position, name, args, positions, context);
                    if (Chars.equals(name, "in")) {
                        hasNativeChild[0] |= Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS) > memoryBefore;
                    }
                    return result;
                }
            };
            final OutputSchema input = new OutputSchema().add(7, "id", ColumnType.LONG, true);
            try (FunctionBindingHarness binder = new FunctionBindingHarness(parser)) {
                final BoundExpression expression = binder.bind(cast(in(constant("2")), "string"), input, null, sqlExecutionContext);
                Assert.assertTrue(hasNativeChild[0]);
                Assert.assertEquals(1, foldedCastCalls[0]);
                Assert.assertTrue(expression instanceof ConstantExpression);
                Assert.assertEquals(ColumnType.STRING, expression.getDataType());
                try (Function first = binder.instantiate(expression, input, sqlExecutionContext);
                     Function worker = binder.instantiate(expression, input, sqlExecutionContext)) {
                    TestUtils.assertEquals("true", first.getStrA(null));
                    TestUtils.assertEquals("true", worker.getStrA(null));
                }
                binder.clear();
                hasNativeChild[0] = false;
                bindVariableService.clear();
                bindVariableService.setLong(0, 2);
                try {
                    binder.bind(cast(in(parameter("$1")), "uuid"), input, null, sqlExecutionContext);
                    Assert.fail("BOOLEAN to UUID cast must be rejected");
                } catch (SqlException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "no matching function");
                }
                Assert.assertTrue(hasNativeChild[0]);
                Assert.assertEquals(memoryBefore, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS));
                final BoundExpression recovered = binder.bind(cast(literal("id"), "byte"), input, null, sqlExecutionContext);
                try (Function function = binder.instantiate(recovered, input, sqlExecutionContext)) {
                    Assert.assertEquals((byte) 130, function.getByte(longRecord(0, 130)));
                }
            }
            Assert.assertEquals(memoryBefore, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS));
        });
    }

    @Test
    public void testSameTypeCastOfUnconstructedCallFeedsConstructedParent() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (l LONG, d DOUBLE)");
            execute("INSERT INTO t VALUES (2, 5.0), (3, 5.0)");
            assertQuery("SELECT d >= (l * l)::LONG ge, (l * l)::LONG + 0 > 4 gt FROM t").expectSize().returns("""
                    ge	gt
                    true	false
                    false	true
                    """);
        });
    }

    @Test
    public void testSymbolNumericCastsKeepDictionaryCapabilitiesAndIndependentWorkers() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE fb_cast_symbol(unused INT,s SYMBOL)");
            execute("INSERT INTO fb_cast_symbol VALUES(1,'17'),(2,'bad'),(3,null),(4,'1_000')");
            final int[] expected = {17, Numbers.INT_NULL, Numbers.INT_NULL, 1000};
            sqlExecutionContext.setCloneSymbolTables(true);
            try {
                for (boolean isDynamic : new boolean[]{false, true}) {
                    final ObjList<Function> constructed = new ObjList<>();
                    final FunctionParser parser = parser(constructed);
                    try (RecordCursorFactory original = select("SELECT unused,s FROM fb_cast_symbol");
                         RecordCursorFactory narrowed = select(isDynamic
                                 ? "SELECT s FROM fb_cast_symbol UNION ALL SELECT s FROM fb_cast_symbol"
                                 : "SELECT s FROM fb_cast_symbol");
                         FunctionBindingHarness binder = new FunctionBindingHarness(parser)) {
                        final OutputSchema full = schema(original.getMetadata(), 100);
                        final OutputSchema pruned = schema(narrowed.getMetadata(), 101);
                        final FunctionExpression expression = (FunctionExpression) binder.bind(
                                cast(literal("s"), "int"), full, null, sqlExecutionContext);
                        TestUtils.assertEquals("cast(Ki)", expression.getSignature());
                        try (Function owner = binder.instantiate(expression, pruned, narrowed.getMetadata(), sqlExecutionContext);
                             Function worker = binder.instantiate(expression, full, original.getMetadata(), sqlExecutionContext)) {
                            Assert.assertEquals(2, constructed.size());
                            Assert.assertNotSame(owner, worker);
                            Assert.assertSame(constructed.getQuick(0), owner);
                            binder.clear();
                            parser.clear();
                            for (int pass = 0; pass < 2; pass++) {
                                int row = 0;
                                try (RecordCursor cursor = narrowed.getCursor(sqlExecutionContext)) {
                                    owner.init(cursor, sqlExecutionContext);
                                    while (cursor.hasNext()) {
                                        Assert.assertEquals(expected[row++ % expected.length], owner.getInt(cursor.getRecord()));
                                    }
                                    Assert.assertEquals(isDynamic ? 8 : 4, row);
                                }
                                owner.cursorClosed();
                                row = 0;
                                try (RecordCursor cursor = original.getCursor(sqlExecutionContext)) {
                                    worker.init(cursor, sqlExecutionContext);
                                    while (cursor.hasNext()) {
                                        Assert.assertEquals(expected[row++], worker.getInt(cursor.getRecord()));
                                    }
                                    Assert.assertEquals(4, row);
                                }
                                worker.cursorClosed();
                            }
                        }
                    }
                }
            } finally {
                sqlExecutionContext.setCloneSymbolTables(false);
            }
        });
    }

    @Test
    public void testTextCastsBindUnconstructedAndBuildPrivateBuffersAfterPruning() throws Exception {
        assertMemoryLeak(() -> {
            for (int type : new int[]{ColumnType.STRING, ColumnType.VARCHAR}) {
                final ObjList<Function> constructed = new ObjList<>();
                final FunctionParser parser = parser(constructed);
                final OutputSchema original = wideSchema(ColumnType.LONG);
                final OutputSchema firstLayout = new OutputSchema().add(70, "value", ColumnType.LONG, true);
                final OutputSchema secondLayout = new OutputSchema().add(80, "unused", ColumnType.INT, true)
                        .add(70, "value", ColumnType.LONG, true);
                try (FunctionBindingHarness binder = new FunctionBindingHarness(parser)) {
                    final ExpressionNode source = cast(literal("value"), type == ColumnType.STRING ? "string" : "varchar");
                    final FunctionExpression expression = (FunctionExpression) binder.bind(source, original, null, sqlExecutionContext);
                    Assert.assertEquals(type, expression.getDataType());
                    TestUtils.assertEquals(type == ColumnType.STRING ? "cast(Ls)" : "cast(Lø)", expression.getSignature());
                    Assert.assertEquals(0, constructed.size());
                    source.clear();
                    try (Function owner = binder.instantiate(expression, firstLayout, sqlExecutionContext);
                         Function worker = binder.instantiate(expression, secondLayout, sqlExecutionContext)) {
                        Assert.assertSame(constructed.getQuick(0), owner);
                        Assert.assertNotSame(owner, worker);
                        Assert.assertEquals(2, constructed.size());
                        Assert.assertFalse(owner.isThreadSafe());
                        Assert.assertFalse(worker.isThreadSafe());
                        binder.clear();
                        parser.clear();
                        original.clear();
                        firstLayout.clear();
                        secondLayout.clear();
                        if (type == ColumnType.STRING) {
                            final CharSequence a = owner.getStrA(longRecord(0, 123));
                            final CharSequence b = owner.getStrB(longRecord(0, 456));
                            final CharSequence other = worker.getStrA(longRecord(1, 789));
                            TestUtils.assertEquals("123", a);
                            TestUtils.assertEquals("456", b);
                            TestUtils.assertEquals("789", other);
                            Assert.assertNull(owner.getStrA(longRecord(0, Numbers.LONG_NULL)));
                        } else {
                            final Utf8Sequence a = owner.getVarcharA(longRecord(0, 123));
                            final Utf8Sequence b = owner.getVarcharB(longRecord(0, 456));
                            final Utf8Sequence other = worker.getVarcharA(longRecord(1, 789));
                            Assert.assertEquals("123", Utf8s.toString(a));
                            Assert.assertEquals("456", Utf8s.toString(b));
                            Assert.assertEquals("789", Utf8s.toString(other));
                            Assert.assertNull(owner.getVarcharA(longRecord(0, Numbers.LONG_NULL)));
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testVarcharToCharRetainsUnicodeNullAndWorkerState() throws Exception {
        assertMemoryLeak(() -> {
            final ObjList<Function> constructed = new ObjList<>();
            final FunctionParser parser = parser(constructed);
            final OutputSchema original = wideSchema(ColumnType.VARCHAR);
            final OutputSchema firstLayout = new OutputSchema().add(70, "value", ColumnType.VARCHAR, true);
            final OutputSchema secondLayout = new OutputSchema().add(80, "unused", ColumnType.INT, true)
                    .add(70, "value", ColumnType.VARCHAR, true);
            try (FunctionBindingHarness binder = new FunctionBindingHarness(parser)) {
                final FunctionExpression expression = (FunctionExpression) binder.bind(
                        cast(literal("value"), "char"), original, null, sqlExecutionContext);
                TestUtils.assertEquals("cast(Øa)", expression.getSignature());
                try (Function owner = binder.instantiate(expression, firstLayout, sqlExecutionContext);
                     Function worker = binder.instantiate(expression, secondLayout, sqlExecutionContext)) {
                    Assert.assertSame(constructed.getQuick(0), owner);
                    Assert.assertNotSame(owner, worker);
                    Assert.assertEquals(2, constructed.size());
                    Assert.assertFalse(owner.isThreadSafe());
                    Assert.assertFalse(worker.isThreadSafe());
                    binder.clear();
                    parser.clear();
                    Assert.assertEquals('é', owner.getChar(varcharRecord(0, "éclair")));
                    Assert.assertEquals('中', worker.getChar(varcharRecord(1, "中文")));
                    Assert.assertEquals('x', owner.getChar(varcharRecord(0, "x")));
                    Assert.assertEquals(0, owner.getChar(varcharRecord(0, "")));
                    Assert.assertEquals(0, worker.getChar(varcharRecord(1, null)));
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
        return ExpressionNode.FACTORY.newInstance().of(ExpressionNode.CONSTANT, value, 0, 0);
    }

    private static ExpressionNode in(ExpressionNode value) {
        final ExpressionNode node = ExpressionNode.FACTORY.newInstance().of(ExpressionNode.FUNCTION, "in", 0, 0);
        node.args.add(constant("4"));
        node.args.add(constant("3"));
        node.args.add(constant("2"));
        node.args.add(value);
        node.paramCount = 4;
        return node;
    }

    private static ExpressionNode literal(String value) {
        return ExpressionNode.FACTORY.newInstance().of(ExpressionNode.LITERAL, value, 0, 0);
    }

    private static ExpressionNode parameter(String token) {
        return ExpressionNode.FACTORY.newInstance().of(ExpressionNode.BIND_VARIABLE, token, 0, 0);
    }

    private static Record longRecord(int expectedIndex, long value) {
        return new Record() {
            @Override
            public long getLong(int columnIndex) {
                Assert.assertEquals(expectedIndex, columnIndex);
                return value;
            }
        };
    }

    private static OutputSchema schema(RecordMetadata metadata, int firstId) {
        final OutputSchema result = new OutputSchema();
        for (int i = 0; i < metadata.getColumnCount(); i++) {
            result.add(firstId + i, metadata.getColumnName(i), metadata.getColumnType(i), true);
            result.setSymbolTableStatic(i, metadata.isSymbolTableStatic(i));
        }
        return result;
    }

    private static Record varcharRecord(int expectedIndex, String value) {
        final Utf8String bytes = value == null ? null : new Utf8String(value);
        return new Record() {
            @Override
            public Utf8Sequence getVarcharA(int columnIndex) {
                Assert.assertEquals(expectedIndex, columnIndex);
                return bytes;
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
