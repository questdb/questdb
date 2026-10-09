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
import io.questdb.griffin.FunctionResolver;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.constants.SymbolConstant;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class FunctionBinderSymbolTest extends AbstractCairoTest {
    @Test
    public void testAdoptedStatelessLeavesRemainReadableAfterCloseAndInit() throws Exception {
        assertMemoryLeak(() -> {
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
            final OutputSchema input = new OutputSchema().add(1, "i", ColumnType.INT, true)
                    .add(2, "s", ColumnType.STRING, true).add(3, "v", ColumnType.VARCHAR, true);
            final Utf8String text = new Utf8String("中");
            final Record record = new Record() {
                @Override
                public int getInt(int index) {
                    Assert.assertEquals(0, index);
                    return 42;
                }

                @Override
                public CharSequence getStrA(int index) {
                    Assert.assertEquals(1, index);
                    return "text";
                }

                @Override
                public Utf8Sequence getVarcharA(int index) {
                    Assert.assertEquals(2, index);
                    return text;
                }
            };
            try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser);
                 Function integer = binder.instantiate(binder.bind(literal("i"), input, null, sqlExecutionContext), input);
                 Function string = binder.instantiate(binder.bind(literal("s"), input, null, sqlExecutionContext), input);
                 Function varchar = binder.instantiate(binder.bind(literal("v"), input, null, sqlExecutionContext), input)) {
                binder.clear();
                integer.close();
                string.close();
                varchar.close();
                integer.init(null, sqlExecutionContext);
                string.init(null, sqlExecutionContext);
                varchar.init(null, sqlExecutionContext);
                Assert.assertEquals(42, integer.getInt(record));
                Assert.assertEquals("text", string.getStrA(record));
                Assert.assertEquals("中", varchar.getVarcharA(record).toString());
            }
        });
    }

    @Test
    public void testFinalDynamicDictionaryBuildsSelectedClosure() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE fb_symbol(s SYMBOL,t SYMBOL)");
            execute("INSERT INTO fb_symbol VALUES('alpha','alpha'),('beta','gamma'),(null,null)");
            final ObjList<Function> constructed = new ObjList<>();
            final FunctionParser parser = parser(constructed);
            try (RecordCursorFactory dynamic = select("SELECT s,t FROM fb_symbol UNION ALL SELECT t,s FROM fb_symbol");
                 FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                Assert.assertFalse(dynamic.getMetadata().isSymbolTableStatic(0));
                final OutputSchema boundInput = schema(dynamic.getMetadata(), 100);
                boundInput.setSymbolTableStatic(0, true);
                boundInput.setSymbolTableStatic(1, true);
                final BoundExpression expression = binder.bind(binary("=", literal("s"), literal("t")), boundInput, null, sqlExecutionContext);
                try (Function actual = binder.instantiate(expression, boundInput, dynamic.getMetadata(), sqlExecutionContext)) {
                    Assert.assertEquals(1, constructed.size());
                    Assert.assertSame(constructed.getQuick(0), actual);
                    binder.clear();
                    parser.clear();
                    for (int pass = 0; pass < 2; pass++) {
                        try (RecordCursor cursor = dynamic.getCursor(sqlExecutionContext)) {
                            actual.init(cursor, sqlExecutionContext);
                            while (cursor.hasNext()) {
                                final Record record = cursor.getRecord();
                                Assert.assertEquals(symbolsEqual(record), actual.getBool(record));
                            }
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testSchemaCopiesAndDecodedSymbolConstant() {
        final OutputSchema original = new OutputSchema().add(7, "s", ColumnType.SYMBOL, false);
        original.setSymbolTableStatic(0, true);
        final OutputSchema copy = new OutputSchema();
        copy.copyFrom(original);
        original.clear();
        Assert.assertTrue(copy.isSymbolTableStatic(0));
        Assert.assertFalse(copy.isVisible(0));
        copy.setSymbolTableStatic(0, false);
        Assert.assertFalse(copy.isSymbolTableStatic(0));
        Assert.assertEquals("'alpha'", SymbolConstant.fromValue("'alpha'").getSymbol(null));
        copy.clear();
        copy.add(8, "next", ColumnType.SYMBOL, true);
        Assert.assertFalse(copy.isSymbolTableStatic(0));
        Assert.assertTrue(copy.isVisible(0));
    }

    @Test
    public void testStaticDictionaryWorkersAndCompilerLifetime() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE fb_symbol(unused INT,s SYMBOL,t SYMBOL)");
            execute("INSERT INTO fb_symbol VALUES(1,'alpha','alpha'),(2,'beta','gamma'),(3,null,null)");
            final ObjList<Function> constructed = new ObjList<>();
            final FunctionParser parser = parser(constructed);
            try (RecordCursorFactory original = select("SELECT unused,s,t FROM fb_symbol");
                 RecordCursorFactory narrowed = select("SELECT s,t FROM fb_symbol");
                 FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                final OutputSchema full = schema(original.getMetadata(), 100);
                final OutputSchema pruned = schema(narrowed.getMetadata(), 101);
                final BoundExpression expression = binder.bind(binary("=", literal("s"), literal("t")), full, null, sqlExecutionContext);
                Assert.assertEquals(0, constructed.size());
                try (Function owner = binder.instantiate(expression, pruned, narrowed.getMetadata(), sqlExecutionContext);
                     Function worker = binder.instantiate(expression, full, original.getMetadata(), sqlExecutionContext)) {
                    Assert.assertSame(constructed.getQuick(0), owner);
                    Assert.assertNotSame(owner, worker);
                    Assert.assertEquals(2, constructed.size());
                    Assert.assertFalse(owner.isThreadSafe());
                    binder.clear();
                    parser.clear();
                    full.clear();
                    pruned.clear();
                    for (int pass = 0; pass < 2; pass++) {
                        try (RecordCursor left = narrowed.getCursor(sqlExecutionContext);
                             RecordCursor right = original.getCursor(sqlExecutionContext)) {
                            owner.init(left, sqlExecutionContext);
                            worker.init(right, sqlExecutionContext);
                            while (left.hasNext()) {
                                Assert.assertTrue(right.hasNext());
                                final Record record = left.getRecord();
                                final boolean expected = symbolsEqual(record);
                                Assert.assertEquals(expected, owner.getBool(record));
                                Assert.assertEquals(expected, worker.getBool(right.getRecord()));
                            }
                            Assert.assertFalse(right.hasNext());
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testSymbolSwitchClosesWorkerDictionariesAndRebuildsChangedCapability() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE fb_symbol(unused INT,s SYMBOL)");
            execute("INSERT INTO fb_symbol VALUES(1,'alpha'),(2,'beta'),(3,null)");
            sqlExecutionContext.setCloneSymbolTables(true);
            try {
                for (boolean isDynamic : new boolean[]{false, true}) {
                    final ObjList<Function> constructed = new ObjList<>();
                    final FunctionParser parser = parser(constructed);
                    try (RecordCursorFactory original = select("SELECT unused,s FROM fb_symbol");
                         RecordCursorFactory narrowed = select(isDynamic
                                 ? "SELECT s FROM fb_symbol UNION ALL SELECT s FROM fb_symbol"
                                 : "SELECT s FROM fb_symbol");
                         FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                        final OutputSchema full = schema(original.getMetadata(), 100);
                        final OutputSchema pruned = schema(narrowed.getMetadata(), 101);
                        final ObjList<ExpressionNode> args = new ObjList<>(literal("s"), constant("'alpha'"), constant("10"), constant("-1"));
                        final BoundExpression expression = binder.bind(call("switch", args), full, null, sqlExecutionContext);
                        try (Function owner = binder.instantiate(expression, pruned, narrowed.getMetadata(), sqlExecutionContext);
                             Function worker = binder.instantiate(expression, full, original.getMetadata(), sqlExecutionContext)) {
                            Assert.assertEquals(isDynamic ? 3 : 2, constructed.size());
                            Assert.assertNotSame(owner, worker);
                            if (isDynamic) {
                                Assert.assertNotSame(constructed.getQuick(0), owner);
                            } else {
                                Assert.assertSame(constructed.getQuick(0), owner);
                            }
                            Assert.assertFalse(owner.isThreadSafe());
                            Assert.assertFalse(worker.isThreadSafe());
                            binder.clear();
                            parser.clear();
                            for (int pass = 0; pass < 2; pass++) {
                                try (RecordCursor cursor = narrowed.getCursor(sqlExecutionContext)) {
                                    owner.init(cursor, sqlExecutionContext);
                                    while (cursor.hasNext()) {
                                        Assert.assertEquals(Chars.equalsNc("alpha", cursor.getRecord().getSymA(0)) ? 10 : -1,
                                                owner.getInt(cursor.getRecord()));
                                    }
                                }
                                owner.cursorClosed();
                                try (RecordCursor cursor = original.getCursor(sqlExecutionContext)) {
                                    worker.init(cursor, sqlExecutionContext);
                                    while (cursor.hasNext()) {
                                        Assert.assertEquals(Chars.equalsNc("alpha", cursor.getRecord().getSymA(1)) ? 10 : -1,
                                                worker.getInt(cursor.getRecord()));
                                    }
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

    private static ExpressionNode binary(String name, ExpressionNode left, ExpressionNode right) {
        final ExpressionNode node = ExpressionNode.FACTORY.newInstance().of(ExpressionNode.FUNCTION, name, 0, 0);
        node.lhs = left;
        node.rhs = right;
        node.paramCount = 2;
        return node;
    }

    private static ExpressionNode call(String name, ObjList<ExpressionNode> arguments) {
        final ExpressionNode expression = ExpressionNode.FACTORY.newInstance().of(ExpressionNode.FUNCTION, name, 0, 0);
        expression.paramCount = arguments.size();
        for (int i = arguments.size() - 1; i >= 0; i--) {
            expression.args.add(arguments.getQuick(i));
        }
        return expression;
    }

    private static ExpressionNode constant(String token) {
        return ExpressionNode.FACTORY.newInstance().of(ExpressionNode.CONSTANT, token, 0, 0);
    }

    private static ExpressionNode literal(String token) {
        return ExpressionNode.FACTORY.newInstance().of(ExpressionNode.LITERAL, token, 0, 0);
    }

    private static OutputSchema schema(RecordMetadata metadata, int firstId) {
        final OutputSchema schema = new OutputSchema();
        for (int i = 0; i < metadata.getColumnCount(); i++) {
            schema.add(firstId + i, metadata.getColumnName(i), metadata.getColumnType(i), true);
            schema.setSymbolTableStatic(i, metadata.isSymbolTableStatic(i));
        }
        return schema;
    }

    private static boolean symbolsEqual(Record record) {
        final CharSequence left = record.getSymA(0);
        final CharSequence right = record.getSymB(1);
        return left == null ? right == null : Chars.equalsNc(left, right);
    }

    private FunctionParser parser(ObjList<Function> constructed) {
        return new FunctionParser(configuration, new FunctionResolver(configuration, engine.getFunctionFactoryCache()) {
            @Override
            public Function createFunction(FunctionFactoryDescriptor overload, int position, CharSequence name,
                                           ObjList<Function> args, IntList positions, SqlExecutionContext context) throws SqlException {
                final Function result = super.createFunction(overload, position, name, args, positions, context);
                constructed.add(result);
                return result;
            }
        });
    }

}
