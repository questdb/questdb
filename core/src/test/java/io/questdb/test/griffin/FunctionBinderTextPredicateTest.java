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
import io.questdb.griffin.FunctionResolver;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class FunctionBinderTextPredicateTest extends AbstractCairoTest {
    @Test
    public void testLikeMatcherWorkersRelocateAndReinitializeIndependently() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setStr(0, "a_c");
            final ObjList<Function> constructed = new ObjList<>();
            final FunctionParser parser = new FunctionParser(configuration, new FunctionResolver(configuration, engine.getFunctionFactoryCache()) {
                @Override
                public Function createFunction(FunctionFactoryDescriptor overload, int position, CharSequence name,
                                               ObjList<Function> args, IntList positions, SqlExecutionContext context) throws SqlException {
                    final Function result = super.createFunction(overload, position, name, args, positions, context);
                    constructed.add(result);
                    return result;
                }
            });
            final OutputSchema original = new OutputSchema().add(5, "unused", ColumnType.LONG, true)
                    .add(70, "v", ColumnType.VARCHAR, true);
            final OutputSchema pruned = new OutputSchema().add(70, "v", ColumnType.VARCHAR, true);
            try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                final FunctionExpression expression = (FunctionExpression) binder.bind(binary("like", unary("trim", literal("v")), parameter("$1")), original, null, sqlExecutionContext);
                Assert.assertEquals(2, constructed.size());
                try (Function owner = binder.instantiate(expression, pruned, sqlExecutionContext);
                     Function worker = binder.instantiate(expression, original, sqlExecutionContext)) {
                    Assert.assertSame(constructed.getQuick(1), owner);
                    Assert.assertNotSame(owner, worker);
                    Assert.assertEquals(4, constructed.size());
                    Assert.assertFalse(owner.isThreadSafe());
                    Assert.assertFalse(worker.isThreadSafe());
                    owner.init(null, sqlExecutionContext);
                    worker.init(null, sqlExecutionContext);
                    Assert.assertTrue(owner.getBool(record(0, " abc ")));
                    Assert.assertTrue(worker.getBool(record(1, " axc ")));
                    bindVariableService.setStr(0, "%中");
                    owner.init(null, sqlExecutionContext);
                    Assert.assertTrue(owner.getBool(record(0, " é中 ")));
                    Assert.assertFalse(worker.getBool(record(1, " é中 ")));
                    worker.init(null, sqlExecutionContext);
                    Assert.assertTrue(worker.getBool(record(1, " é中 ")));
                    binder.clear();
                    parser.clear();
                    original.clear();
                    pruned.clear();
                    Assert.assertFalse(owner.getBool(record(0, null)));
                    Assert.assertTrue(worker.getBool(record(1, " 中 ")));
                }
            }
        });
    }

    @Test
    public void testVarcharRuntimeEqualityPreservesSelectedAliasAndPrefixCache() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setVarchar(0, new Utf8String("abcdef中"));
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
            final OutputSchema original = new OutputSchema().add(5, "unused", ColumnType.LONG, true)
                    .add(70, "v", ColumnType.VARCHAR, true);
            final OutputSchema pruned = new OutputSchema().add(70, "v", ColumnType.VARCHAR, true);
            try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                final FunctionExpression expression = (FunctionExpression) binder.bind(binary("!=", parameter("$1"), literal("v")), original, null, sqlExecutionContext);
                try (Function owner = binder.instantiate(expression, pruned, sqlExecutionContext);
                     Function worker = binder.instantiate(expression, original, sqlExecutionContext)) {
                    owner.init(null, sqlExecutionContext);
                    worker.init(null, sqlExecutionContext);
                    Assert.assertFalse(owner.getBool(record(0, "abcdef中")));
                    Assert.assertFalse(worker.getBool(record(1, "abcdef中")));
                    Assert.assertTrue(owner.getBool(record(0, "abcdefé")));
                    bindVariableService.setVarchar(0, null);
                    owner.init(null, sqlExecutionContext);
                    worker.init(null, sqlExecutionContext);
                    Assert.assertFalse(owner.getBool(record(0, null)));
                    Assert.assertFalse(worker.getBool(record(1, null)));
                    bindVariableService.setVarchar(0, new Utf8String("changed"));
                    owner.init(null, sqlExecutionContext);
                    worker.init(null, sqlExecutionContext);
                    binder.clear();
                    parser.clear();
                    original.clear();
                    pruned.clear();
                    Assert.assertFalse(owner.getBool(record(0, "changed")));
                    Assert.assertFalse(worker.getBool(record(1, "changed")));
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

    private static ExpressionNode literal(String token) {
        return ExpressionNode.FACTORY.newInstance().of(ExpressionNode.LITERAL, token, 0, 0);
    }

    private static ExpressionNode parameter(String name) {
        return ExpressionNode.FACTORY.newInstance().of(ExpressionNode.BIND_VARIABLE, name, 0, 0);
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
