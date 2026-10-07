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
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.FunctionFactoryDescriptor;
import io.questdb.griffin.FunctionParser;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.WindowExpression;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class FunctionBinderSubsampleTest extends AbstractCairoTest {
    @Test
    public void testConstantValidationPrecedesOverloadSelection() throws Exception {
        assertMemoryLeak(() -> {
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
            try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                configure();
                try {
                    assertError(binder, window("uniform", constant("1.5", 17)), 17, "integer expected for target point count");
                    assertError(binder, window("uniform", constant("'3'", 17)), 17, "integer expected for target point count");
                    assertError(binder, window("uniform", constant("null", 17)), 17, "target point count must be set");
                    assertError(binder, window("uniform", constant("1", 17)), 17, "target points must be at least 2");
                    assertError(binder, window("uniform", constant("2147483648L", 17)), 17, "target points exceeds maximum of 2147483647");
                    assertError(binder, window("cadence", constant("1.5", 17)), 17, "integer expected for stride");
                    assertError(binder, window("cadence", constant("null", 17)), 17, "stride must be set");
                    assertError(binder, window("cadence", constant("0", 17)), 17, "stride must be at least 1");
                    assertError(binder, window("cadence", constant("2147483648L", 17)), 17, "stride exceeds maximum of 2147483647");
                    assertError(binder, window("cadence", constant("2", 17), constant("1.5", 31)), 31, "integer or NULL expected for seed");
                    assertError(binder, window("cadence", constant("2", 17), constant("'seed'", 31)), 31, "integer or NULL expected for seed");
                    binder.bindWindow(window("cadence", constant("1", 17), constant("null", 31)), schema(), null, sqlExecutionContext);
                } finally {
                    sqlExecutionContext.clearWindowContext();
                }
            }
        });
    }

    @Test
    public void testNativeArgumentValidationFailureReleasesChildren() throws Exception {
        assertMemoryLeak(() -> {
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
            try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                configure();
                try {
                    for (int i = 0; i < 5; i++) {
                        assertError(binder, window("uniform", membership(17)), 17, "target point count must be a constant or bind variable");
                        assertError(binder, window("cadence", constant("2", 17), membership(31)), 31, "seed must be a constant, bind variable, or NULL");
                        binder.clear();
                        parser.clear();
                    }
                    binder.bindWindow(window("uniform", constant("3", 17)), schema(), null, sqlExecutionContext);
                } finally {
                    sqlExecutionContext.clearWindowContext();
                }
            }
        });
    }

    @Test
    public void testOrdinaryWindowDiagnosticsRemainUnchanged() throws Exception {
        assertMemoryLeak(() -> {
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
            try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                configure();
                try {
                    final ExpressionNode ordinary = window("uniform", constant("1.5", 17));
                    ordinary.windowExpression.setSubsampleKeepFlag(false);
                    final SqlException original = Assert.assertThrows(SqlException.class,
                            () -> parser.parseFunction(ordinary, metadata(), sqlExecutionContext));
                    final String expected = original.getFlyweightMessage().toString();
                    final int position = original.getPosition();
                    final ExpressionNode bound = window("uniform", constant("1.5", 17));
                    bound.windowExpression.setSubsampleKeepFlag(false);
                    final SqlException actual = Assert.assertThrows(SqlException.class,
                            () -> binder.bindWindow(bound, schema(), null, sqlExecutionContext));
                    TestUtils.assertEquals(expected, actual.getFlyweightMessage());
                    Assert.assertEquals(position, actual.getPosition());
                } finally {
                    sqlExecutionContext.clearWindowContext();
                }
            }
        });
    }

    @Test
    public void testRuntimeTargetsValidateAtEveryExecution() throws Exception {
        assertMemoryLeak(() -> {
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
            final OutputSchema input = schema();
            try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                configure();
                try {
                    final ExpressionNode target = ExpressionNode.FACTORY.newInstance().of(ExpressionNode.BIND_VARIABLE, "$1", 0, 17);
                    final FunctionExpression expression = binder.bindWindow(window("uniform", target), input, null, sqlExecutionContext);
                    try (WindowFunction first = binder.instantiateWindow(expression, input, metadata(), sqlExecutionContext);
                         WindowFunction second = binder.instantiateWindow(expression, input, metadata(), sqlExecutionContext)) {
                        binder.clear();
                        parser.clear();
                        bindVariableService.setLong(0, 3);
                        first.init(null, sqlExecutionContext);
                        second.init(null, sqlExecutionContext);
                        first.cursorClosed();
                        second.cursorClosed();
                        bindVariableService.setLong(0, 1);
                        final SqlException error = Assert.assertThrows(SqlException.class, () -> first.init(null, sqlExecutionContext));
                        Assert.assertEquals(17, error.getPosition());
                        TestUtils.assertEquals("target points must be at least 2", error.getFlyweightMessage());
                        bindVariableService.setLong(0, 4);
                        first.init(null, sqlExecutionContext);
                        second.init(null, sqlExecutionContext);
                    }
                } finally {
                    sqlExecutionContext.clearWindowContext();
                }
            }
        });
    }

    @Test
    public void testTargetExpressionConstructedOnlyOnceDuringBinding() throws Exception {
        assertMemoryLeak(() -> {
            final ObjList<String> constructions = new ObjList<>();
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache()) {
                @Override
                public Function createFunction(FunctionFactoryDescriptor overload, int position, CharSequence name,
                                               ObjList<Function> args, IntList positions, SqlExecutionContext context) throws SqlException {
                    constructions.add(overload.getName().toString());
                    return super.createFunction(overload, position, name, args, positions, context);
                }
            };
            final ExpressionNode sum = ExpressionNode.FACTORY.newInstance().of(ExpressionNode.OPERATION, "+", 0, 17);
            sum.paramCount = 2;
            sum.lhs = constant("2", 17);
            sum.rhs = constant("1", 21);
            final OutputSchema input = schema();
            try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                configure();
                try {
                    final FunctionExpression expression = binder.bindWindow(window("uniform", sum), input, null, sqlExecutionContext);
                    Assert.assertEquals(2, constructions.size());
                    Assert.assertEquals("+", constructions.getQuick(0));
                    Assert.assertEquals("uniform", constructions.getQuick(1));
                    try (WindowFunction function = binder.instantiateWindow(expression, input, metadata(), sqlExecutionContext)) {
                        Assert.assertEquals(3, constructions.size());
                        Assert.assertEquals("uniform", constructions.getQuick(2));
                        function.init(null, sqlExecutionContext);
                    }
                } finally {
                    sqlExecutionContext.clearWindowContext();
                }
            }
        });
    }

    private void assertError(FunctionBindingHarness binder, ExpressionNode expression, int position, String message) throws Exception {
        final SqlException error = Assert.assertThrows(SqlException.class,
                () -> binder.bindWindow(expression, schema(), null, sqlExecutionContext));
        Assert.assertEquals(position, error.getPosition());
        TestUtils.assertEquals(message, error.getFlyweightMessage());
    }

    private void configure() throws SqlException {
        sqlExecutionContext.configureWindowContext(null, null, new ArrayColumnTypes(), true,
                RecordCursorFactory.SCAN_DIRECTION_FORWARD, 0, true, WindowExpression.FRAMING_RANGE,
                Long.MIN_VALUE, (char) 0, 0, 0, 0, (char) 0, 0, 0, WindowExpression.EXCLUDE_NO_OTHERS, 0,
                0, ColumnType.TIMESTAMP_MICRO, false, 0);
    }

    private static ExpressionNode constant(String token, int position) {
        return ExpressionNode.FACTORY.newInstance().of(ExpressionNode.CONSTANT, token, 0, position);
    }

    private static ExpressionNode membership(int position) {
        final ExpressionNode node = ExpressionNode.FACTORY.newInstance().of(ExpressionNode.FUNCTION, "in", 0, position);
        node.paramCount = 3;
        node.args.add(constant("2", position + 3));
        node.args.add(constant("1", position + 2));
        node.args.add(ExpressionNode.FACTORY.newInstance().of(ExpressionNode.LITERAL, "i", 0, position + 1));
        return node;
    }

    private static GenericRecordMetadata metadata() {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("ts", ColumnType.TIMESTAMP_MICRO));
        metadata.add(new TableColumnMetadata("i", ColumnType.INT));
        metadata.setTimestampIndex(0);
        return metadata;
    }

    private static OutputSchema schema() {
        final OutputSchema schema = new OutputSchema().add(7, "ts", ColumnType.TIMESTAMP_MICRO, true)
                .add(9, "i", ColumnType.INT, true);
        schema.setTimestampIndex(0);
        return schema;
    }

    private static ExpressionNode window(String name, ExpressionNode target) {
        final ExpressionNode node = ExpressionNode.FACTORY.newInstance().of(ExpressionNode.FUNCTION, name, 0, 0);
        node.paramCount = 1;
        node.rhs = target;
        node.windowExpression = WindowExpression.FACTORY.newInstance();
        node.windowExpression.setSubsampleKeepFlag(true);
        return node;
    }

    private static ExpressionNode window(String name, ExpressionNode target, ExpressionNode seed) {
        final ExpressionNode node = window(name, target);
        node.paramCount = 2;
        node.lhs = target;
        node.rhs = seed;
        return node;
    }
}
