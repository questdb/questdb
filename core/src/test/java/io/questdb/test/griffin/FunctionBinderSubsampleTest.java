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
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.FunctionFactoryDescriptor;
import io.questdb.griffin.FunctionParser;
import io.questdb.griffin.FunctionResolver;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.WindowExpression;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.Numbers;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.test.griffin.FunctionBindingHarness.constant;

public class FunctionBinderSubsampleTest extends AbstractCairoTest {
    @Test
    public void testConstantValidationPrecedesOverloadSelection() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (ts TIMESTAMP, i INT) TIMESTAMP(ts) PARTITION BY DAY");
            assertExceptionNoLeakCheck("SELECT ts, i FROM x SUBSAMPLE uniform(1.5)", 38, "integer expected for target point count");
            assertExceptionNoLeakCheck("SELECT ts, i FROM x SUBSAMPLE uniform('3')", 38, "integer expected for target point count");
            assertExceptionNoLeakCheck("SELECT ts, i FROM x SUBSAMPLE uniform(null)", 38, "target point count must be set");
            assertExceptionNoLeakCheck("SELECT ts, i FROM x SUBSAMPLE uniform(1)", 38, "target points must be at least 2");
            assertExceptionNoLeakCheck("SELECT ts, i FROM x SUBSAMPLE uniform(2147483648L)", 38, "target points exceeds maximum of 2147483647");
            assertExceptionNoLeakCheck("SELECT ts, i FROM x SUBSAMPLE cadence(1.5)", 38, "integer expected for stride");
            assertExceptionNoLeakCheck("SELECT ts, i FROM x SUBSAMPLE cadence(null)", 38, "stride must be set");
            assertExceptionNoLeakCheck("SELECT ts, i FROM x SUBSAMPLE cadence(0)", 38, "stride must be at least 1");
            assertExceptionNoLeakCheck("SELECT ts, i FROM x SUBSAMPLE cadence(2147483648L)", 38, "stride exceeds maximum of 2147483647");
            assertExceptionNoLeakCheck("SELECT ts, i FROM x SUBSAMPLE cadence(2, 1.5)", 41, "integer or NULL expected for seed");
            assertExceptionNoLeakCheck("SELECT ts, i FROM x SUBSAMPLE cadence(2, 'seed')", 41, "integer or NULL expected for seed");
            assertQuery("SELECT ts, i FROM x SUBSAMPLE cadence(1, null)")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("ts\ti\n");
        });
    }

    @Test
    public void testIntegerBoundariesAndFoldedTargets() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (ts TIMESTAMP, i INT) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES (0, 1), (1, 2), (2, 3)");
            assertExceptionNoLeakCheck("SELECT ts, i FROM x SUBSAMPLE uniform(null::LONG)", 42,
                    "target point count must be set");
            assertExceptionNoLeakCheck("SELECT ts, i FROM x SUBSAMPLE cadence(null::LONG)", 42,
                    "stride must be set");
            assertExceptionNoLeakCheck("SELECT ts, i FROM x SUBSAMPLE uniform(-2)", 38,
                    "target points must be at least 2");
            assertExceptionNoLeakCheck("SELECT ts, i FROM x SUBSAMPLE cadence(-1)", 38,
                    "stride must be at least 1");
            assertQuery("SELECT ts, i FROM x SUBSAMPLE uniform(1 + 1)").noLeakCheck().timestamp("ts").returns("""
                    ts	i
                    1970-01-01T00:00:00.000000Z	1
                    1970-01-01T00:00:00.000002Z	3
                    """);
            assertQuery("SELECT ts, i FROM x SUBSAMPLE uniform(2::BYTE)").noLeakCheck().timestamp("ts").returns("""
                    ts	i
                    1970-01-01T00:00:00.000000Z	1
                    1970-01-01T00:00:00.000002Z	3
                    """);
            assertQuery("SELECT ts, i FROM x SUBSAMPLE uniform(2_147_483_647L)").noLeakCheck().timestamp("ts").returns("""
                    ts	i
                    1970-01-01T00:00:00.000000Z	1
                    1970-01-01T00:00:00.000001Z	2
                    1970-01-01T00:00:00.000002Z	3
                    """);
            assertQuery("SELECT ts, i FROM x SUBSAMPLE cadence(1::SHORT, null::LONG)").noLeakCheck().timestamp("ts").returns("""
                    ts	i
                    1970-01-01T00:00:00.000000Z	1
                    1970-01-01T00:00:00.000001Z	2
                    1970-01-01T00:00:00.000002Z	3
                    """);
        });
    }

    @Test
    public void testRuntimeCadenceStrideRevalidatesAfterFailure() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (ts TIMESTAMP, i INT) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES (0, 1), (1, 2), (2, 3)");
            bindVariableService.setLong(0, 1);
            try (RecordCursorFactory factory = select("SELECT ts, i FROM x SUBSAMPLE cadence($1, null)")) {
                final String expected = """
                        ts	i
                        1970-01-01T00:00:00.000000Z	1
                        1970-01-01T00:00:00.000001Z	2
                        1970-01-01T00:00:00.000002Z	3
                        """;
                assertFactory(factory).withContext(sqlExecutionContext).timestamp("ts").returns(expected);
                final long[] strides = {Numbers.LONG_NULL, 0, 2_147_483_648L};
                final ObjList<String> errors = new ObjList<>("stride must be set", "stride must be at least 1",
                        "stride exceeds maximum of 2147483647");
                for (int i = 0; i < strides.length; i++) {
                    bindVariableService.setLong(0, strides[i]);
                    final SqlException error = Assert.assertThrows(SqlException.class, () -> {
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            while (cursor.hasNext()) {
                            }
                        }
                    });
                    Assert.assertEquals(38, error.getPosition());
                    TestUtils.assertEquals(errors.getQuick(i), error.getFlyweightMessage());
                    bindVariableService.setLong(0, 1);
                    assertFactory(factory).withContext(sqlExecutionContext).timestamp("ts").returns(expected);
                }
            }
        });
    }

    @Test
    public void testNativeArgumentValidationFailureReleasesChildren() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (ts TIMESTAMP, i INT) TIMESTAMP(ts) PARTITION BY DAY");
            for (int i = 0; i < 5; i++) {
                assertExceptionNoLeakCheck("SELECT ts, i FROM x SUBSAMPLE uniform((SELECT 1))", 39,
                        "target point count must be a constant or bind variable");
                assertExceptionNoLeakCheck("SELECT ts, i FROM x SUBSAMPLE cadence(2, (SELECT 1))", 42,
                        "seed must be a constant, bind variable, or NULL");
                assertExceptionNoLeakCheck("SELECT ts, i FROM x SUBSAMPLE uniform((SELECT 1 FROM long_sequence(1) WHERE x = (SELECT 1)))", 39,
                        "target point count must be a constant or bind variable");
                assertExceptionNoLeakCheck("SELECT ts, i FROM x SUBSAMPLE cadence(2, (SELECT 1 FROM long_sequence(1) WHERE x = (SELECT 1)))", 42,
                        "seed must be a constant, bind variable, or NULL");
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
            final FunctionParser parser = new FunctionParser(configuration, new FunctionResolver(configuration, engine.getFunctionFactoryCache()) {
                @Override
                public Function createFunction(FunctionFactoryDescriptor overload, int position, CharSequence name,
                                               ObjList<Function> args, IntList positions, SqlExecutionContext context) throws SqlException {
                    constructions.add(overload.getName().toString());
                    return super.createFunction(overload, position, name, args, positions, context);
                }
            });
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
        final ExpressionNode subsample = ExpressionNode.FACTORY.newInstance().of(ExpressionNode.FUNCTION, name, 0, 0);
        subsample.paramCount = 1;
        subsample.args.add(target);
        final ExpressionNode node = ExpressionNode.FACTORY.newInstance().of(ExpressionNode.FUNCTION, name, 0, 0);
        node.paramCount = 1;
        node.rhs = target;
        node.windowExpression = WindowExpression.FACTORY.newInstance();
        node.windowExpression.setSubsampleKeepFlag(true);
        node.windowExpression.setPendingSubsample(subsample, 0, true);
        return node;
    }

    private void configure() throws SqlException {
        sqlExecutionContext.configureWindowContext(null, null, new ArrayColumnTypes(), true,
                RecordCursorFactory.SCAN_DIRECTION_FORWARD, 0, WindowExpression.FRAMING_RANGE,
                Long.MIN_VALUE, (char) 0, 0, 0, 0, (char) 0, 0, 0, WindowExpression.EXCLUDE_NO_OTHERS, 0,
                0, ColumnType.TIMESTAMP_MICRO, false, 0);
    }

}
