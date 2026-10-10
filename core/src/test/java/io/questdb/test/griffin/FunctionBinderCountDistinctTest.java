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
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.FunctionParser;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.columns.ColumnFunction;
import io.questdb.griffin.engine.functions.columns.SymbolColumn;
import io.questdb.griffin.engine.groupby.FastGroupByAllocator;
import io.questdb.griffin.engine.groupby.SimpleMapValue;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.ConstantExpression;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.ScanPlan;
import io.questdb.std.Long256;
import io.questdb.std.Long256Impl;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.test.griffin.FunctionBindingHarness.binary;
import static io.questdb.test.griffin.FunctionBindingHarness.call;
import static io.questdb.test.griffin.FunctionBindingHarness.metadata;
import static io.questdb.test.griffin.FunctionBindingHarness.parser;
import static io.questdb.test.griffin.FunctionBindingHarness.unary;


public class FunctionBinderCountDistinctTest extends AbstractCairoTest {
    @Test
    public void testAllRegistrationsReconstructFinalLayoutsAndIndependentState() throws Exception {
        assertMemoryLeak(() -> {
            for (int type : new int[]{ColumnType.INT, ColumnType.LONG, ColumnType.IPv4, ColumnType.STRING,
                    ColumnType.VARCHAR, ColumnType.SYMBOL, ColumnType.UUID, ColumnType.LONG256}) {
                final ObjList<Function> constructions = new ObjList<>();
                final FunctionParser parser = parser(engine, constructions);
                final OutputSchema original = new OutputSchema().add(10, "unused", ColumnType.INT, true)
                        .add(27, "v", type, true);
                final OutputSchema pruned = new OutputSchema().add(27, "v", type, true);
                final GenericRecordMetadata originalMetadata = metadata(type, true);
                final GenericRecordMetadata prunedMetadata = metadata(type, false);
                try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                    final ExpressionNode ast = unary("count_distinct", literal("v"));
                    final FunctionExpression expression = binder.bindAggregate(ast, original, null, sqlExecutionContext);
                    ast.clear();
                    Assert.assertEquals(1, constructions.size());
                    Assert.assertTrue(expression.isAggregate());
                    Assert.assertFalse(expression.getOverload().isRelocatableScalar());
                    Assert.assertEquals(type, expression.argumentAt(0).getDataType());
                    Assert.assertEquals(27, ((ColumnExpression) expression.argumentAt(0)).getColumnId());
                    try (Function owner = binder.instantiateAggregate(expression, pruned, prunedMetadata, sqlExecutionContext);
                         Function worker = binder.instantiateAggregate(expression, original, originalMetadata, sqlExecutionContext);
                         FastGroupByAllocator ownerAllocator = new FastGroupByAllocator(1024, 4096);
                         FastGroupByAllocator workerAllocator = new FastGroupByAllocator(1024, 4096);
                         SimpleMapValue ownerValue = new SimpleMapValue(2);
                         SimpleMapValue workerValue = new SimpleMapValue(5)) {
                        Assert.assertEquals(3, constructions.size());
                        Assert.assertNotSame(constructions.getQuick(0), owner);
                        Assert.assertNotSame(owner, worker);
                        Assert.assertEquals(0, ((ColumnFunction) ((UnaryFunction) owner).getArg()).getColumnIndex());
                        Assert.assertEquals(1, ((ColumnFunction) ((UnaryFunction) worker).getArg()).getColumnIndex());
                        Assert.assertEquals(type != ColumnType.STRING && type != ColumnType.VARCHAR, owner.supportsParallelism());
                        final GroupByFunction first = (GroupByFunction) owner;
                        final GroupByFunction second = (GroupByFunction) worker;
                        final ArrayColumnTypes values = new ArrayColumnTypes();
                        first.initValueTypes(values);
                        Assert.assertEquals(2, values.getColumnCount());
                        second.initValueIndex(3);
                        first.setAllocator(ownerAllocator);
                        second.setAllocator(workerAllocator);
                        binder.clear();
                        parser.clear();
                        original.clear();
                        pruned.clear();
                        final ValueRecord firstRecord = new ValueRecord(0);
                        final ValueRecord secondRecord = new ValueRecord(1);
                        first.computeFirst(ownerValue, firstRecord.of(1), 0);
                        first.computeNext(ownerValue, firstRecord.of(2), 1);
                        first.computeNext(ownerValue, firstRecord.of(1), 2);
                        first.computeNext(ownerValue, firstRecord.of(Numbers.LONG_NULL), 3);
                        second.computeFirst(workerValue, secondRecord.of(7), 0);
                        second.computeNext(workerValue, secondRecord.of(7), 1);
                        Assert.assertEquals(2, owner.getLong(ownerValue));
                        Assert.assertEquals(1, worker.getLong(workerValue));
                        first.clear();
                        first.toTop();
                        first.computeFirst(ownerValue, firstRecord.of(Numbers.LONG_NULL), 0);
                        Assert.assertEquals(0, owner.getLong(ownerValue));
                        first.computeNext(ownerValue, firstRecord.of(9), 1);
                        Assert.assertEquals(1, owner.getLong(ownerValue));
                        Assert.assertEquals(1, worker.getLong(workerValue));
                    }
                }
            }
        });
    }

    @Test
    public void testCopiedRemapsLeaveOriginalPreparationOwned() throws Exception {
        assertMemoryLeak(() -> {
            final ObjList<Function> constructions = new ObjList<>();
            final OutputSchema original = new OutputSchema().add(27, "v", ColumnType.INT, true);
            try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser(engine, constructions))) {
                final BoundExpression expression = binder.bind(binary("|", literal("v"), constant("2")), original, null, sqlExecutionContext);
                final ProjectPlan left = projection(10);
                final ProjectPlan right = projection(11);
                final BoundExpression firstCopy = binder.copyRemappedColumns(expression, left);
                final BoundExpression secondCopy = binder.copyRemappedColumns(expression, right);
                Assert.assertEquals(1, constructions.size());
                try (Function first = binder.instantiate(firstCopy, left.getInput().getOutput(), sqlExecutionContext);
                     Function second = binder.instantiate(secondCopy, right.getInput().getOutput(), sqlExecutionContext);
                     Function retained = binder.instantiate(expression, original, sqlExecutionContext)) {
                    Assert.assertSame(constructions.getQuick(0), retained);
                    Assert.assertNotSame(first, second);
                    Assert.assertEquals(3, constructions.size());
                    binder.clear();
                    final ValueRecord record = new ValueRecord(0).of(5);
                    Assert.assertEquals(7, first.getInt(record));
                    Assert.assertEquals(7, second.getInt(record));
                    Assert.assertEquals(7, retained.getInt(record));
                }
            }
        });
    }

    @Test
    public void testOwnedScalarArgumentsCloseOnFailureAndReconstruction() throws Exception {
        assertMemoryLeak(() -> {
            final FunctionParser parser = parser(engine, new ObjList<>());
            final OutputSchema full = new OutputSchema().add(1, "unused", ColumnType.INT, true)
                    .add(27, "v", ColumnType.LONG, true);
            final OutputSchema pruned = new OutputSchema().add(27, "v", ColumnType.LONG, true);
            final ObjList<ExpressionNode> inArgs = new ObjList<>();
            inArgs.add(literal("v"));
            inArgs.add(constant("1"));
            inArgs.add(constant("2"));
            inArgs.add(constant("3"));
            final ObjList<ExpressionNode> caseArgs = new ObjList<>();
            caseArgs.add(call("in", inArgs));
            caseArgs.add(literal("v"));
            caseArgs.add(constant("null"));
            final ExpressionNode counted = unary("count_distinct", call("case", caseArgs));
            try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                try {
                    binder.bindAggregate(unary("count_distinct", counted), full, null, sqlExecutionContext);
                    Assert.fail("nested aggregate accepted");
                } catch (SqlException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "Aggregate function cannot be passed as an argument");
                }
                final FunctionExpression expression = binder.bindAggregate(counted, full, null, sqlExecutionContext);
                try (Function owner = binder.instantiateAggregate(expression, pruned, metadata(ColumnType.LONG, false), sqlExecutionContext);
                     Function worker = binder.instantiateAggregate(expression, full, metadata(ColumnType.LONG, true), sqlExecutionContext);
                     FastGroupByAllocator allocator = new FastGroupByAllocator(1024, 4096);
                     SimpleMapValue value = new SimpleMapValue(2)) {
                    binder.clear();
                    parser.clear();
                    final GroupByFunction aggregate = (GroupByFunction) owner;
                    aggregate.initValueTypes(new ArrayColumnTypes());
                    aggregate.setAllocator(allocator);
                    owner.init(null, sqlExecutionContext);
                    worker.init(null, sqlExecutionContext);
                    final ValueRecord record = new ValueRecord(0);
                    aggregate.computeFirst(value, record.of(1), 0);
                    aggregate.computeNext(value, record.of(2), 1);
                    aggregate.computeNext(value, record.of(4), 2);
                    Assert.assertEquals(2, owner.getLong(value));
                }
                // A new unclaimed preparation must also release its native IN set.
                binder.bindAggregate(counted, full, null, sqlExecutionContext);
            }
        });
    }

    @Test
    public void testScalarAndNestedAggregateContextsRemainRejected() throws Exception {
        assertMemoryLeak(() -> {
            final ObjList<Function> constructions = new ObjList<>();
            final OutputSchema input = new OutputSchema().add(27, "v", ColumnType.INT, true);
            try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser(engine, constructions))) {
                try {
                    binder.bind(unary("count_distinct", literal("v")), input, null, sqlExecutionContext);
                    Assert.fail("aggregate accepted as scalar");
                } catch (SqlException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "aggregate functions are not allowed in this context");
                }
                try {
                    binder.bindAggregate(unary("count_distinct", unary("count_distinct", literal("v"))), input, null, sqlExecutionContext);
                    Assert.fail("nested aggregate accepted");
                } catch (SqlException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "Aggregate function cannot be passed as an argument");
                }
                Assert.assertEquals(0, constructions.size());
                binder.bindAggregate(unary("count_distinct", literal("v")), input, null, sqlExecutionContext);
                Assert.assertEquals(1, constructions.size());
            }
        });
    }

    @Test
    public void testSymbolEarlyExitRefreshesDictionaryAfterCompilerReset() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE fb_count_symbol(unused INT,s SYMBOL)");
            execute("INSERT INTO fb_count_symbol VALUES(1,'alpha'),(2,'beta'),(3,null),(4,'alpha')");
            final FunctionParser parser = parser(engine, new ObjList<>());
            try (RecordCursorFactory source = select("SELECT s FROM fb_count_symbol");
                 FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                final OutputSchema schema = new OutputSchema().add(27, "s", ColumnType.SYMBOL, true);
                schema.setSymbolTableStatic(0, true);
                final FunctionExpression expression = binder.bindAggregate(unary("count_distinct", literal("s")), schema, null, sqlExecutionContext);
                try (Function function = binder.instantiateAggregate(expression, schema, source.getMetadata(), sqlExecutionContext);
                     FastGroupByAllocator allocator = new FastGroupByAllocator(1024, 4096);
                     SimpleMapValue value = new SimpleMapValue(2)) {
                    Assert.assertTrue(((UnaryFunction) function).getArg() instanceof SymbolColumn);
                    final GroupByFunction aggregate = (GroupByFunction) function;
                    aggregate.initValueTypes(new ArrayColumnTypes());
                    aggregate.setAllocator(allocator);
                    binder.clear();
                    parser.clear();
                    schema.clear();
                    for (int pass = 0; pass < 2; pass++) {
                        aggregate.clear();
                        aggregate.toTop();
                        try (RecordCursor cursor = source.getCursor(sqlExecutionContext)) {
                            function.init(cursor, sqlExecutionContext);
                            Assert.assertTrue(cursor.hasNext());
                            aggregate.computeFirst(value, cursor.getRecord(), 0);
                            Assert.assertFalse(aggregate.earlyExit(value));
                            while (cursor.hasNext()) {
                                aggregate.computeNext(value, cursor.getRecord(), 0);
                            }
                            Assert.assertEquals(2 + pass, function.getLong(value));
                            Assert.assertTrue(aggregate.earlyExit(value));
                            function.cursorClosed();
                        }
                        if (pass == 0) {
                            execute("INSERT INTO fb_count_symbol VALUES(5,'gamma')");
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testWideNullPredicatesConstantsAndParameters() throws Exception {
        assertMemoryLeak(() -> {
            final FunctionParser parser = parser(engine, new ObjList<>());
            try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                for (int type : new int[]{ColumnType.IPv4, ColumnType.UUID, ColumnType.LONG256}) {
                    final OutputSchema full = new OutputSchema().add(1, "unused", ColumnType.INT, true).add(27, "v", type, true);
                    final OutputSchema pruned = new OutputSchema().add(27, "v", type, true);
                    final BoundExpression predicate = binder.bind(binary("!=", constant("null"), literal("v")), full, null, sqlExecutionContext);
                    try (Function owner = binder.instantiate(predicate, pruned, sqlExecutionContext);
                         Function worker = binder.instantiate(predicate, full, sqlExecutionContext)) {
                        final ValueRecord left = new ValueRecord(0);
                        final ValueRecord right = new ValueRecord(1);
                        Assert.assertTrue(owner.getBool(left.of(1)));
                        Assert.assertTrue(worker.getBool(right.of(1)));
                        Assert.assertFalse(owner.getBool(left.of(Numbers.LONG_NULL)));
                        Assert.assertFalse(worker.getBool(right.of(Numbers.LONG_NULL)));
                    }
                    bindVariableService.clear();
                    if (type == ColumnType.IPv4) {
                        bindVariableService.setIPv4(0, 11);
                    } else if (type == ColumnType.UUID) {
                        bindVariableService.setUuid(0, 11, 12);
                    } else {
                        bindVariableService.setLong256(0, 11, 12, 13, 14);
                    }
                    final ExpressionNode parameter = ExpressionNode.FACTORY.newInstance().of(ExpressionNode.BIND_VARIABLE, "$1", 0, 2);
                    final FunctionExpression count = binder.bindAggregate(unary("count_distinct", parameter), new OutputSchema(), null, sqlExecutionContext);
                    try (Function function = binder.instantiateAggregate(count, new OutputSchema(), new GenericRecordMetadata(), sqlExecutionContext);
                         FastGroupByAllocator allocator = new FastGroupByAllocator(1024, 4096);
                         SimpleMapValue value = new SimpleMapValue(2)) {
                        final GroupByFunction aggregate = (GroupByFunction) function;
                        aggregate.initValueTypes(new ArrayColumnTypes());
                        aggregate.setAllocator(allocator);
                        function.init(null, sqlExecutionContext);
                        aggregate.computeFirst(value, null, 0);
                        aggregate.computeNext(value, null, 1);
                        Assert.assertEquals(1, function.getLong(value));
                    }
                }
                final BoundExpression invalidUuid = binder.bind(binary("=", literal("v"), constant("'invalid uuid'")),
                        new OutputSchema().add(27, "v", ColumnType.UUID, true), null, sqlExecutionContext);
                try (Function function = binder.instantiate(invalidUuid, new OutputSchema(), sqlExecutionContext)) {
                    Assert.assertFalse(function.getBool(null));
                }
                final ConstantExpression constant = (ConstantExpression) binder.bind(constant("0x0123456789abcdef"), new OutputSchema(), null, sqlExecutionContext);
                Assert.assertEquals(ColumnType.LONG256, constant.getDataType());
                try (Function owner = binder.instantiate(constant, new OutputSchema(), sqlExecutionContext);
                     Function worker = binder.instantiate(constant, new OutputSchema(), sqlExecutionContext)) {
                    binder.clear();
                    parser.clear();
                    Assert.assertEquals(0x123456789abcdefL, owner.getLong256A(null).getLong0());
                    Assert.assertEquals(0x123456789abcdefL, worker.getLong256A(null).getLong0());
                }
            }
        });
    }

    private static ExpressionNode constant(String token) {
        return ExpressionNode.FACTORY.newInstance().of(ExpressionNode.CONSTANT, token, 0, 2);
    }

    private static ExpressionNode literal(String name) {
        return ExpressionNode.FACTORY.newInstance().of(ExpressionNode.LITERAL, name, 0, 1);
    }

    private static ProjectPlan projection(int inputId) {
        final ScanPlan input = new ScanPlan();
        input.getOutput().add(inputId, "physical", ColumnType.INT, true);
        final ProjectPlan project = new ProjectPlan().of(input, 0);
        project.getOutput().add(27, "v", ColumnType.INT, true);
        project.getExpressions().add(new ColumnExpression().of(inputId, ColumnType.INT, 0));
        return project;
    }

    private static class ValueRecord implements Record {
        private final int index;
        private final Long256Impl long256 = new Long256Impl();
        private long value;

        private ValueRecord(int index) {
            this.index = index;
        }

        @Override
        public int getInt(int col) {
            Assert.assertEquals(index, col);
            return value == Numbers.LONG_NULL ? Numbers.INT_NULL : (int) value;
        }

        @Override
        public int getIPv4(int col) {
            Assert.assertEquals(index, col);
            return value == Numbers.LONG_NULL ? Numbers.IPv4_NULL : (int) value;
        }

        @Override
        public long getLong(int col) {
            Assert.assertEquals(index, col);
            return value;
        }

        @Override
        public long getLong128Lo(int col) {
            Assert.assertEquals(index, col);
            return value;
        }

        @Override
        public long getLong128Hi(int col) {
            Assert.assertEquals(index, col);
            return value == Numbers.LONG_NULL ? Numbers.LONG_NULL : value + 1;
        }

        @Override
        public Long256 getLong256A(int col) {
            Assert.assertEquals(index, col);
            return long256;
        }

        @Override
        public Long256 getLong256B(int col) {
            return getLong256A(col);
        }

        @Override
        public void getLong256(int col, CharSink<?> sink) {
            Assert.assertEquals(index, col);
            long256.toSink(sink);
        }

        @Override
        public CharSequence getStrA(int col) {
            Assert.assertEquals(index, col);
            return value == Numbers.LONG_NULL ? null : Long.toString(value);
        }

        @Override
        public CharSequence getStrB(int col) {
            return getStrA(col);
        }

        @Override
        public Utf8Sequence getVarcharA(int col) {
            final CharSequence text = getStrA(col);
            return text == null ? null : new Utf8String(text);
        }

        @Override
        public Utf8Sequence getVarcharB(int col) {
            return getVarcharA(col);
        }

        private ValueRecord of(long value) {
            this.value = value;
            long256.setAll(value, value, value, value);
            return this;
        }
    }
}
