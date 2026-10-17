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
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.FunctionFactoryDescriptor;
import io.questdb.griffin.FunctionParser;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.columns.ColumnFunction;
import io.questdb.griffin.engine.functions.groupby.FirstIntGroupByFunctionFactory;
import io.questdb.griffin.engine.groupby.FastGroupByAllocator;
import io.questdb.griffin.engine.groupby.FlyweightPackedMapValue;
import io.questdb.griffin.engine.groupby.SimpleMapValue;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.test.griffin.FunctionBindingHarness.metadata;
import static io.questdb.test.griffin.FunctionBindingHarness.parser;


public class FunctionBinderOrdinaryAggregateTest extends AbstractCairoTest {
    @Test
    public void testFirstLastKeepRowOrderNullsAndIndependentWorkerState() throws Exception {
        assertMemoryLeak(() -> {
            for (boolean isFirst : new boolean[]{true, false}) {
                final String name = isFirst ? "first" : "last";
                for (int type : new int[]{ColumnType.BOOLEAN, ColumnType.BYTE, ColumnType.SHORT, ColumnType.CHAR,
                        ColumnType.INT, ColumnType.LONG, ColumnType.FLOAT, ColumnType.DOUBLE, ColumnType.DATE,
                        ColumnType.TIMESTAMP_MICRO, ColumnType.TIMESTAMP_NANO, ColumnType.STRING,
                        ColumnType.VARCHAR, ColumnType.IPv4, ColumnType.UUID}) {
                    final ObjList<Function> constructions = new ObjList<>();
                    final FunctionParser parser = parser(engine, constructions);
                    final OutputSchema full = schema(type, true);
                    final OutputSchema pruned = schema(type, false);
                    try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                        final FunctionExpression expression = binder.bindAggregate(call(name), full, null, sqlExecutionContext);
                        Assert.assertTrue(expression.getOverload().isOrderSensitiveAggregate());
                        Assert.assertEquals(type, expression.getDataType());
                        Assert.assertEquals(27, ((ColumnExpression) expression.argumentAt(0)).getColumnId());
                        try (Function owner = binder.instantiateAggregate(expression, pruned, metadata(type, false), sqlExecutionContext);
                             Function worker = binder.instantiateAggregate(expression, full, metadata(type, true), sqlExecutionContext);
                             FastGroupByAllocator ownerAllocator = new FastGroupByAllocator(1024, 4096);
                             FastGroupByAllocator workerAllocator = new FastGroupByAllocator(1024, 4096);
                             SimpleMapValue ownerValue = new SimpleMapValue(3);
                             SimpleMapValue workerValue = new SimpleMapValue(7)) {
                            Assert.assertEquals(3, constructions.size());
                            Assert.assertNotSame(constructions.getQuick(0), owner);
                            Assert.assertNotSame(owner, worker);
                            assertColumnIndex(owner, 0);
                            assertColumnIndex(worker, 1);
                            final GroupByFunction first = prepare(owner, ownerAllocator);
                            final GroupByFunction second = prepare(worker, workerAllocator);
                            second.initValueIndex(4);
                            binder.clear();
                            parser.clear();
                            full.clear();
                            pruned.clear();
                            final ValueRecord a = new ValueRecord(0);
                            final ValueRecord b = new ValueRecord(1);
                            first.computeFirst(ownerValue, a.of(3), 10);
                            first.computeNext(ownerValue, a.of(2), 4);
                            first.computeNext(ownerValue, a.of(7), 40);
                            a.of(99);
                            second.computeFirst(workerValue, b.of(9), 20);
                            second.computeNext(workerValue, b.of(5), 30);
                            b.of(99);
                            assertValue(owner, ownerValue, isFirst ? 2 : 7);
                            assertValue(worker, workerValue, isFirst ? 9 : 5);
                            first.computeNext(ownerValue, a.of(Numbers.LONG_NULL), isFirst ? 1 : 100);
                            assertValue(owner, ownerValue, Numbers.LONG_NULL);
                            assertValue(worker, workerValue, isFirst ? 9 : 5);
                            first.setNull(ownerValue);
                            assertValue(owner, ownerValue, Numbers.LONG_NULL);
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testMinMaxPreserveTemporalPrecisionAndCaptureFinalColumnIndex() throws Exception {
        assertMemoryLeak(() -> {
            for (boolean isMin : new boolean[]{true, false}) {
                final String name = isMin ? "min" : "max";
                for (int type : new int[]{ColumnType.DATE, ColumnType.TIMESTAMP_MICRO, ColumnType.TIMESTAMP_NANO}) {
                    final FunctionParser parser = parser(engine, new ObjList<>());
                    final OutputSchema full = schema(type, true);
                    final OutputSchema pruned = schema(type, false);
                    final int[] lookups = {0, 0};
                    try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                        final FunctionExpression expression = binder.bindAggregate(call(name), full, null, sqlExecutionContext);
                        Assert.assertEquals(type, expression.getDataType());
                        Assert.assertFalse(expression.getOverload().isOrderSensitiveAggregate());
                        try (Function owner = binder.instantiateAggregate(expression, pruned, metadata(type, false), sqlExecutionContext);
                             Function worker = binder.instantiateAggregate(expression, full, metadata(type, true), sqlExecutionContext);
                             PageFrameMemoryRecord ownerFrame = new PageFrameMemoryRecord() {
                                 @Override
                                 public long getPageAddress(int columnIndex) {
                                     Assert.assertEquals(0, columnIndex);
                                     lookups[0]++;
                                     return 0;
                                 }
                             };
                             PageFrameMemoryRecord workerFrame = new PageFrameMemoryRecord() {
                                 @Override
                                 public long getPageAddress(int columnIndex) {
                                     Assert.assertEquals(1, columnIndex);
                                     lookups[1]++;
                                     return 0;
                                 }
                             };
                             SimpleMapValue value = new SimpleMapValue(2)) {
                            final GroupByFunction first = (GroupByFunction) owner;
                            final GroupByFunction second = (GroupByFunction) worker;
                            final ArrayColumnTypes types = new ArrayColumnTypes();
                            first.initValueTypes(types);
                            second.initValueTypes(types);
                            final FlyweightPackedMapValue packed = new FlyweightPackedMapValue(types);
                            first.computeKeyedBatch(ownerFrame, packed, 0, 0, 0, 0);
                            second.computeKeyedBatch(workerFrame, packed, 0, 0, 0, 0);
                            Assert.assertEquals(1, lookups[0]);
                            Assert.assertEquals(1, lookups[1]);
                            binder.clear();
                            parser.clear();
                            final ValueRecord a = new ValueRecord(0);
                            first.computeFirst(value, a.of(Numbers.LONG_NULL), 0);
                            first.computeNext(value, a.of(3), 1);
                            first.computeNext(value, a.of(1), 2);
                            first.computeNext(value, a.of(2), 3);
                            first.computeNext(value, a.of(Numbers.LONG_NULL), 4);
                            second.computeFirst(value, new ValueRecord(1).of(8), 0);
                            assertValue(owner, value, name.equals("min") ? 1 : 3);
                            assertValue(worker, value, 8);
                            first.setNull(value);
                            assertValue(owner, value, Numbers.LONG_NULL);
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testScalarAndNestedAggregatesRemainRejected() throws Exception {
        assertMemoryLeak(() -> {
            final OutputSchema input = schema(ColumnType.INT, false);
            try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser(engine, new ObjList<>()))) {
                try {
                    binder.bind(call("first"), input, null, sqlExecutionContext);
                    Assert.fail("aggregate accepted as scalar");
                } catch (SqlException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "aggregate functions are not allowed in this context");
                }
                final ExpressionNode nested = call("last");
                nested.rhs = call("first");
                try {
                    binder.bindAggregate(nested, input, null, sqlExecutionContext);
                    Assert.fail("nested aggregate accepted");
                } catch (SqlException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "Aggregate function cannot be passed as an argument");
                }
                Assert.assertTrue(binder.bindAggregate(call("first_not_null"), input, null, sqlExecutionContext)
                        .getOverload().isOrderSensitiveAggregate());
            }
            final FunctionFactoryDescriptor descriptor = new FunctionFactoryDescriptor(new FirstIntGroupByFunctionFactory() {
            });
            Assert.assertTrue(descriptor.isOrderSensitiveAggregate());
        });
    }

    @Test
    public void testSymbolFirstLastUseFinalDictionaryCapabilitiesAfterCompilerReset() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE fb_ordered_symbol(unused INT,v SYMBOL)");
            execute("INSERT INTO fb_ordered_symbol VALUES(1,'alpha'),(2,null),(3,'beta')");
            sqlExecutionContext.setCloneSymbolTables(true);
            try {
                for (boolean isFirst : new boolean[]{true, false}) {
                    final String name = isFirst ? "first" : "last";
                    for (boolean isDynamic : new boolean[]{false, true}) {
                        final FunctionParser parser = parser(engine, new ObjList<>());
                        try (RecordCursorFactory original = select("SELECT unused,v FROM fb_ordered_symbol");
                             RecordCursorFactory narrowed = select(isDynamic
                                     ? "SELECT v FROM fb_ordered_symbol UNION ALL SELECT v FROM fb_ordered_symbol"
                                     : "SELECT v FROM fb_ordered_symbol");
                             FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                            final OutputSchema full = schema(ColumnType.SYMBOL, true);
                            final OutputSchema pruned = schema(ColumnType.SYMBOL, false);
                            full.setSymbolTableStatic(1, true);
                            final FunctionExpression expression = binder.bindAggregate(call(name), full, null, sqlExecutionContext);
                            try (Function owner = binder.instantiateAggregate(expression, pruned, narrowed.getMetadata(), sqlExecutionContext);
                                 Function worker = binder.instantiateAggregate(expression, full, original.getMetadata(), sqlExecutionContext);
                                 SimpleMapValue ownerValue = new SimpleMapValue(2);
                                 SimpleMapValue workerValue = new SimpleMapValue(2)) {
                                Assert.assertEquals(!isDynamic, ((SymbolFunction) owner).isSymbolTableStatic());
                                Assert.assertTrue(((SymbolFunction) worker).isSymbolTableStatic());
                                Assert.assertTrue(expression.getOverload().isOrderSensitiveAggregate());
                                final GroupByFunction first = (GroupByFunction) owner;
                                final GroupByFunction second = (GroupByFunction) worker;
                                first.initValueTypes(new ArrayColumnTypes());
                                second.initValueTypes(new ArrayColumnTypes());
                                binder.clear();
                                parser.clear();
                                for (int pass = 0; pass < 2; pass++) {
                                    computeSymbols(owner, first, ownerValue, narrowed, isFirst ? "alpha" : "beta");
                                    computeSymbols(worker, second, workerValue, original, isFirst ? "alpha" : "beta");
                                }
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
    public void testTextMinMaxCopyMutableValuesAndPreserveNulls() throws Exception {
        assertMemoryLeak(() -> {
            for (boolean isMin : new boolean[]{true, false}) {
                final String name = isMin ? "min" : "max";
                for (int type : new int[]{ColumnType.STRING, ColumnType.VARCHAR}) {
                    final FunctionParser parser = parser(engine, new ObjList<>());
                    final OutputSchema full = schema(type, true);
                    final OutputSchema pruned = schema(type, false);
                    try (FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                        final FunctionExpression expression = binder.bindAggregate(call(name), full, null, sqlExecutionContext);
                        Assert.assertFalse(expression.getOverload().isOrderSensitiveAggregate());
                        try (Function owner = binder.instantiateAggregate(expression, pruned, metadata(type, false), sqlExecutionContext);
                             Function worker = binder.instantiateAggregate(expression, full, metadata(type, true), sqlExecutionContext);
                             FastGroupByAllocator ownerAllocator = new FastGroupByAllocator(1024, 4096);
                             FastGroupByAllocator workerAllocator = new FastGroupByAllocator(1024, 4096);
                             SimpleMapValue ownerValue = new SimpleMapValue(1);
                             SimpleMapValue workerValue = new SimpleMapValue(1)) {
                            Assert.assertFalse(owner.isThreadSafe());
                            Assert.assertNotSame(owner, worker);
                            final GroupByFunction first = prepare(owner, ownerAllocator);
                            final GroupByFunction second = prepare(worker, workerAllocator);
                            binder.clear();
                            parser.clear();
                            final ValueRecord a = new ValueRecord(0);
                            final ValueRecord b = new ValueRecord(1);
                            first.computeFirst(ownerValue, a.of(Numbers.LONG_NULL), 0);
                            first.computeNext(ownerValue, a.of(3), 1);
                            first.computeNext(ownerValue, a.of(1), 2);
                            first.computeNext(ownerValue, a.of(2), 3);
                            first.computeNext(ownerValue, a.of(Numbers.LONG_NULL), 4);
                            a.of(99);
                            second.computeFirst(workerValue, b.of(8), 0);
                            b.of(99);
                            assertValue(owner, ownerValue, name.equals("min") ? 1 : 3);
                            assertValue(worker, workerValue, 8);
                            first.setNull(ownerValue);
                            assertValue(owner, ownerValue, Numbers.LONG_NULL);
                        }
                    }
                }
            }
        });
    }

    private static void assertColumnIndex(Function function, int index) {
        Assert.assertEquals(index, ((ColumnFunction) ((UnaryFunction) function).getArg()).getColumnIndex());
    }

    private static void assertValue(Function function, Record record, long value) {
        final boolean isNull = value == Numbers.LONG_NULL;
        switch (ColumnType.tagOf(function.getType())) {
            case ColumnType.BOOLEAN -> Assert.assertEquals(!isNull && (value & 1) != 0, function.getBool(record));
            case ColumnType.BYTE -> Assert.assertEquals(isNull ? 0 : (byte) value, function.getByte(record));
            case ColumnType.SHORT -> Assert.assertEquals(isNull ? 0 : (short) value, function.getShort(record));
            case ColumnType.CHAR -> Assert.assertEquals(isNull ? 0 : (char) value, function.getChar(record));
            case ColumnType.INT ->
                    Assert.assertEquals(isNull ? Numbers.INT_NULL : (int) value, function.getInt(record));
            case ColumnType.IPv4 ->
                    Assert.assertEquals(isNull ? Numbers.IPv4_NULL : (int) value, function.getIPv4(record));
            case ColumnType.LONG -> Assert.assertEquals(value, function.getLong(record));
            case ColumnType.DATE -> Assert.assertEquals(value, function.getDate(record));
            case ColumnType.TIMESTAMP -> Assert.assertEquals(value, function.getTimestamp(record));
            case ColumnType.FLOAT -> Assert.assertEquals(isNull ? Float.NaN : value, function.getFloat(record), 0);
            case ColumnType.DOUBLE -> Assert.assertEquals(isNull ? Double.NaN : value, function.getDouble(record), 0);
            case ColumnType.STRING ->
                    TestUtils.assertEquals(isNull ? null : "value-" + value, function.getStrA(record));
            case ColumnType.VARCHAR -> {
                final Utf8Sequence actual = function.getVarcharA(record);
                TestUtils.assertEquals(isNull ? null : "value-" + value,
                        actual == null ? null : Utf8s.stringFromUtf8Bytes(actual));
            }
            case ColumnType.UUID -> {
                Assert.assertEquals(value, function.getLong128Lo(record));
                Assert.assertEquals(isNull ? value : value + 1, function.getLong128Hi(record));
            }
            default -> Assert.fail("unhandled type " + function.getType());
        }
    }

    private static ExpressionNode call(String name) {
        final ExpressionNode node = ExpressionNode.FACTORY.newInstance().of(ExpressionNode.FUNCTION, name, 0, 0);
        node.rhs = ExpressionNode.FACTORY.newInstance().of(ExpressionNode.LITERAL, "v", 0, 6);
        node.paramCount = 1;
        return node;
    }

    private static GroupByFunction prepare(Function function, FastGroupByAllocator allocator) {
        final GroupByFunction aggregate = (GroupByFunction) function;
        aggregate.initValueTypes(new ArrayColumnTypes());
        aggregate.setAllocator(allocator);
        return aggregate;
    }

    private static OutputSchema schema(int type, boolean isFull) {
        final OutputSchema schema = new OutputSchema();
        if (isFull) {
            schema.add(10, "unused", ColumnType.INT, true);
        }
        return schema.add(27, "v", type, true);
    }

    private void computeSymbols(Function function, GroupByFunction aggregate, SimpleMapValue value, RecordCursorFactory source, String expected) throws SqlException {
        function.toTop();
        try (RecordCursor cursor = source.getCursor(sqlExecutionContext)) {
            function.init(cursor, sqlExecutionContext);
            Assert.assertTrue(cursor.hasNext());
            aggregate.computeFirst(value, cursor.getRecord(), 0);
            long rowId = 1;
            while (cursor.hasNext()) {
                aggregate.computeNext(value, cursor.getRecord(), rowId++);
            }
            TestUtils.assertEquals(expected, function.getSymbol(value));
            aggregate.setNull(value);
            Assert.assertNull(function.getSymbol(value));
            function.cursorClosed();
        }
    }

    private static class ValueRecord implements Record {
        private final int index;
        private final StringSink text = new StringSink();
        private final Utf8StringSink utf8 = new Utf8StringSink();
        private long value;

        private ValueRecord(int index) {
            this.index = index;
        }

        @Override
        public boolean getBool(int col) {
            Assert.assertEquals(index, col);
            return value != Numbers.LONG_NULL && (value & 1) != 0;
        }

        @Override
        public byte getByte(int col) {
            Assert.assertEquals(index, col);
            return value == Numbers.LONG_NULL ? 0 : (byte) value;
        }

        @Override
        public char getChar(int col) {
            Assert.assertEquals(index, col);
            return value == Numbers.LONG_NULL ? 0 : (char) value;
        }

        @Override
        public long getDate(int col) {
            return getLong(col);
        }

        @Override
        public double getDouble(int col) {
            Assert.assertEquals(index, col);
            return value == Numbers.LONG_NULL ? Double.NaN : value;
        }

        @Override
        public float getFloat(int col) {
            Assert.assertEquals(index, col);
            return value == Numbers.LONG_NULL ? Float.NaN : value;
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
        public long getLong128Hi(int col) {
            Assert.assertEquals(index, col);
            return value == Numbers.LONG_NULL ? value : value + 1;
        }

        @Override
        public long getLong128Lo(int col) {
            return getLong(col);
        }

        @Override
        public short getShort(int col) {
            Assert.assertEquals(index, col);
            return value == Numbers.LONG_NULL ? 0 : (short) value;
        }

        @Override
        public CharSequence getStrA(int col) {
            Assert.assertEquals(index, col);
            return value == Numbers.LONG_NULL ? null : text;
        }

        @Override
        public long getTimestamp(int col) {
            return getLong(col);
        }

        @Override
        public Utf8Sequence getVarcharA(int col) {
            Assert.assertEquals(index, col);
            return value == Numbers.LONG_NULL ? null : utf8;
        }

        private ValueRecord of(long value) {
            this.value = value;
            text.clear();
            text.put("value-").put(value);
            utf8.clear();
            utf8.put(text);
            return this;
        }
    }
}
