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

package io.questdb.test.griffin.engine.join;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoConfigurationWrapper;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.EntityColumnFilter;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.RecordSinkFactory;
import io.questdb.cairo.sql.DelegatingRecord;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.LimitOverflowException;
import io.questdb.griffin.engine.QueryProgress;
import io.questdb.griffin.engine.join.CrossJoinRecordCursorFactory;
import io.questdb.griffin.engine.join.MaterializedRecordCursorFactory;
import io.questdb.griffin.engine.join.NestedLoopLeftJoinRecordCursorFactory;
import io.questdb.griffin.engine.join.NestedLoopFullJoinRecordCursorFactory;
import io.questdb.griffin.engine.join.NullRecordFactory;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.LimitedMemoryTracker;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class MaterializedRecordCursorFactoryTest extends AbstractCairoTest {
    @Test
    public void testBaseErrorsPropagateAndFactoryReopens() throws Exception {
        assertMemoryLeak(() -> {
            final CountingFactory base = new CountingFactory(select("SELECT x FROM long_sequence(5)"));
            try (RecordCursorFactory factory = materialize(base, 16, 1)) {
                for (int i = 0; i < 2; i++) {
                    final CairoException failure = i == 0
                            ? CairoException.nonCritical().setOutOfMemory(true).put("base getter OOM")
                            : LimitOverflowException.instance().put("base getter page limit");
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        base.getterFailure = failure;
                        Assert.assertSame(failure, Assert.assertThrows(CairoException.class, cursor::hasNext));
                        base.getterFailure = null;
                    }
                }
                for (int i = 0; i < 2; i++) {
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        if (i == 1) {
                            assertRows(cursor, 1, 3);
                        }
                        base.nextFailure = CairoException.nonCritical().setOutOfMemory(true).put("base next OOM");
                        Assert.assertSame(base.nextFailure, Assert.assertThrows(CairoException.class, cursor::hasNext));
                        base.nextFailure = null;
                    }
                }
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    assertRows(cursor, 1, 5);
                    Assert.assertFalse(cursor.hasNext());
                }
            }
        });
    }

    @Test
    public void testCalculateSizeAfterPartialPrefixRewind() throws Exception {
        assertMemoryLeak(() -> {
            final CountingFactory base = new CountingFactory(select("SELECT x FROM long_sequence(5)"));
            try (RecordCursorFactory factory = materialize(base, 16, 1);
                 RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                assertRows(cursor, 1, 2);
                cursor.toTop();
                assertRows(cursor, 1, 1);
                final RecordCursor.Counter count = new RecordCursor.Counter();
                count.set(10);
                cursor.calculateSize(null, count);
                Assert.assertEquals(14, count.get());
                Assert.assertEquals(1, base.calculateSizeCalls);
                Assert.assertFalse(cursor.hasNext());
                cursor.toTop();
                count.clear();
                cursor.calculateSize(sqlExecutionContext.getCircuitBreaker(), count);
                Assert.assertEquals(5, count.get());
            }
        });
    }

    @Test
    public void testCloseFailureReleasesCache() throws Exception {
        assertMemoryLeak(() -> {
            final CountingFactory base = new CountingFactory(select("SELECT x FROM long_sequence(5)"));
            try (RecordCursorFactory factory = materialize(base, 16, 8)) {
                final long before = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_RECORD_CHAIN);
                final RecordCursor cursor = factory.getCursor(sqlExecutionContext);
                assertRows(cursor, 1, 1);
                Assert.assertTrue(Unsafe.getMemUsedByTag(MemoryTag.NATIVE_RECORD_CHAIN) > before);
                base.closeFailure = new IllegalStateException("base close");
                Assert.assertSame(base.closeFailure, Assert.assertThrows(IllegalStateException.class, cursor::close));
                base.closeFailure = null;
                Assert.assertEquals(before, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_RECORD_CHAIN));
                try (RecordCursor reopened = factory.getCursor(sqlExecutionContext)) {
                    assertRows(reopened, 1, 5);
                }
            }
        });
    }

    @Test
    public void testExactPageRetryRetainsCache() throws Exception {
        assertMemoryLeak(() -> {
            for (int maxPages : new int[]{3, 8}) {
                try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(maxPages == 3 ? 128 : 48)) {
                    sqlExecutionContext.setMemoryTracker(tracker);
                    final CountingFactory base = new CountingFactory(select("SELECT x FROM long_sequence(5)"));
                    try (RecordCursorFactory factory = materialize(base, 16, maxPages);
                         RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        assertRows(cursor, 1, 5);
                        Assert.assertFalse(cursor.hasNext());
                        Assert.assertEquals(48, tracker.getUsed());
                        final int reads = base.hasNextCalls;
                        cursor.toTop();
                        assertRows(cursor, 1, 5);
                        Assert.assertFalse(cursor.hasNext());
                        Assert.assertEquals(reads, base.hasNextCalls);
                    } finally {
                        sqlExecutionContext.setMemoryTracker(null);
                    }
                    Assert.assertEquals(0, tracker.getUsed());
                }
            }
        });
    }

    @Test
    public void testNoInitialHeadroomAndChangingLimit() throws Exception {
        assertMemoryLeak(() -> {
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(15)) {
                sqlExecutionContext.setMemoryTracker(tracker);
                final CountingFactory base = new CountingFactory(select("SELECT x FROM long_sequence(5)"));
                try (RecordCursorFactory factory = materialize(base, 16, 8)) {
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        assertRows(cursor, 1, 1);
                        Assert.assertEquals(0, tracker.getUsed());
                        tracker.setLimit(128);
                        assertRows(cursor, 2, 5);
                        Assert.assertEquals(0, tracker.getUsed());
                        cursor.toTop();
                        Assert.assertEquals(1, base.toTopCalls);
                    }
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        assertRows(cursor, 1, 2);
                        Assert.assertEquals(16, tracker.getUsed());
                        tracker.setLimit(16);
                        assertRows(cursor, 3, 5);
                        Assert.assertEquals(0, tracker.getUsed());
                    }
                } finally {
                    sqlExecutionContext.setMemoryTracker(null);
                }
                Assert.assertEquals(0, tracker.getUsed());
            }
        });
    }

    @Test
    public void testOuterJoinConsumersKeepRecordAcrossFallback() throws Exception {
        assertMemoryLeak(() -> {
            for (int i = 0; i < 2; i++) {
                final GenericRecordMetadata metadata = new GenericRecordMetadata();
                metadata.add(new TableColumnMetadata("m", ColumnType.LONG));
                metadata.add(new TableColumnMetadata("s", ColumnType.LONG));
                final CountingFactory master = new CountingFactory(select("SELECT x FROM long_sequence(3)"));
                final RecordCursorFactory slave = materialize(new CountingFactory(select("SELECT x FROM long_sequence(5)")), 16, 1);
                final BooleanFunction filter = new BooleanFunction() {
                    @Override
                    public boolean getBool(Record record) {
                        return record.getLong(0) > record.getLong(1);
                    }
                };
                try (RecordCursorFactory factory = i == 0
                        ? new NestedLoopLeftJoinRecordCursorFactory(metadata, master, slave, 1, filter,
                        NullRecordFactory.getInstance(slave.getMetadata()))
                        : new NestedLoopFullJoinRecordCursorFactory(metadata, master, slave, 1, filter,
                        NullRecordFactory.getInstance(master.getMetadata()), NullRecordFactory.getInstance(slave.getMetadata()))) {
                    assertFactory(factory).withContext(sqlExecutionContext).noRandomAccess().returns("""
                            m\ts
                            1\tnull
                            2\t1
                            3\t1
                            3\t2
                            """ + (i == 0 ? "" : "null\t3\nnull\t4\nnull\t5\n"));
                }
            }
        });
    }

    @Test
    public void testReplaySkipsAndCountsWithoutBaseWork() throws Exception {
        assertMemoryLeak(() -> {
            final CountingFactory base = new CountingFactory(select("SELECT x FROM long_sequence(5)"));
            try (RecordCursorFactory factory = materialize(base, 16, 8);
                 RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Assert.assertFalse(factory.recordCursorSupportsRandomAccess());
                Assert.assertThrows(UnsupportedOperationException.class, cursor::getRecordB);
                Assert.assertThrows(UnsupportedOperationException.class, () -> cursor.recordAt(cursor.getRecord(), 0));
                final Record record = cursor.getRecord();
                assertRows(cursor, 1, 5);
                Assert.assertFalse(cursor.hasNext());
                final int reads = base.hasNextCalls;
                cursor.toTop();
                final RecordCursor.Counter count = new RecordCursor.Counter();
                count.set(2);
                cursor.skipRows(count, RecordCursor.UNBOUNDED_ROW_COUNT);
                Assert.assertEquals(0, count.get());
                assertRows(cursor, 3, 3);
                cursor.calculateSize(sqlExecutionContext.getCircuitBreaker(), count);
                Assert.assertEquals(2, count.get());
                Assert.assertFalse(cursor.hasNext());
                Assert.assertEquals(reads, base.hasNextCalls);
                Assert.assertEquals(0, base.toTopCalls);
                Assert.assertEquals(0, base.calculateSizeCalls);
                Assert.assertSame(record, cursor.getRecord());
                Assert.assertEquals(5, cursor.size());
            }
        });
    }

    @Test
    public void testSkipAcrossFallbackAndEmptyInput() throws Exception {
        assertMemoryLeak(() -> {
            for (int rows : new int[]{0, 5}) {
                final CountingFactory base = new CountingFactory(select("SELECT x FROM long_sequence(" + rows + ")"));
                try (RecordCursorFactory factory = materialize(base, 16, 1);
                     RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    final RecordCursor.Counter count = new RecordCursor.Counter();
                    count.set(4);
                    cursor.skipRows(count, RecordCursor.UNBOUNDED_ROW_COUNT);
                    Assert.assertEquals(rows == 0 ? 4 : 0, count.get());
                    if (rows > 0) {
                        Assert.assertEquals(1, base.skipCalls);
                        assertRows(cursor, 5, 5);
                    }
                    Assert.assertFalse(cursor.hasNext());
                    count.set(10);
                    cursor.skipRows(count, RecordCursor.UNBOUNDED_ROW_COUNT);
                    Assert.assertEquals(10, count.get());
                    cursor.toTop();
                    count.clear();
                    cursor.calculateSize(null, count);
                    Assert.assertEquals(rows, count.get());
                }
            }
        });
    }

    @Test
    public void testStreamingAfterPartialPrefixRewind() throws Exception {
        assertMemoryLeak(() -> {
            final CountingFactory base = new CountingFactory(select("SELECT x FROM long_sequence(5)"));
            try (RecordCursorFactory factory = materialize(base, 16, 1);
                 RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                final long before = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_RECORD_CHAIN);
                final Record record = cursor.getRecord();
                assertRows(cursor, 1, 2);
                Assert.assertEquals(before + 16, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_RECORD_CHAIN));
                cursor.toTop();
                assertRows(cursor, 1, 3);
                Assert.assertSame(record, cursor.getRecord());
                Assert.assertEquals(before, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_RECORD_CHAIN));
                assertRows(cursor, 4, 5);
                Assert.assertFalse(cursor.hasNext());
                cursor.toTop();
                Assert.assertEquals(1, base.toTopCalls);
                final RecordCursor.Counter count = new RecordCursor.Counter();
                count.set(3);
                cursor.skipRows(count, RecordCursor.UNBOUNDED_ROW_COUNT);
                Assert.assertEquals(1, base.skipCalls);
                Assert.assertEquals(0, count.get());
                assertRows(cursor, 4, 5);
                Assert.assertFalse(cursor.hasNext());
                Assert.assertSame(record, cursor.getRecord());
            }
        });
    }

    @Test
    public void testVariableWidthAuxiliaryHeadroom() throws Exception {
        assertMemoryLeak(() -> {
            for (long limit : new long[]{131_072, 139_264, 270_336}) {
                try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(limit)) {
                    sqlExecutionContext.setMemoryTracker(tracker);
                    final CountingFactory base = new CountingFactory(select("SELECT x, 'a'::STRING s FROM long_sequence(1)"));
                    try (RecordCursorFactory factory = materialize(base, 131_072, 2);
                         RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        assertRows(cursor, 1, 1);
                        Assert.assertEquals("a", cursor.getRecord().getStrA(1).toString());
                        Assert.assertFalse(cursor.hasNext());
                        Assert.assertEquals(limit < 139_264 ? 0 : 139_264, tracker.getUsed());
                        cursor.toTop();
                        assertRows(cursor, 1, 1);
                        Assert.assertEquals(limit < 139_264 ? 1 : 0, base.toTopCalls);
                    } finally {
                        sqlExecutionContext.setMemoryTracker(null);
                    }
                    Assert.assertEquals(0, tracker.getUsed());
                }
            }
        });
    }

    @Test
    public void testVariableWidthDataAndAuxiliaryPageAllowances() throws Exception {
        assertMemoryLeak(() -> {
            final CountingFactory base = new CountingFactory(select(
                    "SELECT x, rpad('v', 65_000, 'v')::STRING s FROM long_sequence(2)"));
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(270_336)) {
                sqlExecutionContext.setMemoryTracker(tracker);
                try (RecordCursorFactory factory = materialize(base, 131_072, 2);
                     RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    assertRows(cursor, 1, 2);
                    Assert.assertFalse(cursor.hasNext());
                    Assert.assertEquals(270_336, tracker.getUsed());
                    final int reads = base.hasNextCalls;
                    cursor.toTop();
                    assertRows(cursor, 1, 2);
                    Assert.assertEquals(65_000, cursor.getRecord().getStrLen(1));
                    Assert.assertEquals(reads, base.hasNextCalls);
                } finally {
                    sqlExecutionContext.setMemoryTracker(null);
                }
                Assert.assertEquals(0, tracker.getUsed());
            }
        });
    }

    @Test
    public void testSymbolsAndNullsAcrossFallback() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE symbols (x LONG, s SYMBOL)");
            execute("INSERT INTO symbols VALUES (1, 'a'), (2, null), (3, 'b')");
            final CountingFactory base = new CountingFactory(select("SELECT x, s FROM symbols"));
            try (RecordCursorFactory factory = materialize(base, 16, 1);
                 RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                final Record record = cursor.getRecord();
                for (int pass = 0; pass < 2; pass++) {
                    for (int row = 1; row <= 3; row++) {
                        assertRows(cursor, row, row);
                        final String expected = row == 1 ? "a" : row == 2 ? null : "b";
                        final CharSequence symbol = record.getSymA(1);
                        Assert.assertEquals(expected, symbol == null ? null : symbol.toString());
                        final CharSequence symbolB = record.getSymB(1);
                        Assert.assertEquals(expected, symbolB == null ? null : symbolB.toString());
                        TestUtils.assertEquals(expected, cursor.getSymbolTable(1).valueOf(record.getInt(1)));
                        TestUtils.assertEquals(expected, cursor.newSymbolTable(1).valueOf(record.getInt(1)));
                        Assert.assertSame(record, cursor.getRecord());
                    }
                    Assert.assertFalse(cursor.hasNext());
                    cursor.toTop();
                }
            }
        });
    }

    @Test
    public void testWideValuesStreamWithoutCopying() throws Exception {
        assertMemoryLeak(() -> {
            final ObjList<String> expressions = new ObjList<>(
                    "rpad('v', 200_000, 'v')::STRING",
                    "rpad('v', 400_000, 'v')::VARCHAR",
                    "array_build(1, 50_000, 1.0)"
            );
            for (int i = 0; i < expressions.size(); i++) {
                final String expression = expressions.getQuick(i);
                final CountingFactory base = new CountingFactory(select("SELECT x, " + expression + " v FROM long_sequence(3)"));
                try (RecordCursorFactory factory = materialize(base, 131_072, 2);
                     RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    final Record record = cursor.getRecord();
                    final long before = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_RECORD_CHAIN);
                    for (int pass = 0; pass < 2; pass++) {
                        for (int row = 1; row <= 3; row++) {
                            Assert.assertTrue(cursor.hasNext());
                            Assert.assertEquals(row, record.getLong(0));
                            final int type = factory.getMetadata().getColumnType(1);
                            switch (ColumnType.tagOf(type)) {
                                case ColumnType.STRING -> Assert.assertEquals(200_000, record.getStrLen(1));
                                case ColumnType.VARCHAR -> Assert.assertEquals(400_000, record.getVarcharSize(1));
                                case ColumnType.ARRAY ->
                                        Assert.assertEquals(50_000, record.getArray(1, type).getFlatViewLength());
                                default -> Assert.fail("unexpected type");
                            }
                            Assert.assertEquals(before, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_RECORD_CHAIN));
                            Assert.assertSame(record, cursor.getRecord());
                        }
                        Assert.assertFalse(cursor.hasNext());
                        cursor.toTop();
                    }
                    Assert.assertEquals(2, base.toTopCalls);
                }
            }
        });
    }

    @Test
    public void testCrossJoinConsumerSkipsAcrossFallback() throws Exception {
        assertMemoryLeak(() -> {
            final GenericRecordMetadata metadata = new GenericRecordMetadata();
            metadata.add(new TableColumnMetadata("m", ColumnType.LONG));
            metadata.add(new TableColumnMetadata("s", ColumnType.LONG));
            final CountingFactory master = new CountingFactory(select("SELECT x FROM long_sequence(2)"));
            final RecordCursorFactory slave = materialize(new CountingFactory(select("SELECT x FROM long_sequence(5)")), 16, 1);
            try (RecordCursorFactory factory = new CrossJoinRecordCursorFactory(metadata, master, slave, 1)) {
                for (int pass = 0; pass < 2; pass++) {
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        final Record record = cursor.getRecord();
                        final RecordCursor.Counter count = new RecordCursor.Counter();
                        // A fresh skip calculates the slave size; a partial skip advances its current scan.
                        if (pass == 1) {
                            Assert.assertTrue(cursor.hasNext());
                            Assert.assertEquals(1, record.getLong(0));
                            Assert.assertEquals(1, record.getLong(1));
                        }
                        count.set(pass == 0 ? 4 : 3);
                        cursor.skipRows(count, RecordCursor.UNBOUNDED_ROW_COUNT);
                        Assert.assertEquals(0, count.get());
                        TestUtils.assertCursor("""
                                m\ts
                                1\t5
                                2\t1
                                2\t2
                                2\t3
                                2\t4
                                2\t5
                                """, cursor, metadata, true, sink);
                        Assert.assertSame(record, cursor.getRecord());
                        cursor.toTop();
                        TestUtils.assertCursor("""
                                m\ts
                                1\t1
                                1\t2
                                1\t3
                                1\t4
                                1\t5
                                2\t1
                                2\t2
                                2\t3
                                2\t4
                                2\t5
                                """, cursor, metadata, true, sink);
                    }
                }
            }
        });
    }

    @Test
    public void testFullJoinEmptyMasterAcrossFallback() throws Exception {
        assertMemoryLeak(() -> {
            final GenericRecordMetadata metadata = new GenericRecordMetadata();
            metadata.add(new TableColumnMetadata("m", ColumnType.LONG));
            metadata.add(new TableColumnMetadata("s", ColumnType.LONG));
            final CountingFactory master = new CountingFactory(select("SELECT x FROM long_sequence(0)"));
            final RecordCursorFactory slave = materialize(new CountingFactory(select("SELECT x FROM long_sequence(5)")), 16, 1);
            try (RecordCursorFactory factory = new NestedLoopFullJoinRecordCursorFactory(
                    metadata, master, slave, 1, BooleanConstant.TRUE,
                    NullRecordFactory.getInstance(master.getMetadata()), NullRecordFactory.getInstance(slave.getMetadata())
            )) {
                assertFactory(factory).withContext(sqlExecutionContext).noRandomAccess().returns("""
                        m\ts
                        null\t1
                        null\t2
                        null\t3
                        null\t4
                        null\t5
                        """);
            }
        });
    }

    @Test
    public void testWrappedMaterializerKeepsJoinRecordAcrossFallback() throws Exception {
        assertMemoryLeak(() -> {
            final GenericRecordMetadata metadata = new GenericRecordMetadata();
            metadata.add(new TableColumnMetadata("m", ColumnType.LONG));
            metadata.add(new TableColumnMetadata("s", ColumnType.LONG));
            final CountingFactory master = new CountingFactory(select("SELECT x FROM long_sequence(2)"));
            final RecordCursorFactory slave = new CountingFactory(materialize(
                    new CountingFactory(select("SELECT x FROM long_sequence(5)")), 16, 1
            ));
            try (RecordCursorFactory factory = new CrossJoinRecordCursorFactory(metadata, master, slave, 1)) {
                assertFactory(factory).withContext(sqlExecutionContext).noRandomAccess().expectSize().returns("""
                        m\ts
                        1\t1
                        1\t2
                        1\t3
                        1\t4
                        1\t5
                        2\t1
                        2\t2
                        2\t3
                        2\t4
                        2\t5
                        """);
            }
        });
    }

    private static void assertRows(RecordCursor cursor, int lo, int hi) {
        for (int row = lo; row <= hi; row++) {
            Assert.assertTrue(cursor.hasNext());
            Assert.assertEquals(row, cursor.getRecord().getLong(0));
        }
    }

    private static RecordCursorFactory materialize(RecordCursorFactory base, int pageSize, int maxPages) {
        final EntityColumnFilter filter = new EntityColumnFilter();
        filter.of(base.getMetadata().getColumnCount());
        return new MaterializedRecordCursorFactory(new CairoConfigurationWrapper(configuration) {
            @Override
            public int getSqlHashJoinLightValuePageSize() {
                return pageSize;
            }

            @Override
            public int getSqlHashJoinValueMaxPages() {
                return maxPages;
            }

            @Override
            public int getSqlHashJoinValuePageSize() {
                return pageSize;
            }
        }, base, RecordSinkFactory.getInstance(configuration, new BytecodeAssembler(), base.getMetadata(), filter));
    }

    private static class CountingFactory extends AbstractRecordCursorFactory {
        private final RecordCursorFactory base;
        private int calculateSizeCalls;
        private RuntimeException closeFailure;
        private CairoException getterFailure;
        private int hasNextCalls;
        private CairoException nextFailure;
        private int skipCalls;
        private int toTopCalls;

        private CountingFactory(RecordCursorFactory base) {
            super(base.getMetadata());
            this.base = base;
        }

        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
            // Query registration installs its own tracker; these tests supply the tracker directly.
            final RecordCursor cursor = (base instanceof QueryProgress ? base.getBaseFactory() : base).getCursor(executionContext);
            final DelegatingRecord record = new DelegatingRecord() {
                @Override
                public long getLong(int col) {
                    if (getterFailure != null) {
                        throw getterFailure;
                    }
                    return super.getLong(col);
                }
            };
            record.of(cursor.getRecord());
            return new NoRandomAccessRecordCursor() {
                @Override
                public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
                    calculateSizeCalls++;
                    cursor.calculateSize(circuitBreaker, counter);
                }

                @Override
                public void close() {
                    cursor.close();
                    if (closeFailure != null) {
                        throw closeFailure;
                    }
                }

                @Override
                public Record getRecord() {
                    return record;
                }

                @Override
                public SymbolTable getSymbolTable(int columnIndex) {
                    return cursor.getSymbolTable(columnIndex);
                }

                @Override
                public boolean hasNext() {
                    hasNextCalls++;
                    if (nextFailure != null) {
                        throw nextFailure;
                    }
                    return cursor.hasNext();
                }

                @Override
                public SymbolTable newSymbolTable(int columnIndex) {
                    return cursor.newSymbolTable(columnIndex);
                }

                @Override
                public long preComputedStateSize() {
                    return cursor.preComputedStateSize();
                }

                @Override
                public long size() {
                    return cursor.size();
                }

                @Override
                public void skipRows(Counter rowCount, long maxRowsAfterSkip) {
                    skipCalls++;
                    cursor.skipRows(rowCount, maxRowsAfterSkip);
                }

                @Override
                public void toTop() {
                    toTopCalls++;
                    cursor.toTop();
                }
            };
        }

        @Override
        public boolean recordCursorSupportsRandomAccess() {
            return false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            base.toPlan(sink);
        }

        @Override
        protected void _close() {
            base.close();
        }
    }
}
