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

package io.questdb.test.griffin.engine.window;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.WindowSPI;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.columns.LongColumn;
import io.questdb.griffin.engine.functions.window.BaseWindowFunction;
import io.questdb.griffin.engine.window.DeferredEmitWindowRecordCursorFactory;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Phase 2 cursor unit tests for {@link DeferredEmitWindowRecordCursorFactory}.
 * <p>
 * Uses a real on-disk table for the base factory (so we get a real random-access cursor) and a
 * synthetic streaming-LEAD function to drive the deferred-emit path. The synthetic function only
 * implements the streaming protocol; its cached fallback {@code pass1} throws on entry, so any test
 * that accidentally routes through the cached path fails loudly.
 */
public class DeferredEmitWindowRecordCursorFactoryTest extends AbstractCairoTest {

    @Test
    public void testEmptyInputProducesNoRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x long, ts timestamp) timestamp(ts) partition by day");
            // No inserts.

            try (RecordCursorFactory factory = newDeferredFactory(1)) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertFalse(cursor.hasNext());
                }
            }
        });
    }

    @Test
    public void testFlushFillsRemainingPendingWithNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x long, ts timestamp) timestamp(ts) partition by day");
            // Two rows + LEAD(1) → row 0 emits with arg(row1); row 1 emits with NULL via flush.
            execute("insert into t values (10, 0), (20, 1000)");

            try (RecordCursorFactory factory = newDeferredFactory(1)) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Record rec = cursor.getRecord();

                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertEquals(10L, rec.getLong(0));
                    Assert.assertEquals(20L, rec.getLong(1)); // LEAD = next row's x.

                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertEquals(20L, rec.getLong(0));
                    Assert.assertEquals(Numbers.LONG_NULL, rec.getLong(1)); // flushed.

                    Assert.assertFalse(cursor.hasNext());
                }
            }
        });
    }

    @Test
    public void testLargerLookaheadFlushesMultiple() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x long, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t values (10, 0), (20, 1000), (30, 2000), (40, 3000), (50, 4000)");

            try (RecordCursorFactory factory = newDeferredFactory(3)) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Record rec = cursor.getRecord();

                    // Lookahead 3 over 5 rows: rows 0,1 in-stream (paired with rows 3,4); rows 2,3,4 flushed.
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertEquals(10L, rec.getLong(0));
                    Assert.assertEquals(40L, rec.getLong(1));

                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertEquals(20L, rec.getLong(0));
                    Assert.assertEquals(50L, rec.getLong(1));

                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertEquals(30L, rec.getLong(0));
                    Assert.assertEquals(Numbers.LONG_NULL, rec.getLong(1));

                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertEquals(40L, rec.getLong(0));
                    Assert.assertEquals(Numbers.LONG_NULL, rec.getLong(1));

                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertEquals(50L, rec.getLong(0));
                    Assert.assertEquals(Numbers.LONG_NULL, rec.getLong(1));

                    Assert.assertFalse(cursor.hasNext());
                }
            }
        });
    }

    @Test
    public void testLeadOneOnFiveRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x long, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t values (10, 0), (20, 1000), (30, 2000), (40, 3000), (50, 4000)");

            try (RecordCursorFactory factory = newDeferredFactory(1)) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Record rec = cursor.getRecord();

                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertEquals(10L, rec.getLong(0));
                    Assert.assertEquals(20L, rec.getLong(1));

                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertEquals(20L, rec.getLong(0));
                    Assert.assertEquals(30L, rec.getLong(1));

                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertEquals(30L, rec.getLong(0));
                    Assert.assertEquals(40L, rec.getLong(1));

                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertEquals(40L, rec.getLong(0));
                    Assert.assertEquals(50L, rec.getLong(1));

                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertEquals(50L, rec.getLong(0));
                    Assert.assertEquals(Numbers.LONG_NULL, rec.getLong(1));

                    Assert.assertFalse(cursor.hasNext());
                }
            }
        });
    }

    @Test
    public void testRejectsAllWindowFunctionsZeroLookahead() throws Exception {
        // Phase 6 still requires AT LEAST ONE positive-lookahead function. If all window functions
        // have lookahead=0, the factory rejects (the planner would route through
        // WindowRecordCursorFactory instead).
        assertMemoryLeak(() -> {
            execute("create table t (x long, ts timestamp) timestamp(ts) partition by day");

            RecordCursorFactory base;
            try (io.questdb.griffin.SqlCompiler compiler = engine.getSqlCompiler()) {
                base = compiler.compile("select x from t", sqlExecutionContext).getRecordCursorFactory();
            }

            GenericRecordMetadata metadata = new GenericRecordMetadata();
            metadata.add(new TableColumnMetadata("x", ColumnType.LONG));
            metadata.add(new TableColumnMetadata("zero_pass_lag", ColumnType.LONG));

            ObjList<Function> functions = new ObjList<>();
            functions.add(LongColumn.newInstance(0));
            TestZeroLookaheadFunction lag = new TestZeroLookaheadFunction(LongColumn.newInstance(0));
            lag.setColumnIndex(1);
            functions.add(lag);

            try {
                new DeferredEmitWindowRecordCursorFactory(base, metadata, functions, null, null, null, 1_048_576);
                Assert.fail("expected CairoException when no positive-lookahead functions present");
            } catch (CairoException e) {
                Assert.assertTrue(
                        "unexpected error message: " + e.getFlyweightMessage(),
                        e.getFlyweightMessage().toString().contains("positive-lookahead")
                );
                Misc.free(base);
                Misc.freeObjList(functions);
            }
        });
    }

    @Test
    public void testRejectsRingTimesLeadCountOverflow() throws Exception {
        // (lookahead+1) * leadCount must be <= 64 for the LEAD pending-bit mask to fit in one long.
        // Two LEAD functions with lookahead=33 each gives ringCapacity=34, 34*2=68, should reject.
        assertMemoryLeak(() -> {
            execute("create table t (x long, ts timestamp) timestamp(ts) partition by day");

            RecordCursorFactory base;
            try (io.questdb.griffin.SqlCompiler compiler = engine.getSqlCompiler()) {
                base = compiler.compile("select x from t", sqlExecutionContext).getRecordCursorFactory();
            }

            GenericRecordMetadata metadata = new GenericRecordMetadata();
            metadata.add(new TableColumnMetadata("x", ColumnType.LONG));
            metadata.add(new TableColumnMetadata("lead_x_1", ColumnType.LONG));
            metadata.add(new TableColumnMetadata("lead_x_2", ColumnType.LONG));

            ObjList<Function> functions = new ObjList<>();
            functions.add(LongColumn.newInstance(0));
            TestStreamingLeadLongFunction lead1 = new TestStreamingLeadLongFunction(LongColumn.newInstance(0), 33);
            lead1.setColumnIndex(1);
            functions.add(lead1);
            TestStreamingLeadLongFunction lead2 = new TestStreamingLeadLongFunction(LongColumn.newInstance(0), 33);
            lead2.setColumnIndex(2);
            functions.add(lead2);

            try {
                new DeferredEmitWindowRecordCursorFactory(base, metadata, functions, null, null, null, 1_048_576);
                Assert.fail("expected CairoException when (lookahead+1)*leadCount > 64");
            } catch (CairoException e) {
                Assert.assertTrue(
                        "unexpected error message: " + e.getFlyweightMessage(),
                        e.getFlyweightMessage().toString().contains("must be <= 64")
                );
                Misc.free(base);
                Misc.freeObjList(functions);
            }
        });
    }

    @Test
    public void testSingleRowFlushedAsNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x long, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t values (42, 0)");

            try (RecordCursorFactory factory = newDeferredFactory(1)) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Record rec = cursor.getRecord();
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertEquals(42L, rec.getLong(0));
                    Assert.assertEquals(Numbers.LONG_NULL, rec.getLong(1));
                    Assert.assertFalse(cursor.hasNext());
                }
            }
        });
    }

    @Test
    public void testToTopReexecutesIdentically() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x long, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t values (10, 0), (20, 1000), (30, 2000)");

            try (RecordCursorFactory factory = newDeferredFactory(1)) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    // First pass.
                    Record rec = cursor.getRecord();
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertEquals(10L, rec.getLong(0));
                    Assert.assertEquals(20L, rec.getLong(1));
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertEquals(20L, rec.getLong(0));
                    Assert.assertEquals(30L, rec.getLong(1));
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertEquals(30L, rec.getLong(0));
                    Assert.assertEquals(Numbers.LONG_NULL, rec.getLong(1));
                    Assert.assertFalse(cursor.hasNext());

                    // Re-execute via toTop().
                    cursor.toTop();
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertEquals(10L, rec.getLong(0));
                    Assert.assertEquals(20L, rec.getLong(1));
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertEquals(20L, rec.getLong(0));
                    Assert.assertEquals(30L, rec.getLong(1));
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertEquals(30L, rec.getLong(0));
                    Assert.assertEquals(Numbers.LONG_NULL, rec.getLong(1));
                    Assert.assertFalse(cursor.hasNext());
                }
            }
        });
    }

    /**
     * Builds the deferred-emit factory wrapping the trivial base query {@code SELECT x FROM t} with a
     * synthetic LEAD on x at the given offset. Two output columns: {@code (x, lead_x)} both LONG.
     */
    private RecordCursorFactory newDeferredFactory(int leadOffset) throws SqlException {
        final RecordCursorFactory base;
        try (io.questdb.griffin.SqlCompiler compiler = engine.getSqlCompiler()) {
            base = compiler.compile("select x from t", sqlExecutionContext).getRecordCursorFactory();
        }

        GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("x", ColumnType.LONG));
        metadata.add(new TableColumnMetadata("lead_x", ColumnType.LONG));

        ObjList<Function> functions = new ObjList<>();
        functions.add(LongColumn.newInstance(0));
        TestStreamingLeadLongFunction lead = new TestStreamingLeadLongFunction(
                LongColumn.newInstance(0),
                leadOffset
        );
        lead.setColumnIndex(1);
        functions.add(lead);

        try {
            return new DeferredEmitWindowRecordCursorFactory(base, metadata, functions, null, null, null, 1_048_576);
        } catch (Throwable t) {
            Misc.free(base);
            Misc.freeObjList(functions);
            throw t;
        }
    }

    /**
     * Synthetic streaming LEAD-Long function for tests. Implements the deferred-emit protocol only;
     * {@link #pass1} throws because no test routes through the cached executor.
     */
    private static final class TestStreamingLeadLongFunction extends BaseWindowFunction implements WindowFunction {
        private final int lookahead;

        TestStreamingLeadLongFunction(Function arg, int lookahead) {
            super(arg);
            this.lookahead = lookahead;
        }

        @Override
        public int getLookahead() {
            return lookahead;
        }

        @Override
        public long getLong(Record rec) {
            // The cursor reads LEAD values directly from its pending memory via OutputRecord, not via
            // this function. Should not be invoked by tests; if it is, fail loudly.
            throw new UnsupportedOperationException("TestStreamingLeadLongFunction.getLong unexpected");
        }

        @Override
        public String getName() {
            return "test_streaming_lead";
        }

        @Override
        public int getPassCount() {
            return ZERO_PASS;
        }

        @Override
        public int getType() {
            return ColumnType.LONG;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            // Cached fallback is not implemented for the test function; any accidental cached routing
            // surfaces as a clear failure.
            throw new UnsupportedOperationException("TestStreamingLeadLongFunction does not implement cached pass1");
        }

        @Override
        public void streamingBackfill(Record source, long pendingSlot, WindowSPI spi) {
            long v = arg.getLong(source);
            Unsafe.getUnsafe().putLong(spi.getAddress(pendingSlot, columnIndex), v);
        }

        @Override
        public void streamingFlushDefault(long pendingSlot, WindowSPI spi) {
            Unsafe.getUnsafe().putLong(spi.getAddress(pendingSlot, columnIndex), Numbers.LONG_NULL);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName()).val('(').val(arg).val(", ").val(lookahead).val(") over ()");
        }
    }

    /**
     * Trivial ZERO_PASS + lookahead==0 window function used to verify that the factory rejects mixed
     * lookahead/non-lookahead constructions in Phase 2.
     */
    private static final class TestZeroLookaheadFunction extends BaseWindowFunction implements WindowFunction {

        TestZeroLookaheadFunction(Function arg) {
            super(arg);
        }

        @Override
        public int getLookahead() {
            return 0;
        }

        @Override
        public long getLong(Record rec) {
            return 0L;
        }

        @Override
        public String getName() {
            return "test_zero_lookahead";
        }

        @Override
        public int getPassCount() {
            return ZERO_PASS;
        }

        @Override
        public int getType() {
            return ColumnType.LONG;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            // Unused in this test.
        }
    }
}
