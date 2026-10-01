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

import io.questdb.PropertyKey;
import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.DefaultSqlExecutionCircuitBreakerConfiguration;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.window.CachedWindowLightRecordCursorFactory;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class SubsampleRegistryCancellationTest extends AbstractCairoTest {
    private static final int ROWS = 8193;

    @Test
    public void testCancellationAtScanCompletion() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, "true");
            execute("CREATE TABLE t AS (SELECT x::TIMESTAMP ts, CASE WHEN x = 4096 THEN NULL ELSE x::DOUBLE END v FROM long_sequence(" + ROWS + ")) TIMESTAMP(ts)");
            for (String selection : List.of("uniform(500)", "cadence(2)", "m4(v,8193)", "m4(v,500)", "minmax(v,500)", "lttb(v,500)", "sdt(v,0.5)")) {
                final SqlExecutionCircuitBreaker originalBreaker = sqlExecutionContext.getCircuitBreaker();
                final CancellationState state = new CancellationState();
                try (NetworkSqlExecutionCircuitBreaker breaker = new NetworkSqlExecutionCircuitBreaker(engine, new DefaultSqlExecutionCircuitBreakerConfiguration() {
                    @Override
                    public int getCircuitBreakerThrottle() {
                        return 2_000_000;
                    }
                }) {
                    @Override
                    public void statefulThrowExceptionIfTrippedTimeThrottled() {
                        if (state.hasCancelled) {
                            state.checksAfterCancel++;
                            Assert.assertEquals("selection allocated after cancellation: " + selection, state.memoryAtCancel, state.tracker.getUsed());
                        }
                        super.statefulThrowExceptionIfTrippedTimeThrottled();
                    }
                }) {
                    ((SqlExecutionContextImpl) sqlExecutionContext).with(breaker);
                    try (RecordCursorFactory factory = select("SELECT ts, v FROM t SUBSAMPLE " + selection)) {
                        final CachedWindowLightRecordCursorFactory light = findLight(factory);
                        light.wrapBaseFactory(base -> new CancellingFactory(base, state));
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            state.tracker = sqlExecutionContext.getMemoryTracker();
                            Assert.assertNotNull(state.tracker);
                            try {
                                cursor.hasNext();
                                Assert.fail("expected cancellation before the first output row: " + selection);
                            } catch (CairoException e) {
                                Assert.assertTrue(e.isCancellation());
                                Assert.assertTrue(state.hasCancelled);
                                Assert.assertEquals(ROWS, state.rows);
                                Assert.assertEquals(1, state.checksAfterCancel);
                            }
                        }
                        Assert.assertEquals(0, state.tracker.getUsed());
                        Assert.assertEquals(0, engine.getBusyReaderCount());
                        LongList ids = new LongList();
                        engine.getQueryRegistry().getEntryIds(ids);
                        Assert.assertEquals(0, ids.size());
                    }
                } finally {
                    ((SqlExecutionContextImpl) sqlExecutionContext).with(originalBreaker);
                }
            }
        });
    }

    private static CachedWindowLightRecordCursorFactory findLight(RecordCursorFactory factory) {
        for (RecordCursorFactory current = factory; current != null; current = current.getBaseFactory()) {
            if (current instanceof CachedWindowLightRecordCursorFactory light) {
                return light;
            }
        }
        throw new AssertionError("expected a cached LIGHT window factory");
    }

    private static class CancellationState {
        private int checksAfterCancel;
        private boolean hasCancelled;
        private long memoryAtCancel;
        private int rows;
        private MemoryTracker tracker;
    }

    private static class CancellingCursor implements RecordCursor {
        private final CancellationState state;
        private RecordCursor base;

        CancellingCursor(RecordCursor base, CancellationState state) {
            this.base = base;
            this.state = state;
        }

        @Override
        public void close() {
            base = Misc.free(base);
        }

        @Override
        public Record getRecord() {
            return base.getRecord();
        }

        @Override
        public Record getRecordB() {
            return base.getRecordB();
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return base.getSymbolTable(columnIndex);
        }

        @Override
        public boolean hasNext() {
            if (base.hasNext()) {
                state.rows++;
                return true;
            }
            // Cancel through the registry only after the real table cursor finishes scanning.
            // This does not reset the breaker's count throttle or set its cancel() sentinel.
            if (!state.hasCancelled) {
                state.memoryAtCancel = state.tracker.getUsed();
                state.hasCancelled = engine.getQueryRegistry().cancel(state.tracker.getQueryId(), sqlExecutionContext);
                Assert.assertTrue(state.hasCancelled);
            }
            return false;
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return base.newSymbolTable(columnIndex);
        }

        @Override
        public long preComputedStateSize() {
            return base.preComputedStateSize();
        }

        @Override
        public void recordAt(Record record, long rowId) {
            base.recordAt(record, rowId);
        }

        @Override
        public long size() {
            return base.size();
        }

        @Override
        public void toTop() {
            base.toTop();
        }
    }

    private static class CancellingFactory extends AbstractRecordCursorFactory {
        private final RecordCursorFactory base;
        private final CancellationState state;

        CancellingFactory(RecordCursorFactory base, CancellationState state) {
            super(base.getMetadata());
            this.base = base;
            this.state = state;
        }

        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
            return new CancellingCursor(base.getCursor(executionContext), state);
        }

        @Override
        public int getScanDirection() {
            return base.getScanDirection();
        }

        @Override
        public boolean recordCursorSupportsRandomAccess() {
            return base.recordCursorSupportsRandomAccess();
        }

        @Override
        protected void _close() {
            Misc.free(base);
        }
    }
}
