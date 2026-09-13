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

package io.questdb.test.griffin.engine.table;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SqlExecutionCircuitBreakerConfiguration;
import io.questdb.griffin.DefaultSqlExecutionCircuitBreakerConfiguration;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.CountingSqlExecutionCircuitBreaker;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Regression guard for the per-frame cancellation defect in {@link io.questdb.griffin.engine.table.PageFrameRecordCursorImpl}.
 * <p>
 * The per-frame breaker check used the throttled {@code statefulThrowExceptionIfTripped()}. Under the
 * production throttle (2,000,000), after the at-open real check the per-frame checks merely increment the
 * throttle counter and never fire a real check for any realistic table (the next real check would land
 * ~2,000,000 frames later), so a {@code CANCEL QUERY} issued mid-scan was silently ignored by the cursor.
 * <p>
 * With the fix (un-throttled per-frame check), advancing to the next page frame always observes a tripped
 * breaker. This test runs at the production throttle and forces one row per page frame so a tiny table
 * yields many frames; it would FAIL before the fix (the scan drains to completion) and PASS after.
 */
public class PageFrameScanCancellationTest extends AbstractCairoTest {

    @Before
    public void setUp() {
        // Production throttle: a throttle of 0 would mask the defect by making every check a real check.
        final SqlExecutionCircuitBreakerConfiguration config = new DefaultSqlExecutionCircuitBreakerConfiguration() {
            @Override
            public int getCircuitBreakerThrottle() {
                return 2_000_000;
            }
        };
        circuitBreaker = new NetworkSqlExecutionCircuitBreaker(engine, config) {
            @Override
            protected boolean testConnection(long fd) {
                return false;
            }
        };
        super.setUp();
        ((SqlExecutionContextImpl) sqlExecutionContext).with(circuitBreaker);
    }

    @Test
    public void testPartitionSizeAndRejectedIntervalTraversalCancellation() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select x, timestamp_sequence('2020-01-01T12:00:00', 86400000000) ts from long_sequence(64)) timestamp(ts) partition by DAY");
            SqlExecutionCircuitBreaker previous = sqlExecutionContext.getCircuitBreaker();
            try {
                for (boolean isInterval : new boolean[]{false, true}) {
                    for (int order : new int[]{PartitionFrameCursorFactory.ORDER_ASC, PartitionFrameCursorFactory.ORDER_DESC}) {
                        String sql = "select * from t" + (isInterval ? " where ts in '2020-01-01;1s;1d;64'" : "");
                        try (RecordCursorFactory factory = select(sql)) {
                            CountingSqlExecutionCircuitBreaker breaker = new CountingSqlExecutionCircuitBreaker(SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER) {
                                @Override
                                public void statefulThrowExceptionIfTrippedTimeThrottled() {
                                    super.statefulThrowExceptionIfTrippedTimeThrottled();
                                    if (getCheckCount() == 16) {
                                        throw CairoException.queryCancelled(-1);
                                    }
                                }
                            };
                            ((SqlExecutionContextImpl) sqlExecutionContext).with(breaker);
                            try (PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, order)) {
                                try {
                                    if (isInterval) {
                                        cursor.next();
                                    } else {
                                        cursor.calculateSize(new RecordCursor.Counter());
                                    }
                                    Assert.fail("expected cancellation inside partition/interval traversal");
                                } catch (CairoException ex) {
                                    Assert.assertTrue(ex.isCancellation());
                                }
                            }
                            Assert.assertEquals(16, breaker.getCheckCount());
                            ((SqlExecutionContextImpl) sqlExecutionContext).with(previous);
                            try (PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, order)) {
                                RecordCursor.Counter counter = new RecordCursor.Counter();
                                cursor.calculateSize(counter);
                                Assert.assertEquals(isInterval ? 0 : 64, counter.get());
                            }
                        }
                    }
                }
            } finally {
                ((SqlExecutionContextImpl) sqlExecutionContext).with(previous);
            }
        });
    }

    @Test
    public void testDirectFrameCursorsObserveCancellationAndRebind() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select x, timestamp_sequence('2020-01-01', 1000000) ts from long_sequence(64)) timestamp(ts) partition by DAY");
            for (boolean parquet : new boolean[]{false, true}) {
                if (parquet) {
                    execute("alter table t convert partition to parquet where ts >= '2020-01-01'");
                }
                for (int order : new int[]{PartitionFrameCursorFactory.ORDER_ASC, PartitionFrameCursorFactory.ORDER_DESC}) {
                    circuitBreaker.resetTimer();
                    circuitBreaker.setTimeout(Long.MAX_VALUE);
                    try (RecordCursorFactory factory = select("select * from t")) {
                        sqlExecutionContext.changePageFrameSizes(1, 1);
                        try (PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, order)) {
                            Assert.assertNotNull(cursor.next());
                            circuitBreaker.cancel();
                            try {
                                cursor.next();
                                Assert.fail("expected cancellation while preparing frames");
                            } catch (CairoException ex) {
                                Assert.assertTrue(ex.isCancellation());
                            }
                        }
                        circuitBreaker.resetTimer();
                        try (PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, order)) {
                            Assert.assertNotNull(cursor.next());
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testMultiFrameScanObservesMidScanCancellation() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select x, timestamp_sequence(0, 1_000_000) ts from long_sequence(64)) timestamp(ts) partition by DAY");

            circuitBreaker.resetTimer();
            circuitBreaker.setTimeout(Long.MAX_VALUE); // never time out; we test explicit cancellation only

            try (RecordCursorFactory factory = select("select * from t")) {
                // One row per page frame => the 64-row table yields 64 frames, so the per-frame check (not
                // the intra-frame fast path) is what must observe the mid-scan cancellation. Set this after
                // compilation (which can reset the context) and before getCursor (which reads the sizes).
                ((SqlExecutionContextImpl) sqlExecutionContext).changePageFrameSizes(1, 1);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    // First frame: breaker not tripped yet, so the scan starts normally.
                    Assert.assertTrue(cursor.hasNext());

                    // User issues CANCEL QUERY mid-scan. cancel() flips whichever cancelled-flag is currently
                    // installed (the query registry replaces it during getCursor), so this is robust.
                    circuitBreaker.cancel();

                    try {
                        //noinspection StatementWithEmptyBody
                        while (cursor.hasNext()) {
                            // advancing to the next page frame must observe the cancellation
                        }
                        Assert.fail("expected the multi-frame scan to abort on a tripped circuit breaker mid-scan");
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "cancelled by user");
                        Assert.assertTrue("expected cancellation classification", e.isCancellation());
                        Assert.assertTrue("expected interruption classification", e.isInterruption());
                    }
                }
            }
        });
    }
}
