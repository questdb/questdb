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

package io.questdb.test.griffin.engine;

import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.engine.QueryProgress;
import io.questdb.metrics.HealthMetrics;
import io.questdb.std.LongList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * A query fails when any cursor call that reads its rows fails: hasNext(), and
 * also skipRows(), calculateSize() and size(), which protocols call on the
 * query's cursor to page through the result and count it. {@link QueryProgress}
 * records such a failure as the query's error, once, and unregisters the query.
 */
public class QueryProgressCursorErrorTest extends AbstractCairoTest {
    private static final String FAILURE = "inconvertible value: 10000 [LONG -> DECIMAL(4,0)]";
    // the filter fails at x = 10, whose 10 * 1_000 does not fit DECIMAL(4,0)
    private static final String FILTER_FAILS_AT_ROW_10 = "SELECT x FROM long_sequence(100) WHERE (x * 1_000)::DECIMAL(4,0) > 0::DECIMAL(4,0)";

    @Test
    public void testCalculateSizeFailureIsQueryError() throws Exception {
        assertFailureIsQueryError(
                FILTER_FAILS_AT_ROW_10,
                false,
                cursor -> cursor.calculateSize(sqlExecutionContext.getCircuitBreaker(), new RecordCursor.Counter())
        );
    }

    @Test
    public void testSizeFailureIsQueryError() throws Exception {
        // the size of a full-fat hash join counts the master rows that find a
        // match, so it reads the master rows, which fail at x = 10
        assertFailureIsQueryError(
                "SELECT a.x FROM (" + FILTER_FAILS_AT_ROW_10 + ") a JOIN long_sequence(100) b ON a.x = b.x",
                true,
                RecordCursor::size
        );
    }

    @Test
    public void testSkipRowsFailureIsQueryError() throws Exception {
        assertFailureIsQueryError(FILTER_FAILS_AT_ROW_10, false, cursor -> {
            final RecordCursor.Counter rows = new RecordCursor.Counter();
            rows.set(20);
            cursor.skipRows(rows, RecordCursor.UNBOUNDED_ROW_COUNT);
        });
    }

    @Test
    public void testSuccessfulCallsAreNoQueryError() throws Exception {
        assertMemoryLeak(() -> {
            final HealthMetrics metrics = engine.getMetrics().healthMetrics();
            final long errorsBefore = metrics.queryErrorCounter();
            // the even numbers 2, 4, ..., 100
            try (RecordCursorFactory factory = select("SELECT x FROM long_sequence(100) WHERE x % 2 = 0")) {
                Assert.assertTrue(factory instanceof QueryProgress);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertEquals("a filter hides the size", -1, cursor.size());
                    final RecordCursor.Counter rows = new RecordCursor.Counter();
                    rows.set(10);
                    cursor.skipRows(rows, RecordCursor.UNBOUNDED_ROW_COUNT);
                    Assert.assertEquals("all 10 rows skipped", 0, rows.get());
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertEquals(22, cursor.getRecord().getLong(0));
                    cursor.calculateSize(sqlExecutionContext.getCircuitBreaker(), rows);
                    Assert.assertEquals("the rows after 22: 24, 26, ..., 100", 39, rows.get());
                    Assert.assertFalse(cursor.hasNext());
                }
            }
            Assert.assertEquals(errorsBefore, metrics.queryErrorCounter());
            assertNoQueryRegistered();
        });
    }

    private static void assertNoQueryRegistered() {
        final LongList ids = new LongList();
        engine.getQueryRegistry().getEntryIds(ids);
        Assert.assertEquals("registered queries: " + ids, 0, ids.size());
    }

    private void assertFailureIsQueryError(String sql, boolean isFullFatJoin, CursorCall call) throws Exception {
        assertMemoryLeak(() -> {
            final HealthMetrics metrics = engine.getMetrics().healthMetrics();
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                compiler.setFullFatJoins(isFullFatJoin);
                try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                    Assert.assertTrue(factory instanceof QueryProgress);
                    final long errorsBefore = metrics.queryErrorCounter();
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        try {
                            call.run(cursor);
                            Assert.fail("the call must fail");
                        } catch (ImplicitCastException e) {
                            TestUtils.assertEquals(FAILURE, e.getFlyweightMessage());
                        }
                        Assert.assertEquals("the failure is the query's error", errorsBefore + 1, metrics.queryErrorCounter());
                        assertNoQueryRegistered();
                    }
                    Assert.assertEquals("closing the cursor counts no second error", errorsBefore + 1, metrics.queryErrorCounter());

                    // the factory opens its cursor again, which reads the rows ahead of the failing one
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        Assert.assertTrue(cursor.hasNext());
                        Assert.assertEquals(1, cursor.getRecord().getLong(0));
                    }
                    Assert.assertEquals(errorsBefore + 1, metrics.queryErrorCounter());
                    assertNoQueryRegistered();
                }
            }
        });
    }

    @FunctionalInterface
    private interface CursorCall {
        void run(RecordCursor cursor);
    }
}
