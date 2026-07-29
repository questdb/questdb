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

package io.questdb.test.griffin.engine.orderby;

import io.questdb.PropertyKey;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.orderby.LimitedSizeSortedLightRecordCursorFactory;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Covers {@link io.questdb.griffin.engine.orderby.LimitedSizeSortedLightRecordCursorFactory} re-execution.
 * The factory picks its cursor implementation once but re-derives the limits from bind variables on every
 * execution, so a cached plan can hand a last-N limit to the first-N cursor.
 * <p>
 * {@code cairo.sql.orderby.sort.enabled=false} is required: with the default {@code true},
 * {@code SqlCodeGenerator} emits {@code EncodedSortLimitedLightRecordCursorFactory} instead.
 */
public class LimitedSizeSortedLightRecordCursorFactoryTest extends AbstractCairoTest {

    @Override
    public void setUp() {
        node1.setProperty(PropertyKey.CAIRO_SQL_ORDER_BY_SORT_ENABLED, false);
        node1.setProperty(PropertyKey.CAIRO_SQL_PARALLEL_TOP_K_ENABLED, false);
        super.setUp();
    }

    @Test
    public void testCachedFactoryHandlesLimitSignFlip() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE y AS (
                                SELECT x i, timestamp_sequence(0, 60_000_000L) ts, x::CHAR c
                                FROM long_sequence(10)
                            ) TIMESTAMP(ts) PARTITION BY DAY"""
            );

            bindVariableService.setLong(0, 3L);
            try (RecordCursorFactory factory = select("SELECT i FROM y ORDER BY ts, c LIMIT $1")) {
                // Pin the routing. Without this the test still passes if codegen sends the query to
                // the encoded-sort or top-K factory instead - both re-derive the direction per
                // execution and so handle the flip - leaving the legacy factory uncovered.
                assertFactoryChainContains(factory, LimitedSizeSortedLightRecordCursorFactory.class);
                assertPartiallySortedCursor(factory, "lo: $0::long");

                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    assertCursorTwoPass("i\n1\n2\n3\n", cursor, factory.getMetadata());
                }

                // Same factory, opposite sign: last three rows, not the first three of a truncated
                // scan. The second pass re-runs toTop(), which re-derives rowsLeft from the skips
                // updateLimits() just rewrote.
                bindVariableService.setLong(0, -3L);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    assertCursorTwoPass("i\n8\n9\n10\n", cursor, factory.getMetadata());
                }
            }
        });
    }

    @Test
    public void testCachedFactoryHandlesLoHiSignFlip() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE y AS (
                                SELECT x i, timestamp_sequence(0, 60_000_000L) ts, x::CHAR c
                                FROM long_sequence(10)
                            ) TIMESTAMP(ts) PARTITION BY DAY"""
            );

            bindVariableService.setLong(0, 0L);
            bindVariableService.setLong(1, 3L);
            try (RecordCursorFactory factory = select("SELECT i FROM y ORDER BY ts, c LIMIT $1, $2")) {
                assertFactoryChainContains(factory, LimitedSizeSortedLightRecordCursorFactory.class);
                assertPartiallySortedCursor(factory, "lo: $0::long hi: $1::long");

                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    assertCursorTwoPass("i\n1\n2\n3\n", cursor, factory.getMetadata());
                }

                // The two-bind analogue of the one-bind flip above, and the only shape that pairs a
                // last-N cursor with a positive retained limit: lo = -5, hi = -2 makes
                // computeLimits() derive limit = 5 and skipLast = 2 with isFirstN = false. The old
                // `limit >= 0` guard therefore still fired the first-N early stop, cutting the scan
                // at row 6 and emitting the head of the range instead of its tail. The other legacy
                // two-bind shape, lo >= 0 with hi < 0, sets limit = -1 and so never reached it.
                bindVariableService.setLong(0, -5L);
                bindVariableService.setLong(1, -2L);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    assertCursorTwoPass("i\n6\n7\n8\n", cursor, factory.getMetadata());
                }
            }
        });
    }

    private static void assertFactoryChainContains(RecordCursorFactory factory, Class<?> expected) {
        for (RecordCursorFactory f = factory; f != null; f = f.getBaseFactory()) {
            if (expected.isInstance(f)) {
                return;
            }
        }
        Assert.fail("factory chain does not contain " + expected.getSimpleName()
                + "; the query is no longer routed to the factory under test");
    }

    /**
     * Asserts the factory will hand out the partially sorted cursor, not just that the factory itself
     * is the one under test. It picks between {@code LimitedSizePartiallySortedLightRecordCursor} and
     * {@code LimitedSizeSortedLightRecordCursor} once, off the same
     * {@code baseCursorTimestampIndex} the plan reports as {@code partiallySorted}, and only the
     * partially sorted one stops the base scan early - the fully sorted one drains it either way and
     * cannot reproduce the truncated scan. A codegen change that dropped the pre-sorted-timestamp
     * detection would otherwise silently downgrade these tests to the bug-free cursor.
     */
    private void assertPartiallySortedCursor(RecordCursorFactory factory, String limits) {
        planSink.clear();
        planSink.of(factory, sqlExecutionContext);
        TestUtils.assertContains(planSink.getSink(), "Sort light " + limits + " partiallySorted: true");
    }
}
