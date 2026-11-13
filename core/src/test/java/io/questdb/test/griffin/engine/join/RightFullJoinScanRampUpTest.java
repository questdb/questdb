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

import io.questdb.DefaultFactoryProvider;
import io.questdb.PropertyKey;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.StatefulAtom;
import io.questdb.cairo.sql.async.WorkStealingStrategy;
import io.questdb.cairo.sql.async.WorkStealingStrategyFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.Plannable;
import io.questdb.griffin.TextPlanSink;
import io.questdb.griffin.engine.table.AsyncFilteredRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncJitFilteredRecordCursorFactory;
import io.questdb.jit.JitUtil;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * The scan ahead of a RIGHT or FULL join takes no LIMIT, so a LIMIT above the join reads it until
 * the join has its rows. Its parallel filter starts with page frames of
 * cairo.sql.page.frame.ramp.start.rows rows and grows them, so that the join returns its first
 * rows without waiting for the filter to scan a regular frame. Reads that do not stop early, and
 * filters that feed no RIGHT or FULL join, keep their regular frames.
 * <p>
 * No worker pool runs here: the query owner reduces the frames itself, so a LIMIT that the rows of
 * the first frame satisfy leaves one frame reduced. t holds one day of 1_000 rows, and with
 * cairo.sql.page.frame.min.rows and max.rows of 1_000 a regular frame holds the whole day. The test
 * execution context runs one shared query worker, so from a first frame of 10 rows each frame holds
 * as many rows as the frames before it: 10, 10, 20, 40, 80, 160, 320 and the 360 rows left. t.k is
 * id % 5, so the ids that match w keys 1 and 2 are 1, 2, 6, 7, 11, 12 and so on, and no t row
 * matches w key 7.
 */
public class RightFullJoinScanRampUpTest extends AbstractCairoTest {
    private static final String FULL_JOIN = "SELECT t.id, w.k FROM t FULL JOIN w ON t.k = w.k WHERE t.x > 0";
    private static final String RAMPED_FRAMES = "10 10 20 40 80 160 320 360";
    private static final String REGULAR_FRAMES = "1000";
    private static final String RIGHT_JOIN = "SELECT t.id, w.k FROM t RIGHT JOIN w ON t.k = w.k WHERE t.x > 0";
    // The reduce task counter of the last page frame sequence that a cursor opened.
    private static volatile AtomicInteger reducedFrameCount;

    @BeforeClass
    public static void setUpStatic() throws Exception {
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_SHARD_COUNT, 1);
        // room for every frame of t in flight
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_REDUCE_QUEUE_CAPACITY, 16);
        AbstractCairoTest.setUpStatic();
    }

    @Override
    @Before
    public void setUp() {
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 1_000);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS, 1_000);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_ENABLED, "true");
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_DISPATCH_LIMIT, 4);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_RAMP_START_ROWS, 10);
        // The page frame sequence hands its reduce task counter to the strategy it binds.
        factoryProvider = new DefaultFactoryProvider() {
            @Override
            public @NotNull WorkStealingStrategy getWorkStealingStrategy(
                    @NotNull CairoConfiguration configuration,
                    int workerCount,
                    @NotNull StatefulAtom atom
            ) {
                final WorkStealingStrategy strategy = WorkStealingStrategyFactory.getInstance(configuration, workerCount);
                return new WorkStealingStrategy() {
                    @Override
                    public WorkStealingStrategy of(AtomicInteger startedCounter) {
                        reducedFrameCount = startedCounter;
                        strategy.of(startedCounter);
                        return this;
                    }

                    @Override
                    public void onBeforeDirectSteal() {
                        strategy.onBeforeDirectSteal();
                    }

                    @Override
                    public boolean shouldSteal(int finishedCount) {
                        return strategy.shouldSteal(finishedCount);
                    }
                };
            }
        };
        super.setUp();
    }

    @Test
    public void testBindVariableLimitOnOneFactory() throws Exception {
        // the same factory runs with a positive, a negative and a positive LIMIT again, as a
        // prepared statement does: only the reads of the first rows ramp up
        assertMemoryLeak(() -> {
            createTables();
            bindVariableService.clear();
            bindVariableService.setLong(0, 3);
            try (RecordCursorFactory factory = select(RIGHT_JOIN + " LIMIT $1")) {
                assertReads(factory, "1\t1\n2\t2\n6\t1\n", RAMPED_FRAMES, 1);
                bindVariableService.setLong(0, -3);
                assertReads(factory, "992\t2\n996\t1\n997\t2\n", REGULAR_FRAMES, 1);
                bindVariableService.setLong(0, 3);
                assertReads(factory, "1\t1\n2\t2\n6\t1\n", RAMPED_FRAMES, 1);
            }
        });
    }

    @Test
    public void testDefaultRampStart() throws Exception {
        // cairo.sql.page.frame.ramp.start.rows defaults to 1_000 rows: one day of 100_000 rows in
        // regular frames of 100_000 rows ramps up from 1_000 rows
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_RAMP_START_ROWS, (String) null);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 100_000);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS, 100_000);
        assertMemoryLeak(() -> {
            // an embedded engine defaults to the same 1_000 rows
            Assert.assertEquals(1_000, new DefaultCairoConfiguration(root).getSqlPageFrameRampStartRows());
            createTables(100_000);
            try (RecordCursorFactory factory = select(RIGHT_JOIN + " LIMIT 3")) {
                assertReads(factory, "1\t1\n2\t2\n6\t1\n", "1000 1000 2000 4000 8000 16000 32000 36000", 1);
            }
        });
    }

    @Test
    public void testEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            createTables(0);
            try (RecordCursorFactory factory = select(RIGHT_JOIN + " LIMIT 3")) {
                final AsyncScanFilter scanFilter = new AsyncScanFilter(findScanFilter(factory));
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    // the WHERE drops the w rows that the RIGHT join NULL-extends
                    assertRows(factory, cursor, "");
                    Assert.assertEquals("", scanFilter.frames());
                    // the filter, which has no frame to collect, asks for its frames again, and the
                    // ramp-up leaves the frames that it listed once alone
                    cursor.toTop();
                    assertRows(factory, cursor, "");
                    Assert.assertEquals("", scanFilter.frames());
                }
            }
        });
    }

    @Test
    public void testFiltersOutsideRightAndFullJoinsKeepRegularFrames() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // the filter takes the LIMIT itself
            try (RecordCursorFactory factory = select("SELECT id, k FROM t WHERE x > 0 LIMIT 3")) {
                assertReads(factory, "1\t1\n2\t2\n3\t3\n", REGULAR_FRAMES, 1);
            }
            // the filter takes the LIMIT as advice
            try (RecordCursorFactory factory = select("SELECT t.id, w.k FROM t LEFT JOIN w ON t.k = w.k WHERE t.x > 0 LIMIT 3")) {
                assertReads(factory, "1\t1\n2\t2\n3\tnull\n", REGULAR_FRAMES, 1);
            }
            // LIMIT lo, hi gives the filter no advice, and the join reads it until the LIMIT stops
            try (RecordCursorFactory factory = select("SELECT t.id, w.k FROM t JOIN w ON t.k = w.k WHERE t.x > 0 LIMIT 1, 4")) {
                assertReads(factory, "2\t2\n6\t1\n7\t2\n", REGULAR_FRAMES, 1);
            }
            // an INNER join, or a filter after a LEFT join, drops rows, so the filter takes no LIMIT advice,
            // see JoinScanLimitTest, and the join reads it until the LIMIT stops
            try (RecordCursorFactory factory = select("SELECT t.id, w.k FROM t JOIN w ON t.k = w.k WHERE t.x > 0 LIMIT 3")) {
                assertReads(factory, "1\t1\n2\t2\n6\t1\n", REGULAR_FRAMES, 1);
            }
            try (RecordCursorFactory factory = select("SELECT t.id, w.k FROM t LEFT JOIN w ON t.k = w.k WHERE t.x > 0 AND w.k > 0 LIMIT 3")) {
                assertReads(factory, "1\t1\n2\t2\n6\t1\n", REGULAR_FRAMES, 1);
            }
        });
    }

    @Test
    public void testFullJoinLimitRampsUpScanFrames() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // every row of t passes the filter, and the rows that match no w key come NULL-extended
            try (RecordCursorFactory factory = select(FULL_JOIN + " LIMIT 3")) {
                assertReads(factory, "1\t1\n2\t2\n3\tnull\n", RAMPED_FRAMES, 1);
            }
            try (RecordCursorFactory factory = select(FULL_JOIN + " LIMIT 8, 10")) {
                assertReads(factory, "9\tnull\n10\tnull\n", RAMPED_FRAMES, 1);
            }
        });
    }

    @Test
    public void testJavaFilterRampsUpScanFrames() throws Exception {
        assertMemoryLeak(() -> {
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
            createTables();
            try (RecordCursorFactory factory = select(RIGHT_JOIN + " LIMIT 3")) {
                Assert.assertTrue(findScanFilter(factory) instanceof AsyncFilteredRecordCursorFactory);
                assertReads(factory, "1\t1\n2\t2\n6\t1\n", RAMPED_FRAMES, 1);
            }
            try (RecordCursorFactory factory = select(FULL_JOIN + " LIMIT 3")) {
                assertReads(factory, "1\t1\n2\t2\n3\tnull\n", RAMPED_FRAMES, 1);
            }
        });
    }

    @Test
    public void testJitFilterRampsUpScanFrames() throws Exception {
        Assume.assumeTrue(JitUtil.isJitSupported());
        assertMemoryLeak(() -> {
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
            createTables();
            try (RecordCursorFactory factory = select(RIGHT_JOIN + " LIMIT 3")) {
                Assert.assertTrue(findScanFilter(factory) instanceof AsyncJitFilteredRecordCursorFactory);
                assertReads(factory, "1\t1\n2\t2\n6\t1\n", RAMPED_FRAMES, 1);
            }
            try (RecordCursorFactory factory = select(RIGHT_JOIN + " LIMIT 1, 4")) {
                assertReads(factory, "2\t2\n6\t1\n7\t2\n", RAMPED_FRAMES, 1);
            }
        });
    }

    @Test
    public void testLimitPastFirstFramesReadsRampedFramesInOrder() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // rows 156 to 160 of the join are ids 387, 391, 392, 396 and 397, rows 386 to 396 of t,
            // which frame 6 holds; the owner may reduce the frames it dispatched ahead of the reader
            // as well, so the test leaves their count unchecked
            try (RecordCursorFactory factory = select(RIGHT_JOIN + " LIMIT 155, 160")) {
                assertReads(factory, "387\t2\n391\t1\n392\t2\n396\t1\n397\t2\n", RAMPED_FRAMES, -1);
            }
        });
    }

    @Test
    public void testRampUpOff() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_RAMP_START_ROWS, 0);
        assertMemoryLeak(() -> {
            createTables();
            try (RecordCursorFactory factory = select(RIGHT_JOIN + " LIMIT 3")) {
                assertReads(factory, "1\t1\n2\t2\n6\t1\n", REGULAR_FRAMES, 1);
            }
        });
    }

    @Test
    public void testReadAgainKeepsRampedFrames() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (RecordCursorFactory factory = select(RIGHT_JOIN + " LIMIT 3")) {
                final AsyncScanFilter scanFilter = new AsyncScanFilter(findScanFilter(factory));
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    assertRows(factory, cursor, "1\t1\n2\t2\n6\t1\n");
                    // toTop() reads the frames that the cursor listed again
                    cursor.toTop();
                    assertRows(factory, cursor, "1\t1\n2\t2\n6\t1\n");
                    Assert.assertEquals(RAMPED_FRAMES, scanFilter.frames());
                }
                // the next cursor lists its frames anew
                assertReads(factory, "1\t1\n2\t2\n6\t1\n", RAMPED_FRAMES, 1);
            }
        });
    }

    @Test
    public void testReadsThatDoNotStopEarlyKeepRegularFrames() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // no LIMIT
            try (RecordCursorFactory factory = select("SELECT count() FROM (" + RIGHT_JOIN + ")")) {
                assertReads(factory, "400\n", REGULAR_FRAMES, 1);
            }
            // LIMIT -3 reads every row to find the last ones
            try (RecordCursorFactory factory = select(RIGHT_JOIN + " LIMIT -3")) {
                assertReads(factory, "992\t2\n996\t1\n997\t2\n", REGULAR_FRAMES, 1);
            }
            // the sort reads every row before the LIMIT
            try (RecordCursorFactory factory = select(RIGHT_JOIN + " ORDER BY t.id DESC LIMIT 3")) {
                assertReads(factory, "997\t2\n996\t1\n992\t2\n", REGULAR_FRAMES, 1);
            }
        });
    }

    private static void assertRows(RecordCursorFactory factory, RecordCursor cursor, String expected) {
        final StringSink sink = new StringSink();
        final Record record = cursor.getRecord();
        while (cursor.hasNext()) {
            TestUtils.println(record, factory.getMetadata(), sink);
        }
        TestUtils.assertEquals(expected, sink);
    }

    private static RecordCursorFactory findScanFilter(RecordCursorFactory factory) {
        final RecordCursorFactory[] found = new RecordCursorFactory[1];
        final TextPlanSink sink = new TextPlanSink() {
            @Override
            public PlanSink child(Plannable p) {
                if (p instanceof AsyncJitFilteredRecordCursorFactory || p instanceof AsyncFilteredRecordCursorFactory) {
                    Assert.assertNull("one parallel filter", found[0]);
                    found[0] = (RecordCursorFactory) p;
                }
                return super.child(p);
            }
        };
        sink.of(factory, sqlExecutionContext);
        Assert.assertNotNull("no parallel filter under " + factory.getClass().getName(), found[0]);
        return found[0];
    }

    // reads every row of the factory, which returns the expected rows, and checks the page frames
    // of its parallel filter and, unless the expected count is -1, the frames that the owner reduced
    // before the read ended
    private void assertReads(RecordCursorFactory factory, String expectedRows, String expectedFrames, int expectedReducedFrameCount) throws Exception {
        final AsyncScanFilter scanFilter = new AsyncScanFilter(findScanFilter(factory));
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            assertRows(factory, cursor, expectedRows);
            Assert.assertEquals(expectedFrames, scanFilter.frames());
            if (expectedReducedFrameCount > -1) {
                Assert.assertEquals(expectedReducedFrameCount, reducedFrameCount.get());
            }
        }
    }

    private void createTables() throws Exception {
        createTables(1_000);
    }

    private void createTables(int rowCount) throws Exception {
        execute("CREATE TABLE t AS (SELECT x::INT id, (x % 5)::INT k, x::INT x, timestamp_sequence('2024-01-01'::TIMESTAMP, 100_000) ts"
                + " FROM long_sequence(" + rowCount + ")) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
        execute("CREATE TABLE w (k INT)");
        execute("INSERT INTO w VALUES (1), (2), (7)");
    }

    // the page frames that the open cursor of a parallel filter lists, by their rows
    private record AsyncScanFilter(RecordCursorFactory factory) {
        String frames() {
            final StringSink sink = new StringSink();
            final int frameCount = factory instanceof AsyncJitFilteredRecordCursorFactory jit
                    ? jit.getFrameCount()
                    : ((AsyncFilteredRecordCursorFactory) factory).getFrameCount();
            for (int i = 0; i < frameCount; i++) {
                if (i > 0) {
                    sink.put(' ');
                }
                sink.put(factory instanceof AsyncJitFilteredRecordCursorFactory jit
                        ? jit.getFrameRowCount(i)
                        : ((AsyncFilteredRecordCursorFactory) factory).getFrameRowCount(i));
            }
            return sink.toString();
        }
    }
}
