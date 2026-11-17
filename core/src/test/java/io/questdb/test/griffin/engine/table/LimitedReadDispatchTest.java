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

import io.questdb.DefaultFactoryProvider;
import io.questdb.PropertyKey;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.StatefulAtom;
import io.questdb.cairo.sql.async.WorkStealingStrategy;
import io.questdb.cairo.sql.async.WorkStealingStrategyFactory;
import io.questdb.griffin.engine.table.AsyncFilteredRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncHorizonJoinProjectionRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncJitFilteredRecordCursorFactory;
import io.questdb.jit.JitUtil;
import io.questdb.test.AbstractCairoTest;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntSupplier;

/**
 * A LIMIT over a parallel filter or a HORIZON JOIN projection sizes the page frame tasks that it
 * keeps in flight to the rows its reader reads. No worker pool runs here: the query owner reduces
 * every frame itself, one at a time in frame order, so the dispatch limit and the number of
 * reduced frames follow from the rows read.
 * <p>
 * Table t holds one row with v = 0 per page frame: x runs from 1 to 100_000, each hourly
 * partition holds 1_000 rows and makes one page frame, and v = x % 1_000 is 0 on the last row of
 * each frame. The expected limits apply the rules of AdaptiveDispatchLimit with an initial limit
 * of 4: the larger of 4, the collected frames rounded down to a power of two and, when the reader
 * announced its rows, ceil(rowsLeft * collectedFrames / max(foundRows, 1)).
 */
public class LimitedReadDispatchTest extends AbstractCairoTest {
    private static final int INITIAL_LIMIT = 4;
    // The reduce task counter of the last page frame sequence that a cursor opened.
    private static volatile AtomicInteger reducedFrameCount;

    @BeforeClass
    public static void setUpStatic() throws Exception {
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_SHARD_COUNT, 1);
        // room for every frame of t in flight
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_REDUCE_QUEUE_CAPACITY, 128);
        AbstractCairoTest.setUpStatic();
    }

    @Override
    @Before
    public void setUp() {
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 1_000);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS, 1_000);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_ENABLED, "true");
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HORIZON_JOIN_ENABLED, "true");
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_DISPATCH_LIMIT, INITIAL_LIMIT);
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
    public void testCloseSkipsTheFramesTheReaderLeftUnstarted() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (RecordCursorFactory factory = select("SELECT * FROM t WHERE v = 0 LIMIT 0, 60")) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    // the first row comes from frame 0, while frames 1 to 3 wait in the queue
                    Assert.assertTrue(cursor.hasNext());
                }
                Assert.assertEquals(1, reducedFrameCount.get());

                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    assertRows(cursor, 60);
                    // frames 60 to 90 wait in the queue: from frame 32 on, the limit of 32 kept 32 frames in flight
                    Assert.assertEquals(32, dispatchLimitOf(factory).getAsInt());
                }
                Assert.assertEquals(60, reducedFrameCount.get());
            }
        });
    }

    @Test
    public void testHorizonJoinLimitSizesToTheRowsLeft() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // two offsets: each master row that passes the filter makes two rows
            final String horizon = "SELECT a.x, h.offset, b.y FROM t a HORIZON JOIN q b LIST (0s, 1s) AS h WHERE a.v = 0";
            try (RecordCursorFactory factory = select(horizon + " LIMIT 0, 60")) {
                final IntSupplier limit = dispatchLimitOf(factory);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertEquals(INITIAL_LIMIT, limit.getAsInt());
                    // frame 0 makes 2 rows: 58 rows left at 2 rows per frame take 29 frames
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertEquals(1_000, cursor.getRecord().getLong(0));
                    Assert.assertEquals(29, limit.getAsInt());
                    // 10 rows read from frames 0 to 4: 50 rows left take 25 frames
                    for (int i = 1; i < 10; i++) {
                        Assert.assertTrue(cursor.hasNext());
                    }
                    Assert.assertEquals(5_000, cursor.getRecord().getLong(0));
                    Assert.assertEquals(25, limit.getAsInt());

                    // toTop() restarts the read, and the LIMIT skips no row
                    cursor.toTop();
                    Assert.assertEquals(INITIAL_LIMIT, limit.getAsInt());
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertEquals(29, limit.getAsInt());
                }
            }
            try (RecordCursorFactory factory = select(horizon + " LIMIT 50, 60")) {
                final IntSupplier limit = dispatchLimitOf(factory);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    // 50 rows skipped in frames 0 to 24, the 51st read from frame 25: 8 rows left at
                    // 2 rows per frame take 4 frames, the 26 collected frames keep the limit at 16
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertEquals(26_000, cursor.getRecord().getLong(0));
                    Assert.assertEquals(16, limit.getAsInt());
                }
            }
        });
    }

    @Test
    public void testJavaFilterLimitSizesToTheRowsLeft() throws Exception {
        assertMemoryLeak(() -> {
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
            createTables();
            assertFilterLimitSizesToTheRowsLeft(AsyncFilteredRecordCursorFactory.class);
        });
    }

    @Test
    public void testJitFilterLimitSizesToTheRowsLeft() throws Exception {
        Assume.assumeTrue(JitUtil.isJitSupported());
        assertMemoryLeak(() -> {
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
            createTables();
            assertFilterLimitSizesToTheRowsLeft(AsyncJitFilteredRecordCursorFactory.class);
        });
    }

    @Test
    public void testLimitWithoutRowsGrowsWithTheFramesRead() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (RecordCursorFactory factory = select("SELECT * FROM t WHERE v = -1 LIMIT 0, 10")) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertFalse(cursor.hasNext());
                    // 10 rows at 1 row, as none was found, per 100 frames
                    Assert.assertEquals(1_000, dispatchLimitOf(factory).getAsInt());
                }
            }
        });
    }

    @Test
    public void testPushedDownLimitSizesToTheRowsLeft() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (RecordCursorFactory factory = select("SELECT * FROM t WHERE v = 0 LIMIT 60")) {
                final IntSupplier limit = dispatchLimitOf(factory);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertEquals(INITIAL_LIMIT, limit.getAsInt());
                    Assert.assertTrue(cursor.hasNext());
                    // 59 rows left at 1 row per frame
                    Assert.assertEquals(59, limit.getAsInt());
                }
            }
        });
    }

    @Test
    public void testReadOfEveryRowIsUnlimited() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (RecordCursorFactory factory = select("SELECT * FROM t WHERE v = 0")) {
                final IntSupplier limit = dispatchLimitOf(factory);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertEquals(Integer.MAX_VALUE, limit.getAsInt());
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertEquals(Integer.MAX_VALUE, limit.getAsInt());
                }
            }
            // the LIMIT reads every row past the fifth, as questdb-connect renders OFFSET 5
            try (RecordCursorFactory factory = select("SELECT * FROM t WHERE v = 0 LIMIT 5, 9_223_372_036_854_775_807")) {
                final IntSupplier limit = dispatchLimitOf(factory);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertEquals(INITIAL_LIMIT, limit.getAsInt());
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertEquals(6_000, cursor.getRecord().getLong(0));
                    Assert.assertEquals(Integer.MAX_VALUE, limit.getAsInt());
                }
            }
        });
    }

    private static void assertRows(RecordCursor cursor, int rowCount) {
        for (int i = 1; i <= rowCount; i++) {
            Assert.assertTrue(cursor.hasNext());
            Assert.assertEquals(i * 1_000L, cursor.getRecord().getLong(0));
        }
    }

    private static IntSupplier dispatchLimitOf(RecordCursorFactory factory) {
        for (RecordCursorFactory f = factory; f != null; f = f.getBaseFactory()) {
            if (f instanceof AsyncJitFilteredRecordCursorFactory jitFilter) {
                return jitFilter::getDispatchLimit;
            }
            if (f instanceof AsyncFilteredRecordCursorFactory filter) {
                return filter::getDispatchLimit;
            }
            if (f instanceof AsyncHorizonJoinProjectionRecordCursorFactory horizon) {
                return horizon::getDispatchLimit;
            }
        }
        throw new AssertionError("no parallel cursor under " + factory.getClass().getName());
    }

    private static void findFactory(RecordCursorFactory factory, Class<? extends RecordCursorFactory> factoryClass) {
        for (RecordCursorFactory f = factory; f != null; f = f.getBaseFactory()) {
            if (f.getClass() == factoryClass) {
                return;
            }
        }
        throw new AssertionError("no " + factoryClass.getName() + " under " + factory.getClass().getName());
    }

    private void assertFilterLimitSizesToTheRowsLeft(Class<? extends RecordCursorFactory> filterFactoryClass) throws Exception {
        try (RecordCursorFactory factory = select("SELECT * FROM t WHERE v = 0 LIMIT 0, 60")) {
            // the filter runs on the factory class the test asks for
            findFactory(factory, filterFactoryClass);
            final IntSupplier limit = dispatchLimitOf(factory);
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                // LimitRecordCursorFactory announced a limited read, no frame is collected yet
                Assert.assertEquals(INITIAL_LIMIT, limit.getAsInt());
                // the first row comes from frame 0: 59 rows left take 59 frames
                assertRows(cursor, 1);
                Assert.assertEquals(59, limit.getAsInt());
                assertRowsFrom(cursor, 2, 10);
                // 50 rows left, 10 frames collected
                Assert.assertEquals(50, limit.getAsInt());
                assertRowsFrom(cursor, 11, 60);
                // 1 row left at 1 row per frame: the 60 collected frames keep the limit at 32
                Assert.assertEquals(32, limit.getAsInt());
                Assert.assertFalse(cursor.hasNext());

                // toTop() restarts the read, and the LIMIT skips no row, which collects frame 0
                cursor.toTop();
                Assert.assertEquals(59, limit.getAsInt());
                assertRows(cursor, 10);
                Assert.assertEquals(50, limit.getAsInt());
            }
        }
    }

    private void assertRowsFrom(RecordCursor cursor, int first, int last) {
        for (int i = first; i <= last; i++) {
            Assert.assertTrue(cursor.hasNext());
            Assert.assertEquals(i * 1_000L, cursor.getRecord().getLong(0));
        }
    }

    private void createTables() throws Exception {
        execute("""
                CREATE TABLE t AS (
                    SELECT x, x % 1_000 v, timestamp_sequence(0, 3_600_000) ts
                    FROM long_sequence(100_000)
                ) TIMESTAMP(ts) PARTITION BY HOUR
                """);
        execute("""
                CREATE TABLE q AS (
                    SELECT x y, timestamp_sequence(0, 3_600_000) ts
                    FROM long_sequence(100_000)
                ) TIMESTAMP(ts) PARTITION BY HOUR
                """);
    }
}
