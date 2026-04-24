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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.TimeFrame;
import io.questdb.cairo.sql.TimeFrameCursor;
import io.questdb.griffin.engine.QueryProgress;
import io.questdb.griffin.engine.table.ConcurrentTimeFrameCursor;
import io.questdb.griffin.engine.table.ConcurrentTimeFrameState;
import io.questdb.griffin.engine.table.TablePageFrameCursor;
import io.questdb.griffin.engine.table.TimeFrameCursorImpl;
import io.questdb.mp.WorkerPool;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.Rows;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.cairo.fuzz.FailureFileFacade;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

public class TimeFrameCursorTest extends AbstractCairoTest {

    @Test
    public void testAlreadyOpenPartitionFastPath() throws Exception {
        assertMemoryLeak(() -> {
            // 72 rows across 3 partitions.
            execute("CREATE TABLE x AS (SELECT" +
                    " rnd_int() a," +
                    " timestamp_sequence(0, 60 * 60 * 1_000_000L) t" +
                    " FROM long_sequence(72)" +
                    ") TIMESTAMP (t) PARTITION BY DAY");

            // Force-open partition 0 via a regular query so that the table reader
            // has it open before we create the time frame cursor. This exercises
            // the addOpenPartitionFrames() fast path for already-open partitions.
            assertSql("count\n24\n", "SELECT count() FROM x WHERE t < '1970-01-02'");

            try (RecordCursorFactory factory = select("x")) {
                Assert.assertTrue(factory.supportsTimeFrameCursor());
                // Collect expected data via the SQL engine.
                TestUtils.printSql(engine, sqlExecutionContext, "x", sink);
                RecordMetadata metadata = factory.getMetadata();

                // TimeFrameCursorImpl: partition 0 is already open, partitions 1-2 are lazy.
                try (TimeFrameCursor cursor = factory.getTimeFrameCursor(sqlExecutionContext)) {
                    assertForwardScanWithColumnData(cursor, metadata, sink);
                }
                // ConcurrentTimeFrameCursorImpl: same scenario.
                testWithConcurrentCursor(factory, cursor -> assertForwardScanWithColumnData(cursor, metadata, sink));
            }
            execute("DROP TABLE x");
        });
    }

    @Test
    public void testBuildFrameCacheDoesNotOpenPartitions() throws Exception {
        assertMemoryLeak(() -> {
            // 72 rows across 3 partitions (days 1970-01-01, 1970-01-02, 1970-01-03).
            execute("CREATE TABLE x AS (SELECT" +
                    " rnd_int() a," +
                    " timestamp_sequence(0, 60 * 60 * 1_000_000L) t" +
                    " FROM long_sequence(72)" +
                    ") TIMESTAMP (t) PARTITION BY DAY");

            try (RecordCursorFactory factory = select("x")) {
                RecordCursorFactory baseFactory = factory instanceof QueryProgress ? factory.getBaseFactory() : factory;
                TablePageFrameCursor pageFrameCursor = (TablePageFrameCursor) baseFactory.getPageFrameCursor(
                        sqlExecutionContext,
                        PartitionFrameCursorFactory.ORDER_ASC
                );
                TableReader reader = pageFrameCursor.getTableReader();
                RecordMetadata metadata = baseFactory.getMetadata();

                try (TimeFrameCursorImpl cursor = new TimeFrameCursorImpl(configuration, metadata)) {
                    cursor.of(
                            pageFrameCursor,
                            sqlExecutionContext.getPageFrameMinRows(),
                            sqlExecutionContext.getPageFrameMaxRows(),
                            1
                    );

                    Assert.assertEquals(0, reader.getOpenPartitionCount());

                    // Iterating next() builds the frame cache but must not open partitions.
                    int frameCount = 0;
                    while (cursor.next()) {
                        frameCount++;
                        Assert.assertEquals(0, reader.getOpenPartitionCount());
                    }
                    Assert.assertTrue(frameCount > 0);

                    // Opening frames must open partitions lazily.
                    cursor.toTop();
                    Assert.assertTrue(cursor.next());
                    cursor.open();
                    Assert.assertTrue(reader.getOpenPartitionCount() > 0);
                }
            }
            execute("DROP TABLE x");
        });
    }

    @Test
    public void testConcurrentStatePartitionOpeningFromMultipleThreads() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (SELECT" +
                    " rnd_int() a," +
                    " timestamp_sequence(0, 60 * 60 * 1_000_000L) t" +
                    " FROM long_sequence(120)" +
                    ") TIMESTAMP (t) PARTITION BY DAY");

            try (RecordCursorFactory factory = select("x")) {
                RecordCursorFactory baseFactory = factory instanceof QueryProgress ? factory.getBaseFactory() : factory;
                ConcurrentTimeFrameState sharedState = new ConcurrentTimeFrameState();
                try {
                    TablePageFrameCursor pageFrameCursor = (TablePageFrameCursor) baseFactory.getPageFrameCursor(
                            sqlExecutionContext,
                            PartitionFrameCursorFactory.ORDER_ASC
                    );
                    RecordMetadata metadata = baseFactory.getMetadata();
                    sharedState.of(
                            pageFrameCursor,
                            metadata,
                            pageFrameCursor.getColumnMapping(),
                            pageFrameCursor.isExternal(),
                            sqlExecutionContext.getPageFrameMinRows(),
                            sqlExecutionContext.getPageFrameMaxRows(),
                            sqlExecutionContext.getSharedQueryWorkerCount()
                    );

                    int partitionCount = sharedState.getPartitionCount();
                    Assert.assertTrue(partitionCount >= 5);

                    // Open all partitions concurrently from multiple threads.
                    // Even threads iterate forward, odd threads iterate backward
                    // to maximize contention on the same partitions.
                    int threadCount = 4;
                    CyclicBarrier barrier = new CyclicBarrier(threadCount);
                    AtomicInteger errors = new AtomicInteger(0);
                    Thread[] threads = new Thread[threadCount];
                    for (int i = 0; i < threadCount; i++) {
                        final boolean isForward = (i % 2) == 0;
                        threads[i] = new Thread(() -> {
                            try {
                                barrier.await();
                                if (isForward) {
                                    for (int p = 0; p < partitionCount; p++) {
                                        sharedState.ensurePartitionOpened(p);
                                    }
                                } else {
                                    for (int p = partitionCount - 1; p >= 0; p--) {
                                        sharedState.ensurePartitionOpened(p);
                                    }
                                }
                            } catch (Throwable e) {
                                errors.incrementAndGet();
                            }
                        });
                        threads[i].start();
                    }
                    for (Thread thread : threads) {
                        thread.join();
                    }
                    Assert.assertEquals(0, errors.get());

                    // Verify cursor reads all data correctly after concurrent opening,
                    // including column values (not just row count).
                    TestUtils.printSql(engine, sqlExecutionContext, "x", sink);
                    ConcurrentTimeFrameCursor cursor = baseFactory.newTimeFrameCursor();
                    Assert.assertNotNull(cursor);
                    try {
                        cursor.of(sharedState, pageFrameCursor, metadata.getTimestampIndex());
                        assertForwardScanWithColumnData(cursor, metadata, sink);
                    } finally {
                        Misc.free(cursor);
                    }
                } finally {
                    Misc.free(sharedState);
                }
            }
            execute("DROP TABLE x");
        });
    }

    @Test
    public void testConcurrentStateRetryAfterFailedPartitionOpen() throws Exception {
        FailureFileFacade failureFf = new FailureFileFacade(TestFilesFacadeImpl.INSTANCE);
        assertMemoryLeak(failureFf, () -> {
            execute("CREATE TABLE x AS (SELECT" +
                    " rnd_int() a," +
                    " timestamp_sequence(0, 60 * 60 * 1_000_000L) t" +
                    " FROM long_sequence(49)" +
                    ") TIMESTAMP (t) PARTITION BY DAY");

            try (RecordCursorFactory factory = select("x")) {
                RecordCursorFactory baseFactory = factory instanceof QueryProgress
                        ? factory.getBaseFactory() : factory;
                ConcurrentTimeFrameState sharedState = new ConcurrentTimeFrameState();
                ConcurrentTimeFrameCursor cursor = baseFactory.newTimeFrameCursor();
                Assert.assertNotNull(cursor);
                try {
                    TablePageFrameCursor pageFrameCursor = (TablePageFrameCursor) baseFactory.getPageFrameCursor(
                            sqlExecutionContext,
                            PartitionFrameCursorFactory.ORDER_ASC
                    );
                    RecordMetadata metadata = baseFactory.getMetadata();
                    sharedState.of(
                            pageFrameCursor,
                            metadata,
                            pageFrameCursor.getColumnMapping(),
                            pageFrameCursor.isExternal(),
                            sqlExecutionContext.getPageFrameMinRows(),
                            sqlExecutionContext.getPageFrameMaxRows(),
                            sqlExecutionContext.getSharedQueryWorkerCount()
                    );
                    cursor.of(sharedState, pageFrameCursor, metadata.getTimestampIndex());

                    // Verify pre-computed frame count is positive.
                    Assert.assertTrue(sharedState.getFrameCount() > 0);

                    // Partition 0: open successfully.
                    sharedState.ensurePartitionOpened(0);

                    // Partition 1: arm failure so the next file op fails.
                    failureFf.setToFailAfter(1);
                    try {
                        sharedState.ensurePartitionOpened(1);
                        Assert.fail("Expected CairoException");
                    } catch (CairoException expected) {
                        // Partition open failed as expected.
                    }

                    // Partition 1: retry should succeed.
                    sharedState.ensurePartitionOpened(1);

                    // Verify cursor navigates and opens all partitions correctly.
                    Assert.assertTrue(cursor.next());
                    Assert.assertTrue(cursor.open() > 0);
                    Assert.assertTrue(cursor.next());
                    Assert.assertTrue(cursor.open() > 0);
                    Assert.assertTrue(cursor.next());
                    Assert.assertTrue(cursor.open() > 0);
                    Assert.assertFalse(cursor.next());
                } finally {
                    Misc.free(cursor);
                    Misc.free(sharedState);
                }
            }
            execute("DROP TABLE x");
        });
    }

    @Test
    public void testConcurrentStateWithIntervalFilter() throws Exception {
        assertMemoryLeak(() -> {
            // 72 rows across 3 partitions (days 1970-01-01, 1970-01-02, 1970-01-03).
            execute("CREATE TABLE x AS (SELECT" +
                    " rnd_int() a," +
                    " timestamp_sequence(0, 60 * 60 * 1_000_000L) t" +
                    " FROM long_sequence(72)" +
                    ") TIMESTAMP (t) PARTITION BY DAY");

            // Timestamp filter narrows to 2 partitions (48 rows).
            // This triggers the eager fallback in ConcurrentTimeFrameState.of().
            try (RecordCursorFactory factory = select("x WHERE t < '1970-01-03'")) {
                RecordCursorFactory baseFactory = factory instanceof QueryProgress ? factory.getBaseFactory() : factory;
                ConcurrentTimeFrameState sharedState = new ConcurrentTimeFrameState();
                ConcurrentTimeFrameCursor cursor = baseFactory.newTimeFrameCursor();
                Assert.assertNotNull(cursor);
                try {
                    TablePageFrameCursor pageFrameCursor = (TablePageFrameCursor) baseFactory.getPageFrameCursor(
                            sqlExecutionContext,
                            PartitionFrameCursorFactory.ORDER_ASC
                    );
                    Assert.assertTrue("expected interval filter", pageFrameCursor.hasIntervalFilter());

                    RecordMetadata metadata = baseFactory.getMetadata();
                    sharedState.of(
                            pageFrameCursor,
                            metadata,
                            pageFrameCursor.getColumnMapping(),
                            pageFrameCursor.isExternal(),
                            sqlExecutionContext.getPageFrameMinRows(),
                            sqlExecutionContext.getPageFrameMaxRows(),
                            sqlExecutionContext.getSharedQueryWorkerCount()
                    );
                    cursor.of(sharedState, pageFrameCursor, metadata.getTimestampIndex());

                    Assert.assertTrue(sharedState.getFrameCount() > 0);
                    TestUtils.printSql(engine, sqlExecutionContext, "x WHERE t < '1970-01-03'", sink);
                    assertForwardScanWithColumnData(cursor, metadata, sink);
                } finally {
                    Misc.free(cursor);
                    Misc.free(sharedState);
                }
            }
            execute("DROP TABLE x");
        });
    }

    @Test
    public void testCursorRetryAfterFailedPartitionOpen() throws Exception {
        FailureFileFacade failureFf = new FailureFileFacade(TestFilesFacadeImpl.INSTANCE);
        assertMemoryLeak(failureFf, () -> {
            execute("CREATE TABLE x AS (SELECT" +
                    " rnd_int() a," +
                    " timestamp_sequence(0, 60 * 60 * 1_000_000L) t" +
                    " FROM long_sequence(49)" +
                    ") TIMESTAMP (t) PARTITION BY DAY");

            try (RecordCursorFactory factory = select("x")) {
                Assert.assertTrue(factory.supportsTimeFrameCursor());
                try (TimeFrameCursor cursor = factory.getTimeFrameCursor(sqlExecutionContext)) {
                    TimeFrame frame = cursor.getTimeFrame();

                    // Navigate to first frame and open it successfully.
                    Assert.assertTrue(cursor.next());
                    cursor.open();
                    Assert.assertTrue(frame.getRowHi() - frame.getRowLo() > 0);

                    // Navigate to second frame — arm failure so the next file op fails.
                    Assert.assertTrue(cursor.next());
                    failureFf.setToFailAfter(1);
                    try {
                        cursor.open();
                        Assert.fail("Expected CairoException");
                    } catch (CairoException expected) {
                        // Partition open failed as expected.
                    }

                    // Retry should succeed.
                    cursor.open();
                    Assert.assertTrue(frame.getRowHi() - frame.getRowLo() > 0);
                }
            }
            execute("DROP TABLE x");
        });
    }

    @Test
    public void testDroppedPartitionSkippedInLazyPath() throws Exception {
        assertMemoryLeak(() -> {
            // 72 rows across 3 partitions (days 1970-01-01, 1970-01-02, 1970-01-03).
            execute("CREATE TABLE x AS (SELECT" +
                    " rnd_int() a," +
                    " timestamp_sequence(0, 60 * 60 * 1_000_000L) t" +
                    " FROM long_sequence(72)" +
                    ") TIMESTAMP (t) PARTITION BY DAY");
            // Drop the middle partition, leaving an empty gap.
            execute("ALTER TABLE x DROP PARTITION LIST '1970-01-02'");

            try (RecordCursorFactory factory = select("x")) {
                Assert.assertTrue(factory.supportsTimeFrameCursor());
                // Test TimeFrameCursorImpl
                try (TimeFrameCursor cursor = factory.getTimeFrameCursor(sqlExecutionContext)) {
                    assertForwardScan(cursor, 48);
                }
                // Test ConcurrentTimeFrameCursorImpl
                testWithConcurrentCursor(factory, cursor -> assertForwardScan(cursor, 48));
            }
            execute("DROP TABLE x");
        });
    }

    @Test
    public void testEagerFallbackWithIntervalFilter() throws Exception {
        assertMemoryLeak(() -> {
            // 72 rows across 3 partitions (days 1970-01-01, 1970-01-02, 1970-01-03).
            execute("CREATE TABLE x AS (SELECT" +
                    " rnd_int() a," +
                    " timestamp_sequence(0, 60 * 60 * 1_000_000L) t" +
                    " FROM long_sequence(72)" +
                    ") TIMESTAMP (t) PARTITION BY DAY");

            // Timestamp filter narrows to 2 partitions (48 rows).
            // This triggers the buildFrameCacheEagerly() fallback in TimeFrameCursorImpl.
            try (RecordCursorFactory factory = select("x WHERE t < '1970-01-03'")) {
                Assert.assertTrue(factory.supportsTimeFrameCursor());
                try (TimeFrameCursor cursor = factory.getTimeFrameCursor(sqlExecutionContext)) {
                    assertForwardScan(cursor, 48);
                }
            }
            execute("DROP TABLE x");
        });
    }

    @Test
    public void testEmptyTable() throws Exception {
        testBothCursors(
                "CREATE TABLE x (" +
                        " i INT," +
                        " t TIMESTAMP NOT NULL" +
                        ") TIMESTAMP (t) PARTITION BY DAY",
                cursor -> {
                    Assert.assertFalse(cursor.next());
                    Assert.assertFalse(cursor.prev());
                }
        );
    }

    @Test
    public void testJumpToLazyPartition() throws Exception {
        testBothCursors(
                "CREATE TABLE x AS (SELECT" +
                        " rnd_int() a," +
                        " timestamp_sequence(0, 60 * 60 * 1_000_000L) t" +
                        " FROM long_sequence(72)" +
                        ") TIMESTAMP (t) PARTITION BY DAY",
                cursor -> {
                    TimeFrame frame = cursor.getTimeFrame();
                    Record record = cursor.getRecord();
                    int tsIndex = cursor.getTimestampIndex();

                    // Jump directly to the last frame without opening any prior frames.
                    cursor.jumpTo(2);
                    Assert.assertEquals(2, frame.getFrameIndex());

                    // Open the frame — triggers lazy partition open.
                    long rowCount = cursor.open();
                    Assert.assertTrue(rowCount > 0);

                    // Verify data is readable.
                    cursor.recordAt(record, Rows.toRowID(frame.getFrameIndex(), 0));
                    long ts = record.getTimestamp(tsIndex);
                    Assert.assertTrue(ts >= 2 * Micros.DAY_MICROS);

                    // Navigate backward from the jumped position.
                    Assert.assertTrue(cursor.prev());
                    Assert.assertEquals(1, frame.getFrameIndex());
                    cursor.open();
                    cursor.recordAt(record, Rows.toRowID(frame.getFrameIndex(), 0));
                    ts = record.getTimestamp(tsIndex);
                    Assert.assertTrue(ts >= Micros.DAY_MICROS);
                    Assert.assertTrue(ts < 2 * Micros.DAY_MICROS);
                }
        );
    }

    @Test
    public void testJumpToOutOfBounds() throws Exception {
        testBothCursors(
                "CREATE TABLE x AS (SELECT" +
                        " rnd_int() a," +
                        " timestamp_sequence(0, 60 * 60 * 1_000_000L) t" +
                        " FROM long_sequence(72)" +
                        ") TIMESTAMP (t) PARTITION BY DAY",
                cursor -> {
                    try {
                        cursor.jumpTo(-1);
                        Assert.fail("Expected CairoException for negative index");
                    } catch (CairoException expected) {
                        TestUtils.assertContains(expected.getFlyweightMessage(), "frame index out of bounds");
                    }
                    try {
                        cursor.jumpTo(3);
                        Assert.fail("Expected CairoException for index >= frameCount");
                    } catch (CairoException expected) {
                        TestUtils.assertContains(expected.getFlyweightMessage(), "frame index out of bounds");
                    }
                }
        );
    }

    @Test
    public void testLazyOpenWithColumnTops() throws Exception {
        assertMemoryLeak(() -> {
            // 12 rows in partition 0 (hours 0-11).
            execute("CREATE TABLE x AS (SELECT" +
                    " rnd_int() a," +
                    " timestamp_sequence(0, 60 * 60 * 1_000_000L) t" +
                    " FROM long_sequence(12)" +
                    ") TIMESTAMP (t) PARTITION BY DAY");
            // Add column after initial data — creates column tops.
            execute("ALTER TABLE x ADD COLUMN b INT");
            // 36 more rows: 12 append to partition 0 (column b top = 12),
            // 24 go to partition 1 (column b top = 0).
            execute("INSERT INTO x" +
                    " SELECT rnd_int(), timestamp_sequence(12 * 60 * 60 * 1_000_000L, 60 * 60 * 1_000_000L), rnd_int()" +
                    " FROM long_sequence(36)");

            // Verify column data including NULLs in column top region.
            TestUtils.printSql(engine, sqlExecutionContext, "x", sink);
            try (RecordCursorFactory factory = select("x")) {
                Assert.assertTrue(factory.supportsTimeFrameCursor());
                RecordMetadata metadata = factory.getMetadata();
                try (TimeFrameCursor cursor = factory.getTimeFrameCursor(sqlExecutionContext)) {
                    assertForwardScanWithColumnData(cursor, metadata, sink);
                }
                testWithConcurrentCursor(factory, cursor -> assertForwardScanWithColumnData(cursor, metadata, sink));
            }
            execute("DROP TABLE x");
        });
    }

    @Test
    public void testLazyOpenWithMultipleColumnTops() throws Exception {
        assertMemoryLeak(() -> {
            // 6 rows in partition 0 (hours 0-5).
            execute("CREATE TABLE x AS (SELECT" +
                    " rnd_int() a," +
                    " timestamp_sequence(0, 60 * 60 * 1_000_000L) t" +
                    " FROM long_sequence(6)" +
                    ") TIMESTAMP (t) PARTITION BY DAY");
            // Add column b after 6 rows — creates column top at row 6.
            execute("ALTER TABLE x ADD COLUMN b INT");
            // 6 more rows (hours 6-11) — column b has data, column c doesn't exist yet.
            execute("INSERT INTO x" +
                    " SELECT rnd_int(), timestamp_sequence(6 * 60 * 60 * 1_000_000L, 60 * 60 * 1_000_000L), rnd_int()" +
                    " FROM long_sequence(6)");
            // Add column c after 12 rows — creates second column top at row 12.
            execute("ALTER TABLE x ADD COLUMN c INT");
            // 36 more rows: 12 append to partition 0 (b top = 6, c top = 12),
            // 24 go to partition 1 (b top = 0, c top = 0).
            execute("INSERT INTO x" +
                    " SELECT rnd_int(), timestamp_sequence(12 * 60 * 60 * 1_000_000L, 60 * 60 * 1_000_000L), rnd_int(), rnd_int()" +
                    " FROM long_sequence(36)");

            // Verify column data: partition 0 should have 3 frame splits
            // (rows 0-5 with b=NULL,c=NULL; 6-11 with c=NULL; 12-23 with all columns).
            TestUtils.printSql(engine, sqlExecutionContext, "x", sink);
            try (RecordCursorFactory factory = select("x")) {
                Assert.assertTrue(factory.supportsTimeFrameCursor());
                RecordMetadata metadata = factory.getMetadata();
                try (TimeFrameCursor cursor = factory.getTimeFrameCursor(sqlExecutionContext)) {
                    assertForwardScanWithColumnData(cursor, metadata, sink);
                }
                testWithConcurrentCursor(factory, cursor -> assertForwardScanWithColumnData(cursor, metadata, sink));
            }
            execute("DROP TABLE x");
        });
    }

    @Test
    public void testLazyOpenWithVarSizeColumnTops() throws Exception {
        assertMemoryLeak(() -> {
            // 12 rows in partition 0 (hours 0-11).
            execute("CREATE TABLE x AS (SELECT" +
                    " rnd_int() a," +
                    " timestamp_sequence(0, 60 * 60 * 1_000_000L) t" +
                    " FROM long_sequence(12)" +
                    ") TIMESTAMP (t) PARTITION BY DAY");
            // Add VARCHAR column after initial data — creates column tops with aux pages.
            execute("ALTER TABLE x ADD COLUMN s VARCHAR");
            // 36 more rows: 12 append to partition 0 (column s top = 12),
            // 24 go to partition 1 (column s top = 0).
            execute("INSERT INTO x" +
                    " SELECT rnd_int(), timestamp_sequence(12 * 60 * 60 * 1_000_000L, 60 * 60 * 1_000_000L), rnd_varchar(5, 10, 0)" +
                    " FROM long_sequence(36)");

            // Verify column data including NULLs in column top region.
            TestUtils.printSql(engine, sqlExecutionContext, "x", sink);
            try (RecordCursorFactory factory = select("x")) {
                Assert.assertTrue(factory.supportsTimeFrameCursor());
                RecordMetadata metadata = factory.getMetadata();
                try (TimeFrameCursor cursor = factory.getTimeFrameCursor(sqlExecutionContext)) {
                    assertForwardScanWithColumnData(cursor, metadata, sink);
                }
                testWithConcurrentCursor(factory, cursor -> assertForwardScanWithColumnData(cursor, metadata, sink));
            }
            execute("DROP TABLE x");
        });
    }

    @Test
    public void testNavigateBackward() throws Exception {
        final int nRowsBase = 500;
        final int N = 10;
        for (int i = 0; i < N; i++) {
            final int nRows = nRowsBase * (i + 1);
            testBothCursors(
                    "CREATE TABLE x AS (SELECT" +
                            " rnd_int() a," +
                            " rnd_str() b," +
                            " timestamp_sequence(100000000, 100000000) t" +
                            " FROM long_sequence(" + nRows + ")" +
                            ") TIMESTAMP (t) PARTITION BY DAY",
                    cursor -> {
                        TestUtils.printSql(engine, sqlExecutionContext, "x order by t desc", sink);
                        final StringSink actualSink = new StringSink();
                        actualSink.put("a\tb\tt\n");

                        Record record = cursor.getRecord();
                        TimeFrame frame = cursor.getTimeFrame();
                        while (cursor.next()) {
                            Assert.assertEquals(-1, frame.getRowLo());
                            Assert.assertEquals(-1, frame.getRowHi());
                        }
                        // backward scan
                        while (cursor.prev()) {
                            Assert.assertEquals(-1, frame.getRowLo());
                            Assert.assertEquals(-1, frame.getRowHi());
                            cursor.open();
                            for (long row = frame.getRowHi() - 1; row >= frame.getRowLo(); row--) {
                                cursor.recordAt(record, Rows.toRowID(frame.getFrameIndex(), row));
                                actualSink.put(record.getInt(0));
                                actualSink.put('\t');
                                actualSink.put(record.getStrA(1));
                                actualSink.put('\t');
                                MicrosFormatUtils.appendDateTimeUSec(actualSink, record.getTimestamp(2));
                                actualSink.put('\n');
                            }
                        }
                        TestUtils.assertEquals(sink, actualSink);
                    }
            );
        }
    }

    @Test
    public void testNavigateBothDirections() throws Exception {
        final int nRowsBase = 500;
        final int N = 10;
        for (int i = 0; i < N; i++) {
            final int nRows = nRowsBase * (i + 1);
            testBothCursors(
                    "CREATE TABLE x AS (SELECT" +
                            " rnd_int() a," +
                            " rnd_str() b," +
                            " timestamp_sequence(100000000, 100000000) t" +
                            " FROM long_sequence(" + nRows + ")" +
                            ") TIMESTAMP (t) PARTITION BY DAY",
                    cursor -> {
                        TestUtils.printSql(engine, sqlExecutionContext, "x union all (x order by t desc)", sink);
                        final StringSink actualSink = new StringSink();
                        actualSink.put("a\tb\tt\n");

                        Record record = cursor.getRecord();
                        TimeFrame frame = cursor.getTimeFrame();
                        // forward scan
                        while (cursor.next()) {
                            Assert.assertEquals(-1, frame.getRowLo());
                            Assert.assertEquals(-1, frame.getRowHi());
                            cursor.open();
                            for (long row = frame.getRowLo(); row < frame.getRowHi(); row++) {
                                cursor.recordAt(record, Rows.toRowID(frame.getFrameIndex(), row));
                                actualSink.put(record.getInt(0));
                                actualSink.put('\t');
                                actualSink.put(record.getStrA(1));
                                actualSink.put('\t');
                                MicrosFormatUtils.appendDateTimeUSec(actualSink, record.getTimestamp(2));
                                actualSink.put('\n');
                            }
                        }
                        // backward scan
                        while (cursor.prev()) {
                            Assert.assertEquals(-1, frame.getRowLo());
                            Assert.assertEquals(-1, frame.getRowHi());
                            cursor.open();
                            for (long row = frame.getRowHi() - 1; row >= frame.getRowLo(); row--) {
                                cursor.recordAt(record, Rows.toRowID(frame.getFrameIndex(), row));
                                actualSink.put(record.getInt(0));
                                actualSink.put('\t');
                                actualSink.put(record.getStrA(1));
                                actualSink.put('\t');
                                MicrosFormatUtils.appendDateTimeUSec(actualSink, record.getTimestamp(2));
                                actualSink.put('\n');
                            }
                        }
                        TestUtils.assertEquals(sink, actualSink);
                    }
            );
        }
    }

    @Test
    public void testNavigateForward() throws Exception {
        final int nRowsBase = 500;
        final int N = 10;
        for (int i = 0; i < N; i++) {
            final int nRows = nRowsBase * (i + 1);
            testBothCursors(
                    "CREATE TABLE x AS (SELECT" +
                            " rnd_int() a," +
                            " rnd_str() b," +
                            " timestamp_sequence(100000000, 100000000) t" +
                            " FROM long_sequence(" + nRows + ")" +
                            ") TIMESTAMP (t) PARTITION BY DAY",
                    cursor -> {
                        TestUtils.printSql(engine, sqlExecutionContext, "x", sink);
                        final StringSink actualSink = new StringSink();
                        actualSink.put("a\tb\tt\n");

                        Record record = cursor.getRecord();
                        TimeFrame frame = cursor.getTimeFrame();
                        // forward scan
                        while (cursor.next()) {
                            Assert.assertEquals(-1, frame.getRowLo());
                            Assert.assertEquals(-1, frame.getRowHi());
                            cursor.open();
                            for (long row = frame.getRowLo(); row < frame.getRowHi(); row++) {
                                cursor.recordAt(record, Rows.toRowID(frame.getFrameIndex(), row));
                                actualSink.put(record.getInt(0));
                                actualSink.put('\t');
                                actualSink.put(record.getStrA(1));
                                actualSink.put('\t');
                                MicrosFormatUtils.appendDateTimeUSec(actualSink, record.getTimestamp(2));
                                actualSink.put('\n');
                            }
                        }
                        TestUtils.assertEquals(sink, actualSink);
                    }
            );
        }
    }

    @Test
    public void testNavigateForwardNoPartitionBy() throws Exception {
        testBothCursors(
                "CREATE TABLE x AS (SELECT" +
                        " rnd_long() a," +
                        " rnd_boolean() b," +
                        " timestamp_sequence(0, 100000000) t" +
                        " FROM long_sequence(1000)" +
                        ") TIMESTAMP (t)",
                cursor -> {
                    TestUtils.printSql(engine, sqlExecutionContext, "x", sink);
                    final StringSink actualSink = new StringSink();
                    actualSink.put("a\tb\tt\n");

                    Record record = cursor.getRecord();
                    TimeFrame frame = cursor.getTimeFrame();
                    // forward scan
                    while (cursor.next()) {
                        Assert.assertEquals(-1, frame.getRowLo());
                        Assert.assertEquals(-1, frame.getRowHi());
                        cursor.open();
                        for (long row = frame.getRowLo(); row < frame.getRowHi(); row++) {
                            cursor.recordAt(record, Rows.toRowID(frame.getFrameIndex(), row));
                            actualSink.put(record.getLong(0));
                            actualSink.put('\t');
                            actualSink.put(record.getBool(1));
                            actualSink.put('\t');
                            MicrosFormatUtils.appendDateTimeUSec(actualSink, record.getTimestamp(2));
                            actualSink.put('\n');
                        }
                    }
                    TestUtils.assertEquals(sink, actualSink);
                }
        );
    }

    @Test
    public void testNotSupportedForNonPartitionedTable() throws Exception {
        record TestCase(String table, String ddl, String query) {
        }

        final TestCase[] testCases = new TestCase[]{
                new TestCase(
                        "non_partitioned_t",
                        "create table non_partitioned_t as (select x from long_sequence(10))",
                        "non_partitioned_t"
                ),
                new TestCase(
                        "filtered_t",
                        "create table filtered_t (i int, ts timestamp NOT NULL) timestamp(ts) partition by day",
                        "filtered_t where i > 0"
                ),
                new TestCase(
                        "desc_ordered_t",
                        "create table desc_ordered_t (ts timestamp NOT NULL) timestamp(ts) partition by day",
                        "desc_ordered_t order by ts desc"
                )
        };

        for (TestCase tc : testCases) {
            assertMemoryLeak(() -> {
                execute(tc.ddl);
                try (RecordCursorFactory factory = select(tc.query)) {
                    Assert.assertFalse("time frame cursor shouldn't be supported for " + tc.table, factory.supportsTimeFrameCursor());
                }
            });
        }
    }

    @Test
    public void testParquetPartitionWithLazyPath() throws Exception {
        assertMemoryLeak(() -> {
            // 72 rows across 3 partitions (days 1970-01-01, 1970-01-02, 1970-01-03).
            execute("CREATE TABLE x AS (SELECT" +
                    " rnd_int() a," +
                    " timestamp_sequence(0, 60 * 60 * 1_000_000L) t" +
                    " FROM long_sequence(72)" +
                    ") TIMESTAMP (t) PARTITION BY DAY");
            // Convert middle partition to parquet.
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '1970-01-02'");

            try (RecordCursorFactory factory = select("x")) {
                Assert.assertTrue(factory.supportsTimeFrameCursor());
                try (TimeFrameCursor cursor = factory.getTimeFrameCursor(sqlExecutionContext)) {
                    assertForwardScan(cursor, 72);
                }
                testWithConcurrentCursor(factory, cursor -> assertForwardScan(cursor, 72));
            }
            execute("DROP TABLE x");
        });
    }

    @Test
    public void testRecordAtOnUnopenedPartition() throws Exception {
        testBothCursors(
                "CREATE TABLE x AS (SELECT" +
                        " rnd_int() a," +
                        " timestamp_sequence(0, 60 * 60 * 1_000_000L) t" +
                        " FROM long_sequence(72)" +
                        ") TIMESTAMP (t) PARTITION BY DAY",
                cursor -> {
                    Record record = cursor.getRecord();
                    int tsIndex = cursor.getTimestampIndex();

                    // Navigate to the last frame and open it.
                    Assert.assertTrue(cursor.next());
                    Assert.assertTrue(cursor.next());
                    Assert.assertTrue(cursor.next());
                    cursor.open();

                    // recordAt() on frame 0 which was never opened via open().
                    // This must trigger lazy partition open internally.
                    cursor.recordAt(record, Rows.toRowID(0, 0));
                    long ts = record.getTimestamp(tsIndex);
                    Assert.assertEquals(0, ts);

                    // Also test a row in the middle of partition 0.
                    cursor.recordAt(record, Rows.toRowID(0, 12));
                    ts = record.getTimestamp(tsIndex);
                    Assert.assertEquals(12 * 60 * 60 * 1_000_000L, ts);

                    // recordAt() on frame 1 which was also never opened via open().
                    cursor.recordAt(record, Rows.toRowID(1, 0));
                    ts = record.getTimestamp(tsIndex);
                    Assert.assertTrue(ts >= Micros.DAY_MICROS);
                }
        );
    }

    @Test
    public void testSeekEstimate() throws Exception {
        testBothCursors(
                "CREATE TABLE x AS (SELECT" +
                        " timestamp_sequence('2020-01-01', 3_600_000_000) t" +
                        " FROM long_sequence(72)" +
                        ") TIMESTAMP (t) PARTITION BY DAY",
                cursor -> {
                    TimeFrame frame = cursor.getTimeFrame();

                    // Collect frame estimate boundaries
                    long[] estimateHi = new long[3];
                    for (int i = 0; i < 3; i++) {
                        Assert.assertTrue(cursor.next());
                        Assert.assertEquals(i, frame.getFrameIndex());
                        estimateHi[i] = frame.getTimestampEstimateHi();
                    }
                    Assert.assertFalse(cursor.next());

                    // Seek before all frames: positions at -1
                    cursor.seekEstimate(0);
                    Assert.assertEquals(-1, frame.getFrameIndex());
                    // next() after seek should return first frame
                    Assert.assertTrue(cursor.next());
                    Assert.assertEquals(0, frame.getFrameIndex());

                    // Seek after all frames: positions at last frame
                    cursor.seekEstimate(Long.MAX_VALUE);
                    Assert.assertEquals(2, frame.getFrameIndex());
                    // next() should return false
                    Assert.assertFalse(cursor.next());

                    // Seek to ceiling of first partition: should find frame 0
                    cursor.seekEstimate(estimateHi[0]);
                    Assert.assertEquals(0, frame.getFrameIndex());
                    // next() should return frame 1
                    Assert.assertTrue(cursor.next());
                    Assert.assertEquals(1, frame.getFrameIndex());

                    // Seek to just before ceiling of first partition: no frame qualifies
                    cursor.seekEstimate(estimateHi[0] - 1);
                    Assert.assertEquals(-1, frame.getFrameIndex());
                    // next() should return frame 0
                    Assert.assertTrue(cursor.next());
                    Assert.assertEquals(0, frame.getFrameIndex());

                    // Seek to ceiling of second partition: should find frame 1
                    cursor.seekEstimate(estimateHi[1]);
                    Assert.assertEquals(1, frame.getFrameIndex());
                    // next() should return frame 2
                    Assert.assertTrue(cursor.next());
                    Assert.assertEquals(2, frame.getFrameIndex());
                }
        );
    }

    @Test
    public void testSeekEstimateEmptyTable() throws Exception {
        testBothCursors(
                "CREATE TABLE x (" +
                        " i INT," +
                        " t TIMESTAMP NOT NULL" +
                        ") TIMESTAMP (t) PARTITION BY DAY",
                cursor -> {
                    TimeFrame frame = cursor.getTimeFrame();

                    cursor.seekEstimate(0);
                    Assert.assertEquals(-1, frame.getFrameIndex());
                    Assert.assertFalse(cursor.next());

                    cursor.seekEstimate(Long.MAX_VALUE);
                    Assert.assertEquals(-1, frame.getFrameIndex());
                    Assert.assertFalse(cursor.next());
                }
        );
    }

    @Test
    public void testSeekEstimateNoPartition() throws Exception {
        testBothCursors(
                "CREATE TABLE x AS (SELECT" +
                        " timestamp_sequence(0, 100_000_000) t" +
                        " FROM long_sequence(100)" +
                        ") TIMESTAMP (t)",
                cursor -> {
                    TimeFrame frame = cursor.getTimeFrame();

                    // Single partition with ceiling = Long.MAX_VALUE
                    // Any real timestamp is less than Long.MAX_VALUE, so seek positions at -1
                    cursor.seekEstimate(0);
                    Assert.assertEquals(-1, frame.getFrameIndex());
                    Assert.assertTrue(cursor.next());
                    Assert.assertEquals(0, frame.getFrameIndex());

                    // Long.MAX_VALUE matches the ceiling, so seek finds frame 0
                    cursor.seekEstimate(Long.MAX_VALUE);
                    Assert.assertEquals(0, frame.getFrameIndex());
                    Assert.assertFalse(cursor.next());
                }
        );
    }

    @Test
    public void testSeekEstimateSplitPartitions() throws Exception {
        executeWithPool((engine, compiler, executionContext) -> {
            // Create a table spanning 3 calendar-day partitions (Feb 3-5), then insert
            // OOO data into the last partition to trigger a partition split.
            execute(
                    compiler,
                    "CREATE TABLE x AS (" +
                            "  SELECT timestamp_sequence('2020-02-03T13', 60 * 1_000_000L) ts" +
                            "  FROM long_sequence(60 * 24 * 2 + 300)" +
                            ") TIMESTAMP (ts) PARTITION BY DAY",
                    executionContext
            );
            execute(
                    compiler,
                    "INSERT INTO x" +
                            "  SELECT timestamp_sequence('2020-02-05T17:01', 60 * 1_000_000L) ts" +
                            "  FROM long_sequence(50)",
                    executionContext
            );

            try (RecordCursorFactory factory = engine.select("x", executionContext)) {
                Assert.assertTrue(factory.supportsTimeFrameCursor());
                try (TimeFrameCursor cursor = factory.getTimeFrameCursor(executionContext)) {
                    TimeFrame frame = cursor.getTimeFrame();

                    // Enumerate all frames, collecting their estimate ceilings.
                    LongList ceilings = new LongList();
                    while (cursor.next()) {
                        ceilings.add(frame.getTimestampEstimateHi());
                    }
                    int frameCount = ceilings.size();
                    Assert.assertTrue("expected more frames than partitions", frameCount > 3);

                    // Seek before all frames
                    cursor.seekEstimate(0);
                    Assert.assertEquals(-1, frame.getFrameIndex());
                    Assert.assertTrue(cursor.next());
                    Assert.assertEquals(0, frame.getFrameIndex());

                    // Seek after all frames
                    cursor.seekEstimate(Long.MAX_VALUE);
                    Assert.assertEquals(frameCount - 1, frame.getFrameIndex());
                    Assert.assertFalse(cursor.next());

                    // For split partitions, multiple frames share the same ceiling.
                    // seekEstimate should return the LAST frame with that ceiling.
                    for (int i = 0; i < frameCount; i++) {
                        // Find the last frame with the same ceiling as frame i
                        int lastWithSameCeiling = i;
                        while (lastWithSameCeiling + 1 < frameCount
                                && ceilings.getQuick(lastWithSameCeiling + 1) == ceilings.getQuick(i)) {
                            lastWithSameCeiling++;
                        }

                        cursor.seekEstimate(ceilings.getQuick(i));
                        Assert.assertEquals(
                                "seekEstimate(" + ceilings.getQuick(i) + ") should find last frame with ceiling",
                                lastWithSameCeiling,
                                frame.getFrameIndex()
                        );

                        // next() should advance to the frame after the split group
                        if (lastWithSameCeiling + 1 < frameCount) {
                            Assert.assertTrue(cursor.next());
                            Assert.assertEquals(lastWithSameCeiling + 1, frame.getFrameIndex());
                        } else {
                            Assert.assertFalse(cursor.next());
                        }
                    }

                    // Seek to just before the first ceiling: no frame qualifies
                    cursor.seekEstimate(ceilings.getQuick(0) - 1);
                    Assert.assertEquals(-1, frame.getFrameIndex());
                    Assert.assertTrue(cursor.next());
                    Assert.assertEquals(0, frame.getFrameIndex());
                }
            }
        });
    }

    @Test
    public void testSeekEstimateWithLazyPartitions() throws Exception {
        assertMemoryLeak(() -> {
            // 72 rows across 3 partitions.
            execute("CREATE TABLE x AS (SELECT" +
                    " rnd_int() a," +
                    " timestamp_sequence('2020-01-01', 3_600_000_000) t" +
                    " FROM long_sequence(72)" +
                    ") TIMESTAMP (t) PARTITION BY DAY");

            try (RecordCursorFactory factory = select("x")) {
                Assert.assertTrue(factory.supportsTimeFrameCursor());
                try (TimeFrameCursor cursor = factory.getTimeFrameCursor(sqlExecutionContext)) {
                    TimeFrame frame = cursor.getTimeFrame();

                    // Seek to the last partition without opening any partitions first.
                    cursor.seekEstimate(Long.MAX_VALUE);
                    Assert.assertEquals(2, frame.getFrameIndex());

                    // Open the frame — triggers lazy partition open.
                    long rowCount = cursor.open();
                    Assert.assertTrue(rowCount > 0);

                    // Verify data can be read from the lazily opened partition.
                    Record record = cursor.getRecord();
                    cursor.recordAt(record, Rows.toRowID(frame.getFrameIndex(), 0));
                    // Timestamp must be within the third day.
                    long ts = record.getTimestamp(1);
                    Assert.assertTrue(ts >= 2 * Micros.DAY_MICROS);
                }
            }
            execute("DROP TABLE x");
        });
    }

    @Test
    public void testSingleRowPartitions() throws Exception {
        testBothCursors(
                "CREATE TABLE x AS (SELECT" +
                        " rnd_int() a," +
                        " timestamp_sequence(0, 24 * 60 * 60 * 1_000_000L) t" +
                        " FROM long_sequence(5)" +
                        ") TIMESTAMP (t) PARTITION BY DAY",
                cursor -> assertForwardScan(cursor, 5)
        );
    }

    @Test
    public void testSingleRowTable() throws Exception {
        testBothCursors(
                "CREATE TABLE x AS (SELECT" +
                        " rnd_int() a," +
                        " timestamp_sequence(0, 1_000_000L) t" +
                        " FROM long_sequence(1)" +
                        ") TIMESTAMP (t) PARTITION BY DAY",
                cursor -> assertForwardScan(cursor, 1)
        );
    }

    @Test
    public void testTimeFrameBoundaries() throws Exception {
        final int[] partitionBys = new int[]{
                PartitionBy.NONE,
                PartitionBy.HOUR,
                PartitionBy.DAY,
                PartitionBy.WEEK,
                PartitionBy.MONTH,
                PartitionBy.YEAR,
        };

        for (int partitionBy : partitionBys) {
            testBothCursors(
                    "CREATE TABLE x AS (SELECT" +
                            " timestamp_sequence(100000000, " + 365 * Micros.DAY_MICROS + ") t" +
                            " FROM long_sequence(3)" +
                            ") TIMESTAMP (t) PARTITION BY " + PartitionBy.toString(partitionBy),
                    cursor -> {
                        Record record = cursor.getRecord();
                        TimeFrame frame = cursor.getTimeFrame();
                        // forward scan
                        while (cursor.next()) {
                            Assert.assertEquals(-1, frame.getRowLo());
                            Assert.assertEquals(-1, frame.getRowHi());
                            cursor.open();
                            cursor.recordAt(record, Rows.toRowID(frame.getFrameIndex(), frame.getRowLo()));
                            long tsLo = record.getTimestamp(0);

                            TimestampDriver.TimestampFloorMethod floorMethod = PartitionBy.getPartitionFloorMethod(ColumnType.TIMESTAMP, partitionBy);
                            TimestampDriver.TimestampCeilMethod ceilMethod = PartitionBy.getPartitionCeilMethod(ColumnType.TIMESTAMP, partitionBy);

                            long expectedEstimateTsLo = floorMethod != null ? floorMethod.floor(tsLo) : 0;
                            long expectedEstimateTsHi = ceilMethod != null ? ceilMethod.ceil(tsLo) : Long.MAX_VALUE;

                            Assert.assertEquals(expectedEstimateTsLo, frame.getTimestampEstimateLo());
                            Assert.assertEquals(expectedEstimateTsHi, frame.getTimestampEstimateHi());

                            Assert.assertEquals(tsLo, frame.getTimestampLo());
                            cursor.recordAt(record, Rows.toRowID(frame.getFrameIndex(), frame.getRowHi() - 1));
                            long tsHi = record.getTimestamp(0);
                            Assert.assertEquals(tsHi + 1, frame.getTimestampHi());
                        }
                    }
            );
        }
    }

    @Test
    public void testTimeFrameBoundariesSplitPartitions() throws Exception {
        executeWithPool((engine, compiler, executionContext) -> {
            // produce split partition
            execute(
                    compiler,
                    "create table x as (" +
                            "  select timestamp_sequence('2020-02-03T13', 60*1000000L) ts " +
                            "  from long_sequence(60*24*2+300)" +
                            ") timestamp (ts) partition by DAY",
                    executionContext
            );
            execute(
                    compiler,
                    "create table z as (" +
                            "  select timestamp_sequence('2020-02-05T17:01', 60*1000000L) ts " +
                            "  from long_sequence(50)" +
                            ")",
                    executionContext
            );
            execute(
                    compiler,
                    "create table y as (select * from x union all select * from z)",
                    executionContext
            );
            execute(
                    compiler,
                    "insert into x select * from z",
                    executionContext
            );

            try (RecordCursorFactory factory = engine.select("x", executionContext)) {
                Assert.assertTrue(factory.supportsTimeFrameCursor());
                try (TimeFrameCursor timeFrameCursor = factory.getTimeFrameCursor(executionContext)) {
                    Record record = timeFrameCursor.getRecord();
                    TimeFrame frame = timeFrameCursor.getTimeFrame();
                    long prevSplitTimestampHi = -1;
                    // forward scan
                    while (timeFrameCursor.next()) {
                        Assert.assertEquals(-1, frame.getRowLo());
                        Assert.assertEquals(-1, frame.getRowHi());
                        timeFrameCursor.open();
                        timeFrameCursor.recordAt(record, Rows.toRowID(frame.getFrameIndex(), frame.getRowLo()));
                        long tsLo = record.getTimestamp(0);

                        TimestampDriver.TimestampFloorMethod floorMethod = PartitionBy.getPartitionFloorMethod(ColumnType.TIMESTAMP, PartitionBy.DAY);
                        TimestampDriver.TimestampCeilMethod ceilMethod = PartitionBy.getPartitionCeilMethod(ColumnType.TIMESTAMP, PartitionBy.DAY);

                        long expectedEstimateTsLo = floorMethod != null ? floorMethod.floor(tsLo) : 0;
                        long expectedEstimateTsHi = ceilMethod != null ? ceilMethod.ceil(tsLo) : Long.MAX_VALUE;

                        Assert.assertTrue(frame.getTimestampEstimateLo() >= expectedEstimateTsLo);
                        if (prevSplitTimestampHi != -1) {
                            // this must be split partition
                            Assert.assertEquals(prevSplitTimestampHi, frame.getTimestampEstimateLo());
                        }
                        Assert.assertTrue(frame.getTimestampEstimateHi() <= expectedEstimateTsHi);
                        if (frame.getTimestampEstimateHi() < expectedEstimateTsHi) {
                            // this must be split partition
                            prevSplitTimestampHi = frame.getTimestampEstimateHi();
                        } else {
                            prevSplitTimestampHi = -1;
                        }

                        Assert.assertEquals(tsLo, frame.getTimestampLo());
                        timeFrameCursor.recordAt(record, Rows.toRowID(frame.getFrameIndex(), frame.getRowHi() - 1));
                        long tsHi = record.getTimestamp(0);
                        Assert.assertEquals(tsHi + 1, frame.getTimestampHi());
                    }
                }
            }
        });
    }

    @Test
    public void testToTopAfterPartialIteration() throws Exception {
        testBothCursors(
                "CREATE TABLE x AS (SELECT" +
                        " rnd_int() a," +
                        " timestamp_sequence(0, 60 * 60 * 1_000_000L) t" +
                        " FROM long_sequence(72)" +
                        ") TIMESTAMP (t) PARTITION BY DAY",
                cursor -> {
                    // Iterate halfway (open first partition only).
                    Assert.assertTrue(cursor.next());
                    cursor.open();

                    // Reset and re-iterate fully — must produce all rows.
                    cursor.toTop();
                    assertForwardScan(cursor, 72);
                }
        );
    }

    private static void assertForwardScan(TimeFrameCursor cursor, long expectedRows) {
        Record record = cursor.getRecord();
        TimeFrame frame = cursor.getTimeFrame();
        int tsIndex = cursor.getTimestampIndex();
        long totalRows = 0;
        long prevTimestamp = Long.MIN_VALUE;
        while (cursor.next()) {
            cursor.open();
            for (long row = frame.getRowLo(); row < frame.getRowHi(); row++) {
                cursor.recordAt(record, Rows.toRowID(frame.getFrameIndex(), row));
                long ts = record.getTimestamp(tsIndex);
                Assert.assertTrue("timestamps must be non-decreasing", ts >= prevTimestamp);
                prevTimestamp = ts;
                totalRows++;
            }
        }
        Assert.assertEquals(expectedRows, totalRows);
    }

    private static void assertForwardScanWithColumnData(
            TimeFrameCursor cursor,
            RecordMetadata metadata,
            CharSequence expected
    ) {
        Record record = cursor.getRecord();
        TimeFrame frame = cursor.getTimeFrame();
        final StringSink actualSink = new StringSink();
        CursorPrinter.println(metadata, actualSink);
        while (cursor.next()) {
            cursor.open();
            for (long row = frame.getRowLo(); row < frame.getRowHi(); row++) {
                cursor.recordAt(record, Rows.toRowID(frame.getFrameIndex(), row));
                TestUtils.println(record, metadata, actualSink);
            }
        }
        TestUtils.assertEquals(expected, actualSink);
    }

    private static void executeWithPool(CustomisableRunnable runnable) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final CairoConfiguration configuration = new DefaultTestCairoConfiguration(root) {
                @Override
                public long getPartitionO3SplitMinSize() {
                    return 1000;
                }
            };
            WorkerPool pool = new WorkerPool(() -> 2);
            TestUtils.execute(pool, runnable, configuration, LOG);
        });
    }

    private void testBothCursors(String ddl, TimeFrameCursorAssertion assertion) throws Exception {
        assertMemoryLeak(() -> {
            execute(ddl);
            try (RecordCursorFactory factory = select("x")) {
                Assert.assertTrue(factory.supportsTimeFrameCursor());
                // Test TimeFrameCursorImpl
                try (TimeFrameCursor cursor = factory.getTimeFrameCursor(sqlExecutionContext)) {
                    assertion.test(cursor);
                }
                // Test ConcurrentTimeFrameCursorImpl
                testWithConcurrentCursor(factory, assertion);
            }
            execute("DROP TABLE x");
        });
    }

    private void testWithConcurrentCursor(RecordCursorFactory factory, TimeFrameCursorAssertion assertion) throws Exception {
        // Unwrap QueryProgress to get the base factory with direct access to TablePageFrameCursor
        RecordCursorFactory baseFactory = factory instanceof QueryProgress ? factory.getBaseFactory() : factory;
        ConcurrentTimeFrameCursor cursor = baseFactory.newTimeFrameCursor();
        Assert.assertNotNull(cursor);
        ConcurrentTimeFrameState sharedState = new ConcurrentTimeFrameState();
        try {
            TablePageFrameCursor pageFrameCursor = (TablePageFrameCursor) baseFactory.getPageFrameCursor(
                    sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC
            );
            RecordMetadata metadata = baseFactory.getMetadata();

            sharedState.of(
                    pageFrameCursor,
                    metadata,
                    pageFrameCursor.getColumnMapping(),
                    pageFrameCursor.isExternal(),
                    sqlExecutionContext.getPageFrameMinRows(),
                    sqlExecutionContext.getPageFrameMaxRows(),
                    sqlExecutionContext.getSharedQueryWorkerCount()
            );

            cursor.of(sharedState, pageFrameCursor, metadata.getTimestampIndex());

            assertion.test(cursor);
        } finally {
            Misc.free(cursor);
            Misc.free(sharedState);
        }
    }

    @FunctionalInterface
    private interface TimeFrameCursorAssertion {
        void test(TimeFrameCursor cursor) throws Exception;
    }
}
