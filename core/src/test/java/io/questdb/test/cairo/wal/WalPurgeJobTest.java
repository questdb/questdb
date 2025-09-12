/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test.cairo.wal;

import io.questdb.PropertyKey;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.wal.DefaultWalDirectoryPolicy;
import io.questdb.cairo.wal.WalPurgeJob;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.mp.SimpleWaitingLock;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.FindVisitor;
import io.questdb.std.LongList;
import io.questdb.std.ObjList;
import io.questdb.std.str.DirectUtf8StringZ;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static io.questdb.cairo.TableUtils.TXN_FILE_NAME;
import static io.questdb.cairo.wal.WalUtils.SEQ_DIR;

public class WalPurgeJobTest extends AbstractCairoTest {
    private final FilesFacade ff = TestFilesFacadeImpl.INSTANCE;

    @Before
    public void setUp() {
        super.setUp();
        engine.setWalDirectoryPolicy(
                new DefaultWalDirectoryPolicy() {
                    @Override
                    public boolean isInUse(Path path) {
                        int size = path.size();
                        try {
                            return ff.exists(path.concat(".pending").concat("test.pending").$());
                        } finally {
                            path.trimTo(size);
                        }
                    }
                }
        );
    }

    @Test
    public void testChunkedSequencerPartsRemoved() throws Exception {
        assertMemoryLeak(() -> {
            int txnChunkSize = 10;
            node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 10);

            String tableName = testName.getMethodName();
            execute("create table " + tableName + " as (" +
                    "select x, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts " +
                    " from long_sequence(5)" +
                    ") timestamp(ts) partition by DAY WAL");

            TableToken tableToken = engine.verifyTableName(tableName);

            assertWalExistence(true, tableName, 1);
            assertSegmentExistence(true, tableName, 1, 0);

            int txns = (int) (2.5 * txnChunkSize);
            for (int i = 0; i < txns; i++) {
                execute("insert into " + tableName + " values (" + i + ", '2022-02-24')");
            }

            assertSeqPartExistence(true, tableToken, 0);
            assertSeqPartExistence(true, tableToken, 1);
            assertSeqPartExistence(true, tableToken, 2);
            assertWalExistence(true, tableToken, 1);

            drainPurgeJob();

            assertSeqPartExistence(true, tableToken, 0);
            assertSeqPartExistence(true, tableToken, 1);
            assertSeqPartExistence(true, tableToken, 2);
            assertWalExistence(true, tableToken, 1);

            drainWalQueue();
            drainPurgeJob();

            assertExistence(true, tableToken);
            assertSeqPartExistence(false, tableToken, 0);
            assertSeqPartExistence(false, tableToken, 1);
            assertSeqPartExistence(true, tableToken, 2);
            assertWalExistence(true, tableToken, 1);

            engine.releaseAllWalWriters();
            drainPurgeJob();

            assertWalExistence(false, tableToken, 1);
        });
    }

    @Test
    public void testClosedButUnappliedSegment() throws Exception {
        // Test two segment with changes committed to the sequencer, but never applied to the table.
        // The WAL is closed but nothing can be purged.
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            execute("create table " + tableName + "("
                    + "x long,"
                    + "ts timestamp"
                    + ") timestamp(ts) partition by DAY WAL");
            execute("insert into " + tableName + " values (1, '2022-02-24T00:00:00.000000Z')");
            execute("alter table " + tableName + " add column s1 string");
            execute("insert into " + tableName + " values (2, '2022-02-24T00:00:01.000000Z', 'x')");
            assertWalExistence(true, tableName, 1);
            assertSegmentExistence(true, tableName, 1, 0);
            assertSegmentExistence(true, tableName, 1, 1);
            assertWalLockEngagement(true, tableName, 1);
            assertSegmentLockEngagement(false, tableName, 1, 0);  // Old segment is unlocked.
            assertSegmentLockEngagement(true, tableName, 1, 1);

            // Release WAL and segments.
            engine.releaseInactive();
            assertWalLockEngagement(false, tableName, 1);
            assertSegmentLockEngagement(false, tableName, 1, 0);
            assertSegmentLockEngagement(false, tableName, 1, 1);

            // The segments are not going to be cleaned up despite being both unlocked.
            drainPurgeJob();
            assertWalExistence(true, tableName, 1);
            assertSegmentExistence(true, tableName, 1, 0);
            assertSegmentExistence(true, tableName, 1, 1);

            // Idempotency check
            for (int count = 0; count < 1000; ++count) {
                drainPurgeJob();
                assertWalExistence(true, tableName, 1);
                assertSegmentExistence(true, tableName, 1, 0);
                assertSegmentExistence(true, tableName, 1, 1);
            }

            // After draining, it's all deleted.
            drainWalQueue();
            assertSql("x\tts\ts1\n" +
                    "1\t2022-02-24T00:00:00.000000Z\t\n" +
                    "2\t2022-02-24T00:00:01.000000Z\tx\n", tableName);
            drainPurgeJob();
            assertWalExistence(false, tableName, 1);
            assertWalLockExistence(false, tableName, 1);
            assertSegmentExistence(false, tableName, 1, 0);
            assertSegmentLockExistence(false, tableName, 1, 0);
            assertSegmentExistence(false, tableName, 1, 1);
            assertSegmentLockExistence(false, tableName, 1, 1);
        });
    }

    @Test
    public void testDirectorySequencerRace() throws Exception {
        // We need to enter a state where `tableName`:
        //   * Has two WAL directories, wal1 and wal2.
        //   * Both wal1 and wal2 have an inactive segment 0 and an active segment 1.
        //   * The sequencer is tracking both WALs and all segments.
        //
        // However, to simulate a race condition, during the `purgeWalSegments()` scan we
        // fudge the `FilesFacade` to only list wal0 (and not wal1).
        // This means that the logic will encounter (and ignore) wal1.
        //
        // We will then assert that only wal1/0 is cleaned and wal2/0 is not.

        assertMemoryLeak(() -> {
            final String tableName = testName.getMethodName();
            execute("create table " + tableName + "("
                    + "x long,"
                    + "ts timestamp"
                    + ") timestamp(ts) partition by DAY WAL");

            execute("insert into " + tableName + " values (1, '2022-02-24T00:00:00.000000Z')");
            assertWalExistence(true, tableName, 1);

            // A test FilesFacade that hides the "wal2" directory.
            String dirNamePath = Files.SEPARATOR + Chars.toString(engine.verifyTableName(tableName).getDirName());
            FilesFacade testFF = new TestFilesFacadeImpl() {
                @Override
                public void iterateDir(LPSZ path, FindVisitor func) {
                    if (Utf8s.endsWithAscii(path, dirNamePath)) {
                        final DirectUtf8StringZ name = new DirectUtf8StringZ();
                        super.iterateDir(path, (long pUtf8NameZ, int type) -> {
                            name.of(pUtf8NameZ);
                            if (!name.toString().equals("wal2")) {
                                func.onFind(pUtf8NameZ, type);
                            }
                        });
                    } else {
                        super.iterateDir(path, func);
                    }
                }
            };

            try (WalWriter walWriter1 = getWalWriter(tableName)) {
                // Assert we've obtained a writer to wal1 and we're not already on wal2.
                assertWalExistence(true, tableName, 1);
                assertWalExistence(false, tableName, 2);
                assertSegmentExistence(true, tableName, 1, 0);
                assertSegmentExistence(false, tableName, 1, 1);
                assertSegmentLockEngagement(true, tableName, 1, 0);

                addColumn(walWriter1, "i1");
                TableWriter.Row row2 = walWriter1.newRow(MicrosTimestampDriver.floor("2022-02-25"));
                row2.putLong(0, 2);
                row2.putInt(2, 2);
                row2.append();

                // We assert that we've created a new segment.
                assertSegmentExistence(true, tableName, 1, 0);
                assertSegmentExistence(true, tableName, 1, 1);
                assertSegmentLockEngagement(false, tableName, 1, 0);
                assertSegmentLockEngagement(true, tableName, 1, 1);

                // We commit the segment to the sequencer.
                walWriter1.commit();

                try (WalWriter walWriter2 = getWalWriter(tableName)) {
                    assertWalExistence(true, tableName, 1);
                    assertWalExistence(true, tableName, 2);
                    assertSegmentExistence(true, tableName, 2, 0);
                    assertSegmentExistence(false, tableName, 2, 1);

                    TableWriter.Row row3 = walWriter2.newRow(MicrosTimestampDriver.floor("2022-02-26"));
                    row3.putLong(0, 3);
                    row3.putInt(2, 3);
                    row3.append();
                    walWriter2.commit();

                    addColumn(walWriter2, "i2");
                    TableWriter.Row row4 = walWriter2.newRow(MicrosTimestampDriver.floor("2022-02-27"));
                    row4.putLong(0, 4);
                    row4.putInt(2, 4);
                    row4.putInt(3, 4);
                    row4.append();

                    assertSegmentExistence(true, tableName, 2, 0);
                    assertSegmentExistence(true, tableName, 2, 1);
                    assertSegmentLockEngagement(false, tableName, 2, 0);
                    assertSegmentLockEngagement(true, tableName, 2, 1);

                    walWriter2.commit();

                    drainWalQueue();

                    assertSql("x\tts\ti1\ti2\n" +
                            "1\t2022-02-24T00:00:00.000000Z\tnull\tnull\n" +
                            "2\t2022-02-25T00:00:00.000000Z\t2\tnull\n" +
                            "3\t2022-02-26T00:00:00.000000Z\t3\tnull\n" +
                            "4\t2022-02-27T00:00:00.000000Z\t4\t4\n", tableName);

                    assertWalExistence(true, tableName, 1);
                    assertSegmentExistence(true, tableName, 1, 0);
                    assertSegmentLockEngagement(false, tableName, 1, 0);
                    assertSegmentExistence(true, tableName, 1, 1);
                    assertSegmentLockEngagement(true, tableName, 1, 1);  // wal1/1 locked
                    assertWalExistence(true, tableName, 2);
                    assertSegmentExistence(true, tableName, 2, 0);
                    assertSegmentLockEngagement(false, tableName, 2, 0);
                    assertSegmentExistence(true, tableName, 2, 1);
                    assertSegmentLockEngagement(true, tableName, 2, 1);  // wal2/1 locked
                    TestUtils.drainPurgeJob(engine, testFF);
                    assertSegmentLockEngagement(true, tableName, 1, 1);
                    assertWalExistence(true, tableName, 1);
                    assertSegmentExistence(false, tableName, 1, 0);  // DELETED
                    assertSegmentExistence(true, tableName, 1, 1);  // wal1/1 kept
                    assertSegmentLockEngagement(true, tableName, 1, 1);  // wal1/1 locked
                    assertWalExistence(true, tableName, 2);
                    assertSegmentExistence(false, tableName, 2, 0);  // Segment wal2/0 is applied and inactive (unlocked)
                    assertSegmentExistence(true, tableName, 2, 1);  // wal2/1 kept
                    assertSegmentLockEngagement(true, tableName, 2, 1);  // wal2/1 locked
                }
            }
        });
    }

    @Test
    public void testDroppedTablePendingSequencer() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            execute("create table " + tableName + " as (" +
                    "select x, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts " +
                    " from long_sequence(5)" +
                    ") timestamp(ts) partition by DAY WAL");

            TableToken tableToken = engine.verifyTableName(tableName);

            assertWalExistence(true, tableName, 1);
            assertSegmentExistence(true, tableName, 1, 0);
            createPendingFile(tableToken);
            execute("drop table " + tableName);

            drainWalQueue();
            drainPurgeJob();

            assertExistence(true, tableToken);

            engine.releaseAllWalWriters();

            drainPurgeJob();
            assertExistence(true, tableToken);
            assertWalExistence(false, tableToken, 1);

            removePendingFile(tableToken);
            drainPurgeJob();
            assertExistence(false, tableToken);
        });
    }

    @Test
    public void testInterval() throws Exception {
        AtomicInteger counter = new AtomicInteger();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long findFirst(LPSZ path) {
                counter.incrementAndGet();
                return super.findFirst(path);
            }
        };

        assertMemoryLeak(ff, () -> {
            final String tableName = testName.getMethodName();
            execute("create table " + tableName + "("
                    + "x long,"
                    + "ts timestamp"
                    + ") timestamp(ts) partition by DAY WAL");

            final long interval = engine.getConfiguration().getWalPurgeInterval() * 1000;  // ms to us.
            setCurrentMicros(interval + 1);  // Set to some point in time that's not 0.

            try (WalPurgeJob walPurgeJob = new WalPurgeJob(engine, ff, configuration.getMicrosecondClock())) {
                counter.set(0);

                walPurgeJob.delayByHalfInterval();
                walPurgeJob.run(0);
                Assert.assertEquals(0, counter.get());
                setCurrentMicros(currentMicros + interval / 2 + 1);
                walPurgeJob.run(0);
                Assert.assertEquals(1, counter.get());
                setCurrentMicros(currentMicros + interval / 2 + 1);
                walPurgeJob.run(0);
                walPurgeJob.run(0);
                walPurgeJob.run(0);
                Assert.assertEquals(1, counter.get());
                setCurrentMicros(currentMicros + interval);
                walPurgeJob.run(0);
                Assert.assertEquals(2, counter.get());
                setCurrentMicros(currentMicros + 10 * interval);
                walPurgeJob.run(0);
                Assert.assertEquals(3, counter.get());
            }
        });
    }

    @Test
    public void testLastSegmentUnlockedPrevLocked() {
        /*
          discovered=[
              (1,1),(1,2),(1,3),(1,4),(1,5),(1,6:locked),(1,7),(wal1:locked),
              (2,0),(2,1),(2,2),(2,3),(2,4:locked),(wal2:locked),
              (3,0),(3,1),(3,2),(3,3:locked),(wal3:locked),
              (4,0),(4,1),(4,2),(4,3),(4,4),(4,5),(4,6:locked),(wal4:locked)],
          nextToApply=[
              (1,1),(2,0),(3,0),(4,0)]
         */
        TestDeleter deleter = new TestDeleter();
        WalPurgeJob.Logic logic = new WalPurgeJob.Logic(deleter, 0);
        TableToken tableToken = new TableToken("test", "test~1", null, 42, true, false, false);
        logic.reset(tableToken);
        logic.trackDiscoveredSegment(1, 1, 1);
        logic.trackDiscoveredSegment(1, 2, 2);
        logic.trackDiscoveredSegment(1, 3, 3);
        logic.trackDiscoveredSegment(1, 4, 4);
        logic.trackDiscoveredSegment(1, 5, 5);
        logic.trackDiscoveredSegment(1, 6, -1);
        logic.trackDiscoveredSegment(1, 7, 6);
        logic.trackDiscoveredWal(1, -1);
        logic.trackDiscoveredSegment(2, 0, 7);
        logic.trackDiscoveredSegment(2, 1, 8);
        logic.trackDiscoveredSegment(2, 2, 9);
        logic.trackDiscoveredSegment(2, 3, 10);
        logic.trackDiscoveredSegment(2, 4, -1);
        logic.trackDiscoveredWal(2, -1);
        logic.trackDiscoveredSegment(3, 0, 11);
        logic.trackDiscoveredSegment(3, 1, 12);
        logic.trackDiscoveredSegment(3, 2, 13);
        logic.trackDiscoveredSegment(3, 3, -1);
        logic.trackDiscoveredWal(3, -1);
        logic.trackDiscoveredSegment(4, 0, 14);
        logic.trackDiscoveredSegment(4, 1, 15);
        logic.trackDiscoveredSegment(4, 2, 16);
        logic.trackDiscoveredSegment(4, 3, 17);
        logic.trackDiscoveredSegment(4, 4, 18);
        logic.trackDiscoveredSegment(4, 5, 19);
        logic.trackDiscoveredSegment(4, 6, -1);
        logic.trackDiscoveredWal(4, -1);
        logic.trackNextToApplySegment(1, 1);
        logic.trackNextToApplySegment(2, 0);
        logic.trackNextToApplySegment(3, 0);
        logic.trackNextToApplySegment(4, 0);
        logic.run();

        int evIndex = 0;
        assertNoMoreEvents(deleter, evIndex);
    }

    @Test
    public void testLastSegmentUnlockedPrevLocked2() {
        /*
          discovered=[
              (1,1),(1,2:locked),(1,3),(wal1:locked),
              (2,0:locked),(wal2:locked)],
          nextToApply=[
              (1,1),(2,0)]
         */
        TestDeleter deleter = new TestDeleter();
        WalPurgeJob.Logic logic = new WalPurgeJob.Logic(deleter, 0);
        TableToken tableToken = new TableToken("test", "test~1", null, 42, true, false, false);
        logic.reset(tableToken);
        logic.trackDiscoveredSegment(1, 1, 1);
        logic.trackDiscoveredSegment(1, 2, -1);
        logic.trackDiscoveredSegment(1, 3, 2);
        logic.trackDiscoveredWal(1, -1);
        logic.trackDiscoveredSegment(2, 0, -1);
        logic.trackDiscoveredWal(2, -1);
        logic.trackNextToApplySegment(1, 1);
        logic.trackNextToApplySegment(2, 0);
        logic.run();

        int evIndex = 0;
        assertNoMoreEvents(deleter, evIndex);
    }

    @Test
    public void testOneSegment() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            execute("create table " + tableName + " as (" +
                    "select x, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts " +
                    " from long_sequence(5)" +
                    ") timestamp(ts) partition by DAY WAL");

            assertWalExistence(true, tableName, 1);
            assertSegmentExistence(true, tableName, 1, 0);

            drainWalQueue();

            assertWalExistence(true, tableName, 1);

            assertSql("x\tts\n" +
                    "1\t2022-02-24T00:00:00.000000Z\n" +
                    "2\t2022-02-24T00:00:01.000000Z\n" +
                    "3\t2022-02-24T00:00:02.000000Z\n" +
                    "4\t2022-02-24T00:00:03.000000Z\n" +
                    "5\t2022-02-24T00:00:04.000000Z\n", tableName);

            drainPurgeJob();

            assertSegmentExistence(true, tableName, 1, 0);
            assertWalExistence(true, tableName, 1);

            engine.releaseInactive();

            drainPurgeJob();

            assertSegmentExistence(false, tableName, 1, 0);
            assertWalExistence(false, tableName, 1);
        });
    }

    @Test
    public void testPendingSegmentTasks() throws Exception {
        assertMemoryLeak(() -> {
            // Creating a table creates a new WAL with a first segment.
            final String tableName = testName.getMethodName();
            execute("create table " + tableName + " as (" +
                    "select x, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts " +
                    " from long_sequence(5)" +
                    ") timestamp(ts) partition by DAY WAL");
            assertWalExistence(true, tableName, 1);
            assertWalLockExistence(true, tableName, 1);
            assertSegmentExistence(true, tableName, 1, 0);
            assertSegmentLockExistence(true, tableName, 1, 0);
            assertSegmentExistence(false, tableName, 1, 1);
            assertSegmentLockExistence(false, tableName, 1, 1);

            // Create a second segment.
            execute("alter table " + tableName + " add column sss string");
            execute("insert into " + tableName + " values (6, '2022-02-24T00:00:05.000000Z', 'x')");
            assertWalExistence(true, tableName, 1);
            assertSegmentExistence(true, tableName, 1, 0);
            final File segment1DirPath = assertSegmentExistence(true, tableName, 1, 1);
            assertSegmentExistence(false, tableName, 1, 2);
            assertSegmentLockExistence(false, tableName, 1, 2);

            // We write a marker file to prevent the second segment "wal1/1" from being reaped.
            final File pendingDirPath = new File(segment1DirPath, WalUtils.WAL_PENDING_FS_MARKER);
            Assert.assertTrue(pendingDirPath.mkdirs());
            final File pendingFilePath = new File(pendingDirPath, "test.pending");
            Assert.assertTrue(pendingFilePath.createNewFile());

            // Create a third segment.
            execute("alter table " + tableName + " add column ttt string");
            execute("insert into " + tableName + " values (7, '2022-02-24T00:00:06.000000Z', 'x', 'y')");
            assertWalExistence(true, tableName, 1);
            assertSegmentExistence(true, tableName, 1, 0);
            assertSegmentExistence(true, tableName, 1, 1);
            assertSegmentExistence(true, tableName, 1, 2);
            assertSegmentExistence(false, tableName, 1, 3);

            drainWalQueue();
            engine.releaseInactive();

            assertWalExistence(true, tableName, 1);
            assertWalLockEngagement(false, tableName, 1);
            assertSegmentExistence(true, tableName, 1, 0);
            assertSegmentExistence(true, tableName, 1, 1);
            assertSegmentExistence(true, tableName, 1, 2);

            drainPurgeJob();

            assertWalExistence(true, tableName, 1);
            assertSegmentExistence(false, tableName, 1, 0); // Only the first segment is reaped.
            assertSegmentExistence(true, tableName, 1, 1);
            assertSegmentExistence(false, tableName, 1, 2);

            // We remove the marker file to allow the second segment "wal1/1" to be reaped.
            // Since all changes are applied and the wal is unlocked, the whole WAL is reaped.
            Assert.assertTrue(pendingFilePath.delete());

            drainPurgeJob();

            assertWalExistence(false, tableName, 1);
        });
    }

    @Test
    public void testPendingSegmentTasksOnDeletedTable() throws Exception {
        assertMemoryLeak(() -> {
            // Creating a table creates a new WAL with a first segment.
            final String tableName = testName.getMethodName();
            execute("create table " + tableName + " as (" +
                    "select x, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts " +
                    " from long_sequence(5)" +
                    ") timestamp(ts) partition by DAY WAL");
            TableToken tableToken = engine.verifyTableName(tableName);
            assertWalExistence(true, tableName, 1);
            assertWalLockExistence(true, tableName, 1);
            final File segment0DirPath = assertSegmentExistence(true, tableName, 1, 0);
            assertSegmentLockExistence(true, tableName, 1, 0);
            assertSegmentExistence(false, tableName, 1, 1);
            assertSegmentLockExistence(false, tableName, 1, 1);

            // We write a marker file to prevent the segment "wal1/0" from being reaped.
            final File pendingDirPath = new File(segment0DirPath, WalUtils.WAL_PENDING_FS_MARKER);
            Assert.assertTrue(pendingDirPath.mkdirs());
            final File pendingFilePath = new File(pendingDirPath, "test.pending");
            Assert.assertTrue(pendingFilePath.createNewFile());

            // Drop the table and apply all outstanding operations.
            execute("drop table " + tableName);
            drainWalQueue();
            engine.releaseInactive();
            drainPurgeJob();

            // The table, wal and the segment are intact, but unlocked.
            assertTableExistence(true, tableToken);
            assertWalExistence(true, tableToken, 1);
            assertWalLockEngagement(false, tableToken, 1);
            assertSegmentExistence(true, tableToken, 1, 0);
            assertSegmentLockEngagement(false, tableToken, 1, 0);

            // We remove the marker file to allow the segment directory, wal and the whole table dir to be reaped.
            Assert.assertTrue(pendingFilePath.delete());

            drainPurgeJob();
            assertWalExistence(false, tableToken, 1);
            assertTableExistence(false, tableToken);
        });
    }

    @Test
    public void testRemoveWalLockFailure() throws Exception {
        AtomicBoolean allowRemove = new AtomicBoolean();
        FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public int errno() {
                if (!allowRemove.get()) {
                    return 5;  // Access denied.
                } else {
                    return super.errno();
                }
            }

            @Override
            public boolean rmdir(Path name, boolean lazy) {
                if (!allowRemove.get()) {
                    return false;
                } else {
                    return super.rmdir(name, lazy);
                }
            }
        };

        assertMemoryLeak(ff, () -> {
            String tableName = testName.getMethodName();
            execute("create table " + tableName + "("
                    + "x long,"
                    + "ts timestamp"
                    + ") timestamp(ts) partition by DAY WAL");

            execute("insert into " + tableName + " values (1, '2022-02-24T00:00:00.000000Z')");

            drainWalQueue();

            allowRemove.set(true);
            engine.releaseInactive();

            allowRemove.set(false);
            TestUtils.drainPurgeJob(engine, ff);
            assertWalExistence(true, tableName, 1);

            allowRemove.set(true);
            TestUtils.drainPurgeJob(engine, ff);
            assertWalExistence(false, tableName, 1);
        });
    }

    @Test
    public void testRmWalDirFailure() throws Exception {
        String tableName = testName.getMethodName();
        execute("create table " + tableName + "("
                + "x long,"
                + "ts timestamp"
                + ") timestamp(ts) partition by DAY WAL");

        execute("insert into " + tableName + " values (1, '2022-02-24T00:00:00.000000Z')");

        drainWalQueue();

        engine.releaseInactive();

        AtomicBoolean canDelete = new AtomicBoolean(false);
        FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public boolean rmdir(Path path, boolean lazy) {
                if (Utf8s.endsWithAscii(path, Files.SEPARATOR + WalUtils.WAL_NAME_BASE + "1") && !canDelete.get()) {
                    return false;
                } else {
                    return super.rmdir(path, lazy);
                }
            }
        };

        TestUtils.drainPurgeJob(engine, ff);

        assertWalExistence(true, tableName, 1);

        canDelete.set(true);
        TestUtils.drainPurgeJob(engine, ff);

        assertWalExistence(false, tableName, 1);
    }

    @Test
    public void testRollback() throws Exception {
        String tableName = testName.getMethodName();
        execute("create table " + tableName + "("
                + "x long,"
                + "ts timestamp"
                + ") timestamp(ts) partition by DAY WAL");

        execute("insert into " + tableName + " values (1, '2022-02-24T00:00:00.000000Z')");

        try (WalWriter walWriter1 = getWalWriter(tableName)) {
            // Alter is committed.
            addColumn(walWriter1, "i1");

            // Row insert is rolled back.
            TableWriter.Row row = walWriter1.newRow(MicrosTimestampDriver.floor("2022-02-25"));
            row.putLong(0, 2);
            row.putLong(2, 2);
            row.append();
            walWriter1.rollback();
        }

        assertWalExistence(true, tableName, 1);
        assertSegmentExistence(true, tableName, 1, 0);
        assertWalExistence(false, tableName, 2);

        drainWalQueue();
        engine.releaseInactive();
        drainPurgeJob();
        assertSql("x\tts\ti1\n" +
                "1\t2022-02-24T00:00:00.000000Z\tnull\n", tableName);
        assertWalExistence(false, tableName, 1);
    }

    @Test
    public void testSegmentDirnamePattern() throws Exception {
        // We create a directory called "stuff" inside the wal1 and ensure it's not deleted.
        // This tests that non-numeric directories aren't matched.
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            execute("create table " + tableName + "("
                    + "x long,"
                    + "ts timestamp"
                    + ") timestamp(ts) partition by DAY WAL");
            execute("insert into " + tableName + " values (1, '2022-02-24T00:00:00.000000Z')");

            drainWalQueue();
            assertWalExistence(true, tableName, 1);
            assertWalLockExistence(true, tableName, 1);
            assertSegmentExistence(true, tableName, 1, 0);
            assertSegmentLockExistence(true, tableName, 1, 0);
            assertSegmentLockEngagement(true, tableName, 1, 0);  // Segment 0 is locked.
            assertWalLockEngagement(true, tableName, 1);

            execute("alter table " + tableName + " add column s1 string");
            execute("insert into " + tableName + " values (2, '2022-02-24T00:00:01.000000Z', 'x')");
            assertWalLockEngagement(true, tableName, 1);
            assertSegmentExistence(true, tableName, 1, 1);
            assertSegmentLockExistence(true, tableName, 1, 1);
            assertSegmentLockEngagement(false, tableName, 1, 0);  // Segment 0 is unlocked.
            assertSegmentLockEngagement(true, tableName, 1, 1);  // Segment 1 is locked.

            CharSequence root = engine.getConfiguration().getDbRoot();
            try (Path path = new Path()) {
                final FilesFacade ff = engine.getConfiguration().getFilesFacade();
                path.of(root).concat(engine.verifyTableName(tableName)).concat("wal1").concat("stuff");
                ff.mkdir(path.$(), configuration.getMkDirMode());
                Assert.assertTrue(path.toString(), ff.exists(path.$()));

                drainPurgeJob();
                assertSegmentLockExistence(false, tableName, 1, 0);

                // "stuff" is untouched.
                Assert.assertTrue(path.toString(), ff.exists(path.$()));

                // After draining, releasing and purging, it's all deleted.
                drainWalQueue();
                engine.releaseInactive();
                drainPurgeJob();

                Assert.assertFalse(path.toString(), ff.exists(path.$()));
                assertWalExistence(false, tableName, 1);
            }
        });
    }

    @Test
    public void testSegmentLockedWhenSweeping() throws Exception {

        AtomicReference<WalWriter> walWriter1Ref = new AtomicReference<>();
        FilesFacade testFF = new TestFilesFacadeImpl() {

            @Override
            public boolean close(long fd) {
                if (fd > -1 && fd == this.fd) {
                    // Create another 2 segments after Sequencer transaction scan
                    if (walWriter1Ref.get() != null) {
                        walWriter1Ref.get().commit();
                        addColumnAndRow(walWriter1Ref.get(), "i1");
                    }
                    this.fd = -1;
                }
                return super.close(fd);
            }

            @Override
            public long openRO(LPSZ name) {
                if (Utf8s.endsWithAscii(name, TXN_FILE_NAME)) {
                    return this.fd = super.openRO(name);
                }
                return super.openRO(name);
            }
        };

        assertMemoryLeak(testFF, () -> {
            final String tableName = testName.getMethodName();
            execute("create table " + tableName + "("
                    + "x long,"
                    + "ts timestamp"
                    + ") timestamp(ts) partition by DAY WAL");

            execute("insert into " + tableName + " values (1, '2022-02-24T00:00:00.000000Z')");
            assertWalExistence(true, tableName, 1);
            assertSegmentExistence(true, tableName, 1, 0);
            drainWalQueue();

            try (WalWriter walWriter1 = getWalWriter(tableName)) {
                TableWriter.Row row = walWriter1.newRow(MicrosTimestampDriver.floor("2022-02-24"));
                row.putLong(0, 11);
                row.append();

                walWriter1Ref.set(walWriter1);
                // This will create new segments 1 and 2 in wal1 after Sequencer transaction scan in overridden FilesFacade
                drainPurgeJob();
                walWriter1Ref.set(null);

                assertSegmentExistence(true, tableName, walWriter1.getWalId(), 0);
                assertSegmentExistence(true, tableName, walWriter1.getWalId(), 1);

                drainWalQueue();

                assertSql("x\tts\ti1\n" +
                        "1\t2022-02-24T00:00:00.000000Z\tnull\n" +
                        "11\t2022-02-24T00:00:00.000000Z\tnull\n" +
                        "2\t2022-02-25T00:00:00.000000Z\t2\n", tableName);


                // All applied, all segments can be deleted.
                walWriter1.close();
                drainPurgeJob();

                assertSegmentExistence(false, tableName, walWriter1.getWalId(), 0);
                assertSegmentExistence(true, tableName, walWriter1.getWalId(), 1);

                releaseInactive(engine);
                drainPurgeJob();
                assertWalExistence(false, tableName, walWriter1.getWalId());
            }
        });
    }

    @Test
    public void testSegmentsCreatedWhenSweeping() throws Exception {

        AtomicReference<WalWriter> walWriter1Ref = new AtomicReference<>();
        FilesFacade testFF = new TestFilesFacadeImpl() {

            @Override
            public boolean close(long fd) {
                if (fd > -1 && fd == this.fd) {
                    // Create another 2 segments after Sequencer transaction scan
                    if (walWriter1Ref.get() != null) {
                        addColumnAndRow(walWriter1Ref.get(), "i1");
                        addColumnAndRow(walWriter1Ref.get(), "i2");
                    }
                    this.fd = -1;
                }
                return super.close(fd);
            }

            @Override
            public long openRO(LPSZ name) {
                if (Utf8s.endsWithAscii(name, TXN_FILE_NAME)) {
                    return this.fd = super.openRO(name);
                }
                return super.openRO(name);
            }
        };

        assertMemoryLeak(testFF, () -> {
            final String tableName = testName.getMethodName();
            execute(
                    "create table " + tableName + "("
                            + "x long,"
                            + "ts timestamp"
                            + ") timestamp(ts) partition by DAY WAL"
            );

            execute("insert into " + tableName + " values (1, '2022-02-24T00:00:00.000000Z')");
            assertWalExistence(true, tableName, 1);
            assertSegmentExistence(true, tableName, 1, 0);
            drainWalQueue();

            try (WalWriter walWriter1 = getWalWriter(tableName)) {
                walWriter1Ref.set(walWriter1);
                // This will create new segments 1 and 2 in wal1 after Sequencer transaction scan in overridden FilesFacade
                drainPurgeJob();
                walWriter1Ref.set(null);

                assertSegmentExistence(true, tableName, walWriter1.getWalId(), 1);
                assertSegmentExistence(true, tableName, walWriter1.getWalId(), 2);

                drainWalQueue();

                assertSql("x\tts\ti1\ti2\n" +
                        "1\t2022-02-24T00:00:00.000000Z\tnull\tnull\n" +
                        "2\t2022-02-25T00:00:00.000000Z\t2\tnull\n" +
                        "2\t2022-02-25T00:00:00.000000Z\t2\tnull\n", tableName);


                // All applied, all segments can be deleted.
                walWriter1.close();
                drainPurgeJob();

                assertSegmentExistence(false, tableName, walWriter1.getWalId(), 1);
                assertSegmentExistence(true, tableName, walWriter1.getWalId(), 2);

                releaseInactive(engine);
                drainPurgeJob();
                assertWalExistence(false, tableName, walWriter1.getWalId());
            }
        });
    }

    @Test
    public void testTwoSegments() throws Exception {
        assertMemoryLeak(() -> {
            // Creating a table creates a new WAL with a single segment.
            String tableName = testName.getMethodName();
            execute("create table " + tableName + " as (" +
                    "select x, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts " +
                    " from long_sequence(5)" +
                    ") timestamp(ts) partition by DAY WAL");
            assertWalExistence(true, tableName, 1);
            assertWalLockExistence(true, tableName, 1);
            assertSegmentExistence(true, tableName, 1, 0);
            assertSegmentLockExistence(true, tableName, 1, 0);
            assertSegmentExistence(false, tableName, 1, 1);
            assertSegmentLockExistence(false, tableName, 1, 1);

            // Altering the table doesn't create a new segment yet.
            execute("alter table " + tableName + " add column sss string");
            assertSegmentExistence(false, tableName, 1, 1);

            // Inserting data with the added column creates a new segment.
            execute("insert into " + tableName + " values (6, '2022-02-24T00:00:05.000000Z', 'x')");
            assertWalExistence(true, tableName, 1);
            assertSegmentExistence(true, tableName, 1, 0);
            assertSegmentExistence(true, tableName, 1, 1);

            // Purging does nothing as the WAL has not been drained yet.
            drainPurgeJob();
            assertWalExistence(true, tableName, 1);
            assertSegmentExistence(true, tableName, 1, 0);
            assertSegmentExistence(true, tableName, 1, 1);

            // After draining the wal queue, all the WAL data is still there.
            drainWalQueue();
            assertSql("x\tts\tsss\n" +
                    "1\t2022-02-24T00:00:00.000000Z\t\n" +
                    "2\t2022-02-24T00:00:01.000000Z\t\n" +
                    "3\t2022-02-24T00:00:02.000000Z\t\n" +
                    "4\t2022-02-24T00:00:03.000000Z\t\n" +
                    "5\t2022-02-24T00:00:04.000000Z\t\n" +
                    "6\t2022-02-24T00:00:05.000000Z\tx\n", tableName);
            assertWalExistence(true, tableName, 1);
            assertSegmentExistence(true, tableName, 1, 0);
            assertSegmentExistence(true, tableName, 1, 1);

            // Purging will now clean up the inactive segmentId==0, but leave segmentId==1
            drainPurgeJob();
            assertWalExistence(true, tableName, 1);
            assertWalLockExistence(true, tableName, 1);
            assertSegmentExistence(false, tableName, 1, 0);
            assertSegmentLockExistence(false, tableName, 1, 0);
            assertSegmentExistence(true, tableName, 1, 1);
            assertSegmentLockExistence(true, tableName, 1, 1);

            // Releasing inactive writers and purging will also delete the wal directory.
            engine.releaseInactive();
            drainPurgeJob();
            assertWalExistence(false, tableName, 1);
            assertWalLockExistence(false, tableName, 1);
        });
    }

    @Test
    public void testUntrackedSegment() throws Exception {
        // Test a segment that was created but never tracked by the sequencer.
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            execute("create table " + tableName + "("
                    + "x long,"
                    + "ts timestamp"
                    + ") timestamp(ts) partition by DAY WAL");
            assertWalExistence(false, tableName, 1);
            getTableWriterAPI(tableName).close();

            assertWalExistence(true, tableName, 1);
            assertWalLockExistence(true, tableName, 1);
            assertSegmentExistence(true, tableName, 1, 0);

            // Released before committing anything to the sequencer.
            engine.releaseInactive();

            drainPurgeJob();
            assertWalExistence(false, tableName, 1);
            assertWalLockExistence(false, tableName, 1);
        });
    }

    @Test
    public void testWalDirnamePatterns() throws Exception {
        // We create a directory called "waldo" inside the table dir and ensure it's not deleted.
        // This tests that the directory isn't matched.
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            execute("create table " + tableName + "("
                    + "x long,"
                    + "ts timestamp"
                    + ") timestamp(ts) partition by DAY WAL");

            CharSequence root = engine.getConfiguration().getDbRoot();
            try (Path path = new Path()) {
                final FilesFacade ff = engine.getConfiguration().getFilesFacade();
                TableToken tableToken = engine.verifyTableName(tableName);
                path.of(root).concat(tableToken).$();
                Assert.assertTrue(path.toString(), ff.exists(path.$()));
                path.of(root).concat(tableToken).concat("waldo");
                ff.mkdir(path.$(), configuration.getMkDirMode());
                Assert.assertTrue(path.toString(), ff.exists(path.$()));

                // Purging will not delete waldo: Wal name not matched.
                drainPurgeJob();
                Assert.assertTrue(path.toString(), ff.exists(path.$()));

                // idempotency check.
                for (int count = 0; count < 1000; ++count) {
                    drainPurgeJob();
                    Assert.assertTrue(path.toString(), ff.exists(path.$()));
                }

                path.of(root).concat(tableToken).concat("wal1000");
                ff.mkdir(path.$(), configuration.getMkDirMode());
                Assert.assertTrue(path.toString(), ff.exists(path.$()));

                // Purging will delete wal1000: Wal name matched and the WAL has no lock.
                drainPurgeJob();
                Assert.assertFalse(path.toString(), ff.exists(path.$()));
            }
        });
    }

    @Test
    public void testWalPurgeJobLock() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            execute("create table " + tableName + " as (" +
                    "select x, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts " +
                    " from long_sequence(5)" +
                    ") timestamp(ts) partition by DAY WAL");

            assertWalExistence(true, tableName, 1);
            assertSegmentExistence(true, tableName, 1, 0);

            drainWalQueue();

            assertWalExistence(true, tableName, 1);

            assertSql("x\tts\n" +
                    "1\t2022-02-24T00:00:00.000000Z\n" +
                    "2\t2022-02-24T00:00:01.000000Z\n" +
                    "3\t2022-02-24T00:00:02.000000Z\n" +
                    "4\t2022-02-24T00:00:03.000000Z\n" +
                    "5\t2022-02-24T00:00:04.000000Z\n", tableName);

            engine.releaseInactive();

            final long interval = engine.getConfiguration().getWalPurgeInterval() * 1000;  // ms to us.
            setCurrentMicros(interval + 1);  // Set to some point in time that's not 0.
            try (WalPurgeJob job = new WalPurgeJob(engine)) {
                final SimpleWaitingLock lock = job.getRunLock();
                try {
                    lock.lock();
                    job.drain(0);
                } finally {
                    lock.unlock();
                    assertSegmentExistence(true, tableName, 1, 0);
                    assertWalExistence(true, tableName, 1);
                }

                setCurrentMicros(currentMicros + 2 * interval);
                job.drain(0);

                assertSegmentExistence(false, tableName, 1, 0);
                assertWalExistence(false, tableName, 1);
            }
        });
    }

    @Test
    public void testWalPurgedAfterUpdateZeroRecordsTransaction() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            execute("create table " + tableName + " as (" +
                    "select x, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts " +
                    " from long_sequence(5)" +
                    ") timestamp(ts) partition by DAY WAL");

            update("update " + tableName + " set x = 1 where x < 0");

            drainWalQueue();

            assertWalExistence(true, tableName, 1);

            assertSql("x\tts\n" +
                    "1\t2022-02-24T00:00:00.000000Z\n" +
                    "2\t2022-02-24T00:00:01.000000Z\n" +
                    "3\t2022-02-24T00:00:02.000000Z\n" +
                    "4\t2022-02-24T00:00:03.000000Z\n" +
                    "5\t2022-02-24T00:00:04.000000Z\n", tableName);

            engine.releaseInactive();

            drainPurgeJob();

            assertSegmentExistence(false, tableName, 1, 0);
            assertWalExistence(false, tableName, 1);
        });
    }

    private static void addColumnAndRow(WalWriter writer, String columnName) {
        TestUtils.unchecked(() -> {
            addColumn(writer, columnName);
            TableWriter.Row row = writer.newRow(MicrosTimestampDriver.floor("2022-02-25"));
            row.putLong(0, 2);
            row.putInt(2, 2);
            row.append();
            writer.commit();
        });
    }

    private void assertExistence(boolean exists, TableToken tableToken) {
        final CharSequence root = engine.getConfiguration().getDbRoot();
        try (Path path = new Path()) {
            path.of(root).concat(tableToken).concat(WalUtils.SEQ_DIR).$();
            Assert.assertEquals(Utf8s.toString(path), exists, TestFilesFacadeImpl.INSTANCE.exists(path.$()));
        }
    }

    private void assertNoMoreEvents(TestDeleter deleter, int evIndex) {
        if (deleter.events.size() > evIndex) {
            StringBuilder sb = new StringBuilder();
            for (int i = evIndex; i < deleter.events.size(); i++) {
                sb.append(deleter.events.get(i)).append(", ");
            }
            Assert.fail("Unexpected events: " + sb);
        }
    }

    private void assertSeqPartExistence(boolean exists, TableToken tableToken, int partNo) {
        Path path = Path.getThreadLocal(engine.getConfiguration().getDbRoot());
        path.of(root).concat(tableToken).concat(SEQ_DIR).concat(WalUtils.TXNLOG_PARTS_DIR).concat(String.valueOf(partNo)).$();
        Assert.assertEquals(Utf8s.toString(path), exists, TestFilesFacadeImpl.INSTANCE.exists(path.$()));
    }

    private void createPendingFile(TableToken tableToken) {
        final CharSequence root = engine.getConfiguration().getDbRoot();
        try (Path path = new Path()) {
            path.of(root).concat(tableToken).concat(WalUtils.SEQ_DIR).concat(WalUtils.WAL_PENDING_FS_MARKER);
            ff.mkdir(path.$(), configuration.getMkDirMode());
            path.concat("test.pending");
            ff.touch(path.$());
        }
    }

    private void removePendingFile(TableToken tableToken) {
        final CharSequence root = engine.getConfiguration().getDbRoot();
        try (Path path = new Path()) {
            path.of(root).concat(tableToken).concat(WalUtils.SEQ_DIR).concat(WalUtils.WAL_PENDING_FS_MARKER);
            path.concat("test.pending");
            ff.remove(path.$());
        }
    }

    static void addColumn(WalWriter writer, String columnName) {
        writer.addColumn(columnName, ColumnType.INT);
    }

    private static class DeletionEvent {
        final Integer segmentId;
        final int walId;

        public DeletionEvent(int walId) {
            this.walId = walId;
            this.segmentId = null;
        }

        public DeletionEvent(int walId, int segmentId) {
            this.walId = walId;
            this.segmentId = segmentId;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof DeletionEvent) {
                DeletionEvent other = (DeletionEvent) obj;
                return walId == other.walId && Objects.equals(segmentId, other.segmentId);
            }
            return false;
        }

        @Override
        public String toString() {
            if (segmentId == null) {
                return "wal" + walId;
            } else {
                return "wal" + walId + "/" + segmentId;
            }
        }
    }

    private static class TestDeleter implements WalPurgeJob.Deleter {
        public final LongList closedFds = new LongList();
        public final ObjList<DeletionEvent> events = new ObjList<>();

        @Override
        public void deleteSegmentDirectory(int walId, int segmentId, long lockFd) {
            events.add(new DeletionEvent(walId, segmentId));
        }

        @Override
        public void deleteSequencerPart(int seqPart) {
            assert false;
        }

        @Override
        public void deleteWalDirectory(int walId, long lockFd) {
            events.add(new DeletionEvent(walId));
        }

        @Override
        public void unlock(long lockFd) {
            if (lockFd > -1) {
                closedFds.add(lockFd);
            }
        }
    }
}
