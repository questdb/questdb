/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.test.griffin.wal;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.wal.WalPurgeJob;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.mp.SimpleWaitingLock;
import io.questdb.std.*;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.NativeLPSZ;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractGriffinTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static io.questdb.cairo.TableUtils.TXN_FILE_NAME;

public class WalPurgeJobTest extends AbstractGriffinTest {
    @Test
    public void testClosedButUnappliedSegment() throws Exception {
        // Test two segment with changes committed to the sequencer, but never applied to the table.
        // The WAL is closed but nothing can be purged.
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            compile("create table " + tableName + "("
                    + "x long,"
                    + "ts timestamp"
                    + ") timestamp(ts) partition by DAY WAL");
            compile("insert into " + tableName + " values (1, '2022-02-24T00:00:00.000000Z')");
            compile("alter table " + tableName + " add column s1 string");
            compile("insert into " + tableName + " values (2, '2022-02-24T00:00:01.000000Z', 'x')");
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
            runWalPurgeJob();
            assertWalExistence(true, tableName, 1);
            assertSegmentExistence(true, tableName, 1, 0);
            assertSegmentExistence(true, tableName, 1, 1);

            // Idempotency check
            for (int count = 0; count < 1000; ++count) {
                runWalPurgeJob();
                assertWalExistence(true, tableName, 1);
                assertSegmentExistence(true, tableName, 1, 0);
                assertSegmentExistence(true, tableName, 1, 1);
            }

            // After draining, it's all deleted.
            drainWalQueue();
            assertSql(tableName, "x\tts\ts1\n" +
                    "1\t2022-02-24T00:00:00.000000Z\t\n" +
                    "2\t2022-02-24T00:00:01.000000Z\tx\n");
            runWalPurgeJob();
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

        final String tableName = testName.getMethodName();
        compile("create table " + tableName + "("
                + "x long,"
                + "ts timestamp"
                + ") timestamp(ts) partition by DAY WAL");

        compile("insert into " + tableName + " values (1, '2022-02-24T00:00:00.000000Z')");
        assertWalExistence(true, tableName, 1);

        // A test FilesFacade that hides the "wal2" directory.
        String dirNamePath = Files.SEPARATOR + Chars.toString(engine.verifyTableName(tableName).getDirName());
        FilesFacade testFF = new TestFilesFacadeImpl() {
            @Override
            public void iterateDir(LPSZ path, FindVisitor func) {
                if (Chars.endsWith(path, dirNamePath)) {
                    final NativeLPSZ name = new NativeLPSZ();
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
            TableWriter.Row row2 = walWriter1.newRow(IntervalUtils.parseFloorPartialTimestamp("2022-02-25"));
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

                TableWriter.Row row3 = walWriter2.newRow(IntervalUtils.parseFloorPartialTimestamp("2022-02-26"));
                row3.putLong(0, 3);
                row3.putInt(2, 3);
                row3.append();
                walWriter2.commit();

                addColumn(walWriter2, "i2");
                TableWriter.Row row4 = walWriter2.newRow(IntervalUtils.parseFloorPartialTimestamp("2022-02-27"));
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

                assertSql(tableName, "x\tts\ti1\ti2\n" +
                        "1\t2022-02-24T00:00:00.000000Z\tNaN\tNaN\n" +
                        "2\t2022-02-25T00:00:00.000000Z\t2\tNaN\n" +
                        "3\t2022-02-26T00:00:00.000000Z\t3\tNaN\n" +
                        "4\t2022-02-27T00:00:00.000000Z\t4\t4\n");

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

                runWalPurgeJob(testFF);

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
            compile("create table " + tableName + "("
                    + "x long,"
                    + "ts timestamp"
                    + ") timestamp(ts) partition by DAY WAL");

            final long interval = engine.getConfiguration().getWalPurgeInterval() * 1000;  // ms to us.
            currentMicros = interval + 1;  // Set to some point in time that's not 0.

            try (WalPurgeJob walPurgeJob = new WalPurgeJob(engine, ff, configuration.getMicrosecondClock())) {
                counter.set(0);

                walPurgeJob.delayByHalfInterval();
                walPurgeJob.run(0);
                Assert.assertEquals(0, counter.get());
                currentMicros += interval / 2 + 1;
                walPurgeJob.run(0);
                Assert.assertEquals(1, counter.get());
                currentMicros += interval / 2 + 1;
                walPurgeJob.run(0);
                walPurgeJob.run(0);
                walPurgeJob.run(0);
                Assert.assertEquals(1, counter.get());
                currentMicros += interval;
                walPurgeJob.run(0);
                Assert.assertEquals(2, counter.get());
                currentMicros += 10 * interval;
                walPurgeJob.run(0);
                Assert.assertEquals(3, counter.get());
            }
        });
    }

    @Test
    public void testLastSegmentUnlockedPrevLocked() throws Exception {
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
        WalPurgeJob.Logic logic = new WalPurgeJob.Logic(deleter);
        TableToken tableToken = new TableToken("test", "test~1", 42, true);
        logic.reset(tableToken);
        logic.trackDiscoveredSegment(1, 1, false, false);
        logic.trackDiscoveredSegment(1, 2, false, false);
        logic.trackDiscoveredSegment(1, 3, false, false);
        logic.trackDiscoveredSegment(1, 4, false, false);
        logic.trackDiscoveredSegment(1, 5, false, false);
        logic.trackDiscoveredSegment(1, 6, false, true);
        logic.trackDiscoveredSegment(1, 7, false, false);
        logic.trackDiscoveredWal(1, false, true);
        logic.trackDiscoveredSegment(2, 0, false, false);
        logic.trackDiscoveredSegment(2, 1, false, false);
        logic.trackDiscoveredSegment(2, 2, false, false);
        logic.trackDiscoveredSegment(2, 3, false, false);
        logic.trackDiscoveredSegment(2, 4, false, true);
        logic.trackDiscoveredWal(2, false, true);
        logic.trackDiscoveredSegment(3, 0, false, false);
        logic.trackDiscoveredSegment(3, 1, false, false);
        logic.trackDiscoveredSegment(3, 2, false, false);
        logic.trackDiscoveredSegment(3, 3, false, true);
        logic.trackDiscoveredWal(3, false, true);
        logic.trackDiscoveredSegment(4, 0, false, false);
        logic.trackDiscoveredSegment(4, 1, false, false);
        logic.trackDiscoveredSegment(4, 2, false, false);
        logic.trackDiscoveredSegment(4, 3, false, false);
        logic.trackDiscoveredSegment(4, 4, false, false);
        logic.trackDiscoveredSegment(4, 5, false, false);
        logic.trackDiscoveredSegment(4, 6, false, true);
        logic.trackDiscoveredWal(4, false, true);
        logic.trackNextToApplySegment(1, 1);
        logic.trackNextToApplySegment(2, 0);
        logic.trackNextToApplySegment(3, 0);
        logic.trackNextToApplySegment(4, 0);
        logic.run();

//        logDeletionEvents(deleter.events);
        int evIndex = 0;
        assertNoMoreEvents(deleter, evIndex);
    }

    @Test
    public void testLastSegmentUnlockedPrevLocked2() throws Exception {
        /*
          discovered=[
              (1,1),(1,2:locked),(1,3),(wal1:locked),
              (2,0:locked),(wal2:locked)],
          nextToApply=[
              (1,1),(2,0)]
         */
        TestDeleter deleter = new TestDeleter();
        WalPurgeJob.Logic logic = new WalPurgeJob.Logic(deleter);
        TableToken tableToken = new TableToken("test", "test~1", 42, true);
        logic.reset(tableToken);
        logic.trackDiscoveredSegment(1, 1, false, false);
        logic.trackDiscoveredSegment(1, 2, false, true);
        logic.trackDiscoveredSegment(1, 3, false, false);
        logic.trackDiscoveredWal(1, false, true);
        logic.trackDiscoveredSegment(2, 0, false, true);
        logic.trackDiscoveredWal(2, false, true);
        logic.trackNextToApplySegment(1, 1);
        logic.trackNextToApplySegment(2, 0);
        logic.run();

//        logDeletionEvents(deleter.events);
        int evIndex = 0;
        assertNoMoreEvents(deleter, evIndex);
    }

    @Test
    public void testOneSegment() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            compile("create table " + tableName + " as (" +
                    "select x, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts " +
                    " from long_sequence(5)" +
                    ") timestamp(ts) partition by DAY WAL");

            assertWalExistence(true, tableName, 1);
            assertSegmentExistence(true, tableName, 1, 0);

            drainWalQueue();

            assertWalExistence(true, tableName, 1);

            assertSql(tableName, "x\tts\n" +
                    "1\t2022-02-24T00:00:00.000000Z\n" +
                    "2\t2022-02-24T00:00:01.000000Z\n" +
                    "3\t2022-02-24T00:00:02.000000Z\n" +
                    "4\t2022-02-24T00:00:03.000000Z\n" +
                    "5\t2022-02-24T00:00:04.000000Z\n");

            runWalPurgeJob();

            assertSegmentExistence(true, tableName, 1, 0);
            assertWalExistence(true, tableName, 1);

            engine.releaseInactive();

            runWalPurgeJob();

            assertSegmentExistence(false, tableName, 1, 0);
            assertWalExistence(false, tableName, 1);
        });
    }

    @Test
    public void testPendingSegmentTasks() throws Exception {
        assertMemoryLeak(() -> {
            // Creating a table creates a new WAL with a first segment.
            final String tableName = testName.getMethodName();
            compile("create table " + tableName + " as (" +
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
            compile("alter table " + tableName + " add column sss string");
            executeInsert("insert into " + tableName + " values (6, '2022-02-24T00:00:05.000000Z', 'x')");
            assertWalExistence(true, tableName, 1);
            assertSegmentExistence(true, tableName, 1, 0);
            final File segment1DirPath = assertSegmentExistence(true, tableName, 1, 1);
            assertSegmentExistence(false, tableName, 1, 2);
            assertSegmentLockExistence(false, tableName, 1, 2);

            // We write a marker file to prevent the second segment "wal1/1" from being reaped.
            final File pendingDirPath = new File(segment1DirPath, ".pending");
            Assert.assertTrue(pendingDirPath.mkdirs());
            final File pendingFilePath = new File(pendingDirPath, "task.pending");
            Assert.assertTrue(pendingFilePath.createNewFile());

            // Create a third segment.
            compile("alter table " + tableName + " add column ttt string");
            executeInsert("insert into " + tableName + " values (7, '2022-02-24T00:00:06.000000Z', 'x', 'y')");
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

            runWalPurgeJob();

            assertWalExistence(true, tableName, 1);
            assertSegmentExistence(false, tableName, 1, 0); // Only the first segment is reaped.
            assertSegmentExistence(true, tableName, 1, 1);
            assertSegmentExistence(false, tableName, 1, 2);

            // We remove the marker file to allow the second segment "wal1/1" to be reaped.
            // Since all changes are applied and the wal is unlocked, the whole WAL is reaped.
            Assert.assertTrue(pendingFilePath.delete());

            runWalPurgeJob();

            assertWalExistence(false, tableName, 1);
        });
    }

    @Test
    public void testPendingSegmentTasksOnDeletedTable() throws Exception {
        assertMemoryLeak(() -> {
            // Creating a table creates a new WAL with a first segment.
            final String tableName = testName.getMethodName();
            compile("create table " + tableName + " as (" +
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
            final File pendingDirPath = new File(segment0DirPath, ".pending");
            Assert.assertTrue(pendingDirPath.mkdirs());
            final File pendingFilePath = new File(pendingDirPath, "task.pending");
            Assert.assertTrue(pendingFilePath.createNewFile());

            // Drop the table and apply all outstanding operations.
            compile("drop table " + tableName);
            drainWalQueue();
            engine.releaseInactive();
            runWalPurgeJob();

            // The table, wal and the segment are intact, but unlocked.
            assertTableExistence(true, tableToken);
            assertWalExistence(true, tableToken, 1);
            assertWalLockEngagement(false, tableToken, 1);
            assertSegmentExistence(true, tableToken, 1, 0);
            assertSegmentLockEngagement(false, tableToken, 1, 0);

            // We remove the marker file to allow the segment directory, wal and the whole table dir to be reaped.
            Assert.assertTrue(pendingFilePath.delete());

            runWalPurgeJob();
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
            public boolean remove(LPSZ name) {
                if (!allowRemove.get()) {
                    return false;
                } else {
                    return super.remove(name);
                }
            }
        };

        assertMemoryLeak(ff, () -> {
            String tableName = testName.getMethodName();
            compile("create table " + tableName + "("
                    + "x long,"
                    + "ts timestamp"
                    + ") timestamp(ts) partition by DAY WAL");

            compile("insert into " + tableName + " values (1, '2022-02-24T00:00:00.000000Z')");

            drainWalQueue();

            engine.releaseInactive();


            allowRemove.set(false);
            runWalPurgeJob(ff);
            assertWalLockExistence(true, tableName, 1);

            allowRemove.set(true);
            runWalPurgeJob(ff);
            assertWalLockExistence(false, tableName, 1);
        });
    }

    @Test
    public void testRmWalDirFailure() throws Exception {
        String tableName = testName.getMethodName();
        compile("create table " + tableName + "("
                + "x long,"
                + "ts timestamp"
                + ") timestamp(ts) partition by DAY WAL");

        compile("insert into " + tableName + " values (1, '2022-02-24T00:00:00.000000Z')");

        drainWalQueue();

        engine.releaseInactive();

        AtomicBoolean canDelete = new AtomicBoolean(false);
        FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public int rmdir(Path path) {
                if (Chars.endsWith(path, Files.SEPARATOR + WalUtils.WAL_NAME_BASE + "1") && !canDelete.get()) {
                    return 5;  // Access denied.
                } else {
                    return super.rmdir(path);
                }
            }
        };

        runWalPurgeJob(ff);

        assertWalExistence(true, tableName, 1);

        canDelete.set(true);
        runWalPurgeJob(ff);

        assertWalExistence(false, tableName, 1);
    }

    @Test
    public void testRollback() throws Exception {
        String tableName = testName.getMethodName();
        compile("create table " + tableName + "("
                + "x long,"
                + "ts timestamp"
                + ") timestamp(ts) partition by DAY WAL");

        compile("insert into " + tableName + " values (1, '2022-02-24T00:00:00.000000Z')");

        try (WalWriter walWriter1 = getWalWriter(tableName)) {
            // Alter is committed.
            addColumn(walWriter1, "i1");

            // Row insert is rolled back.
            TableWriter.Row row = walWriter1.newRow(IntervalUtils.parseFloorPartialTimestamp("2022-02-25"));
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
        runWalPurgeJob();
        assertSql(tableName, "x\tts\ti1\n" +
                "1\t2022-02-24T00:00:00.000000Z\tNaN\n");
        assertWalExistence(false, tableName, 1);
    }

    @Test
    public void testSegmentDirnamePattern() throws Exception {
        // We create a directory called "stuff" inside the wal1 and ensure it's not deleted.
        // This tests that non-numeric directories aren't matched.
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            compile("create table " + tableName + "("
                    + "x long,"
                    + "ts timestamp"
                    + ") timestamp(ts) partition by DAY WAL");
            compile("insert into " + tableName + " values (1, '2022-02-24T00:00:00.000000Z')");

            drainWalQueue();
            assertWalExistence(true, tableName, 1);
            assertWalLockExistence(true, tableName, 1);
            assertSegmentExistence(true, tableName, 1, 0);
            assertSegmentLockExistence(true, tableName, 1, 0);
            assertSegmentLockEngagement(true, tableName, 1, 0);  // Segment 0 is locked.
            assertWalLockEngagement(true, tableName, 1);

            compile("alter table " + tableName + " add column s1 string");
            compile("insert into " + tableName + " values (2, '2022-02-24T00:00:01.000000Z', 'x')");
            assertWalLockEngagement(true, tableName, 1);
            assertSegmentExistence(true, tableName, 1, 1);
            assertSegmentLockExistence(true, tableName, 1, 1);
            assertSegmentLockEngagement(false, tableName, 1, 0);  // Segment 0 is unlocked.
            assertSegmentLockEngagement(true, tableName, 1, 1);  // Segment 1 is locked.

            CharSequence root = engine.getConfiguration().getRoot();
            try (Path path = new Path()) {
                final FilesFacade ff = engine.getConfiguration().getFilesFacade();
                path.of(root).concat(engine.verifyTableName(tableName)).concat("wal1").concat("stuff").$();
                ff.mkdir(path, configuration.getMkDirMode());
                Assert.assertTrue(path.toString(), ff.exists(path));

                runWalPurgeJob();
                assertSegmentLockExistence(false, tableName, 1, 0);

                // "stuff" is untouched.
                Assert.assertTrue(path.toString(), ff.exists(path));

                // After draining, releasing and purging, it's all deleted.
                drainWalQueue();
                engine.releaseInactive();
                runWalPurgeJob();

                Assert.assertFalse(path.toString(), ff.exists(path));
                assertWalExistence(false, tableName, 1);
            }
        });
    }

    @Test
    public void testSegmentLockedWhenSweeping() throws Exception {

        AtomicReference<WalWriter> walWriter1Ref = new AtomicReference<>();
        FilesFacade testFF = new TestFilesFacadeImpl() {
            int txnFd = -1;

            @Override
            public boolean close(int fd) {
                if (fd == txnFd) {
                    // Create another 2 segments after Sequencer transaction scan
                    if (walWriter1Ref.get() != null) {
                        walWriter1Ref.get().commit();
                        addColumnAndRow(walWriter1Ref.get(), "i1");
                    }
                    txnFd = -1;
                }
                return super.close(fd);
            }

            @Override
            public int openRO(LPSZ name) {
                if (Chars.endsWith(name, TXN_FILE_NAME)) {
                    return txnFd = super.openRO(name);
                }
                return super.openRO(name);
            }
        };

        assertMemoryLeak(testFF, () -> {
            final String tableName = testName.getMethodName();
            compile("create table " + tableName + "("
                    + "x long,"
                    + "ts timestamp"
                    + ") timestamp(ts) partition by DAY WAL");

            compile("insert into " + tableName + " values (1, '2022-02-24T00:00:00.000000Z')");
            assertWalExistence(true, tableName, 1);
            assertSegmentExistence(true, tableName, 1, 0);
            drainWalQueue();

            WalWriter walWriter1 = getWalWriter(tableName);
            TableWriter.Row row = walWriter1.newRow(IntervalUtils.parseFloorPartialTimestamp("2022-02-24"));
            row.putLong(0, 11);
            row.append();

            walWriter1Ref.set(walWriter1);
            // This will create new segments 1 and 2 in wal1 after Sequencer transaction scan in overridden FilesFacade
            runWalPurgeJob();
            walWriter1Ref.set(null);

            assertSegmentExistence(true, tableName, walWriter1.getWalId(), 0);
            assertSegmentExistence(true, tableName, walWriter1.getWalId(), 1);

            drainWalQueue();

            assertSql(tableName, "x\tts\ti1\n" +
                    "1\t2022-02-24T00:00:00.000000Z\tNaN\n" +
                    "11\t2022-02-24T00:00:00.000000Z\tNaN\n" +
                    "2\t2022-02-25T00:00:00.000000Z\t2\n");


            // All applied, all segments can be deleted.
            walWriter1.close();
            runWalPurgeJob();

            assertSegmentExistence(false, tableName, walWriter1.getWalId(), 0);
            assertSegmentExistence(true, tableName, walWriter1.getWalId(), 1);

            releaseInactive(engine);
            runWalPurgeJob();
            assertWalExistence(false, tableName, walWriter1.getWalId());
        });
    }

    @Test
    public void testSegmentsCreatedWhenSweeping() throws Exception {

        AtomicReference<WalWriter> walWriter1Ref = new AtomicReference<>();
        FilesFacade testFF = new TestFilesFacadeImpl() {
            int txnFd = -1;

            @Override
            public boolean close(int fd) {
                if (fd == txnFd) {
                    // Create another 2 segments after Sequencer transaction scan
                    if (walWriter1Ref.get() != null) {
                        addColumnAndRow(walWriter1Ref.get(), "i1");
                        addColumnAndRow(walWriter1Ref.get(), "i2");
                    }
                    txnFd = -1;
                }
                return super.close(fd);
            }

            @Override
            public int openRO(LPSZ name) {
                if (Chars.endsWith(name, TXN_FILE_NAME)) {
                    return txnFd = super.openRO(name);
                }
                return super.openRO(name);
            }
        };

        assertMemoryLeak(testFF, () -> {
            final String tableName = testName.getMethodName();
            compile("create table " + tableName + "("
                    + "x long,"
                    + "ts timestamp"
                    + ") timestamp(ts) partition by DAY WAL");

            compile("insert into " + tableName + " values (1, '2022-02-24T00:00:00.000000Z')");
            assertWalExistence(true, tableName, 1);
            assertSegmentExistence(true, tableName, 1, 0);
            drainWalQueue();

            WalWriter walWriter1 = getWalWriter(tableName);
            walWriter1Ref.set(walWriter1);
            // This will create new segments 1 and 2 in wal1 after Sequencer transaction scan in overridden FilesFacade
            runWalPurgeJob();
            walWriter1Ref.set(null);

            assertSegmentExistence(true, tableName, walWriter1.getWalId(), 1);
            assertSegmentExistence(true, tableName, walWriter1.getWalId(), 2);

            drainWalQueue();

            assertSql(tableName, "x\tts\ti1\ti2\n" +
                    "1\t2022-02-24T00:00:00.000000Z\tNaN\tNaN\n" +
                    "2\t2022-02-25T00:00:00.000000Z\t2\tNaN\n" +
                    "2\t2022-02-25T00:00:00.000000Z\t2\tNaN\n");


            // All applied, all segments can be deleted.
            walWriter1.close();
            runWalPurgeJob();

            assertSegmentExistence(false, tableName, walWriter1.getWalId(), 1);
            assertSegmentExistence(true, tableName, walWriter1.getWalId(), 2);

            releaseInactive(engine);
            runWalPurgeJob();
            assertWalExistence(false, tableName, walWriter1.getWalId());
        });
    }

    @Test
    public void testTwoSegments() throws Exception {
        assertMemoryLeak(() -> {
            // Creating a table creates a new WAL with a single segment.
            String tableName = testName.getMethodName();
            compile("create table " + tableName + " as (" +
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
            compile("alter table " + tableName + " add column sss string");
            assertSegmentExistence(false, tableName, 1, 1);

            // Inserting data with the added column creates a new segment.
            executeInsert("insert into " + tableName + " values (6, '2022-02-24T00:00:05.000000Z', 'x')");
            assertWalExistence(true, tableName, 1);
            assertSegmentExistence(true, tableName, 1, 0);
            assertSegmentExistence(true, tableName, 1, 1);

            // Purging does nothing as the WAL has not been drained yet.
            runWalPurgeJob();
            assertWalExistence(true, tableName, 1);
            assertSegmentExistence(true, tableName, 1, 0);
            assertSegmentExistence(true, tableName, 1, 1);

            // After draining the wal queue, all the WAL data is still there.
            drainWalQueue();
            assertSql(tableName, "x\tts\tsss\n" +
                    "1\t2022-02-24T00:00:00.000000Z\t\n" +
                    "2\t2022-02-24T00:00:01.000000Z\t\n" +
                    "3\t2022-02-24T00:00:02.000000Z\t\n" +
                    "4\t2022-02-24T00:00:03.000000Z\t\n" +
                    "5\t2022-02-24T00:00:04.000000Z\t\n" +
                    "6\t2022-02-24T00:00:05.000000Z\tx\n");
            assertWalExistence(true, tableName, 1);
            assertSegmentExistence(true, tableName, 1, 0);
            assertSegmentExistence(true, tableName, 1, 1);

            // Purging will now clean up the inactive segmentId==0, but leave segmentId==1
            runWalPurgeJob();
            assertWalExistence(true, tableName, 1);
            assertWalLockExistence(true, tableName, 1);
            assertSegmentExistence(false, tableName, 1, 0);
            assertSegmentLockExistence(false, tableName, 1, 0);
            assertSegmentExistence(true, tableName, 1, 1);
            assertSegmentLockExistence(true, tableName, 1, 1);

            // Releasing inactive writers and purging will also delete the wal directory.
            engine.releaseInactive();
            runWalPurgeJob();
            assertWalExistence(false, tableName, 1);
            assertWalLockExistence(false, tableName, 1);
        });
    }

    @Test
    public void testUntrackedSegment() throws Exception {
        // Test a segment that was created but never tracked by the sequencer.
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            compile("create table " + tableName + "("
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

            runWalPurgeJob();
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
            compile("create table " + tableName + "("
                    + "x long,"
                    + "ts timestamp"
                    + ") timestamp(ts) partition by DAY WAL");

            CharSequence root = engine.getConfiguration().getRoot();
            try (Path path = new Path()) {
                final FilesFacade ff = engine.getConfiguration().getFilesFacade();
                TableToken tableToken = engine.verifyTableName(tableName);
                path.of(root).concat(tableToken).$();
                Assert.assertTrue(path.toString(), ff.exists(path));
                path.of(root).concat(tableToken).concat("waldo").$();
                ff.mkdir(path, configuration.getMkDirMode());
                Assert.assertTrue(path.toString(), ff.exists(path));

                // Purging will not delete waldo: Wal name not matched.
                runWalPurgeJob();
                Assert.assertTrue(path.toString(), ff.exists(path));

                // idempotency check.
                for (int count = 0; count < 1000; ++count) {
                    runWalPurgeJob();
                    Assert.assertTrue(path.toString(), ff.exists(path));
                }

                path.of(root).concat(tableToken).concat("wal1000").$();
                ff.mkdir(path, configuration.getMkDirMode());
                Assert.assertTrue(path.toString(), ff.exists(path));

                // Purging will delete wal1000: Wal name matched and the WAL has no lock.
                runWalPurgeJob();
                Assert.assertFalse(path.toString(), ff.exists(path));
            }
        });
    }

    @Test
    public void testWalPurgeJobLock() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            compile("create table " + tableName + " as (" +
                    "select x, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts " +
                    " from long_sequence(5)" +
                    ") timestamp(ts) partition by DAY WAL");

            assertWalExistence(true, tableName, 1);
            assertSegmentExistence(true, tableName, 1, 0);

            drainWalQueue();

            assertWalExistence(true, tableName, 1);

            assertSql(tableName, "x\tts\n" +
                    "1\t2022-02-24T00:00:00.000000Z\n" +
                    "2\t2022-02-24T00:00:01.000000Z\n" +
                    "3\t2022-02-24T00:00:02.000000Z\n" +
                    "4\t2022-02-24T00:00:03.000000Z\n" +
                    "5\t2022-02-24T00:00:04.000000Z\n");

            engine.releaseInactive();

            final long interval = engine.getConfiguration().getWalPurgeInterval() * 1000;  // ms to us.
            currentMicros = interval + 1;  // Set to some point in time that's not 0.
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

                currentMicros += 2 * interval;
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
            compile("create table " + tableName + " as (" +
                    "select x, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts " +
                    " from long_sequence(5)" +
                    ") timestamp(ts) partition by DAY WAL");

            compile("update " + tableName + " set x = 1 where x < 0");

            drainWalQueue();

            assertWalExistence(true, tableName, 1);

            assertSql(tableName, "x\tts\n" +
                    "1\t2022-02-24T00:00:00.000000Z\n" +
                    "2\t2022-02-24T00:00:01.000000Z\n" +
                    "3\t2022-02-24T00:00:02.000000Z\n" +
                    "4\t2022-02-24T00:00:03.000000Z\n" +
                    "5\t2022-02-24T00:00:04.000000Z\n");

            engine.releaseInactive();

            runWalPurgeJob();

            assertSegmentExistence(false, tableName, 1, 0);
            assertWalExistence(false, tableName, 1);
        });
    }

    private static void addColumnAndRow(WalWriter writer, String columnName) {
        TestUtils.unchecked(() -> {
            addColumn(writer, columnName);
            TableWriter.Row row = writer.newRow(IntervalUtils.parseFloorPartialTimestamp("2022-02-25"));
            row.putLong(0, 2);
            row.putInt(2, 2);
            row.append();
            writer.commit();
        });
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

    private void logDeletionEvents(List<DeletionEvent> events) {
        System.err.println("Deletion events:");
        for (DeletionEvent event : events) {
            System.err.println("  " + event);
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
        public final ObjList<DeletionEvent> events = new ObjList<>();

        @Override
        public void deleteSegmentDirectory(int walId, int segmentId) {
            events.add(new DeletionEvent(walId, segmentId));
        }

        @Override
        public void deleteWalDirectory(int walId) {
            events.add(new DeletionEvent(walId));
        }
    }
}
