/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.griffin.wal;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.wal.TableWriterFrontend;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlUtil;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.griffin.engine.ops.AlterOperationBuilder;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.std.*;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.NativeLPSZ;
import io.questdb.std.str.Path;
import org.junit.Assert;
import org.junit.Test;

public class WalPurgeJobTest  extends AbstractGriffinTest {

    private void assertWalExistence(boolean expectExists, String tableName, int walId) {
        final CharSequence root = engine.getConfiguration().getRoot();
        try (Path path = new Path()) {
            path.of(root).concat(tableName).concat("wal").put(walId).$();
            Assert.assertEquals(Chars.toString(path), expectExists, FilesFacadeImpl.INSTANCE.exists(path));
        }
    }

    private void assertWalLockExistence(boolean expectExists, String tableName, int walId) {
        final CharSequence root = engine.getConfiguration().getRoot();
        try (Path path = new Path()) {
            path.of(root).concat(tableName).concat("wal").put(walId).put(".lock").$();
            Assert.assertEquals(Chars.toString(path), expectExists, FilesFacadeImpl.INSTANCE.exists(path));
        }
    }

    private void assertSegmentExistence(boolean expectExists, String tableName, int walId, int segmentId) {
        final CharSequence root = engine.getConfiguration().getRoot();
        try (Path path = new Path()) {
            path.of(root).concat(tableName).concat("wal").put(walId).slash().put(segmentId).$();
            Assert.assertEquals(Chars.toString(path), expectExists, FilesFacadeImpl.INSTANCE.exists(path));
        }
    }

    private void assertSegmentLockExistence(boolean expectExists, String tableName, int walId, int segmentId) {
        final CharSequence root = engine.getConfiguration().getRoot();
        try (Path path = new Path()) {
            path.of(root).concat(tableName).concat("wal").put(walId).slash().put(segmentId).put(".lock").$();
            Assert.assertEquals(Chars.toString(path), expectExists, FilesFacadeImpl.INSTANCE.exists(path));
        }
    }

    private static boolean couldObtainLock(Path path) {
        final long lockFd = TableUtils.lock(FilesFacadeImpl.INSTANCE, path, false);
        if (lockFd != -1L) {
            FilesFacadeImpl.INSTANCE.close(lockFd);
            return true;  // Could lock/unlock.
        }
        return false;  // Could not obtain lock.
    }

    private void assertWalLockEngagement(boolean expectLocked, String tableName, int walId) {
        final CharSequence root = engine.getConfiguration().getRoot();
        try (Path path = new Path()) {
            path.of(root).concat(tableName).concat("wal").put(walId).put(".lock").$();
            final boolean could = couldObtainLock(path);
            Assert.assertEquals(Chars.toString(path), expectLocked, !could);
        }
    }

    private void assertSegmentLockEngagement(boolean expectLocked, String tableName, int walId, int segmentId) {
        final CharSequence root = engine.getConfiguration().getRoot();
        try (Path path = new Path()) {
            path.of(root).concat(tableName).concat("wal").put(walId).slash().put(segmentId).put(".lock").$();
            final boolean could = couldObtainLock(path);
            Assert.assertEquals(Chars.toString(path), expectLocked, !could);
        }
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

            // After draining the wall queue, all the WAL data is still there.
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
    public void testUntrackedSegment()  throws Exception {
        // Test a segment that was created but never tracked by the sequencer.
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            compile("create table " + tableName + "("
                    + "x long,"
                    + "ts timestamp"
                    + ") timestamp(ts) partition by DAY WAL");
            assertWalExistence(false, tableName, 1);
            try (TableWriterFrontend twf = engine.getTableWriterFrontEnd(sqlExecutionContext.getCairoSecurityContext(), tableName, "test")) {
                // No-op. We just want to create a WAL.
            }

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
                path.of(root).concat(tableName).$();
                Assert.assertEquals(path.toString(), true, ff.exists(path));
                path.of(root).concat(tableName).concat("waldo").$();
                ff.mkdir(path, configuration.getMkDirMode());
                Assert.assertEquals(path.toString(), true, ff.exists(path));

                // Purging will not delete waldo: Wal name not matched.
                runWalPurgeJob();
                Assert.assertEquals(path.toString(), true, ff.exists(path));

                // idempotency check.
                for (int count = 0; count < 1000; ++count) {
                    runWalPurgeJob();
                    Assert.assertEquals(path.toString(), true, ff.exists(path));
                }

                path.of(root).concat(tableName).concat("wal1000").$();
                ff.mkdir(path, configuration.getMkDirMode());
                Assert.assertEquals(path.toString(), true, ff.exists(path));

                // Purging will delete wal1000: Wal name matched and the WAL has no lock.
                runWalPurgeJob();
                Assert.assertEquals(path.toString(), false, ff.exists(path));
            }
        });
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
                path.of(root).concat(tableName).concat("wal1").concat("stuff").$();
                ff.mkdir(path, configuration.getMkDirMode());
                Assert.assertEquals(path.toString(), true, ff.exists(path));

                runWalPurgeJob();
                assertSegmentLockExistence(false, tableName, 1, 0);

                // "stuff" is untouched.
                Assert.assertEquals(path.toString(), true, ff.exists(path));

                // After draining, releasing and purging, it's all deleted.
                drainWalQueue();
                engine.releaseInactive();
                runWalPurgeJob();

                Assert.assertEquals(path.toString(), false, ff.exists(path));
                assertWalExistence(false, tableName, 1);
            }
        });
    }

    static void addColumn(TableWriterFrontend writer, String columnName, int columnType) throws SqlException {
        AlterOperationBuilder addColumnC = new AlterOperationBuilder().ofAddColumn(0, Chars.toString(writer.getTableName()), 0);
        addColumnC.ofAddColumn(columnName, columnType, 0, false, false, 0);
        writer.applyAlter(addColumnC.build(), true);
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
        FilesFacade testFF = new FilesFacadeImpl() {
            @Override
            public void iterateDir(LPSZ path, FindVisitor func) {
                if (path.toString().endsWith(Files.SEPARATOR + tableName)) {
                    final NativeLPSZ name = new NativeLPSZ();
                    super.iterateDir(path, (long pUtf8NameZ, int type) -> {
                        name.of(pUtf8NameZ);
                        if (!name.toString().equals("wal2")) {
                            func.onFind(pUtf8NameZ, type);
                        }
                    });
                }
                else {
                    super.iterateDir(path, func);
                }
            }
        };

        try (WalWriter walWriter1 = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
            // Assert we've obtained a writer to wal1 and we're not already on wal2.
            assertWalExistence(true, tableName, 1);
            assertWalExistence(false, tableName, 2);
            assertSegmentExistence(true, tableName, 1, 0);
            assertSegmentExistence(false, tableName, 1, 1);
            assertSegmentLockEngagement(true, tableName, 1, 0);

            addColumn(walWriter1, "i1", ColumnType.INT);
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

            try (WalWriter walWriter2 = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
                assertWalExistence(true, tableName, 1);
                assertWalExistence(true, tableName, 2);
                assertSegmentExistence(true, tableName, 2, 0);
                assertSegmentExistence(false, tableName, 2, 1);

                TableWriter.Row row3 = walWriter2.newRow(IntervalUtils.parseFloorPartialTimestamp("2022-02-26"));
                row3.putLong(0, 3);
                row3.putInt(2, 3);
                row3.append();
                walWriter2.commit();

                addColumn(walWriter2, "i2", ColumnType.INT);
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
                assertSegmentExistence(true, tableName, 2, 0);  // KEPT!
                assertSegmentExistence(true, tableName, 2, 1);  // wal2/1 kept
                assertSegmentLockEngagement(true, tableName, 2, 1);  // wal2/1 locked
            }
        }
    }

    @Test
    public void testRollback() throws Exception {
        String tableName = testName.getMethodName();
        compile("create table " + tableName + "("
                + "x long,"
                + "ts timestamp"
                + ") timestamp(ts) partition by DAY WAL");

        compile("insert into " + tableName + " values (1, '2022-02-24T00:00:00.000000Z')");

        try (WalWriter walWriter1 = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
            // Alter is committed.
            addColumn(walWriter1, "i1", ColumnType.INT);

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
}
