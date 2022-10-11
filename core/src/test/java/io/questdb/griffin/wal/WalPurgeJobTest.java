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

import io.questdb.cairo.TableUtils;
import io.questdb.cairo.wal.Sequencer;
import io.questdb.cairo.wal.TableWriterFrontend;
import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.str.Path;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

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

            purgeWalSegments();

            assertSegmentExistence(true, tableName, 1, 0);
            assertWalExistence(true, tableName, 1);

            engine.releaseInactive();

            purgeWalSegments();

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
            purgeWalSegments();
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
            purgeWalSegments();
            assertWalExistence(true, tableName, 1);
            assertWalLockExistence(true, tableName, 1);
            assertSegmentExistence(false, tableName, 1, 0);
            assertSegmentLockExistence(false, tableName, 1, 0);
            assertSegmentExistence(true, tableName, 1, 1);
            assertSegmentLockExistence(true, tableName, 1, 1);

            // Releasing inactive writers and purging will also delete the wal directory.
            engine.releaseInactive();
            purgeWalSegments();
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

            purgeWalSegments();
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
            purgeWalSegments();
            assertWalExistence(true, tableName, 1);
            assertSegmentExistence(true, tableName, 1, 0);
            assertSegmentExistence(true, tableName, 1, 1);

            // After draining, it's all deleted.
            drainWalQueue();
            assertSql(tableName, "x\tts\ts1\n" +
                    "1\t2022-02-24T00:00:00.000000Z\t\n" +
                    "2\t2022-02-24T00:00:01.000000Z\tx\n");
            purgeWalSegments();
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
                ff.mkdir(path, configuration.getMkDirMode());
                Assert.assertEquals(path.toString(), true, ff.exists(path));
                path.of(root).concat(tableName).concat("waldo").$();
                ff.mkdir(path, configuration.getMkDirMode());
                Assert.assertEquals(path.toString(), true, ff.exists(path));

                // Purging will not delete waldo: Wal name not matched.
                purgeWalSegments();
                Assert.assertEquals(path.toString(), true, ff.exists(path));

                path.of(root).concat(tableName).concat("wal1000").$();
                ff.mkdir(path, configuration.getMkDirMode());
                Assert.assertEquals(path.toString(), true, ff.exists(path));

                // Purging will delete wal1000: Wal name matched and the WAL has no lock.
                purgeWalSegments();
                Assert.assertEquals(path.toString(), false, ff.exists(path));
            }
        });
    }
}
