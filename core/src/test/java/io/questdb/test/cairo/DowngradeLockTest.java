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

package io.questdb.test.cairo;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnVersionReader;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.RecoveryCoordinator;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Every commit mode but NOSYNC stores a checksum trailer after each {@code _cv} area. An older release does
 * not know the trailer: it rewrites an area in place and leaves the trailer's magic next to data it never
 * checksummed, which this release would then read as a torn area. So a table written under such a mode is
 * downgrade-locked in {@code _meta} ({@link ColumnType#DOWNGRADE_LOCKED_STORAGE_VERSION}), which an older
 * release refuses to open, and a NOSYNC start removes the trailers and the lock
 * ({@code RecoveryCoordinator.unlockTablesForDowngrade}): the documented way back to an older release.
 */
public class DowngradeLockTest extends AbstractCairoTest {

    @Test
    public void testCheckpointImageFollowsInstanceMode() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        try {
            assertMemoryLeak(() -> {
                execute("create table t (ts timestamp, v long) timestamp(ts) partition by day wal");
                execute("insert into t values ('2024-09-01T00:00:00.000000Z', 1)");
                drainWalQueue();
                final TableToken token = engine.verifyTableName("t");
                try {
                    execute("CHECKPOINT CREATE");
                    try (Path path = new Path()) {
                        path.of(configuration.getCheckpointRoot()).concat(configuration.getDbDirectory()).concat(token);
                        Assert.assertEquals(ColumnType.DOWNGRADE_LOCKED_STORAGE_VERSION, metaStorageVersion(path));
                        Assert.assertTrue("a checksummed checkpoint image must carry _cv trailers", cvLiveAreaHasTrailer(path));
                    }
                } finally {
                    execute("CHECKPOINT RELEASE");
                }
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        }
    }

    @Test
    public void testNosyncNeverLocks() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into t values ('2024-09-01T00:00:00.000000Z', 1)");
            execute("alter table t add column s symbol");
            execute("insert into t values ('2024-09-02T00:00:00.000000Z', 2, 'a')");
            drainWalQueue();
            engine.releaseAllWriters();
            final TableToken token = engine.verifyTableName("t");
            try (Path path = new Path()) {
                tablePath(path, token);
                Assert.assertEquals(ColumnType.VERSION, metaStorageVersion(path));
                Assert.assertFalse("a NOSYNC commit must not store a _cv trailer", cvLiveAreaHasTrailer(path));
                assertOlderReleaseOpens(path);
            }
        });
    }

    @Test
    public void testNosyncStartUnlocksNonWalTable() throws Exception {
        assertLockedThenUnlocked("create table t (ts timestamp, v long) timestamp(ts) partition by day bypass wal");
    }

    @Test
    public void testNosyncStartUnlocksWalTable() throws Exception {
        assertLockedThenUnlocked("create table t (ts timestamp, v long) timestamp(ts) partition by day wal");
    }

    @Test
    public void testStorageVersionMarkers() {
        Assert.assertEquals(ColumnType.VERSION, ColumnType.storageVersion(false, false));
        Assert.assertEquals(ColumnType.COMPOSITE_STORAGE_VERSION, ColumnType.storageVersion(true, false));
        Assert.assertEquals(ColumnType.DOWNGRADE_LOCKED_STORAGE_VERSION, ColumnType.storageVersion(false, true));
        Assert.assertEquals(ColumnType.DOWNGRADE_LOCKED_COMPOSITE_STORAGE_VERSION, ColumnType.storageVersion(true, true));
        Assert.assertEquals(ColumnType.MAX_STORAGE_VERSION, ColumnType.DOWNGRADE_LOCKED_COMPOSITE_STORAGE_VERSION);
        for (int v = ColumnType.VERSION; v <= ColumnType.MAX_STORAGE_VERSION; v++) {
            final boolean composite = ColumnType.isCompositeStorageVersion(v);
            final boolean locked = ColumnType.isDowngradeLockedStorageVersion(v);
            if (v == ColumnType.VERSION || v > ColumnType.MIGRATION_VERSION - 1) {
                Assert.assertEquals(v, ColumnType.storageVersion(composite, locked));
            } else {
                // Migration-step numbers in between carry neither marker.
                Assert.assertFalse(composite);
                Assert.assertFalse(locked);
            }
        }
        Assert.assertTrue(CommitMode.writesChecksums(CommitMode.SYNC));
        Assert.assertTrue(CommitMode.writesChecksums(CommitMode.ASYNC));
        Assert.assertTrue(CommitMode.writesChecksums(CommitMode.ADAPTIVE));
        Assert.assertFalse(CommitMode.writesChecksums(CommitMode.NOSYNC));
    }

    @Test
    public void testSyncLocksToo() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "sync");
        try {
            assertMemoryLeak(() -> {
                execute("create table t (ts timestamp, v long) timestamp(ts) partition by day wal");
                execute("insert into t values ('2024-09-01T00:00:00.000000Z', 1)");
                execute("alter table t add column s symbol");
                execute("insert into t values ('2024-09-02T00:00:00.000000Z', 2, 'a')");
                drainWalQueue();
                engine.releaseAllWriters();
                try (Path path = new Path()) {
                    tablePath(path, engine.verifyTableName("t"));
                    Assert.assertEquals(ColumnType.DOWNGRADE_LOCKED_STORAGE_VERSION, metaStorageVersion(path));
                    Assert.assertTrue("a checksummed commit must store a _cv trailer", cvLiveAreaHasTrailer(path));
                    assertOlderReleaseRefuses(path);
                }
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        }
    }

    /**
     * The exact-match check a release before 10.1 runs on open, and the range check 10.1 runs: both must accept
     * the table.
     */
    private static void assertOlderReleaseOpens(Path tablePath) {
        final int len = tablePath.size();
        try (MemoryMR mem = Vm.getCMRInstance()) {
            tablePath.concat(TableUtils.META_FILE_NAME);
            mem.smallFile(configuration.getFilesFacade(), tablePath.$(), MemoryTag.MMAP_DEFAULT);
            TableUtils.validateMetaVersion(tablePath.$(), mem, TableUtils.META_OFFSET_VERSION, ColumnType.VERSION);
            TableUtils.validateMetaVersion(tablePath.$(), mem, TableUtils.META_OFFSET_VERSION, ColumnType.VERSION, ColumnType.COMPOSITE_STORAGE_VERSION);
        } finally {
            tablePath.trimTo(len);
        }
    }

    /**
     * The exact-match check a release before 10.1 runs on open, and the range check 10.1 runs, which accepts
     * at most {@link ColumnType#COMPOSITE_STORAGE_VERSION}: both must refuse the table.
     */
    private static void assertOlderReleaseRefuses(Path tablePath) {
        final int len = tablePath.size();
        try (MemoryMR mem = Vm.getCMRInstance()) {
            tablePath.concat(TableUtils.META_FILE_NAME);
            mem.smallFile(configuration.getFilesFacade(), tablePath.$(), MemoryTag.MMAP_DEFAULT);
            try {
                TableUtils.validateMetaVersion(tablePath.$(), mem, TableUtils.META_OFFSET_VERSION, ColumnType.VERSION);
                Assert.fail("a release that only knows ColumnType.VERSION opens a downgrade-locked table");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "metadata version does not match runtime version");
            }
            try {
                TableUtils.validateMetaVersion(tablePath.$(), mem, TableUtils.META_OFFSET_VERSION, ColumnType.VERSION, ColumnType.COMPOSITE_STORAGE_VERSION);
                Assert.fail("a release that knows composite partitions but no _cv trailer opens a downgrade-locked table");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "metadata version does not match runtime version");
            }
        } finally {
            tablePath.trimTo(len);
        }
    }

    private static boolean cvAreaHasTrailer(FilesFacade ff, long fd, Path path, long tempMem8b, int offsetField, int sizeField) {
        final long offset = TableUtils.readLongOrFail(ff, fd, offsetField, tempMem8b, path.$());
        final long size = TableUtils.readLongOrFail(ff, fd, sizeField, tempMem8b, path.$());
        if (offset < ColumnVersionReader.HEADER_SIZE || offset + size + TableUtils.CV_CHECKSUM_TRAILER_SIZE > ff.length(fd)) {
            return false;
        }
        return TableUtils.readLongOrFail(ff, fd, offset + size, tempMem8b, path.$()) == TableUtils.CV_CHECKSUM_MAGIC;
    }

    private static boolean cvAnyAreaHasTrailer(Path tablePath) {
        return cvHasTrailer(tablePath, true);
    }

    private static boolean cvHasTrailer(Path tablePath, boolean anyArea) {
        final FilesFacade ff = configuration.getFilesFacade();
        final int len = tablePath.size();
        final long tempMem8b = Unsafe.malloc(Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        try {
            tablePath.concat(TableUtils.COLUMN_VERSION_FILE_NAME);
            final long fd = TableUtils.openRO(ff, tablePath.$(), LOG);
            try {
                final long version = TableUtils.readLongOrFail(ff, fd, ColumnVersionReader.OFFSET_VERSION_64, tempMem8b, tablePath.$());
                final boolean liveIsA = (version & 1L) == 0L;
                final boolean a = cvAreaHasTrailer(ff, fd, tablePath, tempMem8b, ColumnVersionReader.OFFSET_OFFSET_A_64, ColumnVersionReader.OFFSET_SIZE_A_64);
                final boolean b = cvAreaHasTrailer(ff, fd, tablePath, tempMem8b, ColumnVersionReader.OFFSET_OFFSET_B_64, ColumnVersionReader.OFFSET_SIZE_B_64);
                return anyArea ? (a || b) : (liveIsA ? a : b);
            } finally {
                ff.close(fd);
            }
        } finally {
            Unsafe.free(tempMem8b, Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            tablePath.trimTo(len);
        }
    }

    private static boolean cvLiveAreaHasTrailer(Path tablePath) {
        return cvHasTrailer(tablePath, false);
    }

    private static int metaStorageVersion(Path tablePath) {
        final int len = tablePath.size();
        try (MemoryMR mem = Vm.getCMRInstance()) {
            tablePath.concat(TableUtils.META_FILE_NAME);
            mem.smallFile(configuration.getFilesFacade(), tablePath.$(), MemoryTag.MMAP_DEFAULT);
            return mem.getInt(TableUtils.META_OFFSET_VERSION);
        } finally {
            tablePath.trimTo(len);
        }
    }

    private static Path tablePath(Path path, TableToken token) {
        return path.of(configuration.getDbRoot()).concat(token);
    }

    private void assertLockedThenUnlocked(String createTable) throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        try {
            assertMemoryLeak(() -> {
                execute(createTable);
                execute("insert into t values ('2024-09-01T00:00:00.000000Z', 1)");
                // A column change puts a record into _cv, so the area has a size and a trailer to remove.
                execute("alter table t add column s symbol");
                execute("insert into t values ('2024-09-02T00:00:00.000000Z', 2, 'a')");
                drainWalQueue();
                engine.releaseAllWriters();
                engine.releaseAllReaders();
                final TableToken token = engine.verifyTableName("t");

                try (Path path = new Path()) {
                    tablePath(path, token);
                    Assert.assertEquals(ColumnType.DOWNGRADE_LOCKED_STORAGE_VERSION, metaStorageVersion(path));
                    Assert.assertTrue("a checksummed commit must store a _cv trailer", cvLiveAreaHasTrailer(path));
                    assertOlderReleaseRefuses(path);

                    // An adaptive start keeps the lock.
                    engine.getTableSequencerAPI().resetForReboot(token);
                    new RecoveryCoordinator(engine).recover();
                    Assert.assertEquals(ColumnType.DOWNGRADE_LOCKED_STORAGE_VERSION, metaStorageVersion(path));

                    // The way down: a NOSYNC start removes the trailers of both areas and the lock.
                    setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
                    engine.getTableSequencerAPI().resetForReboot(token);
                    new RecoveryCoordinator(engine).recover();
                    Assert.assertEquals(ColumnType.VERSION, metaStorageVersion(path));
                    Assert.assertFalse("the NOSYNC start must remove every _cv trailer", cvAnyAreaHasTrailer(path));
                    assertOlderReleaseOpens(path);

                    // The table is intact, and NOSYNC writes keep it that way.
                    assertQuery("t").timestamp("ts").expectSize().returns("ts\tv\ts\n2024-09-01T00:00:00.000000Z\t1\t\n2024-09-02T00:00:00.000000Z\t2\ta\n");
                    execute("alter table t add column u int");
                    execute("insert into t values ('2024-09-03T00:00:00.000000Z', 3, 'b', 4)");
                    drainWalQueue();
                    engine.releaseAllWriters();
                    Assert.assertEquals(ColumnType.VERSION, metaStorageVersion(path));
                    Assert.assertFalse(cvAnyAreaHasTrailer(path));
                    assertQuery("select count() from t").noRandomAccess().expectSize().returns("count\n3\n");

                    // Running again is a no-op.
                    new RecoveryCoordinator(engine).recover();
                    Assert.assertEquals(ColumnType.VERSION, metaStorageVersion(path));
                }
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        }
    }
}
