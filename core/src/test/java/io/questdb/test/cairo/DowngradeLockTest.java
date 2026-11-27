/*******************************************************************************
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

package io.questdb.test.cairo;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnVersionReader;
import io.questdb.cairo.ColumnVersionWriter;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.ParquetMetaFileReader;
import io.questdb.cairo.RecoveryCoordinator;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Every commit mode stores a checksum trailer after each {@code _cv} area. An older release does not know the
 * trailer: it rewrites an area in place and leaves the trailer's magic next to data it never checksummed, which
 * this release would then read as a torn area. So every table this release writes is downgrade-locked in
 * {@code _meta} ({@link ColumnType#DOWNGRADE_LOCKED_STORAGE_VERSION}), which an older release refuses to open.
 * A start with {@code cairo.commit.mode.adaptive.downgrade=true} removes the trailers and the lock
 * ({@code RecoveryCoordinator.unlockTablesForDowngrade}) and writes neither while it runs: the documented way
 * back to an older release.
 */
public class DowngradeLockTest extends AbstractCairoTest {
    private static final String NON_WAL_TABLE = "create table t (ts timestamp, v long) timestamp(ts) partition by day bypass wal";
    // The _pm footer flags of the dedup sort section and of the index row map section.
    private static final long PM_DEDUP_SORT_BIT = 1L << 2;
    private static final long PM_INDEX_ROW_MAP_BIT = 1L << 32;
    private static final String WAL_TABLE = "create table t (ts timestamp, v long) timestamp(ts) partition by day wal";

    @Test
    public void testAdaptiveLocks() throws Exception {
        assertCommitModeLocks("adaptive");
    }

    @Test
    public void testAsyncLocks() throws Exception {
        assertCommitModeLocks("async");
    }

    @Test
    public void testCheckpointImageIsLocked() throws Exception {
        // The engine refuses CHECKPOINT CREATE on Windows, which has no sync(2).
        Assume.assumeFalse(Os.isWindows());
        // NOSYNC, the shipped default: the mode that stored no trailer before.
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        assertMemoryLeak(() -> {
            execute(WAL_TABLE);
            execute("insert into t values ('2024-09-01T00:00:00.000000Z', 1)");
            execute("alter table t add column s symbol");
            drainWalQueue();
            final TableToken token = engine.verifyTableName("t");
            try {
                execute("CHECKPOINT CREATE");
                try (Path path = new Path()) {
                    path.of(configuration.getCheckpointRoot()).concat(configuration.getDbDirectory()).concat(token);
                    Assert.assertEquals(ColumnType.DOWNGRADE_LOCKED_STORAGE_VERSION, metaStorageVersion(path));
                    Assert.assertTrue("a checkpoint image must carry a _cv trailer", cvLiveAreaHasTrailer(path));
                    assertOlderReleaseRefuses(path);
                }
            } finally {
                execute("CHECKPOINT RELEASE");
            }
        });
    }

    @Test
    public void testCheckpointImageOfDowngradeStartIsUnlocked() throws Exception {
        // The engine refuses CHECKPOINT CREATE on Windows, which has no sync(2).
        Assume.assumeFalse(Os.isWindows());
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        setProperty(PropertyKey.CAIRO_COMMIT_MODE_ADAPTIVE_DOWNGRADE, "true");
        assertMemoryLeak(() -> {
            execute(WAL_TABLE);
            execute("insert into t values ('2024-09-01T00:00:00.000000Z', 1)");
            execute("alter table t add column s symbol");
            drainWalQueue();
            final TableToken token = engine.verifyTableName("t");
            try {
                execute("CHECKPOINT CREATE");
                try (Path path = new Path()) {
                    path.of(configuration.getCheckpointRoot()).concat(configuration.getDbDirectory()).concat(token);
                    Assert.assertEquals(ColumnType.VERSION, metaStorageVersion(path));
                    Assert.assertFalse("a checkpoint image of a downgrade start must carry no _cv trailer", cvAnyAreaHasTrailer(path));
                    assertOlderReleaseOpens(path);
                }
            } finally {
                execute("CHECKPOINT RELEASE");
            }
        });
    }

    @Test
    public void testDowngradeStartClearsDedupSortedBits() throws Exception {
        // An older release does not know the dedup sorted bit of a native partition in _txn: it keeps the bit when
        // it writes the partition, so after a downgrade and an upgrade the bit could claim an order the rows no
        // longer have. A downgrade start leaves no partition with the bit, and its own writes set none.
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, k long, v long) timestamp(ts) partition by day wal dedup upsert keys(ts, k)");
            execute("insert into t values ('2024-09-01T00:00:00.000000Z', 2, 1), ('2024-09-01T00:00:00.000000Z', 1, 2), ('2024-09-02T00:00:00.000000Z', 1, 3)");
            drainWalQueue();
            engine.releaseAllWriters();
            engine.releaseAllReaders();
            final TableToken token = engine.verifyTableName("t");
            assertDedupSortedBits(token, "11");

            try (Path path = new Path()) {
                tablePath(path, token);
                downgradeStart(token);
                Assert.assertEquals(ColumnType.VERSION, metaStorageVersion(path));
                assertOlderReleaseOpens(path);
                assertDedupSortedBits(token, "00");

                // A new partition and an append of a dedup commit set the bit in a normal start.
                execute("insert into t values ('2024-09-02T00:00:01.000000Z', 1, 4), ('2024-09-03T00:00:00.000000Z', 1, 5)");
                drainWalQueue();
                engine.releaseAllWriters();
                engine.releaseAllReaders();
                Assert.assertEquals(ColumnType.VERSION, metaStorageVersion(path));
                assertDedupSortedBits(token, "000");

                // The way back up: the writes set the bit again.
                setProperty(PropertyKey.CAIRO_COMMIT_MODE_ADAPTIVE_DOWNGRADE, "false");
                engine.getTableSequencerAPI().resetForReboot(token);
                new RecoveryCoordinator(engine).recover();
                execute("insert into t values ('2024-09-04T00:00:00.000000Z', 1, 6)");
                drainWalQueue();
                engine.releaseAllWriters();
                engine.releaseAllReaders();
                Assert.assertEquals(ColumnType.DOWNGRADE_LOCKED_STORAGE_VERSION, metaStorageVersion(path));
                assertDedupSortedBits(token, "0001");
                assertQuery("t").timestamp("ts").expectSize().returns("""
                        ts\tk\tv
                        2024-09-01T00:00:00.000000Z\t1\t2
                        2024-09-01T00:00:00.000000Z\t2\t1
                        2024-09-02T00:00:00.000000Z\t1\t3
                        2024-09-02T00:00:01.000000Z\t1\t4
                        2024-09-03T00:00:00.000000Z\t1\t5
                        2024-09-04T00:00:00.000000Z\t1\t6
                        """);
            }
        });
    }

    @Test
    public void testDowngradeStartDropsParquetDedupSortSection() throws Exception {
        // The dedup sort section of a _pm footer (bit 2) sits before the index row map (bit 32). An older release
        // that knows the map, not the section, would read the map from the start of the section and write that wrong
        // map with its next O3 commit. A downgrade start leaves each footer with the map only, and its own writes
        // add no section.
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        assertMemoryLeak(() -> {
            final TableToken token = createParquetPartitionWithBothSections();
            final long nameTxn = parquetPartitionNameTxn(token);
            final String indexRowMap = pmIndexRowMap(token);

            try (Path path = new Path()) {
                tablePath(path, token);
                downgradeStart(token);
                Assert.assertEquals(ColumnType.VERSION, metaStorageVersion(path));
                assertOlderReleaseOpens(path);
                Assert.assertEquals(PM_INDEX_ROW_MAP_BIT, pmSectionFlags(token));
                Assert.assertEquals(indexRowMap, pmIndexRowMap(token));

                // Running again changes nothing.
                engine.getTableSequencerAPI().resetForReboot(token);
                new RecoveryCoordinator(engine).recover();
                Assert.assertEquals(PM_INDEX_ROW_MAP_BIT, pmSectionFlags(token));
                Assert.assertEquals(indexRowMap, pmIndexRowMap(token));

                // The O3 commits of the downgrade start add no section.
                execute("insert into t values ('a', 600, '2024-01-01T09:00:00.000000Z'), ('d', 601, '2024-01-01T10:30:00.000000Z')");
                drainWalQueue();
                engine.releaseAllWriters();
                engine.releaseAllReaders();
                Assert.assertEquals(nameTxn, parquetPartitionNameTxn(token));
                Assert.assertEquals(ColumnType.VERSION, metaStorageVersion(path));
                Assert.assertEquals(PM_INDEX_ROW_MAP_BIT, pmSectionFlags(token));

                // The way back up: the O3 commits add the section again.
                setProperty(PropertyKey.CAIRO_COMMIT_MODE_ADAPTIVE_DOWNGRADE, "false");
                engine.getTableSequencerAPI().resetForReboot(token);
                new RecoveryCoordinator(engine).recover();
                execute("insert into t values ('b', 700, '2024-01-01T11:00:00.000000Z'), ('a', 701, '2024-01-01T11:00:00.000000Z')");
                drainWalQueue();
                engine.releaseAllWriters();
                engine.releaseAllReaders();
                Assert.assertEquals(nameTxn, parquetPartitionNameTxn(token));
                Assert.assertEquals(ColumnType.DOWNGRADE_LOCKED_STORAGE_VERSION, metaStorageVersion(path));
                Assert.assertEquals(PM_DEDUP_SORT_BIT | PM_INDEX_ROW_MAP_BIT, pmSectionFlags(token));
                assertQuery("select * from t where s = 'a'").timestamp("ts").returns("""
                        s\tv\tts
                        a\t1\t2024-01-01T01:00:00.000000Z
                        a\t4\t2024-01-01T04:00:00.000000Z
                        a\t501\t2024-01-01T05:00:00.000000Z
                        a\t7\t2024-01-01T07:00:00.000000Z
                        a\t600\t2024-01-01T09:00:00.000000Z
                        a\t10\t2024-01-01T10:00:00.000000Z
                        a\t701\t2024-01-01T11:00:00.000000Z
                        a\t99\t2024-01-02T00:00:00.000000Z
                        """);
                assertQuery("select * from t order by ts, s").timestamp("ts").expectSize().returns("""
                        s\tv\tts
                        a\t1\t2024-01-01T01:00:00.000000Z
                        b\t2\t2024-01-01T02:00:00.000000Z
                        c\t3\t2024-01-01T03:00:00.000000Z
                        a\t4\t2024-01-01T04:00:00.000000Z
                        a\t501\t2024-01-01T05:00:00.000000Z
                        b\t500\t2024-01-01T05:00:00.000000Z
                        \t6\t2024-01-01T06:00:00.000000Z
                        a\t7\t2024-01-01T07:00:00.000000Z
                        c\t502\t2024-01-01T07:30:00.000000Z
                        b\t8\t2024-01-01T08:00:00.000000Z
                        a\t600\t2024-01-01T09:00:00.000000Z
                        c\t9\t2024-01-01T09:00:00.000000Z
                        a\t10\t2024-01-01T10:00:00.000000Z
                        d\t601\t2024-01-01T10:30:00.000000Z
                        a\t701\t2024-01-01T11:00:00.000000Z
                        b\t700\t2024-01-01T11:00:00.000000Z
                        c\t12\t2024-01-01T12:00:00.000000Z
                        a\t99\t2024-01-02T00:00:00.000000Z
                        """);
            }
        });
    }

    @Test
    public void testDowngradeStartFailureLeavesTableLocked() throws Exception {
        final AtomicBoolean isCvReadFailing = new AtomicBoolean();
        final AtomicBoolean isMetaWriteFailing = new AtomicBoolean();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            private long cvFd = -1;
            private long metaFd = -1;

            @Override
            public boolean close(long fd) {
                if (fd == cvFd) {
                    cvFd = -1;
                }
                if (fd == metaFd) {
                    metaFd = -1;
                }
                return super.close(fd);
            }

            @Override
            public long openRW(LPSZ name, int opts) {
                final long fd = super.openRW(name, opts);
                if (isCvReadFailing.get() && Utf8s.endsWithAscii(name, TableUtils.COLUMN_VERSION_FILE_NAME)) {
                    cvFd = fd;
                }
                if (isMetaWriteFailing.get() && Utf8s.endsWithAscii(name, TableUtils.META_FILE_NAME)) {
                    metaFd = fd;
                }
                return fd;
            }

            @Override
            public long read(long fd, long buf, long len, long offset) {
                if (fd == cvFd) {
                    return -1;
                }
                return super.read(fd, buf, len, offset);
            }

            @Override
            public long write(long fd, long address, long len, long offset) {
                if (fd == metaFd) {
                    return -1;
                }
                return super.write(fd, address, len, offset);
            }
        };
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        assertMemoryLeak(ff, () -> {
            execute(NON_WAL_TABLE);
            execute("insert into t values ('2024-09-01T00:00:00.000000Z', 1)");
            execute("update t set v = 10");
            execute("update t set v = 11");
            execute("update t set v = 12");
            engine.releaseAllWriters();
            engine.releaseAllReaders();
            final TableToken token = engine.verifyTableName("t");
            try (Path path = new Path()) {
                tablePath(path, token);
                Assert.assertEquals(3, cvMagicCount(path));

                // _cv cannot be read: the start fails and changes nothing.
                isCvReadFailing.set(true);
                try {
                    downgradeStart(token);
                    Assert.fail("the downgrade start must fail when it cannot read _cv");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "could not read _cv");
                }
                isCvReadFailing.set(false);
                Assert.assertEquals(ColumnType.DOWNGRADE_LOCKED_STORAGE_VERSION, metaStorageVersion(path));
                Assert.assertEquals(3, cvMagicCount(path));
                assertOlderReleaseRefuses(path);

                // _meta cannot be written: the trailers are gone, the lock stays, and this release still reads
                // the table.
                isMetaWriteFailing.set(true);
                try {
                    downgradeStart(token);
                    Assert.fail("the downgrade start must fail when it cannot write _meta");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "could not write");
                }
                isMetaWriteFailing.set(false);
                Assert.assertEquals(ColumnType.DOWNGRADE_LOCKED_STORAGE_VERSION, metaStorageVersion(path));
                Assert.assertEquals(0, cvMagicCount(path));
                assertOlderReleaseRefuses(path);
                assertQuery("t").timestamp("ts").expectSize().returns("ts\tv\n2024-09-01T00:00:00.000000Z\t12\n");
                engine.releaseAllReaders();

                // The next downgrade start finishes the job.
                downgradeStart(token);
                Assert.assertEquals(ColumnType.VERSION, metaStorageVersion(path));
                Assert.assertEquals(0, cvMagicCount(path));
                assertOlderReleaseOpens(path);
                assertQuery("t").timestamp("ts").expectSize().returns("ts\tv\n2024-09-01T00:00:00.000000Z\t12\n");
            }
        });
    }

    @Test
    public void testDowngradeStartFailureOnParquetMetadataLeavesTableLocked() throws Exception {
        final AtomicBoolean isParquetMetaOpenFailing = new AtomicBoolean();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRWExisting(LPSZ name) {
                if (isParquetMetaOpenFailing.get() && Utf8s.endsWithAscii(name, TableUtils.PARQUET_METADATA_FILE_NAME)) {
                    return -1;
                }
                return super.openRWExisting(name);
            }
        };
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        assertMemoryLeak(ff, () -> {
            final TableToken token = createParquetPartitionWithBothSections();
            final String indexRowMap = pmIndexRowMap(token);
            try (Path path = new Path()) {
                tablePath(path, token);

                // _pm cannot be opened: the start fails, and the table keeps the lock and its section.
                isParquetMetaOpenFailing.set(true);
                try {
                    downgradeStart(token);
                    Assert.fail("the downgrade start must fail when it cannot open _pm");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "could not open _pm");
                }
                isParquetMetaOpenFailing.set(false);
                Assert.assertEquals(ColumnType.DOWNGRADE_LOCKED_STORAGE_VERSION, metaStorageVersion(path));
                assertOlderReleaseRefuses(path);
                Assert.assertEquals(PM_DEDUP_SORT_BIT | PM_INDEX_ROW_MAP_BIT, pmSectionFlags(token));

                // The next downgrade start finishes the job.
                downgradeStart(token);
                Assert.assertEquals(ColumnType.VERSION, metaStorageVersion(path));
                assertOlderReleaseOpens(path);
                Assert.assertEquals(PM_INDEX_ROW_MAP_BIT, pmSectionFlags(token));
                Assert.assertEquals(indexRowMap, pmIndexRowMap(token));
            }
        });
    }

    @Test
    public void testDowngradeStartKeepsColumnVersionDataThatReadsAsTrailer() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        assertMemoryLeak(() -> {
            execute(NON_WAL_TABLE);
            execute("insert into t values ('2024-09-01T00:00:00.000000Z', 1)");
            engine.releaseAllWriters();
            engine.releaseAllReaders();
            final TableToken token = engine.verifyTableName("t");
            try (Path path = new Path()) {
                tablePath(path, token);
                // A column top that reads as the trailer magic. No table produces the value, but nothing in the
                // format forbids the word in the data, and the downgrade start must not take it for a trailer.
                // Two commits put it into both areas the header names.
                final int len = path.size();
                try (ColumnVersionWriter w = new ColumnVersionWriter(configuration, path.concat(TableUtils.COLUMN_VERSION_FILE_NAME).$(), true)) {
                    final long partitionTimestamp = MicrosTimestampDriver.floor("2024-09-01T00:00:00.000000Z");
                    w.upsert(partitionTimestamp, 1, 5, TableUtils.CV_CHECKSUM_MAGIC);
                    w.commit();
                    w.upsert(partitionTimestamp, 0, 6, 0);
                    w.commit();
                } finally {
                    path.trimTo(len);
                }
                Assert.assertEquals(ColumnType.DOWNGRADE_LOCKED_STORAGE_VERSION, metaStorageVersion(path));
                Assert.assertEquals("two words of data and two trailers", 4, cvMagicCount(path));
                Assert.assertEquals(2, cvMagicCountInNamedData(path));

                downgradeStart(token);
                Assert.assertEquals(ColumnType.VERSION, metaStorageVersion(path));
                Assert.assertFalse(cvAnyAreaHasTrailer(path));
                Assert.assertEquals("the data of both areas must stay", 2, cvMagicCount(path));
                Assert.assertEquals(2, cvMagicCountInNamedData(path));
            }
        });
    }

    @Test
    public void testDowngradeStartKeepsCompositeMarker() throws Exception {
        assertMemoryLeak(() -> {
            // A composite day that is not the last partition, as CompositeStorageVersionTest builds it.
            execute("CREATE TABLE t AS (" +
                    "SELECT cast(x AS int) i, rnd_str(5, 16, 2) s," +
                    " timestamp_sequence('2024-01-01', 1_000_000L) ts" +
                    " FROM long_sequence(20_000)) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();
            execute("INSERT INTO t SELECT cast(x AS int) + 100_000 i, rnd_str(5, 16, 2) s," +
                    " timestamp_sequence('2024-01-03', 1_000_000L) ts FROM long_sequence(1_000)");
            drainWalQueue();
            for (int i = 0; i < 2; i++) {
                execute("INSERT INTO t SELECT cast(x AS int) + 300_000 i, rnd_str(5, 16, 2) s," +
                        " timestamp_sequence('2024-01-01T01:00:00', 1_000_000L) ts FROM long_sequence(200)");
                drainWalQueue();
            }
            final TableToken token = engine.verifyTableName("t");
            try (TableReader reader = engine.getReader(token)) {
                Assert.assertTrue("fixture produced no composite partition", reader.getTxFile().hasCompositePartitions());
            }
            engine.releaseAllWriters();
            engine.releaseAllReaders();

            try (Path path = new Path()) {
                tablePath(path, token);
                Assert.assertEquals(ColumnType.DOWNGRADE_LOCKED_COMPOSITE_STORAGE_VERSION, metaStorageVersion(path));

                downgradeStart(token);
                // The lock goes, the composite marker stays: 10.1 reads composite partitions, older releases
                // do not.
                Assert.assertEquals(ColumnType.COMPOSITE_STORAGE_VERSION, metaStorageVersion(path));
                Assert.assertFalse(cvAnyAreaHasTrailer(path));
                final int len = path.size();
                try (MemoryMR mem = Vm.getCMRInstance()) {
                    path.concat(TableUtils.META_FILE_NAME);
                    mem.smallFile(configuration.getFilesFacade(), path.$(), MemoryTag.MMAP_DEFAULT);
                    TableUtils.validateMetaVersion(path.$(), mem, TableUtils.META_OFFSET_VERSION, ColumnType.VERSION, ColumnType.COMPOSITE_STORAGE_VERSION);
                } finally {
                    path.trimTo(len);
                }

                // A writer of the downgrade start leaves it that way.
                execute("INSERT INTO t SELECT cast(x AS int) + 500_000 i, rnd_str(5, 16, 2) s," +
                        " timestamp_sequence('2024-01-04', 1_000_000L) ts FROM long_sequence(10)");
                drainWalQueue();
                engine.releaseAllWriters();
                Assert.assertEquals(ColumnType.COMPOSITE_STORAGE_VERSION, metaStorageVersion(path));
                assertQuery("select count() from t").noRandomAccess().expectSize().returns("count\n21410\n");
            }
        });
    }

    @Test
    public void testDowngradeStartKeepsLockOverTornTxn() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, k long, v long) timestamp(ts) partition by day wal dedup upsert keys(ts, k)");
            execute("insert into t values ('2024-09-01T00:00:00.000000Z', 1, 1)");
            drainWalQueue();
            execute("insert into t values ('2024-09-02T00:00:00.000000Z', 1, 2)");
            drainWalQueue();
            engine.releaseAllWriters();
            engine.releaseAllReaders();
            final TableToken token = engine.verifyTableName("t");
            assertDedupSortedBits(token, "11");

            try (Path path = new Path()) {
                // Tear the live _txn area: its body checksum no longer matches, the previous area still does.
                tablePath(path, token).concat(TableUtils.TXN_FILE_NAME);
                final FilesFacade ff = configuration.getFilesFacade();
                final long tempMem8b = Unsafe.malloc(Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                final long fd = TableUtils.openRW(ff, path.$(), LOG, configuration.getWriterFileOpenOpts());
                try {
                    final long version = TableUtils.readLongOrFail(ff, fd, TableUtils.TX_BASE_OFFSET_VERSION_64, tempMem8b, path.$());
                    final long areaOffset = TableUtils.readIntOrFail(ff, fd, (version & 1) == 0 ? TableUtils.TX_BASE_OFFSET_A_32 : TableUtils.TX_BASE_OFFSET_B_32, tempMem8b, path);
                    final long maxTimestampOffset = areaOffset + TableUtils.TX_OFFSET_MAX_TIMESTAMP_64;
                    final long maxTimestamp = TableUtils.readLongOrFail(ff, fd, maxTimestampOffset, tempMem8b, path.$());
                    TableUtils.writeLongOrFail(ff, fd, maxTimestampOffset, maxTimestamp + 1, tempMem8b, path);
                } finally {
                    ff.close(fd);
                    Unsafe.free(tempMem8b, Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                }

                // The start cannot clear the bits of a torn _txn, so it leaves the table locked.
                tablePath(path, token);
                downgradeStart(token);
                Assert.assertEquals(ColumnType.DOWNGRADE_LOCKED_STORAGE_VERSION, metaStorageVersion(path));
                assertOlderReleaseRefuses(path);

                // The writer repairs _txn on open, and the next downgrade start unlocks the table.
                engine.getWriter(token, "test").close();
                drainWalQueue();
                engine.releaseAllWriters();
                engine.releaseAllReaders();
                downgradeStart(token);
                Assert.assertEquals(ColumnType.VERSION, metaStorageVersion(path));
                assertOlderReleaseOpens(path);
                assertDedupSortedBits(token, "00");
                assertQuery("t").timestamp("ts").expectSize().returns("""
                        ts\tk\tv
                        2024-09-01T00:00:00.000000Z\t1\t1
                        2024-09-02T00:00:00.000000Z\t1\t2
                        """);
            }
        });
    }

    @Test
    public void testDowngradeStartRemovesLeftoverTrailers() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        assertMemoryLeak(() -> {
            execute(NON_WAL_TABLE);
            execute("insert into t values ('2024-09-01T00:00:00.000000Z', 1)");
            // Three _cv commits of one size: the third reuses the start of the file and leaves the trailer of
            // the first behind, outside both areas the header names. An older release that later ends an area
            // right before it would make this release read that area as torn.
            execute("update t set v = 10");
            execute("update t set v = 11");
            execute("update t set v = 12");
            engine.releaseAllWriters();
            engine.releaseAllReaders();
            final TableToken token = engine.verifyTableName("t");
            try (Path path = new Path()) {
                tablePath(path, token);
                Assert.assertEquals(ColumnType.DOWNGRADE_LOCKED_STORAGE_VERSION, metaStorageVersion(path));
                Assert.assertTrue(cvLiveAreaHasTrailer(path));
                Assert.assertEquals("the fixture must leave one trailer outside the two named areas", 3, cvMagicCount(path));

                downgradeStart(token);
                Assert.assertEquals(ColumnType.VERSION, metaStorageVersion(path));
                Assert.assertEquals("the downgrade start must remove every _cv trailer, named or not", 0, cvMagicCount(path));
                assertOlderReleaseOpens(path);
                assertQuery("t").timestamp("ts").expectSize().returns("ts\tv\n2024-09-01T00:00:00.000000Z\t12\n");
            }
        });
    }

    @Test
    public void testDowngradeStartSkipsMissingParquetMetadata() throws Exception {
        // A parquet partition without a local _pm has no footer to change: the start unlocks the table and creates
        // no _pm.
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        assertMemoryLeak(() -> {
            final TableToken token = createParquetPartitionWithBothSections();
            try (Path path = new Path(); Path pmPath = new Path()) {
                tablePath(path, token);
                parquetMetadataPath(pmPath, token);
                Assert.assertTrue(configuration.getFilesFacade().removeQuiet(pmPath.$()));

                downgradeStart(token);
                Assert.assertEquals(ColumnType.VERSION, metaStorageVersion(path));
                assertOlderReleaseOpens(path);
                Assert.assertFalse("the downgrade start must not create a missing _pm", configuration.getFilesFacade().exists(pmPath.$()));
            }
        });
    }

    @Test
    public void testDowngradeStartSkipsWhatItCannotUnlock() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        assertMemoryLeak(() -> {
            // Healthy, without _meta, with a _meta too short to hold a version and without _cv.
            for (String table : new String[]{"t", "no_meta", "short_meta", "no_cv"}) {
                execute(NON_WAL_TABLE.replace("table t", "table " + table));
                execute("insert into " + table + " values ('2024-09-01T00:00:00.000000Z', 1)");
                execute("alter table " + table + " add column s symbol");
            }
            // Never changed, so _cv holds no area: as the writer leaves it, and cut to the header, as created.
            for (String table : new String[]{"bare_cv", "fresh_cv"}) {
                execute(NON_WAL_TABLE.replace("table t", "table " + table));
                execute("insert into " + table + " values ('2024-09-01T00:00:00.000000Z', 1)");
            }
            // A view has no _meta at all.
            execute("create view vw as (select * from t)");
            engine.releaseAllWriters();
            engine.releaseAllReaders();

            final FilesFacade ff = configuration.getFilesFacade();
            try (Path path = new Path()) {
                tablePath(path, engine.verifyTableName("no_meta")).concat(TableUtils.META_FILE_NAME);
                Assert.assertTrue(ff.removeQuiet(path.$()));

                tablePath(path, engine.verifyTableName("short_meta")).concat(TableUtils.META_FILE_NAME);
                long fd = TableUtils.openRW(ff, path.$(), LOG, configuration.getWriterFileOpenOpts());
                try {
                    Assert.assertTrue(ff.truncate(fd, TableUtils.META_OFFSET_VERSION + 2));
                } finally {
                    ff.close(fd);
                }

                tablePath(path, engine.verifyTableName("no_cv")).concat(TableUtils.COLUMN_VERSION_FILE_NAME);
                Assert.assertTrue(ff.removeQuiet(path.$()));

                tablePath(path, engine.verifyTableName("fresh_cv")).concat(TableUtils.COLUMN_VERSION_FILE_NAME);
                fd = TableUtils.openRW(ff, path.$(), LOG, configuration.getWriterFileOpenOpts());
                try {
                    Assert.assertTrue(ff.length(fd) >= ColumnVersionReader.HEADER_SIZE);
                    Assert.assertTrue(ff.truncate(fd, ColumnVersionReader.HEADER_SIZE));
                } finally {
                    ff.close(fd);
                }

                for (String table : new String[]{"bare_cv", "fresh_cv"}) {
                    tablePath(path, engine.verifyTableName(table));
                    Assert.assertEquals(table, ColumnType.DOWNGRADE_LOCKED_STORAGE_VERSION, metaStorageVersion(path));
                    Assert.assertEquals(table, 0, cvMagicCount(path));
                }

                downgradeStart(engine.verifyTableName("t"));

                // The start leaves the damaged tables to their writers and unlocks the rest.
                tablePath(path, engine.verifyTableName("no_meta")).concat(TableUtils.META_FILE_NAME);
                Assert.assertFalse("the start must not create a _meta", ff.exists(path.$()));
                tablePath(path, engine.verifyTableName("short_meta")).concat(TableUtils.META_FILE_NAME);
                Assert.assertEquals(TableUtils.META_OFFSET_VERSION + 2, ff.length(path.$()));
                for (String table : new String[]{"t", "no_cv", "bare_cv", "fresh_cv"}) {
                    tablePath(path, engine.verifyTableName(table));
                    Assert.assertEquals(table, ColumnType.VERSION, metaStorageVersion(path));
                    assertOlderReleaseOpens(path);
                }
                tablePath(path, engine.verifyTableName("fresh_cv")).concat(TableUtils.COLUMN_VERSION_FILE_NAME);
                Assert.assertEquals(ColumnVersionReader.HEADER_SIZE, ff.length(path.$()));
                tablePath(path, engine.verifyTableName("no_cv")).concat(TableUtils.COLUMN_VERSION_FILE_NAME);
                Assert.assertFalse("the start must not create a _cv", ff.exists(path.$()));
                tablePath(path, engine.verifyTableName("t"));
                Assert.assertEquals(0, cvMagicCount(path));
                assertQuery("select count() from t").noRandomAccess().expectSize().returns("count\n1\n");
                assertQuery("select count() from bare_cv").noRandomAccess().expectSize().returns("count\n1\n");
                assertQuery("select count() from fresh_cv").noRandomAccess().expectSize().returns("count\n1\n");
            }
        });
    }

    @Test
    public void testDowngradeStartUnlocksNonWalTableAfterNosync() throws Exception {
        assertLockedThenUnlocked(NON_WAL_TABLE, "nosync");
    }

    @Test
    public void testDowngradeStartUnlocksNonWalTableAfterSync() throws Exception {
        assertLockedThenUnlocked(NON_WAL_TABLE, "sync");
    }

    @Test
    public void testDowngradeStartUnlocksWalTableAfterAdaptive() throws Exception {
        assertLockedThenUnlocked(WAL_TABLE, "adaptive");
    }

    @Test
    public void testDowngradeStartUnlocksWalTableAfterNosync() throws Exception {
        assertLockedThenUnlocked(WAL_TABLE, "nosync");
    }

    @Test
    public void testDowngradeWriterKeepsLockTheStartDidNotRemove() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        assertMemoryLeak(() -> {
            execute(WAL_TABLE);
            execute("insert into t values ('2024-09-01T00:00:00.000000Z', 1)");
            execute("alter table t add column s symbol");
            drainWalQueue();
            engine.releaseAllWriters();
            final TableToken token = engine.verifyTableName("t");
            try (Path path = new Path()) {
                tablePath(path, token);
                Assert.assertEquals(ColumnType.DOWNGRADE_LOCKED_STORAGE_VERSION, metaStorageVersion(path));

                // The setting without its start: the unlock pass did not run, so the _cv area the writer does
                // not rewrite still holds a trailer, and the writer must not unlock the table on its own.
                setProperty(PropertyKey.CAIRO_COMMIT_MODE_ADAPTIVE_DOWNGRADE, "true");
                execute("alter table t add column u int");
                execute("insert into t values ('2024-09-02T00:00:00.000000Z', 2, 'a', 3)");
                drainWalQueue();
                engine.releaseAllWriters();
                Assert.assertEquals(ColumnType.DOWNGRADE_LOCKED_STORAGE_VERSION, metaStorageVersion(path));
                Assert.assertFalse("a commit of a downgrade start must not store a _cv trailer", cvLiveAreaHasTrailer(path));
                Assert.assertTrue("the other _cv area keeps its trailer until a downgrade start", cvAnyAreaHasTrailer(path));
                assertOlderReleaseRefuses(path);
                assertQuery("select count() from t").noRandomAccess().expectSize().returns("count\n2\n");
            }
        });
    }

    @Test
    public void testNosyncLocks() throws Exception {
        assertCommitModeLocks("nosync");
    }

    @Test
    public void testStorageVersionMarkers() {
        Assert.assertEquals(ColumnType.VERSION, ColumnType.storageVersion(false, false, false));
        Assert.assertEquals(ColumnType.COMPOSITE_STORAGE_VERSION, ColumnType.storageVersion(true, false, false));
        Assert.assertEquals(ColumnType.DOWNGRADE_LOCKED_STORAGE_VERSION, ColumnType.storageVersion(false, false, true));
        Assert.assertEquals(ColumnType.DOWNGRADE_LOCKED_COMPOSITE_STORAGE_VERSION, ColumnType.storageVersion(true, false, true));
        Assert.assertEquals(ColumnType.DELTA_STORAGE_VERSION, ColumnType.storageVersion(false, true, false));
        Assert.assertEquals(ColumnType.DELTA_STORAGE_VERSION, ColumnType.storageVersion(true, true, false));
        Assert.assertEquals(ColumnType.DOWNGRADE_LOCKED_DELTA_STORAGE_VERSION, ColumnType.storageVersion(false, true, true));
        Assert.assertEquals(ColumnType.DOWNGRADE_LOCKED_DELTA_STORAGE_VERSION, ColumnType.storageVersion(true, true, true));
        Assert.assertEquals(ColumnType.MAX_STORAGE_VERSION, ColumnType.DOWNGRADE_LOCKED_DELTA_STORAGE_VERSION);
        for (int v = ColumnType.VERSION; v <= ColumnType.MAX_STORAGE_VERSION; v++) {
            final boolean composite = ColumnType.isCompositeStorageVersion(v);
            final boolean delta = ColumnType.isDeltaStorageVersion(v);
            final boolean locked = ColumnType.isDowngradeLockedStorageVersion(v);
            if (v == ColumnType.VERSION || v > ColumnType.MIGRATION_VERSION - 1) {
                Assert.assertEquals(v, ColumnType.storageVersion(composite, delta, locked));
            } else {
                // Migration-step numbers in between carry no marker.
                Assert.assertFalse(composite);
                Assert.assertFalse(delta);
                Assert.assertFalse(locked);
            }
        }
    }

    @Test
    public void testSyncLocks() throws Exception {
        assertCommitModeLocks("sync");
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

    private static void assertDedupSortedBits(TableToken token, String expected) {
        try (TableReader reader = engine.getReader(token)) {
            final StringBuilder actual = new StringBuilder();
            for (int i = 0, n = reader.getPartitionCount(); i < n; i++) {
                actual.append(reader.getTxFile().isPartitionDedupSorted(i) ? '1' : '0');
            }
            Assert.assertEquals(expected, actual.toString());
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

    /**
     * How many times the trailer magic occurs in {@code _cv} past the header, in the named areas or not. The
     * tables of this test hold no column version data that reads as the magic.
     */
    private static int cvMagicCount(Path tablePath) {
        final FilesFacade ff = configuration.getFilesFacade();
        final int len = tablePath.size();
        final long tempMem8b = Unsafe.malloc(Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        try {
            tablePath.concat(TableUtils.COLUMN_VERSION_FILE_NAME);
            final long fd = TableUtils.openRO(ff, tablePath.$(), LOG);
            try {
                int count = 0;
                for (long offset = ColumnVersionReader.HEADER_SIZE, n = ff.length(fd); offset + Long.BYTES <= n; offset += Long.BYTES) {
                    if (TableUtils.readLongOrFail(ff, fd, offset, tempMem8b, tablePath.$()) == TableUtils.CV_CHECKSUM_MAGIC) {
                        count++;
                    }
                }
                return count;
            } finally {
                ff.close(fd);
            }
        } finally {
            Unsafe.free(tempMem8b, Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            tablePath.trimTo(len);
        }
    }

    /**
     * How many words of the data of the two areas the {@code _cv} header names read as the trailer magic.
     */
    private static int cvMagicCountInNamedData(Path tablePath) {
        final FilesFacade ff = configuration.getFilesFacade();
        final int len = tablePath.size();
        final long tempMem8b = Unsafe.malloc(Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        try {
            tablePath.concat(TableUtils.COLUMN_VERSION_FILE_NAME);
            final long fd = TableUtils.openRO(ff, tablePath.$(), LOG);
            try {
                int count = 0;
                final int[] fields = {
                        ColumnVersionReader.OFFSET_OFFSET_A_64, ColumnVersionReader.OFFSET_SIZE_A_64,
                        ColumnVersionReader.OFFSET_OFFSET_B_64, ColumnVersionReader.OFFSET_SIZE_B_64
                };
                for (int i = 0; i < fields.length; i += 2) {
                    final long offset = TableUtils.readLongOrFail(ff, fd, fields[i], tempMem8b, tablePath.$());
                    final long size = TableUtils.readLongOrFail(ff, fd, fields[i + 1], tempMem8b, tablePath.$());
                    Assert.assertTrue("the header must name two written areas", offset >= ColumnVersionReader.HEADER_SIZE && size > 0);
                    for (long o = offset; o < offset + size; o += Long.BYTES) {
                        if (TableUtils.readLongOrFail(ff, fd, o, tempMem8b, tablePath.$()) == TableUtils.CV_CHECKSUM_MAGIC) {
                            count++;
                        }
                    }
                }
                return count;
            } finally {
                ff.close(fd);
            }
        } finally {
            Unsafe.free(tempMem8b, Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            tablePath.trimTo(len);
        }
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

    /**
     * The {@code _pm} of the first partition, as {@code _txn} names it.
     */
    private static Path parquetMetadataPath(Path path, TableToken token) {
        try (TableReader reader = engine.getReader(token)) {
            TableUtils.setPathForParquetPartitionMetadata(
                    tablePath(path, token),
                    reader.getMetadata().getTimestampType(),
                    reader.getPartitionedBy(),
                    reader.getTxFile().getPartitionTimestampByIndex(0),
                    reader.getTxFile().getPartitionNameTxn(0)
            );
            return path;
        }
    }

    private static long parquetPartitionNameTxn(TableToken token) {
        try (TableReader reader = engine.getReader(token)) {
            Assert.assertTrue(reader.getTxFile().isPartitionParquet(0));
            return reader.getTxFile().getPartitionNameTxn(0);
        }
    }

    /**
     * The index row count and the first index row of each row group, as the first partition's {@code _pm} holds them.
     */
    private static String pmIndexRowMap(TableToken token) {
        try (TableReader reader = engine.getReader(token)) {
            reader.openPartition(0);
            final ParquetMetaFileReader pm = reader.getAndInitParquetPartitionDecoder(0).metadata();
            Assert.assertTrue(pm.hasIndexRowMap());
            final StringBuilder sink = new StringBuilder().append(pm.getIndexRowCount()).append(':');
            long rowGroupStartRow = 0;
            for (int i = 0, n = pm.getRowGroupCount(); i < n; i++) {
                sink.append(' ').append(pm.getRowGroupIndexRowLo(i, rowGroupStartRow));
                rowGroupStartRow += pm.getRowGroupSize(i);
            }
            return sink.toString();
        }
    }

    /**
     * The dedup sort and index row map flags of the {@code _pm} footer that describes the first partition's parquet
     * file, found as a reader finds it: the footer chain is walked back from the committed size until a footer names
     * that parquet file size.
     */
    private static long pmSectionFlags(TableToken token) throws IOException {
        final long parquetFileSize;
        try (TableReader reader = engine.getReader(token); Path path = new Path()) {
            parquetFileSize = reader.getTxFile().getPartitionParquetFileSize(0);
            parquetMetadataPath(path, token);
            final ByteBuffer pm = ByteBuffer.wrap(Files.readAllBytes(Paths.get(path.toString()))).order(ByteOrder.LITTLE_ENDIAN);
            int size = (int) pm.getLong(0);
            while (true) {
                final int footer = size - Integer.BYTES - pm.getInt(size - Integer.BYTES);
                if (pm.getLong(footer) + pm.getInt(footer + 8) + 8 == parquetFileSize) {
                    return pm.getLong(footer + 32) & (PM_DEDUP_SORT_BIT | PM_INDEX_ROW_MAP_BIT);
                }
                final int prevSize = (int) pm.getLong(footer + 24);
                Assert.assertTrue("no _pm footer names the parquet file size", prevSize > 0 && prevSize < size);
                size = prevSize;
            }
        }
    }

    private static Path tablePath(Path path, TableToken token) {
        return path.of(configuration.getDbRoot()).concat(token);
    }

    private void assertCommitModeLocks(String commitMode) throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, commitMode);
        assertMemoryLeak(() -> {
            execute(WAL_TABLE);
            execute("insert into t values ('2024-09-01T00:00:00.000000Z', 1)");
            execute("alter table t add column s symbol");
            execute("insert into t values ('2024-09-02T00:00:00.000000Z', 2, 'a')");
            drainWalQueue();
            engine.releaseAllWriters();
            try (Path path = new Path()) {
                tablePath(path, engine.verifyTableName("t"));
                Assert.assertEquals(ColumnType.DOWNGRADE_LOCKED_STORAGE_VERSION, metaStorageVersion(path));
                Assert.assertTrue("a commit must store a _cv trailer under " + commitMode, cvLiveAreaHasTrailer(path));
                assertOlderReleaseRefuses(path);
            }
        });
    }

    private void assertLockedThenUnlocked(String createTable, String commitMode) throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, commitMode);
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
                Assert.assertTrue("a commit must store a _cv trailer", cvLiveAreaHasTrailer(path));
                assertOlderReleaseRefuses(path);

                // A start without the setting keeps the lock, in the same mode and in NOSYNC.
                engine.getTableSequencerAPI().resetForReboot(token);
                new RecoveryCoordinator(engine).recover();
                Assert.assertEquals(ColumnType.DOWNGRADE_LOCKED_STORAGE_VERSION, metaStorageVersion(path));
                setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
                engine.getTableSequencerAPI().resetForReboot(token);
                new RecoveryCoordinator(engine).recover();
                Assert.assertEquals(ColumnType.DOWNGRADE_LOCKED_STORAGE_VERSION, metaStorageVersion(path));
                Assert.assertTrue(cvLiveAreaHasTrailer(path));

                // The way down: a downgrade start removes the trailers of both areas and the lock.
                downgradeStart(token);
                Assert.assertEquals(ColumnType.VERSION, metaStorageVersion(path));
                Assert.assertFalse("the downgrade start must remove every _cv trailer", cvAnyAreaHasTrailer(path));
                assertOlderReleaseOpens(path);

                // The table is intact, and the writes of the downgrade start keep it that way.
                assertQuery("t").timestamp("ts").expectSize().returns("ts\tv\ts\n2024-09-01T00:00:00.000000Z\t1\t\n2024-09-02T00:00:00.000000Z\t2\ta\n");
                execute("alter table t add column u int");
                execute("insert into t values ('2024-09-03T00:00:00.000000Z', 3, 'b', 4)");
                drainWalQueue();
                engine.releaseAllWriters();
                Assert.assertEquals(ColumnType.VERSION, metaStorageVersion(path));
                Assert.assertFalse(cvAnyAreaHasTrailer(path));
                assertOlderReleaseOpens(path);
                assertQuery("select count() from t").noRandomAccess().expectSize().returns("count\n3\n");

                // Running again is a no-op.
                engine.releaseAllReaders();
                engine.getTableSequencerAPI().resetForReboot(token);
                new RecoveryCoordinator(engine).recover();
                Assert.assertEquals(ColumnType.VERSION, metaStorageVersion(path));
                Assert.assertFalse(cvAnyAreaHasTrailer(path));

                // The way back up: without the setting the next writer locks the table again and its commits
                // store trailers again.
                setProperty(PropertyKey.CAIRO_COMMIT_MODE_ADAPTIVE_DOWNGRADE, "false");
                setProperty(PropertyKey.CAIRO_COMMIT_MODE, commitMode);
                engine.getTableSequencerAPI().resetForReboot(token);
                new RecoveryCoordinator(engine).recover();
                Assert.assertEquals("only a writer locks a table", ColumnType.VERSION, metaStorageVersion(path));
                execute("alter table t add column w int");
                execute("insert into t values ('2024-09-04T00:00:00.000000Z', 4, 'c', 5, 6)");
                drainWalQueue();
                engine.releaseAllWriters();
                Assert.assertEquals(ColumnType.DOWNGRADE_LOCKED_STORAGE_VERSION, metaStorageVersion(path));
                Assert.assertTrue(cvLiveAreaHasTrailer(path));
                assertOlderReleaseRefuses(path);
                assertQuery("t").timestamp("ts").expectSize().returns("""
                        ts\tv\ts\tu\tw
                        2024-09-01T00:00:00.000000Z\t1\t\tnull\tnull
                        2024-09-02T00:00:00.000000Z\t2\ta\tnull\tnull
                        2024-09-03T00:00:00.000000Z\t3\tb\t4\tnull
                        2024-09-04T00:00:00.000000Z\t4\tc\t5\t6
                        """);
            }
        });
    }

    /**
     * A dedup table with an indexed symbol whose parquet partition 2024-01-01 an O3 commit updated in place: the
     * partition's {@code _pm} footer holds both the dedup sort section and the index row map.
     */
    private TableToken createParquetPartitionWithBothSections() throws Exception {
        // Small row groups and permissive rewrite thresholds keep the O3 merge in place.
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_RATIO, "1.0");
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, Long.MAX_VALUE);
        execute("create table t (s symbol index, v int, ts timestamp) timestamp(ts) partition by day wal dedup upsert keys(ts, s)");
        execute("""
                insert into t values
                ('a', 1, '2024-01-01T01:00:00.000000Z'), ('b', 2, '2024-01-01T02:00:00.000000Z'),
                ('c', 3, '2024-01-01T03:00:00.000000Z'), ('a', 4, '2024-01-01T04:00:00.000000Z'),
                ('b', 5, '2024-01-01T05:00:00.000000Z'), (null, 6, '2024-01-01T06:00:00.000000Z'),
                ('a', 7, '2024-01-01T07:00:00.000000Z'), ('b', 8, '2024-01-01T08:00:00.000000Z'),
                ('c', 9, '2024-01-01T09:00:00.000000Z'), ('a', 10, '2024-01-01T10:00:00.000000Z'),
                ('b', 11, '2024-01-01T11:00:00.000000Z'), ('c', 12, '2024-01-01T12:00:00.000000Z'),
                ('a', 99, '2024-01-02T00:00:00.000000Z')
                """);
        drainWalQueue();
        execute("alter table t convert partition to parquet list '2024-01-01'");
        drainWalQueue();
        final TableToken token = engine.verifyTableName("t");
        final long nameTxn = parquetPartitionNameTxn(token);
        execute("""
                insert into t values
                ('b', 500, '2024-01-01T05:00:00.000000Z'),
                ('a', 501, '2024-01-01T05:00:00.000000Z'),
                ('c', 502, '2024-01-01T07:30:00.000000Z')
                """);
        drainWalQueue();
        engine.releaseAllWriters();
        engine.releaseAllReaders();
        Assert.assertEquals("the O3 merge must update the parquet partition in place", nameTxn, parquetPartitionNameTxn(token));
        Assert.assertEquals(PM_DEDUP_SORT_BIT | PM_INDEX_ROW_MAP_BIT, pmSectionFlags(token));
        return token;
    }

    /**
     * A start with {@code cairo.commit.mode.adaptive.downgrade=true}, which the server accepts under NOSYNC only.
     */
    private void downgradeStart(TableToken token) {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        setProperty(PropertyKey.CAIRO_COMMIT_MODE_ADAPTIVE_DOWNGRADE, "true");
        engine.getTableSequencerAPI().resetForReboot(token);
        new RecoveryCoordinator(engine).recover();
    }
}
