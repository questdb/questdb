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

package io.questdb.test.griffin.engine.table.parquet;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ParquetMetaFileReader;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.griffin.engine.table.parquet.ParquetCompression;
import io.questdb.griffin.engine.table.parquet.PartitionDescriptor;
import io.questdb.griffin.engine.table.parquet.PartitionEncoder;
import io.questdb.griffin.engine.table.parquet.PartitionUpdater;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.TestTimestampType;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class PartitionUpdaterTest extends AbstractCairoTest {
    private final TestTimestampType timestampType;

    public PartitionUpdaterTest(TestTimestampType timestampType) {
        this.timestampType = timestampType;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> testParams() {
        return Arrays.asList(new Object[][]{
                {TestTimestampType.MICRO}, {TestTimestampType.NANO}
        });
    }

    @Test
    public void testBadUpdate() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "x";
            final long rows = 10;
            final FilesFacade ff = configuration.getFilesFacade();
            execute("create table " + tableName + " as (select" +
                    " x id," +
                    " timestamp_sequence(400000000000, 500)::" + timestampType.getTypeName() + " designated_ts" +
                    " from long_sequence(" + rows + ")) timestamp(designated_ts) partition by day");

            try (
                    Path path = new Path();
                    PartitionDescriptor descriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader(tableName);
                    PartitionUpdater updater = new PartitionUpdater()
            ) {
                // Initial partition dir created.
                final TableToken table = engine.getTableTokenIfExists(tableName);
                path
                        .concat(root)
                        .concat(table.getDirNameUtf8())
                        .concat("1970-01-05")
                        .slash$();
                final int partitionDirLen = path.size();
                Assert.assertTrue(ff.exists(path.$()));
                PartitionEncoder.populateFromTableReader(reader, descriptor, 0);

                final CairoException badPathExc = Assert.assertThrows(
                        CairoException.class,
                        () -> PartitionEncoder.encode(descriptor, path));
                TestUtils.assertContains(
                        badPathExc.getMessage(),
                        "Could not create parquet file");

                path.trimTo(partitionDirLen);
                path.put(".1").slash$();
                ff.mkdirs(path, configuration.getMkDirMode());
                Assert.assertTrue(ff.exists(path.$()));
                Assert.assertTrue(ff.isDirOrSoftLinkDir(path.$()));
                path.concat("data.parquet").$();

                PartitionEncoder.populateFromTableReader(reader, descriptor, 0);
                PartitionEncoder.encode(descriptor, path);

                // The parquet file has been created.
                // The old directory is still there. We will not do the extra accounting here in this test.
                Assert.assertTrue(ff.exists(path.$()));
                final long parquetPartitionSize = ff.length(path.$());

                // Update the partition, adding a row.
                final int opts = configuration.getWriterFileOpenOpts();
                final int readerFd = Files.detach(ff.openRONoCache(path.$()));
                final int writerFd = Files.detach(ff.openRW(path.$(), opts));
                updater.of(
                        path.$(),
                        readerFd,
                        parquetPartitionSize,
                        writerFd,
                        parquetPartitionSize,
                        1,  // index of the timestamp column
                        0L, // uncompressed
                        false,
                        false,
                        0L,
                        0L,
                        0.01,
                        0.0,
                        -1, // no _pm fd
                        0L,
                        0L,
                        -1L,
                        -1L // seq_txn
                );

                PartitionEncoder.populateFromTableReader(reader, descriptor, 0);

                // Once with a bad row group id
                final CairoException badGroupIdExc = Assert.assertThrows(
                        CairoException.class,
                        () -> updater.updateRowGroup((short) 1000, descriptor));
                TestUtils.assertContains(
                        badGroupIdExc.getMessage(),
                        "Row group ordinal must be less then 1, got 1000");

                // Once again with the correct row group id
                PartitionEncoder.populateFromTableReader(reader, descriptor, 0);
                updater.updateRowGroup((short) 0, descriptor);
                updater.updateFileMetadata();

                final long updatedParquetPartitionSize = ff.length(path.$());
                Assert.assertTrue(updatedParquetPartitionSize > parquetPartitionSize);
            }
        });
    }

    @Test
    public void testCommitPublishesHeaderWhileCommittedReaderResolvesOldFooter() throws Exception {
        // Exercises commitParquetMeta's real header-publish through two real
        // updates (no spliced buffer, no fail-point). Update A full-creates the
        // _pm at committedHead. Update B is a normal incremental update: its
        // end() appends a new committed footer at [committedHead, newHead) but
        // leaves the header at committedHead, then commitParquetMeta fsyncs the
        // appended footer, patches the header to newHead, and fsyncs again. That
        // reproduces the "tail durable, header published, then crash before the
        // _txn commit" sub-window:
        // getFileSize() reports the published-ahead header newHead, yet a reader
        // still pinned to the unchanged committed _txn resolves the committed
        // footer at committedHead by walking the MVCC chain back -- no torn read.
        // The sync calls themselves are not fault-injected here; their ordering
        // is covered through the Rust helper's injected sync hook. Each update
        // appends a row group to data.parquet, so the two
        // snapshots' footers carry distinct parquet-size tokens; resolveFooter
        // keys on that parquet data size (from _txn), so the committed token
        // (committedParquetSize) resolves Update A's footer while
        // resolveLastFooter() takes Update B's.

        assertMemoryLeak(() -> {
            final String tableName = "commit_publish_test";
            final long rows = 10;
            final FilesFacade ff = configuration.getFilesFacade();
            execute("CREATE TABLE " + tableName + " AS (SELECT" +
                    " x id," +
                    " timestamp_sequence(400_000_000_000, 500)::" + timestampType.getTypeName() + " designated_ts" +
                    " FROM long_sequence(" + rows + ")) TIMESTAMP(designated_ts) PARTITION BY DAY");

            try (
                    Path path = new Path();
                    PartitionDescriptor descriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader(tableName);
                    PartitionUpdater updater = new PartitionUpdater()
            ) {
                final TableToken table = engine.getTableTokenIfExists(tableName);
                path.concat(root).concat(table.getDirNameUtf8()).concat("1970-01-05").slash$();
                path.put(".1").slash$();
                final int versionedDirLen = path.size();
                ff.mkdirs(path, configuration.getMkDirMode());
                path.concat("data.parquet").$();

                PartitionEncoder.populateFromTableReader(reader, descriptor, 0);
                PartitionEncoder.encode(descriptor, path);
                final long parquetDataSize0 = ff.length(path.$());
                final int opts = configuration.getWriterFileOpenOpts();

                // Update A: full-create the _pm (gate == 0). The full-create
                // writes the header as part of its bytes, so the on-disk header
                // already equals committedHead -- no commitParquetMeta needed.
                path.trimTo(versionedDirLen).concat(TableUtils.PARQUET_METADATA_FILE_NAME).$();
                int parquetMetaFd = Files.detach(ff.openRW(path.$(), opts));
                path.trimTo(versionedDirLen).concat("data.parquet").$();
                int readerFd = Files.detach(ff.openRONoCache(path.$()));
                int writerFd = Files.detach(ff.openRW(path.$(), opts));
                updater.of(path.$(), readerFd, parquetDataSize0, writerFd, parquetDataSize0,
                        1, 0L, false, false, 0L, 0L, 0.01, 0.0, parquetMetaFd, parquetDataSize0, parquetDataSize0, 0L, -1);
                PartitionEncoder.populateFromTableReader(reader, descriptor, 0);
                updater.updateRowGroup((short) 0, descriptor);
                updater.updateFileMetadata();
                final long committedHead = updater.getResultParquetMetaFileSize();
                Assert.assertTrue(committedHead > 0);
                // Update A appended a row group + new footer: data.parquet grew,
                // and committedParquetSize is the committed footer's MVCC token.
                final long committedParquetSize = ff.length(path.$());
                // The full-create wrote the header as part of its bytes, so the
                // on-disk _pm length already equals committedHead.
                path.trimTo(versionedDirLen).concat(TableUtils.PARQUET_METADATA_FILE_NAME).$();
                Assert.assertEquals("full-create _pm length is committedHead", committedHead, ff.length(path.$()));

                // Update B: a normal incremental update (gate > 0). Parse anchor
                // and append base both sit cleanly at the committed head (NOT the
                // dirty-ahead case). end() appends another row group + footer to
                // data.parquet and writes the new _pm footer at
                // [committedHead, newHead), leaving the header at committedHead.
                path.trimTo(versionedDirLen).concat(TableUtils.PARQUET_METADATA_FILE_NAME).$();
                parquetMetaFd = Files.detach(ff.openRW(path.$(), opts));
                path.trimTo(versionedDirLen).concat("data.parquet").$();
                readerFd = Files.detach(ff.openRONoCache(path.$()));
                writerFd = Files.detach(ff.openRW(path.$(), opts));
                updater.of(path.$(), readerFd, committedParquetSize, writerFd, committedParquetSize,
                        1, 0L, false, false, 0L, 0L, 0.01, 0.0, parquetMetaFd, committedHead, committedHead, committedParquetSize, -1L);
                PartitionEncoder.populateFromTableReader(reader, descriptor, 0);
                updater.updateRowGroup((short) 0, descriptor);
                updater.updateFileMetadata();
                final long newHead = updater.getResultParquetMetaFileSize();
                Assert.assertTrue("incremental update must extend the _pm", newHead > committedHead);
                path.trimTo(versionedDirLen).concat(TableUtils.PARQUET_METADATA_FILE_NAME).$();
                Assert.assertEquals(
                        "metadata append must not publish the new header",
                        committedHead,
                        ParquetMetaFileReader.readParquetMetaFileSize(ff, path.$())
                );

                // Publish through the real tail-sync, header-patch, header-sync
                // sequence.
                updater.commitParquetMeta(true);

                path.trimTo(versionedDirLen).concat(TableUtils.PARQUET_METADATA_FILE_NAME).$();
                Assert.assertEquals("physical _pm length is newHead", newHead, ff.length(path.$()));

                // A reader pinned to the OLD committed _txn (the committed parquet
                // data size token) resolves the committed footer, even though the
                // header was published ahead to newHead.
                ParquetMetaFileReader parquetMetaReader = new ParquetMetaFileReader();
                long addr = ParquetMetaFileReader.openAndMapRO(ff, path.$(), parquetMetaReader);
                Assert.assertTrue("openAndMapRO must map the _pm", addr != 0);
                long mappedSize = parquetMetaReader.getFileSize();
                try {
                    Assert.assertEquals("header published ahead to newHead", newHead, parquetMetaReader.getFileSize());
                    // resolveFooter verifies the resolved footer's cumulative CRC,
                    // so corruption would throw here.
                    Assert.assertTrue(parquetMetaReader.resolveFooter(committedParquetSize));
                    Assert.assertEquals("pinned reader resolves the committed footer", committedHead, parquetMetaReader.getResolvedFileSize());
                    Assert.assertTrue("committed head precedes the published header", parquetMetaReader.getResolvedFileSize() < parquetMetaReader.getFileSize());
                } finally {
                    parquetMetaReader.clear();
                    ff.munmap(addr, mappedSize, MemoryTag.MMAP_PARQUET_METADATA_READER);
                }

                // The newly published footer is itself valid: a post-recovery
                // reader (once _txn advances to newHead) takes the physically-last
                // footer and resolves newHead.
                parquetMetaReader = new ParquetMetaFileReader();
                addr = ParquetMetaFileReader.openAndMapRO(ff, path.$(), parquetMetaReader);
                Assert.assertTrue("openAndMapRO must map the _pm", addr != 0);
                mappedSize = parquetMetaReader.getFileSize();
                try {
                    Assert.assertTrue(parquetMetaReader.resolveLastFooter());
                    Assert.assertEquals("physically-last footer resolves to newHead", newHead, parquetMetaReader.getResolvedFileSize());
                } finally {
                    parquetMetaReader.clear();
                    ff.munmap(addr, mappedSize, MemoryTag.MMAP_PARQUET_METADATA_READER);
                }
            }
        });
    }

    @Test
    public void testPerColumnEncodingWithCompression() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "enc_test";
            final long rows = 10;
            final FilesFacade ff = configuration.getFilesFacade();
            execute("CREATE TABLE " + tableName + " AS (SELECT" +
                    " x id," +
                    " timestamp_sequence(400_000_000_000, 500)::" + timestampType.getTypeName() + " designated_ts" +
                    " FROM long_sequence(" + rows + ")) TIMESTAMP(designated_ts) PARTITION BY DAY");

            // Set per-column parquet encoding
            execute("ALTER TABLE " + tableName + " ALTER COLUMN id SET PARQUET(delta_binary_packed, zstd)");

            try (
                    Path path = new Path();
                    PartitionDescriptor descriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader(tableName);
                    PartitionUpdater updater = new PartitionUpdater()
            ) {
                final TableToken table = engine.getTableTokenIfExists(tableName);
                path.concat(root)
                        .concat(table.getDirNameUtf8())
                        .concat("1970-01-05")
                        .slash$();
                path.put(".1").slash$();
                ff.mkdirs(path, configuration.getMkDirMode());
                path.concat("data.parquet").$();

                // Encode with per-column encoding config from metadata
                PartitionEncoder.populateFromTableReader(reader, descriptor, 0);
                PartitionEncoder.encode(descriptor, path);

                Assert.assertTrue(ff.exists(path.$()));
                final long parquetPartitionSize = ff.length(path.$());

                // Update with ZSTD compression
                long compressionCodec = ParquetCompression.packCompressionCodecLevel(
                        ParquetCompression.COMPRESSION_ZSTD, 1
                );

                // Update the partition, adding a row.
                final int opts = configuration.getWriterFileOpenOpts();
                final int readerFd = Files.detach(ff.openRONoCache(path.$()));
                final int writerFd = Files.detach(ff.openRW(path.$(), opts));
                updater.of(
                        path.$(),
                        readerFd,
                        parquetPartitionSize,
                        writerFd,
                        parquetPartitionSize,
                        1,  // index of the timestamp column
                        compressionCodec,
                        false,
                        false,
                        0L,
                        0L,
                        0.01,
                        0.0,
                        -1, // no _pm fd
                        0L,
                        0L,
                        -1L,
                        -1L // seq_txn
                );

                PartitionEncoder.populateFromTableReader(reader, descriptor, 0);
                updater.updateRowGroup((short) 0, descriptor);
                updater.updateFileMetadata();

                final long updatedParquetPartitionSize = ff.length(path.$());
                Assert.assertTrue(updatedParquetPartitionSize > parquetPartitionSize);
            }
        });
    }

    @Test
    public void testUpdateAppendsPastDeadTail() throws Exception {
        // Crash-window path: an in-place _pm update appends past an orphaned dead
        // footer when the header is dirty-ahead (a prior update published a footer
        // past the committed head, then crashed before its _txn commit). We build
        // a committed _pm, splice a dead tail and patch the header ahead of the
        // committed footer, then run an incremental update and assert it appends
        // strictly past the dead tail and the result still resolves (its
        // cumulative CRC spans the tail).
        assertMemoryLeak(() -> {
            final String tableName = "dead_tail_test";
            final long rows = 10;
            final FilesFacade ff = configuration.getFilesFacade();
            execute("CREATE TABLE " + tableName + " AS (SELECT" +
                    " x id," +
                    " timestamp_sequence(400_000_000_000, 500)::" + timestampType.getTypeName() + " designated_ts" +
                    " FROM long_sequence(" + rows + ")) TIMESTAMP(designated_ts) PARTITION BY DAY");

            try (
                    Path path = new Path();
                    PartitionDescriptor descriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader(tableName);
                    PartitionUpdater updater = new PartitionUpdater()
            ) {
                final TableToken table = engine.getTableTokenIfExists(tableName);
                path.concat(root).concat(table.getDirNameUtf8()).concat("1970-01-05").slash$();
                path.put(".1").slash$();
                final int versionedDirLen = path.size();
                ff.mkdirs(path, configuration.getMkDirMode());
                path.concat("data.parquet").$();

                PartitionEncoder.populateFromTableReader(reader, descriptor, 0);
                PartitionEncoder.encode(descriptor, path);
                final long parquetDataSize0 = ff.length(path.$());
                final int opts = configuration.getWriterFileOpenOpts();

                // Update 1: full-create the _pm (gate == 0).
                path.trimTo(versionedDirLen).concat(TableUtils.PARQUET_METADATA_FILE_NAME).$();
                int parquetMetaFd = Files.detach(ff.openRW(path.$(), opts));
                path.trimTo(versionedDirLen).concat("data.parquet").$();
                int readerFd = Files.detach(ff.openRONoCache(path.$()));
                int writerFd = Files.detach(ff.openRW(path.$(), opts));
                updater.of(path.$(), readerFd, parquetDataSize0, writerFd, parquetDataSize0,
                        1, 0L, false, false, 0L, 0L, 0.01, 0.0, parquetMetaFd, parquetDataSize0, parquetDataSize0, 0L, -1L);
                PartitionEncoder.populateFromTableReader(reader, descriptor, 0);
                updater.updateRowGroup((short) 0, descriptor);
                updater.updateFileMetadata();
                final long committedHead = updater.getResultParquetMetaFileSize();
                Assert.assertTrue(committedHead > 0);
                final long parquetDataSize1 = ff.length(path.$());

                // Splice an orphaned dead tail (here just garbage -- the next
                // update folds it into the CRC, it is never parsed as a footer)
                // past the committed head and patch the header to point past it.
                // This is the crash-window state: a published footer the _txn
                // commit never reached. The committed footer stays at
                // committedHead; the next update appends past the dead tail.
                final int deadBytes = 96;
                final long physicalWithDeadTail = committedHead + deadBytes;
                path.trimTo(versionedDirLen).concat(TableUtils.PARQUET_METADATA_FILE_NAME).$();
                final long deadTailFd = ff.openRW(path.$(), opts);
                final long scratch = Unsafe.malloc(deadBytes, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < deadBytes; i++) {
                        Unsafe.putByte(scratch + i, (byte) (0xAB + i));
                    }
                    Assert.assertEquals(deadBytes, ff.write(deadTailFd, scratch, deadBytes, committedHead));
                    // Dirty-ahead header: published size points past the dead tail.
                    Unsafe.putLong(scratch, physicalWithDeadTail);
                    Assert.assertEquals(Long.BYTES, ff.write(deadTailFd, scratch, Long.BYTES, 0));
                } finally {
                    Unsafe.free(scratch, deadBytes, MemoryTag.NATIVE_DEFAULT);
                    ff.close(deadTailFd);
                }
                Assert.assertEquals(physicalWithDeadTail, ff.length(path.$()));

                // Update 2: incremental update (gate > 0, parse anchor = the
                // committed head, append base = the dirty-ahead header). It must
                // append past the dead tail.
                parquetMetaFd = Files.detach(ff.openRW(path.$(), opts));
                path.trimTo(versionedDirLen).concat("data.parquet").$();
                readerFd = Files.detach(ff.openRONoCache(path.$()));
                writerFd = Files.detach(ff.openRW(path.$(), opts));
                updater.of(path.$(), readerFd, parquetDataSize1, writerFd, parquetDataSize1,
                        1, 0L, false, false, 0L, 0L, 0.01, 0.0, parquetMetaFd, committedHead, physicalWithDeadTail, parquetDataSize1, -1L);
                PartitionEncoder.populateFromTableReader(reader, descriptor, 0);
                updater.updateRowGroup((short) 0, descriptor);
                updater.updateFileMetadata();
                final long newHead = updater.getResultParquetMetaFileSize();
                // Publish the new snapshot (patch header + fsync), as the O3 job
                // does after the index build.
                updater.commitParquetMeta(true);

                // The new footer landed strictly past the dead tail.
                Assert.assertTrue("must append past the dead tail", newHead > physicalWithDeadTail);
                path.trimTo(versionedDirLen).concat(TableUtils.PARQUET_METADATA_FILE_NAME).$();
                Assert.assertEquals(newHead, ff.length(path.$()));

                // The published _pm resolves: openAndMapRO maps the new header and
                // resolveFooter verifies the resolved footer's CRC -- which spans
                // the dead tail -- so a wrong CRC would throw here.
                final ParquetMetaFileReader parquetMetaReader = new ParquetMetaFileReader();
                final long addr = ParquetMetaFileReader.openAndMapRO(ff, path.$(), parquetMetaReader);
                Assert.assertTrue("openAndMapRO must map the _pm", addr != 0);
                final long mappedSize = parquetMetaReader.getFileSize();
                try {
                    Assert.assertEquals("header published to the new head", newHead, parquetMetaReader.getFileSize());
                    Assert.assertTrue(parquetMetaReader.resolveLastFooter());
                    Assert.assertEquals(newHead, parquetMetaReader.getResolvedFileSize());
                } finally {
                    parquetMetaReader.clear();
                    ff.munmap(addr, mappedSize, MemoryTag.MMAP_PARQUET_METADATA_READER);
                }
            }
        });
    }

    @Test
    public void testUpdateWithParquetMetaFd() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "parquet_meta_test";
            final long rows = 10;
            final FilesFacade ff = configuration.getFilesFacade();
            execute("CREATE TABLE " + tableName + " AS (SELECT" +
                    " x id," +
                    " timestamp_sequence(400_000_000_000, 500)::" + timestampType.getTypeName() + " designated_ts" +
                    " FROM long_sequence(" + rows + ")) TIMESTAMP(designated_ts) PARTITION BY DAY");

            try (
                    Path path = new Path();
                    PartitionDescriptor descriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader(tableName);
                    PartitionUpdater updater = new PartitionUpdater()
            ) {
                final TableToken table = engine.getTableTokenIfExists(tableName);
                path.concat(root)
                        .concat(table.getDirNameUtf8())
                        .concat("1970-01-05")
                        .slash$();
                final int partitionDirLen = path.size();
                path.put(".1").slash$();
                final int versionedDirLen = path.size();
                ff.mkdirs(path, configuration.getMkDirMode());
                path.concat("data.parquet").$();

                PartitionEncoder.populateFromTableReader(reader, descriptor, 0);
                PartitionEncoder.encode(descriptor, path);

                Assert.assertTrue(ff.exists(path.$()));
                final long parquetPartitionSize = ff.length(path.$());

                // Open _pm file for writing alongside the parquet file.
                path.trimTo(versionedDirLen).concat(TableUtils.PARQUET_METADATA_FILE_NAME).$();
                final int opts = configuration.getWriterFileOpenOpts();
                final int parquetMetaFd = Files.detach(ff.openRW(path.$(), opts));

                // Open parquet reader/writer fds.
                path.trimTo(versionedDirLen).concat("data.parquet").$();
                final int readerFd = Files.detach(ff.openRONoCache(path.$()));
                final int writerFd = Files.detach(ff.openRW(path.$(), opts));

                updater.of(
                        path.$(),
                        readerFd,
                        parquetPartitionSize,
                        writerFd,
                        parquetPartitionSize,
                        1,  // index of the timestamp column
                        0L, // uncompressed
                        false,
                        false,
                        0L,
                        0L,
                        0.01,
                        0.0,
                        parquetMetaFd,
                        parquetPartitionSize,
                        parquetPartitionSize,
                        0L,
                        -1L // seq_txn
                );

                PartitionEncoder.populateFromTableReader(reader, descriptor, 0);
                updater.updateRowGroup((short) 0, descriptor);
                updater.updateFileMetadata();

                final long parquetMetaFileSize = updater.getResultParquetMetaFileSize();
                Assert.assertTrue("_pm file size must be > 0", parquetMetaFileSize > 0);

                // Verify the _pm file exists on disk with the reported size.
                path.trimTo(versionedDirLen).concat(TableUtils.PARQUET_METADATA_FILE_NAME).$();
                Assert.assertTrue("_pm file must exist", ff.exists(path.$()));
                Assert.assertEquals("_pm file size on disk", parquetMetaFileSize, ff.length(path.$()));
            }
        });
    }

}
