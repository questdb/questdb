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

package io.questdb.test.cairo.mig;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ParquetMetaFileReader;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.TxWriter;
import io.questdb.cairo.mig.EngineMigration;
import io.questdb.cairo.mig.Mig941;
import io.questdb.cairo.mig.MigrationContext;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryARW;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.nio.file.Files;

public class Mig941Test extends AbstractCairoTest {

    /// Committed parquet fixture: single designated-ts column, two row
    /// groups of 10 rows each, row values 0..19, no min/max stats on the ts
    /// column. Regenerate via
    /// `cargo test emit_mig941_ts_no_stats_fixture -- --ignored`.
    private static final String TS_NO_STATS_FIXTURE = "/mig941/ts_no_stats.parquet";

    @Test
    public void testMigrateAbortOnCorruptParquetFile() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute("CREATE TABLE t (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES(1, '2024-06-10T00:00:00.000000Z')");
            execute("INSERT INTO t VALUES(2, '2024-06-11T00:00:00.000000Z')");
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET WHERE ts > 0");

            final FilesFacade ff = configuration.getFilesFacade();
            final TableToken token = engine.verifyTableName("t");

            long partitionTs;
            long partitionNameTxn;
            try (TableReader reader = engine.getReader(token)) {
                partitionTs = reader.getTxFile().getPartitionTimestampByIndex(0);
                partitionNameTxn = reader.getTxFile().getPartitionNameTxn(0);
            }

            // Delete _pm, then overwrite data.parquet with garbage to corrupt it.
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(token);
                TableUtils.setPathForParquetPartitionMetadata(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTs, partitionNameTxn);
                ff.remove(path.$());

                path.of(configuration.getDbRoot()).concat(token);
                TableUtils.setPathForParquetPartition(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTs, partitionNameTxn);
                long fd = ff.openRW(path.$(), 0);
                try {
                    // Overwrite the parquet footer (at the end of the file) with garbage.
                    long fileSize = ff.length(fd);
                    long buf = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
                    try {
                        Unsafe.putLong(buf, 0xDEADBEEFDEADBEEFL);
                        ff.write(fd, buf, 8, fileSize - 8);
                    } finally {
                        Unsafe.free(buf, 8, MemoryTag.NATIVE_DEFAULT);
                    }
                } finally {
                    ff.close(fd);
                }
            }

            try {
                runMig941(token);
                Assert.fail("Expected exception from corrupt parquet file");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "parquet");
            }
        });
    }

    @Test
    public void testMigrateAbortOnEmptyParquetFile() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute("CREATE TABLE t (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES(1, '2024-06-10T00:00:00.000000Z')");
            execute("INSERT INTO t VALUES(2, '2024-06-11T00:00:00.000000Z')");
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET WHERE ts > 0");

            final FilesFacade ff = configuration.getFilesFacade();
            final TableToken token = engine.verifyTableName("t");

            long partitionTs;
            long partitionNameTxn;
            try (TableReader reader = engine.getReader(token)) {
                partitionTs = reader.getTxFile().getPartitionTimestampByIndex(0);
                partitionNameTxn = reader.getTxFile().getPartitionNameTxn(0);
            }

            // Delete _pm, then truncate data.parquet to 0 bytes.
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(token);
                TableUtils.setPathForParquetPartitionMetadata(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTs, partitionNameTxn);
                ff.remove(path.$());

                path.of(configuration.getDbRoot()).concat(token);
                TableUtils.setPathForParquetPartition(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTs, partitionNameTxn);
                long fd = ff.openRW(path.$(), 0);
                try {
                    ff.truncate(fd, 0);
                } finally {
                    ff.close(fd);
                }
            }

            try {
                runMig941(token);
                Assert.fail("Expected CairoException");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "parquet file empty or unreadable");
            }
        });
    }

    @Test
    public void testMigrateAbortOnMissingParquetFile() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute("CREATE TABLE t (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES(1, '2024-06-10T00:00:00.000000Z')");
            execute("INSERT INTO t VALUES(2, '2024-06-11T00:00:00.000000Z')");
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET WHERE ts > 0");

            final FilesFacade ff = configuration.getFilesFacade();
            final TableToken token = engine.verifyTableName("t");

            long partitionTs;
            long partitionNameTxn;
            try (TableReader reader = engine.getReader(token)) {
                partitionTs = reader.getTxFile().getPartitionTimestampByIndex(0);
                partitionNameTxn = reader.getTxFile().getPartitionNameTxn(0);
            }

            // Delete data.parquet and _pm to simulate corrupt pre-migration state.
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(token);
                TableUtils.setPathForParquetPartition(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTs, partitionNameTxn);
                ff.remove(path.$());

                path.of(configuration.getDbRoot()).concat(token);
                TableUtils.setPathForParquetPartitionMetadata(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTs, partitionNameTxn);
                ff.remove(path.$());
            }

            try {
                runMig941(token);
                Assert.fail("Expected CairoException");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "parquet file not found");
            }
        });
    }

    @Test
    public void testMigrateBackfillsMissingTsStats() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute("CREATE TABLE t (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES('2024-06-10T00:00:00.000000Z')");
            execute("INSERT INTO t VALUES('2024-06-11T00:00:00.000000Z')");
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET WHERE ts > 0");

            final FilesFacade ff = configuration.getFilesFacade();
            final TableToken token = engine.verifyTableName("t");
            long partitionTs;
            long partitionNameTxn;
            try (TableReader reader = engine.getReader(token)) {
                partitionTs = reader.getTxFile().getPartitionTimestampByIndex(0);
                partitionNameTxn = reader.getTxFile().getPartitionNameTxn(0);
            }
            engine.releaseAllWriters();
            engine.releaseAllReaders();
            engine.releaseInactive();

            // Overwrite data.parquet with a committed fixture that has QdbMeta
            // marking col 0 as designated ts but NO inline stats. The fixture
            // is a different size than the original parquet, so patch _txn
            // field 3 to the new size to preserve the production invariant
            // that _txn carries the authoritative parquet file size.
            long fixtureParquetFileSize;
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(token);
                TableUtils.setPathForParquetPartition(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTs, partitionNameTxn);
                try (InputStream is = Mig941Test.class.getResourceAsStream(TS_NO_STATS_FIXTURE)) {
                    Assert.assertNotNull("fixture missing: " + TS_NO_STATS_FIXTURE, is);
                    byte[] fixtureBytes = is.readAllBytes();
                    Files.write(java.nio.file.Path.of(path.toString()), fixtureBytes);
                    fixtureParquetFileSize = fixtureBytes.length;
                }
                path.of(configuration.getDbRoot()).concat(token);
                TableUtils.setPathForParquetPartitionMetadata(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTs, partitionNameTxn);
                ff.remove(path.$());
            }
            patchTxnParquetFileSize(token, partitionTs, fixtureParquetFileSize);
            runMig941(token);

            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(token);
                TableUtils.setPathForParquetPartitionMetadata(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTs, partitionNameTxn);
                long parquetMetaSize = ParquetMetaFileReader.readParquetMetaFileSize(ff, path.$());
                long parquetMetaAddr = TableUtils.mapRO(ff, path.$(), LOG, parquetMetaSize, MemoryTag.MMAP_DEFAULT);
                try {
                    ParquetMetaFileReader reader = new ParquetMetaFileReader();
                    reader.of(parquetMetaAddr, parquetMetaSize);
                    reader.resolveLastFooter();
                    Assert.assertEquals(1, reader.getColumnCount());
                    Assert.assertEquals(2, reader.getRowGroupCount());
                    // MIN_PRESENT=bit0, MIN_INLINED=bit1, MAX_PRESENT=bit3, MAX_INLINED=bit4.
                    final int minMask = (1 << 0) | (1 << 1);
                    final int maxMask = (1 << 3) | (1 << 4);
                    for (int rg = 0; rg < 2; rg++) {
                        int flags = reader.getChunkStatFlags(rg, 0);
                        Assert.assertEquals("rg " + rg + " min flags", minMask, flags & minMask);
                        Assert.assertEquals("rg " + rg + " max flags", maxMask, flags & maxMask);
                        long expectedMin = rg * 10L;
                        long expectedMax = expectedMin + 9;
                        Assert.assertEquals("rg " + rg + " min", expectedMin, reader.getRowGroupMinTimestamp(rg, 0));
                        Assert.assertEquals("rg " + rg + " max", expectedMax, reader.getRowGroupMaxTimestamp(rg, 0));
                    }
                } finally {
                    ff.munmap(parquetMetaAddr, parquetMetaSize, MemoryTag.MMAP_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testMigrateContinuesAfterCorruptPm() throws Exception {
        // A _pm whose header claims a size larger than the actual file length
        // (e.g. from a torn write of a prior interrupted migration) must not
        // abort the migration of remaining partitions. Mig941 has to treat the
        // unreadable _pm as stale and regenerate it just like the missing-file
        // case.
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute("CREATE TABLE t (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES(1, '2024-06-10T00:00:00.000000Z')," +
                    "(2, '2024-06-11T00:00:00.000000Z')," +
                    "(3, '2024-06-12T00:00:00.000000Z')");
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET WHERE ts > 0");

            final FilesFacade ff = configuration.getFilesFacade();
            final TableToken token = engine.verifyTableName("t");

            long[] partitionTimestamps = new long[2];
            long[] partitionNameTxns = new long[2];
            try (TableReader reader = engine.getReader(token)) {
                Assert.assertEquals(3, reader.getPartitionCount());
                Assert.assertTrue("first partition must be parquet", reader.getTxFile().isPartitionParquet(0));
                Assert.assertTrue("second partition must be parquet", reader.getTxFile().isPartitionParquet(1));
                for (int i = 0; i < 2; i++) {
                    partitionTimestamps[i] = reader.getTxFile().getPartitionTimestampByIndex(i);
                    partitionNameTxns[i] = reader.getTxFile().getPartitionNameTxn(i);
                }
            }

            // Truncate the first partition's _pm to a size smaller than the
            // value its header records at offset 0. The first 8 bytes survive
            // the truncate, so openAndMapRO reads parquetMetaFileSize > actual
            // length and throws CairoException. The second partition's _pm is
            // left intact.
            long preCorruptionSize;
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(token);
                TableUtils.setPathForParquetPartitionMetadata(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTimestamps[0], partitionNameTxns[0]);
                long fd = ff.openRW(path.$(), 0);
                try {
                    preCorruptionSize = ff.length(fd);
                    Assert.assertTrue("_pm must be larger than 50 bytes for the corruption to be observable", preCorruptionSize > 50);
                    Assert.assertTrue("truncate _pm to 50 bytes", ff.truncate(fd, 50));
                } finally {
                    ff.close(fd);
                }
            }

            // Migration must complete without throwing, even though the first
            // partition's _pm reports an inconsistent size.
            runMig941(token);

            // Both _pm files exist, are readable, and resolve a footer.
            for (int i = 0; i < 2; i++) {
                try (Path path = new Path()) {
                    path.of(configuration.getDbRoot()).concat(token);
                    TableUtils.setPathForParquetPartitionMetadata(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTimestamps[i], partitionNameTxns[i]);
                    Assert.assertTrue("_pm must exist after migration (partition " + i + ")", ff.exists(path.$()));

                    long parquetMetaSize = ParquetMetaFileReader.readParquetMetaFileSize(ff, path.$());
                    Assert.assertTrue("_pm must have positive size (partition " + i + ")", parquetMetaSize > 0);

                    long parquetMetaAddr = TableUtils.mapRO(ff, path.$(), LOG, parquetMetaSize, MemoryTag.MMAP_DEFAULT);
                    try {
                        ParquetMetaFileReader parquetMetaReader = new ParquetMetaFileReader();
                        parquetMetaReader.of(parquetMetaAddr, parquetMetaSize);
                        Assert.assertTrue(
                                "regenerated _pm must resolve a footer (partition " + i + ")",
                                parquetMetaReader.resolveLastFooter()
                        );
                        Assert.assertEquals(2, parquetMetaReader.getColumnCount());
                    } finally {
                        ff.munmap(parquetMetaAddr, parquetMetaSize, MemoryTag.MMAP_DEFAULT);
                    }
                }
            }
        });
    }

    @Test
    public void testMigrateDoesNotRerunOnRestart() throws Exception {
        // With cairo.repeat.migration.from.version at its default (-1), once the
        // dispatcher has stamped MIGRATION_VERSION into _upgrade.d it short-circuits
        // on subsequent startups (EngineMigration.java:114). The forward-compatible
        // Mig941 must NOT re-run on every boot. Regression guard for the every-startup
        // _pm regeneration that occurred when the repeat version defaulted to
        // ColumnType.VERSION and so reset the recorded migration version each start.
        node1.setProperty(PropertyKey.CAIRO_REPEAT_MIGRATION_FROM_VERSION, -1);

        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute("CREATE TABLE t (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES(1, '2024-06-10T00:00:00.000000Z')");
            execute("INSERT INTO t VALUES(2, '2024-06-11T00:00:00.000000Z')");
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET WHERE ts > 0");

            final FilesFacade ff = configuration.getFilesFacade();
            final TableToken token = engine.verifyTableName("t");

            long partitionTs;
            long partitionNameTxn;
            try (TableReader reader = engine.getReader(token)) {
                partitionTs = reader.getTxFile().getPartitionTimestampByIndex(0);
                partitionNameTxn = reader.getTxFile().getPartitionNameTxn(0);
            }

            engine.releaseAllWriters();
            engine.releaseAllReaders();
            engine.releaseInactive();

            // First dispatcher run stamps MIGRATION_VERSION into _upgrade.d.
            EngineMigration.migrateEngineTo(engine, ColumnType.VERSION, ColumnType.MIGRATION_VERSION, false);

            // Remove the _pm. A re-run of the migration would regenerate it.
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(token);
                TableUtils.setPathForParquetPartitionMetadata(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTs, partitionNameTxn);
                ff.remove(path.$());
                Assert.assertFalse("_pm should be deleted", ff.exists(path.$()));
            }

            // Second dispatcher run must short-circuit: the migration version is
            // already recorded and the repeat default does not force a re-run, so
            // Mig941 does not execute and the deleted _pm stays absent.
            EngineMigration.migrateEngineTo(engine, ColumnType.VERSION, ColumnType.MIGRATION_VERSION, false);

            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(token);
                TableUtils.setPathForParquetPartitionMetadata(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTs, partitionNameTxn);
                Assert.assertFalse(
                        "migration must not re-run on restart; the deleted _pm should stay absent",
                        ff.exists(path.$())
                );
            }
        });
    }

    @Test
    public void testMigrateGeneratesPmForParquetPartitions() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute("CREATE TABLE t (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES(1, '2024-06-10T00:00:00.000000Z')");
            execute("INSERT INTO t VALUES(2, '2024-06-11T00:00:00.000000Z')");
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET WHERE ts > 0");

            final FilesFacade ff = configuration.getFilesFacade();
            final TableToken token = engine.verifyTableName("t");

            // Record partition info AFTER conversion (name txn changes).
            long partitionTs;
            long partitionNameTxn;
            try (TableReader reader = engine.getReader(token)) {
                // Only the first partition was converted — second is active.
                partitionTs = reader.getTxFile().getPartitionTimestampByIndex(0);
                partitionNameTxn = reader.getTxFile().getPartitionNameTxn(0);
            }

            // Delete existing _pm file to simulate pre-migration state.
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(token);
                TableUtils.setPathForParquetPartitionMetadata(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTs, partitionNameTxn);
                ff.remove(path.$());
                Assert.assertFalse("_pm should be deleted", ff.exists(path.$()));
            }

            // Run migration.
            runMig941(token);

            // Verify _pm file was generated.
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(token);
                TableUtils.setPathForParquetPartitionMetadata(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTs, partitionNameTxn);
                Assert.assertTrue("_pm should exist after migration", ff.exists(path.$()));

                long parquetMetaSize = ParquetMetaFileReader.readParquetMetaFileSize(ff, path.$());
                Assert.assertTrue("_pm should have positive size", parquetMetaSize > 0);

                // Read and validate the generated _pm file.
                long parquetMetaAddr = TableUtils.mapRO(ff, path.$(), LOG, parquetMetaSize, MemoryTag.MMAP_DEFAULT);
                try {
                    ParquetMetaFileReader reader = new ParquetMetaFileReader();
                    reader.of(parquetMetaAddr, parquetMetaSize);
                    reader.resolveLastFooter();
                    Assert.assertEquals(2, reader.getColumnCount());
                    Assert.assertEquals(1, reader.getRowGroupCount());
                    Assert.assertTrue(reader.getParquetFileSize() > 0);
                } finally {
                    ff.munmap(parquetMetaAddr, parquetMetaSize, MemoryTag.MMAP_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testMigrateInlinesBloomFiltersInPm() throws Exception {
        // Regression cover: an earlier version of Mig941 recorded each chunk's
        // bloom_filter_offset in the regenerated _pm but never read the bitset,
        // leaving the BLOOM_FILTERS feature-flag bit clear in the header.
        // Readers then had to fall back to reading the bitset from data.parquet
        // on every query touching the bloom-indexed column. The migration must
        // produce a _pm equivalent to the one the write path produces, with
        // the BLOOM_FILTERS bit set.
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute("CREATE TABLE t (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("ALTER TABLE t ALTER COLUMN id SET PARQUET(bloom_filter)");
            execute("INSERT INTO t VALUES(1, '2024-06-10T00:00:00.000000Z')," +
                    "(2, '2024-06-10T01:00:00.000000Z')," +
                    "(3, '2024-06-10T02:00:00.000000Z')");
            execute("INSERT INTO t VALUES(4, '2024-06-11T00:00:00.000000Z')");
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET WHERE ts < '2024-06-11'");

            final FilesFacade ff = configuration.getFilesFacade();
            final TableToken token = engine.verifyTableName("t");

            long partitionTs;
            long partitionNameTxn;
            try (TableReader reader = engine.getReader(token)) {
                partitionTs = reader.getTxFile().getPartitionTimestampByIndex(0);
                partitionNameTxn = reader.getTxFile().getPartitionNameTxn(0);
            }

            // The header feature-flags word is at offset 8 (HEADER_FEATURE_FLAGS_OFF
            // in ParquetMetaFileReader); BLOOM_FILTERS is bit 0.
            final int headerFeatureFlagsOff = 8;
            final long bloomFiltersBit = 1L;

            // Sanity: the write-path _pm produced by CONVERT PARTITION already
            // carries the BLOOM_FILTERS bit. If not, the rest of the test is
            // meaningless because the parquet file has no bloom filter to inline.
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(token);
                TableUtils.setPathForParquetPartitionMetadata(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTs, partitionNameTxn);
                long writePmSize = ParquetMetaFileReader.readParquetMetaFileSize(ff, path.$());
                long writePmAddr = TableUtils.mapRO(ff, path.$(), LOG, writePmSize, MemoryTag.MMAP_DEFAULT);
                try {
                    long writeFlags = Unsafe.getLong(writePmAddr + headerFeatureFlagsOff);
                    Assert.assertTrue(
                            "write-path _pm should already declare BLOOM_FILTERS; flags=0x"
                                    + Long.toHexString(writeFlags),
                            (writeFlags & bloomFiltersBit) != 0
                    );
                } finally {
                    ff.munmap(writePmAddr, writePmSize, MemoryTag.MMAP_DEFAULT);
                }
            }

            // Delete the write-path _pm so the migration path has to regenerate it.
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(token);
                TableUtils.setPathForParquetPartitionMetadata(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTs, partitionNameTxn);
                ff.remove(path.$());
                Assert.assertFalse("_pm should be deleted", ff.exists(path.$()));
            }

            runMig941(token);
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(token);
                TableUtils.setPathForParquetPartitionMetadata(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTs, partitionNameTxn);
                Assert.assertTrue("_pm should exist after migration", ff.exists(path.$()));

                long parquetMetaSize = ParquetMetaFileReader.readParquetMetaFileSize(ff, path.$());
                Assert.assertTrue("_pm should have positive size", parquetMetaSize > 0);

                long parquetMetaAddr = TableUtils.mapRO(ff, path.$(), LOG, parquetMetaSize, MemoryTag.MMAP_DEFAULT);
                try {
                    long featureFlags = Unsafe.getLong(parquetMetaAddr + headerFeatureFlagsOff);
                    Assert.assertTrue(
                            "_pm header should declare BLOOM_FILTERS after migration; flags=0x"
                                    + Long.toHexString(featureFlags),
                            (featureFlags & bloomFiltersBit) != 0
                    );
                } finally {
                    ff.munmap(parquetMetaAddr, parquetMetaSize, MemoryTag.MMAP_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testMigrateMixedNativeAndParquetPartitions() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            // Three day partitions; WHERE ts > 0 converts every inactive
            // partition to parquet, leaving the active (last) partition
            // native. Mig941 must regenerate _pm on the parquet partitions
            // and leave the native one alone.
            execute("CREATE TABLE t (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES(1, '2024-06-10T00:00:00.000000Z')," +
                    "(2, '2024-06-11T00:00:00.000000Z')," +
                    "(3, '2024-06-12T00:00:00.000000Z')");
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET WHERE ts > 0");

            final FilesFacade ff = configuration.getFilesFacade();
            final TableToken token = engine.verifyTableName("t");

            long[] partitionTimestamps = new long[3];
            long[] partitionNameTxns = new long[3];
            boolean[] isParquet = new boolean[3];
            try (TableReader reader = engine.getReader(token)) {
                Assert.assertEquals(3, reader.getPartitionCount());
                for (int i = 0; i < 3; i++) {
                    partitionTimestamps[i] = reader.getTxFile().getPartitionTimestampByIndex(i);
                    partitionNameTxns[i] = reader.getTxFile().getPartitionNameTxn(i);
                    isParquet[i] = reader.getTxFile().isPartitionParquet(i);
                }
            }
            Assert.assertTrue("first partition should be parquet", isParquet[0]);
            Assert.assertTrue("second partition should be parquet", isParquet[1]);
            Assert.assertFalse("third (active) partition must be native", isParquet[2]);

            // Delete _pm from both parquet partitions.
            for (int i = 0; i < 2; i++) {
                try (Path path = new Path()) {
                    path.of(configuration.getDbRoot()).concat(token);
                    TableUtils.setPathForParquetPartitionMetadata(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTimestamps[i], partitionNameTxns[i]);
                    ff.remove(path.$());
                    Assert.assertFalse("_pm should be deleted", ff.exists(path.$()));
                }
            }

            runMig941(token);

            // Both parquet partitions have _pm regenerated and readable.
            for (int i = 0; i < 2; i++) {
                try (Path path = new Path()) {
                    path.of(configuration.getDbRoot()).concat(token);
                    TableUtils.setPathForParquetPartitionMetadata(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTimestamps[i], partitionNameTxns[i]);
                    Assert.assertTrue("_pm should exist after migration (parquet partition " + i + ")", ff.exists(path.$()));

                    long parquetMetaSize = ParquetMetaFileReader.readParquetMetaFileSize(ff, path.$());
                    Assert.assertTrue("_pm should have positive size", parquetMetaSize > 0);

                    long parquetMetaAddr = TableUtils.mapRO(ff, path.$(), LOG, parquetMetaSize, MemoryTag.MMAP_DEFAULT);
                    try {
                        ParquetMetaFileReader parquetMetaReader = new ParquetMetaFileReader();
                        parquetMetaReader.of(parquetMetaAddr, parquetMetaSize);
                        parquetMetaReader.resolveLastFooter();
                        Assert.assertEquals(2, parquetMetaReader.getColumnCount());
                        Assert.assertEquals(1, parquetMetaReader.getRowGroupCount());
                    } finally {
                        ff.munmap(parquetMetaAddr, parquetMetaSize, MemoryTag.MMAP_DEFAULT);
                    }
                }
            }

            // Native partition must have no _pm file.
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(token);
                TableUtils.setPathForParquetPartitionMetadata(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTimestamps[2], partitionNameTxns[2]);
                Assert.assertFalse("native partition must have no _pm", ff.exists(path.$()));
            }
        });
    }

    @Test
    public void testMigrateRegeneratesStalePm() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute("CREATE TABLE t (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES(1, '2024-06-10T00:00:00.000000Z')");
            execute("INSERT INTO t VALUES(2, '2024-06-11T00:00:00.000000Z')");
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET WHERE ts > 0");

            final FilesFacade ff = configuration.getFilesFacade();
            final TableToken token = engine.verifyTableName("t");

            long partitionTs;
            long partitionNameTxn;
            try (TableReader reader = engine.getReader(token)) {
                partitionTs = reader.getTxFile().getPartitionTimestampByIndex(0);
                partitionNameTxn = reader.getTxFile().getPartitionNameTxn(0);
            }

            // Corrupt _pm by truncating to 0 (simulates stale metadata).
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(token);
                TableUtils.setPathForParquetPartitionMetadata(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTs, partitionNameTxn);
                long fd = ff.openRW(path.$(), 0);
                try {
                    Assert.assertTrue("truncate should succeed", ff.truncate(fd, 0));
                } finally {
                    ff.close(fd);
                }
            }

            // Run migration — should detect staleness and regenerate.
            runMig941(token);

            // Verify _pm was regenerated and is valid.
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(token);
                TableUtils.setPathForParquetPartitionMetadata(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTs, partitionNameTxn);
                Assert.assertTrue("_pm should exist after migration", ff.exists(path.$()));

                long parquetMetaSize = ParquetMetaFileReader.readParquetMetaFileSize(ff, path.$());
                Assert.assertTrue("_pm should have positive size", parquetMetaSize > 0);

                long parquetMetaAddr = TableUtils.mapRO(ff, path.$(), LOG, parquetMetaSize, MemoryTag.MMAP_DEFAULT);
                try {
                    ParquetMetaFileReader reader = new ParquetMetaFileReader();
                    reader.of(parquetMetaAddr, parquetMetaSize);
                    reader.resolveLastFooter();
                    Assert.assertEquals(2, reader.getColumnCount());
                    Assert.assertEquals(1, reader.getRowGroupCount());
                } finally {
                    ff.munmap(parquetMetaAddr, parquetMetaSize, MemoryTag.MMAP_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testMigrateRepeatableViaEscapeHatch() throws Exception {
        // cairo.repeat.migration.from.version forces EngineMigration to
        // re-invoke Mig941 on an already-upgraded database. The check at
        // EngineMigration.java:110 resets the recorded migration version
        // back to the table version so the standard dispatcher runs it
        // again. This test drives the full dispatcher path (not the direct
        // Mig941.migrate() call that runMig941() uses) to prove the
        // documented recovery flow.
        node1.setProperty(PropertyKey.CAIRO_REPEAT_MIGRATION_FROM_VERSION, ColumnType.VERSION);

        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute("CREATE TABLE t (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES(1, '2024-06-10T00:00:00.000000Z')");
            execute("INSERT INTO t VALUES(2, '2024-06-11T00:00:00.000000Z')");
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET WHERE ts > 0");

            final FilesFacade ff = configuration.getFilesFacade();
            final TableToken token = engine.verifyTableName("t");

            long partitionTs;
            long partitionNameTxn;
            try (TableReader reader = engine.getReader(token)) {
                partitionTs = reader.getTxFile().getPartitionTimestampByIndex(0);
                partitionNameTxn = reader.getTxFile().getPartitionNameTxn(0);
            }

            // Delete _pm so we can observe the dispatcher actually invoking Mig941.
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(token);
                TableUtils.setPathForParquetPartitionMetadata(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTs, partitionNameTxn);
                ff.remove(path.$());
                Assert.assertFalse("_pm should be deleted", ff.exists(path.$()));
            }

            // Release handles so the migration can reopen files.
            engine.releaseAllWriters();
            engine.releaseAllReaders();
            engine.releaseInactive();

            // Drive the dispatcher end-to-end. The escape hatch forces Mig941
            // to re-run even though the database is already at MIGRATION_VERSION.
            EngineMigration.migrateEngineTo(engine, ColumnType.VERSION, ColumnType.MIGRATION_VERSION, true);

            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(token);
                TableUtils.setPathForParquetPartitionMetadata(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTs, partitionNameTxn);
                Assert.assertTrue("_pm should be regenerated by the dispatcher", ff.exists(path.$()));

                long parquetMetaSize = ParquetMetaFileReader.readParquetMetaFileSize(ff, path.$());
                Assert.assertTrue("_pm should have positive size", parquetMetaSize > 0);

                long parquetMetaAddr = TableUtils.mapRO(ff, path.$(), LOG, parquetMetaSize, MemoryTag.MMAP_DEFAULT);
                try {
                    ParquetMetaFileReader reader = new ParquetMetaFileReader();
                    reader.of(parquetMetaAddr, parquetMetaSize);
                    reader.resolveLastFooter();
                    Assert.assertEquals(2, reader.getColumnCount());
                    Assert.assertEquals(1, reader.getRowGroupCount());
                } finally {
                    ff.munmap(parquetMetaAddr, parquetMetaSize, MemoryTag.MMAP_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testMigrateRespectsTxnParquetFileSize() throws Exception {
        // Regression: Mig941 must regenerate _pm against the parquet file
        // size recorded in _txn field 3, NOT the actual on-disk file length.
        // ParquetMetaFileReader.java's class doc states this explicitly --
        // bytes past _txn field 3 may belong to an in-progress, unpublished
        // append and are not a valid commit boundary.
        //
        // Setup writes data.parquet, captures _txn field 3 (the committed
        // size) and the partition row count, then appends junk bytes to
        // data.parquet to simulate an unpublished partial write. _txn is
        // left untouched (still pointing at the original boundary). _pm is
        // deleted to force regeneration. The migration must read only the
        // first _txn-field-3 bytes of data.parquet and produce a _pm
        // describing the original row count.
        //
        // Negative case: a Mig941 that called ff.length() on data.parquet
        // would try to parse the trailing junk as a parquet footer and
        // either fail with a parse error or emit garbage metadata.
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute("CREATE TABLE t (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES(1, '2024-06-10T00:00:00.000000Z')");
            execute("INSERT INTO t VALUES(2, '2024-06-11T00:00:00.000000Z')");
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET WHERE ts > 0");

            final FilesFacade ff = configuration.getFilesFacade();
            final TableToken token = engine.verifyTableName("t");

            long partitionTs;
            long partitionNameTxn;
            long committedParquetFileSize;
            long committedRowCount;
            try (TableReader reader = engine.getReader(token)) {
                partitionTs = reader.getTxFile().getPartitionTimestampByIndex(0);
                partitionNameTxn = reader.getTxFile().getPartitionNameTxn(0);
                committedParquetFileSize = reader.getTxFile().getPartitionParquetFileSize(0);
                committedRowCount = reader.getTxFile().getPartitionSize(0);
            }
            Assert.assertTrue("committed parquet size must be positive", committedParquetFileSize > 0);
            Assert.assertTrue("committed row count must be positive", committedRowCount > 0);

            engine.releaseAllWriters();
            engine.releaseAllReaders();
            engine.releaseInactive();

            // Append junk bytes past the committed boundary in data.parquet.
            // _txn field 3 stays at committedParquetFileSize. ff.length now
            // returns committedParquetFileSize + JUNK_LEN.
            final int JUNK_LEN = 1024;
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(token);
                TableUtils.setPathForParquetPartition(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTs, partitionNameTxn);
                long fd = ff.openRW(path.$(), 0);
                try {
                    Assert.assertEquals(
                            "actual parquet file size must equal _txn field 3 before junk append",
                            committedParquetFileSize,
                            ff.length(fd)
                    );
                    long junkBuf = Unsafe.malloc(JUNK_LEN, MemoryTag.NATIVE_DEFAULT);
                    try {
                        for (long i = 0; i < JUNK_LEN; i += 8) {
                            Unsafe.putLong(junkBuf + i, 0xDEADBEEFDEADBEEFL);
                        }
                        Assert.assertEquals(
                                "junk bytes must be appended in full",
                                JUNK_LEN,
                                ff.write(fd, junkBuf, JUNK_LEN, committedParquetFileSize)
                        );
                    } finally {
                        Unsafe.free(junkBuf, JUNK_LEN, MemoryTag.NATIVE_DEFAULT);
                    }
                    Assert.assertEquals(
                            "ff.length must now exceed _txn field 3 by the junk length",
                            committedParquetFileSize + JUNK_LEN,
                            ff.length(fd)
                    );
                } finally {
                    ff.close(fd);
                }
            }

            // Drop _pm so the migration is forced to regenerate it.
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(token);
                TableUtils.setPathForParquetPartitionMetadata(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTs, partitionNameTxn);
                ff.remove(path.$());
                Assert.assertFalse("_pm must be deleted to force regeneration", ff.exists(path.$()));
            }

            runMig941(token);

            // Regenerated _pm must reflect the committed boundary: its
            // footer chain matches _txn field 3, and the row count equals
            // the original committed row count. The trailing junk bytes
            // are invisible.
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(token);
                TableUtils.setPathForParquetPartitionMetadata(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTs, partitionNameTxn);
                Assert.assertTrue("_pm exists after migration", ff.exists(path.$()));
                long pmSize = ParquetMetaFileReader.readParquetMetaFileSize(ff, path.$());
                long pmAddr = TableUtils.mapRO(ff, path.$(), LOG, pmSize, MemoryTag.MMAP_DEFAULT);
                try {
                    ParquetMetaFileReader pmReader = new ParquetMetaFileReader();
                    pmReader.of(pmAddr, pmSize);
                    Assert.assertTrue(
                            "regenerated _pm must resolve the committed parquet size from _txn",
                            pmReader.resolveFooter(committedParquetFileSize)
                    );
                    Assert.assertEquals(
                            "regenerated _pm row count must match the committed row count "
                                    + "(proves migration ignored junk bytes past _txn field 3)",
                            committedRowCount,
                            pmReader.getPartitionRowCount()
                    );
                } finally {
                    ff.munmap(pmAddr, pmSize, MemoryTag.MMAP_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testMigrateRollbackThenUpgradeViaEscapeHatch() throws Exception {
        // Rollback-then-upgrade scenario from the PR's "Rollback safety" note.
        // The on-disk shape after a rollback is:
        //   - data.parquet has been advanced (e.g. by an O3 merge under the
        //     older build that doesn't know about _pm).
        //   - _txn field 3 carries the new parquet file size.
        //   - _pm is left over from before the rollback, so its footer chain
        //     has no footer matching _txn field 3.
        //
        // The dispatcher's normal upgrade path will not re-run Mig941 on its
        // own because _upgrade.d already records MIGRATION_VERSION. The
        // documented recovery is cairo.repeat.migration.from.version, which
        // resets the recorded migration version (EngineMigration.java:110)
        // so the dispatcher proceeds and Mig941 regenerates _pm.
        //
        // This test stages the rollback shape by performing an O3 merge
        // under the new build (which advances data.parquet, _pm, and _txn
        // field 3 together) and then restoring _pm to its pre-merge bytes.
        // It then drives the production upgrade path with force=false and
        // asserts the regenerated _pm resolves to the post-merge parquet
        // file size.
        node1.setProperty(PropertyKey.CAIRO_REPEAT_MIGRATION_FROM_VERSION, ColumnType.VERSION);

        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute("CREATE TABLE t (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES(1, '2024-06-10T00:00:00.000000Z')");
            execute("INSERT INTO t VALUES(2, '2024-06-11T00:00:00.000000Z')");
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET WHERE ts > 0");

            final FilesFacade ff = configuration.getFilesFacade();
            final TableToken token = engine.verifyTableName("t");

            long partitionTs;
            long preMergeNameTxn;
            long preMergeParquetFileSize;
            try (TableReader reader = engine.getReader(token)) {
                partitionTs = reader.getTxFile().getPartitionTimestampByIndex(0);
                preMergeNameTxn = reader.getTxFile().getPartitionNameTxn(0);
                preMergeParquetFileSize = reader.getTxFile().getPartitionParquetFileSize(0);
            }

            // Capture the pre-merge _pm bytes. These are what an older build
            // would leave on disk after rolling back from this build.
            final long preMergePmSize;
            final long preMergePmAddr;
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(token);
                TableUtils.setPathForParquetPartitionMetadata(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTs, preMergeNameTxn);
                preMergePmSize = ff.length(path.$());
                Assert.assertTrue("pre-merge _pm exists with positive size", preMergePmSize > 0);
                preMergePmAddr = Unsafe.malloc(preMergePmSize, MemoryTag.NATIVE_DEFAULT);
                long mappedAddr = TableUtils.mapRO(ff, path.$(), LOG, preMergePmSize, MemoryTag.MMAP_DEFAULT);
                try {
                    Vect.memcpy(preMergePmAddr, mappedAddr, preMergePmSize);
                } finally {
                    ff.munmap(mappedAddr, preMergePmSize, MemoryTag.MMAP_DEFAULT);
                }
            }
            try {
                // O3 merge into the parquet partition: appends a new row group
                // to data.parquet, adds a fresh footer to _pm, and advances
                // _txn field 3 to the new parquet file size.
                execute("INSERT INTO t VALUES(99, '2024-06-10T01:00:00.000000Z')");

                long postMergeNameTxn;
                long postMergeParquetFileSize;
                try (TableReader reader = engine.getReader(token)) {
                    postMergeNameTxn = reader.getTxFile().getPartitionNameTxn(0);
                    postMergeParquetFileSize = reader.getTxFile().getPartitionParquetFileSize(0);
                }
                Assert.assertNotEquals(
                        "the O3 merge must change the parquet file size for the test to be meaningful",
                        preMergeParquetFileSize,
                        postMergeParquetFileSize
                );

                // Restore _pm to its pre-merge bytes. data.parquet and _txn
                // remain at the post-merge state, which is the rollback shape.
                try (Path path = new Path()) {
                    path.of(configuration.getDbRoot()).concat(token);
                    TableUtils.setPathForParquetPartitionMetadata(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTs, postMergeNameTxn);
                    long fd = ff.openRW(path.$(), 0);
                    try {
                        Assert.assertTrue("truncate _pm to 0", ff.truncate(fd, 0));
                        Assert.assertEquals(
                                "write captured pre-merge _pm bytes",
                                preMergePmSize,
                                ff.write(fd, preMergePmAddr, preMergePmSize, 0)
                        );
                    } finally {
                        ff.close(fd);
                    }
                }

                // Confirm the staged _pm is genuinely stale: its chain has no
                // footer matching the post-merge parquet file size.
                try (Path path = new Path()) {
                    path.of(configuration.getDbRoot()).concat(token);
                    TableUtils.setPathForParquetPartitionMetadata(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTs, postMergeNameTxn);
                    long size = ParquetMetaFileReader.readParquetMetaFileSize(ff, path.$());
                    long addr = TableUtils.mapRO(ff, path.$(), LOG, size, MemoryTag.MMAP_DEFAULT);
                    try {
                        ParquetMetaFileReader pmReader = new ParquetMetaFileReader();
                        pmReader.of(addr, size);
                        Assert.assertFalse(
                                "stale _pm must not resolve the post-merge parquet file size",
                                pmReader.resolveFooter(postMergeParquetFileSize)
                        );
                    } finally {
                        ff.munmap(addr, size, MemoryTag.MMAP_DEFAULT);
                    }
                }

                engine.releaseAllWriters();
                engine.releaseAllReaders();
                engine.releaseInactive();

                // Production upgrade path with force=false. The escape hatch
                // resets currentMigrationVersion to currentTableVersion so the
                // dispatcher proceeds despite _upgrade.d already recording
                // MIGRATION_VERSION.
                EngineMigration.migrateEngineTo(engine, ColumnType.VERSION, ColumnType.MIGRATION_VERSION, false);

                // _pm has been regenerated and now resolves to the post-merge
                // parquet file size.
                try (Path path = new Path()) {
                    path.of(configuration.getDbRoot()).concat(token);
                    TableUtils.setPathForParquetPartitionMetadata(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTs, postMergeNameTxn);
                    Assert.assertTrue("_pm exists after migration", ff.exists(path.$()));
                    long size = ParquetMetaFileReader.readParquetMetaFileSize(ff, path.$());
                    Assert.assertTrue("_pm has positive size", size > 0);
                    long addr = TableUtils.mapRO(ff, path.$(), LOG, size, MemoryTag.MMAP_DEFAULT);
                    try {
                        ParquetMetaFileReader pmReader = new ParquetMetaFileReader();
                        pmReader.of(addr, size);
                        Assert.assertTrue(
                                "regenerated _pm must resolve the current parquet file size",
                                pmReader.resolveFooter(postMergeParquetFileSize)
                        );
                        Assert.assertEquals(2, pmReader.getColumnCount());
                    } finally {
                        ff.munmap(addr, size, MemoryTag.MMAP_DEFAULT);
                    }
                }
            } finally {
                Unsafe.free(preMergePmAddr, preMergePmSize, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testMigrateRunsAfterMig702EraStamp() throws Exception {
        // EngineMigration.migrateEngineTo() short-circuits when _upgrade.d offset 4
        // equals MIGRATION_VERSION (EngineMigration.java:114). If Mig941's slot
        // collides with a slot that any prior, distinct migration once stamped
        // into _upgrade.d, the dispatcher will see "already at MIGRATION_VERSION"
        // on those databases and skip Mig941 — so their parquet partitions never
        // get _pm sidecars and become unreadable.
        //
        // This test stamps _upgrade.d offset 4 to slot 427 (a historical
        // migration stamp) and asserts Mig941 still runs and regenerates _pm.
        // Pinning this requires Mig941 to be registered at a slot strictly
        // greater than 427 with MIGRATION_VERSION bumped accordingly.
        final int MIG702_SLOT = 427;

        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute("CREATE TABLE t (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES(1, '2024-06-10T00:00:00.000000Z')");
            execute("INSERT INTO t VALUES(2, '2024-06-11T00:00:00.000000Z')");
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET WHERE ts > 0");

            final FilesFacade ff = configuration.getFilesFacade();
            final TableToken token = engine.verifyTableName("t");

            long partitionTs;
            long partitionNameTxn;
            try (TableReader reader = engine.getReader(token)) {
                partitionTs = reader.getTxFile().getPartitionTimestampByIndex(0);
                partitionNameTxn = reader.getTxFile().getPartitionNameTxn(0);
            }

            // Drop _pm to mimic a parquet partition produced by a build that
            // predates _pm sidecars.
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(token);
                TableUtils.setPathForParquetPartitionMetadata(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTs, partitionNameTxn);
                ff.remove(path.$());
                Assert.assertFalse("_pm should be deleted", ff.exists(path.$()));
            }

            engine.releaseAllWriters();
            engine.releaseAllReaders();
            engine.releaseInactive();

            // Engine setup wrote the new build's MIGRATION_VERSION into
            // _upgrade.d offset 4. Overwrite it with 427 to mimic a database
            // last touched by a build that stamped slot 427.
            try (Path upgradePath = new Path().of(configuration.getDbRoot()).concat(TableUtils.UPGRADE_FILE_NAME)) {
                long fd = TableUtils.openFileRWOrFail(ff, upgradePath.$(), configuration.getWriterFileOpenOpts());
                long tempMem = Unsafe.malloc(Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                try {
                    TableUtils.writeIntOrFail(ff, fd, 4, MIG702_SLOT, tempMem, upgradePath);
                } finally {
                    Unsafe.free(tempMem, Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                    ff.fsyncAndClose(fd);
                }
            }

            // Drive the production upgrade path (force=false). If Mig941's slot
            // collides with the stamp written above, the dispatcher returns early
            // and Mig941 never runs.
            EngineMigration.migrateEngineTo(engine, ColumnType.VERSION, ColumnType.MIGRATION_VERSION, false);

            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(token);
                TableUtils.setPathForParquetPartitionMetadata(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTs, partitionNameTxn);
                Assert.assertTrue(
                        "Mig941 must run when _upgrade.d carries a stamp from a prior, distinct migration; " +
                                "otherwise databases at that stamp keep their parquet partitions without _pm sidecars",
                        ff.exists(path.$())
                );

                long parquetMetaSize = ParquetMetaFileReader.readParquetMetaFileSize(ff, path.$());
                Assert.assertTrue("_pm should have positive size", parquetMetaSize > 0);

                long parquetMetaAddr = TableUtils.mapRO(ff, path.$(), LOG, parquetMetaSize, MemoryTag.MMAP_DEFAULT);
                try {
                    ParquetMetaFileReader reader = new ParquetMetaFileReader();
                    reader.of(parquetMetaAddr, parquetMetaSize);
                    reader.resolveLastFooter();
                    Assert.assertEquals(2, reader.getColumnCount());
                    Assert.assertEquals(1, reader.getRowGroupCount());
                } finally {
                    ff.munmap(parquetMetaAddr, parquetMetaSize, MemoryTag.MMAP_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testMigrateRegeneratesEquivalentPm() throws Exception {
        // Mig941 carries no staleness check: it regenerates every parquet partition's
        // _pm unconditionally. This pins that the regeneration is idempotent -- it
        // reproduces the write-path _pm (same size) and a second run leaves the file
        // unchanged -- so re-running the migration never damages a healthy _pm.
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute("CREATE TABLE t (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES(1, '2024-06-10T00:00:00.000000Z')");
            execute("INSERT INTO t VALUES(2, '2024-06-11T00:00:00.000000Z')");
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET WHERE ts > 0");

            final FilesFacade ff = configuration.getFilesFacade();
            final TableToken token = engine.verifyTableName("t");

            long partitionTs;
            long partitionNameTxn;
            try (TableReader reader = engine.getReader(token)) {
                partitionTs = reader.getTxFile().getPartitionTimestampByIndex(0);
                partitionNameTxn = reader.getTxFile().getPartitionNameTxn(0);
            }

            // Size of the write-path _pm produced by CONVERT PARTITION.
            long writePathPmSize;
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(token);
                TableUtils.setPathForParquetPartitionMetadata(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTs, partitionNameTxn);
                writePathPmSize = ff.length(path.$());
                Assert.assertTrue("_pm should exist", writePathPmSize > 0);
            }

            // First migration run regenerates the _pm to a file equivalent to the
            // write-path output.
            runMig941(token);
            long firstRunSize;
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(token);
                TableUtils.setPathForParquetPartitionMetadata(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTs, partitionNameTxn);
                firstRunSize = ff.length(path.$());
                Assert.assertEquals("regenerated _pm should match the write-path _pm size", writePathPmSize, firstRunSize);
            }

            // Second run is idempotent: regenerating an unchanged partition yields
            // the same _pm.
            runMig941(token);
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(token);
                TableUtils.setPathForParquetPartitionMetadata(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTs, partitionNameTxn);
                Assert.assertEquals("re-running the migration must not change the _pm", firstRunSize, ff.length(path.$()));
            }
        });
    }

    @Test
    public void testMigrateRemoteGeneratedPartitionMasksParquetFileSize() throws Exception {
        // A partition that is both REMOTE and parquet_generated keeps a local
        // data.parquet, so the migration regenerates its _pm — but offset 3
        // carries the REMOTE marker in bit 63, so the raw (negative) word must
        // be masked before it is used as the parquet file size.
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute("CREATE TABLE t (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES(1, '2024-06-10T00:00:00.000000Z')," +
                    "(2, '2024-06-11T00:00:00.000000Z')");
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '2024-06-10'");

            final FilesFacade ff = configuration.getFilesFacade();
            final TableToken token = engine.verifyTableName("t");

            long partitionTs;
            long partitionNameTxn;
            try (TableReader reader = engine.getReader(token)) {
                Assert.assertTrue("first partition must be parquet", reader.getTxFile().isPartitionParquet(0));
                partitionTs = reader.getTxFile().getPartitionTimestampByIndex(0);
                partitionNameTxn = reader.getTxFile().getPartitionNameTxn(0);
            }

            engine.releaseAllWriters();
            engine.releaseAllReaders();
            engine.releaseInactive();

            // Stamp REMOTE while keeping parquet_generated: bit 63 of offset 3 is now
            // set, so the raw word is negative while data.parquet stays on disk.
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(token).concat(TableUtils.TXN_FILE_NAME);
                try (TxWriter txWriter = new TxWriter(ff, configuration).ofRW(path.$(), ColumnType.TIMESTAMP, PartitionBy.DAY)) {
                    txWriter.setPartitionParquetGenerated(partitionTs, true);
                    txWriter.setPartitionRemoteByTimestamp(partitionTs, true);
                    txWriter.commit(new ObjList<>());
                }
            }

            long dataParquetSize;
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(token).concat(TableUtils.TXN_FILE_NAME);
                try (TxReader txReader = new TxReader(ff)) {
                    txReader.ofRO(path.$(), ColumnType.TIMESTAMP, PartitionBy.DAY);
                    txReader.unsafeLoadAll();
                    Assert.assertTrue("partition 0 must be stamped remote", txReader.isPartitionRemote(0));
                    Assert.assertTrue("partition 0 must keep parquet_generated", txReader.isPartitionParquetGenerated(0));
                }

                // The local data.parquet stays; its true length is the mask oracle.
                path.of(configuration.getDbRoot()).concat(token);
                TableUtils.setPathForParquetPartition(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTs, partitionNameTxn);
                dataParquetSize = ff.length(path.$());
                Assert.assertTrue("data.parquet must stay on disk", dataParquetSize > 0);

                // Delete _pm so the migration must regenerate it via the masked size.
                path.of(configuration.getDbRoot()).concat(token);
                TableUtils.setPathForParquetPartitionMetadata(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTs, partitionNameTxn);
                ff.remove(path.$());
                Assert.assertFalse("_pm should be deleted", ff.exists(path.$()));
            }

            runMig941(token);

            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(token);
                TableUtils.setPathForParquetPartitionMetadata(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTs, partitionNameTxn);
                Assert.assertTrue("_pm must be regenerated for the remote+generated partition", ff.exists(path.$()));

                long parquetMetaSize = ParquetMetaFileReader.readParquetMetaFileSize(ff, path.$());
                Assert.assertTrue("_pm should have positive size", parquetMetaSize > 0);

                long parquetMetaAddr = TableUtils.mapRO(ff, path.$(), LOG, parquetMetaSize, MemoryTag.MMAP_DEFAULT);
                try {
                    ParquetMetaFileReader reader = new ParquetMetaFileReader();
                    reader.of(parquetMetaAddr, parquetMetaSize);
                    // Freshly regenerated _pm: a single footer at the tail, no
                    // MVCC chain to match a committed size against.
                    Assert.assertTrue(reader.resolveLastFooter());
                    Assert.assertEquals(2, reader.getColumnCount());
                    Assert.assertEquals(1, reader.getRowGroupCount());
                    Assert.assertEquals(
                            "the regenerated _pm must record the masked (true) parquet file size",
                            dataParquetSize,
                            reader.getParquetFileSize()
                    );
                } finally {
                    ff.munmap(parquetMetaAddr, parquetMetaSize, MemoryTag.MMAP_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testMigrateSkipsRemotePartitionWithoutDataParquet() throws Exception {
        // A remotely served partition has had its data.parquet uploaded
        // and the local copy removed, leaving only _pm; its _txn entry stays
        // parquet-format with REMOTE set and parquet_generated cleared.
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute("CREATE TABLE t (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES(1, '2024-06-10T00:00:00.000000Z')," +
                    "(2, '2024-06-11T00:00:00.000000Z')," +
                    "(3, '2024-06-12T00:00:00.000000Z')");
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET WHERE ts > 0");

            final FilesFacade ff = configuration.getFilesFacade();
            final TableToken token = engine.verifyTableName("t");

            long remotePartitionTs;
            long remotePartitionNameTxn;
            long warmPartitionTs;
            long warmPartitionNameTxn;
            try (TableReader reader = engine.getReader(token)) {
                Assert.assertEquals(3, reader.getPartitionCount());
                Assert.assertTrue("first partition must be parquet", reader.getTxFile().isPartitionParquet(0));
                Assert.assertTrue("second partition must be parquet", reader.getTxFile().isPartitionParquet(1));
                Assert.assertFalse("third (active) partition must be native", reader.getTxFile().isPartitionParquet(2));
                remotePartitionTs = reader.getTxFile().getPartitionTimestampByIndex(0);
                remotePartitionNameTxn = reader.getTxFile().getPartitionNameTxn(0);
                warmPartitionTs = reader.getTxFile().getPartitionTimestampByIndex(1);
                warmPartitionNameTxn = reader.getTxFile().getPartitionNameTxn(1);
            }

            engine.releaseAllWriters();
            engine.releaseAllReaders();
            engine.releaseInactive();

            // mark the partition as remotely served
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(token).concat(TableUtils.TXN_FILE_NAME);
                try (TxWriter txWriter = new TxWriter(ff, configuration).ofRW(path.$(), ColumnType.TIMESTAMP, PartitionBy.DAY)) {
                    txWriter.setPartitionParquetGenerated(remotePartitionTs, false);
                    txWriter.setPartitionRemoteByTimestamp(remotePartitionTs, true);
                    txWriter.commit(new ObjList<>());
                }
            }
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(token).concat(TableUtils.TXN_FILE_NAME);
                try (TxReader txReader = new TxReader(ff)) {
                    txReader.ofRO(path.$(), ColumnType.TIMESTAMP, PartitionBy.DAY);
                    txReader.unsafeLoadAll();
                    Assert.assertTrue("partition 0 must be stamped remotely served", txReader.isPartitionRemotelyServed(0));
                    Assert.assertFalse("partition 1 must remain a normal parquet partition", txReader.isPartitionRemotelyServed(1));
                }
            }

            // The remote partition keeps only _pm; its data.parquet is gone.
            // Capture the _pm length to prove the skip leaves it untouched.
            long remotePmSize;
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(token);
                TableUtils.setPathForParquetPartition(path, ColumnType.TIMESTAMP, PartitionBy.DAY, remotePartitionTs, remotePartitionNameTxn);
                Assert.assertTrue("remote partition data.parquet should exist before deletion", ff.exists(path.$()));
                ff.remove(path.$());
                Assert.assertFalse("remote partition data.parquet must be deleted", ff.exists(path.$()));

                path.of(configuration.getDbRoot()).concat(token);
                TableUtils.setPathForParquetPartitionMetadata(path, ColumnType.TIMESTAMP, PartitionBy.DAY, remotePartitionTs, remotePartitionNameTxn);
                remotePmSize = ff.length(path.$());
                Assert.assertTrue("remote partition _pm should exist with positive size", remotePmSize > 0);
            }

            // Delete the warm partition's _pm so the migration must regenerate it.
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(token);
                TableUtils.setPathForParquetPartitionMetadata(path, ColumnType.TIMESTAMP, PartitionBy.DAY, warmPartitionTs, warmPartitionNameTxn);
                ff.remove(path.$());
                Assert.assertFalse("warm partition _pm should be deleted", ff.exists(path.$()));
            }

            // Must complete without throwing despite the remote partition's absent data.parquet.
            runMig941(token);

            // remote partition: skipped. data.parquet stays absent (the migration
            // never tried to read it) and its _pm is left exactly as it was.
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(token);
                TableUtils.setPathForParquetPartition(path, ColumnType.TIMESTAMP, PartitionBy.DAY, remotePartitionTs, remotePartitionNameTxn);
                Assert.assertFalse("migration must not recreate the remote partition's data.parquet", ff.exists(path.$()));

                path.of(configuration.getDbRoot()).concat(token);
                TableUtils.setPathForParquetPartitionMetadata(path, ColumnType.TIMESTAMP, PartitionBy.DAY, remotePartitionTs, remotePartitionNameTxn);
                Assert.assertTrue("remote partition _pm must survive the skip", ff.exists(path.$()));
                Assert.assertEquals("skipped remote partition _pm must be left untouched", remotePmSize, ff.length(path.$()));
            }

            // Warm partition: _pm regenerated and valid, proving the migration
            // ran past the skipped remote partition to completion.
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(token);
                TableUtils.setPathForParquetPartitionMetadata(path, ColumnType.TIMESTAMP, PartitionBy.DAY, warmPartitionTs, warmPartitionNameTxn);
                Assert.assertTrue("warm partition _pm must be regenerated", ff.exists(path.$()));

                long parquetMetaSize = ParquetMetaFileReader.readParquetMetaFileSize(ff, path.$());
                Assert.assertTrue("warm partition _pm should have positive size", parquetMetaSize > 0);

                long parquetMetaAddr = TableUtils.mapRO(ff, path.$(), LOG, parquetMetaSize, MemoryTag.MMAP_DEFAULT);
                try {
                    ParquetMetaFileReader reader = new ParquetMetaFileReader();
                    reader.of(parquetMetaAddr, parquetMetaSize);
                    // Freshly regenerated _pm: a single footer at the tail, no
                    // MVCC chain to match a committed size against.
                    Assert.assertTrue(reader.resolveLastFooter());
                    Assert.assertEquals(2, reader.getColumnCount());
                    Assert.assertEquals(1, reader.getRowGroupCount());
                } finally {
                    ff.munmap(parquetMetaAddr, parquetMetaSize, MemoryTag.MMAP_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testMigrateSkipsNonPartitionedTable() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute("CREATE TABLE t (id INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO t VALUES(1, '2024-06-10T00:00:00.000000Z')");

            final TableToken token = engine.verifyTableName("t");
            // Should return without error — non-partitioned tables are skipped.
            runMig941(token);
        });
    }

    @Test
    public void testMigrateSplitPartition() throws Exception {
        // Mig941 iterates partitions by index in _txn (Mig941.java:151) and
        // resolves directory paths via setPathForNativePartition(timestamp,
        // nameTxn). This test exercises the path-resolution surface for a
        // partition that originated as a split — i.e., its nameTxn is
        // non-zero. CONVERT to parquet may merge split sub-partitions into a
        // single physical directory, so the contract under test is "every
        // post-CONVERT parquet partition entry, no matter how its nameTxn
        // got bumped, gets a regenerated _pm". A regression that resolved
        // paths by floored timestamp alone would skip these.
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute("CREATE TABLE t (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES" +
                    "(1, '2024-06-10T00:00:00.000000Z')," +
                    "(2, '2024-06-10T01:00:00.000000Z')," +
                    "(3, '2024-06-10T02:00:00.000000Z')," +
                    "(4, '2024-06-10T03:00:00.000000Z')," +
                    // Trailing 2024-06-11 row makes 2024-06-11 the active
                    // partition so 2024-06-10 is inactive and convertible.
                    "(5, '2024-06-11T00:00:00.000000Z')");

            final FilesFacade ff = configuration.getFilesFacade();
            final TableToken token = engine.verifyTableName("t");

            // Holding a reader pins the current partition snapshot so the next
            // O3 insert lands as a split rather than getting squashed back
            // into the original directory.
            try (TableReader ignored = engine.getReader(token)) {
                execute("INSERT INTO t VALUES(99, '2024-06-10T02:30:00.000000Z')");
            }

            int splitPartitionCountBeforeConvert;
            try (TableReader reader = engine.getReader(token)) {
                splitPartitionCountBeforeConvert = reader.getPartitionCount();
            }
            // Confirm the split actually produced more than one physical entry
            // for 2024-06-10 — otherwise the test would not be exercising the
            // gap's pre-condition. Expect 3+ entries (2024-06-10 splits +
            // 2024-06-11 active).
            Assert.assertTrue(
                    "expected at least 3 partition entries after O3 split, got "
                            + splitPartitionCountBeforeConvert,
                    splitPartitionCountBeforeConvert >= 3
            );

            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET WHERE ts < '2024-06-11'");

            // Snapshot whatever the post-CONVERT layout looks like. CONVERT
            // may merge the split sub-partitions into a single parquet
            // directory or preserve them; either way Mig941 must regenerate a
            // _pm for every parquet entry. Capture nameTxns — at least one
            // will be non-zero because CONVERT bumps it.
            int partitionCount;
            long[] partitionTimestamps;
            long[] partitionNameTxns;
            boolean[] isParquetEntry;
            try (TableReader reader = engine.getReader(token)) {
                partitionCount = reader.getPartitionCount();
                partitionTimestamps = new long[partitionCount];
                partitionNameTxns = new long[partitionCount];
                isParquetEntry = new boolean[partitionCount];
                for (int i = 0; i < partitionCount; i++) {
                    partitionTimestamps[i] = reader.getTxFile().getPartitionTimestampByIndex(i);
                    partitionNameTxns[i] = reader.getTxFile().getPartitionNameTxn(i);
                    isParquetEntry[i] = reader.getTxFile().isPartitionParquet(i);
                }
            }
            int parquetCount = 0;
            boolean anyNonZeroNameTxn = false;
            for (int i = 0; i < partitionCount; i++) {
                if (isParquetEntry[i]) {
                    parquetCount++;
                    if (partitionNameTxns[i] != 0) {
                        anyNonZeroNameTxn = true;
                    }
                }
            }
            Assert.assertTrue("expected at least one parquet partition", parquetCount >= 1);
            Assert.assertTrue(
                    "expected at least one parquet partition with non-zero nameTxn (CONVERT bumps it)",
                    anyNonZeroNameTxn
            );

            // Delete every parquet sub-partition's _pm so the migration has
            // to regenerate them all.
            for (int i = 0; i < partitionCount; i++) {
                if (!isParquetEntry[i]) {
                    continue;
                }
                try (Path path = new Path()) {
                    path.of(configuration.getDbRoot()).concat(token);
                    TableUtils.setPathForParquetPartitionMetadata(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTimestamps[i], partitionNameTxns[i]);
                    Assert.assertTrue("_pm should exist before deletion: " + path, ff.exists(path.$()));
                    ff.remove(path.$());
                    Assert.assertFalse("_pm should be deleted: " + path, ff.exists(path.$()));
                }
            }

            runMig941(token);

            // Every parquet sub-directory now has a regenerated, valid _pm.
            for (int i = 0; i < partitionCount; i++) {
                if (!isParquetEntry[i]) {
                    continue;
                }
                try (Path path = new Path()) {
                    path.of(configuration.getDbRoot()).concat(token);
                    TableUtils.setPathForParquetPartitionMetadata(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTimestamps[i], partitionNameTxns[i]);
                    Assert.assertTrue("_pm regenerated for sub-partition " + i + " at " + path, ff.exists(path.$()));

                    long pmSize = ParquetMetaFileReader.readParquetMetaFileSize(ff, path.$());
                    Assert.assertTrue("regenerated _pm size > 0", pmSize > 0);
                    long pmAddr = TableUtils.mapRO(ff, path.$(), LOG, pmSize, MemoryTag.MMAP_DEFAULT);
                    try {
                        ParquetMetaFileReader r = new ParquetMetaFileReader();
                        r.of(pmAddr, pmSize);
                        Assert.assertTrue("resolveFooter on regenerated _pm " + i, r.resolveLastFooter());
                        Assert.assertEquals(2, r.getColumnCount());
                        Assert.assertTrue("at least one row group", r.getRowGroupCount() >= 1);
                    } finally {
                        ff.munmap(pmAddr, pmSize, MemoryTag.MMAP_DEFAULT);
                    }
                }
            }
        });
    }

    private void patchTxnParquetFileSize(TableToken token, long partitionTs, long newParquetFileSize) {
        final FilesFacade ff = configuration.getFilesFacade();
        try (Path path = new Path()) {
            path.of(configuration.getDbRoot()).concat(token).concat(TableUtils.TXN_FILE_NAME);
            try (TxWriter txWriter = new TxWriter(ff, configuration).ofRW(path.$(), ColumnType.TIMESTAMP, PartitionBy.DAY)) {
                txWriter.setPartitionParquet(partitionTs, newParquetFileSize);
                txWriter.commit(new ObjList<>());
            }
        }
    }

    @Test
    public void testMig941RegeneratesPmFromConsistentO3MergedFooter() throws Exception {
        // End-to-end guard that a real O3 merge leaves a footer Mig941 can
        // regenerate _pm from without crashing. A replica restoring a backup has
        // no _pm sidecar, so bootstrap runs Mig941, which reads the on-disk
        // footer through extract_sorting_columns.
        //
        // Before the fix an O3 merge in rewrite mode copied the unchanged row
        // groups without their sorting columns while writing the touched ones
        // with the timestamp sort column, so the footer declared sorting columns
        // on some row groups and none on others -- and extract_sorting_columns
        // aborted with "sorting columns differ between row groups", crashing the
        // replica. Fix 2 now stamps every row group (copied and fresh) with the
        // same dense sort column, so the footer this merge produces is internally
        // consistent and Mig941 reads it cleanly.
        //
        // This asserts that consistent end state plus the dense sort index
        // (Fix 3). It does NOT exercise extract_sorting_columns' tolerance of a
        // genuinely mixed or conflicting footer, which can only originate from a
        // pre-fix binary: Fix 2 makes the post-fix merge consistent, so the
        // tolerance path is unreachable here. That path is covered by the Rust
        // unit tests extract_sorting_columns_tolerates_groups_without_sorting and
        // convert_from_parquet_tolerates_conflicting_sort_indices_via_qdb_meta.
        //
        // A small row group size yields more than one row group per partition.
        // An ADD COLUMN before the O3 insert makes the partition schema differ
        // from the on-disk parquet, which forces O3PartitionJob down the rewrite
        // path (hasSchemaChange, O3PartitionJob.java:210) rather than an in-place
        // append: it copies the untouched row group through
        // copy_row_group_with_null_columns -- the site that used to drop the
        // sorting columns -- and writes the touched one fresh.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);

        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            // WAL table: an intra-partition O3 merge keeps the partition in
            // parquet format (a non-WAL table would fall back to native).
            execute("CREATE TABLE t (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            // Eight in-order rows at row group size 4 -> two row groups, both
            // declaring the timestamp sort column once converted.
            execute(
                    """
                            INSERT INTO t VALUES
                            (1, '2024-06-10T00:00:00.000000Z'),
                            (2, '2024-06-10T01:00:00.000000Z'),
                            (3, '2024-06-10T02:00:00.000000Z'),
                            (4, '2024-06-10T03:00:00.000000Z'),
                            (5, '2024-06-10T04:00:00.000000Z'),
                            (6, '2024-06-10T05:00:00.000000Z'),
                            (7, '2024-06-10T06:00:00.000000Z'),
                            (8, '2024-06-10T07:00:00.000000Z')
                            """
            );
            drainWalQueue();
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET WHERE ts > 0");
            drainWalQueue();

            // ADD COLUMN makes the next O3 merge a schema-change rewrite: it
            // copies the untouched row group (backfilling the new column with
            // nulls) and writes the touched one fresh.
            execute("ALTER TABLE t ADD COLUMN v INT");
            drainWalQueue();

            // O3 insert into the first row group's range: the merge rewrites the
            // touched group and copies the untouched one. Pre-fix the copied
            // group lost its sorting columns; post-fix both groups declare the
            // same dense sort column.
            execute("INSERT INTO t VALUES(99, '2024-06-10T00:30:00.000000Z', 7)");
            drainWalQueue();

            final FilesFacade ff = configuration.getFilesFacade();
            final TableToken token = engine.verifyTableName("t");

            long partitionTs;
            long partitionNameTxn;
            try (TableReader reader = engine.getReader(token)) {
                partitionTs = reader.getTxFile().getPartitionTimestampByIndex(0);
                partitionNameTxn = reader.getTxFile().getPartitionNameTxn(0);
            }

            // Remove _pm so Mig941 regenerates it from the on-disk footer, the
            // way a replica bootstrapping from a sidecar-less backup does.
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(token);
                TableUtils.setPathForParquetPartitionMetadata(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTs, partitionNameTxn);
                Assert.assertTrue("_pm should exist before removal", ff.exists(path.$()));
                Assert.assertTrue("remove _pm sidecar", ff.removeQuiet(path.$()));
            }

            // The regression: pre-fix the inconsistent footer aborted here; the
            // consistent footer the post-fix merge produces must migrate cleanly.
            runMig941(token);

            // _pm regenerated from the real merged footer, recording a single
            // sort column at the dense designated-timestamp position. The schema
            // is [id, ts, v], so the timestamp sits at dense index 1.
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(token);
                TableUtils.setPathForParquetPartitionMetadata(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTs, partitionNameTxn);
                Assert.assertTrue("_pm should exist after migration", ff.exists(path.$()));

                long parquetMetaSize = ParquetMetaFileReader.readParquetMetaFileSize(ff, path.$());
                Assert.assertTrue("_pm should have positive size", parquetMetaSize > 0);

                long parquetMetaAddr = TableUtils.mapRO(ff, path.$(), LOG, parquetMetaSize, MemoryTag.MMAP_DEFAULT);
                try {
                    ParquetMetaFileReader reader = new ParquetMetaFileReader();
                    reader.of(parquetMetaAddr, parquetMetaSize);
                    reader.resolveLastFooter();
                    Assert.assertEquals(3, reader.getColumnCount());
                    // The consistency guarantee is only meaningfully exercised
                    // when copied and fresh row groups coexist, so guard that the
                    // merge actually left more than one row group.
                    Assert.assertTrue(
                            "expected the O3 merge to leave more than one row group",
                            reader.getRowGroupCount() > 1
                    );
                    Assert.assertEquals(1, reader.getSortingColumnCount());
                    Assert.assertEquals(1, reader.getSortingColumnIndex(0));
                    Assert.assertEquals(1, reader.getDesignatedTimestampColumnIndex());
                } finally {
                    ff.munmap(parquetMetaAddr, parquetMetaSize, MemoryTag.MMAP_DEFAULT);
                }
            }
        });
    }

    private void runMig941(TableToken token) {
        engine.releaseAllWriters();
        engine.releaseAllReaders();
        engine.releaseInactive();
        long tempMem = Unsafe.malloc(1024, MemoryTag.NATIVE_MIG_MMAP);
        try (
                MemoryMARW rwMem = Vm.getCMARWInstance();
                MemoryARW tempVirtualMem = Vm.getCARWInstance(ff.getPageSize(), Integer.MAX_VALUE, MemoryTag.NATIVE_MIG_MMAP);
                Path tablePath = new Path().of(configuration.getDbRoot()).concat(token).slash();
                Path tablePath2 = new Path().of(configuration.getDbRoot()).concat(token).slash()
        ) {
            MigrationContext ctx = new MigrationContext(engine, tempMem, 1024, tempVirtualMem, rwMem);
            ctx.of(tablePath, tablePath2, -1);
            Mig941.migrate(ctx);
        } finally {
            Unsafe.free(tempMem, 1024, MemoryTag.NATIVE_MIG_MMAP);
        }
    }
}
