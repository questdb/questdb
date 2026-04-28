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
import io.questdb.cairo.mig.EngineMigration;
import io.questdb.cairo.mig.Mig940;
import io.questdb.cairo.mig.MigrationContext;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryARW;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
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

public class Mig940Test extends AbstractCairoTest {

    /// Committed parquet fixture: single designated-ts column, two row
    /// groups of 10 rows each, row values 0..19, no min/max stats on the ts
    /// column. Regenerate via
    /// `cargo test emit_mig940_ts_no_stats_fixture -- --ignored`.
    private static final String TS_NO_STATS_FIXTURE = "/mig940/ts_no_stats.parquet";

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
            // marking col 0 as designated ts but NO inline stats.
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(token);
                TableUtils.setPathForParquetPartition(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTs, partitionNameTxn);
                try (InputStream is = Mig940Test.class.getResourceAsStream(TS_NO_STATS_FIXTURE)) {
                    Assert.assertNotNull("fixture missing: " + TS_NO_STATS_FIXTURE, is);
                    Files.write(java.nio.file.Path.of(path.toString()), is.readAllBytes());
                }
                path.of(configuration.getDbRoot()).concat(token);
                TableUtils.setPathForParquetPartitionMetadata(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTs, partitionNameTxn);
                ff.remove(path.$());
            }

            runMig940(token);

            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(token);
                TableUtils.setPathForParquetPartitionMetadata(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTs, partitionNameTxn);
                long parquetMetaSize = ParquetMetaFileReader.readParquetMetaFileSize(ff, path.$());
                long parquetMetaAddr = TableUtils.mapRO(ff, path.$(), LOG, parquetMetaSize, MemoryTag.MMAP_DEFAULT);
                try {
                    ParquetMetaFileReader reader = new ParquetMetaFileReader();
                    reader.of(parquetMetaAddr, parquetMetaSize);
                    reader.resolveFooter(Long.MAX_VALUE);
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
            runMig940(token);

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
                    reader.resolveFooter(Long.MAX_VALUE);
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
    public void testMigrateMixedNativeAndParquetPartitions() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            // Three day partitions; WHERE ts > 0 converts every inactive
            // partition to parquet, leaving the active (last) partition
            // native. Mig940 must regenerate _pm on the parquet partitions
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

            runMig940(token);

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
                        parquetMetaReader.resolveFooter(Long.MAX_VALUE);
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
    public void testMigrateSkipsNonPartitionedTable() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute("CREATE TABLE t (id INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO t VALUES(1, '2024-06-10T00:00:00.000000Z')");

            final TableToken token = engine.verifyTableName("t");
            // Should return without error — non-partitioned tables are skipped.
            runMig940(token);
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
                runMig940(token);
                Assert.fail("Expected CairoException");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "parquet file not found");
            }
        });
    }

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
                runMig940(token);
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
                runMig940(token);
                Assert.fail("Expected CairoException");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "parquet file empty or unreadable");
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
            runMig940(token);

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
                    reader.resolveFooter(Long.MAX_VALUE);
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
        // re-invoke Mig940 on an already-upgraded database. The check at
        // EngineMigration.java:110 resets the recorded migration version
        // back to the table version so the standard dispatcher runs it
        // again. This test drives the full dispatcher path (not the direct
        // Mig940.migrate() call that runMig940() uses) to prove the
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

            // Delete _pm so we can observe the dispatcher actually invoking Mig940.
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

            // Drive the dispatcher end-to-end. The escape hatch forces Mig940
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
                    reader.resolveFooter(Long.MAX_VALUE);
                    Assert.assertEquals(2, reader.getColumnCount());
                    Assert.assertEquals(1, reader.getRowGroupCount());
                } finally {
                    ff.munmap(parquetMetaAddr, parquetMetaSize, MemoryTag.MMAP_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testMigrateRunsAfterMig702EraStamp() throws Exception {
        // EngineMigration.migrateEngineTo() short-circuits when _upgrade.d offset 4
        // equals MIGRATION_VERSION (EngineMigration.java:114). If Mig940's slot
        // collides with a slot that any prior, distinct migration once stamped
        // into _upgrade.d, the dispatcher will see "already at MIGRATION_VERSION"
        // on those databases and skip Mig940 — so their parquet partitions never
        // get _pm sidecars and become unreadable.
        //
        // This test stamps _upgrade.d offset 4 to slot 427 (a historical
        // migration stamp) and asserts Mig940 still runs and regenerates _pm.
        // Pinning this requires Mig940 to be registered at a slot strictly
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

            // Drive the production upgrade path (force=false). If Mig940's slot
            // collides with the stamp written above, the dispatcher returns early
            // and Mig940 never runs.
            EngineMigration.migrateEngineTo(engine, ColumnType.VERSION, ColumnType.MIGRATION_VERSION, false);

            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(token);
                TableUtils.setPathForParquetPartitionMetadata(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTs, partitionNameTxn);
                Assert.assertTrue(
                        "Mig940 must run when _upgrade.d carries a stamp from a prior, distinct migration; " +
                                "otherwise databases at that stamp keep their parquet partitions without _pm sidecars",
                        ff.exists(path.$())
                );

                long parquetMetaSize = ParquetMetaFileReader.readParquetMetaFileSize(ff, path.$());
                Assert.assertTrue("_pm should have positive size", parquetMetaSize > 0);

                long parquetMetaAddr = TableUtils.mapRO(ff, path.$(), LOG, parquetMetaSize, MemoryTag.MMAP_DEFAULT);
                try {
                    ParquetMetaFileReader reader = new ParquetMetaFileReader();
                    reader.of(parquetMetaAddr, parquetMetaSize);
                    reader.resolveFooter(Long.MAX_VALUE);
                    Assert.assertEquals(2, reader.getColumnCount());
                    Assert.assertEquals(1, reader.getRowGroupCount());
                } finally {
                    ff.munmap(parquetMetaAddr, parquetMetaSize, MemoryTag.MMAP_DEFAULT);
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
        // The dispatcher's normal upgrade path will not re-run Mig940 on its
        // own because _upgrade.d already records MIGRATION_VERSION. The
        // documented recovery is cairo.repeat.migration.from.version, which
        // resets the recorded migration version (EngineMigration.java:110)
        // so the dispatcher proceeds and Mig940 picks up the stale chain via
        // isParquetMetadataStale() and regenerates _pm.
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
    public void testMigrateSkipsHealthyPm() throws Exception {
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

            // Record _pm size before migration.
            long parquetMetaSizeBefore;
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(token);
                TableUtils.setPathForParquetPartitionMetadata(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTs, partitionNameTxn);
                parquetMetaSizeBefore = ff.length(path.$());
                Assert.assertTrue("_pm should exist", parquetMetaSizeBefore > 0);
            }

            // Run migration — should skip healthy _pm.
            runMig940(token);

            // Verify _pm size is unchanged.
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(token);
                TableUtils.setPathForParquetPartitionMetadata(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTs, partitionNameTxn);
                Assert.assertEquals("_pm should not be rewritten", parquetMetaSizeBefore, ff.length(path.$()));
            }
        });
    }

    @Test
    public void testMigrateSplitPartition() throws Exception {
        // Mig940 iterates partitions by index in _txn (Mig940.java:151) and
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
            // directory or preserve them; either way Mig940 must regenerate a
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

            runMig940(token);

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
                        Assert.assertTrue("resolveFooter on regenerated _pm " + i, r.resolveFooter(Long.MAX_VALUE));
                        Assert.assertEquals(2, r.getColumnCount());
                        Assert.assertTrue("at least one row group", r.getRowGroupCount() >= 1);
                    } finally {
                        ff.munmap(pmAddr, pmSize, MemoryTag.MMAP_DEFAULT);
                    }
                }
            }
        });
    }

    private void runMig940(TableToken token) {
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
            Mig940.migrate(ctx);
        } finally {
            Unsafe.free(tempMem, 1024, MemoryTag.NATIVE_MIG_MMAP);
        }
    }
}
