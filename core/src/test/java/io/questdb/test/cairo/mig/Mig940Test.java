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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ParquetMetaFileReader;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.mig.Mig940;
import io.questdb.cairo.mig.MigrationContext;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryARW;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class Mig940Test extends AbstractCairoTest {

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

                long pmSize = ff.length(path.$());
                Assert.assertTrue("_pm should have positive size", pmSize > 0);

                // Read and validate the generated _pm file.
                long pmAddr = TableUtils.mapRO(ff, path.$(), LOG, pmSize, MemoryTag.MMAP_DEFAULT);
                try {
                    ParquetMetaFileReader reader = new ParquetMetaFileReader();
                    reader.of(pmAddr, pmSize);
                    Assert.assertEquals(2, reader.getColumnCount());
                    Assert.assertEquals(1, reader.getRowGroupCount());
                    Assert.assertTrue(reader.getParquetFileSize() > 0);
                } finally {
                    ff.munmap(pmAddr, pmSize, MemoryTag.MMAP_DEFAULT);
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
            runMig940(token);
        });
    }

    @Test
    public void testMigrateAbortOnMissingParquetFile() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute("CREATE TABLE t (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES(1, '2024-06-10T00:00:00.000000Z')");
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
                    // Overwrite magic bytes with garbage.
                    long buf = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
                    try {
                        Unsafe.getUnsafe().putLong(buf, 0xDEADBEEFDEADBEEFL);
                        ff.write(fd, buf, 8, 0);
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
            } catch (Exception e) {
                // ParquetMetadataWriter.generate() throws on corrupt input.
            }
        });
    }

    @Test
    public void testMigrateAbortOnEmptyParquetFile() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute("CREATE TABLE t (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES(1, '2024-06-10T00:00:00.000000Z')");
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
