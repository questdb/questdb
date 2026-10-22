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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.mig.EngineMigration;
import io.questdb.cairo.mig.Mig1003;
import io.questdb.cairo.mig.MigrationContext;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryARW;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.griffin.engine.table.ParquetRowGroupFilter;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.zip.CRC32;

public class Mig1003Test extends AbstractCairoTest {
    private static final String[] PREDICATES = {"NOT (d < 4901.0)", "NOT (f < 4901.0)"};

    @Test
    public void testEngineMigrationRecordsQuestDBWriterFromRecordedVersion() throws Exception {
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        assertMemoryLeak(() -> {
            createConvertedTable();
            final String parquetMetaPath = parquetMetaPath();
            clearWrittenByQuestDB(parquetMetaPath);
            writeMigrationVersion(ColumnType.MIGRATION_VERSION - 1);
            engine.clear();
            EngineMigration.migrateEngineTo(engine, ColumnType.VERSION, ColumnType.MIGRATION_VERSION, false);
            for (String predicate : PREDICATES) {
                Assert.assertEquals(predicate, 49, prunedCount(predicate, "count\n100\n"));
            }
        });
    }

    @Test
    public void testMigrationLeavesAnotherWritersPartitionUnrecorded() throws Exception {
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        assertMemoryLeak(() -> {
            createConvertedTable();
            execute("ALTER TABLE x DETACH PARTITION LIST '2024-01-01'");
            final java.nio.file.Path table = Paths.get(configuration.getDbRoot().toString(), engine.verifyTableName("x").getDirName());
            final java.nio.file.Path attach = table.resolve("2024-01-01" + configuration.getAttachPartitionSuffix());
            Files.move(table.resolve("2024-01-01.detached"), attach);
            Files.delete(attach.resolve(TableUtils.PARQUET_METADATA_FILE_NAME));
            TestUtils.rewriteParquetCreatedBy(attach.resolve(TableUtils.PARQUET_PARTITION_NAME).toString());
            execute("ALTER TABLE x ATTACH PARTITION LIST '2024-01-01'");
            // QuestDB rewrites row groups of the attached file but keeps its created_by.
            execute("INSERT INTO x VALUES (2.0, 2.0, '2024-01-01T00:00:00.000001Z')");

            final String parquetMetaPath = parquetMetaPath();
            final long size = Files.size(Paths.get(parquetMetaPath));
            runMig1003();
            Assert.assertEquals(size, Files.size(Paths.get(parquetMetaPath)));
            for (String predicate : PREDICATES) {
                Assert.assertEquals(predicate, 0, prunedCount(predicate, "count\n100\n"));
            }
        });
    }

    @Test
    public void testMigrationRecordsQuestDBWriterOnce() throws Exception {
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        assertMemoryLeak(() -> {
            createConvertedTable();
            for (String predicate : PREDICATES) {
                Assert.assertEquals(predicate, 49, prunedCount(predicate, "count\n100\n"));
            }
            final String parquetMetaPath = parquetMetaPath();
            clearWrittenByQuestDB(parquetMetaPath);
            for (String predicate : PREDICATES) {
                Assert.assertEquals(predicate, 0, prunedCount(predicate, "count\n100\n"));
            }

            final long size = Files.size(Paths.get(parquetMetaPath));
            runMig1003();
            final long markedSize = Files.size(Paths.get(parquetMetaPath));
            Assert.assertTrue(markedSize > size);
            for (String predicate : PREDICATES) {
                Assert.assertEquals(predicate, 49, prunedCount(predicate, "count\n100\n"));
            }
            runMig1003();
            Assert.assertEquals(markedSize, Files.size(Paths.get(parquetMetaPath)));
        });
    }

    // Clears the record of the writer in the only footer of a _pm, as a _pm written before the record existed
    // has it, and recomputes the footer CRC, which covers the file from offset 8.
    private static void clearWrittenByQuestDB(String parquetMetaPath) throws Exception {
        engine.releaseAllWriters();
        engine.releaseAllReaders();
        engine.releaseInactive();
        final java.nio.file.Path file = Paths.get(parquetMetaPath);
        final byte[] bytes = Files.readAllBytes(file);
        final ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        final int size = (int) buffer.getLong(0);
        final int footer = size - 4 - buffer.getInt(size - 4);
        Assert.assertEquals("single footer expected", 0, buffer.getLong(footer + 24));
        final long flags = buffer.getLong(footer + 32);
        Assert.assertNotEquals("writer recorded", 0, flags & (1L << 2));
        buffer.putLong(footer + 32, flags & ~(1L << 2));
        final CRC32 crc = new CRC32();
        crc.update(bytes, 8, size - 16);
        buffer.putInt(size - 8, (int) crc.getValue());
        Files.write(file, bytes);
    }

    private static void createConvertedTable() throws Exception {
        execute("""
                CREATE TABLE x AS (
                    SELECT x::DOUBLE d, x::FLOAT f, timestamp_sequence('2024-01-01', 10_000) ts
                    FROM long_sequence(5_000)
                ) TIMESTAMP(ts) PARTITION BY DAY
                """);
        execute("INSERT INTO x VALUES (1.0, 1.0, '2024-01-02')");
        execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-02'");
    }

    private static String parquetMetaPath() {
        final TableToken token = engine.verifyTableName("x");
        try (TableReader reader = engine.getReader(token); Path path = new Path()) {
            path.of(configuration.getDbRoot()).concat(token);
            TableUtils.setPathForParquetPartitionMetadata(
                    path,
                    ColumnType.TIMESTAMP,
                    PartitionBy.DAY,
                    reader.getTxFile().getPartitionTimestampByIndex(0),
                    reader.getTxFile().getPartitionNameTxn(0)
            );
            return path.toString();
        }
    }

    private static int prunedCount(String predicate, String expected) throws Exception {
        ParquetRowGroupFilter.resetRowGroupsSkipped();
        try (
                RecordCursorFactory factory = select("SELECT count() FROM x WHERE ts < '2024-01-02' AND " + predicate);
                RecordCursor cursor = factory.getCursor(sqlExecutionContext)
        ) {
            final StringSink sink = new StringSink();
            CursorPrinter.println(cursor, factory.getMetadata(), sink);
            TestUtils.assertEquals(expected, sink);
        }
        return ParquetRowGroupFilter.getRowGroupsSkipped();
    }

    private static void runMig1003() {
        final TableToken token = engine.verifyTableName("x");
        engine.releaseAllWriters();
        engine.releaseAllReaders();
        engine.releaseInactive();
        final long tempMem = Unsafe.malloc(1024, MemoryTag.NATIVE_MIG_MMAP);
        try (
                MemoryMARW rwMem = Vm.getCMARWInstance();
                MemoryARW tempVirtualMem = Vm.getCARWInstance(configuration.getFilesFacade().getPageSize(), Integer.MAX_VALUE, MemoryTag.NATIVE_MIG_MMAP);
                Path tablePath = new Path().of(configuration.getDbRoot()).concat(token).slash();
                Path tablePath2 = new Path().of(configuration.getDbRoot()).concat(token).slash()
        ) {
            final MigrationContext context = new MigrationContext(engine, tempMem, 1024, tempVirtualMem, rwMem);
            context.of(tablePath, tablePath2, -1);
            Mig1003.migrate(context);
        } finally {
            Unsafe.free(tempMem, 1024, MemoryTag.NATIVE_MIG_MMAP);
        }
    }

    private static void writeMigrationVersion(int migrationVersion) {
        final long mem = Unsafe.malloc(Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        try (Path path = new Path().of(configuration.getDbRoot()).concat(TableUtils.UPGRADE_FILE_NAME)) {
            final long fd = configuration.getFilesFacade().openRW(path.$(), configuration.getWriterFileOpenOpts());
            Assert.assertTrue(fd > -1);
            try {
                TableUtils.writeIntOrFail(configuration.getFilesFacade(), fd, 0, ColumnType.VERSION, mem, path);
                TableUtils.writeIntOrFail(configuration.getFilesFacade(), fd, Integer.BYTES, migrationVersion, mem, path);
            } finally {
                configuration.getFilesFacade().close(fd);
            }
        } finally {
            Unsafe.free(mem, Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        }
    }
}
