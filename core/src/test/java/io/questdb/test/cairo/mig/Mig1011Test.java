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
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.mig.EngineMigration;
import io.questdb.cairo.mig.Mig1011;
import io.questdb.cairo.mig.MigrationContext;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryARW;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.griffin.engine.table.ParquetRowGroupFilter;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.crash.CrashSimulationError;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.LogCapture;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.zip.CRC32;

public class Mig1011Test extends AbstractCairoTest {
    private static final int MIG1011_VERSION = 431;
    private static final String[] PARQUET_DAYS = {"2024-01-01", "2024-01-02", "2024-01-03"};
    private static final String[] PREDICATES = {"NOT (d < 4901.0)", "NOT (f < 4901.0)"};
    private static final LogCapture capture = new LogCapture();

    @Override
    @Before
    public void setUp() {
        LogFactory.enableGuaranteedLogging(EngineMigration.class, Mig1011.class);
        super.setUp();
        capture.start();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        try {
            capture.stop();
            super.tearDown();
        } finally {
            LogFactory.disableGuaranteedLogging(EngineMigration.class, Mig1011.class);
        }
    }

    @Test
    public void testEngineMigrationRecordsQuestDBWriterFromRecordedVersion() throws Exception {
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        assertMemoryLeak(() -> {
            createConvertedTable("x");
            final String parquetMetaPath = parquetMetaPath("x");
            clearWrittenByQuestDB(parquetMetaPath);
            writeMigrationVersion(MIG1011_VERSION - 1);
            engine.clear();
            EngineMigration.migrateEngineTo(engine, ColumnType.VERSION, ColumnType.MIGRATION_VERSION, false);
            for (String predicate : PREDICATES) {
                Assert.assertEquals(predicate, 49, prunedCount(predicate, "count\n100\n"));
            }
        });
    }

    @Test
    public void testEngineMigrationRecordsQuestDBWriterInCompositeTable() throws Exception {
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        assertMemoryLeak(() -> {
            createCompositeTableWithConvertedPartition();
            final String parquetMetaPath = parquetMetaPath("x");
            clearWrittenByQuestDB(parquetMetaPath);
            writeMigrationVersion(MIG1011_VERSION - 1);
            engine.clear();
            EngineMigration.migrateEngineTo(engine, ColumnType.VERSION, ColumnType.MIGRATION_VERSION, false);
            Assert.assertEquals(ColumnType.MAX_STORAGE_VERSION, metaStorageVersion("x"));
            Assert.assertTrue(isComposite("x", "2024-01-02"));
            for (String predicate : PREDICATES) {
                Assert.assertEquals(predicate, 49, prunedCount(predicate, "count\n100\n"));
            }

            // a crash before _upgrade.d records the migration reruns it
            final long markedSize = Files.size(Paths.get(parquetMetaPath));
            writeMigrationVersion(MIG1011_VERSION - 1);
            engine.clear();
            EngineMigration.migrateEngineTo(engine, ColumnType.VERSION, ColumnType.MIGRATION_VERSION, false);
            Assert.assertEquals(markedSize, Files.size(Paths.get(parquetMetaPath)));
            Assert.assertEquals(ColumnType.MAX_STORAGE_VERSION, metaStorageVersion("x"));
        });
    }

    @Test
    public void testEngineMigrationRecoversFromCrashAfterAppend() throws Exception {
        assertEngineMigrationRecoversFromCrash(CrashStage.AFTER_APPEND);
    }

    @Test
    public void testEngineMigrationRecoversFromCrashAfterPublication() throws Exception {
        assertEngineMigrationRecoversFromCrash(CrashStage.AFTER_PUBLICATION);
    }

    @Test
    public void testEngineMigrationRecoversFromCrashBeforeAppend() throws Exception {
        assertEngineMigrationRecoversFromCrash(CrashStage.BEFORE_APPEND);
    }

    @Test
    public void testEngineMigrationRunsNoOtherMigrationOnCompositeTable() throws Exception {
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        assertMemoryLeak(() -> {
            createCompositeTableWithConvertedPartition();
            createConvertedTable("y");
            final String compositeParquetMetaPath = parquetMetaPath("x");
            final String plainParquetMetaPath = parquetMetaPath("y");
            clearWrittenByQuestDB(compositeParquetMetaPath);
            clearWrittenByQuestDB(plainParquetMetaPath);
            engine.clear();
            EngineMigration.migrateEngineTo(engine, ColumnType.VERSION, ColumnType.MIGRATION_VERSION, true);
            // Mig941 regenerates a _pm as a single snapshot; Mig1011 appends one to the committed chain.
            Assert.assertEquals(0, previousSnapshotSize(plainParquetMetaPath));
            Assert.assertNotEquals(0, previousSnapshotSize(compositeParquetMetaPath));
            Assert.assertEquals(ColumnType.MAX_STORAGE_VERSION, metaStorageVersion("x"));
            Assert.assertTrue(isComposite("x", "2024-01-02"));
            for (String predicate : PREDICATES) {
                Assert.assertEquals(predicate, 49, prunedCount(predicate, "count\n100\n"));
            }
            final String skipped = "skipping storage version dependent migration on table with composite partitions \\[path=\\S*"
                    + engine.verifyTableName("x").getDirName() + "/?, toVersion=";
            capture.drain();
            capture.assertOnlyOnce(skipped + "429\\]");
            capture.assertOnlyOnce(skipped + "430\\]");
        });
    }

    @Test
    public void testEngineMigrationSkipsTableWithIncompleteMeta() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE healthy (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO healthy VALUES (42, '2024-01-01T00:00:00.000000Z')");
            final FilesFacade ff = configuration.getFilesFacade();
            try (Path path = new Path().of(configuration.getDbRoot()).concat("damaged")) {
                Assert.assertEquals(0, ff.mkdir(path.$(), configuration.getMkDirMode()));
                ff.close(TableUtils.openRW(ff, path.concat(TableUtils.META_FILE_NAME).$(), LOG, configuration.getWriterFileOpenOpts()));
            }
            writeMigrationVersion(MIG1011_VERSION - 1);
            engine.clear();
            EngineMigration.migrateEngineTo(engine, ColumnType.VERSION, ColumnType.MIGRATION_VERSION, false);
            capture.drain();
            capture.assertOnlyOnce("skipping table, _meta is shorter than its header \\[path=\\S+damaged\\S+_meta, size=0\\]");
            Assert.assertEquals(ColumnType.MIGRATION_VERSION, readMigrationVersion());
            assertQuery("SELECT v FROM healthy")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            v
                            42
                            """);
        });
    }

    @Test
    public void testEngineMigrationSkipsTableWithIncompleteTxn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE healthy (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO healthy VALUES (42, '2024-01-01T00:00:00.000000Z')");
            final FilesFacade ff = configuration.getFilesFacade();
            try (
                    Path meta = new Path().of(configuration.getDbRoot()).concat(engine.verifyTableName("healthy")).concat(TableUtils.META_FILE_NAME);
                    Path damaged = new Path().of(configuration.getDbRoot()).concat("damaged")
            ) {
                Assert.assertEquals(0, ff.mkdir(damaged.$(), configuration.getMkDirMode()));
                final int damagedLen = damaged.size();
                Assert.assertTrue(ff.copy(meta.$(), damaged.concat(TableUtils.META_FILE_NAME).$()) >= 0);
                ff.close(TableUtils.openRW(ff, damaged.trimTo(damagedLen).concat(TableUtils.TXN_FILE_NAME).$(), LOG, configuration.getWriterFileOpenOpts()));
            }
            for (int i = 0; i < 2; i++) {
                writeMigrationVersion(MIG1011_VERSION - 1);
                engine.clear();
                EngineMigration.migrateEngineTo(engine, ColumnType.VERSION, ColumnType.MIGRATION_VERSION, false);
                Assert.assertEquals(ColumnType.MIGRATION_VERSION, readMigrationVersion());
                assertQuery("SELECT v FROM healthy")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                v
                                42
                                """);
            }
            capture.drain();
            capture.assertLoggedRE("skipping table, _txn is shorter than its header \\[path=\\S+damaged\\S+_txn, size=0\\]");
        });
    }

    @Test
    public void testMigrationCountsUnreadableParquetFileAsFailed() throws Exception {
        final ParquetOpenFailingFilesFacade ff = new ParquetOpenFailingFilesFacade();
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        assertMemoryLeak(ff, () -> {
            createConvertedTable("x");
            final String parquetMetaPath = parquetMetaPath("x");
            clearWrittenByQuestDB(parquetMetaPath);
            final long size = Files.size(Paths.get(parquetMetaPath));
            ff.isParquetOpenFailing = true;
            try {
                runMig1011();
            } finally {
                ff.isParquetOpenFailing = false;
            }
            Assert.assertEquals(size, Files.size(Paths.get(parquetMetaPath)));
            capture.drain();
            capture.assertLoggedRE("recorded parquet writer in _pm files \\[path=\\S+, marked=0, unchanged=0, failed=1\\]");
            capture.assertLoggedRE("could not open parquet file \\[path=\\S+" + TableUtils.PARQUET_PARTITION_NAME + ", errno=");

            // the partition left unrecorded gains the record on a rerun
            runMig1011();
            for (String predicate : PREDICATES) {
                Assert.assertEquals(predicate, 49, prunedCount(predicate, "count\n100\n"));
            }
        });
    }

    @Test
    public void testMigrationLeavesAnotherWritersPartitionUnrecorded() throws Exception {
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        assertMemoryLeak(() -> {
            createConvertedTable("x");
            execute("ALTER TABLE x DETACH PARTITION LIST '2024-01-01'");
            final java.nio.file.Path table = Paths.get(configuration.getDbRoot().toString(), engine.verifyTableName("x").getDirName());
            final java.nio.file.Path attach = table.resolve("2024-01-01" + configuration.getAttachPartitionSuffix());
            Files.move(table.resolve("2024-01-01.detached"), attach);
            Files.delete(attach.resolve(TableUtils.PARQUET_METADATA_FILE_NAME));
            TestUtils.rewriteParquetCreatedBy(attach.resolve(TableUtils.PARQUET_PARTITION_NAME).toString());
            execute("ALTER TABLE x ATTACH PARTITION LIST '2024-01-01'");
            // QuestDB rewrites row groups of the attached file but keeps its created_by.
            execute("INSERT INTO x VALUES (2.0, 2.0, '2024-01-01T00:00:00.000001Z')");

            final String parquetMetaPath = parquetMetaPath("x");
            final long size = Files.size(Paths.get(parquetMetaPath));
            runMig1011();
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
            createConvertedTable("x");
            for (String predicate : PREDICATES) {
                Assert.assertEquals(predicate, 49, prunedCount(predicate, "count\n100\n"));
            }
            final String parquetMetaPath = parquetMetaPath("x");
            clearWrittenByQuestDB(parquetMetaPath);
            for (String predicate : PREDICATES) {
                Assert.assertEquals(predicate, 0, prunedCount(predicate, "count\n100\n"));
            }

            final long size = Files.size(Paths.get(parquetMetaPath));
            runMig1011();
            final long markedSize = Files.size(Paths.get(parquetMetaPath));
            Assert.assertTrue(markedSize > size);
            for (String predicate : PREDICATES) {
                Assert.assertEquals(predicate, 49, prunedCount(predicate, "count\n100\n"));
            }
            runMig1011();
            Assert.assertEquals(markedSize, Files.size(Paths.get(parquetMetaPath)));
        });
    }

    private void assertEngineMigrationRecoversFromCrash(CrashStage crashStage) throws Exception {
        final MigrationCrashingFilesFacade ff = new MigrationCrashingFilesFacade();
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        assertMemoryLeak(ff, () -> {
            execute("""
                    CREATE TABLE x AS (
                        SELECT
                            ((x - 1) % 5_000 + 1)::DOUBLE d,
                            ((x - 1) % 5_000 + 1)::FLOAT f,
                            timestamp_sequence('2024-01-01', 17_280_000) ts
                        FROM long_sequence(15_000)
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("INSERT INTO x VALUES (1.0, 1.0, '2024-01-04')");
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-04'");
            final ObjList<String> parquetMetaPaths = new ObjList<>();
            final ObjList<byte[]> clearedFiles = new ObjList<>();
            for (int i = 0; i < PARQUET_DAYS.length; i++) {
                final String parquetMetaPath = parquetMetaPath("x", i);
                clearWrittenByQuestDB(parquetMetaPath);
                parquetMetaPaths.add(parquetMetaPath);
                clearedFiles.add(Files.readAllBytes(Paths.get(parquetMetaPath)));
            }
            writeMigrationVersion(MIG1011_VERSION - 1);
            engine.clear();
            ff.crashStage = crashStage;
            try {
                EngineMigration.migrateEngineTo(engine, ColumnType.VERSION, ColumnType.MIGRATION_VERSION, false);
                Assert.fail("migration did not crash");
            } catch (CrashSimulationError e) {
                capture.drain();
                capture.assertOnlyOnce("failed to upgrade table \\[path=\\S+" + engine.verifyTableName("x").getDirName() + "/?, e=");
            } finally {
                ff.crashStage = null;
            }
            if (crashStage == CrashStage.AFTER_APPEND) {
                writeCommittedSize(parquetMetaPaths.getQuick(1), clearedFiles.getQuick(1).length);
            }
            Assert.assertEquals(MIG1011_VERSION - 1, readMigrationVersion());
            assertRowsOfEveryPartition();
            for (int i = 0; i < PARQUET_DAYS.length; i++) {
                for (String predicate : PREDICATES) {
                    Assert.assertEquals(
                            PARQUET_DAYS[i] + ' ' + predicate,
                            i < crashStage.recordedPartitionCount ? 49 : 0,
                            prunedCount("ts IN '" + PARQUET_DAYS[i] + "'", predicate, "count\n100\n")
                    );
                }
            }

            engine.clear();
            EngineMigration.migrateEngineTo(engine, ColumnType.VERSION, ColumnType.MIGRATION_VERSION, false);
            Assert.assertEquals(ColumnType.MIGRATION_VERSION, readMigrationVersion());
            capture.drain();
            capture.assertOnlyOnce("recorded parquet writer in _pm files \\[path=\\S+, marked="
                    + (PARQUET_DAYS.length - crashStage.recordedPartitionCount)
                    + ", unchanged=" + crashStage.recordedPartitionCount + ", failed=0\\]");
            for (int i = 0; i < PARQUET_DAYS.length; i++) {
                assertSingleRecordedFooter(parquetMetaPaths.getQuick(i), clearedFiles.getQuick(i));
            }
            assertRowsOfEveryPartition();
            for (String day : PARQUET_DAYS) {
                for (String predicate : PREDICATES) {
                    Assert.assertEquals(day + ' ' + predicate, 49, prunedCount("ts IN '" + day + "'", predicate, "count\n100\n"));
                }
            }
        });
    }

    private void assertRowsOfEveryPartition() throws Exception {
        assertQuery("SELECT ts, count(), avg(d), min(f), max(f) FROM x SAMPLE BY 1d")
                .noLeakCheck()
                .timestamp("ts")
                .expectSize()
                .returns("""
                        ts\tcount\tavg\tmin\tmax
                        2024-01-01T00:00:00.000000Z\t5000\t2500.5\t1.0\t5000.0
                        2024-01-02T00:00:00.000000Z\t5000\t2500.5\t1.0\t5000.0
                        2024-01-03T00:00:00.000000Z\t5000\t2500.5\t1.0\t5000.0
                        2024-01-04T00:00:00.000000Z\t1\t1.0\t1.0\t1.0
                        """);
    }

    private static void assertSingleRecordedFooter(String parquetMetaPath, byte[] snapshot) throws Exception {
        final byte[] bytes = Files.readAllBytes(Paths.get(parquetMetaPath));
        final ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        final int size = (int) buffer.getLong(0);
        Assert.assertEquals(bytes.length, size);
        Assert.assertArrayEquals(Arrays.copyOfRange(snapshot, 8, snapshot.length), Arrays.copyOfRange(bytes, 8, snapshot.length));
        final int footer = committedFooterOffset(buffer);
        Assert.assertEquals(snapshot.length, buffer.getLong(footer + 24));
        Assert.assertNotEquals(0, buffer.getLong(footer + 32) & (1L << 2));
        final CRC32 crc = new CRC32();
        crc.update(bytes, 8, size - 16);
        Assert.assertEquals((int) crc.getValue(), buffer.getInt(size - 8));
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
        final int footer = committedFooterOffset(buffer);
        Assert.assertEquals("single footer expected", 0, buffer.getLong(footer + 24));
        final long flags = buffer.getLong(footer + 32);
        Assert.assertNotEquals("writer recorded", 0, flags & (1L << 2));
        buffer.putLong(footer + 32, flags & ~(1L << 2));
        final CRC32 crc = new CRC32();
        crc.update(bytes, 8, size - 16);
        buffer.putInt(size - 8, (int) crc.getValue());
        Files.write(file, bytes);
    }

    private static int committedFooterOffset(ByteBuffer buffer) {
        final int size = (int) buffer.getLong(0);
        return size - 4 - buffer.getInt(size - 4);
    }

    // A composite 2024-01-02 between a converted 2024-01-01 and a plain 2024-01-03.
    private static void createCompositeTableWithConvertedPartition() throws Exception {
        execute("CREATE TABLE x (d DOUBLE, f FLOAT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("""
                INSERT INTO x
                SELECT x::DOUBLE d, x::FLOAT f, timestamp_sequence('2024-01-01', 10_000) ts
                FROM long_sequence(5_000)
                """);
        execute("""
                INSERT INTO x
                SELECT x::DOUBLE d, x::FLOAT f, timestamp_sequence('2024-01-02', 1_000_000) ts
                FROM long_sequence(20_000)
                """);
        drainWalQueue();
        execute("""
                INSERT INTO x
                SELECT x::DOUBLE d, x::FLOAT f, timestamp_sequence('2024-01-03', 1_000_000) ts
                FROM long_sequence(1_000)
                """);
        drainWalQueue();
        for (int i = 0; i < 2; i++) {
            execute("""
                    INSERT INTO x
                    SELECT x::DOUBLE d, x::FLOAT f, timestamp_sequence('2024-01-02T01:00:00', 1_000_000) ts
                    FROM long_sequence(200)
                    """);
            drainWalQueue();
        }
        execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
        drainWalQueue();
        Assert.assertTrue("fixture produced no composite partition", isComposite("x", "2024-01-02"));
        Assert.assertEquals(ColumnType.MAX_STORAGE_VERSION, metaStorageVersion("x"));
    }

    private static void createConvertedTable(String tableName) throws Exception {
        execute("""
                CREATE TABLE %s AS (
                    SELECT x::DOUBLE d, x::FLOAT f, timestamp_sequence('2024-01-01', 10_000) ts
                    FROM long_sequence(5_000)
                ) TIMESTAMP(ts) PARTITION BY DAY
                """.formatted(tableName));
        execute("INSERT INTO " + tableName + " VALUES (1.0, 1.0, '2024-01-02')");
        execute("ALTER TABLE " + tableName + " CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-02'");
    }

    private static boolean isComposite(String tableName, String day) {
        try (TableReader reader = engine.getReader(engine.verifyTableName(tableName))) {
            final TxReader txReader = reader.getTxFile();
            final int partitionIndex = txReader.getPartitionIndex(MicrosTimestampDriver.floor(day + "T00:00:00.000000Z"));
            return partitionIndex > -1 && txReader.isPartitionComposite(partitionIndex);
        }
    }

    private static int metaStorageVersion(String tableName) {
        try (MemoryMR mem = Vm.getCMRInstance(); Path path = new Path()) {
            path.of(configuration.getDbRoot()).concat(engine.verifyTableName(tableName)).concat(TableUtils.META_FILE_NAME);
            mem.smallFile(configuration.getFilesFacade(), path.$(), MemoryTag.MMAP_DEFAULT);
            return mem.getInt(TableUtils.META_OFFSET_VERSION);
        }
    }

    private static String parquetMetaPath(String tableName) {
        return parquetMetaPath(tableName, 0);
    }

    private static String parquetMetaPath(String tableName, int partitionIndex) {
        final TableToken token = engine.verifyTableName(tableName);
        try (TableReader reader = engine.getReader(token); Path path = new Path()) {
            path.of(configuration.getDbRoot()).concat(token);
            TableUtils.setPathForParquetPartitionMetadata(
                    path,
                    ColumnType.TIMESTAMP,
                    PartitionBy.DAY,
                    reader.getTxFile().getPartitionTimestampByIndex(partitionIndex),
                    reader.getTxFile().getPartitionNameTxn(partitionIndex)
            );
            return path.toString();
        }
    }

    private static long previousSnapshotSize(String parquetMetaPath) throws Exception {
        final ByteBuffer buffer = ByteBuffer.wrap(Files.readAllBytes(Paths.get(parquetMetaPath))).order(ByteOrder.LITTLE_ENDIAN);
        return buffer.getLong(committedFooterOffset(buffer) + 24);
    }

    private static int prunedCount(String predicate, String expected) throws Exception {
        return prunedCount("ts < '2024-01-02'", predicate, expected);
    }

    private static int prunedCount(String interval, String predicate, String expected) throws Exception {
        ParquetRowGroupFilter.resetRowGroupsSkipped();
        try (
                RecordCursorFactory factory = select("SELECT count() FROM x WHERE " + interval + " AND " + predicate);
                RecordCursor cursor = factory.getCursor(sqlExecutionContext)
        ) {
            final StringSink sink = new StringSink();
            CursorPrinter.println(cursor, factory.getMetadata(), sink);
            TestUtils.assertEquals(expected, sink);
        }
        return ParquetRowGroupFilter.getRowGroupsSkipped();
    }

    private static int readMigrationVersion() throws Exception {
        final byte[] bytes = Files.readAllBytes(Paths.get(configuration.getDbRoot().toString(), TableUtils.UPGRADE_FILE_NAME));
        return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getInt(Integer.BYTES);
    }

    private static void runMig1011() {
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
            Mig1011.migrate(context);
        } finally {
            Unsafe.free(tempMem, 1024, MemoryTag.NATIVE_MIG_MMAP);
        }
    }

    private static void writeCommittedSize(String parquetMetaPath, long committedSize) throws Exception {
        final java.nio.file.Path file = Paths.get(parquetMetaPath);
        final byte[] bytes = Files.readAllBytes(file);
        ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).putLong(0, committedSize);
        Files.write(file, bytes);
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

    private enum CrashStage {
        BEFORE_APPEND(1),
        AFTER_APPEND(1),
        AFTER_PUBLICATION(2);

        private final int recordedPartitionCount;

        CrashStage(int recordedPartitionCount) {
            this.recordedPartitionCount = recordedPartitionCount;
        }
    }

    // Crashes the migration at the _pm of the second parquet partition: on its open, or on its close, which
    // follows the native append and publication.
    private static class MigrationCrashingFilesFacade extends TestFilesFacadeImpl {
        private CrashStage crashStage;
        private long parquetMetaFd = -1;

        @Override
        public boolean close(long fd) {
            final boolean isClosed = super.close(fd);
            if (parquetMetaFd != -1 && fd == parquetMetaFd) {
                parquetMetaFd = -1;
                throw new CrashSimulationError(crashStage.ordinal());
            }
            return isClosed;
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            if (crashStage == null
                    || !Utf8s.containsAscii(name, PARQUET_DAYS[1])
                    || !Utf8s.endsWithAscii(name, TableUtils.PARQUET_METADATA_FILE_NAME)) {
                return super.openRW(name, opts);
            }
            if (crashStage == CrashStage.BEFORE_APPEND) {
                throw new CrashSimulationError(crashStage.ordinal());
            }
            return parquetMetaFd = super.openRW(name, opts);
        }
    }

    private static class ParquetOpenFailingFilesFacade extends TestFilesFacadeImpl {
        private boolean isParquetOpenFailing;

        @Override
        public long openRO(LPSZ name) {
            if (isParquetOpenFailing && Utf8s.endsWithAscii(name, TableUtils.PARQUET_PARTITION_NAME)) {
                return -1;
            }
            return super.openRO(name);
        }
    }
}
