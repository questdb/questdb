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
import io.questdb.cairo.SymbolCountProvider;
import io.questdb.cairo.SymbolMapWriter;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TxWriter;
import io.questdb.cairo.mig.EngineMigration;
import io.questdb.cairo.mig.Mig1002;
import io.questdb.cairo.mig.MigrationContext;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryARW;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class Mig1002Test extends AbstractCairoTest {
    private static final int LARGE_SYMBOL_COUNT = 10_000;

    @Test
    public void testEngineMigrationRepairsNullFlagOfConvertedColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE stale (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO stale VALUES ('2024-01-05T00:00:00Z', 1), ('2024-01-05T01:00:00Z', 2)");
            execute("ALTER TABLE stale ADD COLUMN s STRING");
            execute("INSERT INTO stale VALUES ('2024-01-06T00:00:00Z', 3, 'A'), ('2024-01-01T00:00:00Z', 4, 'B')");
            execute("ALTER TABLE stale ALTER COLUMN s TYPE SYMBOL");
            unsetSymbolNullFlag("stale", "s");
            engine.releaseAllWriters();
            engine.releaseAllReaders();
            engine.releaseInactive();
            EngineMigration.migrateEngineTo(engine, ColumnType.VERSION, ColumnType.MIGRATION_VERSION, true);
            Assert.assertTrue(containsSymbolNullValue("stale", "s"));
            assertQuery("SELECT x, s FROM stale WHERE s = 'B' OR s = NULL LATEST ON ts PARTITION BY s")
                    .noLeakCheck().inferRandomAccess().sizeMayVary().returns("x\ts\n4\tB\n2\t\n");
        });
    }

    @Test
    public void testEngineMigrationRepeatsFromConfiguredVersion() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_REPEAT_MIGRATION_FROM_VERSION, ColumnType.VERSION);
        assertMemoryLeak(() -> {
            createTableWithStaleNullFlag();
            writeMigrationVersion(ColumnType.MIGRATION_VERSION);
            engine.clear();
            EngineMigration.migrateEngineTo(engine, ColumnType.VERSION, ColumnType.MIGRATION_VERSION, false);
            Assert.assertEquals(ColumnType.MIGRATION_VERSION, readMigrationVersion());
            Assert.assertTrue(containsSymbolNullValue("t", "s"));
        });
    }

    @Test
    public void testEngineMigrationResumesFromRecordedMigrationVersion() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithStaleNullFlag();
            final int nextMigrationVersion = ColumnType.MIGRATION_VERSION + 1;
            writeMigrationVersion(ColumnType.MIGRATION_VERSION);
            engine.clear();
            EngineMigration.migrateEngineTo(engine, ColumnType.VERSION, nextMigrationVersion, false);
            Assert.assertEquals(nextMigrationVersion, readMigrationVersion());
            Assert.assertFalse(containsSymbolNullValue("t", "s"));

            writeMigrationVersion(ColumnType.MIGRATION_VERSION - 1);
            engine.clear();
            EngineMigration.migrateEngineTo(engine, ColumnType.VERSION, nextMigrationVersion, false);
            Assert.assertEquals(nextMigrationVersion, readMigrationVersion());
            Assert.assertTrue(containsSymbolNullValue("t", "s"));
            assertQuery("SELECT x, s FROM t LATEST ON ts PARTITION BY s")
                    .noLeakCheck().inferRandomAccess().sizeMayVary().returns("x\ts\n1\t\n2\tA\n");
        });
    }

    @Test
    public void testFailedNullFlagWriteKeepsMigrationVersionUntilRetry() throws Exception {
        final NullFlagWriteFailingFilesFacade ff = new NullFlagWriteFailingFilesFacade();
        assertMemoryLeak(ff, () -> {
            createTableWithStaleNullFlag();
            final int previousMigrationVersion = ColumnType.MIGRATION_VERSION - 1;
            writeMigrationVersion(previousMigrationVersion);

            engine.clear();
            ff.isNullFlagWriteFailing = true;
            try {
                EngineMigration.migrateEngineTo(engine, ColumnType.VERSION, ColumnType.MIGRATION_VERSION, false);
                Assert.fail();
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "could not write symbol null flag");
            } finally {
                ff.isNullFlagWriteFailing = false;
            }
            Assert.assertEquals(previousMigrationVersion, readMigrationVersion());
            Assert.assertFalse(containsSymbolNullValue("t", "s"));

            engine.clear();
            EngineMigration.migrateEngineTo(engine, ColumnType.VERSION, ColumnType.MIGRATION_VERSION, false);
            Assert.assertEquals(ColumnType.MIGRATION_VERSION, readMigrationVersion());
            Assert.assertTrue(containsSymbolNullValue("t", "s"));
            assertQuery("SELECT x, s FROM t LATEST ON ts PARTITION BY s")
                    .noLeakCheck().inferRandomAccess().sizeMayVary().returns("x\ts\n1\t\n2\tA\n");
        });
    }

    @Test
    public void testKeepsLargeSymbolMapWhenFlagAlreadySet() throws Exception {
        assertMemoryLeak(() -> {
            createLargeSymbolMapTable();
            Assert.assertTrue(containsSymbolNullValue("t", "s"));
            final long offsetFileLength = offsetFileLength();
            runMig1002("t");
            Assert.assertTrue(containsSymbolNullValue("t", "s"));
            Assert.assertEquals(offsetFileLength, offsetFileLength());
            assertLargeSymbolMapReadable();
        });
    }

    @Test
    public void testKeepsNullFlagUnsetWithoutColumnTops() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, s SYMBOL, d SYMBOL) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO t VALUES ('2024-01-05T00:00:00Z', 'A', 'x'), ('2024-01-06T00:00:00Z', 'B', 'y')");
            execute("ALTER TABLE t DROP COLUMN d");
            Assert.assertFalse(containsSymbolNullValue("t", "s"));
            runMig1002("t");
            Assert.assertFalse(containsSymbolNullValue("t", "s"));
            assertQuery("SELECT s FROM t LATEST ON ts PARTITION BY s")
                    .noLeakCheck().inferRandomAccess().sizeMayVary().returns("s\nA\nB\n");
        });
    }

    @Test
    public void testKeepsNullFlagUnsetWhenNoPartitionHoldsNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, x INT, s SYMBOL INDEX) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00Z', 1, 'A'), ('2024-01-02T00:00:00Z', 2, 'B'), ('2024-01-03T00:00:00Z', 3, 'A')");
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            Assert.assertFalse(containsSymbolNullValue("t", "s"));
            runMig1002("t");
            Assert.assertFalse(containsSymbolNullValue("t", "s"));
            assertQuery("SELECT x, s FROM t LATEST ON ts PARTITION BY s")
                    .noLeakCheck().inferRandomAccess().sizeMayVary().returns("x\ts\n2\tB\n3\tA\n");
        });
    }

    @Test
    public void testKeepsRemoteKnownZeroNullFlagUnsetWithoutOpeningData() throws Exception {
        final MigrationDataGuardFilesFacade ff = new MigrationDataGuardFilesFacade();
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE t (ts TIMESTAMP, x INT, s SYMBOL INDEX) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00Z', 1, 'A'), ('2024-01-02T00:00:00Z', 2, 'B')");
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            assertParquetSymbolNullCountStat(true);
            makeFirstPartitionRemote();
            final long offsetLength = offsetFileLength();
            ff.isDataOpenProhibited = true;
            try {
                runMig1002("t");
                runMig1002("t");
                Assert.assertFalse(containsSymbolNullValue("t", "s"));
                Assert.assertEquals(offsetLength, offsetFileLength());
            } finally {
                ff.isDataOpenProhibited = false;
            }
        });
    }

    @Test
    public void testNativeAttachAtHeadRestoresFlagBeforeMigration() throws Exception {
        assertMemoryLeak(() -> {
            createReattachedNullPartition(false);
            assertNativeTopZeroAndFullScan("x\ts\n1\t\n2\t\n");
            Assert.assertTrue(containsSymbolNullValue("t", "s"));
            assertNullFlagAfterTwoMigrations("x\ts\n2\t\n");
        });
    }

    @Test
    public void testRepairsNullFlagOfNativePartitionAfterParquetAttachRoundTrip() throws Exception {
        assertMemoryLeak(() -> {
            createReattachedNullPartition(true);
            execute("ALTER TABLE t CONVERT PARTITION TO NATIVE LIST '2024-01-01'");
            assertNativeTopZeroAndFullScan("x\ts\n1\t\n2\t\n");
            assertNullFlagAfterTwoMigrations("x\ts\n2\t\n");
        });
    }

    @Test
    public void testLatestOnFindsNullGroupOfLegacyAttachedPartitionWithoutFlag() throws Exception {
        assertMemoryLeak(() -> {
            createReattachedNullPartition(false);
            unsetSymbolNullFlag("t", "s");
            runMig1002("t");
            Assert.assertFalse(containsSymbolNullValue("t", "s"));
            assertQuery("SELECT x, s FROM t LATEST ON ts PARTITION BY s")
                    .noLeakCheck().inferRandomAccess().sizeMayVary().returns("x\ts\n2\t\n");
            assertQuery("SELECT x, s FROM t WHERE s NOT IN ('A', 'B') LATEST ON ts PARTITION BY s")
                    .noLeakCheck().inferRandomAccess().sizeMayVary().returns("x\ts\n2\t\n");
        });
    }

    @Test
    public void testKeepsNullFlagUnsetForParquetPartitionWithoutStatistics() throws Exception {
        final MigrationDataGuardFilesFacade ff = new MigrationDataGuardFilesFacade();
        assertMemoryLeak(ff, () -> {
            setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_STATISTICS_ENABLED, "false");
            execute("CREATE TABLE t (ts TIMESTAMP, x INT, s SYMBOL) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00Z', 1, 'A'), ('2024-01-01T01:00:00Z', 2, NULL), ('2024-01-02T00:00:00Z', 3, 'B')");
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            assertParquetSymbolNullCountStat(false);
            unsetSymbolNullFlag("t", "s");
            engine.clear();
            ff.isDataOpenProhibited = true;
            try {
                runMig1002("t");
            } finally {
                ff.isDataOpenProhibited = false;
            }
            Assert.assertFalse(containsSymbolNullValue("t", "s"));
        });
    }

    @Test
    public void testRepairsNullFlagUsingBitmapIndexWithoutReadingData() throws Exception {
        final MigrationDataGuardFilesFacade ff = new MigrationDataGuardFilesFacade();
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE t (ts TIMESTAMP, x INT, s SYMBOL INDEX, d SYMBOL INDEX) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00Z', 1, 'A', 'X'), ('2024-01-01T01:00:00Z', 2, NULL, 'X'), ('2024-01-02T00:00:00Z', 3, 'B', 'Y')");
            unsetSymbolNullFlag("t", "s");
            engine.clear();
            ff.isDataOpenProhibited = true;
            try {
                runMig1002("t");
            } finally {
                ff.isDataOpenProhibited = false;
            }
            Assert.assertTrue(containsSymbolNullValue("t", "s"));
            Assert.assertFalse(containsSymbolNullValue("t", "d"));
            assertQuery("SELECT x, s FROM t LATEST ON ts PARTITION BY s")
                    .noLeakCheck().inferRandomAccess().sizeMayVary().returns("x\ts\n1\tA\n2\t\n3\tB\n");
        });
    }

    @Test
    public void testRepairsNullFlagOfColumnAbsentFromOlderPartitions() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO t VALUES ('2024-01-05T00:00:00Z', 1), ('2024-01-06T00:00:00Z', 2)");
            execute("ALTER TABLE t ADD COLUMN s SYMBOL");
            execute("INSERT INTO t VALUES ('2024-01-07T00:00:00Z', 3, 'A')");
            unsetSymbolNullFlag("t", "s");
            runMig1002("t");
            Assert.assertTrue(containsSymbolNullValue("t", "s"));
            assertQuery("SELECT x, s FROM t LATEST ON ts PARTITION BY s")
                    .noLeakCheck().inferRandomAccess().sizeMayVary().returns("x\ts\n2\t\n3\tA\n");
        });
    }

    @Test
    public void testRepairsNullFlagOfConvertedColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE stale (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO stale VALUES ('2024-01-05T00:00:00Z', 1), ('2024-01-05T01:00:00Z', 2)");
            execute("ALTER TABLE stale ADD COLUMN s STRING");
            execute("INSERT INTO stale VALUES ('2024-01-06T00:00:00Z', 3, 'A'), ('2024-01-01T00:00:00Z', 4, 'B')");
            execute("ALTER TABLE stale ALTER COLUMN s TYPE SYMBOL");
            execute("CREATE TABLE keys (k STRING)");
            execute("INSERT INTO keys VALUES ('A'), ('B')");
            Assert.assertTrue(containsSymbolNullValue("stale", "s"));
            unsetSymbolNullFlag("stale", "s");
            runMig1002("stale");
            Assert.assertTrue(containsSymbolNullValue("stale", "s"));

            final String[] predicates = {
                    "",
                    "WHERE s = 'A' OR s = 'B' ",
                    "WHERE s IN (SELECT k FROM keys) ",
                    "WHERE s NOT IN ('A', 'B') ",
                    "WHERE s = 'B' OR s = NULL ",
            };
            final String[] expected = {
                    "x\ts\n4\tB\n2\t\n3\tA\n",
                    "x\ts\n4\tB\n3\tA\n",
                    "x\ts\n4\tB\n3\tA\n",
                    "x\ts\n2\t\n",
                    "x\ts\n4\tB\n2\t\n",
            };
            for (int i = 0; i < predicates.length; i++) {
                assertQuery("SELECT x, s FROM stale " + predicates[i] + "LATEST ON ts PARTITION BY s")
                        .noLeakCheck().inferRandomAccess().sizeMayVary().returns(expected[i]);
            }
        });
    }

    @Test
    public void testRepairsNullFlagOfConvertedColumnAfterParquetConversion() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE stale (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO stale VALUES ('2024-01-01T00:00:00Z', 1)");
            execute("ALTER TABLE stale ADD COLUMN s STRING");
            execute("""
                    INSERT INTO stale VALUES
                        ('2024-01-01T01:00:00Z', 2, 'A'),
                        ('2024-01-02T00:00:00Z', 3, 'B')
                    """);
            execute("ALTER TABLE stale ALTER COLUMN s TYPE SYMBOL");
            unsetSymbolNullFlag("stale", "s");
            execute("ALTER TABLE stale CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            try (TableReader reader = getReader("stale")) {
                Assert.assertTrue(reader.getTxFile().isPartitionParquet(0));
                final int writerIndex = reader.getMetadata().getWriterIndex(reader.getMetadata().getColumnIndex("s"));
                Assert.assertEquals(0, reader.getColumnVersionReader().getColumnTop(reader.getPartitionTimestampByIndex(0), writerIndex));
            }
            assertQuery("SELECT x, s FROM stale")
                    .noLeakCheck().expectSize().inferRandomAccess().returns("x\ts\n1\t\n2\tA\n3\tB\n");
            runMig1002("stale");
            Assert.assertTrue(containsSymbolNullValue("stale", "s"));
            assertQuery("SELECT x, s FROM stale LATEST ON ts PARTITION BY s")
                    .noLeakCheck().inferRandomAccess().sizeMayVary().returns("x\ts\n1\t\n2\tA\n3\tB\n");
            assertQuery("SELECT x, s FROM stale WHERE s NOT IN ('A', 'B') LATEST ON ts PARTITION BY s")
                    .noLeakCheck().inferRandomAccess().sizeMayVary().returns("x\ts\n1\t\n");
        });
    }

    @Test
    public void testRepairsNullFlagOfConvertedColumnInNonPartitionedTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE stale (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY NONE");
            execute("INSERT INTO stale VALUES ('2024-01-05T00:00:00Z', 1), ('2024-01-05T01:00:00Z', 2)");
            execute("ALTER TABLE stale ADD COLUMN s STRING");
            execute("INSERT INTO stale VALUES ('2024-01-06T00:00:00Z', 3, 'A'), ('2024-01-07T00:00:00Z', 4, 'B')");
            execute("ALTER TABLE stale ALTER COLUMN s TYPE SYMBOL");
            unsetSymbolNullFlag("stale", "s");
            runMig1002("stale");
            Assert.assertTrue(containsSymbolNullValue("stale", "s"));
            assertQuery("SELECT x, s FROM stale LATEST ON ts PARTITION BY s")
                    .noLeakCheck().inferRandomAccess().sizeMayVary().returns("x\ts\n2\t\n3\tA\n4\tB\n");
            assertQuery("SELECT x, s FROM stale WHERE s NOT IN ('A', 'B') LATEST ON ts PARTITION BY s")
                    .noLeakCheck().inferRandomAccess().sizeMayVary().returns("x\ts\n2\t\n");
        });
    }

    @Test
    public void testRepairsNullFlagOfLargeSymbolMap() throws Exception {
        assertMemoryLeak(() -> {
            createLargeSymbolMapTable();
            unsetSymbolNullFlag("t", "s");
            final long offsetFileLength = offsetFileLength();
            runMig1002("t");
            Assert.assertTrue(containsSymbolNullValue("t", "s"));
            Assert.assertEquals(offsetFileLength, offsetFileLength());
            assertLargeSymbolMapReadable();
        });
    }

    private static void assertParquetSymbolNullCountStat(boolean isPresent) {
        final FilesFacade ff = configuration.getFilesFacade();
        try (TableReader reader = getReader("t"); Path path = new Path().of(configuration.getDbRoot()).concat(reader.getTableToken())) {
            final long partitionTs = reader.getPartitionTimestampByIndex(0);
            final long partitionNameTxn = reader.getTxFile().getPartitionNameTxn(0);
            final long parquetFileSize = reader.getTxFile().getPartitionParquetFileSize(0);
            final int symbolWriterIndex = reader.getMetadata().getWriterIndex(reader.getMetadata().getColumnIndex("s"));
            TableUtils.setPathForParquetPartitionMetadata(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTs, partitionNameTxn);
            final ParquetMetaFileReader metadata = new ParquetMetaFileReader();
            final long addr = ParquetMetaFileReader.openAndMapRO(ff, path.$(), metadata);
            Assert.assertNotEquals(0, addr);
            final long size = metadata.getFileSize();
            try {
                Assert.assertTrue(metadata.resolveFooter(parquetFileSize));
                Assert.assertEquals(1, metadata.getRowGroupCount());
                Assert.assertEquals(isPresent, metadata.hasChunkNullCount(0, metadata.getColumnIndexById(symbolWriterIndex)));
            } finally {
                metadata.clear();
                ff.munmap(addr, size, MemoryTag.MMAP_PARQUET_METADATA_READER);
            }
        }
    }

    private static void createLargeSymbolMapTable() throws Exception {
        execute("CREATE TABLE t (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
        execute("INSERT INTO t VALUES ('2024-01-05T00:00:00Z', 0)");
        execute("ALTER TABLE t ADD COLUMN s SYMBOL");
        execute("INSERT INTO t SELECT '2024-01-06T00:00:00Z'::TIMESTAMP + x, x::INT, 'sym' || x FROM long_sequence("
                + LARGE_SYMBOL_COUNT + ")");
    }

    private static void createReattachedNullPartition(boolean isParquet) throws Exception {
        execute("CREATE TABLE t (ts TIMESTAMP, x INT, s SYMBOL) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
        execute("INSERT INTO t VALUES ('2024-01-01T00:00:00Z', 1, NULL), ('2024-01-01T01:00:00Z', 2, NULL), ('2024-01-02T00:00:00Z', 3, 'A')");
        if (isParquet) {
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
        }
        execute("ALTER TABLE t DETACH PARTITION LIST '2024-01-01'");
        execute("TRUNCATE TABLE t");
        Assert.assertFalse(containsSymbolNullValue("t", "s"));
        final java.nio.file.Path tablePath = java.nio.file.Path.of(configuration.getDbRoot().toString(), engine.verifyTableName("t").getDirName());
        java.nio.file.Files.move(
                tablePath.resolve("2024-01-01" + TableUtils.DETACHED_DIR_MARKER),
                tablePath.resolve("2024-01-01" + configuration.getAttachPartitionSuffix())
        );
        execute("ALTER TABLE t ATTACH PARTITION LIST '2024-01-01'");
    }

    private static void createTableWithStaleNullFlag() throws Exception {
        execute("CREATE TABLE t (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
        execute("INSERT INTO t VALUES ('2024-01-05T00:00:00Z', 1)");
        execute("ALTER TABLE t ADD COLUMN s SYMBOL");
        execute("INSERT INTO t VALUES ('2024-01-06T00:00:00Z', 2, 'A')");
        unsetSymbolNullFlag("t", "s");
    }

    private static void makeFirstPartitionRemote() {
        final FilesFacade ff = configuration.getFilesFacade();
        final long partitionTs;
        final long partitionNameTxn;
        final int symbolCount;
        try (TableReader reader = getReader("t")) {
            partitionTs = reader.getPartitionTimestampByIndex(0);
            partitionNameTxn = reader.getTxFile().getPartitionNameTxn(0);
            symbolCount = reader.getSymbolMapReader(reader.getMetadata().getColumnIndex("s")).getSymbolCount();
        }
        engine.clear();
        try (Path path = new Path().of(configuration.getDbRoot()).concat(engine.verifyTableName("t"))) {
            final int tablePathLen = path.size();
            path.concat(TableUtils.TXN_FILE_NAME);
            try (TxWriter txWriter = new TxWriter(ff, configuration).ofRW(path.$(), ColumnType.TIMESTAMP, PartitionBy.DAY)) {
                txWriter.setPartitionParquetGenerated(partitionTs, false);
                txWriter.setPartitionRemoteByTimestamp(partitionTs, true);
                final ObjList<SymbolCountProvider> counts = new ObjList<>();
                counts.add(() -> symbolCount);
                txWriter.commit(counts);
            }
            TableUtils.setPathForParquetPartition(path.trimTo(tablePathLen), ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTs, partitionNameTxn);
            ff.remove(path.$());
            Assert.assertFalse(ff.exists(path.$()));
        }
        try (TableReader reader = getReader("t")) {
            Assert.assertTrue(reader.getTxFile().isPartitionRemotelyServed(0));
        }
        engine.clear();
    }

    private static long offsetFileLength() {
        engine.releaseAllWriters();
        try (
                TableReader reader = engine.getReader(engine.verifyTableName("t"));
                Path path = new Path().of(configuration.getDbRoot()).concat(reader.getTableToken())
        ) {
            final int columnIndex = reader.getMetadata().getColumnIndex("s");
            final int writerIndex = reader.getMetadata().getWriterIndex(columnIndex);
            final long columnNameTxn = reader.getColumnVersionReader().getSymbolTableNameTxn(writerIndex);
            return configuration.getFilesFacade().length(TableUtils.offsetFileName(path, "s", columnNameTxn));
        }
    }

    private static int readMigrationVersion() {
        final FilesFacade ff = configuration.getFilesFacade();
        try (Path path = new Path().of(configuration.getDbRoot()).concat(TableUtils.UPGRADE_FILE_NAME)) {
            final long fd = ff.openRO(path.$());
            Assert.assertTrue(fd > -1);
            try {
                return ff.readNonNegativeInt(fd, Integer.BYTES);
            } finally {
                ff.close(fd);
            }
        }
    }

    private static void runMig1002(String tableName) {
        final TableToken token = engine.verifyTableName(tableName);
        final FilesFacade ff = configuration.getFilesFacade();
        engine.releaseAllWriters();
        engine.releaseAllReaders();
        engine.releaseInactive();
        final long tempMem = Unsafe.malloc(1024, MemoryTag.NATIVE_MIG_MMAP);
        try (
                MemoryMARW rwMem = Vm.getCMARWInstance();
                MemoryARW tempVirtualMem = Vm.getCARWInstance(ff.getPageSize(), Integer.MAX_VALUE, MemoryTag.NATIVE_MIG_MMAP);
                Path tablePath = new Path().of(configuration.getDbRoot()).concat(token).slash();
                Path tablePath2 = new Path().of(configuration.getDbRoot()).concat(token).slash()
        ) {
            final MigrationContext ctx = new MigrationContext(engine, tempMem, 1024, tempVirtualMem, rwMem);
            ctx.of(tablePath, tablePath2, -1);
            Mig1002.migrate(ctx);
        } finally {
            Unsafe.free(tempMem, 1024, MemoryTag.NATIVE_MIG_MMAP);
        }
    }

    private static void writeMigrationVersion(int migrationVersion) {
        final FilesFacade ff = configuration.getFilesFacade();
        final long mem = Unsafe.malloc(Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        try (Path path = new Path().of(configuration.getDbRoot()).concat(TableUtils.UPGRADE_FILE_NAME)) {
            final long fd = ff.openRW(path.$(), configuration.getWriterFileOpenOpts());
            Assert.assertTrue(fd > -1);
            try {
                TableUtils.writeIntOrFail(ff, fd, 0, ColumnType.VERSION, mem, path);
                TableUtils.writeIntOrFail(ff, fd, Integer.BYTES, migrationVersion, mem, path);
            } finally {
                ff.close(fd);
            }
        } finally {
            Unsafe.free(mem, Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private void assertNativeTopZeroAndFullScan(String expected) throws Exception {
        engine.clear();
        try (TableReader reader = getReader("t")) {
            Assert.assertFalse(reader.getTxFile().isPartitionParquet(0));
            final int writerIndex = reader.getMetadata().getWriterIndex(reader.getMetadata().getColumnIndex("s"));
            Assert.assertEquals(0, reader.getColumnVersionReader().getColumnTop(reader.getPartitionTimestampByIndex(0), writerIndex));
        }
        assertQuery("SELECT x, s FROM t")
                .noLeakCheck().expectSize().inferRandomAccess().returns(expected);
    }

    private void assertNullFlagAfterTwoMigrations(String expected) throws Exception {
        final long originalOffsetLength = offsetFileLength();
        for (int pass = 0; pass < 2; pass++) {
            runMig1002("t");
            engine.clear();
            assertQuery("SELECT x, s FROM t LATEST ON ts PARTITION BY s")
                    .noLeakCheck().inferRandomAccess().sizeMayVary().returns(expected);
            Assert.assertTrue(containsSymbolNullValue("t", "s"));
            Assert.assertEquals(originalOffsetLength, offsetFileLength());
        }
    }

    private void assertLargeSymbolMapReadable() throws Exception {
        assertQuery("SELECT count() FROM (SELECT * FROM t LATEST ON ts PARTITION BY s)")
                .noLeakCheck().inferRandomAccess().expectSize().returns("count\n" + (LARGE_SYMBOL_COUNT + 1) + "\n");
        assertQuery("SELECT x, s FROM t WHERE s IN ('sym1', 'sym" + LARGE_SYMBOL_COUNT + "')")
                .noLeakCheck().inferRandomAccess().returns("x\ts\n1\tsym1\n" + LARGE_SYMBOL_COUNT + "\tsym" + LARGE_SYMBOL_COUNT + "\n");
    }

    private static class NullFlagWriteFailingFilesFacade extends TestFilesFacadeImpl {
        private boolean isNullFlagWriteFailing;

        @Override
        public long write(long fd, long address, long len, long offset) {
            if (isNullFlagWriteFailing && offset == SymbolMapWriter.HEADER_NULL_FLAG && len == Byte.BYTES) {
                return -1;
            }
            return super.write(fd, address, len, offset);
        }
    }

    private static class MigrationDataGuardFilesFacade extends TestFilesFacadeImpl {
        private boolean isDataOpenProhibited;

        @Override
        public long openRO(LPSZ name) {
            if (isDataOpenProhibited && (Utf8s.endsWithAscii(name, TableUtils.PARQUET_PARTITION_NAME) || Utf8s.endsWithAscii(name, ".d"))) {
                Assert.fail("unexpected data open: " + name);
            }
            return super.openRO(name);
        }
    }
}
