/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.cairo;

import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryARW;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.std.*;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static io.questdb.cairo.EngineMigration.*;
import static io.questdb.cairo.TableUtils.*;

public class EngineMigrationTest extends AbstractGriffinTest {
    @Before
    public void setUp3() {
        SharedRandom.RANDOM.set(new Rnd());
    }

    @Test
    public void testMigrateTableNoSymbolsNoPartitions() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "x_none", PartitionBy.NONE)) {

                createPopulateTable(
                        src.col("s1", ColumnType.SYMBOL).indexed(true, 4096)
                                .col("c1", ColumnType.INT)
                                .col("s2", ColumnType.SYMBOL)
                                .col("c2", ColumnType.LONG)
                                .col("ts", ColumnType.TIMESTAMP).timestamp(),
                        100, "2020-01-01", 0
                );

                String query = "select sum(c1) from x_none";
                assertMigration(query);
            }
        });
    }

    @Test
    public void testMigrateTableWithDayPartitions() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "x_day", PartitionBy.DAY)) {

                createPopulateTable(
                        src.col("s1", ColumnType.SYMBOL).indexed(true, 4096)
                                .col("c1", ColumnType.INT)
                                .col("s2", ColumnType.SYMBOL)
                                .col("c2", ColumnType.LONG)
                                .col("ts", ColumnType.TIMESTAMP).timestamp(),
                        100, "2020-01-01", 10
                );

                String query = "select sum(c1) from x_day";
                assertMigration(query);
            }
        });
    }

    @Test
    public void testMigrateTableWithMonthPartitions() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "x_month", PartitionBy.MONTH)) {

                createPopulateTable(
                        src.col("s1", ColumnType.SYMBOL).indexed(true, 4096)
                                .col("c1", ColumnType.INT)
                                .col("s2", ColumnType.SYMBOL)
                                .col("c2", ColumnType.LONG)
                                .col("ts", ColumnType.TIMESTAMP).timestamp(),
                        100, "2020-01-01", 3
                );

                String query = "select sum(c1) from x_month";
                assertMigration(query);
            }
        });
    }

    @Test
    public void testMigrateTableWithYearPartitions() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "x_year", PartitionBy.YEAR)) {

                createPopulateTable(
                        src.col("s1", ColumnType.SYMBOL).indexed(true, 4096)
                                .col("c1", ColumnType.INT)
                                .col("s2", ColumnType.SYMBOL)
                                .col("c2", ColumnType.LONG)
                                .col("ts", ColumnType.TIMESTAMP).timestamp(),
                        100, "2020-01-01", 3
                );

                String query = "select sum(c1) from x_year";
                assertMigration(query);
            }
        });
    }

    @Test
    public void testMigrateTableWithSymbols() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "x_none", PartitionBy.NONE)) {

                createPopulateTable(
                        src.col("s1", ColumnType.SYMBOL).indexed(true, 4096)
                                .col("c1", ColumnType.INT)
                                .col("s2", ColumnType.SYMBOL)
                                .col("c2", ColumnType.LONG)
                                .col("ts", ColumnType.TIMESTAMP).timestamp(),
                        100, "2020-01-01", 0
                );

                String query = "select distinct s1, s2 from x_none";
                assertMigration(query);
            }
        });
    }

    @Test
    public void testMigrateTableWithDayPartitionsAndSymbols() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "x_day", PartitionBy.DAY)) {

                createPopulateTable(
                        src.col("s1", ColumnType.SYMBOL).indexed(true, 4096)
                                .col("c1", ColumnType.INT)
                                .col("s2", ColumnType.SYMBOL)
                                .col("c2", ColumnType.LONG)
                                .col("ts", ColumnType.TIMESTAMP).timestamp(),
                        100, "2020-01-01", 10
                );

                String query = "select distinct s1, s2 from x_day";
                assertMigration(query);
            }
        });
    }

    @Test
    public void testMigrateTableWithDayRemovedPartition() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "x_day", PartitionBy.DAY)) {

                createPopulateTable(
                        src.col("s1", ColumnType.SYMBOL).indexed(true, 4096)
                                .col("c1", ColumnType.INT)
                                .col("s2", ColumnType.SYMBOL)
                                .col("c2", ColumnType.LONG)
                                .col("ts", ColumnType.TIMESTAMP).timestamp(),
                        100, "2020-01-01", 10
                );

                String queryOld = "select sum(c1) from x_day where ts not in '2020-01-01'";
                String queryNew = "select sum(c1) from x_day";
                assertMigration(queryOld, queryNew, "/migration/assertMigrationPartitionsRemoved.zip");
            }
        });
    }

    @Test
    public void testCannotReadMetadata() throws Exception {
        assertMemoryLeak(() -> {
            // roll table id up
            for (int i = 0; i < 10; i++) {
                engine.getNextTableId();
            }
            // old table
            try (TableModel model = new TableModel(configuration, "y_416", PartitionBy.DAY).col("aaa", ColumnType.SYMBOL).timestamp()
            ) {
                CairoTestUtils.createTableWithVersion(model, 416);
            }

            FilesFacade ff = new FilesFacadeImpl() {
                @Override
                public long read(long fd, long buf, long len, long offset) {
                    return 0;
                }
            };

            // we need to remove "upgrade" file for the engine to upgrade tables
            // remember, this is the second instance of the engine

            assertRemoveUpgradeFile();

            try {
                new CairoEngine(new DefaultCairoConfiguration(root) {
                    @Override
                    public FilesFacade getFilesFacade() {
                        return ff;
                    }
                });
                Assert.fail();
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "Could not update table");
            }
        });
    }

    @Test
    public void testMigrateTxFileFailsToSaveTableMetaVersion() throws Exception {
        assertMemoryLeak(() -> {
            assertRemoveUpgradeFile();
            replaceDbContent("/migration/testMigrateTxFileFailsToSaveTableMetaVersion.zip");
            DefaultCairoConfiguration config = new DefaultCairoConfiguration(root) {
                private final FilesFacadeImpl ff = failToWriteMetaOffset(META_OFFSET_VERSION, "meta");

                @Override
                public FilesFacade getFilesFacade() {
                    return ff;
                }
            };

            try {
                try (CairoEngine ignored = new CairoEngine(config)) {
                    Assert.fail();
                }
            } catch (CairoException e) {
                Assert.assertTrue(e.getMessage().contains("failed to write updated version to table Metadata file"));
            }
        });
    }

    @Test
    public void testMigrateFailsToSaveTableMetaId() throws Exception {
        assertMemoryLeak(() -> {
            replaceDbContent("/migration/testMigrateFailsToSaveTableMetaId.zip");
            assertRemoveUpgradeFile();

            DefaultCairoConfiguration config = new DefaultCairoConfiguration(root) {
                private final FilesFacadeImpl ff = failToWriteMetaOffset(META_OFFSET_TABLE_ID, "meta");

                @Override
                public FilesFacade getFilesFacade() {
                    return ff;
                }
            };

            try {
                try (CairoEngine ignored = new CairoEngine(config)) {
                    Assert.fail();
                }
            } catch (CairoException e) {
                Assert.assertTrue(e.getMessage().contains("Could not update table id"));
            }
        });
    }

    @Test
    public void testMigrateToSaveGlobalUpdateVersion() throws Exception {
        assertMemoryLeak(() -> {
            replaceDbContent("/migration/testMigrateFailsToSaveTableMetaId.zip");
            assertRemoveUpgradeFile();

            DefaultCairoConfiguration config = new DefaultCairoConfiguration(root) {
                private final FilesFacadeImpl ff = failToWriteMetaOffset(META_OFFSET_TABLE_ID, TableUtils.UPGRADE_FILE_NAME);

                @Override
                public FilesFacade getFilesFacade() {
                    return ff;
                }
            };

            CairoEngine ignored = new CairoEngine(config);
            // Migration should be successful, not exceptions
            ignored.close();
        });
    }

    @Test
    public void testMigrateTableSimple() throws Exception {
        configOverrideMaxUncommittedRows = 50001;
        configOverrideCommitLag = 777777;

        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "x_none", PartitionBy.NONE)) {

                createPopulateTable(
                        src.col("s1", ColumnType.SYMBOL).indexed(true, 4096)
                                .col("c1", ColumnType.INT)
                                .col("s2", ColumnType.SYMBOL)
                                .col("c2", ColumnType.LONG)
                                .col("ts", ColumnType.TIMESTAMP).timestamp(),
                        100, "2020-01-01", 0
                );

                String query = "select sum(c1) from x_none";
                assertMetadataMigration(src, query);
            }
        });
    }

    @Test
    public void testCannotUpdateLagMetadata1() throws Exception {
        configOverrideMaxUncommittedRows = 1231231;
        configOverrideCommitLag = 85754;
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "x_none", PartitionBy.NONE)) {

                createPopulateTable(
                        src.col("s1", ColumnType.SYMBOL).indexed(true, 4096)
                                .col("c1", ColumnType.INT)
                                .col("s2", ColumnType.SYMBOL)
                                .col("c2", ColumnType.LONG)
                                .col("ts", ColumnType.TIMESTAMP).timestamp(),
                        100, "2020-01-01", 0
                );

                ff = new FilesFacadeImpl() {
                    @Override
                    public long write(long fd, long buf, long len, long offset) {
                        if (META_OFFSET_MAX_UNCOMMITTED_ROWS == offset) {
                            return 0;
                        }
                        return super.write(fd, buf, len, offset);
                    }
                };

                try {
                    assertMetadataMigration(src, "select sum(c1) from x_none");
                    Assert.fail();
                } catch (SqlException e) {
                    Chars.contains(e.getFlyweightMessage(), "Metadata version does not match runtime version");
                }

                ff = new FilesFacadeImpl() {
                    @Override
                    public long write(long fd, long buf, long len, long offset) {
                        if (META_OFFSET_COMMIT_LAG == offset) {
                            return 0;
                        }
                        return super.write(fd, buf, len, offset);
                    }
                };

                try {
                    new EngineMigration(engine, configuration).migrateEngineTo(ColumnType.VERSION);
                    assertMetadataMigration(src, "select sum(c1) from x_none");
                    Assert.fail();
                } catch (SqlException e) {
                    Chars.contains(e.getFlyweightMessage(), "Metadata version does not match runtime version");
                }

                ff = new FilesFacadeImpl();
                new EngineMigration(engine, configuration).migrateEngineTo(ColumnType.VERSION);
                assertMetadataMigration(src, "select sum(c1) from x_none");
            }
        });
    }

    private void assertMetadataMigration(TableModel src, String query) throws SqlException, IOException {
        assertMetadataMigration(src, query, query);
    }

    private void assertMetadataMigration(TableModel src, String queryOld, String queryNew) throws SqlException, IOException {
        CharSequence expected = executeSql(queryOld).toString();
        if (!queryOld.equals(queryNew)) {
            // if queries are different they must produce different results
            CharSequence expectedNewEquivalent = executeSql(queryNew).toString();
            Assert.assertNotEquals(expected, expectedNewEquivalent);
        }

        // Downgrade version meta
        replaceDbContent("/migration/assertMetadataMigration.zip");
        assertRemoveUpgradeFile();

        // Act
        new EngineMigration(engine, configuration).migrateEngineTo(ColumnType.VERSION);

        // Verify
        TestUtils.assertEquals(expected, executeSql(queryNew));

        // Second run of migration should not do anything
        new EngineMigration(engine, configuration).migrateEngineTo(ColumnType.VERSION);
        TestUtils.assertEquals(expected, executeSql(queryNew));

        // Third time, downgrade and migrate
        replaceDbContent("/migration/assertMetadataMigration.zip");
        assertRemoveUpgradeFile();

        new EngineMigration(engine, configuration).migrateEngineTo(ColumnType.VERSION);
        TestUtils.assertEquals(expected, executeSql(queryNew));

        assertSql("select maxUncommittedRows, commitLag from tables where name = '" + src.getName() + "'",
                "maxUncommittedRows\tcommitLag\n" +
                        configOverrideMaxUncommittedRows + "\t" + configOverrideCommitLag + "\n");
    }

    private void downgradeMetaDataFile(TableModel tableModel) {
        engine.clear();
        FilesFacade ff = configuration.getFilesFacade();

        try (Path path = new Path()) {
            setMetadataVersion(tableModel, ff, path, VERSION_TBL_META_COMMIT_LAG);

            path.concat(root).concat(tableModel.getName()).concat(TableUtils.META_FILE_NAME);
            try (MemoryMARW rwTx = Vm.getSmallMARWInstance(ff, path.$())) {
                rwTx.putInt(META_OFFSET_MAX_UNCOMMITTED_ROWS, 0);
                rwTx.putLong(META_OFFSET_COMMIT_LAG, 0);
            }

            setMetadataVersion(tableModel, ff, path, VERSION_TBL_META_COMMIT_LAG);
            downgradeUpdateFileTo(ff, path);
        }
    }

    private void setMetadataVersion(TableModel tableModel, FilesFacade ff, Path path, int version) {
        final int pathLen = path.length();

        try {
            path.trimTo(0).concat(root).concat(tableModel.getName()).concat(TableUtils.META_FILE_NAME);
            try (MemoryARW rwTx = Vm.getSmallMARWInstance(ff, path.$())) {
                if (rwTx.getInt(META_OFFSET_VERSION) > version - 1) {
                    rwTx.putInt(META_OFFSET_VERSION, version - 1);
                }
            }
        } finally {
            path.trimTo(pathLen);
        }
    }

    private void downgradeUpdateFileTo(FilesFacade ff, Path path) {
        path.trimTo(0).concat(root).concat(UPGRADE_FILE_NAME);
        if (ff.exists(path.$())) {
            try (MemoryCMARW rwTx = Vm.getSmallCMARWInstance(ff, path.$())) {
                rwTx.putInt(0, EngineMigration.VERSION_TBL_META_COMMIT_LAG - 1);
            }
        }
    }

    private FilesFacadeImpl failToWriteMetaOffset(final long metaOffsetVersion, final String filename) {
        return new FilesFacadeImpl() {
            private long metaFd = -1;

            @Override
            public long openRW(LPSZ name) {
                long fd = super.openRW(name);
                if (name.toString().contains(filename)) {
                    this.metaFd = fd;
                }

                return fd;
            }

            @Override
            public long write(long fd, long address, long len, long offset) {
                if (fd == metaFd && offset == metaOffsetVersion) {
                    return 0;
                }
                return super.write(fd, address, len, offset);
            }
        };
    }

    public static void assertRemoveUpgradeFile() {
        try (Path path = new Path()) {
            path.of(configuration.getRoot()).concat(TableUtils.UPGRADE_FILE_NAME).$();
            Assert.assertTrue(!FilesFacadeImpl.INSTANCE.exists(path) || FilesFacadeImpl.INSTANCE.remove(path));
        }
    }

    private static DateFormat getPartitionDateFmt(int partitionBy) {
        switch (partitionBy) {
            case PartitionBy.DAY:
                return fmtDay;
            case PartitionBy.MONTH:
                return fmtMonth;
            case PartitionBy.YEAR:
                return fmtYear;
            default:
                throw new UnsupportedOperationException("partition by " + partitionBy + " does not have date format");
        }
    }

    private void assertMigration(String query) throws SqlException, IOException {
        assertMigration(query, query, "/migration/assertMigrationPartitionsKept.zip");
    }

    private void assertMigration(String queryOld, String queryNew, String file) throws SqlException, IOException {
        CharSequence expected = executeSql(queryOld).toString();
        if (!queryOld.equals(queryNew)) {
            // if queries are different they must produce different results
            CharSequence expectedNewEquivalent = executeSql(queryNew).toString();
            Assert.assertNotEquals(expected, expectedNewEquivalent);
        }

        // There are no symbols, no partition, tx file is same. Downgrade version
        replaceDbContent(file);
        assertRemoveUpgradeFile();

        // Act
        new EngineMigration(engine, configuration).migrateEngineTo(ColumnType.VERSION);

        // Verify
        TestUtils.assertEquals(expected, executeSql(queryNew));

        // Second run of migration should not do anything
        new EngineMigration(engine, configuration).migrateEngineTo(ColumnType.VERSION);
        TestUtils.assertEquals(expected, executeSql(queryNew));

        // Third time, downgrade and migrate
        replaceDbContent(file);
        assertRemoveUpgradeFile();

        new EngineMigration(engine, configuration).migrateEngineTo(ColumnType.VERSION);
        TestUtils.assertEquals(expected, executeSql(queryNew));
    }

    private void downgradeTxFile(TableModel src) {
        engine.clear();
        downgradeMetaDataFile(src);

        try (Path path = new Path()) {
            path.of(root).concat(src.getName()).concat(TableUtils.META_FILE_NAME);
            FilesFacade ff = configuration.getFilesFacade();

            // Read current symbols list
            IntList symbolCounts = new IntList();
            path.trimTo(0).concat(root).concat(src.getName());
            LongList attachedPartitions = new LongList();
            try (TxReader txFile = new TxReader(ff, path, src.getPartitionBy())) {
                txFile.readUnchecked();

                for (int i = 0; i < txFile.getPartitionCount() - 1; i++) {
                    attachedPartitions.add(txFile.getPartitionTimestamp(i));
                    attachedPartitions.add(txFile.getPartitionSize(i));
                }
                txFile.readSymbolCounts(symbolCounts);
            }

            path.of(root).concat(src.getName()).concat(TXN_FILE_NAME);
            try (MemoryCMARW rwTx = Vm.getSmallCMARWInstance(ff, path.$())) {
                rwTx.toTop();
                rwTx.putInt(TX_STRUCT_UPDATE_1_OFFSET_MAP_WRITER_COUNT, symbolCounts.size());
                rwTx.skip(TX_STRUCT_UPDATE_1_OFFSET_MAP_WRITER_COUNT + 4);

                // Tx file used to have 4 bytes per symbol
                for (int i = 0; i < symbolCounts.size(); i++) {
                    rwTx.putInt(symbolCounts.getQuick(i));
                }

                // and stored removed partitions list
                rwTx.putInt(0);
            }

            // and have file _archive in each folder the file size except last partition
            if (src.getPartitionBy() != PartitionBy.NONE) {
                DateFormat partitionFmt = getPartitionDateFmt(src.getPartitionBy());
                StringSink sink = new StringSink();
                for (int i = 0; i < attachedPartitions.size() / 2; i++) {
                    long partitionTs = attachedPartitions.getQuick(i * 2);
                    long partitionSize = attachedPartitions.getQuick(i * 2 + 1);
                    sink.clear();
                    partitionFmt.format(partitionTs, null, null, sink);
                    path.trimTo(0).concat(root).concat(src.getName()).concat(sink).concat("_archive");
                    if (ff.exists(path.$())) {
                        ff.remove(path);
                    }
                    try (MemoryCMARW rwAr = Vm.getSmallCMARWInstance(ff, path.$())) {
                        rwAr.putLong(partitionSize);
                    }
                }
            }

            setMetadataVersion(src, ff, path, VERSION_TX_STRUCT_UPDATE_1);

            path.trimTo(0).concat(root).concat(UPGRADE_FILE_NAME);
            if (ff.exists(path.$())) {
                ff.remove(path.$());
            }
        }
    }

    private CharSequence executeSql(String sql) throws SqlException {
        TestUtils.printSql(
                compiler,
                sqlExecutionContext,
                sql,
                sink
        );
        return sink;
    }

    public static void replaceDbContent(String path) throws IOException {

        engine.releaseAllReaders();
        engine.releaseAllWriters();

        final byte[] buffer = new byte[1024 * 1024];
        URL resource = EngineMigrationTest.class.getResource(path);
        Assert.assertNotNull(resource);
        try (final InputStream is = EngineMigrationTest.class.getResourceAsStream(path)) {
            Assert.assertNotNull(is);
            try (ZipInputStream zip = new ZipInputStream(is)) {
                ZipEntry ze;
                while ((ze = zip.getNextEntry()) != null) {
                    final File dest = new File((String) root, ze.getName());
                    if (!ze.isDirectory()) {
                        copyInputStream(true, buffer, dest, zip);
                    }
                    zip.closeEntry();
                }
            }
        }
    }

    private static void copyInputStream(boolean force, byte[] buffer, File out, InputStream is) throws IOException {
        final boolean exists = out.exists();
        if (force || !exists) {
            File dir = out.getParentFile();
            Assert.assertTrue(dir.exists() || dir.mkdirs());
            try (FileOutputStream fos = new FileOutputStream(out)) {
                int n;
                while ((n = is.read(buffer, 0, buffer.length)) > 0) {
                    fos.write(buffer, 0, n);
                }
            }
        }
    }

    @Test
    public void testAllColumns() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "x_cols", PartitionBy.NONE)) {
                createPopulateTable(src.col("bool", ColumnType.BOOLEAN)
                                .col("byte", ColumnType.BYTE)
                                .col("short", ColumnType.SHORT)
                                .col("char", ColumnType.CHAR)
                                .col("int", ColumnType.INT)
                                .col("long", ColumnType.LONG)
                                .col("date", ColumnType.DATE)
                                .col("float", ColumnType.FLOAT)
                                .col("double", ColumnType.DOUBLE)
                                .col("string", ColumnType.STRING)
                                .col("long256", ColumnType.LONG256)
                                .col("symbol", ColumnType.SYMBOL).indexed(true, 4096)
                                .col("ts", ColumnType.TIMESTAMP).timestamp(),
                        100, "2020-01-01", 0
                );

                String query = "select * from x_cols";
                assertMigration(query);
            }
        });
    }

    @Test
    @Ignore
    public void testGenerateTablesForMigrationTest() throws Exception {
        assertMemoryLeak(() -> {

            SharedRandom.RANDOM.set(new Rnd());
            try (CairoEngine engine = new CairoEngine(configuration)) {
                // roll table id up
                for (int i = 0; i < 10; i++) {
                    engine.getNextTableId();
                }

                try (TableModel model = new TableModel(configuration, "y_416", PartitionBy.DAY).col("aaa", ColumnType.SYMBOL).timestamp()) {
                    CairoTestUtils.createTableWithVersion(model, 416);
                    downgradeTxFile(model);
                }

                try (TableModel model = new TableModel(configuration, "y_419", PartitionBy.DAY).col("aaa", ColumnType.SYMBOL).timestamp()) {
                    TableUtils.createTable(
                            model.getCairoCfg(),
                            model.getMem(),
                            model.getPath(),
                            model,
                            ColumnType.VERSION,
                            (int) engine.getNextTableId()
                    );
                }
            }

            try (TableModel src = new TableModel(configuration, "x_none", PartitionBy.NONE)) {
                createPopulateTable(
                        src.col("s1", ColumnType.SYMBOL).indexed(true, 4096)
                                .col("c1", ColumnType.INT)
                                .col("s2", ColumnType.SYMBOL)
                                .col("c2", ColumnType.LONG)
                                .col("ts", ColumnType.TIMESTAMP).timestamp(),
                        100, "2020-01-01", 0
                );
            }

            SharedRandom.RANDOM.set(new Rnd());
            try (TableModel src = new TableModel(configuration, "x_day", PartitionBy.DAY)) {
                createPopulateTable(
                        src.col("s1", ColumnType.SYMBOL).indexed(true, 4096)
                                .col("c1", ColumnType.INT)
                                .col("s2", ColumnType.SYMBOL)
                                .col("c2", ColumnType.LONG)
                                .col("ts", ColumnType.TIMESTAMP).timestamp(),
                        100, "2020-01-01", 10
                );
            }

            SharedRandom.RANDOM.set(new Rnd());
            try (TableModel src = new TableModel(configuration, "x_month", PartitionBy.MONTH)) {
                createPopulateTable(
                        src.col("s1", ColumnType.SYMBOL).indexed(true, 4096)
                                .col("c1", ColumnType.INT)
                                .col("s2", ColumnType.SYMBOL)
                                .col("c2", ColumnType.LONG)
                                .col("ts", ColumnType.TIMESTAMP).timestamp(),
                        100, "2020-01-01", 3
                );
            }

            SharedRandom.RANDOM.set(new Rnd());
            try (TableModel src = new TableModel(configuration, "x_year", PartitionBy.YEAR)) {
                createPopulateTable(
                        src.col("s1", ColumnType.SYMBOL).indexed(true, 4096)
                                .col("c1", ColumnType.INT)
                                .col("s2", ColumnType.SYMBOL)
                                .col("c2", ColumnType.LONG)
                                .col("ts", ColumnType.TIMESTAMP).timestamp(),
                        100, "2020-01-01", 3
                );
            }

            SharedRandom.RANDOM.set(new Rnd());
            try (TableModel src = new TableModel(configuration, "x_cols", PartitionBy.NONE)) {
                createPopulateTable(src.col("bool", ColumnType.BOOLEAN)
                                .col("byte", ColumnType.BYTE)
                                .col("short", ColumnType.SHORT)
                                .col("char", ColumnType.CHAR)
                                .col("int", ColumnType.INT)
                                .col("long", ColumnType.LONG)
                                .col("date", ColumnType.DATE)
                                .col("float", ColumnType.FLOAT)
                                .col("double", ColumnType.DOUBLE)
                                .col("string", ColumnType.STRING)
                                .col("long256", ColumnType.LONG256)
                                .col("symbol", ColumnType.SYMBOL).indexed(true, 4096)
                                .col("ts", ColumnType.TIMESTAMP).timestamp(),
                        100, "2020-01-01", 0
                );
            }
        });
    }

}
