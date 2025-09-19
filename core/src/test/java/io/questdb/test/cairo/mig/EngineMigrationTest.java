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

package io.questdb.test.cairo.mig;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.CairoTable;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.MetadataCacheReader;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.TxWriter;
import io.questdb.cairo.mig.EngineMigration;
import io.questdb.griffin.SqlException;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class EngineMigrationTest extends AbstractCairoTest {

    public static void replaceDbContent(String path) throws IOException {
        engine.releaseAllReaders();
        engine.releaseAllWriters();
        engine.releaseInactive();
        engine.closeNameRegistry();

        final byte[] buffer = new byte[1024 * 1024];
        URL resource = EngineMigrationTest.class.getResource(path);
        Assert.assertNotNull(resource);
        try (final InputStream is = EngineMigrationTest.class.getResourceAsStream(path)) {
            Assert.assertNotNull(is);
            try (ZipInputStream zip = new ZipInputStream(is)) {
                ZipEntry ze;
                while ((ze = zip.getNextEntry()) != null) {
                    final File dest = new File(root, ze.getName());
                    if (!ze.isDirectory()) {
                        copyInputStream(buffer, dest, zip);
                    }
                    zip.closeEntry();
                }
            }
        }

        engine.reloadTableNames();
    }

    @BeforeClass
    public static void setUpStatic() throws Exception {
        AbstractCairoTest.setUpStatic();
        configOverrideMangleTableDirNames(false);
    }

    @Test
    public void test416() throws Exception {
        doMigration("/migration/data_416.zip", false, false, false, false, false, false);
    }

    @Test
    public void test417() throws Exception {
        doMigration("/migration/data_417.zip", true, false, false, false, false, false);
    }

    @Test
    public void test419() throws Exception {
        doMigration("/migration/data_419.zip", true, false, false, false, false, true);
    }

    @Test
    public void test420() throws Exception {
        doMigration("/migration/data_420.zip", true, false, false, false, false, true);
    }

    @Test
    public void test421() throws Exception {
        doMigration("/migration/data_421.zip", true, true, false, false, false, true);
    }

    @Test
    public void test422() throws Exception {
        doMigration("/migration/data_422.zip", true, true, false, false, false, true);
    }

    @Test
    public void test423() throws Exception {
        doMigration("/migration/data_423.zip", true, true, false, false, false, true);
    }

    @Test
    public void test424() throws Exception {
        doMigration("/migration/data_424.zip", true, true, true, false, false, true);
    }

    @Test
    public void test425() throws Exception {
        doMigration("/migration/data_425.zip", true, true, true, true, false, true);
    }

    @Test
    public void test426() throws Exception {
        doMigration("/migration/data_426.zip", true, true, true, true, true, false);
    }

    @Test
    @Ignore
    public void testGenerateTables() throws Exception {
        assertMemoryLeak(() -> {
            generateMigrationTables();
            engine.releaseAllWriters();
            assertData(true, true, true, true);
        });
    }

    @Test
    public void testMig702HandlesMissingTxn() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_REPEAT_MIGRATION_FROM_VERSION, 426);

            execute("create table abc (a int, ts timestamp) timestamp(ts) partition by DAY WAL");
            execute("create table def (a int, ts timestamp) timestamp(ts) partition by DAY WAL");
            TableToken tokenAbc = engine.verifyTableName("abc");
            TableToken tokenDef = engine.verifyTableName("def");

            engine.releaseInactive();

            CairoConfiguration config = engine.getConfiguration();
            FilesFacade ff = config.getFilesFacade();

            // Make abc _txn too short
            Path abcTxnPath = Path.getThreadLocal(config.getDbRoot()).concat(tokenAbc).concat(TableUtils.TXN_FILE_NAME);
            long fd = TableUtils.openRW(ff, abcTxnPath.$(), LOG, config.getWriterFileOpenOpts());
            Assert.assertTrue(ff.truncate(fd, 50));
            ff.close(fd);

            // Mess, run migration and check
            TestUtils.messTxnUnallocated(ff, Path.getThreadLocal(config.getDbRoot()), new Rnd(), tokenDef);
            EngineMigration.migrateEngineTo(engine, ColumnType.VERSION, ColumnType.MIGRATION_VERSION, true);
            checkTxnFile(ff, config, tokenDef, 0, 0, Long.MAX_VALUE, Long.MIN_VALUE);

            // Remove _txn file for table abc
            abcTxnPath = Path.getThreadLocal(config.getDbRoot()).concat(tokenAbc).concat(TableUtils.TXN_FILE_NAME);
            Assert.assertTrue(ff.removeQuiet(abcTxnPath.$()));

            // Mess, run migration and check
            TestUtils.messTxnUnallocated(ff, Path.getThreadLocal(config.getDbRoot()), new Rnd(), tokenDef);
            EngineMigration.migrateEngineTo(engine, ColumnType.VERSION, ColumnType.MIGRATION_VERSION, true);
            checkTxnFile(ff, config, tokenDef, 0, 0, Long.MAX_VALUE, Long.MIN_VALUE);
        });
    }

    @Test
    public void testMig702NonRepeatable() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_REPEAT_MIGRATION_FROM_VERSION, -1);
            // Run migration
            EngineMigration.migrateEngineTo(engine, ColumnType.VERSION, ColumnType.MIGRATION_VERSION, true);

            execute("create table abc (a int, ts timestamp) timestamp(ts) partition by DAY WAL");
            TableToken token = engine.verifyTableName("abc");
            CairoConfiguration config = engine.getConfiguration();

            TestUtils.messTxnUnallocated(
                    config.getFilesFacade(),
                    Path.getThreadLocal(config.getDbRoot()),
                    new Rnd(123, 123),
                    token
            );

            // Run migration
            EngineMigration.migrateEngineTo(engine, ColumnType.VERSION, ColumnType.MIGRATION_VERSION, false);

            // Check txn file is upgraded
            try (TxReader txReader = new TxReader(config.getFilesFacade())) {
                Path p = Path.getThreadLocal(config.getDbRoot());
                txReader.ofRO(p.concat(token).concat(TableUtils.TXN_FILE_NAME).$(), ColumnType.TIMESTAMP, PartitionBy.DAY);
                txReader.unsafeLoadAll();

                Assert.assertNotEquals(0, txReader.getLagRowCount());
                Assert.assertNotEquals(0, txReader.getLagTxnCount());
                Assert.assertNotEquals(0L, txReader.getLagMinTimestamp());
                Assert.assertNotEquals(0L, txReader.getLagMaxTimestamp());
            }
        });
    }

    @Test
    public void testMig702Repeatable() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_REPEAT_MIGRATION_FROM_VERSION, 426);

            execute("create table abc (a int, ts timestamp) timestamp(ts) partition by DAY WAL");
            TableToken token = engine.verifyTableName("abc");

            CairoConfiguration config = engine.getConfiguration();
            try (TxWriter txWriter = new TxWriter(config.getFilesFacade(), config)) {
                Path p = Path.getThreadLocal(config.getDbRoot());
                txWriter.ofRW(p.concat(token).concat(TableUtils.TXN_FILE_NAME).$(), ColumnType.TIMESTAMP, PartitionBy.DAY);

                txWriter.setLagRowCount(100);
                txWriter.setLagTxnCount(1);
                txWriter.setLagMinTimestamp(MicrosTimestampDriver.floor("2022-02-24"));
                txWriter.setLagMaxTimestamp(MicrosTimestampDriver.floor("2023-03-20"));

                txWriter.commit(new ObjList<>());
            }

            // Run migration
            EngineMigration.migrateEngineTo(engine, ColumnType.VERSION, ColumnType.MIGRATION_VERSION, false);

            // Check txn file not upgraded
            checkTxnFile(config.getFilesFacade(), config, token, 100, 1, MicrosTimestampDriver.floor("2022-02-24"), MicrosTimestampDriver.floor("2023-03-20"));

            TestUtils.messTxnUnallocated(
                    config.getFilesFacade(),
                    Path.getThreadLocal(config.getDbRoot()),
                    new Rnd(),
                    token
            );

            // Run migration
            EngineMigration.migrateEngineTo(engine, ColumnType.VERSION, ColumnType.MIGRATION_VERSION, false);

            // Check txn file is upgraded
            checkTxnFile(config.getFilesFacade(), config, token, 0, 0, Long.MAX_VALUE, Long.MIN_VALUE);
        });
    }

    private static void assertCairoMetadata(CharSequence expected, String tableName, boolean ignoreMaxLag) {
        try (MetadataCacheReader ro = engine.getMetadataCache().readLock()) {
            TableToken token = engine.verifyTableName(tableName);
            CairoTable table = ro.getTable(token);
            Assert.assertNotNull(table);
            sink.clear();
            table.toSink(sink);
            expected = expected.toString().replace("id=1", "id=" + token.getTableId());
            CharSequence actual = sink;
            if (table.getPartitionBy() == PartitionBy.NONE || ignoreMaxLag) {
                // Some older files contains o3MaxLag as 0 for no good reason for non-partitioned tables
                actual = sink.toString().replace("o3MaxLag=0,", "o3MaxLag=300000000,");
            }
            TestUtils.assertEquals(expected, actual);
        }
    }

    private static void assertShort2TableMeta(boolean ignoreMaxLag, String... params) {
        for (int i = 0; i < params.length; i += 2) {
            String tableName = params[i];
            String partitionBy = params[i + 1];
            assertCairoMetadata(
                    "CairoTable [name=" + tableName + ", id=1, directoryName=" + tableName + ", hasDedup=false, isSoftLink=false, metadataVersion=2, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=" + partitionBy + ", timestampIndex=2, timestampName=ts, ttlHours=0, walEnabled=false, columnCount=5]\n" +
                            "\t\tCairoColumn [name=x, position=0, type=LONG, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=0]\n" +
                            "\t\tCairoColumn [name=m, position=1, type=SYMBOL, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=true, symbolCapacity=128, isIndexed=true, indexBlockCapacity=256, writerIndex=1]\n" +
                            "\t\tCairoColumn [name=ts, position=2, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=2]\n" +
                            "\t\tCairoColumn [name=день, position=3, type=SYMBOL, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=true, symbolCapacity=128, isIndexed=false, indexBlockCapacity=256, writerIndex=3]\n" +
                            "\t\tCairoColumn [name=str, position=4, type=STRING, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=256, writerIndex=4]",
                    tableName,
                    ignoreMaxLag
            );
        }
    }

    private static void assertShortTableMeta(boolean ignoreMaxLag, String... params) {
        for (int i = 0; i < params.length; i += 2) {
            String tableName = params[i];
            String partitionBy = params[i + 1];
            assertCairoMetadata(
                    "CairoTable [name=" + tableName + ", id=1, directoryName=" + tableName + ", hasDedup=false, isSoftLink=false, metadataVersion=1, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=" + partitionBy + ", timestampIndex=2, timestampName=ts, ttlHours=0, walEnabled=false, columnCount=4]\n" +
                            "\t\tCairoColumn [name=x, position=0, type=LONG, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=0]\n" +
                            "\t\tCairoColumn [name=m, position=1, type=SYMBOL, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=true, symbolCapacity=128, isIndexed=true, indexBlockCapacity=256, writerIndex=1]\n" +
                            "\t\tCairoColumn [name=ts, position=2, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=2]\n" +
                            "\t\tCairoColumn [name=y, position=3, type=LONG, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=256, writerIndex=3]",
                    tableName,
                    ignoreMaxLag
            );
        }
    }

    private static void assertShortWalTableMeta(boolean ignoreMaxLag, String... params) {
        for (int i = 0; i < params.length; i += 2) {
            String tableName = params[i];
            String partitionBy = params[i + 1];
            assertCairoMetadata(
                    "CairoTable [name=" + tableName + ", id=1, directoryName=" + tableName + "~14, hasDedup=false, isSoftLink=false, metadataVersion=2, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=" + partitionBy + ", timestampIndex=2, timestampName=ts, ttlHours=0, walEnabled=true, columnCount=5]\n" +
                            "\t\tCairoColumn [name=x, position=0, type=LONG, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=0]\n" +
                            "\t\tCairoColumn [name=m, position=1, type=SYMBOL, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=true, symbolCapacity=128, isIndexed=true, indexBlockCapacity=256, writerIndex=1]\n" +
                            "\t\tCairoColumn [name=ts, position=2, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=2]\n" +
                            "\t\tCairoColumn [name=день, position=3, type=SYMBOL, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=true, symbolCapacity=128, isIndexed=false, indexBlockCapacity=256, writerIndex=3]\n" +
                            "\t\tCairoColumn [name=str, position=4, type=STRING, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=256, writerIndex=4]",
                    tableName,
                    ignoreMaxLag
            );
        }
    }

    private static void assertStandardTable(boolean ignoreMaxLag, String tableName, String partitionBy) {
        String columns = "\t\tCairoColumn [name=a, position=0, type=BYTE, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=0]\n" +
                "\t\tCairoColumn [name=b, position=1, type=CHAR, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=1]\n" +
                "\t\tCairoColumn [name=c, position=2, type=SHORT, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=2]\n" +
                "\t\tCairoColumn [name=d, position=3, type=INT, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=3]\n" +
                "\t\tCairoColumn [name=e, position=4, type=LONG, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=4]\n" +
                "\t\tCairoColumn [name=f, position=5, type=FLOAT, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=5]\n" +
                "\t\tCairoColumn [name=g, position=6, type=DOUBLE, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=6]\n" +
                "\t\tCairoColumn [name=h, position=7, type=DATE, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=7]\n" +
                "\t\tCairoColumn [name=i, position=8, type=TIMESTAMP, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=8]\n" +
                "\t\tCairoColumn [name=j, position=9, type=STRING, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=9]\n" +
                "\t\tCairoColumn [name=k, position=10, type=SYMBOL, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=true, symbolCapacity=128, isIndexed=false, indexBlockCapacity=0, writerIndex=10]\n" +
                "\t\tCairoColumn [name=l, position=11, type=BOOLEAN, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=11]\n" +
                "\t\tCairoColumn [name=m, position=12, type=SYMBOL, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=true, symbolCapacity=128, isIndexed=true, indexBlockCapacity=256, writerIndex=12]\n" +
                "\t\tCairoColumn [name=n, position=13, type=LONG256, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=13]\n" +
                "\t\tCairoColumn [name=o, position=14, type=BINARY, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=14]\n" +
                "\t\tCairoColumn [name=ts, position=15, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=15]";

        assertCairoMetadata(
                "CairoTable [name=" + tableName + ", id=1, directoryName=" + tableName + ", hasDedup=false, isSoftLink=false, metadataVersion=0, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=" + partitionBy + ", timestampIndex=15, timestampName=ts, ttlHours=0, walEnabled=false, columnCount=16]\n" +
                        columns,
                tableName,
                ignoreMaxLag
        );
    }

    private static void assertTableMeta(boolean ignoreMaxLag, String... params) {
        for (int i = 0; i < params.length; i += 2) {
            String tableName = params[i];
            String partitionBy = params[i + 1];
            if (partitionBy.equals("NONE_NTS")) {
                assertCairoMetadata(
                        "CairoTable [name=" + tableName + ", id=1, directoryName=" + tableName + ", hasDedup=false, isSoftLink=false, metadataVersion=0, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=NONE, timestampIndex=-1, timestampName=, ttlHours=0, walEnabled=false, columnCount=15]\n" +
                                "\t\tCairoColumn [name=a, position=0, type=BYTE, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=0]\n" +
                                "\t\tCairoColumn [name=b, position=1, type=CHAR, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=1]\n" +
                                "\t\tCairoColumn [name=c, position=2, type=SHORT, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=2]\n" +
                                "\t\tCairoColumn [name=d, position=3, type=INT, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=3]\n" +
                                "\t\tCairoColumn [name=e, position=4, type=LONG, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=4]\n" +
                                "\t\tCairoColumn [name=f, position=5, type=FLOAT, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=5]\n" +
                                "\t\tCairoColumn [name=g, position=6, type=DOUBLE, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=6]\n" +
                                "\t\tCairoColumn [name=h, position=7, type=DATE, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=7]\n" +
                                "\t\tCairoColumn [name=i, position=8, type=TIMESTAMP, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=8]\n" +
                                "\t\tCairoColumn [name=j, position=9, type=STRING, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=9]\n" +
                                "\t\tCairoColumn [name=k, position=10, type=SYMBOL, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=true, symbolCapacity=128, isIndexed=false, indexBlockCapacity=0, writerIndex=10]\n" +
                                "\t\tCairoColumn [name=l, position=11, type=BOOLEAN, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=11]\n" +
                                "\t\tCairoColumn [name=m, position=12, type=SYMBOL, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=true, symbolCapacity=128, isIndexed=true, indexBlockCapacity=256, writerIndex=12]\n" +
                                "\t\tCairoColumn [name=n, position=13, type=LONG256, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=13]\n" +
                                "\t\tCairoColumn [name=o, position=14, type=BINARY, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=14]",
                        tableName,
                        ignoreMaxLag
                );
            } else {
                assertStandardTable(ignoreMaxLag, tableName, partitionBy);
            }
        }
    }

    private static void checkTxnFile(
            FilesFacade ff,
            CairoConfiguration config,
            TableToken tokenDef,
            int rowCount,
            int lagTxnCount,
            long maxValue,
            long minValue
    ) {
        // Check txn file is upgraded
        try (TxReader txReader = new TxReader(ff)) {
            Path p = Path.getThreadLocal(config.getDbRoot());
            txReader.ofRO(p.concat(tokenDef).concat(TableUtils.TXN_FILE_NAME).$(), ColumnType.TIMESTAMP, PartitionBy.DAY);
            txReader.unsafeLoadAll();

            Assert.assertEquals(rowCount, txReader.getLagRowCount());
            Assert.assertEquals(lagTxnCount, txReader.getLagTxnCount());
            Assert.assertEquals(maxValue, txReader.getLagMinTimestamp());
            Assert.assertEquals(minValue, txReader.getLagMaxTimestamp());
        }
    }

    private static void copyInputStream(byte[] buffer, File out, InputStream is) throws IOException {
        File dir = out.getParentFile();
        if (!dir.exists()) {
            Assert.assertTrue(dir.mkdirs());
        }
        try (FileOutputStream fos = new FileOutputStream(out)) {
            int n;
            while ((n = is.read(buffer, 0, buffer.length)) > 0) {
                fos.write(buffer, 0, n);
            }
        }
    }

    private static void createTableWithColumnTops(String createTable, String tableName) throws SqlException {
        execute(createTable);
        execute("alter table " + tableName + " add column день symbol");
        execute("alter table " + tableName + " add column str string");
        execute(
                "insert into " + tableName + " " +
                        "select " +
                        " x" +
                        ", rnd_symbol('a', 'b', 'c', null) m" +
                        ", timestamp_sequence('1970-01-05T02:30', " + Micros.HOUR_MICROS + "L) ts" +
                        ", rnd_symbol('a', 'b', 'c', null)" +
                        ", rnd_str()" +
                        " from long_sequence(10),"
        );
        execute(
                "insert into " + tableName + " " +
                        "select " +
                        " x" +
                        ", rnd_symbol('a', 'b', 'c', null) m" +
                        ", timestamp_sequence('1970-01-01T01:30', " + Micros.HOUR_MICROS + "L) ts" +
                        ", rnd_symbol('a', 'b', 'c', null)" +
                        ", rnd_str()" +
                        " from long_sequence(36)"
        );
    }

    private static void insertData(String tableName) throws SqlException {
        Rnd rnd = sqlExecutionContext.getRandom();
        long seed0 = rnd.getSeed0();
        long seed1 = rnd.getSeed1();

        execute(
                "insert into " + tableName + " " +
                        "select " +
                        " x" +
                        ", rnd_symbol('a', 'b', 'c', null) m" +
                        ", timestamp_sequence('1970-01-05T04:25', " + Micros.HOUR_MICROS + "L) ts" +
                        ", rnd_symbol('a', 'b', 'c', null)" +
                        ", rnd_str()" +
                        " from long_sequence(10),"
        );
        execute(
                "insert into " + tableName + " " +
                        "select " +
                        " x" +
                        ", rnd_symbol('a', 'b', 'c', null) m" +
                        ", timestamp_sequence('1970-01-01T01:27', " + Micros.HOUR_MICROS + "L) ts" +
                        ", rnd_symbol('a', 'b', 'c', null)" +
                        ", rnd_str()" +
                        " from long_sequence(36)"
        );

        rnd.reset(seed0, seed1);
    }

    @NotNull
    private String appendCommonColumns() {
        return " rnd_byte() a," +
                " rnd_char() b," +
                " rnd_short() c," +
                " rnd_int(-77888, 999001, 2) d," + // ensure we have nulls
                " rnd_long(-100000, 100000, 2) e," + // ensure we have nulls
                " rnd_float(2) f," + // ensure we have nulls
                " rnd_double(2) g," + // ensure we have nulls
                " rnd_date(199999999, 399999999999, 2) h," + // ensure we have nulls
                " cast(rnd_long(-7999999, 800000, 10) as timestamp)  i," + // ensure we have nulls
                " rnd_str(4,5,2) j," +
                " rnd_symbol('newsymbol1','newsymbol12', null) k," +
                " rnd_boolean() l," +
                " rnd_symbol('newsymbol1','newsymbol12', null) m," +
                " rnd_long256() n," +
                " rnd_bin(2,10, 2) o";
    }

    private void appendData(boolean withColTopO3, boolean withWalTxn) throws SqlException {
        engine.releaseAllReaders();
        engine.releaseAllWriters();

        // Insert some data
        execute("insert into t_year select " +
                appendCommonColumns() +
                ", timestamp_sequence('2021-01-01', 200000000L) ts" +
                " from long_sequence(5)");

        // Insert same data to have O3 append tested
        execute("insert into t_year select " +
                appendCommonColumns() +
                ", timestamp_sequence('2020-01-01', 200000000L) ts" +
                " from long_sequence(5)");

        if (withColTopO3) {
            insertData("t_col_top_ooo_day");
        }

        if (withWalTxn) {
            insertData("t_col_top_ooo_day_wal");
            drainWalQueue();
        }
    }

    private void assertAppendedData(boolean withColTopO3, boolean withWalTxn) throws SqlException {
        engine.releaseAllReaders();
        engine.releaseAllWriters();
        assertSql(
                "a\tb\tc\td\te\tf\tg\th\ti\tj\tk\tl\tm\tn\to\tts\n" +
                        "109\tP\t-12455\t263934\t49960\t0.679264\t0.09831693674866282\t1977-08-08T19:44:03.856Z\t1969-12-31T23:59:55.049605Z\tHQBJP\tc\tfalse\taaa\t0xbe15104d1d36d615cac36ab298393e52b06836c8abd67a44787ce11d6fc88eab\t00000000 37 58 2c 0d b0 d0 9c 57 02 75\t2096-10-02T07:10:00.000000Z\n" +
                        "73\tB\t-1271\t-47644\t4999\t0.858377\t0.12392055368261845\t1975-06-03T19:26:19.012Z\t1969-12-31T23:59:55.604499Z\t\tc\ttrue\taaa\t0x4f669e76b0311ac3438ec9cc282caa7043a05a3edd41f45aa59f873d1c729128\t00000000 34 01 0e 4d 2b 00 fa 34\t2103-02-04T02:43:20.000000Z\n" +
                        "83\tL\t-32289\t127321\t40837\t0.1335336\t0.515824820198022\t\t1969-12-31T23:59:53.582959Z\tKFMO\taaa\tfalse\t\t0x8131875cd498c4b888762e985137f4e843b8167edcd59cf345c105202f875495\t\t2109-06-06T22:16:40.000000Z\n" +
                        "51\tS\t-28311\tnull\t-72973\t0.5956609\t0.20897460269739654\t1973-03-28T21:58:08.545Z\t1969-12-31T23:59:54.332988Z\t\tc\tfalse\taaa\t0x50113ffcc219fb1a9bc4f6389de1764097e7bcd897ae8a54aa2883a41581608f\t00000000 83 94 b5\t2115-10-08T17:50:00.000000Z\n" +
                        "49\tN\t-11147\t392567\t-9830\t0.5247657\t0.1095692511246914\t\t1969-12-31T23:59:56.849475Z\tIFBE\tc\tfalse\taaa\t0x36055358bd232c9d775e2e80754e5fcda2353931c7033ad5c38c294e9227895a\t\t2122-02-08T13:23:20.000000Z\n" +
                        "57\tI\t-22903\t874980\t-28069\tnull\t0.016793228004843286\t1975-09-29T05:10:33.275Z\t1969-12-31T23:59:55.690794Z\tBZSM\t\ttrue\tc\t0xa2c84382c65eb07087cf6cb291b2c3e7a9ffe8560d2cec518dea50b88b87fe43\t00000000 b9 c4 18 2b aa 3d\t2128-06-11T08:56:40.000000Z\n" +
                        "127\tW\t-16809\t288758\t-22272\t0.05352205\t0.5855510665931698\t\t1969-12-31T23:59:52.689490Z\t\taaa\tfalse\tc\t0x918ae2d78481070577c7d4c3a758a5ea3dd771714ac964ab4b350afc9b599b28\t\t2134-10-13T04:30:00.000000Z\n" +
                        "20\tM\t-7043\t251501\t-85499\t0.94033486\t0.9135840078861264\t1977-05-12T19:20:06.113Z\t1969-12-31T23:59:54.045277Z\tHOXL\tbbbbbb\tfalse\tbbbbbb\t0xc3b0de059fff72dbd7b99af08ac0d1cddb2990725a3338e377155edb531cb644\t\t2141-02-13T00:03:20.000000Z\n" +
                        "26\tG\t-24830\t-56840\t-32956\t0.8282491\t0.017280895313585898\t1982-07-16T03:52:53.454Z\t1969-12-31T23:59:54.115165Z\tJEJH\taaa\tfalse\tbbbbbb\t0x16f70de9c6af11071d35d9faec5d18fd1cf3bbbc825b72a92ecb8ff0286bf649\t00000000 c2 62 f8 53 7d 05 65\t2147-06-16T19:36:40.000000Z\n" +
                        "127\tY\t19592\t224361\t37963\t0.6930264\t0.006817672510656014\t1975-11-29T09:47:45.706Z\t1969-12-31T23:59:56.186242Z\t\t\ttrue\taaa\t0x88926dd483caaf4031096402997f21c833b142e887fa119e380dc9b54493ff70\t00000000 23 c3 9d 75 26 f2 0d b5 7a 3f\t2153-10-17T15:10:00.000000Z\n",
                "select * FROM t_year LIMIT -10"
        );

        if (withColTopO3) {
            assertSql("x\tm\tts\tдень\tstr\n" +
                    "1\t\t1970-01-01T01:27:00.000000Z\ta\tTLQZSLQ\n" +
                    "6\tc\t1970-01-01T06:30:00.000000Z\ta\tSFCI\n" +
                    "12\ta\t1970-01-01T12:30:00.000000Z\ta\tJNOXB\n" +
                    "14\t\t1970-01-01T14:30:00.000000Z\ta\tLJYFXSBNVN\n" +
                    "16\tb\t1970-01-01T16:30:00.000000Z\ta\tTPUL\n" +
                    "19\tb\t1970-01-01T19:27:00.000000Z\ta\tTZODWKOCPF\n" +
                    "21\tb\t1970-01-01T21:30:00.000000Z\ta\tGQWSZMUMXM\n" +
                    "24\t\t1970-01-02T00:30:00.000000Z\ta\tNTPYXUB\n" +
                    "31\ta\t1970-01-02T07:27:00.000000Z\ta\tGFI\n" +
                    "32\ta\t1970-01-02T08:27:00.000000Z\ta\tVZWEV\n" +
                    "33\tb\t1970-01-02T09:30:00.000000Z\ta\tFLNGCEFBTD\n" +
                    "34\tb\t1970-01-02T10:30:00.000000Z\ta\tTIGUTKI\n" +
                    "35\t\t1970-01-02T11:27:00.000000Z\ta\tPTYXYGYFUX\n" +
                    "1\t\t1970-01-05T02:30:00.000000Z\ta\tHQJHN\n" +
                    "4\tc\t1970-01-05T05:30:00.000000Z\ta\tXRGUOXFH\n" +
                    "5\t\t1970-01-05T06:30:00.000000Z\ta\tFVFFOB\n" +
                    "7\tb\t1970-01-05T10:25:00.000000Z\ta\tHFLPBNH\n" +
                    "9\tc\t1970-01-05T10:30:00.000000Z\ta\tLEQD\n" +
                    "8\t\t1970-01-05T11:25:00.000000Z\ta\tCCNGTNLE\n" +
                    "10\t\t1970-01-05T11:30:00.000000Z\ta\tKNHV\n" +
                    "9\ta\t1970-01-05T12:25:00.000000Z\ta\tHIUG\n", "t_col_top_ooo_day where день = 'a'"
            );
        }

        if (withWalTxn) {
            assertSql("x\tm\tts\tдень\tstr\n" +
                    "3\tc\t1970-01-05T04:30:00.000000Z\ta\tBLJTFSDQIE\n" +
                    "2\ta\t1970-01-05T05:25:00.000000Z\tc\tXHFVWSWSR\n" +
                    "4\t\t1970-01-05T05:30:00.000000Z\ta\tNEL\n" +
                    "3\tc\t1970-01-05T06:25:00.000000Z\t\tFCLTJC\n" +
                    "5\tc\t1970-01-05T06:30:00.000000Z\t\tGOFBJEB\n" +
                    "4\tc\t1970-01-05T07:25:00.000000Z\tb\tNTO\n" +
                    "6\tc\t1970-01-05T07:30:00.000000Z\tb\tKPXZEW\n" +
                    "5\tb\t1970-01-05T08:25:00.000000Z\tc\tKLGMXS\n" +
                    "7\ta\t1970-01-05T08:30:00.000000Z\ta\tFWMLBWUYKD\n" +
                    "6\tb\t1970-01-05T09:25:00.000000Z\t\tYOPHNIMYFF\n" +
                    "8\tb\t1970-01-05T09:30:00.000000Z\ta\tYQZ\n" +
                    "7\tb\t1970-01-05T10:25:00.000000Z\ta\tHFLPBNH\n" +
                    "9\tb\t1970-01-05T10:30:00.000000Z\tb\tOQMEYOJYZ\n" +
                    "8\t\t1970-01-05T11:25:00.000000Z\ta\tCCNGTNLE\n" +
                    "10\tc\t1970-01-05T11:30:00.000000Z\tb\tGQY\n" +
                    "9\ta\t1970-01-05T12:25:00.000000Z\ta\tHIUG\n" +
                    "10\ta\t1970-01-05T13:25:00.000000Z\t\tRZLCBDMIGQ\n", "t_col_top_ooo_day_wal where ts > '1970-01-05T04:25'"
            );
        }
    }

    private void assertColTops() throws SqlException {
        String part1Expected = "x\tm\tts\ty\n" +
                "1\ta\t1970-01-01T00:03:20.000000Z\tnull\n" +
                "2\tb\t1970-08-20T11:36:40.000000Z\tnull\n" +
                "3\tc\t1971-04-08T23:10:00.000000Z\tnull\n" +
                "4\tc\t1971-11-26T10:43:20.000000Z\tnull\n" +
                "5\tb\t1972-07-14T22:16:40.000000Z\tnull\n" +
                "6\t\t1973-03-03T09:50:00.000000Z\tnull\n" +
                "7\tc\t1973-10-20T21:23:20.000000Z\tnull\n" +
                "8\t\t1974-06-09T08:56:40.000000Z\tnull\n" +
                "9\tc\t1975-01-26T20:30:00.000000Z\tnull\n" +
                "10\tc\t1975-09-15T08:03:20.000000Z\tnull\n" +
                "11\tb\t1976-05-03T19:36:40.000000Z\tnull\n" +
                "12\tb\t1976-12-21T07:10:00.000000Z\tnull\n" +
                "13\tb\t1977-08-09T18:43:20.000000Z\tnull\n" +
                "14\t\t1978-03-29T06:16:40.000000Z\tnull\n" +
                "15\tb\t1978-11-15T17:50:00.000000Z\tnull\n";

        String part2Expected = "x\tm\tts\ty\n" +
                "16\te\t1979-07-05T05:23:20.000000Z\t16\n" +
                "17\td\t1980-02-21T16:56:40.000000Z\t17\n" +
                "18\t\t1980-10-10T04:30:00.000000Z\t18\n" +
                "19\te\t1981-05-29T16:03:20.000000Z\t19\n" +
                "20\te\t1982-01-16T03:36:40.000000Z\t20\n" +
                "21\tf\t1982-09-04T15:10:00.000000Z\t21\n" +
                "22\tf\t1983-04-24T02:43:20.000000Z\t22\n" +
                "23\te\t1983-12-11T14:16:40.000000Z\t23\n" +
                "24\td\t1984-07-30T01:50:00.000000Z\t24\n" +
                "25\tf\t1985-03-18T13:23:20.000000Z\t25\n" +
                "26\te\t1985-11-05T00:56:40.000000Z\t26\n" +
                "27\t\t1986-06-24T12:30:00.000000Z\t27\n" +
                "28\te\t1987-02-11T00:03:20.000000Z\t28\n" +
                "29\t\t1987-09-30T11:36:40.000000Z\t29\n" +
                "30\te\t1988-05-18T23:10:00.000000Z\t30\n";

        assertSql(part1Expected, "t_col_top_year where y = null");
        assertSql(part2Expected, "t_col_top_year where y != null");

        assertSql(part1Expected, "t_col_top_none where y = null");
        assertSql(part2Expected, "t_col_top_none where y != null");

        assertSql("x\tm\tts\tдень\n" +
                "1\tc\t1970-01-01T00:33:20.000000Z\tnull\n" +
                "2\tb\t1970-01-01T06:06:40.000000Z\tnull\n" +
                "3\tb\t1970-01-01T11:40:00.000000Z\tnull\n" +
                "4\tc\t1970-01-01T17:13:20.000000Z\tnull\n" +
                "5\tc\t1970-01-01T22:46:40.000000Z\tnull\n" +
                "6\tc\t1970-01-02T04:20:00.000000Z\tnull\n" +
                "7\tb\t1970-01-02T09:53:20.000000Z\tnull\n" +
                "8\t\t1970-01-02T15:26:40.000000Z\tnull\n" +
                "9\t\t1970-01-02T21:00:00.000000Z\tnull\n" +
                "10\tc\t1970-01-03T02:33:20.000000Z\tnull\n" +
                "11\tb\t1970-01-03T08:06:40.000000Z\tnull\n" +
                "12\tb\t1970-01-03T13:40:00.000000Z\tnull\n" +
                "13\tb\t1970-01-03T19:13:20.000000Z\tnull\n" +
                "14\tb\t1970-01-04T00:46:40.000000Z\tnull\n" +
                "15\t\t1970-01-04T06:20:00.000000Z\tnull\n", "t_col_top_день where день = null"
        );

        assertSql("x\tm\tts\tдень\n" +
                "16\te\t1970-01-03T07:36:40.000000Z\t16\n" +
                "17\tf\t1970-01-03T08:10:00.000000Z\t17\n" +
                "18\te\t1970-01-03T08:43:20.000000Z\t18\n" +
                "19\t\t1970-01-03T09:16:40.000000Z\t19\n" +
                "20\te\t1970-01-03T09:50:00.000000Z\t20\n" +
                "21\td\t1970-01-03T10:23:20.000000Z\t21\n" +
                "22\td\t1970-01-03T10:56:40.000000Z\t22\n" +
                "23\tf\t1970-01-03T11:30:00.000000Z\t23\n" +
                "24\t\t1970-01-03T12:03:20.000000Z\t24\n" +
                "25\td\t1970-01-03T12:36:40.000000Z\t25\n" +
                "26\te\t1970-01-03T13:10:00.000000Z\t26\n" +
                "27\te\t1970-01-03T13:43:20.000000Z\t27\n" +
                "28\tf\t1970-01-03T14:16:40.000000Z\t28\n" +
                "29\te\t1970-01-03T14:50:00.000000Z\t29\n" +
                "30\td\t1970-01-03T15:23:20.000000Z\t30\n", "t_col_top_день where день != null"
        );
    }

    private void assertColTopsO3() throws SqlException {
        assertSql("x\tm\tts\tдень\tstr\n" +
                "6\tc\t1970-01-01T06:30:00.000000Z\ta\tSFCI\n" +
                "4\tc\t1970-01-05T05:30:00.000000Z\ta\tXRGUOXFH\n" +
                "9\tc\t1970-01-05T10:30:00.000000Z\ta\tLEQD\n", "t_col_top_ooo_day where m = 'c' and день = 'a'"
        );

        assertSql("x\tm\tts\tдень\tstr\n" +
                "81\tc\t1970-01-04T09:00:00.000000Z\t\t\n" +
                "82\tc\t1970-01-04T10:00:00.000000Z\t\t\n" +
                "84\ta\t1970-01-04T12:00:00.000000Z\t\t\n" +
                "85\tb\t1970-01-04T13:00:00.000000Z\t\t\n" +
                "86\tb\t1970-01-04T14:00:00.000000Z\t\t\n" +
                "87\tb\t1970-01-04T15:00:00.000000Z\t\t\n" +
                "88\tc\t1970-01-04T16:00:00.000000Z\t\t\n" +
                "89\tc\t1970-01-04T17:00:00.000000Z\t\t\n" +
                "90\ta\t1970-01-04T18:00:00.000000Z\t\t\n" +
                "92\ta\t1970-01-04T20:00:00.000000Z\t\t\n" +
                "93\tb\t1970-01-04T21:00:00.000000Z\t\t\n" +
                "94\ta\t1970-01-04T22:00:00.000000Z\t\t\n" +
                "96\ta\t1970-01-05T00:00:00.000000Z\t\t\n" +
                "2\ta\t1970-01-05T03:30:00.000000Z\tc\tWTBBMMDB\n" +
                "3\tb\t1970-01-05T04:30:00.000000Z\tc\tGXIID\n" +
                "4\tc\t1970-01-05T05:30:00.000000Z\ta\tXRGUOXFH\n" +
                "6\ta\t1970-01-05T07:30:00.000000Z\tc\tQYDQVLY\n" +
                "7\tc\t1970-01-05T08:30:00.000000Z\t\tGNVZWJR\n" +
                "8\tb\t1970-01-05T09:30:00.000000Z\tc\tMLMGICUW\n" +
                "9\tc\t1970-01-05T10:30:00.000000Z\ta\tLEQD\n", "t_col_top_ooo_day where m != null limit -20"
        );

        assertSql("x\tm\tts\tдень\tstr\n" +
                "71\tc\t1970-01-03T23:00:00.000000Z\t\t\n" +
                "72\tb\t1970-01-04T00:00:00.000000Z\t\t\n" +
                "76\tc\t1970-01-04T04:00:00.000000Z\t\t\n" +
                "77\tc\t1970-01-04T05:00:00.000000Z\t\t\n" +
                "78\tb\t1970-01-04T06:00:00.000000Z\t\t\n" +
                "79\ta\t1970-01-04T07:00:00.000000Z\t\t\n" +
                "81\tc\t1970-01-04T09:00:00.000000Z\t\t\n" +
                "82\tc\t1970-01-04T10:00:00.000000Z\t\t\n" +
                "84\ta\t1970-01-04T12:00:00.000000Z\t\t\n" +
                "85\tb\t1970-01-04T13:00:00.000000Z\t\t\n" +
                "86\tb\t1970-01-04T14:00:00.000000Z\t\t\n" +
                "87\tb\t1970-01-04T15:00:00.000000Z\t\t\n" +
                "88\tc\t1970-01-04T16:00:00.000000Z\t\t\n" +
                "89\tc\t1970-01-04T17:00:00.000000Z\t\t\n" +
                "90\ta\t1970-01-04T18:00:00.000000Z\t\t\n" +
                "92\ta\t1970-01-04T20:00:00.000000Z\t\t\n" +
                "93\tb\t1970-01-04T21:00:00.000000Z\t\t\n" +
                "94\ta\t1970-01-04T22:00:00.000000Z\t\t\n" +
                "96\ta\t1970-01-05T00:00:00.000000Z\t\t\n" +
                "7\tc\t1970-01-05T08:30:00.000000Z\t\tGNVZWJR\n", "t_col_top_ooo_day where день = null and m != null limit -20"
        );

        assertSql("x\tm\tts\tдень\tstr\n" +
                "6\tc\t1970-01-01T06:30:00.000000Z\ta\tSFCI\n" +
                "12\ta\t1970-01-01T12:30:00.000000Z\ta\tJNOXB\n" +
                "14\t\t1970-01-01T14:30:00.000000Z\ta\tLJYFXSBNVN\n" +
                "16\tb\t1970-01-01T16:30:00.000000Z\ta\tTPUL\n" +
                "21\tb\t1970-01-01T21:30:00.000000Z\ta\tGQWSZMUMXM\n" +
                "24\t\t1970-01-02T00:30:00.000000Z\ta\tNTPYXUB\n" +
                "33\tb\t1970-01-02T09:30:00.000000Z\ta\tFLNGCEFBTD\n" +
                "34\tb\t1970-01-02T10:30:00.000000Z\ta\tTIGUTKI\n" +
                "1\t\t1970-01-05T02:30:00.000000Z\ta\tHQJHN\n" +
                "4\tc\t1970-01-05T05:30:00.000000Z\ta\tXRGUOXFH\n" +
                "5\t\t1970-01-05T06:30:00.000000Z\ta\tFVFFOB\n" +
                "9\tc\t1970-01-05T10:30:00.000000Z\ta\tLEQD\n" +
                "10\t\t1970-01-05T11:30:00.000000Z\ta\tKNHV\n", "t_col_top_ooo_day where день = 'a'"
        );
    }

    private void assertData(boolean withO3, boolean withColTops, boolean withColTopO3, boolean withWalTxn) throws SqlException {
        assertNoneNts();
        assertNone();
        assertDay();
        assertMonth();
        assertYear();
        if (withO3) {
            assertDayO3();
            assertMonthO3();
            assertYearO3();
        }
        if (withColTops) {
            assertColTops();
        }
        if (withColTopO3) {
            assertColTopsO3();
            assertMissingPartitions();
        }
        if (withWalTxn) {
            assertWalTxn();
        }
    }

    private void assertDay() throws SqlException {

        assertSql(
                "a\tb\tc\td\te\tf\tg\th\ti\tj\tk\tl\tm\tn\to\tts\n" +
                        "77\tW\t-29635\t36806\t39204\t0.024422824\t0.4452645911659644\t1974-10-14T05:22:22.780Z\t1970-01-01T00:00:00.181107Z\tJJDSR\tbbbbbb\ttrue\t\t0xd89fec5fab3f4af1a0ca0ec5d6448e2d798d79cb982de744b96a662a0b9f32d7\t00000000 aa 41 c5 55 ef\t1970-06-12T00:56:40.000000Z\n" +
                        "63\tI\t-32736\t467385\t-76685\tnull\t0.31674150508412846\t1975-03-23T06:58:43.118Z\t1970-01-01T00:00:00.400171Z\tVIRB\t\tfalse\t\t0xf6f008b8e0bd93ffe884685ca40e1575d3389fdb74d0af7b8e349d4900aaf080\t00000000 1e 18\t1970-07-28T08:03:20.000000Z\n" +
                        "93\tI\t-2323\t726142\tnull\t0.81225455\t0.9067116554825199\t1979-05-11T13:11:40.594Z\t1969-12-31T23:59:57.415320Z\t\t\ttrue\t\t0xc1b67a610f845c38bf73e9dd66895579d14a19b0f4078a02f5aceb288984dbc4\t00000000 48 ef 10 1b\t1970-10-28T22:16:40.000000Z\n" +
                        "40\tS\t-20754\t640026\tnull\t0.06777322\tnull\t1974-08-22T14:47:36.113Z\t1969-12-31T23:59:57.637072Z\tMXCV\tbbbbbb\tfalse\t\t0x99addce59fcc0d3d9830ab914dab674c60bb78c3b0ee86df40da1c6ee057e8d1\t00000000 36 ab\t1970-11-21T01:50:00.000000Z\n" +
                        "62\tZ\t-5605\t897677\t-25929\tnull\t0.5082431508355564\t1980-04-18T20:36:30.440Z\t1969-12-31T23:59:54.628420Z\tSPTT\tbbbbbb\ttrue\t\t0xa240ef161b45a1e48cf44c1c5bb6f9ab9d0f3bc8b1362801a7f6d25047c701e6\t00000000 cf b3\t1970-12-14T05:23:20.000000Z\n" +
                        "117\tY\t-14561\tnull\t-89408\t0.06261629\t0.6810629367436306\t1980-11-24T14:57:59.806Z\t1969-12-31T23:59:59.009732Z\tXVBHB\taaa\tfalse\t\t0xc8d5d8e5f9c2007489b7325f72e6f3d0a2402a0318a5834ee45764d96505b53b\t\t1971-11-03T07:10:00.000000Z\n",
                "t_day where m = null"
        );

        assertSql(
                "k\n" +
                        "\n" +
                        "aaa\n" +
                        "bbbbbb\n" +
                        "c\n", "select distinct k from t_day order by k"
        );

        assertSql(
                "a\tb\tc\td\te\tf\tg\th\ti\tj\tk\tl\tm\tn\to\tts\n" +
                        "108\tJ\t-11076\tnull\t29326\t0.16564327\t0.18227834647927288\t1973-07-03T11:43:25.390Z\t1969-12-31T23:59:58.122080Z\t\tc\tfalse\tbbbbbb\t0xef8b237502911d607b2c40a4af457c366e7571b9df3a7c23e8da1ba561cec298\t\t1970-01-01T00:03:20.000000Z\n" +
                        "56\tJ\t-20167\t278104\t-3552\t0.52233934\tnull\t1976-09-08T12:29:54.721Z\t1969-12-31T23:59:58.405688Z\tLEOOT\t\tfalse\tbbbbbb\t0x778a900a82a7b55e93023940ab7f1c09b5899bf0ee7c97de971dba16373fc809\t00000000 5d fd 71 5b f4\t1970-01-24T03:36:40.000000Z\n" +
                        "84\tW\t22207\t119693\t-53159\t0.7673439\t0.9031596755527048\t1981-06-03T08:19:47.717Z\t1969-12-31T23:59:53.440321Z\tXSFNG\t\tfalse\tc\t0x61554422489276b898793b266f0b9ea889eb18c5658e6b101acb3e4b0ac462ad\t00000000 bb e2\t1970-02-16T07:10:00.000000Z\n" +
                        "21\tQ\t3496\t578025\t-98876\t0.42481637\t0.09648061880838898\t\t1969-12-31T23:59:55.291958Z\tKHPDG\taaa\tfalse\tc\t0x8312419bf7d5dca6c0cd3d0426f714237af2b1e5f23f205b6c80faa3b741b282\t\t1970-03-11T10:43:20.000000Z\n" +
                        "112\tO\t-3429\t239209\t36115\t0.25313568\t0.985179782114092\t1970-10-03T03:15:48.703Z\t1969-12-31T23:59:53.582491Z\tYBDS\tbbbbbb\ttrue\tc\t0x4599ece7e898dd5324fb604ddf70c54037966205b94caf995a585e64bbd4204d\t00000000 93 b2 7b c7 55\t1970-04-03T14:16:40.000000Z\n" +
                        "80\tB\t-14851\t618043\t29379\t0.57509255\t0.06115382042103079\t1970-10-11T15:55:16.483Z\t1969-12-31T23:59:58.741262Z\t\tc\tfalse\taaa\t0x63ca8f11fefb381c5034365a1ad8cfbc6351b32c301693a89ec31d67e4bc804a\t\t1970-04-26T17:50:00.000000Z\n" +
                        "59\tD\t-25955\t982077\tnull\t0.9965097\t0.023780164968562834\t1976-04-09T11:44:34.276Z\t1970-01-01T00:00:00.214711Z\tPBJC\tc\ttrue\tc\t0x168045b95793bbeb0ee8ff621c5e48fd3e811a742f3bb51c78292859b45b4767\t\t1970-05-19T21:23:20.000000Z\n" +
                        "77\tW\t-29635\t36806\t39204\t0.024422824\t0.4452645911659644\t1974-10-14T05:22:22.780Z\t1970-01-01T00:00:00.181107Z\tJJDSR\tbbbbbb\ttrue\t\t0xd89fec5fab3f4af1a0ca0ec5d6448e2d798d79cb982de744b96a662a0b9f32d7\t00000000 aa 41 c5 55 ef\t1970-06-12T00:56:40.000000Z\n" +
                        "83\tU\t-11505\tnull\t12242\t0.32157594\t0.01267185841652596\t\t1969-12-31T23:59:52.165524Z\tVVWM\tbbbbbb\ttrue\taaa\t0x996a433b566bb6584cb8c2e5513491232a1690ea6ff5f1863f80bc189977dfe3\t\t1970-07-05T04:30:00.000000Z\n" +
                        "63\tI\t-32736\t467385\t-76685\tnull\t0.31674150508412846\t1975-03-23T06:58:43.118Z\t1970-01-01T00:00:00.400171Z\tVIRB\t\tfalse\t\t0xf6f008b8e0bd93ffe884685ca40e1575d3389fdb74d0af7b8e349d4900aaf080\t00000000 1e 18\t1970-07-28T08:03:20.000000Z\n" +
                        "71\tO\t-18571\t699017\tnull\t0.78266335\t0.9308109024121684\t1978-05-28T23:57:59.482Z\t1970-01-01T00:00:00.678374Z\t\tbbbbbb\tfalse\tbbbbbb\t0x44e5f7a335ee650a0a8b38ca752d04b41ccd99af986677b82e1c703573ce9d4f\t00000000 cb 9e 2f 0c 42 fa 8c b9 cc 4b\t1970-08-20T11:36:40.000000Z\n" +
                        "91\tH\t18079\t892554\t91585\t0.45945716\t0.9156453066150101\t1970-10-13T14:26:59.351Z\t1969-12-31T23:59:55.314601Z\tPCXYZ\taaa\ttrue\tc\t0xcd1953974ba8d46feb17cc29c85b213a9c522fadfd26d20430f1e9aa665fa3f7\t00000000 22 ac a3 2d\t1970-09-12T15:10:00.000000Z\n" +
                        "53\tT\t12219\t703085\t-38022\t0.358779\t0.6485577517124145\t\t1969-12-31T23:59:59.580244Z\tHGPKX\t\ttrue\taaa\t0xa3d07224463d9738b1a6665ab4644be3bbfad2a44a30f84aa70288296446782c\t00000000 3e 99 60\t1970-10-05T18:43:20.000000Z\n" +
                        "93\tI\t-2323\t726142\tnull\t0.81225455\t0.9067116554825199\t1979-05-11T13:11:40.594Z\t1969-12-31T23:59:57.415320Z\t\t\ttrue\t\t0xc1b67a610f845c38bf73e9dd66895579d14a19b0f4078a02f5aceb288984dbc4\t00000000 48 ef 10 1b\t1970-10-28T22:16:40.000000Z\n" +
                        "40\tS\t-20754\t640026\tnull\t0.06777322\tnull\t1974-08-22T14:47:36.113Z\t1969-12-31T23:59:57.637072Z\tMXCV\tbbbbbb\tfalse\t\t0x99addce59fcc0d3d9830ab914dab674c60bb78c3b0ee86df40da1c6ee057e8d1\t00000000 36 ab\t1970-11-21T01:50:00.000000Z\n" +
                        "62\tZ\t-5605\t897677\t-25929\tnull\t0.5082431508355564\t1980-04-18T20:36:30.440Z\t1969-12-31T23:59:54.628420Z\tSPTT\tbbbbbb\ttrue\t\t0xa240ef161b45a1e48cf44c1c5bb6f9ab9d0f3bc8b1362801a7f6d25047c701e6\t00000000 cf b3\t1970-12-14T05:23:20.000000Z\n" +
                        "95\tC\t-15985\t176110\t70084\t0.69335353\t0.15363439252599098\t\t1969-12-31T23:59:57.580964Z\tJBWUL\tbbbbbb\ttrue\tc\t0xa7c0b11aa7ddf33f6c595c65977627ea0bfead10d478f3ee22cdbc98872d39f6\t\t1971-01-06T08:56:40.000000Z\n" +
                        "125\tZ\t-10713\t371844\t84132\t0.8621424\t0.2508999003038844\t\t1969-12-31T23:59:57.403164Z\tCDUI\tbbbbbb\tfalse\tc\t0x34f81ebac91b1d1a2ae9bef212d736c25a08b548519baff07ab873d0d0a9fd3d\t00000000 7b 1e\t1971-01-29T12:30:00.000000Z\n" +
                        "76\tE\t16948\tnull\t-51683\t0.22928524\tnull\t\t1969-12-31T23:59:56.380382Z\tVKYQJ\t\tfalse\taaa\t0xcba8fd1a62c96efebb77d1385fe652d28915affdb5c63060cbebb5fea81ad62e\t00000000 6d 46 ad 16 79 58 cd\t1971-02-21T16:03:20.000000Z\n" +
                        "123\tC\t-5778\t967519\t-75175\t0.13986492\t0.010165385801274796\t1973-08-17T06:06:47.663Z\t1969-12-31T23:59:54.612524Z\tKJKNZ\taaa\ttrue\tc\t0x71031551e5cf812198c2fb5501ed706de28a27ae45b942b2cbaf06bbb06e7456\t00000000 20 b9 97 5a 8a 80\t1971-03-16T19:36:40.000000Z\n" +
                        "103\tU\t-1923\t479823\t-30531\tnull\tnull\t1981-07-09T21:16:23.406Z\t1969-12-31T23:59:57.889468Z\tIMYS\t\ttrue\taaa\t0xaf3dd1ee4746fc4994282c8ee2d7086f5ece732b9624346416451f8e583fb972\t00000000 c1 0e 21 fd 77\t1971-04-08T23:10:00.000000Z\n" +
                        "45\tW\t-425\t875599\t15597\tnull\tnull\t1972-09-05T18:56:20.615Z\t1969-12-31T23:59:59.743987Z\tVYRZO\tbbbbbb\tfalse\tbbbbbb\t0xa3a258166f137cd67dee650ead11bfea6d4aaac202727c064541228329d53e80\t\t1971-05-02T02:43:20.000000Z\n" +
                        "126\tT\t-12465\t427824\t-25987\t0.5469257\tnull\t1973-08-27T12:11:42.793Z\t1969-12-31T23:59:54.102211Z\t\tbbbbbb\tfalse\tbbbbbb\t0x5d07e75ccc568037500e39f10ee3b8f8c0f5c2a3e10d06e286c6aa16cec3af38\t00000000 3a d8\t1971-05-25T06:16:40.000000Z\n" +
                        "55\tG\t2234\t662718\t-61238\t0.06681883\tnull\t1977-09-11T20:16:36.619Z\t1969-12-31T23:59:55.378625Z\tECVFF\t\ttrue\taaa\t0xab1c5ce7dcb50cb4ce0fdf8d23f841a2e3fcb395ad3f16d4c103d859de058032\t\t1971-06-17T09:50:00.000000Z\n" +
                        "74\tJ\t22479\tnull\t41900\t0.059055388\t0.6092038333046033\t1971-04-02T20:36:22.096Z\t1969-12-31T23:59:55.594215Z\tBWZRJ\tbbbbbb\ttrue\tc\t0xd7f4199375d9353ea191edc98a6167778483177c32851224a01f3e8c5cbc7a82\t00000000 56 cc a1 80 09 f0\t1971-07-10T13:23:20.000000Z\n" +
                        "79\tF\t-14578\t450030\t38187\tnull\t0.3883845787817346\t\t\t\taaa\tfalse\taaa\t0x8055ab03d5d5c47d62b1d77660c32e8e7e5278c7ef6bde237ffa86c6b59779db\t00000000 2d e8 95 30 44 e2 5d\t1971-08-02T16:56:40.000000Z\n" +
                        "43\tC\t-8135\t213809\t-1275\tnull\t0.5418085488978492\t1971-07-29T01:51:45.288Z\t1969-12-31T23:59:54.180636Z\tEMPP\tbbbbbb\tfalse\tc\t0x9b862233a0c38afca5ba600b164aa6b871cc06618b2b88cc88a4933315f646ec\t00000000 4e a5 4b 2b 97 cc\t1971-08-25T20:30:00.000000Z\n" +
                        "54\tU\t-30570\t374221\tnull\t0.7460444\t0.7437656766929067\t1980-12-01T08:11:10.449Z\t1969-12-31T23:59:57.538081Z\t\tbbbbbb\ttrue\tbbbbbb\t0x1edcdcb17865610639479e2958bfb643aa4861ba1f6724e5ae3ac4819410bc23\t00000000 ce a4 ea 3d c9\t1971-09-18T00:03:20.000000Z\n" +
                        "89\tX\t-5823\t133431\tnull\t0.11185706\t0.16347836851309816\t1982-07-31T14:53:51.284Z\t1969-12-31T23:59:57.661336Z\tYEKNP\t\ttrue\taaa\t0x21ef7ce14b550b25832181a273bc6a92948314655a3b28352acb6fd98d895a10\t\t1971-10-11T03:36:40.000000Z\n" +
                        "117\tY\t-14561\tnull\t-89408\t0.06261629\t0.6810629367436306\t1980-11-24T14:57:59.806Z\t1969-12-31T23:59:59.009732Z\tXVBHB\taaa\tfalse\t\t0xc8d5d8e5f9c2007489b7325f72e6f3d0a2402a0318a5834ee45764d96505b53b\t\t1971-11-03T07:10:00.000000Z\n",
                "t_day"
        );
    }

    private void assertDayO3() throws SqlException {

        assertSql(
                "a\tb\tc\td\te\tf\tg\th\ti\tj\tk\tl\tm\tn\to\tts\n" +
                        "113\tD\t-5450\t304258\t-35463\tnull\tnull\t\t1970-01-01T00:00:00.098394Z\tBGNP\t\tfalse\t\t0x77e1be06c460b246870c47c1891e7fd0ac87f7392b452025c6125e52fbd1dca5\t00000000 9a 09 23 b7 fe e0 66 8e ab be\t1970-01-01T00:56:40.000000Z\n" +
                        "21\tU\t9394\t116017\t28613\t0.9541228\t0.4462835932759871\t\t1969-12-31T23:59:52.698389Z\tJKUJD\tbbbbbb\ttrue\t\t0x77ff57ae7d4edbc04e01b7136579e35b6825ef23c1058aa9773e4dc6440131c1\t\t1970-01-24T04:30:00.000000Z\n" +
                        "32\tE\t-22886\t-53239\t97228\t0.5323387\t0.5460002994573423\t\t1969-12-31T23:59:54.421883Z\tCYRE\t\tfalse\t\t0xb2ca70c2dbad1880ed7c7deb78927731cb475df06066a1dec6cf26b6079345d1\t00000000 84 64\t1970-03-11T10:43:20.000000Z\n" +
                        "63\tJ\t25414\t75639\tnull\t0.49868053\t0.012936401281057996\t1974-03-10T13:01:46.469Z\t1969-12-31T23:59:53.426029Z\tQGPO\taaa\ttrue\t\t0x4b22ca548f932a4446f90de2e997fd102edf54145e4416127c2111d1daa23099\t\t1970-06-12T00:56:40.000000Z\n" +
                        "84\tR\t30808\t427742\t46236\t0.27729326\t0.1394740703793117\t1981-08-30T12:40:12.324Z\t1969-12-31T23:59:52.637810Z\t\tbbbbbb\ttrue\t\t0x8c8b6a1300caf2bcb0db8b2fa736bbf8612b1789ba10fd0c54e1573b4caa7bbb\t\t1970-08-20T11:36:40.000000Z\n" +
                        "103\tP\t-12598\t836287\t9492\t0.63984436\t0.790078109706257\t1972-05-29T16:00:17.462Z\t1969-12-31T23:59:53.580725Z\tJVKW\taaa\tfalse\t\t0x61399f22a34531046a48b5b5f2d4aec3a9030c063a55bc306c9c559bb3c93d06\t\t1970-10-05T18:43:20.000000Z\n" +
                        "51\tS\t22146\t687421\t-55216\t0.3373062\t0.9526743205271402\t1975-10-16T05:00:17.425Z\t1970-01-01T00:00:00.009806Z\tHVYVJ\tc\tfalse\t\t0x945cf7e9f2855229a488c243dfc766564ebc51a2dd0ab714916913d81e6c9883\t00000000 16 4b\t1970-10-05T19:36:40.000000Z\n" +
                        "66\tD\t-5300\t814924\t23886\t0.79929304\t0.6834975299862411\t\t1969-12-31T23:59:55.960730Z\tRUCW\t\ttrue\t\t0xe6e8724e0d55db8bc5c2baab99008d37d61065128bd9ac80cdc779c297ba2d9b\t00000000 7d bf aa 4e c7 70 67 76\t1970-11-21T01:50:00.000000Z\n",
                "t_day_ooo where m = null"
        );

        assertSql(
                "k\n" +
                        "\n" +
                        "aaa\n" +
                        "bbbbbb\n" +
                        "c\n", "select distinct k from t_day_ooo order by k"
        );

        assertSql(
                "a\tb\tc\td\te\tf\tg\th\ti\tj\tk\tl\tm\tn\to\tts\n" +
                        "67\tD\t4054\t129626\t-74037\t0.7729607\t0.7504751831632615\t1973-12-02T23:46:21.858Z\t\tTRJK\tc\ttrue\tc\t0xadab9269a1afcfa8a209c923df856c3267c5f44b36a8e8be5304bb5970a2cbf4\t00000000 39 ae 3a 61 47 59\t1970-01-01T00:03:20.000000Z\n" +
                        "113\tD\t-5450\t304258\t-35463\tnull\tnull\t\t1970-01-01T00:00:00.098394Z\tBGNP\t\tfalse\t\t0x77e1be06c460b246870c47c1891e7fd0ac87f7392b452025c6125e52fbd1dca5\t00000000 9a 09 23 b7 fe e0 66 8e ab be\t1970-01-01T00:56:40.000000Z\n" +
                        "98\tZ\t30001\t552351\t-81397\tnull\t0.8838658419954148\t1972-04-19T12:09:28.393Z\t1969-12-31T23:59:57.954401Z\t\tc\ttrue\tc\t0x8caf7921a1624ab15ab308699faa68fda07fbc4ac8e40c3e9ff3b6344e33baf7\t\t1970-01-24T03:36:40.000000Z\n" +
                        "21\tU\t9394\t116017\t28613\t0.9541228\t0.4462835932759871\t\t1969-12-31T23:59:52.698389Z\tJKUJD\tbbbbbb\ttrue\t\t0x77ff57ae7d4edbc04e01b7136579e35b6825ef23c1058aa9773e4dc6440131c1\t\t1970-01-24T04:30:00.000000Z\n" +
                        "50\tL\t-23630\t625273\t10881\t0.42210323\t0.1614220569523407\t1972-06-02T15:23:57.981Z\t1969-12-31T23:59:57.925772Z\tLMCH\tbbbbbb\tfalse\tc\t0x813df64652e4539f8d1704812a86db599d2f395397471617e8c0aa059fd2ff7e\t00000000 a1 26 4d 20 c7 3d 27 56\t1970-02-16T07:10:00.000000Z\n" +
                        "89\tY\t11702\tnull\t-66201\t0.41341025\t0.5916798984476636\t1975-03-20T22:59:09.088Z\t1969-12-31T23:59:56.557344Z\tQOCKW\taaa\tfalse\taaa\t0x8b20bd04c1469d59a3aa669bc023de9b9ef4af4e7bb33b4886a6a1327a9305b4\t\t1970-02-16T08:03:20.000000Z\n" +
                        "32\tE\t-22886\t-53239\t97228\t0.5323387\t0.5460002994573423\t\t1969-12-31T23:59:54.421883Z\tCYRE\t\tfalse\t\t0xb2ca70c2dbad1880ed7c7deb78927731cb475df06066a1dec6cf26b6079345d1\t00000000 84 64\t1970-03-11T10:43:20.000000Z\n" +
                        "63\tD\t-13237\t349682\t58269\t0.8899592\tnull\t1972-09-11T03:03:29.420Z\t1970-01-01T00:00:00.349012Z\tDLYD\tc\ttrue\tbbbbbb\t0xa372c456bef4dfd959efe9cc24ca36283f082f4bebd899165a04f2f61ae5f616\t00000000 64 f1 88 84 ce 2f 18 2f\t1970-03-11T11:36:40.000000Z\n" +
                        "56\tE\t16923\t292702\t-42438\t0.22151184\tnull\t1977-04-25T05:00:52.665Z\t1969-12-31T23:59:59.551179Z\t\tc\tfalse\tc\t0xb6d3d8ef9d270659ba4505a45607a671dff633c8162db8f865999c7fce96f7cf\t00000000 af d9 c4 ce\t1970-04-03T14:16:40.000000Z\n" +
                        "100\tL\t-21087\tnull\t8483\t0.72218794\t0.5367705697885076\t1972-07-04T16:07:22.633Z\t1969-12-31T23:59:59.259168Z\tKHWIH\tc\tfalse\tbbbbbb\t0xc04116c648bfc0cfc39a6577b9023ae683da24fefe793c735ee265e38b644743\t00000000 3a 95 54 64 f2 d2 d3 f2 0a\t1970-04-03T15:10:00.000000Z\n" +
                        "127\tX\t-6754\t634555\t-58749\t0.45769918\t0.1314717313816608\t1980-04-09T03:28:19.579Z\t1969-12-31T23:59:55.363250Z\tQKQWP\tbbbbbb\ttrue\taaa\t0x73545ffed36d6b39393354332407500c2e05793b7ac41bd651de5085560c403e\t00000000 b7 36 56 28\t1970-04-26T17:50:00.000000Z\n" +
                        "117\tZ\t19280\t708029\t7629\t0.88966525\t0.07729135833370693\t1971-05-26T20:32:30.186Z\t1969-12-31T23:59:57.260889Z\tDCXGB\tc\tfalse\tc\t0x58004b1cab1eb285a13e739a5d7f909b79a74566d6205b02943645787af3660a\t00000000 7f 19 83 5d\t1970-04-26T18:43:20.000000Z\n" +
                        "74\tD\t22755\t47698\t83591\t0.347165\t0.9731459713995845\t\t1969-12-31T23:59:54.739164Z\tQBTJ\tbbbbbb\ttrue\taaa\t0x6129d17444242622658873ba5637c583d0364c5670e373daf33edd2f1a2f5dc1\t\t1970-05-19T21:23:20.000000Z\n" +
                        "35\tV\t30763\t454782\t21576\t0.54902285\t0.918078226055601\t1979-07-30T03:13:38.335Z\t1970-01-01T00:00:00.494497Z\t\taaa\tfalse\taaa\t0x4f957d6d4c11b309993e21df91a6c336b3ec12c7605e4d2bafc4e8011e9e8585\t00000000 d5 0e 13 5b 66 92 63\t1970-05-19T22:16:40.000000Z\n" +
                        "63\tJ\t25414\t75639\tnull\t0.49868053\t0.012936401281057996\t1974-03-10T13:01:46.469Z\t1969-12-31T23:59:53.426029Z\tQGPO\taaa\ttrue\t\t0x4b22ca548f932a4446f90de2e997fd102edf54145e4416127c2111d1daa23099\t\t1970-06-12T00:56:40.000000Z\n" +
                        "23\tW\t-22179\t668674\t56239\t0.7834721\t0.8296487842503996\t\t1969-12-31T23:59:56.324655Z\tJEYIB\t\tfalse\taaa\t0x4b50c0b8b63364337f1dcc900e3d86d7766ea5bb46ac34808c3cb23cb3a3b1e5\t00000000 35 84 93 09 b6 0a a3\t1970-06-12T01:50:00.000000Z\n" +
                        "38\tT\t28632\t871464\t36443\t0.41481304\tnull\t1977-07-08T09:49:37.136Z\t1969-12-31T23:59:56.876692Z\tMEFGB\taaa\ttrue\taaa\t0xcfe0f613a3f885e0872552c741a4f1a059cad7938cedb7127a7871adc5bc7187\t\t1970-07-05T04:30:00.000000Z\n" +
                        "22\tW\t-31843\t141735\tnull\t0.6955848\t0.5746332516686926\t1982-04-17T13:14:14.137Z\t1969-12-31T23:59:59.172667Z\tNYLP\tbbbbbb\ttrue\taaa\t0x9e3c52f318512793aaa5c4e9097c6ff7e2a8f8b27f3bd0a7ba67ab8dc56779ab\t00000000 20 99 ca f5 23 fc 8b d3 da\t1970-07-05T05:23:20.000000Z\n" +
                        "121\tM\t-10006\tnull\t25084\t0.25855094\t0.19090281591031055\t\t1969-12-31T23:59:58.529372Z\t\t\ttrue\tc\t0x5e0a5014c858a2155c54b8fe88c3f28bc0a5aa2b7a64749b82c0f0baa065b87a\t00000000 f7 32 58 95 1a dc\t1970-07-28T08:03:20.000000Z\n" +
                        "94\tD\t2347\tnull\tnull\t0.38938713\t0.8465458515997965\t1982-01-08T09:55:48.631Z\t1969-12-31T23:59:54.822700Z\tXOSCL\tc\tfalse\tc\t0x74320ff14ac6b0e5af43205486800f546a6dfe2359fcb48868895c6af864bc30\t00000000 23 8d 4e f2 24\t1970-07-28T08:56:40.000000Z\n" +
                        "84\tR\t30808\t427742\t46236\t0.27729326\t0.1394740703793117\t1981-08-30T12:40:12.324Z\t1969-12-31T23:59:52.637810Z\t\tbbbbbb\ttrue\t\t0x8c8b6a1300caf2bcb0db8b2fa736bbf8612b1789ba10fd0c54e1573b4caa7bbb\t\t1970-08-20T11:36:40.000000Z\n" +
                        "79\tH\t-6052\t381347\t-2410\t0.45439512\t0.6038174272571708\t1976-01-08T16:25:57.265Z\t1969-12-31T23:59:59.845206Z\tPTSGO\tbbbbbb\ttrue\tc\t0x8aea6385566991f9545056f68175de1951b91b80845e794446e152b713066729\t\t1970-08-20T12:30:00.000000Z\n" +
                        "82\tT\t-5480\tnull\t14044\t0.51954716\t0.6153338736725058\t1979-01-05T16:34:18.864Z\t1969-12-31T23:59:57.923737Z\tXXXMG\taaa\tfalse\tc\t0xd6d5ed5cdda5769582614094132b92b270792b8a5cb4ad77c84691b43abf49bb\t00000000 d2 fa 8a 0d 8e\t1970-09-12T15:10:00.000000Z\n" +
                        "23\tR\t7130\tnull\t-16659\t0.3326251\t0.716710988270706\t1978-07-29T09:01:38.361Z\t\tJFSHS\tc\ttrue\taaa\t0x8f5846b61894ad198cc85c25e3cab09d7dcccdd8d3e47403963c72b64fc90c3b\t00000000 cd 9a 8a 35 bf 30 e5\t1970-09-12T16:03:20.000000Z\n" +
                        "103\tP\t-12598\t836287\t9492\t0.63984436\t0.790078109706257\t1972-05-29T16:00:17.462Z\t1969-12-31T23:59:53.580725Z\tJVKW\taaa\tfalse\t\t0x61399f22a34531046a48b5b5f2d4aec3a9030c063a55bc306c9c559bb3c93d06\t\t1970-10-05T18:43:20.000000Z\n" +
                        "51\tS\t22146\t687421\t-55216\t0.3373062\t0.9526743205271402\t1975-10-16T05:00:17.425Z\t1970-01-01T00:00:00.009806Z\tHVYVJ\tc\tfalse\t\t0x945cf7e9f2855229a488c243dfc766564ebc51a2dd0ab714916913d81e6c9883\t00000000 16 4b\t1970-10-05T19:36:40.000000Z\n" +
                        "72\tI\t-13055\t80205\t72212\t0.8594243\tnull\t1979-12-14T03:00:27.232Z\t1969-12-31T23:59:53.910807Z\tULUD\tc\tfalse\tc\t0x9ca139e6aa7bad8fd920224e4c134dc36257adb6f218cf114bf20bf3af7f0f21\t\t1970-10-28T22:16:40.000000Z\n" +
                        "78\tG\t-17940\t80875\t-51648\t0.6760418\t0.11832421260528181\t1975-05-31T11:59:53.684Z\t1969-12-31T23:59:58.301229Z\t\tc\ttrue\tc\t0x865d10d8ed10c87e834c69559a9466d6c203e9ee6688fb00a82be0da997d3319\t00000000 5b e4 24 6e 3b 78\t1970-10-28T23:10:00.000000Z\n" +
                        "66\tD\t-5300\t814924\t23886\t0.79929304\t0.6834975299862411\t\t1969-12-31T23:59:55.960730Z\tRUCW\t\ttrue\t\t0xe6e8724e0d55db8bc5c2baab99008d37d61065128bd9ac80cdc779c297ba2d9b\t00000000 7d bf aa 4e c7 70 67 76\t1970-11-21T01:50:00.000000Z\n" +
                        "116\tW\t1209\tnull\t-54703\t0.38266093\t0.8336512245695875\t1973-04-20T21:58:44.387Z\t1969-12-31T23:59:57.953146Z\tWQLO\t\ttrue\taaa\t0x63d3edab19deae0e8de4fd1623e40b3592c091ab093e983862af3f4b563813ce\t00000000 21 13\t1970-11-21T02:43:20.000000Z\n",
                "t_day_ooo"
        );

        assertSql(
                "a\tb\tt\n" +
                        "\t\t2014-09-10T01:00:00.000000Z\n" +
                        "\t\t2014-09-10T01:00:00.000000Z\n" +
                        "\t\t2014-09-10T01:00:00.100000Z\n" +
                        "\t\t2014-09-10T01:00:00.200000Z\n" +
                        "\t\t2014-09-10T01:00:00.300000Z\n" +
                        "\t\t2014-09-10T01:00:00.400000Z\n" +
                        "\t\t2014-09-10T01:00:00.500000Z\n" +
                        "\t\t2014-09-10T01:00:00.600000Z\n" +
                        "\t\t2014-09-10T01:00:00.700000Z\n" +
                        "\t\t2014-09-10T01:00:00.800000Z\n" +
                        "\t\t2014-09-10T01:00:00.900000Z\n" +
                        "\t\t2014-09-10T01:00:01.000000Z\n" +
                        "\t\t2014-09-10T01:00:01.100000Z\n" +
                        "\t\t2014-09-10T01:00:01.200000Z\n" +
                        "\t\t2014-09-10T01:00:01.300000Z\n" +
                        "\t\t2014-09-10T01:00:01.400000Z\n" +
                        "\t\t2014-09-10T01:00:01.500000Z\n" +
                        "\t\t2014-09-10T01:00:01.600000Z\n" +
                        "\t\t2014-09-10T01:00:01.700000Z\n" +
                        "\t\t2014-09-10T01:00:01.800000Z\n" +
                        "\t\t2014-09-10T01:00:01.900000Z\n" +
                        "\t\t2014-09-10T01:00:02.000000Z\n" +
                        "\t\t2014-09-10T01:00:02.100000Z\n" +
                        "\t\t2014-09-10T01:00:02.200000Z\n" +
                        "\t\t2014-09-10T01:00:02.300000Z\n" +
                        "\t\t2014-09-10T01:00:02.400000Z\n" +
                        "\t\t2014-09-10T01:00:02.500000Z\n" +
                        "\t\t2014-09-10T01:00:02.600000Z\n" +
                        "\t\t2014-09-10T01:00:02.700000Z\n" +
                        "\t\t2014-09-10T01:00:02.800000Z\n" +
                        "\t\t2014-09-10T01:00:02.900000Z\n" +
                        "\t\t2014-09-10T01:00:03.000000Z\n" +
                        "\t\t2014-09-10T01:00:03.100000Z\n" +
                        "\t\t2014-09-10T01:00:03.200000Z\n" +
                        "\t\t2014-09-10T01:00:03.300000Z\n" +
                        "\t\t2014-09-10T01:00:03.400000Z\n" +
                        "\t\t2014-09-10T01:00:03.500000Z\n" +
                        "\t\t2014-09-10T01:00:03.600000Z\n" +
                        "\t\t2014-09-10T01:00:03.700000Z\n" +
                        "\t\t2014-09-10T01:00:03.800000Z\n" +
                        "\t\t2014-09-10T01:00:03.900000Z\n" +
                        "\t\t2014-09-10T01:00:04.000000Z\n" +
                        "\t\t2014-09-10T01:00:04.100000Z\n" +
                        "\t\t2014-09-10T01:00:04.200000Z\n" +
                        "\t\t2014-09-10T01:00:04.300000Z\n" +
                        "\t\t2014-09-10T01:00:04.400000Z\n" +
                        "\t\t2014-09-10T01:00:04.500000Z\n" +
                        "\t\t2014-09-10T01:00:04.600000Z\n" +
                        "\t\t2014-09-10T01:00:04.700000Z\n" +
                        "\t\t2014-09-10T01:00:04.800000Z\n" +
                        "\t\t2014-09-10T01:00:04.900000Z\n" +
                        "\t\t2014-09-10T01:00:05.000000Z\n" +
                        "\t\t2014-09-10T01:00:05.100000Z\n" +
                        "\t\t2014-09-10T01:00:05.200000Z\n" +
                        "\t\t2014-09-10T01:00:05.300000Z\n" +
                        "\t\t2014-09-10T01:00:05.400000Z\n" +
                        "\t\t2014-09-10T01:00:05.500000Z\n" +
                        "\t\t2014-09-10T01:00:05.600000Z\n" +
                        "\t\t2014-09-10T01:00:05.700000Z\n" +
                        "\t\t2014-09-10T01:00:05.800000Z\n" +
                        "\t\t2014-09-10T01:00:05.900000Z\n" +
                        "\t\t2014-09-10T01:00:06.000000Z\n" +
                        "\t\t2014-09-10T01:00:06.100000Z\n" +
                        "\t\t2014-09-10T01:00:06.200000Z\n" +
                        "\t\t2014-09-10T01:00:06.300000Z\n" +
                        "\t\t2014-09-10T01:00:06.400000Z\n" +
                        "\t\t2014-09-10T01:00:06.500000Z\n" +
                        "\t\t2014-09-10T01:00:06.600000Z\n" +
                        "\t\t2014-09-10T01:00:06.700000Z\n" +
                        "\t\t2014-09-10T01:00:06.800000Z\n" +
                        "\t\t2014-09-10T01:00:06.900000Z\n" +
                        "\t\t2014-09-10T01:00:07.000000Z\n" +
                        "\t\t2014-09-10T01:00:07.100000Z\n" +
                        "\t\t2014-09-10T01:00:07.200000Z\n" +
                        "\t\t2014-09-10T01:00:07.300000Z\n" +
                        "\t\t2014-09-10T01:00:07.400000Z\n" +
                        "\t\t2014-09-10T01:00:07.500000Z\n" +
                        "\t\t2014-09-10T01:00:07.600000Z\n" +
                        "\t\t2014-09-10T01:00:07.700000Z\n" +
                        "\t\t2014-09-10T01:00:07.800000Z\n" +
                        "\t\t2014-09-10T01:00:07.900000Z\n" +
                        "\t\t2014-09-10T01:00:08.000000Z\n" +
                        "\t\t2014-09-10T01:00:08.100000Z\n" +
                        "\t\t2014-09-10T01:00:08.200000Z\n" +
                        "\t\t2014-09-10T01:00:08.300000Z\n" +
                        "\t\t2014-09-10T01:00:08.400000Z\n" +
                        "\t\t2014-09-10T01:00:08.500000Z\n" +
                        "\t\t2014-09-10T01:00:08.600000Z\n" +
                        "\t\t2014-09-10T01:00:08.700000Z\n" +
                        "\t\t2014-09-10T01:00:08.800000Z\n" +
                        "\t\t2014-09-10T01:00:08.900000Z\n" +
                        "\t\t2014-09-10T01:00:09.000000Z\n" +
                        "\t\t2014-09-10T01:00:09.100000Z\n" +
                        "\t\t2014-09-10T01:00:09.200000Z\n" +
                        "\t\t2014-09-10T01:00:09.300000Z\n" +
                        "\t\t2014-09-10T01:00:09.400000Z\n" +
                        "\t\t2014-09-10T01:00:09.500000Z\n" +
                        "\t\t2014-09-10T01:00:09.600000Z\n" +
                        "\t\t2014-09-10T01:00:09.700000Z\n" +
                        "\t\t2014-09-10T01:00:09.800000Z\n" +
                        "\t\t2014-09-10T01:00:09.900000Z\n" +
                        "\t\t2014-09-11T01:00:00.000000Z\n" +
                        "\t\t2014-09-11T01:00:00.100000Z\n" +
                        "\t\t2014-09-11T01:00:00.200000Z\n" +
                        "\t\t2014-09-11T01:00:00.300000Z\n" +
                        "\t\t2014-09-11T01:00:00.400000Z\n" +
                        "\t\t2014-09-11T01:00:00.500000Z\n" +
                        "\t\t2014-09-11T01:00:00.600000Z\n" +
                        "\t\t2014-09-11T01:00:00.700000Z\n" +
                        "\t\t2014-09-11T01:00:00.800000Z\n" +
                        "\t\t2014-09-11T01:00:00.900000Z\n" +
                        "\t\t2014-09-11T01:00:01.000000Z\n" +
                        "\t\t2014-09-11T01:00:01.100000Z\n" +
                        "\t\t2014-09-11T01:00:01.200000Z\n" +
                        "\t\t2014-09-11T01:00:01.300000Z\n" +
                        "\t\t2014-09-11T01:00:01.400000Z\n" +
                        "\t\t2014-09-11T01:00:01.500000Z\n" +
                        "\t\t2014-09-11T01:00:01.600000Z\n" +
                        "\t\t2014-09-11T01:00:01.700000Z\n" +
                        "\t\t2014-09-11T01:00:01.800000Z\n" +
                        "\t\t2014-09-11T01:00:01.900000Z\n" +
                        "\t\t2014-09-11T01:00:02.000000Z\n" +
                        "\t\t2014-09-11T01:00:02.100000Z\n" +
                        "\t\t2014-09-11T01:00:02.200000Z\n" +
                        "\t\t2014-09-11T01:00:02.300000Z\n" +
                        "\t\t2014-09-11T01:00:02.400000Z\n" +
                        "\t\t2014-09-11T01:00:02.500000Z\n" +
                        "\t\t2014-09-11T01:00:02.600000Z\n" +
                        "\t\t2014-09-11T01:00:02.700000Z\n" +
                        "\t\t2014-09-11T01:00:02.800000Z\n" +
                        "\t\t2014-09-11T01:00:02.900000Z\n" +
                        "\t\t2014-09-11T01:00:03.000000Z\n" +
                        "\t\t2014-09-11T01:00:03.100000Z\n" +
                        "\t\t2014-09-11T01:00:03.200000Z\n" +
                        "\t\t2014-09-11T01:00:03.300000Z\n" +
                        "\t\t2014-09-11T01:00:03.400000Z\n" +
                        "\t\t2014-09-11T01:00:03.500000Z\n" +
                        "\t\t2014-09-11T01:00:03.600000Z\n" +
                        "\t\t2014-09-11T01:00:03.700000Z\n" +
                        "\t\t2014-09-11T01:00:03.800000Z\n" +
                        "\t\t2014-09-11T01:00:03.900000Z\n" +
                        "\t\t2014-09-11T01:00:04.000000Z\n" +
                        "\t\t2014-09-11T01:00:04.100000Z\n" +
                        "\t\t2014-09-11T01:00:04.200000Z\n" +
                        "\t\t2014-09-11T01:00:04.300000Z\n" +
                        "\t\t2014-09-11T01:00:04.400000Z\n" +
                        "\t\t2014-09-11T01:00:04.500000Z\n" +
                        "\t\t2014-09-11T01:00:04.600000Z\n" +
                        "\t\t2014-09-11T01:00:04.700000Z\n" +
                        "\t\t2014-09-11T01:00:04.800000Z\n" +
                        "\t\t2014-09-11T01:00:04.900000Z\n" +
                        "\t\t2014-09-11T01:00:05.000000Z\n" +
                        "\t\t2014-09-11T01:00:05.100000Z\n" +
                        "\t\t2014-09-11T01:00:05.200000Z\n" +
                        "\t\t2014-09-11T01:00:05.300000Z\n" +
                        "\t\t2014-09-11T01:00:05.400000Z\n" +
                        "\t\t2014-09-11T01:00:05.500000Z\n" +
                        "\t\t2014-09-11T01:00:05.600000Z\n" +
                        "\t\t2014-09-11T01:00:05.700000Z\n" +
                        "\t\t2014-09-11T01:00:05.800000Z\n" +
                        "\t\t2014-09-11T01:00:05.900000Z\n" +
                        "\t\t2014-09-11T01:00:06.000000Z\n" +
                        "\t\t2014-09-11T01:00:06.100000Z\n" +
                        "\t\t2014-09-11T01:00:06.200000Z\n" +
                        "\t\t2014-09-11T01:00:06.300000Z\n" +
                        "\t\t2014-09-11T01:00:06.400000Z\n" +
                        "\t\t2014-09-11T01:00:06.500000Z\n" +
                        "\t\t2014-09-11T01:00:06.600000Z\n" +
                        "\t\t2014-09-11T01:00:06.700000Z\n" +
                        "\t\t2014-09-11T01:00:06.800000Z\n" +
                        "\t\t2014-09-11T01:00:06.900000Z\n" +
                        "\t\t2014-09-11T01:00:07.000000Z\n" +
                        "\t\t2014-09-11T01:00:07.100000Z\n" +
                        "\t\t2014-09-11T01:00:07.200000Z\n" +
                        "\t\t2014-09-11T01:00:07.300000Z\n" +
                        "\t\t2014-09-11T01:00:07.400000Z\n" +
                        "\t\t2014-09-11T01:00:07.500000Z\n" +
                        "\t\t2014-09-11T01:00:07.600000Z\n" +
                        "\t\t2014-09-11T01:00:07.700000Z\n" +
                        "\t\t2014-09-11T01:00:07.800000Z\n" +
                        "\t\t2014-09-11T01:00:07.900000Z\n" +
                        "\t\t2014-09-11T01:00:08.000000Z\n" +
                        "\t\t2014-09-11T01:00:08.100000Z\n" +
                        "\t\t2014-09-11T01:00:08.200000Z\n" +
                        "\t\t2014-09-11T01:00:08.300000Z\n" +
                        "\t\t2014-09-11T01:00:08.400000Z\n" +
                        "\t\t2014-09-11T01:00:08.500000Z\n" +
                        "\t\t2014-09-11T01:00:08.600000Z\n" +
                        "\t\t2014-09-11T01:00:08.700000Z\n" +
                        "\t\t2014-09-11T01:00:08.800000Z\n" +
                        "\t\t2014-09-11T01:00:08.900000Z\n" +
                        "\t\t2014-09-11T01:00:09.000000Z\n" +
                        "\t\t2014-09-11T01:00:09.100000Z\n" +
                        "\t\t2014-09-11T01:00:09.200000Z\n" +
                        "\t\t2014-09-11T01:00:09.300000Z\n" +
                        "\t\t2014-09-11T01:00:09.400000Z\n" +
                        "\t\t2014-09-11T01:00:09.500000Z\n" +
                        "\t\t2014-09-11T01:00:09.600000Z\n" +
                        "\t\t2014-09-11T01:00:09.700000Z\n" +
                        "\t\t2014-09-11T01:00:09.800000Z\n" +
                        "\t\t2014-09-11T01:00:09.900000Z\n", "o3_0"
        );
    }

    private void assertMetadataCache(boolean withO3, boolean withColTops, boolean withColTopO3, boolean withWalTxn, boolean ignoreMaxLag) {

        assertTableMeta(ignoreMaxLag,
                "t_none_nts", "NONE_NTS",
                "t_none", "NONE",
                "t_day", "DAY",
                "t_month", "MONTH",
                "t_year", "YEAR"
        );

        if (withO3) {
            assertTableMeta(ignoreMaxLag,
                    "t_day_ooo", "DAY",
                    "t_month_ooo", "MONTH",
                    "t_year_ooo", "YEAR"
            );
        }

        if (withColTops) {
            assertShortTableMeta(ignoreMaxLag,
                    "t_col_top_none", "NONE"
            );
        }

        if (withColTopO3) {
            assertShort2TableMeta(ignoreMaxLag,
                    "t_col_top_ooo_day", "DAY"
            );
        }

        if (withWalTxn) {
            assertShortWalTableMeta(ignoreMaxLag,
                    "t_col_top_ooo_day_wal", "DAY"
            );
        }
    }

    private void assertMissingPartitions() throws SqlException {
        execute("alter table t_col_top_день_missing_parts drop partition where ts < '1970-01-02'");
        assertSql(
                "x\tm\tts\tдень\n" +
                        "6\tc\t1970-01-02T04:20:00.000000Z\tnull\n" +
                        "7\tb\t1970-01-02T09:53:20.000000Z\tnull\n" +
                        "8\t\t1970-01-02T15:26:40.000000Z\tnull\n" +
                        "9\t\t1970-01-02T21:00:00.000000Z\tnull\n" +
                        "10\tc\t1970-01-03T02:33:20.000000Z\tnull\n" +
                        "11\tb\t1970-01-03T08:06:40.000000Z\tnull\n" +
                        "12\tb\t1970-01-03T13:40:00.000000Z\tnull\n" +
                        "13\tb\t1970-01-03T19:13:20.000000Z\tnull\n", "t_col_top_день_missing_parts where день = null"
        );

        assertSql(
                "x\tm\tts\tдень\n" +
                        "16\te\t1970-01-03T07:36:40.000000Z\t16\n" +
                        "17\tf\t1970-01-03T08:10:00.000000Z\t17\n" +
                        "18\te\t1970-01-03T08:43:20.000000Z\t18\n" +
                        "19\t\t1970-01-03T09:16:40.000000Z\t19\n" +
                        "20\te\t1970-01-03T09:50:00.000000Z\t20\n" +
                        "21\td\t1970-01-03T10:23:20.000000Z\t21\n" +
                        "22\td\t1970-01-03T10:56:40.000000Z\t22\n" +
                        "23\tf\t1970-01-03T11:30:00.000000Z\t23\n" +
                        "24\t\t1970-01-03T12:03:20.000000Z\t24\n" +
                        "25\td\t1970-01-03T12:36:40.000000Z\t25\n" +
                        "26\te\t1970-01-03T13:10:00.000000Z\t26\n" +
                        "27\te\t1970-01-03T13:43:20.000000Z\t27\n" +
                        "28\tf\t1970-01-03T14:16:40.000000Z\t28\n" +
                        "29\te\t1970-01-03T14:50:00.000000Z\t29\n" +
                        "30\td\t1970-01-03T15:23:20.000000Z\t30\n", "t_col_top_день_missing_parts where день != null"
        );
    }

    private void assertMonth() throws SqlException {
        assertSql(
                "a\tb\tc\td\te\tf\tg\th\ti\tj\tk\tl\tm\tn\to\tts\n" +
                        "119\tV\t3178\t837756\t-81585\t0.7627065\t0.5219904713000894\t1978-05-08T19:14:22.368Z\t1969-12-31T23:59:59.756314Z\tNCPVU\tbbbbbb\tfalse\t\t0x92e216e13e061220bcc73ae25c8f5957ced00e989db8c99c77b67aaf034c0258\t00000000 5f a3 32\t1970-01-01T00:03:20.000000Z\n" +
                        "121\tR\t-32248\t396050\t-35751\t0.78169966\t0.9959337662774681\t\t1969-12-31T23:59:58.588233Z\tDVQMI\tc\ttrue\t\t0xa5bb89788829b308c718693d412ea1d96036838211b943c2609c2d1104b89752\t00000000 31 5e e0\t1970-08-20T11:36:40.000000Z\n" +
                        "127\tU\t-2321\t463796\t18370\t0.1165275\t0.9443378372150231\t1977-08-17T04:40:59.569Z\t1969-12-31T23:59:52.196177Z\tWURWB\tc\tfalse\t\t0x6f25b86bb472b338ab35729b5cee1b3871d3cc3f2f8d645824bf6d6e797dfc3c\t00000000 21 57 eb c4 9c\t1971-11-26T10:43:20.000000Z\n" +
                        "112\tP\t9802\tnull\t75166\t0.118374825\t0.3494250368753934\t1975-09-30T10:09:31.504Z\t1969-12-31T23:59:55.762374Z\t\taaa\tfalse\t\t0x5c0d7d492392e518bc746464a1bc90a182d3dc20f47b3454237a6eab48804528\t00000000 cd b2 fa d5 2a 19 1b c9\t1978-11-15T17:50:00.000000Z\n" +
                        "124\tI\t-30723\t159684\t-45800\t0.057298005\t0.8248769037655503\t1975-09-08T12:04:39.272Z\t1969-12-31T23:59:57.670332Z\tYNOR\taaa\ttrue\t\t0xcd186db59e62b037b98b79aef5acec1fdb8cfee61427b2cc7d13df361ce20ad9\t\t1979-07-05T05:23:20.000000Z\n" +
                        "25\tZ\t-6353\t96442\t-49636\tnull\t0.40549501732612403\t1971-07-13T02:33:26.128Z\t1969-12-31T23:59:55.204464Z\t\t\tfalse\t\t0xb9b7551ff3f855ab6cf5a8f96caceffc3ed28138e7ede3ed9ec0ec50fa4f863b\t00000000 bb f4 7c 7a c2 a4 77 a6 8f aa\t1987-02-11T00:03:20.000000Z\n" +
                        "71\tD\t694\t772203\t-32603\t0.9552407\t0.2769130518011955\t1978-12-25T19:08:51.960Z\t1969-12-31T23:59:55.754471Z\t\tc\tfalse\t\t0x310d0e7a171e7f3c97f6387cdfad01f8cd2b0e4fd8b234fba3804ab8de1f8951\t00000000 80 24 24 8c d4 48 55 31 89\t1987-09-30T11:36:40.000000Z\n" +
                        "103\tL\t18557\tnull\t-19361\t0.11996418\tnull\t1970-08-08T18:27:58.955Z\t1969-12-31T23:59:58.439595Z\t\tc\ttrue\t\t0xbc7827bb6dc3ce15b85b002a9fea625b95bdcc28b3dfce1e85524b903924c9c2\t00000000 18 e2 56 c6 ce\t1988-05-18T23:10:00.000000Z\n",
                "t_month where m = null"
        );

        assertSql(
                "k\n" +
                        "\n" +
                        "aaa\n" +
                        "bbbbbb\n" +
                        "c\n", "select distinct k from t_month order by k"
        );

        assertSql(
                "count\n" +
                        "30\n", "select count() from t_month"
        );

        assertSql(
                "a\tb\tc\td\te\tf\tg\th\ti\tj\tk\tl\tm\tn\to\tts\n" +
                        "119\tV\t3178\t837756\t-81585\t0.7627065\t0.5219904713000894\t1978-05-08T19:14:22.368Z\t1969-12-31T23:59:59.756314Z\tNCPVU\tbbbbbb\tfalse\t\t0x92e216e13e061220bcc73ae25c8f5957ced00e989db8c99c77b67aaf034c0258\t00000000 5f a3 32\t1970-01-01T00:03:20.000000Z\n" +
                        "121\tR\t-32248\t396050\t-35751\t0.78169966\t0.9959337662774681\t\t1969-12-31T23:59:58.588233Z\tDVQMI\tc\ttrue\t\t0xa5bb89788829b308c718693d412ea1d96036838211b943c2609c2d1104b89752\t00000000 31 5e e0\t1970-08-20T11:36:40.000000Z\n" +
                        "116\tS\t11789\t285245\tnull\t0.2748559\t0.48199798371959646\t1982-01-20T22:04:01.395Z\t1969-12-31T23:59:59.374384Z\t\t\ttrue\tbbbbbb\t0x675acee433526bf1d88d74b0004aefb5c821d25caad824b8917d8cbbb2aedd63\t00000000 ee ae 90 2b b8 c3 15 8d\t1971-04-08T23:10:00.000000Z\n" +
                        "127\tU\t-2321\t463796\t18370\t0.1165275\t0.9443378372150231\t1977-08-17T04:40:59.569Z\t1969-12-31T23:59:52.196177Z\tWURWB\tc\tfalse\t\t0x6f25b86bb472b338ab35729b5cee1b3871d3cc3f2f8d645824bf6d6e797dfc3c\t00000000 21 57 eb c4 9c\t1971-11-26T10:43:20.000000Z\n" +
                        "74\tD\t25362\t682088\t37064\tnull\t0.09929966140707747\t1980-04-11T15:49:40.209Z\t\tLVCFY\tbbbbbb\ttrue\tc\t0x7759fff2e17726f537c5aa6210177ec0322db0e47bccc1339c7a7acc9ce0d624\t00000000 83 a5 d3\t1972-07-14T22:16:40.000000Z\n" +
                        "62\tE\t10511\tnull\t-99912\tnull\t0.8431363786541575\t1974-01-22T15:44:54.116Z\t1969-12-31T23:59:56.458163Z\t\taaa\ttrue\tbbbbbb\t0xa5ee4544ec1bf467758e2a1d53764b76714175f1867c790ad148a849a06b6d92\t00000000 af 1f 6d 01 87 02\t1973-03-03T09:50:00.000000Z\n" +
                        "65\tW\t-2885\t851731\t-79233\t0.6637018\t0.0032702345951136635\t1978-03-26T02:39:47.370Z\t1969-12-31T23:59:53.607294Z\tEQXY\tc\ttrue\taaa\t0x2a2d8dfbb6b68e3385a2cfc3f2af8639ac5f33609469d81353bc6da60030d8c4\t\t1973-10-20T21:23:20.000000Z\n" +
                        "41\tC\t-16308\t320735\t-8435\t0.33063477\t0.4365314986211333\t1972-07-10T12:25:13.365Z\t1969-12-31T23:59:57.792714Z\tOMCD\tbbbbbb\ttrue\tc\t0x9408a2099e8f9970956e71dfbbc6c2589f95d733600f062ce1631618978acea7\t00000000 08 72\t1974-06-09T08:56:40.000000Z\n" +
                        "122\tO\t17929\t499038\t85526\t0.82511437\t0.7117525079805059\t1978-06-27T11:47:04.188Z\t1969-12-31T23:59:55.096132Z\tZQWOZ\tc\tfalse\taaa\t0xb29789a0923b219b7aad19b3ada4cbd13129ef29aa7547dc37c477321055e4ae\t00000000 03 a6\t1975-01-26T20:30:00.000000Z\n" +
                        "97\tY\t7715\t873148\t-42788\t0.9384666\t0.28698925385676677\t\t1969-12-31T23:59:54.146676Z\tGUSNT\t\ttrue\taaa\t0xc703358a596b6695b93045abac9b3264c417d9b6495ba67e9ee699fb01f7932a\t00000000 34 03 98 a6 0b 6d\t1975-09-15T08:03:20.000000Z\n" +
                        "53\tH\t1057\t496919\tnull\t0.63052213\t0.4861198096781342\t\t1969-12-31T23:59:56.674949Z\tEDVJQ\t\tfalse\taaa\t0xce33ca110168c203965a2a2c32bff717790ccc4f2c72b5dfb13efba1586d6a49\t00000000 08 06 7d d7 68 a7 02\t1976-05-03T19:36:40.000000Z\n" +
                        "56\tG\t-14345\t178321\t-69806\t0.49869508\t0.6847998121758173\t1974-01-20T21:14:56.131Z\t1970-01-01T00:00:00.463647Z\tNSIID\tbbbbbb\tfalse\taaa\t0xca42b6d6997edbf080188ba249467388ad8e3c136f645a57895df157b1395e4a\t00000000 6c 80 c7 bd 2d 37 3e 30\t1976-12-21T07:10:00.000000Z\n" +
                        "113\tC\t24888\t343744\tnull\tnull\t0.16178961011999748\t1975-03-28T01:50:13.399Z\t1969-12-31T23:59:55.139343Z\tVOJM\tbbbbbb\ttrue\tbbbbbb\t0x9049f886c9351b5cdaa5dffda51d3576abf19dc190f69f2b4073ebcdd52b5e55\t00000000 66 7d 27 8c e6 ef 80\t1977-08-09T18:43:20.000000Z\n" +
                        "87\tZ\t-22371\t505981\t99587\tnull\t0.2410835757980544\t1974-04-20T17:12:15.237Z\t1969-12-31T23:59:53.138973Z\tSPMP\tc\ttrue\tc\t0x32bc4a46e7af0a4f6aba310c55d4a66b726939a19e0078d75d0fae7542ac9d47\t00000000 98 26 52 23 06 24\t1978-03-29T06:16:40.000000Z\n" +
                        "112\tP\t9802\tnull\t75166\t0.118374825\t0.3494250368753934\t1975-09-30T10:09:31.504Z\t1969-12-31T23:59:55.762374Z\t\taaa\tfalse\t\t0x5c0d7d492392e518bc746464a1bc90a182d3dc20f47b3454237a6eab48804528\t00000000 cd b2 fa d5 2a 19 1b c9\t1978-11-15T17:50:00.000000Z\n" +
                        "124\tI\t-30723\t159684\t-45800\t0.057298005\t0.8248769037655503\t1975-09-08T12:04:39.272Z\t1969-12-31T23:59:57.670332Z\tYNOR\taaa\ttrue\t\t0xcd186db59e62b037b98b79aef5acec1fdb8cfee61427b2cc7d13df361ce20ad9\t\t1979-07-05T05:23:20.000000Z\n" +
                        "35\tV\t10152\t937720\tnull\t0.23416382\t0.24387410849395863\t1980-09-02T06:34:03.436Z\t1969-12-31T23:59:56.811075Z\t\tbbbbbb\ttrue\taaa\t0xb892843928ce6cf655b1e0cdb76095985530678f628cf540ac51fd65e909dd2b\t00000000 31 18 f8 f7\t1980-02-21T16:56:40.000000Z\n" +
                        "41\tZ\t26037\tnull\t89462\t0.060157895\tnull\t\t1969-12-31T23:59:54.904714Z\tMMIF\t\tfalse\tbbbbbb\t0x710cc7c02bc588156c43b9f262cc9e0f6931f3fb92b26a54523533e24a77670a\t00000000 58 d7 0b ee a4 44 3d 4e 9d 5e\t1980-10-10T04:30:00.000000Z\n" +
                        "46\tK\t14022\t64332\t-60781\tnull\t0.18417308856834846\t1971-01-26T11:21:09.657Z\t1969-12-31T23:59:55.195798Z\tGVGG\t\ttrue\tc\t0xec20eb4da5ad065ad9caaa437b378e7cc5bf7a38aaeb64829a20ed2c585143a3\t00000000 b9 02 a2 6d c8 d1 3f\t1981-05-29T16:03:20.000000Z\n" +
                        "39\tX\t-8230\tnull\t35694\tnull\t0.3269701313081723\t\t1969-12-31T23:59:54.970857Z\t\taaa\tfalse\tbbbbbb\t0x7af6bbe78536821a8bf4811e7aed057998a23d19950a827feabe384366e6192e\t00000000 ae e3 d4 a9 9f e2 80 4a 8f\t1982-01-16T03:36:40.000000Z\n" +
                        "108\tO\t-4769\t694856\t85538\t0.19972318\t0.8749274453087652\t\t1969-12-31T23:59:55.522272Z\tGQQG\tc\tfalse\tbbbbbb\t0x6146ac1de762ff9b1c4ff53c1a7e75c348a8542e784d0b46894e90960c2b1566\t00000000 0e 9f 19 7a a5 59 1c 46 73 13\t1982-09-04T15:10:00.000000Z\n" +
                        "50\tX\t-27123\t560566\t-82780\t0.41461885\t0.9759549117828917\t1981-07-18T09:19:11.677Z\t1969-12-31T23:59:59.898755Z\t\taaa\tfalse\tbbbbbb\t0x67508865e9fd829377fcbc62132240bba42f8cb906dbfd678a531d95fe4daa38\t00000000 e8 0d d8 46\t1983-04-24T02:43:20.000000Z\n" +
                        "111\tW\t-6198\t884762\t5649\tnull\t0.8884541492501596\t\t1969-12-31T23:59:53.963943Z\tNSEPE\t\tfalse\tbbbbbb\t0xd11cbdb698d1ef4aae1214cc77df7212862a8351d1db91556f2ecb55af0f815b\t00000000 37 6c 12\t1983-12-11T14:16:40.000000Z\n" +
                        "86\tI\t-18677\tnull\t86707\t0.8186389\t0.24968743442275476\t1971-01-31T08:29:50.740Z\t1969-12-31T23:59:57.896244Z\tDBDZ\t\tfalse\tbbbbbb\t0xb457723c814406d987e706dfb816e47365239b4387a4df1148a39f2d3190b0d7\t00000000 ee bc b6 13 7f 1a d1\t1984-07-30T01:50:00.000000Z\n" +
                        "23\tV\t9344\t937468\t83002\tnull\t0.9087477391550631\t\t1969-12-31T23:59:52.816621Z\tDQSJE\taaa\tfalse\taaa\t0xbeed39c0ca2d6240b48eca1f317663704544fc9ecbcca1a7380e208857582bdb\t\t1985-03-18T13:23:20.000000Z\n" +
                        "23\tL\t17130\t50424\t-91882\t0.70989305\t0.03485868034117512\t1974-10-10T00:42:59.711Z\t1969-12-31T23:59:56.393043Z\tDOVTB\taaa\tfalse\taaa\t0x0db35b850a0c86a863bf891222dca6dc7bcf843650f3904384f84eb517304fcd\t00000000 ae 18 29\t1985-11-05T00:56:40.000000Z\n" +
                        "27\tO\t-4792\tnull\tnull\t0.19139338\t0.5861227201637977\t\t1969-12-31T23:59:55.767548Z\tYXVN\tc\ttrue\taaa\t0x7fd8b541faab2da1b7711dca20fee22bbfc672ef949ba13cb63f0a64aba2d306\t00000000 77 db 6a 54 0e 5a 51\t1986-06-24T12:30:00.000000Z\n" +
                        "25\tZ\t-6353\t96442\t-49636\tnull\t0.40549501732612403\t1971-07-13T02:33:26.128Z\t1969-12-31T23:59:55.204464Z\t\t\tfalse\t\t0xb9b7551ff3f855ab6cf5a8f96caceffc3ed28138e7ede3ed9ec0ec50fa4f863b\t00000000 bb f4 7c 7a c2 a4 77 a6 8f aa\t1987-02-11T00:03:20.000000Z\n" +
                        "71\tD\t694\t772203\t-32603\t0.9552407\t0.2769130518011955\t1978-12-25T19:08:51.960Z\t1969-12-31T23:59:55.754471Z\t\tc\tfalse\t\t0x310d0e7a171e7f3c97f6387cdfad01f8cd2b0e4fd8b234fba3804ab8de1f8951\t00000000 80 24 24 8c d4 48 55 31 89\t1987-09-30T11:36:40.000000Z\n" +
                        "103\tL\t18557\tnull\t-19361\t0.11996418\tnull\t1970-08-08T18:27:58.955Z\t1969-12-31T23:59:58.439595Z\t\tc\ttrue\t\t0xbc7827bb6dc3ce15b85b002a9fea625b95bdcc28b3dfce1e85524b903924c9c2\t00000000 18 e2 56 c6 ce\t1988-05-18T23:10:00.000000Z\n",
                "t_month"
        );
    }

    private void assertMonthO3() throws SqlException {
        assertSql(
                "a\tb\tc\td\te\tf\tg\th\ti\tj\tk\tl\tm\tn\to\tts\n" +
                        "76\tC\t-6159\tnull\t-47298\tnull\t0.04029576739795815\t1980-04-06T13:17:02.619Z\t1969-12-31T23:59:58.847174Z\tLCDE\taaa\ttrue\t\t0x8e5ff8afa4bb22ba555224aff85d7ae59e70afeef8bf7911cd2730f82982745b\t00000000 4f f8 0b 28 e3 e1 8a\t1970-01-10T06:16:40.000000Z\n" +
                        "89\tR\t16737\t700506\tnull\t0.8182188\t0.48212733968742016\t1975-11-12T07:08:04.487Z\t1969-12-31T23:59:54.459793Z\tOCZXQ\tbbbbbb\tfalse\t\t0xace30dd772e713688fbec5064d883f3d63e84502157622acae3baa02ee6dc8b4\t00000000 73 ac 38 fe\t1970-02-18T14:43:20.000000Z\n" +
                        "56\tN\t-13977\tnull\tnull\t0.36352593\t0.4958708721417553\t\t1969-12-31T23:59:52.185697Z\tMIOXL\t\ttrue\t\t0xc49971608fded028c761f1bc5d5ba1e7abd10e7211586b4e60ca698146bc627f\t00000000 26 48 60 f4 8d\t1970-02-23T05:50:00.000000Z\n" +
                        "120\tC\t20283\t24877\t-32701\t0.84118104\t0.8276500637033738\t1972-04-27T14:53:51.600Z\t1970-01-01T00:00:00.375644Z\tLBCXT\t\ttrue\t\t0x1cd81105b3a98dad50dc5c1fc342d7d2b49280fe8b981e37bd73da8b84fba286\t00000000 7e 48 58 fd 26 62 c1\t1970-02-27T20:56:40.000000Z\n" +
                        "39\tX\t-51\t722893\t-59953\t0.58863914\t0.6523221269876124\t1975-04-04T19:38:40.208Z\t1969-12-31T23:59:53.255009Z\t\tbbbbbb\ttrue\t\t0x650974c3396b94babeadba139e982b7ac53bbab68a52aecd53ee535940320d7f\t00000000 ad 33\t1970-03-04T12:03:20.000000Z\n" +
                        "35\tK\t-24686\t474219\t-44332\t0.25079244\t0.5236698757856622\t1970-08-23T01:09:12.705Z\t1969-12-31T23:59:52.435593Z\t\taaa\ttrue\t\t0x459cc9560b19d18f53c435d3bdade7537d85f7f1e808a3f9b9cd793021444a58\t00000000 d7 a3 d4 71 f6\t1970-03-09T03:10:00.000000Z\n" +
                        "74\tZ\t25607\tnull\t-28785\t0.12615377\t0.040739265188051266\t1976-02-23T13:06:41.835Z\t1969-12-31T23:59:57.049228Z\tPNVF\taaa\ttrue\t\t0x363a46a2eb524da5414d7fa89ef213b068d1deba7f3c9494b1ba038517282210\t00000000 d1 d8 bf 0f 44\t1970-03-11T10:43:20.000000Z\n",
                "t_month_ooo where m = null"
        );

        assertSql(
                "k\n" +
                        "\n" +
                        "aaa\n" +
                        "bbbbbb\n" +
                        "c\n", "select distinct k from t_month_ooo order by k"
        );

        assertSql(
                "count\n" +
                        "30\n", "select count() from t_month_ooo"
        );

        assertSql(
                "a\tb\tc\td\te\tf\tg\th\ti\tj\tk\tl\tm\tn\to\tts\n" +
                        "52\tT\t16706\t68378\t-78657\t0.42829126\t0.7036706174814127\t1979-08-31T00:24:50.963Z\t1970-01-01T00:00:00.025861Z\t\tc\ttrue\taaa\t0xdef84e0878262440d0ba4a828304a4a587b372b44bb0b41a4b2afd479b9f1a5d\t00000000 5e 40 a1 90 1a 89 45\t1970-01-01T00:03:20.000000Z\n" +
                        "75\tO\t-24469\t329502\t68133\t0.20968294\t0.16862809174782356\t1979-08-24T21:09:47.607Z\t1969-12-31T23:59:54.013525Z\tNXOT\t\tfalse\tc\t0x7b989002d6aa701281bbcd72bd1f7e608e4adc097857a6bc65e761ec15e1c225\t00000000 a5 81\t1970-01-03T07:36:40.000000Z\n" +
                        "80\tC\t19182\t591323\t-42206\t0.5648551\t0.396391474383065\t1979-05-14T16:15:11.240Z\t1969-12-31T23:59:57.108301Z\tTDBP\t\ttrue\taaa\t0x9c7f2070b0cc7edfeaa931738c4ad4e99ef0dff8554c78544ac88fc3c5270afa\t00000000 0b 2a e3 59 93 d7 74 29\t1970-01-05T15:10:00.000000Z\n" +
                        "95\tD\t4133\t-33210\tnull\t0.5310245\t0.5175107269089727\t1974-03-26T12:58:30.002Z\t1969-12-31T23:59:52.330043Z\tYWEHF\tc\ttrue\taaa\t0xb1ba46c06a603052aa207b62a4c841daa74d2c63e604400a38baf9691f2df4bb\t00000000 e7 80 1a 01 ca 41 f1 b2 d7 d2\t1970-01-07T22:43:20.000000Z\n" +
                        "76\tC\t-6159\tnull\t-47298\tnull\t0.04029576739795815\t1980-04-06T13:17:02.619Z\t1969-12-31T23:59:58.847174Z\tLCDE\taaa\ttrue\t\t0x8e5ff8afa4bb22ba555224aff85d7ae59e70afeef8bf7911cd2730f82982745b\t00000000 4f f8 0b 28 e3 e1 8a\t1970-01-10T06:16:40.000000Z\n" +
                        "92\tR\t31386\t142790\t-64322\t0.6879977\t0.3677923764894949\t1971-11-27T22:10:28.232Z\t1969-12-31T23:59:52.232189Z\tEHRNV\tbbbbbb\ttrue\taaa\t0x68fde7f52733d3236f60d225c9aba26670d0731b570cb3c86b1f471ba0caef60\t\t1970-01-12T13:50:00.000000Z\n" +
                        "112\tX\t5761\tnull\t-13515\t0.3747481\tnull\t1977-11-25T18:01:47.338Z\t1969-12-31T23:59:59.184119Z\tTWPSY\taaa\tfalse\tc\t0xba795159793baa30a95aec8b1ac7e592928e19470aeb21839a3f5965984021f0\t\t1970-01-14T21:23:20.000000Z\n" +
                        "116\tK\t23745\t407209\tnull\t0.8127893\t0.2964591833933836\t\t\t\taaa\tfalse\taaa\t0x8d946494f95206a9a90d15ea7f57ba72b0737231f15475039fb5ef39709f5e58\t00000000 35 26 62\t1970-01-17T04:56:40.000000Z\n" +
                        "94\tJ\t27157\t35525\t56930\tnull\tnull\t1978-03-02T17:53:07.419Z\t1969-12-31T23:59:52.571813Z\tQTZQD\t\tfalse\tc\t0x3b951d41bcd140ba89a0cc367d7577bb77cf8bef01f93225233169c04307f97b\t\t1970-01-19T12:30:00.000000Z\n" +
                        "75\tJ\t-3580\tnull\t-9112\t0.35322815\tnull\t1977-08-15T07:30:38.221Z\t1969-12-31T23:59:55.422262Z\tTBHN\t\tfalse\tc\t0xea6b749f95e0d51ae9a3bab372dd22e39a757cc58991db9f86cbea14cdebf09d\t00000000 f4 c4 85 cb f8 b1 2a 73 7e 88\t1970-01-21T20:03:20.000000Z\n" +
                        "100\tK\t-2630\t824498\t37222\t0.33879888\t0.3872770161018947\t1972-04-22T00:31:45.747Z\t1969-12-31T23:59:52.459496Z\tIPOFZ\taaa\ttrue\taaa\t0x9a0900f7cd2597e564ecee003285eca04102030bdc7918c87f51eab58e779e4b\t00000000 57 0b 95 28 1e\t1970-01-24T03:36:40.000000Z\n" +
                        "69\tL\t30296\t974380\t68487\tnull\tnull\t1975-12-18T03:18:15.800Z\t1969-12-31T23:59:54.647837Z\tWYOBO\tc\tfalse\tbbbbbb\t0x9f322a70a6423c6b74f6cbd8a4c9d70172041c8526332380be5ab00b4aa15cee\t00000000 93 7f ec 61 8c 98\t1970-01-26T11:10:00.000000Z\n" +
                        "114\tX\t-490\t458006\t-5272\t0.35898328\t0.49230847324498883\t\t1969-12-31T23:59:52.523436Z\tSYOHG\t\tfalse\tbbbbbb\t0xa86e1e4ddbcb3a517b09de6d670d06e4382010bb07cac0836409f95a76202073\t00000000 7a b8 1d 35\t1970-01-28T18:43:20.000000Z\n" +
                        "82\tK\t-30102\t215419\t-89036\t0.72750413\tnull\t\t1970-01-01T00:00:00.121755Z\tBHOPD\taaa\tfalse\tbbbbbb\t0x379b618db3ea9e95341096f3681e0f3b78452cf81ed6114b64f543d22605edeb\t\t1970-01-31T02:16:40.000000Z\n" +
                        "28\tC\t16073\tnull\t-18651\t0.68242544\tnull\t\t1969-12-31T23:59:53.152188Z\tWCJH\taaa\tfalse\tbbbbbb\t0x5170c41c2c7e9fe986bb6a66dbc21d2aee5909790e5d1a098bf1c43993bae56b\t00000000 91 71 a5 ed bd 7c cc 1c 26\t1970-02-02T09:50:00.000000Z\n" +
                        "106\tV\t14541\t889673\t59780\t0.1352424\t0.8741489533047525\t1982-08-13T17:49:22.307Z\t1969-12-31T23:59:58.350462Z\t\tc\ttrue\tc\t0xb597b59d67719c8b96f829f24619bd2bc9c5e5e5cc982e1ad73df9edb71663cb\t00000000 7f 1d 87 08 e0 b1\t1970-02-07T00:56:40.000000Z\n" +
                        "42\tK\t-5320\t863386\t58304\t0.6804409\t0.4610606296625652\t\t1970-01-01T00:00:00.461219Z\tYNLE\taaa\ttrue\tbbbbbb\t0xdc1be99e9e4030bfee6a5e96e8b63bb8b45855c338559d9480d0a1ad8fee719f\t00000000 32 a6 b7 e5 8b 3b\t1970-02-09T08:30:00.000000Z\n" +
                        "97\tS\t27070\t868093\t34640\tnull\t0.28813673096485004\t1979-05-27T04:40:32.951Z\t1969-12-31T23:59:53.543837Z\tKHFMI\tbbbbbb\tfalse\tc\t0x3b6fe34c449aacf17655328bedb92906a815c3e69c772225b3f1338890fc4918\t00000000 dc 44\t1970-02-11T16:03:20.000000Z\n" +
                        "53\tS\t-31442\t852730\t74861\tnull\t0.8959861802210562\t1977-12-28T16:22:13.434Z\t1970-01-01T00:00:00.106444Z\tRJYJX\tbbbbbb\tfalse\tc\t0x390e3993f587f07a5aab095930a8f813a74d4b52092cc831b811d3ecc96dd4ac\t00000000 4d 10 15 fc 19 72\t1970-02-13T23:36:40.000000Z\n" +
                        "115\tI\t18220\t-11965\t-94917\t0.07309085\t0.24261979334147554\t1979-04-28T11:57:39.990Z\t1969-12-31T23:59:58.778213Z\tTCODK\tc\tfalse\taaa\t0xe78c07c28f640f0189f1710e1c678a864d61deaa06d3209794c950a551627f0e\t\t1970-02-16T07:10:00.000000Z\n" +
                        "89\tR\t16737\t700506\tnull\t0.8182188\t0.48212733968742016\t1975-11-12T07:08:04.487Z\t1969-12-31T23:59:54.459793Z\tOCZXQ\tbbbbbb\tfalse\t\t0xace30dd772e713688fbec5064d883f3d63e84502157622acae3baa02ee6dc8b4\t00000000 73 ac 38 fe\t1970-02-18T14:43:20.000000Z\n" +
                        "110\tX\t-8101\t467172\t34280\t0.487001\t0.9426365080361772\t\t1969-12-31T23:59:58.037553Z\tYQDL\tbbbbbb\ttrue\taaa\t0x7d2164549a44f33a060475948250929a718104314d8739a3a1982de2f0b2a569\t\t1970-02-20T22:16:40.000000Z\n" +
                        "56\tN\t-13977\tnull\tnull\t0.36352593\t0.4958708721417553\t\t1969-12-31T23:59:52.185697Z\tMIOXL\t\ttrue\t\t0xc49971608fded028c761f1bc5d5ba1e7abd10e7211586b4e60ca698146bc627f\t00000000 26 48 60 f4 8d\t1970-02-23T05:50:00.000000Z\n" +
                        "64\tR\t-20528\t535246\tnull\t0.5488618\tnull\t1978-09-28T08:03:21.550Z\t1969-12-31T23:59:57.601189Z\tFDWJR\t\tfalse\tc\t0xd792630d1b233450d50c70aa7a631a6281a73f5ba5e67bb740695327e2cbf5dc\t00000000 82 40 f8\t1970-02-25T13:23:20.000000Z\n" +
                        "120\tC\t20283\t24877\t-32701\t0.84118104\t0.8276500637033738\t1972-04-27T14:53:51.600Z\t1970-01-01T00:00:00.375644Z\tLBCXT\t\ttrue\t\t0x1cd81105b3a98dad50dc5c1fc342d7d2b49280fe8b981e37bd73da8b84fba286\t00000000 7e 48 58 fd 26 62 c1\t1970-02-27T20:56:40.000000Z\n" +
                        "41\tE\t-14190\t506083\t-46237\t0.54315954\t0.047501640528959\t\t1969-12-31T23:59:52.353217Z\tNPFG\tbbbbbb\tfalse\tbbbbbb\t0x78b9f0db3bbbf2ec41c0b3ddc8bb31b18b27a973c395ce5e9258f0846b760260\t00000000 fa d5\t1970-03-02T04:30:00.000000Z\n" +
                        "39\tX\t-51\t722893\t-59953\t0.58863914\t0.6523221269876124\t1975-04-04T19:38:40.208Z\t1969-12-31T23:59:53.255009Z\t\tbbbbbb\ttrue\t\t0x650974c3396b94babeadba139e982b7ac53bbab68a52aecd53ee535940320d7f\t00000000 ad 33\t1970-03-04T12:03:20.000000Z\n" +
                        "41\tE\t-17464\t414570\t-44496\tnull\t0.49098295150202786\t1980-09-10T18:34:09.161Z\t1969-12-31T23:59:59.087691Z\tRXWC\tc\tfalse\tc\t0xd4eee3564ee48f798a1c255e732fba3d3b69e155c2eefaa535f7b353cfaeb3f5\t00000000 1b 70 df ce 89 df 09 cd\t1970-03-06T19:36:40.000000Z\n" +
                        "35\tK\t-24686\t474219\t-44332\t0.25079244\t0.5236698757856622\t1970-08-23T01:09:12.705Z\t1969-12-31T23:59:52.435593Z\t\taaa\ttrue\t\t0x459cc9560b19d18f53c435d3bdade7537d85f7f1e808a3f9b9cd793021444a58\t00000000 d7 a3 d4 71 f6\t1970-03-09T03:10:00.000000Z\n" +
                        "74\tZ\t25607\tnull\t-28785\t0.12615377\t0.040739265188051266\t1976-02-23T13:06:41.835Z\t1969-12-31T23:59:57.049228Z\tPNVF\taaa\ttrue\t\t0x363a46a2eb524da5414d7fa89ef213b068d1deba7f3c9494b1ba038517282210\t00000000 d1 d8 bf 0f 44\t1970-03-11T10:43:20.000000Z\n",
                "t_month_ooo"
        );
    }

    private void assertNone() throws SqlException {
        assertSql(
                "a\tb\tc\td\te\tf\tg\th\ti\tj\tk\tl\tm\tn\to\tts\n" +
                        "35\tN\t-27680\t935010\t96731\t0.39818722\t0.6951385535362276\t1977-06-23T09:50:16.611Z\t1969-12-31T23:59:56.584555Z\tUMMZS\taaa\tfalse\t\t0x6282dda91ca20ccda519bc9c0850a07eaa0106bdfb6d9cb66a8b4eb2174a93ff\t00000000 66 94 89\t1970-01-01T00:00:00.000100Z\n" +
                        "123\tW\t-9190\t4359\t53700\t0.5511841\t0.7751886508004251\t\t1969-12-31T23:59:56.997792Z\t\taaa\ttrue\t\t0xb53a88f6d83626dbba48ceb2cd9f1f07ae01938cee1007258d8b20fb9ccc2ead\t00000000 33 6e\t1970-01-01T00:03:20.000100Z\n" +
                        "28\tX\t16531\t290127\tnull\t0.9410396\t0.19073234832401043\t1970-09-08T05:43:11.975Z\t1969-12-31T23:59:58.870246Z\tZNLC\taaa\ttrue\t\t0x54377431fb8f0a1d69f0d9820fd1cadec864e47ae90eb46081c811d8777fbd9e\t00000000 7e f3 04 4a 73 f0 31 3e 55 3e\t1970-01-01T00:06:40.000100Z\n" +
                        "105\tO\t-30317\t914327\t-61430\tnull\t0.9510816389659975\t1970-10-21T18:26:17.382Z\t1970-01-01T00:00:00.156025Z\t\tc\ttrue\t\t0x5b8def4e7a017e884a3c2c504403708b49fb8d5fe0ff283cbac6499e71ce5b30\t00000000 a6 6b\t1970-01-01T00:10:00.000100Z\n" +
                        "111\tP\t-329\t311595\t-45587\t0.8941438\t0.7579806386710018\t1978-03-20T18:13:35.843Z\t1969-12-31T23:59:54.837806Z\tBCZI\taaa\tfalse\t\t0x4099211c7746712f1eafc5dd81b883a70842384e566d4677022f1bcf3743bcd7\t\t1970-01-01T00:20:00.000100Z\n" +
                        "109\tC\t18436\t792419\t-4632\t0.45481485\t0.4127332979349321\t1977-05-27T20:12:14.870Z\t1969-12-31T23:59:56.408102Z\tWXCYX\tc\ttrue\t\t0xeabafb21d80fdee5ee61d9b1164bab329cae4ab2116295ca3cb2022847cd5463\t00000000 52 c6 94 c3 18 c9 7c 70 9f\t1970-01-01T00:36:40.000100Z\n" +
                        "72\tR\t-30107\t357231\tnull\t0.862236\t0.2232959099494619\t1980-03-14T04:19:57.116Z\t1969-12-31T23:59:58.790561Z\tTDTF\taaa\tfalse\t\t0xdb80a4d5776e596386f3c6ec12ee8cfe67efb684ddbe470d799808408f62675f\t\t1970-01-01T01:13:20.000100Z\n",
                "t_none where m = null"
        );

        assertSql(
                "k\n" +
                        "\n" +
                        "aaa\n" +
                        "bbbbbb\n" +
                        "c\n", "select distinct k from t_none order by k"
        );

        assertSql(
                "count\n" +
                        "30\n", "select count() from t_none"
        );

        assertSql(
                "a\tb\tc\td\te\tf\tg\th\ti\tj\tk\tl\tm\tn\to\tts\n" +
                        "35\tN\t-27680\t935010\t96731\t0.39818722\t0.6951385535362276\t1977-06-23T09:50:16.611Z\t1969-12-31T23:59:56.584555Z\tUMMZS\taaa\tfalse\t\t0x6282dda91ca20ccda519bc9c0850a07eaa0106bdfb6d9cb66a8b4eb2174a93ff\t00000000 66 94 89\t1970-01-01T00:00:00.000100Z\n" +
                        "123\tW\t-9190\t4359\t53700\t0.5511841\t0.7751886508004251\t\t1969-12-31T23:59:56.997792Z\t\taaa\ttrue\t\t0xb53a88f6d83626dbba48ceb2cd9f1f07ae01938cee1007258d8b20fb9ccc2ead\t00000000 33 6e\t1970-01-01T00:03:20.000100Z\n" +
                        "28\tX\t16531\t290127\tnull\t0.9410396\t0.19073234832401043\t1970-09-08T05:43:11.975Z\t1969-12-31T23:59:58.870246Z\tZNLC\taaa\ttrue\t\t0x54377431fb8f0a1d69f0d9820fd1cadec864e47ae90eb46081c811d8777fbd9e\t00000000 7e f3 04 4a 73 f0 31 3e 55 3e\t1970-01-01T00:06:40.000100Z\n" +
                        "105\tO\t-30317\t914327\t-61430\tnull\t0.9510816389659975\t1970-10-21T18:26:17.382Z\t1970-01-01T00:00:00.156025Z\t\tc\ttrue\t\t0x5b8def4e7a017e884a3c2c504403708b49fb8d5fe0ff283cbac6499e71ce5b30\t00000000 a6 6b\t1970-01-01T00:10:00.000100Z\n" +
                        "35\tD\t-5793\t89447\t-35803\t0.030030489\t0.8061988461374605\t1980-01-22T14:53:03.123Z\t1969-12-31T23:59:55.867852Z\tEGHL\taaa\ttrue\tc\t0x82fa2d8ff66e01977f4f8e692631f943b2334e48803d047ea14e7377e6f5d520\t00000000 5c d9\t1970-01-01T00:13:20.000100Z\n" +
                        "118\tV\t16219\t688593\tnull\t0.59561634\t0.23956847762469535\t1977-07-31T16:16:34.689Z\t1969-12-31T23:59:59.130577Z\t\t\ttrue\tbbbbbb\t0x955a48b4c0d3e6b4ea7faedda2849361b9bcdfc30f4bd6234d9b57fb6d09ec03\t00000000 03 5b 11 44 83 06 63 2b 58\t1970-01-01T00:16:40.000100Z\n" +
                        "111\tP\t-329\t311595\t-45587\t0.8941438\t0.7579806386710018\t1978-03-20T18:13:35.843Z\t1969-12-31T23:59:54.837806Z\tBCZI\taaa\tfalse\t\t0x4099211c7746712f1eafc5dd81b883a70842384e566d4677022f1bcf3743bcd7\t\t1970-01-01T00:20:00.000100Z\n" +
                        "22\tT\t-24267\t-10028\t63657\t0.7536836\t0.5570298738371094\t\t1970-01-01T00:00:00.483602Z\t\tc\tfalse\taaa\t0xa2192a97fa3480616ba3f8da2d48aa34a5686de9830593f7c707cb8de0171211\t00000000 51 d7 eb b1 07 71\t1970-01-01T00:23:20.000100Z\n" +
                        "102\tS\t-1873\t804344\t-91510\t0.8669667\t0.7374999472642795\t1972-09-16T00:05:16.136Z\t1969-12-31T23:59:58.908865Z\tMZCC\t\ttrue\tc\t0x6c05f272348619932296dd7689d89e416ae51cd947740a0e939e78ab607cc23f\t00000000 ca 10 2f 60 ce 59 1c 79 dd\t1970-01-01T00:26:40.000100Z\n" +
                        "118\tP\t-18809\t425874\t-75558\tnull\t0.2522102209201954\t1980-07-27T11:56:48.011Z\t1969-12-31T23:59:59.168876Z\tKRZU\tbbbbbb\tfalse\tc\t0x73081d3e950671a5c12ecd4e28671aad67e71f507f89fb753923be5f519fda86\t\t1970-01-01T00:30:00.000100Z\n" +
                        "111\tY\t-14488\t952580\t22063\t0.48990256\t0.47768252726422167\t\t1969-12-31T23:59:57.420903Z\tFFLTR\taaa\tfalse\tc\t0x72cc09749762925e4a98f54e6e543538451196db6d823470484849dd2d80d95e\t00000000 8f 10 c3 50 ce 4a 20 0f 7f\t1970-01-01T00:33:20.000100Z\n" +
                        "109\tC\t18436\t792419\t-4632\t0.45481485\t0.4127332979349321\t1977-05-27T20:12:14.870Z\t1969-12-31T23:59:56.408102Z\tWXCYX\tc\ttrue\t\t0xeabafb21d80fdee5ee61d9b1164bab329cae4ab2116295ca3cb2022847cd5463\t00000000 52 c6 94 c3 18 c9 7c 70 9f\t1970-01-01T00:36:40.000100Z\n" +
                        "84\tL\t19528\tnull\t-88960\t0.47833854\t0.1061278014852981\t1979-05-27T12:29:17.272Z\t1969-12-31T23:59:55.725272Z\tRTGZ\tc\ttrue\tbbbbbb\t0x7429f999bffc9548aa3df14bfed429697199d69ef47f96aab53be4222160be9c\t00000000 d6 14 8b 7f 03 4f\t1970-01-01T00:40:00.000100Z\n" +
                        "118\tK\t12829\t858070\t-57757\t0.7399948\t0.654226248740447\t1976-04-25T02:26:30.330Z\t1969-12-31T23:59:55.439991Z\tFWZSG\tbbbbbb\tfalse\tc\t0xa01929b632a0080165ffef9d779142923625ff207eb768073038742f72879e14\t00000000 a1 31\t1970-01-01T00:43:20.000100Z\n" +
                        "73\tR\t32450\t937836\t-72140\tnull\t0.20943619156614035\t1972-02-08T06:20:20.488Z\t1969-12-31T23:59:54.714881Z\tKIBWF\tc\ttrue\taaa\t0xc95c1d5a4869bab982a8831f8a8662d35fa2bf53cef84c2997af9db84b80545e\t00000000 95 fa 1f 92 24 b1 b8 67 65\t1970-01-01T00:46:40.000100Z\n" +
                        "28\tU\t-13320\t939322\t-37762\t0.050598323\t0.9030520258856007\t1974-12-19T14:37:33.525Z\t1970-01-01T00:00:00.692966Z\tKRPC\tbbbbbb\tfalse\tc\t0xe07f8fe692bcc063a85a5fc20776e82b36c1cdbfe34eb2636eec4ffc0b44f925\t00000000 1c 47 7d b6 46 ba bb 98\t1970-01-01T00:50:00.000100Z\n" +
                        "54\tY\t16062\t646278\tnull\t0.47725338\t0.3082260347287745\t1980-03-10T00:39:59.534Z\t1969-12-31T23:59:55.419704Z\tRMDB\tc\tfalse\tc\t0xe1d2020be2cb7be9c5b68f9ea1bd30c789e6d0729d44b64390678b574ed0f592\t00000000 54 02 9f c2 37\t1970-01-01T00:53:20.000100Z\n" +
                        "28\tY\t25787\t419229\t-25197\t0.4970311\t0.48264093321778834\t1979-11-27T15:52:08.376Z\t1969-12-31T23:59:56.532200Z\tVUYGM\taaa\ttrue\tc\t0x8ea5fba6cf9bfc926616c7a12fd0faf9776d2b6ac26ea2865e890a15089598bc\t00000000 92 08 f1 96 7f a0 cf 00\t1970-01-01T00:56:40.000100Z\n" +
                        "84\tV\t8754\t-18574\t16222\t0.09542447\t0.5143498202128743\t1972-10-30T22:56:25.960Z\t1969-12-31T23:59:55.022309Z\tHKKN\t\ttrue\tc\t0x8ef88e7b4b8c0f89e6185e455d7864ace44dd284cc61759cabb37a3474237eba\t00000000 e7 1f eb 30\t1970-01-01T01:00:00.000100Z\n" +
                        "96\tQ\t-16328\t963834\t91698\tnull\t0.1211383169485164\t\t1969-12-31T23:59:52.690565Z\tZJGTB\tbbbbbb\tfalse\tbbbbbb\t0x647a0fb6a2550ff790dd7f3c6ce51e179717e2dae759fdb855724661cfcc811f\t00000000 c1 a7 5c c3 31 17 dd 8d c1 cf\t1970-01-01T01:03:20.000100Z\n" +
                        "20\tX\t-16590\t799945\tnull\tnull\t0.5501791172519537\t1976-06-04T12:13:35.262Z\t1969-12-31T23:59:54.629759Z\tFHXDB\taaa\ttrue\tbbbbbb\t0x49e63c20e929b2c452fa9d86a13a75d798896b6a7d7a7394dba36f0c738708ba\t00000000 fc d2 8e 79 ec\t1970-01-01T01:06:40.000100Z\n" +
                        "90\tB\t-21455\t481425\t-91115\t0.80895203\t0.14319965942499036\t1972-04-24T19:08:46.637Z\t1969-12-31T23:59:54.151602Z\tEODD\tbbbbbb\ttrue\tc\t0x36394efec5b6ad384c7a93c9b229b5ef70e4d39b4fc580388fb2a24b0fac5693\t00000000 cb 8b 64 50 48\t1970-01-01T01:10:00.000100Z\n" +
                        "72\tR\t-30107\t357231\tnull\t0.862236\t0.2232959099494619\t1980-03-14T04:19:57.116Z\t1969-12-31T23:59:58.790561Z\tTDTF\taaa\tfalse\t\t0xdb80a4d5776e596386f3c6ec12ee8cfe67efb684ddbe470d799808408f62675f\t\t1970-01-01T01:13:20.000100Z\n" +
                        "113\tV\t-14509\t351532\t34176\t0.7622604\t0.24633823409315458\t1975-07-10T16:22:32.263Z\t1969-12-31T23:59:53.013535Z\tVTERO\tbbbbbb\ttrue\taaa\t0x93106ae5d36f7edcaddc44581a9b5083831267507abc5f248b4ca194333fe648\t00000000 b9 0f 97 f5 77 7e a3 2d\t1970-01-01T01:16:40.000100Z\n" +
                        "110\tE\t-6421\tnull\t-28244\tnull\t0.300574411191983\t1973-05-31T01:07:59.989Z\t1970-01-01T00:00:00.648812Z\tKNJGS\tbbbbbb\tfalse\taaa\t0x89cdfca4049617afc4ba3ddab10afad6c112d03b0d81666f95a1c05ce5a93104\t00000000 84 d5\t1970-01-01T01:20:00.000100Z\n" +
                        "30\tI\t-9871\t741546\t22371\t0.7672765\tnull\t1970-05-07T02:05:10.859Z\t1969-12-31T23:59:56.465828Z\tKMDC\tbbbbbb\ttrue\tc\t0xbd01cf83632884ae8b7083f888554b0c90a55c025349360f709e6f59acfd4c27\t00000000 a1 ce bf 46 36 0d 5b\t1970-01-01T01:23:20.000100Z\n" +
                        "85\tI\t-3438\tnull\t-17738\t0.6571365\t0.06194919049264147\t1971-10-28T15:11:37.298Z\t1969-12-31T23:59:57.991632Z\tHCTIV\tbbbbbb\ttrue\tbbbbbb\t0x4419df686878377072755f7fe4dcbef46c87ca839f7f85336695f15390d20b0f\t00000000 ba 0b 3a\t1970-01-01T01:26:40.000100Z\n" +
                        "124\tB\t-17121\t-5234\t-51881\t0.4302147\tnull\t1978-02-23T09:04:24.042Z\t1969-12-31T23:59:58.232565Z\t\t\ttrue\tc\t0x9dbf5fdd03586800c0c0031b11dca914a7f9a315f2636c92b3ab2c18e5048644\t\t1970-01-01T01:30:00.000100Z\n" +
                        "74\tP\t-6108\t692153\t-13210\t0.9183027\t0.4972049796950656\t1975-12-19T18:26:40.375Z\t1970-01-01T00:00:00.503530Z\t\t\ttrue\tbbbbbb\t0x70a45fae3a920cb74e272e9dfde7bb12618178f7feba5021382a8c47a28fefa4\t\t1970-01-01T01:33:20.000100Z\n" +
                        "41\tS\t14835\t790240\t20957\tnull\t0.30583440932161066\t1977-04-04T23:35:16.699Z\t1969-12-31T23:59:55.741431Z\tWKGZ\tbbbbbb\tfalse\tc\t0x266d0651dbeb8bcf8c484ee474dc1f93b352346df86ce19f9846b46e3dbd5e87\t00000000 54 67 7d 39 dc 8c 6c 6b ac\t1970-01-01T01:36:40.000100Z\n",
                "t_none"
        );
    }

    private void assertNoneNts() throws SqlException {
        assertSql(
                "a\tb\tc\td\te\tf\tg\th\ti\tj\tk\tl\tm\tn\to\n" +
                        "120\tB\t10795\t469264\t11722\t0.625966\tnull\t\t1969-12-31T23:59:55.466926Z\tQSRL\taaa\tfalse\t\t0x2f171b3f06f6387d2fd2b4a60ba2ba3b45430a38ef1cd9dc5bee3da4840085a6\t00000000 92 fe 69 38 e1 77 9a e7 0c 89\n" +
                        "71\tB\t18904\t857617\t31118\t0.18336213\t0.6455967424250787\t1976-10-15T11:00:30.016Z\t1970-01-01T00:00:00.254417Z\tKRGII\taaa\ttrue\t\t0x3eef3f158e0843624d0fa2564c3517679a2dfd07dad695f78d5c4bed8432de98\t00000000 b0 ec 0b 92 58 7d 24 bc 2e 60\n" +
                        "28\tQ\t-11530\t640914\t-54143\t0.32287866\t0.6852762111021103\t\t1969-12-31T23:59:52.759890Z\tJEUK\taaa\ttrue\t\t0x93e0ee36dbf0e422654cc358385f061661bd22a0228f78a299fe06bcdcb3a9e7\t00000000 1c 9c 1d 5c\n" +
                        "87\tL\t-2003\t842028\t12726\t0.7280037\tnull\t1982-02-21T09:56:08.687Z\t1969-12-31T23:59:52.977649Z\tFDYP\taaa\tfalse\t\t0x6fc129c805c2f5075fd086d7ede63e98492997ce455a2c2962e616e3a640fbca\t00000000 5b d6 cf 09 69 01\n" +
                        "115\tN\t312\t335480\t4602\t0.14756352\t0.23673087740006105\t\t1969-12-31T23:59:57.132665Z\tRTPIQ\taaa\tfalse\t\t0xb139c160cb54bc065e565229e0e964b83e2dbd2d9d8d4081a00e467f6e96e622\t00000000 af 38 71 1f e1 e4 91 7d e9\n" +
                        "27\tJ\t-15254\t978974\t-36356\t0.7910659\t0.7128505998532723\t1976-03-14T08:19:05.571Z\t1969-12-31T23:59:56.726487Z\tPZNYV\t\ttrue\t\t0x39af691594d0654567af4ec050eea188b8074532ac9f3c87c68ce6f3720e2b62\t00000000 20 13\n",
                "t_none_nts where m = null"
        );

        assertSql(
                "count\n" +
                        "30\n", "select count() from t_none_nts"
        );

        assertSql(
                "k\n" +
                        "\n" +
                        "aaa\n" +
                        "bbbbbb\n" +
                        "c\n", "select distinct k from t_none_nts order by k"
        );
        assertSql(
                "a\tb\tc\td\te\tf\tg\th\ti\tj\tk\tl\tm\tn\to\n" +
                        "76\tT\t-11455\t269293\t-12569\tnull\t0.9344604857394011\t1975-06-01T05:39:43.711Z\t1969-12-31T23:59:54.300840Z\tXGZS\t\tfalse\tbbbbbb\t0xa0d8cea7196b33a07e828f56aaa12bde8d076bf991c0ee88c8b1863d4316f9c7\t\n" +
                        "84\tG\t21781\t18201\t-29318\t0.87567717\t0.1985581797355932\t1972-06-01T21:02:08.250Z\t1969-12-31T23:59:59.060058Z\tWLPDX\tbbbbbb\tfalse\taaa\t0x4c0094500fbffdfe76fb2001fe5dfb09acea66fbe47c5e39bccb30ed7795ebc8\t00000000 30 78\n" +
                        "70\tE\t-7374\t-56838\t84125\t0.78830653\t0.810161274171258\t1971-03-15T07:13:33.865Z\t1969-12-31T23:59:54.241410Z\t\tbbbbbb\ttrue\tc\t0xb771e27f939096b9c356f99ae70523b585b80cec619f91784cd64b0b0a344f8e\t\n" +
                        "29\tM\t-5637\t-18625\t-72619\t0.38642335\t0.92050039469858\t1973-11-03T02:24:00.817Z\t1969-12-31T23:59:54.204688Z\t\tc\tfalse\taaa\t0xa5f80be4b45bf437492990e1a29afcac07efe23cedb3250630d46a3a4749c41d\t00000000 82 89 2b 4d\n" +
                        "25\tS\t32326\t260025\t-94148\t0.81641823\t0.8685154305419587\t1974-02-11T22:27:22.302Z\t1969-12-31T23:59:54.568746Z\tWCKY\taaa\tfalse\taaa\t0x8b4e4831499fc2a526567f4430b46b7f78c594c496995885aa1896d0ad3419d2\t00000000 4a 9d 46 7c 8d dd 93 e6\n" +
                        "120\tB\t10795\t469264\t11722\t0.625966\tnull\t\t1969-12-31T23:59:55.466926Z\tQSRL\taaa\tfalse\t\t0x2f171b3f06f6387d2fd2b4a60ba2ba3b45430a38ef1cd9dc5bee3da4840085a6\t00000000 92 fe 69 38 e1 77 9a e7 0c 89\n" +
                        "32\tV\t21347\tnull\t-51252\t0.56994444\t0.9820662735672192\t1973-10-01T11:12:39.426Z\t1969-12-31T23:59:52.627140Z\tHRIPZ\taaa\ttrue\tbbbbbb\t0x89661af328d0e234d7eb56647bc4ff5794f1056ca855461d8c7d2ff9c4797b43\t\n" +
                        "71\tB\t18904\t857617\t31118\t0.18336213\t0.6455967424250787\t1976-10-15T11:00:30.016Z\t1970-01-01T00:00:00.254417Z\tKRGII\taaa\ttrue\t\t0x3eef3f158e0843624d0fa2564c3517679a2dfd07dad695f78d5c4bed8432de98\t00000000 b0 ec 0b 92 58 7d 24 bc 2e 60\n" +
                        "70\tZ\t-26101\t222052\tnull\t0.05158454\t0.750281471677565\t1977-07-02T02:10:59.118Z\t1969-12-31T23:59:54.759805Z\t\t\ttrue\taaa\t0x7e25b91255572a8f86fd0ebdb6707e478b12abb31e08a62758e4b9de341dcbab\t\n" +
                        "55\tQ\t-24452\tnull\t79091\t0.050941825\tnull\t1982-04-02T20:50:18.862Z\t1969-12-31T23:59:59.076086Z\tTWNWI\tbbbbbb\tfalse\tbbbbbb\t0x7db4b866b1f58ae47cb0e477eaa56479c23d0fe108be331ae7d356cb104e507a\t00000000 3a dc 5c 65 ff 27 67 77 12\n" +
                        "44\tV\t-16944\t506196\t62842\t0.68108517\t0.20727557301543031\t1970-06-16T21:34:17.821Z\t1969-12-31T23:59:54.363025Z\t\tc\tfalse\tbbbbbb\t0x99904624c49b6d8a7d85ee2916b209c779406ab1f85e333a800f40e7a7d50d70\t00000000 cd 47 0b 0c 39\n" +
                        "114\tF\t-22523\t939724\t-3793\t0.70794505\t0.03973283003449557\t1973-08-04T17:12:26.499Z\t1969-12-31T23:59:58.398062Z\tUQDYO\taaa\tfalse\tbbbbbb\t0x6d5992f2da279bf54f1ae0eca85e79fa9bae41871fd934427cbab83425e7712e\t\n" +
                        "105\tT\t29923\t529048\t-1987\t0.8196554\t0.8407989131363496\t1970-01-16T02:45:17.567Z\t1969-12-31T23:59:55.475362Z\tGTNLE\taaa\tfalse\taaa\t0x79423d4d320d2649767a4feda060d4fb6923c0c7d965969da1b1140a2be25241\t00000000 49 96 cf 2b b3 71 a7\n" +
                        "107\tD\t-1263\t408527\tnull\t0.76532555\t0.1511578096923386\t1972-09-19T00:27:27.667Z\t1969-12-31T23:59:58.883048Z\tVFGP\tbbbbbb\ttrue\tbbbbbb\t0x13e2c5f1f106cfe2181d04a53e6dc020180c9457eb5fa83d71c1d71dab71b171\t00000000 64 43 84 55 a0 dd 44 11 e2\n" +
                        "21\tN\t22350\t944298\t14332\t0.4349324\t0.11296257318851766\t1980-07-28T12:55:01.616Z\t1969-12-31T23:59:58.440889Z\tGRMDG\tc\tfalse\tbbbbbb\t0x32ee0b100004f6c45ec6d73428fb1c01b680be3ee552450eef8b1c47f7e7f9ec\t00000000 13 8f bb 2a 4b af 8f 89\n" +
                        "71\tE\t24975\t910466\t-49474\t0.37286544\t0.7842455970681089\t1977-01-14T05:30:32.213Z\t1969-12-31T23:59:53.588953Z\t\tc\ttrue\tc\t0x94812c09d22d975f5220a353eab6ca9454ed8c2bc62a7a3bb1f4f1789b74c505\t00000000 dc f8 43 b2\n" +
                        "123\tJ\t-4254\t-22141\tnull\t0.13312209\tnull\t\t1969-12-31T23:59:54.329404Z\t\taaa\ttrue\tc\t0x64a4822086748dc4b096d89b65baebefc4a411134408f49dae7e2758c6eca43b\t00000000 cf fb 9d\n" +
                        "81\tE\t8596\t395835\t79443\t0.5598187\t0.5900836401674938\t1978-04-29T01:58:41.274Z\t1969-12-31T23:59:57.719809Z\tHHMGZ\taaa\ttrue\tbbbbbb\t0x6e87bac2e97465292db016df5ec483152f30671fd02ecc3517a745338e084738\t00000000 c3 2f ed b0 ba 08\n" +
                        "96\tL\t1774\t804030\tnull\t0.5251698\t0.8977957942059742\t1975-03-15T08:49:25.586Z\t1969-12-31T23:59:56.081422Z\tVZNCL\taaa\ttrue\taaa\t0x12ed35e1f90f6b8d5ccc22f4b59eaa47d256526470b1ff197ec1b56d70fe6ce9\t00000000 bc fe b9 52 dd 4d f3 f9 76\n" +
                        "54\tC\t2731\t913355\t52597\tnull\t0.743599174001969\t1970-12-12T18:41:43.397Z\t1969-12-31T23:59:55.075088Z\tHWQXY\t\ttrue\tbbbbbb\t0x8f36ac9372219b207d34861c589dab59aebf198afad2484dcf111b837d7ecc13\t00000000 a1 00 f8 42\n" +
                        "119\tR\t32259\tnull\tnull\t0.73383796\t0.0016532800623808575\t1982-02-19T13:22:55.280Z\t1969-12-31T23:59:54.024711Z\t\taaa\ttrue\taaa\t0x700aee7f321a695da0cd12e6d39f169a9f8806288f4b53ad6ff303216fe26ea0\t00000000 72 d7 97 cb f6\n" +
                        "92\tE\t-21435\t978972\t28032\t0.53806263\t0.7370823954391381\t1979-04-11T04:18:46.284Z\t\tVOCUG\t\tfalse\tc\t0xe6a2713114b420cb73b256fc9f7245e364b4cb5876665e9b5a11e0d21f2a16a3\t00000000 8a b0 35\n" +
                        "120\tS\t-32705\t455919\tnull\t0.31861842\t0.06001827721556019\t1974-06-05T14:26:31.420Z\t1969-12-31T23:59:57.649378Z\tHNOJI\t\tfalse\taaa\t0x71be94dd9da614c69509495193ef4c819645939abaa44b209164f00487d05f10\t\n" +
                        "60\tW\t-27643\tnull\t-15471\t0.07425696\t0.38881940598288367\t1973-07-23T19:51:25.899Z\t1969-12-31T23:59:58.690432Z\tCEBYW\taaa\ttrue\tc\t0x9efeae7dc4ddb201971f4134354bfdc88055ebf2c14f61705f3f358f3f41ca27\t00000000 c9 3a 5b 7e 0e 98 0a 8a 0b 1e\n" +
                        "24\tD\t14242\t64863\t48044\t0.4755193\t0.7617663592833062\t1981-06-04T06:48:30.566Z\t1969-12-31T23:59:58.908420Z\t\tc\ttrue\tbbbbbb\t0x8f5549cf32a4976c5dc373cbc950a49041457ebc5a02a2b542cbd49414e022a0\t\n" +
                        "45\tJ\t26761\t319767\tnull\t0.09662402\t0.5675831821917149\t1978-11-13T05:09:01.878Z\t1970-01-01T00:00:00.569800Z\tRGFI\tbbbbbb\ttrue\tbbbbbb\t0xbba8ab48c3defc985c174a8c1da880da3e292b77c7836d67a62eec8815a4ecb2\t00000000 bf 4f ea 5f\n" +
                        "28\tQ\t-11530\t640914\t-54143\t0.32287866\t0.6852762111021103\t\t1969-12-31T23:59:52.759890Z\tJEUK\taaa\ttrue\t\t0x93e0ee36dbf0e422654cc358385f061661bd22a0228f78a299fe06bcdcb3a9e7\t00000000 1c 9c 1d 5c\n" +
                        "87\tL\t-2003\t842028\t12726\t0.7280037\tnull\t1982-02-21T09:56:08.687Z\t1969-12-31T23:59:52.977649Z\tFDYP\taaa\tfalse\t\t0x6fc129c805c2f5075fd086d7ede63e98492997ce455a2c2962e616e3a640fbca\t00000000 5b d6 cf 09 69 01\n" +
                        "115\tN\t312\t335480\t4602\t0.14756352\t0.23673087740006105\t\t1969-12-31T23:59:57.132665Z\tRTPIQ\taaa\tfalse\t\t0xb139c160cb54bc065e565229e0e964b83e2dbd2d9d8d4081a00e467f6e96e622\t00000000 af 38 71 1f e1 e4 91 7d e9\n" +
                        "27\tJ\t-15254\t978974\t-36356\t0.7910659\t0.7128505998532723\t1976-03-14T08:19:05.571Z\t1969-12-31T23:59:56.726487Z\tPZNYV\t\ttrue\t\t0x39af691594d0654567af4ec050eea188b8074532ac9f3c87c68ce6f3720e2b62\t00000000 20 13\n",
                "t_none_nts"
        );
    }

    private void assertWalTxn() throws SqlException {
        assertSql(
                "x\tm\tts\tдень\tstr\n" +
                        "1\tb\t1970-01-01T01:30:00.000000Z\ta\tUVFMQXL\n" +
                        "4\t\t1970-01-01T04:30:00.000000Z\ta\tTOKMJCP\n" +
                        "9\t\t1970-01-01T09:30:00.000000Z\ta\tNHEBR\n" +
                        "23\tb\t1970-01-01T23:30:00.000000Z\ta\tXNVLE\n" +
                        "29\tc\t1970-01-02T05:30:00.000000Z\ta\tHPVNVKZXSH\n" +
                        "31\tc\t1970-01-02T07:30:00.000000Z\ta\tOHSWKWI\n" +
                        "32\ta\t1970-01-02T08:30:00.000000Z\ta\tLXIJKQZWR\n" +
                        "34\tb\t1970-01-02T10:30:00.000000Z\ta\tPMJWIP\n" +
                        "35\t\t1970-01-02T11:30:00.000000Z\ta\tUTBCSWDH\n" +
                        "36\ta\t1970-01-02T12:30:00.000000Z\ta\tQWYVWLUGT\n" +
                        "3\tc\t1970-01-05T04:30:00.000000Z\ta\tBLJTFSDQIE\n" +
                        "4\t\t1970-01-05T05:30:00.000000Z\ta\tNEL\n" +
                        "7\ta\t1970-01-05T08:30:00.000000Z\ta\tFWMLBWUYKD\n" +
                        "8\tb\t1970-01-05T09:30:00.000000Z\ta\tYQZ\n", "t_col_top_ooo_day_wal where день = 'a'"
        );
    }

    private void assertYear() throws SqlException {
        assertSql(
                "a\tb\tc\td\te\tf\tg\th\ti\tj\tk\tl\tm\tn\to\tts\n" +
                        "92\tF\t28028\tnull\tnull\t0.25543988\t0.18371611611756045\t1981-02-16T02:44:16.585Z\t1969-12-31T23:59:59.627107Z\t\tc\ttrue\t\t0x9de662c110ea4073e8728187e8352d5fc9d79f015b315fe985b29ab573d60b7d\t\t1982-09-04T15:10:00.000000Z\n" +
                        "27\tZ\t16702\t241981\t-97518\tnull\t0.11513682045365181\t1978-06-15T03:10:19.987Z\t1969-12-31T23:59:56.486114Z\tPWGBN\tc\tfalse\t\t0x57f7ca4006c25f277854b37a145d2d6eb1a2709c5a91109c72459255ef20260d\t\t2001-09-09T01:50:00.000000Z\n" +
                        "115\tV\t1407\t828287\t-19261\t0.5616952\t0.6751819291749697\t1976-02-13T23:55:05.568Z\t1970-01-01T00:00:00.341973Z\tPOVR\taaa\tfalse\t\t0xa52d6aeb2bb452b5d536e81d23cc0ccccb3ab47e72e6c8d19f42388a80bda41e\t\t2033-05-18T03:36:40.000000Z\n" +
                        "51\tS\t51\tnull\t3911\t0.54980594\t0.21143168923450806\t1971-07-24T11:46:39.611Z\t1969-12-31T23:59:59.584289Z\tXORR\tc\tfalse\t\t0x793dc8ec3035a4d52f345b2d97c3d52c6cc96e4115ea53bb73f55836b900abc8\t00000000 ac d5 a5 c6 3a 4e 29 ca c3 65\t2090-06-01T11:36:40.000000Z\n" +
                        "83\tL\t-32289\t127321\t40837\t0.1335336\t0.515824820198022\t\t1969-12-31T23:59:53.582959Z\tKFMO\taaa\tfalse\t\t0x8131875cd498c4b888762e985137f4e843b8167edcd59cf345c105202f875495\t\t2109-06-06T22:16:40.000000Z\n",
                "t_year where m = null"
        );

        assertSql(
                "k\n" +
                        "\n" +
                        "aaa\n" +
                        "bbbbbb\n" +
                        "c\n", "select distinct k from t_year order by k"
        );

        assertSql(
                "count\n" +
                        "30\n", "select count() from t_year"
        );

        assertSql(
                "a\tb\tc\td\te\tf\tg\th\ti\tj\tk\tl\tm\tn\to\tts\n" +
                        "112\tY\t18252\t527691\t-61919\t0.60099024\t0.11234979325337158\t1979-08-14T20:45:18.393Z\t1969-12-31T23:59:59.440365Z\tLVKR\tbbbbbb\tfalse\taaa\t0x8d70ff24aa6bc1ef633fa3240281dbf15c141df8f68b896d654ad128c2cc260e\t\t1970-01-01T00:03:20.000000Z\n" +
                        "89\tM\t10579\t312660\t-34472\t0.43812627\t0.9857252839045136\t1976-10-12T22:39:24.028Z\t1969-12-31T23:59:55.269914Z\tXWLUK\taaa\tfalse\tc\t0xead0f34ae4973cadeecfae4b49c48a80ebc4fdd2342465cac8cc5c88521b09ee\t\t1976-05-03T19:36:40.000000Z\n" +
                        "92\tF\t28028\tnull\tnull\t0.25543988\t0.18371611611756045\t1981-02-16T02:44:16.585Z\t1969-12-31T23:59:59.627107Z\t\tc\ttrue\t\t0x9de662c110ea4073e8728187e8352d5fc9d79f015b315fe985b29ab573d60b7d\t\t1982-09-04T15:10:00.000000Z\n" +
                        "109\tS\t-3798\t880448\t40626\t0.35783017\t0.5350104350364769\t1978-07-25T16:54:09.798Z\t1969-12-31T23:59:57.018965Z\tTXDEH\taaa\tfalse\taaa\t0x652829c06d3a3a999da1a84950a96f02d2f84050fd9b3cd5949d89fa0fa954fa\t\t1989-01-05T10:43:20.000000Z\n" +
                        "28\tJ\t-2192\t474110\t-90147\t0.3451512\t0.5690127219513472\t1975-09-08T20:10:23.592Z\t1970-01-01T00:00:00.115667Z\tOVGNC\tbbbbbb\tfalse\tbbbbbb\t0xa65e9c91b87026ec86ad3659d535a73857e804d0b771f6f47998801853f0e22d\t\t1995-05-09T06:16:40.000000Z\n" +
                        "27\tZ\t16702\t241981\t-97518\tnull\t0.11513682045365181\t1978-06-15T03:10:19.987Z\t1969-12-31T23:59:56.486114Z\tPWGBN\tc\tfalse\t\t0x57f7ca4006c25f277854b37a145d2d6eb1a2709c5a91109c72459255ef20260d\t\t2001-09-09T01:50:00.000000Z\n" +
                        "41\tT\t18688\t190380\tnull\t0.47476274\t0.3646620411241238\t1973-06-06T04:07:03.662Z\t1969-12-31T23:59:52.414292Z\tTMPCO\taaa\ttrue\tbbbbbb\t0x5f499e93050b5c576632a8d2d02c3bc99612611e7cf872ae99763daee67eb715\t00000000 2a 12 54 e8 8d d9 0d\t2008-01-10T21:23:20.000000Z\n" +
                        "120\tQ\t-13641\t798379\t66973\t0.06980729\tnull\t\t1969-12-31T23:59:58.766666Z\tMNSZV\tbbbbbb\ttrue\tc\t0xa3100ad5b8f483debaeabfd165574dccd4115bfdaa003943d754367b64aab4b8\t\t2014-05-13T16:56:40.000000Z\n" +
                        "120\tN\t-25478\t906251\t52444\t0.2173773\t0.7214359500445924\t1972-01-14T15:50:49.976Z\t1969-12-31T23:59:53.500256Z\tVRVUO\tbbbbbb\ttrue\tbbbbbb\t0x225a9524d57602fa337e25a5e8454adc232847d409dbb3c6770928b479ba1539\t\t2020-09-13T12:30:00.000000Z\n" +
                        "97\tZ\t-22633\t584739\t80188\t0.281897\t0.531091271619974\t1972-11-05T01:47:18.571Z\t1969-12-31T23:59:57.518586Z\tHOKX\taaa\tfalse\tc\t0x7cc1de102e7a4ab6c760cfffc10d6b96c5d5a2ea72272b52b96a946df47c9238\t\t2027-01-15T08:03:20.000000Z\n" +
                        "115\tV\t1407\t828287\t-19261\t0.5616952\t0.6751819291749697\t1976-02-13T23:55:05.568Z\t1970-01-01T00:00:00.341973Z\tPOVR\taaa\tfalse\t\t0xa52d6aeb2bb452b5d536e81d23cc0ccccb3ab47e72e6c8d19f42388a80bda41e\t\t2033-05-18T03:36:40.000000Z\n" +
                        "108\tR\t-10001\t603147\tnull\t0.098261535\tnull\t1982-01-31T01:47:50.703Z\t1969-12-31T23:59:52.647352Z\tEHIOF\tbbbbbb\ttrue\taaa\t0x6f3213396842e60844fabb05f62eb19b39b02ad7b1317342735884fc062b28e6\t00000000 54 b9 30\t2039-09-18T23:10:00.000000Z\n" +
                        "101\tF\t13128\t689961\t37591\t0.27481902\t0.9418087554045725\t\t1969-12-31T23:59:53.389231Z\t\taaa\ttrue\taaa\t0xf1a20ee3ef468ebc77fda24495e8ae8372eab3da74bf2433af04c21bc27d99d5\t\t2046-01-19T18:43:20.000000Z\n" +
                        "54\tC\t-6357\t275620\t-13434\t0.18731463\t0.6096055633557564\t1974-03-05T02:17:06.113Z\t1969-12-31T23:59:54.708484Z\tXVFG\tbbbbbb\ttrue\tbbbbbb\t0xb155cc19f6c018ac9172b15e727f85ad9d81624b36e54a69deeeb0a6400a52b6\t00000000 8f 48 63 38 66 66 8a 2c f6\t2052-05-22T14:16:40.000000Z\n" +
                        "49\tK\t4850\tnull\tnull\t0.54886574\t0.5606786012693848\t1973-12-30T03:59:24.202Z\t1969-12-31T23:59:57.366085Z\tDVCL\tbbbbbb\ttrue\tbbbbbb\t0x45dbdddf1cd99c527b3011c3aab8d452c66aef1d2d40aec3cbdaab013e4fed0e\t00000000 ca 99\t2058-09-23T09:50:00.000000Z\n" +
                        "91\tJ\t25494\t694565\t-10172\t0.35789376\t0.7057107661934603\t1979-08-28T13:02:30.203Z\t1969-12-31T23:59:57.785346Z\tQNKCY\t\ttrue\taaa\t0xa5b7f979670f70bc99a1c14975e7aed9e2d309dfffa3bfbaad8c21f0583d522d\t00000000 0b 33 a3 d0 07 24 a1\t2065-01-24T05:23:20.000000Z\n" +
                        "20\tU\t7348\t676443\t-19665\tnull\t0.4671328238025483\t1971-09-27T16:40:48.318Z\t1969-12-31T23:59:59.395697Z\tMBWS\tc\tfalse\tbbbbbb\t0x28f473a32e838adc36acb12866a5ddb5972ff0cf1298cf2bbfccf2c193715cb1\t00000000 d2 45 8b 39 ff\t2071-05-28T00:56:40.000000Z\n" +
                        "118\tX\t32736\tnull\tnull\t0.14771074\t0.007535382752122954\t1974-05-12T18:29:30.460Z\t1969-12-31T23:59:53.264943Z\tPSWJ\tbbbbbb\tfalse\tc\t0x35c857e5b83d443313d82d7ebbe3e4cd18e73a1d6f040cf525f019bdde013aa2\t\t2077-09-27T20:30:00.000000Z\n" +
                        "98\tZ\t-8242\t740823\t97395\t0.602944\t0.7146891119224845\t\t1969-12-31T23:59:59.549076Z\tCUGWX\t\ttrue\taaa\t0x4b4eb5f8f81b00661f1aa9638e21fd380d8fba85878633e95e6bbb938ae6feef\t\t2084-01-29T16:03:20.000000Z\n" +
                        "51\tS\t51\tnull\t3911\t0.54980594\t0.21143168923450806\t1971-07-24T11:46:39.611Z\t1969-12-31T23:59:59.584289Z\tXORR\tc\tfalse\t\t0x793dc8ec3035a4d52f345b2d97c3d52c6cc96e4115ea53bb73f55836b900abc8\t00000000 ac d5 a5 c6 3a 4e 29 ca c3 65\t2090-06-01T11:36:40.000000Z\n" +
                        "109\tP\t-12455\t263934\t49960\t0.679264\t0.09831693674866282\t1977-08-08T19:44:03.856Z\t1969-12-31T23:59:55.049605Z\tHQBJP\tc\tfalse\taaa\t0xbe15104d1d36d615cac36ab298393e52b06836c8abd67a44787ce11d6fc88eab\t00000000 37 58 2c 0d b0 d0 9c 57 02 75\t2096-10-02T07:10:00.000000Z\n" +
                        "73\tB\t-1271\t-47644\t4999\t0.858377\t0.12392055368261845\t1975-06-03T19:26:19.012Z\t1969-12-31T23:59:55.604499Z\t\tc\ttrue\taaa\t0x4f669e76b0311ac3438ec9cc282caa7043a05a3edd41f45aa59f873d1c729128\t00000000 34 01 0e 4d 2b 00 fa 34\t2103-02-04T02:43:20.000000Z\n" +
                        "83\tL\t-32289\t127321\t40837\t0.1335336\t0.515824820198022\t\t1969-12-31T23:59:53.582959Z\tKFMO\taaa\tfalse\t\t0x8131875cd498c4b888762e985137f4e843b8167edcd59cf345c105202f875495\t\t2109-06-06T22:16:40.000000Z\n" +
                        "51\tS\t-28311\tnull\t-72973\t0.5956609\t0.20897460269739654\t1973-03-28T21:58:08.545Z\t1969-12-31T23:59:54.332988Z\t\tc\tfalse\taaa\t0x50113ffcc219fb1a9bc4f6389de1764097e7bcd897ae8a54aa2883a41581608f\t00000000 83 94 b5\t2115-10-08T17:50:00.000000Z\n" +
                        "49\tN\t-11147\t392567\t-9830\t0.5247657\t0.1095692511246914\t\t1969-12-31T23:59:56.849475Z\tIFBE\tc\tfalse\taaa\t0x36055358bd232c9d775e2e80754e5fcda2353931c7033ad5c38c294e9227895a\t\t2122-02-08T13:23:20.000000Z\n" +
                        "57\tI\t-22903\t874980\t-28069\tnull\t0.016793228004843286\t1975-09-29T05:10:33.275Z\t1969-12-31T23:59:55.690794Z\tBZSM\t\ttrue\tc\t0xa2c84382c65eb07087cf6cb291b2c3e7a9ffe8560d2cec518dea50b88b87fe43\t00000000 b9 c4 18 2b aa 3d\t2128-06-11T08:56:40.000000Z\n" +
                        "127\tW\t-16809\t288758\t-22272\t0.05352205\t0.5855510665931698\t\t1969-12-31T23:59:52.689490Z\t\taaa\tfalse\tc\t0x918ae2d78481070577c7d4c3a758a5ea3dd771714ac964ab4b350afc9b599b28\t\t2134-10-13T04:30:00.000000Z\n" +
                        "20\tM\t-7043\t251501\t-85499\t0.94033486\t0.9135840078861264\t1977-05-12T19:20:06.113Z\t1969-12-31T23:59:54.045277Z\tHOXL\tbbbbbb\tfalse\tbbbbbb\t0xc3b0de059fff72dbd7b99af08ac0d1cddb2990725a3338e377155edb531cb644\t\t2141-02-13T00:03:20.000000Z\n" +
                        "26\tG\t-24830\t-56840\t-32956\t0.8282491\t0.017280895313585898\t1982-07-16T03:52:53.454Z\t1969-12-31T23:59:54.115165Z\tJEJH\taaa\tfalse\tbbbbbb\t0x16f70de9c6af11071d35d9faec5d18fd1cf3bbbc825b72a92ecb8ff0286bf649\t00000000 c2 62 f8 53 7d 05 65\t2147-06-16T19:36:40.000000Z\n" +
                        "127\tY\t19592\t224361\t37963\t0.6930264\t0.006817672510656014\t1975-11-29T09:47:45.706Z\t1969-12-31T23:59:56.186242Z\t\t\ttrue\taaa\t0x88926dd483caaf4031096402997f21c833b142e887fa119e380dc9b54493ff70\t00000000 23 c3 9d 75 26 f2 0d b5 7a 3f\t2153-10-17T15:10:00.000000Z\n",
                "t_year"
        );
    }

    private void assertYearO3() throws SqlException {
        assertSql(
                "a\tb\tc\td\te\tf\tg\th\ti\tj\tk\tl\tm\tn\to\tts\n" +
                        "30\tY\t28953\t940414\tnull\t0.14894158\t0.7201409952630464\t1973-04-19T19:30:20.534Z\t1969-12-31T23:59:52.777697Z\tCINNT\tc\ttrue\t\t0xa95f8be5525f99c26e4a67960aea1b73283d2a67ee18dcd8598b67e75160219d\t00000000 34 9c\t1972-07-14T22:16:40.000000Z\n" +
                        "20\tG\t-32702\t466493\t10882\t0.55115765\t0.9010953692323533\t1972-07-20T02:41:59.050Z\t1969-12-31T23:59:52.899932Z\tIMDCR\tbbbbbb\ttrue\t\t0xcf43b4a50c2f74dc88b0b3ebba859e225933bd4072be924aadb205299e434030\t\t1972-07-14T22:16:40.000000Z\n" +
                        "88\tF\t22245\t-13115\t-35689\t0.50154144\tnull\t1971-06-28T04:50:13.659Z\t\tPTQJT\tc\tfalse\t\t0xd0095bafed48d2f18aac0415dc543d60482b3adb9e7d7c03336ec052bc6e05d9\t\t1975-01-26T20:30:00.000000Z\n" +
                        "96\tR\t16953\t215865\t61524\tnull\tnull\t1972-10-04T00:24:18.418Z\t1969-12-31T23:59:56.858900Z\tRJYVL\taaa\tfalse\t\t0x11ed4bbe74444f5e5292bb1ea309179799270b730708699d9b2ab296918fa38b\t\t1975-09-15T08:03:20.000000Z\n" +
                        "55\tJ\t-27369\t927084\t95531\t0.18054938\t0.897470515136098\t1975-10-12T05:31:12.519Z\t1969-12-31T23:59:55.291662Z\tSNCQ\tbbbbbb\ttrue\t\t0xd8dd0c4574b8b817b4a20e63c638e3a661ce46dbb8356b314e8c222a0238c229\t00000000 cd dd ee 68 e0\t1976-12-21T07:10:00.000000Z\n" +
                        "30\tH\t-39\tnull\t-59605\t0.26782525\t0.9748023951436231\t1970-07-07T01:26:05.143Z\t1969-12-31T23:59:56.029262Z\tPMYE\t\ttrue\t\t0x3b483074de6cc58a4dbde91603ddb6977f61d7b6275ba8735566b5b68a4470c8\t00000000 88 ed d7\t1978-11-15T17:50:00.000000Z\n",
                "t_year_ooo where m = null"
        );

        assertSql(
                "k\n" +
                        "\n" +
                        "aaa\n" +
                        "bbbbbb\n" +
                        "c\n", "select distinct k from t_year_ooo order by k"
        );

        assertSql(
                "count\n" +
                        "30\n", "select count() from t_year_ooo"
        );

        assertSql(
                "a\tb\tc\td\te\tf\tg\th\ti\tj\tk\tl\tm\tn\to\tts\n" +
                        "30\tE\t14468\t886907\tnull\t0.88576806\t0.31572235047445973\t1981-01-11T21:58:27.911Z\t1970-01-01T00:00:00.415843Z\tVBRV\t\ttrue\tbbbbbb\t0x96cc4fbcf71c771f8c667acbcfd84633b80d15f0235584bbbc044a6042cc92e7\t00000000 a6 c4 93 ba e3 49 e9 00 72 9a\t1970-01-01T00:03:20.000000Z\n" +
                        "113\tU\t-2695\t631042\t86722\t0.8057087\t0.22402711640228767\t1980-05-14T09:54:57.177Z\t1969-12-31T23:59:52.124262Z\tFRWQ\tc\tfalse\taaa\t0x5a7919272adc71e952998dfe330e0c8444b617902e3a8304aaeb546ee505d2fc\t00000000 7a 9e 98 62 91 37 59 e6 c6\t1970-01-01T00:03:20.000000Z\n" +
                        "69\tP\t-3926\tnull\t40091\t0.8425074\t0.8544443116640783\t1973-01-17T18:40:51.857Z\t1969-12-31T23:59:58.817711Z\tGWLUJ\tbbbbbb\tfalse\taaa\t0xcb3c99a59632ac4799f28b74cec0474e4bc16b0a153acc8e8c59b84b0b5d67a7\t00000000 0f eb 4c 0b 80 75 b8 95\t1970-08-20T11:36:40.000000Z\n" +
                        "84\tL\t-17526\t634700\t-85077\t0.10888952\t0.6553548904692522\t1972-04-04T08:38:10.019Z\t1969-12-31T23:59:53.704195Z\t\tbbbbbb\tfalse\taaa\t0xcf4592fecc265767932a38f5e88af3fa86b332869d1b4ce2540d9372bb66e9bf\t00000000 13 37 0a 4f\t1970-08-20T11:36:40.000000Z\n" +
                        "58\tP\t-26649\tnull\t-52716\t0.76011986\tnull\t\t1969-12-31T23:59:54.485298Z\tGGWS\tbbbbbb\ttrue\tbbbbbb\t0x81c4016ca47000eac24045cec8ced74ad03ed320869f0089d7ae13f8c65f171b\t00000000 ba 75 42 71 c1 9c\t1971-04-08T23:10:00.000000Z\n" +
                        "89\tP\t3916\t33585\tnull\tnull\t0.6566348556397864\t1979-08-24T19:15:35.854Z\t\t\taaa\tfalse\tc\t0xd5fe1fa737a93eeb7f4ccc7af35cb15f100903355479fa813c6b947abbb88504\t00000000 45 db 35 83 6d 84\t1971-04-08T23:10:00.000000Z\n" +
                        "57\tZ\t8635\t145531\t5481\t0.51154476\t0.3760853023577718\t1979-05-07T06:33:31.960Z\t1970-01-01T00:00:00.230019Z\tEXVM\taaa\ttrue\taaa\t0x226d162fd8fb1f8a4f10dc8a88ded0417a53808de72edbc45b9937de926e4cf1\t\t1971-11-26T10:43:20.000000Z\n" +
                        "82\tV\t-3177\t222912\t16511\t0.9807362\t0.10738520400294715\t1975-03-28T09:38:36.927Z\t1969-12-31T23:59:55.200488Z\tFMOW\t\tfalse\tc\t0x61ede67c0a12ff1e6b8a3e8aed4b1273c4540ce4d1327e646854dcd31a4c3b23\t00000000 a4 44 83\t1971-11-26T10:43:20.000000Z\n" +
                        "30\tY\t28953\t940414\tnull\t0.14894158\t0.7201409952630464\t1973-04-19T19:30:20.534Z\t1969-12-31T23:59:52.777697Z\tCINNT\tc\ttrue\t\t0xa95f8be5525f99c26e4a67960aea1b73283d2a67ee18dcd8598b67e75160219d\t00000000 34 9c\t1972-07-14T22:16:40.000000Z\n" +
                        "20\tG\t-32702\t466493\t10882\t0.55115765\t0.9010953692323533\t1972-07-20T02:41:59.050Z\t1969-12-31T23:59:52.899932Z\tIMDCR\tbbbbbb\ttrue\t\t0xcf43b4a50c2f74dc88b0b3ebba859e225933bd4072be924aadb205299e434030\t\t1972-07-14T22:16:40.000000Z\n" +
                        "121\tL\t19757\t406439\t37945\t0.08539766\t0.7586845205228345\t1977-04-28T12:42:50.036Z\t1969-12-31T23:59:55.504894Z\tKMVSI\taaa\ttrue\taaa\t0x4a2a20a846be3ece98a801d0177bb11a6786ded1720890cd09b976925503d440\t00000000 c6 ec 59\t1973-03-03T09:50:00.000000Z\n" +
                        "32\tC\t-6440\tnull\t-92459\tnull\tnull\t\t1970-01-01T00:00:00.656222Z\t\t\ttrue\tbbbbbb\t0x92c7fd6f34274449cd144d84e7cc98e39bd071bf17f9949b3bfe8c70b742f51a\t00000000 a5 5c ae ed 86 eb 05\t1973-03-03T09:50:00.000000Z\n" +
                        "117\tL\t-15020\t454612\t-72371\t0.5150547\t0.6735927305413085\t\t1969-12-31T23:59:53.224828Z\t\taaa\ttrue\tbbbbbb\t0x937eb26b92dfa486720b23656233b262baf875d7d3daee70abd407d70bfc5a99\t00000000 a5 b5 9e 30 57\t1973-10-20T21:23:20.000000Z\n" +
                        "94\tV\t17451\tnull\t-77463\t0.8449852\tnull\t1975-09-05T12:28:55.858Z\t1969-12-31T23:59:56.130900Z\t\taaa\tfalse\tbbbbbb\t0xe4b09cba2585460abdfba5b5f9f389bb44f4371843728b38407e976ce3bce45b\t\t1973-10-20T21:23:20.000000Z\n" +
                        "20\tV\t13404\t741055\tnull\tnull\t0.6531751152441161\t1982-07-20T14:24:59.486Z\t1970-01-01T00:00:00.413642Z\tPGQHU\t\ttrue\taaa\t0x4d4295eaba359d2d6d6874818b5f00f880a2e85f8b388eb1b94ac5bae67cb4f7\t00000000 39 83 94\t1974-06-09T08:56:40.000000Z\n" +
                        "97\tT\t30348\tnull\t8911\t0.040170252\t0.6932671141793485\t1980-01-16T08:26:45.505Z\t1969-12-31T23:59:59.284446Z\tZQRX\t\ttrue\tbbbbbb\t0x7e990b52f5cb94959bf25a46aa5cd34975d22ffed02ea3862e9e238f49fcb13d\t00000000 0b 0c c2 db f5\t1974-06-09T08:56:40.000000Z\n" +
                        "88\tF\t22245\t-13115\t-35689\t0.50154144\tnull\t1971-06-28T04:50:13.659Z\t\tPTQJT\tc\tfalse\t\t0xd0095bafed48d2f18aac0415dc543d60482b3adb9e7d7c03336ec052bc6e05d9\t\t1975-01-26T20:30:00.000000Z\n" +
                        "20\tT\t-7964\t487134\tnull\tnull\t0.32481701097934323\t1974-11-07T16:01:35.907Z\t1969-12-31T23:59:59.891530Z\t\taaa\tfalse\tc\t0x52dcf8397eac98ef5c7fbc9524fe6d5978dd04a27388bd37af091f2ca528ddf3\t\t1975-01-26T20:30:00.000000Z\n" +
                        "30\tE\t31733\t397670\t-81518\t0.68707186\t0.22867035293136528\t1975-01-02T14:01:10.618Z\t1969-12-31T23:59:59.654430Z\tGCJDV\t\ttrue\tbbbbbb\t0x407df33a90d0f1f44251f05e203c6dd9939494de7eed643eda57a93265e1eb84\t\t1975-09-15T08:03:20.000000Z\n" +
                        "96\tR\t16953\t215865\t61524\tnull\tnull\t1972-10-04T00:24:18.418Z\t1969-12-31T23:59:56.858900Z\tRJYVL\taaa\tfalse\t\t0x11ed4bbe74444f5e5292bb1ea309179799270b730708699d9b2ab296918fa38b\t\t1975-09-15T08:03:20.000000Z\n" +
                        "38\tU\t-28620\t409568\t83809\t0.24511242\t0.25770104717094067\t1979-06-18T07:13:42.738Z\t1969-12-31T23:59:57.566601Z\tRMKLJ\tc\tfalse\tc\t0x7f43807b07535592603732454d94094e4443d81103f8131a75b74fcd9a50133b\t00000000 de fe 0e\t1976-05-03T19:36:40.000000Z\n" +
                        "75\tN\t-29738\tnull\t40164\t0.90402895\t0.9650799281003015\t1981-11-24T03:54:48.256Z\t1969-12-31T23:59:53.574580Z\t\tc\tfalse\tbbbbbb\t0xc99a8163c17b3ff9b1f85a5d7a4009a96f4d325aa734bef683a57bdd2374eb9d\t00000000 20 00\t1976-05-03T19:36:40.000000Z\n" +
                        "124\tB\t-8962\t297563\t-7445\t0.31634915\t0.3981679625290535\t1971-04-23T00:21:15.212Z\t1969-12-31T23:59:52.570474Z\t\tbbbbbb\ttrue\taaa\t0x6b6217ae8d71ba80af9fc14bce3cc2968263467bcaf1ea393870dad4697de33d\t\t1976-12-21T07:10:00.000000Z\n" +
                        "55\tJ\t-27369\t927084\t95531\t0.18054938\t0.897470515136098\t1975-10-12T05:31:12.519Z\t1969-12-31T23:59:55.291662Z\tSNCQ\tbbbbbb\ttrue\t\t0xd8dd0c4574b8b817b4a20e63c638e3a661ce46dbb8356b314e8c222a0238c229\t00000000 cd dd ee 68 e0\t1976-12-21T07:10:00.000000Z\n" +
                        "109\tI\t-1994\t158125\t-37445\t0.90728426\tnull\t\t1969-12-31T23:59:54.144775Z\tCPWVE\taaa\tfalse\tbbbbbb\t0x59196c786d68585c6572182e069eefcdb2a403b44dc36cc982ce57a0a914d293\t00000000 61 75 d8 f2\t1977-08-09T18:43:20.000000Z\n" +
                        "65\tW\t31029\t155464\t19282\t0.7803245\t0.0862969811569263\t1980-06-14T10:26:11.590Z\t1969-12-31T23:59:56.595899Z\t\tbbbbbb\tfalse\tc\t0x888bf3c7dbd0143b79df4ef99efe0dba8fb872091fa178c6ccf47c760e3b4d37\t\t1977-08-09T18:43:20.000000Z\n" +
                        "86\tM\t562\tnull\t-32827\t0.042348206\t0.3223371926728724\t1970-12-08T11:09:40.315Z\t1969-12-31T23:59:58.764688Z\tIUNM\t\ttrue\taaa\t0xbf3517c7b6484d12bd3ebedf47746a42c82b2a2afb88fb619de9e1ba0b0a0367\t00000000 d2 04 02 4a e7\t1978-03-29T06:16:40.000000Z\n" +
                        "124\tT\t-11263\t816098\t3226\t0.57710207\t0.6861586888119515\t1970-09-07T07:15:07.438Z\t1969-12-31T23:59:52.423847Z\tFHXM\tc\tfalse\tc\t0xa53f9fb342e92d02af45bc4664a03cc08ec2870883768abf6d607389e20eca5c\t00000000 0b b1 ec\t1978-03-29T06:16:40.000000Z\n" +
                        "87\tZ\t13370\t597583\t-75904\t0.64399195\tnull\t1974-05-11T12:57:45.268Z\t1969-12-31T23:59:54.233609Z\tYUKU\tbbbbbb\ttrue\taaa\t0x878d272bf8aaf1418efa5ede5895af6aab253098b9889b19d6950c05179f68ba\t00000000 12 57 47 87 4e a8 48 36\t1978-11-15T17:50:00.000000Z\n" +
                        "30\tH\t-39\tnull\t-59605\t0.26782525\t0.9748023951436231\t1970-07-07T01:26:05.143Z\t1969-12-31T23:59:56.029262Z\tPMYE\t\ttrue\t\t0x3b483074de6cc58a4dbde91603ddb6977f61d7b6275ba8735566b5b68a4470c8\t00000000 88 ed d7\t1978-11-15T17:50:00.000000Z\n",
                "t_year_ooo"
        );
    }

    @NotNull
    private String commonColumns() {
        return " rnd_byte() a," +
                " rnd_char() b," +
                " rnd_short() c," +
                " rnd_int(-77888, 999001, 2) d," + // ensure we have nulls
                " rnd_long(-100000, 100000, 2) e," + // ensure we have nulls
                " rnd_float(2) f," + // ensure we have nulls
                " rnd_double(2) g," + // ensure we have nulls
                " rnd_date(199999999, 399999999999, 2) h," + // ensure we have nulls
                " cast(rnd_long(-7999999, 800000, 10) as timestamp)  i," + // ensure we have nulls
                " rnd_str(4,5,2) j," +
                " rnd_symbol('aaa','bbbbbb', 'c', null) k," +
                " rnd_boolean() l," +
                " rnd_symbol('aaa','bbbbbb', 'c', null) m," +
                " rnd_long256() n," +
                " rnd_bin(2,10, 2) o";
    }

    private void doMigration(String dataZip, boolean freeTableId, boolean withO3, boolean withColTops, boolean withColTopO3, boolean withWalTxn, boolean ignoreMaxLag) throws Exception {
        if (freeTableId) {
            engine.getTableIdGenerator().close();
        }
        assertMemoryLeak(() -> {
            replaceDbContent(dataZip);
            EngineMigration.migrateEngineTo(engine, ColumnType.VERSION, ColumnType.MIGRATION_VERSION, true);
            engine.reloadTableNames();
            engine.getMetadataCache().onStartupAsyncHydrator();
            assertData(withO3, withColTops, withColTopO3, withWalTxn);
            appendData(withColTopO3, withWalTxn);
            assertAppendedData(withColTopO3, withWalTxn);
            assertMetadataCache(withO3, withColTops, withColTopO3, withWalTxn, ignoreMaxLag);
        });
    }

    private void generateMigrationTables() throws SqlException, NumericException {
        execute(
                "create table t_none_nts as (" +
                        "select" +
                        commonColumns() +
                        " from long_sequence(30)," +
                        "), index(m)"
        );

        execute(
                "create table t_none as (" +
                        "select" +
                        commonColumns() +
                        ", timestamp_sequence(100, 200000000L) ts" +
                        " from long_sequence(30)," +
                        "), index(m) timestamp(ts) partition by NONE"
        );

        execute(
                "create table t_day as (" +
                        "select" +
                        commonColumns() +
                        ", timestamp_sequence(200000000L, 2000000000000L) ts" +
                        " from long_sequence(30)," +
                        "), index(m) timestamp(ts) partition by DAY"
        );

        execute(
                "create table t_month as (" +
                        "select" +
                        commonColumns() +
                        ", timestamp_sequence(200000000L, 20000000000000L) ts" +
                        " from long_sequence(30)," +
                        "), index(m) timestamp(ts) partition by MONTH"
        );

        execute(
                "create table t_year as (" +
                        "select" +
                        commonColumns() +
                        ", timestamp_sequence(200000000L, 200000000000000L) ts" +
                        " from long_sequence(30)," +
                        "), index(m) timestamp(ts) partition by YEAR"
        );

        execute(
                "create table t_day_ooo as (" +
                        "select" +
                        commonColumns() +
                        ", timestamp_sequence(16 * 200000000L + 200000000L, 2000000000000L) ts" +
                        " from long_sequence(15)" +
                        "), index(m) timestamp(ts) partition by DAY"
        );

        execute("insert into t_day_ooo " +
                "select " +
                commonColumns() +
                ", timestamp_sequence(200000000L, 2000000000000L) ts" +
                " from long_sequence(15)"
        );

        execute(
                "create table t_month_ooo as (" +
                        "select" +
                        commonColumns() +
                        ", timestamp_sequence(16 * 200000000000L + 200000000L, 200000000000L) ts" +
                        " from long_sequence(15)" +
                        "), index(m) timestamp(ts) partition by MONTH"
        );

        execute("insert into t_month_ooo " +
                "select " +
                commonColumns() +
                ", timestamp_sequence(200000000L, 200000000000L) ts" +
                " from long_sequence(15)"
        );

        execute(
                "create table t_year_ooo as (" +
                        "select" +
                        commonColumns() +
                        ", timestamp_sequence(200000000L, 20000000000000L) ts" +
                        " from long_sequence(15)," +
                        "), index(m) timestamp(ts) partition by YEAR"
        );

        execute("insert into t_year_ooo " +
                "select " +
                commonColumns() +
                ", timestamp_sequence(200000000L, 20000000000000L) ts" +
                " from long_sequence(15)"
        );

        execute("create table o3_0(a string, b binary, t timestamp) timestamp(t) partition by DAY");

        try (TableWriter w = getWriter("o3_0")) {
            TableWriter.Row r;

            // insert day 1
            long t = MicrosFormatUtils.parseTimestamp("2014-09-10T01:00:00.000000Z");
            for (int i = 0; i < 100; i++) {
                r = w.newRow(t);
                r.putStr(0, null);
                r.putBin(1, null);
                r.append();
                t += 100000L;
            }

            // day 2
            t = MicrosFormatUtils.parseTimestamp("2014-09-11T01:00:00.000000Z");
            for (int i = 0; i < 100; i++) {
                r = w.newRow(t);
                r.putStr(0, null);
                r.putBin(1, null);
                r.append();
                t += 100000L;
            }

            // O3
            t = MicrosFormatUtils.parseTimestamp("2014-09-10T01:00:00.000000Z");
            r = w.newRow(t);
            r.putStr(0, null);
            r.putBin(1, null);
            r.append();

            w.commit();
        }

        execute(
                "create table t_col_top_year as (" +
                        "select " +
                        " x" +
                        ", rnd_symbol('a', 'b', 'c', null) m" +
                        ", timestamp_sequence(200000000L, 20000000000000L) ts" +
                        " from long_sequence(15)," +
                        "), index(m) timestamp(ts) partition by YEAR"
        );

        execute("alter table t_col_top_year add column y long");
        execute("insert into t_col_top_year " +
                "select " +
                " x + 15 as x" +
                ", rnd_symbol('d', 'e', 'f', null) as m" +
                ", timestamp_sequence(200000000L + 15 * 20000000000000L, 20000000000000L) ts" +
                ", x + 15 as y" +
                " from long_sequence(15)"
        );

        execute(
                "create table t_col_top_none as (" +
                        "select x, m, ts from t_col_top_year where x <= 15" +
                        "), index(m) timestamp(ts) partition by NONE"
        );

        execute("alter table t_col_top_none add column y long", sqlExecutionContext);
        execute("insert into t_col_top_none " +
                "select x, m, ts, y from t_col_top_year where x > 15" +
                " from long_sequence(15)"
        );

        execute(
                "create table t_col_top_день as (" +
                        "select " +
                        " x" +
                        ", rnd_symbol('a', 'b', 'c', null) m" +
                        ", timestamp_sequence(2000000000L, 20000000000L) ts" +
                        " from long_sequence(15)," +
                        "), index(m) timestamp(ts) partition by DAY"
        );

        execute("alter table t_col_top_день add column день long");
        execute("insert into t_col_top_день " +
                "select " +
                " x + 15 as x" +
                ", rnd_symbol('d', 'e', 'f', null) as m" +
                ", timestamp_sequence(200000000L + 10 * 20000000000L, 2000000000L) ts" +
                ", x + 15 as y" +
                " from long_sequence(15)"
        );

        createTableWithColumnTops("create table t_col_top_ooo_day as (" +
                "select " +
                " x" +
                ", rnd_symbol('a', 'b', 'c', null) m" +
                ", timestamp_sequence('1970-01-01T01', " + Micros.HOUR_MICROS + "L) ts" +
                " from long_sequence(96)," +
                "), index(m) timestamp(ts) partition by DAY", "t_col_top_ooo_day");

        createTableWithColumnTops(
                "create table t_col_top_ooo_day_wal as (" +
                        "select " +
                        " x" +
                        ", rnd_symbol('a', 'b', 'c', null) m" +
                        ", timestamp_sequence('1970-01-01T01', " + Micros.HOUR_MICROS + "L) ts" +
                        " from long_sequence(96)," +
                        "), index(m) timestamp(ts) partition by DAY WAL",
                "t_col_top_ooo_day_wal"
        );
        drainWalQueue();

        TestUtils.messTxnUnallocated(
                engine.getConfiguration().getFilesFacade(),
                Path.getThreadLocal(root),
                new Rnd(),
                engine.verifyTableName("t_col_top_ooo_day_wal")
        );

        Path from = Path.getThreadLocal(configuration.getDbRoot()).concat("t_col_top_день");
        String copyTableWithMissingPartitions = "t_col_top_день_missing_parts";
        Path to = Path.getThreadLocal2(configuration.getDbRoot()).concat(copyTableWithMissingPartitions);

        TestUtils.copyDirectory(from, to, configuration.getMkDirMode());
        FilesFacade ff = TestFilesFacadeImpl.INSTANCE;

        // Remove first partition
        to.of(configuration.getDbRoot()).concat(copyTableWithMissingPartitions);
        if (!ff.rmdir(to.concat("1970-01-01").put(Files.SEPARATOR))) {
            throw CairoException.critical(ff.errno()).put("cannot remove ").put(to);
        }

        // Rename last partition from 1970-01-04 to 1970-01-04.4
        from.of(configuration.getDbRoot()).concat(copyTableWithMissingPartitions).concat("1970-01-04");
        to.trimTo(0).put(from).put(".4");

        if (ff.rename(from.put(Files.SEPARATOR).$(), to.put(Files.SEPARATOR).$()) != Files.FILES_RENAME_OK) {
            throw CairoException.critical(ff.errno()).put("cannot rename to ").put(to);
        }

        // Remove table name file
        ff.remove(
                to.of(configuration.getDbRoot())
                        .concat(copyTableWithMissingPartitions)
                        .concat(TableUtils.TABLE_NAME_FILE)
                        .$()
        );
        engine.reloadTableNames();
    }
}
