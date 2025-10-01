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

package io.questdb.test.griffin;

import io.questdb.PropServerConfiguration;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.Misc;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.microtime.MicrosFormatCompiler;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.MutableUtf16Sink;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.AbstractTest;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class TableBackupTest extends AbstractTest {
    private static final int ERRNO_EIO = 5;
    private static final StringSink sink1 = new StringSink();
    private static final StringSink sink2 = new StringSink();
    private final boolean isWal;
    private final int partitionBy;
    @Rule
    public TemporaryFolder temp = new TemporaryFolder();
    @Rule
    public TestName testName = new TestName();
    private CharSequence backupRoot;
    private Path finalBackupPath;
    private int finalBackupPathLen;
    private SqlCompiler mainCompiler;
    private CairoConfiguration mainConfiguration;
    private CairoEngine mainEngine;
    private SqlExecutionContext mainSqlExecutionContext;
    private int mkdirsErrno;
    private int mkdirsErrnoCountDown = 0;
    private Path path;
    private int renameErrno;
    private FilesFacade testFf;

    public TableBackupTest(AbstractCairoTest.WalMode walMode, int partitionBy) {
        isWal = walMode == AbstractCairoTest.WalMode.WITH_WAL;
        this.partitionBy = partitionBy;
    }

    @Parameterized.Parameters(name = "{0}-{1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {AbstractCairoTest.WalMode.WITH_WAL, PartitionBy.HOUR},
                {AbstractCairoTest.WalMode.WITH_WAL, PartitionBy.DAY},
                {AbstractCairoTest.WalMode.WITH_WAL, PartitionBy.WEEK},
                {AbstractCairoTest.WalMode.WITH_WAL, PartitionBy.MONTH},
                {AbstractCairoTest.WalMode.WITH_WAL, PartitionBy.YEAR},
                {AbstractCairoTest.WalMode.NO_WAL, PartitionBy.NONE},
                {AbstractCairoTest.WalMode.NO_WAL, PartitionBy.HOUR},
                {AbstractCairoTest.WalMode.NO_WAL, PartitionBy.DAY},
                {AbstractCairoTest.WalMode.NO_WAL, PartitionBy.WEEK},
                {AbstractCairoTest.WalMode.NO_WAL, PartitionBy.MONTH},
                {AbstractCairoTest.WalMode.NO_WAL, PartitionBy.YEAR}
        });
    }

    public static TableToken executeCreateTableStmt(
            @NotNull String tableName,
            int partitionBy,
            boolean isWal,
            int numRows,
            @NotNull CairoEngine engine,
            @NotNull SqlExecutionContext context
    ) throws SqlException {
        engine.execute(
                "CREATE TABLE '" + tableName + "' AS (" +
                        selectGenerator(numRows) +
                        "), INDEX(symbol2 CAPACITY 32) TIMESTAMP(timestamp2) " +
                        "PARTITION BY " + PartitionBy.toString(partitionBy) +
                        (isWal ? " WAL" : " BYPASS WAL"),
                context
        );
        return engine.verifyTableName(tableName);
    }

    public static void executeInsertGeneratorStmt(
            TableToken tableToken,
            int size,
            CairoEngine engine,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        CharSequence insertSql = "INSERT INTO '" + tableToken.getTableName() + "' SELECT * FROM (" + selectGenerator(size) + ')';
        engine.execute(insertSql, sqlExecutionContext);
    }

    public static String testTableName(String tableName, String tableNameSuffix) {
        int idx = tableName.indexOf('[');
        return (idx > 0 ? tableName.substring(0, idx) : tableName) + '_' + tableNameSuffix;
    }

    @Before
    public void setup() throws IOException {
        path = new Path();
        finalBackupPath = new Path();
        mkdirsErrno = -1;
        renameErrno = -1;
        testFf = new TestFilesFacadeImpl() {
            private int nextErrno = -1;

            @Override
            public int errno() {
                if (nextErrno != -1) {
                    int errno = nextErrno;
                    nextErrno = -1;
                    return errno;
                }
                return super.errno();
            }

            @Override
            public int mkdirs(Path path, int mode) {
                if (mkdirsErrno != -1 && --mkdirsErrnoCountDown < 1) {
                    nextErrno = mkdirsErrno;
                    mkdirsErrno = -1;
                    mkdirsErrnoCountDown = 0;
                    return -1;
                }
                return super.mkdirs(path, mode);
            }

            @Override
            public int rename(LPSZ from, LPSZ to) {
                if (renameErrno != -1) {
                    nextErrno = renameErrno;
                    renameErrno = -1;
                    return Files.FILES_RENAME_ERR_OTHER;
                }
                return super.rename(from, to);
            }
        };
        CharSequence root = temp.newFolder(String.format("dbRoot%c%s", Files.SEPARATOR, PropServerConfiguration.DB_DIRECTORY)).getAbsolutePath();
        backupRoot = temp.newFolder("dbBackupRoot").getAbsolutePath();
        mainConfiguration = new DefaultTestCairoConfiguration(root) {
            @Override
            public DateFormat getBackupDirTimestampFormat() {
                return new MicrosFormatCompiler().compile("ddMMMyyyy");
            }

            @Override
            public CharSequence getBackupRoot() {
                return backupRoot;
            }

            @Override
            public @NotNull FilesFacade getFilesFacade() {
                return testFf;
            }

            @Override
            public int getMetadataPoolCapacity() {
                return 1;
            }
        };
        mainEngine = new CairoEngine(mainConfiguration);
        mainCompiler = mainEngine.getSqlCompiler();
        mainSqlExecutionContext = TestUtils.createSqlExecutionCtx(mainEngine);
        File confRoot = new File(PropServerConfiguration.rootSubdir(root, PropServerConfiguration.CONFIG_DIRECTORY));  // dummy configuration
        Assert.assertTrue(confRoot.mkdirs());
        Assert.assertTrue(new File(confRoot, "server.conf").createNewFile());
        Assert.assertTrue(new File(confRoot, "mime.types").createNewFile());
        Assert.assertTrue(new File(confRoot, "log-file.conf").createNewFile());
        Assert.assertTrue(new File(confRoot, "date.formats").createNewFile());
    }

    @After
    public void tearDown() {
        Misc.free(finalBackupPath);
        Misc.free(path);
        Misc.free(mainSqlExecutionContext);
        Misc.free(mainCompiler);
        Misc.free(mainEngine);
    }

    @Test
    public void testBackupDatabase() throws Exception {
        assertMemoryLeak(() -> {
            TableToken table1 = executeCreateTableStmt(testName.getMethodName());
            TableToken table2 = executeCreateTableStmt(table1.getTableName() + "_sugus");
            backupDatabase();
            setFinalBackupPath();
            assertTables(table1);
            assertTables(table2);
            assertDatabase();
        });
    }

    @Test
    public void testBackupDatabaseGeohashColumnsWithColumnTops() throws Exception {
        Assume.assumeTrue(PartitionBy.isPartitioned(partitionBy));
        assertMemoryLeak(() -> {
            TableToken tableToken = executeCreateTableStmt(testName.getMethodName());
            ddlAndDrainWalQueue("alter table " + tableToken.getTableName() + " add column new_g4 geohash(30b)");
            ddlAndDrainWalQueue("alter table " + tableToken.getTableName() + " add column new_g8 geohash(32b)");
            ddlAndDrainWalQueue("INSERT INTO '" + tableToken.getTableName() + "' (new_g4, new_g8, timestamp2) SELECT * FROM (" +
                    " SELECT" +
                    "     rnd_geohash(30)," +
                    "     rnd_geohash(32)," +
                    "     timestamp_sequence(" +
                    "         to_timestamp('2023-04-14T17:00:00', 'yyyy-MM-ddTHH:mm:ss'), " +
                    "         100000L)" +
                    " FROM long_sequence(3))");
            backupDatabase();
            setFinalBackupPath();
            assertTables(tableToken);
            assertDatabase();
        });
    }

    @Test
    public void testBackupMatView() throws Exception {
        Assume.assumeTrue(isWal);
        assertMemoryLeak(() -> {
            String baseTableName = "base_table";
            String viewName = baseTableName + "_mv";
            TableToken matViewToken = executeCreateTableAndMatViewStmt(baseTableName, viewName);
            TableToken baseTableToken = mainEngine.verifyTableName(baseTableName);
            backupTable(baseTableToken);
            setFinalBackupPath();
            assertTableOrMatView(baseTableToken);
            backupMatView(matViewToken);
            setFinalBackupPath(1);
            assertTableOrMatView(matViewToken);
        });
    }

    @Test
    public void testBackupTable() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = executeCreateTableStmt(testName.getMethodName());
            backupTable(tableToken);
            setFinalBackupPath();
            assertTables(tableToken);
        });
    }

    @Test
    public void testCompromisedTableName() throws Exception {
        assertMemoryLeak(() -> {
            try {
                TableToken tableToken = executeCreateTableStmt(testName.getMethodName());
                ddlAndDrainWalQueue("backup table .." + Files.SEPARATOR + tableToken.getTableName());
                Assert.fail();
            } catch (SqlException ex) {
                TestUtils.assertEquals("'.' is an invalid table name", ex.getFlyweightMessage());
            }
        });
    }

    @Test
    public void testIncorrectConfig() throws Exception {
        backupRoot = null;
        assertMemoryLeak(() -> {
            try {
                TableToken tableToken = executeCreateTableStmt(testName.getMethodName());
                backupTable(tableToken);
                Assert.fail();
            } catch (CairoException ex) {
                TestUtils.assertEquals("backup is disabled, server.conf property 'cairo.sql.backup.root' is not set", ex.getFlyweightMessage());
            }
        });
    }

    @Test
    public void testInvalidSql1() throws Exception {
        assertMemoryLeak(() -> {
            try {
                ddlAndDrainWalQueue("backup something");
                Assert.fail();
            } catch (SqlException ex) {
                Assert.assertEquals(7, ex.getPosition());
                TestUtils.assertEquals("expected 'table', 'materialized view' or 'database'", ex.getFlyweightMessage());
            }
        });
    }

    @Test
    public void testInvalidSql2() throws Exception {
        assertMemoryLeak(() -> {
            try {
                ddlAndDrainWalQueue("backup table");
                Assert.fail();
            } catch (SqlException e) {
                Assert.assertEquals(12, e.getPosition());
                TestUtils.assertEquals("expected a table name", e.getFlyweightMessage());
            }
        });
    }

    @Test
    public void testInvalidSql3() throws Exception {
        assertMemoryLeak(() -> {
            try {
                TableToken tableToken = executeCreateTableStmt(testName.getMethodName());
                ddlAndDrainWalQueue("backup table " + tableToken.getTableName() + " tb2");
                Assert.fail();
            } catch (SqlException ex) {
                TestUtils.assertEquals("expected ','", ex.getFlyweightMessage());
            }
        });
    }

    @Test
    public void testInvalidSqlMatViews1() throws Exception {
        assertMemoryLeak(() -> {
            try {
                ddlAndDrainWalQueue("backup materialized");
                Assert.fail();
            } catch (SqlException e) {
                Assert.assertEquals(7, e.getPosition());
                TestUtils.assertEquals("expected 'table', 'materialized view' or 'database'", e.getFlyweightMessage());
            }
        });
    }

    @Test
    public void testInvalidSqlMatViews2() throws Exception {
        assertMemoryLeak(() -> {
            try {
                ddlAndDrainWalQueue("backup materialized view");
                Assert.fail();
            } catch (SqlException e) {
                Assert.assertEquals(24, e.getPosition());
                TestUtils.assertEquals("expected a table name", e.getFlyweightMessage());
            }
        });
    }

    @Test
    public void testMissingTable() throws Exception {
        assertMemoryLeak(() -> {
            try {
                TableToken tableToken = executeCreateTableStmt(testName.getMethodName());
                ddlAndDrainWalQueue("backup table " + tableToken.getTableName() + ", tb2");
                Assert.fail();
            } catch (SqlException e) {
                TestUtils.assertEquals("table does not exist [table=tb2]", e.getFlyweightMessage());
            }
        });
    }

    @Test
    public void testMultipleTable() throws Exception {
        assertMemoryLeak(() -> {
            TableToken token1 = executeCreateTableStmt(testName.getMethodName());
            TableToken token2 = executeCreateTableStmt(token1.getTableName() + "_yip");
            ddlAndDrainWalQueue("backup table " + token1.getTableName() + ", " + token2.getTableName());
            setFinalBackupPath();
            assertTables(token1);
            assertTables(token2);
        });
    }

    @Test
    public void testRenameFailure() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = executeCreateTableStmt(testName.getMethodName());
            renameErrno = ERRNO_EIO;
            try {
                backupTable(tableToken);
                Assert.fail();
            } catch (CairoException ex) {
                Assert.assertTrue(ex.getMessage().startsWith("[5] could not rename "));
            }
            backupTable(tableToken);
            setFinalBackupPath(1);
            assertTables(tableToken);
        });
    }

    @Test
    public void testSuccessiveBackups() throws Exception {
        Assume.assumeTrue(PartitionBy.isPartitioned(partitionBy));
        assertMemoryLeak(() -> {
            TableToken tableToken = executeCreateTableStmt(testName.getMethodName());
            backupTable(tableToken);
            setFinalBackupPath();
            StringSink firstBackup = new StringSink();
            selectAll(tableToken, false, sink1);
            selectAll(tableToken, true, firstBackup);
            Assert.assertEquals(sink1, firstBackup);

            executeInsertGeneratorStmt(tableToken);
            backupTable(tableToken);
            setFinalBackupPath(1);
            assertTables(tableToken);
            // Check previous backup is unaffected
            selectAll(tableToken, true, sink2);
            Assert.assertNotEquals(sink2, firstBackup);
        });
    }

    @Test
    public void testTableBackupDirExists() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = executeCreateTableStmt(testName.getMethodName());
            try (Path path = new Path()) {
                path.of(mainConfiguration.getBackupRoot()).concat("tmp").concat(tableToken).slash$();
                Assert.assertEquals(0, TestFilesFacadeImpl.INSTANCE.mkdirs(path, mainConfiguration.getBackupMkDirMode()));
                backupTable(tableToken);
                Assert.fail();
            } catch (CairoException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "backup dir already exists [path=");
                TestUtils.assertContains(ex.getFlyweightMessage(), ", table=" + tableToken.getTableName() + ']');
            }
        });
    }

    @Test
    public void testTableBackupDirNotWritable() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = executeCreateTableStmt(testName.getMethodName());
            try {
                mkdirsErrno = 13;
                mkdirsErrnoCountDown = 2;
                backupTable(tableToken);
                Assert.fail();
            } catch (CairoException ex) {
                Assert.assertTrue(ex.getMessage().startsWith("[13] could not create backup "));
            }
        });
    }

    private static String selectGenerator(int size) {
        return " SELECT" +
                "     rnd_boolean() bool," +
                "     rnd_char() char," +
                "     rnd_byte(2,50) byte," +
                "     rnd_short() short1," +
                "     rnd_short(10,1024) short2," +
                "     rnd_int() int1," +
                "     rnd_int(0, 30, 2) int2," +
                "     rnd_long() long1," +
                "     rnd_long(100,200,2) long2," +
                "     rnd_float(2) float," +
                "     rnd_double(2) double," +
                "     rnd_date(to_date('2022', 'yyyy'), to_date('2023', 'yyyy'), 2) date," +
                "     rnd_timestamp(" +
                "         to_timestamp('2022', 'yyyy'), " +
                "         to_timestamp('2023', 'yyyy'), " +
                "         2) timestamp1," +
                "     timestamp_sequence(" +
                "         to_timestamp('2023-04-14T17:00:00', 'yyyy-MM-ddTHH:mm:ss'), " +
                "         100000L) timestamp2," +
                "     rnd_symbol(4,4,4,2) symbol1," +
                "     rnd_symbol(4,4,4,2) symbol2," +
                "     rnd_str(3,3,2) string," +
                "     rnd_bin(10, 20, 2) binary," +
                "     rnd_geohash(7) g7," +
                "     rnd_geohash(15) g15," +
                "     rnd_geohash(23) g23," +
                "     rnd_geohash(31) g31," +
                "     rnd_geohash(60) g60," +
                "     rnd_uuid4() uuid," +
                "     rnd_long256() long256" +
                " FROM long_sequence(" + size + ')';
    }

    private void assertDatabase() {
        path.of(mainConfiguration.getDbRoot()).concat(TableUtils.TAB_INDEX_FILE_NAME);
        Assert.assertTrue(Files.exists(path.$()));
        finalBackupPath.trimTo(finalBackupPathLen).concat(mainConfiguration.getDbDirectory()).concat(TableUtils.TAB_INDEX_FILE_NAME);
        Assert.assertTrue(Files.exists(finalBackupPath.$()));

        finalBackupPath.trimTo(finalBackupPathLen).concat(PropServerConfiguration.CONFIG_DIRECTORY).slash$();
        final int trimLen = finalBackupPath.size();
        Assert.assertTrue(Files.exists(finalBackupPath.concat("server.conf").$()));
        Assert.assertTrue(Files.exists(finalBackupPath.trimTo(trimLen).concat("mime.types").$()));
        Assert.assertTrue(Files.exists(finalBackupPath.trimTo(trimLen).concat("log-file.conf").$()));
        Assert.assertTrue(Files.exists(finalBackupPath.trimTo(trimLen).concat("date.formats").$()));

        if (isWal) {
            path.parent().concat(WalUtils.TABLE_REGISTRY_NAME_FILE).putAscii(".0");
            Assert.assertTrue(Files.exists(path.$()));
            finalBackupPath.trimTo(finalBackupPathLen).concat(mainConfiguration.getDbDirectory()).concat(WalUtils.TABLE_REGISTRY_NAME_FILE).putAscii(".0");
            Assert.assertTrue(Files.exists(finalBackupPath.$()));
        }
    }

    private void assertMemoryLeak(TestUtils.LeakProneCode code) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try {
                code.run();
                mainEngine.releaseInactive();
                Assert.assertEquals(0, mainEngine.getBusyWriterCount());
                Assert.assertEquals(0, mainEngine.getBusyReaderCount());
            } finally {
                mainEngine.clear();
            }
        });
    }

    private void assertTableOrMatView(TableToken tableToken) throws Exception {
        selectAll(tableToken, false, sink1);
        selectAll(tableToken, true, sink2);
        TestUtils.assertEquals(sink1, sink2);
    }

    private void assertTables(TableToken tableToken) throws Exception {
        selectAll(tableToken, false, sink1);
        selectAll(tableToken, true, sink2);
        TestUtils.assertEquals(sink1, sink2);

        String sql = "INSERT INTO '" + tableToken.getTableName() + "'(timestamp2) VALUES('2123')";
        executeBackupSqlStmt(sql);
        ddlAndDrainWalQueue(sql);

        selectAll(tableToken, false, sink1);
        selectAll(tableToken, true, sink2);
        TestUtils.assertEquals(sink1, sink2);
    }

    private void backupDatabase() throws SqlException {
        mainCompiler.compile("BACKUP DATABASE", mainSqlExecutionContext);
    }

    private void backupMatView(TableToken matViewToken) throws SqlException {
        mainCompiler.compile("BACKUP MATERIALIZED VIEW \"" + matViewToken.getTableName() + '"', mainSqlExecutionContext);
    }

    private void backupTable(TableToken tableToken) throws SqlException {
        mainCompiler.compile("BACKUP TABLE \"" + tableToken.getTableName() + '"', mainSqlExecutionContext);
    }

    private void ddlAndDrainWalQueue(String sql) throws SqlException {
        mainEngine.execute(sql, mainSqlExecutionContext);
        drainWalQueue();
    }

    private void drainWalQueue() {
        drainWalQueueConditionally(mainEngine);
    }

    private void drainWalQueueConditionally(CairoEngine engine) {
        if (isWal) {
            drainWalQueue(engine);
        }
    }

    private void executeBackupSqlStmt(String sql) throws SqlException {
        try (
                CairoEngine engine = new CairoEngine(new DefaultTestCairoConfiguration(finalBackupPath.toString()));
                SqlExecutionContext context = TestUtils.createSqlExecutionCtx(engine);
                SqlCompiler compiler = engine.getSqlCompiler()
        ) {
            compiler.compile(sql, context).execute(null).await();
            drainWalQueueConditionally(engine);
        }
    }

    private TableToken executeCreateTableAndMatViewStmt(String tableName, String matViewName) throws SqlException {
        mainEngine.execute("create table '" + tableName + "' (sym varchar, price double, ts timestamp) " +
                "timestamp(ts) partition by " + PartitionBy.toString(partitionBy) + " WAL");
        mainEngine.execute("create materialized view '" + matViewName +
                "' as (select sym, last(price) as price, ts from '" + tableName + "' sample by 1h) partition by "
                + PartitionBy.toString(partitionBy));

        drainWalQueue();
        mainEngine.execute("insert into '" + tableName + "' values('gbpusd', 1.320, '2024-09-10T12:01')" +
                ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                ",('jpyusd', 103.21, '2024-09-10T12:02')" +
                ",('gbpusd', 1.321, '2024-09-10T13:02')"
        );

        drainWalQueue();
        drainMatViewQueue(mainEngine);
        TableToken tableToken = mainEngine.verifyTableName(matViewName);
        Assert.assertNotNull(tableToken);
        Assert.assertTrue(tableToken.isMatView());
        return tableToken;
    }

    private TableToken executeCreateTableStmt(String tableName) throws SqlException {
        TableToken tableToken = executeCreateTableStmt(
                testTableName(tableName, "すばらしい"),
                partitionBy,
                isWal,
                10000,
                mainEngine,
                mainSqlExecutionContext
        );
        drainWalQueue();
        return tableToken;
    }

    private void executeInsertGeneratorStmt(TableToken tableToken) throws SqlException {
        executeInsertGeneratorStmt(tableToken, 6, mainEngine, mainSqlExecutionContext);
        drainWalQueue();
    }

    private void selectAll(TableToken tableToken, boolean backup, MutableUtf16Sink sink) throws Exception {
        CairoEngine engine = mainEngine;
        SqlCompiler compiler = mainCompiler;
        SqlExecutionContext context = mainSqlExecutionContext;
        try {
            if (backup) {
                engine = new CairoEngine(new DefaultTestCairoConfiguration(finalBackupPath.toString()));
                context = TestUtils.createSqlExecutionCtx(engine);
                compiler = engine.getSqlCompiler();
            }
            TestUtils.printSql(compiler, context, tableToken.getTableName(), sink);
        } finally {
            if (backup) {
                Misc.free(compiler);
                Misc.free(context);
                Misc.free(engine);
            }
        }
    }

    private void setFinalBackupPath() {
        setFinalBackupPath(0);
    }

    private void setFinalBackupPath(int n) {
        DateFormat timestampFormat = mainConfiguration.getBackupDirTimestampFormat();
        finalBackupPath.of(mainConfiguration.getBackupRoot()).slash();
        timestampFormat.format(mainConfiguration.getMicrosecondClock().getTicks(), mainConfiguration.getDefaultDateLocale(), null, finalBackupPath);
        if (n > 0) {
            finalBackupPath.put('.');
            finalBackupPath.put(n);
        }
        finalBackupPath.slash$();
        finalBackupPathLen = finalBackupPath.size();
        finalBackupPath.trimTo(finalBackupPathLen).concat(PropServerConfiguration.DB_DIRECTORY).slash$();
    }
}
