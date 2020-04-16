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

package io.questdb.griffin;

import io.questdb.MessageBus;
import io.questdb.MessageBusImpl;
import io.questdb.cairo.*;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.functions.bind.BindVariableService;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.Misc;
import io.questdb.std.microtime.DateFormatCompiler;
import io.questdb.std.microtime.TimestampFormat;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

public class TableBackupTest {
    // todo: rename test methods to fall inline with other test method names, e.g. testSomething
    private static final StringSink sink = new StringSink();
    private static final RecordCursorPrinter printer = new RecordCursorPrinter(sink);
    private static final int ERRNO_EIO = 5;
    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    private CharSequence backupRoot;
    private Path finalBackupPath;

    private CairoConfiguration mainConfiguration;
    private CairoEngine mainEngine;
    private SqlCompiler mainCompiler;
    private SqlExecutionContext mainSqlExecutionContext;
    private int renameErrno;
    private int mkdirsErrno;
    private int mkdirsErrnoCountDown = 0;

    @Before
    public void setup() throws IOException {
        finalBackupPath = new Path();
        CharSequence root = temp.newFolder("dbRoot").getAbsolutePath();
        backupRoot = temp.newFolder("dbBackupRoot").getAbsolutePath();
        mkdirsErrno = -1;
        renameErrno = -1;
        FilesFacade ff = new FilesFacadeImpl() {
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
            public int mkdirs(LPSZ path, int mode) {
                if (mkdirsErrno != -1 && --mkdirsErrnoCountDown < 1) {
                    nextErrno = mkdirsErrno;
                    mkdirsErrno = -1;
                    mkdirsErrnoCountDown = 0;
                    return -1;
                }
                return super.mkdirs(path, mode);
            }

            @Override
            public boolean rename(LPSZ from, LPSZ to) {
                if (renameErrno != -1) {
                    nextErrno = renameErrno;
                    renameErrno = -1;
                    return false;
                }
                return super.rename(from, to);
            }
        };
        mainConfiguration = new DefaultCairoConfiguration(root) {
            @Override
            public FilesFacade getFilesFacade() {
                return ff;
            }

            @Override
            public CharSequence getBackupRoot() {
                return backupRoot;
            }

            @Override
            public TimestampFormat getBackupDirTimestampFormat() {
                return new DateFormatCompiler().compile("ddMMMyyyy");
            }
        };
        MessageBus mainMessageBus = new MessageBusImpl();
        mainEngine = new CairoEngine(mainConfiguration, mainMessageBus);
        mainCompiler = new SqlCompiler(mainEngine);
        mainSqlExecutionContext = new SqlExecutionContextImpl().with(AllowAllCairoSecurityContext.INSTANCE, new BindVariableService(), mainMessageBus);
    }

    @After
    public void tearDown() {
        finalBackupPath.close();
    }

    @Test
    public void testAllTypesPartitionedTable() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "testTable2";
            // @formatter:off
            mainCompiler.compile("create table " + tableName + " as (" +
                    "select" +
                    " rnd_char() ch," +
                    " rnd_long256() ll," +
                    " rnd_int() a1," +
                    " rnd_int(0, 30, 2) a," +
                    " rnd_boolean() b," +
                    " rnd_str(3,3,2) c," +
                    " rnd_double(2) d," +
                    " rnd_float(2) e," +
                    " rnd_short(10,1024) f," +
                    " rnd_short() f1," +
                    " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                    " rnd_timestamp(to_timestamp('2015', 'yyyy'), to_timestamp('2016', 'yyyy'), 2) h," +
                    " rnd_symbol(4,4,4,2) i," +
                    " rnd_long(100,200,2) j," +
                    " rnd_long() j1," +
                    " timestamp_sequence(0, 1000000000) k," +
                    " rnd_byte(2,50) l," +
                    " rnd_bin(10, 20, 2) m" +
                    " from long_sequence(1000)" +
                    ")  timestamp(k) partition by DAY", mainSqlExecutionContext);
            // @formatter:on

            mainCompiler.compile("backup table " + tableName, mainSqlExecutionContext);
            setFinalBackupPath();
            String sourceSelectAll = selectAll(tableName, false);
            String backupSelectAll = selectAll(tableName, true);
            Assert.assertEquals(sourceSelectAll, backupSelectAll);
        });
    }

    @Test
    public void testBackupDatabase() throws Exception {
        assertMemoryLeak(() -> {
            // @formatter:off
            mainCompiler.compile("create table tb1 as (select" +
                    " rnd_symbol(4,4,4,2) sym," +
                    " rnd_double(2) d," +
                    " timestamp_sequence(0, 1000000000) ts" +
                    " from long_sequence(10000)) timestamp(ts)", mainSqlExecutionContext);
            mainCompiler.compile("create table tb2 as (select" +
                    " rnd_long256() ll," +
                    " timestamp_sequence(10000000000, 500000000) ts" +
                    " from long_sequence(100000)) timestamp(ts)", mainSqlExecutionContext);
            // @formatter:on

            mainCompiler.compile("backup database", mainSqlExecutionContext);

            setFinalBackupPath();

            String sourceSelectAll = selectAll("tb1", false);
            String backupSelectAll = selectAll("tb1", true);
            Assert.assertEquals(sourceSelectAll, backupSelectAll);

            sourceSelectAll = selectAll("tb2", false);
            backupSelectAll = selectAll("tb2", true);
            Assert.assertEquals(sourceSelectAll, backupSelectAll);
        });
    }

    @Test
    public void testCompromisedTableName() throws Exception {
        assertMemoryLeak(() -> {
            try {
                // @formatter:off
                mainCompiler.compile("create table tb1 as (select" +
                        " rnd_symbol(4,4,4,2) sym," +
                        " rnd_double(2) d," +
                        " timestamp_sequence(0, 1000000000) ts" +
                        " from long_sequence(10)) timestamp(ts)", mainSqlExecutionContext);
                // @formatter:on

                mainCompiler.compile("backup table ../tb1", mainSqlExecutionContext);
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
                String tableName = "testTable1";
                // @formatter:off
                mainCompiler.compile("create table " + tableName + " as (select" +
                        " rnd_symbol(4,4,4,2) sym," +
                        " rnd_double(2) d," +
                        " timestamp_sequence(0, 1000000000) ts" +
                        " from long_sequence(10000)) timestamp(ts)", mainSqlExecutionContext);
                // @formatter:on

                mainCompiler.compile("backup table " + tableName, mainSqlExecutionContext);
                Assert.fail();
            } catch (CairoException ex) {
                TestUtils.assertEquals("Backup is disabled, no backup root directory is configured in the server configuration ['cairo.sql.backup.root' property]", ex.getFlyweightMessage());
            }
        });
    }

    @Test
    public void testInvalidSql1() throws Exception {
        assertMemoryLeak(() -> {
            try {
                mainCompiler.compile("backup something", mainSqlExecutionContext);
                Assert.fail();
            } catch (SqlException ex) {
                Assert.assertEquals(7, ex.getPosition());
                TestUtils.assertEquals("expected 'table' or 'database'", ex.getFlyweightMessage());
            }
        });
    }

    @Test
    public void testInvalidSql2() throws Exception {
        assertMemoryLeak(() -> {
            try {
                mainCompiler.compile("backup table", mainSqlExecutionContext);
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
                // @formatter:off
                mainCompiler.compile("create table tb1 as (select" +
                        " rnd_symbol(4,4,4,2) sym," +
                        " rnd_double(2) d," +
                        " timestamp_sequence(0, 1000000000) ts" +
                        " from long_sequence(10000)) timestamp(ts)", mainSqlExecutionContext);
                // @formatter:on

                mainCompiler.compile("backup table tb1 tb2", mainSqlExecutionContext);
                Assert.fail();
            } catch (SqlException ex) {
                Assert.assertEquals(17, ex.getPosition());
                TestUtils.assertEquals("expected ','", ex.getFlyweightMessage());
            }
        });
    }

    @Test
    public void testMissingTable() throws Exception {
        assertMemoryLeak(() -> {
            try {
                // @formatter:off
                mainCompiler.compile("create table tb1 as (select" +
                        " rnd_symbol(4,4,4,2) sym," +
                        " rnd_double(2) d," +
                        " timestamp_sequence(0, 1000000000) ts" +
                        " from long_sequence(10000)) timestamp(ts)", mainSqlExecutionContext);
                // @formatter:on

                mainCompiler.compile("backup table tb1, tb2", mainSqlExecutionContext);
                Assert.fail();
            } catch (SqlException e) {
                Assert.assertEquals(18, e.getPosition());
                TestUtils.assertEquals("'tb2' is not  a valid table", e.getFlyweightMessage());
            }
        });
    }

    @Test
    public void testMultipleTable() throws Exception {
        assertMemoryLeak(() -> {
            // @formatter:off
            mainCompiler.compile("create table tb1 as (select" +
                    " rnd_symbol(4,4,4,2) sym," +
                    " rnd_double(2) d," +
                    " timestamp_sequence(0, 1000000000) ts" +
                    " from long_sequence(10000)) timestamp(ts)", mainSqlExecutionContext);
            mainCompiler.compile("create table tb2 as (select" +
                    " rnd_long256() ll," +
                    " timestamp_sequence(10000000000, 500000000) ts" +
                    " from long_sequence(100000)) timestamp(ts)", mainSqlExecutionContext);
            // @formatter:on

            mainCompiler.compile("backup table tb1, tb2", mainSqlExecutionContext);

            setFinalBackupPath();

            String sourceSelectAll = selectAll("tb1", false);
            String backupSelectAll = selectAll("tb1", true);
            Assert.assertEquals(sourceSelectAll, backupSelectAll);

            sourceSelectAll = selectAll("tb2", false);
            backupSelectAll = selectAll("tb2", true);
            Assert.assertEquals(sourceSelectAll, backupSelectAll);
        });
    }

    @Test
    public void testRenameFailure() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "testTable1";
            // @formatter:off
            mainCompiler.compile("create table " + tableName + " as (select" +
                    " rnd_symbol(4,4,4,2) sym," +
                    " rnd_double(2) d," +
                    " timestamp_sequence(0, 1000000000) ts" +
                    " from long_sequence(10000)) timestamp(ts)", mainSqlExecutionContext);
            // @formatter:on

            renameErrno = ERRNO_EIO;
            try {
                mainCompiler.compile("backup table " + tableName + ";", mainSqlExecutionContext);
                Assert.fail();
            } catch (CairoException ex) {
                Assert.assertTrue(ex.getMessage().startsWith("[5] Could not rename "));
            }

            mainCompiler.compile("backup table " + tableName + ";", mainSqlExecutionContext);
            setFinalBackupPath(1);
            String sourceSelectAll = selectAll(tableName, false);
            String backupSelectAll = selectAll(tableName, true);
            Assert.assertEquals(sourceSelectAll, backupSelectAll);
        });
    }

    @Test
    public void testSimpleTable1() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "testTable1";
            // @formatter:off
            mainCompiler.compile("create table " + tableName + " as (select" +
                    " rnd_symbol(4,4,4,2) sym," +
                    " rnd_double(2) d," +
                    " timestamp_sequence(0, 1000000000) ts" +
                    " from long_sequence(10000)) timestamp(ts)", mainSqlExecutionContext);
            // @formatter:on

            mainCompiler.compile("backup table " + tableName + ";", mainSqlExecutionContext);
            setFinalBackupPath();
            String sourceSelectAll = selectAll(tableName, false);
            String backupSelectAll = selectAll(tableName, true);
            Assert.assertEquals(sourceSelectAll, backupSelectAll);
        });
    }

    @Test
    public void testSuccessiveBackups() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "testTable1";
            // @formatter:off
            mainCompiler.compile("create table " + tableName + " as (select" +
                    " rnd_symbol(4,4,4,2) sym," +
                    " rnd_double(2) d," +
                    " timestamp_sequence(0, 1000000000) ts" +
                    " from long_sequence(10)) timestamp(ts)", mainSqlExecutionContext);
            // @formatter:on

            mainCompiler.compile("backup table " + tableName, mainSqlExecutionContext);
            setFinalBackupPath();
            String sourceSelectAll = selectAll(tableName, false);
            String backupSelectAll1 = selectAll(tableName, true);
            Assert.assertEquals(sourceSelectAll, backupSelectAll1);

            // @formatter:off
            mainCompiler.compile("insert into " + tableName +
                    " select * from (" +
                    " select rnd_symbol(4,4,4,2) sym, rnd_double(2) d, timestamp_sequence(10000000000, 500000000) ts from long_sequence(5)" +
                    ") timestamp(ts)", mainSqlExecutionContext);
            // @formatter:on

            mainCompiler.compile("backup table " + tableName, mainSqlExecutionContext);

            sourceSelectAll = selectAll(tableName, false);
            setFinalBackupPath(1);
            String backupSelectAll2 = selectAll(tableName, true);
            Assert.assertEquals(sourceSelectAll, backupSelectAll2);

            // Check previous backup is unaffected
            setFinalBackupPath();
            String backupSelectAllOriginal = selectAll(tableName, true);
            Assert.assertEquals(backupSelectAll1, backupSelectAllOriginal);
        });
    }

    @Test
    public void testTableBackupDirExists() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "testTable1";
            // @formatter:off
            mainCompiler.compile("create table " + tableName + " as (select" +
                    " rnd_symbol(4,4,4,2) sym," +
                    " rnd_double(2) d," +
                    " timestamp_sequence(0, 1000000000) ts" +
                    " from long_sequence(10000)) timestamp(ts)", mainSqlExecutionContext);
            // @formatter:on

            try (Path path = new Path()) {
                path.of(mainConfiguration.getBackupRoot()).concat("tmp").concat(tableName).put(Files.SEPARATOR).$();
                int rc = FilesFacadeImpl.INSTANCE.mkdirs(path, mainConfiguration.getBackupMkDirMode());
                Assert.assertEquals(0, rc);
            }
            try {
                mainCompiler.compile("backup table " + tableName + ";", mainSqlExecutionContext);
                Assert.fail();
            } catch (CairoException ex) {
                Assert.assertTrue(ex.getMessage().startsWith("[0] Backup dir for table \"testTable1\" already exists"));
            }
        });
    }

    @Test
    public void testTableBackupDirUnwritable() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "testTable1";
            // @formatter:off
            mainCompiler.compile("create table " + tableName + " as (select" +
                    " rnd_symbol(4,4,4,2) sym," +
                    " rnd_double(2) d," +
                    " timestamp_sequence(0, 1000000000) ts" +
                    " from long_sequence(10000)) timestamp(ts)", mainSqlExecutionContext);
            // @formatter:on

            try {
                mkdirsErrno = 13;
                mkdirsErrnoCountDown = 2;
                mainCompiler.compile("backup table " + tableName + ";", mainSqlExecutionContext);
                Assert.fail();
            } catch (CairoException ex) {
                Assert.assertTrue(ex.getMessage().startsWith("[13] Could not create "));
            }
        });
    }

    private void assertMemoryLeak(TestUtils.LeakProneCode code) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try {
                code.run();
                mainEngine.releaseInactive();
                Assert.assertEquals(0, mainEngine.getBusyWriterCount());
                Assert.assertEquals(0, mainEngine.getBusyReaderCount());
            } finally {
                mainEngine.releaseAllReaders();
                mainEngine.releaseAllWriters();
            }
        });
    }

    private String selectAll(SqlCompiler compiler, SqlExecutionContext sqlExecutionContext, String tableName) throws Exception {
        CompiledQuery compiledQuery = compiler.compile("select * from " + tableName, sqlExecutionContext);
        try (RecordCursorFactory factory = compiledQuery.getRecordCursorFactory(); RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            sink.clear();
            printer.print(cursor, factory.getMetadata(), true);
        }
        return sink.toString();
    }

    private String selectAll(String tableName, boolean backup) throws Exception {
        CairoEngine engine = null;
        SqlCompiler compiler = null;
        SqlExecutionContext sqlExecutionContext;
        try {
            if (backup) {
                CairoConfiguration backupConfiguration = new DefaultCairoConfiguration(finalBackupPath.toString());
                MessageBus backupMessageBus = new MessageBusImpl();
                sqlExecutionContext = new SqlExecutionContextImpl().with(AllowAllCairoSecurityContext.INSTANCE, new BindVariableService(), backupMessageBus);
                engine = new CairoEngine(backupConfiguration, backupMessageBus);
                compiler = new SqlCompiler(engine);
            } else {
                engine = mainEngine;
                compiler = mainCompiler;
                sqlExecutionContext = mainSqlExecutionContext;
            }
            return selectAll(compiler, sqlExecutionContext, tableName);
        } finally {
            if (backup) {
                Misc.free(engine);
                Misc.free(compiler);
            }
        }
    }

    private void setFinalBackupPath(int n) {
        TimestampFormat timestampFormat = mainConfiguration.getBackupDirTimestampFormat();
        finalBackupPath.of(mainConfiguration.getBackupRoot()).put(Files.SEPARATOR);
        timestampFormat.format(mainConfiguration.getMicrosecondClock().getTicks(), mainConfiguration.getDefaultTimestampLocale(), null, finalBackupPath);
        if (n > 0) {
            finalBackupPath.put('.');
            finalBackupPath.put(n);
        }
        finalBackupPath.put(Files.SEPARATOR).$();
    }

    private void setFinalBackupPath() {
        setFinalBackupPath(0);
    }
}
