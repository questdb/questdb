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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.Misc;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.microtime.TimestampFormatCompiler;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.MutableCharSink;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

public class TableBackupTest {
    private static final StringSink sink1 = new StringSink();
    private static final StringSink sink2 = new StringSink();
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
            public DateFormat getBackupDirTimestampFormat() {
                return new TimestampFormatCompiler().compile("ddMMMyyyy");
            }
        };
        mainEngine = new CairoEngine(mainConfiguration);
        mainCompiler = new SqlCompiler(mainEngine);
        mainSqlExecutionContext = new SqlExecutionContextImpl(mainEngine, 1).with(AllowAllCairoSecurityContext.INSTANCE, new BindVariableServiceImpl(mainConfiguration), null, -1, null);
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
            assertTables(tableName);
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

            assertTables("tb1");
            assertTables("tb2");
        });
    }

    @Test
    public void testBackupDatabaseGeohashColumns() throws Exception {
        assertMemoryLeak(() -> {
            // @formatter:off
            mainCompiler.compile("create table tb1 as (select" +
                    " rnd_geohash(2) g1," +
                    " rnd_geohash(15) g2," +
                    " timestamp_sequence(0, 1000000000) ts" +
                    " from long_sequence(1)) timestamp(ts)", mainSqlExecutionContext);
            mainCompiler.compile("create table tb2 as (select" +
                    " rnd_geohash(31) g4," +
                    " rnd_geohash(42) g8," +
                    " timestamp_sequence(10000000000, 500000000) ts" +
                    " from long_sequence(1)) timestamp(ts)", mainSqlExecutionContext);
            // @formatter:on

            mainCompiler.compile("backup database", mainSqlExecutionContext);

            setFinalBackupPath();

            assertTables("tb1");
            assertTables("tb2");
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

            assertTables("tb1");
            assertTables("tb2");
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
                Assert.assertTrue(ex.getMessage().startsWith("[5] could not rename "));
            }

            mainCompiler.compile("backup table " + tableName + ";", mainSqlExecutionContext);
            setFinalBackupPath(1);
            assertTables(tableName);
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
            assertTables(tableName);
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
            StringSink sink3 = new StringSink();
            selectAll(tableName, false, sink1);
            selectAll(tableName, true, sink3);
            Assert.assertEquals(sink1, sink3);

            // @formatter:off
            mainCompiler.compile("insert into " + tableName +
                    " select * from (" +
                    " select rnd_symbol(4,4,4,2) sym, rnd_double(2) d, timestamp_sequence(10000000000, 500000000) ts from long_sequence(5)" +
                    ") timestamp(ts)", mainSqlExecutionContext);
            // @formatter:on

            mainCompiler.compile("backup table " + tableName, mainSqlExecutionContext);

            selectAll(tableName, false, sink1);
            setFinalBackupPath(1);
            selectAll(tableName, true, sink2);
            TestUtils.assertEquals(sink1, sink2);

            // Check previous backup is unaffected
            setFinalBackupPath();
            selectAll(tableName, true, sink1);
            TestUtils.assertEquals(sink3, sink1);
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
                path.of(mainConfiguration.getBackupRoot()).concat("tmp").concat(tableName).slash$();
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
                mainEngine.clear();
            }
        });
    }

    private void selectAll(String tableName, boolean backup, MutableCharSink sink) throws Exception {
        CairoEngine engine = null;
        SqlCompiler compiler = null;
        SqlExecutionContext sqlExecutionContext;
        try {
            if (backup) {
                final CairoConfiguration backupConfiguration = new DefaultCairoConfiguration(finalBackupPath.toString());
                engine = new CairoEngine(backupConfiguration);
                sqlExecutionContext = new SqlExecutionContextImpl(engine, 1).with(AllowAllCairoSecurityContext.INSTANCE,
                        new BindVariableServiceImpl(backupConfiguration),
                        null,
                        -1,
                        null);
                compiler = new SqlCompiler(engine);
            } else {
                engine = mainEngine;
                compiler = mainCompiler;
                sqlExecutionContext = mainSqlExecutionContext;
            }
            TestUtils.printSql(
                    compiler,
                    sqlExecutionContext,
                    "select * from " + tableName,
                    sink
            );
        } finally {
            if (backup) {
                Misc.free(engine);
                Misc.free(compiler);
            }
        }
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
    }

    private void setFinalBackupPath() {
        setFinalBackupPath(0);
    }

    private void assertTables(String tb1) throws Exception {
        selectAll(tb1, false, sink1);
        selectAll(tb1, true, sink2);
        TestUtils.assertEquals(sink1, sink2);
    }
}
