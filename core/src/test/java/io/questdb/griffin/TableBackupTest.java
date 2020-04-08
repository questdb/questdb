package io.questdb.griffin;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import io.questdb.MessageBus;
import io.questdb.MessageBusImpl;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.RecordCursorPrinter;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.functions.bind.BindVariableService;
import io.questdb.std.Files;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;

public class TableBackupTest {
    private static final StringSink sink = new StringSink();
    private static final RecordCursorPrinter printer = new RecordCursorPrinter(sink);
    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    private CharSequence backupRoot;
    private Path finalBackupPath;

    private CairoConfiguration mainConfiguration;
    private CairoEngine mainEngine;
    private SqlCompiler mainCompiler;
    private SqlExecutionContext mainSqlExecutionContext;

    @Test
    public void simpleTableTest1() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "testTable1";
            // @formatter:off
            mainCompiler.compile("create table " + tableName + " as (select" +
                    " rnd_symbol(4,4,4,2) sym," +
                    " rnd_double(2) d," +
                    " timestamp_sequence(0, 1000000000) ts" +
                    " from long_sequence(10000)) timestamp(ts)", mainSqlExecutionContext);
            // @formatter:on

            mainCompiler.compile("backup table " + tableName, mainSqlExecutionContext);
            setFinalBackupPath();
            String sourceSelectAll = selectAll(tableName, false);
            String backupSelectAll = selectAll(tableName, true);
            Assert.assertEquals(sourceSelectAll, backupSelectAll);
        });
    }

    @Test
    public void allTypesPartitionedTableTest1() throws Exception {
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
    public void multipleTableTest() throws Exception {
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
    public void successiveBackupsTest() throws Exception {
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
    public void missingTableTest() throws Exception {
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
                Assert.assertTrue(false);
            } catch (SqlException ex) {
                Assert.assertEquals("io.questdb.griffin.SqlException: [18]  'tb2' is not  a valid table", ex.toString());
            }
        });
    }

    @Test
    public void incorrectConfigTest() throws Exception {
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
                Assert.assertTrue(false);
            } catch (CairoException ex) {
                Assert.assertEquals(
                        "io.questdb.cairo.CairoException: [0] Backup is disabled, no backup root directory is configured in the server configuration ['cairo.sql.backup.root' property]",
                        ex.toString());
            }
        });
    }

    @Test
    public void backupDatabaseTest() throws Exception {
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
    public void invalidSqlTest1() throws Exception {
        assertMemoryLeak(() -> {
            try {
                mainCompiler.compile("backup something", mainSqlExecutionContext);
            } catch (SqlException ex) {
                Assert.assertEquals("io.questdb.griffin.SqlException: [7]  expected 'table' or 'database'", ex.toString());
            }
        });
    }

    @Test
    public void invalidSqlTest2() throws Exception {
        assertMemoryLeak(() -> {
            try {
                mainCompiler.compile("backup table", mainSqlExecutionContext);
            } catch (SqlException ex) {
                Assert.assertEquals("io.questdb.griffin.SqlException: [7]  expected a table name", ex.toString());
            }
        });
    }

    @Test
    public void invalidSqlTest3() throws Exception {
        assertMemoryLeak(() -> {
            try {
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

                mainCompiler.compile("backup table tb1 tb2", mainSqlExecutionContext);
            } catch (SqlException ex) {
                Assert.assertEquals("io.questdb.griffin.SqlException: [17]  expected ','", ex.toString());
            }
        });
    }

    @Test
    public void compromisedTableNameTest() throws Exception {
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
                setFinalBackupPath();
                String sourceSelectAll = selectAll("tb1", false);
                String backupSelectAll = selectAll("tb1", true);
                Assert.assertEquals(sourceSelectAll, backupSelectAll);
            } catch (SqlException ex) {
                Assert.assertEquals("io.questdb.griffin.SqlException: [13]  expected a valid table name", ex.toString());
            }
        });
    }

    private void setFinalBackupPath() {
        setFinalBackupPath(0);
    }

    private void setFinalBackupPath(int n) {
        DateTimeFormatter formatter = mainConfiguration.getBackupDirDateTimeFormatter();
        String subDirNm = formatter.format(LocalDate.now());
        if (n > 0) {
            subDirNm += "." + n;
        }
        finalBackupPath.of(mainConfiguration.getBackupRoot()).put(Files.SEPARATOR).concat(subDirNm).put(Files.SEPARATOR).$();
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
            return selectAll(engine, compiler, sqlExecutionContext, tableName);
        } finally {
            if (backup) {
                engine.close();
                compiler.close();
            }
        }
    }

    private String selectAll(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext, String tableName) throws Exception {
        CompiledQuery compiledQuery = compiler.compile("select * from " + tableName, sqlExecutionContext);
        try (RecordCursorFactory factory = compiledQuery.getRecordCursorFactory(); RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            sink.clear();
            printer.print(cursor, factory.getMetadata(), true);
        }
        return sink.toString();
    }

    @Before
    public void setup() throws IOException {
        finalBackupPath = new Path();
        CharSequence root = temp.newFolder("dbRoot").getAbsolutePath();
        backupRoot = temp.newFolder("dbBackupRoot").getAbsolutePath();

        mainConfiguration = new DefaultCairoConfiguration(root) {
            @Override
            public CharSequence getBackupRoot() {
                return backupRoot;
            }

            @Override
            public DateTimeFormatter getBackupDirDateTimeFormatter() {
                return DateTimeFormatter.ofPattern("ddMMMyyy'-backup'");
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
}
