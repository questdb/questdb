/*******************************************************************************
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

package io.questdb.test.recovery;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.SqlException;
import io.questdb.recovery.BoundedMetaReader;
import io.questdb.recovery.BoundedTxnReader;
import io.questdb.recovery.ConsoleRenderer;
import io.questdb.recovery.MetaStateService;
import io.questdb.recovery.RecoverySession;
import io.questdb.recovery.TableDiscoveryService;
import io.questdb.recovery.TxnStateService;
import io.questdb.std.FilesFacade;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;

public class RecoverySessionTest extends AbstractCairoTest {
    private static final FilesFacade FF = TestFilesFacadeImpl.INSTANCE;

    @Test
    public void testCdBareGoesToRoot() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithRows("nav_bare", 2);
            String[] result = runSession("cd nav_bare\ncd\npwd\nquit\n");
            Assert.assertTrue(result[0].contains("/\n"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testCdDeepFromPartition() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithRows("nav_deep", 2);
            String[] result = runSession("cd nav_deep\ncd 0\ncd foo\nquit\n");
            Assert.assertTrue(result[1].contains("already at leaf level"));
        });
    }

    @Test
    public void testCdDotDotAtRoot() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithRows("nav_dotdot_root", 1);
            String[] result = runSession("cd ..\npwd\nquit\n");
            Assert.assertTrue(result[0].contains("/\n"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testCdDotDotFromPartition() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithRows("nav_dotdot_part", 2);
            String[] result = runSession("cd nav_dotdot_part\ncd 0\ncd ..\nls\nquit\n");
            // after cd .. from partition, ls should show partition list
            Assert.assertTrue(result[0].contains("partition"));
            Assert.assertTrue(result[0].contains("rows"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testCdDotDotFromTable() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithRows("nav_dotdot_tbl", 2);
            String[] result = runSession("cd nav_dotdot_tbl\ncd ..\nls\nquit\n");
            // after cd .. from table, ls should show tables list
            Assert.assertTrue(result[0].contains("table_name"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testCdIntoPartitionAndLs() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithRows("nav_part_ls", 2);
            String[] result = runSession("cd nav_part_ls\ncd 0\nls\nquit\n");
            // column listing
            Assert.assertTrue(result[0].contains("sym"));
            Assert.assertTrue(result[0].contains("ts"));
            Assert.assertTrue(result[0].contains("SYMBOL"));
            Assert.assertTrue(result[0].contains("TIMESTAMP"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testCdIntoPartitionByName() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithRows("nav_part_name", 2);
            String[] result = runSession("cd nav_part_name\ncd 1970-01-01\nls\nquit\n");
            // should be inside the partition, ls shows columns
            Assert.assertTrue(result[0].contains("column_name"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testCdIntoPartitionByYearName() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table nav_year (val long, ts timestamp) timestamp(ts) partition by YEAR WAL");
            execute("insert into nav_year values (1, '2024-06-15T00:00:00.000000Z')");
            execute("insert into nav_year values (2, '2025-03-01T00:00:00.000000Z')");
            waitForAppliedRows("nav_year", 2);

            // "cd 2024" should match partition name "2024", not treat 2024 as a 0-based index
            String[] result = runSession("cd nav_year\ncd 2024\nls\nquit\n");
            Assert.assertTrue(
                    "should enter partition 2024 and show columns, got: " + result[0],
                    result[0].contains("column_name")
            );
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testCdIntoTableAndLs() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithRows("nav_tbl_ls", 4);
            String[] result = runSession("cd nav_tbl_ls\nls\nquit\n");
            Assert.assertTrue(result[0].contains("partition"));
            Assert.assertTrue(result[0].contains("1970-01-01"));
            Assert.assertTrue(result[0].contains("rows"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testCdIntoTableByDirName() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithRows("nav_tbl_dir", 2);
            TableToken token = engine.verifyTableName("nav_tbl_dir");
            String dirName = token.getDirName();
            String[] result = runSession("cd " + dirName + "\nls\nquit\n");
            Assert.assertTrue(result[0].contains("partition"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testCdIntoTableByIndex() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithRows("nav_tbl_idx", 2);
            // ls first to discover tables, then cd by index
            String[] result = runSession("ls\ncd 1\nls\nquit\n");
            // second ls should show partitions
            Assert.assertTrue(result[0].contains("partition"));
            Assert.assertTrue(result[0].contains("rows"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testCdNonexistentPartition() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithRows("nav_nopart", 2);
            String[] result = runSession("cd nav_nopart\ncd 999\nquit\n");
            Assert.assertTrue(result[1].contains("partition not found"));
        });
    }

    @Test
    public void testCdNonexistentTable() throws Exception {
        assertMemoryLeak(() -> {
            String[] result = runSession("cd nosuch\nquit\n");
            Assert.assertTrue(result[1].contains("table not found"));
        });
    }

    @Test
    public void testCdSlashFromDeep() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithRows("nav_slash_deep", 2);
            String[] result = runSession("cd nav_slash_deep\ncd 0\ncd /\npwd\nquit\n");
            String outText = result[0];
            // after cd /, the prompt should be at root, and pwd should output "/"
            // the prompt is "recover:/> " and pwd prints "/" on a separate line
            Assert.assertTrue(outText.contains("recover:/>"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testCdSlashGoesToRoot() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithRows("nav_slash", 2);
            String[] result = runSession("cd nav_slash\ncd /\npwd\nquit\n");
            Assert.assertTrue(result[0].contains("/\n"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testCdTableAutoDiscovers() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithRows("nav_auto", 2);
            // cd without prior ls or tables should auto-discover
            String[] result = runSession("cd nav_auto\nls\nquit\n");
            Assert.assertTrue(result[0].contains("partition"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testCdTableMissingMeta() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithRows("nav_nometa", 2);

            // delete _meta file
            try (Path path = new Path()) {
                TableToken token = engine.verifyTableName("nav_nometa");
                path.of(configuration.getDbRoot()).concat(token).concat(TableUtils.META_FILE_NAME);
                Assert.assertTrue(FF.removeQuiet(path.$()));
            }

            String[] result = runSession("cd nav_nometa\nls\nquit\n");
            // should still show partitions from _txn, but with raw timestamps
            String outText = result[0];
            Assert.assertTrue(outText.contains("partition") || outText.contains("rows"));
            // Bug #2: meta issues should be printed even though partition list format degrades
            Assert.assertTrue(
                    "meta issues should be printed when _meta is missing",
                    outText.contains("MISSING_FILE") || outText.contains("meta issues")
            );
        });
    }

    @Test
    public void testCdTableMissingMetaShowsIssuesInColumnListing() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithRows("nav_nometa_col", 2);

            // delete _meta file
            try (Path path = new Path()) {
                TableToken token = engine.verifyTableName("nav_nometa_col");
                path.of(configuration.getDbRoot()).concat(token).concat(TableUtils.META_FILE_NAME);
                Assert.assertTrue(FF.removeQuiet(path.$()));
            }

            // Bug #2: cd into partition, ls should show "No columns." but ALSO meta issues
            String[] result = runSession("cd nav_nometa_col\ncd 0\nls\nquit\n");
            String outText = result[0];
            Assert.assertTrue(outText.contains("No columns"));
            Assert.assertTrue(
                    "meta issues should be printed even when column list is empty",
                    outText.contains("MISSING_FILE") || outText.contains("meta issues")
            );
        });
    }

    @Test
    public void testCdTableMissingTxn() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithRows("nav_notxn", 2);

            // delete _txn file
            try (Path path = new Path()) {
                TableToken token = engine.verifyTableName("nav_notxn");
                path.of(configuration.getDbRoot()).concat(token).concat(TableUtils.TXN_FILE_NAME);
                Assert.assertTrue(FF.removeQuiet(path.$()));
            }

            String[] result = runSession("cd nav_notxn\nls\nquit\n");
            String outText = result[0];
            // Bug #2: should show "No partitions" AND txn issues (MISSING_FILE)
            Assert.assertTrue(outText.contains("No partitions"));
            Assert.assertTrue(
                    "txn issues should be printed even when partition list is empty",
                    outText.contains("MISSING_FILE") || outText.contains("txn issues")
            );
        });
    }

    @Test
    public void testCdTableCorruptMeta() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithRows("nav_corrupt_meta", 2);

            // truncate _meta to 10 bytes
            try (Path path = new Path()) {
                TableToken token = engine.verifyTableName("nav_corrupt_meta");
                path.of(configuration.getDbRoot()).concat(token).concat(TableUtils.META_FILE_NAME);
                long fd = FF.openRW(path.$(), CairoConfiguration.O_NONE);
                Assert.assertTrue(fd > -1);
                try {
                    Assert.assertTrue(FF.truncate(fd, 10));
                } finally {
                    FF.close(fd);
                }
            }

            String[] result = runSession("cd nav_corrupt_meta\nls\nquit\n");
            String outText = result[0];
            // should show partitions but with meta issues
            Assert.assertTrue(outText.contains("meta issues") || outText.contains("SHORT_FILE"));
        });
    }

    @Test
    public void testCdTableCorruptPartitionBy() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithRows("nav_corrupt_pb", 2);

            // write invalid partitionBy value (99) into _meta
            try (Path path = new Path()) {
                TableToken token = engine.verifyTableName("nav_corrupt_pb");
                path.of(configuration.getDbRoot()).concat(token).concat(TableUtils.META_FILE_NAME);
                long fd = FF.openRW(path.$(), CairoConfiguration.O_NONE);
                Assert.assertTrue(fd > -1);
                try {
                    // Bug #3: partitionBy at offset 4; write invalid value 99
                    long scratch = io.questdb.std.Unsafe.malloc(Integer.BYTES, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
                    try {
                        io.questdb.std.Unsafe.getUnsafe().putInt(scratch, 99);
                        FF.write(fd, scratch, Integer.BYTES, TableUtils.META_OFFSET_PARTITION_BY);
                    } finally {
                        io.questdb.std.Unsafe.free(scratch, Integer.BYTES, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
                    }
                } finally {
                    FF.close(fd);
                }
            }

            // ls should show partitions with raw timestamps, not throw
            String[] result = runSession("cd nav_corrupt_pb\nls\nquit\n");
            String outText = result[0];
            Assert.assertTrue(outText.contains("partition"));
            Assert.assertTrue(outText.contains("rows"));
            // must NOT have "command failed" from an exception
            Assert.assertFalse(
                    "corrupt partitionBy should degrade gracefully, not throw",
                    result[1].contains("command failed")
            );
        });
    }

    @Test
    public void testColumnsShowIndexedFlag() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithTypes("nav_indexed", 1);
            String[] result = runSession("cd nav_indexed\ncd 0\nls\nquit\n");
            String outText = result[0];
            // sym should show "yes" for indexed
            Assert.assertTrue(outText.contains("yes"));
            // other columns should show "no"
            Assert.assertTrue(outText.contains("no"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testColumnsShowTypes() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithTypes("nav_coltypes", 1);
            String[] result = runSession("cd nav_coltypes\ncd 0\nls\nquit\n");
            String outText = result[0];
            Assert.assertTrue(outText.contains("INT"));
            Assert.assertTrue(outText.contains("LONG"));
            Assert.assertTrue(outText.contains("DOUBLE"));
            Assert.assertTrue(outText.contains("STRING"));
            Assert.assertTrue(outText.contains("SYMBOL"));
            Assert.assertTrue(outText.contains("TIMESTAMP"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testEmptyLineInsideTable() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithRows("nav_empty_line", 2);
            String[] result = runSession("cd nav_empty_line\n\nls\nquit\n");
            // empty line should be skipped, ls should work
            Assert.assertTrue(result[0].contains("partition"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testLsAtRoot() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithRows("nav_ls_root", 2);
            String[] result = runSession("ls\nquit\n");
            Assert.assertTrue(result[0].contains("nav_ls_root"));
            Assert.assertTrue(result[0].contains("table_name"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testLsPartitionByDay() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithRows("nav_day", 4);
            String[] result = runSession("cd nav_day\nls\nquit\n");
            Assert.assertTrue(result[0].contains("1970-01-01"));
            Assert.assertTrue(result[0].contains("1970-01-02"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testLsPartitionByMonth() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table nav_month (val long, ts timestamp) timestamp(ts) partition by MONTH WAL");
            execute("insert into nav_month select x, timestamp_sequence('1970-01-01', 2592000000000L) from long_sequence(3)");
            waitForAppliedRows("nav_month", 3);

            String[] result = runSession("cd nav_month\nls\nquit\n");
            Assert.assertTrue(result[0].contains("1970-01"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testLsPartitionByNone() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table nav_none (val long, ts timestamp) timestamp(ts) partition by NONE");
            execute("insert into nav_none select x, timestamp_sequence('1970-01-01', 1000000L) from long_sequence(2)");

            String[] result = runSession("cd nav_none\nls\nquit\n");
            Assert.assertTrue(result[0].contains("default"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testLsPartitionByDayNanos() throws Exception {
        assertMemoryLeak(() -> {
            // Create a TIMESTAMP_NS table; partition names must still be correct dates
            execute("create table nav_nanos (ts TIMESTAMP_NS) timestamp(ts) partition by DAY WAL");
            execute("insert into nav_nanos values ('1970-01-01T00:00:00.000000000Z')");
            execute("insert into nav_nanos values ('1970-01-02T00:00:00.000000000Z')");
            waitForAppliedRows("nav_nanos", 2);

            String[] result = runSession("cd nav_nanos\nls\nquit\n");
            String outText = result[0];
            // Must show correct date-formatted partition names, not garbage from
            // interpreting nanosecond timestamps as microseconds
            Assert.assertTrue(
                    "partition name should be 1970-01-01 for nanos table, got: " + outText,
                    outText.contains("1970-01-01")
            );
            Assert.assertTrue(
                    "partition name should be 1970-01-02 for nanos table, got: " + outText,
                    outText.contains("1970-01-02")
            );
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testLsTableMultiplePartitions() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithRows("nav_multi", 10);
            String[] result = runSession("cd nav_multi\nls\nquit\n");
            // should have 10 partitions (1 per day)
            String outText = result[0];
            // Count lines with "1970-01-" which are partition entries
            int partCount = 0;
            for (String line : outText.split("\n")) {
                if (line.contains("1970-01-")) {
                    partCount++;
                }
            }
            Assert.assertEquals(10, partCount);
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testLsTableNoPartitions() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table nav_empty (val long, ts timestamp) timestamp(ts) partition by DAY WAL");
            // Don't insert any rows
            waitForEmptyTable("nav_empty");

            String[] result = runSession("cd nav_empty\nls\nquit\n");
            Assert.assertTrue(result[0].contains("No partitions"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testNonWalTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table nav_nonwal (val long, ts timestamp) timestamp(ts) partition by DAY");
            execute("insert into nav_nonwal select x, timestamp_sequence('1970-01-01', 86400000000L) from long_sequence(3)");

            String[] result = runSession("cd nav_nonwal\nls\nquit\n");
            Assert.assertTrue(result[0].contains("partition"));
            Assert.assertTrue(result[0].contains("1970-01-01"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testPromptContainsPath() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithRows("nav_prompt", 2);
            String[] result = runSession("cd nav_prompt\nquit\n");
            Assert.assertTrue(result[0].contains("recover:/nav_prompt>"));
        });
    }

    @Test
    public void testPwdAtEachLevel() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithRows("nav_pwd", 2);
            String[] result = runSession("pwd\ncd nav_pwd\npwd\ncd 0\npwd\nquit\n");
            String outText = result[0];
            Assert.assertTrue(outText.contains("/\n"));
            Assert.assertTrue(outText.contains("/nav_pwd\n"));
            Assert.assertTrue(outText.contains("/nav_pwd/1970-01-01\n"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testSessionTablesAndShowCommands() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithRows("session_tbl", 4);

            String[] result = runSession("tables\nshow session_tbl\nquit\n");
            String outText = result[0];
            Assert.assertTrue(outText.contains("session_tbl"));
            Assert.assertTrue(outText.contains("table: session_tbl"));
            Assert.assertTrue(outText.contains("partitions:"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testShowNoArgsInsideTable() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithRows("nav_show", 2);
            String[] result = runSession("cd nav_show\nshow\nquit\n");
            String outText = result[0];
            Assert.assertTrue(outText.contains("table: nav_show"));
            Assert.assertTrue(outText.contains("partitions:"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testShowWithArgInsideTable() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithRows("nav_show1", 2);
            createTableWithRows("nav_show2", 3);
            String[] result = runSession("cd nav_show1\nshow nav_show2\nquit\n");
            String outText = result[0];
            // should show nav_show2, not nav_show1
            Assert.assertTrue(outText.contains("table: nav_show2"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testUnknownCommandInsideTable() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithRows("nav_unknown", 2);
            String[] result = runSession("cd nav_unknown\nfoobar\nquit\n");
            Assert.assertTrue(result[1].contains("Unknown command"));
        });
    }

    private static void createTableWithRows(String tableName, int rowCount) throws SqlException {
        execute("create table " + tableName + " (sym symbol, ts timestamp) timestamp(ts) partition by DAY WAL");
        execute(
                "insert into "
                        + tableName
                        + " select rnd_symbol('AA', 'BB', 'CC'), timestamp_sequence('1970-01-01', "
                        + Micros.DAY_MICROS
                        + "L) from long_sequence("
                        + rowCount
                        + ")"
        );
        waitForAppliedRows(tableName, rowCount);
    }

    private static void createTableWithTypes(String tableName, int rowCount) throws SqlException {
        execute("create table " + tableName
                + " (i int, l long, d double, s string, sym symbol index, ts timestamp)"
                + " timestamp(ts) partition by DAY WAL");
        execute(
                "insert into " + tableName
                        + " select"
                        + " rnd_int(),"
                        + " rnd_long(),"
                        + " rnd_double(),"
                        + " rnd_str('a','b','c'),"
                        + " rnd_symbol('AA','BB','CC'),"
                        + " timestamp_sequence('1970-01-01', " + Micros.DAY_MICROS + "L)"
                        + " from long_sequence(" + rowCount + ")"
        );
        waitForAppliedRows(tableName, rowCount);
    }

    private static long getRowCount(String tableName) throws SqlException {
        try (RecordCursorFactory factory = select("select count() from " + tableName)) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Assert.assertTrue(cursor.hasNext());
                return cursor.getRecord().getLong(0);
            }
        }
    }

    private static String[] runSession(String commands) throws Exception {
        RecoverySession session = new RecoverySession(
                configuration.getDbRoot(),
                new MetaStateService(new BoundedMetaReader(FF)),
                new TableDiscoveryService(FF),
                new TxnStateService(new BoundedTxnReader(FF)),
                new ConsoleRenderer()
        );

        ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
        ByteArrayOutputStream errBytes = new ByteArrayOutputStream();
        try (
                PrintStream out = new PrintStream(outBytes, true, StandardCharsets.UTF_8);
                PrintStream err = new PrintStream(errBytes, true, StandardCharsets.UTF_8)
        ) {
            int code = session.run(
                    new BufferedReader(new StringReader(commands)),
                    out,
                    err
            );
            Assert.assertEquals(0, code);
        }

        return new String[]{
                outBytes.toString(StandardCharsets.UTF_8),
                errBytes.toString(StandardCharsets.UTF_8)
        };
    }

    private static void waitForAppliedRows(String tableName, int expectedRows) throws SqlException {
        for (int i = 0; i < 20; i++) {
            engine.releaseAllWriters();
            engine.releaseAllReaders();
            drainWalQueue(engine);
            if (getRowCount(tableName) == expectedRows) {
                return;
            }
        }
        Assert.assertEquals(expectedRows, getRowCount(tableName));
    }

    private static void waitForEmptyTable(String tableName) throws SqlException {
        for (int i = 0; i < 20; i++) {
            engine.releaseAllWriters();
            engine.releaseAllReaders();
            drainWalQueue(engine);
        }
    }
}
