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
import io.questdb.cairo.mv.MatViewDefinition;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.view.ViewDefinition;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.SqlException;
import io.questdb.recovery.BoundedColumnVersionReader;
import io.questdb.recovery.BoundedMetaReader;
import io.questdb.recovery.BoundedRegistryReader;
import io.questdb.recovery.BoundedTxnReader;
import io.questdb.recovery.ColumnCheckService;
import io.questdb.recovery.ColumnValueReader;
import io.questdb.recovery.ColumnVersionStateService;
import io.questdb.recovery.ConsoleRenderer;
import io.questdb.recovery.MetaStateService;
import io.questdb.recovery.PartitionScanService;
import io.questdb.recovery.RecoverySession;
import io.questdb.recovery.RegistryStateService;
import io.questdb.recovery.TableDiscoveryService;
import io.questdb.recovery.TxnStateService;
import io.questdb.std.Chars;
import io.questdb.std.FilesFacade;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
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
            // "foo" is not a valid column name, so we get "column not found"
            Assert.assertTrue(result[1].contains("column not found"));
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
            Assert.assertTrue(result[0].contains("dir"));
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
    public void testCdIntoMatView() throws Exception {
        assertMemoryLeak(() -> {
            createMatViewWithBase("cd_mv", "cd_mv_base", 5);
            String[] result = runSession("tables\ncd cd_mv\npwd\nquit\n");
            Assert.assertTrue(result[0].contains("/cd_mv\n"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testCdIntoMatViewPartition() throws Exception {
        assertMemoryLeak(() -> {
            createMatViewWithBase("cd_mv_part", "cd_mv_part_base", 5);
            String[] result = runSession("tables\ncd cd_mv_part\nls\ncd 0\nls\nquit\n");
            // should show columns inside the partition
            Assert.assertTrue(result[0].contains("column_name"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testCdIntoView() throws Exception {
        assertMemoryLeak(() -> {
            createViewWithBase("cd_view", "cd_view_base");
            String[] result = runSession("tables\ncd cd_view\npwd\nquit\n");
            Assert.assertTrue(result[0].contains("/cd_view\n"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testCdIntoMissingPartition() throws Exception {
        assertMemoryLeak(() -> {
            createNonWalTableWithRows("nav_cd_missing", 3);
            deletePartitionDir("nav_cd_missing", "1970-01-02");
            String[] result = runSession("cd nav_cd_missing\ncd 2\npwd\nquit\n");
            // MISSING entry for deleted partition; cd by index should still work
            Assert.assertTrue(result[0].contains("1970-01-02"));
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
    public void testCdIntoOrphanPartition() throws Exception {
        assertMemoryLeak(() -> {
            createNonWalTableWithRows("nav_cd_orphan", 2);
            createOrphanDir("nav_cd_orphan", "old-backup");
            String[] result = runSession("cd nav_cd_orphan\ncd old-backup\npwd\nquit\n");
            Assert.assertTrue(result[0].contains("old-backup"));
        });
    }

    @Test
    public void testCdIntoPartitionByDirName() throws Exception {
        assertMemoryLeak(() -> {
            // Create table with O3 inserts to get nameTxn > -1 (dir names with ".N" suffix)
            execute("create table nav_part_dir (val long, ts timestamp) timestamp(ts) partition by DAY WAL");
            execute("insert into nav_part_dir values (1, '1970-01-02T00:00:00.000000Z')");
            waitForAppliedRows("nav_part_dir", 1);
            // O3 insert into the same partition triggers rename with nameTxn
            execute("insert into nav_part_dir values (2, '1970-01-02T12:00:00.000000Z')");
            waitForAppliedRows("nav_part_dir", 2);

            // ls to discover the dir name (may or may not have .N suffix)
            String[] result = runSession("cd nav_part_dir\nls\nquit\n");
            String outText = result[0];
            // extract the dir name from the listing
            String dirName = null;
            for (String line : outText.split("\n")) {
                if (line.contains("1970-01-02")) {
                    String[] parts = line.trim().split("\\s+");
                    if (parts.length >= 2) {
                        dirName = parts[1];
                    }
                    break;
                }
            }
            Assert.assertNotNull("should find dir name in output", dirName);

            // cd by that exact dir name
            String[] result2 = runSession("cd nav_part_dir\ncd " + dirName + "\nls\nquit\n");
            Assert.assertTrue(result2[0].contains("column_name"));
            Assert.assertEquals("", result2[1]);
        });
    }

    @Test
    public void testCdIntoTableAndLs() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithRows("nav_tbl_ls", 4);
            String[] result = runSession("cd nav_tbl_ls\nls\nquit\n");
            Assert.assertTrue(result[0].contains("dir"));
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
            Assert.assertTrue(result[0].contains("dir"));
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
            Assert.assertTrue(result[0].contains("dir"));
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
            Assert.assertTrue(result[0].contains("dir"));
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
            Assert.assertTrue(outText.contains("dir") || outText.contains("rows"));
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
            // with _txn deleted, partition dirs on disk become ORPHAN
            Assert.assertTrue(outText.contains("ORPHAN"));
            Assert.assertTrue(
                    "txn issues should be printed when _txn is missing",
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
            Assert.assertTrue(outText.contains("dir"));
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
            Assert.assertTrue(result[0].contains("dir"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testDroppedMatViewNotInRegistry() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table dmv_base (sym symbol, price double, ts timestamp) timestamp(ts) partition by DAY WAL");
            drainWalQueue(engine);
            execute("create materialized view dmv_target as (select sym, last(price) as price, ts from dmv_base sample by 1h) partition by DAY");
            drainWalAndMatViewQueues();
            execute("drop materialized view dmv_target");
            drainWalAndMatViewQueues();

            String[] result = runSession("tables\nquit\n");
            String outText = result[0];
            // the dir may still exist on disk after drop, but registry should not have it
            for (String line : outText.split("\n")) {
                if (line.contains("dmv_target")) {
                    Assert.assertTrue("dropped matview should show NOT_IN_REG", line.contains("NOT_IN_REG"));
                    break;
                }
            }
        });
    }

    @Test
    public void testDroppedViewNotInRegistry() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table dv_base (sym symbol, price double, ts timestamp) timestamp(ts) partition by DAY WAL");
            drainWalQueue(engine);
            execute("create view dv_target as (select sym, last(price) as price, ts from dv_base sample by 1h)");
            execute("drop view dv_target");
            drainWalQueue(engine);

            String[] result = runSession("tables\nquit\n");
            String outText = result[0];
            // the dir may still exist on disk after drop, but registry should not have it
            for (String line : outText.split("\n")) {
                if (line.contains("dv_target")) {
                    Assert.assertTrue("dropped view should show NOT_IN_REG", line.contains("NOT_IN_REG"));
                    break;
                }
            }
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
    public void testLsFiltersInternalDirs() throws Exception {
        assertMemoryLeak(() -> {
            // WAL tables have wal*, txn_seq, seq dirs
            execute("create table nav_filter (val long, ts timestamp) timestamp(ts) partition by DAY WAL");
            execute("insert into nav_filter values (1, '1970-01-01T00:00:00.000000Z')");
            waitForAppliedRows("nav_filter", 1);

            String[] result = runSession("cd nav_filter\nls\nquit\n");
            String outText = result[0];
            // internal dirs should NOT appear in the listing
            for (String line : outText.split("\n")) {
                // skip header and non-data lines
                if (line.trim().startsWith("idx") || line.trim().isEmpty() || line.contains("recover:") || line.contains("issues")) {
                    continue;
                }
                Assert.assertFalse("wal dir should be filtered: " + line, line.contains("wal"));
                Assert.assertFalse("txn_seq dir should be filtered: " + line, line.contains("txn_seq"));
                Assert.assertFalse("seq dir should be filtered: " + line, line.contains(" seq "));
            }
        });
    }

    @Test
    public void testLsFiltersUnderscoreAndDotDirs() throws Exception {
        assertMemoryLeak(() -> {
            createNonWalTableWithRows("nav_filter_ud", 1);
            createOrphanDir("nav_filter_ud", "_fake");
            createOrphanDir("nav_filter_ud", ".hidden");

            String[] result = runSession("cd nav_filter_ud\nls\nquit\n");
            String outText = result[0];
            Assert.assertFalse("_fake dir should be filtered", outText.contains("_fake"));
            Assert.assertFalse(".hidden dir should be filtered", outText.contains(".hidden"));
        });
    }

    @Test
    public void testLsInMatView() throws Exception {
        assertMemoryLeak(() -> {
            createMatViewWithBase("ls_mv", "ls_mv_base", 5);
            String[] result = runSession("tables\ncd ls_mv\nls\nquit\n");
            // matview has partitions like a WAL table
            Assert.assertTrue(result[0].contains("dir"));
            Assert.assertTrue(result[0].contains("rows"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testLsInView() throws Exception {
        assertMemoryLeak(() -> {
            createViewWithBase("ls_view", "ls_view_base");
            String[] result = runSession("tables\ncd ls_view\nls\nquit\n");
            // views have no partitions
            Assert.assertTrue(result[0].contains("No partitions"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testLsNonWalTableOrphanDir() throws Exception {
        assertMemoryLeak(() -> {
            createNonWalTableWithRows("nav_nonwal_orphan", 2);
            createOrphanDir("nav_nonwal_orphan", "stale-backup");

            String[] result = runSession("cd nav_nonwal_orphan\nls\nquit\n");
            String outText = result[0];
            Assert.assertTrue(outText.contains("ORPHAN"));
            Assert.assertTrue(outText.contains("stale-backup"));
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
    public void testLsPartitionScanOrderMatchedFirst() throws Exception {
        assertMemoryLeak(() -> {
            createNonWalTableWithRows("nav_order", 3);
            createOrphanDir("nav_order", "orphan-dir");

            String[] result = runSession("cd nav_order\nls\nquit\n");
            String outText = result[0];
            // matched partitions should appear before the orphan
            int firstMatchedPos = outText.indexOf("1970-01-01");
            int orphanPos = outText.indexOf("orphan-dir");
            Assert.assertTrue("matched partitions before orphan", firstMatchedPos < orphanPos);
        });
    }

    @Test
    public void testLsShowsBothOrphanAndMissing() throws Exception {
        assertMemoryLeak(() -> {
            createNonWalTableWithRows("nav_both", 2);
            deletePartitionDir("nav_both", "1970-01-02");
            createOrphanDir("nav_both", "stale-dir");

            String[] result = runSession("cd nav_both\nls\nquit\n");
            String outText = result[0];
            Assert.assertTrue("ORPHAN should be present", outText.contains("ORPHAN"));
            Assert.assertTrue("MISSING should be present", outText.contains("MISSING"));
        });
    }

    @Test
    public void testLsShowsDirColumn() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithRows("nav_dircol", 2);
            String[] result = runSession("cd nav_dircol\nls\nquit\n");
            String outText = result[0];
            // should show raw dir names
            Assert.assertTrue(outText.contains("1970-01-01"));
            Assert.assertTrue(outText.contains("dir"));
        });
    }

    @Test
    public void testLsShowsMissingPartition() throws Exception {
        assertMemoryLeak(() -> {
            createNonWalTableWithRows("nav_missing", 3);
            deletePartitionDir("nav_missing", "1970-01-02");

            String[] result = runSession("cd nav_missing\nls\nquit\n");
            String outText = result[0];
            Assert.assertTrue("MISSING should appear for deleted partition", outText.contains("MISSING"));
            // other partitions should not have MISSING/ORPHAN status
            // count MISSING occurrences — should be exactly 1
            int count = 0;
            for (String line : outText.split("\n")) {
                if (line.contains("MISSING")) {
                    count++;
                }
            }
            Assert.assertEquals("exactly one MISSING partition", 1, count);
        });
    }

    @Test
    public void testLsShowsOrphanDirectory() throws Exception {
        assertMemoryLeak(() -> {
            createNonWalTableWithRows("nav_orphan", 2);
            createOrphanDir("nav_orphan", "old-backup");

            String[] result = runSession("cd nav_orphan\nls\nquit\n");
            String outText = result[0];
            Assert.assertTrue(outText.contains("old-backup"));
            Assert.assertTrue(outText.contains("ORPHAN"));
        });
    }

    @Test
    public void testLsWithCorruptTxnShowsAllAsOrphan() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithRows("nav_corrupt_txn", 2);

            // delete _txn file
            try (Path path = new Path()) {
                TableToken token = engine.verifyTableName("nav_corrupt_txn");
                path.of(configuration.getDbRoot()).concat(token).concat(TableUtils.TXN_FILE_NAME);
                Assert.assertTrue(FF.removeQuiet(path.$()));
            }

            String[] result = runSession("cd nav_corrupt_txn\nls\nquit\n");
            String outText = result[0];
            // all partition dirs should show as ORPHAN
            Assert.assertTrue(outText.contains("ORPHAN"));
            Assert.assertTrue(
                    "txn issues should be printed",
                    outText.contains("MISSING_FILE") || outText.contains("txn issues")
            );
        });
    }

    @Test
    public void testMatViewOrphanDir() throws Exception {
        assertMemoryLeak(() -> {
            // create orphan dir with _mv file to simulate an unregistered matview
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat("orphan_mv").slash$();
                Assert.assertEquals(0, FF.mkdirs(path, configuration.getMkDirMode()));
                // create _mv marker file
                path.trimTo(path.size() - 1).concat(MatViewDefinition.MAT_VIEW_DEFINITION_FILE_NAME).$();
                long fd = FF.openRW(path.$(), CairoConfiguration.O_NONE);
                Assert.assertTrue(fd > -1);
                FF.close(fd);
            }

            String[] result = runSession("tables\nquit\n");
            String outText = result[0];
            Assert.assertTrue("orphan matview dir should show matview type", outText.contains("matview"));
            Assert.assertTrue("orphan matview dir should show NOT_IN_REG", outText.contains("NOT_IN_REG"));
        });
    }

    @Test
    public void testMatViewRegistryBlank() throws Exception {
        assertMemoryLeak(() -> {
            createMatViewWithBase("reg_mv", "reg_mv_base", 5);
            String[] result = runSession("tables\nquit\n");
            String outText = result[0];
            for (String line : outText.split("\n")) {
                if (line.contains("reg_mv") && !line.contains("reg_mv_base")) {
                    Assert.assertFalse("registry should be blank for matched matview", line.contains("MISMATCH"));
                    Assert.assertFalse("registry should be blank for matched matview", line.contains("NOT_IN_REG"));
                    break;
                }
            }
        });
    }

    @Test
    public void testNonWalTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table nav_nonwal (val long, ts timestamp) timestamp(ts) partition by DAY");
            execute("insert into nav_nonwal select x, timestamp_sequence('1970-01-01', 86400000000L) from long_sequence(3)");

            String[] result = runSession("cd nav_nonwal\nls\nquit\n");
            Assert.assertTrue(result[0].contains("dir"));
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
    public void testShowMatView() throws Exception {
        assertMemoryLeak(() -> {
            createMatViewWithBase("show_mv", "show_mv_base", 5);
            String[] result = runSession("tables\nshow show_mv\nquit\n");
            String outText = result[0];
            Assert.assertTrue(outText.contains("table: show_mv"));
            Assert.assertTrue(outText.contains("partitions:"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testShowView() throws Exception {
        assertMemoryLeak(() -> {
            createViewWithBase("show_view", "show_view_base");
            String[] result = runSession("tables\nshow show_view\nquit\n");
            String outText = result[0];
            Assert.assertTrue(outText.contains("table: show_view"));
            // views are created through the same path as tables and have _txn files
            Assert.assertTrue(outText.contains("_txn path:"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testShowViewWhileCdInto() throws Exception {
        assertMemoryLeak(() -> {
            createViewWithBase("show_v_cd", "show_v_cd_base");
            String[] result = runSession("tables\ncd show_v_cd\nshow\nquit\n");
            String outText = result[0];
            Assert.assertTrue(outText.contains("table: show_v_cd"));
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

    @Test
    public void testTablesCorruptRegistryShowsIssues() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithRows("reg_corrupt", 1);

            // delete all tables.d.* files
            deleteAllRegistryFiles();

            String[] result = runSession("tables\nquit\n");
            String outText = result[0];
            Assert.assertTrue(
                    "should show registry issue about missing file",
                    outText.contains("MISSING_FILE") || outText.contains("registry issues")
            );
        });
    }

    @Test
    public void testTablesNotInRegistry() throws Exception {
        assertMemoryLeak(() -> {
            // create an orphan dir with _txn file but no registry entry
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat("orphan_dir").slash$();
                Assert.assertEquals(0, FF.mkdirs(path, configuration.getMkDirMode()));
            }

            String[] result = runSession("tables\nquit\n");
            String outText = result[0];
            Assert.assertTrue(
                    "orphan dir should show NOT_IN_REG",
                    outText.contains("NOT_IN_REG")
            );
        });
    }

    @Test
    public void testTablesMatViewDiscoveryState() throws Exception {
        assertMemoryLeak(() -> {
            createMatViewWithBase("state_mv", "state_mv_base", 5);
            String[] result = runSession("tables\nquit\n");
            String outText = result[0];
            // matview has _txn file, should show HAS_TXN state
            for (String line : outText.split("\n")) {
                if (line.contains("state_mv") && !line.contains("state_mv_base")) {
                    Assert.assertTrue("matview should show HAS_TXN state", line.contains("HAS_TXN"));
                    break;
                }
            }
        });
    }

    @Test
    public void testTablesShowsMatViewType() throws Exception {
        assertMemoryLeak(() -> {
            createMatViewWithBase("type_mv", "type_mv_base", 5);
            String[] result = runSession("tables\nquit\n");
            String outText = result[0];
            boolean found = false;
            for (String line : outText.split("\n")) {
                if (line.contains("type_mv") && !line.contains("type_mv_base")) {
                    Assert.assertTrue("matview should show 'matview' type", line.contains("matview"));
                    found = true;
                    break;
                }
            }
            Assert.assertTrue("should find type_mv in output", found);
        });
    }

    @Test
    public void testTablesShowsMixedTypes() throws Exception {
        assertMemoryLeak(() -> {
            createNonWalTableWithRows("mix_nonwal", 1);
            createTableWithRows("mix_wal", 1);
            createViewWithBase("mix_view", "mix_view_base");
            createMatViewWithBase("mix_mv", "mix_mv_base", 1);

            String[] result = runSession("tables\nquit\n");
            String outText = result[0];
            // check type column for each table type
            boolean foundNonwal = false, foundWal = false, foundView = false, foundMv = false;
            for (String line : outText.split("\n")) {
                if (line.contains("mix_nonwal")) {
                    Assert.assertTrue("non-WAL table should show 'table' type", line.contains("table"));
                    foundNonwal = true;
                } else if (line.contains("mix_wal") && !line.contains("mix_view") && !line.contains("mix_mv")) {
                    Assert.assertTrue("WAL table should show 'table' type", line.contains("table"));
                    foundWal = true;
                } else if (line.contains("mix_view") && !line.contains("mix_view_base")) {
                    Assert.assertTrue("view should show 'view' type", line.contains("view"));
                    foundView = true;
                } else if (line.contains("mix_mv") && !line.contains("mix_mv_base")) {
                    Assert.assertTrue("matview should show 'matview' type", line.contains("matview"));
                    foundMv = true;
                }
            }
            Assert.assertTrue("should find non-WAL table", foundNonwal);
            Assert.assertTrue("should find WAL table", foundWal);
            Assert.assertTrue("should find view", foundView);
            Assert.assertTrue("should find matview", foundMv);
        });
    }

    @Test
    public void testTablesShowsViewType() throws Exception {
        assertMemoryLeak(() -> {
            createViewWithBase("type_view", "type_view_base");
            String[] result = runSession("tables\nquit\n");
            String outText = result[0];
            boolean found = false;
            for (String line : outText.split("\n")) {
                if (line.contains("type_view") && !line.contains("type_view_base")) {
                    Assert.assertTrue("view should show 'view' type", line.contains("view"));
                    found = true;
                    break;
                }
            }
            Assert.assertTrue("should find type_view in output", found);
        });
    }

    @Test
    public void testTablesViewDiscoveryState() throws Exception {
        assertMemoryLeak(() -> {
            createViewWithBase("state_view", "state_view_base");
            String[] result = runSession("tables\nquit\n");
            String outText = result[0];
            // views are created through the same path as tables and have _txn files
            for (String line : outText.split("\n")) {
                if (line.contains("state_view") && !line.contains("state_view_base")) {
                    Assert.assertTrue("view should show HAS_TXN state", line.contains("HAS_TXN"));
                    break;
                }
            }
        });
    }

    @Test
    public void testTablesRegistryDirMissing() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithRows("reg_dir_missing", 1);

            // delete the table directory
            TableToken token = engine.verifyTableName("reg_dir_missing");
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(token.getDirName()).$();
                Assert.assertTrue(FF.rmdir(path));
            }

            String[] result = runSession("tables\nquit\n");
            String outText = result[0];
            Assert.assertTrue(
                    "should show DIR_MISSING entry for table with deleted dir",
                    outText.contains("missing directories") || outText.contains("reg_dir_missing")
            );
        });
    }

    @Test
    public void testTablesRegistryMatchBlank() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithRows("reg_match", 1);

            String[] result = runSession("tables\nquit\n");
            String outText = result[0];
            // find the line with reg_match
            boolean found = false;
            for (String line : outText.split("\n")) {
                if (line.contains("reg_match")) {
                    // registry column should be blank (not MISMATCH, not NOT_IN_REG)
                    Assert.assertFalse("registry should be blank for matching table", line.contains("MISMATCH"));
                    Assert.assertFalse("registry should be blank for matching table", line.contains("NOT_IN_REG"));
                    found = true;
                    break;
                }
            }
            Assert.assertTrue("should find reg_match in output", found);
        });
    }

    @Test
    public void testTablesShowsRegistryColumn() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithRows("reg_col", 1);

            String[] result = runSession("tables\nquit\n");
            String outText = result[0];
            Assert.assertTrue(
                    "output should contain registry column header",
                    outText.contains("registry")
            );
        });
    }

    @Test
    public void testViewOrphanDir() throws Exception {
        assertMemoryLeak(() -> {
            // create orphan dir with _view file to simulate an unregistered view
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat("orphan_view").slash$();
                Assert.assertEquals(0, FF.mkdirs(path, configuration.getMkDirMode()));
                // create _view marker file
                path.trimTo(path.size() - 1).concat(ViewDefinition.VIEW_DEFINITION_FILE_NAME).$();
                long fd = FF.openRW(path.$(), CairoConfiguration.O_NONE);
                Assert.assertTrue(fd > -1);
                FF.close(fd);
            }

            String[] result = runSession("tables\nquit\n");
            String outText = result[0];
            Assert.assertTrue("orphan view dir should show view type", outText.contains("view"));
            Assert.assertTrue("orphan view dir should show NOT_IN_REG", outText.contains("NOT_IN_REG"));
        });
    }

    @Test
    public void testViewRegistryBlank() throws Exception {
        assertMemoryLeak(() -> {
            createViewWithBase("reg_view", "reg_view_base");
            String[] result = runSession("tables\nquit\n");
            String outText = result[0];
            for (String line : outText.split("\n")) {
                if (line.contains("reg_view") && !line.contains("reg_view_base")) {
                    Assert.assertFalse("registry should be blank for matched view", line.contains("MISMATCH"));
                    Assert.assertFalse("registry should be blank for matched view", line.contains("NOT_IN_REG"));
                    break;
                }
            }
        });
    }

    @Test
    public void testCheckColumnsAtPartitionLevel() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithRows("chk_part_level", 3);
            String[] result = runSession("cd chk_part_level\ncd 0\ncheck columns\nquit\n");
            String outText = result[0];
            Assert.assertTrue(outText.contains("checking chk_part_level"));
            Assert.assertTrue(outText.contains("OK"));
            Assert.assertTrue(outText.contains("check complete"));
            Assert.assertTrue(outText.contains("1 partitions checked"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testCheckColumnsAtRootLevel() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithRows("chk_root_a", 2);
            createTableWithRows("chk_root_b", 1);
            String[] result = runSession("check columns\nquit\n");
            String outText = result[0];
            Assert.assertTrue(outText.contains("chk_root_a"));
            Assert.assertTrue(outText.contains("chk_root_b"));
            Assert.assertTrue(outText.contains("check complete"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testCheckColumnsAtTableLevel() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithRows("chk_tbl_level", 3);
            String[] result = runSession("cd chk_tbl_level\ncheck columns\nquit\n");
            String outText = result[0];
            Assert.assertTrue(outText.contains("checking chk_tbl_level"));
            Assert.assertTrue(outText.contains("OK"));
            Assert.assertTrue(outText.contains("check complete"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testCheckColumnsCorruptDFile() throws Exception {
        assertMemoryLeak(() -> {
            createNonWalTableWithRows("chk_corrupt_d", 2);

            // truncate the sym.d file in the first partition
            try (Path path = new Path()) {
                TableToken token = engine.verifyTableName("chk_corrupt_d");
                path.of(configuration.getDbRoot()).concat(token).concat("1970-01-01").concat("sym.d");
                long fd = FF.openRW(path.$(), CairoConfiguration.O_NONE);
                Assert.assertTrue(fd > -1);
                try {
                    Assert.assertTrue(FF.truncate(fd, 1));
                } finally {
                    FF.close(fd);
                }
            }

            String[] result = runSession("cd chk_corrupt_d\ncheck columns\nquit\n");
            String outText = result[0];
            Assert.assertTrue("should detect corrupt .d file", outText.contains("ERROR"));
            Assert.assertTrue("should show file too short", outText.contains("too short"));
        });
    }

    @Test
    public void testCheckColumnsCorruptLastPartitionWithOrphanDir() throws Exception {
        assertMemoryLeak(() -> {
            // Create a non-WAL table with 2 rows in 2 partitions (1970-01-01, 1970-01-02).
            // The last partition (1970-01-02) is the active/transient partition whose row count
            // is stored as transientRowCount in the _txn header, not in the partition entry
            // (which stores 0).
            createNonWalTableWithRows("chk_orphan_rc", 2);

            // Add an orphan dir so that partitionScan has 3 entries:
            //   [1970-01-01(MATCHED), 1970-01-02(MATCHED), zzz-orphan(ORPHAN)]
            // resolvePartitionRowCount must use the _txn index (not the scan list index)
            // to correctly identify the last _txn partition and return transientRowCount.
            createOrphanDir("chk_orphan_rc", "zzz-orphan");

            // Corrupt sym.d in the last partition so the check detects an error —
            // but only if the row count is resolved correctly (1, not 0).
            truncateColumnFileTo("chk_orphan_rc", "1970-01-02", "sym.d", 1);

            String[] result = runSession("cd chk_orphan_rc\ncheck columns\nquit\n");
            String outText = result[0];
            Assert.assertTrue(
                    "last partition corruption should be detected even with orphan dir present",
                    outText.contains("ERROR")
            );
            Assert.assertFalse(
                    "last partition should not have rows=0 (transientRowCount must be used)",
                    outText.contains("(rows=0)")
            );
        });
    }

    @Test
    public void testCheckColumnsDroppedColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table chk_drop (val int, sym symbol, ts timestamp) timestamp(ts) partition by DAY WAL");
            execute("insert into chk_drop values (1, 'A', '1970-01-01T00:00:00.000000Z')");
            waitForAppliedRows("chk_drop", 1);
            execute("alter table chk_drop drop column val");
            drainWalQueue(engine);

            String[] result = runSession("cd chk_drop\ncheck columns\nquit\n");
            String outText = result[0];
            Assert.assertTrue("should show SKIPPED for dropped column", outText.contains("SKIPPED"));
            Assert.assertTrue("should mention dropped column", outText.contains("dropped column"));
        });
    }

    @Test
    public void testCheckColumnsEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table chk_empty (val long, ts timestamp) timestamp(ts) partition by DAY WAL");
            waitForEmptyTable("chk_empty");

            String[] result = runSession("cd chk_empty\ncheck columns\nquit\n");
            String outText = result[0];
            Assert.assertTrue(outText.contains("check complete"));
            Assert.assertTrue(outText.contains("0 errors"));
        });
    }

    @Test
    public void testCheckColumnsFixedSizeOk() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithTypes("chk_fixed", 2);
            String[] result = runSession("cd chk_fixed\ncheck columns\nquit\n");
            String outText = result[0];
            Assert.assertTrue(outText.contains("OK"));
            Assert.assertTrue(outText.contains("0 errors"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testCheckColumnsMissingPartition() throws Exception {
        assertMemoryLeak(() -> {
            createNonWalTableWithRows("chk_miss_part", 3);
            deletePartitionDir("chk_miss_part", "1970-01-02");

            String[] result = runSession("cd chk_miss_part\ncheck columns\nquit\n");
            String outText = result[0];
            Assert.assertTrue("should skip MISSING partition", outText.contains("skipping"));
            Assert.assertTrue(outText.contains("MISSING"));
        });
    }

    @Test
    public void testCheckColumnsNoCvFile() throws Exception {
        assertMemoryLeak(() -> {
            createNonWalTableWithRows("chk_no_cv", 2);

            // delete the _cv file
            try (Path path = new Path()) {
                TableToken token = engine.verifyTableName("chk_no_cv");
                path.of(configuration.getDbRoot()).concat(token).concat("_cv");
                FF.removeQuiet(path.$());
            }

            String[] result = runSession("cd chk_no_cv\ncheck columns\nquit\n");
            String outText = result[0];
            // should gracefully degrade - assume colTop=0
            Assert.assertTrue(outText.contains("check complete"));
            Assert.assertTrue(outText.contains("OK"));
        });
    }

    @Test
    public void testCheckColumnsPartitionByNone() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table chk_none (val long, ts timestamp) timestamp(ts) partition by NONE");
            execute("insert into chk_none select x, timestamp_sequence('1970-01-01', 1000000L) from long_sequence(5)");

            String[] result = runSession("cd chk_none\ncheck columns\nquit\n");
            String outText = result[0];
            Assert.assertTrue(outText.contains("check complete"));
            Assert.assertTrue(outText.contains("OK"));
        });
    }

    @Test
    public void testCheckColumnsVarSizeOk() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table chk_var (s string, v varchar, ts timestamp) timestamp(ts) partition by DAY WAL");
            execute(
                    "insert into chk_var select rnd_str('a','b','c'), rnd_varchar('x','y','z'),"
                            + " timestamp_sequence('1970-01-01', " + Micros.DAY_MICROS + "L) from long_sequence(2)"
            );
            waitForAppliedRows("chk_var", 2);

            String[] result = runSession("cd chk_var\ncheck columns\nquit\n");
            String outText = result[0];
            Assert.assertTrue(outText.contains("OK"));
            Assert.assertTrue(outText.contains("0 errors"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testCheckColumnsArrayDFileOk() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table chk_arr_ok (arr double[], ts timestamp) timestamp(ts) partition by DAY");
            execute(
                    "insert into chk_arr_ok select ARRAY[1.0, 2.0, 3.0],"
                            + " timestamp_sequence('1970-01-01', 1000000L) from long_sequence(2)"
            );

            String[] result = runSession("cd chk_arr_ok\ncheck columns\nquit\n");
            String outText = result[0];
            Assert.assertTrue(outText.contains("OK"));
            Assert.assertTrue(outText.contains("0 errors"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testCheckColumnsArrayDFileTruncated() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table chk_arr_trunc (arr double[], ts timestamp) timestamp(ts) partition by DAY");
            execute(
                    "insert into chk_arr_trunc select ARRAY[1.0, 2.0, 3.0],"
                            + " timestamp_sequence('1970-01-01', 1000000L) from long_sequence(2)"
            );

            // truncate .d to 1 byte — guaranteed shorter than data regardless of alignment
            truncateColumnFileTo("chk_arr_trunc", "1970-01-01", "arr.d", 1);

            String[] result = runSession("cd chk_arr_trunc\ncheck columns\nquit\n");
            String outText = result[0];
            Assert.assertTrue("should detect corrupt .d file", outText.contains("ERROR"));
            Assert.assertTrue("should show data file too short", outText.contains("data file too short"));
        });
    }

    @Test
    public void testCheckColumnsStringDFileTruncated() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table chk_str_trunc (s string, ts timestamp) timestamp(ts) partition by DAY");
            execute(
                    "insert into chk_str_trunc select rnd_str('hello','world','test'),"
                            + " timestamp_sequence('1970-01-01', 1000000L) from long_sequence(3)"
            );

            // truncate .d to 1 byte — guaranteed shorter than data regardless of alignment
            truncateColumnFileTo("chk_str_trunc", "1970-01-01", "s.d", 1);

            String[] result = runSession("cd chk_str_trunc\ncheck columns\nquit\n");
            String outText = result[0];
            Assert.assertTrue("should detect corrupt .d file", outText.contains("ERROR"));
            Assert.assertTrue("should show data file too short", outText.contains("data file too short"));
        });
    }

    @Test
    public void testCheckColumnsStringDFileTruncatedToZero() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table chk_str_zero (s string, ts timestamp) timestamp(ts) partition by DAY");
            execute(
                    "insert into chk_str_zero select rnd_str('hello','world','test'),"
                            + " timestamp_sequence('1970-01-01', 1000000L) from long_sequence(3)"
            );

            truncateColumnFileTo("chk_str_zero", "1970-01-01", "s.d", 0);

            String[] result = runSession("cd chk_str_zero\ncheck columns\nquit\n");
            String outText = result[0];
            Assert.assertTrue("should detect corrupt .d file", outText.contains("ERROR"));
        });
    }

    @Test
    public void testCheckColumnsStringNullsOk() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table chk_str_null (s string, ts timestamp) timestamp(ts) partition by DAY");
            execute(
                    "insert into chk_str_null select rnd_str('hello', null, 'test'),"
                            + " timestamp_sequence('1970-01-01', 1000000L) from long_sequence(3)"
            );

            String[] result = runSession("cd chk_str_null\ncheck columns\nquit\n");
            String outText = result[0];
            Assert.assertTrue(outText.contains("OK"));
            Assert.assertTrue(outText.contains("0 errors"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testCheckColumnsVarcharDFileTruncated() throws Exception {
        assertMemoryLeak(() -> {
            // use long strings (>9 bytes) to ensure they are NOT inlined
            execute("create table chk_vc_trunc (v varchar, ts timestamp) timestamp(ts) partition by DAY");
            execute(
                    "insert into chk_vc_trunc select rnd_varchar('this_is_a_long_string','another_long_string_here','yet_more_long_text'),"
                            + " timestamp_sequence('1970-01-01', 1000000L) from long_sequence(3)"
            );

            // truncate .d to 1 byte — guaranteed shorter than data regardless of alignment
            truncateColumnFileTo("chk_vc_trunc", "1970-01-01", "v.d", 1);

            String[] result = runSession("cd chk_vc_trunc\ncheck columns\nquit\n");
            String outText = result[0];
            Assert.assertTrue("should detect corrupt .d file", outText.contains("ERROR"));
            Assert.assertTrue("should show data file too short", outText.contains("data file too short"));
        });
    }

    @Test
    public void testCheckColumnsVarcharInlinedOk() throws Exception {
        assertMemoryLeak(() -> {
            // short strings (<=9 bytes) are fully inlined, .d file not needed
            execute("create table chk_vc_inline (v varchar, ts timestamp) timestamp(ts) partition by DAY");
            execute(
                    "insert into chk_vc_inline select rnd_varchar('a','bb','ccc'),"
                            + " timestamp_sequence('1970-01-01', 1000000L) from long_sequence(3)"
            );

            String[] result = runSession("cd chk_vc_inline\ncheck columns\nquit\n");
            String outText = result[0];
            Assert.assertTrue(outText.contains("OK"));
            Assert.assertTrue(outText.contains("0 errors"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testCheckColumnsVarcharMixedInlinedAndNonInlined() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table chk_vc_mix (v varchar, ts timestamp) timestamp(ts) partition by DAY");
            execute(
                    "insert into chk_vc_mix select rnd_varchar('a','this_is_a_long_string','bb','another_long_string_here'),"
                            + " timestamp_sequence('1970-01-01', 1000000L) from long_sequence(4)"
            );

            String[] result = runSession("cd chk_vc_mix\ncheck columns\nquit\n");
            String outText = result[0];
            Assert.assertTrue(outText.contains("OK"));
            Assert.assertTrue(outText.contains("0 errors"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testCheckColumnsWithColumnTop() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table chk_top (val long, ts timestamp) timestamp(ts) partition by DAY WAL");
            execute("insert into chk_top values (1, '1970-01-01T00:00:00.000000Z')");
            execute("insert into chk_top values (2, '1970-01-02T00:00:00.000000Z')");
            waitForAppliedRows("chk_top", 2);

            execute("alter table chk_top add column new_int int");
            drainWalQueue(engine);

            execute("insert into chk_top values (3, '1970-01-03T00:00:00.000000Z', 42)");
            waitForAppliedRows("chk_top", 3);

            String[] result = runSession("cd chk_top\ncheck columns\nquit\n");
            String outText = result[0];
            Assert.assertTrue(outText.contains("check complete"));
            Assert.assertTrue(outText.contains("0 errors"));
            // new_int should be SKIPPED for old partitions (not in partition) or OK
            Assert.assertTrue(
                    outText.contains("SKIPPED") || outText.contains("OK")
            );
        });
    }

    @Test
    public void testCdDeepFromColumn() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithRows("nav_col_deep", 2);
            String[] result = runSession("cd nav_col_deep\ncd 0\ncd sym\ncd foo\nquit\n");
            Assert.assertTrue(result[1].contains("already at leaf level"));
        });
    }

    @Test
    public void testCdDotDotFromColumn() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithRows("nav_col_dotdot", 2);
            String[] result = runSession("cd nav_col_dotdot\ncd 0\ncd sym\ncd ..\nls\nquit\n");
            // after cd .. from column, ls should show column list
            Assert.assertTrue(result[0].contains("column_name"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testCdIntoColumnByIndex() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithRows("nav_col_idx", 2);
            String[] result = runSession("cd nav_col_idx\ncd 0\ncd 0\npwd\nquit\n");
            Assert.assertTrue(result[0].contains("/nav_col_idx/1970-01-01/sym\n"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testCdIntoColumnByName() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithRows("nav_col_name", 2);
            String[] result = runSession("cd nav_col_name\ncd 0\ncd ts\npwd\nquit\n");
            Assert.assertTrue(result[0].contains("/nav_col_name/1970-01-01/ts\n"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testCdIntoDroppedColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table nav_col_drop (val int, sym symbol, ts timestamp) timestamp(ts) partition by DAY WAL");
            execute("insert into nav_col_drop values (1, 'A', '1970-01-01T00:00:00.000000Z')");
            waitForAppliedRows("nav_col_drop", 1);
            execute("alter table nav_col_drop drop column val");
            drainWalQueue(engine);

            String[] result = runSession("cd nav_col_drop\ncd 0\ncd val\nquit\n");
            Assert.assertTrue(result[1].contains("cannot enter dropped column"));
        });
    }

    @Test
    public void testCdIntoNonexistentColumn() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithRows("nav_col_nope", 2);
            String[] result = runSession("cd nav_col_nope\ncd 0\ncd nosuchcol\nquit\n");
            Assert.assertTrue(result[1].contains("column not found"));
        });
    }

    @Test
    public void testCdSlashFromColumn() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithRows("nav_col_slash", 2);
            String[] result = runSession("cd nav_col_slash\ncd 0\ncd sym\ncd /\npwd\nquit\n");
            Assert.assertTrue(result[0].contains("recover:/>"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testLsAtColumnLevel() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithRows("nav_col_ls", 2);
            String[] result = runSession("cd nav_col_ls\ncd 0\ncd sym\nls\nquit\n");
            String outText = result[0];
            Assert.assertTrue(outText.contains("column: sym"));
            Assert.assertTrue(outText.contains("type: SYMBOL"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testLsColumnsShowsNotInPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table ls_nip (val long, ts timestamp) timestamp(ts) partition by DAY WAL");
            execute("insert into ls_nip values (1, '1970-01-01T00:00:00.000000Z')");
            execute("insert into ls_nip values (2, '1970-01-02T00:00:00.000000Z')");
            waitForAppliedRows("ls_nip", 2);

            execute("alter table ls_nip add column new_int int");
            drainWalQueue(engine);

            execute("insert into ls_nip values (3, '1970-01-03T00:00:00.000000Z', 42)");
            waitForAppliedRows("ls_nip", 3);

            // ls at partition 0 (1970-01-01) should mark new_int as "not in partition"
            String[] result = runSession("cd ls_nip\ncd 0\nls\nquit\n");
            String outText = result[0];
            Assert.assertTrue("note column header should appear", outText.contains("note"));
            Assert.assertTrue(
                    "new_int should be marked not in partition",
                    outText.contains("not in partition")
            );
            // val and ts should NOT be marked
            for (String line : outText.split("\n")) {
                if (line.contains("val") && !line.contains("note")) {
                    Assert.assertFalse(
                            "val should not be marked not in partition",
                            line.contains("not in partition")
                    );
                }
            }
            Assert.assertEquals("", result[1]);

            // ls at partition 2 (1970-01-03) should NOT mark new_int
            String[] result2 = runSession("cd ls_nip\ncd 2\nls\nquit\n");
            String outText2 = result2[0];
            Assert.assertFalse(
                    "new_int should not be marked in partition where it exists",
                    outText2.contains("not in partition")
            );
            Assert.assertEquals("", result2[1]);
        });
    }

    @Test
    public void testPrintAtWrongLevel() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithRows("nav_print_wrong", 2);
            String[] result = runSession("cd nav_print_wrong\ncd 0\nprint 0\nquit\n");
            Assert.assertTrue(result[1].contains("print is only valid at column level"));
        });
    }

    @Test
    public void testPrintFixedSizeInt() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table print_int (val int, ts timestamp) timestamp(ts) partition by DAY WAL");
            execute("insert into print_int values (42, '1970-01-01T00:00:00.000000Z')");
            waitForAppliedRows("print_int", 1);

            String[] result = runSession("cd print_int\ncd 0\ncd val\nprint 0\nquit\n");
            Assert.assertTrue(result[0].contains("[0] = 42"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testPrintFixedSizeTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table print_ts (val int, ts timestamp) timestamp(ts) partition by DAY WAL");
            execute("insert into print_ts values (1, '2024-06-15T12:30:00.000000Z')");
            waitForAppliedRows("print_ts", 1);

            String[] result = runSession("cd print_ts\ncd 0\ncd ts\nprint 0\nquit\n");
            String outText = result[0];
            Assert.assertTrue(outText.contains("[0] = 2024-06-15"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testPrintInvalidRowNumber() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table print_bad (val int, ts timestamp) timestamp(ts) partition by DAY WAL");
            execute("insert into print_bad values (1, '1970-01-01T00:00:00.000000Z')");
            waitForAppliedRows("print_bad", 1);

            String[] result = runSession("cd print_bad\ncd 0\ncd val\nprint abc\nquit\n");
            Assert.assertTrue(result[1].contains("invalid row number"));
        });
    }

    @Test
    public void testPrintNullValue() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table print_null (val int, ts timestamp) timestamp(ts) partition by DAY WAL");
            execute("insert into print_null values (null, '1970-01-01T00:00:00.000000Z')");
            waitForAppliedRows("print_null", 1);

            String[] result = runSession("cd print_null\ncd 0\ncd val\nprint 0\nquit\n");
            Assert.assertTrue(result[0].contains("[0] = null"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testPrintWithColumnTop() throws Exception {
        assertMemoryLeak(() -> {
            // create table, insert into first partition, add column, insert into same partition
            execute("create table print_top (val long, ts timestamp) timestamp(ts) partition by DAY WAL");
            execute("insert into print_top values (1, '1970-01-01T00:00:00.000000Z')");
            waitForAppliedRows("print_top", 1);

            execute("alter table print_top add column new_int int");
            drainWalQueue(engine);

            execute("insert into print_top values (2, '1970-01-01T12:00:00.000000Z', 42)");
            waitForAppliedRows("print_top", 2);

            // new_int has columnTop=1 in the first partition (row 0 has no data, row 1 has value 42)
            String[] result = runSession("cd print_top\ncd 0\ncd new_int\nprint 0\nprint 1\nquit\n");
            String outText = result[0];
            Assert.assertTrue("row 0 should show column top null", outText.contains("[0] = null (column top)"));
            Assert.assertTrue("row 1 should show 42", outText.contains("[1] = 42"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testPrintRowOutOfRange() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table print_oor (val int, ts timestamp) timestamp(ts) partition by DAY WAL");
            execute("insert into print_oor values (1, '1970-01-01T00:00:00.000000Z')");
            waitForAppliedRows("print_oor", 1);

            String[] result = runSession("cd print_oor\ncd 0\ncd val\nprint 999\nquit\n");
            Assert.assertTrue(result[1].contains("row out of range"));
        });
    }

    @Test
    public void testPrintString() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table print_str (s string, ts timestamp) timestamp(ts) partition by DAY WAL");
            execute("insert into print_str values ('hello', '1970-01-01T00:00:00.000000Z')");
            waitForAppliedRows("print_str", 1);

            String[] result = runSession("cd print_str\ncd 0\ncd s\nprint 0\nquit\n");
            Assert.assertTrue(result[0].contains("[0] = hello"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testPrintVarchar() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table print_vc (v varchar, ts timestamp) timestamp(ts) partition by DAY WAL");
            execute("insert into print_vc values ('world', '1970-01-01T00:00:00.000000Z')");
            waitForAppliedRows("print_vc", 1);

            String[] result = runSession("cd print_vc\ncd 0\ncd v\nprint 0\nquit\n");
            Assert.assertTrue(result[0].contains("[0] = world"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testPromptAtColumnLevel() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithRows("nav_col_prompt", 2);
            String[] result = runSession("cd nav_col_prompt\ncd 0\ncd sym\nquit\n");
            Assert.assertTrue(result[0].contains("recover:/nav_col_prompt/1970-01-01/sym>"));
        });
    }

    @Test
    public void testPwdAtColumnLevel() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithRows("nav_col_pwd", 2);
            String[] result = runSession("cd nav_col_pwd\ncd 0\ncd sym\npwd\nquit\n");
            Assert.assertTrue(result[0].contains("/nav_col_pwd/1970-01-01/sym\n"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testShowAtColumnLevel() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithRows("nav_col_show", 2);
            String[] result = runSession("cd nav_col_show\ncd 0\ncd sym\nshow\nquit\n");
            String outText = result[0];
            Assert.assertTrue(outText.contains("column: sym"));
            Assert.assertTrue(outText.contains("type: SYMBOL"));
            Assert.assertTrue(outText.contains("effectiveRows:"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testShowAtColumnLevelVarSize() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table nav_col_show_var (v varchar, ts timestamp) timestamp(ts) partition by DAY WAL");
            execute("insert into nav_col_show_var values ('hello', '1970-01-01T00:00:00.000000Z')");
            waitForAppliedRows("nav_col_show_var", 1);

            String[] result = runSession("cd nav_col_show_var\ncd 0\ncd v\nshow\nquit\n");
            String outText = result[0];
            Assert.assertTrue(outText.contains("column: v"));
            Assert.assertTrue(outText.contains("type: VARCHAR"));
            Assert.assertTrue(outText.contains("actual aux size:"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testPrintColumnNotInPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table print_nip (val long, ts timestamp) timestamp(ts) partition by DAY WAL");
            execute("insert into print_nip values (1, '1970-01-01T00:00:00.000000Z')");
            execute("insert into print_nip values (2, '1970-01-02T00:00:00.000000Z')");
            waitForAppliedRows("print_nip", 2);

            execute("alter table print_nip add column new_int int");
            drainWalQueue(engine);

            execute("insert into print_nip values (3, '1970-01-03T00:00:00.000000Z', 42)");
            waitForAppliedRows("print_nip", 3);

            // cd into 1970-01-01 partition where new_int doesn't exist
            String[] result = runSession("cd print_nip\ncd 0\ncd new_int\nprint 0\nquit\n");
            Assert.assertTrue(
                    "should report column not in partition",
                    result[1].contains("column not in this partition")
            );
        });
    }

    @Test
    public void testShowColumnInPartitionWithTop() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table show_top (val long, ts timestamp) timestamp(ts) partition by DAY WAL");
            execute("insert into show_top values (1, '1970-01-01T00:00:00.000000Z')");
            waitForAppliedRows("show_top", 1);

            execute("alter table show_top add column new_int int");
            drainWalQueue(engine);

            execute("insert into show_top values (2, '1970-01-01T12:00:00.000000Z', 42)");
            waitForAppliedRows("show_top", 2);

            // cd into 1970-01-01 partition where new_int has columnTop=1
            String[] result = runSession("cd show_top\ncd 0\ncd new_int\nshow\nquit\n");
            String outText = result[0];
            Assert.assertTrue(outText.contains("columnTop:"));
            Assert.assertTrue(outText.contains("effectiveRows:"));
            Assert.assertFalse(
                    "should NOT say not in partition",
                    outText.contains("not in partition")
            );
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testShowColumnNotInPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table show_nip (val long, ts timestamp) timestamp(ts) partition by DAY WAL");
            execute("insert into show_nip values (1, '1970-01-01T00:00:00.000000Z')");
            execute("insert into show_nip values (2, '1970-01-02T00:00:00.000000Z')");
            waitForAppliedRows("show_nip", 2);

            execute("alter table show_nip add column new_int int");
            drainWalQueue(engine);

            execute("insert into show_nip values (3, '1970-01-03T00:00:00.000000Z', 42)");
            waitForAppliedRows("show_nip", 3);

            // cd into 1970-01-01 partition where new_int doesn't exist
            String[] result = runSession("cd show_nip\ncd 0\ncd new_int\nshow\nquit\n");
            String outText = result[0];
            Assert.assertTrue(
                    "should say not in partition",
                    outText.contains("not in partition")
            );
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testViewWithDeletedViewFile() throws Exception {
        assertMemoryLeak(() -> {
            createViewWithBase("del_view", "del_view_base");

            // delete _view file
            TableToken token = engine.verifyTableName("del_view");
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(token.getDirName())
                        .concat(ViewDefinition.VIEW_DEFINITION_FILE_NAME).$();
                Assert.assertTrue(FF.removeQuiet(path.$()));
            }

            String[] result = runSession("tables\nquit\n");
            String outText = result[0];
            // table should still be discovered (dir exists), type unknown since no _view
            // but registry may provide type
            boolean found = false;
            for (String line : outText.split("\n")) {
                if (line.contains("del_view") && !line.contains("del_view_base")) {
                    found = true;
                    break;
                }
            }
            Assert.assertTrue("view with deleted _view file should still be discovered", found);
        });
    }

    private static void deleteAllRegistryFiles() {
        try (Path path = new Path()) {
            long findPtr = FF.findFirst(path.of(configuration.getDbRoot()).$());
            if (findPtr < 1) {
                return;
            }
            try {
                io.questdb.std.str.StringSink nameSink = new io.questdb.std.str.StringSink();
                do {
                    nameSink.clear();
                    Utf8s.utf8ToUtf16Z(FF.findName(findPtr), nameSink);
                    if (Chars.startsWith(nameSink, "tables.d.")) {
                        try (Path filePath = new Path()) {
                            FF.removeQuiet(filePath.of(configuration.getDbRoot()).concat(nameSink).$());
                        }
                    }
                } while (FF.findNext(findPtr) > 0);
            } finally {
                FF.findClose(findPtr);
            }
        }
    }

    private static void createMatViewWithBase(String mvName, String baseName, int rowCount) throws SqlException {
        execute("create table " + baseName
                + " (sym symbol, price double, ts timestamp) timestamp(ts) partition by DAY WAL");
        if (rowCount > 0) {
            execute("insert into " + baseName
                    + " select rnd_symbol('A','B'), rnd_double(),"
                    + " timestamp_sequence('2024-01-01', 3600000000L) from long_sequence(" + rowCount + ")");
        }
        drainWalQueue(engine);
        execute("create materialized view " + mvName
                + " as (select sym, last(price) as price, ts from " + baseName
                + " sample by 1h) partition by DAY");
        drainWalAndMatViewQueues();
    }

    private static void createOrphanDir(String tableName, String orphanName) {
        TableToken token = engine.verifyTableName(tableName);
        try (Path path = new Path()) {
            path.of(configuration.getDbRoot()).concat(token.getDirName()).concat(orphanName).slash$();
            Assert.assertEquals("failed to create orphan dir: " + path, 0, FF.mkdirs(path, configuration.getMkDirMode()));
        }
    }

    private static void createNonWalTableWithRows(String tableName, int rowCount) throws SqlException {
        execute("create table " + tableName + " (sym symbol, ts timestamp) timestamp(ts) partition by DAY");
        execute(
                "insert into "
                        + tableName
                        + " select rnd_symbol('AA', 'BB', 'CC'), timestamp_sequence('1970-01-01', "
                        + Micros.DAY_MICROS
                        + "L) from long_sequence("
                        + rowCount
                        + ")"
        );
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

    private static void createViewWithBase(String viewName, String baseName) throws SqlException {
        execute("create table " + baseName
                + " (sym symbol, price double, ts timestamp) timestamp(ts) partition by DAY WAL");
        drainWalQueue(engine);
        execute("create view " + viewName
                + " as (select sym, last(price) as price, ts from " + baseName + " sample by 1h)");
    }

    private static void truncateColumnFileTo(String tableName, String partitionDirName, String columnFileName, long targetSize) {
        TableToken token = engine.verifyTableName(tableName);
        try (Path path = new Path()) {
            path.of(configuration.getDbRoot()).concat(token).concat(partitionDirName).concat(columnFileName);
            long fd = FF.openRW(path.$(), CairoConfiguration.O_NONE);
            Assert.assertTrue("failed to open file: " + path, fd > -1);
            try {
                Assert.assertTrue("failed to truncate file: " + path, FF.truncate(fd, targetSize));
            } finally {
                FF.close(fd);
            }
        }
    }

    private static void deletePartitionDir(String tableName, String partitionDirName) {
        TableToken token = engine.verifyTableName(tableName);
        try (Path path = new Path()) {
            path.of(configuration.getDbRoot()).concat(token.getDirName()).concat(partitionDirName).$();
            Assert.assertTrue("failed to remove partition dir: " + path, FF.rmdir(path));
        }
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
                new ColumnCheckService(FF),
                new ColumnValueReader(FF),
                new ColumnVersionStateService(new BoundedColumnVersionReader(FF)),
                FF,
                new MetaStateService(new BoundedMetaReader(FF)),
                new PartitionScanService(FF),
                new RegistryStateService(new BoundedRegistryReader(FF)),
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
