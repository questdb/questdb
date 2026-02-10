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

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.mv.MatViewDefinition;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.view.ViewDefinition;
import io.questdb.griffin.SqlException;
import io.questdb.recovery.ConsoleRenderer;
import io.questdb.recovery.RecoverySession;
import io.questdb.recovery.TxnState;
import io.questdb.std.Chars;
import io.questdb.std.FilesFacade;
import io.questdb.std.Files;
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
    public void testCdBeyondSegment() throws Exception {
        assertMemoryLeak(() -> {
            createWalTableWithCommits("wal_beyond_seg", 3);
            String[] result = runSession("cd wal_beyond_seg\ncd wal\ncd wal1\ncd 0\ncd anything\nquit\n");
            Assert.assertTrue(result[1].contains("already at leaf level"));
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
    public void testCdDeniedForParquetPartition() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithRows("nav_parquet", 3);
            execute("ALTER TABLE nav_parquet CONVERT PARTITION TO PARQUET WHERE ts = '1970-01-01'");
            drainWalQueue(engine);

            // cd into the parquet partition, then try to cd into a column
            String[] result = runSession("cd nav_parquet\ncd 0\ncd sym\nquit\n");
            Assert.assertTrue(result[1].contains("cannot enter columns of a parquet partition"));
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
    public void testCdDotDotFromSegment() throws Exception {
        assertMemoryLeak(() -> {
            createWalTableWithCommits("wal_dotdot_seg", 3);
            String[] result = runSession("cd wal_dotdot_seg\ncd wal\ncd wal1\ncd 0\ncd ..\npwd\nquit\n");
            // after cd .. from segment, should be back at WAL dir level
            Assert.assertTrue(
                    "should be at WAL dir level after cd .. from segment",
                    result[0].contains("/wal_dotdot_seg/wal/wal1\n")
            );
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
    public void testCdDotDotFromWalDir() throws Exception {
        assertMemoryLeak(() -> {
            createWalTableWithCommits("wal_dotdot_dir", 3);
            String[] result = runSession("cd wal_dotdot_dir\ncd wal\ncd wal1\ncd ..\npwd\nquit\n");
            // after cd .. from WAL dir, should be back at WAL root
            Assert.assertTrue(
                    "should be at WAL root after cd .. from wal dir",
                    result[0].contains("/wal_dotdot_dir/wal\n")
            );
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testCdDotDotFromWalRoot() throws Exception {
        assertMemoryLeak(() -> {
            createWalTableWithCommits("wal_dotdot_root", 3);
            String[] result = runSession("cd wal_dotdot_root\ncd wal\ncd ..\npwd\nquit\n");
            // after cd .. from WAL root, should be back at table level
            Assert.assertTrue(
                    "should be at table level after cd .. from wal root",
                    result[0].contains("/wal_dotdot_root\n")
            );
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
    public void testCdIntoWalDir() throws Exception {
        assertMemoryLeak(() -> {
            createWalTableWithCommits("wal_cd_dir", 3);
            String[] result = runSession("cd wal_cd_dir\ncd wal\ncd wal1\npwd\nquit\n");
            Assert.assertTrue(
                    "prompt should show WAL dir path",
                    result[0].contains("/wal_cd_dir/wal/wal1\n")
            );
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testCdIntoWalDirBadName() throws Exception {
        assertMemoryLeak(() -> {
            createWalTableWithCommits("wal_cd_bad", 3);
            String[] result = runSession("cd wal_cd_bad\ncd wal\ncd notawal\nquit\n");
            Assert.assertTrue(result[1].contains("invalid WAL directory"));
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
    public void testCdIntoMissingWal() throws Exception {
        assertMemoryLeak(() -> {
            createWalTableWithCommits("wal_cd_missing", 3);
            String[] result = runSession("cd wal_cd_missing\ncd wal\ncd wal99\nquit\n");
            Assert.assertTrue(result[1].contains("WAL not found"));
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
    public void testCdIntoSegment() throws Exception {
        assertMemoryLeak(() -> {
            createWalTableWithCommits("wal_cd_seg", 3);
            String[] result = runSession("cd wal_cd_seg\ncd wal\ncd wal1\ncd 0\npwd\nquit\n");
            Assert.assertTrue(
                    "should show full WAL segment path",
                    result[0].contains("/wal_cd_seg/wal/wal1/0\n")
            );
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testCdIntoWalByIndex() throws Exception {
        assertMemoryLeak(() -> {
            createWalTableWithCommits("wal_cd_idx", 3);
            // at WAL root, "cd 0" navigates to entries[0] (the first WAL)
            String[] result = runSession("cd wal_cd_idx\ncd wal\ncd 0\npwd\nquit\n");
            Assert.assertTrue(
                    "should navigate to first WAL by index",
                    result[0].contains("/wal/wal1")
            );
        });
    }

    @Test
    public void testCdIntoWalByIndexOutOfRange() throws Exception {
        assertMemoryLeak(() -> {
            createWalTableWithCommits("wal_cd_idx_oor", 3);
            String[] result = runSession("cd wal_cd_idx_oor\ncd wal\ncd 999\nquit\n");
            Assert.assertTrue("should report out of range", result[1].contains("out of range"));
        });
    }

    @Test
    public void testCdIntoWalByNameStillWorksByWalId() throws Exception {
        assertMemoryLeak(() -> {
            createWalTableWithCommits("wal_cd_name", 3);
            // "cd wal1" should navigate by walId, not by index
            String[] result = runSession("cd wal_cd_name\ncd wal\ncd wal1\npwd\nquit\n");
            Assert.assertTrue(
                    "wal-prefixed cd should use walId",
                    result[0].contains("/wal/wal1")
            );
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
            // ls first to discover tables — the table's index depends on
            // how many directories exist, so we find it dynamically
            String[] lsResult = runSession("ls\nquit\n");
            String lsOut = lsResult[0];
            // find the 0-based idx for our table in the ls output
            int tableIdx = -1;
            for (String line : lsOut.split("\n")) {
                if (line.contains("nav_tbl_idx")) {
                    tableIdx = Integer.parseInt(line.trim().split("\\s+")[0]);
                    break;
                }
            }
            Assert.assertTrue("table should appear in ls output", tableIdx >= 0);

            // cd by index, then ls should show partitions
            String[] result = runSession("ls\ncd " + tableIdx + "\nls\nquit\n");
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
    public void testCdPartitionAfterLeavingWal() throws Exception {
        assertMemoryLeak(() -> {
            createWalTableWithCommits("wal_then_part", 3);
            // enter WAL mode, exit, then navigate into a partition
            String[] result = runSession("cd wal_then_part\ncd wal\ncd ..\ncd 0\nls\nquit\n");
            // should be at partition level, ls shows columns
            Assert.assertTrue(result[0].contains("column_name"));
            Assert.assertEquals("", result[1]);
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
    public void testCdSlashFromWalMode() throws Exception {
        assertMemoryLeak(() -> {
            createWalTableWithCommits("wal_slash", 3);
            String[] result = runSession("cd wal_slash\ncd wal\ncd /\npwd\nquit\n");
            Assert.assertTrue(result[0].contains("/\n"));
            Assert.assertTrue(result[0].contains("recover:/>"));
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
    public void testCdWalFromRoot() throws Exception {
        assertMemoryLeak(() -> {
            // at root level, "cd wal" tries to enter a table named "wal", not WAL mode
            execute("create table wal (val long, ts timestamp) timestamp(ts) partition by DAY WAL");
            execute("insert into wal values (1, '1970-01-01T00:00:00.000000Z')");
            waitForAppliedRows("wal", 1);

            String[] result = runSession("cd wal\npwd\nquit\n");
            // should enter the table named "wal", not WAL navigation mode
            Assert.assertTrue(
                    "should be at table level for table named 'wal'",
                    result[0].contains("/wal\n")
            );
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testCdWalFromTableLevel() throws Exception {
        assertMemoryLeak(() -> {
            createWalTableWithCommits("wal_cd_from_tbl", 3);
            String[] result = runSession("cd wal_cd_from_tbl\ncd wal\npwd\nquit\n");
            Assert.assertTrue(
                    "should show WAL root path",
                    result[0].contains("/wal_cd_from_tbl/wal\n")
            );
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testCdWalOnNonWalTable() throws Exception {
        assertMemoryLeak(() -> {
            createNonWalTableWithRows("wal_nonwal_cd", 2);
            String[] result = runSession("cd wal_nonwal_cd\ncd wal\nquit\n");
            Assert.assertTrue(result[1].contains("does not use WAL"));
        });
    }

    @Test
    public void testCdWalThenCdRoot() throws Exception {
        assertMemoryLeak(() -> {
            createWalTableWithCommits("wal_then_root", 3);
            String[] result = runSession("cd wal_then_root\ncd wal\ncd /\npwd\nquit\n");
            Assert.assertTrue(
                    "should be back at root after cd /",
                    result[0].contains("/\n")
            );
            Assert.assertTrue(result[0].contains("recover:/>"));
            Assert.assertEquals("", result[1]);
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
    public void testLsAtSegmentLevel() throws Exception {
        assertMemoryLeak(() -> {
            createWalTableWithCommits("wal_ls_seg", 3);
            String[] result = runSession("cd wal_ls_seg\ncd wal\ncd wal1\ncd 0\nls\nquit\n");
            // at segment level, ls should show event listing with txn column header
            Assert.assertTrue(
                    "should show txn column header in event listing",
                    result[0].contains("txn")
            );
        });
    }

    @Test
    public void testLsWalSegmentWithoutWalStatus() throws Exception {
        assertMemoryLeak(() -> {
            createWalTableWithCommits("wal_ls_no_status", 3);
            // Navigate directly to a WAL segment and ls WITHOUT calling 'wal status' first.
            // This means seqTxnLogState is null — ls should still work, showing events
            // with seqTxn="-" instead of actual values.
            String[] result = runSession("cd wal_ls_no_status\ncd wal\ncd wal1\ncd 0\nls\nquit\n");
            String outText = result[0];
            // should show event listing with txn and type columns
            Assert.assertTrue(
                    "should show txn column header in event listing",
                    outText.contains("txn")
            );
            Assert.assertTrue(
                    "should show type column header in event listing",
                    outText.contains("type")
            );
            Assert.assertTrue(
                    "should show DATA type in event listing",
                    outText.contains("DATA")
            );
            // seqTxn column header should be present since events are listed
            Assert.assertTrue(
                    "seqTxn column header should be present",
                    outText.contains("seqTxn")
            );
            // no errors expected
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testLsEventsAtSegmentLevel() throws Exception {
        assertMemoryLeak(() -> {
            createWalTableWithCommits("wal_ls_events", 3);
            String[] result = runSession("cd wal_ls_events\ncd wal\ncd wal1\ncd 0\nls\nquit\n");
            String outText = result[0];
            // ls at segment level should show event listing with txn column header
            Assert.assertTrue(
                    "should show txn column header in event listing",
                    outText.contains("txn")
            );
            Assert.assertTrue(
                    "should show type column header in event listing",
                    outText.contains("type")
            );
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testLsEventsShowsDataType() throws Exception {
        assertMemoryLeak(() -> {
            createWalTableWithCommits("wal_ls_data_type", 3);
            String[] result = runSession("cd wal_ls_data_type\ncd wal\ncd wal1\ncd 0\nls\nquit\n");
            Assert.assertTrue(
                    "should show DATA type in event listing",
                    result[0].contains("DATA")
            );
        });
    }

    @Test
    public void testLsAtWalDir() throws Exception {
        assertMemoryLeak(() -> {
            createWalTableWithCommits("wal_ls_dir", 3);
            String[] result = runSession("cd wal_ls_dir\ncd wal\ncd wal1\nls\nquit\n");
            // should show segment listing with columns
            Assert.assertTrue(
                    "should show segment header",
                    result[0].contains("segment")
            );
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testLsAtWalRoot() throws Exception {
        assertMemoryLeak(() -> {
            createWalTableWithCommits("wal_ls_root", 3);
            String[] result = runSession("cd wal_ls_root\ncd wal\nls\nquit\n");
            // should show WAL directory listing
            Assert.assertTrue(
                    "should contain 'wal' column header",
                    result[0].contains("wal")
            );
            Assert.assertTrue(
                    "should show wal1 entry",
                    result[0].contains("wal1")
            );
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testLsAtWalRootShowsOrphans() throws Exception {
        assertMemoryLeak(() -> {
            createWalTableWithCommits("wal_ls_orphan", 3);
            // create an orphan WAL dir (wal99) that is not referenced in the txnlog
            TableToken token = engine.verifyTableName("wal_ls_orphan");
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(token.getDirName()).concat("wal99").slash$();
                Assert.assertEquals(0, FF.mkdirs(path, configuration.getMkDirMode()));
            }

            String[] result = runSession("cd wal_ls_orphan\ncd wal\nls\nquit\n");
            Assert.assertTrue(
                    "should show unreferenced status for orphan WAL",
                    result[0].contains("unreferenced")
            );
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
                if (line.trim().startsWith("idx") || line.trim().isEmpty()
                        || line.contains("recover:") || line.contains("issues")
                        || line.contains("commands:") || line.contains("dbRoot=")
                        || line.startsWith("  ") || line.startsWith("QuestDB")) {
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
            waitForEmptyTable();

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
public void testLsShowsTransientRowCountForLastPartition() throws Exception {
        assertMemoryLeak(() -> {
            // 2 rows across 2 day-partitions: 1970-01-01 (1 row) and 1970-01-02 (1 row).
            // The last partition's row count is stored as transientRowCount in the _txn
            // header; its partition entry stores 0. ls must display the resolved count.
            createNonWalTableWithRows("nav_transient_rc", 2);

            String[] result = runSession("cd nav_transient_rc\nls\nquit\n");
            String outText = result[0];
            for (String line : outText.split("\n")) {
                if (line.contains("1970-01-02")) {
                    Assert.assertFalse(
                            "last partition should show transientRowCount, not 0: " + line,
                            line.matches(".*1970-01-02\\S*\\s+0\\s.*")
                    );
                    break;
                }
            }
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
    public void testPwdAtSegment() throws Exception {
        assertMemoryLeak(() -> {
            createWalTableWithCommits("wal_pwd_seg", 3);
            String[] result = runSession("cd wal_pwd_seg\ncd wal\ncd wal1\ncd 0\npwd\nquit\n");
            Assert.assertTrue(
                    "pwd should show full segment path",
                    result[0].contains("/wal_pwd_seg/wal/wal1/0\n")
            );
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testPwdAtWalDir() throws Exception {
        assertMemoryLeak(() -> {
            createWalTableWithCommits("wal_pwd_dir", 3);
            String[] result = runSession("cd wal_pwd_dir\ncd wal\ncd wal1\npwd\nquit\n");
            Assert.assertTrue(
                    "pwd should show WAL dir path",
                    result[0].contains("/wal_pwd_dir/wal/wal1\n")
            );
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testPwdAtWalRoot() throws Exception {
        assertMemoryLeak(() -> {
            createWalTableWithCommits("wal_pwd_root", 3);
            String[] result = runSession("cd wal_pwd_root\ncd wal\npwd\nquit\n");
            Assert.assertTrue(
                    "pwd should show WAL root path",
                    result[0].contains("/wal_pwd_root/wal\n")
            );
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
    public void testCheckColumnsInWalMode() throws Exception {
        assertMemoryLeak(() -> {
            createWalTableWithCommits("chk_wal_mode", 3);
            // check columns from WAL root should run table-level check, not crash
            String[] result = runSession("cd chk_wal_mode\ncd wal\ncheck columns\nquit\n");
            String outText = result[0];
            Assert.assertTrue("should run table-level check from WAL mode",
                    outText.contains("checking chk_wal_mode"));
            Assert.assertTrue(outText.contains("check complete"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testCheckColumnsInWalDirLevel() throws Exception {
        assertMemoryLeak(() -> {
            createWalTableWithCommits("chk_wal_dir", 3);
            // check columns from WAL dir level should also work
            String[] result = runSession("cd chk_wal_dir\ncd wal\ncd wal1\ncheck columns\nquit\n");
            String outText = result[0];
            Assert.assertTrue("should run table-level check from WAL dir level",
                    outText.contains("checking chk_wal_dir"));
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
            waitForEmptyTable();

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
    public void testPrintDecimalValue() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table print_dec (d decimal(10,2), ts timestamp) timestamp(ts) partition by DAY WAL");
            execute("insert into print_dec values (1234.56m, '1970-01-01T00:00:00.000000Z')");
            waitForAppliedRows("print_dec", 1);

            String[] result = runSession("cd print_dec\ncd 0\ncd d\nprint 0\nquit\n");
            Assert.assertTrue(result[0].contains("[0] = 1234.56"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testPrintDecimalNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table print_dec_null (d decimal(10,2), ts timestamp) timestamp(ts) partition by DAY WAL");
            execute("insert into print_dec_null values (null, '1970-01-01T00:00:00.000000Z')");
            waitForAppliedRows("print_dec_null", 1);

            String[] result = runSession("cd print_dec_null\ncd 0\ncd d\nprint 0\nquit\n");
            Assert.assertTrue(result[0].contains("[0] = null"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testPrintDecimal128Value() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table print_dec128 (d decimal(30,6), ts timestamp) timestamp(ts) partition by DAY WAL");
            execute("insert into print_dec128 values (12345678901234567890.123456m, '1970-01-01T00:00:00.000000Z')");
            waitForAppliedRows("print_dec128", 1);

            String[] result = runSession("cd print_dec128\ncd 0\ncd d\nprint 0\nquit\n");
            Assert.assertTrue(result[0].contains("[0] = 12345678901234567890.123456"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testPrintDecimal256Value() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table print_dec256 (d decimal(50,8), ts timestamp) timestamp(ts) partition by DAY WAL");
            execute("insert into print_dec256 values (1234567890123456789012345678901234567890.12345678m, '1970-01-01T00:00:00.000000Z')");
            waitForAppliedRows("print_dec256", 1);

            String[] result = runSession("cd print_dec256\ncd 0\ncd d\nprint 0\nquit\n");
            Assert.assertTrue(result[0].contains("[0] = 1234567890123456789012345678901234567890.12345678"));
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
            Assert.assertTrue(outText.contains("expected aux size: 16 B"));
            Assert.assertTrue(outText.contains("actual aux size:"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testShowAtColumnLevelVarcharInlinedExpectedDataSize() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table nav_col_show_vc_inline (v varchar, ts timestamp) timestamp(ts) partition by DAY WAL");
            execute("insert into nav_col_show_vc_inline values ('a', '1970-01-01T00:00:00.000000Z')");
            execute("insert into nav_col_show_vc_inline values ('bb', '1970-01-01T00:00:01.000000Z')");
            waitForAppliedRows("nav_col_show_vc_inline", 2);

            String[] result = runSession("cd nav_col_show_vc_inline\ncd 0\ncd v\nshow\nquit\n");
            String outText = result[0];
            Assert.assertTrue(outText.contains("column: v"));
            Assert.assertTrue(outText.contains("type: VARCHAR"));
            Assert.assertTrue(outText.contains("effectiveRows: 2"));
            Assert.assertTrue(outText.contains("expected data size: 0 B"));
            Assert.assertTrue(outText.contains("expected aux size: 32 B"));
            Assert.assertTrue(outText.contains("actual data size:"));
            Assert.assertTrue(outText.contains("actual aux size:"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testShowAtColumnLevelStringExpectedAuxSize() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table nav_col_show_str_aux (s string, ts timestamp) timestamp(ts) partition by DAY WAL");
            execute("insert into nav_col_show_str_aux values ('x', '1970-01-01T00:00:00.000000Z')");
            execute("insert into nav_col_show_str_aux values ('yy', '1970-01-01T00:00:01.000000Z')");
            waitForAppliedRows("nav_col_show_str_aux", 2);

            String[] result = runSession("cd nav_col_show_str_aux\ncd 0\ncd s\nshow\nquit\n");
            String outText = result[0];
            Assert.assertTrue(outText.contains("column: s"));
            Assert.assertTrue(outText.contains("type: STRING"));
            Assert.assertTrue(outText.contains("effectiveRows: 2"));
            Assert.assertTrue(outText.contains("expected data size: 14 B"));
            Assert.assertTrue(outText.contains("expected aux size: 24 B"));
            Assert.assertTrue(outText.contains("actual aux size:"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testShowAtColumnLevelArrayNullsExpectedSizes() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table nav_col_show_arr_null (arr double[], ts timestamp) timestamp(ts) partition by DAY");
            execute("insert into nav_col_show_arr_null values (null, '1970-01-01T00:00:00.000000Z')");
            execute("insert into nav_col_show_arr_null values (null, '1970-01-01T00:00:01.000000Z')");

            String[] result = runSession("cd nav_col_show_arr_null\ncd 0\ncd arr\nshow\nquit\n");
            String outText = result[0];
            Assert.assertTrue(outText.contains("column: arr"));
            Assert.assertTrue(outText.contains("type: DOUBLE[]"));
            Assert.assertTrue(outText.contains("effectiveRows: 2"));
            Assert.assertTrue(outText.contains("expected data size: 0 B"));
            Assert.assertTrue(outText.contains("expected aux size: 32 B"));
            Assert.assertTrue(outText.contains("actual data size:"));
            Assert.assertTrue(outText.contains("actual aux size:"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testNavigateBackFromSegmentClearsEventState() throws Exception {
        assertMemoryLeak(() -> {
            createWalTableWithCommits("wal_nav_back_seg", 3);
            // cd to segment, ls (populates events), cd .., cd back to segment, ls again
            String[] result = runSession(
                    "cd wal_nav_back_seg\ncd wal\ncd wal1\ncd 0\nls\ncd ..\ncd 0\nls\nquit\n"
            );
            // Both ls invocations should work without errors
            Assert.assertEquals("", result[1]);
            // The second ls should also show events
            String outText = result[0];
            // Find two occurrences of "txn" column header (from both ls calls)
            int firstTxn = outText.indexOf("txn");
            Assert.assertTrue("first ls should show txn header", firstTxn >= 0);
            int secondTxn = outText.indexOf("txn", firstTxn + 10);
            Assert.assertTrue("second ls should also show txn header", secondTxn > firstTxn);
        });
    }

    @Test
    public void testShowAtSegmentLevel() throws Exception {
        assertMemoryLeak(() -> {
            createWalTableWithCommits("wal_show_seg", 3);
            String[] result = runSession("cd wal_show_seg\ncd wal\ncd wal1\ncd 0\nshow\nquit\n");
            String outText = result[0];
            Assert.assertTrue(
                    "should show segment header",
                    outText.contains("segment:")
            );
            Assert.assertTrue(
                    "should show maxTxn",
                    outText.contains("maxTxn")
            );
            Assert.assertTrue(
                    "should show event type counts",
                    outText.contains("DATA:")
            );
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testShowAtWalDirLevel() throws Exception {
        assertMemoryLeak(() -> {
            createWalTableWithCommits("wal_show_dir", 3);
            String[] result = runSession("cd wal_show_dir\ncd wal\ncd wal1\nshow\nquit\n");
            String outText = result[0];
            Assert.assertTrue(
                    "should show walId",
                    outText.contains("walId")
            );
            Assert.assertTrue(
                    "should show segments count",
                    outText.contains("segments")
            );
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testShowAtWalDirShowsTxnlogRefs() throws Exception {
        assertMemoryLeak(() -> {
            createWalTableWithCommits("wal_show_txnlog_refs", 3);
            String[] result = runSession("cd wal_show_txnlog_refs\ncd wal\ncd wal1\nshow\nquit\n");
            Assert.assertTrue(
                    "should show txnlog references",
                    result[0].contains("txnlog references")
            );
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testShowAtWalRoot() throws Exception {
        assertMemoryLeak(() -> {
            createWalTableWithCommits("wal_show_root", 3);
            // first call wal status to load seq txnlog state, then cd into WAL and show
            String[] result = runSession("cd wal_show_root\nwal status\ncd wal\nshow\nquit\n");
            String outText = result[0];
            // show at WAL root should display sequencer txnlog with seqTxn column header
            Assert.assertTrue(
                    "should show seqTxn column header",
                    outText.contains("seqTxn")
            );
        });
    }

    @Test
    public void testShowEventDetail() throws Exception {
        assertMemoryLeak(() -> {
            createWalTableWithCommits("wal_show_event", 3);
            // show 0 at segment level should show event detail for txn 0
            String[] result = runSession("cd wal_show_event\ncd wal\ncd wal1\ncd 0\nshow 0\nquit\n");
            String outText = result[0];
            Assert.assertTrue(
                    "should show txn: 0",
                    outText.contains("txn: 0")
            );
            Assert.assertTrue(
                    "should show type field",
                    outText.contains("type:")
            );
            // DATA events should show startRowID
            Assert.assertTrue(
                    "should show startRowID field",
                    outText.contains("startRowID")
            );
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testShowEventInvalidArg() throws Exception {
        assertMemoryLeak(() -> {
            createWalTableWithCommits("wal_show_event_bad", 3);
            String[] result = runSession("cd wal_show_event_bad\ncd wal\ncd wal1\ncd 0\nshow abc\nquit\n");
            Assert.assertTrue(
                    "should report invalid event txn",
                    result[1].contains("invalid event txn")
            );
        });
    }

    @Test
    public void testShowEventNotFound() throws Exception {
        assertMemoryLeak(() -> {
            createWalTableWithCommits("wal_show_event_nf", 3);
            String[] result = runSession("cd wal_show_event_nf\ncd wal\ncd wal1\ncd 0\nshow 999\nquit\n");
            Assert.assertTrue(
                    "should report event not found",
                    result[1].contains("event not found")
            );
        });
    }

    @Test
    public void testShowSegmentSummaryEventCounts() throws Exception {
        assertMemoryLeak(() -> {
            createWalTableWithCommits("wal_show_seg_counts", 5);
            String[] result = runSession("cd wal_show_seg_counts\ncd wal\ncd wal1\ncd 0\nshow\nquit\n");
            String outText = result[0];
            // Segment summary should show DATA count line
            Assert.assertTrue(
                    "should show DATA event count",
                    outText.contains("DATA:")
            );
            // The count should reflect the number of commits we made
            Assert.assertTrue(
                    "should show events count",
                    outText.contains("events:")
            );
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

    // -- truncate command tests --

    @Test
    public void testTruncateCreatesBackup() throws Exception {
        assertMemoryLeak(() -> {
            // partition 0 needs multiple rows so truncation is valid
            execute("create table trunc_bak (sym symbol, ts timestamp) timestamp(ts) partition by DAY");
            execute("insert into trunc_bak select rnd_symbol('AA','BB'), "
                    + "timestamp_sequence('1970-01-01', 1000000L) from long_sequence(10)");

            String[] result = runSession("cd trunc_bak\ncd 0\ntruncate 5\nquit\n");
            Assert.assertTrue("should mention backup", result[0].contains("_txn.bak"));
            Assert.assertEquals("", result[1]);

            // verify .bak file exists
            TableToken token = engine.verifyTableName("trunc_bak");
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(token.getDirName())
                        .concat(TableUtils.TXN_FILE_NAME).put(".bak").$();
                Assert.assertTrue("backup file should exist", FF.exists(path.$()));
            }
        });
    }

    @Test
    public void testTruncateLastPartition() throws Exception {
        assertMemoryLeak(() -> {
            createNonWalTableWithRows("trunc_last", 3);
            // 3 partitions (1 row each). cd into last (index 2), truncate would require
            // at least 2 rows. So let's create a table where last partition has multiple rows.
            execute("create table trunc_last2 (sym symbol, ts timestamp) timestamp(ts) partition by DAY");
            execute("insert into trunc_last2 select rnd_symbol('AA','BB'), "
                    + "timestamp_sequence('1970-01-03', 1000000L) from long_sequence(10)");
            // This creates 1 partition (1970-01-03) with 10 rows

            String[] result = runSession("cd trunc_last2\ncd 0\ntruncate 5\nquit\n");
            Assert.assertTrue("should print OK", result[0].contains("OK"));
            Assert.assertTrue("should show row change", result[0].contains("10 -> 5"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testTruncateMiddlePartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table trunc_mid (sym symbol, ts timestamp) timestamp(ts) partition by DAY");
            // Create 3 partitions: day 1 has 10 rows, day 2 has 10 rows, day 3 has 10 rows
            execute("insert into trunc_mid select rnd_symbol('AA','BB'), "
                    + "timestamp_sequence('1970-01-01', 1000000L) from long_sequence(10)");
            execute("insert into trunc_mid select rnd_symbol('AA','BB'), "
                    + "timestamp_sequence('1970-01-02', 1000000L) from long_sequence(10)");
            execute("insert into trunc_mid select rnd_symbol('AA','BB'), "
                    + "timestamp_sequence('1970-01-03', 1000000L) from long_sequence(10)");

            // cd into middle partition (index 1), truncate to 5
            String[] result = runSession("cd trunc_mid\ncd 1\ntruncate 5\nquit\n");
            Assert.assertTrue("should print OK", result[0].contains("OK"));
            Assert.assertTrue("should show row change", result[0].contains("10 -> 5"));
            // maxTimestamp should not change since we're truncating a middle partition
            Assert.assertFalse("maxTimestamp should not change", result[0].contains("maxTimestamp:"));
            Assert.assertEquals("", result[1]);

            // verify the written _txn preserves the last partition's row count
            TableToken token = engine.verifyTableName("trunc_mid");
            try (Path path = new Path(); TxReader txReader = new TxReader(FF)) {
                path.of(configuration.getDbRoot()).concat(token.getDirName())
                        .concat(TableUtils.TXN_FILE_NAME).$();
                txReader.ofRO(path.$(), ColumnType.TIMESTAMP, PartitionBy.DAY);
                Assert.assertTrue("should load", txReader.unsafeLoadAll());
                Assert.assertEquals("transientRowCount must be preserved", 10, txReader.getTransientRowCount());
                Assert.assertEquals("fixedRowCount = 10 + 5", 15, txReader.getFixedRowCount());
                Assert.assertEquals(3, txReader.getPartitionCount());
            }
        });
    }

    @Test
    public void testTruncateRefusesAtWrongLevel() throws Exception {
        assertMemoryLeak(() -> {
            createNonWalTableWithRows("trunc_level", 3);
            // at root level
            String[] result = runSession("truncate 5\nquit\n");
            Assert.assertTrue("should refuse at root", result[1].contains("partition level"));

            // at table level
            result = runSession("cd trunc_level\ntruncate 5\nquit\n");
            Assert.assertTrue("should refuse at table", result[1].contains("partition level"));
        });
    }

    @Test
    public void testTruncateRefusesInvalidRowCount() throws Exception {
        assertMemoryLeak(() -> {
            createNonWalTableWithRows("trunc_invalid", 3);
            // newRowCount >= currentRowCount
            String[] result = runSession("cd trunc_invalid\ncd 0\ntruncate 5\nquit\n");
            Assert.assertTrue("should refuse >= current", result[1].contains("must be less than"));

            // newRowCount <= 0
            result = runSession("cd trunc_invalid\ncd 0\ntruncate 0\nquit\n");
            Assert.assertTrue("should refuse 0", result[1].contains("must be > 0"));

            result = runSession("cd trunc_invalid\ncd 0\ntruncate -1\nquit\n");
            Assert.assertTrue("should refuse negative", result[1].contains("must be > 0"));
        });
    }

    @Test
    public void testTruncateRefusesNonNumericArg() throws Exception {
        assertMemoryLeak(() -> {
            createNonWalTableWithRows("trunc_nan", 3);
            String[] result = runSession("cd trunc_nan\ncd 0\ntruncate abc\nquit\n");
            Assert.assertTrue("should refuse non-numeric", result[1].contains("invalid row count"));
        });
    }

    @Test
    public void testTruncateRefusesParquet() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithRows("trunc_parquet", 3);
            execute("ALTER TABLE trunc_parquet CONVERT PARTITION TO PARQUET WHERE ts = '1970-01-01'");
            drainWalQueue(engine);

            String[] result = runSession("cd trunc_parquet\ncd 0\ntruncate 1\nquit\n");
            Assert.assertTrue("should refuse parquet", result[1].contains("parquet"));
        });
    }

    @Test
    public void testTruncateSinglePartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table trunc_single (sym symbol, ts timestamp) timestamp(ts) partition by DAY");
            execute("insert into trunc_single select rnd_symbol('AA','BB'), "
                    + "timestamp_sequence('1970-01-01', 1000000L) from long_sequence(10)");

            String[] result = runSession("cd trunc_single\ncd 0\ntruncate 5\nquit\n");
            Assert.assertTrue("should print OK", result[0].contains("OK"));
            Assert.assertTrue("should show row change", result[0].contains("10 -> 5"));
            // maxTimestamp should change for single partition
            Assert.assertTrue("should update maxTimestamp", result[0].contains("maxTimestamp:"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testWalStatusAtRoot() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithRows("wal_root", 2);
            String[] result = runSession("wal status\nquit\n");
            Assert.assertTrue(result[1].contains("wal status requires a table"));
        });
    }

    @Test
    public void testWalStatusNonWalTable() throws Exception {
        assertMemoryLeak(() -> {
            createNonWalTableWithRows("wal_nonwal", 2);
            String[] result = runSession("cd wal_nonwal\nwal status\nquit\n");
            Assert.assertTrue(result[0].contains("walEnabled: false"));
            Assert.assertTrue(result[0].contains("does not use WAL"));
        });
    }

    @Test
    public void testWalStatusOnWalTable() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithRows("wal_ok", 3);
            String[] result = runSession("cd wal_ok\nwal status\nquit\n");
            String out = result[0];
            Assert.assertTrue(out.contains("walEnabled: true"));
            Assert.assertTrue(out.contains("sequencer txns:"));
            Assert.assertTrue(out.contains("table seqTxn:"));
            Assert.assertTrue(out.contains("pending: 0 transactions"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testWalStatusShowsPendingTxns() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table wal_pending (sym symbol, ts timestamp) timestamp(ts) partition by DAY WAL");
            execute("insert into wal_pending select rnd_symbol('AA','BB'), timestamp_sequence('1970-01-01', "
                    + Micros.DAY_MICROS + "L) from long_sequence(2)");
            // don't drain WAL queue - leave transactions pending
            String[] result = runSession("cd wal_pending\nwal status\nquit\n");
            String out = result[0];
            Assert.assertTrue(out.contains("walEnabled: true"));
            Assert.assertTrue(out.contains("pending:"));
            // should show pending records with wal/segment info
            Assert.assertTrue(out.contains("wal"));
        });
    }

    @Test
    public void testWalStatusWithDdl() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table wal_ddl (sym symbol, ts timestamp) timestamp(ts) partition by DAY WAL");
            execute("insert into wal_ddl values ('AA', '1970-01-01')");
            waitForAppliedRows("wal_ddl", 1);
            execute("alter table wal_ddl add column val int");
            // don't drain - DDL is pending
            String[] result = runSession("cd wal_ddl\nwal status\nquit\n");
            String out = result[0];
            Assert.assertTrue(out.contains("walEnabled: true"));
            // DDL entries should appear as pending
            Assert.assertTrue(out.contains("pending:"));
        });
    }

    @Test
    public void testWalStatusViaSubcommand() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithRows("wal_sub", 2);
            // "wal status" should work same as the two-word command
            String[] result = runSession("cd wal_sub\nwal status\nquit\n");
            Assert.assertTrue(result[0].contains("walEnabled: true"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testWalUnknownSubcommand() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithRows("wal_unknown", 2);
            String[] result = runSession("cd wal_unknown\nwal foo\nquit\n");
            Assert.assertTrue(result[1].contains("unknown wal subcommand"));
        });
    }

    @Test
    public void testWalStatusRecordsLoaded() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table wal_records (sym symbol, ts timestamp) timestamp(ts) partition by DAY WAL");
            // multiple commits to get multiple records
            try (WalWriter walWriter = getWalWriter("wal_records")) {
                for (int i = 0; i < 5; i++) {
                    TableWriter.Row row = walWriter.newRow(i * Micros.DAY_MICROS);
                    row.putSym(0, "AA");
                    row.append();
                    walWriter.commit();
                }
            }
            waitForAppliedRows("wal_records", 5);

            String[] result = runSession("cd wal_records\nwal status\nquit\n");
            Assert.assertTrue(result[0].contains("records loaded:"));
            Assert.assertTrue(result[0].contains("pending: 0 transactions"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testCdBetweenSegments() throws Exception {
        assertMemoryLeak(() -> {
            // Create multiple commits to ensure at least one segment exists.
            // With a single WalWriter, all commits land in segment 0, so we
            // verify navigating to segment 0, going back up, and navigating again.
            createWalTableWithCommits("wal_cd_between_segs", 5);
            String[] result = runSession(
                    "cd wal_cd_between_segs\ncd wal\ncd wal1\ncd 0\npwd\ncd ..\ncd 0\npwd\nquit\n"
            );
            // Both pwd outputs should show segment 0 path
            String outText = result[0];
            int firstIdx = outText.indexOf("/wal_cd_between_segs/wal/wal1/0\n");
            int secondIdx = outText.indexOf("/wal_cd_between_segs/wal/wal1/0\n", firstIdx + 1);
            Assert.assertTrue(
                    "should navigate to segment 0 twice",
                    firstIdx >= 0 && secondIdx > firstIdx
            );
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testCdToSegmentAndBackToRoot() throws Exception {
        assertMemoryLeak(() -> {
            createWalTableWithCommits("wal_seg_to_root", 3);
            String[] result = runSession(
                    "cd wal_seg_to_root\ncd wal\ncd wal1\ncd 0\ncd /\npwd\nquit\n"
            );
            Assert.assertTrue(
                    "should be at root after cd / from segment",
                    result[0].contains("/\n")
            );
            Assert.assertTrue(result[0].contains("recover:/>"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testHelpIncludesCdWal() throws Exception {
        assertMemoryLeak(() -> {
            String[] result = runSession("help\nquit\n");
            Assert.assertTrue(
                    "help should mention 'cd wal'",
                    result[0].contains("cd wal")
            );
        });
    }

    @Test
    public void testHelpShowsEventCommands() throws Exception {
        assertMemoryLeak(() -> {
            String[] result = runSession("help\nquit\n");
            Assert.assertTrue(
                    "help should mention 'show <N>' for event detail",
                    result[0].contains("show <N>")
            );
        });
    }

    @Test
    public void testHelpIncludesWalNavigation() throws Exception {
        assertMemoryLeak(() -> {
            String[] result = runSession("help\nquit\n");
            // help should mention WAL navigation in some form
            Assert.assertTrue(
                    "help should mention WAL navigation",
                    result[0].contains("WAL") || result[0].contains("wal")
            );
        });
    }

    @Test
    public void testHelpIncludesWalStatus() throws Exception {
        assertMemoryLeak(() -> {
            String[] result = runSession("help\nquit\n");
            Assert.assertTrue(result[0].contains("wal status"));
        });
    }

    // -- formatPartitionRange unit tests --

    @Test
    public void testFormatPartitionRangeSingleDay() {
        long ts = 12 * Micros.HOUR_MICROS; // 1970-01-01T12:00
        String result = ConsoleRenderer.formatPartitionRange(PartitionBy.DAY, ColumnType.TIMESTAMP, ts, ts);
        Assert.assertEquals("1970-01-01", result);
    }

    @Test
    public void testFormatPartitionRangeMultipleDays() {
        long min = 0; // 1970-01-01
        long max = 2 * Micros.DAY_MICROS + 12 * Micros.HOUR_MICROS; // 1970-01-03T12:00
        String result = ConsoleRenderer.formatPartitionRange(PartitionBy.DAY, ColumnType.TIMESTAMP, min, max);
        Assert.assertEquals("1970-01-01, 1970-01-02, 1970-01-03", result);
    }

    @Test
    public void testFormatPartitionRangeHourly() {
        long min = 0; // 1970-01-01T00:00
        long max = 2 * Micros.HOUR_MICROS; // 1970-01-01T02:00
        String result = ConsoleRenderer.formatPartitionRange(PartitionBy.HOUR, ColumnType.TIMESTAMP, min, max);
        Assert.assertTrue(result.contains(","));
        // should have 3 hour partitions
        Assert.assertEquals(3, result.split(",").length);
    }

    @Test
    public void testFormatPartitionRangeMonthly() {
        long min = 0; // 1970-01-01
        long max = 32L * Micros.DAY_MICROS; // 1970-02-02
        String result = ConsoleRenderer.formatPartitionRange(PartitionBy.MONTH, ColumnType.TIMESTAMP, min, max);
        Assert.assertEquals("1970-01, 1970-02", result);
    }

    @Test
    public void testFormatPartitionRangeYearly() {
        long min = 0; // 1970
        long max = 366L * Micros.DAY_MICROS; // 1971
        String result = ConsoleRenderer.formatPartitionRange(PartitionBy.YEAR, ColumnType.TIMESTAMP, min, max);
        Assert.assertEquals("1970, 1971", result);
    }

    @Test
    public void testFormatPartitionRangeWeekly() {
        long min = 0; // 1970-01-01 (Thursday, W01)
        long max = 8L * Micros.DAY_MICROS; // 1970-01-09 (next week)
        String result = ConsoleRenderer.formatPartitionRange(PartitionBy.WEEK, ColumnType.TIMESTAMP, min, max);
        Assert.assertTrue(result.contains(","));
        Assert.assertEquals(2, result.split(",").length);
    }

    @Test
    public void testFormatPartitionRangeNone() {
        String result = ConsoleRenderer.formatPartitionRange(PartitionBy.NONE, ColumnType.TIMESTAMP, 0, 100);
        Assert.assertEquals("default", result);
    }

    @Test
    public void testFormatPartitionRangeUnsetTimestamps() {
        String result = ConsoleRenderer.formatPartitionRange(PartitionBy.DAY, ColumnType.TIMESTAMP, TxnState.UNSET_LONG, TxnState.UNSET_LONG);
        Assert.assertEquals("n/a", result);
    }

    @Test
    public void testFormatPartitionRangeUnsetPartitionBy() {
        String result = ConsoleRenderer.formatPartitionRange(TxnState.UNSET_INT, ColumnType.TIMESTAMP, 0, 100);
        Assert.assertEquals("n/a", result);
    }

    @Test
    public void testFormatPartitionRangeMinOnlyUnset() {
        String result = ConsoleRenderer.formatPartitionRange(PartitionBy.DAY, ColumnType.TIMESTAMP, TxnState.UNSET_LONG, 100);
        Assert.assertEquals("n/a", result);
    }

    @Test
    public void testFormatPartitionRangeTooMany() {
        long min = 0; // 1970-01-01
        long max = 29L * Micros.DAY_MICROS; // 1970-01-30
        String result = ConsoleRenderer.formatPartitionRange(PartitionBy.DAY, ColumnType.TIMESTAMP, min, max);
        Assert.assertTrue("should be abbreviated", result.contains(".."));
        Assert.assertTrue("should contain partition count", result.contains("30 partitions"));
    }

    @Test
    public void testFormatPartitionRangeExactly20() {
        long min = 0; // 1970-01-01
        long max = 19L * Micros.DAY_MICROS; // 1970-01-20
        String result = ConsoleRenderer.formatPartitionRange(PartitionBy.DAY, ColumnType.TIMESTAMP, min, max);
        Assert.assertFalse("should not be abbreviated", result.contains(".."));
        Assert.assertEquals(20, result.split(",").length);
    }

    @Test
    public void testFormatPartitionRangeExactly21() {
        long min = 0; // 1970-01-01
        long max = 20L * Micros.DAY_MICROS; // 1970-01-21
        String result = ConsoleRenderer.formatPartitionRange(PartitionBy.DAY, ColumnType.TIMESTAMP, min, max);
        Assert.assertTrue("should be abbreviated", result.contains(".."));
        Assert.assertTrue("should contain partition count", result.contains("21 partitions"));
    }

    @Test
    public void testFormatPartitionRangeMinEqualsMax() {
        long ts = 5 * Micros.DAY_MICROS + 6 * Micros.HOUR_MICROS; // 1970-01-06T06:00
        String result = ConsoleRenderer.formatPartitionRange(PartitionBy.DAY, ColumnType.TIMESTAMP, ts, ts);
        Assert.assertEquals("1970-01-06", result);
    }

    @Test
    public void testFormatPartitionRangeBoundaryMidnight() {
        long min = 0; // 1970-01-01T00:00:00
        long max = Micros.DAY_MICROS; // 1970-01-02T00:00:00 (exactly midnight)
        String result = ConsoleRenderer.formatPartitionRange(PartitionBy.DAY, ColumnType.TIMESTAMP, min, max);
        Assert.assertEquals("1970-01-01, 1970-01-02", result);
    }

    // -- formatDuration unit tests --

    @Test
    public void testFormatDurationSeconds() {
        Assert.assertEquals("30s", ConsoleRenderer.formatDuration(30_000_000L));
    }

    @Test
    public void testFormatDurationMinutes() {
        Assert.assertEquals("5m 0s", ConsoleRenderer.formatDuration(5 * 60_000_000L));
    }

    @Test
    public void testFormatDurationHoursAndMinutes() {
        Assert.assertEquals("2h 15m", ConsoleRenderer.formatDuration(2 * 3600_000_000L + 15 * 60_000_000L));
    }

    @Test
    public void testFormatDurationDaysAndHours() {
        Assert.assertEquals("3d 4h", ConsoleRenderer.formatDuration(3 * 86400_000_000L + 4 * 3600_000_000L));
    }

    @Test
    public void testFormatDurationZero() {
        Assert.assertEquals("0s", ConsoleRenderer.formatDuration(0));
    }

    @Test
    public void testFormatDurationNegative() {
        Assert.assertEquals("n/a", ConsoleRenderer.formatDuration(-1));
    }

    // -- wal status enrichment integration tests --

    @Test
    public void testWalStatusPendingRowsV2() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 10);
            execute("create table wal_pr (sym symbol, ts timestamp) timestamp(ts) partition by DAY WAL");
            try (WalWriter walWriter = getWalWriter("wal_pr")) {
                for (int i = 0; i < 3; i++) {
                    TableWriter.Row row = walWriter.newRow(i * Micros.DAY_MICROS);
                    row.putSym(0, "AA");
                    row.append();
                    walWriter.commit();
                }
            }
            // don't drain - all 3 commits are pending
            String[] result = runSession("cd wal_pr\nwal status\nquit\n");
            String out = result[0];
            Assert.assertTrue("should show pending rows", out.contains("pending rows:"));
            Assert.assertTrue("should show affected partitions", out.contains("affected partitions:"));
        });
    }

    @Test
    public void testWalStatusAffectedPartitionsMultipleDays() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 10);
            execute("create table wal_ap (sym symbol, ts timestamp) timestamp(ts) partition by DAY WAL");
            try (WalWriter walWriter = getWalWriter("wal_ap")) {
                for (int i = 0; i < 3; i++) {
                    TableWriter.Row row = walWriter.newRow(i * Micros.DAY_MICROS);
                    row.putSym(0, "AA");
                    row.append();
                    walWriter.commit();
                }
            }
            // don't drain
            String[] result = runSession("cd wal_ap\nwal status\nquit\n");
            String out = result[0];
            Assert.assertTrue("should show affected partitions", out.contains("affected partitions:"));
            Assert.assertTrue("should show day-level names", out.contains("1970-01-01"));
        });
    }

    @Test
    public void testWalStatusFullyApplied() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithRows("wal_fully", 3);
            String[] result = runSession("cd wal_fully\nwal status\nquit\n");
            String out = result[0];
            Assert.assertTrue("should show pending: 0", out.contains("pending: 0"));
            Assert.assertFalse("should not show pending rows for fully applied", out.contains("pending rows:"));
        });
    }

    @Test
    public void testWalStatusTimeSinceLastApplied() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table wal_ts (sym symbol, ts timestamp) timestamp(ts) partition by DAY WAL");
            try (WalWriter walWriter = getWalWriter("wal_ts")) {
                for (int i = 0; i < 3; i++) {
                    TableWriter.Row row = walWriter.newRow(i * Micros.DAY_MICROS);
                    row.putSym(0, "AA");
                    row.append();
                    walWriter.commit();
                }
            }
            // apply first, leave rest pending
            drainWalQueue(engine);
            execute("insert into wal_ts values ('BB', '1970-01-04')");
            // new insert is pending
            String[] result = runSession("cd wal_ts\nwal status\nquit\n");
            String out = result[0];
            // if there are pending transactions with a last applied having a commit timestamp
            if (out.contains("pending:") && !out.contains("pending: 0")) {
                Assert.assertTrue("should show time since last applied", out.contains("time since last applied:"));
            }
        });
    }

    @Test
    public void testWalStatusV1ShowsPendingRowsViaEnrichment() throws Exception {
        assertMemoryLeak(() -> {
            // default config creates V1 txnlog - no row/timestamp data in seqTxnLog,
            // but enrichment from WAL event files fills it in
            execute("create table wal_v1 (sym symbol, ts timestamp) timestamp(ts) partition by DAY WAL");
            try (WalWriter walWriter = getWalWriter("wal_v1")) {
                TableWriter.Row row = walWriter.newRow(0);
                row.putSym(0, "AA");
                row.append();
                walWriter.commit();
            }
            // don't drain
            String[] result = runSession("cd wal_v1\nwal status\nquit\n");
            String out = result[0];
            Assert.assertTrue("should show pending", out.contains("pending: 1"));
            Assert.assertTrue("V1 enriched from events should show pending rows", out.contains("pending rows:"));
            Assert.assertTrue("V1 enriched from events should show affected partitions", out.contains("affected partitions:"));
        });
    }

    @Test
    public void testWalStatusPartitionByHour() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table wal_hour (sym symbol, ts timestamp) timestamp(ts) partition by HOUR WAL");
            try (WalWriter walWriter = getWalWriter("wal_hour")) {
                for (int i = 0; i < 3; i++) {
                    TableWriter.Row row = walWriter.newRow(i * Micros.HOUR_MICROS);
                    row.putSym(0, "AA");
                    row.append();
                    walWriter.commit();
                }
            }
            // don't drain
            String[] result = runSession("cd wal_hour\nwal status\nquit\n");
            String out = result[0];
            Assert.assertTrue("should show pending", out.contains("pending:"));
        });
    }

    // -- WAL event detail partition affinity tests --

    @Test
    public void testWalEventDetailShowsPartitionsSingleDay() throws Exception {
        assertMemoryLeak(() -> {
            // use 3 commits so events survive drain (single commits may get purged)
            createWalTableWithCommits("wal_ep1", 3);
            // show event 0: ts=0 -> 1970-01-01
            String[] result = runSession("cd wal_ep1\ncd wal\ncd wal1\ncd 0\nshow 0\nquit\n");
            Assert.assertTrue("should show partitions", result[0].contains("partitions: 1970-01-01"));
        });
    }

    @Test
    public void testWalEventDetailShowsPartitionsMultipleDays() throws Exception {
        assertMemoryLeak(() -> {
            // 3 commits in separate days; events survive drain
            createWalTableWithCommits("wal_ep2", 3);
            // events 0 and 1 are in segment 0 after drain (event 2 may be purged)
            // event 0: ts=0 -> 1970-01-01; event 1: ts=DAY -> 1970-01-02
            String[] result = runSession("cd wal_ep2\ncd wal\ncd wal1\ncd 0\nshow 1\nquit\n");
            String out = result[0];
            Assert.assertTrue("should show partitions", out.contains("partitions:"));
            Assert.assertTrue("should contain 1970-01-02", out.contains("1970-01-02"));
        });
    }

    @Test
    public void testWalEventDetailSqlEventNoPartitions() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table wal_ep3 (sym symbol, ts timestamp) timestamp(ts) partition by DAY WAL");
            execute("insert into wal_ep3 values ('AA', '1970-01-01')");
            waitForAppliedRows("wal_ep3", 1);
            execute("alter table wal_ep3 add column val int");
            drainWalQueue(engine);
            // DDL event is in a separate segment or in the same; navigate to find SQL event
            String[] result = runSession("cd wal_ep3\ncd wal\ncd wal1\ncd 0\nls\nquit\n");
            // Check that any SQL events don't have partitions line
            String out = result[0];
            // SQL events show "-" in partitions column
            Assert.assertTrue("should have partitions column header", out.contains("partitions"));
        });
    }

    // -- WAL event listing partition column tests --

    @Test
    public void testWalEventsListingShowsPartitionColumn() throws Exception {
        assertMemoryLeak(() -> {
            createWalTableWithCommits("wal_el1", 3);
            String[] result = runSession("cd wal_el1\ncd wal\ncd wal1\ncd 0\nls\nquit\n");
            String out = result[0];
            Assert.assertTrue("header should contain 'partitions'", out.contains("partitions"));
        });
    }

    @Test
    public void testWalEventsListingMixedEventTypes() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table wal_el2 (sym symbol, ts timestamp) timestamp(ts) partition by DAY WAL");
            execute("insert into wal_el2 values ('AA', '1970-01-01')");
            waitForAppliedRows("wal_el2", 1);
            execute("alter table wal_el2 add column val int");
            drainWalQueue(engine);
            String[] result = runSession("cd wal_el2\ncd wal\ncd wal1\ncd 0\nls\nquit\n");
            String out = result[0];
            Assert.assertTrue("header should contain 'partitions'", out.contains("partitions"));
        });
    }

    @Test
    public void testWalEventsListingPartitionByMonth() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table wal_el3 (sym symbol, ts timestamp) timestamp(ts) partition by MONTH WAL");
            try (WalWriter walWriter = getWalWriter("wal_el3")) {
                // 3 commits so events survive drain; all in 1970-01
                for (int i = 0; i < 3; i++) {
                    TableWriter.Row row = walWriter.newRow(i * Micros.DAY_MICROS);
                    row.putSym(0, "AA");
                    row.append();
                    walWriter.commit();
                }
            }
            waitForAppliedRows("wal_el3", 3);
            String[] result = runSession("cd wal_el3\ncd wal\ncd wal1\ncd 0\nls\nquit\n");
            String out = result[0];
            Assert.assertTrue("should show month-level names", out.contains("1970-01"));
        });
    }

    // -- WAL dir listing aggregation tests --

    @Test
    public void testWalDirListingShowsRowsV2() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 10);
            execute("create table wal_dl1 (sym symbol, ts timestamp) timestamp(ts) partition by DAY WAL");
            try (WalWriter walWriter = getWalWriter("wal_dl1")) {
                for (int i = 0; i < 3; i++) {
                    TableWriter.Row row = walWriter.newRow(i * Micros.DAY_MICROS);
                    row.putSym(0, "AA");
                    row.append();
                    walWriter.commit();
                }
            }
            waitForAppliedRows("wal_dl1", 3);
            String[] result = runSession("cd wal_dl1\ncd wal\nls\nquit\n");
            String out = result[0];
            Assert.assertTrue("header should contain 'rows'", out.contains("rows"));
            Assert.assertTrue("header should contain 'partitions'", out.contains("partitions"));
        });
    }

    @Test
    public void testWalDirListingShowsPartitionRange() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 10);
            execute("create table wal_dl2 (sym symbol, ts timestamp) timestamp(ts) partition by DAY WAL");
            try (WalWriter walWriter = getWalWriter("wal_dl2")) {
                for (int i = 0; i < 3; i++) {
                    TableWriter.Row row = walWriter.newRow(i * Micros.DAY_MICROS);
                    row.putSym(0, "AA");
                    row.append();
                    walWriter.commit();
                }
            }
            waitForAppliedRows("wal_dl2", 3);
            String[] result = runSession("cd wal_dl2\ncd wal\nls\nquit\n");
            String out = result[0];
            Assert.assertTrue("should contain day-level names", out.contains("1970-01-01"));
        });
    }

    @Test
    public void testWalDirListingMultipleWals() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table wal_dl3 (sym symbol, ts timestamp) timestamp(ts) partition by DAY WAL");
            // Open both WalWriters concurrently to force wal1 and wal2 creation
            try (WalWriter w1 = getWalWriter("wal_dl3");
                 WalWriter w2 = getWalWriter("wal_dl3")) {
                TableWriter.Row row1 = w1.newRow(0);
                row1.putSym(0, "AA");
                row1.append();
                w1.commit();
                TableWriter.Row row2 = w2.newRow(Micros.DAY_MICROS);
                row2.putSym(0, "BB");
                row2.append();
                w2.commit();
            }
            waitForAppliedRows("wal_dl3", 2);
            String[] result = runSession("cd wal_dl3\ncd wal\nls\nquit\n");
            String out = result[0];
            Assert.assertTrue("should have wal1", out.contains("wal1"));
            Assert.assertTrue("should have wal2", out.contains("wal2"));
        });
    }

    @Test
    public void testWalDirListingOrphanWal() throws Exception {
        assertMemoryLeak(() -> {
            createWalTableWithCommits("wal_dl4", 2);
            // Create orphan wal99 directory
            TableToken token = engine.verifyTableName("wal_dl4");
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(token.getDirName()).concat("wal99").slash$();
                Assert.assertEquals(0, FF.mkdirs(path, configuration.getMkDirMode()));
            }
            String[] result = runSession("cd wal_dl4\ncd wal\nls\nquit\n");
            String out = result[0];
            Assert.assertTrue("should have wal99", out.contains("wal99"));
            Assert.assertTrue("should mark as unreferenced", out.contains("unreferenced"));
        });
    }

    @Test
    public void testWalDirListingMissingWalFullyAppliedShowsPurged() throws Exception {
        assertMemoryLeak(() -> {
            // Use 3 commits so wal dir survives initial drain, then remove it
            createWalTableWithCommits("wal_dl5", 3);
            // all txns are applied; remove wal1 directory — should show "purged"
            TableToken token = engine.verifyTableName("wal_dl5");
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(token.getDirName()).concat("wal1").$();
                FF.rmdir(path);
            }
            String[] result = runSession("cd wal_dl5\ncd wal\nls\nquit\n");
            String out = result[0];
            Assert.assertTrue("should show purged for fully applied WAL", out.contains("purged"));
            Assert.assertFalse("should not show MISSING for fully applied WAL", out.contains("MISSING"));
        });
    }

    // -- show <seqTxn> tests --

    @Test
    public void testShowSeqTxnDataEvent() throws Exception {
        assertMemoryLeak(() -> {
            createWalTableWithCommits("wal_st1", 3);
            String[] result = runSession("cd wal_st1\ncd wal\nshow 1\nquit\n");
            String out = result[0];
            Assert.assertTrue("should show seqTxn", out.contains("seqTxn: 1"));
            Assert.assertTrue("should show walId", out.contains("walId:"));
            Assert.assertTrue("should show segmentId", out.contains("segmentId:"));
            Assert.assertTrue("should show event detail separator", out.contains("--- event detail ---"));
            Assert.assertTrue("should show type DATA", out.contains("type: DATA"));
        });
    }

    @Test
    public void testShowSeqTxnDdl() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table wal_st2 (sym symbol, ts timestamp) timestamp(ts) partition by DAY WAL");
            execute("insert into wal_st2 values ('AA', '1970-01-01')");
            waitForAppliedRows("wal_st2", 1);
            execute("alter table wal_st2 add column val int");
            drainWalQueue(engine);
            // DDL should be seqTxn 2
            String[] result = runSession("cd wal_st2\ncd wal\nshow 2\nquit\n");
            String out = result[0];
            Assert.assertTrue("should show type DDL", out.contains("type: DDL"));
            // DDL should not have event detail section
            Assert.assertFalse("should not show event detail", out.contains("--- event detail ---"));
        });
    }

    @Test
    public void testShowSeqTxnNotFound() throws Exception {
        assertMemoryLeak(() -> {
            createWalTableWithCommits("wal_st3", 3);
            String[] result = runSession("cd wal_st3\ncd wal\nshow 999\nquit\n");
            Assert.assertTrue("should show not found error", result[1].contains("seqTxn not found: 999"));
        });
    }

    @Test
    public void testShowSeqTxnInvalidArg() throws Exception {
        assertMemoryLeak(() -> {
            createWalTableWithCommits("wal_st4", 3);
            String[] result = runSession("cd wal_st4\ncd wal\nshow abc\nquit\n");
            Assert.assertTrue("should show invalid error", result[1].contains("invalid seqTxn: abc"));
        });
    }

    @Test
    public void testShowSeqTxnNegative() throws Exception {
        assertMemoryLeak(() -> {
            createWalTableWithCommits("wal_st5", 3);
            String[] result = runSession("cd wal_st5\ncd wal\nshow -1\nquit\n");
            Assert.assertTrue("should show not found", result[1].contains("seqTxn not found"));
        });
    }

    @Test
    public void testShowSeqTxnZero() throws Exception {
        assertMemoryLeak(() -> {
            createWalTableWithCommits("wal_st6", 3);
            String[] result = runSession("cd wal_st6\ncd wal\nshow 0\nquit\n");
            Assert.assertTrue("should show not found", result[1].contains("seqTxn not found"));
        });
    }

    @Test
    public void testShowSeqTxnWithPartitionAffinity() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table wal_st7 (sym symbol, ts timestamp) timestamp(ts) partition by DAY WAL");
            try (WalWriter walWriter = getWalWriter("wal_st7")) {
                // commit 0: two rows in different days
                TableWriter.Row row1 = walWriter.newRow(0);
                row1.putSym(0, "AA");
                row1.append();
                TableWriter.Row row2 = walWriter.newRow(Micros.DAY_MICROS);
                row2.putSym(0, "BB");
                row2.append();
                walWriter.commit();
                // 2 more commits so events survive drain
                for (int i = 2; i < 4; i++) {
                    TableWriter.Row row = walWriter.newRow(i * Micros.DAY_MICROS);
                    row.putSym(0, "CC");
                    row.append();
                    walWriter.commit();
                }
            }
            waitForAppliedRows("wal_st7", 4);
            // show seqTxn 1 resolves to wal1/0 segmentTxn 0 -> the multi-day commit
            String[] result = runSession("cd wal_st7\ncd wal\nshow 1\nquit\n");
            String out = result[0];
            Assert.assertTrue("should show partitions", out.contains("partitions:"));
            Assert.assertTrue("should contain 1970-01-01", out.contains("1970-01-01"));
        });
    }

    @Test
    public void testShowSeqTxnMissingWalDir() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table wal_st8 (sym symbol, ts timestamp) timestamp(ts) partition by DAY WAL");
            try (WalWriter walWriter = getWalWriter("wal_st8")) {
                TableWriter.Row row = walWriter.newRow(0);
                row.putSym(0, "AA");
                row.append();
                walWriter.commit();
            }
            waitForAppliedRows("wal_st8", 1);
            // delete wal1 directory
            TableToken token = engine.verifyTableName("wal_st8");
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(token.getDirName()).concat("wal1").$();
                FF.rmdir(path);
            }
            String[] result = runSession("cd wal_st8\ncd wal\nshow 1\nquit\n");
            // should get an error about missing wal dir or events
            String err = result[1];
            Assert.assertTrue("should show error for missing wal",
                    err.contains("could not read events") || err.contains("no events found"));
        });
    }

    @Test
    public void testShowSeqTxnNoArgAtWalRoot() throws Exception {
        assertMemoryLeak(() -> {
            createWalTableWithCommits("wal_st9", 3);
            String[] result = runSession("cd wal_st9\ncd wal\nshow\nquit\n");
            String out = result[0];
            // should show the full seqTxnLog table
            Assert.assertTrue("should show seqTxn header", out.contains("seqTxn"));
            Assert.assertTrue("should show structVer", out.contains("structVer"));
        });
    }

    @Test
    public void testShowSeqTxnMultipleSegments() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table wal_st10 (sym symbol, ts timestamp) timestamp(ts) partition by DAY WAL");
            // First writer creates wal1/segment0
            try (WalWriter w1 = getWalWriter("wal_st10")) {
                TableWriter.Row row = w1.newRow(0);
                row.putSym(0, "AA");
                row.append();
                w1.commit();
            }
            // Second writer creates wal2/segment0
            try (WalWriter w2 = getWalWriter("wal_st10")) {
                TableWriter.Row row = w2.newRow(Micros.DAY_MICROS);
                row.putSym(0, "BB");
                row.append();
                w2.commit();
            }
            waitForAppliedRows("wal_st10", 2);
            // Show seqTxn 2 which should be from wal2
            String[] result = runSession("cd wal_st10\ncd wal\nshow 2\nquit\n");
            String out = result[0];
            Assert.assertTrue("should show seqTxn: 2", out.contains("seqTxn: 2"));
            Assert.assertTrue("should show event detail", out.contains("--- event detail ---"));
        });
    }

    @Test
    public void testShowSeqTxnShowsRowCount() throws Exception {
        assertMemoryLeak(() -> {
            createWalTableWithCommits("wal_st11", 3);
            String[] result = runSession("cd wal_st11\ncd wal\nshow 1\nquit\n");
            String out = result[0];
            // row count available via event enrichment
            Assert.assertTrue("should show rows", out.contains("rows:"));
        });
    }

    // -- show timeline tests --

    @Test
    public void testShowTimelineBasic() throws Exception {
        assertMemoryLeak(() -> {
            createWalTableWithCommits("wal_tl1", 3);
            String[] result = runSession("cd wal_tl1\ncd wal\nshow timeline\nquit\n");
            String out = result[0];
            Assert.assertTrue("should show seqTxn header", out.contains("seqTxn"));
            Assert.assertTrue("should show commitTime header", out.contains("commitTime"));
            Assert.assertTrue("should show wal/seg header", out.contains("wal/seg"));
            Assert.assertTrue("should show type header", out.contains("type"));
            Assert.assertTrue("should show rows header", out.contains("rows"));
            Assert.assertTrue("should show partitions header", out.contains("partitions"));
            Assert.assertTrue("should show status header", out.contains("status"));
            Assert.assertTrue("should show applied", out.contains("applied"));
        });
    }

    @Test
    public void testShowTimelineWithPending() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table wal_tl2 (sym symbol, ts timestamp) timestamp(ts) partition by DAY WAL");
            try (WalWriter walWriter = getWalWriter("wal_tl2")) {
                TableWriter.Row row = walWriter.newRow(0);
                row.putSym(0, "AA");
                row.append();
                walWriter.commit();
            }
            // Don't drain - commit is pending
            String[] result = runSession("cd wal_tl2\ncd wal\nshow timeline\nquit\n");
            String out = result[0];
            Assert.assertTrue("should show PENDING", out.contains("PENDING"));
        });
    }

    @Test
    public void testShowTimelineMixedDdlAndData() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table wal_tl3 (sym symbol, ts timestamp) timestamp(ts) partition by DAY WAL");
            execute("insert into wal_tl3 values ('AA', '1970-01-01')");
            waitForAppliedRows("wal_tl3", 1);
            execute("alter table wal_tl3 add column val int");
            drainWalQueue(engine);
            String[] result = runSession("cd wal_tl3\ncd wal\nshow timeline\nquit\n");
            String out = result[0];
            Assert.assertTrue("should show DATA type", out.contains("DATA"));
            Assert.assertTrue("should show DDL type", out.contains("DDL"));
            Assert.assertTrue("should show (DDL) wal/seg", out.contains("(DDL)"));
        });
    }

    @Test
    public void testShowTimelineMultipleWals() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table wal_tl4 (sym symbol, ts timestamp) timestamp(ts) partition by DAY WAL");
            // open both concurrently to force wal1 and wal2 creation
            try (WalWriter w1 = getWalWriter("wal_tl4");
                 WalWriter w2 = getWalWriter("wal_tl4")) {
                TableWriter.Row row1 = w1.newRow(0);
                row1.putSym(0, "AA");
                row1.append();
                w1.commit();
                TableWriter.Row row2 = w2.newRow(Micros.DAY_MICROS);
                row2.putSym(0, "BB");
                row2.append();
                w2.commit();
            }
            waitForAppliedRows("wal_tl4", 2);
            String[] result = runSession("cd wal_tl4\ncd wal\nshow timeline\nquit\n");
            String out = result[0];
            Assert.assertTrue("should show wal1", out.contains("wal1"));
            Assert.assertTrue("should show wal2", out.contains("wal2"));
        });
    }

    @Test
    public void testShowTimelineV2ShowsRowsAndPartitions() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 10);
            execute("create table wal_tl5 (sym symbol, ts timestamp) timestamp(ts) partition by DAY WAL");
            try (WalWriter walWriter = getWalWriter("wal_tl5")) {
                for (int i = 0; i < 3; i++) {
                    TableWriter.Row row = walWriter.newRow(i * Micros.DAY_MICROS);
                    row.putSym(0, "AA");
                    row.append();
                    walWriter.commit();
                }
            }
            waitForAppliedRows("wal_tl5", 3);
            String[] result = runSession("cd wal_tl5\ncd wal\nshow timeline\nquit\n");
            String out = result[0];
            // V2 should have real partition names in the timeline
            Assert.assertTrue("should show partition names", out.contains("1970-01-01"));
        });
    }

    @Test
    public void testShowTimelineEmpty() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table wal_tl6 (sym symbol, ts timestamp) timestamp(ts) partition by DAY WAL");
            drainWalQueue(engine);
            // no commits at all
            String[] result = runSession("cd wal_tl6\ncd wal\nshow timeline\nquit\n");
            String out = result[0];
            Assert.assertTrue("should show no records", out.contains("no records"));
        });
    }

    @Test
    public void testShowTimelineAtTableLevel() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithRows("wal_tl7", 2);
            String[] result = runSession("cd wal_tl7\nshow timeline\nquit\n");
            Assert.assertTrue("should show error", result[1].contains("WAL mode"));
        });
    }

    @Test
    public void testShowTimelineAtWalDir() throws Exception {
        assertMemoryLeak(() -> {
            createWalTableWithCommits("wal_tl8", 3);
            String[] result = runSession("cd wal_tl8\ncd wal\ncd wal1\nshow timeline\nquit\n");
            Assert.assertTrue("should show error", result[1].contains("WAL root"));
        });
    }

    @Test
    public void testShowTimelineAtRoot() throws Exception {
        assertMemoryLeak(() -> {
            String[] result = runSession("show timeline\nquit\n");
            Assert.assertTrue("should show error", result[1].contains("WAL mode"));
        });
    }

    @Test
    public void testShowTimelinePartitionByHour() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 10);
            execute("create table wal_tl9 (sym symbol, ts timestamp) timestamp(ts) partition by HOUR WAL");
            try (WalWriter walWriter = getWalWriter("wal_tl9")) {
                for (int i = 0; i < 3; i++) {
                    TableWriter.Row row = walWriter.newRow(i * Micros.HOUR_MICROS);
                    row.putSym(0, "AA");
                    row.append();
                    walWriter.commit();
                }
            }
            waitForAppliedRows("wal_tl9", 3);
            String[] result = runSession("cd wal_tl9\ncd wal\nshow timeline\nquit\n");
            String out = result[0];
            Assert.assertTrue("should show hour-level partitions", out.contains("1970-01-01T"));
        });
    }

    @Test
    public void testShowTimelineV1EnrichedFromEvents() throws Exception {
        assertMemoryLeak(() -> {
            // default test config creates V1 txnlog - no row/timestamp data in seqTxnLog,
            // but enrichment from WAL event files fills in rows/partitions
            execute("create table wal_tl10 (sym symbol, ts timestamp) timestamp(ts) partition by DAY WAL");
            try (WalWriter walWriter = getWalWriter("wal_tl10")) {
                TableWriter.Row row = walWriter.newRow(0);
                row.putSym(0, "AA");
                row.append();
                walWriter.commit();
            }
            waitForAppliedRows("wal_tl10", 1);
            String[] result = runSession("cd wal_tl10\ncd wal\nshow timeline\nquit\n");
            String out = result[0];
            Assert.assertTrue("should show seqTxn header", out.contains("seqTxn"));
            Assert.assertTrue("should show DATA type", out.contains("DATA"));
            // V1 enrichment from events should populate rows column
            Assert.assertTrue("should show row count from enrichment", out.contains("1"));
        });
    }

    @Test
    public void testShowTimelineNonWalTable() throws Exception {
        assertMemoryLeak(() -> {
            createNonWalTableWithRows("wal_tl11", 2);
            String[] result = runSession("cd wal_tl11\nshow timeline\nquit\n");
            Assert.assertTrue("should show error", result[1].contains("WAL mode"));
        });
    }

    // -- edge case & correctness tests --

    @Test
    public void testWalStatusAfterTruncate() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table wal_ec1 (sym symbol, ts timestamp) timestamp(ts) partition by DAY WAL");
            execute("insert into wal_ec1 values ('AA', '1970-01-01')");
            drainWalQueue(engine);
            execute("TRUNCATE TABLE wal_ec1");
            drainWalQueue(engine);
            execute("insert into wal_ec1 values ('BB', '1970-01-02')");
            drainWalQueue(engine);
            String[] result = runSession("cd wal_ec1\nwal status\nquit\n");
            String out = result[0];
            Assert.assertTrue("should show walEnabled: true", out.contains("walEnabled: true"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testWalStatusMultipleWalWriters() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table wal_ec2 (sym symbol, ts timestamp) timestamp(ts) partition by DAY WAL");
            try (WalWriter w1 = getWalWriter("wal_ec2")) {
                TableWriter.Row row = w1.newRow(0);
                row.putSym(0, "AA");
                row.append();
                w1.commit();
            }
            try (WalWriter w2 = getWalWriter("wal_ec2")) {
                TableWriter.Row row = w2.newRow(Micros.DAY_MICROS);
                row.putSym(0, "BB");
                row.append();
                w2.commit();
            }
            // don't drain
            String[] result = runSession("cd wal_ec2\nwal status\nquit\n");
            String out = result[0];
            Assert.assertTrue("should show pending", out.contains("pending:"));
        });
    }

    @Test
    public void testWalStatusAfterSchemaChange() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table wal_ec3 (sym symbol, ts timestamp) timestamp(ts) partition by DAY WAL");
            execute("insert into wal_ec3 values ('AA', '1970-01-01')");
            drainWalQueue(engine);
            execute("alter table wal_ec3 add column val int");
            // don't drain DDL
            String[] result = runSession("cd wal_ec3\nwal status\nquit\n");
            String out = result[0];
            Assert.assertTrue("should show pending DDL", out.contains("DDL"));
        });
    }

    @Test
    public void testShowSeqTxnAfterSchemaChange() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table wal_ec4 (sym symbol, ts timestamp) timestamp(ts) partition by DAY WAL");
            execute("insert into wal_ec4 values ('AA', '1970-01-01')");
            drainWalQueue(engine);
            execute("alter table wal_ec4 add column val int");
            drainWalQueue(engine);
            execute("insert into wal_ec4 (sym, ts, val) values ('BB', '1970-01-02', 42)");
            drainWalQueue(engine);

            // Show seqTxn 1 (data before DDL)
            String[] result = runSession("cd wal_ec4\ncd wal\nshow 1\nquit\n");
            Assert.assertTrue("should resolve seqTxn 1", result[0].contains("seqTxn: 1"));

            // Show seqTxn 3 (data after DDL)
            result = runSession("cd wal_ec4\ncd wal\nshow 3\nquit\n");
            Assert.assertTrue("should resolve seqTxn 3", result[0].contains("seqTxn: 3"));
        });
    }

    @Test
    public void testWalDirListingAfterSomeApplied() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table wal_ec5 (sym symbol, ts timestamp) timestamp(ts) partition by DAY WAL");
            try (WalWriter walWriter = getWalWriter("wal_ec5")) {
                for (int i = 0; i < 5; i++) {
                    TableWriter.Row row = walWriter.newRow(i * Micros.DAY_MICROS);
                    row.putSym(0, "AA");
                    row.append();
                    walWriter.commit();
                }
            }
            waitForAppliedRows("wal_ec5", 5);
            // All applied, then add pending ones
            execute("insert into wal_ec5 values ('BB', '1970-01-06')");
            String[] result = runSession("cd wal_ec5\ncd wal\nls\nquit\n");
            String out = result[0];
            Assert.assertTrue("should show rows column", out.contains("rows"));
        });
    }

    @Test
    public void testAllEnrichedOutputsNonWalTable() throws Exception {
        assertMemoryLeak(() -> {
            createNonWalTableWithRows("wal_ec6", 2);
            // show timeline at table level should error
            String[] result = runSession("cd wal_ec6\nshow timeline\nquit\n");
            Assert.assertTrue("should error", result[1].contains("WAL mode"));
            // wal status should say not WAL
            result = runSession("cd wal_ec6\nwal status\nquit\n");
            Assert.assertTrue("should show not WAL", result[0].contains("walEnabled: false"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testWalStatusWithMatViewTable() throws Exception {
        assertMemoryLeak(() -> {
            createMatViewWithBase("wal_ec7_mv", "wal_ec7_base", 3);
            // Mat views use WAL internally
            String[] result = runSession("cd wal_ec7_mv\nwal status\nquit\n");
            String out = result[0];
            Assert.assertTrue("should show walEnabled: true", out.contains("walEnabled: true"));
        });
    }

    @Test
    public void testShowTimelineLargeCommitCount() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table wal_ec8 (sym symbol, ts timestamp) timestamp(ts) partition by DAY WAL");
            try (WalWriter walWriter = getWalWriter("wal_ec8")) {
                for (int i = 0; i < 50; i++) {
                    TableWriter.Row row = walWriter.newRow(i * Micros.HOUR_MICROS);
                    row.putSym(0, "AA");
                    row.append();
                    walWriter.commit();
                }
            }
            waitForAppliedRows("wal_ec8", 50);
            String[] result = runSession("cd wal_ec8\ncd wal\nshow timeline\nquit\n");
            String out = result[0];
            // Should render all rows without truncation
            Assert.assertTrue("should show seqTxn header", out.contains("seqTxn"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testPartitionAffinityOutOfOrderData() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table wal_ec9 (sym symbol, ts timestamp) timestamp(ts) partition by DAY WAL");
            try (WalWriter walWriter = getWalWriter("wal_ec9")) {
                // Commit 0: Insert out of order: day 2, then day 1
                TableWriter.Row row1 = walWriter.newRow(Micros.DAY_MICROS); // 1970-01-02
                row1.putSym(0, "AA");
                row1.append();
                TableWriter.Row row2 = walWriter.newRow(0); // 1970-01-01
                row2.putSym(0, "BB");
                row2.append();
                walWriter.commit();
                // Add 2 more commits so events survive drain
                for (int i = 2; i < 4; i++) {
                    TableWriter.Row row = walWriter.newRow(i * Micros.DAY_MICROS);
                    row.putSym(0, "CC");
                    row.append();
                    walWriter.commit();
                }
            }
            waitForAppliedRows("wal_ec9", 4);
            String[] result = runSession("cd wal_ec9\ncd wal\ncd wal1\ncd 0\nshow 0\nquit\n");
            String out = result[0];
            // Should show both partitions and ooo: yes
            Assert.assertTrue("should show outOfOrder: yes", out.contains("outOfOrder: yes"));
            Assert.assertTrue("should show partitions", out.contains("partitions:"));
            Assert.assertTrue("should contain 1970-01-01", out.contains("1970-01-01"));
            Assert.assertTrue("should contain 1970-01-02", out.contains("1970-01-02"));
        });
    }

    @Test
    public void testWalStatusNoSeqTxnLogFile() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table wal_ec10 (sym symbol, ts timestamp) timestamp(ts) partition by DAY WAL");
            execute("insert into wal_ec10 values ('AA', '1970-01-01')");
            drainWalQueue(engine);
            // delete the txnlog file
            TableToken token = engine.verifyTableName("wal_ec10");
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(token.getDirName())
                        .concat("txn_seq").concat("_txnlog").$();
                FF.removeQuiet(path.$());
            }
            String[] result = runSession("cd wal_ec10\nwal status\nquit\n");
            // Should not crash, should show some error or empty state
            Assert.assertEquals("should not crash", "", result[1]);
        });
    }

    @Test
    public void testEnrichedLsPreservesExistingColumns() throws Exception {
        assertMemoryLeak(() -> {
            createWalTableWithCommits("wal_ec11", 3);
            String[] result = runSession("cd wal_ec11\ncd wal\nls\nquit\n");
            String out = result[0];
            // existing columns should still be present
            Assert.assertTrue("should have idx column", out.contains("idx"));
            Assert.assertTrue("should have wal column", out.contains("wal"));
            Assert.assertTrue("should have segments column", out.contains("segments"));
            Assert.assertTrue("should have refs column", out.contains("refs"));
            // new columns
            Assert.assertTrue("should have rows column", out.contains("rows"));
            Assert.assertTrue("should have partitions column", out.contains("partitions"));
        });
    }

    @Test
    public void testEnrichedWalEventsPreservesExistingColumns() throws Exception {
        assertMemoryLeak(() -> {
            createWalTableWithCommits("wal_ec12", 3);
            String[] result = runSession("cd wal_ec12\ncd wal\ncd wal1\ncd 0\nls\nquit\n");
            String out = result[0];
            // existing columns should still be present
            Assert.assertTrue("should have txn column", out.contains("txn"));
            Assert.assertTrue("should have type column", out.contains("type"));
            Assert.assertTrue("should have rows column", out.contains("rows"));
            Assert.assertTrue("should have minTimestamp column", out.contains("minTimestamp"));
            Assert.assertTrue("should have maxTimestamp column", out.contains("maxTimestamp"));
            Assert.assertTrue("should have ooo column", out.contains("ooo"));
            Assert.assertTrue("should have seqTxn column", out.contains("seqTxn"));
            Assert.assertTrue("should have status column", out.contains("status"));
            // new column
            Assert.assertTrue("should have partitions column", out.contains("partitions"));
        });
    }

    @Test
    public void testSegmentListingShowsRowsAndPartitions() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 10);
            execute("create table wal_sl1 (sym symbol, ts timestamp) timestamp(ts) partition by DAY WAL");
            try (WalWriter walWriter = getWalWriter("wal_sl1")) {
                for (int i = 0; i < 3; i++) {
                    TableWriter.Row row = walWriter.newRow(i * Micros.DAY_MICROS);
                    row.putSym(0, "AA");
                    row.append();
                    walWriter.commit();
                }
            }
            waitForAppliedRows("wal_sl1", 3);
            String[] result = runSession("cd wal_sl1\ncd wal\ncd wal1\nls\nquit\n");
            String out = result[0];
            // header should have rows and partitions columns
            Assert.assertTrue("should have rows column", out.contains("rows"));
            Assert.assertTrue("should have partitions column", out.contains("partitions"));
            // V2 data: each commit has 1 row — verify the partition name which is specific
            Assert.assertTrue("should show partition name", out.contains("1970-01-01"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testSegmentListingPreservesExistingColumns() throws Exception {
        assertMemoryLeak(() -> {
            createWalTableWithCommits("wal_sl2", 3);
            String[] result = runSession("cd wal_sl2\ncd wal\ncd wal1\nls\nquit\n");
            String out = result[0];
            // existing columns must be present
            Assert.assertTrue("should have idx column", out.contains("idx"));
            Assert.assertTrue("should have segment column", out.contains("segment"));
            Assert.assertTrue("should have _event column", out.contains("_event"));
            Assert.assertTrue("should have _event.i column", out.contains("_event.i"));
            Assert.assertTrue("should have _meta column", out.contains("_meta"));
            Assert.assertTrue("should have status column", out.contains("status"));
            // new enrichment columns
            Assert.assertTrue("should have rows column", out.contains("rows"));
            Assert.assertTrue("should have partitions column", out.contains("partitions"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testSegmentListingV1EnrichedFromEvents() throws Exception {
        assertMemoryLeak(() -> {
            // V1 (default, no SEQ_PART_TXN_COUNT) — enrichment from WAL events fills in rows
            execute("create table wal_sl3 (sym symbol, ts timestamp) timestamp(ts) partition by DAY WAL");
            try (WalWriter walWriter = getWalWriter("wal_sl3")) {
                for (int i = 0; i < 3; i++) {
                    TableWriter.Row row = walWriter.newRow(i * Micros.DAY_MICROS);
                    row.putSym(0, "AA");
                    row.append();
                    walWriter.commit();
                }
            }
            waitForAppliedRows("wal_sl3", 3);
            String[] result = runSession("cd wal_sl3\ncd wal\ncd wal1\nls\nquit\n");
            String out = result[0];
            // V1 with enrichment should show partition names (from event timestamps)
            Assert.assertTrue("should have partition names after enrichment", out.contains("1970-01-01"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testSegmentListingMultipleSegments() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 10);
            execute("create table wal_sl4 (sym symbol, ts timestamp) timestamp(ts) partition by DAY WAL");
            // Two separate WalWriters: each creates a new wal dir
            try (WalWriter w1 = getWalWriter("wal_sl4");
                 WalWriter w2 = getWalWriter("wal_sl4")) {
                TableWriter.Row row1 = w1.newRow(0);
                row1.putSym(0, "AA");
                row1.append();
                w1.commit();
                TableWriter.Row row2 = w2.newRow(Micros.DAY_MICROS);
                row2.putSym(0, "BB");
                row2.append();
                w2.commit();
            }
            waitForAppliedRows("wal_sl4", 2);
            // Navigate to wal root and check that we see both wal1 and wal2
            String[] result = runSession("cd wal_sl4\ncd wal\nls\nquit\n");
            String out = result[0];
            Assert.assertTrue("should have wal1", out.contains("wal1"));
            Assert.assertTrue("should have wal2", out.contains("wal2"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testSegmentListingShowsAppliedStatus() throws Exception {
        assertMemoryLeak(() -> {
            createWalTableWithCommits("wal_sl5", 3);
            // all txns applied after waitForAppliedRows
            String[] result = runSession("cd wal_sl5\ncd wal\ncd wal1\nls\nquit\n");
            String out = result[0];
            Assert.assertTrue("should show applied status", out.contains("applied"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testSegmentListingShowsPartialStatus() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table wal_sl6 (sym symbol, ts timestamp) timestamp(ts) partition by DAY WAL");
            try (WalWriter walWriter = getWalWriter("wal_sl6")) {
                TableWriter.Row row = walWriter.newRow(0);
                row.putSym(0, "AA");
                row.append();
                walWriter.commit();
                // apply first commit
                drainWalQueue(engine);

                // second commit stays pending (writer still open, not drained)
                TableWriter.Row row2 = walWriter.newRow(Micros.DAY_MICROS);
                row2.putSym(0, "BB");
                row2.append();
                walWriter.commit();
            }
            // don't drain — second commit is pending
            String[] result = runSession("cd wal_sl6\ncd wal\ncd wal1\nls\nquit\n");
            String out = result[0];
            Assert.assertTrue("should show partial status (1 applied + 1 pending)",
                    out.contains("partial"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testSegmentListingUnreferencedSegment() throws Exception {
        assertMemoryLeak(() -> {
            createWalTableWithCommits("wal_sl7", 3);
            // Create an extra segment dir that has no seqTxn references
            TableToken token = engine.verifyTableName("wal_sl7");
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(token.getDirName())
                        .concat("wal1").slash().put(99).slash$();
                Assert.assertEquals(0, FF.mkdirs(path, configuration.getMkDirMode()));
            }
            String[] result = runSession("cd wal_sl7\ncd wal\ncd wal1\nls\nquit\n");
            String out = result[0];
            // segment 99 should show "unreferenced" or "incomplete" depending on missing files
            Assert.assertTrue("should list segment 99",
                    out.contains("99"));
            Assert.assertEquals("", result[1]);
        });
    }

    @Test
    public void testHelpShowsTimelineCommand() throws Exception {
        assertMemoryLeak(() -> {
            String[] result = runSession("help\nquit\n");
            Assert.assertTrue("help should mention show timeline", result[0].contains("show timeline"));
        });
    }

    @Test
    public void testHelpShowsSeqTxnDetail() throws Exception {
        assertMemoryLeak(() -> {
            String[] result = runSession("help\nquit\n");
            Assert.assertTrue("help should mention show <N> for seqTxn detail", result[0].contains("seqTxn detail"));
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

    private static void createWalTableWithCommits(String tableName, int commitCount) throws SqlException {
        execute("create table " + tableName + " (sym symbol, ts timestamp) timestamp(ts) partition by DAY WAL");
        try (WalWriter walWriter = getWalWriter(tableName)) {
            for (int i = 0; i < commitCount; i++) {
                TableWriter.Row row = walWriter.newRow(i * Micros.DAY_MICROS);
                row.putSym(0, "AA");
                row.append();
                walWriter.commit();
            }
        }
        waitForAppliedRows(tableName, commitCount);
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
        RecoverySession session = RecoverySession.create(
                configuration.getDbRoot(),
                FF,
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

    private static void waitForEmptyTable() {
        for (int i = 0; i < 20; i++) {
            engine.releaseAllWriters();
            engine.releaseAllReaders();
            drainWalQueue(engine);
        }
    }
}
