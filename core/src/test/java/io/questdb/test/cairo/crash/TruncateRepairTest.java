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

package io.questdb.test.cairo.crash;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.DurableEpochManifest;
import io.questdb.cairo.O3PartitionPurgeJob;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.RecoveryCoordinator;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.std.Files;
import io.questdb.std.MemoryTag;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.stream.Stream;

/**
 * A non-WAL TRUNCATE arms a {@code _todo_} entry before it removes rows, and clears it once it has published the
 * empty {@code _txn}. When the TRUNCATE stops in between, on an I/O error or a process kill, the next writer open
 * completes it: the table must end up empty, with the symbol maps the TRUNCATE asked for, and take new rows. A
 * TRUNCATE that stops before it has armed its entry must leave the table as it was.
 */
public class TruncateRepairTest extends AbstractAdaptiveCrashTest {
    private static final String NONE_DDL = """
            CREATE TABLE %s (
                s SYMBOL,
                si SYMBOL INDEX,
                sp SYMBOL INDEX TYPE POSTING,
                v VARCHAR,
                str STRING,
                x LONG,
                arr DOUBLE[]
            )""";
    private static final String NONE_EXPECTED_AFTER_INSERT = """
            s\tsi\tsp\tv\tstr\tx\tarr\ty
            a\tk1\tp1\tv5\ts5\t5\t[5.0]\t50
            z\tk9\tp9\tv6\ts6\t6\t[6.0]\t60
            """;
    // the rows of createNonPartitioned(): x was rewritten by UPDATE, y was added after the first two rows
    private static final String NONE_EXPECTED_BEFORE_TRUNCATE = """
            s\tsi\tsp\tv\tstr\tx\tarr\ty
            a\tk1\tp1\tv1\ts1\t101\t[1.0]\tnull
            b\tk2\tp2\tv2\ts2\t102\t[2.0]\tnull
            c\tk3\tp3\tv3\ts3\t103\t[3.0]\t30
            """;
    private static final String NONE_INSERT_AFTER_REPAIR = """
            INSERT INTO %s VALUES
                ('a', 'k1', 'p1', 'v5', 's5', 5, ARRAY[5.0], 50),
                ('z', 'k9', 'p9', 'v6', 's6', 6, ARRAY[6.0], 60)""";
    private static final String NONE_SETUP_INSERT_1 = """
            INSERT INTO %s VALUES
                ('a', 'k1', 'p1', 'v1', 's1', 1, ARRAY[1.0]),
                ('b', 'k2', 'p2', 'v2', 's2', 2, ARRAY[2.0])""";
    private static final String NONE_SETUP_INSERT_2 = """
            INSERT INTO %s VALUES ('c', 'k3', 'p3', 'v3', 's3', 3, ARRAY[3.0], 30)""";
    private static final String PARTITIONED_DDL = """
            CREATE TABLE %s (
                ts TIMESTAMP,
                s SYMBOL,
                si SYMBOL INDEX,
                sp SYMBOL INDEX TYPE POSTING,
                v VARCHAR,
                str STRING,
                x LONG,
                arr DOUBLE[]
            ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL""";
    private static final String PARTITIONED_EXPECTED_AFTER_INSERT = """
            ts\ts\tsi\tsp\tv\tstr\tx\tarr\ty
            2024-01-01T00:00:00.000000Z\ta\tk1\tp1\tv5\ts5\t5\t[5.0]\t50
            2024-01-02T00:00:00.000000Z\tz\tk9\tp9\tv6\ts6\t6\t[6.0]\t60
            """;
    // the rows of createPartitioned(): UPDATE rewrote x of the 2024-01-01 row only
    private static final String PARTITIONED_EXPECTED_BEFORE_TRUNCATE = """
            ts\ts\tsi\tsp\tv\tstr\tx\tarr\ty
            2024-01-01T00:00:00.000000Z\ta\tk1\tp1\tv1\ts1\t101\t[1.0]\tnull
            2024-01-02T00:00:00.000000Z\tb\tk2\tp2\tv2\ts2\t2\t[2.0]\tnull
            2024-01-02T01:00:00.000000Z\tc\tk1\tp3\tv3\ts3\t3\t[3.0]\t30
            2024-01-03T00:00:00.000000Z\ta\tk3\tp1\tv4\ts4\t4\t[4.0]\t40
            """;
    // 2024-01-01 was converted to parquet and had x rewritten by UPDATE; y was added while 2024-01-02 held rows
    private static final String PARTITIONED_INSERT_AFTER_REPAIR = """
            INSERT INTO %s VALUES
                ('2024-01-01T00:00:00.000000Z', 'a', 'k1', 'p1', 'v5', 's5', 5, ARRAY[5.0], 50),
                ('2024-01-02T00:00:00.000000Z', 'z', 'k9', 'p9', 'v6', 's6', 6, ARRAY[6.0], 60)""";
    private static final String PARTITIONED_SETUP_INSERT_1 = """
            INSERT INTO %s VALUES
                ('2024-01-01T00:00:00.000000Z', 'a', 'k1', 'p1', 'v1', 's1', 1, ARRAY[1.0]),
                ('2024-01-02T00:00:00.000000Z', 'b', 'k2', 'p2', 'v2', 's2', 2, ARRAY[2.0])""";
    private static final String PARTITIONED_SETUP_INSERT_2 = """
            INSERT INTO %s VALUES
                ('2024-01-02T01:00:00.000000Z', 'c', 'k1', 'p3', 'v3', 's3', 3, ARRAY[3.0], 30),
                ('2024-01-03T00:00:00.000000Z', 'a', 'k3', 'p1', 'v4', 's4', 4, ARRAY[4.0], 40)""";
    // the rows of createSymbolFreeTable() and the row x = 4 that the tests insert after them
    private static final String SYMBOL_FREE_EXPECTED_AFTER_INSERT = """
            ts\tx\ty
            2024-01-01T00:00:00.000000Z\t1\tnull
            2024-01-02T00:00:00.000000Z\t2\tnull
            2024-01-02T01:00:00.000000Z\t3\t30
            2024-01-03T00:00:00.000000Z\t4\t40
            """;
    // the rows of createSymbolFreeTable()
    private static final String SYMBOL_FREE_EXPECTED_BEFORE_TRUNCATE = """
            ts\tx\ty
            2024-01-01T00:00:00.000000Z\t1\tnull
            2024-01-02T00:00:00.000000Z\t2\tnull
            2024-01-02T01:00:00.000000Z\t3\t30
            """;
    // Layout of a _todo_ entry (TableWriter.truncate0()) and its codes (TableUtils.TODO_*)
    private static final long TODO_CODE_OFFSET = 40;
    private static final long TODO_COUNT_OFFSET = 32;
    private static final long TODO_RESTORE_META = 2;
    private static final long TODO_TRUNCATE = 1;

    @Test
    public void testFailedTruncateIsCompletedByNextWriterOpen() throws Exception {
        // TRUNCATE stops on an I/O error in truncateColumns(), after it armed _todo_. The next writer open must
        // complete it, whatever the number of commits before it.
        final FailingTruncateFacade ff = new FailingTruncateFacade();
        assertMemoryLeak(ff, () -> {
            for (int keep = 0; keep < 2; keep++) {
                final boolean isKeepSymbolMaps = keep == 1;
                for (int priorCommits = 1; priorCommits <= 3; priorCommits++) {
                    final String table = "t_" + keep + "_" + priorCommits;
                    execute(String.format(NONE_DDL, table));
                    for (int i = 0; i < priorCommits; i++) {
                        execute(String.format(NONE_SETUP_INSERT_1, table));
                    }
                    execute("ALTER TABLE " + table + " ADD COLUMN y INT");
                    execute(String.format(NONE_SETUP_INSERT_2, table));

                    ff.isArmed = true;
                    try {
                        execute(truncateSql(table, isKeepSymbolMaps));
                        Assert.fail("TRUNCATE must fail [table=" + table + ']');
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "Cannot truncate fd=");
                    } finally {
                        ff.isArmed = false;
                    }
                    Assert.assertTrue("the failure was not injected", ff.isFired);
                    ff.isFired = false;
                    // the writer closes, as on a restart
                    engine.releaseAllWriters();

                    execute(String.format(NONE_INSERT_AFTER_REPAIR, table));
                    // KEEP SYMBOL MAPS keeps the three values of each symbol column
                    assertSymbolCounts(table, isKeepSymbolMaps ? 4 : 2);
                    assertNoneTable(table, NONE_EXPECTED_AFTER_INSERT);

                    // the next open finds nothing to repair
                    engine.releaseAllWriters();
                    execute("INSERT INTO " + table + " VALUES ('c', 'k1', 'p7', 'v7', 's7', 7, ARRAY[7.0], 70)");
                    assertQuery("SELECT * FROM " + table)
                            .noLeakCheck()
                            .expectSize()
                            .returns(NONE_EXPECTED_AFTER_INSERT + "c\tk1\tp7\tv7\ts7\t7\t[7.0]\t70\n");

                    // the repaired table truncates again
                    execute(truncateSql(table, isKeepSymbolMaps));
                    execute("INSERT INTO " + table + " VALUES ('b', 'k2', 'p2', 'v8', 's8', 8, ARRAY[8.0], 80)");
                    assertQuery("SELECT * FROM " + table)
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    s\tsi\tsp\tv\tstr\tx\tarr\ty
                                    b\tk2\tp2\tv8\ts8\t8\t[8.0]\t80
                                    """);
                }
            }
        });
    }

    @Test
    public void testKeepSymbolMapsKilledBeforeTodoCodeAfterTruncate() throws Exception {
        // A completed TRUNCATE leaves its code, TODO_TRUNCATE, in _todo_ for as long as its writer stays open. A
        // TRUNCATE ... KEEP SYMBOL MAPS through that writer, killed before it stores its own code, must leave the
        // table as it was: a repair with the code of the plain TRUNCATE would empty the symbol maps too.
        setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, Files.PAGE_SIZE);
        runWithCrashFacade(() -> {
            final String table = "t";
            createNonPartitioned(table);
            execute(truncateSql(table, false));
            execute(String.format(NONE_INSERT_AFTER_REPAIR, table));
            engine.releaseAllReaders();
            final long txnBefore = readTxn(table, PartitionBy.NONE);
            Assert.assertEquals("code in _todo_", TODO_TRUNCATE, readTodoLong(table, TODO_CODE_OFFSET));

            killTruncateBeforeTodoCode(table, truncateSql(table, true));

            Assert.assertEquals(txnBefore, readTxn(table, PartitionBy.NONE));
            try (TableWriter writer = getWriter(table)) {
                Assert.assertEquals(2, writer.size());
            }
            assertSymbolCounts(table, 2);
            assertNoneTable(table, NONE_EXPECTED_AFTER_INSERT);

            // the table truncates and keeps its symbol maps
            execute(truncateSql(table, true));
            assertSymbolCounts(table, 2);
            execute(String.format(NONE_INSERT_AFTER_REPAIR, table));
            assertSymbolCounts(table, 2);
            assertNoneTable(table, NONE_EXPECTED_AFTER_INSERT);
        });
    }

    @Test
    public void testNonPartitionedKeepSymbolMapsKilledBeforeTodoCodeKeepsTable() throws Exception {
        assertKeepSymbolMapsKilledBeforeTodoCodeKeepsTable(false);
    }

    @Test
    public void testNonPartitionedKeepSymbolMapsSurvivesEveryKillPoint() throws Exception {
        assertTruncateSurvivesEveryKillPoint(false, true);
    }

    @Test
    public void testNonPartitionedSurvivesEveryKillPoint() throws Exception {
        assertTruncateSurvivesEveryKillPoint(false, false);
    }

    @Test
    public void testNonPartitionedTruncateKilledBeforeTodoCodeKeepsTable() throws Exception {
        assertTruncateKilledBeforeTodoCodeKeepsTable(false);
    }

    @Test
    public void testPartitionedKeepSymbolMapsKilledBeforeTodoCodeKeepsTable() throws Exception {
        assertKeepSymbolMapsKilledBeforeTodoCodeKeepsTable(true);
    }

    @Test
    public void testPartitionedKeepSymbolMapsSurvivesEveryKillPoint() throws Exception {
        assertTruncateSurvivesEveryKillPoint(true, true);
    }

    @Test
    public void testPartitionedSurvivesEveryKillPoint() throws Exception {
        assertTruncateSurvivesEveryKillPoint(true, false);
    }

    @Test
    public void testPartitionedTruncateKilledBeforeTodoCodeKeepsTable() throws Exception {
        assertTruncateKilledBeforeTodoCodeKeepsTable(true);
    }

    @Test
    public void testRepairKeepSymbolMapsSurvivesEveryKillPoint() throws Exception {
        assertRepairSurvivesEveryKillPoint(true);
    }

    @Test
    public void testRepairSurvivesEveryKillPoint() throws Exception {
        assertRepairSurvivesEveryKillPoint(false);
    }

    @Test
    public void testRepairWaitsForReaderOfOldRows() throws Exception {
        // A query reads the rows of an interrupted TRUNCATE ... KEEP SYMBOL MAPS while the writer open repairs the
        // table and new rows arrive: the repair leaves the partition versions the query holds to the purge job, and
        // the new symbols leave the symbol keys of the old rows alone. A TRUNCATE without KEEP SYMBOL MAPS empties
        // the symbol maps before it arms _todo_, so a query of its interrupted state cannot resolve the old keys
        // even before the repair.
        setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, Files.PAGE_SIZE);
        runWithCrashFacade(() -> {
            final String table = "t";
            createPartitioned(table);
            engine.releaseAllReaders();
            // KEEP SYMBOL MAPS leaves the symbol maps alone, so every kill point of the TRUNCATE comes after _todo_
            crashFf.armKillAt(crashFf.killPointCount() + 1, tableDir(table));
            execute(truncateSql(table, true));
            Assert.assertTrue(crashFf.isKillImageTaken());
            restoreKillImage(table);
            final long partitionDirs = countPartitionDirs(table);
            Assert.assertTrue(partitionDirs >= 3);

            try (RecordCursorFactory factory = select("SELECT * FROM " + table)) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    try (TableWriter writer = getWriter(table)) {
                        Assert.assertEquals(0, writer.size());
                    }
                    Assert.assertEquals(partitionDirs, countPartitionDirs(table));
                    // new symbols z and k9, and the partitions of the old rows
                    execute(String.format(PARTITIONED_INSERT_AFTER_REPAIR, table));
                    sink.clear();
                    TestUtils.assertCursor(
                            """
                                    ts\ts\tsi\tsp\tv\tstr\tx\tarr\ty
                                    2024-01-01T00:00:00.000000Z\ta\tk1\tp1\tv1\ts1\t101\t[1.0]\tnull
                                    2024-01-02T00:00:00.000000Z\tb\tk2\tp2\tv2\ts2\t2\t[2.0]\tnull
                                    2024-01-02T01:00:00.000000Z\tc\tk1\tp3\tv3\ts3\t3\t[3.0]\t30
                                    2024-01-03T00:00:00.000000Z\ta\tk3\tp1\tv4\ts4\t4\t[4.0]\t40
                                    """,
                            cursor,
                            factory.getMetadata(),
                            true,
                            sink
                    );
                }
            }
            engine.releaseAllReaders();
            try (O3PartitionPurgeJob purgeJob = new O3PartitionPurgeJob(engine, 1)) {
                purgeJob.drain(0);
            }
            assertPartitionedTable(table, PARTITIONED_EXPECTED_AFTER_INSERT);
            assertSymbolCounts(table, 4);
        });
    }

    @Test
    public void testTableLeftByOlderVersionOpens() throws Exception {
        // An older version stamped a TRUNCATE _todo_ entry with a counter of its own, and failed every writer open
        // once that counter matched the _txn txn. Such a table opens: the writer completes the TRUNCATE.
        assertMemoryLeak(() -> {
            final String table = "t";
            execute(String.format(NONE_DDL, table));
            execute(String.format(NONE_SETUP_INSERT_1, table));
            execute("ALTER TABLE " + table + " ADD COLUMN y INT");
            execute(String.format(NONE_SETUP_INSERT_2, table));
            engine.releaseAllWriters();
            engine.releaseAllReaders();

            final long txn = readTxn(table, PartitionBy.NONE);
            final TableToken token = engine.verifyTableName(table);
            try (
                    MemoryCMARW mem = Vm.getCMARWInstance();
                    Path path = new Path().of(root).concat(token).concat(TableUtils.TODO_FILE_NAME)
            ) {
                mem.smallFile(TestFilesFacadeImpl.INSTANCE, path.$(), MemoryTag.MMAP_DEFAULT);
                mem.putLong(0, txn);
                mem.putLong(24, txn);
                mem.putLong(32, 1);
                mem.putLong(40, 1); // TableUtils.TODO_TRUNCATE
                mem.jumpTo(48);
            }

            execute(String.format(NONE_INSERT_AFTER_REPAIR, table));
            assertSymbolCounts(table, 2);
            assertNoneTable(table, NONE_EXPECTED_AFTER_INSERT);
        });
    }

    @Test
    public void testTruncateFailsCleanlyWhenOlderTodoFileCannotGrow() throws Exception {
        // The code store of a TRUNCATE grows the 40-byte _todo_ of an older version, and the disk is full: the failed
        // TRUNCATE must leave no armed entry behind.
        final TodoAllocateFailingFacade ff = new TodoAllocateFailingFacade();
        assertMemoryLeak(ff, () -> {
            final String table = "t";
            execute("CREATE TABLE t (x LONG)");
            execute("INSERT INTO t VALUES (1), (2), (3)");
            engine.releaseAllWriters();
            engine.releaseAllReaders();
            cutTodoToClearedEntry(table);

            ff.isArmed = true;
            try {
                execute(truncateSql(table, false));
                Assert.fail("TRUNCATE must fail");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "No space left");
            } finally {
                ff.isArmed = false;
            }
            Assert.assertTrue("the failure was not injected", ff.isFired);
            Assert.assertEquals("count of the _todo_ entry", 0, readTodoLong(table, TODO_COUNT_OFFSET));
            assertQuery("SELECT * FROM t")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\n1\n2\n3\n");

            // the writer open finds nothing to repair
            engine.releaseAllWriters();
            execute("INSERT INTO t VALUES (4)");
            assertQuery("SELECT * FROM t")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\n1\n2\n3\n4\n");

            // the table truncates and takes rows
            execute(truncateSql(table, false));
            assertQuery("SELECT * FROM t")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\n");
            execute("INSERT INTO t VALUES (5)");
            assertQuery("SELECT * FROM t")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\n5\n");
        });
    }

    @Test
    public void testTruncateOfOlderTodoFileKilledAtCodeStore() throws Exception {
        // The code store of a TRUNCATE grows the 40-byte _todo_ of an older version: a process killed in that growth
        // must leave no armed entry behind.
        assumeCrashHarnessSupported();
        final TodoRemapKillFacade ff = new TodoRemapKillFacade();
        crashFf = ff;
        ff.setDbRoot(root);
        try {
            assertMemoryLeak(ff, () -> {
                final String table = "t";
                execute("CREATE TABLE t (x LONG)");
                execute("INSERT INTO t VALUES (1), (2), (3)");
                engine.releaseAllWriters();
                engine.releaseAllReaders();
                cutTodoToClearedEntry(table);
                final long txnBefore = readTxn(table, PartitionBy.NONE);

                ff.killTableDir = tableDir(table);
                execute(truncateSql(table, false));
                Assert.assertTrue("TRUNCATE did not grow _todo_", ff.isKillImageTaken());
                restoreKillImage(table);

                Assert.assertEquals("count of the _todo_ entry", 0, readTodoLong(table, TODO_COUNT_OFFSET));
                Assert.assertEquals(txnBefore, readTxn(table, PartitionBy.NONE));
                try (TableWriter writer = getWriter(table)) {
                    Assert.assertEquals(3, writer.size());
                }
                execute("INSERT INTO t VALUES (4)");
                assertQuery("SELECT * FROM t")
                        .noLeakCheck()
                        .expectSize()
                        .returns("x\n1\n2\n3\n4\n");
            });
        } finally {
            ff.reset();
            engine.releaseCrashOrphanedWalWriters();
            engine.releaseCrashOrphanedWriters();
        }
    }

    @Test
    public void testWalTableTruncateKilledBeforeTodoCodeKeepsTable() throws Exception {
        assertWalTableTruncateKilledBeforeTodoCodeKeepsTable(false);
    }

    @Test
    public void testWalTableTruncateKilledBeforeTodoCodeKeepsTableAfterAdaptiveRecovery() throws Exception {
        assertWalTableTruncateKilledBeforeTodoCodeKeepsTable(true);
    }

    // every SYMBOL column of the test tables holds the same number of distinct values
    private static void assertSymbolCounts(String table, int expected) {
        try (TableReader reader = getReader(table)) {
            for (String column : new String[]{"s", "si", "sp"}) {
                final int columnIndex = reader.getMetadata().getColumnIndex(column);
                Assert.assertEquals(column, expected, reader.getSymbolMapReader(columnIndex).getSymbolCount());
            }
        }
    }

    private static long countPartitionDirs(String table) {
        try (Stream<java.nio.file.Path> dirs = java.nio.file.Files.list(java.nio.file.Path.of(tableDir(table)))) {
            return dirs.filter(d -> d.getFileName().toString().matches("\\d{4}-\\d{2}-\\d{2}(\\.\\d+)?")).count();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void createNonPartitioned(String table) throws Exception {
        execute(String.format(NONE_DDL, table));
        execute(String.format(NONE_SETUP_INSERT_1, table));
        execute("ALTER TABLE " + table + " ADD COLUMN y INT");
        execute(String.format(NONE_SETUP_INSERT_2, table));
        execute("UPDATE " + table + " SET x = x + 100");
    }

    private static void createPartitioned(String table) throws Exception {
        execute(String.format(PARTITIONED_DDL, table));
        execute(String.format(PARTITIONED_SETUP_INSERT_1, table));
        execute("ALTER TABLE " + table + " ADD COLUMN y INT");
        execute(String.format(PARTITIONED_SETUP_INSERT_2, table));
        execute("UPDATE " + table + " SET x = x + 100 WHERE ts < '2024-01-02'");
        execute("ALTER TABLE " + table + " CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-02'");
    }

    // ts and x, then y, added after the first two rows; a WAL table applies each step before the next one
    private static void createSymbolFreeTable(String table, String tableOptions, boolean isWal) throws Exception {
        execute("CREATE TABLE " + table + " (ts TIMESTAMP, x LONG)" + tableOptions);
        execute("""
                INSERT INTO %s VALUES
                    ('2024-01-01T00:00:00.000000Z', 1),
                    ('2024-01-02T00:00:00.000000Z', 2)""".formatted(table));
        if (isWal) {
            drainWalQueue();
        }
        execute("ALTER TABLE " + table + " ADD COLUMN y INT");
        execute("INSERT INTO " + table + " VALUES ('2024-01-02T01:00:00.000000Z', 3, 30)");
        if (isWal) {
            drainWalQueue();
        }
    }

    private static void createTable(String table, boolean isPartitioned) throws Exception {
        if (isPartitioned) {
            createPartitioned(table);
        } else {
            createNonPartitioned(table);
        }
    }

    /**
     * Cuts _todo_ to the 40 bytes of a cleared entry, the length a clean writer close left before writers kept the
     * file one page long. The next writer maps 40 bytes, and the code store of a TRUNCATE, its first store past them,
     * grows the file.
     */
    private static void cutTodoToClearedEntry(String table) {
        final TestFilesFacadeImpl ff = TestFilesFacadeImpl.INSTANCE;
        try (Path path = new Path().of(root).concat(engine.verifyTableName(table)).concat(TableUtils.TODO_FILE_NAME)) {
            final long fd = ff.openRW(path.$(), CairoConfiguration.O_NONE);
            Assert.assertTrue("open _todo_", fd > -1);
            try {
                Assert.assertTrue("cut _todo_", ff.truncate(fd, TODO_CODE_OFFSET));
            } finally {
                ff.close(fd);
            }
        }
    }

    private static boolean hasMetaPrev(String table) {
        final TableToken token = engine.verifyTableName(table);
        try (Path path = new Path().of(root).concat(token).concat(TableUtils.META_PREV_FILE_NAME)) {
            return TestFilesFacadeImpl.INSTANCE.exists(path.$());
        }
    }

    private static boolean hasSavePoint(TableToken token) {
        final TestFilesFacadeImpl ff = TestFilesFacadeImpl.INSTANCE;
        try (Path path = new Path().of(root).concat(token)) {
            return DurableEpochManifest.loadCandidates(ff, path, path.size(), token.getTableId()).length > 0;
        }
    }

    // a long of the _todo_ file as the page cache holds it, with the stores of a writer that maps the file
    private static long readTodoLong(String table, long offset) {
        final TestFilesFacadeImpl ff = TestFilesFacadeImpl.INSTANCE;
        try (Path path = new Path().of(root).concat(engine.verifyTableName(table)).concat(TableUtils.TODO_FILE_NAME)) {
            final long fd = ff.openRO(path.$());
            Assert.assertTrue("open _todo_", fd > -1);
            try {
                return ff.readNonNegativeLong(fd, offset);
            } finally {
                ff.close(fd);
            }
        }
    }

    private static long readTxn(String table, int partitionBy) {
        final TableToken token = engine.verifyTableName(table);
        try (
                TxReader txReader = new TxReader(engine.getConfiguration().getFilesFacade());
                Path path = new Path().of(root).concat(token).concat(TableUtils.TXN_FILE_NAME)
        ) {
            txReader.ofRO(path.$(), ColumnType.TIMESTAMP, partitionBy);
            TableUtils.safeReadTxn(txReader, configuration.getMillisecondClock(), configuration.getSpinLockTimeout());
            return txReader.getTxn();
        }
    }

    private static String tableDir(String table) {
        return root + Files.SEPARATOR + engine.verifyTableName(table).getDirName();
    }

    private static String truncateSql(String table, boolean isKeepSymbolMaps) {
        return "TRUNCATE TABLE " + table + (isKeepSymbolMaps ? " KEEP SYMBOL MAPS" : "");
    }

    private void assertKeepSymbolMapsKilledBeforeTodoCodeKeepsTable(boolean isPartitioned) throws Exception {
        // ADD COLUMN leaves its code, TODO_RESTORE_META, in _todo_ for as long as its writer stays open. A TRUNCATE
        // through that writer, killed before it stores its own code, must leave the table as it was: a repair with
        // the code of ADD COLUMN would bring back the _meta from before the column.
        setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, Files.PAGE_SIZE);
        runWithCrashFacade(() -> {
            final String table = "t";
            createTable(table, isPartitioned);
            engine.releaseAllReaders();
            final int partitionBy = isPartitioned ? PartitionBy.DAY : PartitionBy.NONE;
            final long txnBefore = readTxn(table, partitionBy);
            Assert.assertEquals("code in _todo_", TODO_RESTORE_META, readTodoLong(table, TODO_CODE_OFFSET));

            killTruncateBeforeTodoCode(table, truncateSql(table, true));

            Assert.assertEquals(txnBefore, readTxn(table, partitionBy));
            try (TableWriter writer = getWriter(table)) {
                Assert.assertEquals(isPartitioned ? 4 : 3, writer.size());
                Assert.assertEquals("columns with y", isPartitioned ? 9 : 8, writer.getMetadata().getColumnCount());
            }
            assertSymbolCounts(table, 3);
            assertQuery("SELECT * FROM " + table)
                    .noLeakCheck()
                    .expectSize()
                    .timestamp(isPartitioned ? "ts" : null)
                    .returns(isPartitioned ? PARTITIONED_EXPECTED_BEFORE_TRUNCATE : NONE_EXPECTED_BEFORE_TRUNCATE);

            // the table truncates, keeps its symbol maps and takes rows
            execute(truncateSql(table, true));
            assertSymbolCounts(table, 3);
            if (isPartitioned) {
                execute(String.format(PARTITIONED_INSERT_AFTER_REPAIR, table));
                assertPartitionedTable(table, PARTITIONED_EXPECTED_AFTER_INSERT);
            } else {
                execute(String.format(NONE_INSERT_AFTER_REPAIR, table));
                assertNoneTable(table, NONE_EXPECTED_AFTER_INSERT);
            }
            assertSymbolCounts(table, 4);
            // the next open finds nothing to repair
            engine.releaseAllWriters();
            try (TableWriter writer = getWriter(table)) {
                Assert.assertEquals(2, writer.size());
            }
        });
    }

    private void assertNoneTable(String table, String expected) throws Exception {
        assertQuery("SELECT * FROM " + table)
                .noLeakCheck()
                .expectSize()
                .returns(expected);
        assertQuery("SELECT s, x FROM " + table + " WHERE si = 'k9'")
                .noLeakCheck()
                .returns("s\tx\nz\t6\n");
        assertQuery("SELECT s, x FROM " + table + " WHERE sp = 'p1'")
                .noLeakCheck()
                .returns("s\tx\na\t5\n");
        assertQuery("SELECT count() FROM " + table + " WHERE si = 'k2'")
                .noLeakCheck()
                .expectSize()
                .noRandomAccess()
                .returns("count\n0\n");
    }

    private void assertPartitionedTable(String table, String expected) throws Exception {
        assertQuery("SELECT * FROM " + table)
                .noLeakCheck()
                .expectSize()
                .timestamp("ts")
                .returns(expected);
        assertQuery("SELECT ts, x FROM " + table + " WHERE si = 'k9'")
                .noLeakCheck()
                .timestamp("ts")
                .returns("ts\tx\n2024-01-02T00:00:00.000000Z\t6\n");
        assertQuery("SELECT ts, x FROM " + table + " WHERE sp = 'p1'")
                .noLeakCheck()
                .timestamp("ts")
                .returns("ts\tx\n2024-01-01T00:00:00.000000Z\t5\n");
        assertQuery("SELECT count() FROM " + table + " WHERE si = 'k2'")
                .noLeakCheck()
                .expectSize()
                .noRandomAccess()
                .returns("count\n0\n");
        Assert.assertEquals(2, countPartitionDirs(table));
    }

    private void assertRepairSurvivesEveryKillPoint(boolean isKeepSymbolMaps) throws Exception {
        // A kill during the writer open that repairs the table: the open after it must finish the repair.
        setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, Files.PAGE_SIZE);
        runWithCrashFacade(() -> {
            final int keepPoints = countTruncateKillPoints(true, true);
            // the first kill point after TRUNCATE armed _todo_
            final int point = isKeepSymbolMaps ? 1 : countTruncateKillPoints(true, false) - keepPoints + 1;

            final String countTable = "count_repair";
            interruptTruncate(countTable, point, isKeepSymbolMaps);
            final int before = crashFf.killPointCount();
            getWriter(countTable).close();
            final int repairPoints = crashFf.killPointCount() - before;
            Assert.assertTrue("the repair has no kill points", repairPoints > 0);
            execute("DROP TABLE " + countTable);

            for (int k = 1; k <= repairPoints + 1; k++) {
                final String table = "t_" + k;
                final long txnBefore = interruptTruncate(table, point, isKeepSymbolMaps);
                Assert.assertEquals(
                        "the kill must stop TRUNCATE before it publishes _txn",
                        txnBefore,
                        readTxn(table, PartitionBy.DAY)
                );
                if (k <= repairPoints) {
                    crashFf.armKillAt(crashFf.killPointCount() + k, tableDir(table));
                    getWriter(table).close();
                    Assert.assertTrue("kill point " + k + " was not reached", crashFf.isKillImageTaken());
                } else {
                    getWriter(table).close();
                    crashFf.takeKillImage(tableDir(table));
                }
                restoreKillImage(table);
                try (TableWriter writer = getWriter(table)) {
                    Assert.assertEquals("kill point " + k, 0, writer.size());
                }
                assertSymbolCounts(table, isKeepSymbolMaps ? 3 : 0);
                execute(String.format(PARTITIONED_INSERT_AFTER_REPAIR, table));
                assertPartitionedTable(table, PARTITIONED_EXPECTED_AFTER_INSERT);
                assertSymbolCounts(table, isKeepSymbolMaps ? 4 : 2);
                execute("DROP TABLE " + table);
            }
        });
    }

    private void assertTruncateSurvivesEveryKillPoint(boolean isPartitioned, boolean isKeepSymbolMaps) throws Exception {
        setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, Files.PAGE_SIZE);
        runWithCrashFacade(() -> {
            final int keepPoints = countTruncateKillPoints(isPartitioned, true);
            final int points = isKeepSymbolMaps ? keepPoints : countTruncateKillPoints(isPartitioned, false);
            // TRUNCATE without KEEP SYMBOL MAPS empties the symbol maps before it arms _todo_. No writer open repairs
            // a kill there, so the sweep starts at the first kill point after it, the first of a KEEP SYMBOL MAPS run.
            final int firstPoint = points - keepPoints + 1;
            Assert.assertTrue("TRUNCATE has no kill points", keepPoints > 0);
            Assert.assertTrue("emptying the symbol maps has no kill points", isKeepSymbolMaps || firstPoint > 1);
            final int partitionBy = isPartitioned ? PartitionBy.DAY : PartitionBy.NONE;

            for (int k = firstPoint; k <= points + 1; k++) {
                final String table = "t_" + k;
                createTable(table, isPartitioned);
                engine.releaseAllReaders();
                final long txnBefore = readTxn(table, partitionBy);
                if (k <= points) {
                    crashFf.armKillAt(crashFf.killPointCount() + k, tableDir(table));
                }
                execute(truncateSql(table, isKeepSymbolMaps));
                if (k > points) {
                    crashFf.takeKillImage(tableDir(table));
                }
                Assert.assertTrue("kill point " + k + " was not reached", crashFf.isKillImageTaken());
                restoreKillImage(table);

                final long txnAfterKill = readTxn(table, partitionBy);
                final boolean isTruncatePublished = txnAfterKill != txnBefore;
                if (isTruncatePublished) {
                    Assert.assertEquals("kill point " + k, txnBefore + 1, txnAfterKill);
                }
                try (TableWriter writer = getWriter(table)) {
                    Assert.assertEquals("kill point " + k, 0, writer.size());
                    Assert.assertEquals("kill point " + k, txnBefore + 1, writer.getTxn());
                }
                // KEEP SYMBOL MAPS keeps the three values of each symbol column, whether the writer open repaired the
                // table or the TRUNCATE published _txn before the kill
                assertSymbolCounts(table, isKeepSymbolMaps ? 3 : 0);

                if (isPartitioned) {
                    execute(String.format(PARTITIONED_INSERT_AFTER_REPAIR, table));
                    assertPartitionedTable(table, PARTITIONED_EXPECTED_AFTER_INSERT);
                    assertSymbolCounts(table, isKeepSymbolMaps ? 4 : 2);
                    // the next open finds nothing to repair
                    engine.releaseAllWriters();
                    execute("""
                            INSERT INTO %s VALUES
                                ('2024-01-03T00:00:00.000000Z', 'c', 'k1', 'p7', 'v7', 's7', 7, ARRAY[7.0], 70)""".formatted(table));
                    assertQuery("SELECT * FROM " + table)
                            .noLeakCheck()
                            .expectSize()
                            .timestamp("ts")
                            .returns(
                                    PARTITIONED_EXPECTED_AFTER_INSERT
                                            + "2024-01-03T00:00:00.000000Z\tc\tk1\tp7\tv7\ts7\t7\t[7.0]\t70\n"
                            );
                } else {
                    execute(String.format(NONE_INSERT_AFTER_REPAIR, table));
                    assertNoneTable(table, NONE_EXPECTED_AFTER_INSERT);
                    assertSymbolCounts(table, isKeepSymbolMaps ? 4 : 2);
                    // the next open finds nothing to repair
                    engine.releaseAllWriters();
                    execute("INSERT INTO " + table + " VALUES ('c', 'k1', 'p7', 'v7', 's7', 7, ARRAY[7.0], 70)");
                    assertQuery("SELECT * FROM " + table)
                            .noLeakCheck()
                            .expectSize()
                            .returns(NONE_EXPECTED_AFTER_INSERT + "c\tk1\tp7\tv7\ts7\t7\t[7.0]\t70\n");
                }
                execute("DROP TABLE " + table);
            }
        });
    }

    private void assertSymbolFreeTable(String table, boolean isPartitioned, String expected) throws Exception {
        assertQuery("SELECT * FROM " + table)
                .noLeakCheck()
                .expectSize()
                .timestamp(isPartitioned ? "ts" : null)
                .returns(expected);
    }

    private void assertTruncateKilledBeforeTodoCodeKeepsTable(boolean isPartitioned) throws Exception {
        // As assertKeepSymbolMapsKilledBeforeTodoCodeKeepsTable(), for a TRUNCATE without KEEP SYMBOL MAPS. The table
        // has no SYMBOL column: such a TRUNCATE empties the symbol maps before it arms _todo_.
        setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, Files.PAGE_SIZE);
        runWithCrashFacade(() -> {
            final String table = "t";
            createSymbolFreeTable(table, isPartitioned ? " TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL" : "", false);
            engine.releaseAllReaders();
            final int partitionBy = isPartitioned ? PartitionBy.DAY : PartitionBy.NONE;
            final long txnBefore = readTxn(table, partitionBy);
            Assert.assertEquals("code in _todo_", TODO_RESTORE_META, readTodoLong(table, TODO_CODE_OFFSET));

            killTruncateBeforeTodoCode(table, truncateSql(table, false));

            Assert.assertEquals(txnBefore, readTxn(table, partitionBy));
            try (TableWriter writer = getWriter(table)) {
                Assert.assertEquals(3, writer.size());
                Assert.assertEquals("columns with y", 3, writer.getMetadata().getColumnCount());
            }
            assertSymbolFreeTable(table, isPartitioned, SYMBOL_FREE_EXPECTED_BEFORE_TRUNCATE);
            execute("INSERT INTO t VALUES ('2024-01-03T00:00:00.000000Z', 4, 40)");
            assertSymbolFreeTable(table, isPartitioned, SYMBOL_FREE_EXPECTED_AFTER_INSERT);

            // the table truncates and takes rows
            execute(truncateSql(table, false));
            assertSymbolFreeTable(table, isPartitioned, "ts\tx\ty\n");
            // the next open finds nothing to repair
            engine.releaseAllWriters();
            execute("INSERT INTO t VALUES ('2024-01-04T00:00:00.000000Z', 5, 50)");
            assertSymbolFreeTable(table, isPartitioned, "ts\tx\ty\n2024-01-04T00:00:00.000000Z\t5\t50\n");
        });
    }

    private void assertWalTableTruncateKilledBeforeTodoCodeKeepsTable(boolean isAdaptive) throws Exception {
        // A failed COPY into an existing WAL table truncates it through TableWriter.truncate(), with the pooled
        // writer that applied the WAL: an ADD COLUMN from the WAL left its code in _todo_. The writer open acts on an
        // armed entry, and so does recovery at start on a table with a save point, which adaptive commit gives it.
        setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, Files.PAGE_SIZE);
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, isAdaptive ? "adaptive" : "nosync");
        runWithCrashFacade(() -> {
            final String table = "t";
            createSymbolFreeTable(table, " TIMESTAMP(ts) PARTITION BY DAY WAL", true);
            final TableToken token = engine.verifyTableName(table);
            Assert.assertEquals("a save point", isAdaptive, hasSavePoint(token));
            Assert.assertEquals("code in _todo_", TODO_RESTORE_META, readTodoLong(table, TODO_CODE_OFFSET));

            TableWriter.setTruncateTodoCodeBarrier(() -> crashFf.takeKillImage(tableDir(table)));
            try (TableWriter writer = getWriter(token)) {
                writer.truncate();
            } finally {
                TableWriter.setTruncateTodoCodeBarrier(null);
            }
            Assert.assertTrue("TRUNCATE did not reach the store of its _todo_ code", crashFf.isKillImageTaken());
            simulateRestart(new TableToken[]{token});
            new RecoveryCoordinator(engine).recover();
            // recovery renames _meta.prev over _meta when it acts on an entry of ADD COLUMN
            Assert.assertTrue("_meta.prev of ADD COLUMN", hasMetaPrev(table));
            engine.notifyWalTxnRepublisher(token);
            drainWalQueue();

            try (TableWriter writer = getWriter(token)) {
                Assert.assertEquals(3, writer.size());
                Assert.assertEquals("columns with y", 3, writer.getMetadata().getColumnCount());
            }
            assertSymbolFreeTable(table, true, SYMBOL_FREE_EXPECTED_BEFORE_TRUNCATE);
            execute("INSERT INTO t VALUES ('2024-01-03T00:00:00.000000Z', 4, 40)");
            drainWalQueue();
            assertSymbolFreeTable(table, true, SYMBOL_FREE_EXPECTED_AFTER_INSERT);
        });
    }

    private int countTruncateKillPoints(boolean isPartitioned, boolean isKeepSymbolMaps) throws Exception {
        final String table = "count_" + (isKeepSymbolMaps ? "keep" : "plain");
        createTable(table, isPartitioned);
        engine.releaseAllReaders();
        final int before = crashFf.killPointCount();
        execute(truncateSql(table, isKeepSymbolMaps));
        final int points = crashFf.killPointCount() - before;
        execute("DROP TABLE " + table);
        return points;
    }

    /**
     * Creates a partitioned table and kills TRUNCATE of it at {@code point}, after it armed _todo_ and before it
     * published _txn. Returns the _txn txn before TRUNCATE.
     */
    private long interruptTruncate(String table, int point, boolean isKeepSymbolMaps) throws Exception {
        createPartitioned(table);
        engine.releaseAllReaders();
        final long txnBefore = readTxn(table, PartitionBy.DAY);
        crashFf.armKillAt(crashFf.killPointCount() + point, tableDir(table));
        execute(truncateSql(table, isKeepSymbolMaps));
        Assert.assertTrue(crashFf.isKillImageTaken());
        restoreKillImage(table);
        return txnBefore;
    }

    /**
     * Runs {@code truncateSql} with the process killed right before TRUNCATE stores the code of its _todo_ entry,
     * then restores the kill image, as a restart does.
     */
    private void killTruncateBeforeTodoCode(String table, String truncateSql) throws Exception {
        TableWriter.setTruncateTodoCodeBarrier(() -> crashFf.takeKillImage(tableDir(table)));
        try {
            execute(truncateSql);
        } finally {
            TableWriter.setTruncateTodoCodeBarrier(null);
        }
        Assert.assertTrue("TRUNCATE did not reach the store of its _todo_ code", crashFf.isKillImageTaken());
        restoreKillImage(table);
    }

    /**
     * Process restart after a kill: the engine lets go of the table, and its files go back to the kill image.
     */
    private void restoreKillImage(String table) {
        engine.releaseAllReaders();
        engine.releaseAllWriters();
        crashFf.crash(tableDir(table));
        // a fresh process starts with an empty scoreboard, whose max txn may be past the restored _txn
        engine.getTxnScoreboardPool().remove(engine.verifyTableName(table));
    }

    /**
     * Fails truncate() of the data file of column x while armed: TRUNCATE of a non-partitioned table fails after it
     * armed _todo_.
     */
    private static class FailingTruncateFacade extends TestFilesFacadeImpl {
        volatile boolean isArmed;
        volatile boolean isFired;
        private long fd = -1;

        @Override
        public long openRW(LPSZ name, int opts) {
            final long fd = super.openRW(name, opts);
            if (Utf8s.endsWithAscii(name, Files.SEPARATOR + "x.d")) {
                this.fd = fd;
            }
            return fd;
        }

        @Override
        public boolean truncate(long fd, long size) {
            if (isArmed && fd == this.fd) {
                isFired = true;
                return false;
            }
            return super.truncate(fd, size);
        }
    }

    /**
     * Fails, once while armed, the disk space allocation that grows a _todo_ file.
     */
    private static class TodoAllocateFailingFacade extends TestFilesFacadeImpl {
        volatile boolean isArmed;
        volatile boolean isFired;
        private volatile long todoFd = -1;

        @Override
        public boolean allocate(long fd, long size) {
            if (isArmed && fd == todoFd) {
                isArmed = false;
                isFired = true;
                return false;
            }
            return super.allocate(fd, size);
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            final long fd = super.openRW(name, opts);
            if (Utf8s.endsWithAscii(name, Files.SEPARATOR + TableUtils.TODO_FILE_NAME)) {
                todoFd = fd;
            }
            return fd;
        }
    }

    /**
     * Takes the kill image of a table directory when a remap of its _todo_ file starts, before the store that grows
     * the file lands.
     */
    private static class TodoRemapKillFacade extends CrashFaultFilesFacade {
        volatile String killTableDir;

        @Override
        public long mremap(long fd, long addr, long previousSize, long newSize, long offset, int mode, int memoryTag) {
            final String tableDir = killTableDir;
            if (tableDir != null) {
                final String path = pathOf(fd);
                if (path != null && path.endsWith(Files.SEPARATOR + TableUtils.TODO_FILE_NAME)) {
                    killTableDir = null;
                    takeKillImage(tableDir);
                }
            }
            return super.mremap(fd, addr, previousSize, newSize, offset, mode, memoryTag);
        }
    }
}
