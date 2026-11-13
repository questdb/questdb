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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.O3PartitionPurgeJob;
import io.questdb.cairo.PartitionBy;
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
 * completes it: the table must end up empty, with the symbol maps the TRUNCATE asked for, and take new rows.
 */
public class TruncateRepairTest extends AbstractCrashConsistencyTest {
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
    public void testNonPartitionedKeepSymbolMapsSurvivesEveryKillPoint() throws Exception {
        assertTruncateSurvivesEveryKillPoint(false, true);
    }

    @Test
    public void testNonPartitionedSurvivesEveryKillPoint() throws Exception {
        assertTruncateSurvivesEveryKillPoint(false, false);
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

    private static void createTable(String table, boolean isPartitioned) throws Exception {
        if (isPartitioned) {
            createPartitioned(table);
        } else {
            createNonPartitioned(table);
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
}
