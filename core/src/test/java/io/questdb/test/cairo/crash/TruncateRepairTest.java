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
import io.questdb.cairo.O3PartitionPurgeJob;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.std.Files;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8s;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.stream.Stream;

/**
 * A non-WAL TRUNCATE publishes the empty {@code _txn} before it empties anything. When it stops on an I/O error or a
 * process kill, the table is either as it was or empty, with the symbol maps the TRUNCATE asked for, and takes new rows.
 * A TRUNCATE of an earlier build armed a {@code _todo_} entry before it removed rows: the writer open that finds one
 * still completes that TRUNCATE.
 */
public class TruncateRepairTest extends AbstractCrashConsistencyTest {
    // the rows createNonPartitioned() writes
    private static final String NONE_COMMITTED_ROWS = """
            s\tsi\tsp\tv\tstr\tx\tarr\ty
            a\tk1\tp1\tv1\ts1\t101\t[1.0]\tnull
            b\tk2\tp2\tv2\ts2\t102\t[2.0]\tnull
            c\tk3\tp3\tv3\ts3\t103\t[3.0]\t30
            """;
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
    private static final String NONE_HEADER = "s\tsi\tsp\tv\tstr\tx\tarr\ty\n";
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
    // the rows createPartitioned() writes
    private static final String PARTITIONED_COMMITTED_ROWS = """
            ts\ts\tsi\tsp\tv\tstr\tx\tarr\ty
            2024-01-01T00:00:00.000000Z\ta\tk1\tp1\tv1\ts1\t101\t[1.0]\tnull
            2024-01-02T00:00:00.000000Z\tb\tk2\tp2\tv2\ts2\t2\t[2.0]\tnull
            2024-01-02T01:00:00.000000Z\tc\tk1\tp3\tv3\ts3\t3\t[3.0]\t30
            2024-01-03T00:00:00.000000Z\ta\tk3\tp1\tv4\ts4\t4\t[4.0]\t40
            """;
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
    private static final String PARTITIONED_HEADER = "ts\ts\tsi\tsp\tv\tstr\tx\tarr\ty\n";
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
    public void testFailedTruncateLeavesEmptyTable() throws Exception {
        // TRUNCATE stops on an I/O error in truncateColumns(), which runs once it has published the empty _txn: the
        // table is empty before any writer opens it again, whatever the number of commits before it.
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
                        TestUtils.assertContains(e.getFlyweightMessage(), "Data has been persisted, but we could not perform housekeeping");
                        TestUtils.assertContains(e.getFlyweightMessage(), "Cannot truncate fd=");
                    } finally {
                        ff.isArmed = false;
                    }
                    Assert.assertTrue("the failure was not injected", ff.isFired);
                    ff.isFired = false;
                    assertQuery("SELECT * FROM " + table)
                            .noLeakCheck()
                            .expectSize()
                            .returns("s\tsi\tsp\tv\tstr\tx\tarr\ty\n");
                    // KEEP SYMBOL MAPS keeps the three values of each symbol column
                    assertSymbolCounts(table, isKeepSymbolMaps ? 3 : 0);
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
    public void testNonPartitionedRepairKeepSymbolMapsSurvivesEveryKillPoint() throws Exception {
        assertRepairSurvivesEveryKillPoint(false, true);
    }

    @Test
    public void testNonPartitionedRepairSurvivesEveryKillPoint() throws Exception {
        assertRepairSurvivesEveryKillPoint(false, false);
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
        assertRepairSurvivesEveryKillPoint(true, true);
    }

    @Test
    public void testRepairSurvivesEveryKillPoint() throws Exception {
        assertRepairSurvivesEveryKillPoint(true, false);
    }

    @Test
    public void testRepairWaitsForReaderOfOldRows() throws Exception {
        // A query reads the rows of a TRUNCATE ... KEEP SYMBOL MAPS that an earlier build interrupted while the writer
        // open repairs the table and new rows arrive: the repair leaves the partition versions the query holds to the
        // purge job, and the new symbols leave the symbol keys of the old rows alone. That build emptied the symbol
        // maps of a TRUNCATE without KEEP SYMBOL MAPS before it armed _todo_, so a query of its interrupted state
        // could not resolve the old keys even before the repair. A non-partitioned table is covered by
        // TruncateRepairOpenQueryTest.
        setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, Files.PAGE_SIZE);
        runWithCrashFacade(() -> {
            final String table = "t";
            armInterruptedTruncate(table, true, true);
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
                            PARTITIONED_COMMITTED_ROWS,
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
            LegacyTruncateTodo.arm(engine, table, PartitionBy.NONE, false);

            execute(String.format(NONE_INSERT_AFTER_REPAIR, table));
            assertSymbolCounts(table, 2);
            assertNoneTable(table, NONE_EXPECTED_AFTER_INSERT);
        });
    }

    /**
     * Creates a table and arms the _todo_ entry of a TRUNCATE that an earlier build interrupted before it published
     * _txn. Returns the _txn txn the entry is stamped with.
     */
    private static long armInterruptedTruncate(String table, boolean isPartitioned, boolean isKeepSymbolMaps) throws Exception {
        createTable(table, isPartitioned);
        engine.releaseAllReaders();
        engine.releaseAllWriters();
        return LegacyTruncateTodo.arm(engine, table, isPartitioned ? PartitionBy.DAY : PartitionBy.NONE, isKeepSymbolMaps);
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

    private static String tableDir(String table) {
        return root + Files.SEPARATOR + engine.verifyTableName(table).getDirName();
    }

    private static String truncateSql(String table, boolean isKeepSymbolMaps) {
        return "TRUNCATE TABLE " + table + (isKeepSymbolMaps ? " KEEP SYMBOL MAPS" : "");
    }

    // Asserts that a query reads the table either as createTable() wrote it or empty, and tells whether it is as it was
    private boolean assertAsItWasOrEmpty(String table, boolean isPartitioned, int k) throws Exception {
        final boolean isEmpty = "count\n0\n".equals(TestUtils.printSqlToString(engine, sqlExecutionContext, "SELECT count() FROM " + table, sink));
        try {
            if (isPartitioned) {
                assertQuery("SELECT * FROM " + table)
                        .noLeakCheck()
                        .expectSize()
                        .timestamp("ts")
                        .returns(isEmpty ? PARTITIONED_HEADER : PARTITIONED_COMMITTED_ROWS);
            } else {
                assertQuery("SELECT * FROM " + table)
                        .noLeakCheck()
                        .expectSize()
                        .returns(isEmpty ? NONE_HEADER : NONE_COMMITTED_ROWS);
            }
        } catch (AssertionError e) {
            throw new AssertionError("kill point " + k, e);
        }
        return !isEmpty;
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

    private void assertRepairSurvivesEveryKillPoint(boolean isPartitioned, boolean isKeepSymbolMaps) throws Exception {
        // A kill during the writer open that completes a TRUNCATE an earlier build interrupted: the open after it must
        // finish the repair.
        setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, Files.PAGE_SIZE);
        runWithCrashFacade(() -> {
            final String countTable = "count_repair";
            armInterruptedTruncate(countTable, isPartitioned, isKeepSymbolMaps);
            final int before = crashFf.killPointCount();
            getWriter(countTable).close();
            final int repairPoints = crashFf.killPointCount() - before;
            Assert.assertTrue("the repair has no kill points", repairPoints > 0);
            execute("DROP TABLE " + countTable);

            for (int k = 1; k <= repairPoints + 1; k++) {
                final String table = "t_" + k;
                final long txnBefore = armInterruptedTruncate(table, isPartitioned, isKeepSymbolMaps);
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
                    Assert.assertEquals("kill point " + k, txnBefore + 1, writer.getTxn());
                }
                assertSymbolCounts(table, isKeepSymbolMaps ? 3 : 0);
                if (isPartitioned) {
                    execute(String.format(PARTITIONED_INSERT_AFTER_REPAIR, table));
                    assertPartitionedTable(table, PARTITIONED_EXPECTED_AFTER_INSERT);
                } else {
                    execute(String.format(NONE_INSERT_AFTER_REPAIR, table));
                    assertNoneTable(table, NONE_EXPECTED_AFTER_INSERT);
                }
                assertSymbolCounts(table, isKeepSymbolMaps ? 4 : 2);
                execute("DROP TABLE " + table);
            }
        });
    }

    private void assertTruncateSurvivesEveryKillPoint(boolean isPartitioned, boolean isKeepSymbolMaps) throws Exception {
        setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, Files.PAGE_SIZE);
        final SymbolMapTruncateCrashFacade ff = new SymbolMapTruncateCrashFacade();
        runWithCrashFacade(ff, () -> {
            // the kill points of one TRUNCATE, and the ones SymbolMapWriter.truncate() takes among them
            final String countTable = "count_truncate";
            createTable(countTable, isPartitioned);
            engine.releaseAllReaders();
            final int base = ff.killPointCount();
            ff.startRecording();
            execute(truncateSql(countTable, isKeepSymbolMaps));
            ff.stopRecording();
            final int points = ff.killPointCount() - base;
            execute("DROP TABLE " + countTable);
            Assert.assertTrue("TRUNCATE has no kill points", points > 0);
            // the sweep reaches the emptying of the symbol maps, which TRUNCATE without KEEP SYMBOL MAPS does
            Assert.assertEquals("emptying the symbol maps", !isKeepSymbolMaps, ff.symbolMapTruncatePointCount() > 0);

            for (int k = 1; k <= points + 1; k++) {
                final String table = "t_" + k;
                createTable(table, isPartitioned);
                engine.releaseAllReaders();
                if (k <= points) {
                    ff.armKillAt(ff.killPointCount() + k, tableDir(table));
                }
                execute(truncateSql(table, isKeepSymbolMaps));
                if (k > points) {
                    ff.takeKillImage(tableDir(table));
                }
                Assert.assertTrue("kill point " + k + " was not reached", ff.isKillImageTaken());
                restoreKillImage(table);

                // Before any writer opens the table again, a query reads it as it was or empty, and the writer open
                // completes nothing. KEEP SYMBOL MAPS keeps the three values of each symbol column either way.
                final boolean isAsItWas = assertAsItWasOrEmpty(table, isPartitioned, k);
                getWriter(table).close();
                Assert.assertEquals("kill point " + k, isAsItWas, assertAsItWasOrEmpty(table, isPartitioned, k));
                assertSymbolCounts(table, isAsItWas || isKeepSymbolMaps ? 3 : 0);
                if (isAsItWas) {
                    // TRUNCATE runs again, to the end
                    execute(truncateSql(table, isKeepSymbolMaps));
                    assertSymbolCounts(table, isKeepSymbolMaps ? 3 : 0);
                }

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
     * published the empty _txn.
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
