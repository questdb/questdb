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
import io.questdb.cairo.SymbolMapReader;
import io.questdb.cairo.SymbolMapWriter;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.std.Files;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.cairo.crash.LegacyTruncateTodo.SymbolMapState;
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
    public void testRepairAtOffsetFilePageBoundary() throws Exception {
        // A writer close cuts the offset file of a symbol map to whole pages. With as many symbols as fill its first
        // page, the end offset of their chars is the last long of the file: the map holds them. An emptied map that
        // _txn counts one symbol more for has its offset file end where that end offset would start: it holds none, and
        // the repair does not read past the end of the file.
        assertMemoryLeak(() -> {
            // the end offset of the chars of this many symbols is the last long of the first page of the offset file
            final int pageSymbolCount = (int) ((Files.PAGE_SIZE - SymbolMapWriter.HEADER_SIZE) / Long.BYTES) - 1;

            createSymbolTable("t_intact", pageSymbolCount);
            final long intactNameTxn = getSymbolTableNameTxn("t_intact", "s");
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            Assert.assertEquals(
                    SymbolMapWriter.keyToOffset(pageSymbolCount) + Long.BYTES,
                    offsetFileLength("t_intact", "s", intactNameTxn)
            );
            LegacyTruncateTodo.arm(engine, "t_intact", PartitionBy.NONE, SymbolMapState.INTACT);
            execute("INSERT INTO t_intact VALUES ('s1', 1), ('new', 2)");
            assertSymbolCount("t_intact", "s", pageSymbolCount + 1);
            try (TableReader reader = getReader("t_intact")) {
                final SymbolMapReader symbols = reader.getSymbolMapReader(0);
                Assert.assertEquals(0, symbols.keyOf("s1"));
                TestUtils.assertEquals("s" + pageSymbolCount, symbols.valueOf(pageSymbolCount - 1));
                Assert.assertEquals(pageSymbolCount, symbols.keyOf("new"));
            }

            createSymbolTable("t_emptied", pageSymbolCount + 1);
            final long emptiedNameTxn = getSymbolTableNameTxn("t_emptied", "s");
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            LegacyTruncateTodo.arm(engine, "t_emptied", PartitionBy.NONE, SymbolMapState.EMPTIED_CLOSED);
            Assert.assertEquals(
                    SymbolMapWriter.keyToOffset(pageSymbolCount + 1),
                    offsetFileLength("t_emptied", "s", emptiedNameTxn)
            );
            execute("INSERT INTO t_emptied VALUES ('s1', 1), ('new', 2)");
            assertSymbolCount("t_emptied", "s", 2);
            assertQuery("SELECT * FROM t_emptied")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            s\tx
                            s1\t1
                            new\t2
                            """);
        });
    }

    @Test
    public void testRepairFailsBeforeChangeWhenSymbolMapUnreadable() throws Exception {
        // The repair reads every symbol map before it changes anything. A map it cannot read fails the writer open and
        // leaves the table as the interrupted TRUNCATE ... KEEP SYMBOL MAPS did, rather than drop the symbols of a map
        // it takes for emptied: the next open completes the repair, and keeps them.
        final UnreadableSymbolMapFacade ff = new UnreadableSymbolMapFacade();
        assertMemoryLeak(ff, () -> {
            final String table = "t";
            createPartitioned(table);
            ff.offsetFile = offsetFilePath(table, "s", getSymbolTableNameTxn(table, "s"));
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            final long txnBefore = LegacyTruncateTodo.arm(engine, table, PartitionBy.DAY, SymbolMapState.INTACT);

            for (SymbolMapFault fault : SymbolMapFault.values()) {
                ff.fault = fault;
                try {
                    execute(String.format(PARTITIONED_INSERT_AFTER_REPAIR, table));
                    Assert.fail("the repair must fail [fault=" + fault + ']');
                } catch (CairoException e) {
                    switch (fault) {
                        case OPEN -> {
                            TestUtils.assertContains(e.getFlyweightMessage(), "could not open");
                            TestUtils.assertContains(e.getFlyweightMessage(), ff.offsetFile);
                        }
                        case LENGTH -> TestUtils.assertContains(e.getFlyweightMessage(), "Checking file size failed");
                        case READ -> TestUtils.assertContains(
                                e.getFlyweightMessage(),
                                "could not read long [path=" + ff.offsetFile + ", fd="
                        );
                    }
                }
                Assert.assertNull("the fault was not injected [fault=" + fault + ']', ff.fault);
                try (TableReader reader = getReader(table)) {
                    Assert.assertEquals("the failed repair published _txn, fault " + fault, txnBefore, reader.getTxn());
                }
                assertQuery("SELECT * FROM " + table)
                        .noLeakCheck()
                        .expectSize()
                        .timestamp("ts")
                        .returns(PARTITIONED_COMMITTED_ROWS);
                engine.releaseAllReaders();
            }

            execute(String.format(PARTITIONED_INSERT_AFTER_REPAIR, table));
            assertPartitionedTable(table, PARTITIONED_EXPECTED_AFTER_INSERT);
            // KEEP SYMBOL MAPS kept the three values of each symbol column
            assertSymbolCounts(table, 4);
        });
    }

    @Test
    public void testRepairKeepSymbolMapsSurvivesEveryKillPoint() throws Exception {
        assertRepairSurvivesEveryKillPoint(true, true);
    }

    @Test
    public void testRepairKeepsEmptyStringSymbol() throws Exception {
        // The empty string is a symbol, and the shortest one: a map of '' alone, or of '' and symbols of one char,
        // holds the least chars a writer opening with its count accepts. The repair keeps their counts.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL, s2 SYMBOL, x LONG)");
            execute("INSERT INTO t VALUES ('', '', 1), ('', 'a', 2), ('', 'b', 3)");
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            LegacyTruncateTodo.arm(engine, "t", PartitionBy.NONE, SymbolMapState.INTACT);
            try (TableWriter writer = getWriter("t")) {
                Assert.assertEquals(0, writer.size());
            }
            assertSymbolCount("t", "s", 1);
            assertSymbolCount("t", "s2", 3);

            execute("INSERT INTO t VALUES ('', 'b', 4)");
            assertSymbolCount("t", "s", 1);
            assertSymbolCount("t", "s2", 3);
            assertQuery("SELECT count() FROM t WHERE s = ''")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n1\n");
            assertQuery("SELECT x FROM t WHERE s2 = 'b'")
                    .noLeakCheck()
                    .returns("x\n4\n");
        });
    }

    @Test
    public void testRepairKeepsSymbolCountOfEachIntactMap() throws Exception {
        // Each symbol map tells on its own whether it holds what _txn counts. Only s was emptied, a state that only a
        // power loss during the TRUNCATE leaves: s starts over, si keeps its symbols and their keys.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL, si SYMBOL INDEX, x LONG)");
            execute("INSERT INTO t VALUES ('a', 'k1', 1), ('b', 'k2', 2), ('c', 'k3', 3)");
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            LegacyTruncateTodo.arm(engine, "t", PartitionBy.NONE, SymbolMapState.EMPTIED_KILLED, "s");
            try (TableWriter writer = getWriter("t")) {
                Assert.assertEquals(0, writer.size());
            }
            assertSymbolCount("t", "s", 0);
            assertSymbolCount("t", "si", 3);

            execute("INSERT INTO t VALUES ('z', 'k9', 9), ('a', 'k1', 1)");
            assertSymbolCount("t", "s", 2);
            assertSymbolCount("t", "si", 4);
            assertQuery("SELECT * FROM t")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            s\tsi\tx
                            z\tk9\t9
                            a\tk1\t1
                            """);
            assertQuery("SELECT s, x FROM t WHERE si = 'k1'")
                    .noLeakCheck()
                    .returns("s\tx\na\t1\n");
            assertQuery("SELECT s, x FROM t WHERE si = 'k9'")
                    .noLeakCheck()
                    .returns("s\tx\nz\t9\n");
            assertQuery("SELECT count() FROM t WHERE si = 'k2'")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n0\n");
        });
    }

    @Test
    public void testRepairKeepsZeroCountOfNullOnlySymbolColumn() throws Exception {
        // A SYMBOL column that holds only NULL has no symbols in its map, and neither has one added after the rows: the
        // repair keeps their zero counts, and the count of the column beside them.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (sn SYMBOL, s SYMBOL, x LONG)");
            execute("INSERT INTO t VALUES (NULL, 'a', 1), (NULL, 'b', 2), (NULL, 'c', 3)");
            execute("ALTER TABLE t ADD COLUMN sa SYMBOL");
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            LegacyTruncateTodo.arm(engine, "t", PartitionBy.NONE, SymbolMapState.INTACT);
            try (TableWriter writer = getWriter("t")) {
                Assert.assertEquals(0, writer.size());
            }
            assertSymbolCount("t", "sn", 0);
            assertSymbolCount("t", "s", 3);
            assertSymbolCount("t", "sa", 0);

            execute("INSERT INTO t VALUES ('q', 'd', 4, 'q')");
            assertSymbolCount("t", "sn", 1);
            assertSymbolCount("t", "s", 4);
            assertSymbolCount("t", "sa", 1);
            assertQuery("SELECT * FROM t")
                    .noLeakCheck()
                    .expectSize()
                    .returns("sn\ts\tx\tsa\nq\td\t4\tq\n");
        });
    }

    @Test
    public void testRepairOfKeepSymbolMapsTruncateAttachesDetachedPartition() throws Exception {
        // The workflow KEEP SYMBOL MAPS is for: a partition detached before the TRUNCATE attaches after it with its
        // symbols, which the maps keep. An earlier build armed the same _todo_ entry for it as for a TRUNCATE without
        // KEEP SYMBOL MAPS, and the writer open that completes the TRUNCATE keeps the maps it finds intact, whether
        // new symbols arrive before the ATTACH or not. The same workflow with a TRUNCATE that ran to the end is the
        // reference.
        assertMemoryLeak(() -> {
            for (int i = 0; i < 2; i++) {
                final boolean isInsertBeforeAttach = i == 1;
                final String table = "t_" + i;
                final String ref = "ref_" + i;
                createTableWithDetachedPartition(table);
                createTableWithDetachedPartition(ref);
                engine.releaseAllReaders();
                engine.releaseAllWriters();
                LegacyTruncateTodo.arm(engine, table, PartitionBy.DAY, SymbolMapState.INTACT);
                execute("TRUNCATE TABLE " + ref + " KEEP SYMBOL MAPS");
                for (String name : new String[]{table, ref}) {
                    makeDetachedPartitionAttachable(name, "2024-01-01");
                    if (isInsertBeforeAttach) {
                        execute("INSERT INTO " + name + " VALUES ('2024-01-03T00:00:00.000000Z', 'x', 'k9', 'n9')");
                    }
                    execute("ALTER TABLE " + name + " ATTACH PARTITION LIST '2024-01-01'");
                }

                assertQuery("SELECT * FROM " + table)
                        .noLeakCheck()
                        .expectSize()
                        .timestamp("ts")
                        .returns(
                                """
                                        ts\ts\tsi\tsn
                                        2024-01-01T00:00:00.000000Z\ta\tk1\t\u00e9
                                        2024-01-01T01:00:00.000000Z\tb\tk2\t\u20ac
                                        """
                                        + (isInsertBeforeAttach ? "2024-01-03T00:00:00.000000Z\tx\tk9\tn9\n" : "")
                        );
                assertSqlCursors("SELECT * FROM " + ref, "SELECT * FROM " + table);
                assertQuery("SELECT ts FROM " + table + " WHERE si = 'k1'")
                        .noLeakCheck()
                        .timestamp("ts")
                        .returns("ts\n2024-01-01T00:00:00.000000Z\n");
                assertQuery("SELECT ts FROM " + table + " WHERE si = 'k9'")
                        .noLeakCheck()
                        .timestamp("ts")
                        .returns("ts\n" + (isInsertBeforeAttach ? "2024-01-03T00:00:00.000000Z\n" : ""));
                assertQuery("SELECT ts FROM " + table + " WHERE s = 'a'")
                        .noLeakCheck()
                        .timestamp("ts")
                        .returns("ts\n2024-01-01T00:00:00.000000Z\n");
                assertQuery("SELECT ts FROM " + table + " WHERE sn = '\u20ac'")
                        .noLeakCheck()
                        .timestamp("ts")
                        .returns("ts\n2024-01-01T01:00:00.000000Z\n");
                for (String column : new String[]{"s", "si", "sn"}) {
                    // a, b and c, and x with the INSERT; likewise in si and sn
                    assertSymbolCount(table, column, isInsertBeforeAttach ? 4 : 3);
                }
            }
        });
    }

    @Test
    public void testRepairOfKeepSymbolMapsTruncateOfTableConvertedToWal() throws Exception {
        // A table converts to WAL with the entry of an interrupted TRUNCATE ... KEEP SYMBOL MAPS armed: the conversion
        // needs only a reader, and keeps the _txn txn the entry is stamped with. The WAL writer resolves the symbols of
        // its rows in the maps the TRUNCATE kept, before the writer open of the apply job completes the TRUNCATE.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, s SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO t VALUES
                        ('2024-01-01T00:00:00.000000Z', 'a', 1),
                        ('2024-01-01T01:00:00.000000Z', 'b', 2),
                        ('2024-01-02T00:00:00.000000Z', 'c', 3)""");
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            LegacyTruncateTodo.arm(engine, "t", PartitionBy.DAY, SymbolMapState.INTACT);
            execute("ALTER TABLE t SET TYPE WAL");
            // the restart converts the table
            engine.releaseInactive();
            engine.load();
            Assert.assertTrue(engine.verifyTableName("t").isWal());

            execute("""
                    INSERT INTO t VALUES
                        ('2024-01-03T00:00:00.000000Z', 'a', 4),
                        ('2024-01-03T01:00:00.000000Z', 'x', 5)""");
            drainWalQueue();
            assertQuery("SELECT * FROM t")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            ts\ts\tx
                            2024-01-03T00:00:00.000000Z\ta\t4
                            2024-01-03T01:00:00.000000Z\tx\t5
                            """);
            assertQuery("SELECT x FROM t WHERE s = 'a'")
                    .noLeakCheck()
                    .returns("x\n4\n");
            // a, b, c and x
            assertSymbolCount("t", "s", 4);
        });
    }

    @Test
    public void testRepairOfLargeSymbolMaps() throws Exception {
        // 10_000 symbols span pages of the offset file. The map emptied with its writer open keeps the size the empty
        // map was given, and the closed one a single page: in both the offset file ends before the end offset of
        // 10_000 symbols.
        assertMemoryLeak(() -> {
            for (SymbolMapState state : SymbolMapState.values()) {
                final boolean isIntact = state == SymbolMapState.INTACT;
                final String table = "t_" + state.ordinal();
                createSymbolTable(table, 10_000);
                engine.releaseAllReaders();
                engine.releaseAllWriters();
                LegacyTruncateTodo.arm(engine, table, PartitionBy.NONE, state);
                try (TableWriter writer = getWriter(table)) {
                    Assert.assertEquals(state.name(), 0, writer.size());
                }
                assertSymbolCount(table, "s", isIntact ? 10_000 : 0);

                execute("INSERT INTO " + table + " VALUES ('s5', 5), ('new', 10_001)");
                assertSymbolCount(table, "s", isIntact ? 10_001 : 2);
                try (TableReader reader = getReader(table)) {
                    final SymbolMapReader symbols = reader.getSymbolMapReader(0);
                    Assert.assertEquals(state.name(), isIntact ? 4 : 0, symbols.keyOf("s5"));
                    Assert.assertEquals(state.name(), isIntact ? 10_000 : 1, symbols.keyOf("new"));
                    if (isIntact) {
                        TestUtils.assertEquals("s10000", symbols.valueOf(9_999));
                    }
                }
                assertQuery("SELECT * FROM " + table)
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                s\tx
                                s5\t5
                                new\t10001
                                """);
            }
        });
    }

    @Test
    public void testRepairReadsSymbolMapsUnderTheirNameTxn() throws Exception {
        // The maps of a renamed column and of one whose capacity changed live under a name txn, and a dropped SYMBOL
        // column leaves no _txn count: the repair reads each map that the writer then opens, with its count. The
        // columns hold 1, 2 and 3 symbols, so that a count read for the wrong column shows.
        assertMemoryLeak(() -> {
            for (SymbolMapState state : new SymbolMapState[]{SymbolMapState.INTACT, SymbolMapState.EMPTIED_KILLED}) {
                final boolean isIntact = state == SymbolMapState.INTACT;
                final String table = "t_" + state.ordinal();
                execute("CREATE TABLE " + table + " (s0 SYMBOL, s1 SYMBOL, s2 SYMBOL INDEX, x LONG)");
                execute("""
                        INSERT INTO %s VALUES
                            ('a', 'b1', 'c1', 1),
                            ('a', 'b2', 'c2', 2),
                            ('a', 'b1', 'c3', 3)""".formatted(table));
                execute("ALTER TABLE " + table + " DROP COLUMN s0");
                execute("ALTER TABLE " + table + " RENAME COLUMN s1 TO s1r");
                execute("ALTER TABLE " + table + " ALTER COLUMN s2 SYMBOL CAPACITY 512");
                Assert.assertNotEquals(TableUtils.COLUMN_NAME_TXN_NONE, getSymbolTableNameTxn(table, "s1r"));
                Assert.assertNotEquals(TableUtils.COLUMN_NAME_TXN_NONE, getSymbolTableNameTxn(table, "s2"));
                engine.releaseAllReaders();
                engine.releaseAllWriters();
                LegacyTruncateTodo.arm(engine, table, PartitionBy.NONE, state);
                try (TableWriter writer = getWriter(table)) {
                    Assert.assertEquals(state.name(), 0, writer.size());
                }
                assertSymbolCount(table, "s1r", isIntact ? 2 : 0);
                assertSymbolCount(table, "s2", isIntact ? 3 : 0);

                execute("INSERT INTO " + table + " VALUES ('b2', 'c9', 9)");
                assertSymbolCount(table, "s1r", isIntact ? 2 : 1);
                assertSymbolCount(table, "s2", isIntact ? 4 : 1);
                assertQuery("SELECT * FROM " + table)
                        .noLeakCheck()
                        .expectSize()
                        .returns("s1r\ts2\tx\nb2\tc9\t9\n");
                assertQuery("SELECT s1r, x FROM " + table + " WHERE s2 = 'c9'")
                        .noLeakCheck()
                        .returns("s1r\tx\nb2\t9\n");
            }
        });
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
            LegacyTruncateTodo.arm(engine, table, PartitionBy.NONE, SymbolMapState.EMPTIED_KILLED);

            execute(String.format(NONE_INSERT_AFTER_REPAIR, table));
            assertSymbolCounts(table, 2);
            assertNoneTable(table, NONE_EXPECTED_AFTER_INSERT);
        });
    }

    /**
     * Creates a table and arms the _todo_ entry of a TRUNCATE that an earlier build interrupted before it published
     * _txn. Without KEEP SYMBOL MAPS, the process died with the symbol maps emptied. Returns the _txn txn the entry is
     * stamped with.
     */
    private static long armInterruptedTruncate(String table, boolean isPartitioned, boolean isKeepSymbolMaps) throws Exception {
        createTable(table, isPartitioned);
        engine.releaseAllReaders();
        engine.releaseAllWriters();
        return LegacyTruncateTodo.arm(
                engine,
                table,
                isPartitioned ? PartitionBy.DAY : PartitionBy.NONE,
                isKeepSymbolMaps ? SymbolMapState.INTACT : SymbolMapState.EMPTIED_KILLED
        );
    }

    private static void assertSymbolCount(String table, String column, int expected) {
        try (TableReader reader = getReader(table)) {
            final int columnIndex = reader.getMetadata().getColumnIndex(column);
            Assert.assertEquals(column, expected, reader.getSymbolMapReader(columnIndex).getSymbolCount());
        }
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

    // the SYMBOL column s holds the symbols s1 to s<symbolCount>, in this order
    private static void createSymbolTable(String table, int symbolCount) throws Exception {
        execute("CREATE TABLE " + table + " (s SYMBOL, x LONG)");
        execute("INSERT INTO " + table + " SELECT 's' || x, x FROM long_sequence(" + symbolCount + ")");
    }

    private static void createTable(String table, boolean isPartitioned) throws Exception {
        if (isPartitioned) {
            createPartitioned(table);
        } else {
            createNonPartitioned(table);
        }
    }

    // a cached, an indexed and an uncached SYMBOL column, and the partition 2024-01-01 detached
    private static void createTableWithDetachedPartition(String table) throws Exception {
        execute("""
                CREATE TABLE %s (ts TIMESTAMP, s SYMBOL, si SYMBOL INDEX, sn SYMBOL NOCACHE)
                TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL""".formatted(table));
        execute("""
                INSERT INTO %s VALUES
                    ('2024-01-01T00:00:00.000000Z', 'a', 'k1', '\u00e9'),
                    ('2024-01-01T01:00:00.000000Z', 'b', 'k2', '\u20ac'),
                    ('2024-01-02T00:00:00.000000Z', 'c', 'k3', '\ud83d\ude00')""".formatted(table));
        execute("ALTER TABLE " + table + " DETACH PARTITION LIST '2024-01-01'");
    }

    private static long getSymbolTableNameTxn(String table, String column) {
        try (TableReader reader = getReader(table)) {
            final int writerIndex = reader.getMetadata().getWriterIndex(reader.getMetadata().getColumnIndex(column));
            return reader.getColumnVersionReader().getSymbolTableNameTxn(writerIndex);
        }
    }

    private static void makeDetachedPartitionAttachable(String table, String partition) {
        final TableToken token = engine.verifyTableName(table);
        try (Path detached = new Path(); Path attachable = new Path()) {
            detached.of(root).concat(token).concat(partition).put(TableUtils.DETACHED_DIR_MARKER);
            attachable.of(root).concat(token).concat(partition).put(configuration.getAttachPartitionSuffix());
            final int result = TestFilesFacadeImpl.INSTANCE.rename(detached.$(), attachable.$());
            Assert.assertEquals(Files.FILES_RENAME_OK, result);
        }
    }

    private static long offsetFileLength(String table, String column, long symbolTableNameTxn) {
        try (Path path = new Path().of(root).concat(engine.verifyTableName(table))) {
            return TestFilesFacadeImpl.INSTANCE.length(TableUtils.offsetFileName(path, column, symbolTableNameTxn));
        }
    }

    private static String offsetFilePath(String table, String column, long symbolTableNameTxn) {
        try (Path path = new Path().of(root).concat(engine.verifyTableName(table))) {
            TableUtils.offsetFileName(path, column, symbolTableNameTxn);
            return path.toString();
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

    // a failure of the truncate repair to read the offset file of a symbol map
    private enum SymbolMapFault {
        OPEN, LENGTH, READ
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

    /**
     * Fails, once armed, the next read-only open of the offset file of one symbol map, or the length or the read of the
     * fd that open returns, and disarms. While armed, only the truncate repair of a writer open opens the file
     * read-only.
     */
    private static class UnreadableSymbolMapFacade extends TestFilesFacadeImpl {
        volatile SymbolMapFault fault;
        volatile String offsetFile;
        private long offsetFd = -1;

        @Override
        public long length(long fd) {
            if (fault == SymbolMapFault.LENGTH && fd == offsetFd) {
                fault = null;
                // what FilesFacadeImpl.length() throws when fstat fails
                throw CairoException.critical(5).put("Checking file size failed");
            }
            return super.length(fd);
        }

        @Override
        public long openRO(LPSZ name) {
            if (fault != null && Utf8s.equalsAscii(offsetFile, name)) {
                if (fault == SymbolMapFault.OPEN) {
                    fault = null;
                    return -1;
                }
                offsetFd = super.openRO(name);
                return offsetFd;
            }
            return super.openRO(name);
        }

        @Override
        public long read(long fd, long buf, long len, long offset) {
            if (fault == SymbolMapFault.READ && fd == offsetFd) {
                fault = null;
                return -1;
            }
            return super.read(fd, buf, len, offset);
        }
    }
}
