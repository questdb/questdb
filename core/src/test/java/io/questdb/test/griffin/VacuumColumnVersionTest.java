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

package io.questdb.test.griffin;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnPurgeJob;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.Os;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;

public class VacuumColumnVersionTest extends AbstractCairoTest {
    private static final String[] ROGUE_SYMBOL_FILES = {"rog_sym.c", "rog_sym.o", "rog_sym.k", "rog_sym.v"};
    private static final String[] TABLE_ROOT = {"."};
    private int iteration;

    @BeforeClass
    public static void setUpStatic() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_COLUMN_PURGE_QUEUE_CAPACITY, 2);
        AbstractCairoTest.setUpStatic();
    }

    @Before
    public void setUp() {
        iteration = 1;
        setCurrentMicros(0);
        node1.setProperty(PropertyKey.CAIRO_SQL_COLUMN_PURGE_RETRY_DELAY, 1);
        node1.setProperty(PropertyKey.CAIRO_SQL_COLUMN_PURGE_QUEUE_CAPACITY, 2);
        super.setUp();
    }

    @Test
    public void testVacuumColumnIndexDropped() throws Exception {
        Assume.assumeFalse(Os.isWindows());
        FilesFacade ff = engine.getConfiguration().getFilesFacade();
        assertMemoryLeak(() -> {
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                execute(
                        "create table testPurge as" +
                                " (select timestamp_sequence('1970-01-01', 24 * 60 * 60 * 1000000L) ts," +
                                " x," +
                                " rnd_str('a', 'b', 'c', 'd') str," +
                                " rnd_symbol('A', 'B', 'C', 'D') sym1," +
                                " rnd_symbol('1', '2', '3', '4') sym2" +
                                " from long_sequence(5)), index(sym2)" +
                                " timestamp(ts) PARTITION BY DAY"
                );

                execute("checkpoint create");
                execute("alter table testPurge alter column sym2 drop index");

                purgeJob.run();
                String[] partitions = new String[]{"1970-01-01", "1970-01-02", "1970-01-03", "1970-01-04", "1970-01-05"};
                String[] files = {"sym2.k", "sym2.v"};
                assertFilesExist(partitions, "testPurge", files, "", true);

                execute("checkpoint release");

                runPurgeJob(purgeJob);

                assertFilesExist(partitions, "testPurge", files, "", false);

                Path path = Path.getThreadLocal(configuration.getDbRoot());
                path.concat(engine.verifyTableName("testPurge"));
                int pathLen = path.size();
                for (String partition : partitions) {
                    for (String file : files) {
                        path.trimTo(pathLen).concat(partition).concat(file).$();
                        ff.touch(path.$());
                    }
                }

                assertFilesExist(partitions, "testPurge", files, "", true);

                runTableVacuum("testPurge");
                runPurgeJob(purgeJob);

                assertFilesExist(partitions, "testPurge", files, "", false);
            }
        });
    }

    @Test
    public void testVacuumErrorWhenCheckpointInProgress() throws Exception {
        Assume.assumeFalse(Os.isWindows());
        assertMemoryLeak(() -> {
            execute(
                    "create table testPurge as" +
                            " (select timestamp_sequence('1970-01-01', 24 * 60 * 60 * 1000000L) ts," +
                            " x," +
                            " rnd_str('a', 'b', 'c', 'd') str," +
                            " rnd_symbol('A', 'B', 'C', 'D') sym1," +
                            " rnd_symbol('1', '2', '3', '4') sym2" +
                            " from long_sequence(5)), index(sym2)" +
                            " timestamp(ts) PARTITION BY DAY"
            );

            execute("checkpoint create");
            try {
                runTableVacuum("testPurge");
                Assert.fail();
            } catch (CairoException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(),
                        "cannot vacuum while checkpoint is in progress");
            } finally {
                execute("checkpoint release");
            }
        });
    }

    @Test
    public void testVacuumInMiddleOfUpdate() throws Exception {
        AtomicReference<ColumnPurgeJob> purgeJobInstance = new AtomicReference<>();

        FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (purgeJobInstance.get() != null && (Utf8s.endsWithAscii(name, "/1970-01-05/sym1.d.2") || Utf8s.endsWithAscii(name, "\\1970-01-05\\sym1.d.2"))) {
                    try {
                        runTableVacuum("testPurge");
                        runPurgeJob(purgeJobInstance.get());
                    } catch (SqlException e) {
                        throw new RuntimeException(e);
                    }
                }
                return super.openRW(name, opts);
            }
        };
        assertMemoryLeak(ff, () -> {
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                execute(
                        "create table testPurge as" +
                                " (select timestamp_sequence('1970-01-01', 24 * 60 * 60 * 1000000L) ts," +
                                " x," +
                                " rnd_str('a', 'b', 'c', 'd') str," +
                                " rnd_symbol('A', 'B', 'C', 'D') sym1," +
                                " rnd_symbol('1', '2', '3', '4') sym2" +
                                " from long_sequence(5)), index(sym2)" +
                                " timestamp(ts) PARTITION BY DAY"
                );
                execute("alter table testPurge drop column x");

                purgeJobInstance.set(purgeJob);
                update("UPDATE testPurge SET sym1='123'");

                assertQuery("testPurge")
                        .noLeakCheck()
                        .expectSize()
                        .timestamp("ts")
                        .returns("""
                                ts\tstr\tsym1\tsym2
                                1970-01-01T00:00:00.000000Z\ta\t123\t2
                                1970-01-02T00:00:00.000000Z\td\t123\t4
                                1970-01-03T00:00:00.000000Z\tc\t123\t3
                                1970-01-04T00:00:00.000000Z\ta\t123\t1
                                1970-01-05T00:00:00.000000Z\tc\t123\t2
                                """);

                String[] partitions = new String[]{"1970-01-01", "1970-01-02", "1970-01-03", "1970-01-04", "1970-01-05"};
                String[] files = {"sym1.d"};
                assertFilesExist(partitions, "testPurge", files, "", false);
            }
        });
    }

    @Test
    public void testVacuumKeepsCoveringIndexOfNameTxnZero() throws Exception {
        // A column added before the first commit has name txn 0, which the cover file names cannot tell apart from
        // no name txn. VACUUM must take the index's cover files for the live version's.
        assertVacuumKeepsCoveringIndex("t_vac_cov_txn0", "sym", "sym\\.pc\\d+\\.0\\.\\d+");
    }

    @Test
    public void testVacuumKeepsCoveringIndexOfRenamedIndexColumn() throws Exception {
        // RENAME moves the index column to a name txn above 0, which its posting file names carry before the seal txn.
        assertVacuumKeepsCoveringIndex("t_vac_cov_renamed", "s", "s\\.pc\\d+\\.[1-9]\\d*\\.\\d+");
    }

    @Test
    public void testVacuumPurgesColumnVersionsAfterColumnDrop() throws Exception {
        assertMemoryLeak(() -> {
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                execute(
                        "create table testPurge as" +
                                " (select timestamp_sequence('1970-01-01', 24 * 60 * 60 * 1000000L) ts," +
                                " x," +
                                " rnd_str('a', 'b', 'c', 'd') str," +
                                " rnd_symbol('A', 'B', 'C', 'D') sym1," +
                                " rnd_symbol('1', '2', '3', '4') sym2" +
                                " from long_sequence(5)), index(sym2)" +
                                " timestamp(ts) PARTITION BY DAY"
                );
                execute("alter table testPurge drop column x");

                try (TableReader rdr = getReader("testPurge")) {
                    update("UPDATE testPurge SET sym1='123', str='abcd', sym2='EE' WHERE ts >= '1970-01-02'");
                    rdr.openPartition(0);
                }

                runPurgeJob(purgeJob);

                String[] partitions = new String[]{"1970-01-02", "1970-01-03", "1970-01-04"};
                String[] files = {"sym2.d", "sym2.k", "sym2.v"};
                assertFilesExist(partitions, "testPurge", files, "", true);

                runTableVacuum("testPurge");
                assertFilesExist(partitions, "testPurge", files, "", false);
                Assert.assertEquals(0, purgeJob.getOutstandingPurgeTasks());
            }
        });
    }

    @Test
    public void testVacuumPurgesColumnVersionsAsync() throws Exception {
        assertMemoryLeak(() -> {
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                execute(
                        "create table testPurge as" +
                                " (select timestamp_sequence('1970-01-01', 24 * 60 * 60 * 1000000L) ts," +
                                " x," +
                                " rnd_str('a', 'b', 'c', 'd') str," +
                                " rnd_symbol('A', 'B', 'C', 'D') sym1," +
                                " rnd_symbol('1', '2', '3', '4') sym2" +
                                " from long_sequence(5)), index(sym2)" +
                                " timestamp(ts) PARTITION BY DAY"
                );
                String[] partitions = new String[]{"1970-01-02", "1970-01-03", "1970-01-04"};
                String[] files = {"sym2.d", "sym2.k", "sym2.v"};

                try (TableReader rdr = getReader("testPurge")) {
                    update("UPDATE testPurge SET x = 100, sym2='EE' WHERE ts >= '1970-01-02'");
                    runPurgeJob(purgeJob);

                    // Open reader does not allow to delete the files
                    runTableVacuum("testPurge");
                    rdr.openPartition(0);
                }

                assertFilesExist(partitions, "testPurge", files, "", true);
                runPurgeJob(purgeJob);
                assertFilesExist(partitions, "testPurge", files, "", false);

                Assert.assertEquals(0, purgeJob.getOutstandingPurgeTasks());
            }
        });
    }

    @Test
    public void testVacuumPurgesStalePostingFilesOfNameTxnZero() throws Exception {
        // UPDATE of a covered column moves the index column off name txn 0. Posting files left behind under name txn
        // 0 are stale, and VACUUM must purge them, the cover files too, which name txn 0 like no name txn.
        Assume.assumeFalse(Os.isWindows());
        assertMemoryLeak(() -> {
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                createIndexWithNameTxnZeroTable("t_vac_stale_txn0");
                engine.releaseAllReaders();
                execute("UPDATE t_vac_stale_txn0 SET val = val + 100 WHERE ts IN '2024-01-01'");
                final File partitionDir = partitionDir("t_vac_stale_txn0", "2024-01-01");
                final String[] stale = {"sym.d.0", "sym.pk.0", "sym.pci.0", "sym.pv.0.0", "sym.pv.0.1", "sym.pc0.0.0", "sym.pc1.0.1"};
                for (String name : stale) {
                    Assert.assertFalse("fixture: the update purged " + name, new File(partitionDir, name).exists());
                    Assert.assertTrue(TestFilesFacadeImpl.INSTANCE.touch(Path.getThreadLocal(new File(partitionDir, name).getAbsolutePath()).$()));
                }

                runTableVacuum("t_vac_stale_txn0");
                runPurgeJob(purgeJob);

                for (String name : stale) {
                    Assert.assertFalse("stale file purged: " + name, new File(partitionDir, name).exists());
                }
                assertCoveringQuery("t_vac_stale_txn0", "sym", """
                        sym\tval\tts
                        A\t101.0\t2024-01-01T00:00:00.000000Z
                        A\t4.0\t2024-01-02T00:00:00.000000Z
                        """);
            }
        });
    }

    @Test
    public void testVacuumRogueColumnFiles() throws Exception {
        FilesFacade ff = engine.getConfiguration().getFilesFacade();
        assertMemoryLeak(() -> {
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                execute(
                        "create table testPurge as" +
                                " (select timestamp_sequence('1970-01-01', 24 * 60 * 60 * 1000000L) ts," +
                                " x," +
                                " rnd_str('a', 'b', 'c', 'd') str," +
                                " rnd_symbol('A', 'B', 'C', 'D') sym1," +
                                " rnd_symbol('1', '2', '3', '4') sym2" +
                                " from long_sequence(5)), index(sym2)" +
                                " timestamp(ts) PARTITION BY DAY"
                );

                String[] partitions = new String[]{"1970-01-01", "1970-01-02", "1970-01-03", "1970-01-04", "1970-01-05"};
                String[] files = {"sym_rogue.k", "sym_rogue.v", "sym_rogue.d"};
                assertFilesExist(partitions, "testPurge", files, "", false);

                execute("checkpoint release");

                runPurgeJob(purgeJob);

                assertFilesExist(partitions, "testPurge", files, "", false);

                Path path = Path.getThreadLocal(configuration.getDbRoot());
                path.concat(engine.verifyTableName("testPurge"));
                int pathLen = path.size();
                for (String partition : partitions) {
                    for (String file : files) {
                        path.trimTo(pathLen).concat(partition).concat(file).$();
                        ff.touch(path.$());
                    }
                }

                assertFilesExist(partitions, "testPurge", files, "", true);

                runTableVacuum("testPurge");
                runPurgeJob(purgeJob);

                assertFilesExist(partitions, "testPurge", files, "", false);
            }
        });
    }

    @Test
    public void testVacuumRogueSymbolFilesInTableRoot() throws Exception {
        assertMemoryLeak(() -> {
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                execute(
                        "create table testPurge as" +
                                " (select timestamp_sequence('1970-01-01', 24 * 60 * 60 * 1000000L) ts," +
                                " x," +
                                " rnd_str('a', 'b', 'c', 'd') str," +
                                " rnd_symbol('A', 'B', 'C', 'D') sym1," +
                                " rnd_symbol('1', '2', '3', '4') sym2" +
                                " from long_sequence(5)), index(sym2)" +
                                " timestamp(ts) PARTITION BY DAY"
                );
                assertVacuumPurgesRogueSymbolFilesInTableRoot("testPurge", purgeJob);
            }
        });
    }

    @Test
    public void testVacuumRogueSymbolFilesInTableRootKeepsFilesOfOtherTable() throws Exception {
        assertMemoryLeak(() -> {
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                // The symbol column comes last, so it is the last column VACUUM of "other" visits.
                execute("CREATE TABLE other (ts TIMESTAMP, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY");
                execute("""
                        INSERT INTO other VALUES
                        ('2024-01-01T00:00:00.000000Z', 'A'),
                        ('2024-01-02T00:00:00.000000Z', 'B')
                        """);
                execute("CREATE TABLE testPurge (x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
                touchRogueSymbolFilesInTableRoot("testPurge");

                // One compiler runs both VACUUMs, as a pooled compiler does for successive VACUUM statements.
                try (SqlCompiler compiler = engine.getSqlCompiler()) {
                    engine.releaseInactive();
                    compiler.compile("VACUUM TABLE other", sqlExecutionContext);
                    engine.releaseInactive();
                    compiler.compile("VACUUM TABLE testPurge", sqlExecutionContext);
                }
                runPurgeJob(purgeJob);

                assertFilesExist(TABLE_ROOT, "other", new String[]{"sym.c", "sym.o", "sym.k", "sym.v"}, "", true);
                assertQuery("SELECT * FROM other")
                        .noLeakCheck()
                        .timestamp("ts")
                        .expectSize()
                        .returns("""
                                ts\tsym
                                2024-01-01T00:00:00.000000Z\tA
                                2024-01-02T00:00:00.000000Z\tB
                                """);
                assertFilesExist(TABLE_ROOT, "testPurge", ROGUE_SYMBOL_FILES, "", false);
            }
        });
    }

    @Test
    public void testVacuumRogueSymbolFilesInTableRootOfEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                // No symbol column and no partition, so the rogue column's files are the only column files on disk.
                execute("CREATE TABLE testPurge (x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
                assertVacuumPurgesRogueSymbolFilesInTableRoot("testPurge", purgeJob);
            }
        });
    }

    @Test
    public void testVacuumRogueSymbolFilesInTableRootWithParquetPartitionsOnly() throws Exception {
        assertMemoryLeak(() -> {
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                // No symbol column, and a WAL table converts its active partition too, so no partition keeps
                // native column files: the rogue column's files are the only column files on disk.
                execute("CREATE TABLE testPurge (x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                execute("""
                        INSERT INTO testPurge VALUES
                        (1, '2024-01-01T00:00:00.000000Z'),
                        (2, '2024-01-02T00:00:00.000000Z')
                        """);
                drainWalQueue();
                execute("ALTER TABLE testPurge CONVERT PARTITION TO PARQUET WHERE ts >= 0");
                drainWalQueue();
                assertQuery("SELECT name, isParquet FROM table_partitions('testPurge')")
                        .noLeakCheck()
                        .expectSize()
                        .noRandomAccess()
                        .returns("""
                                name\tisParquet
                                2024-01-01\ttrue
                                2024-01-02\ttrue
                                """);

                assertVacuumPurgesRogueSymbolFilesInTableRoot("testPurge", purgeJob);

                assertQuery("SELECT * FROM testPurge")
                        .noLeakCheck()
                        .timestamp("ts")
                        .expectSize()
                        .returns("""
                                x\tts
                                1\t2024-01-01T00:00:00.000000Z
                                2\t2024-01-02T00:00:00.000000Z
                                """);
            }
        });
    }

    @Test
    public void testVacuumSupportsWithO3InsertsUpdates() throws Exception {
        assertMemoryLeak(() -> {
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                String tableName = "testPurge1";
                String[] partitions = update3ColumnsWithOpenReader(purgeJob, tableName);

                String[] files = {"x.d"};
                assertFilesExist(partitions, tableName, files, ".2", true);

                runTableVacuum(tableName);
                assertFilesExist(partitions, tableName, files, ".2", false);
                Assert.assertEquals(0, purgeJob.getOutstandingPurgeTasks());
            }
        });
    }

    @Test
    public void testVacuumSync() throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(0);
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                execute(
                        "create table testPurge as" +
                                " (select timestamp_sequence('1970-01-01', 24 * 60 * 60 * 1000000L) ts," +
                                " x," +
                                " rnd_str('a', 'b', 'c', 'd') str," +
                                " rnd_symbol('A', 'B', 'C', 'D') sym1," +
                                " rnd_symbol('1', '2', '3', '4') sym2" +
                                " from long_sequence(5)), index(sym2)" +
                                " timestamp(ts) PARTITION BY DAY"
                );

                try (TableReader rdr = getReader("testPurge")) {
                    update("UPDATE testPurge SET x = 100, str='abcd', sym2='EE' WHERE ts >= '1970-01-02'");
                    rdr.openPartition(0);
                }

                runPurgeJob(purgeJob);

                String[] partitions = new String[]{"1970-01-02", "1970-01-03", "1970-01-04"};
                String[] files = {"sym2.d", "sym2.k", "sym2.v"};
                assertFilesExist(partitions, "testPurge", files, "", true);

                runTableVacuum("testPurge");
                assertFilesExist(partitions, "testPurge", files, "", false);
                Assert.assertEquals(0, purgeJob.getOutstandingPurgeTasks());
            }
        });
    }

    @Test
    public void testVacuumSync2Tables() throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(0);
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                execute(
                        "create table testPurge1 as" +
                                " (select timestamp_sequence('1970-01-01', 24 * 60 * 60 * 1000000L) ts," +
                                " x," +
                                " rnd_str('a', 'b', 'c', 'd') str," +
                                " rnd_symbol('A', 'B', 'C', 'D') sym1," +
                                " rnd_symbol('1', '2', '3', '4') sym2" +
                                " from long_sequence(5)), index(sym2)" +
                                " timestamp(ts) PARTITION BY DAY"
                );

                execute(
                        "create table testPurge2 as" +
                                " (select timestamp_sequence('1970-01-01', 24 * 60 * 60 * 1000000L) ts," +
                                " x," +
                                " rnd_str('a', 'b', 'c', 'd') str," +
                                " rnd_symbol('A', 'B', 'C', 'D') sym1," +
                                " rnd_symbol('1', '2', '3', '4') sym2" +
                                " from long_sequence(5)), index(sym2)" +
                                " timestamp(ts) PARTITION BY DAY"
                );

                try (
                        TableReader rdr1 = getReader("testPurge1");
                        TableReader rdr2 = getReader("testPurge2")
                ) {
                    update("UPDATE testPurge1 SET x = 100, str = 'abc' WHERE ts >= '1970-01-02'");
                    update("UPDATE testPurge2 SET x = 100, str = 'dcf' WHERE ts >= '1970-01-02'");
                    rdr1.openPartition(0);
                    rdr2.openPartition(0);
                }

                runPurgeJob(purgeJob);

                String[] partitions = new String[]{"1970-01-02", "1970-01-03", "1970-01-04"};
                String[] files = new String[]{"str.d", "str.i"};
                assertFilesExist(partitions, "testPurge2", files, "", true);

                runTableVacuum("testPurge1");
                runTableVacuum("testPurge2");
                Assert.assertEquals(0, purgeJob.getOutstandingPurgeTasks());
            }
        });
    }

    @Test
    public void testVacuumSyncFailsQueueSize() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table testPurge as" +
                            " (select timestamp_sequence('1970-01-01', 24 * 60 * 60 * 1000000L) ts," +
                            " x," +
                            " rnd_str('a', 'b', 'c', 'd') str," +
                            " rnd_symbol('A', 'B', 'C', 'D') sym1," +
                            " rnd_symbol('1', '2', '3', '4') sym2" +
                            " from long_sequence(5)), index(sym2)" +
                            " timestamp(ts) PARTITION BY DAY"
            );

            try (TableReader rdr = getReader("testPurge")) {
                update("UPDATE testPurge SET x = 100, str='abcd', sym2='EE' WHERE ts >= '1970-01-02'");
                rdr.openPartition(0);

                String[] partitions = new String[]{"1970-01-02", "1970-01-03", "1970-01-04"};
                String[] files = {"sym2.d", "sym2.k", "sym2.v"};
                assertFilesExist(partitions, "testPurge", files, "", true);

                try {
                    runTableVacuum("testPurge");
                    Assert.fail();
                } catch (CairoException ex) {
                    TestUtils.assertContains(ex.getFlyweightMessage(),
                            "cairo.sql.column.purge.queue.capacity");
                    TestUtils.assertContains(ex.getFlyweightMessage(),
                            "failed to schedule column version purge, queue is full");
                }
            }
        });
    }

    @Test
    public void testVacuumWhenColumnReAdded() throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(0);
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                execute(
                        "create table testPurge as" +
                                " (select timestamp_sequence('1970-01-01', 24 * 60 * 60 * 1000000L) ts," +
                                " x," +
                                " rnd_str('a', 'b', 'c', 'd') str," +
                                " rnd_symbol('A', 'B', 'C', 'D') sym1," +
                                " rnd_symbol('1', '2', '3', '4') sym2" +
                                " from long_sequence(5)), index(sym2)" +
                                " timestamp(ts) PARTITION BY DAY"
                );
                execute("alter table testPurge drop column x");
                execute("alter table testPurge add column x int");
                execute("insert into testPurge(str,sym1,sym2,x,ts) values('str', 'sym1', 'sym2', 123, '1970-02-01')");

                try (TableReader rdr = getReader("testPurge")) {
                    update("UPDATE testPurge SET str='abcd', sym2='EE',x=1 WHERE ts >= '1970-01-02'");
                    rdr.openPartition(0);
                }

                runPurgeJob(purgeJob);
                String[] partitions = new String[]{"1970-02-01.2"};
                String[] files = {"x.d"};
                assertFilesExist(partitions, "testPurge", files, ".2", true);

                runTableVacuum("testPurge");
                assertFilesExist(partitions, "testPurge", files, ".2", false);
                Assert.assertEquals(0, purgeJob.getOutstandingPurgeTasks());
            }
        });
    }

    @Test
    public void testVacuumWithInvalidFileNames() throws Exception {
        assertMemoryLeak(() -> {
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                String tableName = "testPurge2";
                String[] partitions = update3ColumnsWithOpenReader(purgeJob, tableName);

                Path path = Path.getThreadLocal(configuration.getDbRoot());
                TableToken tableToken = engine.verifyTableName(tableName);
                path.concat(tableToken).concat(partitions[0]).concat("invalid_file.dk");
                FilesFacade ff = configuration.getFilesFacade();
                ff.touch(path.$());
                path.of(configuration.getDbRoot()).concat(tableToken).concat(partitions[0]).concat("x.d.abcd");
                ff.touch(path.$());

                String[] files = {"x.d"};
                assertFilesExist(partitions, tableName, files, ".2", true);

                runTableVacuum(tableName);
                assertFilesExist(partitions, tableName, files, ".2", false);
                Assert.assertEquals(0, purgeJob.getOutstandingPurgeTasks());

                path.of(configuration.getDbRoot()).concat(tableToken).concat(partitions[0]).concat("x.d.abcd");
                Assert.assertTrue(ff.exists(path.$()));

                path.of(configuration.getDbRoot()).concat(tableToken).concat(partitions[0]).concat("invalid_file.dk");
                Assert.assertTrue(ff.exists(path.$()));
            }
        });
    }

    @Test
    public void testVacuumWithInvalidPartitionDirectoryNames() throws Exception {
        assertMemoryLeak(() -> {
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                String tableName = "testPurge3";
                String[] partitions = update3ColumnsWithOpenReader(purgeJob, tableName);

                Path path = Path.getThreadLocal(configuration.getDbRoot());
                TableToken tableToken = engine.verifyTableName(tableName);
                path.concat(tableToken).concat("abcd").put(Files.SEPARATOR);
                FilesFacade ff = configuration.getFilesFacade();
                ff.mkdirs(path, configuration.getMkDirMode());

                path.of(configuration.getDbRoot()).concat(tableToken).concat("2020-01-04.abcd").put(Files.SEPARATOR);
                ff.mkdirs(path, configuration.getMkDirMode());

                String[] files = {"x.d"};
                assertFilesExist(partitions, tableName, files, ".2", true);

                runTableVacuum(tableName);
                assertFilesExist(partitions, tableName, files, ".2", false);
                Assert.assertEquals(0, purgeJob.getOutstandingPurgeTasks());

                path = Path.getThreadLocal(configuration.getDbRoot());
                path.concat(tableToken).concat("abcd").put(Files.SEPARATOR);
                Assert.assertTrue(ff.exists(path.$()));

                path.of(configuration.getDbRoot()).concat(tableToken).concat("2020-01-04.abcd").put(Files.SEPARATOR);
                Assert.assertTrue(ff.exists(path.$()));
            }
        });
    }

    private static int countFiles(File dir, String regex) {
        final String[] names = dir.list();
        Assert.assertNotNull(names);
        int count = 0;
        for (String name : names) {
            if (name.matches(regex)) {
                count++;
            }
        }
        return count;
    }

    /**
     * A non-WAL table whose POSTING index on sym INCLUDEs val. sym is added before the first commit, so it has name
     * txn 0, and 'A' has a row in each of 2024-01-01 and 2024-01-02.
     */
    private static void createIndexWithNameTxnZeroTable(String tableName) throws SqlException {
        execute("CREATE TABLE " + tableName + " (ts TIMESTAMP, val DOUBLE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
        execute("ALTER TABLE " + tableName + " ADD COLUMN sym SYMBOL");
        execute("ALTER TABLE " + tableName + " ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (val)");
        execute("INSERT INTO " + tableName + " VALUES ('2024-01-01T00:00:00', 1.0, 'A'), ('2024-01-01T01:00:00', 2.0, 'B'), ('2024-01-02T00:00:00', 4.0, 'A')");
    }

    private static File partitionDir(String tableName, String partitionName) {
        final File tableDir = new File(configuration.getDbRoot(), engine.verifyTableName(tableName).getDirName());
        final File[] dirs = tableDir.listFiles((dir, name) -> name.equals(partitionName) || name.startsWith(partitionName + "."));
        Assert.assertNotNull(dirs);
        Assert.assertEquals("one directory for partition " + partitionName + ": " + Arrays.toString(dirs), 1, dirs.length);
        return dirs[0];
    }

    private void assertCoveringQuery(String tableName, String indexColumn, String expected) throws Exception {
        assertQuery("SELECT " + indexColumn + ", val, ts FROM " + tableName + " WHERE " + indexColumn + " = 'A'")
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .timestamp("ts")
                .withPlanContaining("CoveringIndex on: " + indexColumn + " with: val, ts")
                .returns(expected);
    }

    private void assertFilesExist(String[] partitions, String tableName, String[] files, String colSuffix, boolean exist) {
        for (int i = 0; i < partitions.length; i++) {
            String partition = partitions[i];
            assertFilesExist(tableName, partition, files, colSuffix, exist);
        }
    }

    private void assertFilesExist(String tableName, String partition, String[] files, String colSuffix, boolean exist) {
        Path path = Path.getThreadLocal(configuration.getDbRoot());
        TableToken tableToken = engine.verifyTableName(tableName);

        for (int i = files.length - 1; i > -1; i--) {
            String file = files[i];
            path.of(configuration.getDbRoot()).concat(tableToken).concat(partition).concat(file).put(colSuffix).$();
            Assert.assertEquals(Utf8s.toString(path), exist, TestFilesFacadeImpl.INSTANCE.exists(path.$()));
        }
    }

    /**
     * VACUUM, then the purge it schedules, must leave the cover files of the live index, the 2024-01-01 files that
     * match {@code coverFileRegex}, and the covering query must read them.
     */
    private void assertVacuumKeepsCoveringIndex(String tableName, String indexColumn, String coverFileRegex) throws Exception {
        Assume.assumeFalse(Os.isWindows());
        assertMemoryLeak(() -> {
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                createIndexWithNameTxnZeroTable(tableName);
                if (!"sym".equals(indexColumn)) {
                    execute("ALTER TABLE " + tableName + " RENAME COLUMN sym TO " + indexColumn);
                }
                final String expected = indexColumn + """
                        \tval\tts
                        A\t1.0\t2024-01-01T00:00:00.000000Z
                        A\t4.0\t2024-01-02T00:00:00.000000Z
                        """;
                assertCoveringQuery(tableName, indexColumn, expected);
                final File partitionDir = partitionDir(tableName, "2024-01-01");
                final int coverFiles = countFiles(partitionDir, coverFileRegex);
                Assert.assertTrue("fixture: the live index has cover files", coverFiles > 0);

                runTableVacuum(tableName);
                runPurgeJob(purgeJob);

                Assert.assertEquals("live cover files kept", coverFiles, countFiles(partitionDir, coverFileRegex));
                assertCoveringQuery(tableName, indexColumn, expected);
            }
        });
    }

    /**
     * Asserts that VACUUM, followed by the purge it schedules, removes symbol files in the table root that belong to
     * no column of the table.
     */
    private void assertVacuumPurgesRogueSymbolFilesInTableRoot(String tableName, ColumnPurgeJob purgeJob) throws SqlException {
        touchRogueSymbolFilesInTableRoot(tableName);

        runTableVacuum(tableName);
        runPurgeJob(purgeJob);

        assertFilesExist(TABLE_ROOT, tableName, ROGUE_SYMBOL_FILES, "", false);
    }

    @NotNull
    private ColumnPurgeJob createPurgeJob() throws SqlException {
        return new ColumnPurgeJob(engine);
    }

    private void runPurgeJob(ColumnPurgeJob purgeJob) {
        engine.releaseInactive();
        setCurrentMicros(currentMicros + 10L * iteration++);
        purgeJob.run();
        setCurrentMicros(currentMicros + 10L * iteration++);
        purgeJob.run();
    }

    private void runTableVacuum(String tableName) throws SqlException {
        if (Os.isWindows()) {
            engine.releaseInactive();
        }
        execute("VACUUM TABLE " + tableName);
    }

    private void touchRogueSymbolFilesInTableRoot(String tableName) {
        assertFilesExist(TABLE_ROOT, tableName, ROGUE_SYMBOL_FILES, "", false);
        final FilesFacade ff = engine.getConfiguration().getFilesFacade();
        final Path path = Path.getThreadLocal(configuration.getDbRoot()).concat(engine.verifyTableName(tableName));
        final int pathLen = path.size();
        for (String file : ROGUE_SYMBOL_FILES) {
            Assert.assertTrue(file, ff.touch(path.trimTo(pathLen).concat(file).$()));
        }
        assertFilesExist(TABLE_ROOT, tableName, ROGUE_SYMBOL_FILES, "", true);
    }

    private String[] update3ColumnsWithOpenReader(ColumnPurgeJob purgeJob, String tableName) throws SqlException {
        execute(
                "create table " + tableName + " as" +
                        " (select timestamp_sequence('1970-01-01T00:01', 24 * 60 * 60 * 1000000L) ts," +
                        " x," +
                        " rnd_str('a', 'b', 'c', 'd') str," +
                        " rnd_symbol('A', 'B', 'C', 'D') sym1," +
                        " rnd_symbol('1', '2', '3', '4') sym2" +
                        " from long_sequence(5)), index(sym2)" +
                        " timestamp(ts) PARTITION BY DAY"
        );
        execute("alter table " + tableName + " drop column x");
        execute("alter table " + tableName + " add column x int");
        try (TableReader rdr = getReader(tableName)) {
            execute("insert into " + tableName + "(ts, x, str,sym1,sym2) " +
                    "select timestamp_sequence('1970-01-01', 24 * 60 * 60 * 1000000L) ts," +
                    " x," +
                    " rnd_str('a', 'b', 'c', 'd') str," +
                    " rnd_symbol('A', 'B', 'C', 'D') sym1," +
                    " rnd_symbol('1', '2', '3', '4') sym2" +
                    " from long_sequence(5)");

            update("UPDATE " + tableName + " SET str='abcd', sym2='EE',x=1 WHERE ts >= '1970-01-02'");
            rdr.openPartition(0);
        }

        runPurgeJob(purgeJob);
        return new String[]{"1970-01-02.3", "1970-01-03.3", "1970-01-04.3"};
    }
}
