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
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.idx.BitmapIndexUtils;
import io.questdb.cairo.idx.PostingIndexUtils;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.Os;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.QueryAssertion;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class VacuumColumnVersionTest extends AbstractCairoTest {
    // the index clauses of s in the POSTING tests; the last one writes no .pc files
    private static final String[] POSTING_INDEX_CLAUSES = {
            "POSTING INCLUDE (d, vc)", "POSTING DELTA INCLUDE (d, vc)", "POSTING EF INCLUDE (d, vc)", "POSTING"
    };
    // the rows insertPostingRows() writes with s = 'y'
    private static final String POSTING_ROWS_OF_Y = """
            ts\ts\td\tvc
            2024-01-01T00:00:00.000000Z\ty\t1.5\ta1
            2024-01-01T01:00:00.000000Z\ty\t2.5\ta2
            2024-01-01T03:00:00.000000Z\ty\t4.5\ta4
            2024-01-02T00:00:00.000000Z\ty\t5.5\ta5
            """;
    private static final String[] WAL_MODES = {"WAL", "BYPASS WAL"};
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

                String[] partitions = new String[]{"."};
                String[] files = {"rog_sym.c", "rog_sym.o", "rog_sym.k", "rog_sym.v"};
                assertFilesExist(partitions, "testPurge", files, "", false);

                Path path = Path.getThreadLocal(configuration.getDbRoot());
                path.concat(engine.verifyTableName("testPurge"));
                int pathLen = path.size();

                path.trimTo(pathLen).concat("rog_sym.c").$();
                ff.touch(path.$());
                path.trimTo(pathLen).concat("rog_sym.o").$();
                ff.touch(path.$());
                path.trimTo(pathLen).concat("rog_sym.k").$();
                ff.touch(path.$());
                path.trimTo(pathLen).concat("rog_sym.v").$();
                ff.touch(path.$());
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
    public void testVacuumParquetIndexVersionDroppedByFullPurgeQueue() throws Exception {
        // In-place O3 merges into a parquet partition move the index to a new version each time. With a
        // reader pinned, each commit queues the purge of the version it replaced; the purge queue holds
        // two tasks, so the third one is dropped. The next commit does not retry it, and VACUUM TABLE
        // removes it.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_RATIO, "1.0");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, Long.MAX_VALUE);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE pt (s SYMBOL INDEX, v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO pt VALUES
                    ('a', 1, '2024-01-01T00:00:00.000000Z'), ('b', 2, '2024-01-01T01:00:00.000000Z'),
                    ('c', 3, '2024-01-01T02:00:00.000000Z'), ('a', 4, '2024-01-01T03:00:00.000000Z'),
                    ('b', 5, '2024-01-01T04:00:00.000000Z'), ('c', 6, '2024-01-01T05:00:00.000000Z'),
                    ('a', 7, '2024-01-01T06:00:00.000000Z'), ('b', 8, '2024-01-01T07:00:00.000000Z'),
                    ('c', 9, '2024-01-01T08:00:00.000000Z'), ('a', 10, '2024-01-01T09:00:00.000000Z'),
                    ('b', 11, '2024-01-01T10:00:00.000000Z'), ('c', 12, '2024-01-01T11:00:00.000000Z'),
                    ('a', 99, '2024-01-02T00:00:00.000000Z')""");
            execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");

            final long[] versions = new long[5];
            final String partitionDir;
            try (TableReader pinned = getReader("pt")) {
                versions[0] = pinned.getColumnVersionReader().getColumnNameTxn(pinned.getPartitionTimestampByIndex(0), 0);
                partitionDir = "2024-01-01." + pinned.getTxFile().getPartitionNameTxn(0);
                for (int i = 1; i <= 3; i++) {
                    // a merge names the new version after the txn the commit starts from
                    versions[i] = getTableTxn();
                    execute("INSERT INTO pt VALUES ('a', " + (100 + i) + ", '2024-01-01T0" + i + ":30:00.000000Z')");
                }
            }
            for (int i = 0; i <= 3; i++) {
                assertFilesExist("pt", partitionDir, new String[]{"s.k", "s.v"}, versionSuffix(versions[i]), true);
            }

            // no reader: the next merge removes the version it replaces, and leaves the dropped one
            engine.releaseInactive();
            versions[4] = getTableTxn();
            engine.releaseInactive();
            execute("INSERT INTO pt VALUES ('a', 104, '2024-01-01T04:30:00.000000Z')");
            assertFilesExist("pt", partitionDir, new String[]{"s.k", "s.v"}, versionSuffix(versions[3]), false);
            assertFilesExist("pt", partitionDir, new String[]{"s.k", "s.v"}, versionSuffix(versions[2]), true);

            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                // the two queued tasks
                runPurgeJob(purgeJob);
                assertFilesExist("pt", partitionDir, new String[]{"s.k", "s.v"}, versionSuffix(versions[0]), false);
                assertFilesExist("pt", partitionDir, new String[]{"s.k", "s.v"}, versionSuffix(versions[1]), false);
                assertFilesExist("pt", partitionDir, new String[]{"s.k", "s.v"}, versionSuffix(versions[2]), true);

                runTableVacuum("pt");
                runPurgeJob(purgeJob);
                Assert.assertEquals(0, purgeJob.getOutstandingPurgeTasks());
            }
            assertFilesExist("pt", partitionDir, new String[]{"s.k", "s.v"}, versionSuffix(versions[2]), false);
            assertFilesExist("pt", partitionDir, new String[]{"s.k", "s.v"}, versionSuffix(versions[4]), true);
            assertQuery("SELECT s, v, ts FROM pt WHERE s = 'a' AND ts IN '2024-01-01'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            s\tv\tts
                            a\t1\t2024-01-01T00:00:00.000000Z
                            a\t101\t2024-01-01T01:30:00.000000Z
                            a\t102\t2024-01-01T02:30:00.000000Z
                            a\t4\t2024-01-01T03:00:00.000000Z
                            a\t103\t2024-01-01T03:30:00.000000Z
                            a\t104\t2024-01-01T04:30:00.000000Z
                            a\t7\t2024-01-01T06:00:00.000000Z
                            a\t10\t2024-01-01T09:00:00.000000Z
                            """);
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

    @Test
    public void testVacuumInMiddleOfUpdatePostingInclude() throws Exception {
        // VACUUM runs while UPDATE writes the .pv file of the new instance U of s. The new .pv name
        // carries U, which the reader of VACUUM does not see yet, so VACUUM leaves it alone.
        final AtomicReference<String> updateValueFile = new AtomicReference<>();
        final AtomicReference<ColumnPurgeJob> purgeJobInstance = new AtomicReference<>();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                final String valueFile = updateValueFile.get();
                if (valueFile != null && Utf8s.containsAscii(name, valueFile)) {
                    updateValueFile.set(null);
                    try {
                        runTableVacuum("pt");
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
                createPostingTableOfColumnNameTxnZero("BYPASS WAL", POSTING_INDEX_CLAUSES[0]);
                insertPostingRows();
                final String partitionDir = livePartitionDirs("pt")[0];
                final ArrayList<String> instanceZeroFiles = new ArrayList<>();
                for (String file : listFiles("pt", partitionDir).split(" ")) {
                    if (file.startsWith("s.")) {
                        instanceZeroFiles.add(file);
                    }
                }
                Assert.assertTrue(instanceZeroFiles.toString(), instanceZeroFiles.contains("s.pci.0"));

                final long updateTxn = getTableTxn();
                purgeJobInstance.set(purgeJob);
                updateValueFile.set(Files.SEPARATOR + "s.pv." + updateTxn + ".");
                update("UPDATE pt SET s = 'y' WHERE v = 3");
                Assert.assertNull("VACUUM ran during the UPDATE", updateValueFile.get());
                runPurgeJob(purgeJob);
                Assert.assertEquals(0, purgeJob.getOutstandingPurgeTasks());

                Assert.assertEquals(updateTxn, getColumnNameTxn("pt", "s", 0));
                final String remaining = listFiles("pt", partitionDir);
                for (String file : instanceZeroFiles) {
                    Assert.assertFalse(file + " in " + remaining, (" " + remaining + " ").contains(" " + file + " "));
                }
                Assert.assertTrue(remaining, hasCoverDataFile(remaining));
                assertPostingRowsOfY("after UPDATE", true, """
                        ts\ts\td\tvc
                        2024-01-01T00:00:00.000000Z\ty\t1.5\ta1
                        2024-01-01T01:00:00.000000Z\ty\t2.5\ta2
                        2024-01-01T02:00:00.000000Z\ty\t3.5\ta3
                        2024-01-01T03:00:00.000000Z\ty\t4.5\ta4
                        2024-01-02T00:00:00.000000Z\ty\t5.5\ta5
                        """);
            }
        });
    }

    @Test
    public void testVacuumKeepsPostingFilesOfColumnConvertedAtTxnZeroNonPartitioned() throws Exception {
        // In the default partition of a table that is not partitioned, a column type change before the
        // first commit writes instance 0 of s next to instance NONE, whose purge a reader defers. s then
        // gets a POSTING INCLUDE index, whose .pc files carry the token 0 that NONE also writes. VACUUM
        // keeps the NONE files rather than purging them along with the .pc files of instance 0.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE pt (v INT, ts TIMESTAMP, s VARCHAR, d DOUBLE, vc VARCHAR) TIMESTAMP(ts)");
            try (TableReader ignore = getReader("pt")) {
                execute("ALTER TABLE pt ALTER COLUMN s TYPE SYMBOL");
            }
            execute("ALTER TABLE pt ALTER COLUMN s ADD INDEX TYPE POSTING INCLUDE (d, vc)");
            insertPostingRows();
            Assert.assertEquals(0, getColumnNameTxn("pt", "s", 0));
            final String before = listFiles("pt", "default");
            final ArrayList<String> unsealedFiles = new ArrayList<>();
            for (String file : filesOf(before, "s.").split(" ")) {
                if (!file.startsWith("s.pv.") && !isCoverDataFile(file)) {
                    unsealedFiles.add(file);
                }
            }
            Assert.assertEquals("s.d s.d.0 s.i s.pci.0 s.pk.0", String.join(" ", unsealedFiles));
            Assert.assertTrue(before, before.contains(" s.pc0.0."));

            engine.releaseInactive();
            execute("VACUUM TABLE pt");
            Assert.assertEquals(before, listFiles("pt", "default"));
            assertPostingRowsOfY("after VACUUM", true, POSTING_ROWS_OF_Y);

            // the purge that the type change queued removes the files of instance NONE
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                runPurgeJob(purgeJob);
                Assert.assertEquals(0, purgeJob.getOutstandingPurgeTasks());
            }
            final ArrayList<String> remaining = new ArrayList<>(Arrays.asList(before.split(" ")));
            remaining.remove("s.d");
            remaining.remove("s.i");
            Assert.assertEquals(String.join(" ", remaining), listFiles("pt", "default"));
            assertPostingRowsOfY("after the purge job", true, POSTING_ROWS_OF_Y);
        });
    }

    @Test
    public void testVacuumKeepsPostingIncludeFilesAfterO3Insert() throws Exception {
        // An O3 row rewrites 2024-01-01 into a new directory and reseals the index of s.
        assertMemoryLeak(() -> {
            for (String walMode : WAL_MODES) {
                for (boolean isTxnZero : new boolean[]{true, false}) {
                    final String context = walMode + ", txn " + (isTxnZero ? "0" : "NONE");
                    execute("DROP TABLE IF EXISTS pt");
                    if (isTxnZero) {
                        createPostingTableOfColumnNameTxnZero(walMode, POSTING_INDEX_CLAUSES[0]);
                    } else {
                        createPostingTableOfColumnNameTxnNone(walMode, POSTING_INDEX_CLAUSES[0]);
                    }
                    execute("""
                            INSERT INTO pt (v, ts, s, d, vc) VALUES
                            (1, '2024-01-01T01:00:00.000000Z', 'y', 1.5, 'a1'),
                            (3, '2024-01-01T03:00:00.000000Z', 'y', 3.5, 'a3'),
                            (4, '2024-01-02T00:00:00.000000Z', 'y', 4.5, 'a4')""");
                    drainWalQueue();
                    final String dirBeforeO3 = livePartitionDirs("pt")[0];
                    execute("INSERT INTO pt (v, ts, s, d, vc) VALUES (2, '2024-01-01T02:00:00.000000Z', 'y', 2.5, 'a2')");
                    drainWalQueue();
                    Assert.assertNotEquals(context, dirBeforeO3, livePartitionDirs("pt")[0]);
                    assertVacuumKeepsPostingFiles(context, true, """
                            ts\ts\td\tvc
                            2024-01-01T01:00:00.000000Z\ty\t1.5\ta1
                            2024-01-01T02:00:00.000000Z\ty\t2.5\ta2
                            2024-01-01T03:00:00.000000Z\ty\t3.5\ta3
                            2024-01-02T00:00:00.000000Z\ty\t4.5\ta4
                            """);
                }
            }
        });
    }

    @Test
    public void testVacuumKeepsPostingIncludeFilesInParquetPartition() throws Exception {
        assertMemoryLeak(() -> {
            for (String walMode : WAL_MODES) {
                for (boolean isTxnZero : new boolean[]{true, false}) {
                    final String context = walMode + ", txn " + (isTxnZero ? "0" : "NONE");
                    execute("DROP TABLE IF EXISTS pt");
                    if (isTxnZero) {
                        createPostingTableOfColumnNameTxnZero(walMode, POSTING_INDEX_CLAUSES[0]);
                    } else {
                        createPostingTableOfColumnNameTxnNone(walMode, POSTING_INDEX_CLAUSES[0]);
                    }
                    insertPostingRows();
                    execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
                    drainWalQueue();
                    final String parquetDir = livePartitionDirs("pt")[0];
                    Assert.assertTrue(context, listFiles("pt", parquetDir).contains("data.parquet"));
                    assertVacuumKeepsPostingFiles(context + ", parquet", true, POSTING_ROWS_OF_Y);

                    execute("ALTER TABLE pt CONVERT PARTITION TO NATIVE LIST '2024-01-01'");
                    drainWalQueue();
                    Assert.assertFalse(context, listFiles("pt", livePartitionDirs("pt")[0]).contains("data.parquet"));
                    assertVacuumKeepsPostingFiles(context + ", native again", true, POSTING_ROWS_OF_Y);
                }
            }
        });
    }

    @Test
    public void testVacuumKeepsPostingIncludeFilesNonPartitioned() throws Exception {
        assertMemoryLeak(() -> {
            for (boolean isTxnZero : new boolean[]{true, false}) {
                final String context = "txn " + (isTxnZero ? "0" : "NONE");
                execute("DROP TABLE IF EXISTS pt");
                if (isTxnZero) {
                    execute("CREATE TABLE pt (v INT, ts TIMESTAMP) TIMESTAMP(ts)");
                    execute("ALTER TABLE pt ADD COLUMN s SYMBOL");
                    execute("ALTER TABLE pt ADD COLUMN d DOUBLE");
                    execute("ALTER TABLE pt ADD COLUMN vc VARCHAR");
                    execute("ALTER TABLE pt ALTER COLUMN s ADD INDEX TYPE POSTING INCLUDE (d, vc)");
                } else {
                    execute("CREATE TABLE pt (v INT, ts TIMESTAMP, s SYMBOL INDEX TYPE POSTING INCLUDE (d, vc), d DOUBLE, vc VARCHAR) TIMESTAMP(ts)");
                }
                insertPostingRows();
                Assert.assertEquals(context, isTxnZero ? 0 : TableUtils.COLUMN_NAME_TXN_NONE, getColumnNameTxn("pt", "s", 0));
                Assert.assertArrayEquals(context, new String[]{"default"}, livePartitionDirs("pt"));
                assertVacuumKeepsPostingFiles(context, true, POSTING_ROWS_OF_Y);
            }
        });
    }

    @Test
    public void testVacuumKeepsPostingIncludeFilesOfColumnNameTxnNone() throws Exception {
        // s is declared in CREATE TABLE, so its column name txn is NONE: s.pv.<seal> carries no txn
        // segment and its .pc files carry the token 0.
        assertMemoryLeak(() -> {
            for (String walMode : WAL_MODES) {
                for (String indexClause : POSTING_INDEX_CLAUSES) {
                    final String context = walMode + ", " + indexClause;
                    execute("DROP TABLE IF EXISTS pt");
                    createPostingTableOfColumnNameTxnNone(walMode, indexClause);
                    insertPostingRows();
                    Assert.assertEquals(context, TableUtils.COLUMN_NAME_TXN_NONE, getColumnNameTxn("pt", "s", 0));
                    assertVacuumKeepsPostingFiles(context, indexClause.contains("INCLUDE"), POSTING_ROWS_OF_Y);
                }
            }
        });
    }

    @Test
    public void testVacuumKeepsPostingIncludeFilesOfColumnNameTxnZero() throws Exception {
        // s is the first column added to the empty table, so its column name txn is 0 and its file
        // names have two txn segments: s.pv.0.<seal> and s.pc<i>.0.<coveredTxn>.<seal>.
        assertMemoryLeak(() -> {
            for (String walMode : WAL_MODES) {
                for (String indexClause : POSTING_INDEX_CLAUSES) {
                    final String context = walMode + ", " + indexClause;
                    execute("DROP TABLE IF EXISTS pt");
                    createPostingTableOfColumnNameTxnZero(walMode, indexClause);
                    insertPostingRows();
                    Assert.assertEquals(context, 0, getColumnNameTxn("pt", "s", 0));
                    assertVacuumKeepsPostingFiles(context, indexClause.contains("INCLUDE"), POSTING_ROWS_OF_Y);
                }
            }
        });
    }

    @Test
    public void testVacuumKeepsPostingIncludeFilesOfNonAsciiColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE pt (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("ALTER TABLE pt ADD COLUMN сим SYMBOL");
            execute("ALTER TABLE pt ADD COLUMN d DOUBLE");
            execute("ALTER TABLE pt ALTER COLUMN сим ADD INDEX TYPE POSTING INCLUDE (d)");
            execute("""
                    INSERT INTO pt (v, ts, сим, d) VALUES
                    (1, '2024-01-01T00:00:00.000000Z', 'y', 1.5),
                    (2, '2024-01-01T01:00:00.000000Z', 'x', 2.5),
                    (3, '2024-01-02T00:00:00.000000Z', 'y', 3.5)""");
            Assert.assertEquals(0, getColumnNameTxn("pt", "сим", 0));
            final String partitionDir = livePartitionDirs("pt")[0];
            final String before = listFiles("pt", partitionDir);
            Assert.assertTrue(before, before.contains("сим.pc0.0."));

            final int outstandingPurgeTasks = vacuumAndPurge();
            Assert.assertEquals(before, listFiles("pt", partitionDir));
            Assert.assertEquals(0, outstandingPurgeTasks);
            assertQuery("SELECT ts, сим, d FROM pt WHERE сим = 'y'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .timestamp("ts")
                    .withPlanContaining("CoveringIndex on: сим with: ts, d")
                    .returns("""
                            ts\tсим\td
                            2024-01-01T00:00:00.000000Z\ty\t1.5
                            2024-01-02T00:00:00.000000Z\ty\t3.5
                            """);
        });
    }

    @Test
    public void testVacuumKeepsPostingIncludeFilesWithStrayFiles() throws Exception {
        // Names that look like files of s but parse to no txn stay on disk, as x.d.abcd does in
        // testVacuumWithInvalidFileNames, and do not make VACUUM purge the live files of s.
        assertMemoryLeak(() -> {
            for (boolean isTxnZero : new boolean[]{true, false}) {
                final String context = "txn " + (isTxnZero ? "0" : "NONE");
                execute("DROP TABLE IF EXISTS pt");
                if (isTxnZero) {
                    createPostingTableOfColumnNameTxnZero("BYPASS WAL", POSTING_INDEX_CLAUSES[0]);
                } else {
                    createPostingTableOfColumnNameTxnNone("BYPASS WAL", POSTING_INDEX_CLAUSES[0]);
                }
                insertPostingRows();
                final FilesFacade ff = configuration.getFilesFacade();
                final Path path = Path.getThreadLocal(configuration.getDbRoot());
                final TableToken tableToken = engine.verifyTableName("pt");
                for (String partitionDir : livePartitionDirs("pt")) {
                    for (String stray : new String[]{"s.d.", "s.d.junk", "s.pv.junk", "s.pc0.x", "s.pv.1.2.3"}) {
                        path.of(configuration.getDbRoot()).concat(tableToken).concat(partitionDir).concat(stray);
                        Assert.assertTrue(ff.touch(path.$()));
                    }
                }
                assertVacuumKeepsPostingFiles(context, true, POSTING_ROWS_OF_Y);
            }
        });
    }

    @Test
    public void testVacuumPostingIndexOnEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE pt (v INT, ts TIMESTAMP, s SYMBOL INDEX TYPE POSTING INCLUDE (d, vc), d DOUBLE, vc VARCHAR) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            final String before = listFiles("pt", "");
            Assert.assertTrue(before, before.contains("s.c s.k s.o s.v"));
            Assert.assertEquals(0, vacuumAndPurge());
            Assert.assertEquals(before, listFiles("pt", ""));
            Assert.assertEquals(0, livePartitionDirs("pt").length);
            assertQuery("SELECT ts, s, d, vc FROM pt WHERE s = 'y'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .timestamp("ts")
                    .withPlanContaining("CoveringIndex on: s with: ts, d, vc")
                    .returns("ts\ts\td\tvc\n");
        });
    }

    @Test
    public void testVacuumPostingStaleSidecarRemoveFails() throws Exception {
        // The first removal of an orphan .pc file of an old instance of s fails; the column purge job
        // removes it on retry.
        final AtomicReference<String> failingFile = new AtomicReference<>();
        final AtomicInteger failedRemoveCount = new AtomicInteger();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public boolean removeQuiet(LPSZ name) {
                final String file = failingFile.get();
                if (file != null && Utf8s.endsWithAscii(name, file)) {
                    failingFile.set(null);
                    failedRemoveCount.incrementAndGet();
                    return false;
                }
                return super.removeQuiet(name);
            }
        };
        assertMemoryLeak(ff, () -> {
            final StalePostingInstance stale = createStalePostingInstance();
            failingFile.set(Files.SEPARATOR + stale.coverDataFile);
            final int outstandingPurgeTasks = vacuumAndPurge();
            Assert.assertEquals(1, failedRemoveCount.get());
            Assert.assertEquals(stale.liveFiles, listFiles("pt", stale.partitionDir));
            Assert.assertEquals(0, outstandingPurgeTasks);
            assertPostingRowsOfY("after the retry", true, stale.rowsOfY);
        });
    }

    @Test
    public void testVacuumPurgesOrphanIndexFilesOfEveryPartition() throws Exception {
        // UPDATE with an open reader leaves the old instance of each updated column in every partition.
        // The test then deletes the column file of each, and the .pk of s, so that only these remain:
        // the .pci, .pv and .pc files of s, the .k and .v files of b, the .i files of w and str, and
        // the .d of x. VACUUM alone removes all of them, in every partition, without removing a directory.
        final ArrayList<String> directoryRemovals = new ArrayList<>();
        assertMemoryLeak(newOrphanPurgeFilesFacade(directoryRemovals, new AtomicReference<>(), new AtomicInteger()), () -> {
            for (String walMode : WAL_MODES) {
                for (String partitionBy : new String[]{"DAY", "NONE"}) {
                    if (partitionBy.equals("NONE") && walMode.equals("WAL")) {
                        // a WAL table is always partitioned
                        continue;
                    }
                    for (String indexClause : POSTING_INDEX_CLAUSES) {
                        final String context = walMode + ", " + partitionBy + ", " + indexClause;
                        final OrphanIndexFiles orphans = createOrphanIndexFiles(context, walMode, partitionBy, indexClause);
                        Assert.assertEquals(context, partitionBy.equals("DAY") ? 3 : 1, orphans.partitionDirs.length);

                        vacuumTablePt();
                        Assert.assertEquals(context, "[]", directoryRemovals.toString());
                        orphans.assertLiveFilesOnly(context + ", after VACUUM");

                        try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                            // replays the purge tasks of the UPDATE
                            runPurgeJob(purgeJob);
                            Assert.assertEquals(context, 0, purgeJob.getOutstandingPurgeTasks());
                        }
                        orphans.assertLiveFilesOnly(context + ", after the purge job");
                        Assert.assertEquals(context, "[]", directoryRemovals.toString());
                        assertOrphanIndexFilesTableRows(context, !indexClause.equals("POSTING"));
                    }
                }
            }
        });
    }

    @Test
    public void testVacuumPurgesOrphanPostingFilesOfParquetPartition() throws Exception {
        // Orphan .pv and .pc files of a stale instance of s in a parquet partition and in a native one.
        // VACUUM alone removes both sets, without removing a directory.
        final ArrayList<String> directoryRemovals = new ArrayList<>();
        assertMemoryLeak(newOrphanPurgeFilesFacade(directoryRemovals, new AtomicReference<>(), new AtomicInteger()), () -> {
            for (String walMode : WAL_MODES) {
                execute("DROP TABLE IF EXISTS pt");
                createPostingTableOfColumnNameTxnNone(walMode, POSTING_INDEX_CLAUSES[0]);
                insertPostingRows();
                execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
                drainWalQueue();
                final String[] partitionDirs = livePartitionDirs("pt");
                Assert.assertEquals(walMode, 2, partitionDirs.length);
                Assert.assertTrue(walMode, listFiles("pt", partitionDirs[0]).contains("data.parquet"));
                Assert.assertFalse(walMode, listFiles("pt", partitionDirs[1]).contains("data.parquet"));
                // txn 1 is committed and is not the column name txn of s or d in either partition
                final long staleTxn = 1;
                Assert.assertTrue(walMode, getTableTxn() > staleTxn);
                final String[] liveFiles = new String[2];
                try (Path path = new Path()) {
                    final String valueFile = fileName(PostingIndexUtils.valueFileName(path.of(""), "s", staleTxn, 0));
                    final String coverDataFile = fileName(PostingIndexUtils.coverDataFileName(path.of(""), "s", 0, staleTxn, TableUtils.COLUMN_NAME_TXN_NONE, 0));
                    for (int i = 0; i < 2; i++) {
                        Assert.assertEquals(walMode, TableUtils.COLUMN_NAME_TXN_NONE, getColumnNameTxn("pt", "s", i));
                        liveFiles[i] = listFiles("pt", partitionDirs[i]);
                        Assert.assertTrue(walMode + ", " + liveFiles[i], hasCoverDataFile(liveFiles[i]));
                        path.of(configuration.getDbRoot()).concat(engine.verifyTableName("pt")).concat(partitionDirs[i]);
                        final int pathLen = path.size();
                        Assert.assertTrue(configuration.getFilesFacade().touch(path.trimTo(pathLen).concat(valueFile).$()));
                        Assert.assertTrue(configuration.getFilesFacade().touch(path.trimTo(pathLen).concat(coverDataFile).$()));
                        Assert.assertEquals(walMode, sortedFiles(liveFiles[i], valueFile, coverDataFile), listFiles("pt", partitionDirs[i]));
                    }
                }

                vacuumTablePt();
                Assert.assertEquals(walMode, "[]", directoryRemovals.toString());
                for (int i = 0; i < 2; i++) {
                    Assert.assertEquals(walMode + ", " + partitionDirs[i], liveFiles[i], listFiles("pt", partitionDirs[i]));
                }
                try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                    runPurgeJob(purgeJob);
                    Assert.assertEquals(walMode, 0, purgeJob.getOutstandingPurgeTasks());
                }
                for (int i = 0; i < 2; i++) {
                    Assert.assertEquals(walMode + ", " + partitionDirs[i], liveFiles[i], listFiles("pt", partitionDirs[i]));
                }
                assertPostingRowsOfY(walMode, true, POSTING_ROWS_OF_Y);
            }
        });
    }

    @Test
    public void testVacuumPurgesOrphanPostingFilesWhenMiddlePartitionRemoveFails() throws Exception {
        // The removal of an orphan .pv file of s in the middle one of three partitions fails once. VACUUM
        // removes the orphan files of the other two partitions and of the rest of the middle one, and the
        // column purge job removes the failed file on retry.
        final ArrayList<String> directoryRemovals = new ArrayList<>();
        final AtomicReference<String> failingFile = new AtomicReference<>();
        final AtomicInteger failedRemoveCount = new AtomicInteger();
        assertMemoryLeak(newOrphanPurgeFilesFacade(directoryRemovals, failingFile, failedRemoveCount), () -> {
            final String context = POSTING_INDEX_CLAUSES[0];
            final OrphanIndexFiles orphans = createOrphanIndexFiles(context, "BYPASS WAL", "DAY", POSTING_INDEX_CLAUSES[0]);
            Assert.assertEquals(3, orphans.partitionDirs.length);
            // VACUUM reads the token 0 in the .pc names of the old instance as txn 0 and purges that txn
            // too, which removes the same .pc files again; only the old instance owns its .pv files
            final String orphanValueFile = "s.pv.1";
            Assert.assertTrue((" " + listFiles("pt", orphans.partitionDirs[1]) + " ").contains(" " + orphanValueFile + " "));
            failingFile.set(orphans.partitionDirs[1] + Files.SEPARATOR + orphanValueFile);

            vacuumTablePt();
            Assert.assertEquals("[]", directoryRemovals.toString());
            Assert.assertEquals(1, failedRemoveCount.get());
            Assert.assertEquals(orphans.liveFiles[0], listFiles("pt", orphans.partitionDirs[0]));
            Assert.assertEquals(sortedFiles(orphans.liveFiles[1], orphanValueFile), listFiles("pt", orphans.partitionDirs[1]));
            Assert.assertEquals(orphans.liveFiles[2], listFiles("pt", orphans.partitionDirs[2]));

            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                runPurgeJob(purgeJob);
                Assert.assertEquals(0, purgeJob.getOutstandingPurgeTasks());
            }
            orphans.assertLiveFilesOnly("after the retry");
            Assert.assertEquals(1, failedRemoveCount.get());
            Assert.assertEquals("[]", directoryRemovals.toString());
            assertOrphanIndexFilesTableRows(context, true);
        });
    }

    @Test
    public void testVacuumPurgesPostingFilesOfReAddedColumn() throws Exception {
        // s is dropped while a reader holds the table, so its NONE instance stays on disk, and added back
        // as a new instance T. VACUUM removes the NONE files and keeps those of T.
        assertMemoryLeak(() -> {
            for (String walMode : WAL_MODES) {
                execute("DROP TABLE IF EXISTS pt");
                createPostingTableOfColumnNameTxnNone(walMode, POSTING_INDEX_CLAUSES[0]);
                insertPostingRows();
                final String[] partitionDirs = livePartitionDirs("pt");
                Assert.assertEquals(walMode, 2, partitionDirs.length);
                // the files of instance NONE
                final String[] staleFiles = new String[2];
                for (int i = 0; i < 2; i++) {
                    staleFiles[i] = filesOf(listFiles("pt", partitionDirs[i]), "s.");
                    Assert.assertTrue(walMode + ", " + staleFiles[i], hasCoverDataFile(staleFiles[i]));
                }
                Assert.assertTrue(walMode, (" " + staleFiles[0] + " ").contains(" s.d s.pc0.0."));
                Assert.assertTrue(walMode, (" " + staleFiles[0] + " ").contains(" s.pci s.pk s.pv."));
                try (TableReader ignore = getReader("pt")) {
                    execute("ALTER TABLE pt DROP COLUMN s");
                    drainWalQueue();
                }
                execute("ALTER TABLE pt ADD COLUMN s SYMBOL");
                execute("ALTER TABLE pt ALTER COLUMN s ADD INDEX TYPE POSTING INCLUDE (d, vc)");
                drainWalQueue();
                execute("INSERT INTO pt (v, ts, s, d, vc) VALUES (6, '2024-01-01T05:00:00.000000Z', 'y', 6.5, 'a6')");
                drainWalQueue();
                final long columnNameTxn = getColumnNameTxn("pt", "s", 0);
                Assert.assertTrue(walMode, columnNameTxn > 0);
                Assert.assertArrayEquals(walMode, partitionDirs, livePartitionDirs("pt"));

                final String[] expected = new String[2];
                for (int i = 0; i < 2; i++) {
                    final ArrayList<String> files = new ArrayList<>(Arrays.asList(listFiles("pt", partitionDirs[i]).split(" ")));
                    files.removeAll(Arrays.asList(staleFiles[i].split(" ")));
                    expected[i] = String.join(" ", files);
                }
                // 2024-01-01 keeps the files of instance T
                Assert.assertTrue(walMode + ", " + expected[0], expected[0].contains("s.pci." + columnNameTxn));

                engine.releaseInactive();
                execute("VACUUM TABLE pt");
                for (int i = 0; i < 2; i++) {
                    Assert.assertEquals(walMode + ", " + partitionDirs[i], expected[i], listFiles("pt", partitionDirs[i]));
                }
                try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                    runPurgeJob(purgeJob);
                    Assert.assertEquals(0, purgeJob.getOutstandingPurgeTasks());
                }
                for (int i = 0; i < 2; i++) {
                    Assert.assertEquals(walMode + ", " + partitionDirs[i], expected[i], listFiles("pt", partitionDirs[i]));
                }
                assertPostingRowsOfY(walMode, true, """
                        ts\ts\td\tvc
                        2024-01-01T05:00:00.000000Z\ty\t6.5\ta6
                        """);
                assertQuery("SELECT v, s FROM pt")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                v\ts
                                1\t
                                2\t
                                3\t
                                4\t
                                6\ty
                                5\t
                                """);
            }
        });
    }

    @Test
    public void testVacuumPurgesStaleFilesOfColumnConvertedAtTxnZeroNonPartitioned() throws Exception {
        // Same shape as testVacuumKeepsPostingFilesOfColumnConvertedAtTxnZeroNonPartitioned with a column
        // that has no POSTING index: VACUUM purges the file of instance NONE.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE pt (v INT, ts TIMESTAMP, x INT) TIMESTAMP(ts)");
            try (TableReader ignore = getReader("pt")) {
                execute("ALTER TABLE pt ALTER COLUMN x TYPE LONG");
            }
            execute("INSERT INTO pt (v, ts, x) VALUES (1, '2024-01-01T00:00:00.000000Z', 10), (2, '2024-01-02T00:00:00.000000Z', 20)");
            Assert.assertEquals(0, getColumnNameTxn("pt", "x", 0));
            Assert.assertEquals("x.d x.d.0", filesOf(listFiles("pt", "default"), "x."));

            engine.releaseInactive();
            execute("VACUUM TABLE pt");
            Assert.assertEquals("x.d.0", filesOf(listFiles("pt", "default"), "x."));
            assertQuery("SELECT v, ts, x FROM pt")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            v\tts\tx
                            1\t2024-01-01T00:00:00.000000Z\t10
                            2\t2024-01-02T00:00:00.000000Z\t20
                            """);
        });
    }

    @Test
    public void testVacuumPurgesStalePostingSealedFiles() throws Exception {
        // Orphan .pv and .pc files of an old instance of s, whose .pk and .d are already gone.
        assertMemoryLeak(() -> {
            final StalePostingInstance stale = createStalePostingInstance();
            final int outstandingPurgeTasks = vacuumAndPurge();
            Assert.assertEquals(stale.liveFiles, listFiles("pt", stale.partitionDir));
            Assert.assertEquals(0, outstandingPurgeTasks);
            assertPostingRowsOfY("after VACUUM", true, stale.rowsOfY);
        });
    }

    @Test
    public void testVacuumPurgesStaleRootSymbolFilesOfTxnNone() throws Exception {
        // A symbol capacity change before the first commit writes the symbol table files of txn 0 next
        // to the CREATE TABLE ones of txn NONE. VACUUM purges the NONE files in the table root, also for
        // a POSTING column, whose root files carry no posting index.
        assertMemoryLeak(() -> {
            for (String walMode : WAL_MODES) {
                for (boolean isPosting : new boolean[]{true, false}) {
                    final String context = walMode + (isPosting ? ", POSTING" : ", no index");
                    execute("DROP TABLE IF EXISTS pt");
                    execute("CREATE TABLE pt (v INT, ts TIMESTAMP, s SYMBOL" + (isPosting ? " INDEX TYPE POSTING INCLUDE (d, vc)" : "")
                            + ", d DOUBLE, vc VARCHAR) TIMESTAMP(ts) PARTITION BY DAY " + walMode);
                    try (TableReader ignore = getReader("pt")) {
                        execute("ALTER TABLE pt ALTER COLUMN s SYMBOL CAPACITY 1024");
                        drainWalQueue();
                    }
                    final String[] noneFiles;
                    final String[] zeroFiles;
                    try (Path path = new Path(); TableReader reader = getReader("pt")) {
                        final int writerIndex = reader.getMetadata().getWriterIndex(reader.getMetadata().getColumnIndex("s"));
                        Assert.assertEquals(context, 0, reader.getColumnVersionReader().getSymbolTableNameTxn(writerIndex));
                        noneFiles = symbolTableFiles(path, TableUtils.COLUMN_NAME_TXN_NONE);
                        zeroFiles = symbolTableFiles(path, 0);
                    }
                    Assert.assertEquals(context, sortedFiles(String.join(" ", noneFiles), String.join(" ", zeroFiles)), filesOf(listFiles("pt", ""), "s."));

                    insertPostingRows();
                    final String partitionDir = livePartitionDirs("pt")[0];
                    final String partitionFiles = listFiles("pt", partitionDir);
                    Assert.assertEquals(context, isPosting, hasCoverDataFile(partitionFiles));
                    engine.releaseInactive();
                    execute("VACUUM TABLE pt");
                    Assert.assertEquals(context, String.join(" ", zeroFiles), filesOf(listFiles("pt", ""), "s."));
                    try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                        runPurgeJob(purgeJob);
                        Assert.assertEquals(0, purgeJob.getOutstandingPurgeTasks());
                    }
                    Assert.assertEquals(context, String.join(" ", zeroFiles), filesOf(listFiles("pt", ""), "s."));
                    Assert.assertEquals(context, partitionFiles, listFiles("pt", partitionDir));
                    assertPostingRowsOfY(context, isPosting, POSTING_ROWS_OF_Y);
                }
            }
        });
    }

    private static String fileName(LPSZ name) {
        return Utf8s.stringFromUtf8Bytes(name);
    }

    /**
     * A files facade that records every removeQuiet() whose target is a directory, and fails the first
     * removal of the file whose path ends with the value of {@code failingFile}.
     */
    private static FilesFacade newOrphanPurgeFilesFacade(
            ArrayList<String> directoryRemovals,
            AtomicReference<String> failingFile,
            AtomicInteger failedRemoveCount
    ) {
        return new TestFilesFacadeImpl() {
            @Override
            public boolean removeQuiet(LPSZ name) {
                if (new File(fileName(name)).isDirectory()) {
                    directoryRemovals.add(fileName(name));
                }
                final String file = failingFile.get();
                if (file != null && Utf8s.endsWithAscii(name, file)) {
                    failingFile.set(null);
                    failedRemoveCount.incrementAndGet();
                    return false;
                }
                return super.removeQuiet(name);
            }
        };
    }

    // the files of the space-separated listing that are not in the space-separated files
    private static String withoutFiles(String listing, String files) {
        final ArrayList<String> remaining = new ArrayList<>(Arrays.asList(listing.split(" ")));
        remaining.removeAll(Arrays.asList(files.split(" ")));
        return String.join(" ", remaining);
    }

    // the files of the listing whose names start with prefix
    private static String filesOf(String listing, String prefix) {
        final ArrayList<String> files = new ArrayList<>();
        for (String file : listing.split(" ")) {
            if (file.startsWith(prefix)) {
                files.add(file);
            }
        }
        return String.join(" ", files);
    }

    private static long getColumnNameTxn(String tableName, String columnName, int partitionIndex) {
        try (TableReader reader = getReader(tableName)) {
            final int writerIndex = reader.getMetadata().getWriterIndex(reader.getMetadata().getColumnIndex(columnName));
            return reader.getColumnVersionReader().getColumnNameTxn(reader.getPartitionTimestampByIndex(partitionIndex), writerIndex);
        }
    }

    private static boolean hasCoverDataFile(String listing) {
        for (String file : listing.split(" ")) {
            if (isCoverDataFile(file)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isCoverDataFile(String file) {
        return file.startsWith("s.pc") && !file.startsWith("s.pci");
    }

    // the files of a directory of the table, "" for the table root, in name order
    private static String listFiles(String tableName, String dir) {
        final File tableDir = new File(configuration.getDbRoot().toString(), engine.verifyTableName(tableName).getDirName());
        final File[] files = (dir.isEmpty() ? tableDir : new File(tableDir, dir)).listFiles(File::isFile);
        Assert.assertNotNull(dir, files);
        final String[] names = new String[files.length];
        for (int i = 0; i < files.length; i++) {
            names[i] = files[i].getName();
        }
        Arrays.sort(names);
        return String.join(" ", names);
    }

    // the directories of the partitions that the table reader sees
    private static String[] livePartitionDirs(String tableName) {
        try (TableReader reader = getReader(tableName)) {
            final String[] dirs = new String[reader.getTxFile().getPartitionCount()];
            final StringSink sink = new StringSink();
            for (int i = 0; i < dirs.length; i++) {
                sink.clear();
                TableUtils.setSinkForNativePartition(
                        sink,
                        reader.getMetadata().getTimestampType(),
                        reader.getPartitionedBy(),
                        reader.getTxFile().getPartitionTimestampByIndex(i),
                        reader.getTxFile().getPartitionNameTxn(i)
                );
                dirs[i] = sink.toString();
            }
            return dirs;
        }
    }

    // the space-separated file names of the listings, in name order
    private static String sortedFiles(String... listings) {
        final String[] files = String.join(" ", listings).split(" ");
        Arrays.sort(files);
        return String.join(" ", files);
    }

    private static String[] symbolTableFiles(Path path, long symbolTableNameTxn) {
        return new String[]{
                fileName(TableUtils.charFileName(path.of(""), "s", symbolTableNameTxn)),
                fileName(BitmapIndexUtils.keyFileName(path.of(""), "s", symbolTableNameTxn)),
                fileName(TableUtils.offsetFileName(path.of(""), "s", symbolTableNameTxn)),
                fileName(BitmapIndexUtils.valueFileName(path.of(""), "s", symbolTableNameTxn))
        };
    }

    private void assertPostingRowsOfY(String context, boolean isCovering, String expected) throws Exception {
        final String query = "SELECT ts, s, d, vc FROM pt WHERE s = 'y'";
        final QueryAssertion assertion = assertQuery(query)
                .noLeakCheck()
                .supportsRandomAccess(!isCovering)
                .expectSize(isCovering)
                .timestamp("ts");
        if (isCovering) {
            assertion.withPlanContaining("CoveringIndex on: s with: ts, d, vc");
        }
        try {
            assertion.returns(expected);
        } catch (AssertionError e) {
            throw new AssertionError(context, e);
        }
        // the same rows through a filter that reads no index
        assertSqlCursors("SELECT ts, s, d, vc FROM pt WHERE s::STRING = 'y'", query);
    }

    /**
     * Lists every live partition directory of pt, runs VACUUM TABLE and the column purge job, and
     * asserts that no file changed, that the covered rows of s = 'y' read back, and again after the
     * table is reopened.
     */
    private void assertVacuumKeepsPostingFiles(String context, boolean isCovering, String expectedRowsOfY) throws Exception {
        final String[] partitionDirs = livePartitionDirs("pt");
        final String[] before = new String[partitionDirs.length];
        for (int i = 0; i < partitionDirs.length; i++) {
            before[i] = listFiles("pt", partitionDirs[i]);
        }
        Assert.assertEquals(context, isCovering, hasCoverDataFile(before[0]));
        assertPostingRowsOfY(context + ", before VACUUM", isCovering, expectedRowsOfY);

        final int outstandingPurgeTasks = vacuumAndPurge();
        for (int i = 0; i < partitionDirs.length; i++) {
            Assert.assertEquals(context + ", " + partitionDirs[i], before[i], listFiles("pt", partitionDirs[i]));
        }
        Assert.assertEquals(context, 0, outstandingPurgeTasks);
        assertPostingRowsOfY(context + ", after VACUUM", isCovering, expectedRowsOfY);

        engine.releaseAllReaders();
        engine.releaseAllWriters();
        assertPostingRowsOfY(context + ", reopened", isCovering, expectedRowsOfY);
    }

    private void assertOrphanIndexFilesTableRows(String context, boolean isCovering) throws Exception {
        assertPostingRowsOfY(context, isCovering, """
                ts\ts\td\tvc
                2024-01-01T00:00:00.000000Z\ty\t1.5\ta1
                2024-01-02T00:00:00.000000Z\ty\t2.5\ta2
                """);
        try {
            assertQuery("SELECT v, s, b, w, str, x FROM pt")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            v\ts\tb\tw\tstr\tx
                            1\ty\tp\tw1\ts1\t10
                            4\tz\tz\tz\tz\t41
                            2\ty\tq\tw2\ts2\t20
                            5\tz\tz\tz\tz\t51
                            3\tx\tp\tw3\ts3\t30
                            6\tz\tz\tz\tz\t61
                            """);
        } catch (AssertionError e) {
            throw new AssertionError(context, e);
        }
    }

    /**
     * Creates pt with the POSTING-indexed s, the BITMAP-indexed b, VARCHAR w, STRING str and INT x, all
     * in CREATE TABLE, writes two commits into each of three days, and updates all five columns in every
     * partition with a reader open, so the old instances stay on disk; a purge job records the purge
     * tasks of the UPDATE in its log. Then deletes s.d, s.pk, b.d, w.d and str.d of the old instances,
     * which leaves their index and .i files, and the .d of x, as orphans.
     */
    private OrphanIndexFiles createOrphanIndexFiles(String context, String walMode, String partitionBy, String indexClause) throws SqlException {
        execute("DROP TABLE IF EXISTS pt");
        execute("CREATE TABLE pt (v INT, ts TIMESTAMP, s SYMBOL INDEX TYPE " + indexClause
                + ", d DOUBLE, vc VARCHAR, b SYMBOL INDEX, w VARCHAR, str STRING, x INT) TIMESTAMP(ts) PARTITION BY "
                + partitionBy + " " + walMode);
        // the six rows in timestamp order; each commit writes a row into each day, or, for a table that is
        // not partitioned and takes no out-of-order rows, the first three and the last three rows
        final String[] rows = {
                "(1, '2024-01-01T00:00:00.000000Z', 'y', 1.5, 'a1', 'p', 'w1', 's1', 10)",
                "(4, '2024-01-01T01:00:00.000000Z', 'x', 4.5, 'a4', 'q', 'w4', 's4', 40)",
                "(2, '2024-01-02T00:00:00.000000Z', 'y', 2.5, 'a2', 'q', 'w2', 's2', 20)",
                "(5, '2024-01-02T01:00:00.000000Z', 'y', 5.5, 'a5', 'p', 'w5', 's5', 50)",
                "(3, '2024-01-03T00:00:00.000000Z', 'x', 3.5, 'a3', 'p', 'w3', 's3', 30)",
                "(6, '2024-01-03T01:00:00.000000Z', 'x', 6.5, 'a6', 'q', 'w6', 's6', 60)"
        };
        final boolean isPartitioned = !partitionBy.equals("NONE");
        execute("INSERT INTO pt (v, ts, s, d, vc, b, w, str, x) VALUES "
                + (isPartitioned ? String.join(", ", rows[0], rows[2], rows[4]) : String.join(", ", rows[0], rows[1], rows[2])));
        drainWalQueue();
        execute("INSERT INTO pt (v, ts, s, d, vc, b, w, str, x) VALUES "
                + (isPartitioned ? String.join(", ", rows[1], rows[3], rows[5]) : String.join(", ", rows[3], rows[4], rows[5])));
        drainWalQueue();
        final String[] partitionDirs = livePartitionDirs("pt");
        final String[] oldInstanceFiles = new String[partitionDirs.length];
        for (int i = 0; i < partitionDirs.length; i++) {
            final String listing = listFiles("pt", partitionDirs[i]);
            oldInstanceFiles[i] = sortedFiles(
                    filesOf(listing, "s."), filesOf(listing, "b."), filesOf(listing, "w."), filesOf(listing, "str."), filesOf(listing, "x.")
            );
            Assert.assertTrue(context + ", " + oldInstanceFiles[i], (" " + oldInstanceFiles[i] + " ").contains(" s.pv."));
        }

        try (TableReader ignore = getReader("pt"); ColumnPurgeJob purgeJob = createPurgeJob()) {
            update("UPDATE pt SET s = 'z', b = 'z', w = 'z', str = 'z', x = x + 1 WHERE v > 3");
            drainWalQueue();
            // the reader holds the old instances, so the purge job only records the purge tasks in its
            // log; the purge queue holds two tasks, so the UPDATE drops the purge tasks of the others
            runPurgeJob(purgeJob);
        }
        Assert.assertArrayEquals(context, partitionDirs, livePartitionDirs("pt"));

        final String[] liveFiles = new String[partitionDirs.length];
        final File tableDir = new File(configuration.getDbRoot().toString(), engine.verifyTableName("pt").getDirName());
        for (int i = 0; i < partitionDirs.length; i++) {
            final String listing = listFiles("pt", partitionDirs[i]);
            Assert.assertEquals(context, oldInstanceFiles[i], withoutFiles(listing, withoutFiles(listing, oldInstanceFiles[i])));
            liveFiles[i] = withoutFiles(listing, oldInstanceFiles[i]);
            for (String file : new String[]{"s.d", "s.pk", "b.d", "w.d", "str.d"}) {
                Assert.assertTrue(context + ", " + file, new File(new File(tableDir, partitionDirs[i]), file).delete());
            }
        }
        return new OrphanIndexFiles(partitionDirs, liveFiles);
    }

    private void createPostingTableOfColumnNameTxnNone(String walMode, String indexClause) throws SqlException {
        execute("CREATE TABLE pt (v INT, ts TIMESTAMP, s SYMBOL INDEX TYPE " + indexClause
                + ", d DOUBLE, vc VARCHAR) TIMESTAMP(ts) PARTITION BY DAY " + walMode);
    }

    private void createPostingTableOfColumnNameTxnZero(String walMode, String indexClause) throws SqlException {
        execute("CREATE TABLE pt (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY " + walMode);
        execute("ALTER TABLE pt ADD COLUMN s SYMBOL");
        execute("ALTER TABLE pt ADD COLUMN d DOUBLE");
        execute("ALTER TABLE pt ADD COLUMN vc VARCHAR");
        execute("ALTER TABLE pt ALTER COLUMN s ADD INDEX TYPE " + indexClause);
        drainWalQueue();
    }

    /**
     * Creates pt where s is added after the first commit (column name txn T), UPDATE rewrites s in
     * 2024-01-01 (instance U), and plants there the orphan .pv and .pc files of T, built by the encoders.
     */
    private StalePostingInstance createStalePostingInstance() throws SqlException {
        execute("CREATE TABLE pt (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
        execute("INSERT INTO pt (v, ts) VALUES (0, '2024-01-03T00:00:00.000000Z')");
        execute("ALTER TABLE pt ADD COLUMN s SYMBOL");
        execute("ALTER TABLE pt ADD COLUMN d DOUBLE");
        execute("ALTER TABLE pt ADD COLUMN vc VARCHAR");
        execute("ALTER TABLE pt ALTER COLUMN s ADD INDEX TYPE POSTING INCLUDE (d, vc)");
        insertPostingRows();
        final long staleTxn = getColumnNameTxn("pt", "s", 0);
        Assert.assertTrue(staleTxn > 0);
        final long coveredTxn;
        try (TableReader reader = getReader("pt")) {
            final int writerIndex = reader.getMetadata().getWriterIndex(reader.getMetadata().getColumnIndex("d"));
            coveredTxn = reader.getColumnVersionReader().getColumnNameTxn(reader.getPartitionTimestampByIndex(0), writerIndex);
        }
        update("UPDATE pt SET s = 'y' WHERE v = 3");
        Assert.assertTrue(getColumnNameTxn("pt", "s", 0) > staleTxn);
        final String partitionDir = livePartitionDirs("pt")[0];
        final String liveFiles = listFiles("pt", partitionDir);
        Assert.assertTrue(liveFiles, hasCoverDataFile(liveFiles));

        final String valueFile;
        final String coverDataFile;
        try (Path path = new Path()) {
            valueFile = fileName(PostingIndexUtils.valueFileName(path.of(""), "s", staleTxn, 0));
            coverDataFile = fileName(PostingIndexUtils.coverDataFileName(path.of(""), "s", 0, staleTxn, coveredTxn, 0));
            path.of(configuration.getDbRoot()).concat(engine.verifyTableName("pt")).concat(partitionDir);
            final int pathLen = path.size();
            Assert.assertFalse(configuration.getFilesFacade().exists(PostingIndexUtils.keyFileName(path, "s", staleTxn)));
            Assert.assertFalse(configuration.getFilesFacade().exists(TableUtils.dFile(path.trimTo(pathLen), "s", staleTxn)));
            Assert.assertTrue(configuration.getFilesFacade().touch(path.trimTo(pathLen).concat(valueFile).$()));
            Assert.assertTrue(configuration.getFilesFacade().touch(path.trimTo(pathLen).concat(coverDataFile).$()));
        }
        Assert.assertEquals(sortedFiles(liveFiles, valueFile, coverDataFile), listFiles("pt", partitionDir));
        return new StalePostingInstance(partitionDir, liveFiles, coverDataFile, """
                ts\ts\td\tvc
                2024-01-01T00:00:00.000000Z\ty\t1.5\ta1
                2024-01-01T01:00:00.000000Z\ty\t2.5\ta2
                2024-01-01T02:00:00.000000Z\ty\t3.5\ta3
                2024-01-01T03:00:00.000000Z\ty\t4.5\ta4
                2024-01-02T00:00:00.000000Z\ty\t5.5\ta5
                """);
    }

    /**
     * Inserts two commits into pt: three rows into 2024-01-01, then one more row there and a single row
     * into 2024-01-02, so the index of 2024-01-01 is sealed twice.
     */
    private void insertPostingRows() throws SqlException {
        execute("""
                INSERT INTO pt (v, ts, s, d, vc) VALUES
                (1, '2024-01-01T00:00:00.000000Z', 'y', 1.5, 'a1'),
                (2, '2024-01-01T01:00:00.000000Z', 'y', 2.5, 'a2'),
                (3, '2024-01-01T02:00:00.000000Z', 'x', 3.5, 'a3')""");
        drainWalQueue();
        execute("""
                INSERT INTO pt (v, ts, s, d, vc) VALUES
                (4, '2024-01-01T03:00:00.000000Z', 'y', 4.5, 'a4'),
                (5, '2024-01-02T00:00:00.000000Z', 'y', 5.5, 'a5')""");
        drainWalQueue();
    }

    // runs VACUUM TABLE pt, then the column purge job until it has no task left or ran 10 times, and
    // returns the purge tasks left outstanding
    private int vacuumAndPurge() throws SqlException {
        vacuumTablePt();
        try (ColumnPurgeJob purgeJob = createPurgeJob()) {
            for (int i = 0; i < 10 && (i == 0 || purgeJob.getOutstandingPurgeTasks() > 0); i++) {
                runPurgeJob(purgeJob);
            }
            return purgeJob.getOutstandingPurgeTasks();
        }
    }

    /**
     * Runs VACUUM TABLE pt through a new compiler. VacuumColumnVersions takes its files facade when the
     * compiler is created, so a pooled compiler would purge with the files facade of an earlier test.
     */
    private void vacuumTablePt() throws SqlException {
        engine.releaseInactive();
        try (SqlCompiler compiler = new SqlCompilerImpl(engine)) {
            execute(compiler, "VACUUM TABLE pt");
        }
    }

    private record StalePostingInstance(String partitionDir, String liveFiles, String coverDataFile, String rowsOfY) {
    }

    private record OrphanIndexFiles(String[] partitionDirs, String[] liveFiles) {
        void assertLiveFilesOnly(String context) {
            for (int i = 0; i < partitionDirs.length; i++) {
                Assert.assertEquals(context + ", " + partitionDirs[i], liveFiles[i], listFiles("pt", partitionDirs[i]));
            }
        }
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

    private static long getTableTxn() {
        try (TableReader reader = getReader("pt")) {
            return reader.getTxn();
        }
    }

    private static String versionSuffix(long columnNameTxn) {
        return columnNameTxn > -1 ? "." + columnNameTxn : "";
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
