/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnPurgeJob;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlException;
import io.questdb.std.*;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractGriffinTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class VacuumColumnVersionTest extends AbstractGriffinTest {
    private int iteration;

    @BeforeClass
    public static void setUpStatic() throws Exception {
        columnVersionPurgeQueueCapacity = 2;
        AbstractGriffinTest.setUpStatic();
    }

    @Before
    public void setUp() {
        iteration = 1;
        currentMicros = 0;
        columnPurgeRetryDelay = 1;
        columnVersionPurgeQueueCapacity = 2;
        super.setUp();
    }

    @Test
    public void testVacuumPurgesColumnVersionsAfterColumnDrop() throws Exception {
        assertMemoryLeak(() -> {
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                compiler.compile(
                        "create table testPurge as" +
                                " (select timestamp_sequence('1970-01-01', 24 * 60 * 60 * 1000000L) ts," +
                                " x," +
                                " rnd_str('a', 'b', 'c', 'd') str," +
                                " rnd_symbol('A', 'B', 'C', 'D') sym1," +
                                " rnd_symbol('1', '2', '3', '4') sym2" +
                                " from long_sequence(5)), index(sym2)" +
                                " timestamp(ts) PARTITION BY DAY",
                        sqlExecutionContext
                );
                compile("alter table testPurge drop column x");

                try (TableReader rdr = getReader("testPurge")) {
                    executeUpdate("UPDATE testPurge SET sym1='123', str='abcd', sym2='EE' WHERE ts >= '1970-01-02'");
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
                compiler.compile(
                        "create table testPurge as" +
                                " (select timestamp_sequence('1970-01-01', 24 * 60 * 60 * 1000000L) ts," +
                                " x," +
                                " rnd_str('a', 'b', 'c', 'd') str," +
                                " rnd_symbol('A', 'B', 'C', 'D') sym1," +
                                " rnd_symbol('1', '2', '3', '4') sym2" +
                                " from long_sequence(5)), index(sym2)" +
                                " timestamp(ts) PARTITION BY DAY",
                        sqlExecutionContext
                );
                String[] partitions = new String[]{"1970-01-02", "1970-01-03", "1970-01-04"};
                String[] files = {"sym2.d", "sym2.k", "sym2.v"};

                try (TableReader rdr = getReader("testPurge")) {
                    executeUpdate("UPDATE testPurge SET x = 100, sym2='EE' WHERE ts >= '1970-01-02'");
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
            currentMicros = 0;

            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                compiler.compile(
                        "create table testPurge as" +
                                " (select timestamp_sequence('1970-01-01', 24 * 60 * 60 * 1000000L) ts," +
                                " x," +
                                " rnd_str('a', 'b', 'c', 'd') str," +
                                " rnd_symbol('A', 'B', 'C', 'D') sym1," +
                                " rnd_symbol('1', '2', '3', '4') sym2" +
                                " from long_sequence(5)), index(sym2)" +
                                " timestamp(ts) PARTITION BY DAY",
                        sqlExecutionContext
                );

                try (TableReader rdr = getReader("testPurge")) {
                    executeUpdate("UPDATE testPurge SET x = 100, str='abcd', sym2='EE' WHERE ts >= '1970-01-02'");
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
            currentMicros = 0;

            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                compiler.compile(
                        "create table testPurge1 as" +
                                " (select timestamp_sequence('1970-01-01', 24 * 60 * 60 * 1000000L) ts," +
                                " x," +
                                " rnd_str('a', 'b', 'c', 'd') str," +
                                " rnd_symbol('A', 'B', 'C', 'D') sym1," +
                                " rnd_symbol('1', '2', '3', '4') sym2" +
                                " from long_sequence(5)), index(sym2)" +
                                " timestamp(ts) PARTITION BY DAY",
                        sqlExecutionContext
                );

                compiler.compile(
                        "create table testPurge2 as" +
                                " (select timestamp_sequence('1970-01-01', 24 * 60 * 60 * 1000000L) ts," +
                                " x," +
                                " rnd_str('a', 'b', 'c', 'd') str," +
                                " rnd_symbol('A', 'B', 'C', 'D') sym1," +
                                " rnd_symbol('1', '2', '3', '4') sym2" +
                                " from long_sequence(5)), index(sym2)" +
                                " timestamp(ts) PARTITION BY DAY",
                        sqlExecutionContext
                );

                try (
                        TableReader rdr1 = getReader("testPurge1");
                        TableReader rdr2 = getReader("testPurge2")
                ) {
                    executeUpdate("UPDATE testPurge1 SET x = 100, str = 'abc' WHERE ts >= '1970-01-02'");
                    executeUpdate("UPDATE testPurge2 SET x = 100, str = 'dcf' WHERE ts >= '1970-01-02'");
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
            compiler.compile(
                    "create table testPurge as" +
                            " (select timestamp_sequence('1970-01-01', 24 * 60 * 60 * 1000000L) ts," +
                            " x," +
                            " rnd_str('a', 'b', 'c', 'd') str," +
                            " rnd_symbol('A', 'B', 'C', 'D') sym1," +
                            " rnd_symbol('1', '2', '3', '4') sym2" +
                            " from long_sequence(5)), index(sym2)" +
                            " timestamp(ts) PARTITION BY DAY",
                    sqlExecutionContext
            );

            try (TableReader rdr = getReader("testPurge")) {
                executeUpdate("UPDATE testPurge SET x = 100, str='abcd', sym2='EE' WHERE ts >= '1970-01-02'");
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
            currentMicros = 0;

            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                compiler.compile(
                        "create table testPurge as" +
                                " (select timestamp_sequence('1970-01-01', 24 * 60 * 60 * 1000000L) ts," +
                                " x," +
                                " rnd_str('a', 'b', 'c', 'd') str," +
                                " rnd_symbol('A', 'B', 'C', 'D') sym1," +
                                " rnd_symbol('1', '2', '3', '4') sym2" +
                                " from long_sequence(5)), index(sym2)" +
                                " timestamp(ts) PARTITION BY DAY",
                        sqlExecutionContext
                );
                compile("alter table testPurge drop column x");
                compile("alter table testPurge add column x int");
                compile("insert into testPurge(str,sym1,sym2,x,ts) values('str', 'sym1', 'sym2', 123, '1970-02-01')");

                try (TableReader rdr = getReader("testPurge")) {
                    executeUpdate("UPDATE testPurge SET str='abcd', sym2='EE',x=1 WHERE ts >= '1970-01-02'");
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

                Path path = Path.getThreadLocal(configuration.getRoot());
                TableToken tableToken = engine.verifyTableName(tableName);
                path.concat(tableToken).concat(partitions[0]).concat("invalid_file.d");
                FilesFacade ff = configuration.getFilesFacade();
                ff.touch(path.$());
                path.of(configuration.getRoot()).concat(tableToken).concat(partitions[0]).concat("x.d.abcd");
                ff.touch(path.$());

                String[] files = {"x.d"};
                assertFilesExist(partitions, tableName, files, ".2", true);

                runTableVacuum(tableName);
                assertFilesExist(partitions, tableName, files, ".2", false);
                Assert.assertEquals(0, purgeJob.getOutstandingPurgeTasks());

                path.of(configuration.getRoot()).concat(tableToken).concat(partitions[0]).concat("x.d.abcd");
                Assert.assertTrue(ff.exists(path.$()));

                path.of(configuration.getRoot()).concat(tableToken).concat(partitions[0]).concat("invalid_file.d");
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

                Path path = Path.getThreadLocal(configuration.getRoot());
                TableToken tableToken = engine.verifyTableName(tableName);
                path.concat(tableToken).concat("abcd").put(Files.SEPARATOR);
                FilesFacade ff = configuration.getFilesFacade();
                ff.mkdirs(path.$(), configuration.getMkDirMode());

                path.of(configuration.getRoot()).concat(tableToken).concat("2020-01-04.abcd").put(Files.SEPARATOR);
                ff.mkdirs(path.$(), configuration.getMkDirMode());

                String[] files = {"x.d"};
                assertFilesExist(partitions, tableName, files, ".2", true);

                runTableVacuum(tableName);
                assertFilesExist(partitions, tableName, files, ".2", false);
                Assert.assertEquals(0, purgeJob.getOutstandingPurgeTasks());

                path = Path.getThreadLocal(configuration.getRoot());
                path.concat(tableToken).concat("abcd").put(Files.SEPARATOR);
                Assert.assertTrue(ff.exists(path.$()));

                path.of(configuration.getRoot()).concat(tableToken).concat("2020-01-04.abcd").put(Files.SEPARATOR);
                Assert.assertTrue(ff.exists(path.$()));
            }
        });
    }

    private void assertFilesExist(String[] partitions, String tableName, String[] files, String colSuffix, boolean exist) {
        for (int i = 0; i < partitions.length; i++) {
            String partition = partitions[i];
            assertFilesExist(tableName, partition, files, colSuffix, exist);
        }
    }

    private void assertFilesExist(String tableName, String partition, String[] files, String colSuffix, boolean exist) {
        Path path = Path.getThreadLocal(configuration.getRoot());
        TableToken tableToken = engine.verifyTableName(tableName);

        for (int i = files.length - 1; i > -1; i--) {
            String file = files[i];
            path.of(configuration.getRoot()).concat(tableToken).concat(partition).concat(file).put(colSuffix).$();
            Assert.assertEquals(Chars.toString(path), exist, TestFilesFacadeImpl.INSTANCE.exists(path));
        }
    }

    @NotNull
    private ColumnPurgeJob createPurgeJob() throws SqlException {
        return new ColumnPurgeJob(engine, null);
    }

    private void executeUpdate(String query) throws SqlException {
        final CompiledQuery cq = compiler.compile(query, sqlExecutionContext);
        Assert.assertEquals(CompiledQuery.UPDATE, cq.getType());
        try (OperationFuture fut = cq.execute(null)) {
            fut.await();
        }
    }

    private void runPurgeJob(ColumnPurgeJob purgeJob) {
        engine.releaseInactive();
        currentMicros += 10L * iteration++;
        purgeJob.run(0);
        currentMicros += 10L * iteration++;
        purgeJob.run(0);
    }

    private void runTableVacuum(String tableName) throws SqlException {
        if (Os.isWindows()) {
            engine.releaseInactive();
        }
        compile("VACUUM TABLE " + tableName);
    }

    private String[] update3ColumnsWithOpenReader(ColumnPurgeJob purgeJob, String tableName) throws SqlException {
        compile(
                "create table " + tableName + " as" +
                        " (select timestamp_sequence('1970-01-01', 24 * 60 * 60 * 1000000L) ts," +
                        " x," +
                        " rnd_str('a', 'b', 'c', 'd') str," +
                        " rnd_symbol('A', 'B', 'C', 'D') sym1," +
                        " rnd_symbol('1', '2', '3', '4') sym2" +
                        " from long_sequence(5)), index(sym2)" +
                        " timestamp(ts) PARTITION BY DAY"
        );
        compile("alter table " + tableName + " drop column x");
        compile("alter table " + tableName + " add column x int");
        try (TableReader rdr = getReader(tableName)) {
            compile("insert into " + tableName + "(ts, x, str,sym1,sym2) " +
                    "select timestamp_sequence('1970-01-01', 24 * 60 * 60 * 1000000L) ts," +
                    " x," +
                    " rnd_str('a', 'b', 'c', 'd') str," +
                    " rnd_symbol('A', 'B', 'C', 'D') sym1," +
                    " rnd_symbol('1', '2', '3', '4') sym2" +
                    " from long_sequence(5)");

            executeUpdate("UPDATE " + tableName + " SET str='abcd', sym2='EE',x=1 WHERE ts >= '1970-01-02'");
            rdr.openPartition(0);
        }

        runPurgeJob(purgeJob);
        return new String[]{"1970-01-02.3", "1970-01-03.3", "1970-01-04.3"};
    }
}
