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

package io.questdb.test.cairo;

import io.questdb.cairo.*;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.log.LogFactory;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.*;

import java.util.concurrent.atomic.AtomicInteger;

public class RecoverVarIndexTest extends AbstractCairoTest {
    private static SqlCompiler compiler;
    private static SqlExecutionContext sqlExecutionContext;
    private final RecoverVarIndex rebuildVarColumn = new RecoverVarIndex();
    TableWriter tempWriter;

    @BeforeClass
    public static void setUpStatic() throws Exception {
        AbstractCairoTest.setUpStatic();
        compiler = new SqlCompiler(engine);
        sqlExecutionContext = TestUtils.createSqlExecutionCtx(engine, new BindVariableServiceImpl(configuration));
    }

    @AfterClass
    public static void tearDownStatic() throws Exception {
        compiler.close();
        LogFactory.configureAsync();
        AbstractCairoTest.tearDownStatic();
    }

    @After
    public void cleanup() {
        rebuildVarColumn.close();
    }

    @Test
    public void testEmptyTable() throws Exception {
        String createTableSql = "create table xxx as (" +
                "select " +
                "rnd_symbol('A', 'B', 'C') as sym1," +
                "rnd_symbol(4,4,4,2) as sym2," +
                "x," +
                "timestamp_sequence(0, 100000000) ts " +
                "from long_sequence(0)" +
                "), index(sym1), index(sym2) timestamp(ts) PARTITION BY DAY";

        checkRecoverVarIndex(
                createTableSql,
                (tablePath) -> {
                },
                RecoverVarIndex::rebuildAll
        );
    }

    @Test
    public void testNonPartitionedWithColumnTop() throws Exception {
        assertMemoryLeak(() -> {
            String createAlterInsertSql = "create table xxx as (" +
                    "select " +
                    "rnd_str('A', 'Bbb', 'Ccccc') as str1," +
                    "x," +
                    "timestamp_sequence(0, 100000000) ts " +
                    "from long_sequence(5000)" +
                    "); " +

                    "alter table xxx add column str2 string; " +

                    "insert into xxx " +
                    "select " +
                    "rnd_str('A', 'B', 'C') as str1," +
                    "x," +
                    "timestamp_sequence(100000000L * 5000L, 100000000) ts, " +
                    "rnd_str(4,4,4,2) as str2 " +
                    "from long_sequence(5000)";

            checkRecoverVarIndex(createAlterInsertSql,
                    tablePath -> removeFileAtPartition("str2.i.1", PartitionBy.NONE, tablePath, 0, -1L),
                    rebuildIndex -> rebuildIndex.reindexColumn("str2"));
        });
    }

    @Test
    public void testNonPartitionedWithColumnTopAddedLast() throws Exception {
        assertMemoryLeak(() -> {
            String createAlterInsertSql = "create table xxx as (" +
                    "select " +
                    "x," +
                    "timestamp_sequence(0, 100000000) ts " +
                    "from long_sequence(5000)" +
                    ");" +
                    "alter table xxx add column str1 string;" +
                    "alter table xxx add column str2 string";

            checkRecoverVarIndex(createAlterInsertSql,
                    tablePath -> {
                    },
                    RecoverVarIndex::rebuildAll);

            engine.releaseAllWriters();
            compiler
                    .compile("insert into xxx values(500100000000L, 50001, 'D', 'I2')", sqlExecutionContext)
                    .getInsertOperation()
                    .execute(sqlExecutionContext)
                    .await();
            int sym1D = countByFullScanWhereValueD();
            Assert.assertEquals(1, sym1D);
        });
    }

    @Test
    public void testNonePartitionedOneColumn() throws Exception {
        assertMemoryLeak(() -> {
            String createTableSql = "create table xxx as (" +
                    "select " +
                    "rnd_str('A', 'Bbb', 'Ccccc') as str1," +
                    "rnd_str('A', 'Bbb', 'Ccccc', '412312', '2212321') as str2," +
                    "x," +
                    "timestamp_sequence(0, 100000000) ts " +
                    "from long_sequence(10000)" +
                    ")";

            checkRecoverVarIndex(createTableSql,
                    tablePath -> removeFileAtPartition("str1.i", PartitionBy.NONE, tablePath, 0, -1L),
                    rebuildIndex -> rebuildIndex.reindexColumn("str1"));
        });
    }

    @Test
    public void testPartitionedDaily() throws Exception {
        assertMemoryLeak(() -> {
            String createTableSql = "create table xxx as (" +
                    "select " +
                    "rnd_str('A', 'Bbb', 'Ccccc') as str1," +
                    "rnd_str('A', 'Bbb', 'Ccccc', '412312', '2212321') as str2," +
                    "x," +
                    "timestamp_sequence(0, 100000000) ts " +
                    "from long_sequence(10000)" +
                    ") timestamp(ts) PARTITION BY DAY";

            checkRecoverVarIndex(
                    createTableSql,
                    (tablePath) -> {
                        removeFileAtPartition("str1.i", PartitionBy.DAY, tablePath, 0, -1L);
                        removeFileAtPartition("str2.i", PartitionBy.DAY, tablePath, 0, -1L);
                    },
                    RecoverVarIndex::rebuildAll
            );
        });
    }

    @Test
    public void testPartitionedNone() throws Exception {
        assertMemoryLeak(() -> {
            String createTableSql = "create table xxx as (" +
                    "select " +
                    "rnd_str('A', 'Bbb', 'Ccccc') as str1," +
                    "rnd_str('A', 'Bbb', 'Ccccc', '412312', '2212321') as str2," +
                    "x," +
                    "timestamp_sequence(0, 100000000) ts " +
                    "from long_sequence(10000)" +
                    ")";

            checkRecoverVarIndex(
                    createTableSql,
                    (tablePath) -> {
                        removeFileAtPartition("str1.i", PartitionBy.NONE, tablePath, 0, -1L);
                        removeFileAtPartition("str2.i", PartitionBy.NONE, tablePath, 0, -1L);
                    },
                    RecoverVarIndex::rebuildAll
            );
        });
    }

    @Test
    public void testPartitionedOneColumn() throws Exception {
        assertMemoryLeak(() -> {
            String createTableSql = "create table xxx as (" +
                    "select " +
                    "rnd_str('A', 'Bbb', 'Ccccc') as str1," +
                    "rnd_str('A', 'Bbb', 'Ccccc', '412312', '2212321') as str2," +
                    "x," +
                    "timestamp_sequence(0, 100000000) ts " +
                    "from long_sequence(10000)" +
                    ") timestamp(ts) PARTITION BY DAY";

            checkRecoverVarIndex(createTableSql,
                    tablePath -> removeFileAtPartition("str1.i", PartitionBy.DAY, tablePath, 0, -1L),
                    rebuildIndex -> rebuildIndex.reindexColumn("str1"));
        });
    }

    @Test
    public void testPartitionedOneColumnFirstPartition() throws Exception {
        assertMemoryLeak(() -> {
            String createTableSql = "create table xxx as (" +
                    "select " +
                    "rnd_str('A', 'Bbb', 'Ccccc') as str1," +
                    "rnd_str('A', 'Bbb', 'Ccccc', '412312', '2212321') as str2," +
                    "x," +
                    "timestamp_sequence(0, 100000000) ts " +
                    "from long_sequence(10000)" +
                    ") timestamp(ts) PARTITION BY DAY";

            checkRecoverVarIndex(createTableSql,
                    tablePath -> removeFileAtPartition("str1.i", PartitionBy.DAY, tablePath, 0, -1L),
                    rebuildIndex -> rebuildIndex.reindex("1970-01-01", "str1"));
        });
    }

    @Test
    public void testPartitionedWithColumnTop() throws Exception {
        assertMemoryLeak(() -> {
            String createAlterInsertSql = "create table xxx as (" +
                    "select " +
                    "rnd_str('A', 'Bbb', 'Ccccc') as str1," +
                    "x," +
                    "timestamp_sequence(0, 100000000) ts " +
                    "from long_sequence(5000)" +
                    ") timestamp(ts) PARTITION BY DAY; " +

                    "alter table xxx add column str2 string; " +

                    "insert into xxx " +
                    "select " +
                    "rnd_str('A', 'B', 'C') as str1," +
                    "x," +
                    "timestamp_sequence(100000000L * 5000L, 100000000) ts, " +
                    "rnd_str(4,4,4,2) as str2 " +
                    "from long_sequence(5000)";

            checkRecoverVarIndex(createAlterInsertSql,
                    tablePath -> removeFileAtPartition("str2.i.1", PartitionBy.DAY, tablePath, Timestamps.DAY_MICROS * 11, 1L),
                    rebuildIndex -> rebuildIndex.reindexColumn("str2"));
        });
    }

    @Test
    public void testRebuildColumnTableWriterLockedFails() throws Exception {
        assertMemoryLeak(() -> {
            String createTableSql = "create table xxx as (" +
                    "select " +
                    "rnd_str('A', 'Bbb', 'Ccccc') as str1," +
                    "rnd_str('A', 'Bbb', 'Ccccc', '412312', '2212321') as str2," +
                    "x," +
                    "timestamp_sequence(0, 100000000) ts " +
                    "from long_sequence(10000)" +
                    ") timestamp(ts) PARTITION BY DAY";

            tempWriter = null;
            try {
                checkRecoverVarIndex(createTableSql,
                        tablePath -> tempWriter = getWriter(engine, "xxx"),
                        rebuildIndex -> {
                            try {
                                rebuildIndex.reindexColumn("str1");
                            } finally {
                                tempWriter.close();
                            }
                        });
                Assert.fail();
            } catch (CairoException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "Cannot lock table");
            }
        });
    }

    @Test
    public void testRebuildFailsWriteIFile() throws Exception {
        assertMemoryLeak(() -> {
            String createTableSql = "create table xxx as (" +
                    "select " +
                    "rnd_str('A', 'Bbb', 'Ccccc') as str1," +
                    "rnd_str('A', 'Bbb', 'Ccccc', '412312', '2212321') as str2," +
                    "x," +
                    "timestamp_sequence(0, 100000000) ts " +
                    "from long_sequence(10000)" +
                    ") timestamp(ts) PARTITION BY DAY";

            AtomicInteger count = new AtomicInteger();
            ff = new TestFilesFacadeImpl() {
                @Override
                public int openRW(LPSZ name, long opts) {
                    if (Chars.contains(name, "str2.i") && count.incrementAndGet() == 14) {
                        return -1;
                    }
                    return super.openRW(name, opts);
                }
            };

            try {
                checkRecoverVarIndex(createTableSql,
                        tablePath -> {
                        },
                        rebuildIndex -> rebuildIndex.reindexColumn("str2"));
                Assert.fail();
            } catch (CairoException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "could not open read-write");
            }
        });
    }

    @Test
    public void testRebuildWrongColumn() throws Exception {
        String createTableSql = "create table xxx as (" +
                "select " +
                "rnd_str('A', 'Bbb', 'Ccccc') as str1," +
                "rnd_str('A', 'Bbb', 'Ccccc', '412312', '2212321') as str2," +
                "x," +
                "timestamp_sequence(0, 100000000) ts " +
                "from long_sequence(10000)" +
                ") timestamp(ts) PARTITION BY DAY";

        try {
            checkRecoverVarIndex(createTableSql,
                    tablePath -> {
                    },
                    rebuildIndex -> rebuildIndex.reindexColumn("x"));
            Assert.fail();
        } catch (CairoException ex) {
            TestUtils.assertContains(ex.getFlyweightMessage(), "Wrong column type");
        }
    }

    private void checkRecoverVarIndex(String createTableSql, Action<String> changeTable, Action<RecoverVarIndex> rebuildIndexAction) throws Exception {
        assertMemoryLeak(ff, () -> {
            for (String sql : createTableSql.split(";")) {
                compiler.compile(sql, sqlExecutionContext).execute(null).await();
            }
            compiler.compile("create table copytbl as (select * from xxx)", sqlExecutionContext);

            engine.releaseAllReaders();
            engine.releaseAllWriters();

            TableToken xxx = engine.verifyTableName("xxx");
            String tablePath = configuration.getRoot().toString() + Files.SEPARATOR + xxx.getDirName();
            changeTable.run(tablePath);

            rebuildVarColumn.clear();
            rebuildVarColumn.of(tablePath, configuration);
            rebuildIndexAction.run(rebuildVarColumn);

            TestUtils.assertSqlCursors(compiler, sqlExecutionContext, "copytbl", "xxx", LOG);
        });
    }

    private int countByFullScanWhereValueD() throws SqlException {
        int recordCount = 0;
        try (RecordCursorFactory factory = compiler.compile("select * from xxx where str1 = 'D'", sqlExecutionContext).getRecordCursorFactory()) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                while (cursor.hasNext()) {
                    recordCount++;
                }
            }
        }
        return recordCount;
    }

    private void removeFileAtPartition(String fileName, int partitionBy, String tablePath, long partitionTs, long partitionNameTxn) {
        try (Path path = new Path()) {
            path.concat(tablePath);
            path.put(Files.SEPARATOR);
            PartitionBy.setSinkForPartition(path, partitionBy, partitionTs, false);
            TableUtils.txnPartitionConditionally(path, partitionNameTxn);
            path.concat(fileName);
            LOG.info().$("removing ").utf8(path).$();
            Assert.assertTrue(Files.remove(path.$()));
        }
    }

    @FunctionalInterface
    interface Action<T> {
        void run(T val);
    }
}
