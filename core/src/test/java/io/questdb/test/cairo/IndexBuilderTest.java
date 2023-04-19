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
import io.questdb.cairo.sql.InvalidColumnException;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
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

public class IndexBuilderTest extends AbstractCairoTest {
    protected static CharSequence root;
    private static SqlCompiler compiler;
    private static SqlExecutionContext sqlExecutionContext;
    private final IndexBuilder indexBuilder = new IndexBuilder();
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
        AbstractCairoTest.tearDownStatic();
    }

    @After
    public void cleanup() {
        indexBuilder.close();
    }

    @Test
    public void testCannotRemoveOldFiles() throws Exception {
        assertMemoryLeak(() -> {
            String createTableSql = "create table xxx as (" +
                    "select " +
                    "rnd_symbol('A', 'B', 'C') as sym1," +
                    "rnd_symbol(4,4,4,2) as sym2," +
                    "rnd_symbol(4,4,4,2) as sym3," +
                    "x," +
                    "timestamp_sequence(0, 100000000) ts " +
                    "from long_sequence(10000)" +
                    "), index(sym1), index(sym2) timestamp(ts) PARTITION BY DAY";

            ff = new TestFilesFacadeImpl() {
                @Override
                public boolean remove(LPSZ path) {
                    if (Chars.endsWith(path, ".v") || Chars.endsWith(path, ".k")) {
                        return false;
                    }
                    return super.remove(path);
                }
            };

            try {
                checkRebuildIndexes(createTableSql,
                        tablePath -> {
                        },
                        indexBuilder -> indexBuilder.reindexColumn("sym2"));
                Assert.fail();
            } catch (CairoException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "cannot remove index file");
            }
        });
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

        checkRebuildIndexes(
                createTableSql,
                (tablePath) -> {
                },
                IndexBuilder::rebuildAll
        );
    }

    @Test
    public void testNonPartitionedWithColumnTop() throws Exception {
        String createAlterInsertSql = "create table xxx as (" +
                "select " +
                "rnd_symbol('A', 'B', 'C') as sym1," +
                "x," +
                "timestamp_sequence(0, 100000000) ts " +
                "from long_sequence(5000)" +
                "), index(sym1); " +

                "alter table xxx add column sym2 symbol index; " +

                "insert into xxx " +
                "select " +
                "rnd_symbol('A', 'B', 'C') as sym1," +
                "x," +
                "timestamp_sequence(100000000L * 5000L, 100000000) ts, " +
                "rnd_symbol(4,4,4,2) as sym2 " +
                "from long_sequence(5000)";

        checkRebuildIndexes(createAlterInsertSql,
                tablePath -> removeFileAtPartition("sym2.k.1", PartitionBy.NONE, tablePath, 0, -1L),
                indexBuilder -> indexBuilder.reindexColumn("sym2"));
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
                    "alter table xxx add column sym1 symbol index;" +
                    "alter table xxx add column sym2 symbol index";

            checkRebuildIndexes(createAlterInsertSql,
                    tablePath -> {
                    },
                    IndexBuilder::rebuildAll);

            engine.releaseAllWriters();
            compiler
                    .compile("insert into xxx values(500100000000L, 50001, 'D', 'I2')", sqlExecutionContext)
                    .getInsertOperation()
                    .execute(sqlExecutionContext)
                    .await();
            int sym1D = countByFullScan("select * from xxx where sym1 = 'D'");
            Assert.assertEquals(1, sym1D);
            int sym2I2 = countByFullScan("select * from xxx where sym2 = 'I2'");
            Assert.assertEquals(1, sym2I2);
        });
    }

    @Test
    public void testNonePartitionedOneColumn() throws Exception {
        String createTableSql = "create table xxx as (" +
                "select " +
                "rnd_symbol('A', 'B', 'C') as sym1," +
                "rnd_symbol(4,4,4,2) as sym2," +
                "x," +
                "timestamp_sequence(0, 100000000) ts " +
                "from long_sequence(10000)" +
                "), index(sym1), index(sym2)";

        checkRebuildIndexes(createTableSql,
                tablePath -> {
                    removeFileAtPartition("sym1.v", PartitionBy.NONE, tablePath, 0, -1L);
                    removeFileAtPartition("sym1.k", PartitionBy.NONE, tablePath, 0, -1L);
                },
                indexBuilder -> indexBuilder.reindexColumn("sym1"));
    }

    @Test
    public void testPartitionedDaily() throws Exception {
        String createTableSql = "create table xxx as (" +
                "select " +
                "rnd_symbol('A', 'B', 'C') as sym1," +
                "rnd_symbol(4,4,4,2) as sym2," +
                "x," +
                "timestamp_sequence(0, 100000000) ts " +
                "from long_sequence(10000)" +
                "), index(sym1), index(sym2) timestamp(ts) PARTITION BY DAY";

        checkRebuildIndexes(
                createTableSql,
                (tablePath) -> {
                    removeFileAtPartition("sym1.v", PartitionBy.DAY, tablePath, 0, -1L);
                    removeFileAtPartition("sym2.k", PartitionBy.DAY, tablePath, 0, -1L);
                },
                IndexBuilder::rebuildAll
        );
    }

    @Test
    public void testPartitionedNone() throws Exception {
        String createTableSql = "create table xxx as (" +
                "select " +
                "rnd_symbol('A', 'B', 'C') as sym1," +
                "rnd_symbol(4,4,4,2) as sym2," +
                "x," +
                "timestamp_sequence(0, 100000000) ts " +
                "from long_sequence(10000)" +
                "), index(sym1), index(sym2)";

        checkRebuildIndexes(
                createTableSql,
                (tablePath) -> {
                    removeFileAtPartition("sym1.v", PartitionBy.NONE, tablePath, 0, -1L);
                    removeFileAtPartition("sym2.k", PartitionBy.NONE, tablePath, 0, -1L);
                },
                IndexBuilder::rebuildAll
        );
    }

    @Test
    public void testPartitionedNoneSqlSyntax() throws Exception {
        String createTableSql = "create table xxx as (" +
                "select " +
                "rnd_symbol('A', 'B', 'C') as sym1," +
                "rnd_symbol(4,4,4,2) as sym2," +
                "x," +
                "timestamp_sequence(0, 100000000) ts " +
                "from long_sequence(10000)" +
                "), index(sym1), index(sym2)";

        checkRebuildIndexes(
                createTableSql,
                (tablePath) -> {
                    removeFileAtPartition("sym1.v", PartitionBy.NONE, tablePath, 0, -1L);
                    removeFileAtPartition("sym2.k", PartitionBy.NONE, tablePath, 0, -1L);
                },
                indexBuilder -> runReindexSql("REINDEX TABLE xxx LOCK EXCLUSIVE")
        );
    }

    @Test
    public void testPartitionedOneColumn() throws Exception {
        String createTableSql = "create table xxx as (" +
                "select " +
                "rnd_symbol('A', 'B', 'C') as sym1," +
                "rnd_symbol(4,4,4,2) as sym2," +
                "x," +
                "timestamp_sequence(0, 100000000) ts " +
                "from long_sequence(10000)" +
                "), index(sym1), index(sym2) timestamp(ts) PARTITION BY DAY";

        checkRebuildIndexes(createTableSql,
                tablePath -> {
                    removeFileAtPartition("sym1.v", PartitionBy.DAY, tablePath, 0, -1L);
                    removeFileAtPartition("sym1.k", PartitionBy.DAY, tablePath, 0, -1L);
                },
                indexBuilder -> indexBuilder.reindexColumn("sym1"));
    }

    @Test
    public void testPartitionedOneColumnFirstPartition() throws Exception {
        String createTableSql = "create table xxx as (" +
                "select " +
                "rnd_symbol('A', 'B', 'C') as sym1," +
                "rnd_symbol(4,4,4,2) as sym2," +
                "x," +
                "timestamp_sequence(0, 100000000) ts " +
                "from long_sequence(10000)" +
                "), index(sym1), index(sym2) timestamp(ts) PARTITION BY DAY";

        checkRebuildIndexes(createTableSql,
                tablePath -> {
                    removeFileAtPartition("sym1.v", PartitionBy.DAY, tablePath, 0, -1L);
                    removeFileAtPartition("sym1.k", PartitionBy.DAY, tablePath, 0, -1L);
                },
                indexBuilder -> indexBuilder.reindex("1970-01-01", "sym1"));
    }

    @Test
    public void testPartitionedOneColumnFirstPartitionSql() throws Exception {
        String createTableSql = "create table xxx as (" +
                "select " +
                "rnd_symbol('A', 'B', 'C') as sym1," +
                "rnd_symbol(4,4,4,2) as sym2," +
                "x," +
                "timestamp_sequence(0, 100000000) ts " +
                "from long_sequence(10000)" +
                "), index(sym1), index(sym2) timestamp(ts) PARTITION BY DAY";

        checkRebuildIndexes(createTableSql,
                tablePath -> {
                    removeFileAtPartition("sym1.v", PartitionBy.DAY, tablePath, 0, -1L);
                    removeFileAtPartition("sym1.k", PartitionBy.DAY, tablePath, 0, -1L);
                },
                indexBuilder -> runReindexSql("REINDEX TABLE xxx COLUMN sym1 PARTITION '1970-01-01' LOCK EXCLUSIVE"));
    }

    @Test
    public void testPartitionedWithColumnTop() throws Exception {
        String createAlterInsertSql = "create table xxx as (" +
                "select " +
                "rnd_symbol('A', 'B', 'C') as sym1," +
                "x," +
                "timestamp_sequence(0, 100000000) ts " +
                "from long_sequence(5000)" +
                "), index(sym1) timestamp(ts) PARTITION BY DAY; " +

                "alter table xxx add column sym2 symbol index; " +

                "insert into xxx " +
                "select " +
                "rnd_symbol('A', 'B', 'C') as sym1," +
                "x," +
                "timestamp_sequence(100000000L * 5000L, 100000000) ts, " +
                "rnd_symbol(4,4,4,2) as sym2 " +
                "from long_sequence(5000)";

        checkRebuildIndexes(createAlterInsertSql,
                tablePath -> removeFileAtPartition("sym2.k.1", PartitionBy.DAY, tablePath, Timestamps.DAY_MICROS * 11, 1L),
                indexBuilder -> indexBuilder.reindexColumn("sym2"));
    }

    @Test
    public void testRebuildColumnTableWriterLockedFails() throws Exception {
        assertMemoryLeak(() -> {
            String createTableSql = "create table xxx as (" +
                    "select " +
                    "rnd_symbol('A', 'B', 'C') as sym1," +
                    "rnd_symbol(4,4,4,2) as sym2," +
                    "rnd_symbol(4,4,4,2) as sym3," +
                    "x," +
                    "timestamp_sequence(0, 100000000) ts " +
                    "from long_sequence(10000)" +
                    "), index(sym1), index(sym2) timestamp(ts) PARTITION BY DAY";

            tempWriter = null;
            try {
                checkRebuildIndexes(createTableSql,
                        tablePath -> tempWriter = TestUtils.getWriter(engine, "xxx"),
                        indexBuilder -> {
                            try {
                                indexBuilder.reindexColumn("sym2");
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
    public void testRebuildColumnWrongName() throws Exception {
        String createTableSql = "create table xxx as (" +
                "select " +
                "rnd_symbol('A', 'B', 'C') as sym1," +
                "rnd_symbol(4,4,4,2) as sym2," +
                "rnd_symbol(4,4,4,2) as sym3," +
                "x," +
                "timestamp_sequence(0, 100000000) ts " +
                "from long_sequence(10000)" +
                "), index(sym1), index(sym2) timestamp(ts) PARTITION BY DAY";

        try {
            checkRebuildIndexes(createTableSql,
                    tablePath -> {
                    },
                    indexBuilder -> indexBuilder.reindexColumn("sym4"));
            Assert.fail();
        } catch (InvalidColumnException ignore) {
        }
    }

    @Test
    public void testRebuildFailsWriteKFile() throws Exception {
        assertMemoryLeak(() -> {
            String createTableSql = "create table xxx as (" +
                    "select " +
                    "rnd_symbol('A', 'B', 'C') as sym1," +
                    "rnd_symbol(4,4,4,2) as sym2," +
                    "rnd_symbol(4,4,4,2) as sym3," +
                    "x," +
                    "timestamp_sequence(0, 100000000) ts " +
                    "from long_sequence(10000)" +
                    "), index(sym1), index(sym2) timestamp(ts) PARTITION BY DAY";

            AtomicInteger count = new AtomicInteger();
            ff = new TestFilesFacadeImpl() {
                @Override
                public int openRW(LPSZ name, long opts) {
                    if (Chars.contains(name, "sym2.k")) {
                        if (count.incrementAndGet() == 29) {
                            return -1;
                        }
                    }
                    return super.openRW(name, opts);
                }
            };

            try {
                checkRebuildIndexes(createTableSql,
                        tablePath -> {
                        },
                        indexBuilder -> indexBuilder.reindexColumn("sym2"));
                Assert.fail();
            } catch (CairoException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "could not open read-write");
            }
        });
    }

    @Test
    public void testRebuildFailsWriteVFile() throws Exception {
        assertMemoryLeak(() -> {
            String createTableSql = "create table xxx as (" +
                    "select " +
                    "rnd_symbol('A', 'B', 'C') as sym1," +
                    "rnd_symbol(4,4,4,2) as sym2," +
                    "rnd_symbol(4,4,4,2) as sym3," +
                    "x," +
                    "timestamp_sequence(0, 100000000) ts " +
                    "from long_sequence(10000)" +
                    "), index(sym1), index(sym2) timestamp(ts) PARTITION BY DAY";

            AtomicInteger count = new AtomicInteger();
            ff = new TestFilesFacadeImpl() {
                @Override
                public boolean touch(LPSZ path) {
                    if (Chars.contains(path, "sym2.v") && count.incrementAndGet() == 17) {
                        return false;
                    }
                    return Files.touch(path);
                }
            };

            try {
                checkRebuildIndexes(createTableSql,
                        tablePath -> {
                        },
                        indexBuilder -> indexBuilder.reindexColumn("sym2"));
                Assert.fail();
            } catch (CairoException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "could not create index");
            }
        });
    }

    @Test
    public void testRebuildIndexCustomBlockSize() throws Exception {
        String createTableSql = "create table xxx as (" +
                "select " +
                "rnd_symbol('A', 'B', 'C') as sym1," +
                "rnd_symbol(4,4,4,2) as sym2," +
                "x," +
                "timestamp_sequence(0, 100000000) ts " +
                "from long_sequence(10000)" +
                "), index(sym1 capacity 512), index(sym2 capacity 1024) timestamp(ts) PARTITION BY DAY";

        checkRebuildIndexes(createTableSql,
                tablePath -> {
                    removeFileAtPartition("sym1.v", PartitionBy.DAY, tablePath, 0, -1L);
                    removeFileAtPartition("sym2.k", PartitionBy.DAY, tablePath, 0, -1L);
                },
                indexBuilder -> indexBuilder.reindexAllInPartition("1970-01-01"));

        assertMemoryLeak(() -> {
            try (TableReader reader = engine.getReader("xxx")) {
                TableReaderMetadata metadata = reader.getMetadata();
                int columnIndex = metadata.getColumnIndex("sym1");
                Assert.assertTrue("Column sym1 must exist", columnIndex >= 0);
                int columnIndex2 = metadata.getColumnIndex("sym2");
                Assert.assertTrue("Column sym2 must exist", columnIndex2 >= 0);
                Assert.assertEquals(
                        511,
                        reader.getBitmapIndexReader(0, columnIndex, BitmapIndexReader.DIR_FORWARD).getValueBlockCapacity()
                );
                Assert.assertEquals(
                        1023,
                        reader.getBitmapIndexReader(0, columnIndex2, BitmapIndexReader.DIR_FORWARD).getValueBlockCapacity()
                );
            }
        });
    }

    @Test
    public void testRebuildOnePartition() throws Exception {
        String createTableSql = "create table xxx as (" +
                "select " +
                "rnd_symbol('A', 'B', 'C') as sym1," +
                "rnd_symbol(4,4,4,2) as sym2," +
                "x," +
                "timestamp_sequence(0, 100000000) ts " +
                "from long_sequence(10000)" +
                "), index(sym1), index(sym2) timestamp(ts) PARTITION BY DAY";

        checkRebuildIndexes(createTableSql,
                tablePath -> {
                    removeFileAtPartition("sym1.v", PartitionBy.DAY, tablePath, 0, -1L);
                    removeFileAtPartition("sym1.k", PartitionBy.DAY, tablePath, 0, -1L);
                    removeFileAtPartition("sym2.k", PartitionBy.DAY, tablePath, 0, -1L);
                },
                indexBuilder -> indexBuilder.reindexAllInPartition("1970-01-01"));
    }

    @Test
    public void testRebuildTableWithNoIndex() throws Exception {
        String createTableSql = "create table xxx as (" +
                "select " +
                "rnd_symbol('A', 'B', 'C') as sym1," +
                "rnd_symbol(4,4,4,2) as sym2," +
                "rnd_symbol(4,4,4,2) as sym3," +
                "x," +
                "timestamp_sequence(0, 100000000) ts " +
                "from long_sequence(10000)" +
                ")";

        try {
            checkRebuildIndexes(
                    createTableSql,
                    (tablePath) -> {
                    },
                    IndexBuilder::rebuildAll
            );
            Assert.fail();
        } catch (CairoException ex) {
            TestUtils.assertContains(ex.getFlyweightMessage(), "Table does not have any indexes");
        }
    }

    @Test
    public void testRebuildUnindexedColumn() throws Exception {
        String createTableSql = "create table xxx as (" +
                "select " +
                "rnd_symbol('A', 'B', 'C') as sym1," +
                "rnd_symbol(4,4,4,2) as sym2," +
                "rnd_symbol(4,4,4,2) as sym3," +
                "x," +
                "timestamp_sequence(0, 100000000) ts " +
                "from long_sequence(10000)" +
                "), index(sym1), index(sym2) timestamp(ts) PARTITION BY DAY";

        try {
            checkRebuildIndexes(createTableSql,
                    tablePath -> {
                    },
                    indexBuilder -> indexBuilder.reindexColumn("sym3"));
            Assert.fail();
        } catch (CairoException ex) {
            TestUtils.assertContains(ex.getFlyweightMessage(), "Column is not indexed");
        }
    }

    @Test
    public void testReindexPartitionSqlSyntax() throws Exception {
        String createTableSql = "create table xxx as (" +
                "select " +
                "rnd_symbol('A', 'B', 'C') as sym1," +
                "rnd_symbol(4,4,4,2) as sym2," +
                "x," +
                "timestamp_sequence(0, 100000000) ts " +
                "from long_sequence(10000)" +
                "), index(sym1 capacity 512), index(sym2 capacity 1024) timestamp(ts) PARTITION BY DAY";

        checkRebuildIndexes(createTableSql,
                tablePath -> {
                    removeFileAtPartition("sym1.v", PartitionBy.DAY, tablePath, 0, -1L);
                    removeFileAtPartition("sym2.k", PartitionBy.DAY, tablePath, 0, -1L);
                },
                indexBuilder -> runReindexSql("REINDEX TABLE xxx PARTITION '1970-01-01' LOCK EXCLUSIVE")
        );

        assertMemoryLeak(() -> {
            try (TableReader reader = engine.getReader("xxx")) {
                TableReaderMetadata metadata = reader.getMetadata();
                int columnIndex = metadata.getColumnIndex("sym1");
                Assert.assertTrue("Column sym1 must exist", columnIndex >= 0);
                int columnIndex2 = metadata.getColumnIndex("sym2");
                Assert.assertTrue("Column sym2 must exist", columnIndex2 >= 0);
                Assert.assertEquals(
                        511,
                        reader.getBitmapIndexReader(0, columnIndex, BitmapIndexReader.DIR_FORWARD).getValueBlockCapacity()
                );
                Assert.assertEquals(
                        1023,
                        reader.getBitmapIndexReader(0, columnIndex2, BitmapIndexReader.DIR_FORWARD).getValueBlockCapacity()
                );
            }
        });
    }

    private static void runReindexSql(String query) {
        try {
            compiler.compile(query, sqlExecutionContext);
        } catch (SqlException ex) {
            LOG.error().$((Throwable) ex).I$();
            Assert.fail(ex.getMessage());
        }
    }

    private void checkRebuildIndexes(String createTableSql, Action<String> changeTable, Action<IndexBuilder> rebuildIndexAction) throws Exception {
        assertMemoryLeak(ff, () -> {
            for (String sql : createTableSql.split(";")) {
                compiler.compile(sql, sqlExecutionContext).execute(null).await();
            }
            int sym1A = countByFullScan("select * from xxx where sym1 = 'A'");
            int sym1B = countByFullScan("select * from xxx where sym1 = 'B'");
            int sym1C = countByFullScan("select * from xxx where sym1 = 'C'");
            compiler.compile("create table copy as (select * from xxx)", sqlExecutionContext);

            engine.releaseAllReaders();
            engine.releaseAllWriters();

            TableToken xxx = engine.verifyTableName("xxx");
            String tablePath = configuration.getRoot().toString() + Files.SEPARATOR + xxx.getDirName();
            changeTable.run(tablePath);

            indexBuilder.clear();
            indexBuilder.of(tablePath, configuration);
            rebuildIndexAction.run(indexBuilder);

            int sym1A2 = countByFullScan("select * from xxx where sym1 = 'A'");
            int sym1B2 = countByFullScan("select * from xxx where sym1 = 'B'");
            int sym1C2 = countByFullScan("select * from xxx where sym1 = 'C'");

            Assert.assertEquals(sym1A, sym1A2);
            Assert.assertEquals(sym1B, sym1B2);
            Assert.assertEquals(sym1C, sym1C2);

            compiler.compile("insert into xxx select * from copy", sqlExecutionContext);

            int sym1A3 = countByFullScan("select * from xxx where sym1 = 'A'");
            int sym1B3 = countByFullScan("select * from xxx where sym1 = 'B'");
            int sym1C3 = countByFullScan("select * from xxx where sym1 = 'C'");

            Assert.assertEquals(2 * sym1A, sym1A3);
            Assert.assertEquals(2 * sym1B, sym1B3);
            Assert.assertEquals(2 * sym1C, sym1C3);
        });
    }

    private int countByFullScan(String sql) throws SqlException {
        int recordCount = 0;
        try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
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
