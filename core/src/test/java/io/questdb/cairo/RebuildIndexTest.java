/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.cairo;

import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.*;

import java.util.concurrent.atomic.AtomicInteger;

public class RebuildIndexTest extends AbstractCairoTest {
    protected static CharSequence root;
    private static SqlCompiler compiler;
    private static SqlExecutionContextImpl sqlExecutionContext;
    private final RebuildIndex rebuildIndex = new RebuildIndex();
    TableWriter tempWriter;

    @BeforeClass
    public static void setUpStatic() {
        AbstractCairoTest.setUpStatic();
        compiler = new SqlCompiler(engine);
        BindVariableServiceImpl bindVariableService = new BindVariableServiceImpl(configuration);
        sqlExecutionContext = new SqlExecutionContextImpl(engine, 1)
                .with(
                        AllowAllCairoSecurityContext.INSTANCE,
                        bindVariableService,
                        null,
                        -1,
                        null);
        bindVariableService.clear();
    }

    @AfterClass
    public static void tearDownStatic() {
        compiler.close();
    }

    @After
    public void cleanup() {
        rebuildIndex.close();
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
                RebuildIndex::rebuildAll
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
                tablePath -> removeFileAtPartition("sym2.k.1", PartitionBy.NONE, tablePath, 0),
                rebuildIndex -> rebuildIndex.rebuildColumn("sym2"));
    }

    @Test
    public void testNonPartitionedWithColumnTopAddedLast() throws Exception {
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
                RebuildIndex::rebuildAll);

        engine.releaseAllWriters();
        compiler.compile("insert into xxx values(500100000000L, 50001, 'D', 'I2')", sqlExecutionContext).execute(null).await();
        int sym1D = countByFullScan("select * from xxx where sym1 = 'D'");
        Assert.assertEquals(1, sym1D);
        int sym2I2 = countByFullScan("select * from xxx where sym2 = 'I2'");
        Assert.assertEquals(1, sym2I2);
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
                    removeFileAtPartition("sym1.v", PartitionBy.NONE, tablePath, 0);
                    removeFileAtPartition("sym1.k", PartitionBy.NONE, tablePath, 0);
                },
                rebuildIndex -> rebuildIndex.rebuildColumn("sym1"));
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
                    removeFileAtPartition("sym1.v", PartitionBy.DAY, tablePath, 0);
                    removeFileAtPartition("sym2.k", PartitionBy.DAY, tablePath, 0);
                },
                RebuildIndex::rebuildAll
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
                    removeFileAtPartition("sym1.v", PartitionBy.NONE, tablePath, 0);
                    removeFileAtPartition("sym2.k", PartitionBy.NONE, tablePath, 0);
                },
                RebuildIndex::rebuildAll
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
                    removeFileAtPartition("sym1.v", PartitionBy.DAY, tablePath, 0);
                    removeFileAtPartition("sym1.k", PartitionBy.DAY, tablePath, 0);
                },
                rebuildIndex -> rebuildIndex.rebuildColumn("sym1"));
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
                    removeFileAtPartition("sym1.v", PartitionBy.DAY, tablePath, 0);
                    removeFileAtPartition("sym1.k", PartitionBy.DAY, tablePath, 0);
                },
                rebuildIndex -> rebuildIndex.rebuildPartitionColumn("1970-01-01", "sym1"));
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
                tablePath -> removeFileAtPartition("sym2.k.1", PartitionBy.DAY, tablePath, Timestamps.DAY_MICROS * 11),
                rebuildIndex -> rebuildIndex.rebuildColumn("sym2"));
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
                        tablePath -> tempWriter = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "xxx", "test lock"),
                        rebuildIndex -> {
                            try {
                                rebuildIndex.rebuildColumn("sym2");
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
                    rebuildIndex -> rebuildIndex.rebuildColumn("sym4"));
            Assert.fail();
        } catch (CairoException ex) {
            TestUtils.assertContains(ex.getFlyweightMessage(), "Column does not exist");
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
            ff = new FilesFacadeImpl() {
                @Override
                public long openRW(LPSZ name, long opts) {
                    if (Chars.contains(name, "sym2.k") && count.incrementAndGet() == 29) {
                        return -1;
                    }
                    return Files.openRW(name, opts);
                }
            };

            try {
                checkRebuildIndexes(createTableSql,
                        tablePath -> {
                        },
                        rebuildIndex -> rebuildIndex.rebuildColumn("sym2"));
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
            ff = new FilesFacadeImpl() {
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
                        rebuildIndex -> rebuildIndex.rebuildColumn("sym2"));
                Assert.fail();
            } catch (CairoException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "could not create index");
            }
        });
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

            ff = new FilesFacadeImpl() {
                @Override
                public boolean remove(LPSZ path) {
                    if (Chars.endsWith(path, ".v") || Chars.endsWith(path, ".k")) {
                        return false;
                    }
                    return Files.remove(path);
                }
            };

            try {
                checkRebuildIndexes(createTableSql,
                        tablePath -> {
                        },
                        rebuildIndex -> rebuildIndex.rebuildColumn("sym2"));
                Assert.fail();
            } catch (CairoException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "cannot remove index file");
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
                    removeFileAtPartition("sym1.v", PartitionBy.DAY, tablePath, 0);
                    removeFileAtPartition("sym2.k", PartitionBy.DAY, tablePath, 0);
                },
                rebuildIndex -> rebuildIndex.rebuildPartition("1970-01-01"));

        assertMemoryLeak(() -> {
            try (TableReader reader = engine.getReader(sqlExecutionContext.getCairoSecurityContext(), "xxx")) {
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
                    removeFileAtPartition("sym1.v", PartitionBy.DAY, tablePath, 0);
                    removeFileAtPartition("sym1.k", PartitionBy.DAY, tablePath, 0);
                    removeFileAtPartition("sym2.k", PartitionBy.DAY, tablePath, 0);
                },
                rebuildIndex -> rebuildIndex.rebuildPartition("1970-01-01"));
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
                    rebuildIndex -> rebuildIndex.rebuildColumn("sym3"));
            Assert.fail();
        } catch (CairoException ex) {
            TestUtils.assertContains(ex.getFlyweightMessage(), "Column is not indexed");
        }
    }

    private void checkRebuildIndexes(String createTableSql, Action<String> changeTable, Action<RebuildIndex> rebuildIndexAction) throws Exception {
        assertMemoryLeak(ff, () -> {
            for (String sql : createTableSql.split(";")) {
                compiler.compile(sql, sqlExecutionContext).execute(null).await();
            }
            int sym1A = countByFullScan("select * from xxx where sym1 = 'A'");
            int sym1B = countByFullScan("select * from xxx where sym1 = 'B'");
            int sym1C = countByFullScan("select * from xxx where sym1 = 'C'");
            compiler.compile("create table copy as (select * from xxx)", sqlExecutionContext).execute(null).await();

            engine.releaseAllReaders();
            engine.releaseAllWriters();

            String tablePath = configuration.getRoot().toString() + Files.SEPARATOR + "xxx";
            changeTable.run(tablePath);

            rebuildIndex.clear();
            rebuildIndex.of(tablePath, configuration);
            rebuildIndexAction.run(rebuildIndex);

            int sym1A2 = countByFullScan("select * from xxx where sym1 = 'A'");
            int sym1B2 = countByFullScan("select * from xxx where sym1 = 'B'");
            int sym1C2 = countByFullScan("select * from xxx where sym1 = 'C'");

            Assert.assertEquals(sym1A, sym1A2);
            Assert.assertEquals(sym1B, sym1B2);
            Assert.assertEquals(sym1C, sym1C2);

            compiler.compile("insert into xxx select * from copy", sqlExecutionContext).execute(null).await();

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

    private void removeFileAtPartition(String fileName, int partitionBy, String tablePath, long partitionTs) {
        try (Path path = new Path()) {
            path.concat(tablePath);
            path.put(Files.SEPARATOR);
            PartitionBy.setSinkForPartition(path, partitionBy, partitionTs, false);
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
