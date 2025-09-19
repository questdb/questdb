/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.RebuildColumnBase;
import io.questdb.cairo.RecoverVarIndex;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.std.Files;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

public class RecoverVarIndexTest extends AbstractCairoTest {
    private final RecoverVarIndex rebuildVarColumn = new RecoverVarIndex(configuration);
    TableWriter tempWriter;

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
                RebuildColumnBase::rebuildAll
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

            checkRecoverVarIndex(
                    createAlterInsertSql,
                    tablePath -> {
                    },
                    RebuildColumnBase::rebuildAll
            );

            engine.releaseAllWriters();
            execute("insert into xxx values(500100000000L, 50001, 'D', 'I2')");
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
                    RebuildColumnBase::rebuildAll
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
                    RebuildColumnBase::rebuildAll
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
                    tablePath -> removeFileAtPartition("str2.i.1", PartitionBy.DAY, tablePath, Micros.DAY_MICROS * 11, 1L),
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
                TestUtils.assertContains(ex.getFlyweightMessage(), "cannot lock table");
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
                public long openRW(LPSZ name, int opts) {
                    if (Utf8s.containsAscii(name, "str2.i") && count.incrementAndGet() == 2) {
                        return -1;
                    }
                    return super.openRW(name, opts);
                }
            };

            try {
                checkRecoverVarIndex(createTableSql,
                        tablePath -> {
                        },
                        rebuildIndex -> rebuildIndex.reindexColumn(ff, "str2"));
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
                execute(sql);
            }
            execute("create table copytbl as (select * from xxx)", sqlExecutionContext);

            engine.releaseAllReaders();
            engine.releaseAllWriters();

            TableToken xxx = engine.verifyTableName("xxx");
            String tablePath = configuration.getDbRoot() + Files.SEPARATOR + xxx.getDirName();
            changeTable.run(tablePath);

            rebuildVarColumn.clear();
            rebuildVarColumn.of(new Utf8String(tablePath));
            rebuildIndexAction.run(rebuildVarColumn);

            assertSqlCursors("copytbl", "xxx");
        });
    }

    private int countByFullScanWhereValueD() throws SqlException {
        int recordCount = 0;
        try (RecordCursorFactory factory = select("select * from xxx where str1 = 'D'")) {
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
            TableUtils.setPathForNativePartition(path, ColumnType.TIMESTAMP, partitionBy, partitionTs, partitionNameTxn);
            path.concat(fileName);
            LOG.info().$("removing ").$(path).$();
            Assert.assertTrue(Files.remove(path.$()));
        }
    }

    @FunctionalInterface
    interface Action<T> {
        void run(T val);
    }
}
