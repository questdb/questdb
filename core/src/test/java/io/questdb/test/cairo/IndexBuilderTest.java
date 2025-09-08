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

import io.questdb.cairo.BitmapIndexReader;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.IndexBuilder;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.RebuildColumnBase;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.InvalidColumnException;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
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

public class IndexBuilderTest extends AbstractCairoTest {
    private final IndexBuilder indexBuilder = new IndexBuilder(configuration);
    TableWriter tempWriter;

    @After
    public void cleanup() {
        indexBuilder.close();
    }

    @Test
    public void testCannotRemoveOldFiles() throws Exception {
        FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public boolean removeQuiet(LPSZ path) {
                if (Utf8s.endsWithAscii(path, ".v") || Utf8s.endsWithAscii(path, ".k")) {
                    return false;
                }
                return super.removeQuiet(path);
            }
        };

        assertMemoryLeak(ff, () -> {
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
                checkRebuildIndexes(
                        ff,
                        createTableSql,
                        tablePath -> {
                        },
                        indexBuilder -> indexBuilder.reindexColumn(ff, "sym2")
                );
                Assert.fail();
            } catch (CairoException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "could not remove index file");
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
                ff,
                createTableSql,
                (tablePath) -> {
                },
                RebuildColumnBase::rebuildAll
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

        checkRebuildIndexes(
                ff,
                createAlterInsertSql,
                tablePath -> removeFileAtPartition("sym2.k.1", PartitionBy.NONE, tablePath, 0, -1L),
                indexBuilder -> indexBuilder.reindexColumn("sym2")
        );
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

            checkRebuildIndexes(
                    ff,
                    createAlterInsertSql,
                    tablePath -> {
                    },
                    RebuildColumnBase::rebuildAll
            );

            engine.releaseAllWriters();

            execute("insert into xxx values(500100000000L, 50001, 'D', 'I2')");

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

        checkRebuildIndexes(
                ff,
                createTableSql,
                tablePath -> {
                    removeFileAtPartition("sym1.v", PartitionBy.NONE, tablePath, 0, -1L);
                    removeFileAtPartition("sym1.k", PartitionBy.NONE, tablePath, 0, -1L);
                },
                indexBuilder -> indexBuilder.reindexColumn("sym1")
        );
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
                ff,
                createTableSql,
                (tablePath) -> {
                    removeFileAtPartition("sym1.v", PartitionBy.DAY, tablePath, 0, -1L);
                    removeFileAtPartition("sym2.k", PartitionBy.DAY, tablePath, 0, -1L);
                },
                RebuildColumnBase::rebuildAll
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
                ff,
                createTableSql,
                (tablePath) -> {
                    removeFileAtPartition("sym1.v", PartitionBy.NONE, tablePath, 0, -1L);
                    removeFileAtPartition("sym2.k", PartitionBy.NONE, tablePath, 0, -1L);
                },
                RebuildColumnBase::rebuildAll
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
                ff,
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

        checkRebuildIndexes(
                ff,
                createTableSql,
                tablePath -> {
                    removeFileAtPartition("sym1.v", PartitionBy.DAY, tablePath, 0, -1L);
                    removeFileAtPartition("sym1.k", PartitionBy.DAY, tablePath, 0, -1L);
                },
                indexBuilder -> indexBuilder.reindexColumn("sym1")
        );
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

        checkRebuildIndexes(
                ff,
                createTableSql,
                tablePath -> {
                    removeFileAtPartition("sym1.v", PartitionBy.DAY, tablePath, 0, -1L);
                    removeFileAtPartition("sym1.k", PartitionBy.DAY, tablePath, 0, -1L);
                },
                indexBuilder -> indexBuilder.reindex("1970-01-01", "sym1")
        );
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

        checkRebuildIndexes(
                ff,
                createTableSql,
                tablePath -> {
                    removeFileAtPartition("sym1.v", PartitionBy.DAY, tablePath, 0, -1L);
                    removeFileAtPartition("sym1.k", PartitionBy.DAY, tablePath, 0, -1L);
                },
                indexBuilder -> runReindexSql("REINDEX TABLE xxx COLUMN sym1 PARTITION '1970-01-01' LOCK EXCLUSIVE")
        );
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

        checkRebuildIndexes(
                ff,
                createAlterInsertSql,
                tablePath -> removeFileAtPartition("sym2.k.1", PartitionBy.DAY, tablePath, Micros.DAY_MICROS * 11, 1L),
                indexBuilder -> indexBuilder.reindexColumn("sym2")
        );
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
                checkRebuildIndexes(
                        ff,
                        createTableSql,
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
                TestUtils.assertContains(ex.getFlyweightMessage(), "cannot lock table");
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
            checkRebuildIndexes(
                    ff,
                    createTableSql,
                    tablePath -> {
                    },
                    indexBuilder -> indexBuilder.reindexColumn("sym4")
            );
            Assert.fail();
        } catch (InvalidColumnException ignore) {
        }
    }

    @Test
    public void testRebuildFailsWriteKFile() throws Exception {
        AtomicInteger count = new AtomicInteger();
        FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (Utf8s.containsAscii(name, "sym2.k")) {
                    if (count.incrementAndGet() == 29) {
                        return -1;
                    }
                }
                return super.openRW(name, opts);
            }
        };

        assertMemoryLeak(ff, () -> {
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
                checkRebuildIndexes(
                        ff,
                        createTableSql,
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

        AtomicInteger count = new AtomicInteger();
        FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public boolean touch(LPSZ path) {
                if (Utf8s.containsAscii(path, "sym2.v") && count.incrementAndGet() == 17) {
                    return false;
                }
                return Files.touch(path);
            }
        };

        assertMemoryLeak(ff, () -> {
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
                checkRebuildIndexes(
                        ff,
                        createTableSql,
                        tablePath -> {
                        },
                        indexBuilder -> indexBuilder.reindexColumn(ff, "sym2"));
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

        checkRebuildIndexes(
                ff,
                createTableSql,
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

        checkRebuildIndexes(
                ff,
                createTableSql,
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
                    ff,
                    createTableSql,
                    (tablePath) -> {
                    },
                    RebuildColumnBase::rebuildAll
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
            checkRebuildIndexes(
                    ff,
                    createTableSql,
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

        checkRebuildIndexes(
                ff,
                createTableSql,
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
            execute(query);
        } catch (SqlException ex) {
            LOG.error().$((Throwable) ex).I$();
            Assert.fail(ex.getMessage());
        }
    }

    private void checkRebuildIndexes(FilesFacade ff, String createTableSql, Action<String> changeTable, Action<IndexBuilder> rebuildIndexAction) throws Exception {
        assertMemoryLeak(ff, () -> {
            for (String sql : createTableSql.split(";")) {
                execute(sql);
            }
            int sym1A = countByFullScan("select * from xxx where sym1 = 'A'");
            int sym1B = countByFullScan("select * from xxx where sym1 = 'B'");
            int sym1C = countByFullScan("select * from xxx where sym1 = 'C'");
            execute("create table copy as (select * from xxx)", sqlExecutionContext);

            engine.releaseAllReaders();
            engine.releaseAllWriters();

            TableToken xxx = engine.verifyTableName("xxx");
            String tablePath = configuration.getDbRoot() + Files.SEPARATOR + xxx.getDirName();
            changeTable.run(tablePath);

            indexBuilder.clear();
            indexBuilder.of(new Utf8String(tablePath));
            rebuildIndexAction.run(indexBuilder);

            int sym1A2 = countByFullScan("select * from xxx where sym1 = 'A'");
            int sym1B2 = countByFullScan("select * from xxx where sym1 = 'B'");
            int sym1C2 = countByFullScan("select * from xxx where sym1 = 'C'");

            Assert.assertEquals(sym1A, sym1A2);
            Assert.assertEquals(sym1B, sym1B2);
            Assert.assertEquals(sym1C, sym1C2);

            execute("insert into xxx select * from copy");

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
        try (RecordCursorFactory factory = select(sql)) {
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
