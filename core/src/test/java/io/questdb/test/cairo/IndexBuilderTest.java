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

package io.questdb.test.cairo;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.IndexBuilder;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.RebuildColumnBase;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.idx.IndexReader;
import io.questdb.cairo.idx.PostingIndexUtils;
import io.questdb.cairo.sql.InvalidColumnException;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.Rnd;
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

import static io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE;

public class IndexBuilderTest extends AbstractCairoTest {
    private final IndexBuilder indexBuilder = new IndexBuilder(configuration);
    TableWriter tempWriter;
    private byte indexType;

    @After
    public void cleanup() {
        indexBuilder.close();
    }

    @Override
    public void setUp() {
        Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.CAIRO_DEFAULT_SYMBOL_INDEX_TYPE, TestUtils.randomSymbolIndexTypeName(rnd));
        super.setUp();
        indexType = configuration.getDefaultSymbolIndexType();
    }

    @Test
    public void testCannotRemoveOldFiles() throws Exception {
        FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public boolean removeQuiet(LPSZ path) {
                if (Utf8s.endsWithAscii(path, ".v") || Utf8s.endsWithAscii(path, ".k")
                        || Utf8s.endsWithAscii(path, ".pk") || Utf8s.containsAscii(path, ".pk.")
                        || Utf8s.containsAscii(path, ".pv.")) {
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
                        _ -> {
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
                (_) -> {
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
                tablePath -> removeKeyFileAtPartition("sym2", 1L, PartitionBy.NONE, tablePath, 0, -1L),
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
                    _ -> {
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
                    removeValueFilesAtPartition(PartitionBy.NONE, tablePath);
                    removeKeyFileAtPartition("sym1", COLUMN_NAME_TXN_NONE, PartitionBy.NONE, tablePath, 0, -1L);
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
                    removeValueFilesAtPartition(PartitionBy.DAY, tablePath);
                    removeKeyFileAtPartition("sym2", COLUMN_NAME_TXN_NONE, PartitionBy.DAY, tablePath, 0, -1L);
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
                    removeValueFilesAtPartition(PartitionBy.NONE, tablePath);
                    removeKeyFileAtPartition("sym2", COLUMN_NAME_TXN_NONE, PartitionBy.NONE, tablePath, 0, -1L);
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
                    removeValueFilesAtPartition(PartitionBy.NONE, tablePath);
                    removeKeyFileAtPartition("sym2", COLUMN_NAME_TXN_NONE, PartitionBy.NONE, tablePath, 0, -1L);
                },
                _ -> runReindexSql("REINDEX TABLE xxx LOCK EXCLUSIVE")
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
                    removeValueFilesAtPartition(PartitionBy.DAY, tablePath);
                    removeKeyFileAtPartition("sym1", COLUMN_NAME_TXN_NONE, PartitionBy.DAY, tablePath, 0, -1L);
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
                    removeValueFilesAtPartition(PartitionBy.DAY, tablePath);
                    removeKeyFileAtPartition("sym1", COLUMN_NAME_TXN_NONE, PartitionBy.DAY, tablePath, 0, -1L);
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
                    removeValueFilesAtPartition(PartitionBy.DAY, tablePath);
                    removeKeyFileAtPartition("sym1", COLUMN_NAME_TXN_NONE, PartitionBy.DAY, tablePath, 0, -1L);
                },
                _ -> runReindexSql("REINDEX TABLE xxx COLUMN sym1 PARTITION '1970-01-01' LOCK EXCLUSIVE")
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
                tablePath -> removeKeyFileAtPartition("sym2", 1L, PartitionBy.DAY, tablePath, Micros.DAY_MICROS * 11, 1L),
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
                        _ -> tempWriter = TestUtils.getWriter(engine, "xxx"),
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
                    _ -> {
                    },
                    indexBuilder -> indexBuilder.reindexColumn("sym4")
            );
            Assert.fail();
        } catch (InvalidColumnException ignore) {
        }
    }

    @Test
    public void testRebuildFailsWriteKFile() throws Exception {
        final String keyFragment = isPostingDefault() ? "sym2.pk" : "sym2.k";
        final int triggerCount = isPostingDefault() ? 1 : 29;
        AtomicInteger count = new AtomicInteger();
        FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (Utf8s.containsAscii(name, keyFragment)) {
                    if (count.incrementAndGet() == triggerCount) {
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
                        _ -> {
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
        final String valueFragment = isPostingDefault() ? "sym2.pv" : "sym2.v";
        final int triggerCount = isPostingDefault() ? 1 : 17;
        AtomicInteger count = new AtomicInteger();
        FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public boolean touch(LPSZ path) {
                if (Utf8s.containsAscii(path, valueFragment) && count.incrementAndGet() == triggerCount) {
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
                        _ -> {
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
        indexType = IndexType.BITMAP;
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
                    removeValueFilesAtPartition(PartitionBy.DAY, tablePath);
                    removeKeyFileAtPartition("sym2", COLUMN_NAME_TXN_NONE, PartitionBy.DAY, tablePath, 0, -1L);
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
                        reader.getIndexReader(0, columnIndex, IndexReader.DIR_FORWARD).getValueBlockCapacity()
                );
                Assert.assertEquals(
                        1023,
                        reader.getIndexReader(0, columnIndex2, IndexReader.DIR_FORWARD).getValueBlockCapacity()
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
                    removeValueFilesAtPartition(PartitionBy.DAY, tablePath);
                    removeKeyFileAtPartition("sym1", COLUMN_NAME_TXN_NONE, PartitionBy.DAY, tablePath, 0, -1L);
                    removeKeyFileAtPartition("sym2", COLUMN_NAME_TXN_NONE, PartitionBy.DAY, tablePath, 0, -1L);
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
                    (_) -> {
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
                    _ -> {
                    },
                    indexBuilder -> indexBuilder.reindexColumn("sym3"));
            Assert.fail();
        } catch (CairoException ex) {
            TestUtils.assertContains(ex.getFlyweightMessage(), "Column is not indexed");
        }
    }

    @Test
    public void testReindexPartitionSqlSyntax() throws Exception {
        indexType = IndexType.BITMAP;
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
                    removeValueFilesAtPartition(PartitionBy.DAY, tablePath);
                    removeKeyFileAtPartition("sym2", COLUMN_NAME_TXN_NONE, PartitionBy.DAY, tablePath, 0, -1L);
                },
                _ -> runReindexSql("REINDEX TABLE xxx PARTITION '1970-01-01' LOCK EXCLUSIVE")
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
                        reader.getIndexReader(0, columnIndex, IndexReader.DIR_FORWARD).getValueBlockCapacity()
                );
                Assert.assertEquals(
                        1023,
                        reader.getIndexReader(0, columnIndex2, IndexReader.DIR_FORWARD).getValueBlockCapacity()
                );
            }
        });
    }

    @Test
    public void testReindexRecreatesPostingCoveringSidecars() throws Exception {
        // Regression test for #20: REINDEX must recreate the .pci and
        // .pc<N>.*.* sidecar files for a POSTING+covering index. Without
        // the fix, removeIndexFiles wipes the sidecars and seal recreates
        // only .pk + .pv, leaving readers to silently fall back to the
        // slower column-file read path on every covering query.
        indexType = IndexType.POSTING;
        ff = TestFilesFacadeImpl.INSTANCE;
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_reindex_cov (" +
                    "ts TIMESTAMP, " +
                    "sym SYMBOL INDEX TYPE POSTING INCLUDE (price), " +
                    "price DOUBLE" +
                    ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO t_reindex_cov VALUES " +
                    "('2024-01-01T00:00:00','A',10.0)," +
                    "('2024-01-01T01:00:00','B',20.0)," +
                    "('2024-01-01T02:00:00','A',30.0)");
            engine.releaseAllWriters();

            TableToken tok = engine.verifyTableName("t_reindex_cov");
            try (Path part = new Path()) {
                part.of(configuration.getDbRoot()).concat(tok).concat("2024-01-01");
                String pciPath = io.questdb.std.str.Utf8s.stringFromUtf8Bytes(
                        PostingIndexUtils.coverInfoFileName(part, "sym", COLUMN_NAME_TXN_NONE));

                // Sanity: sidecars exist after the initial seal.
                try (Path probe = new Path()) {
                    Assert.assertTrue("setup: .pci must exist after initial seal",
                            ff.exists(probe.of(pciPath).$()));
                }

                runReindexSql("REINDEX TABLE t_reindex_cov LOCK EXCLUSIVE");

                // After REINDEX: .pci must exist again (regenerated by the
                // seal-with-covering path). Without the fix, it was wiped by
                // removeIndexFiles and never recreated, leaving covering
                // queries to silently degrade to the column-file fallback.
                try (Path probe = new Path()) {
                    Assert.assertTrue(".pci must exist after REINDEX",
                            ff.exists(probe.of(pciPath).$()));
                }
            }

            // The covering query must still return correct rows.
            assertSql("price\n10.0\n30.0\n", "SELECT price FROM t_reindex_cov WHERE sym = 'A' ORDER BY ts");
        });
    }

    @Test
    public void testReindexWithCoveringColumnAddedLater() throws Exception {
        // Red test for the cluster of bugs around column-added-later +
        // POSTING covering index. Two distinct issues both fail this test:
        //
        // 1. IndexBuilder.java:263 propagates the -1 returned by
        //    ColumnVersionReader.getColumnTop (per
        //    ColumnVersionReader.java:148) without clamping. The peer code
        //    at TableSnapshotRestore.java:686 clamps with Math.max(0, ...);
        //    IndexBuilder must do the same.
        //
        // 2. PostingIndexWriter.writeNullSentinel(long addr, int valueSize,
        //    int columnType) at PostingIndexWriter.java:1307 only branches
        //    on GEO/IPv4/FLOAT/DOUBLE — every other type (INT, SHORT, BYTE,
        //    SYMBOL, DECIMAL32, DECIMAL64, TIMESTAMP, DATE, ...) falls
        //    through to the generic zero-fill. For widths < 8 bytes the
        //    Long.MIN_VALUE overlay loop never runs, so an INT NULL is
        //    sealed as four zero bytes, colliding with a valid INT value 0.
        //    CursorPrinter has no way to detect NULL on read.
        //
        // Both bugs converge here: REINDEX walks the column-added-later
        // partition, hits writeNullSentinel for the rows whose rowId is
        // before columnTop, and the sidecar ends up holding zero where the
        // column file would correctly read NULL via columnTop fallback.
        // Covering and no_covering paths must agree.
        indexType = IndexType.POSTING;
        ff = TestFilesFacadeImpl.INSTANCE;
        assertMemoryLeak(() -> {
            // sym + price exist from table creation. qty is added later, so
            // partition 2024-01-01 has rowId < colTop for qty (or -1 if the
            // partition predates the column add entirely).
            execute("CREATE TABLE t_reindex_top (" +
                    "ts TIMESTAMP, " +
                    "sym SYMBOL, " +
                    "price DOUBLE" +
                    ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO t_reindex_top VALUES " +
                    "('2024-01-01T00:00:00','A',10.0)," +
                    "('2024-01-01T01:00:00','B',20.0)");
            execute("ALTER TABLE t_reindex_top ADD COLUMN qty INT");
            execute("INSERT INTO t_reindex_top VALUES " +
                    "('2024-01-02T00:00:00','A',30.0,100)," +
                    "('2024-01-02T01:00:00','B',40.0,200)");
            execute("ALTER TABLE t_reindex_top ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, qty)");
            engine.releaseAllWriters();

            // Force the IndexBuilder path to walk both partitions, including
            // the one where qty did not exist at row time.
            runReindexSql("REINDEX TABLE t_reindex_top LOCK EXCLUSIVE");
            engine.releaseAllReaders();

            // Both paths must agree: the column-added-later rows render as
            // NULL.
            String expected = """
                    qty
                    null
                    100
                    """;
            assertSql(expected, "SELECT /*+ no_covering */ qty FROM t_reindex_top WHERE sym = 'A' ORDER BY ts");
            assertSql(expected, "SELECT qty FROM t_reindex_top WHERE sym = 'A' ORDER BY ts");
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

    private boolean isPostingDefault() {
        return indexType != IndexType.BITMAP;
    }

    private void removeFileAtPartition(String fileName, int partitionBy, String tablePath, long partitionTs, long partitionNameTxn) {
        try (Path path = new Path()) {
            path.concat(tablePath);
            path.put(Files.SEPARATOR);
            TableUtils.setPathForNativePartition(path, ColumnType.TIMESTAMP, partitionBy, partitionTs, partitionNameTxn);
            path.concat(fileName);
            LOG.info().$("removing ").$(path).$();
            Assert.assertTrue(TestUtils.remove(path.$()));
        }
    }

    private void removeKeyFileAtPartition(String colName, long columnNameTxn, int partitionBy, String tablePath, long partitionTs, long partitionNameTxn) {
        String ext = isPostingDefault() ? ".pk" : ".k";
        String name = columnNameTxn == COLUMN_NAME_TXN_NONE
                ? colName + ext
                : colName + ext + "." + columnNameTxn;
        removeFileAtPartition(name, partitionBy, tablePath, partitionTs, partitionNameTxn);
    }

    private void removeValueFilesAtPartition(int partitionBy, String tablePath) {
        if (!isPostingDefault()) {
            removeFileAtPartition("sym1.v", partitionBy, tablePath, 0, -1L);
            return;
        }
        try (Path p = new Path()) {
            p.concat(tablePath).put(Files.SEPARATOR);
            TableUtils.setPathForNativePartition(p, ColumnType.TIMESTAMP, partitionBy, 0, -1L);
            int plen = p.size();
            FilesFacade facade = configuration.getFilesFacade();
            AtomicInteger removed = new AtomicInteger();
            PostingIndexUtils.scanSealedFiles(facade, p, plen, "sym1", new PostingIndexUtils.SealedFileVisitor() {
                @Override
                public void onCoverDataFile(int includeIdx, long postingColumnNameTxn, long coveredColumnNameTxn, long sealTxn) {
                }

                @Override
                public void onValueFile(long postingColumnNameTxn, long sealTxn) {
                    if (postingColumnNameTxn != TableUtils.COLUMN_NAME_TXN_NONE) {
                        return;
                    }
                    LPSZ pv = PostingIndexUtils.valueFileName(p.trimTo(plen), "sym1", postingColumnNameTxn, sealTxn);
                    LOG.info().$("removing ").$(pv).$();
                    Assert.assertTrue(facade.removeQuiet(pv));
                    p.trimTo(plen);
                    removed.incrementAndGet();
                }
            });
            Assert.assertTrue("expected at least one posting value file for " + "sym1", removed.get() > 0);
        }
    }

    @FunctionalInterface
    interface Action<T> {
        void run(T val);
    }
}
