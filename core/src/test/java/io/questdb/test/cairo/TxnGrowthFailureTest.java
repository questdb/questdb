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
import io.questdb.cairo.ColumnVersionReader;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.TxWriter;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

/**
 * A non-WAL commit has to grow _txn and cannot: the disk is full, or the remap of the grown file fails. The
 * statement fails, and the table stays consistent and usable through the same writer: no JVM crash, no torn
 * _txn, the failed rows never become visible, and the next write lands. A failed TRUNCATE is the exception: it
 * leaves its writer distressed, and the pool opens another one.
 */
@RunWith(Parameterized.class)
public class TxnGrowthFailureTest extends AbstractCairoTest {
    private static final String EXPECTED_AFTER_NEXT_INSERT = """
            s\tv\tts
            a\t1\t2024-01-01T00:00:00.000000Z
            a\t2\t2024-01-02T00:00:00.000000Z
            y\t9\t2025-06-01T00:00:00.000000Z
            """;
    private static final String NEXT_INSERT = "INSERT INTO t VALUES ('y', 9, '2025-06-01T00:00:00.000000Z')";
    private final boolean isRemapFailure;

    public TxnGrowthFailureTest(boolean isRemapFailure) {
        this.isRemapFailure = isRemapFailure;
    }

    @Parameterized.Parameters(name = "remap={0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{{true}, {false}});
    }

    @Before
    @Override
    public void setUp() {
        super.setUp();
        // hundreds of partitions: keep their column files small
        setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, 4096);
    }

    // A compaction swap commits one partition-table range around an open transaction. A swap that grows the record
    // and fails leaves the writer, its mapping and the open transaction as they were, and it can commit again.
    @Test
    public void testCommitPartitionRangeGrowingTxn() throws Exception {
        final TxnExtendFailingFacade ff = txnFailingFacade();
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE t (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            final TableToken token = engine.verifyTableName("t");
            try (
                    Path path = new Path().of(configuration.getDbRoot()).concat(token).concat(TableUtils.TXN_FILE_NAME);
                    TxWriter txWriter = new TxWriter(ff, configuration).ofRW(path.$(), ColumnType.TIMESTAMP, PartitionBy.DAY)
            ) {
                // every other day: the partition table of 200 entries outgrows the 4 KB the mapping grows by
                for (int i = 0; i < 200; i++) {
                    txWriter.updatePartitionSizeByTimestamp(2L * i * Micros.DAY_MICROS, i + 1);
                }
                txWriter.updateMaxTimestamp(398 * Micros.DAY_MICROS + 1);
                txWriter.finishPartitionSizeUpdate();
                txWriter.commit(new ObjList<>());
                final long txn = txWriter.getTxn();

                // swap the empty committed range [1, 1) for one new partition, day 1
                txWriter.insertPartition(1, Micros.DAY_MICROS, 7, txn - 1);
                ff.isArmed.set(true);
                try {
                    txWriter.commitPartitionRange(CommitMode.NOSYNC, 1, 1, 1);
                    Assert.fail("the swap did not fail");
                } catch (CairoException e) {
                    assertInjectedFailure(ff, e);
                }
                Assert.assertNotEquals(-1, txWriter.getFd());
                Assert.assertEquals(txn, txWriter.getTxn());
                Assert.assertEquals(200, txWriter.getCommittedPartitionCount());
                Assert.assertEquals(201, txWriter.getPartitionCount());
                Assert.assertEquals(txn, txnOnDisk());

                txWriter.commitPartitionRange(CommitMode.NOSYNC, 1, 1, 1);
                Assert.assertEquals(txn + 1, txWriter.getTxn());
                Assert.assertEquals(201, txWriter.getCommittedPartitionCount());
            }
            try (
                    Path path = new Path().of(configuration.getDbRoot()).concat(token).concat(TableUtils.TXN_FILE_NAME);
                    TxReader txReader = new TxReader(configuration.getFilesFacade())
            ) {
                txReader.ofRO(path.$(), ColumnType.TIMESTAMP, PartitionBy.DAY);
                Assert.assertTrue(txReader.unsafeLoadAll());
                Assert.assertEquals(201, txReader.getPartitionCount());
                Assert.assertEquals(Micros.DAY_MICROS, txReader.getPartitionTimestampByIndex(1));
                Assert.assertEquals(7, txReader.getPartitionSize(1));
                Assert.assertEquals(2 * Micros.DAY_MICROS, txReader.getPartitionTimestampByIndex(2));
            }
        });
    }

    // The last partition holds fewer rows than the one before it, which keeps the open transaction visible to
    // inTransaction() even on master, where the pool's rollback then read the unmapped _txn.
    @Test
    public void testFailedCommitWithShorterLastPartition() throws Exception {
        final TxnExtendFailingFacade ff = txnFailingFacade();
        assertMemoryLeak(ff, () -> {
            final TableWriter writer = failInsertGrowingTxn(ff, 2, 399);
            execute(NEXT_INSERT);
            // assertQuery() releases the pooled writers: check the writer first
            Assert.assertSame(writer, pooledWriter());
            assertNextInsertResult();
        });
    }

    // The F2FixTest E sequence: the next INSERT through the pooled writer crashed the JVM (remap) or published
    // a _txn record that readers rejected as torn, which lost the acknowledged row (disk full).
    @Test
    public void testNextInsertThroughPooledWriter() throws Exception {
        final TxnExtendFailingFacade ff = txnFailingFacade();
        assertMemoryLeak(ff, () -> {
            final TableWriter writer = failInsertGrowingTxn(ff, 1, 400);
            execute(NEXT_INSERT);
            Assert.assertSame(writer, pooledWriter());
            Assert.assertEquals(2, txnOnDisk());
            assertNextInsertResult();
        });
    }

    // An out-of-order row into an existing partition, then hundreds of new partitions: the O3 commit grows _txn.
    @Test
    public void testO3CommitGrowingTxn() throws Exception {
        final TxnExtendFailingFacade ff = txnFailingFacade();
        assertMemoryLeak(ff, () -> {
            final TableWriter writer = createTable();
            ff.isArmed.set(true);
            try {
                execute("""
                        INSERT INTO t (s, v, ts) SELECT 'x', x::INT,
                        CASE WHEN x = 1 THEN '2024-01-01T12:00:00.000000Z'::TIMESTAMP
                        ELSE dateadd('d', x::INT, '2024-03-01T00:00:00.000000Z'::TIMESTAMP) END
                        FROM long_sequence(400)
                        """);
                Assert.fail("the INSERT did not fail");
            } catch (CairoException e) {
                assertInjectedFailure(ff, e);
            }
            Assert.assertEquals(1, txnOnDisk());
            execute(NEXT_INSERT);
            Assert.assertSame(writer, pooledWriter());
            assertNextInsertResult();
        });
    }

    // The writer the failed commit returns to the pool rolls back to the last committed record.
    @Test
    public void testPooledWriterRollsBackToCommittedRecord() throws Exception {
        final TxnExtendFailingFacade ff = txnFailingFacade();
        assertMemoryLeak(ff, () -> {
            final TableWriter writer = failInsertGrowingTxn(ff, 1, 400);
            try (TableWriter w = getWriter("t")) {
                Assert.assertSame(writer, w);
                Assert.assertFalse(w.isDistressed());
                Assert.assertFalse(w.inTransaction());
                Assert.assertNotEquals(-1, w.getTxWriter().getFd());
                Assert.assertEquals(1, w.getTxn());
                Assert.assertEquals(2, w.getRowCount());
                Assert.assertEquals(2, w.getPartitionCount());
            }
        });
    }

    // The writer whose commit failed commits again: the rows go in once, under the next txn.
    @Test
    public void testRetryCommitThroughSameWriter() throws Exception {
        testRetryCommitThroughSameWriter(false);
    }

    @Test
    public void testRetryO3CommitThroughSameWriter() throws Exception {
        testRetryCommitThroughSameWriter(true);
    }

    // The writer that survived the failed commit truncates the table, commits a full record for the empty table and
    // takes rows again.
    @Test
    public void testTruncateAfterFailedCommit() throws Exception {
        final TxnExtendFailingFacade ff = txnFailingFacade();
        assertMemoryLeak(ff, () -> {
            final TableWriter writer = failInsertGrowingTxn(ff, 1, 400);
            execute("TRUNCATE TABLE t");
            execute("ALTER TABLE t ADD COLUMN z INT");
            Assert.assertEquals(0, writer.getRowCount());
            Assert.assertEquals(0, writer.getPartitionCount());
            execute("INSERT INTO t VALUES ('y', 9, '2025-06-01T00:00:00.000000Z', 7)");
            Assert.assertSame(writer, pooledWriter());
            assertQuery("SELECT s, v, ts, z FROM t")
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            s\tv\tts\tz
                            y\t9\t2025-06-01T00:00:00.000000Z\t7
                            """);
        });
    }

    // The truncated record lands past the part of _txn a new writer maps, and _txn cannot grow. TRUNCATE maps that
    // record before it removes anything: the table keeps its rows, its symbol table and its column versions.
    @Test
    public void testTruncateGrowingTxn() throws Exception {
        testTruncateGrowingTxn("TRUNCATE TABLE t");
    }

    @Test
    public void testTruncateKeepSymbolMapsGrowingTxn() throws Exception {
        testTruncateGrowingTxn("TRUNCATE TABLE t KEEP SYMBOL MAPS");
    }

    // The truncated record of a non-partitioned table keeps its default partition: TxWriter.truncate() maps the area
    // before it drops the partitions, and asserts the size it mapped.
    @Test
    public void testTruncateNonPartitionedTable() throws Exception {
        final TxnExtendFailingFacade ff = txnFailingFacade();
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE t (s SYMBOL, v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY NONE BYPASS WAL");
            execute("INSERT INTO t VALUES ('a', 1, '2024-01-01T00:00:00.000000Z'), ('a', 2, '2024-01-02T00:00:00.000000Z')");
            execute("TRUNCATE TABLE t");
            execute("ALTER TABLE t ADD COLUMN z INT");
            execute("INSERT INTO t VALUES ('y', 9, '2025-06-01T00:00:00.000000Z', 7)");
            assertQuery("SELECT s, v, ts, z FROM t")
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            s\tv\tts\tz
                            y\t9\t2025-06-01T00:00:00.000000Z\t7
                            """);
        });
    }

    private void assertNextInsertResult() throws Exception {
        assertQuery("SELECT s, v, ts FROM t")
                .timestamp("ts")
                .expectSize()
                .returns(EXPECTED_AFTER_NEXT_INSERT);
    }

    // _cv holds the version _txn names, not one ahead of it that readers would wait for
    private static void assertColumnVersionMatchesTxn() {
        final TableToken token = engine.verifyTableName("t");
        try (
                Path path = new Path().of(configuration.getDbRoot()).concat(token);
                TxReader txReader = new TxReader(configuration.getFilesFacade());
                ColumnVersionReader columnVersionReader = new ColumnVersionReader()
        ) {
            final int tableDirLen = path.size();
            txReader.ofRO(path.concat(TableUtils.TXN_FILE_NAME).$(), ColumnType.TIMESTAMP, PartitionBy.DAY);
            Assert.assertTrue(txReader.unsafeLoadAll());
            columnVersionReader.ofRO(configuration.getFilesFacade(), path.trimTo(tableDirLen).concat(TableUtils.COLUMN_VERSION_FILE_NAME).$());
            Assert.assertEquals(txReader.getColumnVersion(), columnVersionReader.readUnsafe());
        }
    }

    private static TableWriter createTable() throws Exception {
        execute("CREATE TABLE t (s SYMBOL, v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
        execute("INSERT INTO t VALUES ('a', 1, '2024-01-01T00:00:00.000000Z'), ('a', 2, '2024-01-02T00:00:00.000000Z')");
        return pooledWriter();
    }

    // The writer the pool hands out. A failed commit must not cost it: a rollback, not a reopen, recovers it.
    private static TableWriter pooledWriter() {
        try (TableWriter w = getWriter("t")) {
            return w;
        }
    }

    private static long txnOnDisk() {
        final TableToken token = engine.verifyTableName("t");
        try (
                Path path = new Path().of(configuration.getDbRoot()).concat(token).concat(TableUtils.TXN_FILE_NAME);
                TxReader txReader = new TxReader(configuration.getFilesFacade())
        ) {
            txReader.ofRO(path.$(), ColumnType.TIMESTAMP, PartitionBy.DAY);
            Assert.assertTrue(txReader.unsafeLoadAll());
            return txReader.getTxn();
        }
    }

    private void assertInjectedFailure(TxnExtendFailingFacade ff, CairoException e) {
        Assert.assertFalse("the armed failure did not fire", ff.isArmed.get());
        TestUtils.assertContains(e.getFlyweightMessage(), isRemapFailure ? "could not remap file" : "No space left");
    }

    // rowCount rows, rowsPerPartition to a new daily partition: the partition table outgrows the first 4 KB of _txn.
    private TableWriter failInsertGrowingTxn(TxnExtendFailingFacade ff, int rowsPerPartition, int rowCount) throws Exception {
        final TableWriter writer = createTable();
        ff.isArmed.set(true);
        try {
            execute("INSERT INTO t (s, v, ts) SELECT 'x', x::INT, dateadd('d', ((x - 1) / " + rowsPerPartition
                    + ")::INT, '2024-03-02T00:00:00.000000Z'::TIMESTAMP) FROM long_sequence(" + rowCount + ")");
            Assert.fail("the INSERT did not fail");
        } catch (CairoException e) {
            assertInjectedFailure(ff, e);
        }
        Assert.assertEquals(1, txnOnDisk());
        return writer;
    }

    private void testRetryCommitThroughSameWriter(boolean isO3) throws Exception {
        final TxnExtendFailingFacade ff = txnFailingFacade();
        assertMemoryLeak(ff, () -> {
            createTable();
            try (TableWriter w = getWriter("t")) {
                final long ts0 = MicrosFormatUtils.parseTimestamp("2024-03-02T00:00:00.000000Z");
                if (isO3) {
                    final TableWriter.Row row = w.newRow(MicrosFormatUtils.parseTimestamp("2024-01-01T12:00:00.000000Z"));
                    row.putSym(0, "o");
                    row.putInt(1, 0);
                    row.append();
                }
                for (int i = 0; i < 400; i++) {
                    final TableWriter.Row row = w.newRow(ts0 + i * Micros.DAY_MICROS);
                    row.putSym(0, "x");
                    row.putInt(1, i + 1);
                    row.append();
                }
                ff.isArmed.set(true);
                try {
                    w.commit();
                    Assert.fail("the commit did not fail");
                } catch (CairoException e) {
                    assertInjectedFailure(ff, e);
                }
                Assert.assertTrue(w.inTransaction());
                Assert.assertEquals(1, w.getTxn());
                Assert.assertEquals(1, txnOnDisk());
                w.commit();
                Assert.assertEquals(2, w.getTxn());
            }
            Assert.assertEquals(2, txnOnDisk());
            assertQuery("SELECT s, count(), count_distinct(ts), min(v), max(v) FROM t ORDER BY s")
                    .expectSize()
                    .returns(isO3 ? """
                            s\tcount\tcount_distinct\tmin\tmax
                            a\t2\t2\t1\t2
                            o\t1\t1\t0\t0
                            x\t400\t400\t1\t400
                            """ : """
                            s\tcount\tcount_distinct\tmin\tmax
                            a\t2\t2\t1\t2
                            x\t400\t400\t1\t400
                            """);
        });
    }

    private void testTruncateGrowingTxn(String truncateSql) throws Exception {
        final String rowsQuery = "SELECT s, count() n, min(v) lo, max(v) hi, min(ts) ts_lo, max(ts) ts_hi, count(z) nz FROM t ORDER BY s";
        final String insertSql = "INSERT INTO t VALUES ('y', 9, '2025-06-01T00:00:00.000000Z', 7)";
        final String insertedRow = "y\t1\t9\t9\t2025-06-01T00:00:00.000000Z\t2025-06-01T00:00:00.000000Z\t1\n";
        final String rowsBeforeTruncate = """
                s\tn\tlo\thi\tts_lo\tts_hi\tnz
                a\t120\t1\t120\t2024-01-01T00:00:00.000000Z\t2024-04-29T00:00:00.000000Z\t0
                b\t1\t1000\t1000\t2024-04-29T01:00:00.000000Z\t2024-04-29T01:00:00.000000Z\t1
                """;
        final TxnExtendFailingFacade ff = txnFailingFacade();
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE t (s SYMBOL, v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            // 120 daily partitions: a record of almost 4 KB
            execute("INSERT INTO t (s, v, ts) SELECT 'a', x::INT, dateadd('d', (x - 1)::INT, '2024-01-01T00:00:00.000000Z'::TIMESTAMP) FROM long_sequence(120)");
            // a column added after the data gives _cv records, which TRUNCATE commits
            execute("ALTER TABLE t ADD COLUMN z INT");
            // this commit writes its record at the start of _txn, and the truncated record goes right after it,
            // across the first 4 KB
            execute("INSERT INTO t VALUES ('b', 1000, '2024-04-29T01:00:00.000000Z', 0)");
            // a new writer maps the first 4 KB of _txn only
            engine.releaseAllWriters();
            final TableWriter writer = pooledWriter();

            ff.isArmed.set(true);
            try {
                execute(truncateSql);
                Assert.fail("the TRUNCATE did not fail");
            } catch (CairoException e) {
                assertInjectedFailure(ff, e);
            }
            // the pool closed the writer that the failed TRUNCATE left distressed
            Assert.assertFalse(writer.isOpen());
            Assert.assertEquals(3, txnOnDisk());
            assertColumnVersionMatchesTxn();
            assertQuery(rowsQuery)
                    .expectSize()
                    .returns(rowsBeforeTruncate);

            execute(insertSql);
            Assert.assertNotSame(writer, pooledWriter());
            assertQuery(rowsQuery)
                    .expectSize()
                    .returns(rowsBeforeTruncate + insertedRow);

            execute(truncateSql);
            execute(insertSql);
            assertQuery(rowsQuery)
                    .expectSize()
                    .returns("s\tn\tlo\thi\tts_lo\tts_hi\tnz\n" + insertedRow);
        });
    }

    private TxnExtendFailingFacade txnFailingFacade() {
        return new TxnExtendFailingFacade("t", isRemapFailure);
    }
}
