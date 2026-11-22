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

package io.questdb.test.cairo.crash;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnPurgeJob;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.cairo.crash.LegacyTruncateTodo.SymbolMapState;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A non-WAL TRUNCATE of an earlier build stopped after it armed its {@code _todo_} entry, and a query opens the table
 * before the first writer open completes the TRUNCATE. TRUNCATE itself runs only while no query reads the table, but the
 * repair runs in whichever writer open comes first: the query must keep reading the rows it opened, through the
 * repair, the rows written after it and the writer close, and their files must go only once the query is done with
 * them. TRUNCATE no longer arms the entry, see {@link TruncateRepairTest}, so the tests arm it as that build left it.
 */
public class TruncateRepairOpenQueryTest extends AbstractCrashConsistencyTest {
    private static final String COMMITTED_ROWS = """
            s\tsi\tsp\tv\tstr\tx\tarr\ty
            a\tk1\tp1\tv1\ts1\t101\t[1.0]\tnull
            b\tk2\tp2\tv2\ts2\t102\t[2.0]\tnull
            c\tk3\tp3\tv3\ts3\t103\t[3.0]\t30
            """;
    private static final String DDL = """
            CREATE TABLE %s (
                s SYMBOL,
                si SYMBOL INDEX,
                sp SYMBOL INDEX TYPE POSTING INCLUDE (x),
                v VARCHAR,
                str STRING,
                x LONG,
                arr DOUBLE[]
            )""";
    private static final String INSERT_AFTER_REPAIR = """
            INSERT INTO %s VALUES
                ('a', 'k1', 'p1', 'v5', 's5', 5, ARRAY[5.0], 50),
                ('z', 'k9', 'p9', 'v6', 's6', 6, ARRAY[6.0], 60)""";
    private static final String ROWS_AFTER_REPAIR = """
            s\tsi\tsp\tv\tstr\tx\tarr\ty
            a\tk1\tp1\tv5\ts5\t5\t[5.0]\t50
            z\tk9\tp9\tv6\ts6\t6\t[6.0]\t60
            """;
    private static final int ROWS_OF_LARGE_TABLE = 10_000;
    private final StringSink printSink = new StringSink();

    @Before
    public void setUpPurge() {
        // the column purge job retries on the test clock
        setCurrentMicros(0);
        setProperty(PropertyKey.CAIRO_SQL_COLUMN_PURGE_RETRY_DELAY, 1);
        // one page per append segment: the old rows span more than one page of their files
        setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, Files.PAGE_SIZE);
    }

    @Test
    public void testAllColumnTypesKeepRowsOfOpenQuery() throws Exception {
        // every column type moves to a new file version, the var-size ones with their aux files; a dropped column
        // has no files to move
        runWithCrashFacade(() -> {
            final String table = "t_types";
            execute("""
                    CREATE TABLE t_types (
                        gone VARCHAR,
                        bo BOOLEAN, by BYTE, sh SHORT, ch CHAR, i INT, l LONG, f FLOAT, d DOUBLE,
                        dt DATE, ts TIMESTAMP, tn TIMESTAMP_NS, u UUID, l256 LONG256,
                        g1 GEOHASH(1c), g2 GEOHASH(2c), g4 GEOHASH(4c), g8 GEOHASH(8c), ip IPV4,
                        dc8 DECIMAL(2,1), dc16 DECIMAL(4,1), dc32 DECIMAL(9,2), dc64 DECIMAL(18,3), dc128 DECIMAL(38,4),
                        dc256 DECIMAL(76,5), s SYMBOL, str STRING, v VARCHAR, bin BINARY, arr DOUBLE[], arr2 DOUBLE[][]
                    )""");
            execute("""
                    INSERT INTO t_types VALUES
                        ('gone1', true, 1, 2, 'a', 3, 4, 5.5, 6.5, '2024-01-01'::DATE, '2024-01-01T00:00:01.000001Z',
                         '2024-01-01T00:00:01.000000001Z', '11111111-1111-1111-1111-111111111111',
                         '0x01'::LONG256, #u, #u3, #u33d, #u33d33d1, '1.1.1.1', 1.5m, 12.5m, 1234567.25m,
                         123456789012345.125m, 1234567890123456789012345678901234.1234m, 1.12345m,
                         'sym1', 'string1', 'varchar1', null, ARRAY[1.0, 2.0], ARRAY[[1.0, 2.0], [3.0, 4.0]]),
                        ('gone2', false, -1, -2, 'b', -3, -4, -5.5, -6.5, '1969-12-31'::DATE, '1969-12-31T23:59:59.999999Z',
                         '1969-12-31T23:59:59.999999999Z', '22222222-2222-2222-2222-222222222222',
                         '0x02'::LONG256, #v, #v4, #v44d, #v44d44d4, '2.2.2.2', -1.5m, -12.5m, -1234567.25m,
                         -123456789012345.125m, -1234567890123456789012345678901234.1234m, -1.12345m,
                         'sym2', 'string2', 'varchar2-\u00e9\u20ac\ud83d\ude00', null, ARRAY[3.0], ARRAY[[5.0]]),
                        (null, null, 0, 0, null, null, null, null, null, null, null, null, null, null, null, null, null,
                         null, null, null, null, null, null, null, null, null, null, null, null, null, null)""");
            execute("ALTER TABLE t_types DROP COLUMN gone");
            final String committedRows = printQuery("SELECT * FROM t_types");
            final long txnBefore = interruptTruncate(table, true);
            assertOpenQueryKeepsRows(
                    "SELECT * FROM t_types",
                    committedRows,
                    "INSERT INTO t_types (l, str, v) VALUES (7, 'string7', 'varchar7'), (8, 'string8', 'varchar8')"
            );
            assertQuery("SELECT l, str, v FROM t_types")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            l\tstr\tv
                            7\tstring7\tvarchar7
                            8\tstring8\tvarchar8
                            """);
            assertOldVersionPurged(table, txnBefore, "l", false);
            assertOldVersionPurged(table, txnBefore, "v", true);
            assertOldVersionPurged(table, txnBefore, "arr2", true);
        });
    }

    @Test
    public void testFailedTruncateLeavesNothingToRepair() throws Exception {
        // TRUNCATE stops on an I/O error once it has published the empty _txn, and the pool closes its writer. It armed
        // no _todo_ entry: a query opened before the next writer open reads the empty table, that open completes
        // nothing, and the columns keep their file versions.
        final FailingTruncateFacade ff = new FailingTruncateFacade();
        assertMemoryLeak(ff, () -> {
            final String table = "t_failed";
            execute(String.format(DDL, table));
            execute(String.format("""
                    INSERT INTO %s VALUES
                        ('a', 'k1', 'p1', 'v1', 's1', 101, ARRAY[1.0]),
                        ('b', 'k2', 'p2', 'v2', 's2', 102, ARRAY[2.0])""", table));
            execute("ALTER TABLE " + table + " ADD COLUMN y INT");
            execute("INSERT INTO " + table + " VALUES ('c', 'k3', 'p3', 'v3', 's3', 103, ARRAY[3.0], 30)");
            engine.releaseAllReaders();
            final long txnBefore = readTxn(table);

            ff.isFailArmed = true;
            try {
                execute("TRUNCATE TABLE " + table + " KEEP SYMBOL MAPS");
                Assert.fail("TRUNCATE must fail");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "Data has been persisted, but we could not perform housekeeping");
                TestUtils.assertContains(e.getFlyweightMessage(), "Cannot truncate fd=");
            } finally {
                ff.isFailArmed = false;
            }
            Assert.assertTrue("the failure was not injected", ff.isFired);
            Assert.assertEquals("the failed TRUNCATE published _txn", txnBefore + 1, readTxn(table));

            try (RecordCursorFactory factory = select("SELECT * FROM " + table)) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertFalse(cursor.hasNext());
                    execute(String.format(INSERT_AFTER_REPAIR, table));
                    Assert.assertFalse(cursor.hasNext());
                    cursor.toTop();
                    Assert.assertFalse(cursor.hasNext());
                }
            }
            assertRowsAfterRepair(table);
            // KEEP SYMBOL MAPS kept the three values of each symbol column
            assertSymbolCounts(table, 4);
            assertColumnFileExists(table, "v", -1, true);
            assertColumnFileExists(table, "v", txnBefore, false);
        });
    }

    @Test
    public void testKeepSymbolMapsKeepsRowsOfOpenQuery() throws Exception {
        runWithCrashFacade(() -> {
            final String table = "t_keep";
            createTable(table);
            final long txnBefore = interruptTruncate(table, true);
            assertOpenQueryKeepsRows("SELECT * FROM " + table, COMMITTED_ROWS, String.format(INSERT_AFTER_REPAIR, table));
            assertRowsAfterRepair(table);
            assertSymbolCounts(table, 4);
            assertOldVersionPurged(table, txnBefore, "s", false);
            assertOldVersionPurged(table, txnBefore, "v", true);
            assertOldIndexPurged(table, txnBefore, "si");
        });
    }

    @Test
    public void testLargeTableKeepsRowsOfOpenQuery() throws Exception {
        // The old rows span many pages of their files: the writer close cuts only the files of the new rows. The
        // table has a designated timestamp, and the new rows come before the old ones: the repair resets it.
        runWithCrashFacade(() -> {
            final String table = "t_large";
            execute("CREATE TABLE t_large (x LONG, v VARCHAR, s SYMBOL, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t_large
                    SELECT x, 'v' || x, 's' || (x % 10), timestamp_sequence('2024-01-01', 1_000_000)
                    FROM long_sequence(""" + ROWS_OF_LARGE_TABLE + ")");
            final long txnBefore = interruptTruncate(table, true);

            try (ColumnPurgeJob purgeJob = new ColumnPurgeJob(engine)) {
                try (RecordCursorFactory factory = select("SELECT * FROM t_large")) {
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        final Record record = cursor.getRecord();
                        Assert.assertTrue(cursor.hasNext());
                        assertLargeTableRow(record, 1);
                        execute("INSERT INTO t_large VALUES (-1, 'new1', 'n1', '2020-01-01T00:00:01'), (-2, 'new2', 'n2', '2020-01-01T00:00:02')");
                        Assert.assertEquals(ROWS_OF_LARGE_TABLE - 1, assertLargeTableRows(cursor, record, 2));
                        cursor.toTop();
                        Assert.assertEquals(ROWS_OF_LARGE_TABLE, assertLargeTableRows(cursor, record, 1));
                        engine.releaseAllWriters();
                        cursor.toTop();
                        Assert.assertEquals(ROWS_OF_LARGE_TABLE, assertLargeTableRows(cursor, record, 1));

                        // the query still reads the old files: the purge leaves them alone
                        runPurgeJob(purgeJob);
                        assertColumnFileExists(table, "x", -1, true);
                    }
                }
                engine.releaseAllReaders();
                runPurgeJob(purgeJob);
                assertColumnFileExists(table, "x", -1, false);
                assertColumnFileExists(table, "v", -1, false);
                assertColumnFileExists(table, "x", txnBefore, true);
            }
            assertQuery("SELECT * FROM t_large")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            x\tv\ts\tts
                            -1\tnew1\tn1\t2020-01-01T00:00:01.000000Z
                            -2\tnew2\tn2\t2020-01-01T00:00:02.000000Z
                            """);
        });
    }

    @Test
    public void testNoQueryPurgesOldFilesAtRepair() throws Exception {
        // nothing reads the old rows: the repair removes their files as soon as _txn names the new versions
        runWithCrashFacade(() -> {
            final String table = "t_no_query";
            createTable(table);
            final long txnBefore = interruptTruncate(table, true);
            assertColumnFileExists(table, "s", -1, true);
            try (TableWriter writer = getWriter(table)) {
                Assert.assertEquals(0, writer.size());
                Assert.assertEquals(txnBefore + 1, writer.getTxn());
            }
            for (String column : new String[]{"s", "si", "sp", "v", "str", "arr"}) {
                assertColumnFileExists(table, column, -1, false);
                assertColumnFileExists(table, column, txnBefore, true);
            }
            assertIndexFileExists(table, "si", -1, false);
            assertIndexFileExists(table, "si", txnBefore, true);
            execute(String.format(INSERT_AFTER_REPAIR, table));
            assertRowsAfterRepair(table);

            // the repaired table keeps working: TRUNCATE, UPDATE and column changes across a restart
            engine.releaseAllWriters();
            engine.releaseAllReaders();
            execute("UPDATE " + table + " SET x = x + 1000");
            execute("ALTER TABLE " + table + " RENAME COLUMN str TO str2");
            execute("ALTER TABLE " + table + " DROP COLUMN arr");
            execute("ALTER TABLE " + table + " ADD COLUMN z INT");
            execute("INSERT INTO " + table + " VALUES ('c', 'k1', 'p7', 'v7', 's7', 7, 70, 700)");
            engine.releaseAllWriters();
            engine.releaseAllReaders();
            assertQuery("SELECT * FROM " + table)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            s\tsi\tsp\tv\tstr2\tx\ty\tz
                            a\tk1\tp1\tv5\ts5\t1005\t50\tnull
                            z\tk9\tp9\tv6\ts6\t1006\t60\tnull
                            c\tk1\tp7\tv7\ts7\t7\t70\t700
                            """);
            assertQuery("SELECT s, x FROM " + table + " WHERE sp = 'p9'")
                    .noLeakCheck()
                    .returns("s\tx\nz\t1006\n");
            execute("TRUNCATE TABLE " + table);
            execute("INSERT INTO " + table + " VALUES ('b', 'k2', 'p2', 'v8', 's8', 8, 80, 800)");
            assertQuery("SELECT * FROM " + table)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            s\tsi\tsp\tv\tstr2\tx\ty\tz
                            b\tk2\tp2\tv8\ts8\t8\t80\t800
                            """);
        });
    }

    @Test
    public void testPlainTruncateKeepsRowsOfOpenQuery() throws Exception {
        // The earlier build's TRUNCATE without KEEP SYMBOL MAPS emptied the symbol maps before it armed _todo_, and a
        // query cannot open a SYMBOL column of its interrupted state. A table without one takes the same repair.
        runWithCrashFacade(() -> {
            final String table = "t_plain";
            execute("CREATE TABLE t_plain (v VARCHAR, str STRING, x LONG, arr DOUBLE[])");
            execute("INSERT INTO t_plain VALUES ('v1', 's1', 1, ARRAY[1.0]), ('v2', 's2', 2, ARRAY[2.0])");
            execute("ALTER TABLE t_plain ADD COLUMN y INT");
            execute("INSERT INTO t_plain VALUES ('v3', 's3', 3, ARRAY[3.0], 30)");
            execute("UPDATE t_plain SET x = x + 100");
            final long txnBefore = interruptTruncate(table, false);
            assertOpenQueryKeepsRows(
                    "SELECT * FROM t_plain",
                    """
                            v\tstr\tx\tarr\ty
                            v1\ts1\t101\t[1.0]\tnull
                            v2\ts2\t102\t[2.0]\tnull
                            v3\ts3\t103\t[3.0]\t30
                            """,
                    "INSERT INTO t_plain VALUES ('v5', 's5', 5, ARRAY[5.0], 50), ('v6', 's6', 6, ARRAY[6.0], 60)"
            );
            assertQuery("SELECT * FROM t_plain")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            v\tstr\tx\tarr\ty
                            v5\ts5\t5\t[5.0]\t50
                            v6\ts6\t6\t[6.0]\t60
                            """);
            assertOldVersionPurged(table, txnBefore, "str", true);
            assertOldVersionPurged(table, txnBefore, "arr", true);
        });
    }

    @Test
    public void testQueryOpenedWhileRepairRemovesOldFilesReadsEmptyTable() throws Exception {
        // No query reads the old rows, so the repair removes their files at once. It does so after it published the
        // empty _txn: a query that opens the table while the files go must read the empty table, not the old rows.
        final BlockingPurgeFacade ff = new BlockingPurgeFacade();
        assertMemoryLeak(ff, () -> {
            final String table = "t_concurrent";
            execute("CREATE TABLE t_concurrent (x LONG, v VARCHAR)");
            execute("INSERT INTO t_concurrent VALUES (1, 'v1'), (2, 'v2'), (3, 'v3')");
            final long txnBefore = interruptTruncate(table, false);

            ff.isBlockArmed = true;
            final AtomicReference<Throwable> insertError = new AtomicReference<>();
            final Thread insertThread = new Thread(() -> {
                try (SqlExecutionContext context = TestUtils.createSqlExecutionCtx(engine)) {
                    // the writer open repairs the table, and blocks in the removal of the old file of x
                    engine.execute("INSERT INTO t_concurrent VALUES (5, 'v5'), (6, 'v6')", context);
                } catch (Throwable th) {
                    insertError.set(th);
                } finally {
                    ff.purgeReached.countDown();
                    Path.clearThreadLocals();
                }
            });
            insertThread.start();
            try {
                Assert.assertTrue("the repair did not reach the removal of the old files", ff.purgeReached.await(60, TimeUnit.SECONDS));
                Assert.assertNull(insertError.get());
                Assert.assertEquals(txnBefore + 1, readTxn(table));
                assertQuery("SELECT * FROM t_concurrent")
                        .noLeakCheck()
                        .expectSize()
                        .returns("x\tv\n");
                engine.releaseAllReaders();
            } finally {
                ff.purgeReleased.countDown();
                insertThread.join();
            }
            Assert.assertNull(insertError.get());
            assertQuery("SELECT * FROM t_concurrent")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            x\tv
                            5\tv5
                            6\tv6
                            """);
            assertColumnFileExists(table, "x", -1, false);
            assertColumnFileExists(table, "v", -1, false);
            assertColumnFileExists(table, "x", txnBefore, true);
        });
    }

    @Test
    public void testStaleFileThatStaysFailsRepair() throws Exception {
        // A file under the version the repair moves a column to cannot be removed: the writer open fails and leaves
        // the table as the interrupted TRUNCATE did, and the next open, once the file is gone, completes it.
        runWithCrashFacade(() -> {
            final String table = "t_stuck";
            createTable(table);
            final long txnBefore = interruptTruncate(table, true);
            final FilesFacade ff = engine.getConfiguration().getFilesFacade();
            try (Path path = partitionPath(table)) {
                // a directory that holds a file, in place of the new data file of v: remove() refuses it
                Assert.assertEquals(0, ff.mkdir(TableUtils.dFile(path, "v", txnBefore), configuration.getMkDirMode()));
                Assert.assertTrue(ff.touch(path.concat("obstacle").$()));
            }
            try {
                execute(String.format(INSERT_AFTER_REPAIR, table));
                Assert.fail("the repair must fail");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "cannot remove stale column file [path=");
                TestUtils.assertContains(e.getFlyweightMessage(), "v.d." + txnBefore + ']');
            }
            Assert.assertEquals("the failed repair published _txn", txnBefore, readTxn(table));
            assertQuery("SELECT * FROM " + table)
                    .noLeakCheck()
                    .expectSize()
                    .returns(COMMITTED_ROWS);
            engine.releaseAllReaders();

            try (Path path = partitionPath(table)) {
                TableUtils.dFile(path, "v", txnBefore);
                Assert.assertTrue(ff.rmdir(path, false));
            }
            execute(String.format(INSERT_AFTER_REPAIR, table));
            assertRowsAfterRepair(table);
            assertSymbolCounts(table, 4);
            assertOldVersionPurged(table, txnBefore, "v", true);
        });
    }

    @Test
    public void testStaleFilesUnderNewVersionAreReplaced() throws Exception {
        // An UPDATE rolled back at the txn the repair starts from leaves files under the version the repair moves
        // the columns to: data of the updated column x, and, for sp, whose index INCLUDEs x, a hard link to its
        // committed data (UpdateOperatorImpl.moveIndexedColumnToNewVersion). The rows written after the repair must
        // go to new files, not into the link, which a query of the old rows reads through the old name.
        runWithCrashFacade(() -> {
            final String table = "t_stale";
            createTable(table);
            final long txnBefore = interruptTruncate(table, true);
            final long spColumnNameTxn;
            try (TableReader reader = getReader(table)) {
                spColumnNameTxn = reader.getColumnVersionReader().getColumnNameTxn(0, reader.getMetadata().getWriterIndex(reader.getMetadata().getColumnIndex("sp")));
            }
            engine.releaseAllReaders();
            final FilesFacade ff = engine.getConfiguration().getFilesFacade();
            try (Path committed = partitionPath(table); Path link = partitionPath(table)) {
                Assert.assertEquals(0, ff.hardLink(TableUtils.dFile(committed, "sp", spColumnNameTxn), TableUtils.dFile(link, "sp", txnBefore)));
            }
            try (Path path = partitionPath(table)) {
                TableUtils.dFile(path, "x", txnBefore);
                final byte[] garbage = new byte[4096];
                java.util.Arrays.fill(garbage, (byte) 0x7f);
                java.nio.file.Files.write(java.nio.file.Path.of(path.toString()), garbage);
                Assert.assertTrue(TestFilesFacadeImpl.INSTANCE.exists(path.$()));
            }

            assertOpenQueryKeepsRows("SELECT * FROM " + table, COMMITTED_ROWS, String.format(INSERT_AFTER_REPAIR, table));
            assertRowsAfterRepair(table);
            assertSymbolCounts(table, 4);
        });
    }

    private static void assertColumnFileExists(String table, String column, long columnNameTxn, boolean isExpected) {
        try (Path path = partitionPath(table)) {
            final int partitionDirLen = path.size();
            Assert.assertEquals(
                    column + " data file, version " + columnNameTxn,
                    isExpected,
                    TestFilesFacadeImpl.INSTANCE.exists(TableUtils.dFile(path, column, columnNameTxn))
            );
            path.trimTo(partitionDirLen);
        }
    }

    private static void assertIndexFileExists(String table, String column, long columnNameTxn, boolean isExpected) {
        try (Path path = partitionPath(table)) {
            final int partitionDirLen = path.size();
            final LPSZ keyFile = io.questdb.cairo.idx.BitmapIndexUtils.keyFileName(path, column, columnNameTxn);
            Assert.assertEquals(column + " index key file, version " + columnNameTxn, isExpected, TestFilesFacadeImpl.INSTANCE.exists(keyFile));
            final LPSZ valueFile = io.questdb.cairo.idx.BitmapIndexUtils.valueFileName(path.trimTo(partitionDirLen), column, columnNameTxn);
            Assert.assertEquals(column + " index value file, version " + columnNameTxn, isExpected, TestFilesFacadeImpl.INSTANCE.exists(valueFile));
        }
    }

    // row k of t_large holds x = k, v = 'v' || k, s = 's' || (k % 10), ts = 2024-01-01 plus k - 1 seconds
    private static void assertLargeTableRow(Record record, long k) {
        Assert.assertEquals("x of row " + k, k, record.getLong(0));
        TestUtils.assertEquals("v of row " + k, "v" + k, Utf8s.toString(record.getVarcharA(1)));
        TestUtils.assertEquals("s of row " + k, "s" + (k % 10), record.getSymA(2));
        Assert.assertEquals("ts of row " + k, 1_704_067_200_000_000L + (k - 1) * 1_000_000L, record.getTimestamp(3));
    }

    private static long assertLargeTableRows(RecordCursor cursor, Record record, long firstK) {
        long k = firstK;
        while (cursor.hasNext()) {
            assertLargeTableRow(record, k++);
        }
        return k - firstK;
    }

    private static void assertSymbolCounts(String table, int expected) {
        try (TableReader reader = getReader(table)) {
            for (String column : new String[]{"s", "si", "sp"}) {
                final int columnIndex = reader.getMetadata().getColumnIndex(column);
                Assert.assertEquals(column, expected, reader.getSymbolMapReader(columnIndex).getSymbolCount());
            }
        }
    }

    /**
     * Arms the _todo_ entry of a TRUNCATE of the table that an earlier build interrupted before it published _txn, and
     * lets the engine go of the table, as on a restart. Without KEEP SYMBOL MAPS, the process died with the symbol maps
     * emptied. Returns the _txn txn the entry is stamped with.
     */
    private static long interruptTruncate(String table, boolean isKeepSymbolMaps) {
        engine.releaseAllReaders();
        engine.releaseAllWriters();
        return LegacyTruncateTodo.arm(
                engine,
                table,
                PartitionBy.NONE,
                isKeepSymbolMaps ? SymbolMapState.INTACT : SymbolMapState.EMPTIED_KILLED
        );
    }

    private static Path partitionPath(String table) {
        return new Path().of(root).concat(engine.verifyTableName(table)).concat(TableUtils.DEFAULT_PARTITION_NAME);
    }

    private static long readTxn(String table) {
        final TableToken token = engine.verifyTableName(table);
        try (
                TxReader txReader = new TxReader(engine.getConfiguration().getFilesFacade());
                Path path = new Path().of(root).concat(token).concat(TableUtils.TXN_FILE_NAME)
        ) {
            txReader.ofRO(path.$(), ColumnType.TIMESTAMP, PartitionBy.NONE);
            TableUtils.safeReadTxn(txReader, configuration.getMillisecondClock(), configuration.getSpinLockTimeout());
            return txReader.getTxn();
        }
    }

    private static void runPurgeJob(ColumnPurgeJob purgeJob) {
        engine.releaseInactive();
        setCurrentMicros(currentMicros + 1_000);
        purgeJob.run();
        setCurrentMicros(currentMicros + 1_000);
        purgeJob.run();
    }

    private void assertOldIndexPurged(String table, long txnBefore, String column) {
        assertIndexFileExists(table, column, -1, false);
        assertIndexFileExists(table, column, txnBefore, true);
    }

    private void assertOldVersionPurged(String table, long txnBefore, String column, boolean isVarSize) {
        assertColumnFileExists(table, column, -1, false);
        assertColumnFileExists(table, column, txnBefore, true);
        if (isVarSize) {
            try (Path path = partitionPath(table)) {
                final int partitionDirLen = path.size();
                Assert.assertFalse(column + " old aux file", TestFilesFacadeImpl.INSTANCE.exists(TableUtils.iFile(path, column, -1)));
                Assert.assertTrue(column + " new aux file", TestFilesFacadeImpl.INSTANCE.exists(TableUtils.iFile(path.trimTo(partitionDirLen), column, txnBefore)));
            }
        }
    }

    /**
     * Opens {@code query} over the old rows, reads its first row, runs {@code insertSql} - the first write after the
     * interrupted TRUNCATE, whose writer open completes it - and reads the rest, then again from the top with the
     * writer open, and once more after the writer closed. Every pass must return {@code expected}. The column purge
     * leaves the old files to the open query and removes them once it is closed.
     */
    private void assertOpenQueryKeepsRows(String query, String expected, String insertSql) throws Exception {
        try (ColumnPurgeJob purgeJob = new ColumnPurgeJob(engine)) {
            try (RecordCursorFactory factory = select(query)) {
                final RecordMetadata metadata = factory.getMetadata();
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    final Record record = cursor.getRecord();
                    printSink.clear();
                    CursorPrinter.println(metadata, printSink);
                    Assert.assertTrue(cursor.hasNext());
                    CursorPrinter.println(record, metadata, printSink);

                    execute(insertSql);
                    while (cursor.hasNext()) {
                        CursorPrinter.println(record, metadata, printSink);
                    }
                    TestUtils.assertEquals("the rows read across the repair", expected, printSink);

                    cursor.toTop();
                    CursorPrinter.println(cursor, metadata, printSink);
                    TestUtils.assertEquals("the rows read after the repair", expected, printSink);

                    engine.releaseAllWriters();
                    cursor.toTop();
                    CursorPrinter.println(cursor, metadata, printSink);
                    TestUtils.assertEquals("the rows read after the writer closed", expected, printSink);

                    runPurgeJob(purgeJob);
                    cursor.toTop();
                    CursorPrinter.println(cursor, metadata, printSink);
                    TestUtils.assertEquals("the rows read after the column purge ran", expected, printSink);
                }
            }
            engine.releaseAllReaders();
            runPurgeJob(purgeJob);
        }
    }

    private void assertRowsAfterRepair(String table) throws Exception {
        assertQuery("SELECT * FROM " + table)
                .noLeakCheck()
                .expectSize()
                .returns(ROWS_AFTER_REPAIR);
        assertQuery("SELECT s, x FROM " + table + " WHERE si = 'k9'")
                .noLeakCheck()
                .returns("s\tx\nz\t6\n");
        assertQuery("SELECT s, x FROM " + table + " WHERE sp = 'p1'")
                .noLeakCheck()
                .returns("s\tx\na\t5\n");
    }

    // the rows of COMMITTED_ROWS: y added while the table held rows, then x moved to a new version by UPDATE
    private void createTable(String table) throws Exception {
        execute(String.format(DDL, table));
        execute(String.format("""
                INSERT INTO %s VALUES
                    ('a', 'k1', 'p1', 'v1', 's1', 1, ARRAY[1.0]),
                    ('b', 'k2', 'p2', 'v2', 's2', 2, ARRAY[2.0])""", table));
        execute("ALTER TABLE " + table + " ADD COLUMN y INT");
        execute("INSERT INTO " + table + " VALUES ('c', 'k3', 'p3', 'v3', 's3', 3, ARRAY[3.0], 30)");
        execute("UPDATE " + table + " SET x = x + 100");
    }

    private String printQuery(String query) throws Exception {
        try (RecordCursorFactory factory = select(query)) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                CursorPrinter.println(cursor, factory.getMetadata(), printSink);
            }
        }
        return printSink.toString();
    }

    /**
     * Stops, once armed, the thread that removes the old data file of x, until the test lets it go.
     */
    private static class BlockingPurgeFacade extends TestFilesFacadeImpl {
        final CountDownLatch purgeReached = new CountDownLatch(1);
        final CountDownLatch purgeReleased = new CountDownLatch(1);
        volatile boolean isBlockArmed;

        @Override
        public boolean removeQuiet(LPSZ name) {
            if (isBlockArmed && Utf8s.endsWithAscii(name, Files.SEPARATOR + TableUtils.DEFAULT_PARTITION_NAME + Files.SEPARATOR + "x.d")) {
                isBlockArmed = false;
                purgeReached.countDown();
                try {
                    if (!purgeReleased.await(60, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("the test did not release the purge");
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            return super.removeQuiet(name);
        }
    }

    /**
     * Fails truncate() of the data file of column x while armed: TRUNCATE of a non-partitioned table fails after it
     * published the empty _txn.
     */
    private static class FailingTruncateFacade extends TestFilesFacadeImpl {
        volatile boolean isFailArmed;
        volatile boolean isFired;
        private long fd = -1;

        @Override
        public long openRW(LPSZ name, int opts) {
            final long fd = super.openRW(name, opts);
            if (Utf8s.endsWithAscii(name, Files.SEPARATOR + "x.d")) {
                this.fd = fd;
            }
            return fd;
        }

        @Override
        public boolean truncate(long fd, long size) {
            if (isFailArmed && fd == this.fd) {
                isFired = true;
                return false;
            }
            return super.truncate(fd, size);
        }
    }
}
