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

package io.questdb.test.cairo.parquet;

import io.questdb.PropertyKey;
import io.questdb.cairo.O3PartitionJob;
import io.questdb.cairo.PartitionCompactionScanJob;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.wal.TableWriterPressureControl;
import io.questdb.cairo.wal.WalTxnDetails;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.seq.TransactionLogCursor;
import io.questdb.std.FilesFacade;
import io.questdb.std.Misc;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.str.DirectUtf8StringZ;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * A requested parquet conversion writes a whole logical partition - one plain folder, a composite folder whose pieces
 * sit anywhere in its files, a run of MOVE-TAIL splits - into one parquet file in a single pass, its live pieces handed
 * to the encoder as frames that need not line up with row groups. Every test compares the converted table against a
 * native copy taken before the conversion, and reads it through its symbol index too. The {@code DiscardsTheSwap}
 * tests commit into the day between the conversion's build and its swap, which the swap has to decline.
 */
public class PartitionCompactionParquetConversionTest extends AbstractCairoTest {
    private static final long DAY = parse("2020-01-01T00:00:00.000000Z");
    // The rows a test commits into the day between the conversion's build and its swap.
    private static final int LATE_ROWS = 200;

    @Override
    @Before
    public void setUp() {
        super.setUp();
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_CHECK_INTERVAL, -1);
        // Row groups far smaller than any folder or piece, and not a divisor of either, so row groups cut across
        // frames and frames across row groups.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 997);
    }

    @Test
    public void testAlterConvertsACompositeDay() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");
        assertMemoryLeak(() -> {
            createTable("ac", true);
            insertBackfill("ac", "2020-01-01T04:00:07");
            assertFolders("ac", 1, true);
            assertAlterConversion("ac", true);
        });
    }

    @Test
    public void testAlterConvertsAPlainDay() throws Exception {
        assertMemoryLeak(() -> {
            createTable("ap", true);
            assertFolders("ap", 1, false);
            assertAlterConversion("ap", true);
        });
    }

    @Test
    public void testAlterConvertsASplitDay() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");
        node1.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 50);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_HOT_COMMITS, 0);
        assertMemoryLeak(() -> {
            createTable("asd", true);
            insertBackfill("asd", "2020-01-01T22:00:07");
            assertFolders("asd", 2, false);
            assertAlterConversion("asd", true);
        });
    }

    /**
     * The active day goes through the fold before its conversion. Its split folded away, the day is the last
     * partition again, so the writer has to let go of the native files it had open on it - a column a later
     * ALTER COLUMN TYPE swaps out must not leave the others open over the parquet partition.
     */
    @Test
    public void testAlterConvertsASplitActiveDay() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MAX_SPLITS, 50);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE aa (i INT, s SYMBOL INDEX, late LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO aa SELECT x::INT, rnd_symbol('k1', 'k2', NULL), x, timestamp_sequence('2020-01-01', 10 * 1_000_000L)" +
                    " FROM long_sequence(4000)");
            drainWalQueue();
            // Lands near the end of the active day, so the O3 path splits it.
            execute("INSERT INTO aa SELECT x::INT + 70_000, rnd_symbol('k1', 'k2', NULL), x + 1_000_000," +
                    " timestamp_sequence('2020-01-01T11:06:07', 5 * 1_000_000L) FROM long_sequence(200)");
            drainWalQueue();
            assertFolders("aa", 2, false);

            execute("ALTER TABLE aa CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();
            assertFolders("aa", 1, false);
            execute("ALTER TABLE aa ALTER COLUMN late TYPE DOUBLE");
            // O3 into the parquet last partition, under the changed schema.
            execute("INSERT INTO aa SELECT x::INT + 90_000, rnd_symbol('k1', 'k2', NULL), x + 2_000_000," +
                    " timestamp_sequence('2020-01-01T05:00:03', 7 * 1_000_000L) FROM long_sequence(300)");
            drainWalQueue();
            assertQuery("SELECT count(), sum(i), sum(late) FROM aa")
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            count\tsum\tsum1
                            4500\t49067250\t8.0806725E8
                            """);
            try (TableReader reader = engine.getReader("aa")) {
                Assert.assertTrue("the day must be parquet", reader.getTxFile().isPartitionParquet(0));
            }
        });
    }

    @Test
    public void testAlterConvertsASplitDayOfANonWalTable() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");
        node1.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 50);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_HOT_COMMITS, 0);
        assertMemoryLeak(() -> {
            createTable("an", false);
            insertBackfill("an", "2020-01-01T22:00:07");
            assertFolders("an", 2, false);
            assertAlterConversion("an", false);
        });
    }

    @Test
    public void testAlterConvertsASplitDayWithBloomFilterOverride() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");
        node1.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 50);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_HOT_COMMITS, 0);
        assertMemoryLeak(() -> {
            createTable("ab", true);
            insertBackfill("ab", "2020-01-01T22:00:07");
            assertFolders("ab", 2, false);
            execute("CREATE TABLE ab_oracle AS (SELECT * FROM ab) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            assertAlterConversionMatchesOracle("ab", true, " WITH (bloom_filter_columns = 'i, late', fpp = '0.05')");
            // A point lookup the bloom filters can prune row groups for still finds its row.
            for (String filter : new String[]{"i = 70_005", "late = 1_000_007", "i = -1"}) {
                TestUtils.assertSqlCursors(
                        engine,
                        sqlExecutionContext,
                        "SELECT * FROM ab_oracle WHERE " + filter,
                        "SELECT * FROM ab WHERE " + filter,
                        LOG
                );
            }
        });
    }

    @Test
    public void testConvertsACompositeDay() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");
        assertMemoryLeak(() -> {
            createTable("cc", true);
            // Lands only inside 2020-01-01, cutting it into pieces.
            insertBackfill("cc", "2020-01-01T04:00:07");
            assertFolders("cc", 1, true);
            assertConversion("cc", true);
        });
    }

    @Test
    public void testConvertsAPlainDay() throws Exception {
        assertMemoryLeak(() -> {
            createTable("cp", true);
            assertFolders("cp", 1, false);
            assertConversion("cp", true);
        });
    }

    @Test
    public void testConvertsASplitDay() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");
        node1.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 50);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_HOT_COMMITS, 0);
        assertMemoryLeak(() -> {
            createTable("cs", true);
            // Lands near the END of 2020-01-01, so the O3 path splits the day rather than rewriting it whole.
            insertBackfill("cs", "2020-01-01T22:00:07");
            assertFolders("cs", 2, false);
            assertConversion("cs", true);
        });
    }

    /**
     * A non-WAL writer holding a transaction open does not stop the conversion: the swap commits on its own, and the
     * transaction's rows - for the last partition only - stay the owner's to commit.
     */
    @Test
    public void testConvertsASplitDayAroundANonWalOpenTransaction() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");
        node1.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 50);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_HOT_COMMITS, 0);
        assertMemoryLeak(() -> {
            createTable("cn", true);
            insertBackfill("cn", "2020-01-01T22:00:07");
            execute("ALTER TABLE cn SET TYPE BYPASS WAL");
            engine.releaseInactive();
            engine.load();
            final TableToken token = engine.verifyTableName("cn");
            Assert.assertFalse(token.isWal());
            assertFolders("cn", 2, false);
            execute("CREATE TABLE cn_oracle AS (SELECT * FROM cn) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            try (TableWriter ownerWriter = engine.getWriter(token, "owner")) {
                final TableWriter.Row row = ownerWriter.newRow(parse("2020-01-03T05:00:00.000000Z"));
                row.putInt(0, 99_999);
                row.append();
                Assert.assertTrue(ownerWriter.inTransaction());

                Assert.assertTrue(engine.getPartitionCompactionRequestQueue().tryRequestParquetConversion(token, DAY));
                runJobOnAnotherThread();
                ownerWriter.tick();
                Assert.assertTrue("the swap must leave the transaction open", ownerWriter.inTransaction());
                assertConverted("cn");
                TestUtils.assertSqlCursors(engine, sqlExecutionContext, "SELECT * FROM cn_oracle", "SELECT * FROM cn", LOG);
                ownerWriter.commit();
            }
            execute("INSERT INTO cn_oracle (i, ts) VALUES (99_999, '2020-01-03T05:00:00.000000Z')");
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "SELECT * FROM cn_oracle", "SELECT * FROM cn", LOG);
        });
    }

    /**
     * A non-WAL writer - the one ILP holds - appends rows past the day's last row in place: the folder keeps its
     * name txn and grows. The swap's row-count check is the one that catches it.
     */
    @Test
    public void testDiscardsTheSwapWhenANonWalWriterAppendsToTheDayBetweenBuildAndSwap() throws Exception {
        assertMemoryLeak(() -> {
            createTable("na", false);
            assertFolders("na", 1, false);
            assertWriteBetweenBuildAndSwapIsKept("na", false, "2020-01-01T23:30:00.000000Z", 1, false);
        });
    }

    /**
     * A WAL transaction appends rows past the day's last row in place: the folder keeps its name txn and grows.
     * The swap's row-count check is the one that catches it.
     */
    @Test
    public void testDiscardsTheSwapWhenAWalTransactionAppendsToTheDayBetweenBuildAndSwap() throws Exception {
        assertMemoryLeak(() -> {
            createTable("wa", true);
            assertFolders("wa", 1, false);
            assertWriteBetweenBuildAndSwapIsKept("wa", true, "2020-01-01T23:30:00.000000Z", 1, false);
        });
    }

    /**
     * A WAL transaction landing near the end of the day splits it: a second folder of the same day appears
     * behind the one the build read, taking the day's tail rows with it. The swap's run-shape check catches it,
     * and so does the first folder's row count on its own.
     */
    @Test
    public void testDiscardsTheSwapWhenAWalTransactionSplitsTheDayBetweenBuildAndSwap() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");
        node1.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 50);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_HOT_COMMITS, 0);
        assertMemoryLeak(() -> {
            createTable("ws", true);
            assertFolders("ws", 1, false);
            assertWriteBetweenBuildAndSwapIsKept("ws", true, "2020-01-01T22:00:07.000000Z", 2, false);
        });
    }

    /**
     * A WAL transaction landing inside the day turns it composite in place: the folder keeps its name txn, grows
     * and moves its generation. The swap's row-count and generation checks are the ones that catch it.
     */
    @Test
    public void testDiscardsTheSwapWhenAWalTransactionTurnsTheDayCompositeBetweenBuildAndSwap() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");
        assertMemoryLeak(() -> {
            createTable("wc", true);
            assertFolders("wc", 1, false);
            assertWriteBetweenBuildAndSwapIsKept("wc", true, "2020-01-01T04:00:07.000000Z", 1, true);
        });
    }

    /**
     * The conversion never touches the active logical partition, nor one already in parquet.
     */
    @Test
    public void testLeavesTheActiveAndAParquetDayAlone() throws Exception {
        assertMemoryLeak(() -> {
            createTable("ca", true);
            final TableToken token = engine.verifyTableName("ca");
            try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(engine)) {
                Assert.assertTrue(engine.getPartitionCompactionRequestQueue().tryRequestParquetConversion(token, parse("2020-01-03T00:00:00.000000Z")));
                Assert.assertFalse("the active day must not be converted", job.run());
                Assert.assertTrue(engine.getPartitionCompactionRequestQueue().tryRequestParquetConversion(token, DAY));
                Assert.assertTrue(job.run());
                Assert.assertTrue(engine.getPartitionCompactionRequestQueue().tryRequestParquetConversion(token, DAY));
                Assert.assertFalse("a parquet day must not be converted again", job.run());
            }
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            assertConverted("ca");
        });
    }

    private static void assertConverted(String tableName) {
        try (TableReader reader = engine.getReader(tableName)) {
            final TxReader tx = reader.getTxFile();
            final int index = tx.getPartitionIndex(DAY);
            Assert.assertEquals("the day must be one folder", 0, index);
            Assert.assertTrue("the day must be parquet", tx.isPartitionParquet(index));
            Assert.assertFalse("the day must not be composite", tx.isPartitionComposite(index));
            Assert.assertNotEquals("the day must have no split left", DAY, tx.getLogicalPartitionTimestamp(tx.getPartitionTimestampByIndex(index + 1)));
            for (int c = 0, n = reader.getMetadata().getColumnCount(); c < n; c++) {
                Assert.assertEquals("every column top must be zero", 0, reader.getColumnVersionReader().getColumnTop(DAY, reader.getMetadata().getWriterIndex(c)));
            }
        }
    }

    private static void assertFolders(String tableName, int folderCount, boolean isComposite) {
        try (TableReader reader = engine.getReader(tableName)) {
            final TxReader tx = reader.getTxFile();
            int count = 0;
            boolean anyComposite = false;
            for (int i = 0, n = tx.getPartitionCount(); i < n; i++) {
                if (tx.getLogicalPartitionTimestamp(tx.getPartitionTimestampByIndex(i)) == DAY) {
                    count++;
                    anyComposite |= tx.isPartitionComposite(i);
                }
            }
            Assert.assertEquals("folders of 2020-01-01", folderCount, count);
            Assert.assertEquals("2020-01-01 composite", isComposite, anyComposite);
        }
    }

    private static void assertNoStagingDirectory(TableToken token) {
        try (Path path = new Path()) {
            final DirectUtf8StringZ name = new DirectUtf8StringZ();
            final FilesFacade ff = engine.getConfiguration().getFilesFacade();
            ff.iterateDir(path.of(engine.getConfiguration().getDbRoot()).concat(token.getDirName()).$(), (pUtf8NameZ, _) -> {
                final String entry = name.of(pUtf8NameZ).toString();
                Assert.assertFalse("a declined swap must remove its staging directory: " + entry, entry.contains(TableUtils.MERGING_DIR_MARKER));
            });
        }
    }

    private static long dayNameTxn(String tableName) {
        try (TableReader reader = engine.getReader(tableName)) {
            final TxReader tx = reader.getTxFile();
            return tx.getPartitionNameTxn(tx.getPartitionIndex(DAY));
        }
    }

    /**
     * {@link #LATE_ROWS} rows every 5 seconds from {@code from}, deterministic so {@link #commitLateRows} can write
     * the same rows without SQL.
     */
    private static String lateRowsInsert(String tableName, String from) {
        return "INSERT INTO " + tableName + " (i, s, late, ts) SELECT x::INT + 80_000, CASE WHEN x % 2 = 0 THEN 'k1' ELSE 'k2' END," +
                " x + 2_000_000, timestamp_sequence('" + from + "', 5 * 1_000_000L) FROM long_sequence(" + LATE_ROWS + ")";
    }

    private static long parse(String timestamp) {
        try {
            return MicrosFormatUtils.parseTimestamp(timestamp);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void runJobOnAnotherThread() throws InterruptedException {
        final Throwable[] failure = new Throwable[1];
        final Thread thread = new Thread(() -> {
            try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(engine)) {
                job.run();
            } catch (Throwable e) {
                failure[0] = e;
            } finally {
                Misc.free(O3PartitionJob.THREAD_LOCAL_CLEANER);
                Path.clearThreadLocals();
            }
        });
        thread.start();
        thread.join();
        if (failure[0] != null) {
            throw new AssertionError("the job failed on its own thread", failure[0]);
        }
    }

    private static long tableTxn(String tableName) {
        try (TableReader reader = engine.getReader(tableName)) {
            return reader.getTxn();
        }
    }

    private void assertAlterConversion(String tableName, boolean isWal) throws Exception {
        execute("CREATE TABLE " + tableName + "_oracle AS (SELECT * FROM " + tableName + ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
        assertAlterConversionMatchesOracle(tableName, isWal, "");
    }

    /**
     * CONVERT PARTITION on the table writer converts the whole logical partition in a single pass too: one commit,
     * with no fold of its splits or pieces first.
     */
    private void assertAlterConversionMatchesOracle(String tableName, boolean isWal, String withClause) throws Exception {
        engine.releaseAllReaders();
        engine.releaseAllWriters();
        final TableToken token = engine.verifyTableName(tableName);
        Assert.assertEquals(isWal, token.isWal());
        final long txnBefore = tableTxn(tableName);
        execute("ALTER TABLE " + tableName + " CONVERT PARTITION TO PARQUET LIST '2020-01-01'" + withClause);
        drainWalQueue();
        Assert.assertEquals("the conversion must commit once, with no fold first", txnBefore + 1, tableTxn(tableName));
        engine.releaseAllReaders();
        engine.releaseAllWriters();
        assertConvertedMatchesOracle(tableName);
    }

    private void assertConversion(String tableName, boolean isWal) throws Exception {
        execute("CREATE TABLE " + tableName + "_oracle AS (SELECT * FROM " + tableName + ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
        assertConversionMatchesOracle(tableName, isWal);
    }

    private void assertConversionMatchesOracle(String tableName, boolean isWal) throws Exception {
        engine.releaseAllReaders();
        engine.releaseAllWriters();
        final TableToken token = engine.verifyTableName(tableName);
        Assert.assertEquals(isWal, token.isWal());
        try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(engine)) {
            Assert.assertTrue(engine.getPartitionCompactionRequestQueue().tryRequestParquetConversion(token, DAY));
            Assert.assertTrue("the request must dispatch the conversion", job.run());
        }
        engine.releaseAllReaders();
        engine.releaseAllWriters();
        assertConvertedMatchesOracle(tableName);
    }

    private void assertConvertedMatchesOracle(String tableName) throws Exception {
        assertConverted(tableName);
        TestUtils.assertSqlCursors(
                engine, sqlExecutionContext, "SELECT * FROM " + tableName + "_oracle", "SELECT * FROM " + tableName, LOG
        );
        // Through the index the conversion built off the new file.
        for (String filter : new String[]{"s = 'k1'", "s = 'k2'", "s IS NULL", "s IN ('k1', 'k2')"}) {
            TestUtils.assertSqlCursors(
                    engine,
                    sqlExecutionContext,
                    "SELECT * FROM " + tableName + "_oracle WHERE " + filter,
                    "SELECT * FROM " + tableName + " WHERE " + filter,
                    LOG
            );
        }
    }

    /**
     * Requests the conversion while holding the writer, so the job builds the file off its reader snapshot and
     * queues the swap; commits {@link #LATE_ROWS} rows from {@code from} into the day through the held writer; then
     * ticks the swap - the order the WAL apply loop runs a swap queued mid-batch in. The swap must decline: the
     * day stays native in the shape the write left it, under the same name txn, with every row, and the next
     * request converts it with the late rows in the file.
     */
    private void assertWriteBetweenBuildAndSwapIsKept(
            String tableName,
            boolean isWal,
            String from,
            int folderCount,
            boolean isComposite
    ) throws Exception {
        execute("CREATE TABLE " + tableName + "_oracle AS (SELECT * FROM " + tableName + ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
        engine.releaseAllReaders();
        engine.releaseAllWriters();
        final TableToken token = engine.verifyTableName(tableName);
        Assert.assertEquals(isWal, token.isWal());
        final long nameTxnBefore = dayNameTxn(tableName);

        try (TableWriter ownerWriter = engine.getWriter(token, "owner")) {
            Assert.assertTrue(engine.getPartitionCompactionRequestQueue().tryRequestParquetConversion(token, DAY));
            runJobOnAnotherThread();
            commitLateRows(ownerWriter, from);
            Assert.assertFalse(ownerWriter.inTransaction());
            // The queued swap runs against the day as the commit left it.
            ownerWriter.tick();
        }
        execute(lateRowsInsert(tableName + "_oracle", from));
        engine.releaseAllReaders();
        engine.releaseAllWriters();

        assertFolders(tableName, folderCount, isComposite);
        try (TableReader reader = engine.getReader(tableName)) {
            final TxReader tx = reader.getTxFile();
            Assert.assertFalse("a declined swap must leave the day native", tx.isPartitionParquet(tx.getPartitionIndex(DAY)));
        }
        // Every write here leaves the folder's name txn alone, so the swap's name txn check cannot be what declined it.
        Assert.assertEquals("the day's name txn", nameTxnBefore, dayNameTxn(tableName));
        assertNoStagingDirectory(token);
        TestUtils.assertSqlCursors(
                engine, sqlExecutionContext, "SELECT * FROM " + tableName + "_oracle", "SELECT * FROM " + tableName, LOG
        );
        // The next request builds off the day as the write left it.
        assertConversionMatchesOracle(tableName, isWal);
    }

    /**
     * Commits {@link #lateRowsInsert}'s rows through the held writer. A non-WAL writer takes them as rows; a WAL
     * table takes them as a WAL transaction, applied on the held writer the way {@code ApplyWal2TableJob} does.
     */
    private void commitLateRows(TableWriter ownerWriter, String from) throws Exception {
        final TableToken token = ownerWriter.getTableToken();
        if (!token.isWal()) {
            final long fromTs = parse(from);
            for (int k = 1; k <= LATE_ROWS; k++) {
                final TableWriter.Row row = ownerWriter.newRow(fromTs + (k - 1) * 5_000_000L);
                row.putInt(0, 80_000 + k);
                row.putSym(1, k % 2 == 0 ? "k1" : "k2");
                row.putLong(7, 2_000_000 + k);
                row.append();
            }
            ownerWriter.commit();
            return;
        }
        execute(lateRowsInsert(token.getTableName(), from));
        final long seqTxn = ownerWriter.getAppliedSeqTxn() + 1;
        try (TransactionLogCursor cursor = engine.getTableSequencerAPI().getCursor(token, ownerWriter.getAppliedSeqTxn())) {
            ownerWriter.readWalTxnDetails(cursor);
        }
        final WalTxnDetails details = ownerWriter.getWalTnxDetails();
        try (Path walPath = new Path()) {
            walPath.of(configuration.getDbRoot()).concat(token).slash()
                    .putAscii(WalUtils.WAL_NAME_BASE).put(details.getWalId(seqTxn)).slash().put(details.getSegmentId(seqTxn));
            ownerWriter.commitWalInsertTransactions(walPath, seqTxn, TableWriterPressureControl.EMPTY);
        }
        Assert.assertEquals("the late rows must be applied before the swap runs", seqTxn, ownerWriter.getAppliedSeqTxn());
    }

    /**
     * 2020-01-01 with 5760 rows, a column added part-way through so the day's folders carry a column top, every
     * kind of column the frames have to slice - fixed, symbol, var-size of both aux layouts, array - and an active
     * 2020-01-03.
     */
    private void createTable(String tableName, boolean isWal) throws Exception {
        execute("CREATE TABLE " + tableName + " (i INT, s SYMBOL INDEX, v VARCHAR, str STRING, d DOUBLE, arr DOUBLE[], ts TIMESTAMP)" +
                " TIMESTAMP(ts) PARTITION BY DAY " + (isWal ? "WAL" : "BYPASS WAL"));
        execute("INSERT INTO " + tableName + " SELECT x::INT i, rnd_symbol('k1', 'k2', NULL) s," +
                " rnd_varchar(1, 40, 1) v, rnd_str(1, 30, 1) str, rnd_double(2) d, rnd_double_array(1, 1) arr," +
                " timestamp_sequence('2020-01-01', 10 * 1_000_000L) ts FROM long_sequence(4000)");
        drainWalQueue();
        execute("ALTER TABLE " + tableName + " ADD COLUMN late LONG");
        drainWalQueue();
        execute("INSERT INTO " + tableName + " SELECT x::INT + 10_000 i, rnd_symbol('k1', 'k2', NULL) s," +
                " rnd_varchar(1, 40, 1) v, rnd_str(1, 30, 1) str, rnd_double(2) d, rnd_double_array(1, 1) arr," +
                " timestamp_sequence('2020-01-01T11:06:40', 25 * 1_000_000L) ts, x late FROM long_sequence(1760)");
        drainWalQueue();
        execute("INSERT INTO " + tableName + " (i, ts) SELECT x::INT + 90_000 i, timestamp_sequence('2020-01-03', 60 * 1_000_000L) ts" +
                " FROM long_sequence(50)");
        drainWalQueue();
    }

    private void insertBackfill(String tableName, String from) throws Exception {
        execute("INSERT INTO " + tableName + " SELECT x::INT + 70_000 i, rnd_symbol('k1', 'k2', NULL) s," +
                " rnd_varchar(1, 40, 1) v, rnd_str(1, 30, 1) str, rnd_double(2) d, rnd_double_array(1, 1) arr," +
                " timestamp_sequence('" + from + "', 5 * 1_000_000L) ts, x + 1_000_000 late FROM long_sequence(200)");
        drainWalQueue();
    }
}
