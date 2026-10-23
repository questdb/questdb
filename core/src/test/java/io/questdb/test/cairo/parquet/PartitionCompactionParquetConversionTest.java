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
import io.questdb.cairo.CairoException;
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
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.Misc;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.str.DirectUtf8StringZ;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A requested parquet conversion writes a whole logical partition (plain, composite or split) into one parquet file in
 * a single pass. Tests compare against a native copy taken before the conversion.
 */
public class PartitionCompactionParquetConversionTest extends AbstractCairoTest {
    private static final long DAY = parse("2020-01-01T00:00:00.000000Z");
    private static final int LATE_ROWS = 200;

    @Override
    @Before
    public void setUp() {
        super.setUp();
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_CHECK_INTERVAL, -1);
        // Not a divisor of any folder or piece size, so row groups and frames cut across each other.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 997);
    }

    @Test
    public void testAlterConversionThatFailsToLinkAnIndexLeavesThePlainDayNative() throws Exception {
        final AtomicBoolean isLinkFailing = new AtomicBoolean(true);
        final FilesFacade linkFailingFf = new TestFilesFacadeImpl() {
            @Override
            public int hardLink(LPSZ src, LPSZ hardLink) {
                if (isLinkFailing.get() && isDayIndexFile(src, "s")) {
                    return -1;
                }
                return super.hardLink(src, hardLink);
            }
        };
        assertMemoryLeak(linkFailingFf, () -> {
            createTable("lf", false);
            execute("CREATE TABLE lf_oracle AS (SELECT * FROM lf) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            final long nameTxnBefore = dayNameTxn("lf");
            try {
                execute("ALTER TABLE lf CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
                Assert.fail("the conversion must fail when an index file does not link");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "could not create hard link");
            }
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            try (TableReader reader = engine.getReader("lf")) {
                final TxReader tx = reader.getTxFile();
                Assert.assertFalse("a failed conversion must leave the day native", tx.isPartitionParquet(tx.getPartitionIndex(DAY)));
            }
            Assert.assertEquals("the day's name txn", nameTxnBefore, dayNameTxn("lf"));
            Assert.assertEquals("a failed conversion must remove its directory", 1, countDayDirectories(engine.verifyTableName("lf")));
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "SELECT * FROM lf_oracle", "SELECT * FROM lf", LOG);

            isLinkFailing.set(false);
            assertAlterConversionMatchesOracle("lf", false, "");
        });
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

    /**
     * A plain day keeps its row ids, so a no-top column's index is linked; columns with a top get theirs rebuilt.
     */
    @Test
    public void testAlterConvertsAPlainDayLinkingTheIndexesOfColumnsWithNoTop() throws Exception {
        final AtomicInteger noTopLinks = new AtomicInteger();
        final AtomicInteger topLinks = new AtomicInteger();
        final AtomicInteger absentLinks = new AtomicInteger();
        final FilesFacade linkCountingFf = new TestFilesFacadeImpl() {
            @Override
            public int hardLink(LPSZ src, LPSZ hardLink) {
                if (isDayIndexFile(src, "s")) {
                    noTopLinks.incrementAndGet();
                } else if (isDayIndexFile(src, "top_s")) {
                    topLinks.incrementAndGet();
                } else if (isDayIndexFile(src, "absent_s")) {
                    absentLinks.incrementAndGet();
                }
                return super.hardLink(src, hardLink);
            }
        };
        assertMemoryLeak(linkCountingFf, () -> {
            final String[] indexClauses = {
                    "INDEX",
                    "INDEX TYPE POSTING",
                    "INDEX TYPE POSTING DELTA",
                    "INDEX TYPE POSTING EF",
                    "INDEX TYPE POSTING INCLUDE (i, v)"
            };
            for (int t = 0; t < indexClauses.length; t++) {
                for (boolean isWal : new boolean[]{true, false}) {
                    final String tableName = "pl" + t + (isWal ? "w" : "n");
                    createTableWithIndexedColumnTops(tableName, isWal, indexClauses[t]);
                    execute("CREATE TABLE " + tableName + "_oracle AS (SELECT * FROM " + tableName + ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
                    noTopLinks.set(0);
                    topLinks.set(0);
                    absentLinks.set(0);
                    assertAlterConversionMatchesOracle(tableName, isWal, "");
                    Assert.assertTrue("the index of a column with no top must be linked [" + indexClauses[t] + ']', noTopLinks.get() > 0);
                    Assert.assertEquals("the index of a column with a top must be built [" + indexClauses[t] + ']', 0, topLinks.get());
                    Assert.assertEquals("the index of a column the day predates must be built [" + indexClauses[t] + ']', 0, absentLinks.get());
                    for (String filter : new String[]{
                            "top_s = 't1'", "top_s = 't2'", "top_s IS NULL", "absent_s = 'a1'", "absent_s IS NULL"
                    }) {
                        TestUtils.assertSqlCursors(
                                engine,
                                sqlExecutionContext,
                                "SELECT * FROM " + tableName + "_oracle WHERE " + filter,
                                "SELECT * FROM " + tableName + " WHERE " + filter,
                                LOG
                        );
                    }
                }
            }
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
     * After the conversion the writer must release the native files it had open on the last partition, else a later
     * ALTER COLUMN TYPE leaves them open over the parquet partition.
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
            execute("INSERT INTO aa SELECT x::INT + 70_000, rnd_symbol('k1', 'k2', NULL), x + 1_000_000," +
                    " timestamp_sequence('2020-01-01T11:06:07', 5 * 1_000_000L) FROM long_sequence(200)");
            drainWalQueue();
            assertFolders("aa", 2, false);

            execute("ALTER TABLE aa CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();
            assertFolders("aa", 1, false);
            execute("ALTER TABLE aa ALTER COLUMN late TYPE DOUBLE");
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
            insertBackfill("cs", "2020-01-01T22:00:07");
            assertFolders("cs", 2, false);
            assertConversion("cs", true);
        });
    }

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
     * An in-place append keeps the name txn; only the swap's row-count check catches it.
     */
    @Test
    public void testDiscardsTheSwapWhenANonWalWriterAppendsToTheDayBetweenBuildAndSwap() throws Exception {
        assertMemoryLeak(() -> {
            createTable("na", false);
            assertFolders("na", 1, false);
            assertWriteBetweenBuildAndSwapIsKept("na", false, "2020-01-01T23:30:00.000000Z", 1, false);
        });
    }

    @Test
    public void testDiscardsTheSwapWhenAWalTransactionAppendsToTheDayBetweenBuildAndSwap() throws Exception {
        assertMemoryLeak(() -> {
            createTable("wa", true);
            assertFolders("wa", 1, false);
            assertWriteBetweenBuildAndSwapIsKept("wa", true, "2020-01-01T23:30:00.000000Z", 1, false);
        });
    }

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

    private static int countDayDirectories(TableToken token) {
        final int[] count = new int[1];
        try (Path path = new Path()) {
            final DirectUtf8StringZ name = new DirectUtf8StringZ();
            final FilesFacade ff = engine.getConfiguration().getFilesFacade();
            ff.iterateDir(path.of(engine.getConfiguration().getDbRoot()).concat(token.getDirName()).$(), (pUtf8NameZ, _) -> {
                if (name.of(pUtf8NameZ).toString().startsWith("2020-01-01")) {
                    count[0]++;
                }
            });
        }
        return count[0];
    }

    private static long dayNameTxn(String tableName) {
        try (TableReader reader = engine.getReader(tableName)) {
            final TxReader tx = reader.getTxFile();
            return tx.getPartitionNameTxn(tx.getPartitionIndex(DAY));
        }
    }

    private static boolean isDayIndexFile(LPSZ file, String columnName) {
        final String filePath = Utf8s.stringFromUtf8Bytes(file);
        final int nameLo = filePath.lastIndexOf(Files.SEPARATOR) + 1;
        final String fileName = filePath.substring(nameLo);
        return filePath.substring(0, nameLo).contains(Files.SEPARATOR + "2020-01-01")
                && fileName.startsWith(columnName + '.')
                && !fileName.equals(columnName + ".d");
    }

    /**
     * Deterministic, so {@link #commitLateRows} can write the same rows without SQL.
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
     * Builds the conversion while holding the writer, commits late rows, then ticks the swap - the order the WAL apply
     * loop runs a swap queued mid-batch in. The swap must decline and leave the day native with every row.
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
        assertConversionMatchesOracle(tableName, isWal);
    }

    /**
     * For WAL, applies the transaction on the held writer the way {@code ApplyWal2TableJob} does.
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
     * 2020-01-01 with 5760 rows, a column top, every column kind the frames slice, and an active 2020-01-03.
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

    /**
     * Indexed symbols: {@code s} has no top, {@code top_s} a top in 2020-01-01, {@code absent_s} postdates 2020-01-01.
     */
    private void createTableWithIndexedColumnTops(String tableName, boolean isWal, String indexClause) throws Exception {
        execute("CREATE TABLE " + tableName + " (i INT, s SYMBOL " + indexClause + ", v VARCHAR, ts TIMESTAMP)" +
                " TIMESTAMP(ts) PARTITION BY DAY " + (isWal ? "WAL" : "BYPASS WAL"));
        execute("INSERT INTO " + tableName + " SELECT x::INT i, rnd_symbol('k1', 'k2', NULL) s, rnd_varchar(1, 40, 1) v," +
                " timestamp_sequence('2020-01-01', 10 * 1_000_000L) ts FROM long_sequence(4000)");
        drainWalQueue();
        final String addedIndexClause = indexClause.contains("INCLUDE") ? "INDEX TYPE POSTING" : indexClause;
        execute("ALTER TABLE " + tableName + " ADD COLUMN top_s SYMBOL " + addedIndexClause);
        drainWalQueue();
        execute("INSERT INTO " + tableName + " SELECT x::INT + 10_000 i, rnd_symbol('k1', 'k2', NULL) s, rnd_varchar(1, 40, 1) v," +
                " timestamp_sequence('2020-01-01T11:06:40', 25 * 1_000_000L) ts, rnd_symbol('t1', 't2', NULL) top_s" +
                " FROM long_sequence(1760)");
        drainWalQueue();
        execute("INSERT INTO " + tableName + " (i, ts) SELECT x::INT + 90_000 i, timestamp_sequence('2020-01-03', 60 * 1_000_000L) ts" +
                " FROM long_sequence(50)");
        drainWalQueue();
        execute("ALTER TABLE " + tableName + " ADD COLUMN absent_s SYMBOL " + addedIndexClause);
        drainWalQueue();
        execute("INSERT INTO " + tableName + " (i, ts, absent_s) SELECT x::INT + 95_000 i," +
                " timestamp_sequence('2020-01-03T01:00', 60 * 1_000_000L) ts, rnd_symbol('a1', 'a2', NULL) absent_s" +
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
