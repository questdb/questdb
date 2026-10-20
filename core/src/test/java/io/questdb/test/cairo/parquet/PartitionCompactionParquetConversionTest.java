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
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TxReader;
import io.questdb.std.Misc;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
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
 * native copy taken before the conversion, and reads it through its symbol index too.
 */
public class PartitionCompactionParquetConversionTest extends AbstractCairoTest {
    private static final long DAY = parse("2020-01-01T00:00:00.000000Z");

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

    private void assertConversion(String tableName, boolean isWal) throws Exception {
        execute("CREATE TABLE " + tableName + "_oracle AS (SELECT * FROM " + tableName + ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
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
     * 2020-01-01 with 5760 rows, a column added part-way through so the day's folders carry a column top, every
     * kind of column the frames have to slice - fixed, symbol, var-size of both aux layouts, array - and an active
     * 2020-01-03.
     */
    private void createTable(String tableName, boolean isWal) throws Exception {
        execute("CREATE TABLE " + tableName + " (i INT, s SYMBOL INDEX, v VARCHAR, str STRING, d DOUBLE, arr DOUBLE[], ts TIMESTAMP)" +
                " TIMESTAMP(ts) PARTITION BY DAY " + (isWal ? "WAL" : "BYPASS WAL"));
        execute("INSERT INTO " + tableName + " SELECT x::INT i, rnd_symbol('k1', 'k2', NULL) s," +
                " rnd_varchar(1, 40, 1) v, rnd_str(1, 30, 1) str, rnd_double(2) d, rnd_double_array(1, 1) arr," +
                " timestamp_sequence('2020-01-01', 10 * 1000000L) ts FROM long_sequence(4000)");
        drainWalQueue();
        execute("ALTER TABLE " + tableName + " ADD COLUMN late LONG");
        drainWalQueue();
        execute("INSERT INTO " + tableName + " SELECT x::INT + 10_000 i, rnd_symbol('k1', 'k2', NULL) s," +
                " rnd_varchar(1, 40, 1) v, rnd_str(1, 30, 1) str, rnd_double(2) d, rnd_double_array(1, 1) arr," +
                " timestamp_sequence('2020-01-01T11:06:40', 25 * 1000000L) ts, x late FROM long_sequence(1760)");
        drainWalQueue();
        execute("INSERT INTO " + tableName + " (i, ts) SELECT x::INT + 90_000 i, timestamp_sequence('2020-01-03', 60 * 1000000L) ts" +
                " FROM long_sequence(50)");
        drainWalQueue();
    }

    private void insertBackfill(String tableName, String from) throws Exception {
        execute("INSERT INTO " + tableName + " SELECT x::INT + 70_000 i, rnd_symbol('k1', 'k2', NULL) s," +
                " rnd_varchar(1, 40, 1) v, rnd_str(1, 30, 1) str, rnd_double(2) d, rnd_double_array(1, 1) arr," +
                " timestamp_sequence('" + from + "', 5 * 1000000L) ts, x + 1_000_000 late FROM long_sequence(200)");
        drainWalQueue();
    }
}
