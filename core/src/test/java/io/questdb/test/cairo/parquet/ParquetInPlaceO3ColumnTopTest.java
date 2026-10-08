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
import io.questdb.cairo.ColumnVersionReader;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * An in-place parquet O3 merge rewrites every column's file column_top to 0
 * (update.rs end()), so the table's _cv must record top 0 for every live column
 * of that partition too. These tests add a column before CONVERT TO PARQUET,
 * which leaves _cv with no record (-1) or a top equal to the row count, then
 * merge O3 rows in place and compare against a native twin.
 */
public class ParquetInPlaceO3ColumnTopTest extends AbstractCairoTest {

    @Override
    @Before
    public void setUp() {
        // super.setUp() resets per-node cairo state, so set overrides after it.
        super.setUp();
        // 12 rows per day at 2h spacing -> 3 row groups of 4 per parquet partition.
        // Disable the dead-bytes rewrite triggers so O3 merges run in place.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_RATIO, "1.0");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, Long.MAX_VALUE);
    }

    @Test
    public void testColumnTopsZeroAfterInPlacePublish() throws Exception {
        assertMemoryLeak(() -> {
            createTwins("INT", false);
            convertToParquet("2024-01-03");
            long txnBefore = partitionNameTxn(0);
            executeBoth("INSERT INTO %s (id, ts, extra) VALUES (1000, '2024-01-01T09:00', 2000)");
            Assert.assertEquals(txnBefore, partitionNameTxn(0));
            try (TableReader reader = engine.getReader("pq")) {
                final ColumnVersionReader cv = reader.getColumnVersionReader();
                final TableReaderMetadata metadata = reader.getMetadata();
                final long partitionTs = reader.getTxFile().getPartitionTimestampByIndex(0);
                for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
                    final int writerIndex = metadata.getWriterIndex(i);
                    Assert.assertEquals(
                            "column top of " + metadata.getColumnName(i),
                            0,
                            cv.getColumnTop(partitionTs, writerIndex)
                    );
                }
            }
            assertSqlCursors("nat", "pq");
        });
    }

    @Test
    public void testDedupUpsertOnColumnAddedBeforeConvert() throws Exception {
        assertMemoryLeak(() -> {
            createTwins("INT", true);
            executeBoth("ALTER TABLE %s DEDUP ENABLE UPSERT KEYS(ts, extra)");
            convertToParquet("2024-01-04");
            long txnBefore = partitionNameTxn(0);
            executeBoth("INSERT INTO %s (id, ts, extra) VALUES (1000, '2024-01-01T09:00', 2000)");
            executeBoth("INSERT INTO %s (id, ts, extra) VALUES (2000, '2024-01-01T09:00', 2000)");
            Assert.assertEquals(txnBefore, partitionNameTxn(0));
            assertSqlCursors("nat", "pq");
            assertQuery("SELECT id, ts, extra FROM pq WHERE ts IN '2024-01-01T09'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            id\tts\textra
                            2000\t2024-01-01T09:00:00.000000Z\t2000
                            """);
        });
    }

    @Test
    public void testPlainO3AfterColumnAddedBeforeConvert() throws Exception {
        assertMemoryLeak(() -> {
            createTwins("INT", false);
            convertToParquet("2024-01-03");
            long txnBefore = partitionNameTxn(0);
            executeBoth("INSERT INTO %s (id, ts, extra) VALUES (1000, '2024-01-01T09:00', 2000)");
            Assert.assertEquals(txnBefore, partitionNameTxn(0));
            assertSqlCursors("nat", "pq");
            convertToNative("2024-01-03");
            assertSqlCursors("nat", "pq");
            assertQuery("SELECT id, ts, extra FROM pq WHERE id = 1000")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            id\tts\textra
                            1000\t2024-01-01T09:00:00.000000Z\t2000
                            """);
        });
    }

    @Test
    public void testPlainO3OnPartitionWithColumnTopEqualRowCount() throws Exception {
        assertMemoryLeak(() -> {
            // 2024-01-03 was the last partition at ADD COLUMN time, so its _cv top
            // equals its row count; a row at 2024-01-04 makes it non-last.
            createTwins("INT", true);
            convertToParquet("2024-01-04");
            long txnBefore = partitionNameTxn(2);
            executeBoth("INSERT INTO %s (id, ts, extra) VALUES (1000, '2024-01-03T09:00', 2000)");
            Assert.assertEquals(txnBefore, partitionNameTxn(2));
            assertSqlCursors("nat", "pq");
            convertToNative("2024-01-04");
            assertSqlCursors("nat", "pq");
            assertQuery("SELECT id, ts, extra FROM pq WHERE id = 1000")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            id\tts\textra
                            1000\t2024-01-03T09:00:00.000000Z\t2000
                            """);
        });
    }

    @Test
    public void testSymbolIndexOnColumnAddedBeforeConvert() throws Exception {
        assertMemoryLeak(() -> {
            createTwins("SYMBOL INDEX", true);
            convertToParquet("2024-01-04");
            final long txn0 = partitionNameTxn(0);
            final long txn1 = partitionNameTxn(1);
            final long txn2 = partitionNameTxn(2);
            // 'x' lands in every partition: in place into each parquet partition,
            // appended to the native 2024-01-04
            executeBoth("""
                    INSERT INTO %s (id, ts, extra) VALUES
                    (1000, '2024-01-01T09:00', 'x'),
                    (1001, '2024-01-02T09:00', 'x'),
                    (1002, '2024-01-03T09:00', 'x'),
                    (1003, '2024-01-04T02:00', 'x')
                    """);
            Assert.assertEquals(txn0, partitionNameTxn(0));
            Assert.assertEquals(txn1, partitionNameTxn(1));
            Assert.assertEquals(txn2, partitionNameTxn(2));
            assertSymbolLookups();
            convertToNative("2024-01-04");
            assertSymbolLookups();
        });
    }

    private static void convertToNative(String before) throws Exception {
        execute("ALTER TABLE pq CONVERT PARTITION TO NATIVE WHERE ts < '" + before + "'");
        drainWalQueue();
    }

    private static void convertToParquet(String before) throws Exception {
        execute("ALTER TABLE pq CONVERT PARTITION TO PARQUET WHERE ts < '" + before + "'");
        drainWalQueue();
    }

    /**
     * Creates nat and pq with 36 rows at 2h spacing over 3 daily partitions, then
     * adds {@code extra} of the given type. With extraPartition, both tables also get
     * a row at 2024-01-04, so 2024-01-03 (last at ADD COLUMN time, _cv top equal to
     * its row count) becomes non-last and convertible.
     */
    private static void createTwins(String extraType, boolean extraPartition) throws Exception {
        executeBoth("CREATE TABLE %s (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        executeBoth("INSERT INTO %s SELECT x::INT, '2024-01-01'::TIMESTAMP + (x - 1) * 7_200_000_000L FROM long_sequence(36)");
        executeBoth("ALTER TABLE %s ADD COLUMN extra " + extraType);
        if (extraPartition) {
            executeBoth("INSERT INTO %s (id, ts) VALUES (99, '2024-01-04T01:00')");
        }
    }

    private static void executeBoth(String sqlTemplate) throws Exception {
        execute(String.format(sqlTemplate, "nat"));
        execute(String.format(sqlTemplate, "pq"));
        drainWalQueue();
    }

    private static long partitionNameTxn(int partitionIndex) {
        try (TableReader reader = engine.getReader("pq")) {
            return reader.getTxFile().getPartitionNameTxn(partitionIndex);
        }
    }

    private void assertSymbolLookups() throws Exception {
        assertSqlCursors("SELECT * FROM nat WHERE extra = 'x'", "SELECT * FROM pq WHERE extra = 'x'");
        assertSqlCursors(
                "SELECT * FROM nat WHERE ts < '2024-01-04' LATEST ON ts PARTITION BY extra",
                "SELECT * FROM pq WHERE ts < '2024-01-04' LATEST ON ts PARTITION BY extra"
        );
        assertQuery("SELECT id, ts, extra FROM pq WHERE extra = 'x'")
                .noLeakCheck()
                .timestamp("ts")
                .returns("""
                        id\tts\textra
                        1000\t2024-01-01T09:00:00.000000Z\tx
                        1001\t2024-01-02T09:00:00.000000Z\tx
                        1002\t2024-01-03T09:00:00.000000Z\tx
                        1003\t2024-01-04T02:00:00.000000Z\tx
                        """);
        // the ts filter keeps the latest 'x' inside the parquet partitions
        assertQuery("SELECT id, ts, extra FROM pq WHERE ts < '2024-01-04' LATEST ON ts PARTITION BY extra")
                .noLeakCheck()
                .expectSize()
                .timestamp("ts")
                .returns("""
                        id\tts\textra
                        1002\t2024-01-03T09:00:00.000000Z\tx
                        36\t2024-01-03T22:00:00.000000Z\t
                        """);
    }
}
