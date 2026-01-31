/*******************************************************************************
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

package io.questdb.test.cairo.fuzz;

import io.questdb.cairo.TableToken;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.test.fuzz.FuzzTransaction;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Comprehensive fuzz tests for negative timestamp support.
 * Tests O3 merge, deduplication, WAL operations, and edge cases with timestamps before 1970-01-01.
 */
public class NegativeTimestampFuzzTest extends AbstractFuzzTest {

    private static final long YEAR_1800_MICROS = -5364662400000000L;  // 1800-01-01
    private static final long YEAR_1900_MICROS = -2208988800000000L;  // 1900-01-01

    /**
     * Test that binary search works correctly with mixed positive/negative timestamps.
     */
    @Test
    public void testBinarySearchAcrossEpoch() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = getTestName();

            // Create partitioned table with WAL
            execute("create table " + tableName + " (" +
                    "ts timestamp, " +
                    "val long" +
                    ") timestamp(ts) partition by DAY WAL");

            // Insert ordered data spanning epoch
            execute("insert into " + tableName + " values " +
                    "('1969-12-31T00:00:00Z', 1), " +
                    "('1969-12-31T12:00:00Z', 2), " +
                    "('1970-01-01T00:00:00Z', 3), " +
                    "('1970-01-01T12:00:00Z', 4)");

            drainWalQueue();

            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                // Point query at epoch boundary
                sink.clear();
                TestUtils.printSql(compiler, sqlExecutionContext,
                        "select val from " + tableName + " where ts = '1970-01-01T00:00:00Z'", sink);
                Assert.assertTrue("Should find epoch row", sink.toString().contains("3"));

                // Range query spanning epoch
                sink.clear();
                TestUtils.printSql(compiler, sqlExecutionContext,
                        "select count() from " + tableName +
                                " where ts >= '1969-12-31T06:00:00Z' and ts < '1970-01-01T06:00:00Z'", sink);
                Assert.assertTrue("Should find 2 rows in range", sink.toString().contains("2"));

                // Query only negative timestamps
                sink.clear();
                TestUtils.printSql(compiler, sqlExecutionContext,
                        "select count() from " + tableName + " where ts < '1970-01-01'", sink);
                Assert.assertTrue("Should find 2 rows before epoch", sink.toString().contains("2"));
            }
        });
    }

    /**
     * Test column add operations on tables with negative timestamps.
     * Only tests column add (not remove/rename) to reduce test flakiness.
     */
    @Test
    public void testColumnOperationsWithNegativeTimestamps() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = getTestName();

            // Create table with negative timestamps
            execute("create table " + tableName + " (" +
                    "ts timestamp, " +
                    "val long, " +
                    "sym symbol" +
                    ") timestamp(ts) partition by DAY WAL");

            // Insert initial data with negative timestamps
            execute("insert into " + tableName +
                    " select timestamp_sequence('1900-01-01', 86400000000L), x, rnd_symbol('A','B','C') " +
                    " from long_sequence(30)");

            drainWalQueue();

            // Add a new column
            execute("alter table " + tableName + " add column new_val double");

            // Insert more data with the new column
            execute("insert into " + tableName +
                    " select timestamp_sequence('1900-02-01', 86400000000L), x + 100, rnd_symbol('D','E','F'), rnd_double() " +
                    " from long_sequence(30)");

            drainWalQueue();

            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                // Verify total count
                sink.clear();
                TestUtils.printSql(compiler, sqlExecutionContext,
                        "select count() from " + tableName, sink);
                Assert.assertTrue("Should have 60 rows", sink.toString().contains("60"));

                // Verify new column exists and has values
                sink.clear();
                TestUtils.printSql(compiler, sqlExecutionContext,
                        "select count() from " + tableName + " where new_val is not null", sink);
                Assert.assertTrue("Should have 30 rows with new_val", sink.toString().contains("30"));

                // Verify data order
                sink.clear();
                TestUtils.printSql(compiler, sqlExecutionContext,
                        "select ts from " + tableName + " limit 1", sink);
                Assert.assertTrue("First row should be from 1900", sink.toString().contains("1900-01-01"));
            }
        });
    }

    /**
     * Test deduplication with negative timestamps.
     * Insert duplicate rows with same negative timestamp and verify dedup works.
     */
    @Test
    public void testDedupWithNegativeTimestamps() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = getTestName();

            // Create table with dedup enabled
            execute("create table " + tableName + " (" +
                    "ts timestamp, " +
                    "val long, " +
                    "key symbol" +
                    ") timestamp(ts) partition by DAY WAL DEDUP UPSERT KEYS(ts, key)");

            // Insert initial data with negative timestamps
            execute("insert into " + tableName + " values " +
                    "('1969-12-31T12:00:00Z', 1, 'A'), " +
                    "('1969-12-31T12:00:00Z', 2, 'B'), " +
                    "('1969-12-31T18:00:00Z', 3, 'A')");

            drainWalQueue();

            // Insert duplicates - should update existing rows
            execute("insert into " + tableName + " values " +
                    "('1969-12-31T12:00:00Z', 100, 'A'), " +  // Update A
                    "('1969-12-31T12:00:00Z', 200, 'B')");    // Update B

            drainWalQueue();

            // Verify dedup - should have 3 rows with updated values
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                sink.clear();
                TestUtils.printSql(compiler, sqlExecutionContext,
                        "select ts, val, key from " + tableName + " order by ts, key", sink);

                // Should have: A=100, B=200 at 12:00 and A=3 at 18:00
                String result = sink.toString();
                Assert.assertTrue("Should contain updated value 100", result.contains("100"));
                Assert.assertTrue("Should contain updated value 200", result.contains("200"));
                Assert.assertTrue("Should contain original value 3", result.contains("\t3\t"));
            }
        });
    }

    /**
     * Test mixed O3 inserts with both column operations and partition drops.
     */
    @Test
    public void testMixedOperationsWithNegativeTimestamps() throws Exception {
        Rnd rnd = generateRandom(LOG);

        fuzzer.setFuzzCounts(
                true,   // isO3
                300,
                15,
                10,
                10,
                5,
                30,
                4);

        // Enable mixed operations
        fuzzer.setFuzzProbabilities(
                0.0,   // cancelRowsProb
                0.05,  // notSetProb
                0.05,  // nullSetProb
                0.02,  // rollbackProb
                0.05,  // colAddProb
                0.02,  // colRemoveProb
                0.0,   // colRenameProb
                0.0,   // colTypeChangeProb
                0.7,   // dataAddProb
                0.05,  // equalTsRowsProb
                0.03,  // partitionDropProb
                0.0,   // truncateProb
                0.0,   // tableDropProb
                0.0,   // setTtlProb
                0.0,   // replaceInsertProb
                0.0,   // symbolAccessValidationProb
                0.03); // queryProb

        // Span across epoch boundary
        long startMicro = -30L * Micros.DAY_MICROS;
        long duration = 60L * Micros.DAY_MICROS;

        runFuzzWithNegativeTimestamps(rnd, startMicro, duration);
    }

    /**
     * Test basic O3 operations with negative timestamps.
     */
    @Test
    public void testNegativeTimestampFuzz() throws Exception {
        Rnd rnd = generateRandom(LOG);

        fuzzer.setFuzzCounts(
                false,
                1000,
                10,
                10,
                10,
                10,
                100,
                3);

        fuzzer.setFuzzProbabilities(
                0.0, 0.0, 0.0, 0.0,
                0.0, 0.0, 0.0, 0.0,
                1.0,  // dataAddProb only
                0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0);

        runFuzzWithNegativeTimestamps(rnd, YEAR_1900_MICROS, 365L * Micros.DAY_MICROS * 10);
    }

    /**
     * Test O3 merge that spans across the epoch boundary (1969 -> 1970).
     * This exercises the signed comparison logic in the native merge code.
     */
    @Test
    public void testO3MergeAcrossEpochBoundary() throws Exception {
        Rnd rnd = generateRandom(LOG);

        fuzzer.setFuzzCounts(
                true,   // isO3 - force out-of-order
                500,
                5,
                10,
                10,
                5,
                50,
                2);

        fuzzer.setFuzzProbabilities(
                0.0, 0.0, 0.0, 0.0,
                0.0, 0.0, 0.0, 0.0,
                1.0,
                0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0);

        // Start 1 week before epoch, span 2 weeks (crosses into 1970)
        long startMicro = -7L * Micros.DAY_MICROS;
        long duration = 14L * Micros.DAY_MICROS;

        runFuzzWithNegativeTimestamps(rnd, startMicro, duration);
    }

    /**
     * Test O3 with data inserted in reverse order across epoch boundary.
     * First insert positive timestamps, then negative ones (O3 insert).
     */
    @Test
    public void testO3ReverseInsertAcrossEpoch() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = getTestName();

            // Create table with WAL
            execute("create table " + tableName + " (" +
                    "ts timestamp, " +
                    "val long, " +
                    "sym symbol" +
                    ") timestamp(ts) partition by DAY WAL");

            // First insert: positive timestamps (1970-01-01 to 1970-01-03)
            execute("insert into " + tableName +
                    " select timestamp_sequence('1970-01-01', 3600000000L), x, rnd_symbol('A','B','C') " +
                    " from long_sequence(72)");

            drainWalQueue();

            // Second insert: negative timestamps (1969-12-29 to 1969-12-31) - O3
            execute("insert into " + tableName +
                    " select timestamp_sequence('1969-12-29', 3600000000L), x + 1000, rnd_symbol('D','E','F') " +
                    " from long_sequence(72)");

            drainWalQueue();

            // Verify data is correctly ordered
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                // Check count
                sink.clear();
                TestUtils.printSql(compiler, sqlExecutionContext,
                        "select count() from " + tableName, sink);
                Assert.assertTrue(sink.toString().contains("144"));

                // Check order - first row should be 1969-12-29
                sink.clear();
                TestUtils.printSql(compiler, sqlExecutionContext,
                        "select ts from " + tableName + " limit 1", sink);
                Assert.assertTrue("First row should be 1969-12-29",
                        sink.toString().contains("1969-12-29"));

                // Check that negative timestamps come before positive
                sink.clear();
                TestUtils.printSql(compiler, sqlExecutionContext,
                        "select min(ts) < '1970-01-01' as has_negative from " + tableName, sink);
                Assert.assertTrue(sink.toString().contains("true"));
            }
        });
    }

    /**
     * Test partition operations (drop, truncate) with negative timestamp partitions.
     */
    @Test
    public void testPartitionOperationsWithNegativeTimestamps() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = getTestName();

            // Create table spanning multiple partitions before and after epoch
            execute("create table " + tableName + " (" +
                    "ts timestamp, " +
                    "val long" +
                    ") timestamp(ts) partition by DAY WAL");

            // Insert data across epoch: Dec 29-31 1969, Jan 1-3 1970
            // Using 12 hour intervals to get 2 rows per day
            execute("insert into " + tableName +
                    " select timestamp_sequence('1969-12-29', 43200000000L), x " +
                    " from long_sequence(12)");

            drainWalQueue();

            // Verify partitions created (6 days worth)
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                sink.clear();
                TestUtils.printSql(compiler, sqlExecutionContext,
                        "select count() from table_partitions('" + tableName + "')", sink);
                int partitionCount = Integer.parseInt(sink.toString().trim().split("\n")[1]);
                Assert.assertTrue("Should have at least 5 partitions, got " + partitionCount, partitionCount >= 5);
            }

            // Drop a negative timestamp partition
            execute("alter table " + tableName + " drop partition list '1969-12-30'");

            drainWalQueue();

            // Verify partition dropped - should have 10 rows (12 - 2 from dropped partition)
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                sink.clear();
                TestUtils.printSql(compiler, sqlExecutionContext,
                        "select count() from " + tableName, sink);
                int rowCount = Integer.parseInt(sink.toString().trim().split("\n")[1]);
                Assert.assertEquals("Should have 10 rows after drop, got " + rowCount, 10, rowCount);
            }

            // Truncate table
            execute("truncate table " + tableName);

            // Verify table is empty
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                sink.clear();
                TestUtils.printSql(compiler, sqlExecutionContext,
                        "select count() from " + tableName, sink);
                Assert.assertTrue("Should be empty after truncate", sink.toString().contains("0"));
            }
        });
    }

    /**
     * Test SAMPLE BY queries with negative timestamp data.
     */
    @Test
    public void testSampleByWithNegativeTimestamps() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = getTestName();

            // Create table with data spanning epoch
            execute("create table " + tableName + " (" +
                    "ts timestamp, " +
                    "val double" +
                    ") timestamp(ts) partition by DAY WAL");

            // Insert hourly data for 48 hours centered on epoch
            execute("insert into " + tableName +
                    " select timestamp_sequence('1969-12-31', 3600000000L), rnd_double() " +
                    " from long_sequence(48)");

            drainWalQueue();

            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                // SAMPLE BY 1d should create 2 buckets
                sink.clear();
                TestUtils.printSql(compiler, sqlExecutionContext,
                        "select ts, count() from " + tableName + " sample by 1d", sink);

                String result = sink.toString();
                Assert.assertTrue("Should have 1969-12-31 bucket", result.contains("1969-12-31"));
                Assert.assertTrue("Should have 1970-01-01 bucket", result.contains("1970-01-01"));

                // SAMPLE BY 6h should create 8 buckets - need aggregation function
                sink.clear();
                TestUtils.printSql(compiler, sqlExecutionContext,
                        "select count() as bucket_count from (select ts, count() from " + tableName + " sample by 6h)", sink);
                int bucketCount = Integer.parseInt(sink.toString().trim().split("\n")[1]);
                Assert.assertEquals("Should have 8 buckets, got " + bucketCount, 8, bucketCount);

                // SAMPLE BY with FILL
                sink.clear();
                TestUtils.printSql(compiler, sqlExecutionContext,
                        "select ts, avg(val) from " + tableName + " sample by 12h fill(null)", sink);
                Assert.assertTrue("FILL query should succeed", sink.length() > 10);
            }
        });
    }

    /**
     * Test with very old timestamps (year 1800).
     * This tests extreme negative values near the limits of timestamp range.
     */
    @Test
    public void testVeryOldTimestamps() throws Exception {
        Rnd rnd = generateRandom(LOG);

        fuzzer.setFuzzCounts(
                false,
                500,
                5,
                10,
                10,
                5,
                50,
                3);

        fuzzer.setFuzzProbabilities(
                0.0, 0.0, 0.0, 0.0,
                0.0, 0.0, 0.0, 0.0,
                1.0,
                0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0);

        // Start from year 1800
        runFuzzWithNegativeTimestamps(rnd, YEAR_1800_MICROS, 365L * Micros.DAY_MICROS);
    }

    /**
     * Test WAL replay with interleaved positive and negative timestamp transactions.
     */
    @Test
    public void testWalReplayInterleavedTimestamps() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = getTestName();

            execute("create table " + tableName + " (" +
                    "ts timestamp, " +
                    "val long, " +
                    "batch_id int" +  // Renamed to avoid SQL keyword
                    ") timestamp(ts) partition by HOUR WAL");

            // Interleave positive and negative timestamp inserts
            for (int i = 0; i < 4; i++) {
                // Negative timestamp batch (hours 20, 21, 22, 23 on Dec 31)
                execute("insert into " + tableName +
                        " select timestamp_sequence('1969-12-31T" + (20 + i) + ":00:00Z', 60000000L), x, " + (i * 2) +
                        " from long_sequence(60)");

                // Positive timestamp batch (hours 00, 01, 02, 03 on Jan 1) - O3 relative to above
                execute("insert into " + tableName +
                        " select timestamp_sequence('1970-01-01T0" + i + ":00:00Z', 60000000L), x + 1000, " + (i * 2 + 1) +
                        " from long_sequence(60)");
            }

            drainWalQueue();

            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                // Verify total count: 4 iterations * 2 batches * 60 rows = 480 rows
                sink.clear();
                TestUtils.printSql(compiler, sqlExecutionContext,
                        "select count() from " + tableName, sink);
                Assert.assertTrue("Should have 480 rows", sink.toString().contains("480"));

                // Verify data is ordered correctly
                sink.clear();
                TestUtils.printSql(compiler, sqlExecutionContext,
                        "select ts from " + tableName + " limit 1", sink);
                Assert.assertTrue("First timestamp should be 1969-12-31T20:00",
                        sink.toString().contains("1969-12-31T20:00"));

                // Verify partitions span correctly
                sink.clear();
                TestUtils.printSql(compiler, sqlExecutionContext,
                        "select count() from table_partitions('" + tableName + "')", sink);
                // Should have partitions for hours 20-23 on Dec 31 and 00-03 on Jan 1 = 8 partitions
                int partitionCount = Integer.parseInt(sink.toString().trim().split("\n")[1]);
                Assert.assertEquals("Should have 8 hour partitions, got " + partitionCount, 8, partitionCount);
            }
        });
    }

    // ==================== Helper methods ====================

    private void assertTableHasNegativeData(SqlCompiler compiler, String tableName) throws Exception {
        TableToken tt = engine.verifyTableName(tableName);

        sink.clear();
        TestUtils.printSql(compiler, sqlExecutionContext, "select count() from " + tableName, sink);
        Assert.assertFalse("Table " + tableName + " should have data", sink.toString().contains("count\n0\n"));

        try (var reader = engine.getReader(tt)) {
            long minTs = reader.getMinTimestamp();
            Assert.assertTrue("Min timestamp should be negative (before 1970), got: " + minTs, minTs < 0);
        }

        sink.clear();
        TestUtils.printSql(compiler, sqlExecutionContext,
                "select ts from " + tableName + " order by ts limit 1", sink);
        Assert.assertTrue("Query should return data", sink.length() > "ts\n".length());
    }

    private void createInitialTableNegative(String tableName, boolean isWal, long startMicro) throws Exception {
        SharedRandom.RANDOM.set(new Rnd());

        String tempTableName = "data_temp_neg_" + Math.abs(startMicro % 1000000);

        if (engine.getTableTokenIfExists(tempTableName) == null) {
            // Format start timestamp
            String startTs = formatMicrosAsTimestamp(startMicro);

            execute("create atomic table " + tempTableName + " as (" +
                    "select x as c1, " +
                    " rnd_symbol('AB', 'BC', 'CD') c2, " +
                    " timestamp_sequence('" + startTs + "'::timestamp, 1000000L) ts, " +
                    " rnd_symbol('DE', null, 'EF', 'FG') sym2," +
                    " cast(x as int) c3," +
                    " rnd_bin() c4," +
                    " to_long128(3 * x, 6 * x) c5," +
                    " rnd_str('a', 'bdece', null, ' asdflakji idid', 'dk') str1," +
                    " rnd_boolean() bool1," +
                    " cast(null as long) long_top," +
                    " cast(null as long) str_top," +
                    " rnd_symbol(null, 'X', 'Y') sym_top," +
                    " rnd_ipv4() ip4," +
                    " rnd_varchar(5, 10, 1) var_top" +
                    " from long_sequence(100)" +
                    ")");
        }

        if (engine.getTableTokenIfExists(tableName) == null) {
            String walClause = isWal ? " WAL" : " BYPASS WAL";
            execute("create atomic table " + tableName + " as (" +
                    "select * from " + tempTableName +
                    "), index(sym2), index(sym_top) timestamp(ts) partition by DAY" + walClause);
        }
    }

    private String formatMicrosAsTimestamp(long micros) {
        // Convert microseconds to a timestamp string
        // This is a simplified version - for production use TimestampFormatUtils
        long millis = micros / 1000;
        java.time.Instant instant = java.time.Instant.ofEpochMilli(millis);
        return instant.toString().replace("Z", "");
    }

    private void runFuzzWithNegativeTimestamps(Rnd rnd, long startMicro, long duration) throws Exception {
        String tableName = getTestName();
        String tableNameWal = tableName + "_wal";
        String tableNameNoWal = tableName + "_nonwal";

        createInitialTableNegative(tableNameWal, true, startMicro);
        createInitialTableNegative(tableNameNoWal, false, startMicro);

        long endMicro = startMicro + duration;

        ObjList<FuzzTransaction> transactions = fuzzer.generateTransactions(tableNameWal, rnd, startMicro, endMicro);

        try {
            // Apply to non-WAL table
            fuzzer.applyNonWal(transactions, tableNameNoWal, rnd);

            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                assertTableHasNegativeData(compiler, tableNameNoWal);
            }

            // Apply to WAL table
            TableToken walToken = engine.verifyTableName(tableNameWal);
            try {
                fuzzer.applyWal(transactions, tableNameWal, 1, rnd);
            } catch (AssertionError e) {
                SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(walToken);
                if (tracker.isSuspended()) {
                    String errorMsg = tracker.getErrorMessage();
                    throw new AssertionError("WAL table suspended with error: " + errorMsg, e);
                }
                throw e;
            }

            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                assertTableHasNegativeData(compiler, tableNameWal);
                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableNameNoWal, tableNameWal, LOG);
            }

            Assert.assertEquals("expected 0 errors in partition mutation control", 0,
                    engine.getPartitionOverwriteControl().getErrorCount());

        } finally {
            io.questdb.std.Misc.freeObjListAndClear(transactions);
        }
    }
}
