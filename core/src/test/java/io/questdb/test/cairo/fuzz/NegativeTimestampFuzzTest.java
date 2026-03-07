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
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.str.StringSink;
import io.questdb.test.fuzz.FuzzTransaction;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Fuzz tests for negative timestamp support.
 * Tests O3 merge, deduplication, WAL operations, and edge cases with timestamps before 1970-01-01.
 */
public class NegativeTimestampFuzzTest extends AbstractFuzzTest {

    private static final long YEAR_1800_MICROS = -5_364_662_400_000_000L;  // 1800-01-01
    private static final long YEAR_1900_MICROS = -2_208_988_800_000_000L;  // 1900-01-01

    @Test
    public void testBinarySearchAcrossEpoch() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = getTestName();

            execute("CREATE TABLE " + tableName + " (" +
                    "ts TIMESTAMP, " +
                    "val LONG" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            execute("INSERT INTO " + tableName + " VALUES " +
                    "('1969-12-31T00:00:00Z', 1), " +
                    "('1969-12-31T12:00:00Z', 2), " +
                    "('1970-01-01T00:00:00Z', 3), " +
                    "('1970-01-01T12:00:00Z', 4)");

            drainWalQueue();

            // Point query at epoch boundary
            assertSql("val\n3\n",
                    "SELECT val FROM " + tableName + " WHERE ts = '1970-01-01T00:00:00Z'");

            // Range query spanning epoch
            assertSql("count\n2\n",
                    "SELECT count() FROM " + tableName +
                            " WHERE ts >= '1969-12-31T06:00:00Z' AND ts < '1970-01-01T06:00:00Z'");

            // Query only negative timestamps
            assertSql("count\n2\n",
                    "SELECT count() FROM " + tableName + " WHERE ts < '1970-01-01'");
        });
    }

    @Test
    public void testColumnOperationsWithNegativeTimestamps() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = getTestName();

            execute("CREATE TABLE " + tableName + " (" +
                    "ts TIMESTAMP, " +
                    "val LONG, " +
                    "sym SYMBOL" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            execute("INSERT INTO " + tableName +
                    " SELECT timestamp_sequence('1900-01-01', 86_400_000_000L), x, rnd_symbol('A','B','C')" +
                    " FROM long_sequence(30)");

            drainWalQueue();

            execute("ALTER TABLE " + tableName + " ADD COLUMN new_val DOUBLE");

            execute("INSERT INTO " + tableName +
                    " SELECT timestamp_sequence('1900-02-01', 86_400_000_000L), x + 100, rnd_symbol('D','E','F'), rnd_double()" +
                    " FROM long_sequence(30)");

            drainWalQueue();

            assertSql("count\n60\n", "SELECT count() FROM " + tableName);
            assertSql("count\n30\n", "SELECT count() FROM " + tableName + " WHERE new_val IS NOT NULL");
            assertSql("ts\n1900-01-01T00:00:00.000000Z\n", "SELECT ts FROM " + tableName + " LIMIT 1");
        });
    }

    @Test
    public void testDedupWithNegativeTimestamps() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = getTestName();

            execute("CREATE TABLE " + tableName + " (" +
                    "ts TIMESTAMP, " +
                    "val LONG, " +
                    "key SYMBOL" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, key)");

            execute("INSERT INTO " + tableName + " VALUES " +
                    "('1969-12-31T12:00:00Z', 1, 'A'), " +
                    "('1969-12-31T12:00:00Z', 2, 'B'), " +
                    "('1969-12-31T18:00:00Z', 3, 'A')");

            drainWalQueue();

            // Upsert — should update A and B at 12:00, leave A at 18:00 unchanged
            execute("INSERT INTO " + tableName + " VALUES " +
                    "('1969-12-31T12:00:00Z', 100, 'A'), " +
                    "('1969-12-31T12:00:00Z', 200, 'B')");

            drainWalQueue();

            assertSql("""
                    ts\tval\tkey
                    1969-12-31T12:00:00.000000Z\t100\tA
                    1969-12-31T12:00:00.000000Z\t200\tB
                    1969-12-31T18:00:00.000000Z\t3\tA
                    """, "SELECT ts, val, key FROM " + tableName + " ORDER BY ts, key");
        });
    }

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

    @Test
    public void testO3MergeAcrossEpochBoundary() throws Exception {
        Rnd rnd = generateRandom(LOG);

        fuzzer.setFuzzCounts(
                true,   // isO3 — force out-of-order
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

    @Test
    public void testO3ReverseInsertAcrossEpoch() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = getTestName();

            execute("CREATE TABLE " + tableName + " (" +
                    "ts TIMESTAMP, " +
                    "val LONG, " +
                    "sym SYMBOL" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            // First insert: positive timestamps (1970-01-01 to 1970-01-03)
            execute("INSERT INTO " + tableName +
                    " SELECT timestamp_sequence('1970-01-01', 3_600_000_000L), x, rnd_symbol('A','B','C')" +
                    " FROM long_sequence(72)");

            drainWalQueue();

            // Second insert: negative timestamps (1969-12-29 to 1969-12-31) — O3
            execute("INSERT INTO " + tableName +
                    " SELECT timestamp_sequence('1969-12-29', 3_600_000_000L), x + 1000, rnd_symbol('D','E','F')" +
                    " FROM long_sequence(72)");

            drainWalQueue();

            assertSql("count\n144\n", "SELECT count() FROM " + tableName);
            assertSql("ts\n1969-12-29T00:00:00.000000Z\n", "SELECT ts FROM " + tableName + " LIMIT 1");
            assertSql("has_negative\ntrue\n",
                    "SELECT min(ts) < '1970-01-01' AS has_negative FROM " + tableName);
        });
    }

    @Test
    public void testPartitionOperationsWithNegativeTimestamps() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = getTestName();

            // Insert 12 rows at 12h intervals from 1969-12-29 — creates 6 day partitions
            execute("CREATE TABLE " + tableName + " (" +
                    "ts TIMESTAMP, " +
                    "val LONG" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            execute("INSERT INTO " + tableName +
                    " SELECT timestamp_sequence('1969-12-29', 43_200_000_000L), x" +
                    " FROM long_sequence(12)");

            drainWalQueue();

            // 6 days: Dec 29, Dec 30, Dec 31, Jan 1, Jan 2, Jan 3
            assertSql("count\n6\n",
                    "SELECT count() FROM table_partitions('" + tableName + "')");

            // Drop a negative-timestamp partition
            execute("ALTER TABLE " + tableName + " DROP PARTITION LIST '1969-12-30'");

            drainWalQueue();

            assertSql("count\n10\n", "SELECT count() FROM " + tableName);

            execute("TRUNCATE TABLE " + tableName);
            drainWalQueue();

            assertSql("count\n0\n", "SELECT count() FROM " + tableName);
        });
    }

    @Test
    public void testSampleByWithNegativeTimestamps() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = getTestName();

            // 48 hourly rows: Dec 31 00:00–23:00 and Jan 1 00:00–23:00
            execute("CREATE TABLE " + tableName + " (" +
                    "ts TIMESTAMP, " +
                    "val DOUBLE" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            execute("INSERT INTO " + tableName +
                    " SELECT timestamp_sequence('1969-12-31', 3_600_000_000L), rnd_double()" +
                    " FROM long_sequence(48)");

            drainWalQueue();

            // SAMPLE BY 1d: 2 buckets (24 rows each)
            assertSql("""
                    ts\tcount
                    1969-12-31T00:00:00.000000Z\t24
                    1970-01-01T00:00:00.000000Z\t24
                    """, "SELECT ts, count() FROM " + tableName + " SAMPLE BY 1d");

            // SAMPLE BY 6h: 8 buckets
            assertSql("bucket_count\n8\n",
                    "SELECT count() AS bucket_count FROM (" +
                            "SELECT ts, count() FROM " + tableName + " SAMPLE BY 6h)");

            // SAMPLE BY with FILL(NULL): 4 buckets at 12h boundaries
            assertSql("count\n4\n",
                    "SELECT count() FROM (" +
                            "SELECT ts, avg(val) FROM " + tableName + " SAMPLE BY 12h FILL(NULL))");
        });
    }

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

    @Test
    public void testWalReplayInterleavedTimestamps() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = getTestName();

            execute("CREATE TABLE " + tableName + " (" +
                    "ts TIMESTAMP, " +
                    "val LONG, " +
                    "batch_id INT" +
                    ") TIMESTAMP(ts) PARTITION BY HOUR WAL");

            // Interleave negative (Dec 31 hours 20–23) and positive (Jan 1 hours 00–03) inserts
            for (int i = 0; i < 4; i++) {
                execute("INSERT INTO " + tableName +
                        " SELECT timestamp_sequence('1969-12-31T" + (20 + i) + ":00:00Z', 60_000_000L), x, " + (i * 2) +
                        " FROM long_sequence(60)");
                execute("INSERT INTO " + tableName +
                        " SELECT timestamp_sequence('1970-01-01T0" + i + ":00:00Z', 60_000_000L), x + 1000, " + (i * 2 + 1) +
                        " FROM long_sequence(60)");
            }

            drainWalQueue();

            // 4 iterations × 2 batches × 60 rows = 480 rows
            assertSql("count\n480\n", "SELECT count() FROM " + tableName);
            assertSql("ts\n1969-12-31T20:00:00.000000Z\n", "SELECT ts FROM " + tableName + " LIMIT 1");
            // Dec 31 hours 20–23 and Jan 1 hours 00–03 = 8 hour partitions
            assertSql("count\n8\n",
                    "SELECT count() FROM table_partitions('" + tableName + "')");
        });
    }

    // ==================== Helper methods ====================

    private void assertTableHasNegativeData(String tableName) throws Exception {
        // Verifies the table is non-empty and its minimum timestamp is before 1970.
        assertSql("has_negative\ntrue\n",
                "SELECT min(ts) < '1970-01-01' AS has_negative FROM " + tableName);
    }

    private void createInitialTableNegative(String tableName, boolean isWal, long startMicro) throws Exception {
        SharedRandom.RANDOM.set(new Rnd());

        String tempTableName = "data_temp_neg_" + Math.abs(startMicro % 1_000_000);

        if (engine.getTableTokenIfExists(tempTableName) == null) {
            StringSink tsSink = new StringSink();
            MicrosFormatUtils.appendDateTimeUSec(tsSink, startMicro);
            String startTs = tsSink.toString();

            execute("CREATE ATOMIC TABLE " + tempTableName + " AS (" +
                    "SELECT x AS c1, " +
                    " rnd_symbol('AB', 'BC', 'CD') c2, " +
                    " timestamp_sequence('" + startTs + "'::timestamp, 1_000_000L) ts, " +
                    " rnd_symbol('DE', null, 'EF', 'FG') sym2," +
                    " CAST(x AS INT) c3," +
                    " rnd_bin() c4," +
                    " to_long128(3 * x, 6 * x) c5," +
                    " rnd_str('a', 'bdece', null, ' asdflakji idid', 'dk') str1," +
                    " rnd_boolean() bool1," +
                    " CAST(null AS LONG) long_top," +
                    " CAST(null AS LONG) str_top," +
                    " rnd_symbol(null, 'X', 'Y') sym_top," +
                    " rnd_ipv4() ip4," +
                    " rnd_varchar(5, 10, 1) var_top" +
                    " FROM long_sequence(100)" +
                    ")");
        }

        if (engine.getTableTokenIfExists(tableName) == null) {
            String walClause = isWal ? " WAL" : " BYPASS WAL";
            execute("CREATE ATOMIC TABLE " + tableName + " AS (" +
                    "SELECT * FROM " + tempTableName +
                    "), index(sym2), index(sym_top) TIMESTAMP(ts) PARTITION BY DAY" + walClause);
        }
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
            fuzzer.applyNonWal(transactions, tableNameNoWal, rnd);
            assertTableHasNegativeData(tableNameNoWal);

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

            assertTableHasNegativeData(tableNameWal);

            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableNameNoWal, tableNameWal, LOG);
            }

            Assert.assertEquals("expected 0 errors in partition mutation control", 0,
                    engine.getPartitionOverwriteControl().getErrorCount());

        } finally {
            io.questdb.std.Misc.freeObjListAndClear(transactions);
        }
    }
}
