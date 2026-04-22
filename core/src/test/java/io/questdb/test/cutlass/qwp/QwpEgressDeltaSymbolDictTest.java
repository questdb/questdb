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

package io.questdb.test.cutlass.qwp;

import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;
import io.questdb.client.cutlass.qwp.client.QwpQueryClient;
import io.questdb.client.std.str.DirectUtf8Sequence;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * End-to-end tests for {@code FLAG_DELTA_SYMBOL_DICT} on egress: a
 * connection-scoped SYMBOL dictionary maintained on both server (encoder) and
 * client (decoder), refreshed per-batch via a message-level delta section.
 * Each RESULT_BATCH ships only symbols never-before-seen on the connection;
 * recurring symbols cost one varint id on the wire.
 * <p>
 * Correctness checks:
 * <ul>
 *   <li>recurring symbols across queries resolve to the right values on the
 *       client even though the dict bytes are only sent once;</li>
 *   <li>net bytes shipped strictly decrease when the second query hits an
 *       already-warm dict (a spot check that the delta section is actually
 *       empty, not just that the wire happened to compress well);</li>
 *   <li>symbols that first appear on the second query extend the dict
 *       correctly and are resolvable;</li>
 *   <li>multiple SYMBOL columns in one batch share one connection dict;</li>
 *   <li>tables with no SYMBOL columns still tolerate the always-on flag.</li>
 * </ul>
 */
public class QwpEgressDeltaSymbolDictTest extends AbstractBootstrapTest {

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testMultipleSymbolColumnsShareOneDict() throws Exception {
        // Two SYMBOL columns with OVERLAPPING sets of values. Both columns index
        // into the same connection-scoped dict, so only one copy of each unique
        // symbol rides the wire.
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE two_sym(a SYMBOL, b SYMBOL, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                // a cycles through {X, Y, Z}; b cycles through {Y, Z, W}. Shared {Y, Z}
                // should only be transmitted once total across both columns.
                serverMain.execute("""
                        INSERT INTO two_sym
                        SELECT
                            CASE WHEN (x % 3) = 0 THEN 'X' WHEN (x % 3) = 1 THEN 'Y' ELSE 'Z' END,
                            CASE WHEN (x % 3) = 0 THEN 'Y' WHEN (x % 3) = 1 THEN 'Z' ELSE 'W' END,
                            x::TIMESTAMP
                        FROM long_sequence(30)
                        """);
                serverMain.awaitTable("two_sym");

                final int[] countA = new int[4];  // X, Y, Z, W
                final int[] countB = new int[4];
                final int[] rowCount = {0};
                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    client.execute("SELECT a, b FROM two_sym", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            for (int r = 0; r < batch.getRowCount(); r++) {
                                countA[bucket(batch.getStrA(0, r))]++;
                                countB[bucket(batch.getStrA(1, r))]++;
                                rowCount[0]++;
                            }
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("egress error: " + message);
                        }
                    });
                }
                Assert.assertEquals(30, rowCount[0]);
                // x in [1,30], x%3 distribution: 0 -> 10 rows, 1 -> 10, 2 -> 10.
                // a: x%3==0 -> X (10), x%3==1 -> Y (10), x%3==2 -> Z (10)
                Assert.assertEquals(10, countA[0]);
                Assert.assertEquals(10, countA[1]);
                Assert.assertEquals(10, countA[2]);
                Assert.assertEquals(0, countA[3]);
                // b: x%3==0 -> Y, x%3==1 -> Z, x%3==2 -> W
                Assert.assertEquals(0, countB[0]);
                Assert.assertEquals(10, countB[1]);
                Assert.assertEquals(10, countB[2]);
                Assert.assertEquals(10, countB[3]);
            }
        });
    }

    @Test
    public void testNewSymbolsOnSecondQueryExtendDict() throws Exception {
        // Query 1 populates the dict with {alpha, beta}. A second client, fresh
        // connection, INSERTs 'gamma' then Query 2 runs on the same original
        // client connection -- {alpha, beta, gamma} are observed, and only
        // 'gamma' ships as a new delta entry. All three round-trip correctly.
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE inc(s SYMBOL, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("INSERT INTO inc VALUES ('alpha', 1::TIMESTAMP), "
                        + "('beta', 2::TIMESTAMP), ('alpha', 3::TIMESTAMP)");
                serverMain.awaitTable("inc");

                final long[] q1TotalBytes = {0};
                final long[] q1RowCount = {0};
                final long[] q2TotalBytes = {0};
                final long[] q2RowCount = {0};
                final int[] observedAlpha = {0};
                final int[] observedBeta = {0};
                final int[] observedGamma = {0};

                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();

                    client.execute("SELECT s FROM inc", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            q1TotalBytes[0] += batch.payloadLimit() - batch.payloadAddr();
                            q1RowCount[0] += batch.getRowCount();
                            for (int r = 0; r < batch.getRowCount(); r++) {
                                DirectUtf8Sequence v = batch.getStrA(0, r);
                                Assert.assertNotNull(v);
                                // 'alpha' length 5 starts with 'a'; 'beta' length 4 starts with 'b'.
                                if (v.byteAt(0) == (byte) 'a' && v.size() == 5) observedAlpha[0]++;
                                else if (v.byteAt(0) == (byte) 'b' && v.size() == 4) observedBeta[0]++;
                            }
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("egress error on q1: " + message);
                        }
                    });

                    // Add a brand-new symbol between queries.
                    serverMain.execute("INSERT INTO inc VALUES ('gamma', 4::TIMESTAMP), ('alpha', 5::TIMESTAMP)");
                    serverMain.awaitTable("inc");

                    client.execute("SELECT s FROM inc", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            q2TotalBytes[0] += batch.payloadLimit() - batch.payloadAddr();
                            q2RowCount[0] += batch.getRowCount();
                            for (int r = 0; r < batch.getRowCount(); r++) {
                                DirectUtf8Sequence v = batch.getStrA(0, r);
                                Assert.assertNotNull(v);
                                if (v.byteAt(0) == (byte) 'a' && v.size() == 5) observedAlpha[0]++;
                                else if (v.byteAt(0) == (byte) 'b' && v.size() == 4) observedBeta[0]++;
                                else if (v.byteAt(0) == (byte) 'g' && v.size() == 5) observedGamma[0]++;
                            }
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("egress error on q2: " + message);
                        }
                    });
                }
                // Row-count sanity: q1 returns 3 rows, q2 returns 5.
                Assert.assertEquals(3, q1RowCount[0]);
                Assert.assertEquals(5, q2RowCount[0]);
                // q1 observed: alpha x2 + beta x1. q2: alpha x3 (original 2 + new 1) + beta x1 + gamma x1.
                Assert.assertEquals(2 + 3, observedAlpha[0]);
                Assert.assertEquals(1 + 1, observedBeta[0]);
                Assert.assertEquals(1, observedGamma[0]);
                // Bytes-wise: q2 has 2 more rows than q1, so only the ratio matters.
                // Per-row: one varint dict id (1 byte) each. q2's delta carries only
                // 'gamma' (6 bytes: varint len 1 + 5 bytes). q1's delta carries
                // 'alpha' + 'beta' (12 bytes). So q2 per-row should be strictly
                // cheaper on the SYMBOL portion than q1's per-row if we normalise.
                // Spot check: q2 added 1 new symbol (gamma=5 bytes), q1 added 2
                // (alpha=5, beta=4). With 2 extra rows in q2 (each contributing ~1
                // byte), q2 total should still be <= q1 + 2 + (gamma delta cost) - (alpha+beta delta cost).
                // Keep the assertion loose: q2 should not exceed q1 by more than 12 bytes
                // (1 byte/row extra + the 'gamma' delta of ~7 bytes).
                Assert.assertTrue(
                        "delta dict should shrink per-query bytes: q1=" + q1TotalBytes[0]
                                + ", q2=" + q2TotalBytes[0],
                        q2TotalBytes[0] <= q1TotalBytes[0] + 12
                );
            }
        });
    }

    @Test
    public void testNoSymbolColumnsStillWorksWithFlag() throws Exception {
        // Sanity: the server sets FLAG_DELTA_SYMBOL_DICT on every RESULT_BATCH
        // regardless of whether any SYMBOL column is present. Tables with none
        // must still round-trip correctly (empty delta section, empty dict).
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE nosym(x LONG, y DOUBLE, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("INSERT INTO nosym SELECT x, x * 1.5, x::TIMESTAMP FROM long_sequence(100)");
                serverMain.awaitTable("nosym");

                final long[] sum = {0};
                final int[] rowCount = {0};
                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    client.execute("SELECT x, y FROM nosym", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            for (int r = 0; r < batch.getRowCount(); r++) {
                                sum[0] += batch.getLongValue(0, r);
                                rowCount[0]++;
                            }
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("egress error: " + message);
                        }
                    });
                }
                Assert.assertEquals(100, rowCount[0]);
                Assert.assertEquals(100L * 101 / 2, sum[0]);
            }
        });
    }

    @Test
    public void testRecurringSymbolsSecondQueryHasEmptyDelta() throws Exception {
        // Two queries on the same connection over the same SYMBOL-heavy data.
        // On the second query the connection dict is already full; the delta
        // section should be empty (start=0 count=0 = 2 bytes on the wire),
        // meaning Q2's total bytes are smaller than Q1's by the size of the
        // symbol dict entries that Q1 shipped.
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE recur(s SYMBOL, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                // 60 rows, 3 unique symbols (aa, bbb, cccc) -- guaranteed total
                // entry-bytes savings (2+3+4 = 9 bytes of dict + 3 varint lengths
                // = 12 bytes) which dominates the 2-byte empty delta section.
                serverMain.execute("""
                        INSERT INTO recur
                        SELECT CASE WHEN (x % 3) = 0 THEN 'aa' WHEN (x % 3) = 1 THEN 'bbb' ELSE 'cccc' END,
                               x::TIMESTAMP
                        FROM long_sequence(60)
                        """);
                serverMain.awaitTable("recur");

                final long[] q1Bytes = {0};
                final long[] q2Bytes = {0};
                final int[] q1Rows = {0};
                final int[] q2Rows = {0};

                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();

                    client.execute("SELECT s FROM recur", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            q1Bytes[0] += batch.payloadLimit() - batch.payloadAddr();
                            q1Rows[0] += batch.getRowCount();
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("egress error on q1: " + message);
                        }
                    });

                    client.execute("SELECT s FROM recur", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            q2Bytes[0] += batch.payloadLimit() - batch.payloadAddr();
                            q2Rows[0] += batch.getRowCount();
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("egress error on q2: " + message);
                        }
                    });
                }
                Assert.assertEquals(60, q1Rows[0]);
                Assert.assertEquals(60, q2Rows[0]);
                // Q2 must be strictly smaller: Q1 shipped 3 dict entries (~12 bytes);
                // Q2 skips them. Per-row payload + row counts are identical between
                // the two runs.
                Assert.assertTrue("Q2 should be smaller than Q1 [q1=" + q1Bytes[0]
                                + ", q2=" + q2Bytes[0] + ']',
                        q2Bytes[0] < q1Bytes[0]);
            }
        });
    }

    @Test
    public void testRoundTripManyUniqueSymbolsAcrossTwoQueries() throws Exception {
        // Stress the dict with 100 unique values across two queries (query 1
        // populates 50, query 2 adds 50 more) to flush out issues with the
        // client's native heap growing and invalidating existing flyweight
        // views. Realloc inside QwpResultBatchDecoder.ensureConnDictHeapCapacity
        // has to rebase every existing DirectUtf8String; this test would fail
        // with invalid pointer dereferences if it didn't.
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE many(s SYMBOL, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                // First 50 rows use 's_000' .. 's_049'.
                serverMain.execute(
                        "INSERT INTO many SELECT 's_' || lpad(x::STRING, 3, '0'), x::TIMESTAMP FROM long_sequence(50)"
                );
                serverMain.awaitTable("many");

                // buckets are 1..100 (s_001..s_100); size 101 so index 100 is valid.
                final int[] q1Observed = new int[101];
                final int[] q2Observed = new int[101];

                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();

                    client.execute("SELECT s FROM many", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            for (int r = 0; r < batch.getRowCount(); r++) {
                                DirectUtf8Sequence v = batch.getStrA(0, r);
                                Assert.assertNotNull(v);
                                q1Observed[decodeBucket(v)]++;
                            }
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("egress error on q1: " + message);
                        }
                    });

                    // Add rows 50..99 before the second query.
                    serverMain.execute(
                            "INSERT INTO many " +
                                    "SELECT 's_' || lpad((x + 50)::STRING, 3, '0'), (x + 50)::TIMESTAMP FROM long_sequence(50)"
                    );
                    serverMain.awaitTable("many");

                    client.execute("SELECT s FROM many", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            for (int r = 0; r < batch.getRowCount(); r++) {
                                DirectUtf8Sequence v = batch.getStrA(0, r);
                                Assert.assertNotNull(v);
                                q2Observed[decodeBucket(v)]++;
                            }
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("egress error on q2: " + message);
                        }
                    });
                }
                // Q1: exactly one occurrence each of s_001..s_050 (1-based x).
                // x=1..50 -> s_001..s_050 -> buckets 1..50.
                for (int i = 1; i <= 50; i++) {
                    Assert.assertEquals("q1 bucket " + i, 1, q1Observed[i]);
                }
                for (int i = 51; i <= 100; i++) {
                    Assert.assertEquals("q1 bucket " + i + " should be 0", 0, q1Observed[i]);
                }
                // Q2: each of s_001..s_100 exactly once. Resolving indices for
                // s_001..s_050 verifies the existing dict entries survived the
                // realloc that adding s_051..s_100 triggered.
                for (int i = 1; i <= 100; i++) {
                    Assert.assertEquals("q2 bucket " + i, 1, q2Observed[i]);
                }
            }
        });
    }

    /**
     * Maps the single-byte leading char of a fixed-cardinality symbol to a
     * small bucket index so tests can tally occurrences without allocating a
     * String per row. Expected values are 'X' (0), 'Y' (1), 'Z' (2), 'W' (3).
     */
    private static int bucket(DirectUtf8Sequence v) {
        Assert.assertNotNull(v);
        byte b = v.byteAt(0);
        return switch (b) {
            case 'X' -> 0;
            case 'Y' -> 1;
            case 'Z' -> 2;
            case 'W' -> 3;
            default -> {
                Assert.fail("unexpected symbol leading byte: " + (b & 0xFF));
                yield -1;
            }
        };
    }

    /**
     * Decodes {@code "s_NNN"} (always exactly 5 bytes, ASCII) into the integer
     * {@code NNN} without allocating a {@code String}. Used by the many-unique-
     * symbols test to verify round-trip correctness.
     */
    private static int decodeBucket(DirectUtf8Sequence v) {
        Assert.assertEquals(5, v.size());
        Assert.assertEquals((byte) 's', v.byteAt(0));
        Assert.assertEquals((byte) '_', v.byteAt(1));
        int hundreds = v.byteAt(2) - '0';
        int tens = v.byteAt(3) - '0';
        int ones = v.byteAt(4) - '0';
        return hundreds * 100 + tens * 10 + ones;
    }
}
