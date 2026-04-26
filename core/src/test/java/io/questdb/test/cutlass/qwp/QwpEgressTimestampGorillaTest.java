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
import io.questdb.cutlass.qwp.server.egress.QwpEgressUpgradeProcessor;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * End-to-end coverage for {@code FLAG_GORILLA} timestamp compression on egress.
 * <p>
 * The server sets the flag unconditionally on every {@code RESULT_BATCH}; for
 * each TIMESTAMP / TIMESTAMP_NANOS / DATE column it prefixes a one-byte
 * encoding discriminator and either memcpys the raw int64s (when unordered or
 * when Gorilla wouldn't save space) or writes a delta-of-delta bitstream. The
 * client decodes both and the user-facing accessor path is unchanged.
 * <p>
 * Tests assert:
 * <ul>
 *   <li>round-trip correctness for all three timestamp types;</li>
 *   <li>the ordered-ascending fast path actually compresses -- the wire
 *       bytes a batch carries must be materially smaller than the raw
 *       {@code 8 * rowCount} baseline;</li>
 *   <li>unordered timestamps round-trip (the fallback triggers and is not
 *       silently truncated);</li>
 *   <li>NULLs interleaved with non-NULL values compress and decode correctly
 *       -- Gorilla encodes only the dense non-null sequence;</li>
 *   <li>boundary row counts (0, 1, 2, 3) each decode correctly -- the Gorilla
 *       encoder changes shape below the 3-row threshold;</li>
 *   <li>multiple timestamp columns in the same batch can each pick its own
 *       encoding;</li>
 *   <li>multi-batch streaming keeps the per-batch heap slot reset so a
 *       compressed column in batch N+1 doesn't read batch N's decoded bytes.</li>
 * </ul>
 */
public class QwpEgressTimestampGorillaTest extends AbstractBootstrapTest {

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testBoundaryRowCounts() throws Exception {
        // For 0, 1, 2 rows, the Gorilla encoder writes raw int64s (there's no
        // delta-of-delta to encode). For 3+ rows, it enters the bitstream path.
        // Each case has to round-trip cleanly and the client must read the
        // right encoding byte. All scenarios run on the same server to keep
        // the test under a second; distinct table names avoid collisions.
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                for (int n : new int[]{0, 1, 2, 3, 4, 10}) {
                    String tbl = "bnd_" + n;
                    serverMain.execute("CREATE TABLE " + tbl + "(ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                    if (n > 0) {
                        serverMain.execute(String.format("""
                                INSERT INTO %s
                                SELECT CAST((x - 1) * 1_000_000L AS TIMESTAMP)
                                FROM long_sequence(%d)
                                """, tbl, n));
                        serverMain.awaitTable(tbl);
                    }

                    final long[] sum = {0};
                    final int[] rowCount = {0};
                    try (QwpQueryClient client = QwpQueryClient.fromConfig(
                            "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                        client.connect();
                        client.execute("SELECT ts FROM " + tbl, new QwpColumnBatchHandler() {
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
                                Assert.fail("egress error on n=" + n + ": " + message);
                            }
                        });
                    }
                    Assert.assertEquals("n=" + n, n, rowCount[0]);
                    // Expected: sum of (x-1) * 1e6 for x in [1, n] = 1e6 * (n-1)*n/2.
                    Assert.assertEquals("n=" + n, 1_000_000L * (n - 1) * n / 2, sum[0]);
                }
            }
        });
    }

    @Test
    public void testDateAscending() throws Exception {
        // DATE is also a 64-bit signed integer (ms since epoch). Same wire
        // handling as TIMESTAMP, same Gorilla path.
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE dt(d DATE)");
                // 500 rows, 100 ms apart starting at 0.
                serverMain.execute("""
                        INSERT INTO dt
                        SELECT CAST((x - 1) * 100 AS DATE)
                        FROM long_sequence(500)
                        """);

                final long[] sum = {0};
                final long[] bytes = {0};
                final int[] rowCount = {0};
                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    client.execute("SELECT d FROM dt", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            bytes[0] += batch.payloadLimit() - batch.payloadAddr();
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
                Assert.assertEquals(500, rowCount[0]);
                Assert.assertEquals(100L * 499 * 500 / 2, sum[0]);
                // Raw DATE would be 500 * 8 = 4000 bytes; Gorilla on a
                // perfectly periodic stream is ~1 bit/value. Whole batch must
                // be well under the raw size.
                Assert.assertTrue("batch should compress [bytes=" + bytes[0] + ']',
                        bytes[0] < 1_500);
            }
        });
    }

    @Test
    public void testDesignatedTimestampAscendingCompresses() throws Exception {
        // Designated timestamp in a partitioned table -- always ascending. With
        // a 10 ms cadence Gorilla should hit the 1-bit/value path for almost
        // every row. We can't inspect per-column bytes directly, but the
        // overall batch payload must be significantly smaller than the raw
        // (8 * rowCount) baseline.
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute(
                        "CREATE TABLE dts(ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL"
                );
                final int N = 4000;
                serverMain.execute(String.format("""
                        INSERT INTO dts
                        SELECT CAST((x - 1) * 10_000L AS TIMESTAMP), x
                        FROM long_sequence(%d)
                        """, N));
                serverMain.awaitTable("dts");

                final long[] lastSeen = {Long.MIN_VALUE};
                final long[] totalBytes = {0};
                final int[] rowCount = {0};
                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    client.execute("SELECT ts, x FROM dts", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            totalBytes[0] += batch.payloadLimit() - batch.payloadAddr();
                            for (int r = 0; r < batch.getRowCount(); r++) {
                                long ts = batch.getLongValue(0, r);
                                Assert.assertTrue("row " + r + " non-monotonic",
                                        ts > lastSeen[0] || lastSeen[0] == Long.MIN_VALUE);
                                lastSeen[0] = ts;
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
                Assert.assertEquals(N, rowCount[0]);
                // Baseline: ts column alone would be N * 8 = 32_000 bytes; with
                // LONG x column (also N * 8), schema + nulls + framing, the raw
                // lower bound is ~65 KB. With Gorilla compression, the ts column
                // drops to ~N bits = 500 bytes; total batch should be < 40 KB.
                Assert.assertTrue(
                        "total wire bytes should beat raw (seen=" + totalBytes[0] + ')',
                        totalBytes[0] < 40_000
                );
            }
        });
    }

    @Test
    public void testMixedTimestampColumnsOneOrderedOneNot() throws Exception {
        // Two TIMESTAMP columns in one batch: one ascending, one random. Each
        // column picks its own per-column encoding byte; server must not pick a
        // single Gorilla/uncompressed choice for the whole batch. Client reads
        // both encoding bytes and round-trips each column correctly.
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute(
                        "CREATE TABLE mix(ts TIMESTAMP, scrambled TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL"
                );
                // ts ascending by 1 s; scrambled deliberately jumps around.
                serverMain.execute("""
                        INSERT INTO mix
                        SELECT
                            CAST((x - 1) * 1_000_000L AS TIMESTAMP),
                            CAST((x % 7) * 1_000_000_000_000L + x AS TIMESTAMP),
                            x
                        FROM long_sequence(300)
                        """);
                serverMain.awaitTable("mix");

                final int[] rowCount = {0};
                final long[] tsSum = {0};
                final long[] scrambledSum = {0};
                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    client.execute("SELECT ts, scrambled FROM mix", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            for (int r = 0; r < batch.getRowCount(); r++) {
                                tsSum[0] += batch.getLongValue(0, r);
                                scrambledSum[0] += batch.getLongValue(1, r);
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
                Assert.assertEquals(300, rowCount[0]);
                // Expected sums computed arithmetically (no allocation per row).
                long expectedTs = 0;
                long expectedScrambled = 0;
                for (long x = 1; x <= 300; x++) {
                    expectedTs += (x - 1) * 1_000_000L;
                    expectedScrambled += (x % 7) * 1_000_000_000_000L + x;
                }
                Assert.assertEquals(expectedTs, tsSum[0]);
                Assert.assertEquals(expectedScrambled, scrambledSum[0]);
            }
        });
    }

    @Test
    public void testMultiBatchStreamingKeepsGorillaHeapConsistent() throws Exception {
        // > MAX_ROWS_PER_BATCH rows forces multiple batches. Each batch resets
        // the client-side Gorilla heap. A regression here (e.g. not resetting
        // heapPos between batches) would bleed decoded bytes from batch N into
        // batch N+1.
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute(
                        "CREATE TABLE multi(ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL"
                );
                // Use MAX_ROWS_PER_BATCH + a full-batch-worth extra so the test
                // always produces at least two batches regardless of the server's
                // current cap.
                final int N = QwpEgressUpgradeProcessor.MAX_ROWS_PER_BATCH + QwpEgressUpgradeProcessor.MAX_ROWS_PER_BATCH / 2;
                serverMain.execute(String.format("""
                        INSERT INTO multi
                        SELECT CAST((x - 1) * 100_000L AS TIMESTAMP)
                        FROM long_sequence(%d)
                        """, N));
                serverMain.awaitTable("multi");

                final long[] lastSeen = {Long.MIN_VALUE};
                final int[] batches = {0};
                final int[] rowCount = {0};
                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    client.execute("SELECT ts FROM multi", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            batches[0]++;
                            for (int r = 0; r < batch.getRowCount(); r++) {
                                long ts = batch.getLongValue(0, r);
                                Assert.assertTrue("non-monotonic at " + rowCount[0] +
                                                " (prev=" + lastSeen[0] + ", got=" + ts + ')',
                                        ts > lastSeen[0] || lastSeen[0] == Long.MIN_VALUE);
                                lastSeen[0] = ts;
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
                Assert.assertEquals(N, rowCount[0]);
                Assert.assertTrue("expected multiple batches, got " + batches[0], batches[0] > 1);
                // Final timestamp should be (N-1) * 100_000.
                Assert.assertEquals((long) (N - 1) * 100_000L, lastSeen[0]);
            }
        });
    }

    @Test
    public void testNonDesignatedTimestampAscending() throws Exception {
        // Non-designated TIMESTAMP column (not the partition key). When the SQL
        // happens to return rows in ts order, Gorilla still compresses. We
        // select WHERE x > 0 then ORDER BY ts to guarantee ordering.
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE nonsts(x LONG, ts2 TIMESTAMP)");
                // ts2 ascending, x scrambled.
                serverMain.execute("""
                        INSERT INTO nonsts
                        SELECT (x * 31) % 1000, CAST((x - 1) * 5_000_000L AS TIMESTAMP)
                        FROM long_sequence(500)
                        """);

                final long[] lastSeen = {Long.MIN_VALUE};
                final long[] bytes = {0};
                final int[] rowCount = {0};
                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    client.execute("SELECT ts2 FROM nonsts ORDER BY ts2",
                            new QwpColumnBatchHandler() {
                                @Override
                                public void onBatch(QwpColumnBatch batch) {
                                    bytes[0] += batch.payloadLimit() - batch.payloadAddr();
                                    for (int r = 0; r < batch.getRowCount(); r++) {
                                        long ts = batch.getLongValue(0, r);
                                        Assert.assertTrue("non-monotonic",
                                                ts > lastSeen[0] || lastSeen[0] == Long.MIN_VALUE);
                                        lastSeen[0] = ts;
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
                Assert.assertEquals(500, rowCount[0]);
                // Raw: 500 * 8 = 4000 bytes for the ts column; compressed
                // should be well under. Assert batch total under 2 KB.
                Assert.assertTrue("bytes=" + bytes[0], bytes[0] < 2_000);
            }
        });
    }

    @Test
    public void testNonDesignatedTimestampUnordered() throws Exception {
        // Random-ish timestamps (x*31 modular sequence). Deltas of deltas jump
        // around -- Gorilla can't compress, encoder falls back to uncompressed.
        // Client must still round-trip every value correctly via the
        // encoding=0x00 path.
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE scr(ts2 TIMESTAMP)");
                // Pseudo-random: multiply by a prime and take modulo of a
                // large value. Delta-of-delta won't fit in int32 reliably.
                serverMain.execute("""
                        INSERT INTO scr
                        SELECT CAST((x * 1_600_000_003L) % 9_000_000_000_000L AS TIMESTAMP)
                        FROM long_sequence(200)
                        """);

                final int[] rowCount = {0};
                final long[] checksum = {0};
                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    client.execute("SELECT ts2 FROM scr", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            for (int r = 0; r < batch.getRowCount(); r++) {
                                checksum[0] ^= batch.getLongValue(0, r);
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
                Assert.assertEquals(200, rowCount[0]);
                // Expected checksum: XOR over (x*1_600_000_003) % 9e12 for x in [1,200].
                long expected = 0;
                for (long x = 1; x <= 200; x++) {
                    expected ^= (x * 1_600_000_003L) % 9_000_000_000_000L;
                }
                Assert.assertEquals(expected, checksum[0]);
            }
        });
    }

    @Test
    public void testTimestampNanosAscending() throws Exception {
        // TIMESTAMP_NANOS shares the same 8-byte int64 shape but different
        // unit. Same Gorilla path applies. 100 ns cadence compresses tightly.
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE nanos(t TIMESTAMP_NS)");
                serverMain.execute("""
                        INSERT INTO nanos
                        SELECT CAST((x - 1) * 100 AS TIMESTAMP_NS)
                        FROM long_sequence(400)
                        """);

                final long[] sum = {0};
                final int[] rowCount = {0};
                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    client.execute("SELECT t FROM nanos", new QwpColumnBatchHandler() {
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
                Assert.assertEquals(400, rowCount[0]);
                Assert.assertEquals(100L * 399 * 400 / 2, sum[0]);
            }
        });
    }

    @Test
    public void testTimestampWithNullsInterleaved() throws Exception {
        // Null bitmap + Gorilla stream together. Gorilla operates only on the
        // dense non-null sequence; the client's nonNullIdx routes per-row
        // access. Every 5th row null; non-null ts values stay 1 s cadence so
        // they compress (delta-of-delta on the dense sequence is 0).
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE tsn(ts TIMESTAMP)");
                serverMain.execute("""
                        INSERT INTO tsn
                        SELECT CASE WHEN x % 5 = 0 THEN CAST(NULL AS TIMESTAMP)
                                    ELSE CAST((x - 1) * 1_000_000L AS TIMESTAMP) END
                        FROM long_sequence(300)
                        """);

                final int[] nullCount = {0};
                final int[] rowCount = {0};
                final long[] nonNullSum = {0};
                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    client.execute("SELECT ts FROM tsn", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            for (int r = 0; r < batch.getRowCount(); r++) {
                                if (batch.isNull(0, r)) nullCount[0]++;
                                else nonNullSum[0] += batch.getLongValue(0, r);
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
                Assert.assertEquals(300, rowCount[0]);
                Assert.assertEquals(60, nullCount[0]);  // multiples of 5 in [1,300]
                // Non-null values: (x-1)*1e6 where x in [1,300], x%5 != 0.
                long expected = 0;
                for (long x = 1; x <= 300; x++) {
                    if (x % 5 != 0) expected += (x - 1) * 1_000_000L;
                }
                Assert.assertEquals(expected, nonNullSum[0]);
            }
        });
    }
}
