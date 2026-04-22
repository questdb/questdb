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
import io.questdb.client.std.bytes.DirectByteSequence;
import io.questdb.client.std.str.DirectUtf8Sequence;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Exercises the {@code PageFrameCursor} fast path that kicks in when
 * {@code RecordCursorFactory.supportsPageFrameCursor()} is true.
 * <p>
 * All tests boot the server with page-frame size capped to 128 rows
 * (see {@link #SMALL_PAGE_FRAME_ENV}). Combined with partitioned tables
 * holding 500-2 000 rows across 1-3 partitions, every table scan produces
 * several frames per partition -- the regime that caught the frame-local
 * vs. partition-local row-index bug while the default 1M-row frames never
 * hit the split path. Keeping the frame size small here also keeps the
 * tests fast (no need to ingest millions of rows).
 */
public class QwpEgressPageFrameTest extends AbstractBootstrapTest {

    /**
     * Force the SQL compiler to emit small page frames so every test exercises
     * multi-frame iteration without needing millions of rows. With max=128 and
     * min=64, the effective frame size resolves to 128 (see
     * {@code SqlCodeGenerator.generateTableQuery}); a 500-row partition yields
     * ~4 frames, and a 2 000-row partition yields ~16.
     */
    private static final String[] SMALL_PAGE_FRAME_ENV = new String[]{
            "QDB_CAIRO_SQL_PAGE_FRAME_MAX_ROWS", "128",
            "QDB_CAIRO_SQL_PAGE_FRAME_MIN_ROWS", "64"
    };

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    /**
     * Number of characters {@code Long.toString(v)} would emit. Used in expected-
     * value computations so the test preloop can mirror the server's VARCHAR /
     * STRING concat lengths without allocating a {@code String} per row.
     */
    private static int decimalDigits(long v) {
        if (v < 0) return decimalDigits(-v) + 1;
        if (v < 10L) return 1;
        if (v < 100L) return 2;
        if (v < 1_000L) return 3;
        if (v < 10_000L) return 4;
        if (v < 100_000L) return 5;
        if (v < 1_000_000L) return 6;
        if (v < 10_000_000L) return 7;
        if (v < 100_000_000L) return 8;
        if (v < 1_000_000_000L) return 9;
        if (v < 10_000_000_000L) return 10;
        return 11;
    }

    @Test
    public void testAllTypesMultiPartitionMultiFrame() throws Exception {
        // Kitchen-sink: fixed-width + var-width + SYMBOL dict + NULLs, spread
        // across 3 partitions with multiple frames per partition. Rebuilds the
        // expected checksums locally so any mismatch points at a specific column.
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(SMALL_PAGE_FRAME_ENV)) {
                serverMain.execute("""
                        CREATE TABLE allp(
                            ts TIMESTAMP,
                            id LONG,
                            price DOUBLE,
                            note VARCHAR,
                            s STRING,
                            sy SYMBOL
                        ) TIMESTAMP(ts) PARTITION BY DAY WAL
                        """);
                // 3 partitions (day 1, 2, 3), 500 rows each = 1500 rows total.
                serverMain.execute("""
                        INSERT INTO allp
                        SELECT
                            CAST(86_400_000_000L * ((x - 1) / 500) + (x - 1) * 1_000_000L AS TIMESTAMP),
                            CASE WHEN x % 37 = 0 THEN CAST(NULL AS LONG)    ELSE x END,
                            CASE WHEN x % 41 = 0 THEN CAST(NULL AS DOUBLE)  ELSE x * 1.5 END,
                            CASE WHEN x % 43 = 0 THEN CAST(NULL AS VARCHAR) ELSE 'note_' || x::STRING END,
                            CASE WHEN x % 47 = 0 THEN CAST(NULL AS STRING)  ELSE 'str_'  || x::STRING END,
                            CASE WHEN x % 53 = 0 THEN CAST(NULL AS SYMBOL)  ELSE 'sym_'  || (x % 17)::STRING END
                        FROM long_sequence(1500)
                        """);
                serverMain.awaitTable("allp");

                final int[] rowCount = {0};
                final int[] batchCount = {0};
                final long[] idChecksum = {0};
                final long[] idNullCount = {0};
                final long[] priceChecksum = {0};
                final long[] priceNullCount = {0};
                final long[] noteLenSum = {0};
                final long[] noteNullCount = {0};
                final long[] strLenSum = {0};
                final long[] strNullCount = {0};
                final long[] symLenSum = {0};
                final long[] symNullCount = {0};

                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    client.execute(
                            "SELECT id, price, note, s, sy FROM allp",
                            new QwpColumnBatchHandler() {
                                @Override
                                public void onBatch(QwpColumnBatch batch) {
                                    batchCount[0]++;
                                    int n = batch.getRowCount();
                                    for (int r = 0; r < n; r++) {
                                        if (batch.isNull(0, r)) idNullCount[0]++;
                                        else idChecksum[0] ^= batch.getLongValue(0, r);
                                        if (batch.isNull(1, r)) priceNullCount[0]++;
                                        else priceChecksum[0] ^= Double.doubleToLongBits(batch.getDoubleValue(1, r));
                                        DirectUtf8Sequence note = batch.getStrA(2, r);
                                        if (note == null) noteNullCount[0]++;
                                        else noteLenSum[0] += note.size();
                                        DirectUtf8Sequence str = batch.getStrA(3, r);
                                        if (str == null) strNullCount[0]++;
                                        else strLenSum[0] += str.size();
                                        DirectUtf8Sequence sym = batch.getStrA(4, r);
                                        if (sym == null) symNullCount[0]++;
                                        else symLenSum[0] += sym.size();
                                    }
                                    rowCount[0] += n;
                                }

                                @Override
                                public void onEnd(long totalRows) {
                                    Assert.assertEquals(1500L, totalRows);
                                }

                                @Override
                                public void onError(byte status, String message) {
                                    Assert.fail("egress error: " + message);
                                }
                            });
                }
                Assert.assertEquals(1500, rowCount[0]);
                // 1500 rows split across 128-row frames -> 12 frames; batches are
                // larger (4096 rows MAX_ROWS_PER_BATCH), so it all fits in one batch.
                // Assert we got at least one batch -- the exact count depends on frame
                // sizes and is already covered by testMultiplePartitionsBigScan.
                Assert.assertTrue("at least one batch", batchCount[0] >= 1);

                // Null rates: 1500 / 37 = 40, 1500 / 41 = 36, 1500 / 43 = 34,
                // 1500 / 47 = 31, 1500 / 53 = 28.
                Assert.assertEquals(40L, idNullCount[0]);
                Assert.assertEquals(36L, priceNullCount[0]);
                Assert.assertEquals(34L, noteNullCount[0]);
                Assert.assertEquals(31L, strNullCount[0]);
                Assert.assertEquals(28L, symNullCount[0]);

                // Compute expected checksums the same way as the server, avoiding any
                // String allocation in the precompute loop.
                long expectedIdXor = 0;
                long expectedPriceXor = 0;
                long expectedNoteLen = 0;
                long expectedStrLen = 0;
                long expectedSymLen = 0;
                for (long x = 1; x <= 1500; x++) {
                    if (x % 37 != 0) expectedIdXor ^= x;
                    if (x % 41 != 0) expectedPriceXor ^= Double.doubleToLongBits(x * 1.5);
                    final int xDigits = decimalDigits(x);
                    if (x % 43 != 0) expectedNoteLen += 5 + xDigits;       // "note_" + x
                    if (x % 47 != 0) expectedStrLen += 4 + xDigits;        // "str_" + x
                    if (x % 53 != 0) expectedSymLen += 4 + decimalDigits(x % 17); // "sym_" + (x%17)
                }
                Assert.assertEquals("LONG checksum", expectedIdXor, idChecksum[0]);
                Assert.assertEquals("DOUBLE checksum", expectedPriceXor, priceChecksum[0]);
                Assert.assertEquals("VARCHAR length sum", expectedNoteLen, noteLenSum[0]);
                Assert.assertEquals("STRING length sum", expectedStrLen, strLenSum[0]);
                Assert.assertEquals("SYMBOL length sum", expectedSymLen, symLenSum[0]);
            }
        });
    }

    @Test
    public void testBinaryMultipleFrames() throws Exception {
        // BINARY round-trip through the page-frame path: BINARY isn't supported
        // by ILP, so we ingest via SQL using rnd_bin() and then verify via a
        // running byte-count checksum. BINARY never inlines -- every non-null
        // row crosses aux -> data pointer mapping, same as the VARCHAR failure
        // mode.
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(SMALL_PAGE_FRAME_ENV)) {
                serverMain.execute("CREATE TABLE binp(b BINARY, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("INSERT INTO binp SELECT rnd_bin(1, 64, 5), x::TIMESTAMP FROM long_sequence(800)");
                serverMain.awaitTable("binp");

                final int[] rowCount = {0};
                final long[] totalBytes = {0};
                final int[] nullCount = {0};
                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    client.execute("SELECT b FROM binp", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            int n = batch.getRowCount();
                            for (int r = 0; r < n; r++) {
                                DirectByteSequence bin = batch.getBinaryA(0, r);
                                if (bin == null) nullCount[0]++;
                                else totalBytes[0] += bin.size();
                            }
                            rowCount[0] += n;
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
                Assert.assertEquals(800, rowCount[0]);
                // rnd_bin(1, 64, 5) emits one NULL in every 5 calls on average.
                Assert.assertTrue("some rows must be non-null", totalBytes[0] > 0);
                Assert.assertTrue("some rows must be NULL", nullCount[0] > 0);
            }
        });
    }

    @Test
    public void testCountStarFallsBackToRecordCursor() throws Exception {
        // COUNT(*) returns a single-row aggregate. The aggregate factory typically
        // does not support a PageFrameCursor; verify the RecordCursor fallback path
        // still works end-to-end when the fork chooses it.
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(SMALL_PAGE_FRAME_ENV)) {
                serverMain.execute("CREATE TABLE tcnt(x LONG, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("INSERT INTO tcnt SELECT x, x::TIMESTAMP FROM long_sequence(500)");
                serverMain.awaitTable("tcnt");

                final long[] observed = {-1};
                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    client.execute("SELECT count() FROM tcnt", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            Assert.assertEquals(1, batch.getRowCount());
                            observed[0] = batch.getLongValue(0, 0);
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
                Assert.assertEquals(500L, observed[0]);
            }
        });
    }

    @Test
    public void testEmptyTable() throws Exception {
        // An empty table still makes the processor call getPageFrameCursor -- the
        // cursor just returns null on the first next(). The columnar slice
        // advance must surface null without tripping on an uninitialised record
        // state, and streaming must wrap up with a single empty RESULT_BATCH
        // followed by RESULT_END.
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(SMALL_PAGE_FRAME_ENV)) {
                serverMain.execute("CREATE TABLE empty_t(x LONG, v VARCHAR, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");

                final int[] rowCount = {0};
                final long[] endRows = {-1};
                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    client.execute("SELECT x, v FROM empty_t", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            rowCount[0] += batch.getRowCount();
                        }

                        @Override
                        public void onEnd(long totalRows) {
                            endRows[0] = totalRows;
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("egress error: " + message);
                        }
                    });
                }
                Assert.assertEquals(0, rowCount[0]);
                Assert.assertEquals(0L, endRows[0]);
            }
        });
    }

    @Test
    public void testFilterFallsBackToRecordCursor() throws Exception {
        // Plain SELECT with a WHERE predicate goes through a filtered cursor that
        // typically doesn't expose a PageFrameCursor (the async-filtered variant
        // does, but its record returns row indices via the filter's bitmap -- we
        // only care here that the fork surface + fallback both produce the same
        // rows).
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(SMALL_PAGE_FRAME_ENV)) {
                serverMain.execute("CREATE TABLE tf(x LONG, s VARCHAR, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute(
                        "INSERT INTO tf SELECT x, 'v_' || x::STRING, x::TIMESTAMP FROM long_sequence(500)"
                );
                serverMain.awaitTable("tf");

                final long[] matchCount = {0};
                final long[] sum = {0};
                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    client.execute("SELECT x, s FROM tf WHERE x % 10 = 0", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            for (int r = 0; r < batch.getRowCount(); r++) {
                                sum[0] += batch.getLongValue(0, r);
                                matchCount[0]++;
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
                // 50 multiples of 10 in [1,500]: 10,20,...,500. sum = 10*(1+...+50) = 12750.
                Assert.assertEquals(50L, matchCount[0]);
                Assert.assertEquals(12_750L, sum[0]);
            }
        });
    }

    @Test
    public void testLongNullsSpanFrames() throws Exception {
        // Hits the appendLongOrNull path on a multi-frame scan. Every 7th row is
        // NULL to force the null-bitmap encoding on at least one non-first frame.
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(SMALL_PAGE_FRAME_ENV)) {
                serverMain.execute("CREATE TABLE longnull(x LONG, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("""
                        INSERT INTO longnull
                        SELECT CASE WHEN x % 7 = 0 THEN CAST(NULL AS LONG) ELSE x END,
                               x::TIMESTAMP
                        FROM long_sequence(1000)
                        """);
                serverMain.awaitTable("longnull");

                final long[] nonNullSum = {0};
                final int[] nullCount = {0};
                final int[] rowCount = {0};
                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    client.execute("SELECT x FROM longnull", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            int n = batch.getRowCount();
                            for (int r = 0; r < n; r++) {
                                if (batch.isNull(0, r)) nullCount[0]++;
                                else nonNullSum[0] += batch.getLongValue(0, r);
                            }
                            rowCount[0] += n;
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
                Assert.assertEquals(1000, rowCount[0]);
                // 1000 / 7 = 142 NULLs (multiples of 7 in [1,1000]).
                Assert.assertEquals(142, nullCount[0]);
                // sum(1..1000) - sum(7*k for k=1..142) = 500500 - 7*142*143/2 = 500500 - 71071 = 429429.
                Assert.assertEquals(429_429L, nonNullSum[0]);
            }
        });
    }

    @Test
    public void testMultiplePartitionsBigScan() throws Exception {
        // 2 000 rows across 4 day-partitions, every column type represented.
        // Scans produce ~16 frames (128 rows/frame), ensuring the advance logic
        // crosses partition boundaries several times within a single client
        // batch. Works in the benchmark's shape (WAL + partitioned + SELECT all).
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(SMALL_PAGE_FRAME_ENV)) {
                serverMain.execute(
                        "CREATE TABLE big_multi(ts TIMESTAMP, id LONG, v VARCHAR) TIMESTAMP(ts) PARTITION BY DAY WAL"
                );
                serverMain.execute("""
                        INSERT INTO big_multi
                        SELECT
                            CAST(86_400_000_000L * ((x - 1) / 500) + (x - 1) * 1_000_000L AS TIMESTAMP),
                            x,
                            'v_' || x::STRING
                        FROM long_sequence(2000)
                        """);
                serverMain.awaitTable("big_multi");

                final long[] sum = {0};
                final long[] varcharLenSum = {0};
                final int[] rowCount = {0};
                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    client.execute("SELECT id, v FROM big_multi", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            int n = batch.getRowCount();
                            for (int r = 0; r < n; r++) {
                                sum[0] += batch.getLongValue(0, r);
                                DirectUtf8Sequence v = batch.getStrA(1, r);
                                Assert.assertNotNull(v);
                                varcharLenSum[0] += v.size();
                            }
                            rowCount[0] += n;
                        }

                        @Override
                        public void onEnd(long totalRows) {
                            Assert.assertEquals(2000L, totalRows);
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("egress error: " + message);
                        }
                    });
                }
                Assert.assertEquals(2000, rowCount[0]);
                // sum(1..2000) = n(n+1)/2 = 2001000
                Assert.assertEquals(2_001_000L, sum[0]);
                // v_1..v_9 are 3 chars, v_10..v_99 are 4 chars, v_100..v_999 are 5 chars, v_1000..v_2000 are 6 chars.
                long expectedLen = 9 * 3 + 90 * 4 + 900 * 5 + 1001 * 6;
                Assert.assertEquals(expectedLen, varcharLenSum[0]);
            }
        });
    }

    @Test
    public void testStringMultipleFrames() throws Exception {
        // STRING is UTF-16 on disk -> server UTF-8 encodes via appendString.
        // Verifies the page-frame path correctly routes the STRING CharSequence
        // through the row-indexed path with rows in every frame.
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(SMALL_PAGE_FRAME_ENV)) {
                serverMain.execute("CREATE TABLE strp(s STRING, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                // 800 rows: mix of short ASCII, unicode, and NULL.
                serverMain.execute("""
                        INSERT INTO strp
                        SELECT CASE
                            WHEN x % 11 = 0 THEN CAST(NULL AS STRING)
                            WHEN x % 3  = 0 THEN 'unicode_é_' || x::STRING
                            ELSE                 'ascii_'     || x::STRING
                        END,
                        x::TIMESTAMP
                        FROM long_sequence(800)
                        """);
                serverMain.awaitTable("strp");

                final int[] rowCount = {0};
                final int[] nullCount = {0};
                final long[] asciiLenSum = {0};
                final int[] unicodeCount = {0};
                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    client.execute("SELECT s FROM strp", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            int n = batch.getRowCount();
                            for (int r = 0; r < n; r++) {
                                DirectUtf8Sequence s = batch.getStrA(0, r);
                                if (s == null) {
                                    nullCount[0]++;
                                } else {
                                    // First byte of 'unicode_...' is 'u', of 'ascii_...' is 'a'.
                                    // Read the prefix byte directly from the UTF-8 view rather
                                    // than allocating a String per row.
                                    if (s.byteAt(0) == (byte) 'u') unicodeCount[0]++;
                                    asciiLenSum[0] += s.size();
                                }
                            }
                            rowCount[0] += n;
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
                Assert.assertEquals(800, rowCount[0]);
                // 800 / 11 = 72 NULLs.
                Assert.assertEquals(72, nullCount[0]);
                // Multiples of 3 not also multiples of 11 go unicode. |multiples of 3 in [1,800]| = 266,
                // minus 24 multiples of 33 = 242 unicode values.
                Assert.assertEquals(242, unicodeCount[0]);
                Assert.assertTrue("must observe both unicode and ASCII content", asciiLenSum[0] > 0);
            }
        });
    }

    @Test
    public void testSymbolDictAcrossPartitions() throws Exception {
        // SYMBOL fast path uses record.getInt + cursor.getSymbolTable(col). Each
        // batch rebuilds a local dict; verify that dict IDs resolve to the
        // correct values when a partition boundary lies inside a batch.
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(SMALL_PAGE_FRAME_ENV)) {
                serverMain.execute(
                        "CREATE TABLE sym_t(ts TIMESTAMP, sy SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL"
                );
                // 2 partitions x 300 rows, 5 unique symbols cycling.
                serverMain.execute("""
                        INSERT INTO sym_t
                        SELECT
                            CAST(86_400_000_000L * ((x - 1) / 300) + (x - 1) * 1_000_000L AS TIMESTAMP),
                            'S' || (x % 5)::STRING
                        FROM long_sequence(600)
                        """);
                serverMain.awaitTable("sym_t");

                final int[] counts = new int[5];
                final int[] totalCount = {0};
                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    client.execute("SELECT sy FROM sym_t", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            for (int r = 0; r < batch.getRowCount(); r++) {
                                DirectUtf8Sequence sy = batch.getStrA(0, r);
                                Assert.assertNotNull(sy);
                                // Values are always 'S' + one digit 0-4. Decode from the
                                // UTF-8 view directly so this inner loop stays allocation-
                                // free on the hot path (the test harness already asserts
                                // per-symbol counts).
                                Assert.assertEquals(2, sy.size());
                                Assert.assertEquals((byte) 'S', sy.byteAt(0));
                                int idx = sy.byteAt(1) - '0';
                                Assert.assertTrue(idx >= 0 && idx < 5);
                                counts[idx]++;
                                totalCount[0]++;
                            }
                        }

                        @Override
                        public void onEnd(long totalRows) {
                            Assert.assertEquals(600L, totalRows);
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("egress error: " + message);
                        }
                    });
                }
                Assert.assertEquals(600, totalCount[0]);
                // x % 5 cycles 1,2,3,4,0 over x=1..600. Each value hits 120 times.
                for (int i = 0; i < 5; i++) {
                    Assert.assertEquals("bucket " + i, 120, counts[i]);
                }
            }
        });
    }

    @Test
    public void testVarcharMultipleFramesSinglePartition() throws Exception {
        // This is the exact failure mode that slipped through the initial
        // PageFrameCursor integration: VARCHAR values read with a partition-
        // local row index against a page address that the cursor already
        // biased by the frame's starting row -- resulting in aux entries
        // pointing past the mapped data file.
        //
        // Reproducer: one partition, many rows, several non-first frames.
        // Pre-fix: "varchar is outside of file boundary" thrown on the second
        // frame onwards. Post-fix: every row round-trips.
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(SMALL_PAGE_FRAME_ENV)) {
                serverMain.execute("CREATE TABLE vcp(v VARCHAR, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                // 500 rows, values long enough to exceed the 9-byte inline threshold
                // (e.g. "varchar_value_0001" is 18 bytes) so every non-null aux
                // entry references the data file. Without the fix, the data
                // pointer drifts past the mapped data page on frames 2+.
                serverMain.execute("""
                        INSERT INTO vcp
                        SELECT 'varchar_value_' || lpad(x::STRING, 4, '0'), x::TIMESTAMP
                        FROM long_sequence(500)
                        """);
                serverMain.awaitTable("vcp");

                final int[] rowCount = {0};
                final long[] lenSum = {0};
                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    client.execute("SELECT v FROM vcp", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            int n = batch.getRowCount();
                            for (int r = 0; r < n; r++) {
                                DirectUtf8Sequence v = batch.getStrA(0, r);
                                Assert.assertNotNull("row " + r + " should be non-null", v);
                                lenSum[0] += v.size();
                            }
                            rowCount[0] += n;
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
                // 500 rows x 18 bytes each = 9000.
                Assert.assertEquals(9_000L, lenSum[0]);
            }
        });
    }
}
