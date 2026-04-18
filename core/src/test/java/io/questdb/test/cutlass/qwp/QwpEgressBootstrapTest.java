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
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * End-to-end Phase-1 smoke test for QWP egress: boot an embedded QuestDB,
 * populate a table via SQL, open {@link QwpQueryClient} against /read/v1,
 * issue a SELECT, and assert the decoded batches match.
 */
public class QwpEgressBootstrapTest extends AbstractBootstrapTest {

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testSelectLong() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE t(x LONG)");
                serverMain.execute("INSERT INTO t VALUES (1), (2), (3)");

                List<Long> collected = new ArrayList<>();
                AtomicLong totalRows = new AtomicLong(-1);
                AtomicBoolean errorSeen = new AtomicBoolean(false);

                try (QwpQueryClient client = QwpQueryClient.newPlainText("127.0.0.1", HTTP_PORT)) {
                    client.connect();
                    client.execute("SELECT x FROM t ORDER BY x", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            Assert.assertEquals(1, batch.getColumnCount());
                            Assert.assertEquals("x", batch.getColumnName(0));
                            for (int r = 0; r < batch.getRowCount(); r++) {
                                collected.add(batch.getLong(0, r));
                            }
                        }

                        @Override
                        public void onEnd(long rows) {
                            totalRows.set(rows);
                        }

                        @Override
                        public void onError(byte status, String message) {
                            errorSeen.set(true);
                            Assert.fail("egress query error: status=" + status + " msg=" + message);
                        }
                    });
                }

                Assert.assertFalse(errorSeen.get());
                Assert.assertEquals(List.of(1L, 2L, 3L), collected);
            }
        });
    }

    @Test
    public void testSelectMixedTypes() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE mixed(id LONG, px DOUBLE, sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute(
                        "INSERT INTO mixed VALUES " +
                                "  (1, 1.5, 'AAPL', '2024-01-01T00:00:00.000Z'), " +
                                "  (2, 2.5, 'MSFT', '2024-01-01T00:00:01.000Z'), " +
                                "  (3, 3.5, 'AAPL', '2024-01-01T00:00:02.000Z')"
                );
                serverMain.awaitTxn("mixed", 1);

                final int[] rows = {0};
                final String[] expectedSym = {"AAPL", "MSFT", "AAPL"};
                final double[] expectedPx = {1.5, 2.5, 3.5};

                try (QwpQueryClient client = QwpQueryClient.newPlainText("127.0.0.1", HTTP_PORT)) {
                    client.connect();
                    client.execute("SELECT id, px, sym, ts FROM mixed ORDER BY id", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            Assert.assertEquals(4, batch.getColumnCount());
                            for (int r = 0; r < batch.getRowCount(); r++, rows[0]++) {
                                Assert.assertEquals((long) (rows[0] + 1), batch.getLong(0, r));
                                Assert.assertEquals(expectedPx[rows[0]], batch.getDouble(1, r), 1e-9);
                                Assert.assertEquals(expectedSym[rows[0]], batch.getString(2, r));
                            }
                        }

                        @Override
                        public void onEnd(long totalRows) {
                            // reachable
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("egress query error: status=" + status + " msg=" + message);
                        }
                    });
                }
                Assert.assertEquals(3, rows[0]);
            }
        });
    }

    @Test
    public void testAllPrimitiveTypes() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute(
                        "CREATE TABLE allp(" +
                                "  b BOOLEAN," +
                                "  bt BYTE," +
                                "  sh SHORT," +
                                "  ch CHAR," +
                                "  i INT," +
                                "  l LONG," +
                                "  f FLOAT," +
                                "  d DOUBLE," +
                                "  dt DATE," +
                                "  ts TIMESTAMP," +
                                "  s STRING," +
                                "  v VARCHAR," +
                                "  sy SYMBOL" +
                                ")"
                );
                // Row 0: all set
                serverMain.execute(
                        "INSERT INTO allp VALUES (" +
                                "  true, 127, 32767, 'A', 999, 999999999999L, 1.5f, 3.14, " +
                                "  '2024-01-01'::DATE, '2024-01-01T00:00:00.000Z', 'hello', 'world', 'SYM1'" +
                                ")"
                );
                // Row 1: nulls (only types that can hold NULL)
                serverMain.execute(
                        "INSERT INTO allp VALUES (" +
                                "  false, 0, 0, 'B', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL" +
                                ")"
                );

                final Object[][] row = new Object[2][];
                try (QwpQueryClient client = QwpQueryClient.newPlainText("127.0.0.1", HTTP_PORT)) {
                    client.connect();
                    client.execute("SELECT * FROM allp", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            Assert.assertEquals(13, batch.getColumnCount());
                            Assert.assertEquals(2, batch.getRowCount());
                            for (int r = 0; r < 2; r++) {
                                Object[] cells = new Object[13];
                                for (int c = 0; c < 13; c++) {
                                    cells[c] = batch.isNull(c, r) ? null : batch.getValue(c, r);
                                }
                                row[r] = cells;
                            }
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("egress query error: " + message);
                        }
                    });
                }

                // Row 0 assertions
                Assert.assertEquals(Boolean.TRUE, row[0][0]);                        // BOOLEAN
                Assert.assertEquals(127L, row[0][1]);                                // BYTE
                Assert.assertEquals(32767L, row[0][2]);                              // SHORT
                Assert.assertEquals((long) 'A', row[0][3]);                          // CHAR
                Assert.assertEquals(999L, row[0][4]);                                // INT
                Assert.assertEquals(999_999_999_999L, row[0][5]);                    // LONG
                Assert.assertEquals(1.5f, (Float) row[0][6], 0.0f);                  // FLOAT
                Assert.assertEquals(3.14, (Double) row[0][7], 1e-9);                 // DOUBLE
                Assert.assertTrue((Long) row[0][8] > 0);                             // DATE
                Assert.assertTrue((Long) row[0][9] > 0);                             // TIMESTAMP
                Assert.assertEquals("hello", row[0][10]);                            // STRING
                Assert.assertArrayEquals("world".getBytes(), (byte[]) row[0][11]);   // VARCHAR
                Assert.assertEquals("SYM1", row[0][12]);                             // SYMBOL

                // Row 1: NULL-capable types come back as null
                Assert.assertNull(row[1][4]);  // INT
                Assert.assertNull(row[1][5]);  // LONG
                Assert.assertNull(row[1][6]);  // FLOAT
                Assert.assertNull(row[1][7]);  // DOUBLE
                Assert.assertNull(row[1][8]);  // DATE
                Assert.assertNull(row[1][9]);  // TIMESTAMP
                Assert.assertNull(row[1][10]); // STRING
                Assert.assertNull(row[1][11]); // VARCHAR
                Assert.assertNull(row[1][12]); // SYMBOL
                // BOOLEAN/BYTE/SHORT/CHAR cannot represent NULL in QuestDB — stored values round-trip.
            }
        });
    }

    @Test
    public void testUuidAndLong256() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE wide(u UUID, l256 LONG256)");
                serverMain.execute(
                        "INSERT INTO wide VALUES ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', CAST('0x01' AS LONG256))"
                );
                serverMain.execute(
                        "INSERT INTO wide VALUES (CAST(NULL AS UUID), CAST(NULL AS LONG256))"
                );

                final Object[][] rows = new Object[2][];
                try (QwpQueryClient client = QwpQueryClient.newPlainText("127.0.0.1", HTTP_PORT)) {
                    client.connect();
                    client.execute("SELECT * FROM wide", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            Assert.assertEquals(2, batch.getColumnCount());
                            Assert.assertEquals(2, batch.getRowCount());
                            for (int r = 0; r < 2; r++) {
                                rows[r] = new Object[]{
                                        batch.isNull(0, r) ? null : batch.getLongArray(0, r),
                                        batch.isNull(1, r) ? null : batch.getLongArray(1, r)
                                };
                            }
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("egress query error: " + message);
                        }
                    });
                }
                // UUID round-trip: 2 longs (lo, hi)
                Assert.assertNotNull(rows[0][0]);
                Assert.assertEquals(2, ((long[]) rows[0][0]).length);
                // LONG256 round-trip: 4 longs, least significant first. 0x01 → {1, 0, 0, 0}
                long[] l256 = (long[]) rows[0][1];
                Assert.assertEquals(1L, l256[0]);
                Assert.assertEquals(0L, l256[1]);
                Assert.assertEquals(0L, l256[2]);
                Assert.assertEquals(0L, l256[3]);
                // NULL rows
                Assert.assertNull(rows[1][0]);
                Assert.assertNull(rows[1][1]);
            }
        });
    }

    @Test
    public void testGeohash() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE geo(g20 GEOHASH(20b), g40 GEOHASH(40b))");
                // Geohash literal: #<base-32-chars>; 4 chars = 20 bits, 8 chars = 40 bits
                serverMain.execute("INSERT INTO geo VALUES (#dr5r, #dr5rsjut)");
                serverMain.execute(
                        "INSERT INTO geo VALUES (CAST(NULL AS GEOHASH(20b)), CAST(NULL AS GEOHASH(40b)))"
                );

                final Long[] g20Values = new Long[2];
                final Long[] g40Values = new Long[2];
                final int[] precisionBits = new int[2];
                try (QwpQueryClient client = QwpQueryClient.newPlainText("127.0.0.1", HTTP_PORT)) {
                    client.connect();
                    client.execute("SELECT g20, g40 FROM geo", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            precisionBits[0] = batch.getGeohashPrecisionBits(0);
                            precisionBits[1] = batch.getGeohashPrecisionBits(1);
                            for (int r = 0; r < batch.getRowCount(); r++) {
                                g20Values[r] = batch.isNull(0, r) ? null : batch.getLong(0, r);
                                g40Values[r] = batch.isNull(1, r) ? null : batch.getLong(1, r);
                            }
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("egress query error: " + message);
                        }
                    });
                }
                Assert.assertEquals(20, precisionBits[0]);
                Assert.assertEquals(40, precisionBits[1]);
                Assert.assertNotNull(g20Values[0]);
                Assert.assertNotNull(g40Values[0]);
                Assert.assertNull(g20Values[1]);
                Assert.assertNull(g40Values[1]);
            }
        });
    }

    @Test
    public void testDecimal() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE dec(d64 DECIMAL(18,4), d128 DECIMAL(38,6))");
                serverMain.execute("INSERT INTO dec VALUES (1234.5678m, 987654321.123456m)");
                serverMain.execute("INSERT INTO dec VALUES (CAST(NULL AS DECIMAL(18,4)), CAST(NULL AS DECIMAL(38,6)))");

                final Long[] d64 = new Long[2];
                final long[][] d128 = new long[2][];
                try (QwpQueryClient client = QwpQueryClient.newPlainText("127.0.0.1", HTTP_PORT)) {
                    client.connect();
                    client.execute("SELECT d64, d128 FROM dec", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            for (int r = 0; r < batch.getRowCount(); r++) {
                                d64[r] = batch.isNull(0, r) ? null : batch.getLong(0, r);
                                d128[r] = batch.isNull(1, r) ? null : batch.getLongArray(1, r);
                            }
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("egress query error: " + message);
                        }
                    });
                }
                // 1234.5678 with scale 4 → unscaled = 12345678
                Assert.assertEquals(Long.valueOf(12_345_678L), d64[0]);
                Assert.assertNull(d64[1]);
                Assert.assertNotNull(d128[0]);
                Assert.assertNull(d128[1]);
            }
        });
    }

    @Test
    public void testLargeResultSet() throws Exception {
        // 20,000 rows → spans at least 5 batches with MAX_ROWS_PER_BATCH=4096.
        // Exercises: schema-reference mode (mode 0x01) after the first batch,
        // client recv-buffer growth, multi-batch reassembly on decode loop.
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE big(x LONG)");
                // Generate 20,000 rows in one INSERT via long_sequence.
                serverMain.execute(
                        "INSERT INTO big SELECT x FROM long_sequence(20000)"
                );

                int[] count = {0};
                int[] batches = {0};
                long[] sum = {0};
                try (QwpQueryClient client = QwpQueryClient.newPlainText("127.0.0.1", HTTP_PORT)) {
                    client.connect();
                    client.execute("SELECT x FROM big", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            batches[0]++;
                            for (int r = 0; r < batch.getRowCount(); r++) {
                                sum[0] += batch.getLong(0, r);
                                count[0]++;
                            }
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("egress query error: " + message);
                        }
                    });
                }
                Assert.assertEquals(20_000, count[0]);
                Assert.assertTrue("expected multi-batch streaming, got " + batches[0] + " batches",
                        batches[0] > 1);
                // sum(1..20000) = n(n+1)/2 = 20000 * 20001 / 2
                Assert.assertEquals(200_010_000L, sum[0]);
            }
        });
    }

    @Test
    public void testSqlSyntaxError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                final byte[] errorStatus = {(byte) 0xFF};
                final String[] errorMsg = {null};
                try (QwpQueryClient client = QwpQueryClient.newPlainText("127.0.0.1", HTTP_PORT)) {
                    client.connect();
                    // Missing table name after FROM → syntax error at a specific position.
                    client.execute("SELECT * FROM", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            Assert.fail("unexpected batch on malformed SQL");
                        }

                        @Override
                        public void onEnd(long totalRows) {
                            Assert.fail("unexpected end on malformed SQL");
                        }

                        @Override
                        public void onError(byte status, String message) {
                            errorStatus[0] = status;
                            errorMsg[0] = message;
                        }
                    });
                }
                Assert.assertNotNull("expected error message", errorMsg[0]);
                Assert.assertEquals("PARSE_ERROR status expected", 0x05, errorStatus[0]);
                // SqlException.getMessage() format: "[<position>] <text>"
                Assert.assertTrue(
                        "message should contain position marker, got: " + errorMsg[0],
                        errorMsg[0].startsWith("[") && errorMsg[0].contains("]")
                );
            }
        });
    }

    @Test
    public void testTableNotFound() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                final String[] errorMsg = {null};
                try (QwpQueryClient client = QwpQueryClient.newPlainText("127.0.0.1", HTTP_PORT)) {
                    client.connect();
                    client.execute("SELECT * FROM no_such_table", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            Assert.fail("unexpected batch");
                        }

                        @Override
                        public void onEnd(long totalRows) {
                            Assert.fail("unexpected end");
                        }

                        @Override
                        public void onError(byte status, String message) {
                            errorMsg[0] = message;
                        }
                    });
                }
                Assert.assertNotNull(errorMsg[0]);
                // Expect the message to reference the unknown table
                Assert.assertTrue(
                        "message should mention the unknown table, got: " + errorMsg[0],
                        errorMsg[0].toLowerCase().contains("no_such_table")
                                || errorMsg[0].toLowerCase().contains("table does not exist")
                                || errorMsg[0].toLowerCase().contains("not found")
                );
            }
        });
    }

    @Test
    public void testNonSelectRejected() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE dummy(x LONG)");
                final byte[] status = {0};
                final String[] msg = {null};
                try (QwpQueryClient client = QwpQueryClient.newPlainText("127.0.0.1", HTTP_PORT)) {
                    client.connect();
                    // DROP is a DDL, not a SELECT — Phase 1 rejects with PARSE_ERROR.
                    client.execute("DROP TABLE dummy", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            Assert.fail("unexpected batch");
                        }

                        @Override
                        public void onEnd(long totalRows) {
                            Assert.fail("unexpected end");
                        }

                        @Override
                        public void onError(byte s, String m) {
                            status[0] = s;
                            msg[0] = m;
                        }
                    });
                }
                Assert.assertEquals(0x05, status[0]);
                Assert.assertNotNull(msg[0]);
                Assert.assertTrue(
                        "message should indicate SELECT-only restriction, got: " + msg[0],
                        msg[0].toLowerCase().contains("select")
                );
            }
        });
    }

    @Test
    public void testFromConfigConnectString() throws Exception {
        // The fromConfig(String) factory mirrors Sender.fromConfig: schema::key=value;...
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE cs(x LONG)");
                serverMain.execute("INSERT INTO cs VALUES (42), (43)");

                List<Long> rows = new ArrayList<>();
                String conf = "ws::addr=127.0.0.1:" + HTTP_PORT + ";path=/read/v1;client_id=conf-test/1.0;buffer_pool_size=2;";
                try (QwpQueryClient client = QwpQueryClient.fromConfig(conf)) {
                    client.connect();
                    client.execute("SELECT x FROM cs ORDER BY x", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            for (int r = 0; r < batch.getRowCount(); r++) {
                                rows.add(batch.getLong(0, r));
                            }
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("egress query error: " + message);
                        }
                    });
                }
                Assert.assertEquals(List.of(42L, 43L), rows);
            }
        });
    }

    @Test
    public void testFromConfigRejectsBadSchema() {
        try {
            QwpQueryClient.fromConfig("http::addr=localhost:9000;");
            Assert.fail("expected unsupported-schema error");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("unsupported schema"));
        }
        try {
            QwpQueryClient.fromConfig("ws::");
            Assert.fail("expected missing-addr error");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("addr"));
        }
        try {
            QwpQueryClient.fromConfig("ws::addr=h:9000;buffer_pool_size=0;");
            Assert.fail("expected bad pool-size error");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("buffer_pool_size"));
        }
        try {
            QwpQueryClient.fromConfig("wss::addr=h:9000;");
            Assert.fail("expected wss-not-supported error");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("wss"));
        }
    }

    @Test
    public void testIpv4NullSentinel() throws Exception {
        // Regression: IPv4 maps to wire TYPE_INT but uses 0 (Numbers.IPv4_NULL) as the
        // null sentinel, not Integer.MIN_VALUE. The egress server must check 0, not
        // INT_NULL, otherwise a NULL row would ship as the valid bit pattern 0 and
        // appear non-null on the wire. (QuestDB itself treats 0.0.0.0 as NULL — there
        // is no way to store a "real" 0.0.0.0 — so both '0.0.0.0' and NULL inserts
        // must come back as null.)
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE ipx(addr IPv4)");
                serverMain.execute("INSERT INTO ipx VALUES " +
                        "('0.0.0.0'), " +
                        "(NULL), " +
                        "('192.168.1.1')");

                final boolean[] nullSeen = new boolean[3];
                final int[] count = {0};
                try (QwpQueryClient client = QwpQueryClient.newPlainText("127.0.0.1", HTTP_PORT)) {
                    client.connect();
                    client.execute("SELECT addr FROM ipx", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            for (int r = 0; r < batch.getRowCount(); r++, count[0]++) {
                                nullSeen[count[0]] = batch.isNull(0, r);
                            }
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("egress query error: " + message);
                        }
                    });
                }
                Assert.assertEquals(3, count[0]);
                Assert.assertTrue("'0.0.0.0' is the IPv4 NULL sentinel — must surface as null", nullSeen[0]);
                Assert.assertTrue("explicit NULL must surface as null", nullSeen[1]);
                Assert.assertFalse("real address must surface as non-null", nullSeen[2]);
            }
        });
    }

    @Test
    public void testSelectWithNulls() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE n(x LONG, s STRING)");
                serverMain.execute("INSERT INTO n VALUES (1, 'a'), (NULL, NULL), (3, 'c')");

                List<Object[]> rows = new ArrayList<>();
                try (QwpQueryClient client = QwpQueryClient.newPlainText("127.0.0.1", HTTP_PORT)) {
                    client.connect();
                    client.execute("SELECT x, s FROM n", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            for (int r = 0; r < batch.getRowCount(); r++) {
                                rows.add(new Object[]{
                                        batch.isNull(0, r) ? null : batch.getLong(0, r),
                                        batch.getString(1, r)
                                });
                            }
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("egress query error: status=" + status + " msg=" + message);
                        }
                    });
                }
                Assert.assertEquals(3, rows.size());
                Assert.assertEquals(1L, rows.get(0)[0]);
                Assert.assertEquals("a", rows.get(0)[1]);
                Assert.assertNull(rows.get(1)[0]);
                Assert.assertNull(rows.get(1)[1]);
                Assert.assertEquals(3L, rows.get(2)[0]);
                Assert.assertEquals("c", rows.get(2)[1]);
            }
        });
    }
}
