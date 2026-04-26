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

import io.questdb.client.cutlass.qwp.client.QwpBindSetter;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;
import io.questdb.client.cutlass.qwp.client.QwpQueryClient;
import io.questdb.client.cutlass.qwp.client.WebSocketResponse;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.UUID;

/**
 * End-to-end bind-parameter round-trip tests. Every supported scalar bind
 * type runs through: create a table of the target type, insert a known value,
 * issue a SELECT bound to that value via {@link QwpQueryClient#execute(String, QwpBindSetter, QwpColumnBatchHandler)},
 * assert the server sees the correct bind and returns the expected row.
 * <p>
 * NULL-bind paths use the projection form {@code SELECT CAST($1 AS T) AS v}
 * rather than a filter (SQL NULL does not compare equal to NULL).
 * <p>
 * Type coverage mirrors the server-side {@code QwpEgressRequestDecoder}
 * switch: BOOLEAN / BYTE / SHORT / CHAR / INT / LONG / FLOAT / DOUBLE /
 * TIMESTAMP / TIMESTAMP_NANOS / DATE / VARCHAR / UUID / LONG256 / GEOHASH /
 * DECIMAL64 / DECIMAL128 / DECIMAL256. ARRAY, BINARY, and IPv4 are rejected
 * server-side and verified here via the client's setNull guard.
 */
public class QwpEgressBindRoundTripTest extends AbstractBootstrapTest {

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testBindBooleanFilter() throws Exception {
        runFilterMatch(
                "CREATE TABLE t(c BOOLEAN, part_ts TIMESTAMP) TIMESTAMP(part_ts) PARTITION BY DAY WAL",
                "INSERT INTO t VALUES (true, 1::TIMESTAMP), (false, 2::TIMESTAMP)",
                "SELECT c FROM t WHERE c = $1",
                binds -> binds.setBoolean(0, true),
                batch -> {
                    Assert.assertEquals(1, batch.getRowCount());
                    Assert.assertTrue(batch.getBoolValue(0, 0));
                }
        );
    }

    @Test
    public void testBindByteFilter() throws Exception {
        runFilterMatch(
                "CREATE TABLE t(c BYTE, part_ts TIMESTAMP) TIMESTAMP(part_ts) PARTITION BY DAY WAL",
                "INSERT INTO t VALUES (127, 1::TIMESTAMP), (-128, 2::TIMESTAMP), (0, 3::TIMESTAMP)",
                "SELECT c FROM t WHERE c = $1",
                binds -> binds.setByte(0, (byte) -128),
                batch -> {
                    Assert.assertEquals(1, batch.getRowCount());
                    Assert.assertEquals((byte) -128, batch.getByteValue(0, 0));
                }
        );
    }

    @Test
    public void testBindCharFilter() throws Exception {
        runFilterMatch(
                "CREATE TABLE t(c CHAR, part_ts TIMESTAMP) TIMESTAMP(part_ts) PARTITION BY DAY WAL",
                "INSERT INTO t VALUES ('A', 1::TIMESTAMP), ('Z', 2::TIMESTAMP)",
                "SELECT c FROM t WHERE c = $1",
                binds -> binds.setChar(0, 'Z'),
                batch -> {
                    Assert.assertEquals(1, batch.getRowCount());
                    Assert.assertEquals('Z', batch.getCharValue(0, 0));
                }
        );
    }

    @Test
    public void testBindDateFilter() throws Exception {
        long knownMs = 1_700_000_000_000L;
        runFilterMatch(
                "CREATE TABLE t(c DATE, part_ts TIMESTAMP) TIMESTAMP(part_ts) PARTITION BY DAY WAL",
                "INSERT INTO t VALUES (" + knownMs + "::DATE, 1::TIMESTAMP), (0::DATE, 2::TIMESTAMP)",
                "SELECT c FROM t WHERE c = $1",
                binds -> binds.setDate(0, knownMs),
                batch -> {
                    Assert.assertEquals(1, batch.getRowCount());
                    Assert.assertEquals(knownMs, batch.getLongValue(0, 0));
                }
        );
    }

    @Test
    public void testBindDecimal128Filter() throws Exception {
        runFilterMatch(
                "CREATE TABLE t(c DECIMAL(38,6), part_ts TIMESTAMP) TIMESTAMP(part_ts) PARTITION BY DAY WAL",
                "INSERT INTO t VALUES (123.456789m, 1::TIMESTAMP), (999.999999m, 2::TIMESTAMP)",
                "SELECT c FROM t WHERE c = $1",
                // 123.456789 at scale 6 -> unscaled 123456789.
                binds -> binds.setDecimal128(0, 6, 123456789L, 0L),
                batch -> {
                    Assert.assertEquals(1, batch.getRowCount());
                    Assert.assertEquals(QwpConstants.TYPE_DECIMAL128, batch.getColumnWireType(0));
                    Assert.assertEquals(6, batch.getDecimalScale(0));
                    Assert.assertEquals(123456789L, batch.getDecimal128Low(0, 0));
                    Assert.assertEquals(0L, batch.getDecimal128High(0, 0));
                }
        );
    }

    @Test
    public void testBindDecimal256Filter() throws Exception {
        runFilterMatch(
                "CREATE TABLE t(c DECIMAL(76,10), part_ts TIMESTAMP) TIMESTAMP(part_ts) PARTITION BY DAY WAL",
                "INSERT INTO t VALUES (42.0m, 1::TIMESTAMP), (7.5m, 2::TIMESTAMP)",
                "SELECT c FROM t WHERE c = $1",
                // 42.0 at scale 10 -> unscaled 42 * 10^10 = 420_000_000_000
                binds -> binds.setDecimal256(0, 10, 420_000_000_000L, 0L, 0L, 0L),
                batch -> {
                    Assert.assertEquals(1, batch.getRowCount());
                    Assert.assertEquals(QwpConstants.TYPE_DECIMAL256, batch.getColumnWireType(0));
                    Assert.assertEquals(10, batch.getDecimalScale(0));
                }
        );
    }

    @Test
    public void testBindDecimal64Filter() throws Exception {
        runFilterMatch(
                "CREATE TABLE t(c DECIMAL(18,4), part_ts TIMESTAMP) TIMESTAMP(part_ts) PARTITION BY DAY WAL",
                "INSERT INTO t VALUES (12345.6789m, 1::TIMESTAMP), (99999.9999m, 2::TIMESTAMP)",
                "SELECT c FROM t WHERE c = $1",
                // 12345.6789 at scale 4 -> unscaled 123_456_789
                binds -> binds.setDecimal64(0, 4, 123_456_789L),
                batch -> {
                    Assert.assertEquals(1, batch.getRowCount());
                    Assert.assertEquals(QwpConstants.TYPE_DECIMAL64, batch.getColumnWireType(0));
                    Assert.assertEquals(4, batch.getDecimalScale(0));
                    Assert.assertEquals(123_456_789L, batch.getLongValue(0, 0));
                }
        );
    }

    @Test
    public void testBindDoubleFilter() throws Exception {
        runFilterMatch(
                "CREATE TABLE t(c DOUBLE, part_ts TIMESTAMP) TIMESTAMP(part_ts) PARTITION BY DAY WAL",
                "INSERT INTO t VALUES (2.718281828, 1::TIMESTAMP), (3.14, 2::TIMESTAMP)",
                "SELECT c FROM t WHERE c = $1",
                binds -> binds.setDouble(0, 2.718281828),
                batch -> {
                    Assert.assertEquals(1, batch.getRowCount());
                    Assert.assertEquals(2.718281828, batch.getDoubleValue(0, 0), 0.0);
                }
        );
    }

    @Test
    public void testBindErrorSurfacesViaOnError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE t(c LONG, part_ts TIMESTAMP) TIMESTAMP(part_ts) PARTITION BY DAY WAL");
                serverMain.awaitTable("t");

                final byte[] status = {Byte.MIN_VALUE};
                final String[] msg = {null};
                try (QwpQueryClient client = QwpQueryClient.fromConfig("ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    client.execute(
                            "SELECT c FROM t WHERE c = $1",
                            binds -> binds.setGeohash(0, 999, 0L),  // invalid precision
                            new QwpColumnBatchHandler() {
                                @Override
                                public void onBatch(QwpColumnBatch batch) {
                                    Assert.fail("unexpected batch: bind encode should have failed");
                                }

                                @Override
                                public void onEnd(long totalRows) {
                                    Assert.fail("unexpected onEnd: bind encode should have failed");
                                }

                                @Override
                                public void onError(byte s, String m) {
                                    status[0] = s;
                                    msg[0] = m;
                                }
                            }
                    );
                }
                Assert.assertEquals(WebSocketResponse.STATUS_INTERNAL_ERROR, status[0]);
                Assert.assertNotNull(msg[0]);
                Assert.assertTrue("message: " + msg[0], msg[0].contains("bind encoding failed"));
            }
        });
    }

    @Test
    public void testBindFloatFilter() throws Exception {
        runFilterMatch(
                "CREATE TABLE t(c FLOAT, part_ts TIMESTAMP) TIMESTAMP(part_ts) PARTITION BY DAY WAL",
                "INSERT INTO t VALUES (3.14f, 1::TIMESTAMP), (1.0f, 2::TIMESTAMP)",
                "SELECT c FROM t WHERE c = $1",
                binds -> binds.setFloat(0, 3.14f),
                batch -> {
                    Assert.assertEquals(1, batch.getRowCount());
                    Assert.assertEquals(3.14f, batch.getFloatValue(0, 0), 0.0f);
                }
        );
    }

    @Test
    public void testBindGeohashFilter() throws Exception {
        long value = 0x0FFFFFFFFFFFFFFFL;
        runFilterMatch(
                "CREATE TABLE t(c GEOHASH(60b), part_ts TIMESTAMP) TIMESTAMP(part_ts) PARTITION BY DAY WAL",
                "INSERT INTO t VALUES (cast('zzzzzzzzzzzz' AS GEOHASH(60b)), 1::TIMESTAMP)",
                "SELECT c FROM t WHERE c = $1::GEOHASH(60b)",
                binds -> binds.setGeohash(0, 60, value),
                batch -> Assert.assertEquals(1, batch.getRowCount())
        );
    }

    @Test
    public void testBindIntFilter() throws Exception {
        // Integer.MIN_VALUE is QuestDB's INT NULL sentinel, so use MIN_VALUE + 1
        // as the "min non-null" marker that round-trips faithfully.
        int minNonNull = Integer.MIN_VALUE + 1;
        runFilterMatch(
                "CREATE TABLE t(c INT, part_ts TIMESTAMP) TIMESTAMP(part_ts) PARTITION BY DAY WAL",
                "INSERT INTO t VALUES (2147483647, 1::TIMESTAMP), (" + minNonNull + ", 2::TIMESTAMP), (0, 3::TIMESTAMP)",
                "SELECT c FROM t WHERE c = $1",
                binds -> binds.setInt(0, minNonNull),
                batch -> {
                    Assert.assertEquals(1, batch.getRowCount());
                    Assert.assertEquals(minNonNull, batch.getIntValue(0, 0));
                }
        );
    }

    @Test
    public void testBindLongFilter() throws Exception {
        runFilterMatch(
                "CREATE TABLE t(c LONG, part_ts TIMESTAMP) TIMESTAMP(part_ts) PARTITION BY DAY WAL",
                "INSERT INTO t VALUES (9223372036854775807, 1::TIMESTAMP), (1, 2::TIMESTAMP), (0, 3::TIMESTAMP)",
                "SELECT c FROM t WHERE c = $1",
                binds -> binds.setLong(0, Long.MAX_VALUE),
                batch -> {
                    Assert.assertEquals(1, batch.getRowCount());
                    Assert.assertEquals(Long.MAX_VALUE, batch.getLongValue(0, 0));
                }
        );
    }

    @Test
    public void testBindMultipleParametersFilter() throws Exception {
        runFilterMatch(
                "CREATE TABLE t(a LONG, b VARCHAR, c DOUBLE, part_ts TIMESTAMP) TIMESTAMP(part_ts) PARTITION BY DAY WAL",
                "INSERT INTO t VALUES (1, 'AAPL', 100.0, 1::TIMESTAMP), (2, 'MSFT', 200.0, 2::TIMESTAMP), (3, 'GOOG', 300.0, 3::TIMESTAMP)",
                "SELECT a FROM t WHERE b = $1 AND c > $2",
                binds -> binds.setVarchar(0, "MSFT").setDouble(1, 150.0),
                batch -> {
                    Assert.assertEquals(1, batch.getRowCount());
                    Assert.assertEquals(2L, batch.getLongValue(0, 0));
                }
        );
    }

    @Test
    public void testBindNoBindsViaNullLambda() throws Exception {
        // Sanity: when the user passes the 3-arg form with no setter, the wire
        // still encodes bind_count=0. Identical to the 2-arg overload.
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE t(c LONG, part_ts TIMESTAMP) TIMESTAMP(part_ts) PARTITION BY DAY WAL");
                serverMain.execute("INSERT INTO t VALUES (42, 1::TIMESTAMP)");
                serverMain.awaitTable("t");

                final long[] value = {Long.MIN_VALUE};
                try (QwpQueryClient client = QwpQueryClient.fromConfig("ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    client.execute(
                            "SELECT c FROM t",
                            null,
                            batchHandler(batch -> value[0] = batch.getLongValue(0, 0))
                    );
                }
                Assert.assertEquals(42L, value[0]);
            }
        });
    }

    @Test
    public void testBindNullBoolean() throws Exception {
        runProjectionNull(
                "SELECT CAST($1 AS BOOLEAN) AS v FROM long_sequence(1)",
                binds -> binds.setNull(0, QwpConstants.TYPE_BOOLEAN),
                QwpConstants.TYPE_BOOLEAN,
                // BOOLEAN cannot be NULL in QuestDB; NULL bind reads back as false.
                batch -> Assert.assertFalse(batch.getBoolValue(0, 0))
        );
    }

    @Test
    public void testBindNullDate() throws Exception {
        runProjectionNull(
                "SELECT CAST($1 AS DATE) AS v FROM long_sequence(1)",
                binds -> binds.setNull(0, QwpConstants.TYPE_DATE),
                QwpConstants.TYPE_DATE,
                batch -> Assert.assertTrue(batch.isNull(0, 0))
        );
    }

    @Test
    public void testBindNullInt() throws Exception {
        runProjectionNull(
                "SELECT CAST($1 AS INT) AS v FROM long_sequence(1)",
                binds -> binds.setNull(0, QwpConstants.TYPE_INT),
                QwpConstants.TYPE_INT,
                batch -> Assert.assertTrue(batch.isNull(0, 0))
        );
    }

    @Test
    public void testBindNullLong() throws Exception {
        runProjectionNull(
                "SELECT CAST($1 AS LONG) AS v FROM long_sequence(1)",
                binds -> binds.setNull(0, QwpConstants.TYPE_LONG),
                QwpConstants.TYPE_LONG,
                batch -> Assert.assertTrue(batch.isNull(0, 0))
        );
    }

    @Test
    public void testBindNullTimestamp() throws Exception {
        runProjectionNull(
                "SELECT CAST($1 AS TIMESTAMP) AS v FROM long_sequence(1)",
                binds -> binds.setNull(0, QwpConstants.TYPE_TIMESTAMP),
                QwpConstants.TYPE_TIMESTAMP,
                batch -> Assert.assertTrue(batch.isNull(0, 0))
        );
    }

    @Test
    public void testBindNullUuid() throws Exception {
        runProjectionNull(
                "SELECT CAST($1 AS UUID) AS v FROM long_sequence(1)",
                binds -> binds.setUuid(0, (UUID) null),
                QwpConstants.TYPE_UUID,
                batch -> Assert.assertTrue(batch.isNull(0, 0))
        );
    }

    @Test
    public void testBindNullVarchar() throws Exception {
        runProjectionNull(
                "SELECT CAST($1 AS VARCHAR) AS v FROM long_sequence(1)",
                binds -> binds.setVarchar(0, null),
                QwpConstants.TYPE_VARCHAR,
                batch -> Assert.assertTrue(batch.isNull(0, 0))
        );
    }

    @Test
    public void testBindSameSqlDifferentBindsFactoryCacheReuse() throws Exception {
        // Runs the exact same SQL text with different bind values across many
        // iterations. Asserts each result is the value just bound. The server's
        // SQL-text-keyed factory cache compiles once on the first call; every
        // subsequent call with the same text reuses the cached factory.
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE t(id LONG, part_ts TIMESTAMP) TIMESTAMP(part_ts) PARTITION BY DAY WAL");
                serverMain.execute("INSERT INTO t VALUES (1, 1::TIMESTAMP), (2, 2::TIMESTAMP), (3, 3::TIMESTAMP), (4, 4::TIMESTAMP), (5, 5::TIMESTAMP)");
                serverMain.awaitTable("t");

                try (QwpQueryClient client = QwpQueryClient.fromConfig("ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    String sql = "SELECT id FROM t WHERE id = $1";
                    for (int i = 1; i <= 5; i++) {
                        final long target = i;
                        final long[] observed = {-1L};
                        client.execute(sql, binds -> binds.setLong(0, target), new QwpColumnBatchHandler() {
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
                                Assert.fail("iteration " + target + ": " + message);
                            }
                        });
                        Assert.assertEquals("iteration " + target, target, observed[0]);
                    }
                }
            }
        });
    }

    @Test
    public void testBindShortFilter() throws Exception {
        runFilterMatch(
                "CREATE TABLE t(c SHORT, part_ts TIMESTAMP) TIMESTAMP(part_ts) PARTITION BY DAY WAL",
                "INSERT INTO t VALUES (32767, 1::TIMESTAMP), (-32768, 2::TIMESTAMP), (0, 3::TIMESTAMP)",
                "SELECT c FROM t WHERE c = $1",
                binds -> binds.setShort(0, Short.MIN_VALUE),
                batch -> {
                    Assert.assertEquals(1, batch.getRowCount());
                    Assert.assertEquals(Short.MIN_VALUE, batch.getShortValue(0, 0));
                }
        );
    }

    @Test
    public void testBindTimestampMicrosFilter() throws Exception {
        long micros = 1_700_000_000_000_000L;
        runFilterMatch(
                "CREATE TABLE t(c TIMESTAMP, part_ts TIMESTAMP) TIMESTAMP(part_ts) PARTITION BY DAY WAL",
                "INSERT INTO t VALUES (" + micros + "::TIMESTAMP, 1::TIMESTAMP), (0::TIMESTAMP, 2::TIMESTAMP)",
                "SELECT c FROM t WHERE c = $1",
                binds -> binds.setTimestampMicros(0, micros),
                batch -> {
                    Assert.assertEquals(1, batch.getRowCount());
                    Assert.assertEquals(micros, batch.getLongValue(0, 0));
                }
        );
    }

    @Test
    public void testBindTimestampNanosDistinctFromMicros() throws Exception {
        // Projection-form test: confirms TIMESTAMP_NANOS is tagged distinctly
        // from TIMESTAMP (micros). Routing through setTimestamp would produce
        // a value 1000x off.
        long nanos = 1_700_000_000_123_456_789L;
        runProjectionNonNull(
                "SELECT CAST($1 AS TIMESTAMP_NS) AS v FROM long_sequence(1)",
                binds -> binds.setTimestampNanos(0, nanos),
                batch -> {
                    Assert.assertEquals(QwpConstants.TYPE_TIMESTAMP_NANOS, batch.getColumnWireType(0));
                    Assert.assertEquals(nanos, batch.getLongValue(0, 0));
                }
        );
    }

    @Test
    public void testBindUuidFilter() throws Exception {
        UUID uuid = UUID.fromString("123e4567-e89b-12d3-a456-426614174000");
        runFilterMatch(
                "CREATE TABLE t(c UUID, part_ts TIMESTAMP) TIMESTAMP(part_ts) PARTITION BY DAY WAL",
                "INSERT INTO t VALUES ('123e4567-e89b-12d3-a456-426614174000'::UUID, 1::TIMESTAMP), " +
                        "('00000000-0000-0000-0000-000000000001'::UUID, 2::TIMESTAMP)",
                "SELECT c FROM t WHERE c = $1",
                binds -> binds.setUuid(0, uuid),
                batch -> Assert.assertEquals(1, batch.getRowCount())
        );
    }

    @Test
    public void testBindVarcharAsciiFilter() throws Exception {
        runFilterMatch(
                "CREATE TABLE t(c VARCHAR, part_ts TIMESTAMP) TIMESTAMP(part_ts) PARTITION BY DAY WAL",
                "INSERT INTO t VALUES ('hello', 1::TIMESTAMP), ('world', 2::TIMESTAMP), ('', 3::TIMESTAMP)",
                "SELECT c FROM t WHERE c = $1",
                binds -> binds.setVarchar(0, "world"),
                batch -> {
                    Assert.assertEquals(1, batch.getRowCount());
                    Assert.assertEquals("world", batch.getString(0, 0));
                }
        );
    }

    @Test
    public void testBindVarcharEmptyFilter() throws Exception {
        runFilterMatch(
                "CREATE TABLE t(c VARCHAR, part_ts TIMESTAMP) TIMESTAMP(part_ts) PARTITION BY DAY WAL",
                "INSERT INTO t VALUES ('x', 1::TIMESTAMP), ('', 2::TIMESTAMP)",
                "SELECT c FROM t WHERE c = $1",
                binds -> binds.setVarchar(0, ""),
                batch -> {
                    Assert.assertEquals(1, batch.getRowCount());
                    Assert.assertEquals("", batch.getString(0, 0));
                }
        );
    }

    @Test
    public void testBindVarcharUnicodeFilter() throws Exception {
        runFilterMatch(
                "CREATE TABLE t(c VARCHAR, part_ts TIMESTAMP) TIMESTAMP(part_ts) PARTITION BY DAY WAL",
                "INSERT INTO t VALUES ('café', 1::TIMESTAMP), ('plain', 2::TIMESTAMP)",
                "SELECT c FROM t WHERE c = $1",
                binds -> binds.setVarchar(0, "café"),
                batch -> {
                    Assert.assertEquals(1, batch.getRowCount());
                    Assert.assertEquals("café", batch.getString(0, 0));
                }
        );
    }

    @Test
    public void testRepeatedExecuteResetsPriorBinds() throws Exception {
        // Executes two distinct queries on the same client back-to-back: the
        // bindValues scratch must reset between calls so the second query's
        // binds don't leak state from the first.
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE t(id LONG, part_ts TIMESTAMP) TIMESTAMP(part_ts) PARTITION BY DAY WAL");
                serverMain.execute("INSERT INTO t VALUES (1, 1::TIMESTAMP), (2, 2::TIMESTAMP), (3, 3::TIMESTAMP)");
                serverMain.awaitTable("t");

                try (QwpQueryClient client = QwpQueryClient.fromConfig("ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();

                    // First: 3 binds.
                    final long[] first = {-1L};
                    client.execute(
                            "SELECT id FROM t WHERE id IN ($1, $2, $3) ORDER BY id DESC LIMIT 1",
                            binds -> binds.setLong(0, 1).setLong(1, 2).setLong(2, 3),
                            batchHandler(batch -> first[0] = batch.getLongValue(0, 0))
                    );
                    Assert.assertEquals(3L, first[0]);

                    // Second: 1 bind. If the scratch didn't reset, bind_count
                    // would be 4 on the wire and the server would read garbage.
                    final long[] second = {-1L};
                    client.execute(
                            "SELECT id FROM t WHERE id = $1",
                            binds -> binds.setLong(0, 2),
                            batchHandler(batch -> second[0] = batch.getLongValue(0, 0))
                    );
                    Assert.assertEquals(2L, second[0]);
                }
            }
        });
    }

    private QwpColumnBatchHandler batchHandler(BatchConsumer body) {
        return new QwpColumnBatchHandler() {
            @Override
            public void onBatch(QwpColumnBatch batch) {
                body.consume(batch);
            }

            @Override
            public void onEnd(long totalRows) {
            }

            @Override
            public void onError(byte status, String message) {
                Assert.fail("egress query error: " + message);
            }
        };
    }

    private void runFilterMatch(
            String createSql,
            String insertSql,
            String selectSql,
            QwpBindSetter binds,
            BatchAsserter asserter
    ) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute(createSql);
                serverMain.execute(insertSql);
                serverMain.awaitTable("t");

                final boolean[] observed = {false};
                try (QwpQueryClient client = QwpQueryClient.fromConfig("ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    client.execute(selectSql, binds, new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            asserter.run(batch);
                            observed[0] = true;
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
                Assert.assertTrue("expected at least one batch", observed[0]);
            }
        });
    }

    private void runProjectionNonNull(
            String selectSql,
            QwpBindSetter binds,
            BatchAsserter asserter
    ) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain ignored = startWithEnvVariables()) {
                final boolean[] observed = {false};
                try (QwpQueryClient client = QwpQueryClient.fromConfig("ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    client.execute(selectSql, binds, new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            Assert.assertEquals(1, batch.getRowCount());
                            asserter.run(batch);
                            observed[0] = true;
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
                Assert.assertTrue("expected exactly one batch", observed[0]);
            }
        });
    }

    private void runProjectionNull(
            String selectSql,
            QwpBindSetter binds,
            byte expectedWireType,
            BatchAsserter nullAsserter
    ) throws Exception {
        runProjectionNonNull(selectSql, binds, batch -> {
            Assert.assertEquals(expectedWireType, batch.getColumnWireType(0));
            nullAsserter.run(batch);
        });
    }

    @FunctionalInterface
    private interface BatchAsserter {
        void run(QwpColumnBatch batch);
    }

    @FunctionalInterface
    private interface BatchConsumer {
        void consume(QwpColumnBatch batch);
    }
}
