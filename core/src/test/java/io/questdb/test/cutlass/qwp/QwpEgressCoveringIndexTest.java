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

import io.questdb.PropertyKey;
import io.questdb.client.cutlass.qwp.client.QwpBindSetter;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;
import io.questdb.client.cutlass.qwp.client.QwpQueryClient;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.std.Decimal128;
import io.questdb.client.std.Decimal256;
import io.questdb.client.std.Decimal64;
import io.questdb.griffin.engine.table.CoveringIndexRecordCursorFactory;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

/**
 * QWP egress over a POSTING index with INCLUDE columns (covering index). A single-key covering
 * scan ({@code sym = 'x'}) produces page frames that carry no column data: the covered columns
 * are decoded from the index sidecar only by the in-engine parallel decode. Each test checks the
 * values the client receives against the inserted data or against the same query under
 * {@code /*+ no_covering *}{@code /}, which reads the column files instead of the sidecar.
 * A multi-key scan ({@code sym IN (...)}) merges the keys into frames it fills itself, and the
 * stream frees each frame once it has copied its rows, so the tests that cap covering frames
 * at a few rows stream many frames and free them as they go.
 */
public class QwpEgressCoveringIndexTest extends AbstractReusedServerQwpEgressTest {

    // Every column type an INCLUDE list accepts, with NULL and boundary values. Rows 1, 3 and 5
    // have the 'A' key, row 2 'B' and row 4 a NULL key; row 5 holds NULL in every nullable column.
    // The decimal literals carry no thousands separators: the decimal parser rejects them.
    private static final String ALL_TYPES_DDL = """
            CREATE TABLE t_all (
                sym SYMBOL,
                b BOOLEAN, bt BYTE, sh SHORT, ch CHAR, i INT, l LONG, f FLOAT, d DOUBLE,
                dt DATE, ts2 TIMESTAMP, tsn TIMESTAMP_NS, ip IPV4,
                d2 DECIMAL(2,1), d4 DECIMAL(4,2), d9 DECIMAL(9,2), d18 DECIMAL(18,2), d38 DECIMAL(38,2), d76 DECIMAL(76,2),
                vc VARCHAR, s STRING, sy SYMBOL, u UUID, l256 LONG256,
                g1 GEOHASH(5b), g2 GEOHASH(2c), g4 GEOHASH(4c), g8 GEOHASH(8c),
                arr DOUBLE[], arr2 DOUBLE[][], bin BINARY,
                ts TIMESTAMP
            ) TIMESTAMP(ts) PARTITION BY DAY WAL
            """;
    private static final String ALL_TYPES_INCLUDE = "b, bt, sh, ch, i, l, f, d, dt, ts2, tsn, ip, d2, d4, d9, d18, d38, d76, "
            + "vc, s, sy, u, l256, g1, g2, g4, g8, arr, arr2, bin";
    private static final String ALL_TYPES_INSERT = """
            INSERT INTO t_all VALUES
                ('A', true, 127::BYTE, 32_767::SHORT, 'a', 2_147_483_647, 9_223_372_036_854_775_807, 3.4028234663852886E38::FLOAT, 1.7976931348623157E308,
                 '1969-12-31T23:59:59.999Z'::DATE, '1969-12-31T23:59:59.999999Z'::TIMESTAMP, '2200-01-01T00:00:00.123456789Z'::TIMESTAMP_NS, '255.255.255.255'::IPV4,
                 9.9m, 99.99m, 9999999.99m, 9999999999999999.99m,
                 999999999999999999999999999999999999.99m, 12345678901234567890123456789012345678901234567890.12m,
                 'ascii', 'str', 'x', '11111111-1111-1111-1111-111111111111', CAST('0x01' AS LONG256),
                 #u, #u3, #u33d, #u33d3d3d, ARRAY[1.0, -2.5], ARRAY[[1.0, 2.0], [3.0, 4.0]], NULL,
                 '2024-01-01T00:00:00.000000Z'),
                ('B', false, 1::BYTE, 1::SHORT, 'b', 1, 1, 1.0::FLOAT, 1.0,
                 '2024-01-01T00:00:00.000Z'::DATE, '2024-01-01T00:00:00.000000Z'::TIMESTAMP, '2024-01-01T00:00:00.000000000Z'::TIMESTAMP_NS, '1.1.1.1'::IPV4,
                 1.0m, 1.00m, 1.00m, 1.00m, 1.00m, 1.00m,
                 'b', 'b', 'y', '22222222-2222-2222-2222-222222222222', CAST('0x02' AS LONG256),
                 #v, #v3, #v33d, #v33d3d3d, ARRAY[2.0], ARRAY[[2.0]], NULL,
                 '2024-01-01T01:00:00.000000Z'),
                ('A', false, (-128)::BYTE, (-32_768)::SHORT, 'Z', -2_147_483_647, -9_223_372_036_854_775_807, -0.0::FLOAT, -4.9E-324,
                 '2262-01-01T00:00:00.000Z'::DATE, '2262-01-01T00:00:00.000001Z'::TIMESTAMP, '1969-12-31T23:59:59.999999999Z'::TIMESTAMP_NS, '0.0.0.1'::IPV4,
                 -9.9m, -99.99m, -9999999.99m, -9999999999999999.99m,
                 -999999999999999999999999999999999999.99m, -12345678901234567890123456789012345678901234567890.12m,
                 'мульти€😀', '', NULL, '33333333-3333-3333-3333-333333333333', CAST('0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff' AS LONG256),
                 #0, #00, #0000, #00000000, ARRAY[-0.0, 5.0], ARRAY[[-1.0]], NULL,
                 '2024-01-01T02:00:00.000000Z'),
                (NULL, true, 2::BYTE, 2::SHORT, 'n', 2, 2, 2.0::FLOAT, 2.0,
                 '2024-01-02T00:00:00.000Z'::DATE, '2024-01-02T00:00:00.000000Z'::TIMESTAMP, '2024-01-02T00:00:00.000000000Z'::TIMESTAMP_NS, '2.2.2.2'::IPV4,
                 2.0m, 2.00m, 2.00m, 2.00m, 2.00m, 2.00m,
                 'n', 'n', 'z', '44444444-4444-4444-4444-444444444444', CAST('0x04' AS LONG256),
                 #w, #w3, #w33d, #w33d3d3d, ARRAY[4.0], ARRAY[[4.0]], NULL,
                 '2024-01-01T03:00:00.000000Z'),
                ('A', false, 0::BYTE, 0::SHORT, '0', NULL, NULL, NULL, NULL,
                 NULL, NULL, NULL, NULL,
                 NULL, NULL, NULL, NULL, NULL, NULL,
                 NULL, NULL, NULL, NULL, NULL,
                 NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                 '2024-01-02T04:00:00.000000Z')
            """;

    @Override
    public void tearDown() throws Exception {
        CoveringIndexRecordCursorFactory.setMaxRowsPerFrameForTesting(-1);
        super.tearDown();
    }

    @Test
    public void testIssueReproductionLongDoubleAndDecimals() throws Exception {
        // GitHub issue 8266: the fixed-width INCLUDE columns of a single-key covering scan
        // came back NULL over QWP, while the in-process SQL engine returned the stored values.
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startEgressServer()) {
                serverMain.execute("CREATE TABLE tc (sym SYMBOL, price LONG, dbl DOUBLE, d18 DECIMAL(18,2), d9 DECIMAL(9,2), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("""
                        INSERT INTO tc VALUES
                            ('A', 1, 1.5, 10.25::DECIMAL(18,2), 1.25::DECIMAL(9,2), '1970-01-01T00:00:00Z'),
                            ('B', 2, 2.5, 20.25::DECIMAL(18,2), 2.25::DECIMAL(9,2), '1970-01-01T01:00:00Z'),
                            ('A', 3, 3.5, 30.25::DECIMAL(18,2), 3.25::DECIMAL(9,2), '1970-01-01T02:00:00Z')
                        """);
                serverMain.awaitTable("tc");
                serverMain.execute("ALTER TABLE tc ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, dbl, d18, d9)");
                serverMain.awaitTable("tc");

                final String sql = "SELECT price, dbl, d18, d9 FROM tc WHERE sym = 'A'";
                assertQuery(serverMain, sql).assertsPlan("""
                        SelectedRecord
                            CoveringIndex on: sym with: price, dbl, d18, d9
                              filter: sym='A'
                        """);
                serverMain.assertSql(sql, "price\tdbl\td18\td9\n1\t1.5\t10.25\t1.25\n3\t3.5\t30.25\t3.25\n");
                Assert.assertEquals("1\t1.5\t10.25\t1.25\n3\t3.5\t30.25\t3.25\n", queryQwp(sql));
            }
        });
    }

    @Test
    public void testMultiKeyAndNoCoveringPlansReturnIncludedValues() throws Exception {
        // The multi-key merge (IN list) materializes covered values into its frames and keeps
        // the page-frame path; the no_covering hint reads column files. Both must keep working.
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startEgressServer()) {
                createPriceTable(serverMain, "tp", true);
                final String inList = "SELECT sym, price, dbl FROM tp WHERE sym IN ('A', 'C')";
                assertQuery(serverMain, inList).assertsPlanContaining("CoveringIndex on: sym with: price, dbl");
                // the merge emits rows in designated-timestamp order, keys interleaved
                Assert.assertEquals("A\t1\t1.5\nA\t3\t3.5\nC\t4\t4.5\n", queryQwp(inList));
                final String noCovering = "SELECT /*+ no_covering */ sym, price, dbl FROM tp WHERE sym = 'A'";
                assertQuery(serverMain, noCovering).assertsPlanNotContaining("CoveringIndex");
                Assert.assertEquals("A\t1\t1.5\nA\t3\t3.5\n", queryQwp(noCovering));
            }
        });
    }

    @Test
    public void testMultiKeyBindVariablesExecutedTwice() throws Exception {
        // force_use_covering drops the no_covering backup a bind key would otherwise bring, so
        // the IN list streams merged page frames. The connection reuses the cached factory, and
        // with it the page-frame cursor, for each execution after frames were freed.
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startEgressServer()) {
                createPriceTable(serverMain, "tp", true);
                CoveringIndexRecordCursorFactory.setMaxRowsPerFrameForTesting(1);
                final String sql = "SELECT /*+ force_use_covering */ sym, price, dbl FROM tp WHERE sym IN ($1, $2)";
                try (QwpQueryClient client = newClient()) {
                    client.connect();
                    final String ac = "A\t1\t1.5\nA\t3\t3.5\nC\t4\t4.5\n";
                    Assert.assertEquals(ac, queryQwp(client, sql, binds -> binds.setVarchar(0, "A").setVarchar(1, "C")));
                    Assert.assertEquals("B\t2\t2.5\n", queryQwp(client, sql, binds -> binds.setVarchar(0, "B").setVarchar(1, "nope")));
                    Assert.assertEquals("", queryQwp(client, sql, binds -> binds.setVarchar(0, "nope").setVarchar(1, "other")));
                    Assert.assertEquals(ac, queryQwp(client, sql, binds -> binds.setVarchar(0, "C").setVarchar(1, "A")));
                }
            }
        });
    }

    @Test
    public void testMultiKeyCancelledMidStreamThenRerun() throws Exception {
        // The client cancels after the first batch, with frames already freed and one current;
        // the stream's teardown frees the rest, and the next execution streams every row.
        final int rows = 60_000;
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startEgressServer()) {
                createKeyTable(serverMain, "tx", rows);
                CoveringIndexRecordCursorFactory.setMaxRowsPerFrameForTesting(1_000);
                final String sql = "SELECT price FROM tx WHERE sym IN ('A', 'C')";
                assertQuery(serverMain, sql).assertsPlanContaining("CoveringIndex on: sym with: price");
                try (QwpQueryClient client = QwpQueryClient.fromConfig("ws::addr=127.0.0.1:" + HTTP_PORT + ";failover=off;").withInitialCredit(1)) {
                    client.connect();
                    final byte[] status = {-1};
                    final boolean[] ended = {false};
                    final int[] batches = {0};
                    client.execute(sql, new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            if (++batches[0] == 1) {
                                client.cancel();
                            }
                        }

                        @Override
                        public void onEnd(long totalRows) {
                            ended[0] = true;
                        }

                        @Override
                        public void onError(byte s, String message) {
                            status[0] = s;
                        }
                    });
                    Assert.assertFalse(ended[0]);
                    Assert.assertEquals(QwpConstants.STATUS_CANCELLED, status[0]);
                    assertKeyTableStream(client, sql, rows);
                }
            }
        });
    }

    @Test
    public void testMultiKeyEveryIncludeTypeAcrossManyFrames() throws Exception {
        // Frames of 7 rows make the stream free a frame every 7 rows while a batch spans many
        // frames, for every column type an INCLUDE list accepts, var-size ones included.
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startEgressServer()) {
                serverMain.execute(ALL_TYPES_DDL);
                serverMain.execute(ALL_TYPES_INSERT);
                serverMain.execute("""
                        INSERT INTO t_all SELECT
                            rnd_symbol('A', 'B', 'C', NULL),
                            x % 3 = 0, (x % 100)::BYTE, (x % 30_000)::SHORT, rnd_char(),
                            CASE WHEN x % 10 = 0 THEN NULL ELSE x::INT END, x * 1_000_003, (x / 3.0)::FLOAT, x / 7.0,
                            (x * 86_400_000L)::DATE, (x * 1_000_000L)::TIMESTAMP, (x * 1_000_000_000L)::TIMESTAMP_NS, rnd_ipv4(),
                            (x % 9)::DECIMAL(2,1), (x % 99)::DECIMAL(4,2), x::DECIMAL(9,2), x::DECIMAL(18,2), x::DECIMAL(38,2), x::DECIMAL(76,2),
                            rnd_varchar(1, 40, 1), rnd_str(1, 30, 1), rnd_symbol('x', 'y', 'z', NULL), rnd_uuid4(), rnd_long256(),
                            rnd_geohash(5), rnd_geohash(10), rnd_geohash(20), rnd_geohash(40),
                            CASE WHEN x % 13 = 0 THEN NULL ELSE ARRAY[x::DOUBLE, -x::DOUBLE, x / 3.0] END,
                            CASE WHEN x % 17 = 0 THEN NULL ELSE ARRAY[[x::DOUBLE], [x / 7.0]] END,
                            rnd_bin(1, 48, 1),
                            '2024-01-01T05:00:00.000000Z'::TIMESTAMP + x * 60_000_000L
                        FROM long_sequence(3_000)
                        """);
                serverMain.awaitTable("t_all");
                serverMain.execute("ALTER TABLE t_all ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (" + ALL_TYPES_INCLUDE + ")");
                serverMain.awaitTable("t_all");
                CoveringIndexRecordCursorFactory.setMaxRowsPerFrameForTesting(7);
                final String[] projections = {
                        // the covering factory itself is the root: no projection wrapper
                        "*",
                        // a SelectedRecord projection: reordered, a column repeated, the key column in the middle
                        "d18, l, l, sym, vc, s, bin, arr, arr2, ts, i",
                };
                final String[] keyLists = {"'A', 'C'", "'B', 'nope'"};
                for (String projection : projections) {
                    for (String keys : keyLists) {
                        final String covering = "SELECT " + projection + " FROM t_all WHERE sym IN (" + keys + ")";
                        assertQuery(serverMain, covering).assertsPlanContaining("CoveringIndex on: sym");
                        final String reference = "SELECT /*+ no_covering */ " + projection + " FROM t_all WHERE sym IN (" + keys + ")";
                        assertQuery(serverMain, reference).assertsPlanNotContaining("CoveringIndex");
                        final String expected = queryQwp(reference);
                        Assert.assertTrue(countLines(expected) > 500);
                        Assert.assertEquals(covering, expected, queryQwp(covering));
                    }
                }
            }
        });
    }

    @Test
    public void testMultiKeyIncludeColumnAddedAfterData() throws Exception {
        // Rows written before the INCLUDE column existed read as NULL (a column top), in frames
        // of one row that the stream frees one by one.
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startEgressServer()) {
                serverMain.execute("CREATE TABLE tt (sym SYMBOL, price LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("INSERT INTO tt VALUES ('A', 1, '2024-01-01T00:00:00Z'), ('C', 2, '2024-01-02T00:00:00Z')");
                serverMain.awaitTable("tt");
                serverMain.execute("ALTER TABLE tt ADD COLUMN qty DOUBLE");
                serverMain.execute("INSERT INTO tt VALUES ('A', 3, '2024-01-02T01:00:00Z', 3.5), ('B', 4, '2024-01-02T02:00:00Z', 4.5), ('C', 5, '2024-01-02T03:00:00Z', 5.5)");
                serverMain.awaitTable("tt");
                serverMain.execute("ALTER TABLE tt ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, qty)");
                serverMain.awaitTable("tt");
                CoveringIndexRecordCursorFactory.setMaxRowsPerFrameForTesting(1);
                final String sql = "SELECT price, qty FROM tt WHERE sym IN ('A', 'C')";
                assertQuery(serverMain, sql).assertsPlanContaining("CoveringIndex on: sym with: price, qty");
                Assert.assertEquals("1\tnull\n2\tnull\n3\t3.5\n5\t5.5\n", queryQwp(sql));
            }
        });
    }

    @Test
    public void testMultiKeyLimitAndOrderBy() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startEgressServer()) {
                createPriceTable(serverMain, "tp", true);
                CoveringIndexRecordCursorFactory.setMaxRowsPerFrameForTesting(1);
                final String[] suffixes = {"", " ORDER BY ts", " ORDER BY ts DESC", " LIMIT 2", " LIMIT -2", " LIMIT 1, 3"};
                final String[] expected = {
                        "1\t1.5\n3\t3.5\n4\t4.5\n",
                        "1\t1.5\n3\t3.5\n4\t4.5\n",
                        "4\t4.5\n3\t3.5\n1\t1.5\n",
                        "1\t1.5\n3\t3.5\n",
                        "3\t3.5\n4\t4.5\n",
                        "3\t3.5\n4\t4.5\n",
                };
                for (int i = 0; i < suffixes.length; i++) {
                    final String sql = "SELECT price, dbl FROM tp WHERE sym IN ('A', 'C')" + suffixes[i];
                    Assert.assertEquals(sql, expected[i], queryQwp(sql));
                    Assert.assertEquals(sql, expected[i], queryQwp("SELECT /*+ no_covering */ price, dbl FROM tp WHERE sym IN ('A', 'C')" + suffixes[i]));
                }
            }
        });
    }

    @Test
    public void testMultiKeyNonWalAndNonPartitionedTables() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startEgressServer()) {
                createPriceTable(serverMain, "tp_bypass", false);
                CoveringIndexRecordCursorFactory.setMaxRowsPerFrameForTesting(1);
                assertQuery(serverMain, "SELECT price, dbl FROM tp_bypass WHERE sym IN ('A', 'C')").assertsPlanContaining("CoveringIndex");
                Assert.assertEquals("1\t1.5\n3\t3.5\n4\t4.5\n", queryQwp("SELECT price, dbl FROM tp_bypass WHERE sym IN ('A', 'C')"));

                // one partition: the merge splits it into a frame per row
                serverMain.execute("CREATE TABLE tp_none (sym SYMBOL, price LONG, dbl DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY NONE");
                serverMain.execute("INSERT INTO tp_none VALUES ('A', 1, 1.5, '2024-01-01T00:00:00Z'), ('B', 2, 2.5, '2024-01-01T01:00:00Z'), ('C', 3, 3.5, '2024-01-02T00:00:00Z'), ('A', 4, 4.5, '2024-01-02T01:00:00Z')");
                serverMain.execute("ALTER TABLE tp_none ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, dbl)");
                assertQuery(serverMain, "SELECT price, dbl FROM tp_none WHERE sym IN ('A', 'C')").assertsPlanContaining("CoveringIndex");
                Assert.assertEquals("1\t1.5\n3\t3.5\n4\t4.5\n", queryQwp("SELECT price, dbl FROM tp_none WHERE sym IN ('A', 'C')"));
            }
        });
    }

    @Test
    public void testMultiKeyParquetPartition() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startEgressServer()) {
                createPriceTable(serverMain, "tp", true);
                // rows 1 and 2 sit in the first day partition; the last (active) one stays native
                serverMain.execute("ALTER TABLE tp CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-02'");
                serverMain.awaitTable("tp");
                CoveringIndexRecordCursorFactory.setMaxRowsPerFrameForTesting(1);
                final String sql = "SELECT price, dbl FROM tp WHERE sym IN ('A', 'C')";
                assertQuery(serverMain, sql).assertsPlanContaining("CoveringIndex");
                Assert.assertEquals("1\t1.5\n3\t3.5\n4\t4.5\n", queryQwp(sql));
                Assert.assertEquals(queryQwp("SELECT /*+ no_covering */ price, dbl FROM tp WHERE sym IN ('A', 'C')"), queryQwp(sql));
            }
        });
    }

    @Test
    public void testMultiKeyStreamStaysUnderQueryMemoryLimit() throws Exception {
        // The merge fills each frame into buffers of its own: 4096 rows of price, dbl and the
        // symbol key. The query memory limit holds three such frames. The stream frees each
        // frame before it pulls the next, so its 40 frames fit; keeping every frame until the
        // query ends would breach the limit at the fourth.
        final int rows = 60_000;
        final long frameBytes = 4_096L * (Long.BYTES + Double.BYTES + Integer.BYTES);
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startServerWithRetry(
                    PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES.getEnvVarName(), Long.toString(3 * frameBytes + frameBytes / 2),
                    PropertyKey.DEBUG_HTTP_FORCE_RECV_FRAGMENTATION_CHUNK_SIZE.getEnvVarName(), Integer.toString(recvChunk),
                    PropertyKey.DEBUG_HTTP_FORCE_SEND_FRAGMENTATION_CHUNK_SIZE.getEnvVarName(), Integer.toString(sendChunk)
            )) {
                createKeyTable(serverMain, "tm", rows);
                final String sql = "SELECT price, dbl FROM tm WHERE sym IN ('A', 'C')";
                assertQuery(serverMain, sql).assertsPlanContaining("CoveringIndex on: sym with: price, dbl");
                try (QwpQueryClient client = newClient()) {
                    client.connect();
                    // a single frame of 40_000 rows outgrows the limit on its own
                    CoveringIndexRecordCursorFactory.setMaxRowsPerFrameForTesting(rows);
                    final String[] error = {null};
                    client.execute(sql, new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            Assert.fail("the frame cannot be built under the limit");
                        }

                        @Override
                        public void onEnd(long totalRows) {
                            Assert.fail("the frame cannot be built under the limit");
                        }

                        @Override
                        public void onError(byte status, String message) {
                            error[0] = message;
                        }
                    });
                    Assert.assertNotNull(error[0]);
                    TestUtils.assertContains(error[0], "query memory limit exceeded");

                    // the same connection streams the 40 frames of 1_000 rows under the same limit
                    CoveringIndexRecordCursorFactory.setMaxRowsPerFrameForTesting(1_000);
                    assertKeyTableStream(client, sql, rows);
                    assertKeyTableStream(client, sql, rows);
                }
            }
        });
    }

    @Test
    public void testMultiKeyThroughViewCteAndSubquery() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startEgressServer()) {
                createPriceTable(serverMain, "tp", true);
                CoveringIndexRecordCursorFactory.setMaxRowsPerFrameForTesting(1);
                serverMain.execute("CREATE VIEW tp_ac AS (SELECT price, dbl FROM tp WHERE sym IN ('A', 'C'))");
                assertQuery(serverMain, "SELECT * FROM tp_ac").assertsPlanContaining("CoveringIndex");
                Assert.assertEquals("1\t1.5\n3\t3.5\n4\t4.5\n", queryQwp("SELECT * FROM tp_ac"));
                Assert.assertEquals("1\t1.5\n3\t3.5\n4\t4.5\n", queryQwp("WITH c AS (SELECT price, dbl FROM tp WHERE sym IN ('A', 'C')) SELECT * FROM c"));
                Assert.assertEquals("1.5\t1\n3.5\t3\n4.5\t4\n", queryQwp("SELECT dbl, price FROM (SELECT price, dbl FROM tp WHERE sym IN ('A', 'C'))"));
            }
        });
    }

    @Test
    public void testMultiKeyUnknownKeysAndEmptyTable() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startEgressServer()) {
                serverMain.execute("CREATE TABLE tn (sym SYMBOL, price LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("ALTER TABLE tn ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price)");
                serverMain.awaitTable("tn");
                CoveringIndexRecordCursorFactory.setMaxRowsPerFrameForTesting(1);
                Assert.assertEquals(0, queryQwpRowCount("SELECT price FROM tn WHERE sym IN ('A', 'B')"));

                serverMain.execute("INSERT INTO tn VALUES ('A', 1, '2024-01-01T00:00:00Z'), (NULL, 2, '2024-01-01T01:00:00Z'), ('B', 3, '2024-01-02T00:00:00Z'), (NULL, 4, '2024-01-02T01:00:00Z')");
                serverMain.awaitTable("tn");
                Assert.assertEquals(0, queryQwpRowCount("SELECT price FROM tn WHERE sym IN ('nope', 'other')"));
                Assert.assertEquals("1\n", queryQwp("SELECT price FROM tn WHERE sym IN ('A', 'nope')"));
                Assert.assertEquals("1\n3\n", queryQwp("SELECT price FROM tn WHERE sym IN ('A', 'B')"));
                Assert.assertEquals("1\n2\n4\n", queryQwp("SELECT price FROM tn WHERE sym IN ('A', NULL)"));
            }
        });
    }

    @Test
    public void testSingleKeyAfterIndexDroppedAndRecreated() throws Exception {
        // The connection caches the compiled covering plan; dropping the index makes it stale,
        // and the recompiled plan reads column files. Re-adding the index restores covering.
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startEgressServer()) {
                createPriceTable(serverMain, "tp", true);
                final String sql = "SELECT price, dbl FROM tp WHERE sym = 'A'";
                try (QwpQueryClient client = newClient()) {
                    client.connect();
                    Assert.assertEquals("1\t1.5\n3\t3.5\n", queryQwp(client, sql, null));
                    serverMain.execute("ALTER TABLE tp ALTER COLUMN sym DROP INDEX");
                    serverMain.awaitTable("tp");
                    assertQuery(serverMain, sql).assertsPlanNotContaining("CoveringIndex");
                    Assert.assertEquals("1\t1.5\n3\t3.5\n", queryQwp(client, sql, null));
                    serverMain.execute("ALTER TABLE tp ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, dbl)");
                    serverMain.awaitTable("tp");
                    assertQuery(serverMain, sql).assertsPlanContaining("CoveringIndex");
                    Assert.assertEquals("1\t1.5\n3\t3.5\n", queryQwp(client, sql, null));
                }
            }
        });
    }

    @Test
    public void testSingleKeyBindVariableExecutedTwice() throws Exception {
        // A bind variable resolves the key per execution; the cached factory is reused for the
        // later executions with other keys on the same connection. A bind key can be NULL, so this
        // plan carries the no_covering backup and streams through the row cursor either way.
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startEgressServer()) {
                createPriceTable(serverMain, "tp", true);
                final String sql = "SELECT sym, price, dbl FROM tp WHERE sym = $1";
                try (QwpQueryClient client = newClient()) {
                    client.connect();
                    Assert.assertEquals("A\t1\t1.5\nA\t3\t3.5\n", queryQwp(client, sql, binds -> binds.setVarchar(0, "A")));
                    Assert.assertEquals("B\t2\t2.5\n", queryQwp(client, sql, binds -> binds.setVarchar(0, "B")));
                    Assert.assertEquals("", queryQwp(client, sql, binds -> binds.setVarchar(0, "nope")));
                    Assert.assertEquals("A\t1\t1.5\nA\t3\t3.5\n", queryQwp(client, sql, binds -> binds.setVarchar(0, "A")));
                }
            }
        });
    }

    @Test
    public void testSingleKeyCancelledMidStreamThenRerun() throws Exception {
        final int rows = 100_000;
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startEgressServer()) {
                serverMain.execute("CREATE TABLE tx (sym SYMBOL, price LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("INSERT INTO tx SELECT 'A', x, (x * 1_000_000L)::TIMESTAMP FROM long_sequence(" + rows + ")");
                serverMain.awaitTable("tx");
                serverMain.execute("ALTER TABLE tx ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price)");
                serverMain.awaitTable("tx");
                final String sql = "SELECT price FROM tx WHERE sym = 'A'";
                try (QwpQueryClient client = QwpQueryClient.fromConfig("ws::addr=127.0.0.1:" + HTTP_PORT + ";failover=off;").withInitialCredit(1)) {
                    client.connect();
                    final byte[] status = {-1};
                    final boolean[] ended = {false};
                    final int[] batches = {0};
                    client.execute(sql, new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            if (++batches[0] == 1) {
                                client.cancel();
                            }
                        }

                        @Override
                        public void onEnd(long totalRows) {
                            ended[0] = true;
                        }

                        @Override
                        public void onError(byte s, String message) {
                            status[0] = s;
                        }
                    });
                    Assert.assertFalse(ended[0]);
                    Assert.assertEquals(QwpConstants.STATUS_CANCELLED, status[0]);

                    final long[] sum = {0};
                    final long[] count = {0};
                    client.execute(sql, new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            for (int r = 0, n = batch.getRowCount(); r < n; r++) {
                                sum[0] += batch.getLongValue(0, r);
                            }
                            count[0] += batch.getRowCount();
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte s, String message) {
                            Assert.fail(message);
                        }
                    });
                    Assert.assertEquals(rows, count[0]);
                    Assert.assertEquals((long) rows * (rows + 1) / 2, sum[0]);
                }
            }
        });
    }

    @Test
    public void testSingleKeyEveryIncludeType() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startEgressServer()) {
                serverMain.execute(ALL_TYPES_DDL);
                serverMain.execute(ALL_TYPES_INSERT);
                // Non-NULL BINARY values for the 'A' key; the other columns of these rows are NULL.
                serverMain.execute("INSERT INTO t_all (sym, bin, ts) SELECT 'A', rnd_bin(1, 16, 0), '2024-01-02T05:00:00.000000Z'::TIMESTAMP + x FROM long_sequence(2)");
                serverMain.awaitTable("t_all");
                serverMain.execute("ALTER TABLE t_all ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (" + ALL_TYPES_INCLUDE + ")");
                serverMain.awaitTable("t_all");

                final String[] projections = {
                        // the covering factory itself is the root: no projection wrapper
                        "*",
                        // a SelectedRecord projection: reordered, a column repeated, the key column in the middle
                        "d18, l, l, sym, vc, ts, i",
                };
                for (String projection : projections) {
                    final String covering = "SELECT " + projection + " FROM t_all WHERE sym = 'A'";
                    assertQuery(serverMain, covering).assertsPlanContaining("CoveringIndex on: sym");
                    final String reference = "SELECT /*+ no_covering */ " + projection + " FROM t_all WHERE sym = 'A'";
                    assertQuery(serverMain, reference).assertsPlanNotContaining("CoveringIndex");
                    final String expected = queryQwp(reference);
                    Assert.assertEquals(5, countLines(expected));
                    Assert.assertEquals(projection, expected, queryQwp(covering));
                }
                // Spot-check exact values against the inserted literals, so a fault shared by both
                // plans cannot pass as agreement.
                Assert.assertEquals(
                        """
                                A\t9223372036854775807\t2147483647\t9999999999999999.99\t1.7976931348623157E308\tascii\ttrue\tx
                                A\t-9223372036854775807\t-2147483647\t-9999999999999999.99\t-4.9E-324\tмульти€😀\tfalse\tnull
                                A\tnull\tnull\tnull\tnull\tnull\tfalse\tnull
                                A\tnull\tnull\tnull\tnull\tnull\tfalse\tnull
                                A\tnull\tnull\tnull\tnull\tnull\tfalse\tnull
                                """,
                        queryQwp("SELECT sym, l, i, d18, d, vc, b, sy FROM t_all WHERE sym = 'A'")
                );
            }
        });
    }

    @Test
    public void testSingleKeyIncludeColumnAddedAfterData() throws Exception {
        // Rows written before the INCLUDE column existed read as NULL (a column top).
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startEgressServer()) {
                serverMain.execute("CREATE TABLE tt (sym SYMBOL, price LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("INSERT INTO tt VALUES ('A', 1, '2024-01-01T00:00:00Z'), ('A', 2, '2024-01-02T00:00:00Z')");
                serverMain.awaitTable("tt");
                serverMain.execute("ALTER TABLE tt ADD COLUMN qty DOUBLE");
                serverMain.execute("INSERT INTO tt VALUES ('A', 3, '2024-01-02T01:00:00Z', 3.5), ('B', 4, '2024-01-02T02:00:00Z', 4.5)");
                serverMain.awaitTable("tt");
                serverMain.execute("ALTER TABLE tt ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, qty)");
                serverMain.awaitTable("tt");
                final String sql = "SELECT price, qty FROM tt WHERE sym = 'A'";
                assertQuery(serverMain, sql).assertsPlanContaining("CoveringIndex on: sym with: price, qty");
                Assert.assertEquals("1\tnull\n2\tnull\n3\t3.5\n", queryQwp(sql));
            }
        });
    }

    @Test
    public void testSingleKeyKeyColumnAndIncludedSymbol() throws Exception {
        // The key column comes from the WHERE key, an INCLUDE'd SYMBOL column from the sidecar;
        // both resolve through the table's symbol tables into the connection's symbol dictionary.
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startEgressServer()) {
                serverMain.execute("CREATE TABLE ts1 (sym SYMBOL, venue SYMBOL, price LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("""
                        INSERT INTO ts1 VALUES
                            ('A', 'nyse', 1, '2024-01-01T00:00:00Z'),
                            ('B', 'lse', 2, '2024-01-01T01:00:00Z'),
                            ('A', NULL, 3, '2024-01-01T02:00:00Z'),
                            ('A', 'lse', 4, '2024-01-02T00:00:00Z')
                        """);
                serverMain.awaitTable("ts1");
                serverMain.execute("ALTER TABLE ts1 ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (venue, price)");
                serverMain.awaitTable("ts1");
                try (QwpQueryClient client = newClient()) {
                    client.connect();
                    final String a = "SELECT sym, venue, price FROM ts1 WHERE sym = 'A'";
                    assertQuery(serverMain, a).assertsPlanContaining("CoveringIndex on: sym with: venue, price");
                    Assert.assertEquals("A\tnyse\t1\nA\tnull\t3\nA\tlse\t4\n", queryQwp(client, a, null));
                    Assert.assertEquals("B\tlse\t2\n", queryQwp(client, "SELECT sym, venue, price FROM ts1 WHERE sym = 'B'", null));
                    Assert.assertEquals("lse\tA\n", queryQwp(client, "SELECT venue, sym FROM ts1 WHERE sym = 'A' AND ts >= '2024-01-02'", null));
                }
            }
        });
    }

    @Test
    public void testSingleKeyLimitAndOrderBy() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startEgressServer()) {
                createPriceTable(serverMain, "tp", true);
                Assert.assertEquals("1\t1.5\n", queryQwp("SELECT price, dbl FROM tp WHERE sym = 'A' LIMIT 1"));
                Assert.assertEquals("3\t3.5\n", queryQwp("SELECT price, dbl FROM tp WHERE sym = 'A' LIMIT -1"));
                Assert.assertEquals("3\t3.5\n1\t1.5\n", queryQwp("SELECT price, dbl FROM tp WHERE sym = 'A' ORDER BY ts DESC"));
            }
        });
    }

    @Test
    public void testSingleKeyManyRowsAcrossBatchesAndPartitions() throws Exception {
        // More matching rows than one QWP batch, spread over several partitions, with a tiny
        // client credit so the server parks after every batch and resumes the same cursor.
        final int rows = 100_000;
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startEgressServer()) {
                serverMain.execute("CREATE TABLE tm (sym SYMBOL, price LONG, dbl DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                // 'A' on odd x, 'B' on even x; 5 partitions of 20_000 rows each
                serverMain.execute("INSERT INTO tm SELECT CASE WHEN x % 2 = 1 THEN 'A' ELSE 'B' END, x, x * 0.5, (x * 4_320_000L)::TIMESTAMP FROM long_sequence(" + rows + ")");
                serverMain.awaitTable("tm");
                serverMain.execute("ALTER TABLE tm ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, dbl)");
                serverMain.awaitTable("tm");
                final String sql = "SELECT price, dbl FROM tm WHERE sym = 'A'";
                assertQuery(serverMain, sql).assertsPlanContaining("CoveringIndex on: sym with: price, dbl");

                try (QwpQueryClient client = newClient().withInitialCredit(1)) {
                    client.connect();
                    for (int run = 0; run < 2; run++) {
                        final long[] next = {1};
                        final int[] batches = {0};
                        client.execute(sql, new QwpColumnBatchHandler() {
                            @Override
                            public void onBatch(QwpColumnBatch batch) {
                                batches[0]++;
                                for (int r = 0, n = batch.getRowCount(); r < n; r++) {
                                    final long expected = next[0];
                                    Assert.assertFalse(batch.isNull(0, r));
                                    Assert.assertEquals(expected, batch.getLongValue(0, r));
                                    Assert.assertEquals(expected * 0.5, batch.getDoubleValue(1, r), 0.0);
                                    next[0] += 2;
                                }
                            }

                            @Override
                            public void onEnd(long totalRows) {
                                Assert.assertEquals(rows / 2, totalRows);
                            }

                            @Override
                            public void onError(byte status, String message) {
                                Assert.fail(message);
                            }
                        });
                        Assert.assertEquals(rows + 1, next[0]);
                        Assert.assertTrue("expected several batches, got " + batches[0], batches[0] > 1);
                    }
                }
            }
        });
    }

    @Test
    public void testSingleKeyNonWalAndNonPartitionedTables() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startEgressServer()) {
                createPriceTable(serverMain, "tp_bypass", false);
                assertQuery(serverMain, "SELECT price, dbl FROM tp_bypass WHERE sym = 'A'").assertsPlanContaining("CoveringIndex");
                Assert.assertEquals("1\t1.5\n3\t3.5\n", queryQwp("SELECT price, dbl FROM tp_bypass WHERE sym = 'A'"));

                serverMain.execute("CREATE TABLE tp_none (sym SYMBOL, price LONG, dbl DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY NONE");
                serverMain.execute("INSERT INTO tp_none VALUES ('A', 1, 1.5, '2024-01-01T00:00:00Z'), ('B', 2, 2.5, '2024-01-01T01:00:00Z'), ('A', 3, 3.5, '2024-01-02T00:00:00Z')");
                serverMain.execute("ALTER TABLE tp_none ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, dbl)");
                assertQuery(serverMain, "SELECT price, dbl FROM tp_none WHERE sym = 'A'").assertsPlanContaining("CoveringIndex");
                Assert.assertEquals("1\t1.5\n3\t3.5\n", queryQwp("SELECT price, dbl FROM tp_none WHERE sym = 'A'"));
            }
        });
    }

    @Test
    public void testSingleKeyParquetPartition() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startEgressServer()) {
                createPriceTable(serverMain, "tp", true);
                // rows 1 and 2 sit in the first day partition; the last (active) one stays native
                serverMain.execute("ALTER TABLE tp CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-02'");
                serverMain.awaitTable("tp");
                final String sql = "SELECT price, dbl FROM tp WHERE sym = 'A'";
                assertQuery(serverMain, sql).assertsPlanContaining("CoveringIndex");
                Assert.assertEquals("1\t1.5\n3\t3.5\n", queryQwp(sql));
                Assert.assertEquals(queryQwp("SELECT /*+ no_covering */ price, dbl FROM tp WHERE sym = 'A'"), queryQwp(sql));
            }
        });
    }

    @Test
    public void testSingleKeyResidualFilterAndAggregates() throws Exception {
        // A residual filter and an aggregate run over the covered page frames inside the engine,
        // which decodes them in parallel; QWP streams the result of the parent operator.
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startEgressServer()) {
                createPriceTable(serverMain, "tp", true);
                Assert.assertEquals("3\t3.5\n", queryQwp("SELECT price, dbl FROM tp WHERE sym = 'A' AND price > 1"));
                Assert.assertEquals("2\t4\t5.0\n", queryQwp("SELECT count(), sum(price), sum(dbl) FROM tp WHERE sym = 'A'"));
            }
        });
    }

    @Test
    public void testSingleKeyStreamIsSnapshotWhileTableReceivesRows() throws Exception {
        // The server parks the stream after the first batch (credit 1). While it is parked,
        // rows for the same key land in the partition the open cursor reads. The stream must
        // finish with the rows of its snapshot, and the next execution must see the new rows.
        final int rows = 40_000;
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startEgressServer()) {
                serverMain.execute("CREATE TABLE tw (sym SYMBOL, price LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("INSERT INTO tw SELECT 'A', x, (x * 1_000_000L)::TIMESTAMP FROM long_sequence(" + rows + ")");
                serverMain.awaitTable("tw");
                serverMain.execute("ALTER TABLE tw ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price)");
                serverMain.awaitTable("tw");
                final String sql = "SELECT price FROM tw WHERE sym = 'A'";
                assertQuery(serverMain, sql).assertsPlanContaining("CoveringIndex on: sym with: price");

                try (QwpQueryClient client = newClient().withInitialCredit(1)) {
                    client.connect();
                    final long[] next = {1};
                    final boolean[] inserted = {false};
                    client.execute(sql, new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            for (int r = 0, n = batch.getRowCount(); r < n; r++) {
                                Assert.assertEquals(next[0]++, batch.getLongValue(0, r));
                            }
                            if (!inserted[0]) {
                                inserted[0] = true;
                                // same day partition as the last rows the cursor has yet to read
                                serverMain.execute("INSERT INTO tw SELECT 'A', " + rows + " + x, ((" + rows + " + x) * 1_000_000L)::TIMESTAMP FROM long_sequence(1000)");
                                serverMain.awaitTable("tw");
                            }
                        }

                        @Override
                        public void onEnd(long totalRows) {
                            Assert.assertEquals(rows, totalRows);
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail(message);
                        }
                    });
                    Assert.assertTrue(inserted[0]);
                    Assert.assertEquals(rows + 1, next[0]);

                    final long[] count = {0};
                    final long[] last = {0};
                    client.execute(sql, new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            count[0] += batch.getRowCount();
                            last[0] = batch.getLongValue(0, batch.getRowCount() - 1);
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail(message);
                        }
                    });
                    Assert.assertEquals(rows + 1000, count[0]);
                    Assert.assertEquals(rows + 1000, last[0]);
                }
            }
        });
    }

    @Test
    public void testSingleKeyThroughViewCteAndSubquery() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startEgressServer()) {
                createPriceTable(serverMain, "tp", true);
                serverMain.execute("CREATE VIEW tp_a AS (SELECT price, dbl FROM tp WHERE sym = 'A')");
                assertQuery(serverMain, "SELECT * FROM tp_a").assertsPlanContaining("CoveringIndex");
                Assert.assertEquals("1\t1.5\n3\t3.5\n", queryQwp("SELECT * FROM tp_a"));
                Assert.assertEquals("1\t1.5\n3\t3.5\n", queryQwp("WITH c AS (SELECT price, dbl FROM tp WHERE sym = 'A') SELECT * FROM c"));
                Assert.assertEquals("1.5\t1\n3.5\t3\n", queryQwp("SELECT dbl, price FROM (SELECT price, dbl FROM tp WHERE sym = 'A')"));
            }
        });
    }

    @Test
    public void testSingleKeyUnknownKeyNullKeyAndEmptyTable() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startEgressServer()) {
                serverMain.execute("CREATE TABLE tn (sym SYMBOL, price LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("ALTER TABLE tn ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price)");
                serverMain.awaitTable("tn");
                Assert.assertEquals(0, queryQwpRowCount("SELECT price FROM tn WHERE sym = 'A'"));

                serverMain.execute("INSERT INTO tn VALUES ('A', 1, '2024-01-01T00:00:00Z'), (NULL, 2, '2024-01-01T01:00:00Z'), ('B', 3, '2024-01-02T00:00:00Z'), (NULL, 4, '2024-01-02T01:00:00Z')");
                serverMain.awaitTable("tn");
                Assert.assertEquals(0, queryQwpRowCount("SELECT price FROM tn WHERE sym = 'nope'"));
                Assert.assertEquals("2\n4\n", queryQwp("SELECT price FROM tn WHERE sym IS NULL"));
                Assert.assertEquals("1\n", queryQwp("SELECT price FROM tn WHERE sym = 'A'"));
            }
        });
    }

    // Streams sql, whose first column is the price of a createKeyTable() table and second, if
    // any, its dbl, and checks that every row of keys 'A' and 'C' arrives once, in order.
    private static void assertKeyTableStream(QwpQueryClient client, String sql, int rows) {
        final long[] next = {2};
        final long[] count = {0};
        client.execute(sql, new QwpColumnBatchHandler() {
            @Override
            public void onBatch(QwpColumnBatch batch) {
                for (int r = 0, n = batch.getRowCount(); r < n; r++) {
                    final long x = next[0];
                    Assert.assertEquals(x, batch.getLongValue(0, r));
                    if (batch.getColumnCount() > 1) {
                        Assert.assertEquals(x / 2.0, batch.getDoubleValue(1, r), 0.0);
                    }
                    // x % 3 == 2 is a 'C' row, followed by the 'A' row x + 1; an 'A' row by x + 2
                    next[0] += x % 3 == 2 ? 1 : 2;
                    count[0]++;
                }
            }

            @Override
            public void onEnd(long totalRows) {
                Assert.assertEquals(rows / 3 * 2, totalRows);
            }

            @Override
            public void onError(byte status, String message) {
                Assert.fail("[" + sql + "] failed: " + message);
            }
        });
        Assert.assertEquals(rows / 3 * 2, count[0]);
    }

    private static int countLines(String text) {
        int n = 0;
        for (int i = 0, len = text.length(); i < len; i++) {
            if (text.charAt(i) == '\n') {
                n++;
            }
        }
        return n;
    }

    // Rows x = 1..rows one second apart, all in one day partition: x % 3 picks the key
    // (0 'A', 1 'B', 2 'C'), price is x and dbl x / 2.
    private static void createKeyTable(TestServerMain serverMain, String name, int rows) {
        serverMain.execute("CREATE TABLE " + name + " (sym SYMBOL, price LONG, dbl DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        serverMain.execute("INSERT INTO " + name + " SELECT CASE x % 3 WHEN 0 THEN 'A' WHEN 1 THEN 'B' ELSE 'C' END, x, x / 2.0, (x * 1_000_000L)::TIMESTAMP FROM long_sequence(" + rows + ")");
        serverMain.awaitTable(name);
        serverMain.execute("ALTER TABLE " + name + " ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, dbl)");
        serverMain.awaitTable(name);
    }

    // 'A' rows: (1, 1.5) on day 1 and (3, 3.5) on day 2; 'B': (2, 2.5); 'C': (4, 4.5) on day 2.
    private static void createPriceTable(TestServerMain serverMain, String name, boolean wal) {
        serverMain.execute("CREATE TABLE " + name + " (sym SYMBOL, price LONG, dbl DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY " + (wal ? "WAL" : "BYPASS WAL"));
        serverMain.execute("INSERT INTO " + name + " VALUES ('A', 1, 1.5, '2024-01-01T00:00:00Z'), ('B', 2, 2.5, '2024-01-01T01:00:00Z'), ('A', 3, 3.5, '2024-01-02T00:00:00Z'), ('C', 4, 4.5, '2024-01-02T01:00:00Z')");
        if (wal) {
            serverMain.awaitTable(name);
        }
        serverMain.execute("ALTER TABLE " + name + " ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, dbl)");
        if (wal) {
            serverMain.awaitTable(name);
        }
    }

    private static QwpQueryClient newClient() {
        return QwpQueryClient.fromConfig("ws::addr=127.0.0.1:" + HTTP_PORT + ";");
    }

    private static String queryQwp(String sql) {
        try (QwpQueryClient client = newClient()) {
            client.connect();
            return queryQwp(client, sql, null);
        }
    }

    private static String queryQwp(QwpQueryClient client, String sql, QwpBindSetter binds) {
        final StringBuilder sb = new StringBuilder();
        final QwpColumnBatchHandler handler = new QwpColumnBatchHandler() {
            @Override
            public void onBatch(QwpColumnBatch batch) {
                for (int r = 0, n = batch.getRowCount(); r < n; r++) {
                    for (int c = 0, m = batch.getColumnCount(); c < m; c++) {
                        if (c > 0) {
                            sb.append('\t');
                        }
                        renderCell(batch, c, r, sb);
                    }
                    sb.append('\n');
                }
            }

            @Override
            public void onEnd(long totalRows) {
            }

            @Override
            public void onError(byte status, String message) {
                Assert.fail("[" + sql + "] failed: " + message);
            }
        };
        if (binds == null) {
            client.execute(sql, handler);
        } else {
            client.execute(sql, binds, handler);
        }
        return sb.toString();
    }

    private static int queryQwpRowCount(String sql) {
        return countLines(queryQwp(sql));
    }

    private static void renderCell(QwpColumnBatch batch, int col, int row, StringBuilder sb) {
        final byte wireType = batch.getColumnWireType(col);
        // BOOLEAN, BYTE, SHORT and CHAR cannot carry NULL on the wire.
        switch (wireType) {
            case QwpConstants.TYPE_BOOLEAN -> {
                sb.append(batch.getBoolValue(col, row));
                return;
            }
            case QwpConstants.TYPE_BYTE -> {
                sb.append(batch.getByteValue(col, row));
                return;
            }
            case QwpConstants.TYPE_SHORT -> {
                sb.append(batch.getShortValue(col, row));
                return;
            }
            case QwpConstants.TYPE_CHAR -> {
                sb.append("char(").append((int) batch.getCharValue(col, row)).append(')');
                return;
            }
            default -> {
                if (batch.isNull(col, row)) {
                    sb.append("null");
                    return;
                }
            }
        }
        switch (wireType) {
            case QwpConstants.TYPE_INT -> sb.append(batch.getIntValue(col, row));
            case QwpConstants.TYPE_IPv4 ->
                    sb.append("ip(").append(Integer.toUnsignedString(batch.getIntValue(col, row))).append(')');
            case QwpConstants.TYPE_LONG, QwpConstants.TYPE_DATE, QwpConstants.TYPE_TIMESTAMP,
                 QwpConstants.TYPE_TIMESTAMP_NANOS -> sb.append(batch.getLongValue(col, row));
            case QwpConstants.TYPE_FLOAT -> sb.append(batch.getFloatValue(col, row));
            case QwpConstants.TYPE_DOUBLE -> sb.append(batch.getDoubleValue(col, row));
            case QwpConstants.TYPE_DECIMAL64 -> {
                final Decimal64 d = new Decimal64();
                Assert.assertTrue(batch.getDecimal64(col, row, d));
                sb.append(d);
            }
            case QwpConstants.TYPE_DECIMAL128 -> {
                final Decimal128 d = new Decimal128();
                Assert.assertTrue(batch.getDecimal128(col, row, d));
                sb.append(d);
            }
            case QwpConstants.TYPE_DECIMAL256 -> {
                final Decimal256 d = new Decimal256();
                Assert.assertTrue(batch.getDecimal256(col, row, d));
                sb.append(d);
            }
            case QwpConstants.TYPE_SYMBOL -> sb.append(batch.getSymbol(col, row));
            case QwpConstants.TYPE_VARCHAR -> sb.append(batch.getString(col, row));
            case QwpConstants.TYPE_UUID -> sb.append("uuid(").append(Long.toHexString(batch.getUuidHi(col, row)))
                    .append(':').append(Long.toHexString(batch.getUuidLo(col, row))).append(')');
            case QwpConstants.TYPE_LONG256 -> {
                sb.append("l256(");
                for (int w = 3; w >= 0; w--) {
                    sb.append(Long.toHexString(batch.getLong256Word(col, row, w)));
                    if (w > 0) {
                        sb.append(':');
                    }
                }
                sb.append(')');
            }
            case QwpConstants.TYPE_GEOHASH -> sb.append("geo(").append(batch.getGeohashPrecisionBits(col))
                    .append(':').append(Long.toBinaryString(batch.getGeohashValue(col, row))).append(')');
            case QwpConstants.TYPE_DOUBLE_ARRAY -> sb.append("arr").append(batch.getArrayNDims(col, row))
                    .append(Arrays.toString(batch.getDoubleArrayElements(col, row)));
            case QwpConstants.TYPE_BINARY -> sb.append("bin").append(Arrays.toString(batch.getBinary(col, row)));
            default -> Assert.fail("unexpected wire type " + wireType + " in column " + batch.getColumnName(col));
        }
    }
}
