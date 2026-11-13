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
import io.questdb.client.std.Decimal256;
import io.questdb.client.std.bytes.DirectByteSequence;
import io.questdb.client.std.str.DirectUtf8Sequence;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.std.LongList;
import io.questdb.std.ObjList;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

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
public class QwpEgressPageFrameTest extends AbstractQwpBootstrapTest {

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
     * Renders one cell of any column type the server streams, fixed-width values
     * by their raw bits, so that the rows of two streams compare exactly.
     */
    private static void appendCell(StringBuilder sink, QwpColumnBatch batch, int col, int row, Decimal256 decimal256) {
        if (batch.isNull(col, row)) {
            sink.append("null");
            return;
        }
        final byte wireType = batch.getColumnWireType(col);
        switch (wireType) {
            case QwpConstants.TYPE_BOOLEAN -> sink.append(batch.getBoolValue(col, row));
            case QwpConstants.TYPE_BYTE -> sink.append(batch.getByteValue(col, row));
            case QwpConstants.TYPE_SHORT -> sink.append(batch.getShortValue(col, row));
            case QwpConstants.TYPE_CHAR -> sink.append((int) batch.getCharValue(col, row));
            case QwpConstants.TYPE_INT, QwpConstants.TYPE_IPV4 -> sink.append(batch.getIntValue(col, row));
            case QwpConstants.TYPE_LONG, QwpConstants.TYPE_DATE, QwpConstants.TYPE_TIMESTAMP,
                 QwpConstants.TYPE_TIMESTAMP_NANOS, QwpConstants.TYPE_DECIMAL64 ->
                    sink.append(batch.getLongValue(col, row));
            case QwpConstants.TYPE_FLOAT -> sink.append(Float.floatToRawIntBits(batch.getFloatValue(col, row)));
            case QwpConstants.TYPE_DOUBLE -> sink.append(Double.doubleToRawLongBits(batch.getDoubleValue(col, row)));
            case QwpConstants.TYPE_SYMBOL, QwpConstants.TYPE_VARCHAR -> sink.append(batch.getString(col, row));
            case QwpConstants.TYPE_BINARY -> sink.append(Arrays.toString(batch.getBinary(col, row)));
            case QwpConstants.TYPE_UUID ->
                    sink.append(batch.getUuidHi(col, row)).append('/').append(batch.getUuidLo(col, row));
            case QwpConstants.TYPE_LONG256 -> {
                for (int word = 0; word < 4; word++) {
                    sink.append(batch.getLong256Word(col, row, word)).append('/');
                }
            }
            case QwpConstants.TYPE_DECIMAL128 ->
                    sink.append(batch.getDecimal128High(col, row)).append('/').append(batch.getDecimal128Low(col, row));
            case QwpConstants.TYPE_DECIMAL256 -> {
                Assert.assertTrue(batch.getDecimal256(col, row, decimal256));
                sink.append(decimal256);
            }
            case QwpConstants.TYPE_GEOHASH -> sink.append(batch.getGeohashValue(col, row));
            case QwpConstants.TYPE_DOUBLE_ARRAY -> sink.append(Arrays.toString(batch.getDoubleArrayElements(col, row)));
            default -> Assert.fail("unexpected wire type 0x" + Integer.toHexString(wireType & 0xFF)
                    + " of column " + batch.getColumnName(col));
        }
    }

    private static void assertRows(String sql, ObjList<String> expected, ObjList<String> actual) {
        Assert.assertEquals(sql, expected.size(), actual.size());
        for (int i = 0, n = expected.size(); i < n; i++) {
            Assert.assertEquals(sql + " at row " + i, expected.getQuick(i), actual.getQuick(i));
        }
    }

    private static long[] collectLongColumn(QwpQueryClient client, String sql) {
        final LongList values = new LongList();
        client.execute(sql, new QwpColumnBatchHandler() {
            @Override
            public void onBatch(QwpColumnBatch batch) {
                for (int r = 0; r < batch.getRowCount(); r++) {
                    values.add(batch.getLongValue(0, r));
                }
            }

            @Override
            public void onEnd(long totalRows) {
                Assert.assertEquals(values.size(), totalRows);
            }

            @Override
            public void onError(byte status, String message) {
                Assert.fail("egress error: " + message);
            }
        });
        final long[] result = new long[values.size()];
        for (int i = 0; i < result.length; i++) {
            result[i] = values.getQuick(i);
        }
        return result;
    }

    /**
     * Streams {@code sql} and renders each row, its cells joined, into one string.
     */
    private static ObjList<String> collectRows(QwpQueryClient client, String sql) {
        final ObjList<String> rows = new ObjList<>();
        final StringBuilder sink = new StringBuilder();
        final Decimal256 decimal256 = new Decimal256();
        client.execute(sql, new QwpColumnBatchHandler() {
            @Override
            public void onBatch(QwpColumnBatch batch) {
                for (int r = 0, n = batch.getRowCount(); r < n; r++) {
                    sink.setLength(0);
                    for (int c = 0, columnCount = batch.getColumnCount(); c < columnCount; c++) {
                        appendCell(sink, batch, c, r, decimal256);
                        sink.append('|');
                    }
                    rows.add(sink.toString());
                }
            }

            @Override
            public void onEnd(long totalRows) {
                Assert.assertEquals(sql, rows.size(), totalRows);
            }

            @Override
            public void onError(byte status, String message) {
                Assert.fail(sql + ": " + message);
            }
        });
        return rows;
    }

    /**
     * Creates {@code bwd_types}: three DAY partitions of {@code rowsPerPartition}
     * rows each, with a column of every type the page-frame path copies, NULLs
     * and the extreme values of each, and seven columns added after the first
     * partition was written, so that partition reads them as column tops. The
     * first partition is then converted to parquet.
     */
    private static void createAllTypesTable(TestServerMain serverMain, int rowsPerPartition, boolean isWal) {
        serverMain.execute("""
                CREATE TABLE bwd_types (
                    ts TIMESTAMP, l LONG, i INT, sh SHORT, b BYTE, c CHAR, bo BOOLEAN, d DOUBLE, f FLOAT,
                    dt DATE, tn TIMESTAMP_NS, ip IPv4, sy SYMBOL, v VARCHAR, s STRING, bin BINARY, u UUID,
                    l256 LONG256, g4 GEOHASH(4b), g40 GEOHASH(40b), d2 DECIMAL(2, 1), d4 DECIMAL(4, 2),
                    d9 DECIMAL(9, 3), d18 DECIMAL(18, 4), d38 DECIMAL(38, 5), d76 DECIMAL(76, 6), arr DOUBLE[]
                ) TIMESTAMP(ts) PARTITION BY DAY
                """ + (isWal ? " WAL" : " BYPASS WAL"));
        // y numbers the rows 1, 2, 3, ... across the partitions, one second apart
        final String values = """
                (86_400_000_000L * ((y - 1) / %d) + (y - 1) * 1_000_000L)::TIMESTAMP,
                CASE WHEN y %% 7 = 0 THEN NULL WHEN y %% 11 = 0 THEN 9_223_372_036_854_775_807L
                    WHEN y %% 13 = 0 THEN -9_223_372_036_854_775_807L ELSE y * 3 END,
                CASE WHEN y %% 7 = 1 THEN NULL WHEN y %% 11 = 1 THEN 2_147_483_647
                    WHEN y %% 13 = 1 THEN -2_147_483_647 ELSE (y * 5)::INT END,
                CASE WHEN y %% 17 = 0 THEN 32_767 WHEN y %% 19 = 0 THEN -32_768 ELSE y %% 1_000 - 500 END::SHORT,
                CASE WHEN y %% 17 = 1 THEN 127 WHEN y %% 19 = 1 THEN -128 ELSE y %% 200 - 100 END::BYTE,
                CASE WHEN y %% 23 = 0 THEN CAST(NULL AS CHAR) WHEN y %% 2 = 0 THEN 'a'::CHAR ELSE '\u20AC'::CHAR END,
                y %% 3 = 0,
                CASE WHEN y %% 7 = 2 THEN NULL WHEN y %% 11 = 2 THEN -0.0 WHEN y %% 13 = 2 THEN 1.7976931348623157E308
                    WHEN y %% 17 = 2 THEN 4.9E-324 ELSE y * 1.25 END,
                CASE WHEN y %% 7 = 3 THEN NULL WHEN y %% 11 = 3 THEN -0.0 ELSE y * 0.5 END::FLOAT,
                CASE WHEN y %% 7 = 4 THEN NULL ELSE (y * 86_400_000L)::DATE END,
                CASE WHEN y %% 7 = 5 THEN NULL ELSE (y * 1_000_000_007L)::TIMESTAMP_NS END,
                CASE WHEN y %% 7 = 6 THEN NULL WHEN y %% 11 = 6 THEN '255.255.255.255'
                    ELSE '10.0.' || (y %% 256) || '.' || (y %% 200 + 1) END::IPv4,
                CASE WHEN y %% 9 = 0 THEN NULL ELSE 'sym_' || (y %% 17) END,
                CASE WHEN y %% 9 = 1 THEN NULL WHEN y %% 9 = 2 THEN '' ELSE 'v_\u00E9\u20AC\uD83D\uDE00_' || y END,
                CASE WHEN y %% 9 = 3 THEN NULL ELSE 's_' || y END,
                CASE WHEN y %% 9 = 4 THEN NULL ELSE rnd_bin(1, 16, 0) END,
                CASE WHEN y %% 9 = 5 THEN NULL ELSE rnd_uuid4() END,
                CASE WHEN y %% 9 = 6 THEN NULL ELSE rnd_long256() END,
                CASE WHEN y %% 9 = 7 THEN NULL ELSE rnd_geohash(4) END,
                CASE WHEN y %% 9 = 8 THEN NULL ELSE rnd_geohash(40) END,
                CASE WHEN y %% 10 = 0 THEN NULL ELSE CAST(y %% 9 AS DECIMAL(2, 1)) END,
                CASE WHEN y %% 10 = 1 THEN NULL ELSE CAST(y %% 99 AS DECIMAL(4, 2)) END,
                CASE WHEN y %% 10 = 2 THEN NULL ELSE CAST(y * 7 AS DECIMAL(9, 3)) END,
                CASE WHEN y %% 10 = 3 THEN NULL ELSE CAST(y * 1_000 AS DECIMAL(18, 4)) END,
                CASE WHEN y %% 10 = 4 THEN NULL ELSE CAST(y AS DECIMAL(38, 5)) END,
                CASE WHEN y %% 10 = 5 THEN NULL ELSE CAST(y AS DECIMAL(76, 6)) END,
                CASE WHEN y %% 10 = 6 THEN NULL ELSE ARRAY[y * 1.0, y * 2.0] END
                """.formatted(rowsPerPartition);
        serverMain.execute("INSERT INTO bwd_types SELECT " + values
                + " FROM (SELECT x y FROM long_sequence(" + rowsPerPartition + "))");
        serverMain.execute("ALTER TABLE bwd_types ADD COLUMN late_l LONG");
        serverMain.execute("ALTER TABLE bwd_types ADD COLUMN late_d DOUBLE");
        serverMain.execute("ALTER TABLE bwd_types ADD COLUMN late_sh SHORT");
        serverMain.execute("ALTER TABLE bwd_types ADD COLUMN late_b BYTE");
        serverMain.execute("ALTER TABLE bwd_types ADD COLUMN late_bo BOOLEAN");
        serverMain.execute("ALTER TABLE bwd_types ADD COLUMN late_sy SYMBOL");
        serverMain.execute("ALTER TABLE bwd_types ADD COLUMN late_v VARCHAR");
        serverMain.execute("INSERT INTO bwd_types SELECT " + values + """
                ,
                CASE WHEN y % 5 = 0 THEN NULL ELSE y * 11 END,
                CASE WHEN y % 5 = 1 THEN NULL ELSE y * 0.25 END,
                (y % 1_000)::SHORT,
                (y % 100)::BYTE,
                y % 2 = 0,
                CASE WHEN y % 5 = 2 THEN NULL ELSE 'late_' || (y % 5) END,
                CASE WHEN y % 5 = 3 THEN NULL ELSE 'lv_' || y END
                """ + " FROM (SELECT x + " + rowsPerPartition + " y FROM long_sequence(" + 2 * rowsPerPartition + "))");
        serverMain.awaitTable("bwd_types");
        serverMain.execute("ALTER TABLE bwd_types CONVERT PARTITION TO PARQUET WHERE ts < '1970-01-02'");
        serverMain.awaitTable("bwd_types");
        serverMain.assertSql("SELECT count() FROM table_partitions('bwd_types') WHERE isParquet", "count\n1\n");
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
            try (final TestServerMain serverMain = startFragmented(SMALL_PAGE_FRAME_ENV)) {
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

    /**
     * A backward scan copies each page frame's rows last first, column by column.
     * Every column type, NULLs and extreme values, column tops and a parquet
     * partition must come back exactly as the RecordCursor path streams them,
     * which the {@code LIMIT} variant of each query takes, and as the forward
     * scan's rows in reverse. Frames of 128 rows, split across batches of 100
     * rows and across a credit-limited stream.
     */
    @Test
    public void testBackwardScanAllTypesMatchesRecordCursor() throws Exception {
        assertBackwardScanMatchesRecordCursor(500, true, SMALL_PAGE_FRAME_ENV);
    }

    @Test
    public void testBackwardScanEmptyAndSingleRowTables() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startFragmented(SMALL_PAGE_FRAME_ENV)) {
                serverMain.execute("CREATE TABLE bwd_empty(x LONG, v VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("CREATE TABLE bwd_one(x LONG, v VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("INSERT INTO bwd_one VALUES (42, 'only', 7::TIMESTAMP)");
                serverMain.awaitTable("bwd_empty");
                serverMain.awaitTable("bwd_one");
                try (QwpQueryClient client = QwpQueryClient.fromConfig("ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    Assert.assertEquals(0, collectRows(client, "SELECT * FROM bwd_empty ORDER BY ts DESC").size());
                    final ObjList<String> one = collectRows(client, "SELECT * FROM bwd_one ORDER BY ts DESC");
                    Assert.assertEquals(1, one.size());
                    Assert.assertEquals("42|only|7|", one.getQuick(0));
                }
            }
        });
    }

    /**
     * With the default frame size each 1_500-row partition is one frame, which a
     * backward scan takes in slices of at most 1_024 rows from its top. The table
     * is written without WAL. The send chunk floor keeps the megabytes this test
     * streams from parking every few hundred bytes; the small-frame variant keeps
     * the fine fragmentation.
     */
    @Test
    public void testBackwardScanSlicesLargeFrames() throws Exception {
        sendChunk = Math.max(sendChunk, 1_024);
        assertBackwardScanMatchesRecordCursor(1_500, false);
    }

    /**
     * A backward scan ({@code ORDER BY ts DESC}) hands out its page frames last
     * first, but each frame's rows are still stored ascending. The page-frame
     * path used to copy every frame in storage order, so a three-partition table
     * came back with each partition's rows ascending. It now copies each frame's
     * rows last first. The {@code LIMIT} variant, which has no page frames, is
     * the oracle.
     */
    @Test
    public void testBackwardScanStreamsRowsInDescendingOrder() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startFragmented(SMALL_PAGE_FRAME_ENV)) {
                serverMain.execute("CREATE TABLE bwd(x LONG, v VARCHAR, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY HOUR WAL");
                // 3 hourly partitions of 500 rows, 4 page frames each: x = 1..1500,
                // ts = (x - 1) * 7.2s
                serverMain.execute("INSERT INTO bwd SELECT x, 'v' || x, ((x - 1) * 7_200_000)::TIMESTAMP FROM long_sequence(1_500)");
                serverMain.awaitTable("bwd");

                try (QwpQueryClient client = QwpQueryClient.fromConfig("ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    final long[] descending = collectLongColumn(client, "SELECT x, v FROM bwd ORDER BY ts DESC");
                    Assert.assertArrayEquals(
                            collectLongColumn(client, "SELECT x, v FROM bwd ORDER BY ts DESC LIMIT 2_000"),
                            descending
                    );
                    Assert.assertEquals(1_500, descending.length);
                    for (int i = 0; i < descending.length; i++) {
                        Assert.assertEquals(1_500 - i, descending[i]);
                    }
                    // the forward scan keeps the page-frame path and its order
                    final long[] ascending = collectLongColumn(client, "SELECT x, v FROM bwd");
                    Assert.assertEquals(1_500, ascending.length);
                    for (int i = 0; i < ascending.length; i++) {
                        Assert.assertEquals(i + 1, ascending[i]);
                    }
                }
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
            try (final TestServerMain serverMain = startFragmented(SMALL_PAGE_FRAME_ENV)) {
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
            try (final TestServerMain serverMain = startFragmented(SMALL_PAGE_FRAME_ENV)) {
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
            try (final TestServerMain serverMain = startFragmented(SMALL_PAGE_FRAME_ENV)) {
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
            try (final TestServerMain serverMain = startFragmented(SMALL_PAGE_FRAME_ENV)) {
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
            try (final TestServerMain serverMain = startFragmented(SMALL_PAGE_FRAME_ENV)) {
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
            try (final TestServerMain serverMain = startFragmented(SMALL_PAGE_FRAME_ENV)) {
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
            try (final TestServerMain serverMain = startFragmented(SMALL_PAGE_FRAME_ENV)) {
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
            try (final TestServerMain serverMain = startFragmented(SMALL_PAGE_FRAME_ENV)) {
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
            try (final TestServerMain serverMain = startFragmented(SMALL_PAGE_FRAME_ENV)) {
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

    /**
     * Streams backward scans of {@code bwd_types}, see {@link #createAllTypesTable},
     * and checks each against the RecordCursor path, which the {@code LIMIT}
     * variant of the query takes, and the full scan against the forward scan in
     * reverse. Each runs over a connection with the default batch size, one with
     * 100-row batches, and one that also waits for credit every few KB.
     */
    private void assertBackwardScanMatchesRecordCursor(int rowsPerPartition, boolean isWal, String... env) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startFragmented(env)) {
                createAllTypesTable(serverMain, rowsPerPartition, isWal);
                final String[] queries = {
                        "SELECT * FROM bwd_types ORDER BY ts DESC",
                        // a projection that reorders the columns
                        "SELECT late_sy, l, v, ts, sh, late_bo, d4, c FROM bwd_types ORDER BY ts DESC",
                        // an interval scan over the middle, native partition
                        "SELECT * FROM bwd_types WHERE ts IN '1970-01-02' ORDER BY ts DESC",
                };
                final int[] rowCounts = {3 * rowsPerPartition, 3 * rowsPerPartition, rowsPerPartition};
                final String url = "ws::addr=127.0.0.1:" + HTTP_PORT + ";";
                for (int connection = 0; connection < 3; connection++) {
                    final QwpQueryClient client = switch (connection) {
                        case 0 -> QwpQueryClient.fromConfig(url);
                        case 1 -> QwpQueryClient.fromConfig(url + "max_batch_rows=100;");
                        default -> QwpQueryClient.fromConfig(url + "max_batch_rows=100;").withInitialCredit(4 * 1024);
                    };
                    try (client) {
                        client.connect();
                        ObjList<String> fullScan = null;
                        for (int q = 0; q < queries.length; q++) {
                            final String sql = queries[q];
                            final ObjList<String> backward = collectRows(client, sql);
                            Assert.assertEquals(sql, rowCounts[q], backward.size());
                            assertRows(sql, collectRows(client, sql + " LIMIT 1_000_000"), backward);
                            if (q == 0) {
                                fullScan = backward;
                            }
                        }
                        final ObjList<String> forward = collectRows(client, "SELECT * FROM bwd_types");
                        final ObjList<String> reversed = new ObjList<>(forward.size());
                        for (int i = forward.size() - 1; i > -1; i--) {
                            reversed.add(forward.getQuick(i));
                        }
                        assertRows("the forward scan in reverse", reversed, fullScan);
                    }
                }
            }
        });
    }
}
