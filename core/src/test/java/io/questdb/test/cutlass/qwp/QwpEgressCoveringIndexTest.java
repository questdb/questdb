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
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.std.Chars;
import io.questdb.std.str.StringSink;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Arrays;

/**
 * A covering-index (POSTING INCLUDE) projection over one symbol key must return the same rows on
 * every egress path as the plan without the covering index ({@code no_covering}).
 * <p>
 * QWP egress streams a factory that supports page frames through its page-frame path. A covering
 * single-key scan produces metadata-only frames: their raw column addresses are zero placeholders,
 * and only the page-frame memory pool's covered decode holds the values. The egress read the raw
 * addresses, so every fixed-width and SYMBOL column came back NULL with the right row count. The
 * tests here compare the covering plan with the {@code no_covering} plan over QWP, HTTP /exp and
 * pgwire, for every column type a covering index can include, a covered SYMBOL column, column tops,
 * NULLs, writes after the index was built (in order and O3), a key absent from some partitions and
 * Parquet partitions.
 */
public class QwpEgressCoveringIndexTest extends AbstractQwpBootstrapTest {
    private static final String COLUMNS = "ts, sym, s2, b, bt, sh, ch, i, l, f, d, dt, tn, vc, st, bin, u, l256, ip, g1, g2, g4, g8, d64, d128, d256, arr, late";
    private static final String INCLUDE = "s2, b, bt, sh, ch, i, l, f, d, dt, tn, vc, st, bin, u, l256, ip, g1, g2, g4, g8, d64, d128, d256, arr, late";
    // small frames: a covering scan spans several frames per partition
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

    @Test
    public void testCoveredProjectionMatchesNoCoveringOnAllPaths() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startFragmented(SMALL_PAGE_FRAME_ENV)) {
                createAndFill(serverMain, false);
                assertAllPaths(serverMain);
            }
        });
    }

    @Test
    public void testCoveredProjectionMatchesNoCoveringWithParquetPartitions() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startFragmented(SMALL_PAGE_FRAME_ENV)) {
                createAndFill(serverMain, true);
                assertAllPaths(serverMain);
            }
        });
    }

    @Test
    public void testCoveredProjectionMatchesNoCoveringWithDefaultFrames() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startFragmented()) {
                createAndFill(serverMain, false);
                assertAllPaths(serverMain);
            }
        });
    }

    private static void appendCell(StringSink sink, QwpColumnBatch batch, int c, int r) {
        if (batch.isNull(c, r)) {
            sink.put("null");
            return;
        }
        switch (batch.getColumnWireType(c)) {
            case QwpConstants.TYPE_BOOLEAN -> sink.put(batch.getBoolValue(c, r));
            case QwpConstants.TYPE_BYTE -> sink.put(batch.getByteValue(c, r));
            case QwpConstants.TYPE_SHORT -> sink.put(batch.getShortValue(c, r));
            case QwpConstants.TYPE_CHAR -> sink.put((int) batch.getCharValue(c, r));
            case QwpConstants.TYPE_INT, QwpConstants.TYPE_IPv4 -> sink.put(batch.getIntValue(c, r));
            case QwpConstants.TYPE_LONG, QwpConstants.TYPE_DATE, QwpConstants.TYPE_TIMESTAMP,
                 QwpConstants.TYPE_TIMESTAMP_NANOS, QwpConstants.TYPE_DECIMAL64 -> sink.put(batch.getLongValue(c, r));
            case QwpConstants.TYPE_FLOAT -> sink.put(Float.floatToRawIntBits(batch.getFloatValue(c, r)));
            case QwpConstants.TYPE_DOUBLE -> sink.put(Double.doubleToRawLongBits(batch.getDoubleValue(c, r)));
            case QwpConstants.TYPE_SYMBOL -> sink.put(batch.getSymbol(c, r));
            case QwpConstants.TYPE_VARCHAR -> sink.put(batch.getString(c, r));
            case QwpConstants.TYPE_BINARY -> sink.put(Arrays.toString(batch.getBinary(c, r)));
            case QwpConstants.TYPE_UUID -> sink.put(batch.getUuidHi(c, r)).put('/').put(batch.getUuidLo(c, r));
            case QwpConstants.TYPE_LONG256, QwpConstants.TYPE_DECIMAL256 -> {
                for (int w = 0; w < 4; w++) {
                    sink.put(batch.getLong256Word(c, r, w)).put('/');
                }
            }
            case QwpConstants.TYPE_DECIMAL128 ->
                    sink.put(batch.getDecimal128High(c, r)).put('/').put(batch.getDecimal128Low(c, r));
            case QwpConstants.TYPE_GEOHASH -> sink.put(batch.getGeohashValue(c, r));
            case QwpConstants.TYPE_DOUBLE_ARRAY -> sink.put(Arrays.toString(batch.getDoubleArrayElements(c, r)));
            default -> Assert.fail("unexpected wire type " + batch.getColumnWireType(c) + " in column " + c);
        }
    }

    private static void assertAllPaths(TestServerMain serverMain) throws Exception {
        final String[] keys = {"k0", "k3", "rare", "absent"};
        final String[] shapes = {
                "SELECT " + COLUMNS + " FROM t WHERE sym = '%s'",
                "SELECT ts, d, l, s2, sym, i, late FROM t WHERE sym = '%s'",
                "SELECT s2, f, vc, bin FROM t WHERE sym = '%s' AND ts >= '1970-01-02' AND ts < '1970-01-03T12'",
                "SELECT ts, l, s2 FROM t WHERE sym = '%s' ORDER BY ts DESC",
                "SELECT ts, d, s2 FROM t WHERE sym = '%s' LIMIT 7",
                "SELECT ts, d, s2 FROM t WHERE sym = '%s' LIMIT -7",
        };
        int nonEmpty = 0;
        for (String shape : shapes) {
            for (String key : keys) {
                // the frequent keys may take the scan by default; use_index keeps them on the covering path
                for (String hint : new String[]{"", "/*+ use_index */ "}) {
                    assertPaths(serverMain, String.format(shape, key).replace("SELECT ", "SELECT " + hint), shape, key, hint.isEmpty() && !"rare".equals(key));
                }
                if (!"absent".equals(key)) {
                    nonEmpty++;
                }
            }
        }
        Assert.assertEquals(shapes.length * 3, nonEmpty);
    }

    private static void assertPaths(TestServerMain serverMain, String sql, String shape, String key, boolean mayScan) throws Exception {
        final String reference = sql.replace("/*+ use_index */ ", "").replace("SELECT ", "SELECT /*+ no_covering */ ");
        if (!mayScan && !"absent".equals(key) && shape.startsWith("SELECT " + COLUMNS)) {
            assertPlanContains(serverMain, sql, "CoveringIndex");
        }
        final String qwp = qwp(sql);
        Assert.assertEquals("QWP covering vs no_covering: " + sql, qwp(reference), qwp);
        Assert.assertEquals("HTTP /exp covering vs no_covering: " + sql, exp(reference), exp(sql));
        Assert.assertEquals("pgwire covering vs no_covering: " + sql, pg(reference), pg(sql));
        if (!"absent".equals(key)) {
            // not vacuous: the result has rows, and a covered column holds values
            Assert.assertTrue("rows expected: " + sql, qwp.indexOf('\n') > 0);
            Assert.assertTrue("non-null covered values expected: " + sql + '\n' + qwp, hasNonNullValue(qwp));
        } else {
            Assert.assertEquals("", qwp);
        }
    }

    private static void assertPlanContains(TestServerMain serverMain, String query, String fragment) throws Exception {
        final StringSink sink = new StringSink();
        TestUtils.printSql(serverMain.getEngine(), serverMain.getSqlExecutionContext(), "explain " + query, sink);
        Assert.assertTrue("plan must contain [" + fragment + "]: " + sink, Chars.contains(sink, fragment));
    }

    private static void createAndFill(TestServerMain serverMain, boolean parquet) {
        serverMain.execute("""
                CREATE TABLE t (
                    ts TIMESTAMP, sym SYMBOL, s2 SYMBOL, b BOOLEAN, bt BYTE, sh SHORT, ch CHAR, i INT, l LONG,
                    f FLOAT, d DOUBLE, dt DATE, tn TIMESTAMP_NS, vc VARCHAR, st STRING, bin BINARY, u UUID,
                    l256 LONG256, ip IPv4, g1 GEOHASH(3b), g2 GEOHASH(2c), g4 GEOHASH(5c), g8 GEOHASH(10c),
                    d64 DECIMAL(12, 3), d128 DECIMAL(30, 4), d256 DECIMAL(60, 5), arr DOUBLE[]
                ) TIMESTAMP(ts) PARTITION BY DAY WAL
                """);
        // days 1-3, 1500 rows; 'rare' only on days 1 and 3, so it is absent from day 2
        insertRows(serverMain, 1, 1500, 0);
        serverMain.awaitTable("t");
        // a column added after the first rows: a column top in every existing partition
        serverMain.execute("ALTER TABLE t ADD COLUMN late LONG");
        insertRows(serverMain, 1501, 600, 7);
        serverMain.awaitTable("t");
        serverMain.execute("ALTER TABLE t ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (" + INCLUDE + ")");
        serverMain.awaitTable("t");
        // after the index: in-order appends on the last day, and O3 rows into the first two days
        insertRows(serverMain, 2101, 300, 11);
        insertO3Rows(serverMain);
        serverMain.awaitTable("t");
        if (parquet) {
            serverMain.execute("ALTER TABLE t CONVERT PARTITION TO PARQUET WHERE ts < '1970-01-03'");
            serverMain.awaitTable("t");
        }
    }

    private static String exp(String sql) throws Exception {
        final String url = "http://127.0.0.1:" + HTTP_PORT + "/exp?query=" + URLEncoder.encode(sql, StandardCharsets.UTF_8);
        final HttpURLConnection connection = (HttpURLConnection) URI.create(url).toURL().openConnection();
        try {
            Assert.assertEquals(sql, 200, connection.getResponseCode());
            try (InputStream in = connection.getInputStream()) {
                final ByteArrayOutputStream out = new ByteArrayOutputStream();
                in.transferTo(out);
                return out.toString(StandardCharsets.UTF_8);
            }
        } finally {
            connection.disconnect();
        }
    }

    private static boolean hasNonNullValue(String rendered) {
        // the first column of every shape is a fixed-width or SYMBOL covered column (ts or s2),
        // the columns that came back NULL
        for (String line : rendered.split("\n")) {
            final int end = line.indexOf('|');
            final String cell = end < 0 ? line : line.substring(0, end);
            if (!"null".equals(cell)) {
                return true;
            }
        }
        return false;
    }

    private static void insertO3Rows(TestServerMain serverMain) {
        // 120 rows interleaved into day 1 and day 2, below their existing timestamps' maximum
        serverMain.execute("""
                INSERT INTO t (""" + COLUMNS + """
                ) SELECT
                    CAST(((x - 1) % 2) * 86_400_000_000L + 500_000 + (x - 1) * 7_000_000L AS TIMESTAMP),
                    'k' || (x % 5), 's' || (x % 4), x % 2 = 0, CAST(x AS BYTE), CAST(x AS SHORT), rnd_char(),
                    CAST(x * 3 AS INT), x * 1000, CAST(x / 3.0 AS FLOAT), x / 7.0, CAST(x * 86_400_000L AS DATE),
                    CAST(x * 1_000_000_000L AS TIMESTAMP_NS), 'o3v' || x, 'o3s' || x, rnd_bin(4, 4, 0),
                    rnd_uuid4(), rnd_long256(), CAST('10.1.0.' || (x % 200) AS IPv4),
                    rnd_geohash(3), rnd_geohash(10), rnd_geohash(25), rnd_geohash(50),
                    CAST(x * 1.25 AS DECIMAL(12, 3)), CAST(x * 2.5 AS DECIMAL(30, 4)), CAST(x * 5.125 AS DECIMAL(60, 5)),
                    ARRAY[x * 1.0, x * 3.0], x * 13
                FROM long_sequence(120)
                """);
    }

    private static void insertRows(TestServerMain serverMain, long from, long count, int lateMultiplier) {
        // x runs from 1; row n = from + x - 1; 500 rows per day; key 'rare' every 97th row, never on day 2
        final String n = "(x + " + (from - 1) + ")";
        serverMain.execute("INSERT INTO t (" + COLUMNS.replace(", late", "") + (lateMultiplier == 0 ? "" : ", late") + ") SELECT "
                + "CAST(86_400_000_000L * ((" + n + " - 1) / 700) + (" + n + " - 1) * 1_000_000L AS TIMESTAMP),"
                + "CASE WHEN " + n + " % 97 = 0 AND ((" + n + " - 1) / 700) <> 1 THEN 'rare' ELSE 'k' || (" + n + " % 5) END,"
                + "CASE WHEN " + n + " % 11 = 0 THEN NULL ELSE 's' || (" + n + " % 4) END,"
                + n + " % 3 = 0,"
                + "CAST(" + n + " % 120 AS BYTE),"
                + "CAST(" + n + " % 30000 AS SHORT),"
                + "rnd_char(),"
                + "CASE WHEN " + n + " % 13 = 0 THEN NULL ELSE CAST(" + n + " * 7 AS INT) END,"
                + "CASE WHEN " + n + " % 17 = 0 THEN NULL ELSE " + n + " * 100_003 END,"
                + "CASE WHEN " + n + " % 19 = 0 THEN NULL ELSE CAST(" + n + " / 9.0 AS FLOAT) END,"
                + "CASE WHEN " + n + " % 23 = 0 THEN NULL ELSE " + n + " * 1.5 END,"
                + "CASE WHEN " + n + " % 29 = 0 THEN NULL ELSE CAST(" + n + " * 86_400_000L AS DATE) END,"
                + "CASE WHEN " + n + " % 31 = 0 THEN NULL ELSE CAST(" + n + " * 1_000_000_007L AS TIMESTAMP_NS) END,"
                + "CASE WHEN " + n + " % 37 = 0 THEN NULL WHEN " + n + " % 5 = 0 THEN '' ELSE 'varchar_value_' || " + n + " END,"
                + "CASE WHEN " + n + " % 41 = 0 THEN NULL ELSE 'str' || " + n + " END,"
                + "CASE WHEN " + n + " % 43 = 0 THEN NULL ELSE rnd_bin(1, 9, 0) END,"
                + "CASE WHEN " + n + " % 47 = 0 THEN NULL ELSE rnd_uuid4() END,"
                + "CASE WHEN " + n + " % 53 = 0 THEN NULL ELSE rnd_long256() END,"
                + "CASE WHEN " + n + " % 59 = 0 THEN NULL ELSE CAST('192.168.' || (" + n + " % 250) || '.' || (" + n + " % 199) AS IPv4) END,"
                + "rnd_geohash(3), rnd_geohash(10), rnd_geohash(25), rnd_geohash(50),"
                + "CASE WHEN " + n + " % 61 = 0 THEN NULL ELSE CAST(" + n + " * 1.125 AS DECIMAL(12, 3)) END,"
                + "CASE WHEN " + n + " % 67 = 0 THEN NULL ELSE CAST(" + n + " * 3.0625 AS DECIMAL(30, 4)) END,"
                + "CASE WHEN " + n + " % 71 = 0 THEN NULL ELSE CAST(" + n + " * 7.03125 AS DECIMAL(60, 5)) END,"
                + "CASE WHEN " + n + " % 73 = 0 THEN NULL ELSE ARRAY[" + n + " * 1.0, " + n + " * 0.5, -" + n + " * 1.0] END"
                + (lateMultiplier == 0 ? "" : ", CASE WHEN " + n + " % 79 = 0 THEN NULL ELSE " + n + " * " + lateMultiplier + " END")
                + " FROM long_sequence(" + count + ")");
    }

    private static String pg(String sql) throws Exception {
        final StringSink sink = new StringSink();
        try (Connection connection = getConnection("admin", "quest", PG_PORT); Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                final ResultSetMetaData md = rs.getMetaData();
                final int columnCount = md.getColumnCount();
                while (rs.next()) {
                    for (int c = 1; c <= columnCount; c++) {
                        if (c > 1) {
                            sink.put('|');
                        }
                        final Object value = rs.getObject(c);
                        if (value instanceof byte[] bytes) {
                            sink.put(Arrays.toString(bytes));
                        } else if (value instanceof java.sql.Array array) {
                            sink.put(Arrays.deepToString((Object[]) array.getArray()));
                        } else {
                            sink.put(String.valueOf(value));
                        }
                    }
                    sink.put('\n');
                }
            }
        }
        return sink.toString();
    }

    private static String qwp(String sql) {
        final StringSink sink = new StringSink();
        try (QwpQueryClient client = QwpQueryClient.fromConfig("ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
            client.connect();
            client.execute(sql, new QwpColumnBatchHandler() {
                @Override
                public void onBatch(QwpColumnBatch batch) {
                    for (int r = 0, n = batch.getRowCount(); r < n; r++) {
                        for (int c = 0, m = batch.getColumnCount(); c < m; c++) {
                            if (c > 0) {
                                sink.put('|');
                            }
                            appendCell(sink, batch, c, r);
                        }
                        sink.put('\n');
                    }
                }

                @Override
                public void onEnd(long totalRows) {
                }

                @Override
                public void onError(byte status, String message) {
                    Assert.fail("egress error: " + message + " [sql=" + sql + ']');
                }
            });
        }
        return sink.toString();
    }
}
