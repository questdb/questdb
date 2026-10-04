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
import io.questdb.cutlass.qwp.codec.QwpEgressMsgKind;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.cutlass.qwp.protocol.QwpVarint;
import io.questdb.cutlass.qwp.server.egress.QwpEgressMetrics;
import io.questdb.cutlass.qwp.server.egress.QwpEgressUpgradeProcessor;
import io.questdb.cutlass.qwp.websocket.WebSocketOpcode;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Cursor results that offer {@link io.questdb.cairo.sql.RecordBlock}s are filled column by column
 * ({@code QwpResultBatchBuffer.appendBlock}). The client must receive exactly the bytes the row
 * by row fill ({@code appendRow}) sends: every test runs the same exchange twice on a fresh
 * connection, once with the block fill switched off and once on, and compares every frame the
 * server sent, byte for byte. Each also checks that the block fill did run, so that no comparison
 * is between two row fills.
 */
public class QwpEgressBlockFillTest extends AbstractBootstrapTest {
    private static final String ALL_TYPES_DDL = "create table at (" +
            "b boolean, by byte, sh short, ch char, i int, ip ipv4, l long, d date, ts timestamp, tn timestamp_ns, " +
            "f float, db double, s symbol, s2 symbol, u uuid, l256 long256, g1 geohash(1c), g3 geohash(3c), g6 geohash(6c), " +
            "g12 geohash(12c), dc64 decimal(12,2), dc128 decimal(30,4), dc256 decimal(60,6), v varchar, st string, bn binary, " +
            "arr double[]) timestamp(ts) partition by DAY BYPASS WAL";
    private static final String ALL_TYPES_SELECT = "select x % 3 = 0, (x % 120)::byte, (x * 7 % 30000)::short, rnd_char(), " +
            "case when x % 11 = 0 then null else x::int end, case when x % 13 = 0 then null else rnd_ipv4() end, " +
            "case when x % 5 = 0 then null else x * 1000003 end, case when x % 9 = 0 then null else (x * 86400000)::date end, " +
            "(x * 30000000)::timestamp, case when x % 6 = 0 then null else (x * 1000)::timestamp_ns end, " +
            "case when x % 4 = 0 then null else (x / 3.0)::float end, case when x % 7 = 0 then null else x * 1.5 end, " +
            "case when x % 17 = 0 then null else 'k' || (x % 50) end, 'z' || (x % 7), rnd_uuid4(), rnd_long256(), " +
            "rnd_geohash(5), rnd_geohash(15), rnd_geohash(30), rnd_geohash(60), rnd_decimal(12,2,5), rnd_decimal(30,4,5), " +
            "rnd_decimal(60,6,5), case when x % 8 = 0 then null else 'v' || x end, case when x % 10 = 0 then null else 's' || x end, " +
            "rnd_bin(1, 8, 3), rnd_double_array(1, 1)";
    // idx 50's shape, with passthrough columns of every fixed-size type a task chain holds
    private static final String WINDOW_SQL = "select sym, ex, b, by, sh, ch, i, ip, l, d, f, db, u, g6, dc64, ts, " +
            "avg((bsize * bid + asize * ask) / (bsize + asize)) over (partition by sym rows between 4 preceding and current row) mid, " +
            "avg(asize + bsize) over (partition by sym rows between 4 preceding and current row) size " +
            "from q where sym in ('A', 'B', 'C1', 'C2', 'C3', 'C5', 'C8', 'D') order by sym";

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @After
    public void resetBlockFill() {
        QwpEgressUpgradeProcessor.DEBUG_DISABLE_BLOCK_FILL = false;
    }

    @Test
    public void testAllTypesAcrossManyBatches() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startWithEnvVariables(smallFrames())) {
                createAllTypes(serverMain, 20_000);
                // LIMIT over a table scan is a cursor result: the scan offers its frames
                final String[] sqls = {
                        "select * from at limit 1000000",
                        "select * from at limit 13, 7013",
                        "select * from at limit -2500",
                        "select ts, s, db, l, s2, b from at limit 15000",
                };
                assertBlockFillMatchesRowFill(sqls, "?qwp_max_batch_rows=1000", 0, -1);
                assertBlockFillMatchesRowFill(sqls, "", 0, -1);
            }
        });
    }

    @Test
    public void testAsyncWindow() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startWithEnvVariables(windowEnv())) {
                createWindowTable(serverMain);
                // alone first: only the Async Window offers blocks here, so the block fill ran on its tasks
                assertBlockFillMatchesRowFill(new String[]{WINDOW_SQL}, "", 0, -1);
                final String[] sqls = {
                        WINDOW_SQL,
                        "select * from (" + WINDOW_SQL + ") limit 7, 20007",
                        // twice on one connection: the second query reuses the connection's symbols
                        WINDOW_SQL,
                };
                assertBlockFillMatchesRowFill(sqls, "?qwp_max_batch_rows=777", 0, -1);
                assertBlockFillMatchesRowFill(sqls, "", 0, -1);
            }
        });
    }

    @Test
    public void testCancelMidStream() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startWithEnvVariables(windowEnv())) {
                createWindowTable(serverMain);
                createAllTypes(serverMain, 5_000);
                // credit of 1 byte: the server parks after the first batch, the CANCEL lands, the
                // next CREDIT resumes it into the cancellation; then a full query on the same connection
                assertBlockFillMatchesRowFill(new String[]{WINDOW_SQL, WINDOW_SQL}, "?qwp_max_batch_rows=500", 1, 0);
                assertBlockFillMatchesRowFill(new String[]{"select * from at limit 4000", "select * from at limit 4000"},
                        "?qwp_max_batch_rows=500", 1, 0);
            }
        });
    }

    @Test
    public void testCreditFlow() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startWithEnvVariables(windowEnv())) {
                createWindowTable(serverMain);
                createAllTypes(serverMain, 5_000);
                final QwpEgressMetrics metrics = serverMain.getEngine().getMetrics().qwpEgressMetrics();
                final long suspensions = metrics.creditSuspensionsCount();
                // a small initial credit, topped up by each batch's size as the client reads it
                assertBlockFillMatchesRowFill(new String[]{WINDOW_SQL, "select * from at limit 5000"}, "?qwp_max_batch_rows=300", 4096, -1);
                Assert.assertTrue("the streams must have parked on credit", metrics.creditSuspensionsCount() > suspensions);
            }
        });
    }

    @Test
    public void testNewSymbolsSplitBatchesOnTheDictionaryBudget() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // the smallest send buffer the handshake accepts: the dictionary budget is 60% of it
            try (TestServerMain serverMain = startWithEnvVariables(
                    "QDB_HTTP_SEND_BUFFER_SIZE", "163840",
                    "QDB_METRICS_ENABLED", "true",
                    PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS.getEnvVarName(), "1000")) {
                // two SYMBOL columns of long values, new ones arriving all through the result
                serverMain.execute("create table sy (a symbol capacity 65536, n long, b symbol capacity 65536, d double, ts timestamp) " +
                        "timestamp(ts) partition by DAY BYPASS WAL");
                serverMain.execute("insert into sy select " +
                        "case when x % 31 = 0 then null else rpad('a' || (x % 20000), 150, '.') end, x, " +
                        "case when x % 3 = 0 then rpad('b' || (x % 9000), 120, '-') else 'b' end, x * 0.25, " +
                        "(x * 1000000)::timestamp from long_sequence(40000)");
                final QwpEgressMetrics metrics = serverMain.getEngine().getMetrics().qwpEgressMetrics();
                final long splits = metrics.batchOverflowSplitCount();
                final String[] sqls = {
                        "select * from sy limit 100000",
                        "select b, n, a from sy limit 100, 35000",
                        "select * from sy limit 100000",
                };
                assertBlockFillMatchesRowFill(sqls, "", 0, -1);
                Assert.assertTrue("the dictionary budget must have split batches", metrics.batchOverflowSplitCount() > splits);
            }
        });
    }

    private static byte[] buildCancel(long requestId) {
        final byte[] p = new byte[9];
        p[0] = QwpEgressMsgKind.CANCEL;
        for (int s = 0; s < 8; s++) {
            p[1 + s] = (byte) (requestId >>> (8 * s));
        }
        return p;
    }

    private static byte[] buildQueryRequest(long requestId, String sql, long initialCredit) {
        final byte[] sqlBytes = sql.getBytes(StandardCharsets.UTF_8);
        final byte[] p = new byte[1 + 8 + 10 + sqlBytes.length + 10 + 1];
        int i = 0;
        p[i++] = QwpEgressMsgKind.QUERY_REQUEST;
        for (int s = 0; s < 8; s++) {
            p[i++] = (byte) (requestId >>> (8 * s));
        }
        i = QwpVarint.encode(p, i, sqlBytes.length);
        System.arraycopy(sqlBytes, 0, p, i, sqlBytes.length);
        i += sqlBytes.length;
        i = QwpVarint.encode(p, i, initialCredit);
        p[i++] = 0; // bind_count
        return Arrays.copyOf(p, i);
    }

    private static void createAllTypes(TestServerMain serverMain, int rows) {
        // the first rows have column tops for the columns added later
        serverMain.execute(ALL_TYPES_DDL.replace(", u uuid, l256 long256, g1 geohash(1c), g3 geohash(3c), g6 geohash(6c), " +
                "g12 geohash(12c), dc64 decimal(12,2), dc128 decimal(30,4), dc256 decimal(60,6), v varchar, st string, bn binary, " +
                "arr double[]", ""));
        serverMain.execute("insert into at select x % 3 = 0, (x % 120)::byte, (x * 7 % 30000)::short, rnd_char(), x::int, rnd_ipv4(), x, " +
                "(x * 86400000)::date, (x * 30000000)::timestamp, (x * 1000)::timestamp_ns, (x / 3.0)::float, x * 1.5, " +
                "'k' || (x % 50), 'z' || (x % 7) from long_sequence(1000)");
        serverMain.execute("alter table at add column u uuid, l256 long256, g1 geohash(1c), g3 geohash(3c), g6 geohash(6c), " +
                "g12 geohash(12c), dc64 decimal(12,2), dc128 decimal(30,4), dc256 decimal(60,6), v varchar, st string, bn binary, arr double[]");
        serverMain.execute("insert into at " + ALL_TYPES_SELECT.replace("(x * 30000000)::timestamp", "((x + 1000) * 30000000)::timestamp")
                + " from long_sequence(" + rows + ")");
    }

    private static void createWindowTable(TestServerMain serverMain) {
        // keys A and B above max.key.rows, the C keys small, NULL keys too
        serverMain.execute("create table q (sym symbol index type posting, ex symbol, b boolean, by byte, sh short, ch char, " +
                "i int, ip ipv4, l long, d date, f float, db double, u uuid, g6 geohash(6c), dc64 decimal(12,2), " +
                "bid float, bsize int, ask float, asize int, ts timestamp) timestamp(ts) partition by DAY BYPASS WAL");
        serverMain.execute("insert into q select " +
                "case when x % 3 = 0 then 'A' when x % 5 = 0 then 'B' when x % 23 = 0 then null when x % 29 = 0 then 'D' else 'C' || (x % 9) end, " +
                "rnd_symbol('N', 'P', 'Q', null), x % 2 = 0, (x % 100)::byte, (x % 999)::short, rnd_char(), " +
                "case when x % 11 = 0 then null else x::int end, rnd_ipv4(), case when x % 13 = 0 then null else x end, " +
                "(x * 1000)::date, case when x % 4 = 0 then null else (x / 7.0)::float end, x * 0.5, rnd_uuid4(), rnd_geohash(30), " +
                "rnd_decimal(12,2,5), case when x % 7 = 0 then null else rnd_float() * 100 end, rnd_int(1, 100, 0), " +
                "rnd_float() * 100, rnd_int(1, 100, 0), (x * 3000000)::timestamp from long_sequence(60000)");
    }

    private static String describe(List<byte[]> frames, int index) {
        final byte[] f = frames.get(index);
        return "frame " + index + " of " + frames.size() + ", kind 0x" + Integer.toHexString(f[QwpConstants.HEADER_SIZE] & 0xFF) + ", " + f.length + " bytes";
    }

    private static String[] smallFrames() {
        return new String[]{
                PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS.getEnvVarName(), "128",
                PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS.getEnvVarName(), "64",
        };
    }

    private static String[] windowEnv() {
        return new String[]{
                PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS.getEnvVarName(), "128",
                PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS.getEnvVarName(), "64",
                PropertyKey.SHARED_QUERY_WORKER_COUNT.getEnvVarName(), "2",
                PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_TASK_ROWS.getEnvVarName(), "500",
                PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_MAX_KEY_ROWS.getEnvVarName(), "4000",
                PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_MIN_ROWS.getEnvVarName(), "1000",
                PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_ROUND_ROWS.getEnvVarName(), "3000",
                "QDB_METRICS_ENABLED", "true",
        };
    }

    /**
     * Runs the queries one after another on one connection, with the block fill off and then on,
     * and asserts both runs received the same frames.
     *
     * @param initialCredit      0 for an unbounded stream; else the query's initial credit,
     *                           topped up by each batch's size as it arrives
     * @param cancelAfterBatches -1 for none; else the batches after which the first query is
     *                           cancelled (with a 1-byte credit, the server parks there)
     */
    private void assertBlockFillMatchesRowFill(String[] sqls, String urlQuery, long initialCredit, int cancelAfterBatches) throws Exception {
        QwpEgressUpgradeProcessor.DEBUG_DISABLE_BLOCK_FILL = true;
        final long blockRowsBefore = QwpEgressUpgradeProcessor.BLOCK_FILL_ROWS.sum();
        final List<byte[]> rowFill = exchange(sqls, urlQuery, initialCredit, cancelAfterBatches);
        Assert.assertEquals("the row fill must not use blocks", blockRowsBefore, QwpEgressUpgradeProcessor.BLOCK_FILL_ROWS.sum());
        QwpEgressUpgradeProcessor.DEBUG_DISABLE_BLOCK_FILL = false;
        final List<byte[]> blockFill = exchange(sqls, urlQuery, initialCredit, cancelAfterBatches);
        Assert.assertTrue("the block fill must have run", QwpEgressUpgradeProcessor.BLOCK_FILL_ROWS.sum() > blockRowsBefore);
        Assert.assertTrue(rowFill.size() > sqls.length);
        for (int i = 0, n = Math.min(rowFill.size(), blockFill.size()); i < n; i++) {
            if (!Arrays.equals(rowFill.get(i), blockFill.get(i))) {
                Assert.fail("block fill differs at " + describe(blockFill, i) + "; row fill: " + describe(rowFill, i)
                        + ", first differing byte " + Arrays.mismatch(rowFill.get(i), blockFill.get(i)));
            }
        }
        Assert.assertEquals("frame count", rowFill.size(), blockFill.size());
    }

    /**
     * @return every frame the server sent for the queries, SERVER_INFO excluded
     */
    private List<byte[]> exchange(String[] sqls, String urlQuery, long initialCredit, int cancelAfterBatches) throws Exception {
        final List<byte[]> frames = new ArrayList<>();
        // the same request ids in both runs: they are in every frame
        long requestIdSeq = 1;
        try (Socket socket = new Socket("127.0.0.1", HTTP_PORT)) {
            socket.setSoTimeout(120_000);
            QwpWireTestFixtures.performReadHandshake(socket, urlQuery);
            final OutputStream out = socket.getOutputStream();
            final InputStream in = new BufferedInputStream(socket.getInputStream(), 1 << 16);
            for (int q = 0; q < sqls.length; q++) {
                final long requestId = requestIdSeq++;
                final boolean cancel = q == 0 && cancelAfterBatches >= 0;
                out.write(QwpWireTestFixtures.maskedFrame(WebSocketOpcode.BINARY, buildQueryRequest(requestId, sqls[q], initialCredit)));
                out.flush();
                int batches = 0;
                while (true) {
                    final byte[] frame = QwpWireTestFixtures.readServerFrame(in);
                    final byte kind = frame[QwpConstants.HEADER_SIZE];
                    if (kind == QwpEgressMsgKind.SERVER_INFO) {
                        continue;
                    }
                    frames.add(frame);
                    if (kind == QwpEgressMsgKind.RESULT_END || kind == QwpEgressMsgKind.QUERY_ERROR) {
                        Assert.assertEquals("only a cancelled query may fail", cancel, kind == QwpEgressMsgKind.QUERY_ERROR);
                        break;
                    }
                    if (kind == QwpEgressMsgKind.RESULT_BATCH) {
                        batches++;
                        if (cancel && batches == cancelAfterBatches + 1) {
                            out.write(QwpWireTestFixtures.maskedFrame(WebSocketOpcode.BINARY, buildCancel(requestId)));
                        }
                        if (initialCredit > 0) {
                            out.write(QwpWireTestFixtures.maskedFrame(WebSocketOpcode.BINARY,
                                    QwpWireTestFixtures.buildCreditFrame(requestId, cancel ? 1 : frame.length)));
                        }
                        out.flush();
                    }
                }
            }
        }
        return frames;
    }
}
