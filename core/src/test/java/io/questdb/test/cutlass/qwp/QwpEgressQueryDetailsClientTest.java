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
import io.questdb.client.Query;
import io.questdb.client.QueryException;
import io.questdb.client.QuestDB;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;
import io.questdb.client.cutlass.qwp.client.QwpQueryClient;
import io.questdb.client.cutlass.qwp.client.QwpQueryDetails;
import io.questdb.client.cutlass.qwp.client.QwpQueryOptions;
import io.questdb.cutlass.qwp.codec.QwpEgressMsgKind;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.griffin.CompiledQuery;
import io.questdb.jit.JitUtil;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * The query-details flags of {@code QUERY_REQUEST} through the pinned
 * {@code java-questdb-client}: its {@link QwpQueryOptions} put them on the wire
 * and its handler callbacks and {@link QueryException} receive what the server
 * answers. Checked against independent oracles: SQL {@code LIMIT} and the full
 * result for row windows and counts, and /exec on the same server for error
 * positions and JIT use. {@link QwpEgressQueryDetailsWireTest} pins the frames
 * byte by byte.
 */
public class QwpEgressQueryDetailsClientTest extends AbstractQwpQueryDetailsTest {

    @Test
    public void testClientConstantsMatchServer() {
        Assert.assertEquals(QwpEgressMsgKind.CAP_QUERY_DETAILS,
                io.questdb.client.cutlass.qwp.client.QwpEgressMsgKind.CAP_QUERY_DETAILS);
        Assert.assertEquals(QwpEgressMsgKind.QUERY_FLAG_ERROR_POSITION,
                io.questdb.client.cutlass.qwp.client.QwpEgressMsgKind.QUERY_FLAG_ERROR_POSITION);
        Assert.assertEquals(QwpEgressMsgKind.QUERY_FLAG_STATS,
                io.questdb.client.cutlass.qwp.client.QwpEgressMsgKind.QUERY_FLAG_STATS);
        Assert.assertEquals(QwpEgressMsgKind.QUERY_FLAG_COUNT_ROWS,
                io.questdb.client.cutlass.qwp.client.QwpEgressMsgKind.QUERY_FLAG_COUNT_ROWS);
        Assert.assertEquals(QwpEgressMsgKind.QUERY_FLAG_SKIP_ROWS,
                io.questdb.client.cutlass.qwp.client.QwpEgressMsgKind.QUERY_FLAG_SKIP_ROWS);
        Assert.assertEquals(QwpEgressMsgKind.QUERY_FLAG_MAX_ROWS,
                io.questdb.client.cutlass.qwp.client.QwpEgressMsgKind.QUERY_FLAG_MAX_ROWS);
        Assert.assertEquals(QwpEgressMsgKind.NO_ERROR_POSITION,
                io.questdb.client.cutlass.qwp.client.QwpEgressMsgKind.NO_ERROR_POSITION);
        Assert.assertEquals(QwpEgressMsgKind.STATS_FLAG_JIT_COMPILED,
                io.questdb.client.cutlass.qwp.client.QwpEgressMsgKind.STATS_FLAG_JIT_COMPILED);
    }

    @Test
    public void testErrorPositionMatchesExec() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startFragmented();
                 QwpQueryClient client = newClient()) {
                client.connect();
                Assert.assertEquals(QwpEgressMsgKind.CAP_QUERY_DETAILS,
                        client.getServerInfo().getCapabilities() & QwpEgressMsgKind.CAP_QUERY_DETAILS);
                createWindowTable(serverMain, 10);
                serverMain.execute("CREATE TABLE qd_arr (a DOUBLE[], b DOUBLE[])");
                serverMain.execute("INSERT INTO qd_arr VALUES (ARRAY[1.0, 2.0], ARRAY[1.0, 2.0, 3.0])");
                // {sql, status, bare message, position}
                final Object[][] cases = {
                        {"SELECT x FROM qd_missing", QwpConstants.STATUS_PARSE_ERROR,
                                "table does not exist [table=qd_missing]", 14},
                        {"SELECT nope FROM qd_window", QwpConstants.STATUS_PARSE_ERROR, "Invalid column: nope", 7},
                        // a non-SELECT that fails at execution
                        {"DROP TABLE qd_missing", QwpConstants.STATUS_PARSE_ERROR,
                                "table does not exist [table=qd_missing]", 11},
                        // a runtime error raised while the rows stream
                        {"SELECT a + b FROM qd_arr", QwpConstants.STATUS_INTERNAL_ERROR,
                                "arrays have incompatible shapes [leftShape=[2], rightShape=[3]]", 7},
                        // the position counts chars, not UTF-8 bytes
                        {"SELECT 'é€\uD83D\uDE00' s FROM qd_missing", QwpConstants.STATUS_PARSE_ERROR,
                                "table does not exist [table=qd_missing]", 21},
                };
                final QwpQueryOptions withPosition = new QwpQueryOptions().errorPosition(true);
                for (Object[] c : cases) {
                    final String sql = (String) c[0];
                    final Collector withPos = new Collector();
                    client.execute(sql, withPos, withPosition);
                    Assert.assertEquals(sql, 1, withPos.errorCount);
                    Assert.assertEquals(sql, (byte) c[1], withPos.errorStatus);
                    Assert.assertEquals(sql, c[2], withPos.errorMessage);
                    Assert.assertEquals(sql, (int) c[3], withPos.errorPosition);
                    Assert.assertEquals(sql, execPosition(sql), withPos.errorPosition);

                    // without the option, the message keeps the [n] prefix of the
                    // baseline frame and the callback gets no position
                    final Collector plain = new Collector();
                    client.execute(sql, plain);
                    Assert.assertEquals(sql, 1, plain.errorCount);
                    TestUtils.assertContains(plain.errorMessage, "] " + c[2]);
                    Assert.assertTrue(sql, plain.errorMessage.startsWith("["));
                    Assert.assertEquals(sql, QwpEgressMsgKind.NO_ERROR_POSITION, plain.errorPosition);
                }
                final Collector plain = new Collector();
                client.execute("SELECT x FROM qd_missing", plain);
                Assert.assertEquals("[14] table does not exist [table=qd_missing]", plain.errorMessage);
            }
        });
    }

    @Test
    public void testFacadeQueryOptions() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startFragmented()) {
                createWindowTable(serverMain, 100);
                try (QuestDB db = QuestDB.connect("ws::addr=127.0.0.1:" + HTTP_PORT
                        + ";sender_pool_min=0;query_pool_min=1;query_pool_max=1;");
                     Query q = db.borrowQuery()) {
                    final Collector page = new Collector();
                    q.sql("SELECT x FROM qd_window ORDER BY ts DESC")
                            .options(new QwpQueryOptions().skipRows(10).maxRows(5).countRows(true).stats(true))
                            .handler(page)
                            .submit()
                            .await();
                    assertSequence(page.longs, 89, -1, 5);
                    Assert.assertEquals(5, page.totalRows);
                    Assert.assertEquals(100, page.rowCount);
                    Assert.assertTrue(page.hasStats);

                    q.sql("SELECT nope FROM qd_window")
                            .options(new QwpQueryOptions().errorPosition(true))
                            .handler(new Collector());
                    try {
                        q.submit().await();
                        Assert.fail("the query must fail");
                    } catch (QueryException e) {
                        Assert.assertEquals(QwpConstants.STATUS_PARSE_ERROR, e.getStatus());
                        Assert.assertEquals("Invalid column: nope", e.getMessage());
                        Assert.assertEquals(7, e.getPosition());
                    }
                }
            }
        });
    }

    @Test
    public void testRowWindowAndCountMatchSqlLimit() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startFragmented();
                 QwpQueryClient client = QwpQueryClient.fromConfig("ws::addr=127.0.0.1:" + HTTP_PORT + ";max_batch_rows=64;")) {
                client.connect();
                createWindowTable(serverMain, 1_000);
                final String[] queries = {
                        "SELECT x FROM qd_window",
                        "SELECT x FROM qd_window WHERE x % 3 = 0",
                        "SELECT x FROM qd_window ORDER BY ts DESC",
                        "SELECT x % 37 k FROM qd_window GROUP BY k ORDER BY k",
                };
                final long[][] windows = {
                        {0, 10}, {10, 10}, {0, 0}, {995, 10}, {329, 1}, {5_000, 1},
                        {0, Long.MAX_VALUE}, {17, Long.MAX_VALUE}, {Long.MAX_VALUE, 1},
                };
                for (String sql : queries) {
                    final Collector all = new Collector();
                    client.execute(sql, all);
                    final long count = all.longs.size();
                    for (long[] w : windows) {
                        final long skip = w[0];
                        final long max = w[1];
                        final String msg = sql + " [skip=" + skip + ", max=" + max + ']';
                        final LongList limited = new LongList();
                        if (max > 0 && skip < count) {
                            final Collector oracle = new Collector();
                            client.execute("SELECT * FROM (" + sql + ") LIMIT " + skip + ", "
                                    + (skip + Math.min(max, count - skip)), oracle);
                            limited.add(oracle.longs);
                        }
                        final Collector windowed = new Collector();
                        client.execute(sql, windowed, new QwpQueryOptions().skipRows(skip).maxRows(max).countRows(true));
                        TestUtils.assertEquals(limited, windowed.longs);
                        Assert.assertEquals(msg, limited.size(), windowed.totalRows);
                        Assert.assertTrue(msg, windowed.hasRowCount);
                        Assert.assertEquals(msg, count, windowed.rowCount);
                        Assert.assertFalse(msg, windowed.hasStats);
                    }
                }

                // bind variables precede the row window on the wire
                final Collector bound = new Collector();
                client.execute("SELECT x FROM qd_window WHERE x >= $1 AND x < $2",
                        binds -> binds.setLong(0, 100).setLong(1, 200),
                        bound, new QwpQueryOptions().skipRows(95).maxRows(10).countRows(true));
                assertSequence(bound.longs, 195, 1, 5);
                Assert.assertEquals(100, bound.rowCount);
            }
        });
    }

    @Test
    public void testStatsWithFrozenClock() throws Exception {
        // A frozen HTTP clock makes every duration 0, so the reported values are
        // fully known; the JIT flag matches the explain of /exec.
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startFragmented(PropertyKey.HTTP_FROZEN_CLOCK.getEnvVarName(), "true");
                 QwpQueryClient client = newClient()) {
                client.connect();
                createWindowTable(serverMain, 1_000);
                final QwpQueryOptions stats = new QwpQueryOptions().stats(true);
                boolean isAnyJit = false;
                for (String sql : new String[]{"SELECT x FROM qd_window WHERE x > 500", "SELECT x FROM qd_window"}) {
                    final Collector select = new Collector();
                    client.execute(sql, select, stats);
                    assertFrozenStats(sql, select, execJitCompiled(sql));
                    Assert.assertFalse(sql, select.hasRowCount);
                    isAnyJit |= select.isJitCompiled;
                }
                Assert.assertEquals(JitUtil.isJitSupported(), isAnyJit);

                final Collector insert = new Collector();
                client.execute("INSERT INTO qd_window VALUES (1_000, 1_000::TIMESTAMP)", insert,
                        new QwpQueryOptions().stats(true).countRows(true).maxRows(1));
                Assert.assertEquals(CompiledQuery.INSERT, insert.opType);
                Assert.assertEquals(1, insert.rowsAffected);
                Assert.assertFalse("statements without rows report no count", insert.hasRowCount);
                assertFrozenStats("INSERT", insert, false);

                final Collector ddl = new Collector();
                client.execute("CREATE TABLE qd_other (x LONG)", ddl, stats);
                Assert.assertEquals(CompiledQuery.CREATE_TABLE, ddl.opType);
                assertFrozenStats("CREATE TABLE", ddl, false);

                // a failed query reports no statistics
                final Collector failed = new Collector();
                client.execute("SELECT x FROM qd_missing", failed, stats);
                Assert.assertEquals(1, failed.errorCount);
                Assert.assertEquals("[14] table does not exist [table=qd_missing]", failed.errorMessage);
                Assert.assertFalse(failed.hasStats);
            }
        });
    }

    private static void assertFrozenStats(String msg, Collector c, boolean isJitCompiled) {
        Assert.assertTrue(msg, c.hasStats);
        Assert.assertEquals(msg, 0, c.compileNanos);
        Assert.assertEquals(msg, 0, c.executeNanos);
        Assert.assertEquals(msg, 0, c.countNanos);
        Assert.assertEquals(msg, 0, c.clientWaitNanos);
        Assert.assertEquals(msg, isJitCompiled, c.isJitCompiled);
    }

    private static void assertSequence(LongList actual, long first, long step, long count) {
        Assert.assertEquals(count, actual.size());
        for (int i = 0; i < count; i++) {
            Assert.assertEquals(first + i * step, actual.getQuick(i));
        }
    }

    private static QwpQueryClient newClient() {
        return QwpQueryClient.fromConfig("ws::addr=127.0.0.1:" + HTTP_PORT + ";");
    }

    /**
     * Collects the LONG values of a single-column result, or NULL markers for
     * other column types, and what the detailed terminal callbacks receive.
     */
    private static final class Collector implements QwpColumnBatchHandler {
        final LongList longs = new LongList();
        long clientWaitNanos = -1;
        long compileNanos = -1;
        long countNanos = -1;
        int errorCount;
        String errorMessage;
        int errorPosition;
        byte errorStatus;
        long executeNanos = -1;
        boolean hasRowCount;
        boolean hasStats;
        boolean isJitCompiled;
        short opType = -1;
        long rowCount = -1;
        long rowsAffected = -1;
        long totalRows = -1;

        @Override
        public void onBatch(QwpColumnBatch batch) {
            for (int row = 0; row < batch.getRowCount(); row++) {
                longs.add(batch.isNull(0, row) ? Numbers.LONG_NULL : batch.getLongValue(0, row));
            }
        }

        @Override
        public void onEnd(long totalRows) {
            Assert.fail("the client must call onEnd(requestId, totalRows, details)");
        }

        @Override
        public void onEnd(long requestId, long totalRows, QwpQueryDetails details) {
            this.totalRows = totalRows;
            copy(details);
        }

        @Override
        public void onError(byte status, String message) {
            Assert.fail("the client must call onError(requestId, status, message, position)");
        }

        @Override
        public void onError(long requestId, byte status, String message, int position) {
            errorCount++;
            errorStatus = status;
            errorMessage = message;
            errorPosition = position;
        }

        @Override
        public void onExecDone(long requestId, short opType, long rowsAffected, QwpQueryDetails details) {
            this.opType = opType;
            this.rowsAffected = rowsAffected;
            copy(details);
        }

        private void copy(QwpQueryDetails details) {
            hasRowCount = details.hasRowCount();
            rowCount = details.getRowCount();
            hasStats = details.hasStats();
            compileNanos = details.getCompileNanos();
            executeNanos = details.getExecuteNanos();
            countNanos = details.getCountNanos();
            clientWaitNanos = details.getClientWaitNanos();
            isJitCompiled = details.isJitCompiled();
        }
    }
}
