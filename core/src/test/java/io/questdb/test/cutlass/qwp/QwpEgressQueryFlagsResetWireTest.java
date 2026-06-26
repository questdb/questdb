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
import io.questdb.cutlass.qwp.server.egress.QwpEgressMetrics;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * End-to-end coverage of the per-query {@code QUERY_FLAG_RESET_DICT} flag over
 * the wire. The client requests a reset via {@code execute(..., true)}, which
 * appends the {@code query_flags} trailer when the server advertised
 * {@code CAP_QUERY_FLAGS}; the server clears its connection-scoped SYMBOL dict
 * at the query boundary and ships {@code CACHE_RESET} before the first
 * {@code RESULT_BATCH}.
 * <p>
 * Caps are left at their defaults so the only reset trigger under test is the
 * flag, not a soft cap -- this is the complement to
 * {@link QwpEgressCacheResetWireTest}, which drives the cap path. Each test
 * asserts both the server-side reset metric and that the client still resolves
 * every symbol after the flush: correct resolution proves the {@code CACHE_RESET}
 * landed before the batch and the following delta restarted at {@code deltaStart=0},
 * since a misordered frame or non-zero delta would desync the client decoder and
 * surface as a decode error or a missing symbol.
 */
public class QwpEgressQueryFlagsResetWireTest extends AbstractQwpBootstrapTest {

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    /**
     * A SELECT carrying the flag on a connection whose dict an earlier query
     * already grew fires exactly one {@code CACHE_RESET}, and the flagged query
     * still resolves every symbol from its own freshly populated dict.
     */
    @Test
    public void testResetFlagFiresCacheResetBeforeBatch() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startFragmented("QDB_METRICS_ENABLED", "true")) {
                serverMain.execute("CREATE TABLE flag_fires(sym SYMBOL, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("""
                        INSERT INTO flag_fires VALUES
                            ('a', 1::TIMESTAMP), ('b', 2::TIMESTAMP),
                            ('c', 3::TIMESTAMP), ('d', 4::TIMESTAMP)
                        """);
                serverMain.awaitTable("flag_fires");
                QwpEgressMetrics metrics = serverMain.getEngine().getMetrics().qwpEgressMetrics();
                long resetsBefore = metrics.cacheResetDictCount();

                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();

                    // Query 1 (no flag): grows the connection-scoped dict to {a,b,c,d}.
                    java.util.Set<String> q1 = new java.util.HashSet<>();
                    client.execute("SELECT sym FROM flag_fires", collectInto(q1));
                    Assert.assertEquals(java.util.Set.of("a", "b", "c", "d"), q1);
                    Assert.assertEquals("no flag yet => no reset", resetsBefore, metrics.cacheResetDictCount());

                    // Query 2 (flag): the non-empty dict is reset before the query
                    // streams, then repopulated; the client must resolve everything.
                    java.util.Set<String> q2 = new java.util.HashSet<>();
                    client.execute("SELECT sym FROM flag_fires", collectInto(q2), true);
                    Assert.assertEquals(java.util.Set.of("a", "b", "c", "d"), q2);
                    Assert.assertEquals("flag on a non-empty dict must fire exactly one reset",
                            resetsBefore + 1, metrics.cacheResetDictCount());
                }
            }
        });
    }

    /**
     * Empty-dict guard, over the wire: the first flagged query on a fresh
     * connection asks for a reset, but the dict is empty so the server emits no
     * {@code CACHE_RESET} frame. The query still streams its symbols normally.
     */
    @Test
    public void testResetFlagOnEmptyDictEmitsNoFrame() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startFragmented("QDB_METRICS_ENABLED", "true")) {
                serverMain.execute("CREATE TABLE flag_empty(sym SYMBOL, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("INSERT INTO flag_empty VALUES ('a', 1::TIMESTAMP), ('b', 2::TIMESTAMP)");
                serverMain.awaitTable("flag_empty");
                QwpEgressMetrics metrics = serverMain.getEngine().getMetrics().qwpEgressMetrics();
                long resetsBefore = metrics.cacheResetDictCount();

                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    // First query on the connection: dict is empty, so the flag is
                    // a no-op and no CACHE_RESET goes out.
                    java.util.Set<String> got = new java.util.HashSet<>();
                    client.execute("SELECT sym FROM flag_empty", collectInto(got), true);
                    Assert.assertEquals(java.util.Set.of("a", "b"), got);
                    Assert.assertEquals("empty dict => no reset frame",
                            resetsBefore, metrics.cacheResetDictCount());
                }
            }
        });
    }

    /**
     * Deferred emit: a non-SELECT carrying the flag clears the server dict
     * immediately but returns via EXEC_DONE without streaming, so the staged
     * {@code CACHE_RESET} surfaces on the next result-producing query. The
     * follow-up SELECT must still resolve every symbol, proving the deferred
     * frame restored client/server lockstep. This is the trickiest path in the
     * change.
     */
    @Test
    public void testResetFlagOnNonSelectDefersToNextSelect() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startFragmented("QDB_METRICS_ENABLED", "true")) {
                serverMain.execute("CREATE TABLE flag_defer(sym SYMBOL, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("""
                        INSERT INTO flag_defer VALUES
                            ('a', 1::TIMESTAMP), ('b', 2::TIMESTAMP),
                            ('c', 3::TIMESTAMP), ('d', 4::TIMESTAMP)
                        """);
                serverMain.awaitTable("flag_defer");
                QwpEgressMetrics metrics = serverMain.getEngine().getMetrics().qwpEgressMetrics();
                long resetsBefore = metrics.cacheResetDictCount();

                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();

                    // Query 1 (no flag): grows the dict to {a,b,c,d}.
                    java.util.Set<String> q1 = new java.util.HashSet<>();
                    client.execute("SELECT sym FROM flag_defer", collectInto(q1));
                    Assert.assertEquals(java.util.Set.of("a", "b", "c", "d"), q1);

                    // Query 2 (non-SELECT, flag): clears the dict and stages the
                    // reset, but EXEC_DONE returns before any CACHE_RESET is sent.
                    // Inserts a duplicate symbol so query 3's symbol set is unchanged.
                    boolean[] execDone = {false};
                    client.execute("INSERT INTO flag_defer VALUES ('a', 5::TIMESTAMP)", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            Assert.fail("non-SELECT must not stream batches");
                        }

                        @Override
                        public void onEnd(long totalRows) {
                            Assert.fail("non-SELECT must complete via onExecDone");
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("insert must succeed: " + message);
                        }

                        @Override
                        public void onExecDone(short opType, long rowsAffected) {
                            execDone[0] = true;
                        }
                    }, true);
                    Assert.assertTrue("non-SELECT must complete", execDone[0]);
                    Assert.assertEquals("the non-SELECT flag must clear the dict and count one reset",
                            resetsBefore + 1, metrics.cacheResetDictCount());

                    // Query 3 (no flag): the deferred CACHE_RESET must land before
                    // this query's first batch, so the client resolves everything.
                    java.util.Set<String> q3 = new java.util.HashSet<>();
                    client.execute("SELECT sym FROM flag_defer", collectInto(q3));
                    Assert.assertEquals(java.util.Set.of("a", "b", "c", "d"), q3);
                    Assert.assertEquals("query 3 must not fire its own reset",
                            resetsBefore + 1, metrics.cacheResetDictCount());
                }
            }
        });
    }

    /**
     * Per-query scoping over a run of flagged SELECTs: the first (empty-dict)
     * query is a no-op, then every subsequent flagged query resets the dict the
     * previous one left behind. Each query resolves its own symbols.
     */
    @Test
    public void testResetFlagScopesEachQuery() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startFragmented("QDB_METRICS_ENABLED", "true")) {
                serverMain.execute("CREATE TABLE flag_scope(sym SYMBOL, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("INSERT INTO flag_scope VALUES ('a', 1::TIMESTAMP), ('b', 2::TIMESTAMP)");
                serverMain.awaitTable("flag_scope");
                QwpEgressMetrics metrics = serverMain.getEngine().getMetrics().qwpEgressMetrics();
                long resetsBefore = metrics.cacheResetDictCount();

                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    final int runs = 4;
                    for (int i = 0; i < runs; i++) {
                        java.util.Set<String> got = new java.util.HashSet<>();
                        client.execute("SELECT sym FROM flag_scope", collectInto(got), true);
                        Assert.assertEquals(java.util.Set.of("a", "b"), got);
                    }
                    // First run starts empty (no reset); each later run resets the
                    // dict the prior run repopulated.
                    Assert.assertEquals("one reset per flagged query after the first",
                            resetsBefore + (runs - 1), metrics.cacheResetDictCount());
                }
            }
        });
    }

    private static QwpColumnBatchHandler collectInto(java.util.Set<String> sink) {
        return new QwpColumnBatchHandler() {
            @Override
            public void onBatch(QwpColumnBatch batch) {
                for (int r = 0; r < batch.getRowCount(); r++) {
                    DirectUtf8Sequence v = batch.getStrA(0, r);
                    if (v != null) {
                        sink.add(v.toString());
                    }
                }
            }

            @Override
            public void onEnd(long total) {
            }

            @Override
            public void onError(byte status, String message) {
                Assert.fail("query must succeed: " + message);
            }
        };
    }
}
