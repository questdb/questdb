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

import java.util.ArrayList;
import java.util.List;

/**
 * Regression coverage for the rollback invariant on the QWP egress resume
 * path. When {@code QwpEgressUpgradeProcessor.resumeSend} (or its sibling
 * {@code handleCredit}) catches a non-transport {@code Throwable} from
 * {@code streamResults}, it must call
 * {@code batchBuffer.rollbackCurrentBatch()} before {@code state.endStreaming()}.
 * <p>
 * The in-flight batch has already grown the connection-scoped SYMBOL dict
 * via {@code addEntry} (during row assembly). Without rollback, those entries
 * linger after the batch's frame fails to ship. The next query on the same
 * connection then re-encounters the same symbol bytes, {@code addEntry} hits
 * the dedup map and returns the orphan id (which is below the new batch's
 * {@code batchDeltaStart}), {@code emitDeltaSection} omits those entries on
 * the wire, and the row payload references an id the client was never taught.
 * The client's decoder then fails bounds-check on the SYMBOL dict and the
 * second query surfaces a decode error despite the server's healthy state.
 * <p>
 * Arms a one-shot debug hook inside {@code streamResults} that throws a
 * synthetic {@code CairoException} after at least one batch has been
 * committed (so the throw lands in the resumeSend Throwable catch rather
 * than the first-pass catch in handleQueryRequest). Runs a first query whose
 * batch 1 introduces fresh symbols, then a second query on the same
 * connection that re-uses those symbols. The second query must complete
 * without error; the assertion fails until the fix adds rollback to the
 * resumeSend / handleCredit Throwable catches.
 */
public class QwpEgressResumeRollbackTest extends AbstractBootstrapTest {

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testResumeThrowableMustRollbackInFlightDictEntries() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain server = startWithEnvVariables(
                    PropertyKey.DEBUG_HTTP_FORCE_SEND_FRAGMENTATION_CHUNK_SIZE.getEnvVarName(), "23"
            )) {
                // Rows are grouped so batch 0 (first two rows) contains only
                // 'alpha' and batch 1 (next two rows) introduces 'beta' as a
                // fresh symbol. The fresh symbol is the one that orphans in
                // the dict when batch 1 aborts before reaching the wire.
                server.execute("CREATE TABLE t (sym SYMBOL, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                server.execute("INSERT INTO t VALUES "
                        + "('alpha', 1::TIMESTAMP), ('alpha', 2::TIMESTAMP), "
                        + "('beta', 3::TIMESTAMP), ('beta', 4::TIMESTAMP), "
                        + "('gamma', 5::TIMESTAMP), ('gamma', 6::TIMESTAMP)");
                server.awaitTable("t");

                // failover=off is load-bearing: with the default failover=on,
                // a Query 2 decode failure caused by the orphan dict would
                // silently reconnect the client to a fresh socket (new server-
                // side connDict, no orphans) and the retry would succeed,
                // masking the bug. We want to observe the decode failure
                // directly.
                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";max_batch_rows=2;failover=off;")) {
                    client.connect();

                    QwpEgressUpgradeProcessor.DEBUG_FORCE_INTERNAL_ERROR_ON_RESUME = 1;
                    try {
                        final StringBuilder q1Error = new StringBuilder();
                        client.execute("SELECT sym FROM t ORDER BY ts",
                                new QwpColumnBatchHandler() {
                                    @Override
                                    public void onBatch(QwpColumnBatch batch) {
                                    }

                                    @Override
                                    public void onEnd(long totalRows) {
                                    }

                                    @Override
                                    public void onError(byte status, String message) {
                                        q1Error.append(message);
                                    }
                                });
                        Assert.assertFalse("Query 1 must surface the synthetic internal error "
                                + "thrown from the resume path [msg=" + q1Error + "]", q1Error.isEmpty());
                    } finally {
                        // Clear the hook even on test failure so subsequent tests
                        // in the same JVM do not inherit an armed value.
                        QwpEgressUpgradeProcessor.DEBUG_FORCE_INTERNAL_ERROR_ON_RESUME = 0;
                    }

                    // Query 2 on the same connection. With the rollback missing,
                    // the server's connSymbolDict still holds the entries that
                    // batch 1 added for 'beta', but the client never received
                    // them. Query 2 will ship row payload referencing an id the
                    // client never learned, and the client's decoder will fail
                    // the SYMBOL dict bounds check.
                    final List<String> q2Symbols = new ArrayList<>();
                    final StringBuilder q2Error = new StringBuilder();
                    client.execute("SELECT sym FROM t ORDER BY ts",
                            new QwpColumnBatchHandler() {
                                @Override
                                public void onBatch(QwpColumnBatch batch) {
                                    for (int r = 0; r < batch.getRowCount(); r++) {
                                        q2Symbols.add(batch.getSymbol(0, r));
                                    }
                                }

                                @Override
                                public void onEnd(long totalRows) {
                                }

                                @Override
                                public void onError(byte status, String message) {
                                    q2Error.append(message);
                                }
                            });

                    Assert.assertEquals(
                            "Query 2 on a healthy connection must not error. "
                                    + "Orphan dict entries from Query 1's aborted "
                                    + "batch 1 broke the wire state [err=" + q2Error + "]",
                            "", q2Error.toString()
                    );
                    Assert.assertEquals(6, q2Symbols.size());
                    Assert.assertEquals("alpha", q2Symbols.get(0));
                    Assert.assertEquals("alpha", q2Symbols.get(1));
                    Assert.assertEquals("beta", q2Symbols.get(2));
                    Assert.assertEquals("beta", q2Symbols.get(3));
                    Assert.assertEquals("gamma", q2Symbols.get(4));
                    Assert.assertEquals("gamma", q2Symbols.get(5));
                }
            }
        });
    }
}
