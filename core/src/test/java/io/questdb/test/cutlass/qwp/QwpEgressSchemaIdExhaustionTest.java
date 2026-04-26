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
import org.junit.Ignore;
import org.junit.Test;

/**
 * Regression tests for M1 (PR 6991 review): the server's schema id counter
 * used to increment per query without content dedup, so any connection that
 * ran more than 65_535 queries ran into the client decoder's schema id cap
 * and tore down the stream with "schema_id out of range".
 * <p>
 * The fix makes schema ids content-addressed: queries with identical
 * {@code (name, questdbColumnType)} tuples reuse the same id on the wire and
 * only consume one schemaRegistry slot on the client. Genuinely
 * unique-per-query schemas still trip the cap -- the server translates that
 * into a controlled {@code STATUS_LIMIT_EXCEEDED} error that keeps the
 * connection usable for already-registered schemas.
 * <p>
 * Tests are {@link Ignore @Ignore}d because each takes on the order of a
 * minute of wall clock; run them manually after changes to the dedup path.
 * Unit-level coverage of the dedup map, the exhaustion sentinel, and the
 * reuse flag lives in {@code QwpEgressProcessorStateTest}.
 */
@Ignore("long-running (~1-2 min); run manually")
public class QwpEgressSchemaIdExhaustionTest extends AbstractBootstrapTest {

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testIdenticalSchemasReuseIdPast65_535Queries() throws Exception {
        // Pre-fix: every query burned a fresh schema id; the 65_536th hit the
        // client cap and tore the connection down with "schema_id out of range".
        // Post-fix: the server fingerprints the schema shape and reuses ids,
        // so running the *same* SELECT 65_536 times consumes exactly one slot
        // and all queries succeed.
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE t(id LONG, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("INSERT INTO t VALUES (1, 0::TIMESTAMP)");
                serverMain.awaitTable("t");

                try (QwpQueryClient client = QwpQueryClient.newPlainText("127.0.0.1", HTTP_PORT)) {
                    client.connect();

                    CountingHandler handler = new CountingHandler();
                    for (int i = 0; i < 70_000; i++) {
                        handler.reset();
                        client.execute("SELECT id FROM t", handler);
                        if (handler.errorSeen) {
                            Assert.fail("query #" + i + " failed unexpectedly (regression in "
                                    + "schema-id dedup): " + handler.errorMessage);
                        }
                    }
                }
            }
        });
    }

    private static final class CountingHandler implements QwpColumnBatchHandler {
        String errorMessage;
        boolean errorSeen;

        @Override
        public void onBatch(QwpColumnBatch batch) {
        }

        @Override
        public void onEnd(long totalRows) {
        }

        @Override
        public void onError(byte status, String message) {
            errorSeen = true;
            errorMessage = message;
        }

        void reset() {
            errorSeen = false;
            errorMessage = null;
        }
    }
}
