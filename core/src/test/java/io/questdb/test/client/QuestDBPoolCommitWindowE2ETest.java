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

package io.questdb.test.client;

import io.questdb.client.QuestDB;
import io.questdb.client.Sender;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.Test;

/**
 * End-to-end tests of the pooled sender's commit window against an embedded
 * server. Every lease borrows a sender, writes one row and closes it, so each
 * release is one window member. The window changes how many WAL transactions
 * the rows land in, never which rows land: the table contents must be the same
 * with and without it.
 */
public class QuestDBPoolCommitWindowE2ETest extends AbstractBootstrapTest {
    private static final String EXPECTED_CONTENTS = """
            count\tsum\tmin\tmax
            3000\t4498500\t0\t2999
            """;
    private static final int LEASES = 3000;
    // One slot, so every lease joins the same window.
    private static final String POOL = "sender_pool_min=1;sender_pool_max=1;";

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testDefaultWindowKeepsContents() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain server = startWithEnvVariables()) {
                createTable(server, "d");
                ingestOneRowPerLease("d", "");
                awaitApplied(server, "d");
                server.assertSql("SELECT count(), sum(v), min(v), max(v) FROM d", EXPECTED_CONTENTS);
                // the derived default window (1000 rows / 100 ms) commits fewer times than once per release
                server.assertSql(
                        "SELECT sequencerTxn < " + LEASES + " AS windowed FROM wal_tables() WHERE name = 'd'",
                        """
                                windowed
                                true
                                """
                );
            }
        });
    }

    @Test
    public void testWindowCommitsOncePerWindowWithIdenticalContents() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain server = startWithEnvVariables()) {
                createTable(server, "nw");
                createTable(server, "w");
                ingestOneRowPerLease("nw", "commit_rows=0;commit_interval_ms=0;");
                ingestOneRowPerLease("w", "commit_rows=1000;commit_interval_ms=60000;");
                awaitApplied(server, "nw");
                awaitApplied(server, "w");
                server.assertSql("SELECT count(), sum(v), min(v), max(v) FROM nw", EXPECTED_CONTENTS);
                server.assertSql("SELECT count(), sum(v), min(v), max(v) FROM w", EXPECTED_CONTENTS);
                // no window: one WAL transaction per release; a 1000-row window: one per 1000 releases
                server.assertSql(
                        "SELECT name, sequencerTxn FROM wal_tables() WHERE name IN ('nw', 'w') ORDER BY name",
                        """
                                name\tsequencerTxn
                                nw\t3000
                                w\t3
                                """
                );
            }
        });
    }

    private static void awaitApplied(TestServerMain server, String table) throws Exception {
        // db.close() returned after every frame was acknowledged, so the sequencer
        // already holds every transaction: wait for the writer to apply them.
        TestUtils.assertEventually(() -> server.assertSql(
                "SELECT writerTxn = sequencerTxn AS applied FROM wal_tables() WHERE name = '" + table + "'",
                """
                        applied
                        true
                        """
        ));
    }

    private static void createTable(TestServerMain server, String table) {
        server.execute("CREATE TABLE " + table + "(v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        server.awaitTable(table);
    }

    private static void ingestOneRowPerLease(String table, String window) {
        try (QuestDB db = QuestDB.connect("ws::addr=127.0.0.1:" + HTTP_PORT + ";" + POOL + window)) {
            for (int i = 0; i < LEASES; i++) {
                try (Sender s = db.borrowSender()) {
                    s.table(table).longColumn("v", i).atNow();
                }
            }
        }
    }
}
