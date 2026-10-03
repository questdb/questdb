/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test.cutlass.qwp.e2e;

import io.questdb.client.LineSenderSchemaException;
import io.questdb.client.Sender;
import io.questdb.client.SenderConnectionEvent;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Drives the real {@code java-questdb-client} against a real QWP ingress that
 * gets stopped and restarted, and pins the {@code schema_mode} contract for a
 * table the sender has never seen while the wire is down:
 * <ul>
 *   <li>{@code auto} (the default) writes the row with the legacy contract
 *       without waiting, queues it in store-and-forward, and the row lands
 *       when the server returns; the next batch for that table adopts the
 *       schema contract again.</li>
 *   <li>{@code strict} waits out {@code schema_wait_millis} and fails typed,
 *       then works again once the server is back.</li>
 * </ul>
 * This is the scenario that produced the {@code SCHEMA_UNAVAILABLE} failures
 * across the store-and-forward CI suites when {@code table()} always waited.
 */
public class QwpSchemaOutageE2ETest extends AbstractCairoTest {
    private static final String KNOWN_TABLE = "schema_outage_known";
    private static final String UNSEEN_TABLE = "schema_outage_unseen";

    @Test
    public void testAutoWritesUnseenTableWhileServerIsDown() throws Exception {
        assertMemoryLeak(() -> {
            createKnownTable();
            int port = RestartableQwpServer.pickFreePort();
            String sfDir = temp.newFolder("qwp-schema-outage-auto").getAbsolutePath();
            CountDownLatch disconnected = new CountDownLatch(1);

            try (RestartableQwpServer server = new RestartableQwpServer(engine, configuration, port)) {
                server.start();
                try (Sender sender = sender(port, sfDir, "", disconnected)) {
                    sender.table(KNOWN_TABLE).longColumn("id", 1).atNow();
                    sender.flush();
                    Assert.assertTrue("known-table row was not acked before the outage", sender.drain(30_000));

                    server.stop();
                    Assert.assertTrue("sender did not observe the outage", disconnected.await(10, TimeUnit.SECONDS));

                    // The wire is down and the table is not cached. The row must not
                    // wait out the 30 s default schema budget; the bound below is
                    // generous only to keep the assertion stable on slow CI hosts.
                    long startNanos = System.nanoTime();
                    sender.table(UNSEEN_TABLE).longColumn("id", 1).stringColumn("marker", "offline").atNow();
                    sender.flush();
                    long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
                    Assert.assertTrue("offline row blocked for " + elapsedMillis + " ms", elapsedMillis < 10_000);

                    server.start();
                    // The first row after the flush looks the table up on the fresh
                    // connection and adopts the schema contract.
                    sender.table(UNSEEN_TABLE).longColumn("id", 2).stringColumn("marker", "online").atNow();
                    sender.flush();
                    Assert.assertTrue("rows were not acked after the restart", sender.drain(30_000));
                }

                drainWalQueue();
                engine.awaitTable(UNSEEN_TABLE, 30, TimeUnit.SECONDS);
                TestUtils.assertEventually(() -> {
                    drainWalQueue();
                    assertQuery("SELECT id, marker FROM " + UNSEEN_TABLE)
                            .noLeakCheck()
                            .expectSize()
                            .returns(
                                    "id\tmarker\n"
                                            + "1\toffline\n"
                                            + "2\tonline\n"
                            );
                });
            }
        });
    }

    @Test
    public void testStrictFailsUnseenTableWhileServerIsDownAndRecovers() throws Exception {
        assertMemoryLeak(() -> {
            createKnownTable();
            int port = RestartableQwpServer.pickFreePort();
            String sfDir = temp.newFolder("qwp-schema-outage-strict").getAbsolutePath();
            CountDownLatch disconnected = new CountDownLatch(1);

            try (RestartableQwpServer server = new RestartableQwpServer(engine, configuration, port)) {
                server.start();
                try (Sender sender = sender(port, sfDir, "schema_mode=strict;schema_wait_millis=500;", disconnected)) {
                    sender.table(KNOWN_TABLE).longColumn("id", 1).atNow();
                    sender.flush();
                    Assert.assertTrue("known-table row was not acked before the outage", sender.drain(30_000));

                    server.stop();
                    Assert.assertTrue("sender did not observe the outage", disconnected.await(10, TimeUnit.SECONDS));

                    LineSenderSchemaException e = Assert.assertThrows(
                            LineSenderSchemaException.class,
                            () -> sender.table(UNSEEN_TABLE)
                    );
                    Assert.assertEquals(LineSenderSchemaException.Reason.SCHEMA_UNAVAILABLE, e.getReason());
                    // The known table's snapshot is cached, so it stays writable offline.
                    sender.table(KNOWN_TABLE).longColumn("id", 2).atNow();

                    server.start();
                    sender.table(UNSEEN_TABLE).longColumn("id", 1).stringColumn("marker", "online").atNow();
                    sender.flush();
                    Assert.assertTrue("rows were not acked after the restart", sender.drain(30_000));
                }

                drainWalQueue();
                engine.awaitTable(UNSEEN_TABLE, 30, TimeUnit.SECONDS);
                TestUtils.assertEventually(() -> {
                    drainWalQueue();
                    assertQuery("SELECT id, marker FROM " + UNSEEN_TABLE)
                            .noLeakCheck()
                            .expectSize()
                            .returns("id\tmarker\n1\tonline\n");
                    assertQuery("SELECT id FROM " + KNOWN_TABLE)
                            .noLeakCheck()
                            .expectSize()
                            .returns("id\n1\n2\n");
                });
            }
        });
    }

    private static Sender sender(int port, String sfDir, String extraConfig, CountDownLatch disconnected) {
        return Sender.builder("ws::addr=localhost:" + port + ";sf_dir=" + sfDir
                        + ";reconnect_initial_backoff_millis=10;reconnect_max_backoff_millis=100;"
                        + "auto_flush_rows=2147483647;auto_flush_bytes=0;auto_flush_interval=2147483646;"
                        + extraConfig)
                .connectionListener(event -> {
                    if (event.getKind() == SenderConnectionEvent.Kind.DISCONNECTED) {
                        disconnected.countDown();
                    }
                })
                .build();
    }

    private void createKnownTable() throws Exception {
        execute("CREATE TABLE " + KNOWN_TABLE + " (id LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
    }
}
