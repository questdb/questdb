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

package io.questdb.test.cutlass.websocket;

import io.questdb.PropertyKey;
import io.questdb.client.Sender;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorWebSocketSendLoop;
import io.questdb.std.Os;
import io.questdb.test.TestServerMain;
import io.questdb.test.cutlass.qwp.AbstractQwpBootstrapTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;

/**
 * End-to-end coverage of the {@code STATUS_LOCAL_DURABLE_ACK} stream, driving
 * the real pinned {@code java-questdb-client} against a live server -- the
 * repo rule for wire-contract changes. The client requests the {@code local}
 * durable-ack tier, the server grants it, fsyncs the transaction log within
 * the adaptive commit window, and reports the local frontier; the client
 * releases its store-and-forward copy only on that ack.
 */
public class QwpLocalDurableAckTest extends AbstractQwpBootstrapTest {

    @Override
    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testLocalAckArrivesOnIdleTailAndTrims() throws Exception {
        // The idle-tail case: one burst of rows, then silence. The background
        // sweep fsyncs the transaction log within the commit window, and the
        // client's own keepalive PING gives the passive server its write
        // opportunity -- the local ack must arrive with no help from the test.
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startFragmented(
                    PropertyKey.CAIRO_COMMIT_MODE.getEnvVarName(), "adaptive"
            )) {
                int httpPort = serverMain.getHttpServerPort();
                serverMain.execute("CREATE TABLE local_ack_test (" +
                        "value LONG, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = (QwpWebSocketSender) Sender.fromConfig(
                        "ws::addr=localhost:" + httpPort + ";request_durable_ack=local;")) {
                    for (int i = 0; i < 10; i++) {
                        sender.table("local_ack_test")
                                .longColumn("value", i)
                                .at(1_000_000_000_000L + i, ChronoUnit.MICROS);
                    }
                    sender.flush();

                    CursorWebSocketSendLoop loop = sender.cursorSendLoopForTest();
                    Assert.assertNotNull("send loop must exist after flush", loop);
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
                    while ((loop.getTotalLocalDurableAcks() == 0 || loop.getTotalDurableTrimAdvances() == 0)
                            && System.nanoTime() < deadline) {
                        Os.sleep(10);
                    }
                    Assert.assertTrue(
                            "client must receive STATUS_LOCAL_DURABLE_ACK frames",
                            loop.getTotalLocalDurableAcks() > 0);
                    Assert.assertTrue(
                            "local acks must advance the store-and-forward trim",
                            loop.getTotalDurableTrimAdvances() > 0);
                    Assert.assertEquals(
                            "a local-only grant must never produce STATUS_DURABLE_ACK frames",
                            0, loop.getTotalDurableAcks());
                }

                serverMain.awaitTable("local_ack_test");
                serverMain.assertSql(
                        "SELECT count() FROM local_ack_test",
                        "count\n10\n"
                );
            }
        });
    }

    @Test
    public void testNoOptInMeansNoAckFrames() throws Exception {
        // Spec contract: the server must not emit durable-ack frames of either
        // kind to a connection that never requested them.
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startFragmented(
                    PropertyKey.CAIRO_COMMIT_MODE.getEnvVarName(), "adaptive"
            )) {
                int httpPort = serverMain.getHttpServerPort();
                serverMain.execute("CREATE TABLE no_opt_in_test (" +
                        "value LONG, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = (QwpWebSocketSender) Sender.fromConfig(
                        "ws::addr=localhost:" + httpPort + ";")) {
                    for (int i = 0; i < 5; i++) {
                        sender.table("no_opt_in_test")
                                .longColumn("value", i)
                                .at(1_000_000_000_000L + i, ChronoUnit.MICROS);
                    }
                    sender.flush();
                    serverMain.awaitTable("no_opt_in_test");
                    // Grace period past the commit window and sweep so a
                    // misbehaving server would have had its chance to emit.
                    Os.sleep(500);
                    CursorWebSocketSendLoop loop = sender.cursorSendLoopForTest();
                    Assert.assertEquals(0, loop.getTotalLocalDurableAcks());
                    Assert.assertEquals(0, loop.getTotalDurableAcks());
                }

                serverMain.assertSql(
                        "SELECT count() FROM no_opt_in_test",
                        "count\n5\n"
                );
            }
        });
    }

    @Test
    public void testRequestsIncludingReplicatedAreDeniedOnOss() throws Exception {
        // OSS has no replication, so every request set that includes the
        // replicated tier -- including the shipped legacy "on" -- is denied in
        // full: no confirmation header, client fails loudly. All-or-nothing;
        // the local half of "local,replicated" is never granted on its own.
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startFragmented(
                    PropertyKey.CAIRO_COMMIT_MODE.getEnvVarName(), "adaptive"
            )) {
                int httpPort = serverMain.getHttpServerPort();
                serverMain.execute("CREATE TABLE deny_test (" +
                        "value LONG, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                for (String tiers : new String[]{"on", "replicated", "local,replicated"}) {
                    Throwable failure = null;
                    try (Sender sender = Sender.fromConfig(
                            "ws::addr=localhost:" + httpPort + ";request_durable_ack=" + tiers + ";")) {
                        sender.table("deny_test")
                                .longColumn("value", 1)
                                .at(1_000_000_000_000L, ChronoUnit.MICROS);
                        sender.flush();
                    } catch (Throwable e) {
                        failure = e;
                    }
                    Assert.assertNotNull(
                            "request_durable_ack=" + tiers + " must be denied by an OSS server",
                            failure);
                    TestUtils.assertContains(failure.getMessage(), "durable ack");
                }
            }
        });
    }
}
