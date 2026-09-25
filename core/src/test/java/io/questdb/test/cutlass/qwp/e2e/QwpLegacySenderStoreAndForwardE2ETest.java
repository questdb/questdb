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

package io.questdb.test.cutlass.qwp.e2e;

import io.questdb.client.LineSenderServerException;
import io.questdb.client.Sender;
import io.questdb.client.SenderError;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.std.Decimal64;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.time.temporal.ChronoUnit;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Store-and-forward contracts of the legacy (schema-less) QWP path, driven by a
 * real sender with {@code schema_mode=off} so the server, not the client, does
 * the value conversion. Older clients use this path against a current server.
 */
public class QwpLegacySenderStoreAndForwardE2ETest extends AbstractCairoTest {
    private static final long BASE_TS_MICROS = 1_700_000_000_000_000L;

    @Test
    public void testRejectedFrameIsRetainedAndReplayedWhole() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE legacy_nack (id LONG, d DECIMAL(10,2), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            int port = RestartableQwpServer.pickFreePort();
            String config = config(port, temp.newFolder("legacy-nack").getAbsolutePath());
            Decimal64 fits = new Decimal64(125, 2);
            // 13 digits cannot fit DECIMAL(10,2), so the server rejects the frame
            Decimal64 overflows = new Decimal64(1_000_000_000_000L, 2);

            try (RestartableQwpServer server = new RestartableQwpServer(engine, configuration, port)) {
                server.start();

                CompletableFuture<SenderError> terminal = new CompletableFuture<>();
                try (Sender sender = Sender.builder(config).errorHandler(e -> completeOnTerminal(terminal, e)).build()) {
                    sender.table("legacy_nack").longColumn("id", 1).decimalColumn("d", fits).at(BASE_TS_MICROS + 1, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.assertTrue(sender.drain(30_000));

                    // one frame: the bad row sits between two well-formed ones
                    sender.table("legacy_nack").longColumn("id", 2).decimalColumn("d", fits).at(BASE_TS_MICROS + 2, ChronoUnit.MICROS);
                    sender.table("legacy_nack").longColumn("id", 3).decimalColumn("d", overflows).at(BASE_TS_MICROS + 3, ChronoUnit.MICROS);
                    sender.table("legacy_nack").longColumn("id", 4).decimalColumn("d", fits).at(BASE_TS_MICROS + 4, ChronoUnit.MICROS);
                    flushIgnoringTerminal(sender);

                    assertSchemaMismatch(terminal.get(30, TimeUnit.SECONDS));
                    try {
                        sender.flush();
                        Assert.fail("flush() after a terminal rejection must throw");
                    } catch (LineSenderServerException e) {
                        Assert.assertEquals(SenderError.Category.SCHEMA_MISMATCH, e.getServerError().getCategory());
                    }
                }
                assertOnlyBaselineRow();

                // The rejected frame must still be in the slot: a fresh sender
                // replays it and gets the same rejection, still all-or-nothing.
                CompletableFuture<SenderError> replayTerminal = new CompletableFuture<>();
                try (Sender ignore = Sender.builder(config).errorHandler(e -> completeOnTerminal(replayTerminal, e)).build()) {
                    assertSchemaMismatch(replayTerminal.get(30, TimeUnit.SECONDS));
                }
                assertOnlyBaselineRow();
            }
        });
    }

    @Test
    public void testRowsBufferedWhileServerIsDownArriveAfterRestart() throws Exception {
        assertMemoryLeak(() -> {
            // DEDUP absorbs at-least-once replay, so the oracle below is exact
            // no matter when the connection dropped.
            execute("CREATE TABLE legacy_restart (id LONG, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, id)");
            int port = RestartableQwpServer.pickFreePort();
            String config = config(port, temp.newFolder("legacy-restart").getAbsolutePath());

            try (RestartableQwpServer server = new RestartableQwpServer(engine, configuration, port)) {
                server.start();
                try (Sender sender = Sender.fromConfig(config)) {
                    // LONG values into a DOUBLE column: the server converts them
                    for (int id = 0; id < 3; id++) {
                        sender.table("legacy_restart").longColumn("id", id).longColumn("v", id * 10).at(BASE_TS_MICROS + id, ChronoUnit.MICROS);
                    }
                    sender.flush();
                    Assert.assertTrue(sender.drain(30_000));

                    server.stop();
                    for (int id = 3; id < 6; id++) {
                        sender.table("legacy_restart").longColumn("id", id).longColumn("v", id * 10).at(BASE_TS_MICROS + id, ChronoUnit.MICROS);
                    }
                    sender.flush();

                    server.start();
                    Assert.assertTrue(sender.drain(60_000));
                    Assert.assertEquals(0, ((QwpWebSocketSender) sender).getTotalServerErrors());
                }
            }
            drainWalQueue();
            assertQuery("SELECT id, v, ts FROM legacy_restart ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            id\tv\tts
                            0\t0.0\t2023-11-14T22:13:20.000000Z
                            1\t10.0\t2023-11-14T22:13:20.000001Z
                            2\t20.0\t2023-11-14T22:13:20.000002Z
                            3\t30.0\t2023-11-14T22:13:20.000003Z
                            4\t40.0\t2023-11-14T22:13:20.000004Z
                            5\t50.0\t2023-11-14T22:13:20.000005Z
                            """);
        });
    }

    private static void assertSchemaMismatch(SenderError error) {
        Assert.assertEquals(SenderError.Policy.TERMINAL, error.getAppliedPolicy());
        Assert.assertEquals(SenderError.Category.SCHEMA_MISMATCH, error.getCategory());
    }

    private static void completeOnTerminal(CompletableFuture<SenderError> future, SenderError error) {
        if (error.getAppliedPolicy() == SenderError.Policy.TERMINAL) {
            future.complete(error);
        }
    }

    private static String config(int port, String sfDir) {
        return "ws::addr=localhost:" + port
                + ";sf_dir=" + sfDir
                + ";schema_mode=off"
                + ";initial_connect_retry=true"
                + ";close_flush_timeout_millis=0"
                + ";error_inbox_capacity=4096;";
    }

    private static void flushIgnoringTerminal(Sender sender) {
        try {
            sender.flush();
        } catch (LineSenderServerException ignore) {
            // the I/O thread may latch the terminal before flush() polls for errors
        }
    }

    private void assertOnlyBaselineRow() throws Exception {
        drainWalQueue();
        assertQuery("SELECT id, d FROM legacy_nack")
                .noLeakCheck()
                .expectSize()
                .returns("id\td\n1\t1.25\n");
    }
}
