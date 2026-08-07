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

import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.std.Os;
import org.junit.Assert;
import org.junit.Test;

import java.time.temporal.ChronoUnit;

/**
 * Pinned-client coverage for RENAME TABLE racing active QWP ingestion.
 * The server-side unit tests freeze the rename window at the cache level;
 * these tests pin the CLIENT-VISIBLE contract: post-rename rows never land
 * silently in the renamed table, and a salvage-modified ack stream is
 * accepted by the real client.
 * <p>
 * Step 1 discovery: the client-side switch that keeps rows buffered
 * server-side across frames is {@link QwpWebSocketSender#setDeferCommit(boolean)}
 * (see {@code QwpWebSocketEncoder.setDeferCommit}, which sets
 * {@code FLAG_DEFER_COMMIT} on the frame). The server reads that flag into
 * {@code QwpIngressProcessorState.isDeferCommit()}
 * (core/src/main/java/io/questdb/cutlass/qwp/server/QwpIngressUpgradeProcessor.java:1430)
 * and skips the commit for the frame, leaving the row appended but
 * uncommitted in the cached writer. An existing e2e test already drives this
 * exact toggle end-to-end --
 * {@code QwpSenderE2ETest#testDeferredFramesNotAckedUntilCommit}
 * (core/src/test/java/io/questdb/test/cutlass/qwp/e2e/QwpSenderE2ETest.java:2748-2780)
 * calls {@code sender.setDeferCommit(true)} before the buffered rows and
 * {@code sender.setDeferCommit(false)} before the row whose flush must
 * commit the group. Scenario B below models that pattern: v=1 is sent with
 * deferCommit(true) so it stays buffered (uncommitted) in the writer cached
 * under "sal_ws"; after the rename+recreate, v=2 is sent with
 * deferCommit(false) so its flush is the "next frame" that
 * {@code QwpTudCache.getTableUpdateDetails} finds stale
 * (core/src/main/java/io/questdb/cutlass/qwp/server/QwpTudCache.java:475-500),
 * triggering {@code evictStaleTud} to salvage the buffered v=1 row into
 * "sal_ws_old" (core/src/main/java/io/questdb/cutlass/qwp/server/QwpTudCache.java:691-723)
 * before a fresh writer is acquired for the newly created "sal_ws".
 */
public class QwpSenderRenameE2ETest extends AbstractQwpWebSocketTest {

    @Test
    public void testRenameAndRecreateSalvageAckIsAcceptedByClient() throws Exception {
        // M7d: the salvage commit reports the RENAMED table's name through
        // the ack consumer, changing the ack payload the client sees. The
        // pinned client must process that ack without misclassifying it.
        runInContext((port) -> {
            try (QwpWebSocketSender sender = connectWs(port)) {
                // Per Step 1: deferCommit(true) keeps v=1 buffered server-side
                // (appended to the writer, not committed) across the RENAME
                // and CREATE below.
                sender.setDeferCommit(true);
                sender.table("sal_ws").longColumn("v", 1).at(1_000_000_000_000L, ChronoUnit.MICROS);
                sender.flush();
                // flush() only publishes into the client's local send engine;
                // the I/O thread transmits asynchronously and a deferred frame
                // is never acked (see
                // QwpSenderE2ETest#testDeferredFramesNotAckedUntilCommit), so
                // there is no ack-based signal to await here. Give the I/O
                // thread time to actually deliver the frame and have the
                // server append v=1 to the writer cached under "sal_ws"
                // before renaming out from under it -- the same grace-window
                // idiom that test uses for the same reason.
                Os.sleep(500);

                execute("RENAME TABLE sal_ws TO sal_ws_old");
                execute("CREATE TABLE sal_ws (v LONG, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY WAL");
                drainWalQueue();

                // Next frame triggers evictStaleTud: salvage commits v=1 into
                // sal_ws_old, the consumer records it, and the following ack
                // carries the renamed table's entry. The client must accept
                // the ack and keep the session healthy for v=2. deferCommit(false)
                // makes this flush a committing frame for the freshly
                // (re)acquired "sal_ws" writer.
                sender.setDeferCommit(false);
                sender.table("sal_ws").longColumn("v", 2).at(1_000_000_001_000L, ChronoUnit.MICROS);
                sender.flush();
            }

            drainWalQueue();
            assertQuery("SELECT v FROM sal_ws_old")
                    .noLeakCheck()
                    .expectSize()
                    .returns("v\n1\n");
            assertQuery("SELECT v FROM sal_ws")
                    .noLeakCheck()
                    .expectSize()
                    .returns("v\n2\n");
        });
    }

    @Test
    public void testRenameMidConnectionDoesNotMisrouteRows() throws Exception {
        // C1 e2e: rows sent for the OLD name after RENAME must not appear in
        // the renamed table. The server's dir-name guard keeps the cached
        // writer on the old token, the commit fails with
        // TableReferenceOutOfDateException, and the client surfaces the
        // failure instead of receiving an OK for misrouted rows.
        runInContext((port) -> {
            try (QwpWebSocketSender sender = connectWs(port)) {
                sender.table("trades").longColumn("v", 1).at(1_000_000_000_000L, ChronoUnit.MICROS);
                // drain() flushes and blocks until the server acks the row:
                // the commit (and therefore the table's creation, unlocked in
                // the name registry) must complete before the RENAME below,
                // or RENAME can race table creation -- observed as a CairoException
                // "table name is reserved" when this used a bare flush().
                Assert.assertTrue(sender.drain(30_000));
                drainWalQueue();

                execute("RENAME TABLE trades TO trades_archive");
                drainWalQueue();

                try {
                    sender.table("trades").longColumn("v", 2).at(1_000_000_001_000L, ChronoUnit.MICROS);
                    sender.flush();
                    // Depending on the client's NACK classification the
                    // failure may surface on this flush or on close; both are
                    // acceptable. What is NOT acceptable is a silent OK with
                    // the row landing in trades_archive -- the assertion below.
                } catch (LineSenderException expected) {
                }
            } catch (LineSenderException expectedOnClose) {
            }

            drainWalQueue();
            assertQuery("SELECT v FROM trades_archive")
                    .noLeakCheck()
                    .expectSize()
                    .returns("v\n1\n");
        });
    }
}
