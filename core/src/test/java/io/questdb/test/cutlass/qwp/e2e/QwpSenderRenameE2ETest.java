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
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.time.temporal.ChronoUnit;

/**
 * Pinned-client coverage for RENAME TABLE racing active QWP ingestion.
 * The server-side unit tests freeze the rename window at the cache level;
 * these tests pin the CLIENT-VISIBLE contract: post-rename rows never land
 * silently in the renamed table (they either fail loudly or land in a fresh
 * auto-created table under the old name, never in the rename's destination),
 * and a salvage-modified ack stream is accepted by the real client.
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
                // there is no ack-based signal to await v=1's delivery. Poll
                // for the auto-created "sal_ws" registry entry instead of a
                // bare sleep: getTableTokenIfExists returns null while the
                // name is locked mid-create. The token is published inside
                // getOrCreateTable BEFORE the writer acquisition and the
                // append, so a non-null token proves the frame's synchronous
                // processing has STARTED, not finished; the residual window is
                // the tail of that one processMessage call, and the RENAME
                // below is a full SQL round-trip issued after this poll, which
                // in practice lands well after that window. If the salvage
                // assertions at the end of this test ever observe zero rows,
                // close the window for real: send a second deferred probe
                // frame for a throwaway table and poll for THAT table's token
                // -- same-connection frames are processed in order, a true
                // happens-after for v=1's append.
                //
                // Then pin the buffered premise the salvage below depends
                // on: v=1 reached the writer (table exists) but stayed
                // UNCOMMITTED (count 0) because deferCommit(true) held it
                // back. If a future change made setDeferCommit stop
                // buffering, sal_ws would already show v=1 here -- RENAME
                // would just carry the already-committed row forward as
                // ordinary rename semantics, the salvage branch would never
                // run, and the assertions at the end of this test would
                // still pass by accident. This assertion is what makes that
                // regression fail loudly, right here.
                TestUtils.assertEventually(() -> {
                    Assert.assertNotNull("sal_ws must be auto-created once v=1's deferred frame reaches the server",
                            engine.getTableTokenIfExists("sal_ws"));
                    assertQuery("SELECT count() FROM sal_ws")
                            .noLeakCheck()
                            .expectSize()
                            .noRandomAccess()
                            .returns("count\n0\n");
                }, 10);

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
        // writer on the old token, and the commit fails with
        // TableReferenceOutOfDateException (see the server log line "cached
        // query plan cannot be used because table schema has changed
        // [table=trades]", that exception's exact message prefix). This is a
        // pure rename with the old name left free (nothing recreates
        // "trades"), so the QWP layer maps the failure to a RETRIABLE
        // strike.
        //
        // Observed, pinned client-visible contract -- NOT the naive
        // assumption that a NACK must surface as a thrown exception:
        // the store-and-forward sender reconnects and replays the frame
        // transparently, so neither flush() nor close() throws. The replay
        // lands on a brand-new connection with its own fresh QwpTudCache,
        // which has never seen "trades"; its lookup finds the name free
        // (trades_archive now owns the old directory) and auto-creates a
        // NEW "trades" table, so v=2 lands there. What must never happen,
        // and is the actual load-bearing invariant, is v=2 landing in
        // trades_archive. Both assertions below are pinned so a future
        // change to either half of this contract -- a NACK that starts
        // throwing, or a reconnect that misroutes into trades_archive --
        // fails this test loudly instead of silently.
        runInContext((port) -> {
            boolean threwOnFlush = false;
            boolean threwOnClose = false;
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
                } catch (LineSenderException onFlush) {
                    threwOnFlush = true;
                }
            } catch (LineSenderException onClose) {
                threwOnClose = true;
            }
            Assert.assertFalse("flush() must not throw for this retriable, transparently-replayed NACK",
                    threwOnFlush);
            Assert.assertFalse("close() must not throw for this retriable, transparently-replayed NACK",
                    threwOnClose);

            drainWalQueue();
            // The replay's fresh QwpTudCache auto-creates "trades" again
            // (the old name was left free by the rename) and lands v=2
            // there, never in trades_archive.
            assertQuery("SELECT v FROM trades")
                    .noLeakCheck()
                    .expectSize()
                    .returns("v\n2\n");
            assertQuery("SELECT v FROM trades_archive")
                    .noLeakCheck()
                    .expectSize()
                    .returns("v\n1\n");
        });
    }
}
