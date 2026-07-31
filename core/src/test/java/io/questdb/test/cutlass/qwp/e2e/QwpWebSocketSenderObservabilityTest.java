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
import io.questdb.client.SenderConnectionEvent;
import io.questdb.client.SenderConnectionListener;
import io.questdb.client.SenderError;
import io.questdb.client.SenderErrorHandler;
import io.questdb.client.SenderProgressHandler;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.std.ObjList;
import org.junit.Assert;
import org.junit.Test;

import java.time.temporal.ChronoUnit;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Coverage for the ten {@link QwpWebSocketSender} observability accessors
 * that previously had no direct tests. Each test names the method(s) it
 * exercises in its javadoc so a future refactor can find the regression
 * coverage by grep.
 * <p>
 * The drainer-pool accessors only have non-zero meaning when
 * {@code startOrphanDrainers} runs against real on-disk SF slot paths; the
 * tests here cover the documented "returns 0 when nothing is configured"
 * contract plus the no-op short-circuits inside {@code startOrphanDrainers}.
 */
public class QwpWebSocketSenderObservabilityTest extends AbstractQwpWebSocketTest {

    @Test
    public void testFlushAndGetSequenceReturnsNegativeOneWhenNothingPublished() throws Exception {
        runInContext((port) -> {
            try (QwpWebSocketSender sender = connectWs(port)) {
                sender.table("empty_flush")
                        .longColumn("v", 1L)
                        .at(1_000_000L, ChronoUnit.MICROS);
                long fsn1 = sender.flushAndGetSequence();
                Assert.assertEquals("first flush returns FSN", 0L, fsn1);

                long fsn2 = sender.flushAndGetSequence();
                Assert.assertEquals("empty flush returns -1", -1L, fsn2);
            }
            drainWalQueue();
        });
    }

    /**
     * Validates the key scenario from issue #7142: {@code drain()} after a
     * prior {@code flush()} with unacked frames must still wait for the
     * server to acknowledge those frames, even though the inner
     * {@code flushAndGetSequence()} returns {@code -1} (nothing published
     * by the empty flush). Without the watermark-based {@code drain()}
     * override, the default implementation would short-circuit immediately.
     */
    @Test
    public void testDrainWaitsForPriorUnackedFrames() throws Exception {
        runInContext((port) -> {
            try (QwpWebSocketSender sender = connectWs(port)) {
                // Publish data via flush (not drain), so ACK may be in-flight.
                sender.table("drain_prior")
                        .longColumn("v", 1L)
                        .at(1_000_000L, ChronoUnit.MICROS);
                sender.flush();

                // Now drain() with no new data buffered. The inner
                // flushAndGetSequence() returns -1, but drain() must still
                // wait for the prior publish to be acknowledged.
                boolean drained = sender.drain(10_000L);
                Assert.assertTrue("drain() must wait for prior unacked frames", drained);

                // Verify the server actually processed it.
                long ackedFsn = sender.getAckedFsn();
                Assert.assertTrue("ackedFsn must be >= 0 after drain", ackedFsn >= 0L);
            }
            drainWalQueue();
        });
    }

    /**
     * Exercises {@link QwpWebSocketSender#getActiveBackgroundDrainers()}.
     */
    @Test
    public void testGetActiveBackgroundDrainersZeroWhenNotStarted() throws Exception {
        runInContext((port) -> {
            try (QwpWebSocketSender sender = connectWs(port)) {
                Assert.assertEquals(0, sender.getActiveBackgroundDrainers());
            }
        });
    }

    /**
     * Exercises {@link QwpWebSocketSender#getDroppedConnectionNotifications()}.
     */
    @Test
    public void testGetDroppedConnectionNotificationsZeroInNormalFlow() throws Exception {
        runInContext((port) -> {
            // With a fast listener and a single CONNECTED event there is nothing
            // to drop; the counter must stay at zero.
            CountDownLatch connected = new CountDownLatch(1);
            try (QwpWebSocketSender sender = (QwpWebSocketSender) Sender.builder(Sender.Transport.WEBSOCKET)
                    .address("localhost:" + port)
                    .connectionListener(event -> {
                        if (event.getKind() == SenderConnectionEvent.Kind.CONNECTED) {
                            connected.countDown();
                        }
                    })
                    .closeFlushTimeoutMillis(60_000L)
                    .build()) {
                sender.table("dropped_conn_zero")
                        .longColumn("v", 1L)
                        .at(1_000_000L, ChronoUnit.MICROS);
                sender.flush();
                Assert.assertTrue("CONNECTED must fire", connected.await(10, TimeUnit.SECONDS));
                Assert.assertEquals(0L, sender.getDroppedConnectionNotifications());
            }
            drainWalQueue();
        });
    }

    /**
     * Exercises {@link QwpWebSocketSender#getDroppedErrorNotifications()}.
     */
    @Test
    public void testGetDroppedErrorNotificationsZeroInNormalFlow() throws Exception {
        runInContext((port) -> {
            try (QwpWebSocketSender sender = connectWs(port, _ -> { /* no errors expected */ })) {
                sender.table("dropped_err_zero")
                        .longColumn("v", 1L)
                        .at(1_000_000L, ChronoUnit.MICROS);
                sender.flush();
                Assert.assertEquals(0L, sender.getDroppedErrorNotifications());
            }
            drainWalQueue();
        });
    }

    /**
     * Exercises {@link QwpWebSocketSender#getTotalBackgroundDrainersFailed()}.
     */
    @Test
    public void testGetTotalBackgroundDrainersFailedZeroWhenNotStarted() throws Exception {
        runInContext((port) -> {
            try (QwpWebSocketSender sender = connectWs(port)) {
                Assert.assertEquals(0L, sender.getTotalBackgroundDrainersFailed());
            }
        });
    }

    /**
     * Exercises {@link QwpWebSocketSender#getTotalBackgroundDrainersSucceeded()}.
     */
    @Test
    public void testGetTotalBackgroundDrainersSucceededZeroWhenNotStarted() throws Exception {
        runInContext((port) -> {
            try (QwpWebSocketSender sender = connectWs(port)) {
                Assert.assertEquals(0L, sender.getTotalBackgroundDrainersSucceeded());
            }
        });
    }

    /**
     * Exercises {@link QwpWebSocketSender#getTotalBackpressureStalls()}.
     * Light load with default sizing never stalls; the counter must stay at zero.
     */
    @Test
    public void testGetTotalBackpressureStallsZeroForLightLoad() throws Exception {
        runInContext((port) -> {
            try (QwpWebSocketSender sender = connectWs(port)) {
                for (int i = 0; i < 50; i++) {
                    sender.table("backpressure_zero")
                            .longColumn("v", i)
                            .at(1_000_000_000_000L + i * 1000L, ChronoUnit.MICROS);
                }
                sender.flush();
                Assert.assertEquals(0L, sender.getTotalBackpressureStalls());
            }
            drainWalQueue();
        });
    }

    /**
     * Exercises {@link QwpWebSocketSender#getTotalConnectionEventsDelivered()}.
     * One CONNECTED event must surface to the listener; the delivered counter
     * advances past zero.
     */
    @Test
    public void testGetTotalConnectionEventsDeliveredAfterListenerObservesConnect() throws Exception {
        runInContext((port) -> {
            CountDownLatch connected = new CountDownLatch(1);
            SenderConnectionListener listener = event -> {
                if (event.getKind() == SenderConnectionEvent.Kind.CONNECTED) {
                    connected.countDown();
                }
            };
            try (QwpWebSocketSender sender = (QwpWebSocketSender) Sender.builder(Sender.Transport.WEBSOCKET)
                    .address("localhost:" + port)
                    .connectionListener(listener)
                    .closeFlushTimeoutMillis(60_000L)
                    .build()) {
                sender.table("conn_events_delivered")
                        .longColumn("v", 1L)
                        .at(1_000_000L, ChronoUnit.MICROS);
                sender.flush();
                Assert.assertTrue("CONNECTED must fire", connected.await(10, TimeUnit.SECONDS));
                long delivered = sender.getTotalConnectionEventsDelivered();
                Assert.assertTrue("expected at least one delivery, got " + delivered, delivered >= 1L);
            }
            drainWalQueue();
        });
    }

    /**
     * Exercises {@link QwpWebSocketSender#getTotalErrorNotificationsDelivered()}.
     * Send a string into a column that was first created as DOUBLE; the server
     * rejects with SCHEMA_MISMATCH (a latched TERMINAL under NACK policy v2),
     * the error handler fires, and the delivered counter advances past zero.
     */
    @Test
    public void testGetTotalErrorNotificationsDeliveredAfterSchemaMismatch() throws Exception {
        runInContext((port) -> {
            String table = "err_notif_delivered";

            // First seed the table with a DOUBLE column.
            try (QwpWebSocketSender seed = connectWs(port)) {
                seed.table(table).doubleColumn("v", 1.0).at(1_000_000L, ChronoUnit.MICROS);
                seed.flush();
            }
            drainWalQueue();

            CompletableFuture<SenderError> firstErrFut = new CompletableFuture<>();
            CompletableFuture<SenderError> terminalFut = new CompletableFuture<>();
            QwpWebSocketSender sender = connectWs(port, err -> {
                if (err.getAppliedPolicy() == SenderError.Policy.TERMINAL) {
                    terminalFut.complete(err);
                }
                firstErrFut.complete(err);
            });
            SenderError.Category expectedTerminalCategory = null;
            try {
                sender.table(table).stringColumn("v", "not-a-double").at(2_000_000L, ChronoUnit.MICROS);
                try {
                    sender.flush();
                } catch (LineSenderServerException ignored) {
                    // the I/O thread latched the terminal before flush()'s
                    // own error poll ran
                }
                SenderError err = firstErrFut.get(10, TimeUnit.SECONDS);
                Assert.assertEquals(SenderError.Category.SCHEMA_MISMATCH, err.getCategory());
                long delivered = sender.getTotalErrorNotificationsDelivered();
                Assert.assertTrue("expected at least one delivery, got " + delivered, delivered >= 1L);
                // Sanity: error did not surface as a drop on the dispatcher inbox.
                Assert.assertEquals(0L, sender.getDroppedErrorNotifications());
                expectedTerminalCategory = SenderError.Category.SCHEMA_MISMATCH;
            } finally {
                assertRejectionTerminalOnClose(sender, terminalFut, expectedTerminalCategory);
            }
            drainWalQueue();
        });
    }

    /**
     * Exercises {@link QwpWebSocketSender#getTotalFramesSent()}. Once flush()
     * returns, at least one frame must have hit the wire; the counter is
     * strictly greater than its pre-flush snapshot (which itself is zero on
     * a freshly-connected sender).
     */
    @Test
    public void testGetTotalFramesSentIncrementsAfterFlush() throws Exception {
        runInContext((port) -> {
            try (QwpWebSocketSender sender = connectWs(port)) {
                long before = sender.getTotalFramesSent();
                Assert.assertEquals(0L, before);
                for (int i = 0; i < 10; i++) {
                    sender.table("frames_sent")
                            .longColumn("v", i)
                            .at(1_000_000_000_000L + i * 1000L, ChronoUnit.MICROS);
                }
                long fsn = sender.flushAndGetSequence();
                // Wait for ACK on the FSN this flush returned, so the wire-level
                // send is definitely accounted for before we sample the counter.
                Assert.assertTrue("flush ack within 10s", sender.awaitAckedFsn(fsn, 10_000L));
                long after = sender.getTotalFramesSent();
                Assert.assertTrue("expected frames sent to grow, before=" + before + " after=" + after,
                        after > before);
            }
            drainWalQueue();
        });
    }

    /**
     * Exercises {@link QwpWebSocketSender#setProgressHandler(SenderProgressHandler)}.
     * Verifies the handler fires (strictly-increasing FSN) after a flush and
     * that a follow-up {@code setProgressHandler(null)} reverts to the no-op
     * default without crashing the dispatcher on further activity.
     */
    @Test
    public void testSetProgressHandlerFiresThenNullReverts() throws Exception {
        runInContext((port) -> {
            CountDownLatch fired = new CountDownLatch(1);
            AtomicLong lastFsn = new AtomicLong(-1L);
            SenderProgressHandler handler = fsn -> {
                lastFsn.set(fsn);
                fired.countDown();
            };
            try (QwpWebSocketSender sender = connectWs(port)) {
                sender.setProgressHandler(handler);
                sender.table("progress_handler")
                        .longColumn("v", 1L)
                        .at(1_000_000L, ChronoUnit.MICROS);
                sender.flush();
                Assert.assertTrue("progress handler must fire within 10s", fired.await(10, TimeUnit.SECONDS));
                Assert.assertTrue("FSN must be >= 0, was " + lastFsn.get(), lastFsn.get() >= 0L);

                // Revert to no-op; subsequent activity must not throw or crash
                // the dispatcher even though the handler reference is now the
                // default.
                sender.setProgressHandler(null);
                sender.table("progress_handler")
                        .longColumn("v", 2L)
                        .at(2_000_000L, ChronoUnit.MICROS);
                long fsn2 = sender.flushAndGetSequence();
                Assert.assertTrue("post-revert ack within 10s",
                        sender.awaitAckedFsn(fsn2, 10_000L));
            }
            drainWalQueue();
        });
    }

    /**
     * Exercises {@link QwpWebSocketSender#startOrphanDrainers} and
     * {@link QwpWebSocketSender#getActiveBackgroundDrainers()} together:
     * the no-op short-circuits (empty list, max=0) must not allocate the
     * drainer pool, so the active count stays at zero.
     */
    @Test
    public void testStartOrphanDrainersNoOpShortCircuits() throws Exception {
        runInContext((port) -> {
            try (QwpWebSocketSender sender = connectWs(port)) {
                ObjList<String> empty = new ObjList<>();
                sender.startOrphanDrainers(empty, 4, 1024, 4096);
                Assert.assertEquals("empty path list must not allocate drainers",
                        0, sender.getActiveBackgroundDrainers());

                ObjList<String> withOne = new ObjList<>();
                withOne.add("/nonexistent/slot");
                sender.startOrphanDrainers(withOne, 0, 1024, 4096);
                Assert.assertEquals("maxBackgroundDrainers=0 must not allocate drainers",
                        0, sender.getActiveBackgroundDrainers());

                // Both totals must also still be zero because the pool was never built.
                Assert.assertEquals(0L, sender.getTotalBackgroundDrainersSucceeded());
                Assert.assertEquals(0L, sender.getTotalBackgroundDrainersFailed());
            }
        });
    }

    /**
     * Catch-all for the error handler that fails the test if it ever fires.
     */
    @SuppressWarnings("unused")
    private static SenderErrorHandler failOnError() {
        return err -> Assert.fail("unexpected SenderError: category=" + err.getCategory()
                + " message=" + err.getServerMessage());
    }
}
