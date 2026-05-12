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

package io.questdb.cutlass.qwp.server.egress;

import io.questdb.cairo.TableToken;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.ServerDisconnectException;

/**
 * SPI for plugging additional QWP egress message handlers into the
 * processor without coupling the processor to specific message kinds.
 * The default implementation ({@link DefaultQwpEgressExtension}) is a
 * no-op so the processor behaves exactly as it would without an
 * extension installed.
 * <p>
 * Implementations are responsible for:
 * <ul>
 *   <li>Decoding inbound frames whose {@code msg_kind} is not one of
 *       the kinds the processor already handles directly
 *       (QUERY_REQUEST, CANCEL, CREDIT).</li>
 *   <li>Driving outbound writes during {@link #resumeSend} when the
 *       processor has no own work to do.</li>
 *   <li>Releasing per-connection state on {@link #onConnectionClosed}.</li>
 * </ul>
 * Implementations may keep per-connection state in their own
 * {@link io.questdb.cutlass.http.LocalValue} keyed by the connection context.
 */
public interface QwpEgressExtension {

    /**
     * Producer-credit hint for ingest acks. Default
     * {@link Integer#MAX_VALUE} means "no back-pressure pressure to
     * surface" so the processor continues to emit unconditional STATUS_OK
     * acks. Implementations that surface downstream back-pressure return
     * a smaller value to ask the producer to slow down, or zero to stop.
     * <p>
     * Called from the QWP-WS-ingest ack path only when the client opted
     * in via the {@code X-QWP-Request-Hints} handshake header. The
     * processor uses the returned value to populate the credits trailer
     * on STATUS_OK_WITH_HINTS frames.
     *
     * @param table the table the ack is for
     * @return non-negative credit value, units defined by the implementation
     */
    default int computeIngestCredits(TableToken table) {
        return Integer.MAX_VALUE;
    }

    /**
     * Handle an inbound WebSocket binary frame whose first-byte {@code msgKind}
     * is not handled by the processor itself.
     *
     * @param context connection context
     * @param state   shared per-connection state; the extension may read
     *                negotiated version/compression and reuse the frame
     *                writer scratches via getters but must not touch the
     *                streaming-query slot
     * @param msgKind first byte of the binary frame body
     * @param payload native pointer to the start of the frame body (msgKind included)
     * @param length  body length in bytes
     * @return {@code true} if the extension fully consumed the frame; {@code false}
     * to let the processor log the unknown-kind error and disconnect
     */
    boolean handleMessage(
            HttpConnectionContext context,
            QwpEgressProcessorState state,
            byte msgKind,
            long payload,
            int length
    ) throws PeerIsSlowToReadException, PeerDisconnectedException, ServerDisconnectException;

    /**
     * Hook polled by {@code resumeRecv} when {@code socket.recv} returns 0
     * bytes. Default returns {@code false}; the processor then throws
     * {@code PeerIsSlowToWriteException} and the dispatcher re-arms the
     * connection for READ. Implementations with pending outbound work
     * for this connection return {@code true}; the processor throws
     * {@code PeerIsSlowToReadException} instead, so the dispatcher arms
     * WRITE for the next round.
     * <p>
     * This is the recovery path out of the dispatcher's lost-WRITE
     * starvation pattern: when a server-initiated
     * {@code registerChannel(WRITE)} loses to a {@code registerChannel(READ)}
     * in {@code epoll_ctl(MOD)}'s last-arm-wins ordering, the eventual
     * dispatched READ event lands here, converts itself into a WRITE arm
     * for the next round, and the queued outbound work drains via
     * {@code resumeSend} on the next dispatch cycle. Just a boolean check;
     * the actual drain stays on the WRITE path so we keep one-worker-per-
     * context invariants intact (no concurrent {@code resumeSend} from
     * two workers via the recv path).
     */
    default boolean hasPendingOutboundWork(HttpConnectionContext context) {
        return false;
    }

    /**
     * Notification that the processor has just emitted (and applied) a
     * {@code CACHE_RESET} frame on this connection. Implementations whose
     * outbound state references the connection-scoped schema cache or symbol
     * dict should fail-safe: the
     * previously allocated schema ids and symbol-dict entries are no longer
     * valid, so any in-flight stream that would have referenced them must
     * terminate or rebind.
     *
     * @param mask the reset mask: bitwise OR of
     *             {@link io.questdb.cutlass.qwp.codec.QwpEgressMsgKind#RESET_MASK_DICT}
     *             and
     *             {@link io.questdb.cutlass.qwp.codec.QwpEgressMsgKind#RESET_MASK_SCHEMAS}
     */
    void onCacheReset(HttpConnectionContext context, QwpEgressProcessorState state, byte mask);

    /**
     * Notification that the connection is being closed. Implementations must
     * free any per-connection resources keyed off the context.
     */
    void onConnectionClosed(HttpConnectionContext context);

    /**
     * Drive any extension-side outbound work for this connection. Called
     * from the processor's {@code resumeSend} after it has finished its
     * own deferred-flush + streaming work, or when it has nothing
     * pending. Implementations should write any pending frames to the
     * response socket via the supplied state's frame writer scratches,
     * throwing {@link PeerIsSlowToReadException} when the socket parks.
     *
     * @param context connection context
     * @param state   shared per-connection state, or {@code null} if no
     *                state has been allocated yet (handshake not complete)
     */
    void resumeSend(
            HttpConnectionContext context,
            QwpEgressProcessorState state
    ) throws PeerIsSlowToReadException, PeerDisconnectedException, ServerDisconnectException;
}
