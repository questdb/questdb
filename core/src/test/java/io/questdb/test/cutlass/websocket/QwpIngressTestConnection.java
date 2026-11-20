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

package io.questdb.test.cutlass.websocket;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cutlass.http.DefaultHttpServerConfiguration;
import io.questdb.cutlass.http.HttpFullFatServerConfiguration;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.cutlass.qwp.server.QwpIngressProcessorState;
import io.questdb.cutlass.qwp.server.QwpIngressUpgradeProcessor;
import io.questdb.cutlass.qwp.websocket.WebSocketOpcode;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.ServerDisconnectException;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;

import static io.questdb.test.cutlass.websocket.QwpIngressWireHarness.*;

/**
 * One QWP ingress connection fed a scripted client byte stream over in-memory sockets: frames delivered a receive
 * pass at a time, and the frames the server sends back.
 */
public class QwpIngressTestConnection implements QuietCloseable {
    // Large enough for the ack of a batch of thousands of tables, which the server then sends whole.
    private static final int TEST_SEND_BUFFER_SIZE = 1 << 20;
    private final TestableContext context;
    private final byte[][] frames;
    private final PhasedNetworkFacade nf;
    private final QwpIngressUpgradeProcessor processor;
    private final RecordingRawSocket rawSocket;
    private final long recvBuf;
    private final int recvBufferSize;
    private final long sendBuf;
    private final int sendBufferSize;
    private final QwpIngressProcessorState state;
    private int nextFrame;

    public QwpIngressTestConnection(CairoEngine engine, CairoConfiguration configuration, int recvBufferSize, byte[]... frames) throws Exception {
        this(engine, testHttpConfig(configuration), recvBufferSize, frames);
    }

    /**
     * A connection served by {@code state}, which it closes, through a receive buffer of {@code recvBufferSize} and
     * a send buffer of {@code sendBufferSize} bytes. With {@code isHalfClosing} the socket's half-close succeeds, as
     * a real socket's does, so a fatal CLOSE leaves the connection draining instead of disconnecting at once.
     */
    QwpIngressTestConnection(
            CairoEngine engine,
            HttpFullFatServerConfiguration httpConfig,
            QwpIngressProcessorState state,
            SecurityContext securityContext,
            boolean isDurableAckEnabled,
            int recvBufferSize,
            int sendBufferSize,
            boolean isHalfClosing,
            byte[]... frames
    ) throws Exception {
        this.frames = frames;
        this.recvBufferSize = recvBufferSize;
        this.sendBufferSize = sendBufferSize;
        this.processor = new QwpIngressUpgradeProcessor(engine, httpConfig);
        this.nf = isHalfClosing ? new HalfClosingNetworkFacade(concat(frames)) : new PhasedNetworkFacade(concat(frames));
        this.recvBuf = Unsafe.malloc(recvBufferSize, MemoryTag.NATIVE_DEFAULT);
        this.sendBuf = Unsafe.malloc(sendBufferSize, MemoryTag.NATIVE_DEFAULT);
        this.rawSocket = new RecordingRawSocket(sendBuf, sendBufferSize);
        this.context = new TestableContext(httpConfig, nf, rawSocket, recvBuf, recvBufferSize);
        this.state = state;
        state.of(-1, securityContext);
        state.setDurableAckEnabled(isDurableAckEnabled);
        getLV().set(context, state);
    }

    private QwpIngressTestConnection(CairoEngine engine, HttpFullFatServerConfiguration httpConfig, int recvBufferSize, byte[]... frames) throws Exception {
        this(
                engine,
                httpConfig,
                new QwpIngressProcessorState(recvBufferSize, httpConfig.getSendBufferSize(), engine, httpConfig.getLineHttpProcessorConfiguration()),
                AllowAllSecurityContext.INSTANCE,
                false,
                recvBufferSize,
                TEST_SEND_BUFFER_SIZE,
                false,
                frames
        );
    }

    /**
     * The highest sequence a cumulative OK ack of the server covers, or -1.
     */
    public static long maxCumulativeOkAck(ObjList<byte[]> sentFrames) {
        long max = -1;
        for (int i = 0, n = sentFrames.size(); i < n; i++) {
            final byte[] f = sentFrames.getQuick(i);
            if ((f[0] & 0x0F) != WebSocketOpcode.BINARY) {
                continue;
            }
            // server frames are unmasked; a payload above 125 bytes carries an extended length
            final int len7 = f[1] & 0x7F;
            final int offset = len7 < 126 ? 2 : len7 == 126 ? 4 : 10;
            if (f[offset] == QwpConstants.STATUS_OK) {
                max = Math.max(max, readLeLong(f, offset + 1));
            }
        }
        return max;
    }

    /**
     * A masked BINARY frame of a one-row message: column v and the designated timestamp.
     */
    public static byte[] oneRowFrame(String table, long value, long tsMicros) {
        return createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(table, value, tsMicros));
    }

    @Override
    public void close() {
        context.close();
        Unsafe.free(recvBuf, recvBufferSize, MemoryTag.NATIVE_DEFAULT);
        Unsafe.free(sendBuf, sendBufferSize, MemoryTag.NATIVE_DEFAULT);
    }

    /**
     * Delivers the client's next {@code count} frames in one receive pass. Returns true when the server tore the
     * connection down.
     */
    public boolean deliverFrames(int count) throws Exception {
        int bytes = 0;
        for (int i = 0; i < count; i++) {
            bytes += frames[nextFrame++].length;
        }
        try {
            drive(processor, context, nf, bytes);
            return false;
        } catch (ServerDisconnectException e) {
            return true;
        }
    }

    public QwpIngressProcessorState getState() {
        return state;
    }

    public ObjList<byte[]> sentFrames() {
        return rawSocket.sentFrames;
    }

    // Makes the server's send numbered sendNumber, counted from 1, park as a slow peer's does.
    void blockSend(int sendNumber) {
        rawSocket.blockedSends.add(sendNumber);
    }

    /**
     * Delivers the client's next {@code count} frames in one receive pass, in which a send may park. Returns true
     * when one did: the frames behind it wait in the receive buffer for {@link #resumeSend()}.
     */
    boolean deliverFramesUntilSendParks(int count) throws Exception {
        try {
            deliverFrames(count);
            return false;
        } catch (PeerIsSlowToReadException e) {
            return true;
        }
    }

    // What the dispatcher does once the connection is gone.
    void onConnectionClosed() {
        processor.onConnectionClosed(context);
    }

    // What the dispatcher does once the parked send's peer reads.
    void resumeSend() throws Exception {
        processor.resumeSend(context);
    }

    private static HttpFullFatServerConfiguration testHttpConfig(CairoConfiguration configuration) {
        return new DefaultHttpServerConfiguration(configuration) {
            @Override
            public int getSendBufferSize() {
                return TEST_SEND_BUFFER_SIZE;
            }
        };
    }

    private static class HalfClosingNetworkFacade extends PhasedNetworkFacade {
        HalfClosingNetworkFacade(byte[] data) {
            super(data);
        }

        @Override
        public int shutdown(long fd, int how) {
            return 0;
        }
    }
}
