/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.nfsdb.net;

import com.nfsdb.exceptions.JournalNetworkException;
import com.nfsdb.exceptions.JournalRuntimeException;
import com.nfsdb.exceptions.SlowReadableChannelException;
import com.nfsdb.logging.Logger;
import com.nfsdb.misc.ByteBuffers;

import javax.net.ssl.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ByteChannel;

public class NonBlockingSecureSocketChannel<T extends ByteChannel> implements WrappedByteChannel<T> {

    private static final Logger LOGGER = Logger.getLogger(NonBlockingSecureSocketChannel.class);

    private final T channel;
    private final SSLEngine engine;
    private final ByteBuffer in;
    private final ByteBuffer out;
    private final int sslDataLimit;
    private final boolean client;
    private boolean inData = false;
    private SSLEngineResult.HandshakeStatus handshakeStatus = SSLEngineResult.HandshakeStatus.NEED_WRAP;
    private ByteBuffer unwrapped;
    private ReadState readState = ReadState.READ_CLEAN_CHANNEL;

    public NonBlockingSecureSocketChannel(T channel, SslConfig sslConfig) throws JournalNetworkException {
        this.channel = channel;
        SSLContext sslc = sslConfig.getSslContext();
        this.engine = sslc.createSSLEngine();
        this.engine.setEnableSessionCreation(true);
        this.engine.setUseClientMode(sslConfig.isClient());
        this.engine.setNeedClientAuth(sslConfig.isRequireClientAuth());
        this.client = sslConfig.isClient();
        SSLSession session = engine.getSession();
        this.sslDataLimit = session.getApplicationBufferSize();
        in = ByteBuffer.allocateDirect(session.getPacketBufferSize()).order(ByteOrder.LITTLE_ENDIAN);
        out = ByteBuffer.allocateDirect(session.getPacketBufferSize()).order(ByteOrder.LITTLE_ENDIAN);
        unwrapped = ByteBuffer.allocateDirect(sslDataLimit * 2).order(ByteOrder.LITTLE_ENDIAN);
    }

    public T getChannel() {
        return channel;
    }

    @Override
    public boolean isOpen() {
        return channel.isOpen();
    }

    @Override
    public void close() throws IOException {
        channel.close();
        ByteBuffers.release(in);
        ByteBuffers.release(out);
        ByteBuffers.release(unwrapped);
        if (engine.isOutboundDone()) {
            engine.closeOutbound();
        }

        while (!engine.isInboundDone()) {
            try {
                engine.closeInbound();
            } catch (SSLException ignored) {
                // ignore
            }
        }
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {

        if (handshakeStatus != SSLEngineResult.HandshakeStatus.FINISHED) {
            handshake();
        }

        int p = dst.position();

        int limit = dst.remaining();

        if (limit == 0) {
            return 0;
        }

        if (unwrapped.hasRemaining()) {
//            System.out.println("copied");
            ByteBuffers.copy(unwrapped, dst);
        }

        OUT:
        while ((limit = dst.remaining()) > 0) {

//            System.out.println(readState);

            switch (readState) {
                case READ_CLEAN_CHANNEL:
                    in.clear();
                    readState = ReadState.READ_CHANNEL;
                    // fall through
                case READ_CHANNEL:
                    try {
//                        System.out.println(in.remaining());
                        ByteBuffers.copyNonBlocking(channel, in, 1);
                        in.flip();
                        if (limit < sslDataLimit) {
                            readState = ReadState.UNWRAP_CLEAN_CACHED;
                        } else {
                            readState = ReadState.UNWRAP_DIRECT;
                        }
                    } catch (SlowReadableChannelException e) {
//                        System.out.println("slow?");
                        break OUT;
                    }
                    break;
                case UNWRAP_DIRECT:
                    switch (engine.unwrap(in, dst).getStatus()) {
                        case BUFFER_OVERFLOW:
                            readState = ReadState.UNWRAP_CLEAN_CACHED;
                            break;
                        case OK:
                            if (in.remaining() == 0) {
                                readState = ReadState.READ_CLEAN_CHANNEL;
                            }
                            break;
                        case BUFFER_UNDERFLOW:
                            in.compact();
                            readState = ReadState.READ_CHANNEL;
                            break;
                    }
                    break;
                case UNWRAP_CLEAN_CACHED:
                    unwrapped.clear();
                    readState = ReadState.UNWRAP_CACHED;
                    // fall through
                case UNWRAP_CACHED:
                    switch (engine.unwrap(in, unwrapped).getStatus()) {
                        case BUFFER_OVERFLOW:
                            readState = ReadState.UNWRAP_CLEAN_CACHED;
                            break;
                        case OK:
                            if (in.remaining() == 0) {
                                readState = ReadState.READ_CLEAN_CHANNEL;
                            } else {
                                readState = ReadState.UNWRAP_CLEAN_CACHED;
                            }
                            break;
                        case BUFFER_UNDERFLOW:
                            in.compact();
                            readState = ReadState.READ_CHANNEL;
                            break;
                    }
                    unwrapped.flip();
                    ByteBuffers.copy(unwrapped, dst);
                    break;

            }
        }
        return dst.position() - p;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        if (handshakeStatus != SSLEngineResult.HandshakeStatus.FINISHED) {
            handshake();
        }

        if (out.remaining() > 0) {
            ByteBuffers.copyNonBlocking(out, channel, 10);
        }

        int r = src.remaining();
        while (src.remaining() > 0) {
            out.clear();
            SSLEngineResult result = engine.wrap(src, out);

            if (result.getStatus() != SSLEngineResult.Status.OK) {
                throw new IOException("Expected OK, got: " + result.getStatus());
            }
            out.flip();
            ByteBuffers.copyNonBlocking(out, channel, 10);
        }
        return r - src.remaining();
    }

    private void closureOnException() throws IOException {
        unwrapped.position(0);
        unwrapped.limit(0);
        SSLEngineResult sslEngineResult;
        do {
            out.clear();
            sslEngineResult = engine.wrap(unwrapped, out);
            out.flip();
            channel.write(out);
            if (sslEngineResult.getStatus() == SSLEngineResult.Status.CLOSED) {
                break;
            }
        } while (sslEngineResult.getStatus() != SSLEngineResult.Status.CLOSED && !engine.isInboundDone());
        engine.closeOutbound();
    }

    private void handshake() throws IOException {

        if (handshakeStatus == SSLEngineResult.HandshakeStatus.FINISHED) {
            return;
        }

        engine.beginHandshake();

        while (handshakeStatus != SSLEngineResult.HandshakeStatus.FINISHED) {
            switch (handshakeStatus) {
                case NOT_HANDSHAKING:
                    throw new IOException("Not handshaking");
                case NEED_WRAP:
                    out.clear();
                    unwrapped.clear();
                    try {
                        handshakeStatus = engine.wrap(unwrapped, out).getHandshakeStatus();
                    } catch (SSLException e) {
                        LOGGER.error("Server SSL handshake failed: %s", e.getMessage());
                        closureOnException();
                        throw e;
                    }
                    out.flip();
                    channel.write(out);
                    break;
                case NEED_UNWRAP:

                    if (!inData || !in.hasRemaining()) {
                        in.clear();
                        channel.read(in);
                        in.flip();
                        inData = true;
                    }

                    try {
                        SSLEngineResult res = engine.unwrap(in, unwrapped);
                        handshakeStatus = res.getHandshakeStatus();
                        switch (res.getStatus()) {
                            case BUFFER_UNDERFLOW:
                                in.compact();
                                channel.read(in);
                                in.flip();
                                break;
                            case BUFFER_OVERFLOW:
                                throw new IOException("Did not expect OVERFLOW here");
                            case OK:
                                break;
                            case CLOSED:
                                throw new IOException("Did not expect CLOSED");
                        }
                    } catch (SSLException e) {
                        LOGGER.error("Client SSL handshake failed: %s", e.getMessage());
                        throw e;
                    }
                    break;
                case NEED_TASK:
                    Runnable task;
                    while ((task = engine.getDelegatedTask()) != null) {
                        task.run();
                    }
                    handshakeStatus = engine.getHandshakeStatus();
                    break;
                default:
                    throw new JournalRuntimeException("Unknown handshake status: %s", handshakeStatus);
            }
        }

        in.clear();
        // make sure unwrapped starts by having remaining() == false
        unwrapped.position(unwrapped.limit());

        LOGGER.info("Handshake SSL complete: %s", client ? "CLIENT" : "SERVER");
    }

    private enum ReadState {
        READ_CLEAN_CHANNEL, READ_CHANNEL, UNWRAP_DIRECT, UNWRAP_CLEAN_CACHED, UNWRAP_CACHED
    }
}
