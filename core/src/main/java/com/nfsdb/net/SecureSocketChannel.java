/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (C) 2014-2016 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 *
 ******************************************************************************/

package com.nfsdb.net;

import com.nfsdb.ex.JournalNetworkException;
import com.nfsdb.ex.JournalRuntimeException;
import com.nfsdb.log.Log;
import com.nfsdb.log.LogFactory;
import com.nfsdb.misc.ByteBuffers;

import javax.net.ssl.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;

public class SecureSocketChannel implements WrappedByteChannel {

    private static final Log LOG = LogFactory.getLog(SecureSocketChannel.class);

    private final SocketChannel socketChannel;
    private final SSLEngine engine;
    private final ByteBuffer inBuf;
    private final ByteBuffer outBuf;
    private final int sslDataLimit;
    private final boolean client;
    private final ByteBuffer swapBuf;
    private boolean inData = false;
    private SSLEngineResult.HandshakeStatus handshakeStatus = SSLEngineResult.HandshakeStatus.NEED_WRAP;
    private boolean fillInBuf = true;

    public SecureSocketChannel(SocketChannel socketChannel, SslConfig sslConfig) {
        this.socketChannel = socketChannel;
        SSLContext sslc = sslConfig.getSslContext();
        this.engine = sslc.createSSLEngine();
        this.engine.setEnableSessionCreation(true);
        this.engine.setUseClientMode(sslConfig.isClient());
        this.engine.setNeedClientAuth(sslConfig.isRequireClientAuth());
        this.client = sslConfig.isClient();
        SSLSession session = engine.getSession();
        this.sslDataLimit = session.getApplicationBufferSize();
        inBuf = ByteBuffer.allocateDirect(session.getPacketBufferSize()).order(ByteOrder.LITTLE_ENDIAN);
        outBuf = ByteBuffer.allocateDirect(session.getPacketBufferSize()).order(ByteOrder.LITTLE_ENDIAN);
        swapBuf = ByteBuffer.allocateDirect(sslDataLimit * 2).order(ByteOrder.LITTLE_ENDIAN);
    }

    public SocketChannel getChannel() {
        return socketChannel;
    }

    @Override
    public boolean isOpen() {
        return socketChannel.isOpen();
    }

    @Override
    public void close() throws IOException {
        socketChannel.close();
        ByteBuffers.release(inBuf);
        ByteBuffers.release(outBuf);
        ByteBuffers.release(swapBuf);
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

        while (true) {

            int limit = dst.remaining();
            if (limit == 0) {
                break;
            }

            // check if anything is remaining in swapBuf
            if (swapBuf.hasRemaining()) {

                ByteBuffers.copy(swapBuf, dst);

            } else {

                if (fillInBuf) {
                    // read from channel
                    inBuf.clear();
                    int result = socketChannel.read(inBuf);

                    if (result == -1) {
                        throw new IOException("Did not expect -1 from socket channel");
                    }

                    if (result == 0) {
                        throw new IOException("Blocking connection must not return 0");
                    }

                    inBuf.flip();
                }

                // dst is larger than minimum?
                if (limit < sslDataLimit) {
                    // no, dst is small, use swap
                    swapBuf.clear();
                    fillInBuf = unwrap(swapBuf);
                    swapBuf.flip();
                    ByteBuffers.copy(swapBuf, dst);
                } else {
                    fillInBuf = unwrap(dst);
                }
            }
        }
        return dst.position() - p;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        if (handshakeStatus != SSLEngineResult.HandshakeStatus.FINISHED) {
            handshake();
        }

        int count = src.remaining();
        while (src.hasRemaining()) {
            outBuf.clear();
            SSLEngineResult result = engine.wrap(src, outBuf);
            if (result.getStatus() != SSLEngineResult.Status.OK) {
                throw new IOException("Expected OK, got: " + result.getStatus());
            }
            outBuf.flip();
            try {
                ByteBuffers.copy(outBuf, socketChannel);
            } catch (JournalNetworkException e) {
                throw new IOException(e);
            }
        }
        return count;
    }

    private void closureOnException() throws IOException {
        swapBuf.position(0);
        swapBuf.limit(0);
        SSLEngineResult sslEngineResult;
        do {
            outBuf.clear();
            sslEngineResult = engine.wrap(swapBuf, outBuf);
            outBuf.flip();
            socketChannel.write(outBuf);
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
                    outBuf.clear();
                    swapBuf.clear();
                    try {
                        handshakeStatus = engine.wrap(swapBuf, outBuf).getHandshakeStatus();
                    } catch (SSLException e) {
                        LOG.error().$("Server SSL handshake failed: ").$(e.getMessage()).$();
                        closureOnException();
                        throw e;
                    }
                    outBuf.flip();
                    socketChannel.write(outBuf);
                    break;
                case NEED_UNWRAP:

                    if (!inData || !inBuf.hasRemaining()) {
                        inBuf.clear();
                        socketChannel.read(inBuf);
                        inBuf.flip();
                        inData = true;
                    }

                    try {
                        SSLEngineResult res = engine.unwrap(inBuf, swapBuf);
                        handshakeStatus = res.getHandshakeStatus();
                        switch (res.getStatus()) {
                            case BUFFER_UNDERFLOW:
                                inBuf.compact();
                                socketChannel.read(inBuf);
                                inBuf.flip();
                                break;
                            case BUFFER_OVERFLOW:
                                throw new IOException("Did not expect OVERFLOW here");
                            case OK:
                                break;
                            case CLOSED:
                                throw new IOException("Did not expect CLOSED");
                            default:
                                break;
                        }
                    } catch (SSLException e) {
                        LOG.error().$("Client SSL handshake failed: ").$(e.getMessage()).$();
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

        inBuf.clear();
        // make sure swapBuf starts by having remaining() == false
        swapBuf.position(swapBuf.limit());

        LOG.info().$("Handshake SSL complete: ").$(client ? "CLIENT" : "SERVER").$();
    }

    private boolean unwrap(ByteBuffer dst) throws IOException {
        while (inBuf.hasRemaining()) {
            SSLEngineResult.Status status = engine.unwrap(inBuf, dst).getStatus();
            switch (status) {
                case BUFFER_UNDERFLOW:
                    inBuf.compact();
                    socketChannel.read(inBuf);
                    inBuf.flip();
                    break;
                case BUFFER_OVERFLOW:
                    return false;
                case OK:
                    break;
                case CLOSED:
                    throw new IOException("Did not expect CLOSED");
                default:
                    break;
            }
        }
        return true;
    }
}
