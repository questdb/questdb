/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
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

import com.nfsdb.ex.DisconnectedChannelException;
import com.nfsdb.ex.JournalRuntimeException;
import com.nfsdb.ex.SlowReadableChannelException;
import com.nfsdb.log.Log;
import com.nfsdb.log.LogFactory;
import com.nfsdb.misc.ByteBuffers;
import com.nfsdb.net.http.IOHttpJob;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.net.ssl.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class NonBlockingSecureSocketChannel implements NetworkChannel {

    private static final Log LOG = LogFactory.getLog(NonBlockingSecureSocketChannel.class);

    private final NetworkChannel channel;
    private final SSLEngine engine;
    private final ByteBuffer in;
    private final ByteBuffer out;
    private final int sslDataLimit;
    private final ByteBuffer unwrapped;
    private boolean inData = false;
    private SSLEngineResult.HandshakeStatus handshakeStatus = SSLEngineResult.HandshakeStatus.NEED_WRAP;
    private ReadState readState = ReadState.READ_CLEAN_CHANNEL;

    public NonBlockingSecureSocketChannel(NetworkChannel channel, SslConfig sslConfig) {
        this.channel = channel;
        SSLContext sslc = sslConfig.getSslContext();
        this.engine = sslc.createSSLEngine();
        this.engine.setEnableSessionCreation(true);
        this.engine.setUseClientMode(sslConfig.isClient());
        this.engine.setNeedClientAuth(sslConfig.isRequireClientAuth());
        SSLSession session = engine.getSession();
        this.sslDataLimit = session.getApplicationBufferSize();
        in = ByteBuffer.allocateDirect(session.getPacketBufferSize()).order(ByteOrder.BIG_ENDIAN);
        out = ByteBuffer.allocateDirect(session.getPacketBufferSize()).order(ByteOrder.BIG_ENDIAN);
        unwrapped = ByteBuffer.allocateDirect(sslDataLimit * 2).order(ByteOrder.BIG_ENDIAN);
    }

    @Override
    public long getFd() {
        return channel.getFd();
    }

    @Override
    public long getTotalWrittenAndReset() {
        return channel.getTotalWrittenAndReset();
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
            }
        }
    }

    @SuppressFBWarnings("SF_SWITCH_FALLTHROUGH")
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
                        ByteBuffers.copyNonBlocking(channel, in, 1000);
                        in.flip();
                        if (limit < sslDataLimit) {
                            readState = ReadState.UNWRAP_CLEAN_CACHED;
                        } else {
                            readState = ReadState.UNWRAP_DIRECT;
                        }
                    } catch (SlowReadableChannelException e) {
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
                        case CLOSED:
                            throw DisconnectedChannelException.INSTANCE;
                        default:
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
                        case CLOSED:
                            throw DisconnectedChannelException.INSTANCE;
                        default:
                            break;
                    }
                    unwrapped.flip();
                    ByteBuffers.copy(unwrapped, dst);
                    break;
                default:
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
            ByteBuffers.copyNonBlocking(out, channel, IOHttpJob.SO_WRITE_RETRY_COUNT);
        }

        int r = src.remaining();
        while (src.remaining() > 0) {
            out.clear();
            SSLEngineResult result = engine.wrap(src, out);

            if (result.getStatus() != SSLEngineResult.Status.OK) {
                throw new IOException("Expected OK, got: " + result.getStatus());
            }
            out.flip();
            ByteBuffers.copyNonBlocking(out, channel, IOHttpJob.SO_WRITE_RETRY_COUNT);
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
                        LOG.error().$("Server SSL handshake failed: ").$(e.getMessage()).$();
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

        in.clear();
        // make sure unwrapped starts by having remaining() == false
        unwrapped.position(unwrapped.limit());
    }

    private enum ReadState {
        READ_CLEAN_CHANNEL, READ_CHANNEL, UNWRAP_DIRECT, UNWRAP_CLEAN_CACHED, UNWRAP_CACHED
    }
}
