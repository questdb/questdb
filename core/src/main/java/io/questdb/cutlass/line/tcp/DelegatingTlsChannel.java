/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.cutlass.line.tcp;

import io.questdb.cutlass.line.LineChannel;
import io.questdb.std.Misc;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.Arrays;

public final class DelegatingTlsChannel implements LineChannel {
    private static final int INITIAL_BUFFER_CAPACITY = 64 * 1024;
    private static final long ADDRESS_FIELD_OFFSET;

    private final LineChannel upstream;
    private final SSLEngine sslEngine;

    private final ByteBuffer wrapInputBuffer;
    private ByteBuffer wrapOutputBuffer;
    private ByteBuffer unwrapInputBuffer;
    private ByteBuffer unwrapOutputBuffer;
    private final ByteBuffer dummyBuffer;

    private long wrapOutputBufferPtr;
    private long unwrapInputBufferPtr;

    private boolean initialized;

    private static final TrustManager ALLOW_ALL_TRUSTMANAGER = new X509TrustManager() {
        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType) {

        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType) {
            System.out.println(Arrays.toString(chain));
        }

        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
        }
    };

    static {
        Field addressField;
        try {
            // todo: is this a good idea? we could implement our own ByteBuffer
            addressField = Buffer.class.getDeclaredField("address");
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
        ADDRESS_FIELD_OFFSET = Unsafe.getUnsafe().objectFieldOffset(addressField);
    }

    public DelegatingTlsChannel(LineChannel upstream) {
        this.upstream = upstream;
        this.sslEngine = createSslEngine();
        this.wrapInputBuffer = ByteBuffer.allocateDirect(INITIAL_BUFFER_CAPACITY);
        this.wrapOutputBuffer = ByteBuffer.allocateDirect(INITIAL_BUFFER_CAPACITY);
        this.unwrapInputBuffer = ByteBuffer.allocateDirect(INITIAL_BUFFER_CAPACITY);
        this.unwrapOutputBuffer = ByteBuffer.allocateDirect(INITIAL_BUFFER_CAPACITY);
        this.wrapOutputBufferPtr = Unsafe.getUnsafe().getLong(wrapOutputBuffer, ADDRESS_FIELD_OFFSET);
        this.unwrapInputBufferPtr = Unsafe.getUnsafe().getLong(unwrapInputBuffer, ADDRESS_FIELD_OFFSET);
        this.dummyBuffer = ByteBuffer.allocate(0);
    }

    private static SSLEngine createSslEngine() {
        try {
            SSLContext sslContext;
            // intentionally not exposed to end user as an option
            // it's used for testing, but dangerous in prod
            if (Boolean.getBoolean("questdb.dangerous.tls.trust.all")) {
                sslContext = SSLContext.getInstance("SSL");
                TrustManager[] trustManagers = new TrustManager[]{ALLOW_ALL_TRUSTMANAGER};
                sslContext.init(null, trustManagers, new SecureRandom());
            } else {
                sslContext = SSLContext.getDefault();
            }
            SSLEngine sslEngine = sslContext.createSSLEngine();
            sslEngine.setUseClientMode(true);
            return sslEngine;
        } catch (NoSuchAlgorithmException | KeyManagementException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void send(long ptr, int len) {
        try {
            handshakeIfNeeded();

            while (len != 0) {
                int i = ptrToByteBuffer(ptr, len, wrapInputBuffer);
                ptr += i;
                len -= i;
                wrapInputBuffer.flip();

                wrapLoop(wrapInputBuffer);
                assert !wrapInputBuffer.hasRemaining();
                wrapInputBuffer.clear();

                sendWrapOutputBufferAndClear();
            }
        } catch (SSLException e) {
            throw new RuntimeException(e);
        }
    }

    private void handshakeIfNeeded() throws SSLException {
        if (initialized) {
            return;
        }

        // trigger handshaking
        sslEngine.beginHandshake();
        for (;;) {
            SSLEngineResult.HandshakeStatus status = sslEngine.getHandshakeStatus();
            switch (status) {
                case NOT_HANDSHAKING:
                    initialized = true;
                    return;
                case FINISHED:
                    throw new IllegalStateException("getHandshakeStatus() returns FINISHED. This is not possible.");
                case NEED_TASK:
                    sslEngine.getDelegatedTask().run();
                    break;
                case NEED_WRAP:
                    wrapLoop(dummyBuffer);
                    sendWrapOutputBufferAndClear();
                    break;
                case NEED_UNWRAP:
                    unwrapLoop();
                    break;
                case NEED_UNWRAP_AGAIN:
                    // fall-through
                default:
                    throw new UnsupportedOperationException(status + "not implemented yet");
            }
        }
    }

    private void sendWrapOutputBufferAndClear() {
        int len = wrapOutputBuffer.position();
        assert Unsafe.getUnsafe().getLong(wrapOutputBuffer, ADDRESS_FIELD_OFFSET) == wrapOutputBufferPtr;
        upstream.send(wrapOutputBufferPtr, len);
        wrapOutputBuffer.clear();
    }

    private void growWrapOutputBuffer() {
        wrapOutputBuffer = expandBuffer(wrapInputBuffer);
        wrapOutputBufferPtr = Unsafe.getUnsafe().getLong(wrapOutputBuffer, ADDRESS_FIELD_OFFSET);
    }

    private void growUnwrapOutputBuffer() {
        unwrapOutputBuffer = expandBuffer(unwrapOutputBuffer);
    }

    private void growUnwrapInputBuffer() {
        unwrapInputBuffer = expandBuffer(unwrapInputBuffer);
        unwrapInputBufferPtr = Unsafe.getUnsafe().getLong(unwrapInputBuffer, ADDRESS_FIELD_OFFSET);
    }

    @NotNull
    private static ByteBuffer expandBuffer(ByteBuffer buffer) {
        // do we need a cap on max size?
        ByteBuffer newBuffer = ByteBuffer.allocateDirect(buffer.capacity() * 2);
        buffer.flip();
        return newBuffer.put(buffer);
    }

    private void wrapLoop(ByteBuffer src) throws SSLException {
        for (;;) {
            SSLEngineResult result = sslEngine.wrap(src, wrapOutputBuffer);
            switch (result.getStatus()) {
                case BUFFER_UNDERFLOW:
                    throw new IllegalStateException("should not happen");
                case BUFFER_OVERFLOW:
                    growWrapOutputBuffer();
                    break;
                case OK:
                    assert !src.hasRemaining();
                    return;
                case CLOSED:
                    throw new IllegalStateException("Connection closed");
            }
        }
    }

    private void unwrapLoop() throws SSLException {
        for (;;) {
            if (unwrapOutputBuffer.position() != 0) {
                // we have some decoded data ready to be read
                // no need to unwrap more
                return;
            }

            readFromUpstream(false);
            unwrapInputBuffer.flip();
            SSLEngineResult result = sslEngine.unwrap(unwrapInputBuffer, unwrapOutputBuffer);
            unwrapInputBuffer.compact();
            switch (result.getStatus()) {
                case BUFFER_UNDERFLOW:
                    // we need more input no matter what. so let's force reading from the upstream channel
                    readFromUpstream(true);
                    break;
                case BUFFER_OVERFLOW:
                    if (unwrapOutputBuffer.position() != 0) {
                        // we have at least something, that's enough
                        // if it's not enough then it's up to the caller to call us again
                        return;
                    }

                    // there was overflow and we have nothing
                    // apparently the output buffer cannot fit even a single TLS record. let's grow it!
                    growUnwrapOutputBuffer();
                    break;
                case OK:
                    return;
                case CLOSED:
                    throw new IllegalStateException("connection closed");
            }
        }
    }

    private void readFromUpstream(boolean force) {
        if (unwrapInputBuffer.position() != 0 && !force) {
            // we don't want to block on receive() if there are still data to be processed
            // unless we are forced to do so
            return;
        }

        assert unwrapInputBuffer.limit() == unwrapInputBuffer.capacity();
        int remainingLen = unwrapInputBuffer.remaining();
        if (remainingLen == 0) {
            growUnwrapInputBuffer();
            remainingLen = unwrapInputBuffer.remaining();
        }
        assert Unsafe.getUnsafe().getLong(unwrapInputBuffer, ADDRESS_FIELD_OFFSET) == unwrapInputBufferPtr;
        long adjustedPtr = unwrapInputBufferPtr + unwrapInputBuffer.position();

        int receive = upstream.receive(adjustedPtr, remainingLen);
        if (receive < 0) {
            throw new IllegalStateException("connection closed");
        }
        unwrapInputBuffer.position(unwrapInputBuffer.position() + receive);
    }

    @Override
    public int receive(long ptr, int len) {
        try {
            handshakeIfNeeded();

            unwrapLoop();
            unwrapOutputBuffer.flip();
            int i = byteBufferToPtr(unwrapOutputBuffer, ptr, len);
            unwrapOutputBuffer.compact();
            return i;
        } catch (SSLException e) {
            throw new RuntimeException(e);
        }
    }

    private static int byteBufferToPtr(ByteBuffer src, long ptr, int len) {
        int i;
        for (i = 0; i < len && src.hasRemaining(); i++) {
            Unsafe.getUnsafe().putByte(ptr + i, src.get());
        }
        return i;
    }

    private static int ptrToByteBuffer(long ptr, int len, ByteBuffer dst) {
        int i;
        for (i = 0; i < len && dst.hasRemaining(); i++) {
            dst.put(Unsafe.getUnsafe().getByte(ptr + i));
        }
        return i;
    }

    @Override
    public int errno() {
        // for now, we throw exception eagerly so this is not really useful
        return 0;
    }

    @Override
    public void close() throws IOException {
        sslEngine.closeOutbound();
        sslEngine.closeInbound();
        wrapLoop(dummyBuffer);
        sendWrapOutputBufferAndClear();

        Misc.free(upstream);
    }
}
