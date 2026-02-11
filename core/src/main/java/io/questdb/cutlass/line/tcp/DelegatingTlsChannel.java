/*******************************************************************************
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

package io.questdb.cutlass.line.tcp;

import io.questdb.client.Sender;
import io.questdb.cutlass.line.LineChannel;
import io.questdb.cutlass.line.LineSenderException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

import javax.net.ssl.*;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;

public final class DelegatingTlsChannel implements LineChannel {
    private static final long ADDRESS_FIELD_OFFSET;
    private static final int AFTER_HANDSHAKE = 1;
    private static final TrustManager[] BLIND_TRUST_MANAGERS = new TrustManager[]{new X509TrustManager() {
        public void checkClientTrusted(X509Certificate[] certs, String t) {
        }

        public void checkServerTrusted(X509Certificate[] certs, String t) {
        }

        public X509Certificate[] getAcceptedIssuers() {
            return null;
        }
    }};
    private static final long CAPACITY_FIELD_OFFSET;
    private static final int CLOSED = 3;
    private static final int CLOSING = 2;
    private static final int INITIAL_BUFFER_CAPACITY = 64 * 1024;
    private static final int INITIAL_STATE = 0;
    private static final long LIMIT_FIELD_OFFSET;
    private static final Log LOG = LogFactory.getLog(DelegatingTlsChannel.class);
    private final ByteBuffer dummyBuffer;
    private final SSLEngine sslEngine;

    private final ByteBuffer wrapInputBuffer;
    private LineChannel delegate;
    private int state = INITIAL_STATE;
    private ByteBuffer unwrapInputBuffer;
    private long unwrapInputBufferPtr;
    private ByteBuffer unwrapOutputBuffer;
    private long unwrapOutputBufferPtr;
    private ByteBuffer wrapOutputBuffer;
    private long wrapOutputBufferPtr;

    public DelegatingTlsChannel(LineChannel delegate, String trustStorePath, char[] password,
                                Sender.TlsValidationMode validationMode, String peerHost) {
        this.delegate = delegate;
        this.sslEngine = createSslEngine(trustStorePath, password, validationMode, peerHost);

        // wrapInputBuffer is just a placeholder, we set the internal address, capacity and limit in send()
        this.wrapInputBuffer = ByteBuffer.allocateDirect(0);

        // allows to override in tests, but we don't necessary want to expose this to users.
        int initialCapacity = Integer.getInteger("questdb.experimental.tls.buffersize", INITIAL_BUFFER_CAPACITY);

        // we want to track allocated memory hence we just create dummy direct byte buffers
        // and later reset it to manually allocated memory
        this.wrapOutputBuffer = ByteBuffer.allocateDirect(0);
        this.unwrapInputBuffer = ByteBuffer.allocateDirect(0);
        this.unwrapOutputBuffer = ByteBuffer.allocateDirect(0);

        this.wrapOutputBufferPtr = allocateMemoryAndResetBuffer(wrapOutputBuffer, initialCapacity);
        this.unwrapInputBufferPtr = allocateMemoryAndResetBuffer(unwrapInputBuffer, initialCapacity);
        this.unwrapOutputBufferPtr = allocateMemoryAndResetBuffer(unwrapOutputBuffer, initialCapacity);

        this.dummyBuffer = ByteBuffer.allocate(0);

        try {
            handshakeLoop();
        } catch (Throwable e) {
            // do not close the delegate - we don't own it when our own constructors fails
            close0(false);
            throw new LineSenderException("could not perform TLS handshake", e);
        }
    }

    @Override
    public void close() {
        close0(true);
    }

    public void close0(boolean closeDelegate) {
        int prevState = state;
        if (prevState == CLOSED) {
            return;
        }
        state = CLOSING;
        if (prevState == AFTER_HANDSHAKE) {
            try {
                sslEngine.closeOutbound();
                wrapLoop(dummyBuffer);
                writeToUpstreamAndClear();
            } catch (Throwable e) {
                LOG.error().$("could not send TLS close_notify alert").$(e).$();
            }
        }
        state = CLOSED;

        if (closeDelegate) {
            delegate = Misc.free(delegate);
        }

        // a bit of ceremony to make sure there is no point that a buffer or a pointer is referencing unallocated memory
        int capacity = wrapOutputBuffer.capacity();
        long ptrToFree = wrapOutputBufferPtr;
        wrapOutputBuffer = null; // if there is an attempt to use a buffer after close() then it's better to throw NPE than segfaulting
        wrapOutputBufferPtr = 0;
        Unsafe.free(ptrToFree, capacity, MemoryTag.NATIVE_TLS_RSS);

        capacity = unwrapInputBuffer.capacity();
        ptrToFree = unwrapInputBufferPtr;
        unwrapInputBuffer = null;
        unwrapInputBufferPtr = 0;
        Unsafe.free(ptrToFree, capacity, MemoryTag.NATIVE_TLS_RSS);

        capacity = unwrapOutputBuffer.capacity();
        ptrToFree = unwrapOutputBufferPtr;
        unwrapOutputBuffer = null;
        unwrapOutputBufferPtr = 0;
        Unsafe.free(ptrToFree, capacity, MemoryTag.NATIVE_TLS_RSS);
    }

    @Override
    public int errno() {
        return delegate.errno();
    }

    @Override
    public int receive(long ptr, int len) {
        try {
            unwrapLoop();
            unwrapOutputBuffer.flip();
            int i = unwrapOutputBufferToPtr(ptr, len);
            unwrapOutputBuffer.compact();
            return i;
        } catch (SSLException e) {
            throw new LineSenderException("could not unwrap SSL packet", e);
        }
    }

    @Override
    public void send(long ptr, int len) {
        try {
            resetBufferToPointer(wrapInputBuffer, ptr, len);
            wrapInputBuffer.position(0);
            wrapLoop(wrapInputBuffer);
            assert !wrapInputBuffer.hasRemaining();
        } catch (SSLException e) {
            throw new LineSenderException("error while sending data to questdb server", e);
        }
    }

    private static long allocateMemoryAndResetBuffer(ByteBuffer buffer, int capacity) {
        long newAddress = Unsafe.malloc(capacity, MemoryTag.NATIVE_TLS_RSS);
        resetBufferToPointer(buffer, newAddress, capacity);
        return newAddress;
    }

    private static SSLEngine createSslEngine(String trustStorePath, char[] trustStorePassword, Sender.TlsValidationMode validationMode, String peerHost) {
        assert trustStorePath == null || validationMode == Sender.TlsValidationMode.DEFAULT;
        try {
            SSLContext sslContext;
            if (trustStorePath != null) {
                sslContext = SSLContext.getInstance("TLS");
                TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                KeyStore jks = KeyStore.getInstance("JKS");
                try (InputStream trustStoreStream = openTruststoreStream(trustStorePath)) {
                    jks.load(trustStoreStream, trustStorePassword);
                }
                tmf.init(jks);
                TrustManager[] trustManagers = tmf.getTrustManagers();
                sslContext.init(null, trustManagers, new SecureRandom());
            } else if (validationMode == Sender.TlsValidationMode.INSECURE) {
                sslContext = SSLContext.getInstance("TLS");
                sslContext.init(null, BLIND_TRUST_MANAGERS, new SecureRandom());
            } else {
                sslContext = SSLContext.getDefault();
            }

            // SSLEngine needs to know hostname during TLS handshake to validate a server certificate was issued
            // for the server we are connecting to. For details see the comment below.
            // Hostname validation does not use port at all hence we can get away with a dummy value -1
            SSLEngine sslEngine = sslContext.createSSLEngine(peerHost, -1);
            if (validationMode != Sender.TlsValidationMode.INSECURE) {
                SSLParameters sslParameters = sslEngine.getSSLParameters();
                // The https validation algorithm? That looks confusing! After all we are not using any
                // https here at so what does it mean?
                // It's actually simple: It just instructs the SSLEngine to perform the same hostname validation
                // as it does during HTTPS connections. SSLEngine does not do hostname validation by default. Without
                // this option SSLEngine would happily accept any certificate as long as it's signed by a trusted CA.
                // This option will make sure certificates are accepted only if they were issued for the
                // server we are connecting to.
                sslParameters.setEndpointIdentificationAlgorithm("https");
                sslEngine.setSSLParameters(sslParameters);
            }
            sslEngine.setUseClientMode(true);
            return sslEngine;
        } catch (Throwable t) {
            if (t instanceof LineSenderException) {
                throw (LineSenderException) t;
            }
            throw new LineSenderException("could not create SSL engine", t);
        }
    }

    private static long expandBuffer(ByteBuffer buffer, long oldAddress) {
        int oldCapacity = buffer.capacity();
        int newCapacity = oldCapacity * 2;
        long newAddress = Unsafe.realloc(oldAddress, oldCapacity, newCapacity, MemoryTag.NATIVE_TLS_RSS);
        resetBufferToPointer(buffer, newAddress, newCapacity);
        return newAddress;
    }

    private static InputStream openTruststoreStream(String trustStorePath) throws FileNotFoundException {
        InputStream trustStoreStream;
        if (trustStorePath.startsWith("classpath:")) {
            String adjustedPath = trustStorePath.substring("classpath:".length());
            trustStoreStream = DelegatingTlsChannel.class.getResourceAsStream(adjustedPath);
            if (trustStoreStream == null) {
                throw new LineSenderException("configured trust store is unavailable ")
                        .put("[path=").put(trustStorePath).put("]");
            }
            return trustStoreStream;
        }
        return new FileInputStream(trustStorePath);
    }

    private static void resetBufferToPointer(ByteBuffer buffer, long ptr, int len) {
        assert buffer.isDirect();
        Unsafe.getUnsafe().putLong(buffer, ADDRESS_FIELD_OFFSET, ptr);
        Unsafe.getUnsafe().putLong(buffer, LIMIT_FIELD_OFFSET, len);
        Unsafe.getUnsafe().putLong(buffer, CAPACITY_FIELD_OFFSET, len);
    }

    private void growUnwrapInputBuffer() {
        unwrapInputBufferPtr = expandBuffer(unwrapInputBuffer, unwrapInputBufferPtr);
    }

    private void growUnwrapOutputBuffer() {
        unwrapOutputBufferPtr = expandBuffer(unwrapOutputBuffer, unwrapOutputBufferPtr);
    }

    private void growWrapOutputBuffer() {
        wrapOutputBufferPtr = expandBuffer(wrapOutputBuffer, wrapOutputBufferPtr);
    }

    private void handshakeLoop() throws SSLException {
        if (state != INITIAL_STATE) {
            return;
        }

        // trigger handshaking - otherwise the initial state is NOT_HANDSHAKING
        sslEngine.beginHandshake();
        for (; ; ) {
            SSLEngineResult.HandshakeStatus status = sslEngine.getHandshakeStatus();
            switch (status) {
                case NOT_HANDSHAKING:
                    state = AFTER_HANDSHAKE;
                    return;
                case NEED_TASK:
                    sslEngine.getDelegatedTask().run();
                    break;
                case NEED_WRAP:
                    wrapLoop(dummyBuffer);
                    break;
                case NEED_UNWRAP:
                    unwrapLoop();
                    break;
                case FINISHED:
                    throw new LineSenderException("getHandshakeStatus() returned FINISHED. It should not have been possible.");
                default:
                    throw new LineSenderException(status + "not supported");
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

        int receive = delegate.receive(adjustedPtr, remainingLen);
        if (receive < 0) {
            throw new LineSenderException("connection closed");
        }
        unwrapInputBuffer.position(unwrapInputBuffer.position() + receive);
    }

    private void unwrapLoop() throws SSLException {
        // we want the loop to return as soon as we have some unwrapped data in the output buffer
        while (unwrapOutputBuffer.position() == 0) {
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

                    // there was overflow, and we have nothing
                    // apparently the output buffer cannot fit even a single TLS record. let's grow it!
                    growUnwrapOutputBuffer();
                    break;
                case OK:
                    return;
                case CLOSED:
                    throw new LineSenderException("server closed connection unexpectedly");
            }
        }
    }

    private int unwrapOutputBufferToPtr(long dstPtr, int dstLen) {
        int oldPosition = unwrapOutputBuffer.position();

        assert Unsafe.getUnsafe().getLong(unwrapOutputBufferPtr, ADDRESS_FIELD_OFFSET) == unwrapOutputBufferPtr;
        long srcPtr = unwrapOutputBufferPtr + oldPosition;
        int srcLen = unwrapOutputBuffer.remaining();
        int len = Math.min(dstLen, srcLen);
        Vect.memcpy(dstPtr, srcPtr, len);
        unwrapOutputBuffer.position(oldPosition + len);
        return len;
    }

    private void wrapLoop(ByteBuffer src) throws SSLException {
        do {
            SSLEngineResult result = sslEngine.wrap(src, wrapOutputBuffer);
            switch (result.getStatus()) {
                case BUFFER_UNDERFLOW:
                    throw new LineSenderException("should not happen");
                case BUFFER_OVERFLOW:
                    growWrapOutputBuffer();
                    break;
                case OK:
                    writeToUpstreamAndClear();
                    break;
                case CLOSED:
                    if (state != CLOSING) {
                        throw new LineSenderException("server closed connection unexpectedly");
                    }
                    return;
            }
        } while (src.hasRemaining());
    }

    private void writeToUpstreamAndClear() {
        assert wrapOutputBuffer.limit() == wrapOutputBuffer.capacity();

        // we don't flip the wrapOutputBuffer before reading from it
        // hence the writer position is the actual length to be sent to the upstream channel
        int len = wrapOutputBuffer.position();

        assert Unsafe.getUnsafe().getLong(wrapOutputBuffer, ADDRESS_FIELD_OFFSET) == wrapOutputBufferPtr;
        delegate.send(wrapOutputBufferPtr, len);

        // we know limit == capacity
        // thus setting the position to 0 is equivalent to clearing
        wrapOutputBuffer.position(0);
    }

    static {
        Field addressField;
        Field limitField;
        Field capacityField;
        try {
            addressField = Buffer.class.getDeclaredField("address");
            limitField = Buffer.class.getDeclaredField("limit");
            capacityField = Buffer.class.getDeclaredField("capacity");
        } catch (NoSuchFieldException e) {
            // possible improvement: implement a fallback strategy when reflection is unavailable for any reason.
            throw new ExceptionInInitializerError(e);
        }
        ADDRESS_FIELD_OFFSET = Unsafe.getUnsafe().objectFieldOffset(addressField);
        LIMIT_FIELD_OFFSET = Unsafe.getUnsafe().objectFieldOffset(limitField);
        CAPACITY_FIELD_OFFSET = Unsafe.getUnsafe().objectFieldOffset(capacityField);
    }
}
