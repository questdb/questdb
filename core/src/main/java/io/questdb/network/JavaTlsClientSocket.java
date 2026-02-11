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

package io.questdb.network;

import io.questdb.ClientTlsConfiguration;
import io.questdb.cutlass.line.LineSenderException;
import io.questdb.cutlass.line.tcp.DelegatingTlsChannel;
import io.questdb.log.Log;
import io.questdb.std.Chars;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

public final class JavaTlsClientSocket implements Socket {

    private static final long ADDRESS_FIELD_OFFSET;
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
    private static final int INITIAL_BUFFER_CAPACITY_BYTES = 256 * 1024;
    private static final long LIMIT_FIELD_OFFSET;
    private static final int STATE_CLOSING = 3;
    private static final int STATE_EMPTY = 0;
    private static final int STATE_PLAINTEXT = 1;
    private static final int STATE_TLS = 2;
    private final Socket delegate;
    private final Log log;
    private final ClientTlsConfiguration tlsConfig;
    private final ByteBuffer unwrapInputBuffer;
    private final ByteBuffer unwrapOutputBuffer;
    private final ByteBuffer wrapInputBuffer;
    private final ByteBuffer wrapOutputBuffer;
    private SSLEngine sslEngine;
    private int state = STATE_EMPTY;
    private long unwrapInputBufferPtr;
    private long wrapOutputBufferPtr;

    JavaTlsClientSocket(NetworkFacade nf, Log log, ClientTlsConfiguration tlsConfig) {
        this.delegate = new PlainSocket(nf, log);
        this.log = log;
        this.tlsConfig = tlsConfig;

        // wrapInputBuffer are just placeholders. we set the internal address, capacity and limit in send() and recv().
        // so read/write from/to a buffer supplied by the caller and avoid unnecessary memory copies.
        // also, handshake does not to read/write from/to these buffers so it does not matter if they have capacity = 0
        // during handshake.
        this.wrapInputBuffer = ByteBuffer.allocateDirect(0);
        this.unwrapOutputBuffer = ByteBuffer.allocateDirect(0);

        // wrapOutputBuffer and unwrapInputBuffer are crated with capacity 0. why?
        // we allocate the actual memory only when starting a new TLS session.
        // this way we can reuse the same ByteBuffer instances for multiple TLS sessions.
        this.wrapOutputBuffer = ByteBuffer.allocateDirect(0);
        this.unwrapInputBuffer = ByteBuffer.allocateDirect(0);
    }

    @Override
    public void close() {
        log.debug().$("closing TLS socket [fd=").$(delegate.getFd()).$(']').$();
        switch (state) {
            case STATE_CLOSING: // intentional fall through
            case STATE_EMPTY:
                return;
            case STATE_TLS: {
                assert sslEngine != null;
                state = STATE_CLOSING;
                sslEngine.closeOutbound();
                try {
                    // we don't care about the result. wrap() is just to generate a close_notify TLS record
                    // if that fails, we don't care, we are closing anyway
                    sslEngine.wrap(wrapInputBuffer, wrapOutputBuffer);
                    while (wantsTlsWrite()) {
                        int n = tlsIO(Socket.WRITE_FLAG);
                        if (n < 0) {
                            log.debug().$("could not send TLS close_notify").$();
                            break;
                        }
                    }
                } catch (SSLException e) {
                    log.debug().$("could not send TLS close_notify").$(e).$();
                }
                sslEngine = null;
            } // fall through
            case STATE_PLAINTEXT:
                state = STATE_CLOSING;
                // it could be that we allocated buffers but failed to start a TLS session
                // so we need to free the buffers even in the STATE_PLAINTEXT state
                freeInternalBuffers();
                delegate.close();
                state = STATE_EMPTY;
                break;
        }
    }

    @Override
    public long getFd() {
        return delegate.getFd();
    }

    @Override
    public boolean isClosed() {
        return delegate.isClosed();
    }

    @Override
    public boolean isMorePlaintextBuffered() {
        return false;
    }

    @Override
    public boolean isTlsSessionStarted() {
        return sslEngine != null;
    }

    @Override
    public void of(long fd) {
        assert state == STATE_EMPTY;
        delegate.of(fd);
        state = STATE_PLAINTEXT;
    }

    @Override
    public int recv(long bufferPtr, int bufferLen) {
        assert sslEngine != null;

        resetBufferToPointer(unwrapOutputBuffer, bufferPtr, bufferLen);
        unwrapOutputBuffer.position(0);

        try {
            int plainBytesReceived = 0;
            for (; ; ) {
                int n = readFromSocket();
                assert unwrapInputBuffer.position() == 0 : "unwrapInputBuffer is not compacted";
                int bytesAvailable = unwrapInputBuffer.limit();
                if (n < 0 && bytesAvailable == 0) {
                    if (plainBytesReceived == 0) {
                        // we didn't manage to read anything from the socket, let's return the error
                        return n;
                    }
                    // we have some data to return, let's return it
                    return plainBytesReceived;
                }


                if (bytesAvailable == 0) {
                    // nothing to unwrap, we are done
                    return plainBytesReceived;
                }

                SSLEngineResult result = sslEngine.unwrap(unwrapInputBuffer, unwrapOutputBuffer);
                plainBytesReceived += result.bytesProduced();

                // compact the TLS buffer
                int bytesConsumed = result.bytesConsumed();
                int bytesRemaining = bytesAvailable - bytesConsumed;
                Vect.memcpy(unwrapInputBufferPtr, unwrapInputBufferPtr + bytesConsumed, bytesRemaining);
                unwrapInputBuffer.position(0);
                unwrapInputBuffer.limit(bytesRemaining);

                switch (result.getStatus()) {
                    case BUFFER_UNDERFLOW:
                        // we need more data to unwrap, let's return whatever we have
                        return plainBytesReceived;
                    case BUFFER_OVERFLOW:
                        if (unwrapOutputBuffer.position() == 0) {
                            // not even a single byte was written to the output buffer even the buffer is empty
                            throw new AssertionError("Output buffer too small to fit a single TLS record. This should not happen, please report as a bug.");
                        }
                        // we have some data to return, let's return it
                        return plainBytesReceived;
                    case OK:
                        break;
                    case CLOSED:
                        log.debug().$("SSL engine closed").$();
                        // We received a TLS close notification from the server. We don't expect any further data from this connection.
                        // If we have some previously unwrapped data then let's return it so the caller has a chance to process them.
                        // If a caller calls recv() again and we have no remaining plaintext to return, we will return -1 so the
                        // caller learned that the connection is closed.
                        // If we have no plaintext data to return now then we can immediately indicate that we are done with the connection.
                        return plainBytesReceived == 0 ? -1 : plainBytesReceived;
                }
            }
        } catch (SSLException e) {
            log.error().$("could not unwrap SSL packet").$(e).$();
            return -1;
        }
    }

    @Override
    public int send(long bufferPtr, int bufferLen) {
        try {
            resetBufferToPointer(wrapInputBuffer, bufferPtr, bufferLen);
            wrapInputBuffer.position(0);
            int plainBytesConsumed = 0;
            for (; ; ) {
                // try to send whatever we have in the encrypted buffer
                int bytesToSend = wrapOutputBuffer.position();
                if (bytesToSend > 0) {
                    int sent = writeToSocket(bytesToSend);
                    if (sent < 0) {
                        return sent;
                    } else if (sent < bytesToSend) {
                        // we didn't manage to send everything we wanted, the socket is full
                        return plainBytesConsumed;
                    }
                }

                if (wrapInputBuffer.remaining() == 0) {
                    // we sent whatever we could and there is nothing left to be wrapped
                    return plainBytesConsumed;
                }

                SSLEngineResult result = sslEngine.wrap(wrapInputBuffer, wrapOutputBuffer);
                plainBytesConsumed += result.bytesConsumed();
                switch (result.getStatus()) {
                    case BUFFER_UNDERFLOW:
                        throw new AssertionError("Underflow while reading a plain text. This should not happen, please report as a bug");
                    case BUFFER_OVERFLOW:
                        if (wrapOutputBuffer.position() == 0) {
                            // not even a single byte was written to the output buffer even the buffer is empty
                            // apparently the output buffer cannot fit even a single TLS record. let's grow it and try again!
                            growWrapOutputBuffer();
                        }
                        break;
                    case OK:
                        break;
                    case CLOSED:
                        log.error().$("Attempt to send to a closed SSLEngine").$();
                        return -1;
                }
            }
        } catch (SSLException e) {
            log.error().$("could not wrap SSL packet").$(e).$();
            return -1;
        }
    }

    @Override
    public int shutdown(int how) {
        return delegate.shutdown(how);
    }

    @Override
    public void startTlsSession(CharSequence peerName) throws TlsSessionInitFailedException {
        assert state == STATE_PLAINTEXT;
        prepareInternalBuffers();
        try {
            this.sslEngine = createSslEngine(peerName);
            this.sslEngine.beginHandshake();
            SSLEngineResult.HandshakeStatus handshakeStatus = sslEngine.getHandshakeStatus();
            while (handshakeStatus != SSLEngineResult.HandshakeStatus.FINISHED) {
                switch (handshakeStatus) {
                    case NEED_TASK:
                        Runnable task;
                        while ((task = sslEngine.getDelegatedTask()) != null) {
                            task.run();
                        }
                        handshakeStatus = sslEngine.getHandshakeStatus();
                        break;
                    case NEED_WRAP: {
                        SSLEngineResult result = sslEngine.wrap(wrapInputBuffer, wrapOutputBuffer);
                        handshakeStatus = result.getHandshakeStatus();
                        switch (result.getStatus()) {
                            case BUFFER_UNDERFLOW:
                                // there cannot be underflow since wrap() during handshake does not read from the input buffer at all
                                throw new AssertionError("Buffer underflow during TLS handshake. This should not happen. please report as a bug");
                            case BUFFER_OVERFLOW:
                                // in theory, this can happen if the output buffer is too small to fit a single TLS handshake record, but that would indicate
                                // our starting buffer is too small.
                                throw new AssertionError("Buffer overflow during TLS handshake. This should not happen, please report as a bug");
                            case OK:
                                // wrapOutputBuffer: write mode
                                int written = 0;
                                int bufferLimit = wrapOutputBuffer.position();
                                while (written < bufferLimit) {
                                    int n = delegate.send(wrapOutputBufferPtr + written, bufferLimit - written);
                                    if (n < 0) {
                                        throw TlsSessionInitFailedException.instance("socket write error");
                                    }
                                    written += n;
                                }
                                wrapOutputBuffer.clear();
                                break;
                            case CLOSED:
                                throw TlsSessionInitFailedException.instance("server closed connection unexpectedly");
                        }
                        break;
                    }
                    case NEED_UNWRAP: {
                        int n = readFromSocket();
                        if (n < 0) {
                            throw TlsSessionInitFailedException.instance("socket read error");
                        }
                        SSLEngineResult result = sslEngine.unwrap(unwrapInputBuffer, unwrapOutputBuffer);
                        handshakeStatus = result.getHandshakeStatus();
                        switch (result.getStatus()) {
                            case BUFFER_UNDERFLOW:
                                // we need to receive more data from a socket, let's try again
                                break;
                            case BUFFER_OVERFLOW:
                                throw new AssertionError("Buffer overflow during TLS handshake. This should not happen, please report as a bug");
                            case OK:
                                // good, let's see what we need to do next
                                break;
                            case CLOSED:
                                throw TlsSessionInitFailedException.instance("server closed connection unexpectedly");
                        }
                    }
                    break;
                }
            }
            // unwrap input buffer: read mode and empty
            unwrapInputBuffer.position(0);
            unwrapInputBuffer.limit(0);

            // write mode and empty
            unwrapOutputBuffer.clear();
            wrapOutputBuffer.clear();
            state = STATE_TLS;
        } catch (KeyManagementException | NoSuchAlgorithmException | KeyStoreException | IOException |
                 CertificateException e) {
            throw TlsSessionInitFailedException.instance("TLS session creation failed [error=").put(e.getMessage()).put(']');
        }
    }

    @Override
    public boolean supportsTls() {
        return true;
    }

    @Override
    public int tlsIO(int readinessFlags) {
        if ((readinessFlags & WRITE_FLAG) != 0) {
            int bytesToSend = wrapOutputBuffer.position();
            if (bytesToSend > 0) {
                int n = writeToSocket(bytesToSend);
                return Math.min(n, 0);
            }
        }
        return 0;
    }

    @Override
    public boolean wantsTlsRead() {
        // Ops, we cannot tell if we want to read from a network socket. How is that possible?
        // The problem is that we don't know if we have enough data in our internal buffer to unwrap.
        // We can only tell when we try to unwrap it. It could be we have *some* data, but they are not enough to unwrap
        // a single TLS record. In this case we need to read more data from the socket.
        // Maybe you are asking: Cannot we simply always return true? No, we cannot. If we return true, then the
        // event loop will wait until there is data to read from the socket. But maybe we do have a full TLS record in
        // the buffer and we can unwrap it. In this case we don't want to wait for socket ot have more data to be
        // available for reading.
        // So it's better to pretend we never want to read and the caller simple calls recv(). If recv()
        // returns 0 then we know there is not enough data available and we need to wait for socket to have more data.
        return false;
    }

    @Override
    public boolean wantsTlsWrite() {
        // we want to write if we have TLS data to send
        return wrapOutputBuffer.position() > 0;
    }

    private static long allocateMemoryAndResetBuffer(ByteBuffer buffer, int capacity) {
        long newAddress = Unsafe.malloc(capacity, MemoryTag.NATIVE_TLS_RSS);
        resetBufferToPointer(buffer, newAddress, capacity);
        return newAddress;
    }

    private static long expandBuffer(ByteBuffer buffer, long oldAddress) {
        int oldCapacity = buffer.capacity();
        int newCapacity = oldCapacity * 2;
        long newAddress = Unsafe.realloc(oldAddress, oldCapacity, newCapacity, MemoryTag.NATIVE_TLS_RSS);
        resetBufferToPointer(buffer, newAddress, newCapacity);
        return newAddress;
    }

    private static InputStream openTrustStoreStream(String trustStorePath) throws FileNotFoundException {
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
        buffer.position(0);
    }

    private SSLEngine createSslEngine(CharSequence serverName) throws KeyManagementException, NoSuchAlgorithmException, KeyStoreException, IOException, CertificateException {
        SSLContext sslContext;
        String trustStorePath = tlsConfig.trustStorePath();
        int tlsValidationMode = tlsConfig.tlsValidationMode();
        if (trustStorePath != null) {
            sslContext = SSLContext.getInstance("TLS");
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            KeyStore jks = KeyStore.getInstance("JKS");
            try (InputStream trustStoreStream = openTrustStoreStream(trustStorePath)) {
                jks.load(trustStoreStream, tlsConfig.trustStorePassword());
            }
            tmf.init(jks);
            TrustManager[] trustManagers = tmf.getTrustManagers();
            sslContext.init(null, trustManagers, new SecureRandom());
        } else if (tlsValidationMode == ClientTlsConfiguration.TLS_VALIDATION_MODE_NONE) {
            sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, BLIND_TRUST_MANAGERS, new SecureRandom());
        } else {
            sslContext = SSLContext.getDefault();
        }

        SSLEngine sslEngine = sslContext.createSSLEngine(Chars.toString(serverName), -1);
        if (tlsValidationMode != ClientTlsConfiguration.TLS_VALIDATION_MODE_NONE) {
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
    }

    private void freeInternalBuffers() {
        long ptrToFree = wrapOutputBufferPtr;
        if (ptrToFree != 0) {
            int capacity = wrapOutputBuffer.capacity();
            assert capacity != 0;
            // reset to dummy buffer so a bug in the code will not dereference a dangling pointer
            // if a bug results in a dereference of null then a subsequent crash produces a better diagnostics
            resetBufferToPointer(wrapOutputBuffer, 0, 0);
            wrapOutputBufferPtr = 0;
            Unsafe.free(ptrToFree, capacity, MemoryTag.NATIVE_TLS_RSS);

            // if the first buffer was initialized then the 2nd buffer must have been initialized too
            assert unwrapInputBufferPtr != 0;
            capacity = unwrapInputBuffer.capacity();
            assert capacity != 0;
            resetBufferToPointer(unwrapInputBuffer, 0, 0);
            ptrToFree = unwrapInputBufferPtr;
            unwrapInputBufferPtr = 0;
            Unsafe.free(ptrToFree, capacity, MemoryTag.NATIVE_TLS_RSS);
        }
    }

    private void growWrapOutputBuffer() {
        wrapOutputBufferPtr = expandBuffer(wrapOutputBuffer, wrapOutputBufferPtr);
    }

    private void prepareInternalBuffers() {
        int initialCapacity = Integer.getInteger("questdb.experimental.tls.buffersize", INITIAL_BUFFER_CAPACITY_BYTES);
        this.wrapOutputBufferPtr = allocateMemoryAndResetBuffer(wrapOutputBuffer, initialCapacity);
        this.unwrapInputBufferPtr = allocateMemoryAndResetBuffer(unwrapInputBuffer, initialCapacity);
        unwrapInputBuffer.flip(); // read mode
    }

    private int readFromSocket() {
        // unwrap input buffer: read mode

        int writerPos = unwrapInputBuffer.limit(); // we are in the read mode, so limit (for reader) = position for writer
        int freeSpace = unwrapInputBuffer.capacity() - writerPos;
        if (freeSpace == 0) {
            // no point in reading if we have no space left
            return 0;
        }

        assert Unsafe.getUnsafe().getLong(unwrapInputBuffer, ADDRESS_FIELD_OFFSET) == unwrapInputBufferPtr;
        long adjustedPtr = unwrapInputBufferPtr + writerPos;

        int n = delegate.recv(adjustedPtr, freeSpace);
        if (n < 0) {
            return n;
        }
        unwrapInputBuffer.limit(writerPos + n);
        return n;
    }

    private int writeToSocket(int bytesToSend) {
        // wrapOutputBuffer is in the write mode
        int n = delegate.send(wrapOutputBufferPtr, bytesToSend);
        if (n < 0) {
            // ops, something went wrong
            return n;
        }

        int bytesRemaining = bytesToSend - n;
        // compact the buffer
        Vect.memmove(wrapOutputBufferPtr, wrapOutputBufferPtr + n, bytesRemaining);
        wrapOutputBuffer.position(bytesRemaining);
        return n;
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
