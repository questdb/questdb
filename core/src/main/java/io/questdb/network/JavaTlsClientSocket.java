/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.cutlass.http.client.HttpClientException;
import io.questdb.log.Log;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;

import javax.net.ssl.*;
import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
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
    private static final int INITIAL_BUFFER_CAPACITY = 64 * 1024;
    private static final long LIMIT_FIELD_OFFSET;
    private static final int STATE_CLOSING = 2;
    private static final int STATE_HANDSHAKE_COMPLETED = 1;
    private static final int STATE_INITIAL = 0;
    private final Socket delegate;
    private final Log log;
    private final ByteBuffer unwrapOutputBuffer;
    private final ByteBuffer wrapInputBuffer;
    private SSLEngine sslEngine;
    private int state = STATE_INITIAL;
    private ByteBuffer unwrapInputBuffer;
    private long unwrapInputBufferPtr;
    private ByteBuffer wrapOutputBuffer;
    private long wrapOutputBufferPtr;

    JavaTlsClientSocket(NetworkFacade nf, Log log) {
        this.delegate = new PlainSocket(nf, log);
        this.log = log;
        int initialCapacity = Integer.getInteger("questdb.experimental.tls.buffersize", INITIAL_BUFFER_CAPACITY);

        // wrapInputBuffer is just a placeholder, we set the internal address, capacity and limit in send()
        this.wrapInputBuffer = ByteBuffer.allocateDirect(0);
        // also a placeholder
        this.unwrapOutputBuffer = ByteBuffer.allocateDirect(0);

        // we want to track allocated memory hence we just create dummy direct byte buffers
        // and later reset it to manually allocated memory
        this.wrapOutputBuffer = ByteBuffer.allocateDirect(0);
        this.unwrapInputBuffer = ByteBuffer.allocateDirect(0);


        this.wrapOutputBufferPtr = allocateMemoryAndResetBuffer(wrapOutputBuffer, initialCapacity);
        this.unwrapInputBufferPtr = allocateMemoryAndResetBuffer(unwrapInputBuffer, initialCapacity);
    }

    @Override
    public void close() {
        // todo: do a proper TLS shutdown
        sslEngine.closeOutbound();
        state = STATE_CLOSING;
        delegate.close();

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
    }

    @Override
    public int getFd() {
        return delegate.getFd();
    }

    @Override
    public boolean isClosed() {
        return delegate.isClosed();
    }

    @Override
    public boolean isTlsSessionStarted() {
        return sslEngine != null;
    }

    @Override
    public void of(int fd) {
        delegate.of(fd);
        if (startTlsSession() < 0) {
            throw new HttpClientException("could not start TLS session");
        }
        unwrapInputBuffer.clear();
        unwrapOutputBuffer.clear();
        wrapOutputBuffer.clear();
    }

    @Override
    public int recv(long bufferPtr, int bufferLen) {
        assert sslEngine != null;
        // unwrap  input buffer: write mode
        // unwrap output buffer: write mode

        resetBufferToPointer(unwrapOutputBuffer, bufferPtr, bufferLen);
        unwrapOutputBuffer.position(0);

        // first, try to read whatever we have in the output buffer
        int plainTextReadBytes = 0;

        try {
            for (; ; ) {
                // unwrap input buffer: write mode
                // unwrap output buffer: write mode
                int n = readFromUpstream();
                if (n < 0) {
                    return n;
                } else if (unwrapInputBuffer.position() == 0) {
                    // nothing to unwrap
                    return plainTextReadBytes;
                }

                unwrapInputBuffer.flip();
                // unwrap input buffer: read mode
                // unwrap output buffer: write mode
                SSLEngineResult result = sslEngine.unwrap(unwrapInputBuffer, unwrapOutputBuffer);
                plainTextReadBytes += result.bytesProduced();
                unwrapInputBuffer.compact();
                // unwrap input buffer: write mode
                // unwrap output buffer: write mode

                switch (result.getStatus()) {
                    case BUFFER_UNDERFLOW:
                        // we need more data to unwrap, let's return whatever we have
                        return plainTextReadBytes;
                    case BUFFER_OVERFLOW:
                        if (unwrapOutputBuffer.position() == 0) {
                            // not even a single byte was written to the output buffer even the buffer is empty
                            // apparently the output buffer cannot fit even a single TLS record. let's grow it and try again!
                            throw new AssertionError("output buffer to small");
                        }
                        return plainTextReadBytes;
                    case OK:
                        if (result.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.NEED_TASK || result.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.NEED_WRAP) {
                            return plainTextReadBytes;
                        }
                        break;
                    case CLOSED:
                        throw new HttpClientException("server closed connection unexpectedly");
                }
            }
        } catch (SSLException e) {
            throw new HttpClientException("could not unwrap SSL packet", e);
        }
    }

    @Override
    public int send(long bufferPtr, int bufferLen) {
        try {
            resetBufferToPointer(wrapInputBuffer, bufferPtr, bufferLen);
            wrapInputBuffer.position(0);
            // wrapOutputBuffer: write mode

            int plainBytesConsumed = 0;
            for (; ; ) {

                // try to send whatever we have in the encrypted buffer
                int wrapOutputBufferPos = wrapOutputBuffer.position();
                if (wrapOutputBufferPos > 0) {
                    // there are some bytes in the output buffer, let's try to send them
                    wrapOutputBuffer.flip();
                    int limit = wrapOutputBuffer.limit();
                    int n = delegate.send(wrapOutputBufferPtr, limit);
                    if (n < 0) {
                        // ops, something went wrong
                        return n;
                    }
                    wrapOutputBuffer.position(n);
                    wrapOutputBuffer.compact();
                    if (n == 0) {
                        // we didn't manage to send anything, the socket is full, no point in trying to send more
                        return plainBytesConsumed;
                    }
                }

                SSLEngineResult result = sslEngine.wrap(wrapInputBuffer, wrapOutputBuffer);
                plainBytesConsumed += result.bytesConsumed();
                switch (result.getStatus()) {
                    case BUFFER_UNDERFLOW:
                        throw new AssertionError("should not happen, report as bug");
                    case BUFFER_OVERFLOW:
                        if (wrapOutputBuffer.position() == 0) {
                            // not even a single byte was written to the output buffer even the buffer is empty
                            // apparently the output buffer cannot fit even a single TLS record. let's grow it and try again!
                            growWrapOutputBuffer();
                            break;
                        }
                        // wrapOutputBuffer: write mode
                        break;
                    case OK:
                        if (wrapInputBuffer.remaining() == 0) {
                            // all sent, nothing left to be done.
                            return plainBytesConsumed;
                        }
                        break;
                    case CLOSED:
                        if (state != STATE_CLOSING) {
                            throw new HttpClientException("server closed connection unexpectedly");
                        }
                        return -1;
                }
            }
        } catch (SSLException e) {
            throw new HttpClientException("error while sending data to questdb server", e);
        }
    }

    @Override
    public int shutdown(int how) {
        return 0;
    }

    @Override
    public int startTlsSession() {
        try {
            this.sslEngine = createSslEngine();
            this.sslEngine.beginHandshake();
            SSLEngineResult.HandshakeStatus handshakeStatus;
            while ((handshakeStatus = sslEngine.getHandshakeStatus()) != SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING) {
                switch (handshakeStatus) {
                    case NEED_TASK:
                        Runnable task;
                        while ((task = sslEngine.getDelegatedTask()) != null) {
                            task.run();
                        }
                        break;
                    case NEED_WRAP:
                        SSLEngineResult result = sslEngine.wrap(wrapInputBuffer, wrapOutputBuffer);
                        switch (result.getStatus()) {
                            case BUFFER_UNDERFLOW:
                            case BUFFER_OVERFLOW:
                                throw new AssertionError("should not happen, report as bug");
                            case OK:
                                // wrapOutputBuffer: write mode
                                int written = 0;
                                int bufferLimit = wrapOutputBuffer.position();
                                while (written < bufferLimit) {
                                    int n = delegate.send(wrapOutputBufferPtr + written, bufferLimit - written);
                                    if (n < 0) {
                                        return n;
                                    }
                                    written += n;
                                }
                                wrapOutputBuffer.clear();
                                break;
                            case CLOSED:
                                log.error().$("server closed connection unexpectedly").$();
                                return -1;
                        }
                        break;
                    case NEED_UNWRAP:
                        int n = recv(0, 0);
                        if (n < 0) {
                            return n;
                        }
                        break;
                    case FINISHED:
                        break;
                }
            }
            state = STATE_HANDSHAKE_COMPLETED;
            return 0;
        } catch (Exception e) {
            log.error().$("could not start SSL session").$(e).$();
            return -1;
        }
    }

    @Override
    public boolean supportsTls() {
        return true;
    }

    @Override
    public int tlsIO(int readinessFlags) {
        if ((readinessFlags & WRITE_FLAG) != 0) {
            int wrapOutputBufferPos = wrapOutputBuffer.position();
            if (wrapOutputBufferPos > 0) {
                // there are some encrypted bytes in the output buffer, let's try to send them
                wrapOutputBuffer.flip();
                int limit = wrapOutputBuffer.limit();
                int sentBytes = delegate.send(wrapOutputBufferPtr, limit);
                if (sentBytes < 0) {
                    return -1;
                }
                wrapOutputBuffer.position(sentBytes);
                wrapOutputBuffer.compact();
            }
        }
        return 0;
    }

    @Override
    public boolean wantsTlsRead() {
        return false;
    }

    @Override
    public boolean wantsTlsWrite() {
        return wrapOutputBuffer.position() > 0;
    }

    private static long allocateMemoryAndResetBuffer(ByteBuffer buffer, int capacity) {
        // TODO: what is the right tag here?
        long newAddress = Unsafe.malloc(capacity, MemoryTag.NATIVE_TLS_RSS);
        resetBufferToPointer(buffer, newAddress, capacity);
        return newAddress;
    }

    private static SSLEngine createSslEngine() {
        // todo: make configurable: CAs, peer advisory, etc.

        SSLContext sslContext;
        try {
            sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, BLIND_TRUST_MANAGERS, new SecureRandom());
        } catch (NoSuchAlgorithmException | KeyManagementException e) {
            throw new RuntimeException(e);
        }
        SSLEngine sslEngine = sslContext.createSSLEngine();
        sslEngine.setUseClientMode(true);
        return sslEngine;
    }

    private static long expandBuffer(ByteBuffer buffer, long oldAddress) {
        int oldCapacity = buffer.capacity();
        int newCapacity = oldCapacity * 2;
        long newAddress = Unsafe.realloc(oldAddress, oldCapacity, newCapacity, MemoryTag.NATIVE_TLS_RSS);
        resetBufferToPointer(buffer, newAddress, newCapacity);
        return newAddress;
    }

    private static void resetBufferToPointer(ByteBuffer buffer, long ptr, int len) {
        assert buffer.isDirect();
        Unsafe.getUnsafe().putLong(buffer, ADDRESS_FIELD_OFFSET, ptr);
        Unsafe.getUnsafe().putLong(buffer, LIMIT_FIELD_OFFSET, len);
        Unsafe.getUnsafe().putLong(buffer, CAPACITY_FIELD_OFFSET, len);
    }

    private void growWrapOutputBuffer() {
        wrapOutputBufferPtr = expandBuffer(wrapOutputBuffer, wrapOutputBufferPtr);
    }

    private int readFromUpstream() {
        assert unwrapInputBuffer.limit() == unwrapInputBuffer.capacity();
        int remainingLen = unwrapInputBuffer.remaining();
        if (remainingLen == 0) {
            // no point in reading if we have no space left
            return 0;
        }

        assert Unsafe.getUnsafe().getLong(unwrapInputBuffer, ADDRESS_FIELD_OFFSET) == unwrapInputBufferPtr;
        long adjustedPtr = unwrapInputBufferPtr + unwrapInputBuffer.position();

        int n = delegate.recv(adjustedPtr, remainingLen);
        if (n < 0) {
            return n;
        }
        unwrapInputBuffer.position(unwrapInputBuffer.position() + n);
        return n;
    }

//    private int unwrapOutputBufferToPtr(long dstPtr, int dstLen) {
//        // assume unwrapOutputBuffer is in read mode
//
//        int oldPosition = unwrapOutputBuffer.position();
//
//        assert Unsafe.getUnsafe().getLong(unwrapOutputBufferPtr, ADDRESS_FIELD_OFFSET) == unwrapOutputBufferPtr;
//        long srcPtr = unwrapOutputBufferPtr + oldPosition;
//        int srcLen = unwrapOutputBuffer.remaining();
//        int len = Math.min(dstLen, srcLen);
//        Vect.memcpy(dstPtr, srcPtr, len);
//        unwrapOutputBuffer.position(oldPosition + len);
//        return len;
//    }

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
