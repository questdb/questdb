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
import io.questdb.std.Vect;

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
        unwrapInputBuffer.flip(); // read mode
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
        // unwrap input buffer: read mode and empty
        unwrapInputBuffer.position(0);
        unwrapInputBuffer.limit(0);

        // write mode and empty
        unwrapOutputBuffer.clear();
        wrapOutputBuffer.clear();
    }

    @Override
    public int recv(long bufferPtr, int bufferLen) {
        assert sslEngine != null;

        resetBufferToPointer(unwrapOutputBuffer, bufferPtr, bufferLen);
        unwrapOutputBuffer.position(0);


        try {
            int bytesProduced = 0;
            for (; ; ) {
                int n = readFromSocket();
                if (n < 0) {
                    return n;
                }

                assert unwrapInputBuffer.position() == 0 : "missing unwrapInputBuffer compact call";
                int bytesToConsume = unwrapInputBuffer.limit();
                if (bytesToConsume == 0) {
                    // nothing to unwrap, we are done
                    return bytesProduced;
                }

                SSLEngineResult result = sslEngine.unwrap(unwrapInputBuffer, unwrapOutputBuffer);
                bytesProduced += result.bytesProduced();

                // compact the buffer
                int bytesConsumed = result.bytesConsumed();
                int bytesRemaining = bytesToConsume - bytesConsumed;
                Vect.memcpy(unwrapInputBufferPtr, unwrapInputBufferPtr + bytesConsumed, bytesRemaining);
                unwrapInputBuffer.position(0);
                unwrapInputBuffer.limit(bytesRemaining);


                switch (result.getStatus()) {
                    case BUFFER_UNDERFLOW:
                        // we need more data to unwrap, let's return whatever we have
                        return bytesProduced;
                    case BUFFER_OVERFLOW:
                        if (unwrapOutputBuffer.position() == 0) {
                            throw new AssertionError("output buffer to small");
                        }
                        return bytesProduced;
                    case OK:
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
                int bytesToSend = wrapOutputBuffer.position();
                if (bytesToSend > 0) {
                    int sent = writeToSocket(bytesToSend);
                    if (sent < 0) {
                        return sent;
                    } else if (sent == 0) {
                        // we didn't manage to send anything, the network socket is full, no point in trying to send more
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
        return delegate.shutdown(how);
    }

    @Override
    public int startTlsSession() {
        try {
            this.sslEngine = createSslEngine();
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
                                throw new AssertionError("buffer underflow during handshaking, this should not happen. please report as a bug");
                            case BUFFER_OVERFLOW:
                                throw new AssertionError("buffer overflow during handshaking, this should not happen, please report as a bug");
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
                    }
                    case NEED_UNWRAP: {
                        int n = readFromSocket();
                        if (n < 0) {
                            return n;
                        }
                        SSLEngineResult result = sslEngine.unwrap(unwrapInputBuffer, unwrapOutputBuffer);
                        handshakeStatus = result.getHandshakeStatus();
                        switch (result.getStatus()) {
                            case BUFFER_UNDERFLOW:
                                // we need to receive more data from a socket, let's try again
                                break;
                            case BUFFER_OVERFLOW:
                                throw new AssertionError("buffer overflow, this should not happen, please report as a bug");
                            case OK:
                                // good, let's see what we need to do next
                                break;
                            case CLOSED:
                                log.error().$("server closed connection unexpectedly").$();
                                return -1;
                        }
                    }
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
        return wrapOutputBuffer.position() > 0;
    }

    private static long allocateMemoryAndResetBuffer(ByteBuffer buffer, int capacity) {
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
        Vect.memcpy(wrapOutputBufferPtr, wrapOutputBufferPtr + n, bytesRemaining);
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
