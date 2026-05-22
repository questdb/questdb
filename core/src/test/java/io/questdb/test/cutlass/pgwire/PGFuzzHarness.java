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

package io.questdb.test.cutlass.pgwire;

import io.questdb.DefaultFactoryProvider;
import io.questdb.FactoryProvider;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.cutlass.auth.AuthenticatorException;
import io.questdb.cutlass.auth.SocketAuthenticator;
import io.questdb.cutlass.pgwire.DefaultPGAuthenticatorFactory;
import io.questdb.cutlass.pgwire.DefaultPGCircuitBreakerRegistry;
import io.questdb.cutlass.pgwire.DefaultPGConfiguration;
import io.questdb.cutlass.pgwire.PGAuthenticatorFactory;
import io.questdb.cutlass.pgwire.PGCircuitBreakerRegistry;
import io.questdb.cutlass.pgwire.PGConfiguration;
import io.questdb.cutlass.pgwire.PGConnectionContext;
import io.questdb.cutlass.pgwire.PGMessageProcessingException;
import io.questdb.cutlass.pgwire.TypesAndSelect;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.Log;
import io.questdb.network.IOOperation;
import io.questdb.network.NetworkFacade;
import io.questdb.network.PlainSocket;
import io.questdb.network.Socket;
import io.questdb.network.SocketFactory;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.NoOpAssociativeCache;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;

final class PGFuzzHarness implements Closeable {
    static final int INPUT_BUFFER_SIZE = 1 << 20;

    private static final char[] HEX = "0123456789abcdef".toCharArray();
    private static final int MAX_FAILURE_HEX_BYTES = 4096;

    private final PGConnectionContext context;
    private final SocketAuthenticator authenticator;
    private final boolean assumeAuthenticated;
    private final PGCircuitBreakerRegistry registry;
    private final FuzzSocketFactory socketFactory = new FuzzSocketFactory();
    private long inputBuffer;

    PGFuzzHarness(CairoEngine engine) throws Exception {
        this(engine, true);
    }

    PGFuzzHarness(CairoEngine engine, boolean assumeAuthenticated) throws Exception {
        this(engine, assumeAuthenticated ? AuthMode.FUZZ_AUTHENTICATED : AuthMode.FUZZ_STARTUP);
    }

    static PGFuzzHarness newCleartextAuth(CairoEngine engine) throws Exception {
        return new PGFuzzHarness(engine, AuthMode.CLEARTEXT);
    }

    private PGFuzzHarness(CairoEngine engine, AuthMode authMode) throws Exception {
        final FactoryProvider factoryProvider = new FuzzFactoryProvider(socketFactory, authMode);
        final PGConfiguration configuration = new DefaultPGConfiguration() {
            @Override
            public FactoryProvider getFactoryProvider() {
                return factoryProvider;
            }

            @Override
            public boolean isSelectCacheEnabled() {
                return false;
            }
        };

        PGConnectionContext context = null;
        SocketAuthenticator authenticator = null;
        NetworkSqlExecutionCircuitBreaker circuitBreaker = null;
        PGCircuitBreakerRegistry registry = null;
        boolean authenticatorOwnsCircuitBreaker = false;
        long inputBuffer = 0;
        try {
            circuitBreaker = new NetworkSqlExecutionCircuitBreaker(
                    engine,
                    configuration.getCircuitBreakerConfiguration(),
                    MemoryTag.NATIVE_CB5
            );
            context = new PGConnectionContext(
                    engine,
                    configuration,
                    new SqlExecutionContextImpl(engine, 1),
                    circuitBreaker,
                    new NoOpAssociativeCache<TypesAndSelect>()
            );
            if (authMode == AuthMode.CLEARTEXT) {
                registry = new DefaultPGCircuitBreakerRegistry(configuration, engine.getConfiguration());
            }
            authenticator = factoryProvider.getPgWireAuthenticatorFactory().getPgWireAuthenticator(
                    configuration,
                    circuitBreaker,
                    registry,
                    context
            );
            context.setAuthenticator(authenticator);
            authenticatorOwnsCircuitBreaker = true;
            context.init();
            if (authMode == AuthMode.FUZZ_AUTHENTICATED) {
                ((FuzzAuthenticator) authenticator).assumeAuthenticatedForFuzz();
                context.assumeAuthenticatedForFuzz(AllowAllSecurityContext.INSTANCE);
            }
            inputBuffer = Unsafe.malloc(INPUT_BUFFER_SIZE, MemoryTag.NATIVE_PGW_CONN);

            this.context = context;
            this.authenticator = authenticator;
            this.assumeAuthenticated = authMode == AuthMode.FUZZ_AUTHENTICATED;
            this.registry = registry;
            this.inputBuffer = inputBuffer;
        } catch (Throwable th) {
            if (inputBuffer != 0) {
                Unsafe.free(inputBuffer, INPUT_BUFFER_SIZE, MemoryTag.NATIVE_PGW_CONN);
            }
            Misc.free(context);
            if (!authenticatorOwnsCircuitBreaker) {
                Misc.free(circuitBreaker);
            }
            if (registry != null) {
                registry.close();
            }
            throw th;
        }
    }

    void acceptStartupForFuzz(byte[] input) throws Exception {
        socketFactory.setInput(input);
        try {
            context.handleClientOperation(IOOperation.READ);
        } finally {
            socketFactory.clearInput();
        }
    }

    PGConnectionContext context() {
        return context;
    }

    void assertOutputFramesWellFormed() {
        assertOutputFramesWellFormed(false);
    }

    void assertOutputFramesWellFormed(boolean allowNegotiationResponses) {
        int p = 0;
        final int n = socketFactory.getOutputSize();
        while (p < n) {
            if (allowNegotiationResponses && socketFactory.getOutputByte(p) == 'N' && !isCompleteFrameAt(p, n)) {
                p++;
                continue;
            }
            if (n - p < 5) {
                throw new AssertionError("truncated pgwire response frame header [offset=" + p + ", outputSize=" + n + ']');
            }
            final int len = socketFactory.getOutputInt(p + 1);
            if (len < Integer.BYTES) {
                throw new AssertionError("invalid pgwire response frame length [offset=" + p + ", len=" + len + ']');
            }
            final int frameLen = 1 + len;
            if (frameLen > n - p) {
                throw new AssertionError("truncated pgwire response frame [offset=" + p + ", len=" + len + ", outputSize=" + n + ']');
            }
            p += frameLen;
        }
    }

    private boolean isCompleteFrameAt(int offset, int outputSize) {
        if (outputSize - offset < 5) {
            return false;
        }
        final int len = socketFactory.getOutputInt(offset + 1);
        return len >= Integer.BYTES && 1 + len <= outputSize - offset;
    }

    void assertPipelinePoolBalanced() {
        final int outieCount = context.getPipelineEntryPoolOutieCountForFuzz();
        if (outieCount != 0) {
            throw new AssertionError("unreleased pgwire pipeline entries [outieCount=" + outieCount + ']');
        }
    }

    int countOutputFrames(byte type) {
        int count = 0;
        int p = 0;
        final int n = socketFactory.getOutputSize();
        while (p < n) {
            final int len = socketFactory.getOutputInt(p + 1);
            if (socketFactory.getOutputByte(p) == (type & 0xff)) {
                count++;
            }
            p += 1 + len;
        }
        return count;
    }

    String outputFrameTypes() {
        final StringBuilder types = new StringBuilder();
        int p = 0;
        final int n = socketFactory.getOutputSize();
        while (p < n) {
            final int len = socketFactory.getOutputInt(p + 1);
            types.append((char) socketFactory.getOutputByte(p));
            p += 1 + len;
        }
        return types.toString();
    }

    void copyInput(byte[] input) {
        if (input.length > INPUT_BUFFER_SIZE) {
            throw new IllegalArgumentException("input too large");
        }
        Unsafe.copyMemory(input, Unsafe.BYTE_OFFSET, null, inputBuffer, input.length);
    }

    void copyFrame(byte type, byte[] body, int bodyOffset, int bodyLength) {
        if (bodyLength > INPUT_BUFFER_SIZE - 5) {
            throw new IllegalArgumentException("frame too large");
        }
        Unsafe.getUnsafe().putByte(inputBuffer, type);
        PGConnectionContext.putInt(inputBuffer + 1, bodyLength + Integer.BYTES);
        if (bodyLength > 0) {
            Unsafe.copyMemory(body, Unsafe.BYTE_OFFSET + bodyOffset, null, inputBuffer + 5, bodyLength);
        }
    }

    long inputBuffer() {
        return inputBuffer;
    }

    void reset() {
        socketFactory.reset();
        context.resetForFuzz();
        context.resetAuthenticatorForFuzz();
        if (assumeAuthenticated) {
            ((FuzzAuthenticator) authenticator).assumeAuthenticatedForFuzz();
            context.assumeAuthenticatedForFuzz(AllowAllSecurityContext.INSTANCE);
        }
    }

    void rethrowUnexpectedProcessingError(PGMessageProcessingException ex, String target, byte[] input) {
        rethrowUnexpectedProcessingError(ex, target, input, -1, (byte) 0, -1, -1, null);
    }

    void rethrowUnexpectedProcessingError(
            PGMessageProcessingException ex,
            String target,
            byte[] input,
            int step,
            byte type,
            int bodyOffset,
            int bodyLength
    ) {
        rethrowUnexpectedProcessingError(ex, target, input, step, type, bodyOffset, bodyLength, null);
    }

    void rethrowUnexpectedProcessingError(
            PGMessageProcessingException ex,
            String target,
            byte[] input,
            int step,
            byte type,
            int bodyOffset,
            int bodyLength,
            CharSequence extraDiagnostics
    ) {
        final Throwable cause = ex.getFlyweightCause();
        if (!(cause instanceof Error) && (!(cause instanceof RuntimeException) || isExpectedSqlRuntimeException(cause))) {
            return;
        }

        StringBuilder message = new StringBuilder();
        message.append("unexpected throwable hidden inside PGMessageProcessingException [target=").append(target);
        message.append(", cause=").append(cause.getClass().getName());
        if (cause.getMessage() != null) {
            message.append(": ").append(cause.getMessage());
        }
        message.append(", pgMessage=").append(ex.getFlyweightMessage());
        message.append(", inputLength=").append(input.length);
        if (step > -1) {
            message.append(", step=").append(step);
            final int typeByte = type & 0xff;
            message.append(", typeByte=").append(typeByte);
            if (typeByte >= 32 && typeByte < 127) {
                message.append(" ('").append((char) typeByte).append("')");
            }
            message.append(", bodyOffset=").append(bodyOffset);
            message.append(", bodyLength=").append(bodyLength);
            message.append(", bodyHex=");
            appendHex(message, input, bodyOffset, bodyLength);
        }
        message.append(", inputHex=");
        appendHex(message, input, 0, input.length);
        if (extraDiagnostics != null && extraDiagnostics.length() > 0) {
            message.append(", ").append(extraDiagnostics);
        }
        message.append(']');
        throw new AssertionError(message.toString(), cause);
    }

    private static boolean isExpectedSqlRuntimeException(Throwable cause) {
        return cause instanceof CairoException && !((CairoException) cause).isCritical() ||
                cause instanceof ImplicitCastException ||
                cause instanceof TableReferenceOutOfDateException;
    }

    @Override
    public void close() {
        Misc.free(context);
        if (inputBuffer != 0) {
            inputBuffer = Unsafe.free(inputBuffer, INPUT_BUFFER_SIZE, MemoryTag.NATIVE_PGW_CONN);
        }
        if (registry != null) {
            registry.close();
        }
    }

    private static void appendHex(StringBuilder sink, byte[] bytes, int offset, int length) {
        if (offset < 0 || length < 0 || offset > bytes.length) {
            sink.append("<invalid>");
            return;
        }

        final int hi = Math.min(bytes.length, offset + Math.min(length, MAX_FAILURE_HEX_BYTES));
        for (int i = offset; i < hi; i++) {
            int value = bytes[i] & 0xff;
            sink.append(HEX[value >>> 4]);
            sink.append(HEX[value & 0x0f]);
        }
        if (length > MAX_FAILURE_HEX_BYTES) {
            sink.append("...");
        }
    }

    private enum AuthMode {
        CLEARTEXT,
        FUZZ_AUTHENTICATED,
        FUZZ_STARTUP
    }

    private static final class FuzzAuthenticator implements SocketAuthenticator {
        private NetworkSqlExecutionCircuitBreaker circuitBreaker;
        private Socket socket;
        private long recvBuffer;
        private long recvBufferLimit;
        private long recvBufferPos;
        private long recvBufferPseudoStart;
        private boolean authenticated;

        private FuzzAuthenticator(NetworkSqlExecutionCircuitBreaker circuitBreaker) {
            this.circuitBreaker = circuitBreaker;
        }

        @Override
        public void clear() {
            if (circuitBreaker != null) {
                circuitBreaker.clear();
            }
        }

        @Override
        public void close() {
            circuitBreaker = Misc.free(circuitBreaker);
        }

        @Override
        public CharSequence getPrincipal() {
            return "fuzz";
        }

        @Override
        public long getRecvBufPos() {
            return recvBufferPos;
        }

        @Override
        public long getRecvBufPseudoStart() {
            return recvBufferPseudoStart;
        }

        @Override
        public int handleIO() {
            final int n = socket.recv(recvBuffer, (int) Math.min(Integer.MAX_VALUE, recvBufferLimit - recvBuffer));
            if (n < 0) {
                return NEEDS_DISCONNECT;
            }
            if (n == 0) {
                return NEEDS_READ;
            }
            recvBufferPos = recvBuffer + n;
            recvBufferPseudoStart = recvBuffer + n;
            if (n >= Integer.BYTES) {
                final int startupLen = getInt(recvBuffer);
                if (startupLen >= 2 * Integer.BYTES && startupLen <= n) {
                    recvBufferPseudoStart = recvBuffer + startupLen;
                }
            }
            return OK;
        }

        @Override
        public void init(@NotNull Socket socket, long recvBuffer, long recvBufferLimit, long sendBuffer, long sendBufferLimit) {
            this.socket = socket;
            this.recvBuffer = recvBuffer;
            this.recvBufferLimit = recvBufferLimit;
            resetForFuzz();
        }

        @Override
        public boolean isAuthenticated() {
            return authenticated;
        }

        @Override
        public int loginOK() throws AuthenticatorException {
            authenticated = true;
            return OK;
        }

        private void assumeAuthenticatedForFuzz() {
            authenticated = true;
            recvBufferPos = recvBuffer;
            recvBufferPseudoStart = recvBuffer;
        }

        private static int getInt(long address) {
            return ((Unsafe.getUnsafe().getByte(address) & 0xff) << 24)
                    | ((Unsafe.getUnsafe().getByte(address + 1) & 0xff) << 16)
                    | ((Unsafe.getUnsafe().getByte(address + 2) & 0xff) << 8)
                    | (Unsafe.getUnsafe().getByte(address + 3) & 0xff);
        }

        private void resetForFuzz() {
            authenticated = false;
            recvBufferPos = recvBuffer;
            recvBufferPseudoStart = recvBuffer;
        }
    }

    private static final class FuzzFactoryProvider extends DefaultFactoryProvider {
        private final AuthMode authMode;
        private final FuzzSocketFactory socketFactory;

        private FuzzFactoryProvider(FuzzSocketFactory socketFactory, AuthMode authMode) {
            this.authMode = authMode;
            this.socketFactory = socketFactory;
        }

        @Override
        public @NotNull SocketFactory getPGWireSocketFactory() {
            return socketFactory;
        }

        @Override
        public @NotNull PGAuthenticatorFactory getPgWireAuthenticatorFactory() {
            if (authMode == AuthMode.CLEARTEXT) {
                return new DefaultPGAuthenticatorFactory(null);
            }
            return (configuration, circuitBreaker, registry, optionsListener) -> new FuzzAuthenticator(circuitBreaker);
        }
    }

    private static final class FuzzSocket extends PlainSocket {
        private static final int SEND_CAPTURE_SIZE = 1 << 20;

        private final byte[] sendBuffer = new byte[SEND_CAPTURE_SIZE];
        private byte[] recvBuffer;
        private int recvOffset;
        private int sendSize;

        private FuzzSocket(NetworkFacade nf, Log log) {
            super(nf, log);
        }

        @Override
        public void close() {
            reset();
            super.close();
        }

        @Override
        public int recv(long bufferPtr, int bufferLen) {
            if (recvBuffer == null || recvOffset == recvBuffer.length) {
                return 0;
            }
            final int n = Math.min(bufferLen, recvBuffer.length - recvOffset);
            Unsafe.copyMemory(recvBuffer, Unsafe.BYTE_OFFSET + recvOffset, null, bufferPtr, n);
            recvOffset += n;
            return n;
        }

        @Override
        public int send(long bufferPtr, int bufferLen) {
            final int n = Math.min(bufferLen, sendBuffer.length - sendSize);
            if (n > 0) {
                Unsafe.copyMemory(null, bufferPtr, sendBuffer, Unsafe.BYTE_OFFSET + sendSize, n);
                sendSize += n;
            }
            return n;
        }

        private void reset() {
            recvBuffer = null;
            recvOffset = 0;
            sendSize = 0;
        }

        private void clearInput() {
            recvBuffer = null;
            recvOffset = 0;
        }

        private int getOutputInt(int offset) {
            return ((sendBuffer[offset] & 0xff) << 24)
                    | ((sendBuffer[offset + 1] & 0xff) << 16)
                    | ((sendBuffer[offset + 2] & 0xff) << 8)
                    | (sendBuffer[offset + 3] & 0xff);
        }

        private int getOutputByte(int offset) {
            return sendBuffer[offset] & 0xff;
        }

        private int getOutputSize() {
            return sendSize;
        }

        private void setInput(byte[] input) {
            recvBuffer = input;
            recvOffset = 0;
        }
    }

    private static final class FuzzSocketFactory implements SocketFactory {
        private FuzzSocket socket;

        @Override
        public Socket newInstance(NetworkFacade nf, Log log) {
            return socket = new FuzzSocket(nf, log);
        }

        private void reset() {
            if (socket != null) {
                socket.reset();
            }
        }

        private void clearInput() {
            if (socket != null) {
                socket.clearInput();
            }
        }

        private int getOutputInt(int offset) {
            return socket.getOutputInt(offset);
        }

        private int getOutputByte(int offset) {
            return socket.getOutputByte(offset);
        }

        private int getOutputSize() {
            return socket == null ? 0 : socket.getOutputSize();
        }

        private void setInput(byte[] input) {
            socket.setInput(input);
        }
    }
}
