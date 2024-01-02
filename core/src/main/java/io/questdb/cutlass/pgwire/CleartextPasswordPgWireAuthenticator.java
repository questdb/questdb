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

package io.questdb.cutlass.pgwire;


import io.questdb.cairo.CairoException;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cutlass.auth.Authenticator;
import io.questdb.cutlass.auth.AuthenticatorException;
import io.questdb.griffin.CharacterStore;
import io.questdb.griffin.CharacterStoreEntry;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.NoSpaceLeftInResponseBufferException;
import io.questdb.network.Socket;
import io.questdb.std.*;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8Sink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class CleartextPasswordPgWireAuthenticator implements Authenticator {
    public static final char STATUS_IDLE = 'I';
    private static final int INIT_CANCEL_REQUEST = 80877102;
    private static final int INIT_GSS_REQUEST = 80877104;
    private static final int INIT_SSL_REQUEST = 80877103;
    private static final int INIT_STARTUP_MESSAGE = 196608;
    private static final Log LOG = LogFactory.getLog(CleartextPasswordPgWireAuthenticator.class);
    private static final byte MESSAGE_TYPE_ERROR_RESPONSE = 'E';
    private static final byte MESSAGE_TYPE_LOGIN_RESPONSE = 'R';
    private static final byte MESSAGE_TYPE_PARAMETER_STATUS = 'S';
    private static final byte MESSAGE_TYPE_PASSWORD_MESSAGE = 'p';
    private static final byte MESSAGE_TYPE_READY_FOR_QUERY = 'Z';
    private final CharacterStore characterStore;
    private final NetworkSqlExecutionCircuitBreaker circuitBreaker;
    private final int circuitBreakerId;
    private final DirectUtf8String dus = new DirectUtf8String();
    private final boolean matcherOwned;
    private final OptionsListener optionsListener;
    private final CircuitBreakerRegistry registry;
    private final String serverVersion;
    private final ResponseSink sink;
    private UsernamePasswordMatcher matcher;
    private long recvBufEnd;
    private long recvBufReadPos;
    private long recvBufStart;
    private long recvBufWritePos;
    private long sendBufEnd;
    private long sendBufReadPos;
    private long sendBufStart;
    private long sendBufWritePos;
    private Socket socket;
    private State state = State.EXPECT_INIT_MESSAGE;
    private CharSequence username;

    public CleartextPasswordPgWireAuthenticator(
            PGWireConfiguration configuration,
            NetworkSqlExecutionCircuitBreaker circuitBreaker,
            CircuitBreakerRegistry registry,
            OptionsListener optionsListener,
            UsernamePasswordMatcher matcher,
            boolean matcherOwned
    ) {
        this.matcher = matcher;
        this.matcherOwned = matcherOwned;
        this.characterStore = new CharacterStore(
                configuration.getCharacterStoreCapacity(),
                configuration.getCharacterStorePoolCapacity()
        );
        this.circuitBreakerId = registry.add(circuitBreaker);
        this.registry = registry;
        this.sink = new ResponseSink();
        this.serverVersion = configuration.getServerVersion();
        this.circuitBreaker = circuitBreaker;
        this.optionsListener = optionsListener;
    }

    @Override
    public void clear() {
        circuitBreaker.setSecret(-1);
        circuitBreaker.resetMaxTimeToDefault();
        circuitBreaker.unsetTimer();
    }

    @Override
    public void close() {
        registry.remove(circuitBreakerId);
        Misc.free(circuitBreaker);
        if (matcherOwned) {
            matcher = Misc.freeIfCloseable(matcher);
        }
    }

    @Override
    public int denyAccess(CharSequence message) throws AuthenticatorException {
        prepareErrorResponse(message);
        state = State.WRITE_AND_AUTH_FAILURE;
        return handleIO();
    }

    @Override
    public byte getAuthType() {
        return SecurityContext.AUTH_TYPE_CREDENTIALS;
    }

    public CharSequence getPrincipal() {
        return username;
    }

    public long getRecvBufPos() {
        // where to start appending new data from socket
        return recvBufWritePos;
    }

    public long getRecvBufPseudoStart() {
        // where to start reading data from
        return recvBufReadPos;
    }

    public int handleIO() throws AuthenticatorException {
        try {
            for (; ; ) {
                switch (state) {
                    case EXPECT_INIT_MESSAGE: {
                        int r = readFromSocket();
                        if (r != Authenticator.OK) {
                            return r;
                        }
                        r = processInitMessage();
                        if (r != Authenticator.OK) {
                            return r;
                        }
                        break;
                    }
                    case EXPECT_PASSWORD_MESSAGE: {
                        int r = readFromSocket();
                        if (r != Authenticator.OK) {
                            return r;
                        }
                        r = processPasswordMessage();
                        if (r != Authenticator.OK) {
                            return r;
                        }
                        break;
                    }
                    case WRITE_AND_EXPECT_PASSWORD_MESSAGE: {
                        int r = writeToSocketAndAdvance(State.EXPECT_PASSWORD_MESSAGE);
                        if (r != Authenticator.OK) {
                            return r;
                        }
                        break;
                    }
                    case WRITE_AND_EXPECT_INIT_MESSAGE: {
                        int r = writeToSocketAndAdvance(State.EXPECT_INIT_MESSAGE);
                        if (r != Authenticator.OK) {
                            return r;
                        }
                        break;
                    }
                    case WRITE_AND_AUTH_SUCCESS: {
                        int r = writeToSocketAndAdvance(State.AUTH_SUCCESS);
                        if (r != Authenticator.OK) {
                            return r;
                        }
                        break;
                    }
                    case WRITE_AND_AUTH_FAILURE: {
                        int r = writeToSocketAndAdvance(State.AUTH_FAILED);
                        if (r != Authenticator.OK) {
                            return r;
                        }
                        break;
                    }
                    case AUTH_SUCCESS:
                        circuitBreaker.of(socket.getFd());
                        return Authenticator.OK;
                    case AUTH_FAILED:
                        return Authenticator.NEEDS_DISCONNECT;
                    default:
                        assert false;
                }
            }
        } catch (BadProtocolException e) {
            throw AuthenticatorException.INSTANCE;
        }
    }

    @Override
    public void init(@NotNull Socket socket, long recvBuffer, long recvBufferLimit, long sendBuffer, long sendBufferLimit) {
        this.circuitBreaker.setSecret(registry.getNewSecret());
        this.state = State.EXPECT_INIT_MESSAGE;
        this.username = null;
        this.socket = socket;
        this.recvBufStart = recvBuffer;
        this.recvBufReadPos = recvBuffer;
        this.recvBufWritePos = recvBuffer;
        this.recvBufEnd = recvBufferLimit;

        this.sendBufStart = sendBuffer;
        this.sendBufReadPos = sendBuffer;
        this.sendBufWritePos = sendBuffer;
        this.sendBufEnd = sendBufferLimit;
    }

    public boolean isAuthenticated() {
        return state == State.AUTH_SUCCESS;
    }

    @Override
    public int loginOK() throws AuthenticatorException {
        compactRecvBuf();
        prepareLoginOk();
        state = State.WRITE_AND_AUTH_SUCCESS;
        return handleIO();
    }

    private static int getIntUnsafe(long address) {
        return Numbers.bswap(Unsafe.getUnsafe().getInt(address));
    }

    private int availableToRead() {
        return (int) (recvBufWritePos - recvBufReadPos);
    }

    private void compactRecvBuf() {
        long len = recvBufWritePos - recvBufReadPos;
        if (len > 0) {
            Vect.memcpy(recvBufStart, recvBufReadPos, len);
        }
        recvBufReadPos = recvBufStart;
        recvBufWritePos = recvBufStart + len;
    }

    private void compactSendBuf() {
        long len = sendBufWritePos - sendBufReadPos;
        if (len > 0) {
            Vect.memcpy(sendBufStart, sendBufReadPos, len);
        }
        sendBufReadPos = sendBufStart;
        sendBufWritePos = sendBufStart + len;
    }

    private void ensureCapacity(long capacity) {
        if (sendBufWritePos + capacity > sendBufEnd) {
            throw NoSpaceLeftInResponseBufferException.INSTANCE;
        }
    }

    private void prepareBackendKeyData(ResponseSink responseSink) {
        responseSink.put('K');
        responseSink.putInt(Integer.BYTES * 3); // length of this message
        responseSink.putInt(circuitBreakerId);
        responseSink.putInt(circuitBreaker.getSecret());
    }

    private void prepareErrorResponse(CharSequence errorMessage) {
        sink.put(MESSAGE_TYPE_ERROR_RESPONSE);
        long addr = sink.skip();
        sink.put('C');
        sink.encodeUtf8Z("00000");
        sink.put('M');
        sink.encodeUtf8Z(errorMessage);
        sink.put('S');
        sink.encodeUtf8Z("ERROR");
        sink.put((char) 0);
        sink.putLen(addr);
    }

    private void prepareGssResponse() {
        sink.put('N');
    }

    private void prepareLoginOk() {
        sink.put(MESSAGE_TYPE_LOGIN_RESPONSE);
        sink.putInt(Integer.BYTES * 2);
        sink.putInt(0);
        prepareParams(sink, "TimeZone", "GMT");
        prepareParams(sink, "application_name", "QuestDB");
        prepareParams(sink, "server_version", serverVersion);
        prepareParams(sink, "integer_datetimes", "on");
        prepareParams(sink, "client_encoding", "UTF8");
        prepareBackendKeyData(sink);
        prepareReadyForQuery();
    }

    private void prepareLoginResponse() {
        sink.put(MESSAGE_TYPE_LOGIN_RESPONSE);
        sink.putInt(Integer.BYTES * 2);
        sink.putInt(3);
    }

    private void prepareParams(ResponseSink sink, CharSequence name, CharSequence value) {
        sink.put(MESSAGE_TYPE_PARAMETER_STATUS);
        final long addr = sink.skip();
        sink.encodeUtf8Z(name);
        sink.encodeUtf8Z(value);
        sink.putLen(addr);
    }

    private void prepareReadyForQuery() {
        sink.put(MESSAGE_TYPE_READY_FOR_QUERY);
        sink.putInt(Integer.BYTES + Byte.BYTES);
        sink.put(STATUS_IDLE);
    }

    private void prepareSslResponse() {
        sink.put('N');
    }

    private void processCancelMessage() {
        // From https://www.postgresql.org/docs/current/protocol-flow.html :
        // To issue a cancel request, the frontend opens a new connection to the server and sends a CancelRequest message, rather than the StartupMessage message
        // that would ordinarily be sent across a new connection. The server will process this request and then close the connection.
        // For security reasons, no direct reply is made to the cancel request message.
        int pid = getIntUnsafe(recvBufReadPos); // thread id really
        recvBufReadPos += Integer.BYTES;
        int secret = getIntUnsafe(recvBufReadPos);
        recvBufReadPos += Integer.BYTES;
        LOG.info().$("cancel request [pid=").$(pid).I$();
        try {
            registry.cancel(pid, secret);
        } catch (CairoException e) { // error message should not be sent to client
            LOG.error().$(e.getFlyweightMessage()).$();
        }
    }

    private int processInitMessage() throws BadProtocolException {
        int availableToRead = availableToRead();
        if (availableToRead < Integer.BYTES) { // size of message
            return Authenticator.NEEDS_READ;
        }
        int msgLen = getIntUnsafe(recvBufReadPos);
        if (msgLen > availableToRead) {
            return Authenticator.NEEDS_READ;
        }
        // at this point we have a full message available ready to be processed
        recvBufReadPos += Integer.BYTES; // first move beyond the msgLen
        int protocol = getIntUnsafe(recvBufReadPos);
        recvBufReadPos += Integer.BYTES;

        switch (protocol) {
            case INIT_STARTUP_MESSAGE:
                processStartupMessage(msgLen);
                break;
            case INIT_CANCEL_REQUEST:
                processCancelMessage();
                return Authenticator.NEEDS_DISCONNECT;
            case INIT_SSL_REQUEST:
                compactRecvBuf();
                prepareSslResponse();
                state = State.WRITE_AND_EXPECT_INIT_MESSAGE;
                break;
            case INIT_GSS_REQUEST:
                compactRecvBuf();
                prepareGssResponse();
                state = State.WRITE_AND_EXPECT_INIT_MESSAGE;
                break;
            default:
                LOG.error().$("unknown init message [protocol=").$(protocol).$(']').$();
                throw BadProtocolException.INSTANCE;
        }
        return Authenticator.OK;
    }

    private int processPasswordMessage() throws BadProtocolException {
        int availableToRead = availableToRead();
        if (availableToRead < 1 + Integer.BYTES) { // msgType + msgLen
            return Authenticator.NEEDS_READ;
        }
        byte msgType = Unsafe.getUnsafe().getByte(recvBufReadPos);
        assert msgType == MESSAGE_TYPE_PASSWORD_MESSAGE;

        int msgLen = getIntUnsafe(recvBufReadPos + 1);
        long msgLimit = (recvBufReadPos + msgLen + 1); // +1 for the type byte which is not included in msgLen
        if (recvBufWritePos < msgLimit) {
            return Authenticator.NEEDS_READ;
        }

        // at this point we have a full message available ready to be processed
        recvBufReadPos += 1 + Integer.BYTES; // first move beyond the msgType and msgLen

        long hi = PGConnectionContext.getStringLength(recvBufReadPos, msgLimit, "bad password length");
        if (matcher.verifyPassword(username, recvBufReadPos, (int) (hi - recvBufReadPos))) {
            recvBufReadPos = msgLimit;
            state = State.AUTH_SUCCESS;
        } else {
            LOG.info().$("bad password for user [user=").$(username).$(']').$();
            prepareErrorResponse("invalid username/password");
            state = State.WRITE_AND_AUTH_FAILURE;
        }
        return Authenticator.OK;
    }

    private void processStartupMessage(int msgLen) throws BadProtocolException {
        long msgLimit = (recvBufStart + msgLen);
        long lo = recvBufReadPos;

        // there is an extra byte at the end, and it has to be 0
        while (lo < msgLimit - 1) {
            final long nameLo = lo;
            final long nameHi = PGConnectionContext.getStringLength(lo, msgLimit, "malformed property name");
            final long valueLo = nameHi + 1;
            final long valueHi = PGConnectionContext.getStringLength(valueLo, msgLimit, "malformed property value");
            lo = valueHi + 1;

            // store user
            if (PGKeywords.isUser(nameLo, nameHi - nameLo)) {
                CharacterStoreEntry e = characterStore.newEntry();
                e.put(dus.of(valueLo, valueHi));
                this.username = e.toImmutable();
            }
            boolean parsed = true;
            if (PGKeywords.isOptions(nameLo, nameHi - nameLo)) {
                if (PGKeywords.startsWithTimeoutOption(valueLo, valueHi - valueLo)) {
                    try {
                        dus.of(valueLo + 21, valueHi);
                        long statementTimeout = Numbers.parseLong(dus);
                        optionsListener.setStatementTimeout(statementTimeout);
                    } catch (NumericException ex) {
                        parsed = false;
                    }
                } else {
                    parsed = false;
                }
            }
            if (parsed) {
                LOG.debug().$("property [name=").$(dus.of(nameLo, nameHi)).$(", value=").$(dus.of(valueLo, valueHi)).$(']').$();
            } else {
                LOG.info().$("invalid property [name=").$(dus.of(nameLo, nameHi)).$(", value=").$(dus.of(valueLo, valueHi)).$(']').$();
            }
        }
        characterStore.clear();
        recvBufReadPos = msgLimit;
        compactRecvBuf();
        prepareLoginResponse();
        state = State.WRITE_AND_EXPECT_PASSWORD_MESSAGE;
    }

    private int readFromSocket() {
        int bytesRead = socket.recv(recvBufWritePos, (int) (recvBufEnd - recvBufWritePos));
        if (bytesRead < 0) {
            return Authenticator.NEEDS_DISCONNECT;
        }
        recvBufWritePos += bytesRead;
        return Authenticator.OK;
    }

    private int writeToSocketAndAdvance(State nextState) {
        int toWrite = (int) (sendBufWritePos - sendBufReadPos);
        int n = socket.send(sendBufReadPos, toWrite);
        if (n < 0) {
            return Authenticator.NEEDS_DISCONNECT;
        }
        sendBufReadPos += n;
        compactSendBuf();
        if (sendBufReadPos == sendBufWritePos) {
            state = nextState;
            return Authenticator.OK;
        }
        // we could try to call socket.send() again as there could be space in the socket buffer now
        // but: auth messages are small and we assume that the socket buffer is large enough to accommodate them in one go
        // thus this return should be rare and we will just wait for the next select() call
        return Authenticator.NEEDS_WRITE;
    }


    private enum State {
        EXPECT_INIT_MESSAGE,
        EXPECT_PASSWORD_MESSAGE,
        WRITE_AND_EXPECT_INIT_MESSAGE,
        WRITE_AND_EXPECT_PASSWORD_MESSAGE,
        WRITE_AND_AUTH_SUCCESS,
        WRITE_AND_AUTH_FAILURE,
        AUTH_SUCCESS,
        AUTH_FAILED
    }

    private class ResponseSink implements Utf8Sink {

        @Override
        public Utf8Sink put(long lo, long hi) {
            final long size = hi - lo;
            ensureCapacity(size);
            Vect.memcpy(sendBufWritePos, lo, size);
            sendBufWritePos += size;
            return this;
        }

        @Override
        public Utf8Sink put(@Nullable Utf8Sequence us) {
            return this;
        }

        @Override
        public Utf8Sink put(byte b) {
            ensureCapacity(Byte.BYTES);
            Unsafe.getUnsafe().putByte(sendBufWritePos++, b);
            return this;
        }

        public void putInt(int i) {
            ensureCapacity(Integer.BYTES);
            Unsafe.getUnsafe().putInt(sendBufWritePos, Numbers.bswap(i));
            sendBufWritePos += Integer.BYTES;
        }

        public void putLen(long start) {
            int len = (int) (sendBufWritePos - start);
            Unsafe.getUnsafe().putInt(start, Numbers.bswap(len));
        }

        void encodeUtf8Z(CharSequence value) {
            put(value);
            ensureCapacity(Byte.BYTES);
            put((byte) 0);
        }

        long skip() {
            ensureCapacity(Integer.BYTES);
            long checkpoint = sendBufWritePos;
            sendBufWritePos += Integer.BYTES;
            return checkpoint;
        }
    }
}