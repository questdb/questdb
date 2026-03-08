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

package io.questdb.cutlass.pgwire;

import io.questdb.BuildInformation;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cutlass.auth.AuthenticatorException;
import io.questdb.cutlass.auth.SocketAuthenticator;
import io.questdb.cutlass.auth.UsernamePasswordMatcher;
import io.questdb.griffin.CharacterStore;
import io.questdb.griffin.CharacterStoreEntry;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.NoSpaceLeftInResponseBufferException;
import io.questdb.network.Socket;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8Sink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.cairo.SecurityContext.AUTH_TYPE_NONE;
import static io.questdb.cutlass.pgwire.PGConnectionContext.dumpBuffer;

public class PGCleartextPasswordAuthenticator implements SocketAuthenticator {
    public static final char STATUS_IDLE = 'I';
    private static final int INIT_CANCEL_REQUEST = 80877102;
    private static final int INIT_GSS_REQUEST = 80877104;
    private static final int INIT_SSL_REQUEST = 80877103;
    private static final int INIT_STARTUP_MESSAGE = 196608;
    private static final Log LOG = LogFactory.getLog(PGCleartextPasswordAuthenticator.class);
    private static final byte MESSAGE_TYPE_ERROR_RESPONSE = 'E';
    private static final byte MESSAGE_TYPE_LOGIN_RESPONSE = 'R';
    private static final byte MESSAGE_TYPE_PARAMETER_STATUS = 'S';
    private static final byte MESSAGE_TYPE_PASSWORD_MESSAGE = 'p';
    private static final byte MESSAGE_TYPE_READY_FOR_QUERY = 'Z';
    private final BuildInformation buildInformation;
    private final CharacterStore characterStore;
    private final NetworkSqlExecutionCircuitBreaker circuitBreaker;
    private final int circuitBreakerId;
    private final boolean dumpNetworkTraffic;
    private final DirectUtf8String dus = new DirectUtf8String();
    private final boolean matcherOwned;
    private final OptionsListener optionsListener;
    private final PGCircuitBreakerRegistry registry;
    private final String serverVersion;
    private final ResponseSink sink;
    protected CharSequence username;
    private byte authType = AUTH_TYPE_NONE;
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

    public PGCleartextPasswordAuthenticator(
            PGConfiguration configuration,
            BuildInformation buildInformation,
            NetworkSqlExecutionCircuitBreaker circuitBreaker,
            PGCircuitBreakerRegistry registry,
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
        this.dumpNetworkTraffic = configuration.getDumpNetworkTraffic();
        this.buildInformation = buildInformation;
    }

    @Override
    public void clear() {
        authType = AUTH_TYPE_NONE;
        circuitBreaker.setSecret(-1);
        circuitBreaker.resetMaxTimeToDefault();
        circuitBreaker.unsetTimer();
        Misc.clear(characterStore);
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
        return authType;
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
                        if (r != SocketAuthenticator.OK) {
                            return r;
                        }
                        r = processInitMessage();
                        if (r != SocketAuthenticator.OK) {
                            return r;
                        }
                        break;
                    }
                    case EXPECT_PASSWORD_MESSAGE: {
                        int r = readFromSocket();
                        if (r != SocketAuthenticator.OK) {
                            return r;
                        }
                        r = processPasswordMessage();
                        if (r != SocketAuthenticator.OK) {
                            return r;
                        }
                        break;
                    }
                    case WRITE_AND_EXPECT_PASSWORD_MESSAGE: {
                        int r = writeToSocketAndAdvance(State.EXPECT_PASSWORD_MESSAGE);
                        if (r != SocketAuthenticator.OK) {
                            return r;
                        }
                        break;
                    }
                    case WRITE_AND_EXPECT_INIT_MESSAGE: {
                        int r = writeToSocketAndAdvance(State.EXPECT_INIT_MESSAGE);
                        if (r != SocketAuthenticator.OK) {
                            return r;
                        }
                        break;
                    }
                    case WRITE_AND_AUTH_SUCCESS: {
                        int r = writeToSocketAndAdvance(State.AUTH_SUCCESS);
                        if (r != SocketAuthenticator.OK) {
                            return r;
                        }
                        break;
                    }
                    case WRITE_AND_AUTH_FAILURE: {
                        int r = writeToSocketAndAdvance(State.AUTH_FAILED);
                        if (r != SocketAuthenticator.OK) {
                            return r;
                        }
                        break;
                    }
                    case AUTH_SUCCESS:
                        circuitBreaker.of(socket.getFd());
                        return SocketAuthenticator.OK;
                    case AUTH_FAILED:
                        return SocketAuthenticator.NEEDS_DISCONNECT;
                    default:
                        assert false;
                }
            }
        } catch (PGMessageProcessingException e) {
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

    private void checkCapacity(long capacity) {
        if (sendBufWritePos + capacity > sendBufEnd) {
            throw NoSpaceLeftInResponseBufferException.instance(capacity, sendBufEnd - sendBufWritePos, sendBufEnd - sendBufStart);
        }
    }

    private void compactRecvBuf() {
        long len = recvBufWritePos - recvBufReadPos;
        if (len > 0) {
            Vect.memmove(recvBufStart, recvBufReadPos, len);
        }
        recvBufReadPos = recvBufStart;
        recvBufWritePos = recvBufStart + len;
    }

    private void compactSendBuf() {
        long len = sendBufWritePos - sendBufReadPos;
        if (len > 0) {
            Vect.memmove(sendBufStart, sendBufReadPos, len);
        }
        sendBufReadPos = sendBufStart;
        sendBufWritePos = sendBufStart + len;
    }

    private void prepareBackendKeyData(ResponseSink responseSink) {
        responseSink.put('K');
        responseSink.putInt(Integer.BYTES * 3); // length of this message

        // the below 8 bytes will not match when dumping PG traffic!
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
        if (!dumpNetworkTraffic && buildInformation != null) {
            prepareParams(sink, "questdb_version", buildInformation.getSwVersion());
        }
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
            LOG.error().$safe(e.getFlyweightMessage()).$();
        }
    }

    private int processInitMessage() throws PGMessageProcessingException {
        int availableToRead = availableToRead();
        if (availableToRead < Integer.BYTES) { // size of message
            return SocketAuthenticator.NEEDS_READ;
        }
        int msgLen = getIntUnsafe(recvBufReadPos);
        if (msgLen > availableToRead) {
            return SocketAuthenticator.NEEDS_READ;
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
                return SocketAuthenticator.NEEDS_DISCONNECT;
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
                throw PGMessageProcessingException.INSTANCE;
        }
        return SocketAuthenticator.OK;
    }

    private int processPasswordMessage() throws PGMessageProcessingException {
        int availableToRead = availableToRead();
        if (availableToRead < 1 + Integer.BYTES) { // msgType + msgLen
            return SocketAuthenticator.NEEDS_READ;
        }
        byte msgType = Unsafe.getUnsafe().getByte(recvBufReadPos);
        assert msgType == MESSAGE_TYPE_PASSWORD_MESSAGE;

        int msgLen = getIntUnsafe(recvBufReadPos + 1);
        long msgLimit = (recvBufReadPos + msgLen + 1); // +1 for the type byte which is not included in msgLen
        if (recvBufWritePos < msgLimit) {
            return SocketAuthenticator.NEEDS_READ;
        }

        // at this point we have a full message available ready to be processed
        recvBufReadPos += 1 + Integer.BYTES; // first move beyond the msgType and msgLen

        long hi = PGConnectionContext.getUtf8StrSize(recvBufReadPos, msgLimit, "bad password length", null);
        authType = verifyPassword(username, recvBufReadPos, (int) (hi - recvBufReadPos));
        if (authType != AUTH_TYPE_NONE) {
            recvBufReadPos = msgLimit;
            state = State.AUTH_SUCCESS;
        } else {
            LOG.info().$("bad password for user [user=").$(username).$(']').$();
            prepareErrorResponse("invalid username/password");
            state = State.WRITE_AND_AUTH_FAILURE;
        }
        return SocketAuthenticator.OK;
    }

    private void processStartupMessage(int msgLen) throws PGMessageProcessingException {
        long msgLimit = (recvBufStart + msgLen);
        long lo = recvBufReadPos;

        // there is an extra byte at the end, and it has to be 0
        while (lo < msgLimit - 1) {
            final long nameLo = lo;
            final long nameHi = PGConnectionContext.getUtf8StrSize(lo, msgLimit, "malformed property name", null);
            final long valueLo = nameHi + 1;
            final long valueHi = PGConnectionContext.getUtf8StrSize(valueLo, msgLimit, "malformed property value", null);
            lo = valueHi + 1;

            // store user
            if (PGKeywords.isUser(nameLo, nameHi - nameLo)) {
                CharacterStoreEntry e = characterStore.newEntry();
                e.put(dus.of(valueLo, valueHi, false));
                this.username = e.toImmutable();
            }
            boolean parsed = true;
            if (PGKeywords.isOptions(nameLo, nameHi - nameLo)) {
                if (PGKeywords.startsWithTimeoutOption(valueLo, valueHi - valueLo)) {
                    try {
                        dus.of(valueLo + 21, valueHi, false);
                        long statementTimeout = Numbers.parseLong(dus);
                        optionsListener.setSqlTimeout(statementTimeout);
                    } catch (NumericException ex) {
                        parsed = false;
                    }
                } else {
                    parsed = false;
                }
            }
            if (parsed) {
                LOG.debug().$("property [name=").$(dus.of(nameLo, nameHi, false))
                        .$(", value=").$(dus.of(valueLo, valueHi, false))
                        .$(']').$();
            } else {
                LOG.info().$("invalid property [name=").$safe(dus.of(nameLo, nameHi, false))
                        .$(", value=").$(dus.of(valueLo, valueHi, false))
                        .$(']').$();
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
        dumpBuffer('>', recvBufWritePos, bytesRead, dumpNetworkTraffic);
        if (bytesRead < 0) {
            return SocketAuthenticator.NEEDS_DISCONNECT;
        }
        recvBufWritePos += bytesRead;
        return SocketAuthenticator.OK;
    }

    private int writeToSocketAndAdvance(State nextState) {
        int toWrite = (int) (sendBufWritePos - sendBufReadPos);
        int n = socket.send(sendBufReadPos, toWrite);
        dumpBuffer('<', sendBufReadPos, n, dumpNetworkTraffic);
        if (n < 0) {
            return SocketAuthenticator.NEEDS_DISCONNECT;
        }
        sendBufReadPos += n;
        compactSendBuf();
        if (sendBufReadPos == sendBufWritePos) {
            state = nextState;
            return SocketAuthenticator.OK;
        }
        // we could try to call socket.send() again as there could be space in the socket buffer now
        // but: auth messages are small and we assume that the socket buffer is large enough to accommodate them in one go
        // thus this return should be rare and we will just wait for the next select() call
        return SocketAuthenticator.NEEDS_WRITE;
    }

    // kept protected for enterprise
    protected byte verifyPassword(CharSequence username, long passwordPtr, int passwordLen) {
        return matcher.verifyPassword(username, passwordPtr, passwordLen);
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
        public Utf8Sink put(@Nullable Utf8Sequence us) {
            return this;
        }

        @Override
        public Utf8Sink put(byte b) {
            checkCapacity(Byte.BYTES);
            Unsafe.getUnsafe().putByte(sendBufWritePos++, b);
            return this;
        }

        public void putInt(int i) {
            checkCapacity(Integer.BYTES);
            Unsafe.getUnsafe().putInt(sendBufWritePos, Numbers.bswap(i));
            sendBufWritePos += Integer.BYTES;
        }

        public void putLen(long start) {
            int len = (int) (sendBufWritePos - start);
            Unsafe.getUnsafe().putInt(start, Numbers.bswap(len));
        }

        @Override
        public Utf8Sink putNonAscii(long lo, long hi) {
            final long size = hi - lo;
            checkCapacity(size);
            Vect.memcpy(sendBufWritePos, lo, size);
            sendBufWritePos += size;
            return this;
        }

        void encodeUtf8Z(CharSequence value) {
            put(value);
            checkCapacity(Byte.BYTES);
            put((byte) 0);
        }

        long skip() {
            checkCapacity(Integer.BYTES);
            long checkpoint = sendBufWritePos;
            sendBufWritePos += Integer.BYTES;
            return checkpoint;
        }
    }
}
