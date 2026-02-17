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

package io.questdb.cutlass.line.tcp.auth;

import io.questdb.cairo.SecurityContext;
import io.questdb.cutlass.auth.AuthUtils;
import io.questdb.cutlass.auth.AuthenticatorException;
import io.questdb.cutlass.auth.ChallengeResponseMatcher;
import io.questdb.cutlass.auth.SocketAuthenticator;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.Socket;
import io.questdb.std.MemoryTag;
import io.questdb.std.ThreadLocal;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.Utf8s;
import org.jetbrains.annotations.NotNull;

import java.security.SecureRandom;

public class EllipticCurveAuthenticator implements SocketAuthenticator {
    private static final Log LOG = LogFactory.getLog(EllipticCurveAuthenticator.class);

    private static final ThreadLocal<SecureRandom> tlSrand = new ThreadLocal<>(SecureRandom::new);
    private final ChallengeResponseMatcher challengeResponseMatcher;
    private final DirectUtf8String userNameFlyweight = new DirectUtf8String();
    protected long recvBufPseudoStart;
    private AuthState authState;
    private long challengePtr;
    private String principal;
    private long recvBufEnd;
    private long recvBufPos;
    private long recvBufStart;
    private Socket socket;

    public EllipticCurveAuthenticator(ChallengeResponseMatcher challengeResponseMatcher) {
        this.challengeResponseMatcher = challengeResponseMatcher;
        this.challengePtr = Unsafe.malloc(AuthUtils.CHALLENGE_LEN, MemoryTag.NATIVE_DEFAULT);
    }

    @Override
    public void close() {
        challengePtr = Unsafe.free(challengePtr, AuthUtils.CHALLENGE_LEN, MemoryTag.NATIVE_DEFAULT);
    }

    @Override
    public byte getAuthType() {
        return SecurityContext.AUTH_TYPE_JWK_TOKEN;
    }

    @Override
    public CharSequence getPrincipal() {
        return principal;
    }

    @Override
    public long getRecvBufPos() {
        return recvBufPos;
    }

    @Override
    public long getRecvBufPseudoStart() {
        return recvBufPseudoStart;
    }

    @Override
    public int handleIO() throws AuthenticatorException {
        switch (authState) {
            case WAITING_FOR_KEY_ID:
                readKeyId();
                if (authState != AuthState.SENDING_CHALLENGE) {
                    // fall-through if we are sending challenge
                    break;
                }
            case SENDING_CHALLENGE:
                sendChallenge();
                break;
            case WAITING_FOR_RESPONSE:
                int p = waitForResponse();
                if (authState == AuthState.COMPLETE && recvBufPos > recvBufStart) {
                    recvBufPseudoStart = recvBufStart + p + 1;
                }
                break;
            default:
                break;
        }
        return authState.ioContextResult;
    }

    @Override
    public void init(@NotNull Socket socket, long recvBuffer, long recvBufferLimit, long sendBuffer, long sendBufferLimit) {
        this.socket = socket;
        authState = AuthState.WAITING_FOR_KEY_ID;
        this.recvBufStart = recvBuffer;
        this.recvBufPos = recvBuffer;
        this.recvBufEnd = recvBufferLimit;
    }

    @Override
    public boolean isAuthenticated() {
        return authState == AuthState.COMPLETE;
    }

    private int findLineEnd() throws AuthenticatorException {
        int bufferRemaining = (int) (recvBufEnd - recvBufPos);
        if (bufferRemaining > 0) {
            int bytesRead = socket.recv(recvBufPos, bufferRemaining);
            if (bytesRead > 0) {
                recvBufPos += bytesRead;
            } else if (bytesRead < 0) {
                LOG.info().$('[').$(socket.getFd()).$("] authentication disconnected by peer when reading token").$();
                throw AuthenticatorException.INSTANCE;
            }
        }
        int len = (int) (recvBufPos - recvBufStart);
        int n = 0;
        int lineEnd = -1;
        while (n < len) {
            byte b = Unsafe.getUnsafe().getByte(recvBufStart + n);
            if (b == (byte) '\n') {
                lineEnd = n;
                break;
            }
            n++;
        }

        if (lineEnd != -1) {
            return lineEnd;
        }

        if (recvBufPos == recvBufEnd) {
            LOG.info().$('[').$(socket.getFd()).$("] authentication token is too long").$();
            throw AuthenticatorException.INSTANCE;
        }

        return lineEnd;
    }

    private void readKeyId() throws AuthenticatorException {
        int lineEnd = findLineEnd();
        if (lineEnd != -1) {
            userNameFlyweight.of(recvBufStart, recvBufStart + lineEnd);
            principal = Utf8s.toString(userNameFlyweight);
            LOG.info().$('[').$(socket.getFd()).$("] authentication read key id [keyId=").$(userNameFlyweight).I$();
            recvBufPos = recvBufStart;
            // Generate a challenge with printable ASCII characters 0x20 to 0x7e
            int n = 0;
            SecureRandom srand = tlSrand.get();
            while (n < AuthUtils.CHALLENGE_LEN) {
                assert recvBufStart + n < recvBufEnd;
                int r = (int) (srand.nextDouble() * 0x5f) + 0x20;
                Unsafe.getUnsafe().putByte(recvBufStart + n, (byte) r);
                Unsafe.getUnsafe().putByte(challengePtr + n, (byte) r);
                n++;
            }
            Unsafe.getUnsafe().putByte(recvBufStart + n, (byte) '\n');
            authState = AuthState.SENDING_CHALLENGE;
        }
    }

    private void sendChallenge() throws AuthenticatorException {
        int n = AuthUtils.CHALLENGE_LEN + 1 - (int) (recvBufPos - recvBufStart);
        assert n > 0;
        while (true) {
            int nWritten = socket.send(recvBufPos, n);
            if (nWritten > 0) {
                if (n == nWritten) {
                    recvBufPos = recvBufStart;
                    authState = AuthState.WAITING_FOR_RESPONSE;
                    return;
                }
                recvBufPos += nWritten;
                continue;
            }

            if (nWritten == 0) {
                return;
            }

            break;
        }
        LOG.info().$('[').$(socket.getFd()).$("] authentication peer disconnected when challenge was being sent").$();
        throw AuthenticatorException.INSTANCE;
    }

    private int waitForResponse() throws AuthenticatorException {
        int lineEnd = findLineEnd();
        if (lineEnd != -1) {
            // Verify signature
            if (lineEnd > AuthUtils.MAX_SIGNATURE_LENGTH_BASE64) {
                LOG.info().$('[').$(socket.getFd()).$("] authentication signature is too long").$();
                throw AuthenticatorException.INSTANCE;
            }
            authState = AuthState.FAILED;
            boolean verified = challengeResponseMatcher.verifyJwk(principal, challengePtr, AuthUtils.CHALLENGE_LEN, recvBufStart, lineEnd);
            if (!verified) {
                LOG.info().$('[').$(socket.getFd()).$("] authentication failed, signature was not verified").$();
                throw AuthenticatorException.INSTANCE;
            }

            authState = AuthState.COMPLETE;
            LOG.info().$('[').$(socket.getFd()).$("] authentication success").$();
        }
        return lineEnd;
    }

    private enum AuthState {
        WAITING_FOR_KEY_ID(SocketAuthenticator.NEEDS_READ),
        SENDING_CHALLENGE(SocketAuthenticator.NEEDS_WRITE),
        WAITING_FOR_RESPONSE(SocketAuthenticator.NEEDS_READ),
        COMPLETE(SocketAuthenticator.OK),
        FAILED(SocketAuthenticator.NEEDS_DISCONNECT);

        private final int ioContextResult;

        AuthState(int ioContextResult) {
            this.ioContextResult = ioContextResult;
        }
    }
}
