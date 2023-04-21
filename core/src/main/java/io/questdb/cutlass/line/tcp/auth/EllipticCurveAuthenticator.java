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

package io.questdb.cutlass.line.tcp.auth;

import io.questdb.cairo.CairoException;
import io.questdb.cutlass.auth.AuthUtils;
import io.questdb.cutlass.auth.Authenticator;
import io.questdb.cutlass.auth.AuthenticatorException;
import io.questdb.cutlass.auth.PublicKeyRepo;
import io.questdb.cutlass.line.tcp.LineTcpConnectionContext;
import io.questdb.cutlass.line.tcp.NetworkIOJob;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.NetworkFacade;
import io.questdb.std.Chars;
import io.questdb.std.ThreadLocal;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectByteCharSequence;

import java.security.*;
import java.util.Base64;

public class EllipticCurveAuthenticator implements Authenticator {
    private static final int CHALLENGE_LEN = 512;
    private static final Log LOG = LogFactory.getLog(EllipticCurveAuthenticator.class);
    private static final int MIN_BUF_SIZE = CHALLENGE_LEN + 1;
    private static final ThreadLocal<Signature> tlSigDER = new ThreadLocal<>(() -> {
        try {
            return Signature.getInstance(AuthUtils.SIGNATURE_TYPE_DER);
        } catch (NoSuchAlgorithmException ex) {
            throw new Error(ex);
        }
    });
    private static final ThreadLocal<Signature> tlSigP1363 = new ThreadLocal<>(() -> {
        try {
            return Signature.getInstance(AuthUtils.SIGNATURE_TYPE_P1363);
        } catch (NoSuchAlgorithmException ex) {
            throw new Error(ex);
        }
    });
    private static final ThreadLocal<SecureRandom> tlSrand = new ThreadLocal<>(SecureRandom::new);
    private final byte[] challengeBytes = new byte[CHALLENGE_LEN];
    private final NetworkFacade nf;
    private final PublicKeyRepo publicKeyRepo;
    private final long recvBufEnd;
    private final long recvBufStart;
    private final DirectByteCharSequence userNameFlyweight = new DirectByteCharSequence();
    protected long recvBufPseudoStart;
    private AuthState authState;
    private int fd;
    private String principal;
    private PublicKey pubKey;
    private long recvBufPos;

    public EllipticCurveAuthenticator(
            NetworkFacade networkFacade,
            PublicKeyRepo publicKeyRepo,
            long recvBufStart,
            long recvBufEnd
    ) {
        if (recvBufEnd - recvBufStart < MIN_BUF_SIZE) {
            throw CairoException.critical(0).put("Minimum buffer length is ").put(MIN_BUF_SIZE);
        }
        this.publicKeyRepo = publicKeyRepo;
        this.recvBufStart = recvBufPos = recvBufStart;
        this.recvBufEnd = recvBufEnd;
        this.nf = networkFacade;
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
    public LineTcpConnectionContext.IOContextResult handleIO(NetworkIOJob job) throws AuthenticatorException {
        switch (authState) {
            case WAITING_FOR_KEY_ID:
                readKeyId();
                break;
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
    public void init(int fd) {
        this.fd = fd;
        authState = AuthState.WAITING_FOR_KEY_ID;
        pubKey = null;
        recvBufPos = recvBufStart;
    }

    @Override
    public boolean isAuthenticated() {
        return authState == AuthState.COMPLETE;
    }

    private static boolean checkAllZeros(byte[] signatureRaw) {
        int n = signatureRaw.length;
        for (int i = 0; i < n; i++) {
            if (signatureRaw[i] != 0) {
                return false;
            }
        }
        return true;
    }

    private int findLineEnd() throws AuthenticatorException {
        int bufferRemaining = (int) (recvBufEnd - recvBufPos);
        if (bufferRemaining > 0) {
            int bytesRead = nf.recv(fd, recvBufPos, bufferRemaining);
            if (bytesRead > 0) {
                recvBufPos += bytesRead;
            } else if (bytesRead < 0) {
                LOG.info().$('[').$(fd).$("] authentication disconnected by peer when reading token").$();
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
            LOG.info().$('[').$(fd).$("] authentication token is too long").$();
            throw AuthenticatorException.INSTANCE;
        }

        return lineEnd;
    }

    private void readKeyId() throws AuthenticatorException {
        int lineEnd = findLineEnd();
        if (lineEnd != -1) {
            userNameFlyweight.of(recvBufStart, recvBufStart + lineEnd);
            principal = Chars.toString(userNameFlyweight);
            LOG.info().$('[').$(fd).$("] authentication read key id [keyId=").$(userNameFlyweight).$(']').$();
            pubKey = publicKeyRepo.getPublicKey(userNameFlyweight);
            recvBufPos = recvBufStart;
            // Generate a challenge with printable ASCII characters 0x20 to 0x7e
            int n = 0;
            SecureRandom srand = tlSrand.get();
            while (n < CHALLENGE_LEN) {
                assert recvBufStart + n < recvBufEnd;
                int r = (int) (srand.nextDouble() * 0x5f) + 0x20;
                Unsafe.getUnsafe().putByte(recvBufStart + n, (byte) r);
                challengeBytes[n] = (byte) r;
                n++;
            }
            Unsafe.getUnsafe().putByte(recvBufStart + n, (byte) '\n');
            authState = AuthState.SENDING_CHALLENGE;
        }
    }

    private void sendChallenge() throws AuthenticatorException {
        int n = CHALLENGE_LEN + 1 - (int) (recvBufPos - recvBufStart);
        assert n > 0;
        while (true) {
            int nWritten = nf.send(fd, recvBufPos, n);
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
        LOG.info().$('[').$(fd).$("] authentication peer disconnected when challenge was being sent").$();
        throw AuthenticatorException.INSTANCE;
    }

    private int waitForResponse() throws AuthenticatorException {
        int lineEnd = findLineEnd();
        if (lineEnd != -1) {
            // Verify signature
            if (null == pubKey) {
                LOG.info().$('[').$(fd).$("] authentication failed, unknown key id").$();
                throw AuthenticatorException.INSTANCE;
            }

            byte[] signature = new byte[lineEnd];
            for (int n = 0; n < lineEnd; n++) {
                signature[n] = Unsafe.getUnsafe().getByte(recvBufStart + n);
            }

            authState = AuthState.FAILED;

            byte[] signatureRaw = Base64.getDecoder().decode(signature);
            Signature sig = signatureRaw.length == 64 ? tlSigP1363.get() : tlSigDER.get();
            boolean verified;
            try {
                // On some out of date JDKs zeros can be valid signature because of a bug in the JDK code
                // Check that it's not the case.
                if (checkAllZeros(signatureRaw)) {
                    LOG.info().$('[').$(fd).$("] invalid signature, can be cyber attack!").$();
                    throw AuthenticatorException.INSTANCE;
                }
                sig.initVerify(pubKey);
                sig.update(challengeBytes);
                verified = sig.verify(signatureRaw);
            } catch (InvalidKeyException | SignatureException ex) {
                LOG.info().$('[').$(fd).$("] authentication exception ").$(ex).$();
                throw AuthenticatorException.INSTANCE;
            }

            if (!verified) {
                LOG.info().$('[').$(fd).$("] authentication failed, signature was not verified").$();
                throw AuthenticatorException.INSTANCE;
            }

            authState = AuthState.COMPLETE;
            LOG.info().$('[').$(fd).$("] authentication success").$();
        }
        return lineEnd;
    }

    private enum AuthState {
        WAITING_FOR_KEY_ID(LineTcpConnectionContext.IOContextResult.NEEDS_READ),
        SENDING_CHALLENGE(LineTcpConnectionContext.IOContextResult.NEEDS_WRITE),
        WAITING_FOR_RESPONSE(LineTcpConnectionContext.IOContextResult.NEEDS_READ),
        COMPLETE(LineTcpConnectionContext.IOContextResult.NEEDS_READ),
        FAILED(LineTcpConnectionContext.IOContextResult.NEEDS_DISCONNECT);

        private final LineTcpConnectionContext.IOContextResult ioContextResult;

        AuthState(LineTcpConnectionContext.IOContextResult ioContextResult) {
            this.ioContextResult = ioContextResult;
        }
    }

}
