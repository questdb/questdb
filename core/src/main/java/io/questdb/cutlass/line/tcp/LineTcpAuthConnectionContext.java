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

package io.questdb.cutlass.line.tcp;

import io.questdb.Metrics;
import io.questdb.cairo.CairoException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.ThreadLocal;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectByteCharSequence;

import java.security.*;
import java.util.Base64;

public class LineTcpAuthConnectionContext extends LineTcpConnectionContext {
    private static final int CHALLENGE_LEN = 512;
    private static final Log LOG = LogFactory.getLog(LineTcpAuthConnectionContext.class);
    private static final int MIN_BUF_SIZE = CHALLENGE_LEN + 1;
    private static final ThreadLocal<Signature> tlSigDER = new ThreadLocal<>(() -> {
        try {
            return Signature.getInstance(AuthDb.SIGNATURE_TYPE_DER);
        } catch (NoSuchAlgorithmException ex) {
            throw new Error(ex);
        }
    });
    private static final ThreadLocal<Signature> tlSigP1363 = new ThreadLocal<>(() -> {
        try {
            return Signature.getInstance(AuthDb.SIGNATURE_TYPE_P1363);
        } catch (NoSuchAlgorithmException ex) {
            throw new Error(ex);
        }
    });
    private static final ThreadLocal<SecureRandom> tlSrand = new ThreadLocal<>(SecureRandom::new);
    private final AuthDb authDb;
    private final byte[] challengeBytes = new byte[CHALLENGE_LEN];
    private final DirectByteCharSequence charSeq = new DirectByteCharSequence();
    private AuthState authState;
    private boolean authenticated;
    private PublicKey pubKey;

    public LineTcpAuthConnectionContext(
            LineTcpReceiverConfiguration configuration,
            AuthDb authDb,
            LineTcpMeasurementScheduler scheduler,
            Metrics metrics
    ) {
        super(configuration, scheduler, metrics);
        if (configuration.getNetMsgBufferSize() < MIN_BUF_SIZE) {
            throw CairoException.critical(0).put("Minimum buffer length is ").put(MIN_BUF_SIZE);
        }
        this.authDb = authDb;
    }

    @Override
    public void clear() {
        authenticated = false;
        authState = AuthState.WAITING_FOR_KEY_ID;
        pubKey = null;
        super.clear();
    }

    private boolean checkAllZeros(byte[] signatureRaw) {
        int n = signatureRaw.length;
        for (int i = 0; i < n; i++) {
            if (signatureRaw[i] != 0) {
                return false;
            }
        }
        return true;
    }

    private int findLineEnd() {
        read();
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
            authState = AuthState.FAILED;
            return -1;
        }
        if (peerDisconnected) {
            LOG.info().$('[').$(fd).$("] authentication disconnected by peer when reading token").$();
            authState = AuthState.FAILED;
            return -1;
        }
        return -1;
    }

    private IOContextResult handleAuth(NetworkIOJob netIoJob) {
        switch (authState) {
            case WAITING_FOR_KEY_ID:
                readKeyId();
                break;
            case SENDING_CHALLENGE:
                sendChallenge();
                break;
            case WAITING_FOR_RESPONSE:
                waitForResponse();
                if (authenticated && recvBufPos > recvBufStart) {
                    // if authentication is completed and there are still bytes remaining in the buffer
                    // we have to parse them
                    return parseMeasurements(netIoJob);
                }
                break;
            default:
                break;
        }
        return authState.ioContextResult;
    }

    private void readKeyId() {
        int lineEnd = findLineEnd();
        if (lineEnd != -1) {
            charSeq.of(recvBufStart, recvBufStart + lineEnd);
            LOG.info().$('[').$(fd).$("] authentication read key id [keyId=").$(charSeq).$(']').$();
            pubKey = authDb.getPublicKey(charSeq);

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

    private void sendChallenge() {
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
        authState = AuthState.FAILED;
    }

    private void waitForResponse() {
        int lineEnd = findLineEnd();
        if (lineEnd != -1) {
            // Verify signature
            if (null == pubKey) {
                LOG.info().$('[').$(fd).$("] authentication failed, unknown key id").$();
                authState = AuthState.FAILED;
                return;
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
                    authState = AuthState.FAILED;
                    return;
                }
                sig.initVerify(pubKey);
                sig.update(challengeBytes);
                verified = sig.verify(signatureRaw);
            } catch (InvalidKeyException | SignatureException ex) {
                LOG.info().$('[').$(fd).$("] authentication exception ").$(ex).$();
                verified = false;
            }

            if (!verified) {
                LOG.info().$('[').$(fd).$("] authentication failed, signature was not verified").$();
                authState = AuthState.FAILED;
                return;
            }

            authenticated = true;
            authState = AuthState.COMPLETE;
            compactBuffer(recvBufStart + lineEnd + 1);
            // we must reset start of measurement address
            resetParser();
            LOG.info().$('[').$(fd).$("] authentication success").$();
        }
    }

    @Override
    public IOContextResult handleIO(NetworkIOJob netIoJob) {
        if (authenticated) {
            return super.handleIO(netIoJob);
        }
        return handleAuth(netIoJob);
    }

    private enum AuthState {
        WAITING_FOR_KEY_ID(IOContextResult.NEEDS_READ), SENDING_CHALLENGE(IOContextResult.NEEDS_WRITE), WAITING_FOR_RESPONSE(IOContextResult.NEEDS_READ),
        COMPLETE(IOContextResult.NEEDS_READ), FAILED(IOContextResult.NEEDS_DISCONNECT);

        private final IOContextResult ioContextResult;

        AuthState(IOContextResult ioContextResult) {
            this.ioContextResult = ioContextResult;
        }
    }
}
