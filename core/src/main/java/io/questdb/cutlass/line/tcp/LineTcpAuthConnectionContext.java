package io.questdb.cutlass.line.tcp;

import io.questdb.cairo.CairoException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectByteCharSequence;

import java.security.*;
import java.util.Base64;

class LineTcpAuthConnectionContext extends LineTcpConnectionContext {
    private static final Log LOG = LogFactory.getLog(LineTcpAuthConnectionContext.class);
    private static final int CHALLENGE_LEN = 512;
    private static final int MIN_BUF_SIZE = CHALLENGE_LEN + 1;
    private final SecureRandom srand = new SecureRandom();
    private final AuthDb authDb;
    private final Signature sig;
    private final DirectByteCharSequence charSeq = new DirectByteCharSequence();
    private final byte[] challengeBytes = new byte[CHALLENGE_LEN];
    private PublicKey pubKey;
    private boolean authenticated;

    private enum AuthState {
        WAITING_FOR_KEY_ID(IOContextResult.NEEDS_READ), SENDING_CHALLENGE(IOContextResult.NEEDS_WRITE), WAITING_FOR_RESPONSE(IOContextResult.NEEDS_READ),
        COMPLETE(IOContextResult.NEEDS_READ), FAILED(IOContextResult.NEEDS_DISCONNECT);

        private final IOContextResult ioContextResult;

        AuthState(IOContextResult ioContextResult) {
            this.ioContextResult = ioContextResult;
        }
    }

    private AuthState authState;

    LineTcpAuthConnectionContext(LineTcpReceiverConfiguration configuration, AuthDb authDb, LineTcpMeasurementScheduler scheduler) {
        super(configuration, scheduler);
        if (configuration.getNetMsgBufferSize() < MIN_BUF_SIZE) {
            throw CairoException.instance(0).put("Minimum buffer length is " + MIN_BUF_SIZE);
        }
        this.authDb = authDb;
        try {
            sig = Signature.getInstance(AuthDb.SIGNATURE_TYPE);
        } catch (NoSuchAlgorithmException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    IOContextResult handleIO() {
        if (authenticated) {
            return super.handleIO();
        }
        return handleAuth();
    }

    private IOContextResult handleAuth() {
            switch (authState) {
                case WAITING_FOR_KEY_ID:
                    readKeyId();
                    break;
                case SENDING_CHALLENGE:
                    sendChallenge();
                    break;
                case WAITING_FOR_RESPONSE:
                    waitForResponse();
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
        int nWritten = nf.send(fd, recvBufPos, n);
        if (nWritten >= 0) {
            if (n == nWritten) {
                recvBufPos = recvBufStart;
                authState = AuthState.WAITING_FOR_RESPONSE;
                return;
            }
            recvBufPos += nWritten;
            return;
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
            boolean verified;
            try {
                sig.initVerify(pubKey);
                sig.update(challengeBytes);
                verified = sig.verify(signatureRaw);
            } catch (InvalidKeyException | SignatureException ex) {
                LOG.info().$('[').$(fd).$("] authentication exception ").$(ex);
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
            LOG.info().$('[').$(fd).$("] authentication success").$();
        }
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

    @Override
    public void clear() {
        authenticated = false;
        authState = AuthState.WAITING_FOR_KEY_ID;
        pubKey = null;
        super.clear();
    }
}
