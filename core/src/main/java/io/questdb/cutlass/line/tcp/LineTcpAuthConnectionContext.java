package io.questdb.cutlass.line.tcp;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.Signature;
import java.security.SignatureException;
import java.util.Base64;

import io.questdb.cairo.CairoException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.IODispatcher;
import io.questdb.network.IOOperation;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectByteCharSequence;

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
        WAITING_FOR_KEY_ID, SENDING_CHALLENGE, WAITING_FOR_RESPONSE, COMPLETE
    };

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
    boolean handleIO() {
        if (authenticated) {
            return super.handleIO();
        }
        return handleAuth();
    }

    private boolean handleAuth() {

        if (null != authState) {
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
            }
        }

        if (null != authState) {
            switch (authState) {
                case WAITING_FOR_KEY_ID:
                case WAITING_FOR_RESPONSE:
                case COMPLETE:
                    dispatcher.registerChannel(this, IOOperation.READ);
                    break;
                case SENDING_CHALLENGE:
                    dispatcher.registerChannel(this, IOOperation.WRITE);
                    break;
            }
            return true;
        }

        dispatcher.disconnect(this);
        return false;
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
            n++;
            authState = AuthState.SENDING_CHALLENGE;
        }
    }

    private void sendChallenge() {
        int n = CHALLENGE_LEN + 1 - (int) (recvBufPos - recvBufStart);
        assert n > 0;
        int nWritten = nf.send(fd, recvBufPos, n);
        if (nWritten >= 0) {
            n -= nWritten;
            if (n == 0) {
                authState = AuthState.WAITING_FOR_RESPONSE;
                return;
            }
            return;
        }
        LOG.info().$('[').$(fd).$("] authentication peer disconnected when challenge was being sent").$();
        authState = null;
        return;
    }

    private void waitForResponse() {
        int lineEnd = findLineEnd();
        if (lineEnd != -1) {
            // Verify signature
            if (null == pubKey) {
                LOG.info().$('[').$(fd).$("] authentication failed, unknown key id").$();
                authState = null;
                return;
            }

            int sz = lineEnd;
            byte[] signature = new byte[sz];
            for (int n = 0; n < sz; n++) {
                signature[n] = Unsafe.getUnsafe().getByte(recvBufStart + n);
            }
            authState = null;

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
                authState = null;
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
            authState = null;
            return -1;
        }
        if (peerDisconnected) {
            LOG.info().$('[').$(fd).$("] authentication disconnected by peer when reading token").$();
            authState = null;
            return -1;
        }
        return -1;
    }

    @Override
    LineTcpConnectionContext of(long clientFd, IODispatcher<LineTcpConnectionContext> dispatcher) {
        authenticated = false;
        authState = AuthState.WAITING_FOR_KEY_ID;
        pubKey = null;
        return super.of(clientFd, dispatcher);
    }

    @Override
    public void clear() {
        super.clear();
    }
}
