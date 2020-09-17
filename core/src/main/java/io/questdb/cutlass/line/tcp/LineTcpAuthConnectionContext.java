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
    private static final Log LOG = LogFactory.getLog(LineTcpConnectionContext.class);
    private static final int CHALLENGE_LEN = 512;
    private static final int MIN_BUF_SIZE = CHALLENGE_LEN + 1;
    private final SecureRandom srand = new SecureRandom();
    private final AuthDb authDb;
    private final Signature sig;
    private final DirectByteCharSequence charSeq = new DirectByteCharSequence();
    private final byte[] challengeBytes = new byte[CHALLENGE_LEN];
    private PublicKey pubKey;
    private boolean authenticated;
    private boolean challengeGenerated;
    private boolean challengeSent;

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

        if (peerDisconnected) {
            LOG.info().$('[').$(fd).$("] peer disconnected before authentication completed").$();
            dispatcher.disconnect(this);
            return false;
        }

        if (challengeSent) {
            // Read signature
            read();
            int lineEnd = findLineEnd();
            if (lineEnd == -1) {
                if (recvBufPos == recvBufEnd) {
                    LOG.info().$('[').$(fd).$("] authentication challenge response is invalid").$();
                    dispatcher.disconnect(this);
                    return false;
                }
                dispatcher.registerChannel(this, IOOperation.READ);
                return true;
            }

            // Verify signature
            if (null == pubKey) {
                LOG.info().$('[').$(fd).$("] authentication failed, unknown key id").$();
                dispatcher.disconnect(this);
                return false;
            }

            int sz = lineEnd;
            byte[] signature = new byte[sz];
            for (int n = 0; n < sz; n++) {
                signature[n] = Unsafe.getUnsafe().getByte(recvBufStart + n);
            }

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
                dispatcher.disconnect(this);
                return false;
            }

            authenticated = true;
            compactBuffer(recvBufStart + lineEnd + 1);
            LOG.info().$('[').$(fd).$("] authentication success").$();

            return true;
        }

        if (challengeGenerated) {
            // Write challenge
            int n = CHALLENGE_LEN + 1 - (int) (recvBufPos - recvBufStart);
            assert n > 0;
            int nWritten = nf.send(fd, recvBufPos, n);
            if (nWritten >= 0) {
                n -= nWritten;
                if (n == 0) {
                    challengeSent = true;
                    dispatcher.registerChannel(this, IOOperation.READ);
                    return true;
                }
                dispatcher.registerChannel(this, IOOperation.WRITE);
                return nWritten > 0;
            }
            peerDisconnected = true;
            LOG.info().$('[').$(fd).$("] peer disconnected when challenge was being sent").$();
            dispatcher.disconnect(this);
            return false;
        }

        if (null == pubKey) {
            // Read keyid
            read();
            int n;
            int lineEnd = findLineEnd();
            if (lineEnd == -1) {
                if (recvBufPos == recvBufEnd) {
                    LOG.info().$('[').$(fd).$("] authentication key id is invalid").$();
                    dispatcher.disconnect(this);
                    return false;
                }
                dispatcher.registerChannel(this, IOOperation.READ);
                return true;
            }

            charSeq.of(recvBufStart, recvBufStart + lineEnd);
            LOG.info().$('[').$(fd).$("] authentication key id is ").$(charSeq).$();
            pubKey = authDb.getPublicKey(charSeq);

            recvBufPos = recvBufStart;
            // Generate a challenge with printable ASCII characters 0x20 to 0x7e
            n = 0;
            while (n < CHALLENGE_LEN) {
                assert recvBufStart + n < recvBufEnd;
                int r = (int) (srand.nextDouble() * 0x5f) + 0x20;
                Unsafe.getUnsafe().putByte(recvBufStart + n, (byte) r);
                challengeBytes[n] = (byte) r;
                n++;
            }
            Unsafe.getUnsafe().putByte(recvBufStart + n, (byte) '\n');
            n++;
            challengeGenerated = true;
            dispatcher.registerChannel(this, IOOperation.WRITE);
            return true;
        }

        return false;
    }

    private int findLineEnd() {
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
        return lineEnd;
    }

    @Override
    LineTcpConnectionContext of(long clientFd, IODispatcher<LineTcpConnectionContext> dispatcher) {
        authenticated = false;
        challengeGenerated = false;
        challengeSent = false;
        pubKey = null;
        return super.of(clientFd, dispatcher);
    }

    @Override
    public void clear() {
        super.clear();
    }
}
