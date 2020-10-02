package io.questdb.cutlass.line.tcp;

import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.Signature;
import java.security.SignatureException;
import java.util.Base64;

import io.questdb.cutlass.line.tcp.AuthDb;
import io.questdb.cutlass.line.tcp.LineTCPProtoSender;
import io.questdb.cutlass.line.udp.LineProtoSender;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.NetworkError;
import io.questdb.std.Unsafe;

public class AuthenticatedLineTCPProtoSender extends LineTCPProtoSender {
    private static final Log LOG = LogFactory.getLog(LineProtoSender.class);
    private static final long BUF_SZ = 1024;
    private final byte[] keyIdBytes;
    private final PrivateKey privateKey;
    private final Signature sig;
    private long buffer;

    public AuthenticatedLineTCPProtoSender(String keyId, PrivateKey privateKey, int sendToIPv4Address, int sendToPort, int bufferCapacity) {
        super(sendToIPv4Address, sendToPort, bufferCapacity);
        keyIdBytes = keyId.getBytes(StandardCharsets.UTF_8);
        if (keyIdBytes.length >= (BUF_SZ - 1)) {
            throw new IllegalArgumentException("keyId \"" + keyId + "\" is too long");
        }
        this.privateKey = privateKey;
        buffer = -1;
        try {
            sig = Signature.getInstance(AuthDb.SIGNATURE_TYPE_DER);
        } catch (NoSuchAlgorithmException ex) {
            throw new Error(ex);
        }
    }

    public void authenticate() throws NetworkError {
        if (buffer == -1) {
            buffer = Unsafe.malloc(BUF_SZ);
        }

        // Send key id
        int n = 0;
        while (n < keyIdBytes.length) {
            Unsafe.getUnsafe().putByte(buffer + n, keyIdBytes[n]);
            n++;
        }
        Unsafe.getUnsafe().putByte(buffer + n, (byte) '\n');
        n++;
        if (nf.send(fd, buffer, n) != n) {
            throw NetworkError.instance(nf.errno()).put("send error");
        }

        // Receive challenge
        n = 0;
        while (true) {
            int rc = nf.recv(fd, buffer + n, 1);
            if (rc < 0) {
                throw NetworkError.instance(nf.errno()).put("disconnected during authentication");
            }
            byte b = Unsafe.getUnsafe().getByte(buffer + n);
            if (b == (byte) '\n') {
                break;
            }
            n++;
        }

        int sz = n;
        byte[] challengeBytes = new byte[sz];
        for (n = 0; n < sz; n++) {
            challengeBytes[n] = Unsafe.getUnsafe().getByte(buffer + n);
        }

        // Send signature
        byte[] rawSignature;
        try {
            sig.initSign(privateKey);
            sig.update(challengeBytes);
            rawSignature = sig.sign();
        } catch (InvalidKeyException | SignatureException ex) {
            throw new RuntimeException(ex);
        }

        byte[] signature = Base64.getEncoder().encode(rawSignature);
        for (n = 0; n < signature.length; n++) {
            Unsafe.getUnsafe().putByte(buffer + n, signature[n]);
        }
        Unsafe.getUnsafe().putByte(buffer + n, (byte) '\n');
        n++;
        if (nf.send(fd, buffer, n) != n) {
            throw NetworkError.instance(nf.errno()).put("send error");
        }
        LOG.info().$("authenticated").$();
    }

    @Override
    protected void sendToSocket(long fd, long lo, long sockaddr, int len) throws NetworkError {
        super.sendToSocket(fd, lo, sockaddr, len);
    }

    @Override
    public void close() {
        if (buffer != -1) {
            Unsafe.free(buffer, BUF_SZ);
            buffer = -1;
        }
        super.close();
    }
}
