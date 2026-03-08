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

package io.questdb.cutlass.auth;

import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.Chars;
import io.questdb.std.ThreadLocal;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.AlgorithmParameters;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.security.SignatureException;
import java.security.interfaces.ECKey;
import java.security.spec.AlgorithmParameterSpec;
import java.security.spec.ECGenParameterSpec;
import java.security.spec.ECParameterSpec;
import java.security.spec.ECPoint;
import java.security.spec.ECPrivateKeySpec;
import java.security.spec.ECPublicKeySpec;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.InvalidParameterSpecException;
import java.util.Base64;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class AuthUtils {
    public static final int CHALLENGE_LEN = 512;
    public static final String EC_ALGORITHM = "EC";
    public static final String EC_CURVE = "secp256r1";
    // there are 2 types of signatures:
    // 1. sigP1363 format is 64 bytes
    // 2. sigDER format is 72 bytes
    public static final int MAX_SIGNATURE_LENGTH = 72;
    public static final int MAX_SIGNATURE_LENGTH_BASE64 = (MAX_SIGNATURE_LENGTH / 3) * 4; // 96
    public static final String SIGNATURE_TYPE_DER = "SHA256withECDSA";
    public static final String SIGNATURE_TYPE_P1363 = "SHA256withECDSAinP1363Format";
    private static final Pattern TOKEN_PATTERN = Pattern.compile("\\s*(\\S+)(.*)");

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

    public static boolean isSignatureMatch(PublicKey publicKey, byte[] challenge, ByteBuffer signature) throws InvalidKeyException, SignatureException {
        Signature sig = signature.remaining() == 64 ? tlSigP1363.get() : tlSigDER.get();
        // On some out of date JDKs zeros can be valid signature because of a bug in the JDK code
        // Check that it's not the case.
        if (checkAllZeros(signature)) {
            return false;
        }
        sig.initVerify(publicKey);
        sig.update(challenge);
        return sig.verify(signature.array(), signature.position(), signature.remaining());
    }

    public static CharSequenceObjHashMap<PublicKey> loadAuthDb(String authDbPath) {
        CharSequenceObjHashMap<PublicKey> publicKeyByKeyId = new CharSequenceObjHashMap<>();
        int nLine = 0;
        String[] tokens = new String[4];
        try (BufferedReader r = new BufferedReader(new InputStreamReader(new BufferedInputStream(new FileInputStream(authDbPath))))) {
            String line;
            do {
                int nTokens = 0;
                line = r.readLine();
                nLine++;
                while (line != null) {
                    Matcher m = TOKEN_PATTERN.matcher(line);
                    if (!m.matches()) {
                        break;
                    }
                    String token = m.group(1);
                    if (token.startsWith("#")) {
                        break;
                    }
                    if (nTokens == tokens.length) {
                        throw new IllegalArgumentException("Too many tokens");
                    }
                    tokens[nTokens] = m.group(1);
                    nTokens++;
                    line = m.group(2);
                }

                if (nTokens == 0) {
                    continue;
                }

                if (nTokens != 4) {
                    throw new IllegalArgumentException("Was expecting 4 tokens");
                }

                if (!"ec-p-256-sha256".equals(tokens[1])) {
                    throw new IllegalArgumentException("Unrecognized type " + tokens[1]);
                }

                CharSequence keyId = tokens[0];
                if (!publicKeyByKeyId.excludes(keyId)) {
                    throw new IllegalArgumentException("Duplicate keyId " + keyId);
                }

                PublicKey publicKey = AuthUtils.toPublicKey(tokens[2], tokens[3]);
                publicKeyByKeyId.put(keyId, publicKey);
            } while (line != null);
        } catch (Exception ex) {
            throw new IllegalArgumentException("IO error, failed to read auth db file " + authDbPath + " at line " + nLine, ex);
        }
        return publicKeyByKeyId;
    }

    public static PrivateKey toPrivateKey(String encodedPrivateKey) {
        byte[] dBytes = Base64.getUrlDecoder().decode(encodedPrivateKey);

        try {
            BigInteger privateKeyInt = new BigInteger(1, dBytes);
            KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance(EC_ALGORITHM);
            AlgorithmParameterSpec prime256v1ParamSpec = new ECGenParameterSpec(EC_CURVE);
            keyPairGenerator.initialize(prime256v1ParamSpec);
            ECParameterSpec parameterSpec = ((ECKey) keyPairGenerator.generateKeyPair().getPrivate()).getParams();
            ECPrivateKeySpec privateKeySpec = new ECPrivateKeySpec(privateKeyInt, parameterSpec);
            return KeyFactory.getInstance(EC_ALGORITHM).generatePrivate(privateKeySpec);
        } catch (NoSuchAlgorithmException | InvalidAlgorithmParameterException | InvalidKeySpecException ex) {
            throw new IllegalArgumentException("Failed to decode " + encodedPrivateKey, ex);
        }
    }

    public static PublicKey toPublicKey(CharSequence encodedX, CharSequence encodedY) {
        byte[] encodedXbytes = Chars.asciiToByteArray(encodedX);
        byte[] encodedYbytes = Chars.asciiToByteArray(encodedY);
        byte[] xBytes = Base64.getUrlDecoder().decode(encodedXbytes);
        byte[] yBytes = Base64.getUrlDecoder().decode(encodedYbytes);
        try {
            BigInteger x = new BigInteger(1, xBytes);
            BigInteger y = new BigInteger(1, yBytes);
            ECPoint point = new ECPoint(x, y);

            AlgorithmParameters parameters = AlgorithmParameters.getInstance(EC_ALGORITHM);
            parameters.init(new ECGenParameterSpec(EC_CURVE));
            ECParameterSpec ecParameters = parameters.getParameterSpec(ECParameterSpec.class);
            ECPublicKeySpec pubKeySpec = new ECPublicKeySpec(point, ecParameters);
            return KeyFactory.getInstance(EC_ALGORITHM).generatePublic(pubKeySpec);
        } catch (NoSuchAlgorithmException | InvalidKeySpecException | InvalidParameterSpecException ex) {
            throw new IllegalArgumentException("Failed to decode " + encodedX + "," + encodedY, ex);
        }
    }

    private static boolean checkAllZeros(ByteBuffer signatureRaw) {
        int n = signatureRaw.limit();
        for (int i = signatureRaw.position(); i < n; i++) {
            if (signatureRaw.get(i) != 0) {
                return false;
            }
        }
        return true;
    }
}
