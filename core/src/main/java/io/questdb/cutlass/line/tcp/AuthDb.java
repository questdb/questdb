/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

import io.questdb.std.CharSequenceObjHashMap;
import org.jetbrains.annotations.NotNull;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.security.*;
import java.security.interfaces.ECKey;
import java.security.spec.*;
import java.util.Base64;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AuthDb {
    private static final Pattern TOKEN_PATTERN = Pattern.compile("\\s*(\\S+)(.*)");
    public static final String EC_ALGORITHM = "EC";
    public static final String EC_CURVE = "secp256r1";
    public static final String SIGNATURE_TYPE_DER = "SHA256withECDSA";
    public static final String SIGNATURE_TYPE_P1363 = "SHA256withECDSAinP1363Format";

    public static PrivateKey importPrivateKey(String encodedPrivateKey) {
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

    public static PublicKey importPublicKey(String encodedX, String encodedY) {
        byte[] xBytes = Base64.getUrlDecoder().decode(encodedX);
        byte[] yBytes = Base64.getUrlDecoder().decode(encodedY);
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

    private final CharSequenceObjHashMap<PublicKey> publicKeyByKeyId = new CharSequenceObjHashMap<>();

    AuthDb(LineTcpReceiverConfiguration configuration) {
        int nLine = 0;
        String[] tokens = new String[4];
        try (BufferedReader r = new BufferedReader(new InputStreamReader(new BufferedInputStream(new FileInputStream(configuration.getAuthDbPath()))))) {
            String line;
            do {
                int nTokens = 0;
                line = r.readLine();
                nLine++;
                while (null != line) {
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

                PublicKey publicKey = importPublicKey(tokens[2], tokens[3]);
                publicKeyByKeyId.put(keyId, publicKey);
            } while (null != line);
        } catch (Exception ex) {
            throw new IllegalArgumentException("IO error, failed to read auth db file " + configuration.getAuthDbPath() + " at line " + nLine, ex);
        }
    }

    public PublicKey getPublicKey(@NotNull CharSequence keyId) {
        if (keyId.length() == 0 && publicKeyByKeyId.size() == 1) {
            return publicKeyByKeyId.valueQuick(0);
        }
        return publicKeyByKeyId.get(keyId);
    }
}
