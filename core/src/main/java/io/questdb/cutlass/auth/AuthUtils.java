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

package io.questdb.cutlass.auth;

import io.questdb.std.Chars;

import java.math.BigInteger;
import java.security.*;
import java.security.interfaces.ECKey;
import java.security.spec.*;
import java.util.Base64;

public final class AuthUtils {
    public static final String EC_ALGORITHM = "EC";
    public static final String EC_CURVE = "secp256r1";
    public static final String SIGNATURE_TYPE_DER = "SHA256withECDSA";
    public static final String SIGNATURE_TYPE_P1363 = "SHA256withECDSAinP1363Format";

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
}
