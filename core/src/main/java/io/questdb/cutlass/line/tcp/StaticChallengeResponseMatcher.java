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

package io.questdb.cutlass.line.tcp;

import io.questdb.cutlass.auth.AuthUtils;
import io.questdb.cutlass.auth.ChallengeResponseMatcher;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.Chars;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectUtf8String;

import java.nio.ByteBuffer;
import java.security.InvalidKeyException;
import java.security.PublicKey;
import java.security.SignatureException;

public class StaticChallengeResponseMatcher implements ChallengeResponseMatcher {
    private static final Log LOG = LogFactory.getLog(StaticChallengeResponseMatcher.class);
    private final byte[] challengeBytes = new byte[AuthUtils.CHALLENGE_LEN];
    private final CharSequenceObjHashMap<PublicKey> publicKeyByKeyId;
    private final ByteBuffer signatureBuffer = ByteBuffer.allocate(AuthUtils.MAX_SIGNATURE_LENGTH);
    private final DirectUtf8String signatureFlyweight = new DirectUtf8String();

    public StaticChallengeResponseMatcher(CharSequenceObjHashMap<PublicKey> authDb) {
        this.publicKeyByKeyId = authDb;
    }

    @Override
    public boolean verifyJwk(CharSequence username, long challengePtr, int challengeLen, long signaturePtr, int signatureLen) {
        assert challengeLen == AuthUtils.CHALLENGE_LEN;
        PublicKey publicKey = getPublicKey(username);
        if (publicKey == null) {
            LOG.info().$("authentication failed, unknown key [id=").$(username).$(']').$();
            return false;
        }
        signatureBuffer.clear();
        signatureFlyweight.of(signaturePtr, signaturePtr + signatureLen);
        Chars.base64Decode(signatureFlyweight.asAsciiCharSequence(), signatureBuffer);
        signatureBuffer.flip();
        for (int i = 0; i < challengeLen; i++) {
            challengeBytes[i] = Unsafe.getUnsafe().getByte(challengePtr + i);
        }
        try {
            return AuthUtils.isSignatureMatch(publicKey, challengeBytes, signatureBuffer);
        } catch (InvalidKeyException | SignatureException ex) {
            LOG.info().$(" authentication exception ").$(ex).$();
            return false;
        }
    }

    private PublicKey getPublicKey(CharSequence keyId) {
        return publicKeyByKeyId.get(keyId);
    }
}
