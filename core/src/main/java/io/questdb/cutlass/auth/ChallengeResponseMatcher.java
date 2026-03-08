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

public interface ChallengeResponseMatcher {

    /**
     * Verify a signature of a challenge. The signature is expected to be encoded in base64. The challenge is taken as is.
     * <p>
     * This also returns false if user does not have an active JWK token or when username is null or empty.
     *
     * @param username           user name
     * @param challengePtr       pointer to challenge
     * @param challengeLen       length of challenge
     * @param base64SignaturePtr pointer to base64 encoded signature
     * @param base64SignatureLen length of base64 encoded signature
     * @return true if signature matches
     */
    boolean verifyJwk(CharSequence username, long challengePtr, int challengeLen, long base64SignaturePtr, int base64SignatureLen);
}
