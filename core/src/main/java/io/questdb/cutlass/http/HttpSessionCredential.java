/*+*****************************************************************************
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

package io.questdb.cutlass.http;

/**
 * A credential that authenticated the principal of an HTTP session, such as a password or a REST token.
 * <p>
 * A session keeps the credential it was opened with, and the one it was last authenticated with, and
 * checks both every time it is used. Revoking a credential, for example changing or removing a password,
 * or dropping a token, therefore ends the sessions the credential authenticated, even when the client
 * sends nothing but the session cookie.
 * <p>
 * Implementations must be immutable and thread-safe: a session holds its credentials for its whole
 * lifetime, and the concurrent requests of a session check them in parallel.
 */
@FunctionalInterface
public interface HttpSessionCredential {

    /**
     * Checks whether the credential would still authenticate the principal of the session.
     *
     * @return false once the credential has been revoked, true otherwise
     */
    boolean isValid();

    /**
     * Tells whether the session can vouch for the credential once it has verified it: a later request of the
     * session that carries the same credential is not verified again until the credential revalidation
     * interval passes. A credential whose verification has a side effect the session must not skip, such as
     * a token whose TTL slides each time it is verified, returns false, and is verified on every request.
     *
     * @return true if a verification of the credential can serve the later requests of the session
     */
    default boolean isVerificationReusable() {
        return true;
    }
}
