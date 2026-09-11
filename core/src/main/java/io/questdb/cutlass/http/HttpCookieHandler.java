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

import io.questdb.cairo.SecurityContext;

public interface HttpCookieHandler {
    /**
     * Formats the value of the session cookie, including its attributes.
     *
     * <p>The returned sequence may be backed by thread-local scratch and is
     * valid only until the next cookie-handler call on the current thread.
     * Callers that defer writing it must copy it first.</p>
     */
    default CharSequence getSessionCookieValue(CharSequence sessionId) {
        return null;
    }

    default boolean parseCookies(HttpConnectionContext context) {
        return true;
    }

    default boolean processServiceAccountCookie(HttpConnectionContext context, SecurityContext securityContext) {
        return true;
    }

    default CharSequence processSessionCookie(HttpConnectionContext context) {
        return null;
    }

    default void setServiceAccountCookie(HttpResponseHeader header, SecurityContext securityContext) {
    }

    default void setSessionCookie(HttpResponseHeader header, CharSequence sessionId) {
    }
}
