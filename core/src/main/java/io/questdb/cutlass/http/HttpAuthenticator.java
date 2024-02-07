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

package io.questdb.cutlass.http;

import io.questdb.cairo.SecurityContext;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.Nullable;

public interface HttpAuthenticator extends QuietCloseable {

    /**
     * Authenticates incoming HTTP request.
     *
     * @param headers request headers
     * @return true if the authentication succeeded, false - otherwise
     */
    boolean authenticate(HttpRequestHeader headers);

    default void clear() {
    }

    @Override
    default void close() {
    }

    default byte getAuthType() {
        return SecurityContext.AUTH_TYPE_NONE;
    }

    /**
     * Returns list of groups provided by external identity provider, such as OpenID Connect provider.
     * For other authentication types returns null.
     */
    @Nullable ObjList<CharSequence> getGroups();

    CharSequence getPrincipal();
}
