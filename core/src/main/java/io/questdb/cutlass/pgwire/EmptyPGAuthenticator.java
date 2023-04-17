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

package io.questdb.cutlass.pgwire;

import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.security.CairoSecurityContextFactory;

public final class EmptyPGAuthenticator implements PGAuthenticator {
    private final boolean readOnly;
    private final CairoSecurityContextFactory securityContextFactory;

    // todo: this is not meant to be used in production
    // it's for testing authorization only
    public EmptyPGAuthenticator(CairoSecurityContextFactory securityContextFactory, boolean readOnly) {
        this.securityContextFactory = securityContextFactory;
        this.readOnly = readOnly;
    }

    @Override
    public SecurityContext authenticate(CharSequence username, long msg, long msgLimit) throws BadProtocolException, AuthenticationException {
        return securityContextFactory.getInstance(username, readOnly);
    }
}
