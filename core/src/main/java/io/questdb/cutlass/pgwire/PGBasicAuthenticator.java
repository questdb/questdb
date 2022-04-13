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

package io.questdb.cutlass.pgwire;

import io.questdb.cairo.CairoSecurityContext;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cairo.security.CairoSecurityContextImpl;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Chars;
import io.questdb.std.str.DirectByteCharSequence;

public class PGBasicAuthenticator implements PGAuthenticator {
    private static final Log LOG = LogFactory.getLog(PGBasicAuthenticator.class);

    private final String username;
    private final String password;
    private final DirectByteCharSequence dbcs = new DirectByteCharSequence();
    private final CairoSecurityContext securityContext;

    public PGBasicAuthenticator(String username, String password, boolean readOnlyContext) {
        this.username = username;
        this.password = password;
        this.securityContext = readOnlyContext ? new CairoSecurityContextImpl(false) : AllowAllCairoSecurityContext.INSTANCE;
    }

    @Override
    public CairoSecurityContext authenticate(
            CharSequence username,
            long msg,
            long msgLimit
    ) throws BadProtocolException, AuthenticationException {
        if (Chars.equals(this.username, username)) {
            // check 'p' message
            // +1 is 'type' byte that message length does not account for
            long hi = PGConnectionContext.getStringLength(msg, msgLimit, "bad password length");
            dbcs.of(msg, hi);

            // check password
            if (Chars.equals(this.password, dbcs)) {
                return securityContext;
            }
            LOG.error().$("invalid password [user=").$(username).$(']').$();
        } else {
            LOG.error().$("invalid user [").$(username).$(']').$();
        }
        // -1 position here means we will not send it to postgresql
        throw AuthenticationException.INSTANCE;
    }
}
