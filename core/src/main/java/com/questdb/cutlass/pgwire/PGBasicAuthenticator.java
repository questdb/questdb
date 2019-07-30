/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.cutlass.pgwire;

import com.questdb.cairo.CairoSecurityContext;
import com.questdb.cairo.security.AllowAllCairoSecurityContext;
import com.questdb.griffin.SqlException;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.std.Chars;
import com.questdb.std.str.DirectByteCharSequence;

public class PGBasicAuthenticator implements PGAuthenticator {
    private static final Log LOG = LogFactory.getLog(PGBasicAuthenticator.class);

    private final String username;
    private final String password;
    private final DirectByteCharSequence dbcs = new DirectByteCharSequence();

    public PGBasicAuthenticator(String username, String password) {
        this.username = username;
        this.password = password;
    }

    @Override
    public CairoSecurityContext authenticate(
            CharSequence username,
            long msg,
            long msgLimit
    ) throws BadProtocolException, SqlException {
        if (Chars.equals(this.username, username)) {
            // check 'p' message
            // +1 is 'type' byte that message length does not account for
            long hi = PGConnectionContext.getStringLength(msg, msgLimit);
            if (hi == -1) {
                // we did not find 0 within message limit
                LOG.error().$("bad password length").$();
                throw BadProtocolException.INSTANCE;
            }

            dbcs.of(msg, hi);

            // check password
            if (Chars.equals(this.password, dbcs)) {
                return AllowAllCairoSecurityContext.INSTANCE;
            }
            LOG.error().$("invalid password [user=").$(username).$(']').$();
        } else {
            LOG.error().$("invalid user [").$(username).$(']').$();
        }
        // -1 position here means we will not send it to postgresql
        throw SqlException.$(-1, "invalid username/password");
    }
}
