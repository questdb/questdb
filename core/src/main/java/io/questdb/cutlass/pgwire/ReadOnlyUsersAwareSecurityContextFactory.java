/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.security.ReadOnlySecurityContext;
import io.questdb.cairo.security.SecurityContextFactory;
import io.questdb.std.Chars;
import io.questdb.std.ObjList;

public final class ReadOnlyUsersAwareSecurityContextFactory implements SecurityContextFactory {
    private final boolean httpReadOnly;
    private final boolean pgWireReadOnly;
    private final String pgWireReadOnlyUser;

    public ReadOnlyUsersAwareSecurityContextFactory(boolean pgWireReadOnly, String pgWireReadOnlyUser, boolean httpReadOnly) {
        this.pgWireReadOnly = pgWireReadOnly;
        this.pgWireReadOnlyUser = pgWireReadOnlyUser;
        this.httpReadOnly = httpReadOnly;
    }

    @Override
    public SecurityContext getInstance(CharSequence principal, ObjList<CharSequence> groups, byte authType, byte interfaceId) {
        switch (interfaceId) {
            case SecurityContextFactory.HTTP:
                return httpReadOnly ? ReadOnlySecurityContext.INSTANCE : AllowAllSecurityContext.INSTANCE;
            case SecurityContextFactory.PGWIRE:
                return isReadOnlyPgWireUser(principal) ? ReadOnlySecurityContext.INSTANCE : AllowAllSecurityContext.INSTANCE;
            default:
                return AllowAllSecurityContext.INSTANCE;
        }
    }

    private boolean isReadOnlyPgWireUser(CharSequence principal) {
        return pgWireReadOnly || (pgWireReadOnlyUser != null && principal != null && Chars.equals(pgWireReadOnlyUser, principal));
    }
}
