/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2017 Appsicle
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

package com.questdb.net.ha.krb;

import com.questdb.net.ha.auth.AuthorizationHandler;
import com.questdb.std.ObjList;
import com.questdb.store.JournalKey;

public class KrbAuthenticator implements AuthorizationHandler {

    private final String krb5Conf;
    private final String principal;
    private final String keyTab;
    private final String serviceName;
    private final KrbAuthorizer authorizer;

    public KrbAuthenticator(String krb5Conf, String principal, String keyTab, String serviceName, KrbAuthorizer authorizer) {
        this.krb5Conf = krb5Conf;
        this.principal = principal;
        this.keyTab = keyTab;
        this.serviceName = serviceName;
        this.authorizer = authorizer;
    }

    @Override
    public boolean isAuthorized(byte[] token, ObjList<JournalKey> requestedKeys) throws Exception {
        try (ActiveDirectoryConnection connection = new ActiveDirectoryConnection(krb5Conf, principal, keyTab)) {
            String principal = connection.decodeServiceToken(serviceName, token);
            return authorizer != null ? authorizer.isAuthorized(principal, requestedKeys) : principal != null;
        }
    }
}
