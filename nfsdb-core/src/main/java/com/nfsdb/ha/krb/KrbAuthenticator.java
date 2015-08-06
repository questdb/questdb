/*
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.ha.krb;

import com.nfsdb.JournalKey;
import com.nfsdb.collections.ObjList;
import com.nfsdb.ha.auth.AuthorizationHandler;

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
