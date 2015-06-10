/*******************************************************************************
 *   _  _ ___ ___     _ _
 *  | \| | __/ __| __| | |__
 *  | .` | _|\__ \/ _` | '_ \
 *  |_|\_|_| |___/\__,_|_.__/
 *
 *  Copyright (c) 2014-2015. The NFSdb project and its contributors.
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
 ******************************************************************************/
package com.nfsdb.ha.krb;

import com.nfsdb.ha.auth.CredentialProvider;

public class KrbCredentialProvider implements CredentialProvider {

    private final String krb5Conf;
    private final String principal;
    private final String keyTab;
    private final String serviceName;

    public KrbCredentialProvider(String krb5Conf, String principal, String keyTab, String serviceName) {
        this.krb5Conf = krb5Conf;
        this.principal = principal;
        this.keyTab = keyTab;
        this.serviceName = serviceName;
    }

    @Override
    public byte[] createToken() throws Exception {
        try (ActiveDirectoryConnection connection = new ActiveDirectoryConnection(krb5Conf, principal, keyTab)) {
            return connection.encodeServiceToken(serviceName);
        }
    }
}
