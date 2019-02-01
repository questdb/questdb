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

package com.questdb.net.ha.krb;

import com.questdb.net.ha.auth.CredentialProvider;

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
