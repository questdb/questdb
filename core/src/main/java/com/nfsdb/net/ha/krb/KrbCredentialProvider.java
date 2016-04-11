/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (C) 2014-2016 Appsicle
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
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 *
 ******************************************************************************/

package com.nfsdb.net.ha.krb;

import com.nfsdb.net.ha.auth.CredentialProvider;

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
