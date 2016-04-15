/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
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

package com.nfsdb.net;

import com.nfsdb.ex.NetworkError;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.io.InputStream;
import java.security.*;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

@SuppressFBWarnings({"LII_LIST_INDEXED_ITERATING"})
public class SslConfig {
    private static final X509TrustManager[] allowAllTrustManagers = new X509TrustManager[]{
            new AllowAllTrustManager()
    };
    private boolean secure = false;
    private boolean requireClientAuth = false;
    private SSLContext sslContext;
    private KeyManagerFactory keyManagerFactory;
    private TrustManagerFactory trustManagerFactory;
    private boolean client = false;
    private boolean trustAll = false;

    public SSLContext getSslContext() {
        if (sslContext == null) {
            sslContext = createSSLContext();
        }
        return sslContext;
    }

    public boolean isClient() {
        return client;
    }

    public void setClient(boolean client) {
        this.client = client;
    }

    public boolean isRequireClientAuth() {
        return requireClientAuth;
    }

    public void setRequireClientAuth(boolean requireClientAuth) {
        this.requireClientAuth = requireClientAuth;
    }

    public boolean isSecure() {
        return secure;
    }

    public void setSecure(boolean secure) {
        this.secure = secure;
    }

    public boolean isTrustAll() {
        return trustAll;
    }

    public void setTrustAll(boolean trustAll) {
        this.trustAll = trustAll;
    }

    public void setKeyStore(InputStream stream, String password) throws UnrecoverableKeyException, CertificateException, NoSuchAlgorithmException, KeyStoreException, IOException {
        this.setKeyStore("JKS", stream, password);
    }

    public void setTrustStore(InputStream stream, String password) throws KeyStoreException, CertificateException, NoSuchAlgorithmException, IOException {
        setTrustStore("JKS", stream, password, null);
    }

    private SSLContext createSSLContext() {
        try {
            SSLContext sslContext = SSLContext.getInstance("TLS");
            SecureRandom sr = new SecureRandom();
            sr.nextInt();
            sslContext.init(keyManagerFactory != null ? keyManagerFactory.getKeyManagers() : null
                    , trustManagerFactory != null ? trustManagerFactory.getTrustManagers() : (trustAll ? allowAllTrustManagers : null), sr);
            return sslContext;
        } catch (Exception e) {
            throw new NetworkError(e);
        }
    }

    private KeyStore loadKeyStore(String type, InputStream stream, String password, String alias) throws KeyStoreException, CertificateException, NoSuchAlgorithmException, IOException {
        if (stream == null) {
            throw new KeyStoreException("NULL key store");
        }

        KeyStore keyStore = KeyStore.getInstance(type);
        keyStore.load(stream, password == null ? null : password.toCharArray());

        // delete aliases other than the one we need

        if (alias != null) {
            List<String> aliases = new ArrayList<>();
            for (Enumeration<String> e = keyStore.aliases(); e.hasMoreElements(); ) {
                aliases.add(e.nextElement());
            }

            for (int i = 0; i < aliases.size(); i++) {
                if (!aliases.get(i).equals(alias)) {
                    keyStore.deleteEntry(aliases.get(i));
                }
            }
        }

        return keyStore;
    }

    private void setKeyStore(String type, InputStream stream, String password) throws UnrecoverableKeyException, CertificateException, NoSuchAlgorithmException, KeyStoreException, IOException {
        this.setKeyStore(type, stream, password, null, password);
    }

    private void setKeyStore(String type, InputStream stream, String password, String alias, String aliasPassword) throws KeyStoreException, CertificateException, NoSuchAlgorithmException, IOException, UnrecoverableKeyException {
        KeyStore keyStore = loadKeyStore(type, stream, password, alias);
        keyManagerFactory = KeyManagerFactory.getInstance("SunX509");
        keyManagerFactory.init(keyStore, aliasPassword == null ? null : aliasPassword.toCharArray());
    }

    private void setTrustStore(String type, InputStream stream, String password, String alias) throws KeyStoreException, CertificateException, NoSuchAlgorithmException, IOException {
        KeyStore keyStore = loadKeyStore(type, stream, password, alias);
        trustManagerFactory = TrustManagerFactory.getInstance("SunX509");
        trustManagerFactory.init(keyStore);
    }

    @SuppressFBWarnings({"WEAK_TRUST_MANAGER", "BED_BOGUS_EXCEPTION_DECLARATION"})
    private final static class AllowAllTrustManager implements X509TrustManager {
        @Override
        public void checkClientTrusted(X509Certificate[] x509Certificates, String s) {
        }

        @Override
        public void checkServerTrusted(X509Certificate[] x509Certificates, String s) {
        }

        @SuppressFBWarnings({"WEAK_TRUST_MANAGER"})
        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
        }
    }
}
