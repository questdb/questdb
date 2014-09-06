/*
 * Copyright (c) 2014. Vlad Ilyushchenko
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

package com.nfsdb.journal.net.config;

import com.nfsdb.journal.exceptions.JournalNetworkException;

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

    public void setKeyStore(InputStream stream, String password) throws UnrecoverableKeyException, CertificateException, NoSuchAlgorithmException, KeyStoreException, IOException {
        this.setKeyStore("JKS", stream, password);
    }

    public void setKeyStore(String type, InputStream stream, String password) throws UnrecoverableKeyException, CertificateException, NoSuchAlgorithmException, KeyStoreException, IOException {
        this.setKeyStore(type, stream, password, null, password);
    }

    public void setKeyStore(String type, InputStream stream, String password, String alias, String aliasPassword) throws KeyStoreException, CertificateException, NoSuchAlgorithmException, IOException, UnrecoverableKeyException {
        KeyStore keyStore = loadKeyStore(type, stream, password, alias);
        keyManagerFactory = KeyManagerFactory.getInstance("SunX509");
        keyManagerFactory.init(keyStore, aliasPassword == null ? null : aliasPassword.toCharArray());
    }

    public void setTrustStore(InputStream stream, String password) throws KeyStoreException, CertificateException, NoSuchAlgorithmException, IOException {
        setTrustStore("JKS", stream, password, null);
    }

    public void setTrustStore(String type, InputStream stream, String password, String alias) throws KeyStoreException, CertificateException, NoSuchAlgorithmException, IOException {
        KeyStore keyStore = loadKeyStore(type, stream, password, alias);
        trustManagerFactory = TrustManagerFactory.getInstance("SunX509");
        trustManagerFactory.init(keyStore);
    }

    public boolean isRequireClientAuth() {
        return requireClientAuth;
    }

    public void setRequireClientAuth(boolean requireClientAuth) {
        this.requireClientAuth = requireClientAuth;
    }

    public SSLContext getSslContext() throws JournalNetworkException {
        if (sslContext == null) {
            sslContext = createSSLContext();
        }
        return sslContext;
    }

    public boolean isSecure() {
        return secure;
    }

    public void setSecure(boolean secure) {
        this.secure = secure;
    }

    public boolean isClient() {
        return client;
    }

    public void setClient(boolean client) {
        this.client = client;
    }

    public boolean isTrustAll() {
        return trustAll;
    }

    public void setTrustAll(boolean trustAll) {
        this.trustAll = trustAll;
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

            for (String s : aliases) {
                if (!s.equals(alias)) {
                    keyStore.deleteEntry(s);
                }
            }
        }

        return keyStore;
    }

    private SSLContext createSSLContext() throws JournalNetworkException {
        try {
            SSLContext sslContext = SSLContext.getInstance("TLS");
            SecureRandom sr = new SecureRandom();
            sr.nextInt();
            sslContext.init(keyManagerFactory != null ? keyManagerFactory.getKeyManagers() : null
                    , trustManagerFactory != null ? trustManagerFactory.getTrustManagers() : (trustAll ? allowAllTrustManagers : null), sr);
            return sslContext;
        } catch (Exception e) {
            throw new JournalNetworkException(e);
        }
    }

    private final static class AllowAllTrustManager implements X509TrustManager {
        @Override
        public void checkClientTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {
        }

        @Override
        public void checkServerTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {
        }

        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
        }
    }
}
