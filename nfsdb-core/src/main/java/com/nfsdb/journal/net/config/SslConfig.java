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
import java.io.IOException;
import java.io.InputStream;
import java.security.*;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

public class SslConfig {
    private boolean secure = false;
    private boolean requireClientAuth = false;
    private SSLContext sslContext;
    private KeyManagerFactory keyManagerFactory;
    private TrustManagerFactory trustManagerFactory;

    public void setKeyStore(InputStream stream, String password) throws UnrecoverableKeyException, CertificateException, NoSuchAlgorithmException, KeyStoreException, IOException {
        this.setKeyStore("JKS", stream, password);
    }

    public void setKeyStore(String type, InputStream stream, String password) throws UnrecoverableKeyException, CertificateException, NoSuchAlgorithmException, KeyStoreException, IOException {
        this.setKeyStore(type, stream, password, null, password);
    }

    public void setKeyStore(String type, InputStream stream, String password, String alias, String aliasPassword) throws KeyStoreException, CertificateException, NoSuchAlgorithmException, IOException, UnrecoverableKeyException {
        if (stream == null) {
            throw new KeyStoreException("NULL key store");
        }

        KeyStore ksKeys = KeyStore.getInstance(type);
        ksKeys.load(stream, password == null ? null : password.toCharArray());

        // delete aliases other than the one we need

        if (alias != null) {
            List<String> aliases = new ArrayList<>();
            for (Enumeration<String> e = ksKeys.aliases(); e.hasMoreElements(); ) {
                aliases.add(e.nextElement());
            }

            for (String s : aliases) {
                if (!s.equals(alias)) {
                    ksKeys.deleteEntry(s);
                }
            }
        }
        keyManagerFactory = KeyManagerFactory.getInstance("SunX509");
        keyManagerFactory.init(ksKeys, aliasPassword == null ? null : aliasPassword.toCharArray());
    }

    public void setTrustStore(String type, InputStream stream, String password) throws KeyStoreException, CertificateException, NoSuchAlgorithmException, IOException {
        KeyStore ksTrust = KeyStore.getInstance(type);
        ksTrust.load(stream, password == null ? null : password.toCharArray());
        trustManagerFactory = TrustManagerFactory.getInstance("SunX509");
        trustManagerFactory.init(ksTrust);
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

    public void setSslContext(SSLContext sslContext) {
        this.sslContext = sslContext;
    }

    public boolean isSecure() {
        return secure;
    }

    public void setSecure(boolean secure) {
        this.secure = secure;
    }

    private SSLContext createSSLContext() throws JournalNetworkException {
        try {
            SSLContext sslContext = SSLContext.getInstance("TLS");
            SecureRandom sr = new SecureRandom();
            sr.nextInt();
            sslContext.init(keyManagerFactory != null ? keyManagerFactory.getKeyManagers() : null
                    , trustManagerFactory != null ? trustManagerFactory.getTrustManagers() : null, sr);

            return sslContext;
        } catch (Exception e) {
            throw new JournalNetworkException(e);
        }
    }
}
