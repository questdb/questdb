/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
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

package com.nfsdb.net;

import com.nfsdb.Journal;
import com.nfsdb.JournalKey;
import com.nfsdb.JournalWriter;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.exceptions.JournalNetworkException;
import com.nfsdb.model.Quote;
import com.nfsdb.net.auth.AuthorizationHandler;
import com.nfsdb.net.auth.CredentialProvider;
import com.nfsdb.net.config.ClientConfig;
import com.nfsdb.net.config.ServerConfig;
import com.nfsdb.net.config.ServerNode;
import com.nfsdb.net.krb.SSOCredentialProvider;
import com.nfsdb.test.tools.AbstractTest;
import com.nfsdb.test.tools.TestUtils;
import com.nfsdb.tx.TxListener;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class AuthorizationTest extends AbstractTest {

    private final ClientConfig local = new ClientConfig("localhost") {{
        addNode(new ServerNode(1, "xyz"));
        addNode(new ServerNode(2, "localhost"));
    }};

    @Test
    public void testClientAndServerSuccessfulAuth() throws Exception {

        JournalServer server = new JournalServer(
                new ServerConfig() {{
                    setHeartbeatFrequency(TimeUnit.MILLISECONDS.toMillis(100));
                    setEnableMultiCast(false);
                }}
                , factory
                ,
                new AuthorizationHandler() {
                    @Override
                    public boolean isAuthorized(byte[] token, List<JournalKey> requestedKeys) {
                        return "SECRET".equals(new String(token));
                    }
                });


        JournalClient client = new JournalClient(local, factory, new CredentialProvider() {
            @Override
            public byte[] createToken() {
                return "SECRET".getBytes();
            }
        });


        beginSync(server, client);
    }

    @Test
    public void testClientWrongAuth() throws Exception {
        JournalServer server = new JournalServer(
                new ServerConfig() {{
                    setHeartbeatFrequency(TimeUnit.MILLISECONDS.toMillis(500));
                    setEnableMultiCast(false);
                }}
                , factory
                ,
                new AuthorizationHandler() {
                    @Override
                    public boolean isAuthorized(byte[] token, List<JournalKey> requestedKeys) {
                        return "SECRET".equals(new String(token));
                    }
                });


        JournalClient client = new JournalClient(local, factory, new CredentialProvider() {
            @Override
            public byte[] createToken() {
                return "NON_SECRET".getBytes();
            }
        });

        try {
            beginSync(server, client);
        } catch (JournalNetworkException e) {
            Assert.assertEquals("Authorization failed", e.getMessage());
        }
    }

    @Test
    public void testExceptionInCredentialProvider() throws Exception {
        JournalServer server = new JournalServer(
                new ServerConfig() {{
                    setHeartbeatFrequency(TimeUnit.MILLISECONDS.toMillis(500));
                    setEnableMultiCast(false);
                }}
                , factory
                ,
                new AuthorizationHandler() {
                    @Override
                    public boolean isAuthorized(byte[] token, List<JournalKey> requestedKeys) {
                        return "SECRET".equals(new String(token));
                    }
                });


        JournalClient client = new JournalClient(local, factory, new SSOCredentialProvider("HOST/test"));

        try {
            beginSync(server, client);
        } catch (JournalNetworkException e) {
            Assert.assertTrue(e.getMessage().startsWith("java.io.IOException: ERROR") || e.getMessage().startsWith("java.io.IOException: ActiveDirectory SSO"));
        }
    }

    @Test
    public void testServerAuthException() throws Exception {
        JournalServer server = new JournalServer(
                new ServerConfig() {{
                    setHeartbeatFrequency(TimeUnit.MILLISECONDS.toMillis(500));
                    setEnableMultiCast(false);
                }}
                , factory
                ,
                new AuthorizationHandler() {
                    @Override
                    public boolean isAuthorized(byte[] token, List<JournalKey> requestedKeys) {
                        throw new RuntimeException("BANG!");
                    }
                });


        JournalClient client = new JournalClient(local, factory, new CredentialProvider() {
            @Override
            public byte[] createToken() {
                return "SECRET".getBytes();
            }
        });

        try {
            beginSync(server, client);
        } catch (JournalNetworkException e) {
            Assert.assertEquals("Authorization failed", e.getMessage());
        }
    }

    @Test
    public void testSillyClient() throws Exception {
        JournalServer server = new JournalServer(
                new ServerConfig() {{
                    setHeartbeatFrequency(TimeUnit.MILLISECONDS.toMillis(500));
                    setEnableMultiCast(false);
                }}
                , factory
                ,
                new AuthorizationHandler() {
                    @Override
                    public boolean isAuthorized(byte[] token, List<JournalKey> requestedKeys) {
                        return "SECRET".equals(new String(token));
                    }
                });


        JournalClient client = new JournalClient(local, factory);

        try {
            beginSync(server, client);
        } catch (JournalNetworkException e) {
            Assert.assertEquals("Server requires authentication. Supply CredentialProvider.", e.getMessage());
        }
    }

    private void beginSync(JournalServer server, JournalClient client) throws JournalException, JournalNetworkException, InterruptedException {
        int size = 100000;
        JournalWriter<Quote> remote = factory.writer(Quote.class, "remote", 2 * size);
        server.publish(remote);
        server.start();
        try {
            final CountDownLatch latch = new CountDownLatch(1);
            client.subscribe(Quote.class, "remote", "local", 2 * size, new TxListener() {
                @Override
                public void onCommit() {
                    latch.countDown();
                }

                @Override
                public void onError() {

                }
            });

            client.start();

            try {
                TestUtils.generateQuoteData(remote, size);

                latch.await();

                Journal<Quote> local = factory.reader(Quote.class, "local");
                TestUtils.assertDataEquals(remote, local);

            } finally {
                client.halt();
            }
        } finally {
            server.halt(0, TimeUnit.SECONDS);
        }
    }
}
