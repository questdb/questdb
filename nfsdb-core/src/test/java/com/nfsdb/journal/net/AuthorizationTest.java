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

package com.nfsdb.journal.net;

import com.nfsdb.journal.Journal;
import com.nfsdb.journal.JournalKey;
import com.nfsdb.journal.JournalWriter;
import com.nfsdb.journal.exceptions.JournalException;
import com.nfsdb.journal.exceptions.JournalNetworkException;
import com.nfsdb.journal.model.Quote;
import com.nfsdb.journal.net.auth.Authorizer;
import com.nfsdb.journal.net.auth.CredentialProvider;
import com.nfsdb.journal.net.config.ClientConfig;
import com.nfsdb.journal.net.config.ServerConfig;
import com.nfsdb.journal.net.krb.SSOCredentialProvider;
import com.nfsdb.journal.test.tools.AbstractTest;
import com.nfsdb.journal.test.tools.TestUtils;
import com.nfsdb.journal.tx.TxListener;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class AuthorizationTest extends AbstractTest {
    @Test
    public void testClientAndServerSuccessfulAuth() throws Exception {

        JournalServer server = new JournalServer(
                new ServerConfig() {{
                    setHostname("localhost");
                    setHeartbeatFrequency(TimeUnit.MILLISECONDS.toMillis(500));
                }}
                , factory
                ,
                new Authorizer() {
                    @Override
                    public boolean isAuthorized(byte[] token, List<JournalKey> requestedKeys) {
                        return "SECRET".equals(new String(token));
                    }
                });


        JournalClient client = new JournalClient(ClientConfig.INSTANCE, factory, new CredentialProvider() {
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
                    setHostname("localhost");
                    setHeartbeatFrequency(TimeUnit.MILLISECONDS.toMillis(500));
                }}
                , factory
                ,
                new Authorizer() {
                    @Override
                    public boolean isAuthorized(byte[] token, List<JournalKey> requestedKeys) {
                        return "SECRET".equals(new String(token));
                    }
                });


        JournalClient client = new JournalClient(ClientConfig.INSTANCE, factory, new CredentialProvider() {
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
    public void testServerAuthException() throws Exception {
        JournalServer server = new JournalServer(
                new ServerConfig() {{
                    setHostname("localhost");
                    setHeartbeatFrequency(TimeUnit.MILLISECONDS.toMillis(500));
                }}
                , factory
                ,
                new Authorizer() {
                    @Override
                    public boolean isAuthorized(byte[] token, List<JournalKey> requestedKeys) {
                        throw new RuntimeException("BANG!");
                    }
                });


        JournalClient client = new JournalClient(ClientConfig.INSTANCE, factory, new CredentialProvider() {
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
                    setHostname("localhost");
                    setHeartbeatFrequency(TimeUnit.MILLISECONDS.toMillis(500));
                }}
                , factory
                ,
                new Authorizer() {
                    @Override
                    public boolean isAuthorized(byte[] token, List<JournalKey> requestedKeys) {
                        return "SECRET".equals(new String(token));
                    }
                });


        JournalClient client = new JournalClient(ClientConfig.INSTANCE, factory);

        try {
            beginSync(server, client);
        } catch (JournalNetworkException e) {
            Assert.assertEquals("Server requires authentication. Supply CredentialProvider.", e.getMessage());
        }
    }

    @Test
    public void testExceptionInCredentialProvider() throws Exception {
        JournalServer server = new JournalServer(
                new ServerConfig() {{
                    setHostname("localhost");
                    setHeartbeatFrequency(TimeUnit.MILLISECONDS.toMillis(500));
                }}
                , factory
                ,
                new Authorizer() {
                    @Override
                    public boolean isAuthorized(byte[] token, List<JournalKey> requestedKeys) {
                        return "SECRET".equals(new String(token));
                    }
                });


        JournalClient client = new JournalClient(ClientConfig.INSTANCE, factory, new SSOCredentialProvider("HOST/test"));

        try {
            beginSync(server, client);
        } catch (JournalNetworkException e) {
            Assert.assertTrue(e.getMessage().startsWith("java.io.IOException: ERROR"));
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
            server.halt();
        }
    }
}
