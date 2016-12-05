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
 ******************************************************************************/

package com.questdb.net.ha;

import com.questdb.Journal;
import com.questdb.JournalKey;
import com.questdb.JournalWriter;
import com.questdb.ex.FatalError;
import com.questdb.ex.JournalException;
import com.questdb.ex.JournalNetworkException;
import com.questdb.ex.NumericException;
import com.questdb.model.Quote;
import com.questdb.net.ha.auth.AuthorizationHandler;
import com.questdb.net.ha.auth.CredentialProvider;
import com.questdb.net.ha.config.ClientConfig;
import com.questdb.net.ha.config.ServerConfig;
import com.questdb.net.ha.config.ServerNode;
import com.questdb.net.ha.krb.SSOCredentialProvider;
import com.questdb.std.ObjList;
import com.questdb.store.TxListener;
import com.questdb.test.tools.AbstractTest;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

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
                    public boolean isAuthorized(byte[] token, ObjList<JournalKey> requestedKeys) {
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
                    public boolean isAuthorized(byte[] token, ObjList<JournalKey> requestedKeys) {
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
                    public boolean isAuthorized(byte[] token, ObjList<JournalKey> requestedKeys) {
                        return "SECRET".equals(new String(token));
                    }
                });


        JournalClient client = new JournalClient(local, factory, new SSOCredentialProvider("HOST/test"));

        try {
            beginSync(server, client);
            Assert.fail();
        } catch (JournalNetworkException ignore) {
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
                    public boolean isAuthorized(byte[] token, ObjList<JournalKey> requestedKeys) {
                        throw new FatalError("BANG!");
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
                    public boolean isAuthorized(byte[] token, ObjList<JournalKey> requestedKeys) {
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

    private void beginSync(JournalServer server, JournalClient client) throws JournalException, JournalNetworkException, InterruptedException, NumericException {
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
