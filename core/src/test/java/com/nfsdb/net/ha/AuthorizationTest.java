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

package com.nfsdb.net.ha;

import com.nfsdb.Journal;
import com.nfsdb.JournalKey;
import com.nfsdb.JournalWriter;
import com.nfsdb.ex.FatalError;
import com.nfsdb.ex.JournalException;
import com.nfsdb.ex.JournalNetworkException;
import com.nfsdb.ex.NumericException;
import com.nfsdb.model.Quote;
import com.nfsdb.net.ha.auth.AuthorizationHandler;
import com.nfsdb.net.ha.auth.CredentialProvider;
import com.nfsdb.net.ha.config.ClientConfig;
import com.nfsdb.net.ha.config.ServerConfig;
import com.nfsdb.net.ha.config.ServerNode;
import com.nfsdb.net.ha.krb.SSOCredentialProvider;
import com.nfsdb.std.ObjList;
import com.nfsdb.store.TxListener;
import com.nfsdb.test.tools.AbstractTest;
import com.nfsdb.test.tools.TestUtils;
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
