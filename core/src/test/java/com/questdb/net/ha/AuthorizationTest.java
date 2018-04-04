/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

import com.questdb.model.Quote;
import com.questdb.net.ha.config.ClientConfig;
import com.questdb.net.ha.config.ServerConfig;
import com.questdb.net.ha.config.ServerNode;
import com.questdb.net.ha.krb.SSOCredentialProvider;
import com.questdb.std.NumericException;
import com.questdb.std.ex.FatalError;
import com.questdb.std.ex.JournalException;
import com.questdb.std.ex.JournalNetworkException;
import com.questdb.store.Journal;
import com.questdb.store.JournalListener;
import com.questdb.store.JournalWriter;
import com.questdb.test.tools.AbstractTest;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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
                , getFactory()
                ,
                (token, requestedKeys) -> "SECRET".equals(new String(token)));


        JournalClient client = new JournalClient(local, getFactory(), "SECRET"::getBytes);
        beginSync(server, client);
    }

    @Test
    public void testClientWithoutAuthProvider() throws Exception {
        JournalServer server = new JournalServer(
                new ServerConfig() {{
                    setHeartbeatFrequency(TimeUnit.MILLISECONDS.toMillis(500));
                    setEnableMultiCast(false);
                }}
                , getFactory()
                ,
                (token, requestedKeys) -> "SECRET".equals(new String(token)));

        server.start();
        try {

            final AtomicInteger authErrors = new AtomicInteger();
            final CountDownLatch error = new CountDownLatch(1);
            JournalClient client = new JournalClient(local, getFactory(), null, evt -> {
                switch (evt) {
                    case JournalClientEvents.EVT_AUTH_CONFIG_ERROR:
                        authErrors.incrementAndGet();
                        break;
                    case JournalClientEvents.EVT_TERMINATED:
                        error.countDown();
                        break;
                    default:
                        break;
                }
            });

            client.start();
            Assert.assertTrue(error.await(5, TimeUnit.SECONDS));
            Assert.assertFalse(client.isRunning());
        } finally {
            server.halt();
        }
    }

    @Test
    public void testClientWrongAuth() throws Exception {
        JournalServer server = new JournalServer(
                new ServerConfig() {{
                    setHeartbeatFrequency(TimeUnit.MILLISECONDS.toMillis(500));
                    setEnableMultiCast(false);
                }}
                , getFactory()
                ,
                (token, requestedKeys) -> "SECRET".equals(new String(token)));


        final AtomicInteger authErrorCount = new AtomicInteger();
        final CountDownLatch serverError = new CountDownLatch(1);

        JournalClient client = new JournalClient(
                local,
                getFactory(),
                "NON_SECRET"::getBytes,
                evt -> {
                    switch (evt) {
                        case JournalClientEvents.EVT_AUTH_ERROR:
                            authErrorCount.incrementAndGet();
                            break;
                        case JournalClientEvents.EVT_TERMINATED:
                            serverError.countDown();
                            break;
                        default:
                            break;
                    }
                });

        server.start();
        try {
            client.start();
            Assert.assertTrue(serverError.await(5, TimeUnit.SECONDS));
            Assert.assertFalse(client.isRunning());
            Assert.assertEquals(1, authErrorCount.get());
        } finally {
            server.halt();
        }

    }

    @Test
    public void testExceptionInCredentialProvider() throws Exception {
        JournalServer server = new JournalServer(
                new ServerConfig() {{
                    setHeartbeatFrequency(TimeUnit.MILLISECONDS.toMillis(500));
                    setEnableMultiCast(false);
                }}
                , getFactory()
                ,
                (token, requestedKeys) -> "SECRET".equals(new String(token)));


        final AtomicInteger authErrorCount = new AtomicInteger();
        final CountDownLatch terminated = new CountDownLatch(1);
        JournalClient client = new JournalClient(local, getFactory(), new SSOCredentialProvider("HOST/test"),
                evt -> {
                    switch (evt) {
                        case JournalClientEvents.EVT_AUTH_CONFIG_ERROR:
                            authErrorCount.incrementAndGet();
                            break;
                        case JournalClientEvents.EVT_TERMINATED:
                            terminated.countDown();
                            break;
                        default:
                            break;
                    }
                });

        server.start();
        try {
            client.start();
            Assert.assertTrue(terminated.await(5, TimeUnit.SECONDS));
            Assert.assertEquals(1, authErrorCount.get());
            Assert.assertFalse(client.isRunning());
        } finally {
            server.halt();
        }
    }

    @Test
    public void testServerAuthException() throws Exception {
        JournalServer server = new JournalServer(
                new ServerConfig() {{
                    setHeartbeatFrequency(TimeUnit.MILLISECONDS.toMillis(500));
                    setEnableMultiCast(false);
                }}
                , getFactory()
                ,
                (token, requestedKeys) -> {
                    throw new FatalError("BANG!");
                });

        final AtomicInteger authErrorCount = new AtomicInteger();
        final CountDownLatch serverError = new CountDownLatch(1);

        JournalClient client = new JournalClient(local, getFactory(), "SECRET"::getBytes, evt -> {
            switch (evt) {
                case JournalClientEvents.EVT_AUTH_ERROR:
                    authErrorCount.incrementAndGet();
                    break;
                case JournalClientEvents.EVT_TERMINATED:
                    serverError.countDown();
                    break;
                default:
                    break;
            }

        });


        server.start();
        try {
            client.start();
            Assert.assertTrue(serverError.await(5, TimeUnit.SECONDS));
            Assert.assertFalse(client.isRunning());
            Assert.assertEquals(1, authErrorCount.get());
        } finally {
            server.halt();
        }
    }

    private void beginSync(JournalServer server, JournalClient client) throws JournalException, JournalNetworkException, InterruptedException, NumericException {
        int size = 100000;
        try (JournalWriter<Quote> remote = getFactory().writer(Quote.class, "remote", 2 * size)) {
            server.publish(remote);
            server.start();
            try {
                final CountDownLatch latch = new CountDownLatch(1);
                client.subscribe(Quote.class, "remote", "local", 2 * size, new JournalListener() {
                    @Override
                    public void onCommit() {
                        latch.countDown();
                    }

                    @Override
                    public void onEvent(int event) {

                    }
                });

                client.start();

                try {
                    TestUtils.generateQuoteData(remote, size);

                    latch.await();

                    try (Journal<Quote> local = getFactory().reader(Quote.class, "local")) {
                        TestUtils.assertDataEquals(remote, local);
                    }

                } finally {
                    client.halt();
                }
            } finally {
                server.halt(0, TimeUnit.SECONDS);
            }
        }
    }
}
