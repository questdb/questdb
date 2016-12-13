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
import com.questdb.JournalWriter;
import com.questdb.factory.configuration.JournalConfigurationBuilder;
import com.questdb.misc.Files;
import com.questdb.model.Quote;
import com.questdb.net.ha.config.ClientConfig;
import com.questdb.net.ha.config.ServerConfig;
import com.questdb.test.tools.JournalTestFactory;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.io.InputStream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class SSLTest {

    @Rule
    public final JournalTestFactory factory = new JournalTestFactory(new JournalConfigurationBuilder() {{
        $(Quote.class).recordCountHint(2000)
                .$sym("sym").valueCountHint(20)
                .$sym("mode")
                .$sym("ex")
        ;


    }}.build(Files.makeTempDir()));

    @Test
    public void testAuthBothCertsMissing() throws Exception {

        JournalServer server = new JournalServer(new ServerConfig() {{
            setHeartbeatFrequency(TimeUnit.MILLISECONDS.toMillis(500));
            getSslConfig().setSecure(true);
            getSslConfig().setRequireClientAuth(true);
            try (InputStream is = this.getClass().getResourceAsStream("/keystore/singlekey.ks")) {
                getSslConfig().setKeyStore(is, "changeit");
            }
            setEnableMultiCast(false);
            setHeartbeatFrequency(50);
        }}, factory);


        final AtomicInteger serverErrorCount = new AtomicInteger();
        final CountDownLatch terminated = new CountDownLatch(1);

        JournalClient client = new JournalClient(new ClientConfig("localhost") {{
            getSslConfig().setSecure(true);
            try (InputStream is = this.getClass().getResourceAsStream("/keystore/singlekey.ks")) {
                getSslConfig().setTrustStore(is, "changeit");
            }
        }}, factory, null, new JournalClient.Callback() {
            @Override
            public void onEvent(int evt) {
                switch (evt) {
                    case JournalClient.EVT_SERVER_ERROR:
                        serverErrorCount.incrementAndGet();
                        break;
                    case JournalClient.EVT_TERMINATED:
                        terminated.countDown();
                        break;
                    default:
                        break;
                }
            }
        });

        JournalWriter<Quote> remote = factory.writer(Quote.class, "remote");
        server.publish(remote);
        server.start();

        try {
            client.subscribe(Quote.class, "remote", "local");
            client.start();
            Assert.assertTrue(terminated.await(5, TimeUnit.SECONDS));
            Assert.assertEquals(0, server.getConnectedClients());
            Assert.assertFalse(client.isRunning());
            Assert.assertEquals(1, serverErrorCount.get());
        } finally {
            server.halt();
        }
    }

    @Test
    public void testClientAuth() throws Exception {
        int size = 2000;

        JournalServer server = new JournalServer(new ServerConfig() {{
            setHeartbeatFrequency(TimeUnit.MILLISECONDS.toMillis(500));
            getSslConfig().setSecure(true);
            getSslConfig().setRequireClientAuth(true);
            try (InputStream is = this.getClass().getResourceAsStream("/keystore/singlekey.ks")) {
                getSslConfig().setKeyStore(is, "changeit");
            }
            try (InputStream is = this.getClass().getResourceAsStream("/keystore/singlekey.ks")) {
                getSslConfig().setTrustStore(is, "changeit");
            }
            setEnableMultiCast(false);
            setHeartbeatFrequency(50);
        }}, factory);

        JournalClient client = new JournalClient(new ClientConfig("localhost") {{
            getSslConfig().setSecure(true);
            try (InputStream is = this.getClass().getResourceAsStream("/keystore/singlekey.ks")) {
                getSslConfig().setKeyStore(is, "changeit");
            }
            try (InputStream is = this.getClass().getResourceAsStream("/keystore/singlekey.ks")) {
                getSslConfig().setTrustStore(is, "changeit");
            }
        }}, factory);

        JournalWriter<Quote> remote = factory.writer(Quote.class, "remote");
        server.publish(remote);
        server.start();

        client.subscribe(Quote.class, "remote", "local");
        client.start();

        TestUtils.generateQuoteData(remote, size);
        Thread.sleep(1000);

        client.halt();
        server.halt();
        Journal<Quote> local = factory.reader(Quote.class, "local");
        TestUtils.assertDataEquals(remote, local);
    }

    @Test
    public void testNoCertTrustAllSSL() throws Exception {
        int size = 2000;

        JournalServer server = new JournalServer(new ServerConfig() {{
            setHeartbeatFrequency(TimeUnit.MILLISECONDS.toMillis(500));
            getSslConfig().setSecure(true);
            try (InputStream is = this.getClass().getResourceAsStream("/keystore/singlekey.ks")) {
                getSslConfig().setKeyStore(is, "changeit");
            }
            setEnableMultiCast(false);
            setHeartbeatFrequency(50);
        }}, factory);

        JournalClient client = new JournalClient(new ClientConfig("localhost") {{
            getSslConfig().setSecure(true);
            getSslConfig().setTrustAll(true);
        }}, factory);

        JournalWriter<Quote> remote = factory.writer(Quote.class, "remote");
        server.publish(remote);
        server.start();

        client.subscribe(Quote.class, "remote", "local");
        client.start();

        TestUtils.generateQuoteData(remote, size);
        Thread.sleep(1000);

        client.halt();
        server.halt();
        Journal<Quote> local = factory.reader(Quote.class, "local");
        TestUtils.assertDataEquals(remote, local);
    }

    @Test
    public void testNonAuthClientTrustMissing() throws Exception {
        JournalServer server = new JournalServer(new ServerConfig() {{
            setHeartbeatFrequency(TimeUnit.MILLISECONDS.toMillis(500));
            getSslConfig().setSecure(true);
            try (InputStream is = this.getClass().getResourceAsStream("/keystore/singlekey.ks")) {
                getSslConfig().setKeyStore(is, "changeit");
            }
            setEnableMultiCast(false);
            setHeartbeatFrequency(50);
        }}, factory);


        final AtomicInteger serverErrorCount = new AtomicInteger();
        final CountDownLatch terminated = new CountDownLatch(1);

        JournalClient client = new JournalClient(new ClientConfig("localhost") {{
            getSslConfig().setSecure(true);
        }}, factory, null, new JournalClient.Callback() {
            @Override
            public void onEvent(int evt) {
                switch (evt) {
                    case JournalClient.EVT_SERVER_ERROR:
                        serverErrorCount.incrementAndGet();
                        break;
                    case JournalClient.EVT_TERMINATED:
                        terminated.countDown();
                        break;
                    default:
                        break;
                }
            }
        });

        JournalWriter<Quote> remote = factory.writer(Quote.class, "remote");
        server.publish(remote);
        server.start();

        client.subscribe(Quote.class, "remote", "local");

        try {
            client.subscribe(Quote.class, "remote", "local");
            client.start();
            Assert.assertTrue(terminated.await(5, TimeUnit.SECONDS));
//            Assert.assertEquals(0, server.getConnectedClients());
            Assert.assertFalse(client.isRunning());
            Assert.assertEquals(1, serverErrorCount.get());
        } finally {
            server.halt();
        }
    }

    @Test
    public void testServerTrustMissing() throws Exception {
        JournalServer server = new JournalServer(new ServerConfig() {{
            setHeartbeatFrequency(TimeUnit.MILLISECONDS.toMillis(500));
            getSslConfig().setSecure(true);
            getSslConfig().setRequireClientAuth(true);
            try (InputStream is = this.getClass().getResourceAsStream("/keystore/singlekey.ks")) {
                getSslConfig().setKeyStore(is, "changeit");
            }
            setEnableMultiCast(false);
            setHeartbeatFrequency(50);
        }}, factory);


        final AtomicInteger serverErrorCount = new AtomicInteger();
        final CountDownLatch terminated = new CountDownLatch(1);

        JournalClient client = new JournalClient(new ClientConfig("localhost") {{
            getSslConfig().setSecure(true);
            try (InputStream is = this.getClass().getResourceAsStream("/keystore/singlekey.ks")) {
                getSslConfig().setTrustStore(is, "changeit");
            }
            try (InputStream is = this.getClass().getResourceAsStream("/keystore/singlekey.ks")) {
                getSslConfig().setKeyStore(is, "changeit");
            }
        }}, factory, null, new JournalClient.Callback() {
            @Override
            public void onEvent(int evt) {
                switch (evt) {
                    case JournalClient.EVT_SERVER_ERROR:
                        serverErrorCount.incrementAndGet();
                        break;
                    case JournalClient.EVT_TERMINATED:
                        terminated.countDown();
                        break;
                    default:
                        break;
                }
            }
        });

        JournalWriter<Quote> remote = factory.writer(Quote.class, "remote");
        server.publish(remote);
        server.start();
        try {
            client.subscribe(Quote.class, "remote", "local");
            client.start();
            Assert.assertTrue(terminated.await(5, TimeUnit.SECONDS));
            Assert.assertFalse(client.isRunning());
            Assert.assertEquals(1, serverErrorCount.get());
        } finally {
            server.halt();
        }
    }

    @Test
    public void testSingleKeySSL() throws Exception {
        int size = 1000;

        JournalServer server = new JournalServer(new ServerConfig() {{
            setHeartbeatFrequency(TimeUnit.MILLISECONDS.toMillis(500));
            getSslConfig().setSecure(true);
            try (InputStream is = this.getClass().getResourceAsStream("/keystore/singlekey.ks")) {
                getSslConfig().setKeyStore(is, "changeit");
            }
            setEnableMultiCast(false);
            setHeartbeatFrequency(50);
        }}, factory);

        JournalClient client = new JournalClient(new ClientConfig("localhost") {{
            setTcpNoDelay(false);
            try (InputStream is = this.getClass().getResourceAsStream("/keystore/singlekey.ks")) {
                getSslConfig().setTrustStore(is, "changeit");
            }
            getSslConfig().setSecure(true);
        }}, factory);

        JournalWriter<Quote> remote = factory.writer(Quote.class, "remote");
        server.publish(remote);
        server.start();

        client.subscribe(Quote.class, "remote", "local");
        client.start();

        TestUtils.generateQuoteData(remote, size);
        Thread.sleep(500);

        client.halt();
        server.halt();
        Journal<Quote> local = factory.reader(Quote.class, "local");
        TestUtils.assertDataEquals(remote, local);
    }
}
