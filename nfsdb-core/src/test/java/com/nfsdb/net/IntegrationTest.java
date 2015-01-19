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
import com.nfsdb.JournalWriter;
import com.nfsdb.exceptions.JournalNetworkException;
import com.nfsdb.model.Quote;
import com.nfsdb.model.TestEntity;
import com.nfsdb.net.config.ClientConfig;
import com.nfsdb.net.config.ServerConfig;
import com.nfsdb.test.tools.AbstractTest;
import com.nfsdb.test.tools.TestUtils;
import com.nfsdb.tx.TxListener;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class IntegrationTest extends AbstractTest {

    private JournalClient client;
    private JournalServer server;

    @Before
    public void setUp() throws Exception {
        server = new JournalServer(new ServerConfig() {{
            setHostname("localhost");
            setHeartbeatFrequency(TimeUnit.MILLISECONDS.toMillis(100));
            setEnableMulticast(false);
        }}, factory);
        client = new JournalClient(new ClientConfig("localhost"), factory);
    }

    @Test
    public void testSingleJournalSync() throws Exception {
        int size = 100000;
        JournalWriter<Quote> remote = factory.writer(Quote.class, "remote", 2 * size);
        server.publish(remote);
        server.start();

        final CountDownLatch latch = new CountDownLatch(1);
        client.subscribe(Quote.class, "remote", "local", 2 * size, new TxListener() {
            @Override
            public void onCommit() {
                latch.countDown();
            }
        });
        client.start();

        TestUtils.generateQuoteData(remote, size);

        latch.await();

        client.halt();
        server.halt();
        Journal<Quote> local = factory.reader(Quote.class, "local");
        TestUtils.assertDataEquals(remote, local);
    }

    @Test
    public void testTwoJournalsSync() throws Exception {
        int size = 10000;
        JournalWriter<Quote> remote1 = factory.writer(Quote.class, "remote1", 2 * size);
        JournalWriter<TestEntity> remote2 = factory.writer(TestEntity.class, "remote2", 2 * size);
        server.publish(remote1);
        server.publish(remote2);
        server.start();

        final CountDownLatch latch = new CountDownLatch(2);
        client.subscribe(Quote.class, "remote1", "local1", 2 * size, new TxListener() {
            @Override
            public void onCommit() {
                latch.countDown();
            }
        });

        client.subscribe(TestEntity.class, "remote2", "local2", 2 * size, new TxListener() {
            @Override
            public void onCommit() {
                latch.countDown();
            }
        });
        client.start();

        TestUtils.generateQuoteData(remote1, size);
        TestUtils.generateTestEntityData(remote2, size);

        latch.await();

        client.halt();
        server.halt();

        Journal<Quote> local1 = factory.reader(Quote.class, "local1");
        Assert.assertEquals("Local1 has wrong size", size, local1.size());

        Journal<TestEntity> local2 = factory.reader(TestEntity.class, "local2");
        Assert.assertEquals("Remote2 has wrong size", size, remote2.size());
        Assert.assertEquals("Local2 has wrong size", size, local2.size());
    }

    @Test
    public void testTwoClientsSync() throws Exception {
        int size = 10000;
        JournalWriter<Quote> origin = factory.writer(Quote.class, "origin");
        TestUtils.generateQuoteData(origin, size);

        JournalWriter<Quote> remote = factory.writer(Quote.class, "remote");
        remote.append(origin.query().all().asResultSet().subset(0, 3000));
        server.publish(remote);
        server.start();

        try (JournalWriter<Quote> local1 = factory.writer(Quote.class, "local1")) {
            local1.append(origin.query().all().asResultSet().subset(0, 1000));
        }

        try (JournalWriter<Quote> local2 = factory.writer(Quote.class, "local2")) {
            local2.append(origin.query().all().asResultSet().subset(0, 1500));
        }

        final CountDownLatch latch = new CountDownLatch(2);

        client.subscribe(Quote.class, "remote", "local1", new TxListener() {
            @Override
            public void onCommit() {
                latch.countDown();
            }
        });

        JournalClient client2 = new JournalClient(new ClientConfig("localhost"), factory);

        client2.subscribe(Quote.class, "remote", "local2", new TxListener() {
            @Override
            public void onCommit() {
                latch.countDown();
            }
        });

        client.start();
        client2.start();

        remote.append(origin.query().all().asResultSet().subset(3000, 10000));
        remote.commit();

        latch.await();

        client.halt();
        client2.halt();
        server.halt();

        Journal<Quote> local1r = factory.reader(Quote.class, "local1");
        Journal<Quote> local2r = factory.reader(Quote.class, "local2");

        Assert.assertEquals(size, local1r.size());
        Assert.assertEquals(size, local2r.size());
    }

    @Test
    public void testServerStartStop() throws Exception {
        server.start();
        server.halt();
        Assert.assertFalse(server.isRunning());
    }

    @Test(expected = JournalNetworkException.class)
    public void testClientConnect() throws Exception {
        client.start();
    }

    @Test
    public void testClientConnectServerHalt() throws Exception {
        server.start();
        client.start();
        Thread.sleep(TimeUnit.SECONDS.toMillis(1));
        server.halt();
        Assert.assertEquals(0, server.getConnectedClients());
        Assert.assertFalse(server.isRunning());
        Thread.sleep(500);
        Assert.assertFalse(client.isRunning());
        client.halt();
    }

    @Test
    public void testClientDisconnect() throws Exception {
        server.start();
        client.start();
        Thread.sleep(100);
        client.halt();
        Assert.assertFalse(client.isRunning());
        Thread.sleep(100);
        Assert.assertEquals(0, server.getConnectedClients());
        server.halt();
    }

    @Test
    public void testWriterShutdown() throws Exception {
        int size = 10000;
        try (JournalWriter<Quote> remote = factory.writer(Quote.class, "remote", 2 * size)) {
            server.publish(remote);
            server.start();

            client.subscribe(Quote.class, "remote", "local", 2 * size);
            client.start();

            TestUtils.generateQuoteData(remote, size, 0);
        }

        client.halt();
        server.halt();
    }
}
