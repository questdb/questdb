/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
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
 ******************************************************************************/

package com.nfsdb.ha;

import com.nfsdb.Journal;
import com.nfsdb.JournalWriter;
import com.nfsdb.exceptions.JournalNetworkException;
import com.nfsdb.ha.config.ClientConfig;
import com.nfsdb.ha.config.ServerConfig;
import com.nfsdb.model.Quote;
import com.nfsdb.model.TestEntity;
import com.nfsdb.storage.TxListener;
import com.nfsdb.test.tools.AbstractTest;
import com.nfsdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class IntegrationTest extends AbstractTest {

    private JournalClient client;
    private JournalServer server;

    @Before
    public void setUp() {
        server = new JournalServer(new ServerConfig() {{
            setHeartbeatFrequency(TimeUnit.MILLISECONDS.toMillis(100));
            setEnableMultiCast(false);
        }}, factory);
        client = new JournalClient(new ClientConfig("localhost"), factory);
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

    /**
     * Create two journal that are in sync.
     * Disconnect synchronisation and advance client by two transaction and server by one
     * Server will offer rollback by proving txn of its latest transaction.
     * Client will have same txn but different pin, because it was advancing out of sync with server.
     * Client should produce and error by reporting unknown txn from server.
     *
     * @throws Exception
     */
    @Test
    public void testOutOfSyncClient() throws Exception {
        int size = 10000;
        JournalWriter<Quote> remote = factory.writer(Quote.class, "remote", 2 * size);
        server.publish(remote);
        server.start();

        final AtomicInteger counter = new AtomicInteger();
        client.subscribe(Quote.class, "remote", "local", 2 * size, new TxListener() {
            @Override
            public void onCommit() {
                counter.incrementAndGet();
            }

            @Override
            public void onError() {

            }
        });
        client.start();

        TestUtils.generateQuoteData(remote, size);

        TestUtils.assertCounter(counter, 1, 1, TimeUnit.SECONDS);

        client.halt();

        Journal<Quote> local = factory.reader(Quote.class, "local");
        TestUtils.assertDataEquals(remote, local);

        TestUtils.generateQuoteData(remote, 10000, remote.getMaxTimestamp());
        remote.commit();

        JournalWriter<Quote> localW = factory.writer(Quote.class, "local");

        TestUtils.generateQuoteData(localW, 10000, localW.getMaxTimestamp());
        localW.commit();

        TestUtils.generateQuoteData(localW, 10000, localW.getMaxTimestamp());
        localW.commit();

        localW.close();

        final AtomicInteger errorCounter = new AtomicInteger();
        client = new JournalClient(new ClientConfig("localhost"), factory);
        client.subscribe(Quote.class, "remote", "local", 2 * size, new TxListener() {
            @Override
            public void onCommit() {
                counter.incrementAndGet();
            }

            @Override
            public void onError() {
                errorCounter.incrementAndGet();
            }
        });
        client.start();

        TestUtils.assertCounter(counter, 1, 1, TimeUnit.SECONDS);
        TestUtils.assertCounter(errorCounter, 1, 1, TimeUnit.SECONDS);

        client.halt();
        server.halt();
    }

    @Test
    public void testOutOfSyncServerSide() throws Exception {
        int size = 10000;
        JournalWriter<Quote> remote = factory.writer(Quote.class, "remote", 2 * size);
        server.publish(remote);
        server.start();

        final AtomicInteger counter = new AtomicInteger();
        client.subscribe(Quote.class, "remote", "local", 2 * size, new TxListener() {
            @Override
            public void onCommit() {
                counter.incrementAndGet();
            }

            @Override
            public void onError() {

            }
        });
        client.start();

        TestUtils.generateQuoteData(remote, size);

        TestUtils.assertCounter(counter, 1, 1, TimeUnit.SECONDS);

        client.halt();

        Journal<Quote> local = factory.reader(Quote.class, "local");
        TestUtils.assertDataEquals(remote, local);

        // -------------------------------

        TestUtils.generateQuoteData(remote, 10000, remote.getMaxTimestamp());
        remote.commit();
        TestUtils.generateQuoteData(remote, 10000, remote.getMaxTimestamp());
        remote.commit();
        TestUtils.generateQuoteData(remote, 10000, remote.getMaxTimestamp());
        remote.commit();

        JournalWriter<Quote> localW = factory.writer(Quote.class, "local");

        TestUtils.generateQuoteData(localW, 10000, localW.getMaxTimestamp());
        localW.commit();

        TestUtils.generateQuoteData(localW, 10000, localW.getMaxTimestamp());
        localW.commit();

        localW.close();

        final AtomicInteger errorCounter = new AtomicInteger();
        client = new JournalClient(new ClientConfig("localhost"), factory);
        client.subscribe(Quote.class, "remote", "local", 2 * size, new TxListener() {
            @Override
            public void onCommit() {
                counter.incrementAndGet();
            }

            @Override
            public void onError() {
                errorCounter.incrementAndGet();
            }
        });
        client.start();

        TestUtils.assertCounter(counter, 1, 1, TimeUnit.SECONDS);
        TestUtils.assertCounter(errorCounter, 1, 1, TimeUnit.SECONDS);

        client.halt();
        server.halt();
    }

    @Test
    public void testServerIdleStartStop() throws Exception {
        JournalWriter<Quote> remote = factory.writer(Quote.class, "remote");
        server.publish(remote);
        server.start();
        client.subscribe(Quote.class, "remote", "local");
        client.start();
        Thread.sleep(100);
        server.halt();
        Assert.assertFalse(server.isRunning());
    }

/*
    @Test
    public void testSingleJournalSync2() throws Exception {

        int size = 100000;
        JournalWriter<Price> remote = factory.writer(Price.class, "remote");
        server.publish(remote);
        server.start();



        final AtomicInteger counter = new AtomicInteger();
        client.subscribe(Price.class, "remote", "local", new TxListener() {
            @Override
            public void onCommit() {
                counter.incrementAndGet();
            }
        });
        client.start();

        Price p = new Price();
        long t = remote.getMaxTimestamp();
        for (int i = 0; i < size; i++) {
            p.setTimestamp(t += i);
            p.setNanos(System.currentTimeMillis());
            p.setSym(String.valueOf(i % 20));
            p.setPrice(i * 1.04598 + i);
            remote.append(p);
        }
        remote.commit();

        TestUtils.assertCounter(counter, 1, 2, TimeUnit.SECONDS);

        Journal<Price> r = factory.bulkReader(Price.class, "local");
        for (Price v: r.bufferedIterator()) {
            System.out.println(v.getSym());
        }
        client.halt();
        server.halt();
    }
*/

    @Test
    public void testServerStartStop() throws Exception {
        server.start();
        server.halt();
        Assert.assertFalse(server.isRunning());
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

            @Override
            public void onError() {

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
    public void testTwoClientSync() throws Exception {
        int size = 10000;
        JournalWriter<Quote> origin = factory.writer(Quote.class, "origin");
        TestUtils.generateQuoteData(origin, size);

        JournalWriter<Quote> remote = factory.writer(Quote.class, "remote");
        remote.append(origin.query().all().asResultSet().subset(0, 1000));
        remote.commit();

        server.publish(remote);
        server.start();

        final AtomicInteger counter = new AtomicInteger();
        JournalClient client1 = new JournalClient(new ClientConfig("localhost"), factory);
        client1.subscribe(Quote.class, "remote", "local1", new TxListener() {
            @Override
            public void onCommit() {
                counter.incrementAndGet();
            }

            @Override
            public void onError() {

            }
        });
        client1.start();

        JournalClient client2 = new JournalClient(new ClientConfig("localhost"), factory);
        client2.subscribe(Quote.class, "remote", "local2", new TxListener() {
            @Override
            public void onCommit() {
                counter.incrementAndGet();
            }

            @Override
            public void onError() {

            }
        });
        client2.start();

        TestUtils.assertCounter(counter, 2, 2, TimeUnit.SECONDS);
        client1.halt();

        remote.append(origin.query().all().asResultSet().subset(1000, 1500));
        remote.commit();

        TestUtils.assertCounter(counter, 3, 2, TimeUnit.SECONDS);


        client1 = new JournalClient(new ClientConfig("localhost"), factory);
        client1.subscribe(Quote.class, "remote", "local1", new TxListener() {
            @Override
            public void onCommit() {
                counter.incrementAndGet();
            }

            @Override
            public void onError() {

            }
        });
        client1.start();

        remote.append(origin.query().all().asResultSet().subset(1500, size));
        remote.commit();

        TestUtils.assertCounter(counter, 6, 2, TimeUnit.SECONDS);

        Journal<Quote> local1r = factory.reader(Quote.class, "local1");
        Journal<Quote> local2r = factory.reader(Quote.class, "local2");

        Assert.assertEquals(size, local1r.size());
        Assert.assertEquals(size, local2r.size());

        client1.halt();
        client2.halt();
        server.halt();
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

            @Override
            public void onError() {

            }
        });

        client.subscribe(TestEntity.class, "remote2", "local2", 2 * size, new TxListener() {
            @Override
            public void onCommit() {
                latch.countDown();
            }

            @Override
            public void onError() {

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
