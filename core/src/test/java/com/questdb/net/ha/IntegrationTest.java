/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2017 Appsicle
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
import com.questdb.ex.JournalException;
import com.questdb.factory.configuration.JournalConfigurationBuilder;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.misc.Dates;
import com.questdb.model.Quote;
import com.questdb.model.TestEntity;
import com.questdb.net.ha.config.ClientConfig;
import com.questdb.net.ha.config.ServerConfig;
import com.questdb.store.JournalEvents;
import com.questdb.store.JournalListener;
import com.questdb.test.tools.AbstractTest;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class IntegrationTest extends AbstractTest {

    private static final Log LOG = LogFactory.getLog(IntegrationTest.class);

    private JournalClient client;
    private JournalServer server;

    @Before
    public void setUp() {
        server = new JournalServer(new ServerConfig() {{
            setHeartbeatFrequency(TimeUnit.MILLISECONDS.toMillis(100));
            setEnableMultiCast(false);
        }}, getFactory());
        client = new JournalClient(new ClientConfig("localhost"), getFactory());
    }

    @Test
    @Ignore
    public void testBadJournalDoesNotResubscribe() throws Exception {
        // todo: test that when client reconnects it doesn't retry bad subscriptions
        Assert.fail();
    }

    @Test
    public void testBadSubscriptionOnTheFlyFollowedByReconnect() throws Exception {
        //todo: check that bad subscription doesn't interrupt data flow on good subscription

        try (final JournalWriter<Quote> origin = getFactory().writer(Quote.class, "origin")) {
            final int batchSize = 1000;
            final int batchCount = 100;

            server.publish(origin);

            server.start();
            try {
                final CountDownLatch terminated = new CountDownLatch(1);
                JournalClient client = new JournalClient(new ClientConfig("localhost"), getFactory(), null, evt -> {
                    if (evt == JournalClientEvents.EVT_TERMINATED) {
                        terminated.countDown();
                    }
                });

                client.start();


                final AtomicInteger commits = new AtomicInteger();
                final AtomicInteger errors = new AtomicInteger();
                final CountDownLatch localSubscribed = new CountDownLatch(1);
                final CountDownLatch dataReceived = new CountDownLatch(1);
                try {

                    // create empty journal
                    getFactory().writer(Quote.class, "local").close();

                    try (final Journal local = getFactory().reader("local")) {
                        client.subscribe(Quote.class, "origin", "local", new JournalListener() {
                            @Override
                            public void onCommit() {
                                commits.incrementAndGet();
                                try {
                                    local.refresh();
                                    if (local.size() == batchCount * batchSize) {
                                        dataReceived.countDown();
                                    }
                                } catch (JournalException e) {
                                    e.printStackTrace();
                                    errors.incrementAndGet();
                                }
                            }

                            @Override
                            public void onEvent(int event) {
                                switch (event) {
                                    case JournalEvents.EVT_JNL_SUBSCRIBED:
                                        localSubscribed.countDown();
                                        break;
                                    default:
                                        errors.incrementAndGet();
                                        break;
                                }
                            }
                        });

                        final CountDownLatch published = new CountDownLatch(1);
                        final CyclicBarrier barrier = new CyclicBarrier(2);
                        final AtomicInteger publisherErrors = new AtomicInteger();

                        new Thread(() -> {
                            try {
                                barrier.await();

                                long timestamp = Dates.parseDateTime("2013-09-04T10:00:00.000Z");
                                long increment = 1000L;

                                for (int i = 0; i < batchCount; i++) {
                                    TestUtils.generateQuoteData(origin, batchSize, timestamp, increment);
                                    timestamp += increment * (batchSize);
                                    origin.commit();
                                }
                            } catch (Throwable e) {
                                e.printStackTrace();
                                publisherErrors.incrementAndGet();
                            }
                            published.countDown();
                        }).start();

                        Assert.assertTrue(localSubscribed.await(10, TimeUnit.SECONDS));
                        barrier.await();


                        // after publishing stream is setup we attempt to subscribe bad journal
                        // todo: this part breaks server, fix server and continue
//                        readerFactory.writer(new JournalConfigurationBuilder().$("x").$int("x").$()).close();
//
//                        client.subscribe(Quote.class, "origin", "x", new JournalListener() {
//                            @Override
//                            public void onCommit() {
//
//                            }
//
//                            @Override
//                            public void onEvent(int event) {
//                                System.out.println("bad event: " + event);
//                            }
//                        });


                        Assert.assertTrue(published.await(60, TimeUnit.SECONDS));
                        Assert.assertTrue(dataReceived.await(60, TimeUnit.SECONDS));
                        Assert.assertEquals(0, publisherErrors.get());
                        Assert.assertEquals(0, errors.get());
                        Assert.assertTrue(commits.get() > 0);

                        local.refresh();
                        Assert.assertEquals(batchSize * batchCount, local.size());
                    }
                } catch (Throwable e) {
                    e.printStackTrace();
                    Assert.fail();
                } finally {
                    client.halt();
                }
                Assert.assertTrue(terminated.await(5, TimeUnit.SECONDS));
            } finally {
                server.halt();
            }
        }

//        Assert.fail();
    }

    @Test
    public void testClientConnect() throws Exception {
        final CountDownLatch error = new CountDownLatch(1);
        client = new JournalClient(new ClientConfig("localhost"), getFactory(), null, evt -> {
            if (evt == JournalClientEvents.EVT_SERVER_ERROR) {
                error.countDown();
            }
        });

        client.start();

        Assert.assertTrue(error.await(30, TimeUnit.SECONDS));
    }

    @Test
    public void testClientConnectServerHalt() throws Exception {
        server.start();
        client.start();
        Thread.sleep(TimeUnit.SECONDS.toMillis(1));
        server.halt();
        Assert.assertEquals(0, server.getConnectedClients());
        Assert.assertFalse(server.isRunning());
        Thread.sleep(700);
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
     */
    @Test
    public void testOutOfSyncClient() throws Exception {
        int size = 10000;
        try (JournalWriter<Quote> remote = getFactory().writer(Quote.class, "remote", 2 * size)) {
            server.publish(remote);
            server.start();

            try {

                final CountDownLatch commitLatch1 = new CountDownLatch(1);
                client.subscribe(Quote.class, "remote", "local", 2 * size, new JournalListener() {
                    @Override
                    public void onCommit() {
                        commitLatch1.countDown();
                    }

                    @Override
                    public void onEvent(int event) {

                    }
                });
                client.start();

                TestUtils.generateQuoteData(remote, size);

                Assert.assertTrue(commitLatch1.await(5, TimeUnit.SECONDS));

                client.halt();

                try (Journal<Quote> local = getFactory().reader(Quote.class, "local")) {
                    TestUtils.assertDataEquals(remote, local);
                }

                TestUtils.generateQuoteData(remote, 10000, remote.getMaxTimestamp());
                remote.commit();

                try (JournalWriter<Quote> localW = getFactory().writer(Quote.class, "local")) {
                    TestUtils.generateQuoteData(localW, 10000, localW.getMaxTimestamp());
                    localW.commit();

                    TestUtils.generateQuoteData(localW, 10000, localW.getMaxTimestamp());
                    localW.commit();
                }

                final CountDownLatch errorCountDown = new CountDownLatch(1);

                client = new JournalClient(new ClientConfig("localhost"), getFactory());
                client.subscribe(Quote.class, "remote", "local", 2 * size, new JournalListener() {
                    @Override
                    public void onCommit() {
                    }

                    @Override
                    public void onEvent(int event) {
                        errorCountDown.countDown();
                    }
                });
                client.start();

                Assert.assertTrue(errorCountDown.await(5, TimeUnit.SECONDS));

                client.halt();
            } finally {
                server.halt();
            }
        }
    }

    @Test
    @Ignore
    // this is failing intermittently, replication is up for rewrite, cant be bothered fixing badly designed code
    public void testOutOfSyncServerSide() throws Exception {
        int size = 10000;
        try (JournalWriter<Quote> remote = getFactory().writer(Quote.class, "remote", 2 * size)) {
            server.publish(remote);
            server.start();

            try {

                final AtomicInteger serverErrors = new AtomicInteger();
                final AtomicInteger commits = new AtomicInteger();
                client = new JournalClient(new ClientConfig("localhost"), getFactory(), null, evt -> {
                    if (evt == JournalClientEvents.EVT_SERVER_DIED) {
                        serverErrors.incrementAndGet();
                    }
                });
                client.subscribe(Quote.class, "remote", "local", 2 * size, new JournalListener() {
                    @Override
                    public void onCommit() {
                        commits.incrementAndGet();
                    }

                    @Override
                    public void onEvent(int event) {

                    }
                });
                client.start();

                TestUtils.generateQuoteData(remote, size);

                TestUtils.assertCounter(commits, 1, 1, TimeUnit.SECONDS);

                client.halt();

                try (Journal<Quote> local = getFactory().reader(Quote.class, "local")) {
                    TestUtils.assertDataEquals(remote, local);
                }

                // -------------------------------

                TestUtils.generateQuoteData(remote, 10000, remote.getMaxTimestamp());
                remote.commit();
                TestUtils.generateQuoteData(remote, 10000, remote.getMaxTimestamp());
                remote.commit();
                TestUtils.generateQuoteData(remote, 10000, remote.getMaxTimestamp());
                remote.commit();

                try (JournalWriter<Quote> localW = getFactory().writer(Quote.class, "local")) {

                    TestUtils.generateQuoteData(localW, 10000, localW.getMaxTimestamp());
                    localW.commit();

                    TestUtils.generateQuoteData(localW, 10000, localW.getMaxTimestamp());
                    localW.commit();
                }

                final AtomicInteger errorCounter = new AtomicInteger();
                client = new JournalClient(new ClientConfig("localhost"), getFactory(), null, evt -> {
                    if (evt == JournalClientEvents.EVT_SERVER_DIED) {
                        serverErrors.incrementAndGet();
                    }
                });
                client.subscribe(Quote.class, "remote", "local", 2 * size, new JournalListener() {
                    @Override
                    public void onCommit() {
                        commits.incrementAndGet();
                    }

                    @Override
                    public void onEvent(int event) {
                        errorCounter.incrementAndGet();
                        System.out.println("EV: " + event);
                    }
                });
                client.start();

                TestUtils.assertCounter(commits, 1, 1, TimeUnit.SECONDS);
                TestUtils.assertCounter(errorCounter, 1, 1, TimeUnit.SECONDS);

                client.halt();

                Assert.assertEquals(0, serverErrors.get());

            } finally {
                server.halt();
            }
        }
    }

    @Test
    public void testResubscribeAfterBadSubscription() throws Exception {
        // check that bad subscription doesn't cause dupe check to go haywire

        int size = 1000;

        try (JournalWriter<Quote> origin = getFactory().writer(Quote.class, "origin")) {
            TestUtils.generateQuoteData(origin, size);
            server.publish(origin);

            server.start();
            try {

                getFactory().writer(new JournalConfigurationBuilder().$("local").$int("x").$()).close();

                final CountDownLatch terminated = new CountDownLatch(1);
                final AtomicInteger serverDied = new AtomicInteger();
                JournalClient client = new JournalClient(new ClientConfig("localhost"), getFactory(), null, evt -> {
                    switch (evt) {
                        case JournalClientEvents.EVT_TERMINATED:
                            terminated.countDown();
                            break;
                        case JournalClientEvents.EVT_SERVER_DIED:
                            serverDied.incrementAndGet();
                            break;
                        default:
                            break;
                    }
                });

                client.start();

                try {
                    final CountDownLatch incompatible = new CountDownLatch(1);
                    client.subscribe(Quote.class, "origin", "local", new JournalListener() {
                        @Override
                        public void onCommit() {

                        }

                        @Override
                        public void onEvent(int event) {
                            if (event == JournalEvents.EVT_JNL_INCOMPATIBLE) {
                                incompatible.countDown();
                            }
                        }
                    });

                    Assert.assertTrue(incompatible.await(500, TimeUnit.SECONDS));

                    // delete incompatible journal
                    getFactory().delete("local");


                    // subscribe again and have client create compatible journal from server's metadata
                    final AtomicInteger errorCount = new AtomicInteger();
                    final CountDownLatch commit = new CountDownLatch(1);
                    client.subscribe(Quote.class, "origin", "local", new JournalListener() {
                        @Override
                        public void onCommit() {
                            commit.countDown();
                        }

                        @Override
                        public void onEvent(int event) {
                            if (event != JournalEvents.EVT_JNL_SUBSCRIBED) {
                                errorCount.incrementAndGet();
                            }
                        }
                    });

                    Assert.assertTrue(commit.await(30, TimeUnit.SECONDS));
                    Assert.assertEquals(0, errorCount.get());
                } finally {
                    client.halt();
                }

                Assert.assertTrue(terminated.await(5, TimeUnit.SECONDS));
                Assert.assertEquals(0, serverDied.get());

                try (Journal r = getFactory().reader("local")) {
                    Assert.assertEquals(size, r.size());
                }
            } finally {
                server.halt();
            }
        }
    }

    @Test
    @Ignore
    public void testResubscribeAfterUnsubscribe() throws Exception {
        //todo: test that it is possible to re-subscribe after unsubscribe call
        Assert.fail();
    }

    @Test
    public void testServerIdleStartStop() throws Exception {
        try (JournalWriter<Quote> remote = getFactory().writer(Quote.class, "remote")) {
            server.publish(remote);
            server.start();
            client.subscribe(Quote.class, "remote", "local");
            client.start();
            Thread.sleep(100);
            server.halt();
            Assert.assertFalse(server.isRunning());
        }
    }

    @Test
    public void testServerStartStop() throws Exception {
        server.start();
        server.halt();
        Assert.assertFalse(server.isRunning());
    }

    @Test
    public void testSingleJournalSync() throws Exception {
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

                TestUtils.generateQuoteData(remote, size);

                latch.await();

                client.halt();
            } finally {
                server.halt();
            }

            try (Journal<Quote> local = getFactory().reader(Quote.class, "local")) {
                TestUtils.assertDataEquals(remote, local);
            }
        }
    }

    @Test
    @Ignore
    public void testSubscribeCopyOnTheFly() throws Exception {
        // todo: test that server can multiplex journal when needed
        Assert.fail();
    }

    @Test
    public void testSubscribeIncompatible() throws Exception {
        int size = 10000;

        try (JournalWriter<Quote> origin = getFactory().writer(Quote.class, "origin")) {
            TestUtils.generateQuoteData(origin, size);

            try (JournalWriter<Quote> remote = getFactory().writer(Quote.class, "remote")) {

                server.publish(remote);

                server.start();
                try {


                    remote.append(origin.query().all().asResultSet().subset(0, 1000));
                    remote.commit();


                    getFactory().writer(new JournalConfigurationBuilder().$("local").$int("x").$()).close();

                    final CountDownLatch terminated = new CountDownLatch(1);
                    JournalClient client = new JournalClient(new ClientConfig("localhost"), getFactory(), null, evt -> {

                        if (evt == JournalClientEvents.EVT_TERMINATED) {
                            terminated.countDown();
                        }
                    });

                    client.start();


                    final CountDownLatch incompatible = new CountDownLatch(1);
                    try {

                        client.subscribe(Quote.class, "remote", "local", new JournalListener() {
                            @Override
                            public void onCommit() {

                            }

                            @Override
                            public void onEvent(int event) {
                                if (event == JournalEvents.EVT_JNL_INCOMPATIBLE) {
                                    incompatible.countDown();
                                }
                            }
                        });

                        Assert.assertTrue(incompatible.await(500, TimeUnit.SECONDS));

                        remote.append(origin.query().all().asResultSet().subset(1000, 2000));
                        remote.commit();

                    } finally {
                        client.halt();
                    }

                    Assert.assertTrue(terminated.await(5, TimeUnit.SECONDS));
                } finally {
                    server.halt();
                }
            }

        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSubscribeIncompatibleWriter() throws Exception {
        int size = 10000;

        try (JournalWriter<Quote> origin = getFactory().writer(Quote.class, "origin")) {
            TestUtils.generateQuoteData(origin, size);

            try (JournalWriter<Quote> remote = getFactory().writer(Quote.class, "remote")) {

                server.publish(remote);

                server.start();
                try {
                    remote.append(origin.query().all().asResultSet().subset(0, 1000));
                    remote.commit();


                    try (JournalWriter writer = getFactory().writer(new JournalConfigurationBuilder().$("local").$int("x").$())) {

                        final CountDownLatch terminated = new CountDownLatch(1);
                        final AtomicInteger serverErrors = new AtomicInteger();
                        JournalClient client = new JournalClient(new ClientConfig("localhost"), getFactory(), null, evt -> {

                            if (evt == JournalClientEvents.EVT_TERMINATED) {
                                terminated.countDown();
                            }

                            if (evt == JournalClientEvents.EVT_SERVER_DIED) {
                                serverErrors.incrementAndGet();
                            }
                        });

                        client.start();


                        final CountDownLatch incompatible = new CountDownLatch(1);
                        try {

                            client.subscribe(new JournalKey<>("remote"), writer, new JournalListener() {
                                @Override
                                public void onCommit() {

                                }

                                @Override
                                public void onEvent(int event) {
                                    if (event == JournalEvents.EVT_JNL_INCOMPATIBLE) {
                                        incompatible.countDown();
                                    }
                                }
                            });

                            Assert.assertTrue(incompatible.await(500, TimeUnit.SECONDS));

                            remote.append(origin.query().all().asResultSet().subset(1000, 2000));
                            remote.commit();

                        } finally {
                            client.halt();
                        }

                        Assert.assertTrue(terminated.await(5, TimeUnit.SECONDS));
                        Assert.assertEquals(0, serverErrors.get());
                    }
                } finally {
                    server.halt();
                }
            }
        }
    }

    @Test
    public void testSubscribeOnTheFly() throws Exception {
        int size = 5000;

        try (JournalWriter<Quote> origin = getFactory().writer(Quote.class, "origin")) {
            TestUtils.generateQuoteData(origin, size);

            try (JournalWriter<Quote> remote1 = getFactory().writer(Quote.class, "remote1")) {
                try (JournalWriter<Quote> remote2 = getFactory().writer(Quote.class, "remote2")) {

                    server.publish(remote1);
                    server.publish(remote2);

                    server.start();
                    try {
                        remote1.append(origin.query().all().asResultSet().subset(0, 1000));
                        remote1.commit();

                        remote2.append(origin.query().all().asResultSet().subset(0, 1000));
                        remote2.commit();

                        final AtomicInteger counter = new AtomicInteger();
                        JournalClient client = new JournalClient(new ClientConfig("localhost"), getFactory());
                        client.start();

                        try {

                            client.subscribe(Quote.class, "remote1", "local1", new JournalListener() {
                                @Override
                                public void onCommit() {
                                    counter.incrementAndGet();
                                }

                                @Override
                                public void onEvent(int event) {

                                }
                            });

                            TestUtils.assertCounter(counter, 1, 2, TimeUnit.SECONDS);

                            try (Journal r = getFactory().reader("local1")) {
                                Assert.assertEquals(1000, r.size());
                            }

                            client.subscribe(Quote.class, "remote2", "local2", new JournalListener() {
                                @Override
                                public void onCommit() {
                                    counter.incrementAndGet();
                                }

                                @Override
                                public void onEvent(int event) {

                                }
                            });

                            TestUtils.assertCounter(counter, 2, 2, TimeUnit.SECONDS);

                            try (Journal r = getFactory().reader("local2")) {
                                Assert.assertEquals(1000, r.size());
                            }

                        } finally {
                            client.halt();
                        }
                    } finally {
                        server.halt();
                    }
                }
            }

        }
    }

    @Test
    public void testSubscribeTwice() throws Exception {
        int size = 10000;

        try (JournalWriter<Quote> origin = getFactory().writer(Quote.class, "origin")) {
            TestUtils.generateQuoteData(origin, size);

            try (JournalWriter<Quote> remote1 = getFactory().writer(Quote.class, "remote1")) {
                try (JournalWriter<Quote> remote2 = getFactory().writer(Quote.class, "remote2")) {

                    server.publish(remote1);
                    server.publish(remote2);

                    server.start();
                    try {


                        remote1.append(origin.query().all().asResultSet().subset(0, 1000));
                        remote1.commit();

                        remote2.append(origin.query().all().asResultSet().subset(0, 1000));
                        remote2.commit();

                        final AtomicInteger counter = new AtomicInteger();
                        final AtomicInteger errors = new AtomicInteger();
                        JournalClient client = new JournalClient(new ClientConfig("localhost"), getFactory());
                        client.start();

                        try {

                            client.subscribe(Quote.class, "remote1", "local1", new JournalListener() {
                                @Override
                                public void onCommit() {
                                    counter.incrementAndGet();
                                }

                                @Override
                                public void onEvent(int event) {
                                    errors.incrementAndGet();
                                }
                            });

                            TestUtils.assertCounter(counter, 1, 2, TimeUnit.SECONDS);

                            try (Journal r = getFactory().reader("local1")) {
                                Assert.assertEquals(1000, r.size());
                            }

                            client.subscribe(Quote.class, "remote2", "local1", new JournalListener() {
                                @Override
                                public void onCommit() {
                                    counter.incrementAndGet();
                                }

                                @Override
                                public void onEvent(int event) {
                                    errors.incrementAndGet();
                                }
                            });

                            TestUtils.assertCounter(counter, 1, 2, TimeUnit.SECONDS);
                            TestUtils.assertCounter(errors, 1, 2, TimeUnit.SECONDS);

                            try (Journal r = getFactory().reader("local1")) {
                                Assert.assertEquals(1000, r.size());
                            }

                        } finally {
                            client.halt();
                        }
                    } finally {
                        server.halt();
                    }
                }
            }

        }
    }

    @Test
    public void testTwoClientSync() throws Exception {
        int size = 10000;
        try (JournalWriter<Quote> origin = getFactory().writer(Quote.class, "origin")) {
            TestUtils.generateQuoteData(origin, size);

            try (JournalWriter<Quote> remote = getFactory().writer(Quote.class, "remote")) {
                remote.append(origin.query().all().asResultSet().subset(0, 1000));
                remote.commit();

                server.publish(remote);
                server.start();

                final AtomicInteger counter = new AtomicInteger();
                JournalClient client1 = new JournalClient(new ClientConfig("localhost"), getFactory());
                client1.subscribe(Quote.class, "remote", "local1", new JournalListener() {
                    @Override
                    public void onCommit() {
                        counter.incrementAndGet();
                    }

                    @Override
                    public void onEvent(int event) {

                    }
                });
                client1.start();

                JournalClient client2 = new JournalClient(new ClientConfig("localhost"), getFactory());
                client2.subscribe(Quote.class, "remote", "local2", new JournalListener() {
                    @Override
                    public void onCommit() {
                        counter.incrementAndGet();
                    }

                    @Override
                    public void onEvent(int event) {

                    }
                });
                client2.start();

                TestUtils.assertCounter(counter, 2, 2, TimeUnit.SECONDS);
                client1.halt();

                remote.append(origin.query().all().asResultSet().subset(1000, 1500));
                remote.commit();

                TestUtils.assertCounter(counter, 3, 2, TimeUnit.SECONDS);

                LOG.info().$("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~").$();


                // this client should receive an update that gets it up to speed
                // wait until this happens before adding more rows to remote

                final CountDownLatch waitForUpdate = new CountDownLatch(1);

                client1 = new JournalClient(new ClientConfig("localhost"), getFactory());
                client1.subscribe(Quote.class, "remote", "local1", new JournalListener() {
                    @Override
                    public void onCommit() {
                        counter.incrementAndGet();
                        waitForUpdate.countDown();
                    }

                    @Override
                    public void onEvent(int event) {

                    }
                });
                client1.start();

                waitForUpdate.await(2, TimeUnit.SECONDS);

                remote.append(origin.query().all().asResultSet().subset(1500, size));
                remote.commit();

                TestUtils.assertCounter(counter, 6, 2, TimeUnit.SECONDS);

                try (Journal<Quote> local1r = getFactory().reader(Quote.class, "local1")) {
                    Assert.assertEquals(size, local1r.size());
                }

                try (Journal<Quote> local2r = getFactory().reader(Quote.class, "local2")) {
                    Assert.assertEquals(size, local2r.size());
                }

                client1.halt();
                client2.halt();
                server.halt();
            }
        }
    }

    @Test
    public void testTwoJournalsSync() throws Exception {
        int size = 10000;
        try (JournalWriter<Quote> remote1 = getFactory().writer(Quote.class, "remote1", 2 * size)) {
            try (JournalWriter<TestEntity> remote2 = getFactory().writer(TestEntity.class, "remote2", 2 * size)) {
                server.publish(remote1);
                server.publish(remote2);
                server.start();

                final CountDownLatch latch = new CountDownLatch(2);
                client.subscribe(Quote.class, "remote1", "local1", 2 * size, new JournalListener() {
                    @Override
                    public void onCommit() {
                        latch.countDown();
                    }

                    @Override
                    public void onEvent(int event) {

                    }
                });

                client.subscribe(TestEntity.class, "remote2", "local2", 2 * size, new JournalListener() {
                    @Override
                    public void onCommit() {
                        latch.countDown();
                    }

                    @Override
                    public void onEvent(int event) {

                    }
                });
                client.start();

                TestUtils.generateQuoteData(remote1, size);
                TestUtils.generateTestEntityData(remote2, size);

                latch.await();

                client.halt();
                server.halt();

                try (Journal<Quote> local1 = getFactory().reader(Quote.class, "local1")) {
                    Assert.assertEquals("Local1 has wrong size", size, local1.size());
                }

                try (Journal<TestEntity> local2 = getFactory().reader(TestEntity.class, "local2")) {
                    Assert.assertEquals("Remote2 has wrong size", size, remote2.size());
                    Assert.assertEquals("Local2 has wrong size", size, local2.size());
                }
            }
        }
    }

    @Test
    @Ignore
    public void testUnsubscribe() throws Exception {
        //todo: check that unsubscribe triggers correct event sequence
        Assert.fail();
    }

    @Test
    @Ignore
    public void testUnsubscribeOnTheFly() throws Exception {
        //todo: test that unsubscribe on the fly does not impact existing data flow
        Assert.fail();
    }

    @Test
    @Ignore
    public void testUnsubscribeReconnectBehaviour() throws Exception {
        // todo: test that unsubscribed journal does not cause re-subscription on client failover
        Assert.fail();
    }

    @Test
    public void testWriterShutdown() throws Exception {
        int size = 10000;
        try (JournalWriter<Quote> remote = getFactory().writer(Quote.class, "remote", 2 * size)) {
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
