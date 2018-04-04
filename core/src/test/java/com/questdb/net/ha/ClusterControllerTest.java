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

import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.model.Quote;
import com.questdb.model.configuration.ModelConfiguration;
import com.questdb.net.ha.config.ClientConfig;
import com.questdb.net.ha.config.ServerConfig;
import com.questdb.net.ha.config.ServerNode;
import com.questdb.std.NumericException;
import com.questdb.std.ex.JournalException;
import com.questdb.store.JournalWriter;
import com.questdb.store.factory.Factory;
import com.questdb.test.tools.AbstractTest;
import com.questdb.test.tools.FactoryContainer;
import com.questdb.test.tools.TestUtils;
import org.junit.*;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class ClusterControllerTest extends AbstractTest {

    private final static Log LOG = LogFactory.getLog(ClusterController.class);
    @Rule
    public final FactoryContainer tf = new FactoryContainer(ModelConfiguration.MAIN);

    @Rule
    public final FactoryContainer tf1 = new FactoryContainer(ModelConfiguration.MAIN);

    @Rule
    public final FactoryContainer tf2 = new FactoryContainer(ModelConfiguration.MAIN);

    @Rule
    public final FactoryContainer tf3 = new FactoryContainer(ModelConfiguration.MAIN);

    @Rule
    public final FactoryContainer tf4 = new FactoryContainer(ModelConfiguration.MAIN);

    @Rule
    public final FactoryContainer tf5 = new FactoryContainer(ModelConfiguration.MAIN);

    @After
    public void tearDown() {
        Assert.assertEquals(0, tf.getFactory().getBusyWriterCount());
        Assert.assertEquals(0, tf1.getFactory().getBusyWriterCount());
        Assert.assertEquals(0, tf2.getFactory().getBusyWriterCount());
        Assert.assertEquals(0, tf3.getFactory().getBusyWriterCount());
        Assert.assertEquals(0, tf4.getFactory().getBusyWriterCount());
        Assert.assertEquals(0, tf5.getFactory().getBusyWriterCount());

        Assert.assertEquals(0, tf.getFactory().getBusyReaderCount());
        Assert.assertEquals(0, tf1.getFactory().getBusyReaderCount());
        Assert.assertEquals(0, tf2.getFactory().getBusyReaderCount());
        Assert.assertEquals(0, tf3.getFactory().getBusyReaderCount());
        Assert.assertEquals(0, tf4.getFactory().getBusyReaderCount());
        Assert.assertEquals(0, tf5.getFactory().getBusyReaderCount());
    }

    @Test
    @Ignore
    public void testBusyFailOver() throws Exception {

        try (JournalWriter<Quote> writer1 = getFactory().writer(Quote.class)) {
            try (final JournalWriter<Quote> writer2 = tf.getFactory().writer(Quote.class)) {

                final CountDownLatch active1 = new CountDownLatch(1);
                final CountDownLatch active2 = new CountDownLatch(1);
                final CountDownLatch standby2 = new CountDownLatch(1);

                final AtomicLong expected = new AtomicLong();
                final AtomicLong actual = new AtomicLong();


                ClusterController controller1 = new ClusterController(
                        new ServerConfig() {{
                            addNode(new ServerNode(0, "localhost:7080"));
                            addNode(new ServerNode(1, "localhost:7090"));
                            setEnableMultiCast(false);
                            setHeartbeatFrequency(50);
                        }},
                        new ClientConfig() {{
                            setEnableMultiCast(false);
                        }},
                        getFactory(),
                        0,
                        new ArrayList<JournalWriter>() {{
                            add(writer1);
                        }},
                        new ClusterStatusListener() {
                            @Override
                            public void goActive() {
                                try {
                                    TestUtils.generateQuoteData(writer1, 100000);
                                    TestUtils.generateQuoteData(writer1, 100000, writer1.getMaxTimestamp());
                                    writer1.commit();
                                    TestUtils.generateQuoteData(writer1, 100000, writer1.getMaxTimestamp());
                                    writer1.commit();
                                    TestUtils.generateQuoteData(writer1, 100000, writer1.getMaxTimestamp());
                                    writer1.commit();
                                    TestUtils.generateQuoteData(writer1, 100000, writer1.getMaxTimestamp());
                                    writer1.commit();
                                    expected.set(writer1.size());
                                    active1.countDown();
                                } catch (JournalException | NumericException e) {
                                    e.printStackTrace();
                                }
                            }

                            @Override
                            public void goPassive(ServerNode activeNode) {
                            }

                            @Override
                            public void onShutdown() {
                            }
                        }
                );

                ClusterController controller2 = new ClusterController(
                        new ServerConfig() {{
                            addNode(new ServerNode(0, "localhost:7080"));
                            addNode(new ServerNode(1, "localhost:7090"));
                            setEnableMultiCast(false);
                            setHeartbeatFrequency(50);
                        }},
                        new ClientConfig() {{
                            setEnableMultiCast(false);
                        }},
                        tf.getFactory(),
                        1,
                        new ArrayList<JournalWriter>() {{
                            add(writer2);
                        }},
                        new ClusterStatusListener() {
                            @Override
                            public void goActive() {
                                try {
                                    actual.set(writer2.size());
                                    active2.countDown();
                                } catch (JournalException e) {
                                    e.printStackTrace();
                                }
                            }

                            @Override
                            public void goPassive(ServerNode activeNode) {
                                standby2.countDown();
                            }

                            @Override
                            public void onShutdown() {
                            }
                        }
                );

                controller1.start();
                Assert.assertTrue(active1.await(30, TimeUnit.SECONDS));
                Assert.assertEquals(0, active1.getCount());

                controller2.start();
                standby2.await(60, TimeUnit.SECONDS);
                Assert.assertEquals(0, standby2.getCount());

                controller1.halt();

                active2.await(10, TimeUnit.SECONDS);
                Assert.assertEquals(0, active2.getCount());

                controller2.halt();
                Assert.assertTrue(expected.get() > 0);
                Assert.assertEquals(expected.get(), actual.get());
            }
        }
    }

    @Test
    public void testFiveNodesVoting() throws Exception {

        AtomicInteger active = new AtomicInteger();
        AtomicInteger standby = new AtomicInteger();
        AtomicInteger shutdown = new AtomicInteger();

        LOG.info().$("======= VOTING TEST ==========").$();

        try (JournalWriter writer1 = tf1.getFactory().writer(Quote.class)) {

            try (JournalWriter writer2 = tf2.getFactory().writer(Quote.class)) {

                try (JournalWriter writer3 = tf3.getFactory().writer(Quote.class)) {

                    try (JournalWriter writer4 = tf4.getFactory().writer(Quote.class)) {

                        try (JournalWriter writer5 = tf5.getFactory().writer(Quote.class)) {

                            ClusterController c1 = createController2(writer1, 0, tf1.getFactory(), active, standby, shutdown);
                            ClusterController c2 = createController2(writer2, 1, tf2.getFactory(), active, standby, shutdown);
                            ClusterController c3 = createController2(writer3, 2, tf3.getFactory(), active, standby, shutdown);
                            ClusterController c4 = createController2(writer4, 3, tf4.getFactory(), active, standby, shutdown);
                            ClusterController c5 = createController2(writer5, 4, tf5.getFactory(), active, standby, shutdown);


                            c1.start();
                            c2.start();
                            c3.start();
                            c4.start();
                            c5.start();

                            long t;

                            t = System.currentTimeMillis();
                            while (standby.get() < 4 && TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - t) < 600) {
                                Thread.yield();
                            }
                            Assert.assertEquals(4, standby.get());


                            t = System.currentTimeMillis();
                            while (active.get() < 1 && TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - t) < 600) {
                                Thread.yield();
                            }
                            Assert.assertEquals(1, active.get());

                            // on slower system instances can be subject to staggered startup, which can create noise in message loop
                            // this noise should get cancelled out given some time.
                            // 1 second should be plenty of time for any ELECTION message to be suppressed.
                            Thread.sleep(1000);

                            standby.set(0);
                            active.set(0);

                            LOG.info().$("Stage 1, halt leader").$();

                            if (c5.isLeader()) {
                                c5.halt();
                                LOG.info().$("halted 4").$();
                            } else if (c4.isLeader()) {
                                c4.halt();
                                LOG.info().$("halted 3").$();
                            } else if (c3.isLeader()) {
                                c3.halt();
                                LOG.info().$("halted 2").$();
                            } else if (c2.isLeader()) {
                                c2.halt();
                                LOG.info().$("halted 1").$();
                            } else if (c1.isLeader()) {
                                c1.halt();
                                LOG.info().$("halted 0").$();
                            } else {
                                Assert.fail("No leader");
                            }

                            LOG.info().$("Stage 2, waiting for election process to complete").$();
                            t = System.currentTimeMillis();
                            while ((active.get() < 1 || standby.get() < 3) && TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - t) < 180) {
                                Thread.yield();
                            }

                            LOG.info().$("Checking leader").$();

                            try {
                                Assert.assertEquals(3, standby.get());
                                Assert.assertEquals(1, active.get());

                                LOG.info().$("Test complete").$();
                            } finally {
                                c1.halt();
                                c2.halt();
                                c3.halt();
                                c4.halt();
                            }
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testStaggeredFailOver() throws Exception {
        final CountDownLatch active1Latch = new CountDownLatch(1);
        final CountDownLatch active2Latch = new CountDownLatch(1);
        final CountDownLatch standby1Latch = new CountDownLatch(1);
        final CountDownLatch standby2Latch = new CountDownLatch(1);
        final CountDownLatch shutdown1 = new CountDownLatch(1);
        final CountDownLatch shutdown2 = new CountDownLatch(1);

        try (JournalWriter writer1 = getFactory().writer(Quote.class)) {

            try (JournalWriter writer2 = tf.getFactory().writer(Quote.class)) {

                ClusterController controller1 = createControllerX(writer1, 0, getFactory(), active1Latch, standby1Latch, shutdown1);
                controller1.start();

                Assert.assertTrue(active1Latch.await(5, TimeUnit.SECONDS));
                Assert.assertEquals("Node 1 is expected to be active", 0, active1Latch.getCount());
                standby1Latch.await(200, TimeUnit.MILLISECONDS);
                Assert.assertEquals("Node 1 standby callback is not expected to be called", 1, standby1Latch.getCount());

                ClusterController controller2 = createControllerX(writer2, 1, tf.getFactory(), active2Latch, standby2Latch, shutdown2);
                controller2.start();

                standby2Latch.await(5, TimeUnit.SECONDS);
                Assert.assertEquals("Node 2 is expected to be standing by", 0, standby2Latch.getCount());
                active2Latch.await(200, TimeUnit.MILLISECONDS);
                Assert.assertEquals("Node 2 active() callback is not expected to be called", 1, active2Latch.getCount());

                controller1.halt();
                shutdown1.await(5, TimeUnit.SECONDS);
                Assert.assertEquals(0, shutdown1.getCount());

                active2Latch.await(5, TimeUnit.SECONDS);
                Assert.assertEquals(0, active2Latch.getCount());

                controller2.halt();
                shutdown2.await(5, TimeUnit.SECONDS);
                Assert.assertEquals(0, shutdown2.getCount());
            }
        }
    }

    @Test
    public void testStaggeredStartup() throws Exception {
        final CountDownLatch active1Latch = new CountDownLatch(1);
        final CountDownLatch active2Latch = new CountDownLatch(1);
        final CountDownLatch standby1Latch = new CountDownLatch(1);
        final CountDownLatch standby2Latch = new CountDownLatch(1);
        final CountDownLatch shutdown1 = new CountDownLatch(1);
        final CountDownLatch shutdown2 = new CountDownLatch(1);

        try (JournalWriter writer1 = getFactory().writer(Quote.class)) {

            try (JournalWriter writer2 = tf.getFactory().writer(Quote.class)) {

                ClusterController controller1 = createControllerX(writer1, 0, getFactory(), active1Latch, standby1Latch, shutdown1);
                controller1.start();

                Assert.assertTrue(active1Latch.await(5, TimeUnit.SECONDS));
                Assert.assertEquals("Node 1 is expected to be active", 0, active1Latch.getCount());
                standby1Latch.await(200, TimeUnit.MILLISECONDS);
                Assert.assertEquals("Node 1 standby callback is not expected to be called", 1, standby1Latch.getCount());

                ClusterController controller2 = createControllerX(writer2, 1, tf.getFactory(), active2Latch, standby2Latch, shutdown2);
                controller2.start();

                standby2Latch.await(5, TimeUnit.SECONDS);
                Assert.assertEquals("Node 2 is expected to be standing by", 0, standby2Latch.getCount());
                active2Latch.await(200, TimeUnit.MILLISECONDS);
                Assert.assertEquals("Node 2 active() callback is not expected to be called", 1, active2Latch.getCount());

                controller2.halt();
                shutdown2.await(5, TimeUnit.SECONDS);
                Assert.assertEquals(0, shutdown2.getCount());

                controller1.halt();
                shutdown1.await(5, TimeUnit.SECONDS);
                Assert.assertEquals(0, shutdown1.getCount());
            }
        }
    }

    @Test
    public void testStandalone() throws Exception {
        final CountDownLatch active = new CountDownLatch(1);
        final CountDownLatch standby = new CountDownLatch(1);
        final CountDownLatch shutdown = new CountDownLatch(1);

        try (JournalWriter writer = getFactory().writer(Quote.class)) {
            ClusterController controller = createControllerX(writer, 1, getFactory(), active, standby, shutdown);

            controller.start();
            Assert.assertTrue(active.await(5, TimeUnit.SECONDS));
            Assert.assertEquals("goActive() did not fire", 0, active.getCount());
            standby.await(200, TimeUnit.MILLISECONDS);
            Assert.assertEquals("goPassive() not expected to fire", 1, standby.getCount());

            controller.halt();
            shutdown.await(5, TimeUnit.SECONDS);
            Assert.assertEquals(0, shutdown.getCount());
            controller.halt();
        }
    }

    @Test
    public void testTiebreakFailOver() throws Exception {

        final CountDownLatch active1Latch = new CountDownLatch(1);
        final CountDownLatch active2Latch = new CountDownLatch(1);
        final CountDownLatch standby1Latch = new CountDownLatch(1);
        final CountDownLatch standby2Latch = new CountDownLatch(1);
        final CountDownLatch shutdown1 = new CountDownLatch(1);
        final CountDownLatch shutdown2 = new CountDownLatch(1);

        try (JournalWriter writer1 = getFactory().writer(Quote.class)) {

            try (JournalWriter writer2 = tf.getFactory().writer(Quote.class)) {
                ClusterController controller1 = createControllerX(writer1, 0, getFactory(), active1Latch, standby1Latch, shutdown1);
                ClusterController controller2 = createControllerX(writer2, 1, tf.getFactory(), active2Latch, standby2Latch, shutdown2);

                // start two controller without pause
                controller2.start();
                controller1.start();

                getFactory().close();

                long t = System.currentTimeMillis();
                do {
                    active1Latch.await(1, TimeUnit.MICROSECONDS);
                    active2Latch.await(1, TimeUnit.MICROSECONDS);
                }
                while (active1Latch.getCount() > 0 && active2Latch.getCount() > 0 && (System.currentTimeMillis() - t) < 2000);

                Assert.assertFalse("Two nodes are active simultaneously", active1Latch.getCount() == 0 && active2Latch.getCount() == 0);
                Assert.assertFalse("No leader", active1Latch.getCount() > 0 && active2Latch.getCount() > 0);


                if (active1Latch.getCount() == 0) {
                    standby2Latch.await(2, TimeUnit.SECONDS);
                    Assert.assertEquals("Node 2 is expected to be on standby", 0, standby2Latch.getCount());

                    standby1Latch.await(200, TimeUnit.MILLISECONDS);
                    Assert.assertEquals("Node 1 is NOT expected to be on standby", 1, standby1Latch.getCount());
                } else {
                    standby1Latch.await(2, TimeUnit.SECONDS);
                    Assert.assertEquals("Node 1 is expected to be on standby", 0, standby1Latch.getCount());

                    standby2Latch.await(200, TimeUnit.MILLISECONDS);
                    Assert.assertEquals("Node 2 is NOT expected to be on standby", 1, standby2Latch.getCount());
                }

                controller2.halt();


                shutdown2.await(5, TimeUnit.SECONDS);
                Assert.assertEquals("Controller 2 should have shut down", 0, shutdown2.getCount());

                active1Latch.await(10, TimeUnit.SECONDS);
                Assert.assertEquals("Node 1 is expected to become active", 0, active1Latch.getCount());

                controller1.halt();
                shutdown1.await(10, TimeUnit.SECONDS);
                Assert.assertEquals("Controller 1 should have shut down", 0, shutdown1.getCount());
            }
        }
    }

    private ClusterController createController2(final JournalWriter writer, int instance, final Factory factory, final AtomicInteger active, final AtomicInteger standby, final AtomicInteger shutdown) {
        return new ClusterController(
                new ServerConfig() {{
                    addNode(new ServerNode(4, "localhost:7040"));
                    addNode(new ServerNode(3, "localhost:7041"));
                    addNode(new ServerNode(2, "localhost:7042"));
                    addNode(new ServerNode(1, "localhost:7043"));
                    addNode(new ServerNode(0, "localhost:7044"));
                    setHeartbeatFrequency(50);
                    setEnableMultiCast(false);
                }},
                new ClientConfig() {{
                    setEnableMultiCast(false);
                    setConnectionTimeout(30000);
                }},
                factory,
                instance,
                new ArrayList<JournalWriter>() {{
                    add(writer);
                }},
                new ClusterStatusListener() {
                    @Override
                    public void goActive() {
                        active.incrementAndGet();
                    }

                    @Override
                    public void goPassive(ServerNode activeNode) {
                        standby.incrementAndGet();
                    }

                    @Override
                    public void onShutdown() {
                        shutdown.incrementAndGet();
                    }
                }
        );
    }

    private ClusterController createControllerX(final JournalWriter writer, int instance, final Factory factory, final CountDownLatch active, final CountDownLatch standby, final CountDownLatch shutdown) {
        return new ClusterController(
                new ServerConfig() {{
                    addNode(new ServerNode(0, "localhost:7080"));
                    addNode(new ServerNode(1, "localhost:7090"));
                    setEnableMultiCast(false);
                    setHeartbeatFrequency(50);
                }},
                new ClientConfig() {{
                    setEnableMultiCast(false);
                }},
                factory,
                instance,
                new ArrayList<JournalWriter>() {{
                    add(writer);
                }},
                new ClusterStatusListener() {
                    @Override
                    public void goActive() {
                        active.countDown();
                    }

                    @Override
                    public void goPassive(ServerNode activeNode) {
                        standby.countDown();
                    }

                    @Override
                    public void onShutdown() {
                        shutdown.countDown();
                    }
                }
        );
    }

}
