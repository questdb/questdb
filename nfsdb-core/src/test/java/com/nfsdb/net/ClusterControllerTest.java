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

import com.nfsdb.JournalWriter;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.factory.JournalFactory;
import com.nfsdb.model.Quote;
import com.nfsdb.model.configuration.ModelConfiguration;
import com.nfsdb.net.cluster.ClusterController;
import com.nfsdb.net.cluster.ClusterStatusListener;
import com.nfsdb.net.config.ClientConfig;
import com.nfsdb.net.config.ServerConfig;
import com.nfsdb.net.config.ServerNode;
import com.nfsdb.test.tools.AbstractTest;
import com.nfsdb.test.tools.JournalTestFactory;
import com.nfsdb.test.tools.TestUtils;
import com.nfsdb.utils.Files;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class ClusterControllerTest extends AbstractTest {

    @Rule
    public final JournalTestFactory factory2 = new JournalTestFactory(ModelConfiguration.MAIN.build(Files.makeTempDir()));

    @Test
    public void testBusyFailOver() throws Exception {

        final JournalWriter<Quote> writer1 = factory.writer(Quote.class);
        final JournalWriter<Quote> writer2 = factory2.writer(Quote.class);

        final CountDownLatch active1 = new CountDownLatch(1);
        final CountDownLatch active2 = new CountDownLatch(1);
        final CountDownLatch standby2 = new CountDownLatch(1);

        final AtomicLong expected = new AtomicLong();
        final AtomicLong actual = new AtomicLong();


        ClusterController controller1 = new ClusterController(
                new ServerConfig() {{
                    addNode(new ServerNode(1, "localhost:7080"));
                    addNode(new ServerNode(2, "localhost:7090"));
                    setEnableMultiCast(false);
                }},
                new ClientConfig() {{
                    setEnableMultiCast(false);
                }},
                factory,
                1,
                new ArrayList<JournalWriter>() {{
                    add(writer1);
                }},
                new ClusterStatusListener() {
                    @Override
                    public void onNodeActive() {
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
                        } catch (JournalException e) {
                            e.printStackTrace();
                        }
                    }

                    @Override
                    public void onNodeStandingBy(ServerNode activeNode) {
                    }

                    @Override
                    public void onShutdown() {
                    }
                }
        );

        ClusterController controller2 = new ClusterController(
                new ServerConfig() {{
                    addNode(new ServerNode(1, "localhost:7080"));
                    addNode(new ServerNode(2, "localhost:7090"));
                    setEnableMultiCast(false);
                }},
                new ClientConfig() {{
                    setEnableMultiCast(false);
                }},
                factory2,
                2,
                new ArrayList<JournalWriter>() {{
                    add(writer2);
                }},
                new ClusterStatusListener() {
                    @Override
                    public void onNodeActive() {
                        try {
                            actual.set(writer2.size());
                            active2.countDown();
                        } catch (JournalException e) {
                            e.printStackTrace();
                        }
                    }

                    @Override
                    public void onNodeStandingBy(ServerNode activeNode) {
                        standby2.countDown();
                    }

                    @Override
                    public void onShutdown() {
                    }
                }
        );

        controller1.start();
        active1.await(30, TimeUnit.SECONDS);
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

    @Test
    public void testStaggeredFailOver() throws Exception {
        final CountDownLatch active1Latch = new CountDownLatch(1);
        final CountDownLatch active2Latch = new CountDownLatch(1);
        final CountDownLatch standby1Latch = new CountDownLatch(1);
        final CountDownLatch standby2Latch = new CountDownLatch(1);
        final CountDownLatch shutdown1 = new CountDownLatch(1);
        final CountDownLatch shutdown2 = new CountDownLatch(1);

        ClusterController controller1 = createController(1, factory, active1Latch, standby1Latch, shutdown1);
        controller1.start();

        active1Latch.await(5, TimeUnit.SECONDS);
        Assert.assertEquals("Node 1 is expected to be active", 0, active1Latch.getCount());
        standby1Latch.await(200, TimeUnit.MILLISECONDS);
        Assert.assertEquals("Node 1 standby callback is not expected to be called", 1, standby1Latch.getCount());

        ClusterController controller2 = createController(2, factory2, active2Latch, standby2Latch, shutdown2);
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

    @Test
    public void testStaggeredStartup() throws Exception {
        final CountDownLatch active1Latch = new CountDownLatch(1);
        final CountDownLatch active2Latch = new CountDownLatch(1);
        final CountDownLatch standby1Latch = new CountDownLatch(1);
        final CountDownLatch standby2Latch = new CountDownLatch(1);
        final CountDownLatch shutdown1 = new CountDownLatch(1);
        final CountDownLatch shutdown2 = new CountDownLatch(1);

        ClusterController controller1 = createController(1, factory, active1Latch, standby1Latch, shutdown1);
        controller1.start();

        active1Latch.await(5, TimeUnit.SECONDS);
        Assert.assertEquals("Node 1 is expected to be active", 0, active1Latch.getCount());
        standby1Latch.await(200, TimeUnit.MILLISECONDS);
        Assert.assertEquals("Node 1 standby callback is not expected to be called", 1, standby1Latch.getCount());

        ClusterController controller2 = createController(2, factory2, active2Latch, standby2Latch, shutdown2);
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

    @Test
    public void testStandalone() throws Exception {
        final CountDownLatch active = new CountDownLatch(1);
        final CountDownLatch standby = new CountDownLatch(1);
        final CountDownLatch shutdown = new CountDownLatch(1);

        ClusterController controller = createController(1, factory, active, standby, shutdown);

        controller.start();
        active.await(5, TimeUnit.SECONDS);
        Assert.assertEquals("onNodeActive() did not fire", 0, active.getCount());
        standby.await(200, TimeUnit.MILLISECONDS);
        Assert.assertEquals("onNodeStandingBy() not expected to fire", 1, standby.getCount());

        controller.halt();
        shutdown.await(5, TimeUnit.SECONDS);
        Assert.assertEquals(0, shutdown.getCount());
    }

    @Test
    public void testTiebreakFailOver() throws Exception {

        final CountDownLatch active1Latch = new CountDownLatch(1);
        final CountDownLatch active2Latch = new CountDownLatch(1);
        final CountDownLatch standby1Latch = new CountDownLatch(1);
        final CountDownLatch standby2Latch = new CountDownLatch(1);
        final CountDownLatch shutdown1 = new CountDownLatch(1);
        final CountDownLatch shutdown2 = new CountDownLatch(1);

        ClusterController controller1 = createController(1, factory, active1Latch, standby1Latch, shutdown1);
        ClusterController controller2 = createController(2, factory2, active2Latch, standby2Latch, shutdown2);

        // start two controller without pause
        controller1.start();
        controller2.start();

        factory.close();

        do {
            active1Latch.await(1, TimeUnit.MICROSECONDS);
            active2Latch.await(1, TimeUnit.MICROSECONDS);
        } while (active1Latch.getCount() > 0 && active2Latch.getCount() > 0);

        Assert.assertFalse("Two nodes are active simultaneously", active1Latch.getCount() == 0 && active2Latch.getCount() == 0);

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

    private ClusterController createController(int instance, final JournalFactory fact, final CountDownLatch active, final CountDownLatch standby, final CountDownLatch shutdown) throws JournalException {
        return new ClusterController(
                new ServerConfig() {{
                    addNode(new ServerNode(1, "localhost:7080"));
                    addNode(new ServerNode(2, "localhost:7090"));
                    setEnableMultiCast(false);
                }},
                new ClientConfig() {{
                    setEnableMultiCast(false);
                }},
                fact,
                instance,
                new ArrayList<JournalWriter>() {{
                    add(fact.writer(Quote.class));
                }},
                new ClusterStatusListener() {
                    @Override
                    public void onNodeActive() {
                        active.countDown();
                    }

                    @Override
                    public void onNodeStandingBy(ServerNode activeNode) {
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
