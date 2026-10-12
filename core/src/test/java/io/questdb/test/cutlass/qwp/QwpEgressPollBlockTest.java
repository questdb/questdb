/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.test.cutlass.qwp;

import io.questdb.PropertyKey;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;
import io.questdb.client.cutlass.qwp.client.QwpQueryClient;
import io.questdb.std.Os;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

/**
 * While a QWP connection is open, an idle network worker waits in the HTTP dispatcher's poll ({@code http.net.poll.block.timeout})
 * in place of its nap or sleep, and must still answer every request as soon as it arrives: a
 * socket event wakes the poll, and the worker never waits there while a connection is out with a
 * worker, whose re-registration could not wake it. The tests make the workers sleep, and so wait
 * in the poll, for far longer than any request takes, so a request that waited for the wait to end
 * would show; and they check that a worker did wait in {@code epoll_wait}, so that a fast answer
 * is not the trivial one of a poll that never blocks.
 */
public class QwpEgressPollBlockTest extends AbstractReusedServerQwpEgressTest {
    // far above what a request takes: a request that waited for the park to end fails the test
    private static final long MAX_REQUEST_MILLIS = 500;
    private static final String PARK_MILLIS = "1000";

    @Override
    @Before
    public void setUp() {
        Assume.assumeTrue(Os.isLinux());
        super.setUp();
    }

    @Test
    public void testBackToBackRequestsDoNotWaitForTheTimeout() throws Exception {
        // the dispatcher may block from its first idle pass on: between two requests on one
        // connection, it blocks only once the connection is back in the poll
        TestUtils.assertMemoryLeak(() -> {
            try (
                    TestServerMain ignore = startPollBlockServer("2", "100");
                    QwpQueryClient client = QwpQueryClient.newPlainText("127.0.0.1", HTTP_PORT)
            ) {
                client.connect();
                final long start = System.nanoTime();
                for (int i = 0; i < 100; i++) {
                    assertSelectOneFast(client, i);
                }
                Assert.assertTrue(
                        "100 requests back to back took " + (System.nanoTime() - start) / 1_000_000 + "ms",
                        System.nanoTime() - start < 10 * MAX_REQUEST_MILLIS * 1_000_000L
                );
                Assert.assertTrue("the dispatcher never blocked in its poll", awaitBlockedPoll());
            }
        });
    }

    @Test
    public void testBlockedPollWakesOnRequest() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (
                    TestServerMain ignore = startPollBlockServer("2", "100");
                    QwpQueryClient client = QwpQueryClient.newPlainText("127.0.0.1", HTTP_PORT)
            ) {
                client.connect();
                for (int i = 0; i < 20; i++) {
                    // the request lands on a dispatcher blocked in its poll
                    Assert.assertTrue("the dispatcher never blocked in its poll", awaitBlockedPoll());
                    assertSelectOneFast(client, i);
                }
            }
        });
    }

    @Test
    public void testNewConnectionWakesBlockedPoll() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (
                    TestServerMain ignore = startPollBlockServer("2", "100");
                    // the worker waits in the poll only while the dispatcher has a connection
                    QwpQueryClient idle = QwpQueryClient.newPlainText("127.0.0.1", HTTP_PORT)
            ) {
                idle.connect();
                for (int i = 0; i < 5; i++) {
                    Assert.assertTrue("the dispatcher never blocked in its poll", awaitBlockedPoll());
                    final long start = System.nanoTime();
                    try (QwpQueryClient client = QwpQueryClient.newPlainText("127.0.0.1", HTTP_PORT)) {
                        client.connect();
                        assertSelectOneFast(client, i);
                    }
                    final long millis = (System.nanoTime() - start) / 1_000_000;
                    Assert.assertTrue("connect and request " + i + " took " + millis + "ms", millis < 2 * MAX_REQUEST_MILLIS);
                }
            }
        });
    }

    @Test
    public void testNoBlockUnderBackToBackRequests() throws Exception {
        // a worker that just served a request spins and picks the next one up; one waiting in the
        // poll would hold the dispatcher and keep it from doing so. Requests well under a
        // millisecond apart never find a worker in the poll
        TestUtils.assertMemoryLeak(() -> {
            try (
                    TestServerMain ignore = startPollBlockServer("2", "100");
                    QwpQueryClient client = QwpQueryClient.newPlainText("127.0.0.1", HTTP_PORT)
            ) {
                client.connect();
                Assert.assertTrue("the dispatcher never blocked in its poll", awaitBlockedPoll());
                assertSelectOneFast(client, 0);
                final AtomicBoolean done = new AtomicBoolean();
                final AtomicInteger blocked = new AtomicInteger();
                final AtomicInteger samples = new AtomicInteger();
                final Thread sampler = new Thread(() -> {
                    while (!done.get()) {
                        samples.incrementAndGet();
                        if (isPollBlocked()) {
                            blocked.incrementAndGet();
                        }
                        LockSupport.parkNanos(50_000);
                    }
                });
                sampler.start();
                try {
                    final long deadline = System.nanoTime() + 1_500_000_000L;
                    for (int i = 1; System.nanoTime() < deadline; i++) {
                        assertSelectOneFast(client, i);
                        LockSupport.parkNanos(200_000);
                    }
                } finally {
                    done.set(true);
                    sampler.join();
                }
                // a request delayed past a millisecond by scheduling may let a worker wait for once;
                // waiting between back-to-back requests puts a worker there in a third of samples
                Assert.assertTrue(samples.get() > 100);
                Assert.assertTrue(
                        "samples with a worker in the poll: " + blocked.get() + " of " + samples.get(),
                        blocked.get() * 100 < samples.get()
                );
            }
        });
    }

    @Test
    public void testNoBlockWhileConnectionIsOut() throws Exception {
        // a connection out with a worker comes back through the interest queue, which cannot wake
        // the poll: while one is out, no worker waits there, though another connection is armed
        TestUtils.assertMemoryLeak(() -> {
            try (
                    TestServerMain ignore = startPollBlockServer("2", "100");
                    QwpQueryClient idle = QwpQueryClient.newPlainText("127.0.0.1", HTTP_PORT);
                    QwpQueryClient client = QwpQueryClient.newPlainText("127.0.0.1", HTTP_PORT)
            ) {
                idle.connect();
                client.connect();
                Assert.assertTrue("the dispatcher never blocked in its poll", awaitBlockedPoll());
                final Throwable[] error = {null};
                final Thread sleeper = new Thread(() -> {
                    try {
                        client.execute("sleep(2)", new QwpColumnBatchHandler() {
                            @Override
                            public void onBatch(QwpColumnBatch batch) {
                            }

                            @Override
                            public void onEnd(long totalRows) {
                            }

                            @Override
                            public void onError(byte status, String message) {
                                error[0] = new AssertionError(message);
                            }
                        });
                    } catch (Throwable t) {
                        error[0] = t;
                    }
                });
                sleeper.start();
                try {
                    // the request reaches the server and leaves the poll
                    Os.sleep(300);
                    final long deadline = System.currentTimeMillis() + 1_200;
                    while (System.currentTimeMillis() < deadline) {
                        Assert.assertFalse("a worker waits in the HTTP poll while a connection is out", isPollBlocked());
                        Os.sleep(1);
                    }
                } finally {
                    sleeper.join();
                }
                Assert.assertNull(error[0]);
                assertSelectOneFast(client, 1);
            }
        });
    }

    @Test
    public void testNoBlockWhileWorkersSpin() throws Exception {
        // a worker waits in the poll only in place of its nap or sleep: workers that never back
        // off that far never wait there, however idle the dispatcher
        TestUtils.assertMemoryLeak(() -> {
            try (
                    TestServerMain ignore = startPollBlockServer("2", "1000000000000000");
                    QwpQueryClient client = QwpQueryClient.newPlainText("127.0.0.1", HTTP_PORT)
            ) {
                client.connect();
                assertSelectOneFast(client, 0);
                assertNoBlockedPoll();
                assertSelectOneFast(client, 1);
            }
        });
    }

    @Test
    public void testNoBlockWithOneWorker() throws Exception {
        // a lone fiber-hosting worker is the pool's only fiber wake target, which a worker waiting
        // in the poll cannot be: it sleeps as before
        TestUtils.assertMemoryLeak(() -> {
            try (
                    TestServerMain ignore = startPollBlockServer("1", "100");
                    QwpQueryClient client = QwpQueryClient.newPlainText("127.0.0.1", HTTP_PORT)
            ) {
                client.connect();
                assertNoBlockedPoll();
            }
        });
    }

    @Test
    public void testNoBlockWithPlainHttpConnection() throws Exception {
        // the wait is for QWP connections: an idle plain HTTP connection leaves the workers to
        // sleep as before
        TestUtils.assertMemoryLeak(() -> {
            try (
                    TestServerMain ignore = startPollBlockServer("2", "100");
                    Socket ignored = new Socket("127.0.0.1", HTTP_PORT)
            ) {
                assertNoBlockedPoll();
            }
        });
    }

    @Test
    public void testNoBlockWithoutConnections() throws Exception {
        // with no connection there is no request to wait for: the workers sleep as before
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain ignore = startPollBlockServer("2", "100")) {
                assertNoBlockedPoll();
                try (QwpQueryClient client = QwpQueryClient.newPlainText("127.0.0.1", HTTP_PORT)) {
                    client.connect();
                    Assert.assertTrue("the dispatcher never blocked in its poll", awaitBlockedPoll());
                }
            }
        });
    }

    private static void assertNoBlockedPoll() {
        final long deadline = System.currentTimeMillis() + 1_500;
        while (System.currentTimeMillis() < deadline) {
            Assert.assertFalse("a worker waits in the HTTP poll", isPollBlocked());
            Os.sleep(1);
        }
    }

    private static void assertSelectOneFast(QwpQueryClient client, int i) {
        final long[] value = {0};
        final long start = System.nanoTime();
        client.execute("SELECT " + i + "::long", new QwpColumnBatchHandler() {
            @Override
            public void onBatch(QwpColumnBatch batch) {
                value[0] = batch.getLongValue(0, 0);
            }

            @Override
            public void onEnd(long totalRows) {
            }

            @Override
            public void onError(byte status, String message) {
                Assert.fail("SELECT failed (status=" + status + "): " + message);
            }
        });
        final long millis = (System.nanoTime() - start) / 1_000_000;
        Assert.assertEquals(i, value[0]);
        Assert.assertTrue("request " + i + " took " + millis + "ms", millis < MAX_REQUEST_MILLIS);
    }

    // whether, within a few seconds, a network worker of this process waits in epoll_wait
    private static boolean awaitBlockedPoll() {
        // past the naps, into the sleep that the wait in the poll replaces
        Os.sleep(100);
        final long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline) {
            if (isPollBlocked()) {
                return true;
            }
            Os.sleep(1);
        }
        return false;
    }

    // whether a network worker of this process waits in epoll_wait now
    private static boolean isPollBlocked() {
        final File[] tasks = new File("/proc/self/task").listFiles();
        if (tasks != null) {
            for (File task : tasks) {
                try {
                    if (Files.readString(new File(task, "comm").toPath()).startsWith("shared-network")
                            && "ep_poll".equals(Files.readString(new File(task, "wchan").toPath()).trim())) {
                        return true;
                    }
                } catch (IOException ignore) {
                    // the thread exited
                }
            }
        }
        return false;
    }

    // The workers nap (1 ms) after napThreshold idle passes and, about 20 naps later, sleep for
    // PARK_MILLIS; a worker that naps or sleeps may wait in the poll for as long instead. The naps
    // cover the dispatcher's own idle threshold (1 ms), within which no worker waits in the poll
    // and a worker that fell asleep would answer only when it woke.
    private static TestServerMain startPollBlockServer(String networkWorkers, String napThreshold) {
        return startServerWithRetry(
                PropertyKey.HTTP_NET_POLL_BLOCK_TIMEOUT.getEnvVarName(), PARK_MILLIS,
                PropertyKey.SHARED_WORKER_NAP_THRESHOLD.getEnvVarName(), napThreshold,
                PropertyKey.SHARED_WORKER_SLEEP_THRESHOLD.getEnvVarName(), String.valueOf(Long.parseLong(napThreshold) + 20),
                PropertyKey.SHARED_WORKER_SLEEP_TIMEOUT.getEnvVarName(), PARK_MILLIS,
                PropertyKey.SHARED_NETWORK_WORKER_COUNT.getEnvVarName(), networkWorkers,
                PropertyKey.SHARED_WORKER_COUNT.getEnvVarName(), "2"
        );
    }
}
