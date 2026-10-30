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
import io.questdb.network.Epoll;
import io.questdb.network.EpollFacadeImpl;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.str.StringSink;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

/**
 * While a QWP connection is open, an idle network worker waits in the HTTP dispatcher's poll ({@code http.net.poll.block.timeout})
 * in place of its nap or sleep, and must still answer every request as soon as it arrives: a
 * socket event wakes the poll, and the worker never waits there while a connection is out with a
 * worker, whose re-registration could not wake it. The tests make the workers sleep, and so wait
 * in the poll, for far longer than any request takes, so a request that waited for the wait to end
 * would show; and they check that a worker did wait in {@code epoll_wait}, so that a fast answer
 * is not the trivial one of a poll that never blocks.
 * <p>
 * A network worker waits in {@code epoll_wait} when its {@code /proc/self/task/<tid>/syscall}
 * names that system call. The number is the same on every kernel build, unlike the kernel function
 * that {@code wchan} names: Debian's 6.12 kernels inline {@code ep_poll} and read
 * {@code do_epoll_wait}. A check that cannot see fails rather than passes:
 * {@link #setUpProbe()} makes sure the probe sees a thread known to wait in {@code epoll_wait}, and
 * each server's network workers must all be found.
 */
public class QwpEgressPollBlockTest extends AbstractReusedServerQwpEgressTest {
    // far above what a request takes: a request that waited for the park to end fails the test
    private static final long MAX_REQUEST_MILLIS = 500;
    // Linux keeps the first 15 bytes of a thread name: the comm of shared-network_0 is shared-network_
    private static final String NETWORK_WORKER_NAME = "shared-network_";
    private static final String PARK_MILLIS = "1000";
    private static final String PROBE_THREAD_NAME = "epoll-probe";
    // the /proc/self/task entries of the network workers of the server under test
    private final ObjList<File> networkWorkers = new ObjList<>();

    @BeforeClass
    public static void setUpProbe() throws Exception {
        Assume.assumeTrue(Os.isLinux());
        // a probe blind to epoll_wait would pass every check that no worker waits in the poll
        try (Epoll epoll = new Epoll(EpollFacadeImpl.INSTANCE, 1)) {
            final AtomicBoolean stop = new AtomicBoolean();
            // the call the dispatcher's poll makes, with nothing to wake it
            final Thread waiter = new Thread(() -> {
                while (!stop.get()) {
                    epoll.poll(10);
                }
            }, PROBE_THREAD_NAME);
            waiter.start();
            try {
                final File task = awaitThreads(PROBE_THREAD_NAME, new ObjList<>(), 1).getQuick(0);
                final long deadline = System.currentTimeMillis() + 5_000;
                String syscall;
                while (!isEpollWait(syscall = readSyscall(task))) {
                    Assert.assertTrue(
                            "the probe does not see a thread wait in epoll_wait [arch=" + Os.archName + ", syscall=" + syscall + ']',
                            System.currentTimeMillis() < deadline
                    );
                    Os.sleep(1);
                }
            } finally {
                stop.set(true);
                waiter.join();
            }
        }
    }

    @Test
    public void testBackToBackRequestsDoNotWaitForTheTimeout() throws Exception {
        // the dispatcher may block from its first idle pass on: between two requests on one
        // connection, it blocks only once the connection is back in the poll
        TestUtils.assertMemoryLeak(() -> {
            try (
                    TestServerMain ignore = startPollBlockServer(2, "100");
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
                assertBlockedPoll();
            }
        });
    }

    @Test
    public void testBlockedPollWakesOnRequest() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (
                    TestServerMain ignore = startPollBlockServer(2, "100");
                    QwpQueryClient client = QwpQueryClient.newPlainText("127.0.0.1", HTTP_PORT)
            ) {
                client.connect();
                for (int i = 0; i < 20; i++) {
                    // the request lands on a dispatcher blocked in its poll
                    assertBlockedPoll();
                    assertSelectOneFast(client, i);
                }
            }
        });
    }

    @Test
    public void testNewConnectionWakesBlockedPoll() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (
                    TestServerMain ignore = startPollBlockServer(2, "100");
                    // the worker waits in the poll only while the dispatcher has a connection
                    QwpQueryClient idle = QwpQueryClient.newPlainText("127.0.0.1", HTTP_PORT)
            ) {
                idle.connect();
                for (int i = 0; i < 5; i++) {
                    assertBlockedPoll();
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
                    TestServerMain ignore = startPollBlockServer(2, "100");
                    QwpQueryClient client = QwpQueryClient.newPlainText("127.0.0.1", HTTP_PORT)
            ) {
                client.connect();
                assertBlockedPoll();
                assertSelectOneFast(client, 0);
                final AtomicBoolean done = new AtomicBoolean();
                final AtomicInteger blocked = new AtomicInteger();
                final AtomicInteger samples = new AtomicInteger();
                final AtomicReference<Throwable> samplerError = new AtomicReference<>();
                final Thread sampler = new Thread(() -> {
                    try {
                        while (!done.get()) {
                            samples.incrementAndGet();
                            if (findWorkerInPoll() != null) {
                                blocked.incrementAndGet();
                            }
                            LockSupport.parkNanos(50_000);
                        }
                    } catch (Throwable th) {
                        samplerError.set(th);
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
                if (samplerError.get() != null) {
                    throw new AssertionError("the sampler failed", samplerError.get());
                }
                // a request delayed past a millisecond by scheduling may let a worker wait for once;
                // waiting between back-to-back requests puts a worker there in a third of samples
                Assert.assertTrue("samples: " + samples.get(), samples.get() > 100);
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
                    TestServerMain ignore = startPollBlockServer(2, "100");
                    QwpQueryClient idle = QwpQueryClient.newPlainText("127.0.0.1", HTTP_PORT);
                    QwpQueryClient client = QwpQueryClient.newPlainText("127.0.0.1", HTTP_PORT)
            ) {
                idle.connect();
                client.connect();
                assertBlockedPoll();
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
                    // the request reaches the server and wakes the poll, and its connection leaves
                    // it for the two seconds the query runs. Until the request arrives, a worker
                    // still waits there: a pause can hold the sending thread back past the sleep
                    Os.sleep(300);
                    final long deadline = System.currentTimeMillis() + 1_000;
                    String syscall;
                    while ((syscall = findWorkerInPoll()) != null) {
                        Assert.assertTrue(
                                "a worker waits in the HTTP poll while a connection is out [syscall=" + syscall + ']',
                                System.currentTimeMillis() < deadline
                        );
                        Os.sleep(1);
                    }
                    assertNoBlockedPoll(1_200, "a worker waits in the HTTP poll while a connection is out");
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
                    TestServerMain ignore = startPollBlockServer(2, "1000000000000000");
                    QwpQueryClient client = QwpQueryClient.newPlainText("127.0.0.1", HTTP_PORT)
            ) {
                client.connect();
                assertSelectOneFast(client, 0);
                assertNoBlockedPoll(1_500, "a worker waits in the HTTP poll");
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
                    TestServerMain ignore = startPollBlockServer(1, "100");
                    QwpQueryClient client = QwpQueryClient.newPlainText("127.0.0.1", HTTP_PORT)
            ) {
                client.connect();
                assertNoBlockedPoll(1_500, "a worker waits in the HTTP poll");
            }
        });
    }

    @Test
    public void testNoBlockWithPlainHttpConnection() throws Exception {
        // the wait is for QWP connections: an idle plain HTTP connection leaves the workers to
        // sleep as before
        TestUtils.assertMemoryLeak(() -> {
            try (
                    TestServerMain ignore = startPollBlockServer(2, "100");
                    Socket ignored = new Socket("127.0.0.1", HTTP_PORT)
            ) {
                assertNoBlockedPoll(1_500, "a worker waits in the HTTP poll");
                // a QWP connection next to it lets a worker wait there
                try (QwpQueryClient client = QwpQueryClient.newPlainText("127.0.0.1", HTTP_PORT)) {
                    client.connect();
                    assertBlockedPoll();
                }
            }
        });
    }

    @Test
    public void testNoBlockWithoutConnections() throws Exception {
        // with no connection there is no request to wait for: the workers sleep as before
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain ignore = startPollBlockServer(2, "100")) {
                assertNoBlockedPoll(1_500, "a worker waits in the HTTP poll");
                try (QwpQueryClient client = QwpQueryClient.newPlainText("127.0.0.1", HTTP_PORT)) {
                    client.connect();
                    assertBlockedPoll();
                }
            }
        });
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

    // The /proc/self/task entries of the threads whose name starts with namePrefix, but for those
    // in excluded, once there are count of them. A thread takes its name only once it runs.
    private static ObjList<File> awaitThreads(String namePrefix, ObjList<File> excluded, int count) {
        final long deadline = System.currentTimeMillis() + 5_000;
        while (true) {
            final ObjList<File> threads = findThreads(namePrefix, excluded);
            if (threads.size() == count) {
                return threads;
            }
            Assert.assertTrue(
                    "found " + threads.size() + " threads named " + namePrefix + "*, expected " + count,
                    threads.size() < count && System.currentTimeMillis() < deadline
            );
            Os.sleep(1);
        }
    }

    private static ObjList<File> findThreads(String namePrefix, ObjList<File> excluded) {
        final File[] tasks = new File("/proc/self/task").listFiles();
        Assert.assertNotNull("cannot list /proc/self/task", tasks);
        final ObjList<File> threads = new ObjList<>();
        for (File task : tasks) {
            if (excluded.indexOf(task) < 0 && readName(task).startsWith(namePrefix)) {
                threads.add(task);
            }
        }
        return threads;
    }

    // whether a thread waits in epoll_wait(), by the system call that readSyscall() returns: glibc's
    // epoll_wait() enters epoll_wait on x86-64 and epoll_pwait on aarch64, which has no epoll_wait
    private static boolean isEpollWait(String syscall) {
        final int end = syscall.indexOf(' ');
        if (end < 0) {
            // "running"
            return false;
        }
        final String nr = syscall.substring(0, end);
        return switch (Os.arch) {
            // epoll_wait, epoll_pwait, epoll_pwait2
            case Os.ARCH_X86_64 -> nr.equals("232") || nr.equals("281") || nr.equals("441");
            // epoll_pwait, epoll_pwait2
            case Os.ARCH_AARCH64 -> nr.equals("22") || nr.equals("441");
            default -> throw new AssertionError("no epoll_wait system call numbers [arch=" + Os.archName + ']');
        };
    }

    private static String readName(File task) {
        try {
            return Files.readString(new File(task, "comm").toPath());
        } catch (IOException e) {
            // the thread exited; a live worker that could not be read would leave awaitThreads()
            // short of its count
            return "";
        }
    }

    // The system call a thread waits in: "<nr> <6 args> <sp> <pc>" for a thread blocked in a system
    // call, "-1 <sp> <pc>" for a thread blocked outside of one, "running" for a running thread.
    private static String readSyscall(File task) {
        try {
            return Files.readString(new File(task, "syscall").toPath()).trim();
        } catch (IOException e) {
            throw new AssertionError("cannot read " + task + "/syscall", e);
        }
    }

    // asserts that, within a few seconds, a network worker waits in epoll_wait
    private void assertBlockedPoll() {
        // past the naps, into the sleep that the wait in the poll replaces
        Os.sleep(100);
        final long deadline = System.currentTimeMillis() + 5_000;
        while (findWorkerInPoll() == null) {
            if (System.currentTimeMillis() > deadline) {
                final StringSink sink = new StringSink();
                for (int i = 0, n = networkWorkers.size(); i < n; i++) {
                    final File worker = networkWorkers.getQuick(i);
                    sink.put(i == 0 ? "tid=" : ", tid=").put(worker.getName()).put(" syscall=").put(readSyscall(worker));
                }
                Assert.fail("the dispatcher never blocked in its poll [" + sink + ']');
            }
            Os.sleep(1);
        }
    }

    private void assertNoBlockedPoll(long millis, String message) {
        final long deadline = System.currentTimeMillis() + millis;
        while (System.currentTimeMillis() < deadline) {
            final String syscall = findWorkerInPoll();
            if (syscall != null) {
                Assert.fail(message + " [syscall=" + syscall + ']');
            }
            Os.sleep(1);
        }
    }

    // the system call of a network worker that waits in epoll_wait now, null when none does
    private String findWorkerInPoll() {
        for (int i = 0, n = networkWorkers.size(); i < n; i++) {
            final String syscall = readSyscall(networkWorkers.getQuick(i));
            if (isEpollWait(syscall)) {
                return syscall;
            }
        }
        return null;
    }

    // The workers nap (1 ms) after napThreshold idle passes and, about 20 naps later, sleep for
    // PARK_MILLIS; a worker that naps or sleeps may wait in the poll for as long instead. The naps
    // cover the dispatcher's own idle threshold (1 ms), within which no worker waits in the poll
    // and a worker that fell asleep would answer only when it woke.
    private TestServerMain startPollBlockServer(int networkWorkerCount, String napThreshold) {
        // a worker of the previous test's server may outlive it for a moment
        final ObjList<File> earlierWorkers = findThreads(NETWORK_WORKER_NAME, new ObjList<>());
        final TestServerMain server = startServerWithRetry(
                PropertyKey.HTTP_NET_POLL_BLOCK_TIMEOUT.getEnvVarName(), PARK_MILLIS,
                PropertyKey.SHARED_WORKER_NAP_THRESHOLD.getEnvVarName(), napThreshold,
                PropertyKey.SHARED_WORKER_SLEEP_THRESHOLD.getEnvVarName(), String.valueOf(Long.parseLong(napThreshold) + 20),
                PropertyKey.SHARED_WORKER_SLEEP_TIMEOUT.getEnvVarName(), PARK_MILLIS,
                PropertyKey.SHARED_NETWORK_WORKER_COUNT.getEnvVarName(), String.valueOf(networkWorkerCount),
                PropertyKey.SHARED_WORKER_COUNT.getEnvVarName(), "2"
        );
        try {
            networkWorkers.clear();
            networkWorkers.addAll(awaitThreads(NETWORK_WORKER_NAME, earlierWorkers, networkWorkerCount));
            return server;
        } catch (Throwable th) {
            Misc.free(server);
            throw th;
        }
    }
}
