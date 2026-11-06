/*+*****************************************************************************
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

package io.questdb.test.network;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.DefaultIODispatcherConfiguration;
import io.questdb.network.EpollAccessor;
import io.questdb.network.EpollFacade;
import io.questdb.network.EpollFacadeImpl;
import io.questdb.network.IOContext;
import io.questdb.network.IODispatcherLinux;
import io.questdb.network.IOOperation;
import io.questdb.network.IORequestProcessor;
import io.questdb.network.Net;
import io.questdb.network.NetworkFacade;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.network.PlainSocket;
import io.questdb.std.LongList;
import io.questdb.std.Os;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

/**
 * Drives {@link IODispatcherLinux} one poll round at a time against an in-memory epoll and
 * network, so that a single round can carry several events, a listener event, a duplicate event,
 * a TLS failure or a failing epoll_ctl. The fake epoll rejects the calls the kernel rejects: adding
 * an fd that is already registered, and modifying or deleting one that isn't. Every test asserts
 * that no call was rejected, which catches new-connection rows registered at the wrong position.
 */
public class IODispatcherLinuxRoundTest {
    private static final long FIRST_CONNECTION_FD = 1000;
    private static final long LISTENER_FD = 999;
    private static final Log LOG = LogFactory.getLog(IODispatcherLinuxRoundTest.class);
    @Rule
    public Timeout timeout = Timeout.builder()
            .withTimeout(60, TimeUnit.SECONDS)
            .withLookingForStuckThread(true)
            .build();

    @Before
    public void setUp() {
        // EpollAccessor loads its constants from the Linux native library
        Assume.assumeTrue(Os.isLinux());
    }

    @Test
    public void testAbortedRoundKeepsNewConnectionsRegistered() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (Harness h = new Harness(0)) {
                final long[] fds = h.connect(3);
                final long a = fds[0];
                final long b = fds[1];

                // A is handed to a worker, then B's write event makes the dispatcher re-arm B,
                // and that epoll_ctl fails, so the round ends with A's row still marked
                h.epoll.throwOnCtlFd = b;
                try {
                    h.round(read(a), write(b));
                    Assert.fail("injected epoll_ctl failure expected");
                } catch (RuntimeException e) {
                    TestUtils.assertContains(e.getMessage(), "injected epoll_ctl failure");
                }
                h.epoll.throwOnCtlFd = Long.MIN_VALUE;
                h.drain();
                Assert.assertEquals(1, h.publishedCount(a));

                final long c = h.network.queueIncoming(1)[0];
                h.round(listener());
                Assert.assertEquals(1, h.epoll.addCount(c));

                h.round(read(c));
                Assert.assertEquals(1, h.publishedCount(c));
                Assert.assertEquals(0, h.epoll.rejectedCount);
            }
        });
    }

    @Test
    public void testDuplicateEventPublishedOnce() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (Harness h = new Harness(0)) {
                final long[] fds = h.connect(3);
                final long a = fds[0];
                final long c = h.network.queueIncoming(1)[0];

                h.round(read(a), readAgain(a), listener());

                Assert.assertEquals(1, h.publishedCount(a));
                Assert.assertEquals(1, h.epoll.addCount(c));
                h.round(read(c));
                Assert.assertEquals(1, h.publishedCount(c));
                Assert.assertEquals(0, h.epoll.rejectedCount);
            }
        });
    }

    @Test
    public void testIdleTimeoutsInRoundWithEvents() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (Harness h = new Harness(50)) {
                h.clock.tick = 0;
                final long[] old = h.connect(2);
                h.clock.tick = 40;
                final long[] fresh = h.connect(3);

                // at 60 the rows created at 0 are past the idle timeout and those created at 40
                // aren't; the idle scan runs in the same round as fresh[0]'s event
                h.clock.tick = 60;
                h.round(read(fresh[0]));

                Assert.assertEquals(1, h.publishedCount(fresh[0]));
                Assert.assertEquals(1, h.network.closedCount(old[0]));
                Assert.assertEquals(1, h.network.closedCount(old[1]));
                Assert.assertEquals(0, h.network.closedCount(fresh[1]));
                Assert.assertEquals(0, h.network.closedCount(fresh[2]));
                Assert.assertEquals(1, h.epoll.ctlCount(fresh[1]));
                Assert.assertEquals(1, h.epoll.ctlCount(fresh[2]));
                Assert.assertEquals(0, h.epoll.rejectedCount);
            }
        });
    }

    @Test
    public void testManyEventsAndAcceptInOneRound() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (Harness h = new Harness(0)) {
                final long[] fds = h.connect(8);
                final long a = fds[0];
                final long b = fds[1];
                final long d = fds[2];
                final long e = fds[3];
                final long f = fds[4];
                final long fId = h.epoll.idOf(f);
                final long[] incoming = h.network.queueIncoming(3);

                // the listener sits between the events, so the accepted rows are appended while
                // other rows are marked; F gets a write event while it waits to read
                h.round(read(a), read(b), listener(), read(d), read(e), write(f));

                for (long fd : new long[]{a, b, d, e}) {
                    Assert.assertEquals(1, h.publishedCount(fd));
                }
                Assert.assertEquals(0, h.publishedCount(f));
                Assert.assertEquals(fId, h.epoll.idOf(f));
                Assert.assertTrue(h.epoll.isArmed(f));
                for (long fd : incoming) {
                    Assert.assertEquals(1, h.epoll.addCount(fd));
                }
                for (int i = 5; i < 8; i++) {
                    Assert.assertEquals(1, h.epoll.ctlCount(fds[i]));
                    Assert.assertEquals(0, h.publishedCount(fds[i]));
                }

                // re-arm the handled connections, then fire the new ones and the re-armed ones
                h.settle();
                h.round(read(incoming[0]), read(incoming[1]), read(incoming[2]), read(f), read(a));
                for (long fd : incoming) {
                    Assert.assertEquals(1, h.publishedCount(fd));
                }
                Assert.assertEquals(1, h.publishedCount(f));
                Assert.assertEquals(2, h.publishedCount(a));
                Assert.assertEquals(0, h.epoll.rejectedCount);
            }
        });
    }

    @Test
    public void testTlsFailureInRound() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (Harness h = new Harness(0, 10)) {
                final long[] fds = h.connect(3);
                final long a = fds[0];
                final long b = fds[1];
                final long x = fds[2];
                final long c = h.network.queueIncoming(1)[0];
                h.socket(b).isTlsFailing = true;

                h.round(read(a), read(b), listener());

                Assert.assertEquals(1, h.publishedCount(a));
                Assert.assertEquals(0, h.publishedCount(b));
                Assert.assertEquals(1, h.network.closedCount(b));
                Assert.assertEquals(1, h.epoll.addCount(c));
                h.round(read(c));
                Assert.assertEquals(1, h.publishedCount(c));
                h.settle();

                // every row gets a heartbeat, which removes its fd from epoll and adds it back
                // once the worker answers; a row left behind by B would try to remove fd -1
                h.clock.tick = 20;
                h.settle();
                h.settle();
                for (long fd : new long[]{a, x, c}) {
                    Assert.assertEquals(2, h.epoll.addCount(fd));
                }
                Assert.assertEquals(0, h.epoll.rejectedCount);
            }
        });
    }

    private static Event listener() {
        return new Event(LISTENER_FD, EpollAccessor.EPOLLIN, false);
    }

    private static Event read(long fd) {
        return new Event(fd, EpollAccessor.EPOLLIN, false);
    }

    // the same event again in the same poll, although the first one disarmed the fd
    private static Event readAgain(long fd) {
        return new Event(fd, EpollAccessor.EPOLLIN, true);
    }

    private static Event write(long fd) {
        return new Event(fd, EpollAccessor.EPOLLOUT, false);
    }

    private record Ctl(int op, long fd, long id) {
    }

    private record Event(long fd, int events, boolean isForced) {
    }

    /**
     * An epoll interest list kept in memory. A registration stays until it is deleted or its fd is
     * closed; a one-shot registration is disarmed when it fires and re-armed by EPOLL_CTL_MOD.
     * The epoll fd itself is real, so that the dispatcher can close it as usual.
     */
    private static final class FakeEpoll implements EpollFacade {
        // struct epoll_event is 12 bytes on x86-64, where it is packed, and 16 on aarch64
        private static final int EVENT_SIZE = EpollAccessor.DATA_OFFSET + Long.BYTES;
        private final ArrayList<Ctl> ctlLog = new ArrayList<>();
        private final NetworkFacade nf;
        private final HashMap<Long, Registration> registrations = new HashMap<>();
        private final ArrayList<Event> script = new ArrayList<>();
        int rejectedCount;
        // Long.MIN_VALUE matches no fd; -1 is the fd of a closed context
        long throwOnCtlFd = Long.MIN_VALUE;

        FakeEpoll(NetworkFacade nf) {
            this.nf = nf;
        }

        @Override
        public int epollCreate() {
            return EpollFacadeImpl.INSTANCE.epollCreate();
        }

        @Override
        public int epollCtl(long epfd, int op, long fd, long eventPtr) {
            if (fd == throwOnCtlFd) {
                throw new RuntimeException("injected epoll_ctl failure [fd=" + fd + ']');
            }
            final int events = Unsafe.getInt(eventPtr + EpollAccessor.EVENTS_OFFSET);
            final long id = Unsafe.getLong(eventPtr + EpollAccessor.DATA_OFFSET);
            final Registration registration = registrations.get(fd);
            if (op == EpollAccessor.EPOLL_CTL_ADD) {
                if (registration != null) {
                    rejectedCount++;
                    return -1;
                }
                registrations.put(fd, new Registration(id, events));
            } else if (op == EpollAccessor.EPOLL_CTL_MOD) {
                if (registration == null) {
                    rejectedCount++;
                    return -1;
                }
                registration.id = id;
                registration.events = events;
                registration.isArmed = true;
            } else if (op == EpollAccessor.EPOLL_CTL_DEL) {
                if (registration == null) {
                    rejectedCount++;
                    return -1;
                }
                registrations.remove(fd);
            }
            ctlLog.add(new Ctl(op, fd, id));
            return 0;
        }

        @Override
        public int epollWait(long epfd, long eventPtr, int eventCount, int timeout) {
            int n = 0;
            for (int i = 0, k = script.size(); i < k; i++) {
                final Event event = script.get(i);
                final Registration registration = registrations.get(event.fd());
                if (registration == null || !(registration.isArmed || event.isForced())) {
                    continue;
                }
                Assert.assertTrue(n < eventCount);
                final long p = eventPtr + (long) n++ * EVENT_SIZE;
                Unsafe.putInt(p + EpollAccessor.EVENTS_OFFSET, event.events());
                Unsafe.putLong(p + EpollAccessor.DATA_OFFSET, registration.id);
                if ((registration.events & EpollAccessor.EPOLLONESHOT) != 0) {
                    registration.isArmed = false;
                }
            }
            script.clear();
            return n;
        }

        @Override
        public int errno() {
            return 0;
        }

        @Override
        public NetworkFacade getNetworkFacade() {
            return nf;
        }

        int addCount(long fd) {
            int count = 0;
            for (int i = 0, n = ctlLog.size(); i < n; i++) {
                final Ctl ctl = ctlLog.get(i);
                if (ctl.fd() == fd && ctl.op() == EpollAccessor.EPOLL_CTL_ADD) {
                    count++;
                }
            }
            return count;
        }

        int ctlCount(long fd) {
            int count = 0;
            for (int i = 0, n = ctlLog.size(); i < n; i++) {
                if (ctlLog.get(i).fd() == fd) {
                    count++;
                }
            }
            return count;
        }

        // closing an fd removes it from the interest list
        void forget(long fd) {
            registrations.remove(fd);
        }

        long idOf(long fd) {
            return registrations.get(fd).id;
        }

        boolean isArmed(long fd) {
            return registrations.get(fd).isArmed;
        }

        void script(Event... events) {
            for (Event event : events) {
                script.add(event);
            }
        }
    }

    /**
     * Hands out the fds queued by the test from accept(), and keeps everything else about those
     * fds in memory. Calls on any other fd, such as the dispatcher's epoll fd, go to the real
     * network facade.
     */
    private static final class FakeNetwork extends NetworkFacadeImpl {
        private final LongList closed = new LongList();
        private final LongList incoming = new LongList();
        FakeEpoll epoll;
        private long nextFd = FIRST_CONNECTION_FD;

        @Override
        public long accept(long serverFd) {
            if (incoming.size() == 0) {
                return -1;
            }
            final long fd = incoming.getQuick(0);
            incoming.removeIndex(0);
            return fd;
        }

        @Override
        public boolean bindTcp(long fd, int address, int port) {
            return true;
        }

        @Override
        public int close(long fd) {
            if (isFake(fd)) {
                closed.add(fd);
                epoll.forget(fd);
                return 0;
            }
            return super.close(fd);
        }

        @Override
        public void close(long fd, Log logger) {
            if (isFake(fd)) {
                close(fd);
            } else {
                super.close(fd, logger);
            }
        }

        @Override
        public void configureKeepAlive(long fd) {
        }

        @Override
        public void configureNoLinger(long fd) {
        }

        @Override
        public int configureNonBlocking(long fd) {
            return 0;
        }

        @Override
        public int errno() {
            return Net.EWOULDBLOCK;
        }

        @Override
        public long getPeerIP(long fd) {
            return 0x7f000001L;
        }

        @Override
        public void listen(long serverFd, int backlog) {
        }

        @Override
        public int setTcpNoDelay(long fd, boolean noDelay) {
            return 0;
        }

        @Override
        public long socketTcp(boolean blocking) {
            return LISTENER_FD;
        }

        int closedCount(long fd) {
            int count = 0;
            for (int i = 0, n = closed.size(); i < n; i++) {
                if (closed.getQuick(i) == fd) {
                    count++;
                }
            }
            return count;
        }

        long[] queueIncoming(int count) {
            final long[] fds = new long[count];
            for (int i = 0; i < count; i++) {
                fds[i] = nextFd++;
                incoming.add(fds[i]);
            }
            return fds;
        }

        private boolean isFake(long fd) {
            return fd == LISTENER_FD || (fd >= FIRST_CONNECTION_FD && fd < nextFd);
        }
    }

    /**
     * A dispatcher on the fakes, stepped from the test thread. The processor stands in for the
     * workers: it records each published read or write and re-arms the connection for reading,
     * and it answers each heartbeat. Either takes effect at the end of the next round.
     */
    private static final class Harness implements QuietCloseable {
        final TestClock clock = new TestClock();
        final HashMap<Long, TestContext> contexts = new HashMap<>();
        final IODispatcherLinux<TestContext> dispatcher;
        final FakeEpoll epoll;
        final FakeNetwork network;
        final LongList published = new LongList();
        private final IORequestProcessor<TestContext> processor;

        Harness(long idleTimeout) {
            this(idleTimeout, -1);
        }

        Harness(long idleTimeout, long heartbeatInterval) {
            network = new FakeNetwork();
            epoll = new FakeEpoll(network);
            network.epoll = epoll;
            processor = (operation, context, d) -> {
                if (operation == IOOperation.HEARTBEAT) {
                    d.registerChannel(context, IOOperation.HEARTBEAT);
                } else {
                    published.add(context.getFd());
                    d.registerChannel(context, IOOperation.READ);
                }
                return true;
            };
            dispatcher = new IODispatcherLinux<>(
                    new DefaultIODispatcherConfiguration() {
                        @Override
                        public MillisecondClock getClock() {
                            return clock;
                        }

                        @Override
                        public EpollFacade getEpollFacade() {
                            return epoll;
                        }

                        @Override
                        public long getHeartbeatInterval() {
                            return heartbeatInterval;
                        }

                        @Override
                        public NetworkFacade getNetworkFacade() {
                            return network;
                        }

                        @Override
                        public long getTimeout() {
                            return idleTimeout;
                        }
                    },
                    fd -> {
                        final TestContext context = new TestContext(network).of(fd);
                        contexts.put(fd, context);
                        return context;
                    }
            );
        }

        @Override
        public void close() {
            dispatcher.close();
        }

        long[] connect(int count) {
            final long[] fds = network.queueIncoming(count);
            round(listener());
            return fds;
        }

        void drain() {
            dispatcher.drainIOQueue(processor);
        }

        int publishedCount(long fd) {
            int count = 0;
            for (int i = 0, n = published.size(); i < n; i++) {
                if (published.getQuick(i) == fd) {
                    count++;
                }
            }
            return count;
        }

        void round(Event... events) {
            epoll.script(events);
            dispatcher.run();
            drain();
        }

        // a round without events, which applies the re-arms queued by the processor
        void settle() {
            round();
        }

        TestSocket socket(long fd) {
            return (TestSocket) contexts.get(fd).getSocket();
        }
    }

    private static final class Registration {
        int events;
        long id;
        boolean isArmed = true;

        Registration(long id, int events) {
            this.id = id;
            this.events = events;
        }
    }

    private static final class TestClock implements MillisecondClock {
        long tick;

        @Override
        public long getTicks() {
            return tick;
        }
    }

    private static final class TestContext extends IOContext<TestContext> {
        TestContext(NetworkFacade nf) {
            super(TestSocket::new, nf, LOG);
        }
    }

    private static final class TestSocket extends PlainSocket {
        boolean isTlsFailing;

        TestSocket(NetworkFacade nf, Log log) {
            super(nf, log);
        }

        @Override
        public int tlsIO(int readinessFlags) {
            return isTlsFailing ? -1 : 0;
        }
    }
}
