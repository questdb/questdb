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

package io.questdb.test.cutlass;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.DefaultIODispatcherConfiguration;
import io.questdb.network.IOContext;
import io.questdb.network.IODispatcher;
import io.questdb.network.IODispatchers;
import io.questdb.network.IOOperation;
import io.questdb.network.IORequestProcessor;
import io.questdb.network.Net;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.network.PlainSocketFactory;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static io.questdb.test.tools.TestUtils.assertMemoryLeak;

/**
 * Regression coverage for a server-side socket leak on shutdown.
 * <p>
 * When a query parks on a worker continuation (e.g. {@code sleep()}), its connection
 * context is checked out of the pool and detached from every collection the dispatcher
 * tracks. If it unwinds after the dispatcher has already closed, its terminal
 * {@code disconnect()} / {@code registerChannel()} lands on a closed dispatcher. Before
 * the fix those calls silently dropped the context -- {@code close()} could not see it
 * either -- so its socket was freed by nobody and leaked for the lifetime of the JVM.
 * This is what made {@code ServerMainSleepTest.testSleepCancelledByConnectionDrop} flake
 * on its {@code assertMemoryLeak} check.
 * <p>
 * These tests reproduce the detached-context state directly at the dispatcher level, so
 * the leak is deterministic rather than dependent on the shutdown scheduling race.
 */
public class IODispatcherCloseParkedContextTest {
    private static final Log LOG = LogFactory.getLog(IODispatcherCloseParkedContextTest.class);

    @Rule
    public Timeout timeout = Timeout.builder()
            .withTimeout(60, TimeUnit.SECONDS)
            .withLookingForStuckThread(true)
            .build();

    @Test
    public void testCloseFreesParkedContextOnDisconnect() throws Exception {
        assertParkedContextFreedAfterClose(false, 1);
    }

    @Test
    public void testCloseFreesParkedContextOnRegisterChannel() throws Exception {
        assertParkedContextFreedAfterClose(true, 1);
    }

    @Test
    public void testDoubleDisconnectAfterCloseFreesContextOnce() throws Exception {
        assertParkedContextFreedAfterClose(false, 2);
    }

    @Test
    public void testDoubleRegisterChannelAfterCloseFreesContextOnce() throws Exception {
        assertParkedContextFreedAfterClose(true, 2);
    }

    private void assertParkedContextFreedAfterClose(boolean reRegisterInsteadOfDisconnect, int terminalCalls) throws Exception {
        assertMemoryLeak(() -> {
            final AtomicInteger accepted = new AtomicInteger();
            final AtomicReference<TestContext> parked = new AtomicReference<>();

            // Emulates a parked query: processIOQueue already consumed the context from the
            // event queue, and this processor deliberately does not re-register it, so on
            // return the context is checked out and tracked by none of the dispatcher's
            // collections -- exactly the state a sleep() suspend leaves behind.
            final IORequestProcessor<TestContext> parkingProcessor = (operation, context, dispatcher) -> {
                parked.compareAndSet(null, context);
                return true;
            };

            IODispatcher<TestContext> dispatcher = IODispatchers.create(
                    new DefaultIODispatcherConfiguration() {
                        @Override
                        public int getBindPort() {
                            // Ephemeral port: isolated from any other listener on the default port.
                            return 0;
                        }
                    },
                    fd -> {
                        accepted.incrementAndGet();
                        return new TestContext(fd);
                    }
            );

            final long buf = Unsafe.malloc(1, MemoryTag.NATIVE_DEFAULT);
            Unsafe.putByte(buf, (byte) 'x');
            final long sockAddr = Net.sockaddr("127.0.0.1", dispatcher.getPort());
            final long clientFd = Net.socketTcp(true);
            boolean dispatcherClosed = false;
            try {
                Net.configureNonBlocking(clientFd);
                Net.connect(clientFd, sockAddr);

                // Accept the connection (the factory bumps `accepted`).
                while (accepted.get() == 0) {
                    dispatcher.run();
                }

                // Send a byte so the dispatcher fires a READ and hands the context to the
                // parking processor, which checks it out and leaves it un-re-registered.
                Assert.assertEquals(1, Net.send(clientFd, buf, 1));
                while (parked.get() == null) {
                    dispatcher.run();
                    dispatcher.processIOQueue(parkingProcessor);
                }

                // Close the dispatcher first: its sweeps cannot see the checked-out context.
                dispatcher.close();
                dispatcherClosed = true;

                // The context's terminal step once it unwinds at shutdown. On a closed dispatcher
                // both paths must free it; before the fix they silently dropped it and its socket
                // leaked. A second call (terminalCalls == 2) stands in for a real concurrent
                // close()-sweep reaching the same context: doDisconnect() must free it exactly once,
                // never double-close the fd or double-free the buffers.
                for (int i = 0; i < terminalCalls; i++) {
                    if (reRegisterInsteadOfDisconnect) {
                        dispatcher.registerChannel(parked.get(), IOOperation.READ);
                    } else {
                        dispatcher.disconnect(parked.get(), IODispatcher.DISCONNECT_REASON_TEST);
                    }
                }

                // Counted once at accept(); after the free the count is balanced, and a redundant
                // terminal call must not drive it negative.
                Assert.assertEquals(0, dispatcher.getConnectionCount());
            } finally {
                if (!dispatcherClosed) {
                    dispatcher.close();
                }
                Net.close(clientFd);
                Net.freeSockAddr(sockAddr);
                Unsafe.free(buf, 1, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    private static class TestContext extends IOContext<TestContext> {
        // A native allocation so a leaked context trips assertMemoryLeak's memory check as
        // well as its file-descriptor check, mirroring a real connection context's buffers.
        private final long buffer = Unsafe.malloc(4, MemoryTag.NATIVE_DEFAULT);

        private TestContext(long fd) {
            super(PlainSocketFactory.INSTANCE, NetworkFacadeImpl.INSTANCE, LOG);
            socket.of(fd);
        }

        @Override
        public void close() {
            Unsafe.free(buffer, 4, MemoryTag.NATIVE_DEFAULT);
            super.close();
        }

        @Override
        public boolean invalid() {
            return false;
        }
    }
}
