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
import io.questdb.network.IORequestProcessor;
import io.questdb.network.Net;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.network.PlainSocketFactory;
import io.questdb.network.TlsSessionInitFailedException;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.test.tools.TestUtils.assertMemoryLeak;

/**
 * Verifies I/O dispatcher cleanup when request processing fails.
 */
public class IODispatcherProcessErrorTest {
    private static final String INJECTED_FAILURE = "injected processing failure";
    private static final Log LOG = LogFactory.getLog(IODispatcherProcessErrorTest.class);

    @Rule
    public Timeout timeout = Timeout.builder()
            .withTimeout(60, TimeUnit.SECONDS)
            .withLookingForStuckThread(true)
            .build();

    @Test
    public void testContextInitThrowDisconnectsContext() throws Exception {
        assertThrowingStepDisconnectsContext(true);
    }

    @Test
    public void testProcessorThrowDisconnectsContext() throws Exception {
        assertThrowingStepDisconnectsContext(false);
    }

    private void assertThrowingStepDisconnectsContext(boolean isFailingInInit) throws Exception {
        assertMemoryLeak(() -> {
            final AtomicInteger accepted = new AtomicInteger();

            // Throws from the processor unless the context is set to throw from init() first.
            final IORequestProcessor<TestContext> throwingProcessor = (_, _, _) -> {
                throw new IllegalStateException(INJECTED_FAILURE);
            };
            final IORequestProcessor<TestContext> passiveProcessor = (_, _, _) -> true;

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
                        return new TestContext(fd, isFailingInInit);
                    }
            );

            final long buf = Unsafe.malloc(1, MemoryTag.NATIVE_DEFAULT);
            Unsafe.putByte(buf, (byte) 'x');
            final long sockAddr = Net.sockaddr("127.0.0.1", dispatcher.getPort());
            final long clientFd = Net.socketTcp(true);
            try {
                Net.configureNonBlocking(clientFd);
                Net.connect(clientFd, sockAddr);

                while (accepted.get() == 0) {
                    dispatcher.run();
                }
                Assert.assertEquals(1, dispatcher.getConnectionCount());

                // Send a byte so the dispatcher fires a READ and hands the context to the
                // failing step.
                Assert.assertEquals(1, Net.send(clientFd, buf, 1));
                Throwable thrown = null;
                while (thrown == null) {
                    dispatcher.run();
                    try {
                        dispatcher.processIOQueue(isFailingInInit ? passiveProcessor : throwingProcessor);
                    } catch (Throwable th) {
                        thrown = th;
                    }
                }
                Assert.assertEquals(INJECTED_FAILURE, thrown.getMessage());

                // The guard must hand the context back to the dispatcher to be closed. Without
                // it the context stays checked out, the count never drops, and the fd and the
                // context's buffer leak past close().
                final long deadlineMillis = System.currentTimeMillis() + 10_000;
                while (dispatcher.getConnectionCount() > 0 && System.currentTimeMillis() < deadlineMillis) {
                    dispatcher.run();
                }
                Assert.assertEquals(0, dispatcher.getConnectionCount());
            } finally {
                dispatcher.close();
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
        private final boolean isFailingInInit;

        private TestContext(long fd, boolean isFailingInInit) {
            super(PlainSocketFactory.INSTANCE, NetworkFacadeImpl.INSTANCE, LOG);
            this.isFailingInInit = isFailingInInit;
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

        @Override
        protected void doInit() throws TlsSessionInitFailedException {
            if (isFailingInInit) {
                throw new IllegalStateException(INJECTED_FAILURE);
            }
            super.doInit();
        }
    }
}
