/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.test.cutlass.line.tcp;

import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.SqlWalMode;
import io.questdb.cairo.SuspendException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.pool.PoolListener;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.security.SecurityContextFactory;
import io.questdb.network.SuspendEvent;
import io.questdb.network.SuspendEventFactory;
import io.questdb.std.Chars;
import io.questdb.std.Os;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;

/**
 * This test verifies ILP suspendability while waiting for table permissions.
 */
@RunWith(Parameterized.class)
public class LineTcpSuspendabilityTest extends AbstractLineTcpReceiverTest {
    private final boolean walEnabled;

    public LineTcpSuspendabilityTest(WalMode walMode) {
        this.walEnabled = (walMode == WalMode.WITH_WAL);
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {WalMode.WITH_WAL}, {WalMode.NO_WAL}
        });
    }

    @Before
    @Override
    public void setUp() {
        super.setUp();
        configOverrideDefaultTableWriteMode(walEnabled ? SqlWalMode.WAL_ENABLED : SqlWalMode.WAL_DISABLED);
    }

    @Test
    public void testSuspendEventImmediatelyTriggered() throws Exception {
        final TestSecurityContext securityContext = new TestSecurityContext(() -> {
            final SuspendEvent suspendEvent = SuspendEventFactory.newInstance(ioDispatcherConfiguration);
            suspendEvent.trigger();
            suspendEvent.close();
            return suspendEvent;
        }, 3);
        securityContextFactory = new TestSecurityContextFactory(securityContext);

        runInContext((receiver) -> {
            final String tableName = "up";
            final String lineData = "up out=1.0 631150000000000000\n" +
                    "up out=2.0 631152000000000000\n";

            final CountDownLatch released = new CountDownLatch(1);
            engine.setPoolListener((factoryType, thread, name, event, segment, position) -> {
                if (name != null && Chars.equalsNc(name.getTableName(), tableName)) {
                    if (PoolListener.isWalOrWriter(factoryType) && event == PoolListener.EV_RETURN) {
                        released.countDown();
                    }
                }
            });
            send("up", WAIT_NO_WAIT, () -> sendToSocket(lineData));

            released.await();
            if (walEnabled) {
                mayDrainWalQueue();
            }
            String expected = "out\ttimestamp\n" +
                    "1.0\t1989-12-31T23:26:40.000000Z\n" +
                    "2.0\t1990-01-01T00:00:00.000000Z\n";
            assertTable(expected, "up");
        });
    }

    @Test
    public void testSuspendEventTriggeredAfterDelay() throws Exception {
        final SuspendEvent suspendEvent = SuspendEventFactory.newInstance(ioDispatcherConfiguration);
        final TestSecurityContext securityContext = new TestSecurityContext(() -> suspendEvent, 1);
        securityContextFactory = new TestSecurityContextFactory(securityContext);

        try {
            runInContext((receiver) -> {
                final String tableName = "up";
                final String lineData = "up out=42.0 631150000000000000\n";

                final CountDownLatch released = new CountDownLatch(1);
                engine.setPoolListener((factoryType, thread, name, event, segment, position) -> {
                    if (name != null && Chars.equalsNc(name.getTableName(), tableName)) {
                        if (PoolListener.isWalOrWriter(factoryType) && event == PoolListener.EV_RETURN) {
                            released.countDown();
                        }
                    }
                });

                try (Socket socket = getSocket()) {
                    sendToSocket(socket, lineData);

                    if (walEnabled) {
                        mayDrainWalQueue();
                    }
                    assertTableExistsEventually(engine, "up");

                    Os.sleep(50);

                    // At this point, there should be no data in the table as ILP waits for the suspend event.
                    assertTable("out\ttimestamp\n", "up");

                    // Trigger the event and check that the data becomes visible.
                    suspendEvent.trigger();
                }

                released.await();
                if (walEnabled) {
                    mayDrainWalQueue();
                }
                assertTable("out\ttimestamp\n42.0\t1989-12-31T23:26:40.000000Z\n", "up");
            });
        } finally {
            suspendEvent.close();
        }
    }

    private void mayDrainWalQueue() {
        if (walEnabled) {
            drainWalQueue();
        }
    }

    private interface TestSuspendEventFactory {
        SuspendEvent newInstance();
    }

    private static class TestSecurityContext extends AllowAllSecurityContext {
        private final TestSuspendEventFactory factory;
        private final int suspendCount;
        private int awaitCounter;

        public TestSecurityContext(TestSuspendEventFactory factory, int suspendCount) {
            this.factory = factory;
            this.suspendCount = suspendCount;
        }

        @Override
        public void awaitForTxn(long txn) throws SuspendException {
            if (awaitCounter++ < suspendCount) {
                final SuspendEvent suspendEvent = factory.newInstance();
                throw SuspendException.instance(suspendEvent);
            }
        }

        @Override
        public long onTableCreated(TableToken tableToken) {
            return 42;
        }
    }

    private static class TestSecurityContextFactory implements SecurityContextFactory {
        private final TestSecurityContext securityContext;

        private TestSecurityContextFactory(TestSecurityContext securityContext) {
            this.securityContext = securityContext;
        }

        @Override
        public SecurityContext getInstance(CharSequence principal, int interfaceId) {
            return securityContext;
        }
    }
}
