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
import io.questdb.cairo.TableToken;
import io.questdb.cairo.YieldException;
import io.questdb.cairo.pool.PoolListener;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.security.SecurityContextFactory;
import io.questdb.network.YieldEvent;
import io.questdb.network.YieldEventFactory;
import io.questdb.network.YieldEventFactoryImpl;
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
 * This test verifies yield support in ILP while waiting for table permissions.
 */
@RunWith(Parameterized.class)
public class LineTcpYieldTest extends AbstractLineTcpReceiverTest {
    private final boolean walEnabled;

    public LineTcpYieldTest(WalMode walMode) {
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
    public void testYieldEventImmediatelyTriggered() throws Exception {
        final YieldEventFactory yieldEventFactory = new YieldEventFactoryImpl(ioDispatcherConfiguration);
        final TestSecurityContext securityContext = new TestSecurityContext(() -> {
            final YieldEvent yieldEvent = yieldEventFactory.newInstance();
            yieldEvent.trigger();
            yieldEvent.close();
            return yieldEvent;
        }, 3);
        securityContextFactory = new TestSecurityContextFactory(securityContext);

        runInContext((receiver) -> {
            final String tableName = "up";
            final String lineData = tableName + " out=1.0 631150000000000000\n" +
                    tableName + " out=2.0 631152000000000000\n";

            final CountDownLatch released = new CountDownLatch(1);
            engine.setPoolListener((factoryType, thread, name, event, segment, position) -> {
                if (name != null && Chars.equalsNc(name.getTableName(), tableName)) {
                    if (PoolListener.isWalOrWriter(factoryType) && event == PoolListener.EV_RETURN) {
                        released.countDown();
                    }
                }
            });
            send(tableName, WAIT_NO_WAIT, () -> sendToSocket(lineData));

            released.await();
            if (walEnabled) {
                mayDrainWalQueue();
            }
            String expected = "out\ttimestamp\n" +
                    "1.0\t1989-12-31T23:26:40.000000Z\n" +
                    "2.0\t1990-01-01T00:00:00.000000Z\n";
            assertTable(expected, tableName);
        });
    }

    @Test
    public void testYieldEventNeverTriggered() throws Exception {
        tablePermissionsTimeout = 10;

        final YieldEventFactory yieldEventFactory = new YieldEventFactoryImpl(ioDispatcherConfiguration);
        final YieldEvent yieldEvent = yieldEventFactory.newInstance();
        final TestSecurityContext securityContext = new TestSecurityContext(() -> yieldEvent, 1);
        securityContextFactory = new TestSecurityContextFactory(securityContext);

        try {
            runInContext((receiver) -> {
                final String tableName = "up";
                final String lineData = tableName + " out=42.0 631150000000000000\n";

                try (Socket socket = getSocket()) {
                    sendToSocket(socket, lineData);

                    if (walEnabled) {
                        mayDrainWalQueue();
                    }
                    assertTableExistsEventually(engine, tableName);

                    Os.sleep(5 * tablePermissionsTimeout);

                    // At this point, there should be no data in the table as ILP waits for the yield event.
                    assertTable("out\ttimestamp\n", tableName);

                    // Trigger the event.
                    yieldEvent.trigger();

                    Os.sleep(5 * tablePermissionsTimeout);
                    if (walEnabled) {
                        mayDrainWalQueue();
                    }

                    // The data shouldn't be there as the connection should be closed due to the timeout
                    // with pending data ignored.
                    assertTable("out\ttimestamp\n", tableName);
                }
            });
        } finally {
            yieldEvent.close();
        }
    }

    @Test
    public void testYieldEventTriggeredAfterDelay() throws Exception {
        final YieldEventFactory yieldEventFactory = new YieldEventFactoryImpl(ioDispatcherConfiguration);
        final YieldEvent yieldEvent = yieldEventFactory.newInstance();
        final TestSecurityContext securityContext = new TestSecurityContext(() -> yieldEvent, 1);
        securityContextFactory = new TestSecurityContextFactory(securityContext);

        try {
            runInContext((receiver) -> {
                final String tableName = "up";
                final String lineData = tableName + " out=42.0 631150000000000000\n";

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
                    assertTableExistsEventually(engine, tableName);

                    Os.sleep(50);

                    // At this point, there should be no data in the table as ILP waits for the yield event.
                    assertTable("out\ttimestamp\n", tableName);

                    // Trigger the event and check that the data becomes visible.
                    yieldEvent.trigger();
                }

                released.await();
                if (walEnabled) {
                    mayDrainWalQueue();
                }
                assertTable("out\ttimestamp\n42.0\t1989-12-31T23:26:40.000000Z\n", tableName);
            });
        } finally {
            yieldEvent.close();
        }
    }

    private void mayDrainWalQueue() {
        if (walEnabled) {
            drainWalQueue();
        }
    }

    private interface TestYieldEventFactory {
        YieldEvent newInstance();
    }

    private static class TestSecurityContext extends AllowAllSecurityContext {
        private final TestYieldEventFactory factory;
        private final int yieldCount;
        private int awaitCounter;

        public TestSecurityContext(TestYieldEventFactory factory, int yieldCount) {
            this.factory = factory;
            this.yieldCount = yieldCount;
        }

        @Override
        public long onTableCreated(TableToken tableToken) {
            return 42;
        }

        @Override
        public void yieldUntilTxn(long txn) throws YieldException {
            if (awaitCounter++ < yieldCount) {
                final YieldEvent yieldEvent = factory.newInstance();
                throw YieldException.instance(yieldEvent);
            }
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
