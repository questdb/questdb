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

package io.questdb.test.cutlass.line.udp;

import io.questdb.cairo.CairoEngine;
import io.questdb.cutlass.line.udp.AbstractLineProtoUdpReceiver;
import io.questdb.cutlass.line.udp.DefaultLineUdpReceiverConfiguration;
import io.questdb.cutlass.line.udp.LineUdpReceiver;
import io.questdb.cutlass.line.udp.LineUdpReceiverConfiguration;
import io.questdb.std.Os;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.lang.reflect.Field;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Witness for the UDP receiver close-timeout branch (the FP-#10 armed dismissal).
 * <p>
 * AbstractLineProtoUdpReceiver.close() bounds its started.await() to 5 seconds: if start() flipped the
 * running CAS but the spawned receiver thread never reached started.countDown() (e.g. an OOM in Thread
 * construction or start()), an unbounded await would block close() forever and the receiver thread, if
 * it ever woke, would touch a half-freed parser. The bounded await lets close() proceed to fd cleanup
 * and free the parser exactly once.
 * <p>
 * This test simulates that path deterministically by flipping the receiver's running flag to true via
 * reflection WITHOUT calling start() (so nothing ever counts down the started latch), then calling
 * close() on a worker thread. It asserts close() returns within the bounded window rather than
 * deadlocking, and that a second close() is a harmless no-op (the fd is already cleaned up).
 */
public class LineUdpReceiverCloseTimeoutTest extends AbstractCairoTest {

    private static final LineUdpReceiverConfiguration RCVR_CONF = new DefaultLineUdpReceiverConfiguration();

    @Rule
    public Timeout timeout = Timeout.builder()
            .withTimeout(60, TimeUnit.SECONDS)
            .withLookingForStuckThread(true)
            .build();

    @Test
    public void testCloseDoesNotDeadlockWhenStartedLatchNeverFires() throws Exception {
        assertMemoryLeak(() -> {
            try (CairoEngine engine = new CairoEngine(configuration)) {
                // LinuxMMLineUdpReceiver and LineUdpReceiver share the same close() in the abstract
                // base; LineUdpReceiver is the portable receiver and binds a real UDP socket here.
                final AbstractLineProtoUdpReceiver receiver = new LineUdpReceiver(RCVR_CONF, engine, null);

                // Simulate start() having won the running CAS while the spawned thread never reached
                // started.countDown(): set running=true directly, leaving the started latch at 1.
                setRunningTrue(receiver);

                final AtomicBoolean closeReturned = new AtomicBoolean(false);
                final AtomicReference<Throwable> closeError = new AtomicReference<>();
                final Thread closer = new Thread(() -> {
                    try {
                        receiver.close();
                        closeReturned.set(true);
                    } catch (Throwable th) {
                        closeError.set(th);
                    }
                }, "udp-receiver-closer");
                closer.start();

                // The bounded started.await() is 5s; give close() a generous ceiling above that. If the
                // await were unbounded, close() would never return and this join would leave the thread
                // alive (asserted below).
                closer.join(TimeUnit.SECONDS.toMillis(15));

                Assert.assertNull("close() must not throw", closeError.get());
                Assert.assertTrue(
                        "close() must return within the bounded await window, not deadlock on the started latch",
                        closeReturned.get()
                );
                Assert.assertFalse("the closer thread must have terminated", closer.isAlive());

                // A second close() after fd cleanup is a harmless no-op (fd is already -1).
                receiver.close();
                Os.sleep(1);
            }
        });
    }

    private static void setRunningTrue(AbstractLineProtoUdpReceiver receiver) throws Exception {
        Field f = AbstractLineProtoUdpReceiver.class.getDeclaredField("running");
        f.setAccessible(true);
        AtomicBoolean running = (AtomicBoolean) f.get(receiver);
        running.set(true);
    }
}
