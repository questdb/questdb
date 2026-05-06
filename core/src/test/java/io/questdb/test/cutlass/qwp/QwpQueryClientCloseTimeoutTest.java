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

package io.questdb.test.cutlass.qwp;

import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;
import io.questdb.client.cutlass.qwp.client.QwpQueryClient;
import io.questdb.std.Os;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;

/**
 * Pins the contract of {@link QwpQueryClient#wasLastCloseTimedOut()}:
 * when {@code close()} cannot join the I/O thread within the configured
 * timeout it logs at ERROR, flags the timeout, and intentionally leaks the
 * daemon / buffer pool / socket so a still-running thread can't SIGSEGV
 * the JVM by reading freed memory.
 * <p>
 * The timeout branch is otherwise unreachable in normal runs -- the real
 * I/O thread responds to {@code Thread.interrupt} promptly and exits well
 * inside the five-second budget. To exercise the branch synthetically the
 * test hijacks {@code ioThreadHandle} via reflection with a daemon that
 * deliberately ignores interrupts, and shortens {@code shutdownJoinMs}
 * so the test completes in under a second.
 */
public class QwpQueryClientCloseTimeoutTest extends AbstractBootstrapTest {

    private static volatile boolean stubStop;

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testCleanCloseReportsNoTimeout() throws Exception {
        // Happy-path coverage: after a normal close(), the flag must read false.
        // Without this, the getter would stay dead even with the timeout test
        // only exercising the true branch.
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE t(id LONG, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("INSERT INTO t SELECT x, x::TIMESTAMP FROM long_sequence(50)");
                serverMain.awaitTable("t");
                QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";");
                client.connect();
                // Run a real query so the I/O thread has actually done work
                // before close() joins it.
                client.execute("SELECT * FROM t", new NoopHandler());
                client.close();
                Assert.assertFalse(
                        "clean close must not flag timeout",
                        client.wasLastCloseTimedOut());
            }
        });
    }

    @Test
    public void testTimeoutFlagSetWhenIoThreadIgnoresInterrupt() throws Exception {
        // No assertMemoryLeak: this branch is specifically the "leak rather
        // than SIGSEGV" path, so native buffers owned by the hijacked
        // io-thread handle do not get freed. The test cleans up its own
        // hijacked thread, but the original connection's native resources
        // (batch buffers + WebSocket recv buffer) are the deliberate leak
        // that the flag announces.
        try (final TestServerMain serverMain = startWithEnvVariables()) {
            serverMain.execute("CREATE TABLE t(id LONG, ts TIMESTAMP) "
                    + "TIMESTAMP(ts) PARTITION BY DAY WAL");
            QwpQueryClient client = QwpQueryClient.fromConfig(
                    "ws::addr=127.0.0.1:" + HTTP_PORT + ";");
            client.connect();

            // Shrink the join budget so the test takes ~150 ms, not 5 s. The
            // production field is declared volatile (not final) precisely so
            // this reflective write lands cleanly.
            setLongField(client);

            // Replace ioThreadHandle with a daemon that never responds to
            // Thread.interrupt. The real I/O thread is still running in the
            // background with its own Thread reference; shutdown() will tell
            // it to exit, and it will, but close() won't see that because it
            // joins on our hijacked handle.
            stubStop = false;
            Thread stubHandle = new Thread(() -> {
                while (!stubStop) {
                    Os.sleep(20);
                }
            }, "qwp-close-timeout-stub");
            stubHandle.setDaemon(true);
            stubHandle.start();

            Thread originalHandle = (Thread) getField(client);
            setField(client, stubHandle);

            try {
                long t0 = System.nanoTime();
                client.close();
                long elapsedMs = (System.nanoTime() - t0) / 1_000_000L;

                Assert.assertTrue("close() must flag the timeout", client.wasLastCloseTimedOut());
                Assert.assertTrue(
                        "close() must return promptly after the shortened join budget, elapsed="
                                + elapsedMs + " ms",
                        elapsedMs < 2_000);
            } finally {
                // Tell the stub to stop and wait for the real I/O thread to
                // exit on its own (it received shutdown() inside close()).
                stubStop = true;
                stubHandle.join();
                if (originalHandle != null) {
                    originalHandle.join(5_000);
                }
            }
        }
    }

    private static Object getField(Object target) throws Exception {
        Field f = target.getClass().getDeclaredField("ioThreadHandle");
        f.setAccessible(true);
        return f.get(target);
    }

    private static void setField(Object target, Object value) throws Exception {
        Field f = target.getClass().getDeclaredField("ioThreadHandle");
        f.setAccessible(true);
        f.set(target, value);
    }

    private static void setLongField(Object target) throws Exception {
        Field f = target.getClass().getDeclaredField("shutdownJoinMs");
        f.setAccessible(true);
        f.setLong(target, 150L);
    }

    private static final class NoopHandler implements QwpColumnBatchHandler {
        @Override
        public void onBatch(QwpColumnBatch batch) {
        }

        @Override
        public void onEnd(long totalRows) {
        }

        @Override
        public void onError(byte status, String message) {
            Assert.fail("unexpected QWP error [status=" + status + "]: " + message);
        }
    }
}
