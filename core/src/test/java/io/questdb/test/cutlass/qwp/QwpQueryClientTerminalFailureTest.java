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
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Covers the terminal-failure latch on {@link QwpQueryClient}: once the I/O
 * thread records a transport- or protocol-level failure, subsequent
 * {@code execute()} calls short-circuit via {@code handler.onError} without
 * submitting the query to the now-broken connection.
 */
public class QwpQueryClientTerminalFailureTest extends AbstractBootstrapTest {

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testExecuteShortCircuitsAfterTerminalFailure() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE t(id LONG, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("INSERT INTO t SELECT x, x::TIMESTAMP FROM long_sequence(8)");
                serverMain.awaitTable("t");

                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();

                    // Sanity: a real query runs and delivers rows before any terminal
                    // failure is latched. Without this, a later assertion that the
                    // next execute() short-circuits would also trivially pass for a
                    // client that always reports onError.
                    AtomicBoolean firstOk = new AtomicBoolean();
                    client.execute("SELECT * FROM t", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                        }

                        @Override
                        public void onEnd(long totalRows) {
                            firstOk.set(totalRows == 8);
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("unexpected first-query error [status=" + status
                                    + "]: " + message);
                        }
                    });
                    Assert.assertTrue("first query must complete normally", firstOk.get());

                    // Simulate an I/O-thread-detected terminal failure. This is what
                    // the listener wires to when the real I/O thread sees onClose,
                    // a truncated/unknown frame, or a send/receive exception.
                    invokeRecordTerminalFailure(client, (byte) 42, "synthetic terminal failure");

                    // Execute must short-circuit: onError fires immediately with the
                    // stored status/message, and no query is dispatched to the server.
                    AtomicBoolean batchCalled = new AtomicBoolean();
                    AtomicBoolean endCalled = new AtomicBoolean();
                    AtomicReference<String> msg = new AtomicReference<>();
                    AtomicReference<Byte> status = new AtomicReference<>();
                    long t0 = System.nanoTime();
                    client.execute("SELECT * FROM t", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            batchCalled.set(true);
                        }

                        @Override
                        public void onEnd(long totalRows) {
                            endCalled.set(true);
                        }

                        @Override
                        public void onError(byte s, String m) {
                            status.set(s);
                            msg.set(m);
                        }
                    });
                    long elapsedMs = (System.nanoTime() - t0) / 1_000_000L;

                    Assert.assertFalse("onBatch must not fire after terminal failure", batchCalled.get());
                    Assert.assertFalse("onEnd must not fire after terminal failure", endCalled.get());
                    Assert.assertNotNull("onError must fire after terminal failure", msg.get());
                    Assert.assertEquals("stored status must surface verbatim",
                            Byte.valueOf((byte) 42), status.get());
                    Assert.assertEquals("stored message must surface verbatim",
                            "synthetic terminal failure", msg.get());
                    // A latched failure means no server round-trip: the short-circuit
                    // must return within a few milliseconds even under a loaded CI.
                    Assert.assertTrue("short-circuit must not hit the network, elapsed="
                            + elapsedMs + "ms", elapsedMs < 1_000);
                }
            }
        });
    }

    @Test
    public void testRecordTerminalFailureKeepsFirstFailure() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain ignored = startWithEnvVariables()) {
                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();

                    invokeRecordTerminalFailure(client, (byte) 1, "first failure");
                    // Second call must not overwrite the first -- the user needs the
                    // original root cause, not the last error the I/O thread saw as
                    // it wound down.
                    invokeRecordTerminalFailure(client, (byte) 2, "later failure");

                    AtomicReference<String> msg = new AtomicReference<>();
                    AtomicReference<Byte> status = new AtomicReference<>();
                    client.execute("SELECT 1", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte s, String m) {
                            status.set(s);
                            msg.set(m);
                        }
                    });
                    Assert.assertEquals(Byte.valueOf((byte) 1), status.get());
                    Assert.assertEquals("first failure", msg.get());
                }
            }
        });
    }

    private static void invokeRecordTerminalFailure(QwpQueryClient client, byte status, String message)
            throws Exception {
        Method m = QwpQueryClient.class.getDeclaredMethod(
                "recordTerminalFailure", byte.class, String.class);
        m.setAccessible(true);
        m.invoke(client, status, message);
    }
}
