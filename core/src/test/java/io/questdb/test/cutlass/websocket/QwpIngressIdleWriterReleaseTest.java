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

package io.questdb.test.cutlass.websocket;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.pool.WalWriterPool;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cutlass.http.DefaultHttpServerConfiguration;
import io.questdb.cutlass.http.HttpFullFatServerConfiguration;
import io.questdb.cutlass.http.HttpRawSocket;
import io.questdb.cutlass.http.processors.LineHttpProcessorConfiguration;
import io.questdb.cutlass.qwp.server.QwpIngressProcessorState;
import io.questdb.cutlass.qwp.server.QwpIngressUpgradeProcessor;
import io.questdb.cutlass.qwp.server.QwpTudCache;
import io.questdb.cutlass.qwp.websocket.WebSocketOpcode;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.ServerDisconnectException;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.MicrosecondClock;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cutlass.qwp.QwpWireTestFixtures;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;

import static io.questdb.test.cutlass.websocket.QwpIngressWireHarness.*;

public class QwpIngressIdleWriterReleaseTest extends AbstractCairoTest {
    private static final byte[] PING = createMaskedFrame(WebSocketOpcode.PING, new byte[0]);

    @Test
    public void testClockSampleAfterSendBackpressureAndBufferedResume() throws Exception {
        assertMemoryLeak(() -> {
            byte[] data = clientEncodedFrame(1, "tab");
            try (IdleReleaseFixture f = new IdleReleaseFixture(30_000, concat(data, PING, data, PING, PING))) {
                f.onSend = () -> f.clock.nowMicros = 61_000_000;
                f.isSendBlocked = true;
                try {
                    f.drive(data.length + PING.length + data.length + PING.length);
                    Assert.fail("ACK send must block");
                } catch (PeerIsSlowToReadException expected) {
                    Assert.assertTrue(f.state.isSending());
                }
                Assert.assertEquals(1, f.clock.calls);
                Assert.assertEquals(61_000_000, f.state.getLastDataPassEndMicros());
                Assert.assertEquals(data.length + PING.length, f.state.getRecvBufferLen());

                f.isSendBlocked = false;
                f.onSend = () -> f.clock.nowMicros = 101_000_000;
                f.clock.calls = 0;
                f.clock.nowMicros = 71_000_000;
                f.processor.resumeSend(f.context);
                Assert.assertEquals(1, f.clock.calls);
                Assert.assertEquals(101_000_000, f.state.getLastDataPassEndMicros());
                Assert.assertEquals(0, f.state.getRecvBufferLen());
                Assert.assertEquals(1, f.tudCache.size());

                f.onSend = null;
                f.clock.nowMicros = 131_000_000;
                f.drive(PING.length);
                Assert.assertEquals("resumed pass must reset its activity flag", 0, f.tudCache.size());
            }
            drainWalQueue();
            assertQuery("SELECT count() FROM tab").expectSize().noRandomAccess().returns("count\n2\n");
        });
    }

    @Test
    public void testClockSampleAfterTransportError() throws Exception {
        assertMemoryLeak(() -> {
            byte[] data = clientEncodedFrame(1, "tab");
            try (IdleReleaseFixture f = new IdleReleaseFixture(30_000, concat(data, data))) {
                f.onSend = () -> f.clock.nowMicros = 61_000_000;
                f.isSendDisconnected = true;
                try {
                    f.drive(data.length * 2);
                    Assert.fail("transport error must disconnect");
                } catch (ServerDisconnectException expected) {
                    Assert.assertEquals(1, f.clock.calls);
                    Assert.assertEquals(61_000_000, f.state.getLastDataPassEndMicros());
                }
            }
        });
    }

    @Test
    public void testClockSampleOnMalformedFrameExit() throws Exception {
        assertMemoryLeak(() -> {
            byte[] data = clientEncodedFrame(1, "tab");
            // An unmasked client frame is a transport protocol violation.
            byte[] invalid = {(byte) 0x82, 0};
            try (IdleReleaseFixture f = new IdleReleaseFixture(30_000, concat(data, data, invalid))) {
                try {
                    f.drive(data.length * 2 + invalid.length);
                    Assert.fail("malformed frame must disconnect");
                } catch (ServerDisconnectException expected) {
                    Assert.assertEquals(1, f.clock.calls);
                    Assert.assertEquals(f.clock.nowMicros, f.state.getLastDataPassEndMicros());
                    Assert.assertEquals(0, f.state.getRecvBufferLen());
                    Assert.assertFalse(f.state.isCommitBatchPending());
                }
            }
        });
    }

    @Test
    public void testClockSampleOnceAtReceivePassEnd() throws Exception {
        assertMemoryLeak(() -> {
            byte[] data = clientEncodedFrame(1, "tab");
            try (IdleReleaseFixture f = new IdleReleaseFixture(30_000, concat(data, data, data))) {
                f.onSend = () -> f.clock.nowMicros = 61_000_000;
                f.drive(data.length * 3);
                Assert.assertEquals("one idle timestamp per receive pass", 1, f.clock.calls);
                Assert.assertEquals("timestamp must include the final ACK send", 61_000_000, f.state.getLastDataPassEndMicros());
                Assert.assertEquals(1, f.tudCache.size());
            }
            drainWalQueue();
            assertQuery("SELECT count() FROM tab").expectSize().noRandomAccess().returns("count\n3\n");
        });
    }

    @Test
    public void testControlAndDurablePollDoNotPostponeRelease() throws Exception {
        assertMemoryLeak(() -> {
            byte[] data = clientEncodedFrame(1, "tab");
            byte[] pong = createMaskedFrame(WebSocketOpcode.PONG, new byte[0]);
            byte[] poll = createMaskedFrame(WebSocketOpcode.BINARY, QwpWireTestFixtures.durableAckPollMessage());
            try (IdleReleaseFixture f = new IdleReleaseFixture(30_000, concat(data, pong, poll, PING))) {
                f.state.setDurableAckEnabled(true);
                f.drive(data.length);
                f.clock.calls = 0;
                f.clock.nowMicros = 21_000_000;
                f.drive(pong.length + poll.length);
                Assert.assertEquals(0, f.clock.calls);
                Assert.assertEquals(1_000_000, f.state.getLastDataPassEndMicros());
                f.clock.nowMicros = 31_000_000;
                f.drive(PING.length);
                Assert.assertEquals("PING-only pass needs only its idle check", 1, f.clock.calls);
                Assert.assertEquals(0, f.tudCache.size());
                Assert.assertEquals(0, f.walWriterPool.getBusyCount());
            }
        });
    }

    @Test
    public void testDataProtectsWritersDuringSlowReceivePass() throws Exception {
        assertMemoryLeak(() -> {
            byte[] data = clientEncodedFrame(1, "tab");
            try (IdleReleaseFixture f = new IdleReleaseFixture(30_000, concat(data, PING, PING, PING))) {
                // The first PING flushes the data ACK; simulate a slow send before
                // the second PING in the same receive pass.
                f.onSend = () -> f.clock.nowMicros = 61_000_000;
                f.drive(data.length + PING.length * 2);
                Assert.assertEquals("same-pass PING must retain the active writer", 1, f.tudCache.size());
                Assert.assertEquals(1, f.walWriterPool.getBusyCount());
                Assert.assertEquals(1, f.clock.calls);
                Assert.assertEquals(61_000_000, f.state.getLastDataPassEndMicros());
                f.onSend = null;
                f.clock.nowMicros = 91_000_000;
                f.drive(PING.length);
                Assert.assertEquals(0, f.tudCache.size());
                Assert.assertEquals(0, f.walWriterPool.getBusyCount());
            }
        });
    }

    @Test
    public void testDisabledReleaseDoesNotReadClock() throws Exception {
        assertMemoryLeak(() -> {
            byte[] data = clientEncodedFrame(1, "tab");
            try (IdleReleaseFixture f = new IdleReleaseFixture(0, concat(data, data, PING, PING))) {
                f.drive(data.length * 2 + PING.length);
                f.clock.nowMicros = 3_601_000_000L;
                f.drive(PING.length);
                Assert.assertEquals("disabled idle release must not sample its clock", 0, f.clock.calls);
                Assert.assertEquals(1, f.tudCache.size());
            }
        });
    }

    @Test
    public void testDisconnectClearsActivityBeforeReuse() throws Exception {
        assertMemoryLeak(() -> {
            try (IdleReleaseFixture f = new IdleReleaseFixture(30_000, PING)) {
                // Seed activity to verify that disconnect clears it.
                f.state.addData(f.recvBuffer, f.recvBuffer);
                f.state.onDisconnected();
                f.state.of(-1, AllowAllSecurityContext.INSTANCE);
                f.clock.calls = 0;
                f.clock.nowMicros = 31_000_000;
                Assert.assertEquals(0, f.state.getLastDataPassEndMicros());
                f.drive(PING.length);
                Assert.assertEquals("reused state must run the PING idle check", 1, f.clock.calls);
                Assert.assertEquals(0, f.state.getLastDataPassEndMicros());
            }
        });
    }

    @Test
    public void testPartialFrameDoesNotReadClock() throws Exception {
        assertMemoryLeak(() -> {
            byte[] data = clientEncodedFrame(1, "tab");
            try (IdleReleaseFixture f = new IdleReleaseFixture(30_000, concat(data, PING))) {
                f.drive(data.length);
                f.clock.calls = 0;
                f.clock.nowMicros = 21_000_000;
                f.drive(1);
                Assert.assertEquals(0, f.clock.calls);
                Assert.assertEquals(1_000_000, f.state.getLastDataPassEndMicros());
                f.clock.nowMicros = 31_000_000;
                f.drive(PING.length - 1);
                Assert.assertEquals(1, f.clock.calls);
                Assert.assertEquals(0, f.tudCache.size());
            }
        });
    }

    @Test
    public void testPingBeforeDataCanReleaseThenDataProtectsWriter() throws Exception {
        assertMemoryLeak(() -> {
            byte[] data = clientEncodedFrame(1, "tab");
            try (IdleReleaseFixture f = new IdleReleaseFixture(30_000, concat(data, PING, data, PING))) {
                f.drive(data.length);
                f.clock.calls = 0;
                f.clock.nowMicros = 31_000_000;
                f.onSend = () -> {
                    Assert.assertEquals("PING before data must release the idle writer", 0, f.tudCache.size());
                    Assert.assertEquals(0, f.walWriterPool.getBusyCount());
                    f.onSend = null;
                };
                f.drive(PING.length * 2 + data.length);
                Assert.assertEquals("one early PING check and one pass-end sample", 2, f.clock.calls);
                Assert.assertEquals(31_000_000, f.state.getLastDataPassEndMicros());
                Assert.assertEquals(1, f.tudCache.size());
                Assert.assertEquals(1, f.walWriterPool.getBusyCount());
            }
        });
    }

    @Test
    public void testRejectedDataDoesNotPostponeRelease() throws Exception {
        assertMemoryLeak(() -> {
            byte[] data = clientEncodedFrame(1, "tab");
            try (IdleReleaseFixture f = new IdleReleaseFixture(30_000, concat(data, data, PING))) {
                f.drive(data.length);
                f.state.markSequenceUnresolved(1);
                f.clock.calls = 0;
                f.clock.nowMicros = 31_000_000;
                f.drive(data.length);
                Assert.assertEquals(0, f.clock.calls);
                Assert.assertEquals(1_000_000, f.state.getLastDataPassEndMicros());
                f.drive(PING.length);
                Assert.assertEquals(1, f.clock.calls);
                Assert.assertEquals(0, f.tudCache.size());
            }
            drainWalQueue();
            assertQuery("SELECT count() FROM tab").expectSize().noRandomAccess().returns("count\n1\n");
        });
    }

    private static class CountingClock implements MicrosecondClock {
        private int calls;
        private long nowMicros = 1_000_000;

        @Override
        public long getTicks() {
            calls++;
            return nowMicros;
        }
    }

    private static class IdleReleaseFixture implements AutoCloseable {
        private final CountingClock clock = new CountingClock();
        private final PhasedNetworkFacade network;
        private final QwpIngressUpgradeProcessor processor;
        private final QwpIngressProcessorState state;
        private final QwpTudCache tudCache;
        private final WalWriterPool walWriterPool;
        private TestableContext context;
        private boolean isSendBlocked;
        private boolean isSendDisconnected;
        private Runnable onSend;
        private long recvBuffer;
        private long sendBuffer;

        private IdleReleaseFixture(long minIdleMs, byte[] wire) throws Exception {
            execute("CREATE TABLE tab (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            LineHttpProcessorConfiguration lineConfig = new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration) {
                @Override
                public MicrosecondClock getMicrosecondClock() {
                    return clock;
                }

                @Override
                public long getQwpMinIdleMsBeforeWriterRelease() {
                    return minIdleMs;
                }
            };
            HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration) {
                @Override
                public LineHttpProcessorConfiguration getLineHttpProcessorConfiguration() {
                    return lineConfig;
                }
            };
            processor = new QwpIngressUpgradeProcessor(engine, httpConfig);
            network = new PhasedNetworkFacade(wire);
            var localValue = getLV();
            try {
                recvBuffer = Unsafe.malloc(RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                sendBuffer = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                HttpRawSocket faultSocket = new HttpRawSocket() {
                    @Override
                    public long getBufferAddress() {
                        return sendBuffer;
                    }

                    @Override
                    public int getBufferSize() {
                        return SEND_BUFFER_SIZE;
                    }

                    @Override
                    public void send(int size) throws PeerDisconnectedException, PeerIsSlowToReadException {
                        if (onSend != null) {
                            onSend.run();
                        }
                        if (isSendBlocked) {
                            throw PeerIsSlowToReadException.INSTANCE;
                        }
                        if (isSendDisconnected) {
                            throw PeerDisconnectedException.INSTANCE;
                        }
                    }
                };
                context = new TestableContext(httpConfig, network, faultSocket, recvBuffer, RECV_BUFFER_SIZE) {
                    @Override
                    public void resumeResponseSend() {
                        // The injected blocked send completes on resume.
                    }
                };
                state = new QwpIngressProcessorState(RECV_BUFFER_SIZE, SEND_BUFFER_SIZE, engine, lineConfig);
                state.of(-1, AllowAllSecurityContext.INSTANCE);
                localValue.set(context, state);
                Field cacheField = QwpIngressProcessorState.class.getDeclaredField("tudCache");
                cacheField.setAccessible(true);
                tudCache = (QwpTudCache) cacheField.get(state);
                Field poolField = CairoEngine.class.getDeclaredField("walWriterPool");
                poolField.setAccessible(true);
                walWriterPool = (WalWriterPool) poolField.get(engine);
            } catch (Throwable e) {
                close();
                throw e;
            }
        }

        @Override
        public void close() {
            try {
                if (context != null) {
                    context.close();
                }
            } finally {
                recvBuffer = Unsafe.free(recvBuffer, RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                sendBuffer = Unsafe.free(sendBuffer, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            }
        }

        private void drive(int bytes) throws Exception {
            QwpIngressWireHarness.drive(processor, context, network, bytes);
        }
    }
}
