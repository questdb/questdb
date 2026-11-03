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

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.wal.DurabilityTier;
import io.questdb.cairo.wal.DurableAckRegistry;
import io.questdb.cutlass.http.DefaultHttpServerConfiguration;
import io.questdb.cutlass.http.HttpFullFatServerConfiguration;
import io.questdb.cutlass.http.HttpRawSocket;
import io.questdb.cutlass.http.processors.LineHttpProcessorConfiguration;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.cutlass.qwp.server.QwpIngressProcessorState;
import io.questdb.cutlass.qwp.server.QwpIngressUpgradeProcessor;
import io.questdb.cutlass.qwp.websocket.WebSocketOpcode;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.ServerDisconnectException;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.MicrosecondClock;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cutlass.qwp.QwpWireTestFixtures;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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
                Assert.assertEquals(1, f.state.getCachedTableCount());

                f.onSend = null;
                f.clock.nowMicros = 131_000_000;
                f.drive(PING.length);
                Assert.assertEquals("resumed pass must reset its activity flag", 0, f.state.getCachedTableCount());
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
                Assert.assertEquals(1, f.state.getCachedTableCount());
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
                Assert.assertEquals(0, f.state.getCachedTableCount());
                Assert.assertEquals(0, engine.getBusyWalWriterCount());
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
                Assert.assertEquals("same-pass PING must retain the active writer", 1, f.state.getCachedTableCount());
                Assert.assertEquals(1, engine.getBusyWalWriterCount());
                Assert.assertEquals(1, f.clock.calls);
                Assert.assertEquals(61_000_000, f.state.getLastDataPassEndMicros());
                f.onSend = null;
                f.clock.nowMicros = 91_000_000;
                f.drive(PING.length);
                Assert.assertEquals(0, f.state.getCachedTableCount());
                Assert.assertEquals(0, engine.getBusyWalWriterCount());
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
                Assert.assertEquals(1, f.state.getCachedTableCount());
            }
        });
    }

    @Test
    public void testDisconnectClearsActivityBeforeReuse() throws Exception {
        assertMemoryLeak(() -> {
            byte[] data = clientEncodedFrame(1, "tab");
            try (IdleReleaseFixture f = new IdleReleaseFixture(30_000, concat(data, PING))) {
                f.drive(data.length);
                Assert.assertEquals("a data pass stamps its completion time", 1_000_000, f.state.getLastDataPassEndMicros());
                // Seed the activity flag too, to verify that disconnect clears both.
                f.state.addData(f.recvBuffer, f.recvBuffer);
                f.state.onDisconnected();
                f.state.of(-1, AllowAllSecurityContext.INSTANCE);
                Assert.assertEquals(0, f.state.getLastDataPassEndMicros());
                f.clock.calls = 0;
                f.clock.nowMicros = 31_000_000;
                f.drive(PING.length);
                Assert.assertEquals("reused state must run the PING idle check", 1, f.clock.calls);
                Assert.assertEquals(0, f.state.getLastDataPassEndMicros());
            }
        });
    }

    @Test
    public void testDisconnectForgetsReleasedTables() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab1 (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            byte[] first = clientEncodedFrame(1, "tab");
            byte[] second = clientEncodedFrame(1, "tab1");
            try (IdleReleaseFixture f = new IdleReleaseFixture(30_000, 1, concat(first, PING, second, first))) {
                f.drive(first.length);
                f.clock.nowMicros += 30_000_000;
                f.drive(PING.length);
                Assert.assertEquals(0, f.state.getCachedTableCount());

                // The next connection on this state starts with the whole table limit
                // and does not remember the released tab: tab1 takes the only slot.
                f.state.onDisconnected();
                f.state.of(-1, AllowAllSecurityContext.INSTANCE);
                f.sentFrames.clear();
                f.drive(second.length);
                Assert.assertFalse(hasErrorResponseForSeq(f.sentFrames, 0));
                Assert.assertEquals(0, maxCumulativeOkAck(f.sentFrames));

                f.drive(first.length);
                int refusal = indexOfBinaryFrame(f.sentFrames, QwpConstants.STATUS_WRITE_ERROR, 1);
                Assert.assertTrue("tab, released by the previous connection, must not bypass the limit", refusal > -1);
                Assert.assertEquals("too many distinct tables, limit: 1", errorMessage(f.sentFrames.getQuick(refusal)));
            }
            drainWalQueue();
            assertQuery("SELECT count() FROM tab").expectSize().noRandomAccess().returns("count\n1\n");
            assertQuery("SELECT count() FROM tab1").expectSize().noRandomAccess().returns("count\n1\n");
        });
    }

    @Test
    public void testDurableAckCoversReleasedAndReacquiredTable() throws Exception {
        assertMemoryLeak(() -> {
            byte[] first = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("tab", 100, 1_000_000));
            byte[] second = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("tab", 200, 2_000_000));
            try (IdleReleaseFixture f = new IdleReleaseFixture(30_000, concat(first, PING, PING, second, PING))) {
                final String dirName = engine.verifyTableName("tab").getDirName();
                final HashMap<String, Long> frontiers = new HashMap<>();
                frontiers.put(dirName, -1L);
                f.useDurableAck(new FrontierRegistry(frontiers), DurabilityTier.LOCAL);

                f.drive(first.length);
                Assert.assertEquals(1, sequencerTxn("tab"));
                Assert.assertEquals(1, okAckSeqTxn(f.sentFrames, 0, "tab"));

                // The frontier lags: the idle PING releases the writer with a durable ack still owed.
                f.clock.nowMicros += 30_000_000;
                f.drive(PING.length);
                Assert.assertEquals(0, f.state.getCachedTableCount());
                Assert.assertEquals(-1, durableAckSeqTxn(f.sentFrames, "tab"));

                frontiers.put(dirName, 1L);
                f.drive(PING.length);
                Assert.assertEquals("the release must keep the durable ack owed for the table", 1, durableAckSeqTxn(f.sentFrames, "tab"));

                f.drive(second.length);
                Assert.assertEquals(1, f.state.getCachedTableCount());
                Assert.assertEquals(2, sequencerTxn("tab"));
                Assert.assertEquals("OK ack must name the re-acquired table's commit", 2, okAckSeqTxn(f.sentFrames, 1, "tab"));

                frontiers.put(dirName, 2L);
                f.drive(PING.length);
                Assert.assertEquals("the durable ack must cover the re-acquired table's commit", 2, durableAckSeqTxn(f.sentFrames, "tab"));
            }
            drainWalQueue();
            assertQuery("SELECT v, ts FROM tab")
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            v\tts
                            100\t1970-01-01T00:00:01.000000Z
                            200\t1970-01-01T00:00:02.000000Z
                            """);
        });
    }

    @Test
    public void testIdleReleaseCountsEachTableOnce() throws Exception {
        assertMemoryLeak(() -> {
            for (int i = 1; i < 4; i++) {
                execute("CREATE TABLE tab" + i + " (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            }
            byte[] first = concat(clientEncodedFrame(1, "tab"), clientEncodedFrame(1, "tab1"));
            byte[] second = concat(clientEncodedFrame(1, "tab"), clientEncodedFrame(1, "tab2"));
            byte[] third = concat(clientEncodedFrame(1, "tab1"), clientEncodedFrame(1, "tab3"));
            try (IdleReleaseFixture f = new IdleReleaseFixture(30_000, 3, concat(first, PING, second, PING, third))) {
                f.drive(first.length);
                f.clock.nowMicros += 30_000_000;
                f.drive(PING.length);
                Assert.assertEquals(0, f.state.getCachedTableCount());

                // Taking back released tab adds no table: tab2 is the third.
                f.drive(second.length);
                Assert.assertEquals(2, f.state.getCachedTableCount());
                f.clock.nowMicros += 30_000_000;
                f.drive(PING.length);
                Assert.assertEquals(0, f.state.getCachedTableCount());

                // tab, released twice, still counts once; tab3 is the fourth table.
                f.drive(third.length);
                Assert.assertEquals(4, maxCumulativeOkAck(f.sentFrames));
                int refusal = indexOfBinaryFrame(f.sentFrames, QwpConstants.STATUS_WRITE_ERROR, 5);
                Assert.assertTrue("a fourth distinct table must be refused", refusal > -1);
                Assert.assertEquals("too many distinct tables, limit: 3", errorMessage(f.sentFrames.getQuick(refusal)));
            }
            drainWalQueue();
            assertQuery("SELECT count() FROM tab").expectSize().noRandomAccess().returns("count\n2\n");
            assertQuery("SELECT count() FROM tab1").expectSize().noRandomAccess().returns("count\n2\n");
            assertQuery("SELECT count() FROM tab2").expectSize().noRandomAccess().returns("count\n1\n");
            assertQuery("SELECT count() FROM tab3").expectSize().noRandomAccess().returns("count\n0\n");
        });
    }

    @Test
    public void testIdleReleaseCountsTablesWithUncommittedRows() throws Exception {
        assertMemoryLeak(() -> {
            for (int i = 1; i < 3; i++) {
                execute("CREATE TABLE tab" + i + " (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            }
            byte[] committed = clientEncodedFrame(1, "tab1");
            byte[] uncommitted = createMaskedFrame(WebSocketOpcode.BINARY, deferred(oneRowMessage("tab", 1, 1_000_000)));
            byte[] third = clientEncodedFrame(1, "tab2");
            try (IdleReleaseFixture f = new IdleReleaseFixture(30_000, 2, concat(committed, uncommitted, PING, third))) {
                f.drive(committed.length + uncommitted.length);
                f.clock.nowMicros += 30_000_000;
                f.drive(PING.length);
                Assert.assertEquals("tab keeps its writer for the open deferred group", 1, f.state.getCachedTableCount());

                // Cached tab and released tab1 fill the limit of 2.
                f.drive(third.length);
                Assert.assertEquals(0, maxCumulativeOkAck(f.sentFrames));
                int refusal = indexOfBinaryFrame(f.sentFrames, QwpConstants.STATUS_WRITE_ERROR, 2);
                Assert.assertTrue("a third distinct table must be refused", refusal > -1);
                Assert.assertEquals("too many distinct tables, limit: 2", errorMessage(f.sentFrames.getQuick(refusal)));
            }
            drainWalQueue();
            assertQuery("SELECT count() FROM tab").expectSize().noRandomAccess().returns("count\n0\n");
            assertQuery("SELECT count() FROM tab1").expectSize().noRandomAccess().returns("count\n1\n");
            assertQuery("SELECT count() FROM tab2").expectSize().noRandomAccess().returns("count\n0\n");
        });
    }

    @Test
    public void testIdleReleaseKeepsDistinctTableLimit() throws Exception {
        // qwp.max.tables.per.connection caps the distinct tables one connection
        // writes (QwpConstants, server.conf). Returning idle writers to the pool
        // must not reset that count.
        assertMemoryLeak(() -> {
            for (int i = 1; i < 4; i++) {
                execute("CREATE TABLE tab" + i + " (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            }
            byte[] firstThree = concat(
                    clientEncodedFrame(1, "tab"),
                    clientEncodedFrame(1, "tab1"),
                    clientEncodedFrame(1, "tab2")
            );
            byte[] fourth = clientEncodedFrame(1, "tab3");
            try (IdleReleaseFixture f = new IdleReleaseFixture(30_000, 3, concat(firstThree, PING, fourth))) {
                f.drive(firstThree.length);
                f.clock.nowMicros += 30_000_000;
                f.drive(PING.length);
                Assert.assertEquals("idle PING must release every writer", 0, f.state.getCachedTableCount());

                f.drive(fourth.length);
                int refusal = indexOfBinaryFrame(f.sentFrames, QwpConstants.STATUS_WRITE_ERROR, 3);
                Assert.assertTrue("a fourth distinct table must be refused", refusal > -1);
                Assert.assertEquals("too many distinct tables, limit: 3", errorMessage(f.sentFrames.getQuick(refusal)));
                Assert.assertEquals(2, maxCumulativeOkAck(f.sentFrames));
            }
            drainWalQueue();
            assertQuery("SELECT count() FROM tab3").expectSize().noRandomAccess().returns("count\n0\n");
        });
    }

    @Test
    public void testIdleReleaseMatchesTableNamesIgnoringCase() throws Exception {
        assertMemoryLeak(() -> {
            byte[] lower = clientEncodedFrame(1, "tab");
            byte[] upper = clientEncodedFrame(1, "TAB");
            try (IdleReleaseFixture f = new IdleReleaseFixture(30_000, 1, concat(lower, PING, upper))) {
                f.drive(lower.length);
                f.clock.nowMicros += 30_000_000;
                f.drive(PING.length);
                Assert.assertEquals(0, f.state.getCachedTableCount());

                // TAB names released tab, so it takes the table back within the limit of 1.
                f.drive(upper.length);
                Assert.assertFalse(hasErrorResponseForSeq(f.sentFrames, 1));
                Assert.assertEquals(1, maxCumulativeOkAck(f.sentFrames));
            }
            drainWalQueue();
            assertQuery("SELECT count() FROM tab").expectSize().noRandomAccess().returns("count\n2\n");
        });
    }

    @Test
    public void testIdleReleaseSurvivesWriterCloseFailure() throws Exception {
        // Closing a WAL writer msyncs its column files only when the commit mode is not NOSYNC,
        // and the suite runs either mode, so pin one whose msync failure does not stop the engine.
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "async");
        final AtomicBoolean isMsyncFailing = new AtomicBoolean();
        final AtomicInteger failedMsyncCount = new AtomicInteger();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public void msync(long addr, long len, boolean async) {
                if (isMsyncFailing.get()) {
                    failedMsyncCount.incrementAndGet();
                    throw CairoException.critical(5).put("could not msync");
                }
                super.msync(addr, len, async);
            }
        };
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE tab1 (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            byte[] both = clientEncodedFrame(1, "tab", "tab1");
            byte[] more = clientEncodedFrame(1, "tab1");
            try (IdleReleaseFixture f = new IdleReleaseFixture(30_000, concat(both, PING, more))) {
                f.drive(both.length);
                Assert.assertEquals(2, f.state.getCachedTableCount());

                // A dropped table's writer cannot go back to the pool: the release closes it, and the close fails.
                execute("DROP TABLE tab");
                isMsyncFailing.set(true);
                f.clock.nowMicros += 30_000_000;
                f.sentFrames.clear();
                try {
                    f.drive(PING.length);
                } finally {
                    isMsyncFailing.set(false);
                }
                Assert.assertTrue("the release must close the dropped table's writer", failedMsyncCount.get() > 0);
                Assert.assertEquals("the PING must still be answered", 1, countFrames(f.sentFrames, WebSocketOpcode.PONG));
                Assert.assertEquals("the release must go on to the next idle writer", 0, f.state.getCachedTableCount());
                Assert.assertEquals(0, engine.getBusyWalWriterCount());

                f.drive(more.length);
                Assert.assertEquals(1, maxCumulativeOkAck(f.sentFrames));
            }
            drainWalQueue();
            assertQuery("SELECT count() FROM tab1").expectSize().noRandomAccess().returns("count\n2\n");
        });
    }

    @Test
    public void testOkAckNamesReacquiredTableCommit() throws Exception {
        assertMemoryLeak(() -> {
            byte[] first = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("tab", 100, 1_000_000));
            byte[] second = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("tab", 300, 3_000_000));
            byte[] third = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("tab", 400, 4_000_000));
            try (IdleReleaseFixture f = new IdleReleaseFixture(30_000, concat(first, PING, second, third))) {
                f.drive(first.length);
                Assert.assertEquals(1, sequencerTxn("tab"));
                Assert.assertEquals(1, okAckSeqTxn(f.sentFrames, 0, "tab"));

                f.clock.nowMicros += 30_000_000;
                f.drive(PING.length);
                Assert.assertEquals(0, f.state.getCachedTableCount());

                // Another writer commits while the table is released. The re-acquired writer
                // must report this connection's commits, and only those.
                execute("INSERT INTO tab VALUES (200, '1970-01-01T00:00:02.000000Z')");
                Assert.assertEquals(2, sequencerTxn("tab"));

                f.drive(second.length);
                Assert.assertEquals(1, f.state.getCachedTableCount());
                Assert.assertEquals(3, sequencerTxn("tab"));
                Assert.assertEquals("OK ack must name the re-acquired table's commit", 3, okAckSeqTxn(f.sentFrames, 1, "tab"));

                f.drive(third.length);
                Assert.assertEquals(4, sequencerTxn("tab"));
                Assert.assertEquals(4, okAckSeqTxn(f.sentFrames, 2, "tab"));
            }
            drainWalQueue();
            assertQuery("SELECT v, ts FROM tab")
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            v\tts
                            100\t1970-01-01T00:00:01.000000Z
                            200\t1970-01-01T00:00:02.000000Z
                            300\t1970-01-01T00:00:03.000000Z
                            400\t1970-01-01T00:00:04.000000Z
                            """);
        });
    }

    @Test
    public void testOkAckSeqTxnStaysAboveRecreatedNameAfterRelease() throws Exception {
        assertMemoryLeak(() -> {
            byte[] first = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("tab", 100, 1_000_000));
            byte[] second = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("tab", 200, 2_000_000));
            byte[] third = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("tab", 300, 3_000_000));
            try (IdleReleaseFixture f = new IdleReleaseFixture(30_000, concat(first, second, PING, third))) {
                final String oldDirName = engine.verifyTableName("tab").getDirName();
                final HashMap<String, Long> frontiers = new HashMap<>();
                // Durable at once, so nothing stays pending for the name across the release.
                frontiers.put(oldDirName, Long.MAX_VALUE);
                f.useDurableAck(new FrontierRegistry(frontiers), DurabilityTier.LOCAL);

                // Separate receive passes, so each frame commits its own seqTxn.
                f.drive(first.length);
                f.drive(second.length);
                Assert.assertEquals(2, sequencerTxn("tab"));
                Assert.assertEquals("the client holds a durable watermark of 2", 2, durableAckSeqTxn(f.sentFrames, "tab"));

                f.clock.nowMicros += 30_000_000;
                f.drive(PING.length);
                Assert.assertEquals(0, f.state.getCachedTableCount());

                execute("DROP TABLE tab");
                execute("CREATE TABLE tab (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                final TableToken recreated = engine.verifyTableName("tab");
                Assert.assertNotEquals(oldDirName, recreated.getDirName());
                frontiers.put(recreated.getDirName(), -1L);

                f.drive(third.length);
                Assert.assertEquals(1, sequencerTxn("tab"));
                // The re-created table's seqTxn 1 goes on the wire above the watermark 2
                // the client holds for the name: 1 + (2 + 1 - 1) = 3.
                Assert.assertEquals(3, okAckSeqTxn(f.sentFrames, 2, "tab"));
            }
            drainWalQueue();
            assertQuery("SELECT v, ts FROM tab")
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            v\tts
                            300\t1970-01-01T00:00:03.000000Z
                            """);
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
                Assert.assertEquals(0, f.state.getCachedTableCount());
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
                    Assert.assertEquals("PING before data must release the idle writer", 0, f.state.getCachedTableCount());
                    Assert.assertEquals(0, engine.getBusyWalWriterCount());
                    f.onSend = null;
                };
                f.drive(PING.length * 2 + data.length);
                Assert.assertEquals("one early PING check and one pass-end sample", 2, f.clock.calls);
                Assert.assertEquals(31_000_000, f.state.getLastDataPassEndMicros());
                Assert.assertEquals(1, f.state.getCachedTableCount());
                Assert.assertEquals(1, engine.getBusyWalWriterCount());
            }
        });
    }

    @Test
    public void testPingReleasesAllIdleTableWriters() throws Exception {
        assertMemoryLeak(() -> {
            for (int i = 1; i < 4; i++) {
                execute("CREATE TABLE tab" + i + " (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            }
            byte[] data = concat(
                    clientEncodedFrame(1, "tab"),
                    clientEncodedFrame(1, "tab1"),
                    clientEncodedFrame(1, "tab2"),
                    clientEncodedFrame(1, "tab3")
            );
            try (IdleReleaseFixture f = new IdleReleaseFixture(30_000, concat(data, PING))) {
                f.drive(data.length);
                Assert.assertEquals(4, f.state.getCachedTableCount());
                Assert.assertEquals(4, engine.getBusyWalWriterCount());
                f.clock.nowMicros += 30_000_000;
                f.drive(PING.length);
                Assert.assertEquals("one idle PING must release every cached table", 0, f.state.getCachedTableCount());
                Assert.assertEquals(0, engine.getBusyWalWriterCount());
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
                Assert.assertEquals(0, f.state.getCachedTableCount());
            }
            drainWalQueue();
            assertQuery("SELECT count() FROM tab").expectSize().noRandomAccess().returns("count\n1\n");
        });
    }

    private static int countFrames(ObjList<byte[]> frames, int opcode) {
        int count = 0;
        for (int i = 0, n = frames.size(); i < n; i++) {
            if ((frames.getQuick(i)[0] & 0x0F) == opcode) {
                count++;
            }
        }
        return count;
    }

    // Server error frame: [ws header 2][status 1][seq 8][message length 2, LE][message].
    private static String errorMessage(byte[] frame) {
        int length = (frame[11] & 0xFF) | (frame[12] & 0xFF) << 8;
        return new String(frame, 13, length, StandardCharsets.UTF_8);
    }

    private static long sequencerTxn(String table) {
        return engine.getTableSequencerAPI().lastTxn(engine.verifyTableName(table));
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

    // Local-tier registry whose per-directory durable frontier the test sets.
    private record FrontierRegistry(HashMap<String, Long> frontiers) implements DurableAckRegistry {
        @Override
        public long getLocalDurableSeqTxn(CharSequence tableDirName) {
            return frontiers.getOrDefault(tableDirName.toString(), -1L);
        }

        @Override
        public long getReplicatedDurableSeqTxn(CharSequence tableDirName) {
            return -1L;
        }

        @Override
        public boolean isEnabled() {
            return true;
        }

        @Override
        public boolean isTierSetAvailable(int tiers) {
            return !DurabilityTier.hasReplicated(tiers);
        }

        @Override
        public void onTableDropped(TableToken tableToken) {
        }
    }

    private static class IdleReleaseFixture implements AutoCloseable {
        private final CountingClock clock = new CountingClock();
        private final PhasedNetworkFacade network;
        private final QwpIngressUpgradeProcessor processor;
        private final ObjList<byte[]> sentFrames = new ObjList<>();
        private final QwpIngressProcessorState state;
        private TestableContext context;
        private boolean isSendBlocked;
        private boolean isSendDisconnected;
        private Runnable onSend;
        private DurableAckRegistry previousDurableAckRegistry;
        private long recvBuffer;
        private long sendBuffer;

        private IdleReleaseFixture(long minIdleMs, byte[] wire) throws Exception {
            this(minIdleMs, QwpConstants.DEFAULT_MAX_TABLES_PER_CONNECTION, wire);
        }

        private IdleReleaseFixture(long minIdleMs, int maxTables, byte[] wire) throws Exception {
            execute("CREATE TABLE tab (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            LineHttpProcessorConfiguration lineConfig = new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration) {
                @Override
                public MicrosecondClock getMicrosecondClock() {
                    return clock;
                }

                @Override
                public int getQwpMaxTablesPerConnection() {
                    return maxTables;
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
                        byte[] frame = new byte[size];
                        for (int i = 0; i < size; i++) {
                            frame[i] = Unsafe.getByte(sendBuffer + i);
                        }
                        sentFrames.add(frame);
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
                if (previousDurableAckRegistry != null) {
                    engine.setDurableAckRegistry(previousDurableAckRegistry);
                }
            }
        }

        private void drive(int bytes) throws Exception {
            QwpIngressWireHarness.drive(processor, context, network, bytes);
        }

        // Negotiates durable acks on the connection and reads them from the given
        // registry; close() restores the engine's registry.
        private void useDurableAck(DurableAckRegistry registry, int tiers) {
            previousDurableAckRegistry = engine.getDurableAckRegistry();
            engine.setDurableAckRegistry(registry);
            state.setDurableAckEnabled(true);
            state.setDurableAckTiers(tiers);
        }
    }
}
