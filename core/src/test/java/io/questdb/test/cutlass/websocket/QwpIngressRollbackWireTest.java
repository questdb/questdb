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

import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cutlass.http.DefaultHttpServerConfiguration;
import io.questdb.cutlass.http.HttpFullFatServerConfiguration;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.cutlass.qwp.server.QwpIngressProcessorState;
import io.questdb.cutlass.qwp.server.QwpIngressUpgradeProcessor;
import io.questdb.cutlass.qwp.websocket.WebSocketOpcode;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.test.cutlass.websocket.QwpIngressWireHarness.*;

/**
 * The rollback control frame at the wire level. The frame bytes come from the
 * pinned client's encoder, so the server is tested against what the client
 * sends. Frame sequences a correct client never produces -- a rollback right
 * behind a commit-batch frame -- check that the server stays safe anyway.
 */
public class QwpIngressRollbackWireTest extends AbstractCairoTest {

    @Test
    public void testDataFrameCarryingRollbackBitIsNotARollback() throws Exception {
        // Only the exact control frame rolls back. A deferred data frame that also
        // carries FLAG_ROLLBACK keeps its rows, and the plain frame after it
        // commits them.
        assertMemoryLeak(() -> {
            createTable();
            byte[] message = deferred(oneRowMessage(1, 1_000_000));
            message[QwpConstants.HEADER_OFFSET_FLAGS] |= QwpConstants.FLAG_ROLLBACK;
            ObjList<byte[]> sent = ingestInOnePass(
                    createMaskedFrame(WebSocketOpcode.BINARY, message),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(2, 2_000_000))
            );
            drainWalQueue();
            assertQuery("SELECT v FROM tab").expectSize().returns("v\n1\n2\n");
            Assert.assertEquals(1, maxCumulativeOkAck(sent));
        });
    }

    @Test
    public void testRollbackAfterCommitBatchKeepsTheBatch() throws Exception {
        // A plain frame opens a commit batch. The rollback frame cannot join it,
        // so the batch commits first and only the (empty) deferred group goes.
        assertMemoryLeak(() -> {
            createTable();
            ObjList<byte[]> sent = ingestInOnePass(
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(1, 1_000_000)),
                    rollbackFrame()
            );
            drainWalQueue();
            assertQuery("SELECT v FROM tab").expectSize().returns("v\n1\n");
            Assert.assertEquals(1, maxCumulativeOkAck(sent));
            Assert.assertFalse(hasErrorResponseForSeq(sent, 0));
            Assert.assertFalse(hasErrorResponseForSeq(sent, 1));
        });
    }

    @Test
    public void testRollbackDiscardsDeferredRows() throws Exception {
        // The client's open transaction is rolled back, and the transaction after
        // it commits on its own. The ack covers every frame: the rolled-back
        // frames are resolved, so the client may trim them.
        assertMemoryLeak(() -> {
            createTable();
            ObjList<byte[]> sent = ingestInOnePass(
                    createMaskedFrame(WebSocketOpcode.BINARY, deferred(oneRowMessage(1, 1_000_000))),
                    createMaskedFrame(WebSocketOpcode.BINARY, deferred(oneRowMessage(2, 2_000_000))),
                    rollbackFrame(),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(3, 3_000_000))
            );
            drainWalQueue();
            assertQuery("SELECT v FROM tab").expectSize().returns("v\n3\n");
            Assert.assertEquals(3, maxCumulativeOkAck(sent));
            for (long seq = 0; seq < 4; seq++) {
                Assert.assertFalse("seq " + seq, hasErrorResponseForSeq(sent, seq));
            }
        });
    }

    @Test
    public void testRollbackIsAckedEagerly() throws Exception {
        // The rollback closes a run of withheld deferred frames, so its ack goes
        // out at once rather than after ACK_BATCH_SIZE more frames.
        assertMemoryLeak(() -> {
            createTable();
            ObjList<byte[]> sent = ingestInOnePass(
                    createMaskedFrame(WebSocketOpcode.BINARY, deferred(oneRowMessage(1, 1_000_000))),
                    rollbackFrame()
            );
            Assert.assertEquals(1, maxCumulativeOkAck(sent));
            drainWalQueue();
            assertQuery("SELECT count() FROM tab").noRandomAccess().expectSize().returns("count\n0\n");
        });
    }

    @Test
    public void testRollbackReleasesWithheldRowlessFrame() throws Exception {
        // A rowless deferred frame blocks every ack past it until its group
        // commits. Rolling the group back ends that block.
        assertMemoryLeak(() -> {
            createTable();
            ObjList<byte[]> sent = ingestInOnePass(
                    createMaskedFrame(WebSocketOpcode.BINARY, rowlessDeferredMessage()),
                    createMaskedFrame(WebSocketOpcode.BINARY, deferred(oneRowMessage(1, 1_000_000))),
                    rollbackFrame()
            );
            Assert.assertEquals(2, maxCumulativeOkAck(sent));
            drainWalQueue();
            assertQuery("SELECT count() FROM tab").noRandomAccess().expectSize().returns("count\n0\n");
        });
    }

    @Test
    public void testRollbackWithNothingUncommittedIsAcked() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            ObjList<byte[]> sent = ingestInOnePass(rollbackFrame());
            Assert.assertEquals(0, maxCumulativeOkAck(sent));
            Assert.assertFalse(hasErrorResponseForSeq(sent, 0));
        });
    }

    private static void createTable() throws Exception {
        execute("CREATE TABLE tab (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
    }

    private static ObjList<byte[]> ingestInOnePass(byte[]... frames) throws Exception {
        final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
        final QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);
        final byte[] wire = concat(frames);
        assert wire.length <= RECV_BUFFER_SIZE;
        final ObjList<byte[]> sent = new ObjList<>();
        final PhasedNetworkFacade nf = new PhasedNetworkFacade(wire);
        final long recvBuf = Unsafe.malloc(RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
        final long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
        final RecordingRawSocket rawSocket = new RecordingRawSocket(sendBuf, SEND_BUFFER_SIZE);
        try (TestableContext context = new TestableContext(httpConfig, nf, rawSocket, recvBuf, RECV_BUFFER_SIZE)) {
            final QwpIngressProcessorState state = new QwpIngressProcessorState(
                    RECV_BUFFER_SIZE,
                    httpConfig.getSendBufferSize(),
                    engine,
                    httpConfig.getLineHttpProcessorConfiguration()
            );
            state.of(-1, AllowAllSecurityContext.INSTANCE);
            getLV().set(context, state);
            // Every frame arrives in one receive pass, as from a client with many in flight.
            drive(processor, context, nf, wire.length);
        } finally {
            sent.addAll(rawSocket.sentFrames);
            Unsafe.free(recvBuf, RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
        return sent;
    }

    private static byte[] rollbackFrame() {
        return createMaskedFrame(WebSocketOpcode.BINARY, rollbackMessage());
    }
}
