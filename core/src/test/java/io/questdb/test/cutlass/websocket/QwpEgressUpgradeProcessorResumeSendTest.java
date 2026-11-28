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
import io.questdb.cairo.sql.ColumnMapping;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cutlass.http.DefaultHttpServerConfiguration;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.HttpFullFatServerConfiguration;
import io.questdb.cutlass.http.HttpRawSocket;
import io.questdb.cutlass.http.HttpServerConfiguration;
import io.questdb.cutlass.http.LocalValue;
import io.questdb.cutlass.qwp.server.egress.QwpEgressProcessorState;
import io.questdb.cutlass.qwp.server.egress.QwpEgressUpgradeProcessor;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.PlainSocketFactory;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;

/**
 * Tests for {@link QwpEgressUpgradeProcessor#resumeSend} covering all branches:
 * <ol>
 *     <li>null state - flushes deferred bytes, returns silently</li>
 *     <li>handshakeFlushPending - flushes 101 bytes then finalizes the
 *     protocol switch (mirror of the ingress fix in PR 7129)</li>
 *     <li>state present but not streaming and no handshake flush pending -
 *     flushes only</li>
 *     <li>deferred flush itself blocked - PeerIsSlowToReadException propagates
 *     so the framework re-parks the connection</li>
 * </ol>
 */
public class QwpEgressUpgradeProcessorResumeSendTest extends AbstractCairoTest {

    // One row whose RESULT_BATCH fits SEND_BUFFER_SIZE, leaving too little room for RESULT_END.
    private static final String FINAL_BATCH_SQL = "SELECT rpad('a', 190, 'b') s FROM long_sequence(1)";
    private static final int SEND_BUFFER_SIZE = 256;

    @Test
    public void testParkedFinalBatchFactoryIsCachedByResumingProcessor() throws Exception {
        // A fiber-limit reload closes the processor that parked the final RESULT_BATCH of a
        // cacheable SELECT on the two-send path, and the connection resumes on another
        // processor. That processor finishes the query and puts the factory into its own
        // select cache, which frees the factory when it closes.
        assertMemoryLeak(() -> {
            HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            CloseSpyFactory factory = null;
            try {
                MockRawSocket rawSocket = new MockRawSocket(sendBuf, SEND_BUFFER_SIZE);
                try (TestableContext context = new TestableContext(httpConfig, rawSocket)) {
                    QwpEgressProcessorState state = setupState(context);
                    factory = new CloseSpyFactory(select(FINAL_BATCH_SQL));
                    try (QwpEgressUpgradeProcessor processor = new QwpEgressUpgradeProcessor(engine, httpConfig, 1)) {
                        parkFinalBatch(processor, context, rawSocket, state, factory);
                    }
                    try (QwpEgressUpgradeProcessor processor = new QwpEgressUpgradeProcessor(engine, httpConfig, 1)) {
                        rawSocket.setThrowPeerSlow(false);
                        processor.resumeSend(context);
                        // RESULT_END follows the parked RESULT_BATCH.
                        Assert.assertEquals(2, rawSocket.sendCallCount);
                        Assert.assertFalse(state.isStreamingActive());
                        Assert.assertFalse(factory.isClosed);
                    }
                    Assert.assertTrue(factory.isClosed);
                }
            } finally {
                Misc.free(factory);
                Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testParkedFinalBatchFactoryIsFreedAfterItsProcessorCloses() throws Exception {
        // A fiber-limit reload closes processors while their connections stay parked. The
        // final RESULT_BATCH of a cacheable SELECT parks on the two-send path, the processor
        // that ran it closes with its select cache, then the connection drops. The factory
        // must be freed, not put into the closed cache.
        assertMemoryLeak(() -> {
            HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            CloseSpyFactory factory = null;
            try {
                MockRawSocket rawSocket = new MockRawSocket(sendBuf, SEND_BUFFER_SIZE);
                try (TestableContext context = new TestableContext(httpConfig, rawSocket)) {
                    QwpEgressProcessorState state = setupState(context);
                    factory = new CloseSpyFactory(select(FINAL_BATCH_SQL));
                    try (QwpEgressUpgradeProcessor processor = new QwpEgressUpgradeProcessor(engine, httpConfig, 1)) {
                        parkFinalBatch(processor, context, rawSocket, state, factory);
                    }
                    Assert.assertFalse(factory.isClosed);
                }
                // Closing the context drops the connection and ends its streaming.
                Assert.assertTrue(factory.isClosed);
            } finally {
                Misc.free(factory);
                Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testResumeSendDeferredFlushReparks() throws Exception {
        assertMemoryLeak(() -> {
            HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            try (QwpEgressUpgradeProcessor processor = new QwpEgressUpgradeProcessor(engine, httpConfig, 1)) {
                long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                try (TestableContext context = new TestableContext(httpConfig, new MockRawSocket(sendBuf, SEND_BUFFER_SIZE))) {
                    context.setResumeSendThrowsPeerSlow(true);
                    QwpEgressProcessorState state = setupState(context);
                    state.setHandshakeFlushPending(true);

                    try {
                        processor.resumeSend(context);
                        Assert.fail("Expected PeerIsSlowToReadException");
                    } catch (PeerIsSlowToReadException e) {
                        // expected
                    }
                    // Still pending: a future resumeSend should retry the flush.
                    Assert.assertTrue(state.isHandshakeFlushPending());
                    Assert.assertFalse(state.isWsHandshakeSent());
                    Assert.assertFalse(context.isSwitchProtocolCalled());
                } finally {
                    Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testResumeSendLeavesStreamingTimerSuspendedUntilDeferredFlushCompletes() throws Exception {
        // This is transport fault injection, not a client-contract assertion. A retained
        // cursor is already parked when resumeSend starts. The first flush re-parks, so its
        // timer must remain suspended; only the second, successful flush may resume it.
        assertMemoryLeak(() -> {
            HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            try (QwpEgressUpgradeProcessor processor = new QwpEgressUpgradeProcessor(engine, httpConfig, 1)) {
                long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                try (TestableContext context = new TestableContext(httpConfig, new MockRawSocket(sendBuf, SEND_BUFFER_SIZE))) {
                    QwpEgressProcessorState state = setupState(context);
                    TimerSpyRecordCursor cursor = new TimerSpyRecordCursor();
                    state.beginStreaming(1, null, cursor, 0, 1, null, CompiledQuery.NONE, false);
                    state.consumeStreamingCredit(1);
                    state.suspendStreamingTimer();
                    context.setResumeSendThrowsPeerSlow(true);

                    try {
                        processor.resumeSend(context);
                        Assert.fail("Expected PeerIsSlowToReadException");
                    } catch (PeerIsSlowToReadException expected) {
                        // expected: deferred bytes are still not flushed
                    }
                    Assert.assertEquals("re-park must not resume the retained cursor", 0, cursor.resumeCalls);
                    Assert.assertEquals(1, cursor.suspendCalls);

                    context.setResumeSendThrowsPeerSlow(false);
                    processor.resumeSend(context);

                    Assert.assertEquals("successful deferred flush resumes before result production", 1, cursor.resumeCalls);
                    Assert.assertEquals(
                            "credit exhaustion after the resumed flush re-suspends the cursor",
                            2,
                            cursor.suspendCalls
                    );
                    Assert.assertTrue(state.isStreamingCreditSuspended());
                    state.endStreaming();
                    Assert.assertTrue(cursor.isClosed);
                } finally {
                    Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testResumeSendReparkSuspendsStreamingTimer() throws Exception {
        // The deferred flush completed, but resumed result production itself hit PISR.
        // That second park must suspend the cursor before the exception reaches the dispatcher.
        assertMemoryLeak(() -> {
            HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            try (QwpEgressUpgradeProcessor processor = new QwpEgressUpgradeProcessor(engine, httpConfig, 1)) {
                long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                MockRawSocket rawSocket = new MockRawSocket(sendBuf, SEND_BUFFER_SIZE);
                rawSocket.setThrowPeerSlow(true);
                try (TestableContext context = new TestableContext(httpConfig, rawSocket)) {
                    QwpEgressProcessorState state = setupState(context);
                    TimerSpyRecordCursor cursor = new TimerSpyRecordCursor();
                    state.beginStreaming(1, null, cursor, 0, 1, null, CompiledQuery.NONE, false);
                    state.suspendStreamingTimer();

                    try {
                        processor.resumeSend(context);
                        Assert.fail("Expected PeerIsSlowToReadException");
                    } catch (PeerIsSlowToReadException expected) {
                        // expected: result production, not deferred flushing, re-parked
                    }
                    Assert.assertEquals(1, cursor.resumeCalls);
                    Assert.assertEquals(2, cursor.suspendCalls);
                    state.endStreaming();
                } finally {
                    Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testStateForwardsTimersToPageFrameCursor() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE qwp_timer_page (x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO qwp_timer_page SELECT x, x::TIMESTAMP FROM long_sequence(2)");

            try (QwpEgressProcessorState state = new QwpEgressProcessorState(configuration)) {
                RecordCursorFactory factory = null;
                PageFrameCursor pageFrameCursor = null;
                try {
                    factory = select("SELECT * FROM qwp_timer_page");
                    pageFrameCursor = factory.getPageFrameCursor(
                            sqlExecutionContext,
                            PartitionFrameCursorFactory.ORDER_ASC
                    );
                    Assert.assertNotNull(pageFrameCursor);
                    TimerSpyPageFrameCursor cursor = new TimerSpyPageFrameCursor(pageFrameCursor);
                    state.beginStreamingPageFrame(
                            1,
                            factory,
                            cursor,
                            false,
                            null,
                            2,
                            0,
                            "SELECT * FROM qwp_timer_page",
                            CompiledQuery.NONE,
                            false
                    );
                    factory = null;
                    pageFrameCursor = null;

                    state.suspendStreamingTimer();
                    state.resumeStreamingTimer();

                    Assert.assertEquals(1, cursor.suspendCalls);
                    Assert.assertEquals(1, cursor.resumeCalls);
                    state.endStreaming();
                } finally {
                    Misc.free(pageFrameCursor);
                    Misc.free(factory);
                }
            }
        });
    }

    @Test
    public void testResumeSendHandshakeFlushPendingFinalizes() throws Exception {
        // The PR 7129 mirror path: 101 Switching Protocols bytes could not leave
        // the socket in a single write; onRequestComplete parked the context and
        // staged the rest. resumeSend flushes them, then finalizeHandshake sets
        // wsHandshakeSent and calls switchProtocol so subsequent recvs parse
        // WebSocket frames rather than HTTP.
        assertMemoryLeak(() -> {
            HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            try (QwpEgressUpgradeProcessor processor = new QwpEgressUpgradeProcessor(engine, httpConfig, 1)) {
                long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                try (TestableContext context = new TestableContext(httpConfig, new MockRawSocket(sendBuf, SEND_BUFFER_SIZE))) {
                    QwpEgressProcessorState state = setupState(context);
                    state.setNegotiatedVersion((byte) 1);
                    state.setHandshakeFlushPending(true);
                    state.setPendingHandshakeBytes(50);

                    processor.resumeSend(context);

                    Assert.assertTrue(context.isResumeResponseSendCalled());
                    Assert.assertTrue(state.isWsHandshakeSent());
                    Assert.assertFalse(state.isHandshakeFlushPending());
                    Assert.assertEquals(0, state.getPendingHandshakeBytes());
                    Assert.assertTrue(context.isSwitchProtocolCalled());
                }
                Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testResumeSendNullState() throws Exception {
        // No state attached yet: the deferred flush still runs (it's the only
        // path back to the user's socket), but with no QwpEgressProcessorState
        // there is nothing else to drive. Must not throw.
        assertMemoryLeak(() -> {
            HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            try (QwpEgressUpgradeProcessor processor = new QwpEgressUpgradeProcessor(engine, httpConfig, 1)) {
                long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                try (TestableContext context = new TestableContext(httpConfig, new MockRawSocket(sendBuf, SEND_BUFFER_SIZE))) {
                    processor.resumeSend(context);

                    Assert.assertTrue(context.isResumeResponseSendCalled());
                    Assert.assertFalse(context.isSwitchProtocolCalled());
                }
                Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testResumeSendStateNotStreamingNoHandshake() throws Exception {
        // Default state after handshake completed and no streaming in progress:
        // a parked send (for a QUERY_ERROR or a final RESULT_END) finished in
        // the deferred flush. Nothing else to do.
        assertMemoryLeak(() -> {
            HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            try (QwpEgressUpgradeProcessor processor = new QwpEgressUpgradeProcessor(engine, httpConfig, 1)) {
                long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                try (TestableContext context = new TestableContext(httpConfig, new MockRawSocket(sendBuf, SEND_BUFFER_SIZE))) {
                    QwpEgressProcessorState state = setupState(context);
                    Assert.assertFalse(state.isStreamingActive());
                    Assert.assertFalse(state.isHandshakeFlushPending());

                    processor.resumeSend(context);

                    Assert.assertTrue(context.isResumeResponseSendCalled());
                    Assert.assertFalse(context.isSwitchProtocolCalled());
                }
                Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @SuppressWarnings("unchecked")
    private static LocalValue<QwpEgressProcessorState> getLV() throws Exception {
        Field lvField = QwpEgressUpgradeProcessor.class.getDeclaredField("LV");
        lvField.setAccessible(true);
        return (LocalValue<QwpEgressProcessorState>) lvField.get(null);
    }

    // Parks the final RESULT_BATCH of FINAL_BATCH_SQL on the two-send path: the batch fits the
    // send buffer, but RESULT_END does not fit after it.
    private static void parkFinalBatch(
            QwpEgressUpgradeProcessor processor,
            TestableContext context,
            MockRawSocket rawSocket,
            QwpEgressProcessorState state,
            CloseSpyFactory factory
    ) throws Exception {
        RecordMetadata metadata = factory.getMetadata();
        // Room for more rows than the cursor has, so the batch sees the cursor end.
        state.setMaxBatchRows(10);
        state.borrowColumnDefs(1).getQuick(0).of(metadata.getColumnName(0), metadata.getColumnType(0));
        state.beginStreaming(
                1,
                factory,
                factory.getCursor(sqlExecutionContext),
                1,
                0,
                FINAL_BATCH_SQL,
                CompiledQuery.SELECT,
                true
        );
        Assert.assertTrue(state.isStreamingFactoryCacheable());
        rawSocket.setThrowPeerSlow(true);
        try {
            processor.resumeSend(context);
            Assert.fail("Expected PeerIsSlowToReadException");
        } catch (PeerIsSlowToReadException expected) {
            // expected: the RESULT_BATCH send parked
        }
        // Only the RESULT_BATCH frame with the query's only row went out, and the query still
        // streams: the batch took the final-batch path but could not coalesce RESULT_END.
        Assert.assertEquals(1, rawSocket.sendCallCount);
        Assert.assertEquals(1, state.getStreamingRowsEmitted());
        Assert.assertTrue(state.isStreamingActive());
    }

    private static QwpEgressProcessorState setupState(TestableContext context) throws Exception {
        LocalValue<QwpEgressProcessorState> lv = getLV();
        QwpEgressProcessorState state = new QwpEgressProcessorState(configuration);
        state.of(-1, AllowAllSecurityContext.INSTANCE);
        lv.set(context, state);
        return state;
    }

    private static class TimerSpyPageFrameCursor implements PageFrameCursor {
        private final PageFrameCursor base;
        private int resumeCalls;
        private int suspendCalls;

        private TimerSpyPageFrameCursor(PageFrameCursor base) {
            this.base = base;
        }

        @Override
        public void calculateSize(RecordCursor.Counter counter) {
            base.calculateSize(counter);
        }

        @Override
        public void close() {
            base.close();
        }

        @Override
        public ColumnMapping getColumnMapping() {
            return base.getColumnMapping();
        }

        @Override
        public long getRemainingRowsInInterval() {
            return base.getRemainingRowsInInterval();
        }

        @Override
        public StaticSymbolTable getSymbolTable(int columnIndex) {
            return base.getSymbolTable(columnIndex);
        }

        @Override
        public boolean isExternal() {
            return base.isExternal();
        }

        @Override
        public PageFrame next(long skipTarget) {
            return base.next(skipTarget);
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return base.newSymbolTable(columnIndex);
        }

        @Override
        public void resumeTimer() {
            resumeCalls++;
            base.resumeTimer();
        }

        @Override
        public long size() {
            return base.size();
        }

        @Override
        public void suspendTimer() {
            suspendCalls++;
            base.suspendTimer();
        }

        @Override
        public boolean supportsSizeCalculation() {
            return base.supportsSizeCalculation();
        }

        @Override
        public void toTop() {
            base.toTop();
        }
    }

    private static class TimerSpyRecordCursor implements RecordCursor {
        private boolean isClosed;
        private int resumeCalls;
        private int suspendCalls;

        @Override
        public void close() {
            isClosed = true;
        }

        @Override
        public Record getRecord() {
            return null;
        }

        @Override
        public Record getRecordB() {
            return null;
        }

        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public long preComputedStateSize() {
            return 0;
        }

        @Override
        public void recordAt(Record record, long atRowId) {
        }

        @Override
        public void resumeTimer() {
            resumeCalls++;
        }

        @Override
        public long size() {
            return 0;
        }

        @Override
        public void suspendTimer() {
            suspendCalls++;
        }

        @Override
        public void toTop() {
        }
    }

    private static class CloseSpyFactory implements RecordCursorFactory {
        private final RecordCursorFactory base;
        private boolean isClosed;

        CloseSpyFactory(RecordCursorFactory base) {
            this.base = base;
        }

        @Override
        public void close() {
            if (!isClosed) {
                isClosed = true;
                base.close();
            }
        }

        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
            return base.getCursor(executionContext);
        }

        @Override
        public RecordMetadata getMetadata() {
            return base.getMetadata();
        }

        @Override
        public boolean recordCursorSupportsRandomAccess() {
            return base.recordCursorSupportsRandomAccess();
        }
    }

    private static class MockRawSocket implements HttpRawSocket {
        private final long bufferAddress;
        private final int bufferSize;
        int sendCallCount;
        int sentSize;
        private boolean isThrowPeerSlow;

        MockRawSocket(long bufferAddress, int bufferSize) {
            this.bufferAddress = bufferAddress;
            this.bufferSize = bufferSize;
        }

        @Override
        public long getBufferAddress() {
            return bufferAddress;
        }

        @Override
        public int getBufferSize() {
            return bufferSize;
        }

        @Override
        public void send(int size) throws PeerIsSlowToReadException {
            sendCallCount++;
            sentSize = size;
            if (isThrowPeerSlow) {
                throw PeerIsSlowToReadException.INSTANCE;
            }
        }

        void setThrowPeerSlow(boolean isThrowPeerSlow) {
            this.isThrowPeerSlow = isThrowPeerSlow;
        }
    }

    private static class TestableContext extends HttpConnectionContext {
        private final MockRawSocket mockRawSocket;
        private boolean resumeResponseSendCalled;
        private boolean resumeSendThrowsPeerSlow;
        private boolean switchProtocolCalled;

        TestableContext(HttpServerConfiguration config, MockRawSocket mockRawSocket) {
            super(config, PlainSocketFactory.INSTANCE);
            this.mockRawSocket = mockRawSocket;
        }

        @Override
        public HttpRawSocket getRawResponseSocket() {
            return mockRawSocket;
        }

        @Override
        public void resumeResponseSend() throws PeerIsSlowToReadException {
            resumeResponseSendCalled = true;
            if (resumeSendThrowsPeerSlow) {
                throw PeerIsSlowToReadException.INSTANCE;
            }
        }

        @Override
        public void switchProtocol() {
            switchProtocolCalled = true;
            super.switchProtocol();
        }

        boolean isResumeResponseSendCalled() {
            return resumeResponseSendCalled;
        }

        boolean isSwitchProtocolCalled() {
            return switchProtocolCalled;
        }

        void setResumeSendThrowsPeerSlow(boolean v) {
            resumeSendThrowsPeerSlow = v;
        }
    }
}
