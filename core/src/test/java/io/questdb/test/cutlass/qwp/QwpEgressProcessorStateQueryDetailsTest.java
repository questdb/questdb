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

import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cutlass.qwp.codec.QwpEgressFrameWriter;
import io.questdb.cutlass.qwp.codec.QwpEgressMsgKind;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.cutlass.qwp.server.egress.QwpEgressProcessorState;
import io.questdb.cutlass.qwp.server.egress.QwpEgressUpgradeProcessor;
import io.questdb.griffin.CompiledQuery;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.NanosecondClock;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit coverage of the per-query details {@link QwpEgressProcessorState} keeps
 * for the {@code QUERY_REQUEST} query-details flags: the durations
 * {@code QUERY_FLAG_STATS} reports, measured against a clock the test sets by
 * hand, and the result-set size {@code QUERY_FLAG_COUNT_ROWS} reports. The
 * wire behaviour is covered by {@link QwpEgressQueryDetailsWireTest}.
 */
public class QwpEgressProcessorStateQueryDetailsTest extends AbstractCairoTest {

    private final ManualClock clock = new ManualClock();

    @Test
    public void testBeginQueryDropsPreviousDetails() {
        try (QwpEgressProcessorState state = new QwpEgressProcessorState(configuration, clock)) {
            clock.ticks = 1_000;
            state.beginQuery();
            state.setQueryDetails(allDetailFlags(), 10);
            state.addQueryCompileNanos(7);
            state.setQueryJitCompiled(true);
            state.onRowWindowRowsSkipped(3);
            state.onRowWindowRowsAppended(4);
            state.parkSqlExecutionOwner();
            clock.ticks = 1_100;
            state.resumeSqlExecutionOwner();
            state.computeQueryRowCount(true, null);
            Assert.assertEquals(7, state.getQueryRowCount());
            Assert.assertEquals(100, state.getQueryClientWaitNanos());

            clock.ticks = 5_000;
            state.beginQuery();
            assertNoDetails(state);
            clock.ticks = 5_250;
            Assert.assertEquals("the clock restarts with the new query", 250, state.getQueryElapsedNanos());
        }
    }

    @Test
    public void testBeginQueryDropsWaitOfQueryParkedAtItsEnd() {
        // A query can end while parked: a disconnect, or a cancel of a stream
        // waiting for credit. The next query must not inherit the open interval.
        try (QwpEgressProcessorState state = new QwpEgressProcessorState(configuration, clock)) {
            state.beginQuery();
            state.setQueryDetails(QwpEgressMsgKind.QUERY_FLAG_STATS, Long.MAX_VALUE);
            clock.ticks = 10;
            state.parkSqlExecutionOwner();

            clock.ticks = 500;
            state.beginQuery();
            clock.ticks = 600;
            state.resumeSqlExecutionOwner();
            Assert.assertEquals(0, state.getQueryClientWaitNanos());
        }
    }

    @Test
    public void testClearDropsDetails() {
        try (QwpEgressProcessorState state = new QwpEgressProcessorState(configuration, clock)) {
            state.beginQuery();
            state.setQueryDetails(allDetailFlags(), 10);
            state.addQueryCompileNanos(7);
            state.setQueryJitCompiled(true);
            state.onRowWindowRowsSkipped(3);
            state.onRowWindowRowsAppended(4);
            state.computeQueryRowCount(true, null);
            state.parkSqlExecutionOwner();

            state.clear();
            assertNoDetails(state);
        }
    }

    @Test
    public void testClientWaitAddsUpParkedIntervals() {
        try (QwpEgressProcessorState state = new QwpEgressProcessorState(configuration, clock)) {
            clock.ticks = 100;
            state.beginQuery();
            // resuming a query that never parked waits for nothing
            clock.ticks = 105;
            state.resumeSqlExecutionOwner();
            Assert.assertEquals(0, state.getQueryClientWaitNanos());

            clock.ticks = 110;
            state.parkSqlExecutionOwner();
            clock.ticks = 140;
            state.resumeSqlExecutionOwner();
            Assert.assertEquals(30, state.getQueryClientWaitNanos());

            // a second park while parked keeps the first start: a socket park
            // can follow a credit park before the query resumes
            clock.ticks = 150;
            state.parkSqlExecutionOwner();
            clock.ticks = 160;
            state.parkSqlExecutionOwner();
            clock.ticks = 200;
            state.resumeSqlExecutionOwner();
            Assert.assertEquals(30 + 50, state.getQueryClientWaitNanos());

            // resuming twice counts the interval once
            clock.ticks = 300;
            state.resumeSqlExecutionOwner();
            Assert.assertEquals(80, state.getQueryClientWaitNanos());
            Assert.assertEquals(200, state.getQueryElapsedNanos());
        }
    }

    @Test
    public void testCompileNanosAddUpRecompilations() {
        try (QwpEgressProcessorState state = new QwpEgressProcessorState(configuration, clock)) {
            state.beginQuery();
            Assert.assertEquals(0, state.getQueryCompileNanos());
            state.addQueryCompileNanos(1_500);
            state.addQueryCompileNanos(2_500);
            Assert.assertEquals(4_000, state.getQueryCompileNanos());
        }
    }

    @Test
    public void testFlagPredicates() {
        try (QwpEgressProcessorState state = new QwpEgressProcessorState(configuration, clock)) {
            state.beginQuery();
            final int[] flags = {
                    QwpEgressMsgKind.QUERY_FLAG_ERROR_POSITION,
                    QwpEgressMsgKind.QUERY_FLAG_STATS,
                    QwpEgressMsgKind.QUERY_FLAG_COUNT_ROWS
            };
            for (int set : flags) {
                state.setQueryDetails(set | QwpEgressMsgKind.QUERY_FLAG_RESET_DICT, Long.MAX_VALUE);
                Assert.assertEquals(set == QwpEgressMsgKind.QUERY_FLAG_ERROR_POSITION, state.isErrorPositionRequested());
                Assert.assertEquals(set == QwpEgressMsgKind.QUERY_FLAG_STATS, state.isStatsRequested());
                Assert.assertEquals(set == QwpEgressMsgKind.QUERY_FLAG_COUNT_ROWS, state.isRowCountRequested());
                Assert.assertEquals(set == QwpEgressMsgKind.QUERY_FLAG_COUNT_ROWS, state.isRowCountPending());
            }
            // the row-window bits, the reserved timeout bit and unknown bits
            // request none of the details
            state.setQueryDetails(QwpEgressMsgKind.QUERY_FLAG_SKIP_ROWS | QwpEgressMsgKind.QUERY_FLAG_MAX_ROWS
                    | QwpEgressMsgKind.QUERY_FLAG_TIMEOUT | 0x80, 5);
            Assert.assertFalse(state.isErrorPositionRequested());
            Assert.assertFalse(state.isStatsRequested());
            Assert.assertFalse(state.isRowCountRequested());
            Assert.assertEquals(5, state.getRowWindowRemaining());
        }
    }

    @Test
    public void testOneArgConstructorTimesWithConfigurationClock() {
        // the state a test builds without a clock times with the configuration's
        setCurrentMicros(1_000);
        try (QwpEgressProcessorState state = new QwpEgressProcessorState(configuration)) {
            state.beginQuery();
            setCurrentMicros(1_250);
            Assert.assertEquals(250_000, state.getQueryElapsedNanos());
        }
    }

    @Test
    public void testResultEndFitsItsReservation() throws Exception {
        assertMemoryLeak(() -> {
            // The space streamResults keeps for RESULT_END behind the last batch, from
            // the spec: WebSocket header (at most 10) + QWP header (12) + msg_kind (1)
            // + request_id (8) + final_seq and total_rows varints (at most 10 each),
            // plus a row_count varint (10) and the statistics block of four varints
            // and a flags byte (41) when the request asks for them.
            Assert.assertEquals(51, QwpEgressUpgradeProcessor.resultEndWorstCase(false, false));
            Assert.assertEquals(61, QwpEgressUpgradeProcessor.resultEndWorstCase(true, false));
            Assert.assertEquals(92, QwpEgressUpgradeProcessor.resultEndWorstCase(false, true));
            Assert.assertEquals(102, QwpEgressUpgradeProcessor.resultEndWorstCase(true, true));

            // The longest RESULT_END the writer composes, every value at the widest a
            // long takes (9 varint bytes), fits the reservation.
            final int bufSize = 256;
            final long buf = Unsafe.malloc(bufSize, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int details = 0; details < 4; details++) {
                    final boolean isRowCountRequested = (details & 1) != 0;
                    final boolean isStatsRequested = (details & 2) != 0;
                    try (QwpEgressProcessorState state = new QwpEgressProcessorState(configuration, clock)) {
                        clock.ticks = 0;
                        clock.step = 0;
                        state.beginQuery();
                        state.setQueryDetails(
                                (isRowCountRequested ? QwpEgressMsgKind.QUERY_FLAG_COUNT_ROWS : 0)
                                        | (isStatsRequested ? QwpEgressMsgKind.QUERY_FLAG_STATS : 0)
                                        | QwpEgressMsgKind.QUERY_FLAG_MAX_ROWS,
                                1
                        );
                        state.addQueryCompileNanos(Long.MAX_VALUE);
                        state.parkSqlExecutionOwner();
                        clock.ticks = Long.MAX_VALUE;
                        state.resumeSqlExecutionOwner();
                        state.beginStreaming(1, null, new CountingCursor(Long.MAX_VALUE, 0), 1, 0, null, CompiledQuery.SELECT, false);
                        clock.ticks = 0;
                        clock.step = Long.MAX_VALUE;
                        state.computeQueryRowCount(false, null);
                        clock.step = 0;
                        clock.ticks = Long.MAX_VALUE;
                        state.endStreaming();
                        Assert.assertEquals(Long.MAX_VALUE, state.getQueryRowCount());
                        Assert.assertEquals(Long.MAX_VALUE, state.getQueryCompileNanos());
                        Assert.assertEquals(Long.MAX_VALUE, state.getQueryElapsedNanos());
                        Assert.assertEquals(Long.MAX_VALUE, state.getQueryCountNanos());
                        Assert.assertEquals(Long.MAX_VALUE, state.getQueryClientWaitNanos());

                        final long qwpStart = buf + QwpEgressFrameWriter.WS_HEADER_RESERVATION;
                        final long bodyStart = QwpEgressFrameWriter.writeMessageHeader(qwpStart, QwpConstants.VERSION, (byte) 0, 0, 0);
                        final long bodyEnd = QwpEgressUpgradeProcessor.writeResultEndBody(
                                bodyStart, state, Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE
                        );
                        final String msg = "rowCount=" + isRowCountRequested + ", stats=" + isStatsRequested;
                        Assert.assertEquals(
                                msg,
                                QwpEgressFrameWriter.WS_HEADER_RESERVATION + QwpConstants.HEADER_SIZE + 1 + 8 + 9 + 9
                                        + (isRowCountRequested ? 9 : 0) + (isStatsRequested ? 4 * 9 + 1 : 0),
                                bodyEnd - buf
                        );
                        Assert.assertTrue(
                                msg,
                                bodyEnd - buf <= QwpEgressUpgradeProcessor.resultEndWorstCase(isRowCountRequested, isStatsRequested)
                        );
                    }
                }
            } finally {
                Unsafe.free(buf, bufSize, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testRowCountOfDrainedCursorIsSkippedPlusStreamed() {
        try (QwpEgressProcessorState state = new QwpEgressProcessorState(configuration, clock)) {
            state.beginQuery();
            state.setQueryDetails(QwpEgressMsgKind.QUERY_FLAG_COUNT_ROWS, Long.MAX_VALUE);
            state.onRowWindowRowsSkipped(3);
            state.onRowWindowRowsAppended(16_384);
            state.onRowWindowRowsAppended(5);
            Assert.assertTrue(state.isRowCountPending());
            clock.ticks = 42;
            clock.step = 9;
            // the drained cursor is not consulted: there is none on state
            state.computeQueryRowCount(true, null);
            Assert.assertEquals(3 + 16_384 + 5, state.getQueryRowCount());
            // nothing was left to count, so no count time, as on /exec
            Assert.assertEquals(0, state.getQueryCountNanos());
            Assert.assertFalse(state.isRowCountPending());
        }
    }

    @Test
    public void testRowCountOfEmptyResult() {
        try (QwpEgressProcessorState state = new QwpEgressProcessorState(configuration, clock)) {
            state.beginQuery();
            state.setQueryDetails(QwpEgressMsgKind.QUERY_FLAG_COUNT_ROWS, Long.MAX_VALUE);
            state.onRowWindowRowsAppended(0);
            state.computeQueryRowCount(true, null);
            Assert.assertEquals(0, state.getQueryRowCount());
        }
    }

    @Test
    public void testRowCountWhenWindowEndsBeforeCursorOfKnownSize() {
        try (QwpEgressProcessorState state = new QwpEgressProcessorState(configuration, clock)) {
            state.beginQuery();
            state.setQueryDetails(QwpEgressMsgKind.QUERY_FLAG_COUNT_ROWS, 2);
            final CountingCursor cursor = new CountingCursor(1_000, 997);
            state.beginStreaming(1, null, cursor, 1, 0, null, CompiledQuery.SELECT, false);
            state.onRowWindowRowsSkipped(1);
            state.onRowWindowRowsAppended(2);
            Assert.assertEquals(0, state.getRowWindowRemaining());
            clock.ticks = 1_000;
            clock.step = 40;
            state.computeQueryRowCount(false, null);
            // a cursor that knows its size is not read to the end
            Assert.assertEquals(1_000, state.getQueryRowCount());
            Assert.assertEquals(0, cursor.hasNextCalls);
            // asking the cursor for its size counts the rows past the window
            Assert.assertEquals(40, state.getQueryCountNanos());
            state.endStreaming();
            Assert.assertTrue(cursor.isClosed);
            Assert.assertEquals("the count outlives the cursor", 1_000, state.getQueryRowCount());
        }
    }

    @Test
    public void testRowCountWhenWindowEndsBeforeCursorOfUnknownSize() {
        try (QwpEgressProcessorState state = new QwpEgressProcessorState(configuration, clock)) {
            state.beginQuery();
            state.setQueryDetails(QwpEgressMsgKind.QUERY_FLAG_COUNT_ROWS, 2);
            final CountingCursor cursor = new CountingCursor(-1, 997);
            state.beginStreaming(1, null, cursor, 1, 0, null, CompiledQuery.SELECT, false);
            state.onRowWindowRowsSkipped(1);
            state.onRowWindowRowsAppended(2);
            clock.ticks = 1_000;
            clock.step = 250;
            state.computeQueryRowCount(false, null);
            Assert.assertEquals(1 + 2 + 997, state.getQueryRowCount());
            Assert.assertEquals("calculateSize() reads the rest of the cursor", 998, cursor.hasNextCalls);
            Assert.assertEquals(250, state.getQueryCountNanos());
            state.endStreaming();
        }
    }

    @Test
    public void testRowWindowNarrowsWithAppendedRows() {
        try (QwpEgressProcessorState state = new QwpEgressProcessorState(configuration, clock)) {
            state.beginQuery();
            Assert.assertEquals("no window until the request decodes", Long.MAX_VALUE, state.getRowWindowRemaining());
            state.setQueryDetails(QwpEgressMsgKind.QUERY_FLAG_MAX_ROWS, 10);
            state.onRowWindowRowsAppended(4);
            Assert.assertEquals(6, state.getRowWindowRemaining());
            state.onRowWindowRowsAppended(0);
            Assert.assertEquals(6, state.getRowWindowRemaining());
            state.onRowWindowRowsAppended(6);
            Assert.assertEquals(0, state.getRowWindowRemaining());
        }
    }

    private static int allDetailFlags() {
        return QwpEgressMsgKind.QUERY_FLAG_ERROR_POSITION | QwpEgressMsgKind.QUERY_FLAG_STATS
                | QwpEgressMsgKind.QUERY_FLAG_COUNT_ROWS | QwpEgressMsgKind.QUERY_FLAG_MAX_ROWS;
    }

    private static void assertNoDetails(QwpEgressProcessorState state) {
        Assert.assertFalse(state.isErrorPositionRequested());
        Assert.assertFalse(state.isStatsRequested());
        Assert.assertFalse(state.isRowCountRequested());
        Assert.assertFalse(state.isRowCountPending());
        Assert.assertFalse(state.isQueryJitCompiled());
        Assert.assertEquals(0, state.getQueryCompileNanos());
        Assert.assertEquals(0, state.getQueryCountNanos());
        Assert.assertEquals(0, state.getQueryClientWaitNanos());
        Assert.assertEquals(-1, state.getQueryRowCount());
        Assert.assertEquals(Long.MAX_VALUE, state.getRowWindowRemaining());
    }

    /**
     * A cursor with {@code rows} rows left after the window and a fixed
     * {@code size()}, counting how often it is read.
     */
    private static final class CountingCursor implements RecordCursor {
        private final long size;
        private long rows;
        private int hasNextCalls;
        private boolean isClosed;

        private CountingCursor(long size, long rows) {
            this.size = size;
            this.rows = rows;
        }

        @Override
        public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
            while (hasNext()) {
                counter.inc();
            }
        }

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
            hasNextCalls++;
            if (rows > 0) {
                rows--;
                return true;
            }
            return false;
        }

        @Override
        public long preComputedStateSize() {
            return 0;
        }

        @Override
        public void recordAt(Record record, long atRowId) {
        }

        @Override
        public long size() {
            return size;
        }

        @Override
        public void toTop() {
        }
    }

    /**
     * A clock that reads {@code ticks}, then moves it on by {@code step}.
     */
    private static final class ManualClock implements NanosecondClock {
        private long step;
        private long ticks;

        @Override
        public long getTicks() {
            final long now = ticks;
            ticks += step;
            return now;
        }
    }
}
