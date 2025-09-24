/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.griffin.engine.join;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.SingleRecordSink;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.TimeFrame;
import io.questdb.cairo.sql.TimeFrameRecordCursor;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.model.JoinContext;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.Rows;

public final class AsOfJoinFastRecordCursorFactory extends AbstractJoinRecordCursorFactory {
    private final AsOfJoinKeyedFastRecordCursor cursor;
    private final RecordSink masterKeySink;
    private final RecordSink slaveKeySink;
    private final SymbolShortCircuit symbolShortCircuit;
    private final long toleranceInterval;

    public AsOfJoinFastRecordCursorFactory(
            CairoConfiguration configuration,
            RecordMetadata metadata,
            RecordCursorFactory masterFactory,
            RecordSink masterKeySink,
            RecordCursorFactory slaveFactory,
            RecordSink slaveKeySink,
            int columnSplit,
            SymbolShortCircuit symbolShortCircuit,
            JoinContext joinContext,
            long toleranceInterval
    ) {
        super(metadata, joinContext, masterFactory, slaveFactory);
        assert slaveFactory.supportsTimeFrameCursor();
        this.masterKeySink = masterKeySink;
        this.slaveKeySink = slaveKeySink;
        long maxSinkTargetHeapSize = (long) configuration.getSqlHashJoinValuePageSize() * configuration.getSqlHashJoinValueMaxPages();
        this.cursor = new AsOfJoinKeyedFastRecordCursor(
                columnSplit,
                NullRecordFactory.getInstance(slaveFactory.getMetadata()),
                masterFactory.getMetadata().getTimestampIndex(),
                new SingleRecordSink(maxSinkTargetHeapSize, MemoryTag.NATIVE_RECORD_CHAIN),
                masterFactory.getMetadata().getTimestampType(),
                slaveFactory.getMetadata().getTimestampIndex(),
                new SingleRecordSink(maxSinkTargetHeapSize, MemoryTag.NATIVE_RECORD_CHAIN),
                slaveFactory.getMetadata().getTimestampType(),
                configuration.getSqlAsOfJoinLookAhead()
        );
        this.symbolShortCircuit = symbolShortCircuit;
        this.toleranceInterval = toleranceInterval;
    }

    @Override
    public boolean followedOrderByAdvice() {
        return masterFactory.followedOrderByAdvice();
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        RecordCursor masterCursor = masterFactory.getCursor(executionContext);
        TimeFrameRecordCursor slaveCursor = null;
        try {
            slaveCursor = slaveFactory.getTimeFrameCursor(executionContext);
            cursor.of(masterCursor, slaveCursor, executionContext.getCircuitBreaker());
            return cursor;
        } catch (Throwable e) {
            Misc.free(slaveCursor);
            Misc.free(masterCursor);
            throw e;
        }
    }

    @Override
    public int getScanDirection() {
        return masterFactory.getScanDirection();
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("AsOf Join Fast Scan");
        sink.attr("condition").val(joinContext);
        sink.child(masterFactory);
        sink.child(slaveFactory);
    }

    @Override
    protected void _close() {
        Misc.freeIfCloseable(getMetadata());
        Misc.free(masterFactory);
        Misc.free(slaveFactory);
    }

    private class AsOfJoinKeyedFastRecordCursor extends AbstractAsOfJoinFastRecordCursor {
        private final SingleRecordSink masterSinkTarget;
        private final SingleRecordSink slaveSinkTarget;
        private SqlExecutionCircuitBreaker circuitBreaker;
        private boolean origHasSlave;
        private int origSlaveFrameIndex = -1;
        private long origSlaveRowId = -1;

        public AsOfJoinKeyedFastRecordCursor(
                int columnSplit,
                Record nullRecord,
                int masterTimestampIndex,
                SingleRecordSink masterSinkTarget,
                int masterTimestampType,
                int slaveTimestampIndex,
                SingleRecordSink slaveSinkTarget,
                int slaveTimestampType,
                int lookahead
        ) {
            super(columnSplit, nullRecord, masterTimestampIndex, slaveTimestampIndex, masterTimestampType, slaveTimestampType, lookahead);
            this.masterSinkTarget = masterSinkTarget;
            this.slaveSinkTarget = slaveSinkTarget;
        }

        @Override
        public void close() {
            super.close();
            masterSinkTarget.close();
            slaveSinkTarget.close();
        }

        @Override
        public boolean hasNext() {
            if (isMasterHasNextPending) {
                masterHasNext = masterCursor.hasNext();
                isMasterHasNextPending = false;
            }
            if (!masterHasNext) {
                return false;
            }

            if (origSlaveRowId != -1) {
                slaveTimeFrameCursor.recordAt(slaveRecB, Rows.toRowID(origSlaveFrameIndex, origSlaveRowId));
            }
            record.hasSlave(origHasSlave);
            final long masterTimestamp = scaleTimestamp(masterRecord.getTimestamp(masterTimestampIndex), masterTimestampScale);
            if (masterTimestamp >= lookaheadTimestamp) {
                nextSlave(masterTimestamp);
            }

            // we have to set the `isMasterHasNextPending` only now since `nextSlave()` may throw DataUnavailableException
            // and in such case we do not want to call `masterCursor.hasNext()` during the next call to `this.hasNext()`.
            // if we are here then it's clear nextSlave() did not throw DataUnavailableException.
            isMasterHasNextPending = true;

            boolean hasSlave = record.hasSlave();
            origHasSlave = hasSlave;
            if (!hasSlave) {
                // the non-keyd algo did not find a matching record in the slave table.
                // this means the slave table does not have a single record with a timestamp that is less than or equal
                // to the master record's timestamp.
                // thus it cannot possibly have a record with a matching key. since matching timestamps is a prerequisite
                // before we even try to match keys -> we can safely skip the key matching part and report no match.
                return true;
            }

            // backup the original slave frame index and row id as returned from `nextSlave()`
            long rowId = slaveRecB.getRowId();
            int slaveFrameIndex = Rows.toPartitionIndex(rowId);
            origSlaveFrameIndex = slaveFrameIndex;
            long keyedRowId = Rows.toLocalRowID(rowId);
            origSlaveRowId = keyedRowId;

            if (symbolShortCircuit.isShortCircuit(masterRecord)) {
                // the master record's symbol does not match any symbol in the slave table, so we can skip the key matching part
                // and report no match.
                record.hasSlave(false);
                return true;
            }

            // ok, the non-keyed matcher found a record with a matching timestamp.
            // we have to make sure the JOIN keys match as well.
            masterSinkTarget.clear();
            masterKeySink.copy(masterRecord, masterSinkTarget);

            // make sure the cursor points to the frame corresponding to slaveRecB - since `nextSlave()` might have moved it under our feet
            TimeFrame timeFrame = slaveTimeFrameCursor.getTimeFrame();
            final int cursorFrameIndex = timeFrame.getFrameIndex();
            slaveTimeFrameCursor.jumpTo(slaveFrameIndex);
            slaveTimeFrameCursor.open();

            long rowLo = timeFrame.getRowLo();
            int keyedFrameIndex = timeFrame.getFrameIndex();
            for (; ; ) {
                long slaveTimestamp = scaleTimestamp(slaveRecB.getTimestamp(slaveTimestampIndex), slaveTimestampScale);
                if (toleranceInterval != Numbers.LONG_NULL && slaveTimestamp < masterTimestamp - toleranceInterval) {
                    // we are past the tolerance interval, no need to traverse the slave cursor any further
                    record.hasSlave(false);
                    break;
                }

                slaveSinkTarget.clear();
                slaveKeySink.copy(slaveRecB, slaveSinkTarget);
                if (masterSinkTarget.memeq(slaveSinkTarget)) {
                    // we have a match, that's awesome, no need to traverse the slave cursor!
                    break;
                }

                // let's try to move backwards in the slave cursor until we have a match
                keyedRowId--;
                if (keyedRowId < rowLo) {
                    // ops, we exhausted this frame, let's try the previous one
                    if (!slaveTimeFrameCursor.prev()) {
                        // there is no previous frame, we are done, no match :(
                        // if we are here, chances are we are also pretty slow because we are scanning the entire slave cursor
                        // until we either exhaust the cursor or find a matching key.
                        record.hasSlave(false);
                        break;
                    }
                    slaveTimeFrameCursor.open();

                    keyedFrameIndex = timeFrame.getFrameIndex();
                    keyedRowId = timeFrame.getRowHi() - 1;
                    rowLo = timeFrame.getRowLo();
                }
                slaveTimeFrameCursor.recordAt(slaveRecB, Rows.toRowID(keyedFrameIndex, keyedRowId));
                circuitBreaker.statefulThrowExceptionIfTripped();
            }

            // rewind the slave cursor to the original position so the next call to `nextSlave()` will not be affected
            slaveTimeFrameCursor.jumpTo(cursorFrameIndex);
            assert slaveFrameIndex == timeFrame.getFrameIndex();
            slaveTimeFrameCursor.open();
            return true;
        }

        public void of(RecordCursor masterCursor, TimeFrameRecordCursor slaveCursor, SqlExecutionCircuitBreaker circuitBreaker) {
            super.of(masterCursor, slaveCursor);
            symbolShortCircuit.of(slaveCursor);
            masterSinkTarget.reopen();
            slaveSinkTarget.reopen();
            this.circuitBreaker = circuitBreaker;
        }

        @Override
        public long preComputedStateSize() {
            return 0;
        }

        @Override
        public void toTop() {
            super.toTop();
            origSlaveFrameIndex = -1;
            origSlaveRowId = -1;
            origHasSlave = false;
        }
    }
}
