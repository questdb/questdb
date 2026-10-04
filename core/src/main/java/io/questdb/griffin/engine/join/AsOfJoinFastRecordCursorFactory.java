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

package io.questdb.griffin.engine.join;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.SingleRecordSink;
import io.questdb.cairo.sql.ParquetDecodeHint;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.TimeFrameCursor;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.table.SymbolTranslatingRecord;
import io.questdb.griffin.model.JoinContext;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.Rows;
import org.jetbrains.annotations.Nullable;

public final class AsOfJoinFastRecordCursorFactory extends AbstractJoinRecordCursorFactory {
    private final AsOfJoinKeyedFastRecordCursor cursor;
    private final RecordSink masterKeySink;
    // writes the master key with the key positions that share a slave column rotated, null when none do
    private final @Nullable RecordSink rotatedMasterKeySink;
    private final RecordSink slaveKeySink;
    private final SymbolShortCircuit symbolShortCircuit;
    private final long toleranceInterval;
    private @Nullable SymbolTranslatingRecord symbolTranslatingRecord;

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
            long toleranceInterval,
            int @Nullable [] masterSymbolKeyColumnIndices,
            int @Nullable [] slaveSymbolKeyColumnIndices,
            @Nullable RecordSink rotatedMasterKeySink
    ) {
        super(metadata, joinContext, masterFactory, slaveFactory);
        assert slaveFactory.supportsTimeFrameCursor();
        this.masterKeySink = masterKeySink;
        this.rotatedMasterKeySink = rotatedMasterKeySink;
        this.slaveKeySink = slaveKeySink;
        long maxSinkTargetHeapSize = (long) configuration.getSqlHashJoinValuePageSize() * configuration.getSqlHashJoinValueMaxPages();
        final RecordMetadata masterMetadata = masterFactory.getMetadata();
        final RecordMetadata slaveMetadata = slaveFactory.getMetadata();
        final SingleRecordSink masterSinkTarget = new SingleRecordSink(maxSinkTargetHeapSize, MemoryTag.NATIVE_RECORD_CHAIN, SingleRecordSink.OWNER_ASOF_JOIN,
                SingleRecordSink.CONFIG_KEYS_ASOF_JOIN);
        final SingleRecordSink slaveSinkTarget = new SingleRecordSink(maxSinkTargetHeapSize, MemoryTag.NATIVE_RECORD_CHAIN, SingleRecordSink.OWNER_ASOF_JOIN,
                SingleRecordSink.CONFIG_KEYS_ASOF_JOIN);
        // Only a join whose keys share a slave column gets the cursor that checks the master key
        // before it moves the slave cursor, so every other join keeps the plain cursor.
        if (rotatedMasterKeySink == null) {
            this.cursor = new AsOfJoinKeyedFastRecordCursor(
                    columnSplit,
                    NullRecordFactory.getInstance(slaveMetadata),
                    masterMetadata.getTimestampIndex(),
                    masterMetadata.getTimestampType(),
                    masterSinkTarget,
                    slaveMetadata.getTimestampIndex(),
                    slaveMetadata.getTimestampType(),
                    slaveSinkTarget,
                    configuration.getSqlAsOfJoinLookAhead()
            );
        } else {
            this.cursor = new AsOfJoinKeyedFastCheckedRecordCursor(
                    columnSplit,
                    NullRecordFactory.getInstance(slaveMetadata),
                    masterMetadata.getTimestampIndex(),
                    masterMetadata.getTimestampType(),
                    masterSinkTarget,
                    slaveMetadata.getTimestampIndex(),
                    slaveMetadata.getTimestampType(),
                    slaveSinkTarget,
                    configuration.getSqlAsOfJoinLookAhead(),
                    rotatedMasterKeySink
            );
        }
        this.symbolShortCircuit = symbolShortCircuit;
        this.toleranceInterval = toleranceInterval;
        this.symbolTranslatingRecord = masterSymbolKeyColumnIndices != null
                ? new SymbolTranslatingRecord(configuration, masterFactory.getMetadata().getColumnCount(), masterSymbolKeyColumnIndices, slaveSymbolKeyColumnIndices)
                : null;
    }

    @Override
    public boolean followedOrderByAdvice() {
        return masterFactory.followedOrderByAdvice();
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        RecordCursor masterCursor = masterFactory.getCursor(executionContext);
        TimeFrameCursor slaveCursor = null;
        try {
            slaveCursor = slaveFactory.getTimeFrameCursor(executionContext);
            // Bind the per-query tracker before of(); the cursor's of()
            // reopens its SingleRecordSinks, so the first malloc lands
            // under the bound tracker.
            cursor.setMemoryTracker(executionContext.getMemoryTracker());
            slaveCursor.setParquetDecodeHint(ParquetDecodeHint.MONOTONIC);
            cursor.of(masterCursor, slaveCursor, executionContext.getCircuitBreaker());
            return cursor;
        } catch (Throwable th) {
            Misc.free(slaveCursor);
            Misc.free(masterCursor);
            // of() reopens the sinks and caches before adopting the cursors, so close() here frees
            // only the partial heap.
            Misc.free(cursor);
            throw th;
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
        sink.type("AsOf Join Fast");
        sink.attr("condition").val(joinContext);
        if (symbolTranslatingRecord != null) {
            sink.attr("symbolKeyJoin").val(true);
        }
        if (rotatedMasterKeySink != null) {
            sink.attr("sharedKeyCheck").val(true);
        }
        sink.child(masterFactory);
        sink.child(slaveFactory);
    }

    @Override
    protected void _close() {
        final SymbolTranslatingRecord symbolTranslatingRecord = this.symbolTranslatingRecord;
        this.symbolTranslatingRecord = null;
        Throwable failure = closeJoinOwnersBestEffort();
        failure = Misc.freeBestEffort(failure, symbolTranslatingRecord);
        failure = Misc.freeBestEffort(failure, symbolShortCircuit);
        CairoException.rethrowCleanupFailure(failure);
    }

    private class AsOfJoinKeyedFastCheckedRecordCursor extends AsOfJoinKeyedFastRecordCursor {
        private final RecordSink rotatedMasterKeySink;

        public AsOfJoinKeyedFastCheckedRecordCursor(
                int columnSplit,
                Record nullRecord,
                int masterTimestampIndex,
                int masterTimestampType,
                SingleRecordSink masterSinkTarget,
                int slaveTimestampIndex,
                int slaveTimestampType,
                SingleRecordSink slaveSinkTarget,
                int lookahead,
                RecordSink rotatedMasterKeySink
        ) {
            super(columnSplit, nullRecord, masterTimestampIndex, masterTimestampType, masterSinkTarget, slaveTimestampIndex, slaveTimestampType, slaveSinkTarget, lookahead);
            this.rotatedMasterKeySink = rotatedMasterKeySink;
        }

        @Override
        public boolean hasNext() {
            // Consult the breaker at the top, so an empty master still observes cancellation.
            circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
            if (!masterCursor.hasNext()) {
                return false;
            }
            if (!isMasterKeyMatchable()) {
                // No slave row matches master values that differ at key positions sharing a slave
                // column, so skip moving the slave cursor and scanning it.
                record.hasSlave(false);
                return true;
            }
            return findSlaveRecord();
        }

        // A slave row writes one value at all key positions that share its column, so it matches
        // only a master key that equals its rotated copy. The method leaves the master key in
        // masterSinkTarget and its rotated copy in slaveSinkTarget, and performKeyMatching() writes
        // both sinks again before it reads them.
        private boolean isMasterKeyMatchable() {
            if (symbolTranslatingRecord != null) {
                symbolTranslatingRecord.resetNonExistentKeyFlag();
                masterSinkTarget.clear();
                masterKeySink.copy(masterKeyRecord, masterSinkTarget);
                if (symbolTranslatingRecord.hadNonExistentKey()) {
                    return false;
                }
            } else {
                if (symbolShortCircuit.isShortCircuit(masterRecord)) {
                    return false;
                }
                masterSinkTarget.clear();
                masterKeySink.copy(masterKeyRecord, masterSinkTarget);
            }
            slaveSinkTarget.clear();
            rotatedMasterKeySink.copy(masterKeyRecord, slaveSinkTarget);
            return masterSinkTarget.memeq(slaveSinkTarget);
        }
    }

    private class AsOfJoinKeyedFastRecordCursor extends AbstractKeyedAsOfJoinRecordCursor {
        protected final SingleRecordSink masterSinkTarget;
        protected final SingleRecordSink slaveSinkTarget;
        // Record used for master key serialization. Set once in of() to either
        // masterRecord or SymbolTranslatingRecord wrapping it, so that getInt()
        // on symbol key columns returns slave symbol IDs.
        protected Record masterKeyRecord;

        public AsOfJoinKeyedFastRecordCursor(
                int columnSplit,
                Record nullRecord,
                int masterTimestampIndex,
                int masterTimestampType,
                SingleRecordSink masterSinkTarget,
                int slaveTimestampIndex,
                int slaveTimestampType,
                SingleRecordSink slaveSinkTarget,
                int lookahead
        ) {
            super(columnSplit, nullRecord, masterTimestampIndex, masterTimestampType, slaveTimestampIndex, slaveTimestampType, lookahead);
            this.masterSinkTarget = masterSinkTarget;
            this.slaveSinkTarget = slaveSinkTarget;
        }

        @Override
        public void close() {
            super.close();
            masterSinkTarget.close();
            slaveSinkTarget.close();
            Misc.free(symbolTranslatingRecord);
            symbolShortCircuit.close();
        }

        @Override
        public void of(RecordCursor masterCursor, TimeFrameCursor slaveCursor, SqlExecutionCircuitBreaker circuitBreaker) {
            // Reopen the sinks, the short circuit's cache and the translation caches before super.of()
            // adopts the cursors so an open-time breach frees each exactly once.
            masterSinkTarget.reopen();
            slaveSinkTarget.reopen();
            symbolShortCircuit.reopen();
            if (symbolTranslatingRecord != null) {
                symbolTranslatingRecord.initSources(masterCursor, slaveCursor);
            }
            super.of(masterCursor, slaveCursor, circuitBreaker);
            masterKeyRecord = masterRecord;
            if (symbolTranslatingRecord != null) {
                symbolTranslatingRecord.of(masterRecord);
                masterKeyRecord = symbolTranslatingRecord;
            } else {
                symbolShortCircuit.of(slaveCursor);
            }
        }

        @Override
        public void setMemoryTracker(@Nullable MemoryTracker tracker) {
            masterSinkTarget.setMemoryTracker(tracker);
            slaveSinkTarget.setMemoryTracker(tracker);
            if (symbolTranslatingRecord != null) {
                symbolTranslatingRecord.setMemoryTracker(tracker);
            }
            symbolShortCircuit.setMemoryTracker(tracker);
        }

        @Override
        protected void performKeyMatching(long masterTimestamp) {
            if (symbolTranslatingRecord != null) {
                // The non-keyed matcher found a record with a matching timestamp.
                // We have to make sure the JOIN keys match as well.
                symbolTranslatingRecord.resetNonExistentKeyFlag();
                masterSinkTarget.clear();
                masterKeySink.copy(masterKeyRecord, masterSinkTarget);
                // Check if any symbol key was VALUE_NOT_FOUND during copy.
                if (symbolTranslatingRecord.hadNonExistentKey()) {
                    record.hasSlave(false);
                    return;
                }
            } else {
                if (symbolShortCircuit.isShortCircuit(masterRecord)) {
                    record.hasSlave(false);
                    return;
                }
                masterSinkTarget.clear();
                masterKeySink.copy(masterKeyRecord, masterSinkTarget);
            }

            long rowLo = slaveTimeFrame.getRowLo();
            int keyedFrameIndex = slaveTimeFrame.getFrameIndex();
            long keyedRowId = Rows.toLocalRowID(slaveRecB.getRowId());

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
                    record.hasSlave(true);
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

                    keyedFrameIndex = slaveTimeFrame.getFrameIndex();
                    keyedRowId = slaveTimeFrame.getRowHi() - 1;
                    rowLo = slaveTimeFrame.getRowLo();
                }
                slaveTimeFrameCursor.recordAt(slaveRecB, Rows.toRowID(keyedFrameIndex, keyedRowId));
                circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
            }
        }
    }
}
