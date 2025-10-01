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
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.TimeFrameRecordCursor;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.model.JoinContext;
import io.questdb.std.IntLongHashMap;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.Rows;

public final class AsOfJoinMemoizedRecordCursorFactory extends AbstractJoinRecordCursorFactory {
    private final AsOfJoinMemoizedRecordCursor cursor;
    private final RecordSink masterKeySink;
    private final RecordSink slaveKeySink;
    private final int slaveSymbolColumnIndex;
    private final SymbolShortCircuit symbolShortCircuit;
    private final long toleranceInterval;

    public AsOfJoinMemoizedRecordCursorFactory(
            CairoConfiguration configuration,
            RecordMetadata metadata,
            RecordCursorFactory masterFactory,
            RecordSink masterKeySink,
            RecordCursorFactory slaveFactory,
            RecordSink slaveKeySink,
            int columnSplit,
            int slaveSymbolColumnIndex,
            SymbolShortCircuit symbolShortCircuit,
            JoinContext joinContext,
            long toleranceInterval
    ) {
        super(metadata, joinContext, masterFactory, slaveFactory);
        assert slaveFactory.supportsTimeFrameCursor();
        this.masterKeySink = masterKeySink;
        this.slaveKeySink = slaveKeySink;
        this.symbolShortCircuit = symbolShortCircuit;
        this.toleranceInterval = toleranceInterval;
        this.slaveSymbolColumnIndex = slaveSymbolColumnIndex;
        long maxSinkTargetHeapSize = (long) configuration.getSqlHashJoinValuePageSize() * configuration.getSqlHashJoinValueMaxPages();
        this.cursor = new AsOfJoinMemoizedRecordCursor(
                columnSplit,
                NullRecordFactory.getInstance(slaveFactory.getMetadata()),
                masterFactory.getMetadata().getTimestampIndex(),
                masterFactory.getMetadata().getTimestampType(),
                new SingleRecordSink(maxSinkTargetHeapSize, MemoryTag.NATIVE_RECORD_CHAIN),
                slaveFactory.getMetadata().getTimestampIndex(),
                slaveFactory.getMetadata().getTimestampType(),
                new SingleRecordSink(maxSinkTargetHeapSize, MemoryTag.NATIVE_RECORD_CHAIN),
                configuration.getSqlAsOfJoinLookAhead()
        );
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
        sink.type("AsOf Join Memoized Scan");
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

    private class AsOfJoinMemoizedRecordCursor extends AbstractKeyedAsOfJoinRecordCursor {

        private static final long NOT_REMEMBERED = Long.MIN_VALUE;

        private final IntLongHashMap symKeyToRowId = new IntLongHashMap(8, NOT_REMEMBERED);
        private final IntLongHashMap symKeyToValidityPeriodEnd = new IntLongHashMap();
        private final IntLongHashMap symKeyToValidityPeriodStart = new IntLongHashMap();

        public AsOfJoinMemoizedRecordCursor(
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
            super(columnSplit,
                    nullRecord,
                    masterTimestampIndex,
                    masterTimestampType,
                    masterSinkTarget,
                    slaveTimestampIndex,
                    slaveTimestampType,
                    slaveSinkTarget,
                    lookahead
            );
        }

        @Override
        public void of(RecordCursor masterCursor, TimeFrameRecordCursor slaveCursor, SqlExecutionCircuitBreaker circuitBreaker) {
            super.of(masterCursor, slaveCursor, circuitBreaker);
            symbolShortCircuit.of(slaveCursor);
        }

        private boolean isSlaveWithinToleranceInterval(long masterTimestamp, long slaveTimestamp) {
            return toleranceInterval == Numbers.LONG_NULL || slaveTimestamp >= masterTimestamp - toleranceInterval;
        }

        private void rememberSymbolLocation(long masterTimestamp, long slaveTimestamp, int slaveSymbolKey, long slaveRowId) {
            int rowIdKeyIndex = symKeyToRowId.keyIndex(slaveSymbolKey);
            if (rowIdKeyIndex >= 0) {
                // nothing remembered so far, store all the data with no further questions
                symKeyToRowId.put(slaveSymbolKey, slaveRowId);
                symKeyToValidityPeriodStart.put(slaveSymbolKey, slaveTimestamp);
                symKeyToValidityPeriodEnd.put(slaveSymbolKey, masterTimestamp);
            } else {
                int periodStartKeyIndex = symKeyToValidityPeriodStart.keyIndex(slaveSymbolKey);
                int periodEndKeyIndex = symKeyToValidityPeriodEnd.keyIndex(slaveSymbolKey);
                long periodStart = symKeyToValidityPeriodStart.valueAt(periodStartKeyIndex);
                long periodEnd = symKeyToValidityPeriodStart.valueAt(periodEndKeyIndex);
                if (masterTimestamp >= periodStart && slaveTimestamp <= periodEnd) {
                    // Period to remember overlaps existing remembered period -> merge them.

                    // Remembered period start indicates where we previously found the symbol, and we must not
                    // move it back. But we can move back if we remembered a period where we DIDN'T find the symbol.
                    if (slaveTimestamp < periodStart && slaveSymbolKey == SymbolTable.VALUE_NOT_FOUND) {
                        symKeyToValidityPeriodStart.putAt(periodStartKeyIndex, slaveSymbolKey, slaveTimestamp);
                    }
                    if (masterTimestamp > periodEnd) {
                        symKeyToValidityPeriodEnd.putAt(periodEndKeyIndex, slaveSymbolKey, masterTimestamp);
                    }
                }
            }
        }

        @Override
        protected void performKeyMatching(long masterTimestamp) {
            if (symbolShortCircuit.isShortCircuit(masterRecord)) {
                // the master record's symbol does not match any symbol in the slave table, so we can skip the key matching part
                // and report no match.
                record.hasSlave(false);
                return;
            }

            // ok, the non-keyed matcher found a record with a matching timestamp.
            // we have to make sure the JOIN keys match as well.
            masterSinkTarget.clear();
            masterKeySink.copy(masterRecord, masterSinkTarget);

            long rowLo = slaveTimeFrame.getRowLo();
            int keyedFrameIndex = slaveTimeFrame.getFrameIndex();
            long keyedRowId = Rows.toLocalRowID(slaveRecB.getRowId());

            StaticSymbolTable symbolTable = slaveTimeFrameCursor.getSymbolTable(slaveSymbolColumnIndex);
            CharSequence masterSymbolValue = symbolShortCircuit.getMasterValue(masterRecord);
            int slaveSymbolKey = symbolTable.keyOf(masterSymbolValue);
            long rememberedRowId = symKeyToRowId.get(slaveSymbolKey);
            long validityPeriodStart, validityPeriodEnd;
            if (rememberedRowId != NOT_REMEMBERED) {
                validityPeriodStart = symKeyToValidityPeriodStart.get(slaveSymbolKey);
                validityPeriodEnd = symKeyToValidityPeriodEnd.get(slaveSymbolKey);
            } else {
                validityPeriodStart = validityPeriodEnd = Numbers.LONG_NULL;
            }
            for (; ; ) {
                long slaveTimestamp = scaleTimestamp(slaveRecB.getTimestamp(slaveTimestampIndex), slaveTimestampScale);
                if (!isSlaveWithinToleranceInterval(masterTimestamp, slaveTimestamp)) {
                    // we are past the tolerance interval, no need to traverse the slave cursor any further
                    record.hasSlave(false);
                    long minRowScannedWithoutMatch = Rows.toRowID(keyedFrameIndex, keyedRowId);
                    // memorize that we didn't find the matching symbol by saving rowId as (-rowId - 1)
                    rememberSymbolLocation(masterTimestamp, slaveTimestamp, slaveSymbolKey, -minRowScannedWithoutMatch - 1);
                    break;
                }
                if (slaveTimestamp >= validityPeriodStart && slaveTimestamp <= validityPeriodEnd) {
                    // Our search is now within the validity period of the remembered symbol. Let's use it.
                    if (masterTimestamp > validityPeriodEnd) {
                        // The fact that we got to this point in our search means we haven't found a more recent symbol.
                        // Therefore, the remembered symbol is still the applicable one. Same for remembered
                        // non-existence of symbol. We can extend the validity period end to current masterTimestamp.
                        symKeyToValidityPeriodEnd.put(slaveSymbolKey, masterTimestamp);
                    }
                    if (rememberedRowId >= 0) {
                        // We saw this symbol at rememberedRowId. We can now reuse it and complete the search.
                        if (isSlaveWithinToleranceInterval(masterTimestamp, validityPeriodStart)) {
                            record.hasSlave(true);
                            slaveTimeFrameCursor.recordAt(slaveRecB, rememberedRowId);
                        } else {
                            record.hasSlave(false);
                        }
                        break;
                    } else {
                        // We remembered a period within which the symbol doesn't occur. Jump over the entire
                        // period and continue searching, unless that's outside the tolerance interval.
                        if (!isSlaveWithinToleranceInterval(masterTimestamp, validityPeriodStart)) {
                            record.hasSlave(false);
                            break;
                        }
                        long rememberedNoMatchRow = -rememberedRowId - 1;
                        keyedFrameIndex = Rows.toPartitionIndex(rememberedNoMatchRow);
                        keyedRowId = Rows.toLocalRowID(rememberedNoMatchRow);
                        slaveTimeFrameCursor.jumpTo(keyedFrameIndex);
                        slaveTimeFrameCursor.open();
                        slaveTimeFrameCursor.recordAt(slaveRecB, rememberedNoMatchRow);
                        rowLo = slaveTimeFrame.getRowLo();
                    }
                }

                if (slaveRecB.getInt(slaveSymbolColumnIndex) == slaveSymbolKey) {
                    record.hasSlave(true);
                    // We found the symbol that we don't remember. Memorize it now.
                    rememberSymbolLocation(masterTimestamp, slaveTimestamp, slaveSymbolKey, slaveRecB.getRowId());
                    break;
                }

                // let's try to move backwards in the slave cursor until we have a match
                keyedRowId--;
                if (keyedRowId < rowLo) {
                    // we exhausted this frame, let's try the previous one
                    if (!slaveTimeFrameCursor.prev()) {
                        // there is no previous frame, search space is exhausted
                        long minRowScannedWithoutMatch = Rows.toRowID(keyedFrameIndex, keyedRowId + 1);
                        // Memorize that we didn't find the matching symbol by saving rowId as (-rowId - 1)
                        rememberSymbolLocation(masterTimestamp, slaveTimestamp, slaveSymbolKey, -minRowScannedWithoutMatch - 1);
                        record.hasSlave(false);
                        break;
                    }
                    slaveTimeFrameCursor.open();

                    keyedFrameIndex = slaveTimeFrame.getFrameIndex();
                    keyedRowId = slaveTimeFrame.getRowHi() - 1;
                    rowLo = slaveTimeFrame.getRowLo();
                }
                slaveTimeFrameCursor.recordAt(slaveRecB, Rows.toRowID(keyedFrameIndex, keyedRowId));
                circuitBreaker.statefulThrowExceptionIfTripped();
            }
        }
    }
}
