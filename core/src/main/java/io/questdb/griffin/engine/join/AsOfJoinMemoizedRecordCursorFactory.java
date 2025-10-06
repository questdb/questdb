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
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.TimeFrameRecordCursor;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.model.JoinContext;
import io.questdb.std.IntLongHashMap;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.Rows;

public final class AsOfJoinMemoizedRecordCursorFactory extends AbstractJoinRecordCursorFactory {
    private final AsofJoinColumnAccessHelper columnAccessHelper;
    private final AsOfJoinMemoizedRecordCursor cursor;
    private final int slaveSymbolColumnIndex;
    private final long toleranceInterval;

    public AsOfJoinMemoizedRecordCursorFactory(
            CairoConfiguration configuration,
            RecordMetadata metadata,
            RecordCursorFactory masterFactory,
            RecordCursorFactory slaveFactory,
            int columnSplit,
            int slaveSymbolColumnIndex,
            AsofJoinColumnAccessHelper columnAccessHelper,
            JoinContext joinContext,
            long toleranceInterval
    ) {
        super(metadata, joinContext, masterFactory, slaveFactory);
        assert slaveFactory.supportsTimeFrameCursor();
        this.columnAccessHelper = columnAccessHelper;
        this.toleranceInterval = toleranceInterval;
        this.slaveSymbolColumnIndex = slaveSymbolColumnIndex;
        this.cursor = new AsOfJoinMemoizedRecordCursor(
                columnSplit,
                NullRecordFactory.getInstance(slaveFactory.getMetadata()),
                masterFactory.getMetadata().getTimestampIndex(),
                masterFactory.getMetadata().getTimestampType(),
                slaveFactory.getMetadata().getTimestampIndex(),
                slaveFactory.getMetadata().getTimestampType(),
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

        // These three hashmaps are used in strict parallel, always putting the same key in each.
        // When accessing them, we can look up the key index only once, and use it for all three maps.
        private final IntLongHashMap symKeyToRowId = new IntLongHashMap();
        private final IntLongHashMap symKeyToValidityPeriodEnd = new IntLongHashMap();
        private final IntLongHashMap symKeyToValidityPeriodStart = new IntLongHashMap();

        public AsOfJoinMemoizedRecordCursor(
                int columnSplit,
                Record nullRecord,
                int masterTimestampIndex,
                int masterTimestampType,
                int slaveTimestampIndex,
                int slaveTimestampType,
                int lookahead
        ) {
            super(columnSplit, nullRecord, masterTimestampIndex, masterTimestampType, slaveTimestampIndex, slaveTimestampType, lookahead);
        }

        @Override
        public void of(RecordCursor masterCursor, TimeFrameRecordCursor slaveCursor, SqlExecutionCircuitBreaker circuitBreaker) {
            super.of(masterCursor, slaveCursor, circuitBreaker);
            symKeyToRowId.clear();
            symKeyToValidityPeriodStart.clear();
            symKeyToValidityPeriodEnd.clear();
            columnAccessHelper.of(slaveCursor);
        }

        private boolean isSlaveWithinToleranceInterval(long masterTimestamp, long slaveTimestamp) {
            return toleranceInterval == Numbers.LONG_NULL || slaveTimestamp >= masterTimestamp - toleranceInterval;
        }

        private void memorizeSymbolLocation(
                long masterTimestamp,
                long slaveTimestamp,
                int slaveSymbolKey,
                long slaveRowId,
                boolean onlyIfNew
        ) {
            int slaveKeyIndex = symKeyToRowId.keyIndex(slaveSymbolKey);
            if (slaveKeyIndex >= 0) {
                // nothing remembered so far, store all the data with no further questions
                symKeyToRowId.putAt(slaveKeyIndex, slaveSymbolKey, slaveRowId);
                symKeyToValidityPeriodStart.putAt(slaveKeyIndex, slaveSymbolKey, slaveTimestamp);
                symKeyToValidityPeriodEnd.putAt(slaveKeyIndex, slaveSymbolKey, masterTimestamp);
                return;
            }
            if (onlyIfNew) {
                return;
            }
            long periodStart = symKeyToValidityPeriodStart.valueAt(slaveKeyIndex);
            long periodEnd = symKeyToValidityPeriodEnd.valueAt(slaveKeyIndex);
            if (masterTimestamp >= periodStart && slaveTimestamp <= periodEnd) {
                // The period to memorize overlaps existing remembered period.
                // Let's explain under what circumstances we may end up in this branch.
                // We'll use this notation:
                //   ! ---- | we found the symbal at the start of this period
                //   x ---- | we did not find the symbol anywhere within this period
                //   ? ---- | we may or may not have found the symbol within this period
                //
                // We have this invariant: the symbol does not occur anywhere inside either the
                // remembered or the new period. It may only appear at its very start.
                // So, all of these are impossible:
                //
                // ! ------------ | remembered period
                //          ! --------------- | new period
                //
                //          ! --------------- | remembered period
                // ! ------------ | new period
                //
                // ! ------------------ | remembered period
                //     ! ---------- | new period
                //
                //     ! ---------- | remembered period
                // ! ------------------ | new period
                //
                // Furthermore, since our search never rescans the remembered period, it will never give up
                // in the middle of it and try to memorize it didn't find anything. So, this is impossible:
                //
                // ! ------------ | remembered period
                //          x --------------- | new period
                //
                // Also, in this case:
                //
                // ? ------------ | remembered period
                //                x --------------- | new period
                //
                // the algo reached the remembered period, and used it. It didn't call this method, it just
                // directly extended the validity period.
                //
                // This also won't occur, since it would imply we searched beyond the end of the search
                // space:
                //
                //        x --------------- | remembered period
                // ? ---------------------------- | new period
                //
                // So, this is the only way to end up in this branch:
                //
                //          x --------------- | remembered period
                // ? ------------ | new period
                //
                // The master record is in the middle of the remembered period, we skipped it,
                // continued to search further back, and either found the symbol or failed again.
                // We must update the period start, and save the rowId of the symbol.
                assert slaveTimestamp < periodStart : "slaveTimestamp >= periodStart";
                assert masterTimestamp < periodEnd : "masterTimestamp >= periodEnd";
                symKeyToRowId.putAt(slaveKeyIndex, slaveSymbolKey, slaveRowId);
                symKeyToValidityPeriodStart.putAt(slaveKeyIndex, slaveSymbolKey, slaveTimestamp);
            } else if (masterTimestamp - slaveTimestamp > periodEnd - periodStart) {
                // Periods aren't overlapping. Let's memorize the new one if it's longer, saving more work.
                symKeyToRowId.putAt(slaveKeyIndex, slaveSymbolKey, slaveRowId);
                symKeyToValidityPeriodStart.putAt(slaveKeyIndex, slaveSymbolKey, slaveTimestamp);
                symKeyToValidityPeriodEnd.putAt(slaveKeyIndex, slaveSymbolKey, masterTimestamp);
            }
        }

        @Override
        protected void performKeyMatching(long masterTimestamp) {
            if (columnAccessHelper.isShortCircuit(masterRecord)) {
                // the master record's symbol does not match any symbol in the slave table, so we can skip the key matching part
                // and report no match.
                record.hasSlave(false);
                return;
            }

            // ok, the non-keyed matcher found a record with a matching timestamp.
            // we have to make sure the JOIN keys match as well.

            final StaticSymbolTable symbolTable = slaveTimeFrameCursor.getSymbolTable(slaveSymbolColumnIndex);
            final CharSequence masterSymbolValue = columnAccessHelper.getMasterValue(masterRecord);
            final int slaveSymbolKey = symbolTable.keyOf(masterSymbolValue);
            final int slaveKeyIndex = symKeyToRowId.keyIndex(slaveSymbolKey);
            final long rememberedRowId, validityPeriodStart, validityPeriodEnd;
            if (slaveKeyIndex < 0) {
                rememberedRowId = symKeyToRowId.valueAt(slaveKeyIndex);
                validityPeriodStart = symKeyToValidityPeriodStart.valueAt(slaveKeyIndex);
                validityPeriodEnd = symKeyToValidityPeriodEnd.valueAt(slaveKeyIndex);
            } else {
                rememberedRowId = NOT_REMEMBERED;
                validityPeriodStart = validityPeriodEnd = Numbers.LONG_NULL;
            }

            long rowLo = slaveTimeFrame.getRowLo();
            int keyedFrameIndex = slaveTimeFrame.getFrameIndex();
            long keyedRowId = Rows.toLocalRowID(slaveRecB.getRowId());

            for (; ; ) {
                long slaveTimestamp = scaleTimestamp(slaveRecB.getTimestamp(slaveTimestampIndex), slaveTimestampScale);
                boolean didExtendValidityPeriod = false;
                boolean didJumpOverValidityPeriod = false;
                if (slaveTimestamp >= validityPeriodStart && slaveTimestamp <= validityPeriodEnd) {
                    // Our search is now within the validity period of the remembered symbol. Let's use it.
                    // The above check also ensures that rememberedRowId != NOT_REMEMBERED.
                    if (masterTimestamp > validityPeriodEnd) {
                        // We started our search from timestamp that is more recent than the remembered period.
                        // The fact that we got to this point means we haven't found a more recent symbol.
                        // Therefore, the remembered symbol is still the applicable one. Same for the remembered
                        // non-existence of symbol. We can extend the validity period end to current masterTimestamp.
                        symKeyToValidityPeriodEnd.putAt(slaveKeyIndex, slaveSymbolKey, masterTimestamp);
                        didExtendValidityPeriod = true;
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
                        // period and continue searching, unless the remembered period extends beyond the
                        // tolerance interval. If we have just extended the validity period, it definitely
                        // extends beyond the tolerance interval.
                        if (didExtendValidityPeriod || !isSlaveWithinToleranceInterval(masterTimestamp, validityPeriodStart)) {
                            record.hasSlave(false);
                            break;
                        }
                        didJumpOverValidityPeriod = true;
                        long rememberedNoMatchRow = -rememberedRowId - 1;
                        keyedFrameIndex = Rows.toPartitionIndex(rememberedNoMatchRow);
                        keyedRowId = Rows.toLocalRowID(rememberedNoMatchRow);
                        slaveTimeFrameCursor.jumpTo(keyedFrameIndex);
                        slaveTimeFrameCursor.open();
                        slaveTimeFrameCursor.recordAt(slaveRecB, rememberedNoMatchRow);
                        rowLo = slaveTimeFrame.getRowLo();
                        slaveTimestamp = scaleTimestamp(slaveRecB.getTimestamp(slaveTimestampIndex), slaveTimestampScale);
                    }
                }
                if (!isSlaveWithinToleranceInterval(masterTimestamp, slaveTimestamp)) {
                    // We have been searching outside the remembered period and are past the tolerance interval.
                    // Stop and report no match.
                    record.hasSlave(false);
                    long minRowScannedWithoutMatch = Rows.toRowID(keyedFrameIndex, keyedRowId);
                    // memorize that we didn't find the matching symbol by saving rowId as (-rowId - 1)
                    memorizeSymbolLocation(masterTimestamp, slaveTimestamp, slaveSymbolKey, -minRowScannedWithoutMatch - 1, false);
                    break;
                }
                int thisSymbolKey = slaveRecB.getInt(slaveSymbolColumnIndex);
                if (thisSymbolKey == slaveSymbolKey) {
                    record.hasSlave(true);
                    // We found the symbol that we don't remember. Memorize it now.
                    memorizeSymbolLocation(masterTimestamp, slaveTimestamp, slaveSymbolKey, slaveRecB.getRowId(), false);
                    break;
                } else {
                    // This isn't the symbol we're looking for, but memorize it anyway in the hope that some future
                    // master row will need it.
                    memorizeSymbolLocation(masterTimestamp, slaveTimestamp, thisSymbolKey, slaveRecB.getRowId(), true);
                }

                // let's try to move backwards in the slave cursor until we have a match
                keyedRowId--;
                if (keyedRowId < rowLo) {
                    // we exhausted this frame, let's try the previous one
                    if (!slaveTimeFrameCursor.prev()) {
                        // there is no previous frame, search space is exhausted
                        long minRowScannedWithoutMatch = Rows.toRowID(keyedFrameIndex, keyedRowId + 1);
                        long rememberedNoMatchRowId = -rememberedRowId - 1;
                        if (!didJumpOverValidityPeriod || minRowScannedWithoutMatch < rememberedNoMatchRowId) {
                            // This isn't the edge case where we just jumped over the remembered period
                            // with no symbol, only to realize there's nothing left beyond it.
                            // Memorize that we didn't find the matching symbol by saving rowId as (-rowId - 1)
                            memorizeSymbolLocation(masterTimestamp, slaveTimestamp, slaveSymbolKey, -minRowScannedWithoutMatch - 1, false);
                        }
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
