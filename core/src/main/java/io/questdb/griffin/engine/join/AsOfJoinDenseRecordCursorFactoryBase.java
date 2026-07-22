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

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.sql.ParquetDecodeHint;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.TimeFrameCursor;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.model.JoinContext;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.Rows;

/**
 * Dense ASOF JOIN cursor is an improvement over the Light cursor for the case where
 * the slave cursor is a {@link TimeFrameCursor}. While the Light cursor uses a
 * forward-only scan of the slave cursor, the Dense cursor uses two scans: forward and
 * backward. They both start at the slave row that matches the first master row by
 * timestamp (as determined by {@link AbstractAsOfJoinFastRecordCursor#nextSlave
 * nextSlave()}).
 * <p>
 * When encountering another master row, we first resume the forward scan from the
 * previous position until the master timestamp. While scanning, we memorize the join
 * key at each row in a hashmap. Then we check whether the key is in the hashmap. If
 * yes, we're done.
 * <p>
 * Up to this point, the algorithm is identical to the Light cursor. The key difference
 * is, we didn't start the forward scan at the top of the slave cursor, and not finding
 * the key in the hashmap doesn't mean there's no match. We must continue with the
 * backward scan.
 * <p>
 * If we didn't find the join key in the hashmap of the forward scan, we check whether
 * it's in the backward scan's hashmap. If not, we resume the backward scan until we
 * find the key or exhaust the backward scan. In the backward scan, we memorize only new
 * keys (not already encountered in backward scan).
 * <p>
 * The Dense algorithm is the best choice when the master rows are densely interleaved
 * with slave rows. For each master row, we only need to scan a few slave rows. If the
 * interleaving is sparse, we'll still scan everything from the previous position, while
 * the matching row could be only a few rows behind the master.
 * <p>
 * The Fast/Memoized algos are better for sparse interleaving because they use binary
 * search to quickly zero in on the latest slave row ahead of master, and then search
 * backward. In a typical case, this means they are able to entirely ignore most of the
 * slave rows.
 */
public abstract class AsOfJoinDenseRecordCursorFactoryBase extends AbstractJoinRecordCursorFactory {
    protected static final ArrayColumnTypes TYPES_KEY = new ArrayColumnTypes();
    protected static final ArrayColumnTypes TYPES_VALUE = new ArrayColumnTypes();
    private final long toleranceInterval;
    protected AsOfJoinDenseRecordCursorBase cursor;

    public AsOfJoinDenseRecordCursorFactoryBase(
            RecordMetadata metadata,
            RecordCursorFactory masterFactory,
            RecordCursorFactory slaveFactory,
            JoinContext joinContext,
            long toleranceInterval
    ) {
        super(metadata, joinContext, masterFactory, slaveFactory);
        this.toleranceInterval = toleranceInterval;

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
            // reopens its SingleRecordSinks (in the keyed variants), so the
            // first malloc lands under the bound tracker.
            cursor.setMemoryTracker(executionContext.getMemoryTracker());
            slaveCursor.setParquetDecodeHint(ParquetDecodeHint.MONOTONIC);
            cursor.of(masterCursor, slaveCursor, executionContext.getCircuitBreaker());
            return cursor;
        } catch (Throwable e) {
            Misc.free(slaveCursor);
            Misc.free(masterCursor);
            // of() reopens the sinks/maps before adopting the cursors, so close() here frees only the partial heap.
            Misc.free(cursor);
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
        putFactoryType(sink);
        sink.attr("condition").val(joinContext);
        sink.child(masterFactory);
        sink.child(slaveFactory);
    }

    @Override
    protected void _close() {
        final AsOfJoinDenseRecordCursorBase cursor = this.cursor;
        this.cursor = null;
        Throwable failure = closeJoinOwnersBestEffort();
        failure = Misc.freeBestEffort(failure, cursor);
        CairoException.rethrowCleanupFailure(failure);
    }

    protected abstract void putFactoryType(PlanSink sink);

    protected abstract class AsOfJoinDenseRecordCursorBase extends AbstractDenseScanAsOfJoinRecordCursor {
        protected static final int DUMMY_VALUE = -10;
        // Adaptive prelude: when >= 0, serve master rows via the proven Fast keyed loop (super.hasNext()
        // + performKeyMatching back-scan below) until the cumulative back-scan length exceeds the budget,
        // then switch to the resilient forward-scan Dense mode for the remaining rows. -1 = pure Dense.
        private long adaptiveBackScanBudget = -1;
        private long adaptiveBackScanUsed;
        private boolean adaptiveDenseMode;

        protected AsOfJoinDenseRecordCursorBase(
                int columnSplit,
                Map fwdScanKeyToRowId,
                Map bwdScanKeyToRowId,
                Record nullRecord,
                int masterTimestampIndex,
                int masterTimestampType,
                int slaveTimestampIndex,
                int slaveTimestampType
        ) {
            super(columnSplit, fwdScanKeyToRowId, bwdScanKeyToRowId, nullRecord, masterTimestampIndex, masterTimestampType, slaveTimestampIndex, slaveTimestampType, 1);
        }

        @Override
        public boolean hasNext() {
            if (adaptiveBackScanBudget >= 0 && !adaptiveDenseMode) {
                // Fast keyed loop with correct frame save/restore (AbstractKeyedAsOfJoinRecordCursor),
                // driving performKeyMatching (the targeted back-scan) below.
                boolean has = super.hasNext();
                if (adaptiveBackScanUsed > adaptiveBackScanBudget) {
                    switchToDenseMode();
                }
                return has;
            }
            // Consult the breaker at the top, so an empty master still observes cancellation.
            circuitBreaker.statefulThrowExceptionIfTripped();
            if (!masterCursor.hasNext()) {
                return false;
            }
            final long masterTimestamp = scaleTimestamp(masterRecord.getTimestamp(masterTimestampIndex), masterTimestampScale);
            final long minSlaveTimestamp = toleranceInterval == Numbers.LONG_NULL
                    ? Long.MIN_VALUE
                    : masterTimestamp - toleranceInterval;
            int slaveKeyToFind = setupSymbolKeyToFind();
            if (slaveKeyToFind == SymbolTable.VALUE_NOT_FOUND) {
                record.hasSlave(false);
                return true;
            }
            return resolveViaDenseScan(masterTimestamp, minSlaveTimestamp, slaveKeyToFind);
        }

        @Override
        public void of(RecordCursor masterCursor, TimeFrameCursor slaveCursor, SqlExecutionCircuitBreaker circuitBreaker) {
            reopenAndClearDenseScanMaps();
            resetDenseScanState();
            adaptiveBackScanUsed = 0;
            adaptiveDenseMode = false;
            super.of(masterCursor, slaveCursor, circuitBreaker);
        }

        @Override
        public void toTop() {
            super.toTop();
            clearDenseScanMapsIfOpen();
            resetDenseScanState();
            adaptiveBackScanUsed = 0;
            adaptiveDenseMode = false;
        }

        @Override
        protected void performKeyMatching(long masterTimestamp) {
            // Targeted Fast-style back-scan, used only by the adaptive prelude. Mirrors
            // AsOfJoinFastRecordCursorFactory.performKeyMatching but resolves keys via this cursor's
            // key methods. The enclosing super.hasNext() handles slave-frame save/restore.
            int slaveKeyToFind = setupSymbolKeyToFind();
            if (slaveKeyToFind == SymbolTable.VALUE_NOT_FOUND) {
                record.hasSlave(false);
                return;
            }
            long rowLo = slaveTimeFrame.getRowLo();
            int keyedFrameIndex = slaveTimeFrame.getFrameIndex();
            long keyedRowId = Rows.toLocalRowID(slaveRecB.getRowId());
            long scanned = 0;
            for (; ; ) {
                if (toleranceInterval != Numbers.LONG_NULL) {
                    long slaveTimestamp = scaleTimestamp(slaveRecB.getTimestamp(slaveTimestampIndex), slaveTimestampScale);
                    if (slaveTimestamp < masterTimestamp - toleranceInterval) {
                        record.hasSlave(false);
                        break;
                    }
                }
                int slaveKey = getSlaveJoinKey();
                if (joinKeysMatch(slaveKeyToFind, slaveKey)) {
                    record.hasSlave(true);
                    break;
                }
                keyedRowId--;
                if (keyedRowId < rowLo) {
                    if (!slaveTimeFrameCursor.prev()) {
                        record.hasSlave(false);
                        break;
                    }
                    slaveTimeFrameCursor.open();
                    keyedFrameIndex = slaveTimeFrame.getFrameIndex();
                    keyedRowId = slaveTimeFrame.getRowHi() - 1;
                    rowLo = slaveTimeFrame.getRowLo();
                }
                slaveTimeFrameCursor.recordAt(slaveRecB, Rows.toRowID(keyedFrameIndex, keyedRowId));
                scanned++;
                circuitBreaker.statefulThrowExceptionIfTripped();
            }
            adaptiveBackScanUsed += scanned;
        }

        // Switch from the Fast prelude to resilient Dense mode. Reset both the Fast forward trackers
        // and the Dense scan state so the next hasNext re-initialises Dense cleanly from the top.
        private void switchToDenseMode() {
            adaptiveDenseMode = true;
            resetDenseScanState();
            clearDenseScanMapsIfOpen();
            slaveFrameRow = Long.MIN_VALUE;
            slaveFrameIndex = -1;
            lookaheadTimestamp = Long.MIN_VALUE;
            origSlaveRowId = -1;
            origSlaveFrameIndex = -1;
            origHasSlave = false;
        }

        public void setAdaptiveBackScanBudget(long budget) {
            this.adaptiveBackScanBudget = budget;
        }
    }

    static {
        TYPES_KEY.add(ColumnType.INT);
        TYPES_VALUE.add(ColumnType.LONG);
    }
}
