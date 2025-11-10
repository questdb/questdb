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

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.TimeFrameRecordCursor;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.model.JoinContext;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.Rows;

/**
 * Dense ASOF JOIN cursor is an improvement over the Light cursor for the case where
 * the slave cursor is a {@link TimeFrameRecordCursor}. While the Light cursor uses a
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
        putFactoryType(sink);
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

    protected abstract void putFactoryType(PlanSink sink);

    protected abstract class AsOfJoinDenseRecordCursorBase extends AbstractKeyedAsOfJoinRecordCursor {
        protected static final int DUMMY_VALUE = -10;

        private final Map bwdScanKeyToRowId;
        private final Map fwdScanKeyToRowId;
        private long backwardRowId = -1;
        private boolean backwardScanExhausted;
        private long forwardRowId = -1;
        private boolean forwardScanExhausted;
        private boolean slaveCursorReadyForForwardScan;


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
            super(columnSplit, nullRecord, masterTimestampIndex, masterTimestampType, slaveTimestampIndex, slaveTimestampType, 1);
            this.fwdScanKeyToRowId = fwdScanKeyToRowId;
            this.bwdScanKeyToRowId = bwdScanKeyToRowId;
        }

        @Override
        public void close() {
            Misc.free(bwdScanKeyToRowId);
            Misc.free(fwdScanKeyToRowId);
            super.close();
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
            final long masterTimestamp = scaleTimestamp(masterRecord.getTimestamp(masterTimestampIndex), masterTimestampScale);
            final long minSlaveTimestamp = toleranceInterval == Numbers.LONG_NULL
                    ? Long.MIN_VALUE
                    : masterTimestamp - toleranceInterval;
            int slaveKeyToFind = setupSymbolKeyToFind();
            if (slaveKeyToFind == SymbolTable.VALUE_NOT_FOUND) {
                record.hasSlave(false);
                isMasterHasNextPending = true;
                return true;
            }

            if (forwardRowId == -1) {
                // No scanning done yet, initialize state of forward and backward scans
                nextSlave(masterTimestamp);
                if (!record.hasSlave()) {
                    // There are no prevailing slave rows (all are more recent than master row)
                    isMasterHasNextPending = true;
                    return true;
                }
                long rowId = slaveRecB.getRowId();
                backwardRowId = rowId;
                forwardRowId = rowId;
            }

            if (!slaveCursorReadyForForwardScan) {
                slaveTimeFrameCursor.jumpTo(Rows.toPartitionIndex(forwardRowId));
                slaveTimeFrameCursor.open();
                slaveTimeFrameCursor.recordAt(slaveRecB, forwardRowId);
                slaveCursorReadyForForwardScan = true;
            }

            MapKey key;
            MapValue value;
            if (!forwardScanExhausted) {
                scanForward(masterTimestamp, minSlaveTimestamp);
            }

            // Let's see if we saw a matching symbol in forward scan
            key = fwdScanKeyToRowId.withKey();
            putSlaveKeyToFind(key, slaveKeyToFind);
            value = key.findValue();
            if (value != null) {
                return setupSlaveRec(value.getLong(0), minSlaveTimestamp);
            }
            // Symbol not found, see if we already saw it in backward scan
            key = bwdScanKeyToRowId.withKey();
            putSlaveKeyToFind(key, slaveKeyToFind);
            value = key.findValue();
            if (value != null) {
                return setupSlaveRec(value.getLong(0), minSlaveTimestamp);
            }
            if (backwardScanExhausted) {
                // Symbol not found in backward scan, and the scan already reached the end, report no match
                record.hasSlave(false);
                isMasterHasNextPending = true;
                return true;
            }

            // Resume the backward scan
            slaveCursorReadyForForwardScan = false;
            slaveTimeFrameCursor.jumpTo(Rows.toPartitionIndex(backwardRowId));
            slaveTimeFrameCursor.open();
            long frameRowLo = Rows.toRowID(slaveTimeFrame.getFrameIndex(), slaveTimeFrame.getRowLo());
            while (true) {
                slaveTimeFrameCursor.recordAt(slaveRecB, backwardRowId);
                long slaveTimestamp = scaleTimestamp(slaveRecB.getTimestamp(slaveTimestampIndex), slaveTimestampScale);
                if (slaveTimestamp < minSlaveTimestamp) {
                    // minSlaveTimestamp will only get larger in later calls, it's safe to conclude backward scan now
                    backwardScanExhausted = true;
                    break;
                }
                key = bwdScanKeyToRowId.withKey();
                putSlaveJoinKey(key);
                value = key.createValue();
                if (value.isNew()) {
                    value.putLong(0, backwardRowId);
                }
                int slaveKey = getSlaveJoinKey();
                if (joinKeysMatch(slaveKeyToFind, slaveKey)) {
                    return setupSlaveRec(backwardRowId, minSlaveTimestamp);
                }
                if (backwardRowId > frameRowLo) {
                    backwardRowId--;
                } else {
                    if (!slaveTimeFrameCursor.prev()) {
                        backwardScanExhausted = true;
                        break;
                    }
                    slaveTimeFrameCursor.open();
                    int frameIndex = slaveTimeFrame.getFrameIndex();
                    frameRowLo = Rows.toRowID(frameIndex, slaveTimeFrame.getRowLo());
                    backwardRowId = Rows.toRowID(frameIndex, slaveTimeFrame.getRowHi() - 1);
                }
                circuitBreaker.statefulThrowExceptionIfTripped();
            }
            record.hasSlave(false);
            isMasterHasNextPending = true;
            return true;
        }

        @Override
        public void of(RecordCursor masterCursor, TimeFrameRecordCursor slaveCursor, SqlExecutionCircuitBreaker circuitBreaker) {
            super.of(masterCursor, slaveCursor, circuitBreaker);
            fwdScanKeyToRowId.reopen();
            fwdScanKeyToRowId.clear();
            bwdScanKeyToRowId.reopen();
            bwdScanKeyToRowId.clear();
        }

        @Override
        public void toTop() {
            super.toTop();
            if (fwdScanKeyToRowId.isOpen()) {
                fwdScanKeyToRowId.clear();
            }
            if (bwdScanKeyToRowId.isOpen()) {
                bwdScanKeyToRowId.clear();
            }
            isMasterHasNextPending = true;
            slaveCursorReadyForForwardScan = false;
            forwardScanExhausted = false;
            backwardRowId = -1;
            forwardRowId = -1;
        }

        private void scanForward(long masterTimestamp, long minSlaveTimestamp) {
            MapValue value;
            MapKey key;
            long frameRowHi = Rows.toRowID(slaveTimeFrame.getFrameIndex(), slaveTimeFrame.getRowHi());
            while (true) {
                slaveTimeFrameCursor.recordAt(slaveRecB, forwardRowId);
                long slaveTimestamp = scaleTimestamp(slaveRecB.getTimestamp(slaveTimestampIndex), slaveTimestampScale);
                if (slaveTimestamp > masterTimestamp) {
                    break;
                }
                if (slaveTimestamp >= minSlaveTimestamp) {
                    key = fwdScanKeyToRowId.withKey();
                    putSlaveJoinKey(key);
                    value = key.createValue();
                    value.putLong(0, slaveRecB.getRowId());
                }
                forwardRowId++;
                if (forwardRowId == frameRowHi) {
                    if (!slaveTimeFrameCursor.next()) {
                        forwardScanExhausted = true;
                        break;
                    }
                    slaveTimeFrameCursor.open();
                    int frameIndex = slaveTimeFrame.getFrameIndex();
                    frameRowHi = Rows.toRowID(frameIndex, slaveTimeFrame.getRowHi());
                    forwardRowId = Rows.toRowID(frameIndex, slaveTimeFrame.getRowLo());
                }
                circuitBreaker.statefulThrowExceptionIfTripped();
            }
        }

        private boolean setupSlaveRec(long slaveRowId, long minSlaveTimestamp) {
            slaveTimeFrameCursor.recordAt(slaveRecB, slaveRowId);
            long slaveTimestamp = scaleTimestamp(slaveRecB.getTimestamp(slaveTimestampIndex), slaveTimestampScale);
            record.hasSlave(slaveTimestamp >= minSlaveTimestamp);
            isMasterHasNextPending = true;
            return true;
        }

        protected abstract int getSlaveJoinKey();

        protected abstract boolean joinKeysMatch(int slaveKeyToFind, int slaveKey);

        @Override
        protected void performKeyMatching(long masterTimestamp) {
            throw new UnsupportedOperationException("AsOfJoinDenseRecordCursorBase does not use performKeyMatching");
        }

        protected abstract void putSlaveJoinKey(MapKey key);

        protected abstract void putSlaveKeyToFind(MapKey key, int slaveKeyToFind);

        protected abstract int setupSymbolKeyToFind();
    }

    static {
        TYPES_KEY.add(ColumnType.INT);
        TYPES_VALUE.add(ColumnType.LONG);
    }
}
