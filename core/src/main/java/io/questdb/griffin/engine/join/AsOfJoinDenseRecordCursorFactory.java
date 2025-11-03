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
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.TimeFrameRecordCursor;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.model.JoinContext;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.Rows;

/**
 * Dense ASOF JOIN cursor is an improvement over the Light cursor for the case of
 * single-symbol JOIN ON condition, where the slave cursor is a TimeFrameRecordCursor.
 * While the Light cursor only needs a forward-only scan of the slave cursor, the Dense
 * cursor uses two scans: forward and backward. The both start at the slave row that matches
 * the first master row by timestamp (as determined by nextSlave()).
 * <p>
 * When encountering another master row, we first resume the forward scan from the previous
 * position until the master timestamp. While scanning, we memorize the symbol at each row
 * in a hashmap. Then we check whether the symbol is in the hashmap. If yes, we're done.
 * If not, we check whether it's in the backward scan's hashmap. If not, we resume the
 * backward scan until we find the symbol or exhaust the backward scan. In the backward scan,
 * we memorize only new symbols (not already encountered in backward scan).
 * <p>
 * Dense ASOF JOIN algo is the best choice when the master rows are densely interleaved with
 * slave rows. For each master row, we only need to scan a few slave rows. If the interleaving
 * is sparse, we'll still scan everything from the previous position, while the matching row
 * could be only a few rows behind the master.
 * <p>
 * The Fast/Memoized algos are better for sparse interleaving because they use binary search to
 * quickly zero in on the latest slave row ahead of master, and then search backward. In a
 * typical case, this means they are able to entirely ignore most of the slave rows.
 */
public final class AsOfJoinDenseRecordCursorFactory extends AbstractJoinRecordCursorFactory {
    private static final ArrayColumnTypes TYPES_KEY = new ArrayColumnTypes();
    private static final ArrayColumnTypes TYPES_VALUE = new ArrayColumnTypes();
    private final AsofJoinColumnAccessHelper columnAccessHelper;
    private final AsOfJoinDenseRecordCursor cursor;
    private final int slaveSymbolColumnIndex;
    private final long toleranceInterval;

    public AsOfJoinDenseRecordCursorFactory(
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
        this.cursor = new AsOfJoinDenseRecordCursor(
                configuration,
                columnSplit,
                NullRecordFactory.getInstance(slaveFactory.getMetadata()),
                masterFactory.getMetadata().getTimestampIndex(),
                masterFactory.getMetadata().getTimestampType(),
                slaveFactory.getMetadata().getTimestampIndex(),
                slaveFactory.getMetadata().getTimestampType()
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
        sink.type("AsOf Join Dense Scan");
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

    private class AsOfJoinDenseRecordCursor extends AbstractKeyedAsOfJoinRecordCursor {

        private final Map bwdScanKeyToRowId;
        private final Map fwdScanKeyToRowId;
        private long backwardRowId = -1;
        private boolean backwardScanDone;
        private long forwardRowId = -1;
        private boolean forwardScanDone;
        private boolean slaveCursorReadyForForwardScan;

        public AsOfJoinDenseRecordCursor(
                CairoConfiguration configuration,
                int columnSplit,
                Record nullRecord,
                int masterTimestampIndex,
                int masterTimestampType,
                int slaveTimestampIndex,
                int slaveTimestampType
        ) {
            super(
                    columnSplit,
                    nullRecord,
                    masterTimestampIndex,
                    masterTimestampType,
                    slaveTimestampIndex,
                    slaveTimestampType,
                    configuration.getSqlAsOfJoinLookAhead())
            ;
            this.fwdScanKeyToRowId = MapFactory.createUnorderedMap(configuration, TYPES_KEY, TYPES_VALUE);
            this.bwdScanKeyToRowId = MapFactory.createUnorderedMap(configuration, TYPES_KEY, TYPES_VALUE);
        }

        @Override
        public void close() {
            super.close();
            Misc.free(fwdScanKeyToRowId);
            Misc.free(bwdScanKeyToRowId);
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
            int symbolKeyToFind = columnAccessHelper.getSlaveKey(masterRecord);

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
            if (!forwardScanDone) {
                scanForward(masterTimestamp, minSlaveTimestamp);
            }

            // Let's see if we saw a matching symbol in forward scan
            key = fwdScanKeyToRowId.withKey();
            key.putInt(symbolKeyToFind);
            value = key.findValue();
            if (value != null) {
                return setupSlaveRec(value.getLong(0), minSlaveTimestamp);
            }
            // Symbol not found, see if we already saw it in backward scan
            key = bwdScanKeyToRowId.withKey();
            key.putInt(symbolKeyToFind);
            value = key.findValue();
            if (value != null) {
                return setupSlaveRec(value.getLong(0), minSlaveTimestamp);
            }
            if (backwardScanDone) {
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
                    backwardScanDone = true;
                    // minSlaveTimestamp will only get larger in later calls, it's safe to conclude backward scan now
                    break;
                }
                key = bwdScanKeyToRowId.withKey();
                int symbolKey = slaveRecB.getInt(slaveSymbolColumnIndex);
                key.putInt(symbolKey);
                value = key.createValue();
                if (value.isNew()) {
                    value.putLong(0, backwardRowId);
                }
                if (symbolKey == symbolKeyToFind) {
                    return setupSlaveRec(backwardRowId, minSlaveTimestamp);
                }
                if (backwardRowId > frameRowLo) {
                    backwardRowId--;
                } else {
                    if (!slaveTimeFrameCursor.prev()) {
                        backwardScanDone = true;
                        break;
                    }
                    slaveTimeFrameCursor.open();
                    int frameIndex = slaveTimeFrame.getFrameIndex();
                    frameRowLo = Rows.toRowID(frameIndex, slaveTimeFrame.getRowLo());
                    backwardRowId = Rows.toRowID(frameIndex, slaveTimeFrame.getRowHi() - 1);
                }
            }
            record.hasSlave(false);
            isMasterHasNextPending = true;
            return true;
        }

        @Override
        public void of(RecordCursor masterCursor, TimeFrameRecordCursor slaveCursor, SqlExecutionCircuitBreaker circuitBreaker) {
            super.of(masterCursor, slaveCursor, circuitBreaker);
            columnAccessHelper.of(slaveCursor);
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
            forwardScanDone = false;
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
                    key.putInt(slaveRecB.getInt(slaveSymbolColumnIndex));
                    value = key.createValue();
                    value.putLong(0, slaveRecB.getRowId());
                }
                forwardRowId++;
                if (forwardRowId == frameRowHi) {
                    if (!slaveTimeFrameCursor.next()) {
                        forwardScanDone = true;
                        break;
                    }
                    slaveTimeFrameCursor.open();
                    int frameIndex = slaveTimeFrame.getFrameIndex();
                    frameRowHi = Rows.toRowID(frameIndex, slaveTimeFrame.getRowHi());
                    forwardRowId = Rows.toRowID(frameIndex, slaveTimeFrame.getRowLo());
                }
            }
        }

        private boolean setupSlaveRec(long slaveRowId, long minSlaveTimestamp) {
            slaveTimeFrameCursor.recordAt(slaveRecB, slaveRowId);
            long slaveTimestamp = scaleTimestamp(slaveRecB.getTimestamp(slaveTimestampIndex), slaveTimestampScale);
            record.hasSlave(slaveTimestamp >= minSlaveTimestamp);
            isMasterHasNextPending = true;
            return true;
        }

        @Override
        protected void performKeyMatching(long masterTimestamp) {
            throw new UnsupportedOperationException("AsOfJoinDenseRecordCursor does not use performKeyMatching");
        }
    }

    static {
        TYPES_KEY.add(ColumnType.INT);
        TYPES_VALUE.add(ColumnType.LONG);
    }
}
