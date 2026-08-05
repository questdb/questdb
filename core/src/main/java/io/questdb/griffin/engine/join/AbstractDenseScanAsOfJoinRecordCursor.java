/*******************************************************************************
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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.TimeFrameCursor;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.Rows;
import org.jetbrains.annotations.Nullable;

/**
 * Shared, resilient forward+backward "Dense" keyed-ASOF scan, extracted so that both the Dense
 * cursor (which uses it as its primary algorithm) and the Memoized cursor (which falls back to it
 * on dense-timestamp data) can drive it. The scan maintains two hashmaps keyed by the join key:
 * {@code fwdScanKeyToRowId} records the last slave row at or before the master timestamp seen during
 * the forward scan, and {@code bwdScanKeyToRowId} records the first (newest) slave row seen while
 * resuming the backward scan. See {@link AsOfJoinDenseRecordCursorFactoryBase} for the algorithm's
 * rationale.
 * <p>
 * Concrete subclasses supply the join-key hooks. The single-master-row entry point is
 * {@link #resolveViaDenseScan(long, long, int)}; the caller is responsible for advancing the master
 * cursor, computing {@code masterTimestamp}/{@code minSlaveTimestamp}, and resolving the sought key.
 */
public abstract class AbstractDenseScanAsOfJoinRecordCursor extends AbstractKeyedAsOfJoinRecordCursor {

    // Map layout for the dense-scan hashmaps: INT join key -> LONG slave row id.
    public static final ArrayColumnTypes DENSE_SCAN_TYPES_KEY = new ArrayColumnTypes();
    public static final ArrayColumnTypes DENSE_SCAN_TYPES_VALUE = new ArrayColumnTypes();

    private final Map bwdScanKeyToRowId;
    private final Map fwdScanKeyToRowId;
    private long backwardRowId = -1;
    private boolean backwardScanExhausted;
    private long forwardRowId = -1;
    private boolean forwardScanExhausted;
    private boolean slaveCursorReadyForForwardScan;

    protected AbstractDenseScanAsOfJoinRecordCursor(
            int columnSplit,
            Map fwdScanKeyToRowId,
            Map bwdScanKeyToRowId,
            Record nullRecord,
            int masterTimestampIndex,
            int masterTimestampType,
            int slaveTimestampIndex,
            int slaveTimestampType,
            int lookahead
    ) {
        super(columnSplit, nullRecord, masterTimestampIndex, masterTimestampType, slaveTimestampIndex, slaveTimestampType, lookahead);
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
    public void setMemoryTracker(@Nullable MemoryTracker tracker) {
        // Bound lazily before of() reopens them; map malloc/free nets on the per-query counter.
        fwdScanKeyToRowId.setMemoryTracker(tracker);
        bwdScanKeyToRowId.setMemoryTracker(tracker);
    }

    protected void clearDenseScanMapsIfOpen() {
        if (fwdScanKeyToRowId.isOpen()) {
            fwdScanKeyToRowId.clear();
        }
        if (bwdScanKeyToRowId.isOpen()) {
            bwdScanKeyToRowId.clear();
        }
    }

    protected abstract int getSlaveJoinKey();

    protected abstract boolean joinKeysMatch(int slaveKeyToFind, int slaveKey);

    protected abstract void putSlaveJoinKey(MapKey key);

    protected abstract void putSlaveKeyToFind(MapKey key, int slaveKeyToFind);

    protected void reopenAndClearDenseScanMaps() {
        // Reopen the scan maps before of() adopts the cursors so an open-time breach frees each exactly once.
        fwdScanKeyToRowId.reopen();
        fwdScanKeyToRowId.clear();
        bwdScanKeyToRowId.reopen();
        bwdScanKeyToRowId.clear();
    }

    protected void resetDenseScanState() {
        slaveCursorReadyForForwardScan = false;
        forwardScanExhausted = false;
        backwardScanExhausted = false;
        backwardRowId = -1;
        forwardRowId = -1;
    }

    /**
     * Resolve one master row via the resilient forward+backward Dense scan. Sets {@code record.hasSlave}
     * and always returns {@code true}. The caller must have advanced {@code masterCursor}, computed
     * {@code masterTimestamp} and {@code minSlaveTimestamp}, and resolved {@code slaveKeyToFind}
     * (guaranteed != {@code SymbolTable.VALUE_NOT_FOUND}).
     */
    protected boolean resolveViaDenseScan(long masterTimestamp, long minSlaveTimestamp, int slaveKeyToFind) {
        if (forwardRowId == -1) {
            // No scanning done yet, initialize state of forward and backward scans
            nextSlave(masterTimestamp);
            if (!record.hasSlave()) {
                // There are no prevailing slave rows (all are more recent than master row)
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
        return true;
    }

    protected abstract int setupSymbolKeyToFind();

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
        return true;
    }

    static {
        DENSE_SCAN_TYPES_KEY.add(ColumnType.INT);
        DENSE_SCAN_TYPES_VALUE.add(ColumnType.LONG);
    }
}
